use super::*;
use fube_transport::{
    connection::{read_message, write_message},
    message::{ListenInstance, MessageId},
};
use tokio::net::TcpListener;

#[test]
fn scoped_errors_preserve_common_sources_cloning_and_domain_diagnostics() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<NotificationError>();
    for error in [
        Error::Timeout,
        Error::Remote {
            code: -7,
            message: "native failure".into(),
        },
        Error::ResourceLimit("shared decoder"),
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "peer reset",
        )),
    ] {
        let display = error.to_string();
        let scoped = NotificationError::from(error).clone();
        assert_eq!(scoped.to_string(), display);
        let source = std::error::Error::source(&scoped)
            .unwrap()
            .downcast_ref::<Error>()
            .unwrap();
        match source {
            Error::Remote { code, message } => {
                assert_eq!(*code, -7);
                assert_eq!(message, "native failure");
            }
            Error::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
            Error::ResourceLimit(resource) => assert_eq!(*resource, "shared decoder"),
            Error::Timeout => {}
            _ => panic!("unexpected common error"),
        }
    }
    for error in [
        NotificationError::Lagged,
        NotificationError::ResourceLimit("notification filters"),
    ] {
        let cloned = error.clone();
        assert!(std::error::Error::source(&cloned).is_none());
        assert!(cloned.to_string().starts_with("notification "));
    }
}

#[tokio::test]
async fn notification_terminal_cause_is_not_replaced_by_generic_connection_shutdown() {
    let (session, _cleanups, _peer) = session().await;
    session.receiver.fail(NotificationError::Lagged);
    session.channel.abort();
    let mut client = ServiceNotificationClient {
        session: session.clone(),
        worker: None,
        gateway: GatewayDescription {
            address: String::new(),
            node_id: 0,
            node_instance: 0,
            node_name: "test".into(),
        },
    };
    assert!(matches!(
        client.recv().await,
        Some(Err(NotificationError::Lagged))
    ));
    assert!(client.recv().await.is_none());
    assert!(matches!(
        client
            .register_filter(
                &ServiceNotificationFilter {
                    name: "fabric:/App/Service".parse().unwrap(),
                    name_prefix: false,
                    primary_only: false,
                },
                None
            )
            .await,
        Err(NotificationError::Lagged)
    ));
    assert!(matches!(
        client.close(None).await,
        Err(NotificationError::Lagged)
    ));
}

#[tokio::test]
async fn known_driver_failure_precedes_buffered_updates_without_worker_progress() {
    use crate::resolution_protocol::{Cuid, ReplicaSet, TableEntry};
    use fube_transport::serialization::serde::{CountedStrings, CountedValues, Guid, Pointer};
    let (session, _cleanups, _peer) = session().await;
    let page = wire::Page {
        id: wire::PageId {
            activity: wire::Activity {
                guid: Guid([1; 16]),
                index: 1,
            },
            index: 0,
        },
        generation: Generation::default(),
        versions: Pointer(None),
        entries: CountedValues(vec![Pointer(Some(wire::Entry {
            entry: Pointer(Some(TableEntry {
                cuid: Cuid {
                    id: Guid(Uuid::new_v4().to_bytes_le()),
                },
                name: "fabric:/App/Service".into(),
                replicas: ReplicaSet {
                    stateful: false,
                    primary_valid: false,
                    primary: String::new(),
                    replicas: CountedStrings(vec![]),
                    lookup: 1,
                },
                found: true,
            })),
            primary_only: false,
        }))]),
    };
    let memory = session.receiver.budget.reserve(4096).unwrap();
    Progress::default()
        .commit(page, memory, &session.receiver)
        .unwrap();
    let mut client = ServiceNotificationClient {
        session: session.clone(),
        worker: None,
        gateway: GatewayDescription {
            address: String::new(),
            node_id: 0,
            node_instance: 0,
            node_name: "test".into(),
        },
    };
    session.channel.abort();
    assert!(matches!(
        client.recv().await,
        Some(Err(NotificationError::Common(Error::Closed)))
    ));
    assert!(client.recv().await.is_none());
    session.channel.close(teardown_deadline()).await.unwrap();
}

async fn session() -> (Arc<Session>, mpsc::Receiver<Cleanup>, tokio::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        read_message(&mut peer).await.unwrap();
        write_message(
            &mut peer,
            &Message::listen_instance(
                MessageId {
                    guid: [1; 16],
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        peer
    });
    let channel = GatewayChannel::connect_unsecured_loopback(
        address,
        GatewayProfile::Notifications,
        notification_limits(),
        deadline_after(Duration::from_secs(3)).unwrap(),
    )
    .await
    .unwrap();
    let target = channel.resolve_target(&address.to_string()).unwrap();
    let (cleanup, cleanups) = mpsc::channel(MAX_CLEANUPS);
    (
        Arc::new(Session {
            channel,
            target,
            id: "session-test".into(),
            timeout: Duration::from_secs(3),
            state: Mutex::new(Registrations {
                next: 1,
                ..Default::default()
            }),
            cleanup,
            receiver: Arc::new(Receiver::default()),
            wake: Notify::new(),
        }),
        cleanups,
        task.await.unwrap(),
    )
}

#[tokio::test]
async fn full_cleanup_command_path_is_terminal_without_another_queue_write() {
    let (session, _cleanups, _peer) = session().await;
    for id in 0..MAX_CLEANUPS {
        session
            .cleanup
            .try_send(Cleanup {
                id: id as u64,
                deadline: deadline_after(Duration::from_secs(3)).unwrap(),
                complete: None,
            })
            .unwrap_or_else(|_| panic!("queue should fit"));
    }
    session
        .state
        .lock()
        .unwrap()
        .filters
        .insert(100, RegistrationState::Active);
    session.drop_registration(100);
    assert!(session.channel.terminal_error().is_some());
    assert!(matches!(
        session.receiver.recv().await,
        Some(Err(NotificationError::ResourceLimit(_)))
    ));
    assert!(session.receiver.recv().await.is_none());
    session.channel.close(teardown_deadline()).await.unwrap();
}

#[tokio::test]
async fn guard_handoff_has_one_owner_and_retains_slot_until_confirmation() {
    let (session, mut cleanups, _peer) = session().await;
    session
        .state
        .lock()
        .unwrap()
        .filters
        .insert(1, RegistrationState::Active);
    let registration = ServiceNotificationRegistration {
        session: Arc::downgrade(&session),
        id: 1,
        armed: true,
    };
    drop(registration);
    let command = cleanups.recv().await.unwrap();
    assert_eq!(command.id, 1);
    assert_eq!(
        session.state.lock().unwrap().filters.get(&1),
        Some(&RegistrationState::Cleanup)
    );
    session.drop_registration(1);
    assert!(cleanups.try_recv().is_err());
    assert_eq!(session.state.lock().unwrap().filters.len(), 1);
    session.stop();
    session.channel.close(teardown_deadline()).await.unwrap();
}

#[tokio::test]
async fn aborted_unpolled_worker_publishes_terminal_and_stale_guards_cannot_reopen() {
    let (session, _cleanups, _peer) = session().await;
    let exit = WorkerExit(session.clone());
    let task = tokio::spawn(async move {
        let _exit = exit;
        std::future::pending::<()>().await;
    });
    task.abort();
    let _ = task.await;
    assert!(matches!(
        session.receiver.recv().await,
        Some(Err(NotificationError::Common(Error::Protocol(_))))
    ));
    assert!(session.receiver.recv().await.is_none());
    let old = ServiceNotificationRegistration {
        session: Arc::downgrade(&session),
        id: 1,
        armed: true,
    };
    assert!(matches!(
        old.unregister(None).await,
        Err(NotificationError::Common(Error::Closed))
    ));
    session.channel.close(teardown_deadline()).await.unwrap();
}

#[tokio::test]
async fn cancelled_unsent_registration_releases_slot_without_retiring_session() {
    let (session, _cleanups, _peer) = session().await;
    session
        .state
        .lock()
        .unwrap()
        .filters
        .insert(1, RegistrationState::Registering);
    drop(Mutation {
        session: Arc::downgrade(&session),
        id: 1,
        sent: None,
        armed: true,
    });
    assert!(session.state.lock().unwrap().filters.is_empty());
    assert!(session.channel.terminal_error().is_none());
    session.stop();
    session.channel.close(teardown_deadline()).await.unwrap();
}

#[tokio::test]
async fn tracked_invisible_registration_retracts_and_visible_timeout_is_uncertain() {
    for visible in [false, true] {
        let (session, _cleanups, mut peer) = session().await;
        session
            .state
            .lock()
            .unwrap()
            .filters
            .insert(1, RegistrationState::Registering);
        let (mut pending, sent) = session.channel.request_tracked(
            &session.target,
            request_message(
                "RegisterServiceNotificationFilterRequest",
                vec![],
                Instant::now() + Duration::from_millis(30),
            )
            .unwrap(),
            Instant::now() + Duration::from_millis(30),
        );
        let mutation = Mutation {
            session: Arc::downgrade(&session),
            id: 1,
            sent: Some(sent),
            armed: true,
        };
        if visible {
            tokio::select! {
                biased;
                message = read_message(&mut peer) => {
                    assert_eq!(message.unwrap().properties().unwrap().action,
                        "RegisterServiceNotificationFilterRequest");
                },
                result = &mut pending => panic!("unconfirmed mutation completed: {}", result.is_ok()),
            }

            assert!(matches!(pending.await, Err(Error::Timeout)));
            drop(mutation);
            assert!(matches!(
                session.receiver.recv().await,
                Some(Err(NotificationError::Common(Error::Protocol(
                    "notification registration outcome is uncertain"
                ))))
            ));
            assert!(session.channel.terminal_error().is_some());
        } else {
            drop(mutation);
            assert!(matches!(pending.await, Err(Error::Closed)));
            assert!(session.channel.terminal_error().is_none());
            assert!(session.state.lock().unwrap().filters.is_empty());
        }
        session.stop();
        session.channel.close(teardown_deadline()).await.unwrap();
        assert!(read_message(&mut peer).await.is_err());
    }
}

#[tokio::test]
async fn cancelling_close_stops_scoped_worker_socket_and_invalidates_surviving_guard() {
    let (session, cleanups, mut peer) = session().await;
    session
        .state
        .lock()
        .unwrap()
        .filters
        .insert(1, RegistrationState::Active);
    let guard = ServiceNotificationRegistration {
        session: Arc::downgrade(&session),
        id: 1,
        armed: true,
    };
    let native = session.channel.receiver().unwrap();
    let (pages, incoming) = mpsc::channel(MAX_PAGES);
    let worker_session = session.clone();
    let exit = WorkerExit(session.clone());
    let (done, finished) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let _exit = exit;
        tokio::join!(
            stage_loop(worker_session.clone(), native, pages),
            run(
                worker_session.clone(),
                incoming,
                cleanups,
                Progress::default()
            ),
        );
        worker_session
            .channel
            .closed(teardown_deadline())
            .await
            .unwrap();
        done.send(()).unwrap();
    });
    let client = ServiceNotificationClient {
        session: session.clone(),
        worker: Some(worker),
        gateway: GatewayDescription {
            address: String::new(),
            node_id: 0,
            node_instance: 0,
            node_name: "test".into(),
        },
    };
    let mut closing = Box::pin(client.close(None));
    tokio::select! {
        biased;
        request = read_message(&mut peer) => {
            assert_eq!(request.unwrap().properties().unwrap().action,
                "UnregisterServiceNotificationFilterRequest");
        }
        result = &mut closing => panic!("unconfirmed close completed: {}", result.is_ok()),
    }
    drop(closing);
    timeout_at(Instant::now() + Duration::from_secs(1), finished)
        .await
        .unwrap()
        .unwrap();
    assert!(session.state.lock().unwrap().closed);
    assert!(session.state.lock().unwrap().filters.is_empty());
    assert!(matches!(
        guard.unregister(None).await,
        Err(NotificationError::Common(Error::Closed))
    ));
    assert!(timeout_at(
        Instant::now() + Duration::from_secs(1),
        read_message(&mut peer)
    )
    .await
    .unwrap()
    .is_err());
}
