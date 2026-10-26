use super::*;

#[tokio::test]
async fn cancelling_explicit_cleanup_wait_does_not_lose_worker_ownership() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (started, seen) = tokio::sync::oneshot::channel();
    let (allow, allowed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let req = read_message(&mut s).await.unwrap();
        let first = register(&req, &id);
        write_message(
            &mut s,
            &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        assert_eq!(unregister(&remove, &id), first);
        started.send(()).unwrap();
        allowed.await.unwrap();
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        write_message(
            &mut s,
            &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        unregister(&remove, &id);
        write_message(
            &mut s,
            &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        assert!(read_message(&mut s).await.is_err());
    });
    let client = ServiceNotificationClient::connect_clear(address, WAIT)
        .await
        .unwrap();
    let guard = client.register_filter(&filter(), None).await.unwrap();
    tokio::select! {
        biased;
        _=seen=>{},
        result=guard.unregister(None)=>panic!("unexpected early cleanup: {result:?}"),
    }
    allow.send(()).unwrap();
    let next = client.register_filter(&filter(), None).await.unwrap();
    next.unregister(None).await.unwrap();
    client.close(None).await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn close_timeout_still_stops_socket_and_invalidates_surviving_guard() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        write_message(
            &mut s,
            &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let remove = read_message(&mut s).await.unwrap();
        unregister(&remove, &id);
        assert!(read_message(&mut s).await.is_err());
    });
    let client = ServiceNotificationClient::connect_clear(address, WAIT)
        .await
        .unwrap();
    let guard = client.register_filter(&filter(), None).await.unwrap();
    assert!(matches!(
        client.close(Some(Duration::from_millis(20))).await,
        Err(NotificationError::Common(Error::Timeout))
    ));
    assert!(matches!(
        guard.unregister(None).await,
        Err(NotificationError::Common(Error::Closed))
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn definitive_registration_error_is_not_unknown_and_all_filter_flags_are_preserved() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut s, id) = connect_peer(listener).await;
        let req = read_message(&mut s).await.unwrap();
        register(&req, &id);
        let mut error = peer::reply(&req, "ClientOperationFailure", vec![]);
        error.headers.push(
            Header::object(mssf_transport::message::FAULT, |e| {
                e.i64(-2147024809);
                e.boolean(false);
                Ok(())
            })
            .unwrap(),
        );
        write_message(&mut s, &error).await.unwrap();
        for bits in 0..4 {
            let req = read_message(&mut s).await.unwrap();
            register(&req, &id);
            let mut d = Decoder::new(&req.body);
            d.object(|d| {
                d.string()?;
                d.pointer(|d, _| {
                    d.u64()?;
                    d.object(|_| Ok(()))?;
                    d.object(|d| {
                        assert_eq!(d.u32()?, bits);
                        Ok(())
                    })
                })?;
                Ok(())
            })
            .unwrap();
            d.finish().unwrap();
            write_message(
                &mut s,
                &peer::reply(&req, "RegisterServiceNotificationFilterReply", vec![]),
            )
            .await
            .unwrap();
            let remove = read_message(&mut s).await.unwrap();
            unregister(&remove, &id);
            write_message(
                &mut s,
                &peer::reply(&remove, "UnregisterServiceNotificationFilterReply", vec![]),
            )
            .await
            .unwrap();
        }
        assert!(read_message(&mut s).await.is_err());
    });
    let client = ServiceNotificationClient::connect_clear(address, WAIT)
        .await
        .unwrap();
    assert!(matches!(
        client.register_filter(&filter(), None).await,
        Err(NotificationError::Common(Error::Remote { .. }))
    ));
    for bits in 0..4 {
        let mut f = filter();
        f.name_prefix = bits & 1 != 0;
        f.primary_only = bits & 2 != 0;
        client
            .register_filter(&f, None)
            .await
            .unwrap()
            .unregister(None)
            .await
            .unwrap();
    }
    client.close(None).await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn thirty_three_pages_during_connection_overflow_bounded_ingress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut s = peer::accept(listener).await;
        read_message(&mut s).await.unwrap();
        // The worker is not started until session connection confirmation.
        for index in 1..=33 {
            if write_message(&mut s, &page(index, index as i64, 1))
                .await
                .is_err()
            {
                break;
            }
        }
        assert!(read_message(&mut s).await.is_err());
    });
    assert!(matches!(
        ServiceNotificationClient::connect_clear(address, WAIT).await,
        Err(NotificationError::ResourceLimit(_))
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn constructor_preserves_notification_ingress_lag_before_session_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut stream = peer::accept(listener).await;
        read_message(&mut stream).await.unwrap();
        let endpoints = vec!["x".repeat(32768); 32];
        for index in 1..=8 {
            let notification = page_with_endpoints(index, index as i64, 1, &endpoints);
            if write_message(&mut stream, &notification).await.is_err() {
                break;
            }
        }
        assert!(read_message(&mut stream).await.is_err());
    });
    assert!(matches!(
        ServiceNotificationClient::connect_clear(address, Duration::from_secs(10)).await,
        Err(NotificationError::Lagged)
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn decoded_storage_overflow_sends_failure_ack_and_terminates_without_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (registered, registration_done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, id) = connect_peer(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        register(&request, &id);
        write_message(
            &mut stream,
            &peer::reply(&request, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        // The injected failure must follow local registration completion.
        registration_done.await.unwrap();
        // Small empty strings have little wire storage but large Vec<String>
        // backing storage. Entry count alone is below the delivery limit.
        let page = page_with_endpoints(91, 1, 32, &vec![String::new(); 4096]);
        assert!(page.body.len() < 256 * 1024);
        write_message(&mut stream, &page).await.unwrap();
        ack(&mut stream, 91, false).await;
        assert!(read_message(&mut stream).await.is_err());
    });
    let mut client = ServiceNotificationClient::connect_clear(address, WAIT)
        .await
        .unwrap();
    let _registration = client.register_filter(&filter(), None).await.unwrap();
    registered.send(()).unwrap();
    assert!(matches!(
        client.recv().await,
        Some(Err(NotificationError::Lagged))
    ));
    assert!(client.recv().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn fatal_update_before_registration_resumes_can_fail_a_successful_gateway_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen, request_seen) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, id) = connect_peer(listener).await;
        let request = read_message(&mut stream).await.unwrap();
        register(&request, &id);
        seen.send(()).unwrap();
        write_message(
            &mut stream,
            &peer::reply(&request, "RegisterServiceNotificationFilterReply", vec![]),
        )
        .await
        .unwrap();
        let page = page_with_endpoints(92, 1, 32, &vec![String::new(); 4096]);
        write_message(&mut stream, &page).await.unwrap();
        ack(&mut stream, 92, false).await;
        assert!(read_message(&mut stream).await.is_err());
    });
    let mut client = ServiceNotificationClient::connect_clear(address, WAIT)
        .await
        .unwrap();
    let description = filter();
    let mut pending = Box::pin(client.register_filter(&description, None));
    tokio::select! {
        biased;
        _ = request_seen => {}
        result = &mut pending => panic!("registration resumed too early: {}", result.is_ok()),
    }
    // Let the worker process the fatal page without polling the registration.
    task.await.unwrap();
    assert!(matches!(pending.await, Err(NotificationError::Lagged)));
    assert!(matches!(
        client.recv().await,
        Some(Err(NotificationError::Lagged))
    ));
    assert!(client.recv().await.is_none());
}
