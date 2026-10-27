use super::*;
use crate::message::IPC;

fn deadline() -> Instant {
    deadline_after(Duration::from_secs(2)).unwrap()
}
fn ipc(
    capacity: usize,
) -> (
    Connection,
    tokio::io::DuplexStream,
    mpsc::Receiver<IpcRequest>,
) {
    let (stream, peer) = tokio::io::duplex(4096);
    let (tx, rx) = mpsc::channel(capacity);
    let header = Header::object(IPC, |e| {
        e.string("real-host")?;
        e.u32(42);
        Ok(())
    })
    .unwrap();
    let handler: IpcHandler = Arc::new(move |r| {
        tx.try_send(r)
            .map_err(|_| Error::ResourceLimit("IPC ingress"))
    });
    (
        Connection::start_role(
            stream,
            ListenInstance::default(),
            [9; 16],
            None,
            Some((handler, header)),
        ),
        peer,
        rx,
    )
}
fn command(index: u32, actor: i64, expects: bool) -> Message {
    Message {
        headers: vec![
            Header::actor(actor).unwrap(),
            Header::action("native-command").unwrap(),
            Header::message_id(MessageId {
                guid: [3; 16],
                index,
            })
            .unwrap(),
            Header::boolean(EXPECTS_REPLY, expects).unwrap(),
        ],
        body: vec![42],
    }
}

#[tokio::test]
async fn reports_use_ordinary_capacity_and_cancel_obsolete_queued_frames() {
    use std::sync::atomic::AtomicBool;
    let (conn, mut peer, _rx) = ipc(32);
    // Occupy writer with a frame larger than duplex capacity.
    let first = conn.clone();
    let blocked = tokio::spawn(async move {
        first
            .send_one_way_fenced(
                12,
                "first",
                vec![0; 8192],
                deadline(),
                SendStatus::default(),
            )
            .await
    });
    tokio::task::yield_now().await;
    let valid = Arc::new(AtomicBool::new(true));
    let predicate = valid.clone();
    let next = conn.clone();
    let stale = tokio::spawn(async move {
        next.send_one_way_fenced(
            12,
            "obsolete",
            vec![1],
            deadline(),
            SendStatus::fenced(Arc::new(move || predicate.load(Ordering::Acquire))),
        )
        .await
    });
    tokio::task::yield_now().await;
    valid.store(false, Ordering::Release);
    assert_eq!(conn.handle.shared.controls.capacity(), MAX_QUEUED_CONTROLS);
    assert_eq!(
        read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .action,
        "first"
    );
    blocked.await.unwrap().unwrap();
    assert!(matches!(stale.await.unwrap(), Err(Error::Closed)));
    let current = conn.clone();
    let task = tokio::spawn(async move {
        current
            .send_one_way_fenced(12, "current", vec![2], deadline(), SendStatus::default())
            .await
    });
    assert_eq!(
        read_message(&mut peer)
            .await
            .unwrap()
            .properties()
            .unwrap()
            .action,
        "current"
    );
    task.await.unwrap().unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn report_admission_is_explicitly_bounded() {
    let (conn, _peer, _rx) = ipc(32);
    let permits = conn
        .handle
        .shared
        .admission
        .clone()
        .acquire_many_owned(MAX_PENDING_REQUESTS as u32)
        .await
        .unwrap();
    assert!(matches!(
        conn.send_one_way_fenced(
            12,
            "RAPReportLoad",
            vec![1],
            deadline(),
            SendStatus::default()
        )
        .await,
        Err(Error::ResourceLimit("report admission"))
    ));
    assert_eq!(conn.handle.shared.controls.capacity(), MAX_QUEUED_CONTROLS);
    drop(permits);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn generic_actor_dispatch_one_way_and_correlated_body_only_reply() {
    let (conn, mut peer, mut rx) = ipc(32);
    let requester = conn.clone();
    let pending = tokio::spawn(async move {
        requester
            .request(
                "RegisterFabricRuntimeRequest",
                9,
                vec![],
                vec![1],
                deadline(),
            )
            .await
    });
    let request = read_message(&mut peer).await.unwrap();
    assert!(request.headers.iter().any(|h| h.id == IPC));
    for (id, actor, expects) in [(1, 12, false), (2, 8, true)] {
        write_message(&mut peer, &command(id, actor, expects))
            .await
            .unwrap();
        let incoming = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(incoming.message.properties().unwrap().actor, actor);
        if let Some(context) = incoming.reply {
            let reply = tokio::spawn(context.reply(8, None, vec![2], deadline()));
            let response = read_message(&mut peer).await.unwrap();
            assert_eq!(response.properties().unwrap().relates_to.index, id);
            assert!(!response.headers.iter().any(|h| h.id == IPC));
            reply.await.unwrap().unwrap();
        } else {
            let send = conn.clone();
            let task = tokio::spawn(async move {
                send.send_one_way(12, "ReplicaOpenReply", vec![3], deadline())
                    .await
            });
            let response = read_message(&mut peer).await.unwrap();
            assert_eq!(
                response.properties().unwrap().relates_to,
                MessageId::default()
            );
            assert_eq!(response.properties().unwrap().actor, 12);
            assert!(response.headers.iter().any(|h| h.id == IPC));
            task.await.unwrap().unwrap();
        }
    }
    write_message(
        &mut peer,
        &Message {
            headers: vec![Header::relates_to(request.properties().unwrap().id).unwrap()],
            body: vec![7],
        },
    )
    .await
    .unwrap();
    assert_eq!(pending.await.unwrap().unwrap().body, [7]);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn exact_ingress_capacity_and_overflow_fail_closed() {
    let (conn, mut peer, rx) = ipc(32);
    for id in 1..=32 {
        write_message(&mut peer, &command(id, 12, false))
            .await
            .unwrap();
    }
    timeout(Duration::from_secs(2), async {
        while rx.capacity() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!conn.is_closed());
    write_message(&mut peer, &command(33, 12, false))
        .await
        .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(2), conn.closed())
            .await
            .unwrap(),
        Error::ResourceLimit("IPC ingress")
    ));
}

#[tokio::test]
async fn ipc_requires_host_loopback_and_gateway_cannot_one_way() {
    let handler: IpcHandler = Arc::new(|_| Ok(()));
    assert!(Connection::connect_ipc_clear(
        "192.0.2.1:1".parse().unwrap(),
        deadline(),
        "host",
        handler.clone()
    )
    .await
    .is_err());
    assert!(
        Connection::connect_ipc_clear("127.0.0.1:1".parse().unwrap(), deadline(), "", handler)
            .await
            .is_err()
    );
    let (stream, _peer) = tokio::io::duplex(64);
    let gateway = Connection::start(stream, ListenInstance::default(), [1; 16]);
    assert!(gateway
        .send_one_way(12, "ReplicaOpenReply", vec![], deadline())
        .await
        .is_err());
    gateway.close().await.unwrap();
}
