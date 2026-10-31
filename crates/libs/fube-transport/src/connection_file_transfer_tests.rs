use super::*;
use crate::{
    file_transfer::{FileTransferReply, MAX_QUEUED_FILE_REPLIES},
    message::{ACTIVITY, CLIENT_VERSION, UNCORRELATED_REPLY},
    serialization::Encoder,
};
use tokio::io::duplex;

fn deadline() -> Instant {
    deadline_after(Duration::from_secs(2)).unwrap()
}

fn reply() -> Message {
    Message {
        headers: vec![
            Header::object(0x8001, |e| {
                e.i64(25);
                Ok(())
            })
            .unwrap(),
            Header::object(0x8002, |e| e.string("ClientOperationSuccess")).unwrap(),
            Header::object(0x8020, |e| {
                e.object(|e| {
                    e.guid(&[3; 16]);
                    e.u64(0);
                    Ok(())
                })
            })
            .unwrap(),
            Header::object(0x8025, |e| {
                e.i64(1);
                e.i64(2);
                Ok(())
            })
            .unwrap(),
            Header::object(0x803c, |e| {
                e.u64(u64::MAX);
                e.boolean(false);
                e.u64(3 * 1024 * 1024);
                Ok(())
            })
            .unwrap(),
        ],
        body: vec![],
    }
}

fn connection(
    capacity: usize,
) -> (
    Connection,
    tokio::io::DuplexStream,
    mpsc::Receiver<FileTransferReply>,
) {
    let (stream, peer) = duplex(capacity);
    let (tx, rx) = mpsc::channel(MAX_QUEUED_FILE_REPLIES);
    let connection = Connection::start_roles(
        stream,
        ListenInstance::default(),
        [1; 16],
        None,
        None,
        Some(Arc::new(move |reply| {
            tx.try_send(reply)
                .map_err(|_| Error::ResourceLimit("test upload handoff"))
        })),
    );
    (connection, peer, rx)
}

#[tokio::test]
async fn semantic_reply_without_transport_ids_and_management_reply_are_independent() {
    let (connection, mut peer, mut rx) = connection(4096);
    let pending = connection.clone();
    let request = tokio::spawn(async move {
        pending
            .request("ForwardMessage", 5, vec![], vec![], deadline())
            .await
    });
    let management = read_message(&mut peer).await.unwrap();
    write_message(&mut peer, &reply()).await.unwrap();
    let semantic = rx.recv().await.unwrap();
    assert_eq!(semantic.activity, [3; 16]);
    assert_eq!(semantic.actor, 25);
    semantic.result.unwrap();
    write_message(
        &mut peer,
        &Message {
            headers: vec![
                Header::actor(22).unwrap(),
                Header::action("OperationSuccessAction").unwrap(),
                Header::relates_to(management.properties().unwrap().id).unwrap(),
            ],
            body: vec![],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        request.await.unwrap().unwrap().properties().unwrap().actor,
        22
    );
    assert_eq!(connection.unmatched_reply_count(), 0);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn exact_upload_ingress_rejects_invalid_envelopes_and_bodies_before_handoff() {
    for case in 0..15 {
        let (connection, mut peer, mut rx) = connection(4096);
        let mut invalid = reply();
        match case {
            0 => invalid.headers[0] = Header::actor(26).unwrap(),
            1 => invalid.headers[0] = Header::actor(5).unwrap(),
            2 => invalid.headers[1] = Header::action("OtherAction").unwrap(),
            3 => invalid
                .headers
                .push(Header::boolean(UNCORRELATED_REPLY, true).unwrap()),
            4 => invalid
                .headers
                .push(Header::boolean(EXPECTS_REPLY, true).unwrap()),
            5 => invalid.headers.push(
                Header::relates_to(MessageId {
                    guid: [3; 16],
                    index: 7,
                })
                .unwrap(),
            ),
            6 => invalid.headers.retain(|h| h.id != CLIENT_VERSION),
            7 => invalid.headers[3] = Header::version(1, 1).unwrap(),
            8 => invalid.headers.retain(|h| h.id != ACTIVITY),
            9 => invalid.headers[2] = Header::activity([0; 16]).unwrap(),
            10 => invalid.headers.push(invalid.headers[2].clone()),
            11 => invalid.body = vec![0],
            12 => invalid.headers[4].value = vec![0],
            13 => {
                invalid.headers[1] = Header::action("ClientOperationFailure").unwrap();
                let mut e = Encoder::new();
                e.object(|e| {
                    e.i64(0);
                    e.string("invalid zero error")
                })
                .unwrap();
                invalid.body = e.finish();
            }
            14 => {
                invalid.headers[0] = Header::actor(27).unwrap();
                // Commit replies never carry FileSequence.
            }
            _ => unreachable!(),
        }
        write_message(&mut peer, &invalid).await.unwrap();
        assert!(
            matches!(connection.closed().await, Error::Protocol(_)),
            "{case}"
        );
        assert!(
            rx.try_recv().is_err(),
            "invalid message reached handoff: {case}"
        );
    }
}

#[tokio::test]
async fn bounded_semantic_handoff_overflow_retires_the_connection() {
    let (connection, mut peer, _rx) = connection(65536);
    for _ in 0..=MAX_QUEUED_FILE_REPLIES {
        write_message(&mut peer, &reply()).await.unwrap();
    }
    assert!(matches!(
        connection.closed().await,
        Error::ResourceLimit("test upload handoff")
    ));
}

#[tokio::test]
async fn ordinary_gateway_never_dispatches_native_semantic_replies() {
    let (stream, mut peer) = duplex(4096);
    let connection = Connection::start(stream, ListenInstance::default(), [1; 16]);
    write_message(&mut peer, &reply()).await.unwrap();
    timeout(Duration::from_secs(1), async {
        while connection.unmatched_reply_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!connection.is_closed());
    assert!(matches!(
        connection
            .send_file_transfer("FileCreate", vec![], vec![], deadline())
            .await,
        Err(Error::InvalidInput(_))
    ));
    connection.close().await.unwrap();
}

#[tokio::test]
async fn upload_uses_ordinary_admission_and_never_reserved_controls() {
    let (connection, _peer, _rx) = connection(1);
    let shared = &connection.handle.shared;
    let admission = shared
        .admission
        .clone()
        .acquire_many_owned(MAX_PENDING_REQUESTS as u32)
        .await
        .unwrap();
    assert!(matches!(
        connection
            .send_file_transfer("FileContent", vec![], vec![1], deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(shared.controls.capacity(), MAX_QUEUED_CONTROLS);
    drop(admission);
    let mut slots = vec![];
    for _ in 0..MAX_QUEUED_REQUESTS {
        slots.push(connection.handle.commands.try_reserve().unwrap());
    }
    assert!(matches!(
        connection
            .send_file_transfer("FileContent", vec![], vec![1], deadline())
            .await,
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(shared.controls.capacity(), MAX_QUEUED_CONTROLS);
    assert_eq!(shared.admission.available_permits(), MAX_PENDING_REQUESTS);
    drop(slots);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn expired_send_is_not_emitted_and_partial_send_is_retired_on_abort() {
    let (connection, mut peer, _rx) = connection(1);
    assert!(matches!(
        connection
            .send_file_transfer("FileContent", vec![], vec![1], Instant::now())
            .await,
        Err(Error::Timeout)
    ));
    let mut send =
        Box::pin(connection.send_file_transfer("FileContent", vec![], vec![1; 4096], deadline()));
    tokio::select! {
        result = &mut send => panic!("expected blocked frame: {result:?}"),
        result = peer.read_u8() => { result.unwrap(); },
    }
    drop(send);
    connection.abort();
    connection.close().await.unwrap();
    let mut remainder = vec![];
    peer.read_to_end(&mut remainder).await.unwrap();
    assert!(remainder.len() < 4096);
}
