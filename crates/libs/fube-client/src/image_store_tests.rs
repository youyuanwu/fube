use super::*;
use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId, ACTIVITY},
    serialization::Encoder,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

struct Package(PathBuf);
impl Package {
    fn new() -> Self {
        let path =
            PathBuf::from("target/native-upload-tests").join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn file(&self, path: &str, bytes: &[u8]) {
        let path = self.0.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}
impl Drop for Package {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

async fn connected() -> (ImageStoreClient, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        read_message(&mut peer).await.unwrap();
        write_message(
            &mut peer,
            &Message::listen_instance(
                MessageId {
                    guid: [7; 16],
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
    let client = ImageStoreClient::connect_clear(endpoint, Duration::from_secs(3))
        .await
        .unwrap();
    (client, peer.await.unwrap())
}

fn activity(message: &Message) -> [u8; 16] {
    message
        .headers
        .iter()
        .find(|h| h.id == ACTIVITY)
        .unwrap()
        .read(|d| {
            d.object(|d| {
                let guid = d.guid()?;
                assert_eq!(d.u64()?, 0);
                Ok(guid)
            })
        })
        .unwrap()
}
fn failure(code: i64) -> Vec<u8> {
    let mut e = Encoder::new();
    e.object(|e| {
        e.i64(code);
        e.string("native diagnostic")?;
        Ok(())
    })
    .unwrap();
    e.finish()
}
fn ack(
    guid: [u8; 16],
    actor: i64,
    sequence: Option<u64>,
    threshold: u64,
    error: Option<i64>,
) -> Message {
    let mut headers = vec![
        Header::object(0x8001, |e| {
            e.i64(actor);
            Ok(())
        })
        .unwrap(),
        Header::object(0x8002, |e| {
            e.string(if error.is_some() {
                "ClientOperationFailure"
            } else {
                "ClientOperationSuccess"
            })
        })
        .unwrap(),
        Header::object(0x8020, |e| {
            e.object(|e| {
                e.guid(&guid);
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
    ];
    if let Some(sequence) = sequence {
        headers.push(
            Header::object(0x803c, |e| {
                e.u64(sequence);
                e.boolean(false);
                e.u64(threshold);
                Ok(())
            })
            .unwrap(),
        );
    }
    Message {
        headers,
        body: error.map(failure).unwrap_or_default(),
    }
}

async fn delete_request(peer: &mut TcpStream, path: &str) -> Message {
    let request = read_message(peer).await.unwrap();
    let p = request.properties().unwrap();
    assert_eq!(
        (p.actor, p.action.as_str(), p.expects_reply),
        (5, "ForwardMessage", true)
    );
    assert_ne!(p.id, MessageId::default());
    assert_eq!(request.protocol_version().unwrap(), Some((1, 2)));
    let forward = Header::object(0x8028, |e| {
        e.i64(17);
        e.string("ServiceRouteRequest")
    })
    .unwrap();
    let route = Header::object(0x802c, |e| {
        e.i64(22);
        e.string("DeleteAction")?;
        e.next_scope()?;
        e.object(|e| {
            e.object(|e| {
                e.object(|e| {
                    e.string("__FabricSystem")?;
                    e.u32(u32::MAX);
                    Ok(())
                })?;
                e.string("FileStoreService")
            })?;
            e.string("FileStoreServiceType")
        })
    })
    .unwrap();
    let partition = Header::object(0x8034, |e| {
        e.guid(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x30, 0]);
        Ok(())
    })
    .unwrap();
    for expected in [forward, route, partition] {
        assert_eq!(
            request
                .headers
                .iter()
                .find(|h| h.id == expected.id)
                .unwrap(),
            &expected
        );
    }
    let mut body = Encoder::new();
    body.object(|e| e.string(path)).unwrap();
    assert_eq!(request.body, body.finish());
    request
}
async fn delete_success(peer: &mut TcpStream, path: &str, missing: bool) {
    let request = delete_request(peer, path).await;
    write_message(
        peer,
        &Message {
            headers: vec![
                Header::actor(if missing { 5 } else { 22 }).unwrap(),
                Header::action(if missing {
                    "ClientOperationFailure"
                } else {
                    "OperationSuccessAction"
                })
                .unwrap(),
                Header::relates_to(request.properties().unwrap().id).unwrap(),
                // Missing failures intentionally have a new Activity, not request equality.
                Header::activity(if missing { [8; 16] } else { activity(&request) }).unwrap(),
            ],
            body: if missing {
                failure(wire::FILE_NOT_FOUND)
            } else {
                vec![]
            },
        },
    )
    .await
    .unwrap();
}

fn created(message: &Message) -> (String, u64) {
    assert_eq!(message.properties().unwrap().actor, 26);
    assert_eq!(message.properties().unwrap().action, "FileCreate");
    assert!(!message.properties().unwrap().expects_reply);
    assert!(message.body.is_empty());
    message
        .headers
        .iter()
        .find(|h| h.id == 0x804f)
        .unwrap()
        .read(|d| {
            assert_eq!(d.string()?, "fabric:/System/ImageStoreService");
            let path = d.string()?;
            assert!(d.boolean()?);
            Ok((path, d.u64()?))
        })
        .unwrap()
}

async fn receive_file(
    peer: &mut TcpStream,
    create: Message,
    threshold: u64,
    pending_commit: bool,
) -> (String, Vec<u8>, [u8; 16]) {
    let (path, size) = created(&create);
    let guid = activity(&create);
    write_message(peer, &ack(guid, 25, Some(u64::MAX), threshold, None))
        .await
        .unwrap();
    let mut bytes = vec![];
    let mut number = 0;
    loop {
        let content = read_message(peer).await.unwrap();
        assert_eq!(content.properties().unwrap().action, "FileContent");
        assert_eq!(content.protocol_version().unwrap(), Some((1, 2)));
        assert_eq!(activity(&content), guid);
        let (seq, last, count) = content
            .headers
            .iter()
            .find(|h| h.id == 0x803c)
            .unwrap()
            .read(|d| Ok((d.u64()?, d.boolean()?, d.u64()?)))
            .unwrap();
        assert_eq!(
            (seq, last, count),
            (number, false, content.body.len() as u64)
        );
        let negotiated = if threshold == 0 {
            wire::DEFAULT_CHUNK_SIZE
        } else {
            threshold
        };
        assert_eq!(count, (size - bytes.len() as u64).min(negotiated));
        bytes.extend(content.body);
        write_message(peer, &ack(guid, 25, Some(number), threshold, None))
            .await
            .unwrap();
        if bytes.len() as u64 == size {
            break;
        }
        number += 1;
    }
    let commit = read_message(peer).await.unwrap();
    assert_eq!(commit.properties().unwrap().action, "FileUploadCommit");
    assert!(commit.body.is_empty());
    assert_eq!(activity(&commit), guid);
    if pending_commit {
        write_message(
            peer,
            &ack(guid, 27, None, threshold, Some(wire::OPERATIONS_PENDING)),
        )
        .await
        .unwrap();
        let retried = read_message(peer).await.unwrap();
        assert_eq!(retried.properties().unwrap().action, "FileUploadCommit");
        assert_eq!(activity(&retried), guid);
    }
    write_message(peer, &ack(guid, 27, None, threshold, None))
        .await
        .unwrap();
    let committed = read_message(peer).await.unwrap();
    assert_eq!(
        committed.properties().unwrap().action,
        "FileUploadCommitAck"
    );
    assert_eq!(activity(&committed), guid);
    assert!(committed.body.is_empty());
    (path, bytes, guid)
}

#[tokio::test]
async fn negotiated_multichunk_empty_markers_postorder_replacement_and_reuse() {
    let package = Package::new();
    package.file("data λ/space name", b"0123456789");
    package.file("zero", b"");
    package.file("_.dir", b"must be regenerated");
    std::fs::create_dir_all(package.0.join("empty directory")).unwrap();
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        let mut copies = vec![];
        for missing in [true, false] {
            delete_success(&mut peer, "pkg α", missing).await;
            let mut files = vec![];
            let mut previous = None;
            loop {
                let create = read_message(&mut peer).await.unwrap();
                if let Some(old) = previous {
                    // File A's late cleanup failure and duplicate commit arrive while B is active.
                    write_message(&mut peer, &ack(old, 27, None, 4, Some(-2147017488)))
                        .await
                        .unwrap();
                    write_message(&mut peer, &ack(old, 27, None, 4, None))
                        .await
                        .unwrap();
                }
                let (path, bytes, guid) = receive_file(&mut peer, create, 4, true).await;
                previous = Some(guid);
                let last = path == "pkg α/_.dir";
                files.push((path, bytes));
                if last {
                    break;
                }
            }
            copies.push(files);
        }
        (copies, peer)
    });
    for _ in 0..2 {
        client
            .copy_application_package(&package.0, "pkg α", None)
            .await
            .unwrap();
        assert!(!client.is_closed());
    }
    let (copies, _peer) = server.await.unwrap();
    for files in copies {
        assert_eq!(files.len(), 5);
        let find = |name: &str| files.iter().position(|(path, _)| path == name).unwrap();
        assert!(find("pkg α/data λ/space name") < find("pkg α/data λ/_.dir"));
        assert_eq!(files[find("pkg α/data λ/space name")].1, b"0123456789");
        assert!(files[find("pkg α/zero")].1.is_empty());
        assert!(files[find("pkg α/empty directory/_.dir")].1.is_empty());
        assert_eq!(files.last().unwrap(), &("pkg α/_.dir".into(), vec![]));
    }
    client.close().await.unwrap();
}

#[tokio::test]
async fn zero_negotiation_uses_native_threshold_and_sends_one_empty_chunk() {
    let package = Package::new();
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        delete_success(&mut peer, "pkg", false).await;
        let create = read_message(&mut peer).await.unwrap();
        let result = receive_file(&mut peer, create, 0, false).await;
        (result, peer)
    });
    client
        .copy_application_package(&package.0, "pkg", None)
        .await
        .unwrap();
    let ((path, bytes, _), _peer) = server.await.unwrap();
    assert_eq!(path, "pkg/_.dir");
    assert!(bytes.is_empty());
    client.close().await.unwrap();
}

#[tokio::test]
async fn routed_delete_rejection_or_wrong_reply_prevents_file_creation() {
    for (actor, action, body, expected_code) in [
        (
            5,
            "ClientOperationFailure",
            failure(-2147017488),
            Some(-2147017488),
        ),
        (5, "ClientOperationFailure", failure(2), Some(2)),
        (5, "ClientOperationFailure", vec![1], None),
        (5, "OperationSuccessAction", vec![], None),
        (22, "OperationSuccessAction", vec![1], None),
    ] {
        let package = Package::new();
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            let request = delete_request(&mut peer, "pkg").await;
            write_message(
                &mut peer,
                &Message {
                    headers: vec![
                        Header::actor(actor).unwrap(),
                        Header::action(action).unwrap(),
                        Header::relates_to(request.properties().unwrap().id).unwrap(),
                        Header::activity(activity(&request)).unwrap(),
                    ],
                    body,
                },
            )
            .await
            .unwrap();
            assert!(
                read_message(&mut peer).await.is_err(),
                "Delete rejection must not publish anything"
            );
        });
        let error = client
            .copy_application_package(&package.0, "pkg", None)
            .await
            .unwrap_err();
        if let Some(expected) = expected_code {
            assert!(matches!(error, Error::Remote { code, .. } if code == expected));
        } else {
            assert!(matches!(error, Error::Protocol(_)));
        }
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn invalid_source_or_destination_never_deletes_and_keeps_connection() {
    let package = Package::new();
    let (mut client, mut peer) = connected().await;
    let marker_fits = |destination: &str| {
        let headers = vec![
            Header::object(0x8001, |e| {
                e.i64(26);
                Ok(())
            })
            .unwrap(),
            Header::object(0x8002, |e| e.string("FileCreate")).unwrap(),
            Header::object(0x8003, |e| {
                e.guid(&[255; 16]);
                e.u32(u32::MAX);
                Ok(())
            })
            .unwrap(),
            Header::object(0x8020, |e| {
                e.object(|e| {
                    e.guid(&[255; 16]);
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
            Header::object(0x804f, |e| {
                e.string("fabric:/System/ImageStoreService")?;
                e.string(&format!("{destination}/_.dir"))?;
                e.boolean(true);
                e.u64(0);
                Ok(())
            })
            .unwrap(),
            Header::object(0x8023, |e| {
                e.i64(i64::MAX);
                Ok(())
            })
            .unwrap(),
        ];
        Message {
            headers,
            body: vec![],
        }
        .to_frame()
        .and_then(|frame| frame.encode())
        .is_ok()
    };
    let (mut lower, mut upper) = (32_600usize, 32_700usize);
    while lower < upper {
        let midpoint = lower + (upper - lower).div_ceil(2);
        if marker_fits(&"a".repeat(midpoint)) {
            lower = midpoint;
        } else {
            upper = midpoint - 1;
        }
    }
    let boundary = "a".repeat(lower);
    let over_boundary = "a".repeat(lower + 1);
    let demonstrated = "a".repeat(32_700);
    assert!(marker_fits(&boundary));
    assert!(!marker_fits(&over_boundary));
    assert!(!marker_fits(&demonstrated));
    for invalid in [
        "",
        "/",
        ".",
        "./",
        "../pkg",
        "pkg/../other",
        "\\root",
        "C:\\pkg",
        "Store/x",
        "./windowsfabricstore",
        " Store/child ",
        "pkg\0x",
        "./ Store/.",
        "./WindowsFabricStore /.",
        "./ Store/child",
        "Store /.",
        "./ WindowsFabricStore",
        r".\ Store\.",
        r".\WindowsFabricStore \.",
        r".\ Store\child",
        r"Store \.",
        r".\ WindowsFabricStore",
        "./ ./ Store/.",
        "./ ./WindowsFabricStore /.",
        "./ ./ /.",
        "./ ./ .. /.",
        "./ ./ C:\\.",
        "./ . /.",
    ] {
        assert!(matches!(
            client
                .copy_application_package(&package.0, invalid, None)
                .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(!client.is_closed());
    }
    for invalid in [&demonstrated, &over_boundary] {
        assert!(matches!(
            client
                .copy_application_package(&package.0, invalid, None)
                .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(!client.is_closed());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), read_message(&mut peer))
            .await
            .is_err()
    );
    let accepted = boundary.clone();
    let server = tokio::spawn(async move {
        for expected in ["包 space/child space", accepted.as_str()] {
            delete_success(&mut peer, expected, false).await;
            let create = read_message(&mut peer).await.unwrap();
            let (path, bytes, _) = receive_file(&mut peer, create, 4, false).await;
            assert_eq!(path, format!("{expected}/_.dir"));
            assert!(bytes.is_empty());
        }
        peer
    });
    for accepted in [r"./ ./ 包 space\child space\.", boundary.as_str()] {
        client
            .copy_application_package(&package.0, accepted, None)
            .await
            .unwrap();
        assert!(!client.is_closed());
    }
    peer = server.await.unwrap();
    let file = package.0.join("file");
    std::fs::write(&file, b"").unwrap();
    assert!(matches!(
        client.copy_application_package(&file, "pkg", None).await,
        Err(Error::InvalidInput(_))
    ));
    let link = package.0.join("root-link");
    std::os::unix::fs::symlink(&package.0, &link).unwrap();
    assert!(matches!(
        client.copy_application_package(&link, "pkg", None).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), read_message(&mut peer))
            .await
            .is_err()
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn unsupported_local_symlinks_and_special_entries_fail_explicitly() {
    for special in [false, true] {
        let package = Package::new();
        let path = package.0.join("unsupported");
        if special {
            std::os::unix::net::UnixListener::bind(&path).unwrap();
        } else {
            std::os::unix::fs::symlink("missing", &path).unwrap();
        }
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            delete_success(&mut peer, "pkg", false).await;
            assert!(read_message(&mut peer).await.is_err());
        });
        assert!(matches!(
            client
                .copy_application_package(&package.0, "pkg", None)
                .await,
            Err(Error::InvalidInput(_))
        ));
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn oversized_negotiation_is_rejected_before_content_not_shrunk() {
    let package = Package::new();
    package.file("file", b"content");
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        delete_success(&mut peer, "pkg", false).await;
        let create = read_message(&mut peer).await.unwrap();
        write_message(
            &mut peer,
            &ack(activity(&create), 25, Some(u64::MAX), 4 * 1024 * 1024, None),
        )
        .await
        .unwrap();
        let cleanup = read_message(&mut peer).await.unwrap();
        assert_eq!(
            cleanup.properties().unwrap().action,
            "FileUploadDeleteSession"
        );
        assert!(read_message(&mut peer).await.is_err());
    });
    assert!(matches!(
        client
            .copy_application_package(&package.0, "pkg", None)
            .await,
        Err(Error::Protocol(_))
    ));
    assert!(client.is_closed());
    server.await.unwrap();
}

#[tokio::test]
async fn wrong_envelopes_future_sequences_and_remote_rejection_fail() {
    for case in 0..10 {
        let package = Package::new();
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            delete_success(&mut peer, "pkg", false).await;
            let create = read_message(&mut peer).await.unwrap();
            let guid = activity(&create);
            let mut response = ack(guid, 25, Some(u64::MAX), 4, None);
            match case {
                0 => response.headers[0] = Header::actor(28).unwrap(),
                1 => response.headers[1] = Header::action("OtherSuccess").unwrap(),
                2 => response.headers.retain(|h| h.id != 0x8025),
                3 => response.headers[3] = Header::version(1, 1).unwrap(),
                4 => response
                    .headers
                    .push(Header::relates_to(MessageId { guid, index: 1 }).unwrap()),
                5 => response.body = vec![0],
                6 => response = ack(guid, 25, Some(0), 4, None),
                7 => response = ack(guid, 27, None, 4, None),
                8 => response = ack(guid, 25, Some(u64::MAX), 4, Some(-2147017534)),
                9 => response = ack([2; 16], 25, Some(u64::MAX), 4, Some(0)),
                _ => unreachable!(),
            }
            write_message(&mut peer, &response).await.unwrap();
            // Protocol decoded on the driver may close before cleanup; stage/
            // remote errors may flush scoped cleanup first.
            if let Ok(next) = read_message(&mut peer).await {
                assert_eq!(next.properties().unwrap().action, "FileUploadDeleteSession");
                assert!(read_message(&mut peer).await.is_err());
            }
        });
        let error = client
            .copy_application_package(&package.0, "pkg", None)
            .await
            .unwrap_err();
        if case == 8 {
            assert!(matches!(
                error,
                Error::Remote {
                    code: -2147017534,
                    ..
                }
            ));
        } else {
            assert!(matches!(error, Error::Protocol(_)), "{case}: {error}");
        }
        assert!(client.is_closed());
        assert!(client
            .copy_application_package(&package.0, "pkg", None)
            .await
            .is_err());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn deadline_and_future_drop_after_create_content_and_commit_retire_client() {
    for stage in ["FileCreate", "FileContent", "FileUploadCommit"] {
        for drop_future in [true, false] {
            let package = Package::new();
            package.file("file", b"abc");
            let (mut client, mut peer) = connected().await;
            let (reached, milestone) = oneshot::channel();
            let server = tokio::spawn(async move {
                delete_success(&mut peer, "pkg", false).await;
                let create = read_message(&mut peer).await.unwrap();
                let guid = activity(&create);
                if stage != "FileCreate" {
                    write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
                        .await
                        .unwrap();
                    let content = read_message(&mut peer).await.unwrap();
                    assert_eq!(content.properties().unwrap().action, "FileContent");
                    if stage == "FileUploadCommit" {
                        write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
                            .await
                            .unwrap();
                        assert_eq!(
                            read_message(&mut peer)
                                .await
                                .unwrap()
                                .properties()
                                .unwrap()
                                .action,
                            "FileUploadCommit"
                        );
                    }
                }
                reached.send(()).unwrap();
                assert!(
                    read_message(&mut peer).await.is_err(),
                    "expired/dropped upload must retire without waiting for cleanup"
                );
            });
            let mut upload = Box::pin(client.copy_application_package(
                &package.0,
                "pkg",
                Some(Duration::from_millis(200)),
            ));
            tokio::select! {
                result = &mut upload => panic!("upload completed before pending stage: {result:?}"),
                result = milestone => result.unwrap(),
            }
            if !drop_future {
                assert!(matches!(upload.as_mut().await, Err(Error::Timeout)));
            }
            drop(upload);
            assert!(client.is_closed());
            assert!(client
                .copy_application_package(&package.0, "pkg", None)
                .await
                .is_err());
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn whole_deadline_includes_root_delete_and_drop_during_delete() {
    for drop_future in [false, true] {
        let package = Package::new();
        let (mut client, mut peer) = connected().await;
        let (tx, rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            delete_request(&mut peer, "pkg").await;
            tx.send(()).unwrap();
            assert!(read_message(&mut peer).await.is_err());
        });
        let mut copy = Box::pin(client.copy_application_package(
            &package.0,
            "pkg",
            Some(Duration::from_millis(100)),
        ));
        tokio::select! {
            result = &mut copy => panic!("premature Delete completion {result:?}"),
            result = rx => result.unwrap(),
        }
        if !drop_future {
            assert!(matches!(copy.as_mut().await, Err(Error::Timeout)));
        }
        drop(copy);
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn bounded_stale_and_duplicate_replies_never_advance_future_sequence() {
    for (count, same_activity) in [(64, false), (65, false), (64, true), (65, true)] {
        let package = Package::new();
        package.file("file", b"abcdef");
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            delete_success(&mut peer, "pkg", false).await;
            let create = read_message(&mut peer).await.unwrap();
            let guid = activity(&create);
            write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
                .await
                .unwrap();
            let content = read_message(&mut peer).await.unwrap();
            assert_eq!(content.body, b"abcd");
            write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
                .await
                .unwrap();
            let content = read_message(&mut peer).await.unwrap();
            assert_eq!(content.body, b"ef");
            for _ in 0..count {
                write_message(
                    &mut peer,
                    &ack(
                        if same_activity { guid } else { [2; 16] },
                        25,
                        Some(0),
                        4,
                        None,
                    ),
                )
                .await
                .unwrap();
                // Force a bounded, consumed stream rather than testing ingress overflow.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if count == 64 {
                write_message(&mut peer, &ack(guid, 25, Some(1), 4, None))
                    .await
                    .unwrap();
                assert_eq!(
                    read_message(&mut peer)
                        .await
                        .unwrap()
                        .properties()
                        .unwrap()
                        .action,
                    "FileUploadCommit"
                );
                write_message(&mut peer, &ack(guid, 27, None, 4, None))
                    .await
                    .unwrap();
                assert_eq!(
                    read_message(&mut peer)
                        .await
                        .unwrap()
                        .properties()
                        .unwrap()
                        .action,
                    "FileUploadCommitAck"
                );
                let marker = read_message(&mut peer).await.unwrap();
                receive_file(&mut peer, marker, 4, false).await;
            } else {
                let cleanup = read_message(&mut peer).await.unwrap();
                assert_eq!(
                    cleanup.properties().unwrap().action,
                    "FileUploadDeleteSession"
                );
                assert!(read_message(&mut peer).await.is_err());
            }
            peer
        });
        let result = client
            .copy_application_package(&package.0, "pkg", None)
            .await;
        if count == 64 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::Protocol(_))));
        }
        let _peer = server.await.unwrap();
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn chunk_ack_is_not_commit_success_and_primary_error_survives_cleanup_failure() {
    let package = Package::new();
    package.file("file", b"abc");
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        delete_success(&mut peer, "pkg", false).await;
        let create = read_message(&mut peer).await.unwrap();
        let guid = activity(&create);
        write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
            .await
            .unwrap();
        read_message(&mut peer).await.unwrap();
        write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .action,
            "FileUploadCommit"
        );
        write_message(&mut peer, &ack(guid, 27, None, 4, Some(-2147017534)))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .action,
            "FileUploadDeleteSession"
        );
        let _ = write_message(&mut peer, &ack(guid, 27, None, 4, Some(-2147017488))).await;
        assert!(read_message(&mut peer).await.is_err());
    });
    assert!(matches!(
        client
            .copy_application_package(&package.0, "pkg", None)
            .await,
        Err(Error::Remote {
            code: -2147017534,
            ..
        })
    ));
    assert!(client.is_closed());
    server.await.unwrap();
}

#[tokio::test]
async fn future_chunk_commit_stage_and_changed_threshold_are_rejected() {
    for case in 0..5 {
        let package = Package::new();
        package.file("file", b"abc");
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            delete_success(&mut peer, "pkg", false).await;
            let create = read_message(&mut peer).await.unwrap();
            let guid = activity(&create);
            write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
                .await
                .unwrap();
            read_message(&mut peer).await.unwrap();
            let response = match case {
                0 => ack(guid, 25, Some(1), 4, None),
                1 => ack(guid, 27, None, 4, None),
                2 => ack(guid, 25, Some(0), 5, None),
                3 => ack(guid, 25, Some(0), 4, Some(-2147017534)),
                4 => {
                    write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
                        .await
                        .unwrap();
                    assert_eq!(
                        read_message(&mut peer)
                            .await
                            .unwrap()
                            .properties()
                            .unwrap()
                            .action,
                        "FileUploadCommit"
                    );
                    ack(guid, 25, Some(1), 4, None)
                }
                _ => unreachable!(),
            };
            write_message(&mut peer, &response).await.unwrap();
            assert_eq!(
                read_message(&mut peer)
                    .await
                    .unwrap()
                    .properties()
                    .unwrap()
                    .action,
                "FileUploadDeleteSession"
            );
            assert!(read_message(&mut peer).await.is_err());
        });
        let result = client
            .copy_application_package(&package.0, "pkg", None)
            .await;
        if case == 3 {
            assert!(matches!(
                result,
                Err(Error::Remote {
                    code: -2147017534,
                    ..
                })
            ));
        } else {
            assert!(matches!(result, Err(Error::Protocol(_))));
        }
        assert!(client.is_closed());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn disconnect_during_each_transfer_stage_never_succeeds_and_reuse_is_rejected() {
    for stage in ["FileCreate", "FileContent", "FileUploadCommit"] {
        let package = Package::new();
        package.file("file", b"abc");
        let (mut client, mut peer) = connected().await;
        let server = tokio::spawn(async move {
            delete_success(&mut peer, "pkg", false).await;
            let create = read_message(&mut peer).await.unwrap();
            let guid = activity(&create);
            if stage != "FileCreate" {
                write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
                    .await
                    .unwrap();
                read_message(&mut peer).await.unwrap();
                if stage == "FileUploadCommit" {
                    write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
                        .await
                        .unwrap();
                    assert_eq!(
                        read_message(&mut peer)
                            .await
                            .unwrap()
                            .properties()
                            .unwrap()
                            .action,
                        "FileUploadCommit"
                    );
                }
            }
        });
        assert!(matches!(
            client
                .copy_application_package(&package.0, "pkg", None)
                .await,
            Err(Error::Io(_))
        ));
        assert!(client.is_closed());
        assert!(client
            .copy_application_package(&package.0, "pkg", None)
            .await
            .is_err());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn full_frame_sized_negotiation_preserves_offsets_with_one_chunk_outstanding() {
    let package = Package::new();
    let threshold = wire::chunk_capacity().unwrap();
    let bytes = vec![42; threshold as usize + 7];
    package.file("large", &bytes);
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        delete_success(&mut peer, "pkg", false).await;
        let create = read_message(&mut peer).await.unwrap();
        let guid = activity(&create);
        write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), threshold, None))
            .await
            .unwrap();
        let first = read_message(&mut peer).await.unwrap();
        assert_eq!(first.body.len(), threshold as usize);
        assert!(first.to_frame().unwrap().encode().unwrap().len() <= 4 * 1024 * 1024);
        // No second chunk can be emitted until this first ACK.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), read_message(&mut peer))
                .await
                .is_err()
        );
        write_message(&mut peer, &ack(guid, 25, Some(0), threshold, None))
            .await
            .unwrap();
        let second = read_message(&mut peer).await.unwrap();
        assert_eq!(second.body, [42; 7]);
        assert_eq!(
            second
                .headers
                .iter()
                .find(|h| h.id == 0x803c)
                .unwrap()
                .read(|d| d.u64())
                .unwrap(),
            1
        );
        write_message(&mut peer, &ack(guid, 25, Some(1), threshold, None))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .action,
            "FileUploadCommit"
        );
        write_message(&mut peer, &ack(guid, 27, None, threshold, None))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .action,
            "FileUploadCommitAck"
        );
        let marker = read_message(&mut peer).await.unwrap();
        receive_file(&mut peer, marker, threshold, false).await;
        peer
    });
    client
        .copy_application_package(&package.0, "pkg", None)
        .await
        .unwrap();
    let _peer = server.await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn successful_earlier_stages_do_not_reset_whole_operation_deadline() {
    let package = Package::new();
    let (mut client, mut peer) = connected().await;
    let server = tokio::spawn(async move {
        let request = delete_request(&mut peer, "pkg").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        write_message(
            &mut peer,
            &Message {
                headers: vec![
                    Header::actor(22).unwrap(),
                    Header::action("OperationSuccessAction").unwrap(),
                    Header::relates_to(request.properties().unwrap().id).unwrap(),
                    Header::activity(activity(&request)).unwrap(),
                ],
                body: vec![],
            },
        )
        .await
        .unwrap();
        let create = read_message(&mut peer).await.unwrap();
        let guid = activity(&create);
        tokio::time::sleep(Duration::from_millis(50)).await;
        write_message(&mut peer, &ack(guid, 25, Some(u64::MAX), 4, None))
            .await
            .unwrap();
        read_message(&mut peer).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        write_message(&mut peer, &ack(guid, 25, Some(0), 4, None))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut peer)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .action,
            "FileUploadCommit"
        );
        assert!(read_message(&mut peer).await.is_err());
    });
    let start = Instant::now();
    assert!(matches!(
        client
            .copy_application_package(&package.0, "pkg", Some(Duration::from_millis(200)))
            .await,
        Err(Error::Timeout)
    ));
    assert!(start.elapsed() < Duration::from_millis(300));
    assert!(client.is_closed());
    server.await.unwrap();
}
