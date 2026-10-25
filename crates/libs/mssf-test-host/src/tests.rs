use super::*;
use mssf_transport::message::{Header, MessageId};

// These are state/decoder regression tests, not native compatibility proof.
#[test]
fn reject_unowned_identity_unsupported_actions_and_bad_state() {
    let bundle =
        deployment::Deployment::create(&std::env::temp_dir(), &std::env::current_exe().unwrap())
            .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    let mut session =
        ipc::Session::new(socket, listener.local_addr().unwrap().to_string()).unwrap();
    let mut state = Registration {
        handshake: true,
        ..Default::default()
    };
    let mut message = Message {
        headers: vec![
            Header::actor(7).unwrap(),
            Header::action("FinishRegisterApplicationHostRequest").unwrap(),
            Header::message_id(MessageId {
                guid: [1; 16],
                index: 1,
            })
            .unwrap(),
            Header::boolean(mssf_transport::message::EXPECTS_REPLY, true).unwrap(),
        ],
        body: to_vec(&wire::HostId {
            host: "owned".into(),
        })
        .unwrap(),
    };
    assert!(state
        .dispatch(message.clone(), &mut session, "owned", 42, &bundle)
        .unwrap_err()
        .0
        .contains("identity"));
    message.headers.push(Header {
        id: wire::IPC_HEADER,
        value: to_vec(&wire::IpcIdentity {
            host: "owned".into(),
            pid: 43,
        })
        .unwrap(),
    });
    assert!(state
        .dispatch(message.clone(), &mut session, "owned", 42, &bundle)
        .unwrap_err()
        .0
        .contains("unowned"));
    message.headers.last_mut().unwrap().value = to_vec(&wire::IpcIdentity {
        host: "owned".into(),
        pid: 42,
    })
    .unwrap();
    assert!(state
        .dispatch(message.clone(), &mut session, "owned", 42, &bundle)
        .unwrap_err()
        .0
        .contains("finish"));
    message.headers[1] = Header::action("UnsupportedAction").unwrap();
    assert!(state
        .dispatch(message, &mut session, "owned", 42, &bundle)
        .unwrap_err()
        .0
        .contains("unsupported"));
    drop(session);
    std::fs::remove_dir_all(bundle.root).unwrap();
}

#[test]
fn truncated_start_requests_never_succeed() {
    let bytes = to_vec(&wire::HostStart {
        host: "owned".into(),
        kind: 1,
        pid: 42,
        timeout: 1,
        container: false,
        activator: false,
    })
    .unwrap();
    for end in 0..bytes.len() {
        assert!(from_slice::<wire::HostStart>(&bytes[..end]).is_err());
    }
}
