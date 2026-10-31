use std::{path::PathBuf, process::Command, time::Duration};

use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
};
use tokio::{net::TcpListener, time::timeout};
use uuid::Uuid;

struct Package(PathBuf);

impl Package {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("fube-cli-{}", Uuid::new_v4().simple()));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("ApplicationManifest.xml"), b"package").unwrap();
        Self(path)
    }
}

impl Drop for Package {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn invalid_input_cannot_report_upload_success() {
    let package = Package::new();
    for arguments in [
        vec![
            "--endpoint",
            "127.0.0.1:0",
            "application",
            "upload",
            "--path",
            package.0.to_str().unwrap(),
        ],
        vec![
            "--endpoint",
            "http://127.0.0.1:19080",
            "--unsecured",
            "application",
            "upload",
            "--path",
            package.0.to_str().unwrap(),
        ],
        vec![
            "--endpoint",
            "127.0.0.1:0",
            "--unsecured",
            "application",
            "upload",
            "--path",
            package.0.to_str().unwrap(),
            "--timeout",
            "0",
        ],
        vec![
            "--endpoint",
            "127.0.0.1:0",
            "--unsecured",
            "application",
            "upload",
            "--path",
            package.0.to_str().unwrap(),
            "--timeout",
            "18446744073709551615",
        ],
        vec![
            "--endpoint",
            "127.0.0.1:0",
            "--unsecured",
            "application",
            "upload",
            "--path",
            package.0.to_str().unwrap(),
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_fube"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}

fn activity(message: &Message) -> Header {
    message
        .headers
        .iter()
        .find(|header| header.id == 0x8020)
        .unwrap()
        .clone()
}

fn acknowledgement(message: &Message, actor: i64, sequence: Option<u64>) -> Message {
    let mut headers = vec![
        Header::actor(actor).unwrap(),
        Header::action("ClientOperationSuccess").unwrap(),
        activity(message),
        Header::version(1, 2).unwrap(),
    ];
    if let Some(sequence) = sequence {
        headers.push(
            Header::object(0x803c, |encoder| {
                encoder.u64(sequence);
                encoder.boolean(false);
                encoder.u64(4);
                Ok(())
            })
            .unwrap(),
        );
    }
    Message {
        headers,
        body: Vec::new(),
    }
}

#[tokio::test]
async fn command_uses_native_upload_and_waits_for_package_completion() {
    let package = Package::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(
            read_message(&mut stream)
                .await
                .unwrap()
                .properties()
                .unwrap()
                .actor,
            1
        );
        write_message(
            &mut stream,
            &Message::listen_instance(
                MessageId {
                    guid: [9; 16],
                    index: 1,
                },
                &ListenInstance::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let delete = read_message(&mut stream).await.unwrap();
        assert_eq!(delete.properties().unwrap().action, "ForwardMessage");
        write_message(
            &mut stream,
            &Message {
                headers: vec![
                    Header::actor(22).unwrap(),
                    Header::action("OperationSuccessAction").unwrap(),
                    Header::relates_to(delete.properties().unwrap().id).unwrap(),
                    activity(&delete),
                ],
                body: Vec::new(),
            },
        )
        .await
        .unwrap();
        let mut files = Vec::new();
        for expected in [b"package".as_slice(), b"".as_slice()] {
            let create = read_message(&mut stream).await.unwrap();
            assert_eq!(create.properties().unwrap().action, "FileCreate");
            write_message(&mut stream, &acknowledgement(&create, 25, Some(u64::MAX)))
                .await
                .unwrap();
            let mut contents = Vec::new();
            let mut sequence = 0;
            loop {
                let content = read_message(&mut stream).await.unwrap();
                assert_eq!(content.properties().unwrap().action, "FileContent");
                contents.extend_from_slice(&content.body);
                write_message(&mut stream, &acknowledgement(&content, 25, Some(sequence)))
                    .await
                    .unwrap();
                sequence += 1;
                if contents.len() == expected.len() {
                    break;
                }
            }
            assert_eq!(contents, expected);
            let commit = read_message(&mut stream).await.unwrap();
            assert_eq!(commit.properties().unwrap().action, "FileUploadCommit");
            write_message(&mut stream, &acknowledgement(&commit, 27, None))
                .await
                .unwrap();
            let cleanup = read_message(&mut stream).await.unwrap();
            assert_eq!(cleanup.properties().unwrap().action, "FileUploadCommitAck");
            write_message(&mut stream, &acknowledgement(&cleanup, 27, None))
                .await
                .unwrap();
            files.push(contents);
        }
        assert!(read_message(&mut stream).await.is_err());
        files
    });
    let output = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fube"))
            .args([
                "--endpoint",
                &endpoint.to_string(),
                "--unsecured",
                "application",
                "upload",
                "--path",
                package.0.to_str().unwrap(),
                "--timeout",
                "5",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.stdout.is_empty());
    assert_eq!(
        timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap(),
        [b"package".to_vec(), Vec::new()]
    );
}
