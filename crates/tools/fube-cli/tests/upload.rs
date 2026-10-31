use std::{
    io::Read,
    net::SocketAddr,
    os::fd::AsRawFd,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use fube_transport::{
    connection::{read_message, write_message},
    message::{Header, ListenInstance, Message, MessageId},
    serialization::Encoder,
};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
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

fn fube_executable() -> PathBuf {
    // Archive runners remap the runtime path, not a compile-time env! value.
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_fube")
            .expect("Cargo or nextest must provide fube's executable path at runtime"),
    )
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
        let output = Command::new(fube_executable())
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

async fn receive_upload(
    listener: TcpListener,
    mut reject_root_commit: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
) -> Vec<Vec<u8>> {
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
        if expected.is_empty() {
            if let Some((ready, release)) = reject_root_commit.take() {
                ready.send(()).unwrap();
                release.await.unwrap();
                let mut failure = acknowledgement(&commit, 27, None);
                failure.headers[1] = Header::action("ClientOperationFailure").unwrap();
                let mut encoder = Encoder::new();
                encoder
                    .object(|encoder| {
                        encoder.i64(-2147017728);
                        encoder.string("commit rejected")
                    })
                    .unwrap();
                failure.body = encoder.finish();
                write_message(&mut stream, &failure).await.unwrap();
                let cleanup = read_message(&mut stream).await.unwrap();
                assert_eq!(
                    cleanup.properties().unwrap().action,
                    "FileUploadDeleteSession"
                );
                assert!(read_message(&mut stream).await.is_err());
                files.push(contents);
                return files;
            }
        }
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
}

fn upload_command(package: &Package, endpoint: SocketAddr) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(fube_executable());
    command.args([
        "--endpoint",
        &endpoint.to_string(),
        "--unsecured",
        "application",
        "upload",
        "--path",
        package.0.to_str().unwrap(),
        "--timeout",
        "5",
    ]);
    command
}

#[tokio::test]
async fn command_uses_native_upload_and_waits_for_package_completion() {
    let package = Package::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let peer = tokio::spawn(receive_upload(listener, None));
    let output = timeout(
        Duration::from_secs(10),
        upload_command(&package, endpoint).output(),
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

#[tokio::test]
async fn rejected_final_commit_never_prints_success() {
    let package = Package::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (ready, pending) = oneshot::channel();
    let (release, reply) = oneshot::channel();
    let peer = tokio::spawn(receive_upload(listener, Some((ready, reply))));
    let mut child = upload_command(&package, endpoint)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = std::fs::File::from(child.stdout.take().unwrap().into_owned_fd().unwrap());
    let fd = stdout.as_raw_fd();
    // The owned pipe stays alive while its flags are changed for a timer-free observation.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(child.try_wait().unwrap().is_none());
    let error = stdout.read(&mut [0]).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    release.send(()).unwrap();
    let output = timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("commit rejected"));
    let mut remaining = Vec::new();
    stdout.read_to_end(&mut remaining).unwrap();
    assert!(remaining.is_empty());
    timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
}
