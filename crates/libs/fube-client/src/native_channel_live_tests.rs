//! Direct new-foundation qualification; no FabricClient consumer migration.
use std::{
    net::SocketAddr, os::unix::fs::PermissionsExt, path::PathBuf, process::Command, time::Duration,
};

use fube_transport::{
    channel::{MessageChannel, MessageReceiver, RpcChannel},
    frame::{crc32, validate_fixed, Frame, FIXED_HEADER_SIZE},
    message::{Header, Message, MessageId, HIGH_PRIORITY},
    native_channel::{ChannelLimits, GatewayChannel},
    serialization::serde::{to_vec, CountedValues, ObjectArray, Pointer},
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{timeout, Instant},
};

use crate::{notification_protocol as wire, resolution::Generation};

type LiveResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    allocation: String,
    owner: String,
    container_id: String,
    runtime: String,
    endpoint: SocketAddr,
    source_head: String,
    source_sha256: String,
    source_crc32: u32,
}

fn require(condition: bool, message: &'static str) -> LiveResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn compiled_sources() -> Vec<u8> {
    [
        include_bytes!("../../fube-transport/src/native_channel.rs").as_slice(),
        include_bytes!("native_channel_live_tests.rs").as_slice(),
        include_bytes!("notification_protocol.rs").as_slice(),
    ]
    .concat()
}

fn config() -> LiveResult<Config> {
    let path = PathBuf::from(std::env::var("FUBE_CHANNEL_NATIVE_CONFIG")?);
    let metadata = std::fs::symlink_metadata(&path)?;
    require(
        path.is_absolute()
            && metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o777 == 0o600
            && metadata.len() <= 65536,
        "explicit bounded private native-channel config required",
    )?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    require(
        !config.allocation.is_empty()
            && config
                .allocation
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid task-owned allocation name",
    )?;
    require(
        config.runtime == "11.8.121.4"
            && config.endpoint.to_string() == "127.0.0.1:19000"
            && !config.owner.is_empty(),
        "unsupported native fixture profile",
    )?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let state: Value = serde_json::from_slice(&std::fs::read(
        root.join("build/oneboxes")
            .join(&config.allocation)
            .join("state.json"),
    )?)?;
    require(
        state["name"] == config.allocation
            && state["owner"] == config.owner
            && state["project"] == format!("sf-onebox-{}", config.allocation)
            && state["runtime_version"] == config.runtime
            && state["phase"] == "ready",
        "native original allocation identity/readiness mismatch",
    )?;
    let inspected = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{json .}}",
            &format!("sf-onebox-{}-onebox-1", config.allocation),
        ])
        .output()?;
    require(
        inspected.status.success(),
        "native owned container inspection failed",
    )?;
    let container: Value = serde_json::from_slice(&inspected.stdout)?;
    require(
        container["Id"] == config.container_id
            && container["State"]["Running"] == true
            && container["Config"]["Labels"]["io.github.service-fabric-apps.cluster-owner"]
                == config.owner,
        "native container owner/incarnation changed",
    )?;
    let head = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    require(
        head.status.success()
            && String::from_utf8(head.stdout)?.trim() == config.source_head
            && crc32(&compiled_sources()) == config.source_crc32,
        "native test binary/source identity mismatch",
    )?;
    Ok(config)
}

struct Tap {
    address: SocketAddr,
    outbound: Option<mpsc::Receiver<Message>>,
    inbound: Option<mpsc::Receiver<Message>>,
    stop: watch::Sender<bool>,
    worker: Option<JoinHandle<LiveResult<()>>>,
}

impl Drop for Tap {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

async fn forward<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut reader: R,
    mut writer: W,
    records: mpsc::Sender<Message>,
) -> LiveResult<()> {
    loop {
        let mut fixed = [0; FIXED_HEADER_SIZE];
        if reader.read(&mut fixed[..1]).await? == 0 {
            return Ok(());
        }
        timeout(Duration::from_secs(30), async {
            reader.read_exact(&mut fixed[1..]).await?;
            let (size, _) = validate_fixed(&fixed)?;
            let mut bytes = vec![0; size];
            bytes[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
            reader.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
            let message = Message::from_frame(Frame::decode(&bytes)?)?;
            let props = message.properties()?;
            println!("Native tap frame: actor={} action={} expects={} special={} correlated={} headers={:?}",
                props.actor, props.action, props.expects_reply, props.uncorrelated_reply,
                props.relates_to != MessageId::default(),
                message.headers.iter().map(|header| header.id).collect::<Vec<_>>());
            // Record only after unchanged bytes reach the destination's full local write.
            writer.write_all(&bytes).await?;
            records
                .try_send(message)
                .map_err(|_| "bounded native tap recording overflow")?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
    }
}

impl Tap {
    async fn start(endpoint: SocketAddr) -> LiveResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (out_tx, outbound) = mpsc::channel(32);
        let (in_tx, inbound) = mpsc::channel(32);
        let (stop, mut stopped) = watch::channel(false);
        let worker = tokio::spawn(async move {
            tokio::select! {
                _ = stopped.wait_for(|value| *value) => Ok(()),
                result = async {
                    let (local, _) = listener.accept().await?;
                    let native = TcpStream::connect(endpoint).await?;
                    native.set_nodelay(true)?;
                    let (local_r, local_w) = local.into_split();
                    let (native_r, native_w) = native.into_split();
                    // EOF or failure disposes both halves, never leaves a detached helper.
                    tokio::select! {
                        result = forward(local_r, native_w, out_tx) => result,
                        result = forward(native_r, local_w, in_tx) => result,
                    }
                } => result,
            }
        });
        Ok(Self {
            address,
            outbound: Some(outbound),
            inbound: Some(inbound),
            stop,
            worker: Some(worker),
        })
    }

    async fn close(&mut self) -> LiveResult<()> {
        self.stop.send_replace(true);
        if let Some(worker) = self.worker.as_mut() {
            timeout(Duration::from_secs(35), worker).await???;
        }
        self.worker = None;
        Ok(())
    }
}

async fn observed(records: &mut mpsc::Receiver<Message>, action: &str) -> LiveResult<Message> {
    timeout(Duration::from_secs(60), async {
        for _ in 0..32 {
            let message = records
                .recv()
                .await
                .ok_or("native tap ended before exchange")?;
            if message.properties()?.action == action {
                return Ok(message);
            }
        }
        Err("native tap exchange bound exceeded".into())
    })
    .await?
}

fn domain(action: &str, body: Vec<u8>, end: Instant) -> LiveResult<Message> {
    let ticks = i64::try_from(end.duration_since(Instant::now()).as_nanos() / 100)?;
    Ok(Message {
        headers: vec![
            Header::actor(5)?,
            Header::action(action)?,
            Header::activity(uuid::Uuid::new_v4().to_bytes_le())?,
            Header::version(1, 1)?,
            Header::boolean(HIGH_PRIORITY, true)?,
            Header::timeout(ticks)?,
        ],
        body,
    })
}

async fn exchanges(
    channel: GatewayChannel,
    mut outbound: mpsc::Receiver<Message>,
    mut inbound: mpsc::Receiver<Message>,
    config: Config,
    endpoint: SocketAddr,
) -> LiveResult<Value> {
    let end = Instant::now() + Duration::from_secs(120);
    // The exact target is supplied by the owner, not inferred from native metadata.
    let target = channel.resolve_target(&endpoint.to_string())?;
    let receiver = channel.receiver()?;
    let ping = channel
        .request(&target, domain("PingRequest", vec![], end)?, end)
        .await?;
    let gateway = crate::protocol::decode_ping(&ping)?;
    observed(&mut outbound, "PingRequest").await?;
    println!(
        "Native Channel ordinary gateway RPC passed: {}",
        gateway.node_name
    );

    let mut raw = domain("PingRequest", vec![], end)?;
    raw.headers.push(Header::message_id(MessageId {
        guid: uuid::Uuid::new_v4().to_bytes_le(),
        index: 1,
    })?);
    channel.send_one_way(&target, raw.clone(), end).await?;
    require(
        observed(&mut outbound, "PingRequest").await? == raw,
        "raw native envelope was changed",
    )?;
    println!("Native Channel raw complete-local-write passed; no remote ACK claimed.");

    let id = uuid::Uuid::new_v4().to_string();
    let body = to_vec(&wire::Connect {
        client: &id,
        generation: Generation::default(),
        versions: Pointer(Some(wire::Versions {
            ranges: ObjectArray(Vec::new()),
        })),
        filters: CountedValues(Vec::new()),
    })?;
    let connected = wire::decode_connected(
        channel
            .request(
                &target,
                domain("NotificationClientConnectionRequest", body, end)?,
                end,
            )
            .await
            .map_err(|error| {
                format!(
                    "native notification connection: {error}; first terminal: {:?}",
                    channel.terminal_error()
                )
            })?,
    )?;
    let incoming = receiver.receive(end).await.map_err(|error| {
        format!(
            "native notification receive: {error}; first terminal: {:?}",
            channel.terminal_error()
        )
    })?;
    let original = observed(&mut inbound, "ServiceNotificationRequest").await?;
    require(
        incoming.message == original,
        "native notification envelope was changed",
    )?;
    let mut allocated = 0usize;
    let page = wire::decode_page(&incoming.message.body, &mut |bytes| {
        allocated = allocated
            .checked_add(bytes)
            .ok_or(crate::NotificationError::ResourceLimit(
                "live page overflow",
            ))?;
        if allocated > 8 * 1024 * 1024 {
            return Err(crate::NotificationError::ResourceLimit("live page bound"));
        }
        Ok(())
    })?;
    require(
        page.generation == connected.generation,
        "native synchronization generation mismatch",
    )?;
    let original_id = original.properties()?.id;
    let response = Message {
        headers: vec![
            Header::actor(5)?,
            Header::action("ServiceNotificationReply")?,
            Header::boolean(HIGH_PRIORITY, true)?,
        ],
        body: wire::status(true)?,
    };
    incoming
        .reply
        .ok_or("authorized native reply context absent")?
        .reply(response.clone(), end)
        .await?;
    let sent = observed(&mut outbound, "ServiceNotificationReply").await?;
    let mut expected = response;
    expected.headers.push(Header::relates_to(original_id)?);
    require(
        sent == expected,
        "native consuming reply lost original authorization/correlation",
    )?;
    println!("Native Channel unchanged synchronization notification and original consuming reply passed ({} entries).", page.entries.0.len());
    let final_ping = channel
        .request(&target, domain("PingRequest", vec![], end)?, end)
        .await?;
    crate::protocol::decode_ping(&final_ping)?;
    Ok(json!({
        "runtime":config.runtime, "source_head":config.source_head,
        "source_sha256":config.source_sha256, "source_crc32":config.source_crc32,
        "ordinary_rpc":true, "raw_full_local_write":true,
        "unchanged_native_notification":true, "original_reply_forwarded_to_sf":true,
        "notification_entries":page.entries.0.len(), "gateway":gateway.node_name,
        "native_source":"3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d",
        "mssf_source":"35555883476ac5f78f5f53ef0215b6a32e43b574"
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the fresh owner-guarded tests.onebox.native_channel fixture"]
async fn native_channel_live() -> LiveResult<()> {
    let config = config()?;
    // Independent read-only native observation, not mere Docker liveness.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let nodes: Value = http
        .get("http://127.0.0.1:19080/Nodes?api-version=6.2")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let items = nodes["Items"]
        .as_array()
        .ok_or("native node inventory absent")?;
    require(
        items.len() == 5
            && items
                .iter()
                .all(|node| node["NodeStatus"] == "Up" && node["CodeVersion"] == config.runtime),
        "independent native node runtime/readiness mismatch",
    )?;
    let mut tap = Tap::start(config.endpoint).await?;
    let established = GatewayChannel::connect_unsecured_loopback(
        tap.address,
        true,
        ChannelLimits::default(),
        Instant::now() + Duration::from_secs(60),
    )
    .await;
    let channel = match established {
        Ok(channel) => channel,
        Err(error) => {
            tap.close().await?;
            return Err(error.into());
        }
    };
    let scenario = tokio::spawn(exchanges(
        channel.clone(),
        tap.outbound.take().unwrap(),
        tap.inbound.take().unwrap(),
        config,
        tap.address,
    ))
    .await;
    let closed = channel
        .close(Instant::now() + Duration::from_secs(35))
        .await;
    let tap_closed = tap.close().await;
    let primary = scenario
        .map_err(|error| format!("native scenario task: {error}"))
        .and_then(|result| result.map_err(|error| error.to_string()));
    let result = match primary {
        Ok(result) => {
            closed?;
            tap_closed?;
            result
        }
        Err(error) => {
            return Err(
                format!("{error}; channel cleanup={closed:?}; tap cleanup={tap_closed:?}").into(),
            );
        }
    };
    println!(
        "CHANNEL_NATIVE_QUALIFIED {}",
        serde_json::to_string(&result)?
    );
    println!("Native Channel and bounded tap helpers joined; no workload/filter created.");
    Ok(())
}
