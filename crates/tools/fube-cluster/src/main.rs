use clap::Parser;
use fube_cluster::{Cluster, Command, Config, Result, ServiceProfile};
use serde_json::json;
use std::{io::Write, path::PathBuf, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(version, about = "Run a bounded local FM/RA cluster")]
struct Arguments {
    /// Private cluster profile directory.
    #[arg(long, required_unless_present = "internal_node")]
    root: Option<PathBuf>,

    /// Cluster application executable.
    #[arg(long, required_unless_present = "internal_node")]
    app: Option<PathBuf>,

    /// Run one volatile stateful Primary.
    #[arg(long, conflicts_with_all = ["stateful_peer_idle", "stateful_three"])]
    stateful_singleton: bool,

    /// Run a Primary and a peer-created Idle replica.
    #[arg(long, conflicts_with = "stateful_three")]
    stateful_peer_idle: bool,

    /// Run the opt-in three-node volatile replica profile.
    #[arg(long)]
    stateful_three: bool,

    #[arg(long, hide = true, conflicts_with_all = ["root", "app", "stateful_singleton", "stateful_peer_idle", "stateful_three"])]
    internal_node: Option<PathBuf>,
}

struct Lines {
    input: BufReader<tokio::io::Stdin>,
    bytes: Vec<u8>,
    discarding: bool,
    ended: bool,
}
impl Lines {
    async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        if self.ended {
            return Ok(None);
        }
        loop {
            let chunk = self
                .input
                .fill_buf()
                .await
                .map_err(|error| error.to_string())?;
            if chunk.is_empty() {
                self.ended = true;
                return if self.bytes.is_empty() && !self.discarding {
                    Ok(None)
                } else {
                    Err("truncated command line".into())
                };
            }
            let newline = chunk.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(chunk.len(), |index| index + 1);
            if self.bytes.len() + count > 4096 {
                self.discarding = true;
                self.bytes.clear();
            }
            if !self.discarding {
                self.bytes.extend_from_slice(&chunk[..count]);
            }
            self.input.consume(count);
            if newline.is_some() {
                if self.discarding {
                    self.discarding = false;
                    return Err("command line exceeds 4096 bytes".into());
                }
                return Ok(Some(std::mem::take(&mut self.bytes)));
            }
        }
    }
}
fn output(value: &serde_json::Value) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if bytes.len() > 64 * 1024 {
        return Err("foreground result exceeds 64 KiB".into());
    }
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    lock.write_all(&bytes)
        .and_then(|()| lock.write_all(b"\n"))
        .and_then(|()| lock.flush())
        .map_err(|error| error.to_string())
}
async fn foreground(root: PathBuf, app: PathBuf, profile: ServiceProfile) -> Result<bool> {
    let mut cluster = Cluster::new(Config {
        profile,
        root,
        app,
        executable: std::env::current_exe().map_err(|error| error.to_string())?,
    })?;
    if let Err(error) = cluster.start().await {
        let cleanup = cluster.shutdown().await;
        output(&json!({"event":"startup_error","error":error,"cleanup":cleanup}))?;
        return Ok(false);
    }
    let serving = serve(&mut cluster).await;
    let mut cleanup = cluster.shutdown().await;
    cleanup["op"] = json!(serving.as_ref().ok().and_then(|op| op.as_ref()));
    output(&cleanup)?;
    serving?;
    Ok(cleanup["clean"] == true)
}
async fn serve(cluster: &mut Cluster) -> Result<Option<String>> {
    output(&json!({"event":"ready","status":cluster.status()}))?;
    let mut lines = Lines {
        input: BufReader::with_capacity(4096, tokio::io::stdin()),
        bytes: vec![],
        discarding: false,
        ended: false,
    };
    let mut quit = None;
    loop {
        cluster.drive().await;
        let line = tokio::time::timeout(Duration::from_millis(5), lines.next()).await;
        match line {
            Err(_) => continue,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                output(&json!({"state":"rejected","error":error}))?;
            }
            Ok(Ok(Some(line))) => {
                let command = serde_json::from_slice::<Command>(&line);
                match command {
                    Err(error) => output(&json!({"state":"rejected","error":error.to_string()}))?,
                    Ok(command) => {
                        let op = command.op.clone();
                        let stopping = command.action == "quit";
                        match cluster.execute(command).await {
                            Ok(result) => {
                                output(&result)?;
                                if stopping {
                                    quit = Some(op);
                                    break;
                                }
                            }
                            Err(error) => {
                                output(&json!({"op":op,"state":"rejected","error":error}))?
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(quit)
}
async fn run(arguments: Arguments) -> Result<bool> {
    if let Some(path) = arguments.internal_node {
        fube_cluster::internal_node(&path).await?;
        return Ok(true);
    }
    let profile = if arguments.stateful_three {
        ServiceProfile::StatefulThree
    } else if arguments.stateful_singleton {
        ServiceProfile::StatefulSingleton
    } else if arguments.stateful_peer_idle {
        ServiceProfile::StatefulPeerIdle
    } else {
        ServiceProfile::Stateless
    };
    foreground(
        arguments.root.expect("clap requires --root"),
        arguments.app.expect("clap requires --app"),
        profile,
    )
    .await
}
fn main() {
    let arguments = Arguments::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("foreground runtime");
    let result = runtime.block_on(run(arguments));
    let code = match result {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(error) => {
            let _ = output(&json!({"event":"error","error":error}));
            1
        }
    };
    // Tokio stdin is a bounded blocking OS read. Explicit process exit avoids
    // waiting on another operator input after all owned lifecycle work closed.
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_foreground_profiles_and_original_internal_node() {
        for profile in ["--stateful-singleton", "--stateful-peer-idle"] {
            let args = Arguments::try_parse_from([
                "fube-cluster",
                profile,
                "--app",
                "/app",
                "--root",
                "/root",
            ])
            .unwrap();
            assert_eq!(args.root, Some(PathBuf::from("/root")));
            assert_eq!(args.app, Some(PathBuf::from("/app")));
            assert_eq!(args.stateful_singleton, profile == "--stateful-singleton");
            assert_eq!(args.stateful_peer_idle, profile == "--stateful-peer-idle");
        }
        let args =
            Arguments::try_parse_from(["fube-cluster", "--internal-node", "/node.json"]).unwrap();
        assert_eq!(args.internal_node, Some(PathBuf::from("/node.json")));
        assert!(args.root.is_none() && args.app.is_none());
    }

    #[test]
    fn rejects_incomplete_mixed_and_duplicate_modes() {
        for args in [
            vec![],
            vec!["--root", "/root"],
            vec!["--internal-node"],
            vec!["--internal-node", "/node", "--app", "/app"],
            vec!["--internal-node", "/node", "--stateful-peer-idle"],
            vec![
                "--root",
                "/root",
                "--app",
                "/app",
                "--stateful-singleton",
                "--stateful-peer-idle",
            ],
            vec!["--root", "/root", "--root", "/other", "--app", "/app"],
        ] {
            assert!(
                Arguments::try_parse_from(std::iter::once("fube-cluster").chain(args)).is_err()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn preserves_non_unicode_paths() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let root = OsString::from_vec(b"/root-\xff".to_vec());
        let args = Arguments::try_parse_from([
            OsString::from("fube-cluster"),
            OsString::from("--root"),
            root.clone(),
            OsString::from("--app"),
            OsString::from("/app"),
        ])
        .unwrap();
        assert_eq!(args.root, Some(PathBuf::from(root)));
    }
}
