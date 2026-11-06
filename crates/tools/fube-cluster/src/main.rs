use fube_cluster::{Cluster, Command, Config, Result, ServiceProfile};
use serde_json::json;
use std::{io::Write, path::PathBuf, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};

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
async fn run() -> Result<bool> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() == 2 && arguments[0] == "--internal-node" {
        fube_cluster::internal_node(&PathBuf::from(&arguments[1])).await?;
        return Ok(true);
    }
    let profile = if arguments.len() == 5 && arguments[4] == "--stateful-singleton" {
        ServiceProfile::StatefulSingleton
    } else if arguments.len() == 4 {
        ServiceProfile::Stateless
    } else {
        return Err("expected optional --stateful-singleton after --root/--app".into());
    };
    if arguments[0] != "--root" || arguments[2] != "--app" {
        return Err(
            "usage: fube-cluster --root /private/profile --app /absolute/cluster-stateless-app"
                .into(),
        );
    }
    foreground(
        PathBuf::from(&arguments[1]),
        PathBuf::from(&arguments[3]),
        profile,
    )
    .await
}
fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("foreground runtime");
    let result = runtime.block_on(run());
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
