use crate::profile::{err, Launch, Result};
use fube_local_host::process::OwnedApp;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub(crate) const NODE_CONTROL_LIMIT: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NodeControl {
    // Empty struct variants enforce deny_unknown_fields even for fieldless commands.
    Admit {},
    Stop {},
    DialPeer { slot: usize, address: SocketAddr },
    ReplaceHost { replica: i64, instance: i64 },
    PeerDown { slot: usize, instance: u64 },
    PeerUpdate { slot: usize, instance: u64 },
    CreateIdle {},
    BuildIdle {},
    RemoveIdle {},
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeRequest {
    session: String,
    capability: String,
    operation: NodeControl,
}

impl NodeRequest {
    fn new(launch: &Launch, operation: NodeControl) -> Self {
        Self {
            session: launch.session.clone(),
            capability: launch.capability.clone(),
            operation,
        }
    }

    fn authorize(self, launch: &Launch) -> Result<NodeControl> {
        if self.session != launch.session || self.capability != launch.capability {
            return Err(
                "Node control does not match the original launch/session capability".into(),
            );
        }
        Ok(self.operation)
    }
}

pub(crate) fn send(owner: &mut OwnedApp, launch: &Launch, operation: NodeControl) -> Result<()> {
    owner
        .command(&frame(
            &NodeRequest::new(launch, operation),
            NODE_CONTROL_LIMIT,
        )?)
        .map_err(err)
}

pub(crate) fn frame(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(err)?;
    if bytes.len() >= limit {
        return Err(format!("process control frame exceeds {limit} bytes"));
    }
    bytes.push(b'\n');
    Ok(bytes)
}

/// Partial frames belong to the reader, so cancelling a poll cannot discard input.
pub(crate) struct Lines<R> {
    input: R,
    bytes: Vec<u8>,
    limit: usize,
}

impl<R: AsyncBufRead + Unpin> Lines<R> {
    pub(crate) fn new(input: R, limit: usize) -> Self {
        Self {
            input,
            bytes: Vec::new(),
            limit,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let chunk = self.input.fill_buf().await.map_err(err)?;
            if chunk.is_empty() {
                return if self.bytes.is_empty() {
                    Ok(None)
                } else {
                    Err("truncated process control frame".into())
                };
            }
            let newline = chunk.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(chunk.len(), |index| index + 1);
            if self.bytes.len() + count > self.limit {
                return Err(format!(
                    "process control frame exceeds {} bytes",
                    self.limit
                ));
            }
            self.bytes.extend_from_slice(&chunk[..count]);
            self.input.consume(count);
            if newline.is_some() {
                return Ok(Some(std::mem::take(&mut self.bytes)));
            }
        }
    }
}

pub(crate) struct NodeReader<R> {
    lines: Lines<R>,
}

impl<R: AsyncBufRead + Unpin> NodeReader<R> {
    pub(crate) fn new(input: R) -> Self {
        Self {
            lines: Lines::new(input, NODE_CONTROL_LIMIT),
        }
    }

    pub(crate) async fn next(&mut self, launch: &Launch) -> Result<Option<NodeControl>> {
        let Some(bytes) = self.lines.next().await? else {
            return Ok(None);
        };
        serde_json::from_slice::<NodeRequest>(&bytes)
            .map_err(err)?
            .authorize(launch)
            .map(Some)
    }
}

#[cfg(test)]
mod tests;
