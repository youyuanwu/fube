//! Static logical targets above generation-bound native connections.
use super::{err, GenerationHeader, Link, NodeInstance, Result, Role};
use fube_ra::protocol::{NodeUp, NodeUpAck, ReplicaUp};
use std::net::{IpAddr, SocketAddr};
use tokio::{
    net::{TcpSocket, TcpStream},
    time::{timeout_at, Duration, Instant},
};

/// Trusted composition configuration, not identity learned from a socket or hello.
/// The role selects local message policy independently of TCP direction.
pub struct Target {
    node: NodeInstance,
    role: Role,
    address: Option<SocketAddr>,
    generation: GenerationHeader,
    link: Option<Link>,
}

impl Target {
    pub fn new(
        node: NodeInstance,
        role: Role,
        address: Option<SocketAddr>,
        generation: GenerationHeader,
    ) -> Result<Self> {
        identity(&node)?;
        if generation.for_fm_replica {
            return Err("FMM target routing is unsupported".into());
        }
        if let Some(address) = address {
            endpoint(address)?;
        }
        Ok(Self {
            node,
            role,
            address,
            generation,
            link: None,
        })
    }

    fn generation(&self) -> GenerationHeader {
        self.link
            .as_ref()
            .map(Link::generation)
            .unwrap_or_else(|| self.generation.clone())
    }
}

/// A fixed, caller-driven target set. Acquiring a Link never submits or replays work.
///
/// Accepted and outgoing connections have identical routing authority. Returned
/// Links remain pinned to their physical generation, including after replacement.
/// A transport connection does not complete NodeUp or grant serving authority.
pub struct Targets {
    local: NodeInstance,
    targets: Vec<Target>,
    source_ip: Option<IpAddr>,
}

impl Targets {
    pub fn new(local: NodeInstance, targets: Vec<Target>) -> Result<Self> {
        identity(&local)?;
        for (index, target) in targets.iter().enumerate() {
            if target.node.id == local.id
                || targets[..index]
                    .iter()
                    .any(|prior| prior.node.id == target.node.id)
            {
                return Err("duplicate or local logical target".into());
            }
        }
        Ok(Self {
            local,
            targets,
            source_ip: None,
        })
    }

    /// Select the local TCP interface; this does not assert a native identity.
    pub fn source_ip(mut self, address: IpAddr) -> Self {
        self.source_ip = Some(address);
        self
    }

    fn target(&self, node: &NodeInstance) -> Result<&Target> {
        self.targets
            .iter()
            .find(|target| &target.node == node)
            .ok_or_else(|| "unconfigured or stale target incarnation".into())
    }

    fn target_mut(&mut self, node: &NodeInstance) -> Result<&mut Target> {
        self.targets
            .iter_mut()
            .find(|target| &target.node == node)
            .ok_or_else(|| "unconfigured or stale target incarnation".into())
    }

    pub fn link(&self, node: &NodeInstance) -> Result<Option<&Link>> {
        Ok(self
            .target(node)?
            .link
            .as_ref()
            .filter(|link| link.is_live()))
    }

    /// Install an endpoint supplied by the owner, never a remote advertisement.
    pub fn set_address(&mut self, node: &NodeInstance, address: SocketAddr) -> Result<()> {
        endpoint(address)?;
        let target = self.target_mut(node)?;
        if target.address.is_some_and(|old| old != address)
            && target.link.as_ref().is_some_and(Link::is_live)
        {
            return Err("cannot retarget a live target connection".into());
        }
        target.address = Some(address);
        Ok(())
    }

    /// Reuse either connection direction, or establish transport for fresh work.
    /// Only TCP acquisition is retried within the caller's budget, never a message.
    pub async fn connect(&mut self, node: &NodeInstance, until: Instant) -> Result<Link> {
        if Instant::now() >= until {
            return Err("target connection deadline expired".into());
        }
        if let Some(link) = self.link(node)? {
            return Ok(link.clone());
        }
        let target = self.target(node)?;
        let address = target
            .address
            .ok_or("target has no configured dial endpoint")?;
        let stream = loop {
            let socket = if address.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(err)?;
            if let Some(ip) = self.source_ip {
                socket.bind(SocketAddr::new(ip, 0)).map_err(err)?;
            }
            let result = timeout_at(until, socket.connect(address))
                .await
                .map_err(err)?;
            match result {
                Ok(stream) => break stream,
                Err(error) => {
                    let retry = Instant::now() + Duration::from_millis(20);
                    if retry >= until {
                        return Err(format!("target TCP connection failed: {error}"));
                    }
                    tokio::time::sleep_until(retry).await;
                }
            }
        };
        let role = self.target(node)?.role;
        let session = super::initiate(stream, role, until).await?;
        self.install(node, session)
    }

    /// The caller identifies the configured relationship before native establishment.
    /// Duplicate live connections are rejected without disturbing the current Link.
    pub async fn accept(
        &mut self,
        node: &NodeInstance,
        stream: TcpStream,
        until: Instant,
    ) -> Result<Link> {
        if self.link(node)?.is_some() {
            return Err("target already has a live connection".into());
        }
        let role = self.target(node)?.role;
        let session = super::accept(stream, role, until).await?;
        self.install(node, session)
    }

    /// Retry this native idempotent registration on reply timeout only.
    /// Invalid replies and failed generations are surfaced, not replayed.
    pub async fn node_up(
        &mut self,
        node: &NodeInstance,
        body: &NodeUp,
        until: Instant,
    ) -> Result<(NodeUpAck, GenerationHeader)> {
        if self.target(node)?.role != Role::Node {
            return Err("NodeUp requires a Node-to-FM target".into());
        }
        timeout_at(until, async {
            loop {
                let link = self.connect(node, until).await?;
                if let Some(reply) = link.node_up_optional(body).await? {
                    return Ok(reply);
                }
            }
        })
        .await
        .map_err(err)?
    }

    /// Complete the supported fresh, empty-inventory bootstrap on the same Link.
    /// The caller still owns applying the ACK and admitting its local Node.
    pub async fn bootstrap_fresh(
        &mut self,
        node: &NodeInstance,
        body: &NodeUp,
        until: Instant,
    ) -> Result<(NodeUpAck, GenerationHeader)> {
        body.validate_fresh_stateless().map_err(err)?;
        let target = self.target(node)?;
        if target.role != Role::Node {
            return Err("fresh bootstrap requires a Node-to-FM target".into());
        }
        let expected = target.generation();
        let result = timeout_at(until, async {
            let (ack, generation) = self.node_up(node, body, until).await?;
            ack.validate_fresh_activated(&body.node.version)
                .map_err(err)?;
            if generation != expected {
                return Err("foreign NodeUpAck generation".into());
            }
            let link = self
                .link(node)?
                .ok_or("NodeUp completed on a retired target")?;
            let upload = ReplicaUp {
                last: true,
                ..ReplicaUp::default()
            };
            link.replica_up(&upload).await?;
            let incoming = link
                .receive(until)
                .await?
                .ok_or("final inventory reply timeout")?;
            let (reply, header) = incoming.replica_up_reply(link)?;
            if reply != upload || header != generation {
                return Err(
                    "fresh bootstrap requires the matching final empty inventory reply".into(),
                );
            }
            Ok((ack, generation))
        })
        .await
        .map_err(err)
        .and_then(|result| result);
        if result.is_err() {
            if let Some(link) = &self.target(node)?.link {
                link.retire();
            }
        }
        result
    }

    fn install(&mut self, node: &NodeInstance, session: super::Session) -> Result<Link> {
        let local = self.local.clone();
        let target = self.target_mut(node)?;
        let make = if target.role == Role::Peer {
            Link::new_peer
        } else {
            Link::new
        };
        let link = make(session, local, target.node.clone(), target.generation())?;
        target.link = Some(link.clone());
        Ok(link)
    }

    /// Replace a target only after its owner has proved the old Node and apps fenced.
    ///
    /// This method checks an exact predecessor and increasing incarnation and joins
    /// its transport. It cannot prove process fencing or admit the replacement's
    /// inventory: those remain supervisor/FM responsibilities.
    pub async fn replace_after_fencing(
        &mut self,
        previous: &NodeInstance,
        replacement: Target,
        until: Instant,
    ) -> Result<()> {
        let target = self.target(previous)?;
        if replacement.node.id != previous.id
            || replacement.node.instance <= previous.instance
            || replacement.role != target.role
            || replacement
                .generation
                .number
                .older_than(&target.generation().number)
        {
            return Err(
                "replacement must advance the exact target incarnation and generation".into(),
            );
        }
        if let Some(link) = &target.link {
            link.closed(until).await?;
        }
        *self.target_mut(previous)? = replacement;
        Ok(())
    }
}

fn identity(node: &NodeInstance) -> Result<()> {
    if node.id == [0; 16] || node.instance == 0 {
        return Err("target routing requires a nonzero exact Node instance".into());
    }
    Ok(())
}

fn endpoint(address: SocketAddr) -> Result<()> {
    if address.port() == 0
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast())
    {
        return Err("target requires a concrete unicast TCP endpoint".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
