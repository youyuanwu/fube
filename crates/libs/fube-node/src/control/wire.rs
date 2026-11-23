//! Native FM/RA and RA/RA channel policy, bounded decode and exact source binding.
use super::{err, Result, MAX_BODY_BYTES as LIMIT};
use fube_ra::protocol::{
    GenerationHeader, NodeInstance, FM_ACTOR, GENERATION_HEADER, RA_ACTOR, RS_ACTOR,
};
use fubst::serde::from_slice_with_budget;
use furpc::{Body, HeaderId, HeaderRecord, Headers, Message};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

mod bootstrap;
mod fm_client;
mod fm_intake;
mod fm_server;
mod one_way;
mod peer_client;
mod peer_intake;
mod session;
pub use bootstrap::LfumUploadRequest;
pub(super) use fm_client::FmClient;
pub(super) use fm_intake::{generation_parts, Incoming as FmIncoming};
pub use fm_server::{FmPayload, FmReport};
pub(super) use one_way::Call as OneWayCall;
pub(super) use peer_client::PeerClient;
pub(super) use peer_intake::Incoming as PeerIncoming;
pub use session::{Failure, Incoming, Session};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Fm,
    Node,
    Peer,
}

struct Properties {
    actor: i64,
    action: String,
    expects_reply: bool,
    uncorrelated_reply: bool,
    high_priority: bool,
}
impl Properties {
    fn new(message: &Message) -> furpc::Result<Self> {
        Self::from_headers(message.headers())
    }
    fn from_headers(headers: &Headers) -> furpc::Result<Self> {
        let common = headers.effective();
        Ok(Self {
            actor: common.actor.get(),
            action: String::from_utf16(common.action())
                .map_err(|_| furpc::Error::Protocol("invalid reliability action"))?,
            expects_reply: common.expects_reply,
            uncorrelated_reply: common.uncorrelated_reply,
            high_priority: common.high_priority,
        })
    }
}
pub fn action(message: &Message) -> Result<String> {
    Properties::new(message).map(|p| p.action).map_err(err)
}
pub async fn connect(address: std::net::SocketAddr, role: Role, until: Instant) -> Result<Session> {
    let stream = tokio::time::timeout_at(until, tokio::net::TcpStream::connect(address))
        .await
        .map_err(err)?
        .map_err(err)?;
    initiate(stream, role, until).await
}
pub async fn initiate(
    stream: tokio::net::TcpStream,
    role: Role,
    until: Instant,
) -> Result<Session> {
    session::establish(stream, role, until, false).await
}
pub async fn accept(stream: tokio::net::TcpStream, role: Role, until: Instant) -> Result<Session> {
    session::establish(stream, role, until, true).await
}

// Native pin 3988b451: Federation/PToPHeader.h FABRIC_FIELDS_06;
// PToPActor.h Direct=0; Transport/MessageHeaderId.h PToP=0x8013.
// Native NodeUp contains NodeDescription, NOT a made-up incarnation field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PToP {
    from: NodeInstance,
    to: NodeInstance,
    actor: i64,
    from_ring: String,
    to_ring: String,
    exact_instance: bool,
}
const PTOP: u16 = 0x8013;

fn bounded(message: &Message) -> furpc::Result<()> {
    bounded_parts(message.headers(), message.body())
}
fn bounded_parts(headers: &Headers, body: &Body) -> furpc::Result<()> {
    if body.len() > LIMIT
        || headers.records().count() > 16
        || headers
            .records()
            .map(|header| header.value().len())
            .sum::<usize>()
            > 4096
    {
        return Err(furpc::Error::ResourceLimit("cluster envelope"));
    }
    Ok(())
}
fn fm_ingress(message: &Message, props: &Properties) -> furpc::Result<()> {
    bounded(message)?;
    if props.actor != FM_ACTOR
        || props.uncorrelated_reply
        || !matches!(
            props.action.as_str(),
            "NodeUp"
                | "LFUMUpload"
                | "ReplicaUp"
                | "ServiceTypeNotification"
                | "AddInstanceReply"
                | "AddPrimaryReply"
                | "AddReplicaReply"
                | "DoReconfigurationReply"
                | "ChangeConfiguration"
                | "ReplicaEndpointUpdated"
                | "RemoveReplicaReply"
                | "RemoveInstanceReply"
                | "DeleteReplicaReply"
                | "NodeActivateReply"
                | "NodeDeactivateReply"
                | "NodeUpdateServiceReply"
                | "ServiceTypeEnabled"
                | "ServiceTypeDisabled"
                | "ReportLoad"
                | "GenerationUpdateReject"
        )
        || props.expects_reply != matches!(props.action.as_str(), "NodeUp" | "LFUMUpload")
        || props.high_priority
            != matches!(
                props.action.as_str(),
                "NodeUp"
                    | "LFUMUpload"
                    | "ReplicaUp"
                    | "AddInstanceReply"
                    | "AddPrimaryReply"
                    | "AddReplicaReply"
                    | "DoReconfigurationReply"
                    | "ChangeConfiguration"
                    | "RemoveReplicaReply"
                    | "RemoveInstanceReply"
                    | "DeleteReplicaReply"
                    | "GenerationUpdateReject"
            )
    {
        return Err(furpc::Error::Protocol("unsupported FM ingress"));
    }
    Ok(())
}
fn node_ingress(message: &Message, props: &Properties) -> furpc::Result<()> {
    bounded(message)?;
    if props.actor != RA_ACTOR
        || props.expects_reply != (props.action == "GenerationProposal")
        || props.uncorrelated_reply
        || props.high_priority
            != matches!(
                props.action.as_str(),
                "AddInstance"
                    | "AddPrimary"
                    | "AddReplica"
                    | "DoReconfiguration"
                    | "ChangeConfigurationReply"
                    | "RemoveReplica"
                    | "RemoveInstance"
                    | "DeleteReplica"
                    | "ReplicaUpReply"
                    | "GenerationProposal"
                    | "GenerationUpdate"
            )
        || !matches!(
            props.action.as_str(),
            "AddInstance"
                | "AddPrimary"
                | "AddReplica"
                | "DoReconfiguration"
                | "ChangeConfigurationReply"
                | "RemoveReplica"
                | "RemoveInstance"
                | "DeleteReplica"
                | "ReplicaUpReply"
                | "ServiceTypeNotificationReply"
                | "ReplicaEndpointUpdatedReply"
                | "NodeActivateRequest"
                | "NodeDeactivateRequest"
                | "NodeUpdateServiceRequest"
                | "ServiceTypeEnabledReply"
                | "ServiceTypeDisabledReply"
                | "GenerationProposal"
                | "GenerationUpdate"
        )
    {
        return Err(furpc::Error::Protocol("unsupported node ingress"));
    }
    Ok(())
}
fn ack(message: &Message, props: &Properties) -> furpc::Result<()> {
    bounded(message)?;
    if !matches!(
        (props.actor, props.action.as_str()),
        (RS_ACTOR, "NodeUpAck") | (RA_ACTOR, "LFUMUploadReply")
    ) || props.expects_reply
        || props.uncorrelated_reply
        || !props.high_priority
    {
        return Err(furpc::Error::Protocol("unsupported bootstrap reply"));
    }
    Ok(())
}
fn proposal_ack(message: &Message, props: &Properties) -> furpc::Result<()> {
    bounded(message)?;
    if props.actor != FM_ACTOR
        || props.action != "GenerationProposalReply"
        || props.expects_reply
        || props.uncorrelated_reply
        || !props.high_priority
    {
        return Err(furpc::Error::Protocol(
            "unsupported generation proposal reply",
        ));
    }
    Ok(())
}
fn fm_out(message: &Message, props: &Properties, request: bool) -> furpc::Result<()> {
    if request != (props.action == "GenerationProposal") {
        return Err(furpc::Error::Protocol("wrong FM send mechanism"));
    }
    let mut canonical = Properties::new(message)?;
    canonical.expects_reply = canonical.action == "GenerationProposal";
    node_ingress(message, &canonical)
}
fn node_out(message: &Message, props: &Properties, request: bool) -> furpc::Result<()> {
    if request != matches!(props.action.as_str(), "NodeUp" | "LFUMUpload") {
        return Err(furpc::Error::Protocol("wrong node send mechanism"));
    }
    // RPC adds ExpectsReply below outbound policy.
    let mut canonical = Properties::new(message)?;
    canonical.expects_reply = matches!(canonical.action.as_str(), "NodeUp" | "LFUMUpload");
    fm_ingress(message, &canonical)
}
fn peer_ingress(message: &Message, props: &Properties) -> furpc::Result<()> {
    bounded(message)?;
    if props.actor != RA_ACTOR
        || !props.high_priority
        || props.expects_reply
        || props.uncorrelated_reply
        || !matches!(
            props.action.as_str(),
            "CreateReplica"
                | "CreateReplicaReply"
                | "Activate"
                | "ActivateReply"
                | "Deactivate"
                | "DeactivateReply"
                | "GetLSN"
                | "GetLSNReply"
                | "DoReconfiguration"
        )
        || message
            .headers()
            .records()
            .filter(|header| header.id().get() == GENERATION_HEADER)
            .count()
            != 1
    {
        return Err(furpc::Error::Protocol("unsupported peer ingress"));
    }
    Ok(())
}
fn peer_out(message: &Message, props: &Properties, request: bool) -> furpc::Result<()> {
    if request {
        return Err(furpc::Error::Protocol("peer traffic is one-way"));
    }
    peer_ingress(message, props)
}
fn inbound(role: Role, message: &Message) -> furpc::Result<()> {
    let props = Properties::new(message)?;
    match role {
        Role::Fm => fm_ingress(message, &props),
        Role::Node => node_ingress(message, &props),
        Role::Peer => peer_ingress(message, &props),
    }
}
fn outbound(role: Role, message: &Message, request: bool) -> furpc::Result<()> {
    let props = Properties::new(message)?;
    match role {
        Role::Fm => fm_out(message, &props, request),
        Role::Node => node_out(message, &props, request),
        Role::Peer => peer_out(message, &props, request),
    }
}
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let body = Body::from_bytes(bytes.to_vec()).map_err(err)?;
    furpc::codec::decode(&mut one_way::Budgeted::<T>::default(), &body, LIMIT)
        .map_err(one_way::decode_error)
}

// Compatibility validation for byte-facing FM consumers and independent fixtures.
pub fn predecode(message: &Message) -> Result<()> {
    one_way::decode_message(message, one_way::discard)
}

pub(super) fn fm_generation(headers: &Headers) -> Result<(GenerationHeader, bool)> {
    let mut records = headers
        .records()
        .filter(|header| header.id().get() == GENERATION_HEADER);
    let generation = decode(records.next().ok_or("missing FM generation")?.value())?;
    Ok((generation, records.next().is_some()))
}

#[derive(Clone)]
pub struct Link {
    session: Arc<Session>,
    local: NodeInstance,
    remote: NodeInstance,
    generation: Arc<Mutex<GenerationHeader>>,
    kind: LinkKind,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkKind {
    Fm,
    Peer,
}
impl Link {
    pub fn generation(&self) -> GenerationHeader {
        self.generation.lock().unwrap().clone()
    }
    pub fn advance_generation(&self, generation: &GenerationHeader) {
        let mut current = self.generation.lock().unwrap();
        if !generation.number.older_than(&current.number) {
            *current = generation.clone();
        }
    }
    // Only the composition owner constructs a Link, from its original one-use
    // admitted Channel. Deployment/session binding is local capability state,
    // not a fabricated universal native header or a reconnect credential.
    pub fn new(
        session: Session,
        local: NodeInstance,
        remote: NodeInstance,
        generation: GenerationHeader,
    ) -> Result<Self> {
        Ok(Self {
            session: Arc::new(session),
            local,
            remote,
            generation: Arc::new(Mutex::new(generation)),
            kind: LinkKind::Fm,
        })
    }
    /// Bind an original RA peer Channel; stale generations are native message drops.
    pub fn new_peer(
        session: Session,
        local: NodeInstance,
        remote: NodeInstance,
        generation: GenerationHeader,
    ) -> Result<Self> {
        let mut link = Self::new(session, local, remote, generation)?;
        link.kind = LinkKind::Peer;
        Ok(link)
    }
    pub fn seal(&self, message: Message) -> Result<Message> {
        if message
            .headers()
            .records()
            .any(|header| header.id().get() == PTOP)
        {
            return Err("duplicate source header".into());
        }
        let source = HeaderRecord::native(
            HeaderId::new(PTOP).map_err(err)?,
            &PToP {
                from: self.local.clone(),
                to: self.remote.clone(),
                actor: 0,
                from_ring: String::new(),
                to_ring: String::new(),
                exact_instance: true,
            },
        )
        .map_err(err)?;
        let mut builder = Message::builder().body(message.body().clone());
        for header in message.headers().records() {
            builder = builder.append_header(header.to_owned());
        }
        let message = builder.append_header(source).build().map_err(err)?;
        bounded(&message).map_err(err)?;
        Ok(message)
    }
    pub fn validate(&self, message: &Message) -> Result<()> {
        if !self.is_current(message)? {
            return Err("foreign/stale/duplicate native FM generation".into());
        }
        Ok(())
    }
    fn is_current(&self, message: &Message) -> Result<bool> {
        self.is_current_parts(message.headers(), message.body())
    }
    fn is_current_parts(&self, headers: &Headers, body: &Body) -> Result<bool> {
        validate_source(headers, body, &self.local, &self.remote)?;
        let mut generations = headers
            .records()
            .filter(|header| header.id().get() == GENERATION_HEADER);
        if let Some(header) = generations.next() {
            let generation: GenerationHeader = decode(header.value())?;
            let generation_independent = matches!(
                Properties::from_headers(headers)
                    .map_err(err)?
                    .action
                    .as_str(),
                "GenerationProposal" | "GenerationUpdate" | "LFUMUploadReply"
            );
            if generations.next().is_some() || generation.for_fm_replica {
                return Err("foreign/stale/duplicate native FM generation".into());
            }
            if !generation_independent && generation.number.older_than(&self.generation().number) {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub async fn send(&self, message: Message) -> Result<()> {
        self.session
            .send(self.seal(message)?, deadline())
            .await
            .map_err(err)
    }
    pub async fn receive(&self, until: Instant) -> Result<Option<Incoming>> {
        self.receive_checked(until)
            .await
            .map_err(|error| error.to_string())
    }
    /// Preserve transport causes separately from native-envelope validation failures.
    pub async fn receive_checked(
        &self,
        until: Instant,
    ) -> std::result::Result<Option<Incoming>, ReceiveError> {
        match self.session.receive(until).await {
            Ok(Some(mut incoming)) => {
                if incoming.source.generation() != self.session.peer.info().generation {
                    self.retire();
                    return Err(ReceiveError::Invalid(
                        "foreign Channel source capability".into(),
                    ));
                }
                match self.is_current(&incoming.message) {
                    Ok(true) => {}
                    Ok(false) => {
                        if self.kind != LinkKind::Peer {
                            self.retire();
                            return Err(ReceiveError::Invalid(
                                "foreign/stale/duplicate native FM generation".into(),
                            ));
                        }
                        if let Some(slot) = incoming.one_way.take() {
                            if slot
                                .response
                                .send(Ok(furpc::IngressResponse::NoReply))
                                .is_err()
                            {
                                self.retire();
                                return Err(ReceiveError::Invalid(err(furpc::Error::Closed)));
                            }
                        }
                        eprintln!("ignored native message from older FM generation");
                        return Ok(None);
                    }
                    Err(error) => {
                        self.retire();
                        return Err(ReceiveError::Invalid(error));
                    }
                }
                if let Some(slot) = incoming.one_way.take() {
                    match slot.prepare(self).await {
                        Ok(prepared) => incoming.prepared = Some(prepared),
                        Err(error) => {
                            self.retire();
                            return Err(ReceiveError::Invalid(error));
                        }
                    }
                }
                Ok(Some(incoming))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(ReceiveError::Transport(error)),
        }
    }
    pub fn is_live(&self) -> bool {
        self.terminal_error().is_none()
    }
    pub fn terminal_error(&self) -> Option<Failure> {
        self.session.terminal_error()
    }
    pub fn retire(&self) {
        self.session.retire();
    }
    pub async fn closed(&self, until: Instant) -> Result<()> {
        self.session.closed(until).await
    }
    pub async fn close(&self) -> Result<()> {
        self.closed(deadline()).await
    }
}
#[derive(Debug)]
pub enum ReceiveError {
    Transport(Failure),
    Invalid(String),
}
impl std::fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error:?}"),
            Self::Invalid(error) => f.write_str(error),
        }
    }
}
fn validate_source(
    headers: &Headers,
    body: &Body,
    local: &NodeInstance,
    remote: &NodeInstance,
) -> Result<()> {
    bounded_parts(headers, body).map_err(err)?;
    let mut headers = headers.records().filter(|header| header.id().get() == PTOP);
    let header = headers.next().ok_or("missing native source identity")?;
    if headers.next().is_some() {
        return Err("duplicate native source identity".into());
    }
    let mut budget = 8192usize;
    let value: PToP = from_slice_with_budget(header.value(), &mut |bytes| {
        budget = budget
            .checked_sub(bytes)
            .ok_or(fubst::Error::ResourceLimit("PToP decode"))?;
        Ok(())
    })
    .map_err(err)?;
    if value
        != (PToP {
            from: remote.clone(),
            to: local.clone(),
            actor: 0,
            from_ring: String::new(),
            to_ring: String::new(),
            exact_instance: true,
        })
    {
        return Err("foreign/stale native sender or recipient incarnation".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fube_ra::protocol::*;

    pub(super) async fn links(peer: bool) -> (Link, Link) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender = connect(
            listener.local_addr().unwrap(),
            if peer { Role::Peer } else { Role::Node },
            deadline(),
        );
        let (sender, receiver) = tokio::join!(sender, async {
            let (stream, _) = listener.accept().await.unwrap();
            accept(stream, if peer { Role::Peer } else { Role::Fm }, deadline()).await
        });
        let local = NodeInstance {
            id: [1; 16],
            instance: 10,
        };
        let remote = NodeInstance {
            id: [2; 16],
            instance: 20,
        };
        let generation = GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        };
        (
            (if peer { Link::new_peer } else { Link::new })(
                sender.unwrap(),
                local.clone(),
                remote.clone(),
                generation.clone(),
            )
            .unwrap(),
            (if peer { Link::new_peer } else { Link::new })(
                receiver.unwrap(),
                remote,
                local,
                generation,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn stale_peer_generation_releases_transport_but_foreign_incarnation_still_closes_it() {
        let (sender, receiver) = links(true).await;
        let old = sender.generation();
        let mut current = old.clone();
        current.number.generation += 1;
        receiver.advance_generation(&current);
        sender
            .send(get_lsn_reply_message(&GetLsnReply::default(), &old).unwrap())
            .await
            .unwrap();
        assert!(receiver
            .receive_checked(deadline())
            .await
            .unwrap()
            .is_none());
        assert!(receiver.terminal_error().is_none());
        sender
            .send(get_lsn_reply_message(&GetLsnReply::default(), &current).unwrap())
            .await
            .unwrap();
        let incoming = receiver.receive_checked(deadline()).await.unwrap().unwrap();
        assert_eq!(decode_get_lsn_reply(&incoming.message).unwrap().1, current);
        let invalid = sender
            .seal(get_lsn_reply_message(&GetLsnReply::default(), &old).unwrap())
            .unwrap();
        let mut foreign = sender.local.clone();
        foreign.instance += 1;
        let mut builder = Message::builder().body(invalid.body().clone());
        for header in invalid.headers().records().filter(|h| h.id().get() != PTOP) {
            builder = builder.append_header(header.to_owned());
        }
        let invalid = builder
            .append_header(
                HeaderRecord::native(
                    HeaderId::new(PTOP).unwrap(),
                    &PToP {
                        from: foreign,
                        to: sender.remote.clone(),
                        actor: 0,
                        from_ring: String::new(),
                        to_ring: String::new(),
                        exact_instance: true,
                    },
                )
                .unwrap(),
            )
            .build()
            .unwrap();
        sender.session.send(invalid, deadline()).await.unwrap();
        assert!(matches!(
            receiver.receive_checked(deadline()).await,
            Err(ReceiveError::Invalid(_))
        ));
        receiver.close().await.unwrap();
        sender.close().await.unwrap();
    }

    #[tokio::test]
    async fn older_lfum_request_remains_acknowledgeable_after_fm_generation_advances() {
        let (node, fm) = links(false).await;
        let old = node.generation();
        let mut current = old.clone();
        current.number.generation += 1;
        fm.advance_generation(&current);
        let body = LfumUpload {
            generation: old.number.clone(),
            ..LfumUpload::default()
        };
        let (reply, ()) = tokio::join!(
            node.lfum_upload(LfumUploadRequest::new(&body).unwrap()),
            async {
                let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
                assert_eq!(decode_lfum_upload(&incoming.message).unwrap(), body);
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .lfum_upload(&fm, |decoded| {
                        assert_eq!(decoded, body);
                        Ok(old.clone())
                    })
                    .await
                    .unwrap();
            }
        );
        assert_eq!(reply.unwrap().unwrap(), old);
        assert_eq!(fm.generation(), current);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_bootstrap_preserves_native_profiles_and_correlated_results() {
        let (node, fm) = links(false).await;
        let version = FabricVersionInstance::native_default();
        let up = NodeUp::fresh_stateless(
            "node".into(),
            "typed-bootstrap".into(),
            "127.0.0.1:12345".parse().unwrap(),
            version.clone(),
        )
        .unwrap();
        let ack = NodeUpAck::fresh_activated(version, 42).unwrap();
        let generation = node.generation();
        let (reply, ()) = tokio::join!(node.node_up(&up), async {
            let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
            assert_eq!(incoming.message.headers().effective().actor.get(), 11);
            assert_eq!(decode_node_up(&incoming.message).unwrap(), up);
            incoming
                .reply
                .take()
                .unwrap()
                .node_up(&fm, |decoded| {
                    assert_eq!(decoded, up);
                    Ok((ack.clone(), generation.clone()))
                })
                .await
                .unwrap();
        });
        assert_eq!(reply.unwrap(), (ack, generation.clone()));
        let proposal = GenerationProposalReply {
            proposed: generation.number.clone(),
            current: generation.number.clone(),
            error: 0,
        };
        let (reply, ()) = tokio::join!(fm.generation_proposal(&generation), async {
            let mut incoming = node.receive_checked(deadline()).await.unwrap().unwrap();
            assert_eq!(incoming.message.headers().effective().actor.get(), 12);
            assert_eq!(
                decode_generation_proposal(&incoming.message).unwrap(),
                generation
            );
            incoming
                .reply
                .take()
                .unwrap()
                .generation_proposal(&node, |decoded| {
                    assert_eq!(decoded, generation);
                    Ok(proposal.clone())
                })
                .await
                .unwrap();
        });
        assert_eq!(reply.unwrap(), proposal);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_server_keeps_original_request_despite_mutated_message_view() {
        let (node, fm) = links(false).await;
        let body = LfumUpload {
            generation: node.generation().number,
            ..LfumUpload::default()
        };
        let generation = fm.generation();
        let mut calls = 0;
        let (client, server) = tokio::join!(
            node.lfum_upload(LfumUploadRequest::new(&body).unwrap()),
            async {
                let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
                incoming.message = Message::builder()
                    .body(Body::from_bytes(vec![0xff]).unwrap())
                    .build()
                    .unwrap();
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .lfum_upload(&fm, |decoded| {
                        calls += 1;
                        assert_eq!(decoded, body);
                        Ok(generation.clone())
                    })
                    .await
            }
        );
        assert_eq!(calls, 1);
        assert_eq!(client.unwrap(), Some(generation.clone()));
        assert_eq!(server.unwrap(), generation);
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_server_rejects_method_source_and_schema_before_domain_handler() {
        let (node, fm) = links(false).await;
        for invalid in ["method", "source", "schema"] {
            let original = lfum_upload_message(&LfumUpload::default()).unwrap();
            let mut builder = Message::builder().body(Body::from_bytes(vec![0xff]).unwrap());
            for header in original.headers().records() {
                builder = builder.append_header(header.to_owned());
            }
            let message = node.seal(builder.build().unwrap()).unwrap();
            let mut request = furpc::Request::new(message.body().clone());
            *request.headers_mut() = message.headers().clone();
            let peer = node.session.peer.clone();
            let client = tokio::spawn(async move { peer.request(request, deadline()).await });
            let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
            let slot = incoming.reply.take().unwrap();
            let error = match invalid {
                "method" => slot
                    .node_up(&fm, |_| panic!("mismatched method reached handler"))
                    .await
                    .unwrap_err(),
                "source" => {
                    let foreign = Link {
                        remote: NodeInstance {
                            instance: node.local.instance + 1,
                            ..node.local.clone()
                        },
                        ..fm.clone()
                    };
                    slot.lfum_upload(&foreign, |_| panic!("foreign source reached handler"))
                        .await
                        .unwrap_err()
                }
                _ => slot
                    .lfum_upload(&fm, |_| panic!("malformed schema reached handler"))
                    .await
                    .unwrap_err(),
            };
            assert!(error.contains(match invalid {
                "method" => "unsupported bootstrap invocation",
                "source" => "foreign/stale native sender or recipient incarnation",
                _ => "decode",
            }));
            client.abort();
            match client.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(result) => assert!(result.is_err()),
            }
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_lfum_rejects_wrong_method_and_nonempty_success_body() {
        let (node, fm) = links(false).await;
        let generation = fm.generation();
        let nonempty = lfum_upload_reply_message(&generation).unwrap();
        let mut builder = Message::builder().body(Body::from_bytes(vec![0xff]).unwrap());
        for header in nonempty.headers().records() {
            builder = builder.append_header(header.to_owned());
        }
        for (reply, expected) in [
            (
                node_up_ack_message(&NodeUpAck::default(), &generation).unwrap(),
                "unsupported bootstrap reply",
            ),
            (builder.build().unwrap(), "LFUMUploadReply must be bodyless"),
        ] {
            let request = LfumUploadRequest::new(&LfumUpload::default()).unwrap();
            let (result, ()) = tokio::join!(node.lfum_upload(request), async {
                let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .reply(fm.seal(reply).unwrap(), deadline())
                    .await
                    .unwrap();
            });
            assert!(result.unwrap_err().contains(expected));
            assert!(node.is_live());
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_bootstrap_rejects_source_and_generation_before_malformed_body_decode() {
        let (node, fm) = links(false).await;
        let up = NodeUp::fresh_stateless(
            "node".into(),
            "typed-bootstrap".into(),
            "127.0.0.1:12345".parse().unwrap(),
            FabricVersionInstance::native_default(),
        )
        .unwrap();
        for (invalid, expected) in [
            (
                "source",
                "foreign/stale native sender or recipient incarnation",
            ),
            ("stale", "foreign/stale/duplicate native FM generation"),
            ("missing", "missing reliability header"),
            ("duplicate", "foreign/stale/duplicate native FM generation"),
        ] {
            let (result, ()) = tokio::join!(node.node_up(&up), async {
                let mut incoming = fm.receive_checked(deadline()).await.unwrap().unwrap();
                let source = if invalid == "source" {
                    Link {
                        remote: NodeInstance {
                            instance: node.local.instance + 1,
                            ..node.local.clone()
                        },
                        ..fm.clone()
                    }
                } else {
                    fm.clone()
                };
                let mut generation = fm.generation();
                if invalid == "stale" {
                    generation.number.generation -= 1;
                }
                let original = node_up_ack_message(&NodeUpAck::default(), &generation).unwrap();
                let mut builder = Message::builder().body(Body::from_bytes(vec![0xff]).unwrap());
                for header in original.headers().records() {
                    if invalid != "missing" || header.id().get() != GENERATION_HEADER {
                        builder = builder.append_header(header.to_owned());
                    }
                }
                if invalid == "duplicate" {
                    builder = builder.append_header(generation.header().unwrap());
                }
                incoming
                    .reply
                    .take()
                    .unwrap()
                    .reply(source.seal(builder.build().unwrap()).unwrap(), deadline())
                    .await
                    .unwrap();
            });
            assert!(result.unwrap_err().contains(expected));
            assert!(node.is_live());
        }
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_optional_bootstrap_timeouts_preserve_original_session() {
        let (node, fm) = links(false).await;
        let request = LfumUploadRequest::new(&LfumUpload::default()).unwrap();
        let generation = fm.generation();
        let (lfum, proposal, retained) = tokio::join!(
            node.lfum_upload(request),
            fm.generation_proposal_optional(&generation),
            async {
                let lfum = fm.receive_checked(deadline()).await.unwrap().unwrap();
                let proposal = node.receive_checked(deadline()).await.unwrap().unwrap();
                (lfum, proposal)
            }
        );
        assert!(lfum.unwrap().is_none());
        assert!(proposal.unwrap().is_none());
        assert!(node.is_live());
        assert!(fm.is_live());
        let (mut lfum, mut proposal) = retained;
        assert!(lfum
            .reply
            .take()
            .unwrap()
            .lfum_upload(&fm, |_| panic!("expired LFUM handler executed"))
            .await
            .is_err());
        assert!(proposal
            .reply
            .take()
            .unwrap()
            .generation_proposal(&node, |_| panic!("expired proposal handler executed"))
            .await
            .is_err());
        node.close().await.unwrap();
        fm.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_close_joins_original_io_and_revokes_retained_response_slots() {
        let (node, fm) = links(false).await;
        let request_owner = node.clone();
        let request = tokio::spawn(async move {
            request_owner
                .lfum_upload(LfumUploadRequest::new(&LfumUpload::default()).unwrap())
                .await
        });
        let mut incoming = fm.receive(deadline()).await.unwrap().unwrap();
        let clone = node.clone();
        let (first, second) = tokio::join!(node.close(), clone.close());
        first.unwrap();
        second.unwrap();
        assert!(!node.is_live());
        assert!(request.await.unwrap().is_err());
        fm.close().await.unwrap();
        assert!(incoming
            .reply
            .take()
            .unwrap()
            .lfum_upload(&fm, |_| panic!("retired LFUM handler executed"))
            .await
            .is_err());
        assert!(node
            .send(replica_up_message(&ReplicaUp::default(), Owner::Fm).unwrap())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn concurrent_receive_waits_share_the_original_absolute_deadline() {
        let (sender, receiver) = links(true).await;
        let other = receiver.clone();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(async move {
            entered.send(()).unwrap();
            other.receive(deadline()).await
        });
        waiting.await.unwrap();
        let until = Instant::now() + Duration::from_millis(5);
        assert!(tokio::time::timeout_at(
            until + Duration::from_millis(100),
            receiver.receive(until)
        )
        .await
        .unwrap()
        .unwrap()
        .is_none());
        receiver.close().await.unwrap();
        assert!(first.await.unwrap().is_err());
        sender.close().await.unwrap();
    }

    #[tokio::test]
    async fn verified_domain_inbox_overflow_retires_its_original_session() {
        let (sender, receiver) = links(true).await;
        for count in 1..=8 {
            sender
                .send(get_lsn_reply_message(&GetLsnReply::default(), &sender.generation()).unwrap())
                .await
                .unwrap();
            tokio::time::timeout_at(deadline(), async {
                while receiver.session.queued() != count {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }
        sender
            .send(get_lsn_reply_message(&GetLsnReply::default(), &sender.generation()).unwrap())
            .await
            .unwrap();
        tokio::time::timeout_at(deadline(), async {
            while receiver.terminal_error().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            receiver.terminal_error(),
            Some(Failure::Native(furpc::Error::ResourceLimit(
                "Node control inbox"
            )))
        ));
        receiver.close().await.unwrap();
        assert_eq!(receiver.session.queued(), 0);
        sender.close().await.unwrap();
    }

    #[test]
    fn peer_idle_channel_admits_only_generation_bound_high_priority_one_way_ra_actions() {
        use fube_ra::protocol::*;
        let generation = GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        };
        let body = CreateReplica::new(AddInstance::default(), ReplicaDeactivationInfo::default());
        for message in [
            create_replica_message(&body, &generation).unwrap(),
            create_replica_reply_message(&CreateReplicaReply::default(), &generation).unwrap(),
        ] {
            let properties = Properties::new(&message).unwrap();
            peer_ingress(&message, &properties).unwrap();
            predecode(&message).unwrap();
            assert!(peer_out(&message, &properties, true).is_err());
            for invalid in [
                Properties {
                    actor: FM_ACTOR,
                    ..Properties::new(&message).unwrap()
                },
                Properties {
                    high_priority: false,
                    ..Properties::new(&message).unwrap()
                },
                Properties {
                    expects_reply: true,
                    ..Properties::new(&message).unwrap()
                },
                Properties {
                    uncorrelated_reply: true,
                    ..Properties::new(&message).unwrap()
                },
                Properties {
                    action: "AddPrimary".into(),
                    ..Properties::new(&message).unwrap()
                },
            ] {
                assert!(peer_ingress(&message, &invalid).is_err());
            }
            let mut builder = Message::builder().body(message.body().clone());
            for h in message
                .headers()
                .records()
                .filter(|h| h.id().get() != GENERATION_HEADER)
            {
                builder = builder.append_header(h.to_owned());
            }
            let missing = builder.build().unwrap();
            assert!(peer_ingress(&missing, &properties).is_err());
            let mut builder = Message::builder().body(message.body().clone());
            for h in message.headers().records() {
                builder = builder.append_header(h.to_owned());
            }
            let duplicate = builder
                .append_header(generation.header().unwrap())
                .build()
                .unwrap();
            assert!(peer_ingress(&duplicate, &properties).is_err());
        }
    }

    #[test]
    fn native_sender_incarnations_are_not_taken_from_node_description_or_actor() {
        let local = NodeInstance {
            id: [1; 16],
            instance: 10,
        };
        let remote = NodeInstance {
            id: [2; 16],
            instance: 20,
        };
        let identity = PToP {
            from: remote.clone(),
            to: local.clone(),
            actor: 0,
            from_ring: String::new(),
            to_ring: String::new(),
            exact_instance: true,
        };
        let message = |identity: &PToP| {
            Message::builder()
                .append_header(
                    HeaderRecord::native(HeaderId::new(PTOP).unwrap(), identity).unwrap(),
                )
                .build()
                .unwrap()
        };
        let validate =
            |message: Message| validate_source(message.headers(), message.body(), &local, &remote);
        validate(message(&identity)).unwrap();
        let mut stale = identity.clone();
        stale.from.instance -= 1;
        assert!(validate(message(&stale)).is_err());
        stale = identity.clone();
        stale.to.instance -= 1;
        assert!(validate(message(&stale)).is_err());
        stale = identity.clone();
        stale.from.id = [3; 16];
        assert!(validate(message(&stale)).is_err());
        stale = identity.clone();
        stale.exact_instance = false;
        assert!(validate(message(&stale)).is_err());
        let duplicate = Message::builder()
            .append_header(HeaderRecord::native(HeaderId::new(PTOP).unwrap(), &identity).unwrap())
            .append_header(HeaderRecord::native(HeaderId::new(PTOP).unwrap(), &identity).unwrap())
            .build()
            .unwrap();
        assert!(validate(duplicate).is_err());
    }
}
