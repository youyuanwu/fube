//! Lifecycle envelopes and byte submission on one original nonowning Peer.
use super::{action, MAX_INPUT};
use fube_core::protocol::actor as actors;
use furpc::{
    header, ActorId, Body, EncodeBuf, Encoder, Error, Headers, OneWayClient, Peer, PeerSendService,
    Request, Result, SendKind, SendPolicy, Source, WriteReceipt,
};
use tokio::time::Instant;

#[derive(Clone)]
pub(super) struct LifecycleClient {
    client: OneWayClient<PeerSendService>,
}

impl LifecycleClient {
    pub(super) fn new(peer: Peer) -> Result<Self> {
        Ok(Self {
            client: OneWayClient::new(PeerSendService::new(peer))
                .max_encoded_body_bytes(MAX_INPUT)?,
        })
    }

    pub(super) fn send_policy() -> SendPolicy {
        SendPolicy::new(outbound)
    }

    pub(super) async fn send(
        &mut self,
        request: LifecycleRequest,
        deadline: Instant,
    ) -> Result<WriteReceipt> {
        self.client
            .send(request.0, PreparedBodyEncoder, deadline)
            .await
    }
}

pub(super) struct LifecycleRequest(Request<Body>);

#[derive(Clone, Copy)]
pub(super) enum Method {
    Open,
    Close,
    UpdateService,
    BuildIdleReplica,
    RemoveIdleReplica,
    UpdateConfiguration,
    UpdateEpochAndGetStatus,
    EndpointUpdatedReply,
}

impl Method {
    const ALL: [Self; 8] = [
        Self::Open,
        Self::Close,
        Self::UpdateService,
        Self::BuildIdleReplica,
        Self::RemoveIdleReplica,
        Self::UpdateConfiguration,
        Self::UpdateEpochAndGetStatus,
        Self::EndpointUpdatedReply,
    ];

    pub(super) fn select(action: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|method| method.action() == action)
    }

    fn action(self) -> &'static str {
        match self {
            Self::Open => "ReplicaOpen",
            Self::Close => "ReplicaClose",
            Self::UpdateService => "ProxyUpdateServiceDescription",
            Self::BuildIdleReplica => "ReplicatorBuildIdleReplica",
            Self::RemoveIdleReplica => "ReplicatorRemoveIdleReplica",
            Self::UpdateConfiguration => "UpdateConfiguration",
            Self::UpdateEpochAndGetStatus => "ReplicatorUpdateEpochAndGetStatus",
            Self::EndpointUpdatedReply => "ProxyReplicaEndpointUpdatedReply",
        }
    }

    pub(super) fn prepare(self, bytes: Vec<u8>) -> Result<LifecycleRequest> {
        Ok(LifecycleRequest(
            Request::new(Body::from_bytes(bytes)?)
                .append_header(header::actor(ActorId::new(actors::RA)?)?)?
                .append_header(header::action(self.action())?)?,
        ))
    }
}

fn outbound(kind: SendKind, headers: &Headers, _: &Source) -> Result<()> {
    let common = headers.effective();
    if kind != SendKind::OneWay
        || common.actor.get() != actors::RA
        || Method::select(&action(headers)?).is_none()
        || common.expects_reply
        || common.relates_to != furpc::MessageId::default()
    {
        return Err(Error::InvalidInput("invalid Hosting lifecycle send"));
    }
    Ok(())
}

struct PreparedBodyEncoder;

impl Encoder for PreparedBodyEncoder {
    type Item = Body;

    fn encode(
        &mut self,
        body: Body,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), furpc::CodecError> {
        dst.append_body(body)
    }
}
