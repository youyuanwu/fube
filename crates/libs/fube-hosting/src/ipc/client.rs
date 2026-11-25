//! Typed lifecycle sends on one original nonowning Peer and Hosting authority.
use super::{action, session_live, Entry, MAX_INPUT};
use fube_core::protocol::actor as actors;
use furpc::{
    header, ActorId, Body, Encoder, Error, Headers, OneWayClient, Peer, PeerSendService, Request,
    Result, SendKind, SendPolicy, Source, WriteReceipt,
};
use std::{
    sync::{atomic::Ordering, Weak},
    task::{Context, Poll},
};
use tokio::time::Instant;
use tower_service::Service;

#[derive(Clone)]
pub(super) struct LifecycleClient {
    client: OneWayClient<LifecycleService>,
}

impl LifecycleClient {
    pub(super) fn new(peer: Peer, entry: Weak<Entry>) -> Result<Self> {
        Ok(Self {
            client: OneWayClient::new(LifecycleService {
                transport: PeerSendService::new(peer),
                entry,
            })
            .max_encoded_body_bytes(MAX_INPUT)?,
        })
    }

    pub(super) fn send_policy() -> SendPolicy {
        SendPolicy::new(outbound)
    }

    pub(super) async fn send<E>(
        &mut self,
        request: LifecycleRequest<E::Item>,
        encoder: E,
        deadline: Instant,
    ) -> Result<WriteReceipt>
    where
        E: Encoder + Send,
        E::Item: Send,
    {
        self.client.send(request.0, encoder, deadline).await
    }
}

pub(super) struct LifecycleRequest<T>(Request<T>);

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

    pub(super) fn prepare<T>(self, body: T) -> Result<LifecycleRequest<T>> {
        Ok(LifecycleRequest(
            Request::new(body)
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

#[derive(Clone)]
struct LifecycleService {
    transport: PeerSendService,
    entry: Weak<Entry>,
}

impl LifecycleService {
    fn check(&self) -> Result<()> {
        let entry = self.entry.upgrade().ok_or(Error::Closed)?;
        if !session_live(&entry) || !entry.ready.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        Ok(())
    }
}

impl Service<Request<Body>> for LifecycleService {
    type Response = WriteReceipt;
    type Error = Error;
    type Future = <PeerSendService as Service<Request<Body>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.check() {
            return Poll::Ready(Err(error));
        }
        self.transport.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if let Err(error) = self.check() {
            return Box::pin(async move { Err(error) });
        }
        self.transport.call(request)
    }
}
