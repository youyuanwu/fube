//! Runtime routes and schema-typed invocations over the original IPC channel.
//! Byte bodies stay private until the serialized executor admits their handlers.

use std::{
    future::{poll_fn, Future},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

use fube_core::protocol::actor as actors;
use furpc::{
    codec, header, Body, Codec, FubstCodec, Headers, IngressLimits, IngressResponse, MessageId,
    OneWayRouteOptions, Reply, Request, RequestPolicy, RequestRouteOptions, Router, Source,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::oneshot, time::timeout_at};
use tower_service::Service;

use crate::{
    ipc_client::{actor, native_error, Session},
    wire, Error, Result,
};

#[cfg(test)]
#[path = "ipc_server_tests.rs"]
mod tests;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(crate) type ControlHandoff = Arc<dyn Fn(ControlCall) -> Result<()> + Send + Sync>;
pub(crate) type RapHandoff = Arc<dyn Fn(RapCall) -> Result<()> + Send + Sync>;

pub(crate) struct RuntimeServer {
    controls: ControlHandoff,
    rap: RapHandoff,
}

impl RuntimeServer {
    pub(crate) fn new(controls: ControlHandoff, rap: RapHandoff) -> Self {
        Self { controls, rap }
    }

    pub(super) fn router(self, session: Weak<Session>) -> Result<Router> {
        let controls = self.controls.clone();
        let update = self.controls;
        let query = self.rap.clone();
        let mut router = Router::new()
            .request(
                actor(actors::APPLICATION_HOST)?,
                "InvalidateLeaseRequest",
                request_options(),
                RpcRoute::new(
                    session.clone(),
                    actors::APPLICATION_HOST,
                    None,
                    move |call| controls(ControlCall::InvalidateLease(call)),
                ),
            )
            .map_err(native_error)?
            .request(
                actor(actors::APPLICATION_HOST)?,
                "UpdateCodePackageContextRequest",
                request_options(),
                RpcRoute::new(
                    session.clone(),
                    actors::APPLICATION_HOST,
                    None,
                    move |call| update(ControlCall::UpdateContext(call)),
                ),
            )
            .map_err(native_error)?
            .request(
                actor(actors::RA)?,
                "RAPQuery",
                request_options(),
                RpcRoute::new(
                    session.clone(),
                    actors::RA,
                    Some("RAPQueryReply"),
                    move |call| query(RapCall::Query(call)),
                ),
            )
            .map_err(native_error)?;
        for kind in [
            RapAction::ReplicaOpen,
            RapAction::ReplicaClose,
            RapAction::UpdateService,
            RapAction::UpdateConfiguration,
            RapAction::BuildIdleReplica,
            RapAction::RemoveIdleReplica,
            RapAction::GetStatus,
            RapAction::UpdateEpochAndGetStatus,
            RapAction::CancelCatchupReplicaSet,
            RapAction::EndpointUpdatedReply,
        ] {
            let rap = self.rap.clone();
            let limits = IngressLimits::default();
            router = router
                .one_way(
                    actor(actors::RA)?,
                    kind.action(),
                    OneWayRouteOptions::new(incoming, limits.clone()),
                    OneWayRoute {
                        session: session.clone(),
                        max_body_bytes: limits.max_body_bytes,
                        handoff: Arc::new(move |call| rap(RapCall::OneWay(kind, call))),
                    },
                )
                .map_err(native_error)?;
        }
        Ok(router)
    }
}

pub(crate) enum ControlCall {
    InvalidateLease(RpcCall<(), wire::ErrorReply>),
    UpdateContext(RpcCall<wire::UpdateContext, wire::ErrorReply>),
}

impl ControlCall {
    pub(crate) fn is_update(&self) -> bool {
        matches!(self, Self::UpdateContext(_))
    }

    pub(crate) fn received_at(&self) -> std::time::Instant {
        match self {
            Self::InvalidateLease(call) => call.received_at,
            Self::UpdateContext(call) => call.received_at,
        }
    }
}

pub(crate) enum RapCall {
    OneWay(RapAction, OneWayCall<wire::ProxyRequest>),
    Query(RpcCall<wire::ProxyRequest, wire::QueryReply>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RapAction {
    ReplicaOpen,
    ReplicaClose,
    UpdateService,
    UpdateConfiguration,
    BuildIdleReplica,
    RemoveIdleReplica,
    GetStatus,
    UpdateEpochAndGetStatus,
    CancelCatchupReplicaSet,
    EndpointUpdatedReply,
}

impl RapAction {
    pub(crate) fn action(self) -> &'static str {
        match self {
            Self::ReplicaOpen => "ReplicaOpen",
            Self::ReplicaClose => "ReplicaClose",
            Self::UpdateService => "ProxyUpdateServiceDescription",
            Self::UpdateConfiguration => "UpdateConfiguration",
            Self::BuildIdleReplica => "ReplicatorBuildIdleReplica",
            Self::RemoveIdleReplica => "ReplicatorRemoveIdleReplica",
            Self::GetStatus => "ReplicatorGetStatus",
            Self::UpdateEpochAndGetStatus => "ReplicatorUpdateEpochAndGetStatus",
            Self::CancelCatchupReplicaSet => "CancelCatchupReplicaSet",
            Self::EndpointUpdatedReply => "ProxyReplicaEndpointUpdatedReply",
        }
    }
}

/// Deferred body adaptation, bound to the original runner's reply authority.
pub(crate) struct RpcCall<Q, R> {
    request: Request<Body>,
    received_at: std::time::Instant,
    response: oneshot::Sender<Result<IngressResponse>>,
    actor: i64,
    action: Option<&'static str>,
    schema: PhantomData<fn(Q) -> R>,
}

impl<Q, R> RpcCall<Q, R>
where
    Q: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
{
    pub(crate) async fn unary<S>(self, service: S) -> Result<()>
    where
        S: Service<Request<Q>, Response = Reply<R>, Error = Error> + Send + 'static,
        S::Future: Send + 'static,
    {
        let result = furpc::server::Rpc::new(FubstCodec::<R, Q>::default())
            .unary(
                ReplyHeaders {
                    service,
                    actor: self.actor,
                    action: self.action,
                },
                self.request,
            )
            .await
            .map_err(native_error);
        let error = result.as_ref().err().cloned();
        let sent = self.response.send(result).map_err(|_| Error::Closed);
        match error {
            Some(error) => Err(error),
            None => sent,
        }
    }
}

/// One-way serving completes at bounded enqueue, not at domain execution.
pub(crate) struct OneWayCall<Q> {
    request: Request<Body>,
    max_body_bytes: usize,
    schema: PhantomData<fn() -> Q>,
}

impl<Q: DeserializeOwned> OneWayCall<Q> {
    pub(crate) async fn one_way<S>(self, mut service: S) -> Result<()>
    where
        S: Service<Request<Q>, Response = (), Error = Error>,
    {
        // The runner's one-way scope has ended; do not borrow its expired
        // invocation authority via server::Rpc. Decode after domain admission.
        let (parts, body) = self.request.into_parts();
        let value = codec::decode(
            &mut FubstCodec::<(), Q>::default().decoder(),
            &body,
            self.max_body_bytes,
        )
        .map_err(|source| {
            native_error(furpc::Error::Codec {
                phase: "decode",
                source,
            })
        })?;
        drop(body);
        poll_fn(|cx| service.poll_ready(cx)).await?;
        service.call(Request::from_parts(parts, value)).await
    }
}

struct ReplyHeaders<S> {
    service: S,
    actor: i64,
    action: Option<&'static str>,
}

impl<Q, R, S> Service<Request<Q>> for ReplyHeaders<S>
where
    S: Service<Request<Q>, Response = Reply<R>, Error = Error>,
    S::Future: Send + 'static,
    R: Send + 'static,
{
    type Response = Reply<R>;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Q>) -> Self::Future {
        let future = self.service.call(request);
        let actor = self.actor;
        let action = self.action;
        Box::pin(async move {
            let mut reply = future
                .await?
                .append_header(header::actor(self::actor(actor)?).map_err(native_error)?)
                .map_err(native_error)?;
            if let Some(action) = action {
                reply = reply
                    .append_header(header::action(action).map_err(native_error)?)
                    .map_err(native_error)?;
            }
            Ok(reply)
        })
    }
}

type RpcHandoff<Q, R> = Arc<dyn Fn(RpcCall<Q, R>) -> Result<()> + Send + Sync>;

struct RpcRoute<Q, R> {
    session: Weak<Session>,
    handoff: RpcHandoff<Q, R>,
    actor: i64,
    action: Option<&'static str>,
}

impl<Q, R> Clone for RpcRoute<Q, R> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            handoff: self.handoff.clone(),
            actor: self.actor,
            action: self.action,
        }
    }
}

impl<Q, R> RpcRoute<Q, R> {
    fn new(
        session: Weak<Session>,
        actor: i64,
        action: Option<&'static str>,
        handoff: impl Fn(RpcCall<Q, R>) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            session,
            handoff: Arc::new(handoff),
            actor,
            action,
        }
    }
}

impl<Q: 'static, R: 'static> Service<Request<Body>> for RpcRoute<Q, R> {
    type Response = IngressResponse;
    type Error = Error;
    type Future = BoxFuture<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let session = self.session.clone();
        let handoff = self.handoff.clone();
        let actor = self.actor;
        let action = self.action;
        Box::pin(async move {
            let session = session.upgrade().ok_or(Error::Closed)?;
            let mut guard = IngressCall {
                session: session.clone(),
                armed: true,
            };
            let result = async {
                session.check()?;
                let deadline = request
                    .context()
                    .ok_or(Error::Protocol("runtime ingress requires furpc context"))?
                    .deadline();
                let (send, receive) = oneshot::channel();
                handoff(RpcCall {
                    request,
                    received_at: std::time::Instant::now(),
                    response: send,
                    actor,
                    action,
                    schema: PhantomData,
                })?;
                timeout_at(deadline, receive)
                    .await
                    .map_err(|_| Error::Timeout)?
                    .map_err(|_| Error::Closed)?
            }
            .await;
            guard.armed = false;
            if let Err(error) = &result {
                session.retire(error.clone());
            }
            result
        })
    }
}

#[derive(Clone)]
struct OneWayRoute {
    session: Weak<Session>,
    max_body_bytes: usize,
    handoff: Arc<dyn Fn(OneWayCall<wire::ProxyRequest>) -> Result<()> + Send + Sync>,
}

impl Service<Request<Body>> for OneWayRoute {
    type Response = IngressResponse;
    type Error = Error;
    type Future = std::future::Ready<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        std::future::ready((|| {
            let session = self.session.upgrade().ok_or(Error::Closed)?;
            let result = (|| {
                session.check()?;
                if request.context().is_none() {
                    return Err(Error::Protocol("runtime ingress requires furpc context"));
                }
                (self.handoff)(OneWayCall {
                    request,
                    max_body_bytes: self.max_body_bytes,
                    schema: PhantomData,
                })?;
                Ok(IngressResponse::NoReply)
            })();
            if let Err(error) = &result {
                session.retire(error.clone());
            }
            result
        })())
    }
}

struct IngressCall {
    session: Arc<Session>,
    armed: bool,
}

impl Drop for IngressCall {
    fn drop(&mut self) {
        if self.armed {
            self.session.retire(Error::Closed);
        }
    }
}

fn request_options() -> RequestRouteOptions {
    RequestRouteOptions::new(
        RequestPolicy::new(incoming, response),
        IngressLimits::default(),
    )
}

fn incoming(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if common.message_id == MessageId::default()
        || common.uncorrelated_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(furpc::Error::Protocol("invalid runtime IPC ingress"));
    }
    Ok(())
}

fn response(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if (common.actor.get() == actors::APPLICATION_HOST && common.action().is_empty())
        || (common.actor.get() == actors::RA
            && common
                .action()
                .iter()
                .copied()
                .eq("RAPQueryReply".encode_utf16()))
    {
        Ok(())
    } else {
        Err(furpc::Error::InvalidInput(
            "unauthorized runtime IPC response",
        ))
    }
}
