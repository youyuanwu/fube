//! Hosting method dispatch and opaque deferred invocations on the original runner.
use super::{action, rpc, MAX_INPUT};
use fube_core::protocol::actor as actors;
use furpc::{
    header, ActorId, Body, Error, Headers, IngressLimits, IngressOptions, IngressResponse,
    IngressService, Request, RequestPolicy, Result, ServiceBinding, Source,
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tokio::sync::{mpsc, oneshot, watch};
use tower_service::Service;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(super) const HANDLERS: usize = 4;

pub(super) struct IpcMessage {
    pub headers: Headers,
    pub body: Vec<u8>,
}

impl IpcMessage {
    #[cfg(feature = "qualification")]
    pub(super) fn qualification(action: &str, body: Vec<u8>) -> Result<Self> {
        if !matches!(action, "ReplicaOpenReply" | "ReplicaCloseReply") || body.len() > MAX_INPUT {
            return Err(Error::Protocol("invalid qualification reply"));
        }
        let mut headers = Headers::new();
        headers.append(header::actor(ActorId::new(actors::RA)?)?)?;
        headers.append(header::action(action)?)?;
        Ok(Self { headers, body })
    }
}

pub(super) struct Incoming {
    request: Request<Body>,
    action: Result<String>,
    received_at: Instant,
    deadline: tokio::time::Instant,
    response: oneshot::Sender<IngressResponse>,
}

impl Incoming {
    pub(super) fn headers(&self) -> &Headers {
        self.request.headers()
    }

    pub(super) fn received_at(&self) -> Instant {
        self.received_at
    }

    pub(super) fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    pub(super) fn is_semantic(&self) -> bool {
        self.headers().effective().actor.get() == actors::RA
    }

    pub(super) fn validate_action(&self) -> Result<()> {
        self.action.as_ref().map(|_| ()).map_err(Clone::clone)
    }

    pub(super) fn route(self) -> Result<Call> {
        let action = self.action?;
        let actor = self.request.headers().effective().actor.get();
        if actor == actors::RA {
            if !matches!(
                action.as_str(),
                "ReplicaOpenReply"
                    | "ReplicaCloseReply"
                    | "ProxyUpdateServiceDescriptionReply"
                    | "ReplicatorBuildIdleReplicaReply"
                    | "ReplicatorRemoveIdleReplicaReply"
                    | "UpdateConfigurationReply"
                    | "ReplicatorUpdateEpochAndGetStatusReply"
                    | "ProxyReplicaEndpointUpdated"
                    | "RAPReportLoad"
            ) {
                return Err(Error::Protocol("unsupported semantic RAP input"));
            }
            Ok(Call::Semantic(SemanticInvocation {
                request: self.request,
                response: self.response,
            }))
        } else {
            Ok(Call::Rpc(RpcInvocation {
                method: rpc::Method::select(actor, &action)?,
                request: self.request,
                response: self.response,
            }))
        }
    }
}

pub(super) enum Call {
    Rpc(RpcInvocation),
    Semantic(SemanticInvocation),
}

pub(super) struct RpcInvocation {
    method: rpc::Method,
    request: Request<Body>,
    response: oneshot::Sender<IngressResponse>,
}

impl RpcInvocation {
    pub(super) fn method(&self) -> rpc::Method {
        self.method
    }

    pub(super) async fn unary(self, handler: rpc::HostingRpc) -> Result<()> {
        if handler.method() != self.method {
            return Err(Error::Internal("Hosting RPC invocation method"));
        }
        let response = handler.unary(self.request).await?;
        self.response.send(response).map_err(|_| Error::Closed)
    }
}

pub(super) struct SemanticInvocation {
    request: Request<Body>,
    response: oneshot::Sender<IngressResponse>,
}

impl SemanticInvocation {
    pub(super) fn enqueue(self, queue: &mut VecDeque<IpcMessage>) -> Result<()> {
        let (parts, body) = self.request.into_parts();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(body.len())
            .map_err(|_| Error::ResourceLimit("Hosting semantic body"))?;
        for segment in body.segments() {
            bytes.extend_from_slice(segment);
        }
        queue.push_back(IpcMessage {
            headers: parts.headers,
            body: bytes,
        });
        // Completion means domain enqueue, not execution or a semantic ACK.
        self.response
            .send(IngressResponse::NoReply)
            .map_err(|_| Error::Closed)
    }
}

#[derive(Clone)]
pub(super) struct HostingServer {
    send: mpsc::Sender<Incoming>,
    failure: watch::Sender<Option<Error>>,
}

impl HostingServer {
    pub(super) fn new(send: mpsc::Sender<Incoming>, failure: watch::Sender<Option<Error>>) -> Self {
        Self { send, failure }
    }

    pub(super) fn ingress(self) -> Result<IngressService<Self>> {
        // Unknown methods reach the domain owner and retire its exact session.
        IngressService::new(
            self,
            [
                actors::APPLICATION_HOST_MANAGER,
                actors::FABRIC_RUNTIME_MANAGER,
                actors::RA,
                actors::HOSTING,
            ]
            .into_iter()
            .map(|actor| {
                let limits = IngressLimits {
                    max_body_bytes: MAX_INPUT,
                };
                ServiceBinding::actor(
                    ActorId::new(actor)?,
                    if actor == actors::RA {
                        IngressOptions::one_way(allow, limits)
                    } else {
                        IngressOptions::requests(RequestPolicy::new(allow, response), limits)
                    },
                )
            })
            .collect::<Result<_>>()?,
        )
    }
}

impl Service<Request<Body>> for HostingServer {
    type Response = IngressResponse;
    type Error = Error;
    type Future = BoxFuture<Result<IngressResponse>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let send = self.send.clone();
        let failure = self.failure.clone();
        Box::pin(async move {
            let permit = match send.try_reserve_owned() {
                Ok(permit) => permit,
                // Overload rejects this call, not the admitted Hosting session.
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return Err(Error::ResourceLimit("Hosting ingress handoff"));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    failure.send_replace(Some(Error::Closed));
                    return Err(Error::Closed);
                }
            };
            let mut guard = CallGuard {
                failure,
                completed: false,
            };
            let result = async {
                let deadline = request
                    .context()
                    .ok_or(Error::Internal("Hosting ingress context"))?
                    .deadline();
                let received_at = Instant::now();
                // Surface header errors at the executor's original validation point.
                let action = action(request.headers());
                let (response, wait) = oneshot::channel();
                permit.send(Incoming {
                    request,
                    action,
                    received_at,
                    deadline,
                    response,
                });
                wait.await.map_err(|_| Error::Closed)
            }
            .await;
            if let Err(error) = &result {
                guard.failure.send_replace(Some(error.clone()));
            }
            guard.completed = true;
            result
        })
    }
}

fn allow(_: &Headers, _: &Source) -> Result<()> {
    Ok(())
}

fn response(headers: &Headers, _: &Source) -> Result<()> {
    let common = headers.effective();
    if common.actor.get() != actors::EMPTY
        || !common.action().is_empty()
        || common.expects_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(Error::Protocol("invalid Hosting response"));
    }
    Ok(())
}

struct CallGuard {
    failure: watch::Sender<Option<Error>>,
    completed: bool,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.failure
                .send_replace(Some(Error::Timeout("Hosting ingress handoff")));
        }
    }
}
