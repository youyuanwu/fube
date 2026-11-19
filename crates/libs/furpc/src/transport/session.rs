use std::{
    future::{pending, poll_fn},
    sync::Arc,
    time::Duration,
};

use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::{
    sync::watch,
    time::{sleep_until, timeout_at, Instant},
};

use super::{
    io::{IoInfo, TransportIo},
    server::ReadyHook,
    BoxFuture, TransportLimits,
};
use crate::{
    connection::{
        driver::{ConnectionDriver, ConnectionEvent, DropReason},
        ConnectionError, LocalInstance, Role,
    },
    peer::Generation,
    service::{Dispatch, Registry},
    Error, Result,
};

pub(crate) struct Session<I: TransportIo> {
    driver: ConnectionDriver<I>,
    pub(crate) core: Arc<Generation>,
}

pub(crate) async fn establish<I: TransportIo>(
    io: I,
    info: IoInfo,
    role: Role,
    registry: Arc<Registry>,
    limits: &TransportLimits,
    drain_timeout: Duration,
    deadline: Instant,
) -> Result<Session<I>> {
    if registry.is_stopping() {
        return Err(Error::Closed);
    }
    let address = match role {
        Role::Initiating => String::new(),
        Role::Accepted => info
            .local_addr
            .map(|address| address.to_string())
            .unwrap_or_default(),
    };
    let local = LocalInstance {
        instance: u64::from(!address.is_empty()),
        address,
    };
    let (handle, mut driver) =
        ConnectionDriver::new(io, role, local, limits.connection(drain_timeout), deadline)
            .map_err(Error::from)?;
    let event = timeout_at(deadline, poll_fn(|cx| driver.poll_event(cx)))
        .await
        .map_err(|_| Error::Timeout("establishment"))?
        .map_err(Error::from)?;
    let Some(ConnectionEvent::Opened(remote)) = event else {
        return Err(Error::Internal(
            "establishment did not publish an open generation",
        ));
    };
    let core = Generation::new(handle, info, remote, registry)?;
    if core.registry.managed {
        driver.enable_rpc();
    }
    Ok(Session { driver, core })
}

impl<I: TransportIo> Session<I> {
    pub(crate) async fn run(
        mut self,
        hook: Option<ReadyHook>,
        stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let result = self.run_inner(hook, stop).await;
        if let Err(error) = &result {
            self.core
                .handle
                .abort(Arc::new(ConnectionError::Native(error.clone())));
        }
        self.core.rpc.close(
            result
                .as_ref()
                .err()
                .cloned()
                .or_else(|| self.core.handle.observer().error().map(Error::from))
                .unwrap_or(Error::Closed),
        );
        result
    }

    async fn run_inner(
        &mut self,
        hook: Option<ReadyHook>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let registry = self.core.registry.clone();
        let mut jobs: FuturesUnordered<BoxFuture<Result<()>>> = FuturesUnordered::new();
        let mut provisional: Option<Admission> = None;
        let mut hook = if registry.is_stopping() {
            None
        } else {
            hook.map(|hook| hook.run(self.core.peer(Some(hook.policy))))
        };
        let mut draining = false;
        let mut driver_done = false;
        let mut drain_hold = None;
        loop {
            if self.core.rpc.expire() {
                tokio::task::yield_now().await;
            }
            if draining && jobs.is_empty() && hook.is_none() && self.core.rpc.is_empty() {
                drain_hold.take();
            }
            if draining && driver_done && jobs.is_empty() && hook.is_none() {
                return Ok(());
            }
            let admission_deadline = provisional.as_ref().and_then(|admission| match admission {
                Admission::Ingress(dispatch) => Some(dispatch.deadline()),
                Admission::Reply { .. } => None,
            });
            let rpc_deadline = self.core.rpc.deadline();
            tokio::select! {
                biased;
                changed = stop.changed(), if !draining => {
                    if changed.is_err() { return Err(Error::Closed); }
                    if *stop.borrow_and_update() {
                        draining = true;
                        if matches!(provisional, Some(Admission::Ingress(_))) { provisional = None; }
                        drain_hold = Some(self.core.handle.hold_drain());
                        self.core.handle.drain().map_err(Error::from)?;
                    }
                }
                _ = wait_deadline(admission_deadline) => return Err(Error::Timeout("ingress body")),
                result = jobs.next(), if !jobs.is_empty() => {
                    if let Some(Err(error)) = result { registry.diagnose("ingress", &error); }
                }
                result = async { hook.as_mut().expect("enabled hook").await }, if hook.is_some() => {
                    hook = None;
                    result?;
                }
                event = poll_fn(|cx| self.driver.poll_event_with_admission(cx, &mut |headers, body_bytes| {
                    let id = headers.effective().relates_to;
                    provisional = Some(if id != crate::MessageId::default() {
                        let pending = self.core.rpc.lookup(id).ok_or(DropReason::UnmatchedReply)?;
                        Admission::Reply { id, pending }
                    } else { Admission::Ingress(registry.admit(&self.core, headers, body_bytes)?) });
                    Ok(())
                })), if !driver_done => {
                    match event.map_err(Error::from)? {
                        Some(ConnectionEvent::IngressAdmitted) => {}
                        Some(ConnectionEvent::ReadClosed) => {
                            self.core.rpc.close(Error::Closed);
                            if !draining {
                                draining = true;
                                drain_hold = Some(self.core.handle.hold_drain());
                            }
                        }
                        Some(ConnectionEvent::Reply(message)) => {
                            let Some(Admission::Reply { id, pending }) = provisional.take() else {
                                return Err(Error::Internal("reply lost pending correlation"));
                            };
                            self.core.rpc.complete(id, &pending, message, self.core.source.clone());
                        }
                        Some(ConnectionEvent::Message(message)) => {
                            let Some(Admission::Ingress(dispatch)) = provisional.take() else {
                                return Err(Error::Internal("verified ingress lost its admission"));
                            };
                            if !draining { jobs.push(dispatch.run(&self.core, message)); }
                        }
                        Some(ConnectionEvent::Discarded(reason)) => {
                            provisional = None;
                            let error = match reason {
                                DropReason::Capacity => Error::ResourceLimit("handler capacity"),
                                DropReason::Draining => Error::Closed,
                                DropReason::NoDestination => Error::Protocol("no ingress destination"),
                                DropReason::UnknownActor => Error::InvalidInput("unknown ingress Actor"),
                                DropReason::RouteMiss => Error::InvalidInput("unknown ingress Action"),
                                DropReason::PolicyRejected => Error::Protocol("ingress policy rejected"),
                                DropReason::UnmatchedReply => Error::Protocol("unmatched reply"),
                                DropReason::SpecialDuplexUnavailable => Error::Unsupported("special duplex"),
                            };
                            registry.diagnose("discard", &error);
                        }
                        Some(ConnectionEvent::InstanceUpdated(instance)) => {
                            tracing::trace!(generation = self.core.source.generation(), instance = instance.instance,
                                "native ListenInstance updated");
                        }
                        Some(ConnectionEvent::Opened(_)) => return Err(Error::Internal("generation opened twice")),
                        None if draining => { driver_done = true; }
                        None => return Ok(()),
                    }
                }
                _ = wait_deadline(rpc_deadline) => {}
                _ = self.core.rpc.changed() => {}
            }
        }
    }
}

enum Admission {
    Ingress(Dispatch),
    Reply {
        id: crate::MessageId,
        pending: Arc<crate::rpc::Pending>,
    },
}

async fn wait_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending().await,
    }
}
