use std::{
    future::{ready, Future, Ready},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use furpc::{Reply, Request};
use tower_service::Service;

use crate::{
    extensions::ExecutionRuntime, ipc_client::IpcClient, lease::Authority,
    package_context::Updater, wire, Error, Result,
};

pub(crate) struct HostControl {
    pub authority: Arc<Authority>,
    pub connection: IpcClient,
    pub execution: ExecutionRuntime,
    pub updater: Arc<Updater>,
    pub received_at: Instant,
}

impl Service<Request<()>> for HostControl {
    type Response = Reply<wire::ErrorReply>;
    type Error = Error;
    type Future = Ready<Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Request<()>) -> Self::Future {
        self.authority.retire();
        ready(Ok(Reply::new(wire::ErrorReply { error: 0 })))
    }
}

impl Service<Request<wire::UpdateContext>> for HostControl {
    type Response = Reply<wire::ErrorReply>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<wire::UpdateContext>) -> Self::Future {
        let authority = self.authority.clone();
        let connection = self.connection.clone();
        let execution = self.execution.clone();
        let updater = self.updater.clone();
        let received_at = self.received_at;
        Box::pin(async move {
            let context_deadline = request
                .context()
                .ok_or(Error::Protocol("runtime ingress requires furpc context"))?
                .deadline()
                .into_std();
            let update = request.into_body();
            if update.timeout <= 0 {
                return Err(Error::Timeout);
            }
            let duration = Duration::from_nanos((update.timeout.min(300_000_000) as u64) * 100);
            let deadline = received_at
                .checked_add(duration)
                .ok_or(Error::Timeout)?
                .min(context_deadline);
            authority.check()?;
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let call_permit = execution.reserve(
                crate::extensions::CallLane::Native,
                crate::extensions::CallOrigin::default(),
                deadline.saturating_duration_since(Instant::now()),
                false,
            )?;
            let permit = updater.admit()?;
            let worker_authority = authority.clone();
            let mut work = call_permit.run_owned(move || {
                let _permit = permit;
                updater.apply(update, &worker_authority, deadline)
            });
            tokio::select! {
                result = &mut work => result?,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(Error::Timeout),
                _ = connection.closed() => return Err(Error::Closed),
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            authority.check()?;
            Ok(Reply::new(wire::ErrorReply { error: 0 }))
        })
    }
}
