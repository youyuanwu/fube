use crate::{
    connection::{SendAttempt, SendReceipt},
    transport::BoxFuture,
    Body, Error, Message, NotificationPolicy, Peer, Request, Response, Result,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::time::{timeout_at, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Retracted,
    Committed,
    Settled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptDisposition {
    Uncommitted,
    Retracted,
    Committed,
    LocallyWritten,
    Retired { committed: bool },
}

struct State {
    status: AttemptDisposition,
    token: Option<SendAttempt>,
}
#[derive(Clone)]
pub struct Attempt(Arc<Mutex<State>>);
impl Attempt {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(State {
            status: AttemptDisposition::Uncommitted,
            token: None,
        })))
    }
    fn start<T>(
        &self,
        submit: impl FnOnce() -> Result<(SendReceipt, T)>,
    ) -> Result<(SendReceipt, T)> {
        let mut state = self.0.lock().unwrap();
        if state.status == AttemptDisposition::Retracted {
            return Err(Error::Cancelled);
        }
        match submit() {
            Ok((receipt, value)) => {
                state.token = Some(receipt.attempt());
                Ok((receipt, value))
            }
            Err(error) => {
                state.status = AttemptDisposition::Retired { committed: false };
                Err(error)
            }
        }
    }
    pub fn disposition(&self) -> AttemptDisposition {
        let state = self.0.lock().unwrap();
        state
            .token
            .as_ref()
            .map_or(state.status, SendAttempt::disposition)
    }
    pub fn cancel_uncommitted(&self) -> CancelOutcome {
        let mut state = self.0.lock().unwrap();
        if let Some(token) = &state.token {
            if token.cancel() {
                return CancelOutcome::Retracted;
            }
            return match token.disposition() {
                AttemptDisposition::Committed => CancelOutcome::Committed,
                _ => CancelOutcome::Settled,
            };
        }
        match state.status {
            AttemptDisposition::Uncommitted | AttemptDisposition::Retracted => {
                state.status = AttemptDisposition::Retracted;
                CancelOutcome::Retracted
            }
            _ => CancelOutcome::Settled,
        }
    }
}

/// Complete local socket acceptance, not remote delivery or a semantic ACK.
#[derive(Debug, Clone, Copy)]
pub struct WriteReceipt {
    written_at: Instant,
}
impl WriteReceipt {
    pub fn written_at(&self) -> Instant {
        self.written_at
    }
}

/// Inert until polled. Neither the operation nor its attempt owns a connection.
pub struct Operation<T> {
    future: BoxFuture<Result<T>>,
    attempt: Attempt,
}
impl<T> Operation<T> {
    pub fn attempt(&self) -> Attempt {
        self.attempt.clone()
    }
}
impl<T> Future for Operation<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl<T> Drop for Operation<T> {
    fn drop(&mut self) {
        self.attempt.cancel_uncommitted();
    }
}
impl Operation<WriteReceipt> {
    pub(crate) fn send(peer: Peer, message: Message, deadline: Instant) -> Self {
        let attempt = Attempt::new();
        let status = attempt.clone();
        let future = Box::pin(async move {
            let (receipt, ()) =
                status.start(|| peer.submit(message, deadline).map(|receipt| (receipt, ())))?;
            let token = receipt.attempt();
            timeout_at(deadline, receipt)
                .await
                .map_err(|_| Error::Timeout("send"))?
                .map_err(Error::from)?;
            let written_at = token
                .settled_at()
                .ok_or(Error::Internal("written send lacks settlement time"))?;
            if written_at > deadline {
                return Err(Error::Timeout("send"));
            }

            Ok(WriteReceipt { written_at })
        });
        Self { future, attempt }
    }
}

impl Operation<Response<Body>> {
    pub(crate) fn request(
        peer: Peer,
        request: Request<Body>,
        deadline: Instant,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        notification: Option<NotificationPolicy>,
    ) -> Self {
        let deadline = request
            .deadline()
            .map_or(deadline, |local| local.min(deadline));
        let attempt = Attempt::new();
        let status = attempt.clone();
        let future = Box::pin(async move {
            let (mut receipt, registration) = status.start(|| {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout("RPC"));
                }
                peer.start_request(request, deadline, slot, notification)
            })?;
            let pending = registration.pending.clone();
            let mut written = false;
            let result = timeout_at(deadline, async {
                loop {
                    tokio::select! {
                        biased;
                        response = pending.wait(deadline) => return response,
                        result = &mut receipt, if !written => {
                            result.map_err(Error::from)?;
                            written = true;
                        }
                    }
                }
            })
            .await
            .map_err(|_| Error::Timeout("RPC"))?;
            drop(registration);
            result
        });
        Self { future, attempt }
    }
}
