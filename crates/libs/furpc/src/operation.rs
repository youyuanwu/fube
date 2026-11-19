use crate::{connection::SendAttempt, transport::BoxFuture, Error, Message, Peer, Result};
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
        let attempt = Attempt(Arc::new(Mutex::new(State {
            status: AttemptDisposition::Uncommitted,
            token: None,
        })));
        let status = attempt.clone();
        let future = Box::pin(async move {
            let (receipt, token) = {
                let mut state = status.0.lock().unwrap();
                if state.status == AttemptDisposition::Retracted {
                    return Err(Error::Cancelled);
                }
                let receipt = match peer.submit(message, deadline) {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        state.status = AttemptDisposition::Retired { committed: false };
                        return Err(error);
                    }
                };
                let token = receipt.attempt();
                state.token = Some(token.clone());
                (receipt, token)
            };
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
