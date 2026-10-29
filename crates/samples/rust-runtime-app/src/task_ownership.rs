//! Application-owned listeners and requests, not descendants adopted by fube.
//! Their scheduler lifetime/progress belongs to the application. Shutdown must
//! stop and join them before that runtime is disposed; a stalled runtime cannot
//! make progress here.
use fube_runtime::{Cancellation, RuntimeError, RuntimeResult};
use std::{future::Future, pin::Pin, sync::Mutex, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

const REQUEST_CAPACITY: usize = 64;

#[derive(Default)]
pub(super) struct Listener {
    stopped: Cancellation,
    task: Mutex<Option<JoinHandle<()>>>,
    joining: tokio::sync::Mutex<()>,
}

impl Listener {
    pub(super) fn start<F, R>(
        &self,
        listener: TcpListener,
        timeout: Duration,
        request: F,
    ) -> RuntimeResult<()>
    where
        F: Fn(TcpStream) -> R + Send + Sync + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        let mut task = self.task.lock().unwrap();
        if self.stopped.is_cancelled() || task.is_some() {
            return Err(RuntimeError::Closed);
        }
        let stopped = self.stopped.clone();
        let request = std::sync::Arc::new(request);
        *task = Some(tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = stopped.cancelled() => break,
                    // Reap before accepting so completed handles cannot accumulate.
                    _ = requests.join_next(), if !requests.is_empty() => {},
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        if requests.len() == REQUEST_CAPACITY {
                            drop(stream);
                            continue;
                        }
                        let request = request.clone();
                        requests.spawn(async move {
                            let _ = tokio::time::timeout(timeout, async move {
                                request(stream).await;
                            }).await;
                        });
                    }
                }
            }
            drop(listener);
            requests.abort_all();
            while requests.join_next().await.is_some() {}
        }));
        Ok(())
    }

    pub(super) fn stop(&self) {
        self.stopped.cancel();
    }

    pub(super) async fn join(&self) -> RuntimeResult<()> {
        self.stop();
        let _joining = self.joining.lock().await;
        // Borrow rather than take: cancelling this await leaves the listener
        // handle retained, including while it joins its cancelled requests.
        std::future::poll_fn(|cx| {
            let mut task = self.task.lock().unwrap();
            let Some(handle) = task.as_mut() else {
                return std::task::Poll::Ready(Ok(()));
            };
            Pin::new(handle).poll(cx).map(|result| {
                task.take();
                result.map_err(|_| RuntimeError::InvalidInput("application listener task failed"))
            })
        })
        .await
    }
}

#[cfg(test)]
#[path = "task_ownership_tests.rs"]
mod tests;
