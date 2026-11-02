//! Observer-independent common lifecycle continuation on the existing owner.
use crate::Result;
use fube_runtime::{
    extensions::{CommandCleanup, ExecutionRuntime},
    ApiFuture,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
struct Scoped<T> {
    work: ApiFuture<'static, T>,
    cleanup: Option<CommandCleanup>,
}
impl<T> Future for Scoped<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let _scope = this.cleanup.as_ref().map(CommandCleanup::enter);
        this.work.as_mut().poll(cx)
    }
}
pub(crate) fn own<T: Clone + Send + Sync + 'static>(
    execution: &ExecutionRuntime,
    work: ApiFuture<'static, T>,
) -> ApiFuture<'static, T> {
    let mut scoped = Box::pin(Scoped {
        work,
        cleanup: execution.command_cleanup(),
    });
    if let Poll::Ready(result) = scoped
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        return Box::pin(async move { result });
    }
    let (result, mut observer) = tokio::sync::watch::channel(None);
    let result = Arc::new(result);
    drop(tokio::runtime::Handle::current().spawn(async move {
        result.send_replace(Some(scoped.await));
    }));
    Box::pin(async move {
        loop {
            if let Some(result) = observer.borrow_and_update().clone() {
                return result;
            }
            observer.changed().await.map_err(|_| crate::Error::Closed)?;
        }
    })
}
