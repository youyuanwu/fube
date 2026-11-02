//! Runtime interpretation of opaque execution context and authority binding.
use crate::{extensions::NativeInvocation, lease::Authority, Error, Result};
pub use fube_core::hosting::{
    CallLane, CallOrigin, CleanupPermit, CommandCleanup, ConstructionReservation, DeferredCleanup,
    DisposalBatch, DisposalCapture, DisposalReceipt, DisposalTracker, ExecutionRuntime,
    ManagedObject, ObjectPublication, ObjectQuota, ObjectReservation, OwnedCall, OwnedCallPermit,
    OwnedDisposition, PublicationBatch, TerminalPermit, TerminalRecord,
};
use std::sync::Arc;

// Only the runtime retains the backend needed to install this private envelope.
// Borrowed execution handles cannot add arbitrary context destruction obligations.
struct RuntimeInvocation(NativeInvocation);

pub(crate) fn new(
    authority: Arc<Authority>,
    application: tokio::runtime::Handle,
) -> ExecutionRuntime {
    ExecutionRuntime::with_authority(authority, application)
}

#[cfg(test)]
pub(crate) fn current_invocation() -> Option<NativeInvocation> {
    ExecutionRuntime::current_host_context::<RuntimeInvocation>().map(|context| context.0.clone())
}

pub(crate) fn capturable_invocation() -> Option<NativeInvocation> {
    ExecutionRuntime::capturable_host_context::<RuntimeInvocation>()
        .filter(|context| context.0.in_own_execution())
        .map(|context| context.0.clone())
}

pub(crate) fn invocation_scope(invocation: NativeInvocation) -> Result<impl Drop> {
    invocation.validate()?;
    if !invocation.in_own_execution() {
        return Err(Error::InvalidInput(
            "native invocation execution owner mismatch",
        ));
    }
    let execution = invocation.execution();
    let authority: Arc<dyn fube_core::hosting::ExecutionAuthority> = invocation.authority();
    execution.host_context_scope(&authority, Arc::new(RuntimeInvocation(invocation)))
}

pub(crate) fn blocking_deadline_for(authority: &Arc<Authority>) -> Result<std::time::Instant> {
    let authority: Arc<dyn fube_core::hosting::ExecutionAuthority> = authority.clone();
    ExecutionRuntime::blocking_deadline_for(&authority)
}

pub(crate) async fn shutdown(
    execution: &ExecutionRuntime,
    authority: &Arc<Authority>,
) -> Result<()> {
    let authority: Arc<dyn fube_core::hosting::ExecutionAuthority> = authority.clone();
    execution.shutdown_with_authority(&authority).await
}

#[cfg(test)]
#[path = "execution_owned_tests.rs"]
mod owned_tests;
#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;
