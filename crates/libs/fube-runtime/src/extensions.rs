//! Explicit execution-profile capabilities, not SDK trait members.
pub use crate::execution::{
    CallLane, CallOrigin, CleanupPermit, CommandCleanup, ConstructionReservation, DeferredCleanup,
    DisposalBatch, DisposalCapture, DisposalReceipt, DisposalTracker, ExecutionRuntime,
    ManagedObject, ObjectPublication, ObjectQuota, ObjectReservation, OwnedCall, OwnedCallPermit,
    OwnedDisposition, TerminalPermit, TerminalRecord,
};
pub use crate::stateful::binding::{
    NativeControlBinding, NativeEpoch, NativeInvocation, NativeInvocationKind, NativeReplica,
    NativeReplicaSet, NativeRuntimeIntegration, ReplicaBinding,
};
