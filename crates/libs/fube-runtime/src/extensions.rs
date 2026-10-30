//! Explicit execution-profile capabilities, not SDK trait members.
/// Fube's explicit full/primary epoch-domain conversion. It is not a member of
/// the SDK Epoch contract; invalid and valid zero remain distinct.
pub trait EpochDomains {
    fn primary(self) -> crate::Epoch;
}
impl EpochDomains for crate::Epoch {
    fn primary(self) -> Self {
        if self == Self::INVALID {
            return self;
        }
        Self {
            data_loss_number: self.data_loss_number,
            configuration_number: self.configuration_number & !0xffff_ffff,
        }
    }
}
/// Explicit owned activation input for embedding and model hosts. This follows
/// the same native IPC registration, artifact validation, lease and supervision
/// path as `Runtime::attach`; the input itself grants no native authority.
pub async fn attach_runtime(activation: crate::ActivationContext) -> crate::Result<crate::Runtime> {
    crate::Runtime::attach_context(Some(activation)).await
}
pub use crate::api::ServiceIdentity;
pub use crate::execution::{
    CallLane, CallOrigin, CleanupPermit, CommandCleanup, ConstructionReservation, DeferredCleanup,
    DisposalBatch, DisposalCapture, DisposalReceipt, DisposalTracker, ExecutionRuntime,
    ManagedObject, ObjectPublication, ObjectQuota, ObjectReservation, OwnedCall, OwnedCallPermit,
    OwnedDisposition, TerminalPermit, TerminalRecord,
};
pub use crate::instance_integration::{
    InstanceIntegrationProvider, InstanceRuntimeIntegration, ReportCompletion, ReportingObservation,
};
pub use crate::reporting::{FaultDelivery, HealthReceipt};
pub use crate::stateful::binding::{
    NativeControlBinding, NativeEpoch, NativeIntegrationProvider, NativeInvocation,
    NativeInvocationKind, NativeProgressSource, NativeReplica, NativeReplicaSet,
    NativeRuntimeIntegration, ReplicaBinding,
};
pub use crate::stateful_api::{
    CatchUpResult, ConfigurationStage, NativeReplicaDescriptor, NativeReplicaSetConfiguration,
    ReplicaDiagnostics, ReplicaIdentity, ReplicaLifecycle, ReplicaRemoval, ReplicationProgress,
    StatefulCallbackGuard,
};
