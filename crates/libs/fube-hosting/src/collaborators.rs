//! Explicit H1 package premises. FabricRuntimeManager.cpp:296-428 and
//! ApplicationManager.cpp:1079-1093. Real activation/usage/deactivation is H2-H4.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationOutcome {
    Synchronous(Result<()>),
    Pending,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeactivationOutcome {
    /// Underlying Deactivator success: whether a deactivation was scheduled.
    /// false also covers zero usage with infinite applicable grace.
    Success { scheduled: bool },
    /// ApplicationManager translates every scheduling error to hasReplica=false.
    Failure(Error),
}
#[derive(Debug, Clone, Default)]
pub struct LookupModel {
    /// Controlled GUID assignment only when exclusive GetOrAdd lacks a mapping.
    /// This is identity-model data, not exclusive execution admission.
    pub public_activation_assignment: Option<String>,
    pub activation: Option<ActivationOutcome>,
    pub deactivation: Option<DeactivationOutcome>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationRequest {
    pub type_instance: TypeInstance,
    pub requested_version: VersionInstance,
    pub application_name: String,
    pub sequence: u64,
}
impl ActivationRequest {
    pub(crate) fn bytes(&self) -> usize {
        128 + self.type_instance.bytes() + self.application_name.len()
    }
}
pub(crate) fn model_error_bytes(error: Option<&Error>) -> usize {
    32 + match error {
        Some(Error::NativeInvariant(detail)) => detail.len(),
        _ => 0,
    }
}
impl ActivationOutcome {
    pub(crate) fn bytes(&self) -> usize {
        match self {
            Self::Synchronous(result) => model_error_bytes(result.as_ref().err()),
            Self::Pending => 32,
        }
    }
}
impl DeactivationOutcome {
    pub(crate) fn bytes(&self) -> usize {
        match self {
            Self::Failure(error) => model_error_bytes(Some(error)),
            Self::Success { .. } => 32,
        }
    }
    pub(crate) fn has_replica(&self) -> bool {
        matches!(self, Self::Success { scheduled: false })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivationHandle(pub(crate) u64);
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingActivation {
    pub id: u64,
    pub request: ActivationRequest,
}
impl Hosting {
    pub fn pending_activation_count(&self) -> usize {
        self.state.pending_activations.len()
    }
    /// The captured external operation remains owned until a controlled End
    /// observation. Late completion consumes its result, never rewrites lookup,
    /// resurrects a type or establishes deployment/serving success.
    pub fn complete_activation(
        &mut self,
        handle: ActivationHandle,
        result: Result<()>,
    ) -> Result<()> {
        self.change(|s| {
            let i = s
                .pending_activations
                .iter()
                .position(|p| p.id == handle.0)
                .ok_or(Error::InvalidState)?;
            s.pending_activations.remove(i);
            s.obligations
                .push(Obligation::ActivationCompleted { handle, result });
            Ok(())
        })
    }
}
