use crate::*;

/// Native semantic results, local admission failures and source-invariant failures
/// remain distinguishable. `Dependency` preserves an otherwise unmapped native error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectClosed,
    InvalidState,
    NotFound,
    HostAlreadyRegistered,
    HostNotRegistered,
    RuntimeAlreadyRegistered,
    RuntimeNotRegistered,
    OperationFailed,
    InvalidServiceType,
    TypeAlreadyRegistered,
    TypeAlreadyRegisteredToSameRuntime,
    TypeNotRegistered,
    TypeDisabled,
    DeploymentInProgress,
    RegistrationVersionMismatch,
    Dependency(i64),
    Capacity,
    InvalidInput,
    MissingModelOutcome,
    UnsupportedCapability,
    NativeInvariant(&'static str),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rollout {
    pub major: u32,
    pub minor: u32,
}
impl Default for Rollout {
    fn default() -> Self {
        Self { major: 1, minor: 0 }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionInstance {
    pub application: Rollout,
    pub package: Rollout,
    pub instance: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeContext {
    pub runtime_id: String,
    pub host_id: String,
    pub process_id: u32,
    pub package: PackageInstance,
    pub version: VersionInstance,
    pub code_package: String,
    pub package_instance_sequence: i64,
}
impl RuntimeContext {
    pub(crate) fn bytes(&self) -> usize {
        128 + self.runtime_id.len()
            + self.host_id.len()
            + self.package.package.bytes()
            + self.package.public_activation.len()
            + self.code_package.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub type_instance: TypeInstance,
    pub version: VersionInstance,
    pub host_id: String,
    pub process_id: u32,
    pub runtime_id: String,
    pub code_package: String,
    pub package_instance_sequence: i64,
}
impl Registration {
    /// ServiceTypeRegistrationHelper.h:8-23. Identity is selected by the producer,
    /// operational fields by its runtime context.
    pub fn from_context(id: TypeInstance, runtime: &RuntimeContext) -> Self {
        Self {
            type_instance: id,
            version: runtime.version.clone(),
            host_id: runtime.host_id.clone(),
            process_id: runtime.process_id,
            runtime_id: runtime.runtime_id.clone(),
            code_package: runtime.code_package.clone(),
            package_instance_sequence: runtime.package_instance_sequence,
        }
    }
}

/// Logical retained-payload budgets, not allocator/RSS promises. Drained owned
/// outputs belong to the caller, which must impose its own downstream budgets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub packages: usize,
    pub hosts: usize,
    pub runtimes: usize,
    pub types: usize,
    pub failures: usize,
    pub sources: usize,
    pub timers: usize,
    pub operations: usize,
    pub events_per_lane: usize,
    pub obligations: usize,
    pub state_bytes: usize,
    pub event_bytes_per_lane: usize,
    pub obligation_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            packages: 128,
            hosts: 64,
            runtimes: 128,
            types: 256,
            failures: 128,
            sources: 1024,
            timers: 256,
            operations: 128,
            events_per_lane: 256,
            obligations: 256,
            state_bytes: 1024 * 1024,
            event_bytes_per_lane: 1024 * 1024,
            obligation_bytes: 1024 * 1024,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<()> {
        if [
            self.packages,
            self.hosts,
            self.runtimes,
            self.types,
            self.failures,
            self.sources,
            self.timers,
            self.operations,
            self.events_per_lane,
            self.obligations,
            self.state_bytes,
            self.event_bytes_per_lane,
            self.obligation_bytes,
        ]
        .contains(&0)
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

/// Driver-supplied native signed 100ns observation; no wall-clock scheduler.
pub trait Clock {
    fn now_ticks(&self) -> i64;
}
#[derive(Debug, Clone, Copy)]
pub struct ManualClock(pub i64);
impl Clock for ManualClock {
    fn now_ticks(&self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerKind {
    HostFinish(String),
    DisableType {
        type_instance: TypeInstance,
        sequence: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer {
    pub id: u64,
    pub due_ticks: i64,
    pub kind: TimerKind,
}
impl Timer {
    pub(crate) fn bytes(&self) -> usize {
        64 + match &self.kind {
            TimerKind::HostFinish(id) => id.len(),
            TimerKind::DisableType { type_instance, .. } => type_instance.bytes(),
        }
    }
}

/// Cleanup success is not registration success. A secondary cleanup failure
/// never overwrites the original public result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorResult {
    pub result: Result<()>,
    pub cleanup: Option<Result<()>>,
}
