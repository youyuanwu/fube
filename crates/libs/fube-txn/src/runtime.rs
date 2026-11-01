//! Supported ordinary volatile service composition over one lower replicator.
//! Native construction/lifecycle authority: TransactionalReplicator.cpp:257–459
//! and StateManager.cpp:103–179 at the crate's pinned source. This module is a
//! FUBE ADAPTATION for SDK construction, not a second control or logging engine.

use crate::{
    kernel::{
        maintenance::LogSettings,
        registry::RegistrySettings,
        runtime::{RuntimeConfig, VolatileRuntime},
    },
    Error, IDataLossHandler, IStateProvider2Factory, IStatefulServicePartition,
    ITransactionalReplicator, Result,
};
use fube_data::{extensions::ReplicationEndpointOptions, FabricReplicator, Settings};
use fube_runtime::extensions::NativeRuntimeIntegration;
use std::{path::PathBuf, sync::Arc};

/// Native MB-based checkpoint/truncation policy for the volatile log.
///
/// Defaults retain native 50 MiB checkpoint/minimum-log thresholds and factors
/// 2/4. The finite Rust log may fill first: no checkpoint or preferential
/// admission progress is promised at allocation exhaustion. For a small offline
/// profile, 1 MiB thresholds with an 8 MiB log exercise actual maintenance.
#[derive(Clone, Debug)]
pub struct VolatileLogSettings {
    /// Checkpoint interval in MiB (1 through 1024).
    pub checkpoint_mb: u64,
    /// Minimum retained log size in MiB (1 through 1024).
    pub min_log_mb: u64,
    /// Native truncation multiplier; at least 2.
    pub truncate_factor: u64,
    /// Native writer throttle multiplier; greater than truncate_factor.
    pub throttle_factor: u64,
}
impl Default for VolatileLogSettings {
    fn default() -> Self {
        let settings = LogSettings::default();
        Self {
            checkpoint_mb: settings.checkpoint_mb,
            min_log_mb: settings.min_log_mb,
            truncate_factor: settings.truncate_factor,
            throttle_factor: settings.throttle_factor,
        }
    }
}

/// Explicit Rust allocation/profile settings, not native allocator equivalence.
#[derive(Clone, Debug)]
pub struct VolatileRuntimeSettings {
    /// Ordinary shared log allocation budget, 512 KiB through 16 MiB.
    ///
    /// Default: 8 MiB. Logical and physical records share it; there are no
    /// protected End, Barrier, epoch or maintenance pools.
    pub memory_bytes: usize,
    /// Provider Initialize work-folder root; this runtime does not persist a log.
    ///
    /// The host must provide a suitable per-replica location for its provider.
    pub work_folder: PathBuf,
    /// Native maintenance settings, independent of the finite allocation budget.
    pub log: VolatileLogSettings,
}
impl Default for VolatileRuntimeSettings {
    fn default() -> Self {
        Self {
            memory_bytes: 8 * 1024 * 1024,
            work_folder: PathBuf::new(),
            log: VolatileLogSettings::default(),
        }
    }
}

/// Constructible supported A1 service owner and its single FabricReplicator.
///
/// Construct during the stateful replica's `open` callback with the runtime-
/// issued native binding. Retain this owner in that service replica through
/// native Close/Abort; return a clone of [`Self::fabric_replicator`] as the
/// canonical `IPrimaryReplicator`. Native-issued lower Open/role/Close/Abort
/// automatically call the existing upper lifecycle exactly once.
///
/// [`Self::transactional_replicator`] exposes ordinary transactions and primary
/// Add/GetOrAdd/Get/Remove/enumeration using the supplied provider factory. The
/// initial registry is empty; normal null-context Full Build constructs copied
/// providers through that same factory. No test-only provider or Host is needed
/// by an SDK consumer. Provider payload/rollback semantics remain its obligation.
///
/// Direct synchronous second/later `Transaction::add_operation` requires the
/// canonical fube execution entry (use the lower execution runtime's
/// `try_reserve().run_owned(...)` when outside it). Its return observes actual
/// admission, not just enqueue. Async registry methods retain/resume that entry
/// internally. Observer Drop is not rollback; explicit transaction Dispose
/// releases native retained registry locks, including after read-only commit.
///
/// Atomic/redo-only execution, managed/false-progress formats, persisted recovery,
/// restore, backup and change-handler products are explicitly unsupported.
/// This is source-derived offline integration, not executed native-binary,
/// mixed-native or collection-product qualification.
#[derive(Clone)]
pub struct VolatileTransactionalReplicator {
    runtime: Arc<VolatileRuntime>,
}
impl VolatileTransactionalReplicator {
    /// Create the callback-ready composition; application readiness awaits native
    /// Open/bootstrap/role or copy completion and native partition access.
    ///
    /// `native` must come from the registered service factory's captured native
    /// integration, not manufactured membership. `settings` are the existing
    /// lower finite queue/capture settings. The factory is mandatory, including
    /// on empty replicas. Invalid upper settings reject before lower creation.
    /// An empty work-folder root is invalid; the runtime does not create directories.
    pub fn create(
        partition: Arc<dyn IStatefulServicePartition>,
        native: NativeRuntimeIntegration,
        endpoint: ReplicationEndpointOptions,
        settings: Settings,
        factory: Arc<dyn IStateProvider2Factory>,
        data_loss: Arc<dyn IDataLossHandler>,
        config: VolatileRuntimeSettings,
    ) -> Result<Self> {
        let log_settings = LogSettings {
            checkpoint_mb: config.log.checkpoint_mb,
            min_log_mb: config.log.min_log_mb,
            truncate_factor: config.log.truncate_factor,
            throttle_factor: config.log.throttle_factor,
        };
        log_settings.validate()?;
        if !(512 * 1024..=16 * 1024 * 1024).contains(&config.memory_bytes)
            || config.work_folder.as_os_str().is_empty()
        {
            return Err(Error::InvalidInput(
                "invalid volatile runtime allocation/work folder",
            ));
        }
        Ok(Self {
            runtime: VolatileRuntime::create(
                partition,
                native,
                endpoint,
                settings,
                RuntimeConfig {
                    log_settings,
                    registry: Some(RegistrySettings {
                        factory,
                        metadata: vec![],
                    }),
                    providers: vec![],
                    work_folder: config.work_folder,
                    memory_bytes: config.memory_bytes,
                    data_loss,
                },
            )?,
        })
    }

    /// Obtain the existing transaction/provider registry facade.
    ///
    /// Retain the service owner as documented above; this handle alone does not
    /// replace its native lifecycle ownership or grant partition access.
    pub fn transactional_replicator(&self) -> Arc<dyn ITransactionalReplicator> {
        self.runtime.owner.clone()
    }

    /// Obtain the ONE canonical lower control/data object and execution runtime.
    ///
    /// Service Open returns `Box::new((*owner.fabric_replicator()).clone())`.
    /// Do not also open/change-role/close the upper owner: lower root and bound
    /// native controls already forward those callbacks. Do not use raw lower
    /// replicate to bypass the transactional record protocol.
    pub fn fabric_replicator(&self) -> Arc<FabricReplicator> {
        self.runtime.replicator()
    }
}
