//! Experimental Rust-native application hosting for SF-activated Linux ExeHost.
//! Hosting registration is not, by itself, stateless runtime qualification.
//!
//! The supported SDK contracts use UTF-8/UUID/boxed-future Rust representations,
//! not COM/source interchangeability. Potentially waiting synchronous operations
//! require a reserved `extensions::ExecutionRuntime` entry; the sample reserves
//! before moving factories/captures. Registration waits for a real native reply,
//! and configuration/removal include required owner disposal, not enqueue ACKs.
//! Canonical names do not promise whole-code, mssf source or COM ABI parity.
//! See the repository's `docs/runtime/SfApiAlignment.md` for per-method signature,
//! value, completion and supported-capability classifications and provenance.
//!
//! # Scheduling a synchronous SDK call
//! Reserve before transferring captures. Await the owned observation, **not**
//! the synchronous SDK Result. This function type-checks without attaching SF:
//! ```
//! use fube_runtime::{IStatelessServiceFactory, Result, Runtime};
//! async fn register(runtime: Runtime, factory: Box<dyn IStatelessServiceFactory>)
//!     -> Result<Runtime>
//! {
//!     let permit = runtime.execution_runtime().try_reserve()?;
//!     let (runtime, registered) = permit.run_owned(move || {
//!         let registered = runtime.register_stateless_service_factory("MyType", factory);
//!         Ok((runtime, registered))
//!     }).await?;
//!     registered?;
//!     Ok(runtime)
//! }
//! ```
//!
//! Removed contracts and implementation modules are not compatibility paths:
//! ```compile_fail
//! use fube_runtime::StatelessFactory;
//! ```
//! ```compile_fail
//! use fube_runtime::StatefulServiceContext;
//! ```
//! ```compile_fail
//! use fube_runtime::ServiceFuture;
//! ```
//! ```compile_fail
//! use fube_runtime::Replicator;
//! ```
//! ```compile_fail
//! use fube_runtime::api::IStatelessServiceFactory;
//! ```
//! Native discovery is deliberately not an SDK trait member:
//! ```compile_fail
//! fn discover(control: &dyn fube_runtime::IPrimaryReplicator) {
//!     let _ = control.native_binding();
//! }
//! ```
//! ```compile_fail
//! fn discover(partition: &dyn fube_runtime::IStatefulServicePartition) {
//!     let _ = partition.replication_runtime();
//! }
//! ```
//! Epoch-domain masking is an explicit extension, not an old inherent SDK path:
//! ```compile_fail
//! let _ = fube_runtime::Epoch::ZERO.primary();
//! ```
//! Removed concrete/module-qualified reporting surfaces are not aliases:
//! ```compile_fail
//! use fube_runtime::{Partition, StatefulPartition, HealthReport, HealthSequence};
//! ```
//! ```compile_fail
//! use fube_runtime::stateful_api::StatefulFactory;
//! ```
//! Base health reports take only borrowed information, not send options:
//! ```compile_fail
//! fn old(p: &dyn fube_runtime::IStatelessServicePartition, h: &fube_runtime::HealthInformation) {
//!     p.report_instance_health(h, None);
//! }
//! ```
//! Reports are completed synchronous Results, not futures:
//! ```compile_fail
//! async fn old(p: &dyn fube_runtime::IStatefulServicePartition, h: &fube_runtime::HealthInformation) {
//!     p.report_replica_health(h).await;
//! }
//! ```
//! Positive health/health2 synchrony and argument-role witness (uncalled
//! functions; actual waiting calls require the reserved entry shown above):
//! ```
//! use fube_runtime::{IStatelessServicePartition, IStatefulServicePartition,
//!     HealthInformation, HealthReportSendOption, Result};
//! fn reports(s: &dyn IStatelessServicePartition, r: &dyn IStatefulServicePartition,
//!     h: &HealthInformation, o: Option<&HealthReportSendOption>) {
//!     let _: Result<()> = s.report_partition_health(h);
//!     let _: Result<()> = s.report_instance_health(h);
//!     let _: Result<()> = s.report_partition_health2(h, o);
//!     let _: Result<()> = s.report_instance_health2(h, o);
//!     let _: Result<()> = r.report_partition_health(h);
//!     let _: Result<()> = r.report_replica_health(h);
//!     let _: Result<()> = r.report_partition_health2(h, o);
//!     let _: Result<()> = r.report_replica_health2(h, o);
//! }
//! ```

mod api;
mod context;
mod diagnostics;
mod error;
mod execution;
pub mod extensions;
mod instance_integration;
mod lease;
mod lifecycle;
mod managed_participants;
#[cfg(test)]
mod model_tests;
mod package_context;
mod packages;
mod reporting;
mod stateful;
mod stateful_api;
mod wire;
#[cfg(test)]
extern crate self as fube_runtime;
#[cfg(test)]
#[path = "../tests/sdk_contracts.rs"]
mod sdk_contracts;

use std::{
    sync::Arc,
    time::{Duration, Instant as Monotonic},
};

pub use error::{Error, ErrorKind, RejectedRegistrationInput, Result};
use fube_transport::{
    connection::{deadline_after, Connection, IpcRequest},
    serialization::serde::{from_slice, to_vec},
};
use tokio::{sync::mpsc, task::JoinHandle};

pub(crate) use api::PartitionKind;
pub use api::{
    ApiFuture, Cancellation, FaultType, IStatelessServiceFactory, IStatelessServiceInstance,
    IStatelessServicePartition, Int64PartitionInformation, NamedPartitionInformation,
    ServicePartitionInformation, SingletonPartitionInformation,
};
pub use context::ActivationContext;
pub use diagnostics::{RuntimeCounters, RuntimeEvent, RuntimeMetrics};
use lease::Authority;
pub use lifecycle::{MAX_CALLBACKS, MAX_SERVICE_UNITS, MAX_TOMBSTONES};
pub use package_context::{
    ActivationContextHandle, PackageChange, PackageSubscription, MAX_PACKAGE_SUBSCRIPTIONS,
};
pub use packages::{
    ConfigurationPackage, ConfigurationParameter, ConfigurationSection, DataPackage,
    PackageDescription, PackageSnapshot,
};
pub(crate) use reporting::{FaultDelivery, HealthReport};
pub use reporting::{
    HealthInformation, HealthReportSendOption, HealthState, LoadMetric, MoveCost,
    MAX_REPORT_METRICS, MAX_REPORT_NAME_UNITS,
};
#[cfg(test)]
pub(crate) use reporting::{HealthReceipt, HealthSequence};
#[cfg(test)]
pub(crate) use stateful_api::{CatchUpResult, NativeReplicaSetConfiguration, ReplicaRemoval};
pub use stateful_api::{
    Epoch, IPrimaryReplicator, IReplicator, IStatefulServiceFactory, IStatefulServicePartition,
    IStatefulServiceReplica, OpenMode, ReplicaInformation, ReplicaRole, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, ServicePartitionAccessStatus, MAX_REPLICA_SET_SIZE,
};
use wire::*;

pub const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_INCOMING_COMMANDS: usize = 32;

/// Owned node snapshot returned by genuine host registration.
#[derive(Debug, Clone)]
pub struct NodeContext {
    pub id: String,
    pub instance: u64,
    pub name: String,
    pub node_type: String,
    pub ip_or_fqdn: String,
    pub client_address: String,
    pub deployment_directory: String,
    pub work_directory: String,
    pub host_process_id: u32,
    pub initial_lease_ttl_ticks: i64,
    pub lease_duration_ticks: i64,
    pub fresh_poll_ttl_ticks: i64,
    pub fresh_poll_elapsed_micros: u64,
}

/// Process-wide application host attachment. No reconnect or security fallback.
pub struct Runtime {
    activation: ActivationContext,
    current: ActivationContextHandle,
    node: NodeContext,
    connection: Connection,
    authority: Arc<Authority>,
    tasks: Vec<JoinHandle<()>>,
    lifecycle: lifecycle::Engine,
    shutdown: bool,
    core: Option<(tokio::sync::oneshot::Sender<()>, Arc<lease::OwnedThread>)>,
    execution: extensions::ExecutionRuntime,
    executor: tokio::runtime::Handle,
    registrations: std::sync::Mutex<tokio::task::JoinSet<()>>,
}

fn success(reply: ErrorReply) -> Result<()> {
    if reply.error == 0 {
        Ok(())
    } else {
        Err(Error::Remote {
            code: reply.error,
            message: "application runtime operation".into(),
        })
    }
}

async fn call<T: serde::Serialize, R: serde::de::DeserializeOwned>(
    connection: &Connection,
    actor: i64,
    action: &str,
    value: &T,
) -> Result<R> {
    let reply = connection
        .request(
            action,
            actor,
            vec![],
            to_vec(value)?,
            deadline_after(REGISTRATION_TIMEOUT)?,
        )
        .await?;
    let props = reply.properties()?;
    if let Some(fault) = props.fault.filter(|f| f.code != 0) {
        return Err(Error::Remote {
            code: fault.code,
            message: "hosting transport fault".into(),
        });
    }
    Ok(from_slice(&reply.body)?)
}

async fn poll_lease(connection: &Connection) -> Result<(Monotonic, i64)> {
    let sent = Monotonic::now();
    let reply = connection
        .request(
            "",
            15,
            vec![],
            vec![],
            deadline_after(Duration::from_secs(5))?,
        )
        .await?;
    if let Some(fault) = reply.properties()?.fault.filter(|fault| fault.code != 0) {
        return Err(Error::Remote {
            code: fault.code,
            message: "lease poll transport fault".into(),
        });
    }
    let lease: LeaseReply = from_slice(&reply.body)?;
    Ok((sent, lease.ttl))
}

impl Runtime {
    /// Attach using only node-injected context. Once attached, dropping this
    /// owner, failed/cancelled shutdown or loss of node authority terminates the
    /// entire application process within the hard-fence bound. Call `shutdown`
    /// explicitly for successful orderly cleanup.
    pub async fn attach() -> Result<Self> {
        Self::attach_context(None).await
    }

    pub(crate) async fn attach_context(activation: Option<ActivationContext>) -> Result<Self> {
        let application = tokio::runtime::Handle::current();
        let authority = Authority::new(Monotonic::now(), 1_200_000_000)?;
        authority.watchdog()?;
        let startup_authority = authority.clone();
        let (ready, readiness) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("fube-host-core".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(Error::Io(error.into())));
                        return;
                    }
                };
                runtime.block_on(async move {
                    match Self::attach_inner(application, startup_authority.clone(), activation)
                        .await
                    {
                        Ok(host) => {
                            if ready.send(Ok(host)).is_ok() {
                                let _ = stopped.await;
                            }
                        }
                        Err(error) => {
                            startup_authority.retire();
                            let _ = ready.send(Err(error));
                        }
                    }
                });
            })?;
        let thread = authority.own_thread(thread);
        let mut host = tokio::time::timeout(Duration::from_secs(120), readiness)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)??;
        host.core = Some((stop, thread));
        Ok(host)
    }

    async fn attach_inner(
        application: tokio::runtime::Handle,
        authority: Arc<Authority>,
        activation: Option<ActivationContext>,
    ) -> Result<Self> {
        let execution = extensions::ExecutionRuntime::new(authority.clone(), application);
        let mut activation = match activation {
            Some(activation) => activation,
            None => ActivationContext::from_environment()?,
        };
        let ingress_authority = authority.clone();
        let controls_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ingress_ready = controls_ready.clone();
        let (control_tx, mut controls) =
            mpsc::channel::<(Monotonic, IpcRequest)>(MAX_INCOMING_COMMANDS);
        let (command_tx, incoming) = mpsc::channel::<IpcRequest>(MAX_INCOMING_COMMANDS);
        let handler = Arc::new(move |request: IpcRequest| {
            let p = request.message.properties()?;
            match p.actor {
                8 => {
                    if p.action != "UpdateCodePackageContextRequest"
                        || !ingress_ready.load(std::sync::atomic::Ordering::Acquire)
                    {
                        ingress_authority.retire();
                    }
                    control_tx
                        .try_send((Monotonic::now(), request))
                        .map_err(|_| {
                            fube_transport::Error::ResourceLimit("runtime control ingress")
                        })
                }
                12 => command_tx
                    .try_send(request)
                    .map_err(|_| fube_transport::Error::ResourceLimit("runtime ingress")),
                _ => Err(fube_transport::Error::Protocol(
                    "unsupported application IPC actor",
                )),
            }
        });
        let connection = Connection::connect_ipc_clear(
            activation.address,
            deadline_after(REGISTRATION_TIMEOUT)?,
            &activation.host_id,
            handler,
        )
        .await?;
        let start: StartReply = call(
            &connection,
            7,
            "StartRegisterApplicationHostRequest",
            &StartRequest {
                id: activation.host_id.clone(),
                kind: 1,
                pid: std::process::id(),
                timeout: 300_000_000,
                container: false,
                activator: false,
            },
        )
        .await?;
        success(ErrorReply { error: start.error })?;
        authority.check()?;
        if start.lease_handle == 0 || start.node_id.is_empty() || start.node_name.is_empty() {
            connection.abort();
            return Err(Error::Protocol(
                "incomplete native node registration context",
            ));
        }
        let deployment = std::path::PathBuf::from(&start.deployment);
        let load_activation = activation.clone();
        let load_deployment = deployment.clone();
        let packages =
            tokio::task::spawn_blocking(move || packages::load(&load_activation, &load_deployment))
                .await
                .map_err(|_| Error::Protocol("package artifact worker panicked"))??;
        authority.check()?;
        activation.endpoints = packages.endpoints().clone();
        activation.packages = Some(Arc::new(packages));
        success(
            call(
                &connection,
                7,
                "FinishRegisterApplicationHostRequest",
                &IdRequest {
                    id: activation.host_id.clone(),
                },
            )
            .await?,
        )?;
        authority.check()?;
        success(
            call(
                &connection,
                9,
                "RegisterFabricRuntimeRequest",
                &RuntimeRequest {
                    context: activation.runtime.clone(),
                },
            )
            .await?,
        )?;
        authority.check()?;
        // Initial TTL source ambiguity never grants serving authority. A fresh
        // correlated node poll establishes request-send-relative authority.
        let (sent, ttl) = poll_lease(&connection).await?;
        authority.establish(sent, ttl)?;
        let node = NodeContext {
            id: start.node_id,
            instance: start.node_instance,
            name: start.node_name,
            node_type: start.node_type,
            ip_or_fqdn: start.ip,
            client_address: start.client_address,
            deployment_directory: start.deployment,
            work_directory: start.work,
            host_process_id: start.node_pid,
            initial_lease_ttl_ticks: start.initial_ttl,
            lease_duration_ticks: start.lease_duration,
            fresh_poll_ttl_ticks: ttl,
            fresh_poll_elapsed_micros: sent.elapsed().as_micros().min(u64::MAX as u128) as u64,
        };
        let current =
            ActivationContextHandle::with_execution(activation.clone(), execution.clone());
        let monitor_connection = connection.clone();
        let monitor_authority = authority.clone();
        let monitor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = monitor_connection.closed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                if monitor_authority.check().is_err() {
                    break;
                }
                match poll_lease(&monitor_connection).await {
                    Ok((sent, ttl)) => {
                        if monitor_authority
                            .renew(sent, ttl, Monotonic::now())
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) if monitor_connection.is_closed() => break,
                    Err(_) => continue,
                }
            }
            monitor_authority.retire();
        });
        let control_authority = authority.clone();
        let control_connection = connection.clone();
        let control_execution = execution.clone();
        let updater = Arc::new(package_context::Updater::new(current.clone(), &deployment));
        controls_ready.store(true, std::sync::atomic::Ordering::Release);
        let control = tokio::spawn(async move {
            while let Some((received, request)) = controls.recv().await {
                let result: Result<()> = async {
                    let p = request.message.properties()?;
                    let reply = request
                        .reply
                        .ok_or(Error::Protocol("host control requires correlation"))?;
                    match p.action.as_str() {
                        "InvalidateLeaseRequest" if request.message.body.is_empty() => {
                            control_authority.retire();
                            reply
                                .reply(
                                    8,
                                    None,
                                    to_vec(&ErrorReply { error: 0 })?,
                                    deadline_after(Duration::from_millis(250))?,
                                )
                                .await.map_err(Error::from)
                        }
                        "UpdateCodePackageContextRequest" => {
                            let update: UpdateContext = from_slice(&request.message.body)?;
                            if update.timeout <= 0 { return Err(Error::Timeout); }
                            let duration = Duration::from_nanos((update.timeout.min(300_000_000) as u64)*100);
                            let deadline = received.checked_add(duration).ok_or(Error::Timeout)?;
                            control_authority.check()?;
                            if Monotonic::now() >= deadline { return Err(Error::Timeout); }
                            let call_permit = control_execution.reserve(
                                extensions::CallLane::Native, extensions::CallOrigin::default(),
                                deadline.saturating_duration_since(Monotonic::now()), false)?;
                            let worker = updater.clone();
                            let permit = worker.admit()?;
                            let worker_authority = control_authority.clone();
                            let mut work = call_permit.run_owned(move || {
                                let _permit = permit;
                                worker.apply(update,&worker_authority,deadline)
                            });
                            tokio::select! {
                                result = &mut work => result?,
                                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(Error::Timeout),
                                _ = control_connection.closed() => return Err(Error::Closed),
                            }
                            let authority = control_authority.clone();
                            reply.reply_fenced(8,to_vec(&ErrorReply {error:0})?,
                                tokio::time::Instant::from_std(deadline),
                                Arc::new(move || authority.check().is_ok())).await.map_err(Error::from)
                        }
                        _ => Err(Error::Protocol("unsupported host control")),
                    }
                }
                .await;
                if let Err(error) = result {
                    control_connection.abort();
                    control_authority.retire();
                    // No synchronous diagnostics may delay fencing.
                    let _ = error;
                    break;
                }
            }
        });
        let lifecycle = lifecycle::Engine::start_with_execution(
            current.clone(),
            authority.clone(),
            connection.clone(),
            incoming,
            execution.clone(),
        );
        Ok(Self {
            activation,
            current,
            node,
            connection,
            authority,
            tasks: vec![monitor, control],
            lifecycle,
            shutdown: false,
            core: None,
            execution,
            executor: tokio::runtime::Handle::current(),
            registrations: std::sync::Mutex::new(tokio::task::JoinSet::new()),
        })
    }

    pub fn activation_context(&self) -> Arc<ActivationContext> {
        self.current.snapshot()
    }
    pub fn context_handle(&self) -> ActivationContextHandle {
        self.current.clone()
    }
    pub fn node_context(&self) -> &NodeContext {
        &self.node
    }
    pub fn metrics(&self) -> Arc<RuntimeMetrics> {
        self.lifecycle.metrics()
    }

    /// Subscribe to best-effort fube diagnostics, not canonical SF callbacks.
    /// Subscribe before triggering work: no event history is replayed. Slow receivers get
    /// `RecvError::Lagged`; publishers never wait for subscriber progress. The
    /// stream is observational, not an execution barrier or shutdown receipt.
    ///
    /// ```
    /// # async fn read_one(runtime: &fube_runtime::Runtime)
    /// # -> Result<fube_runtime::RuntimeEvent, tokio::sync::broadcast::error::RecvError> {
    /// let mut events = runtime.subscribe_events();
    /// events.recv().await // Propagates lag and closure to the caller.
    /// # }
    /// ```
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<RuntimeEvent> {
        self.lifecycle.subscribe_events()
    }

    /// Keep factory ownership until runtime shutdown. There is no invented
    /// per-service-type unregister operation. Register before placement begins.
    ///
    /// Call through a reserved execution entry. A direct invalid-context call
    /// does not accept or destroy the box: recover it with the error's typed
    /// `take_rejected_registration_input` accessor, then reserve/resubmit.
    /// Before acceptance and after deliberate recovery/drop, destruction is
    /// caller-owned. After admission, timeout is uncertain and retires the host;
    /// the retained factory is not unsafely rolled back.
    pub fn register_stateless_service_factory(
        &self,
        name: &str,
        factory: Box<dyn IStatelessServiceFactory>,
    ) -> Result<()> {
        if let Err(error) = self.execution.blocking_deadline() {
            if error.kind() == ErrorKind::InvalidExecutionContext {
                return Err(Error::rejected(RejectedRegistrationInput::Stateless(
                    factory,
                )));
            }
            return Err(error);
        }
        self.authority.check()?;
        if name.is_empty() || name.encode_utf16().count() > 32_768 {
            return Err(Error::InvalidInput("invalid service type name"));
        }
        self.lifecycle
            .insert_factory(name.to_owned(), Arc::from(factory))?;
        self.register_type(name)
    }

    /// Register a volatile stateful factory on this same host/runtime. Native
    /// placement remains the only source of replica membership and roles.
    /// Like [`Self::register_stateless_service_factory`], this synchronously
    /// waits for native registration on a reserved entry. An invalid-context
    /// rejection retains the original boxed factory in
    /// [`Error::take_rejected_registration_input`], recoverable exactly once.
    /// Accepted timeout is uncertain/terminal, not safe rollback.
    pub fn register_stateful_service_factory(
        &self,
        name: &str,
        factory: Box<dyn IStatefulServiceFactory>,
    ) -> Result<()> {
        if let Err(error) = self.execution.blocking_deadline() {
            if error.kind() == ErrorKind::InvalidExecutionContext {
                return Err(Error::rejected(RejectedRegistrationInput::Stateful(
                    factory,
                )));
            }
            return Err(error);
        }
        self.authority.check()?;
        if name.is_empty() || name.encode_utf16().count() > 32_768 {
            return Err(Error::InvalidInput("invalid service type name"));
        }
        self.lifecycle
            .insert_stateful_factory(name.to_owned(), Arc::from(factory))?;
        self.register_type(name)
    }

    fn register_type(&self, name: &str) -> Result<()> {
        let deadline = self
            .execution
            .blocking_deadline()?
            .min(Monotonic::now() + REGISTRATION_TIMEOUT);
        let activation = self.current.snapshot();
        let connection = self.connection.clone();
        let authority = self.authority.clone();
        let name = name.to_owned();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let mut registrations = self.registrations.lock().unwrap();
        while let Some(joined) = registrations.try_join_next() {
            if joined.is_err() {
                self.authority.retire();
                self.connection.abort();
                return Err(Error::Protocol("registration driver task failed"));
            }
        }
        if registrations.len() >= MAX_CALLBACKS {
            return Err(Error::ResourceLimit("registration driver records"));
        }
        registrations.spawn_on(
            async move {
                let reply: Result<ErrorReply> = call(
                    &connection,
                    9,
                    "RegisterServiceTypeRequest",
                    &TypeRequest {
                        context: activation.runtime.clone(),
                        id: TypeId {
                            package: activation.runtime.code.id.package.clone(),
                            name,
                        },
                    },
                )
                .await;
                let result = match reply {
                    Ok(reply) if reply.error == 0 || reply.error == 0x80071d31u32 as i32 as i64 => {
                        authority.check()
                    }
                    other => {
                        authority.retire();
                        connection.abort();
                        match other {
                            Ok(reply) => success(reply),
                            Err(error) => Err(error),
                        }
                    }
                };
                let _ = send.send(result);
            },
            &self.executor,
        );
        drop(registrations);
        match receive.recv_timeout(deadline.saturating_duration_since(Monotonic::now())) {
            Ok(result) => result,
            Err(error) => {
                self.authority.retire();
                self.connection.abort();
                Err(match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout => Error::Timeout.uncertain(),
                    std::sync::mpsc::RecvTimeoutError::Disconnected => Error::Closed.uncertain(),
                })
            }
        }
    }
    /// Fube-owned safe synchronous-call scheduling, not an SDK execution API.
    /// Reserve before captures, then run_owned and await its observation.
    pub fn execution_runtime(&self) -> extensions::ExecutionRuntime {
        self.execution.clone()
    }
    pub fn native_integration_provider(&self) -> extensions::NativeIntegrationProvider {
        self.lifecycle.native_integration_provider()
    }
    pub fn instance_integration_provider(&self) -> extensions::InstanceIntegrationProvider {
        self.lifecycle.instance_integration_provider()
    }

    pub async fn closed(&self) -> Error {
        self.connection.closed().await.into()
    }

    /// Orderly host/runtime unregistration. A failed shutdown remains terminal
    /// and the independent authority watchdog bounds application lifetime.
    pub async fn shutdown(mut self) -> Result<()> {
        // Query cleanup must not fault the IPC needed for unregistration.
        self.lifecycle.begin_quiesce();
        self.authority.retire();
        let execution = self.execution.clone();
        let permit = execution.reserve(
            extensions::CallLane::Native,
            extensions::CallOrigin::default(),
            Duration::from_millis(400),
            true,
        )?;
        tokio::time::timeout(Duration::from_millis(400), async move {
            let mut host = permit.run_future(self.shutdown_quiesce()).await?;
            host.execution.shutdown().await?;
            if let Some((stop, core)) = host.core.take() {
                let _ = stop.send(());
                core.join_finished().await?;
            }
            host.authority.disarm()?;
            host.shutdown = true;
            host.authority.join_watchdog().await?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn shutdown_quiesce(mut self) -> Result<Self> {
        let mut registrations = std::mem::take(self.registrations.get_mut().unwrap());
        registrations.abort_all(); // library-only request captures; factories stay registry-owned
        while let Some(joined) = registrations.join_next().await {
            if joined.is_err_and(|error| !error.is_cancelled()) {
                return Err(Error::Protocol("registration driver task failed"));
            }
        }
        if let Some(disposed) = self.current.close() {
            disposed.wait().await?;
        }
        self.current.quiescent()?;
        self.lifecycle.quiesce().await?;
        success(
            call(
                &self.connection,
                9,
                "UnregisterFabricRuntimeRequest",
                &IdRequest {
                    id: self.activation.runtime.id.clone(),
                },
            )
            .await?,
        )?;
        success(
            call(
                &self.connection,
                7,
                "UnregisterApplicationHostRequest",
                &UnregisterHost {
                    activity: Activity {
                        id: ActivityId {
                            guid: fube_transport::serialization::serde::Guid(
                                uuid::Uuid::new_v4().to_bytes_le(),
                            ),
                            index: 0,
                        },
                        kind: 0,
                    },
                    id: self.activation.host_id.clone(),
                },
            )
            .await?,
        )?;
        for task in &self.tasks {
            task.abort();
        }
        for task in &mut self.tasks {
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(_) => return Err(Error::Protocol("host core task panicked")),
            }
        }
        self.tasks.clear();
        self.connection.close().await?;
        Ok(self)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if !self.shutdown {
            self.authority.retire();
        }
        self.connection.abort();
        for task in &self.tasks {
            task.abort();
        }
        let _ = self.current.close();
    }
}
