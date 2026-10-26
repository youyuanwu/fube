//! Experimental Rust-native application hosting for SF-activated Linux ExeHost.
//! Hosting registration is not, by itself, stateless runtime qualification.

mod api;
mod context;
mod diagnostics;
mod lease;
mod lifecycle;
#[cfg(test)]
mod model_tests;
mod wire;

use std::{
    sync::Arc,
    time::{Duration, Instant as Monotonic},
};

use mssf_transport::{
    connection::{deadline_after, Connection, IpcRequest},
    serialization::serde::{from_slice, to_vec},
    Error, Result,
};
use tokio::{sync::mpsc, task::JoinHandle};

pub use api::{
    Cancellation, FaultType, Partition, PartitionKind, ServiceContext, ServiceFuture,
    ServiceIdentity, StatelessFactory, StatelessService,
};
pub use context::ActivationContext;
pub use diagnostics::{RuntimeCounters, RuntimeMetrics};
use lease::Authority;
pub use lifecycle::{MAX_CALLBACKS, MAX_SERVICE_UNITS, MAX_TOMBSTONES};
pub use mssf_transport::{Error as RuntimeError, Result as RuntimeResult};
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
    node: NodeContext,
    connection: Connection,
    authority: Arc<Authority>,
    tasks: Vec<JoinHandle<()>>,
    lifecycle: lifecycle::Engine,
    shutdown: bool,
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
    from_slice(&reply.body)
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
        tokio::time::timeout(Duration::from_secs(120), Self::attach_inner())
            .await
            .map_err(|_| Error::Timeout)?
    }

    async fn attach_inner() -> Result<Self> {
        let activation = ActivationContext::from_environment()?;
        let authority = Authority::new(Monotonic::now(), 1_200_000_000)?;
        let ingress_authority = authority.clone();
        let (control_tx, mut controls) = mpsc::channel::<IpcRequest>(MAX_INCOMING_COMMANDS);
        let (command_tx, incoming) = mpsc::channel::<IpcRequest>(MAX_INCOMING_COMMANDS);
        let handler = Arc::new(move |request: IpcRequest| {
            let p = request.message.properties()?;
            let sender = match p.actor {
                8 => {
                    // Every currently admitted host control is terminal in this
                    // profile. Latch before registration can consume its reply.
                    ingress_authority.retire();
                    &control_tx
                }
                12 => &command_tx,
                _ => return Err(Error::Protocol("unsupported application IPC actor")),
            };
            sender
                .try_send(request)
                .map_err(|_| Error::ResourceLimit("runtime ingress"))
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
        authority.watchdog()?;
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
        let control = tokio::spawn(async move {
            while let Some(request) = controls.recv().await {
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
                                .await
                        }
                        // Upgrades are not ACKed while retaining old context.
                        // A changed incarnation requires terminal host restart.
                        "UpdateCodePackageContextRequest" => {
                            let _: UpdateContext = from_slice(&request.message.body)?;
                            Err(Error::Protocol(
                                "package context refresh is not yet supported",
                            ))
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
        let lifecycle = lifecycle::Engine::start(
            activation.clone(),
            authority.clone(),
            connection.clone(),
            incoming,
        );
        Ok(Self {
            activation,
            node,
            connection,
            authority,
            tasks: vec![monitor, control],
            lifecycle,
            shutdown: false,
        })
    }

    pub fn activation_context(&self) -> &ActivationContext {
        &self.activation
    }
    pub fn node_context(&self) -> &NodeContext {
        &self.node
    }
    pub fn metrics(&self) -> Arc<RuntimeMetrics> {
        self.lifecycle.metrics()
    }

    /// Keep factory ownership until runtime shutdown. There is no invented
    /// per-service-type unregister operation. Register before placement begins.
    pub async fn register_stateless(
        &mut self,
        name: &str,
        factory: Arc<dyn StatelessFactory>,
    ) -> Result<()> {
        self.authority.check()?;
        if name.is_empty() || name.encode_utf16().count() > 32_768 {
            return Err(Error::InvalidInput("invalid service type name"));
        }
        self.lifecycle.insert_factory(name.to_owned(), factory)?;
        let reply: Result<ErrorReply> = call(
            &self.connection,
            9,
            "RegisterServiceTypeRequest",
            &TypeRequest {
                context: self.activation.runtime.clone(),
                id: TypeId {
                    package: self.activation.runtime.code.id.package.clone(),
                    name: name.to_owned(),
                },
            },
        )
        .await;
        match reply {
            Ok(reply) if reply.error == 0 || reply.error == 0x80071d31u32 as i32 as i64 => {
                self.authority.check()
            }
            other => {
                self.authority.retire();
                self.connection.abort();
                match other {
                    Ok(reply) => success(reply),
                    Err(error) => Err(error),
                }
            }
        }
    }

    pub async fn closed(&self) -> Error {
        self.connection.closed().await
    }

    /// Orderly host/runtime unregistration. A failed shutdown remains terminal
    /// and the independent authority watchdog bounds application lifetime.
    pub async fn shutdown(mut self) -> Result<()> {
        self.authority.retire();
        let outcome = tokio::time::timeout(Duration::from_millis(400), async {
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
                                guid: mssf_transport::serialization::serde::Guid(
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
            Result::Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?;
        outcome?;
        for task in &self.tasks {
            task.abort();
        }
        self.connection.close().await?;
        self.authority.disarm();
        self.shutdown = true;
        Ok(())
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
    }
}
