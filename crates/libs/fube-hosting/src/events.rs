//! ServiceTypeStateManager.cpp:923-1117,1581-1636; EventDispatcher.cpp:39-197.
//! Native registration/closure and enabled/disabled queues are separate.
//! These are owned enqueue observations, not an execution model of native
//! FifoLifo worker scheduling or a global cross-lane delivery-order claim.
use crate::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClosureScope {
    pub activation: Activation,
    pub public_activation: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosedRegisteredEvent {
    Registered {
        sequence: u64,
        registration: Registration,
    },
    RuntimeClosed {
        sequence: u64,
        runtime_id: String,
        host_id: String,
        scope: ClosureScope,
        affected: Vec<ServiceTypeId>,
    },
    HostClosed {
        sequence: u64,
        host_id: String,
        activity: String,
        scope: ClosureScope,
        affected: Vec<ServiceTypeId>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnabledDisabledEvent {
    Enabled {
        sequence: u64,
        type_instance: TypeInstance,
    },
    Disabled {
        sequence: u64,
        type_instance: TypeInstance,
    },
}

/// Explicit modeled external obligations; not real health, deployment or usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Obligation {
    TypesUnregistered { types: Vec<TypeInstance> },
    HealthSourceReleased { type_instance: TypeInstance },
}

impl Registration {
    pub(crate) fn bytes(&self) -> usize {
        128 + self.type_instance.bytes()
            + self.host_id.len()
            + self.runtime_id.len()
            + self.code_package.len()
    }
}
fn type_bytes(t: &ServiceTypeId) -> usize {
    64 + t.package.bytes() + t.name.len()
}
impl ClosedRegisteredEvent {
    pub(crate) fn bytes(&self) -> usize {
        64 + match self {
            Self::Registered { registration, .. } => registration.bytes(),
            Self::RuntimeClosed {
                runtime_id,
                host_id,
                scope,
                affected,
                ..
            } => {
                runtime_id.len()
                    + host_id.len()
                    + scope.public_activation.len()
                    + affected.iter().map(type_bytes).sum::<usize>()
            }
            Self::HostClosed {
                host_id,
                activity,
                scope,
                affected,
                ..
            } => {
                host_id.len()
                    + activity.len()
                    + scope.public_activation.len()
                    + affected.iter().map(type_bytes).sum::<usize>()
            }
        }
    }
}
impl EnabledDisabledEvent {
    pub(crate) fn bytes(&self) -> usize {
        64 + match self {
            Self::Enabled { type_instance, .. } | Self::Disabled { type_instance, .. } => {
                type_instance.bytes()
            }
        }
    }
}
impl Obligation {
    pub(crate) fn bytes(&self) -> usize {
        64 + match self {
            Self::TypesUnregistered { types } => {
                types.iter().map(|t| 64 + t.bytes()).sum::<usize>()
            }
            Self::HealthSourceReleased { type_instance } => type_instance.bytes(),
        }
    }
}

impl State {
    pub(crate) fn close_types_for(
        &mut self,
        runtime: Option<&str>,
        host: &str,
        activity: &str,
    ) -> Result<()> {
        if self.types_closed {
            return Ok(());
        }
        let sequence = self.type_seq()?;
        let mut scope = ClosureScope::default();
        let mut affected = Vec::new();
        let mut instances = Vec::new();
        for entry in &mut self.types {
            let matches = match runtime {
                Some(id) => compare_names(&entry.runtime_id, id).is_eq(),
                None => compare_names(&entry.host_id, host).is_eq(),
            };
            if !matches {
                continue;
            }
            if entry.runtime_id.is_empty() || entry.host_id.is_empty() {
                return Err(Error::NativeInvariant("closure with empty tentative owner"));
            }
            let status = match entry.status {
                TypeStatus::InProgressEnabled | TypeStatus::RegisteredEnabled => {
                    TypeStatus::NotRegisteredEnabled
                }
                TypeStatus::InProgressDisabled => TypeStatus::NotRegisteredDisabled,
                _ => {
                    return Err(Error::NativeInvariant(
                        "closure matched not-registered owner",
                    ))
                }
            };
            if affected.is_empty() {
                scope = ClosureScope {
                    activation: entry.id.activation,
                    public_activation: entry.id.public_activation.clone(),
                };
            }
            entry.status = status;
            entry.sequence = sequence;
            entry.runtime_id.clear();
            entry.host_id.clear();
            entry.code_package.clear();
            affected.push(entry.id.service_type.clone());
            instances.push(entry.id.clone());
        }
        // Native ApplicationManager resets type timeout tracking before enqueue.
        // Empty input is a native no-op, not a fabricated successful callback.
        if !instances.is_empty() {
            self.obligations
                .push(Obligation::TypesUnregistered { types: instances });
        }
        self.closed_registered.push(match runtime {
            Some(runtime_id) => ClosedRegisteredEvent::RuntimeClosed {
                sequence,
                runtime_id: runtime_id.into(),
                host_id: host.into(),
                scope,
                affected,
            },
            None => ClosedRegisteredEvent::HostClosed {
                sequence,
                host_id: host.into(),
                activity: activity.into(),
                scope,
                affected,
            },
        });
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostUnregisterResult {
    pub reply: Result<()>,
    pub runtime_removal: Option<Result<usize>>,
}
impl Hosting {
    pub fn closed_registered_events(&self) -> &[ClosedRegisteredEvent] {
        &self.state.closed_registered
    }
    pub fn enabled_disabled_events(&self) -> &[EnabledDisabledEvent] {
        &self.state.enabled_disabled
    }
    pub fn obligations(&self) -> &[Obligation] {
        &self.state.obligations
    }
    pub fn take_closed_registered_events(&mut self) -> Vec<ClosedRegisteredEvent> {
        std::mem::take(&mut self.state.closed_registered)
    }
    pub fn take_enabled_disabled_events(&mut self) -> Vec<EnabledDisabledEvent> {
        std::mem::take(&mut self.state.enabled_disabled)
    }
    pub fn take_obligations(&mut self) -> Vec<Obligation> {
        std::mem::take(&mut self.state.obligations)
    }
    /// Low-level native type closure, including empty affected event. Not an OS
    /// exit, host death, package usage release or physical cleanup observation.
    pub fn runtime_closed_input(&mut self, runtime: &str, host: &str) -> Result<()> {
        self.change(|s| s.close_types_for(Some(runtime), host, ""))
    }
    pub fn host_closed_input(&mut self, host: &str, activity: &str) -> Result<()> {
        self.change(|s| s.close_types_for(None, host, activity))
    }
    /// Public unregister reserves the complete projected event/obligation before
    /// publishing destructive table removal. Capacity failure changes nothing.
    pub fn unregister_runtime(&mut self, runtime: &str) -> Result<()> {
        self.check_public_runtime_unregister(runtime)?;
        self.change(|s| {
            let context = s.remove_runtime(runtime)?.ok_or(Error::NativeInvariant(
                "tentative public runtime unregister",
            ))?;
            s.close_types_for(Some(&context.runtime_id), &context.host_id, "")
        })
    }
    pub fn unregister_host(&mut self, host: &str, activity: &str) -> Result<HostUnregisterResult> {
        self.change(|s| {
            let removed = match s.remove_host(host) {
                Ok(removed) => removed,
                Err(e) => {
                    return Ok(HostUnregisterResult {
                        reply: Err(e),
                        runtime_removal: None,
                    })
                }
            };
            let runtime_removal = s.remove_runtimes_for_host(&removed.host_id);
            // Native consumes runtime-removal error and still calls type closure.
            s.close_types_for(None, &removed.host_id, activity)?;
            Ok(HostUnregisterResult {
                reply: Ok(()),
                runtime_removal: Some(runtime_removal),
            })
        })
    }
    /// Controlled external termination input follows the native observed-host
    /// path even when Remove fails. It does not itself observe or own a process.
    pub fn host_termination_input(
        &mut self,
        host: &str,
        activity: &str,
    ) -> Result<HostUnregisterResult> {
        self.change(|s| {
            let reply = s.remove_host(host).map(|_| ());
            let runtime_removal = s.remove_runtimes_for_host(host);
            s.close_types_for(None, host, activity)?;
            Ok(HostUnregisterResult {
                reply,
                runtime_removal: Some(runtime_removal),
            })
        })
    }
}
