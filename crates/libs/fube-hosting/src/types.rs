//! ServiceTypeStateManager.cpp:128-299,425-838,1142-1557.
//! Selected declared managed profile: no adhoc recovery or live failure producer.
use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeStatus {
    NotRegisteredEnabled,
    InProgressEnabled,
    RegisteredEnabled,
    NotRegisteredDisableScheduled,
    NotRegisteredDisabled,
    InProgressDisabled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeEntry {
    pub id: TypeInstance,
    pub status: TypeStatus,
    pub sequence: u64,
    pub runtime_id: String,
    pub host_id: String,
    pub code_package: String,
    pub failure: Option<String>,
    /// Native component-source membership is case-sensitive.
    pub sources: Vec<String>,
    pub disable_timer: Option<u64>,
}
impl TypeEntry {
    pub(crate) fn bytes(&self) -> usize {
        128 + self.id.bytes()
            + self.runtime_id.len()
            + self.host_id.len()
            + self.code_package.len()
            + self.failure.as_ref().map_or(0, String::len)
            + self.sources.iter().map(|s| 32 + s.len()).sum::<usize>()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeState {
    pub status: TypeStatus,
    pub sequence: u64,
    pub runtime_id: String,
}
#[derive(Debug, Clone)]
pub struct Declaration {
    pub id: TypeInstance,
    /// Modeled health-source admission for a NEW entry, not a live health reply.
    pub health_admission: Result<()>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclarationResult {
    pub admitted: usize,
    pub failure: Option<(usize, Error)>,
}

impl State {
    pub(crate) fn type_seq(&mut self) -> Result<u64> {
        self.type_sequence = self.type_sequence.checked_add(1).ok_or(Error::Capacity)?;
        Ok(self.type_sequence)
    }
    pub(crate) fn check_type_identity(&self, id: &TypeInstance) -> Result<()> {
        validate_public(id.activation, &id.public_activation)?;
        // PackageInstanceIdentifier comparison asserts identical public ID once
        // package/context match, even when the short type names differ.
        for e in &self.types {
            if compare_packages(&e.id.service_type.package, &id.service_type.package).is_eq()
                && e.id.activation == id.activation
                && e.id.public_activation != id.public_activation
            {
                return Err(Error::NativeInvariant(
                    "public activation identity disagreement",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn type_index(&self, id: &TypeInstance) -> Result<usize> {
        if self.types_closed {
            return Err(Error::ObjectClosed);
        }
        self.check_type_identity(id)?;
        self.types
            .iter()
            .position(|e| compare_type_instances(&e.id, id).is_eq())
            .ok_or(Error::NotFound)
    }
    pub(crate) fn type_state(&mut self, id: &TypeInstance) -> Result<TypeState> {
        match self.type_index(id) {
            Ok(i) => Ok(TypeState {
                status: self.types[i].status,
                sequence: self.types[i].sequence,
                runtime_id: self.types[i].runtime_id.clone(),
            }),
            Err(Error::NotFound) => Ok(TypeState {
                status: TypeStatus::NotRegisteredEnabled,
                sequence: self.type_seq()?,
                runtime_id: String::new(),
            }),
            Err(e) => Err(e),
        }
    }
    fn reset_failure(&mut self, index: usize) -> Result<TypeEntry> {
        if let Some(failure) = self.types[index].failure.take() {
            self.types[index].sources.retain(|s| s != &failure);
        }
        if self.types[index].sources.is_empty() {
            // Native last-source removal changes the HELD entry to Closed and
            // erases it; an outer helper can still enqueue from that held entry.
            self.types[index].sequence = self.type_seq()?;
            let held = self.types.remove(index);
            self.obligations.push(Obligation::HealthSourceReleased {
                type_instance: held.id.clone(),
            });
            Ok(held)
        } else {
            Ok(self.types[index].clone())
        }
    }
    fn clear_owner(&mut self, index: usize) {
        let e = &mut self.types[index];
        e.runtime_id.clear();
        e.host_id.clear();
        e.code_package.clear();
    }
    pub(crate) fn start_type(&mut self, id: &TypeInstance, context: &RuntimeContext) -> Result<()> {
        if id.activation != context.package.activation {
            return Err(Error::NativeInvariant(
                "type/runtime activation disagreement",
            ));
        }
        let i = match self.type_index(id) {
            Ok(i) => i,
            Err(Error::NotFound) => return Err(Error::InvalidServiceType),
            Err(e) => return Err(e),
        };
        let prior = self.types[i].status;
        if prior == TypeStatus::NotRegisteredDisableScheduled
            && self.types[i]
                .sources
                .iter()
                .all(|source| self.types[i].failure.as_ref() == Some(source))
        {
            // ServiceTypeStateManager.cpp:469-473 asserts a retained component after
            // the helper resets failure. Unlike Finish, last-source erase is not
            // a defined successful Start. Safety veto precedes all mutation.
            return Err(Error::NativeInvariant(
                "scheduled Start removes last component",
            ));
        }
        let status = match prior {
            TypeStatus::NotRegisteredEnabled => TypeStatus::InProgressEnabled,
            TypeStatus::NotRegisteredDisableScheduled => {
                if let Some(timer) = self.types[i].disable_timer.take() {
                    self.cancel_timer(timer);
                }
                TypeStatus::InProgressEnabled
            }
            TypeStatus::NotRegisteredDisabled => TypeStatus::InProgressDisabled,
            TypeStatus::InProgressEnabled | TypeStatus::InProgressDisabled => {
                return Err(Error::InvalidState)
            }
            TypeStatus::RegisteredEnabled => {
                return Err(
                    if compare_names(&self.types[i].runtime_id, &context.runtime_id).is_eq() {
                        Error::TypeAlreadyRegisteredToSameRuntime
                    } else {
                        Error::TypeAlreadyRegistered
                    },
                )
            }
        };
        let sequence = self.type_seq()?;
        let e = &mut self.types[i];
        e.status = status;
        e.sequence = sequence;
        e.runtime_id = context.runtime_id.clone();
        e.host_id = context.host_id.clone();
        e.code_package = context.code_package.clone();
        if prior == TypeStatus::NotRegisteredDisableScheduled {
            self.reset_failure(i)?;
        }
        Ok(())
    }
    pub(crate) fn finish_type(
        &mut self,
        id: &TypeInstance,
        valid: bool,
        context: &RuntimeContext,
    ) -> Result<()> {
        let i = self.type_index(id)?;
        let prior = self.types[i].status;
        let status = match (prior, valid) {
            (TypeStatus::InProgressEnabled | TypeStatus::InProgressDisabled, true) => {
                TypeStatus::RegisteredEnabled
            }
            (TypeStatus::InProgressEnabled, false) => TypeStatus::NotRegisteredEnabled,
            (TypeStatus::InProgressDisabled, false) => TypeStatus::NotRegisteredDisabled,
            (TypeStatus::RegisteredEnabled, _) => return Ok(()), // no-op, not owner fenced
            _ => return Err(Error::InvalidState),
        };
        let sequence = self.type_seq()?;
        if self.types[i].runtime_id.is_empty() || self.types[i].host_id.is_empty() {
            return Err(Error::NativeInvariant("Finish with empty tentative owner"));
        }
        if !valid {
            self.clear_owner(i);
        }
        self.types[i].status = status;
        self.types[i].sequence = sequence;
        let held = if valid && prior == TypeStatus::InProgressDisabled {
            self.reset_failure(i)?
        } else {
            self.types[i].clone()
        };
        if valid {
            self.closed_registered
                .push(ClosedRegisteredEvent::Registered {
                    sequence: held.sequence,
                    registration: Registration::from_context(held.id.clone(), context),
                });
            if prior == TypeStatus::InProgressDisabled {
                self.enabled_disabled.push(EnabledDisabledEvent::Enabled {
                    sequence: held.sequence,
                    type_instance: held.id,
                });
            }
        }
        Ok(())
    }
    pub(crate) fn validate_type_runtime(
        &self,
        id: &TypeInstance,
        context: &RuntimeContext,
    ) -> Result<()> {
        let actual = self.runtime(&context.runtime_id)?;
        if !compare_names(&actual.runtime_id, &context.runtime_id).is_eq()
            || !compare_names(&actual.host_id, &context.host_id).is_eq()
            || actual.package.activation != context.package.activation
        {
            return Err(Error::RuntimeNotRegistered);
        }
        if id.activation != context.package.activation {
            return Err(Error::NativeInvariant(
                "type/runtime activation disagreement",
            ));
        }
        if !self.host_valid(&context.host_id)? {
            return Err(Error::HostNotRegistered);
        }
        Ok(())
    }
    fn remove_source(&mut self, id: &TypeInstance, source: &str) -> Result<()> {
        let i = match self.type_index(id) {
            Ok(i) => i,
            Err(Error::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        let e = &mut self.types[i];
        e.sources.retain(|s| s != source);
        if e.sources.is_empty() {
            if e.failure.is_some()
                || e.disable_timer.is_some()
                || !matches!(
                    e.status,
                    TypeStatus::NotRegisteredEnabled
                        | TypeStatus::InProgressEnabled
                        | TypeStatus::RegisteredEnabled
                )
            {
                return Err(Error::NativeInvariant(
                    "last component removed from disabled entry",
                ));
            }
            self.type_seq()?;
            let removed = self.types.remove(i);
            self.obligations.push(Obligation::HealthSourceReleased {
                type_instance: removed.id,
            });
        }
        Ok(())
    }
}

impl Hosting {
    pub fn type_entries(&self) -> &[TypeEntry] {
        &self.state.types
    }
    pub fn get_type_state(&mut self, id: &TypeInstance) -> Result<TypeState> {
        self.change(|s| s.type_state(id))
    }
    pub fn declare_type(
        &mut self,
        id: TypeInstance,
        source: String,
        health_admission: Result<()>,
    ) -> Result<()> {
        if source.is_empty() {
            return Err(Error::InvalidInput);
        }
        // Supported managed profile only; do not turn adhoc identities into a
        // successful factory/package-execution capability.
        if id.service_type.package.application == ApplicationId::default() {
            return Err(Error::UnsupportedCapability);
        }
        self.change(|s| {
            let index = match s.type_index(&id) {
                Ok(i) => {
                    if !s.types[i].runtime_id.is_empty()
                        || !s.types[i].host_id.is_empty()
                        || !s.types[i].code_package.is_empty()
                    {
                        return Err(Error::NativeInvariant(
                            "activation overlaps registered owner",
                        ));
                    }
                    i
                }
                Err(Error::NotFound) => {
                    health_admission?;
                    let sequence = s.type_seq()?;
                    let position = s
                        .types
                        .iter()
                        .position(|e| compare_type_instances(&e.id, &id).is_gt())
                        .unwrap_or(s.types.len());
                    s.types.insert(
                        position,
                        TypeEntry {
                            id,
                            status: TypeStatus::NotRegisteredEnabled,
                            sequence,
                            runtime_id: String::new(),
                            host_id: String::new(),
                            code_package: String::new(),
                            failure: None,
                            sources: Vec::new(),
                            disable_timer: None,
                        },
                    );
                    position
                }
                Err(e) => return Err(e),
            };
            if s.types[index].sources.contains(&source) {
                return Err(Error::NativeInvariant("duplicate component source"));
            }
            s.types[index].sources.push(source);
            Ok(())
        })
    }
    /// Native activation admission is a per-entry loop. A later native health
    /// failure/capacity veto retains the admitted prefix and reports its boundary.
    pub fn declare_types(
        &mut self,
        declarations: Vec<Declaration>,
        source: &str,
    ) -> DeclarationResult {
        if declarations.is_empty() {
            return DeclarationResult {
                admitted: 0,
                failure: Some((0, Error::InvalidInput)),
            };
        }
        let count = declarations.len();
        for (index, declaration) in declarations.into_iter().enumerate() {
            if let Err(error) =
                self.declare_type(declaration.id, source.into(), declaration.health_admission)
            {
                return DeclarationResult {
                    admitted: index,
                    failure: Some((index, error)),
                };
            }
        }
        DeclarationResult {
            admitted: count,
            failure: None,
        }
    }
    pub fn remove_type_source(&mut self, id: &TypeInstance, source: &str) -> Result<()> {
        self.change(|s| s.remove_source(id, source))
    }
    pub fn start_type_registration(
        &mut self,
        id: &TypeInstance,
        context: &RuntimeContext,
    ) -> Result<()> {
        self.change(|s| s.start_type(id, context))
    }
    pub fn finish_type_registration(
        &mut self,
        id: &TypeInstance,
        valid: bool,
        context: &RuntimeContext,
    ) -> Result<()> {
        self.change(|s| s.finish_type(id, valid, context))
    }
    pub fn validate_type_registration(
        &self,
        id: &TypeInstance,
        context: &RuntimeContext,
    ) -> Result<()> {
        self.state.validate_type_runtime(id, context)
    }
    pub fn cleanup_type_registration(
        &mut self,
        id: &TypeInstance,
        context: &RuntimeContext,
        original: Error,
    ) -> ProcessorResult {
        ProcessorResult {
            result: Err(original),
            cleanup: Some(self.finish_type_registration(id, false, context)),
        }
    }
    pub fn register_type(
        &mut self,
        id: &TypeInstance,
        context: &RuntimeContext,
    ) -> ProcessorResult {
        if let Err(e) = self.start_type_registration(id, context) {
            return ProcessorResult {
                result: Err(e),
                cleanup: None,
            };
        }
        if let Err(e) = self.validate_type_registration(id, context) {
            return self.cleanup_type_registration(id, context, e);
        }
        ProcessorResult {
            result: self.finish_type_registration(id, true, context),
            cleanup: None,
        }
    }
    pub fn register_failure(&mut self, failure: String) -> Result<()> {
        if failure.is_empty() {
            return Err(Error::InvalidInput);
        }
        self.change(|s| {
            if s.types_closed {
                return Err(Error::ObjectClosed);
            }
            if !s
                .failures
                .iter()
                .any(|f| compare_names(f, &failure).is_eq())
            {
                s.failures.push(failure);
            }
            Ok(())
        })
    }
    /// Selected managed registered-failure producer. Unknown failure, closed
    /// manager, blocklisting off and non-NE states are native ignored inputs.
    pub fn disable_type(
        &mut self,
        id: &TypeInstance,
        failure: &str,
        clock: &dyn Clock,
        grace_ticks: i64,
        blocklisting: bool,
    ) -> Result<Option<Timer>> {
        let now = clock.now_ticks();
        self.change(|s| {
            if s.types_closed
                || !blocklisting
                || !s.failures.iter().any(|f| compare_names(f, failure).is_eq())
            {
                return Ok(None);
            }
            let i = match s.type_index(id) {
                Ok(i) => i,
                Err(Error::NotFound) => return Err(Error::UnsupportedCapability),
                Err(e) => return Err(e),
            };
            if s.types[i].status != TypeStatus::NotRegisteredEnabled {
                return Ok(None);
            }
            if s.types[i].sources.iter().any(|c| c == failure) {
                return Err(Error::NativeInvariant("failure/component source collision"));
            }
            let sequence = s.type_seq()?;
            let timer = Timer {
                id: s.id()?,
                due_ticks: now.saturating_add(grace_ticks.max(1)),
                kind: TimerKind::DisableType {
                    type_instance: s.types[i].id.clone(),
                    sequence,
                },
            };
            let e = &mut s.types[i];
            e.status = TypeStatus::NotRegisteredDisableScheduled;
            e.sequence = sequence;
            e.failure = Some(failure.into());
            e.sources.push(failure.into());
            e.disable_timer = Some(timer.id);
            s.timers.push(timer.clone());
            Ok(Some(timer))
        })
    }
    /// Timer delivery is an explicit observation; status AND captured sequence
    /// gate the transition. No global "latest wins" policy is used by Finish.
    pub fn disable_timer_input(&mut self, timer: &Timer) -> Result<bool> {
        let TimerKind::DisableType {
            type_instance,
            sequence,
        } = &timer.kind
        else {
            return Err(Error::InvalidInput);
        };
        self.change(|s| {
            // Finite output reservation also covers cancellation: capacity
            // failure leaves the required callback retryable with no mutation.
            s.cancel_timer(timer.id);
            let i = match s.type_index(type_instance) {
                Ok(i) => i,
                Err(Error::NotFound | Error::ObjectClosed) => return Ok(false),
                Err(e) => return Err(e),
            };
            if s.types[i].status != TypeStatus::NotRegisteredDisableScheduled
                || s.types[i].sequence != *sequence
            {
                return Ok(false);
            }
            let next = s.type_seq()?;
            if let Some(timer) = s.types[i].disable_timer.take() {
                s.cancel_timer(timer);
            }
            s.types[i].status = TypeStatus::NotRegisteredDisabled;
            s.types[i].sequence = next;
            s.enabled_disabled.push(EnabledDisabledEvent::Disabled {
                sequence: next,
                type_instance: s.types[i].id.clone(),
            });
            Ok(true)
        })
    }
    pub fn unregister_failure(&mut self, failure: &str) -> Result<()> {
        self.change(|s| {
            if s.types_closed {
                return Ok(());
            }
            let Some(f) = s
                .failures
                .iter()
                .position(|f| compare_names(f, failure).is_eq())
            else {
                return Ok(());
            };
            let affected: Vec<_> = s
                .types
                .iter()
                .filter_map(|e| {
                    e.failure
                        .as_ref()
                        .filter(|f| compare_names(f, failure).is_eq())
                        .map(|_| e.id.clone())
                })
                .collect();
            // ServiceTypeStateManager.cpp:1515-1524 has contradictory fatal
            // timer assertions. Reject before mutation; no invented successful body.
            if affected.iter().any(|id| {
                s.types[s.type_index(id).expect("associated")].status
                    == TypeStatus::InProgressDisabled
            }) {
                return Err(Error::NativeInvariant(
                    "InProgress_Disabled failure removal",
                ));
            }
            for id in affected {
                let i = s.type_index(&id)?;
                let prior = s.types[i].status;
                if !matches!(
                    prior,
                    TypeStatus::NotRegisteredDisableScheduled | TypeStatus::NotRegisteredDisabled
                ) {
                    return Err(Error::NativeInvariant(
                        "failure attached to unsupported status",
                    ));
                }
                if let Some(timer) = s.types[i].disable_timer.take() {
                    s.cancel_timer(timer);
                }
                s.types[i].status = TypeStatus::NotRegisteredEnabled;
                s.types[i].sequence = s.type_seq()?;
                let held = s.reset_failure(i)?;
                if prior == TypeStatus::NotRegisteredDisabled {
                    s.enabled_disabled.push(EnabledDisabledEvent::Enabled {
                        sequence: held.sequence,
                        type_instance: held.id,
                    });
                }
            }
            s.failures.remove(f);
            Ok(())
        })
    }
    pub fn close_type_manager(&mut self) -> Result<()> {
        self.change(|s| {
            s.types_closed = true;
            s.types.clear();
            s.failures.clear();
            s.timers
                .retain(|t| matches!(t.kind, TimerKind::HostFinish(_)));
            // Accepted owned outputs remain drainable; no fake closure broadcast.
            Ok(())
        })
    }
}
