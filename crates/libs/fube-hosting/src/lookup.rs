//! FabricRuntimeManager.cpp:117-249,296-458; HostingSubsystem.cpp:64-180.
//! Candidate/error/sequence are independent, including error with a candidate.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Immutable lookup inputs; see [`RegistrationLookup`] for native provenance,
/// comparison and supported activating/nonactivating paths.
pub struct LookupRequest {
    pub service_type: ServiceTypeId,
    pub version: VersionInstance,
    pub activation: Activation,
    pub service_name: String,
    pub application_name: String,
    pub should_activate: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
/// Independent native semantic outputs, nested inside local admission.
///
/// A candidate on error is not necessarily acquired or admissible to RA.
/// [`RegistrationLookup::lookup`] documents the source-derived output matrix.
pub struct LookupResult {
    pub result: Result<()>,
    pub candidate: Option<Registration>,
    /// None means native did not write the caller's sequence output, NOT zero.
    pub sequence: Option<u64>,
}
impl LookupResult {
    fn error(error: Error, sequence: Option<u64>) -> Self {
        Self {
            result: Err(error),
            candidate: None,
            sequence,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicActivationEntry {
    pub key: String,
    pub public_activation: String,
    pub service_name: Option<String>,
    pub application_name: Option<String>,
}

/// First-inserted rendered-CI mapping provenance, not package deployment evidence.
///
/// Controlled bindings without request names leave those fields absent.
#[derive(Debug)]
pub struct LookupPublicActivation<'a> {
    pub rendered_key: &'a str,
    pub public_activation: &'a str,
    pub service_name: Option<&'a str>,
    pub application_name: Option<&'a str>,
}
impl State {
    fn public_activation(
        &mut self,
        request: &LookupRequest,
        model: &LookupModel,
    ) -> Result<Option<String>> {
        validate_package(&request.service_type.package)?;
        if request.activation == Activation::SHARED {
            return Ok(Some(String::new()));
        }
        let key = public_activation_map_key(&request.service_type.package, request.activation)?;
        if let Some(p) = self
            .public_activations
            .iter()
            .find(|p| compare_names(&p.key, &key).is_eq())
        {
            return Ok(Some(p.public_activation.clone()));
        }
        if !request.should_activate {
            return Ok(None);
        }
        // Full local-core shutdown prohibits NEW modeled admission. Closing one
        // native table alone is not a universal lookup fence; early nonactivating
        // resolution and already accepted activation End retain their contracts.
        if self.types_closed && self.hosts_closed && self.runtimes_closed {
            return Err(Error::ObjectClosed);
        }
        if request.service_name.is_empty() {
            return Err(Error::NativeInvariant(
                "exclusive public activation requires service name",
            ));
        }
        let public = model
            .public_activation_assignment
            .as_ref()
            .ok_or(Error::MissingModelOutcome)?;
        validate_public(request.activation, public)?;
        self.public_activations.push(PublicActivationEntry {
            key,
            public_activation: public.clone(),
            service_name: Some(request.service_name.clone()),
            application_name: Some(request.application_name.clone()),
        });
        Ok(Some(public.clone()))
    }
}
impl Hosting {
    /// Explicit controlled identity mapping for model-only exclusive scenarios.
    /// Native rendered-key aliases retain the first identity; conflicting
    /// explicit assignments cannot create a second independent mapping.
    pub fn bind_public_activation(&mut self, package: PackageInstance) -> Result<()> {
        let key = public_activation_map_key(&package.package, package.activation)?;
        validate_public(package.activation, &package.public_activation)?;
        if package.activation == Activation::SHARED {
            return Err(Error::InvalidInput); // shared resolver always returns empty
        }
        self.change(|s| {
            if s.types_closed && s.hosts_closed && s.runtimes_closed {
                return Err(Error::ObjectClosed);
            }
            if let Some(existing) = s
                .public_activations
                .iter()
                .find(|p| compare_names(&p.key, &key).is_eq())
            {
                return if existing.public_activation == package.public_activation {
                    Ok(())
                } else {
                    Err(Error::NativeInvariant(
                        "public activation identity disagreement",
                    ))
                };
            }
            s.public_activations.push(PublicActivationEntry {
                key,
                public_activation: package.public_activation,
                service_name: None,
                application_name: None,
            });
            Ok(())
        })
    }
    /// Outer Err is local input/admission/missing-model failure: no publication.
    /// Inner result is native lookup semantics (or an explicitly gated source
    /// invariant/capability boundary) and may retain a candidate.
    /// Modeled dependencies are recorded, never called as success-shaped stubs.
    pub fn lookup(&mut self, request: &LookupRequest, model: &LookupModel) -> Result<LookupResult> {
        self.change(|s| {
            let Some(public_activation) = s.public_activation(request, model)? else {
                return Ok(LookupResult::error(Error::TypeNotRegistered, None));
            };
            let id = TypeInstance {
                service_type: request.service_type.clone(),
                activation: request.activation,
                public_activation,
            };
            let state = match s.type_state(&id) {
                Ok(state) => state,
                Err(error) => return Ok(LookupResult::error(error, None)),
            };
            let sequence = Some(state.sequence);
            match state.status {
                TypeStatus::RegisteredEnabled => {
                    // No host-validity recheck here: selected runtime context is
                    // the native lookup producer's operational authority.
                    let runtime = match s.runtime(&state.runtime_id) {
                        Ok(runtime) => runtime,
                        Err(error) => return Ok(LookupResult::error(error, sequence)),
                    };
                    let candidate = Registration::from_context(id, runtime);
                    let result = if candidate.version == request.version {
                        Ok(())
                    } else {
                        let outcome = model
                            .deactivation
                            .as_ref()
                            .ok_or(Error::MissingModelOutcome)?;
                        let result = if outcome.has_replica() {
                            Err(Error::RegistrationVersionMismatch)
                        } else {
                            Err(Error::NotFound)
                        };
                        s.obligations.push(Obligation::DeactivationIfUnused {
                            package: candidate.type_instance.package_instance(),
                            outcome: outcome.clone(),
                        });
                        result
                    };
                    Ok(LookupResult {
                        result,
                        candidate: Some(candidate),
                        sequence,
                    })
                }
                TypeStatus::NotRegisteredDisabled => {
                    Ok(LookupResult::error(Error::TypeDisabled, sequence))
                }
                TypeStatus::InProgressEnabled | TypeStatus::InProgressDisabled => {
                    Ok(LookupResult::error(Error::TypeNotRegistered, sequence))
                }
                TypeStatus::NotRegisteredEnabled | TypeStatus::NotRegisteredDisableScheduled => {
                    if request.service_type.package.application == ApplicationId::default() {
                        // Native adhoc blocklist producer is explicitly not enabled.
                        return Ok(LookupResult::error(Error::UnsupportedCapability, sequence));
                    }
                    if !request.should_activate {
                        return Ok(LookupResult::error(Error::TypeNotRegistered, sequence));
                    }
                    let outcome = model
                        .activation
                        .as_ref()
                        .ok_or(Error::MissingModelOutcome)?;
                    let activation = ActivationRequest {
                        type_instance: id,
                        requested_version: request.version.clone(),
                        application_name: request.application_name.clone(),
                        sequence: state.sequence,
                    };
                    let (error, pending) = match outcome {
                        ActivationOutcome::Synchronous(Ok(())) => (Error::TypeNotRegistered, None),
                        ActivationOutcome::Synchronous(Err(error)) => (error.clone(), None),
                        ActivationOutcome::Pending => {
                            let id = s.id()?;
                            s.pending_activations.push(PendingActivation {
                                id,
                                request: activation.clone(),
                            });
                            (Error::DeploymentInProgress, Some(ActivationHandle(id)))
                        }
                    };
                    s.obligations.push(Obligation::Activation {
                        request: activation,
                        outcome: outcome.clone(),
                        pending,
                    });
                    Ok(LookupResult::error(error, sequence))
                }
            }
        })
    }

    /// Observe first request spelling for an exclusive rendered-CI public key.
    ///
    /// Shared resolution has no map entry; aliases retain the first inserted
    /// public ID and names (HostingSubsystem.cpp:22–180 at the pinned revision).
    pub fn lookup_public_activation(
        &self,
        package: &PackageId,
        activation: Activation,
    ) -> Result<Option<LookupPublicActivation<'_>>> {
        let key = public_activation_map_key(package, activation)?;
        Ok(self
            .state
            .public_activations
            .iter()
            .find(|p| compare_names(&p.key, &key).is_eq())
            .map(|p| LookupPublicActivation {
                rendered_key: &p.key,
                public_activation: &p.public_activation,
                service_name: p.service_name.as_deref(),
                application_name: p.application_name.as_deref(),
            }))
    }

    /// One pre-admitted lookup projection with actual bounded H2 scheduling.
    ///
    /// Activation/GUID assignment remain explicit controlled observations.
    /// On unequal registered versions, reserve the maximum closed scheduling
    /// outcome before invoking the dependency once. All H1 state/output limits
    /// are checked before that call; replacing its fixed-payload outcome and
    /// publishing thereafter is infallible. External mutation is never claimed
    /// rolled back by H1 admission. Equal versions and early admission failures
    /// do not invoke scheduling. Eligibility, timer admission and physical End
    /// remain separate; candidate and optional sequence survive native errors.
    ///
    /// Native disposition: FabricRuntimeManager.cpp:381–429 and
    /// ApplicationManager.cpp:1079–1093 at pin
    /// `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. Pre-reservation is local safety.
    pub fn lookup_with_scheduling(
        &mut self,
        request: &LookupRequest,
        model: &LookupModel,
        scheduling: &mut impl PackageScheduling,
    ) -> Result<LookupResult> {
        let previous_outputs = self.state.obligations.len();
        let mut prepared = Self {
            state: self.state.clone(),
            limits: self.limits.clone(),
        };
        let mut observations = model.clone();
        // Closed enum's largest representable outcome, not a success fallback.
        observations.deactivation = Some(SchedulingDisposition::SequenceOverflow.outcome());
        let mut result = prepared.lookup(request, &observations)?;
        if let Some(Obligation::DeactivationIfUnused { package, outcome }) =
            prepared.state.obligations.get_mut(previous_outputs)
        {
            let actual = scheduling
                .schedule_deactivation_if_unused(package)
                .outcome();
            result.result = Err(if actual.has_replica() {
                Error::RegistrationVersionMismatch
            } else {
                Error::NotFound
            });
            *outcome = actual;
        }
        // No fallible operation after the external call; the closed result fits
        // the already checked max-size obligation. Core is serialized, not locked.
        self.state = prepared.state;
        Ok(result)
    }
}
