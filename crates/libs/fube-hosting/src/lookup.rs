//! FabricRuntimeManager.cpp:117-249,296-458; HostingSubsystem.cpp:64-180.
//! Candidate/error/sequence are independent, including error with a candidate.
use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupRequest {
    pub service_type: ServiceTypeId,
    pub version: VersionInstance,
    pub activation: Activation,
    pub service_name: String,
    pub application_name: String,
    pub should_activate: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
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
impl State {
    fn public_activation(
        &mut self,
        request: &LookupRequest,
        model: &LookupModel,
    ) -> Result<Option<String>> {
        if request.activation == Activation::SHARED {
            return Ok(Some(String::new()));
        }
        if let Some(p) = self.public_activations.iter().find(|p| {
            compare_packages(&p.package, &request.service_type.package).is_eq()
                && p.activation == request.activation
        }) {
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
        self.public_activations.push(PackageInstance {
            package: request.service_type.package.clone(),
            activation: request.activation,
            public_activation: public.clone(),
        });
        Ok(Some(public.clone()))
    }
}
impl Hosting {
    /// Explicit controlled identity mapping for model-only exclusive scenarios.
    /// Equal package/context cannot be assigned a second public identity.
    pub fn bind_public_activation(&mut self, package: PackageInstance) -> Result<()> {
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
                .find(|p| compare_package_instances(p, &package).is_eq())
            {
                return if existing.public_activation == package.public_activation {
                    Ok(())
                } else {
                    Err(Error::NativeInvariant(
                        "public activation identity disagreement",
                    ))
                };
            }
            s.public_activations.push(package);
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
}
