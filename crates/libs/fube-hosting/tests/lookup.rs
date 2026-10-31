//! FabricRuntimeManager.cpp:117-249,296-458 and ApplicationManager.cpp:1079-1093.
//! Processor/helper-derived exact combinations, not native handler execution.
//! Equal-version and independently changed application/package version outcomes
//! adapt NonActivatedApplicationHost.Test.cpp:350-374 under explicit H1 package
//! premises; candidate fields/scheduling combinations are additional derived tests.
mod common;
use common::*;
use fube_hosting::*;

#[test]
fn lookup_request_vs_event_stored_identity_and_each_runtime_producer() {
    let mut h = hosting();
    completed_host(&mut h, "Host");
    let c = valid_runtime(&mut h, "Runtime", "Host");
    let stored = declared(&mut h, "Widget");
    let mut finish = c.clone();
    finish.runtime_id = "runtime".into();
    finish.host_id = "host".into();
    finish.process_id = 88;
    finish.code_package = "FinishCode".into();
    finish.package_instance_sequence = 456;
    assert_eq!(h.register_type(&stored, &finish).result, Ok(()));
    let events = h.take_closed_registered_events();
    let [ClosedRegisteredEvent::Registered {
        registration: event,
        ..
    }] = events.as_slice()
    else {
        panic!("event");
    };
    let event = event.clone();
    let mut requested = stored.clone();
    requested.service_type.name = "widget".into();
    requested.service_type.package.name = "pkg".into();
    let result = h
        .lookup(
            &request(&requested, c.version.clone()),
            &LookupModel::default(),
        )
        .unwrap();
    assert_eq!(result.result, Ok(()));
    let candidate = result.candidate.unwrap();
    assert_eq!(candidate.type_instance, requested);
    assert_eq!(candidate.runtime_id, "Runtime");
    assert_eq!(candidate.host_id, "Host");
    assert_eq!(candidate.process_id, 42);
    assert_eq!(candidate.code_package, "Code");
    assert_eq!(candidate.package_instance_sequence, 123);
    assert_eq!(event.type_instance, stored);
    assert_eq!(event.runtime_id, "runtime");
    assert_eq!(event.host_id, "host");
    assert_eq!(event.process_id, 88);
    assert_eq!(event.code_package, "FinishCode");
    assert_eq!(event.package_instance_sequence, 456);
    assert!(h.obligations().is_empty()); // equal version skips deactivation
}
#[test]
fn version_matrix_retains_candidate_and_translates_actual_scheduling_outcomes() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = registered(&mut h, "Widget", &c);
    let expected_sequence = h.type_entries()[0].sequence;
    for changed in ["application", "package", "instance"] {
        let mut version = c.version.clone();
        match changed {
            "application" => version.application.minor += 1,
            "package" => version.package.minor += 1,
            _ => version.instance += 1,
        }
        for (outcome, expected) in [
            (
                DeactivationOutcome::Success { scheduled: true },
                Error::NotFound,
            ),
            (
                DeactivationOutcome::Success { scheduled: false },
                Error::RegistrationVersionMismatch,
            ),
            (
                DeactivationOutcome::Failure(Error::InvalidState),
                Error::NotFound,
            ),
            (
                DeactivationOutcome::Failure(Error::NotFound),
                Error::NotFound,
            ),
            (
                DeactivationOutcome::Failure(Error::Dependency(-100)),
                Error::NotFound,
            ),
        ] {
            h.take_obligations();
            let result = h
                .lookup(
                    &request(&id, version.clone()),
                    &LookupModel {
                        deactivation: Some(outcome.clone()),
                        ..LookupModel::default()
                    },
                )
                .unwrap();
            assert_eq!(result.result, Err(expected), "{changed}");
            assert_eq!(result.sequence, Some(expected_sequence));
            assert_eq!(result.candidate.as_ref().unwrap().version, c.version);
            assert!(
                matches!(h.obligations(), [Obligation::DeactivationIfUnused { package, outcome: actual }]
                if *package == id.package_instance() && *actual == outcome)
            );
        }
    }
    // "scheduled=false" deliberately also models usage=0/infinite grace, not
    // a fabricated usage-count predicate. H2 must implement the actual premise.
}
#[test]
fn activation_sync_success_pending_and_specific_failure_preserve_sequence() {
    for outcome in [
        ActivationOutcome::Synchronous(Ok(())),
        ActivationOutcome::Pending,
        ActivationOutcome::Synchronous(Err(Error::Dependency(-99))),
    ] {
        let mut h = hosting();
        let id = declared(&mut h, "Widget");
        let sequence = h.type_entries()[0].sequence;
        let r = request(&id, VersionInstance::default());
        let result = h
            .lookup(
                &r,
                &LookupModel {
                    activation: Some(outcome.clone()),
                    ..LookupModel::default()
                },
            )
            .unwrap();
        assert_eq!(result.sequence, Some(sequence));
        assert!(result.candidate.is_none());
        let expected = match &outcome {
            ActivationOutcome::Synchronous(Ok(())) => Error::TypeNotRegistered,
            ActivationOutcome::Pending => Error::DeploymentInProgress,
            ActivationOutcome::Synchronous(Err(e)) => e.clone(),
        };
        assert_eq!(result.result, Err(expected));
        let [Obligation::Activation {
            request: observed,
            outcome: actual,
            pending,
        }] = h.obligations()
        else {
            panic!("activation observed");
        };
        assert_eq!(actual, &outcome);
        assert_eq!(observed.type_instance, id);
        assert_eq!(observed.requested_version, r.version);
        assert_eq!(observed.application_name, r.application_name);
        assert_eq!(observed.sequence, sequence);
        assert_eq!(pending.is_some(), outcome == ActivationOutcome::Pending);
        assert_eq!(h.type_entries()[0].status, TypeStatus::NotRegisteredEnabled);
    }
}
#[test]
fn lookup_all_selected_statuses_no_candidate_no_unrequested_activation() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = declared(&mut h, "Widget");
    let mut r = request(&id, c.version.clone());
    r.should_activate = false;
    let result = h.lookup(&r, &LookupModel::default()).unwrap();
    assert_eq!(result.result, Err(Error::TypeNotRegistered));
    assert!(h.obligations().is_empty());
    let timer = disable(&mut h, &id);
    assert_eq!(
        h.lookup(&r, &LookupModel::default()).unwrap().result,
        Err(Error::TypeNotRegistered)
    );
    h.disable_timer_input(&timer).unwrap();
    r.should_activate = true;
    assert_eq!(
        h.lookup(&r, &LookupModel::default()).unwrap().result,
        Err(Error::TypeDisabled)
    );
    h.start_type_registration(&id, &c).unwrap();
    assert_eq!(
        h.lookup(&r, &LookupModel::default()).unwrap().result,
        Err(Error::TypeNotRegistered)
    );
    h.finish_type_registration(&id, false, &c).unwrap();
    h.unregister_failure("failure").unwrap();
    h.start_type_registration(&id, &c).unwrap();
    assert_eq!(
        h.lookup(&r, &LookupModel::default()).unwrap().result,
        Err(Error::TypeNotRegistered)
    );
    assert!(h.obligations().is_empty());
}
#[test]
fn synthetic_sequence_missing_entry_and_closed_dependency_write_boundaries() {
    let mut h = hosting();
    let mut r = request(&type_id("Missing"), VersionInstance::default());
    r.should_activate = false;
    let first = h.lookup(&r, &LookupModel::default()).unwrap();
    let second = h.lookup(&r, &LookupModel::default()).unwrap();
    assert!(first.sequence.is_some());
    assert!(second.sequence > first.sequence);
    assert!(h.type_entries().is_empty());
    h.close_type_manager().unwrap();
    let result = h.lookup(&r, &LookupModel::default()).unwrap();
    assert_eq!(result.result, Err(Error::ObjectClosed));
    assert_eq!(result.sequence, None);
    assert!(result.candidate.is_none());
    // Nonactivating exclusive resolver misses BEFORE state query/sequence.
    r.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    let result = h.lookup(&r, &LookupModel::default()).unwrap();
    assert_eq!(result.result, Err(Error::TypeNotRegistered));
    assert_eq!(result.sequence, None);
}
#[test]
fn registered_missing_tentative_or_closed_runtime_keeps_sequence_but_no_candidate() {
    for closed in [false, true] {
        let mut h = hosting();
        completed_host(&mut h, "H");
        let c = valid_runtime(&mut h, "R", "H");
        let id = registered(&mut h, "Widget", &c);
        let sequence = h.type_entries()[0].sequence;
        if closed {
            h.close_runtime_table().unwrap();
        } else {
            h.remove_runtime_entry("R").unwrap();
        }
        let result = h
            .lookup(&request(&id, c.version.clone()), &LookupModel::default())
            .unwrap();
        assert_eq!(
            result.result,
            Err(if closed {
                Error::ObjectClosed
            } else {
                Error::RuntimeNotRegistered
            })
        );
        assert_eq!(result.sequence, Some(sequence));
        assert!(result.candidate.is_none());
        if !closed {
            h.add_runtime(c.clone()).unwrap();
            assert_eq!(
                h.lookup(&request(&id, c.version.clone()), &LookupModel::default())
                    .unwrap()
                    .result,
                Err(Error::RuntimeNotRegistered)
            );
        }
    }
}
#[test]
fn lookup_has_no_extra_host_validity_check() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = registered(&mut h, "Widget", &c);
    h.remove_host_entry("H").unwrap(); // not full host closure
    assert_eq!(
        h.lookup(&request(&id, c.version), &LookupModel::default())
            .unwrap()
            .result,
        Ok(())
    );
}
#[test]
fn late_activation_end_after_registration_or_close_does_not_rewrite_lookup() {
    let mut h = hosting();
    let id = declared(&mut h, "Widget");
    let result = h
        .lookup(
            &request(&id, VersionInstance::default()),
            &LookupModel {
                activation: Some(ActivationOutcome::Pending),
                ..LookupModel::default()
            },
        )
        .unwrap();
    let obligations = h.take_obligations();
    let [Obligation::Activation {
        pending: Some(handle),
        ..
    }] = obligations.as_slice()
    else {
        panic!("pending");
    };
    let handle = *handle;
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    h.register_type(&id, &c).result.unwrap();
    let closed = h.close_core().unwrap();
    assert_eq!(closed.pending_activations, 1);
    h.complete_activation(handle, Err(Error::Dependency(-33)))
        .unwrap();
    assert_eq!(h.pending_activation_count(), 0);
    assert_eq!(result.result, Err(Error::DeploymentInProgress));
    assert!(h.type_entries().is_empty());
    assert!(matches!(
        h.obligations(),
        [Obligation::ActivationCompleted {
            result: Err(Error::Dependency(-33)),
            ..
        }]
    ));
    assert_eq!(
        h.complete_activation(handle, Ok(())),
        Err(Error::InvalidState)
    );
}
#[test]
fn scoped_public_activation_mapping_is_stable_not_an_extra_collision_dimension() {
    let mut h = hosting();
    let mut id = type_id("Widget");
    id.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    id.public_activation = uuid::Uuid::from_u128(2).to_string();
    h.declare_type(id.clone(), "component".into(), Ok(()))
        .unwrap();
    let r = request(&id, VersionInstance::default());
    let result = h
        .lookup(
            &r,
            &LookupModel {
                public_activation_assignment: Some(id.public_activation.clone()),
                activation: Some(ActivationOutcome::Synchronous(Ok(()))),
                ..LookupModel::default()
            },
        )
        .unwrap();
    assert_eq!(result.result, Err(Error::TypeNotRegistered));
    let mut nonactivating = r;
    nonactivating.should_activate = false;
    let result = h.lookup(&nonactivating, &LookupModel::default()).unwrap();
    assert_eq!(result.sequence, Some(h.type_entries()[0].sequence));
    let mut wrong = id.package_instance();
    wrong.public_activation = uuid::Uuid::from_u128(3).to_string();
    assert_eq!(
        h.bind_public_activation(wrong),
        Err(Error::NativeInvariant(
            "public activation identity disagreement"
        ))
    );
}
#[test]
fn required_outcomes_and_finite_activation_obligations_are_admission_not_fake_success() {
    let mut h = Hosting::new(Limits {
        operations: 1,
        obligations: 1,
        ..Limits::default()
    })
    .unwrap();
    let id = type_id("Missing");
    let r = request(&id, VersionInstance::default());
    assert_eq!(
        h.lookup(&r, &LookupModel::default()),
        Err(Error::MissingModelOutcome)
    );
    assert!(h.obligations().is_empty());
    assert!(h.type_entries().is_empty());
    let pending = LookupModel {
        activation: Some(ActivationOutcome::Pending),
        ..LookupModel::default()
    };
    h.lookup(&r, &pending).unwrap();
    let obligations = h.obligations().to_vec();
    assert_eq!(h.lookup(&r, &pending), Err(Error::Capacity));
    assert_eq!(h.obligations(), obligations);
    assert_eq!(h.pending_activation_count(), 1);
    let [Obligation::Activation {
        pending: Some(handle),
        ..
    }] = obligations.as_slice()
    else {
        panic!("pending");
    };
    assert_eq!(h.complete_activation(*handle, Ok(())), Err(Error::Capacity));
    assert_eq!(h.pending_activation_count(), 1);
    h.take_obligations();
    h.complete_activation(*handle, Ok(())).unwrap();
}
#[test]
fn deactivation_obligation_byte_veto_and_missing_outcome_preserve_state() {
    let mut h = Hosting::new(Limits {
        obligation_bytes: 1,
        ..Limits::default()
    })
    .unwrap();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = registered(&mut h, "Widget", &c);
    let mut r = request(&id, c.version.clone());
    r.version.instance += 1;
    let before = h.type_entries().to_vec();
    assert_eq!(
        h.lookup(&r, &LookupModel::default()),
        Err(Error::MissingModelOutcome)
    );
    assert_eq!(
        h.lookup(
            &r,
            &LookupModel {
                deactivation: Some(DeactivationOutcome::Success { scheduled: false }),
                ..LookupModel::default()
            }
        ),
        Err(Error::Capacity)
    );
    assert_eq!(h.type_entries(), before);
    assert!(h.obligations().is_empty());
}

#[test]
fn actual_registered_candidates_are_package_activation_and_host_scoped() {
    let mut h = hosting();
    completed_host(&mut h, "SharedHost");
    let shared = valid_runtime(&mut h, "SharedRuntime", "SharedHost");
    let shared_type = registered(&mut h, "Widget", &shared);
    completed_host(&mut h, "ExclusiveModelHost");
    let mut exclusive = context("ExclusiveRuntime", "ExclusiveModelHost");
    exclusive.package.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    exclusive.package.public_activation = uuid::Uuid::from_u128(2).to_string();
    exclusive.version.instance = 8;
    exclusive.process_id = 100;
    h.bind_public_activation(exclusive.package.clone()).unwrap();
    h.register_runtime(exclusive.clone()).result.unwrap();
    let mut exclusive_type = shared_type.clone();
    exclusive_type.activation = exclusive.package.activation;
    exclusive_type.public_activation = exclusive.package.public_activation.clone();
    h.declare_type(
        exclusive_type.clone(),
        "exclusive-model-component".into(),
        Ok(()),
    )
    .unwrap();
    h.register_type(&exclusive_type, &exclusive).result.unwrap();
    let first = h
        .lookup(
            &request(&shared_type, shared.version),
            &LookupModel::default(),
        )
        .unwrap();
    let second = h
        .lookup(
            &request(&exclusive_type, exclusive.version.clone()),
            &LookupModel::default(),
        )
        .unwrap();
    assert_eq!(first.candidate.unwrap().host_id, "SharedHost");
    let second = second.candidate.unwrap();
    assert_eq!(second.host_id, "ExclusiveModelHost");
    assert_eq!(second.process_id, 100);
    assert_eq!(second.type_instance, exclusive_type);
    h.unregister_host("SharedHost", "close").unwrap();
    assert_eq!(
        h.lookup(
            &request(&exclusive_type, exclusive.version),
            &LookupModel::default()
        )
        .unwrap()
        .result,
        Ok(())
    );
    // Exclusive records qualify identity-model isolation, NOT exclusive execution.
}
#[test]
fn finite_public_mapping_count_and_bytes_reject_without_partial_admission() {
    let mut h = Hosting::new(Limits {
        packages: 1,
        ..Limits::default()
    })
    .unwrap();
    let mut package = type_id("Widget").package_instance();
    package.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    package.public_activation = uuid::Uuid::from_u128(2).to_string();
    h.bind_public_activation(package.clone()).unwrap();
    package.activation = Activation::from_uuid(uuid::Uuid::from_u128(3));
    assert_eq!(
        h.bind_public_activation(package.clone()),
        Err(Error::Capacity)
    );
    let mut r = request(&type_id("Widget"), VersionInstance::default());
    r.activation = package.activation;
    r.should_activate = false;
    assert_eq!(
        h.lookup(&r, &LookupModel::default()).unwrap().sequence,
        None
    );
    let mut h = Hosting::new(Limits {
        state_bytes: 70,
        ..Limits::default()
    })
    .unwrap();
    assert_eq!(h.bind_public_activation(package), Err(Error::Capacity));
}

#[test]
fn closed_core_rejects_new_get_or_add_before_missing_model_or_assignment() {
    let mut h = hosting();
    h.close_core().unwrap();
    let mut r = request(&type_id("Widget"), VersionInstance::default());
    r.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    let mut nonactivating = r.clone();
    nonactivating.should_activate = false;
    let absent = h.lookup(&nonactivating, &LookupModel::default()).unwrap();
    assert_eq!(absent.result, Err(Error::TypeNotRegistered));
    assert_eq!(absent.sequence, None);
    for assignment in [None, Some(uuid::Uuid::from_u128(2).to_string())] {
        assert_eq!(
            h.lookup(
                &r,
                &LookupModel {
                    public_activation_assignment: assignment,
                    activation: Some(ActivationOutcome::Pending),
                    ..LookupModel::default()
                }
            ),
            Err(Error::ObjectClosed)
        );
        assert_eq!(
            h.lookup(&nonactivating, &LookupModel::default()).unwrap(),
            absent
        );
        assert_eq!(h.pending_activation_count(), 0);
        assert!(h.obligations().is_empty());
        assert!(h.type_entries().is_empty());
    }
}
