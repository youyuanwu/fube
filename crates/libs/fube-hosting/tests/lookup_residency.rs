//! H2 processor-derived lookup scenarios, not native execution.
//! Native pin 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d:
//! FabricRuntimeManager.cpp:381-429, ApplicationManager.cpp:1079-1093,
//! Deactivator.cpp:113-145,912-969. Capacity cases are local Rust safety.
mod common;
use common::*;
use fube_hosting::*;

fn fixture(limits: Limits) -> (Hosting, RuntimeContext, TypeInstance, LookupRequest) {
    let mut h = Hosting::new(limits).unwrap();
    completed_host(&mut h, "Host");
    let c = valid_runtime(&mut h, "Runtime", "Host");
    let id = registered(&mut h, "Widget", &c);
    h.take_obligations();
    let mut input = request(&id, c.version.clone());
    input.version.application.minor += 1;
    (h, c, id, input)
}

fn residency(c: &RuntimeContext, grace: GracePeriod) -> Residency {
    let mut r = Residency::new(
        ResidencyLimits::default(),
        ResidencyConfig {
            shared_grace: grace,
            ..ResidencyConfig::default()
        },
        &ManualClock(0),
    )
    .unwrap();
    r.admit_package(ResidencyAdmission {
        package: c.package.clone(),
        version: c.version.clone(),
        package_instance_id: c.package_instance_sequence,
        service_name: "fabric:/app/service".into(),
        application_name: "fabric:/app".into(),
    })
    .unwrap();
    r
}

#[test]
fn actual_zero_usage_infinite_grace_is_mismatch_with_candidate_and_sequence() {
    let (mut h, c, id, input) = fixture(Limits::default());
    let mut r = residency(&c, GracePeriod::Infinite);
    let result = H2LookupAdapter::new(&mut h, &LookupModel::default(), &mut r)
        .lookup(&input)
        .unwrap();
    assert_eq!(result.result, Err(Error::RegistrationVersionMismatch));
    assert_eq!(result.candidate, Some(Registration::from_context(id, &c)));
    assert_eq!(result.sequence, Some(h.type_entries()[0].sequence));
    assert_eq!(r.package_accounting(&c.package).unwrap().usage, 0);
    assert_eq!(r.pending_timers().count(), 0);
}

#[test]
fn actual_timer_admission_failure_is_notfound_without_erasing_candidate() {
    for debug in [false, true] {
        let (mut h, c, id, input) = fixture(Limits::default());
        let mut r = residency(&c, GracePeriod::Finite(10));
        if debug {
            r.set_debug_lock(&c.package, true).unwrap();
        } else {
            r.close_timer_queue();
        }
        let result = h
            .lookup_with_scheduling(&input, &LookupModel::default(), &mut r)
            .unwrap();
        assert_eq!(result.result, Err(Error::NotFound));
        assert_eq!(result.candidate, Some(Registration::from_context(id, &c)));
        assert!(result.sequence.is_some());
        assert_eq!(r.pending_timers().count(), 0);
        assert_eq!(
            r.package_accounting(&c.package)
                .unwrap()
                .last_schedule
                .unwrap()
                .admission,
            if debug {
                TimerAdmission::DebugSuppressed
            } else {
                TimerAdmission::QueueClosed
            }
        );
        assert!(matches!(
            h.take_obligations().as_slice(),
            [Obligation::DeactivationIfUnused {
                outcome: DeactivationOutcome::Success { scheduled: true },
                ..
            }]
        ));
    }
}

#[test]
fn actual_missing_or_closing_accounting_returns_notfound_with_runtime_snapshot() {
    let (mut h, c, _, input) = fixture(Limits::default());
    let mut r = Residency::new(
        ResidencyLimits::default(),
        ResidencyConfig::default(),
        &ManualClock(0),
    )
    .unwrap();
    let missing = h
        .lookup_with_scheduling(&input, &LookupModel::default(), &mut r)
        .unwrap();
    assert_eq!(missing.result, Err(Error::NotFound));
    assert!(missing.candidate.is_some() && missing.sequence.is_some());
    let mut r = residency(&c, GracePeriod::Finite(0));
    assert_eq!(
        r.schedule_deactivation_if_unused(&c.package),
        SchedulingDisposition::Success { scheduled: true }
    );
    let callback = r.dequeue_due().unwrap().unwrap();
    assert!(matches!(
        r.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    let closing = h
        .lookup_with_scheduling(&input, &LookupModel::default(), &mut r)
        .unwrap();
    assert_eq!(closing.result, Err(Error::NotFound));
    assert_eq!(closing.candidate, missing.candidate);
    assert!(matches!(
        h.take_obligations().last().unwrap(),
        Obligation::DeactivationIfUnused {
            outcome: DeactivationOutcome::Failure(Error::InvalidState),
            ..
        }
    ));
}

struct CountingScheduling {
    calls: usize,
    result: SchedulingDisposition,
}
impl PackageScheduling for CountingScheduling {
    fn schedule_deactivation_if_unused(&mut self, _: &PackageInstance) -> SchedulingDisposition {
        self.calls += 1; // Observable external mutation cannot be H1-rolled back.
        self.result
    }
}

#[test]
fn max_bounded_outcome_is_reserved_before_one_mutating_call() {
    let p = type_id("Widget").package_instance();
    let maximum = 128
        + p.package.application.type_name.len()
        + p.package.name.len()
        + p.public_activation.len()
        + SchedulingDisposition::MAX_OUTCOME_BYTES;
    for (budget, calls) in [(maximum - 1, 0), (maximum, 1)] {
        let (mut h, _, _, input) = fixture(Limits {
            obligation_bytes: budget,
            ..Limits::default()
        });
        let sequence = h.type_entries()[0].sequence;
        let mut s = CountingScheduling {
            calls: 0,
            result: SchedulingDisposition::SequenceOverflow,
        };
        let result = h.lookup_with_scheduling(&input, &LookupModel::default(), &mut s);
        assert_eq!(s.calls, calls);
        if calls == 0 {
            assert_eq!(result, Err(Error::Capacity));
            assert!(h.obligations().is_empty());
        } else {
            let result = result.unwrap();
            assert_eq!(result.result, Err(Error::NotFound));
            assert!(result.candidate.is_some());
            assert_eq!(result.sequence, Some(sequence));
            assert!(matches!(
                h.obligations(),
                [Obligation::DeactivationIfUnused {
                    outcome: DeactivationOutcome::Failure(Error::NativeInvariant(
                        "package accounting sequence overflow"
                    )),
                    ..
                }]
            ));
        }
        assert_eq!(h.type_entries()[0].sequence, sequence);
    }
}

#[test]
fn equal_version_does_not_call_scheduling_or_invent_activation_premises() {
    let (mut h, c, _, mut input) = fixture(Limits::default());
    input.version = c.version;
    let mut s = CountingScheduling {
        calls: 0,
        result: SchedulingDisposition::InvalidState,
    };
    let result = h
        .lookup_with_scheduling(&input, &LookupModel::default(), &mut s)
        .unwrap();
    assert_eq!(result.result, Ok(()));
    assert_eq!(s.calls, 0);
    assert!(h.obligations().is_empty());
}

#[test]
fn rendered_ci_map_retains_first_request_names_and_binding_absence() {
    let mut h = hosting();
    let mut id = type_id("Widget");
    id.activation = Activation([1; 16]);
    let first = request(&id, VersionInstance::default());
    let public = "11111111-1111-1111-1111-111111111111".to_owned();
    let model = LookupModel {
        public_activation_assignment: Some(public.clone()),
        activation: Some(ActivationOutcome::Pending),
        ..LookupModel::default()
    };
    h.lookup(&first, &model).unwrap();
    let mut alias = first.clone();
    alias.service_type.package.application.type_name = "app".into();
    alias.service_name = "fabric:/new/service".into();
    alias.application_name = "fabric:/new".into();
    h.lookup(&alias, &model).unwrap();
    let mapping = h
        .lookup_public_activation(&alias.service_type.package, alias.activation)
        .unwrap()
        .unwrap();
    assert_eq!(mapping.service_name, Some(first.service_name.as_str()));
    assert_eq!(
        mapping.application_name,
        Some(first.application_name.as_str())
    );
    assert_eq!(mapping.public_activation, public);

    let mut bound = hosting();
    id.public_activation = public;
    bound.bind_public_activation(id.package_instance()).unwrap();
    bound.lookup(&first, &model).unwrap();
    let mapping = bound
        .lookup_public_activation(&id.service_type.package, id.activation)
        .unwrap()
        .unwrap();
    assert_eq!(mapping.service_name, None);
    assert_eq!(mapping.application_name, None);
}
