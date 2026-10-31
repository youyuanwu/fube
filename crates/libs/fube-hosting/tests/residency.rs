//! Native-derived deterministic H2 accounting, not native execution or deployment.
//! All native anchors pin service-fabric 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.
use fube_hosting::*;

fn package(name: &str, activation: u128) -> PackageInstance {
    PackageInstance {
        package: PackageId {
            application: ApplicationId {
                type_name: "Widget".into(),
                number: 7,
            },
            name: name.into(),
        },
        activation: Activation::from_uuid(uuid::Uuid::from_u128(activation)),
        public_activation: if activation == 0 {
            String::new()
        } else {
            uuid::Uuid::from_u128(activation + 100).to_string()
        },
    }
}

fn admission(package: PackageInstance) -> ResidencyAdmission {
    ResidencyAdmission {
        package,
        version: VersionInstance {
            application: Rollout { major: 3, minor: 2 },
            package: Rollout { major: 4, minor: 1 },
            instance: 19,
        },
        package_instance_id: 97,
        service_name: "fabric:/App/Service".into(),
        application_name: "fabric:/App".into(),
    }
}

fn target(package: &PackageInstance) -> TypeInstance {
    TypeInstance {
        service_type: ServiceTypeId {
            package: package.package.clone(),
            name: "Type".into(),
        },
        activation: package.activation,
        public_activation: package.public_activation.clone(),
    }
}

fn ledger(config: ResidencyConfig) -> Residency {
    Residency::new(ResidencyLimits::default(), config, &ManualClock(0)).unwrap()
}

fn config(shared: GracePeriod, exclusive: GracePeriod, scan: GracePeriod) -> ResidencyConfig {
    ResidencyConfig {
        shared_grace: shared,
        exclusive_grace: exclusive,
        unused_scan_interval: scan,
    }
}

fn finite() -> ResidencyConfig {
    config(
        GracePeriod::Finite(10),
        GracePeriod::Finite(2),
        GracePeriod::Finite(20),
    )
}

fn acquire(h: &mut Residency, p: &PackageInstance) -> Acquisition {
    match h.increment_usage(&target(p)) {
        UsageIncrement::Acquired(a) => a,
        other => panic!("unexpected acquisition: {other:?}"),
    }
}

fn release(h: &mut Residency, a: Acquisition) {
    assert!(matches!(h.decrement_usage(a), UsageRelease::Released));
}

fn due(h: &mut Residency, ticks: i64) -> DeactivationCallback {
    h.advance(&ManualClock(ticks)).unwrap();
    h.dequeue_due().unwrap().expect("due callback")
}

#[test]
fn first_last_use_and_exact_full_target() {
    // Processor-derived: Deactivator.cpp:37-103,330-394; local exact-owner adaptation.
    let p = package("Pkg", 0);
    let admitted = admission(p.clone());
    let mut h = ledger(finite());
    h.admit_package(admitted.clone()).unwrap();
    assert_eq!(h.package_accounting(&p).unwrap().unused_since, Some(0));
    h.advance(&ManualClock(3)).unwrap();
    let a = acquire(&mut h, &p);
    let b = acquire(&mut h, &p);
    assert_eq!(a.target().version, admitted.version);
    assert_eq!(a.target().package_instance_id, 97);
    assert_eq!(h.package_accounting(&p).unwrap().usage, 2);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .sequence,
        2
    );
    assert_eq!(h.package_accounting(&p).unwrap().unused_since, None);
    release(&mut h, a);
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(h.pending_timers().count(), 0);
    h.advance(&ManualClock(5)).unwrap();
    release(&mut h, b);
    let accounting = h.package_accounting(&p).unwrap();
    assert_eq!(
        (
            accounting.usage,
            accounting.sequence,
            accounting.unused_since
        ),
        (0, 4, Some(5))
    );
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .unused_since,
        Some(5)
    );
    assert_eq!(h.pending_timers().count(), 1); // Ordinary app last-use never schedules it.
    assert_eq!(h.pending_timers().next().unwrap().due_ticks, 15);
}

#[test]
fn application_aggregate_package_activation_and_native_spelling_isolation() {
    // Processor-derived: native ServicePackageInstanceIdentifier.cpp:74-109.
    let shared = package("Pkg", 0);
    let exclusive = package("Pkg", 1);
    let other = package("Other", 0);
    let mut h = ledger(finite());
    for p in [&shared, &exclusive, &other] {
        h.admit_package(admission(p.clone())).unwrap();
    }
    let mut equivalent = shared.clone();
    equivalent.package.name = "pKG".into();
    equivalent.package.application.type_name = "wIDGET".into();
    let a = acquire(&mut h, &equivalent);
    let b = acquire(&mut h, &exclusive);
    let c = acquire(&mut h, &other);
    assert_eq!(h.resources().packages, 3);
    assert_eq!(h.resources().applications, 1);
    assert_eq!(h.package_accounting(&shared).unwrap().identity, &shared);
    assert_eq!(
        h.application_accounting(&shared.package.application)
            .unwrap()
            .usage,
        3
    );
    release(&mut h, a);
    assert_eq!(h.package_accounting(&exclusive).unwrap().usage, 1);
    assert_eq!(h.package_accounting(&other).unwrap().usage, 1);
    release(&mut h, b);
    release(&mut h, c);
}

#[test]
fn rendered_ci_alias_preserves_public_map_first_metadata_and_conflicts() {
    // Native-source adaptation: HostingSubsystem.cpp:22-180, including NUL aliases.
    let first = package("Pkg", 1);
    let mut alias = first.clone();
    alias.package.name = "pKG".into();
    alias.package.application.type_name = "widget".into();
    let mut h = ledger(finite());
    h.admit_package(admission(first.clone())).unwrap();
    let mut next = admission(alias.clone());
    next.service_name.clear(); // Existing mapping does not require a new ServiceName.
    next.application_name = "fabric:/Changed".into();
    h.admit_package(next).unwrap();
    let map = h.public_activation_mapping(&alias).unwrap();
    assert_eq!(map.service_name, "fabric:/App/Service");
    assert_eq!(map.application_name, "fabric:/App");
    assert_eq!(map.public_activation, first.public_activation);
    alias.public_activation = uuid::Uuid::from_u128(999).to_string();
    assert!(matches!(
        h.admit_package(admission(alias)),
        Err(Error::NativeInvariant(_))
    ));

    let mut nul = package("Pkg", 2);
    nul.package.application.type_name = "Raw\0First".into();
    h.admit_package(admission(nul.clone())).unwrap();
    let mut distinct = nul.clone();
    distinct.package.application.type_name = "raw\0Other".into();
    distinct.package.name = "Other".into(); // Structured identity differs, rendered key aliases.
    h.admit_package(admission(distinct.clone())).unwrap();
    assert_eq!(
        h.public_activation_mapping(&distinct).unwrap().rendered_key,
        public_activation_map_key(&nul.package, nul.activation).unwrap()
    );
    assert_eq!(h.package_accounting(&distinct).unwrap().identity, &distinct);
}

#[test]
fn admission_is_not_activation_and_never_replaces_version_or_numeric_premise() {
    // Local accounting-only profile; version authority ServicePackageVersionInstance.cpp:99-107.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    let mut changed = admission(p.clone());
    changed.version.application.minor += 1;
    assert_eq!(h.admit_package(changed), Err(Error::InvalidState));
    let mut changed = admission(p.clone());
    changed.package_instance_id += 1;
    assert_eq!(h.admit_package(changed), Err(Error::InvalidState));
    assert_eq!(h.resources().packages, 1);
    assert!(matches!(
        h.increment_usage(&target(&package("Missing", 0))),
        UsageIncrement::Rejected(Error::NotFound)
    ));
    let adhoc = TypeInstance::default();
    assert!(matches!(
        h.increment_usage(&adhoc),
        UsageIncrement::Unmanaged
    ));
    assert_eq!(h.resources().acquisitions, 0);
}

#[test]
fn managed_admission_preserves_constructor_and_public_mapping_preconditions() {
    // Local admission for native assertions: ApplicationIdentifier.cpp:27-34,
    // ServicePackageIdentifier.cpp:42-49, HostingSubsystem.cpp:22-180.
    let mut h = ledger(finite());
    let original = h.resources();
    let mut invalid = package("Pkg", 0);
    invalid.package.application.type_name.clear();
    assert!(matches!(
        h.admit_package(admission(invalid)),
        Err(Error::NativeInvariant(_))
    ));
    let mut invalid = package("Pkg", 1);
    invalid.public_activation.clear();
    assert_eq!(
        h.admit_package(admission(invalid)),
        Err(Error::InvalidInput)
    );
    let mut invalid = admission(package("Pkg", 1));
    invalid.service_name.clear();
    assert!(matches!(
        h.admit_package(invalid),
        Err(Error::NativeInvariant(_))
    ));
    let mut adhoc = package("Pkg", 0);
    adhoc.package.application = ApplicationId::default();
    assert_eq!(
        h.admit_package(admission(adhoc)),
        Err(Error::UnsupportedCapability)
    );
    assert_eq!(h.resources(), original);
}

fn partial(h: &mut Residency, p: &PackageInstance) -> Acquisition {
    // Deterministic interleaving of Deactivator.cpp:337-365 and 504-527.
    h.schedule_application_timer(&p.package.application, 0)
        .unwrap();
    let callback = due(h, 0);
    let beginning = match h.begin_acquisition(&target(p)) {
        AcquisitionBegin::Beginning(a) => a,
        other => panic!("unexpected begin: {other:?}"),
    };
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    match h.finish_acquisition(beginning) {
        UsageIncrement::Unresolved {
            error: Error::InvalidState,
            ownership,
        } => ownership,
        other => panic!("unexpected finish: {other:?}"),
    }
}

#[test]
fn package_first_partial_failure_has_verified_package_only_compensation() {
    // Native partial branch has no rollback. Compensation here is explicitly local verified accounting.
    let p = package("Pkg", 0);
    let mut h = ledger(config(
        GracePeriod::Finite(0),
        GracePeriod::Finite(0),
        GracePeriod::Infinite,
    ));
    h.admit_package(admission(p.clone())).unwrap();
    let a = partial(&mut h, &p);
    let id = a.id();
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .usage,
        0
    );
    assert_eq!(
        h.acquisition(id).unwrap().phase,
        AcquisitionPhase::PackageOnly
    );
    let a = match h.decrement_usage(a) {
        UsageRelease::Unresolved { ownership, .. } => ownership,
        other => panic!("pair decrement cannot compensate package-only: {other:?}"),
    };
    assert_eq!(a.id(), id);
    assert!(matches!(
        h.compensate_package_only(a),
        UsageRelease::Released
    ));
    assert_eq!(h.package_accounting(&p).unwrap().usage, 0);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .sequence,
        0
    );
    assert!(h.acquisition(id).is_none());
    assert_eq!(h.final_obligations().count(), 1); // Still no physical application End.
}

#[test]
fn beginning_drop_and_repeated_finish_cannot_discard_or_duplicate_usage() {
    // Local nonclone ownership adaptation, not native destructor cleanup.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    let a = match h.begin_acquisition(&target(&p)) {
        AcquisitionBegin::Beginning(a) => a,
        other => panic!("{other:?}"),
    };
    let id = a.id();
    drop(a);
    assert_eq!(
        h.acquisition(id).unwrap().phase,
        AcquisitionPhase::Beginning
    );
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .usage,
        0
    );
    let a = acquire(&mut h, &p);
    let a = match h.finish_acquisition(a) {
        UsageIncrement::Unresolved { ownership, .. } => ownership,
        other => panic!("already finished must not increment twice: {other:?}"),
    };
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .usage,
        1
    );
    release(&mut h, a);
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
}

#[test]
fn unresolved_owner_remains_exact_once_only_across_shutdown() {
    // Local retention strengthens native void decrement and native Close clearing.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    let a = acquire(&mut h, &p);
    let id = a.id();
    let original = a.target().clone();
    h.shutdown();
    let mut a = a;
    for _ in 0..2 {
        a = match h.decrement_usage(a) {
            UsageRelease::Unresolved {
                ownership,
                diagnostic: Some(Error::ObjectClosed),
            } => ownership,
            other => panic!("{other:?}"),
        };
        assert_eq!(a.id(), id);
        assert_eq!(a.target(), &original);
        assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    }
    drop(a);
    assert_eq!(h.resources().acquisitions, 1);
    assert_eq!(h.acquisition(id).unwrap().phase, AcquisitionPhase::Paired);
    assert!(matches!(
        h.increment_usage(&target(&p)),
        UsageIncrement::Rejected(Error::ObjectClosed)
    ));
    assert_eq!(
        h.admit_package(admission(package("New", 0))),
        Err(Error::ObjectClosed)
    );
}

#[test]
fn foreign_owner_is_returned_without_touching_either_ledger() {
    // Local exact ownership isolation, no raw IDs authorize decrement.
    let p = package("Pkg", 0);
    let mut a = ledger(finite());
    let mut b = ledger(finite());
    a.admit_package(admission(p.clone())).unwrap();
    b.admit_package(admission(p.clone())).unwrap();
    let token = acquire(&mut a, &p);
    let token = match b.decrement_usage(token) {
        UsageRelease::Unresolved { ownership, .. } => ownership,
        other => panic!("{other:?}"),
    };
    assert_eq!(a.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(b.package_accounting(&p).unwrap().usage, 0);
    release(&mut a, token);
}

#[test]
fn infinite_grace_preserves_unused_ineligibility_and_scan_suppression() {
    // Deactivator.cpp:113-145,252-257,865-875; lookup must see false, not count-only eligibility.
    let p = package("Pkg", 0);
    let mut h = ledger(config(
        GracePeriod::Infinite,
        GracePeriod::Infinite,
        GracePeriod::Infinite,
    ));
    h.admit_package(admission(p.clone())).unwrap();
    let a = acquire(&mut h, &p);
    release(&mut h, a);
    assert_eq!(
        h.schedule_deactivation_if_unused(&p),
        SchedulingDisposition::Success { scheduled: false }
    );
    h.advance(&ManualClock(100)).unwrap();
    assert_eq!(h.scan_unused().unwrap(), 0);
    assert_eq!(h.pending_timers().count(), 0);
}

#[test]
fn current_full_grace_shared_exclusive_and_package_debug_are_independent() {
    // Deactivator.cpp:252-257,912-969,1264-1313. Application has no debug lock check.
    let shared = package("Pkg", 0);
    let exclusive = package("Pkg", 1);
    let mut h = ledger(finite());
    h.admit_package(admission(shared.clone())).unwrap();
    h.admit_package(admission(exclusive.clone())).unwrap();
    h.advance(&ManualClock(100)).unwrap();
    assert_eq!(
        h.schedule_deactivation_if_unused(&exclusive),
        SchedulingDisposition::Success { scheduled: true }
    );
    assert_eq!(h.pending_timers().next().unwrap().due_ticks, 102);
    h.set_debug_lock(&shared, true).unwrap();
    assert_eq!(
        h.schedule_deactivation_if_unused(&shared),
        SchedulingDisposition::Success { scheduled: true }
    );
    assert_eq!(
        h.package_accounting(&shared)
            .unwrap()
            .last_schedule
            .unwrap()
            .admission,
        TimerAdmission::DebugSuppressed
    );
    h.set_config(config(
        GracePeriod::Finite(30),
        GracePeriod::Finite(4),
        GracePeriod::Finite(20),
    ))
    .unwrap();
    h.schedule_application_timer(&shared.package.application, 0)
        .unwrap();
    assert!(h.pending_timers().any(|t| t.due_ticks == 130)); // Full current grace, not elapsed subtraction.
    assert_eq!(h.pending_timers().count(), 2);
}

#[test]
fn scan_selects_application_first_and_guard_allows_remaining_packages() {
    // Deactivator.cpp:445-488,504-527,1333-1344: no invented empty-package-map guard.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    h.advance(&ManualClock(20)).unwrap(); // Native unused-since <= Now - interval.
    assert_eq!(h.scan_unused().unwrap(), 1);
    let callback = due(&mut h, 30);
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    assert!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .closing
    );
    assert_eq!(h.resources().packages, 1);
    assert_eq!(
        h.schedule_deactivation_if_unused(&p),
        SchedulingDisposition::InvalidState
    );
    assert_eq!(h.final_obligations().count(), 1);
}

#[test]
fn scan_packages_when_application_is_used_and_application_infinity_does_not_fall_through() {
    // Processor-derived independent application/package unused scan: Deactivator.cpp:445-488.
    let used = package("Used", 0);
    let unused = package("Unused", 1);
    let mut h = ledger(finite());
    h.admit_package(admission(used.clone())).unwrap();
    h.admit_package(admission(unused.clone())).unwrap();
    let a = acquire(&mut h, &used);
    h.advance(&ManualClock(21)).unwrap();
    assert_eq!(h.scan_unused().unwrap(), 1);
    assert_eq!(h.pending_timers().next().unwrap().due_ticks, 23);
    release(&mut h, a);

    let mut h = ledger(config(
        GracePeriod::Infinite,
        GracePeriod::Finite(2),
        GracePeriod::Finite(20),
    ));
    h.admit_package(admission(unused)).unwrap();
    h.advance(&ManualClock(21)).unwrap();
    assert_eq!(h.scan_unused().unwrap(), 1); // Application selected, scheduling suppressed by infinity.
    assert_eq!(h.pending_timers().count(), 0);
}

#[test]
fn stale_dequeued_callback_cannot_remove_newer_timer_or_close_reacquired_package() {
    // Deactivator.cpp:735-815,947-952,1078-1100: queue removal and execution are independent.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    h.schedule_deactivation_if_unused(&p);
    let stale = due(&mut h, 10);
    let a = acquire(&mut h, &p);
    release(&mut h, a);
    let current = h.pending_timers().next().unwrap().captured_sequence;
    assert_eq!(current, 2);
    assert!(!h.cancel_package_timer(&p, 0).unwrap());
    assert!(matches!(
        h.execute_callback(stale),
        CallbackExecution::Stale
    ));
    assert_eq!(h.pending_timers().count(), 1);
    assert!(!h.package_accounting(&p).unwrap().closing);
    let callback = due(&mut h, 20);
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    assert!(h.package_accounting(&p).unwrap().closing);
    assert_eq!(h.resources().packages, 1); // Final obligation does not perform physical End/removal.
}

#[test]
fn strict_queue_replacement_equal_deadline_and_less_equal_removal() {
    // Native queue algorithm: Deactivator.cpp:735-815; supplied sequences are captured observations.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    assert!(matches!(
        h.schedule_package_timer(&p, 2).unwrap(),
        TimerAdmission::Admitted { .. }
    ));
    h.advance(&ManualClock(3)).unwrap();
    assert_eq!(
        h.schedule_package_timer(&p, 2).unwrap(),
        TimerAdmission::AlreadyExists
    );
    assert_eq!(
        h.schedule_package_timer(&p, 1).unwrap(),
        TimerAdmission::AlreadyExists
    );
    assert_eq!(h.pending_timers().next().unwrap().due_ticks, 10);
    assert!(matches!(
        h.schedule_package_timer(&p, 3).unwrap(),
        TimerAdmission::Admitted { .. }
    ));
    assert_eq!(h.pending_timers().next().unwrap().due_ticks, 13);
    assert!(!h.cancel_package_timer(&p, 2).unwrap());
    assert!(h.cancel_package_timer(&p, 3).unwrap());
    assert_eq!(h.pending_timers().count(), 0);
}

#[test]
fn token_count_and_bytes_reserve_before_package_first_mutation() {
    // Local finite admission, not a native timer-map/count limit.
    let p = package("Pkg", 0);
    let limits = ResidencyLimits {
        acquisitions: 1,
        ..ResidencyLimits::default()
    };
    let mut h = Residency::new(limits, finite(), &ManualClock(0)).unwrap();
    h.admit_package(admission(p.clone())).unwrap();
    drop(acquire(&mut h, &p));
    assert!(matches!(
        h.increment_usage(&target(&p)),
        UsageIncrement::Rejected(Error::Capacity)
    ));
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .usage,
        1
    );

    let mut measure = ledger(finite());
    measure.admit_package(admission(p.clone())).unwrap();
    let bytes = measure.resources().logical_bytes;
    let limits = ResidencyLimits {
        state_bytes: bytes,
        ..ResidencyLimits::default()
    };
    let mut h = Residency::new(limits, finite(), &ManualClock(0)).unwrap();
    h.admit_package(admission(p.clone())).unwrap();
    assert!(matches!(
        h.increment_usage(&target(&p)),
        UsageIncrement::Rejected(Error::Capacity)
    ));
    assert_eq!(h.package_accounting(&p).unwrap().sequence, 0);
    assert_eq!(h.resources().logical_bytes, bytes);
    assert_eq!(
        h.admit_package(admission(package("Other", 0))),
        Err(Error::Capacity)
    );
    assert_eq!(h.resources().packages, 1);
}

#[test]
fn pipeline_reservation_queue_failure_and_final_obligations_are_bounded() {
    // Local escrow guarantees admitted timer -> callback -> final transfer cannot lose required work.
    let p = package("Pkg", 0);
    let q = package("Other", 0);
    let limits = ResidencyLimits {
        timers: 2,
        callbacks: 1,
        final_obligations: 1,
        ..ResidencyLimits::default()
    };
    let mut h = Residency::new(limits, finite(), &ManualClock(0)).unwrap();
    h.admit_package(admission(p.clone())).unwrap();
    h.admit_package(admission(q.clone())).unwrap();
    h.schedule_deactivation_if_unused(&p);
    assert_eq!(
        h.schedule_deactivation_if_unused(&q),
        SchedulingDisposition::Success { scheduled: true }
    );
    assert_eq!(
        h.package_accounting(&q)
            .unwrap()
            .last_schedule
            .unwrap()
            .admission,
        TimerAdmission::Capacity
    );
    let callback = due(&mut h, 10);
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    assert_eq!(h.resources().final_obligations, 1);
    assert_eq!(
        h.schedule_package_timer(&q, 0).unwrap(),
        TimerAdmission::Capacity
    );
    h.close_timer_queue();
    assert_eq!(
        h.schedule_deactivation_if_unused(&q),
        SchedulingDisposition::Success { scheduled: true }
    );
    assert_eq!(
        h.package_accounting(&q)
            .unwrap()
            .last_schedule
            .unwrap()
            .admission,
        TimerAdmission::QueueClosed
    );
    h.shutdown();
    assert_eq!(h.final_obligations().count(), 1);
}

#[test]
fn shutdown_and_dropped_callbacks_retain_admitted_work() {
    // Local safety adaptation over Deactivator.cpp:878-909's native clearing.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    h.schedule_deactivation_if_unused(&p);
    let callback = due(&mut h, 10);
    let id = callback.id();
    h.shutdown();
    let callback = match h.execute_callback(callback) {
        CallbackExecution::Retained {
            callback,
            error: Error::ObjectClosed,
        } => callback,
        other => panic!("{other:?}"),
    };
    drop(callback);
    assert_eq!(h.callbacks().next().unwrap().id, id);
    assert_eq!(h.resources().callbacks, 1);
    assert_eq!(
        h.schedule_deactivation_if_unused(&p),
        SchedulingDisposition::ObjectClosed
    );
    assert_eq!(h.scan_unused(), Err(Error::ObjectClosed));
}

#[test]
fn monotonic_ticks_and_overflow_are_explicit_without_partial_admission() {
    // Local deterministic driver; native signed 100ns time/grace observation is preserved.
    let p = package("Pkg", 0);
    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    h.advance(&ManualClock(i64::MAX - 1)).unwrap();
    assert_eq!(h.advance(&ManualClock(0)), Err(Error::InvalidInput));
    assert_eq!(
        h.schedule_deactivation_if_unused(&p),
        SchedulingDisposition::Success { scheduled: true }
    );
    assert_eq!(
        h.package_accounting(&p)
            .unwrap()
            .last_schedule
            .unwrap()
            .admission,
        TimerAdmission::TimeOverflow
    );
    assert_eq!(h.pending_timers().count(), 0);
    assert_eq!(
        h.set_config(config(
            GracePeriod::Finite(-1),
            GracePeriod::Infinite,
            GracePeriod::Infinite
        )),
        Err(Error::InvalidInput)
    );
    assert_eq!(
        h.set_config(config(
            GracePeriod::Finite(i64::MAX),
            GracePeriod::Infinite,
            GracePeriod::Infinite
        )),
        Err(Error::InvalidInput)
    ); // Native MaxValue is Infinite, not a finite timer-overflow shortcut.
}

#[test]
fn compensation_rejects_changed_partial_premise_and_never_substitutes_pair_release() {
    // Local verified-only compensation: the native partial branch supplies no general rollback.
    let p = package("Pkg", 0);
    let mut h = ledger(config(
        GracePeriod::Finite(0),
        GracePeriod::Finite(0),
        GracePeriod::Infinite,
    ));
    h.admit_package(admission(p.clone())).unwrap();
    h.schedule_application_timer(&p.package.application, 0)
        .unwrap();
    let callback = due(&mut h, 0);
    let mut beginnings = Vec::new();
    for _ in 0..2 {
        match h.begin_acquisition(&target(&p)) {
            AcquisitionBegin::Beginning(owner) => beginnings.push(owner),
            other => panic!("{other:?}"),
        }
    }
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    let mut residuals = Vec::new();
    for beginning in beginnings {
        match h.finish_acquisition(beginning) {
            UsageIncrement::Unresolved { ownership, .. } => residuals.push(ownership),
            other => panic!("{other:?}"),
        }
    }
    let newer = residuals.pop().unwrap();
    let older = residuals.pop().unwrap();
    let id = older.id();
    let older = match h.compensate_package_only(older) {
        UsageRelease::Unresolved { ownership, .. } => ownership,
        other => panic!("{other:?}"),
    };
    assert_eq!(older.id(), id);
    assert_eq!(h.package_accounting(&p).unwrap().usage, 2);
    assert!(matches!(
        h.compensate_package_only(newer),
        UsageRelease::Released
    ));
    assert!(matches!(
        h.compensate_package_only(older),
        UsageRelease::Unresolved { .. }
    ));
    assert_eq!(h.package_accounting(&p).unwrap().usage, 1);
    assert_eq!(h.resources().acquisitions, 1);
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .sequence,
        0
    );

    let mut h = ledger(finite());
    h.admit_package(admission(p.clone())).unwrap();
    let paired = acquire(&mut h, &p);
    let paired = match h.compensate_package_only(paired) {
        UsageRelease::Unresolved { ownership, .. } => ownership,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        h.application_accounting(&p.package.application)
            .unwrap()
            .usage,
        1
    );
    release(&mut h, paired);
}

#[test]
fn package_callback_requires_nonclosing_application_even_when_package_guard_matches() {
    // Deactivator.cpp:641-653,1221-1240: package access guard is stronger than sequence/count alone.
    let p = package("Pkg", 0);
    let mut h = ledger(config(
        GracePeriod::Finite(0),
        GracePeriod::Finite(0),
        GracePeriod::Infinite,
    ));
    h.admit_package(admission(p.clone())).unwrap();
    h.schedule_package_timer(&p, 0).unwrap();
    h.schedule_application_timer(&p.package.application, 0)
        .unwrap();
    let package_callback = due(&mut h, 0);
    let app_callback = due(&mut h, 0);
    assert!(matches!(
        h.execute_callback(app_callback),
        CallbackExecution::Deferred { .. }
    ));
    assert!(matches!(
        h.execute_callback(package_callback),
        CallbackExecution::Stale
    ));
    assert!(!h.package_accounting(&p).unwrap().closing);
    assert_eq!(h.package_accounting(&p).unwrap().sequence, 0);
    assert_eq!(h.package_accounting(&p).unwrap().usage, 0);
    assert_eq!(h.final_obligations().count(), 1);
}

#[test]
fn final_capacity_release_keeps_exact_failed_scheduling_request_without_claiming_end() {
    // Local final escrow and fixed-size request retention; native last decrement scheduling is void.
    let p = package("Pkg", 0);
    let q = package("Other", 0);
    let limits = ResidencyLimits {
        timers: 2,
        callbacks: 2,
        final_obligations: 1,
        ..ResidencyLimits::default()
    };
    let mut h = Residency::new(limits, finite(), &ManualClock(0)).unwrap();
    h.admit_package(admission(p.clone())).unwrap();
    h.admit_package(admission(q.clone())).unwrap();
    h.schedule_deactivation_if_unused(&p);
    let callback = due(&mut h, 10);
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    let token = acquire(&mut h, &q);
    release(&mut h, token);
    let attempt = h.package_accounting(&q).unwrap().last_schedule.unwrap();
    assert_eq!(attempt.captured_sequence, 2);
    assert_eq!(attempt.admission, TimerAdmission::Capacity);
    assert_eq!(h.resources().acquisitions, 0);
    assert_eq!(h.resources().final_obligations, 1);
    let final_work = h.final_obligations().next().unwrap();
    match final_work.subject {
        ResidencySubject::Package(premise) => {
            assert_eq!(premise.package, p);
            assert_eq!(premise.package_instance_id, 97);
            assert_eq!(premise.version, admission(p.clone()).version);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        h.increment_usage(&target(&p)),
        UsageIncrement::Rejected(Error::InvalidState)
    ));
}

#[test]
fn entry_and_timer_count_limits_are_independent_atomic_admissions() {
    // Local finite bounds; the native ordered maps themselves impose no fixed cap.
    let p = package("Pkg", 0);
    let mut other_app = package("Pkg", 0);
    other_app.package.application.number += 1;
    let limits = ResidencyLimits {
        applications: 1,
        packages: 2,
        timers: 1,
        callbacks: 3,
        final_obligations: 3,
        ..ResidencyLimits::default()
    };
    let mut h = Residency::new(limits, finite(), &ManualClock(0)).unwrap();
    h.admit_package(admission(p.clone())).unwrap();
    let before = h.resources();
    assert_eq!(h.admit_package(admission(other_app)), Err(Error::Capacity));
    assert_eq!(h.resources(), before);
    let q = package("Other", 0);
    h.admit_package(admission(q.clone())).unwrap();
    assert_eq!(
        h.admit_package(admission(package("Third", 0))),
        Err(Error::Capacity)
    );
    h.schedule_package_timer(&p, 0).unwrap();
    assert_eq!(
        h.schedule_package_timer(&q, 0).unwrap(),
        TimerAdmission::Capacity
    );
    let bytes = h.resources().logical_bytes;
    assert!(matches!(
        h.schedule_package_timer(&p, 1).unwrap(),
        TimerAdmission::Admitted { .. }
    ));
    assert_eq!(h.resources().logical_bytes, bytes); // Replacement reuses all escrow.
    assert!(h.cancel_package_timer(&p, 1).unwrap());
    assert!(matches!(
        h.schedule_package_timer(&q, 0).unwrap(),
        TimerAdmission::Admitted { .. }
    ));
}
// Processor-derived regression: Deactivator.cpp:113-145 and
// ApplicationManager.cpp:1079-1093 at pin 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.
// Passing the final guard is not physical End, but closing must return error.
#[test]
fn closing_package_is_invalid_state_not_successful_ineligibility() {
    let p = package("Pkg", 0);
    let mut h = ledger(config(
        GracePeriod::Finite(0),
        GracePeriod::Finite(0),
        GracePeriod::Infinite,
    ));
    h.admit_package(admission(p.clone())).unwrap();
    h.schedule_deactivation_if_unused(&p);
    let callback = due(&mut h, 0);
    assert!(matches!(
        h.execute_callback(callback),
        CallbackExecution::Deferred { .. }
    ));
    assert!(h.package_accounting(&p).unwrap().closing);
    assert!(
        !h.application_accounting(&p.package.application)
            .unwrap()
            .closing
    );
    let disposition = h.schedule_deactivation_if_unused(&p);
    assert_eq!(disposition, SchedulingDisposition::InvalidState);
    assert_eq!(
        disposition.outcome(),
        DeactivationOutcome::Failure(Error::InvalidState)
    );
}
