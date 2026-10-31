//! Processor-derived closure/queue tests plus explicitly Rust finite-admission
//! adaptations. Authority: ServiceTypeStateManager.cpp:923-1117,1581-1636;
//! FabricRuntimeManager.cpp:634-679,873-900; EventDispatcher.cpp:39-197.
mod common;
use common::*;
use fube_hosting::*;

// Reusable typed-lane ownership contract, driven by actual source payloads.
// Status may be transferred first without consuming registration/closure.
fn event_lane_contract_case<S: HostingEventSource>(
    source: &mut S,
    status: &[EnabledDisabledEvent],
    closed: &[ClosedRegisteredEvent],
) -> (Vec<EnabledDisabledEvent>, Vec<ClosedRegisteredEvent>) {
    let owned_status = HostingEventSource::take_enabled_disabled_events(source);
    assert_eq!(owned_status, status);
    assert!(HostingEventSource::take_enabled_disabled_events(source).is_empty());
    let owned_closed = HostingEventSource::take_closed_registered_events(source);
    assert_eq!(owned_closed, closed);
    assert!(HostingEventSource::take_closed_registered_events(source).is_empty());
    (owned_status, owned_closed)
}

#[test]
fn contract_typed_lanes_preserve_disabled_finish_fields_and_owned_snapshots() {
    // Native processor-derived Registered+Enabled lanes; explicit deterministic
    // steps and output ownership are Rust adaptations, not worker-order proof.
    let mut h = hosting();
    completed_host(&mut h, "Host");
    let runtime = valid_runtime(&mut h, "Runtime", "Host");
    let id = declared(&mut h, "Widget");
    let timer = disable(&mut h, &id);
    h.disable_timer_input(&timer).unwrap();
    let disabled_sequence = h.type_entries()[0].sequence;
    event_lane_contract_case(
        &mut h,
        &[EnabledDisabledEvent::Disabled {
            sequence: disabled_sequence,
            type_instance: id.clone(),
        }],
        &[],
    );
    h.start_type_registration(&id, &runtime).unwrap();
    h.finish_type_registration(&id, true, &runtime).unwrap();
    assert_eq!(h.type_entries()[0].status, TypeStatus::RegisteredEnabled);
    let sequence = h.type_entries()[0].sequence;
    let expected = Registration::from_context(id.clone(), &runtime);
    let (status, closed) = event_lane_contract_case(
        &mut h,
        &[EnabledDisabledEvent::Enabled {
            sequence,
            type_instance: id.clone(),
        }],
        &[ClosedRegisteredEvent::Registered {
            sequence,
            registration: expected.clone(),
        }],
    );
    h.remove_type_source(&id, "package-component").unwrap();
    assert!(h.type_entries().is_empty());
    assert_eq!(
        closed,
        vec![ClosedRegisteredEvent::Registered {
            sequence,
            registration: expected,
        }]
    );
    assert_eq!(
        status,
        vec![EnabledDisabledEvent::Enabled {
            sequence,
            type_instance: id,
        }]
    );
}

#[test]
fn contract_event_transfer_restores_admission_without_losing_closure_kind() {
    // Rust admission behavior plus native processor-derived runtime/host closure.
    let mut h = Hosting::new(Limits {
        events_per_lane: 1,
        ..Limits::default()
    })
    .unwrap();
    completed_host(&mut h, "Host");
    let runtime = valid_runtime(&mut h, "Runtime", "Host");
    let id = registered(&mut h, "Widget", &runtime);
    let before = h.type_entries().to_vec();
    let runtimes = h.runtime_records().to_vec();
    assert_eq!(h.unregister_runtime("runtime"), Err(Error::Capacity));
    assert_eq!(h.type_entries(), before);
    assert_eq!(h.runtime_records(), runtimes);
    assert!(h.obligations().is_empty());
    let (_, registered) = event_lane_contract_case(
        &mut h,
        &[],
        &[ClosedRegisteredEvent::Registered {
            sequence: before[0].sequence,
            registration: Registration::from_context(id.clone(), &runtime),
        }],
    );
    h.unregister_runtime("runtime").unwrap();
    assert_eq!(h.find_runtime("Runtime"), Err(Error::RuntimeNotRegistered));
    assert_eq!(h.is_host_registered("Host"), Ok(true));
    assert_eq!(h.type_entries()[0].status, TypeStatus::NotRegisteredEnabled);
    let sequence = h.type_entries()[0].sequence;
    event_lane_contract_case(
        &mut h,
        &[],
        &[ClosedRegisteredEvent::RuntimeClosed {
            sequence,
            runtime_id: "Runtime".into(),
            host_id: "Host".into(),
            scope: ClosureScope {
                activation: id.activation,
                public_activation: id.public_activation.clone(),
            },
            affected: vec![id.service_type.clone()],
        }],
    );
    h.unregister_host("host", "model-unregister")
        .unwrap()
        .reply
        .unwrap();
    let expected = h.closed_registered_events().to_vec();
    assert!(matches!(
        expected.as_slice(),
        [ClosedRegisteredEvent::HostClosed { host_id, activity, affected, .. }]
        if host_id == "Host" && activity == "model-unregister" && affected.is_empty()
    ));
    event_lane_contract_case(&mut h, &[], &expected);
    assert!(matches!(
        registered.as_slice(),
        [ClosedRegisteredEvent::Registered { registration, .. }]
        if registration.runtime_id == "Runtime"
    ));
}

#[test]
fn runtime_closure_does_not_close_sibling_runtime_or_host() {
    let mut h = hosting();
    completed_host(&mut h, "Host");
    let a = valid_runtime(&mut h, "RuntimeA", "Host");
    let b = valid_runtime(&mut h, "RuntimeB", "Host");
    let first = registered(&mut h, "First", &a);
    let second = registered(&mut h, "Second", &b);
    h.take_closed_registered_events();
    h.unregister_runtime("runtimea").unwrap();
    assert_eq!(h.find_runtime("RuntimeA"), Err(Error::RuntimeNotRegistered));
    assert!(h.find_runtime("RuntimeB").is_ok());
    assert_eq!(h.is_host_registered("Host"), Ok(true));
    assert_eq!(
        h.get_type_state(&first).unwrap().status,
        TypeStatus::NotRegisteredEnabled
    );
    assert_eq!(
        h.get_type_state(&second).unwrap().status,
        TypeStatus::RegisteredEnabled
    );
    let [ClosedRegisteredEvent::RuntimeClosed {
        runtime_id,
        host_id,
        affected,
        sequence,
        ..
    }] = h.closed_registered_events()
    else {
        panic!("runtime closure only");
    };
    assert_eq!(runtime_id, "RuntimeA"); // stored runtime-table spelling
    assert_eq!(host_id, "Host");
    assert_eq!(affected, std::slice::from_ref(&first.service_type));
    assert_eq!(*sequence, h.type_entries()[0].sequence);
    assert!(
        matches!(h.obligations(), [Obligation::TypesUnregistered { types }] if types == &[first])
    );
    assert_eq!(
        h.require_live_composition(),
        Err(Error::UnsupportedCapability)
    );
}
#[test]
fn host_closure_removes_all_runtimes_uses_shared_sequence_first_scope_and_order() {
    let mut h = hosting();
    completed_host(&mut h, "Host");
    let a = valid_runtime(&mut h, "RuntimeA", "Host");
    let mut b = context("RuntimeB", "Host");
    b.package.package = package("Other", 2);
    b.package.activation = Activation::from_uuid(uuid::Uuid::from_u128(5));
    b.package.public_activation = uuid::Uuid::from_u128(5).to_string();
    h.register_runtime(b.clone()).result.unwrap();
    let z = registered(&mut h, "Z", &a);
    let first = registered(&mut h, "A", &a);
    let mut other = type_id("Other");
    other.service_type.package = b.package.package.clone();
    other.activation = b.package.activation;
    other.public_activation = b.package.public_activation.clone();
    h.declare_type(other.clone(), "other-component".into(), Ok(()))
        .unwrap();
    h.register_type(&other, &b).result.unwrap();
    h.add_runtime(context("Tentative", "Host")).unwrap();
    h.take_closed_registered_events();
    let result = h.unregister_host("host", "model-unregister").unwrap();
    assert_eq!(result.reply, Ok(()));
    assert_eq!(result.runtime_removal, Some(Ok(3)));
    assert!(h.runtime_records().is_empty());
    let [ClosedRegisteredEvent::HostClosed {
        host_id,
        activity,
        scope,
        affected,
        sequence,
    }] = h.closed_registered_events()
    else {
        panic!("one host closure, not runtime fanout");
    };
    assert_eq!(host_id, "Host"); // public unregister uses removed host spelling
    assert_eq!(activity, "model-unregister");
    assert_eq!(scope.activation, first.activation);
    assert_eq!(scope.public_activation, first.public_activation);
    assert_eq!(
        affected,
        &[first.service_type, z.service_type, other.service_type]
    );
    assert!(h
        .type_entries()
        .iter()
        .all(|e| e.sequence == *sequence && e.status == TypeStatus::NotRegisteredEnabled));
    assert!(h.enabled_disabled_events().is_empty());
}
#[test]
fn empty_repeat_closure_is_event_but_missing_public_unregister_is_not() {
    let mut h = hosting();
    h.runtime_closed_input("missing", "Host").unwrap();
    let events = h.take_closed_registered_events();
    let [ClosedRegisteredEvent::RuntimeClosed {
        affected,
        scope,
        sequence,
        ..
    }] = events.as_slice()
    else {
        panic!("empty event");
    };
    assert!(affected.is_empty());
    assert_eq!(*scope, ClosureScope::default());
    let first = *sequence;
    h.runtime_closed_input("missing", "Host").unwrap();
    assert!(
        matches!(h.closed_registered_events(), [ClosedRegisteredEvent::RuntimeClosed { sequence, affected, .. }]
        if *sequence > first && affected.is_empty())
    );
    h.take_closed_registered_events();
    assert_eq!(
        h.unregister_host("missing", "ordinary").unwrap().reply,
        Err(Error::HostNotRegistered)
    );
    assert!(h.closed_registered_events().is_empty());
    assert_eq!(
        h.host_termination_input("missing", "modeled-exit-input")
            .unwrap()
            .reply,
        Err(Error::HostNotRegistered)
    );
    assert!(
        matches!(h.closed_registered_events(), [ClosedRegisteredEvent::HostClosed { affected, .. }]
        if affected.is_empty())
    );
    assert!(h.obligations().is_empty());
}
#[test]
fn timedout_host_retains_runtime_type_no_fabricated_closure() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let timer = h.start_host("H", &ManualClock(0), 10).unwrap();
    let c = context("R", "H");
    h.add_runtime(c.clone()).unwrap();
    let id = declared(&mut h, "Widget");
    h.start_type_registration(&id, &c).unwrap();
    h.host_timeout(&timer).unwrap();
    assert_eq!(h.runtime_records().len(), 1);
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::InProgressEnabled
    );
    assert!(h.closed_registered_events().is_empty());
    assert!(h.enabled_disabled_events().is_empty());
    assert!(h.obligations().is_empty());
}
#[test]
fn disabled_finish_requires_both_lanes_and_independent_delivery_order() {
    let mut h = Hosting::new(Limits {
        events_per_lane: 1,
        ..Limits::default()
    })
    .unwrap();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = declared(&mut h, "Widget");
    let timer = disable(&mut h, &id);
    h.disable_timer_input(&timer).unwrap(); // fills status lane
    h.start_type_registration(&id, &c).unwrap();
    let before = h.type_entries().to_vec();
    assert_eq!(
        h.finish_type_registration(&id, true, &c),
        Err(Error::Capacity)
    );
    assert_eq!(h.type_entries(), before);
    assert!(h.closed_registered_events().is_empty());
    assert_eq!(h.enabled_disabled_events().len(), 1);
    h.take_enabled_disabled_events();
    h.finish_type_registration(&id, true, &c).unwrap();
    // Source enqueued Registered then Enabled, but consumer may drain Enabled first.
    let enabled = h.take_enabled_disabled_events();
    assert!(matches!(
        enabled.as_slice(),
        [EnabledDisabledEvent::Enabled { .. }]
    ));
    assert_eq!(h.closed_registered_events().len(), 1);
    let registered = h.take_closed_registered_events();
    assert!(matches!(
        registered.as_slice(),
        [ClosedRegisteredEvent::Registered { .. }]
    ));
    h.remove_type_source(&id, "package-component").unwrap();
    assert!(h.type_entries().is_empty());
    assert!(
        matches!(registered.as_slice(), [ClosedRegisteredEvent::Registered { registration, .. }]
        if registration.runtime_id == "R")
    ); // immutable owned output survives erase
}
#[test]
fn public_destructive_runtime_and_host_closure_veto_preserves_all_membership() {
    for host in [false, true] {
        let mut h = Hosting::new(Limits {
            events_per_lane: 1,
            ..Limits::default()
        })
        .unwrap();
        completed_host(&mut h, "H");
        let c = valid_runtime(&mut h, "R", "H");
        registered(&mut h, "Widget", &c); // fills closure lane
        let types = h.type_entries().to_vec();
        let runtimes = h.runtime_records().to_vec();
        let result = if host {
            h.unregister_host("H", "close").map(|_| ())
        } else {
            h.unregister_runtime("R")
        };
        assert_eq!(result, Err(Error::Capacity));
        assert_eq!(h.type_entries(), types);
        assert_eq!(h.runtime_records(), runtimes);
        assert_eq!(h.is_host_registered("H"), Ok(true));
        assert!(h.obligations().is_empty());
        h.take_closed_registered_events();
        if host {
            h.unregister_host("H", "close").unwrap();
        } else {
            h.unregister_runtime("R").unwrap();
        }
        assert_eq!(h.closed_registered_events().len(), 1);
        assert!(h.runtime_records().is_empty());
        assert_eq!(h.obligations().len(), 1);
    }
}
#[test]
fn byte_capacity_closure_veto_occurs_before_host_removal() {
    let mut h = Hosting::new(Limits {
        event_bytes_per_lane: 220,
        ..Limits::default()
    })
    .unwrap();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = declared(&mut h, "Widget");
    h.start_type_registration(&id, &c).unwrap(); // no registered event needed
    let before = h.type_entries().to_vec();
    assert_eq!(
        h.unregister_host("H", &"long activity".repeat(100)),
        Err(Error::Capacity)
    );
    assert_eq!(h.type_entries(), before);
    assert!(h.find_runtime("R").is_ok());
    assert_eq!(h.is_host_registered("H"), Ok(true));
    assert!(h.closed_registered_events().is_empty());
}
#[test]
fn timer_failure_source_and_obligation_bounds_rollback() {
    let mut h = Hosting::new(Limits {
        timers: 1,
        failures: 1,
        sources: 3,
        ..Limits::default()
    })
    .unwrap();
    let a = declared(&mut h, "A");
    let b = declared(&mut h, "B");
    h.register_failure("f".into()).unwrap();
    assert_eq!(h.register_failure("other".into()), Err(Error::Capacity));
    h.disable_type(&a, "f", &ManualClock(0), 1, true)
        .unwrap()
        .unwrap();
    let before = h.type_entries().to_vec();
    assert_eq!(
        h.disable_type(&b, "f", &ManualClock(0), 1, true),
        Err(Error::Capacity)
    );
    assert_eq!(h.type_entries(), before);
    let mut h = Hosting::new(Limits {
        obligations: 1,
        ..Limits::default()
    })
    .unwrap();
    let a = declared(&mut h, "A");
    let b = declared(&mut h, "B");
    h.remove_type_source(&a, "package-component").unwrap();
    let before = h.type_entries().to_vec();
    assert_eq!(
        h.remove_type_source(&b, "package-component"),
        Err(Error::Capacity)
    );
    assert_eq!(h.type_entries(), before);
    h.take_obligations();
    h.remove_type_source(&b, "package-component").unwrap();
}
#[test]
fn full_status_lane_does_not_lose_required_disable_callback() {
    let mut h = Hosting::new(Limits {
        events_per_lane: 1,
        ..Limits::default()
    })
    .unwrap();
    let a = declared(&mut h, "A");
    let b = declared(&mut h, "B");
    let ta = disable(&mut h, &a);
    let tb = disable(&mut h, &b);
    h.disable_timer_input(&ta).unwrap();
    let before = h.type_entries().to_vec();
    let timers = h.timers().to_vec();
    assert_eq!(h.disable_timer_input(&tb), Err(Error::Capacity));
    assert_eq!(h.type_entries(), before);
    assert_eq!(h.timers(), timers); // complete cancellation/output reservation
    h.take_enabled_disabled_events();
    assert!(h.disable_timer_input(&tb).unwrap());
    assert_eq!(
        h.get_type_state(&b).unwrap().status,
        TypeStatus::NotRegisteredDisabled
    );
}
#[test]
fn shutdown_no_fake_closure_late_disable_is_ignored_and_outputs_remain_owned() {
    let mut h = hosting();
    let id = declared(&mut h, "Widget");
    let timer = disable(&mut h, &id);
    h.disable_timer_input(&timer).unwrap();
    h.close_type_manager().unwrap();
    assert!(!h.disable_timer_input(&timer).unwrap());
    assert!(h.closed_registered_events().is_empty());
    assert_eq!(h.take_enabled_disabled_events().len(), 1); // not silently dropped
    assert!(h.timers().is_empty());
    h.runtime_closed_input("R", "H").unwrap();
    assert!(h.closed_registered_events().is_empty()); // closed manager no event/seq
}
#[test]
fn host_unregistration_consumes_runtime_table_error_but_still_emits_host_closure() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    registered(&mut h, "Widget", &c);
    h.take_closed_registered_events();
    h.close_runtime_table().unwrap();
    let result = h.unregister_host("H", "close").unwrap();
    assert_eq!(result.reply, Ok(()));
    assert_eq!(result.runtime_removal, Some(Err(Error::ObjectClosed)));
    assert!(matches!(
        h.closed_registered_events(),
        [ClosedRegisteredEvent::HostClosed { .. }]
    ));
}
#[test]
fn incomplete_tentative_owners_veto_closure_and_destructive_membership() {
    for empty_runtime in [false, true] {
        let mut h = hosting();
        completed_host(&mut h, "H");
        let mut c = valid_runtime(&mut h, "R", "H");
        let id = declared(&mut h, "Widget");
        if empty_runtime {
            c.runtime_id.clear();
        } else {
            c.host_id.clear();
        }
        h.start_type_registration(&id, &c).unwrap();
        let before = h.type_entries().to_vec();
        let runtimes = h.runtime_records().to_vec();
        let result = if empty_runtime {
            h.unregister_host("H", "close").map(|_| ())
        } else {
            h.unregister_runtime("R")
        };
        assert_eq!(
            result,
            Err(Error::NativeInvariant("closure with empty tentative owner"))
        );
        assert_eq!(h.type_entries(), before);
        assert_eq!(h.runtime_records(), runtimes);
        assert_eq!(h.is_host_registered("H"), Ok(true));
        assert!(h.closed_registered_events().is_empty());
        assert!(h.obligations().is_empty());
        assert_eq!(h.get_type_state(&id).unwrap().sequence, before[0].sequence);
    }
}

#[test]
fn valid_in_progress_disabled_runtime_or_host_closure_retains_failure_sources() {
    for host in [false, true] {
        let mut h = hosting();
        completed_host(&mut h, "H");
        let c = valid_runtime(&mut h, "R", "H");
        let id = declared(&mut h, "Widget");
        let timer = disable(&mut h, &id);
        h.disable_timer_input(&timer).unwrap();
        h.take_enabled_disabled_events();
        h.start_type_registration(&id, &c).unwrap();
        let before = h.type_entries()[0].clone();
        if host {
            h.unregister_host("H", "close").unwrap();
        } else {
            h.unregister_runtime("R").unwrap();
        }
        let e = &h.type_entries()[0];
        assert_eq!(e.status, TypeStatus::NotRegisteredDisabled);
        assert!(e.sequence > before.sequence);
        assert_eq!(e.failure, before.failure);
        assert_eq!(e.sources, before.sources);
        assert!(e.runtime_id.is_empty() && e.host_id.is_empty());
        assert!(h.timers().is_empty());
        assert!(h.enabled_disabled_events().is_empty());
        assert!(
            matches!(h.closed_registered_events(), [ClosedRegisteredEvent::HostClosed { .. }] if host)
                || matches!(h.closed_registered_events(), [ClosedRegisteredEvent::RuntimeClosed { .. }] if !host)
        );
        let mut request = request(&id, c.version);
        request.should_activate = false;
        assert_eq!(
            h.lookup(&request, &LookupModel::default()).unwrap().result,
            Err(Error::TypeDisabled)
        );
    }
}
