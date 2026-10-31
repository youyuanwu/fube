//! Native-test adapted observations: ServiceTypeStateManager.Test.cpp:132-226
//! (sort/query), NonActivatedApplicationHost.Test.cpp:315-374,495-757
//! (registration/disable). Only their in-memory observations are translated;
//! managed failure premises and exact transition matrices are processor-derived
//! from ServiceTypeStateManager.cpp:425-838,1142-1557. No native handler executed.
mod common;
use common::*;
use fube_hosting::*;

fn at(status: TypeStatus) -> (Hosting, TypeInstance, RuntimeContext) {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    let id = declared(&mut h, "Widget");
    match status {
        TypeStatus::NotRegisteredEnabled => {}
        TypeStatus::NotRegisteredDisableScheduled => {
            disable(&mut h, &id);
        }
        TypeStatus::NotRegisteredDisabled | TypeStatus::InProgressDisabled => {
            let timer = disable(&mut h, &id);
            assert!(h.disable_timer_input(&timer).unwrap());
            if status == TypeStatus::InProgressDisabled {
                h.start_type_registration(&id, &c).unwrap();
            }
        }
        TypeStatus::InProgressEnabled => h.start_type_registration(&id, &c).unwrap(),
        TypeStatus::RegisteredEnabled => assert_eq!(h.register_type(&id, &c).result, Ok(())),
    }
    h.take_closed_registered_events();
    h.take_enabled_disabled_events();
    h.take_obligations();
    (h, id, c)
}
const STATUSES: [TypeStatus; 6] = [
    TypeStatus::NotRegisteredEnabled,
    TypeStatus::InProgressEnabled,
    TypeStatus::RegisteredEnabled,
    TypeStatus::NotRegisteredDisableScheduled,
    TypeStatus::NotRegisteredDisabled,
    TypeStatus::InProgressDisabled,
];

#[test]
fn complete_start_matrix_and_owner_specific_duplicates() {
    for status in STATUSES {
        let (mut h, id, c) = at(status);
        let before = h.type_entries().to_vec();
        let result = h.start_type_registration(&id, &c);
        let expected = match status {
            TypeStatus::NotRegisteredEnabled
            | TypeStatus::NotRegisteredDisableScheduled
            | TypeStatus::NotRegisteredDisabled => Ok(()),
            TypeStatus::InProgressEnabled | TypeStatus::InProgressDisabled => {
                Err(Error::InvalidState)
            }
            TypeStatus::RegisteredEnabled => Err(Error::TypeAlreadyRegisteredToSameRuntime),
        };
        assert_eq!(result, expected, "{status:?}");
        if result.is_err() {
            assert_eq!(h.type_entries(), before);
        } else {
            let e = &h.type_entries()[0];
            assert_eq!(
                e.status,
                if status == TypeStatus::NotRegisteredDisabled {
                    TypeStatus::InProgressDisabled
                } else {
                    TypeStatus::InProgressEnabled
                }
            );
            assert!(e.sequence > before[0].sequence);
            assert_eq!(e.runtime_id, c.runtime_id);
            assert_eq!(e.host_id, c.host_id);
            if status == TypeStatus::NotRegisteredDisableScheduled {
                assert!(e.failure.is_none());
                assert!(h.timers().is_empty());
            }
        }
        assert!(h.closed_registered_events().is_empty());
        assert!(h.enabled_disabled_events().is_empty());
    }
    let (mut h, id, mut c) = at(TypeStatus::RegisteredEnabled);
    c.runtime_id = "r".into();
    assert_eq!(
        h.start_type_registration(&id, &c),
        Err(Error::TypeAlreadyRegisteredToSameRuntime)
    );
    c.runtime_id = "other".into();
    assert_eq!(
        h.start_type_registration(&id, &c),
        Err(Error::TypeAlreadyRegistered)
    );
}
#[test]
fn complete_finish_matrix_no_owner_or_global_sequence_fence() {
    for status in STATUSES {
        for valid in [false, true] {
            let (mut h, id, mut c) = at(status);
            let before = h.type_entries().to_vec();
            c.runtime_id = "DifferentFinishRuntime".into();
            c.code_package = "FinishCode".into();
            let result = h.finish_type_registration(&id, valid, &c);
            let transition = matches!(
                status,
                TypeStatus::InProgressEnabled | TypeStatus::InProgressDisabled
            );
            assert_eq!(
                result,
                if transition || status == TypeStatus::RegisteredEnabled {
                    Ok(())
                } else {
                    Err(Error::InvalidState)
                },
                "{status:?} {valid}"
            );
            if !transition {
                assert_eq!(h.type_entries(), before);
                assert!(h.closed_registered_events().is_empty());
            } else {
                let e = &h.type_entries()[0];
                assert_eq!(
                    e.status,
                    if valid {
                        TypeStatus::RegisteredEnabled
                    } else if status == TypeStatus::InProgressDisabled {
                        TypeStatus::NotRegisteredDisabled
                    } else {
                        TypeStatus::NotRegisteredEnabled
                    }
                );
                assert!(e.sequence > before[0].sequence);
                if valid {
                    let [ClosedRegisteredEvent::Registered {
                        sequence,
                        registration,
                    }] = h.closed_registered_events()
                    else {
                        panic!("registered event");
                    };
                    assert_eq!(*sequence, e.sequence);
                    assert_eq!(registration.runtime_id, c.runtime_id);
                    assert_eq!(registration.code_package, "FinishCode");
                    // State owner is the Start producer, snapshot uses Finish producer.
                    assert_eq!(e.runtime_id, "R");
                    assert!(e.failure.is_none());
                    assert_eq!(
                        h.enabled_disabled_events().len(),
                        usize::from(status == TypeStatus::InProgressDisabled)
                    );
                } else {
                    assert!(e.runtime_id.is_empty());
                    assert!(e.host_id.is_empty());
                    assert!(h.closed_registered_events().is_empty());
                    assert_eq!(
                        e.failure.is_some(),
                        status == TypeStatus::InProgressDisabled
                    );
                }
            }
        }
    }
}
#[test]
fn declared_validation_tentative_cleanup_and_request_checks() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = valid_runtime(&mut h, "R", "H");
    assert_eq!(
        h.register_type(&type_id("Unknown"), &c).result,
        Err(Error::InvalidServiceType)
    );
    let id = declared(&mut h, "Widget");
    let mut wrong = c.clone();
    wrong.runtime_id = "missing".into();
    let failed = h.register_type(&id, &wrong);
    assert_eq!(failed.result, Err(Error::RuntimeNotRegistered));
    assert_eq!(failed.cleanup, Some(Ok(())));
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::NotRegisteredEnabled
    );
    wrong = c.clone();
    wrong.host_id = "OtherHost".into();
    assert_eq!(
        h.register_type(&id, &wrong).result,
        Err(Error::RuntimeNotRegistered)
    );
    wrong = c.clone();
    wrong.package.activation = Activation::from_uuid(uuid::Uuid::from_u128(1));
    assert_eq!(
        h.start_type_registration(&id, &wrong),
        Err(Error::NativeInvariant(
            "type/runtime activation disagreement"
        ))
    );

    // Native validation checks host/runtime/activation, not full context equality.
    let mut selected = c.clone();
    selected.runtime_id = "r".into();
    selected.host_id = "h".into();
    selected.process_id = 999;
    selected.code_package = "RequestCode".into();
    selected.package.package = package("DifferentRequestPackage", 55);
    assert_eq!(h.register_type(&id, &selected).result, Ok(()));
    let [ClosedRegisteredEvent::Registered { registration, .. }] = h.closed_registered_events()
    else {
        panic!("event");
    };
    assert_eq!(registration.process_id, 999);
    assert_eq!(registration.code_package, "RequestCode");
    assert_eq!(
        registration.type_instance.service_type.package,
        id.service_type.package
    );
}
#[test]
fn type_before_runtime_validates_then_cleans_without_success_event() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = context("R", "H");
    h.add_runtime(c.clone()).unwrap();
    let id = declared(&mut h, "Widget");
    let fail = h.register_type(&id, &c);
    assert_eq!(fail.result, Err(Error::RuntimeNotRegistered));
    assert_eq!(fail.cleanup, Some(Ok(())));
    assert!(h.closed_registered_events().is_empty());
    h.validate_runtime("R").unwrap();
    h.start_type_registration(&id, &c).unwrap();
    h.remove_host_entry("H").unwrap();
    assert_eq!(
        h.validate_type_registration(&id, &c),
        Err(Error::HostNotRegistered)
    );
    let fail = h.cleanup_type_registration(&id, &c, Error::HostNotRegistered);
    assert_eq!(fail.result, Err(Error::HostNotRegistered));
    assert_eq!(fail.cleanup, Some(Ok(())));
}
#[test]
fn closure_or_declaration_removal_during_type_registration_rejects_late_finish() {
    let (mut h, id, c) = at(TypeStatus::InProgressEnabled);
    h.unregister_runtime("R").unwrap();
    assert_eq!(
        h.finish_type_registration(&id, true, &c),
        Err(Error::InvalidState)
    );
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::NotRegisteredEnabled
    );
    let (mut h, id, c) = at(TypeStatus::InProgressEnabled);
    h.remove_type_source(&id, "package-component").unwrap();
    assert_eq!(
        h.finish_type_registration(&id, true, &c),
        Err(Error::NotFound)
    );
    let failed = h.cleanup_type_registration(&id, &c, Error::RuntimeNotRegistered);
    assert_eq!(failed.result, Err(Error::RuntimeNotRegistered));
    assert_eq!(failed.cleanup, Some(Err(Error::NotFound)));
}
#[test]
fn failure_grace_recovery_and_stale_timer_are_source_specific() {
    let (mut h, id, c) = at(TypeStatus::NotRegisteredEnabled);
    assert_eq!(
        h.disable_type(&id, "unknown", &ManualClock(0), 1, true),
        Ok(None)
    );
    h.register_failure("failure".into()).unwrap();
    assert_eq!(
        h.disable_type(&id, "failure", &ManualClock(0), 1, false),
        Ok(None)
    );
    let timer = h
        .disable_type(&id, "FAILURE", &ManualClock(0), 0, true)
        .unwrap()
        .unwrap();
    assert_eq!(timer.due_ticks, 1); // zero schedules rather than synchronous disable
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::NotRegisteredDisableScheduled
    );
    h.start_type_registration(&id, &c).unwrap();
    assert!(h.timers().is_empty());
    assert!(!h.disable_timer_input(&timer).unwrap());
    assert!(h.enabled_disabled_events().is_empty()); // scheduled never became disabled
    h.finish_type_registration(&id, false, &c).unwrap();
    let timer2 = h
        .disable_type(&id, "failure", &ManualClock(10), 30, true)
        .unwrap()
        .unwrap();
    assert!(!h.disable_timer_input(&timer).unwrap()); // sequence mismatch, same NS status
    assert!(h.disable_timer_input(&timer2).unwrap());
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::NotRegisteredDisabled
    );
    assert!(h.timers().is_empty()); // managed type has no automatic recovery timer
    h.start_type_registration(&id, &c).unwrap();
    assert_eq!(
        h.unregister_failure("failure"),
        Err(Error::NativeInvariant(
            "InProgress_Disabled failure removal"
        ))
    );
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::InProgressDisabled
    );
    h.finish_type_registration(&id, true, &c).unwrap();
    assert_eq!(
        h.get_type_state(&id).unwrap().status,
        TypeStatus::RegisteredEnabled
    );
    let [ClosedRegisteredEvent::Registered { sequence, .. }] = h.closed_registered_events() else {
        panic!("registered");
    };
    assert!(matches!(h.enabled_disabled_events().last(),
        Some(EnabledDisabledEvent::Enabled { sequence: enabled, .. }) if enabled == sequence));
}
#[test]
fn failure_removal_before_and_after_disabled_has_distinct_notifications() {
    for status in [
        TypeStatus::NotRegisteredDisableScheduled,
        TypeStatus::NotRegisteredDisabled,
    ] {
        let (mut h, id, _) = at(status);
        let old = h.get_type_state(&id).unwrap().sequence;
        h.unregister_failure("FAILURE").unwrap();
        let e = &h.type_entries()[0];
        assert_eq!(e.status, TypeStatus::NotRegisteredEnabled);
        assert!(e.sequence > old);
        assert!(e.failure.is_none());
        assert_eq!(e.sources, ["package-component"]);
        assert!(h.timers().is_empty());
        assert_eq!(
            h.enabled_disabled_events().len(),
            usize::from(status == TypeStatus::NotRegisteredDisabled)
        );
        h.unregister_failure("failure").unwrap();
    }
}
#[test]
fn declaration_prefix_health_failure_and_per_entry_capacity_are_not_atomic_activation() {
    let mut h = hosting();
    let result = h.declare_types(
        vec![
            Declaration {
                id: type_id("A"),
                health_admission: Ok(()),
            },
            Declaration {
                id: type_id("B"),
                health_admission: Err(Error::Dependency(-77)),
            },
            Declaration {
                id: type_id("C"),
                health_admission: Ok(()),
            },
        ],
        "component",
    );
    assert_eq!(
        result,
        DeclarationResult {
            admitted: 1,
            failure: Some((1, Error::Dependency(-77)))
        }
    );
    assert_eq!(h.type_entries().len(), 1);
    assert_eq!(h.type_entries()[0].id.service_type.name, "A");
    h.declare_type(type_id("a"), "another".into(), Err(Error::Dependency(-77)))
        .unwrap();
    assert_eq!(h.type_entries()[0].id.service_type.name, "A"); // health only for creation
    assert_eq!(
        h.declare_type(type_id("a"), "another".into(), Ok(())),
        Err(Error::NativeInvariant("duplicate component source"))
    );
    let mut h = Hosting::new(Limits {
        types: 1,
        ..Limits::default()
    })
    .unwrap();
    let result = h.declare_types(
        vec![
            Declaration {
                id: type_id("A"),
                health_admission: Ok(()),
            },
            Declaration {
                id: type_id("B"),
                health_admission: Ok(()),
            },
        ],
        "component",
    );
    assert_eq!(
        result,
        DeclarationResult {
            admitted: 1,
            failure: Some((1, Error::Capacity))
        }
    );
    assert_eq!(h.type_entries().len(), 1);
    assert_eq!(
        h.declare_types(Vec::new(), "component").failure,
        Some((0, Error::InvalidInput))
    );
}
#[test]
fn activation_package_isolation_stored_query_sort_order_adapted_native_tests() {
    let mut h = hosting();
    let mut expected = Vec::new();
    for app in [2, 1] {
        for pkg in ["Z", "A"] {
            for activation in [2, 1, 0] {
                for name in ["Z", "Widget"] {
                    let mut id = type_id(name);
                    id.service_type.package = package(pkg, app);
                    id.activation = Activation::from_uuid(uuid::Uuid::from_u128(activation));
                    if activation != 0 {
                        id.public_activation = uuid::Uuid::from_u128(activation).to_string();
                    }
                    h.declare_type(id.clone(), "component".into(), Ok(()))
                        .unwrap();
                    expected.push(id);
                }
            }
        }
    }
    // Independent oracle for the valid generated domain of native sort/query tests.
    expected.sort_by_key(|id| {
        (
            id.service_type.package.application.number,
            id.service_type.package.name.clone(),
            id.activation.0,
            id.service_type.name.clone(),
        )
    });
    assert_eq!(
        h.type_entries()
            .iter()
            .map(|e| e.id.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let mut changed = expected[0].clone();
    changed.public_activation = uuid::Uuid::from_u128(9).to_string();
    assert_eq!(
        h.declare_type(changed, "other".into(), Ok(())),
        Err(Error::NativeInvariant(
            "public activation identity disagreement"
        ))
    );
}
#[test]
fn missing_state_consumes_global_sequence_without_creating_declared_entry() {
    let mut h = hosting();
    let one = h.get_type_state(&type_id("Unknown")).unwrap();
    let two = h.get_type_state(&type_id("Unknown")).unwrap();
    assert!(two.sequence > one.sequence);
    assert!(h.type_entries().is_empty());
    let id = declared(&mut h, "Widget");
    assert!(h.get_type_state(&id).unwrap().sequence > two.sequence);
    h.close_type_manager().unwrap();
    assert_eq!(h.get_type_state(&id), Err(Error::ObjectClosed));
    assert_eq!(
        h.start_type_registration(&id, &context("R", "H")),
        Err(Error::ObjectClosed)
    );
    assert_eq!(
        h.finish_type_registration(&id, true, &context("R", "H")),
        Err(Error::ObjectClosed)
    );
}
#[test]
fn disabled_failure_only_source_preserves_native_held_entry_then_erases() {
    let (mut h, id, c) = at(TypeStatus::InProgressDisabled);
    h.remove_type_source(&id, "package-component").unwrap();
    assert_eq!(h.type_entries()[0].sources, ["failure"]);
    let before = h.type_entries()[0].sequence;
    h.finish_type_registration(&id, true, &c).unwrap();
    assert!(h.type_entries().is_empty()); // last failure source reset erases
    let [ClosedRegisteredEvent::Registered {
        sequence,
        registration,
    }] = h.closed_registered_events()
    else {
        panic!("held registration");
    };
    assert!(*sequence > before + 1); // registration sequence then last-source Closed sequence
    assert_eq!(registration.type_instance, id);
    assert!(
        matches!(h.enabled_disabled_events(), [EnabledDisabledEvent::Enabled { sequence: s, .. }]
        if s == sequence)
    );
    assert!(matches!(
        h.obligations(),
        [Obligation::HealthSourceReleased { .. }]
    ));
}
#[test]
fn scheduled_failure_only_start_gates_native_outer_component_assertion() {
    let (mut h, id, c) = at(TypeStatus::NotRegisteredDisableScheduled);
    h.remove_type_source(&id, "package-component").unwrap();
    let before = h.type_entries().to_vec();
    let timers = h.timers().to_vec();
    assert_eq!(
        h.start_type_registration(&id, &c),
        Err(Error::NativeInvariant(
            "scheduled Start removes last component"
        ))
    );
    assert_eq!(h.type_entries(), before);
    assert_eq!(h.timers(), timers);
    assert!(h.obligations().is_empty());
    assert!(h.closed_registered_events().is_empty());
    assert!(h.enabled_disabled_events().is_empty());
}
