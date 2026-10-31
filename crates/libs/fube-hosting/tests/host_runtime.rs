//! Processor-derived tests, not native handler execution.
//! Host: ApplicationHostRegistration.cpp:38-145; ApplicationHostManager.cpp:
//! 1292-1299,1966-2053. Runtime: FabricRuntimeManager.cpp:501-679 and
//! RuntimeRegistrationTable.cpp:53-266. Rust admission tests are labeled below.
mod common;
use common::*;
use fube_hosting::*;

#[test]
fn public_finish_reply_state_validity_proxy_are_independent() {
    let mut h = hosting();
    h.add_host("Host".into()).unwrap();
    assert_eq!(h.is_host_registered("host"), Ok(false));
    let timer = h.start_host("host", &ManualClock(1), 10).unwrap();
    let first = h.finish_host("HOST", true).unwrap();
    assert_eq!(first.reply, Ok(()));
    assert_eq!(first.state_method, Some(Ok(())));
    assert_eq!(first.proxy_callback.as_deref(), Some("Host"));
    assert_eq!(h.is_host_registered("host"), Ok(true));
    assert!(h.timers().is_empty());
    let second = h.finish_host("host", true).unwrap();
    assert_eq!(second.reply, Ok(()));
    assert_eq!(second.state_method, Some(Err(Error::InvalidState)));
    assert!(second.proxy_callback.is_some());
    assert_eq!(h.is_host_registered("host"), Ok(true));
    assert_eq!(h.host_timeout(&timer), Err(Error::InvalidState));
    assert_eq!(h.is_host_registered("host"), Ok(true));
    assert_eq!(h.outstanding_operations(), 0);
    let no_proxy = h.finish_host("host", false).unwrap();
    assert!(no_proxy.proxy_callback.is_none());
}
#[test]
fn timeout_mark_remove_interleaving_and_no_logical_closure() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let timer = h.start_host("H", &ManualClock(0), 10).unwrap();
    let timeout = h.begin_host_timeout(&timer).unwrap();
    assert!(h.timers().is_empty()); // timer cancel precedes Find/mark
    h.mark_host_timeout(timeout).unwrap();
    assert_eq!(h.find_host("H").unwrap().status, HostStatus::Timedout);
    assert_eq!(h.is_host_registered("H"), Ok(false));
    let finish = h.finish_host("H", true).unwrap();
    assert_eq!(finish.reply, Ok(()));
    assert_eq!(finish.state_method, Some(Err(Error::InvalidState)));
    assert!(finish.proxy_callback.is_some());
    assert_eq!(h.find_host("H").unwrap().status, HostStatus::Timedout);
    h.remove_timedout_host(timeout).unwrap();
    let late = h.finish_host("H", true).unwrap();
    assert_eq!(late.reply, Err(Error::HostNotRegistered));
    assert_eq!(late.state_method, None);
    assert_eq!(late.proxy_callback, None);
    assert_eq!(h.retained_host_records(), 0);
    // No event-producing path is called by timeout; later event tests assert this.
}
#[test]
fn closed_table_finish_has_no_state_method_or_proxy() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    h.close_tables().unwrap();
    let late = h.finish_host("H", true).unwrap();
    assert_eq!(late.reply, Err(Error::ObjectClosed));
    assert_eq!(late.state_method, None);
    assert_eq!(late.proxy_callback, None);
    assert_eq!(h.add_host("new".into()), Err(Error::ObjectClosed));
}
#[test]
fn finish_holds_record_across_timeout_and_ordinary_table_removal() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let timer = h.start_host("H", &ManualClock(0), 10).unwrap();
    let finish = h.begin_finish_host("H").unwrap();
    h.host_timeout(&timer).unwrap();
    assert_eq!(h.find_host("H"), Err(Error::HostNotRegistered));
    assert_eq!(h.retained_host_records(), 1);
    let result = h.complete_finish_host(finish, true).unwrap();
    assert_eq!(result.reply, Ok(()));
    assert_eq!(result.state_method, Some(Err(Error::InvalidState)));
    assert!(result.proxy_callback.is_some());
    assert_eq!(h.retained_host_records(), 0);

    h.add_host("H".into()).unwrap();
    h.start_host("H", &ManualClock(0), 10).unwrap();
    let finish = h.begin_finish_host("H").unwrap();
    h.remove_host_entry("H").unwrap();
    let result = h.complete_finish_host(finish, true).unwrap();
    assert_eq!(result.state_method, Some(Ok(())));
    assert_eq!(h.is_host_registered("H"), Err(Error::HostNotRegistered));
    assert_eq!(h.retained_host_records(), 0);
}
#[test]
fn inserted_not_started_finish_and_duplicate_start_are_native_distinctions() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let finish = h.finish_host("h", true).unwrap();
    assert_eq!(finish.reply, Ok(()));
    assert_eq!(finish.state_method, Some(Err(Error::InvalidState)));
    assert_eq!(h.add_host("h".into()), Err(Error::HostAlreadyRegistered));
    h.start_host("H", &ManualClock(0), 1).unwrap();
    assert_eq!(
        h.start_host("H", &ManualClock(0), 1),
        Err(Error::InvalidState)
    );
}
#[test]
fn runtime_order_tentative_duplicate_cleanup_and_host_races() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let c = context("R", "H");
    let fail = h.register_runtime(c.clone());
    assert_eq!(fail.result, Err(Error::HostNotRegistered));
    assert_eq!(fail.cleanup, Some(Ok(())));
    assert!(h.runtime_records().is_empty());
    h.add_runtime(c.clone()).unwrap();
    assert_eq!(h.add_runtime(c.clone()), Err(Error::OperationFailed));
    assert_eq!(h.find_runtime("r"), Err(Error::RuntimeNotRegistered));
    assert_eq!(
        h.check_public_runtime_unregister("r"),
        Err(Error::NativeInvariant(
            "tentative public runtime unregister"
        ))
    );
    assert_eq!(h.runtime_records().len(), 1); // safety veto is non-destructive
    assert_eq!(h.remove_runtime_entry("r"), Ok(None));
    h.start_host("H", &ManualClock(0), 1).unwrap();
    h.finish_host("H", false).unwrap();
    assert_eq!(h.register_runtime(c.clone()).result, Ok(()));
    assert_eq!(
        h.add_runtime(c.clone()),
        Err(Error::RuntimeAlreadyRegistered)
    );
    assert_eq!(h.find_runtime("r").unwrap(), c);

    h.add_runtime(context("R2", "H")).unwrap();
    h.check_runtime_host(&c).unwrap();
    h.remove_host_runtime_entries("h").unwrap(); // includes tentative
    assert_eq!(h.validate_runtime("R2"), Err(Error::RuntimeNotRegistered));
    let fail = h.cleanup_runtime("R2", Error::RuntimeNotRegistered);
    assert_eq!(fail.result, Err(Error::RuntimeNotRegistered));
    assert_eq!(fail.cleanup, Some(Err(Error::RuntimeNotRegistered)));
    assert!(h.runtime_records().is_empty());
}
#[test]
fn validate_has_no_fabricated_second_host_check() {
    let mut h = hosting();
    completed_host(&mut h, "H");
    let c = context("R", "H");
    h.add_runtime(c.clone()).unwrap();
    h.check_runtime_host(&c).unwrap();
    h.remove_host_entry("H").unwrap(); // low-level operation, not full host closure
    assert_eq!(h.validate_runtime("R"), Ok(c));
}
#[test]
fn non_ascii_host_runtime_ids_use_native_comparison_not_unicode_folding() {
    let mut h = hosting();
    completed_host(&mut h, "é");
    assert_eq!(h.find_host("É"), Err(Error::HostNotRegistered));
    let c = context("σ", "é");
    assert_eq!(h.register_runtime(c.clone()).result, Ok(()));
    assert_eq!(h.find_runtime("σ"), Ok(c));
    assert_eq!(h.find_runtime("ς"), Err(Error::RuntimeNotRegistered));
}

// Rust ownership/admission adaptations, not native memory-budget tests.
#[test]
fn finite_records_operations_timers_and_bytes_rollback() {
    let mut h = Hosting::new(Limits {
        hosts: 1,
        operations: 1,
        timers: 1,
        ..Limits::default()
    })
    .unwrap();
    h.add_host("H".into()).unwrap();
    assert_eq!(h.add_host("other".into()), Err(Error::Capacity));
    let finish = h.begin_finish_host("H").unwrap();
    assert_eq!(h.begin_finish_host("H"), Err(Error::Capacity));
    h.remove_host_entry("H").unwrap();
    assert_eq!(h.add_host("other".into()), Err(Error::Capacity)); // held record charged
    h.cancel_host_operation(finish).unwrap();
    h.add_host("other".into()).unwrap();
    assert_eq!(h.retained_host_records(), 1);
    let mut h = Hosting::new(Limits {
        timers: 1,
        ..Limits::default()
    })
    .unwrap();
    h.add_host("a".into()).unwrap();
    h.add_host("b".into()).unwrap();
    h.start_host("a", &ManualClock(0), 1).unwrap();
    assert_eq!(h.start_host("b", &ManualClock(0), 1), Err(Error::Capacity));
    assert_eq!(h.find_host("b").unwrap().status, HostStatus::NotRegistered);
    let mut h = Hosting::new(Limits {
        state_bytes: 70,
        ..Limits::default()
    })
    .unwrap();
    assert_eq!(h.add_host("a long identifier".into()), Err(Error::Capacity));
    assert_eq!(h.retained_host_records(), 0);
    h.add_host("a".into()).unwrap();
    assert_eq!(h.start_host("a", &ManualClock(0), 1), Err(Error::Capacity));
    assert_eq!(h.find_host("a").unwrap().status, HostStatus::NotRegistered);
}
#[test]
fn runtime_admission_is_bounded_and_closed_errors_do_not_become_success() {
    let mut h = Hosting::new(Limits {
        runtimes: 1,
        ..Limits::default()
    })
    .unwrap();
    completed_host(&mut h, "H");
    valid_runtime(&mut h, "R", "H");
    assert_eq!(h.add_runtime(context("other", "H")), Err(Error::Capacity));
    assert_eq!(h.runtime_records().len(), 1);
    h.close_tables().unwrap();
    assert_eq!(h.find_runtime("R"), Err(Error::ObjectClosed));
    assert_eq!(
        h.register_runtime(context("R", "H")).result,
        Err(Error::ObjectClosed)
    );
}
#[test]
fn timeout_before_start_state_cancels_then_start_uses_held_record() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let start = h.begin_start_host("H").unwrap();
    let timer = h.arm_host_start_timer(start, &ManualClock(0), 1).unwrap();
    assert_eq!(h.find_host("H").unwrap().status, HostStatus::NotRegistered);
    assert_eq!(h.host_timeout(&timer), Err(Error::InvalidState));
    assert!(h.timers().is_empty());
    h.complete_start_host(start).unwrap();
    assert_eq!(h.find_host("H").unwrap().status, HostStatus::InProgress);
    assert_eq!(h.is_host_registered("H"), Ok(false));
    assert!(h.timers().is_empty()); // canceled callback not silently rearmed
    assert_eq!(h.outstanding_operations(), 0);
}
#[test]
fn held_start_after_removal_succeeds_without_table_validity() {
    let mut h = hosting();
    h.add_host("H".into()).unwrap();
    let start = h.begin_start_host("H").unwrap();
    h.remove_host_entry("H").unwrap();
    assert_eq!(h.retained_host_records(), 1);
    let timer = h.arm_host_start_timer(start, &ManualClock(0), 1).unwrap();
    h.complete_start_host(start).unwrap();
    assert_eq!(h.is_host_registered("H"), Err(Error::HostNotRegistered));
    assert_eq!(h.retained_host_records(), 0);
    assert!(h.timers().is_empty()); // held record destruction canceled timer
    assert_eq!(h.host_timeout(&timer), Err(Error::HostNotRegistered));
}
