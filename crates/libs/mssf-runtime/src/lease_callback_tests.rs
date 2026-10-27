use super::*;

#[test]
fn callback_deadline_is_independent_of_lease_renewal_and_late_completion_is_terminal() {
    let now = Instant::now();
    let authority = Authority::new(now, 100_000_000).unwrap();
    let guard = authority
        .callback_deadline(Duration::from_secs(1), false)
        .unwrap();
    let deadline = guard.deadline;
    authority.renew(now, 1_000_000_000, now).unwrap();
    assert_eq!(authority.effective_deadline(), deadline);
    drop(guard);
    assert!(authority.check().is_ok());
    let late = authority
        .callback_deadline(Duration::from_millis(1), false)
        .unwrap();
    let deadline = late.deadline;
    std::thread::sleep(Duration::from_millis(3));
    drop(late);
    assert_eq!(authority.retired.load(Ordering::SeqCst), deadline);
    assert!(authority
        .renew(Instant::now(), 1_000_000_000, Instant::now())
        .is_err());
}

#[test]
fn pending_callback_ownership_prevents_disarm_and_has_exact_finite_admission() {
    let authority = Authority::new(Instant::now(), 100_000_000).unwrap();
    let guards: Vec<_> = (0..CALLBACK_DEADLINE_SLOTS)
        .map(|_| {
            authority
                .callback_deadline(Duration::from_secs(60), false)
                .unwrap()
        })
        .collect();
    assert!(matches!(
        authority.callback_deadline(Duration::from_secs(60), false),
        Err(Error::ResourceLimit("callback deadline slots"))
    ));
    assert!(authority.disarm().is_err());
    assert!(!authority.disarmed.load(Ordering::SeqCst));
    assert!(authority
        .callback_deadline(Duration::from_secs(60), true)
        .is_err());
    drop(guards);
    assert!(authority.check().is_err());
}
