//! Owner-supplied physical completion premises are not OS cleanup observations.
use fube_hosting::*;

fn premise(app: u32, name: &str) -> ResidencyAdmission {
    ResidencyAdmission {
        package: PackageInstance {
            package: PackageId {
                application: ApplicationId {
                    type_name: "Local".into(),
                    number: app,
                },
                name: name.into(),
            },
            activation: Activation::SHARED,
            public_activation: String::new(),
        },
        version: VersionInstance {
            instance: 9,
            ..Default::default()
        },
        package_instance_id: 8,
        service_name: "fabric:/Local/Singleton".into(),
        application_name: "fabric:/Local".into(),
    }
}
fn ledger() -> Residency {
    Residency::new(
        ResidencyLimits::default(),
        ResidencyConfig {
            shared_grace: GracePeriod::Finite(0),
            exclusive_grace: GracePeriod::Infinite,
            unused_scan_interval: GracePeriod::Finite(0),
        },
        &ManualClock(0),
    )
    .unwrap()
}
fn finals(h: &mut Residency) {
    while let Some(callback) = h.dequeue_due().unwrap() {
        assert!(matches!(
            h.execute_callback(callback),
            CallbackExecution::Deferred { .. }
        ));
    }
}
fn token(h: &Residency) -> LocalDeactivation {
    h.local_deactivation(h.final_obligations().next().unwrap().id)
        .unwrap()
}
fn target(p: &ResidencyAdmission) -> TypeInstance {
    TypeInstance {
        service_type: ServiceTypeId {
            package: p.package.package.clone(),
            name: "Type".into(),
        },
        activation: Activation::SHARED,
        public_activation: String::new(),
    }
}

#[test]
fn package_then_application_completion_releases_every_charge_and_rejects_duplicate() {
    let p = premise(1, "Pkg");
    let mut h = ledger();
    h.admit_package(p.clone()).unwrap();
    h.schedule_package_timer(&p.package, 0).unwrap();
    finals(&mut h);
    let t = token(&h);
    let before = h.resources();
    let mut wrong = p.clone();
    wrong.package_instance_id += 1;
    assert!(h.finish_local_deactivation(&t, &[wrong]).is_err());
    assert_eq!(
        h.resources(),
        before,
        "failed premise preserves final and entries"
    );
    h.finish_local_deactivation(&t, std::slice::from_ref(&p))
        .unwrap();
    assert!(h
        .finish_local_deactivation(&t, std::slice::from_ref(&p))
        .is_err());
    assert_eq!(h.resources().packages, 0);
    assert_eq!(h.resources().applications, 1);
    finals(&mut h);
    h.finish_local_deactivation(&token(&h), &[]).unwrap();
    assert_eq!(h.resources().logical_bytes, 0);
    assert_eq!(h.resources().applications, 0);
}

#[test]
fn application_first_cascade_requires_every_exact_cleaned_package() {
    let p = premise(1, "Pkg");
    let q = premise(1, "Other");
    let mut h = ledger();
    h.admit_package(p.clone()).unwrap();
    h.admit_package(q.clone()).unwrap();
    h.scan_unused().unwrap();
    finals(&mut h);
    let t = token(&h);
    let before = h.resources();
    assert!(h
        .finish_local_deactivation(&t, std::slice::from_ref(&p))
        .is_err());
    assert!(h
        .finish_local_deactivation(&t, &[p.clone(), p.clone()])
        .is_err());
    assert_eq!(h.resources(), before);
    h.finish_local_deactivation(&t, &[p, q]).unwrap();
    assert_eq!(h.resources().logical_bytes, 0);
    assert_eq!(h.resources().packages, 0);
}

#[test]
fn foreign_ledger_and_live_acquisitions_cannot_be_erased_by_completion() {
    let p = premise(1, "Pkg");
    let mut a = ledger();
    let mut b = ledger();
    for h in [&mut a, &mut b] {
        h.admit_package(p.clone()).unwrap();
    }
    a.scan_unused().unwrap();
    finals(&mut a);
    let t = token(&a);
    let acquired = match b.increment_usage(&target(&p)) {
        UsageIncrement::Acquired(a) => a,
        other => panic!("{other:?}"),
    };
    b.schedule_application_timer(&p.package.package.application, 0)
        .unwrap();
    let callback = b.dequeue_due().unwrap().unwrap();
    assert!(matches!(
        b.execute_callback(callback),
        CallbackExecution::Stale
    ));
    assert!(b
        .finish_local_deactivation(&t, std::slice::from_ref(&p))
        .is_err());
    assert_eq!(b.resources().acquisitions, 1);
    assert_eq!(b.package_accounting(&p.package).unwrap().usage, 1);
    assert!(matches!(
        b.decrement_usage(acquired),
        UsageRelease::Released
    ));
}

#[test]
fn removal_reindexes_unrelated_acquisitions_and_work_without_losing_ownership() {
    let p = premise(1, "Pkg");
    let q = premise(2, "Pkg");
    let mut h = ledger();
    h.admit_package(p.clone()).unwrap();
    h.admit_package(q.clone()).unwrap();
    let acquired = match h.increment_usage(&target(&q)) {
        UsageIncrement::Acquired(a) => a,
        other => panic!("{other:?}"),
    };
    h.schedule_package_timer(&p.package, 0).unwrap();
    finals(&mut h);
    h.finish_local_deactivation(&token(&h), std::slice::from_ref(&p))
        .unwrap();
    finals(&mut h);
    h.finish_local_deactivation(&token(&h), &[]).unwrap();
    assert_eq!(h.package_accounting(&q.package).unwrap().usage, 1);
    assert!(matches!(
        h.decrement_usage(acquired),
        UsageRelease::Released
    ));
    finals(&mut h);
    h.finish_local_deactivation(&token(&h), std::slice::from_ref(&q))
        .unwrap();
    finals(&mut h);
    h.finish_local_deactivation(&token(&h), &[]).unwrap();
    assert_eq!(h.resources().logical_bytes, 0);
}

#[test]
fn old_completion_cannot_retire_a_new_same_identity_package() {
    let p = premise(1, "Pkg");
    let mut h = ledger();
    h.admit_package(p.clone()).unwrap();
    h.schedule_package_timer(&p.package, 0).unwrap();
    finals(&mut h);
    let old = token(&h);
    h.finish_local_deactivation(&old, std::slice::from_ref(&p))
        .unwrap();
    let mut next = p;
    next.package_instance_id += 1;
    next.version.instance += 1;
    h.admit_package(next.clone()).unwrap();
    assert!(h
        .finish_local_deactivation(&old, std::slice::from_ref(&next))
        .is_err());
    assert_eq!(
        h.package_accounting(&next.package)
            .unwrap()
            .package_instance_id,
        next.package_instance_id
    );
}
