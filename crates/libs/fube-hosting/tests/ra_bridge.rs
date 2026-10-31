//! Test-only node composition: actual H1 results -> existing RA issued lookup
//! completions. Package usage/acquisitions and RAP are explicitly simulated.
//! Native source: FabricRuntimeManager.cpp:117-249,383-458;
//! RA/ReconfigurationAgent.cpp:987-1033, ServiceTypeRegistrationWrapper.cpp:78-157,
//! Hosting.ServiceTypeMapEntity.cpp:16-80. No live Hosting event consumer, IPC,
//! package ownership, RAP handler or deployed interoperability is qualified.
mod common;
use common::*;
use fube_hosting as h1;
use fube_ra::{entity as ra, protocol as wire};
use fube_ra_store::{MemoryBackend, Settings as StoreSettings, Store};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

const WAIT: Duration = Duration::from_secs(3); // observation timeout, not an ordering sleep
struct FixedClock;
impl ra::NotificationClock for FixedClock {
    fn now_ticks(&self) -> i64 {
        1_000_000_000
    }
}
fn admission() -> ra::Admission {
    ra::Admission {
        local_node: wire::NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: wire::NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: wire::GenerationNumber {
            generation: 7,
            owner: [8; 16],
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}
fn version(v: &h1::VersionInstance) -> wire::VersionInstance {
    wire::VersionInstance {
        version: wire::PackageVersion {
            application: wire::ApplicationVersion {
                value: wire::Rollout {
                    major: v.application.major,
                    minor: v.application.minor,
                },
            },
            value: wire::Rollout {
                major: v.package.major,
                minor: v.package.minor,
            },
        },
        instance: v.instance,
    }
}
fn service_type(t: &h1::ServiceTypeId) -> wire::ServiceType {
    wire::ServiceType {
        package: wire::PackageId {
            application: wire::ApplicationId {
                type_name: t.package.application.type_name.clone(),
                number: t.package.application.number,
            },
            name: t.package.name.clone(),
        },
        name: t.name.clone(),
    }
}
fn body(id: u8, t: &h1::TypeInstance, v: &h1::VersionInstance) -> wire::AddInstance {
    let mut b = wire::AddInstance::default();
    b.unit.id.guid.0 = [id; 16];
    b.unit.consistency.id = b.unit.id.clone();
    b.unit.current = wire::Epoch {
        configuration: 5,
        data_loss: 1,
    };
    b.replica.node = admission().local_node;
    b.replica.id = 40;
    b.replica.instance = 50;
    b.replica.state = 1;
    b.replica.version = version(v);
    b.service.version = version(v);
    b.service.name = "fabric:/app/stateless".into();
    b.service.service_type = service_type(&t.service_type);
    b
}
async fn fixture() -> (Store, ra::Executor) {
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let ra = ra::Executor::new_with_clock(
        Arc::new(store.clone()),
        admission(),
        ra::Settings::default(),
        Arc::new(FixedClock),
    )
    .unwrap();
    (store, ra)
}
async fn run(ra: &ra::Executor, command: ra::Command) -> ra::Disposition {
    ra.submit(command).unwrap().wait(WAIT).await.unwrap()
}
fn effects(ra: &ra::Executor) -> Vec<ra::Effect> {
    ra.take_effects().into_iter().map(|d| d.effect).collect()
}
#[derive(Clone)]
struct Issued {
    token: u64,
    lookup: u64,
    body: Arc<wire::AddInstance>,
    retained_usage: Option<u64>,
}
fn issued(actions: &[ra::Effect]) -> Issued {
    let e = actions
        .iter()
        .find(|e| {
            matches!(
                e,
                ra::Effect::Open { .. } | ra::Effect::FindRegistration { .. }
            )
        })
        .expect("issued lookup");
    match e {
        ra::Effect::Open {
            token,
            lookup,
            body,
            usage,
        }
        | ra::Effect::FindRegistration {
            token,
            lookup,
            body,
            usage,
        } => Issued {
            token: *token,
            lookup: *lookup,
            body: body.clone(),
            retained_usage: *usage,
        },
        _ => unreachable!(),
    }
}
async fn add(ra: &ra::Executor, b: wire::AddInstance) -> Issued {
    run(
        ra,
        ra::Command::Add {
            body: Box::new(b),
            source: admission().fm_source,
            generation: wire::GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        },
    )
    .await;
    issued(&effects(ra))
}
fn native_error(result: &h1::Result<()>) -> i64 {
    match result {
        Ok(()) => 0,
        Err(h1::Error::TypeNotRegistered) => ra::HOSTING_SERVICE_TYPE_NOT_REGISTERED,
        Err(h1::Error::DeploymentInProgress) => ra::HOSTING_DEPLOYMENT_IN_PROGRESS,
        Err(h1::Error::RuntimeNotRegistered) => ra::HOSTING_RUNTIME_NOT_REGISTERED,
        Err(h1::Error::TypeDisabled) => ra::HOSTING_SERVICE_TYPE_DISABLED,
        Err(h1::Error::RegistrationVersionMismatch) => ra::HOSTING_REGISTRATION_VERSION_MISMATCH,
        Err(h1::Error::NotFound) => ra::HOSTING_NOT_FOUND,
        Err(h1::Error::Dependency(error)) => *error,
        Err(error) => {
            panic!("local capability/admission errors are not fabricated native replies: {error:?}")
        }
    }
}
#[derive(Default)]
struct SimulatedOwnership {
    registrations: BTreeSet<u64>,
    usages: BTreeSet<u64>,
    diagnostics: Vec<h1::Registration>,
    releases: Vec<(bool, u64)>,
}
impl SimulatedOwnership {
    /// Called ONCE for an actual issued result, never again for callback replay.
    fn completion(
        &mut self,
        issued: &Issued,
        result: &h1::LookupResult,
        upgrade: Option<u64>,
        usage_error: i64,
    ) -> ra::RegistrationCompletion {
        let error = native_error(&result.result);
        let acquired = error == 0
            || (error == ra::HOSTING_REGISTRATION_VERSION_MISMATCH && result.candidate.is_some());
        let mut registration = ra::RuntimeRegistration::default();
        let actual_version = if let Some(candidate) = &result.candidate {
            self.diagnostics.push(candidate.clone()); // retain richer H1 observations
            registration = ra::RuntimeRegistration {
                service_type: service_type(&candidate.type_instance.service_type),
                host: candidate.host_id.clone(),
                runtime: candidate.runtime_id.clone(),
                acquisition: if acquired { issued.lookup } else { 0 },
                code_package: candidate.code_package.clone(),
            };
            version(&candidate.version)
        } else {
            wire::VersionInstance::invalid()
        };
        let usage_acquired = acquired && issued.retained_usage.is_none() && usage_error == 0;
        if acquired {
            assert!(
                self.registrations.insert(issued.lookup),
                "duplicate acquisition"
            );
        }
        if usage_acquired {
            assert!(self.usages.insert(issued.lookup));
        }
        ra::RegistrationCompletion {
            unit: issued.body.unit.id.clone(),
            token: issued.token,
            lookup: issued.lookup,
            registration,
            error,
            version: actual_version,
            upgrade_present: upgrade.is_some(),
            required_upgrade: upgrade.unwrap_or_default(),
            usage_acquired,
            usage_error,
            ..ra::RegistrationCompletion::default()
        }
    }
    fn consume_releases(&mut self, actions: &[ra::Effect]) {
        for action in actions {
            match action {
                ra::Effect::ReleaseRegistration(r) => {
                    assert!(
                        self.registrations.remove(&r.acquisition),
                        "wrong/double registration release"
                    );
                    self.releases.push((false, r.acquisition));
                }
                ra::Effect::ReleaseUsage { acquisition } => {
                    assert!(
                        self.usages.remove(acquisition),
                        "wrong/double usage release"
                    );
                    self.releases.push((true, *acquisition));
                }
                _ => {}
            }
        }
    }
}
fn core_result(core: &mut h1::Hosting, i: &Issued, model: h1::LookupModel) -> h1::LookupResult {
    let t = &i.body.service.service_type;
    let v = &i.body.service.version;
    let req = h1::LookupRequest {
        service_type: h1::ServiceTypeId {
            package: h1::PackageId {
                application: h1::ApplicationId {
                    type_name: t.package.application.type_name.clone(),
                    number: t.package.application.number,
                },
                name: t.package.name.clone(),
            },
            name: t.name.clone(),
        },
        version: h1::VersionInstance {
            application: h1::Rollout {
                major: v.version.application.value.major,
                minor: v.version.application.value.minor,
            },
            package: h1::Rollout {
                major: v.version.value.major,
                minor: v.version.value.minor,
            },
            instance: v.instance,
        },
        activation: h1::Activation::SHARED,
        service_name: i.body.service.name.clone(),
        application_name: "fabric:/app".into(),
        should_activate: true,
    };
    core.lookup(&req, &model).unwrap()
}
fn ready_core() -> (h1::Hosting, h1::TypeInstance, h1::RuntimeContext) {
    let mut core = hosting();
    completed_host(&mut core, "H");
    let c = valid_runtime(&mut core, "R", "H");
    let id = registered(&mut core, "Widget", &c);
    (core, id, c)
}
async fn close_fixture(store: Store, ra: ra::Executor) {
    let stats = ra.close(WAIT).await.unwrap();
    assert_eq!(stats.outstanding, 0);
    store.close().await.unwrap();
    // Executor close is not a modeled package release or process cleanup receipt.
}

#[tokio::test]
async fn pending_never_opens_rap_and_later_actual_h1_registration_does() {
    let (store, ra) = fixture().await;
    let mut core = hosting();
    let id = declared(&mut core, "Widget");
    let c = context("R", "H");
    let first = add(&ra, body(1, &id, &c.version)).await;
    let pending = core_result(
        &mut core,
        &first,
        h1::LookupModel {
            activation: Some(h1::ActivationOutcome::Pending),
            ..h1::LookupModel::default()
        },
    );
    assert_eq!(pending.result, Err(h1::Error::DeploymentInProgress));
    let mut ledger = SimulatedOwnership::default();
    let event = ledger.completion(&first, &pending, None, 0);
    run(&ra, ra::Command::Registered(Box::new(event))).await;
    let actions = effects(&ra);
    assert!(!actions
        .iter()
        .any(|e| matches!(e, ra::Effect::RuntimeOpen { .. })));
    assert!(ledger.registrations.is_empty());
    completed_host(&mut core, "H");
    core.register_runtime(c.clone()).result.unwrap();
    core.register_type(&id, &c).result.unwrap();
    run(&ra, ra::Command::RetryOpen(first.body.unit.id.clone())).await;
    let retry = issued(&effects(&ra));
    assert_ne!(retry.lookup, first.lookup);
    let actual = core_result(&mut core, &retry, h1::LookupModel::default());
    let event = ledger.completion(&retry, &actual, None, 0);
    run(&ra, ra::Command::Registered(Box::new(event))).await;
    let actions = effects(&ra);
    assert!(actions
        .iter()
        .any(|e| matches!(e, ra::Effect::RuntimeOpen { registration, .. }
        if registration.runtime == "R" && registration.acquisition == retry.lookup)));
    assert_eq!(ledger.usages, BTreeSet::from([retry.lookup]));
    close_fixture(store, ra).await;
}
#[tokio::test]
async fn mismatch_candidate_acceptance_and_rejection_follow_ra_upgrade_rule() {
    for upgrade in [None, Some(6), Some(7), Some(8)] {
        let (store, ra) = fixture().await;
        let (mut core, id, c) = ready_core();
        let mut requested_version = c.version.clone();
        requested_version.instance = 6;
        let issued = add(&ra, body(1, &id, &requested_version)).await;
        let result = core_result(
            &mut core,
            &issued,
            h1::LookupModel {
                deactivation: Some(h1::DeactivationOutcome::Success { scheduled: false }),
                ..h1::LookupModel::default()
            },
        );
        assert_eq!(result.result, Err(h1::Error::RegistrationVersionMismatch));
        assert_eq!(result.candidate.as_ref().unwrap().version, c.version);
        let mut ledger = SimulatedOwnership::default();
        let event = ledger.completion(&issued, &result, upgrade, 0);
        run(&ra, ra::Command::Registered(Box::new(event.clone()))).await;
        let actions = effects(&ra);
        ledger.consume_releases(&actions);
        if upgrade == Some(8) {
            assert!(!actions
                .iter()
                .any(|e| matches!(e, ra::Effect::RuntimeOpen { .. })));
            assert_eq!(
                ledger.releases,
                [(false, issued.lookup), (true, issued.lookup)]
            );
            assert!(ra.snapshot([1; 16]).committed.unwrap().runtime.is_none());
        } else {
            assert!(actions.iter().any(
                |e| matches!(e, ra::Effect::RuntimeOpen { body, registration, .. }
                if body.service.version.instance == 7 && registration.runtime == "R")
            ));
            assert_eq!(
                ra.snapshot([1; 16])
                    .committed
                    .unwrap()
                    .body
                    .replica
                    .version
                    .instance,
                7
            );
            assert!(ledger.releases.is_empty());
        }
        assert_eq!(
            run(&ra, ra::Command::Registered(Box::new(event))).await,
            ra::Disposition::IgnoredStale
        );
        assert!(effects(&ra).is_empty());
        close_fixture(store, ra).await;
    }
}
#[tokio::test]
async fn notfound_candidate_is_diagnostic_unacquired_not_disposal_owned_by_ra() {
    for outcome in [
        h1::DeactivationOutcome::Success { scheduled: true },
        h1::DeactivationOutcome::Failure(h1::Error::InvalidState),
    ] {
        let (store, ra) = fixture().await;
        let (mut core, id, c) = ready_core();
        let mut requested = c.version.clone();
        requested.instance -= 1;
        let issued = add(&ra, body(1, &id, &requested)).await;
        let result = core_result(
            &mut core,
            &issued,
            h1::LookupModel {
                deactivation: Some(outcome),
                ..h1::LookupModel::default()
            },
        );
        assert_eq!(result.result, Err(h1::Error::NotFound));
        let mut ledger = SimulatedOwnership::default();
        let event = ledger.completion(&issued, &result, None, 0);
        assert!(!event.registration.host.is_empty()); // diagnostic candidate preserved
        assert_eq!(event.registration.acquisition, 0);
        assert!(!event.usage_acquired);
        run(&ra, ra::Command::Registered(Box::new(event))).await;
        let actions = effects(&ra);
        assert!(!actions.iter().any(|e| matches!(
            e,
            ra::Effect::RuntimeOpen { .. }
                | ra::Effect::ReleaseRegistration(_)
                | ra::Effect::ReleaseUsage { .. }
        )));
        assert!(ledger.registrations.is_empty());
        assert!(ledger.usages.is_empty());
        assert_eq!(ledger.diagnostics[0].process_id, 42);
        assert!(ra.snapshot([1; 16]).committed.unwrap().runtime.is_none());
        close_fixture(store, ra).await;
    }
}
#[tokio::test]
async fn actual_h1_success_with_simulated_usage_failure_does_not_attach() {
    let (store, ra) = fixture().await;
    let (mut core, id, c) = ready_core();
    let issued = add(&ra, body(1, &id, &c.version)).await;
    let result = core_result(&mut core, &issued, h1::LookupModel::default());
    let mut ledger = SimulatedOwnership::default();
    let event = ledger.completion(&issued, &result, None, -88);
    run(&ra, ra::Command::Registered(Box::new(event))).await;
    let actions = effects(&ra);
    ledger.consume_releases(&actions);
    assert_eq!(ledger.releases, [(false, issued.lookup)]);
    assert!(ledger.usages.is_empty());
    assert!(ra.snapshot([1; 16]).committed.unwrap().runtime.is_none());
    assert!(!actions
        .iter()
        .any(|e| matches!(e, ra::Effect::RuntimeOpen { .. })));
    close_fixture(store, ra).await;
}
#[tokio::test]
async fn overlapping_actual_lookup_results_and_replay_release_exact_unused_reference() {
    let (store, ra) = fixture().await;
    let (mut core, id, c) = ready_core();
    let first = add(&ra, body(1, &id, &c.version)).await;
    run(&ra, ra::Command::RetryOpen(first.body.unit.id.clone())).await;
    let second = issued(&effects(&ra));
    assert_eq!(second.token, first.token);
    assert_ne!(second.lookup, first.lookup);
    let mut ledger = SimulatedOwnership::default();
    let one = ledger.completion(
        &first,
        &core_result(&mut core, &first, h1::LookupModel::default()),
        None,
        0,
    );
    let two = ledger.completion(
        &second,
        &core_result(&mut core, &second, h1::LookupModel::default()),
        None,
        0,
    );
    run(&ra, ra::Command::Registered(Box::new(one.clone()))).await;
    effects(&ra);
    run(&ra, ra::Command::Registered(Box::new(two.clone()))).await;
    ledger.consume_releases(&effects(&ra));
    assert_eq!(
        ledger.releases,
        [(false, second.lookup), (true, second.lookup)]
    );
    assert_eq!(ledger.registrations, BTreeSet::from([first.lookup]));
    assert_eq!(ledger.usages, BTreeSet::from([first.lookup]));
    for event in [one, two] {
        assert_eq!(
            run(&ra, ra::Command::Registered(Box::new(event))).await,
            ra::Disposition::IgnoredStale
        );
        assert!(effects(&ra).is_empty());
    }
    close_fixture(store, ra).await;
}
#[tokio::test]
async fn retained_simulated_usage_is_not_reacquired_on_new_h1_registration() {
    let (store, ra) = fixture().await;
    let (mut core, id, c) = ready_core();
    let first = add(&ra, body(1, &id, &c.version)).await;
    let mut ledger = SimulatedOwnership::default();
    let event = ledger.completion(
        &first,
        &core_result(&mut core, &first, h1::LookupModel::default()),
        None,
        0,
    );
    run(&ra, ra::Command::Registered(Box::new(event))).await;
    effects(&ra);
    // Separately supplied simulated physical-host-down premise, NOT translation
    // of RuntimeClosed. No live event consumer exists in this composition.
    run(
        &ra,
        ra::Command::HostDown(Box::new(ra::HostDown {
            unit: first.body.unit.id.clone(),
            host: "H".into(),
        })),
    )
    .await;
    ledger.consume_releases(&effects(&ra));
    assert_eq!(ledger.usages, BTreeSet::from([first.lookup]));
    run(&ra, ra::Command::RetryOpen(first.body.unit.id.clone())).await;
    let retry = issued(&effects(&ra));
    assert_eq!(retry.retained_usage, Some(first.lookup));
    let event = ledger.completion(
        &retry,
        &core_result(&mut core, &retry, h1::LookupModel::default()),
        None,
        0,
    );
    assert!(!event.usage_acquired);
    run(&ra, ra::Command::Registered(Box::new(event))).await;
    ledger.consume_releases(&effects(&ra));
    assert_eq!(ledger.usages, BTreeSet::from([first.lookup]));
    assert_eq!(
        ra.snapshot([1; 16]).committed.unwrap().usage,
        Some(first.lookup)
    );
    close_fixture(store, ra).await;
}
#[tokio::test]
async fn first_ra_notification_spelling_from_lookup_or_event_producer_is_retained() {
    for event_first in [false, true] {
        let (store, ra) = fixture().await;
        let (mut core, stored, c) = ready_core();
        let initial_events = core.take_closed_registered_events();
        let [h1::ClosedRegisteredEvent::Registered {
            registration: actual_event,
            ..
        }] = initial_events.as_slice()
        else {
            panic!("actual event");
        };
        let mut requested = stored.clone();
        requested.service_type.name = "widget".into();
        let first = add(&ra, body(1, &requested, &c.version)).await;
        let mut result = core_result(&mut core, &first, h1::LookupModel::default());
        assert_eq!(
            result
                .candidate
                .as_ref()
                .unwrap()
                .type_instance
                .service_type
                .name,
            "widget"
        );
        if event_first {
            // Actual event snapshot projected to an ALREADY ISSUED completion.
            // This is not a newly enabled unsolicited Hosting event RA consumer.
            result.candidate = Some(actual_event.clone());
        }
        let mut ledger = SimulatedOwnership::default();
        let event = ledger.completion(&first, &result, None, 0);
        run(&ra, ra::Command::Registered(Box::new(event))).await;
        let actions = effects(&ra);
        let work = actions
            .iter()
            .find_map(|e| match e {
                ra::Effect::RequestServiceTypeWork { work, .. } => Some(*work),
                _ => None,
            })
            .unwrap();
        let expected = if event_first { "Widget" } else { "widget" };
        assert_eq!(ra.service_types()[0].info.versioned.id.name, expected);

        core.unregister_runtime("R").unwrap();
        let mut next_context = context("NextRuntime", "H");
        next_context.version.instance = 8;
        next_context.code_package = "NextCode".into();
        core.register_runtime(next_context.clone()).result.unwrap();
        core.register_type(&stored, &next_context).result.unwrap();
        let second = add(&ra, body(2, &stored, &next_context.version)).await;
        let result = core_result(&mut core, &second, h1::LookupModel::default());
        let event = ledger.completion(&second, &result, None, 0);
        run(&ra, ra::Command::Registered(Box::new(event))).await;
        effects(&ra);
        assert_eq!(ra.service_types()[0].info.versioned.id.name, expected);
        assert_eq!(ra.service_types()[0].info.versioned.version.instance, 8);
        assert_eq!(ra.service_types()[0].info.code_package, "NextCode");
        assert_eq!(ra.service_types()[0].references, 2);
        run(&ra, ra::Command::CaptureServiceTypeWork { work }).await;
        let actions = effects(&ra);
        let [ra::Effect::ServiceTypeWorkCaptured {
            body: Some(notification),
            ..
        }] = actions.as_slice()
        else {
            panic!("actual notification {actions:?}");
        };
        assert_eq!(notification.infos.0[0].versioned.id.name, expected);
        assert_eq!(notification.infos.0[0].code_package, "NextCode");
        run(&ra, ra::Command::CompleteServiceTypeWork { work }).await;
        effects(&ra);
        assert_eq!(
            core.require_live_composition(),
            Err(h1::Error::UnsupportedCapability)
        );
        assert!(core
            .closed_registered_events()
            .iter()
            .any(|e| matches!(e, h1::ClosedRegisteredEvent::RuntimeClosed { .. })));
        // The RuntimeClosed remained typed; it was not sent as HostDown.
        close_fixture(store, ra).await;
    }
}

#[tokio::test]
async fn late_actual_h1_result_after_recreation_cannot_retire_active_reference() {
    let (store, ra) = fixture().await;
    let (mut core, id, c) = ready_core();
    let first = add(&ra, body(1, &id, &c.version)).await;
    let mut ledger = SimulatedOwnership::default();
    let old_result = core_result(&mut core, &first, h1::LookupModel::default());
    let old = ledger.completion(&first, &old_result, None, 0);
    run(
        &ra,
        ra::Command::Remove {
            body: Box::new(wire::RemoveInstance::new((*first.body).clone(), false)),
            source: admission().fm_source,
            generation: wire::GenerationHeader {
                number: admission().generation,
                for_fm_replica: false,
            },
        },
    )
    .await;
    effects(&ra);
    let mut next = (*first.body).clone();
    next.replica.instance += 1;
    let fresh = add(&ra, next).await;
    assert_ne!(fresh.token, first.token);
    let actual = core_result(&mut core, &fresh, h1::LookupModel::default());
    let new = ledger.completion(&fresh, &actual, None, 0);
    run(&ra, ra::Command::Registered(Box::new(new))).await;
    effects(&ra);
    assert_eq!(
        run(&ra, ra::Command::Registered(Box::new(old.clone()))).await,
        ra::Disposition::IgnoredStale
    );
    ledger.consume_releases(&effects(&ra));
    assert_eq!(
        ledger.releases,
        [(false, first.lookup), (true, first.lookup)]
    );
    assert_eq!(ledger.registrations, BTreeSet::from([fresh.lookup]));
    assert_eq!(ledger.usages, BTreeSet::from([fresh.lookup]));
    assert_eq!(
        ra.snapshot([1; 16])
            .committed
            .unwrap()
            .runtime
            .as_ref()
            .unwrap()
            .acquisition,
        fresh.lookup
    );
    assert_eq!(
        run(&ra, ra::Command::Registered(Box::new(old))).await,
        ra::Disposition::IgnoredStale
    );
    assert!(effects(&ra).is_empty());
    close_fixture(store, ra).await;
}
