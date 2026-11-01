//! Real serialized consumer decisions over committed RA state. Lookup/runtime
//! attachment premises below are qualification-only; no H2/RAP/deployment claim.
use fube_ra::{entity::*, protocol::*};
use fube_ra_store::{MemoryBackend, Settings as StoreSettings, Store};
use std::{sync::Arc, time::Duration};

const WAIT: Duration = Duration::from_secs(3);
fn admission() -> Admission {
    Admission {
        local_node: NodeInstance {
            id: [4; 16],
            instance: 20,
        },
        fm_source: NodeInstance {
            id: [8; 16],
            instance: 30,
        },
        generation: GenerationNumber {
            generation: 7,
            owner: [8; 16],
        },
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    }
}
fn readiness() -> HostingReadiness {
    HostingReadiness {
        fm_ready: true,
        node_active: true,
        upgrading: false,
        ra_open: true,
        ra_closing: false,
    }
}
fn body() -> AddInstance {
    let mut b = AddInstance::default();
    b.unit.id.guid.0 = [1; 16];
    b.unit.consistency.id = b.unit.id.clone();
    b.unit.current = Epoch {
        configuration: 5,
        data_loss: 1,
    };
    b.replica.node = admission().local_node;
    b.replica.id = 40;
    b.replica.instance = 50;
    b.replica.state = 1;
    b.replica.version = VersionInstance::default();
    b.service.name = "fabric:/App/service".into();
    b.service.service_type = ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: "App".into(),
                number: 1,
            },
            name: "Pkg".into(),
        },
        name: "Widget".into(),
    };
    b
}
fn add(b: AddInstance) -> Command {
    Command::Add {
        body: Box::new(b),
        source: admission().fm_source,
        generation: GenerationHeader {
            number: admission().generation,
            for_fm_replica: false,
        },
    }
}

#[tokio::test]
async fn registered_event_revalidates_committed_type_local_version_activation_and_readiness() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let executor = Executor::new(store.clone(), admission(), Settings::default()).unwrap();
    let b = body();
    assert_eq!(
        executor
            .submit(add(b.clone()))
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap(),
        Disposition::Applied
    );
    let owned_lookup_effects = executor.take_effects();
    assert!(matches!(
        owned_lookup_effects[0].effect,
        Effect::Open { .. }
    ));
    let before = executor.snapshot(b.unit.id.guid.0).committed.unwrap();
    for variant in 0..5 {
        let mut event = HostingRegistered {
            unit: b.unit.id.clone(),
            readiness: readiness(),
            service_type: b.service.service_type.clone(),
            version: b.replica.version.clone(),
            activation: PartitionId::default(),
        };
        match variant {
            0 => event.service_type.name = "wIDGET".into(),
            1 => event.service_type.package.name = "Other".into(),
            2 => event.version.instance += 1,
            3 => event.activation.guid.0 = b.unit.id.guid.0,
            _ => event.readiness.fm_ready = false,
        }
        let result = executor
            .submit(Command::HostingRegistered(Box::new(event)))
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        assert_eq!(
            result,
            match variant {
                0 => Disposition::HostingOpenRetryRequired,
                4 => Disposition::IgnoredNotReady,
                _ => Disposition::IgnoredStale,
            }
        );
        assert_eq!(
            executor
                .snapshot(b.unit.id.guid.0)
                .committed
                .as_ref()
                .unwrap(),
            &before
        );
        assert!(executor.take_effects().is_empty());
    }
    executor.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn runtime_closed_and_host_closed_remain_distinct_exact_current_owner_decisions() {
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), StoreSettings::default())
            .await
            .unwrap(),
    );
    let executor = Executor::new(store.clone(), admission(), Settings::default()).unwrap();
    let b = body();
    executor
        .submit(add(b.clone()))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    let effects = executor.take_effects();
    let Effect::Open { token, lookup, .. } = effects[0].effect else {
        panic!("issued lookup required");
    };
    // Actual issued ID/token; simulated acquisition/usage is only this fixture.
    executor
        .submit(Command::Registered(Box::new(RegistrationCompletion {
            unit: b.unit.id.clone(),
            token,
            lookup,
            registration: RuntimeRegistration {
                service_type: b.service.service_type.clone(),
                host: "HOST".into(),
                runtime: "RUNTIME".into(),
                acquisition: lookup,
                code_package: "Code".into(),
            },
            error: 0,
            retryable: false,
            version: b.replica.version.clone(),
            required_upgrade: 0,
            upgrade_present: false,
            usage_acquired: true,
            usage_error: 0,
            error_message: String::new(),
        })))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    let owned_rap_obligations = executor.take_effects();
    assert!(owned_rap_obligations
        .iter()
        .any(|e| matches!(e.effect, Effect::RuntimeOpen { .. })));
    let before = executor.snapshot(b.unit.id.guid.0).committed.unwrap();
    for runtime in ["RUNTIME", "runtime", "OTHER"] {
        let result = executor
            .submit(Command::HostingRuntimeClosed(Box::new(
                HostingRuntimeClosed {
                    unit: b.unit.id.clone(),
                    readiness: readiness(),
                    host: "HOST".into(),
                    runtime: runtime.into(),
                },
            )))
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        assert_eq!(
            result,
            if runtime == "RUNTIME" {
                Disposition::HostingRuntimeCloseRequired
            } else {
                Disposition::IgnoredStale
            }
        );
    }
    let result = executor
        .submit(Command::HostingHostClosed(Box::new(HostingHostClosed {
            unit: b.unit.id.clone(),
            readiness: readiness(),
            host: "HOST".into(),
            activity: "owned-activity".into(),
        })))
        .unwrap()
        .wait(WAIT)
        .await
        .unwrap();
    assert_eq!(result, Disposition::HostingHostCrashRequired);
    assert_eq!(
        executor
            .snapshot(b.unit.id.guid.0)
            .committed
            .as_ref()
            .unwrap(),
        &before
    );
    assert!(executor.take_effects().is_empty());
    executor.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}

#[test]
fn statuses_use_expiring_type_history_coalescing_priority_and_separate_message_sequence() {
    let mut consumer = LocalStatusConsumer::new(4, 16 * 1024).unwrap();
    let a = body().service.service_type;
    let mut b = a.clone();
    b.name = "Other".into();
    assert_eq!(
        consumer.consume(a.clone(), 10, false, true, 0).unwrap(),
        Disposition::HostingStatusProcessed
    );
    assert_eq!(consumer.message_sequence(), 1);
    assert_eq!(
        consumer.consume(a.clone(), 10, true, true, 1).unwrap(),
        Disposition::IgnoredStale
    );
    assert_eq!(
        consumer.consume(a.clone(), 9, true, true, 1).unwrap(),
        Disposition::IgnoredStale
    );
    consumer.consume(b, 11, true, true, 1).unwrap();
    assert_eq!(consumer.next_enabled(), Some(false));
    assert_eq!(consumer.message_sequence(), 1); // disabled priority suppresses increment
    consumer.consume(a.clone(), 12, true, true, 2).unwrap();
    assert_eq!(consumer.next_enabled(), Some(true));
    assert_eq!(consumer.message_sequence(), 2);
    consumer.tick(36_000_000_000).unwrap();
    assert_eq!(consumer.pending().len(), 2); // cleanup is not FM ACK
    consumer.consume(a, 1, false, true, 36_000_000_001).unwrap();
    assert_eq!(consumer.next_enabled(), Some(false)); // expired history admits lower seq
    assert_eq!(consumer.message_sequence(), 3);
}

#[test]
fn status_capacity_and_not_ready_preserve_original_pending_work() {
    let mut consumer = LocalStatusConsumer::new(1, 16 * 1024).unwrap();
    let a = body().service.service_type;
    let mut b = a.clone();
    b.name = "Other".into();
    assert_eq!(
        consumer.consume(a.clone(), 1, false, false, 0).unwrap(),
        Disposition::IgnoredNotReady
    );
    assert!(consumer.pending().is_empty());
    consumer.consume(a.clone(), 1, false, true, 0).unwrap();
    let before = consumer.pending().to_vec();
    assert_eq!(
        consumer.consume(b, 2, true, true, 1),
        Err(Failure::Capacity)
    );
    assert_eq!(consumer.pending(), before);
    assert_eq!(consumer.message_sequence(), 1);
    assert!(consumer.tick(-1).is_err());
    assert_eq!(consumer.pending(), before);
    consumer.consume(a, 2, true, true, 2).unwrap();
    assert_eq!(consumer.pending().len(), 1);
    assert_eq!(consumer.message_sequence(), 2);
}
