use super::*;
use crate::extensions::{NativeReplicaDescriptor, NativeReplicaSet};

const EPOCH: Epoch = Epoch {
    data_loss_number: 1,
    configuration_number: 1 << 32,
};
fn status() -> ReplicaDiagnostics {
    ReplicaDiagnostics {
        lifecycle: ReplicaLifecycle::Open,
        service_role: ReplicaRole::Primary,
        replicator_role: ReplicaRole::Primary,
        replicator_open: true,
        current_role: ReplicaRole::Primary,
        current_epoch: EPOCH,
        previous_epoch: Epoch::INVALID,
        configuration_stage: ConfigurationStage::Current,
        catch_up: CatchUpResult::Completed,
        minimum_replica_set_size: 1,
        configuration: vec![],
        service_address: String::new(),
        replication_address: String::new(),
        progress: ReplicationProgress::EMPTY,
    }
}
#[test]
fn stable_singleton_retains_every_original_host_qualification() {
    assert_eq!(stable_facts(&status()).unwrap().singleton, Some(EPOCH));
    let variants: &[fn(&mut ReplicaDiagnostics)] = &[
        |s| s.lifecycle = ReplicaLifecycle::Closing,
        |s| s.lifecycle = ReplicaLifecycle::Opening,
        |s| s.lifecycle = ReplicaLifecycle::Closed,
        |s| s.configuration_stage = ConfigurationStage::CatchUp,
        |s| s.configuration_stage = ConfigurationStage::PreWriteRevoke,
        |s| s.catch_up = CatchUpResult::NotStarted,
        |s| s.replicator_open = false,
        |s| s.current_role = ReplicaRole::ActiveSecondary,
        |s| s.service_role = ReplicaRole::ActiveSecondary,
        |s| s.replicator_role = ReplicaRole::ActiveSecondary,
        |s| s.minimum_replica_set_size = 2,
        |s| s.configuration.push(primary()),
    ];
    for mutate in variants {
        let mut s = status();
        mutate(&mut s);
        assert!(stable_facts(&s).unwrap().singleton.is_none(), "{s:?}");
    }
    let mut s = status();
    s.current_epoch.configuration_number += 7;
    assert_eq!(stable_facts(&s).unwrap().singleton, Some(EPOCH));
    s.current_epoch.configuration_number += 1 << 32;
    assert_ne!(stable_facts(&s).unwrap().singleton, Some(EPOCH));
}
fn primary() -> NativeReplicaDescriptor {
    NativeReplicaDescriptor {
        node_id: [2; 16],
        node_instance: 1,
        replica_id: 2,
        incarnation: 12,
        role: ReplicaRole::Primary,
        previous_role: ReplicaRole::None,
        up: true,
        ready: true,
        address: "lower-selected-endpoint".into(),
        progress: ReplicationProgress::EMPTY,
        must_catch_up: false,
    }
}
#[test]
fn secondary_routing_is_observation_time_qualified_and_ambiguous_primary_fails() {
    let mut s = status();
    s.current_role = ReplicaRole::ActiveSecondary;
    s.service_role = ReplicaRole::ActiveSecondary;
    s.replicator_role = ReplicaRole::ActiveSecondary;
    s.configuration.push(primary());
    let projected = stable_facts(&s).unwrap().primary.unwrap();
    assert_eq!(projected.0, EPOCH);
    assert_eq!(projected.1.address, s.configuration[0].address);
    let variants: &[fn(&mut ReplicaDiagnostics)] = &[
        |s| s.lifecycle = ReplicaLifecycle::Closing,
        |s| s.replicator_open = false,
        |s| s.current_role = ReplicaRole::Primary,
        |s| s.service_role = ReplicaRole::Primary,
        |s| s.replicator_role = ReplicaRole::Primary,
        |s| s.configuration[0].role = ReplicaRole::ActiveSecondary,
        |s| s.configuration[0].up = false,
        |s| s.configuration[0].ready = false,
    ];
    for mutate in variants {
        let mut changed = s.clone();
        mutate(&mut changed);
        assert!(stable_facts(&changed).unwrap().primary.is_none());
    }
    // These were never secondary routing predicates.
    s.configuration_stage = ConfigurationStage::CatchUp;
    s.catch_up = CatchUpResult::NotStarted;
    assert!(stable_facts(&s).unwrap().primary.is_some());
    s.configuration.push(primary());
    assert!(stable_facts(&s).is_err());
}
#[test]
fn prewrite_qualification_stays_with_issuer_and_preserves_full_epoch_projection() {
    let mut s = status();
    s.current_role = ReplicaRole::ActiveSecondary;
    s.configuration_stage = ConfigurationStage::PreWriteRevoke;
    s.current_epoch.configuration_number += (1 << 32) + 7;
    s.previous_epoch = Epoch {
        configuration_number: EPOCH.configuration_number + 3,
        ..EPOCH
    };
    let projection = crate::stateful::binding::prewrite_projection;
    let (cc, pc) = projection(&s).unwrap();
    assert_eq!(cc.configuration, 2 << 32);
    assert_eq!(pc.configuration, 1 << 32);
    let variants: &[fn(&mut ReplicaDiagnostics)] = &[
        |s| s.configuration_stage = ConfigurationStage::Current,
        |s| s.configuration_stage = ConfigurationStage::CatchUp,
        |s| s.current_role = ReplicaRole::Primary,
        |s| s.current_role = ReplicaRole::IdleSecondary,
        |s| s.service_role = ReplicaRole::ActiveSecondary,
        |s| s.replicator_role = ReplicaRole::ActiveSecondary,
        |s| s.replicator_open = false,
        |s| s.lifecycle = ReplicaLifecycle::Opening,
        |s| s.lifecycle = ReplicaLifecycle::Closing,
        |s| s.lifecycle = ReplicaLifecycle::Closed,
    ];
    for mutate in variants {
        let mut changed = s.clone();
        mutate(&mut changed);
        assert!(projection(&changed).is_none());
    }
    // Epoch matching/denial remains the lower's job, not a second authority.
    let cc = NativeReplicaSet {
        epoch: cc,
        replicas: vec![],
        write_quorum: 1,
        local_member: true,
    };
    assert_eq!(
        project_configuration(&cc)
            .unwrap()
            .epoch
            .configuration_number,
        2 << 32
    );
}
