use super::*;
use AccessStatus::{
    Granted as G, NoWriteQuorum as Q, NotPrimary as N, ReconfigurationPending as T,
};
use ReplicaRole::{Idle, None, Primary, Secondary};

pub(super) fn status() -> ReplicaStatus {
    ReplicaStatus {
        lifecycle: ReplicaLifecycle::Open,
        service_role: Primary,
        replicator_role: Primary,
        replicator_open: true,
        current_role: Primary,
        current_epoch: Epoch {
            data_loss: 1,
            configuration: 2 << 32,
        },
        previous_epoch: Epoch::INVALID,
        configuration_stage: ConfigurationStage::Current,
        catch_up: CatchUpResult::NotStarted,
        minimum_replica_set_size: 1,
        configuration: vec![],
        service_address: "service".into(),
        replication_address: "control".into(),
        progress: ReplicationProgress::EMPTY,
    }
}
fn peer(id: i64, role: ReplicaRole, previous_role: ReplicaRole, ready: bool) -> ReplicaInformation {
    ReplicaInformation {
        node_id: [id as u8; 16],
        node_instance: 1,
        replica_id: id,
        incarnation: 1,
        role,
        previous_role,
        up: true,
        ready,
        address: format!("peer-{id}"),
        progress: ReplicationProgress::EMPTY,
        must_catch_up: false,
    }
}

#[test]
fn primary_epoch_masks_membership_bits_and_orders_data_loss_first() {
    // Native Epoch()/InvalidEpoch() is (0,0), not the -1 LSN sentinel.
    assert_eq!(
        Epoch::INVALID,
        Epoch {
            data_loss: 0,
            configuration: 0
        }
    );
    let epoch = Epoch {
        data_loss: 7,
        configuration: (3 << 32) + 99,
    };
    assert_eq!(epoch.primary().configuration, 3 << 32);
    assert!(
        epoch
            > Epoch {
                data_loss: 6,
                configuration: i64::MAX
            }
    );
    assert_ne!(Epoch::INVALID, epoch);
}

#[test]
fn empty_progress_is_initialized_not_a_control_counter() {
    assert!(ReplicationProgress::EMPTY.ensure_initialized().is_ok());
    for (first, last) in [(-1, -1), (-1, 0), (1, 0)] {
        assert!(ReplicationProgress { first, last }
            .ensure_initialized()
            .is_err());
    }
    assert!(ReplicationProgress { first: 1, last: 2 }
        .ensure_initialized()
        .is_ok());
}

#[test]
fn lifecycle_precedes_reconfiguration_and_role_is_not_authority() {
    let mut s = status();
    assert_eq!(s.access(), (G, G));
    s.lifecycle = ReplicaLifecycle::Opening;
    s.replicator_role = None;
    assert_eq!(s.access(), (T, T));
    s.replicator_role = Primary;
    assert_eq!(s.access(), (G, G));
    s.replicator_open = false;
    assert_eq!(s.access(), (G, Q));
    s.current_role = Idle;
    assert_eq!(s.access(), (N, N));
    s.current_role = Primary;
    s.lifecycle = ReplicaLifecycle::Closing;
    assert_eq!(s.access(), (N, N));
    s.lifecycle = ReplicaLifecycle::Closed;
    assert_eq!(s.access(), (N, N));
}

#[test]
fn minimum_is_configured_cc_size_not_ready_count_or_target() {
    let mut s = status();
    s.minimum_replica_set_size = 3;
    assert_eq!(s.access(), (G, Q));
    s.configuration = vec![
        peer(2, Secondary, None, true),
        peer(3, Secondary, None, false),
    ];
    assert_eq!(s.access(), (G, G)); // 3 configured, 2 Ready satisfies min=3.
    s.configuration[0].up = false;
    assert_eq!(s.access(), (G, Q));
    s.minimum_replica_set_size = 1;
    assert_eq!(s.access(), (G, Q)); // minimum cannot manufacture quorum.
}

#[test]
fn promotion_access_before_and_after_catchup_and_current_configuration() {
    let mut s = status();
    s.previous_epoch = Epoch {
        data_loss: 1,
        configuration: 1 << 32,
    };
    s.configuration_stage = ConfigurationStage::CatchUp;
    s.service_role = Secondary;
    assert_eq!(s.access(), (T, T));
    s.service_role = Primary;
    assert_eq!(s.access(), (T, T));
    s.catch_up = CatchUpResult::Completed;
    assert_eq!(s.access(), (G, T));
    s.configuration_stage = ConfigurationStage::Current;
    assert_eq!(s.access(), (G, G));
    s.catch_up = CatchUpResult::DataLossReported;
    assert_eq!(s.access(), (G, G));
}

#[test]
fn demotion_revokes_after_catchup_before_role_callback_completion() {
    let mut s = status();
    s.previous_epoch = Epoch {
        data_loss: 1,
        configuration: 1 << 32,
    };
    s.current_role = Secondary;
    s.configuration = vec![peer(2, Primary, Secondary, true)];
    s.configuration_stage = ConfigurationStage::PreWriteRevoke;
    assert_eq!(s.access(), (G, G));
    s.configuration_stage = ConfigurationStage::CatchUp;
    assert_eq!(s.access(), (G, T));
    s.catch_up = CatchUpResult::Completed;
    assert_eq!(s.access(), (N, N));
    s.service_role = Secondary;
    s.replicator_role = Secondary;
    assert_eq!(s.access(), (N, N));
}

#[test]
fn unchanged_primary_requires_pc_quorum_until_current_configuration() {
    let mut s = status();
    s.previous_epoch = Epoch {
        configuration: (2 << 32) + 3,
        data_loss: 1,
    };
    s.configuration = vec![
        peer(2, Secondary, None, true),
        peer(3, None, Secondary, false),
        peer(4, None, Secondary, false),
    ];
    assert_eq!(s.access(), (G, Q));
    s.catch_up = CatchUpResult::Completed;
    s.configuration_stage = ConfigurationStage::CatchUp;
    assert_eq!(s.access(), (G, Q));
    s.configuration_stage = ConfigurationStage::Current;
    assert_eq!(s.access(), (G, G));
}

#[test]
fn idle_secondary_and_native_public_access_values() {
    let mut s = status();
    s.current_role = Idle;
    assert_eq!(s.access(), (N, N));
    s.current_role = Secondary;
    s.service_role = Secondary;
    assert_eq!(s.access(), (N, N));
    s.previous_epoch = Epoch {
        configuration: 1 << 32,
        data_loss: 1,
    };
    assert_eq!(s.access(), (N, N)); // empty-remote Idle->Active takes precedence.
    assert_eq!(
        (
            G.native_value(),
            T.native_value(),
            N.native_value(),
            Q.native_value()
        ),
        (1, 2, 3, 4)
    );
}

#[test]
fn empty_typed_remote_vector_preserves_original_stateless_encoding() {
    use fube_transport::serialization::serde::{to_vec, EmptyObjectArray, ObjectArray};
    use serde::Serialize;
    #[derive(Serialize)]
    struct Old {
        remote: EmptyObjectArray,
    }
    #[derive(Serialize)]
    struct New {
        remote: ObjectArray<Vec<crate::wire::Replica>>,
    }
    assert_eq!(
        to_vec(&Old {
            remote: EmptyObjectArray
        })
        .unwrap(),
        to_vec(&New {
            remote: ObjectArray(vec![])
        })
        .unwrap()
    );
}

#[test]
fn stateful_partition_snapshot_cannot_grant_or_revive_owner_authority() {
    let authority = crate::lease::Authority::new(std::time::Instant::now(), 1_000_000_000).unwrap();
    let valid = Arc::new(AtomicBool::new(true));
    let partition = StatefulPartition {
        identity: ReplicaIdentity {
            service_name: "fabric:/app/service".into(),
            service_type: "StatefulType".into(),
            partition_id: uuid::Uuid::new_v4(),
            replica_id: 43,
            incarnation: 7,
            node_id: [1; 16],
            node_instance: 2,
        },
        kind: PartitionKind::Singleton,
        authority: authority.clone(),
        valid: valid.clone(),
        status: Arc::new(Mutex::new(status())),
        reporting: Option::None,
    };
    assert_eq!(partition.identity().replica_id, 43);
    assert_eq!(partition.identity().incarnation, 7);
    assert_eq!(partition.access_status().unwrap(), (G, G));
    let mut copy = partition.status().unwrap();
    copy.service_role = Secondary;
    assert_eq!(copy.service_role, Secondary);
    assert_eq!(partition.access_status().unwrap(), (G, G));
    valid.store(false, Ordering::Release);
    assert!(matches!(partition.status(), Err(Error::Closed)));
    let replacement = StatefulPartition {
        identity: ReplicaIdentity {
            incarnation: 8,
            ..partition.identity.clone()
        },
        valid: Arc::new(AtomicBool::new(true)),
        status: Arc::new(Mutex::new(status())),
        ..partition.clone()
    };
    assert!(replacement.status().is_ok());
    assert!(partition.status().is_err());
    authority.retire();
    assert!(replacement.access_status().is_err());
    assert!(replacement.authority_remaining().is_err());
}
