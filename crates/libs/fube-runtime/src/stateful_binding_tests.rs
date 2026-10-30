//! Real RAP-admission/issuer fixtures, not a deployed/native qualification.
use super::*;
use crate::extensions::{NativeControlBinding, NativeInvocation, NativeInvocationKind};

struct CaptureBinder {
    calls: Arc<StatefulCalls>,
    issued: Arc<Mutex<Vec<NativeInvocation>>>,
}
impl NativeControlBinding for CaptureBinder {
    fn bind_invocation(&self, proof: NativeInvocation) -> Result<Arc<dyn IPrimaryReplicator>> {
        proof.validate()?;
        self.issued.lock().unwrap().push(proof);
        // All fixture control state is in this same Calls backend. No extra
        // engine, listener, provider or executor is constructed by binding.
        Ok(Arc::new(ReplicatorFixture(
            self.calls.clone(),
            20,
            self.calls.partitions.lock().unwrap()[0].clone(),
        )))
    }
}

#[tokio::test]
async fn native_issuer_freezes_full_epochs_memberships_and_fences_old_proofs() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let partition = calls.partitions.lock().unwrap()[0].clone();
    let native = partition.clone();
    assert_eq!(native.binding().identity, *partition.identity());
    assert_eq!(native.binding().generation, 1);
    let actor = uuid::Uuid::new_v4();
    assert!(native
        .reserve_engine(uuid::Uuid::new_v4(), 100, actor)
        .is_err());
    native
        .reserve_engine(partition.identity().partition_id, 100, actor)
        .unwrap();
    assert!(native
        .reserve_engine(partition.identity().partition_id, 100, actor)
        .is_err());
    let issued = Arc::new(Mutex::new(Vec::new()));
    let binder: Arc<dyn NativeControlBinding> = Arc::new(CaptureBinder {
        calls: calls.clone(),
        issued: issued.clone(),
    });
    assert!(native
        .install_binding(uuid::Uuid::new_v4(), &binder)
        .is_err());
    native.install_binding(actor, &binder).unwrap();
    let mut local = open.local.clone();
    local.previous_role = 3;
    let mut remote = peer(&local, 2);
    remote.first_lsn = -1;
    remote.last_lsn = -1;
    let mut dropped = peer(&local, 3);
    dropped.state = 3;
    dropped.up = false;
    let mut configuration = control(&open, "UpdateConfiguration", vec![local, remote, dropped]);
    configuration.flags = 2;
    configuration.unit.current.configuration += 1;
    configuration.unit.previous = open.unit.current.clone();
    h.send("UpdateConfiguration", &configuration).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let proof = issued.lock().unwrap().last().unwrap().clone();
    assert_eq!(proof.kind(), NativeInvocationKind::CatchUpConfiguration);
    assert_eq!(proof.actor(), Some(actor));
    let (cc, pc) = proof.configuration().unwrap();
    assert_eq!((cc.write_quorum, cc.local_member), (2, true));
    assert_eq!(
        (pc.unwrap().write_quorum, pc.unwrap().local_member),
        (2, true)
    );
    assert_eq!(cc.replicas.len(), 1);
    assert_eq!(
        (cc.replicas[0].first_lsn, cc.replicas[0].last_lsn),
        (-1, -1)
    );
    let original_epochs = proof.full_epochs();
    assert_eq!(
        original_epochs.0.configuration,
        configuration.unit.current.configuration
    );
    assert_eq!(
        original_epochs.1.configuration,
        configuration.unit.previous.configuration
    );
    assert_ne!(original_epochs.0.configuration, cc.epoch.configuration);
    proof.enter().unwrap();
    assert!(proof.clone().enter().is_err());
    let mut observational = partition.status().unwrap();
    observational.configuration.clear();
    observational.current_epoch.configuration_number += 100;
    let record = native
        .lookup_record(NativeInvocationKind::CatchUpConfiguration, None, None)
        .unwrap();
    assert_eq!(record.full_epochs(), original_epochs);
    let delayed = async move { record.enter() };
    configuration.unit.current.configuration += 1;
    h.send("UpdateConfiguration", &configuration).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert!(delayed.await.is_err());
    assert!(proof.validate().is_err());
    assert_eq!(proof.full_epochs(), original_epochs);
}

#[tokio::test]
async fn issuer_rejects_ambiguous_id_and_retained_generation_after_close() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let partition = calls.partitions.lock().unwrap()[0].clone();
    let native = partition.clone();
    let mut first = peer(&open.local, 2);
    first.current_role = 2;
    first.previous_role = 0;
    let mut second = peer(&open.local, 3);
    second.id = first.id;
    second.current_role = 2;
    second.previous_role = 0;
    for replica in [first.clone(), second.clone()] {
        let build = control(&open, "ReplicatorBuildIdleReplica", vec![replica]);
        h.send("ReplicatorBuildIdleReplica", &build).await;
        let reply: ProxyReply =
            from_slice(&h.receive("ReplicatorBuildIdleReplicaReply").await.body).unwrap();
        assert_eq!(reply.error.error, 0);
        h.engine
            .shared
            .stateful
            .wait_remote_idle(
                [2; 16],
                [if build.remote.0[0].node.id.0[0] == 2 {
                    2
                } else {
                    3
                }; 16],
            )
            .await;
    }
    assert!(native
        .lookup_record(NativeInvocationKind::Build, Some(first.id), None)
        .is_err());
    let captured = native
        .lookup_record(NativeInvocationKind::Build, Some(first.id), Some([2; 16]))
        .unwrap();
    assert_eq!(captured.replica().unwrap().node_id, [2; 16]);
    h.send("ReplicaClose", &open).await;
    h.receive("ReplicaCloseReply").await;
    assert!(native.validate().is_err());
    assert!(captured.validate().is_err());
    assert!(partition.validate().is_err());
}
