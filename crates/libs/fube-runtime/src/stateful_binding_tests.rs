//! Real RAP-admission/issuer fixtures, not a deployed/native qualification.
use super::*;
use crate::extensions::{NativeControlBinding, NativeInvocation, NativeInvocationKind};

struct CaptureBinder {
    calls: Arc<StatefulCalls>,
    issued: Arc<Mutex<Vec<NativeInvocation>>>,
    mode: u8,
}
impl NativeControlBinding for CaptureBinder {
    fn bind_invocation(&self, proof: NativeInvocation) -> Result<Arc<dyn IPrimaryReplicator>> {
        proof.validate()?;
        self.issued.lock().unwrap().push(proof);
        // All fixture control state is in this same Calls backend. No extra
        // engine, listener, provider or executor is constructed by binding.
        Ok(Arc::new(ReplicatorFixture(
            self.calls.clone(),
            self.mode,
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
        mode: 20,
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

#[tokio::test]
async fn explicitly_registered_custom_binding_selects_distinct_backend_once() {
    let mut h = Harness::new().await;
    let canonical = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let native = canonical.partitions.lock().unwrap()[0].clone();
    let actor = uuid::Uuid::new_v4();
    native
        .reserve_engine(
            native.identity().partition_id,
            native.identity().replica_id,
            actor,
        )
        .unwrap();
    let custom = Arc::new(StatefulCalls::default());
    custom.partitions.lock().unwrap().push(native.clone());
    let proofs = Arc::new(Mutex::new(Vec::new()));
    let binder: Arc<dyn NativeControlBinding> = Arc::new(CaptureBinder {
        calls: custom.clone(),
        issued: proofs.clone(),
        mode: 199,
    });
    native.install_binding(actor, &binder).unwrap();
    let configuration = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    h.send("UpdateConfiguration", &configuration).await;
    let reply: ProxyReply = from_slice(&h.receive("UpdateConfigurationReply").await.body).unwrap();
    assert_eq!(reply.error.error, 0);
    assert_eq!(custom.configurations.lock().unwrap().len(), 1);
    assert!(canonical.configurations.lock().unwrap().is_empty());
    assert_eq!(proofs.lock().unwrap().len(), 1);
    let proof = proofs.lock().unwrap()[0].clone();
    assert!(
        proof.enter().is_err(),
        "selected consumer used a different entered flag"
    );
    assert!(proof.clone().enter().is_err());
}

async fn scope_fixture() -> (
    Harness,
    NativeRuntimeIntegration,
    NativeInvocation,
    NativeInvocation,
) {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let native = calls.partitions.lock().unwrap()[0].clone();
    let actor = uuid::Uuid::new_v4();
    native
        .reserve_engine(
            native.identity().partition_id,
            native.identity().replica_id,
            actor,
        )
        .unwrap();
    let proofs = Arc::new(Mutex::new(Vec::new()));
    let binder: Arc<dyn NativeControlBinding> = Arc::new(CaptureBinder {
        calls,
        issued: proofs.clone(),
        mode: 20,
    });
    native.install_binding(actor, &binder).unwrap();
    let configuration = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    h.send("UpdateConfiguration", &configuration).await;
    h.receive("UpdateConfigurationReply").await;
    let p = proofs.lock().unwrap()[0].clone();
    let mut idle = peer(&open.local, 2);
    idle.current_role = 2;
    idle.previous_role = 0;
    let build = control(&open, "ReplicatorBuildIdleReplica", vec![idle]);
    h.send("ReplicatorBuildIdleReplica", &build).await;
    h.receive("ReplicatorBuildIdleReplicaReply").await;
    let q = proofs.lock().unwrap().last().unwrap().clone();
    assert_eq!(q.kind(), NativeInvocationKind::Build);
    assert!(p.validate().is_ok());
    (h, native, p, q)
}

struct InvocationRelease {
    expected: Option<NativeInvocation>,
    attempted: NativeRuntimeIntegration,
    configuration: ReplicaSetConfig,
    execution: crate::extensions::ExecutionRuntime,
    observed: Arc<Mutex<Vec<(bool, bool, bool)>>>,
}
impl Drop for InvocationRelease {
    fn drop(&mut self) {
        use crate::extensions::{CallLane, CallOrigin, ExecutionRuntime};
        let current = ExecutionRuntime::current_invocation();
        let retained = match (&self.expected, current) {
            (Some(expected), Some(current)) => expected.same_instance(&current),
            (None, None) => true,
            _ => false,
        };
        let direct_denied = self
            .attempted
            .capture_configuration(&self.configuration, None)
            .is_err();
        let attempted = self.attempted.clone();
        let configuration = self.configuration.clone();
        let nested_denied = match self.execution.reserve(
            CallLane::Provider,
            CallOrigin::default(),
            Duration::from_secs(1),
            false,
        ) {
            Ok(permit) => permit
                .run_owned(move || {
                    attempted
                        .capture_configuration(&configuration, None)
                        .map(|_| ())
                })
                .wait()
                .is_err(),
            Err(_) => true,
        };
        self.observed
            .lock()
            .unwrap()
            .push((retained, direct_denied, nested_denied));
    }
}

#[tokio::test]
async fn native_invocation_delayed_disposal_preserves_original_and_cannot_regrant_capture() {
    use crate::extensions::{DisposalTracker, ExecutionRuntime, ObjectQuota};
    let (_h, native, p, q) = scope_fixture().await;
    let execution = native.execution();
    let owned = execution.clone();
    let expected = p.clone();
    let configuration = p.configuration().unwrap().0.sdk().unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe = InvocationRelease {
        expected: Some(p.clone()),
        attempted: native.clone(),
        configuration,
        execution: execution.clone(),
        observed: observed.clone(),
    };
    let (object, receipt) = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _scope = ExecutionRuntime::invocation_scope(expected)?;
            let object = owned
                .reserve_construction(&ObjectQuota::new(1)?, &DisposalTracker::default())?
                .construct(probe);
            let receipt = object.receipt();
            Ok((object, receipt))
        })
        .await
        .unwrap();
    execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _scope = ExecutionRuntime::invocation_scope(q)?;
            drop(object);
            Ok(())
        })
        .await
        .unwrap();
    receipt.wait().await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![(true, true, true)]);
}

#[tokio::test]
async fn native_concurrent_reserved_entries_keep_exact_proof_without_ambient_leak() {
    use crate::extensions::{CallLane, CallOrigin, ExecutionRuntime};
    let (_h, native, p, q) = scope_fixture().await;
    let execution = native.execution();
    let owned = execution.clone();
    let source = native.clone();
    let configuration = p.configuration().unwrap().0.sdk().unwrap();
    let expected = p.clone();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let child = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _p = ExecutionRuntime::invocation_scope(p)?;
            Ok(owned
                .reserve(
                    CallLane::Provider,
                    CallOrigin::default(),
                    Duration::from_secs(2),
                    false,
                )?
                .run_future(async move {
                    let _ = started.send(());
                    released.await.map_err(|_| Error::Closed)?;
                    source.capture_configuration(&configuration, None)
                }))
        })
        .await
        .unwrap();
    entered.await.unwrap();
    let source = native.clone();
    let target = q.replica().unwrap().sdk().unwrap();
    let expected_q = q.clone();
    let q_captured = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _q = ExecutionRuntime::invocation_scope(q)?;
            source.capture_build(&target)
        })
        .await
        .unwrap();
    assert!(q_captured.same_instance(&expected_q));
    let source = native;
    let configuration = expected.configuration().unwrap().0.sdk().unwrap();
    assert!(execution
        .try_reserve()
        .unwrap()
        .run_owned(move || { source.capture_configuration(&configuration, None) })
        .await
        .is_err());
    release.send(()).unwrap();
    assert!(child.await.unwrap().same_instance(&expected));
    assert!(ExecutionRuntime::current_invocation().is_none());
}

#[tokio::test]
async fn native_invocation_foreign_cleanup_has_no_proof_and_nested_reservation_stays_non_authorizing(
) {
    use crate::extensions::ExecutionRuntime;
    let (_a, native_a, p, _) = scope_fixture().await;
    let (_b, native_b, _, _) = scope_fixture().await;
    let execution = native_a.execution();
    let foreign = native_b.execution();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let probe = InvocationRelease {
        expected: None,
        attempted: native_a,
        configuration: p.configuration().unwrap().0.sdk().unwrap(),
        execution: foreign.clone(),
        observed: observed.clone(),
    };
    let receipt = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _scope = ExecutionRuntime::invocation_scope(p)?;
            Ok(foreign
                .reserve_cleanup(Duration::from_secs(1))?
                .dispose(probe))
        })
        .await
        .unwrap();
    receipt.wait().await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![(true, true, true)]);
}

#[tokio::test]
async fn native_invocation_reserved_continuation_nesting_unwind_and_lease_are_exact() {
    use crate::extensions::{CallLane, CallOrigin, ExecutionRuntime};
    let (h, native, p, q) = scope_fixture().await;
    let execution = native.execution();
    let owned = execution.clone();
    let expected = p.clone();
    let configuration = p.configuration().unwrap().0.sdk().unwrap();
    let supplied = native.clone();
    let child = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _p = ExecutionRuntime::invocation_scope(p.clone())?;
            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _q = ExecutionRuntime::invocation_scope(q).unwrap();
                panic!("owned scope unwind probe");
            }));
            assert!(unwind.is_err());
            assert!(ExecutionRuntime::current_invocation()
                .unwrap()
                .same_instance(&p));
            let child = owned
                .reserve(
                    CallLane::Provider,
                    CallOrigin::default(),
                    Duration::from_secs(1),
                    false,
                )?
                .run_owned(move || supplied.capture_configuration(&configuration, None));
            Ok(child)
        })
        .await
        .unwrap();
    let captured = child.await.unwrap();
    assert!(captured.same_instance(&expected));
    captured.enter().unwrap();
    assert!(expected.enter().is_err());
    assert!(ExecutionRuntime::current_invocation().is_none());
    h.engine.shared.execution.retire();
    assert!(native.validate().is_err());
    assert!(captured.validate().is_err());
}

#[tokio::test]
async fn native_same_epoch_configuration_reissue_revokes_original_without_revision_change() {
    let mut h = Harness::new().await;
    let calls = register(&h);
    let open = request(&h, 2, 1, 20);
    h.send("ReplicaOpen", &open).await;
    h.receive("ReplicaOpenReply").await;
    h.engine.shared.stateful.wait_idle([2; 16]).await;
    let native = calls.partitions.lock().unwrap()[0].clone();
    let actor = uuid::Uuid::new_v4();
    native
        .reserve_engine(
            native.identity().partition_id,
            native.identity().replica_id,
            actor,
        )
        .unwrap();
    calls.configuration_failure.store(true, Ordering::Release);
    let issued = Arc::new(Mutex::new(Vec::new()));
    let binder: Arc<dyn NativeControlBinding> = Arc::new(CaptureBinder {
        calls,
        issued: issued.clone(),
        mode: 36,
    });
    native.install_binding(actor, &binder).unwrap();
    let configuration = control(&open, "UpdateConfiguration", vec![open.local.clone()]);
    h.send("UpdateConfiguration", &configuration).await;
    h.receive("UpdateConfigurationReply").await;
    let original = issued.lock().unwrap()[0].clone();
    assert!(original.validate().is_ok());
    h.send("UpdateConfiguration", &configuration).await;
    h.receive("UpdateConfigurationReply").await;
    let replacement = issued.lock().unwrap().last().unwrap().clone();
    assert!(original.same_revision(&replacement));
    assert!(replacement.validate().is_ok());
    assert!(
        original.validate().is_err(),
        "same-revision old record was still current"
    );
    assert!(!original.same_instance(&replacement));
}

#[tokio::test]
async fn native_pending_disposal_under_foreign_invocation_retains_original_not_releasing_thread() {
    use crate::extensions::{DisposalTracker, ExecutionRuntime, ObjectQuota};
    let (_a, native_a, p, _) = scope_fixture().await;
    let (_b, native_b, q, _) = scope_fixture().await;
    let execution = native_a.execution();
    let foreign = native_b.execution();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let expected = p.clone();
    let owned = execution.clone();
    let probe = InvocationRelease {
        expected: Some(p.clone()),
        attempted: native_b,
        configuration: q.configuration().unwrap().0.sdk().unwrap(),
        execution: foreign.clone(),
        observed: observed.clone(),
    };
    let (object, receipt) = execution
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _scope = ExecutionRuntime::invocation_scope(expected)?;
            let object = owned
                .reserve_construction(&ObjectQuota::new(1)?, &DisposalTracker::default())?
                .construct(probe);
            let receipt = object.receipt();
            Ok((object, receipt))
        })
        .await
        .unwrap();
    foreign
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            let _q = ExecutionRuntime::invocation_scope(q)?;
            drop(object);
            Ok(())
        })
        .await
        .unwrap();
    receipt.wait().await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![(true, true, true)]);
}
