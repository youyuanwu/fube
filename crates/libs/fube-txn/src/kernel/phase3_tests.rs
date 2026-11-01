//! Actual lower NULL-context Full Build; providers/time are controlled source
//! fixtures, not collection products or pinned-native-binary qualification.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round4_actual_full_waits_checkpoint_complete_before_selecting_state_and_log() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.checkpoint().await;
    *source.providers.complete_published_copy.lock().unwrap() = true;
    source.write(10, 2, 20).await;
    let entered = Completion::new();
    let release = Completion::new();
    let observed = entered.clone();
    source.point(10).inject_callback(
        CallbackKind::Complete,
        CallbackPlan {
            gate: Some(release.clone()),
            on_enter: Some(Arc::new(move || {
                observed.complete(Ok(()));
            })),
            ..Default::default()
        },
    );
    let maintenance = source.runtime.maintenance();
    let checkpoint = maintenance.start_checkpoint(false).unwrap().unwrap();
    entered.wait().await.as_ref().clone().unwrap();
    assert!(!checkpoint.finished.is_completed());
    assert!(!source
        .point(10)
        .completed_checkpoint()
        .unwrap()
        .contains_key(&2));

    let captured = Completion::new();
    let observed = captured.clone();
    let consistency = maintenance.consistency.clone();
    source.point(10).inject_callback(
        CallbackKind::GetCopy,
        CallbackPlan {
            on_enter: Some(Arc::new(move || {
                assert!(
                    consistency.try_lock().is_ok(),
                    "native selection lock is released before provider stream capture"
                );
                observed.complete(Ok(()));
            })),
            ..Default::default()
        },
    );
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    maintenance
        .copy_selection_entered
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert!(!captured.is_completed());
    assert!(!building.is_finished());
    assert_eq!(
        source
            .runtime
            .owner
            .manager
            .memory()
            .unwrap()
            .lock()
            .unwrap()
            .reader_count(),
        0,
        "copy cannot pin a newer descriptor while the provider still publishes the old checkpoint"
    );
    release.complete(Ok(()));
    building.await.unwrap().unwrap();
    target.copied().await;
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    assert!(captured.is_completed());
    assert_eq!(target.point(10).copied().get(&1), Some(&10));
    assert_eq!(target.point(10).copied().get(&2), Some(&20));
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round3_actual_full_snapshot_capture_blocks_deletion_checkpoint_publication_atomically(
) {
    use super::{
        provider::REGISTRY_ID,
        registry_codec::{ApplyType, RegistryOperation},
    };
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.checkpoint().await;
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    let (entered, observed) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    let resume = Arc::new(Mutex::new(Some(resume)));
    source.point(10).inject_callback(
        CallbackKind::GetCopy,
        CallbackPlan {
            on_enter: Some(Arc::new(move || {
                entered.send(()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            })),
            ..Default::default()
        },
    );
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    tokio::task::spawn_blocking(move || observed.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let transaction = source.runtime.owner.create_transaction().unwrap();
    transaction
        .add_operation(
            Some(
                RegistryOperation {
                    id: 10,
                    name: "fabric:/phase3".into(),
                    kind: ApplyType::Delete,
                }
                .encode()
                .unwrap(),
            ),
            None,
            None,
            REGISTRY_ID,
            None,
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let before = source
        .runtime
        .registry
        .events()
        .iter()
        .filter(|e| **e == "registry-perform")
        .count();
    let checkpoint = source
        .runtime
        .maintenance()
        .start_checkpoint(false)
        .unwrap()
        .unwrap();
    until(|| {
        source
            .runtime
            .registry
            .events()
            .iter()
            .filter(|e| **e == "registry-perform-ready")
            .count()
            > before
    })
    .await;
    assert!(
        !checkpoint.finished.is_completed(),
        "exclusive publication waits for native shared capture lock"
    );
    release.send(()).unwrap();
    building.await.unwrap().unwrap();
    target.copied().await;
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    assert_eq!(target.point(10).copied().get(&1), Some(&10));
    assert_eq!(
        target
            .runtime
            .fault_reports
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "matching old Active metadata/state was captured, not newer DelayDelete plus old state"
    );
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round2_real_secondary_selects_checkpoint_before_reader_delayed_truncation_then_tests_stability(
) {
    let partition = uuid::Uuid::new_v4();
    let source =
        Fixture::start_profile(partition, 1, ReplicaRole::Primary, false, rows(), 16, 8).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    let target = Fixture::start_profile(
        partition,
        2,
        ReplicaRole::IdleSecondary,
        false,
        vec![],
        16,
        4,
    )
    .await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    source
        .configure(vec![NativeReplicaDescriptor {
            role: ReplicaRole::ActiveSecondary,
            ..descriptor(&target)
        }])
        .await;
    let memory = target.runtime.owner.manager.memory().unwrap();
    let reader = memory.lock().unwrap().pin(0).unwrap();
    for key in 0..6 {
        wide_single(&source, key).await;
    }
    until(|| {
        memory
            .lock()
            .unwrap()
            .completed
            .as_ref()
            .unwrap()
            .begin_position
            > 1000
    })
    .await;
    let first = Completion::new();
    let second = Completion::new();
    target.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(first.clone()),
            ..Default::default()
        },
    );
    target.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(second.clone()),
            ..Default::default()
        },
    );
    let producer = source.runtime.clone();
    let native = source.native.clone();
    let writing = tokio::spawn(async move {
        for key in 6..27 {
            let transaction = producer.owner.create_transaction()?;
            let operation = transaction.clone();
            native
                .execution()
                .try_reserve()?
                .run_owned(move || {
                    operation.add_operation(
                        Some(OperationData::new(vec![vec![0; 200 * 1024]])?),
                        None,
                        Some(data(key)),
                        10,
                        None,
                    )
                })
                .await?;
            transaction.commit().await?;
        }
        Ok::<(), Error>(())
    });
    target
        .runtime
        .maintenance()
        .secondary_blocked
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    let checkpoint = target.runtime.maintenance().pending_checkpoint().unwrap();
    let trunc = target.runtime.maintenance().pending_truncation().unwrap();
    trunc.applied.wait().await.as_ref().clone().unwrap();
    assert!(!trunc.done.is_completed());
    let blocked = target.runtime.owner.manager.tail();
    first.complete(Ok(()));
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    until(|| {
        target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .contains(&("replication", blocked, true))
    })
    .await;
    assert!(
        !trunc.done.is_completed(),
        "selected checkpoint completion did not also wait for truncation"
    );
    until(|| {
        target
            .runtime
            .maintenance()
            .pending_checkpoint()
            .is_some_and(|c| c.lsn > checkpoint.lsn)
    })
    .await;
    let next = target.runtime.maintenance().pending_checkpoint().unwrap();
    until(|| target.runtime.owner.manager.tail() > next.lsn).await;
    assert!(target.runtime.operation_events.lock().unwrap().iter().any(|(stream, lsn, ack)| *stream == "replication" && *ack && *lsn >= next.lsn),
        "new not-stable checkpoint was selected before the already-stable truncation, allowing its initiating Barrier ACK and next poll");
    memory.lock().unwrap().unpin(reader);
    trunc.done.wait().await.as_ref().clone().unwrap();
    second.complete(Ok(()));
    writing.await.unwrap().unwrap();
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[test]
fn phase3_round2_native_bootstrap_periodic_timestamp_caches_are_initialized_and_reused() {
    let mut memory = super::memory_log::MemoryLog::new(1024 * 1024).unwrap();
    let mut clock = vec![70002i64, 70001].into_iter();
    memory
        .bootstrap_with_clock(|| Ok(clock.next().unwrap()))
        .unwrap();
    let cached = memory.periodic_times;
    assert_eq!(cached, (70002, 70001));
    let first = memory.completed.clone().unwrap();
    let index = memory
        .indexing(
            1,
            Epoch {
                data_loss_number: 0,
                configuration_number: 0,
            },
        )
        .unwrap();
    let next = memory.begin_checkpoint(1, None).unwrap();
    memory.end_checkpoint().unwrap();
    memory.complete_checkpoint().unwrap();
    for checkpoint in [first, next] {
        let length = u32::from_le_bytes(
            memory
                .read(checkpoint.begin_position, 4)
                .try_into()
                .unwrap(),
        ) as u64;
        assert_eq!(
            memory.read(checkpoint.begin_position + 4 + length - 16, 16),
            [cached.0.to_le_bytes(), cached.1.to_le_bytes()].concat()
        );
    }
    memory.select_runtime_head(index).unwrap();
    let position = memory.entries.back().unwrap().position;
    let length = u32::from_le_bytes(memory.read(position, 4).try_into().unwrap()) as u64;
    assert_eq!(
        memory.read(position + 4 + length - 8, 8),
        cached.1.to_le_bytes()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round2_actual_full_registry_large_individual_rows_split_before_packet_overflow() {
    let partition = uuid::Uuid::new_v4();
    let mut metadata = rows();
    metadata[0].name = "fabric:/z".into();
    metadata[0].parent = 0;
    metadata[0].type_name = "t".into();
    metadata[0].initialization = Some(OperationData::new(vec![vec![0; 1000]]).unwrap());
    metadata[1].name = "fabric:/a".into();
    metadata[1].parent = 0;
    metadata[1].type_name = "t".into();
    metadata[1].initialization = Some(OperationData::new(vec![vec![0; 1048400]]).unwrap());
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, metadata).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    let copied = target.runtime.registry.rows();
    assert_eq!(copied.len(), 2);
    assert_eq!(
        copied
            .iter()
            .find(|r| r.id == 11)
            .unwrap()
            .initialization
            .as_ref()
            .unwrap()
            .bytes(),
        1048400
    );
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round2_reader_release_between_physical_boundary_insertion_and_flush_preserves_owned_dispatch(
) {
    for kind in [7, 12] {
        let source =
            Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, false, rows()).await;
        source.configure(vec![]).await;
        let memory = source.runtime.owner.manager.memory().unwrap();
        let reader = memory.lock().unwrap().pin(0).unwrap();
        for key in 0..12 {
            wide_single(&source, key).await;
        }
        until(|| source.runtime.maintenance().pending_truncation().is_some()).await;
        let trunc = source.runtime.maintenance().pending_truncation().unwrap();
        trunc.applied.wait().await.as_ref().clone().unwrap();
        let logging = source
            .runtime
            .logging
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let maintenance = source.runtime.maintenance();
        // Finish automatic checkpoint work before pausing a new manual boundary.
        logging.group_idle().await;
        if let Some(checkpoint) = maintenance.pending_checkpoint() {
            checkpoint.finished.wait().await.as_ref().clone().unwrap();
        }
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (release, resume) = std::sync::mpsc::sync_channel(1);
        *source
            .runtime
            .owner
            .manager
            .physical_insertion_pause
            .lock()
            .unwrap() = Some((kind, entered, resume));
        let manager = source.runtime.owner.manager.clone();
        let mut work = source
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                if kind == 7 {
                    let activity = maintenance.start_checkpoint(false)?.unwrap();
                    Ok(Some(activity))
                } else {
                    manager.local_boundary(12, 2)?;
                    Ok(None)
                }
            });
        let mut observed =
            tokio::task::spawn_blocking(move || observed.recv_timeout(Duration::from_secs(5)));
        let mut completed = None;
        let paused = tokio::select! {
            paused = &mut observed => paused,
            result = &mut work => {
                completed = Some(result);
                source.runtime.owner.manager.physical_insertion_pause.lock().unwrap().take();
                observed.await
            }
        };
        memory.lock().unwrap().unpin(reader);
        let truncated = trunc.done.wait().await.as_ref().clone();
        let released = release.send(());
        let result = match completed {
            Some(result) => result,
            None => work.await,
        };
        let checkpoint = result.expect("physical boundary insertion succeeds");
        paused
            .expect("physical insertion observer joins")
            .expect("physical boundary reaches the controlled pause");
        truncated.unwrap();
        released.unwrap();
        if let Some(checkpoint) = checkpoint {
            checkpoint.finished.wait().await.as_ref().clone().unwrap();
        }
        drop(source.host);
        source
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_round2_applied_reader_delayed_truncation_survives_healthy_role_transition() {
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, true, rows()).await;
    source.configure(vec![]).await;
    let memory = source.runtime.owner.manager.memory().unwrap();
    let reader = memory.lock().unwrap().pin(0).unwrap();
    for key in 0..12 {
        wide_single(&source, key).await;
    }
    until(|| source.runtime.maintenance().pending_truncation().is_some()).await;
    let trunc = source.runtime.maintenance().pending_truncation().unwrap();
    trunc.applied.wait().await.as_ref().clone().unwrap();
    let before = memory.lock().unwrap().allocated_bytes();
    let changing = source.host.clone();
    let role =
        tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!role.is_finished() && !trunc.done.is_completed());
    memory.lock().unwrap().unpin(reader);
    trunc.done.wait().await.as_ref().clone().unwrap();
    role.await.unwrap().unwrap();
    assert!(memory.lock().unwrap().allocated_bytes() < before);
    assert!(memory.lock().unwrap().head > 0);
    assert_eq!(
        source
            .runtime
            .fault_reports
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    drop(source.host);
    source
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_full_eof_apply_fence_does_not_reopen_concurrent_native_role_callbacks() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let source = Fixture::start(partition, 1, ReplicaRole::Primary, root, rows()).await;
        source.configure(vec![]).await;
        source.write(10, 1, 10).await;
        source.checkpoint().await;
        let logging = source
            .runtime
            .logging
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        logging.group_idle().await;
        let group = Completion::new();
        *logging.group_pause.lock().unwrap() = Some(group.clone());
        let transaction = source.runtime.owner.create_transaction().unwrap();
        source
            .point(10)
            .write(transaction.as_ref(), 10, 2, 20)
            .unwrap();
        let before = source.runtime.owner.manager.tail();
        let commit = transaction.commit();
        until(|| source.runtime.owner.manager.tail() > before).await;
        let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, vec![]).await;
        let apply = Completion::new();
        *target.providers.apply_gate.lock().unwrap() = Some(apply.clone());
        let sender = source.host.clone();
        let member = descriptor(&target);
        let mut building =
            tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
        let observed = tokio::time::timeout(Duration::from_secs(3), async {
            while !target
                .providers
                .objects
                .lock()
                .unwrap()
                .get(&10)
                .is_some_and(|p| {
                    p.point
                        .events()
                        .iter()
                        .any(|e| matches!(e, PointEvent::Apply { key: 2, .. }))
                })
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Build is the public source-side application-copy EOS/finish ACK
        // boundary. It is NOT upper EOF Information/Apply completion.
        let built_early = tokio::time::timeout(Duration::from_secs(3), &mut building).await;
        let build_completed_early = matches!(built_early, Ok(Ok(Ok(()))));
        let generation = target
            .runtime
            .generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let upper_pending = !generation.copy_done.is_completed();
        let public_eos = target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .contains(&("copy-eos", -1, false));
        eprintln!("PUBLIC Full before Apply release: Apply={observed:?} source Build={built_early:?} public_stream_EOS={public_eos} upper_pending={upper_pending}");
        let changing = target.host.clone();
        let active =
            tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
        let lower_role = tokio::time::timeout(Duration::from_secs(3), async {
            while !target.runtime.control_events.lock().unwrap().contains(
                &super::runtime::ControlEvent::RoleResult(ReplicaRole::ActiveSecondary, true),
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let role_pending = !active.is_finished();
        // Release ALL gates and join ownership before any assertion can unwind.
        apply.complete(Ok(()));
        group.complete(Ok(()));
        let built = match built_early {
            Ok(result) => result,
            Err(_) => building.await,
        };
        let changed = active.await;
        let committed = commit.await;
        let readable = target.runtime.owner.get_is_readable();
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(observed.is_ok() && build_completed_early && public_eos && upper_pending);
        assert!(lower_role.is_ok() && role_pending);
        built.unwrap().unwrap();
        changed.unwrap().unwrap();
        committed.unwrap();
        assert!(readable);
    }
}

fn data(key: i64) -> OperationData {
    OperationData::new(vec![[key.to_le_bytes(), (key * 10).to_le_bytes()].concat()]).unwrap()
}
async fn wide_single(source: &Fixture, key: i64) {
    let transaction = source.runtime.owner.create_transaction().unwrap();
    transaction
        .add_operation(
            Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
            None,
            Some(data(key)),
            10,
            None,
        )
        .unwrap();
    transaction.commit().await.unwrap();
}
async fn controlled_replicate(source: &Fixture, record: super::record::Record) -> i64 {
    let mut admitted = source
        .runtime
        .state_replicator()
        .replicate(record.encode().unwrap(), Cancellation::default())
        .await
        .unwrap();
    admitted
        .completion
        .wait(Cancellation::default())
        .await
        .unwrap();
    admitted.sequence_number
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_native_registry_log_insert_resets_stale_retained_state_and_deleted_insert_is_noop(
) {
    use super::{
        provider::REGISTRY_ID,
        record::{Record, RecordKind},
        registry_codec::{ApplyType, InsertRedo, RegistryOperation},
    };
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.checkpoint().await;
    let logging = source
        .runtime
        .logging
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    logging.group_idle().await;
    let create = source.runtime.admitted_lsn() + 1;
    let mut retained = rows();
    retained.push(Metadata {
        id: 12,
        parent: 0,
        name: "fabric:/later".into(),
        type_name: "point-fixture".into(),
        create_lsn: create,
        delete_lsn: -1,
        initialization: None,
        mode: ProviderMode::Active,
    });
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, retained).await;
    let point = target.point(12);
    point.begin_setting_current_state().await.unwrap();
    point
        .set_current_state(0, data(99), Cancellation::default())
        .await
        .unwrap();
    point
        .end_setting_current_state(Cancellation::default())
        .await
        .unwrap();
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    source
        .configure(vec![NativeReplicaDescriptor {
            role: ReplicaRole::ActiveSecondary,
            ..descriptor(&target)
        }])
        .await;
    assert_eq!(point.copied().get(&99), Some(&990));
    let op = |kind| {
        RegistryOperation {
            id: 12,
            name: "fabric:/later".into(),
            kind,
        }
        .encode()
        .unwrap()
    };
    let insert = || Record {
        lsn: -1,
        kind: RecordKind::Begin {
            transaction_id: 123456,
            single: true,
            metadata: Some(
                super::named::encode(REGISTRY_ID, Some(&op(ApplyType::Insert))).unwrap(),
            ),
            redo: Some(
                InsertRedo {
                    type_name: "point-fixture".into(),
                    parent: 0,
                    initialization: None,
                }
                .encode()
                .unwrap(),
            ),
            undo: None,
        },
    };
    // Controlled source-side native wire admission; target uses the actual
    // already-Full-copied replication pump/binder, not a fake replay manager.
    let lsn = controlled_replicate(&source, insert()).await;
    let barrier = controlled_replicate(
        &source,
        Record {
            lsn: -1,
            kind: RecordKind::Barrier {
                last_stable_lsn: lsn,
            },
        },
    )
    .await;
    until(|| target.runtime.owner.manager.version() >= barrier).await;
    assert!(point.copied().is_empty());
    assert!(target
        .providers
        .events
        .lock()
        .unwrap()
        .contains(&(12, "role:ActiveSecondary".into())));
    let deleted = Record {
        lsn: -1,
        kind: RecordKind::Begin {
            transaction_id: 123457,
            single: true,
            metadata: Some(
                super::named::encode(REGISTRY_ID, Some(&op(ApplyType::Delete))).unwrap(),
            ),
            redo: None,
            undo: None,
        },
    };
    let lsn = controlled_replicate(&source, deleted).await;
    let barrier = controlled_replicate(
        &source,
        Record {
            lsn: -1,
            kind: RecordKind::Barrier {
                last_stable_lsn: lsn,
            },
        },
    )
    .await;
    until(|| target.runtime.owner.manager.version() >= barrier).await;
    let before = point.events().len();
    let mut duplicate = insert();
    if let RecordKind::Begin { transaction_id, .. } = &mut duplicate.kind {
        *transaction_id = 123458;
    }
    let lsn = controlled_replicate(&source, duplicate).await;
    let barrier = controlled_replicate(
        &source,
        Record {
            lsn: -1,
            kind: RecordKind::Barrier {
                last_stable_lsn: lsn,
            },
        },
    )
    .await;
    until(|| target.runtime.owner.manager.version() >= barrier).await;
    assert_eq!(point.events().len(), before);
    assert_eq!(
        target
            .runtime
            .registry
            .rows()
            .into_iter()
            .find(|r| r.id == 12)
            .unwrap()
            .mode,
        ProviderMode::DelayDelete
    );
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_actual_full_sender_pin_delays_head_crossing_old_pending_anchor_under_pressure(
) {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    let mut pending = None;
    for key in 0..6 {
        if key == 4 {
            let transaction = source.runtime.owner.create_transaction().unwrap();
            source
                .point(10)
                .write(transaction.as_ref(), 10, 100, 1000)
                .unwrap();
            let writing = transaction.clone();
            let point = source.point(10);
            source
                .native
                .execution()
                .try_reserve()
                .unwrap()
                .run_owned(move || point.write(writing.as_ref(), 10, 101, 1010))
                .await
                .unwrap();
            pending = Some(transaction);
        }
        wide_single(&source, key).await;
    }
    let memory = source.runtime.owner.manager.memory().unwrap();
    until(|| memory.lock().unwrap().completed.as_ref().unwrap().lsn > 1).await;
    let old = memory.lock().unwrap().completed.clone().unwrap();
    assert!(old.reader_position < old.begin_position);
    let entered = Completion::new();
    let release = Completion::new();
    *source.providers.state_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    entered.wait().await.as_ref().clone().unwrap();
    assert_eq!(memory.lock().unwrap().reader_count(), 1);
    pending.take().unwrap().commit().await.unwrap();
    for key in 6..15 {
        wide_single(&source, key).await;
    }
    until(|| {
        source
            .runtime
            .maintenance()
            .pending_truncation()
            .is_some_and(|t| t.position > old.reader_position)
    })
    .await;
    let trunc = source.runtime.maintenance().pending_truncation().unwrap();
    trunc.applied.wait().await.as_ref().clone().unwrap();
    assert!(!trunc.done.is_completed());
    assert!(memory.lock().unwrap().head <= old.reader_position);
    release.complete(Ok(()));
    building.await.unwrap().unwrap();
    target.copied().await;
    trunc.done.wait().await.as_ref().clone().unwrap();
    assert!(memory.lock().unwrap().head > old.reader_position);
    assert_eq!(memory.lock().unwrap().reader_count(), 0);
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_real_replication_barrier_continues_notstable_then_blocks_stable_maintenance(
) {
    let partition = uuid::Uuid::new_v4();
    let source =
        Fixture::start_profile(partition, 1, ReplicaRole::Primary, false, rows(), 16, 8).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    let target = Fixture::start_profile(
        partition,
        2,
        ReplicaRole::IdleSecondary,
        false,
        vec![],
        16,
        4,
    )
    .await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    source
        .configure(vec![NativeReplicaDescriptor {
            role: ReplicaRole::ActiveSecondary,
            ..descriptor(&target)
        }])
        .await;
    let perform = Completion::new();
    target.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(perform.clone()),
            ..Default::default()
        },
    );
    let transaction = big_transaction(&source, 22).await;
    transaction.commit().await.unwrap();
    target
        .runtime
        .maintenance()
        .secondary_blocked
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    let checkpoint = target.runtime.maintenance().pending_checkpoint().unwrap();
    assert!(
        target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .iter()
            .any(|(stream, lsn, ack)| *stream == "replication" && *ack && *lsn >= checkpoint.lsn),
        "not-yet-stable initiating Barrier was ACKed so the later stable Barrier could be polled"
    );
    assert!(target.runtime.owner.manager.tail() > checkpoint.lsn);
    assert!(target.runtime.owner.manager.stable() >= checkpoint.lsn);
    let held = target.runtime.owner.manager.tail();
    assert!(
        !target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .contains(&("replication", held, true)),
        "stable pressure Barrier has not ACKed/dropped while maintenance is held"
    );
    perform.complete(Ok(()));
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    until(|| {
        target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .contains(&("replication", held, true))
    })
    .await;
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_actual_full_copy_excludes_secondary_throttle_wait_and_reaches_first_checkpoint_horizon(
) {
    let partition = uuid::Uuid::new_v4();
    let source =
        Fixture::start_profile(partition, 1, ReplicaRole::Primary, false, rows(), 16, 8).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    let source_perform = Completion::new();
    source.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(source_perform.clone()),
            ..Default::default()
        },
    );
    let transaction = big_transaction(&source, 22).await;
    transaction.commit().await.unwrap();
    let logging = source
        .runtime
        .logging
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    logging.group_idle().await;
    let horizon = source.runtime.owner.manager.tail();
    let target = Fixture::start_profile(
        partition,
        2,
        ReplicaRole::IdleSecondary,
        false,
        vec![],
        16,
        4,
    )
    .await;
    let perform = Completion::new();
    *target.providers.perform_gate.lock().unwrap() = Some(perform.clone());
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    until(|| target.runtime.owner.manager.tail() == horizon).await;
    assert!(!target
        .runtime
        .maintenance()
        .secondary_blocked
        .is_completed());
    {
        let memory = target.runtime.owner.manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        assert!(memory.tail_position - memory.head_identity().0 > 4 * 1024 * 1024);
    }
    assert!(
        !building.is_finished(),
        "only the real first-copy Perform/horizon completion is awaited"
    );
    perform.complete(Ok(()));
    building.await.unwrap().unwrap();
    target.copied().await;
    source_perform.complete(Ok(()));
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}
async fn big_transaction(source: &Fixture, count: i64) -> Arc<dyn Transaction> {
    source.write(10, -1, -10).await;
    let logging = source
        .runtime
        .logging
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    logging.group_idle().await;
    let transaction = source.runtime.owner.create_transaction().unwrap();
    for key in 0..count {
        let writing = transaction.clone();
        source
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                writing.add_operation(
                    Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
                    None,
                    Some(data(key)),
                    10,
                    None,
                )
            })
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "big operation {key}: {error}, state={:?}",
                    transaction.get_state()
                )
            });
    }
    transaction
}
async fn abort_failed_build(
    source: &Fixture,
    target: &Fixture,
    cancel: Cancellation,
    building: tokio::task::JoinHandle<Result<()>>,
) {
    let lower = target.runtime.replicator();
    target
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            lower.abort();
            Ok(())
        })
        .await
        .unwrap();
    let _ = target.runtime.terminal.wait().await;
    target
        .runtime
        .cleanup_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    cancel.cancel();
    let _ = building.await.unwrap();
    source.host.remove(descriptor(target)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_owned_first_checkpoint_errors_ack_transition_without_checkpoint_success()
{
    for kind in [
        CallbackKind::Prepare,
        CallbackKind::Perform,
        CallbackKind::Complete,
    ] {
        let partition = uuid::Uuid::new_v4();
        let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
        source.configure(vec![]).await;
        source.write(10, 1, 10).await;
        let logging = source
            .runtime
            .logging
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        logging.group_idle().await;
        let pause = Completion::new();
        *logging.group_pause.lock().unwrap() = Some(pause.clone());
        source.checkpoint().await;
        let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
        *target.providers.checkpoint_failure.lock().unwrap() = Some(kind);
        let cancel = Cancellation::default();
        let token = cancel.clone();
        let sender = source.host.clone();
        let member = descriptor(&target);
        let building = tokio::spawn(async move { sender.build(member, token).await });
        target
            .runtime
            .fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        until(|| {
            target
                .runtime
                .operation_events
                .lock()
                .unwrap()
                .contains(&("copy", 5, true))
        })
        .await;
        assert!(!target.runtime.owner.get_is_readable());
        let memory = target.runtime.owner.manager.memory().unwrap();
        assert!(!memory
            .lock()
            .unwrap()
            .entries
            .iter()
            .any(|e| e.native_type == 13));
        abort_failed_build(&source, &target, cancel, building).await;
        pause.complete(Ok(()));
        drop(source.host);
        drop(target.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_full_frontier_failed_write_settles_batch_ack_without_fake_checkpoint() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 0, 0).await;
    let transaction = source.runtime.owner.create_transaction().unwrap();
    source
        .point(10)
        .write(transaction.as_ref(), 10, 1, 10)
        .unwrap();
    let writing = transaction.clone();
    let point = source.point(10);
    source
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || point.write(writing.as_ref(), 10, 2, 20))
        .await
        .unwrap();
    source.write(11, 5, 50).await;
    source.checkpoint().await;
    let checkpoint = source
        .runtime
        .owner
        .manager
        .memory()
        .unwrap()
        .lock()
        .unwrap()
        .completed
        .clone()
        .unwrap();
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    *target.runtime.copy_write_failure.lock().unwrap() = Some((
        checkpoint.lsn,
        Error::InvalidInput("owned copied checkpoint-frontier write failed"),
    ));
    let cancel = Cancellation::default();
    let token = cancel.clone();
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, token).await });
    target
        .runtime
        .fault_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    until(|| {
        target
            .runtime
            .operation_events
            .lock()
            .unwrap()
            .iter()
            .any(|(stream, sequence, ack)| *stream == "copy" && *ack && *sequence >= 7)
    })
    .await;
    assert!(!target.runtime.owner.get_is_readable());
    let memory = target.runtime.owner.manager.memory().unwrap();
    assert!(memory.lock().unwrap().completed.is_none());
    abort_failed_build(&source, &target, cancel, building).await;
    transaction.abort().unwrap();
    drop(source.host);
    drop(target.host);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_stream_error_joins_first_copy_perform_before_generation_failure() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    let source_gate = Completion::new();
    source.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(source_gate.clone()),
            ..Default::default()
        },
    );
    for key in 0..7 {
        let transaction = source.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(
                Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
                None,
                Some(data(key)),
                10,
                None,
            )
            .unwrap();
        transaction.commit().await.unwrap();
    }
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    let perform = Completion::new();
    *target.providers.perform_gate.lock().unwrap() = Some(perform.clone());
    *target.runtime.copy_log_read_error.lock().unwrap() = Some(1);
    let cancel = Cancellation::default();
    let token = cancel.clone();
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, token).await });
    until(|| target.runtime.owner.manager.tail() > 2).await;
    let generation = target
        .runtime
        .generation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!generation.copy_done.is_completed());
    perform.complete(Ok(()));
    let error = generation
        .copy_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        Error::InvalidInput("controlled copy stream read failure").to_string()
    );
    assert!(target
        .runtime
        .owner
        .manager
        .memory()
        .unwrap()
        .lock()
        .unwrap()
        .completed
        .is_none());
    assert!(target.point(10).events().iter().any(|e| matches!(
        e,
        PointEvent::Callback {
            kind: CallbackKind::EndCopy,
            completed: true
        }
    )));
    source_gate.complete(Ok(()));
    abort_failed_build(&source, &target, cancel, building).await;
    drop(source.host);
    drop(target.host);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_new_checkpoint_extends_original_build_horizon_and_replication_overlap_is_noop(
) {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.checkpoint().await;
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    let entered = Completion::new();
    let release = Completion::new();
    *source.runtime.copy_selection_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let horizon = source.runtime.admitted_lsn();
    let sender = source.host.clone();
    let member = descriptor(&target);
    let building = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    entered.wait().await.as_ref().clone().unwrap();
    source.write(10, 2, 20).await;
    source.checkpoint().await;
    let checkpoint = source
        .runtime
        .owner
        .manager
        .memory()
        .unwrap()
        .lock()
        .unwrap()
        .completed
        .clone()
        .unwrap();
    assert!(checkpoint.lsn > horizon);
    release.complete(Ok(()));
    building.await.unwrap().unwrap();
    target.copied().await;
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    source
        .configure(vec![NativeReplicaDescriptor {
            role: ReplicaRole::ActiveSecondary,
            ..descriptor(&target)
        }])
        .await;
    source.write(10, 3, 30).await;
    until(|| !target.point(10).versions(3).is_empty()).await;
    assert_eq!(target.point(10).copied().get(&2), Some(&20));
    assert!(
        target.point(10).versions(2).is_empty(),
        "copied-prefix replication is ACKed without a second Apply"
    );
    assert_eq!(
        target
            .runtime
            .fault_reports
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_high_segment_records_build_in_distinct_native_batches_and_stage_limits_reject_before_lsn(
) {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.checkpoint().await;
    for key in [1, 2] {
        let transaction = source.runtime.owner.create_transaction().unwrap();
        transaction
            .add_operation(
                Some(OperationData::new(vec![vec![]; 600]).unwrap()),
                None,
                Some(data(key)),
                10,
                None,
            )
            .unwrap();
        transaction.commit().await.unwrap();
    }
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    assert_eq!(target.point(10).versions(1).len(), 1);
    assert_eq!(target.point(10).versions(2).len(), 1);
    let transaction = source.runtime.owner.create_transaction().unwrap();
    let metadata =
        super::named::encode(10, Some(&OperationData::new(vec![vec![]]).unwrap())).unwrap();
    let base = super::record::Record {
        lsn: -1,
        kind: super::record::RecordKind::Begin {
            transaction_id: transaction.get_transaction_id(),
            single: true,
            metadata: Some(metadata),
            redo: Some(data(3)),
            undo: None,
        },
    }
    .encode()
    .unwrap()
    .bytes();
    let limits = fube_data::Settings::default();
    let bytes = limits.operation_bytes - base - 2;
    transaction
        .add_operation(
            Some(OperationData::new(vec![vec![0; bytes]]).unwrap()),
            None,
            Some(data(3)),
            10,
            None,
        )
        .unwrap();
    let before = source.runtime.admitted_lsn();
    let error = transaction.commit().await.unwrap_err();
    assert_eq!(
        error.kind(),
        fube_runtime::ErrorKind::ReplicationOperationTooLarge
    );
    assert_eq!(source.runtime.admitted_lsn(), before);
    drop(source.host);
    drop(target.host);
    let _ = source.runtime.terminal.wait().await;
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_corrective_applied_checkpoint_survives_healthy_role_transition_without_fault() {
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, true, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    let gate = Completion::new();
    source.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(gate.clone()),
            ..Default::default()
        },
    );
    let checkpoint = source
        .runtime
        .maintenance()
        .start_checkpoint(false)
        .unwrap()
        .unwrap();
    until(|| {
        source.point(10).events().iter().any(|e| {
            matches!(
                e,
                PointEvent::Callback {
                    kind: CallbackKind::Perform,
                    completed: false
                }
            )
        })
    })
    .await;
    let changing = source.host.clone();
    let role =
        tokio::spawn(async move { changing.change_role(ReplicaRole::ActiveSecondary).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!role.is_finished());
    gate.complete(Ok(()));
    role.await.unwrap().unwrap();
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    assert_eq!(
        source
            .runtime
            .fault_reports
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(source.point(10).completed_checkpoint().is_some());
    drop(source.host);
    source
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_registry_delete_delay_copy_and_strict_cleanup_frontier() {
    use super::{
        provider::REGISTRY_ID,
        registry_codec::{ApplyType, RegistryOperation},
    };
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.checkpoint().await;
    let holder = super::copy_sender::CopySender::create(
        &source.runtime,
        source.runtime.owner.manager.tail(),
        None,
    )
    .await
    .unwrap();
    let transaction = source.runtime.owner.create_transaction().unwrap();
    let operation = |id, name: &str| {
        RegistryOperation {
            id,
            name: name.into(),
            kind: ApplyType::Delete,
        }
        .encode()
        .unwrap()
    };
    transaction
        .add_operation(
            Some(operation(10, "fabric:/phase3")),
            None,
            None,
            REGISTRY_ID,
            None,
        )
        .unwrap();
    let writing = transaction.clone();
    let metadata = operation(11, "fabric:/phase3/child");
    source
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || writing.add_operation(Some(metadata), None, None, REGISTRY_ID, None))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let deleted = source
        .runtime
        .registry
        .rows()
        .iter()
        .find(|r| r.id == 10)
        .unwrap()
        .delete_lsn;
    source
        .runtime
        .registry
        .cleanup_before(deleted)
        .await
        .unwrap();
    assert!(
        source.runtime.registry.rows().iter().any(|r| r.id == 10),
        "equality is retained"
    );
    source.checkpoint().await;
    assert!(source
        .runtime
        .registry
        .rows()
        .iter()
        .all(|r| r.mode == ProviderMode::DelayDelete));
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    let events = target.providers.events.lock().unwrap().clone();
    for id in [10, 11] {
        assert!(
            events.contains(&(id, "initialize".into())) && events.contains(&(id, "open".into()))
        );
        assert!(
            !events.contains(&(id, "recover".into())) && !events.contains(&(id, "begin".into()))
        );
    }
    holder.dispose().unwrap();
    source.checkpoint().await;
    assert!(source.runtime.registry.rows().is_empty());
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}
use super::{
    copy_codec::{find_copy_mode, CopyContext, CopyMode, FullReason},
    provider::{CallbackKind, CallbackPlan, PointEvent, PointProvider, ProviderMode},
    registry::RegistrySettings,
    registry_codec::Metadata,
    runtime::{RuntimeConfig, VolatileRuntime},
    version::Completion,
};
use crate::*;
use fube_data::extensions::ReplicationEndpointOptions;
use fube_rap_test_support as rap;
use fube_runtime::{
    extensions::{
        NativeIntegrationProvider, NativeReplicaDescriptor, NativeReplicaSetConfiguration,
        NativeRuntimeIntegration, ReplicationProgress,
    },
    IPrimaryReplicator, IReplicator, IStatefulServiceFactory, IStatefulServiceReplica, OpenMode,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::oneshot;

struct Providers {
    complete_published_copy: Mutex<bool>,
    apply_gate: Mutex<Option<Completion<()>>>,
    checkpoint_failure: Mutex<Option<CallbackKind>>,
    state_pause: Mutex<Option<(Completion<()>, Completion<()>)>>,
    perform_gate: Mutex<Option<Completion<()>>>,
    weak: Weak<Self>,
    runtime: Mutex<Weak<VolatileRuntime>>,
    objects: Mutex<BTreeMap<i64, Arc<Provider>>>,
    events: Mutex<Vec<(i64, String)>>,
    fail: Mutex<Option<(i64, String)>>,
    copy_gate: Mutex<Option<Completion<()>>>,
}
impl Providers {
    fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            complete_published_copy: Mutex::new(false),
            apply_gate: Mutex::new(None),
            checkpoint_failure: Mutex::new(None),
            state_pause: Mutex::new(None),
            perform_gate: Mutex::new(None),
            weak: weak.clone(),
            runtime: Mutex::new(Weak::new()),
            objects: Mutex::new(BTreeMap::new()),
            events: Mutex::new(vec![]),
            fail: Mutex::new(None),
            copy_gate: Mutex::new(None),
        })
    }
}
struct Provider {
    id: i64,
    point: Arc<PointProvider>,
    factory: Weak<Providers>,
}
impl Provider {
    fn factory(&self) -> Arc<Providers> {
        self.factory.upgrade().unwrap()
    }
    fn event(&self, event: impl Into<String>) -> Result<()> {
        let event = event.into();
        self.factory()
            .events
            .lock()
            .unwrap()
            .push((self.id, event.clone()));
        if self.factory().fail.lock().unwrap().as_ref() == Some(&(self.id, event)) {
            Err(Error::InvalidInput("controlled phase3 callback failure"))
        } else {
            Ok(())
        }
    }
}
impl IStateProvider2Factory for Providers {
    fn create(&self, args: &FactoryArguments) -> Result<Arc<dyn IStateProvider2>> {
        let factory = self.weak.clone();
        let provider = Arc::new(Provider {
            id: args.state_provider_id,
            point: PointProvider::preinitialized(&args.name),
            factory,
        });
        self.events
            .lock()
            .unwrap()
            .push((provider.id, "factory".into()));
        self.objects
            .lock()
            .unwrap()
            .insert(provider.id, provider.clone());
        if provider.id == 10 {
            if let Some(gate) = self.apply_gate.lock().unwrap().clone() {
                provider.point.inject_apply(super::provider::ApplyPlan {
                    gate: Some(gate),
                    ..Default::default()
                });
            }
            if let Some(kind) = *self.checkpoint_failure.lock().unwrap() {
                provider.point.inject_callback(
                    kind,
                    CallbackPlan {
                        result: Err(Error::InvalidInput("controlled owned checkpoint failure")),
                        ..Default::default()
                    },
                );
            }
            if let Some(gate) = self.perform_gate.lock().unwrap().clone() {
                provider.point.inject_callback(
                    CallbackKind::Perform,
                    CallbackPlan {
                        gate: Some(gate),
                        ..Default::default()
                    },
                );
            }
            if let Some(gate) = self.copy_gate.lock().unwrap().clone() {
                provider.point.inject_callback(
                    CallbackKind::SetCopy,
                    CallbackPlan {
                        gate: Some(gate),
                        ..Default::default()
                    },
                );
            }
        }
        Ok(provider)
    }
}
macro_rules! delegate {
    ($name:ident ($($arg:ident:$ty:ty),* $(,)?) -> $ret:ty) => {
        fn $name(&self,$($arg:$ty),*) -> $ret {self.point.$name($($arg),*)}
    };
}
impl IStateProvider2 for Provider {
    fn get_name(&self) -> &str {
        self.point.get_name()
    }
    fn get_children(&self, _: &str) -> Result<Vec<StateProviderInfo>> {
        Ok(vec![])
    }
    fn initialize(
        &self,
        owner: Weak<dyn ITransactionalReplicator>,
        _: &Path,
        children: Option<&[Arc<dyn IStateProvider2>]>,
    ) -> Result<()> {
        let owner = owner.upgrade().unwrap();
        assert!(!owner.get_is_readable());
        assert!(owner.create_transaction().is_err());
        if let Some(children) = children {
            for child in children {
                let child = self
                    .factory()
                    .objects
                    .lock()
                    .unwrap()
                    .values()
                    .find(|p| p.get_name() == child.get_name())
                    .unwrap()
                    .id;
                assert!(self
                    .factory()
                    .events
                    .lock()
                    .unwrap()
                    .contains(&(child, "initialize".into())));
            }
        }
        self.event("initialize")
    }
    fn open(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("open") })
    }
    fn recover_checkpoint(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("recover") })
    }
    fn change_role(&self, role: ReplicaRole, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            assert!(
                role == ReplicaRole::None
                    || self.id == 12
                    || !self
                        .factory()
                        .runtime
                        .lock()
                        .unwrap()
                        .upgrade()
                        .unwrap()
                        .owner
                        .get_is_readable()
            );
            self.event(format!("role:{role:?}"))
        })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("close") })
    }
    fn abort(&self) {
        let _ = self.event("abort");
    }
    fn remove_state(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.event("remove") })
    }
    delegate!(
        apply(
            lsn: i64,
            tx: Arc<dyn TransactionBase>,
            context: ApplyContext,
            metadata: Option<OperationData>,
            data: Option<OperationData>,
        ) -> ApiFuture<'_, Option<Arc<dyn OperationContext>>>
    );
    delegate!(unlock(context: &dyn OperationContext) -> Result<()>);
    delegate!(prepare_checkpoint(lsn: i64) -> Result<()>);
    delegate!(perform_checkpoint(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(complete_checkpoint(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(backup_checkpoint(path: PathBuf, cancellation: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(restore_checkpoint(path: PathBuf, cancellation: Cancellation) -> ApiFuture<'_, ()>);
    fn get_current_state(&self) -> Result<Option<Arc<dyn IOperationDataStream>>> {
        if *self.factory().complete_published_copy.lock().unwrap() {
            self.point.set_copy_stream(Some(
                self.point
                    .completed_checkpoint()
                    .expect("source has a completed checkpoint")
                    .into_iter()
                    .map(|(key, value)| PointProvider::payload(key, value))
                    .collect(),
            ));
        }
        let stream = self.point.get_current_state()?;
        if self.id == 10 {
            if let (Some(stream), Some((entered, release))) = (
                stream.clone(),
                self.factory().state_pause.lock().unwrap().clone(),
            ) {
                return Ok(Some(Arc::new(GatedState {
                    stream,
                    entered,
                    release,
                })));
            }
        }
        Ok(stream)
    }
    fn begin_setting_current_state(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("begin")?;
            self.point.begin_setting_current_state().await
        })
    }
    fn set_current_state(
        &self,
        number: i64,
        data: OperationData,
        cancellation: Cancellation,
    ) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.event("set")?;
            self.point
                .set_current_state(number, data, cancellation)
                .await
        })
    }
    delegate!(end_setting_current_state(cancellation: Cancellation) -> ApiFuture<'_, ()>);
    delegate!(
        prepare_for_remove(
            transaction: Arc<dyn TransactionBase>,
            cancellation: Cancellation,
        ) -> ApiFuture<'_, ()>
    );
}
struct DataLoss;
impl IDataLossHandler for DataLoss {
    fn data_loss(&self, _: Cancellation) -> ApiFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
}
type Created =
    Arc<Mutex<Option<oneshot::Sender<(Arc<VolatileRuntime>, NativeRuntimeIntegration)>>>>;
struct Factory {
    memory_bytes: usize,
    throttle: u64,
    native: NativeIntegrationProvider,
    created: Created,
    providers: Arc<Providers>,
    rows: Vec<Metadata>,
    root: bool,
}
struct Replica {
    memory_bytes: usize,
    throttle: u64,
    native: NativeRuntimeIntegration,
    created: Created,
    providers: Arc<Providers>,
    rows: Vec<Metadata>,
    root: bool,
    runtime: Mutex<Option<Arc<VolatileRuntime>>>,
}
impl IStatefulServiceFactory for Factory {
    fn create_replica(
        &self,
        _: String,
        _: String,
        _: &[u8],
        partition: uuid::Uuid,
        id: i64,
    ) -> Result<Box<dyn IStatefulServiceReplica>> {
        Ok(Box::new(Replica {
            memory_bytes: self.memory_bytes,
            throttle: self.throttle,
            native: self.native.capture(partition, id)?,
            created: self.created.clone(),
            providers: self.providers.clone(),
            rows: self.rows.clone(),
            root: self.root,
            runtime: Mutex::new(None),
        }))
    }
}
impl IStatefulServiceReplica for Replica {
    fn open(
        &self,
        _: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        _: Cancellation,
    ) -> ApiFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            let runtime = VolatileRuntime::create(
                partition,
                self.native.clone(),
                ReplicationEndpointOptions {
                    listen_address: "127.0.0.1:0".into(),
                    actor_incarnation: uuid::Uuid::new_v4(),
                },
                fube_data::Settings {
                    provider_operations: 1,
                    ..Default::default()
                },
                RuntimeConfig {
                    log_settings: super::maintenance::LogSettings {
                        checkpoint_mb: 1,
                        min_log_mb: 1,
                        truncate_factor: 2,
                        throttle_factor: self.throttle,
                    },
                    registry: Some(RegistrySettings {
                        factory: self.providers.clone(),
                        metadata: self.rows.clone(),
                    }),
                    providers: vec![],
                    work_folder: PathBuf::from("/tmp/fube-phase3"),
                    memory_bytes: self.memory_bytes,
                    data_loss: Arc::new(DataLoss),
                },
            )?;
            *self.providers.runtime.lock().unwrap() = Arc::downgrade(&runtime);
            *self.runtime.lock().unwrap() = Some(runtime.clone());
            let _ = self
                .created
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send((runtime.clone(), self.native.clone()));
            if self.root {
                Ok(Box::new((*runtime.replicator()).clone()) as Box<dyn IPrimaryReplicator>)
            } else {
                Ok(Box::new(rap::DelegatingPrimary(runtime.replicator()))
                    as Box<dyn IPrimaryReplicator>)
            }
        })
    }
    fn change_role(&self, _: ReplicaRole, _: Cancellation) -> ApiFuture<'_, String> {
        Box::pin(async { Ok("model://phase3".into()) })
    }
    fn close(&self, _: Cancellation) -> ApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn abort(&self) {}
}
struct GatedState {
    stream: Arc<dyn IOperationDataStream>,
    entered: Completion<()>,
    release: Completion<()>,
}
impl IOperationDataStream for GatedState {
    fn get_next(&self, cancel: Cancellation) -> ApiFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            self.entered.complete(Ok(()));
            self.release.wait().await.as_ref().clone()?;
            self.stream.get_next(cancel).await
        })
    }
    fn dispose(&self) -> Result<()> {
        self.stream.dispose()
    }
}
struct Fixture {
    host: Arc<rap::Host>,
    runtime: Arc<VolatileRuntime>,
    native: NativeRuntimeIntegration,
    providers: Arc<Providers>,
}
impl Fixture {
    async fn start(
        partition: uuid::Uuid,
        id: i64,
        role: ReplicaRole,
        root: bool,
        rows: Vec<Metadata>,
    ) -> Self {
        Self::start_profile(partition, id, role, root, rows, 8, 4).await
    }
    async fn start_profile(
        partition: uuid::Uuid,
        id: i64,
        role: ReplicaRole,
        root: bool,
        rows: Vec<Metadata>,
        memory_mb: usize,
        throttle: u64,
    ) -> Self {
        let providers = Providers::new();
        let input = providers.clone();
        let (send, recv) = oneshot::channel();
        let host = rap::Host::start(partition, id, role, move |native| {
            Box::new(Factory {
                memory_bytes: memory_mb * 1024 * 1024,
                throttle,
                native,
                created: Arc::new(Mutex::new(Some(send))),
                providers: input,
                rows,
                root,
            })
        })
        .await;
        let (runtime, native) = recv.await.unwrap();
        Self {
            host,
            runtime,
            native,
            providers,
        }
    }
    async fn configure(&self, replicas: Vec<NativeReplicaDescriptor>) {
        self.host
            .configure(NativeReplicaSetConfiguration {
                epoch: epoch(),
                write_quorum: 1 + replicas.len(),
                replicas,
                local_member: true,
            })
            .await
            .unwrap();
    }
    async fn copied(&self) {
        let generation = self
            .runtime
            .generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        generation.copy_done.wait().await.as_ref().clone().unwrap();
    }
    fn point(&self, id: i64) -> Arc<PointProvider> {
        self.providers.objects.lock().unwrap()[&id].point.clone()
    }
    async fn checkpoint(&self) {
        let activity = self
            .runtime
            .maintenance()
            .start_checkpoint(false)
            .unwrap()
            .unwrap();
        activity.finished.wait().await.as_ref().clone().unwrap();
    }
    async fn write(&self, id: i64, key: i64, value: i64) {
        let tx = self.runtime.owner.create_transaction().unwrap();
        self.point(id).write(tx.as_ref(), id, key, value).unwrap();
        tx.commit().await.unwrap();
    }
}
fn epoch() -> Epoch {
    Epoch {
        data_loss_number: 1,
        configuration_number: 1 << 32,
    }
}
fn rows() -> Vec<Metadata> {
    vec![
        Metadata {
            id: 10,
            parent: 0,
            name: "fabric:/phase3".into(),
            type_name: "point-fixture".into(),
            create_lsn: 0,
            delete_lsn: -1,
            initialization: None,
            mode: ProviderMode::Active,
        },
        Metadata {
            id: 11,
            parent: 10,
            name: "fabric:/phase3/child".into(),
            type_name: "point-fixture".into(),
            create_lsn: 0,
            delete_lsn: -1,
            initialization: Some(OperationData::new(vec![]).unwrap()),
            mode: ProviderMode::Active,
        },
    ]
}
fn descriptor(f: &Fixture) -> NativeReplicaDescriptor {
    NativeReplicaDescriptor {
        node_id: [2; 16],
        node_instance: 1,
        replica_id: 2,
        incarnation: 12,
        role: ReplicaRole::IdleSecondary,
        previous_role: ReplicaRole::None,
        up: true,
        ready: true,
        must_catch_up: false,
        address: f.host.replication_address().unwrap(),
        progress: ReplicationProgress { first: 1, last: 1 },
    }
}
async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn phase3_native_null_context_requires_full_and_pv_context_is_exact() {
    let mut memory = super::memory_log::MemoryLog::new(1024 * 1024).unwrap();
    memory.bootstrap().unwrap();
    let source = CopyContext {
        replica: 1,
        vector: memory.vector.clone(),
        head_epoch: Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        },
        head: 0,
        tail: 1,
        atomic_redo: 0,
    };
    assert_eq!(
        find_copy_mode(&source, &CopyContext::volatile_null(99)).unwrap(),
        CopyMode::Full(FullReason::Trimmed)
    );
    let encoded = source.encode().unwrap();
    assert_eq!(encoded.segments().len(), 1);
    assert_eq!(encoded.bytes(), 92);
    let decoded = CopyContext::decode(&encoded).unwrap();
    assert_eq!(find_copy_mode(&source, &decoded).unwrap(), CopyMode::None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_null_context_full_build_registry_hierarchy_state_log_and_epoch() {
    for root in [false, true] {
        let partition = uuid::Uuid::new_v4();
        let source = Fixture::start(partition, 1, ReplicaRole::Primary, root, rows()).await;
        source.configure(vec![]).await;
        source.write(10, 1, 10).await;
        source.write(11, 2, 20).await;
        source.checkpoint().await;
        source.write(10, 3, 30).await;
        let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, root, vec![]).await;
        source
            .host
            .build(descriptor(&target), Cancellation::default())
            .await
            .unwrap();
        target.copied().await;
        target
            .host
            .change_role(ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        assert_eq!(target.point(10).copied().get(&1), Some(&10));
        assert_eq!(target.point(11).copied().get(&2), Some(&20));
        assert_eq!(target.point(10).versions(3).last().unwrap().value, 30);
        assert!(target.runtime.owner.get_is_readable());
        let events = target.providers.events.lock().unwrap().clone();
        let at = |id, event: &str| {
            events
                .iter()
                .position(|e| e == &(id, event.into()))
                .unwrap()
        };
        assert!(at(11, "initialize") < at(10, "initialize"));
        assert!(
            at(10, "open") < at(10, "recover") && at(10, "recover") < at(10, "role:IdleSecondary")
        );
        assert!(at(10, "role:IdleSecondary") < at(11, "open"));
        let source_log = source.runtime.owner.manager.memory().unwrap();
        let target_log = target.runtime.owner.manager.memory().unwrap();
        assert_eq!(
            source_log.lock().unwrap().vector.last().unwrap().epoch,
            target_log.lock().unwrap().vector.last().unwrap().epoch
        );
        assert!(target_log.lock().unwrap().completed.is_some());
        assert!(target
            .runtime
            .generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .copy_done
            .is_completed());
        assert!(
            target
                .runtime
                .operation_events
                .lock()
                .unwrap()
                .iter()
                .any(|(stream, _, ack)| *stream == "copy" && *ack),
            "actual Full completed with acknowledged/dropped public copy operations"
        );
        drop(source.host);
        drop(target.host);
        target
            .runtime
            .terminal
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
    }
}

#[test]
fn phase3_pv_selection_validates_grouped_prefix_before_none_and_exact_partial_cases() {
    use super::{copy_codec::*, memory_log::ProgressEntry};
    let entry = |configuration, lsn| ProgressEntry {
        epoch: Epoch {
            data_loss_number: 0,
            configuration_number: configuration,
        },
        lsn,
        primary: 9,
        ticks: 33,
    };
    let ctx = |vector, tail| CopyContext {
        replica: 1,
        vector,
        head_epoch: Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        },
        head: 0,
        tail,
        atomic_redo: 0,
    };
    let source = ctx(vec![entry(0, 0), entry(1, 5), entry(3, 10)], 10);
    let target = ctx(vec![entry(0, 0), entry(2, 5), entry(3, 10)], 10);
    assert_eq!(
        find_copy_mode(&source, &target).unwrap(),
        CopyMode::Full(FullReason::Validation)
    );
    let source = ctx(vec![entry(0, 0), entry(1, 1), entry(2, 5)], 10);
    let target = ctx(vec![entry(0, 0), entry(1, 1)], 5);
    assert_eq!(
        find_copy_mode(&source, &target).unwrap(),
        CopyMode::Partial {
            source: 5,
            target: 5
        }
    );
    let mut pressure = target.clone();
    pressure.tail = 6;
    assert_eq!(
        find_copy_mode(&source, &pressure).unwrap(),
        CopyMode::FalseProgress {
            source: 5,
            target: 6
        }
    );
    pressure.head = 6;
    assert_eq!(
        find_copy_mode(&source, &pressure).unwrap(),
        CopyMode::Full(FullReason::InsufficientLogs)
    );
    let source = ctx(vec![entry(0, 0), entry(1, 5), entry(6, 6)], 9);
    let target = ctx(
        vec![
            entry(0, 0),
            entry(1, 5),
            entry(2, 6),
            entry(3, 7),
            entry(4, 8),
        ],
        8,
    );
    assert_eq!(
        find_copy_mode(&source, &target).unwrap(),
        CopyMode::Full(FullReason::Validation)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_full_pending_begin_pv_overlap_extended_provider_horizon() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 0, 0).await;
    let transaction = source.runtime.owner.create_transaction().unwrap();
    source
        .point(10)
        .write(transaction.as_ref(), 10, 1, 10)
        .unwrap();
    let point = source.point(10);
    let writing = transaction.clone();
    source
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || point.write(writing.as_ref(), 10, 2, 20))
        .await
        .unwrap();
    source
        .host
        .change_epoch(Epoch {
            data_loss_number: 1,
            configuration_number: 2 << 32,
        })
        .await
        .unwrap();
    source.write(11, 5, 50).await;
    source.checkpoint().await;
    let memory = source.runtime.owner.manager.memory().unwrap();
    let checkpoint = memory.lock().unwrap().completed.clone().unwrap();
    assert!(checkpoint.starting_lsn < checkpoint.lsn);
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
    // Pause actual sender state ownership; a later committed group extends
    // HighestStateProviderCopiedLsn, not the captured requested copy horizon.
    let gate = Completion::new();
    let state_stream =
        super::copy_sender::CopySender::create(&source.runtime, checkpoint.lsn, None)
            .await
            .unwrap();
    assert!(matches!(state_stream.mode(), CopyMode::Full(_)));
    state_stream.dispose().unwrap();
    // The actual target factory's first callback can gate real Set after open.
    // Source's GetCurrentState hook installs the receiver callback before state.
    *target.providers.copy_gate.lock().unwrap() = Some(gate.clone());
    let sender = source.host.clone();
    let member = descriptor(&target);
    let build = tokio::spawn(async move { sender.build(member, Cancellation::default()).await });
    until(|| {
        target
            .providers
            .objects
            .lock()
            .unwrap()
            .get(&10)
            .is_some_and(|p| {
                p.point
                    .events()
                    .iter()
                    .any(|e| matches!(e, PointEvent::CopySet { .. }))
            })
    })
    .await;
    let requested = source.runtime.owner.manager.tail();
    transaction.commit().await.unwrap();
    assert!(source.runtime.owner.manager.tail() > requested);
    gate.complete(Ok(()));
    build.await.unwrap().unwrap();
    target.copied().await;
    // This native command includes GetStatus: an initial secondary correctly
    // returns NotReady before copy. Transmission carries the new primary epoch;
    // after real copy EOS, synchronize the model's epoch via normal public
    // UpdateEpochAndGetStatus before the Active-role request.
    target
        .host
        .change_epoch(Epoch {
            data_loss_number: 1,
            configuration_number: 2 << 32,
        })
        .await
        .unwrap();
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    until(|| !target.point(10).versions(1).is_empty()).await;
    assert_eq!(target.point(10).versions(1).last().unwrap().value, 10);
    assert_eq!(target.point(10).versions(2).last().unwrap().value, 20);
    let target_memory = target.runtime.owner.manager.memory().unwrap();
    assert_eq!(
        target_memory.lock().unwrap().vector.len(),
        memory.lock().unwrap().vector.len(),
        "copied PV entries are not inserted twice"
    );
    assert_eq!(
        target
            .runtime
            .owner
            .get_current_epoch()
            .unwrap()
            .configuration_number,
        2 << 32
    );
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_full_post_open_factory_failure_retains_cleanup_and_failed_state_no_end_ack()
{
    for failure in ["recover", "role:IdleSecondary", "begin", "set"] {
        let partition = uuid::Uuid::new_v4();
        let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
        source.configure(vec![]).await;
        source.write(10, 1, 10).await;
        source.checkpoint().await;
        let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, vec![]).await;
        *target.providers.fail.lock().unwrap() = Some((10, failure.into()));
        let cancel = Cancellation::default();
        let token = cancel.clone();
        let sender = source.host.clone();
        let member = descriptor(&target);
        let build = tokio::spawn(async move { sender.build(member, token).await });
        target
            .runtime
            .fault_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(!target.runtime.owner.get_is_readable());
        assert!(target.runtime.owner.create_transaction().is_err());
        let last_good_ack = if failure == "set" { 3 } else { 2 };
        until(|| {
            target
                .runtime
                .operation_events
                .lock()
                .unwrap()
                .contains(&("copy", last_good_ack, true))
        })
        .await;
        assert!(
            !target.runtime.operation_events.lock().unwrap().contains(&(
                "copy",
                last_good_ack + 1,
                true
            )),
            "failed state/meta record has no public ACK or completed Full transition"
        );
        let point = target.point(10);
        assert!(!point.events().iter().any(|e| matches!(
            e,
            PointEvent::Callback {
                kind: CallbackKind::EndCopy,
                completed: true
            }
        )));
        let lower = target.runtime.replicator();
        target
            .native
            .execution()
            .try_reserve()
            .unwrap()
            .run_owned(move || {
                lower.abort();
                Ok(())
            })
            .await
            .unwrap();
        let _ = target.runtime.terminal.wait().await;
        target
            .runtime
            .cleanup_done
            .wait()
            .await
            .as_ref()
            .clone()
            .unwrap();
        assert!(target
            .providers
            .events
            .lock()
            .unwrap()
            .contains(&(10, "close".into())));
        cancel.cancel();
        let _ = build.await.unwrap();
        source.host.remove(descriptor(&target)).await.unwrap();
        drop(source.host);
        drop(target.host);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_checkpoint_and_history_reclamation_waits_reader_then_retires_old_ownership()
{
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    let memory = source.runtime.owner.manager.memory().unwrap();
    let reader = memory.lock().unwrap().pin(0).unwrap();
    let mut pending = None;
    for key in 0i64..10 {
        if key == 4 {
            let tx = source.runtime.owner.create_transaction().unwrap();
            source.point(10).write(tx.as_ref(), 10, 100, 1000).unwrap();
            let point = source.point(10);
            let writing = tx.clone();
            source
                .native
                .execution()
                .try_reserve()
                .unwrap()
                .run_owned(move || point.write(writing.as_ref(), 10, 101, 1010))
                .await
                .unwrap();
            pending = Some(tx);
        }
        let tx = source.runtime.owner.create_transaction().unwrap();
        tx.add_operation(
            Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
            None,
            Some(
                OperationData::new(vec![[key.to_le_bytes(), (key * 10).to_le_bytes()].concat()])
                    .unwrap(),
            ),
            10,
            None,
        )
        .unwrap();
        tx.commit().await.unwrap();
        if key == 5 {
            until(|| memory.lock().unwrap().completed.as_ref().unwrap().lsn > 1).await;
            let checkpoint = memory.lock().unwrap().completed.clone().unwrap();
            let id = pending.as_ref().unwrap().get_transaction_id();
            let begin = source
                .runtime
                .owner
                .manager
                .map
                .pending()
                .into_iter()
                .find(|n| n.transaction_id() == Some(id))
                .unwrap();
            assert_eq!(checkpoint.starting_lsn + 1, begin.lsn());
            assert_eq!(
                checkpoint.reader_position,
                memory.lock().unwrap().position_of(&begin).unwrap()
            );
        }
        if key == 7 {
            pending.take().unwrap().commit().await.unwrap();
        }
    }
    let maintenance = source.runtime.maintenance();
    let logging = source
        .runtime
        .logging
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    logging.group_idle().await;
    let flush = Completion::new();
    let group = Completion::new();
    *maintenance.flush_gate.lock().unwrap() = Some(flush.clone());
    *logging.group_pause.lock().unwrap() = Some(group.clone());
    let transaction = source.runtime.owner.create_transaction().unwrap();
    transaction
        .add_operation(
            Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
            None,
            Some(
                OperationData::new(vec![[10i64.to_le_bytes(), 100i64.to_le_bytes()].concat()])
                    .unwrap(),
            ),
            10,
            None,
        )
        .unwrap();
    let commit = transaction.commit(); // independently owned even before polling
    until(|| source.runtime.maintenance().pending_truncation().is_some()).await;
    let trunc = maintenance.pending_truncation().unwrap();
    assert!(!trunc.ready.is_completed() && !trunc.applied.is_completed());
    {
        let memory = memory.lock().unwrap();
        assert!(memory.flushed_position < memory.tail_position);
    }
    flush.complete(Ok(()));
    trunc.ready.wait().await.as_ref().clone().unwrap();
    assert!(
        !trunc.applied.is_completed(),
        "flushed physical record alone is not stable"
    );
    group.complete(Ok(()));
    commit.await.unwrap();
    trunc.applied.wait().await.as_ref().clone().unwrap();
    assert!(!trunc.done.is_completed());
    assert_eq!(
        memory.lock().unwrap().head,
        0,
        "logical selection does not release physical ownership"
    );
    let before = memory.lock().unwrap().allocated_bytes();
    memory.lock().unwrap().unpin(reader);
    trunc.done.wait().await.as_ref().clone().unwrap();
    assert!(memory.lock().unwrap().head > 0);
    assert!(memory.lock().unwrap().allocated_bytes() < before);
    assert!(memory.lock().unwrap().completed.as_ref().unwrap().lsn > 1);
    source.write(10, 999, 9990).await;
    drop(source.host);
    source
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_runtime_checkpoint_prepare_fence_and_abort_retains_actual_perform_owner() {
    use super::provider::ApplyPlan;
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, true, rows()).await;
    source.configure(vec![]).await;
    let apply = Completion::new();
    let perform = Completion::new();
    let point = source.point(10);
    point.inject_apply(ApplyPlan {
        gate: Some(apply.clone()),
        ..Default::default()
    });
    point.inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            gate: Some(perform.clone()),
            ..Default::default()
        },
    );
    let transaction = source.runtime.owner.create_transaction().unwrap();
    point.write(transaction.as_ref(), 10, 1, 10).unwrap();
    drop(transaction.commit());
    until(|| {
        point
            .events()
            .iter()
            .any(|e| matches!(e, PointEvent::Apply { .. }))
    })
    .await;
    let checkpoint = source
        .runtime
        .maintenance()
        .start_checkpoint(false)
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!point
        .events()
        .iter()
        .any(|e| matches!(e, PointEvent::Prepare(_))));
    apply.complete(Ok(()));
    until(|| {
        point.events().iter().any(|e| {
            matches!(
                e,
                PointEvent::Callback {
                    kind: CallbackKind::Perform,
                    completed: false
                }
            )
        })
    })
    .await;
    source.write(11, 2, 20).await; // Perform is NOT a serial root substitute.
    let lower = source.runtime.replicator();
    source
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || {
            lower.abort();
            Ok(())
        })
        .await
        .unwrap();
    assert!(!source.runtime.cleanup_done.is_completed());
    assert!(!source
        .providers
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|(_, e)| e == "close"));
    perform.complete(Ok(()));
    checkpoint.finished.wait().await.as_ref().clone().unwrap();
    let _ = source.runtime.terminal.wait().await;
    source
        .runtime
        .cleanup_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    drop(source.host);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_runtime_checkpoint_end_construction_failure_never_calls_complete_or_publishes_success(
) {
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    let memory = source.runtime.owner.manager.memory().unwrap();
    let inject = memory.clone();
    source.point(10).inject_callback(
        CallbackKind::Perform,
        CallbackPlan {
            on_enter: Some(Arc::new(move || {
                inject.lock().unwrap().write_error = Some(Error::InvalidInput(
                    "actual EndCheckpoint construction failure",
                ));
            })),
            ..Default::default()
        },
    );
    let previous = memory.lock().unwrap().completed.clone().unwrap();
    let checkpoint = source
        .runtime
        .maintenance()
        .start_checkpoint(false)
        .unwrap()
        .unwrap();
    assert!(checkpoint.finished.wait().await.is_err());
    source
        .runtime
        .fault_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
    assert_eq!(
        memory
            .lock()
            .unwrap()
            .completed
            .as_ref()
            .unwrap()
            .begin_position,
        previous.begin_position
    );
    assert!(!source.point(10).events().iter().any(|e| matches!(
        e,
        PointEvent::Callback {
            kind: CallbackKind::Complete,
            ..
        }
    )));
    assert!(!source.runtime.owner.get_is_readable());
    drop(source.host);
    let _ = source.runtime.terminal.wait().await;
    source
        .runtime
        .cleanup_done
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[test]
fn phase3_registry_native_empty_initialization_fixture_and_full_reader_range_identity() {
    use super::{memory_log::MemoryLog, registry_codec};
    let row = Metadata {
        id: 10,
        parent: 0,
        name: "n".into(),
        type_name: "t".into(),
        create_lsn: 1,
        delete_lsn: -1,
        initialization: Some(OperationData::new(vec![]).unwrap()),
        mode: ProviderMode::Active,
    };
    let mut fixture = 48i32.to_le_bytes().to_vec();
    fixture.extend([1, b'n', 1, b't']);
    fixture.extend(1i64.to_le_bytes());
    fixture.extend((-1i64).to_le_bytes());
    fixture.extend(0i32.to_le_bytes());
    fixture.extend(0i32.to_le_bytes());
    fixture.extend(10i64.to_le_bytes());
    fixture.extend(0i64.to_le_bytes());
    assert_eq!(row.encode().unwrap(), fixture);
    let packets = registry_codec::encode_copy(vec![row]).unwrap();
    assert_eq!(packets[0].segments()[0].as_ref(), [1, 0, 0, 0, 0]);
    fixture.push(1);
    assert_eq!(packets[1].segments()[0].as_ref(), fixture);
    let mut memory = MemoryLog::new(1024 * 1024).unwrap();
    memory.bootstrap().unwrap();
    let ordinary = memory.pin(0).unwrap();
    let full = memory.pin_copy(0, true).unwrap();
    assert_eq!(memory.reader_count(), 2);
    memory.unpin(full);
    // Native shared reader range retains FullCopy type until the final reader
    // at this position disposes; adding FullCopy does not replace its start LSN.
    assert_eq!(memory.reader_count(), 1);
    memory.unpin(ordinary);
    assert_eq!(memory.reader_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_native_old_transaction_abort_is_real_owned_end_not_forced_map_removal() {
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    let transaction = source.runtime.owner.create_transaction().unwrap();
    source
        .point(10)
        .write(transaction.as_ref(), 10, 100, 1000)
        .unwrap();
    let point = source.point(10);
    let writing = transaction.clone();
    source
        .native
        .execution()
        .try_reserve()
        .unwrap()
        .run_owned(move || point.write(writing.as_ref(), 10, 101, 1010))
        .await
        .unwrap();
    for key in 0i64..7 {
        let tx = source.runtime.owner.create_transaction().unwrap();
        tx.add_operation(
            Some(OperationData::new(vec![vec![0; 200 * 1024]]).unwrap()),
            None,
            Some(
                OperationData::new(vec![[key.to_le_bytes(), (key * 10).to_le_bytes()].concat()])
                    .unwrap(),
            ),
            10,
            None,
        )
        .unwrap();
        tx.commit().await.unwrap();
    }
    until(|| transaction.get_state() == TransactionState::Aborted).await;
    assert!(!source.point(10).is_locked(100));
    assert!(source.runtime.owner.manager.records().iter().any(|a|matches!(a.node.record().kind,super::record::RecordKind::End{transaction_id,committed:false,..} if transaction_id==transaction.get_transaction_id())));
    drop(source.host);
    source
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_full_resurrects_retained_objects_and_checkpoint_cleans_stale_in_native_order(
) {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    source.write(11, 2, 20).await;
    source.checkpoint().await;
    let mut retained = rows();
    retained.push(Metadata {
        id: 12,
        parent: 0,
        name: "fabric:/orphan".into(),
        type_name: "point-fixture".into(),
        create_lsn: 0,
        delete_lsn: -1,
        initialization: None,
        mode: ProviderMode::Active,
    });
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, false, retained).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    let events = target.providers.events.lock().unwrap().clone();
    assert_eq!(
        events.iter().filter(|e| **e == (10, "open".into())).count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| **e == (10, "recover".into()))
            .count(),
        1
    );
    assert_eq!(target.point(10).copied().get(&1), Some(&10));
    let at = |event: &str| {
        events
            .iter()
            .position(|e| e == &(12, event.into()))
            .unwrap()
    };
    assert!(at("role:None") < at("remove") && at("remove") < at("close"));
    assert!(!target.runtime.registry.rows().iter().any(|r| r.id == 12));
    assert!(!target
        .runtime
        .owner
        .manager
        .directory
        .entries
        .lock()
        .unwrap()
        .contains_key(&12));
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_native_explicit_context_sender_none_partial_preserves_lsn_and_disposes_reader() {
    use super::copy_codec::{remove_stage, CopyHeader, Stage};
    use fube_data::IOperationDataStream as _;
    let source = Fixture::start(uuid::Uuid::new_v4(), 1, ReplicaRole::Primary, false, rows()).await;
    source.configure(vec![]).await;
    source.write(10, 1, 10).await;
    let logging = source
        .runtime
        .logging
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    logging.group_idle().await;
    let memory = source.runtime.owner.manager.memory().unwrap();
    let tail = source.runtime.owner.manager.tail();
    let context = {
        let memory = memory.lock().unwrap();
        let (_, _, head, head_epoch) = memory.head_identity();
        CopyContext {
            replica: 2,
            vector: memory.vector.clone(),
            head_epoch,
            head,
            tail,
            atomic_redo: 0,
        }
    };
    let none = super::copy_sender::CopySender::create(&source.runtime, tail, Some(context.clone()))
        .await
        .unwrap();
    assert_eq!(none.mode(), CopyMode::None);
    assert_eq!(
        CopyHeader::decode(
            &none
                .get_next(Cancellation::default())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap()
        .stage,
        Stage::None
    );
    assert!(none
        .get_next(Cancellation::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(memory.lock().unwrap().reader_count(), 0);
    let mut context = context;
    context.tail = 1;
    let partial = super::copy_sender::CopySender::create(&source.runtime, tail, Some(context))
        .await
        .unwrap();
    assert!(matches!(partial.mode(), CopyMode::Partial { .. }));
    assert_eq!(memory.lock().unwrap().reader_count(), 1);
    assert_eq!(
        CopyHeader::decode(
            &partial
                .get_next(Cancellation::default())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap()
        .stage,
        Stage::Log
    );
    let mut records = vec![];
    while let Some(packet) = partial.get_next(Cancellation::default()).await.unwrap() {
        let (stage, data) = remove_stage(&packet).unwrap();
        assert_eq!(stage, Stage::Log);
        records.extend(super::record::Record::decode_batch(&data).unwrap());
    }
    assert!(records
        .iter()
        .any(|r| matches!(r.kind, super::record::RecordKind::UpdateEpoch { .. }) && r.lsn == 1));
    assert!(records.iter().any(|r| matches!(
        r.kind,
        super::record::RecordKind::Begin { single: true, .. }
    ) && r.lsn == 3)); // native first-primary establishing Barrier owns LSN2
    partial.dispose().unwrap();
    assert_eq!(memory.lock().unwrap().reader_count(), 0);
    assert!(partial.get_next(Cancellation::default()).await.is_err());
    drop(source.host);
    source
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase3_actual_empty_full_bootstrap_seeds_real_epoch_and_finishes_checkpoint() {
    let partition = uuid::Uuid::new_v4();
    let source = Fixture::start(partition, 1, ReplicaRole::Primary, true, vec![]).await;
    source.configure(vec![]).await;
    let target = Fixture::start(partition, 2, ReplicaRole::IdleSecondary, true, vec![]).await;
    source
        .host
        .build(descriptor(&target), Cancellation::default())
        .await
        .unwrap();
    target.copied().await;
    target
        .host
        .change_role(ReplicaRole::ActiveSecondary)
        .await
        .unwrap();
    {
        let memory = target.runtime.owner.manager.memory().unwrap();
        let memory = memory.lock().unwrap();
        assert!(
            memory
                .entries
                .iter()
                .any(|e| e.native_type == 5 && e.lsn == 0),
            "seed is an actual local physical frame, not an ordering-only activity"
        );
        assert!(memory.completed.is_some());
        for entry in memory.entries.iter().filter(|e| e.native_type == 7) {
            let body =
                u32::from_le_bytes(memory.read(entry.position, 4).try_into().unwrap()) as u64;
            assert_eq!(
                memory.read(entry.position + 4 + body - 16, 16),
                [memory.periodic_times.0.to_le_bytes(),memory.periodic_times.1.to_le_bytes()].concat(),
                "native recovered timestamp caches remain populated with periodic scheduling disabled"
            );
        }
    }
    assert_eq!(target.runtime.owner.manager.tail(), 1);
    assert!(target.runtime.owner.get_is_readable());
    drop(source.host);
    drop(target.host);
    target
        .runtime
        .terminal
        .wait()
        .await
        .as_ref()
        .clone()
        .unwrap();
}
