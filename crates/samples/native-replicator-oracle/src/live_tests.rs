use super::*;
use crate::{data::read_segments, stream::end_get_next};
use mssf_core::sync::fabric_begin_end_proxy;

async fn next(stream: &IFabricOperationDataStream) -> Result<Option<IFabricOperationData>> {
    let context = unsafe { stream.BeginGetNext(None)? };
    while !unsafe { context.IsCompleted() } {
        tokio::task::yield_now().await;
    }
    end_get_next(stream, &context)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_native_horizon_waits_for_local_apply_and_copies_ledger() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    let native = app.bridge();
    let copy = unsafe { native.GetCopyState(FABRIC_SEQUENCE_NUMBER(1), None) }.unwrap();
    let first = tokio::time::timeout(Duration::from_millis(20), next(&copy)).await;
    // Dropping a native receiver cancels its actual context; begin a fresh stream.
    assert!(first.is_err());
    let copy = unsafe { native.GetCopyState(FABRIC_SEQUENCE_NUMBER(1), None) }.unwrap();
    let begin = copy.clone();
    let end = copy.clone();
    let rx = fabric_begin_end_proxy(
        move |callback| unsafe { begin.BeginGetNext(callback) },
        move |context| end_get_next(&end, context.ok_or(com::E_POINTER)?),
        None,
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    for sequence in 1..=2 {
        let mutation = fube_data::Mutation {
            id: format!("r{sequence}"),
            key: "same".into(),
            value: sequence.to_string(),
        };
        app.provider
            .apply_replication(sequence, &mutation.operation().unwrap())
            .unwrap();
    }
    let target = MemoryProvider::new();
    let first = rx.await.unwrap().unwrap().unwrap();
    target
        .apply_copy(
            &fube_data::OperationData::new(unsafe { read_segments(&first) }.unwrap()).unwrap(),
        )
        .unwrap();
    while let Some(data) = next(&copy).await.unwrap() {
        target
            .apply_copy(
                &fube_data::OperationData::new(unsafe { read_segments(&data) }.unwrap()).unwrap(),
            )
            .unwrap();
    }
    assert_eq!(target.snapshot().last, 1);
    assert_eq!(target.snapshot().values["same"], "1");
    assert_eq!(target.snapshot().results.len(), 1);
    assert!(next(&copy).await.unwrap().is_none());
    assert_eq!(
        unsafe { native.GetLastCommittedSequenceNumber() }
            .unwrap()
            .0,
        2
    );
    workers.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_context_async_eos_cancellation_and_once_only_end() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    let provider = app.bridge();
    let empty = unsafe { provider.GetCopyContext() }.unwrap();
    assert!(next(&empty).await.unwrap().is_none());
    assert!(unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(0), &empty) }.is_err());
    let stream = unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(10), None) }.unwrap();
    let context = unsafe { stream.BeginGetNext(None) }.unwrap();
    assert!(!unsafe { context.CompletedSynchronously() });
    assert_eq!(
        end_get_next(&stream, &context).unwrap_err().code(),
        crate::async_context::E_PENDING
    );
    unsafe { context.Cancel() }.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !unsafe { context.IsCompleted() } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        end_get_next(&stream, &context).unwrap_err().code(),
        crate::async_context::E_ABORT
    );
    assert_eq!(
        end_get_next(&stream, &context).unwrap_err().code(),
        com::E_INVALIDARG
    );
    workers.join().await.unwrap();
}

struct EmptyThenFailure(std::sync::atomic::AtomicUsize);
impl OperationDataStream for EmptyThenFailure {
    fn next(
        &self,
        _: Cancellation,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = fube_data::Result<Option<fube_data::OperationData>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Some(fube_data::OperationData::new(Vec::new())?))
            } else {
                Err(fube_data::Error::Protocol("injected provider failure"))
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_empty_data_is_not_eos_and_provider_failure_reaches_callback() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    let stream = stream(
        app,
        Source::Ready(Arc::new(EmptyThenFailure(AtomicUsize::new(0)))),
        None,
    )
    .unwrap();
    let empty = next(&stream)
        .await
        .unwrap()
        .expect("non-null empty operation");
    assert!(unsafe { read_segments(&empty) }.unwrap().is_empty());
    assert!(next(&stream).await.is_err());
    assert!(next(&stream).await.is_err());
    workers.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_epoch_and_data_loss_reconcile_real_state_and_reject_foreign_end() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    let provider = app.bridge();
    for sequence in 1..=2 {
        app.provider
            .apply_replication(
                sequence,
                &fube_data::Mutation {
                    id: format!("r{sequence}"),
                    key: "k".into(),
                    value: sequence.to_string(),
                }
                .operation()
                .unwrap(),
            )
            .unwrap();
    }
    assert!(unsafe {
        provider.BeginUpdateEpoch(std::ptr::null(), FABRIC_SEQUENCE_NUMBER(1), None)
    }
    .is_err());
    let epoch = FABRIC_EPOCH {
        DataLossNumber: 7,
        ConfigurationNumber: 9,
        ..Default::default()
    };
    let context =
        unsafe { provider.BeginUpdateEpoch(&epoch, FABRIC_SEQUENCE_NUMBER(1), None) }.unwrap();
    let foreign = crate::context::completed(None.into()).unwrap();
    assert_eq!(
        unsafe { provider.EndUpdateEpoch(&foreign) }
            .ok()
            .unwrap_err()
            .code(),
        com::E_INVALIDARG
    );
    while !unsafe { context.IsCompleted() } {
        tokio::task::yield_now().await;
    }
    unsafe { provider.EndUpdateEpoch(&context) }.unwrap();
    assert!(unsafe { context.IsCompleted() });
    assert!(unsafe { provider.EndUpdateEpoch(&context) }.is_err());
    assert_eq!(app.provider.snapshot().last, 1);
    assert_eq!(app.provider.snapshot().epoch_configuration, 9);
    assert_eq!(app.provider.snapshot().results.len(), 1);
    app.provider.request_data_loss(0).unwrap();
    let begin = provider.clone();
    let end = provider.clone();
    assert!(fabric_begin_end_proxy(
        move |callback| unsafe { begin.BeginOnDataLoss(callback) },
        move |context| unsafe { end.EndOnDataLoss(context) },
        None,
    )
    .await
    .unwrap()
    .unwrap());
    assert!(app.provider.snapshot().results.is_empty());
    workers.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_contexts_publish_panics_and_release_unended_results_once() {
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let workers = Workers::new(tokio::runtime::Handle::current());
    let slot = Slot::<Probe>::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Probe(drops.clone());
    let context = slot
        .begin(&workers, None.into(), move |_| async move { Ok(probe) })
        .unwrap();
    while !unsafe { context.IsCompleted() } {
        tokio::task::yield_now().await;
    }
    drop(slot);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(context);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let slot = Slot::<()>::default();
    let context = slot
        .begin(&workers, None.into(), |_| async {
            panic!("injected callback future panic")
        })
        .unwrap();
    while !unsafe { context.IsCompleted() } {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        slot.end((&context).into()).unwrap_err().code(),
        com::E_UNEXPECTED
    );
    assert!(slot.end((&context).into()).is_err());
    workers.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_drain_retains_its_task_handles_and_work_captures_until_joined() {
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let workers = Workers::new(tokio::runtime::Handle::current());
    let drops = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let work_release = release.clone();
    let probe = Probe(drops.clone());
    workers
        .spawn(async move {
            work_release.notified().await;
            drop(probe);
        })
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), workers.join())
            .await
            .is_err()
    );
    assert!(!workers.can_release());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    release.notify_one();
    workers.join_fully().await.unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(workers.can_release());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_copy_delay_pins_requested_horizon_while_live_writes_advance_and_records_eos() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    assert_eq!(app.copy_delay.load(Ordering::Acquire), 0);
    app.copy_delay.store(20, Ordering::Release);
    for sequence in 1..=5 {
        app.provider
            .apply_replication(
                sequence,
                &fube_data::Mutation {
                    id: format!("r{sequence}"),
                    key: "key".into(),
                    value: sequence.to_string(),
                }
                .operation()
                .unwrap(),
            )
            .unwrap();
    }
    let provider = app.bridge();
    let stream = unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(5), None) }.unwrap();
    let live = app.clone();
    let writer = tokio::spawn(async move {
        for sequence in 6..=105 {
            let _gate = live.gate.lock().await;
            live.provider
                .apply_replication(
                    sequence,
                    &fube_data::Mutation {
                        id: format!("r{sequence}"),
                        key: "key".into(),
                        value: sequence.to_string(),
                    }
                    .operation()
                    .unwrap(),
                )
                .unwrap();
            drop(_gate);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let target = MemoryProvider::new();
    let mut items = 0;
    loop {
        let end = stream.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let callback = mssf_core::sync::AwaitableCallback::new_interface(move |context| {
            let _ = sender.send(end_get_next(&end, context.as_ref().unwrap()));
        });
        let started = tokio::time::Instant::now();
        let context = unsafe { stream.BeginGetNext(&callback) }.unwrap();
        let value = tokio::time::timeout(Duration::from_secs(2), receiver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(end_get_next(&stream, &context).is_err());
        let Some(data) = value else { break };
        assert!(started.elapsed() >= Duration::from_millis(20));
        items += 1;
        target
            .apply_copy(
                &fube_data::OperationData::new(unsafe { read_segments(&data) }.unwrap()).unwrap(),
            )
            .unwrap();
    }
    let observed = app.copy_observations().unwrap();
    let copy = &observed[0];
    assert_eq!(copy["requested_horizon"], 5);
    assert_eq!(copy["build_context"]["requested_horizon"], 5);
    assert!(copy["build_context"]["provider_progress"].as_i64().unwrap() >= 5);
    assert!(copy["provider_progress_at_end"].as_i64().unwrap() > 5);
    assert_eq!(copy["enumeration_end"], true);
    assert!(copy["items"].as_u64().unwrap() > 0);
    assert_eq!(copy["items"], items);
    assert_eq!(copy["begin_count"], items + 1);
    assert_eq!(copy["end_count"], items + 1);
    assert_eq!(copy["callback_count"], items + 1);
    assert_eq!(target.snapshot().last, 5);
    assert_eq!(target.snapshot().values["key"], "5");
    assert_eq!(target.snapshot().results.len(), 5);
    let progress_at_end = copy["provider_progress_at_end"].clone();
    writer.await.unwrap();
    assert!(next(&stream).await.unwrap().is_none());
    assert_eq!(
        app.copy_observations().unwrap()[0]["provider_progress_at_end"],
        progress_at_end
    );
    for horizon in 6..=45 {
        unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(horizon), None) }.unwrap();
    }
    let observed = app.copy_observations().unwrap();
    assert_eq!(observed.as_array().unwrap().len(), 32);
    assert_eq!(observed[0]["requested_horizon"], 14);
    assert_eq!(observed[31]["requested_horizon"], 45);
    assert_eq!(observed[31]["enumeration_end"], false);
    assert!(observed[31]["build_context"].is_null());
    workers.join_fully().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_delayed_copy_item_fails_stream_without_losing_an_item_then_resuming() {
    let workers = Workers::new(tokio::runtime::Handle::current());
    let app = Application::new(workers.clone());
    app.copy_delay.store(100, Ordering::Release);
    let provider = app.bridge();
    let stream = unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(0), None) }.unwrap();
    let context = unsafe { stream.BeginGetNext(None) }.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!unsafe { context.IsCompleted() });
    unsafe { context.Cancel() }.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !unsafe { context.IsCompleted() } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        end_get_next(&stream, &context).unwrap_err().code(),
        crate::async_context::E_ABORT
    );
    assert!(next(&stream).await.is_err());
    let observed = app.copy_observations().unwrap();
    assert_eq!(observed[0]["items"], 0);
    assert_eq!(observed[0]["enumeration_end"], false);
    assert_eq!(observed[0]["end_count"], 2);
    workers.join_fully().await.unwrap();
}
