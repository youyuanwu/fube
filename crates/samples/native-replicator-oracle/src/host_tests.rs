use super::test_doubles::{callback, EngineState};
use super::*;
use std::sync::atomic::AtomicUsize;

#[test]
fn common_http_schema_percent_utf8_and_limits() {
    let Request::Write(mutation) = parse("write", "id=a&key=%C3%A9&value=a%26b%3Dc+z").unwrap()
    else {
        panic!()
    };
    assert_eq!(mutation.key, "é");
    assert_eq!(mutation.value, "a&b=c z");
    for (operation, query) in [
        ("write", "id=a&key=k&unknown=v"),
        ("write", "id=a&key=k&value=v&id=b"),
        ("write", "id=a&key=%FF&value=v"),
        ("write", "id=a&key=%0&value=v"),
        ("service", "unknown=1"),
        ("diagnostics", "unknown=1"),
        ("test", "ack_delay_ms=2001"),
        ("test", "ack_delay_ms=-1"),
        ("test", "ack_delay_ms="),
        ("test", ""),
        ("test", "unknown=0"),
        ("test", "ack_delay_ms=1&copy_delay_ms=1"),
        ("test", "copy_delay_ms=1&copy_delay_ms=1"),
        ("test", "copy_delay_ms=101"),
        ("test", "copy_delay_ms=-1"),
        ("test", "copy_delay_ms="),
        ("test", "copy_delay_ms=18446744073709551616"),
    ] {
        assert!(parse(operation, query).is_err(), "{operation} {query}");
    }

    assert!(parse("service", &"x".repeat(LIMIT + 1)).is_err());
    assert!(matches!(
        parse("test", "ack_delay_ms=2000"),
        Ok(Request::Test {
            copy: false,
            delay: 2000
        })
    ));
    for delay in [0, 100] {
        assert!(matches!(
            parse("test", &format!("copy_delay_ms={delay}")),
            Ok(Request::Test { copy: true, delay: value }) if value == delay
        ));
    }
}

fn route() -> Arc<Route> {
    let workers = Workers::new(tokio::runtime::Handle::current());
    Arc::new(Route {
        app: Application::new(workers.clone()),
        native: Mutex::new(None),
        role: Mutex::new(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_NONE),
        partition: Uuid::new_v4().to_string(),
        replica: 7,
        incarnation: Uuid::new_v4(),
        service_address: "http://127.0.0.1:1/data/test/service".into(),
        node: "test".into(),
        host: "test".into(),
        bind_address: "127.0.0.1:0".into(),
        workers,
        pump: tokio::sync::Mutex::new(None),
        delay: AtomicU64::new(0),
        copy_done: AtomicBool::new(false),
        copy_stream_items: AtomicU64::new(0),
        copy_stream_eos: AtomicU64::new(0),
        replication_stream_items: AtomicU64::new(0),
        replication_stream_eos: AtomicU64::new(0),
        received: AtomicI64::new(0),
        assigned: AtomicI64::new(0),
        completions: Mutex::new(BTreeMap::new()),
        admission: Arc::new(tokio::sync::Semaphore::new(64)),
        control: tokio::sync::Mutex::new(()),
        active: Mutex::new(0),
        inactive: Notify::new(),
        fault: Mutex::new(None),
        stopped: AtomicBool::new(false),
    })
}

fn replica(route: Arc<Route>) -> (IFabricStatefulServiceReplica, Arc<Workers>) {
    let lifecycle = Workers::new(tokio::runtime::Handle::current());
    (
        Replica {
            route,
            lifecycle: lifecycle.clone(),
            open: Slot::default(),
            role: Slot::default(),
            close: Slot::default(),
        }
        .into(),
        lifecycle,
    )
}

async fn completed(context: &IFabricAsyncOperationContext) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !unsafe { context.IsCompleted() } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn primary(
    route: &Arc<Route>,
    engine: &Arc<EngineState>,
) -> (IFabricStatefulServiceReplica, Arc<Workers>) {
    let (replica, lifecycle) = replica(route.clone());
    let (partition, control) = engine.partition();
    let context = unsafe {
        replica.BeginOpen(
            FABRIC_REPLICA_OPEN_MODE::FABRIC_REPLICA_OPEN_MODE_NEW,
            &partition,
            None,
        )
    }
    .unwrap();
    completed(&context).await;
    let actual = unsafe { replica.EndOpen(&context) }.unwrap();
    assert_eq!(actual.as_raw(), control.as_raw());
    assert_eq!(engine.creates.load(Ordering::Acquire), 1);
    assert_eq!(engine.control_opens.load(Ordering::Acquire), 0);
    let context =
        unsafe { replica.BeginChangeRole(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY, None) }
            .unwrap();
    completed(&context).await;
    unsafe { replica.EndChangeRole(&context) }.unwrap();
    (replica, lifecycle)
}

struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_replica_abort_reentered_after_end_open_or_role_drains_other_work() {
    for opening in [true, false] {
        let route = route();
        let engine = EngineState::new();
        let (partition, control) = engine.partition();
        let (replica, lifecycle) = replica(route.clone());
        if !opening {
            let context = unsafe {
                replica.BeginOpen(
                    FABRIC_REPLICA_OPEN_MODE::FABRIC_REPLICA_OPEN_MODE_NEW,
                    &partition,
                    None,
                )
            }
            .unwrap();
            completed(&context).await;
            unsafe { replica.EndOpen(&context) }.unwrap();
        }
        let provider = route.app.bridge();
        let stream = unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(100), None) }.unwrap();
        let pending = unsafe { stream.BeginGetNext(None) }.unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        let stop = route.workers.stop.clone();
        route
            .workers
            .spawn(async move {
                stop.cancelled().await;
                drop(probe);
            })
            .unwrap();
        let callback_drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let owner = replica.clone();
        let observed = route.clone();
        let callback = callback(callback_drops.clone(), move |context| {
            let (ended, once) = if opening {
                let result = unsafe { owner.EndOpen(&context) };
                (
                    result.is_ok_and(|actual| actual.as_raw() == control.as_raw()),
                    unsafe { owner.EndOpen(&context) }.is_err(),
                )
            } else {
                (
                    unsafe { owner.EndChangeRole(&context) }.is_ok(),
                    unsafe { owner.EndChangeRole(&context) }.is_err(),
                )
            };
            // The async lifecycle future (including its control lock) is gone.
            let released = observed.control.try_lock().is_ok();
            unsafe { owner.Abort() };
            let _ = tx.send((
                ended,
                once,
                released,
                observed.stopped.load(Ordering::Acquire),
            ));
        });
        let context = if opening {
            unsafe {
                replica.BeginOpen(
                    FABRIC_REPLICA_OPEN_MODE::FABRIC_REPLICA_OPEN_MODE_NEW,
                    &partition,
                    &callback,
                )
            }
            .unwrap()
        } else {
            unsafe {
                replica.BeginChangeRole(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY, &callback)
            }
            .unwrap()
        };
        drop(callback);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .unwrap()
                .unwrap(),
            (true, true, true, true)
        );
        assert!(unsafe { pending.IsCompleted() });
        assert_eq!(
            crate::stream::end_get_next(&stream, &pending)
                .unwrap_err()
                .code(),
            E_ABORT
        );
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(route.app.callbacks.load(Ordering::Acquire), 0);
        assert!(route.native.lock().unwrap().is_none());
        drop(context);
        lifecycle.join().await.unwrap();
        assert!(lifecycle.can_release());
        assert_eq!(callback_drops.load(Ordering::Acquire), 1);
        engine.workers.join().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activated_host_owns_reentrant_callback_tail_after_all_client_references_are_dropped() {
    let host = Arc::new(Host {
        rt: tokio::runtime::Handle::current(),
        node: "test".into(),
        ip: "127.0.0.1".parse().unwrap(),
        port: 1,
        host: "test".into(),
        routes: Mutex::new(HashMap::new()),
        worker_owners: Mutex::new(Vec::new()),
    });
    let factory: IFabricStatefulServiceFactory = Factory(host.clone()).into();
    let replica = unsafe {
        factory.CreateReplica(
            WString::from(TYPE).as_pcwstr(),
            FABRIC_URI::default(),
            0,
            std::ptr::null(),
            GUID::from_u128(1),
            FABRIC_REPLICA_ID(7),
        )
    }
    .unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let owner = replica.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let callback = callback(drops.clone(), move |context| {
        let ended = unsafe { owner.EndChangeRole(&context) }.is_ok();
        unsafe { owner.Abort() };
        let _ = entered.send(ended);
        let _ = wait.recv();
    });
    let context = unsafe {
        replica.BeginChangeRole(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY, &callback)
    }
    .unwrap();
    drop(callback);
    assert!(tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap());
    drop(context);
    drop(replica);
    let owners = host.worker_owners.lock().unwrap().clone();
    assert_eq!(owners.len(), 2);
    assert!(owners.iter().any(|workers| !workers.can_release()));
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let joined = tokio::spawn(async move {
        for workers in owners {
            workers.join_fully().await.unwrap();
        }
    });
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), joined)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(host
        .worker_owners
        .lock()
        .unwrap()
        .iter()
        .all(|workers| workers.can_release()));
    assert!(host
        .routes
        .lock()
        .unwrap()
        .values()
        .all(|route| route.upgrade().is_none()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_and_copy_callbacks_reenter_actual_abort_with_once_only_end_and_disposal() {
    for kind in 0..3 {
        let route = route();
        let (replica, lifecycle) = replica(route.clone());
        let provider = route.app.bridge();
        let stream = unsafe { provider.GetCopyState(FABRIC_SEQUENCE_NUMBER(0), None) }.unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        let stop = route.workers.stop.clone();
        route
            .workers
            .spawn(async move {
                stop.cancelled().await;
                drop(probe);
            })
            .unwrap();
        let callback_drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let end = provider.clone();
        let enumeration = stream.clone();
        let observed = route.clone();
        let callback = callback(callback_drops.clone(), move |context| {
            let (ended, once) = match kind {
                0 => (
                    unsafe { end.EndUpdateEpoch(&context) }.is_ok(),
                    unsafe { end.EndUpdateEpoch(&context) }.is_err(),
                ),
                1 => (
                    unsafe { end.EndOnDataLoss(&context) }.is_ok(),
                    unsafe { end.EndOnDataLoss(&context) }.is_err(),
                ),
                _ => (
                    crate::stream::end_get_next(&enumeration, &context).is_ok(),
                    crate::stream::end_get_next(&enumeration, &context).is_err(),
                ),
            };
            let released = observed.app.callbacks.load(Ordering::Acquire) == 0;
            unsafe { replica.Abort() };
            let _ = tx.send((
                ended,
                once,
                released,
                observed.stopped.load(Ordering::Acquire),
            ));
        });
        let epoch = FABRIC_EPOCH {
            DataLossNumber: 1,
            ConfigurationNumber: 2,
            ..Default::default()
        };
        let context = match kind {
            0 => unsafe { provider.BeginUpdateEpoch(&epoch, FABRIC_SEQUENCE_NUMBER(0), &callback) }
                .unwrap(),
            1 => unsafe { provider.BeginOnDataLoss(&callback) }.unwrap(),
            _ => unsafe { stream.BeginGetNext(&callback) }.unwrap(),
        };
        drop(callback);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .unwrap()
                .unwrap(),
            (true, true, true, true)
        );
        assert_eq!(drops.load(Ordering::Acquire), 1);
        drop(context);
        route.workers.join().await.unwrap();
        lifecycle.join().await.unwrap();
        assert!(route.workers.can_release());
        assert_eq!(callback_drops.load(Ordering::Acquire), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_callback_aborts_and_outside_full_drain_do_not_join_each_others_stacks() {
    let route = route();
    let (replica, lifecycle) = replica(route.clone());
    let provider = route.app.bridge();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let entered = Arc::new(AtomicUsize::new(0));
    let mut receivers = Vec::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut contexts = Vec::new();
    for loss in [true, false] {
        let barrier = barrier.clone();
        let entered = entered.clone();
        let owner = replica.clone();
        let end = provider.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        receivers.push(receiver);
        let callback = callback(drops.clone(), move |context| {
            let ended = if loss {
                unsafe { end.EndOnDataLoss(&context) }.is_ok()
            } else {
                unsafe { end.EndUpdateEpoch(&context) }.is_ok()
            };
            entered.fetch_add(1, Ordering::AcqRel);
            barrier.wait();
            unsafe { owner.Abort() };
            let _ = sender.send(ended);
        });
        contexts.push(if loss {
            unsafe { provider.BeginOnDataLoss(&callback) }.unwrap()
        } else {
            let epoch = FABRIC_EPOCH {
                DataLossNumber: 1,
                ConfigurationNumber: 2,
                ..Default::default()
            };
            unsafe { provider.BeginUpdateEpoch(&epoch, FABRIC_SEQUENCE_NUMBER(0), &callback) }
                .unwrap()
        });
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while entered.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), route.workers.join_fully())
        .await
        .unwrap()
        .unwrap();
    for receiver in receivers {
        assert!(receiver.await.unwrap());
    }
    drop(contexts);
    lifecycle.join_fully().await.unwrap();
    assert!(route.workers.can_release());
    assert_eq!(drops.load(Ordering::Acquire), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_worker_failure_still_disposes_unended_lifecycle_results() {
    for abort in [false, true] {
        let route = route();
        let (replica, lifecycle) = replica(route.clone());
        let role = unsafe {
            replica.BeginChangeRole(FABRIC_REPLICA_ROLE::FABRIC_REPLICA_ROLE_PRIMARY, None)
        }
        .unwrap();
        completed(&role).await;
        route
            .workers
            .spawn(async { panic!("injected disposal worker failure") })
            .unwrap();
        if abort {
            unsafe { replica.Abort() };
        } else {
            let close = unsafe { replica.BeginClose(None) }.unwrap();
            completed(&close).await;
            assert_eq!(unsafe { replica.EndClose(&close) }, com::E_UNEXPECTED);
            assert_eq!(unsafe { replica.EndClose(&close) }, com::E_INVALIDARG);
        }
        assert_eq!(
            unsafe { replica.EndChangeRole(&role) }.unwrap_err().code(),
            com::E_INVALIDARG
        );
        assert!(route.stopped.load(Ordering::Acquire));
        lifecycle.join_fully().await.unwrap();
        assert!(lifecycle.can_release());
        assert!(route.workers.can_release());
    }
}

fn mutation(id: &str) -> Mutation {
    Mutation {
        id: id.into(),
        key: "key".into(),
        value: "value".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_write_begin_failure_does_not_apply_even_with_dirty_assigned_lsn() {
    let route = route();
    let engine = EngineState::new();
    let (replica, lifecycle) = primary(&route, &engine).await;
    engine.fail_begin.store(true, Ordering::Release);
    let (status, body) = route.write(mutation("failed")).await;
    assert_eq!(status, 503);
    assert_eq!(body["outcome"], "rejected");
    assert_eq!(engine.ends.load(Ordering::Acquire), 0);
    assert_eq!(route.app.provider.snapshot().last, 0);
    assert_eq!(route.assigned.load(Ordering::Acquire), 0);
    assert!(route.completions.lock().unwrap().is_empty());
    unsafe { replica.Abort() };
    lifecycle.join().await.unwrap();
    engine.workers.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assigned_lsn_applies_before_delayed_end_and_retries_keep_native_outcomes() {
    for fail in [false, true] {
        let route = route();
        let engine = EngineState::new();
        let (replica, lifecycle) = primary(&route, &engine).await;
        engine.fail_end.store(fail, Ordering::Release);
        let writer = route.clone();
        let task = tokio::spawn(async move { writer.write(mutation("known")).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while route.app.provider.snapshot().last == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(route.app.provider.snapshot().values["key"], "value");
        assert_eq!(route.assigned.load(Ordering::Acquire), 1);
        assert_eq!(engine.ends.load(Ordering::Acquire), 0);
        assert!(!task.is_finished());
        let retry = route.clone();
        let duplicate = tokio::spawn(async move { retry.write(mutation("known")).await });
        let mut conflict = mutation("known");
        conflict.value = "conflict".into();
        assert_eq!(route.write(conflict).await.0, 503);
        engine.release.notify_one();
        let (status, body) = task.await.unwrap();
        let (retry_status, retry_body) = duplicate.await.unwrap();
        assert_eq!(status, if fail { 202 } else { 200 });
        assert_eq!(retry_status, status);
        assert_eq!(body["sequence"], 1);
        assert_eq!(retry_body["sequence"], 1);
        assert_eq!(retry_body["duplicate"], true);
        assert_eq!(
            body["outcome"],
            if fail { "ambiguous" } else { "committed" }
        );
        let (status, known) = route.write(mutation("known")).await;
        assert_eq!(status, if fail { 202 } else { 200 });
        assert_eq!(known["sequence"], 1);
        assert_eq!(engine.begins.load(Ordering::Acquire), 1);
        assert_eq!(engine.ends.load(Ordering::Acquire), 1);
        unsafe { replica.Abort() };
        lifecycle.join().await.unwrap();
        engine.workers.join().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_http_waiter_does_not_cancel_accepted_native_write_and_copied_retry_is_ambiguous() {
    let route = route();
    let engine = EngineState::new();
    let (replica, lifecycle) = primary(&route, &engine).await;
    let writer = route.clone();
    let waiter = tokio::spawn(async move { writer.write(mutation("disconnected")).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while route.app.provider.snapshot().last == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    engine.release.notify_one();
    let (status, body) = route.write(mutation("disconnected")).await;
    assert_eq!(status, 200);
    assert_eq!(body["duplicate"], true);
    assert_eq!(body["sequence"], 1);
    assert_eq!(engine.ends.load(Ordering::Acquire), 1);
    assert_eq!(engine.begins.load(Ordering::Acquire), 1);
    let source = fube_data::MemoryProvider::new();
    source
        .apply_replication(1, &mutation("copied").operation().unwrap())
        .unwrap();
    let copy = fube_data::StateProvider::copy_state(&*source, 1, None).unwrap();
    let copied_route = self::route();
    while let Some(data) = copy.next(Cancellation::default()).await.unwrap() {
        copied_route.app.provider.apply_copy(&data).unwrap();
    }
    let copied_engine = EngineState::new();
    let (copied_replica, copied_lifecycle) = primary(&copied_route, &copied_engine).await;
    let (status, body) = copied_route.write(mutation("copied")).await;
    assert_eq!(status, 202);
    assert_eq!(body["outcome"], "ambiguous");
    assert_eq!(body["duplicate"], true);
    assert_eq!(body["sequence"], 1);
    assert_eq!(copied_engine.begins.load(Ordering::Acquire), 0);
    unsafe {
        copied_replica.Abort();
        replica.Abort();
    }
    copied_lifecycle.join().await.unwrap();
    copied_engine.workers.join().await.unwrap();
    lifecycle.join().await.unwrap();
    engine.workers.join().await.unwrap();
}

use crate::abi::{NullableOperationStream, NullableOperationStream_Impl};
use mssf_com::FabricRuntime::{IFabricOperation, IFabricOperationData, IFabricOperation_Impl};
use std::collections::VecDeque;

#[implement(IFabricOperation)]
struct Operation {
    data: IFabricOperationData,
    metadata: FABRIC_OPERATION_METADATA,
    applied: Arc<fube_data::MemoryProvider>,
    acknowledgments: Arc<AtomicU64>,
    expected: i64,
}
// Metadata is immutable; its Reserved pointer is always null.
unsafe impl Send for Operation {}
unsafe impl Sync for Operation {}
impl IFabricOperation_Impl for Operation_Impl {
    fn get_Metadata(&self) -> *const FABRIC_OPERATION_METADATA {
        &self.metadata
    }
    fn GetData(&self, count: *mut u32) -> Result<*mut FABRIC_OPERATION_DATA_BUFFER> {
        unsafe { self.data.GetData(count) }
    }
    fn Acknowledge(&self) -> Result<()> {
        if self.applied.snapshot().last != self.expected {
            return Err(com::E_UNEXPECTED.into());
        }
        self.acknowledgments.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[implement(NullableOperationStream)]
struct Operations(Mutex<VecDeque<IFabricOperation>>);
impl NullableOperationStream_Impl for Operations_Impl {
    unsafe fn BeginGetOperation(
        &self,
        callback: Ref<IFabricAsyncOperationCallback>,
        out: *mut Option<IFabricAsyncOperationContext>,
    ) -> Result<()> {
        let context = crate::context::completed(callback)?;
        unsafe { com::return_interface(out, Some(context.clone()))? };
        crate::context::notify(&context);
        Ok(())
    }
    unsafe fn EndGetOperation(
        &self,
        _: Ref<IFabricAsyncOperationContext>,
        out: *mut Option<IFabricOperation>,
    ) -> Result<()> {
        unsafe { com::return_interface(out, self.0.lock().unwrap().pop_front()) }
    }
}
fn operations(operations: Vec<IFabricOperation>) -> IFabricOperationStream {
    let stream: NullableOperationStream = Operations(Mutex::new(operations.into())).into();
    stream.cast().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_operation_pump_applies_before_explicit_ack_and_empty_data_fails() {
    let route = route();
    route.delay.store(80, Ordering::Release);
    let acknowledgments = Arc::new(AtomicU64::new(0));
    let mutation = Mutation {
        id: "r1".into(),
        key: "k".into(),
        value: "v".into(),
    };
    let operation: IFabricOperation = Operation {
        data: operation_data(mutation.operation().unwrap().to_segments()).unwrap(),
        metadata: FABRIC_OPERATION_METADATA {
            Type: FABRIC_OPERATION_TYPE::FABRIC_OPERATION_TYPE_NORMAL,
            SequenceNumber: FABRIC_SEQUENCE_NUMBER(1),
            ..Default::default()
        },
        applied: route.app.provider.clone(),
        acknowledgments: acknowledgments.clone(),
        expected: 1,
    }
    .into();
    let consumer = route.clone();
    let task = tokio::spawn(async move {
        consumer
            .consume(operations(vec![operation]), false, Cancellation::default())
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(acknowledgments.load(Ordering::SeqCst), 0);
    assert_eq!(route.app.provider.snapshot().last, 0);
    task.await.unwrap().unwrap();
    assert_eq!(acknowledgments.load(Ordering::SeqCst), 1);
    assert_eq!(route.app.provider.snapshot().values["k"], "v");
    assert_eq!(route.replication_stream_items.load(Ordering::Acquire), 1);
    assert_eq!(route.replication_stream_eos.load(Ordering::Acquire), 1);
    let empty: IFabricOperation = Operation {
        data: operation_data(vec![]).unwrap(),
        metadata: FABRIC_OPERATION_METADATA {
            Type: FABRIC_OPERATION_TYPE::FABRIC_OPERATION_TYPE_NORMAL,
            SequenceNumber: FABRIC_SEQUENCE_NUMBER(2),
            ..Default::default()
        },
        applied: route.app.provider.clone(),
        acknowledgments: acknowledgments.clone(),
        expected: 2,
    }
    .into();
    assert!(route
        .consume(operations(vec![empty]), false, Cancellation::default())
        .await
        .is_err());
    assert_eq!(acknowledgments.load(Ordering::SeqCst), 1);
    route.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_and_abort_join_pending_provider_contexts_without_claiming_native_execution() {
    for abort in [false, true] {
        let route = route();
        let stream = unsafe {
            route
                .app
                .bridge()
                .GetCopyState(FABRIC_SEQUENCE_NUMBER(1), None)
        }
        .unwrap();
        let pending = unsafe { stream.BeginGetNext(None) }.unwrap();
        let replica: IFabricStatefulServiceReplica = Replica {
            route: route.clone(),
            lifecycle: Workers::new(tokio::runtime::Handle::current()),
            open: Slot::default(),
            role: Slot::default(),
            close: Slot::default(),
        }
        .into();
        if abort {
            unsafe { replica.Abort() };
        } else {
            let close = unsafe { replica.BeginClose(None) }.unwrap();
            while !unsafe { close.IsCompleted() } {
                tokio::task::yield_now().await;
            }
            unsafe { replica.EndClose(&close) }.unwrap();
            assert!(unsafe { replica.EndClose(&close) }.is_err());
        }
        assert!(route.stopped.load(Ordering::Acquire));
        assert!(unsafe { pending.IsCompleted() });
        assert_eq!(
            crate::stream::end_get_next(&stream, &pending)
                .unwrap_err()
                .code(),
            E_ABORT
        );
        assert_eq!(route.app.callbacks.load(Ordering::Acquire), 0);
    }
}
