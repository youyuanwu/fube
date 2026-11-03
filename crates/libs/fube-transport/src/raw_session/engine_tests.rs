use std::collections::HashSet;

use tokio::{
    io::{duplex, AsyncReadExt},
    sync::oneshot,
    time::Duration,
};

use super::*;
use crate::connection::write_message;

#[derive(Default)]
struct TestPolicy {
    live: HashSet<u64>,
    started: Mutex<Vec<u64>>,
    written: Vec<u64>,
    retired: usize,
}

impl Policy for TestPolicy {
    type Charge = Arc<()>;

    fn eligible(&self, token: u64, _: Instant) -> bool {
        self.live.contains(&token)
    }
    fn started(&self, token: u64) {
        self.started.lock().unwrap().push(token);
    }
    fn receipt(&mut self, token: u64, receipt: Receipt, _: Instant) {
        if matches!(receipt, Receipt::Written) {
            self.written.push(token);
        }
    }
    fn retire(&mut self) {
        self.live.clear();
        self.retired += 1;
    }
}

struct TestDriver {
    core: Arc<Core<TestPolicy>>,
    delivered: Mutex<usize>,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
}

impl TestDriver {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            core: Core::new(TestPolicy::default(), ListenInstance::default()),
            delivered: Mutex::new(0),
            hold: Mutex::new(None),
        })
    }
}

impl Driver for TestDriver {
    type Policy = TestPolicy;
    fn core(&self) -> &Arc<Core<TestPolicy>> {
        &self.core
    }
    async fn receive(self: Arc<Self>, _: Message, _: Properties, _: Instant) -> Result<()> {
        *self.delivered.lock().unwrap() += 1;
        let hold = self.hold.lock().unwrap().take();
        if let Some(hold) = hold {
            let _ = hold.await;
        }
        Ok(())
    }
    async fn maintain(self: Arc<Self>) {
        self.core.terminal().await;
    }
}

fn limits() -> Limits {
    Limits {
        max_frame_bytes: 4096,
        frame_time: Duration::from_secs(5),
    }
}

fn frame(token: u64) -> Message {
    Message {
        headers: vec![
            Header::actor(2).unwrap(),
            Header::action("EngineTest").unwrap(),
        ],
        body: vec![token as u8; 256],
    }
}

fn enqueue(driver: &TestDriver, token: u64, reserved: bool, charge: Arc<()>) {
    let mut inner = driver.core.inner.lock().unwrap();
    inner.policy.live.insert(token);
    inner.transport.enqueue(
        WriteJob {
            token,
            bytes: frame(token).to_frame().unwrap().encode().unwrap(),
            deadline: Instant::now() + Duration::from_secs(2),
            valid: None,
            charge,
        },
        reserved,
    );
    drop(inner);
    driver.core.changed.notify_waiters();
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout_at(Instant::now() + Duration::from_secs(1), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn engine_owns_priority_jobs_and_reports_only_local_receipts() {
    let driver = TestDriver::new();
    enqueue(&driver, 1, false, Arc::new(()));
    enqueue(&driver, 2, true, Arc::new(()));
    let (stream, mut peer) = duplex(4096);
    let owner = Owner::start(stream, driver.clone(), limits());
    assert_eq!(
        read_message(&mut peer, limits()).await.unwrap().body,
        vec![2; 256]
    );
    assert_eq!(
        read_message(&mut peer, limits()).await.unwrap().body,
        vec![1; 256]
    );
    until(|| driver.core.inner.lock().unwrap().policy.written.len() == 2).await;
    assert_eq!(driver.core.inner.lock().unwrap().policy.written, [2, 1]);
    driver.core.end(Error::Closed, false);
    owner
        .closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn policy_retirement_keeps_selected_charge_suffix_and_retained_join() {
    let driver = TestDriver::new();
    let charge = Arc::new(());
    let weak = Arc::downgrade(&charge);
    enqueue(&driver, 1, false, charge);
    let (stream, mut peer) = duplex(1);
    let owner = Owner::start(stream, driver.clone(), limits());
    until(|| {
        !driver
            .core
            .inner
            .lock()
            .unwrap()
            .policy
            .started
            .lock()
            .unwrap()
            .is_empty()
    })
    .await;
    driver
        .core
        .end(Error::Protocol("policy first cause"), false);
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        owner
            .closed(Instant::now() + Duration::from_millis(10))
            .await,
        Err(Error::Timeout)
    ));
    assert_eq!(
        read_message(&mut peer, limits()).await.unwrap().body,
        vec![1; 256]
    );
    owner
        .closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(weak.upgrade().is_none());
    driver.core.end(Error::Closed, true);
    assert!(matches!(
        driver.core.error(),
        Some(Error::Protocol("policy first cause"))
    ));
    assert_eq!(driver.core.inner.lock().unwrap().policy.retired, 1);
    assert!(
        driver
            .core
            .inner
            .lock()
            .unwrap()
            .transport
            .dispose_requested
    );
    owner
        .closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test]
async fn policy_handoff_is_inline_before_later_transport_controls() {
    let driver = TestDriver::new();
    let (release, hold) = oneshot::channel();
    *driver.hold.lock().unwrap() = Some(hold);
    let (stream, mut peer) = duplex(4096);
    let owner = Owner::start(stream, driver.clone(), limits());
    write_message(&mut peer, &frame(1)).await.unwrap();
    let remote = ListenInstance {
        instance: 55,
        ..ListenInstance::default()
    };
    write_message(
        &mut peer,
        &Message::listen_instance(MessageId::default(), &remote).unwrap(),
    )
    .await
    .unwrap();
    until(|| *driver.delivered.lock().unwrap() == 1).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_ne!(
        driver.core.inner.lock().unwrap().transport.remote.instance,
        55
    );
    release.send(()).unwrap();
    until(|| driver.core.inner.lock().unwrap().transport.remote.instance == 55).await;
    assert_eq!(*driver.delivered.lock().unwrap(), 1);
    driver.core.end(Error::Closed, false);
    owner
        .closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}
