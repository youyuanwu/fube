//! Owned local registration composition, not a deployed node or FM/HM/RAP.
//! Hosting has no dependency on this crate, RA, runtime or test-host.
#![forbid(unsafe_code)]
use fube_hosting::{self as hosting, ipc::*};
use fube_ra::{entity as ra, protocol};
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex, MutexGuard,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Hosting(hosting::Error),
    Ra(ra::Failure),
    Capacity,
    Closed,
    Unknown,
}
pub type Result<T> = std::result::Result<T, Error>;
struct RaGate(ra::ConsumerLifetime);
impl RegistrationConsumerGate for RaGate {
    fn is_accepting(&self) -> bool {
        self.0.is_accepting()
    }
}
/// Poison recovery is inspection/ownership transfer only. Admission never uses
/// this helper, and the poisoned ledger is never trusted to resume serving.
fn inspect<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[derive(Debug, Clone)]
pub struct Config {
    pub hosting: hosting::ipc::Config,
    /// Trusted composition facts, not inferred FM/deployment readiness.
    /// Default/native report-free qualification can keep this gate closed.
    pub readiness: ra::HostingReadiness,
    pub required: RequiredCapabilities,
    pub max_commands: usize,
    pub command_bytes: usize,
    pub queued_command_bytes: usize,
    pub events_per_lane: usize,
    pub event_bytes_per_lane: usize,
    /// Independent retained target/ticket/result/receipt logical byte budget.
    pub fanout_bytes_per_lane: usize,
    pub max_entities: usize,
    pub status_types: usize,
    pub status_bytes: usize,
    pub decision_deadline: Duration,
}
impl Config {
    pub fn registration(hosting: hosting::ipc::Config) -> Self {
        Self {
            hosting,
            readiness: ra::HostingReadiness::default(),
            required: RequiredCapabilities::default(),
            max_commands: 64,
            command_bytes: 256 * 1024,
            queued_command_bytes: 4 * 1024 * 1024,
            events_per_lane: 256,
            event_bytes_per_lane: 1024 * 1024,
            fanout_bytes_per_lane: 8 * 1024 * 1024,
            max_entities: 32,
            status_types: 64,
            status_bytes: 256 * 1024,
            decision_deadline: Duration::from_secs(30),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.max_commands == 0
            || self.max_commands > 64
            || self.command_bytes == 0
            || self.command_bytes > 256 * 1024
            || self.queued_command_bytes < self.command_bytes
            || self.events_per_lane == 0
            || self.events_per_lane > 256
            || self.event_bytes_per_lane == 0
            || self.event_bytes_per_lane > 1024 * 1024
            || self.fanout_bytes_per_lane == 0
            || self.fanout_bytes_per_lane > 16 * 1024 * 1024
            || self.max_entities == 0
            || self.max_entities > 64
            || self.status_types == 0
            || self.status_types > 256
            || self.decision_deadline.is_zero()
            || self.decision_deadline > Duration::from_secs(30)
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
pub enum Input {
    Admit {
        admission: Admission,
        types: Vec<String>,
        source: String,
    },
    Lookup(hosting::LookupRequest),
    PreparedFailure {
        id: hosting::TypeInstance,
        failure: String,
        grace_ticks: i64,
    },
    RemoveFailure(String),
    HealthSources,
    Operations,
    HostStatus(HostLease),
}
pub enum Response {
    Prepared(PreparedRegistration),
    Lookup(hosting::LookupResult),
    Done,
    Sources(Vec<hosting::HealthSource>),
    Operations(Vec<OperationResult>),
    HostStatus(SessionStatus),
}
struct Record {
    charge: usize,
    result: Mutex<Option<Result<Response>>>,
}
struct Work {
    input: Input,
    record: Arc<Record>,
}
struct Meta {
    accepting: bool,
    next: u64,
    bytes: usize,
    commands: BTreeMap<u64, Arc<Record>>,
    receipts: VecDeque<Receipt>,
    counts: [usize; 3],
    payload: [usize; 3],
    fanout: [usize; 3],
    error: Option<Error>,
    unknown: usize,
}
struct Shared {
    meta: Mutex<Meta>,
    stop: AtomicBool,
    residual: Mutex<Option<Residual>>,
    config: Config,
}
#[derive(Clone)]
pub struct Handle {
    sender: mpsc::SyncSender<Work>,
    shared: Arc<Shared>,
}
/// Cancellation/drop is observation loss, not command retraction or rollback.
pub struct Ticket {
    pub id: u64,
    record: Arc<Record>,
    shared: Arc<Shared>,
}
impl Ticket {
    pub fn take(&self) -> Option<Result<Response>> {
        let mut meta = inspect(&self.shared.meta);
        let result = inspect(&self.record.result).take()?;
        meta.commands.remove(&self.id);
        meta.bytes = meta.bytes.saturating_sub(self.record.charge);
        Some(result)
    }
    pub fn wait(&self, budget: Duration) -> Result<Response> {
        if budget.is_zero() || budget > Duration::from_secs(30) {
            return Err(Error::Unknown);
        }
        let due = Instant::now() + budget;
        loop {
            if let Some(result) = self.take() {
                return result;
            }
            if Instant::now() >= due {
                return Err(Error::Unknown);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
impl Handle {
    pub fn submit(&self, input: Input) -> Result<Ticket> {
        let input_charge = input_bytes(&input);
        let charge = input_charge
            .checked_add(reply_reservation(&input, &self.shared.config))
            .ok_or(Error::Capacity)?;
        let mut meta = self.shared.meta.lock().map_err(|_| Error::Closed)?;
        if !meta.accepting {
            return Err(Error::Closed);
        }
        if input_charge > self.shared.config.command_bytes
            || meta.commands.len() >= self.shared.config.max_commands
            || meta
                .bytes
                .checked_add(charge)
                .is_none_or(|bytes| bytes > self.shared.config.queued_command_bytes)
        {
            return Err(Error::Capacity);
        }
        let next_bytes = meta.bytes.checked_add(charge).ok_or(Error::Capacity)?;
        let id = meta.next.checked_add(1).ok_or(Error::Capacity)?;
        let record = Arc::new(Record {
            charge,
            result: Mutex::new(None),
        });
        self.sender
            .try_send(Work {
                input,
                record: record.clone(),
            })
            .map_err(|e| match e {
                mpsc::TrySendError::Full(_) => Error::Capacity,
                mpsc::TrySendError::Disconnected(_) => Error::Closed,
            })?;
        meta.next = id;
        meta.bytes = next_bytes;
        meta.commands.insert(id, record.clone());
        Ok(Ticket {
            id,
            record,
            shared: self.shared.clone(),
        })
    }
    /// Explicit recovery of completed commands whose observers were lost.
    pub fn recover_commands(&self) -> Vec<(u64, Result<Response>)> {
        let records: Vec<_> = inspect(&self.shared.meta)
            .commands
            .iter()
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        records
            .into_iter()
            .filter_map(|(id, record)| {
                let ticket = Ticket {
                    id,
                    record,
                    shared: self.shared.clone(),
                };
                ticket.take().map(|result| (id, result))
            })
            .collect()
    }
    /// Transfer receipts exactly once. This is not an RA/FM/HM ACK.
    pub fn take_receipts(&self) -> Vec<Receipt> {
        let mut meta = inspect(&self.shared.meta);
        let receipts: Vec<_> = meta.receipts.drain(..).collect();
        for r in &receipts {
            meta.counts[r.lane] -= 1;
            meta.payload[r.lane] -= r.input.retained_bytes();
            meta.fanout[r.lane] -= r.proof;
        }
        receipts
    }
    pub fn take_shutdown(&self) -> Option<Residual> {
        let residual = inspect(&self.shared.residual).take()?;
        let mut meta = inspect(&self.shared.meta);
        for p in &residual.pending {
            meta.counts[p.lane] = meta.counts[p.lane].saturating_sub(1);
            meta.payload[p.lane] = meta.payload[p.lane].saturating_sub(p.input.retained_bytes());
            meta.fanout[p.lane] = meta.fanout[p.lane].saturating_sub(p.proof);
        }
        Some(residual)
    }
    pub fn fanout_usage(&self) -> [usize; 3] {
        inspect(&self.shared.meta).fanout
    }
    pub fn progress(&self) -> ([usize; 3], [usize; 3], usize, Option<Error>) {
        let meta = self.shared.meta.lock().unwrap();
        (meta.counts, meta.payload, meta.unknown, meta.error.clone())
    }
}
#[derive(Debug, Clone)]
pub struct Receipt {
    pub input: Delivery,
    pub root: ra::Disposition,
    pub entities: Vec<(ra::EntityId, ra::Status)>,
    /// Local decisions only; required effects remain capability-gated.
    pub downstream_completion: bool,
    lane: usize,
    proof: usize,
}
pub struct Pending {
    pub input: Delivery,
    pub targets: Vec<ra::EntityId>,
    pub tickets: Vec<Option<ra::Ticket>>,
    pub results: Vec<Option<ra::Status>>,
    pub blocked: Option<Error>,
    lane: usize,
    proof: usize,
    deadline: Instant,
}
pub struct Residual {
    pub hosting: Server,
    pub owner: RegistrationOwner,
    pub ra: Arc<ra::Executor>,
    pub pending: Vec<Pending>,
    pub statuses: ra::LocalStatusConsumer,
    pub shutdown: hosting::Result<()>,
}
/// Nonclone owner of listener, bridge worker and residual event work.
pub struct Node {
    address: SocketAddr,
    handle: Handle,
    thread: Option<JoinHandle<Residual>>,
}
impl Node {
    pub fn bind(address: SocketAddr, config: Config, ra: Arc<ra::Executor>) -> Result<Self> {
        config.validate()?;
        if !ra.consumer_lifetime().is_accepting() {
            return Err(Error::Closed);
        }
        if ra.entity_ids().len() > config.max_entities {
            return Err(Error::Capacity);
        }
        let hosting = Server::bind(address, config.hosting.clone()).map_err(Error::Hosting)?;
        let owner = hosting
            .registration_owner(config.required)
            .map_err(Error::Hosting)?;
        let owner = owner
            .with_consumer_gate(Arc::new(RaGate(ra.consumer_lifetime())))
            .map_err(Error::Hosting)?;
        let statuses = ra::LocalStatusConsumer::new(config.status_types, config.status_bytes)
            .map_err(Error::Ra)?;
        let address = hosting.address();
        let (sender, receiver) = mpsc::sync_channel(config.max_commands);
        let shared = Arc::new(Shared {
            meta: Mutex::new(Meta {
                accepting: true,
                next: 0,
                bytes: 0,
                commands: BTreeMap::new(),
                receipts: VecDeque::new(),
                counts: [0; 3],
                payload: [0; 3],
                fanout: [0; 3],
                error: None,
                unknown: 0,
            }),
            stop: AtomicBool::new(false),
            residual: Mutex::new(None),
            config,
        });
        let handle = Handle {
            sender,
            shared: shared.clone(),
        };
        let thread = std::thread::Builder::new()
            .name("fube-node-events".into())
            .spawn(move || {
                let mut residual = Residual {
                    hosting,
                    owner,
                    ra,
                    pending: Vec::new(),
                    statuses,
                    shutdown: Ok(()),
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drive(&mut residual, receiver, shared.clone());
                }));
                residual.owner.withdraw();
                residual.shutdown = residual.hosting.stop();
                if outcome.is_err() {
                    residual.shutdown = Err(hosting::Error::OperationFailed);
                    if let Ok(mut meta) = shared.meta.lock() {
                        meta.accepting = false;
                        meta.error = Some(Error::Unknown);
                        for record in meta.commands.values() {
                            if let Ok(mut result) = record.result.lock() {
                                if result.is_none() {
                                    *result = Some(Err(Error::Unknown));
                                }
                            }
                        }
                    }
                }
                residual
            })
            .map_err(|_| Error::Closed)?;
        Ok(Self {
            address,
            handle,
            thread: Some(thread),
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn stop(&mut self) -> Result<()> {
        self.handle.shared.stop.store(true, Ordering::SeqCst);
        let poisoned = self.handle.shared.meta.is_poisoned();
        {
            let mut meta = inspect(&self.handle.shared.meta);
            meta.accepting = false;
            if poisoned {
                meta.error = Some(Error::Unknown);
            }
        }
        if let Some(thread) = self.thread.take() {
            let residual = thread.join().map_err(|_| Error::Closed)?;
            let outcome = residual.shutdown.clone().map_err(Error::Hosting);
            *self
                .handle
                .shared
                .residual
                .lock()
                .map_err(|_| Error::Closed)? = Some(residual);
            outcome?;
        }
        if poisoned {
            return Err(Error::Closed);
        }
        Ok(())
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn drive(residual: &mut Residual, receiver: mpsc::Receiver<Work>, shared: Arc<Shared>) {
    let Residual {
        hosting,
        owner,
        ra,
        pending,
        statuses,
        ..
    } = residual;
    let origin = Instant::now();
    loop {
        if !ra.consumer_lifetime().is_accepting() {
            let error = match ra.terminal_result() {
                Some(Err(e)) => Error::Ra(e),
                _ => Error::Ra(ra::Failure::Closed),
            };
            owner.withdraw();
            let mut meta = inspect(&shared.meta);
            meta.accepting = false;
            meta.error = Some(error.clone());
            for p in pending.iter_mut() {
                p.blocked = Some(error.clone());
            }
            shared.stop.store(true, Ordering::SeqCst);
        }
        for _ in 0..4 {
            let Ok(work) = receiver.try_recv() else {
                break;
            };
            let result = process(work.input, hosting, owner, &shared.config);
            *work.record.result.lock().unwrap() = Some(result);
        }
        if shared.stop.load(Ordering::SeqCst) {
            owner.withdraw();
            while let Ok(work) = receiver.try_recv() {
                *work.record.result.lock().unwrap() = Some(Err(Error::Closed));
            }
            return;
        }
        let now = i64::try_from(origin.elapsed().as_nanos() / 100).unwrap_or(i64::MAX);
        if let Err(e) = statuses.tick(now) {
            owner.withdraw();
            shared.meta.lock().unwrap().error = Some(Error::Ra(e));
        }
        if let Err(error) = owner.drive_type_timers() {
            if error != hosting::Error::Capacity {
                owner.withdraw();
            }
            shared.meta.lock().unwrap().error = Some(Error::Hosting(error));
        }
        for (lane, kind) in [
            DeliveryLane::Registration,
            DeliveryLane::Status,
            DeliveryLane::Obligation,
        ]
        .into_iter()
        .enumerate()
        {
            if lane == 0
                && pending
                    .iter()
                    .any(|p| p.lane == 0 && p.tickets.iter().any(Option::is_none))
            {
                continue;
            }
            let proof = proof_charge(&shared.config, lane);
            let available = {
                let meta = shared.meta.lock().unwrap();
                if meta.counts[lane] >= shared.config.events_per_lane
                    || meta.fanout[lane]
                        .checked_add(proof)
                        .is_none_or(|bytes| bytes > shared.config.fanout_bytes_per_lane)
                {
                    0
                } else {
                    shared
                        .config
                        .event_bytes_per_lane
                        .saturating_sub(meta.payload[lane])
                }
            };
            if available == 0 {
                continue;
            }
            let delivery = match owner.next_delivery(kind, available) {
                Ok(Some(e)) => e,
                Ok(None) | Err(hosting::Error::Capacity) => continue,
                Err(_) => {
                    owner.withdraw();
                    continue;
                }
            };
            {
                let mut meta = shared.meta.lock().unwrap();
                meta.counts[lane] += 1;
                meta.payload[lane] += delivery.retained_bytes();
                meta.fanout[lane] += proof;
            }
            // Retain an accepted snapshot before calling any external consumer.
            // During an unwind this placeholder remains an explicit unknown,
            // never a rollback or an event silently lost from the producer lane.
            let placeholder = pending.len();
            pending.push(Pending {
                input: delivery.clone(),
                targets: Vec::new(),
                tickets: Vec::new(),
                results: Vec::new(),
                blocked: Some(Error::Unknown),
                lane,
                proof,
                deadline: Instant::now(),
            });
            let ready = &shared.config.readiness;
            let closing = matches!(
                delivery,
                Delivery::Registration(
                    hosting::ClosedRegisteredEvent::RuntimeClosed { .. }
                        | hosting::ClosedRegisteredEvent::HostClosed { .. }
                )
            );
            let can_run = if closing {
                ready.ra_open || ready.ra_closing
            } else {
                ready.ra_open && ready.node_active && !ready.upgrading
            };
            let root = if !ready.fm_ready || !can_run {
                ra::Disposition::IgnoredNotReady
            } else {
                ra::Disposition::HostingMatchedNoWork
            };
            match &delivery {
                Delivery::Status(e) => {
                    let (id, sequence, enabled) = match e {
                        hosting::EnabledDisabledEvent::Enabled {
                            type_instance,
                            sequence,
                        } => (type_instance, *sequence, true),
                        hosting::EnabledDisabledEvent::Disabled {
                            type_instance,
                            sequence,
                        } => (type_instance, *sequence, false),
                    };
                    match statuses.consume(
                        service_type(&id.service_type),
                        sequence,
                        enabled,
                        ready.fm_ready,
                        now,
                    ) {
                        Ok(disposition) => finish(&shared, delivery, lane, disposition, Vec::new()),
                        Err(e) => {
                            owner.withdraw();
                            pending.push(Pending {
                                input: delivery,
                                targets: Vec::new(),
                                tickets: Vec::new(),
                                results: Vec::new(),
                                blocked: Some(Error::Ra(e)),
                                lane,
                                proof,
                                deadline: Instant::now(),
                            });
                        }
                    }
                }
                Delivery::Obligation(
                    hosting::Obligation::TypesUnregistered { .. }
                    | hosting::Obligation::HealthSourceReleased { .. },
                ) => {
                    // No Opened package/timeout engine exists in this admitted
                    // profile. These remain explicit gated owned obligations.
                    pending.push(Pending {
                        input: delivery,
                        targets: Vec::new(),
                        tickets: Vec::new(),
                        results: Vec::new(),
                        blocked: Some(Error::Hosting(hosting::Error::UnsupportedCapability)),
                        lane,
                        proof,
                        deadline: Instant::now(),
                    });
                }
                Delivery::Obligation(_) => {
                    owner.withdraw();
                    pending.push(Pending {
                        input: delivery,
                        targets: Vec::new(),
                        tickets: Vec::new(),
                        results: Vec::new(),
                        blocked: Some(Error::Hosting(hosting::Error::UnsupportedCapability)),
                        lane,
                        proof,
                        deadline: Instant::now(),
                    });
                }
                Delivery::Registration(_) if root == ra::Disposition::IgnoredNotReady => {
                    finish(&shared, delivery, lane, root, Vec::new())
                }
                Delivery::Registration(_) => {
                    let targets = ra.entity_ids();
                    if targets.len() > shared.config.max_entities {
                        owner.withdraw();
                        pending.push(Pending {
                            input: delivery,
                            targets: Vec::new(),
                            tickets: Vec::new(),
                            results: Vec::new(),
                            blocked: Some(Error::Capacity),
                            lane,
                            proof,
                            deadline: Instant::now(),
                        });
                    } else if targets.is_empty() {
                        finish(
                            &shared,
                            delivery,
                            lane,
                            ra::Disposition::IgnoredStale,
                            Vec::new(),
                        );
                    } else {
                        let count = targets.len();
                        pending.push(Pending {
                            input: delivery,
                            targets,
                            tickets: vec![None; count],
                            results: vec![None; count],
                            blocked: None,
                            lane,
                            proof,
                            deadline: Instant::now() + shared.config.decision_deadline,
                        });
                    }
                }
            }
            pending.remove(placeholder);
        }
        let mut i = 0;
        while i < pending.len() {
            let p = &mut pending[i];
            if p.blocked.as_ref().is_some_and(|e| *e != Error::Unknown) {
                i += 1;
                continue;
            }
            for j in 0..p.targets.len() {
                if p.tickets[j].is_none() {
                    match ra.submit(command(&p.input, p.targets[j], &shared.config.readiness)) {
                        Ok(ticket) => p.tickets[j] = Some(ticket),
                        Err(ra::Failure::Capacity) => {}
                        Err(e) => {
                            p.blocked = Some(Error::Ra(e));
                            owner.withdraw();
                            break;
                        }
                    }
                }
                if let Some(ticket) = &p.tickets[j] {
                    // Never resubmit accepted work, including unknown-store work.
                    match ticket.observation() {
                        ra::TicketObservation::Status(status @ ra::Status::Complete(_)) => {
                            p.results[j] = Some(status);
                        }
                        ra::TicketObservation::OwnerLost => {
                            p.blocked = Some(Error::Ra(ra::Failure::Unknown));
                            owner.withdraw();
                            let mut meta = inspect(&shared.meta);
                            meta.accepting = false;
                            meta.error = p.blocked.clone();
                            shared.stop.store(true, Ordering::SeqCst);
                        }
                        ra::TicketObservation::Status(_) => {}
                    }
                }
            }
            if p.results.iter().all(Option::is_some) {
                let p = pending.remove(i);
                let results = p
                    .targets
                    .into_iter()
                    .zip(p.results.into_iter().map(Option::unwrap))
                    .collect();
                finish(
                    &shared,
                    p.input,
                    p.lane,
                    ra::Disposition::HostingMatchedNoWork,
                    results,
                );
            } else {
                if Instant::now() >= p.deadline
                    && p.blocked.as_ref().is_none_or(|e| *e == Error::Unknown)
                {
                    p.blocked = Some(Error::Unknown);
                }
                i += 1;
            }
        }
        shared.meta.lock().unwrap().unknown = pending
            .iter()
            .filter(|p| p.blocked == Some(Error::Unknown))
            .count();
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn finish(
    shared: &Shared,
    input: Delivery,
    lane: usize,
    root: ra::Disposition,
    entities: Vec<(ra::EntityId, ra::Status)>,
) {
    shared.meta.lock().unwrap().receipts.push_back(Receipt {
        input,
        root,
        entities,
        downstream_completion: false,
        lane,
        proof: proof_charge(&shared.config, lane),
    });
}
fn process(
    input: Input,
    hosting: &Server,
    owner: &RegistrationOwner,
    config: &Config,
) -> Result<Response> {
    match input {
        Input::Admit {
            admission,
            types,
            source,
        } => {
            let existing = owner.health_sources().map_err(Error::Hosting)?;
            // Reserve each possible history+pending key before admitting a type.
            let count = existing.len().saturating_add(types.len());
            let bytes = existing
                .iter()
                .map(|s| status_bytes(&s.id.service_type))
                .sum::<usize>()
                + types
                    .iter()
                    .map(|name| {
                        512 + name.len().saturating_mul(2)
                            + admission
                                .code
                                .id
                                .package
                                .package
                                .name
                                .len()
                                .saturating_mul(2)
                            + admission
                                .code
                                .id
                                .package
                                .package
                                .application
                                .type_name
                                .len()
                                .saturating_mul(2)
                    })
                    .sum::<usize>();
            if count > config.status_types || bytes > config.status_bytes {
                return Err(Error::Capacity);
            }
            hosting
                .admit_registration(owner, admission, types, &source)
                .map(Response::Prepared)
                .map_err(Error::Hosting)
        }
        Input::Lookup(request) => owner
            .lookup(&request)
            .map(Response::Lookup)
            .map_err(Error::Hosting),
        Input::PreparedFailure {
            id,
            failure,
            grace_ticks,
        } => {
            if grace_ticks <= 0 || grace_ticks > 300_000_000 {
                return Err(Error::Hosting(hosting::Error::InvalidInput));
            }
            if !owner
                .health_sources()
                .map_err(Error::Hosting)?
                .iter()
                .any(|s| hosting::compare_type_instances(&s.id, &id).is_eq())
            {
                return Err(Error::Hosting(hosting::Error::InvalidServiceType));
            }
            owner
                .prepared_type_failure(&id, failure, grace_ticks)
                .map(|()| Response::Done)
                .map_err(Error::Hosting)
        }
        Input::RemoveFailure(failure) => owner
            .remove_prepared_failure(&failure)
            .map(|()| Response::Done)
            .map_err(Error::Hosting),
        Input::HealthSources => owner
            .health_sources()
            .map(Response::Sources)
            .map_err(Error::Hosting),
        Input::Operations => owner
            .take_operations()
            .map(Response::Operations)
            .map_err(Error::Hosting),
        Input::HostStatus(lease) => hosting
            .status(&lease)
            .map(Response::HostStatus)
            .map_err(Error::Hosting),
    }
}
fn service_type(id: &hosting::ServiceTypeId) -> protocol::ServiceType {
    protocol::ServiceType {
        package: protocol::PackageId {
            application: protocol::ApplicationId {
                type_name: id.package.application.type_name.clone(),
                number: id.package.application.number,
            },
            name: id.package.name.clone(),
        },
        name: id.name.clone(),
    }
}
fn version(v: &hosting::VersionInstance) -> protocol::VersionInstance {
    protocol::VersionInstance {
        version: protocol::PackageVersion {
            application: protocol::ApplicationVersion {
                value: protocol::Rollout {
                    major: v.application.major,
                    minor: v.application.minor,
                },
            },
            value: protocol::Rollout {
                major: v.package.major,
                minor: v.package.minor,
            },
        },
        instance: v.instance,
    }
}
fn command(input: &Delivery, id: ra::EntityId, readiness: &ra::HostingReadiness) -> ra::Command {
    let unit = partition(id);
    match input {
        Delivery::Registration(hosting::ClosedRegisteredEvent::Registered {
            registration, ..
        }) => ra::Command::HostingRegistered(Box::new(ra::HostingRegistered {
            unit,
            readiness: readiness.clone(),
            service_type: service_type(&registration.type_instance.service_type),
            version: version(&registration.version),
            activation: partition(
                uuid::Uuid::from_bytes(registration.type_instance.activation.0).to_bytes_le(),
            ),
        })),
        Delivery::Registration(hosting::ClosedRegisteredEvent::RuntimeClosed {
            runtime_id,
            host_id,
            ..
        }) => ra::Command::HostingRuntimeClosed(Box::new(ra::HostingRuntimeClosed {
            unit,
            readiness: readiness.clone(),
            host: host_id.clone(),
            runtime: runtime_id.clone(),
        })),
        Delivery::Registration(hosting::ClosedRegisteredEvent::HostClosed {
            host_id,
            activity,
            ..
        }) => ra::Command::HostingHostClosed(Box::new(ra::HostingHostClosed {
            unit,
            readiness: readiness.clone(),
            host: host_id.clone(),
            activity: activity.clone(),
        })),
        _ => unreachable!(),
    }
}
fn partition(id: [u8; 16]) -> protocol::PartitionId {
    let mut unit = protocol::PartitionId::default();
    unit.guid.0 = id;
    unit
}
fn status_bytes(id: &hosting::ServiceTypeId) -> usize {
    512 + 2 * (id.package.application.type_name.len() + id.package.name.len() + id.name.len())
}
fn input_bytes(input: &Input) -> usize {
    512 + match input {
        Input::Admit {
            admission,
            types,
            source,
        } => {
            source.capacity()
                + admission.host.id.capacity()
                + types.capacity() * std::mem::size_of::<String>()
                + types.iter().map(String::capacity).sum::<usize>()
                + [
                    &admission.code.id.package.package.application.type_name,
                    &admission.code.id.package.package.name,
                    &admission.code.id.package.public_id,
                    &admission.code.id.name,
                    &admission.code.application_name,
                ]
                .iter()
                .map(|s| s.capacity())
                .sum::<usize>()
        }
        Input::Lookup(r) => {
            r.service_type.name.capacity()
                + r.service_type.package.name.capacity()
                + r.service_type.package.application.type_name.capacity()
                + r.service_name.capacity()
                + r.application_name.capacity()
        }
        Input::PreparedFailure { id, failure, .. } => {
            id.service_type.name.capacity()
                + id.service_type.package.name.capacity()
                + id.service_type.package.application.type_name.capacity()
                + id.public_activation.capacity()
                + failure.capacity()
        }
        Input::RemoveFailure(f) => f.capacity(),
        Input::HealthSources | Input::Operations | Input::HostStatus(_) => 0,
    }
}
fn proof_charge(config: &Config, lane: usize) -> usize {
    128 + if lane == 0 {
        config.max_entities * 512
    } else {
        0
    }
}
fn reply_reservation(input: &Input, config: &Config) -> usize {
    match input {
        Input::HealthSources => config.hosting.core_limits.state_bytes,
        Input::HostStatus(_) => config.hosting.core_limits.state_bytes,
        Input::Operations => config
            .hosting
            .max_admissions
            .saturating_mul(16)
            .saturating_mul(256),
        Input::Lookup(_) => 64 * 1024,
        Input::Admit { .. } | Input::PreparedFailure { .. } | Input::RemoveFailure(_) => 512,
    }
}

#[cfg(test)]
mod tests;
