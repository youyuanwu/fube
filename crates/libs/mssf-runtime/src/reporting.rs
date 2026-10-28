//! Application-runtime reports, not management-client operations.
use crate::{api::FaultType, lease::Authority, wire::*};
use mssf_transport::{
    connection::{deadline_after, Connection, SendStatus},
    message::{Header, Message},
    serialization::serde::{from_slice, to_vec, EmptyObjectArray, FixedTag, ObjectArray, Pointer},
    Error, Result,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

pub const MAX_REPORT_METRICS: usize = 64;
pub const MAX_REPORT_NAME_UNITS: usize = 4096;
/// Leaves ordinary transport admission available for independent lease polls.
pub const MAX_CONCURRENT_REPORTS: usize = 16;
const REPORT_TIMEOUT: Duration = Duration::from_secs(30);
const FAULT_RETRY: Duration = Duration::from_secs(30);
static SEQUENCE: AtomicI64 = AtomicI64::new(0);

/// A custom metric declared in the current native service description.
#[derive(Debug, Clone)]
pub struct LoadMetric {
    pub name: String,
    /// Native application reporting requires a value strictly less than INT_MAX.
    pub value: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveCost {
    Zero,
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Ok,
    Warning,
    Error,
}
#[derive(Debug, Clone, Copy)]
pub enum HealthSequence {
    Auto,
    /// Must be strictly positive. HM requires a strictly increasing sequence
    /// for the same instance/source/property; stale results are returned errors.
    Explicit(i64),
}
#[derive(Debug, Clone)]
pub struct HealthReport {
    pub source: String,
    pub property: String,
    pub state: HealthState,
    pub description: String,
    /// None is infinite; Some must be at least 100ns and fit native TimeSpan.
    /// Expiration is measured by HM from its own last-modified time.
    pub time_to_live: Option<Duration>,
    pub sequence: HealthSequence,
    pub remove_when_expired: bool,
}
/// Matched HM per-report success, not a guarantee of a fresh durable transaction.
#[derive(Debug, Clone, Copy)]
pub struct HealthReceipt {
    pub sequence: i64,
}
/// Bounded fault-stage observation; no historical receipt collection is kept.
#[derive(Debug, Clone)]
pub struct FaultDelivery {
    pub fault: FaultType,
    pub attempts: u64,
    pub last_error: Option<String>,
    pub cancelled: bool,
    pub superseded_transient: bool,
}

struct FaultStage {
    delivery: FaultDelivery,
    body: Vec<u8>,
    error: Option<Error>,
    current: Arc<AtomicBool>,
    complete: tokio::sync::watch::Sender<Option<Result<FaultDelivery>>>,
}
pub(crate) struct Reporting {
    connection: Connection,
    valid: Arc<AtomicBool>,
    authority: Arc<Authority>,
    request: Mutex<ProxyRequest>,
    admission: Arc<Semaphore>,
    loads: Mutex<BTreeMap<String, ReportedLoad>>,
    fault: Mutex<Option<FaultStage>>,
    cancel: crate::Cancellation,
    global: Arc<Semaphore>,
    fault_changed: tokio::sync::Notify,
    executor: Option<tokio::runtime::Handle>,
}
impl std::fmt::Debug for Reporting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reporting").finish_non_exhaustive()
    }
}
impl Reporting {
    #[cfg(test)]
    pub(crate) fn admission_for_test(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.admission.try_acquire().unwrap()
    }
    pub(crate) fn new(
        connection: Connection,
        valid: Arc<AtomicBool>,
        authority: Arc<Authority>,
        request: ProxyRequest,
        cancel: crate::Cancellation,
        global: Arc<Semaphore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            connection,
            valid,
            authority,
            request: Mutex::new(request),
            admission: Arc::new(Semaphore::new(1)),
            loads: Mutex::new(BTreeMap::new()),
            fault: Mutex::new(None),
            cancel,
            global,
            fault_changed: tokio::sync::Notify::new(),
            executor: tokio::runtime::Handle::try_current().ok(),
        })
    }
    pub(crate) fn update(&self, request: &ProxyRequest) {
        *self.request.lock().unwrap() = request.clone();
    }
    pub(crate) fn loads(&self) -> Vec<ReportedLoad> {
        self.loads.lock().unwrap().values().cloned().collect()
    }
    fn check(&self) -> Result<()> {
        self.authority.check()?;
        if !self.valid.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn fence(self: &Arc<Self>) -> SendStatus {
        let this = Arc::downgrade(self);
        SendStatus::fenced(Arc::new(move || {
            this.upgrade().is_some_and(|s| s.check().is_ok())
        }))
    }
    pub(crate) async fn load(self: &Arc<Self>, metrics: &[LoadMetric]) -> Result<()> {
        self.send_load(metrics, false).await
    }
    pub(crate) async fn move_cost(self: &Arc<Self>, cost: MoveCost) -> Result<()> {
        let value = match cost {
            MoveCost::Zero => 0,
            MoveCost::Low => 1,
            MoveCost::Medium => 2,
            MoveCost::High => 3,
        };
        self.send_load(
            &[LoadMetric {
                name: "_MoveCost_".into(),
                value,
            }],
            true,
        )
        .await
    }
    async fn send_load(self: &Arc<Self>, metrics: &[LoadMetric], move_cost: bool) -> Result<()> {
        self.check()?;
        let _global = self
            .global
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("runtime report admission"))?;
        let _permit = self
            .admission
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("partition report in progress"))?;
        if metrics.is_empty() || metrics.len() > MAX_REPORT_METRICS {
            return Err(Error::InvalidInput("report requires 1..=64 metrics"));
        }
        let request = self.request.lock().unwrap().clone();
        let mut unique = BTreeMap::new();
        for metric in metrics {
            name(&metric.name)?;
            if metric.value >= i32::MAX as u32
                || (!move_cost
                    && (metric.name == "_MoveCost_"
                        || !request
                            .service
                            .metrics
                            .0
                            .iter()
                            .any(|m| m.name == metric.name)))
            {
                return Err(Error::InvalidInput("invalid or undeclared load metric"));
            }
            if unique.insert(metric.name.clone(), metric.value).is_some() {
                return Err(Error::InvalidInput("duplicate load metric"));
            }
        }
        {
            let loads = self.loads.lock().unwrap();
            if loads.len() + unique.keys().filter(|n| !loads.contains_key(*n)).count()
                > MAX_REPORT_METRICS + 1
            {
                return Err(Error::ResourceLimit("retained partition load metrics"));
            }
        }
        let now = stopwatch()?;
        let stats: Vec<_> = unique
            .iter()
            .map(|(name, value)| LoadStats {
                name: name.clone(),
                value: *value,
                count: 1,
                weighted_sum: f64::from(*value),
                weight: 1.0,
                time: now,
            })
            .collect();
        let body = LoadBody {
            legacy: EmptyObjectArray,
            reports: ObjectArray(vec![LoadDescription {
                partition: request.unit.id.id,
                service: request.service.name,
                stateful: false,
                primary: ObjectArray(vec![]),
                secondary: ObjectArray(stats.clone()),
                nodes: ObjectArray(vec![NodeLoads {
                    node: request.local.node.id,
                    loads: ObjectArray(stats),
                }]),
            }]),
            time: now,
        };
        let body = to_vec(&body)?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(Error::Closed),
            result = self.connection.send_one_way_fenced(
                12,
                "RAPReportLoad",
                body,
                deadline_after(REPORT_TIMEOUT)?,
                self.fence(),
            ) => result?,
        }
        self.check()?;
        let utc = utc();
        let mut loads = self.loads.lock().unwrap();
        for (name, value) in unique {
            loads.insert(
                name.clone(),
                ReportedLoad {
                    name,
                    value,
                    time: utc,
                    current: f64::from(value),
                },
            );
        }
        Ok(())
    }
    pub(crate) async fn health(self: &Arc<Self>, report: &HealthReport) -> Result<HealthReceipt> {
        self.check()?;
        let _global = self
            .global
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("runtime report admission"))?;
        let _permit = self
            .admission
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("partition report in progress"))?;
        name(&report.source)?;
        name(&report.property)?;
        if report.source.starts_with("System") || report.property == "DeleteProperty" {
            return Err(Error::InvalidInput("reserved native health namespace"));
        }
        text(&report.description, true)?;
        let ttl = match report.time_to_live {
            None => i64::MAX,
            Some(duration) => {
                let ticks = duration.as_nanos() / 100;
                if ticks == 0 || ticks >= i64::MAX as u128 {
                    return Err(Error::InvalidInput("invalid health TTL"));
                }
                ticks as i64
            }
        };
        let sequence = match report.sequence {
            HealthSequence::Explicit(n) if n > 0 => n,
            HealthSequence::Explicit(_) => {
                return Err(Error::InvalidInput("health sequence must be positive"))
            }
            HealthSequence::Auto => auto_sequence()?,
        };
        let request = self.request.lock().unwrap().clone();
        let entity_property = format!(
            "{}+{}+{}",
            uuid::Uuid::from_bytes_le(request.unit.id.id.0),
            request.local.id,
            report.property
        );
        let body = HealthBody {
            reports: ObjectArray(vec![InstanceHealth {
                entity: Pointer(Some(FixedTag(HealthEntity {
                    kind: 2,
                    partition: request.unit.id.id,
                    instance: request.local.id,
                }))),
                source: report.source.clone(),
                property: report.property.clone(),
                ttl,
                state: match report.state {
                    HealthState::Ok => 1,
                    HealthState::Warning => 2,
                    HealthState::Error => 3,
                },
                description: report.description.clone(),
                sequence,
                time: utc(),
                attributes: Attributes {
                    values: EmptyObjectArray,
                },
                remove: report.remove_when_expired,
            }]),
            streams: EmptyObjectArray,
        };
        let headers = vec![Header::activity(uuid::Uuid::new_v4().to_bytes_le())?];
        let body = to_vec(&body)?;
        let reply = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(Error::Closed),
            result = self.connection.request_decoded_tracked(
                "ReportHealth",
                7,
                headers,
                body,
                deadline_after(REPORT_TIMEOUT)?,
                decode_health,
                self.fence(),
            ) => result?,
        };
        self.check()?;
        if reply.results.0.len() != 1 || !reply.streams.0.is_empty() {
            return Err(Error::Protocol("unexpected health result cardinality"));
        }
        let result = &reply.results.0[0];
        if result.kind != 2
            || result.source != report.source
            || result.sequence != sequence
            || result.entity_property != entity_property
        {
            return Err(Error::Protocol("health result identity mismatch"));
        }
        if result.error != 0 {
            return Err(Error::Remote {
                code: result.error,
                message: "HM per-report result (not acknowledged)".into(),
            });
        }
        Ok(HealthReceipt { sequence })
    }
    pub(crate) fn fault_status(&self) -> Option<FaultDelivery> {
        self.fault.lock().unwrap().as_ref().map(|stage| {
            let mut status = stage.delivery.clone();
            status.cancelled = self.check().is_err();
            status
        })
    }
    pub(crate) async fn fault(self: &Arc<Self>, fault: FaultType) -> Result<FaultDelivery> {
        self.check()?;
        let (first, changed, receiver) = {
            let mut stage = self.fault.lock().unwrap();
            if let Some(existing) = stage
                .as_ref()
                .filter(|s| s.delivery.fault == FaultType::Permanent || s.delivery.fault == fault)
            {
                (false, false, existing.complete.subscribe())
            } else {
                let first = stage.is_none();
                let request = self.request.lock().unwrap().clone();
                let body = to_vec(&FaultBody {
                    unit: request.unit,
                    local: request.local,
                    fault: match fault {
                        FaultType::Transient => 1,
                        FaultType::Permanent => 2,
                    },
                    activity: Activity {
                        id: ActivityId {
                            guid: mssf_transport::serialization::serde::Guid(
                                uuid::Uuid::new_v4().to_bytes_le(),
                            ),
                            index: 0,
                        },
                        kind: 2,
                    },
                })?;
                if let Some(old) = &*stage {
                    old.current.store(false, Ordering::Release);
                    if old.complete.borrow().is_none() {
                        old.complete.send_replace(Some(Err(Error::InvalidInput(
                            "transient fault superseded by permanent",
                        ))));
                    }
                }
                let (complete, receiver) = tokio::sync::watch::channel(None);
                *stage = Some(FaultStage {
                    delivery: FaultDelivery {
                        fault,
                        attempts: 0,
                        last_error: None,
                        cancelled: false,
                        superseded_transient: !first,
                    },
                    error: None,
                    body,
                    current: Arc::new(AtomicBool::new(true)),
                    complete,
                });
                (first, true, receiver)
            }
        };
        if first {
            let this = self.clone();
            // Participant executors may stop immediately after a terminal fault.
            // Retries belong to the runtime that installed this native unit.
            self.executor
                .clone()
                .unwrap_or_else(tokio::runtime::Handle::current)
                .spawn(async move {
                    this.fault_worker().await;
                });
        } else if changed {
            self.fault_changed.notify_one();
        }
        self.wait_fault(receiver).await
    }
    async fn wait_fault(
        &self,
        mut receiver: tokio::sync::watch::Receiver<Option<Result<FaultDelivery>>>,
    ) -> Result<FaultDelivery> {
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(Error::Closed),
                result = receiver.changed() => { result.map_err(|_| Error::Closed)?; }
            }
        }
    }
    async fn fault_worker(self: Arc<Self>) {
        loop {
            if self.check().is_err() {
                break;
            }
            // Consume a wake already represented by the snapshot we are about
            // to send. A later upgrade still wakes the post-write wait.
            tokio::select! {
                biased;
                _ = self.fault_changed.notified() => {},
                _ = std::future::ready(()) => {},
            }
            let (body, current) = {
                let stage = self.fault.lock().unwrap();
                let Some(stage) = &*stage else {
                    break;
                };
                (stage.body.clone(), stage.current.clone())
            };
            let this = Arc::downgrade(&self);
            let token = current.clone();
            let sent = SendStatus::fenced(Arc::new(move || {
                token.load(Ordering::Acquire) && this.upgrade().is_some_and(|s| s.check().is_ok())
            }));
            let operation = async {
                let _permit = self
                    .global
                    .try_acquire()
                    .map_err(|_| Error::ResourceLimit("runtime report admission"))?;
                self.connection
                    .send_one_way_fenced(
                        12,
                        "RAReportFault",
                        body,
                        deadline_after(REPORT_TIMEOUT)?,
                        sent,
                    )
                    .await
            };
            let result = tokio::select! {
                _ = self.cancel.cancelled() => break,
                result = operation => result,
            };
            {
                let mut stage = self.fault.lock().unwrap();
                let Some(stage) = &mut *stage else {
                    break;
                };
                if !Arc::ptr_eq(&stage.current, &current) {
                    continue;
                }
                stage.delivery.attempts = stage.delivery.attempts.saturating_add(1);
                stage.delivery.last_error = result.as_ref().err().map(ToString::to_string);
                stage.error = result.as_ref().err().cloned();
                // Initial completion is immutable, including for an unpolled
                // waiter. Retry outcomes belong only in the delivery snapshot.
                if stage.complete.borrow().is_none() {
                    stage
                        .complete
                        .send_replace(Some(result.map(|()| stage.delivery.clone())));
                }
            }
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = self.fault_changed.notified() => {},
                _ = tokio::time::sleep(FAULT_RETRY) => {},
            }
        }
    }
}

fn text(value: &str, empty: bool) -> Result<()> {
    if (!empty && value.is_empty())
        || value.len() > MAX_REPORT_NAME_UNITS * 3
        || value.contains('\0')
        || value.encode_utf16().count() > MAX_REPORT_NAME_UNITS
    {
        return Err(Error::InvalidInput(
            "report text must fit 4096 UTF-16 units without NUL",
        ));
    }
    Ok(())
}
fn name(value: &str) -> Result<()> {
    text(value, false)
}
fn utc() -> i64 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        / 100
        + 116_444_736_000_000_000)
        .min(i64::MAX as u128) as i64
}
fn auto_sequence() -> Result<i64> {
    let mut previous = SEQUENCE.load(Ordering::Acquire);
    loop {
        let next = previous
            .checked_add(1)
            .ok_or(Error::ResourceLimit("health sequence exhausted"))?
            .max(utc());
        match SEQUENCE.compare_exchange_weak(previous, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(next),
            Err(current) => previous = current,
        }
    }
}
fn stopwatch() -> Result<i64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut time) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(time.tv_sec.saturating_mul(10_000_000) + time.tv_nsec / 100)
}
fn decode_health(message: Message) -> Result<HealthReply> {
    let properties = message.properties()?;
    if let Some(fault) = properties.fault {
        return Err(Error::Remote {
            code: fault.code,
            message: "health transport fault".into(),
        });
    }
    if properties.actor != 16 || properties.action != "OperationSuccessAction" {
        return Err(Error::Protocol("unexpected health reply envelope"));
    }
    from_slice(&message.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequence_is_process_monotonic_and_text_bounded() {
        let previous = auto_sequence().unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..100)
                        .map(|_| auto_sequence().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut values: Vec<_> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        values.sort_unstable();
        assert!(values[0] > previous);
        assert!(values.windows(2).all(|v| v[0] < v[1]));
        assert!(name("").is_err());
        assert!(name("a\0b").is_err());
        assert!(name(&"😀".repeat(2048)).is_ok());
        assert!(name(&"😀".repeat(2049)).is_err());
    }
}
