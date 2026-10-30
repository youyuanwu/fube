//! Native authorities (3988b451): Hosting.ServiceTypeMap.cpp:20-145,
//! Infrastructure.BackgroundWorkManager.cpp:53-225 and RetryTimer.cpp:46-149.
use super::*;
use fube_transport::serialization::serde::ObjectArray;
use std::cmp::Ordering;

/// Linux PAL wc16.c:131-150, native short-wchar build, startup C locale.
/// The exhaustive original-PAL probe changes only ASCII a-z; inputs are not
/// restricted to ASCII. The mismatch operands are signed shorts.
pub fn native_upper_unit(unit: u16) -> i16 {
    if (b'a' as u16..=b'z' as u16).contains(&unit) {
        (unit - 32) as i16
    } else {
        unit as i16
    }
}

fn compare_names(left: &str, right: &str) -> Ordering {
    let mut left = left.encode_utf16().chain(std::iter::once(0));
    let mut right = right.encode_utf16().chain(std::iter::once(0));
    loop {
        let l = native_upper_unit(left.next().unwrap());
        let r = native_upper_unit(right.next().unwrap());
        match l.cmp(&r) {
            Ordering::Equal if l != 0 => {}
            result => return result,
        }
    }
}

/// ServiceTypeIdentifier.cpp:78-118; ApplicationIdentifier.cpp:86-123;
/// ServicePackageIdentifier.cpp:91-123. Raw malformed NUL-app triples need not
/// form a total order even in native; this is a comparator, not name validation.
pub fn compare_service_types(left: &ServiceType, right: &ServiceType) -> Ordering {
    let a = &left.package.application;
    let b = &right.package.application;
    let empty = |a: &ApplicationId| a.number == 0 && a.type_name.is_empty();
    if !empty(a) || !empty(b) {
        let result = a
            .number
            .cmp(&b.number)
            .then_with(|| compare_names(&a.type_name, &b.type_name))
            .then_with(|| compare_names(&left.package.name, &right.package.name));
        if result != Ordering::Equal {
            return result;
        }
    }
    compare_names(&left.name, &right.name)
}

/// Stopwatch observations and intervals are signed native 100ns ticks.
pub trait NotificationClock: Send + Sync {
    fn now_ticks(&self) -> i64;
}

struct NativeClock;
impl NotificationClock for NativeClock {
    fn now_ticks(&self) -> i64 {
        #[repr(C)]
        struct Timespec {
            seconds: std::os::raw::c_long,
            nanoseconds: std::os::raw::c_long,
        }
        unsafe extern "C" {
            fn clock_gettime(clock: std::os::raw::c_int, time: *mut Timespec) -> i32;
        }
        let mut time = Timespec {
            seconds: 0,
            nanoseconds: 0,
        };
        // Stopwatch.cpp:16-29 and pal/src/internal/pal.cpp:655-681 use Linux
        // CLOCK_MONOTONIC_RAW (4), not elapsed time since executor construction.
        // SAFETY: Linux clock_gettime writes the correctly laid out timespec.
        let result = unsafe { clock_gettime(4, &mut time) };
        assert_eq!(result, 0, "CLOCK_MONOTONIC_RAW must be available");
        (i128::from(time.seconds) * 10_000_000 + i128::from(time.nanoseconds) / 100)
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}
pub(super) fn native_clock() -> Arc<dyn NotificationClock> {
    Arc::new(NativeClock)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTypeEntry {
    pub info: ServiceTypeInfo,
    pub references: usize,
    pub pending: bool,
    /// Native LinearRetryPolicy starts/clears at StopwatchTime::Zero.
    pub last_sent_ticks: i64,
}
#[derive(Clone)]
struct Entry {
    value: ServiceTypeEntry,
    incarnation: u64,
    round: u64,
}
#[derive(Clone)]
struct Captured {
    work: u64,
    pending: bool,
    retry_sequence: u64,
    selected: Vec<(u64, u64)>,
}
#[derive(Clone, Default)]
enum Work {
    #[default]
    Idle,
    Waiting(u64),
    Queued {
        id: u64,
        pending: bool,
    },
    Running {
        capture: Captured,
        pending: bool,
    },
}
#[derive(Clone, Default)]
pub(super) struct Coordinator {
    // Direct native comparison avoids inventing a normalized total-order key
    // for malformed raw identities whose native comparator is itself non-total.
    entries: Vec<Entry>,
    next_id: u64,
    last_start: i64,
    work: Work,
    retry_sequence: u64,
    retry_timer: Option<u64>,
}

fn elapsed(now: i64, previous: i64) -> i128 {
    i128::from(now) - i128::from(previous)
}
fn next(value: &mut u64) -> Result<u64> {
    *value = value.checked_add(1).ok_or(Failure::Capacity)?;
    Ok(*value)
}
fn timer_delay(interval: i128) -> i64 {
    // Timer.Linux.cpp:347-370: zero/negative due times become one native tick.
    interval.clamp(1, i128::from(i64::MAX)) as i64
}

impl Coordinator {
    pub fn snapshot(&self) -> Vec<ServiceTypeEntry> {
        self.entries.iter().map(|e| e.value.clone()).collect()
    }
    fn index(&self, id: &ServiceType) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| compare_service_types(&e.value.info.versioned.id, id).is_eq())
    }

    pub fn add(
        &mut self,
        info: ServiceTypeInfo,
        settings: &Settings,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Vec<Effect>> {
        let index = self.index(&info.versioned.id);
        let mut projected = info.clone();
        if let Some(index) = index {
            let prior = &self.entries[index].value.info;
            projected.versioned.id = prior.versioned.id.clone();
            if prior.versioned.version == info.versioned.version {
                projected = prior.clone();
            }
        } else if self.entries.len() >= settings.max_service_types {
            return Err(Failure::Capacity);
        }
        if to_vec(&ServiceTypeNotification {
            infos: ObjectArray(vec![projected.clone()]),
        })
        .map_or(true, |b| b.len() > settings.notification_batch_bytes)
        {
            return Err(Failure::Capacity);
        }
        // Hosting.ServiceTypeMapEntity.cpp:28-47: same-version payload ignored;
        // different full versions retain the first registration's identifier.
        let entry = if let Some(index) = index {
            &mut self.entries[index]
        } else {
            let incarnation = next(&mut self.next_id)?;
            let index = self
                .entries
                .iter()
                .position(|e| {
                    compare_service_types(&e.value.info.versioned.id, &projected.versioned.id)
                        .is_gt()
                })
                .unwrap_or(self.entries.len());
            self.entries.insert(
                index,
                Entry {
                    value: ServiceTypeEntry {
                        info: projected.clone(),
                        references: 0,
                        pending: false,
                        last_sent_ticks: 0,
                    },
                    incarnation,
                    round: 0,
                },
            );
            &mut self.entries[index]
        };
        entry.value.references = entry
            .value
            .references
            .checked_add(1)
            .ok_or(Failure::Capacity)?;
        if entry.value.references == 1
            || entry.value.info.versioned.version != info.versioned.version
        {
            entry.round = entry.round.checked_add(1).ok_or(Failure::Capacity)?;
            entry.value.info = projected;
            entry.value.pending = true;
            entry.value.last_sent_ticks = 0;
            self.request(config, clock)
        } else {
            Ok(vec![])
        }
    }
    pub fn remove(&mut self, id: &ServiceType) {
        let index = self.index(id).expect("attached native map reference");
        self.entries[index].value.references -= 1;
        if self.entries[index].value.references == 0 {
            self.entries.remove(index);
        }
    }
    pub fn acknowledge(&mut self, body: &ServiceTypeNotification) {
        // Hosting.ServiceTypeMap.cpp:116-126: generic generation admission is
        // outside this map; acknowledgment has only key + version instance.
        for info in &body.infos.0 {
            if let Some(index) = self.index(&info.versioned.id) {
                let entry = &mut self.entries[index].value;
                if entry.info.versioned.version.instance == info.versioned.version.instance {
                    entry.pending = false;
                }
            }
        }
    }
    fn schedule(
        &mut self,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Vec<Effect>> {
        let now = clock.now_ticks();
        let interval = config.notification(NotificationProperty::NodeIntervalTicks);
        let due = elapsed(now, self.last_start) >= i128::from(interval);
        let work = next(&mut self.next_id)?;
        let after_ticks = if due {
            self.work = Work::Queued {
                id: work,
                pending: false,
            };
            0
        } else {
            // BGM.cpp:181-206 reads the getter again when arming the remainder.
            let interval = config.notification(NotificationProperty::NodeIntervalTicks);
            self.work = Work::Waiting(work);
            timer_delay(i128::from(interval) - elapsed(now, self.last_start))
        };
        Ok(vec![Effect::RequestServiceTypeWork { work, after_ticks }])
    }
    pub fn request(
        &mut self,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Vec<Effect>> {
        match &mut self.work {
            Work::Idle => self.schedule(config, clock),
            Work::Waiting(_) => Ok(vec![]),
            Work::Queued { pending, .. } | Work::Running { pending, .. } => {
                *pending = true;
                Ok(vec![])
            }
        }
    }
    pub fn capture(
        &mut self,
        work: u64,
        settings: &Settings,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Option<Vec<Effect>>> {
        let pending = match self.work {
            Work::Waiting(id) if id == work => false,
            Work::Queued { id, pending } if id == work => pending,
            _ => return Ok(None),
        };
        // BGM.cpp:114-123 records actual Process start, not the Request time.
        self.last_start = clock.now_ticks();
        let now = clock.now_ticks();
        let retry_sequence = self.retry_sequence;
        let mut is_pending = false;
        let due: Vec<_> = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                is_pending |= entry.value.pending;
                // LinearRetryPolicy.h:19-41 reads current interval on each check.
                (entry.value.pending
                    && elapsed(now, entry.value.last_sent_ticks)
                        >= i128::from(config.notification(NotificationProperty::TypeIntervalTicks)))
                .then_some(index)
            })
            .collect();
        // Map.cpp:87-94 reads count AFTER context capture. A signed negative
        // converts to size_t and selects all finite due entries; zero selects none.
        let limit = config.notification(NotificationProperty::BatchCount) as usize;
        let mut selected = Vec::new();
        let mut infos = Vec::new();
        for index in due.into_iter().take(limit) {
            let entry = &self.entries[index];
            infos.push(entry.value.info.clone());
            if to_vec(&ServiceTypeNotification {
                infos: ObjectArray(infos.clone()),
            })
            .map_or(true, |b| b.len() > settings.notification_batch_bytes)
            {
                infos.pop();
                break;
            }
            selected.push((entry.incarnation, entry.round));
        }
        let body = (!infos.is_empty()).then(|| {
            Box::new(ServiceTypeNotification {
                infos: ObjectArray(infos),
            })
        });
        self.work = Work::Running {
            capture: Captured {
                work,
                pending: is_pending,
                retry_sequence,
                selected,
            },
            pending,
        };
        Ok(Some(vec![Effect::ServiceTypeWorkCaptured { work, body }]))
    }
    pub fn complete(
        &mut self,
        work: u64,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Option<Vec<Effect>>> {
        if !matches!(&self.work, Work::Running { capture, .. } if capture.work == work) {
            return Ok(None);
        }
        let Work::Running { capture, pending } = std::mem::take(&mut self.work) else {
            unreachable!()
        };
        let retry_time = clock.now_ticks();
        // EntityRetryComponent.h:119-133 and RetryState.h:76-94. Deleted
        // incarnations are not recreated; an old round cannot throttle a new one.
        for (incarnation, round) in capture.selected {
            if let Some(entry) = self
                .entries
                .iter_mut()
                .find(|e| e.incarnation == incarnation && e.round == round && e.value.pending)
            {
                entry.value.last_sent_ticks = retry_time;
            }
        }
        // BGMWithRetry.cpp:57-68: finish BGM FIRST, then Set/TryCancel timer.
        let mut effects = if pending {
            self.schedule(config, clock)?
        } else {
            vec![]
        };
        if capture.pending {
            effects.extend(self.set_timer(config)?);
        } else if self.retry_sequence == capture.retry_sequence {
            // RetryTimer.cpp:69-90,106-123: cancellation recreates the native
            // timer, even if unset. Only an actually armed adapter needs cancel.
            if let Some(timer) = self.retry_timer.take() {
                effects.push(Effect::CancelServiceTypeRetry { timer });
            }
        }
        Ok(Some(effects))
    }
    pub fn set_timer(&mut self, config: &NativeConfig) -> Result<Vec<Effect>> {
        next(&mut self.retry_sequence)?;
        if self.retry_timer.is_some() {
            return Ok(vec![]);
        }
        let timer = next(&mut self.next_id)?;
        self.retry_timer = Some(timer);
        Ok(vec![Effect::ArmServiceTypeRetry {
            timer,
            after_ticks: timer_delay(i128::from(
                config.notification(NotificationProperty::RetryIntervalTicks),
            )),
        }])
    }
    pub fn timer(
        &mut self,
        timer: u64,
        config: &NativeConfig,
        clock: &dyn NotificationClock,
    ) -> Result<Option<Vec<Effect>>> {
        if self.retry_timer != Some(timer) {
            return Ok(None);
        }
        // RetryTimer.cpp:126-149 clears isSet BEFORE requesting BGM work.
        self.retry_timer = None;
        self.request(config, clock).map(Some)
    }
}

pub(super) fn info(state: &EntityState) -> ServiceTypeInfo {
    let registration = state.runtime.as_ref().expect("attached registration");
    ServiceTypeInfo {
        versioned: VersionedServiceType {
            id: registration.service_type.clone(),
            version: state.body.service.version.clone(),
        },
        code_package: registration.code_package.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

    struct Clock(AtomicI64);
    impl Clock {
        fn new(now: i64) -> Self {
            Self(AtomicI64::new(now))
        }
        fn set(&self, now: i64) {
            self.0.store(now, AtomicOrdering::SeqCst);
        }
    }
    impl NotificationClock for Clock {
        fn now_ticks(&self) -> i64 {
            self.0.load(AtomicOrdering::SeqCst)
        }
    }
    fn info(name: &str, instance: u64) -> ServiceTypeInfo {
        ServiceTypeInfo {
            versioned: VersionedServiceType {
                id: ServiceType {
                    name: name.into(),
                    ..ServiceType::default()
                },
                version: VersionInstance {
                    instance,
                    ..VersionInstance::default()
                },
            },
            code_package: format!("Code{instance}"),
        }
    }
    fn work(effects: &[Effect]) -> (u64, i64) {
        let [Effect::RequestServiceTypeWork { work, after_ticks }] = effects else {
            panic!("{effects:?}")
        };
        (*work, *after_ticks)
    }
    fn timer(effects: &[Effect]) -> (u64, i64) {
        let [Effect::ArmServiceTypeRetry { timer, after_ticks }] = effects else {
            panic!("{effects:?}")
        };
        (*timer, *after_ticks)
    }
    fn capture(
        map: &mut Coordinator,
        work: u64,
        settings: &Settings,
        config: &NativeConfig,
        clock: &Clock,
    ) -> Option<Box<ServiceTypeNotification>> {
        let effects = map.capture(work, settings, config, clock).unwrap().unwrap();
        let [Effect::ServiceTypeWorkCaptured {
            work: captured,
            body,
        }] = effects.as_slice()
        else {
            panic!("{effects:?}")
        };
        assert_eq!(*captured, work);
        body.clone()
    }
    fn config(settings: &Settings) -> NativeConfig {
        let config = NativeConfig::new(settings);
        config.set_notification(NotificationProperty::NodeIntervalTicks, 0);
        config.set_notification(NotificationProperty::TypeIntervalTicks, 0);
        config
    }

    #[test]
    fn native_linux_units_nul_signed_order_and_application_package_rules() {
        let id = |name| info(name, 1).versioned.id;
        assert_eq!(
            compare_service_types(&id("Type\0x"), &id("tYPE\0y")),
            Ordering::Equal
        );
        assert_eq!(compare_service_types(&id("é"), &id("É")), Ordering::Greater);
        assert_eq!(compare_service_types(&id("σ"), &id("ς")), Ordering::Greater);
        assert_eq!(
            compare_service_types(&id("\u{8000}"), &id("\u{7fff}")),
            Ordering::Less
        );
        assert_eq!(compare_service_types(&id("𐐨"), &id("𐐀")), Ordering::Greater);
        assert_eq!(native_upper_unit(0xd800), 0xd800_u16 as i16);
        let mut a = id("Type");
        let mut b = id("type");
        a.package.name = "Ignored".into();
        b.package.name = "Other".into();
        assert_eq!(compare_service_types(&a, &b), Ordering::Equal);
        a.package.application.number = 1;
        assert_eq!(compare_service_types(&a, &b), Ordering::Greater);
        b.package.application.number = 1;
        a.package.application.type_name = "Z".into();
        b.package.application.type_name = "a".into();
        assert_eq!(compare_service_types(&a, &b), Ordering::Greater);
        b.package.application.number = 2; // application number precedes name
        assert_eq!(compare_service_types(&a, &b), Ordering::Less);
        b.package.application.number = 1;
        b.package.application.type_name = "z".into();
        assert_eq!(compare_service_types(&a, &b), Ordering::Less); // package is now compared
        a.package.application.type_name = "\0suffix".into();
        b.package.application.type_name.clear();
        a.package.application.number = 0;
        b.package.application.number = 0;
        assert_eq!(compare_service_types(&a, &b), Ordering::Less); // original app nonempty
    }

    #[test]
    fn actual_projection_ignores_same_version_payload_and_retains_first_spelling() {
        let settings = Settings {
            notification_batch_bytes: 590,
            max_service_types: 1,
            ..Settings::default()
        };
        let config = config(&settings);
        let clock = Clock::new(1_000_000_000);
        let mut map = Coordinator::default();
        let mut a = info("Type\0first", 2);
        a.versioned.id.package.name = "P".repeat(200);
        map.add(a.clone(), &settings, &config, &clock).unwrap();
        let mut ignored = info("tYPE\0different", 2);
        ignored.code_package = "C".repeat(20_000);
        ignored.versioned.id.package.name = "Ignored".repeat(2000);
        assert!(map
            .add(ignored.clone(), &settings, &config, &clock)
            .unwrap()
            .is_empty());
        assert_eq!(map.snapshot()[0].info, a);
        assert_eq!(map.snapshot()[0].references, 2);
        let before = map.snapshot();
        ignored.versioned.version.instance = 1;
        assert_eq!(
            map.add(ignored.clone(), &settings, &config, &clock),
            Err(Failure::Capacity)
        );
        assert_eq!(map.snapshot(), before);
        ignored.code_package = "Downgrade".into();
        map.add(ignored.clone(), &settings, &config, &clock)
            .unwrap();
        assert_eq!(map.snapshot()[0].info.versioned.id, a.versioned.id);
        assert_eq!(map.snapshot()[0].info.versioned.version.instance, 1);
        map.remove(&ignored.versioned.id);
        assert_eq!(map.snapshot()[0].info.versioned.version.instance, 1); // no fallback
        assert_eq!(
            map.add(info("Other", 1), &settings, &config, &clock),
            Err(Failure::Capacity)
        );
        map.remove(&a.versioned.id);
        map.remove(&a.versioned.id);
        assert!(map.snapshot().is_empty());
        assert_eq!(
            map.add(ignored.clone(), &settings, &config, &clock),
            Err(Failure::Capacity)
        );
        ignored.versioned.id.package.name = "New spelling".into();
        map.add(ignored.clone(), &settings, &config, &clock)
            .unwrap();
        assert_eq!(map.snapshot()[0].info, ignored);
    }

    #[test]
    fn emitted_work_uses_remaining_ticks_and_coalesces_without_retiming() {
        let settings = Settings::default();
        let config = config(&settings);
        config.set_notification(NotificationProperty::NodeIntervalTicks, 17);
        config.set_notification(NotificationProperty::TypeIntervalTicks, 25);
        let clock = Clock::new(0);
        let mut map = Coordinator::default();
        let (first, delay) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
        assert_eq!(delay, 17); // no invented never-sent exemption at clock zero
        assert!(map.request(&config, &clock).unwrap().is_empty());
        config.set_notification(NotificationProperty::NodeIntervalTicks, 500);
        clock.set(17);
        assert!(capture(&mut map, first, &settings, &config, &clock).is_none());
        config.set_notification(NotificationProperty::RetryIntervalTicks, 7);
        assert_eq!(
            timer(&map.complete(first, &config, &clock).unwrap().unwrap()).1,
            7
        );
        config.set_notification(NotificationProperty::NodeIntervalTicks, 13);
        clock.set(19);
        let (second, delay) = work(&map.request(&config, &clock).unwrap());
        assert_eq!(delay, 11); // last actual start=17, not last request=0
        assert!(map.request(&config, &clock).unwrap().is_empty());
        config.set_notification(NotificationProperty::NodeIntervalTicks, 900);
        clock.set(30);
        assert!(capture(&mut map, second, &settings, &config, &clock).is_some());
        clock.set(31);
        assert!(map
            .complete(second, &config, &clock)
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(map.snapshot()[0].last_sent_ticks, 31); // record time, not capture time
        assert!(map.complete(second, &config, &clock).unwrap().is_none());
    }

    #[test]
    fn immediate_enqueued_and_executing_requests_coalesce_one_followup() {
        let settings = Settings::default();
        let config = config(&settings);
        config.set_notification(NotificationProperty::NodeIntervalTicks, 10);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let (first, delay) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
        assert_eq!(delay, 0);
        for _ in 0..3 {
            assert!(map.request(&config, &clock).unwrap().is_empty());
        }
        clock.set(105);
        capture(&mut map, first, &settings, &config, &clock);
        for _ in 0..3 {
            assert!(map.request(&config, &clock).unwrap().is_empty());
        }
        clock.set(108);
        let effects = map.complete(first, &config, &clock).unwrap().unwrap();
        let [Effect::RequestServiceTypeWork {
            work: second,
            after_ticks: 7,
        }, Effect::ArmServiceTypeRetry { .. }] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert_ne!(*second, first);
        assert!(map.request(&config, &clock).unwrap().is_empty());
        clock.set(115);
        capture(&mut map, *second, &settings, &config, &clock);
        assert!(map
            .complete(*second, &config, &clock)
            .unwrap()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn dynamic_signed_count_and_intervals_are_consumed_at_work_and_arm() {
        for (limit, expected) in [(0, 0), (1, 1), (100, 3), (-1, 3), (i32::MIN, 3)] {
            let settings = Settings::default();
            let config = config(&settings);
            let clock = Clock::new(100);
            let mut map = Coordinator::default();
            let (first, _) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
            map.add(info("B", 2), &settings, &config, &clock).unwrap();
            map.add(info("C", 3), &settings, &config, &clock).unwrap();
            config.set_notification(NotificationProperty::BatchCount, i64::from(limit));
            let body = capture(&mut map, first, &settings, &config, &clock);
            assert_eq!(body.as_ref().map_or(0, |b| b.infos.0.len()), expected);
            assert!(map
                .snapshot()
                .iter()
                .all(|e| e.last_sent_ticks == 0 && e.pending));
            config.set_notification(NotificationProperty::BatchCount, 0);
            config.set_notification(NotificationProperty::RetryIntervalTicks, 0);
            let effects = map.complete(first, &config, &clock).unwrap().unwrap();
            assert!(matches!(
                effects.last(),
                Some(Effect::ArmServiceTypeRetry { after_ticks: 1, .. })
            ));
            assert_eq!(
                map.snapshot()
                    .iter()
                    .filter(|e| e.last_sent_ticks == 100)
                    .count(),
                expected
            );
        }
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(0);
        let mut map = Coordinator::default();
        config.set_notification(NotificationProperty::TypeIntervalTicks, 1);
        let (first, _) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
        assert!(capture(&mut map, first, &settings, &config, &clock).is_none());
        map.complete(first, &config, &clock).unwrap();
        config.set_notification(NotificationProperty::TypeIntervalTicks, -1); // native direct setter
        let (second, _) = work(&map.request(&config, &clock).unwrap());
        assert!(capture(&mut map, second, &settings, &config, &clock).is_some());
    }

    #[test]
    fn type_interval_updates_do_not_retime_or_invalidate_captured_sends() {
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let (first, _) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
        assert!(capture(&mut map, first, &settings, &config, &clock).is_some());
        config.set_notification(NotificationProperty::TypeIntervalTicks, 999);
        clock.set(101);
        map.complete(first, &config, &clock).unwrap();
        assert_eq!(map.snapshot()[0].last_sent_ticks, 101);
        config.set_notification(NotificationProperty::TypeIntervalTicks, 5);
        clock.set(104);
        let (second, _) = work(&map.request(&config, &clock).unwrap());
        assert!(capture(&mut map, second, &settings, &config, &clock).is_none());
        map.complete(second, &config, &clock).unwrap();
        assert_eq!(map.snapshot()[0].last_sent_ticks, 101);
        config.set_notification(NotificationProperty::TypeIntervalTicks, 2);
        assert_eq!(map.snapshot()[0].last_sent_ticks, 101);
        let (third, _) = work(&map.request(&config, &clock).unwrap());
        assert!(capture(&mut map, third, &settings, &config, &clock).is_some());
    }

    #[test]
    fn captured_old_body_can_be_sent_but_old_completion_cannot_throttle_or_recreate() {
        for recreate in [false, true] {
            let settings = Settings::default();
            let config = config(&settings);
            let clock = Clock::new(100);
            let mut map = Coordinator::default();
            let old = info("Type", 1);
            let (first, _) = work(&map.add(old.clone(), &settings, &config, &clock).unwrap());
            let body = capture(&mut map, first, &settings, &config, &clock).unwrap();
            assert_eq!(body.infos.0.as_slice(), std::slice::from_ref(&old));
            let mut new = info("TYPE", 2);
            if recreate {
                map.remove(&old.versioned.id);
            }
            map.add(new.clone(), &settings, &config, &clock).unwrap();
            clock.set(200);
            let effects = map.complete(first, &config, &clock).unwrap().unwrap();
            assert_eq!(map.snapshot()[0].last_sent_ticks, 0);
            assert_eq!(
                map.snapshot()[0].info.versioned.id.name,
                if recreate { "TYPE" } else { "Type" }
            );
            assert_eq!(body.infos.0.as_slice(), std::slice::from_ref(&old)); // send after replacement
            let [Effect::RequestServiceTypeWork { work: second, .. }, Effect::ArmServiceTypeRetry { .. }] =
                effects.as_slice()
            else {
                panic!("{effects:?}");
            };
            new.versioned.id.name = "tYpE".into();
            let next_body = capture(&mut map, *second, &settings, &config, &clock).unwrap();
            let mut ack = next_body.infos.0[0].clone();
            ack.versioned.version.version.value.major += 1;
            ack.code_package = "NotMatched".into();
            map.acknowledge(&ServiceTypeNotification {
                infos: ObjectArray(vec![ack]),
            });
            map.complete(*second, &config, &clock).unwrap();
            assert!(!map.snapshot()[0].pending);
            assert_eq!(map.snapshot()[0].last_sent_ticks, 0); // ack before record
            map.remove(&new.versioned.id);
            if !recreate {
                map.remove(&old.versioned.id);
            }
            assert!(map.snapshot().is_empty());
        }
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let old = info("Type", 1);
        let (first, _) = work(&map.add(old.clone(), &settings, &config, &clock).unwrap());
        let body = capture(&mut map, first, &settings, &config, &clock).unwrap();
        map.remove(&old.versioned.id);
        map.complete(first, &config, &clock).unwrap();
        assert_eq!(body.infos.0, [old]);
        assert!(map.snapshot().is_empty());
    }

    #[test]
    fn same_version_add_preserves_captured_round_and_instance_only_partial_ack() {
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let a = info("A", 7);
        let b = info("B", 8);
        let (first, _) = work(&map.add(a.clone(), &settings, &config, &clock).unwrap());
        map.add(b.clone(), &settings, &config, &clock).unwrap();
        let body = capture(&mut map, first, &settings, &config, &clock).unwrap();
        assert_eq!(body.infos.0, [a.clone(), b.clone()]);
        let mut same = a.clone();
        same.code_package = "Ignored".into();
        assert!(map
            .add(same, &settings, &config, &clock)
            .unwrap()
            .is_empty());
        clock.set(123);
        map.complete(first, &config, &clock).unwrap();
        assert_eq!(map.snapshot()[0].last_sent_ticks, 123); // same version kept the round
        let mut wrong = b.clone();
        wrong.versioned.version.instance += 1;
        let mut ack = a;
        ack.versioned.version.version.value.major += 1;
        ack.code_package = "NotMatched".into();
        map.acknowledge(&ServiceTypeNotification {
            infos: ObjectArray(vec![wrong, ack]),
        });
        assert!(!map.snapshot()[0].pending);
        assert_eq!(map.snapshot()[0].last_sent_ticks, 123);
        assert_eq!(map.snapshot()[1].last_sent_ticks, 123);
    }

    #[test]
    fn retry_timer_set_sequence_does_not_rearm_and_guards_cancellation() {
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        config.set_notification(NotificationProperty::RetryIntervalTicks, 19);
        let (armed, delay) = timer(&map.set_timer(&config).unwrap());
        assert_eq!(delay, 19);
        let (first, _) = work(&map.request(&config, &clock).unwrap());
        assert!(capture(&mut map, first, &settings, &config, &clock).is_none());
        config.set_notification(NotificationProperty::RetryIntervalTicks, 3);
        assert!(map.set_timer(&config).unwrap().is_empty()); // sequence advances, arm unchanged
        assert_eq!(map.retry_sequence, 2);
        assert!(map
            .complete(first, &config, &clock)
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(map.retry_timer, Some(armed)); // old sequence cannot cancel new Set
        let (second, _) = work(&map.request(&config, &clock).unwrap());
        capture(&mut map, second, &settings, &config, &clock);
        assert_eq!(
            map.complete(second, &config, &clock).unwrap().unwrap(),
            [Effect::CancelServiceTypeRetry { timer: armed }]
        );
        assert!(map.timer(armed, &config, &clock).unwrap().is_none());
        let (new_timer, delay) = timer(&map.set_timer(&config).unwrap());
        assert_ne!(new_timer, armed);
        assert_eq!(delay, 3);
        let effects = map.timer(new_timer, &config, &clock).unwrap().unwrap();
        assert!(map.retry_timer.is_none()); // cleared before BGM Request
        let (third, _) = work(&effects);
        assert!(map.timer(new_timer, &config, &clock).unwrap().is_none());
        capture(&mut map, third, &settings, &config, &clock);
        assert!(map
            .complete(third, &config, &clock)
            .unwrap()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn stale_clock_observations_are_not_semantic_admission_failures() {
        let settings = Settings::default();
        let config = config(&settings);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let (first, _) = work(&map.add(info("A", 1), &settings, &config, &clock).unwrap());
        capture(&mut map, first, &settings, &config, &clock);
        clock.set(50);
        map.complete(first, &config, &clock).unwrap();
        assert_eq!(map.snapshot()[0].last_sent_ticks, 50);
        clock.set(1);
        config.set_notification(NotificationProperty::NodeIntervalTicks, 7);
        let (_, delay) = work(&map.request(&config, &clock).unwrap());
        assert_eq!(delay, 106); // signed elapsed, not saturating elapsed or rejection
    }

    #[test]
    fn codec_ceiling_emits_valid_prefix_without_losing_remaining_due_work() {
        let settings = Settings {
            max_service_types: 128,
            notification_batch_bytes: 4 * 1024 * 1024,
            ..Settings::default()
        };
        let config = config(&settings);
        config.set_notification(NotificationProperty::TypeIntervalTicks, 10);
        let clock = Clock::new(100);
        let mut map = Coordinator::default();
        let mut first = None;
        for n in 0..65 {
            let mut i = info(&format!("Type{n:02}"), 1);
            i.code_package = "C".repeat(32768);
            let effects = map.add(i, &settings, &config, &clock).unwrap();
            if !effects.is_empty() {
                first = Some(work(&effects).0);
            }
        }
        let first = first.unwrap();
        let body = capture(&mut map, first, &settings, &config, &clock).unwrap();
        let count = body.infos.0.len();
        assert!(count > 0 && count < 65);
        assert!(to_vec(&body).is_ok());
        let effects = map.complete(first, &config, &clock).unwrap().unwrap();
        let [Effect::RequestServiceTypeWork { work: second, .. }, Effect::ArmServiceTypeRetry { .. }] =
            effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        let rest = capture(&mut map, *second, &settings, &config, &clock).unwrap();
        assert_eq!(rest.infos.0.len(), 65 - count);
        assert!(map.snapshot().iter().all(|e| e.pending));
    }
}
