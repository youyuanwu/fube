use crate::{
    lease::Authority, packages::key, wire::UpdateContext, ActivationContext, ConfigurationPackage,
    DataPackage, PackageDescription,
};
use mssf_transport::{Error, Result};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::Instant,
};

pub const MAX_PACKAGE_SUBSCRIPTIONS: usize = 64;

#[derive(Debug, Clone)]
pub enum PackageChange<T> {
    Added(Arc<T>),
    Removed(Arc<T>),
    Modified { previous: Arc<T>, current: Arc<T> },
}
type Callback<T> = Arc<dyn Fn(PackageChange<T>) + Send + Sync>;
type Registrations<T> = Vec<(u64, Callback<T>)>;
#[derive(Default)]
struct Handlers {
    next: u64,
    configuration: Registrations<ConfigurationPackage>,
    data: Registrations<DataPackage>,
}
#[derive(Default)]
struct Registry {
    handlers: Mutex<Handlers>,
    closed: AtomicBool,
    active: AtomicBool,
}

/// Dropping/unregistering excludes future handler-list snapshots, not callbacks
/// already copied for dispatch. There is no replay or drain-on-unregister.
pub struct PackageSubscription {
    registry: Weak<Registry>,
    id: u64,
    configuration: bool,
}
impl PackageSubscription {
    pub fn unregister(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            if self.configuration {
                let removed = {
                    let mut h = registry.handlers.lock().unwrap();
                    h.configuration
                        .iter()
                        .position(|(id, _)| *id == self.id)
                        .map(|p| h.configuration.remove(p))
                };
                drop(removed);
            } else {
                let removed = {
                    let mut h = registry.handlers.lock().unwrap();
                    h.data
                        .iter()
                        .position(|(id, _)| *id == self.id)
                        .map(|p| h.data.remove(p))
                };
                drop(removed);
            }
        }
        self.id = 0;
    }
}
impl Drop for PackageSubscription {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[derive(Clone)]
pub struct ActivationContextHandle {
    current: Arc<RwLock<Arc<ActivationContext>>>,
    registry: Arc<Registry>,
}
impl ActivationContextHandle {
    pub(crate) fn new(activation: ActivationContext) -> Self {
        Self {
            current: Arc::new(RwLock::new(Arc::new(activation))),
            registry: Arc::new(Registry::default()),
        }
    }
    /// One complete immutable generation. Retaining it does not pin directories.
    pub fn snapshot(&self) -> Arc<ActivationContext> {
        self.current.read().unwrap().clone()
    }
    fn register(
        &self,
        configuration: bool,
        insert: impl FnOnce(&mut Handlers, u64),
    ) -> Result<PackageSubscription> {
        let mut h = self.registry.handlers.lock().unwrap();
        if self.registry.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if h.configuration.len() + h.data.len() >= MAX_PACKAGE_SUBSCRIPTIONS {
            return Err(Error::ResourceLimit("package subscriptions"));
        }
        h.next = h
            .next
            .checked_add(1)
            .ok_or(Error::ResourceLimit("package subscription identity"))?;
        let id = h.next;
        insert(&mut h, id);
        Ok(PackageSubscription {
            registry: Arc::downgrade(&self.registry),
            id,
            configuration,
        })
    }
    pub fn subscribe_configuration(
        &self,
        callback: impl Fn(PackageChange<ConfigurationPackage>) + Send + Sync + 'static,
    ) -> Result<PackageSubscription> {
        self.register(true, |h, id| h.configuration.push((id, Arc::new(callback))))
    }
    pub fn subscribe_data(
        &self,
        callback: impl Fn(PackageChange<DataPackage>) + Send + Sync + 'static,
    ) -> Result<PackageSubscription> {
        self.register(false, |h, id| h.data.push((id, Arc::new(callback))))
    }
    pub(crate) fn close(&self) {
        let removed = {
            let mut h = self.registry.handlers.lock().unwrap();
            self.registry.closed.store(true, Ordering::Release);
            (
                std::mem::take(&mut h.configuration),
                std::mem::take(&mut h.data),
            )
        };
        drop(removed); // Captured user destructors may unregister other handles.
    }
    pub(crate) fn quiescent(&self) -> Result<()> {
        if self.registry.active.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct UpdatePermit(Arc<Registry>);
impl Drop for UpdatePermit {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
    }
}

fn changes<T>(
    old: &[Arc<T>],
    new: &[Arc<T>],
    description: impl Fn(&T) -> &PackageDescription,
) -> Vec<PackageChange<T>> {
    let mut result = Vec::new();
    for previous in old {
        match new
            .iter()
            .find(|p| key(description(p).name()) == key(description(previous).name()))
        {
            None => result.push(PackageChange::Removed(previous.clone())),
            Some(current) if description(current).rollout() != description(previous).rollout() => {
                result.push(PackageChange::Modified {
                    previous: previous.clone(),
                    current: current.clone(),
                })
            }
            _ => {}
        }
    }
    for current in new {
        if !old
            .iter()
            .any(|p| key(description(p).name()) == key(description(current).name()))
        {
            result.push(PackageChange::Added(current.clone()));
        }
    }
    result
}

/// Only one control-loop worker may call apply at a time.
pub(crate) struct Updater {
    pub handle: ActivationContextHandle,
    deployment: PathBuf,
}
impl Updater {
    pub fn new(handle: ActivationContextHandle, deployment: &Path) -> Self {
        Self {
            handle,
            deployment: deployment.to_owned(),
        }
    }
}

#[path = "package_context_update.rs"]
mod update;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{packages::tests::Fixture, wire::CodeActivation};
    use std::time::Duration;

    fn setup() -> (Fixture, Updater, Arc<Authority>) {
        let fixture = Fixture::new();
        let mut activation = fixture.activation.clone();
        activation.packages = Some(Arc::new(
            crate::packages::load(&activation, &fixture.root).unwrap(),
        ));
        let updater = Updater::new(ActivationContextHandle::new(activation), &fixture.root);
        (
            fixture,
            updater,
            Authority::new(Instant::now(), 3_000_000_000).unwrap(),
        )
    }
    fn request(updater: &Updater) -> UpdateContext {
        let mut code = updater.handle.snapshot().runtime.code.clone();
        code.version.instance += 1;
        UpdateContext {
            code,
            activation: CodeActivation {
                host: updater.handle.snapshot().host_id.clone(),
                instance: 1234,
            },
            timeout: 300_000_000,
        }
    }
    fn apply(updater: &Updater, update: UpdateContext, authority: &Authority) -> Result<()> {
        let _permit = updater.admit()?;
        updater.apply(update, authority, Instant::now() + Duration::from_secs(5))
    }
    #[test]
    fn atomic_owned_refresh_duplicate_and_incarnation_fences() {
        let (fixture, updater, authority) = setup();
        let old = updater.handle.snapshot();
        let old_package = old
            .packages()
            .unwrap()
            .configuration_package("Config")
            .unwrap();
        let mut duplicate = request(&updater);
        duplicate.code = old.runtime.code.clone();
        apply(&updater, duplicate.clone(), &authority).unwrap();
        assert_eq!(updater.handle.snapshot().generation(), 0);
        assert_eq!(
            updater.handle.snapshot().code_activation_instance(),
            Some(1234)
        );
        fixture.write(2, "new");
        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let handle = updater.handle.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let value = handle.snapshot();
                    let packages = value.packages().unwrap();
                    let version = packages.configuration_package("config").unwrap();
                    assert_eq!(
                        version.description().version(),
                        if value.generation() == 0 { "1" } else { "2" }
                    );
                }
            })
        };
        apply(&updater, request(&updater), &authority).unwrap();
        stop.store(true, Ordering::Release);
        reader.join().unwrap();
        assert_eq!(
            old_package
                .section("Main")
                .unwrap()
                .parameter("Value")
                .unwrap()
                .raw_value(),
            "old"
        );
        assert_eq!(updater.handle.snapshot().generation(), 1);
        assert!(apply(&updater, duplicate, &authority).is_err()); // old context now stale
        let current = updater.handle.snapshot();
        for variant in 0..7 {
            let mut update = request(&updater);
            match variant {
                0 => update.activation.instance = 4321,
                1 => update.activation.host = "other".into(),
                2 => update.code.code_sequence += 1,
                3 => update.code.package_sequence += 1,
                4 => update.code.id.name = "Other".into(),
                5 => update.code.application_name = "fabric:/Other".into(),
                _ => {
                    update.code.version.instance = current.runtime.code.version.instance;
                    update.code.version.version.value.minor += 1;
                }
            }
            assert!(apply(&updater, update, &authority).is_err());
            assert!(Arc::ptr_eq(&current, &updater.handle.snapshot()));
        }
    }

    #[test]
    fn first_binding_invalid_artifacts_code_changes_and_cancellation() {
        let (fixture, updater, authority) = setup();
        for variant in 0..3 {
            let mut update = request(&updater);
            match variant {
                0 => update.activation.host = "wrong".into(),
                1 => update.code.code_sequence += 1,
                _ => update.code.package_sequence += 1,
            }
            assert!(apply(&updater, update, &authority).is_err());
            assert_eq!(updater.handle.snapshot().code_activation_instance(), None);
        }
        for instance in [0, -1] {
            let mut update = request(&updater);
            update.activation.instance = instance;
            assert!(apply(&updater, update, &authority).is_err());
        }
        let path = fixture.base.join("RustService.Package.Current.xml");
        let original = std::fs::read_to_string(&path).unwrap();
        for bad in [
            original.replace("ContentChecksum=\"code\"", "ContentChecksum=\"different\""),
            original.replace(
                "RolloutVersion=\"1.0\" ContentChecksum=\"code\"",
                "RolloutVersion=\"2.0\" ContentChecksum=\"code\"",
            ),
            "<broken".into(),
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(apply(&updater, request(&updater), &authority).is_err());
            assert_eq!(updater.handle.snapshot().code_activation_instance(), None);
            assert_eq!(updater.handle.snapshot().generation(), 0);
        }
        std::fs::write(path, original).unwrap();
        assert!(matches!(
            updater.apply(request(&updater), &authority, Instant::now()),
            Err(Error::Timeout)
        ));
        authority.retire();
        assert!(apply(&updater, request(&updater), &authority).is_err());
    }

    #[test]
    fn copied_lists_reentrant_registration_order_and_equal_rollout() {
        let (fixture, _, authority) = setup();
        fixture.config_count(2, 1, "before");
        let mut activation = fixture.activation.clone();
        activation.packages = Some(Arc::new(
            crate::packages::load(&activation, &fixture.root).unwrap(),
        ));
        let handle = ActivationContextHandle::new(activation);
        let updater = Updater::new(handle.clone(), &fixture.root);
        let log = Arc::new(Mutex::new(Vec::new()));
        let own = Arc::new(Mutex::new(None::<PackageSubscription>));
        let later = Arc::new(Mutex::new(None::<PackageSubscription>));
        let first = {
            let (handle, log, own, later) =
                (handle.clone(), log.clone(), own.clone(), later.clone());
            handle
                .clone()
                .subscribe_configuration(move |event| {
                    let PackageChange::Modified { previous, .. } = event else {
                        panic!("expected modification")
                    };
                    assert_eq!(handle.snapshot().generation(), 1);
                    log.lock()
                        .unwrap()
                        .push(format!("A:{}", previous.description().name()));
                    own.lock().unwrap().take(); // does not retract the second copied event
                    let mut late = later.lock().unwrap();
                    if late.is_none() {
                        let log = log.clone();
                        *late = Some(
                            handle
                                .subscribe_data(move |_| log.lock().unwrap().push("L:Data".into()))
                                .unwrap(),
                        );
                    }
                })
                .unwrap()
        };
        *own.lock().unwrap() = Some(first);
        let _second = {
            let log = log.clone();
            handle
                .subscribe_configuration(move |event| {
                    let PackageChange::Modified { previous, .. } = event else {
                        panic!("expected modification")
                    };
                    log.lock()
                        .unwrap()
                        .push(format!("B:{}", previous.description().name()));
                })
                .unwrap()
        };
        let _data = {
            let log = log.clone();
            handle
                .subscribe_data(move |_| log.lock().unwrap().push("D:Data".into()))
                .unwrap()
        };
        assert!(log.lock().unwrap().is_empty()); // no replay
        fixture.config_count(2, 1, "after");
        let path = fixture.base.join("RustService.Package.Current.xml");
        let xml = std::fs::read_to_string(&path)
            .unwrap()
            .replace(
                "<DigestedConfigPackage RolloutVersion=\"1.0\">",
                "<DigestedConfigPackage RolloutVersion=\"2.0\">",
            )
            .replace(
                "<DigestedDataPackage RolloutVersion=\"1.0\"",
                "<DigestedDataPackage RolloutVersion=\"2.0\"",
            );
        std::fs::write(&path, xml).unwrap();
        apply(&updater, request(&updater), &authority).unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            [
                "A:Config0",
                "A:Config1",
                "B:Config0",
                "B:Config1",
                "D:Data",
                "L:Data"
            ]
        );
        log.lock().unwrap().clear();
        // A newer context with identical package rollouts causes no callbacks.
        for i in 0..2 {
            let path = fixture
                .base
                .join(format!("RustService.Config{i}.1/Settings.xml"));
            let xml = std::fs::read_to_string(&path)
                .unwrap()
                .replace("after", "latest");
            std::fs::write(path, xml).unwrap();
        }
        apply(&updater, request(&updater), &authority).unwrap();
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(updater.handle.snapshot().generation(), 2);
        assert_eq!(
            updater
                .handle
                .snapshot()
                .packages()
                .unwrap()
                .configuration_package("Config0")
                .unwrap()
                .section("Main")
                .unwrap()
                .parameter("P0")
                .unwrap()
                .raw_value(),
            "latest"
        );
    }

    #[test]
    fn subscriptions_and_active_worker_have_exact_bounds() {
        let (_, updater, _) = setup();
        let mut subscriptions = Vec::new();
        for _ in 0..MAX_PACKAGE_SUBSCRIPTIONS {
            subscriptions.push(updater.handle.subscribe_data(|_| {}).unwrap());
        }
        assert!(matches!(
            updater.handle.subscribe_configuration(|_| {}),
            Err(Error::ResourceLimit(_))
        ));
        subscriptions.pop().unwrap().unregister();
        let _replacement = updater.handle.subscribe_configuration(|_| {}).unwrap();
        let permit = updater.admit().unwrap();
        assert!(updater.handle.quiescent().is_err());
        assert!(updater.admit().is_err());
        drop(permit);
        updater.handle.quiescent().unwrap();
        updater.handle.close();
        assert!(matches!(
            updater.handle.subscribe_data(|_| {}),
            Err(Error::Closed)
        ));
    }

    #[test]
    fn unregister_and_close_drop_captured_handles_outside_registry_lock() {
        let (_, updater, _) = setup();
        let nested = updater.handle.subscribe_data(|_| {}).unwrap();
        let subscription = updater
            .handle
            .subscribe_configuration(move |_| {
                let _ = &nested;
            })
            .unwrap();
        drop(subscription);
        assert!(updater
            .handle
            .registry
            .handlers
            .lock()
            .unwrap()
            .data
            .is_empty());
        let nested = updater.handle.subscribe_data(|_| {}).unwrap();
        let _subscription = updater
            .handle
            .subscribe_configuration(move |_| {
                let _ = &nested;
            })
            .unwrap();
        updater.handle.close();
        assert!(updater
            .handle
            .registry
            .handlers
            .lock()
            .unwrap()
            .data
            .is_empty());
    }
}
