//! Bounded deterministic H2 residency, independent of H1 registration snapshots.
//!
//! Native authority is Service Fabric commit
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`, particularly
//! [Deactivator.cpp](https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp).
//! Accounting admission is an explicit premise, not deployment or activation.
//! Shared/exclusive identity selects accounting grace only. Physical execution,
//! successful removal, failure reopening and physical End are not implemented.
//!
//! Native package-first increment can leave a package-only mutation when the
//! application closes. Native normal decrement is application-first. Exact
//! nonclone owners, bounded residual records and verified package-only compensation
//! are stronger local safety rules, not native rollback/void-decrement receipts.
//! Dropping an owner does nothing to its ledger record. Shutdown retains records
//! and admitted work rather than applying native Close's map-clearing behavior.
//!
//! Each timer reserves its callback/final pipeline and logical bytes before
//! admission. A rejected timer request remains inspectable in its entry's
//! preallocated diagnostic slot; eligibility does not promise timer admission.
//! Successful execution guards retain final obligations without calling an engine.
use crate::identity::{validate_package, validate_public};
use crate::*;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static NEXT_LEDGER: AtomicU64 = AtomicU64::new(1);
const WORK_CHARGE: usize = 192;
const SEQUENCE_OVERFLOW: &str = "package accounting sequence overflow";

/// Native signed 100ns duration; native MaxValue must be represented by `Infinite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GracePeriod {
    Finite(i64),
    Infinite,
}

impl GracePeriod {
    fn validate(self) -> Result<()> {
        match self {
            Self::Finite(ticks) if ticks < 0 || ticks == i64::MAX => Err(Error::InvalidInput),
            _ => Ok(()),
        }
    }
}

/// Dynamic, explicitly observed grace; no filesystem/debug/process discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidencyConfig {
    pub shared_grace: GracePeriod,
    pub exclusive_grace: GracePeriod,
    pub unused_scan_interval: GracePeriod,
}

impl Default for ResidencyConfig {
    fn default() -> Self {
        // HostingConfig.h:153-164: 60s shared, 1s exclusive, 600s unused scan.
        Self {
            shared_grace: GracePeriod::Finite(600_000_000),
            exclusive_grace: GracePeriod::Finite(10_000_000),
            unused_scan_interval: GracePeriod::Finite(6_000_000_000),
        }
    }
}

impl ResidencyConfig {
    fn validate(self) -> Result<()> {
        self.shared_grace.validate()?;
        self.exclusive_grace.validate()?;
        self.unused_scan_interval.validate()
    }
}

/// Finite retained logical-payload budgets, not allocator/RSS bounds.
///
/// Timer admission also reserves callback and final capacity. This conservative
/// escrow can reject a timer before its individual timer limit is reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidencyLimits {
    pub applications: usize,
    pub packages: usize,
    pub acquisitions: usize,
    pub timers: usize,
    pub callbacks: usize,
    pub final_obligations: usize,
    pub state_bytes: usize,
}

impl Default for ResidencyLimits {
    fn default() -> Self {
        Self {
            applications: 128,
            packages: 256,
            acquisitions: 1024,
            timers: 256,
            callbacks: 256,
            final_obligations: 512,
            state_bytes: 4 * 1024 * 1024,
        }
    }
}

impl ResidencyLimits {
    fn validate(&self) -> Result<()> {
        if [
            self.applications,
            self.packages,
            self.acquisitions,
            self.timers,
            self.callbacks,
            self.final_obligations,
            self.state_bytes,
        ]
        .contains(&0)
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

/// An admitted accounting premise. Names also supply first public-map provenance.
///
/// A different full version or numeric package instance is not an in-place
/// replacement. There is no physical removal/re-admission capability in H2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidencyAdmission {
    pub package: PackageInstance,
    pub version: VersionInstance,
    pub package_instance_id: i64,
    pub service_name: String,
    pub application_name: String,
}

/// Frozen exact original acquisition target, including the admitted full premise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionTarget {
    pub type_instance: TypeInstance,
    pub version: VersionInstance,
    pub package_instance_id: i64,
}

/// Exact nonclone ownership. Drop neither decrements nor removes its ledger record.
#[derive(Debug)]
#[must_use = "dropping an acquisition leaves bounded reconciliation ownership in its ledger"]
pub struct Acquisition {
    ledger: u64,
    id: u64,
    target: AcquisitionTarget,
}

impl Acquisition {
    /// Diagnostic identifier only; raw IDs cannot authorize mutation.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn target(&self) -> &AcquisitionTarget {
        &self.target
    }
}

/// Explicit intermediate package-first owner; not a fully acquired pair.
#[derive(Debug)]
#[must_use = "a beginning acquisition already owns package mutation"]
pub enum AcquisitionBegin {
    Beginning(Acquisition),
    Unmanaged,
    Rejected(Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionPhase {
    /// Package increment has happened; application increment is not yet attempted.
    Beginning,
    /// Application increment failed after package mutation; exact residual remains.
    PackageOnly,
    /// Both counts were incremented; release must verify application-first decrement.
    Paired,
}

/// Actual timer admission, separate from unused eligibility and physical End.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerAdmission {
    /// Timer and downstream callback/final capacity are reserved, not executed.
    Admitted { timer_id: u64 },
    /// Equal or newer captured sequence already owns the pending slot/deadline.
    AlreadyExists,
    /// Current applicable grace suppresses scheduling.
    InfiniteGrace,
    /// Controlled package-debug observation suppresses this request, not old timers.
    DebugSuppressed,
    /// New timers stopped; previously admitted work remains owned.
    QueueClosed,
    /// Entry/work bytes or timer/callback/final escrow capacity cannot be reserved.
    Capacity,
    /// Full current grace cannot be added to the observed time.
    TimeOverflow,
    /// Local work-ID allocation overflow, not package/application accounting sequence.
    SequenceOverflow,
}

/// Fixed-size latest-attempt diagnostic, independently of source eligibility.
///
/// Each entry preallocates one slot; later attempts overwrite it. This is not an
/// audit queue, timer owner, automatic retry obligation or cleanup proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulingAttempt {
    pub captured_sequence: u64,
    pub admission: TimerAdmission,
}

#[derive(Debug)]
pub struct ApplicationAccounting<'a> {
    pub identity: &'a ApplicationId,
    pub usage: u64,
    pub sequence: u64,
    /// None is native MaxValue (currently used), not an unknown release.
    pub unused_since: Option<i64>,
    pub closing: bool,
    pub last_schedule: Option<SchedulingAttempt>,
}

#[derive(Debug)]
pub struct PackageAccounting<'a> {
    pub identity: &'a PackageInstance,
    pub version: &'a VersionInstance,
    pub package_instance_id: i64,
    pub usage: u64,
    pub sequence: u64,
    pub unused_since: Option<i64>,
    pub closing: bool,
    pub debug_lock: bool,
    pub last_schedule: Option<SchedulingAttempt>,
}

#[derive(Debug)]
pub struct PublicActivationMapping<'a> {
    pub rendered_key: &'a str,
    pub public_activation: &'a str,
    pub service_name: &'a str,
    pub application_name: &'a str,
}

#[derive(Debug)]
pub struct AcquisitionObservation<'a> {
    pub id: u64,
    pub target: &'a AcquisitionTarget,
    pub phase: AcquisitionPhase,
    pub diagnostic: Option<&'a Error>,
}

/// Borrowed immutable premise; work never looks up a replacement description.
#[derive(Debug, Clone, Copy)]
pub enum ResidencySubject<'a> {
    Application(&'a ApplicationId),
    Package(&'a ResidencyAdmission),
}

#[derive(Debug)]
pub struct ResidencyWork<'a> {
    pub id: u64,
    pub captured_sequence: u64,
    pub due_ticks: i64,
    pub subject: ResidencySubject<'a>,
}

/// Nonclone callback ownership; its bounded record survives owner Drop/shutdown.
#[derive(Debug)]
#[must_use = "dropping a callback leaves its admitted record retained"]
pub struct DeactivationCallback {
    ledger: u64,
    id: u64,
}

impl DeactivationCallback {
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug)]
#[must_use = "retained callbacks still own admitted work"]
pub enum CallbackExecution {
    Stale,
    /// Guard passed; owned final work remains, without physical Begin/End.
    Deferred {
        obligation_id: u64,
    },
    Retained {
        callback: DeactivationCallback,
        error: Error,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidencyResources {
    pub applications: usize,
    pub packages: usize,
    pub public_mappings: usize,
    pub acquisitions: usize,
    pub timers: usize,
    pub callbacks: usize,
    pub final_obligations: usize,
    /// Includes owner copies and worst-case admitted callback/final escrow.
    pub logical_bytes: usize,
}

#[derive(Debug)]
struct ApplicationEntry {
    identity: ApplicationId,
    usage: u64,
    sequence: u64,
    unused_since: Option<i64>,
    closing: bool,
    last_schedule: Option<SchedulingAttempt>,
}

#[derive(Debug)]
struct PackageEntry {
    admission: ResidencyAdmission,
    application: usize,
    usage: u64,
    sequence: u64,
    unused_since: Option<i64>,
    closing: bool,
    debug_lock: bool,
    last_schedule: Option<SchedulingAttempt>,
}

#[derive(Debug)]
struct PublicMapEntry {
    rendered_key: String,
    public_activation: String,
    service_name: String,
    application_name: String,
}

#[derive(Debug)]
struct AcquisitionRecord {
    id: u64,
    package: usize,
    target: AcquisitionTarget,
    package_sequence: u64,
    phase: AcquisitionPhase,
    diagnostic: Option<Error>,
    charge: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Subject {
    Application(usize),
    Package(usize),
}

#[derive(Debug)]
struct Work {
    id: u64,
    subject: Subject,
    sequence: u64,
    due_ticks: i64,
}

/// Real deterministic accounting and narrow scheduling, with no lifecycle fake.
///
/// All transitions run under exclusive `&mut self`; no external executor/consumer
/// is invoked. Only fresh admission is supported, never claimed deployment.
#[derive(Debug)]
pub struct Residency {
    ledger: u64,
    limits: ResidencyLimits,
    config: ResidencyConfig,
    now: i64,
    opened: bool,
    queue_opened: bool,
    next_acquisition: u64,
    next_work: u64,
    logical_bytes: usize,
    applications: Vec<ApplicationEntry>,
    packages: Vec<PackageEntry>,
    public_map: Vec<PublicMapEntry>,
    acquisitions: Vec<AcquisitionRecord>,
    timers: Vec<Work>,
    callbacks: Vec<Work>,
    finals: Vec<Work>,
}

fn charge(parts: impl IntoIterator<Item = usize>) -> Result<usize> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total.checked_add(part).ok_or(Error::Capacity)
    })
}

impl Residency {
    /// Admit a fresh deterministic accounting ledger with finite nonzero budgets.
    ///
    /// Time is an explicit signed 100ns observation; finite grace must be
    /// nonnegative and not native MaxValue (use Infinite). Package admission
    /// certifies accounting only, never deployment/process existence. No restore
    /// loader, physical activation or End engine is supplied.
    pub fn new(
        limits: ResidencyLimits,
        config: ResidencyConfig,
        clock: &impl Clock,
    ) -> Result<Self> {
        limits.validate()?;
        config.validate()?;
        let ledger = NEXT_LEDGER
            .try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |id| {
                id.checked_add(1)
            })
            .map_err(|_| Error::NativeInvariant("residency ledger identity overflow"))?;
        Ok(Self {
            ledger,
            limits,
            config,
            now: clock.now_ticks(),
            opened: true,
            queue_opened: true,
            next_acquisition: 1,
            next_work: 1,
            logical_bytes: 0,
            applications: Vec::new(),
            packages: Vec::new(),
            public_map: Vec::new(),
            acquisitions: Vec::new(),
            timers: Vec::new(),
            callbacks: Vec::new(),
            finals: Vec::new(),
        })
    }

    fn require_open(&self) -> Result<()> {
        if self.opened {
            Ok(())
        } else {
            Err(Error::ObjectClosed)
        }
    }

    fn reserve_bytes(&self, additional: usize) -> Result<usize> {
        let total = self
            .logical_bytes
            .checked_add(additional)
            .ok_or(Error::Capacity)?;
        if total > self.limits.state_bytes {
            Err(Error::Capacity)
        } else {
            Ok(total)
        }
    }

    fn application_index(&self, identity: &ApplicationId) -> Result<usize> {
        self.applications
            .iter()
            .position(|entry| compare_applications(&entry.identity, identity) == Ordering::Equal)
            .ok_or(Error::NotFound)
    }

    fn package_index(&self, identity: &PackageInstance) -> Result<usize> {
        validate_package(&identity.package)?;
        validate_public(identity.activation, &identity.public_activation)?;
        let index = self
            .packages
            .iter()
            .position(|entry| {
                compare_package_instances(&entry.admission.package, identity) == Ordering::Equal
            })
            .ok_or(Error::NotFound)?;
        if self.packages[index].admission.package.public_activation != identity.public_activation {
            return Err(Error::NativeInvariant(
                "package public activation conflicts with admitted identity",
            ));
        }
        Ok(index)
    }

    fn accessible_package(&self, identity: &PackageInstance) -> Result<usize> {
        self.require_open()?;
        let index = self.package_index(identity)?;
        if self.applications[self.packages[index].application].closing {
            return Err(Error::InvalidState);
        }
        Ok(index)
    }

    /// Atomically admit application/package accounting and native public mapping.
    ///
    /// Entries start unused at current Now, sequence/count zero. All count/byte
    /// checks precede mutation; equivalent re-admission preserves first spelling.
    pub fn admit_package(&mut self, admission: ResidencyAdmission) -> Result<()> {
        self.require_open()?;
        validate_package(&admission.package.package)?;
        validate_public(
            admission.package.activation,
            &admission.package.public_activation,
        )?;
        if admission.package.package.application == ApplicationId::default() {
            return Err(Error::UnsupportedCapability);
        }
        if admission.package.activation == Activation::SHARED
            && !admission.package.public_activation.is_empty()
        {
            return Err(Error::InvalidInput);
        }
        let existing_app = self
            .application_index(&admission.package.package.application)
            .ok();
        if existing_app.is_some_and(|index| self.applications[index].closing) {
            return Err(Error::InvalidState);
        }
        let existing_package = self.packages.iter().position(|entry| {
            compare_package_instances(&entry.admission.package, &admission.package)
                == Ordering::Equal
        });
        if let Some(index) = existing_package {
            let entry = &self.packages[index];
            if entry.admission.package.public_activation != admission.package.public_activation {
                return Err(Error::NativeInvariant(
                    "package public activation conflicts with admitted identity",
                ));
            }
            if entry.closing
                || entry.admission.version != admission.version
                || entry.admission.package_instance_id != admission.package_instance_id
            {
                return Err(Error::InvalidState);
            }
        }
        let map_key = if admission.package.activation == Activation::SHARED {
            None
        } else {
            Some(public_activation_map_key(
                &admission.package.package,
                admission.package.activation,
            )?)
        };
        let existing_map = map_key.as_ref().and_then(|key| {
            self.public_map
                .iter()
                .position(|entry| compare_names(&entry.rendered_key, key) == Ordering::Equal)
        });
        if let Some(index) = existing_map {
            if self.public_map[index].public_activation != admission.package.public_activation {
                return Err(Error::NativeInvariant(
                    "rendered public activation mapping conflicts",
                ));
            }
        } else if map_key.is_some() && admission.service_name.is_empty() {
            return Err(Error::NativeInvariant(
                "new exclusive mapping requires service name",
            ));
        }
        if existing_package.is_some() {
            return Ok(());
        }
        if self.packages.len() >= self.limits.packages
            || (existing_app.is_none() && self.applications.len() >= self.limits.applications)
        {
            return Err(Error::Capacity);
        }
        let package_charge = charge([
            160,
            admission.package.package.application.type_name.len(),
            admission.package.package.name.len(),
            admission.package.public_activation.len(),
            admission.service_name.len(),
            admission.application_name.len(),
        ])?;
        let app_charge = if existing_app.is_none() {
            charge([96, admission.package.package.application.type_name.len()])?
        } else {
            0
        };
        let map_charge = if existing_map.is_none() {
            match &map_key {
                Some(key) => charge([
                    96,
                    key.len(),
                    admission.package.public_activation.len(),
                    admission.service_name.len(),
                    admission.application_name.len(),
                ])?,
                None => 0,
            }
        } else {
            0
        };
        let total = self.reserve_bytes(charge([package_charge, app_charge, map_charge])?)?;
        let application = existing_app.unwrap_or(self.applications.len());
        if existing_app.is_none() {
            self.applications.push(ApplicationEntry {
                identity: admission.package.package.application.clone(),
                usage: 0,
                sequence: 0,
                unused_since: Some(self.now),
                closing: false,
                last_schedule: None,
            });
        }
        if existing_map.is_none() {
            if let Some(rendered_key) = map_key {
                self.public_map.push(PublicMapEntry {
                    rendered_key,
                    public_activation: admission.package.public_activation.clone(),
                    service_name: admission.service_name.clone(),
                    application_name: admission.application_name.clone(),
                });
            }
        }
        self.packages.push(PackageEntry {
            admission,
            application,
            usage: 0,
            sequence: 0,
            unused_since: Some(self.now),
            closing: false,
            debug_lock: false,
            last_schedule: None,
        });
        self.logical_bytes = total;
        Ok(())
    }

    /// Observe monotonic time, without automatically running timers or scans.
    pub fn advance(&mut self, clock: &impl Clock) -> Result<()> {
        let now = clock.now_ticks();
        if now < self.now {
            return Err(Error::InvalidInput);
        }
        self.now = now;
        Ok(())
    }

    /// Replace configuration for subsequent scheduling attempts while open.
    ///
    /// Existing timer deadlines are unchanged; equal-sequence scheduling does
    /// not replace them. Callback guards do not recheck grace. Native dynamic
    /// observations: Deactivator.cpp:912–969,1264–1313.
    pub fn set_config(&mut self, config: ResidencyConfig) -> Result<()> {
        self.require_open()?;
        config.validate()?;
        self.config = config;
        Ok(())
    }

    /// Supply a controlled debug-lock observation for future package requests.
    ///
    /// Does not inspect a process/filesystem or cancel admitted timers. Callback
    /// guards do not recheck debug suppression; application scheduling is
    /// unaffected. Closed/missing application access remains a checked failure.
    pub fn set_debug_lock(&mut self, package: &PackageInstance, debug_lock: bool) -> Result<()> {
        let index = self.accessible_package(package)?;
        self.packages[index].debug_lock = debug_lock;
        Ok(())
    }

    /// Package-first native mutation with a pre-reserved exact intermediate owner.
    ///
    /// Deactivator.cpp:37-68,330-367. Only `finish_acquisition` increments the
    /// application, allowing a real deterministic application-closing interleaving.
    pub fn begin_acquisition(&mut self, target: &TypeInstance) -> AcquisitionBegin {
        match self.begin_acquisition_inner(target) {
            Ok(Some(owner)) => AcquisitionBegin::Beginning(owner),
            Ok(None) => AcquisitionBegin::Unmanaged,
            Err(error) => AcquisitionBegin::Rejected(error),
        }
    }

    fn begin_acquisition_inner(&mut self, target: &TypeInstance) -> Result<Option<Acquisition>> {
        self.require_open()?;
        validate_package(&target.service_type.package)?;
        if target.service_type.package.application == ApplicationId::default() {
            return Ok(None);
        }
        let package = self.accessible_package(&target.package_instance())?;
        let entry = &self.packages[package];
        if entry.closing {
            return Err(Error::InvalidState);
        }
        let sequence = entry
            .sequence
            .checked_add(1)
            .ok_or(Error::NativeInvariant(SEQUENCE_OVERFLOW))?;
        let usage = entry
            .usage
            .checked_add(1)
            .ok_or(Error::NativeInvariant("package usage overflow"))?;
        if self.acquisitions.len() >= self.limits.acquisitions {
            return Err(Error::Capacity);
        }
        let next_id = self
            .next_acquisition
            .checked_add(1)
            .ok_or(Error::NativeInvariant("acquisition identity overflow"))?;
        let target_bytes = charge([
            target.service_type.package.application.type_name.len(),
            target.service_type.package.name.len(),
            target.service_type.name.len(),
            target.public_activation.len(),
        ])?;
        let owner_charge = charge([160, target_bytes, target_bytes])?;
        let total = self.reserve_bytes(owner_charge)?;
        let exact = AcquisitionTarget {
            type_instance: target.clone(),
            version: entry.admission.version.clone(),
            package_instance_id: entry.admission.package_instance_id,
        };
        let id = self.next_acquisition;
        self.acquisitions.push(AcquisitionRecord {
            id,
            package,
            target: exact.clone(),
            package_sequence: sequence,
            phase: AcquisitionPhase::Beginning,
            diagnostic: None,
            charge: owner_charge,
        });
        self.next_acquisition = next_id;
        self.logical_bytes = total;
        let entry = &mut self.packages[package];
        let first = entry.usage == 0;
        entry.usage = usage;
        entry.sequence = sequence;
        if first {
            entry.unused_since = None;
            self.remove_timer(Subject::Package(package), sequence);
        }
        Ok(Some(Acquisition {
            ledger: self.ledger,
            id,
            target: exact,
        }))
    }

    fn owner_index(&self, owner: &Acquisition) -> Result<usize> {
        if owner.ledger != self.ledger {
            return Err(Error::InvalidInput);
        }
        self.acquisitions
            .iter()
            .position(|record| record.id == owner.id && record.target == owner.target)
            .ok_or(Error::InvalidState)
    }

    /// Complete only the original beginning owner; late failure retains package-only ownership.
    pub fn finish_acquisition(&mut self, owner: Acquisition) -> UsageIncrement<Acquisition> {
        let result = self.finish_acquisition_inner(&owner);
        match result {
            Ok(()) => UsageIncrement::Acquired(owner),
            Err(error) => UsageIncrement::Unresolved {
                error,
                ownership: owner,
            },
        }
    }

    fn finish_acquisition_inner(&mut self, owner: &Acquisition) -> Result<()> {
        let record = self.owner_index(owner)?;
        if self.acquisitions[record].phase != AcquisitionPhase::Beginning {
            return Err(Error::InvalidState);
        }
        let package = self.acquisitions[record].package;
        let application = self.packages[package].application;
        let result = self.require_open().and_then(|()| {
            let entry = &self.applications[application];
            if entry.closing {
                return Err(Error::InvalidState);
            }
            let sequence = entry.sequence.checked_add(1).ok_or(Error::NativeInvariant(
                "application accounting sequence overflow",
            ))?;
            let usage = entry
                .usage
                .checked_add(1)
                .ok_or(Error::NativeInvariant("application usage overflow"))?;
            Ok((sequence, usage))
        });
        let (sequence, usage) = match result {
            Ok(values) => values,
            Err(error) => {
                self.acquisitions[record].phase = AcquisitionPhase::PackageOnly;
                self.acquisitions[record].diagnostic = Some(error.clone());
                return Err(error);
            }
        };
        let entry = &mut self.applications[application];
        let first = entry.usage == 0;
        entry.sequence = sequence;
        entry.usage = usage;
        if first {
            entry.unused_since = None;
            self.remove_timer(Subject::Application(application), sequence);
        }
        self.acquisitions[record].phase = AcquisitionPhase::Paired;
        Ok(())
    }

    fn accounting_is_owned(&self, package: usize) -> bool {
        let application = self.packages[package].application;
        let package_owners = self
            .acquisitions
            .iter()
            .filter(|record| record.package == package)
            .count();
        let application_owners = self
            .acquisitions
            .iter()
            .filter(|record| {
                self.packages[record.package].application == application
                    && record.phase == AcquisitionPhase::Paired
            })
            .count();
        u64::try_from(package_owners).ok() == Some(self.packages[package].usage)
            && u64::try_from(application_owners).ok() == Some(self.applications[application].usage)
    }

    fn release_inner(&mut self, owner: &Acquisition, package_only: bool) -> Result<()> {
        let record = self.owner_index(owner)?;
        self.require_open()?;
        let expected = if package_only {
            AcquisitionPhase::PackageOnly
        } else {
            AcquisitionPhase::Paired
        };
        if self.acquisitions[record].phase != expected {
            return Err(Error::InvalidState);
        }
        let package = self.acquisitions[record].package;
        let application = self.packages[package].application;
        let p = &self.packages[package];
        let a = &self.applications[application];
        if p.closing || (!package_only && a.closing) {
            return Err(Error::InvalidState);
        }
        if !self.accounting_is_owned(package) || p.usage == 0 || (!package_only && a.usage == 0) {
            return Err(Error::NativeInvariant(
                "acquisition accounting is not verified",
            ));
        }
        if package_only && p.sequence != self.acquisitions[record].package_sequence {
            return Err(Error::NativeInvariant(
                "package-only compensation premise changed",
            ));
        }
        let package_sequence = p
            .sequence
            .checked_add(1)
            .ok_or(Error::NativeInvariant(SEQUENCE_OVERFLOW))?;
        let application_sequence = if package_only {
            a.sequence
        } else {
            a.sequence.checked_add(1).ok_or(Error::NativeInvariant(
                "application accounting sequence overflow",
            ))?
        };
        // Deactivator.cpp:369-394. Preflight protects native assert/overflow paths,
        // then verified pair decrement mutates application before package.
        if !package_only {
            let a = &mut self.applications[application];
            a.sequence = application_sequence;
            a.usage -= 1;
            if a.usage == 0 {
                a.unused_since = Some(self.now);
            }
        }
        let p = &mut self.packages[package];
        p.sequence = package_sequence;
        p.usage -= 1;
        let last = p.usage == 0;
        if last {
            p.unused_since = Some(self.now);
        }
        let record = self.acquisitions.remove(record);
        self.logical_bytes -= record.charge;
        if last {
            // Void scheduling may fail: the preallocated entry diagnostic retains
            // this exact sequence, without falsifying locally verified decrement.
            self.schedule_subject(Subject::Package(package), package_sequence);
        }
        Ok(())
    }

    fn release_observation(
        &mut self,
        owner: Acquisition,
        package_only: bool,
    ) -> UsageRelease<Acquisition> {
        match self.release_inner(&owner, package_only) {
            Ok(()) => UsageRelease::Released,
            Err(error) => {
                if let Ok(index) = self.owner_index(&owner) {
                    self.acquisitions[index].diagnostic = Some(error.clone());
                }
                UsageRelease::Unresolved {
                    ownership: owner,
                    diagnostic: Some(error),
                }
            }
        }
    }

    /// Explicit local package-only compensation, never a normal pair decrement.
    ///
    /// Requires that exact residual, unchanged package accounting sequence,
    /// verified owner counts, no full application increment and nonclosing package.
    /// A closing application is allowed because no application decrement occurs.
    /// Anything uncertain returns the same owner; no removal/reopening is invented.
    pub fn compensate_package_only(&mut self, owner: Acquisition) -> UsageRelease<Acquisition> {
        self.release_observation(owner, true)
    }

    fn grace(&self, subject: Subject) -> GracePeriod {
        match subject {
            Subject::Application(_) => self.config.shared_grace,
            Subject::Package(index)
                if self.packages[index].admission.package.activation != Activation::SHARED =>
            {
                self.config.exclusive_grace
            }
            Subject::Package(_) => self.config.shared_grace,
        }
    }

    fn schedule_subject(&mut self, subject: Subject, sequence: u64) -> TimerAdmission {
        let admission = self.admit_timer(subject, sequence);
        let attempt = Some(SchedulingAttempt {
            captured_sequence: sequence,
            admission,
        });
        match subject {
            Subject::Application(index) => self.applications[index].last_schedule = attempt,
            Subject::Package(index) => self.packages[index].last_schedule = attempt,
        }
        admission
    }

    fn admit_timer(&mut self, subject: Subject, sequence: u64) -> TimerAdmission {
        let GracePeriod::Finite(grace) = self.grace(subject) else {
            return TimerAdmission::InfiniteGrace;
        };
        if let Subject::Package(index) = subject {
            if self.packages[index].debug_lock {
                return TimerAdmission::DebugSuppressed;
            }
        }
        if !self.queue_opened || !self.opened {
            return TimerAdmission::QueueClosed;
        }
        let existing = self
            .timers
            .iter()
            .position(|timer| timer.subject == subject);
        if existing.is_some_and(|index| self.timers[index].sequence >= sequence) {
            return TimerAdmission::AlreadyExists;
        }
        let Some(due_ticks) = self.now.checked_add(grace) else {
            return TimerAdmission::TimeOverflow;
        };
        let Some(next_id) = self.next_work.checked_add(1) else {
            return TimerAdmission::SequenceOverflow;
        };
        let total = if existing.is_none() {
            if self.timers.len() >= self.limits.timers
                || self.timers.len() + self.callbacks.len() >= self.limits.callbacks
                || self.timers.len() + self.callbacks.len() + self.finals.len()
                    >= self.limits.final_obligations
            {
                return TimerAdmission::Capacity;
            }
            match self.reserve_bytes(WORK_CHARGE) {
                Ok(total) => total,
                Err(_) => return TimerAdmission::Capacity,
            }
        } else {
            self.logical_bytes
        };
        let id = self.next_work;
        let timer = Work {
            id,
            subject,
            sequence,
            due_ticks,
        };
        if let Some(index) = existing {
            self.timers[index] = timer;
        } else {
            self.timers.push(timer);
        }
        self.next_work = next_id;
        self.logical_bytes = total;
        TimerAdmission::Admitted { timer_id: id }
    }

    /// Queue diagnostic seam with a captured package sequence, not a universal fence.
    ///
    /// Strictly newer sequences replace older pending timers; current accounting
    /// is checked separately when the callback executes (Deactivator.cpp:735-815).
    pub fn schedule_package_timer(
        &mut self,
        package: &PackageInstance,
        captured_sequence: u64,
    ) -> Result<TimerAdmission> {
        let index = self.accessible_package(package)?;
        if self.packages[index].closing {
            return Err(Error::InvalidState);
        }
        Ok(self.schedule_subject(Subject::Package(index), captured_sequence))
    }

    /// Admit captured application-sequence work, not an unused eligibility check.
    ///
    /// Can admit while usage is nonzero. Execution separately checks matching
    /// sequence, zero usage and not closing (not an empty-package-map condition).
    /// Admission reserves retained callback/final work, not physical completion.
    pub fn schedule_application_timer(
        &mut self,
        application: &ApplicationId,
        captured_sequence: u64,
    ) -> Result<TimerAdmission> {
        self.require_open()?;
        let index = self.application_index(application)?;
        if self.applications[index].closing {
            return Err(Error::InvalidState);
        }
        Ok(self.schedule_subject(Subject::Application(index), captured_sequence))
    }

    fn remove_timer(&mut self, subject: Subject, incoming_sequence: u64) -> bool {
        if let Some(index) = self
            .timers
            .iter()
            .position(|timer| timer.subject == subject && timer.sequence <= incoming_sequence)
        {
            self.timers.remove(index);
            self.logical_bytes -= WORK_CHARGE;
            true
        } else {
            false
        }
    }

    /// Native ≤ removal; a stale cancellation cannot erase newer work.
    pub fn cancel_package_timer(
        &mut self,
        package: &PackageInstance,
        incoming_sequence: u64,
    ) -> Result<bool> {
        self.require_open()?;
        let index = self.package_index(package)?;
        Ok(self.remove_timer(Subject::Package(index), incoming_sequence))
    }

    pub fn cancel_application_timer(
        &mut self,
        application: &ApplicationId,
        incoming_sequence: u64,
    ) -> Result<bool> {
        self.require_open()?;
        let index = self.application_index(application)?;
        Ok(self.remove_timer(Subject::Application(index), incoming_sequence))
    }

    /// Run one explicit unused scan at current time; infinity suppresses scanning.
    ///
    /// Application selection precedes package scans and does not fall through
    /// when application grace is infinite (Deactivator.cpp:445-488). Scheduling
    /// uses the full current applicable grace. Returns selected entries, not
    /// admitted timers or physical deactivations.
    pub fn scan_unused(&mut self) -> Result<usize> {
        self.require_open()?;
        let GracePeriod::Finite(interval) = self.config.unused_scan_interval else {
            return Ok(0);
        };
        let lower_bound = self
            .now
            .checked_sub(interval)
            .ok_or(Error::NativeInvariant("unused scan time overflow"))?;
        let mut applications: Vec<usize> = (0..self.applications.len()).collect();
        applications.sort_by(|a, b| {
            compare_applications(
                &self.applications[*a].identity,
                &self.applications[*b].identity,
            )
        });
        let mut selected = 0;
        for application in applications {
            let entry = &self.applications[application];
            if entry.closing {
                continue;
            }
            if entry.usage == 0 && entry.unused_since.is_some_and(|since| since <= lower_bound) {
                let sequence = entry.sequence;
                self.schedule_subject(Subject::Application(application), sequence);
                selected += 1;
            } else {
                let mut packages: Vec<usize> = self
                    .packages
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| entry.application == application)
                    .map(|(index, _)| index)
                    .collect();
                packages.sort_by(|a, b| {
                    compare_package_instances(
                        &self.packages[*a].admission.package,
                        &self.packages[*b].admission.package,
                    )
                });
                for package in packages {
                    let entry = &self.packages[package];
                    if !entry.closing
                        && entry.usage == 0
                        && entry.unused_since.is_some_and(|since| since <= lower_bound)
                    {
                        let sequence = entry.sequence;
                        self.schedule_subject(Subject::Package(package), sequence);
                        selected += 1;
                    }
                }
            }
        }
        Ok(selected)
    }

    /// Dequeue one due timer into its already reserved bounded callback record.
    ///
    /// An already dequeued callback may race reacquisition/newer queue admission.
    /// Execution therefore repeats ≤ queue removal independently of its guard.
    pub fn dequeue_due(&mut self) -> Result<Option<DeactivationCallback>> {
        self.require_open()?;
        let index = self
            .timers
            .iter()
            .enumerate()
            .filter(|(_, timer)| timer.due_ticks <= self.now)
            .min_by_key(|(_, timer)| (timer.due_ticks, timer.id))
            .map(|(index, _)| index);
        let Some(index) = index else {
            return Ok(None);
        };
        if self.callbacks.len() >= self.limits.callbacks {
            return Err(Error::Capacity);
        }
        let timer = self.timers.remove(index);
        let id = timer.id;
        self.callbacks.push(timer);
        Ok(Some(DeactivationCallback {
            ledger: self.ledger,
            id,
        }))
    }

    /// Guard current accounting using the callback's captured exact sequence.
    ///
    /// Package access additionally needs a nonclosing application. Application
    /// guard permits remaining packages. Queue-removal success is not a guard
    /// (Deactivator.cpp:148-166,504-527,1078-1100,1372-1394).
    pub fn execute_callback(&mut self, callback: DeactivationCallback) -> CallbackExecution {
        let result = self.execute_callback_inner(&callback);
        match result {
            Ok(Some(id)) => CallbackExecution::Deferred { obligation_id: id },
            Ok(None) => CallbackExecution::Stale,
            Err(error) => CallbackExecution::Retained { callback, error },
        }
    }

    fn execute_callback_inner(&mut self, callback: &DeactivationCallback) -> Result<Option<u64>> {
        if callback.ledger != self.ledger {
            return Err(Error::InvalidInput);
        }
        let index = self
            .callbacks
            .iter()
            .position(|work| work.id == callback.id)
            .ok_or(Error::InvalidState)?;
        self.require_open()?;
        let subject = self.callbacks[index].subject;
        let sequence = self.callbacks[index].sequence;
        self.remove_timer(subject, sequence);
        let matches = match subject {
            Subject::Application(index) => {
                let entry = &self.applications[index];
                !entry.closing && entry.usage == 0 && entry.sequence == sequence
            }
            Subject::Package(index) => {
                let entry = &self.packages[index];
                !self.applications[entry.application].closing
                    && !entry.closing
                    && entry.usage == 0
                    && entry.sequence == sequence
            }
        };
        if !matches {
            self.callbacks.remove(index);
            self.logical_bytes -= WORK_CHARGE;
            return Ok(None);
        }
        if self.finals.len() >= self.limits.final_obligations {
            return Err(Error::Capacity);
        }
        match subject {
            Subject::Application(index) => self.applications[index].closing = true,
            Subject::Package(index) => self.packages[index].closing = true,
        }
        let work = self.callbacks.remove(index);
        let id = work.id;
        self.finals.push(work);
        Ok(Some(id))
    }

    /// Stop new timer admission without clearing admitted timers/callbacks.
    pub fn close_timer_queue(&mut self) {
        self.queue_opened = false;
    }

    /// Stop fresh admissions and mutation, retaining all exact ownership/work.
    ///
    /// Diagnostic inspection remains available. This is not a release,
    /// reconciliation, successful physical shutdown or asynchronous cleanup receipt.
    pub fn shutdown(&mut self) {
        self.opened = false;
        self.queue_opened = false;
    }

    pub fn resources(&self) -> ResidencyResources {
        ResidencyResources {
            applications: self.applications.len(),
            packages: self.packages.len(),
            public_mappings: self.public_map.len(),
            acquisitions: self.acquisitions.len(),
            timers: self.timers.len(),
            callbacks: self.callbacks.len(),
            final_obligations: self.finals.len(),
            logical_bytes: self.logical_bytes,
        }
    }

    /// Inspect retained accounting even after application/core closing.
    pub fn package_accounting(&self, package: &PackageInstance) -> Result<PackageAccounting<'_>> {
        let entry = &self.packages[self.package_index(package)?];
        Ok(PackageAccounting {
            identity: &entry.admission.package,
            version: &entry.admission.version,
            package_instance_id: entry.admission.package_instance_id,
            usage: entry.usage,
            sequence: entry.sequence,
            unused_since: entry.unused_since,
            closing: entry.closing,
            debug_lock: entry.debug_lock,
            last_schedule: entry.last_schedule,
        })
    }

    pub fn application_accounting(
        &self,
        application: &ApplicationId,
    ) -> Result<ApplicationAccounting<'_>> {
        let entry = &self.applications[self.application_index(application)?];
        Ok(ApplicationAccounting {
            identity: &entry.identity,
            usage: entry.usage,
            sequence: entry.sequence,
            unused_since: entry.unused_since,
            closing: entry.closing,
            last_schedule: entry.last_schedule,
        })
    }

    /// Borrow first-inserted rendered-CI map provenance, not structured identity.
    pub fn public_activation_mapping(
        &self,
        package: &PackageInstance,
    ) -> Result<PublicActivationMapping<'_>> {
        if package.activation == Activation::SHARED {
            return Err(Error::NotFound);
        }
        let key = public_activation_map_key(&package.package, package.activation)?;
        let entry = self
            .public_map
            .iter()
            .find(|entry| compare_names(&entry.rendered_key, &key) == Ordering::Equal)
            .ok_or(Error::NotFound)?;
        Ok(PublicActivationMapping {
            rendered_key: &entry.rendered_key,
            public_activation: &entry.public_activation,
            service_name: &entry.service_name,
            application_name: &entry.application_name,
        })
    }

    pub fn acquisition(&self, id: u64) -> Option<AcquisitionObservation<'_>> {
        self.acquisitions
            .iter()
            .find(|record| record.id == id)
            .map(Self::owner_view)
    }

    pub fn acquisitions(&self) -> impl Iterator<Item = AcquisitionObservation<'_>> {
        self.acquisitions.iter().map(Self::owner_view)
    }

    fn owner_view(record: &AcquisitionRecord) -> AcquisitionObservation<'_> {
        AcquisitionObservation {
            id: record.id,
            target: &record.target,
            phase: record.phase,
            diagnostic: record.diagnostic.as_ref(),
        }
    }

    fn work_view<'a>(&'a self, work: &Work) -> ResidencyWork<'a> {
        ResidencyWork {
            id: work.id,
            captured_sequence: work.sequence,
            due_ticks: work.due_ticks,
            subject: match work.subject {
                Subject::Application(index) => {
                    ResidencySubject::Application(&self.applications[index].identity)
                }
                Subject::Package(index) => {
                    ResidencySubject::Package(&self.packages[index].admission)
                }
            },
        }
    }

    pub fn pending_timers(&self) -> impl Iterator<Item = ResidencyWork<'_>> {
        self.timers.iter().map(|work| self.work_view(work))
    }

    pub fn callbacks(&self) -> impl Iterator<Item = ResidencyWork<'_>> {
        self.callbacks.iter().map(|work| self.work_view(work))
    }

    /// Retained guard-passed work; there is deliberately no physical End method.
    pub fn final_obligations(&self) -> impl Iterator<Item = ResidencyWork<'_>> {
        self.finals.iter().map(|work| self.work_view(work))
    }
}

impl PackageUsage for Residency {
    type Acquisition = Acquisition;

    fn increment_usage(&mut self, target: &TypeInstance) -> UsageIncrement<Self::Acquisition> {
        match self.begin_acquisition(target) {
            AcquisitionBegin::Beginning(owner) => self.finish_acquisition(owner),
            AcquisitionBegin::Unmanaged => UsageIncrement::Unmanaged,
            AcquisitionBegin::Rejected(error) => UsageIncrement::Rejected(error),
        }
    }

    fn decrement_usage(
        &mut self,
        acquisition: Self::Acquisition,
    ) -> UsageRelease<Self::Acquisition> {
        self.release_observation(acquisition, false)
    }
}

impl PackageScheduling for Residency {
    fn schedule_deactivation_if_unused(
        &mut self,
        package: &PackageInstance,
    ) -> SchedulingDisposition {
        let index = match self.accessible_package(package) {
            Ok(index) => index,
            Err(Error::ObjectClosed) => return SchedulingDisposition::ObjectClosed,
            Err(Error::NotFound) => return SchedulingDisposition::NotFound,
            Err(_) => return SchedulingDisposition::InvalidState,
        };
        let entry = &self.packages[index];
        // Deactivator.cpp:113-145 returns InvalidState before writing eligibility
        // on closing. Success(false) would incorrectly grant hasReplica.
        if entry.closing {
            return SchedulingDisposition::InvalidState;
        }
        // Deactivator.cpp:113-145 sets eligibility before the void scheduling
        // routine: debug/duplicate/queue/budget suppression cannot retroactively
        // change the native Success{scheduled:true} lookup disposition.
        let eligible = entry.usage == 0
            && matches!(self.grace(Subject::Package(index)), GracePeriod::Finite(_));
        if eligible {
            let sequence = entry.sequence;
            self.schedule_subject(Subject::Package(index), sequence);
        }
        SchedulingDisposition::Success {
            scheduled: eligible,
        }
    }
}

#[cfg(test)]
mod sequence_tests {
    use super::*;

    fn ledger() -> (Residency, PackageInstance) {
        let package = PackageInstance {
            package: PackageId {
                application: ApplicationId {
                    type_name: "App".into(),
                    number: 1,
                },
                name: "Pkg".into(),
            },
            ..PackageInstance::default()
        };
        let mut ledger = Residency::new(
            ResidencyLimits::default(),
            ResidencyConfig::default(),
            &ManualClock(0),
        )
        .unwrap();
        ledger
            .admit_package(ResidencyAdmission {
                package: package.clone(),
                version: VersionInstance::default(),
                package_instance_id: 1,
                service_name: String::new(),
                application_name: String::new(),
            })
            .unwrap();
        (ledger, package)
    }

    fn target(package: PackageInstance) -> TypeInstance {
        TypeInstance {
            service_type: ServiceTypeId {
                package: package.package,
                name: "Type".into(),
            },
            ..TypeInstance::default()
        }
    }

    #[test]
    fn package_sequence_overflow_rejects_before_mutation_or_reservation() {
        // Local checked-arithmetic boundary; native Deactivator.cpp:37-68 uses uint64.
        let (mut ledger, package) = ledger();
        ledger.packages[0].sequence = u64::MAX;
        let resources = ledger.resources();
        assert!(matches!(
            ledger.increment_usage(&target(package)),
            UsageIncrement::Rejected(Error::NativeInvariant(SEQUENCE_OVERFLOW))
        ));
        assert_eq!(ledger.resources(), resources);
        assert_eq!(ledger.packages[0].usage, 0);
    }

    #[test]
    fn application_sequence_overflow_preserves_package_first_residual() {
        // Local overflow handling combines the native package-first ordering with exact ownership.
        let (mut ledger, package) = ledger();
        ledger.applications[0].sequence = u64::MAX;
        let owner = match ledger.increment_usage(&target(package)) {
            UsageIncrement::Unresolved { ownership, .. } => ownership,
            other => panic!("{other:?}"),
        };
        assert_eq!(ledger.packages[0].usage, 1);
        assert_eq!(ledger.applications[0].usage, 0);
        assert_eq!(
            ledger.acquisition(owner.id()).unwrap().phase,
            AcquisitionPhase::PackageOnly
        );
        assert!(matches!(
            ledger.compensate_package_only(owner),
            UsageRelease::Released
        ));
        assert_eq!(ledger.applications[0].sequence, u64::MAX);
    }
}
