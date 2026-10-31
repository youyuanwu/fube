//! Concrete, bounded TEST-PROFILE composition, not a live node/Hosting adapter.
//! Native processor authority (SF 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d):
//! Hosting.HostingAdapter.cpp:83-203; ServiceTypeRegistrationWrapper.cpp:78-162;
//! Infrastructure.EntityJobItem.h:228-278. Exact tokens, retained terminal IDs,
//! receipt serialization and finite escrow are stronger local Rust safety rules.
use fube_hosting as hosting;
use fube_hosting::PackageUsage;
use fube_ra::{entity as ra, protocol as wire};

const WORK_SLOTS: usize = 32;
const FIXED_ESCROW: usize = 2048;

#[derive(Clone)]
pub struct Issued {
    pub token: u64,
    pub lookup: u64,
    pub body: std::sync::Arc<wire::AddInstance>,
    pub usage: Option<u64>,
}

impl Issued {
    pub fn from_effect(effect: &ra::Effect) -> Self {
        match effect {
            ra::Effect::Open {
                token,
                lookup,
                body,
                usage,
            }
            | ra::Effect::FindRegistration {
                token,
                lookup,
                body,
                usage,
            } => Self {
                token: *token,
                lookup: *lookup,
                body: body.clone(),
                usage: *usage,
            },
            _ => panic!("an actual RA issued registration effect is required"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 16,
            bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Capacity,
    Closed,
    Duplicate,
    MissingOwner,
    Outstanding,
    Terminal,
    InvalidResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    Offered,
    /// RA consumed an issued fresh offer; its authoritative disposal may be queued.
    DisposalPending,
    Adopted,
    Reuses(u64),
    PackageOnly,
    Released,
    Abandoned,
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Diagnostic {
    IncrementFailed,
    SubmitRejected,
    PublicationFailed,
    ReleaseUnresolved,
}

pub struct Offer {
    pub issued: Issued,
    pub snapshot: hosting::LookupResult,
    pub target: hosting::AcquisitionTarget,
    pub completion: ra::RegistrationCompletion,
    pub serialized: Vec<u8>,
    pub ownership: Ownership,
    pub acquisition: Option<hosting::Acquisition>,
    pub original_ticket: Option<ra::Ticket>,
    pub correction_ticket: Option<ra::Ticket>,
    pub work: Vec<ra::Dispatched>,
    pub diagnostic: Option<Diagnostic>,
    pub release_attempts: u8,
    pub releases: u8,
    pub terminal: bool,
    accepted_issued: bool,
    work_bytes: usize,
    work_escrow: usize,
}

impl Offer {
    pub fn ticket(&self) -> Option<&ra::Ticket> {
        self.correction_ticket
            .as_ref()
            .or(self.original_ticket.as_ref())
    }
}

pub struct CloseWork {
    pub entity: [u8; 16],
    pub usage: u64,
    pub proposal: ra::CloseCompletion,
    pub serialized: Vec<u8>,
    pub before: std::sync::Arc<ra::EntityState>,
    pub ticket: Option<ra::Ticket>,
    pub work: Vec<ra::Dispatched>,
    work_bytes: usize,
    work_escrow: usize,
}

pub struct Bridge {
    pub residency: hosting::Residency,
    limits: Limits,
    accepting: bool,
    bytes: usize,
    offers: Vec<Offer>,
    closes: Vec<CloseWork>,
    // Reuses the downstream count/byte escrow reserved before each offer.
    // A fallible classification cannot drop an already drained action batch.
    incoming: Vec<ra::Dispatched>,
}

impl Bridge {
    pub fn new(residency: hosting::Residency, limits: Limits) -> Self {
        assert!(limits.records > 0 && limits.bytes > 0);
        Self {
            residency,
            limits,
            accepting: true,
            bytes: 0,
            offers: Vec::new(),
            closes: Vec::new(),
            incoming: Vec::new(),
        }
    }

    pub fn records(&self) -> usize {
        self.offers.len() + self.closes.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    fn reserve(&mut self, charge: usize) -> Result<(), Error> {
        if !self.accepting {
            return Err(Error::Closed);
        }
        let bytes = self.bytes.checked_add(charge).ok_or(Error::Capacity)?;
        if self.records() >= self.limits.records || bytes > self.limits.bytes {
            return Err(Error::Capacity);
        }
        self.bytes = bytes;
        Ok(())
    }

    pub fn offer(&self, lookup: u64) -> &Offer {
        self.offers
            .iter()
            .find(|offer| offer.issued.lookup == lookup)
            .expect("retained exact lookup")
    }

    fn offer_mut(&mut self, lookup: u64) -> &mut Offer {
        self.offers
            .iter_mut()
            .find(|offer| offer.issued.lookup == lookup)
            .expect("retained exact lookup")
    }

    fn projected(
        &self,
        issued: &Issued,
        snapshot: &hosting::LookupResult,
    ) -> Result<
        (
            hosting::AcquisitionTarget,
            ra::RegistrationCompletion,
            usize,
        ),
        Error,
    > {
        if self
            .offers
            .iter()
            .any(|offer| offer.issued.lookup == issued.lookup)
        {
            return Err(Error::Duplicate);
        }
        // This bounded profile consumes actual equal-version successful H1 results.
        // An error-shaped candidate is NOT silently upgraded into an acquisition.
        if snapshot.result.is_err() || issued.body.service.activation_mode != 0 {
            return Err(Error::InvalidResult);
        }
        let candidate = snapshot.candidate.as_ref().ok_or(Error::InvalidResult)?;
        if candidate.version != hosting_version(&issued.body.service.version)
            || service_type(&candidate.type_instance.service_type)
                != issued.body.service.service_type
        {
            return Err(Error::InvalidResult);
        }
        let accounting = self
            .residency
            .package_accounting(&candidate.type_instance.package_instance())
            .map_err(|_| Error::InvalidResult)?;
        if accounting.version != &candidate.version
            || accounting.package_instance_id != candidate.package_instance_sequence
        {
            return Err(Error::InvalidResult);
        }
        let target = hosting::AcquisitionTarget {
            type_instance: candidate.type_instance.clone(),
            version: accounting.version.clone(),
            package_instance_id: accounting.package_instance_id,
        };
        if let Some(id) = issued.usage {
            let held = self
                .offers
                .iter()
                .find(|offer| offer.issued.lookup == id)
                .ok_or(Error::MissingOwner)?;
            if held.ownership != Ownership::Adopted
                || held.acquisition.is_none()
                || held.target != target
                || held.issued.body.unit.id != issued.body.unit.id
                || held.terminal
            {
                return Err(Error::MissingOwner);
            }
        }
        let completion = ra::RegistrationCompletion {
            unit: issued.body.unit.id.clone(),
            token: issued.token,
            lookup: issued.lookup,
            registration: ra::RuntimeRegistration {
                service_type: service_type(&candidate.type_instance.service_type),
                host: candidate.host_id.clone(),
                runtime: candidate.runtime_id.clone(),
                acquisition: issued.lookup,
                code_package: candidate.code_package.clone(),
            },
            error: 0,
            version: version(&candidate.version),
            usage_acquired: issued.usage.is_none(),
            ..ra::RegistrationCompletion::default()
        };
        let native_body = body_bytes(&issued.body);
        let receipt = completion_bytes(&completion);
        // Each slot pre-reserves all exact copies, two observer-ticket cells,
        // diagnostics/terminal ID, and bounded downstream command/effect escrow.
        let charge = FIXED_ESCROW
            + native_body.len() * 2
            + receipt.len() * 4
            + target_bytes(&target) * 3
            + snapshot_bytes(snapshot)
            + WORK_SLOTS * (native_body.len() + receipt.len() + FIXED_ESCROW);
        Ok((target, completion, charge))
    }

    pub fn required_bytes(
        &self,
        issued: &Issued,
        snapshot: &hosting::LookupResult,
    ) -> Result<usize, Error> {
        self.projected(issued, snapshot)
            .map(|(_, _, charge)| charge)
    }

    pub fn prepare(
        &mut self,
        issued: &Issued,
        snapshot: &hosting::LookupResult,
    ) -> Result<(), Error> {
        self.prepare_with(issued, snapshot, None::<fn(&mut hosting::Residency)>)
    }

    /// Deterministic real package-first/application-closing interleaving.
    pub fn prepare_split(
        &mut self,
        issued: &Issued,
        snapshot: &hosting::LookupResult,
        between: impl FnOnce(&mut hosting::Residency),
    ) -> Result<(), Error> {
        self.prepare_with(issued, snapshot, Some(between))
    }

    fn prepare_with(
        &mut self,
        issued: &Issued,
        snapshot: &hosting::LookupResult,
        between: Option<impl FnOnce(&mut hosting::Residency)>,
    ) -> Result<(), Error> {
        let (target, mut completion, charge) = self.projected(issued, snapshot)?;
        self.reserve(charge)?;
        let increment = if issued.usage.is_some() {
            None
        } else if let Some(between) = between {
            Some(
                match self.residency.begin_acquisition(&target.type_instance) {
                    hosting::AcquisitionBegin::Beginning(owner) => {
                        between(&mut self.residency);
                        self.residency.finish_acquisition(owner)
                    }
                    hosting::AcquisitionBegin::Unmanaged => hosting::UsageIncrement::Unmanaged,
                    hosting::AcquisitionBegin::Rejected(error) => {
                        hosting::UsageIncrement::Rejected(error)
                    }
                },
            )
        } else {
            Some(self.residency.increment_usage(&target.type_instance))
        };
        let (acquisition, ownership, diagnostic) = match increment {
            None => (None, Ownership::Reuses(issued.usage.unwrap()), None),
            Some(hosting::UsageIncrement::Acquired(owner)) => {
                assert_eq!(owner.target(), &target);
                (Some(owner), Ownership::Offered, None)
            }
            Some(hosting::UsageIncrement::Unresolved { ownership, .. }) => {
                assert_eq!(ownership.target(), &target);
                completion.usage_acquired = false;
                // A local adapter diagnostic, not an invented native ErrorCode.
                completion.usage_error = -1;
                (
                    Some(ownership),
                    Ownership::PackageOnly,
                    Some(Diagnostic::IncrementFailed),
                )
            }
            Some(hosting::UsageIncrement::Rejected(_) | hosting::UsageIncrement::Unmanaged) => {
                self.bytes -= charge;
                return Err(Error::InvalidResult);
            }
        };
        let serialized = completion_bytes(&completion);
        let work_escrow =
            WORK_SLOTS * (body_bytes(&issued.body).len() + serialized.len() + FIXED_ESCROW);
        self.offers.push(Offer {
            issued: issued.clone(),
            snapshot: snapshot.clone(),
            target,
            completion,
            serialized,
            ownership,
            acquisition,
            original_ticket: None,
            correction_ticket: None,
            work: Vec::with_capacity(WORK_SLOTS),
            diagnostic,
            release_attempts: 0,
            releases: 0,
            terminal: false,
            accepted_issued: false,
            work_bytes: 0,
            work_escrow,
        });
        Ok(())
    }

    /// Replays/correction submit ONLY the frozen original completion, never lookup
    /// again or increment again. Abandonment is deliberately not replayable.
    pub fn submit(&mut self, executor: &ra::Executor, lookup: u64) -> Result<u64, Error> {
        // Preserve first-offer ownership before replacing the latest observer
        // with a correction/replay. Consumed issuance is monotonic evidence.
        self.classify(executor, lookup);
        let offer = self.offer_mut(lookup);
        if offer.ownership == Ownership::Abandoned
            || (offer.terminal && offer.ownership == Ownership::Unresolved)
        {
            return Err(Error::Terminal);
        }
        if offer
            .ticket()
            .is_some_and(|ticket| !matches!(ticket.status(), ra::Status::Complete(_)))
        {
            return Err(Error::Outstanding);
        }
        // Two fixed cells retain the first accepted ticket and at most one
        // correction/replay ticket. More observations reuse those cells.
        if offer.correction_ticket.is_some() {
            return Err(Error::Capacity);
        }
        assert_eq!(completion_bytes(&offer.completion), offer.serialized);
        let issued_now = executor
            .snapshot(offer.issued.body.unit.id.guid.0)
            .committed
            .is_some_and(|state| state.registration_lookups.contains_key(&lookup));
        match executor.submit(ra::Command::Registered(Box::new(offer.completion.clone()))) {
            Ok(ticket) => {
                offer.accepted_issued |= issued_now;
                let id = ticket.id;
                if offer.original_ticket.is_none() {
                    offer.original_ticket = Some(ticket);
                } else {
                    offer.correction_ticket = Some(ticket);
                }
                Ok(id)
            }
            Err(_) => {
                offer.diagnostic = Some(Diagnostic::SubmitRejected);
                Err(Error::Capacity)
            }
        }
    }

    pub fn classify(&mut self, executor: &ra::Executor, lookup: u64) {
        let offer = self.offer_mut(lookup);
        if offer.terminal {
            return;
        }
        if offer
            .ticket()
            .is_some_and(|ticket| matches!(ticket.status(), ra::Status::Complete(Err(_))))
        {
            offer.diagnostic = Some(Diagnostic::PublicationFailed);
        }
        let committed = executor
            .snapshot(offer.issued.body.unit.id.guid.0)
            .committed;
        // Ticket success alone does not certify adoption. Unissued/stale results
        // without exact RA disposal evidence stay caller-owned.
        if offer.ownership == Ownership::Offered {
            if committed
                .as_ref()
                .is_some_and(|state| state.usage == Some(lookup))
            {
                offer.ownership = Ownership::Adopted;
            } else if offer.accepted_issued
                && offer
                    .ticket()
                    .is_some_and(|ticket| matches!(ticket.status(), ra::Status::Complete(Ok(_))))
                && committed
                    .as_ref()
                    .is_some_and(|state| !state.registration_lookups.contains_key(&lookup))
            {
                // Absence of adoption is NOT withdrawal authority. A consumed
                // issued fresh result can already own queued ReleaseUsage.
                offer.ownership = Ownership::DisposalPending;
            }
        }
    }

    fn release_exact(&mut self, lookup: u64, abandoning: bool) -> Result<(), Error> {
        let index = self
            .offers
            .iter()
            .position(|offer| offer.issued.lookup == lookup)
            .ok_or(Error::MissingOwner)?;
        let offer = &mut self.offers[index];
        if offer.terminal {
            return Err(Error::Terminal);
        }
        let owner = offer.acquisition.take().ok_or(Error::MissingOwner)?;
        // Withdraw local offer and record its terminal ID BEFORE compensation.
        offer.terminal = true;
        offer.release_attempts += 1;
        let partial = self
            .residency
            .acquisition(owner.id())
            .is_some_and(|record| record.phase == hosting::AcquisitionPhase::PackageOnly);
        let release = if partial {
            self.residency.compensate_package_only(owner)
        } else {
            self.residency.decrement_usage(owner)
        };
        match release {
            hosting::UsageRelease::Released => {
                offer.releases += 1;
                offer.ownership = if abandoning {
                    Ownership::Abandoned
                } else {
                    Ownership::Released
                };
            }
            hosting::UsageRelease::Unresolved { ownership, .. } => {
                offer.acquisition = Some(ownership);
                offer.ownership = Ownership::Unresolved;
                offer.diagnostic = Some(Diagnostic::ReleaseUnresolved);
            }
        }
        Ok(())
    }

    pub fn abandon(&mut self, executor: &ra::Executor, lookup: u64) -> Result<(), Error> {
        self.classify(executor, lookup);
        let offer = self.offer(lookup);
        if offer.terminal {
            return Err(Error::Terminal);
        }
        if matches!(
            offer.ownership,
            Ownership::Adopted | Ownership::Reuses(_) | Ownership::DisposalPending
        ) || offer
            .ticket()
            .is_some_and(|ticket| !matches!(ticket.status(), ra::Status::Complete(_)))
        {
            return Err(Error::Outstanding);
        }
        self.release_exact(lookup, true)
    }

    /// All records pre-reserve finite output escrow before their acquisition or
    /// submit. This test profile accepts only outputs for retained entities.
    /// It retains non-release work too; observing it is not executing RAP/BGM.
    pub fn collect(&mut self, executor: &ra::Executor) -> Result<(), Error> {
        let count = executor.statistics().effects;
        if count == 0 && self.incoming.is_empty() {
            return Ok(());
        }
        let free = self
            .offers
            .iter()
            .map(|offer| WORK_SLOTS - offer.work.len())
            .chain(
                self.closes
                    .iter()
                    .map(|close| WORK_SLOTS - close.work.len()),
            )
            .min()
            .unwrap_or(0);
        if self.incoming.is_empty() {
            if count > free {
                return Err(Error::Capacity);
            }
            self.incoming = executor.take_effects();
        }
        let mut first_error = None;
        while let Some(dispatched) = self.incoming.first() {
            let charge = effect_bytes(&dispatched.effect);
            let release = match &dispatched.effect {
                ra::Effect::ReleaseUsage { acquisition } => Some(*acquisition),
                _ => None,
            };
            if let Some(close) = self
                .closes
                .iter_mut()
                .rev()
                .find(|close| close.entity == dispatched.entity)
            {
                if close.work.len() == WORK_SLOTS || close.work_bytes + charge > close.work_escrow {
                    return Err(Error::Capacity); // entire remaining batch still owned
                }
                close.work_bytes += charge;
                close.work.push(self.incoming.remove(0));
            } else {
                let offer = self
                    .offers
                    .iter_mut()
                    .rev()
                    .find(|offer| offer.issued.body.unit.id.guid.0 == dispatched.entity)
                    .ok_or(Error::MissingOwner)?;
                if offer.work.len() == WORK_SLOTS || offer.work_bytes + charge > offer.work_escrow {
                    return Err(Error::Capacity);
                }
                offer.work_bytes += charge;
                offer.work.push(self.incoming.remove(0));
            }
            // Retain the exact action BEFORE any fallible accounting/terminal
            // observation. Continue retaining later actions even on error.
            if let Some(lookup) = release {
                if let Err(error) = self.release_exact(lookup, false) {
                    first_error.get_or_insert(error);
                }
            }
        }
        let lookups: Vec<_> = self
            .offers
            .iter()
            .map(|offer| offer.issued.lookup)
            .collect();
        for lookup in lookups {
            self.classify(executor, lookup);
        }
        first_error.map_or(Ok(()), Err)
    }

    pub fn prepare_close(
        &mut self,
        executor: &ra::Executor,
        entity: [u8; 16],
    ) -> Result<usize, Error> {
        let before = executor
            .snapshot(entity)
            .committed
            .ok_or(Error::InvalidResult)?;
        if before.phase != ra::Phase::Closing {
            return Err(Error::InvalidResult);
        }
        let usage = before.usage.ok_or(Error::MissingOwner)?;
        if self.offer(usage).ownership != Ownership::Adopted {
            return Err(Error::MissingOwner);
        }
        let proposal = ra::CloseCompletion {
            unit: before.body.unit.clone(),
            replica: before.body.replica.clone(),
            error: 0,
        };
        let serialized = close_bytes(&proposal, &before.body);
        let charge = FIXED_ESCROW
            + serialized.len() * 3
            + WORK_SLOTS * (body_bytes(&before.body).len() + FIXED_ESCROW);
        self.reserve(charge)?;
        let work_escrow = WORK_SLOTS * (body_bytes(&before.body).len() + FIXED_ESCROW);
        self.closes.push(CloseWork {
            entity,
            usage,
            proposal,
            serialized,
            before,
            ticket: None,
            work: Vec::with_capacity(WORK_SLOTS),
            work_bytes: 0,
            work_escrow,
        });
        Ok(self.closes.len() - 1)
    }

    pub fn close(&self, index: usize) -> &CloseWork {
        &self.closes[index]
    }

    pub fn submit_close(&mut self, executor: &ra::Executor, index: usize) -> Result<u64, Error> {
        let close = &mut self.closes[index];
        if close.ticket.is_some() {
            return Err(Error::Duplicate);
        }
        assert_eq!(
            close_bytes(&close.proposal, &close.before.body),
            close.serialized
        );
        let ticket = executor
            .submit(ra::Command::CompleteClose(Box::new(close.proposal.clone())))
            .map_err(|_| Error::Capacity)?;
        let id = ticket.id;
        close.ticket = Some(ticket);
        Ok(id)
    }

    /// Shutdown only closes fresh admission; existing records, tickets, frozen
    /// proposals and exact unresolved owners remain available in their old slots.
    pub fn shutdown(&mut self) {
        self.accepting = false;
        self.residency.shutdown();
    }
}

pub fn version(v: &hosting::VersionInstance) -> wire::VersionInstance {
    wire::VersionInstance {
        version: wire::PackageVersion {
            application: wire::ApplicationVersion {
                value: wire::Rollout {
                    major: v.application.major,
                    minor: v.application.minor,
                },
            },
            value: wire::Rollout {
                major: v.package.major,
                minor: v.package.minor,
            },
        },
        instance: v.instance,
    }
}

pub fn hosting_version(v: &wire::VersionInstance) -> hosting::VersionInstance {
    hosting::VersionInstance {
        application: hosting::Rollout {
            major: v.version.application.value.major,
            minor: v.version.application.value.minor,
        },
        package: hosting::Rollout {
            major: v.version.value.major,
            minor: v.version.value.minor,
        },
        instance: v.instance,
    }
}

pub fn service_type(t: &hosting::ServiceTypeId) -> wire::ServiceType {
    wire::ServiceType {
        package: wire::PackageId {
            application: wire::ApplicationId {
                type_name: t.package.application.type_name.clone(),
                number: t.package.application.number,
            },
            name: t.package.name.clone(),
        },
        name: t.name.clone(),
    }
}

pub fn lookup(core: &mut hosting::Hosting, issued: &Issued) -> hosting::LookupResult {
    let t = &issued.body.service.service_type;
    let request = hosting::LookupRequest {
        service_type: hosting::ServiceTypeId {
            package: hosting::PackageId {
                application: hosting::ApplicationId {
                    type_name: t.package.application.type_name.clone(),
                    number: t.package.application.number,
                },
                name: t.package.name.clone(),
            },
            name: t.name.clone(),
        },
        version: hosting_version(&issued.body.service.version),
        activation: hosting::Activation::SHARED,
        service_name: issued.body.service.name.clone(),
        application_name: issued.body.service.application.clone(),
        should_activate: true,
    };
    core.lookup(&request, &hosting::LookupModel::default())
        .unwrap()
}

fn body_bytes(body: &wire::AddInstance) -> Vec<u8> {
    wire::add_instance_message(body, &wire::GenerationHeader::default())
        .expect("actual supported native Add shape")
        .body
}

fn text(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u64).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn wire_version(out: &mut Vec<u8>, value: &wire::VersionInstance) {
    for value in [
        value.version.application.value.major,
        value.version.application.value.minor,
        value.version.value.major,
        value.version.value.minor,
    ] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out.extend_from_slice(&value.instance.to_le_bytes());
}

fn registration_bytes(registration: &ra::RuntimeRegistration) -> Vec<u8> {
    let mut out = Vec::new();
    text(
        &mut out,
        &registration.service_type.package.application.type_name,
    );
    out.extend_from_slice(
        &registration
            .service_type
            .package
            .application
            .number
            .to_le_bytes(),
    );
    for value in [
        &registration.service_type.package.name,
        &registration.service_type.name,
        &registration.host,
        &registration.runtime,
        &registration.code_package,
    ] {
        text(&mut out, value);
    }
    out.extend_from_slice(&registration.acquisition.to_le_bytes());
    out
}

/// Lossless local test-profile serialization of EVERY original completion field.
/// This receipt is NOT the native RA wire format; Executor::submit still uses
/// the real RA serializer and owns that private command/proposal independently.
fn completion_bytes(completion: &ra::RegistrationCompletion) -> Vec<u8> {
    let mut out = completion.unit.guid.0.to_vec();
    out.extend_from_slice(&completion.token.to_le_bytes());
    out.extend_from_slice(&completion.lookup.to_le_bytes());
    out.extend_from_slice(&registration_bytes(&completion.registration));
    out.extend_from_slice(&completion.error.to_le_bytes());
    out.push(u8::from(completion.retryable));
    wire_version(&mut out, &completion.version);
    out.extend_from_slice(&completion.required_upgrade.to_le_bytes());
    out.push(u8::from(completion.upgrade_present));
    out.push(u8::from(completion.usage_acquired));
    out.extend_from_slice(&completion.usage_error.to_le_bytes());
    text(&mut out, &completion.error_message);
    out
}

fn close_bytes(close: &ra::CloseCompletion, body: &wire::AddInstance) -> Vec<u8> {
    let mut body = body.clone();
    body.unit = close.unit.clone();
    body.replica = close.replica.clone();
    let mut out = body_bytes(&body);
    out.extend_from_slice(&close.error.to_le_bytes());
    out
}

fn target_bytes(target: &hosting::AcquisitionTarget) -> usize {
    128 + target
        .type_instance
        .service_type
        .package
        .application
        .type_name
        .len()
        + target.type_instance.service_type.package.name.len()
        + target.type_instance.service_type.name.len()
        + target.type_instance.public_activation.len()
}

fn snapshot_bytes(snapshot: &hosting::LookupResult) -> usize {
    128 + snapshot.candidate.as_ref().map_or(0, |candidate| {
        candidate
            .type_instance
            .service_type
            .package
            .application
            .type_name
            .len()
            + candidate.type_instance.service_type.package.name.len()
            + candidate.type_instance.service_type.name.len()
            + candidate.type_instance.public_activation.len()
            + candidate.host_id.len()
            + candidate.runtime_id.len()
            + candidate.code_package.len()
    })
}

fn effect_bytes(effect: &ra::Effect) -> usize {
    // Closed supported profile: original Add bodies/registrations plus fixed
    // scalar notifications/health. Unsupported outputs are not silently dropped.
    match effect {
        ra::Effect::Open { body, .. } | ra::Effect::FindRegistration { body, .. } => {
            128 + body_bytes(body).len()
        }
        ra::Effect::RuntimeOpen {
            body, registration, ..
        }
        | ra::Effect::Close {
            body, registration, ..
        } => 128 + body_bytes(body).len() + registration_bytes(registration).len(),
        ra::Effect::ReleaseRegistration(registration) => {
            128 + registration_bytes(registration).len()
        }
        ra::Effect::Reply(reply) => {
            128 + wire::add_instance_reply_message(reply, wire::Owner::Fm)
                .unwrap()
                .body
                .len()
        }
        ra::Effect::RemoveReply(reply) => {
            128 + wire::remove_instance_reply_message(reply, wire::Owner::Fm)
                .unwrap()
                .body
                .len()
        }
        ra::Effect::ReleaseUsage { .. }
        | ra::Effect::Health(_)
        | ra::Effect::RequestServiceTypeWork { .. }
        | ra::Effect::RequestReportRetry => 128,
        other => panic!("output outside the explicitly supported test profile: {other:?}"),
    }
}
