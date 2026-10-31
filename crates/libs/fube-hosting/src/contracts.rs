//! Narrow node-side Hosting ports, not a translation of all of IHostingSubsystem.
//!
//! The native authority throughout is Service Fabric commit
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` on Linux. Linked test bodies
//! describe source scenarios, **not newly executed native tests**. These ports
//! impose explicit Rust ownership/admission rules where native pointers, void
//! methods and Begin/End operations cannot express them. Such rules are local
//! adaptations, not additional native wire fields or claimed successful recovery
//! from native assertions.
//!
//! Node composition projects these DTOs to RA's existing command/effect and
//! registration/Open/Close completion contracts. Hosting must not import RA,
//! runtime or test-host production types. In particular a RAP send's admission
//! or transport completion is not its semantic reply, and completion of a
//! termination request is not matching HostDown, OS exit, group drain or reap
//! ([native send action][send], [termination operation][termination]).
//! Native termination End is void/asserting, and a missing proxy can complete
//! Success without proving host death ([termination End][termination-end]).
//! No transport, process, subscription or live RA consumer is supplied here.
//!
//! Existing H1 host processors remain the handshake boundary, not another port:
//! after Find, public Finish discards the state method's InvalidState, replies
//! Success and independently looks up/notifies the proxy. Membership and Completed
//! are separate registration predicates. Held records survive table removal;
//! timeout cancel/Find/mark/Remove are split and do not emit ordinary HostClosed
//! ([Finish][finish], [record][record], [table][table], [timeout][timeout]).
//! The H1 split-step APIs preserve these interleavings, not live process authority.
//!
//! [send]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.SendMessageActions.h#L102-L173
//! [termination]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1003-L1108
//! [termination-end]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingSubsystem.cpp#L1547-L1581
//! [finish]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L2011-L2054
//! [record]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostRegistration.cpp#L84-L158
//! [table]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostRegistrationTable.cpp#L42-L93
//! [timeout]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationHostManager.cpp#L1966-L2002

use crate::*;

/// Immediate registration lookup, separate from residency acquisition and RAP.
///
/// # Inputs, profile and provenance
///
/// Native RA supplies type, full package version, and activation mode/partition
/// context. Lookup uses immutable service Name and ApplicationName for public
/// activation provenance, and ApplicationName for download/activation; no mutable
/// replica configuration is needed in this port ([RA input][input],
/// [lookup processor][lookup]). [`LookupRequest`] projects that subset. Its
/// `should_activate=false` exposes the existing nonactivating lookup path; the
/// ordinary native public Find uses true. Shared activation has an empty public
/// ID; exclusive identity mapping does not enable exclusive process execution.
///
/// Comparison retains raw spelling: the native Linux comparator uppercases UTF-16
/// units, terminates at NUL and uses signed-short differences, not normalization
/// or Unicode full folding. H1 models the startup C-locale comparison profile,
/// accepts non-ASCII strings and does not establish an oracle for every locale.
/// Public activation lookup compares the CI *rendered flat key*, including its
/// delimiter/NUL aliases, not a structured package tuple ([comparison][compare],
/// [PAL][pal], [public map][map]). Raw comparison records are not proof of valid
/// native constructor/public-GUID admission.
///
/// Native constructors assert on empty application type with nonzero application
/// number, or nonempty application with empty package name
/// ([ApplicationIdentifier.cpp:27–35][app-constructor],
/// [ServicePackageIdentifier.cpp:25–49][package-constructor]).
/// A *new missing exclusive* public mapping requires nonempty ServiceName;
/// shared and already-resolved lookups do not impose that blanket condition
/// ([HostingSubsystem.cpp:119–157][map]). H1 rejects these assertion boundaries
/// as local `NativeInvariant` without publication, not recoverable native
/// ErrorCodes. They do not prohibit non-ASCII or normalize identity.
///
/// # Ownership and consumers
///
/// A returned [`Registration`] is a snapshot, not an acquired package residency
/// or an RA-owned reference. RA's issued lookup/acquisition IDs and retain-existing-
/// usage policy remain private node/adapter bookkeeping. The existing RA seam
/// requires one acquisition for an issued result needing fresh usage; retained
/// usage is reused. Replay must not acquire again or retarget disposal. Unissued
/// fresh ownership stays with its caller. Candidate-bearing NotFound is
/// diagnostic/unacquired there, not automatic RA-owned cleanup.
///
/// Native RA can adopt a mismatch candidate under its upgrade-instance policy,
/// but does not adopt candidate-bearing NotFound; it initializes the lookup
/// sequence without imposing a newest-sequence adoption fence ([RA adoption][ra]).
/// This port neither applies that policy nor introduces new wire fields.
///
/// [input]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L987-L1033
/// [lookup]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L117-L249
/// [compare]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/StringUtility.h#L125-L173
/// [pal]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/pal/wc16.cpp#L136-L156
/// [map]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingSubsystem.cpp#L119-L180
/// [app-constructor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ApplicationIdentifier.cpp#L27-L35
/// [package-constructor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/ServiceModel/ServicePackageIdentifier.cpp#L25-L49
/// [ra]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L987-L1033
pub trait RegistrationLookup {
    /// Observe the immediate result; never wait for registration or serving readiness.
    ///
    /// The **outer** error means local input, capacity or missing-dependency-
    /// observation admission failed without publishing a core transition. The
    /// **inner** [`LookupResult::result`] preserves native semantics (and explicitly
    /// gated capabilities/invariants), independently of candidate and sequence:
    ///
    /// | Native branch | Candidate | Sequence write | Semantic result |
    /// |---|---|---|---|
    /// | Public-map miss / state lookup failure | absent | absent | branch error |
    /// | Registered, runtime lookup failure | absent | present | runtime error |
    /// | Registered, equal full version | present | present | Success |
    /// | Registered, unequal version, hasReplica | retained | present | mismatch |
    /// | Registered, unequal version, !hasReplica | retained | present | NotFound |
    /// | Disabled / registration in progress | absent | present | disabled / not registered |
    /// | Activation synchronous End | absent | present | failure, or not registered on Success |
    /// | Activation asynchronous | absent | present | deployment in progress |
    ///
    /// Native resets the candidate, not the caller's sequence. `None` means no
    /// write, not zero; missing type can allocate a new sequence without inserting
    /// an entry ([lookup][processor], [GetState][state]). Lookup candidate type
    /// spelling comes from the request; version, host/PID, runtime, code package
    /// and package-instance sequence come from the producer-selected valid runtime,
    /// not from the requested version or a second host-validity check
    /// ([candidate construction][candidate], [shared helper][helper]).
    ///
    /// Full version means application rollout, package rollout and unsigned
    /// instance. On inequality, ApplicationManager derives `hasReplica` from the
    /// scheduling disposition: scheduling failure maps to false; successful
    /// eligibility maps to `!scheduled`. Zero usage with infinite grace can
    /// therefore produce mismatch. NotFound proves neither package absence nor
    /// physical deactivation ([version/scheduling][version], [wrapper][wrapper],
    /// [eligibility][eligibility], [grace selection][grace],
    /// [later scheduling][scheduling]). Do not replace this with inequality or a count.
    ///
    /// Pending activation owns a later End independently of this returned result.
    /// Its End is consumed, not retroactively substituted into lookup. Missing H1
    /// premises fail; adhoc activation/blocklisting remains explicitly gated.
    /// H1 projected admission reserves state and all required outputs together;
    /// it is a Rust safety adaptation, not a native transactional API promise.
    /// Core shutdown may retain pending operations and outputs; dropping the
    /// observer/adapter does not cancel, consume End, release usage or prove cleanup.
    ///
    /// Native source tests cover pre-registration/disabled/registered lookup
    /// ([NonActivatedApplicationHost.Test.cpp:495–622][test]); the independent
    /// candidate-bearing error matrix above is **processor-derived**, not a claim
    /// that that test executes every combination.
    ///
    /// [processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L117-L249
    /// [state]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L128-L154
    /// [candidate]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L431-L458
    /// [helper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeRegistrationHelper.h#L8-L24
    /// [version]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L381-L429
    /// [wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L1079-L1093
    /// [eligibility]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L113-L145
    /// [grace]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L252-L257
    /// [scheduling]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L912-L971
    /// [test]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/NonActivatedApplicationHost.Test.cpp#L495-L622
    fn lookup(&mut self, request: &LookupRequest) -> Result<LookupResult>;
}

/// Explicit **model-only** implementation of [`RegistrationLookup`] over H1.
///
/// Production lookup inputs remain request-only. The separate borrowed
/// [`LookupModel`] supplies controlled dependency/GUID observations, not a live
/// package engine or a fixture restriction on all implementations. No default
/// observations are invented. The adapter delegates H1's projected admission
/// unchanged; dropping this borrow executes no completion, rollback or release.
/// Reborrow with the next explicit model when an operation needs other premises.
pub struct H1LookupAdapter<'a> {
    core: &'a mut Hosting,
    model: &'a LookupModel,
}

impl<'a> H1LookupAdapter<'a> {
    /// Borrow the actual H1 core with explicit observations.
    ///
    /// All-None observations are sufficient only for branches with no package
    /// dependency (for example equal-version registered lookup). If lookup needs
    /// activation, deactivation or a new exclusive mapping, absent observations
    /// yield `MissingModelOutcome`, not success or a call into live H2.
    pub fn new(core: &'a mut Hosting, model: &'a LookupModel) -> Self {
        Self { core, model }
    }
}

impl RegistrationLookup for H1LookupAdapter<'_> {
    fn lookup(&mut self, request: &LookupRequest) -> Result<LookupResult> {
        self.core.lookup(request, self.model)
    }
}

/// Increment observation with explicit residual ownership.
///
/// This is a Rust ownership adaptation, not a native result DTO. In particular
/// a native error after package increment cannot be represented as a proven
/// mutation-free rejection. No variant implies a registration reference was acquired.
#[derive(Debug)]
#[must_use = "residency or unresolved mutation ownership must be accounted for"]
pub enum UsageIncrement<A> {
    /// Managed residency acquired; release exactly this acquisition once.
    Acquired(A),
    /// Explicitly supported adhoc path: native success without managed residency.
    Unmanaged,
    /// Proven no mutation/acquisition; error may be native or local admission.
    Rejected(Error),
    /// Native error/uncertainty after possible mutation; retain exact reconciliation ownership.
    Unresolved { error: Error, ownership: A },
}

/// Release observation, **not** a recoverable native decrement error return.
#[derive(Debug)]
#[must_use = "unresolved release still owns its original acquisition"]
pub enum UsageRelease<A> {
    /// Adapter verified release of the exact owned acquisition.
    /// This requires stronger evidence than the native public void return.
    Released,
    /// Admission failure, source invariant or unverified native void disposition.
    /// The same acquisition remains owned; a diagnostic is not a rollback receipt.
    Unresolved {
        ownership: A,
        diagnostic: Option<Error>,
    },
}

/// Package residency port, implemented by the deterministic H2 [`Residency`] ledger.
///
/// Native receives service type plus activation context and resolves the managed
/// public activation map. Missing mapping asserts. Adhoc increment succeeds and
/// decrement is a void no-op without managed residency
/// ([HostingSubsystem.cpp:1349–1392][public]). A registration snapshot, host or
/// runtime ID alone is not the residency target.
///
/// RA keeps residency independently from its registration pointer: ReplicaDown
/// drops the pointer but retains usage for reopen; ReplicaClosed releases it once.
/// Increment precedes persistence with same-target cancellation compensation;
/// decrement performs only after commit, not on action cancellation
/// ([wrapper][wrapper], [actions][actions], [job completion][job]).
/// Store definite failure versus unknown proposal/ticket/fence ownership remains
/// RA/store policy. Observer drop/cancel is neither action cancellation nor proof
/// of async rollback. Replaying a result must not increment again.
///
/// These owned outcomes strengthen bookkeeping, not native behavior. An
/// implementation must have a bounded reconciliation owner for partial/unknown
/// operations and shutdown, must not claim destructor Drop executed async release,
/// and must not silently discard a token on error. Interfaces alone enable no
/// physical package capability or live RA acquired-reference integration.
///
/// Native wrapper tests exercise duplicate acquisition, increment failure,
/// down-with-retained-usage and close-once
/// ([Test.Unit.ServiceTypeRegistrationWrapper.cpp:315–513][test]); these use
/// Hosting expectations, not deployed residency qualification.
///
/// [public]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/HostingSubsystem.cpp#L1349-L1392
/// [wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ServiceTypeRegistrationWrapper.cpp#L78-L162
/// [actions]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingAdapter.cpp#L83-L203
/// [job]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h#L228-L278
/// [test]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.ServiceTypeRegistrationWrapper.cpp#L315-L513
pub trait PackageUsage {
    /// Implementation-owned exact package/activation/public-ID acquisition or
    /// reconciliation record. Implementors must not make it Clone/Copy or use
    /// destructor-drop as proof of release. It is not an RA lookup ID or wire field.
    type Acquisition;

    /// Increment managed package residency once, or explicitly observe adhoc no-residency.
    ///
    /// Native unopened/closing/absent entries yield InvalidState/NotFound.
    /// Package increment advances sequence and usage; 0→1 cancels package
    /// deactivation, clears continuous failures and resets unused-since.
    /// Application increment follows package success; a race can return InvalidState
    /// **after package mutation**. Error therefore does not certify rollback
    /// ([Deactivator.cpp:330–367][application], [package increment:37–68][package],
    /// [managed increment:1003–1014][increment]). Such a path requires `Unresolved` ownership,
    /// not `Rejected`. Native assertion/undefined paths are local invariant
    /// boundaries, never successfully recoverable native outcomes.
    ///
    /// [application]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L330-L367
    /// [package]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L37-L68
    /// [increment]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L1003-L1014
    fn increment_usage(&mut self, target: &TypeInstance) -> UsageIncrement<Self::Acquisition>;

    /// Consume only the original managed acquisition, never reconstruct from a new lookup.
    ///
    /// Native public decrement returns void; ApplicationManager traces/consumes
    /// internal errors. Package closing/underflow asserts. A real decrement
    /// advances sequence; 1→0 records unused-since and schedules after releasing
    /// the entry lock. Application zero does not immediately schedule application
    /// removal; successful last-package removal supplies that step
    /// ([public void][void], [decrement][decrement], [last package][last]).
    ///
    /// `Released` means locally verified accounting, **not** that native void
    /// means recoverable success or that physical package deactivation completed.
    /// If invocation cannot be admitted or its effects cannot be verified, return
    /// `Unresolved` with the *same* ownership. Internal traced errors may appear
    /// as local diagnostics, not invented native return codes. No automatic retry
    /// is prescribed: retrying an uncertain decrement can underflow.
    ///
    /// [void]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L1313-L1324
    /// [decrement]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L70-L107
    /// [last]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L543-L580
    fn decrement_usage(
        &mut self,
        acquisition: Self::Acquisition,
    ) -> UsageRelease<Self::Acquisition>;
}

/// Lookup-triggered activation's immediate native End or owned pending operation.
#[derive(Debug)]
#[must_use = "pending activation remains owned until its End is observed"]
pub enum ActivationStart<O> {
    /// Native operation completed synchronously; success is package activation,
    /// not runtime/type registration or serving readiness.
    Synchronous(Result<()>),
    /// Owned pending operation, not a lookup result or observer ticket.
    Pending(O),
}

/// Observation of an already owned activation; every noncompletion retains ownership.
#[derive(Debug)]
#[must_use = "noncompleted activation ownership must be retained"]
pub enum ActivationFinish<O> {
    /// Final End consumed exactly once, whether success or failure.
    Completed(Result<()>),
    /// Not ready; no timeout-to-success shortcut. The original operation remains owned.
    Pending(O),
    /// Local observation/admission failure, not final native completion or cancellation.
    Retained { operation: O, error: Error },
}

/// Definition-only lookup collaborator port; **no package engine is supplied**.
///
/// This is the selected DownloadAndActivate and deactivation-if-unused boundary,
/// not all package deployment/upgrade/Begin/End methods. [`ActivationRequest`]
/// contains requested full version/type, resolved activation/public ID,
/// ApplicationName and original lookup sequence. ServiceName's public-map
/// provenance has already been resolved. Real execution profiles (managed
/// shared/exclusive grace, download/launch, package catalogs) require their own
/// explicit capability admission; identity support is not execution support.
///
/// H1's [`ActivationOutcome`], [`LookupModel`] and [`Obligation`] are controlled
/// observations, **not** implementations of this port. No default-success adapter,
/// invented retry policy, observer timeout or second RAP is supplied.
///
/// Actual native activation can reuse an active same-version application/package;
/// errors are not always pending. Success reports registration-not-found using
/// the original sequence, not proof a registration exists
/// ([ApplicationManager.cpp:357–389,545–635][activation]).
/// Native activation/deactivation source tests drive real find/increment/decrement
/// choreography ([ParallelActivationDeactivation.Test.cpp:301–379][test]);
/// synchronous/pending/late-End combinations are processor-derived.
///
/// [activation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L357-L635
/// [test]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ParallelActivationDeactivation.Test.cpp#L301-L379
pub trait PackageLifecycle: PackageScheduling {
    /// Opaque nonclone operation ownership; no wire/correlation ID is introduced.
    /// Shutdown must retain/consume admitted operations, not rely on observer Drop.
    type Activation;

    /// Begin the selected activation and distinguish synchronous End from pending.
    ///
    /// Outer error is local rejection *before* operation admission. Once admitted,
    /// synchronous error is inside `Synchronous`, or ownership is returned in
    /// `Pending`; it may not be silently dropped by an outer error. Native lookup
    /// translates synchronous success to TypeNotRegistered and pending to
    /// DeploymentInProgress ([FabricRuntimeManager.cpp:296–379][source]).
    /// Completion/cancellation of the observer does not undo admitted package work.
    ///
    /// [source]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L296-L379
    fn activate(
        &mut self,
        request: &ActivationRequest,
    ) -> Result<ActivationStart<Self::Activation>>;

    /// Observe End of the exact owned operation, returning it on every noncompletion.
    ///
    /// Native asynchronous callback invokes End once; a late failure is read/traced,
    /// not substituted into the already returned lookup result
    /// ([FabricRuntimeManager.cpp:322–379][source]). No cancellation method is
    /// invented here. `Pending`/`Retained` does not certify abort, rollback or
    /// release; `Completed` does not certify registration, RAP Open or deployment
    /// interoperability. H1's separate controlled completion API retains its own
    /// handle across output-capacity failure and core closure, without implementing
    /// this production port.
    ///
    /// [source]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/FabricRuntimeManager.cpp#L322-L379
    fn finish_activation(
        &mut self,
        operation: Self::Activation,
    ) -> ActivationFinish<Self::Activation>;
}

/// Narrow H2 scheduling dependency, independent of H4 activation/physical End.
///
/// The closed [`SchedulingDisposition`] makes the result's retained footprint
/// bounded before invocation. Timer-admission diagnostics and final work are owned
/// separately by the scheduler; eligibility is not timer or process success.
pub trait PackageScheduling {
    /// Request scheduling for the candidate's exact package instance, not physical removal.
    ///
    /// [`SchedulingDisposition`] records the underlying scheduling observation;
    /// local projected lookup admission happens before calling this dependency.
    /// The native public wrapper
    /// returns void and derives hasReplica=false on underlying error. Success's
    /// `scheduled` is source eligibility, **not verified timer enqueue**.
    /// Eligibility needs zero usage, not closing and finite applicable grace:
    /// shared DeactivationGraceInterval versus exclusive-mode grace. Infinite grace
    /// suppresses eligibility; debug lock/duplicate/closed queue can suppress actual
    /// scheduling later ([wrapper][wrapper], [eligibility][eligibility],
    /// [grace selection][grace], [later scheduling][scheduling]).
    ///
    /// Package usage advances sequence. Queue replacement/removal respects captured
    /// sequences; firing executes only on matching current sequence, zero usage
    /// and not closing, even if cancellation raced. Registration-not-found timers
    /// separately require their own existing state and matching sequence; these are
    /// **specific** fences, not a universal lookup/event generation rule
    /// ([queue sequences][timer], [execution guard][guard],
    /// [timer callback][callback], [registration timeout][registration]).
    /// A scheduled decision is neither pending activation End nor final deactivation.
    /// Native deactivation has separate End success/failure handling; only
    /// ApplicationManagerApplicationNotFound is treated as missing-app success,
    /// not arbitrary errors ([deactivation][deactivation]). That engine is deferred.
    ///
    /// [wrapper]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L1079-L1093
    /// [eligibility]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L113-L145
    /// [grace]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L252-L257
    /// [scheduling]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L912-L971
    /// [timer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L735-L815
    /// [guard]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L147-L166
    /// [callback]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/Deactivator.cpp#L1078-L1100
    /// [registration]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L866-L893
    /// [deactivation]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ApplicationManager.cpp#L1111-L1211
    fn schedule_deactivation_if_unused(
        &mut self,
        package: &PackageInstance,
    ) -> SchedulingDisposition;
}

/// Fixed-payload native scheduling observation for pre-reserved real lookup.
///
/// Variable local diagnostics must remain with a separately bounded scheduler
/// owner. This is not a native wire enum, rollback receipt or timer admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingDisposition {
    Success { scheduled: bool },
    NotFound,
    InvalidState,
    ObjectClosed,
    SequenceOverflow,
}

impl SchedulingDisposition {
    /// Maximum logical H1 outcome charge after conversion, including static detail.
    pub const MAX_OUTCOME_BYTES: usize = 32 + "package accounting sequence overflow".len();

    /// Preserve native hasReplica disposition in the existing H1 observation DTO.
    pub fn outcome(self) -> DeactivationOutcome {
        match self {
            Self::Success { scheduled } => DeactivationOutcome::Success { scheduled },
            Self::NotFound => DeactivationOutcome::Failure(Error::NotFound),
            Self::InvalidState => DeactivationOutcome::Failure(Error::InvalidState),
            Self::ObjectClosed => DeactivationOutcome::Failure(Error::ObjectClosed),
            Self::SequenceOverflow => DeactivationOutcome::Failure(Error::NativeInvariant(
                "package accounting sequence overflow",
            )),
        }
    }
}

/// Two typed owned event lanes; not native worker/subscription emulation.
///
/// Registered/RuntimeClosed/HostClosed share one native single-worker FifoLifo
/// queue; Enabled/Disabled share another. Normal unsaturated native processing
/// is FIFO; full FifoLifo queues can switch policy. No global cross-lane callback
/// or RA-work completion order exists ([EventDispatcher.cpp:39–198][dispatcher],
/// [JobQueue.h:305–356,480–513][queue]).
///
/// Native state mutation precedes enqueue, which occurs under the producer lock,
/// **not consumer callback under that lock**. EventT snapshots handlers under its
/// event lock and invokes them outside it. Unregister/Close removes handlers but
/// already snapshotted callbacks may still run. Native enqueue is void and ignores
/// false-on-closed queue admission; Fire on closed events is a no-op, so native
/// shutdown does not promise lossless delivery ([producer][producer],
/// [EventT lifetime][lifetime], [dispatcher close][close]).
///
/// This port instead transfers owned H1 enqueue observations by explicit drains.
/// No subscriptions, handlers or acknowledgment are implemented. H1's finite
/// projected admission rejects whole transitions before silently dropping required
/// events; retained shutdown work must be drained/accounted for by a separately
/// bounded delivery owner. Draining is not Fire, callback completion or RA commit.
///
/// [dispatcher]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/EventDispatcher.cpp#L39-L198
/// [queue]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/JobQueue.h#L305-L513
/// [producer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L623-L648
/// [lifetime]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Common/Event.h#L39-L122
/// [close]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/EventDispatcher.cpp#L47-L70
pub trait HostingEventSource {
    /// Transfer the registration/closure lane without normalizing or retargeting fields.
    ///
    /// Registered uses **retained entry** type/activation/public-ID provenance,
    /// but operational version, host/PID, runtime, code package and numeric package
    /// sequence come from the runtime context **supplied to Finish**
    /// ([Finish call sites][finish]). Lookup instead uses requested type spelling
    /// and the valid **runtime-table** context selected by the retained Start owner.
    /// Do not reconstruct Registered from that table, normalize its producer spelling,
    /// or replace the Finish snapshot with Start-owned operational fields.
    /// RuntimeClosed retains sequence, supplied host/runtime,
    /// first affected type's activation/public ID and affected types; HostClosed
    /// retains host/activity instead of runtime. Empty affected closure retains
    /// initialized empty scope/vector ([event producers][events],
    /// [closure selection][closure]). H1's activity string is a model adaptation
    /// of native ActivityDescription, not a wire DTO.
    ///
    /// Hosting selects affected owners CI; native RA's current host/runtime predicates
    /// use exact equality. Reliability forwards registration only, runtime-close
    /// host/runtime only, host-close host/activity only, not a shared sequence fence.
    /// Registered RA work revalidates type/full version/activation mode;
    /// RuntimeClosed initiates normal Close if allowed and **must not become HostDown**
    /// ([forwarding][forward], [consumers][consumers]). Rust live consumers remain
    /// gated; do not discard event variants because RA has no live command for them.
    ///
    /// Native/future-consumer admission is distinct from this H1 drain:
    /// all three native handlers require FM readiness. Registered also requires an
    /// activated, nonupgrading node, with queued-job open RA/FT checks. Exclusive
    /// registration routes first by activation partition GUID; shared read-lock
    /// selection is only an initial filter before entity-lock revalidation
    /// ([Registered routing][routing], [readiness][readiness]).
    /// RuntimeClosed requires nonempty host/runtime IDs and a queued-job nonnull,
    /// open FT with RA open or closing. HostClosed requires nonempty host ID and
    /// nonnull FT with RA open or closing, **without RuntimeClosed's additional
    /// FT-open job flag**; its processor applies current host/state failure policy
    /// ([closure jobs][jobs]). H1 does not execute these consumers or filter
    /// queued outputs using these native admission predicates.
    ///
    /// Native RA source tests cover routing/readiness and runtime-close versus
    /// host-down close choreography ([routing tests][test], [close tests][closetest]).
    /// The latter use persisted replicas, not direct stateless Hosting conformance.
    ///
    /// [events]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L1581-L1636
    /// [finish]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L737-L779
    /// [closure]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L923-L1068
    /// [forward]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ReliabilitySubsystem.cpp#L500-L529
    /// [consumers]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp#L1084-L1215
    /// [routing]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingEventHandler.cpp#L51-L114
    /// [readiness]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingEventHandler.cpp#L229-L254
    /// [jobs]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Hosting.HostingEventHandler.cpp#L118-L220
    /// [test]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.HostingAdapter.cpp#L230-L388
    /// [closetest]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.StateMachine.Close.cpp#L729-L756
    fn take_closed_registered_events(&mut self) -> Vec<ClosedRegisteredEvent>;

    /// Transfer Enabled/Disabled independently; draining the other lane does not consume it.
    ///
    /// Preserve sequence and retained type/activation/public ID
    /// ([status producer][producer]). Native node-level status work requires
    /// FM readiness, ignores activation context for its update key and uses a
    /// per-type *expiring* strictly-greater-sequence fence. Equal sequence asserts/
    /// rejects; lower rejects; expiry can remove remembered history
    /// ([processor][processor], [staleness][staleness], [matching tests][test]).
    /// This differs from Registered/closure consumers and RA registration-map
    /// notification bookkeeping. No blanket newest-sequence fence or silent enabled
    /// traffic drop is authorized; a live status consumer remains unimplemented.
    ///
    /// [producer]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Hosting2/ServiceTypeStateManager.cpp#L1593-L1613
    /// [processor]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.ServiceTypeUpdateProcessor.cpp#L64-L144
    /// [staleness]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Node.ServiceTypeUpdateStalenessChecker.h#L33-L53
    /// [test]: https://github.com/microsoft/service-fabric/blob/3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d/src/prod/src/Reliability/Failover/ra/Test.Unit.Node.ServiceTypeUpdateStalenessChecker.cpp#L143-L215
    fn take_enabled_disabled_events(&mut self) -> Vec<EnabledDisabledEvent>;
}

impl HostingEventSource for Hosting {
    fn take_closed_registered_events(&mut self) -> Vec<ClosedRegisteredEvent> {
        Hosting::take_closed_registered_events(self)
    }

    fn take_enabled_disabled_events(&mut self) -> Vec<EnabledDisabledEvent> {
        Hosting::take_enabled_disabled_events(self)
    }
}
