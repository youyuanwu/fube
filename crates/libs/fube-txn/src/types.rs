// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{EnumerationCompletionResult, Epoch, IStateProvider2, OperationData};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

/// Native FABRIC_SEQUENCE_NUMBER/LONG64, including native invalid values.
pub type SequenceNumber = i64;
/// Native LONG64 transaction identity; atomic identities can be negative.
pub type TransactionId = i64;
/// Native FABRIC_STATE_PROVIDER_ID/LONG64, not a registry authority.
pub type StateProviderId = i64;

/// Native common/TransactionState.h values; no state machine is implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum TransactionState {
    /// Admits operations/read/commit subject to role/manager and state checks.
    Active = 0,
    /// Snapshot registration in progress; overlapping use is rejected.
    Reading = 1,
    /// Commit/atomic operation in progress; not completed quorum/visibility.
    Committing = 2,
    /// Owned abort work pending, not necessarily drained.
    Aborting = 3,
    /// Successful transaction completion; read-only need not have a commit LSN.
    Committed = 4,
    /// Aborted state; distinct from merely requesting synchronous abort.
    Aborted = 5,
    /// Nonretriable transaction failure state.
    Faulted = 6,
}

/// Native common/ApplyContext.h values, including role/operation masks.
/// Operation values are not independent bitflags: Unlock=3, FalseProgress=5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ApplyContext {
    /// Invalid role/operation context.
    Invalid = 0,
    /// Primary role bits alone.
    Primary = 0x1 << 16,
    /// Secondary role bits alone.
    Secondary = 0x2 << 16,
    /// Recovery role bits alone.
    Recovery = 0x4 << 16,
    /// Mask selecting role bits.
    RoleMask = -65536,
    /// Redo operation discriminator.
    Redo = 0x2,
    /// Unlock operation discriminator, not independent redo/role flags.
    Unlock = 0x3,
    /// Undo operation discriminator.
    Undo = 0x4,
    /// False-progress operation discriminator.
    FalseProgress = 0x5,
    /// Mask selecting operation discriminator.
    OperationMask = 65535,
    /// Primary redo.
    PrimaryRedo = (0x1 << 16) | 0x2,
    /// Primary undo.
    PrimaryUndo = (0x1 << 16) | 0x4,
    /// Primary unlock.
    PrimaryUnlock = (0x1 << 16) | 0x3,
    /// Secondary redo.
    SecondaryRedo = (0x2 << 16) | 0x2,
    /// Secondary undo.
    SecondaryUndo = (0x2 << 16) | 0x4,
    /// Secondary unlock.
    SecondaryUnlock = (0x2 << 16) | 0x3,
    /// Secondary false-progress undo path.
    SecondaryFalseProgress = (0x2 << 16) | 0x5,
    /// Recovery redo.
    RecoveryRedo = (0x4 << 16) | 0x2,
    /// Recovery undo.
    RecoveryUndo = (0x4 << 16) | 0x4,
}

/// Native common/StateProviderInfo.h child-provider description.
#[derive(Clone, Debug)]
pub struct StateProviderInfo {
    /// Factory type name for recursive child construction.
    pub type_name: String,
    /// Child URI represented as owned UTF-8.
    pub name: String,
    /// Optional initialization payload; absence differs from empty data.
    pub initialization_parameters: Option<OperationData>,
}

/// Native statemanager/FactoryArguments.h; construction does not initialize/open
/// the provider. Parentage is supplied separately through Initialize's children.
#[derive(Clone, Debug)]
pub struct FactoryArguments {
    /// Provider URI represented as owned UTF-8.
    pub name: String,
    /// Assigned registry routing identity.
    pub state_provider_id: StateProviderId,
    /// Factory type name, not a parent-ID encoding.
    pub type_name: String,
    /// Hosting partition identity.
    pub partition_id: uuid::Uuid,
    /// Hosting replica identity.
    pub replica_id: i64,
    /// Optional initialization payload, passed before separate Initialize.
    pub initialization_parameters: Option<OperationData>,
}

/// Native KeyValuePair<KUri, IStateProvider2> enumerator item.
pub type StateProviderEntry = (String, Arc<dyn IStateProvider2>);

/// Native GetOrAddAsync's stateProvider and alreadyExist out parameters.
#[derive(Clone)]
pub struct GetOrAddResult {
    /// Nonnull provider from locked lookup or newly admitted creation.
    pub state_provider: Arc<dyn IStateProvider2>,
    /// Existed after lookup/recheck; false is admission, not committed creation.
    pub already_exists: bool,
}

/// Native common/TryRemoveVersionResult.h. None preserves a null native set;
/// notification entries represent distinct visibility sequence numbers and only
/// newly registered (provider, VSN) notifications. No new notifications does not
/// prove removability.
/// [Removal](https://github.com/microsoft/service-fabric/blob/51a7703a1f83749a9234d3ceb4296d8622e42cca/src/prod/src/data/txnreplicator/loggingreplicator/VersionManager.cpp#L187-L274).
#[derive(Clone)]
pub struct TryRemoveVersionResult {
    /// Whether reader registrations currently permit caller's removal.
    pub can_be_removed: bool,
    /// Optional set of retaining reader VSNs, possibly conservative at next boundary.
    pub enumeration_set: Option<BTreeSet<SequenceNumber>>,
    /// Optional newly registered completion observations, not all retaining readers.
    pub enumeration_completion_notifications: Option<Vec<Arc<dyn EnumerationCompletionResult>>>,
}

/// Native FABRIC_BACKUP_OPTION values. Declaration does not enable backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BackupOption {
    /// Invalid choice, not an enabled backup mode.
    Invalid = 0,
    /// Full checkpoint and log backup.
    Full = 1,
    /// Incremental logs linked to prior completed backup chain.
    Incremental = 2,
}

/// Native FABRIC_RESTORE_POLICY values. Declaration does not enable restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RestorePolicy {
    /// Invalid choice, not a restore policy.
    Invalid = 0,
    /// Require backup epoch/tail strictly ahead of current state.
    Safe = 1,
    /// Bypass safety comparison only, not restore eligibility/lifecycle checks.
    Force = 2,
}

/// Native common/BackupVersion.h epoch/LSN pair, not a visible commit frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupVersion {
    /// Epoch provenance of backed-up records.
    pub epoch: Epoch,
    /// Backed-up record LSN, not snapshot visibility number.
    pub lsn: SequenceNumber,
}

/// Native common/BackupInfo.h callback/result fields.
#[derive(Clone, Debug)]
pub struct BackupInfo {
    /// Owned local backup directory supplied to external upload callback.
    pub directory: PathBuf,
    /// Full versus incremental capture option.
    pub option: BackupOption,
    /// Highest backed-up epoch/LSN.
    pub version: BackupVersion,
    /// Starting backed-up epoch/LSN.
    pub start_version: BackupVersion,
    /// Identity of this backup.
    pub backup_id: uuid::Uuid,
    /// Full backup uses its own ID; incremental links previous completed backup.
    pub parent_backup_id: uuid::Uuid,
}
