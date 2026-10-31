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
    Active = 0,
    Reading = 1,
    Committing = 2,
    Aborting = 3,
    Committed = 4,
    Aborted = 5,
    Faulted = 6,
}

/// Native common/ApplyContext.h values, including role/operation masks.
/// Operation values are not independent bitflags: Unlock=3, FalseProgress=5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ApplyContext {
    Invalid = 0,
    Primary = 0x1 << 16,
    Secondary = 0x2 << 16,
    Recovery = 0x4 << 16,
    RoleMask = -65536,
    Redo = 0x2,
    Unlock = 0x3,
    Undo = 0x4,
    FalseProgress = 0x5,
    OperationMask = 65535,
    PrimaryRedo = (0x1 << 16) | 0x2,
    PrimaryUndo = (0x1 << 16) | 0x4,
    PrimaryUnlock = (0x1 << 16) | 0x3,
    SecondaryRedo = (0x2 << 16) | 0x2,
    SecondaryUndo = (0x2 << 16) | 0x4,
    SecondaryUnlock = (0x2 << 16) | 0x3,
    SecondaryFalseProgress = (0x2 << 16) | 0x5,
    RecoveryRedo = (0x4 << 16) | 0x2,
    RecoveryUndo = (0x4 << 16) | 0x4,
}

/// Native common/StateProviderInfo.h child-provider description.
#[derive(Clone, Debug)]
pub struct StateProviderInfo {
    pub type_name: String,
    pub name: String,
    pub initialization_parameters: Option<OperationData>,
}

/// Native statemanager/FactoryArguments.h; construction does not initialize/open
/// the provider. Parentage is supplied separately through Initialize's children.
#[derive(Clone, Debug)]
pub struct FactoryArguments {
    pub name: String,
    pub state_provider_id: StateProviderId,
    pub type_name: String,
    pub partition_id: uuid::Uuid,
    pub replica_id: i64,
    pub initialization_parameters: Option<OperationData>,
}

/// Native KeyValuePair<KUri, IStateProvider2> enumerator item.
pub type StateProviderEntry = (String, Arc<dyn IStateProvider2>);

/// Native GetOrAddAsync's stateProvider and alreadyExist out parameters.
#[derive(Clone)]
pub struct GetOrAddResult {
    pub state_provider: Arc<dyn IStateProvider2>,
    pub already_exists: bool,
}

/// Native common/TryRemoveVersionResult.h. None preserves a null native set;
/// notification entries represent distinct visibility sequence numbers.
#[derive(Clone)]
pub struct TryRemoveVersionResult {
    pub can_be_removed: bool,
    pub enumeration_set: Option<BTreeSet<SequenceNumber>>,
    pub enumeration_completion_notifications: Option<Vec<Arc<dyn EnumerationCompletionResult>>>,
}

/// Native FABRIC_BACKUP_OPTION values. Declaration does not enable backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BackupOption {
    Invalid = 0,
    Full = 1,
    Incremental = 2,
}

/// Native FABRIC_RESTORE_POLICY values. Declaration does not enable restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RestorePolicy {
    Invalid = 0,
    Safe = 1,
    Force = 2,
}

/// Native common/BackupVersion.h epoch/LSN pair, not a visible commit frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupVersion {
    pub epoch: Epoch,
    pub lsn: SequenceNumber,
}

/// Native common/BackupInfo.h callback/result fields.
#[derive(Clone, Debug)]
pub struct BackupInfo {
    pub directory: PathBuf,
    pub option: BackupOption,
    pub version: BackupVersion,
    pub start_version: BackupVersion,
    pub backup_id: uuid::Uuid,
    pub parent_backup_id: uuid::Uuid,
}
