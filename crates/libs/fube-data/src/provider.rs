// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Bounded volatile demonstration provider, not a durable store.
//!
//! The history and request-result ledger are copied at the exact requested
//! horizon. No identity silently expires. Capacity rejects NEW admission rather
//! than forgetting a prior request. Whole authoritative volatile-state loss
//! loses both values and identity protection.

use crate::{
    Cancellation, Epoch, Error, OperationData, OperationDataStream, ReplayOperation, Result,
    StateProvider,
};
use fube_runtime::ServiceFuture;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

const CAPACITY: usize = 4096;
const BYTES: usize = 16 * 1024 * 1024;
const MAX_VALUE: usize = 64 * 1024;
const MAX_OPERATION: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation {
    pub id: String,
    pub key: String,
    pub value: String,
}
impl Mutation {
    fn validate(&self) -> Result<()> {
        if self.id.is_empty()
            || self.id.len() > 128
            || self.key.is_empty()
            || self.key.len() > 1024
            || self.value.len() > MAX_VALUE
        {
            return Err(Error::InvalidInput("mutation identity/key/value limits"));
        }
        Ok(())
    }
    pub fn operation(&self) -> Result<OperationData> {
        self.validate()?;
        encode(&Payload::Mutation {
            version: 1,
            mutation: self.clone(),
            origin_epoch: None,
        })
    }
    /// Stamp the actual admitted primary operation epoch, not a future replay's
    /// epoch. Callers must fence submission against this same authority epoch.
    pub fn operation_for_epoch(&self, epoch: Epoch) -> Result<OperationData> {
        self.validate()?;
        if epoch.data_loss < 0 || epoch.configuration < 0 || epoch != epoch.primary() {
            return Err(Error::InvalidInput("mutation primary operation epoch"));
        }
        let data = encode(&Payload::Mutation {
            version: 2,
            mutation: self.clone(),
            origin_epoch: Some(OriginEpoch::from(epoch)),
        })?;
        validate_copy_bound(self, Some(&data))?;
        Ok(data)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderSnapshot {
    pub last: i64,
    pub copy_in_progress: bool,
    pub values: BTreeMap<String, String>,
    pub results: BTreeMap<String, i64>,
    pub history_count: usize,
    pub retained_bytes: usize,
    pub epoch_data_loss: i64,
    pub epoch_configuration: i64,
    pub data_loss_calls: usize,
    pub data_loss_changed_calls: usize,
    pub last_data_loss_boundary: Option<i64>,
}

struct Record {
    sequence: i64,
    mutation: Mutation,
    replay: Option<ReplayOperation>,
    // Drop the charge after all record-owned payloads, including snapshot pins.
    _charge: RetainedCharge,
}
struct RetainedCharge {
    bytes: usize,
    budget: Arc<AtomicUsize>,
}
impl Drop for RetainedCharge {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
#[derive(Default)]
struct State {
    last: i64,
    history: BTreeMap<i64, Arc<Record>>,
    values: BTreeMap<String, String>,
    ledger: BTreeMap<String, (Mutation, i64)>,
    budget: Arc<AtomicUsize>,
    copying: Option<i64>,
    epoch: Option<Epoch>,
    data_loss: Option<i64>,
    data_loss_calls: usize,
    data_loss_changed_calls: usize,
    last_data_loss_boundary: Option<i64>,
}

#[derive(Default)]
pub struct MemoryProvider {
    state: Mutex<State>,
}
impl MemoryProvider {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    /// Validate before submitting to the engine. Some is the original result.
    pub fn admission(&self, mutation: &Mutation) -> Result<Option<i64>> {
        mutation.validate()?;
        let state = self.state.lock().unwrap();
        if state.copying.is_some() {
            return Err(Error::Closed);
        }
        // Largest valid epoch has the largest canonical metadata. This also
        // validates copy escaping before either producer submits an operation.
        let data = mutation.operation_for_epoch(Epoch {
            data_loss: i64::MAX,
            configuration: i64::MAX & !0xffff_ffff,
        })?;
        check_admission(&state, mutation, Some(&data))
    }
    pub fn snapshot(&self) -> ProviderSnapshot {
        let state = self.state.lock().unwrap();
        ProviderSnapshot {
            last: state.last,
            copy_in_progress: state.copying.is_some(),
            values: state.values.clone(),
            results: state
                .ledger
                .iter()
                .map(|(id, (_, lsn))| (id.clone(), *lsn))
                .collect(),
            history_count: state.history.len(),
            retained_bytes: state.budget.load(Ordering::Acquire),
            epoch_data_loss: state.epoch.map_or(0, |e| e.data_loss),
            epoch_configuration: state.epoch.map_or(0, |e| e.configuration),
            data_loss_calls: state.data_loss_calls,
            data_loss_changed_calls: state.data_loss_changed_calls,
            last_data_loss_boundary: state.last_data_loss_boundary,
        }
    }
    /// Primary local application or secondary application BEFORE explicit ACK.
    pub fn apply_replication(&self, sequence: i64, data: &OperationData) -> Result<i64> {
        let (mutation, epoch) = replication_payload(data)?;
        let replay = epoch.map(|epoch| ReplayOperation {
            epoch,
            data: data.clone(),
        });
        let mut state = self.state.lock().unwrap();
        if state.copying.is_some() {
            return Err(Error::Protocol("replication before copied state"));
        }
        apply(&mut state, sequence, mutation, replay)
    }
    pub fn apply_copy(&self, data: &OperationData) -> Result<()> {
        let payload = decode(data)?;
        let mut state = self.state.lock().unwrap();
        match payload {
            Payload::CopyBegin {
                version: 1,
                boundary,
            } if boundary >= 0 => {
                if state.copying.is_some() {
                    return Err(Error::Protocol("overlapping snapshot"));
                }
                let epoch = state.epoch;
                *state = State {
                    budget: state.budget.clone(),
                    copying: Some(boundary),
                    epoch,
                    data_loss: state.data_loss,
                    data_loss_calls: state.data_loss_calls,
                    data_loss_changed_calls: state.data_loss_changed_calls,
                    last_data_loss_boundary: state.last_data_loss_boundary,
                    ..State::default()
                };
            }
            Payload::CopyRecord {
                version,
                sequence,
                mutation,
                replay,
            } if (version == 1 && replay.is_none()) || (version == 2 && replay.is_some()) => {
                let boundary = state
                    .copying
                    .ok_or(Error::Protocol("copy record outside snapshot"))?;
                if sequence > boundary {
                    return Err(Error::Protocol("copy record beyond horizon"));
                }
                let replay = replay.map(|replay| replay.restore(&mutation)).transpose()?;
                apply(&mut state, sequence, mutation, replay)?;
            }
            Payload::CopyEnd {
                version: 1,
                boundary,
            } => {
                if state.copying != Some(boundary) || state.last != boundary {
                    return Err(Error::Protocol("incomplete or inconsistent snapshot"));
                }
                state.copying = None;
            }
            _ => return Err(Error::Protocol("invalid copy provider payload")),
        }
        Ok(())
    }
    /// Explicit qualification hook; callback, not this method, changes history.
    pub fn request_data_loss(&self, boundary: i64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if boundary < 0 || boundary > state.last {
            return Err(Error::InvalidInput("data loss boundary"));
        }
        state.data_loss = Some(boundary);
        Ok(())
    }
}

fn check_admission(
    state: &State,
    mutation: &Mutation,
    data: Option<&OperationData>,
) -> Result<Option<i64>> {
    if let Some((old, result)) = state.ledger.get(&mutation.id) {
        return if old == mutation {
            Ok(Some(*result))
        } else {
            Err(Error::InvalidInput("conflicting request identity"))
        };
    }
    let charge = charge(mutation, data);
    if state.history.len() >= CAPACITY
        || state.ledger.len() >= CAPACITY
        || charge > BYTES.saturating_sub(state.budget.load(Ordering::Acquire))
    {
        return Err(Error::ResourceLimit("provider history/identity ledger"));
    }
    Ok(None)
}
fn charge(m: &Mutation, data: Option<&OperationData>) -> usize {
    // History, value, ledger, BTree/Arc descriptors, and exact replay buffers.
    1024 + m.id.len() * 2
        + m.key.len() * 3
        + m.value.len() * 3
        + data.map_or(0, OperationData::retained_bytes)
}
fn apply(
    state: &mut State,
    sequence: i64,
    mutation: Mutation,
    replay: Option<ReplayOperation>,
) -> Result<i64> {
    mutation.validate()?;
    if sequence <= 0 {
        return Err(Error::Protocol("nonpositive provider LSN"));
    }
    if let Some(old) = state.history.get(&sequence) {
        if old.mutation != mutation
            || old.replay.as_ref().map(|r| (r.epoch, &r.data))
                != replay.as_ref().map(|r| (r.epoch, &r.data))
        {
            return Err(Error::Protocol("conflicting provider LSN"));
        }
        return Ok(state.ledger[&mutation.id].1);
    }
    if sequence
        != state
            .last
            .checked_add(1)
            .ok_or(Error::Protocol("provider LSN overflow"))?
    {
        return Err(Error::Protocol("provider sequence gap"));
    }
    let data = replay.as_ref().map(|r| &r.data);
    validate_copy_bound(&mutation, data)?;
    let existing = check_admission(state, &mutation, data)?;
    let retained = charge(&mutation, data);
    if state.history.len() >= CAPACITY
        || retained > BYTES.saturating_sub(state.budget.load(Ordering::Acquire))
    {
        return Err(Error::ResourceLimit("provider operation history"));
    }
    let result = existing.unwrap_or(sequence);
    if existing.is_none() {
        state
            .values
            .insert(mutation.key.clone(), mutation.value.clone());
        state
            .ledger
            .insert(mutation.id.clone(), (mutation.clone(), sequence));
    }
    state.budget.fetch_add(retained, Ordering::AcqRel);
    state.history.insert(
        sequence,
        Arc::new(Record {
            sequence,
            mutation,
            replay,
            _charge: RetainedCharge {
                bytes: retained,
                budget: state.budget.clone(),
            },
        }),
    );
    state.last = sequence;
    Ok(result)
}
fn reconcile(state: &mut State, boundary: i64) -> Result<()> {
    if boundary < 0 || boundary > state.last {
        return Err(Error::Protocol("unavailable authoritative history"));
    }
    let retained: Vec<_> = state
        .history
        .range(..=boundary)
        .map(|(_, r)| r.clone())
        .collect();
    let epoch = state.epoch;
    *state = State {
        budget: state.budget.clone(),
        epoch,
        data_loss: state.data_loss,
        data_loss_calls: state.data_loss_calls,
        data_loss_changed_calls: state.data_loss_changed_calls,
        last_data_loss_boundary: state.last_data_loss_boundary,
        ..State::default()
    };
    for r in retained {
        if !state.ledger.contains_key(&r.mutation.id) {
            state
                .values
                .insert(r.mutation.key.clone(), r.mutation.value.clone());
            state
                .ledger
                .insert(r.mutation.id.clone(), (r.mutation.clone(), r.sequence));
        }
        state.last = r.sequence;
        state.history.insert(r.sequence, r);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Payload {
    Mutation {
        version: u32,
        mutation: Mutation,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_epoch: Option<OriginEpoch>,
    },
    CopyBegin {
        version: u32,
        boundary: i64,
    },
    CopyRecord {
        version: u32,
        sequence: i64,
        mutation: Mutation,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replay: Option<CopyReplay>,
    },
    CopyEnd {
        version: u32,
        boundary: i64,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginEpoch {
    data_loss: i64,
    configuration: i64,
}
impl From<Epoch> for OriginEpoch {
    fn from(epoch: Epoch) -> Self {
        Self {
            data_loss: epoch.data_loss,
            configuration: epoch.configuration,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopyReplay {
    // The full JSON is UTF8; an individual original segment need not be.
    raw: String,
    segment_lengths: Vec<usize>,
}
impl CopyReplay {
    fn capture(data: &OperationData) -> Result<Self> {
        let bytes = data
            .segments()
            .iter()
            .flat_map(|s| s.iter().copied())
            .collect();
        Ok(Self {
            raw: String::from_utf8(bytes).map_err(|_| Error::Protocol("provider replay UTF8"))?,
            segment_lengths: data.segments().iter().map(|s| s.len()).collect(),
        })
    }
    fn restore(self, mutation: &Mutation) -> Result<ReplayOperation> {
        if self.segment_lengths.len() > 1024 {
            return Err(Error::Protocol("provider replay segments"));
        }
        let mut position = 0usize;
        let mut segments = Vec::with_capacity(self.segment_lengths.len());
        for length in self.segment_lengths {
            let end = position
                .checked_add(length)
                .ok_or(Error::Protocol("provider replay length overflow"))?;
            segments.push(
                self.raw
                    .as_bytes()
                    .get(position..end)
                    .ok_or(Error::Protocol("provider replay lengths"))?
                    .to_vec(),
            );
            position = end;
        }
        if position != self.raw.len() {
            return Err(Error::Protocol("provider replay trailing bytes"));
        }
        let data = OperationData::new(segments)?;
        let (original, epoch) = replication_payload(&data)?;
        if &original != mutation {
            return Err(Error::Protocol("provider replay mutation mismatch"));
        }
        Ok(ReplayOperation {
            epoch: epoch.ok_or(Error::Protocol("provider replay missing origin epoch"))?,
            data,
        })
    }
}

fn replication_payload(data: &OperationData) -> Result<(Mutation, Option<Epoch>)> {
    match decode(data)? {
        Payload::Mutation {
            version: 1,
            mutation,
            origin_epoch: None,
        } => Ok((mutation, None)),
        Payload::Mutation {
            version: 2,
            mutation,
            origin_epoch: Some(epoch),
        } => {
            let epoch = Epoch {
                data_loss: epoch.data_loss,
                configuration: epoch.configuration,
            };
            if epoch.data_loss < 0 || epoch.configuration < 0 || epoch != epoch.primary() {
                return Err(Error::Protocol("provider original operation epoch"));
            }
            Ok((mutation, Some(epoch)))
        }
        _ => Err(Error::Protocol("invalid replication provider payload")),
    }
}

fn copy_record(sequence: i64, mutation: Mutation, data: Option<&OperationData>) -> Result<Payload> {
    Ok(Payload::CopyRecord {
        version: if data.is_some() { 2 } else { 1 },
        sequence,
        mutation,
        replay: data.map(CopyReplay::capture).transpose()?,
    })
}

fn validate_copy_bound(mutation: &Mutation, data: Option<&OperationData>) -> Result<()> {
    // Reserve the largest sequence spelling before admission. Nested JSON
    // escaping and all original segment descriptors must fit one copy item.
    encode(&copy_record(i64::MAX, mutation.clone(), data)?)?;
    Ok(())
}

fn encode(payload: &Payload) -> Result<OperationData> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|_| Error::InvalidInput("provider payload encoding"))?;
    // Two real segments (including an empty prefix) exercise provider segmentation.
    OperationData::new(vec![Vec::new(), bytes])
}
fn decode(data: &OperationData) -> Result<Payload> {
    // Schema strings may expand sixfold through JSON escaping. Match the
    // actual operation/encoder bound, not a smaller unescaped-value estimate.
    if data.bytes() > MAX_OPERATION {
        return Err(Error::Protocol("provider payload bound"));
    }
    let bytes: Vec<_> = data
        .segments()
        .iter()
        .flat_map(|s| s.iter().copied())
        .collect();
    serde_json::from_slice(&bytes).map_err(|_| Error::Protocol("provider payload schema"))
}

struct SnapshotStream {
    boundary: i64,
    records: Vec<Arc<Record>>,
    position: Mutex<usize>,
}
impl OperationDataStream for SnapshotStream {
    fn next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            let mut position = self.position.lock().unwrap();
            let item = if *position == 0 {
                Some(encode(&Payload::CopyBegin {
                    version: 1,
                    boundary: self.boundary,
                })?)
            } else if let Some(r) = self.records.get(*position - 1) {
                Some(encode(&copy_record(
                    r.sequence,
                    r.mutation.clone(),
                    r.replay.as_ref().map(|r| &r.data),
                )?)?)
            } else if *position == self.records.len() + 1 {
                Some(encode(&Payload::CopyEnd {
                    version: 1,
                    boundary: self.boundary,
                })?)
            } else {
                None
            };
            if item.is_some() {
                *position += 1;
            }
            Ok(item)
        })
    }
}
struct Empty;
impl OperationDataStream for Empty {
    fn next(&self, cancellation: Cancellation) -> ServiceFuture<'_, Option<OperationData>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(Error::Closed)
            } else {
                Ok(None)
            }
        })
    }
}
impl StateProvider for MemoryProvider {
    fn last_committed(&self) -> Result<i64> {
        Ok(self.state.lock().unwrap().last)
    }
    fn replication_checkpoint(
        &self,
        sequence: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Option<ReplayOperation>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            let state = self.state.lock().unwrap();
            if state.copying.is_some() {
                return Ok(None);
            }
            Ok(state
                .history
                .get(&sequence)
                .and_then(|record| record.replay.clone()))
        })
    }
    fn update_epoch(
        &self,
        epoch: Epoch,
        previous: i64,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            let mut state = self.state.lock().unwrap();
            if state.epoch.is_some_and(|old| epoch < old) {
                return Err(Error::Protocol("stale provider epoch"));
            }
            if state.copying.is_some() {
                return Err(Error::Protocol("epoch during incomplete provider copy"));
            }
            // Native -1 denotes no prior initialized epoch, not an ordinary LSN.
            if previous >= 0 {
                reconcile(&mut state, previous)?;
            } else if previous != -1 {
                return Err(Error::Protocol("invalid prior epoch boundary"));
            }
            state.epoch = Some(epoch);
            Ok(())
        })
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            let mut state = self.state.lock().unwrap();
            state.data_loss_calls = state.data_loss_calls.saturating_add(1);
            if let Some(boundary) = state.data_loss.take() {
                reconcile(&mut state, boundary)?;
                state.data_loss_changed_calls = state.data_loss_changed_calls.saturating_add(1);
                state.last_data_loss_boundary = Some(boundary);
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
    fn copy_context(&self) -> Result<Arc<dyn OperationDataStream>> {
        Ok(Arc::new(Empty))
    }
    fn copy_state(
        &self,
        up_to: i64,
        context: Option<Arc<dyn OperationDataStream>>,
    ) -> Result<Arc<dyn OperationDataStream>> {
        if context.is_some() {
            return Err(Error::InvalidInput("volatile copy context must be null"));
        }
        let state = self.state.lock().unwrap();
        if up_to < 0 || up_to > state.last || state.copying.is_some() {
            return Err(Error::Protocol("unavailable exact copy horizon"));
        }
        let records = state
            .history
            .range(..=up_to)
            .map(|(_, r)| r.clone())
            .collect();
        Ok(Arc::new(SnapshotStream {
            boundary: up_to,
            records,
            position: Mutex::new(0),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const ORIGINAL_EPOCH: Epoch = Epoch {
        data_loss: 7,
        configuration: 3 << 32,
    };
    fn mutation(n: i64) -> Mutation {
        Mutation {
            id: format!("r{n}"),
            key: format!("k{}", n % 7),
            value: n.to_string(),
        }
    }
    #[tokio::test]
    async fn exact_snapshot_copies_ledger_then_live_tail_without_double_apply() {
        let source = MemoryProvider::new();
        for n in 1..=100 {
            source
                .apply_replication(n, &mutation(n).operation().unwrap())
                .unwrap();
        }
        let stream = source.copy_state(50, None).unwrap();
        for n in 101..=200 {
            source
                .apply_replication(n, &mutation(n).operation().unwrap())
                .unwrap();
        }
        let target = MemoryProvider::new();
        while let Some(data) = stream.next(Cancellation::default()).await.unwrap() {
            target.apply_copy(&data).unwrap();
        }
        assert_eq!(target.snapshot().last, 50);
        assert_eq!(target.admission(&mutation(30)).unwrap(), Some(30));
        for n in 51..=200 {
            target
                .apply_replication(n, &mutation(n).operation().unwrap())
                .unwrap();
        }
        assert_eq!(source.snapshot().values, target.snapshot().values);
        assert_eq!(source.snapshot().results, target.snapshot().results);
        assert!(source.copy_state(201, None).is_err());
        let mut bad = mutation(30);
        bad.value = "different".into();
        assert!(target.admission(&bad).is_err());
        let before = target.snapshot().values;
        target
            .apply_replication(201, &mutation(30).operation().unwrap())
            .unwrap();
        assert_eq!(target.snapshot().values, before);
        assert_eq!(target.admission(&mutation(30)).unwrap(), Some(30));
    }
    #[tokio::test]
    async fn epoch_and_changed_unchanged_data_loss_reconcile_contents_and_progress() {
        let p = MemoryProvider::new();
        for n in 1..=10 {
            p.apply_replication(n, &mutation(n).operation().unwrap())
                .unwrap();
        }
        assert!(!p.on_data_loss(Cancellation::default()).await.unwrap());
        p.update_epoch(
            Epoch {
                data_loss: 1,
                configuration: 1,
            },
            7,
            Cancellation::default(),
        )
        .await
        .unwrap();
        assert_eq!(p.last_committed().unwrap(), 7);
        assert_eq!(p.admission(&mutation(8)).unwrap(), None);
        p.apply_replication(8, &mutation(80).operation().unwrap())
            .unwrap();
        p.request_data_loss(3).unwrap();
        // A node epoch transition cannot silently clear the armed fixture hook.
        p.update_epoch(
            Epoch {
                data_loss: 2,
                configuration: 1,
            },
            8,
            Cancellation::default(),
        )
        .await
        .unwrap();
        assert!(p.on_data_loss(Cancellation::default()).await.unwrap());
        assert_eq!(p.last_committed().unwrap(), 3);
        p.apply_replication(4, &mutation(90).operation().unwrap())
            .unwrap();
        assert_eq!(p.last_committed().unwrap(), 4);
        assert_eq!(p.snapshot().data_loss_calls, 2);
        assert_eq!(p.snapshot().data_loss_changed_calls, 1);
        assert_eq!(p.snapshot().last_data_loss_boundary, Some(3));
    }
    #[test]
    fn capacity_retains_retry_protection_and_rejects_gaps() {
        let p = MemoryProvider::new();
        assert!(p
            .apply_replication(2, &mutation(2).operation().unwrap())
            .is_err());
        for n in 1..=CAPACITY as i64 {
            p.apply_replication(n, &mutation(n).operation().unwrap())
                .unwrap();
        }
        assert!(p.admission(&mutation(CAPACITY as i64 + 1)).is_err());
        assert_eq!(p.admission(&mutation(1)).unwrap(), Some(1));
        assert_eq!(p.snapshot().history_count, CAPACITY);
        assert!(p.snapshot().retained_bytes <= BYTES);
    }

    #[tokio::test]
    async fn admitted_maximum_escaped_strings_decode_for_replication_and_copy() {
        let p = MemoryProvider::new();
        let mutation = Mutation {
            id: "\0".repeat(128),
            key: "\0".repeat(1024),
            value: "\0".repeat(MAX_VALUE),
        };
        assert_eq!(p.admission(&mutation).unwrap(), None);
        let data = mutation
            .operation_for_epoch(Epoch {
                data_loss: i64::MAX,
                configuration: i64::MAX & !0xffff_ffff,
            })
            .unwrap();
        assert!(data.bytes() > MAX_VALUE * 2);
        assert!(data.bytes() < 1024 * 1024);
        p.apply_replication(1, &data).unwrap();
        let copy = p.copy_state(1, None).unwrap();
        let target = MemoryProvider::new();
        while let Some(data) = copy.next(Cancellation::default()).await.unwrap() {
            target.apply_copy(&data).unwrap();
        }
        assert_eq!(p.snapshot().values, target.snapshot().values);
        assert_eq!(target.admission(&mutation).unwrap(), Some(1));
        assert_eq!(
            target
                .replication_checkpoint(1, Cancellation::default())
                .await
                .unwrap()
                .unwrap()
                .data,
            data
        );
    }

    #[tokio::test]
    async fn checkpoints_preserve_original_epoch_bytes_and_segments_through_copy_and_reconciliation(
    ) {
        let source = MemoryProvider::new();
        let mut m = mutation(1);
        m.value = "☃".into();
        let raw = format!(
            " \n{{\"mutation\":{},\"origin_epoch\":{{\"configuration\":{},\"data_loss\":7}},\"version\":2,\"kind\":\"Mutation\"}}\t",
            serde_json::to_string(&m).unwrap(),
            ORIGINAL_EPOCH.configuration
        ).into_bytes();
        let split = raw.windows(3).position(|w| w == "☃".as_bytes()).unwrap() + 1;
        let data = OperationData::new(vec![
            vec![],
            raw[..split].to_vec(),
            vec![],
            raw[split..].to_vec(),
            vec![],
        ])
        .unwrap();
        // A native secondary can receive payloads without any UpdateEpoch call.
        source.apply_replication(1, &data).unwrap();
        assert_eq!(source.state.lock().unwrap().epoch, None);
        source
            .apply_replication(2, &mutation(2).operation_for_epoch(ORIGINAL_EPOCH).unwrap())
            .unwrap();
        let copy = source.copy_state(1, None).unwrap();
        let copied = MemoryProvider::new();
        copied
            .apply_copy(&copy.next(Cancellation::default()).await.unwrap().unwrap())
            .unwrap();
        copied
            .apply_copy(&copy.next(Cancellation::default()).await.unwrap().unwrap())
            .unwrap();
        assert!(copied
            .replication_checkpoint(1, Cancellation::default())
            .await
            .unwrap()
            .is_none());
        copied
            .apply_copy(&copy.next(Cancellation::default()).await.unwrap().unwrap())
            .unwrap();
        let second_copy = copied.copy_state(1, None).unwrap();
        let twice = MemoryProvider::new();
        while let Some(item) = second_copy.next(Cancellation::default()).await.unwrap() {
            twice.apply_copy(&item).unwrap();
        }
        for provider in [&source, &copied, &twice] {
            let checkpoint = provider
                .replication_checkpoint(1, Cancellation::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(checkpoint.epoch, ORIGINAL_EPOCH);
            assert_eq!(checkpoint.data, data);
            assert_eq!(provider.admission(&m).unwrap(), Some(1));
        }
        source
            .update_epoch(
                Epoch {
                    data_loss: 8,
                    configuration: 5 << 32,
                },
                1,
                Cancellation::default(),
            )
            .await
            .unwrap();
        assert!(source
            .replication_checkpoint(2, Cancellation::default())
            .await
            .unwrap()
            .is_none());
        let checkpoint = source
            .replication_checkpoint(1, Cancellation::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.epoch, ORIGINAL_EPOCH);
        assert_eq!(checkpoint.data, data);
        source.request_data_loss(0).unwrap();
        assert!(source.on_data_loss(Cancellation::default()).await.unwrap());
        assert!(source
            .replication_checkpoint(1, Cancellation::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(source.admission(&m).unwrap(), None);
    }

    #[tokio::test]
    async fn legacy_unknown_metadata_and_cancellation_never_synthesize_a_checkpoint() {
        let p = MemoryProvider::new();
        p.apply_replication(1, &mutation(1).operation().unwrap())
            .unwrap();
        for sequence in [-1, 0, 1, 2] {
            assert!(p
                .replication_checkpoint(sequence, Cancellation::default())
                .await
                .unwrap()
                .is_none());
        }
        let copy = p.copy_state(1, None).unwrap();
        let target = MemoryProvider::new();
        while let Some(data) = copy.next(Cancellation::default()).await.unwrap() {
            target.apply_copy(&data).unwrap();
        }
        assert!(target
            .replication_checkpoint(1, Cancellation::default())
            .await
            .unwrap()
            .is_none());
        let cancel = Cancellation::default();
        cancel.cancel();
        assert!(target.replication_checkpoint(1, cancel).await.is_err());
        assert!(p
            .apply_replication(1, &mutation(1).operation_for_epoch(ORIGINAL_EPOCH).unwrap())
            .is_err());
    }

    #[test]
    fn replay_copy_expansion_and_invalid_segment_metadata_reject_before_state_application() {
        let m = mutation(1);
        let mut bytes = vec![b'\t'; MAX_OPERATION / 2];
        bytes.extend(m.operation_for_epoch(ORIGINAL_EPOCH).unwrap().segments()[1].iter());
        let oversized_copy = OperationData::new(vec![bytes]).unwrap();
        let p = MemoryProvider::new();
        assert!(matches!(
            p.apply_replication(1, &oversized_copy),
            Err(Error::ResourceLimit("operation bytes"))
        ));
        assert_eq!(p.snapshot().last, 0);
        assert_eq!(p.snapshot().retained_bytes, 0);
        let data = m.operation_for_epoch(ORIGINAL_EPOCH).unwrap();
        let mut replay = CopyReplay::capture(&data).unwrap();
        replay.segment_lengths.push(1);
        assert!(replay.restore(&m).is_err());
        let mut replay = CopyReplay::capture(&data).unwrap();
        replay.segment_lengths = vec![usize::MAX, 1];
        assert!(replay.restore(&m).is_err());
        let replay = CopyReplay::capture(&data).unwrap();
        assert!(replay.restore(&mutation(2)).is_err());
    }

    #[tokio::test]
    async fn pinned_snapshot_replay_ownership_stays_charged_until_actual_disposal() {
        let p = MemoryProvider::new();
        let mut m = mutation(1);
        m.value = "x".repeat(MAX_VALUE);
        let data = m.operation_for_epoch(ORIGINAL_EPOCH).unwrap();
        for n in 1.. {
            match p.apply_replication(n, &data) {
                Ok(_) => {}
                Err(Error::ResourceLimit(_)) => break,
                other => panic!("unexpected admission: {other:?}"),
            }
        }
        let before = p.snapshot();
        let pinned = p.copy_state(before.last, None).unwrap();
        p.update_epoch(ORIGINAL_EPOCH, 0, Cancellation::default())
            .await
            .unwrap();
        assert_eq!(p.snapshot().retained_bytes, before.retained_bytes);
        assert!(p.apply_replication(1, &data).is_err());
        drop(pinned);
        assert_eq!(p.snapshot().retained_bytes, 0);
        p.apply_replication(1, &data).unwrap();
        assert!(p.snapshot().retained_bytes >= data.retained_bytes());
    }
}
