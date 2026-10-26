use super::*;
use crate::{notification_protocol::*, resolution_protocol::*, NodeId};
use mssf_transport::serialization::serde::{
    CountedStrings, CountedValues, Guid, ObjectArray, Pointer,
};

#[test]
fn prospective_rehash_and_range_copy_peaks_are_reserved_before_clone() {
    let receiver = Receiver::default();
    let mut progress = Progress {
        partitions: (1..=3500).map(|id| (Uuid::from_u128(id), 1)).collect(),
        ranges: (0..4000)
            .map(|i| Interval {
                start: i * 3,
                end: i * 3 + 1,
            })
            .collect(),
        ..Default::default()
    };
    let required = 4
        * (progress.partitions.capacity() * (size_of::<(Uuid, i64)>() + 16)
            + progress.ranges.capacity() * size_of::<Interval>());
    let memory = receiver.budget.reserve(4096).unwrap();
    let _occupied = receiver
        .budget
        .reserve(MAX_MEMORY - receiver._queue_memory.bytes - 4096 - (required - 1))
        .unwrap();
    assert!(matches!(
        progress.commit(page(0, &[(6000, 16000)], &[]), memory, &receiver),
        Err(NotificationError::Lagged)
    ));
    assert_eq!(progress.partitions.len(), 3500);
    assert_eq!(progress.ranges.len(), 4000);
    assert!(receiver.inner.lock().unwrap().queue.is_empty());
}

fn page(generation: i64, entries: &[(u128, i64)], ranges: &[(i64, i64)]) -> Page {
    Page {
        id: PageId {
            activity: Activity {
                guid: Guid([1; 16]),
                index: 1,
            },
            index: 0,
        },
        generation: Generation {
            value: generation,
            owner: NodeId { high: 0, low: 0 },
        },
        versions: Pointer(Some(Versions {
            ranges: ObjectArray(
                ranges
                    .iter()
                    .map(|(start, end)| Interval {
                        start: *start,
                        end: *end,
                    })
                    .collect(),
            ),
        })),
        entries: CountedValues(
            entries
                .iter()
                .map(|(id, lookup)| {
                    Pointer(Some(Entry {
                        entry: Pointer(Some(TableEntry {
                            cuid: Cuid {
                                id: Guid(Uuid::from_u128(*id).to_bytes_le()),
                            },
                            name: "fabric:/App/Service".into(),
                            replicas: ReplicaSet {
                                stateful: false,
                                primary_valid: false,
                                primary: String::new(),
                                replicas: CountedStrings(vec![]),
                                lookup: *lookup,
                            },
                            found: true,
                        })),
                        primary_only: false,
                    }))
                })
                .collect(),
        ),
    }
}
fn commit(progress: &mut Progress, receiver: &Receiver, page: Page) -> NotificationResult<()> {
    let memory = receiver.budget.reserve(1024 * 1024)?;
    progress.commit(page, memory, receiver)
}

#[tokio::test]
async fn retries_stale_versions_generations_and_empty_invalidations() {
    let receiver = Receiver::default();
    let mut progress = Progress::default();
    commit(&mut progress, &receiver, page(1, &[(1, 5)], &[(5, 6)])).unwrap();
    commit(&mut progress, &receiver, page(1, &[(1, 5)], &[(5, 6)])).unwrap();
    commit(&mut progress, &receiver, page(1, &[(1, 4)], &[])).unwrap();
    commit(&mut progress, &receiver, page(0, &[(2, 99)], &[])).unwrap();
    assert_eq!(receiver.inner.lock().unwrap().queue.len(), 1);
    assert!(receiver.recv().await.unwrap().unwrap().endpoints.is_empty());
    commit(&mut progress, &receiver, page(2, &[(1, 1)], &[(1, 2)])).unwrap();
    assert_eq!(receiver.recv().await.unwrap().unwrap().version.lookup, 1);
    assert_eq!(progress.partitions.len(), 1);
}

#[tokio::test]
async fn whole_page_overflow_rolls_back_and_terminal_bypasses_full_delivery() {
    let receiver = Receiver::default();
    let mut progress = Progress::default();
    let entries: Vec<_> = (1..=256).map(|i| (i, i as i64)).collect();
    commit(&mut progress, &receiver, page(1, &entries, &[])).unwrap();
    assert_eq!(receiver.inner.lock().unwrap().queue.len(), MAX_EVENTS);
    assert!(matches!(
        commit(
            &mut progress,
            &receiver,
            page(1, &[(999, 999), (1000, 1000)], &[(999, 1001)])
        ),
        Err(NotificationError::Lagged)
    ));
    assert_eq!(progress.partitions.len(), 256);
    assert!(progress.ranges.is_empty());
    receiver.fail(NotificationError::Lagged);
    assert!(matches!(
        receiver.recv().await,
        Some(Err(NotificationError::Lagged))
    ));
    assert!(receiver.recv().await.is_none());
    assert_eq!(
        *receiver.budget.0.lock().unwrap(),
        receiver._queue_memory.bytes
    );
}

#[test]
fn bookkeeping_limits_do_not_evict_or_advance_on_failure() {
    let receiver = Receiver::default();
    let mut progress = Progress {
        partitions: (1..=4096).map(|id| (Uuid::from_u128(id), 1)).collect(),
        ..Default::default()
    };
    assert!(matches!(
        commit(&mut progress, &receiver, page(0, &[(5000, 5)], &[])),
        Err(NotificationError::ResourceLimit(_))
    ));
    assert_eq!(progress.partitions.len(), MAX_IDENTITIES);
    progress.partitions.clear();
    progress.ranges = (0..4096)
        .map(|i| Interval {
            start: i * 3,
            end: i * 3 + 1,
        })
        .collect();
    assert!(matches!(
        commit(&mut progress, &receiver, page(0, &[], &[(15000, 15001)])),
        Err(NotificationError::ResourceLimit(_))
    ));
    assert_eq!(progress.ranges.len(), MAX_INTERVALS);
    assert!(receiver.inner.lock().unwrap().queue.is_empty());
}

#[test]
fn memory_budget_is_shared_and_released_on_every_path() {
    let budget = Arc::new(Budget::default());
    let memory = budget.reserve(MAX_MEMORY).unwrap();
    assert!(matches!(budget.reserve(1), Err(NotificationError::Lagged)));
    drop(memory);
    assert_eq!(*budget.0.lock().unwrap(), 0);
    let receiver = Receiver::default();
    let mut progress = Progress::default();
    let mut bad = page(0, &[(1, 1)], &[(2, 1)]);
    assert!(commit(&mut progress, &receiver, bad).is_err());
    assert_eq!(
        *receiver.budget.0.lock().unwrap(),
        receiver._queue_memory.bytes
    );
    bad = page(0, &[(1, 1)], &[]);
    bad.entries.0[0].0 = None;
    assert!(commit(&mut progress, &receiver, bad).is_err());
    assert_eq!(
        *receiver.budget.0.lock().unwrap(),
        receiver._queue_memory.bytes
    );
}

#[test]
fn reservation_growth_is_atomic_shared_and_released() {
    let budget = Arc::new(Budget::default());
    let mut memory = budget.reserve(MAX_MEMORY - 1).unwrap();
    let occupied = budget.reserve(1).unwrap();
    assert!(matches!(memory.grow(1), Err(NotificationError::Lagged)));
    assert_eq!(memory.bytes, MAX_MEMORY - 1);
    assert_eq!(*budget.0.lock().unwrap(), MAX_MEMORY);
    drop(occupied);
    memory.grow(1).unwrap();
    assert_eq!(memory.bytes, MAX_MEMORY);
    assert!(matches!(
        memory.grow(usize::MAX),
        Err(NotificationError::Lagged)
    ));
    drop(memory);
    assert_eq!(*budget.0.lock().unwrap(), 0);
}

#[tokio::test]
async fn derived_page_decoding_and_conversion_share_preallocation_budget() {
    use mssf_transport::serialization::serde::to_vec;

    let address = "opaque中🦀".repeat(1000);
    let mut value = page(1, &[(1, 1)], &[(1, 2)]);
    let table = value.entries.0[0]
        .0
        .as_mut()
        .unwrap()
        .entry
        .0
        .as_mut()
        .unwrap();
    table.replicas.stateful = true;
    table.replicas.primary_valid = true;
    table.replicas.primary = address.clone();
    table.replicas.replicas.0.push(address.clone());
    let body = to_vec(&PageBody {
        page: Pointer(Some(value)),
    })
    .unwrap();
    let mut decoded_bytes = 0;
    crate::notification_protocol::decode_page(&body, &mut |bytes| {
        decoded_bytes += bytes;
        Ok(())
    })
    .unwrap();
    let receiver = Receiver::default();
    let fixed = receiver._queue_memory.bytes;
    let mut progress = Progress::default();

    let mut memory = receiver.budget.reserve(body.capacity()).unwrap();
    let occupied = receiver
        .budget
        .reserve(MAX_MEMORY - fixed - memory.bytes - decoded_bytes)
        .unwrap();
    let decoded =
        crate::notification_protocol::decode_page(&body, &mut |bytes| memory.grow(bytes)).unwrap();
    assert_eq!(*receiver.budget.0.lock().unwrap(), MAX_MEMORY);
    assert!(matches!(
        progress.commit(decoded, memory, &receiver),
        Err(NotificationError::Lagged)
    ));
    assert!(progress.partitions.is_empty());
    assert!(progress.ranges.is_empty());
    assert!(receiver.inner.lock().unwrap().queue.is_empty());
    drop(occupied);
    assert_eq!(*receiver.budget.0.lock().unwrap(), fixed);

    let mut memory = receiver.budget.reserve(body.capacity()).unwrap();
    let occupied = receiver
        .budget
        .reserve(MAX_MEMORY - fixed - memory.bytes - (decoded_bytes - 1))
        .unwrap();
    assert!(matches!(
        crate::notification_protocol::decode_page(&body, &mut |bytes| memory.grow(bytes)),
        Err(NotificationError::Lagged)
    ));
    drop(memory);
    assert_eq!(*receiver.budget.0.lock().unwrap(), fixed + occupied.bytes);
    drop(occupied);

    let mut memory = receiver.budget.reserve(body.capacity()).unwrap();
    let decoded =
        crate::notification_protocol::decode_page(&body, &mut |bytes| memory.grow(bytes)).unwrap();
    drop(body);
    progress.commit(decoded, memory, &receiver).unwrap();
    let event = receiver.recv().await.unwrap().unwrap();
    assert_eq!(event.endpoints.len(), 2);
    assert!(event
        .endpoints
        .iter()
        .all(|endpoint| endpoint.address == address));
    assert_eq!(*receiver.budget.0.lock().unwrap(), fixed);
}

#[test]
fn serde_quota_bridge_preserves_scoped_failures_without_relabeling_protocol_errors() {
    let body = mssf_transport::serialization::serde::to_vec(&PageBody {
        page: Pointer(Some(page(1, &[(1, 1)], &[]))),
    })
    .unwrap();
    assert!(matches!(
        crate::notification_protocol::decode_page(&body, &mut |_| {
            Err(NotificationError::ResourceLimit(
                "injected allocation budget",
            ))
        }),
        Err(NotificationError::ResourceLimit(
            "injected allocation budget"
        ))
    ));
    assert!(matches!(
        crate::notification_protocol::decode_page(&body, &mut |_| { Err(Error::Timeout.into()) }),
        Err(NotificationError::Common(Error::Timeout))
    ));
    assert!(matches!(
        crate::notification_protocol::decode_page(&[0], &mut |_| Ok(())),
        Err(NotificationError::Common(Error::Protocol(_)))
    ));
}

#[tokio::test]
async fn cancelled_receive_does_not_consume_and_protocol_only_pages_advance() {
    let receiver = Receiver::default();
    let mut progress = Progress::default();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), receiver.recv())
            .await
            .is_err()
    );
    commit(&mut progress, &receiver, page(1, &[], &[(1, 5), (5, 8)])).unwrap();
    assert_eq!(progress.ranges.len(), 1);
    assert_eq!(progress.ranges[0].end, 8);
    assert!(receiver.inner.lock().unwrap().queue.is_empty());
    commit(&mut progress, &receiver, page(1, &[(1, 8)], &[])).unwrap();
    assert_eq!(receiver.recv().await.unwrap().unwrap().version.lookup, 8);
    receiver.finish();
    assert!(receiver.recv().await.is_none());
}
