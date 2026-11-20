use super::*;
use crate::{notification_protocol::*, resolution_protocol::*, NodeId};
use fubst::serde::{CountedStrings, CountedValues, Guid, ObjectArray, Pointer};

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
    progress.commit(page, receiver)
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
fn invalid_pages_do_not_publish_or_advance_progress() {
    let receiver = Receiver::default();
    let mut progress = Progress::default();
    let mut bad = page(0, &[(1, 1)], &[(2, 1)]);
    assert!(commit(&mut progress, &receiver, bad).is_err());
    bad = page(0, &[(1, 1)], &[]);
    bad.entries.0[0].0 = None;
    assert!(commit(&mut progress, &receiver, bad).is_err());
    assert!(progress.partitions.is_empty());
    assert!(progress.ranges.is_empty());
    assert!(receiver.inner.lock().unwrap().queue.is_empty());
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
