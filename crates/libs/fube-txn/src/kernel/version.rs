// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native dispatching-Apply fence, reader registrations and version retention.
//!
//! SF 51a7703 `loggingreplicator/VersionManager.cpp:133–472` waits the
//! captured dispatch barrier without branching on its status, samples the
//! version provider under the list lock, and retains duplicate readers.
//! The removal predicate uses `[commit,next)`, but a nonempty retaining set
//! includes readers equal to `next`. Notification deduplication is per
//! `(provider,VSN)` and releases after the last duplicate outside the list lock.
//! The injected version callback is the last-barrier observation seam, not
//! log-tail/quorum/stability authority.

use crate::{
    ApiFuture, EnumerationCompletionResult, Error, ITransactionVersionManager, IVersionManager,
    Result, SequenceNumber, StateProviderId, TryRemoveVersionResult,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

/// Cloneable one-shot owned result with race-free independent observations.
///
/// A completion owner need not retain or poll any observation future. Results
/// remain shared as an Arc, so phase3 can distinguish Apply/quorum/processing
/// results while accepted workers survive observer drop.
pub(crate) struct Completion<T> {
    /// Shared sender retains the result even with no subscribed observers.
    result: Arc<watch::Sender<Option<Arc<Result<T>>>>>,
}

impl<T> Clone for Completion<T> {
    fn clone(&self) -> Self {
        Self {
            result: Arc::clone(&self.result),
        }
    }
}

impl<T> Default for Completion<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Completion<T> {
    /// Create an incomplete owned result; no runtime or observer is required.
    pub(crate) fn new() -> Self {
        let (result, _) = watch::channel(None);
        Self {
            result: Arc::new(result),
        }
    }

    /// Set a result at most once and wake every current/future observer.
    ///
    /// Returns false for repeated completion without overwriting the first result.
    pub(crate) fn complete(&self, result: Result<T>) -> bool {
        let result = Arc::new(result);
        self.result.send_if_modified(|slot| {
            if slot.is_some() {
                false
            } else {
                *slot = Some(result);
                true
            }
        })
    }

    /// Observe whether the source has a result, independently of outcome.
    pub(crate) fn is_completed(&self) -> bool {
        self.result.borrow().is_some()
    }

    /// Subscribe before checking the value; no lost-wakeup check/wait window.
    pub(crate) async fn wait(&self) -> Arc<Result<T>> {
        let mut observer = self.result.subscribe();
        loop {
            let result = observer.borrow_and_update().clone();
            if let Some(result) = result {
                return result;
            }
            observer
                .changed()
                .await
                .expect("completion owner retains the sender");
        }
    }

    /// Identify the same reader-release generation without comparing VSN alone.
    fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.result, &other.result)
    }
}

/// Native reader lists and notification registry protected by one list lock.
#[derive(Default)]
struct Readers {
    /// Nondecreasing visibility versions, preserving every duplicate.
    versions: Vec<SequenceNumber>,
    /// Shared completion source for each currently registered distinct VSN.
    removal: BTreeMap<SequenceNumber, Completion<SequenceNumber>>,
    /// Deduplicated provider notification observations and their release generation.
    notifications: BTreeMap<(StateProviderId, SequenceNumber), Completion<SequenceNumber>>,
}

/// Shared native owner retained by accepted registration work.
struct VersionState {
    pending_readers: std::sync::atomic::AtomicUsize,
    max_readers: std::sync::atomic::AtomicUsize,
    max_notifications: std::sync::atomic::AtomicUsize,
    /// Last dispatch barrier; snapshotted synchronously when Register is invoked.
    dispatching_barrier: Mutex<Option<Completion<()>>>,
    /// Native duplicate registrations and notification deduplication.
    readers: Mutex<Readers>,
    /// Narrow GetVersion callback, called under the reader-list lock.
    version_provider: Arc<dyn Fn() -> Result<SequenceNumber> + Send + Sync>,
}
struct PendingReader {
    inner: Arc<VersionState>,
    transferred: bool,
}
impl Drop for PendingReader {
    fn drop(&mut self) {
        if !self.transferred {
            self.inner
                .pending_readers
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

/// Private real version manager implementing the unchanged SDK contracts.
#[derive(Clone)]
pub(crate) struct VersionManager {
    /// Accepted registration retains this owner regardless of caller observation.
    inner: Arc<VersionState>,
}

impl VersionManager {
    /// Construct with the source last-applied-barrier observation callback.
    ///
    /// Native weak version-provider expiry asserts. An unavailable A0 observation
    /// can return a seam error; that is not an inferred native expiry status.
    pub(crate) fn new(
        version_provider: Arc<dyn Fn() -> Result<SequenceNumber> + Send + Sync>,
    ) -> Self {
        Self {
            inner: Arc::new(VersionState {
                pending_readers: std::sync::atomic::AtomicUsize::new(0),
                max_readers: std::sync::atomic::AtomicUsize::new(usize::MAX),
                max_notifications: std::sync::atomic::AtomicUsize::new(usize::MAX),
                dispatching_barrier: Mutex::new(None),
                readers: Mutex::new(Readers::default()),
                version_provider,
            }),
        }
    }

    /// Publish a dispatcher's barrier Apply task, not its quorum/flush task.
    pub(crate) fn update_dispatching_barrier(&self, completion: Completion<()>) {
        *self
            .inner
            .dispatching_barrier
            .lock()
            .expect("version fence poisoned") = Some(completion);
    }

    /// Observe duplicate registrations for deterministic bookkeeping evidence.
    pub(crate) fn registered_versions(&self) -> Vec<SequenceNumber> {
        self.inner
            .readers
            .lock()
            .expect("version readers poisoned")
            .versions
            .clone()
    }
    pub(crate) fn set_limits(&self, readers: usize, notifications: usize) {
        self.inner
            .max_readers
            .store(readers, std::sync::atomic::Ordering::Release);
        self.inner
            .max_notifications
            .store(notifications, std::sync::atomic::Ordering::Release);
    }

    /// Reset native lifecycle lists/fence without synthesizing reader releases.
    pub(crate) fn reuse(&self) {
        *self.inner.readers.lock().expect("version readers poisoned") = Readers::default();
        *self
            .inner
            .dispatching_barrier
            .lock()
            .expect("version fence poisoned") = None;
    }

    /// Sample and append while holding the exclusive native reader-list lock.
    fn reserve_reader(inner: &Arc<VersionState>) -> Result<PendingReader> {
        let readers = inner.readers.lock().expect("version readers poisoned");
        if readers.versions.len().saturating_add(
            inner
                .pending_readers
                .load(std::sync::atomic::Ordering::Acquire),
        ) >= inner.max_readers.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(super::status::Status::ServiceTooBusy.error());
        }
        inner
            .pending_readers
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(PendingReader {
            inner: inner.clone(),
            transferred: false,
        })
    }

    fn register_sample(
        inner: &VersionState,
        mut reserved: PendingReader,
    ) -> Result<SequenceNumber> {
        let mut readers = inner.readers.lock().expect("version readers poisoned");
        let version = (inner.version_provider)()?;
        if version < 0 || readers.versions.last().is_some_and(|last| version < *last) {
            return Err(Error::InvalidInput("nonmonotonic native barrier version"));
        }
        readers.versions.push(version);
        readers.removal.entry(version).or_default();
        reserved.transferred = true;
        inner
            .pending_readers
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        Ok(version)
    }
}

impl ITransactionVersionManager for VersionManager {
    fn register(&self) -> ApiFuture<'_, SequenceNumber> {
        let reserved = match Self::reserve_reader(&self.inner) {
            Ok(reserved) => reserved,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let fence = self
            .inner
            .dispatching_barrier
            .lock()
            .expect("version fence poisoned")
            .clone();
        let inner = Arc::clone(&self.inner);
        // Native KTL starts eagerly and does not suspend on absent/ready fences.
        // Preserve reader admission before returning its observation on this path.
        if fence.as_ref().is_none_or(Completion::is_completed) {
            let result = Self::register_sample(&inner, reserved);
            return Box::pin(async move { result });
        }
        let result = Completion::new();
        let observation = result.clone();
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                return Box::pin(async {
                    Err(Error::InvalidInput(
                        "A0 registration requires an owned Tokio executor",
                    ))
                });
            }
        };
        drop(runtime.spawn(async move {
            if let Some(fence) = fence {
                let _ = fence.wait().await;
            }
            result.complete(Self::register_sample(&inner, reserved));
        }));
        Box::pin(async move { observation.wait().await.as_ref().clone() })
    }

    fn unregister(&self, visibility_version_number: SequenceNumber) -> Result<()> {
        let notification = {
            let mut readers = self.inner.readers.lock().expect("version readers poisoned");
            let index = readers
                .versions
                .binary_search(&visibility_version_number)
                .map_err(|_| Error::InvalidInput("unregistered native visibility version"))?;
            readers.versions.remove(index);
            if readers
                .versions
                .binary_search(&visibility_version_number)
                .is_err()
            {
                readers.removal.remove(&visibility_version_number)
            } else {
                None
            }
        };
        if let Some(notification) = notification {
            notification.complete(Ok(visibility_version_number));
            // Native notification-task continuation follows release outside the lock.
            // Match the captured source as well as VSN if a new generation registered.
            let mut readers = self.inner.readers.lock().expect("version readers poisoned");
            readers.notifications.retain(|(_, version), source| {
                *version != visibility_version_number || !source.same_source(&notification)
            });
        }
        Ok(())
    }
}

impl IVersionManager for VersionManager {
    fn try_remove_checkpoint(
        &self,
        checkpoint_lsn_to_be_removed: SequenceNumber,
        next_checkpoint_lsn: SequenceNumber,
    ) -> ApiFuture<'_, ()> {
        let result = (|| {
            let intervening_barrier =
                checkpoint_lsn_to_be_removed
                    .checked_add(1)
                    .ok_or(Error::InvalidInput(
                        "checkpoint interval arithmetic overflow",
                    ))?;
            if next_checkpoint_lsn <= intervening_barrier {
                return Err(Error::InvalidInput(
                    "checkpoint requires intervening barrier",
                ));
            }
            let readers = self.inner.readers.lock().expect("version readers poisoned");
            let retaining = retaining_versions(
                &readers.versions,
                checkpoint_lsn_to_be_removed,
                next_checkpoint_lsn,
            );
            Ok(retaining
                .into_iter()
                .flatten()
                .map(|version| {
                    readers
                        .removal
                        .get(&version)
                        .expect("retaining reader has release source")
                        .clone()
                })
                .collect::<Vec<_>>())
        })();
        Box::pin(async move {
            for task in result? {
                task.wait().await.as_ref().clone()?;
            }
            Ok(())
        })
    }

    fn try_remove_version(
        &self,
        state_provider_id: StateProviderId,
        commit_lsn: SequenceNumber,
        next_commit_lsn: SequenceNumber,
    ) -> Result<TryRemoveVersionResult> {
        if next_commit_lsn <= commit_lsn {
            return Err(Error::InvalidInput("nonincreasing version interval"));
        }
        let mut readers = self.inner.readers.lock().expect("version readers poisoned");
        let Some(retaining) = retaining_versions(&readers.versions, commit_lsn, next_commit_lsn)
        else {
            return Ok(TryRemoveVersionResult {
                can_be_removed: true,
                enumeration_set: None,
                enumeration_completion_notifications: None,
            });
        };
        let new = retaining
            .iter()
            .filter(|version| {
                !readers
                    .notifications
                    .contains_key(&(state_provider_id, **version))
            })
            .count();
        if readers.notifications.len().saturating_add(new)
            > self
                .inner
                .max_notifications
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::ResourceLimit("version notification registry"));
        }
        let mut notifications: Vec<Arc<dyn EnumerationCompletionResult>> = Vec::new();
        for &version in &retaining {
            let key = (state_provider_id, version);
            if readers.notifications.contains_key(&key) {
                continue;
            }
            let completion = readers
                .removal
                .get(&version)
                .expect("retaining reader has release source")
                .clone();
            readers.notifications.insert(key, completion.clone());
            notifications.push(Arc::new(ReaderNotification {
                version,
                completion,
            }));
        }
        Ok(TryRemoveVersionResult {
            can_be_removed: false,
            enumeration_set: Some(retaining),
            enumeration_completion_notifications: (!notifications.is_empty())
                .then_some(notifications),
        })
    }
}

/// Native first-retainer test plus the inclusive upper collection quirk.
fn retaining_versions(
    versions: &[SequenceNumber],
    commit_lsn: SequenceNumber,
    next_commit_lsn: SequenceNumber,
) -> Option<BTreeSet<SequenceNumber>> {
    let first = versions.partition_point(|version| *version < commit_lsn);
    if first == versions.len() || versions[first] >= next_commit_lsn {
        return None;
    }
    Some(
        versions[first..]
            .iter()
            .copied()
            .take_while(|version| *version <= next_commit_lsn)
            .collect(),
    )
}

/// Shared native VSN/removal task projected through the existing SDK trait.
struct ReaderNotification {
    /// Stored native reader visibility sequence number.
    version: SequenceNumber,
    /// Same release source shared by every duplicate and provider notification.
    completion: Completion<SequenceNumber>,
}

impl EnumerationCompletionResult for ReaderNotification {
    fn get_visibility_sequence_number(&self) -> SequenceNumber {
        self.version
    }

    fn get_notification(&self) -> ApiFuture<'_, SequenceNumber> {
        Box::pin(async { self.completion.wait().await.as_ref().clone() })
    }

    fn get_is_notification_completed(&self) -> bool {
        self.completion.is_completed()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn absent_and_ready_fences_register_before_returning_observer() {
        use std::sync::atomic::{AtomicI64, Ordering};
        for ready_fence in [false, true] {
            let sample = Arc::new(AtomicI64::new(10));
            let source = sample.clone();
            let manager = VersionManager::new(Arc::new(move || Ok(source.load(Ordering::SeqCst))));
            if ready_fence {
                let fence = Completion::new();
                fence.complete(Err(Error::Closed));
                manager.update_dispatching_barrier(fence);
            }
            let observation = manager.register();
            sample.store(20, Ordering::SeqCst);
            assert!(!manager.try_remove_version(7, 5, 15).unwrap().can_be_removed);
            assert_eq!(manager.registered_versions(), vec![10]);
            assert_eq!(observation.await.unwrap(), 10);
            manager.unregister(10).unwrap();
        }
    }
    use super::*;
    use crate::kernel::status::Status;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicI64, Ordering},
        task::Poll,
    };

    fn manager(version: i64) -> (VersionManager, Arc<AtomicI64>) {
        let version = Arc::new(AtomicI64::new(version));
        let sample = Arc::clone(&version);
        (
            VersionManager::new(Arc::new(move || Ok(sample.load(Ordering::SeqCst)))),
            version,
        )
    }

    async fn is_pending<F: Future + Unpin>(future: &mut F) -> bool {
        std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_pending())).await
    }

    #[tokio::test]
    async fn v02_duplicates_notification_dedup_and_last_release() {
        let (manager, sample) = manager(10);
        assert_eq!(manager.register().await.unwrap(), 10);
        assert_eq!(manager.register().await.unwrap(), 10);
        sample.store(20, Ordering::SeqCst);
        manager.register().await.unwrap();
        let removed = manager.try_remove_version(7, 5, 15).unwrap();
        assert!(!removed.can_be_removed);
        assert_eq!(removed.enumeration_set.unwrap(), BTreeSet::from([10]));
        let notification = removed
            .enumeration_completion_notifications
            .unwrap()
            .pop()
            .unwrap();
        let repeat = manager.try_remove_version(7, 5, 15).unwrap();
        assert!(!repeat.can_be_removed);
        assert!(repeat.enumeration_completion_notifications.is_none());
        let other_provider = manager.try_remove_version(8, 5, 15).unwrap();
        let other = other_provider
            .enumeration_completion_notifications
            .unwrap()
            .pop()
            .unwrap();
        manager.unregister(10).unwrap();
        assert!(!notification.get_is_notification_completed());
        let mut wait = notification.get_notification();
        assert!(is_pending(&mut wait).await);
        manager.unregister(10).unwrap();
        assert_eq!(wait.await.unwrap(), 10);
        assert_eq!(other.get_notification().await.unwrap(), 10);
        assert!(notification.get_is_notification_completed());
        assert!(manager.try_remove_version(7, 5, 15).unwrap().can_be_removed);
        assert!(manager.unregister(10).is_err());
        // The completed notification key is removed, allowing the same VSN anew.
        manager.unregister(20).unwrap();
        sample.store(10, Ordering::SeqCst);
        manager.register().await.unwrap();
        let new = manager
            .try_remove_version(7, 5, 15)
            .unwrap()
            .enumeration_completion_notifications
            .unwrap()
            .pop()
            .unwrap();
        assert!(!new.get_is_notification_completed());
    }

    #[tokio::test]
    async fn inclusive_retention_and_checkpoint_wait_capture_all_release_sources() {
        let (manager, sample) = manager(10);
        manager.register().await.unwrap();
        sample.store(20, Ordering::SeqCst);
        manager.register().await.unwrap();
        let removal = manager.try_remove_version(7, 5, 20).unwrap();
        assert_eq!(removal.enumeration_set.unwrap(), BTreeSet::from([10, 20]));
        let mut checkpoint = manager.try_remove_checkpoint(5, 20);
        assert!(is_pending(&mut checkpoint).await);
        manager.unregister(10).unwrap();
        assert!(is_pending(&mut checkpoint).await);
        // 20 alone is removable by the half-open predicate, but the captured
        // checkpoint task also waits 20 because 10 previously retained it.
        assert!(manager.try_remove_version(7, 5, 20).unwrap().can_be_removed);
        manager.unregister(20).unwrap();
        checkpoint.await.unwrap();
        assert!(manager.try_remove_checkpoint(5, 6).await.is_err());
        assert!(manager
            .try_remove_checkpoint(i64::MAX, i64::MAX)
            .await
            .is_err());
        assert!(manager.try_remove_version(7, 5, 5).is_err());
    }

    #[tokio::test]
    async fn successful_and_failed_apply_fences_are_captured_before_sampling() {
        for failed in [false, true] {
            let (manager, sample) = manager(1);
            let fence = Completion::new();
            manager.update_dispatching_barrier(fence.clone());
            let mut registration = manager.register();
            tokio::task::yield_now().await;
            assert!(manager.registered_versions().is_empty());
            assert!(is_pending(&mut registration).await);
            // Replacing the dispatch task must not replace this call's captured fence.
            let later = Completion::new();
            manager.update_dispatching_barrier(later.clone());
            sample.store(12, Ordering::SeqCst);
            fence.complete(if failed {
                Err(Status::ObjectClosed.error())
            } else {
                Ok(())
            });
            assert_eq!(registration.await.unwrap(), 12);
            assert!(!later.is_completed());
            assert_eq!(manager.registered_versions(), vec![12]);
        }
    }

    #[tokio::test]
    async fn registration_owner_survives_observer_drop_and_sampling_failure_is_not_registered() {
        let (manager, _) = manager(12);
        let fence = Completion::new();
        manager.update_dispatching_barrier(fence.clone());
        drop(manager.register());
        fence.complete(Ok(()));
        tokio::task::yield_now().await;
        assert_eq!(manager.registered_versions(), vec![12]);
        let failing = VersionManager::new(Arc::new(|| Err(Status::ObjectClosed.error())));
        assert_eq!(
            failing.register().await.unwrap_err().native_code(),
            Some(Status::ObjectClosed.code())
        );
        assert!(failing.registered_versions().is_empty());
    }

    #[tokio::test]
    async fn completion_retains_first_result_with_no_observer_and_wakes_all() {
        let completion = Completion::new();
        let clone = completion.clone();
        assert!(completion.complete(Err::<i64, _>(Status::NoWriteQuorum.error())));
        assert!(!clone.complete(Ok(1)));
        let first = completion.wait().await;
        let second = clone.wait().await;
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            first.as_ref().as_ref().unwrap_err().native_code(),
            Some(Status::NoWriteQuorum.code())
        );
        let pending = Completion::new();
        let a = pending.clone();
        let b = pending.clone();
        let first = tokio::spawn(async move { a.wait().await });
        let second = tokio::spawn(async move { b.wait().await });
        tokio::task::yield_now().await;
        pending.complete(Ok(42));
        let (first, second) = tokio::join!(first, second);
        assert!(Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
    }
}
