// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native per-transaction gates and pin-specific retry hooks.
//!
//! SF 51a7703 `common/TransactionStateMachine.cpp:36–293` and `.h:67–120`
//! distinguish state errors and execute admission/unregister under the state
//! lock. Completion hooks assert their native predecessor state.
//! `TransactionBase.h:114–125` intentionally does not assign retry values at
//! or below 4096 ms; ordinary retries therefore remain at 32 ms.

use super::status::{retryable, Status};
use crate::{Error, Result, TransactionState};
use std::{
    sync::{atomic::AtomicU64, atomic::Ordering, Mutex},
    time::Duration,
};

/// Native AbortReason.h discriminants, retained separately from lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum AbortReason {
    /// No abort requested.
    Invalid = 0,
    /// Explicit user abort.
    UserAborted = 1,
    /// Active transaction disposed by its user.
    UserDisposed = 2,
    /// Best-effort system abort.
    SystemAborted = 4,
}

/// Both native state observations protected by one transaction-local lock.
struct State {
    /// Native lifecycle state.
    value: TransactionState,
    /// Native abort error discriminator.
    abort_reason: AbortReason,
}

/// Real native state machine, not a transaction scheduler or disposal owner.
pub(crate) struct StateMachine {
    /// Admission callbacks execute while this lock remains held.
    state: Mutex<State>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Decision after an attempted commit fails, not a whole-call deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitFailure {
    /// Retry-classified failure, without cancel/strict timeout.
    Retry,
    /// Cancellation observed only after a transient failure.
    Cancelled,
    /// Elapsed time was strictly greater than the timeout.
    TimedOut,
    /// Terminal attempted-operation failure.
    Faulted,
}

impl StateMachine {
    /// Native transactions, including replay instances, start Active.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                value: TransactionState::Active,
                abort_reason: AbortReason::Invalid,
            }),
        }
    }

    /// Observe state without reserving a state-changing operation.
    pub(crate) fn get_state(&self) -> TransactionState {
        self.state.lock().expect("state lock poisoned").value
    }

    /// Observe native abort reason separately from state.
    pub(crate) fn abort_reason(&self) -> AbortReason {
        self.state.lock().expect("state lock poisoned").abort_reason
    }

    /// Validate Active and execute synchronous admission under the state lock.
    ///
    /// The callback must not reenter this state machine.
    pub(crate) fn add_operation<T>(&self, action: impl FnOnce() -> Result<T>) -> Result<T> {
        let state = self.state.lock().expect("state lock poisoned");
        validate_active(&state)?;
        let result = action();
        drop(state);
        result
    }

    /// Active to Reading; failures leave state unchanged.
    pub(crate) fn begin_read(&self) -> Result<()> {
        self.begin(TransactionState::Reading)
    }

    /// Restore Reading to Active even after private registration failure.
    pub(crate) fn end_read(&self) {
        self.finish(TransactionState::Reading, TransactionState::Active);
    }

    /// Active to Committing; manager acquisition belongs to the caller.
    pub(crate) fn begin_commit(&self) -> Result<()> {
        self.begin(TransactionState::Committing)
    }

    /// Restore Active only at the native cancellation/timeout retry boundary.
    pub(crate) fn commit_cancelled_or_timed_out(&self) {
        self.finish(TransactionState::Committing, TransactionState::Active);
    }

    /// Mark a successfully completed ordinary transaction Committed.
    pub(crate) fn commit_successful(&self) {
        self.finish(TransactionState::Committing, TransactionState::Committed);
    }

    /// Mark a terminal attempted ordinary commit Faulted.
    pub(crate) fn commit_faulted(&self) {
        self.finish(TransactionState::Committing, TransactionState::Faulted);
    }

    /// Preserve exact classification, cancellation precedence and strict timeout.
    pub(crate) fn commit_failure(
        &self,
        error: &Error,
        cancelled: bool,
        elapsed: Duration,
        timeout: Option<Duration>,
    ) -> CommitFailure {
        if !retryable(error) {
            self.commit_faulted();
            CommitFailure::Faulted
        } else if cancelled {
            self.commit_cancelled_or_timed_out();
            CommitFailure::Cancelled
        } else if timeout.is_some_and(|timeout| elapsed > timeout) {
            self.commit_cancelled_or_timed_out();
            CommitFailure::TimedOut
        } else {
            assert_eq!(self.get_state(), TransactionState::Committing);
            CommitFailure::Retry
        }
    }

    /// Active to Aborting with the user-error discriminator.
    pub(crate) fn user_begin_abort(&self) -> Result<()> {
        self.begin_abort(AbortReason::UserAborted)
    }

    /// Active to Aborting with the system-aborted discriminator.
    pub(crate) fn system_begin_abort(&self) -> Result<()> {
        self.begin_abort(AbortReason::SystemAborted)
    }

    /// Unregister first under lock, discard its status, then validate/start abort.
    ///
    /// Reading/Committing disposal is a native assertion boundary. Idempotence,
    /// nontracking locks and scheduling the owned abort belong to the transaction.
    pub(crate) fn dispose_begin_abort(
        &self,
        unregister: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock poisoned");
        assert!(
            !matches!(
                state.value,
                TransactionState::Reading | TransactionState::Committing
            ),
            "native dispose cannot race read or commit"
        );
        let _ = unregister();
        validate_active(&state)?;
        state.value = TransactionState::Aborting;
        state.abort_reason = AbortReason::UserDisposed;
        Ok(())
    }

    /// Mark owned abort work successful; synchronous Abort itself is not this.
    pub(crate) fn abort_successful(&self) {
        self.finish(TransactionState::Aborting, TransactionState::Aborted);
    }

    /// Mark a terminal abort-end attempt Faulted.
    pub(crate) fn abort_faulted(&self) {
        self.finish(TransactionState::Aborting, TransactionState::Faulted);
    }

    /// Recognition-only atomic hook, not an enabled atomic execution API.
    pub(crate) fn begin_atomic_operation(&self) -> Result<()> {
        self.begin(TransactionState::Committing)
    }

    /// Native atomic success uses the same predecessor as ordinary commit.
    pub(crate) fn atomic_successful(&self) {
        self.finish(TransactionState::Committing, TransactionState::Committed);
    }

    /// Native atomic failure uses the same predecessor as ordinary commit.
    pub(crate) fn atomic_faulted(&self) {
        self.finish(TransactionState::Committing, TransactionState::Faulted);
    }

    /// Preserve the nonassigning native retry expression: remains Committing.
    pub(crate) fn atomic_retry(&self) {
        let state = self.state.lock().expect("state lock poisoned");
        assert_eq!(state.value, TransactionState::Committing);
    }

    /// Begin an Active-only nonabort operation.
    fn begin(&self, next: TransactionState) -> Result<()> {
        let mut state = self.state.lock().expect("state lock poisoned");
        validate_active(&state)?;
        state.value = next;
        Ok(())
    }

    /// Begin an Active-only abort with its precise reason.
    fn begin_abort(&self, reason: AbortReason) -> Result<()> {
        let mut state = self.state.lock().expect("state lock poisoned");
        validate_active(&state)?;
        state.value = TransactionState::Aborting;
        state.abort_reason = reason;
        Ok(())
    }

    /// Completion hooks preserve native assertions, not invented state errors.
    fn finish(&self, expected: TransactionState, next: TransactionState) {
        let mut state = self.state.lock().expect("state lock poisoned");
        assert_eq!(state.value, expected, "native completion predecessor");
        state.value = next;
    }
}

/// Native detailed Active-only validation.
fn validate_active(state: &State) -> Result<()> {
    let status = match state.value {
        TransactionState::Active => return Ok(()),
        TransactionState::Reading => Status::MultithreadedTransactions,
        TransactionState::Committing | TransactionState::Committed => Status::TransactionNotActive,
        TransactionState::Aborting | TransactionState::Aborted | TransactionState::Faulted => {
            if state.abort_reason == AbortReason::SystemAborted {
                Status::TransactionAborted
            } else {
                Status::TransactionNotActive
            }
        }
    };
    Err(status.error())
}

/// Transaction-local native retry-delay property, including the setter quirk.
pub(crate) struct RetryDelay {
    /// Milliseconds; atomic only for shared observation, not scheduling authority.
    milliseconds: AtomicU64,
}

impl Default for RetryDelay {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryDelay {
    /// Native initial delay is 32 milliseconds.
    pub(crate) fn new() -> Self {
        Self {
            milliseconds: AtomicU64::new(32),
        }
    }

    /// Observe the timer delay used by ordinary commit/abort retry loops.
    pub(crate) fn get(&self) -> Duration {
        Duration::from_millis(self.milliseconds.load(Ordering::Relaxed))
    }

    /// Assign only when the supplied value exceeds native maximum 4096 ms.
    pub(crate) fn set(&self, value: Duration) {
        if value.as_millis() > 4096 {
            self.milliseconds.store(4096, Ordering::Relaxed);
        }
    }

    /// Native caller's `RetryDelay *= 2`, which is not effective exponential backoff.
    pub(crate) fn double(&self) {
        self.set(self.get() * 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn s01_read_commit_and_abort_have_distinct_statuses() {
        let state = StateMachine::new();
        state.begin_read().unwrap();
        for error in [
            state.add_operation(|| Ok(())).unwrap_err(),
            state.begin_commit().unwrap_err(),
            state.user_begin_abort().unwrap_err(),
        ] {
            assert_eq!(
                error.native_code(),
                Some(Status::MultithreadedTransactions.code())
            );
        }
        state.end_read();
        state.begin_commit().unwrap();
        assert_eq!(
            state.add_operation(|| Ok(())).unwrap_err().native_code(),
            Some(Status::TransactionNotActive.code())
        );
        state.commit_successful();
        assert!(state.begin_read().is_err());
        for system in [false, true] {
            let state = StateMachine::new();
            if system {
                state.system_begin_abort().unwrap();
            } else {
                state.user_begin_abort().unwrap();
            }
            let expected = if system {
                Status::TransactionAborted
            } else {
                Status::TransactionNotActive
            };
            assert_eq!(
                state.begin_read().unwrap_err().native_code(),
                Some(expected.code())
            );
            state.abort_successful();
            assert_eq!(
                state.begin_commit().unwrap_err().native_code(),
                Some(expected.code())
            );
        }
    }

    #[test]
    fn admission_and_unregister_run_under_lock_and_dispose_discards_status() {
        let state = StateMachine::new();
        state
            .add_operation(|| {
                assert!(state.state.try_lock().is_err());
                Err::<(), _>(Status::QueueFull.error())
            })
            .unwrap_err();
        assert_eq!(state.get_state(), TransactionState::Active);
        state
            .dispose_begin_abort(|| {
                assert!(state.state.try_lock().is_err());
                Err(Status::ObjectClosed.error())
            })
            .unwrap();
        assert_eq!(state.abort_reason(), AbortReason::UserDisposed);
        state.abort_successful();
        let called = AtomicBool::new(false);
        let error = state
            .dispose_begin_abort(|| {
                called.store(true, Ordering::Relaxed);
                Err(Status::ObjectClosed.error())
            })
            .unwrap_err();
        assert!(called.load(Ordering::Relaxed));
        assert_eq!(
            error.native_code(),
            Some(Status::TransactionNotActive.code())
        );
    }

    #[test]
    fn s05_atomic_retry_does_not_restore_active() {
        let state = StateMachine::new();
        state.begin_atomic_operation().unwrap();
        state.atomic_retry();
        assert_eq!(state.get_state(), TransactionState::Committing);
        assert_eq!(
            state.begin_atomic_operation().unwrap_err().native_code(),
            Some(Status::TransactionNotActive.code())
        );
    }

    #[test]
    fn abort_reason_validation_survives_both_success_and_fault() {
        for reason in [
            AbortReason::UserAborted,
            AbortReason::UserDisposed,
            AbortReason::SystemAborted,
        ] {
            for fault in [false, true] {
                let state = StateMachine::new();
                match reason {
                    AbortReason::UserAborted => state.user_begin_abort().unwrap(),
                    AbortReason::UserDisposed => state.dispose_begin_abort(|| Ok(())).unwrap(),
                    AbortReason::SystemAborted => state.system_begin_abort().unwrap(),
                    AbortReason::Invalid => unreachable!(),
                }
                let expected = if reason == AbortReason::SystemAborted {
                    Status::TransactionAborted
                } else {
                    Status::TransactionNotActive
                };
                assert_eq!(
                    state.begin_commit().unwrap_err().native_code(),
                    Some(expected.code())
                );
                if fault {
                    state.abort_faulted();
                } else {
                    state.abort_successful();
                }
                assert_eq!(
                    state.add_operation(|| Ok(())).unwrap_err().native_code(),
                    Some(expected.code())
                );
            }
        }
        let state = StateMachine::new();
        state.begin_commit().unwrap();
        state.commit_faulted();
        assert_eq!(
            state.begin_read().unwrap_err().native_code(),
            Some(Status::TransactionNotActive.code())
        );
    }

    #[test]
    fn retry_boundary_is_strict_and_native_delay_setter_is_nonassigning() {
        let delay = RetryDelay::new();
        for _ in 0..8 {
            delay.double();
            assert_eq!(delay.get(), Duration::from_millis(32));
        }
        delay.set(Duration::from_millis(4096));
        assert_eq!(delay.get(), Duration::from_millis(32));
        delay.set(Duration::from_millis(4097));
        assert_eq!(delay.get(), Duration::from_millis(4096));
        let state = StateMachine::new();
        state.begin_commit().unwrap();
        let timeout = Duration::from_millis(10);
        assert_eq!(
            state.commit_failure(&Status::QueueFull.error(), false, timeout, Some(timeout)),
            CommitFailure::Retry
        );
        assert_eq!(
            state.commit_failure(&Status::QueueFull.error(), true, timeout * 2, Some(timeout)),
            CommitFailure::Cancelled
        );
        assert_eq!(state.get_state(), TransactionState::Active);
        state.begin_commit().unwrap();
        assert_eq!(
            state.commit_failure(
                &Status::QueueFull.error(),
                false,
                timeout * 2,
                Some(timeout)
            ),
            CommitFailure::TimedOut
        );
        state.begin_commit().unwrap();
        assert_eq!(
            state.commit_failure(
                &Status::NotPrimary.error(),
                true,
                timeout * 2,
                Some(timeout)
            ),
            CommitFailure::Faulted
        );
        assert_eq!(state.get_state(), TransactionState::Faulted);
    }
}
