// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native signed clock identities and replay high-water advancement.
//!
//! SF 51a7703 `common/TransactionBase.cpp:11–13,78–128` uses a process-static
//! lock and a system-time sample, not an LSN or an incarnation counter.
//! `ktl/src/src/knt.cpp:527–550` specifies 100-ns UTC ticks since 1601.
//! Zero, negative clock samples and arithmetic overflow are checked A0 input
//! exclusions; native does not provide recoverable exhaustion branches.

use crate::{Error, Result, TransactionId};
use std::{
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

/// Process-wide generator shared by ordinary factories and decoded identities.
static GENERATOR: OnceLock<IdGenerator> = OnceLock::new();
/// Difference between native 1601 UTC and the Unix epoch, in seconds.
const NATIVE_EPOCH_SECONDS: u64 = 11_644_473_600;

/// Native sign-derived replay classification, not enabled atomic execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityKind {
    /// Positive ordinary transaction.
    Ordinary,
    /// Negative atomic transaction.
    Atomic,
}

/// The native algorithm with an instance-local high-water seam for tests.
pub(crate) struct IdGenerator {
    /// Last positive identity, protected across generation and replay.
    high_water: Mutex<TransactionId>,
}

impl IdGenerator {
    /// Initialize from a native tick sample; zero is allowed only as a test seed.
    pub(crate) fn new(initial_ticks: i64) -> Result<Self> {
        if initial_ticks < 0 {
            return Err(Error::InvalidInput("negative native identity clock"));
        }
        Ok(Self {
            high_water: Mutex::new(initial_ticks),
        })
    }

    /// Use the sample if newer, otherwise increment the high-water mark.
    pub(crate) fn next(&self, native_ticks: i64) -> Result<TransactionId> {
        if native_ticks < 0 {
            return Err(Error::InvalidInput("negative native identity clock"));
        }
        let mut previous = self.high_water.lock().expect("identity lock poisoned");
        let next = if native_ticks > *previous {
            native_ticks
        } else {
            previous.checked_add(1).ok_or(Error::ResourceLimit(
                "native transaction identity exhausted",
            ))?
        };
        *previous = next;
        Ok(next)
    }

    /// Recognize the sign and raise, never lower, the absolute high-water mark.
    pub(crate) fn replay(&self, id: TransactionId) -> Result<IdentityKind> {
        let (kind, positive) = recognize(id)?;
        let mut previous = self.high_water.lock().expect("identity lock poisoned");
        *previous = (*previous).max(positive);
        Ok(kind)
    }
}

/// Generate a positive ordinary ID using the actual native-unit UTC clock.
pub(crate) fn generate() -> Result<TransactionId> {
    let ticks = native_system_time()?;
    process_generator(ticks).next(ticks)
}

/// Advance the process generator when decoding an ordinary or atomic identity.
pub(crate) fn replay(id: TransactionId) -> Result<IdentityKind> {
    recognize(id)?;
    process_generator(native_system_time()?).replay(id)
}

/// Convert the real UTC clock to the units consumed by native KNt.
pub(crate) fn native_system_time() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidInput("A0 clock predates Unix epoch"))?;
    let seconds = elapsed
        .as_secs()
        .checked_add(NATIVE_EPOCH_SECONDS)
        .ok_or(Error::ResourceLimit("native clock exhausted"))?;
    let ticks = seconds
        .checked_mul(10_000_000)
        .and_then(|value| value.checked_add(u64::from(elapsed.subsec_nanos() / 100)))
        .ok_or(Error::ResourceLimit("native clock exhausted"))?;
    i64::try_from(ticks).map_err(|_| Error::ResourceLimit("native clock exhausted"))
}

/// Initialize exactly once; both production entry points share this instance.
fn process_generator(ticks: i64) -> &'static IdGenerator {
    GENERATOR.get_or_init(|| IdGenerator {
        high_water: Mutex::new(ticks),
    })
}

/// Check the arithmetic exclusions before mutating either generator.
fn recognize(id: TransactionId) -> Result<(IdentityKind, i64)> {
    if id == 0 {
        return Err(Error::InvalidInput("zero native transaction identity"));
    }
    let positive = id.checked_abs().ok_or(Error::InvalidInput(
        "unrepresentable native atomic identity",
    ))?;
    Ok((
        if id < 0 {
            IdentityKind::Atomic
        } else {
            IdentityKind::Ordinary
        },
        positive,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn w04_native_clock_high_water_and_replay() {
        let generator = IdGenerator::new(100).unwrap();
        assert_eq!(generator.next(99).unwrap(), 101);
        assert_eq!(generator.next(100).unwrap(), 102);
        assert_eq!(generator.next(105).unwrap(), 105);
        assert_eq!(generator.replay(-900).unwrap(), IdentityKind::Atomic);
        assert_eq!(generator.next(800).unwrap(), 901);
        assert_eq!(generator.replay(200).unwrap(), IdentityKind::Ordinary);
        assert_eq!(generator.next(100).unwrap(), 902);
    }

    #[test]
    fn exhausted_and_invalid_replay_never_wrap_or_lower_generator() {
        let generator = IdGenerator::new(100).unwrap();
        assert!(generator.replay(0).is_err());
        assert!(generator.replay(i64::MIN).is_err());
        assert!(generator.next(-1).is_err());
        assert_eq!(generator.next(1).unwrap(), 101);
        generator.replay(i64::MAX).unwrap();
        assert!(generator.next(1).is_err());
        assert!(generator.next(i64::MAX).is_err());
    }
}
