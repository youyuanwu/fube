//! Byte-accounting foundation only, not operation/traffic-class admission.
//! Handles own inert ledgers, never connection/task lifetime. Allocation owners
//! retain leases until their payloads are freed, including aliases made before
//! admission. Charges cover payload structs, retained Vec capacities and lease
//! slots; allocator and standard Arc control-block overhead are not measured.

#![allow(
    dead_code,
    reason = "Private resource foundation awaits transport scopes"
)]

use std::{
    fmt,
    mem::size_of,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    InvalidInput(&'static str),
    ResourceLimit(&'static str),
    Internal(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "invalid storage input: {message}"),
            Self::ResourceLimit(resource) => write!(f, "native storage resource limit: {resource}"),
            Self::Internal(message) => write!(f, "native storage invariant: {message}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
struct Ledger {
    limit: usize,
    used: AtomicUsize,
}

#[derive(Debug, Clone)]
pub(crate) struct ByteBudget(Arc<Ledger>);

impl ByteBudget {
    pub(crate) fn new(limit: usize) -> Result<Self> {
        if limit == usize::MAX {
            return Err(Error::InvalidInput("byte budget must be finite"));
        }
        Ok(Self(Arc::new(Ledger {
            limit,
            used: AtomicUsize::new(0),
        })))
    }

    pub(crate) fn used(&self) -> usize {
        self.0.used.load(Ordering::Acquire)
    }

    fn same_ledger(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn try_reserve(&self, bytes: usize) -> Result<ByteLease> {
        self.0
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.0.limit)
            })
            .map_err(|_| Error::ResourceLimit("retained byte budget"))?;
        Ok(ByteLease {
            budget: self.clone(),
            bytes,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ByteLease {
    budget: ByteBudget,
    bytes: usize,
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        let previous = self.budget.0.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ByteScope {
    local: ByteBudget,
    shared: Option<ByteBudget>,
}

impl ByteScope {
    pub(crate) fn new(local: ByteBudget, shared: Option<ByteBudget>) -> Self {
        Self { local, shared }
    }

    fn budgets(&self) -> impl Iterator<Item = &ByteBudget> {
        std::iter::once(&self.local).chain(
            self.shared
                .iter()
                .filter(|shared| !self.local.same_ledger(shared)),
        )
    }

    pub(crate) fn try_reserve(&self, bytes: usize) -> Result<ScopeLease> {
        let mut leases = [None, None];
        for (slot, budget) in leases.iter_mut().zip(self.budgets()) {
            *slot = Some(budget.try_reserve(bytes)?);
        }
        Ok(ScopeLease { _leases: leases })
    }
}

#[derive(Debug)]
pub(crate) struct ScopeLease {
    _leases: [Option<ByteLease>; 2],
}

#[derive(Debug)]
struct LeaseSet {
    leases: Vec<ByteLease>,
    max_ledgers: usize,
}

/// Immutable payload plus a bounded, preallocated ledger-attachment table.
/// `backing_bytes` passed to `new` must include all payload-owned heap capacity,
/// but not allocations independently owned by other SharedAllocations.
#[derive(Debug)]
pub(crate) struct SharedAllocation<T> {
    data: T,
    leases: Mutex<LeaseSet>,
    retained_bytes: usize,
}

impl<T> SharedAllocation<T> {
    pub(crate) fn minimum_bytes(backing_bytes: usize, max_ledgers: usize) -> Result<usize> {
        max_ledgers
            .checked_mul(size_of::<ByteLease>())
            .and_then(|bytes| bytes.checked_add(backing_bytes))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or(Error::ResourceLimit("allocation accounting overflow"))
    }

    pub(crate) fn new(
        data: T,
        backing_bytes: usize,
        max_ledgers: usize,
        max_retained_bytes: usize,
    ) -> Result<Arc<Self>> {
        if Self::minimum_bytes(backing_bytes, max_ledgers)? > max_retained_bytes {
            return Err(Error::ResourceLimit("retained construction bytes"));
        }
        let mut leases = Vec::new();
        leases
            .try_reserve_exact(max_ledgers)
            .map_err(|_| Error::ResourceLimit("lease table allocation"))?;
        let retained_bytes = Self::minimum_bytes(backing_bytes, leases.capacity())?;
        if retained_bytes > max_retained_bytes {
            return Err(Error::ResourceLimit("retained construction bytes"));
        }
        Ok(Arc::new(Self {
            data,
            leases: Mutex::new(LeaseSet {
                leases,
                max_ledgers,
            }),
            retained_bytes,
        }))
    }

    pub(crate) fn data(&self) -> &T {
        &self.data
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn is_admitted(&self, scope: &ByteScope) -> Result<bool> {
        let state = self
            .leases
            .lock()
            .map_err(|_| Error::Internal("poisoned allocation lease table"))?;
        Ok(scope.budgets().all(|budget| {
            state
                .leases
                .iter()
                .any(|lease| lease.budget.same_ledger(budget))
        }))
    }

    pub(crate) fn begin_admission(&self) -> Result<AllocationAdmission<'_>> {
        let state = self
            .leases
            .lock()
            .map_err(|_| Error::Internal("poisoned allocation lease table"))?;
        let initial_len = state.leases.len();
        Ok(AllocationAdmission {
            state,
            initial_len,
            retained_bytes: self.retained_bytes,
            committed: false,
        })
    }
}

pub(crate) struct AllocationAdmission<'a> {
    state: MutexGuard<'a, LeaseSet>,
    initial_len: usize,
    retained_bytes: usize,
    committed: bool,
}

impl AllocationAdmission<'_> {
    pub(crate) fn reserve(&mut self, scope: &ByteScope) -> Result<()> {
        for budget in scope.budgets() {
            if self
                .state
                .leases
                .iter()
                .any(|lease| lease.budget.same_ledger(budget))
            {
                continue;
            }
            if self.state.leases.len() == self.state.max_ledgers {
                return Err(Error::ResourceLimit("allocation ledger slots"));
            }
            let lease = budget.try_reserve(self.retained_bytes)?;
            self.state.leases.push(lease);
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for AllocationAdmission<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.state.leases.truncate(self.initial_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_leases_enforce_exact_limits_and_release_on_drop() {
        let budget = ByteBudget::new(10).unwrap();
        let alias = budget.clone();
        let first = budget.try_reserve(7).unwrap();
        let second = alias.try_reserve(3).unwrap();
        assert_eq!(budget.used(), 10);
        assert!(matches!(
            budget.try_reserve(1),
            Err(Error::ResourceLimit(_))
        ));
        assert!(matches!(
            budget.try_reserve(usize::MAX),
            Err(Error::ResourceLimit(_))
        ));
        drop(first);
        assert_eq!(alias.used(), 3);
        drop(second);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn scope_reservations_rollback_and_identical_ledgers_are_not_charged_twice() {
        let local = ByteBudget::new(10).unwrap();
        let shared = ByteBudget::new(3).unwrap();
        let scope = ByteScope::new(local.clone(), Some(shared.clone()));
        assert!(scope.try_reserve(4).is_err());
        assert_eq!(local.used(), 0);
        assert_eq!(shared.used(), 0);
        let lease = scope.try_reserve(3).unwrap();
        assert_eq!(local.used(), 3);
        assert_eq!(shared.used(), 3);
        drop(lease);
        assert_eq!(local.used(), 0);
        assert_eq!(shared.used(), 0);

        let scope = ByteScope::new(local.clone(), Some(local.clone()));
        let lease = scope.try_reserve(10).unwrap();
        assert_eq!(local.used(), 10);
        drop(lease);
        assert_eq!(local.used(), 0);
    }
}
