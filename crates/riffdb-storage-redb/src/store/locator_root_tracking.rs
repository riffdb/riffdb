//! Fixed tracking headroom retained until every weak root allocation retires.
use crate::checkpoint_root::{ROOT_ALLOCATION_CONTROL_BYTES, TrackedRootWeak};
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

use super::{CheckpointRoot, StorageError, StorageErrorKind, extent_reclamation, storage_error};
use crate::locator_memory::{
    ChargedLocatorAllocation, LocatorAllocationClass, LocatorRetireContents, NativeLocatorPool,
};

pub(crate) struct RootTracking {
    roots: Mutex<TrackedRoots>,
}

impl RootTracking {
    pub(super) fn try_new() -> Result<ChargedLocatorAllocation<Self>, StorageError> {
        // Live descriptors are charged by their source capabilities. This
        // distinct conservative reservation covers their maximum residual Arc
        // allocations and charge-only controls after final strong release,
        // plus the actual larger weak-vector slots. No second residual owner
        // or population-sized control graph is introduced.
        let per_root = std::mem::size_of::<CheckpointRoot>()
            .checked_add(4 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::size_of::<TrackedRootWeak>()))
            .and_then(|n| n.checked_add(ROOT_ALLOCATION_CONTROL_BYTES))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let bytes = extent_reclamation::MAX_EXTENT_READ_ROOTS
            .checked_mul(per_root)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        ChargedLocatorAllocation::try_new(
            &NativeLocatorPool::global(),
            LocatorAllocationClass::Ordinary,
            bytes,
            || {
                Ok(Self {
                    roots: Mutex::new(TrackedRoots {
                        entries: Vec::with_capacity(extent_reclamation::MAX_EXTENT_READ_ROOTS),
                        cursor: 0,
                    }),
                })
            },
        )
    }
}

impl Deref for RootTracking {
    type Target = Mutex<TrackedRoots>;
    fn deref(&self) -> &Self::Target {
        &self.roots
    }
}

impl LocatorRetireContents for RootTracking {
    fn retire_one(&mut self) -> bool {
        let roots = self
            .roots
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roots.entries.pop();
        roots.entries.is_empty()
    }
}

/// One bounded cleanup cursor. Its inherent retain shadows Vec::retain, so
/// every existing tracking cleanup visits at most one admitted reaper quota.
#[derive(Default)]
pub(crate) struct TrackedRoots {
    entries: Vec<TrackedRootWeak>,
    cursor: usize,
}

impl TrackedRoots {
    #[cfg(test)]
    pub(crate) fn clear_for_tests(&mut self) {
        for _ in 0..extent_reclamation::MAX_EXTENT_READ_ROOTS
            / crate::locator_memory::LOCATOR_RETIRE_QUOTA
        {
            if self.entries.is_empty() {
                break;
            }
            self.retain(|_| false);
        }
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&TrackedRootWeak) -> bool) {
        let visits = self
            .entries
            .len()
            .min(crate::locator_memory::LOCATOR_RETIRE_QUOTA);
        for _ in 0..visits {
            if self.entries.is_empty() {
                break;
            }
            self.cursor %= self.entries.len();
            if keep(&self.entries[self.cursor]) {
                self.cursor += 1;
            } else {
                self.entries.swap_remove(self.cursor);
            }
        }
        std::thread::yield_now();
    }
}

impl Deref for TrackedRoots {
    type Target = Vec<TrackedRootWeak>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl DerefMut for TrackedRoots {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}
