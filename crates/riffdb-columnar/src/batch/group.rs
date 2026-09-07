//! Private, inert canonical group-state mechanics.

use std::cmp::Ordering;
use std::mem::size_of;

use riffdb_types::{
    MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_DISTINCT_VALUES_V1,
    MAX_AGGREGATE_STATE_BYTES_V1, canonical_value_encoded_len, encode_canonical_value_into,
};

use super::aggregate::CanonicalPartialIdentity;
use super::program::CheckedProgramLaneFacts;
use crate::segment_v2::{MAX_SEGMENT_V2_COLUMNS, SegmentV2Cell};

const NO_VALUE_BYTES: [u8; 2] = [1, 0];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupStateError {
    InvalidGroupBound,
    InvalidKeyBound,
    InvalidStateBound,
    InvalidWorkBound,
    AllocationFailed,
    InputShape,
    TypeMismatch,
    KeyBoundExceeded,
    GroupBoundExceeded,
    StateBoundExceeded,
    WorkBoundExceeded,
    LaneBoundExceeded,
    ProgramBindingSubstitution,
    RowBoundExceeded,
    RowOrder,
    UnexpectedNoValue,
    ArithmeticOverflow,
    Poisoned,
    InvalidInventory,
    InventoryBoundExceeded,
    DuplicateOrReorderedLeaf,
    InventoryGap,
    ForeignRootInventory,
    ForeignSegment,
    ExcessLeaf,
    InventoryOmission,
    IncompatibleLeaf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GroupWorkBudget {
    remaining: u32,
}

impl GroupWorkBudget {
    pub(super) const fn new(maximum_work: u32) -> Result<Self, GroupStateError> {
        if maximum_work == 0 || maximum_work > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 {
            return Err(GroupStateError::InvalidWorkBound);
        }
        Ok(Self {
            remaining: maximum_work,
        })
    }

    fn charge(&mut self, work: usize) -> Result<(), GroupStateError> {
        let work = u32::try_from(work).map_err(|_| GroupStateError::WorkBoundExceeded)?;
        self.remaining = self
            .remaining
            .checked_sub(work)
            .ok_or(GroupStateError::WorkBoundExceeded)?;
        Ok(())
    }

    fn preflight(&self, maximum_work: usize) -> Result<u32, GroupStateError> {
        let maximum_work =
            u32::try_from(maximum_work).map_err(|_| GroupStateError::WorkBoundExceeded)?;
        if maximum_work > self.remaining {
            return Err(GroupStateError::WorkBoundExceeded);
        }
        Ok(maximum_work)
    }

    fn commit_actual(
        &mut self,
        admitted_maximum: u32,
        actual_work: usize,
    ) -> Result<(), GroupStateError> {
        let actual_work =
            u32::try_from(actual_work).map_err(|_| GroupStateError::ArithmeticOverflow)?;
        if actual_work > admitted_maximum || actual_work > self.remaining {
            return Err(GroupStateError::ArithmeticOverflow);
        }
        self.remaining -= actual_work;
        Ok(())
    }

    #[cfg(test)]
    const fn remaining(self) -> u32 {
        self.remaining
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CanonicalGroupLane {
    facts: CheckedProgramLaneFacts,
}

impl CanonicalGroupLane {
    pub(super) const fn from_checked(facts: CheckedProgramLaneFacts) -> Self {
        Self { facts }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CanonicalGroupBounds {
    maximum_groups: u16,
    maximum_key_bytes: u32,
    maximum_state_bytes: u32,
}

impl CanonicalGroupBounds {
    pub(super) const fn new(
        maximum_groups: u16,
        maximum_key_bytes: u32,
        maximum_state_bytes: u32,
    ) -> Result<Self, GroupStateError> {
        if maximum_groups == 0 || maximum_groups > MAX_AGGREGATE_DISTINCT_VALUES_V1 {
            return Err(GroupStateError::InvalidGroupBound);
        }
        if maximum_key_bytes == 0 || maximum_key_bytes > MAX_AGGREGATE_STATE_BYTES_V1 {
            return Err(GroupStateError::InvalidKeyBound);
        }
        if maximum_state_bytes == 0 || maximum_state_bytes > MAX_AGGREGATE_STATE_BYTES_V1 {
            return Err(GroupStateError::InvalidStateBound);
        }
        Ok(Self {
            maximum_groups,
            maximum_key_bytes,
            maximum_state_bytes,
        })
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupEntry {
    key_start: u32,
    key_len: u32,
    row_count: u64,
}

/// The sole fallibly allocated owner for one leaf or merged group state.
///
/// Both buffers are reserved once at construction. Admission checks capacity
/// before writing, so no row, key, or merge operation can allocate.
struct BoundedGroupOwner {
    arena: Vec<u8>,
    entries: Vec<GroupEntry>,
    bounds: CanonicalGroupBounds,
    retained_key_capacity: usize,
    poisoned: bool,
}

impl BoundedGroupOwner {
    fn new(bounds: CanonicalGroupBounds) -> Result<Self, GroupStateError> {
        let maximum_groups = usize::from(bounds.maximum_groups);
        let requested_entry_bytes = maximum_groups
            .checked_mul(size_of::<GroupEntry>())
            .ok_or(GroupStateError::InvalidStateBound)?;
        let state_ceiling = usize::try_from(bounds.maximum_state_bytes)
            .map_err(|_| GroupStateError::InvalidStateBound)?;
        if requested_entry_bytes > state_ceiling {
            return Err(GroupStateError::InvalidStateBound);
        }

        let mut entries = Vec::new();
        entries
            .try_reserve_exact(maximum_groups)
            .map_err(|_| GroupStateError::AllocationFailed)?;
        let actual_entry_bytes = entries
            .capacity()
            .checked_mul(size_of::<GroupEntry>())
            .ok_or(GroupStateError::InvalidStateBound)?;
        let arena_capacity = state_ceiling
            .checked_sub(actual_entry_bytes)
            .ok_or(GroupStateError::InvalidStateBound)?;
        let maximum_key_bytes = usize::try_from(bounds.maximum_key_bytes)
            .map_err(|_| GroupStateError::InvalidKeyBound)?;
        let retained_key_capacity = arena_capacity
            .checked_sub(maximum_key_bytes)
            .ok_or(GroupStateError::InvalidStateBound)?;
        let mut arena = Vec::new();
        arena
            .try_reserve_exact(arena_capacity)
            .map_err(|_| GroupStateError::AllocationFailed)?;
        let actual_state_bytes = actual_entry_bytes
            .checked_add(arena.capacity())
            .ok_or(GroupStateError::InvalidStateBound)?;
        if actual_state_bytes > state_ceiling {
            return Err(GroupStateError::InvalidStateBound);
        }
        Ok(Self {
            arena,
            entries,
            bounds,
            retained_key_capacity,
            poisoned: false,
        })
    }

    fn poison(&mut self, error: GroupStateError) -> GroupStateError {
        self.entries.clear();
        self.arena.clear();
        self.poisoned = true;
        error
    }

    fn ensure_live(&self) -> Result<(), GroupStateError> {
        if self.poisoned {
            Err(GroupStateError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn admit_cells(
        &mut self,
        lanes: &[CanonicalGroupLane],
        cells: &[SegmentV2Cell],
        count: u64,
        work: &mut GroupWorkBudget,
    ) -> Result<(), GroupStateError> {
        self.ensure_live()?;
        let maximum_work = match row_work_ceiling(lanes.len(), self.entries.len()) {
            Ok(maximum_work) => maximum_work,
            Err(error) => return Err(self.poison(error)),
        };
        let admitted = match work.preflight(maximum_work) {
            Ok(admitted) => admitted,
            Err(error) => return Err(self.poison(error)),
        };
        let (key_len, validation_work) = match encoded_key_len(lanes, cells) {
            Ok(result) => result,
            Err(failure) => {
                if work
                    .commit_actual(admitted, failure.completed_work)
                    .is_err()
                {
                    return Err(self.poison(GroupStateError::ArithmeticOverflow));
                }
                return Err(self.poison(failure.error));
            }
        };
        if key_len > self.bounds.maximum_key_bytes as usize {
            if work.commit_actual(admitted, validation_work).is_err() {
                return Err(self.poison(GroupStateError::ArithmeticOverflow));
            }
            return Err(self.poison(GroupStateError::KeyBoundExceeded));
        }
        let start = self.arena.len();
        if start
            .checked_add(key_len)
            .is_none_or(|end| end > self.arena.capacity())
        {
            if work.commit_actual(admitted, validation_work).is_err() {
                return Err(self.poison(GroupStateError::ArithmeticOverflow));
            }
            return Err(self.poison(GroupStateError::StateBoundExceeded));
        }
        let encoding_work = match append_key(&mut self.arena, cells) {
            Ok(encoding_work) => encoding_work,
            Err(failure) => {
                self.arena.truncate(start);
                let actual_work = match validation_work.checked_add(failure.completed_work) {
                    Some(actual_work) => actual_work,
                    None => return Err(self.poison(GroupStateError::ArithmeticOverflow)),
                };
                if work.commit_actual(admitted, actual_work).is_err() {
                    return Err(self.poison(GroupStateError::ArithmeticOverflow));
                }
                return Err(self.poison(failure.error));
            }
        };
        let end = start + key_len;
        let (location, comparison_work) = self.locate_key(&self.arena[start..end]);
        let shift_work = insertion_shift_work(location, self.entries.len());
        let actual_work = match validation_work
            .checked_add(encoding_work)
            .and_then(|work| work.checked_add(comparison_work))
            .and_then(|work| work.checked_add(shift_work))
        {
            Some(actual_work) => actual_work,
            None => {
                self.arena.truncate(start);
                return Err(self.poison(GroupStateError::ArithmeticOverflow));
            }
        };
        if work.commit_actual(admitted, actual_work).is_err() {
            self.arena.truncate(start);
            return Err(self.poison(GroupStateError::ArithmeticOverflow));
        }
        self.commit_appended(start, key_len, count, location)
    }

    fn admit_encoded(&mut self, key: &[u8], count: u64) -> Result<usize, CountedFailure> {
        self.ensure_live().map_err(|error| CountedFailure {
            error,
            completed_work: 0,
        })?;
        if count == 0 {
            return Err(CountedFailure {
                error: self.poison(GroupStateError::IncompatibleLeaf),
                completed_work: 1,
            });
        }
        if key.len() > self.bounds.maximum_key_bytes as usize {
            return Err(CountedFailure {
                error: self.poison(GroupStateError::KeyBoundExceeded),
                completed_work: 1,
            });
        }
        let start = self.arena.len();
        if start
            .checked_add(key.len())
            .is_none_or(|end| end > self.arena.capacity())
        {
            return Err(CountedFailure {
                error: self.poison(GroupStateError::StateBoundExceeded),
                completed_work: 1,
            });
        }
        self.arena.extend_from_slice(key);
        let end = start + key.len();
        let (location, comparisons) = self.locate_key(&self.arena[start..end]);
        let shifts = insertion_shift_work(location, self.entries.len());
        let completed_work = match comparisons
            .checked_add(shifts)
            .and_then(|work| work.checked_add(1))
        {
            Some(completed_work) => completed_work,
            None => {
                self.arena.truncate(start);
                return Err(CountedFailure {
                    error: self.poison(GroupStateError::ArithmeticOverflow),
                    completed_work: comparisons,
                });
            }
        };
        self.commit_appended(start, key.len(), count, location)
            .map_err(|error| CountedFailure {
                error,
                completed_work,
            })?;
        Ok(completed_work)
    }

    fn locate_key(&self, key: &[u8]) -> (KeyLocation, usize) {
        let mut low = 0;
        let mut high = self.entries.len();
        let mut comparisons = 0;
        while low < high {
            let middle = low + (high - low) / 2;
            comparisons += 1;
            match entry_key(&self.arena, &self.entries[middle]).cmp(key) {
                Ordering::Less => low = middle + 1,
                Ordering::Equal => return (KeyLocation::Existing(middle), comparisons),
                Ordering::Greater => high = middle,
            }
        }
        (KeyLocation::Insert(low), comparisons)
    }

    fn commit_appended(
        &mut self,
        start: usize,
        key_len: usize,
        count: u64,
        location: KeyLocation,
    ) -> Result<(), GroupStateError> {
        let end = start + key_len;
        if let KeyLocation::Existing(index) = location {
            let next = match self.entries[index].row_count.checked_add(count) {
                Some(next) => next,
                None => {
                    self.arena.truncate(start);
                    return Err(self.poison(GroupStateError::ArithmeticOverflow));
                }
            };
            self.arena.truncate(start);
            self.entries[index].row_count = next;
            return Ok(());
        }
        if self.entries.len() >= usize::from(self.bounds.maximum_groups) {
            self.arena.truncate(start);
            return Err(self.poison(GroupStateError::GroupBoundExceeded));
        }
        if end > self.retained_key_capacity {
            self.arena.truncate(start);
            return Err(self.poison(GroupStateError::StateBoundExceeded));
        }
        let key_start = match u32::try_from(start) {
            Ok(value) => value,
            Err(_) => return Err(self.poison(GroupStateError::StateBoundExceeded)),
        };
        let key_len = match u32::try_from(key_len) {
            Ok(value) => value,
            Err(_) => return Err(self.poison(GroupStateError::KeyBoundExceeded)),
        };
        let KeyLocation::Insert(index) = location else {
            return Err(self.poison(GroupStateError::ArithmeticOverflow));
        };
        self.entries.insert(
            index,
            GroupEntry {
                key_start,
                key_len,
                row_count: count,
            },
        );
        Ok(())
    }

    fn key(&self, index: usize) -> Option<&[u8]> {
        self.entries
            .get(index)
            .map(|entry| entry_key(&self.arena, entry))
    }

    fn count(&self, index: usize) -> Option<u64> {
        self.entries.get(index).map(|entry| entry.row_count)
    }

    #[cfg(test)]
    fn allocated_state_bytes(&self) -> usize {
        self.arena.capacity() + self.entries.capacity() * size_of::<GroupEntry>()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyLocation {
    Existing(usize),
    Insert(usize),
}

const fn insertion_shift_work(location: KeyLocation, entries: usize) -> usize {
    match location {
        KeyLocation::Existing(_) => 0,
        KeyLocation::Insert(index) => entries - index,
    }
}

fn entry_key<'arena>(arena: &'arena [u8], entry: &GroupEntry) -> &'arena [u8] {
    let start = entry.key_start as usize;
    &arena[start..start + entry.key_len as usize]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CountedFailure {
    error: GroupStateError,
    completed_work: usize,
}

fn lookup_comparison_ceiling(entries: usize) -> Result<usize, GroupStateError> {
    let mut width = entries;
    let mut comparisons = 0_usize;
    while width > 0 {
        comparisons = comparisons
            .checked_add(1)
            .ok_or(GroupStateError::ArithmeticOverflow)?;
        width /= 2;
    }
    Ok(comparisons)
}

fn row_work_ceiling(lanes: usize, entries: usize) -> Result<usize, GroupStateError> {
    let passes = lanes
        .checked_mul(2)
        .ok_or(GroupStateError::ArithmeticOverflow)?;
    let comparisons = lookup_comparison_ceiling(entries)?;
    passes
        .checked_add(comparisons)
        .and_then(|work| work.checked_add(entries))
        .ok_or(GroupStateError::ArithmeticOverflow)
}

fn encoded_key_len(
    lanes: &[CanonicalGroupLane],
    cells: &[SegmentV2Cell],
) -> Result<(usize, usize), CountedFailure> {
    if lanes.is_empty() || lanes.len() != cells.len() {
        return Err(CountedFailure {
            error: GroupStateError::InputShape,
            completed_work: 0,
        });
    }
    let mut total = 0_usize;
    for (index, (lane, cell)) in lanes.iter().zip(cells).enumerate() {
        let completed_work = index + 1;
        let value_len = match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null if lane.facts.optional() => {
                NO_VALUE_BYTES.len()
            }
            SegmentV2Cell::Missing | SegmentV2Cell::Null => {
                return Err(CountedFailure {
                    error: GroupStateError::UnexpectedNoValue,
                    completed_work,
                });
            }
            SegmentV2Cell::Value(value) => {
                if !lane.facts.logical().accepts(value) {
                    return Err(CountedFailure {
                        error: GroupStateError::TypeMismatch,
                        completed_work,
                    });
                }
                canonical_value_encoded_len(value).map_err(|_| CountedFailure {
                    error: GroupStateError::InputShape,
                    completed_work,
                })?
            }
        };
        let framed = 4_usize.checked_add(value_len).ok_or(CountedFailure {
            error: GroupStateError::ArithmeticOverflow,
            completed_work,
        })?;
        total = total.checked_add(framed).ok_or(CountedFailure {
            error: GroupStateError::ArithmeticOverflow,
            completed_work,
        })?;
    }
    Ok((total, lanes.len()))
}

fn append_key(arena: &mut Vec<u8>, cells: &[SegmentV2Cell]) -> Result<usize, CountedFailure> {
    for (index, cell) in cells.iter().enumerate() {
        let completed_work = index + 1;
        let value_len = match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null => NO_VALUE_BYTES.len(),
            SegmentV2Cell::Value(value) => {
                canonical_value_encoded_len(value).map_err(|_| CountedFailure {
                    error: GroupStateError::InputShape,
                    completed_work,
                })?
            }
        };
        let value_len = u32::try_from(value_len).map_err(|_| CountedFailure {
            error: GroupStateError::KeyBoundExceeded,
            completed_work,
        })?;
        arena.extend_from_slice(&value_len.to_be_bytes());
        match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null => {
                arena.extend_from_slice(&NO_VALUE_BYTES)
            }
            SegmentV2Cell::Value(value) => {
                encode_canonical_value_into(arena, value).map_err(|_| CountedFailure {
                    error: GroupStateError::InputShape,
                    completed_work,
                })?
            }
        }
    }
    Ok(cells.len())
}

pub(super) struct CanonicalGroupLeafBuilder<'schema> {
    identity: CanonicalPartialIdentity,
    lanes: &'schema [CanonicalGroupLane],
    owner: BoundedGroupOwner,
    next_row_ordinal: u32,
}

impl<'schema> CanonicalGroupLeafBuilder<'schema> {
    pub(super) fn new(
        identity: CanonicalPartialIdentity,
        lanes: &'schema [CanonicalGroupLane],
        bounds: CanonicalGroupBounds,
        work: &mut GroupWorkBudget,
    ) -> Result<Self, GroupStateError> {
        validate_group_lanes(lanes, work)?;
        Ok(Self {
            identity,
            lanes,
            owner: BoundedGroupOwner::new(bounds)?,
            next_row_ordinal: 0,
        })
    }

    pub(super) fn push_row(
        &mut self,
        row_ordinal: u32,
        cells: &[SegmentV2Cell],
        work: &mut GroupWorkBudget,
    ) -> Result<(), GroupStateError> {
        self.owner.ensure_live()?;
        if self.next_row_ordinal >= MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 {
            return Err(self.owner.poison(GroupStateError::RowBoundExceeded));
        }
        if row_ordinal != self.next_row_ordinal {
            return Err(self.owner.poison(GroupStateError::RowOrder));
        }
        let next = match self.next_row_ordinal.checked_add(1) {
            Some(next) => next,
            None => return Err(self.owner.poison(GroupStateError::ArithmeticOverflow)),
        };
        let maximum_work = match row_work_ceiling(self.lanes.len(), self.owner.entries.len()) {
            Ok(maximum_work) => maximum_work,
            Err(error) => return Err(self.owner.poison(error)),
        };
        if let Err(error) = work.preflight(maximum_work) {
            return Err(self.owner.poison(error));
        }
        self.owner.admit_cells(self.lanes, cells, 1, work)?;
        self.next_row_ordinal = next;
        Ok(())
    }

    pub(super) const fn is_poisoned(&self) -> bool {
        self.owner.poisoned
    }

    pub(super) fn finish(self) -> Result<FinalizedGroupLeaf<'schema>, GroupStateError> {
        self.owner.ensure_live()?;
        Ok(FinalizedGroupLeaf {
            identity: self.identity,
            lanes: self.lanes,
            owner: self.owner,
        })
    }
}

pub(super) struct FinalizedGroupLeaf<'schema> {
    identity: CanonicalPartialIdentity,
    lanes: &'schema [CanonicalGroupLane],
    owner: BoundedGroupOwner,
}

impl FinalizedGroupLeaf<'_> {
    #[cfg(test)]
    fn group_count(&self) -> usize {
        self.owner.entries.len()
    }
    #[cfg(test)]
    fn row_count_for_test(&self, index: usize) -> Option<u64> {
        self.owner.count(index)
    }
}

pub(super) struct CanonicalGroupMerge<'schema, 'inventory> {
    lanes: &'schema [CanonicalGroupLane],
    inventory: &'inventory [CanonicalPartialIdentity],
    cursor: usize,
    owner: Option<BoundedGroupOwner>,
    poisoned: bool,
}

impl<'schema, 'inventory> CanonicalGroupMerge<'schema, 'inventory> {
    pub(super) fn new(
        lanes: &'schema [CanonicalGroupLane],
        inventory: &'inventory [CanonicalPartialIdentity],
        bounds: CanonicalGroupBounds,
        work: &mut GroupWorkBudget,
    ) -> Result<Self, GroupStateError> {
        if lanes.is_empty() {
            return Err(GroupStateError::InputShape);
        }
        if lanes.len() > MAX_SEGMENT_V2_COLUMNS {
            return Err(GroupStateError::LaneBoundExceeded);
        }
        if inventory.is_empty() {
            return Ok(Self {
                lanes,
                inventory,
                cursor: 0,
                owner: None,
                poisoned: false,
            });
        }
        if inventory.len() > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 as usize {
            return Err(GroupStateError::InventoryBoundExceeded);
        }
        let constructor_work = lanes
            .len()
            .checked_add(inventory.len())
            .ok_or(GroupStateError::ArithmeticOverflow)?;
        work.preflight(constructor_work)?;
        validate_group_lanes(lanes, work)?;
        validate_inventory(inventory, work)?;
        let owner = Some(BoundedGroupOwner::new(bounds)?);
        Ok(Self {
            lanes,
            inventory,
            cursor: 0,
            owner,
            poisoned: false,
        })
    }

    pub(super) fn merge_leaf(
        &mut self,
        leaf: FinalizedGroupLeaf<'_>,
        work: &mut GroupWorkBudget,
    ) -> Result<(), GroupStateError> {
        if self.poisoned {
            return Err(GroupStateError::Poisoned);
        }
        if self.inventory.get(self.cursor) != Some(&leaf.identity) {
            if let Err(error) = work.preflight(self.inventory.len().max(1)) {
                self.poison();
                return Err(error);
            }
            let error = classify_mismatch(self.inventory, self.cursor, leaf.identity);
            if work.charge(self.inventory.len().max(1)).is_err() {
                self.poison();
                return Err(GroupStateError::ArithmeticOverflow);
            }
            self.poison();
            return Err(error);
        }
        let owner_entries = self.owner.as_ref().map_or(0, |owner| owner.entries.len());
        let merge_work =
            match merge_work_ceiling(self.lanes.len(), owner_entries, leaf.owner.entries.len()) {
                Ok(merge_work) => merge_work,
                Err(error) => {
                    self.poison();
                    return Err(error);
                }
            };
        let admitted = match work.preflight(merge_work) {
            Ok(admitted) => admitted,
            Err(error) => {
                self.poison();
                return Err(error);
            }
        };
        if self.lanes != leaf.lanes {
            if work.commit_actual(admitted, self.lanes.len()).is_err() {
                self.poison();
                return Err(GroupStateError::ArithmeticOverflow);
            }
            self.poison();
            return Err(GroupStateError::IncompatibleLeaf);
        }
        let owner = match self.owner.as_mut() {
            Some(owner) => owner,
            None => {
                self.poisoned = true;
                return Err(GroupStateError::ExcessLeaf);
            }
        };
        let mut actual_work = self.lanes.len();
        for index in 0..leaf.owner.entries.len() {
            let Some(key) = leaf.owner.key(index) else {
                self.poison();
                return Err(GroupStateError::IncompatibleLeaf);
            };
            let Some(count) = leaf.owner.count(index) else {
                self.poison();
                return Err(GroupStateError::IncompatibleLeaf);
            };
            match owner.admit_encoded(key, count) {
                Ok(contribution_work) => {
                    actual_work = match actual_work.checked_add(contribution_work) {
                        Some(work) => work,
                        None => {
                            self.poison();
                            return Err(GroupStateError::ArithmeticOverflow);
                        }
                    };
                }
                Err(failure) => {
                    actual_work = match actual_work.checked_add(failure.completed_work) {
                        Some(work) => work,
                        None => {
                            self.poison();
                            return Err(GroupStateError::ArithmeticOverflow);
                        }
                    };
                    if work.commit_actual(admitted, actual_work).is_err() {
                        self.poison();
                        return Err(GroupStateError::ArithmeticOverflow);
                    }
                    self.poisoned = true;
                    return Err(failure.error);
                }
            }
        }
        if work.commit_actual(admitted, actual_work).is_err() {
            self.poison();
            return Err(GroupStateError::ArithmeticOverflow);
        }
        self.cursor += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<CanonicalGroups, GroupStateError> {
        if self.poisoned {
            return Err(GroupStateError::Poisoned);
        }
        if self.cursor != self.inventory.len() {
            self.poison();
            return Err(GroupStateError::InventoryOmission);
        }
        if let Some(owner) = self.owner.as_mut() {
            owner.ensure_live()?;
        }
        Ok(CanonicalGroups { owner: self.owner })
    }

    fn poison(&mut self) {
        if let Some(owner) = self.owner.as_mut() {
            owner.poisoned = true;
            owner.entries.clear();
            owner.arena.clear();
        }
        self.poisoned = true;
    }
}

pub(super) struct CanonicalGroups {
    owner: Option<BoundedGroupOwner>,
}

fn validate_group_lanes(
    lanes: &[CanonicalGroupLane],
    work: &mut GroupWorkBudget,
) -> Result<(), GroupStateError> {
    if lanes.is_empty() {
        return Err(GroupStateError::InputShape);
    }
    if lanes.len() > MAX_SEGMENT_V2_COLUMNS {
        return Err(GroupStateError::LaneBoundExceeded);
    }
    work.charge(lanes.len())?;
    let first = &lanes[0].facts;
    for pair in lanes.windows(2) {
        let previous = &pair[0].facts;
        let next = &pair[1].facts;
        if !first.same_program(next) {
            return Err(GroupStateError::ProgramBindingSubstitution);
        }
        if previous.ordinal() >= next.ordinal() || previous.field() >= next.field() {
            return Err(GroupStateError::InputShape);
        }
    }
    Ok(())
}

fn merge_work_ceiling(
    lanes: usize,
    retained_entries: usize,
    contributions: usize,
) -> Result<usize, GroupStateError> {
    let mut maximum_work = lanes;
    for contribution in 0..contributions {
        let possible_entries = retained_entries
            .checked_add(contribution)
            .ok_or(GroupStateError::ArithmeticOverflow)?;
        let comparisons = lookup_comparison_ceiling(possible_entries)?;
        maximum_work = maximum_work
            .checked_add(1)
            .and_then(|work| work.checked_add(comparisons))
            .ok_or(GroupStateError::ArithmeticOverflow)?;
    }
    // Contributions arrive in canonical order. An inserted contribution can
    // therefore shift only entries retained before this leaf; earlier inserts
    // from the same leaf always precede it and never move right.
    let maximum_shifts = retained_entries
        .checked_mul(contributions)
        .ok_or(GroupStateError::ArithmeticOverflow)?;
    maximum_work
        .checked_add(maximum_shifts)
        .ok_or(GroupStateError::ArithmeticOverflow)
}

fn validate_inventory(
    inventory: &[CanonicalPartialIdentity],
    work: &mut GroupWorkBudget,
) -> Result<(), GroupStateError> {
    if inventory.len() > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 as usize {
        return Err(GroupStateError::InventoryBoundExceeded);
    }
    work.charge(inventory.len())?;
    if inventory.is_empty() {
        return Ok(());
    }
    if inventory.first().is_some_and(|id| id.batch_ordinal() != 0) {
        return Err(GroupStateError::InvalidInventory);
    }
    let valid = inventory.windows(2).all(|pair| {
        let previous = pair[0];
        let next = pair[1];
        if next <= previous {
            return false;
        }
        if previous.root_inventory_ordinal() == next.root_inventory_ordinal()
            && previous.segment_id() == next.segment_id()
        {
            previous.batch_ordinal().checked_add(1) == Some(next.batch_ordinal())
        } else {
            next.batch_ordinal() == 0
        }
    });
    if valid {
        Ok(())
    } else {
        Err(GroupStateError::InvalidInventory)
    }
}

fn classify_mismatch(
    inventory: &[CanonicalPartialIdentity],
    cursor: usize,
    actual: CanonicalPartialIdentity,
) -> GroupStateError {
    let mut seen_before = false;
    let mut seen_after = false;
    let mut root_exists = false;
    let mut segment_exists = false;
    for (index, identity) in inventory.iter().enumerate() {
        seen_before |= index < cursor && *identity == actual;
        seen_after |= index > cursor && *identity == actual;
        root_exists |= identity.root_inventory_ordinal() == actual.root_inventory_ordinal();
        segment_exists |= identity.root_inventory_ordinal() == actual.root_inventory_ordinal()
            && identity.segment_id() == actual.segment_id();
    }
    if seen_before {
        return GroupStateError::DuplicateOrReorderedLeaf;
    }
    if cursor >= inventory.len() {
        return GroupStateError::ExcessLeaf;
    }
    if seen_after {
        return GroupStateError::InventoryGap;
    }
    if !root_exists {
        return GroupStateError::ForeignRootInventory;
    }
    if !segment_exists {
        return GroupStateError::ForeignSegment;
    }
    match actual.cmp(&inventory[cursor]) {
        Ordering::Less | Ordering::Equal => GroupStateError::DuplicateOrReorderedLeaf,
        Ordering::Greater => GroupStateError::InventoryGap,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CanonicalGroupBounds, CanonicalGroupLane, CanonicalGroupLeafBuilder, CanonicalGroupMerge,
        CanonicalGroups, GroupStateError, GroupWorkBudget, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
        MAX_AGGREGATE_DISTINCT_VALUES_V1, MAX_AGGREGATE_STATE_BYTES_V1,
    };
    use crate::batch::aggregate::CanonicalPartialIdentity;
    use crate::batch::program::checked_group_lane_facts_for_test;
    use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};
    use riffdb_types::{
        CanonicalValue, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId,
        FieldId, Money, Timestamp, encode_canonical_value,
    };
    use std::collections::BTreeMap;
    use std::mem::size_of;

    fn identity(root: u16, segment: u8, batch: u32) -> CanonicalPartialIdentity {
        CanonicalPartialIdentity::new(root, SegmentV2SegmentId::from_bytes([segment; 16]), batch)
    }
    fn bounds(groups: u16, key: u32, state: u32) -> CanonicalGroupBounds {
        CanonicalGroupBounds::new(groups, key, state).expect("valid bounds")
    }
    fn schema(
        identity_seed: u8,
        lanes: &[(SegmentV2LogicalType, bool)],
    ) -> Vec<CanonicalGroupLane> {
        checked_group_lane_facts_for_test(
            identity_seed,
            lanes
                .iter()
                .enumerate()
                .map(|(index, (logical, optional))| {
                    (
                        FieldId::new(u32::try_from(index + 1).expect("small lane index"))
                            .expect("nonzero field"),
                        logical.clone(),
                        *optional,
                    )
                })
                .collect(),
        )
        .into_iter()
        .map(CanonicalGroupLane::from_checked)
        .collect()
    }
    fn work() -> GroupWorkBudget {
        GroupWorkBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1).expect("global work ceiling")
    }
    fn oracle_key(cells: &[SegmentV2Cell]) -> Vec<u8> {
        let mut key = Vec::new();
        for cell in cells {
            let value = match cell {
                SegmentV2Cell::Missing | SegmentV2Cell::Null => CanonicalValue::Null,
                SegmentV2Cell::Value(value) => value.clone(),
            };
            let encoded = encode_canonical_value(&value).expect("oracle encoding");
            key.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            key.extend_from_slice(&encoded);
        }
        key
    }
    fn oracle(rows: &[Vec<SegmentV2Cell>]) -> Vec<(Vec<u8>, u64)> {
        let mut groups = BTreeMap::new();
        for row in rows {
            *groups.entry(oracle_key(row)).or_insert(0_u64) += 1;
        }
        groups.into_iter().collect()
    }
    fn group_rows(groups: &CanonicalGroups) -> Vec<(Vec<u8>, u64)> {
        groups.owner.as_ref().map_or_else(Vec::new, |owner| {
            (0..owner.entries.len())
                .map(|index| {
                    (
                        owner.key(index).expect("sealed index").to_vec(),
                        owner.count(index).expect("sealed index"),
                    )
                })
                .collect()
        })
    }
    fn leaf<'schema>(
        id: CanonicalPartialIdentity,
        schema: &'schema [CanonicalGroupLane],
        rows: &[Vec<SegmentV2Cell>],
    ) -> super::FinalizedGroupLeaf<'schema> {
        let mut work = work();
        let mut builder =
            CanonicalGroupLeafBuilder::new(id, schema, bounds(16, 256, 4_096), &mut work)
                .expect("builder");
        for (ordinal, row) in rows.iter().enumerate() {
            builder
                .push_row(ordinal as u32, row, &mut work)
                .expect("canonical row");
        }
        builder.finish().expect("leaf")
    }

    #[test]
    fn canonical_group_leaf_normalizes_missing_and_null_without_colliding_with_values() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), true)]);
        let rows = [
            vec![SegmentV2Cell::Missing],
            vec![SegmentV2Cell::Null],
            vec![SegmentV2Cell::Value(CanonicalValue::U64(0))],
        ];
        let groups = leaf(identity(0, 7, 0), &schema, &rows);
        assert_eq!(groups.group_count(), 2);
        assert_eq!(groups.row_count_for_test(0), Some(2));
        assert_eq!(groups.row_count_for_test(1), Some(1));
        let mut work = work();
        let mut wrong_type = CanonicalGroupLeafBuilder::new(
            identity(0, 7, 0),
            &schema,
            bounds(3, 64, 256),
            &mut work,
        )
        .expect("owner");
        assert_eq!(
            wrong_type.push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::I64(0))],
                &mut work,
            ),
            Err(GroupStateError::TypeMismatch)
        );
        assert!(wrong_type.is_poisoned());
        assert_eq!(wrong_type.finish().err(), Some(GroupStateError::Poisoned));
    }

    #[test]
    fn independent_group_key_bounds_are_exact_and_plus_one_refuses_atomically() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let row = [SegmentV2Cell::Value(CanonicalValue::U64(7))];
        let exact = u32::try_from(oracle_key(&row).len()).expect("length");
        let mut accepted_work = work();
        let mut accepted = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact, 256),
            &mut accepted_work,
        )
        .expect("owner");
        accepted
            .push_row(0, &row, &mut accepted_work)
            .expect("exact");
        assert_eq!(accepted.finish().expect("leaf").group_count(), 1);
        let mut rejected_work = work();
        let mut rejected = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact - 1, 256),
            &mut rejected_work,
        )
        .expect("owner");
        assert_eq!(
            rejected.push_row(0, &row, &mut rejected_work),
            Err(GroupStateError::KeyBoundExceeded)
        );
        assert!(rejected.is_poisoned());
    }

    #[test]
    fn independent_group_count_and_state_bounds_are_exact_and_plus_one_refuses() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let encoded_key_bytes = oracle_key(&[SegmentV2Cell::Value(CanonicalValue::U64(1))]);
        let key_bound = u32::try_from(encoded_key_bytes.len()).expect("small key");
        let group_state =
            u32::try_from(2 * size_of::<super::GroupEntry>() + 3 * encoded_key_bytes.len())
                .expect("small state");
        let mut owner_work = work();
        let mut owner = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, key_bound, group_state),
            &mut owner_work,
        )
        .expect("owner");
        assert_eq!(owner.owner.allocated_state_bytes(), group_state as usize);
        owner
            .push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::U64(1))],
                &mut owner_work,
            )
            .expect("one");
        owner
            .push_row(
                1,
                &[SegmentV2Cell::Value(CanonicalValue::U64(2))],
                &mut owner_work,
            )
            .expect("two");
        assert_eq!(
            owner.push_row(
                2,
                &[SegmentV2Cell::Value(CanonicalValue::U64(3))],
                &mut owner_work,
            ),
            Err(GroupStateError::GroupBoundExceeded)
        );
        assert!(owner.owner.entries.is_empty() && owner.owner.arena.is_empty());

        let exact_key_bytes = oracle_key(&[SegmentV2Cell::Value(CanonicalValue::U64(9))]);
        let exact_state = u32::try_from(size_of::<super::GroupEntry>() + 2 * exact_key_bytes.len())
            .expect("small state");
        let mut exact_work = work();
        let mut exact_state_owner = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact_key_bytes.len() as u32, exact_state),
            &mut exact_work,
        )
        .expect("exact state owner");
        exact_state_owner
            .push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::U64(9))],
                &mut exact_work,
            )
            .expect("exact retained state");
        exact_state_owner
            .push_row(
                1,
                &[SegmentV2Cell::Value(CanonicalValue::U64(9))],
                &mut exact_work,
            )
            .expect("duplicate uses the reserved scratch key");
        assert_eq!(
            exact_state_owner
                .finish()
                .expect("leaf")
                .row_count_for_test(0),
            Some(2)
        );
        let mut short_work = work();
        let mut plus_one_state = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact_key_bytes.len() as u32, exact_state - 1),
            &mut short_work,
        )
        .expect("one-byte-short owner");
        assert_eq!(
            plus_one_state.push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::U64(9))],
                &mut short_work,
            ),
            Err(GroupStateError::StateBoundExceeded)
        );
        assert!(plus_one_state.owner.entries.is_empty() && plus_one_state.owner.arena.is_empty());
        assert!(CanonicalGroupBounds::new(MAX_AGGREGATE_DISTINCT_VALUES_V1, 1, 4_096).is_ok());
        assert_eq!(
            CanonicalGroupBounds::new(MAX_AGGREGATE_DISTINCT_VALUES_V1 + 1, 1, 4_096),
            Err(GroupStateError::InvalidGroupBound)
        );
        assert!(CanonicalGroupBounds::new(1, 1, MAX_AGGREGATE_STATE_BYTES_V1).is_ok());
        assert_eq!(
            CanonicalGroupBounds::new(1, 1, MAX_AGGREGATE_STATE_BYTES_V1 + 1),
            Err(GroupStateError::InvalidStateBound)
        );
        let mut invalid_state_work = work();
        assert_eq!(
            CanonicalGroupLeafBuilder::new(
                identity(0, 1, 0),
                &schema,
                bounds(2, 1, 31),
                &mut invalid_state_work,
            )
            .err(),
            Some(GroupStateError::InvalidStateBound)
        );
    }

    #[test]
    fn canonical_framing_prevents_component_collisions_and_row_order_is_sealed() {
        let string_type = SegmentV2LogicalType::String;
        let schema = schema(
            1,
            &[(string_type.clone(), false), (string_type.clone(), false)],
        );
        let rows = [
            vec![
                SegmentV2Cell::Value(CanonicalValue::string("ab").expect("string")),
                SegmentV2Cell::Value(CanonicalValue::string("c").expect("string")),
            ],
            vec![
                SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
                SegmentV2Cell::Value(CanonicalValue::string("bc").expect("string")),
            ],
        ];
        let result = leaf(identity(0, 1, 0), &schema, &rows);
        assert_eq!(result.group_count(), 2);
        assert_ne!(result.owner.key(0), result.owner.key(1));
        let mut reordered_work = work();
        let mut reordered = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, 128, 512),
            &mut reordered_work,
        )
        .expect("owner");
        assert_eq!(
            reordered.push_row(1, &rows[0], &mut reordered_work),
            Err(GroupStateError::RowOrder)
        );
        assert!(reordered.is_poisoned());
    }

    #[test]
    fn every_closed_group_key_type_matches_the_independent_canonical_oracle() {
        let enum_type = EnumTypeId::new(7).expect("enum type");
        let decimal = DecimalSpec::new(8, 2).expect("decimal");
        let usd = CurrencyCode::new("USD").expect("currency");
        let cases = vec![
            (SegmentV2LogicalType::Bool, CanonicalValue::Bool(true)),
            (SegmentV2LogicalType::I64, CanonicalValue::I64(-7)),
            (SegmentV2LogicalType::U64, CanonicalValue::U64(7)),
            (
                SegmentV2LogicalType::String,
                CanonicalValue::string("group").expect("string"),
            ),
            (
                SegmentV2LogicalType::Bytes,
                CanonicalValue::bytes([0, 1, 2]).expect("bytes"),
            ),
            (
                SegmentV2LogicalType::Timestamp,
                CanonicalValue::Timestamp(Timestamp::new(-1, 9).expect("timestamp")),
            ),
            (
                SegmentV2LogicalType::Date,
                CanonicalValue::Date(Date::new(-1)),
            ),
            (SegmentV2LogicalType::Uuid, CanonicalValue::Uuid([3; 16])),
            (
                SegmentV2LogicalType::Enum(enum_type),
                CanonicalValue::Enum {
                    type_id: enum_type,
                    variant_id: EnumVariantId::new(9).expect("variant"),
                },
            ),
            (
                SegmentV2LogicalType::Decimal(decimal),
                CanonicalValue::Decimal(Decimal::new(decimal, -123).expect("decimal")),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                CanonicalValue::Money(Money::new(usd, Decimal::new(decimal, 123).expect("amount"))),
            ),
        ];
        for (logical_type, value) in cases {
            let schema = schema(1, &[(logical_type.clone(), true)]);
            let rows = vec![
                vec![SegmentV2Cell::Missing],
                vec![SegmentV2Cell::Null],
                vec![SegmentV2Cell::Value(value)],
            ];
            let inventory = [identity(0, 4, 0)];
            let mut merge_work = work();
            let mut merged = CanonicalGroupMerge::new(
                &schema,
                &inventory,
                bounds(4, 256, 1_024),
                &mut merge_work,
            )
            .expect("merge");
            merged
                .merge_leaf(leaf(inventory[0], &schema, &rows), &mut merge_work)
                .expect("leaf");
            assert_eq!(group_rows(&merged.finish().expect("groups")), oracle(&rows));
        }

        let other_decimal = DecimalSpec::new(9, 2).expect("other decimal");
        let money_type = SegmentV2LogicalType::Money {
            currency: usd,
            amount: decimal,
        };
        let schema = schema(1, &[(money_type.clone(), false)]);
        let mut mismatch_work = work();
        let mut mismatched = CanonicalGroupLeafBuilder::new(
            identity(0, 4, 0),
            &schema,
            bounds(2, 128, 512),
            &mut mismatch_work,
        )
        .expect("owner");
        assert_eq!(
            mismatched.push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                    usd,
                    Decimal::new(other_decimal, 1).expect("amount"),
                )))],
                &mut mismatch_work,
            ),
            Err(GroupStateError::TypeMismatch)
        );
    }

    #[test]
    fn canonical_partition_merge_matches_independent_scalar_oracle() {
        let u64_type = SegmentV2LogicalType::U64;
        let string_type = SegmentV2LogicalType::String;
        let schema = schema(1, &[(u64_type.clone(), true), (string_type.clone(), true)]);
        let rows = vec![
            vec![SegmentV2Cell::Missing, SegmentV2Cell::Null],
            vec![SegmentV2Cell::Null, SegmentV2Cell::Missing],
            vec![
                SegmentV2Cell::Value(CanonicalValue::U64(2)),
                SegmentV2Cell::Value(CanonicalValue::string("z").expect("string")),
            ],
            vec![
                SegmentV2Cell::Value(CanonicalValue::U64(1)),
                SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
            ],
            vec![SegmentV2Cell::Missing, SegmentV2Cell::Null],
            vec![
                SegmentV2Cell::Value(CanonicalValue::U64(1)),
                SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
            ],
        ];
        let expected = oracle(&rows);
        for cuts in [vec![0, 2, 6], vec![0, 1, 4, 6], vec![0, 3, 5, 6]] {
            let inventory = match cuts.len() - 1 {
                2 => vec![identity(0, 9, 0), identity(0, 9, 1)],
                3 => vec![identity(0, 8, 0), identity(0, 9, 0), identity(1, 1, 0)],
                _ => unreachable!("closed test partitions"),
            };
            let mut merge_work = work();
            let mut merged = CanonicalGroupMerge::new(
                &schema,
                &inventory,
                bounds(16, 256, 4_096),
                &mut merge_work,
            )
            .expect("merge");
            for (batch, window) in cuts.windows(2).enumerate() {
                merged
                    .merge_leaf(
                        leaf(inventory[batch], &schema, &rows[window[0]..window[1]]),
                        &mut merge_work,
                    )
                    .expect("merge leaf");
            }
            assert_eq!(group_rows(&merged.finish().expect("complete")), expected);
        }

        for permutation in [[0, 1, 2, 3, 4, 5], [5, 4, 3, 2, 1, 0], [2, 0, 5, 1, 4, 3]] {
            let permuted = permutation
                .into_iter()
                .map(|index| rows[index].clone())
                .collect::<Vec<_>>();
            let inventory = [identity(0, 8, 0), identity(0, 9, 0), identity(1, 1, 0)];
            let mut merge_work = work();
            let mut merged = CanonicalGroupMerge::new(
                &schema,
                &inventory,
                bounds(16, 256, 4_096),
                &mut merge_work,
            )
            .expect("merge");
            for (batch, partition) in permuted.chunks(2).enumerate() {
                merged
                    .merge_leaf(leaf(inventory[batch], &schema, partition), &mut merge_work)
                    .expect("canonical partition permutation");
            }
            assert_eq!(group_rows(&merged.finish().expect("complete")), expected);
        }
    }

    #[test]
    fn merge_refuses_duplicate_gap_foreign_excess_and_omission_without_partial_state() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]];
        let inventory = [identity(0, 1, 0), identity(0, 1, 1)];
        for (actual, expected) in [
            (identity(0, 1, 1), GroupStateError::InventoryGap),
            (identity(1, 1, 0), GroupStateError::ForeignRootInventory),
            (identity(0, 2, 0), GroupStateError::ForeignSegment),
        ] {
            let mut merge_work = work();
            let mut merge =
                CanonicalGroupMerge::new(&schema, &inventory, bounds(4, 64, 512), &mut merge_work)
                    .expect("merge");
            assert_eq!(
                merge.merge_leaf(leaf(actual, &schema, &rows), &mut merge_work),
                Err(expected)
            );
            assert!(merge.owner.as_ref().expect("owner").entries.is_empty());
            assert_eq!(merge.finish().err(), Some(GroupStateError::Poisoned));
        }
        let mut duplicate_work = work();
        let mut duplicate =
            CanonicalGroupMerge::new(&schema, &inventory, bounds(4, 64, 512), &mut duplicate_work)
                .expect("merge");
        duplicate
            .merge_leaf(leaf(inventory[0], &schema, &rows), &mut duplicate_work)
            .expect("first");
        assert_eq!(
            duplicate.merge_leaf(leaf(inventory[0], &schema, &rows), &mut duplicate_work,),
            Err(GroupStateError::DuplicateOrReorderedLeaf)
        );
        assert!(duplicate.owner.as_ref().expect("owner").entries.is_empty());
        let complete = [identity(0, 1, 0)];
        let mut excess_work = work();
        let mut excess =
            CanonicalGroupMerge::new(&schema, &complete, bounds(4, 64, 512), &mut excess_work)
                .expect("merge");
        excess
            .merge_leaf(leaf(complete[0], &schema, &rows), &mut excess_work)
            .expect("first");
        assert_eq!(
            excess.merge_leaf(leaf(identity(0, 1, 1), &schema, &rows), &mut excess_work,),
            Err(GroupStateError::ExcessLeaf)
        );
        assert!(excess.owner.as_ref().expect("owner").entries.is_empty());
        let mut omission_work = work();
        let omission =
            CanonicalGroupMerge::new(&schema, &inventory, bounds(4, 64, 512), &mut omission_work)
                .expect("merge");
        assert_eq!(
            omission.finish().err(),
            Some(GroupStateError::InventoryOmission)
        );

        let other_type = SegmentV2LogicalType::I64;
        let other_schema = self::schema(1, &[(other_type.clone(), false)]);
        let other_rows = [vec![SegmentV2Cell::Value(CanonicalValue::I64(1))]];
        let mut incompatible_work = work();
        let mut incompatible = CanonicalGroupMerge::new(
            &schema,
            &complete,
            bounds(4, 64, 512),
            &mut incompatible_work,
        )
        .expect("merge");
        assert_eq!(
            incompatible.merge_leaf(
                leaf(complete[0], &other_schema, &other_rows),
                &mut incompatible_work,
            ),
            Err(GroupStateError::IncompatibleLeaf)
        );
        assert!(
            incompatible
                .owner
                .as_ref()
                .expect("owner")
                .entries
                .is_empty()
        );
    }

    #[test]
    fn malformed_inventory_and_arithmetic_overflow_fail_closed() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let duplicate = [identity(0, 1, 0), identity(0, 1, 0)];
        let gap = [identity(0, 1, 0), identity(0, 1, 2)];
        let nonzero_first = [identity(0, 1, 1)];
        for inventory in [&duplicate[..], &gap, &nonzero_first] {
            let mut invalid_work = work();
            assert_eq!(
                CanonicalGroupMerge::new(
                    &schema,
                    inventory,
                    bounds(4, 64, 512),
                    &mut invalid_work,
                )
                .err(),
                Some(GroupStateError::InvalidInventory)
            );
        }
        let mut owner = super::BoundedGroupOwner::new(bounds(1, 64, 256)).expect("owner");
        owner.admit_encoded(&[0], u64::MAX).expect("first");
        assert_eq!(
            owner
                .admit_encoded(&[0], 1)
                .map_err(|failure| failure.error),
            Err(GroupStateError::ArithmeticOverflow)
        );
        assert!(owner.entries.is_empty() && owner.arena.is_empty());
    }

    #[test]
    fn row_inventory_and_mismatch_work_are_exact_bounded_and_single_attempt() {
        assert_eq!(
            GroupWorkBudget::new(0),
            Err(GroupStateError::InvalidWorkBound)
        );
        assert_eq!(
            GroupWorkBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 + 1),
            Err(GroupStateError::InvalidWorkBound)
        );

        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let maximum_inventory = (0..MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 - 1)
            .map(|batch| identity(0, 1, batch))
            .collect::<Vec<_>>();
        let mut exact_inventory_work =
            GroupWorkBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1).expect("maximum work");
        CanonicalGroupMerge::new(
            &schema,
            &maximum_inventory,
            bounds(1, 16, 64),
            &mut exact_inventory_work,
        )
        .expect("exact combined lane and inventory work");
        assert_eq!(exact_inventory_work.remaining(), 0);

        let mut work_bounded_inventory = maximum_inventory.clone();
        work_bounded_inventory.push(identity(0, 1, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 - 1));
        let mut preflight_work =
            GroupWorkBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1).expect("maximum work");
        assert_eq!(
            CanonicalGroupMerge::new(
                &schema,
                &work_bounded_inventory,
                bounds(1, 16, 64),
                &mut preflight_work,
            )
            .err(),
            Some(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(
            preflight_work.remaining(),
            MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1
        );

        let mut excessive_inventory = work_bounded_inventory;
        excessive_inventory.push(identity(0, 1, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1));
        let mut untouched_work = GroupWorkBudget::new(1).expect("one work");
        assert_eq!(
            CanonicalGroupMerge::new(
                &schema,
                &excessive_inventory,
                bounds(1, 16, 64),
                &mut untouched_work,
            )
            .err(),
            Some(GroupStateError::InventoryBoundExceeded)
        );
        assert_eq!(untouched_work.remaining(), 1);

        let malformed = [identity(0, 1, 0), identity(0, 1, 2)];
        let mut one_short_validation = GroupWorkBudget::new(1).expect("one work");
        assert_eq!(
            CanonicalGroupMerge::new(
                &schema,
                &malformed,
                bounds(1, 16, 64),
                &mut one_short_validation,
            )
            .err(),
            Some(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(one_short_validation.remaining(), 1);

        let inventory = [identity(0, 2, 0), identity(0, 2, 1)];
        let rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]];
        let mut mismatch_work = GroupWorkBudget::new(6).expect("bounded work");
        let mut mismatch =
            CanonicalGroupMerge::new(&schema, &inventory, bounds(1, 16, 64), &mut mismatch_work)
                .expect("merge");
        assert_eq!(mismatch_work.remaining(), 3);
        assert_eq!(
            mismatch.merge_leaf(leaf(inventory[1], &schema, &rows), &mut mismatch_work,),
            Err(GroupStateError::InventoryGap)
        );
        assert_eq!(mismatch_work.remaining(), 1);
        let remaining_after_scan = mismatch_work.remaining();
        assert_eq!(
            mismatch.merge_leaf(leaf(inventory[1], &schema, &rows), &mut mismatch_work,),
            Err(GroupStateError::Poisoned)
        );
        assert_eq!(mismatch_work.remaining(), remaining_after_scan);

        let key_len = oracle_key(&rows[0]).len();
        let exact_state =
            u32::try_from(size_of::<super::GroupEntry>() + 2 * key_len).expect("small state");
        let mut constructor_work = work();
        let mut builder = CanonicalGroupLeafBuilder::new(
            identity(0, 3, 0),
            &schema,
            bounds(1, key_len as u32, exact_state),
            &mut constructor_work,
        )
        .expect("builder");
        for row in 0..MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 {
            let mut exact_row_work =
                GroupWorkBudget::new(4).expect("preflight plus one bounded duplicate row");
            builder
                .push_row(row, &rows[0], &mut exact_row_work)
                .expect("exact row bound");
            assert_eq!(exact_row_work.remaining(), if row == 0 { 2 } else { 1 });
        }
        let mut one_more_work = GroupWorkBudget::new(3).expect("independent work");
        assert_eq!(
            builder.push_row(
                MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
                &rows[0],
                &mut one_more_work,
            ),
            Err(GroupStateError::RowBoundExceeded)
        );
        assert_eq!(one_more_work.remaining(), 3);
        assert!(builder.is_poisoned());
    }

    #[test]
    fn work_exhaustion_after_success_poisons_and_releases_partial_groups() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let first = [SegmentV2Cell::Value(CanonicalValue::U64(1))];
        let second = [SegmentV2Cell::Value(CanonicalValue::U64(2))];

        let mut builder_work = GroupWorkBudget::new(3).expect("seal plus one admitted row");
        let mut builder = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, 32, 256),
            &mut builder_work,
        )
        .expect("builder");
        builder
            .push_row(0, &first, &mut builder_work)
            .expect("first row");
        let work_before = builder_work;
        assert_eq!(
            builder.push_row(1, &second, &mut builder_work),
            Err(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(builder_work, work_before);
        assert!(builder.is_poisoned());
        assert!(builder.owner.entries.is_empty() && builder.owner.arena.is_empty());
        assert_eq!(builder.finish().err(), Some(GroupStateError::Poisoned));

        let inventory = [identity(0, 2, 0), identity(0, 2, 1)];
        let first_rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]];
        let second_rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(2))]];
        let mut merge_work = GroupWorkBudget::new(5).expect("inventory plus first merge");
        let mut merge =
            CanonicalGroupMerge::new(&schema, &inventory, bounds(2, 32, 256), &mut merge_work)
                .expect("merge");
        merge
            .merge_leaf(leaf(inventory[0], &schema, &first_rows), &mut merge_work)
            .expect("first leaf");
        let work_before = merge_work;
        assert_eq!(
            merge.merge_leaf(leaf(inventory[1], &schema, &second_rows), &mut merge_work),
            Err(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(merge_work, work_before);
        assert!(merge.poisoned);
        assert!(merge.owner.as_ref().expect("owner").entries.is_empty());
        assert_eq!(merge.finish().err(), Some(GroupStateError::Poisoned));
    }

    #[test]
    fn checked_program_lane_facts_and_lane_ceiling_are_closed() {
        let u64_type = SegmentV2LogicalType::U64;
        let string_type = SegmentV2LogicalType::String;
        let checked = schema(1, &[(u64_type.clone(), false), (string_type.clone(), true)]);
        assert_eq!(checked[0].facts.field().get(), 1);
        assert_eq!(checked[0].facts.logical(), &u64_type);
        assert!(!checked[0].facts.optional());
        assert_eq!(checked[1].facts.field().get(), 2);
        assert_eq!(checked[1].facts.logical(), &string_type);
        assert!(checked[1].facts.optional());

        let other_program = schema(9, &[(u64_type.clone(), false), (string_type.clone(), true)]);
        let mixed = [checked[0].clone(), other_program[1].clone()];
        let mut mixed_work = GroupWorkBudget::new(2).expect("two lane validations");
        assert_eq!(
            CanonicalGroupLeafBuilder::new(
                identity(0, 1, 0),
                &mixed,
                bounds(2, 64, 256),
                &mut mixed_work,
            )
            .err(),
            Some(GroupStateError::ProgramBindingSubstitution)
        );
        assert_eq!(mixed_work.remaining(), 0);

        let excessive = vec![checked[0].clone(); super::MAX_SEGMENT_V2_COLUMNS + 1];
        let mut untouched = GroupWorkBudget::new(1).expect("untouched work");
        assert_eq!(
            CanonicalGroupLeafBuilder::new(
                identity(0, 1, 0),
                &excessive,
                bounds(2, 64, 256),
                &mut untouched,
            )
            .err(),
            Some(GroupStateError::LaneBoundExceeded)
        );
        assert_eq!(untouched.remaining(), 1);
    }

    #[test]
    fn row_and_merge_pass_work_is_exact_with_plus_one_and_precedence() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = schema(1, &[(logical_type.clone(), false)]);
        let first = [SegmentV2Cell::Value(CanonicalValue::U64(1))];
        let second = [SegmentV2Cell::Value(CanonicalValue::U64(0))];

        let mut seal_work = GroupWorkBudget::new(1).expect("one sealed lane");
        let mut builder = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, 32, 256),
            &mut seal_work,
        )
        .expect("checked builder");
        assert_eq!(seal_work.remaining(), 0);

        let mut first_work = GroupWorkBudget::new(2).expect("validate and encode");
        builder
            .push_row(0, &first, &mut first_work)
            .expect("exact first row");
        assert_eq!(first_work.remaining(), 0);
        let mut second_work = GroupWorkBudget::new(4).expect("passes, lookup, and one entry shift");
        builder
            .push_row(1, &second, &mut second_work)
            .expect("exact second row");
        assert_eq!(second_work.remaining(), 0);

        let mut short_work = GroupWorkBudget::new(5).expect("one short of worst row work");
        assert_eq!(
            builder.push_row(2, &first, &mut short_work),
            Err(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(short_work.remaining(), 5);
        assert!(builder.is_poisoned());

        let inventory = [identity(0, 2, 0)];
        let rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]];
        let mut constructor_work = GroupWorkBudget::new(2).expect("lane and inventory scans");
        let mut merge = CanonicalGroupMerge::new(
            &schema,
            &inventory,
            bounds(2, 32, 256),
            &mut constructor_work,
        )
        .expect("checked merge");
        assert_eq!(constructor_work.remaining(), 0);
        let mut exact_merge_work = GroupWorkBudget::new(2).expect("lane and contribution");
        merge
            .merge_leaf(leaf(inventory[0], &schema, &rows), &mut exact_merge_work)
            .expect("exact merge pass");
        assert_eq!(exact_merge_work.remaining(), 0);

        let shifted_inventory = [identity(0, 4, 0), identity(0, 4, 1)];
        let high_rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(2))]];
        let low_rows = [vec![SegmentV2Cell::Value(CanonicalValue::U64(1))]];
        let mut shifted_constructor =
            GroupWorkBudget::new(3).expect("lane and two inventory scans");
        let mut shifted = CanonicalGroupMerge::new(
            &schema,
            &shifted_inventory,
            bounds(2, 32, 256),
            &mut shifted_constructor,
        )
        .expect("shifted merge");
        let mut first_contribution = GroupWorkBudget::new(2).expect("first contribution");
        shifted
            .merge_leaf(
                leaf(shifted_inventory[0], &schema, &high_rows),
                &mut first_contribution,
            )
            .expect("high key");
        let mut shifted_work = GroupWorkBudget::new(4).expect("lookup and one entry shift");
        shifted
            .merge_leaf(
                leaf(shifted_inventory[1], &schema, &low_rows),
                &mut shifted_work,
            )
            .expect("lower key shifts retained entry");
        assert_eq!(shifted_work.remaining(), 0);
        let shifted = shifted.finish().expect("canonical groups");
        assert_eq!(
            group_rows(&shifted),
            [
                (oracle_key(&low_rows[0]), 1),
                (oracle_key(&high_rows[0]), 1)
            ]
        );

        let mut precedence_work = GroupWorkBudget::new(1).expect("one-short row work");
        let mut precedence_seal = GroupWorkBudget::new(1).expect("sealed lane");
        let mut precedence = CanonicalGroupLeafBuilder::new(
            identity(0, 3, 0),
            &schema,
            bounds(1, 32, 256),
            &mut precedence_seal,
        )
        .expect("precedence builder");
        assert_eq!(
            precedence.push_row(
                9,
                &[SegmentV2Cell::Value(CanonicalValue::I64(1))],
                &mut precedence_work,
            ),
            Err(GroupStateError::RowOrder)
        );
        assert_eq!(precedence_work.remaining(), 1);
        assert!(precedence.is_poisoned());

        let mut type_precedence_work = GroupWorkBudget::new(1).expect("one-short row work");
        let mut type_precedence_seal = GroupWorkBudget::new(1).expect("sealed lane");
        let mut type_precedence = CanonicalGroupLeafBuilder::new(
            identity(0, 5, 0),
            &schema,
            bounds(1, 32, 256),
            &mut type_precedence_seal,
        )
        .expect("type precedence builder");
        assert_eq!(
            type_precedence.push_row(
                0,
                &[SegmentV2Cell::Value(CanonicalValue::I64(1))],
                &mut type_precedence_work,
            ),
            Err(GroupStateError::WorkBoundExceeded)
        );
        assert_eq!(type_precedence_work.remaining(), 1);
        assert!(type_precedence.is_poisoned());

        assert_eq!(
            super::row_work_ceiling(usize::MAX, 0),
            Err(GroupStateError::ArithmeticOverflow)
        );
        assert_eq!(
            super::merge_work_ceiling(usize::MAX, usize::MAX, 2),
            Err(GroupStateError::ArithmeticOverflow)
        );
    }

    // This is deliberately a bounded, group.rs-local structural proof. The
    // repository-wide allocation checker remains a separate integration seam.
    const MAX_GROUP_SOURCE_BYTES: usize = 256 * 1024;
    const MAX_GROUP_SOURCE_TOKENS: usize = 32 * 1024;
    const REVIEWED_GROUP_ITEM_TOKEN_HASHES: [u64; 6] = [
        361_605_024_910_353_321,
        18_325_372_949_028_513_203,
        5_304_916_325_019_661_598,
        14_332_923_306_409_527_796,
        4_583_225_306_792_871_116,
        14_286_021_644_196_959_110,
    ];
    const REVIEWED_PRODUCTION_PREFIX_BYTE_HASH: u64 = 6_761_605_506_267_740_943;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct RustToken<'source> {
        text: &'source str,
        start: usize,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ItemSpan {
        start: usize,
        end: usize,
        label: String,
    }

    fn raw_string_end(bytes: &[u8], start: usize) -> Result<Option<usize>, &'static str> {
        let mut marker = start;
        if bytes.get(marker) == Some(&b'b') && bytes.get(marker + 1) == Some(&b'r') {
            marker += 2;
        } else if bytes.get(marker) == Some(&b'r') {
            marker += 1;
        } else {
            return Ok(None);
        }
        let hash_start = marker;
        while bytes.get(marker) == Some(&b'#') {
            marker += 1;
        }
        let hash_count = marker - hash_start;
        if bytes.get(marker) != Some(&b'"') {
            return Ok(None);
        }
        marker += 1;
        while marker < bytes.len() {
            if bytes[marker] != b'"' {
                marker += 1;
                continue;
            }
            let closing_hashes = marker + 1..marker + 1 + hash_count;
            if closing_hashes.end <= bytes.len()
                && bytes[closing_hashes].iter().all(|byte| *byte == b'#')
            {
                return Ok(Some(marker + 1 + hash_count));
            }
            marker += 1;
        }
        Err("unterminated raw string literal")
    }

    fn lex_group_source(source: &str) -> Result<Vec<RustToken<'_>>, &'static str> {
        if source.len() > MAX_GROUP_SOURCE_BYTES {
            return Err("group source byte ceiling exceeded");
        }
        let bytes = source.as_bytes();
        let mut tokens = Vec::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            if bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
                continue;
            }
            if bytes[cursor..].starts_with(b"//") {
                cursor += 2;
                while cursor < bytes.len() && bytes[cursor] != b'\n' {
                    cursor += 1;
                }
                continue;
            }
            if bytes[cursor..].starts_with(b"/*") {
                cursor += 2;
                let mut depth = 1_u16;
                while cursor < bytes.len() && depth != 0 {
                    if bytes[cursor..].starts_with(b"/*") {
                        depth = depth.checked_add(1).ok_or("comment nesting overflow")?;
                        cursor += 2;
                    } else if bytes[cursor..].starts_with(b"*/") {
                        depth -= 1;
                        cursor += 2;
                    } else {
                        cursor += 1;
                    }
                }
                if depth != 0 {
                    return Err("unterminated block comment");
                }
                continue;
            }
            let start = cursor;
            if let Some(end) = raw_string_end(bytes, cursor)? {
                cursor = end;
            } else if bytes[cursor] == b'"' {
                cursor += 1;
                let mut escaped = false;
                while cursor < bytes.len() {
                    let byte = bytes[cursor];
                    cursor += 1;
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        break;
                    }
                }
                if bytes.get(cursor.wrapping_sub(1)) != Some(&b'"') {
                    return Err("unterminated string literal");
                }
            } else if bytes[cursor] == b'\''
                && ((bytes.get(cursor + 2) == Some(&b'\''))
                    || (bytes.get(cursor + 1) == Some(&b'\\')
                        && bytes.get(cursor + 3) == Some(&b'\'')))
            {
                cursor += if bytes.get(cursor + 1) == Some(&b'\\') {
                    4
                } else {
                    3
                };
            } else if bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_' {
                cursor += 1;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
                {
                    cursor += 1;
                }
            } else {
                cursor += source[cursor..]
                    .chars()
                    .next()
                    .ok_or("invalid UTF-8 cursor")?
                    .len_utf8();
            }
            tokens.push(RustToken {
                text: &source[start..cursor],
                start,
            });
            if tokens.len() > MAX_GROUP_SOURCE_TOKENS {
                return Err("group source token ceiling exceeded");
            }
        }
        Ok(tokens)
    }

    fn matching_brace(tokens: &[RustToken<'_>], opening: usize) -> Option<usize> {
        let mut depth = 0_usize;
        for (index, token) in tokens.iter().enumerate().skip(opening) {
            match token.text {
                "{" => depth = depth.checked_add(1)?,
                "}" => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn production_layout(source: &str) -> Result<(Vec<RustToken<'_>>, usize), &'static str> {
        let tokens = lex_group_source(source)?;
        let marker = ["#", "[", "cfg", "(", "test", ")", "]", "mod", "tests", "{"];
        let test_attribute = ["#", "[", "cfg", "(", "test", ")", "]"];
        let mut brace_depth = 0_usize;
        let mut test_item = None;
        for index in 0..tokens.len() {
            if brace_depth == 0
                && tokens[index..]
                    .iter()
                    .take(test_attribute.len())
                    .map(|token| token.text)
                    .eq(test_attribute)
            {
                if test_item.is_some() {
                    return Err("multiple top-level cfg(test) items");
                }
                if !tokens[index..]
                    .iter()
                    .take(marker.len())
                    .map(|token| token.text)
                    .eq(marker)
                {
                    return Err("top-level cfg(test) item is not the final tests module");
                }
                let opening = index + marker.len() - 1;
                let closing = matching_brace(&tokens, opening).ok_or("unclosed test module")?;
                test_item = Some((index, closing));
            }
            match tokens[index].text {
                "{" => brace_depth = brace_depth.checked_add(1).ok_or("brace depth overflow")?,
                "}" => {
                    brace_depth = brace_depth
                        .checked_sub(1)
                        .ok_or("unmatched closing brace")?
                }
                _ => {}
            }
        }
        if brace_depth != 0 {
            return Err("unclosed source brace");
        }
        let (module_start, module_end) = test_item.ok_or("missing final top-level test module")?;
        if module_end + 1 != tokens.len() {
            return Err("test module must be the final source item");
        }
        Ok((tokens[..module_start].to_vec(), tokens[module_start].start))
    }

    fn brace_depths(tokens: &[RustToken<'_>]) -> Result<Vec<usize>, &'static str> {
        let mut depths = Vec::with_capacity(tokens.len());
        let mut depth = 0_usize;
        for token in tokens {
            depths.push(depth);
            match token.text {
                "{" => depth = depth.checked_add(1).ok_or("brace depth overflow")?,
                "}" => depth = depth.checked_sub(1).ok_or("unmatched closing brace")?,
                _ => {}
            }
        }
        if depth != 0 {
            return Err("unclosed production brace");
        }
        Ok(depths)
    }

    fn function_span(
        tokens: &[RustToken<'_>],
        depths: &[usize],
        start: usize,
    ) -> Result<usize, &'static str> {
        let declaration_depth = depths[start];
        for index in start + 1..tokens.len() {
            if depths[index] == declaration_depth && tokens[index].text == ";" {
                return Ok(index);
            }
            if depths[index] == declaration_depth && tokens[index].text == "{" {
                return matching_brace(tokens, index).ok_or("unclosed function item");
            }
        }
        Err("unterminated function item")
    }

    fn production_items(tokens: &[RustToken<'_>]) -> Result<Vec<ItemSpan>, &'static str> {
        let depths = brace_depths(tokens)?;
        let mut items = Vec::new();
        for index in 0..tokens.len() {
            if depths[index] != 0 {
                continue;
            }
            match tokens[index].text {
                "use" => {
                    let end = (index + 1..tokens.len())
                        .find(|candidate| depths[*candidate] == 0 && tokens[*candidate].text == ";")
                        .ok_or("unterminated use item")?;
                    items.push(ItemSpan {
                        start: index,
                        end,
                        label: "use".to_owned(),
                    });
                }
                "struct" => {
                    let name = tokens.get(index + 1).ok_or("missing struct name")?.text;
                    let opening = (index + 1..tokens.len())
                        .find(|candidate| depths[*candidate] == 0 && tokens[*candidate].text == "{")
                        .ok_or("missing struct body")?;
                    let end = matching_brace(tokens, opening).ok_or("unclosed struct body")?;
                    items.push(ItemSpan {
                        start: index,
                        end,
                        label: format!("struct {name}"),
                    });
                }
                "fn" => {
                    let name = tokens.get(index + 1).ok_or("missing function name")?.text;
                    items.push(ItemSpan {
                        start: index,
                        end: function_span(tokens, &depths, index)?,
                        label: name.to_owned(),
                    });
                }
                "impl" => {
                    let opening = (index + 1..tokens.len())
                        .find(|candidate| depths[*candidate] == 0 && tokens[*candidate].text == "{")
                        .ok_or("missing impl body")?;
                    let mut type_index = index + 1;
                    if tokens
                        .get(type_index)
                        .is_some_and(|token| token.text == "<")
                    {
                        let mut angle_depth = 0_usize;
                        loop {
                            match tokens.get(type_index).ok_or("unclosed impl generics")?.text {
                                "<" => angle_depth += 1,
                                ">" => {
                                    angle_depth -= 1;
                                    if angle_depth == 0 {
                                        type_index += 1;
                                        break;
                                    }
                                }
                                _ => {}
                            }
                            type_index += 1;
                        }
                    }
                    let impl_type = tokens.get(type_index).ok_or("missing impl type")?.text;
                    let closing = matching_brace(tokens, opening).ok_or("unclosed impl body")?;
                    items.push(ItemSpan {
                        start: index,
                        end: closing,
                        label: format!("impl {impl_type}"),
                    });
                    for method in opening + 1..closing {
                        if depths[method] == 1 && tokens[method].text == "fn" {
                            let name = tokens.get(method + 1).ok_or("missing method name")?.text;
                            items.push(ItemSpan {
                                start: method,
                                end: function_span(tokens, &depths, method)?,
                                label: format!("{impl_type}::{name}"),
                            });
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(items)
    }

    fn item_span_for_token(items: &[ItemSpan], index: usize) -> Option<&ItemSpan> {
        items
            .iter()
            .filter(|item| item.start <= index && index <= item.end)
            .min_by_key(|item| item.end - item.start)
    }

    fn fnv1a(bytes: impl IntoIterator<Item = u8>) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    fn token_hash(tokens: &[RustToken<'_>], item: &ItemSpan) -> u64 {
        fnv1a(
            tokens[item.start..=item.end]
                .iter()
                .flat_map(|token| token.text.bytes().chain([0xff])),
        )
    }

    fn reviewed_item_hashes(
        tokens: &[RustToken<'_>],
        items: &[ItemSpan],
    ) -> Result<[u64; 6], &'static str> {
        let labels = [
            "struct BoundedGroupOwner",
            "BoundedGroupOwner::new",
            "encoded_key_len",
            "append_key",
            "BoundedGroupOwner::admit_encoded",
            "BoundedGroupOwner::commit_appended",
        ];
        let mut hashes = [0_u64; 6];
        for (index, label) in labels.iter().enumerate() {
            let mut matches = items.iter().filter(|item| item.label == *label);
            let item = matches.next().ok_or("missing reviewed production item")?;
            if matches.next().is_some() {
                return Err("duplicate reviewed production item");
            }
            hashes[index] = token_hash(tokens, item);
        }
        Ok(hashes)
    }

    fn token_sequence(tokens: &[RustToken<'_>], index: usize, expected: &[&str]) -> bool {
        tokens[index..]
            .iter()
            .take(expected.len())
            .map(|token| token.text)
            .eq(expected.iter().copied())
    }

    fn unique_sequence(
        tokens: &[RustToken<'_>],
        item: &ItemSpan,
        expected: &[&str],
    ) -> Result<usize, &'static str> {
        let mut positions =
            (item.start..=item.end).filter(|index| token_sequence(tokens, *index, expected));
        let position = positions.next().ok_or("missing reviewed token sequence")?;
        if positions.next().is_some() {
            return Err("duplicate reviewed token sequence");
        }
        Ok(position)
    }

    fn guard_dominates(
        tokens: &[RustToken<'_>],
        item: &ItemSpan,
        guard: &[&str],
        mutation: &[&str],
        refusal: &str,
    ) -> Result<(), &'static str> {
        let guard = unique_sequence(tokens, item, guard)?;
        let mutation = unique_sequence(tokens, item, mutation)?;
        if guard >= mutation {
            return Err("guard does not precede mutation");
        }
        let guard_if = (item.start..guard)
            .rev()
            .find(|index| tokens[*index].text == "if")
            .ok_or("guard is not controlled by if")?;
        let opening = (guard..mutation)
            .find(|index| tokens[*index].text == "{")
            .ok_or("guard refusal body missing")?;
        let closing = matching_brace(tokens, opening).ok_or("guard refusal body unclosed")?;
        if guard_if >= opening || closing >= mutation {
            return Err("guard refusal does not dominate mutation");
        }
        let body = &tokens[opening + 1..closing];
        if !body.iter().any(|token| token.text == "return")
            || !body.iter().any(|token| token.text == refusal)
        {
            return Err("guard refusal is not fail closed");
        }
        Ok(())
    }

    fn check_group_allocation_architecture(source: &str) -> Result<(), &'static str> {
        let (tokens, module_start) = production_layout(source)?;
        let items = production_items(&tokens)?;
        let mut owner_vec_fields = 0_usize;
        let mut owner_vec_news = 0_usize;
        let mut encoder_vec_parameter = 0_usize;
        let mut reserve_sites = 0_usize;
        let mut length_helper_imports = 0_usize;
        let mut length_helper_calls = 0_usize;
        let mut encoder_helper_imports = 0_usize;
        let mut encoder_helper_calls = 0_usize;
        let mut encoded_extend_sites = 0_usize;
        let mut key_extend_sites = 0_usize;
        let mut entry_insert_sites = 0_usize;

        for (index, token) in tokens.iter().enumerate() {
            let item_span = item_span_for_token(&items, index);
            let item = item_span.map_or("<unparsed top-level item>", |item| item.label.as_str());
            let identifier = token
                .text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
            if matches!(token.text, "Box" | "String")
                || token.text.starts_with("Vec") && token.text != "Vec"
            {
                return Err("unreviewed owned allocation type");
            }
            if matches!(token.text, "boxed" | "string")
                || token.text == "vec"
                    && !tokens.get(index + 1).is_some_and(|next| next.text == "!")
            {
                return Err("unreviewed allocation module or alias");
            }
            if token.text == "Vec" {
                if item == "struct BoundedGroupOwner"
                    && token_sequence(&tokens, index, &["Vec", "<"])
                {
                    owner_vec_fields += 1;
                } else if item == "BoundedGroupOwner::new"
                    && token_sequence(&tokens, index, &["Vec", ":", ":", "new", "("])
                {
                    owner_vec_news += 1;
                } else if item == "append_key" && token_sequence(&tokens, index, &["Vec", "<"]) {
                    encoder_vec_parameter += 1;
                } else {
                    return Err("unreviewed Vec site or alias");
                }
            }
            if matches!(token.text, "vec" | "format" | "box")
                && tokens.get(index + 1).is_some_and(|next| next.text == "!")
            {
                return Err("unreviewed allocating macro");
            }
            if identifier
                && (["collect", "to_vec", "to_owned", "clone", "from_raw_parts"]
                    .iter()
                    .any(|needle| token.text.contains(needle))
                    || token.text.contains("from_iter")
                    || token.text.contains("reserve") && token.text != "try_reserve_exact")
            {
                return Err("unreviewed allocating call");
            }
            if item == "use" && identifier && token.text.contains("alloc") {
                return Err("aliased allocation module import");
            }
            if identifier
                && (token.text.contains("alloc") || token.text == "encode_canonical_value")
                && tokens
                    .get(index.wrapping_sub(1))
                    .is_none_or(|previous| previous.text != "fn")
                && tokens.get(index + 1).is_some_and(|next| next.text == "(")
            {
                return Err("unreviewed allocation helper");
            }
            if token.text == "try_reserve_exact" {
                if item != "BoundedGroupOwner::new"
                    || !tokens.get(index + 1).is_some_and(|next| next.text == "(")
                {
                    return Err("unreviewed reserve site or alias");
                }
                reserve_sites += 1;
            }
            if token.text == "extend_from_slice" {
                if item == "BoundedGroupOwner::admit_encoded"
                    && index >= 4
                    && token_sequence(
                        &tokens,
                        index - 4,
                        &[
                            "self",
                            ".",
                            "arena",
                            ".",
                            "extend_from_slice",
                            "(",
                            "key",
                            ")",
                        ],
                    )
                {
                    encoded_extend_sites += 1;
                } else if item == "append_key"
                    && index >= 2
                    && token_sequence(
                        &tokens,
                        index - 2,
                        &["arena", ".", "extend_from_slice", "("],
                    )
                {
                    key_extend_sites += 1;
                } else {
                    return Err("unreviewed extend route or site swap");
                }
            }
            if token.text == "insert" {
                if item == "BoundedGroupOwner::commit_appended"
                    && index >= 4
                    && token_sequence(
                        &tokens,
                        index - 4,
                        &["self", ".", "entries", ".", "insert", "("],
                    )
                {
                    entry_insert_sites += 1;
                } else {
                    return Err("unreviewed insert route or site swap");
                }
            }
            for (helper, import_count, call_count, expected_item) in [
                (
                    "canonical_value_encoded_len",
                    &mut length_helper_imports,
                    &mut length_helper_calls,
                    ["encoded_key_len", "append_key"].as_slice(),
                ),
                (
                    "encode_canonical_value_into",
                    &mut encoder_helper_imports,
                    &mut encoder_helper_calls,
                    ["append_key"].as_slice(),
                ),
            ] {
                if token.text != helper {
                    continue;
                }
                if item == "use" {
                    let item_span = item_span.ok_or("missing helper import item")?;
                    if tokens[item_span.start..=item_span.end]
                        .iter()
                        .any(|candidate| candidate.text == "as")
                    {
                        return Err("aliased allocation helper import");
                    }
                    *import_count += 1;
                } else if expected_item.contains(&item)
                    && tokens.get(index + 1).is_some_and(|next| next.text == "(")
                {
                    *call_count += 1;
                } else {
                    return Err("unreviewed allocation helper site or alias");
                }
            }
            if matches!(token.text, "sort" | "sort_by" | "sort_unstable")
                && tokens.get(index + 1).is_some_and(|next| next.text == "(")
            {
                return Err("uncharged final sort");
            }
        }
        if (
            owner_vec_fields,
            owner_vec_news,
            encoder_vec_parameter,
            reserve_sites,
        ) != (2, 2, 1, 2)
        {
            return Err("owned allocation site allowlist changed");
        }
        if (length_helper_imports, length_helper_calls) != (1, 2)
            || (encoder_helper_imports, encoder_helper_calls) != (1, 1)
        {
            return Err("canonical encoder helper allowlist changed");
        }
        if (encoded_extend_sites, key_extend_sites, entry_insert_sites) != (1, 2, 1) {
            return Err("extend or insert site allowlist changed");
        }
        let admit_encoded = items
            .iter()
            .find(|item| item.label == "BoundedGroupOwner::admit_encoded")
            .ok_or("missing admit_encoded item")?;
        guard_dominates(
            &tokens,
            admit_encoded,
            &["self", ".", "arena", ".", "capacity", "(", ")"],
            &[
                "self",
                ".",
                "arena",
                ".",
                "extend_from_slice",
                "(",
                "key",
                ")",
            ],
            "StateBoundExceeded",
        )?;
        let commit_appended = items
            .iter()
            .find(|item| item.label == "BoundedGroupOwner::commit_appended")
            .ok_or("missing commit_appended item")?;
        let entry_insert = &["self", ".", "entries", ".", "insert", "("];
        guard_dominates(
            &tokens,
            commit_appended,
            &[
                "self",
                ".",
                "entries",
                ".",
                "len",
                "(",
                ")",
                ">",
                "=",
                "usize",
                ":",
                ":",
                "from",
                "(",
                "self",
                ".",
                "bounds",
                ".",
                "maximum_groups",
                ")",
            ],
            entry_insert,
            "GroupBoundExceeded",
        )?;
        guard_dominates(
            &tokens,
            commit_appended,
            &["end", ">", "self", ".", "retained_key_capacity"],
            entry_insert,
            "StateBoundExceeded",
        )?;
        if reviewed_item_hashes(&tokens, &items)? != REVIEWED_GROUP_ITEM_TOKEN_HASHES {
            return Err("reviewed production item token hash changed");
        }
        if fnv1a(source[..module_start].bytes()) != REVIEWED_PRODUCTION_PREFIX_BYTE_HASH {
            return Err("reviewed production prefix byte hash changed");
        }
        Ok(())
    }

    #[test]
    fn production_group_owner_has_no_per_row_owned_key_or_map() {
        let source = include_str!("group.rs");
        let (tokens, module_start) = production_layout(source).expect("bounded structural source");
        let items = production_items(&tokens).expect("production items");
        assert_eq!(
            reviewed_item_hashes(&tokens, &items),
            Ok(REVIEWED_GROUP_ITEM_TOKEN_HASHES)
        );
        assert_eq!(
            fnv1a(source[..module_start].bytes()),
            REVIEWED_PRODUCTION_PREFIX_BYTE_HASH
        );
        assert_eq!(check_group_allocation_architecture(source), Ok(()));

        for (mutation, expected) in [
            (
                "fn bypass(){let _=Box/* hidden */::new(1);}",
                "unreviewed owned allocation type",
            ),
            (
                "use std::vec::Vec as HiddenVec; fn bypass(){let _:HiddenVec<u8>=HiddenVec::new();}",
                "unreviewed allocation module or alias",
            ),
            (
                "fn bypass(){let _=vec/* hidden */![1];}",
                "unreviewed allocating macro",
            ),
            (
                "fn bypass(){let _:Vec<u8>=Vec::new();}",
                "unreviewed Vec site or alias",
            ),
            (
                "fn bypass(v:&[u8]){let _=v.to_owned();}",
                "unreviewed allocating call",
            ),
            (
                "fn bypass(v:&[u8]){let _=v.iter().collect::<Vec<_>>();}",
                "unreviewed allocating call",
            ),
            (
                "fn bypass<I:Iterator>(i:I){let _=Iterator::collect(i);}",
                "unreviewed allocating call",
            ),
            (
                "fn bypass<I:IntoIterator>(i:I){let _=FromIterator::from_iter(i);}",
                "unreviewed allocating call",
            ),
            (
                "fn bypass(v:&CanonicalValue){let _=encode_canonical_value(v);}",
                "unreviewed allocation helper",
            ),
            (
                "fn bypass(){allocation_helper();}",
                "unreviewed allocation helper",
            ),
        ] {
            let (_, module_start) = production_layout(source).expect("source layout");
            let mutated = format!(
                "{}\n{mutation}\n{}",
                &source[..module_start],
                &source[module_start..]
            );
            assert_eq!(check_group_allocation_architecture(&mutated), Err(expected));
        }

        let suffix = format!("{source}\nfn hidden_suffix() {{ Vec::new(); }}");
        assert_eq!(
            check_group_allocation_architecture(&suffix),
            Err("test module must be the final source item")
        );

        let (_, module_start) = production_layout(source).expect("source layout");
        let extra_test_item = format!(
            "{}\n#[cfg(test)] fn hidden_test_item() {{}}\n{}",
            &source[..module_start],
            &source[module_start..]
        );
        assert_eq!(
            check_group_allocation_architecture(&extra_test_item),
            Err("top-level cfg(test) item is not the final tests module")
        );

        let raw_decoys = format!(
            "{}\nconst RAW_DECOY:&str=r####\"}} #[cfg(test)] mod tests {{ Vec::new(); \\\"quoted\\\" \"####;\nconst BYTE_RAW_DECOY:&[u8]=br######\"}} Iterator::collect {{\"######;\n{}",
            &source[..module_start],
            &source[module_start..]
        );
        let (raw_tokens, _) =
            production_layout(&raw_decoys).expect("raw-string-aware tokenization");
        assert_eq!(
            raw_tokens
                .iter()
                .filter(|token| token.text == "Vec")
                .count(),
            tokens.iter().filter(|token| token.text == "Vec").count(),
            "raw-string decoy leaked a Vec token"
        );
        assert!(!raw_tokens.iter().any(|token| token.text == "collect"));
        assert_eq!(
            check_group_allocation_architecture(&raw_decoys),
            Err("reviewed production prefix byte hash changed")
        );

        let external_helper = format!(
            "{}\nfn bypass(){{external::make();}}\n{}",
            &source[..module_start],
            &source[module_start..]
        );
        assert_eq!(
            check_group_allocation_architecture(&external_helper),
            Err("reviewed production prefix byte hash changed")
        );

        let without_arena_capacity = source.replacen(
            "if start\n            .checked_add(key.len())\n            .is_none_or(|end| end > self.arena.capacity())",
            "if false",
            1,
        );
        assert_ne!(without_arena_capacity, source);
        assert_eq!(
            check_group_allocation_architecture(&without_arena_capacity),
            Err("missing reviewed token sequence")
        );
        let without_group_capacity = source.replacen(
            "if self.entries.len() >= usize::from(self.bounds.maximum_groups) {",
            "if false {",
            1,
        );
        assert_ne!(without_group_capacity, source);
        assert_eq!(
            check_group_allocation_architecture(&without_group_capacity),
            Err("missing reviewed token sequence")
        );
        let without_state_capacity =
            source.replacen("if end > self.retained_key_capacity {", "if false {", 1);
        assert_ne!(without_state_capacity, source);
        assert_eq!(
            check_group_allocation_architecture(&without_state_capacity),
            Err("missing reviewed token sequence")
        );

        let swapped_extend = source.replacen(
            "self.arena.extend_from_slice(key);",
            "self.entries.extend_from_slice(key);",
            1,
        );
        assert_ne!(swapped_extend, source);
        assert_eq!(
            check_group_allocation_architecture(&swapped_extend),
            Err("unreviewed extend route or site swap")
        );
        let swapped_insert = source.replacen("self.entries.insert(", "self.arena.insert(", 1);
        assert_ne!(swapped_insert, source);
        assert_eq!(
            check_group_allocation_architecture(&swapped_insert),
            Err("unreviewed insert route or site swap")
        );

        let swapped = source.replacen(
            "let mut entries = Vec::new();",
            "let mut arena = Vec::new();",
            1,
        );
        assert_eq!(
            check_group_allocation_architecture(&swapped),
            Err("reviewed production item token hash changed")
        );
    }

    #[test]
    fn empty_inventory_and_sealed_optionality_have_explicit_bounded_contracts() {
        let logical_type = SegmentV2LogicalType::U64;
        let required = schema(1, &[(logical_type.clone(), false)]);
        let optional = schema(1, &[(logical_type.clone(), true)]);
        let mut zero_work = super::GroupWorkBudget::new(1).expect("work ceiling");
        let empty = CanonicalGroupMerge::new(&required, &[], bounds(1, 16, 64), &mut zero_work)
            .expect("exact empty inventory");
        let empty = empty.finish().expect("empty groups");
        assert_eq!(group_rows(&empty), []);
        assert!(empty.owner.is_none());
        assert_eq!(zero_work.remaining(), 1);

        let mut work = super::GroupWorkBudget::new(3).expect("seal and row-pass ceiling");
        let mut required_leaf = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &required,
            bounds(1, 16, 64),
            &mut work,
        )
        .expect("required lane");
        assert_eq!(
            required_leaf.push_row(0, &[SegmentV2Cell::Null], &mut work),
            Err(GroupStateError::UnexpectedNoValue)
        );
        assert_eq!(work.remaining(), 1);
        assert!(required_leaf.owner.entries.is_empty() && required_leaf.owner.arena.is_empty());

        let mut missing_work = super::GroupWorkBudget::new(3).expect("seal and row-pass ceiling");
        let mut required_missing = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &required,
            bounds(1, 16, 64),
            &mut missing_work,
        )
        .expect("required lane");
        assert_eq!(
            required_missing.push_row(0, &[SegmentV2Cell::Missing], &mut missing_work),
            Err(GroupStateError::UnexpectedNoValue)
        );
        assert!(
            required_missing.owner.entries.is_empty() && required_missing.owner.arena.is_empty()
        );

        let mut optional_work =
            super::GroupWorkBudget::new(7).expect("seal and two row preflights");
        let mut optional_leaf = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &optional,
            bounds(2, 16, 128),
            &mut optional_work,
        )
        .expect("optional lane");
        optional_leaf
            .push_row(0, &[SegmentV2Cell::Missing], &mut optional_work)
            .expect("optional missing");
        optional_leaf
            .push_row(1, &[SegmentV2Cell::Null], &mut optional_work)
            .expect("optional null");
        assert_eq!(
            optional_leaf
                .finish()
                .expect("NoValue leaf")
                .row_count_for_test(0),
            Some(2)
        );
    }
}
