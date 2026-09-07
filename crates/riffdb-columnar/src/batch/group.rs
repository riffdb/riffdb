//! Private, inert canonical group-state mechanics.

use std::cmp::Ordering;
use std::mem::size_of;

use riffdb_types::{
    MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_DISTINCT_VALUES_V1,
    MAX_AGGREGATE_STATE_BYTES_V1, canonical_value_encoded_len, encode_canonical_value_into,
};

use super::aggregate::CanonicalPartialIdentity;
use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType};

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

    #[cfg(test)]
    const fn remaining(self) -> u32 {
        self.remaining
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CanonicalGroupLane<'schema> {
    logical_type: &'schema SegmentV2LogicalType,
    optional: bool,
}

impl<'schema> CanonicalGroupLane<'schema> {
    pub(super) const fn new(logical_type: &'schema SegmentV2LogicalType, optional: bool) -> Self {
        Self {
            logical_type,
            optional,
        }
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
        lanes: &[CanonicalGroupLane<'_>],
        cells: &[SegmentV2Cell],
        count: u64,
    ) -> Result<(), GroupStateError> {
        self.ensure_live()?;
        let key_len = match encoded_key_len(lanes, cells) {
            Ok(key_len) => key_len,
            Err(error) => return Err(self.poison(error)),
        };
        if key_len > self.bounds.maximum_key_bytes as usize {
            return Err(self.poison(GroupStateError::KeyBoundExceeded));
        }
        let start = self.arena.len();
        if start
            .checked_add(key_len)
            .is_none_or(|end| end > self.arena.capacity())
        {
            return Err(self.poison(GroupStateError::StateBoundExceeded));
        }
        if let Err(error) = append_key(&mut self.arena, cells) {
            self.arena.truncate(start);
            return Err(self.poison(error));
        }
        self.admit_appended(start, key_len, count)
    }

    fn admit_encoded(&mut self, key: &[u8], count: u64) -> Result<(), GroupStateError> {
        self.ensure_live()?;
        if count == 0 {
            return Err(self.poison(GroupStateError::IncompatibleLeaf));
        }
        if key.len() > self.bounds.maximum_key_bytes as usize {
            return Err(self.poison(GroupStateError::KeyBoundExceeded));
        }
        let start = self.arena.len();
        if start
            .checked_add(key.len())
            .is_none_or(|end| end > self.arena.capacity())
        {
            return Err(self.poison(GroupStateError::StateBoundExceeded));
        }
        self.arena.extend_from_slice(key);
        self.admit_appended(start, key.len(), count)
    }

    fn admit_appended(
        &mut self,
        start: usize,
        key_len: usize,
        count: u64,
    ) -> Result<(), GroupStateError> {
        let end = start + key_len;
        if let Some(index) = self.entries.iter().position(|entry| {
            let prior_start = entry.key_start as usize;
            let prior_end = prior_start + entry.key_len as usize;
            self.arena[prior_start..prior_end] == self.arena[start..end]
        }) {
            self.arena.truncate(start);
            let next = match self.entries[index].row_count.checked_add(count) {
                Some(next) => next,
                None => return Err(self.poison(GroupStateError::ArithmeticOverflow)),
            };
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
        self.entries.push(GroupEntry {
            key_start,
            key_len,
            row_count: count,
        });
        Ok(())
    }

    fn sort_canonical(&mut self) {
        let arena = &self.arena;
        self.entries
            .sort_unstable_by(|left, right| entry_key(arena, left).cmp(entry_key(arena, right)));
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

fn entry_key<'arena>(arena: &'arena [u8], entry: &GroupEntry) -> &'arena [u8] {
    let start = entry.key_start as usize;
    &arena[start..start + entry.key_len as usize]
}

fn encoded_key_len(
    lanes: &[CanonicalGroupLane<'_>],
    cells: &[SegmentV2Cell],
) -> Result<usize, GroupStateError> {
    if lanes.is_empty() || lanes.len() != cells.len() {
        return Err(GroupStateError::InputShape);
    }
    lanes
        .iter()
        .zip(cells)
        .try_fold(0_usize, |total, (lane, cell)| {
            let value_len = match cell {
                SegmentV2Cell::Missing | SegmentV2Cell::Null if lane.optional => {
                    NO_VALUE_BYTES.len()
                }
                SegmentV2Cell::Missing | SegmentV2Cell::Null => {
                    return Err(GroupStateError::UnexpectedNoValue);
                }
                SegmentV2Cell::Value(value) => {
                    if !lane.logical_type.accepts(value) {
                        return Err(GroupStateError::TypeMismatch);
                    }
                    canonical_value_encoded_len(value).map_err(|_| GroupStateError::InputShape)?
                }
            };
            let framed = 4_usize
                .checked_add(value_len)
                .ok_or(GroupStateError::ArithmeticOverflow)?;
            total
                .checked_add(framed)
                .ok_or(GroupStateError::ArithmeticOverflow)
        })
}

fn append_key(arena: &mut Vec<u8>, cells: &[SegmentV2Cell]) -> Result<(), GroupStateError> {
    for cell in cells {
        let value_len = match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null => NO_VALUE_BYTES.len(),
            SegmentV2Cell::Value(value) => {
                canonical_value_encoded_len(value).map_err(|_| GroupStateError::InputShape)?
            }
        };
        let value_len = u32::try_from(value_len).map_err(|_| GroupStateError::KeyBoundExceeded)?;
        arena.extend_from_slice(&value_len.to_be_bytes());
        match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null => {
                arena.extend_from_slice(&NO_VALUE_BYTES)
            }
            SegmentV2Cell::Value(value) => encode_canonical_value_into(arena, value)
                .map_err(|_| GroupStateError::InputShape)?,
        }
    }
    Ok(())
}

pub(super) struct CanonicalGroupLeafBuilder<'schema> {
    identity: CanonicalPartialIdentity,
    lanes: &'schema [CanonicalGroupLane<'schema>],
    owner: BoundedGroupOwner,
    next_row_ordinal: u32,
}

impl<'schema> CanonicalGroupLeafBuilder<'schema> {
    pub(super) fn new(
        identity: CanonicalPartialIdentity,
        lanes: &'schema [CanonicalGroupLane<'schema>],
        bounds: CanonicalGroupBounds,
    ) -> Result<Self, GroupStateError> {
        if lanes.is_empty() {
            return Err(GroupStateError::InputShape);
        }
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
        work.charge(1)?;
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
        self.owner.admit_cells(self.lanes, cells, 1)?;
        self.next_row_ordinal = next;
        Ok(())
    }

    pub(super) const fn is_poisoned(&self) -> bool {
        self.owner.poisoned
    }

    pub(super) fn finish(mut self) -> Result<FinalizedGroupLeaf<'schema>, GroupStateError> {
        self.owner.ensure_live()?;
        self.owner.sort_canonical();
        Ok(FinalizedGroupLeaf {
            identity: self.identity,
            lanes: self.lanes,
            owner: self.owner,
        })
    }
}

pub(super) struct FinalizedGroupLeaf<'schema> {
    identity: CanonicalPartialIdentity,
    lanes: &'schema [CanonicalGroupLane<'schema>],
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
    lanes: &'schema [CanonicalGroupLane<'schema>],
    inventory: &'inventory [CanonicalPartialIdentity],
    cursor: usize,
    owner: Option<BoundedGroupOwner>,
    poisoned: bool,
}

impl<'schema, 'inventory> CanonicalGroupMerge<'schema, 'inventory> {
    pub(super) fn new(
        lanes: &'schema [CanonicalGroupLane<'schema>],
        inventory: &'inventory [CanonicalPartialIdentity],
        bounds: CanonicalGroupBounds,
        work: &mut GroupWorkBudget,
    ) -> Result<Self, GroupStateError> {
        if lanes.is_empty() {
            return Err(GroupStateError::InputShape);
        }
        validate_inventory(inventory, work)?;
        let owner = if inventory.is_empty() {
            None
        } else {
            Some(BoundedGroupOwner::new(bounds)?)
        };
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
            work.charge(self.inventory.len().max(1))?;
            let error = classify_mismatch(self.inventory, self.cursor, leaf.identity);
            self.poison();
            return Err(error);
        }
        let merge_work = self
            .lanes
            .len()
            .checked_add(leaf.owner.entries.len())
            .ok_or(GroupStateError::WorkBoundExceeded)?
            .max(1);
        work.charge(merge_work)?;
        if self.lanes != leaf.lanes {
            self.poison();
            return Err(GroupStateError::IncompatibleLeaf);
        }
        let owner = self.owner.as_mut().ok_or(GroupStateError::ExcessLeaf)?;
        for index in 0..leaf.owner.entries.len() {
            let key = leaf
                .owner
                .key(index)
                .ok_or(GroupStateError::IncompatibleLeaf)?;
            let count = leaf
                .owner
                .count(index)
                .ok_or(GroupStateError::IncompatibleLeaf)?;
            if let Err(error) = owner.admit_encoded(key, count) {
                self.poisoned = true;
                return Err(error);
            }
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
            owner.sort_canonical();
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
    use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};
    use riffdb_types::{
        CanonicalValue, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId, Money,
        Timestamp, encode_canonical_value,
    };
    use std::collections::BTreeMap;
    use std::mem::size_of;

    fn identity(root: u16, segment: u8, batch: u32) -> CanonicalPartialIdentity {
        CanonicalPartialIdentity::new(root, SegmentV2SegmentId::from_bytes([segment; 16]), batch)
    }
    fn bounds(groups: u16, key: u32, state: u32) -> CanonicalGroupBounds {
        CanonicalGroupBounds::new(groups, key, state).expect("valid bounds")
    }
    fn required(logical_type: &SegmentV2LogicalType) -> CanonicalGroupLane<'_> {
        CanonicalGroupLane::new(logical_type, false)
    }
    fn optional(logical_type: &SegmentV2LogicalType) -> CanonicalGroupLane<'_> {
        CanonicalGroupLane::new(logical_type, true)
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
        schema: &'schema [CanonicalGroupLane<'schema>],
        rows: &[Vec<SegmentV2Cell>],
    ) -> super::FinalizedGroupLeaf<'schema> {
        let mut builder =
            CanonicalGroupLeafBuilder::new(id, schema, bounds(16, 256, 4_096)).expect("builder");
        let mut work = work();
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
        let schema = [optional(&logical_type)];
        let rows = [
            vec![SegmentV2Cell::Missing],
            vec![SegmentV2Cell::Null],
            vec![SegmentV2Cell::Value(CanonicalValue::U64(0))],
        ];
        let groups = leaf(identity(0, 7, 0), &schema, &rows);
        assert_eq!(groups.group_count(), 2);
        assert_eq!(groups.row_count_for_test(0), Some(2));
        assert_eq!(groups.row_count_for_test(1), Some(1));
        let mut wrong_type =
            CanonicalGroupLeafBuilder::new(identity(0, 7, 0), &schema, bounds(3, 64, 256))
                .expect("owner");
        let mut work = work();
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
        let schema = [required(&logical_type)];
        let row = [SegmentV2Cell::Value(CanonicalValue::U64(7))];
        let exact = u32::try_from(oracle_key(&row).len()).expect("length");
        let mut accepted =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &schema, bounds(1, exact, 256))
                .expect("owner");
        let mut accepted_work = work();
        accepted
            .push_row(0, &row, &mut accepted_work)
            .expect("exact");
        assert_eq!(accepted.finish().expect("leaf").group_count(), 1);
        let mut rejected =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &schema, bounds(1, exact - 1, 256))
                .expect("owner");
        let mut rejected_work = work();
        assert_eq!(
            rejected.push_row(0, &row, &mut rejected_work),
            Err(GroupStateError::KeyBoundExceeded)
        );
        assert!(rejected.is_poisoned());
    }

    #[test]
    fn independent_group_count_and_state_bounds_are_exact_and_plus_one_refuses() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = [required(&logical_type)];
        let encoded_key_bytes = oracle_key(&[SegmentV2Cell::Value(CanonicalValue::U64(1))]);
        let key_bound = u32::try_from(encoded_key_bytes.len()).expect("small key");
        let group_state =
            u32::try_from(2 * size_of::<super::GroupEntry>() + 3 * encoded_key_bytes.len())
                .expect("small state");
        let mut owner = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, key_bound, group_state),
        )
        .expect("owner");
        let mut owner_work = work();
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
        let mut exact_state_owner = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact_key_bytes.len() as u32, exact_state),
        )
        .expect("exact state owner");
        let mut exact_work = work();
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
        let mut plus_one_state = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(1, exact_key_bytes.len() as u32, exact_state - 1),
        )
        .expect("one-byte-short owner");
        let mut short_work = work();
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
        assert_eq!(
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &schema, bounds(2, 1, 31)).err(),
            Some(GroupStateError::InvalidStateBound)
        );
    }

    #[test]
    fn canonical_framing_prevents_component_collisions_and_row_order_is_sealed() {
        let string_type = SegmentV2LogicalType::String;
        let schema = [required(&string_type), required(&string_type)];
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
        let mut reordered =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &schema, bounds(2, 128, 512))
                .expect("owner");
        let mut reordered_work = work();
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
            let schema = [optional(&logical_type)];
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
        let schema = [required(&money_type)];
        let mut mismatched =
            CanonicalGroupLeafBuilder::new(identity(0, 4, 0), &schema, bounds(2, 128, 512))
                .expect("owner");
        let mut mismatch_work = work();
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
        let schema = [optional(&u64_type), optional(&string_type)];
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
        let schema = [required(&logical_type)];
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
        let other_schema = [required(&other_type)];
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
        let schema = [required(&logical_type)];
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
            owner.admit_encoded(&[0], 1),
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
        let schema = [required(&logical_type)];
        let maximum_inventory = (0..MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1)
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
        .expect("exact maximum inventory");
        assert_eq!(exact_inventory_work.remaining(), 0);

        let mut excessive_inventory = maximum_inventory;
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
        assert_eq!(mismatch_work.remaining(), 4);
        assert_eq!(
            mismatch.merge_leaf(leaf(inventory[1], &schema, &rows), &mut mismatch_work,),
            Err(GroupStateError::InventoryGap)
        );
        assert_eq!(mismatch_work.remaining(), 2);
        let remaining_after_scan = mismatch_work.remaining();
        assert_eq!(
            mismatch.merge_leaf(leaf(inventory[1], &schema, &rows), &mut mismatch_work,),
            Err(GroupStateError::Poisoned)
        );
        assert_eq!(mismatch_work.remaining(), remaining_after_scan);

        let key_len = oracle_key(&rows[0]).len();
        let exact_state =
            u32::try_from(size_of::<super::GroupEntry>() + 2 * key_len).expect("small state");
        let mut builder = CanonicalGroupLeafBuilder::new(
            identity(0, 3, 0),
            &schema,
            bounds(1, key_len as u32, exact_state),
        )
        .expect("builder");
        let mut exact_row_work =
            GroupWorkBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1).expect("maximum work");
        for row in 0..MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 {
            builder
                .push_row(row, &rows[0], &mut exact_row_work)
                .expect("exact row bound");
        }
        assert_eq!(exact_row_work.remaining(), 0);
        let mut one_more_work = GroupWorkBudget::new(1).expect("independent work");
        assert_eq!(
            builder.push_row(
                MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
                &rows[0],
                &mut one_more_work,
            ),
            Err(GroupStateError::RowBoundExceeded)
        );
        assert_eq!(one_more_work.remaining(), 0);
        assert!(builder.is_poisoned());
    }

    #[test]
    fn work_exhaustion_after_success_poisons_and_releases_partial_groups() {
        let logical_type = SegmentV2LogicalType::U64;
        let schema = [required(&logical_type)];
        let first = [SegmentV2Cell::Value(CanonicalValue::U64(1))];
        let second = [SegmentV2Cell::Value(CanonicalValue::U64(2))];

        let mut builder = CanonicalGroupLeafBuilder::new(
            identity(0, 1, 0),
            &schema,
            bounds(2, 32, 256),
        )
        .expect("builder");
        let mut builder_work = GroupWorkBudget::new(1).expect("one admitted row");
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
        let mut merge_work = GroupWorkBudget::new(4).expect("inventory plus first merge");
        let mut merge = CanonicalGroupMerge::new(
            &schema,
            &inventory,
            bounds(2, 32, 256),
            &mut merge_work,
        )
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
    fn production_group_owner_has_no_per_row_owned_key_or_map() {
        let source = include_str!("group.rs");
        let production = source
            .split("\n#[cfg(test)]\nmod tests {")
            .next()
            .expect("complete production source");
        assert!(
            production.contains("arena: Vec<u8>")
                && production.contains("entries: Vec<GroupEntry>")
        );
        assert!(production.contains("struct CanonicalGroupMerge"));
        assert!(production.contains("fn classify_mismatch("));
        assert!(production.contains("if lane.optional"));
        assert!(production.contains("work.charge(self.inventory.len().max(1))"));
        assert!(production.contains("inventory.len() > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1"));
        let classifier = production
            .split("fn classify_mismatch(")
            .nth(1)
            .expect("classifier")
            .split("\n}")
            .next()
            .expect("classifier body");
        assert_eq!(classifier.matches("for (").count(), 1);
        assert!(!classifier.contains(".contains(&actual)"));
        assert!(!classifier.contains(".any("));
        assert_eq!(production.matches(": Vec<").count(), 2);
        for forbidden in [
            "Vec<CanonicalValue>",
            "Vec<SegmentV2Cell>",
            "BTreeMap",
            "HashMap",
            ".to_vec()",
            "Vec::with_capacity",
            ".reserve(",
        ] {
            assert!(
                !production.contains(forbidden),
                "per-row allocation form {forbidden}"
            );
        }
    }

    #[test]
    fn empty_inventory_and_sealed_optionality_have_explicit_bounded_contracts() {
        let logical_type = SegmentV2LogicalType::U64;
        let required = [super::CanonicalGroupLane::new(&logical_type, false)];
        let optional = [super::CanonicalGroupLane::new(&logical_type, true)];
        let mut zero_work = super::GroupWorkBudget::new(1).expect("work ceiling");
        let empty = CanonicalGroupMerge::new(&required, &[], bounds(1, 16, 64), &mut zero_work)
            .expect("exact empty inventory");
        let empty = empty.finish().expect("empty groups");
        assert_eq!(group_rows(&empty), []);
        assert!(empty.owner.is_none());
        assert_eq!(zero_work.remaining(), 1);

        let mut work = super::GroupWorkBudget::new(1).expect("work ceiling");
        let mut required_leaf =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &required, bounds(1, 16, 64))
                .expect("required lane");
        assert_eq!(
            required_leaf.push_row(0, &[SegmentV2Cell::Null], &mut work),
            Err(GroupStateError::UnexpectedNoValue)
        );
        assert_eq!(work.remaining(), 0);
        assert!(required_leaf.owner.entries.is_empty() && required_leaf.owner.arena.is_empty());

        let mut missing_work = super::GroupWorkBudget::new(1).expect("work ceiling");
        let mut required_missing =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &required, bounds(1, 16, 64))
                .expect("required lane");
        assert_eq!(
            required_missing.push_row(0, &[SegmentV2Cell::Missing], &mut missing_work),
            Err(GroupStateError::UnexpectedNoValue)
        );
        assert!(
            required_missing.owner.entries.is_empty() && required_missing.owner.arena.is_empty()
        );

        let mut optional_work = super::GroupWorkBudget::new(2).expect("work ceiling");
        let mut optional_leaf =
            CanonicalGroupLeafBuilder::new(identity(0, 1, 0), &optional, bounds(2, 16, 128))
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
