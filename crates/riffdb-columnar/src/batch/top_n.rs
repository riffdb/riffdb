//! Private profile-bound Top-N mechanics over one validated immutable lane owner.

use std::cmp::Ordering;
use std::mem::size_of;

use riffdb_types::{
    CanonicalValue, MAX_APPLICATION_QUERY_PAGE_ROWS, MAX_APPLICATION_QUERY_RESULT_BYTES,
    MAX_KEY_BYTES, ProjectionGeneration, QueryPlanHash,
};

use crate::PrimaryKeyBytes;
use crate::segment_v2::{
    MAX_SEGMENT_V2_COLUMNS, MAX_SEGMENT_V2_ROWS, SegmentV2Cell, SegmentV2LogicalType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNError {
    AmbiguousComparisonProfile,
    InvalidLimit,
    InvalidOrder,
    InvalidPrimaryKey,
    DuplicatePrimaryKey,
    LaneCount,
    LaneLength,
    RowOutOfOrder,
    UnexpectedNoValue,
    TypeMismatch,
    ArithmeticOverflow,
    StateBoundExceeded,
    InvalidHeapState,
}

/// The only comparison profile this inert primitive implements.
///
/// This is deliberately process-private and nonserializable. It names the
/// existing scalar order frozen by ADR-0145 and retained by ADR-0161; in
/// particular, Decimal and Money retain canonical-value-byte order rather
/// than the numeric order of a possible successor profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNComparisonProfile {
    CanonicalScalarV1,
}

/// Exact compiler plan and Active generation to which one primitive belongs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNProgramBinding {
    comparison_profile: TopNComparisonProfile,
    plan_identity: QueryPlanHash,
    generation: ProjectionGeneration,
}

impl TopNProgramBinding {
    pub(crate) fn new(
        comparison_profile: Option<TopNComparisonProfile>,
        plan_identity: QueryPlanHash,
        generation: ProjectionGeneration,
    ) -> Result<Self, TopNError> {
        let comparison_profile = comparison_profile.ok_or(TopNError::AmbiguousComparisonProfile)?;
        Ok(Self {
            comparison_profile,
            plan_identity,
            generation,
        })
    }

    pub(crate) const fn comparison_profile(self) -> TopNComparisonProfile {
        self.comparison_profile
    }

    pub(crate) const fn plan_identity(self) -> QueryPlanHash {
        self.plan_identity
    }

    pub(crate) const fn generation(self) -> ProjectionGeneration {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNValueDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNNoValuePlacement {
    PresentOnly,
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopNOrderTerm {
    logical_type: SegmentV2LogicalType,
    direction: TopNValueDirection,
    no_value: TopNNoValuePlacement,
}

impl TopNOrderTerm {
    pub(crate) const fn new(
        logical_type: SegmentV2LogicalType,
        direction: TopNValueDirection,
        no_value: TopNNoValuePlacement,
    ) -> Self {
        Self {
            logical_type,
            direction,
            no_value,
        }
    }
}

/// Borrowed lanes and primary keys already owned by one validated immutable
/// segment/Active view. Construction proves aligned lanes and unique canonical
/// key order once, without allocating a second row population.
#[derive(Debug)]
pub(crate) struct TopNSource<'view> {
    binding: TopNProgramBinding,
    order: &'view [TopNOrderTerm],
    primary_keys: &'view [PrimaryKeyBytes],
    order_lanes: &'view [&'view [SegmentV2Cell]],
}

impl<'view> TopNSource<'view> {
    pub(crate) fn new(
        binding: TopNProgramBinding,
        order: &'view [TopNOrderTerm],
        primary_keys: &'view [PrimaryKeyBytes],
        order_lanes: &'view [&'view [SegmentV2Cell]],
    ) -> Result<Self, TopNError> {
        if order.len() > MAX_SEGMENT_V2_COLUMNS {
            return Err(TopNError::InvalidOrder);
        }
        if order_lanes.len() != order.len() {
            return Err(TopNError::LaneCount);
        }
        if primary_keys.len() > MAX_SEGMENT_V2_ROWS
            || order_lanes
                .iter()
                .any(|lane| lane.len() != primary_keys.len())
        {
            return Err(TopNError::LaneLength);
        }
        for primary_key in primary_keys {
            if primary_key.as_bytes().is_empty() || primary_key.as_bytes().len() > MAX_KEY_BYTES {
                return Err(TopNError::InvalidPrimaryKey);
            }
        }
        for pair in primary_keys.windows(2) {
            match pair[0].as_bytes().cmp(pair[1].as_bytes()) {
                Ordering::Equal => return Err(TopNError::DuplicatePrimaryKey),
                Ordering::Greater => return Err(TopNError::InvalidPrimaryKey),
                Ordering::Less => {}
            }
        }
        Ok(Self {
            binding,
            order,
            primary_keys,
            order_lanes,
        })
    }

    pub(crate) const fn binding(&self) -> TopNProgramBinding {
        self.binding
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.primary_keys.len()
    }

    fn primary_key(&self, row: RowOrdinal) -> Result<&PrimaryKeyBytes, TopNError> {
        self.primary_keys
            .get(row.as_usize())
            .ok_or(TopNError::LaneLength)
    }

    fn cell(&self, term: usize, row: RowOrdinal) -> Result<&SegmentV2Cell, TopNError> {
        self.order_lanes
            .get(term)
            .and_then(|lane| lane.get(row.as_usize()))
            .ok_or(TopNError::LaneLength)
    }
}

/// A fixed non-owning handle into the immutable source lanes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowOrdinal(u16);

impl RowOrdinal {
    fn new(row: usize, row_count: usize) -> Result<Self, TopNError> {
        if row >= row_count {
            return Err(TopNError::LaneLength);
        }
        let row = u16::try_from(row).map_err(|_| TopNError::LaneLength)?;
        Ok(Self(row))
    }

    const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug)]
pub(crate) struct BoundedTopN<'view> {
    source: &'view TopNSource<'view>,
    limit: usize,
    capacity: usize,
    retained_allocation_bytes: usize,
    retained: Vec<RowOrdinal>,
    last_row: Option<RowOrdinal>,
    failure: Option<TopNError>,
}

impl<'view> BoundedTopN<'view> {
    pub(crate) fn new(source: &'view TopNSource<'view>, limit: usize) -> Result<Self, TopNError> {
        let maximum = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS)
            .map_err(|_| TopNError::ArithmeticOverflow)?;
        if limit == 0 || limit > maximum {
            return Err(TopNError::InvalidLimit);
        }
        let capacity = limit.checked_add(1).ok_or(TopNError::ArithmeticOverflow)?;
        let requested_bytes = retained_state_bytes(capacity, 0)?;
        enforce_state_ceiling(requested_bytes)?;
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(capacity)
            .map_err(|_| TopNError::StateBoundExceeded)?;
        let retained_allocation_bytes = retained_state_bytes(retained.capacity(), 0)?;
        enforce_state_ceiling(retained_allocation_bytes)?;
        Ok(Self {
            source,
            limit,
            capacity,
            retained_allocation_bytes,
            retained,
            last_row: None,
            failure: None,
        })
    }

    pub(crate) fn push(&mut self, row: usize) -> Result<(), TopNError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let row = match RowOrdinal::new(row, self.source.row_count()) {
            Ok(row) => row,
            Err(error) => return self.poison(error),
        };
        if self.last_row.is_some_and(|previous| previous >= row) {
            return self.poison(TopNError::RowOutOfOrder);
        }
        if let Err(error) = validate_row(self.source, row) {
            return self.poison(error);
        }
        self.last_row = Some(row);

        if self.retained.len() < self.capacity {
            if let Err(error) = push_heap(self.source, &mut self.retained, row) {
                return self.poison(error);
            }
            return Ok(());
        }

        let Some(worst) = self.retained.first().copied() else {
            return self.poison(TopNError::InvalidHeapState);
        };
        let compared = match compare_rows(self.source, row, worst) {
            Ok(compared) => compared,
            Err(error) => return self.poison(error),
        };
        if compared != Ordering::Less {
            return Ok(());
        }
        self.retained[0] = row;
        let end = self.retained.len();
        if let Err(error) = sift_down(self.source, &mut self.retained, 0, end) {
            return self.poison(error);
        }
        Ok(())
    }

    pub(crate) const fn retained_len(&self) -> usize {
        self.retained.len()
    }

    pub(crate) const fn retained_allocation_bytes(&self) -> usize {
        self.retained_allocation_bytes
    }

    pub(crate) fn finish(mut self) -> Result<TopNPage<'view>, TopNError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        heap_sort(self.source, &mut self.retained)?;
        let has_more = self.retained.len() > self.limit;
        if has_more {
            self.retained.truncate(self.limit);
        }
        let cursor = has_more.then(|| self.retained.last().copied()).flatten();
        Ok(TopNPage {
            source: self.source,
            rows: self.retained,
            cursor,
        })
    }

    fn poison(&mut self, error: TopNError) -> Result<(), TopNError> {
        self.retained.clear();
        self.failure = Some(error);
        Err(error)
    }
}

#[derive(Debug)]
pub(crate) struct TopNPage<'view> {
    source: &'view TopNSource<'view>,
    rows: Vec<RowOrdinal>,
    cursor: Option<RowOrdinal>,
}

impl TopNPage<'_> {
    pub(crate) const fn has_more(&self) -> bool {
        self.cursor.is_some()
    }

    pub(crate) fn cursor_primary_key(&self) -> Result<Option<&PrimaryKeyBytes>, TopNError> {
        self.cursor
            .map(|cursor| self.source.primary_key(cursor))
            .transpose()
    }

    pub(crate) fn primary_key_slices(&self) -> impl Iterator<Item = Result<&[u8], TopNError>> {
        self.rows
            .iter()
            .map(|row| self.source.primary_key(*row).map(PrimaryKeyBytes::as_bytes))
    }

    pub(crate) fn compare_row_to_cursor(&self, row: usize) -> Result<Option<Ordering>, TopNError> {
        let Some((row, cursor)) = self.rows.get(row).copied().zip(self.cursor) else {
            return Ok(None);
        };
        compare_rows(self.source, row, cursor).map(Some)
    }
}

fn retained_state_bytes(heap_capacity: usize, arena_capacity: usize) -> Result<usize, TopNError> {
    heap_capacity
        .checked_mul(size_of::<RowOrdinal>())
        .and_then(|heap| heap.checked_add(arena_capacity))
        .ok_or(TopNError::ArithmeticOverflow)
}

fn enforce_state_ceiling(bytes: usize) -> Result<(), TopNError> {
    let ceiling = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES)
        .map_err(|_| TopNError::ArithmeticOverflow)?;
    if bytes > ceiling {
        return Err(TopNError::StateBoundExceeded);
    }
    Ok(())
}

fn validate_row(source: &TopNSource<'_>, row: RowOrdinal) -> Result<(), TopNError> {
    source.primary_key(row)?;
    for (index, term) in source.order.iter().enumerate() {
        validate_cell(term, source.cell(index, row)?)?;
    }
    Ok(())
}

fn validate_cell(term: &TopNOrderTerm, cell: &SegmentV2Cell) -> Result<(), TopNError> {
    match cell {
        SegmentV2Cell::Missing | SegmentV2Cell::Null => match term.no_value {
            TopNNoValuePlacement::PresentOnly => Err(TopNError::UnexpectedNoValue),
            TopNNoValuePlacement::First | TopNNoValuePlacement::Last => Ok(()),
        },
        SegmentV2Cell::Value(value) => validate_present_type(&term.logical_type, value)
            .then_some(())
            .ok_or(TopNError::TypeMismatch),
    }
}

pub(super) fn validate_present_type(
    logical_type: &SegmentV2LogicalType,
    value: &CanonicalValue,
) -> bool {
    match (logical_type, value) {
        (SegmentV2LogicalType::Bool, CanonicalValue::Bool(_))
        | (SegmentV2LogicalType::I64, CanonicalValue::I64(_))
        | (SegmentV2LogicalType::U64, CanonicalValue::U64(_))
        | (SegmentV2LogicalType::String, CanonicalValue::String(_))
        | (SegmentV2LogicalType::Bytes, CanonicalValue::Bytes(_))
        | (SegmentV2LogicalType::Timestamp, CanonicalValue::Timestamp(_))
        | (SegmentV2LogicalType::Date, CanonicalValue::Date(_))
        | (SegmentV2LogicalType::Uuid, CanonicalValue::Uuid(_)) => true,
        (SegmentV2LogicalType::Enum(expected), CanonicalValue::Enum { type_id, .. }) => {
            expected == type_id
        }
        (SegmentV2LogicalType::Decimal(expected), CanonicalValue::Decimal(value)) => {
            *expected == value.spec()
        }
        (SegmentV2LogicalType::Money { currency, amount }, CanonicalValue::Money(value)) => {
            *currency == value.currency() && *amount == value.amount().spec()
        }
        _ => false,
    }
}

fn compare_rows(
    source: &TopNSource<'_>,
    left: RowOrdinal,
    right: RowOrdinal,
) -> Result<Ordering, TopNError> {
    for (index, term) in source.order.iter().enumerate() {
        let compared = compare_cells(
            source.binding.comparison_profile,
            term,
            source.cell(index, left)?,
            source.cell(index, right)?,
        )?;
        if compared != Ordering::Equal {
            return Ok(compared);
        }
    }
    Ok(source
        .primary_key(left)?
        .as_bytes()
        .cmp(source.primary_key(right)?.as_bytes()))
}

fn compare_cells(
    profile: TopNComparisonProfile,
    term: &TopNOrderTerm,
    left: &SegmentV2Cell,
    right: &SegmentV2Cell,
) -> Result<Ordering, TopNError> {
    validate_cell(term, left)?;
    validate_cell(term, right)?;
    let compared = match (left, right) {
        (
            SegmentV2Cell::Missing | SegmentV2Cell::Null,
            SegmentV2Cell::Missing | SegmentV2Cell::Null,
        ) => Ordering::Equal,
        (SegmentV2Cell::Missing | SegmentV2Cell::Null, SegmentV2Cell::Value(_)) => {
            match term.no_value {
                TopNNoValuePlacement::First => Ordering::Less,
                TopNNoValuePlacement::Last => Ordering::Greater,
                TopNNoValuePlacement::PresentOnly => return Err(TopNError::UnexpectedNoValue),
            }
        }
        (SegmentV2Cell::Value(_), SegmentV2Cell::Missing | SegmentV2Cell::Null) => {
            match term.no_value {
                TopNNoValuePlacement::First => Ordering::Greater,
                TopNNoValuePlacement::Last => Ordering::Less,
                TopNNoValuePlacement::PresentOnly => return Err(TopNError::UnexpectedNoValue),
            }
        }
        (SegmentV2Cell::Value(left), SegmentV2Cell::Value(right)) => {
            let compared = compare_present(profile, &term.logical_type, left, right)?;
            match term.direction {
                TopNValueDirection::Ascending => compared,
                TopNValueDirection::Descending => compared.reverse(),
            }
        }
    };
    Ok(compared)
}

pub(super) fn compare_present(
    profile: TopNComparisonProfile,
    logical_type: &SegmentV2LogicalType,
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<Ordering, TopNError> {
    if !validate_present_type(logical_type, left) || !validate_present_type(logical_type, right) {
        return Err(TopNError::TypeMismatch);
    }
    match profile {
        TopNComparisonProfile::CanonicalScalarV1 => {}
    }
    let compared = match (left, right) {
        (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => left.cmp(right),
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.cmp(right),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.cmp(right),
        (CanonicalValue::String(left), CanonicalValue::String(right)) => {
            left.as_str().cmp(right.as_str())
        }
        (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => {
            left.as_bytes().cmp(right.as_bytes())
        }
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => left.cmp(right),
        (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.cmp(right),
        (CanonicalValue::Uuid(left), CanonicalValue::Uuid(right)) => left.cmp(right),
        (
            CanonicalValue::Enum {
                variant_id: left, ..
            },
            CanonicalValue::Enum {
                variant_id: right, ..
            },
        ) => left.cmp(right),
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right)) => left
            .coefficient()
            .to_be_bytes()
            .cmp(&right.coefficient().to_be_bytes()),
        (CanonicalValue::Money(left), CanonicalValue::Money(right)) => left
            .amount()
            .coefficient()
            .to_be_bytes()
            .cmp(&right.amount().coefficient().to_be_bytes()),
        _ => return Err(TopNError::TypeMismatch),
    };
    Ok(compared)
}

fn push_heap(
    source: &TopNSource<'_>,
    retained: &mut Vec<RowOrdinal>,
    row: RowOrdinal,
) -> Result<(), TopNError> {
    retained.push(row);
    let mut child = retained.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        if compare_rows(source, retained[child], retained[parent])? != Ordering::Greater {
            break;
        }
        retained.swap(child, parent);
        child = parent;
    }
    Ok(())
}

fn sift_down(
    source: &TopNSource<'_>,
    retained: &mut [RowOrdinal],
    mut parent: usize,
    end: usize,
) -> Result<(), TopNError> {
    loop {
        let left = parent
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or(TopNError::ArithmeticOverflow)?;
        if left >= end {
            break;
        }
        let right = left.checked_add(1).ok_or(TopNError::ArithmeticOverflow)?;
        let larger = if right < end
            && compare_rows(source, retained[right], retained[left])? == Ordering::Greater
        {
            right
        } else {
            left
        };
        if compare_rows(source, retained[larger], retained[parent])? != Ordering::Greater {
            break;
        }
        retained.swap(parent, larger);
        parent = larger;
    }
    Ok(())
}

fn heap_sort(source: &TopNSource<'_>, retained: &mut [RowOrdinal]) -> Result<(), TopNError> {
    for end in (1..retained.len()).rev() {
        retained.swap(0, end);
        sift_down(source, retained, 0, end)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{
        CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId, Money, Timestamp,
        encode_canonical_value,
    };

    fn binding() -> TopNProgramBinding {
        TopNProgramBinding::new(
            Some(TopNComparisonProfile::CanonicalScalarV1),
            QueryPlanHash::from_bytes([0x71; 32]),
            ProjectionGeneration::new(7).expect("generation"),
        )
        .expect("exact binding")
    }

    fn key(value: u32) -> PrimaryKeyBytes {
        PrimaryKeyBytes::from_entity_key_bytes(value.to_be_bytes())
    }

    fn key_bytes(page: &TopNPage<'_>) -> Vec<Vec<u8>> {
        page.primary_key_slices()
            .map(|key| key.expect("valid retained row").to_vec())
            .collect()
    }

    fn source<'a>(
        order: &'a [TopNOrderTerm],
        keys: &'a [PrimaryKeyBytes],
        lanes: &'a [&'a [SegmentV2Cell]],
    ) -> TopNSource<'a> {
        TopNSource::new(binding(), order, keys, lanes).expect("validated source")
    }

    fn oracle_present(
        logical_type: &SegmentV2LogicalType,
        left: &CanonicalValue,
        right: &CanonicalValue,
    ) -> Ordering {
        match (logical_type, left, right) {
            (
                SegmentV2LogicalType::Bool,
                CanonicalValue::Bool(left),
                CanonicalValue::Bool(right),
            ) => left.cmp(right),
            (SegmentV2LogicalType::I64, CanonicalValue::I64(left), CanonicalValue::I64(right)) => {
                left.cmp(right)
            }
            (SegmentV2LogicalType::U64, CanonicalValue::U64(left), CanonicalValue::U64(right)) => {
                left.cmp(right)
            }
            (
                SegmentV2LogicalType::String,
                CanonicalValue::String(left),
                CanonicalValue::String(right),
            ) => left.as_str().cmp(right.as_str()),
            (
                SegmentV2LogicalType::Bytes,
                CanonicalValue::Bytes(left),
                CanonicalValue::Bytes(right),
            ) => left.as_bytes().cmp(right.as_bytes()),
            (
                SegmentV2LogicalType::Timestamp,
                CanonicalValue::Timestamp(left),
                CanonicalValue::Timestamp(right),
            ) => left.cmp(right),
            (
                SegmentV2LogicalType::Date,
                CanonicalValue::Date(left),
                CanonicalValue::Date(right),
            ) => left.cmp(right),
            (
                SegmentV2LogicalType::Uuid,
                CanonicalValue::Uuid(left),
                CanonicalValue::Uuid(right),
            ) => left.cmp(right),
            (
                SegmentV2LogicalType::Enum(_),
                CanonicalValue::Enum { .. },
                CanonicalValue::Enum { .. },
            )
            | (
                SegmentV2LogicalType::Decimal(_),
                CanonicalValue::Decimal(_),
                CanonicalValue::Decimal(_),
            )
            | (
                SegmentV2LogicalType::Money { .. },
                CanonicalValue::Money(_),
                CanonicalValue::Money(_),
            ) => encode_canonical_value(left)
                .expect("left oracle bytes")
                .cmp(&encode_canonical_value(right).expect("right oracle bytes")),
            _ => panic!("test oracle received mismatched values"),
        }
    }

    fn oracle_cell(term: &TopNOrderTerm, left: &SegmentV2Cell, right: &SegmentV2Cell) -> Ordering {
        let left_no_value = matches!(left, SegmentV2Cell::Missing | SegmentV2Cell::Null);
        let right_no_value = matches!(right, SegmentV2Cell::Missing | SegmentV2Cell::Null);
        match (left_no_value, right_no_value) {
            (true, true) => Ordering::Equal,
            (true, false) => match term.no_value {
                TopNNoValuePlacement::First => Ordering::Less,
                TopNNoValuePlacement::Last => Ordering::Greater,
                TopNNoValuePlacement::PresentOnly => {
                    panic!("PresentOnly corpus contains NoValue")
                }
            },
            (false, true) => match term.no_value {
                TopNNoValuePlacement::First => Ordering::Greater,
                TopNNoValuePlacement::Last => Ordering::Less,
                TopNNoValuePlacement::PresentOnly => {
                    panic!("PresentOnly corpus contains NoValue")
                }
            },
            (false, false) => {
                let (SegmentV2Cell::Value(left), SegmentV2Cell::Value(right)) = (left, right)
                else {
                    unreachable!()
                };
                let compared = oracle_present(&term.logical_type, left, right);
                match term.direction {
                    TopNValueDirection::Ascending => compared,
                    TopNValueDirection::Descending => compared.reverse(),
                }
            }
        }
    }

    fn oracle_row(
        order: &[TopNOrderTerm],
        keys: &[PrimaryKeyBytes],
        lanes: &[&[SegmentV2Cell]],
        left: usize,
        right: usize,
    ) -> Ordering {
        for (term, lane) in order.iter().zip(lanes) {
            let compared = oracle_cell(term, &lane[left], &lane[right]);
            if compared != Ordering::Equal {
                return compared;
            }
        }
        keys[left].cmp(&keys[right])
    }

    fn assert_bounded_truncation_matches_full_sort(
        order: &[TopNOrderTerm],
        keys: &[PrimaryKeyBytes],
        lanes: &[&[SegmentV2Cell]],
    ) -> Vec<usize> {
        let source = source(order, keys, lanes);
        let mut expected = (0..source.row_count()).collect::<Vec<_>>();
        expected.sort_unstable_by(|left, right| oracle_row(order, keys, lanes, *left, *right));

        let maximum_applicable = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS)
            .expect("page maximum")
            .min(source.row_count() - 1);
        let limits = [1, source.row_count() / 2, maximum_applicable];
        for limit in limits {
            let mut top = BoundedTopN::new(&source, limit).expect("bounded top-n");
            for row in 0..source.row_count() {
                top.push(row).expect("valid canonical row");
                assert!(top.retained_len() <= limit + 1);
            }
            let page = top.finish().expect("complete bounded page");
            let expected_keys = expected
                .iter()
                .take(limit)
                .map(|row| keys[*row].as_bytes().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(key_bytes(&page), expected_keys);
            assert!(page.has_more());
            assert_eq!(
                page.cursor_primary_key()
                    .expect("valid retained cursor")
                    .expect("continuation cursor")
                    .as_bytes(),
                keys[expected[limit - 1]].as_bytes()
            );
            assert_eq!(
                page.compare_row_to_cursor(limit - 1),
                Ok(Some(Ordering::Equal))
            );
            if limit > 1 {
                assert_eq!(page.compare_row_to_cursor(0), Ok(Some(Ordering::Less)));
            }
        }
        expected
    }

    fn assert_profile_differential(
        logical_type: SegmentV2LogicalType,
        values: Vec<CanonicalValue>,
    ) {
        let keys = (1..=values.len() as u32).map(key).collect::<Vec<_>>();
        let lane = values
            .into_iter()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        let lanes = [lane.as_slice()];
        for direction in [
            TopNValueDirection::Ascending,
            TopNValueDirection::Descending,
        ] {
            let order = [TopNOrderTerm::new(
                logical_type.clone(),
                direction,
                TopNNoValuePlacement::PresentOnly,
            )];
            let source = source(&order, &keys, &lanes);
            let mut expected = (0..source.row_count()).collect::<Vec<_>>();
            expected.sort_unstable_by(|left, right| {
                let left_value = match &lane[*left] {
                    SegmentV2Cell::Value(value) => value,
                    _ => unreachable!(),
                };
                let right_value = match &lane[*right] {
                    SegmentV2Cell::Value(value) => value,
                    _ => unreachable!(),
                };
                let compared = oracle_present(&logical_type, left_value, right_value);
                match direction {
                    TopNValueDirection::Ascending => compared,
                    TopNValueDirection::Descending => compared.reverse(),
                }
                .then_with(|| keys[*left].cmp(&keys[*right]))
            });
            let mut top = BoundedTopN::new(&source, source.row_count()).expect("bounded top-n");
            for row in 0..source.row_count() {
                top.push(row).expect("valid row");
            }
            let actual = key_bytes(&top.finish().expect("page"));
            let expected = expected
                .into_iter()
                .map(|row| keys[row].as_bytes().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    // Inert mechanics checkpoint only. This deliberately does not claim an
    // ADR-0161 obligation or select the batch path for production execution.
    #[test]
    fn profile_binding_is_exact_and_ambiguous_construction_is_refused() {
        assert_eq!(
            TopNProgramBinding::new(
                None,
                QueryPlanHash::from_bytes([1; 32]),
                ProjectionGeneration::first()
            ),
            Err(TopNError::AmbiguousComparisonProfile)
        );
        let binding = binding();
        assert_eq!(
            binding.comparison_profile(),
            TopNComparisonProfile::CanonicalScalarV1
        );
        assert_eq!(
            binding.plan_identity(),
            QueryPlanHash::from_bytes([0x71; 32])
        );
        assert_eq!(
            binding.generation(),
            ProjectionGeneration::new(7).expect("generation")
        );
    }

    #[test]
    fn all_direction_and_no_value_placement_combinations_are_exact() {
        let keys = vec![key(1), key(2), key(3), key(4)];
        let lane = vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::U64(3)),
            SegmentV2Cell::Value(CanonicalValue::U64(7)),
        ];
        let lanes = [lane.as_slice()];
        for (direction, placement, expected) in [
            (
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::First,
                vec![1, 2, 3, 4],
            ),
            (
                TopNValueDirection::Descending,
                TopNNoValuePlacement::First,
                vec![1, 2, 4, 3],
            ),
            (
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::Last,
                vec![3, 4, 1, 2],
            ),
            (
                TopNValueDirection::Descending,
                TopNNoValuePlacement::Last,
                vec![4, 3, 1, 2],
            ),
        ] {
            let order = [TopNOrderTerm::new(
                SegmentV2LogicalType::U64,
                direction,
                placement,
            )];
            let source = source(&order, &keys, &lanes);
            let mut top = BoundedTopN::new(&source, 4).expect("bounded top-n");
            for row in 0..4 {
                top.push(row).expect("valid row");
            }
            assert_eq!(
                key_bytes(&top.finish().expect("page")),
                expected
                    .into_iter()
                    .map(|value| key(value).as_bytes().to_vec())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn profile_matches_an_independent_full_sort_for_every_admitted_scalar_type() {
        assert_profile_differential(
            SegmentV2LogicalType::Bool,
            vec![CanonicalValue::Bool(true), CanonicalValue::Bool(false)],
        );
        assert_profile_differential(
            SegmentV2LogicalType::I64,
            vec![
                CanonicalValue::I64(1),
                CanonicalValue::I64(-1),
                CanonicalValue::I64(0),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::U64,
            vec![
                CanonicalValue::U64(9),
                CanonicalValue::U64(1),
                CanonicalValue::U64(5),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::String,
            vec![
                CanonicalValue::string("beta").expect("string"),
                CanonicalValue::string("alpha").expect("string"),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::Bytes,
            vec![
                CanonicalValue::bytes([2]).expect("bytes"),
                CanonicalValue::bytes([1]).expect("bytes"),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::Timestamp,
            vec![
                CanonicalValue::Timestamp(Timestamp::new(1, 0).expect("timestamp")),
                CanonicalValue::Timestamp(Timestamp::new(-1, 0).expect("timestamp")),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::Date,
            vec![
                CanonicalValue::Date(Date::new(1)),
                CanonicalValue::Date(Date::new(-1)),
            ],
        );
        assert_profile_differential(
            SegmentV2LogicalType::Uuid,
            vec![CanonicalValue::Uuid([2; 16]), CanonicalValue::Uuid([1; 16])],
        );
        let enum_type = EnumTypeId::new(7).expect("enum type");
        assert_profile_differential(
            SegmentV2LogicalType::Enum(enum_type),
            vec![
                CanonicalValue::Enum {
                    type_id: enum_type,
                    variant_id: EnumVariantId::new(2).expect("variant"),
                },
                CanonicalValue::Enum {
                    type_id: enum_type,
                    variant_id: EnumVariantId::new(1).expect("variant"),
                },
            ],
        );
        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        assert_profile_differential(
            SegmentV2LogicalType::Decimal(decimal),
            vec![
                CanonicalValue::Decimal(Decimal::new(decimal, -1).expect("decimal")),
                CanonicalValue::Decimal(Decimal::new(decimal, 1).expect("decimal")),
                CanonicalValue::Decimal(Decimal::new(decimal, 0).expect("decimal")),
            ],
        );
        let usd = CurrencyCode::new("USD").expect("currency");
        assert_profile_differential(
            SegmentV2LogicalType::Money {
                currency: usd,
                amount: decimal,
            },
            vec![
                CanonicalValue::Money(Money::new(usd, Decimal::new(decimal, -1).expect("amount"))),
                CanonicalValue::Money(Money::new(usd, Decimal::new(decimal, 1).expect("amount"))),
                CanonicalValue::Money(Money::new(usd, Decimal::new(decimal, 0).expect("amount"))),
            ],
        );
    }

    #[test]
    fn bounded_eviction_matches_full_sort_for_states_decimal_money_and_ties() {
        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        let decimal_value = |coefficient| {
            SegmentV2Cell::Value(CanonicalValue::Decimal(
                Decimal::new(decimal, coefficient).expect("decimal"),
            ))
        };
        let keys = (1..=7).map(key).collect::<Vec<_>>();
        let decimal_lane = vec![
            decimal_value(-2),
            SegmentV2Cell::Missing,
            decimal_value(9),
            SegmentV2Cell::Null,
            decimal_value(0),
            decimal_value(1),
            decimal_value(-1),
        ];
        let decimal_lanes = [decimal_lane.as_slice()];
        for direction in [
            TopNValueDirection::Ascending,
            TopNValueDirection::Descending,
        ] {
            for placement in [TopNNoValuePlacement::First, TopNNoValuePlacement::Last] {
                let order = [TopNOrderTerm::new(
                    SegmentV2LogicalType::Decimal(decimal),
                    direction,
                    placement,
                )];
                let expected =
                    assert_bounded_truncation_matches_full_sort(&order, &keys, &decimal_lanes);
                assert_ne!(
                    expected[0], 0,
                    "the limit-one proof must require heap replacement"
                );
            }
        }

        for state in [SegmentV2Cell::Missing, SegmentV2Cell::Null] {
            let present_only_lane = vec![state, decimal_value(0)];
            let present_only_lanes = [present_only_lane.as_slice()];
            for direction in [
                TopNValueDirection::Ascending,
                TopNValueDirection::Descending,
            ] {
                let present_only_order = [TopNOrderTerm::new(
                    SegmentV2LogicalType::Decimal(decimal),
                    direction,
                    TopNNoValuePlacement::PresentOnly,
                )];
                let present_only_source =
                    source(&present_only_order, &keys[..2], &present_only_lanes);
                let mut top = BoundedTopN::new(&present_only_source, 1).expect("bounded top-n");
                assert_eq!(top.push(0), Err(TopNError::UnexpectedNoValue));
                assert_eq!(top.finish().unwrap_err(), TopNError::UnexpectedNoValue);
            }
        }

        let usd = CurrencyCode::new("USD").expect("currency");
        let money_value = |coefficient| {
            SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                usd,
                Decimal::new(decimal, coefficient).expect("amount"),
            )))
        };
        let money_lane = vec![
            money_value(-1),
            money_value(9),
            money_value(1),
            money_value(0),
            money_value(-2),
            money_value(1),
            money_value(0),
        ];
        let money_lanes = [money_lane.as_slice()];
        for direction in [
            TopNValueDirection::Ascending,
            TopNValueDirection::Descending,
        ] {
            let order = [TopNOrderTerm::new(
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                direction,
                TopNNoValuePlacement::PresentOnly,
            )];
            let expected = assert_bounded_truncation_matches_full_sort(&order, &keys, &money_lanes);
            if direction == TopNValueDirection::Ascending {
                assert_eq!(expected[0], 3, "canonical bytes place zero first");
            } else {
                assert_eq!(expected[0], 0, "canonical bytes place minus one first");
            }
        }

        let first_lane = vec![
            decimal_value(-1),
            decimal_value(1),
            decimal_value(1),
            decimal_value(0),
            decimal_value(0),
            decimal_value(1),
            decimal_value(1),
        ];
        let second_lane = vec![
            money_value(0),
            money_value(-1),
            money_value(-1),
            SegmentV2Cell::Null,
            SegmentV2Cell::Missing,
            money_value(1),
            money_value(1),
        ];
        let multi_lanes = [first_lane.as_slice(), second_lane.as_slice()];
        let multi_order = [
            TopNOrderTerm::new(
                SegmentV2LogicalType::Decimal(decimal),
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::Last,
            ),
            TopNOrderTerm::new(
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                TopNValueDirection::Descending,
                TopNNoValuePlacement::First,
            ),
        ];
        let expected =
            assert_bounded_truncation_matches_full_sort(&multi_order, &keys, &multi_lanes);
        assert_eq!(
            expected,
            vec![3, 4, 1, 2, 5, 6, 0],
            "multi-term state placement, canonical values, and PK ties are independently pinned"
        );
    }

    #[test]
    fn source_refuses_duplicate_and_out_of_bound_keys_and_lane_shapes() {
        let order = [TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::PresentOnly,
        )];
        let lane = vec![
            SegmentV2Cell::Value(CanonicalValue::U64(1)),
            SegmentV2Cell::Value(CanonicalValue::U64(2)),
        ];
        let lanes = [lane.as_slice()];
        let duplicate = vec![key(1), key(1)];
        assert_eq!(
            TopNSource::new(binding(), &order, &duplicate, &lanes).unwrap_err(),
            TopNError::DuplicatePrimaryKey
        );
        let reversed = vec![key(2), key(1)];
        assert_eq!(
            TopNSource::new(binding(), &order, &reversed, &lanes).unwrap_err(),
            TopNError::InvalidPrimaryKey
        );

        let empty = vec![PrimaryKeyBytes::from_entity_key_bytes(Vec::new())];
        let one_lane = vec![SegmentV2Cell::Value(CanonicalValue::U64(1))];
        let one_lanes = [one_lane.as_slice()];
        assert_eq!(
            TopNSource::new(binding(), &order, &empty, &one_lanes).unwrap_err(),
            TopNError::InvalidPrimaryKey
        );
        let maximum = vec![PrimaryKeyBytes::from_entity_key_bytes(vec![
            1;
            MAX_KEY_BYTES
        ])];
        assert!(TopNSource::new(binding(), &order, &maximum, &one_lanes).is_ok());
        let oversized = vec![PrimaryKeyBytes::from_entity_key_bytes(vec![
            1;
            MAX_KEY_BYTES
                + 1
        ])];
        assert_eq!(
            TopNSource::new(binding(), &order, &oversized, &one_lanes).unwrap_err(),
            TopNError::InvalidPrimaryKey
        );
        assert_eq!(
            TopNSource::new(binding(), &order, &maximum, &[]).unwrap_err(),
            TopNError::LaneCount
        );
        assert_eq!(
            TopNSource::new(binding(), &order, &maximum, &lanes).unwrap_err(),
            TopNError::LaneLength
        );
    }

    #[test]
    fn exact_order_limit_and_retained_allocation_bounds_are_closed() {
        let maximum_terms = vec![
            TopNOrderTerm::new(
                SegmentV2LogicalType::U64,
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::PresentOnly,
            );
            MAX_SEGMENT_V2_COLUMNS
        ];
        let key_rows = vec![key(1)];
        let lane_storage = (0..MAX_SEGMENT_V2_COLUMNS)
            .map(|_| vec![SegmentV2Cell::Value(CanonicalValue::U64(1))])
            .collect::<Vec<_>>();
        let lane_refs = lane_storage.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let maximum_source = source(&maximum_terms, &key_rows, &lane_refs);
        let maximum_limit = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS).expect("page maximum");
        let top = BoundedTopN::new(&maximum_source, maximum_limit).expect("maximum limit");
        assert_eq!(top.capacity, 65_535);
        assert_eq!(
            top.retained_allocation_bytes(),
            top.retained.capacity() * size_of::<RowOrdinal>()
        );
        assert_eq!(
            top.retained_allocation_bytes(),
            65_535 * size_of::<RowOrdinal>()
        );
        assert_eq!(
            BoundedTopN::new(&maximum_source, 0).unwrap_err(),
            TopNError::InvalidLimit
        );
        assert_eq!(
            BoundedTopN::new(&maximum_source, maximum_limit + 1).unwrap_err(),
            TopNError::InvalidLimit
        );

        let too_many_terms = vec![maximum_terms[0].clone(); MAX_SEGMENT_V2_COLUMNS + 1];
        let too_many_storage = (0..=MAX_SEGMENT_V2_COLUMNS)
            .map(|_| vec![SegmentV2Cell::Value(CanonicalValue::U64(1))])
            .collect::<Vec<_>>();
        let too_many_refs = too_many_storage
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        assert_eq!(
            TopNSource::new(binding(), &too_many_terms, &key_rows, &too_many_refs).unwrap_err(),
            TopNError::InvalidOrder
        );

        let ceiling = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES).expect("state ceiling");
        assert_eq!(enforce_state_ceiling(ceiling), Ok(()));
        assert_eq!(
            enforce_state_ceiling(ceiling + 1),
            Err(TopNError::StateBoundExceeded)
        );
        assert_eq!(
            retained_state_bytes(usize::MAX, usize::MAX),
            Err(TopNError::ArithmeticOverflow)
        );
        assert_eq!(
            RowOrdinal::new(usize::from(u16::MAX), MAX_SEGMENT_V2_ROWS),
            Ok(RowOrdinal(u16::MAX))
        );
        assert_eq!(
            RowOrdinal::new(MAX_SEGMENT_V2_ROWS, MAX_SEGMENT_V2_ROWS),
            Err(TopNError::LaneLength)
        );
    }

    fn expected_cell_error(cell: &SegmentV2Cell) -> TopNError {
        if matches!(cell, SegmentV2Cell::Null | SegmentV2Cell::Missing) {
            TopNError::UnexpectedNoValue
        } else {
            TopNError::TypeMismatch
        }
    }

    #[test]
    fn type_state_money_spec_and_row_order_failures_are_atomic() {
        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        let other_decimal = DecimalSpec::new(9, 2).expect("other decimal spec");
        let usd = CurrencyCode::new("USD").expect("currency");
        let eur = CurrencyCode::new("EUR").expect("currency");
        for (logical_type, invalid) in [
            (SegmentV2LogicalType::U64, SegmentV2Cell::Null),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::I64(1)),
            ),
            (
                SegmentV2LogicalType::Decimal(decimal),
                SegmentV2Cell::Value(CanonicalValue::Decimal(
                    Decimal::new(other_decimal, 1).expect("decimal"),
                )),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                    eur,
                    Decimal::new(decimal, 1).expect("amount"),
                ))),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                    usd,
                    Decimal::new(other_decimal, 1).expect("amount"),
                ))),
            ),
        ] {
            let order = [TopNOrderTerm::new(
                logical_type.clone(),
                TopNValueDirection::Ascending,
                TopNNoValuePlacement::PresentOnly,
            )];
            let valid = match logical_type {
                SegmentV2LogicalType::U64 => SegmentV2Cell::Value(CanonicalValue::U64(1)),
                SegmentV2LogicalType::Decimal(spec) => SegmentV2Cell::Value(
                    CanonicalValue::Decimal(Decimal::new(spec, 1).expect("decimal")),
                ),
                SegmentV2LogicalType::Money { currency, amount } => {
                    SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                        currency,
                        Decimal::new(amount, 1).expect("amount"),
                    )))
                }
                _ => unreachable!(),
            };
            let keys = vec![key(1), key(2)];
            let lane = vec![valid, invalid];
            let lanes = [lane.as_slice()];
            let source = source(&order, &keys, &lanes);
            let expected = expected_cell_error(&lane[1]);
            let mut top = BoundedTopN::new(&source, 1).expect("bounded top-n");
            top.push(0).expect("valid first row");
            assert_eq!(top.push(1), Err(expected));
            assert_eq!(top.retained_len(), 0);
            assert_eq!(top.finish().unwrap_err(), expected);
        }

        let keys = vec![key(1), key(2)];
        let source = source(&[], &keys, &[]);
        let mut top = BoundedTopN::new(&source, 1).expect("bounded top-n");
        top.push(1).expect("later row");
        assert_eq!(top.push(0), Err(TopNError::RowOutOfOrder));
        assert_eq!(top.retained_len(), 0);
        assert_eq!(top.finish().unwrap_err(), TopNError::RowOutOfOrder);
    }

    #[test]
    fn heap_holds_only_limit_plus_probe_and_cursor_uses_final_order() {
        let keys = (1..=32).map(key).collect::<Vec<_>>();
        let lane = (0_u64..32)
            .rev()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::U64(value)))
            .collect::<Vec<_>>();
        let lanes = [lane.as_slice()];
        let order = [TopNOrderTerm::new(
            SegmentV2LogicalType::U64,
            TopNValueDirection::Ascending,
            TopNNoValuePlacement::PresentOnly,
        )];
        let source = source(&order, &keys, &lanes);
        let mut top = BoundedTopN::new(&source, 2).expect("bounded top-n");
        for row in 0..32 {
            top.push(row).expect("valid row");
            assert!(top.retained_len() <= 3);
        }
        let page = top.finish().expect("page");
        assert_eq!(
            key_bytes(&page),
            vec![key(32).as_bytes().to_vec(), key(31).as_bytes().to_vec()]
        );
        assert!(page.has_more());
        assert_eq!(
            page.cursor_primary_key()
                .expect("valid retained row")
                .expect("cursor")
                .as_bytes(),
            key(31).as_bytes()
        );
        assert_eq!(page.compare_row_to_cursor(0), Ok(Some(Ordering::Less)));
        assert_eq!(page.compare_row_to_cursor(1), Ok(Some(Ordering::Equal)));
        assert_eq!(page.compare_row_to_cursor(2), Ok(None));
    }

    #[test]
    fn heap_entries_are_only_fixed_borrowed_row_handles() {
        let production = include_str!("top_n.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source");
        for forbidden in [
            concat!("TopN", "Candidate"),
            concat!("Ordered", "Scalar"),
            concat!("Ranked", "Cell"),
            concat!("payload_", "bytes"),
            concat!("order_cells: ", "Vec"),
        ] {
            assert!(
                !production.contains(forbidden),
                "production retained owned row state: {forbidden}"
            );
        }
        assert!(production.contains("struct RowOrdinal(u16)"));
        assert!(production.contains("retained: Vec<RowOrdinal>"));
    }
}
