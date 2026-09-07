//! Inert bounded mechanics for compiler-sealed V2 columnar batches.

mod aggregate;
mod group;
mod program;

use std::num::NonZeroU16;

use crate::segment_v2::{
    MAX_SEGMENT_V2_COLUMNS, SegmentV2Cell, SegmentV2LogicalType, SegmentV2Predicate,
};

mod top_n;

const CLOSED_BATCH_WIDTHS: [usize; 5] = [64, 128, 256, 512, 1_024];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarBatchBoundError {
    InvalidCompilerMaximum,
    InvalidByteBound,
    NoAdmissibleWidth,
    LaneCount,
    LaneLength,
    SelectionLength,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarBatchWidth(NonZeroU16);

impl ColumnarBatchWidth {
    pub(crate) fn choose(
        compiler_maximum: usize,
        worst_case_row_bytes: usize,
        batch_byte_ceiling: usize,
    ) -> Result<Self, ColumnarBatchBoundError> {
        if !CLOSED_BATCH_WIDTHS.contains(&compiler_maximum) {
            return Err(ColumnarBatchBoundError::InvalidCompilerMaximum);
        }
        if worst_case_row_bytes == 0 || batch_byte_ceiling == 0 {
            return Err(ColumnarBatchBoundError::InvalidByteBound);
        }
        let selected = CLOSED_BATCH_WIDTHS
            .iter()
            .copied()
            .rev()
            .filter(|width| *width <= compiler_maximum)
            .find(|width| {
                width
                    .checked_mul(worst_case_row_bytes)
                    .is_some_and(|bytes| bytes <= batch_byte_ceiling)
            })
            .ok_or(ColumnarBatchBoundError::NoAdmissibleWidth)?;
        let selected = u16::try_from(selected)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(ColumnarBatchBoundError::NoAdmissibleWidth)?;
        Ok(Self(selected))
    }

    pub(crate) const fn get(self) -> usize {
        self.0.get() as usize
    }
}

pub(crate) struct BorrowedLaneBatch<'lanes> {
    lanes: &'lanes [&'lanes [SegmentV2Cell]],
    maximum_width: ColumnarBatchWidth,
    row_count: NonZeroU16,
}

impl<'lanes> BorrowedLaneBatch<'lanes> {
    pub(crate) fn new(
        width: ColumnarBatchWidth,
        lanes: &'lanes [&'lanes [SegmentV2Cell]],
    ) -> Result<Self, ColumnarBatchBoundError> {
        if lanes.is_empty() || lanes.len() > MAX_SEGMENT_V2_COLUMNS {
            return Err(ColumnarBatchBoundError::LaneCount);
        }
        let row_count = lanes[0].len();
        if row_count == 0
            || row_count > width.get()
            || lanes.iter().any(|lane| lane.len() != row_count)
        {
            return Err(ColumnarBatchBoundError::LaneLength);
        }
        let row_count = u16::try_from(row_count)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(ColumnarBatchBoundError::LaneLength)?;
        Ok(Self {
            lanes,
            maximum_width: width,
            row_count,
        })
    }

    pub(crate) const fn maximum_width(&self) -> ColumnarBatchWidth {
        self.maximum_width
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.row_count.get() as usize
    }

    pub(crate) const fn lanes(&self) -> &[&[SegmentV2Cell]] {
        self.lanes
    }
}

pub(crate) struct MonotoneSelection {
    retained: Vec<bool>,
    selected: usize,
}

impl MonotoneSelection {
    pub(crate) fn all(batch: &BorrowedLaneBatch<'_>) -> Self {
        Self {
            retained: vec![true; batch.row_count()],
            selected: batch.row_count(),
        }
    }

    pub(crate) fn retain(&mut self, mask: &[bool]) -> Result<(), ColumnarBatchBoundError> {
        if mask.len() != self.retained.len() {
            return Err(ColumnarBatchBoundError::SelectionLength);
        }
        self.selected = 0;
        for (retained, keep) in self.retained.iter_mut().zip(mask) {
            *retained &= *keep;
            self.selected += usize::from(*retained);
        }
        Ok(())
    }

    pub(crate) const fn selected(&self) -> usize {
        self.selected
    }

    pub(crate) const fn len(&self) -> usize {
        self.retained.len()
    }

    pub(crate) fn indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.retained
            .iter()
            .enumerate()
            .filter_map(|(index, retained)| retained.then_some(index))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarPredicateKernelError {
    InvalidLane,
    UnsupportedLane,
    UnsupportedOperator,
    RightHandTypeMismatch,
    InvalidWorkBound,
    WorkBoundExceeded,
    MissingField,
    UnsupportedPredicate,
    LaneIntegrity,
}

pub(crate) struct PredicateWorkCharge {
    remaining: usize,
}

impl PredicateWorkCharge {
    pub(crate) fn new(maximum: usize) -> Result<Self, ColumnarPredicateKernelError> {
        if maximum == 0 {
            return Err(ColumnarPredicateKernelError::InvalidWorkBound);
        }
        Ok(Self { remaining: maximum })
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.remaining
    }

    fn admits(&self, required: usize) -> Result<(), ColumnarPredicateKernelError> {
        if required > self.remaining {
            return Err(ColumnarPredicateKernelError::WorkBoundExceeded);
        }
        Ok(())
    }

    fn commit(&mut self, consumed: usize) {
        self.remaining -= consumed;
    }
}

pub(crate) struct ColumnarPredicateKernel {
    lane: usize,
    lane_kind: IntegerLaneKind,
    predicate: SegmentV2Predicate,
}

#[derive(Clone, Copy)]
enum IntegerLaneKind {
    I64,
    U64,
}

impl ColumnarPredicateKernel {
    pub(crate) fn new(
        lane: usize,
        lane_types: &[SegmentV2LogicalType],
        predicate: SegmentV2Predicate,
    ) -> Result<Self, ColumnarPredicateKernelError> {
        if lane_types.is_empty()
            || lane_types.len() > MAX_SEGMENT_V2_COLUMNS
            || lane >= lane_types.len()
        {
            return Err(ColumnarPredicateKernelError::InvalidLane);
        }
        let lane_kind = match lane_types[lane] {
            SegmentV2LogicalType::I64 => IntegerLaneKind::I64,
            SegmentV2LogicalType::U64 => IntegerLaneKind::U64,
            _ => return Err(ColumnarPredicateKernelError::UnsupportedLane),
        };
        match &predicate {
            SegmentV2Predicate::IsMissing => {
                return Err(ColumnarPredicateKernelError::UnsupportedOperator);
            }
            SegmentV2Predicate::IsNull | SegmentV2Predicate::IsPresent => {}
            SegmentV2Predicate::Equal(value)
            | SegmentV2Predicate::LessThan(value)
            | SegmentV2Predicate::LessThanOrEqual(value)
            | SegmentV2Predicate::GreaterThan(value)
            | SegmentV2Predicate::GreaterThanOrEqual(value) => {
                let matches_lane = matches!(
                    (lane_kind, value),
                    (IntegerLaneKind::I64, riffdb_types::CanonicalValue::I64(_))
                        | (IntegerLaneKind::U64, riffdb_types::CanonicalValue::U64(_))
                );
                if !matches_lane {
                    return Err(ColumnarPredicateKernelError::RightHandTypeMismatch);
                }
            }
        }
        Ok(Self {
            lane,
            lane_kind,
            predicate,
        })
    }

    pub(crate) fn apply(
        &self,
        batch: &BorrowedLaneBatch<'_>,
        selection: &mut MonotoneSelection,
        charge: &mut PredicateWorkCharge,
    ) -> Result<(), ColumnarPredicateKernelError> {
        if selection.len() != batch.row_count() {
            return Err(ColumnarPredicateKernelError::LaneIntegrity);
        }
        let lane = batch
            .lanes()
            .get(self.lane)
            .ok_or(ColumnarPredicateKernelError::InvalidLane)?;
        let consumed = selection.selected();
        charge.admits(consumed)?;
        let mut next = vec![false; batch.row_count()];
        let mut next_selected = 0_usize;
        for index in selection.indices() {
            if predicate_matches(self.lane_kind, &self.predicate, &lane[index])? {
                next[index] = true;
                next_selected += 1;
            }
        }
        selection.retained = next;
        selection.selected = next_selected;
        charge.commit(consumed);
        Ok(())
    }
}

fn predicate_matches(
    lane_kind: IntegerLaneKind,
    predicate: &SegmentV2Predicate,
    cell: &SegmentV2Cell,
) -> Result<bool, ColumnarPredicateKernelError> {
    match (lane_kind, cell) {
        (IntegerLaneKind::I64, SegmentV2Cell::Value(riffdb_types::CanonicalValue::I64(left))) => {
            signed_predicate_matches(*left, predicate)
        }
        (IntegerLaneKind::U64, SegmentV2Cell::Value(riffdb_types::CanonicalValue::U64(left))) => {
            unsigned_predicate_matches(*left, predicate)
        }
        (_, SegmentV2Cell::Value(_)) => Err(ColumnarPredicateKernelError::LaneIntegrity),
        (_, SegmentV2Cell::Missing) => match predicate {
            SegmentV2Predicate::Equal(_)
            | SegmentV2Predicate::LessThan(_)
            | SegmentV2Predicate::LessThanOrEqual(_)
            | SegmentV2Predicate::GreaterThan(_)
            | SegmentV2Predicate::GreaterThanOrEqual(_) => {
                Err(ColumnarPredicateKernelError::MissingField)
            }
            SegmentV2Predicate::IsNull | SegmentV2Predicate::IsPresent => Ok(false),
            SegmentV2Predicate::IsMissing => {
                Err(ColumnarPredicateKernelError::UnsupportedPredicate)
            }
        },
        (_, SegmentV2Cell::Null) => match predicate {
            SegmentV2Predicate::Equal(_) => Ok(false),
            SegmentV2Predicate::LessThan(_)
            | SegmentV2Predicate::LessThanOrEqual(_)
            | SegmentV2Predicate::GreaterThan(_)
            | SegmentV2Predicate::GreaterThanOrEqual(_) => {
                Err(ColumnarPredicateKernelError::UnsupportedPredicate)
            }
            SegmentV2Predicate::IsNull => Ok(true),
            SegmentV2Predicate::IsPresent => Ok(false),
            SegmentV2Predicate::IsMissing => {
                Err(ColumnarPredicateKernelError::UnsupportedPredicate)
            }
        },
    }
}

fn signed_predicate_matches(
    left: i64,
    predicate: &SegmentV2Predicate,
) -> Result<bool, ColumnarPredicateKernelError> {
    Ok(match predicate {
        SegmentV2Predicate::Equal(riffdb_types::CanonicalValue::I64(right)) => left == *right,
        SegmentV2Predicate::LessThan(riffdb_types::CanonicalValue::I64(right)) => left < *right,
        SegmentV2Predicate::LessThanOrEqual(riffdb_types::CanonicalValue::I64(right)) => {
            left <= *right
        }
        SegmentV2Predicate::GreaterThan(riffdb_types::CanonicalValue::I64(right)) => left > *right,
        SegmentV2Predicate::GreaterThanOrEqual(riffdb_types::CanonicalValue::I64(right)) => {
            left >= *right
        }
        SegmentV2Predicate::IsNull => false,
        // This is the sealed mapping to the scalar `IsNotNull` operator.
        SegmentV2Predicate::IsPresent => true,
        SegmentV2Predicate::Equal(_)
        | SegmentV2Predicate::LessThan(_)
        | SegmentV2Predicate::LessThanOrEqual(_)
        | SegmentV2Predicate::GreaterThan(_)
        | SegmentV2Predicate::GreaterThanOrEqual(_)
        | SegmentV2Predicate::IsMissing => {
            return Err(ColumnarPredicateKernelError::UnsupportedPredicate);
        }
    })
}

fn unsigned_predicate_matches(
    left: u64,
    predicate: &SegmentV2Predicate,
) -> Result<bool, ColumnarPredicateKernelError> {
    Ok(match predicate {
        SegmentV2Predicate::Equal(riffdb_types::CanonicalValue::U64(right)) => left == *right,
        SegmentV2Predicate::LessThan(riffdb_types::CanonicalValue::U64(right)) => left < *right,
        SegmentV2Predicate::LessThanOrEqual(riffdb_types::CanonicalValue::U64(right)) => {
            left <= *right
        }
        SegmentV2Predicate::GreaterThan(riffdb_types::CanonicalValue::U64(right)) => left > *right,
        SegmentV2Predicate::GreaterThanOrEqual(riffdb_types::CanonicalValue::U64(right)) => {
            left >= *right
        }
        SegmentV2Predicate::IsNull => false,
        // This is the sealed mapping to the scalar `IsNotNull` operator.
        SegmentV2Predicate::IsPresent => true,
        SegmentV2Predicate::Equal(_)
        | SegmentV2Predicate::LessThan(_)
        | SegmentV2Predicate::LessThanOrEqual(_)
        | SegmentV2Predicate::GreaterThan(_)
        | SegmentV2Predicate::GreaterThanOrEqual(_)
        | SegmentV2Predicate::IsMissing => {
            return Err(ColumnarPredicateKernelError::UnsupportedPredicate);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CanonicalValue, CurrencyCode, DecimalSpec, EnumTypeId};

    fn lane(width: usize, value: u64) -> Vec<SegmentV2Cell> {
        vec![SegmentV2Cell::Value(riffdb_types::CanonicalValue::U64(value)); width]
    }

    // Inert checkpoint coverage only. This does not discharge an ADR-0161
    // obligation or select the batch path for production execution.
    #[test]
    fn columnar_batch_width_lanes_and_selection_are_strictly_bounded() {
        for (compiler_maximum, row_bytes, ceiling, expected) in [
            (64, 1, 64, Ok(64)),
            (64, 2, 128, Ok(64)),
            (64, 2, 127, Err(ColumnarBatchBoundError::NoAdmissibleWidth)),
            (128, 1, 127, Ok(64)),
            (128, 1, 128, Ok(128)),
            (256, 1, 255, Ok(128)),
            (256, 1, 256, Ok(256)),
            (512, 1, 511, Ok(256)),
            (512, 1, 512, Ok(512)),
            (1_024, 1, 1_023, Ok(512)),
            (1_024, 1, 1_024, Ok(1_024)),
            (256, 16, 2_047, Ok(64)),
            (256, 16, 2_048, Ok(128)),
        ] {
            assert_eq!(
                ColumnarBatchWidth::choose(compiler_maximum, row_bytes, ceiling)
                    .map(ColumnarBatchWidth::get),
                expected
            );
        }
        for closed in CLOSED_BATCH_WIDTHS {
            assert_eq!(
                ColumnarBatchWidth::choose(closed, 1, usize::MAX)
                    .expect("closed maximum")
                    .get(),
                closed
            );
        }
        let width = ColumnarBatchWidth::choose(256, 16, 2_048).expect("bounded width");
        assert_eq!(width.get(), 128);
        assert_eq!(
            ColumnarBatchWidth::choose(255, 16, usize::MAX),
            Err(ColumnarBatchBoundError::InvalidCompilerMaximum)
        );
        assert_eq!(
            ColumnarBatchWidth::choose(64, usize::MAX, usize::MAX),
            Err(ColumnarBatchBoundError::NoAdmissibleWidth)
        );
        assert_eq!(
            ColumnarBatchWidth::choose(63, 0, 0),
            Err(ColumnarBatchBoundError::InvalidCompilerMaximum),
            "compiler identity is validated before dependent resource bounds"
        );
        assert_eq!(
            ColumnarBatchWidth::choose(64, 0, usize::MAX),
            Err(ColumnarBatchBoundError::InvalidByteBound)
        );
        assert_eq!(
            ColumnarBatchWidth::choose(64, 1, 0),
            Err(ColumnarBatchBoundError::InvalidByteBound)
        );

        let first = lane(width.get(), 1);
        let second = lane(width.get(), 2);
        let lanes = [first.as_slice(), second.as_slice()];
        let batch = BorrowedLaneBatch::new(width, &lanes).expect("equal borrowed lanes");
        assert_eq!(batch.maximum_width(), width);
        assert_eq!(batch.row_count(), width.get());
        assert_eq!(batch.lanes().len(), 2);
        let short = lane(width.get() - 1, 3);
        assert!(matches!(
            BorrowedLaneBatch::new(width, &[first.as_slice(), short.as_slice()]),
            Err(ColumnarBatchBoundError::LaneLength)
        ));
        let tail = lane(63, 4);
        let tail_lanes = [tail.as_slice()];
        let tail_batch = BorrowedLaneBatch::new(width, &tail_lanes).expect("partial tail batch");
        assert_eq!(tail_batch.maximum_width(), width);
        assert_eq!(tail_batch.row_count(), 63);
        assert!(matches!(
            BorrowedLaneBatch::new(width, &[]),
            Err(ColumnarBatchBoundError::LaneCount)
        ));
        let empty: Vec<SegmentV2Cell> = Vec::new();
        assert!(matches!(
            BorrowedLaneBatch::new(width, &[empty.as_slice()]),
            Err(ColumnarBatchBoundError::LaneLength)
        ));
        let over_width = lane(width.get() + 1, 5);
        assert!(matches!(
            BorrowedLaneBatch::new(width, &[over_width.as_slice()]),
            Err(ColumnarBatchBoundError::LaneLength)
        ));
        let one = lane(1, 6);
        let maximum_lanes = vec![one.as_slice(); MAX_SEGMENT_V2_COLUMNS];
        let maximum_lane_batch =
            BorrowedLaneBatch::new(width, &maximum_lanes).expect("exact maximum lane count");
        assert_eq!(maximum_lane_batch.lanes().len(), MAX_SEGMENT_V2_COLUMNS);
        let too_many_lanes = vec![one.as_slice(); MAX_SEGMENT_V2_COLUMNS + 1];
        assert!(matches!(
            BorrowedLaneBatch::new(width, &too_many_lanes),
            Err(ColumnarBatchBoundError::LaneCount)
        ));

        let mut selection = MonotoneSelection::all(&batch);
        let all_indices = (0..batch.row_count()).collect::<Vec<_>>();
        assert_eq!(selection.selected(), batch.row_count());
        assert_eq!(selection.indices().collect::<Vec<_>>(), all_indices);
        let mut first_mask = vec![true; batch.row_count()];
        first_mask[1] = false;
        first_mask[7] = false;
        selection.retain(&first_mask).expect("first stage");
        let after_first = (0..batch.row_count())
            .filter(|index| *index != 1 && *index != 7)
            .collect::<Vec<_>>();
        assert_eq!(selection.selected(), after_first.len());
        assert_eq!(selection.indices().collect::<Vec<_>>(), after_first);
        selection
            .retain(&vec![true; batch.row_count()])
            .expect("later permissive stage");
        assert_eq!(selection.selected(), after_first.len());
        assert_eq!(selection.indices().collect::<Vec<_>>(), after_first);
        let restrictive = (0..batch.row_count())
            .map(|index| index % 2 == 0)
            .collect::<Vec<_>>();
        selection
            .retain(&restrictive)
            .expect("later restrictive stage");
        let after_restrictive = after_first
            .into_iter()
            .filter(|index| index % 2 == 0)
            .collect::<Vec<_>>();
        assert_eq!(selection.selected(), after_restrictive.len());
        assert_eq!(selection.indices().collect::<Vec<_>>(), after_restrictive);
        let before_error = selection.indices().collect::<Vec<_>>();
        assert_eq!(
            selection.retain(&vec![true; batch.row_count() - 1]),
            Err(ColumnarBatchBoundError::SelectionLength)
        );
        assert_eq!(selection.indices().collect::<Vec<_>>(), before_error);
    }

    // Inert predicate-kernel checkpoint only. The complete cross-type,
    // cross-operator differential obligation remains pending.
    #[test]
    fn columnar_predicate_kernel_matches_independent_optional_state_oracle() {
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        let cases = [
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(2)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(3)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(2)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(3)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(4)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(3)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(u64::MIN)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(u64::MAX)),
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::IsNull,
                Ok(false),
            ),
            (SegmentV2Cell::Null, SegmentV2Predicate::IsNull, Ok(true)),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::IsNull,
                Ok(false),
            ),
            (
                SegmentV2Cell::Missing,
                SegmentV2Predicate::IsPresent,
                Ok(false),
            ),
            (
                SegmentV2Cell::Null,
                SegmentV2Predicate::IsPresent,
                Ok(false),
            ),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(3)),
                SegmentV2Predicate::IsPresent,
                Ok(true),
            ),
        ];
        for (cell, predicate, expected) in cases {
            let cells = [cell];
            let lanes = [cells.as_slice()];
            let batch = BorrowedLaneBatch::new(width, &lanes).expect("one-row lane");
            let kernel = ColumnarPredicateKernel::new(0, &[SegmentV2LogicalType::U64], predicate)
                .expect("sealed U64 predicate");
            let mut selection = MonotoneSelection::all(&batch);
            let mut charge = PredicateWorkCharge::new(1).expect("one evaluation");
            let result = kernel.apply(&batch, &mut selection, &mut charge);
            match expected {
                Ok(retained) => {
                    assert_eq!(result, Ok(()));
                    assert_eq!(
                        selection.indices().collect::<Vec<_>>(),
                        if retained { vec![0] } else { vec![] }
                    );
                    assert_eq!(charge.remaining(), 0);
                }
                Err(error) => {
                    assert_eq!(result, Err(error));
                    assert_eq!(selection.indices().collect::<Vec<_>>(), vec![0]);
                    assert_eq!(charge.remaining(), 1);
                }
            }
        }

        let equality = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::U64],
            SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
        )
        .expect("equality kernel");
        let cells = vec![
            SegmentV2Cell::Value(CanonicalValue::I64(9)),
            SegmentV2Cell::Value(CanonicalValue::U64(3)),
        ];
        let lanes = [cells.as_slice()];
        let batch = BorrowedLaneBatch::new(width, &lanes).expect("poisoned lane fixture");
        let mut prefiltered = MonotoneSelection::all(&batch);
        let mut policy_mask = vec![true; batch.row_count()];
        policy_mask[0] = false;
        prefiltered.retain(&policy_mask).expect("policy stage");
        let mut charge = PredicateWorkCharge::new(batch.row_count()).expect("bounded charge");
        equality
            .apply(&batch, &mut prefiltered, &mut charge)
            .expect("predicate after policy");
        assert_eq!(
            prefiltered.selected(),
            1,
            "predicate neither evaluates nor re-adds the policy-denied poison row"
        );
        assert_eq!(
            charge.remaining(),
            1,
            "only policy-admitted rows consume predicate work"
        );

        let mut insufficient_selection = MonotoneSelection::all(&batch);
        let before = insufficient_selection.indices().collect::<Vec<_>>();
        let mut insufficient = PredicateWorkCharge::new(1).expect("small cap");
        assert_eq!(
            equality.apply(&batch, &mut insufficient_selection, &mut insufficient),
            Err(ColumnarPredicateKernelError::WorkBoundExceeded)
        );
        assert_eq!(insufficient_selection.indices().collect::<Vec<_>>(), before);
        assert_eq!(insufficient.remaining(), 1);

        let ordered_errors = vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Value(CanonicalValue::I64(9)),
        ];
        let ordered_error_lanes = [ordered_errors.as_slice()];
        let ordered_error_batch =
            BorrowedLaneBatch::new(width, &ordered_error_lanes).expect("ordered errors");
        let mut ordered_error_selection = MonotoneSelection::all(&ordered_error_batch);
        let mut ordered_error_charge = PredicateWorkCharge::new(2).expect("exact cap");
        assert_eq!(
            equality.apply(
                &ordered_error_batch,
                &mut ordered_error_selection,
                &mut ordered_error_charge,
            ),
            Err(ColumnarPredicateKernelError::MissingField),
            "the first selected row's semantic error wins"
        );
        assert_eq!(
            ordered_error_selection.indices().collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(ordered_error_charge.remaining(), 2);

        let later_poison = vec![
            SegmentV2Cell::Value(CanonicalValue::U64(2)),
            SegmentV2Cell::Value(CanonicalValue::I64(9)),
        ];
        let later_poison_lanes = [later_poison.as_slice()];
        let later_poison_batch =
            BorrowedLaneBatch::new(width, &later_poison_lanes).expect("later poison");
        let mut atomic_selection = MonotoneSelection::all(&later_poison_batch);
        let atomic_before = atomic_selection.indices().collect::<Vec<_>>();
        let mut atomic_charge = PredicateWorkCharge::new(2).expect("exact cap");
        assert_eq!(
            equality.apply(
                &later_poison_batch,
                &mut atomic_selection,
                &mut atomic_charge,
            ),
            Err(ColumnarPredicateKernelError::LaneIntegrity)
        );
        assert_eq!(
            atomic_selection.indices().collect::<Vec<_>>(),
            atomic_before
        );
        assert_eq!(atomic_charge.remaining(), 2);

        assert_eq!(
            PredicateWorkCharge::new(0).err(),
            Some(ColumnarPredicateKernelError::InvalidWorkBound)
        );
        for predicate in [
            SegmentV2Predicate::Equal(CanonicalValue::I64(3)),
            SegmentV2Predicate::LessThan(CanonicalValue::I64(3)),
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(3)),
            SegmentV2Predicate::GreaterThan(CanonicalValue::I64(3)),
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(3)),
        ] {
            assert_eq!(
                ColumnarPredicateKernel::new(0, &[SegmentV2LogicalType::U64], predicate).err(),
                Some(ColumnarPredicateKernelError::RightHandTypeMismatch)
            );
        }
        assert_eq!(
            ColumnarPredicateKernel::new(
                1,
                &[SegmentV2LogicalType::Bool],
                SegmentV2Predicate::Equal(CanonicalValue::I64(3)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::InvalidLane),
            "lane identity precedes dependent predicate validation"
        );

        let decimal = DecimalSpec::new(8, 2).expect("decimal type");
        let currency = CurrencyCode::new(b"USD").expect("currency type");
        for unsupported_lane in [
            SegmentV2LogicalType::Bool,
            SegmentV2LogicalType::String,
            SegmentV2LogicalType::Bytes,
            SegmentV2LogicalType::Timestamp,
            SegmentV2LogicalType::Date,
            SegmentV2LogicalType::Uuid,
            SegmentV2LogicalType::Enum(EnumTypeId::new(1).expect("enum type")),
            SegmentV2LogicalType::Decimal(decimal),
            SegmentV2LogicalType::Money {
                currency,
                amount: decimal,
            },
        ] {
            assert_eq!(
                ColumnarPredicateKernel::new(
                    0,
                    &[unsupported_lane],
                    SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                )
                .err(),
                Some(ColumnarPredicateKernelError::UnsupportedLane)
            );
        }
        let excessive_lane_shape = vec![SegmentV2LogicalType::U64; MAX_SEGMENT_V2_COLUMNS + 1];
        assert_eq!(
            ColumnarPredicateKernel::new(0, &excessive_lane_shape, SegmentV2Predicate::IsMissing,)
                .err(),
            Some(ColumnarPredicateKernelError::InvalidLane),
            "invalid lane shape precedes operator validation"
        );

        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &[SegmentV2LogicalType::Bool],
                SegmentV2Predicate::IsMissing,
            )
            .err(),
            Some(ColumnarPredicateKernelError::UnsupportedLane),
            "unsupported lane precedes unsupported operator"
        );
        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &[SegmentV2LogicalType::U64],
                SegmentV2Predicate::IsMissing,
            )
            .err(),
            Some(ColumnarPredicateKernelError::UnsupportedOperator)
        );

        let missing_lane_kernel = ColumnarPredicateKernel::new(
            1,
            &[SegmentV2LogicalType::U64, SegmentV2LogicalType::U64],
            SegmentV2Predicate::IsNull,
        )
        .expect("second sealed lane");
        let valid_cells = [SegmentV2Cell::Value(CanonicalValue::U64(1))];
        let valid_lanes = [valid_cells.as_slice()];
        let one_lane_batch = BorrowedLaneBatch::new(width, &valid_lanes).expect("one lane");
        let mut runtime_missing_selection = MonotoneSelection::all(&one_lane_batch);
        let mut runtime_missing_charge = PredicateWorkCharge::new(1).expect("one evaluation");
        assert_eq!(
            missing_lane_kernel.apply(
                &one_lane_batch,
                &mut runtime_missing_selection,
                &mut runtime_missing_charge,
            ),
            Err(ColumnarPredicateKernelError::InvalidLane)
        );
        assert_eq!(
            runtime_missing_selection.indices().collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(runtime_missing_charge.remaining(), 1);

        let mut wrong_length_selection = MonotoneSelection::all(&batch);
        let wrong_length_before = wrong_length_selection.indices().collect::<Vec<_>>();
        let mut wrong_length_charge = PredicateWorkCharge::new(2).expect("two evaluations");
        assert_eq!(
            equality.apply(
                &one_lane_batch,
                &mut wrong_length_selection,
                &mut wrong_length_charge,
            ),
            Err(ColumnarPredicateKernelError::LaneIntegrity)
        );
        assert_eq!(
            wrong_length_selection.indices().collect::<Vec<_>>(),
            wrong_length_before
        );
        assert_eq!(wrong_length_charge.remaining(), 2);

        for predicate in [
            SegmentV2Predicate::Equal(CanonicalValue::U64(9)),
            SegmentV2Predicate::LessThan(CanonicalValue::U64(9)),
            SegmentV2Predicate::IsNull,
            SegmentV2Predicate::IsPresent,
        ] {
            let kernel = ColumnarPredicateKernel::new(0, &[SegmentV2LogicalType::U64], predicate)
                .expect("supported operator");
            let wrong_cells = [SegmentV2Cell::Value(CanonicalValue::I64(9))];
            let wrong_lanes = [wrong_cells.as_slice()];
            let wrong_batch = BorrowedLaneBatch::new(width, &wrong_lanes).expect("wrong value");
            let mut wrong_selection = MonotoneSelection::all(&wrong_batch);
            let mut wrong_charge = PredicateWorkCharge::new(1).expect("one evaluation");
            assert_eq!(
                kernel.apply(&wrong_batch, &mut wrong_selection, &mut wrong_charge),
                Err(ColumnarPredicateKernelError::LaneIntegrity)
            );
            assert_eq!(wrong_selection.indices().collect::<Vec<_>>(), vec![0]);
            assert_eq!(wrong_charge.remaining(), 1);
        }

        let cumulative_cells = (0_u64..8)
            .map(|value| SegmentV2Cell::Value(CanonicalValue::U64(value)))
            .collect::<Vec<_>>();
        let cumulative_lanes = [cumulative_cells.as_slice()];
        let cumulative_batch =
            BorrowedLaneBatch::new(width, &cumulative_lanes).expect("cumulative batch");
        let lower = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::U64],
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(4)),
        )
        .expect("lower kernel");
        let upper = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::U64],
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(6)),
        )
        .expect("upper kernel");
        let present = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::U64],
            SegmentV2Predicate::IsPresent,
        )
        .expect("present kernel");
        let mut cumulative_selection = MonotoneSelection::all(&cumulative_batch);
        let mut cumulative_charge = PredicateWorkCharge::new(12).expect("cumulative cap");
        lower
            .apply(
                &cumulative_batch,
                &mut cumulative_selection,
                &mut cumulative_charge,
            )
            .expect("first kernel");
        assert_eq!(
            cumulative_selection.indices().collect::<Vec<_>>(),
            vec![4, 5, 6, 7]
        );
        assert_eq!(cumulative_charge.remaining(), 4);
        upper
            .apply(
                &cumulative_batch,
                &mut cumulative_selection,
                &mut cumulative_charge,
            )
            .expect("second kernel");
        assert_eq!(
            cumulative_selection.indices().collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert_eq!(cumulative_charge.remaining(), 0);
        let cumulative_before = cumulative_selection.indices().collect::<Vec<_>>();
        assert_eq!(
            present.apply(
                &cumulative_batch,
                &mut cumulative_selection,
                &mut cumulative_charge,
            ),
            Err(ColumnarPredicateKernelError::WorkBoundExceeded)
        );
        assert_eq!(
            cumulative_selection.indices().collect::<Vec<_>>(),
            cumulative_before
        );
        assert_eq!(cumulative_charge.remaining(), 0);
    }

    fn assert_integer_case(
        width: ColumnarBatchWidth,
        logical_type: SegmentV2LogicalType,
        cell: SegmentV2Cell,
        predicate: SegmentV2Predicate,
        expected: Result<bool, ColumnarPredicateKernelError>,
    ) {
        let one_cell = [cell];
        let lanes = [one_cell.as_slice()];
        let batch = BorrowedLaneBatch::new(width, &lanes).expect("one-row integer batch");
        let kernel = ColumnarPredicateKernel::new(0, &[logical_type], predicate.clone())
            .expect("sealed integer predicate");
        let mut selection = MonotoneSelection::all(&batch);
        let before = selection.indices().collect::<Vec<_>>();
        let mut charge = PredicateWorkCharge::new(1).expect("one evaluation");
        let actual = kernel.apply(&batch, &mut selection, &mut charge);
        match expected {
            Ok(retained) => {
                assert_eq!(actual, Ok(()), "predicate={predicate:?}");
                assert_eq!(selection.selected(), usize::from(retained));
                assert_eq!(charge.remaining(), 0);
            }
            Err(error) => {
                assert_eq!(actual, Err(error), "predicate={predicate:?}");
                assert_eq!(selection.indices().collect::<Vec<_>>(), before);
                assert_eq!(charge.remaining(), 1);
            }
        }
    }

    // Inert integer predicate checkpoint only. Literal expected results keep
    // this proof independent of the implementation-shaped match tree. The
    // cross-crate differential obligation remains pending.
    #[test]
    fn columnar_integer_predicate_kernel_matches_literal_ordering_table() {
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        let cases = vec![
            // Signed equality and strict/inclusive order at negative, zero,
            // and both representable edges.
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                SegmentV2Predicate::Equal(CanonicalValue::I64(i64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::Equal(CanonicalValue::I64(i64::MIN)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(0)),
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(-1)),
                SegmentV2Predicate::LessThan(CanonicalValue::I64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(0)),
                SegmentV2Predicate::LessThan(CanonicalValue::I64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                SegmentV2Predicate::LessThan(CanonicalValue::I64(i64::MIN)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(i64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(0)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(1)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(1)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::I64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(0)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::I64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::I64(i64::MAX)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(i64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(0)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(-1)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                SegmentV2Predicate::LessThan(CanonicalValue::I64(i64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::I64(i64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::Equal(CanonicalValue::I64(i64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(i64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(i64::MIN)),
                Ok(true),
            ),
            // Symmetric unsigned edge witnesses ensure lane-kind dispatch does
            // not regress the existing U64 behavior.
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(u64::MIN)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(u64::MIN)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(1)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(0)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(u64::MAX)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(u64::MAX)),
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(u64::MIN)),
                Ok(true),
            ),
        ];
        for (logical_type, cell, predicate, expected) in cases {
            assert_integer_case(width, logical_type, cell, predicate, expected);
        }
    }

    #[test]
    fn columnar_integer_predicate_states_types_and_constructor_precedence_are_exact() {
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        let cases = vec![
            // Optional-state results are identical for both admitted kinds.
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::LessThan(CanonicalValue::I64(0)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::LessThan(CanonicalValue::I64(0)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::IsNull,
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::IsNull,
                Ok(true),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(-1)),
                SegmentV2Predicate::IsNull,
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::IsPresent,
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::IsPresent,
                Ok(false),
            ),
            (
                SegmentV2LogicalType::I64,
                SegmentV2Cell::Value(CanonicalValue::I64(-1)),
                SegmentV2Predicate::IsPresent,
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::Equal(CanonicalValue::U64(0)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::Equal(CanonicalValue::U64(0)),
                Ok(false),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::LessThan(CanonicalValue::U64(0)),
                Err(ColumnarPredicateKernelError::MissingField),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::LessThan(CanonicalValue::U64(0)),
                Err(ColumnarPredicateKernelError::UnsupportedPredicate),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Missing,
                SegmentV2Predicate::IsNull,
                Ok(false),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Null,
                SegmentV2Predicate::IsNull,
                Ok(true),
            ),
            (
                SegmentV2LogicalType::U64,
                SegmentV2Cell::Value(CanonicalValue::U64(1)),
                SegmentV2Predicate::IsPresent,
                Ok(true),
            ),
        ];
        for (logical_type, cell, predicate, expected) in cases {
            assert_integer_case(width, logical_type, cell, predicate, expected);
        }

        for (logical_type, wrong_rhs) in [
            (SegmentV2LogicalType::I64, CanonicalValue::U64(0)),
            (SegmentV2LogicalType::U64, CanonicalValue::I64(0)),
        ] {
            for predicate in [
                SegmentV2Predicate::Equal(wrong_rhs.clone()),
                SegmentV2Predicate::LessThan(wrong_rhs.clone()),
                SegmentV2Predicate::LessThanOrEqual(wrong_rhs.clone()),
                SegmentV2Predicate::GreaterThan(wrong_rhs.clone()),
                SegmentV2Predicate::GreaterThanOrEqual(wrong_rhs.clone()),
            ] {
                assert_eq!(
                    ColumnarPredicateKernel::new(
                        0,
                        std::slice::from_ref(&logical_type),
                        predicate,
                    )
                    .err(),
                    Some(ColumnarPredicateKernelError::RightHandTypeMismatch)
                );
            }
            assert_eq!(
                ColumnarPredicateKernel::new(
                    1,
                    &[logical_type],
                    SegmentV2Predicate::Equal(wrong_rhs)
                )
                .err(),
                Some(ColumnarPredicateKernelError::InvalidLane),
                "invalid lane identity precedes RHS validation"
            );
        }
        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &[SegmentV2LogicalType::Bool],
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::UnsupportedLane),
            "unsupported lane precedes RHS validation"
        );
        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &[],
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::InvalidLane),
            "empty lane shape precedes RHS validation"
        );
        let excessive = vec![SegmentV2LogicalType::I64; MAX_SEGMENT_V2_COLUMNS + 1];
        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &excessive,
                SegmentV2Predicate::Equal(CanonicalValue::U64(0)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::InvalidLane),
            "over-bound lane shape precedes RHS validation"
        );
        for logical_type in [SegmentV2LogicalType::I64, SegmentV2LogicalType::U64] {
            assert_eq!(
                ColumnarPredicateKernel::new(0, &[logical_type], SegmentV2Predicate::IsMissing)
                    .err(),
                Some(ColumnarPredicateKernelError::UnsupportedOperator)
            );
        }
    }

    #[test]
    fn columnar_integer_predicate_failures_are_atomic_and_mixed_lanes_are_monotone() {
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        for (logical_type, good, poison, binary) in [
            (
                SegmentV2LogicalType::I64,
                CanonicalValue::I64(0),
                CanonicalValue::U64(0),
                SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
            ),
            (
                SegmentV2LogicalType::U64,
                CanonicalValue::U64(0),
                CanonicalValue::I64(0),
                SegmentV2Predicate::Equal(CanonicalValue::U64(0)),
            ),
        ] {
            for (predicate, retained) in [
                (binary, true),
                (SegmentV2Predicate::LessThan(good.clone()), false),
                (SegmentV2Predicate::IsNull, false),
                (SegmentV2Predicate::IsPresent, true),
            ] {
                let cells = [
                    SegmentV2Cell::Value(good.clone()),
                    SegmentV2Cell::Value(poison.clone()),
                ];
                let lanes = [cells.as_slice()];
                let batch = BorrowedLaneBatch::new(width, &lanes).expect("poisoned integer lane");
                let kernel =
                    ColumnarPredicateKernel::new(0, std::slice::from_ref(&logical_type), predicate)
                        .expect("sealed integer predicate");
                let mut selection = MonotoneSelection::all(&batch);
                let before = selection.indices().collect::<Vec<_>>();
                let mut charge = PredicateWorkCharge::new(2).expect("exact work");
                assert_eq!(
                    kernel.apply(&batch, &mut selection, &mut charge),
                    Err(ColumnarPredicateKernelError::LaneIntegrity)
                );
                assert_eq!(selection.indices().collect::<Vec<_>>(), before);
                assert_eq!(charge.remaining(), 2);

                let mut skipped = MonotoneSelection::all(&batch);
                skipped.retain(&[true, false]).expect("policy mask");
                let mut skipped_charge = PredicateWorkCharge::new(1).expect("one admitted row");
                kernel
                    .apply(&batch, &mut skipped, &mut skipped_charge)
                    .expect("deselected poison is not evaluated");
                assert_eq!(
                    skipped.indices().collect::<Vec<_>>(),
                    if retained { vec![0] } else { vec![] }
                );
                assert_eq!(skipped_charge.remaining(), 0);
            }
        }

        for (logical_type, rhs, poison) in [
            (
                SegmentV2LogicalType::I64,
                CanonicalValue::I64(0),
                CanonicalValue::U64(0),
            ),
            (
                SegmentV2LogicalType::U64,
                CanonicalValue::U64(0),
                CanonicalValue::I64(0),
            ),
        ] {
            let equality =
                ColumnarPredicateKernel::new(0, &[logical_type], SegmentV2Predicate::Equal(rhs))
                    .expect("sealed integer equality");
            for (cells, expected) in [
                (
                    vec![SegmentV2Cell::Missing, SegmentV2Cell::Value(poison.clone())],
                    ColumnarPredicateKernelError::MissingField,
                ),
                (
                    vec![SegmentV2Cell::Value(poison.clone()), SegmentV2Cell::Missing],
                    ColumnarPredicateKernelError::LaneIntegrity,
                ),
            ] {
                let lanes = [cells.as_slice()];
                let batch = BorrowedLaneBatch::new(width, &lanes).expect("ordered error batch");
                let mut selection = MonotoneSelection::all(&batch);
                let before = selection.indices().collect::<Vec<_>>();
                let mut charge = PredicateWorkCharge::new(2).expect("exact work");
                assert_eq!(
                    equality.apply(&batch, &mut selection, &mut charge),
                    Err(expected)
                );
                assert_eq!(selection.indices().collect::<Vec<_>>(), before);
                assert_eq!(charge.remaining(), 2);
            }
        }

        let signed = [
            SegmentV2Cell::Value(CanonicalValue::I64(-1)),
            SegmentV2Cell::Value(CanonicalValue::I64(0)),
            SegmentV2Cell::Value(CanonicalValue::I64(1)),
            SegmentV2Cell::Value(CanonicalValue::I64(2)),
        ];
        let unsigned = [
            SegmentV2Cell::Value(CanonicalValue::U64(9)),
            SegmentV2Cell::Value(CanonicalValue::U64(6)),
            SegmentV2Cell::Value(CanonicalValue::U64(5)),
            SegmentV2Cell::Value(CanonicalValue::U64(7)),
        ];
        let lanes = [signed.as_slice(), unsigned.as_slice()];
        let batch = BorrowedLaneBatch::new(width, &lanes).expect("two integer lanes");
        let signed_lower = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::I64, SegmentV2LogicalType::U64],
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(0)),
        )
        .expect("signed lower bound");
        let unsigned_upper = ColumnarPredicateKernel::new(
            1,
            &[SegmentV2LogicalType::I64, SegmentV2LogicalType::U64],
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(6)),
        )
        .expect("unsigned upper bound");
        let mut selection = MonotoneSelection::all(&batch);
        let mut charge = PredicateWorkCharge::new(7).expect("exact sequential work");
        signed_lower
            .apply(&batch, &mut selection, &mut charge)
            .expect("signed phase");
        assert_eq!(selection.indices().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(charge.remaining(), 3);
        unsigned_upper
            .apply(&batch, &mut selection, &mut charge)
            .expect("unsigned phase");
        assert_eq!(selection.indices().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(charge.remaining(), 0);
    }
}
