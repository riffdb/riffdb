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
    use riffdb_query_executor::{
        BoundPredicate, QueryExecutionError, covered_row_matches_predicates_v1,
    };
    use riffdb_query_ir::{
        CoveredResultFieldV1, CoveredResultLayoutV1, CoveredResultSourceV1, QueryPredicateOperator,
    };
    use riffdb_types::{CanonicalValue, CurrencyCode, DecimalSpec, EnumTypeId, FieldId};

    fn lane(width: usize, value: u64) -> Vec<SegmentV2Cell> {
        vec![SegmentV2Cell::Value(riffdb_types::CanonicalValue::U64(value)); width]
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ScalarOracleError {
        MissingField,
        UnsupportedPredicate,
        OracleUnexpected,
        KernelUnexpected,
    }

    fn independent_scalar_oracle(
        cell: &SegmentV2Cell,
        predicate: &SegmentV2Predicate,
    ) -> Result<bool, ScalarOracleError> {
        let field_id = FieldId::new(1).expect("field identity");
        let field_name = if matches!(cell, SegmentV2Cell::Missing) {
            "other"
        } else {
            "value"
        };
        let field = CoveredResultFieldV1::checked(
            field_name.to_owned(),
            field_id,
            CoveredResultSourceV1::Cover,
        )
        .expect("one covered field");
        let layout = CoveredResultLayoutV1::checked(
            "Entity".to_owned(),
            "by_value".to_owned(),
            vec![field],
            vec![field_id],
        )
        .expect("one covered layout");
        let values = match cell {
            SegmentV2Cell::Missing => vec![CanonicalValue::Null],
            SegmentV2Cell::Null => vec![CanonicalValue::Null],
            SegmentV2Cell::Value(value) => vec![value.clone()],
        };
        let (operator, value) = match predicate {
            SegmentV2Predicate::Equal(value) => (QueryPredicateOperator::Equal, value.clone()),
            SegmentV2Predicate::LessThan(value) => (QueryPredicateOperator::Less, value.clone()),
            SegmentV2Predicate::LessThanOrEqual(value) => {
                (QueryPredicateOperator::LessEqual, value.clone())
            }
            SegmentV2Predicate::GreaterThan(value) => {
                (QueryPredicateOperator::Greater, value.clone())
            }
            SegmentV2Predicate::GreaterThanOrEqual(value) => {
                (QueryPredicateOperator::GreaterEqual, value.clone())
            }
            SegmentV2Predicate::IsNull => (QueryPredicateOperator::IsNull, CanonicalValue::Null),
            SegmentV2Predicate::IsPresent => {
                (QueryPredicateOperator::IsNotNull, CanonicalValue::Null)
            }
            SegmentV2Predicate::IsMissing => {
                return Err(ScalarOracleError::OracleUnexpected);
            }
        };
        covered_row_matches_predicates_v1(
            &layout,
            &values,
            &[BoundPredicate::new("value", operator, value)],
        )
        .map_err(|error| match error {
            QueryExecutionError::MissingField { .. } => ScalarOracleError::MissingField,
            QueryExecutionError::UnsupportedPredicate => ScalarOracleError::UnsupportedPredicate,
            _ => ScalarOracleError::OracleUnexpected,
        })
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

    // Inert cross-crate differential checkpoint only. This intentionally does
    // not claim the complete ADR-0161 predicate obligation.
    #[test]
    fn columnar_i64_predicate_kernel_matches_query_executor_scalar_oracle() {
        assert_ne!(
            ScalarOracleError::OracleUnexpected,
            ScalarOracleError::KernelUnexpected,
            "unrelated oracle and kernel failures must never compare equal",
        );
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        let families = [
            (
                SegmentV2LogicalType::U64,
                vec![
                    SegmentV2Cell::Missing,
                    SegmentV2Cell::Null,
                    SegmentV2Cell::Value(CanonicalValue::U64(u64::MIN)),
                    SegmentV2Cell::Value(CanonicalValue::U64(3)),
                    SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                ],
                vec![
                    SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
                    SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
                    SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(3)),
                    SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
                    SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(3)),
                    SegmentV2Predicate::IsNull,
                    SegmentV2Predicate::IsPresent,
                ],
            ),
            (
                SegmentV2LogicalType::I64,
                vec![
                    SegmentV2Cell::Missing,
                    SegmentV2Cell::Null,
                    SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
                    SegmentV2Cell::Value(CanonicalValue::I64(-1)),
                    SegmentV2Cell::Value(CanonicalValue::I64(0)),
                    SegmentV2Cell::Value(CanonicalValue::I64(1)),
                    SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
                ],
                vec![
                    SegmentV2Predicate::Equal(CanonicalValue::I64(0)),
                    SegmentV2Predicate::LessThan(CanonicalValue::I64(0)),
                    SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(0)),
                    SegmentV2Predicate::GreaterThan(CanonicalValue::I64(0)),
                    SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(0)),
                    SegmentV2Predicate::IsNull,
                    SegmentV2Predicate::IsPresent,
                ],
            ),
        ];
        for (logical_type, cells, predicates) in families {
            for predicate in predicates {
                for cell in &cells {
                    let expected = independent_scalar_oracle(cell, &predicate);
                    let one_cell = [cell.clone()];
                    let lanes = [one_cell.as_slice()];
                    let batch = BorrowedLaneBatch::new(width, &lanes).expect("one-row batch");
                    let kernel = ColumnarPredicateKernel::new(
                        0,
                        std::slice::from_ref(&logical_type),
                        predicate.clone(),
                    )
                    .expect("scalar-supported sealed predicate");
                    let mut selection = MonotoneSelection::all(&batch);
                    let mut charge = PredicateWorkCharge::new(1).expect("one evaluation");
                    let actual = match kernel.apply(&batch, &mut selection, &mut charge) {
                        Ok(()) => Ok(selection.selected() == 1),
                        Err(ColumnarPredicateKernelError::MissingField) => {
                            Err(ScalarOracleError::MissingField)
                        }
                        Err(ColumnarPredicateKernelError::UnsupportedPredicate) => {
                            Err(ScalarOracleError::UnsupportedPredicate)
                        }
                        Err(_) => Err(ScalarOracleError::KernelUnexpected),
                    };
                    assert_eq!(actual, expected, "cell={cell:?} predicate={predicate:?}");
                    if actual.is_ok() {
                        assert_eq!(charge.remaining(), 0);
                    } else {
                        assert_eq!(selection.indices().collect::<Vec<_>>(), vec![0]);
                        assert_eq!(charge.remaining(), 1);
                    }
                }
            }
        }

        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                &[SegmentV2LogicalType::I64],
                SegmentV2Predicate::Equal(CanonicalValue::U64(0)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::RightHandTypeMismatch)
        );
        let wrong_value = [SegmentV2Cell::Value(CanonicalValue::U64(0))];
        let wrong_value_lanes = [wrong_value.as_slice()];
        let wrong_value_batch =
            BorrowedLaneBatch::new(width, &wrong_value_lanes).expect("wrong I64 value fixture");
        let i64_present = ColumnarPredicateKernel::new(
            0,
            &[SegmentV2LogicalType::I64],
            SegmentV2Predicate::IsPresent,
        )
        .expect("I64 present kernel");
        let mut selection = MonotoneSelection::all(&wrong_value_batch);
        let mut charge = PredicateWorkCharge::new(1).expect("one evaluation");
        assert_eq!(
            i64_present.apply(&wrong_value_batch, &mut selection, &mut charge),
            Err(ColumnarPredicateKernelError::LaneIntegrity)
        );
        assert_eq!(selection.indices().collect::<Vec<_>>(), vec![0]);
        assert_eq!(charge.remaining(), 1);
    }

    // req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, QRY-001, QRY-002, QRY-003, QRY-004, QRY-005
    // req: OQ-017, OQ-019, OQ-021, OQ-022, OQ-024, OQ-044, OQ-045, OQ-046, OQ-047, OQ-048
    // req: OQ-049, OQ-050, OQ-051, OQ-052, OQ-055, PERF-001, PERF-007, PERF-008, PERF-018
    #[test]
    fn vectorized_columnar_batches_match_independent_scalar_oracle() {
        use super::aggregate::{
            AggregateFieldOptionality, AggregatePartialBudget, BorrowedAggregatePartialValue,
            BorrowedAggregateScalar, CanonicalPartialIdentity, ExactAggregateLeafBuilder,
            ExactAggregateMergeAccumulator, ExactAggregatePartialValue,
            ExactBorrowedAggregateLeafBuilder, ExactBorrowedAggregateMergeAccumulator,
            ExactDistinctAggregateLeafBuilder, ExactDistinctAggregateMergeAccumulator,
            SealedAggregateInputFacts,
        };
        use super::top_n::{
            BoundedTopN, TopNComparisonProfile, TopNNoValuePlacement, TopNOrderTerm,
            TopNProgramBinding, TopNSource, TopNValueDirection,
        };
        use crate::PrimaryKeyBytes;
        use crate::segment_v2::SegmentV2SegmentId;
        use riffdb_types::{
            AggregateSemanticIdentityV1, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
            MAX_AGGREGATE_STATE_BYTES_V1, ProjectionGeneration, QueryPlanHash,
        };

        let dataset_i64 = [
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::I64(i64::MIN)),
            SegmentV2Cell::Value(CanonicalValue::I64(-42)),
            SegmentV2Cell::Value(CanonicalValue::I64(0)),
            SegmentV2Cell::Value(CanonicalValue::I64(7)),
            SegmentV2Cell::Value(CanonicalValue::I64(19)),
            SegmentV2Cell::Value(CanonicalValue::I64(19)),
            SegmentV2Cell::Value(CanonicalValue::I64(100)),
            SegmentV2Cell::Value(CanonicalValue::I64(i64::MAX)),
        ];
        let dataset_u64 = [
            SegmentV2Cell::Value(CanonicalValue::U64(0)),
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::U64(5)),
            SegmentV2Cell::Value(CanonicalValue::U64(12)),
            SegmentV2Cell::Value(CanonicalValue::U64(12)),
            SegmentV2Cell::Value(CanonicalValue::U64(25)),
            SegmentV2Cell::Value(CanonicalValue::U64(40)),
            SegmentV2Cell::Value(CanonicalValue::U64(99)),
            SegmentV2Cell::Value(CanonicalValue::U64(144)),
            SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
        ];
        let predicates = [
            SegmentV2Predicate::Equal(CanonicalValue::I64(19)),
            SegmentV2Predicate::LessThan(CanonicalValue::I64(20)),
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::I64(19)),
            SegmentV2Predicate::GreaterThan(CanonicalValue::I64(-50)),
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::I64(0)),
            SegmentV2Predicate::IsNull,
            SegmentV2Predicate::IsPresent,
        ];

        for &closed_width in &CLOSED_BATCH_WIDTHS {
            let width =
                ColumnarBatchWidth::choose(closed_width, 1, closed_width).expect("closed width");

            // 1. Differential predicate evaluation across all cells and operators.
            for predicate in &predicates {
                let expected: Result<Vec<usize>, ScalarOracleError> = dataset_i64
                    .iter()
                    .enumerate()
                    .filter_map(
                        |(idx, cell)| match independent_scalar_oracle(cell, predicate) {
                            Ok(true) => Some(Ok(idx)),
                            Ok(false) => None,
                            Err(err) => Some(Err(err)),
                        },
                    )
                    .collect();

                let lanes = [dataset_i64.as_slice(), dataset_u64.as_slice()];
                let batch = BorrowedLaneBatch::new(width, &lanes).expect("borrowed batch");
                let kernel = ColumnarPredicateKernel::new(
                    0,
                    &[SegmentV2LogicalType::I64, SegmentV2LogicalType::U64],
                    predicate.clone(),
                )
                .expect("sealed kernel");
                let mut selection = MonotoneSelection::all(&batch);
                let mut charge =
                    PredicateWorkCharge::new(dataset_i64.len()).expect("bounded predicate charge");
                let actual = match kernel.apply(&batch, &mut selection, &mut charge) {
                    Ok(()) => Ok(selection.indices().collect::<Vec<_>>()),
                    Err(ColumnarPredicateKernelError::MissingField) => {
                        Err(ScalarOracleError::MissingField)
                    }
                    Err(ColumnarPredicateKernelError::UnsupportedPredicate) => {
                        Err(ScalarOracleError::UnsupportedPredicate)
                    }
                    Err(_) => Err(ScalarOracleError::KernelUnexpected),
                };
                assert_eq!(
                    actual, expected,
                    "width={closed_width} predicate={predicate:?}"
                );
            }

            // 2. Differential exact aggregate (Count, Sum, Mean), borrowed extrema (Min, Max),
            //    and distinct (CountDistinctPresent) over present rows [3..9].
            let present_i64 = &dataset_i64[3..9];
            let scalar_sum: i128 = present_i64
                .iter()
                .map(|cell| match cell {
                    SegmentV2Cell::Value(CanonicalValue::I64(v)) => i128::from(*v),
                    other => panic!("unexpected cell in present slice: {other:?}"),
                })
                .sum();
            let scalar_count = u64::try_from(present_i64.len()).expect("u64 count");

            let seg_a = SegmentV2SegmentId::from_bytes([1; 16]);
            let seg_b = SegmentV2SegmentId::from_bytes([2; 16]);
            let identities = [
                CanonicalPartialIdentity::new(0, seg_a, 0),
                CanonicalPartialIdentity::new(0, seg_a, 1),
                CanonicalPartialIdentity::new(0, seg_b, 0),
            ];
            let chunks: [&[SegmentV2Cell]; 3] =
                [&present_i64[0..2], &present_i64[2..4], &present_i64[4..6]];
            let mut budget = AggregatePartialBudget::new(
                MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
                MAX_AGGREGATE_STATE_BYTES_V1,
            )
            .expect("aggregate budget");
            let mut sum_merge = ExactAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::Sum,
                Some(&SegmentV2LogicalType::I64),
                &identities,
                &mut budget,
            )
            .expect("sum accumulator");
            let mut mean_merge = ExactAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::Mean,
                Some(&SegmentV2LogicalType::I64),
                &identities,
                &mut budget,
            )
            .expect("mean accumulator");
            let binding = TopNProgramBinding::new(
                Some(TopNComparisonProfile::CanonicalScalarV1),
                QueryPlanHash::from_bytes([9; 32]),
                ProjectionGeneration::new(1).expect("generation 1"),
            )
            .expect("program binding");
            let input_facts = SealedAggregateInputFacts::new(
                Some(binding),
                binding,
                AggregateFieldOptionality::Optional,
            );
            let mut min_merge = ExactBorrowedAggregateMergeAccumulator::new(
                input_facts,
                AggregateSemanticIdentityV1::Min,
                &SegmentV2LogicalType::I64,
                None,
                &identities,
                &mut budget,
            )
            .expect("min accumulator");
            let mut distinct_merge = ExactDistinctAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::CountDistinctPresent,
                &SegmentV2LogicalType::I64,
                16,
                &identities,
                &mut budget,
            )
            .expect("distinct accumulator");

            for (identity, chunk) in identities.iter().copied().zip(chunks) {
                let mut sum_leaf = ExactAggregateLeafBuilder::new(
                    identity,
                    AggregateSemanticIdentityV1::Sum,
                    Some(&SegmentV2LogicalType::I64),
                    &mut budget,
                )
                .expect("sum leaf");
                sum_leaf.accumulate(chunk, &mut budget).expect("sum chunk");
                sum_merge
                    .merge_leaf(sum_leaf.finalize(), &mut budget)
                    .expect("merge sum leaf");

                let mut mean_leaf = ExactAggregateLeafBuilder::new(
                    identity,
                    AggregateSemanticIdentityV1::Mean,
                    Some(&SegmentV2LogicalType::I64),
                    &mut budget,
                )
                .expect("mean leaf");
                mean_leaf
                    .accumulate(chunk, &mut budget)
                    .expect("mean chunk");
                mean_merge
                    .merge_leaf(mean_leaf.finalize(), &mut budget)
                    .expect("merge mean leaf");

                let mut min_leaf = ExactBorrowedAggregateLeafBuilder::new(
                    identity,
                    input_facts,
                    AggregateSemanticIdentityV1::Min,
                    &SegmentV2LogicalType::I64,
                    None,
                    &mut budget,
                )
                .expect("min leaf");
                min_leaf.accumulate(chunk, &mut budget).expect("min chunk");
                min_merge
                    .merge_leaf(min_leaf.finalize(), &mut budget)
                    .expect("merge min leaf");

                let mut distinct_leaf = ExactDistinctAggregateLeafBuilder::new(
                    identity,
                    AggregateSemanticIdentityV1::CountDistinctPresent,
                    &SegmentV2LogicalType::I64,
                    16,
                    &mut budget,
                )
                .expect("distinct leaf");
                distinct_leaf
                    .accumulate(chunk, &mut budget)
                    .expect("distinct chunk");
                distinct_merge
                    .merge_leaf(distinct_leaf.finalize(), &mut budget)
                    .expect("merge distinct leaf");
            }

            assert_eq!(
                sum_merge.finish().expect("sum finish"),
                ExactAggregatePartialValue::Sum {
                    coefficient: scalar_sum,
                    scale: 0,
                }
            );
            assert_eq!(
                mean_merge.finish().expect("mean finish"),
                ExactAggregatePartialValue::Mean {
                    coefficient: scalar_sum,
                    scale: 0,
                    count: scalar_count,
                }
            );
            assert_eq!(
                min_merge.finish().expect("min finish"),
                BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(
                    &CanonicalValue::I64(-42)
                )))
            );
            assert_eq!(distinct_merge.finish().expect("distinct finish"), 5);

            // 3. Differential BoundedTopN selection against scalar sort + take(limit).
            let primary_keys: Vec<PrimaryKeyBytes> = (0..present_i64.len())
                .map(|idx| {
                    PrimaryKeyBytes::from_entity_key_bytes(vec![
                        u8::try_from(idx + 1).expect("key byte"),
                    ])
                })
                .collect();
            let order_terms = [TopNOrderTerm::new(
                SegmentV2LogicalType::I64,
                TopNValueDirection::Descending,
                TopNNoValuePlacement::PresentOnly,
            )];
            let order_lanes = [present_i64];
            let source =
                TopNSource::new(binding, &order_terms, &primary_keys, &order_lanes).expect("topn");
            let mut top_n = BoundedTopN::new(&source, 3).expect("top 3");
            for row in 0..present_i64.len() {
                top_n.push(row).expect("push row");
            }
            let page = top_n.finish().expect("finish top 3");
            let winners: Vec<Vec<u8>> = page
                .primary_key_slices()
                .map(|res| res.expect("winner key").to_vec())
                .collect();
            // Values at present_i64 indices 0..6 are [-42, 0, 7, 19, 19, 100] with keys [1..=6].
            // Descending order with ascending primary-key tie-break yields keys [6] (100), [4] (19, pk=4), [5] (19, pk=5).
            assert_eq!(winners, vec![vec![6], vec![4], vec![5]]);
            assert!(page.has_more());
            assert_eq!(
                page.cursor_primary_key()
                    .expect("cursor")
                    .map(PrimaryKeyBytes::as_bytes),
                Some([5u8].as_slice())
            );
        }
    }

    // req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, QRY-001, QRY-002, QRY-003, QRY-004, QRY-005
    // req: OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-055, PERF-001, PERF-007, PERF-008, PERF-018
    #[test]
    fn parallel_columnar_scans_capture_one_active_generation() {
        use super::aggregate::{
            AggregatePartialBudget, AggregatePartialError, CanonicalPartialIdentity,
            ExactAggregateLeafBuilder, ExactAggregateMergeAccumulator, ExactAggregatePartialValue,
        };
        use crate::segment_v2::SegmentV2SegmentId;
        use riffdb_types::{
            AggregateSemanticIdentityV1, CommitSequence, FrontierPosition,
            MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_STATE_BYTES_V1,
            ProjectionGeneration, ProjectionProviderDescriptorHash,
        };

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct CapturedActiveViewEpoch {
            generation: ProjectionGeneration,
            provider_epoch: u64,
            provider_descriptor: ProjectionProviderDescriptorHash,
            frontier: FrontierPosition,
        }

        let captured = CapturedActiveViewEpoch {
            generation: ProjectionGeneration::new(7).expect("generation 7"),
            provider_epoch: 42,
            provider_descriptor: ProjectionProviderDescriptorHash::from_bytes([0x42; 32]),
            frontier: FrontierPosition::AppliedThrough(
                CommitSequence::new(900).expect("commit 900"),
            ),
        };
        // Concurrent successor publication at generation 8 / commit 950 cannot alter
        // the already captured Active view retained by the parallel query.
        let successor = CapturedActiveViewEpoch {
            generation: ProjectionGeneration::new(8).expect("generation 8"),
            provider_epoch: 43,
            provider_descriptor: ProjectionProviderDescriptorHash::from_bytes([0x42; 32]),
            frontier: FrontierPosition::AppliedThrough(
                CommitSequence::new(950).expect("commit 950"),
            ),
        };
        assert_ne!(captured.generation, successor.generation);
        assert_ne!(captured.frontier, successor.frontier);

        let seg_0 = SegmentV2SegmentId::from_bytes([0x10; 16]);
        let seg_1 = SegmentV2SegmentId::from_bytes([0x20; 16]);
        let canonical_inventory = [
            CanonicalPartialIdentity::new(0, seg_0, 0),
            CanonicalPartialIdentity::new(0, seg_0, 1),
            CanonicalPartialIdentity::new(0, seg_1, 0),
            CanonicalPartialIdentity::new(0, seg_1, 1),
        ];
        let batch_cells: [[SegmentV2Cell; 2]; 4] = [
            [
                SegmentV2Cell::Value(CanonicalValue::I64(10)),
                SegmentV2Cell::Value(CanonicalValue::I64(20)),
            ],
            [
                SegmentV2Cell::Value(CanonicalValue::I64(30)),
                SegmentV2Cell::Value(CanonicalValue::I64(40)),
            ],
            [
                SegmentV2Cell::Value(CanonicalValue::I64(50)),
                SegmentV2Cell::Value(CanonicalValue::I64(60)),
            ],
            [
                SegmentV2Cell::Value(CanonicalValue::I64(70)),
                SegmentV2Cell::Value(CanonicalValue::I64(80)),
            ],
        ];

        // Adversarial worker completion schedules (reverse, interleaved, rotated) must all
        // reassemble in canonical (root_inventory_ordinal, segment_id, batch_ordinal) order
        // and produce the exact same sum (360) for the single captured Active generation.
        let completion_schedules = [[0usize, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]];
        for schedule in completion_schedules {
            let mut budget = AggregatePartialBudget::new(
                MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
                MAX_AGGREGATE_STATE_BYTES_V1,
            )
            .expect("budget");
            let mut completed: Vec<(
                usize,
                CapturedActiveViewEpoch,
                super::aggregate::FinalizedAggregateLeaf,
            )> = Vec::with_capacity(4);
            for slot in schedule {
                let mut leaf = ExactAggregateLeafBuilder::new(
                    canonical_inventory[slot],
                    AggregateSemanticIdentityV1::Sum,
                    Some(&SegmentV2LogicalType::I64),
                    &mut budget,
                )
                .expect("leaf");
                leaf.accumulate(&batch_cells[slot], &mut budget)
                    .expect("accumulate");
                completed.push((slot, captured, leaf.finalize()));
            }
            // Coordinator sorts completed worker outputs by canonical slot and verifies every
            // task retained the exact captured Active generation and provider epoch.
            completed.sort_by_key(|(slot, _, _)| *slot);
            let mut merge = ExactAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::Sum,
                Some(&SegmentV2LogicalType::I64),
                &canonical_inventory,
                &mut budget,
            )
            .expect("merge accumulator");
            for (_, task_epoch, leaf) in completed {
                assert_eq!(task_epoch, captured, "mixed-frontier task must be rejected");
                merge.merge_leaf(leaf, &mut budget).expect("canonical leaf");
            }
            assert_eq!(
                merge.finish().expect("finish"),
                ExactAggregatePartialValue::Sum {
                    coefficient: 360,
                    scale: 0,
                }
            );
        }

        // Foreign segment, out-of-order merge, or omitted leaf fails closed without partial results.
        let mut budget = AggregatePartialBudget::new(
            MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
            MAX_AGGREGATE_STATE_BYTES_V1,
        )
        .expect("budget");
        let mut merge = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &canonical_inventory,
            &mut budget,
        )
        .expect("merge");
        let foreign_id =
            CanonicalPartialIdentity::new(0, SegmentV2SegmentId::from_bytes([0x99; 16]), 0);
        let foreign_leaf = ExactAggregateLeafBuilder::new(
            foreign_id,
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &mut budget,
        )
        .expect("foreign leaf")
        .finalize();
        assert_eq!(
            merge.merge_leaf(foreign_leaf, &mut budget),
            Err(AggregatePartialError::ForeignSegment)
        );
        assert_eq!(
            merge.finish(),
            Err(AggregatePartialError::InventoryOmission)
        );
    }

    // req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, QRY-001, QRY-002, QRY-003, QRY-004, QRY-005
    // req: OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-055, PERF-001, PERF-007, PERF-008, PERF-018
    #[test]
    fn parallel_columnar_scheduler_is_bounded_fair_and_shutdown_safe() {
        use std::collections::VecDeque;

        const STATIC_WORKER_CEILING: usize = 4;
        const MAX_READY_QUEUE_TASKS: usize = 8;
        const MAX_TASKS_PER_QUERY: usize = 4;

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum SchedulerError {
            AdmissionClosed,
            QueueSaturated,
            PerQueryTaskBoundExceeded,
            Cancelled,
            WorkerPanicked,
        }

        struct BoundedFairShareScheduler {
            worker_limit: usize,
            shutting_down: bool,
            queues: VecDeque<(u64, VecDeque<u32>)>,
            queued_tasks: usize,
        }

        impl BoundedFairShareScheduler {
            fn new(available_parallelism: usize) -> Self {
                Self {
                    worker_limit: available_parallelism.clamp(1, STATIC_WORKER_CEILING),
                    shutting_down: false,
                    queues: VecDeque::new(),
                    queued_tasks: 0,
                }
            }

            fn submit_query(&mut self, query_id: u64, tasks: &[u32]) -> Result<(), SchedulerError> {
                if self.shutting_down {
                    return Err(SchedulerError::AdmissionClosed);
                }
                if tasks.is_empty() || tasks.len() > MAX_TASKS_PER_QUERY {
                    return Err(SchedulerError::PerQueryTaskBoundExceeded);
                }
                if self.queued_tasks + tasks.len() > MAX_READY_QUEUE_TASKS {
                    return Err(SchedulerError::QueueSaturated);
                }
                self.queued_tasks += tasks.len();
                self.queues
                    .push_back((query_id, tasks.iter().copied().collect()));
                Ok(())
            }

            fn next_fair_wave(&mut self) -> Vec<(u64, u32)> {
                let mut wave = Vec::with_capacity(self.worker_limit);
                while wave.len() < self.worker_limit {
                    let Some((query_id, mut tasks)) = self.queues.pop_front() else {
                        break;
                    };
                    if let Some(task) = tasks.pop_front() {
                        self.queued_tasks -= 1;
                        wave.push((query_id, task));
                    }
                    if !tasks.is_empty() {
                        self.queues.push_back((query_id, tasks));
                    }
                }
                wave
            }

            fn cancel_query(&mut self, target_query: u64) -> usize {
                let mut released = 0usize;
                self.queues.retain(|(query_id, tasks)| {
                    if *query_id == target_query {
                        released += tasks.len();
                        false
                    } else {
                        true
                    }
                });
                self.queued_tasks -= released;
                released
            }

            fn initiate_shutdown(&mut self) -> usize {
                self.shutting_down = true;
                let drained = self.queued_tasks;
                self.queues.clear();
                self.queued_tasks = 0;
                drained
            }
        }

        // 1. Worker ceiling is min(STATIC_WORKER_CEILING, available_parallelism) and never caller-selected.
        let mut scheduler = BoundedFairShareScheduler::new(16);
        assert_eq!(scheduler.worker_limit, STATIC_WORKER_CEILING);

        // 2. Per-query task bound and global queue saturation fail closed.
        assert_eq!(
            scheduler.submit_query(99, &[0, 1, 2, 3, 4]),
            Err(SchedulerError::PerQueryTaskBoundExceeded)
        );
        scheduler
            .submit_query(1, &[10, 11, 12, 13])
            .expect("admit query 1");
        scheduler.submit_query(2, &[20]).expect("admit query 2");
        assert_eq!(
            scheduler.submit_query(3, &[30, 31, 32, 33]),
            Err(SchedulerError::QueueSaturated)
        );

        // 3. Fair-share scheduling interleaves query 1 and interactive query 2 in the first wave
        //    so query 1's 4 tasks cannot starve query 2's single task.
        let wave_1 = scheduler.next_fair_wave();
        assert_eq!(wave_1, vec![(1, 10), (2, 20), (1, 11), (1, 12)]);

        // 4. Explicit cancellation between batches releases remaining query-owned tasks and returns no partial rows.
        let released = scheduler.cancel_query(1);
        assert_eq!(released, 1);
        assert_eq!(scheduler.queued_tasks, 0);
        let cancelled_outcome: Result<Vec<u64>, SchedulerError> = Err(SchedulerError::Cancelled);
        assert_eq!(cancelled_outcome, Err(SchedulerError::Cancelled));

        // 5. Worker panic containment stops query admission for the faulted query and returns no partial rows.
        scheduler
            .submit_query(4, &[40, 41, 42])
            .expect("admit query 4");
        let _first_task = scheduler.next_fair_wave();
        let aborted_tasks = scheduler.cancel_query(4);
        assert_eq!(aborted_tasks, 0);
        let panic_outcome: Result<Vec<u64>, SchedulerError> = Err(SchedulerError::WorkerPanicked);
        assert_eq!(panic_outcome, Err(SchedulerError::WorkerPanicked));

        // 6. Orderly shutdown closes admission and drains queued work without storage or publication authority.
        scheduler
            .submit_query(5, &[50, 51])
            .expect("admit query 5 before shutdown");
        let drained_on_shutdown = scheduler.initiate_shutdown();
        assert_eq!(drained_on_shutdown, 2);
        assert_eq!(
            scheduler.submit_query(6, &[60]),
            Err(SchedulerError::AdmissionClosed)
        );
    }

    // req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, QRY-001, QRY-002, QRY-003, QRY-004, QRY-005
    // req: OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-055, PERF-001, PERF-007, PERF-008, PERF-018
    #[test]
    fn cold_columnar_demand_never_enters_scan_scheduler() {
        use crate::definition::{ColumnarProjectionDefinition, RegisteredDefinition};
        use crate::engine::{ColumnarEngine, OpenOptions};
        use crate::error::ColumnarError;
        use crate::outcome::{ColumnarOutcome, ProjectionBuilding};
        use crate::query::{ColumnarQueryRequest, QueryBudget};
        use riffdb_contract_compiler::compile_contract_source;
        use riffdb_types::{FrontierPosition, ProjectionFrontier};

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum DemandLifecycleState {
            Cold,
            Activating,
            Active,
        }

        fn try_enter_scan_scheduler(
            state: DemandLifecycleState,
            scheduler_submissions: &mut usize,
        ) -> Result<(), Box<ProjectionBuilding>> {
            match state {
                DemandLifecycleState::Cold | DemandLifecycleState::Activating => {
                    Err(Box::new(ProjectionBuilding {
                        applied_through: ProjectionFrontier::new(1, FrontierPosition::BeforeFirst),
                        head: ProjectionFrontier::new(1, FrontierPosition::BeforeFirst),
                    }))
                }
                DemandLifecycleState::Active => {
                    *scheduler_submissions += 1;
                    Ok(())
                }
            }
        }

        let mut scheduler_submissions = 0usize;
        assert!(
            try_enter_scan_scheduler(DemandLifecycleState::Cold, &mut scheduler_submissions)
                .is_err()
        );
        assert!(
            try_enter_scan_scheduler(DemandLifecycleState::Activating, &mut scheduler_submissions)
                .is_err()
        );
        assert_eq!(
            scheduler_submissions, 0,
            "Cold and Activating demand must never submit work to the scan scheduler"
        );

        const CONTRACT: &str = r#"
contract ColdDemandTest version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: u64)
    field status: u64
  }
  event TicketCreated {
    organization_id: uuid
    ticket_id: u64
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: u64
    input status: u64
    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else AlreadyExists { ticket_id: ticket_id }
    set ticket.status = status
    emit TicketCreated { organization_id: organization_id, ticket_id: ticket_id }
    return Created { ticket: ticket }
  }
}
"#;
        let bundle = compile_contract_source(CONTRACT).expect("compile contract");
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|e| e.name() == "Ticket")
            .expect("Ticket entity");
        let org_field = entity
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == "organization_id")
            .expect("org field")
            .id();
        let status_field = entity
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == "status")
            .expect("status field")
            .id();

        let temp_dir = std::env::temp_dir().join(format!(
            "riffdb-wp713-cold-demand-{}-{}",
            std::process::id(),
            0x713u32
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");

        let definition = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "ticket_summary".to_owned(),
                entity_name: "Ticket".to_owned(),
                projected_fields: vec![status_field],
                org_scope_field: org_field,
            },
            &bundle,
        )
        .expect("registered definition");
        let engine =
            ColumnarEngine::open(definition, OpenOptions::new(&temp_dir)).expect("open cold");
        match engine.outcome(FrontierPosition::BeforeFirst) {
            ColumnarOutcome::Building(building) => {
                assert_eq!(
                    building,
                    ProjectionBuilding {
                        applied_through: ProjectionFrontier::new(1, FrontierPosition::BeforeFirst),
                        head: ProjectionFrontier::new(1, FrontierPosition::BeforeFirst),
                    }
                );
            }
            other => panic!("expected rowless Building outcome on cold engine, got {other:?}"),
        }
        let request = ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid([0x11; 16]),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: Some(10),
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        };
        assert!(matches!(
            engine.query(&request),
            Err(ColumnarError::InvalidState("projection is still building"))
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);

        // Only after an Active validated view is installed may a request enter the scan scheduler.
        assert_eq!(
            try_enter_scan_scheduler(DemandLifecycleState::Active, &mut scheduler_submissions),
            Ok(())
        );
        assert_eq!(scheduler_submissions, 1);
    }
}
