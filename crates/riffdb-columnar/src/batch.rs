//! Inert bounded mechanics for compiler-sealed V2 columnar batches.

use std::cmp::Ordering;
use std::num::NonZeroU16;

use crate::segment_v2::{
    MAX_SEGMENT_V2_COLUMNS, SegmentV2Cell, SegmentV2LogicalType, SegmentV2Predicate, compare_values,
};

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
    InvalidPredicateType,
    InvalidWorkBound,
    WorkBoundExceeded,
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

    fn consume(&mut self, required: usize) -> Result<(), ColumnarPredicateKernelError> {
        self.remaining = self
            .remaining
            .checked_sub(required)
            .ok_or(ColumnarPredicateKernelError::WorkBoundExceeded)?;
        Ok(())
    }
}

pub(crate) struct ColumnarPredicateKernel {
    lane: usize,
    logical_type: SegmentV2LogicalType,
    predicate: SegmentV2Predicate,
}

impl ColumnarPredicateKernel {
    pub(crate) fn new(
        lane: usize,
        lane_count: usize,
        logical_type: SegmentV2LogicalType,
        predicate: SegmentV2Predicate,
    ) -> Result<Self, ColumnarPredicateKernelError> {
        if lane >= lane_count || lane_count == 0 || lane_count > MAX_SEGMENT_V2_COLUMNS {
            return Err(ColumnarPredicateKernelError::InvalidLane);
        }
        if let Some(value) = predicate_value(&predicate)
            && !logical_type.accepts(value)
        {
            return Err(ColumnarPredicateKernelError::InvalidPredicateType);
        }
        Ok(Self {
            lane,
            logical_type,
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
        charge.consume(selection.selected())?;
        let mut matches = vec![false; batch.row_count()];
        for index in selection.indices() {
            matches[index] = predicate_matches(&self.logical_type, &self.predicate, &lane[index])?;
        }
        selection
            .retain(&matches)
            .map_err(|_| ColumnarPredicateKernelError::LaneIntegrity)
    }
}

fn predicate_value(predicate: &SegmentV2Predicate) -> Option<&riffdb_types::CanonicalValue> {
    match predicate {
        SegmentV2Predicate::Equal(value)
        | SegmentV2Predicate::LessThan(value)
        | SegmentV2Predicate::LessThanOrEqual(value)
        | SegmentV2Predicate::GreaterThan(value)
        | SegmentV2Predicate::GreaterThanOrEqual(value) => Some(value),
        SegmentV2Predicate::IsMissing
        | SegmentV2Predicate::IsNull
        | SegmentV2Predicate::IsPresent => None,
    }
}

fn predicate_matches(
    logical_type: &SegmentV2LogicalType,
    predicate: &SegmentV2Predicate,
    cell: &SegmentV2Cell,
) -> Result<bool, ColumnarPredicateKernelError> {
    match (cell, predicate) {
        (SegmentV2Cell::Missing, SegmentV2Predicate::IsMissing)
        | (SegmentV2Cell::Null, SegmentV2Predicate::IsNull)
        | (SegmentV2Cell::Value(_), SegmentV2Predicate::IsPresent) => Ok(true),
        (SegmentV2Cell::Value(left), predicate) => {
            let Some(right) = predicate_value(predicate) else {
                return Ok(false);
            };
            let ordering = compare_values(logical_type, left, right)
                .map_err(|_| ColumnarPredicateKernelError::LaneIntegrity)?;
            Ok(match predicate {
                SegmentV2Predicate::Equal(_) => ordering == Ordering::Equal,
                SegmentV2Predicate::LessThan(_) => ordering == Ordering::Less,
                SegmentV2Predicate::LessThanOrEqual(_) => ordering != Ordering::Greater,
                SegmentV2Predicate::GreaterThan(_) => ordering == Ordering::Greater,
                SegmentV2Predicate::GreaterThanOrEqual(_) => ordering != Ordering::Less,
                SegmentV2Predicate::IsMissing
                | SegmentV2Predicate::IsNull
                | SegmentV2Predicate::IsPresent => false,
            })
        }
        (SegmentV2Cell::Missing | SegmentV2Cell::Null, _) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::CanonicalValue;

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

    fn independent_u64_scalar_match(cell: &SegmentV2Cell, predicate: &SegmentV2Predicate) -> bool {
        match (cell, predicate) {
            (SegmentV2Cell::Missing, SegmentV2Predicate::IsMissing)
            | (SegmentV2Cell::Null, SegmentV2Predicate::IsNull)
            | (SegmentV2Cell::Value(_), SegmentV2Predicate::IsPresent) => true,
            (
                SegmentV2Cell::Value(CanonicalValue::U64(left)),
                SegmentV2Predicate::Equal(CanonicalValue::U64(right)),
            ) => left == right,
            (
                SegmentV2Cell::Value(CanonicalValue::U64(left)),
                SegmentV2Predicate::LessThan(CanonicalValue::U64(right)),
            ) => left < right,
            (
                SegmentV2Cell::Value(CanonicalValue::U64(left)),
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(right)),
            ) => left <= right,
            (
                SegmentV2Cell::Value(CanonicalValue::U64(left)),
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(right)),
            ) => left > right,
            (
                SegmentV2Cell::Value(CanonicalValue::U64(left)),
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(right)),
            ) => left >= right,
            _ => false,
        }
    }

    // Inert predicate-kernel checkpoint only. The complete cross-type,
    // cross-operator differential obligation remains pending.
    #[test]
    fn columnar_predicate_kernel_matches_independent_optional_state_oracle() {
        let width = ColumnarBatchWidth::choose(64, 1, 64).expect("closed width");
        let cells = vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::U64(1)),
            SegmentV2Cell::Value(CanonicalValue::U64(2)),
            SegmentV2Cell::Value(CanonicalValue::U64(3)),
            SegmentV2Cell::Value(CanonicalValue::U64(4)),
            SegmentV2Cell::Value(CanonicalValue::U64(5)),
            SegmentV2Cell::Value(CanonicalValue::U64(6)),
        ];
        let lanes = [cells.as_slice()];
        let batch = BorrowedLaneBatch::new(width, &lanes).expect("bounded lane batch");
        let predicates = [
            SegmentV2Predicate::IsMissing,
            SegmentV2Predicate::IsNull,
            SegmentV2Predicate::IsPresent,
            SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
            SegmentV2Predicate::LessThan(CanonicalValue::U64(3)),
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(3)),
            SegmentV2Predicate::GreaterThan(CanonicalValue::U64(3)),
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(3)),
        ];
        for predicate in predicates {
            let kernel = ColumnarPredicateKernel::new(
                0,
                batch.lanes().len(),
                SegmentV2LogicalType::U64,
                predicate.clone(),
            )
            .expect("compiler-sealed predicate");
            let mut selection = MonotoneSelection::all(&batch);
            let mut charge = PredicateWorkCharge::new(batch.row_count()).expect("exact work cap");
            kernel
                .apply(&batch, &mut selection, &mut charge)
                .expect("exact kernel");
            let expected = cells
                .iter()
                .enumerate()
                .filter_map(|(index, cell)| {
                    independent_u64_scalar_match(cell, &predicate).then_some(index)
                })
                .collect::<Vec<_>>();
            assert_eq!(selection.indices().collect::<Vec<_>>(), expected);
            assert_eq!(charge.remaining(), 0);
        }

        let equality = ColumnarPredicateKernel::new(
            0,
            1,
            SegmentV2LogicalType::U64,
            SegmentV2Predicate::Equal(CanonicalValue::U64(3)),
        )
        .expect("equality kernel");
        let mut prefiltered = MonotoneSelection::all(&batch);
        let mut policy_mask = vec![true; batch.row_count()];
        policy_mask[4] = false;
        prefiltered.retain(&policy_mask).expect("policy stage");
        let mut charge = PredicateWorkCharge::new(batch.row_count()).expect("bounded charge");
        equality
            .apply(&batch, &mut prefiltered, &mut charge)
            .expect("predicate after policy");
        assert_eq!(
            prefiltered.selected(),
            0,
            "predicate cannot re-add a policy-denied row"
        );
        assert_eq!(
            charge.remaining(),
            1,
            "only policy-admitted rows consume predicate work"
        );

        let mut unchanged = MonotoneSelection::all(&batch);
        let before = unchanged.indices().collect::<Vec<_>>();
        let mut insufficient = PredicateWorkCharge::new(batch.row_count() - 1).expect("small cap");
        assert_eq!(
            equality.apply(&batch, &mut unchanged, &mut insufficient),
            Err(ColumnarPredicateKernelError::WorkBoundExceeded)
        );
        assert_eq!(unchanged.indices().collect::<Vec<_>>(), before);
        assert_eq!(insufficient.remaining(), batch.row_count() - 1);
        assert_eq!(
            PredicateWorkCharge::new(0).err(),
            Some(ColumnarPredicateKernelError::InvalidWorkBound)
        );
        assert_eq!(
            ColumnarPredicateKernel::new(
                0,
                1,
                SegmentV2LogicalType::U64,
                SegmentV2Predicate::Equal(CanonicalValue::I64(3)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::InvalidPredicateType)
        );
        assert_eq!(
            ColumnarPredicateKernel::new(
                1,
                1,
                SegmentV2LogicalType::U64,
                SegmentV2Predicate::Equal(CanonicalValue::I64(3)),
            )
            .err(),
            Some(ColumnarPredicateKernelError::InvalidLane),
            "lane identity precedes dependent predicate validation"
        );

        let short_cells = vec![SegmentV2Cell::Value(CanonicalValue::U64(1)); 2];
        let short_lanes = [short_cells.as_slice()];
        let short_batch = BorrowedLaneBatch::new(width, &short_lanes).expect("short batch");
        let mut wrong_selection = MonotoneSelection::all(&batch);
        let mut enough = PredicateWorkCharge::new(batch.row_count()).expect("bounded charge");
        assert_eq!(
            equality.apply(&short_batch, &mut wrong_selection, &mut enough),
            Err(ColumnarPredicateKernelError::LaneIntegrity)
        );
        assert_eq!(enough.remaining(), batch.row_count());
    }
}
