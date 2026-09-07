//! Inert bounded mechanics for compiler-sealed V2 columnar batches.

use std::num::NonZeroU16;

use crate::segment_v2::{MAX_SEGMENT_V2_COLUMNS, SegmentV2Cell};

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

    pub(crate) fn indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.retained
            .iter()
            .enumerate()
            .filter_map(|(index, retained)| retained.then_some(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane(width: usize, value: u64) -> Vec<SegmentV2Cell> {
        vec![SegmentV2Cell::Value(riffdb_types::CanonicalValue::U64(value)); width]
    }

    // req: QRY-001, OQ-019, OQ-022, OQ-050, OQ-051, OQ-055, PERF-008
    #[test]
    fn columnar_batch_width_lanes_and_selection_are_strictly_bounded() {
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

        let mut selection = MonotoneSelection::all(&batch);
        let mut first_mask = vec![true; batch.row_count()];
        first_mask[1] = false;
        first_mask[7] = false;
        selection.retain(&first_mask).expect("first stage");
        assert_eq!(selection.selected(), batch.row_count() - 2);
        selection
            .retain(&vec![true; batch.row_count()])
            .expect("later permissive stage");
        assert_eq!(selection.selected(), batch.row_count() - 2);
        assert_eq!(selection.indices().nth(1), Some(2));
        let before_error = selection.indices().collect::<Vec<_>>();
        assert_eq!(
            selection.retain(&vec![true; batch.row_count() - 1]),
            Err(ColumnarBatchBoundError::SelectionLength)
        );
        assert_eq!(selection.indices().collect::<Vec<_>>(), before_error);
    }
}
