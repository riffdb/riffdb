//! Borrowed checkpoint sizing before row decode and derived reconstruction.
// req: OQ-040
use super::admission::inspect_postings;
use super::{
    ExactTextPartitionIndexV2, ExactTextProviderErrorV1, ExactTextRebuildAdmissionErrorV1,
};
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use riffdb_types::{
    CanonicalRecord, EntityKey, EntityKeyHash, MAX_KEY_BYTES,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1, inspect_canonical_decode_capacity_v1,
};
use std::mem::{align_of, size_of};
use std::sync::Arc;

impl ExactTextPartitionIndexV2 {
    /// Admit decoding, derived reconstruction and canonical verification before
    /// invoking the unchanged recovery path. This is not a semantic trust proof.
    #[doc(hidden)]
    pub fn from_checkpoint_bytes_admitted_v1(
        bytes: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        let cursor =
            BorrowedRows::new(bytes).map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = recovery_maximum(cursor).map_err(|_| {
            ExactTextRebuildAdmissionErrorV1::Provider(ExactTextProviderErrorV1::InvalidCheckpoint)
        })?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider = Self::from_checkpoint_bytes(bytes)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

#[derive(Clone)]
pub(super) struct BorrowedRows<'a> {
    bytes: &'a [u8],
    position: usize,
    pub(super) count: usize,
    remaining: usize,
}
impl<'a> BorrowedRows<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Result<Self, ExactTextProviderErrorV1> {
        if bytes.len() < 60 || bytes.len() > super::MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        if bytes.get(..4) != Some(b"RXTS") || super::read_u16(bytes, 4)? != 2 {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        if riffdb_types::ExactTextProfileV1::from_discriminant(bytes[6]).is_none() || bytes[7] != 0
        {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        if super::read_u64(bytes, 40)? == 0 || super::read_u64(bytes, 48)? == 0 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        let count = usize::try_from(super::read_u32(bytes, 56)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if count > super::MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        Ok(Self {
            bytes,
            position: 60,
            count,
            remaining: count,
        })
    }
    fn take(&mut self, size: usize) -> Result<&'a [u8], OwnedCapacityInspectionErrorV1> {
        let end = self
            .position
            .checked_add(size)
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        self.position = end;
        Ok(bytes)
    }
    fn row(&mut self) -> Result<(&'a [u8], &'a str, &'a [u8]), OwnedCapacityInspectionErrorV1> {
        let key_bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let key_length = usize::from(u16::from_be_bytes(key_bytes));
        if key_length > MAX_KEY_BYTES {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let key = self.take(key_length)?;
        let text_bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let text_length = usize::from(u16::from_be_bytes(text_bytes));
        if text_length > super::MAX_EXACT_TEXT_VALUE_BYTES_V1 {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let text = std::str::from_utf8(self.take(text_length)?)
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let output_bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let output_length = usize::try_from(u32::from_be_bytes(output_bytes))
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        if output_length > super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let output = self.take(output_length)?;
        Ok((key, text, output))
    }
    pub(super) fn complete(&self) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.remaining == 0 && self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(OwnedCapacityInspectionErrorV1)
        }
    }
}
impl<'a> Iterator for BorrowedRows<'a> {
    type Item = Result<(&'a [u8], &'a str, &'a [u8]), OwnedCapacityInspectionErrorV1>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(self.row())
    }
}

pub(super) fn recovery_maximum(
    mut rows: BorrowedRows<'_>,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let postings = rows.clone();
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        // Decoded source map plus the ordinary builder's temporary and retained
        // hash-keyed maps. Payload copies are charged separately below.
        inspection.prospective_btree::<EntityKey, (String, CanonicalRecord)>(rows.count)?;
        inspection.prospective_btree::<EntityKeyHash, String>(rows.count)?;
        inspection.prospective_btree::<EntityKeyHash, String>(rows.count)?;
        inspection.prospective_btree::<EntityKeyHash, EntityKey>(rows.count)?;
        inspection.prospective_btree::<EntityKeyHash, CanonicalRecord>(rows.count)?;
        let count = rows.count;
        for row in rows.by_ref() {
            let (key, text, output) = row?;
            let decoded = inspect_canonical_decode_capacity_v1(output)?;
            // Up to four simultaneously admitted source/build copies. The
            // borrowed canonical bound counts actual decoded collection slots,
            // nested values, strings and vectors rather than encoded length.
            let buffers = key
                .len()
                .checked_add(text.len())
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| bytes.checked_add(4 * 256))
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(buffers, align_of::<usize>())?;
            let heap = decoded
                .owned_heap_bytes()
                .checked_mul(4)
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(heap, align_of::<usize>())?;
        }
        rows.complete()?;
        inspect_postings(
            inspection,
            postings.map(|row| row.map(|(key, text, _)| (key.len(), text))),
            count,
        )?;
        // Recovery re-encodes the complete checkpoint for byte identity. Admit
        // both its initial row hint and geometric growth; empty-text checkpoints
        // can retain an initial hint larger than twice their encoded length.
        let initial = count
            .checked_mul(64)
            .and_then(|bytes| bytes.checked_add(60))
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        let bytes = rows
            .bytes
            .len()
            .checked_mul(2)
            .ok_or(OwnedCapacityInspectionErrorV1)?
            .max(initial);
        inspection.allocation(bytes, align_of::<u8>())?;
        inspection.allocation(
            size_of::<ProviderAllocation<ExactTextPartitionIndexV2>>() + 4096,
            align_of::<ProviderAllocation<ExactTextPartitionIndexV2>>(),
        )
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    report
        .unshared_bytes()
        .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{
        CanonicalValue, CommitSequence, EntityKeyBuilder, EntityTypeId, ExactTextProfileV1,
        FieldId, PartitionKeyHash, ProjectionGeneration,
    };
    use std::collections::BTreeMap;

    fn checkpoint(count: u32, text: &str) -> Vec<u8> {
        let rows = (0..count)
            .map(|index| {
                let mut key = EntityKeyBuilder::new(EntityTypeId::new(1).unwrap());
                key.push_u32(index).unwrap();
                let output = CanonicalRecord::new(vec![(
                    FieldId::new(1).unwrap(),
                    CanonicalValue::string("shared output").unwrap(),
                )])
                .unwrap();
                (key.finish().unwrap(), (text.to_owned(), output))
            })
            .collect::<BTreeMap<_, _>>();
        ExactTextPartitionIndexV2::rebuild(
            PartitionKeyHash::from_bytes([1; 32]),
            ProjectionGeneration::new(1).unwrap(),
            CommitSequence::new(1).unwrap(),
            ExactTextProfileV1::BinaryUtf8V1,
            &rows,
        )
        .unwrap()
        .to_checkpoint_bytes()
        .unwrap()
    }

    #[test]
    fn recovery_preflight_covers_row_hints_and_utf8_posting_reconstruction() {
        for (count, text) in [
            (0, ""),
            (64, ""),
            (4, "abcdefghijklmnopqrstuvwx"),
            (12, "é🌻漢字"),
        ] {
            let bytes = checkpoint(count, text);
            let budget = ProviderBudget::new();
            let scratch = budget
                .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                .unwrap();
            let maximum = recovery_maximum(BorrowedRows::new(&bytes).unwrap()).unwrap();
            let ordinary = ExactTextPartitionIndexV2::from_checkpoint_bytes(&bytes).unwrap();
            let report = OwnedCapacityInspectionV1::inspect(&ordinary).unwrap();
            assert!(maximum > report.unshared_bytes() + OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1);
            drop(report);
            drop(scratch);
            let recovered =
                ExactTextPartitionIndexV2::from_checkpoint_bytes_admitted_v1(&bytes, &budget)
                    .unwrap();
            assert_eq!(&**recovered, &ordinary);
            drop(recovered);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        }
    }

    #[test]
    fn pressure_after_borrowed_inspection_refuses_before_decoded_provider_ownership() {
        let bytes = checkpoint(4, "abcdefghijklmnopqrstuvwx");
        let budget = ProviderBudget::new();
        let available = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1;
        let occupied = budget
            .reserve(
                riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
                    - budget.observation().unwrap().fixed_owner_bytes
                    - available,
            )
            .unwrap();
        assert!(matches!(
            ExactTextPartitionIndexV2::from_checkpoint_bytes_admitted_v1(&bytes, &budget),
            Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(budget.observation().unwrap().reservations, 1);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 0);
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
