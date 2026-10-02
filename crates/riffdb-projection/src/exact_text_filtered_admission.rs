//! Preconstruction admission for base and disjoint filtered text postings.
// req: OQ-040
use super::{
    ExactTextFilterPostingsV3, ExactTextPartitionIndexV3, ExactTextProviderErrorV1,
    ExactTextRebuildAdmissionErrorV1, admission::inspect_postings,
};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyHash, ExactTextProfileV1,
    FieldId, MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1, PartitionKeyHash, ProjectionGeneration,
    canonical_record_encoded_len, canonical_value_encoded_len,
};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::{align_of, size_of};
use std::sync::Arc;

type SourceRows = BTreeMap<EntityKey, (String, CanonicalValue, CanonicalRecord)>;

impl ExactTextPartitionIndexV3 {
    /// Admit the complete ordinary filtered rebuild before copying base rows,
    /// filters, memberships or postings. Source rows and encoded checkpoints have
    /// their own owners; the resulting provider retains final-reference charge.
    #[doc(hidden)]
    pub fn rebuild_admitted_v1(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        profile: ExactTextProfileV1,
        filter_field: FieldId,
        rows: &SourceRows,
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        if rows.len() > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::PartitionRowLimit,
            ));
        }
        for (text, filter, output) in rows.values() {
            if text.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                    ExactTextProviderErrorV1::ValueTooLong,
                ));
            }
            let output_len = canonical_record_encoded_len(output).map_err(|_| {
                ExactTextRebuildAdmissionErrorV1::Provider(ExactTextProviderErrorV1::OutputInvalid)
            })?;
            let filter_len = canonical_value_encoded_len(filter).map_err(|_| {
                ExactTextRebuildAdmissionErrorV1::Provider(ExactTextProviderErrorV1::OutputInvalid)
            })?;
            if output_len > super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1
                || filter_len > super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1
            {
                return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                    ExactTextProviderErrorV1::OutputTooLarge,
                ));
            }
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = rebuild_maximum(rows).map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider = Self::rebuild(partition, generation, frontier, profile, filter_field, rows)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

impl ExactTextPartitionIndexV3 {
    /// Admit ordinary V3 recovery, filter dictionaries and canonical re-encoding
    /// without allocating decoded rows during inspection. Normal recovery still
    /// proves identities, memberships and byte-exact checkpoint canonicality.
    #[doc(hidden)]
    pub fn from_checkpoint_bytes_admitted_v1(
        bytes: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        let frame = BorrowedFilteredCheckpoint::new(bytes)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = recovery_maximum(frame).map_err(|_| {
            ExactTextRebuildAdmissionErrorV1::Provider(ExactTextProviderErrorV1::InvalidCheckpoint)
        })?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider = Self::from_checkpoint_bytes(bytes)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

struct BorrowedFilteredCheckpoint<'a> {
    bytes: &'a [u8],
    base: super::checkpoint_admission::BorrowedRows<'a>,
    filters_start: usize,
    count: usize,
}
impl<'a> BorrowedFilteredCheckpoint<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, ExactTextProviderErrorV1> {
        if bytes.len() < 18
            || bytes.len() > super::MAX_EXACT_TEXT_CHECKPOINT_BYTES_V3
            || bytes.get(..4) != Some(b"RXTF")
            || super::read_u16(bytes, 4)? != 3
        {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        FieldId::new(super::read_u32(bytes, 6)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let length = usize::try_from(super::read_u32(bytes, 10)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let end = 14_usize
            .checked_add(length)
            .filter(|end| end.saturating_add(4) <= bytes.len())
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let base = super::checkpoint_admission::BorrowedRows::new(&bytes[14..end])?;
        let count = usize::try_from(super::read_u32(bytes, end)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if count != base.count || count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        Ok(Self {
            bytes,
            base,
            filters_start: end + 4,
            count,
        })
    }
}

fn recovery_maximum(
    frame: BorrowedFilteredCheckpoint<'_>,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let base_maximum = super::checkpoint_admission::recovery_maximum(frame.base.clone())?;
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.prospective_btree::<EntityKeyHash, CanonicalValue>(frame.count)?;
        inspection.prospective_btree::<Vec<u8>, BTreeSet<EntityKeyHash>>(frame.count)?;
        inspection.prospective_btree::<EntityKeyHash, ()>(frame.count)?;
        inspection.prospective_btree::<Vec<u8>, ExactTextFilterPostingsV3>(frame.count)?;
        inspection.prospective_btree::<EntityKeyHash, String>(frame.count)?;
        inspection.prospective_btree::<EntityKeyHash, EntityKey>(frame.count)?;
        let mut rows = frame.base.clone();
        for row in rows.by_ref() {
            let (key, text, _) = row?;
            inspection.allocation(key.len(), align_of::<u8>())?;
            inspection.allocation(text.len(), align_of::<u8>())?;
        }
        rows.complete()?;
        inspect_postings(
            inspection,
            frame
                .base
                .map(|row| row.map(|(key, text, _)| (key.len(), text))),
            frame.count,
        )?;
        let mut cursor = frame.filters_start;
        for _ in 0..frame.count {
            cursor = cursor
                .checked_add(32)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let length = usize::try_from(
                super::read_u32(frame.bytes, cursor).map_err(|_| OwnedCapacityInspectionErrorV1)?,
            )
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            cursor = cursor
                .checked_add(4)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let end = cursor
                .checked_add(length)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let encoded = frame
                .bytes
                .get(cursor..end)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let decoded = riffdb_types::inspect_canonical_decode_capacity_v1(encoded)?;
            // The ordinary recovery accepts the existing canonical document
            // ceiling here. Do not impose the rebuild-only per-filter ceiling.
            let heap = decoded
                .owned_heap_bytes()
                .checked_mul(4)
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(heap, align_of::<usize>())?;
            for _ in 0..3 {
                inspection.allocation(length, align_of::<u8>())?;
            }
            cursor = end;
        }
        if cursor != frame.bytes.len() {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let encoded = frame
            .bytes
            .len()
            .checked_mul(2)
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        inspection.allocation(
            encoded
                .checked_add(64)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<u8>(),
        )?;
        inspection.allocation(
            size_of::<ProviderAllocation<ExactTextPartitionIndexV3>>() + 4096,
            align_of::<ProviderAllocation<ExactTextPartitionIndexV3>>(),
        )
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    base_maximum
        .checked_add(report.unshared_bytes())
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .ok_or(OwnedCapacityInspectionErrorV1)
}

fn rebuild_maximum(rows: &SourceRows) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        // The base-row clone, hash-keyed base copies and per-filter temporary
        // rows/keys are distinct ownership. Filters and outputs are deep copies.
        for _ in 0..3 {
            inspection.prospective_btree::<EntityKey, (String, CanonicalValue, CanonicalRecord)>(
                rows.len(),
            )?;
            for (key, (text, filter, output)) in rows {
                inspection.visit(key)?;
                inspection.visit(text)?;
                inspection.visit(filter)?;
                inspection.visit(output)?;
            }
        }
        inspection.prospective_btree::<EntityKeyHash, String>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, String>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, EntityKey>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, CanonicalRecord>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, CanonicalValue>(rows.len())?;
        inspection.prospective_btree::<Vec<u8>, BTreeSet<EntityKeyHash>>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, ()>(rows.len())?;
        inspection.prospective_btree::<Vec<u8>, ExactTextFilterPostingsV3>(rows.len())?;
        // Base V2 retains two families. A second complete allowance covers the
        // third filtered family and its transient ordering/dictionary work.
        // No discount assumes filters or terms will be deduplicated.
        for _ in 0..2 {
            inspect_postings(
                inspection,
                rows.iter()
                    .map(|(key, (text, _, _))| Ok((key.as_bytes().len(), text.as_str()))),
                rows.len(),
            )?;
        }
        for (_, filter, _) in rows.values() {
            let bytes =
                canonical_value_encoded_len(filter).map_err(|_| OwnedCapacityInspectionErrorV1)?;
            // Canonical member-map keys, retained filter keys and one temporary
            // encoding. Each buffer receives independent allocator slack.
            for _ in 0..3 {
                inspection.allocation(bytes, align_of::<u8>())?;
            }
        }
        inspection.allocation(
            size_of::<ProviderAllocation<ExactTextPartitionIndexV3>>() + 4096,
            align_of::<ProviderAllocation<ExactTextPartitionIndexV3>>(),
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
    use riffdb_types::{EntityKeyBuilder, EntityTypeId};

    fn rows(text: &str, count: u32) -> SourceRows {
        (0..count)
            .map(|i| {
                let mut key = EntityKeyBuilder::new(EntityTypeId::new(1).unwrap());
                key.push_u32(i).unwrap();
                let filter = match i % 4 {
                    0 => CanonicalValue::Null,
                    1 => CanonicalValue::string("共同").unwrap(),
                    2 => CanonicalValue::U64(u64::from(i)),
                    _ => CanonicalValue::list(vec![CanonicalValue::Bool(true); 8]).unwrap(),
                };
                let output = CanonicalRecord::new(vec![(
                    FieldId::new(2).unwrap(),
                    CanonicalValue::string(format!("result-{i}")).unwrap(),
                )])
                .unwrap();
                (key.finish().unwrap(), (text.to_owned(), filter, output))
            })
            .collect()
    }

    #[test]
    fn filtered_maximum_covers_base_memberships_and_all_filtered_dictionaries() {
        for text in ["", "aaaaaa", "abcdefghijklmnopqrstuvwx", "é🌻漢字"] {
            for count in [0, 1, 4, 16] {
                let rows = rows(text, count);
                let maximum = rebuild_maximum(&rows).unwrap();
                let provider = ExactTextPartitionIndexV3::rebuild(
                    PartitionKeyHash::from_bytes([1; 32]),
                    ProjectionGeneration::first(),
                    CommitSequence::first(),
                    ExactTextProfileV1::BinaryUtf8V1,
                    FieldId::new(1).unwrap(),
                    &rows,
                )
                .unwrap();
                let actual = OwnedCapacityInspectionV1::inspect(&provider).unwrap();
                assert!(actual.shared_allocations().is_empty());
                assert!(
                    maximum >= actual.unshared_bytes() + OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
                );
            }
        }
    }

    #[test]
    fn admitted_filtered_recovery_matches_ordinary_and_refuses_corrupt_framing() {
        for count in [0, 1, 16] {
            let provider = ExactTextPartitionIndexV3::rebuild(
                PartitionKeyHash::from_bytes([1; 32]),
                ProjectionGeneration::first(),
                CommitSequence::first(),
                ExactTextProfileV1::BinaryUtf8V1,
                FieldId::new(1).unwrap(),
                &rows("é🌻漢字", count),
            )
            .unwrap();
            let encoded = provider.to_checkpoint_bytes().unwrap();
            let budget = ProviderBudget::new();
            let recovered =
                ExactTextPartitionIndexV3::from_checkpoint_bytes_admitted_v1(&encoded, &budget)
                    .unwrap();
            assert!(**recovered == provider);
            assert_eq!(recovered.to_checkpoint_bytes().unwrap(), encoded);
            let retained = Arc::clone(&recovered);
            drop(recovered);
            assert!(budget.observation().unwrap().owned_bytes > 0);
            drop(retained);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            for malformed in [
                &encoded[..encoded.len() - 1],
                &[encoded.as_slice(), &[0]].concat(),
            ] {
                assert!(ExactTextPartitionIndexV3::from_checkpoint_bytes(malformed).is_err());
                assert!(
                    ExactTextPartitionIndexV3::from_checkpoint_bytes_admitted_v1(
                        malformed, &budget
                    )
                    .is_err()
                );
                assert_eq!(budget.observation().unwrap().owned_bytes, 0);
                assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            }
        }
    }

    #[test]
    fn filtered_recovery_preserves_existing_filter_ceiling_and_refuses_pressure() {
        use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
        let mut provider = ExactTextPartitionIndexV3::rebuild(
            PartitionKeyHash::from_bytes([1; 32]),
            ProjectionGeneration::first(),
            CommitSequence::first(),
            ExactTextProfileV1::BinaryUtf8V1,
            FieldId::new(1).unwrap(),
            &rows("abcdef", 1),
        )
        .unwrap();
        // Compatibility oracle: recovery has always accepted the canonical
        // document ceiling, wider than the rebuild-only filter ceiling.
        let hash = *provider.filters.keys().next().unwrap();
        provider.filters.insert(
            hash,
            CanonicalValue::bytes(vec![
                7;
                super::super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 + 1
            ])
            .unwrap(),
        );
        let encoded = provider.to_checkpoint_bytes().unwrap();
        let expected = ExactTextPartitionIndexV3::from_checkpoint_bytes(&encoded).unwrap();
        let budget = ProviderBudget::new();
        let occupied = budget
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - budget.observation().unwrap().fixed_owner_bytes
                    - OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
            )
            .unwrap();
        assert!(matches!(
            ExactTextPartitionIndexV3::from_checkpoint_bytes_admitted_v1(&encoded, &budget),
            Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 0);
        assert_eq!(budget.observation().unwrap().reservations, 1);
        drop(occupied);
        let recovered =
            ExactTextPartitionIndexV3::from_checkpoint_bytes_admitted_v1(&encoded, &budget)
                .unwrap();
        assert!(**recovered == expected);
        drop(recovered);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn filtered_pressure_after_inspection_does_not_build_or_publish() {
        use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
        let rows = rows("abcdefghijklmnopqrstuvwx", 1);
        assert!(rebuild_maximum(&rows).unwrap() > OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1);
        let budget = ProviderBudget::new();
        let occupied = budget
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - budget.observation().unwrap().fixed_owner_bytes
                    - OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
            )
            .unwrap();
        assert!(matches!(
            ExactTextPartitionIndexV3::rebuild_admitted_v1(
                PartitionKeyHash::from_bytes([1; 32]),
                ProjectionGeneration::first(),
                CommitSequence::first(),
                ExactTextProfileV1::BinaryUtf8V1,
                FieldId::new(1).unwrap(),
                &rows,
                &budget,
            ),
            Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable)
        ));
        let observation = budget.observation().unwrap();
        assert_eq!(observation.owned_bytes, 0);
        assert_eq!(observation.released_allocations, 0);
        assert_eq!(observation.reservations, 1);
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
