//! Private successor admission preserves the ordinary atomic apply semantics.
// req: OQ-040, PRJ-001
use super::{
    ExactTextFilterPostingsV3, ExactTextIndexMutationV2, ExactTextIndexMutationV3,
    ExactTextPartitionIndexV2, ExactTextPartitionIndexV3, ExactTextRebuildAdmissionErrorV1,
    admission::inspect_postings,
};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    CanonicalRecord, CommitSequence, EntityKey, EntityKeyHash, ExactTextProfileV1,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1,
};
use std::mem::{align_of, size_of};
use std::sync::Arc;

impl ExactTextPartitionIndexV2 {
    /// Admit a private binary-text successor before cloning its predecessor.
    /// Ordinary atomic apply remains the authority for mutation validation.
    #[doc(hidden)]
    pub fn apply_admitted_v1(
        &self,
        epoch: CommitSequence,
        mutations: &[ExactTextIndexMutationV2],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        // Non-binary profiles have separate normalization amplification. Refuse
        // this first-party path until that construction bound is proved.
        if self.index.profile != ExactTextProfileV1::BinaryUtf8V1 {
            return Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable);
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = successor_maximum(
            self,
            mutations.iter().map(|mutation| match mutation {
                ExactTextIndexMutationV2::Upsert { row, value, output } => {
                    (row, Some((value, output)))
                }
                ExactTextIndexMutationV2::Delete { row } => (row, None),
            }),
        )
        .map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let mut next = self.clone();
        next.apply(epoch, mutations)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(next).map_err(Into::into)
    }
}

fn successor_maximum<'a>(
    previous: &ExactTextPartitionIndexV2,
    mutations: impl Clone
    + ExactSizeIterator<Item = (&'a EntityKey, Option<(&'a String, &'a CanonicalRecord)>)>,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    // Count the union without assuming deletions, replacement, deduplication or
    // tree occupancy reduce the peak. apply holds its private outer clone and
    // an atomic inner clone while it replaces postings and ordering arrays.
    let rows = previous
        .index
        .rows
        .len()
        .checked_add(mutations.len())
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        for _ in 0..4 {
            inspection.visit(previous)?;
            inspection.prospective_btree::<EntityKeyHash, EntityKey>(rows)?;
            inspection.prospective_btree::<EntityKeyHash, CanonicalRecord>(rows)?;
            inspection.prospective_btree::<EntityKeyHash, &EntityKey>(mutations.len())?;
            inspection.allocation(
                mutations
                    .len()
                    .checked_mul(4 * size_of::<ExactTextIndexMutationV2>())
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<ExactTextIndexMutationV2>(),
            )?;
            for (row, payload) in mutations.clone() {
                inspection.visit(row)?;
                if let Some((value, output)) = payload {
                    inspection.visit(value)?;
                    inspection.visit(output)?;
                }
            }
            inspect_postings(
                inspection,
                previous
                    .index
                    .rows
                    .iter()
                    .map(|(hash, text)| {
                        previous
                            .keys
                            .get(hash)
                            .map(|key| (key.as_bytes().len(), text.as_str()))
                            .ok_or(OwnedCapacityInspectionErrorV1)
                    })
                    .chain(mutations.clone().filter_map(|(row, payload)| {
                        payload.map(|(value, _)| Ok((row.as_bytes().len(), value.as_str())))
                    })),
                rows,
            )?;
        }
        Ok(())
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    report
        .unshared_bytes()
        .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

impl ExactTextPartitionIndexV3 {
    /// Admit all private base, filter and checkpoint-validation copies before
    /// ordinary atomic apply. The published predecessor is never mutated.
    #[doc(hidden)]
    pub fn apply_admitted_v1(
        &self,
        epoch: CommitSequence,
        mutations: &[ExactTextIndexMutationV3],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        if self.base.index.profile != ExactTextProfileV1::BinaryUtf8V1 {
            return Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable);
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = filtered_successor_maximum(self, mutations).map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let mut next = self.clone();
        next.apply(epoch, mutations)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(next).map_err(Into::into)
    }
}

fn filtered_successor_maximum(
    previous: &ExactTextPartitionIndexV3,
    mutations: &[ExactTextIndexMutationV3],
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let base = successor_maximum(
        &previous.base,
        mutations.iter().map(|mutation| match mutation {
            ExactTextIndexMutationV3::Upsert {
                row, value, output, ..
            } => (row, Some((value, output))),
            ExactTextIndexMutationV3::Delete(row) => (row, None),
        }),
    )?;
    let rows = previous
        .filters
        .len()
        .checked_add(mutations.len())
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        // Full predecessor inventories bound outer/atomic clones and copies of
        // unaffected filtered families; the base maximum bounds new postings.
        for _ in 0..4 {
            inspection.visit(previous)?;
            inspection.prospective_btree::<EntityKeyHash, riffdb_types::CanonicalValue>(rows)?;
            inspection.prospective_btree::<Vec<u8>, ExactTextFilterPostingsV3>(rows)?;
            inspection.prospective_btree::<EntityKeyHash, ()>(rows)?;
            inspection.prospective_btree::<Vec<u8>, ()>(rows)?;
            inspection.prospective_btree::<EntityKeyHash, String>(rows)?;
            inspection.prospective_btree::<EntityKeyHash, EntityKey>(rows)?;
            for filter in previous.filters.values() {
                let encoded = riffdb_types::canonical_value_encoded_len(filter)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?;
                inspection.allocation(encoded, align_of::<u8>())?;
            }
            for mutation in mutations {
                if let ExactTextIndexMutationV3::Upsert {
                    row,
                    value,
                    filter,
                    output,
                } = mutation
                {
                    inspection.visit(row)?;
                    inspection.visit(value)?;
                    inspection.visit(filter)?;
                    inspection.visit(output)?;
                    inspection.allocation(
                        riffdb_types::canonical_value_encoded_len(filter)
                            .map_err(|_| OwnedCapacityInspectionErrorV1)?,
                        align_of::<u8>(),
                    )?;
                }
            }
        }
        // The canonical checkpoint includes the union of old and upserted rows,
        // bounding both Vec's capacity hint and geometric encoding growth.
        let mut encoded = rows
            .checked_mul(128)
            .and_then(|n| n.checked_add(128))
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        for (hash, text) in &previous.base.index.rows {
            let key = previous
                .base
                .keys
                .get(hash)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let output = previous
                .base
                .outputs
                .get(hash)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let filter = previous
                .filters
                .get(hash)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            for bytes in [
                key.as_bytes().len(),
                text.len(),
                riffdb_types::canonical_record_encoded_len(output)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?,
                riffdb_types::canonical_value_encoded_len(filter)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?,
            ] {
                encoded = encoded
                    .checked_add(bytes)
                    .ok_or(OwnedCapacityInspectionErrorV1)?;
            }
        }
        for mutation in mutations {
            if let ExactTextIndexMutationV3::Upsert {
                row,
                value,
                filter,
                output,
            } = mutation
            {
                for bytes in [
                    row.as_bytes().len(),
                    value.len(),
                    riffdb_types::canonical_record_encoded_len(output)
                        .map_err(|_| OwnedCapacityInspectionErrorV1)?,
                    riffdb_types::canonical_value_encoded_len(filter)
                        .map_err(|_| OwnedCapacityInspectionErrorV1)?,
                ] {
                    encoded = encoded
                        .checked_add(bytes)
                        .ok_or(OwnedCapacityInspectionErrorV1)?;
                }
            }
        }
        inspection.allocation(
            encoded
                .checked_mul(4)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<u8>(),
        )?;
        Ok(())
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    base.checked_add(report.unshared_bytes())
        .ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    use riffdb_types::{FieldId, hash_partition_key};
    use std::sync::Arc;

    #[test]
    fn admitted_text_successor_matches_apply_and_preserves_prior_capture() {
        let budget = ProviderBudget::new();
        let partition = hash_partition_key(b"successor");
        let generation = ProjectionGeneration::new(1).unwrap();
        let first = CommitSequence::new(1).unwrap();
        let second = CommitSequence::new(2).unwrap();
        let mut builder =
            riffdb_types::EntityKeyBuilder::new(riffdb_types::EntityTypeId::new(1).unwrap());
        builder.push_u32(1).unwrap();
        let key = builder.finish().unwrap();
        let output =
            CanonicalRecord::new(vec![(FieldId::new(1).unwrap(), CanonicalValue::U64(7))]).unwrap();
        let rows = BTreeMap::from([(key.clone(), ("old".to_owned(), output.clone()))]);
        let prior = ExactTextPartitionIndexV2::rebuild_admitted_v1(
            partition,
            generation,
            first,
            ExactTextProfileV1::BinaryUtf8V1,
            &rows,
            &budget,
        )
        .unwrap();
        let before = prior.to_checkpoint_bytes().unwrap();
        let changes = vec![ExactTextIndexMutationV2::upsert(key, "newé", output).unwrap()];
        let mut expected = (**prior).clone();
        expected.apply(second, &changes).unwrap();
        let available = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
            - budget.observation().unwrap().owned_bytes
            - budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget
            .reserve(available - riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1)
            .unwrap();
        assert!(matches!(
            prior.apply_admitted_v1(second, &changes, &budget),
            Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(prior.to_checkpoint_bytes().unwrap(), before);
        drop(occupied);
        let duplicate = vec![changes[0].clone(), changes[0].clone()];
        assert!(matches!(
            prior.apply_admitted_v1(second, &duplicate, &budget),
            Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::DuplicateRowMutation
            ))
        ));
        assert_eq!(prior.to_checkpoint_bytes().unwrap(), before);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let successor = prior.apply_admitted_v1(second, &changes, &budget).unwrap();
        assert_eq!(&**successor, &expected);
        assert_eq!(prior.to_checkpoint_bytes().unwrap(), before);
        let capture = Arc::clone(&successor);
        drop(successor);
        drop(prior);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    #[test]
    fn admitted_filtered_successor_matches_apply_and_releases_failed_work() {
        let budget = ProviderBudget::new();
        let mut builder =
            riffdb_types::EntityKeyBuilder::new(riffdb_types::EntityTypeId::new(1).unwrap());
        builder.push_u32(1).unwrap();
        let key = builder.finish().unwrap();
        let output =
            CanonicalRecord::new(vec![(FieldId::new(1).unwrap(), CanonicalValue::U64(7))]).unwrap();
        let first = CommitSequence::new(1).unwrap();
        let second = CommitSequence::new(2).unwrap();
        let rows = BTreeMap::from([(
            key.clone(),
            ("old".to_owned(), CanonicalValue::Null, output.clone()),
        )]);
        let prior = ExactTextPartitionIndexV3::rebuild_admitted_v1(
            hash_partition_key(b"successor"),
            ProjectionGeneration::new(1).unwrap(),
            first,
            ExactTextProfileV1::BinaryUtf8V1,
            FieldId::new(2).unwrap(),
            &rows,
            &budget,
        )
        .unwrap();
        let before = prior.to_checkpoint_bytes().unwrap();
        let changes = vec![ExactTextIndexMutationV3::Upsert {
            row: key,
            value: "newé".to_owned(),
            filter: CanonicalValue::U64(5),
            output,
        }];
        let mut expected = (**prior).clone();
        expected.apply(second, &changes).unwrap();
        let successor = prior.apply_admitted_v1(second, &changes, &budget).unwrap();
        assert_eq!(&**successor, &expected);
        assert_eq!(
            successor.to_checkpoint_bytes().unwrap(),
            expected.to_checkpoint_bytes().unwrap()
        );
        assert_eq!(prior.to_checkpoint_bytes().unwrap(), before);
        assert!(matches!(
            prior.apply_admitted_v1(first, &changes, &budget),
            Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::NonAdvancingEpoch
            ))
        ));
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        drop(successor);
        drop(prior);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    #[test]
    fn admitted_successor_sequence_covers_growth_deletion_and_filter_moves() {
        for text in ["", "aaaa", "abcdef", "é日"] {
            let budget = ProviderBudget::new();
            let rows = BTreeMap::new();
            let mut prior = ExactTextPartitionIndexV3::rebuild_admitted_v1(
                hash_partition_key(b"sequence"),
                ProjectionGeneration::new(1).unwrap(),
                CommitSequence::new(1).unwrap(),
                ExactTextProfileV1::BinaryUtf8V1,
                FieldId::new(2).unwrap(),
                &rows,
                &budget,
            )
            .unwrap();
            for ordinal in 2..6 {
                let mut changes = Vec::new();
                for id in 0..4 {
                    let mut builder = riffdb_types::EntityKeyBuilder::new(
                        riffdb_types::EntityTypeId::new(1).unwrap(),
                    );
                    builder.push_u32(id).unwrap();
                    let row = builder.finish().unwrap();
                    changes.push(if ordinal == 5 && id % 2 == 0 {
                        ExactTextIndexMutationV3::Delete(row)
                    } else {
                        ExactTextIndexMutationV3::Upsert {
                            row,
                            value: format!("{text}{ordinal}"),
                            filter: CanonicalValue::U64((id + ordinal) as u64 % 3),
                            output: CanonicalRecord::new(vec![(
                                FieldId::new(1).unwrap(),
                                CanonicalValue::string(text).unwrap(),
                            )])
                            .unwrap(),
                        }
                    });
                }
                let epoch = CommitSequence::new(ordinal as u64).unwrap();
                let mut expected = (**prior).clone();
                expected.apply(epoch, &changes).unwrap();
                let before = prior.to_checkpoint_bytes().unwrap();
                let next = prior.apply_admitted_v1(epoch, &changes, &budget).unwrap();
                assert_eq!(&**next, &expected);
                assert_eq!(prior.to_checkpoint_bytes().unwrap(), before);
                assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
                prior = next;
            }
            drop(prior);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        }
    }
}
