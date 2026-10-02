//! Admission for exact-text rebuilds, including derived substring postings.
// req: OQ-040
use super::{ExactTextPartitionIndexV2, ExactTextProviderErrorV1, PostingList};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    CanonicalRecord, CommitSequence, EntityKey, EntityKeyHash, ExactTextProfileV1,
    MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1, PartitionKeyHash, ProjectionGeneration,
};
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::sync::Arc;

/// Closed distinction between admission pressure and ordinary provider validation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextRebuildAdmissionErrorV1 {
    /// Retriable aggregate capacity or inspection refusal.
    CapacityUnavailable,
    /// Unchanged ordinary rebuild validation failure.
    Provider(ExactTextProviderErrorV1),
}
impl From<BudgetUnavailable> for ExactTextRebuildAdmissionErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::CapacityUnavailable
    }
}
impl std::fmt::Display for ExactTextRebuildAdmissionErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityUnavailable => {
                f.write_str("exact text construction admission unavailable")
            }
            Self::Provider(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ExactTextRebuildAdmissionErrorV1 {}
const _: () = assert!(size_of::<(EntityKeyHash, Vec<u8>, Vec<u8>)>() <= 128);
const _: () = assert!(size_of::<(u16, Vec<u8>, Vec<u8>)>() <= 128);

impl ExactTextPartitionIndexV2 {
    /// First-party rebuild admission. The returned immutable allocation owns
    /// its charge through its final capture. Source materialization and checkpoint
    /// encoding require their own admission; all ordinary rebuild checks remain.
    #[doc(hidden)]
    pub fn rebuild_admitted_v1(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        profile: ExactTextProfileV1,
        rows: &BTreeMap<EntityKey, (String, CanonicalRecord)>,
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, ExactTextRebuildAdmissionErrorV1> {
        if rows.len() > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::PartitionRowLimit,
            ));
        }
        for (value, output) in rows.values() {
            if value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                    ExactTextProviderErrorV1::ValueTooLong,
                ));
            }
            let encoded = riffdb_types::canonical_record_encoded_len(output).map_err(|_| {
                ExactTextRebuildAdmissionErrorV1::Provider(ExactTextProviderErrorV1::OutputInvalid)
            })?;
            if encoded > super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextRebuildAdmissionErrorV1::Provider(
                    ExactTextProviderErrorV1::OutputTooLarge,
                ));
            }
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = rebuild_maximum(rows).map_err(|_| BudgetUnavailable)?;
        // Destroy the census before releasing the inspection allowance. No
        // physical provider, term dictionary or ordering buffer exists yet.
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider = Self::rebuild(partition, generation, frontier, profile, rows)
            .map_err(ExactTextRebuildAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

fn rebuild_maximum(
    rows: &BTreeMap<EntityKey, (String, CanonicalRecord)>,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        // Source copies and temporary hashed rows: the ordinary builder clones
        // text twice and output once. Two complete source inventories also cover
        // all three hash-keyed tree roots and deep output/key copies.
        inspection.visit(rows)?;
        inspection.visit(rows)?;
        // Hash-keyed roots can have a larger inline value than the source root.
        inspection.prospective_btree::<EntityKeyHash, String>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, String>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, EntityKey>(rows.len())?;
        inspection.prospective_btree::<EntityKeyHash, CanonicalRecord>(rows.len())?;
        inspect_postings(
            inspection,
            rows.iter()
                .map(|(key, (value, _))| Ok((key.as_bytes().len(), value.as_str()))),
            rows.len(),
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

pub(super) fn inspect_postings<'a>(
    inspection: &mut OwnedCapacityInspectionV1,
    rows: impl Iterator<Item = Result<(usize, &'a str), OwnedCapacityInspectionErrorV1>>,
    row_count: usize,
) -> Result<(), OwnedCapacityInspectionErrorV1> {
    let mut prefixes = 0usize;
    let mut substrings = 0usize;
    let mut terms = 0usize;
    let mut term_bytes = 0usize;
    let mut ordered_bytes = 0usize;
    for row in rows {
        let (key_bytes, value) = row?;
        let n = value.len();
        // All byte boundaries bound the UTF-8 character-boundary subset.
        // Repeated terms are counted independently, so deduplication can
        // only reduce dictionary and posting allocation below this maximum.
        let sub = mul(n, add(n, 1)?)? / 2;
        let row_terms = add(add(1, mul(2, n)?)?, sub)?;
        prefixes = add(prefixes, n)?;
        substrings = add(substrings, sub)?;
        terms = add(terms, row_terms)?;
        let prefix_suffix_bytes = mul(n, add(n, 1)?)?;
        let substring_bytes = mul(mul(n, add(n, 1)?)?, add(n, 2)?)? / 6;
        term_bytes = add(
            term_bytes,
            add(add(n, prefix_suffix_bytes)?, substring_bytes)?,
        )?;
        ordered_bytes = add(ordered_bytes, mul(row_terms, add(n, key_bytes)?)?)?;
        // terms() creates borrowed term sets and a boundary vector before
        // copying dictionary keys. Admit their peak independently.
        inspection.prospective_btree::<&[u8], ()>(sub)?;
        inspection.prospective_btree::<&[u8], ()>(n)?;
        inspection.prospective_btree::<&[u8], ()>(n)?;
        inspection.allocation(
            mul(2 * size_of::<usize>(), add(n, 1)?)?,
            align_of::<usize>(),
        )?;
    }
    // V2 retains both the V1 hash-ordered and V2 key-ordered dictionaries.
    for _ in 0..2 {
        inspection.prospective_btree::<Vec<u8>, PostingList>(row_count)?;
        inspection.prospective_btree::<Vec<u8>, PostingList>(prefixes)?;
        inspection.prospective_btree::<Vec<u8>, PostingList>(prefixes)?;
        inspection.prospective_btree::<Vec<u8>, PostingList>(substrings)?;
        repeated_allocation(inspection, term_bytes, terms, align_of::<u8>())?;
        // Vec::push: capacity is bounded by twice the logical length plus
        // the four-entry initial allocation, charged per posting occurrence.
        repeated_allocation(
            inspection,
            mul(6 * size_of::<EntityKeyHash>(), terms)?,
            terms,
            align_of::<EntityKeyHash>(),
        )?;
        repeated_allocation(
            inspection,
            mul(size_of::<u16>(), terms)?,
            terms,
            align_of::<u16>(),
        )?;
    }
    // V2 ordering holds tuple arrays, freshly collected ascending/descending
    // arrays, and cloned text/key byte buffers while replacing old vectors.
    repeated_allocation(
        inspection,
        mul(4, ordered_bytes)?,
        mul(4, terms)?,
        align_of::<u8>(),
    )?;
    repeated_allocation(
        inspection,
        mul(4 * 128, terms)?,
        mul(4, terms)?,
        align_of::<usize>(),
    )?;
    // Output verification uses a bounded temporary canonical encoding. Root
    // and settlement ownership metadata use the same conservative allowance.
    inspection.allocation(super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1, align_of::<u8>())?;
    inspection.allocation(
        size_of::<ProviderAllocation<ExactTextPartitionIndexV2>>() + 4096,
        align_of::<ProviderAllocation<ExactTextPartitionIndexV2>>(),
    )
}

fn repeated_allocation(
    inspection: &mut OwnedCapacityInspectionV1,
    bytes: usize,
    allocations: usize,
    alignment: usize,
) -> Result<(), OwnedCapacityInspectionErrorV1> {
    // Aggregation still charges allocator and alignment slack for EACH buffer,
    // not just one aggregate allocation. The visitor adds further root slack.
    inspection.allocation(
        add(bytes, mul(allocations, add(alignment, 128)?)?)?,
        alignment,
    )
}
fn add(a: usize, b: usize) -> Result<usize, OwnedCapacityInspectionErrorV1> {
    a.checked_add(b).ok_or(OwnedCapacityInspectionErrorV1)
}
fn mul(a: usize, b: usize) -> Result<usize, OwnedCapacityInspectionErrorV1> {
    a.checked_mul(b).ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CanonicalValue, EntityKeyBuilder, EntityTypeId, FieldId};

    fn key(index: u32) -> EntityKey {
        let mut builder = EntityKeyBuilder::new(EntityTypeId::new(1).unwrap());
        builder.push_u32(index).unwrap();
        builder.finish().unwrap()
    }
    fn parameters() -> (PartitionKeyHash, ProjectionGeneration, CommitSequence) {
        (
            PartitionKeyHash::from_bytes([1; 32]),
            ProjectionGeneration::new(1).unwrap(),
            CommitSequence::new(1).unwrap(),
        )
    }

    #[test]
    fn admitted_filtered_rebuild_matches_ordinary_and_keeps_final_capture() {
        use super::super::ExactTextPartitionIndexV3;
        for count in [0_u32, 1, 12] {
            let rows = (0..count)
                .map(|i| {
                    (
                        key(i),
                        (
                            "abβ".to_owned(),
                            if i % 2 == 0 {
                                CanonicalValue::Null
                            } else {
                                CanonicalValue::string(i.to_string()).unwrap()
                            },
                            CanonicalRecord::new(vec![]).unwrap(),
                        ),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let (partition, generation, frontier) = parameters();
            let field = FieldId::new(1).unwrap();
            let expected = ExactTextPartitionIndexV3::rebuild(
                partition,
                generation,
                frontier,
                ExactTextProfileV1::BinaryUtf8V1,
                field,
                &rows,
            )
            .unwrap();
            let budget = ProviderBudget::new();
            let actual = ExactTextPartitionIndexV3::rebuild_admitted_v1(
                partition,
                generation,
                frontier,
                ExactTextProfileV1::BinaryUtf8V1,
                field,
                &rows,
                &budget,
            )
            .unwrap();
            assert!(**actual == expected);
            assert_eq!(
                actual.to_checkpoint_bytes().unwrap(),
                expected.to_checkpoint_bytes().unwrap()
            );
            let retained = Arc::clone(&actual);
            drop(actual);
            assert!(budget.observation().unwrap().owned_bytes > 0);
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            drop(retained);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        }
    }

    #[test]
    fn preflight_bounds_empty_repeated_unicode_and_distinct_term_dictionaries() {
        for text in [
            "",
            "a",
            "aaaaaa",
            "abcdefghijklmnopqrstuvwx",
            "é🌻漢字",
            "abababababab",
        ] {
            for count in [0, 1, 4, 12] {
                let rows = (0..count)
                    .map(|index| {
                        let output = CanonicalRecord::new(vec![(
                            FieldId::new(1).unwrap(),
                            CanonicalValue::string(format!("output-{index}")).unwrap(),
                        )])
                        .unwrap();
                        (key(index), (text.to_owned(), output))
                    })
                    .collect::<BTreeMap<_, _>>();
                let budget = ProviderBudget::new();
                let scratch = budget
                    .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                    .unwrap();
                let maximum = rebuild_maximum(&rows).unwrap();
                let (partition, generation, frontier) = parameters();
                let ordinary = ExactTextPartitionIndexV2::rebuild(
                    partition,
                    generation,
                    frontier,
                    ExactTextProfileV1::BinaryUtf8V1,
                    &rows,
                )
                .unwrap();
                let report = OwnedCapacityInspectionV1::inspect(&ordinary).unwrap();
                assert!(
                    maximum > report.unshared_bytes() + OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
                );
                drop(report);
                drop(scratch);
                let admitted = ExactTextPartitionIndexV2::rebuild_admitted_v1(
                    partition,
                    generation,
                    frontier,
                    ExactTextProfileV1::BinaryUtf8V1,
                    &rows,
                    &budget,
                )
                .unwrap();
                assert_eq!(&**admitted, &ordinary);
                assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
                drop(admitted);
                assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            }
        }
    }

    #[test]
    fn invalid_output_and_text_are_refused_before_any_construction_admission() {
        let (partition, generation, frontier) = parameters();
        let budget = ProviderBudget::new();
        let output = CanonicalRecord::new(vec![(
            FieldId::new(1).unwrap(),
            CanonicalValue::bytes(vec![0; super::super::MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1])
                .unwrap(),
        )])
        .unwrap();
        let rows = BTreeMap::from([(key(0), ("a".to_owned(), output))]);
        let result = ExactTextPartitionIndexV2::rebuild_admitted_v1(
            partition,
            generation,
            frontier,
            ExactTextProfileV1::BinaryUtf8V1,
            &rows,
            &budget,
        );
        assert!(matches!(
            result,
            Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::OutputTooLarge
            ))
        ));
        let rows = BTreeMap::from([(
            key(0),
            (
                "a".repeat(MAX_EXACT_TEXT_VALUE_BYTES_V1 + 1),
                CanonicalRecord::new(vec![]).unwrap(),
            ),
        )]);
        assert!(matches!(
            ExactTextPartitionIndexV2::rebuild_admitted_v1(
                partition,
                generation,
                frontier,
                ExactTextProfileV1::BinaryUtf8V1,
                &rows,
                &budget
            ),
            Err(ExactTextRebuildAdmissionErrorV1::Provider(
                ExactTextProviderErrorV1::ValueTooLong
            ))
        ));
        assert_eq!(budget.observation().unwrap().reservations, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 0);
    }
}
