//! Admission before tokenized analyzer and posting construction.
// req: OQ-040
use super::{
    AnalyzedFieldV1, DocumentV1, TokenizedPostingV1, TokenizedTextConfigV1, TokenizedTextErrorV1,
    TokenizedTextMutationV1, TokenizedTextPartitionIndexV1,
};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    CommitSequence, EntityKeyHash, FieldId, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1, PartitionKeyHash,
    ProjectionGeneration, TextAnalyzerV1, UNICODE_FOLD_V1_MAXIMUM_EXPANSION,
};
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::sync::Arc;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedTextAdmissionErrorV1 {
    /// Retriable graph capacity or inspection refusal.
    CapacityUnavailable,
    /// Unchanged ordinary provider validation error.
    Provider(TokenizedTextErrorV1),
}
impl From<BudgetUnavailable> for TokenizedTextAdmissionErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::CapacityUnavailable
    }
}
impl std::fmt::Display for TokenizedTextAdmissionErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityUnavailable => {
                f.write_str("tokenized construction admission unavailable")
            }
            Self::Provider(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for TokenizedTextAdmissionErrorV1 {}

impl TokenizedTextPartitionIndexV1 {
    /// Admit analyzer workspace, document copies, postings and canonical
    /// checkpoint validation before cloning configuration or analyzing fields.
    #[doc(hidden)]
    pub fn rebuild_admitted_v1(
        config: &TokenizedTextConfigV1,
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        documents: &[TokenizedTextMutationV1],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, TokenizedTextAdmissionErrorV1> {
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = rebuild_maximum(config, documents).map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider = Self::rebuild(config.clone(), partition, generation, frontier, documents)
            .map_err(TokenizedTextAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

impl TokenizedTextPartitionIndexV1 {
    /// Inspect supported checkpoint lengths and decoded capacity before ordinary
    /// recovery constructs documents or redundant posting dictionaries.
    #[doc(hidden)]
    pub fn from_checkpoint_bytes_admitted_v1(
        bytes: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, TokenizedTextAdmissionErrorV1> {
        if bytes.len() < 90 || bytes.len() > super::MAX_TOKENIZED_CHECKPOINT_BYTES_V1 {
            return Err(TokenizedTextAdmissionErrorV1::Provider(
                TokenizedTextErrorV1::InvalidCheckpoint,
            ));
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = recovery_maximum(bytes).map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider =
            Self::from_checkpoint_bytes(bytes).map_err(TokenizedTextAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}

fn recovery_maximum(bytes: &[u8]) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        let mut reader = super::Reader::new(bytes);
        reader
            .take(88)
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let fields = usize::from(reader.u16().map_err(|_| OwnedCapacityInspectionErrorV1)?);
        if fields == 0 || fields > 1024 {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        reader
            .take(mul(6, fields)?)
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        inspection.allocation(
            mul(size_of::<super::TokenizedTextFieldV1>(), fields)?,
            align_of::<super::TokenizedTextFieldV1>(),
        )?;
        let documents = count(&mut reader)?;
        if documents > super::MAX_TOKENIZED_DOCUMENTS_V1 {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        inspection.prospective_btree::<EntityKeyHash, DocumentV1>(documents)?;
        for _ in 0..documents {
            reader
                .take(32)
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let key = reader
                .bytes_u16()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(key.len(), align_of::<u8>())?;
            let output = reader
                .bytes_u32()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            if output.len() > super::MAX_TOKENIZED_OUTPUT_BYTES_V1 {
                return Err(OwnedCapacityInspectionErrorV1);
            }
            let heap =
                riffdb_types::inspect_canonical_decode_capacity_v1(output)?.owned_heap_bytes();
            inspection.allocation(
                usize::try_from(heap).map_err(|_| OwnedCapacityInspectionErrorV1)?,
                align_of::<usize>(),
            )?;
            let analyzed_fields =
                usize::from(reader.u16().map_err(|_| OwnedCapacityInspectionErrorV1)?);
            if analyzed_fields > fields {
                return Err(OwnedCapacityInspectionErrorV1);
            }
            inspection.prospective_btree::<FieldId, AnalyzedFieldV1>(analyzed_fields)?;
            let mut total = 0;
            for _ in 0..analyzed_fields {
                reader.take(8).map_err(|_| OwnedCapacityInspectionErrorV1)?;
                let terms = count(&mut reader)?;
                total = add(total, terms)?;
                if total > super::MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1 {
                    return Err(OwnedCapacityInspectionErrorV1);
                }
                inspection.allocation(mul(size_of::<String>(), terms)?, align_of::<String>())?;
                // Reconstruction builds one posting entry per occurrence in
                // the worst case, independently of encoded redundant postings.
                inspection.prospective_btree::<(FieldId, String), BTreeMap<EntityKeyHash, TokenizedPostingV1>>(terms)?;
                inspection.prospective_btree::<EntityKeyHash, TokenizedPostingV1>(terms)?;
                inspection.allocation(
                    add(mul(6 * size_of::<u32>(), terms)?, mul(512, terms)?)?,
                    align_of::<u32>(),
                )?;
                for _ in 0..terms {
                    reader.take(4).map_err(|_| OwnedCapacityInspectionErrorV1)?;
                    let term = reader
                        .bytes_u16()
                        .map_err(|_| OwnedCapacityInspectionErrorV1)?;
                    inspection.allocation(add(mul(2, term.len())?, 512)?, align_of::<u8>())?;
                }
            }
        }
        let posting_count = count(&mut reader)?;
        if posting_count > reader.remaining() / 10 {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        inspection
            .prospective_btree::<(FieldId, String), BTreeMap<EntityKeyHash, TokenizedPostingV1>>(
                posting_count,
            )?;
        for _ in 0..posting_count {
            reader.take(4).map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let term = reader
                .bytes_u16()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(term.len(), align_of::<u8>())?;
            let documents = count(&mut reader)?;
            if documents > super::MAX_TOKENIZED_DOCUMENTS_V1 {
                return Err(OwnedCapacityInspectionErrorV1);
            }
            inspection.prospective_btree::<EntityKeyHash, TokenizedPostingV1>(documents)?;
            for _ in 0..documents {
                reader
                    .take(36)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?;
                let positions = count(&mut reader)?;
                if positions > super::MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1
                    || positions > reader.remaining() / 4
                {
                    return Err(OwnedCapacityInspectionErrorV1);
                }
                let position_bytes = mul(size_of::<u32>(), positions)?;
                reader
                    .take(position_bytes)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?;
                inspection.allocation(position_bytes, align_of::<u32>())?;
            }
        }
        if !reader.is_finished() {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        inspection.allocation(
            size_of::<ProviderAllocation<TokenizedTextPartitionIndexV1>>() + 4096,
            align_of::<ProviderAllocation<TokenizedTextPartitionIndexV1>>(),
        )?;
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
fn count(reader: &mut super::Reader<'_>) -> Result<usize, OwnedCapacityInspectionErrorV1> {
    usize::try_from(reader.u32().map_err(|_| OwnedCapacityInspectionErrorV1)?)
        .map_err(|_| OwnedCapacityInspectionErrorV1)
}

fn rebuild_maximum(
    config: &TokenizedTextConfigV1,
    documents: &[TokenizedTextMutationV1],
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.visit(config)?;
        inspection.visit(config)?;
        inspection.prospective_btree::<EntityKeyHash, DocumentV1>(documents.len())?;
        inspection.prospective_btree::<EntityKeyHash, ()>(documents.len())?;
        let mut encoded = add(128, mul(8, config.fields.len())?)?;
        for document in documents {
            let TokenizedTextMutationV1::Upsert {
                key,
                fields,
                output,
            } = document
            else {
                continue;
            };
            inspection.visit(key)?;
            inspection.visit(output)?;
            inspection.visit(output)?;
            inspection.prospective_btree::<FieldId, AnalyzedFieldV1>(fields.len())?;
            let output_len = riffdb_types::canonical_record_encoded_len(output)
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(output_len, align_of::<u8>())?;
            encoded = add(encoded, add(64, add(key.as_bytes().len(), output_len)?)?)?;
            for (_, value) in fields {
                let (terms, term_bytes) = match config.analyzer {
                    TextAnalyzerV1::KeywordV1 => (1, value.len()),
                    // Word spans are disjoint nonempty UTF-8 slices. Their
                    // count and total source length are bounded by source bytes;
                    // folding uses the existing frozen expansion constant.
                    TextAnalyzerV1::StandardV1 => (
                        value.len(),
                        mul(value.len(), UNICODE_FOLD_V1_MAXIMUM_EXPANSION)?,
                    ),
                };
                inspection
                    .allocation(mul(2 * size_of::<String>(), terms)?, align_of::<String>())?;
                // Analyzer-owned terms, copied document terms and posting keys,
                // including per-buffer allocator slack and temporary fold output.
                inspection.allocation(
                    add(mul(6, term_bytes)?, mul(512, terms)?)?,
                    align_of::<u8>(),
                )?;
                if config.analyzer == TextAnalyzerV1::StandardV1 {
                    // NFKC's bounded decomposition/recomposition workspace,
                    // stable-sort scratch and successive String growth are
                    // independently admitted. Pinned normalization uses an
                    // eight-byte decomposition cell and four-byte composition
                    // cell; 256 bytes per expanded source byte covers their
                    // geometric growth, sort scratch and intermediate strings.
                    inspection.allocation(mul(256, term_bytes)?, align_of::<usize>())?;
                }
                inspection.prospective_btree::<(FieldId, String), BTreeMap<EntityKeyHash, TokenizedPostingV1>>(terms)?;
                inspection.prospective_btree::<EntityKeyHash, TokenizedPostingV1>(terms)?;
                inspection.allocation(
                    add(mul(6 * size_of::<u32>(), terms)?, mul(512, terms)?)?,
                    align_of::<u32>(),
                )?;
                encoded = add(
                    encoded,
                    add(16, add(mul(64, terms)?, mul(2, term_bytes)?)?)?,
                )?;
            }
        }
        // Canonical encoder starts empty and grows geometrically. Its output
        // includes both per-document terms and redundant posting dictionaries.
        inspection.allocation(mul(4, encoded)?, align_of::<u8>())?;
        inspection.allocation(
            size_of::<ProviderAllocation<TokenizedTextPartitionIndexV1>>() + 4096,
            align_of::<ProviderAllocation<TokenizedTextPartitionIndexV1>>(),
        )?;
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
fn add(a: usize, b: usize) -> Result<usize, OwnedCapacityInspectionErrorV1> {
    a.checked_add(b).ok_or(OwnedCapacityInspectionErrorV1)
}
fn mul(a: usize, b: usize) -> Result<usize, OwnedCapacityInspectionErrorV1> {
    a.checked_mul(b).ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    use std::sync::Arc;

    #[test]
    fn tokenized_rebuild_admission_matches_both_analyzers_and_final_capture() {
        for analyzer in [TextAnalyzerV1::KeywordV1, TextAnalyzerV1::StandardV1] {
            for text in [
                "",
                "shared shared shared",
                "Straße ﬁle İ Σ",
                "日本語 cafe\u{0301}",
            ] {
                let budget = ProviderBudget::new();
                let config = TokenizedTextConfigV1::new(
                    [1; 32],
                    analyzer,
                    vec![TokenizedTextFieldV1::new(FieldId::new(1).unwrap(), 1).unwrap()],
                )
                .unwrap();
                let mut builder = riffdb_types::EntityKeyBuilder::new(
                    riffdb_types::EntityTypeId::new(1).unwrap(),
                );
                builder.push_u32(1).unwrap();
                let changes = vec![
                    TokenizedTextMutationV1::upsert(
                        builder.finish().unwrap(),
                        vec![(FieldId::new(1).unwrap(), text.to_owned())],
                        CanonicalRecord::new(vec![]).unwrap(),
                    )
                    .unwrap(),
                ];
                let partition = PartitionKeyHash::from_bytes([2; 32]);
                let generation = ProjectionGeneration::new(1).unwrap();
                let head = CommitSequence::new(1).unwrap();
                let expected = TokenizedTextPartitionIndexV1::rebuild(
                    config.clone(),
                    partition,
                    generation,
                    head,
                    &changes,
                )
                .unwrap();
                let provider = TokenizedTextPartitionIndexV1::rebuild_admitted_v1(
                    &config, partition, generation, head, &changes, &budget,
                )
                .unwrap();
                assert_eq!(&**provider, &expected);
                assert_eq!(
                    provider.to_checkpoint_bytes().unwrap(),
                    expected.to_checkpoint_bytes().unwrap()
                );
                let encoded = provider.to_checkpoint_bytes().unwrap();
                let recovered = TokenizedTextPartitionIndexV1::from_checkpoint_bytes_admitted_v1(
                    &encoded, &budget,
                )
                .unwrap();
                assert_eq!(&**recovered, &expected);
                drop(recovered);
                let retained = budget.observation().unwrap().owned_bytes;
                let mut invalid_field = encoded.clone();
                invalid_field[90..94].fill(0);
                let mut changed_posting = encoded.clone();
                *changed_posting.last_mut().unwrap() ^= 1;
                let mut trailing = encoded.clone();
                trailing.push(0);
                for corrupt in [
                    &encoded[..encoded.len() - 1],
                    invalid_field.as_slice(),
                    changed_posting.as_slice(),
                    trailing.as_slice(),
                ] {
                    assert!(TokenizedTextPartitionIndexV1::from_checkpoint_bytes(corrupt).is_err());
                    assert!(
                        TokenizedTextPartitionIndexV1::from_checkpoint_bytes_admitted_v1(
                            corrupt, &budget
                        )
                        .is_err()
                    );
                    assert_eq!(budget.observation().unwrap().owned_bytes, retained);
                    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
                }
                let available = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
                    - retained
                    - budget.observation().unwrap().fixed_owner_bytes;
                let occupied = budget
                    .reserve(
                        available - riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1,
                    )
                    .unwrap();
                assert!(matches!(
                    TokenizedTextPartitionIndexV1::from_checkpoint_bytes_admitted_v1(
                        &encoded, &budget
                    ),
                    Err(TokenizedTextAdmissionErrorV1::CapacityUnavailable)
                ));
                assert_eq!(budget.observation().unwrap().owned_bytes, retained);
                assert_eq!(budget.observation().unwrap().reservations, 1);
                drop(occupied);
                let capture = Arc::clone(&provider);
                drop(provider);
                assert!(budget.observation().unwrap().owned_bytes > 0);
                drop(capture);
                assert_eq!(budget.observation().unwrap().owned_bytes, 0);
                assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            }
        }
    }
    #[test]
    fn tokenized_rebuild_pressure_and_invalid_population_release_admission() {
        let budget = ProviderBudget::new();
        let config = TokenizedTextConfigV1::new(
            [1; 32],
            TextAnalyzerV1::StandardV1,
            vec![TokenizedTextFieldV1::new(FieldId::new(1).unwrap(), 1).unwrap()],
        )
        .unwrap();
        let mut documents = Vec::new();
        for id in 0..4 {
            let mut builder =
                riffdb_types::EntityKeyBuilder::new(riffdb_types::EntityTypeId::new(1).unwrap());
            builder.push_u32(id).unwrap();
            documents.push(
                TokenizedTextMutationV1::upsert(
                    builder.finish().unwrap(),
                    vec![(
                        FieldId::new(1).unwrap(),
                        "shared ﬁle cafe\u{0301}".to_owned(),
                    )],
                    CanonicalRecord::new(vec![]).unwrap(),
                )
                .unwrap(),
            );
        }
        let partition = PartitionKeyHash::from_bytes([2; 32]);
        let generation = ProjectionGeneration::new(1).unwrap();
        let head = CommitSequence::new(1).unwrap();
        let available = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
            - budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget
            .reserve(available - riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1)
            .unwrap();
        assert!(matches!(
            TokenizedTextPartitionIndexV1::rebuild_admitted_v1(
                &config, partition, generation, head, &documents, &budget
            ),
            Err(TokenizedTextAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reservations, 1);
        drop(occupied);
        let expected = TokenizedTextPartitionIndexV1::rebuild(
            config.clone(),
            partition,
            generation,
            head,
            &documents,
        )
        .unwrap();
        let admitted = TokenizedTextPartitionIndexV1::rebuild_admitted_v1(
            &config, partition, generation, head, &documents, &budget,
        )
        .unwrap();
        assert_eq!(&**admitted, &expected);
        drop(admitted);
        documents.push(documents[0].clone());
        assert!(matches!(
            TokenizedTextPartitionIndexV1::rebuild_admitted_v1(
                &config, partition, generation, head, &documents, &budget
            ),
            Err(TokenizedTextAdmissionErrorV1::Provider(
                TokenizedTextErrorV1::DuplicateDocumentMutation
            ))
        ));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
