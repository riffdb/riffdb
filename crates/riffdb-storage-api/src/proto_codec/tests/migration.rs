use riffdb_types::{
    AggregateTypeId, CanonicalRecord, CanonicalValue, ContractBundleHash, ContractLineage,
    ContractVersion, EntityKeyBuilder, EntityTypeId, FieldId, IndexEntryKey, IndexEntryKeyBuilder,
    IndexId, MAX_CANONICAL_DOCUMENT_BYTES, MAX_CONTRACT_LINEAGE_BYTES, MAX_KEY_BYTES,
    PartitionKeyBuilder,
};

use crate::{
    DurableKeySchemaBindingV1, IndexMigrationSemanticRow, MAX_INDEX_MIGRATION_PAGE_BYTES,
    StoredIndexEntryV1, StoredIndexEntryV2,
};

use super::super::*;
use super::sample;

#[test]
fn migration_factory_binds_both_versions_to_the_exact_key_and_envelope() {
    let legacy = sample::legacy_index_record();
    let legacy_envelope = encode_index_entry_v1(&legacy).expect("legacy fixture encodes");
    let legacy_evidence = decode_index_migration_row(legacy.key(), legacy_envelope.as_bytes())
        .expect("legacy row produces checked evidence");
    assert!(matches!(
        legacy_evidence.row(),
        IndexMigrationSemanticRow::V1(row) if row == &legacy
    ));
    assert_eq!(legacy_evidence.physical_key(), legacy.key());
    assert_eq!(
        legacy_evidence.canonical_envelope(),
        legacy_envelope.as_bytes()
    );

    let (current, _) = sample::index_records();
    let current_envelope = encode_index_entry_v2(&current).expect("current row encodes");
    let current_evidence = decode_index_migration_row(current.key(), current_envelope.as_bytes())
        .expect("current row produces checked evidence");
    assert!(matches!(
        current_evidence.row(),
        IndexMigrationSemanticRow::V2(row) if row == &current
    ));
    assert_eq!(current_evidence.physical_key(), current.key());
    assert_eq!(
        current_evidence.canonical_envelope(),
        current_envelope.as_bytes()
    );
    assert!(
        current_evidence.conservative_v2_envelope_charge().get()
            >= current_envelope.encoded_content_charge().get()
    );
}

#[test]
fn migration_factory_rejects_wrong_keys_types_and_tampered_envelopes() {
    let (current, _) = sample::index_records();
    let envelope = encode_index_entry_v2(&current).expect("current row encodes");

    let wrong_key = distinct_index_key();
    assert_kind(
        decode_index_migration_row(&wrong_key, envelope.as_bytes()),
        DurableCodecErrorKind::CorruptData,
    );

    let wrong_type =
        encode_entity_record_v1(sample::atomic_record_set().entities()[0].post_image())
            .expect("entity encodes");
    assert_kind(
        decode_index_migration_row(current.key(), wrong_type.as_bytes()),
        DurableCodecErrorKind::UnexpectedRecordType,
    );

    let mut checksum = envelope.as_bytes().to_vec();
    checksum[12] ^= 1;
    assert_kind(
        decode_index_migration_row(current.key(), &checksum),
        DurableCodecErrorKind::CorruptData,
    );

    let mut tag = envelope.as_bytes().to_vec();
    tag[5] = u8::MAX;
    assert_kind(
        decode_index_migration_row(current.key(), &tag),
        DurableCodecErrorKind::IncompatibleFormat,
    );

    let mut version = envelope.as_bytes().to_vec();
    version[4] = version[4].saturating_add(1);
    assert_kind(
        decode_index_migration_row(current.key(), &version),
        DurableCodecErrorKind::IncompatibleFormat,
    );

    let mut noncanonical = envelope.as_bytes().to_vec();
    noncanonical.extend_from_slice(&[0x30, 0x00]);
    assert_kind(
        decode_index_migration_row(current.key(), &noncanonical),
        DurableCodecErrorKind::CorruptData,
    );
}

#[test]
fn one_maximum_valid_row_and_replacement_fit_the_migration_page_bound() {
    const CANONICAL_RECORD_OVERHEAD: usize = 16;

    let physical_key = maximum_index_key();
    let covered_values = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::bytes(vec![
            0xa5;
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD
        ])
        .expect("maximum bytes value"),
    )])
    .expect("maximum canonical record");
    let binding = DurableKeySchemaBindingV1::new(
        ContractLineage::new("x".repeat(MAX_CONTRACT_LINEAGE_BYTES)).expect("maximum lineage"),
        ContractVersion::new(u64::MAX).expect("maximum version"),
        ContractBundleHash::from_bytes([0xff; 32]),
    );
    let legacy = StoredIndexEntryV1::new(
        physical_key.clone(),
        binding.clone(),
        covered_values.clone(),
    )
    .expect("maximum legacy row");
    let legacy_envelope = encode_index_entry_v1(&legacy).expect("maximum legacy envelope");
    let evidence = decode_index_migration_row(&physical_key, legacy_envelope.as_bytes())
        .expect("maximum row has a bounded replacement reservation");

    let replacement = StoredIndexEntryV2::new(
        physical_key,
        binding,
        covered_values,
        maximum_partition_key(),
    )
    .expect("maximum replacement row");
    let replacement_envelope =
        encode_index_entry_v2(&replacement).expect("maximum replacement envelope");
    assert!(evidence.evidence_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES);
    assert!(evidence.instruction_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES);
    assert!(
        evidence.conservative_v2_envelope_charge().get()
            >= replacement_envelope.encoded_content_charge().get()
    );
}

fn distinct_index_key() -> IndexEntryKey {
    let entity = EntityKeyBuilder::new(EntityTypeId::first())
        .finish()
        .expect("minimal entity key");
    let mut key = IndexEntryKeyBuilder::new(IndexId::first());
    key.push_str("different").expect("index component");
    key.finish(entity).expect("distinct index key")
}

fn maximum_index_key() -> IndexEntryKey {
    let entity = EntityKeyBuilder::new(EntityTypeId::first())
        .finish()
        .expect("minimal entity key");
    let mut key = IndexEntryKeyBuilder::new(IndexId::first());
    key.push_bytes(&vec![0xff; MAX_KEY_BYTES - 6 - 4 - 4 - 6])
        .expect("maximum index component");
    let key = key.finish(entity).expect("maximum index key");
    assert_eq!(key.as_bytes().len(), MAX_KEY_BYTES);
    key
}

fn maximum_partition_key() -> riffdb_types::PartitionKey {
    let mut key = PartitionKeyBuilder::new(AggregateTypeId::first());
    key.push_bytes(&vec![0xff; MAX_KEY_BYTES - 2 - 4 - 4])
        .expect("maximum partition component");
    let key = key.finish().expect("maximum partition key");
    assert_eq!(key.as_bytes().len(), MAX_KEY_BYTES);
    key
}

fn assert_kind<T: std::fmt::Debug>(
    result: Result<T, DurableCodecError>,
    expected: DurableCodecErrorKind,
) {
    assert_eq!(result.expect_err("fixture must fail").kind(), expected);
}
