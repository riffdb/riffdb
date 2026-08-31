//! Durable tokenized provider-state conformance for ADR-0173.

use riffdb_projection::{
    TokenizedTextConfigV1, TokenizedTextErrorV1, TokenizedTextFieldV1, TokenizedTextMutationV1,
    TokenizedTextPartitionIndexV1,
};
use riffdb_types::{
    CanonicalRecord, CommitSequence, EntityKey, EntityKeyBuilder, EntityTypeId, FieldId,
    PartitionKeyHash, ProjectionGeneration, TextAnalyzerV1, hash_entity_key,
};

fn field(value: u32) -> FieldId {
    FieldId::new(value).unwrap()
}

fn key(value: u64) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u64(value).unwrap();
    builder.finish().unwrap()
}

fn config() -> TokenizedTextConfigV1 {
    TokenizedTextConfigV1::new(
        [7; 32],
        TextAnalyzerV1::StandardV1,
        vec![
            TokenizedTextFieldV1::new(field(1), 4).unwrap(),
            TokenizedTextFieldV1::new(field(2), 1).unwrap(),
        ],
    )
    .unwrap()
}

fn upsert(value: u64, title: &str, body: &str) -> TokenizedTextMutationV1 {
    TokenizedTextMutationV1::upsert(
        key(value),
        vec![(field(1), title.to_owned()), (field(2), body.to_owned())],
        CanonicalRecord::new(Vec::new()).unwrap(),
    )
    .unwrap()
}

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

#[test]
fn segment_carries_positions_frequencies_norms_and_canonical_checkpoint() {
    let partition = PartitionKeyHash::from_bytes([3; 32]);
    let generation = ProjectionGeneration::new(9).unwrap();
    let mut index = TokenizedTextPartitionIndexV1::new(config(), partition, generation);
    index
        .apply(
            sequence(10),
            &[
                upsert(1, "Rust rust database", "durable search"),
                upsert(2, "Rust guide", "search without sidecars"),
            ],
        )
        .unwrap();

    let first = key(1);
    let first_hash = hash_entity_key(first.as_bytes());
    let title_rust = index
        .posting(field(1), "rust")
        .unwrap()
        .get(&first_hash)
        .unwrap();
    assert_eq!(title_rust.frequency(), 2);
    assert_eq!(title_rust.positions(), &[0, 1]);
    assert_eq!(index.field_length(first_hash, field(1)), Some(3));
    assert_eq!(index.document_count(), 2);

    let checkpoint = index.to_checkpoint_bytes().unwrap();
    let recovered = TokenizedTextPartitionIndexV1::from_checkpoint_bytes(&checkpoint).unwrap();
    assert_eq!(recovered, index);
    assert_eq!(recovered.to_checkpoint_bytes().unwrap(), checkpoint);

    let mut compacted = recovered.clone();
    compacted.compact().unwrap();
    assert_eq!(compacted, recovered);

    let rebuilt = TokenizedTextPartitionIndexV1::rebuild(
        config(),
        partition,
        generation,
        sequence(10),
        &[
            upsert(1, "Rust rust database", "durable search"),
            upsert(2, "Rust guide", "search without sidecars"),
        ],
    )
    .unwrap();
    assert_eq!(rebuilt, index);
}

#[test]
fn failed_epoch_is_atomic_and_update_delete_remove_old_postings() {
    let mut index = TokenizedTextPartitionIndexV1::new(
        config(),
        PartitionKeyHash::from_bytes([4; 32]),
        ProjectionGeneration::new(1).unwrap(),
    );
    index
        .apply(sequence(1), &[upsert(1, "old term", "body")])
        .unwrap();
    let before = index.clone();
    let invalid = TokenizedTextMutationV1::upsert(
        key(2),
        vec![(field(99), "not declared".to_owned())],
        CanonicalRecord::new(Vec::new()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        index.apply(sequence(2), &[invalid]),
        Err(TokenizedTextErrorV1::UnknownField)
    );
    assert_eq!(index, before);

    let first_hash = hash_entity_key(key(1).as_bytes());
    index
        .apply(sequence(2), &[upsert(1, "new term", "body")])
        .unwrap();
    assert!(index.posting(field(1), "old").is_none());
    assert!(
        index
            .posting(field(1), "new")
            .unwrap()
            .contains_key(&first_hash)
    );
    index
        .apply(sequence(3), &[TokenizedTextMutationV1::delete(key(1))])
        .unwrap();
    assert_eq!(index.document_count(), 0);
    assert!(index.posting(field(1), "new").is_none());
}

#[test]
fn corrupt_mixed_and_noncanonical_checkpoints_fail_closed() {
    let mut index = TokenizedTextPartitionIndexV1::new(
        config(),
        PartitionKeyHash::from_bytes([5; 32]),
        ProjectionGeneration::new(1).unwrap(),
    );
    index
        .apply(sequence(1), &[upsert(1, "one two", "three")])
        .unwrap();
    let bytes = index.to_checkpoint_bytes().unwrap();

    let mut unknown_version = bytes.clone();
    unknown_version[5] = 2;
    assert_eq!(
        TokenizedTextPartitionIndexV1::from_checkpoint_bytes(&unknown_version),
        Err(TokenizedTextErrorV1::UnsupportedFormat)
    );

    let mut corrupt_posting = bytes.clone();
    let last = corrupt_posting.last_mut().unwrap();
    *last ^= 1;
    assert_eq!(
        TokenizedTextPartitionIndexV1::from_checkpoint_bytes(&corrupt_posting),
        Err(TokenizedTextErrorV1::InvalidCheckpoint)
    );

    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        TokenizedTextPartitionIndexV1::from_checkpoint_bytes(&trailing),
        Err(TokenizedTextErrorV1::InvalidCheckpoint)
    );
}
