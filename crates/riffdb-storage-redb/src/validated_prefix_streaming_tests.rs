//! Isolated checkpoint-head tests; not startup or V3 receipt durability proof.
// req: REP-002, REC-001

use super::*;
use riffdb_storage_api::{EntityChainHeadV1, EntityChainStateV1, encode_entity_chain_head_v1};
use riffdb_types::{EntityKeyBuilder, EntityTransitionHash, EntityTypeId, SchemaHash};

fn head(id: u32) -> EntityChainHeadV1 {
    head_with_key(id, b"\0key\xff")
}

fn head_with_key(id: u32, payload: &[u8]) -> EntityChainHeadV1 {
    let entity_type = EntityTypeId::new(id).unwrap();
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_bytes(payload).unwrap();
    EntityChainHeadV1::from_stored_parts(
        EntityTarget::new(entity_type, key.finish().unwrap()).unwrap(),
        2,
        EntityChainStateV1::Deleted,
        CommitSequence::new(2).unwrap(),
        EntityTransitionHash::from_bytes([0x51; 32]),
    )
    .unwrap()
}

#[test]
fn checkpoint_head_plan_keeps_only_changed_bytes_and_refuses_malformed_old_rows() {
    use riffdb_types::{EntityRecordHash, EntityVersion};
    let directory = crate::test_path::ScopedDirectory::new("checkpoint-plan-replace");
    let database = redb::Database::create(directory.join("data.redb")).unwrap();
    let heads: Vec<_> = (1..=1024).map(head).collect();
    let proof = checkpoint(&heads);
    let prior = EntityChainHeadV1::from_stored_parts(
        heads[511].target().clone(),
        1,
        EntityChainStateV1::Live {
            version: EntityVersion::first(),
            value_hash: EntityRecordHash::from_bytes([0x31; 32]),
        },
        CommitSequence::first(),
        EntityTransitionHash::from_bytes([0x41; 32]),
    )
    .unwrap();
    let write = database.begin_write().unwrap();
    {
        write.open_table(ENTITIES).unwrap();
        let mut source = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        let mut snapshot = write.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        for head in &heads {
            put_head(&mut source, head);
            put_head(&mut snapshot, head);
        }
        put_head(&mut snapshot, &prior);
    }
    write.commit().unwrap();
    let pin = database.begin_read().unwrap();
    let changes = super::plan_checkpoint_head_changes(&pin, &proof).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].key(), prior.target().key().as_bytes());
    assert!(changes[0].matches_prior(Some(
        encode_entity_chain_head_v1(&prior).unwrap().as_bytes()
    )));
    assert_eq!(
        changes[0].value(),
        Some(encode_entity_chain_head_v1(&heads[511]).unwrap().as_bytes())
    );
    let write = database.begin_write().unwrap();
    {
        let mut snapshot = write.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        snapshot
            .insert(
                b"malformed-old-key".as_slice(),
                b"malformed-old-value".as_slice(),
            )
            .unwrap();
    }
    write.commit().unwrap();
    assert_eq!(
        super::plan_checkpoint_head_changes(&pin, &proof).unwrap(),
        changes
    );
    let read = database.begin_read().unwrap();
    assert_eq!(
        super::plan_checkpoint_head_changes(&read, &proof)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(
        read.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
            .unwrap()
            .get(b"malformed-old-key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"malformed-old-value"
    );
}

#[test]
fn checkpoint_head_plan_refuses_oversized_delta_without_publishing_a_partial_snapshot() {
    let directory = crate::test_path::ScopedDirectory::new("checkpoint-plan-bound");
    let database = redb::Database::create(directory.join("data.redb")).unwrap();
    // Each valid head repeats its bounded key in the encoded value and in the
    // physical mutation key. Their complete delta exceeds one 32MiB receipt.
    let heads: Vec<_> = (1..=4200)
        .map(|id| head_with_key(id, &[0x42; 4000]))
        .collect();
    let proof = checkpoint(&heads);
    let write = database.begin_write().unwrap();
    {
        write.open_table(ENTITIES).unwrap();
        write.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        let mut source = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        for head in &heads {
            put_head(&mut source, head);
        }
    }
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        super::plan_checkpoint_head_changes(&read, &proof)
            .unwrap_err()
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        read.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
    assert_eq!(
        read.open_table(ENTITY_CHAIN_HEADS).unwrap().len().unwrap(),
        heads.len() as u64
    );
}

fn put_head(table: &mut redb::Table<&[u8], &[u8]>, head: &EntityChainHeadV1) {
    let encoded = encode_entity_chain_head_v1(head).unwrap();
    table
        .insert(head.target().key().as_bytes(), encoded.as_bytes())
        .unwrap();
}

fn checkpoint(heads: &[EntityChainHeadV1]) -> StoredValidatedPrefixCheckpointV2 {
    let base = StoredValidatedPrefixCheckpointV1::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
        1,
        SchemaHash::from_bytes([0x42; 32]),
        2,
        0,
        ValidatedPrefixSequenceCounts {
            commits_count: 2,
            events_count: 0,
            event_routes_count: 0,
            outbox_count: 0,
            outbox_status_count: 0,
            idempotency_count: 0,
            audit_count: 0,
            audit_by_request_count: 0,
        },
        EntityChainFingerprint::from_bytes([0; 32]),
        ValidatedPrefixRetainedSnapshot {
            next_application_sequence: 3,
            application_sequence_exhausted: false,
            next_administration_sequence: 1,
            administration_sequence_exhausted: false,
        },
        None,
        0,
    )
    .unwrap();
    StoredValidatedPrefixCheckpointV2::new(
        base,
        ValidatedPrefixEntityTransitionCounts {
            live_entity_count: 0,
            deleted_entity_count: heads.len() as u64,
            entity_transition_count: heads.iter().map(EntityChainHeadV1::chain_revision).sum(),
        },
        EntityTransitionFingerprint::from_sorted_heads(heads).unwrap(),
    )
    .unwrap()
}

#[test]
fn checkpoint_fingerprint_streams_one_pinned_table_and_refuses_key_substitution() {
    let directory = crate::test_path::ScopedDirectory::new("checkpoint-stream-pin");
    let database = redb::Database::create(directory.join("data.redb")).unwrap();
    let heads = [head(1), head(255), head(256), head(u32::MAX)];
    let write = database.begin_write().unwrap();
    {
        let mut table = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        for head in &heads {
            put_head(&mut table, head);
        }
        write.open_table(ENTITIES).unwrap();
    }
    write.commit().unwrap();
    let pin = database.begin_read().unwrap();
    let expected = EntityTransitionFingerprint::from_sorted_heads(&heads).unwrap();
    assert_eq!(entity_transition_proof(&pin).unwrap().1, expected);
    let write = database.begin_write().unwrap();
    {
        let mut table = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        let wrong = encode_entity_chain_head_v1(&heads[1]).unwrap();
        table
            .insert(heads[0].target().key().as_bytes(), wrong.as_bytes())
            .unwrap();
    }
    write.commit().unwrap();
    assert_eq!(entity_transition_proof(&pin).unwrap().1, expected);
    assert_eq!(
        entity_transition_proof(&database.begin_read().unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
}

#[test]
fn checkpoint_head_sync_preserves_exact_rows_removes_stale_rows_and_aborts_bad_proof() {
    let directory = crate::test_path::ScopedDirectory::new("checkpoint-stream-sync");
    let database = redb::Database::create(directory.join("data.redb")).unwrap();
    let heads = [head(1), head(256)];
    let proof = checkpoint(&heads);
    let write = database.begin_write().unwrap();
    write.open_table(ENTITIES).unwrap();
    {
        let mut source = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        for head in &heads {
            put_head(&mut source, head);
        }
        let mut snapshot = write.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        put_head(&mut snapshot, &heads[0]);
        put_head(&mut snapshot, &head(7));
    }
    write.commit().unwrap();
    let old = database.begin_read().unwrap();
    let changes = super::plan_checkpoint_head_changes(&old, &proof).unwrap();
    assert_eq!(
        changes.len(),
        2,
        "unchanged heads do not occupy receipt entries"
    );
    assert_eq!(changes[0].key(), head(7).target().key().as_bytes());
    assert_eq!(changes[0].value(), None);
    assert!(changes[0].matches_prior(Some(
        encode_entity_chain_head_v1(&head(7)).unwrap().as_bytes()
    )));
    assert_eq!(changes[1].key(), heads[1].target().key().as_bytes());
    assert!(changes[1].matches_prior(None));
    assert_eq!(
        changes[1].value(),
        Some(encode_entity_chain_head_v1(&heads[1]).unwrap().as_bytes())
    );
    let write = database.begin_write().unwrap();
    replace_checkpoint_entity_heads(&write, &proof).unwrap();
    write.commit().unwrap();
    let assert_snapshot = || {
        let read = database.begin_read().unwrap();
        let table = read.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        for head in &heads {
            assert_eq!(
                table
                    .get(head.target().key().as_bytes())
                    .unwrap()
                    .unwrap()
                    .value(),
                encode_entity_chain_head_v1(head).unwrap().as_bytes()
            );
        }
        assert!(
            table
                .get(head(7).target().key().as_bytes())
                .unwrap()
                .is_none()
        );
    };
    assert_snapshot();
    assert!(
        old.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
            .unwrap()
            .get(head(7).target().key().as_bytes())
            .unwrap()
            .is_some()
    );
    let write = database.begin_write().unwrap();
    replace_checkpoint_entity_heads(&write, &proof).unwrap();
    write.commit().unwrap();
    assert_snapshot();
    let write = database.begin_write().unwrap();
    {
        let mut source = write.open_table(ENTITY_CHAIN_HEADS).unwrap();
        put_head(&mut source, &head(512));
    }
    assert_eq!(
        replace_checkpoint_entity_heads(&write, &proof)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    write.abort().unwrap();
    assert_snapshot();
}
