//! Isolated checkpoint-head tests; not startup or V3 receipt durability proof.
// req: REP-002, REC-001

use super::*;
use riffdb_storage_api::{EntityChainHeadV1, EntityChainStateV1, encode_entity_chain_head_v1};
use riffdb_types::{EntityKeyBuilder, EntityTransitionHash, EntityTypeId, SchemaHash};

fn head(id: u32) -> EntityChainHeadV1 {
    let entity_type = EntityTypeId::new(id).unwrap();
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_bytes(b"\0key\xff").unwrap();
    EntityChainHeadV1::from_stored_parts(
        EntityTarget::new(entity_type, key.finish().unwrap()).unwrap(),
        2,
        EntityChainStateV1::Deleted,
        CommitSequence::new(2).unwrap(),
        EntityTransitionHash::from_bytes([0x51; 32]),
    )
    .unwrap()
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
