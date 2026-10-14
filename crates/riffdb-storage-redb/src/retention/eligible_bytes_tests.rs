// req: STO-030, STO-031, END-007, END-008
use super::*;

pub(super) fn capsule() -> riffdb_storage_api::StoredCommandCapsuleV1 {
    segment().commands()[0].base().clone()
}

pub(super) fn segment() -> riffdb_storage_api::StoredCommandSegmentV1 {
    let wire =
        include_str!("../../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt")
            .lines()
            .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
            .unwrap();
    let bytes = (0..wire.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&wire[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    riffdb_storage_api::decode_command_segment_v1(&bytes)
        .unwrap()
        .into_parts()
        .0
}

#[test]
fn logical_bytes_count_actual_replaced_inline_locators_without_retired_keys_or_mutation() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let capsule = capsule();
    let key = encode_idempotency_key(&capsule.outcome().identity().storage_key().unwrap()).to_vec();
    let locator = riffdb_storage_api::encode_command_locator_v1(
        riffdb_storage_api::StoredCommandLocatorV1::new(capsule.commit_sequence()),
    )
    .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::IDEMPOTENCY)
        .unwrap()
        .insert(key.as_slice(), locator.as_bytes())
        .unwrap();
    write.commit().unwrap();
    let read = ports.begin_composite_read().unwrap();
    let mut views = CountingViews::new(&read);
    materialize_retained_capsule_views(&mut views, &capsule, &[]).unwrap();
    assert_eq!(
        views.bytes().unwrap(),
        Bytes {
            values: locator.as_bytes().len() as u64,
            keys: 0
        }
    );
    let after = ports.begin_composite_read().unwrap();
    assert_eq!(
        after
            .read_value(JournalTable::Idempotency, &key)
            .unwrap()
            .unwrap(),
        locator.as_bytes()
    );
}

#[test]
fn logical_bytes_exclude_identical_retained_outcomes_and_new_protected_views() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let capsule = capsule();
    let key = encode_idempotency_key(&capsule.outcome().identity().storage_key().unwrap()).to_vec();
    let outcome = codec::encode_stored_outcome_v1(capsule.outcome()).unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::IDEMPOTENCY)
        .unwrap()
        .insert(key.as_slice(), outcome.as_bytes())
        .unwrap();
    write.commit().unwrap();
    let read = ports.begin_composite_read().unwrap();
    let mut views = CountingViews::new(&read);
    materialize_retained_capsule_views(&mut views, &capsule, &[]).unwrap();
    assert_eq!(views.bytes().unwrap(), Bytes::default());
    let after = ports.begin_composite_read().unwrap();
    assert_eq!(
        after
            .read_value(JournalTable::Idempotency, &key)
            .unwrap()
            .unwrap(),
        outcome.as_bytes()
    );
    assert!(
        after
            .read_value(
                JournalTable::Provenance,
                &encode_provenance_key(capsule.provenance().provenance_id())
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn logical_bytes_corrupt_locator_refuses_instead_of_crediting_partial_retained_work() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let capsule = capsule();
    let key = encode_idempotency_key(&capsule.outcome().identity().storage_key().unwrap()).to_vec();
    let locator = riffdb_storage_api::encode_command_locator_v1(
        riffdb_storage_api::StoredCommandLocatorV1::new(
            CommitSequence::new(capsule.commit_sequence().get() + 1).unwrap(),
        ),
    )
    .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::IDEMPOTENCY)
        .unwrap()
        .insert(key.as_slice(), locator.as_bytes())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let read = ports.begin_composite_read().unwrap();
    let mut views = CountingViews::new(&read);
    assert_eq!(
        materialize_retained_capsule_views(&mut views, &capsule, &[])
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert!(views.changed.is_empty());
    assert_eq!(
        ports
            .begin_composite_read()
            .unwrap()
            .read_value(JournalTable::Idempotency, &key)
            .unwrap()
            .unwrap(),
        locator.as_bytes()
    );
}

#[test]
fn logical_bytes_workspace_and_numeric_overflow_refuse_without_unbounded_staging() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let read = ports.begin_composite_read().unwrap();
    let mut views = CountingViews::new(&read);
    let oversized = vec![1; crate::journal::MAX_JOURNAL_FRAME_BYTES];
    assert_eq!(
        views
            .put(JournalTable::Provenance, b"fixture", &oversized)
            .unwrap_err()
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert!(views.changed.is_empty());
    assert_eq!(views.workspace, 0);
    let mut bytes = Bytes {
        values: u64::MAX,
        keys: 0,
    };
    assert_eq!(
        bytes.add(1, 0).unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        bytes
            .extend(Bytes { values: 1, keys: 0 })
            .unwrap_err()
            .kind(),
        StorageErrorKind::LimitExceeded
    );
}
