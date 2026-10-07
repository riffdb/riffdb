//! Isolated canonical row fixtures prove metric semantics, not command recovery.
// req: STO-030, STO-031, END-007, END-008
use super::*;

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::store::RedbOperationalPorts,
) {
    let scope = crate::test_path::ScopedDirectory::new("logical-byte-rows");
    let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(19, [0x67; 10]).unwrap();
    store.initialize_legacy_fixture(database).unwrap();
    (
        scope,
        crate::store::RedbOperationalPorts {
            shared: store.shared,
        },
    )
}

fn populate(ports: &crate::store::RedbOperationalPorts, count: u64) -> Vec<usize> {
    let capsule = super::super::tests::capsule();
    let base = capsule.commit();
    let write = ports.shared.database.begin_write().unwrap();
    let lengths = {
        let mut commits = write.open_table(COMMITS).unwrap();
        (1..=count)
            .map(|sequence| {
                let record = riffdb_storage_api::StoredCommitRecordV1::new(
                    CommitSequence::new(sequence).unwrap(),
                    base.admission_request_id(),
                    base.plan().clone(),
                    base.canonical_input_hash(),
                    base.actor().clone(),
                    base.logical_time(),
                    base.partition_hash(),
                    (1..=sequence % 4)
                        .map(|byte| riffdb_types::ConflictKeyHash::from_bytes([byte as u8; 32]))
                        .collect(),
                    base.read_dependencies().clone(),
                    vec![],
                    vec![],
                    base.declared_outcome().clone(),
                    base.provenance_id(),
                    vec![],
                    base.durability_mode(),
                )
                .unwrap();
                let encoded = codec::encode_commit_record_v1(&record).unwrap();
                commits
                    .insert(sequence.to_be_bytes().as_slice(), encoded.as_bytes())
                    .unwrap();
                encoded.as_bytes().len()
            })
            .collect()
    };
    let head = codec::encode_application_sequence_allocator_v1(
        riffdb_storage_api::ApplicationSequenceAllocator::Next(
            CommitSequence::new(count + 1).unwrap(),
        ),
    )
    .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(META_APPLICATION_SEQUENCE, head.as_bytes())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    lengths
}

fn bytes(probe: &Probe, now: Instant) -> Option<Bytes> {
    (probe.completed && now.saturating_duration_since(probe.captured) < MAX_AGE)
        .then_some(probe.bytes)
}

fn segment_fixture(
    deliver: bool,
) -> (
    crate::test_path::ScopedDirectory,
    crate::store::RedbOperationalPorts,
    Bytes,
    u64,
    u64,
) {
    let (scope, ports) = fixture();
    let database = crate::store::read_identity_from_read_transaction(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap();
    let commands = super::super::tests::segment()
        .commands()
        .iter()
        .map(|command| {
            riffdb_storage_api::StoredCommandCapsuleV2::from_base(
                command.base().clone(),
                command.index_generation_transitions().to_vec(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let first = commands[0].commit_sequence();
    let manifest = crate::application::build_command_segment_manifest(&commands, first).unwrap();
    let draft = riffdb_storage_api::StoredCommandSegmentV1::new(
        database,
        1,
        None,
        commands,
        manifest,
        riffdb_storage_api::CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .unwrap();
    let (segment, encoded) = riffdb_storage_api::seal_and_encode_command_segment_v1(draft).unwrap();
    let first = segment.first_commit_sequence().get();
    let last = segment.last_commit_sequence().get();
    let mut expected = Bytes::default();
    expected.add(encoded.as_bytes().len(), 8).unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(COMMITS)
        .unwrap()
        .insert(first.to_be_bytes().as_slice(), encoded.as_bytes())
        .unwrap();
    for command in segment.commands() {
        for event in command.events() {
            let key = crate::keys::encode_event_key(event.event_id());
            let event_row = codec::encode_durable_event_v1(event).unwrap();
            let intent = codec::encode_outbox_intent_v1(
                &riffdb_storage_api::StoredOutboxIntentV1::new(event.clone()),
            )
            .unwrap();
            for (table, row) in [(EVENTS, &event_row), (OUTBOX, &intent)] {
                write
                    .open_table(table)
                    .unwrap()
                    .insert(key.as_slice(), row.as_bytes())
                    .unwrap();
                expected.add(row.as_bytes().len(), key.len()).unwrap();
            }
            let route = codec::encode_event_route_v1(riffdb_storage_api::StoredEventRouteV1::new(
                event.event_id(),
                event.event_type_id(),
                event.event_hash(),
            ))
            .unwrap();
            let route_key =
                encode_event_route_key(command.base().commit().partition_hash(), event.event_id());
            write
                .open_table(crate::layout::EVENT_ROUTES)
                .unwrap()
                .insert(route_key.as_slice(), route.as_bytes())
                .unwrap();
            if deliver {
                let status = StoredOutboxStatusV1::delivered(
                    event.event_id(),
                    std::num::NonZeroU32::MIN,
                    riffdb_storage_api::OutboxDestinationIdV1::new("fixture-sink").unwrap(),
                    Timestamp::new(1700000000, 0).unwrap(),
                );
                let encoded = codec::encode_outbox_status_v1(&status).unwrap();
                write
                    .open_table(OUTBOX_STATUS)
                    .unwrap()
                    .insert(key.as_slice(), encoded.as_bytes())
                    .unwrap();
                expected.add(encoded.as_bytes().len(), key.len()).unwrap();
            }
        }
    }
    if first > 1 {
        let watermark = StoredRetentionWatermarkV1::new(first - 1, 1, None).unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(
                META_RETENTION_WATERMARK,
                encode_retention_watermark_v1(&watermark)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    let head = codec::encode_application_sequence_allocator_v1(
        riffdb_storage_api::ApplicationSequenceAllocator::Next(
            CommitSequence::new(last + 2).unwrap(),
        ),
    )
    .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(META_APPLICATION_SEQUENCE, head.as_bytes())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    (scope, ports, expected, first, last)
}

#[test]
fn logical_bytes_preserve_segment_at_target_boundary_and_require_actual_delivery() {
    let (_scope, ports, expected, first, last) = segment_fixture(true);
    assert!(last >= first);
    let now = Instant::now();
    let mut probe = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    probe.advance(&ports, now).unwrap();
    assert_eq!(bytes(&probe, now), Some(expected));
    assert_eq!(probe.rows, 1);
    assert_eq!(probe.next, last + 1);
    let mut protected =
        Probe::capture(&ports, NonZeroU64::new(last - first + 2).unwrap(), now).unwrap();
    protected.advance(&ports, now).unwrap();
    assert_eq!(bytes(&protected, now), Some(Bytes::default()));
    let (_scope, ports, _, _, _) = segment_fixture(false);
    let mut pending = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    pending.advance(&ports, now).unwrap();
    assert_eq!(
        bytes(&pending, now),
        Some(Bytes::default()),
        "StepLocal fences alone do not prove delivery"
    );
}

#[test]
fn logical_bytes_cover_complete_original_prefix_beyond_one_deletion_batch() {
    let (_scope, ports) = fixture();
    let lengths = populate(&ports, 302);
    assert!(lengths.iter().max() > lengths.iter().min());
    let now = Instant::now();
    let mut probe = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    assert_eq!(probe.ceiling, 301);
    assert_eq!(bytes(&probe, now), None);
    probe.advance(&ports, now).unwrap();
    assert_eq!(probe.next, 257);
    assert_eq!(
        bytes(&probe, now),
        None,
        "the next deletion batch is not the backlog"
    );
    populate(&ports, 400);
    probe.advance(&ports, now).unwrap();
    assert_eq!(
        bytes(&probe, now),
        Some(Bytes {
            values: lengths[..301].iter().sum::<usize>() as u64,
            keys: 301 * 8
        })
    );
    assert_eq!(probe.pages, 2);
    assert_eq!(probe.rows, 301);
    assert!(probe.read.is_none());
    assert_eq!(
        ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Commits, &1u64.to_be_bytes(), None, 500)
            .unwrap()
            .len(),
        400,
        "accounting never prunes"
    );
}

#[test]
fn logical_bytes_preserve_original_operator_hold_and_refuse_unknown_prefix_as_zero() {
    let (_scope, ports) = fixture();
    let lengths = populate(&ports, 8);
    let write = ports.shared.database.begin_write().unwrap();
    let hold = riffdb_storage_api::StoredRetentionHoldsV1::new(vec![
        RetentionHoldV1::new("metric-hold", 4, "fixture").unwrap(),
    ])
    .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            META_RETENTION_HOLDS,
            encode_retention_holds_v1(&hold).unwrap().as_bytes(),
        )
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let now = Instant::now();
    let mut probe = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    let limit = probe.ceiling;
    assert!(limit < 7);
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(META_RETENTION_HOLDS)
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    probe.advance(&ports, now).unwrap();
    assert_eq!(
        bytes(&probe, now),
        Some(Bytes {
            values: lengths[..limit as usize].iter().sum::<usize>() as u64,
            keys: limit * 8
        })
    );
    let mut newer = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    assert_eq!(bytes(&newer, now), None);
    newer.advance(&ports, now).unwrap();
    assert!(newer.bytes.values > probe.bytes.values);
}

#[test]
fn logical_bytes_only_fresh_complete_empty_prefix_is_zero_and_expiry_releases_pin() {
    let (_scope, ports) = fixture();
    let now = Instant::now();
    let mut probe = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    assert_eq!(bytes(&probe, now), None);
    probe.advance(&ports, now).unwrap();
    assert_eq!(bytes(&probe, now), Some(Bytes::default()));
    assert!(probe.read.is_none());
    assert_eq!(bytes(&probe, now + MAX_AGE), None);
    populate(&ports, 300);
    let mut probe = Probe::capture(&ports, NonZeroU64::MIN, now).unwrap();
    let weak = match probe.read.as_ref().unwrap() {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            std::sync::Arc::downgrade(root)
        }
        RedbReadAccess::Composite(view) => {
            std::sync::Arc::downgrade(&view.checkpoint_root_shared())
        }
    };
    probe.advance(&ports, now).unwrap();
    assert!(weak.upgrade().is_some());
    // Retire the cache's legitimate current-root ownership through a real
    // durable publication before testing this observer's old-root lifetime.
    populate(&ports, 301);
    drop(ports.begin_composite_read().unwrap());
    probe.advance(&ports, now + MAX_AGE).unwrap();
    assert!(probe.read.is_none());
    assert!(weak.upgrade().is_none());
    assert_eq!(bytes(&probe, now + MAX_AGE), None);
}

#[test]
fn logical_bytes_gap_refuses_and_owner_retirement_releases_metric_work() {
    let (_scope, ports) = fixture();
    populate(&ports, 8);
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(COMMITS)
        .unwrap()
        .remove(2u64.to_be_bytes().as_slice())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let now = Instant::now();
    let mut observer = Observer {
        probe: Some(Probe::capture(&ports, NonZeroU64::MIN, now).unwrap()),
        next_capture: None,
    };
    assert_eq!(
        observer.advance(&ports, now).unwrap_err().kind(),
        StorageErrorKind::CorruptData
    );
    assert!(observer.probe.is_none());
    populate(&ports, 300);
    observer.probe = Some(Probe::capture(&ports, NonZeroU64::MIN, now).unwrap());
    observer.retire();
    assert!(observer.probe.is_none());
}

#[test]
fn logical_bytes_capture_rate_and_refusal_never_preserve_a_partial_measurement() {
    let (_scope, ports) = fixture();
    let mut observer = Observer::default();
    observer.tick(&ports, NonZeroU64::MIN).unwrap();
    assert!(observer.probe.is_none());
    let next_capture = observer.next_capture.unwrap();
    populate(&ports, 300);
    observer.tick(&ports, NonZeroU64::MIN).unwrap();
    assert!(
        observer.probe.is_none(),
        "a new publication cannot bypass the frozen capture interval"
    );
    assert_eq!(observer.next_capture, Some(next_capture));
    let now = Instant::now();
    observer.probe = Some(Probe::capture(&ports, NonZeroU64::MIN, now).unwrap());
    observer.advance(&ports, now).unwrap();
    assert!(!observer.probe.as_ref().unwrap().completed);
    observer.report_refusal(&storage_error(StorageErrorKind::LimitExceeded));
    assert!(observer.probe.is_none());
    assert_eq!(observer.next_capture, Some(next_capture));
}
