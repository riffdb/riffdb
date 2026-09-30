//! Canonical command before images are identical for primary stubs and full rows.
// req: STO-030, STO-032, REC-005

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalMutationBuffer, JournalTable};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, CommandSegmentDigestV1,
    HistoryTombstoneContentDigest, HistoryTombstoneHash, StoredCommandSegmentV1,
    StoredHistoryTombstoneV1, StoredRetentionWatermarkV1,
};

#[test]
fn prune_overlay_checks_canonical_before_images_without_changing_receipt_hashes() {
    let wire =
        include_str!("../../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt")
            .lines()
            .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
            .expect("frozen segment");
    let bytes = (0..wire.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&wire[i..i + 2], 16).expect("hex"))
        .collect::<Vec<_>>();
    let fixture = riffdb_storage_api::decode_command_segment_v1(&bytes).expect("segment");
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(19, [0x86; 10]).expect("identity");
    // The codec vector intentionally contains opaque prefix images, which the
    // storage validator correctly rejects. Use its valid historical capsule
    // facts without claiming that those synthetic images are storage evidence.
    let commands = fixture
        .value()
        .commands()
        .iter()
        .map(|command| {
            riffdb_storage_api::StoredCommandCapsuleV2::from_base(
                command.base().clone(),
                command.index_generation_transitions().to_vec(),
            )
            .expect("historical capsule")
        })
        .collect::<Vec<_>>();
    let manifest = crate::application::build_command_segment_manifest(
        &commands,
        commands[0].commit_sequence(),
    )
    .expect("manifest");
    let draft = StoredCommandSegmentV1::new(
        database_id,
        1,
        None,
        commands,
        manifest,
        CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .expect("draft");
    let (segment, encoded) =
        riffdb_storage_api::seal_and_encode_command_segment_v1(draft).expect("seal segment");
    let key = crate::keys::encode_application_sequence_key(segment.first_commit_sequence());
    let first = segment.first_commit_sequence().get();
    let last = segment.last_commit_sequence().get();
    let administration = segment
        .commands()
        .last()
        .expect("command")
        .base()
        .terminal_audit()
        .administration_sequence();
    let frame_hash = [0x86; 32];
    let locator = crate::history_stub::StubLocator {
        database_id,
        generation: 1,
        placement: crate::journal::FramePlacement {
            position: 4096,
            padded_len: 4096,
            frame_hash,
        },
    };
    let stub = locator
        .segment_stub(encoded.as_bytes())
        .expect("stub")
        .expect("segment stub");
    let canonical_delete =
        JournalMutation::delete_matching(JournalTable::Commits, key.as_slice(), encoded.as_bytes())
            .expect("canonical delete");
    let tombstone = StoredHistoryTombstoneV1::new(
        first,
        last,
        last - first + 1,
        0,
        0,
        0,
        HistoryTombstoneContentDigest::from_bytes([3; 32]),
        Some(HistoryTombstoneHash::from_bytes([4; 32])),
        1,
    )
    .expect("tombstone");
    let watermark = StoredRetentionWatermarkV1::new(last, 1, Some(SchemaHash::from_bytes([5; 32])))
        .expect("watermark");
    let mutations = vec![
        canonical_delete.clone(),
        JournalMutation::put(
            JournalTable::HistoryTombstones,
            key.as_slice(),
            riffdb_storage_api::encode_history_tombstone_v1(&tombstone)
                .expect("encode")
                .as_bytes()
                .to_vec(),
        )
        .expect("tombstone put"),
        JournalMutation::put(
            JournalTable::Meta,
            crate::layout::META_RETENTION_WATERMARK.as_bytes(),
            riffdb_storage_api::encode_retention_watermark_v1(&watermark)
                .expect("encode")
                .as_bytes()
                .to_vec(),
        )
        .expect("watermark put"),
    ];
    let mut buffer = JournalMutationBuffer::default();
    buffer.extend(mutations).expect("buffer");
    let encoded_frame = JournalFrame::encode_buffered_retention_prune(
        database_id,
        CommitSequence::new(last),
        Some(administration),
        [0; 32],
        buffer,
        None,
    )
    .expect("frame");
    let (frame, _) = JournalFrame::decode(encoded_frame.as_bytes()).expect("decode frame");

    for use_stub in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("canonical-prune-overlay");
        let path = scope.join("db.redb");
        let mut store = crate::store::RedbStore::open(&path).expect("open");
        store
            .initialize_legacy_fixture(database_id)
            .expect("initialize");
        let resolver = crate::history_stub::HistoryResolver::register(
            database_id,
            &path,
            Arc::new(crate::media::RealJournalMedia),
        );
        let pending: Arc<[riffdb_storage_api::CompositeMutationV1]> = Arc::from(vec![
            riffdb_storage_api::CompositeMutationV1::put(
                CompositeTableV1::Commits,
                key.as_slice(),
                encoded.as_bytes(),
            )
            .expect("pending bytes"),
        ]);
        resolver.note_pending_frame(frame_hash, &pending);
        let write = store.shared.database.begin_write().expect("fixture write");
        write
            .open_table(crate::layout::COMMITS)
            .expect("commits")
            .insert(
                key.as_slice(),
                if use_stub {
                    stub.as_slice()
                } else {
                    encoded.as_bytes()
                },
            )
            .expect("fixture command");
        let app = crate::codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(CommitSequence::new(last + 1).expect("next")),
        )
        .expect("allocator");
        let admin = crate::codec::encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::Next(administration.checked_next().expect("next")),
        )
        .expect("allocator");
        {
            let mut meta = write.open_table(crate::layout::META).expect("meta");
            meta.insert(crate::layout::META_APPLICATION_SEQUENCE, app.as_bytes())
                .expect("app");
            meta.insert(
                crate::layout::META_ADMINISTRATION_SEQUENCE,
                admin.as_bytes(),
            )
            .expect("admin");
        }
        write.commit().expect("fixture commit");
        let root = Arc::new(CheckpointRoot::new(
            store
                .shared
                .database
                .begin_read()
                .expect("fixture snapshot"),
            0,
        ));
        read_commit_tail(&root).expect("fixture application tail");
        read_administration_tail(&root).expect("fixture administration tail");
        checkpoint_identity(&root, [0; 32]).expect("fixture checkpoint identity");
        crate::changelog_v3_cursor::ChangelogSuffix::from_checkpoint(&root)
            .expect("fixture changelog roots");
        let published = Arc::new(
            RedbCompositeViewBuilder::from_root(root, [0; 32])
                .expect("capture")
                .freeze(),
        );
        let mut stage = RedbCompositeMutationStage::from_published(&published);
        if use_stub {
            let physical_delete =
                JournalMutation::delete_matching(JournalTable::Commits, key.as_slice(), &stub)
                    .expect("physical hash");
            assert!(
                stage.apply_prune(&physical_delete).is_err(),
                "a physical locator hash cannot replace the canonical before image"
            );
            assert_eq!(
                stage.mutation_count(),
                0,
                "failed validation leaves private state unchanged"
            );
        }
        stage
            .apply_prune(&canonical_delete)
            .expect("canonical check");
        assert_eq!(
            stage.staged_mutations()[0].expected_hash(),
            canonical_delete.expected_hash()
        );
        assert_eq!(
            stage
                .resolve_point(CompositeTableV1::Commits, &key)
                .expect("pruned"),
            None
        );
        let mut recovery = RedbCompositeViewBuilder::from_published(&published);
        recovery
            .apply_frame(&frame)
            .expect("same canonical check on decoded recovery frame");
        let successor = recovery.freeze();
        assert_eq!(
            successor.overlay().terminal_frame_hash(),
            encoded_frame.frame_hash()
        );
        assert_eq!(
            successor
                .resolve_point(CompositeTableV1::Commits, &key)
                .expect("deleted"),
            None
        );
        assert!(
            published
                .resolve_point(CompositeTableV1::Commits, &key)
                .expect("old reader")
                .is_some(),
            "the pinned predecessor still owns its command"
        );
    }
}
