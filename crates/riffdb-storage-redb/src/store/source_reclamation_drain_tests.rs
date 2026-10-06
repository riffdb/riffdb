//! Exact metadata admission only; this does not confer source-cut authority.
// req: STO-030, STO-031, STO-032, REC-001
use super::*;
use crate::journal::{JournalFileHeader, JournalFrame, JournalLane, JournalMutation, JournalTable};

#[test]
fn source_validation_pages_finish_without_requesting_a_writer_turn() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    let live_pins = || {
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|root| root.strong_count() != 0)
            .count()
    };
    let pins_before = live_pins();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert_eq!(live_pins(), pins_before + 1);
    let durable_epoch = ports.shared.durable_commit_epoch();
    let before = ports.shared.mutation_gate.source_census().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    for _ in 0..16 {
        if !control.receipt_validation_pending() {
            break;
        }
        let [initial, continued, invalidated] = control.advance_validation_off_gate().unwrap();
        assert!(initial + continued <= 256);
        assert_eq!(invalidated, 0);
        assert_eq!(
            ports.shared.mutation_gate.source_census().unwrap(),
            before,
            "read-only validation must not request a source turn at exact end"
        );
    }
    assert!(!control.receipt_validation_pending());
    assert_eq!(ports.shared.durable_commit_epoch(), durable_epoch);
    assert_eq!(
        live_pins(),
        pins_before,
        "exact end releases the captured pin"
    );
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(ports.shared.mutation_gate.is_held());
    drop(writer);
}

#[test]
fn off_gate_validation_never_captures_a_missing_or_invalidated_candidate() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    let before = ports.shared.mutation_gate.source_census().unwrap();
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(!control.receipt_validation_pending());
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    let before = ports.shared.mutation_gate.source_census().unwrap();
    ports.shared.invalidate_source_continuation();
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0, 0, 1]);
    assert!(!control.receipt_validation_pending());
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert!(
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .all(|root| root.strong_count() == 0)
    );
}

#[test]
fn off_gate_validation_corruption_releases_proof_and_reader_without_a_writer_turn() {
    let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
    let transaction = ports.shared.database.begin_write().unwrap();
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .insert(
            states[2].tail().sequence().get().to_be_bytes().as_slice(),
            b"invalid-receipt".as_slice(),
        )
        .unwrap();
    ports.shared.commit_durable(transaction).unwrap();
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    let before = ports.shared.mutation_gate.source_census().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    let durable_epoch = ports.shared.durable_commit_epoch();
    let error = control.advance_validation_off_gate().unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert!(!control.receipt_validation_pending());
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert_eq!(ports.shared.durable_commit_epoch(), durable_epoch);
    assert!(
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .all(|root| root.strong_count() == 0)
    );
    assert!(ports.shared.mutation_gate.is_held());
    drop(writer);
}

#[test]
fn unfinished_extent_publication_yields_before_spending_or_waiting_for_a_root_pin() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut roots = Vec::new();
    for _ in 0..crate::store::extent_reclamation::MAX_EXTENT_READ_ROOTS {
        roots.push(ports.shared.capture_checkpoint_root().unwrap());
    }
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    *ports.shared.journal_sealer.lock().unwrap() = Some(std::thread::spawn(move || {
        entered.send(()).unwrap();
        released.recv().unwrap();
    }));
    entry.recv().unwrap();
    let lease = ports
        .shared
        .mutation_gate
        .acquire_source()
        .unwrap()
        .unwrap();
    let readiness = ports.shared.prepare_online_source_root();
    drop(lease);
    release.send(()).unwrap();
    ports.shared.join_journal_sealer();
    assert!(
        matches!(readiness, Ok(Readiness::CheckpointPublication)),
        "known unfinished publication must yield before attempting a root capture"
    );
    assert_eq!(ports.shared.extent_roots.lock().unwrap().len(), roots.len());
    assert!(!ports.shared.mutation_gate.is_held());
}

#[test]
fn source_owner_retirement_releases_its_actual_validation_reader_pin() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let live_pins = || {
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|root| root.strong_count() != 0)
            .count()
    };
    let before = live_pins();
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert_eq!(live_pins(), before + 1);
    ports.shared.retire_online_source_admission();
    control.retire_online_validation();
    assert_eq!(live_pins(), before);
    assert!(!control.receipt_validation_pending());
    assert!(!ports.shared.mutation_gate.is_held());
    assert!(
        ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .is_none()
    );
    drop(ports.shared.mutation_gate.acquire().unwrap());
}

#[test]
fn source_capture_does_not_initialize_an_absent_native_journal_lane() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert!(!ports.shared.mutation_gate.is_held());
}

fn runtime(path: &std::path::Path, value_bytes: usize) -> JournalRuntime {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x82; 10]).unwrap();
    let sequence = CommitSequence::new(1).unwrap();
    let encoded = JournalFrame::new(
        database_id,
        sequence,
        sequence,
        1,
        [0; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                crate::keys::encode_application_sequence_key(sequence),
                [0x41],
            )
            .unwrap(),
            JournalMutation::put(JournalTable::Entities, [0x10], vec![0x20; value_bytes]).unwrap(),
        ],
    )
    .unwrap()
    .encode()
    .unwrap();
    let lane = Arc::new(
        JournalLane::open(path, &JournalFileHeader::new(database_id, None, [0; 32])).unwrap(),
    );
    let length = encoded.as_bytes().len();
    let physical = crate::journal::extent_frame_bytes(length).unwrap();
    let frame = ValidatedCheckpointFrame {
        changelog_binding: None,
        database_id,
        predecessor_sequence: None,
        covered_sequence: Some(sequence),
        predecessor_administration_sequence: None,
        covered_administration_sequence: None,
        previous_hash: [0; 32],
        frame_hash: encoded.frame_hash(),
        transition_count: 1,
        command_count: 1,
        audit_count: 0,
        mutations: Arc::from(Vec::new()),
        extent_generation: lane.generation(),
        placement: crate::journal::frame_placement_after(0, &encoded).unwrap(),
        segment_summaries: Arc::from(Vec::new()),
        encoded,
    };
    JournalRuntime {
        changelog_history: None,
        lane,
        database_id,
        last_sequence: Some(sequence),
        last_administration_sequence: None,
        last_hash: frame.frame_hash,
        published_sequence: Some(sequence),
        published_administration_sequence: None,
        published_hash: frame.frame_hash,
        suffix_transitions: 1,
        suffix_commands: 1,
        suffix_audits: 0,
        suffix_bytes: length,
        suffix_physical_bytes: physical,
        suffix_frames: vec![frame],
        unpublished_transitions: 0,
        unpublished_commands: 0,
        unpublished_audits: 0,
        unpublished_bytes: 0,
        reanchor_required: false,
    }
}

#[test]
fn source_metadata_refuses_every_unknown_or_unpublished_charge_before_materializing() {
    for changed in 0..17 {
        let scope = crate::test_path::ScopedDirectory::new("source-drain-accounting");
        let mut runtime = runtime(&scope.join("journal.riffextent"), 1);
        assert!(suffix_is_bounded(&runtime));
        match changed {
            0 => runtime.suffix_transitions = 257,
            1 => runtime.suffix_bytes = crate::journal::SOURCE_SUFFIX_BYTES + 1,
            2 => runtime.suffix_bytes += 1,
            3 => runtime.suffix_transitions = 2,
            4 => runtime.suffix_commands = 2,
            5 => runtime.suffix_audits = 1,
            6 => runtime.unpublished_transitions = 1,
            7 => runtime.unpublished_commands = 1,
            8 => runtime.unpublished_audits = 1,
            9 => runtime.unpublished_bytes = 1,
            10 => runtime.last_sequence = CommitSequence::new(2),
            11 => runtime.last_administration_sequence = AdministrationSequence::new(1),
            12 => runtime.last_hash = [0; 32],
            13 => runtime.suffix_frames[0].placement.padded_len += 4096,
            14 => runtime.suffix_frames = vec![runtime.suffix_frames[0].clone(); 257],
            15 => runtime.suffix_frames[0].transition_count = 0,
            16 => runtime.suffix_frames.clear(),
            _ => unreachable!(),
        }
        assert!(!suffix_is_bounded(&runtime), "unknown charge {changed}");
        // The accounting check has neither submitted nor taken a frame.
        assert_eq!(
            crate::journal::scan_journal(
                &scope.join("journal.riffextent"),
                runtime.database_id,
                |_| Ok(())
            )
            .unwrap()
            .unwrap()
            .1
            .transition_count,
            0
        );
    }
}

#[test]
fn source_metadata_accepts_exact_closed_transition_and_encoded_byte_limits() {
    let scope = crate::test_path::ScopedDirectory::new("source-drain-boundary");
    let mut first = runtime(&scope.join("transitions.riffextent"), 1);
    first.suffix_frames = vec![first.suffix_frames[0].clone(); 256];
    first.suffix_transitions = 256;
    first.suffix_commands = 256;
    first.suffix_bytes *= 256;
    assert!(suffix_is_bounded(&first));
    let base = runtime(&scope.join("base.riffextent"), 1);
    let value_bytes = 1 + crate::journal::SOURCE_SUFFIX_BYTES - base.suffix_bytes;
    let exact = runtime(&scope.join("exact.riffextent"), value_bytes);
    assert_eq!(exact.suffix_bytes, crate::journal::SOURCE_SUFFIX_BYTES);
    assert!(suffix_is_bounded(&exact));
    let excess = runtime(&scope.join("excess.riffextent"), value_bytes + 1);
    assert!(!suffix_is_bounded(&excess));
}
