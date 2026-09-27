//! ADR-0182 section 6 online retention (WP-761) under seeded crash windows.
//!
//! One online step is one administration-class transition through the
//! ordered writer: its row deletes, tombstone append, watermark advance, and
//! validated-prefix checkpoint deletion commit together. The sweep crashes
//! the simulated disk at every countdown window across that step and proves
//! every recovery reopens through the full structural and catalog pass with
//! zero findings, lands on exactly the pre-step or the post-step watermark
//! (never a partial prune), keeps every entity and every idempotency answer,
//! advances no allocator, and resumes to the retained-sequence target.
// req: STO-032, REC-001, REC-002, SIM-003

use super::*;
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AuthoritativePointReader, OutboxClaimV1,
    OutboxDestinationIdV1, OutboxRepository, OutboxStatusObservationV1, OutboxSucceedV1,
    OutboxTransitionResultV1, StorageErrorKind,
};
use riffdb_storage_redb::{OnlineRetentionHold, OnlineRetentionStep};

fn deliver(ports: &mut RedbOperationalPorts, fixture: &CommandFixture) {
    let claim = OutboxClaimV1::new(
        fixture.records.events()[0].event_id(),
        OutboxStatusObservationV1::AbsentInitialPending,
        OutboxDestinationIdV1::new("sim-online-retention").expect("destination"),
        Timestamp::new(1_700_000_010, 0).expect("claim time"),
        Timestamp::new(1_700_000_020, 0).expect("lease time"),
    )
    .expect("claim");
    let OutboxTransitionResultV1::Applied(delivering) =
        ports.claim_outbox(&claim).expect("claim outbox")
    else {
        panic!("a fresh event claim applies");
    };
    let success = OutboxSucceedV1::new(
        delivering,
        Timestamp::new(1_700_000_011, 0).expect("success time"),
    )
    .expect("success");
    assert!(matches!(
        ports.succeed_outbox(&success).expect("succeed outbox"),
        OutboxTransitionResultV1::Applied(_)
    ));
}

fn retain() -> NonZeroU64 {
    NonZeroU64::new(1).expect("retained sequences")
}

fn allowance() -> NonZeroU64 {
    NonZeroU64::new(riffdb_storage_redb::ONLINE_RETENTION_STEP_SEQUENCES).expect("allowance")
}

/// Whether the step's prefix (sequences 1 and 2) is pruned, asserting it
/// moved as one unit and that the retained head never moved.
fn prefix_pruned(ports: &RedbOperationalPorts, window: u64) -> bool {
    let read = |sequence: u64| {
        ports
            .read_commit(CommitSequence::new(sequence).expect("sequence"))
            .map(|commit| commit.is_some())
            .map_err(|error| error.kind())
    };
    let first = read(1);
    assert_eq!(
        first,
        read(2),
        "a step prunes whole or not at all (window {window})"
    );
    assert_eq!(
        read(3),
        Ok(true),
        "the retained head survives (window {window})"
    );
    match first {
        Ok(true) => false,
        Err(StorageErrorKind::HistoryPruned) => true,
        other => panic!("unexpected prefix read {other:?} (window {window})"),
    }
}

/// The durable state every recovery keeps on either side of the step.
fn assert_retained_state(ports: &RedbOperationalPorts, fixtures: &[CommandFixture], window: u64) {
    for fixture in fixtures {
        assert!(
            matches!(
                ports
                    .lookup_admission(fixture.candidates.clone())
                    .expect("lookup admission"),
                AdmissionLookupResultV1::Found(_)
            ),
            "every command keeps its idempotency answer (window {window})"
        );
    }
    // Commands two and three write one key; three's image is current.
    for fixture in [&fixtures[0], &fixtures[2]] {
        assert_eq!(
            ports.read_entity(&fixture.target).expect("read entity"),
            Some(fixture.records.entities()[0].post_image().clone()),
            "retention leaves entity state untouched (window {window})"
        );
    }
}

struct RetentionSweep {
    crashed: u32,
    torn: u32,
    saw_before: bool,
    saw_after: bool,
}

fn sweep_online_retention_windows(seed: u64) -> RetentionSweep {
    let mut sweep = RetentionSweep {
        crashed: 0,
        torn: 0,
        saw_before: false,
        saw_after: false,
    };
    for window in (1..=160_u64).step_by(2) {
        let disk = SimDisk::new(FaultConfig::quiet(seed));
        let mut ports = prepare_simulated(&disk);
        let fixtures = oracle_fixtures();
        let _ = commit_workload_with_model(&ports, &fixtures);
        deliver(&mut ports, &fixtures[0]);
        deliver(&mut ports, &fixtures[1]);
        assert!(ports.has_undelivered_outbox().expect("outbox state"));

        disk.set_crash_after_operations(Some((window, window + 1)));
        let attempt = ports.online_retention_step(retain(), allowance());
        drop(ports);
        if !disk.is_crashed() {
            // The window exceeded the whole step's operation count.
            assert_eq!(
                attempt.expect("an uncrashed step completes"),
                OnlineRetentionStep::Advanced {
                    through: 2,
                    sequences: 2
                }
            );
            continue;
        }
        sweep.crashed += 1;
        let unsynced = disk.unsynced_mutation_count();
        disk.recover_after_crash();
        disk.set_crash_after_operations(None);
        let counters = disk.counters();
        let torn = counters.torn_kept + counters.torn_dropped + counters.torn_truncated;
        assert_eq!(
            torn, unsynced,
            "one seeded decision per unsynced mutation (window {window})"
        );
        if torn > 0 {
            sweep.torn += 1;
        }

        // The full structural and catalog pass with zero findings.
        let ports = open_operational(open_simulated(&disk));
        let pruned = prefix_pruned(&ports, window);
        if matches!(attempt, Ok(OnlineRetentionStep::Advanced { .. })) {
            assert!(pruned, "an acknowledged step is durable (window {window})");
        }
        if pruned {
            sweep.saw_after = true;
        } else {
            sweep.saw_before = true;
        }
        assert_retained_state(&ports, &fixtures, window);

        // Resume: the lane reaches the target from either side, idempotently.
        let resumed = ports
            .online_retention_step(retain(), allowance())
            .expect("resumed step");
        let expected = if pruned {
            OnlineRetentionStep::Held(OnlineRetentionHold::TargetReached)
        } else {
            OnlineRetentionStep::Advanced {
                through: 2,
                sequences: 2,
            }
        };
        assert_eq!(resumed, expected, "resume (window {window})");
        assert_eq!(
            ports
                .online_retention_step(retain(), allowance())
                .expect("step at the target"),
            OnlineRetentionStep::Held(OnlineRetentionHold::TargetReached)
        );
        // No allocator advance: the next command still commits at 4.
        commit_command_fixture(
            &ports,
            &build_command_fixture(4, 4, None, AdmissionShape::VacantTerminal),
        );
        drop(ports);
        let ports = open_operational(open_simulated(&disk));
        assert!(prefix_pruned(&ports, window));
        assert_retained_state(&ports, &fixtures, window);
    }
    sweep
}

/// OBL-0182-5 under simulation: every seeded crash window across one online
/// step recovers to exactly the pre-step or post-step watermark.
#[test]
fn every_online_retention_crash_window_recovers_to_a_valid_watermark() {
    let mut saw_before = false;
    let mut saw_after = false;
    for seed in [0x51C_0761_u64, 0x51C_0762] {
        let sweep = sweep_online_retention_windows(seed);
        assert!(sweep.crashed > 0, "no window crashed (seed {seed:#x})");
        assert!(
            sweep.torn > 0,
            "no window caught the step unsynced (seed {seed:#x})"
        );
        saw_before |= sweep.saw_before;
        saw_after |= sweep.saw_after;
    }
    assert!(saw_before, "no window recovered to the pre-step watermark");
    assert!(saw_after, "no window recovered to the post-step watermark");
}
