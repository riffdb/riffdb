#![forbid(unsafe_code)]
// req: REP-008, REP-009, PERF-008, END-003

//! WP-750 (`ADR-0178` / `ADR-0093`): receipted replication acceptance campaign
//! verification, `PERF-008` emitter-enabled stability verification under
//! `ADR-0171`, and deterministic simulator follower + archive-sink fault
//! campaign (`OBL-0178-7`).

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::process::Command;

use riffdb_sim::SplitMix64;
use riffdb_storage_api::{
    ArchiveConsumerErrorV1, ArchiveConsumerV1, ArchiveEncryptionPostureV1, ArchiveFrameSinkV1,
    ArchiveFrameV1, ArchiveManifestV1, AuthoritativeMutationV3, AuthoritativeNamespaceV1,
    AuthoritativeStateCursorV3, AuthoritativeStateRowV3, AuthoritativeStateStepV3,
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogCursorErrorV3, ChangelogFrameBindingV3, ChangelogFrameV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, ChangelogTransactionSequence, LeadershipEpochV1,
    ReplicationAuthorityClassV1,
};
use riffdb_testkit::model::{AuthoritativeNamespaceModel, NamespaceModelError};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};
use serde_json::Value;

const RECEIPT_REL_PATH: &str = "release/evidence/replication/replication-acceptance-v1.json";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

struct EmptyBaselineCursor {
    history: ChangelogHistoryStateV3,
    steps: VecDeque<AuthoritativeStateStepV3>,
}

impl AuthoritativeStateCursorV3 for EmptyBaselineCursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        Ok(self.steps.pop_front())
    }
}

fn empty_baseline_cursor(seed: u64) -> EmptyBaselineCursor {
    let mut rand_bytes = [0u8; 10];
    let seed_be = seed.to_be_bytes();
    rand_bytes[..8].copy_from_slice(&seed_be);
    rand_bytes[8] = seed_be[0] ^ 0x5a;
    rand_bytes[9] = seed_be[7] ^ 0xa5;
    let db_id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, rand_bytes)
        .expect("deterministic database id");
    let lineage =
        ChangelogLineageV3::new(db_id, 1, LeadershipEpochV1::new(1).expect("epoch 1")).unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).expect("seq 1"),
        [0x11; 32],
        DualFrontier::new(CommitSequence::new(1), None),
    );
    let steps = AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|ns| ns.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
        .map(AuthoritativeStateStepV3::EndNamespace)
        .collect();
    EmptyBaselineCursor {
        history: ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap(),
        steps,
    }
}

struct SeededArchiveSink {
    fail_next: bool,
    persisted_frames: Vec<Vec<u8>>,
    manifests: Vec<ArchiveManifestV1>,
    full_backup_digest: [u8; 32],
}

impl SeededArchiveSink {
    fn new(full_backup_digest: [u8; 32]) -> Self {
        Self {
            fail_next: false,
            persisted_frames: Vec::new(),
            manifests: Vec::new(),
            full_backup_digest,
        }
    }
}

impl ArchiveFrameSinkV1 for SeededArchiveSink {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        if self.fail_next {
            self.fail_next = false;
            return Err(ArchiveConsumerErrorV1::SinkUnavailable);
        }
        let manifest = match self.manifests.last() {
            None => ArchiveManifestV1::first(
                frame,
                self.full_backup_digest,
                ArchiveEncryptionPostureV1::Unencrypted,
            )?,
            Some(prev) => prev.next(frame)?,
        };
        let encoded = manifest.encode();
        let decoded = ArchiveManifestV1::decode(&encoded)?;
        decoded.verify_predecessor(self.manifests.last())?;
        decoded.verify_frame(frame.as_bytes())?;
        self.persisted_frames.push(frame.as_bytes().to_vec());
        self.manifests.push(decoded);
        Ok(())
    }
}

struct ExactModelStateCursor {
    history: ChangelogHistoryStateV3,
    steps: VecDeque<AuthoritativeStateStepV3>,
}

impl AuthoritativeStateCursorV3 for ExactModelStateCursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        Ok(self.steps.pop_front())
    }
}

fn cursor_from_entities(
    lineage: ChangelogLineageV3,
    anchor: ChangelogHistoryPointV3,
    tail: ChangelogHistoryPointV3,
    entities: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> ExactModelStateCursor {
    let mut steps = VecDeque::new();
    for ns in AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
    {
        if ns == AuthoritativeNamespaceV1::Entities {
            for (k, v) in entities {
                steps.push_back(AuthoritativeStateStepV3::Row(
                    AuthoritativeStateRowV3::new(ns, k, v).unwrap(),
                ));
            }
        }
        steps.push_back(AuthoritativeStateStepV3::EndNamespace(ns));
    }
    ExactModelStateCursor {
        history: ChangelogHistoryStateV3::new(lineage, anchor, tail, anchor).unwrap(),
        steps,
    }
}

fn run_seeded_simulator_follower_cell(seed: u64, receipts_per_seed: usize) {
    let mut rng = SplitMix64::new(seed);
    let mut base_primary = empty_baseline_cursor(seed);
    let mut base_follower = empty_baseline_cursor(seed);
    let mut primary = AuthoritativeNamespaceModel::capture(&mut base_primary).unwrap();
    let mut follower = AuthoritativeNamespaceModel::capture(&mut base_follower).unwrap();
    let anchor = primary.position();
    let lineage = primary.lineage();

    let sink = SeededArchiveSink::new([0x42; 32]);
    let mut archive = ArchiveConsumerV1::new(sink, lineage, anchor);
    let mut prior_frame_hash = anchor.history_hash();

    let mut live_entities: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut apply_faults = 0usize;
    let mut resume_faults = 0usize;
    let mut archive_faults = 0usize;

    for step in 0..receipts_per_seed {
        let key_slot = (rng.next_below(8) as u8) + b'a';
        let key = vec![b'k', key_slot];
        let new_val = format!("seed-{seed:016x}-step-{step}").into_bytes();

        let mutation = match live_entities.get(&key) {
            None => AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::Entities,
                &key,
                None,
                &new_val,
            )
            .unwrap(),
            Some(prior) if rng.chance(1, 4) => AuthoritativeMutationV3::delete_matching(
                AuthoritativeNamespaceV1::Entities,
                &key,
                prior,
            )
            .unwrap(),
            Some(prior) => AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::Entities,
                &key,
                prior,
                &new_val,
            )
            .unwrap(),
        };

        let prev_point = primary.position();
        let next_seq = prev_point.sequence().checked_next().unwrap();
        let next_app = CommitSequence::new(next_seq.get()).unwrap();
        let next_frontier = DualFrontier::new(Some(next_app), None);

        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage.database_id(),
                history_incarnation: lineage.history_incarnation(),
                predecessor: Some(prev_point.sequence()),
                sequence: next_seq,
                predecessor_frontier: prev_point.frontier(),
                covered_frontier: next_frontier,
                prior_history_hash: prev_point.history_hash(),
            },
            ChangelogAttributionV3::JournaledApplicationGroup,
            vec![mutation.clone()],
        )
        .unwrap();

        primary.apply(&receipt).unwrap();
        match mutation.value() {
            Some(v) => {
                live_entities.insert(key.clone(), v.to_vec());
            }
            None => {
                live_entities.remove(&key);
            }
        }

        // Fault 1: Follower apply fault (inject bad prior or bad predecessor hash, verify atomic refusal).
        if step == 2 || rng.chance(1, 3) {
            let before_fault = follower.clone();
            let mut bad_binding = receipt.binding();
            bad_binding.prior_history_hash = [0xff; 32];
            let bad_receipt = AuthoritativeTransactionV3::new(
                bad_binding,
                receipt.attribution(),
                receipt.mutations().to_vec(),
            )
            .unwrap();
            assert_eq!(
                follower.apply(&bad_receipt),
                Err(NamespaceModelError::Position),
                "follower must refuse receipt with mismatched predecessor history hash"
            );
            assert_eq!(
                follower, before_fault,
                "failed follower apply must leave model byte-identical"
            );
            apply_faults += 1;
        }

        follower.apply(&receipt).unwrap();
        assert_eq!(
            follower.apply(&receipt),
            Err(NamespaceModelError::Position),
            "duplicate receipt apply must be refused"
        );

        // Fault 2: Stream kill and gap-free resume (reset transport frame chain at confirmed position).
        if step == 5 || rng.chance(1, 3) {
            archive.begin_stream().unwrap();
            prior_frame_hash = archive.position().history_hash();
            resume_faults += 1;
        }

        let frame_binding = ChangelogFrameBindingV3::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch().get(),
            lineage.catalog_digest(),
            prior_frame_hash,
        )
        .unwrap();
        let frame = ChangelogFrameV3::new(frame_binding, vec![receipt]).unwrap();
        let frame_bytes = frame.encode().unwrap();
        let frame_checksum = *frame_bytes.last_chunk::<32>().unwrap();

        // Fault 3: Archive sink transient unavailability and idempotent pending-frame retry.
        if step == 8 || rng.chance(1, 3) {
            let sink_mut = archive.into_sink();
            let mut armed_sink = sink_mut;
            armed_sink.fail_next = true;
            // Re-wrap at current confirmed position and begin stream to sync prior_frame_checksum
            let confirmed = follower_prev_point(prev_point);
            let mut resumed_archive = ArchiveConsumerV1::new(armed_sink, lineage, confirmed);
            resumed_archive.begin_stream().unwrap();
            let rebound_binding = ChangelogFrameBindingV3::new(
                lineage.database_id(),
                lineage.history_incarnation(),
                lineage.leadership_epoch().get(),
                lineage.catalog_digest(),
                confirmed.history_hash(),
            )
            .unwrap();
            let rebound_frame =
                ChangelogFrameV3::new(rebound_binding, frame.receipts().to_vec()).unwrap();
            let rebound_bytes = rebound_frame.encode().unwrap();
            let rebound_checksum = *rebound_bytes.last_chunk::<32>().unwrap();

            assert_eq!(
                resumed_archive.append(rebound_bytes),
                Err(ArchiveConsumerErrorV1::SinkUnavailable)
            );
            assert!(resumed_archive.has_pending_frame());
            let advanced = resumed_archive.retry_pending().unwrap();
            assert!(!resumed_archive.has_pending_frame());
            assert_eq!(advanced, follower.position());
            prior_frame_hash = rebound_checksum;
            archive = resumed_archive;
            archive_faults += 1;
        } else {
            let advanced = archive.append(frame_bytes).unwrap();
            assert_eq!(advanced, follower.position());
            prior_frame_hash = frame_checksum;
        }
    }

    assert!(apply_faults > 0, "seed {seed} must exercise apply_fault");
    assert!(resume_faults > 0, "seed {seed} must exercise resume_fault");
    assert!(
        archive_faults > 0,
        "seed {seed} must exercise archive_sink_fault"
    );
    assert_eq!(
        follower, primary,
        "follower and primary models must match byte-for-byte"
    );
    assert_eq!(archive.position(), primary.position());

    let mut expected_cursor =
        cursor_from_entities(lineage, anchor, primary.position(), &live_entities);
    follower.verify(&mut expected_cursor).unwrap();

    let final_sink = archive.into_sink();
    assert_eq!(final_sink.manifests.len(), receipts_per_seed);
    for (idx, (manifest, bytes)) in final_sink
        .manifests
        .iter()
        .zip(final_sink.persisted_frames.iter())
        .enumerate()
    {
        let prev = idx.checked_sub(1).and_then(|i| final_sink.manifests.get(i));
        manifest.verify_predecessor(prev).unwrap();
        manifest.verify_frame(bytes).unwrap();
    }
}

const fn follower_prev_point(point: ChangelogHistoryPointV3) -> ChangelogHistoryPointV3 {
    point
}

fn central_three_spread(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[3] / sorted[1]
}

fn median_five(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[2]
}

#[test]
fn replication_acceptance_campaign_meets_every_adr_0093_criterion() {
    let root = workspace_root();
    let receipt_path = root.join(RECEIPT_REL_PATH);
    let raw = std::fs::read_to_string(&receipt_path).expect("read replication acceptance receipt");
    let doc: Value = serde_json::from_str(&raw).expect("parse replication acceptance receipt");

    assert_eq!(doc["schema"], "riffdb.replication-acceptance/v1");
    assert_eq!(doc["work_package"], "WP-750");

    // Verify all 6 ADR-0093 acceptance criteria are present and passed.
    let criteria = doc["criteria"].as_object().expect("criteria object");
    let expected = [
        (
            "prefix_exactness",
            1,
            "OBL-0178-1",
            "follower_applies_exact_prefix_byte_faithfully",
        ),
        (
            "gap_free_resume",
            2,
            "OBL-0178-2",
            "replication_stream_resumes_gap_free_after_repeated_kills",
        ),
        (
            "promotion_drill",
            3,
            "OBL-0178-4",
            "promotion_mints_incarnation_and_refuses_old_lineage_tokens",
        ),
        (
            "retention_fence",
            4,
            "OBL-0178-5",
            "retention_prune_refuses_to_pass_registered_follower_frontier",
        ),
        (
            "follower_reads",
            5,
            "OBL-0178-3",
            "follower_causal_read_waits_for_token_then_matches_primary",
        ),
        (
            "no_hot_path_regression",
            6,
            "OBL-0178-6",
            "archived_suffix_restore_recovers_to_last_archived_sequence",
        ),
    ];
    assert_eq!(criteria.len(), expected.len());
    for (name, index, obl, proof) in expected {
        let entry = &doc["criteria"][name];
        assert_eq!(entry["adr_0093_criterion"].as_u64(), Some(index));
        assert_eq!(entry["obligation_id"].as_str(), Some(obl));
        assert_eq!(entry["proof_test"].as_str(), Some(proof));
        assert_eq!(entry["status"].as_str(), Some("passed"));
    }

    // Verify kill-loop covers primary, follower, stream, and archive_sink.
    let kill_loop = &doc["kill_loop"];
    let completed = kill_loop["completed_iterations"]
        .as_u64()
        .expect("completed_iterations");
    assert_eq!(kill_loop["configured_iterations"].as_u64(), Some(completed));
    let mut sum_kills = 0u64;
    for target in ["primary", "follower", "stream", "archive_sink"] {
        let t = &kill_loop["target_summary"][target];
        let kills = t["kills_injected"].as_u64().expect("kills_injected");
        assert!(kills >= 1);
        assert_eq!(t["recoveries_verified"].as_u64(), Some(kills));
        assert_eq!(t["startup_validation_passed"].as_bool(), Some(true));
        assert_eq!(t["unattributed_gaps"].as_u64(), Some(0));
        assert_eq!(t["duplicate_applies"].as_u64(), Some(0));
        sum_kills += kills;
    }
    assert_eq!(sum_kills, completed);

    // Verify PERF-008 emitter-enabled vs emitter-disabled stability rule on both PERF-018 profiles.
    for profile in ["e2_standard_8_pd_ssd", "n1_standard_8_local_ssd"] {
        for cell_name in ["interactive_c32", "write_only_c32"] {
            let cell = &doc["perf_008_emitter_comparison"]["profiles"][profile][cell_name];
            let dis_tput: Vec<f64> = cell["emitter_disabled_throughput_ops_s"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            let en_tput: Vec<f64> = cell["emitter_enabled_throughput_ops_s"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            let dis_p95: Vec<f64> = cell["emitter_disabled_p95_us"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            let en_p95: Vec<f64> = cell["emitter_enabled_p95_us"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            assert_eq!(dis_tput.len(), 5);
            assert_eq!(en_tput.len(), 5);
            assert_eq!(dis_p95.len(), 5);
            assert_eq!(en_p95.len(), 5);
            assert!(central_three_spread(&dis_tput) <= 1.20);
            assert!(central_three_spread(&en_tput) <= 1.20);
            assert!(central_three_spread(&dis_p95) <= 1.20);
            assert!(central_three_spread(&en_p95) <= 1.20);
            let tput_ratio = median_five(&en_tput) / median_five(&dis_tput);
            let p95_ratio = median_five(&en_p95) / median_five(&dis_p95);
            assert!(
                (0.90..=1.10).contains(&tput_ratio),
                "{profile}.{cell_name} throughput ratio {tput_ratio}"
            );
            assert!(
                (0.90..=1.10).contains(&p95_ratio),
                "{profile}.{cell_name} p95 ratio {p95_ratio}"
            );
            assert_eq!(cell["within_adr_0171_stability_rule"].as_bool(), Some(true));
        }
    }

    // Run the deterministic simulator follower + archive campaign across all recorded seeds.
    let sim = &doc["simulator_follower_campaign"];
    let receipts_per_seed =
        usize::try_from(sim["receipts_per_seed"].as_u64().unwrap()).expect("receipts_per_seed");
    let seeds: Vec<u64> = sim["seeds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_u64().unwrap())
        .collect();
    assert!(seeds.len() >= 4);
    for seed in seeds {
        run_seeded_simulator_follower_cell(seed, receipts_per_seed);
    }

    // Verify scripts/replication-acceptance --self-test passes (including 10 negative cases).
    let status = Command::new(root.join("scripts/replication-acceptance"))
        .arg("--self-test")
        .current_dir(&root)
        .status()
        .expect("run scripts/replication-acceptance --self-test");
    assert!(
        status.success(),
        "scripts/replication-acceptance --self-test failed"
    );
}
