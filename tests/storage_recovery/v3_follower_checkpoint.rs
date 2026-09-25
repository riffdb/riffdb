//! The complete path's validated-prefix fast path, proven where it still runs.
//!
//! ADR-0182's bounded dirty path admits every primary startup whose
//! checkpoint loads, so the prefix skip, the sampled prefix windows, the
//! seeded entity chains, and the exact-end prefix-count check run only on a
//! follower's startup, which always takes the complete path.
use super::*;

/// A follower bootstrapped from a source whose validated-prefix checkpoint
/// covers three committed commands. A follower never writes a checkpoint of
/// its own: bootstrap carries the source's, so the source first takes a
/// complete walk (no covering checkpoint) that writes one at S=head. The
/// source path is returned so the follower's sibling file outlives the test.
fn checkpointed_follower(name: &str) -> (TestDatabasePath, PathBuf) {
    let source = TestDatabasePath::new(name);
    install_fixture(&source.0);
    {
        let (ports, _receiver) = observed_ports(&source.0, RedbCommitProfile::Hardened);
        for ordinal in 1..=3 {
            commit_command_fixture(&ports, &command_fixture_at(ordinal));
        }
    }
    super::super::super::strip_checkpoint_meta(&source.0);
    let (ports, _receiver) = observed_ports(&source.0, RedbCommitProfile::Hardened);
    let path = source.0.with_extension("follower.redb");
    let _ = bootstrap(&ports, &path);
    (source, path)
}

fn follower_session(path: &Path) -> riffdb_storage_redb::RedbStructuralEvidenceSession {
    RedbFollowerStore::open(path)
        .expect("open follower")
        .begin_structural_evidence(inputs())
        .expect("begin follower evidence")
}

#[test]
fn follower_checkpoint_roundtrip_takes_the_sampled_fast_path() {
    let (_source, path) = checkpointed_follower("follower-checkpoint-roundtrip");
    let (_, _, outcome, verified, sampled) =
        super::super::super::complete_session_observing_checkpoint(follower_session(&path));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(verified, "a follower open must verify its checkpoint");
    assert!(
        sampled > 0,
        "a verified checkpoint with S>0 must execute sampled windows (observed {sampled})"
    );
    drop(outcome);

    super::super::super::strip_checkpoint_meta(&path);
    let (_, _, outcome, verified_full, _) =
        super::super::super::complete_session_observing_checkpoint(follower_session(&path));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(!verified_full, "an absent checkpoint forces the full walk");
    drop(outcome);

    assert!(
        !super::super::super::checkpoint_meta_present(&path),
        "a follower's full walk writes no checkpoint of its own"
    );
}

#[test]
fn follower_checkpoint_samples_audit_rows_below_the_bound() {
    // S2: audit histories get nonzero below-S sampling, beyond the commit and
    // event classes alone.
    let (_source, path) = checkpointed_follower("follower-audit-sample-windows");
    let (_, _, outcome, verified, sampled) =
        super::super::super::complete_session_observing_checkpoint(follower_session(&path));
    assert!(verified);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(
        sampled > 2,
        "audit-class rows must be sampled below the bound (observed {sampled})"
    );
}

#[test]
fn follower_checkpoint_accepts_prefix_only_entities_via_seeded_chains() {
    // Every entity is prefix-only under the follower's S=head checkpoint: the
    // seeded chain carries the fingerprint-verified version but no post-image
    // hash, and the version-only arm must accept it without a false
    // MissingCrossLink.
    let (_source, path) = checkpointed_follower("follower-seeded-chain-prefix-only");
    let (_, _, outcome, verified, _) =
        super::super::super::complete_session_observing_checkpoint(follower_session(&path));
    assert!(verified, "the S=head checkpoint must verify");
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "prefix-only entities must validate via seeded chains"
    );
}

/// An under-reported but possible prefix count binds at load. On a follower
/// the complete walk's exact end refuses it. On a primary it admits the
/// ADR-0182 bounded dirty path, which never consumes those counts: the
/// accepted WP-760 boundary, corrected by the next complete walk.
#[test]
fn under_reported_checkpoint_counts_fail_closed_on_the_complete_walk_only() {
    let under_report = |base: &StoredValidatedPrefixCheckpointV1| {
        assert!(
            base.counts().commits_count >= 1,
            "fixture must record a non-empty commits prefix"
        );
        let mut counts = base.counts();
        counts.commits_count = 0;
        StoredValidatedPrefixCheckpointV1::new(
            base.database_id(),
            base.history_incarnation(),
            base.registry_digest(),
            base.checkpoint_commit_sequence(),
            base.audit_sequence_bound(),
            counts,
            base.entity_chain_fingerprint(),
            base.retained(),
            base.previous_checkpoint_hash(),
            0,
        )
        .expect("rehash under-reported counts")
    };

    let (_source, path) = checkpointed_follower("follower-count-mismatch");
    super::super::super::rewrite_checkpoint(&path, under_report);
    let mut session = follower_session(&path);
    assert!(
        session.checkpoint_verified(),
        "under-reported counts still bind at load; the walk end verifies them"
    );
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let failed_closed = loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                if !findings.is_empty() {
                    break true;
                }
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(_)) => break false,
            Err(_) => break true,
        }
    };
    assert!(
        failed_closed,
        "an under-reported below-S commits_count must fail closed at the walk end"
    );

    let primary = TestDatabasePath::new("primary-count-mismatch");
    let _ = super::super::super::prepare_committed_command_database(&primary.0);
    super::super::super::strip_checkpoint_meta(&primary.0);
    drop(super::super::super::complete_startup_pass(
        RedbStore::open(&primary.0).expect("write the S=head checkpoint"),
    ));
    super::super::super::rewrite_checkpoint(&primary.0, under_report);
    let (_, bounded_dirty) = super::super::super::startup_path_observation(&primary.0);
    assert!(
        bounded_dirty,
        "a possible count admits the bounded dirty path, which does not consume it"
    );
}
