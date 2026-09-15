//! Full validation and independent byte-model comparison at the follower's pin.
use super::support::{now, open_primary};
use riffdb_catalog::{CatalogHistoryOutcome, validate_catalog_history};
use riffdb_storage_api::*;
use riffdb_storage_redb::{RedbFollowerApplier, RedbFollowerStore};
use riffdb_testkit::model::AuthoritativeNamespaceModel;
use riffdb_types::*;
use std::path::Path;
use std::sync::{Arc, atomic::AtomicBool};

pub(super) fn baseline(path: &Path) -> AuthoritativeNamespaceModel {
    let primary = open_primary(path);
    let mut cursor = primary
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap();
    AuthoritativeNamespaceModel::capture(cursor.as_mut()).unwrap()
}

fn follower(path: &Path) -> RedbFollowerApplier {
    let key = DigestKeyId::new(1).unwrap();
    let inputs = StartupValidationInputs::new(
        now(),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(key)]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(key)]).unwrap(),
    );
    let mut session = RedbFollowerStore::open(path)
        .unwrap()
        .begin_structural_evidence_cancellable(inputs, Arc::new(AtomicBool::new(false)))
        .unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let end = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "follower structural validation failed");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, history_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(history, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session.finish(end, history_end).unwrap() else {
        panic!("follower startup was not clean")
    };
    opened
        .into_parts()
        .3
        .into_follower_after_catalog_validation()
        .unwrap()
}

pub(super) fn capture_follower(path: &Path) -> AuthoritativeNamespaceModel {
    let applier = follower(path);
    let mut cursor = applier.authoritative_state_v3().unwrap();
    let model = AuthoritativeNamespaceModel::capture(cursor.as_mut()).unwrap();
    drop(cursor);
    applier.close().unwrap();
    model
}

pub(super) fn compare_prefixes(
    mut model: AuthoritativeNamespaceModel,
    primary: &Path,
    prefixes: &[AuthoritativeNamespaceModel],
) {
    assert!(!prefixes.is_empty() && prefixes.len() <= 8);
    for pair in prefixes.windows(2) {
        assert!(pair[0].position().sequence() < pair[1].position().sequence());
    }
    let primary = open_primary(primary);
    let source = primary.published_changelog_snapshot_v3().unwrap();
    let mut receipts = source
        .changelog_receipts_v3(model.lineage(), model.position())
        .unwrap();
    let mut compared = 0;
    for _ in 0..8192 {
        if let Some(expected) = prefixes.get(compared)
            && expected.position() == model.position()
        {
            assert_eq!(
                &model, expected,
                "recovered prefix differs from the independent source transition model"
            );
            compared += 1;
        }
        let Some(receipt) = receipts.next_receipt().unwrap() else {
            assert_eq!(
                compared,
                prefixes.len(),
                "a recovered prefix is absent from source history"
            );
            model
                .verify(source.authoritative_state_v3().unwrap().as_mut())
                .unwrap();
            return;
        };
        model.apply(&receipt).unwrap();
    }
    panic!("source exceeded the explicit campaign receipt bound");
}

pub(super) fn compare(
    mut model: AuthoritativeNamespaceModel,
    primary: &Path,
    replica: &Path,
    last_commit: u64,
) {
    let applier = follower(replica);
    let mut replica_state = applier.authoritative_state_v3().unwrap();
    let target = replica_state.history().tail();
    assert_eq!(
        target.frontier().application().map(|v| v.get()),
        Some(last_commit)
    );
    let primary = open_primary(primary);
    let source = primary.published_changelog_snapshot_v3().unwrap();
    let mut receipts = source
        .changelog_receipts_v3(model.lineage(), model.position())
        .unwrap();
    let mut compared = false;
    // Bounded campaign: no missing position can make this scan run forever.
    for _ in 0..8192 {
        if model.position() == target {
            model.verify(replica_state.as_mut()).unwrap();
            compared = true;
        }
        let Some(receipt) = receipts.next_receipt().unwrap() else {
            assert!(
                compared,
                "source receipt chain never reached the follower's exact hash/frontier"
            );
            model
                .verify(source.authoritative_state_v3().unwrap().as_mut())
                .unwrap();
            drop(replica_state);
            applier.close().unwrap();
            return;
        };
        model.apply(&receipt).unwrap();
    }
    panic!("source exceeded the explicit campaign receipt bound");
}
