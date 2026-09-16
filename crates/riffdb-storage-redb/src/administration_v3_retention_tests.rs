//! Real service-audit publication, checkpointing, consumer fencing and reclamation.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::changelog_v3_activation::HISTORY;
use crate::store::{RedbCommitProfile, RedbDormantPorts, RedbOperationalPorts, RedbStore};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    AuthoritativeStateStepV3, ChangelogAttributionV3 as Source, ChangelogHistoryStateV3,
    ChangelogLineageV3, DatabaseInitializationPort, LeadershipEpochV1, PublishedDurableSnapshot,
    PublishedFrontierAdvancement, ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldV1 as Hold,
};
use std::sync::{Arc, mpsc};

#[derive(Debug)]
struct Observer(mpsc::SyncSender<PublishedFrontierAdvancement>);
impl riffdb_storage_api::ChangelogPublicationPort for Observer {
    fn observe_published_advancement(&self, pin: PublishedFrontierAdvancement) {
        self.0.try_send(pin).unwrap();
    }
}

#[derive(Debug)]
struct V3Observer(mpsc::SyncSender<Arc<dyn PublishedDurableSnapshot>>);
impl riffdb_storage_api::ChangelogPublicationPort for V3Observer {
    fn observe_published_advancement(&self, _: PublishedFrontierAdvancement) {}

    fn observe_published_snapshot_v3(&self, pin: Arc<dyn PublishedDurableSnapshot>) {
        self.0.try_send(pin).unwrap();
    }
}

#[test]
fn control_only_publication_pins_exact_history_without_advancing_application_frontier() {
    let scope = crate::test_path::ScopedDirectory::new("v3-control-publication");
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut store = RedbStore::open_with_changelog_publication_port(
        scope.join("db.redb"),
        RedbCommitProfile::Standard,
        Arc::new(V3Observer(sender)),
    )
    .unwrap();
    store
        .initialize_database(super::tests::database_id())
        .unwrap();
    let lineage =
        ChangelogLineageV3::new(super::tests::database_id(), 1, LeadershipEpochV1::initial())
            .unwrap();
    let mut transaction = store.shared.database.begin_write().unwrap();
    let anchor = crate::changelog_v3_activation::stage_validated(
        &mut transaction,
        lineage,
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    store.shared.commit_durable(transaction).unwrap();
    let ports = RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    while receiver.try_recv().is_ok() {}
    let mut control = ports.replication_source_control();
    let hold = Hold::new(
        ReplicationSourceHoldIdV1::new([0x76; 16]).unwrap(),
        Kind::Bootstrap,
        lineage,
        anchor.tail(),
    );
    assert!(control.register(hold).unwrap());
    let pin = receiver
        .try_recv()
        .expect("control-only receipt must publish a V3 pin");
    assert!(!control.register(hold).unwrap());
    assert!(receiver.try_recv().is_err(), "exact retry must not publish");

    // Cursor reads remain independent of the exclusive mutation capability.
    let _writer = ports
        .begin_attributed_write(Source::CatalogAdministration)
        .unwrap();
    let mut cursor = pin.changelog_receipts_v3(lineage, anchor.tail()).unwrap();
    let receipt = cursor.next_receipt().unwrap().unwrap();
    assert_eq!(receipt.attribution(), Source::ReplicationSourceHold);
    assert!(receipt.mutations().is_empty());
    assert_eq!(
        receipt.binding().predecessor_frontier,
        anchor.tail().frontier()
    );
    assert_eq!(receipt.binding().covered_frontier, anchor.tail().frontier());
    assert_eq!(
        receipt.binding().sequence,
        cursor.history().tail().sequence()
    );
    assert!(cursor.next_receipt().unwrap().is_none());
}

fn submit(
    ports: &mut RedbOperationalPorts,
    receiver: &mpsc::Receiver<PublishedFrontierAdvancement>,
    request: u8,
) -> PublishedFrontierAdvancement {
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[super::tests::denied_audit(request)])
        .unwrap()
    else {
        panic!("standard service audit must use its real journal owner");
    };
    fence.wait().unwrap();
    receiver.try_recv().unwrap()
}

fn checkpoint(ports: &RedbOperationalPorts) -> ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}

fn authority(
    snapshot: &dyn PublishedDurableSnapshot,
) -> BTreeMap<(riffdb_storage_api::AuthoritativeNamespaceV1, Vec<u8>), Vec<u8>> {
    let mut cursor = snapshot.authoritative_state_v3().unwrap();
    let mut rows = BTreeMap::new();
    let mut ends = 0;
    for _ in 0..512 {
        match cursor.next_item().unwrap() {
            Some(AuthoritativeStateStepV3::Row(row)) => {
                let (namespace, key, value) = row.into_parts();
                assert!(
                    rows.insert((namespace, key.into_vec()), value.into_vec())
                        .is_none()
                );
            }
            Some(AuthoritativeStateStepV3::EndNamespace(_)) => ends += 1,
            None => {
                assert_eq!(ends, 53);
                return rows;
            }
        }
    }
    panic!("bounded audit fixture inventory did not end");
}

#[test]
fn changelog_history_reclamation_respects_checkpoint_and_fences() {
    let scope = crate::test_path::ScopedDirectory::new("v3-real-retention");
    let path = scope.join("db.redb");
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut store = RedbStore::open_with_changelog_publication_port(
        &path,
        RedbCommitProfile::Standard,
        Arc::new(Observer(sender)),
    )
    .unwrap();
    store
        .initialize_database(super::tests::database_id())
        .unwrap();
    let lineage =
        ChangelogLineageV3::new(super::tests::database_id(), 1, LeadershipEpochV1::initial())
            .unwrap();
    let mut transaction = store.shared.database.begin_write().unwrap();
    let anchor = crate::changelog_v3_activation::stage_validated(
        &mut transaction,
        lineage,
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    store.shared.commit_durable(transaction).unwrap();
    let mut ports = RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    let mut pins = Vec::new();
    let mut points = Vec::new();
    let mut originals = BTreeMap::new();
    for request in 90..93 {
        let pin = submit(&mut ports, &receiver, request);
        let mut cursor = pin
            .snapshot()
            .changelog_receipts_v3(lineage, anchor.tail())
            .unwrap();
        points.push(cursor.history().tail());
        while let Some(receipt) = cursor.next_receipt().unwrap() {
            assert_eq!(receipt.attribution(), Source::JournaledServiceAudit);
            assert!(!receipt.mutations().is_empty());
            let key = receipt.binding().sequence.get();
            let bytes = receipt.encode().unwrap();
            if let Some(prior) = originals.insert(key, bytes.clone()) {
                assert_eq!(prior, bytes);
            }
        }
        pins.push(pin);
    }
    assert_eq!(
        checkpoint(&ports),
        anchor,
        "published journal receipts need not yet be materialized"
    );
    let mut control = ports.replication_source_control();
    let fence = |kind, index: usize| {
        Hold::new(
            ReplicationSourceHoldIdV1::new([kind as u8; 16]).unwrap(),
            kind,
            lineage,
            points[index],
        )
    };
    for (kind, index) in [
        (Kind::FollowerAcknowledgement, 0),
        (Kind::ArchiveAcknowledgement, 1),
        (Kind::Bootstrap, 2),
    ] {
        assert!(control.register(fence(kind, index)).unwrap());
    }
    let read = ports.shared.database.begin_read().unwrap();
    for (sequence, bytes) in &originals {
        assert_eq!(
            read.open_table(HISTORY)
                .unwrap()
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value(),
            bytes,
            "the draining barrier must materialize each original receipt before releasing its suffix source"
        );
    }
    drop(read);
    assert!(!control.reclaim_history().unwrap());
    assert!(!control.reclaim_history().unwrap());
    let newest = submit(&mut ports, &receiver, 93);
    let before_prune = authority(newest.snapshot().as_ref());
    assert!(control.reclaim_history().unwrap());
    assert_eq!(checkpoint(&ports).minimum_resume(), points[0]);
    assert!(
        control
            .advance_acknowledgement(fence(Kind::FollowerAcknowledgement, 2))
            .unwrap()
    );
    assert!(control.reclaim_history().unwrap());
    assert_eq!(checkpoint(&ports).minimum_resume(), points[1]);
    assert!(
        control
            .advance_acknowledgement(fence(Kind::ArchiveAcknowledgement, 2))
            .unwrap()
    );
    assert!(control.reclaim_history().unwrap());
    assert_eq!(checkpoint(&ports).minimum_resume(), points[2]);
    let current =
        crate::changelog::RedbPublishedSnapshot::new(ports.begin_composite_read().unwrap());
    assert_eq!(
        authority(&current),
        before_prune,
        "no authoritative byte changes during source maintenance"
    );
    assert!(
        matches!(current.changelog_receipts_v3(lineage, anchor.tail()),
        Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error)) if error.kind() == StorageErrorKind::HistoryPruned)
    );
    for (index, pin) in pins.iter().enumerate() {
        let mut cursor = pin
            .snapshot()
            .changelog_receipts_v3(lineage, anchor.tail())
            .unwrap();
        assert_eq!(cursor.history().tail(), points[index]);
        while let Some(receipt) = cursor.next_receipt().unwrap() {
            assert_eq!(
                receipt.encode().unwrap(),
                originals[&receipt.binding().sequence.get()]
            );
        }
    }
    let expected = checkpoint(&ports);
    drop(current);
    drop(newest);
    drop(pins);
    drop(receiver);
    drop(control);
    drop(ports);
    for _ in 0..2 {
        let store = RedbStore::open(&path).unwrap();
        let ports = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .unwrap();
        assert_eq!(checkpoint(&ports), expected);
    }
    crate::changelog_v3_control_tests::v3_source_control_crashes_leave_whole_holds_or_whole_reclamation();
}
