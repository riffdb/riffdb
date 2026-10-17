//! Genuine location-only race on the ordinary retained-history validator.
//! This fresh tiny fixture never opens the protected original failure media.
use super::*;
use crate::error::storage_error;
use crate::source_validation_read::ReceiptRead;
use crate::{RedbOperationalPorts, RedbStore};
use riffdb_storage_api::{
    AuditPrincipalV1, AuthoritativeStateCatalogV2, DatabaseInitializationPort, EvidencePageLimit,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditGroupAppend,
    StorageError, StorageErrorKind,
};
use riffdb_types::{
    ActorId, ActorKind, CapabilityId, DatabaseId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
    Timestamp,
};
use std::{
    cell::RefCell,
    num::NonZeroU64,
    sync::mpsc::{Receiver, SyncSender},
    time::Duration,
};

type CandidatePause = (PathBuf, SyncSender<()>, Receiver<()>);
thread_local! {static CANDIDATE_PAUSE:RefCell<Option<CandidatePause>>=const{RefCell::new(None)};}
pub(super) fn pause_before_candidate_open(path: &Path) -> Result<(), JournalIoError> {
    let pause = CANDIDATE_PAUSE.with(|slot| {
        let selected = slot
            .borrow()
            .as_ref()
            .is_some_and(|(wanted, _, _)| wanted == path);
        selected.then(|| slot.take()).flatten()
    });
    if let Some((_, entered, release)) = pause {
        entered.send(()).map_err(|_| JournalIoError::Stopped)?;
        release
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| JournalIoError::Stopped)?;
    }
    Ok(())
}
fn audit_intent(marker: u8) -> ServiceAuditAppendIntentV1 {
    ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(u64::from(marker), [marker; 10]).unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("rotation-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(31, [0x32; 10]).unwrap(),
            NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap()
}
fn complete_walk(
    mut walk: crate::changelog_v3_retained_validation::RetainedHistoryValidation,
) -> Result<(), StorageError> {
    for _ in 0..16 {
        if walk.advance(EvidencePageLimit::new(256).unwrap())? {
            return Ok(());
        }
    }
    Err(storage_error(StorageErrorKind::LimitExceeded))
}

// req: STO-030, STO-032, REC-001
#[test]
fn ordinary_retained_validation_survives_real_location_only_rotation() {
    let directory = crate::test_path::ScopedDirectory::new("ordinary-retention-real-relocation");
    let database = directory.join("db.redb");
    let mut store =
        RedbStore::open_with_commit_profile(&database, crate::RedbCommitProfile::Standard).unwrap();
    store
        .initialize_database(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_031, [0x39; 10]).unwrap(),
        )
        .unwrap();
    let mut ports = ready_ports(store);
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[audit_intent(31)])
        .unwrap()
    else {
        panic!("genuine_standard_journal_ack_required");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
    // Existing direct materialization retains the actual active extent. No
    // test-authored frame, placement, receipt or rename enters this fixture.
    let actual = ports.materialize_and_capture_retention_fixture().unwrap();
    let source = crate::source_validation_pin::ValidationSource::Checkpoint(Arc::clone(
        actual.checkpoint_pin(),
    ));
    let history = source.history().unwrap().unwrap();
    assert_eq!(
        history.lineage().catalog_digest(),
        AuthoritativeStateCatalogV2.digest()
    );
    let sequence = history.tail().sequence();
    let table = source
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = table
        .get(sequence.get().to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let stub = crate::history_stub::HistoryStub::decode(row.value())
        .unwrap()
        .unwrap();
    assert_eq!(stub.kind, crate::history_stub::StubKind::Receipt);
    drop(row);
    drop(table);
    let active = journal_path(&database);
    let sealed = sealed_journal_path(&database, stub.generation);
    assert_eq!(
        extent_generation_with_media(&RealJournalMedia, &active).unwrap(),
        Some(stub.generation)
    );
    assert!(!sealed.exists(), "captured_generation_must_start_active");
    let before = source
        .receipt(sequence, None)
        .unwrap()
        .unwrap()
        .encode()
        .unwrap();
    let before_history = crate::changelog_v3_roots::validate_retained_history(&source)
        .unwrap()
        .unwrap();
    assert_eq!(before_history, history);
    // Keep the captured checkpoint immutable while a later genuine acknowledged
    // audit supplies the suffix that the real checkpoint worker rotates.
    let ServiceAuditGroupAppend::Submitted(second) = ports
        .submit_service_audit_group(&[audit_intent(32)])
        .unwrap()
    else {
        panic!("second_genuine_standard_ack_required");
    };
    assert_eq!(second.wait().unwrap().len(), 1);
    eprintln!("riffdb-private-ordinary-retention-rotation-v1 stage=1 result=0 kind=0");
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let reader_source = source.clone();
    let reader = std::thread::spawn(move || {
        let walk = crate::changelog_v3_retained_validation::RetainedHistoryValidation::new_source(
            reader_source,
        )?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        assert!(CANDIDATE_PAUSE.with(|slot| {
            slot.replace(Some((active, entered_tx, release_rx)))
                .is_none()
        }));
        complete_walk(walk)
    });
    entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    eprintln!("riffdb-private-ordinary-retention-rotation-v1 stage=2 result=0 kind=0");
    // Exact actual1761 helper below drives native rotation, installation and
    // asynchronous sealer to completion while the reader owns its real pin.
    ports
        .checkpoint_retention_relocation_fixture_for_test()
        .unwrap();
    assert!(
        sealed.exists(),
        "real_sealer_must_publish_same_captured_generation"
    );
    release_tx.send(()).unwrap();
    let result = reader.join().unwrap();
    let after = source
        .receipt(sequence, None)
        .unwrap()
        .unwrap()
        .encode()
        .unwrap();
    assert!(
        before == after,
        "same_pinned_canonical_receipt_must_remain_identical"
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&source).unwrap(),
        Some(history)
    );
    eprintln!("riffdb-private-ordinary-retention-rotation-v1 stage=3 result=0 kind=0");
    let kind = match result.as_ref().err().map(StorageError::kind) {
        None => 0,
        Some(StorageErrorKind::CorruptData) => 3,
        Some(_) => 1,
    };
    eprintln!(
        "riffdb-private-ordinary-retention-rotation-v1 stage=4 result={} kind={kind}",
        u8::from(result.is_err())
    );
    assert!(
        result.is_ok(),
        "ordinary_retained_validation_must_survive_location_only_rotation"
    );
}

// Use the existing complete source/catalog proof join before operational writes.
fn ready_ports(store: RedbStore) -> RedbOperationalPorts {
    use riffdb_storage_api::{
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StartupValidationInputs,
    };
    let key = ReadableDigestKey::v1(riffdb_types::DigestKeyId::new(1).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    crate::startup::validate_source_store_fixture(store, inputs)
}
