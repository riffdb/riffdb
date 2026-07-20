#![forbid(unsafe_code)]

//! Durable bootstrap and service-audit reopen evidence for redb.

use std::num::{NonZeroU16, NonZeroU32};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
    DatabaseInitializationPort, DatabaseInitializationResult, EvidencePageLimit,
    HistoricalEvidenceCursor, HistoricalEvidencePage, PartitionScopeV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    StartupValidationInputs, StorageScanLimit, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStore};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityId, CapabilityTokenDigest, DatabaseId, DigestKeyId,
    Environment, RequestId, ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    TenantScope, Timestamp,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestPath(PathBuf);

impl TestPath {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "riffdb-service-audit-recovery-{}-{}.redb",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        )))
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(1)).expect("database ID")
}

fn capability_id() -> CapabilityId {
    CapabilityId::from_bytes(uuid_bytes(2)).expect("capability ID")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
}

fn digest() -> CapabilityTokenDigest {
    CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key ID"), [0x41; 32])
}

fn requested_record() -> CapabilityRequestedRecordV1 {
    let permissions = CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::unparameterized(
        CapabilityPermissionKindV1::AdministerCapabilities,
    )
    .expect("permission")])
    .expect("permissions");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("grant");
    CapabilityRequestedRecordV1::new(
        database_id(),
        Environment::new("test").expect("environment"),
        ActorId::new("bootstrap-operator").expect("actor"),
        ActorKind::Human,
        NonZeroU32::new(300).expect("duration"),
        vec![Audience::new("riffdb-test").expect("audience")],
        grant,
    )
    .expect("requested capability")
}

fn bootstrap_intent(request_seed: u8, seconds: i64) -> CapabilityBootstrapIntentV1 {
    let issued_at = Timestamp::new(seconds, 0).expect("issued timestamp");
    let start = BootstrapServiceAuditStartV1::new(
        request_id(request_seed),
        issued_at,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id())])
            .expect("audit targets"),
        None,
    )
    .expect("bootstrap audit start");
    CapabilityBootstrapIntentV1::new(
        capability_id(),
        requested_record(),
        BootstrapDigestCandidatesV1::new(vec![digest()], digest()).expect("digest candidates"),
        issued_at,
        Timestamp::new(seconds + 300, 0).expect("expiry"),
        start,
    )
    .expect("bootstrap intent")
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        Timestamp::new(100, 0).expect("authorization timestamp"),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency inventory"),
    )
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .expect("begin structural session");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural, limit)
            .expect("structural evidence")
        {
            StructuralEvidencePage::Page { next, .. } => structural = next,
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let mut historical = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        match session
            .read_historical_evidence(historical, limit)
            .expect("historical evidence")
        {
            HistoricalEvidencePage::Page { next, .. } => historical = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural session");
    let (_, _, dormant) = opened.into_parts();
    dormant.into_operational_after_catalog_validation()
}

fn audit_count(ports: &RedbOperationalPorts) -> usize {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("scan limit"),
        ))
        .expect("scan audit");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("small audit stream reaches exact end");
    };
    assert!(records
        .iter()
        .all(|record| record.encoded_content_charge().get() > 0));
    records.len()
}

#[test]
fn bootstrap_and_linked_service_audit_survive_reopen_and_replay() {
    let path = TestPath::new();
    let mut store = RedbStore::open(&path.0).expect("open database");
    assert_eq!(
        store
            .initialize_database(database_id())
            .expect("initialize database"),
        DatabaseInitializationResult::Installed(database_id())
    );
    let mut ports = open_operational(store);

    let first = bootstrap_intent(10, 10);
    let created = ports
        .bootstrap_capability(&first)
        .expect("bootstrap capability");
    let CapabilityBootstrapResult::BootstrapCreated {
        administration_sequence,
        ..
    } = created
    else {
        panic!("first bootstrap creates the root capability");
    };
    let terminal = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
        first.start(),
        Timestamp::new(11, 0).expect("terminal timestamp"),
        administration_sequence,
    )
    .expect("terminal audit intent");
    assert!(matches!(
        ports
            .append_service_audit(&terminal)
            .expect("append terminal audit"),
        ServiceAuditAppendResult::Appended(_)
    ));
    assert_eq!(audit_count(&ports), 3);
    drop(ports);

    let mut ports = open_operational(RedbStore::open(&path.0).expect("reopen database"));
    assert_eq!(audit_count(&ports), 3);
    assert!(matches!(
        ports
            .bootstrap_capability(&bootstrap_intent(11, 20))
            .expect("replay bootstrap"),
        CapabilityBootstrapResult::BootstrapReplayed { .. }
    ));
    assert_eq!(audit_count(&ports), 4);
}
