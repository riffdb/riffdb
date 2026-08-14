#![forbid(unsafe_code)]

//! Read-only stopped-database evidence for the alpha endurance orchestrator.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use riffdb_catalog::{CatalogHistoryOutcome, validate_catalog_history};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    EvidencePageLimit, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs, StorageScanLimit,
    StoredAdministrationAuditRecordV1, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
use riffdb_types::{DigestKeyId, ServiceAuditPhaseV1, Timestamp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let database = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("database path is required")?;
    if arguments.next().is_some() || !database.is_absolute() || database.is_symlink() {
        return Err("usage: riffdb-endurance-inspect ABSOLUTE_DATABASE_PATH".into());
    }
    let checkpoint =
        riffdb_storage_redb::read_validated_prefix_checkpoint_commit_sequence_fixture(&database)?;
    let audit = audit_summary(&database)?;
    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.alpha-endurance-storage-inspection/v1",
            "checkpoint_commit_sequence": checkpoint,
            "service_audit_records": audit.records,
            "incomplete_service_audit_lifecycles": audit.incomplete,
            "incomplete_service_operation_tags": audit.incomplete_operation_tags,
            "last_service_operation_tag": audit.last_operation_tag,
            "last_service_phase_tag": audit.last_phase_tag,
        })
    );
    Ok(())
}

struct AuditSummary {
    records: u64,
    incomplete: u64,
    incomplete_operation_tags: BTreeMap<u8, u64>,
    last_operation_tag: Option<u8>,
    last_phase_tag: Option<u8>,
}

fn audit_summary(database: &Path) -> Result<AuditSummary, Box<dyn std::error::Error>> {
    let ports = open_operational_offline(database)?;
    let limit = StorageScanLimit::new(64).ok_or("audit scan limit is invalid")?;
    let mut after = None;
    let mut records = 0_u64;
    let mut open = BTreeMap::new();
    let mut last_operation_tag = None;
    let mut last_phase_tag = None;
    loop {
        let (page, next) = match ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(after, limit))?
        {
            AdministrationAuditScan::Page {
                records,
                next_after,
            } => (records, Some(next_after)),
            AdministrationAuditScan::ExactEnd { records } => (records, None),
        };
        for item in page {
            let StoredAdministrationAuditRecordV1::Service(record) = item.into_parts().0 else {
                continue;
            };
            records = records.saturating_add(1);
            last_operation_tag = Some(record.operation().tag());
            last_phase_tag = Some(record.phase().tag());
            if record.phase() == ServiceAuditPhaseV1::Started {
                open.insert(record.request_id(), record.operation());
            } else {
                open.remove(&record.request_id());
            }
        }
        let Some(next) = next else {
            break;
        };
        after = Some(next);
    }
    let mut incomplete_operation_tags = BTreeMap::new();
    for operation in open.into_values() {
        let count = incomplete_operation_tags
            .entry(operation.tag())
            .or_insert(0_u64);
        *count = count.saturating_add(1);
    }
    Ok(AuditSummary {
        records,
        incomplete: incomplete_operation_tags.values().copied().sum(),
        incomplete_operation_tags,
        last_operation_tag,
        last_phase_tag,
    })
}

fn open_operational_offline(
    database: &Path,
) -> Result<RedbOperationalPorts, Box<dyn std::error::Error>> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let observed_at = Timestamp::new(i64::try_from(elapsed.as_secs())?, elapsed.subsec_nanos())?;
    let digest_key = DigestKeyId::new(1).ok_or("digest key ID is invalid")?;
    let inputs = StartupValidationInputs::new(
        observed_at,
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])?,
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])?,
    );
    let store = RedbStore::open(database)?;
    let mut session = store.begin_structural_evidence(inputs)?;
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).ok_or("evidence page limit is invalid")?;
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session.read_structural_evidence(cursor, limit)? {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err("stopped endurance database has structural findings".into());
                }
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)?.into_parts();
    if !matches!(history, CatalogHistoryOutcome::Ready(_)) {
        return Err("stopped endurance database requires catalog migration".into());
    }
    let opened = session.finish(structural_end, historical_end)?;
    let StructuralOpenOutcome::Clean(opened) = opened else {
        return Err("stopped endurance database requires index migration".into());
    };
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    Ok(dormant.into_operational_after_catalog_validation()?)
}
