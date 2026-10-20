//! ADR-0318 D7 attestation of original controls, after complete old validation.
//! The fixed result confers no current control, Source, drain or cut authority.
use super::*;
use riffdb_storage_api::{
    AuthoritativeStateCatalogV2, NativeLegacyControlsCommitmentV1,
    StoredAdministrationAuditRecordV1 as Audit,
};
use riffdb_types::AdministrationSequence;

pub(super) struct AttestedLegacyControlsV1 {
    pub(super) commitment: NativeLegacyControlsCommitmentV1,
    pub(super) control_count: u64,
    pub(super) key_value_bytes: u64,
    pub(super) visited_rows: u64,
}

/// The caller is the closed post-validation owner, not a raw read/lineage pair.
/// All graph-owning original validation results remain alive until this pass
/// completes; only the fixed result survives their joined-thread destruction.
pub(super) fn attest_after_complete_original_validation(
    session: &OfflinePredecessorEvidenceV1<'_>,
) -> Result<AttestedLegacyControlsV1, StorageError> {
    if !session.scan.structural_finished
        || !session.scan.historical_finished
        || session.scan.authoritative_finding_seen
        || session.scan.any_finding_seen
        || session.scan.saw_v1_index
        || session.history.lineage().catalog_digest() != AuthoritativeStateCatalogV2.digest()
        || session.scan.database_id != session.history.lineage().database_id()
    {
        return Err(corrupt());
    }
    check_cancellation(session)?;
    let read = session
        .scan
        .validation_read
        .as_ref()
        .ok_or_else(invariant)?;
    let history = session.history;
    let lineage = history.lineage();
    let head = history
        .tail()
        .frontier()
        .administration()
        .map_or(0, |s| s.get());
    let mut digest = Sha256::new();
    digest.update(b"riffdb/native-catalog-legacy-controls/v1\0");
    digest.update(lineage.database_id().as_bytes());
    digest.update(lineage.history_incarnation().to_be_bytes());
    digest.update(lineage.leadership_epoch().get().to_be_bytes());
    digest.update(head.to_be_bytes());
    digest.update(lineage.catalog_digest());
    let mut previous = None;
    let mut control_count = 0u64;
    let mut key_value_bytes = 0u64;
    let mut visited_rows = 0u64;
    let audit = read.open_table(AUDIT).map_err(table_error)?;
    for row in audit.iter().map_err(precommit_storage_error)? {
        check_cancellation(session)?;
        let (key, value) = row.map_err(precommit_storage_error)?;
        let key = key.value();
        let sequence = checked_key(key, head, &mut previous)?;
        visited_rows = visited_rows.checked_add(1).ok_or_else(limit_exceeded)?;
        // Reuse the sole original administration decoder. Command locators
        // and service audits were already fully validated by the original walk.
        let Some(record) = decode_non_command_administration_audit(value.value())? else {
            continue;
        };
        let record = record.value();
        if record.administration_sequence() != sequence {
            return Err(corrupt());
        }
        let original = match record {
            Audit::Replication(record) => record.after().hold().lineage(),
            Audit::PrimaryFence(record) => record.lineage(),
            Audit::Promotion(record) => {
                let selection = record.attempt().selection().ok_or_else(corrupt)?;
                let original = selection.evidence().source_history().lineage();
                let published = selection.published_lineage();
                if original.database_id() != published.database_id()
                    || original.catalog_digest() != published.catalog_digest()
                {
                    return Err(corrupt());
                }
                published
            }
            Audit::Catalog(_)
            | Audit::QueryModule(_)
            | Audit::ReactiveModule(_)
            | Audit::Capability(_)
            | Audit::Service(_)
            | Audit::Retention(_) => continue,
            Audit::NativeCatalogTransition(_) | Audit::SourceInheritance(_) => {
                return Err(corrupt());
            }
        };
        // Original validated old-incarnation facts remain historical bytes.
        // They must name this database and fixed V2 catalog, never foreign data.
        if original.database_id() != lineage.database_id()
            || original.catalog_digest() != AuthoritativeStateCatalogV2.digest()
        {
            return Err(corrupt());
        }
        let value_length = u64::try_from(value.value().len()).map_err(|_| limit_exceeded())?;
        control_count = control_count.checked_add(1).ok_or_else(limit_exceeded)?;
        key_value_bytes = key_value_bytes
            .checked_add(9)
            .and_then(|n| n.checked_add(value_length))
            .ok_or_else(limit_exceeded)?;
        digest.update(key);
        digest.update(value_length.to_be_bytes());
        digest.update(value.value());
    }
    check_cancellation(session)?;
    digest.update(control_count.to_be_bytes());
    digest.update(key_value_bytes.to_be_bytes());
    Ok(AttestedLegacyControlsV1 {
        commitment: NativeLegacyControlsCommitmentV1::from_bytes(digest.finalize().into()),
        control_count,
        key_value_bytes,
        visited_rows,
    })
}

fn check_cancellation(session: &OfflinePredecessorEvidenceV1<'_>) -> Result<(), StorageError> {
    if session
        .scan
        .cancellation
        .as_ref()
        .is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Acquire))
    {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

fn checked_key(
    key: &[u8],
    head: u64,
    previous: &mut Option<[u8; 9]>,
) -> Result<AdministrationSequence, StorageError> {
    let fixed: [u8; 9] = key.try_into().map_err(|_| corrupt())?;
    let sequence = keys::decode_audit_key(key).map_err(|_| corrupt())?;
    if key != keys::encode_audit_key(sequence)
        || sequence.get() > head
        || previous.as_ref().is_some_and(|prior| prior >= &fixed)
    {
        return Err(corrupt());
    }
    *previous = Some(fixed);
    Ok(sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REC-001, REC-005, REP-005
    #[test]
    fn legacy_control_framing_refuses_malformed_duplicate_reordered_and_after_head_keys() {
        let first = AdministrationSequence::first();
        let second = first.checked_next().unwrap();
        let first_key = keys::encode_audit_key(first);
        let second_key = keys::encode_audit_key(second);
        let mut previous = None;
        assert_eq!(
            checked_key(&first_key, second.get(), &mut previous).unwrap(),
            first
        );
        for key in [&first_key[..8], &[0u8; 9][..], &first_key[..]] {
            assert!(
                matches!(checked_key(key, second.get(), &mut previous), Err(e) if e.kind() == StorageErrorKind::CorruptData)
            );
        }
        assert_eq!(
            checked_key(&second_key, second.get(), &mut previous).unwrap(),
            second
        );
        assert!(
            matches!(checked_key(&first_key, second.get(), &mut previous), Err(e) if e.kind() == StorageErrorKind::CorruptData)
        );
        assert!(
            matches!(checked_key(&second_key, first.get(), &mut None), Err(e) if e.kind() == StorageErrorKind::CorruptData)
        );
        assert!(
            matches!(checked_key(&first_key, 0, &mut None), Err(e) if e.kind() == StorageErrorKind::CorruptData)
        );
    }
}
