//! Exact physical promotion evidence. This module grants no runtime authority.
use crate::error::{codec_error, precommit_storage_error, storage_error};
use redb::ReadableTable;
use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as N,
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogTransactionSequence,
    DurableCodecErrorKind, StorageError, StorageErrorKind,
    StoredPromotionAdministrationV1 as Record, proto_codec::*,
};

pub(crate) fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

/// Until the runtime supplies exact external reconciliation, a current-lineage
/// promotion is never an ordinary source-open path. This check precedes journal
/// recovery and writer construction, including when all ledger files were lost.
pub(crate) fn require_reconciled_source_open(
    transaction: &redb::ReadTransaction,
) -> Result<(), StorageError> {
    if current_record(transaction)?.is_some() {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

/// Bounded original-control discovery only. The returned decoded record does
/// not prove the external attempt, current source validity or readiness.
pub(crate) fn current_record(
    transaction: &redb::ReadTransaction,
) -> Result<Option<Record>, StorageError> {
    use crate::{
        error::table_error,
        layout::{AUDIT, META},
    };
    let meta = match transaction.open_table(META) {
        Ok(meta) => meta,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    let Some(row) = meta
        .get(
            N::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(corrupt)?,
        )
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let current = *decode_changelog_history_state_v3(row.value())
        .map_err(codec_error)?
        .value();
    if current.lineage().catalog_digest()
        != riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
    {
        return Ok(None);
    }
    // A promotion's anchor covers exactly start/control/success, so its control
    // key is determined by the retained anchor even after history reclamation.
    // This is one bounded lookup, preserving the existing V1 CLEAN open path.
    let Some(sequence) = current
        .anchor()
        .frontier()
        .administration()
        .filter(|last| last.get() >= 3)
        .and_then(|last| riffdb_types::AdministrationSequence::new(last.get() - 1))
    else {
        return Ok(None);
    };
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let Some(value) = audit
        .get(crate::keys::encode_audit_key(sequence).as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    match decode_promotion_administration_v1(value.value()) {
        Ok(record) if history(record.value())?.lineage() == current.lineage() => {
            return Ok(Some(record.value().clone()));
        }
        Ok(_) => {}
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }
    Ok(None)
}

/// Reconstructs every new administration row and secondary index. The lineage
/// stamp and watermark rebind establish the anchor's initial snapshot through
/// the existing offline stamp path; they are not predecessor-lineage replay.
pub(crate) fn receipt(record: &Record) -> Result<AuthoritativeTransactionV3, StorageError> {
    let selection = record.attempt().selection().ok_or_else(corrupt)?;
    let lineage = selection.published_lineage();
    let mut mutations = Vec::with_capacity(6);
    let control = encode_promotion_administration_v1(record).map_err(codec_error)?;
    mutations.push(
        Mutation::put(
            N::Audit,
            &crate::keys::encode_audit_key(record.administration_sequence()),
            None,
            control.as_bytes(),
        )
        .map_err(|_| corrupt())?,
    );
    for audit in record.service_audits().map_err(|_| corrupt())? {
        let encoded = encode_service_audit_record_v3(&audit).map_err(codec_error)?;
        mutations.push(
            Mutation::put(
                N::Audit,
                &crate::keys::encode_audit_key(audit.administration_sequence()),
                None,
                encoded.as_bytes(),
            )
            .map_err(|_| corrupt())?,
        );
        let index = encode_service_audit_request_index_v1(StoredServiceAuditRequestIndexV1::new(
            audit.request_id(),
            audit.administration_sequence(),
        ))
        .map_err(codec_error)?;
        mutations.push(
            Mutation::put(
                N::AuditByRequest,
                &crate::keys::encode_audit_by_request_key(
                    audit.request_id(),
                    audit.administration_sequence(),
                ),
                None,
                index.as_bytes(),
            )
            .map_err(|_| corrupt())?,
        );
    }
    let before = encode_administration_sequence_allocator_v1(
        riffdb_storage_api::AdministrationSequenceAllocator::next(record.started_sequence()),
    )
    .map_err(codec_error)?;
    let after = encode_administration_sequence_allocator_v1(record.next_administration())
        .map_err(codec_error)?;
    mutations.push(
        Mutation::replace(
            N::NextAdministrationSequence,
            crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
            before.as_bytes(),
            after.as_bytes(),
        )
        .map_err(|_| corrupt())?,
    );
    mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: None,
            sequence: ChangelogTransactionSequence::new(1).ok_or_else(corrupt)?,
            predecessor_frontier: selection.applied().frontier(),
            covered_frontier: record.covered_frontier(),
            prior_history_hash: [0; 32],
        },
        ChangelogAttributionV3::Promotion,
        mutations,
        lineage.catalog_digest(),
    )
    .map_err(|_| corrupt())
}

pub(crate) fn history(record: &Record) -> Result<ChangelogHistoryStateV3, StorageError> {
    let point = ChangelogHistoryPointV3::from_receipt(&receipt(record)?).map_err(|_| corrupt())?;
    ChangelogHistoryStateV3::new(
        record
            .attempt()
            .selection()
            .ok_or_else(corrupt)?
            .published_lineage(),
        point,
        point,
        point,
    )
    .map_err(|_| corrupt())
}

/// Complete audit reciprocity is retained even when the physical anchor is
/// below the minimum resume point. The original anchor hash remains in roots.
pub(crate) fn validate_record(
    record: &Record,
    retained: ChangelogHistoryStateV3,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    indexes: &impl ReadableTable<&'static [u8], &'static [u8]>,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let expected = history(record)?;
    if expected.lineage().database_id() != retained.lineage().database_id()
        || expected.lineage().history_incarnation() > retained.lineage().history_incarnation()
        || Some(record.succeeded_sequence()) > retained.tail().frontier().administration()
    {
        return Err(corrupt());
    }
    for row in record.service_audits().map_err(|_| corrupt())? {
        let encoded = encode_service_audit_record_v3(&row).map_err(codec_error)?;
        if audit
            .get(crate::keys::encode_audit_key(row.administration_sequence()).as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?
            .value()
            != encoded.as_bytes()
        {
            return Err(corrupt());
        }
        let index = encode_service_audit_request_index_v1(StoredServiceAuditRequestIndexV1::new(
            row.request_id(),
            row.administration_sequence(),
        ))
        .map_err(codec_error)?;
        if indexes
            .get(
                crate::keys::encode_audit_by_request_key(
                    row.request_id(),
                    row.administration_sequence(),
                )
                .as_slice(),
            )
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?
            .value()
            != index.as_bytes()
        {
            return Err(corrupt());
        }
    }
    if expected.lineage().history_incarnation() == retained.lineage().history_incarnation() {
        if expected.lineage() != retained.lineage() || expected.anchor() != retained.anchor() {
            return Err(corrupt());
        }
        if retained.minimum_resume() == retained.anchor()
            && !receipts.is_empty().map_err(precommit_storage_error)?
        {
            let encoded = receipt(record)?.encode().map_err(|_| corrupt())?;
            if receipts
                .get(1u64.to_be_bytes().as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?
                .value()
                != encoded
            {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_retained(
    retained: ChangelogHistoryStateV3,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    indexes: &impl ReadableTable<&'static [u8], &'static [u8]>,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let mut current = false;
    for row in audit.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = match decode_promotion_administration_v1(value.value()) {
            Ok(decoded) => decoded,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => continue,
            Err(error) => return Err(codec_error(error)),
        };
        let record = decoded.value();
        if key.value() != crate::keys::encode_audit_key(record.administration_sequence()) {
            return Err(corrupt());
        }
        validate_record(record, retained, audit, indexes, receipts)?;
        if history(record)?.lineage() == retained.lineage() {
            if current {
                return Err(corrupt());
            }
            current = true;
        }
    }
    // An orphan Promotion anchor is corruption, including erasure of its control row.
    if retained.minimum_resume() == retained.anchor()
        && let Some(row) = receipts
            .get(retained.anchor().sequence().get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
        && AuthoritativeTransactionV3::decode(row.value())
            .map_err(|_| corrupt())?
            .attribution()
            == ChangelogAttributionV3::Promotion
        && !current
    {
        return Err(corrupt());
    }
    Ok(())
}
