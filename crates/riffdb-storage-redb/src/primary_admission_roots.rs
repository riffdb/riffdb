//! Catalog-aware source admission evidence on the caller's immutable root.
//! Consistency is not an application writer or promotion permit.

use redb::{ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    AuthoritativeNamespaceV2, AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2,
    ChangelogHistoryStateV3, DurableCodecErrorKind, ReplicationPrimaryAdmissionV1, StorageError,
    StorageErrorKind, proto_codec::*,
};

use crate::error::{codec_error, precommit_storage_error, storage_error};

impl riffdb_storage_api::ReplicationPrimaryAdmissionReadPort for crate::RedbOperationalPorts {
    fn read_replication_primary_admission(
        &self,
    ) -> Result<ReplicationPrimaryAdmissionV1, StorageError> {
        if self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(crate::error::transaction_error)?;
        read_source_admission(&transaction)
    }
}

pub(crate) fn read_source_admission(
    transaction: &redb::ReadTransaction,
) -> Result<ReplicationPrimaryAdmissionV1, StorageError> {
    let history = crate::changelog_v3_roots::read_checkpoint_roots(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let meta = transaction
        .open_table(crate::layout::META)
        .map_err(crate::error::table_error)?;
    let admission = read(&meta)?.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if admission.lineage() != history.lineage() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(admission)
}

pub(crate) fn key() -> Result<&'static str, StorageError> {
    AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

pub(crate) fn catalog_digest(bytes: &[u8]) -> Result<[u8; 32], StorageError> {
    match decode_authoritative_state_catalog_v1(bytes) {
        Ok(catalog) => Ok(catalog.value().digest()),
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
            Ok(decode_authoritative_state_catalog_v2(bytes)
                .map_err(codec_error)?
                .value()
                .digest())
        }
        Err(error) => Err(codec_error(error)),
    }
}

pub(crate) fn read(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<Option<ReplicationPrimaryAdmissionV1>, StorageError> {
    meta.get(key()?)
        .map_err(precommit_storage_error)?
        .map(|row| {
            // The codec checks its closed envelope bound before allocating.
            decode_replication_primary_admission_v1(row.value())
                .map(|decoded| decoded.value().clone())
                .map_err(codec_error)
        })
        .transpose()
}

/// Additional check at the writer-bearing operational handoff, after complete
/// validation. This neither treats absent state as Active nor validates a root.
pub(crate) fn require_unfenced(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), StorageError> {
    if read(meta)?.is_some_and(|admission| admission.fence().is_some()) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

/// Defense at the physical receipt owner, independent of coordinator admission.
/// The operation owner still validates the audit/control semantics. Fenced state
/// admits only audit mutations and non-mutating lifecycle/drain receipts here.
pub(crate) fn validate_write_receipt(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history: ChangelogHistoryStateV3,
    receipt: &riffdb_storage_api::AuthoritativeTransactionV3,
) -> Result<(), StorageError> {
    use riffdb_storage_api::AuthoritativeNamespaceV1 as N;
    validate_write_receipt_fields(
        meta,
        history,
        receipt.binding(),
        receipt.attribution(),
        receipt.mutations().is_empty(),
        receipt.mutations().iter().all(|mutation| {
            matches!(
                mutation.namespace(),
                N::Audit
                    | N::AuditByRequest
                    | N::AuditByRequestLocators
                    | N::NextAdministrationSequence
            )
        }),
    )
}

pub(crate) fn validate_write_receipt_summary(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history: ChangelogHistoryStateV3,
    summary: &crate::changelog_v3::ValidatedReceiptSummaryV3,
) -> Result<(), StorageError> {
    validate_write_receipt_fields(
        meta,
        history,
        summary.binding(),
        summary.attribution(),
        summary.is_empty(),
        summary.all_audit_fenced_namespaces(),
    )
}

fn validate_write_receipt_fields(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history: ChangelogHistoryStateV3,
    binding: riffdb_storage_api::AuthoritativeTransactionBindingV3,
    attribution: riffdb_storage_api::ChangelogAttributionV3,
    is_empty: bool,
    all_audit_fenced_namespaces: bool,
) -> Result<(), StorageError> {
    use riffdb_storage_api::ChangelogAttributionV3 as A;
    if history.lineage().catalog_digest() != AuthoritativeStateCatalogV2.digest() {
        return Ok(());
    }
    let admission = read(meta)?.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if admission.lineage() != history.lineage() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let Some(fence) = admission.fence() else {
        return Ok(());
    };
    let allowed = match attribution {
        A::JournaledServiceAudit | A::DirectApplicationOrServiceAuditGroup => {
            all_audit_fenced_namespaces
        }
        A::CleanClose | A::DirtyActivation | A::ReplicationSourceHold => is_empty,
        _ => false,
    };
    if !allowed
        || binding.predecessor_frontier.application() != fence.final_application_head()
        || binding.covered_frontier.application() != fence.final_application_head()
    {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

/// Bounded source/follower binding and exact point lookups. Complete startup
/// additionally scans all fence records to detect omitted or duplicate evidence.
pub(crate) fn validate_with_admission(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history: ChangelogHistoryStateV3,
    attached: bool,
    audit: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
    budget: Option<&riffdb_types::exact_provider_budget_v1::ProviderBudget>,
) -> Result<(), StorageError> {
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    let admission = read(meta)?;
    if history.lineage().catalog_digest() == AuthoritativeStateCatalogV1.digest() || attached {
        return if admission.is_none() {
            Ok(())
        } else {
            Err(corrupt())
        };
    }
    if history.lineage().catalog_digest() != AuthoritativeStateCatalogV2.digest() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let admission = admission.ok_or_else(corrupt)?;
    let audit = audit.ok_or_else(corrupt)?;
    let retained = if let Some(fence) = admission.fence() {
        let row = audit
            .get(crate::keys::encode_audit_key(fence.administration_sequence()).as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let retained = decode_primary_fence_administration_v1(row.value()).map_err(codec_error)?;
        crate::primary_fence_write::state::validate_fence_receipt_with_admission(
            history,
            retained.value(),
            receipts,
            budget,
        )?;
        Some(retained.value().clone())
    } else {
        None
    };
    admission
        .validate_source_evidence(
            history.lineage(),
            history.tail().frontier().application(),
            retained.as_ref(),
        )
        .map_err(|_| corrupt())
}

pub(crate) fn validate_retained(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history: ChangelogHistoryStateV3,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    if history.lineage().catalog_digest() == AuthoritativeStateCatalogV2.digest() {
        let admission = read(meta)?.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        crate::primary_fence_write::state::validate_fences(history, &admission, audit, receipts)?;
    }
    Ok(())
}
