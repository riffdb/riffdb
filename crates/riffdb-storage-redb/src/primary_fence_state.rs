//! Complete V2 source evidence under the closed physical fence transaction.
//! This reader grants neither a general writer nor production startup readiness.
use super::*;
use crate::{
    changelog_v3_activation::{HISTORY, SOURCE_HOLDS},
    changelog_v3_write::table_inventory,
    error::{precommit_storage_error, table_error},
    layout::{AUDIT, META},
};
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::PrimaryFenceRefusalV1 as Refusal;
use riffdb_storage_api::{
    AuthoritativeNamespaceV2, DurableCodecErrorKind, ReplicationSourceHoldKindV1,
    ReplicationSourceHoldStateV1, ReplicationSourceHoldV1, proto_codec::*,
};
use std::ops::ControlFlow;

pub(super) struct SourceState {
    pub history: ChangelogHistoryStateV3,
    pub admission: ReplicationPrimaryAdmissionV1,
    pub registration: ReplicationSourceHoldV2,
    pub allocator: AdministrationSequenceAllocator,
}

impl SourceState {
    pub(super) fn read(
        transaction: &WriteTransaction,
        request: PrimaryFenceRequestV1,
    ) -> Result<ControlFlow<Refusal, Self>, StorageError> {
        let tables = table_inventory(transaction)?;
        for namespace in [
            N::Audit,
            N::ChangelogHistory,
            N::ReplicationSourceHolds,
            N::NextAdministrationSequence,
        ] {
            if !tables.contains(namespace.table()) {
                return Err(corrupt());
            }
        }
        let meta = transaction.open_table(META).map_err(table_error)?;
        let history = *decode_changelog_history_state_v3(
            meta.get(key(N::ChangelogHistoryState)?)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?
                .value(),
        )
        .map_err(codec_error)?
        .value();
        let admission_key = AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
            .metadata_key()
            .ok_or_else(corrupt)?;
        let admission = decode_replication_primary_admission_v1(
            meta.get(admission_key)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?
                .value(),
        )
        .map_err(codec_error)?
        .value()
        .clone();
        let allocator = history.tail().frontier().administration().map_or(
            AdministrationSequenceAllocator::initial(),
            |head| {
                head.checked_next().map_or(
                    AdministrationSequenceAllocator::Exhausted,
                    AdministrationSequenceAllocator::next,
                )
            },
        );
        let lineage = history.lineage();
        let target = request.target();
        if lineage.catalog_digest() != AuthoritativeStateCatalogV2.digest()
            || admission.lineage() != lineage
        {
            return Err(corrupt());
        }
        // Compare canonical bounded envelopes directly, without copying unchecked
        // metadata. Includes registry, catalog, both allocators and detached mode.
        for (key, expected) in transaction::source_metadata(history, allocator, &admission)? {
            if meta
                .get(key)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?
                .value()
                != expected.as_bytes()
            {
                return Err(corrupt());
            }
        }
        let holds = transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let receipts = transaction.open_table(HISTORY).map_err(table_error)?;
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        crate::changelog_v3_roots::validate_retained_rows(history, &receipts, &holds)?;
        crate::replication_registration_links::validate(&holds, &audit, history, &receipts)?;
        validate_fences(history, &admission, &audit, &receipts)?;
        // Only a completely validated source can return a request refusal.
        // Caller-selected lineage must never turn healthy stored evidence into
        // corruption or conceal contradictory retained authority.
        if target.database_id() != lineage.database_id()
            || target.history_incarnation() != lineage.history_incarnation()
            || target.leadership_epoch() != lineage.leadership_epoch()
        {
            return Ok(ControlFlow::Break(Refusal::LineageMismatch));
        }
        let hold_key = ReplicationSourceHoldV1::storage_key_for(
            target.hold_id(),
            ReplicationSourceHoldKindV1::FollowerAcknowledgement,
        );
        let Some(row) = holds
            .get(hold_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(ControlFlow::Break(Refusal::RegistrationMissingOrStale));
        };
        let ReplicationSourceHoldStateV1::Registered(registration) =
            *decode_replication_source_hold(row.value())
                .map_err(codec_error)?
                .value()
        else {
            return Ok(ControlFlow::Break(Refusal::RegistrationMissingOrStale));
        };
        if registration.generation() != request.generation()
            || registration.phase() == FollowerRegistrationPhaseV1::Retired
        {
            return Ok(ControlFlow::Break(Refusal::RegistrationMissingOrStale));
        }
        Ok(ControlFlow::Continue(Self {
            history,
            admission,
            registration,
            allocator,
        }))
    }
}

pub(crate) fn validate_fences(
    history: ChangelogHistoryStateV3,
    admission: &ReplicationPrimaryAdmissionV1,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let mut retained = None;
    // Stream to exact end: no early success can conceal a contradictory fence.
    for row in audit.iter().map_err(precommit_storage_error)? {
        let (key, bytes) = row.map_err(precommit_storage_error)?;
        let decoded = match decode_primary_fence_administration_v1(bytes.value()) {
            Ok(decoded) => decoded,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => continue,
            Err(error) => return Err(codec_error(error)),
        };
        let record = decoded.value();
        let lineage = history.lineage();
        if key.value() != crate::keys::encode_audit_key(record.administration_sequence())
            || record.lineage().database_id() != lineage.database_id()
            || Some(record.administration_sequence()) > history.tail().frontier().administration()
        {
            return Err(corrupt());
        }
        if record.lineage() != lineage {
            if record.lineage().history_incarnation() >= lineage.history_incarnation() {
                return Err(corrupt());
            }
            continue;
        }
        if retained.is_some() {
            return Err(corrupt());
        }
        validate_fence_receipt(history, record, receipts)?;
        retained = Some(record.clone());
    }
    admission
        .validate_source_evidence(
            history.lineage(),
            history.tail().frontier().application(),
            retained.as_ref(),
        )
        .map_err(|_| corrupt())
}

pub(crate) fn validate_fence_receipt(
    history: ChangelogHistoryStateV3,
    record: &StoredPrimaryFenceAdministrationV1,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    if record.lineage() != history.lineage()
        || Some(record.administration_sequence()) > history.tail().frontier().administration()
        || !record.observed().precedes_or_equals(history.tail())
    {
        return Err(corrupt());
    }
    crate::replication_registration_links::validate_point(record.observed(), history, receipts)?;
    let expected = receipt::reconstruct(record)?;
    let row = receipts
        .get(expected.binding().sequence.get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    // Check every mutation and beforeimage, not merely the inserted audit row.
    if row.value() != expected.encode().map_err(|_| corrupt())?.as_slice() {
        return Err(corrupt());
    }
    Ok(())
}

fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace.metadata_key().ok_or_else(corrupt)
}
