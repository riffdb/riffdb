//! Bounded exact lookups on one immutable published source; no writer or TLS trust.
use super::*;
use crate::{
    changelog_v3_cursor as cursor,
    error::{precommit_storage_error, table_error},
    journal::JournalTable,
    store::RedbReadAccess,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV2, ChangelogCursorErrorV3, ChangelogHistoryPointV3,
    PrimaryFenceSourceEvidenceV1, ReplicationSourceHoldKindV1, ReplicationSourceHoldStateV1,
    ReplicationSourceHoldV1,
    proto_codec::{
        decode_primary_fence_administration_v1, decode_replication_primary_admission_v1,
        decode_replication_source_hold,
    },
};

pub(crate) fn observe(
    access: &RedbReadAccess,
    request: PrimaryFenceRequestV1,
    applied: ChangelogHistoryPointV3,
) -> Result<Option<PrimaryFenceSourceEvidenceV1>, ChangelogCursorErrorV3> {
    let history = cursor::history(access)?;
    let lineage = history.lineage();
    if lineage.catalog_digest() != AuthoritativeStateCatalogV2.digest() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat).into());
    }
    drop(cursor::open(access, lineage, history.tail())?);
    let key = AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .ok_or_else(corrupt)?;
    let bytes = access
        .read_value(JournalTable::Meta, key.as_bytes())?
        .ok_or_else(corrupt)?;
    let admission = decode_replication_primary_admission_v1(&bytes).map_err(codec_error)?;
    let admission = admission.value();
    if admission.lineage() != lineage {
        return Err(corrupt().into());
    }
    let Some(fence) = admission.fence() else {
        return Ok(None);
    };
    let bytes = access
        .read_value(
            JournalTable::Audit,
            &crate::keys::encode_audit_key(fence.administration_sequence()),
        )?
        .ok_or_else(corrupt)?;
    let retained = decode_primary_fence_administration_v1(&bytes).map_err(codec_error)?;
    admission
        .validate_source_evidence(
            lineage,
            history.tail().frontier().application(),
            Some(retained.value()),
        )
        .map_err(|_| corrupt())?;
    let expected = receipt::reconstruct(fence)?;
    let fence_point = ChangelogHistoryPointV3::from_receipt(&expected).map_err(|_| corrupt())?;
    // Each open checks the complete canonical receipt at the selected position;
    // exact checksums bind every mutation. The immutable publication already
    // owns validated ancestry, including the bounded journal suffix.
    check_record_predecessor(access, history, fence.observed())?;
    drop(cursor::open(access, lineage, fence_point)?);
    if request.target() != fence.target()
        || request.operation_id() != fence.operation_id()
        || request.generation() != fence.generation()
    {
        return Err(ChangelogCursorErrorV3::InvalidPosition);
    }
    let root = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            std::sync::Arc::clone(root)
        }
        RedbReadAccess::Composite(view) => view.checkpoint_root_shared(),
    };
    // Source holds are checkpoint-only control state, never journal overlays.
    let holds = root
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .map_err(table_error)?;
    let hold_key = ReplicationSourceHoldV1::storage_key_for(
        request.target().hold_id(),
        ReplicationSourceHoldKindV1::FollowerAcknowledgement,
    );
    let row = holds
        .get(hold_key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let decoded = decode_replication_source_hold(row.value()).map_err(codec_error)?;
    let ReplicationSourceHoldStateV1::Registered(registration) = *decoded.value() else {
        return Err(corrupt().into());
    };
    if registration.hold().lineage() != lineage
        || registration.hold().storage_key() != hold_key
        || registration.generation() != fence.generation()
        || registration.phase() != FollowerRegistrationPhaseV1::Attached
    {
        return Err(corrupt().into());
    }
    check_record_predecessor(access, history, registration.registered_at())?;
    drop(cursor::open(access, lineage, registration.hold().fence())?);
    if !registration.hold().fence().precedes_or_equals(applied) {
        return Err(ChangelogCursorErrorV3::InvalidPosition);
    }
    drop(cursor::open(access, lineage, applied)?);
    let evidence = PrimaryFenceSourceEvidenceV1::new(fence.clone(), applied, history)
        .map_err(|_| ChangelogCursorErrorV3::InvalidPosition)?;
    Ok(Some(evidence))
}

fn check_record_predecessor(
    access: &RedbReadAccess,
    history: ChangelogHistoryStateV3,
    point: ChangelogHistoryPointV3,
) -> Result<(), ChangelogCursorErrorV3> {
    // As in registration-link startup validation, a pruned predecessor remains
    // bound by its retained administration record in this validated publication.
    // This exception never applies to the candidate, live hold or fence receipt.
    if point.sequence() < history.minimum_resume().sequence() {
        if !point.precedes_or_equals(history.minimum_resume()) {
            return Err(corrupt().into());
        }
    } else {
        drop(cursor::open(access, history.lineage(), point)?);
    }
    Ok(())
}
