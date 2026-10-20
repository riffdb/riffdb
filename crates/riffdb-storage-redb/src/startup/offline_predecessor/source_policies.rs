//! A source-policy-only drain observation on the completely validated old pin.
//! Bootstrap/archive/captured-work inventory remains a separate required proof;
//! this fixed observation cannot authorize Accepted, retirement or a cut.
use super::*;
use riffdb_storage_api::{
    FollowerRegistrationPhaseV1, MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldStateV1,
    proto_codec::decode_replication_source_hold,
};

pub(super) struct SourcePolicyDrainObservationV1 {
    pub(super) retired_policy_count: u64,
    pub(super) retired_policy_native_bytes: u64,
}

pub(super) fn observe_after_complete_original_validation(
    session: &OfflinePredecessorEvidenceV1<'_>,
) -> Result<SourcePolicyDrainObservationV1, StorageError> {
    if !session.scan.structural_finished
        || !session.scan.historical_finished
        || session.scan.authoritative_finding_seen
        || session.scan.any_finding_seen
    {
        return Err(corrupt());
    }
    let read = session
        .scan
        .validation_read
        .as_ref()
        .ok_or_else(invariant)?;
    // This is the same actual old-session read, not a caller-selected read or
    // a current-mode/ID shortcut. Full old validation already proved all roots.
    if crate::follower_lifecycle::is_attached(read)? {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    let holds = read
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .map_err(table_error)?;
    if holds.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
        return Err(limit_exceeded());
    }
    let mut retired_policy_count = 0u64;
    let mut retired_policy_native_bytes = 0u64;
    for row in holds.iter().map_err(precommit_storage_error)? {
        if session
            .scan
            .cancellation
            .as_ref()
            .is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded =
            decode_replication_source_hold(value.value()).map_err(crate::error::codec_error)?;
        let ReplicationSourceHoldStateV1::Registered(policy) = *decoded.value() else {
            return Err(storage_error(StorageErrorKind::Unavailable));
        };
        if key.value() != policy.hold().storage_key().as_slice()
            || policy.hold().lineage() != session.history.lineage()
        {
            return Err(corrupt());
        }
        if policy.phase() != FollowerRegistrationPhaseV1::Retired {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        // The preceding complete original retained-history validation reused
        // RegistrationLinksValidation. Its finish already proved this exact
        // Registered policy's original registration/release reciprocity. Do not
        // create a second release validator or infer release from this phase.
        retired_policy_count = retired_policy_count
            .checked_add(1)
            .ok_or_else(limit_exceeded)?;
        let physical = key
            .value()
            .len()
            .checked_add(value.value().len())
            .ok_or_else(limit_exceeded)?;
        retired_policy_native_bytes = retired_policy_native_bytes
            .checked_add(u64::try_from(physical).map_err(|_| limit_exceeded())?)
            .ok_or_else(limit_exceeded)?;
    }
    if session
        .scan
        .cancellation
        .as_ref()
        .is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Acquire))
    {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(SourcePolicyDrainObservationV1 {
        retired_policy_count,
        retired_policy_native_bytes,
    })
}
