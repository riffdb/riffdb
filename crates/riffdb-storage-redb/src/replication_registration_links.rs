//! Same-pin registration/retirement evidence for the bounded source hold set.

use std::collections::BTreeMap;

use redb::ReadableTable;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History, DurableCodecErrorKind,
    FollowerRegistrationPhaseV1 as Phase, MAX_REPLICATION_SOURCE_HOLDS_V1,
    ReplicationAdministrationActionV1 as Action, ReplicationSourceHoldStateV1 as State,
    ReplicationSourceHoldV2 as Policy, StorageError, StorageErrorKind,
    StoredReplicationAdministrationV1 as Record,
    proto_codec::{decode_replication_administration_v1, decode_replication_source_hold},
};

use crate::error::{codec_error, precommit_storage_error, storage_error};

struct Evidence {
    current: Policy,
    registered: bool,
    released: bool,
}

/// Complete source-only cross-link pass, after retained-chain validation. This
/// streams audit history to its exact end, retaining at most the existing 4096
/// holds and two flags per registration. It is not a checkpoint shortcut, does
/// not run on follower images, and grants no authorization or release capability.
pub(crate) fn validate(
    holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    history: History,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    if holds.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let mut evidence = BTreeMap::new();
    for row in holds.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = decode_replication_source_hold(value.value()).map_err(codec_error)?;
        let (hold, registered) = match *decoded.value() {
            State::Legacy(hold) => (hold, None),
            State::Registered(policy) => (policy.hold(), Some(policy)),
        };
        if key.value() != hold.storage_key() || hold.lineage() != history.lineage() {
            return Err(corrupt());
        }
        if let Some(current) = registered {
            if !current.registered_at().precedes_or_equals(history.tail())
                || current.generation() > history.tail().sequence()
                || !hold.fence().precedes_or_equals(history.tail())
                || current
                    .degraded_at()
                    .is_some_and(|p| !p.precedes_or_equals(history.tail()))
            {
                return Err(corrupt());
            }
            evidence.insert(
                hold.storage_key(),
                Evidence {
                    current,
                    registered: false,
                    released: false,
                },
            );
        }
    }
    for row in audit.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = match decode_replication_administration_v1(value.value()) {
            Ok(decoded) => decoded,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => continue,
            Err(error) => return Err(codec_error(error)),
        };
        let record = decoded.value();
        if key.value() != crate::keys::encode_audit_key(record.administration_sequence()) {
            return Err(corrupt());
        }
        let source = record.after().hold().lineage();
        if source.database_id() != history.lineage().database_id()
            || Some(record.administration_sequence()) > history.tail().frontier().administration()
        {
            return Err(corrupt());
        }
        // Restore and promotion retain older-incarnation audit under the same
        // permanent database ID. It grants no current hold. A future incarnation
        // or another epoch of this incarnation is conflicting source evidence.
        if source != history.lineage() {
            if source.history_incarnation() >= history.lineage().history_incarnation() {
                return Err(corrupt());
            }
            continue;
        }
        if !record.observed().precedes_or_equals(history.tail())
            || record.generation() > history.tail().sequence()
        {
            return Err(corrupt());
        }
        validate_point(record.observed(), history, receipts)?;
        validate_physical_record(record, value.value(), history, receipts)?;
        let entry = evidence
            .get_mut(&record.after().hold().storage_key())
            .ok_or_else(corrupt)?;
        match record.action() {
            Action::RegisterFollower => {
                if entry.registered
                    || entry.released
                    || !matches_registration(record, entry.current)
                {
                    return Err(corrupt());
                }
                entry.registered = true;
            }
            Action::RetireFollower | Action::ExpireFollower => {
                // The checked receipt preserves the exact before policy and
                // validates configured expiry's original registration sequence.
                // Requiring its exact after-image also preserves degradation.
                if !entry.registered || entry.released || record.after() != entry.current {
                    return Err(corrupt());
                }
                entry.released = true;
            }
        }
    }
    if evidence.values().any(|entry| {
        !entry.registered || entry.released != (entry.current.phase() == Phase::Retired)
    }) {
        return Err(corrupt());
    }
    Ok(())
}

/// A pruned point remains bound by its retained administration record. A point
/// in the retained interval must agree byte-for-byte with that chain's receipt.
pub(crate) fn validate_point(
    point: Point,
    history: History,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    if point.sequence() < history.minimum_resume().sequence() {
        return if point.precedes_or_equals(history.minimum_resume()) {
            Ok(())
        } else {
            Err(corrupt())
        };
    }
    if !point.precedes_or_equals(history.tail()) {
        return Err(corrupt());
    }
    let row = receipts
        .get(point.sequence().get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let receipt = AuthoritativeTransactionV3::decode(row.value()).map_err(|_| corrupt())?;
    if Point::from_receipt(&receipt).map_err(|_| corrupt())? != point {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_physical_record(
    record: &Record,
    encoded: &[u8],
    history: History,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let sequence = record
        .observed()
        .sequence()
        .checked_next()
        .ok_or_else(corrupt)?;
    if sequence < history.minimum_resume().sequence() {
        return Ok(());
    }
    let row = receipts
        .get(sequence.get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let receipt = AuthoritativeTransactionV3::decode(row.value()).map_err(|_| corrupt())?;
    let binding = receipt.binding();
    if binding.sequence != sequence
        || binding.database_id != history.lineage().database_id()
        || binding.history_incarnation != history.lineage().history_incarnation()
        || binding.predecessor != Some(record.observed().sequence())
        || binding.predecessor_frontier != record.observed().frontier()
        || binding.prior_history_hash != record.observed().history_hash()
        || binding.covered_frontier
            != riffdb_types::DualFrontier::new(
                record.observed().frontier().application(),
                Some(record.administration_sequence()),
            )
        || receipt.attribution() != ChangelogAttributionV3::RetentionHold
        || !receipt.mutations().iter().any(|mutation| {
            mutation.namespace() == N::Audit
                && mutation.key() == crate::keys::encode_audit_key(record.administration_sequence())
                && mutation.expected_hash().is_none()
                && mutation.value() == Some(encoded)
        })
    {
        return Err(corrupt());
    }
    Ok(())
}

fn matches_registration(record: &Record, current: Policy) -> bool {
    let original = record.after();
    original.registered_at() == current.registered_at()
        && original.generation() == current.generation()
        && original.budget() == current.budget()
        && original.expires_at() == current.expires_at()
        && original
            .hold()
            .fence()
            .precedes_or_equals(current.hold().fence())
        && (current.phase() != Phase::AwaitingBootstrap
            || original.phase() == Phase::AwaitingBootstrap)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
#[path = "replication_registration_links_tests.rs"]
mod tests;
