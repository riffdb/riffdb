//! Same-pin registration/retirement evidence for the bounded source hold set.
use crate::source_validation_read::ReceiptRead;

use redb::ReadableTable;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogAttributionV3, ChangelogHistoryPointV3 as Point,
    ChangelogHistoryStateV3 as History, DurableCodecErrorKind,
    FollowerRegistrationPhaseV1 as Phase, MAX_REPLICATION_SOURCE_HOLDS_V1,
    ReplicationAdministrationActionV1 as Action, ReplicationSourceHoldStateV1 as State,
    ReplicationSourceHoldV2 as Policy, StorageError, StorageErrorKind,
    StoredReplicationAdministrationV1 as Record,
    proto_codec::{decode_replication_administration_v1, decode_replication_source_hold},
};

use crate::error::{codec_error, precommit_storage_error, storage_error};

struct Evidence {
    tracked: bool,
    registered: bool,
    released: bool,
    retired: bool,
}

/// Complete source-only cross-link pass, after retained-chain validation. This
/// streams audit history to its exact end, retaining four flags for each of the existing 4096
/// holds. Policies remain borrowed from the same immutable source pin. It is not a checkpoint shortcut, does
/// not run on follower images, and grants no authorization or release capability.
pub(crate) fn validate(
    holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
    audit: &impl ReadableTable<&'static [u8], &'static [u8]>,
    history: History,
    receipts: &impl ReceiptRead,
) -> Result<(), StorageError> {
    let mut validation = RegistrationLinksValidation::new(holds, history)?;
    for row in audit.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        validation.observe(key.value(), value.value(), holds, receipts)?;
    }
    validation.finish()
}

/// Incremental form of the same exhaustive, bounded-hold validation.
/// Only finish at the caller's exact audit end; partial pages prove nothing.
pub(crate) struct RegistrationLinksValidation {
    history: History,
    evidence: Box<[Evidence]>,
}

impl RegistrationLinksValidation {
    pub(crate) const fn bookkeeping_bound() -> usize {
        std::mem::size_of::<Self>()
            + MAX_REPLICATION_SOURCE_HOLDS_V1 as usize * std::mem::size_of::<Evidence>()
    }

    pub(crate) fn new(
        holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
        history: History,
    ) -> Result<Self, StorageError> {
        if holds.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut evidence = Vec::with_capacity(
            usize::try_from(holds.len().map_err(precommit_storage_error)?)
                .map_err(|_| corrupt())?,
        );
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
            if let Some(current) = registered
                && (!current.registered_at().precedes_or_equals(history.tail())
                    || current.generation() > history.tail().sequence()
                    || !hold.fence().precedes_or_equals(history.tail())
                    || current
                        .degraded_at()
                        .is_some_and(|p| !p.precedes_or_equals(history.tail())))
            {
                return Err(corrupt());
            }
            evidence.push(Evidence {
                tracked: registered.is_some(),
                registered: false,
                released: false,
                retired: registered.is_some_and(|current| current.phase() == Phase::Retired),
            });
        }
        Ok(Self {
            history,
            evidence: evidence.into_boxed_slice(),
        })
    }

    /// Only after the storage publication owner proved the current hold rows
    /// and prior audit interval unchanged. Retains the native evidence census.
    pub(crate) fn rebind_unchanged(&mut self, history: History) -> Result<(), StorageError> {
        if self.history.lineage() != history.lineage() {
            return Err(corrupt());
        }
        self.history = history;
        Ok(())
    }

    pub(crate) fn observe(
        &mut self,
        key: &[u8],
        encoded: &[u8],
        holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
        receipts: &impl ReceiptRead,
    ) -> Result<(), StorageError> {
        let history = self.history;
        let decoded = match decode_replication_administration_v1(encoded) {
            Ok(decoded) => decoded,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                return Ok(());
            }
            Err(error) => return Err(codec_error(error)),
        };
        let record = decoded.value();
        if key != crate::keys::encode_audit_key(record.administration_sequence()) {
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
            return Ok(());
        }
        if !record.observed().precedes_or_equals(history.tail())
            || record.generation() > history.tail().sequence()
        {
            return Err(corrupt());
        }
        validate_point(record.observed(), history, receipts)?;
        validate_physical_record(record, encoded, history, receipts)?;
        // The canonical row ordinal is stable only on this immutable source
        // (or its owner-qualified unchanged-hold successor). Keep no policy
        // copies or map nodes in the continuation's bounded bookkeeping.
        let target = record.after().hold().storage_key();
        let mut found = None;
        for (ordinal, row) in holds
            .range::<&[u8]>(..=target.as_slice())
            .map_err(precommit_storage_error)?
            .enumerate()
        {
            if ordinal >= self.evidence.len() {
                return Err(corrupt());
            }
            let (key, value) = row.map_err(precommit_storage_error)?;
            if key.value() == target {
                let decoded = decode_replication_source_hold(value.value()).map_err(codec_error)?;
                let State::Registered(current) = *decoded.value() else {
                    return Err(corrupt());
                };
                if current.hold().storage_key() != target
                    || current.hold().lineage() != history.lineage()
                {
                    return Err(corrupt());
                }
                found = Some((ordinal, current));
            }
        }
        let (ordinal, current) = found.ok_or_else(corrupt)?;
        let entry = self.evidence.get_mut(ordinal).ok_or_else(corrupt)?;
        if !entry.tracked || entry.retired != (current.phase() == Phase::Retired) {
            return Err(corrupt());
        }
        match record.action() {
            Action::RegisterFollower => {
                if entry.registered || entry.released || !matches_registration(record, current) {
                    return Err(corrupt());
                }
                entry.registered = true;
            }
            Action::RetireFollower | Action::ExpireFollower => {
                // The checked receipt preserves the exact before policy and
                // validates configured expiry's original registration sequence.
                // Requiring its exact after-image also preserves degradation.
                if !entry.registered || entry.released || record.after() != current {
                    return Err(corrupt());
                }
                entry.released = true;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<(), StorageError> {
        if self
            .evidence
            .iter()
            .any(|entry| entry.tracked && (!entry.registered || entry.released != entry.retired))
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

/// A pruned point remains bound by its retained administration record. A point
/// in the retained interval must agree byte-for-byte with that chain's receipt.
pub(crate) fn validate_point(
    point: Point,
    history: History,
    receipts: &impl ReceiptRead,
) -> Result<(), StorageError> {
    validate_point_with_admission(point, history, receipts, None)
}

pub(crate) fn validate_point_with_admission(
    point: Point,
    history: History,
    receipts: &impl ReceiptRead,
    budget: Option<&riffdb_types::exact_provider_budget_v1::ProviderBudget>,
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
    let receipt = receipts
        .receipt(point.sequence(), budget)?
        .ok_or_else(corrupt)?;
    let summary = crate::changelog_v3::ValidatedReceiptSummaryV3::from_receipt(&receipt)
        .map_err(|_| corrupt())?;
    if summary.point() != point {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_physical_record(
    record: &Record,
    encoded: &[u8],
    history: History,
    receipts: &impl ReceiptRead,
) -> Result<(), StorageError> {
    let sequence = record
        .observed()
        .sequence()
        .checked_next()
        .ok_or_else(corrupt)?;
    if sequence < history.minimum_resume().sequence() {
        return Ok(());
    }
    let receipt = receipts.receipt(sequence, None)?.ok_or_else(corrupt)?;
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
pub(crate) mod tests;
