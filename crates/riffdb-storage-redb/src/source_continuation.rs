//! Closed classification of actual, predecessor-checked native mutations.
//! No public caller may assert qualification. Unknown writers invalidate before
//! durability; one checked epoch replaces mutation queues and ancestor pins.

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, ChangelogHistoryStateV3,
    CompositeMutationV1, CompositeTableV1 as T,
};
use riffdb_types::{AdministrationSequence, CommitSequence};

/// Produced only by the sealed native write/materialization owners. Absence
/// of source changes is established from their complete actual mutation set.
#[derive(Clone, Copy)]
pub(crate) struct SourceContinuationFootprint {
    append_only: bool,
}

impl SourceContinuationFootprint {
    pub(crate) const fn unknown() -> Self {
        Self { append_only: false }
    }

    /// A fresh, exclusively owned transaction with no earlier raw writes.
    pub(crate) const fn fresh() -> Self {
        Self { append_only: true }
    }

    pub(crate) fn combine(&mut self, other: Self) {
        self.append_only &= other.append_only;
    }

    pub(crate) const fn is_append_only(self) -> bool {
        self.append_only
    }

    pub(crate) fn captured(
        history: ChangelogHistoryStateV3,
        mutations: &[AuthoritativeMutationV3],
    ) -> Self {
        Self {
            append_only: mutations.iter().all(|mutation| {
                unchanged_source_or_append(
                    mutation.namespace(),
                    mutation.key(),
                    mutation.expected_hash(),
                    mutation.value(),
                    history.tail().frontier().administration(),
                )
            }),
        }
    }

    /// Invoked on the net mutations returned by the checked composite stage,
    /// and again on the very mutations applied by native materialization.
    pub(crate) fn composite(
        prior: Option<AdministrationSequence>,
        mutations: &[CompositeMutationV1],
    ) -> Self {
        Self {
            append_only: mutations.iter().all(|mutation| {
                match mutation.table() {
                    T::Meta => {
                        // The non-recursive receipt owner alone applies this checked
                        // allocator. Ordinary mixed metadata remains a closed set.
                        mutation.key()
                            == N::NextChangelogTransaction
                                .metadata_key()
                                .map(str::as_bytes)
                                .unwrap_or_default()
                            || riffdb_storage_api::AuthoritativeStateCatalogV1
                                .lookup("meta", mutation.key())
                                .is_some_and(|namespace| {
                                    unchanged_source_or_append(
                                        namespace,
                                        mutation.key(),
                                        mutation.expected_hash(),
                                        mutation.value(),
                                        prior,
                                    )
                                })
                    }
                    T::Audit => unchanged_source_or_append(
                        N::Audit,
                        mutation.key(),
                        mutation.expected_hash(),
                        mutation.value(),
                        prior,
                    ),
                    T::AuditByRequest => unchanged_source_or_append(
                        N::AuditByRequest,
                        mutation.key(),
                        mutation.expected_hash(),
                        mutation.value(),
                        prior,
                    ),
                    // This closed enum has no source-control or catalog namespace.
                    T::Entities
                    | T::SecondaryIndexes
                    | T::IndexEpochs
                    | T::Idempotency
                    | T::IdempotencyPending
                    | T::Events
                    | T::EventRoutes
                    | T::Outbox
                    | T::Provenance
                    | T::Commits
                    | T::EntityChainHeads
                    | T::VectorEvidence
                    | T::VectorObservations
                    | T::VectorEvidenceIndex
                    | T::IdempotencyLocators
                    | T::ProvenanceLocators
                    | T::AuditByRequestLocators
                    | T::HistoryTombstones
                    | T::OutboxStatus => true,
                }
            }),
        }
    }

    /// Qualifies the source-independent representation change performed by
    /// native command pruning. Inspect actual predecessor bytes, not merely
    /// the namespace or a claimed retention attribution. A locator and its
    /// materialized record must name the exact same canonical command audit.
    pub(crate) fn composite_checked(
        prior: Option<AdministrationSequence>,
        mutations: &[CompositeMutationV1],
        mut before: impl FnMut(
            &CompositeMutationV1,
        ) -> Result<Option<Vec<u8>>, riffdb_storage_api::StorageError>,
        mut command_row: impl FnMut(
            CommitSequence,
        ) -> Result<
            Option<(Vec<u8>, Vec<u8>)>,
            riffdb_storage_api::StorageError,
        >,
    ) -> Result<Self, riffdb_storage_api::StorageError> {
        let mut segment = None;
        for mutation in mutations {
            if Self::composite(prior, std::slice::from_ref(mutation)).is_append_only() {
                continue;
            }
            if mutation.table() != T::Audit
                || !checked_audit_representation(
                    prior,
                    (mutation.key(), mutation.expected_hash(), mutation.value()),
                    before(mutation)?,
                    &mut command_row,
                    &mut segment,
                )?
            {
                return Ok(Self::unknown());
            }
        }
        Ok(Self::fresh())
    }

    /// Checked canonical service-audit materialization before a decoded journal
    /// source is applied. The same closed audit proof serves both native paths.
    pub(crate) fn captured_checked(
        history: ChangelogHistoryStateV3,
        mutations: &[AuthoritativeMutationV3],
        mut before: impl FnMut(
            &AuthoritativeMutationV3,
        ) -> Result<Option<Vec<u8>>, riffdb_storage_api::StorageError>,
        mut command_row: impl FnMut(
            CommitSequence,
        ) -> Result<
            Option<(Vec<u8>, Vec<u8>)>,
            riffdb_storage_api::StorageError,
        >,
    ) -> Result<Self, riffdb_storage_api::StorageError> {
        let mut segment = None;
        for mutation in mutations {
            if Self::captured(history, std::slice::from_ref(mutation)).is_append_only() {
                continue;
            }
            if mutation.namespace() != N::Audit
                || !checked_audit_representation(
                    history.tail().frontier().administration(),
                    (mutation.key(), mutation.expected_hash(), mutation.value()),
                    before(mutation)?,
                    &mut command_row,
                    &mut segment,
                )?
            {
                return Ok(Self::unknown());
            }
        }
        Ok(Self::fresh())
    }
}

// One decoded segment, shared by the complete bounded native frame. The actual
// preimage is checked before canonical audit classification, never afterward.
fn checked_audit_representation(
    prior: Option<AdministrationSequence>,
    (key, expected_hash, value): (&[u8], Option<[u8; 32]>, Option<&[u8]>),
    before: Option<Vec<u8>>,
    command_row: &mut impl FnMut(
        CommitSequence,
    )
        -> Result<Option<(Vec<u8>, Vec<u8>)>, riffdb_storage_api::StorageError>,
    segment: &mut Option<riffdb_storage_api::StoredCommandSegmentV1>,
) -> Result<bool, riffdb_storage_api::StorageError> {
    // Reuse the checked composite preimage primitive; hashing stays
    // within its existing reviewed integrity boundary.
    let observed = before
        .as_deref()
        .map(|bytes| {
            CompositeMutationV1::delete_matching(T::Audit, key, bytes)
                .map(|checked| checked.expected_hash())
        })
        .transpose();
    let Ok(observed) = observed else {
        return Ok(false);
    };
    if observed.flatten() != expected_hash {
        return Ok(false);
    }
    let Some(after) = value else {
        return Ok(false);
    };
    let Ok(after) = riffdb_storage_api::decode_service_audit_record(after) else {
        return Ok(false);
    };
    let after = after.value();
    if key != crate::keys::encode_audit_key(after.administration_sequence())
        || prior.is_none_or(|prior| after.administration_sequence() > prior)
    {
        return Ok(false);
    }
    // Current command segments provide their audit through derived
    // indexes without a physical AUDIT row. Pruning inserts a canonical
    // Service record below the administration frontier. Actual absence
    // is checked above: no previously validated source audit changes,
    // and all three native source validators ignore this closed type.
    let Some(before) = before.as_deref() else {
        return Ok(true);
    };
    let Ok(locator) = riffdb_storage_api::decode_command_audit_locator_v1(before) else {
        // A physical record may remain byte-for-byte equal, but no
        // changed retained record or source-control audit is eligible.
        if value != Some(before) {
            return Ok(false);
        }
        return Ok(true);
    };
    let locator = *locator.value();
    let sequence = locator.commit_sequence();
    if !segment.as_ref().is_some_and(|segment| {
        segment.first_commit_sequence() <= sequence && sequence <= segment.last_commit_sequence()
    }) {
        drop(segment.take());
        let Some((key, encoded)) = command_row(sequence)? else {
            return Ok(false);
        };
        let Ok(decoded) = crate::history_stub::decode_segment_value(&encoded) else {
            return Ok(false);
        };
        let decoded = decoded.into_parts().0;
        if key != crate::keys::encode_application_sequence_key(decoded.first_commit_sequence()) {
            return Ok(false);
        }
        *segment = Some(decoded);
    }
    let Some(command) = segment.as_ref().and_then(|segment| {
        sequence
            .get()
            .checked_sub(segment.first_commit_sequence().get())
            .and_then(|ordinal| usize::try_from(ordinal).ok())
            .and_then(|ordinal| segment.commands().get(ordinal))
            .filter(|command| command.commit_sequence() == sequence)
    }) else {
        return Ok(false);
    };
    if command.base().audit(locator.member()) != after {
        return Ok(false);
    }
    Ok(true)
}

fn unchanged_source_or_append(
    namespace: N,
    key: &[u8],
    before: Option<[u8; 32]>,
    after: Option<&[u8]>,
    prior: Option<AdministrationSequence>,
) -> bool {
    match namespace {
        N::Audit => {
            before.is_none()
                && after.is_some()
                && crate::keys::decode_audit_key(key)
                    .is_ok_and(|sequence| prior.is_none_or(|prior| sequence > prior))
        }
        N::AuditByRequest => before.is_none() && after.is_some(),
        // Explicitly independent of retained source receipts/audit/lineage.
        N::Entities
        | N::EntityChainHeads
        | N::SecondaryIndexes
        | N::IndexEpochs
        | N::Idempotency
        | N::IdempotencyPending
        | N::Commits
        | N::Provenance
        | N::Events
        | N::EventRoutes
        | N::Outbox
        | N::OutboxStatus
        | N::ProjectionState
        | N::ProjectionFrontier
        | N::ProjectionApplied
        | N::Capabilities
        | N::CapabilityTokens
        | N::RetiredEntities
        | N::HistoryTombstones
        | N::EventConsumers
        | N::EventConsumerDeliveries
        | N::VectorEvidence
        | N::VectorObservations
        | N::VectorEvidenceIndex
        | N::VectorProjectionControls
        | N::ColumnarProjectionControls
        | N::IdempotencyLocators
        | N::ProvenanceLocators
        | N::AuditByRequestLocators
        | N::ValidatedPrefixEntityHeads
        | N::ValidatedPrefixCheckpoint
        | N::RetentionWatermark
        | N::RetentionHolds
        | N::NextApplicationSequence
        | N::NextAdministrationSequence => true,
        // Includes unknown/incompatible catalog, source authority, registration,
        // lineage, receipt, clean-close and restore changes. Fail closed.
        _ => false,
    }
}

// Newly added accounting is scalar; native decoders/validators keep their own
// existing bounds. No mutation history, per-hold copy or additional pin here.
const _: () = assert!(
    std::mem::size_of::<SourceContinuationFootprint>()
        + std::mem::size_of::<std::sync::atomic::AtomicU64>()
        <= 64 * 1024
);

#[cfg(test)]
#[path = "source_continuation_tests.rs"]
mod tests;
