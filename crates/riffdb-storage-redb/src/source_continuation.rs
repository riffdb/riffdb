//! Closed classification of actual, predecessor-checked native mutations.
//! No public caller may assert qualification. Unknown writers invalidate before
//! durability; one checked epoch replaces mutation queues and ancestor pins.

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, ChangelogHistoryStateV3,
    CompositeMutationV1, CompositeTableV1 as T,
};
use riffdb_types::AdministrationSequence;

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
            key.len() == 8
                && before.is_none()
                && after.is_some()
                && prior.is_none_or(|prior| key > prior.get().to_be_bytes().as_slice())
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
mod tests {
    use super::*;

    // req: STO-030, STO-031, STO-032, END-008
    #[test]
    fn continuation_footprint_refuses_old_audit_preimages_and_control_changes() {
        let prior = AdministrationSequence::new(7).unwrap();
        assert!(unchanged_source_or_append(
            N::Audit,
            &8_u64.to_be_bytes(),
            None,
            Some(b"row"),
            Some(prior)
        ));
        for sequence in [6_u64, 7] {
            assert!(!unchanged_source_or_append(
                N::Audit,
                &sequence.to_be_bytes(),
                None,
                Some(b"row"),
                Some(prior)
            ));
        }
        assert!(!unchanged_source_or_append(
            N::Audit,
            &8_u64.to_be_bytes(),
            Some([1; 32]),
            Some(b"row"),
            Some(prior)
        ));
        assert!(!unchanged_source_or_append(
            N::AuditByRequest,
            b"key",
            Some([1; 32]),
            None,
            Some(prior)
        ));
        for namespace in [
            N::CatalogActive,
            N::RecordRegistry,
            N::HistoryIncarnation,
            N::ReplicationSourceHolds,
            N::ChangelogHistory,
            N::ChangelogHistoryState,
            N::LeadershipEpoch,
            N::AuthoritativeStateCatalog,
            N::CleanCloseLifecycle,
        ] {
            assert!(!unchanged_source_or_append(
                namespace,
                b"key",
                None,
                Some(b"row"),
                Some(prior)
            ));
        }
        let mut unknown = SourceContinuationFootprint::fresh();
        unknown.combine(SourceContinuationFootprint::unknown());
        unknown.combine(SourceContinuationFootprint::fresh());
        assert!(!unknown.is_append_only());
    }
}
