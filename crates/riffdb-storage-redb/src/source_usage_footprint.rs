//! Closed Source-only native mutation inventory. It is not the unrelated
//! append-only command-continuation classification and grants no write authority.
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeStateCatalogV3, ChangelogHistoryStateV3,
    CompositeMutationV1, CompositeTableV1 as T, StorageError, StorageErrorKind,
};

#[derive(Clone, Copy)]
pub(crate) struct SourceUsageFootprint {
    changed: bool,
}
impl SourceUsageFootprint {
    /// Exact complete canonical net mutations, already predecessor-checked by
    /// the operation owner. Ordinary command/event/head mutations remain false.
    pub(crate) fn captured(mutations: &[AuthoritativeMutationV3]) -> Self {
        Self {
            changed: mutations
                .iter()
                .any(|row| Self::affects_source_or_incarnation(row.namespace())),
        }
    }

    /// Closed unchanged raw/control owner, not an unclassified transaction
    /// assertion. Its callers are inventoried separately from typed receipts.
    pub(crate) const fn fixed_source_absent() -> Self {
        Self { changed: false }
    }

    fn affects_source_or_incarnation(
        namespace: riffdb_storage_api::AuthoritativeNamespaceV3,
    ) -> bool {
        use riffdb_storage_api::AuthoritativeNamespaceV1 as N;
        crate::source_usage_domain::namespace_affects_source_usage(namespace)
            || matches!(
                namespace.as_v1(),
                Some(
                    N::DatabaseIdentity
                        | N::HistoryIncarnation
                        | N::AuthoritativeStateCatalog
                        | N::RecordRegistry
                        | N::FormatVersion
                )
            )
    }

    /// Fixed physical journal domains use the same sole namespace declaration.
    /// The caller must supply its complete actual table/key inventory; this is
    /// not permission for an unclassified writer to claim absence of changes.
    pub(crate) fn physical(table: &str, key: &[u8]) -> Option<Self> {
        AuthoritativeStateCatalogV3
            .lookup(table, key)
            .map(|namespace| Self {
                changed: Self::affects_source_or_incarnation(namespace),
            })
    }

    /// Actual reset/cut/incarnation transition. Later appends within the same
    /// lineage do not invalidate measured Source byte usage.
    pub(crate) fn lineage_transition(
        before: ChangelogHistoryStateV3,
        after: ChangelogHistoryStateV3,
    ) -> Self {
        Self {
            changed: before.lineage().database_id() != after.lineage().database_id()
                || before.lineage().history_incarnation() != after.lineage().history_incarnation(),
        }
    }
    /// Closed logical journal inventory. Source maintenance is an Immediate
    /// barrier, so a Source allocator post-image must never enter the overlay.
    /// This validates exact net mutations before lane submission; it neither
    /// grants mutation authority nor guesses Source absence from app heads.
    pub(crate) fn require_no_source_overlay(
        mutations: &[CompositeMutationV1],
    ) -> Result<(), StorageError> {
        for mutation in mutations {
            match mutation.table() {
                T::Meta => {
                    let source = Self::physical("meta", mutation.key());
                    // Existing journal metadata has its own validator. Unknown
                    // keys do not acquire Source authority from this check.
                    if source.is_some_and(Self::changed) {
                        return Err(StorageError::new(
                            StorageErrorKind::InvariantViolation,
                            None,
                        ));
                    }
                }
                // Exhaustive: a new Source table cannot silently default to a
                // Source-free overlay. Existing non-Meta domains contain no
                // Source namespace or allocator.
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
                | T::Audit
                | T::AuditByRequest
                | T::EntityChainHeads
                | T::VectorEvidence
                | T::VectorObservations
                | T::VectorEvidenceIndex
                | T::IdempotencyLocators
                | T::ProvenanceLocators
                | T::AuditByRequestLocators
                | T::HistoryTombstones
                | T::OutboxStatus => {}
            }
        }
        Ok(())
    }

    pub(crate) fn combine(&mut self, other: Self) {
        self.changed |= other.changed;
    }
    pub(crate) const fn changed(self) -> bool {
        self.changed
    }
}
