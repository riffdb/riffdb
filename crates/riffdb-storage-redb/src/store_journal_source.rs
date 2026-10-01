//! Source allocation and complete receipt preflight before journal submission.
//! These helpers run under the existing mutation lease and never publish or flush.

use super::*;
use crate::changelog_v3_write::value_error;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3, ChangelogAttributionV3,
    ChangelogHistoryStateV3, CompositeMutationV1, CompositeTableV1,
    proto_codec::encode_changelog_transaction_allocator_v3,
};

impl SharedRedb {
    pub(super) fn stage_changelog_allocator(
        &self,
        mutations: &mut crate::journal::JournalMutationBuffer,
        stage: &mut crate::composite_view::RedbCompositeMutationStage,
    ) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
        let predecessor = self
            .journal_runtime()?
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .changelog_history;
        let Some(history) = predecessor else {
            return Ok(None);
        };
        let (_, successor) = history
            .expected_allocator()
            .allocate_one()
            .map_err(value_error)?;
        let before = encode_changelog_transaction_allocator_v3(history.expected_allocator())
            .map_err(crate::error::codec_error)?;
        let after = encode_changelog_transaction_allocator_v3(successor)
            .map_err(crate::error::codec_error)?;
        let mutation = crate::journal::JournalMutation::replace(
            JournalTable::Meta,
            N::NextChangelogTransaction
                .metadata_key()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .as_bytes(),
            before.as_bytes(),
            after.into_bytes(),
        )
        .map_err(journal_storage_error)?;
        // Any error abandons the whole private invocation. Buffer bounds are
        // checked before adding this source to the stage; no lane submission has
        // occurred, and the stage still checks the exact current before-image.
        mutations
            .extend([mutation.clone()])
            .map_err(journal_storage_error)?;
        stage.apply(&mutation)?;
        Ok(Some(history))
    }
}

impl JournalRuntime {
    pub(super) fn prepare_changelog_successor(
        &self,
        expected: Option<ChangelogHistoryStateV3>,
        source: ChangelogAttributionV3,
        covered: DualFrontier,
        mutations: &[CompositeMutationV1],
    ) -> Result<Option<(AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3)>, StorageError>
    {
        if self.changelog_history != expected {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let Some(history) = expected else {
            if mutations.iter().any(|m| {
                m.table() == CompositeTableV1::Meta
                    && Some(m.key())
                        == N::NextChangelogTransaction
                            .metadata_key()
                            .map(str::as_bytes)
            }) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            return Ok(None);
        };
        if history.lineage().database_id() != self.database_id
            || history.tail().frontier()
                != DualFrontier::new(self.last_sequence, self.last_administration_sequence)
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let binding = AuthoritativeTransactionBindingV3 {
            database_id: self.database_id,
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history
                .expected_allocator()
                .allocate_one()
                .map_err(value_error)?
                .0,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: covered,
            prior_history_hash: history.tail().history_hash(),
        };
        let receipt = crate::changelog_v3::receipt_from_validated_mutations_for_catalog(
            binding,
            source,
            mutations,
            history.lineage().catalog_digest(),
        )
        .map_err(value_error)?;
        // Complete canonical coalescing, bounds and attribution are proven BEFORE
        // lane.submit. Retain only constant-size binding/history; original values
        // remain in the existing bounded retained mutation source, not a copy.
        Ok(Some((
            binding,
            history.advance(&receipt).map_err(value_error)?,
        )))
    }
}
