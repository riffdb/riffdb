//! Backend-private index migration mechanics consumed by the catalog-owned driver.

use super::*;
use crate::hooks::RedbTestOperation;
use redb::Durability;
use riffdb_catalog::{
    CatalogIndexMigrationApplied, CatalogIndexMigrationBackend, CatalogIndexMigrationBundleRequest,
    CatalogIndexMigrationBundleResponse, CatalogIndexMigrationCompletion,
    CatalogIndexMigrationInstruction, CatalogIndexMigrationPendingBatch, CatalogIndexMigrationScan,
    CatalogIndexMigrationScanRequest,
};

/// Exclusive redb migration capability released only by a V1-observing startup pass.
pub struct RedbStartupIndexMigrationPort {
    shared: Arc<SharedRedb>,
    lease: ExclusiveLease,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    next_cursor: IndexMigrationCursor,
    after: Option<riffdb_types::IndexEntryKey>,
    #[cfg(test)]
    substitute_before_apply: Option<riffdb_storage_api::StoredIndexEntryV2>,
}

impl RedbStartupIndexMigrationPort {
    pub(super) fn new(
        shared: Arc<SharedRedb>,
        lease: ExclusiveLease,
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
    ) -> Self {
        Self {
            shared,
            lease,
            database_id,
            open_session_id,
            next_cursor: IndexMigrationCursor::start(database_id, open_session_id),
            after: None,
            #[cfg(test)]
            substitute_before_apply: None,
        }
    }

    #[cfg(test)]
    pub(super) fn substitute_before_apply(
        &mut self,
        replacement: riffdb_storage_api::StoredIndexEntryV2,
    ) {
        self.substitute_before_apply = Some(replacement);
    }
}

impl StartupIndexMigrationPort for RedbStartupIndexMigrationPort {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

impl CatalogIndexMigrationBackend for RedbStartupIndexMigrationPort {
    type Output = RedbStore;

    fn read_index_migration_page(
        self,
        request: CatalogIndexMigrationScanRequest<Self>,
    ) -> Result<CatalogIndexMigrationScan<Self>, StorageError> {
        let cursor = request.cursor();
        if cursor != self.next_cursor {
            return Err(invariant());
        }

        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let lower = self
            .after
            .as_ref()
            .map_or(Unbounded, |key| Excluded(key.as_bytes()));
        let mut scan = table
            .range::<&[u8]>((lower, Unbounded))
            .map_err(precommit_storage_error)?;
        let mut rows = Vec::new();
        let mut evidence_bytes = 0usize;
        let mut instruction_bytes = 0usize;
        let mut exhausted = true;

        for entry in &mut scan {
            let (physical_key, envelope) = entry.map_err(precommit_storage_error)?;
            let physical =
                keys::decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
            let evidence = codec::decode_index_migration_row(&physical, envelope.value())?;
            let next_evidence = evidence_bytes
                .checked_add(evidence.evidence_page_charge())
                .ok_or_else(limit_exceeded)?;
            let next_instruction = instruction_bytes
                .checked_add(evidence.instruction_page_charge())
                .ok_or_else(limit_exceeded)?;
            if rows.len() == riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
                || next_evidence > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                || next_instruction > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
            {
                if rows.is_empty() {
                    return Err(limit_exceeded());
                }
                exhausted = false;
                break;
            }
            evidence_bytes = next_evidence;
            instruction_bytes = next_instruction;
            rows.push(evidence);
        }
        drop(scan);
        drop(table);
        drop(transaction);

        if rows.is_empty() {
            if !exhausted {
                return Err(invariant());
            }
            return request.exact_end(self).map_err(value_error_as_storage);
        }

        let count = u64::try_from(rows.len()).map_err(|_| limit_exceeded())?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let after = rows.last().ok_or_else(invariant)?.physical_key().clone();
        let mut port = self;
        port.next_cursor = next;
        port.after = Some(after);
        port.shared.observe_index_migration_page();
        request
            .page(port, rows, next)
            .map_err(value_error_as_storage)
    }

    fn read_historical_bundle(
        self,
        request: CatalogIndexMigrationBundleRequest<Self>,
    ) -> Result<CatalogIndexMigrationBundleResponse<Self>, StorageError> {
        let binding = request.evidence().row().schema_binding();
        let lineage = binding.lineage().clone();
        let version = binding.contract_version();
        let bundle_hash = binding.bundle_hash();
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let bundle = read_historical_bundle(&transaction, &lineage, version, bundle_hash)?
            .ok_or_else(corrupt)?;
        drop(transaction);
        request
            .respond(self, bundle)
            .map_err(value_error_as_storage)
    }

    fn apply_index_migration_batch(
        self,
        pending: CatalogIndexMigrationPendingBatch<Self>,
    ) -> Result<CatalogIndexMigrationApplied<Self>, StorageError> {
        let batch = pending.batch();
        if self.next_cursor != batch.next()
            || batch.next().database_id() != self.database_id
            || batch.next().open_session_id() != self.open_session_id
        {
            return Err(invariant());
        }
        #[cfg(test)]
        if let Some(replacement) = &self.substitute_before_apply {
            apply_index_migration_substitution_fixture(&self.shared, replacement)?;
        }
        let (v1_rewrites, v2_confirms) =
            batch
                .instructions()
                .iter()
                .fold(
                    (0usize, 0usize),
                    |(v1, v2), instruction| match instruction {
                        CatalogIndexMigrationInstruction::V1Rewrite(_) => (v1 + 1, v2),
                        CatalogIndexMigrationInstruction::V2Confirm(_) => (v1, v2 + 1),
                    },
                );
        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| invariant())?;
        let transaction = crate::store::OperationalWriteTransaction::from_drained(
            transaction,
            riffdb_storage_api::ChangelogAttributionV3::IndexMigrationBatch,
        )?;
        {
            let mut table = transaction
                .open_table(SECONDARY_INDEXES)
                .map_err(table_error)?;
            for instruction in batch.instructions() {
                apply_index_migration_instruction(&mut table, instruction)?;
            }
        }
        self.shared
            .before_test_commit(RedbTestOperation::IndexMigrationBatch)?;
        transaction.finish()?.commit(&self.shared)?;
        self.shared
            .after_test_commit(RedbTestOperation::IndexMigrationBatch)?;
        self.shared
            .observe_index_migration_batch(v1_rewrites, v2_confirms);
        pending.applied(self).map_err(value_error_as_storage)
    }

    fn finish_index_migration(
        self,
        completion: CatalogIndexMigrationCompletion<Self>,
    ) -> Result<RedbStore, StorageError> {
        if completion.final_cursor() != self.next_cursor {
            return Err(invariant());
        }
        let RedbStartupIndexMigrationPort {
            shared,
            lease,
            database_id: _,
            open_session_id: _,
            next_cursor: _,
            after: _,
            #[cfg(test)]
                substitute_before_apply: _,
        } = self;
        drop(lease);
        let store = RedbStore { shared };
        store.complete_partition_index_generation_migration()?;
        Ok(store)
    }
}

#[cfg(test)]
fn apply_index_migration_substitution_fixture(
    shared: &SharedRedb,
    replacement: &riffdb_storage_api::StoredIndexEntryV2,
) -> Result<(), StorageError> {
    let encoded = codec::encode_index_entry_v2(replacement)?;
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| invariant())?;
    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        table
            .insert(replacement.key().as_bytes(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    shared.commit_durable(transaction)
}

fn apply_index_migration_instruction(
    table: &mut crate::changelog_v3_capture::CapturedTable<'_, '_, &'static [u8]>,
    instruction: &CatalogIndexMigrationInstruction,
) -> Result<(), StorageError> {
    let expected = instruction.expected();
    let key = expected.physical_key().as_bytes();
    let current = table
        .get(key)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let current_bytes = current.value();
    match instruction {
        CatalogIndexMigrationInstruction::V1Rewrite(rewrite) => {
            if !rewrite.expected().row().is_v1() {
                return Err(invariant());
            }
            let replacement = codec::encode_index_entry_v2(rewrite.replacement())?;
            if replacement.encoded_content_charge().get()
                > expected.conservative_v2_envelope_charge().get()
            {
                return Err(invariant());
            }
            if current_bytes == rewrite.expected().canonical_envelope() {
                drop(current);
                table
                    .insert(key, replacement.as_bytes())
                    .map_err(precommit_storage_error)?;
            } else if current_bytes != replacement.as_bytes() {
                return Err(corrupt());
            }
        }
        CatalogIndexMigrationInstruction::V2Confirm(confirm) => {
            if confirm.expected().row().is_v1()
                || current_bytes != confirm.expected().canonical_envelope()
            {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}
