//! Offline retention watermark, fencing, prune, and tombstone verification (ADR-0085 A2).

use std::collections::BTreeSet;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    WriteTransaction,
};
use riffdb_storage_api::{
    HISTORY_INCARNATION_INITIAL, HistoryTombstoneContentDigest, HistoryTombstoneHash,
    RetentionAdministrationAction, RetentionFenceBinding, RetentionFencingInputs,
    RetentionHoldKind, RetentionHoldV1, StorageError, StorageErrorKind,
    StoredAdministrationAuditRecordV1, StoredHistoryTombstoneV1, StoredOutboxStatusV1,
    StoredRetentionAdministrationV1, StoredRetentionHoldsV1, StoredRetentionWatermarkV1,
    compute_max_permissible_watermark, history_tombstone_chain_root,
    proto_codec::{
        current_record_registry_digest, decode_columnar_projection_control_v1,
        decode_history_tombstone_v1, decode_projection_control_v1, decode_retention_holds_v1,
        decode_retention_watermark_v1, encode_history_tombstone_v1, encode_retention_holds_v1,
        encode_retention_watermark_v1,
    },
};
use riffdb_types::{
    CommitSequence, FrontierPosition, HashDomain, ProjectionId, SchemaHash, Timestamp, hash,
};

use crate::codec;
use crate::consumer::retention_low_water_from_table;
use crate::error::{
    database_error, precommit_storage_error, storage_error, table_error, transaction_error,
};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::{
    decode_application_sequence_key, decode_event_key, encode_application_sequence_key,
    encode_audit_by_request_key, encode_audit_key, encode_event_route_key, encode_idempotency_key,
    encode_provenance_key,
};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, CONTRACT_MIGRATION_JOURNAL, EVENT_CONSUMERS, EVENT_ROUTES,
    EVENTS, HISTORY_TOMBSTONES, IDEMPOTENCY, META, META_APPLICATION_SEQUENCE, META_DATABASE_ID,
    META_HISTORY_INCARNATION, META_RETENTION_HOLDS, META_RETENTION_WATERMARK,
    META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS, PROJECTION_FRONTIER, PROVENANCE,
};
use crate::store::RedbStore;

/// Inclusive commit sequences pruned per offline sub-range transaction.
pub(crate) const RETENTION_PRUNE_SUBRANGE_SEQUENCES: u64 = 1_024;

/// Status snapshot for offline retention inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionStatusV1 {
    /// Current watermark sequence (0 = unpruned).
    pub watermark_sequence: u64,
    /// History incarnation bound on the watermark (or live incarnation when absent).
    pub history_incarnation: u64,
    /// Max permissible inclusive watermark under current fencing, if computable.
    pub max_permissible_watermark: Option<u64>,
    /// Which fencing input binds the max, when any.
    pub fence_binding: RetentionFenceBinding,
    /// Operator holds currently stored.
    pub holds: StoredRetentionHoldsV1,
    /// Number of history tombstone rows.
    pub tombstone_count: u64,
}

/// Offline exclusive retention maintenance over a closed database file.
pub struct RedbOfflineRetention {
    database_path: PathBuf,
    test_controller: Option<RedbTestController>,
}

impl RedbOfflineRetention {
    /// Binds one offline database file for exclusive retention maintenance.
    pub fn bind(database_path: impl AsRef<Path>) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            test_controller: None,
        }
    }

    /// Binds with a process-test failpoint controller.
    #[doc(hidden)]
    pub fn bind_with_test_controller(
        database_path: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            test_controller: Some(controller),
        }
    }

    /// Reads retention status without mutating.
    pub fn status(&self) -> Result<RetentionStatusV1, StorageError> {
        let database = Database::open(&self.database_path).map_err(database_error)?;
        let transaction = database.begin_read().map_err(transaction_error)?;
        let watermark = load_watermark(&transaction)?;
        let holds = load_holds(&transaction)?;
        let incarnation = load_history_incarnation(&transaction)?;
        let fencing = collect_fencing_inputs(&transaction, &holds)?;
        let (max_perm, binding) = compute_max_permissible_watermark(&fencing);
        let tombstones = transaction
            .open_table(HISTORY_TOMBSTONES)
            .map_err(table_error)?;
        let tombstone_count = tombstones.len().map_err(precommit_storage_error)?;
        Ok(RetentionStatusV1 {
            watermark_sequence: watermark
                .as_ref()
                .map(|w| w.watermark_sequence())
                .unwrap_or(0),
            history_incarnation: watermark
                .as_ref()
                .map(|w| w.history_incarnation())
                .unwrap_or(incarnation),
            max_permissible_watermark: max_perm,
            fence_binding: binding,
            holds,
            tombstone_count,
        })
    }

    /// Adds or replaces one operator hold (canonical order preserved).
    ///
    /// Refuses to replace a projection-detach hold row: those carry a
    /// different kind and are only removed by the audited reattach action.
    pub fn add_hold(
        &self,
        hold_id: impl Into<String>,
        sequence: u64,
        reason: impl Into<String>,
    ) -> Result<(), StorageError> {
        let hold = RetentionHoldV1::new(hold_id, sequence, reason)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let store = RedbStore::open(&self.database_path)?;
        let preparation = store.prepare_offline_retention()?;
        let write = begin_durable_write(preparation.database())?;
        {
            let mut meta = write.open_table(META).map_err(table_error)?;
            let existing = read_holds_meta(&meta)?;
            let mut list = existing.holds().to_vec();
            if let Some(slot) = list.iter_mut().find(|h| h.hold_id() == hold.hold_id()) {
                if slot.kind() != RetentionHoldKind::Operator {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                *slot = hold;
            } else {
                list.push(hold);
                list.sort_by(|a, b| a.hold_id().cmp(b.hold_id()));
            }
            let holds = StoredRetentionHoldsV1::new(list)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let encoded = encode_retention_holds_v1(&holds).map_err(crate::error::codec_error)?;
            meta.insert(META_RETENTION_HOLDS, encoded.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        self.before_commit(RedbTestOperation::RetentionHold)?;
        commit_durable(write)?;
        self.after_commit(RedbTestOperation::RetentionHold)
    }

    /// Removes one OPERATOR hold by id. Missing is a no-op success; a
    /// projection-detach row with this id refuses (audited reattach only).
    pub fn remove_hold(&self, hold_id: &str) -> Result<(), StorageError> {
        let store = RedbStore::open(&self.database_path)?;
        let preparation = store.prepare_offline_retention()?;
        let write = begin_durable_write(preparation.database())?;
        {
            let mut meta = write.open_table(META).map_err(table_error)?;
            let existing = read_holds_meta(&meta)?;
            if existing
                .holds()
                .iter()
                .any(|h| h.hold_id() == hold_id && h.kind() != RetentionHoldKind::Operator)
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let list: Vec<_> = existing
                .holds()
                .iter()
                .filter(|h| h.hold_id() != hold_id)
                .cloned()
                .collect();
            let holds = StoredRetentionHoldsV1::new(list)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let encoded = encode_retention_holds_v1(&holds).map_err(crate::error::codec_error)?;
            meta.insert(META_RETENTION_HOLDS, encoded.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        self.before_commit(RedbTestOperation::RetentionHold)?;
        commit_durable(write)?;
        self.after_commit(RedbTestOperation::RetentionHold)
    }

    /// Detaches a projection identity from the fencing minimum.
    ///
    /// An audited administration action (ADR-0085 A2): the typed detach hold
    /// row and one retention administration audit record commit atomically
    /// through the shared administration sequence space. The timestamp is
    /// operator-supplied; this offline layer holds no clock.
    pub fn detach_projection(
        &self,
        projection_id: ProjectionId,
        reason: &str,
        timestamp: Timestamp,
    ) -> Result<(), StorageError> {
        self.administer_projection_hold(
            projection_id,
            reason,
            timestamp,
            RetentionAdministrationAction::ProjectionDetach,
        )
    }

    /// Reattaches a previously detached projection to the fencing minimum.
    ///
    /// The inverse audited administration action; refuses when the projection
    /// is not currently detached.
    pub fn reattach_projection(
        &self,
        projection_id: ProjectionId,
        reason: &str,
        timestamp: Timestamp,
    ) -> Result<(), StorageError> {
        self.administer_projection_hold(
            projection_id,
            reason,
            timestamp,
            RetentionAdministrationAction::ProjectionReattach,
        )
    }

    fn administer_projection_hold(
        &self,
        projection_id: ProjectionId,
        reason: &str,
        timestamp: Timestamp,
        action: RetentionAdministrationAction,
    ) -> Result<(), StorageError> {
        let store = RedbStore::open(&self.database_path)?;
        let preparation = store.prepare_offline_retention()?;
        let write = begin_durable_write(preparation.database())?;
        let already_detached = {
            let mut meta = write.open_table(META).map_err(table_error)?;
            let existing = read_holds_meta(&meta)?;
            let mut list = existing.holds().to_vec();
            let hold_id = projection_id.get().to_string();
            let position = list.iter().position(|h| h.hold_id() == hold_id);
            let mut idempotent = false;
            match action {
                RetentionAdministrationAction::ProjectionDetach => {
                    if let Some(index) = position {
                        // Already detached is idempotent; an operator hold
                        // under the same id refuses (kind conflict).
                        if list[index].kind() != RetentionHoldKind::ProjectionDetach {
                            return Err(storage_error(StorageErrorKind::InvariantViolation));
                        }
                        idempotent = true;
                    } else {
                        let hold = RetentionHoldV1::new_projection_detach(projection_id, reason)
                            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                        list.push(hold);
                        list.sort_by(|a, b| a.hold_id().cmp(b.hold_id()));
                    }
                }
                RetentionAdministrationAction::ProjectionReattach => {
                    let Some(index) = position else {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    };
                    if list[index].kind() != RetentionHoldKind::ProjectionDetach {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    }
                    list.remove(index);
                }
            }
            if !idempotent {
                let holds = StoredRetentionHoldsV1::new(list)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let encoded =
                    encode_retention_holds_v1(&holds).map_err(crate::error::codec_error)?;
                meta.insert(META_RETENTION_HOLDS, encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            idempotent
        };
        if already_detached {
            return write.abort().map_err(precommit_storage_error);
        }
        // Audited administration action: allocate the next administration
        // sequence and append the retention record in the SAME transaction.
        {
            let allocator = crate::administration::read_administration_allocator(&write)?;
            let next = allocator
                .allocate_consecutive(1)
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            let sequence = next.assigned()[0];
            let record = StoredRetentionAdministrationV1::new(
                sequence,
                action,
                projection_id,
                reason,
                timestamp,
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            crate::administration::append_audit_record(
                &write,
                &StoredAdministrationAuditRecordV1::Retention(record),
            )?;
            crate::administration::write_administration_allocator(&write, allocator, next.next())?;
        }
        self.before_commit(RedbTestOperation::RetentionHold)?;
        commit_durable(write)?;
        self.after_commit(RedbTestOperation::RetentionHold)
    }

    /// Offline prune of commits/events/outbox/outbox_status up to `target_inclusive`.
    ///
    /// Refuses unless the fencing computation authorizes the exact target and
    /// the existing watermark/tombstone state verifies. Every sub-range
    /// commits atomically {delete rows, append chained tombstone, advance
    /// watermark}; the tombstone chain roots at the registry digest current
    /// at ROOTING, recorded durably in the watermark record (ADR-0085 A2).
    pub fn prune_to(&self, target_inclusive: u64) -> Result<RetentionStatusV1, StorageError> {
        if target_inclusive == 0 {
            return self.status();
        }
        // Opening the ordinary store first is the non-bypassable ADR-0085 A4
        // barrier: it recovers the complete checkpoint-plus-suffix authority,
        // rebases the active extent, and removes only proven scratch state.
        // The preparation value borrows that same exclusive redb handle, so no
        // prune write can exist before the storage-internal witness.
        let store = RedbStore::open(&self.database_path)?;
        let preparation = store.prepare_offline_retention()?;
        let database = preparation.database();

        // Pre-verification: refuse to extend a watermark/chain state that
        // does not verify (fail toward NOT deleting). Also fixes the chain
        // rooting digest for this run: carried from the recorded value, or —
        // first rooting only — the digest current right now.
        let chain_root_digest = {
            let transaction = database.begin_read().map_err(transaction_error)?;
            let watermark = load_watermark(&transaction)?;
            let (current, recorded_root) = match watermark.as_ref() {
                None => (0, None),
                Some(wm) => (wm.watermark_sequence(), wm.chain_root_registry_digest()),
            };
            verify_tombstone_chain(&transaction, current, recorded_root)?;
            recorded_root.unwrap_or_else(current_record_registry_digest)
        };

        // First exclusive transaction: delete validated-prefix checkpoint.
        {
            let write = begin_durable_write(database)?;
            {
                let mut meta = write.open_table(META).map_err(table_error)?;
                let _ = meta
                    .remove(META_VALIDATED_PREFIX_CHECKPOINT)
                    .map_err(precommit_storage_error)?;
            }
            self.before_commit(RedbTestOperation::RetentionPruneCheckpointDelete)?;
            commit_durable(write)?;
            self.after_commit(RedbTestOperation::RetentionPruneCheckpointDelete)?;
        }

        loop {
            let transaction = database.begin_read().map_err(transaction_error)?;
            let current = load_watermark(&transaction)?
                .map(|w| w.watermark_sequence())
                .unwrap_or(0);
            if current >= target_inclusive {
                break;
            }
            let holds = load_holds(&transaction)?;
            let fencing = collect_fencing_inputs(&transaction, &holds)?;
            let (max_perm, _) = compute_max_permissible_watermark(&fencing);
            let Some(max_perm) = max_perm else {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            if target_inclusive > max_perm {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let incarnation = load_history_incarnation(&transaction)?;
            let first = current.saturating_add(1);
            if first > target_inclusive {
                break;
            }
            let last = first
                .saturating_add(RETENTION_PRUNE_SUBRANGE_SEQUENCES.saturating_sub(1))
                .min(target_inclusive)
                .min(max_perm);
            if last < first {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let previous_hash = load_last_tombstone_hash(&transaction)?;
            drop(transaction);

            let mut write = begin_durable_write(database)?;
            let (content_digest, counts) = digest_and_count_range(&write, first, last)?;
            // Pre-delete verification: the fencing head guarantees every
            // sequence in [first, last] committed, so exactly one commit row
            // per sequence must be present. A shortfall is pre-existing
            // corruption — refuse rather than launder it into "verified
            // pruned" (fail toward NOT deleting).
            let range_size = last
                .checked_sub(first)
                .and_then(|span| span.checked_add(1))
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if counts.commits != range_size {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            materialize_retained_command_views(&write, first, last)?;
            rewrite_pruned_command_segments(&mut write, first, last)?;
            delete_event_keyed_range(&mut write, EVENTS, first, last)?;
            delete_event_keyed_range(&mut write, OUTBOX, first, last)?;
            delete_event_keyed_range(&mut write, OUTBOX_STATUS, first, last)?;

            let prev = match previous_hash {
                Some(hash) => Some(hash),
                None if first == 1 => Some(history_tombstone_chain_root(chain_root_digest)),
                None => return Err(storage_error(StorageErrorKind::InvariantViolation)),
            };
            let tombstone = StoredHistoryTombstoneV1::new(
                first,
                last,
                counts.commits,
                counts.events,
                counts.outbox,
                counts.outbox_status,
                content_digest,
                prev,
                incarnation,
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            {
                let mut tombstones = write.open_table(HISTORY_TOMBSTONES).map_err(table_error)?;
                let key = encode_application_sequence_key(
                    CommitSequence::new(first)
                        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
                );
                let encoded =
                    encode_history_tombstone_v1(&tombstone).map_err(crate::error::codec_error)?;
                tombstones
                    .insert(key.as_slice(), encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            {
                let mut meta = write.open_table(META).map_err(table_error)?;
                let watermark =
                    StoredRetentionWatermarkV1::new(last, incarnation, Some(chain_root_digest))
                        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let encoded =
                    encode_retention_watermark_v1(&watermark).map_err(crate::error::codec_error)?;
                meta.insert(META_RETENTION_WATERMARK, encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            self.before_commit(RedbTestOperation::RetentionPruneSubrange)?;
            commit_durable(write)?;
            self.after_commit(RedbTestOperation::RetentionPruneSubrange)?;
        }
        // The RedbStore remains the sole owner for the entire prune. Its
        // preparation borrow ends after the final write above.
        drop(store);
        self.status()
    }

    fn before_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller {
            controller.before_commit(operation)?;
        }
        Ok(())
    }

    fn after_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller {
            controller.after_commit(operation)?;
        }
        Ok(())
    }
}

/// Re-materializes the two application views that ADR-0085 retains beyond the
/// prunable commit body. A live capsule is their sole owner, but once retention
/// removes that capsule the idempotency retry contract and provenance lookup
/// must remain complete rather than becoming dangling locators.
///
/// The replacements happen in the same transaction and before the capsule and
/// event rows are deleted. Any absent, substituted, or non-locator peer fails
/// closed and leaves the complete sub-range unchanged.
fn materialize_retained_command_views(
    write: &WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let commits = write.open_table(COMMITS).map_err(table_error)?;
    let events = write.open_table(EVENTS).map_err(table_error)?;
    let mut idempotency = write.open_table(IDEMPOTENCY).map_err(table_error)?;
    let mut provenance = write.open_table(PROVENANCE).map_err(table_error)?;
    let mut audit = write.open_table(AUDIT).map_err(table_error)?;
    let mut audit_by_request = write.open_table(AUDIT_BY_REQUEST).map_err(table_error)?;
    let mut event_routes = write.open_table(EVENT_ROUTES).map_err(table_error)?;
    let first_key = encode_application_sequence_key(
        CommitSequence::new(first)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    let last_key = encode_application_sequence_key(
        CommitSequence::new(last)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );

    for entry in commits
        .range::<&[u8]>((
            Included(first_key.as_slice()),
            Included(last_key.as_slice()),
        ))
        .map_err(precommit_storage_error)?
    {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = decode_application_sequence_key(physical_key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        match riffdb_storage_api::decode_command_segment_v1(value.value()) {
            Ok(segment) => {
                let segment = segment.into_parts().0;
                if segment.first_commit_sequence() != sequence {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                for command in segment.commands() {
                    let command_sequence = command.commit_sequence();
                    if command_sequence.get() >= first && command_sequence.get() <= last {
                        materialize_retained_capsule_views(
                            &mut idempotency,
                            &mut provenance,
                            &mut audit,
                            &mut audit_by_request,
                            &mut event_routes,
                            command.base(),
                            command.events(),
                        )?;
                    }
                }
                continue;
            }
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
            Err(error) => return Err(crate::error::codec_error(error)),
        }
        let is_capsule = match riffdb_storage_api::decode_command_capsule_v2(value.value()) {
            Ok(_) => true,
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                match riffdb_storage_api::decode_command_capsule_event_references(value.value()) {
                    Ok(_) => true,
                    Err(error)
                        if error.kind()
                            == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                    {
                        false
                    }
                    Err(error) => return Err(crate::error::codec_error(error)),
                }
            }
            Err(error) => return Err(crate::error::codec_error(error)),
        };
        if !is_capsule {
            continue;
        }
        let capsule = codec::decode_command_capsule_with_event_table(value.value(), &events)?
            .into_parts()
            .0;
        if capsule.commit_sequence() != sequence {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        materialize_retained_capsule_views(
            &mut idempotency,
            &mut provenance,
            &mut audit,
            &mut audit_by_request,
            &mut event_routes,
            &capsule,
            &[],
        )?;
    }
    Ok(())
}

fn materialize_retained_capsule_views(
    idempotency: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    provenance: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    audit: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    audit_by_request: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    event_routes: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    capsule: &riffdb_storage_api::StoredCommandCapsuleV1,
    events: &[riffdb_storage_api::StoredDurableEventV1],
) -> Result<(), StorageError> {
    let sequence = capsule.commit_sequence();
    let identity_key = capsule
        .outcome()
        .identity()
        .storage_key()
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let idempotency_key = encode_idempotency_key(&identity_key);
    if let Some(stored_outcome) = idempotency
        .get(idempotency_key)
        .map_err(precommit_storage_error)?
    {
        match riffdb_storage_api::decode_command_locator_v1(stored_outcome.value()) {
            Ok(locator) if locator.value().commit_sequence() == sequence => {}
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                let outcome = riffdb_storage_api::decode_stored_outcome_v1(stored_outcome.value());
                if !matches!(outcome, Ok(ref outcome) if outcome.value() == capsule.outcome()) {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
            Ok(_) | Err(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
        }
    }
    let encoded_outcome = codec::encode_stored_outcome_v1(capsule.outcome())?;
    let _ = idempotency
        .insert(idempotency_key, encoded_outcome.as_bytes())
        .map_err(precommit_storage_error)?;

    let provenance_key = encode_provenance_key(capsule.provenance().provenance_id());
    if let Some(stored_provenance) = provenance
        .get(provenance_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        match riffdb_storage_api::decode_command_locator_v1(stored_provenance.value()) {
            Ok(locator) if locator.value().commit_sequence() == sequence => {}
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                let retained =
                    riffdb_storage_api::decode_provenance_record_v1(stored_provenance.value())
                        .map_err(crate::error::codec_error)?;
                if retained.value() != capsule.provenance() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
            Ok(_) | Err(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
        }
    }
    let encoded_provenance = codec::encode_provenance_record_v1(capsule.provenance())?;
    let _ = provenance
        .insert(provenance_key.as_slice(), encoded_provenance.as_bytes())
        .map_err(precommit_storage_error)?;

    for (member, record) in [
        (
            riffdb_storage_api::StoredCommandAuditMemberV1::Started,
            capsule.started_audit(),
        ),
        (
            riffdb_storage_api::StoredCommandAuditMemberV1::Terminal,
            capsule.terminal_audit(),
        ),
    ] {
        let audit_key = encode_audit_key(record.administration_sequence());
        if let Some(stored_audit) = audit
            .get(audit_key.as_slice())
            .map_err(precommit_storage_error)?
        {
            match riffdb_storage_api::decode_command_audit_locator_v1(stored_audit.value()) {
                Ok(locator)
                    if locator.value().commit_sequence() == sequence
                        && locator.value().member() == member => {}
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let retained =
                        riffdb_storage_api::decode_service_audit_record(stored_audit.value())
                            .map_err(crate::error::codec_error)?;
                    if retained.value() != record {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
                Ok(_) | Err(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
            }
        }
        let encoded_audit = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(record.clone()),
        )?;
        let _ = audit
            .insert(audit_key.as_slice(), encoded_audit.as_bytes())
            .map_err(precommit_storage_error)?;
        let request_key =
            encode_audit_by_request_key(record.request_id(), record.administration_sequence());
        let request_row = riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(
            record.request_id(),
            record.administration_sequence(),
        );
        let encoded_request = codec::encode_service_audit_request_index_v1(request_row)?;
        if let Some(prior) = audit_by_request
            .insert(request_key.as_slice(), encoded_request.as_bytes())
            .map_err(precommit_storage_error)?
            && prior.value() != encoded_request.as_bytes()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    for event in events {
        let route = riffdb_storage_api::StoredEventRouteV1::new(
            event.event_id(),
            event.event_type_id(),
            event.event_hash(),
        );
        let key = encode_event_route_key(capsule.commit().partition_hash(), event.event_id());
        let encoded = codec::encode_event_route_v1(route)?;
        if let Some(prior) = event_routes
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?
            && prior.value() != encoded.as_bytes()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct RangeCounts {
    commits: u64,
    events: u64,
    outbox: u64,
    outbox_status: u64,
}

fn begin_durable_write(database: &Database) -> Result<WriteTransaction, StorageError> {
    let mut write = database.begin_write().map_err(transaction_error)?;
    write.set_two_phase_commit(true);
    write
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    Ok(write)
}

fn commit_durable(write: WriteTransaction) -> Result<(), StorageError> {
    write
        .commit()
        .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))
}

pub(crate) fn load_watermark(
    transaction: &redb::ReadTransaction,
) -> Result<Option<StoredRetentionWatermarkV1>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_RETENTION_WATERMARK)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let item = decode_retention_watermark_v1(encoded.value()).map_err(crate::error::codec_error)?;
    Ok(Some(item.into_parts().0))
}

pub(crate) fn load_holds(
    transaction: &redb::ReadTransaction,
) -> Result<StoredRetentionHoldsV1, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    read_holds_meta(&meta)
}

fn read_holds_meta(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<StoredRetentionHoldsV1, StorageError> {
    let Some(encoded) = meta
        .get(META_RETENTION_HOLDS)
        .map_err(precommit_storage_error)?
    else {
        return Ok(StoredRetentionHoldsV1::empty());
    };
    let item = decode_retention_holds_v1(encoded.value()).map_err(crate::error::codec_error)?;
    Ok(item.into_parts().0)
}

fn load_history_incarnation(transaction: &redb::ReadTransaction) -> Result<u64, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let encoded = meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let incarnation = *codec::decode_history_incarnation_v1(encoded.value())?.value();
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(incarnation)
}

fn load_last_tombstone_hash(
    transaction: &redb::ReadTransaction,
) -> Result<Option<HistoryTombstoneHash>, StorageError> {
    let table = transaction
        .open_table(HISTORY_TOMBSTONES)
        .map_err(table_error)?;
    let Some((_, value)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let item = decode_history_tombstone_v1(value.value()).map_err(crate::error::codec_error)?;
    Ok(Some(item.into_parts().0.tombstone_hash()))
}

/// Collect fencing inputs. Any unreadable fencing source refuses (error).
pub(crate) fn collect_fencing_inputs(
    transaction: &redb::ReadTransaction,
    holds: &StoredRetentionHoldsV1,
) -> Result<RetentionFencingInputs, StorageError> {
    let detached = detached_projection_ids(holds);
    let min_projection = min_projection_durable_frontier(transaction, &detached)?;
    let min_hold = holds
        .holds()
        .iter()
        .filter(|h| h.kind() == RetentionHoldKind::Operator)
        .map(RetentionHoldV1::sequence)
        .min();
    let undelivered = undelivered_outbox_low_water(transaction)?;
    let staged = staged_migration_frozen_frontier(transaction)?;
    let head = durable_application_head(transaction)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let database_id = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let database_id = *codec::decode_database_identity_v1(database_id.value())?.value();
    drop(meta);
    let consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    let consumer_low_water = retention_low_water_from_table(&consumers, database_id)?;
    Ok(RetentionFencingInputs {
        durable_application_head: Some(head),
        min_projection_durable_frontier: min_projection,
        min_operator_hold_sequence: min_hold,
        undelivered_outbox_low_water: undelivered,
        staged_migration_frozen_frontier: staged,
        consumer_low_water,
    })
}

fn detached_projection_ids(holds: &StoredRetentionHoldsV1) -> BTreeSet<String> {
    holds
        .holds()
        .iter()
        .filter(|h| h.kind() == RetentionHoldKind::ProjectionDetach)
        .map(|h| h.hold_id().to_owned())
        .collect()
}

/// Last committed application sequence from the durable allocator
/// (`Next(n)` ⇒ `n - 1`, `Exhausted` ⇒ `u64::MAX`, absent row ⇒ 0).
///
/// The allocator is authoritative even when the commits table is empty:
/// after a full prune the rows are gone but the head is not, and the
/// watermark must never pass it (sequences committed later would otherwise
/// be born below the watermark).
pub(crate) fn durable_application_head(
    transaction: &redb::ReadTransaction,
) -> Result<u64, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_APPLICATION_SEQUENCE)
        .map_err(precommit_storage_error)?
    else {
        return Ok(0);
    };
    let allocator = codec::decode_application_sequence_allocator_v1(encoded.value())?
        .into_parts()
        .0;
    Ok(match allocator {
        riffdb_storage_api::ApplicationSequenceAllocator::Next(next) => {
            next.get().saturating_sub(1)
        }
        riffdb_storage_api::ApplicationSequenceAllocator::Exhausted => u64::MAX,
    })
}

pub(crate) fn min_projection_durable_frontier(
    transaction: &redb::ReadTransaction,
    detached: &BTreeSet<String>,
) -> Result<Option<u64>, StorageError> {
    let controls = transaction
        .open_table(PROJECTION_FRONTIER)
        .map_err(table_error)?;
    let mut min_frontier: Option<u64> = None;
    let mut any = false;
    for entry in controls.iter().map_err(precommit_storage_error)? {
        let (_, encoded) = entry.map_err(precommit_storage_error)?;
        let control = decode_projection_control_v1(encoded.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .into_parts()
            .0;
        let id_key = control.identity().projection_id().get().to_string();
        if detached.contains(&id_key) {
            continue;
        }
        any = true;
        let frontier = control
            .published()
            .map(|p| match p.frontier() {
                FrontierPosition::BeforeFirst => 0,
                FrontierPosition::AppliedThrough(seq) => seq.get(),
            })
            .unwrap_or(0);
        min_frontier = Some(match min_frontier {
            None => frontier,
            Some(existing) => existing.min(frontier),
        });
    }
    let columnar_controls = transaction
        .open_table(crate::layout::COLUMNAR_PROJECTION_CONTROLS)
        .map_err(table_error)?;
    let mut columnar_count = 0_usize;
    for entry in columnar_controls.iter().map_err(precommit_storage_error)? {
        columnar_count = columnar_count.saturating_add(1);
        if columnar_count > 256 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let (key, encoded) = entry.map_err(precommit_storage_error)?;
        let source = crate::keys::decode_columnar_projection_control_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let control = decode_columnar_projection_control_v1(encoded.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .into_parts()
            .0;
        if control.source() != &source {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(frontier) = control.retention_frontier() {
            any = true;
            let frontier = match frontier {
                FrontierPosition::BeforeFirst => 0,
                FrontierPosition::AppliedThrough(sequence) => sequence.get(),
            };
            min_frontier = Some(min_frontier.map_or(frontier, |existing| existing.min(frontier)));
        }
    }
    Ok(if any { min_frontier } else { None })
}

/// Lowest commit sequence carrying an undelivered outbox intent, minus one.
///
/// Canonical undelivered semantics (matching the recovery scan in
/// `derived.rs::scan_undelivered_outbox_statuses`): iterate OUTBOX intents;
/// an intent with an ABSENT status row is undelivered (the initial state), and
/// only a present `Delivered` status counts as delivered — pending and
/// dead-letter both fence. Scanning statuses alone is fail-open: never-claimed
/// intents have no status row and would be invisible to the fence.
fn undelivered_outbox_low_water(
    transaction: &redb::ReadTransaction,
) -> Result<Option<u64>, StorageError> {
    let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
    let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut event_ids = BTreeSet::new();
    for entry in commits.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        match riffdb_storage_api::decode_command_segment_v1(value.value()) {
            Ok(segment) => {
                for command in segment.value().commands() {
                    for event in command.events() {
                        if !event_ids.insert(event.event_id()) {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                    }
                }
            }
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
            Err(error) => return Err(crate::error::codec_error(error)),
        }
    }
    for entry in intents.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        event_ids.insert(event_id);
    }
    for event_id in event_ids {
        let key = event_id.to_be_bytes();
        let delivered = match statuses
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
        {
            None => false,
            Some(value) => {
                let status: StoredOutboxStatusV1 = codec::decode_outbox_status_v1(value.value())
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                    .into_parts()
                    .0;
                matches!(
                    status.state(),
                    riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
                )
            }
        };
        if !delivered {
            // Intents iterate in ascending (sequence, index) order, so the
            // first undelivered intent carries the minimum sequence.
            return Ok(Some(event_id.commit_sequence().get().saturating_sub(1)));
        }
    }
    Ok(None)
}

fn staged_migration_frozen_frontier(
    transaction: &redb::ReadTransaction,
) -> Result<Option<u64>, StorageError> {
    let journal = transaction
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .map_err(table_error)?;
    let mut min_frontier: Option<u64> = None;
    for entry in journal.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let item =
            riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(value.value())
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let row = item.into_parts().0;
        if let Some(frontier) = row.frozen_application_frontier() {
            let seq = frontier.get();
            min_frontier = Some(match min_frontier {
                None => seq,
                Some(existing) => existing.min(seq),
            });
        }
    }
    Ok(min_frontier)
}

fn digest_and_count_range(
    write: &WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(HistoryTombstoneContentDigest, RangeCounts), StorageError> {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(b"riffdb.history-tombstone-content/v1\0");
    preimage.extend_from_slice(&first.to_be_bytes());
    preimage.extend_from_slice(&last.to_be_bytes());
    let mut counts = RangeCounts::default();
    let mut segment_event_ids = BTreeSet::new();

    {
        let commits = write.open_table(COMMITS).map_err(table_error)?;
        for entry in commits.iter().map_err(precommit_storage_error)? {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let physical_sequence = decode_application_sequence_key(key.value())
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            match riffdb_storage_api::decode_command_segment_v1(value.value()) {
                Ok(segment) => {
                    let segment = segment.into_parts().0;
                    if segment.first_commit_sequence() != physical_sequence {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    for command in segment.commands() {
                        let sequence = command.commit_sequence().get();
                        if sequence < first || sequence > last {
                            continue;
                        }
                        let encoded = riffdb_storage_api::encode_command_capsule_v2(command)
                            .map_err(crate::error::codec_error)?;
                        append_tombstone_member(
                            &mut preimage,
                            b"commits",
                            &command.commit_sequence().to_be_bytes(),
                            encoded.as_bytes(),
                        )?;
                        counts.commits = counts
                            .commits
                            .checked_add(1)
                            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                        for event in command.events() {
                            if !segment_event_ids.insert(event.event_id()) {
                                return Err(storage_error(StorageErrorKind::CorruptData));
                            }
                            let event_bytes = riffdb_storage_api::encode_durable_event_v1(event)
                                .map_err(crate::error::codec_error)?;
                            append_tombstone_member(
                                &mut preimage,
                                b"events",
                                &event.event_id().to_be_bytes(),
                                event_bytes.as_bytes(),
                            )?;
                            let intent =
                                riffdb_storage_api::StoredOutboxIntentV1::new(event.clone());
                            let outbox_bytes = riffdb_storage_api::encode_outbox_intent_v1(&intent)
                                .map_err(crate::error::codec_error)?;
                            append_tombstone_member(
                                &mut preimage,
                                b"outbox",
                                &event.event_id().to_be_bytes(),
                                outbox_bytes.as_bytes(),
                            )?;
                            counts.events = counts
                                .events
                                .checked_add(1)
                                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                            counts.outbox = counts
                                .outbox
                                .checked_add(1)
                                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                        }
                    }
                }
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let sequence = physical_sequence.get();
                    if sequence < first || sequence > last {
                        continue;
                    }
                    append_tombstone_member(&mut preimage, b"commits", key.value(), value.value())?;
                    counts.commits = counts
                        .commits
                        .checked_add(1)
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                }
                Err(error) => return Err(crate::error::codec_error(error)),
            }
        }
    }

    for (label, definition, counter) in [
        ("events", EVENTS, &mut counts.events),
        ("outbox", OUTBOX, &mut counts.outbox),
        ("outbox_status", OUTBOX_STATUS, &mut counts.outbox_status),
    ] {
        let table = write.open_table(definition).map_err(table_error)?;
        let (lower, upper) = event_keyed_range_bounds(first, last);
        for entry in table
            .range::<&[u8]>((Included(lower.as_slice()), Included(upper.as_slice())))
            .map_err(precommit_storage_error)?
        {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let event_id = decode_event_key(key.value())
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if label != "outbox_status" && segment_event_ids.contains(&event_id) {
                continue;
            }
            append_tombstone_member(&mut preimage, label.as_bytes(), key.value(), value.value())?;
            *counter = counter
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }

    let digest = hash(HashDomain::Schema, &preimage);
    Ok((
        HistoryTombstoneContentDigest::from_bytes(*digest.as_bytes()),
        counts,
    ))
}

fn append_tombstone_member(
    preimage: &mut Vec<u8>,
    label: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<(), StorageError> {
    preimage.extend_from_slice(label);
    preimage.extend_from_slice(b"\0");
    preimage.extend_from_slice(key);
    preimage.extend_from_slice(
        &u64::try_from(value.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?
            .to_be_bytes(),
    );
    preimage.extend_from_slice(value);
    Ok(())
}

fn rewrite_pruned_command_segments(
    write: &mut WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let mut table = write.open_table(COMMITS).map_err(table_error)?;
    let mut deletes = Vec::new();
    let mut replacements = Vec::new();
    let mut rewritten_predecessor = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical_sequence = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        match riffdb_storage_api::decode_command_segment_v1(value.value()) {
            Ok(segment) => {
                let segment = segment.into_parts().0;
                if segment.first_commit_sequence() != physical_sequence {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                if segment.last_commit_sequence().get() < first {
                    continue;
                }
                if segment.last_commit_sequence().get() <= last {
                    deletes.push(key.value().to_vec());
                    continue;
                }
                let commands = if segment.first_commit_sequence().get() <= last {
                    segment
                        .commands()
                        .iter()
                        .filter(|command| command.commit_sequence().get() > last)
                        .cloned()
                        .collect::<Vec<_>>()
                } else if rewritten_predecessor.is_some() {
                    segment.commands().to_vec()
                } else {
                    continue;
                };
                let predecessor =
                    rewritten_predecessor.or_else(|| segment.predecessor_segment_digest());
                let first_retained = commands
                    .first()
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                    .commit_sequence();
                let manifest =
                    crate::application::build_command_segment_manifest(&commands, first_retained)?;
                let draft = riffdb_storage_api::StoredCommandSegmentV1::new(
                    segment.database_id(),
                    segment.history_incarnation(),
                    predecessor,
                    commands.clone(),
                    manifest.clone(),
                    riffdb_storage_api::CommandSegmentDigestV1::from_bytes([0; 32]),
                )
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let digest = riffdb_storage_api::command_segment_digest_v1(&draft);
                let rewritten = riffdb_storage_api::StoredCommandSegmentV1::new(
                    segment.database_id(),
                    segment.history_incarnation(),
                    predecessor,
                    commands,
                    manifest,
                    digest,
                )
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let encoded = riffdb_storage_api::encode_command_segment_v1(&rewritten)
                    .map_err(crate::error::codec_error)?;
                deletes.push(key.value().to_vec());
                replacements.push((
                    encode_application_sequence_key(first_retained).to_vec(),
                    encoded.into_bytes(),
                ));
                rewritten_predecessor = Some(digest);
            }
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                if physical_sequence.get() >= first && physical_sequence.get() <= last {
                    deletes.push(key.value().to_vec());
                }
            }
            Err(error) => return Err(crate::error::codec_error(error)),
        }
    }
    for key in deletes {
        let _ = table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    for (key, value) in replacements {
        if table
            .insert(key.as_slice(), value.as_slice())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

/// Exact inclusive bounds over 12-byte event keys `(sequence: u64, index: u32)`
/// for `[first, last]`. A 16-byte bound would sort every `(first, 0)` key BELOW
/// the lower bound (shorter keys with equal prefix sort first), silently
/// excluding index-0 rows from digest, count, and deletion.
fn event_keyed_range_bounds(first: u64, last: u64) -> ([u8; 12], [u8; 12]) {
    let mut lower = [0u8; 12];
    lower[..8].copy_from_slice(&first.to_be_bytes());
    let mut upper = [0xffu8; 12];
    upper[..8].copy_from_slice(&last.to_be_bytes());
    (lower, upper)
}

fn delete_event_keyed_range(
    write: &mut WriteTransaction,
    definition: TableDefinition<'_, &[u8], &[u8]>,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let mut table = write.open_table(definition).map_err(table_error)?;
    let (lower, upper) = event_keyed_range_bounds(first, last);
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>((Included(lower.as_slice()), Included(upper.as_slice())))
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        keys.push(key.value().to_vec());
    }
    for key in keys {
        let _ = table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}

/// Verifies the tombstone chain covers `[1, watermark]` contiguously.
///
/// `chain_root_registry_digest` is the digest RECORDED in the watermark
/// record at rooting (ADR-0085 A2) — never the process's current digest.
/// Verifies: hash chain from the recorded root, contiguity, abutment at the
/// watermark, and count arithmetic (each range pruned exactly one commit per
/// sequence, so per-tombstone `commits_count == last - first + 1` and the
/// chain total equals the watermark).
pub(crate) fn verify_tombstone_chain(
    transaction: &redb::ReadTransaction,
    watermark: u64,
    chain_root_registry_digest: Option<SchemaHash>,
) -> Result<(), StorageError> {
    let table = transaction
        .open_table(HISTORY_TOMBSTONES)
        .map_err(table_error)?;
    if watermark == 0 {
        if table.len().map_err(precommit_storage_error)? != 0 {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(());
    }
    // A pruned watermark without a recorded rooting digest cannot verify.
    let Some(root_digest) = chain_root_registry_digest else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    let mut expected_previous = Some(history_tombstone_chain_root(root_digest));
    let mut expected_first = 1u64;
    let mut last_seen = 0u64;
    let mut total_commits = 0u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let first_key = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let tombstone = decode_history_tombstone_v1(value.value())
            .map_err(crate::error::codec_error)?
            .into_parts()
            .0;
        if tombstone.first_sequence() != first_key.get()
            || tombstone.first_sequence() != expected_first
            || tombstone.previous_tombstone_hash() != expected_previous
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let computed = tombstone
            .computed_hash()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if computed != tombstone.tombstone_hash() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let range_size = tombstone
            .last_sequence()
            .checked_sub(tombstone.first_sequence())
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if tombstone.commits_count() != range_size {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        total_commits = total_commits
            .checked_add(range_size)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        expected_previous = Some(tombstone.tombstone_hash());
        expected_first = tombstone
            .last_sequence()
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        last_seen = tombstone.last_sequence();
    }
    if last_seen != watermark || total_commits != watermark {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

/// Returns true when `sequence` is covered by the watermark (below-or-equal).
#[must_use]
pub(crate) const fn sequence_covered_by_watermark(sequence: u64, watermark: u64) -> bool {
    sequence > 0 && sequence <= watermark
}

#[cfg(test)]
mod tests {
    use super::sequence_covered_by_watermark;

    #[test]
    fn coverage_predicate_is_exact_below_or_equal_watermark() {
        // The single arbiter of "absence is tombstone-covered". Accepting
        // uncovered absence here would launder corruption into pruned truth.
        assert!(sequence_covered_by_watermark(1, 1));
        assert!(sequence_covered_by_watermark(1, 2));
        assert!(!sequence_covered_by_watermark(2, 1));
        assert!(!sequence_covered_by_watermark(1, 0));
        assert!(!sequence_covered_by_watermark(0, 5));
        assert!(!sequence_covered_by_watermark(0, 0));
    }
}
