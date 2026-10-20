//! Closed off-gate exact physical inspection, followed by Source-version CAS.
//! Hashes, canonical checks and engine reads precede the final Source lease.
use super::*;
use crate::{
    error::{precommit_storage_error, storage_error, table_error, transaction_error},
    layout::META,
    store::{OriginalSourceReadContext, source_usage_domain::SourceUsageMutation},
};
use redb::{ReadableDatabase, TableDefinition};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, MAX_PROJECTION_SOURCE_CONTROL_BYTES,
    MAX_PROJECTION_SOURCE_STEP_BYTES, StorageError, StorageErrorKind,
};

// Constructible only by the actual guarded physical inspection below. Retains
// both real mutation custody and its actual admitted intent through final CAS.
pub(crate) struct NativeSourceImageObservation<'engine> {
    actual: &'engine RedbOperationalPorts,
    mutation: &'engine SourceUsageMutation<'engine>,
    intent: Option<NativeSourceStepIntent>,
    image: VerifiedSourceImage,
}
impl<'engine> NativeSourceImageObservation<'engine> {
    pub(crate) fn into_verified(
        mut self,
        lease: &'engine ExclusiveLease,
    ) -> Result<NativeVerifiedSourceStep<'engine>, StorageError> {
        // Final short Source turn only: ordinary application/head advances do
        // not invalidate an unchanged Source image and immutable own receipt.
        self.mutation
            .verify_for_native_readback(self.actual, lease)?;
        Ok(NativeVerifiedSourceStep {
            actual: self.actual,
            lease,
            mutation: self.mutation,
            intent: self
                .intent
                .take()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            image: match self.image {
                VerifiedSourceImage::ExactBefore => VerifiedSourceImage::ExactBefore,
                VerifiedSourceImage::ExactAfter => VerifiedSourceImage::ExactAfter,
            },
        })
    }
}

impl Drop for NativeSourceImageObservation<'_> {
    fn drop(&mut self) {}
}

pub(crate) fn inspect_physical_source_image<'engine>(
    actual: &'engine RedbOperationalPorts,
    mutation: &'engine SourceUsageMutation<'engine>,
    intent: &NativeSourceStepIntent,
) -> Result<NativeSourceImageObservation<'engine>, StorageError> {
    // Unknown commit read-back remains permitted while writes are fenced. This
    // observation does not remove that fence or authorize another physical write.
    mutation.verify_for_native_observation(actual)?;
    if !intent.belongs_to(actual.shared.database.source_working_budget()) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let context = OriginalSourceReadContext::admit(actual)?;
    let transaction = actual
        .shared
        .database
        .begin_read()
        .map_err(transaction_error)?;
    let history =
        crate::changelog_v3_roots::read_checkpoint_roots_source_v1(&transaction, &context)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if history.lineage() != intent.predecessor.lineage()
        || history.lineage().catalog_digest()
            != riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
        || intent.receipt.attribution()
            != riffdb_storage_api::ChangelogAttributionV3::ProjectionSourceMaintenance
        || intent.locator.database_id() != history.lineage().database_id()
        || intent.locator.allocation_history_incarnation()
            != history.lineage().history_incarnation()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let expected_after = intent
        .predecessor
        .advance_canonical_v1(&intent.canonical_receipt)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    // The original logical receipt hold protects this assigned receipt until
    // definite reconciliation, including archive-stub replacement of its bytes.
    let receipts = transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .map_err(table_error)?;
    let assigned_key = intent.receipt.binding().sequence.get().to_be_bytes();
    let receipt = receipts
        .get(assigned_key.as_slice())
        .map_err(precommit_storage_error)?;
    let receipt_absent = receipt.is_none();
    let exact_receipt = match receipt.as_ref() {
        None => false,
        Some(row) if row.value() == intent.canonical_receipt.as_slice() => true,
        Some(row) if crate::history_stub::HistoryStub::decode(row.value())?.is_some() => {
            context.validate_terminal(row.value(), expected_after)?;
            true
        }
        Some(_) => false,
    };
    // Physical bytes alone cannot complete logical publication. In particular,
    // an old composite base must never turn a committed physical after-image
    // into a rollback proof or release its retained original inputs.
    let logical = actual.begin_composite_read()?;
    let logical_history = crate::changelog_v3_cursor::capture_history_position_v1(&logical)?;
    if logical_history.lineage() != history.lineage() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let logical_includes_assigned =
        logical_history.tail().sequence() >= intent.receipt.binding().sequence;
    let mut controls = 0_u16;
    let mut changed_source = false;
    let mut before = true;
    let mut after = true;
    let mut source_rows = 0_u16;
    let mut physical_row_bytes = 0_u64;
    let mut logical_row_bytes = 0_u64;
    for change in intent.receipt.mutations() {
        if change.key().is_empty() || change.key().len() > riffdb_types::MAX_KEY_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let is_row = change.namespace() == N::ProjectionSourceRows;
        let maximum = match change.namespace() {
            N::ProjectionSourceRows => {
                source_rows = source_rows
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if source_rows > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                MAX_PROJECTION_SOURCE_STEP_BYTES as usize
            }
            N::ProjectionSourceObjects
            | N::ProjectionSourceRoots
            | N::ProjectionSourceMaintenance
            | N::ProjectionSourceAllocator => {
                controls = controls
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                // One fixed step: old/new object+root, progress and allocator.
                if controls > 6 {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                MAX_PROJECTION_SOURCE_CONTROL_BYTES
            }
            _ => return Err(storage_error(StorageErrorKind::CorruptData)),
        };
        let compare =
            |stored: Option<&[u8]>, consumed: u64| -> Result<(bool, bool, u64), StorageError> {
                if stored.is_some_and(|value| value.len() > maximum) {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                let bytes = if is_row {
                    change
                        .key()
                        .len()
                        .checked_add(stored.map_or(0, <[u8]>::len))
                        .and_then(|bytes| u64::try_from(bytes).ok())
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?
                } else {
                    0
                };
                if consumed
                    .checked_add(bytes)
                    .is_none_or(|total| total > MAX_PROJECTION_SOURCE_STEP_BYTES)
                {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                Ok((
                    change.matches_prior(stored),
                    change.value() == stored,
                    bytes,
                ))
            };
        let matched = if let Some(key) = change.namespace().metadata_key() {
            let table = transaction.open_table(META).map_err(table_error)?;
            let value = table.get(key).map_err(precommit_storage_error)?;
            compare(
                value.as_ref().map(|value| value.value()),
                physical_row_bytes,
            )?
        } else {
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(change.namespace().table());
            let table = transaction.open_table(definition).map_err(table_error)?;
            let value = table.get(change.key()).map_err(precommit_storage_error)?;
            compare(
                value.as_ref().map(|value| value.value()),
                physical_row_bytes,
            )?
        };
        let logical_matched = if let Some(key) = change.namespace().metadata_key() {
            // Source allocator has no journal-overlay producer: the closed
            // Source inventory binds it to this exact composite checkpoint.
            let table = logical.open_table(META).map_err(table_error)?;
            let value = table.get(key).map_err(precommit_storage_error)?;
            compare(value.as_ref().map(|value| value.value()), logical_row_bytes)?
        } else {
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(change.namespace().table());
            let table = logical.open_table(definition).map_err(table_error)?;
            let value = table.get(change.key()).map_err(precommit_storage_error)?;
            compare(value.as_ref().map(|value| value.value()), logical_row_bytes)?
        };
        physical_row_bytes = physical_row_bytes
            .checked_add(matched.2)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        logical_row_bytes = logical_row_bytes
            .checked_add(logical_matched.2)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if physical_row_bytes > MAX_PROJECTION_SOURCE_STEP_BYTES
            || logical_row_bytes > MAX_PROJECTION_SOURCE_STEP_BYTES
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        changed_source = true;
        before &= matched.0 && logical_matched.0;
        after &= matched.1 && logical_matched.1;
    }
    if !changed_source {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let image = match (
        before && receipt_absent,
        after && exact_receipt && logical_includes_assigned,
    ) {
        (true, false) => VerifiedSourceImage::ExactBefore,
        (false, true) => VerifiedSourceImage::ExactAfter,
        // Mixed, ambiguous, substituted or unrecognizable outcomes retain their
        // numeric reservation and semantic input obligation. Never guess rollback.
        _ => return Err(storage_error(StorageErrorKind::Unavailable)),
    };
    drop(logical);
    drop(receipt);
    drop(receipts);
    drop(transaction);
    drop(context);
    mutation.verify_for_native_observation(actual)?;
    Ok(NativeSourceImageObservation {
        actual,
        mutation,
        intent: Some(intent.clone()),
        image,
    })
}
