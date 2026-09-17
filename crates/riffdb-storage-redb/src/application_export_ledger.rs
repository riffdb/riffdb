//! Atomic compact export heads and append-only page evidence (ADR-0232).

mod replication;
pub(crate) use replication::validate_received;

use redb::{ReadTransaction, ReadableTable};
use riffdb_storage_api::{
    ApplicationExportLedgerRepository, ApplicationExportOperationRepository,
    ApplicationExportOperationWriteResultV1 as WriteResult, ApplicationExportPageCommitmentV1,
    ChangelogAttributionV3, MAX_ACTIVE_APPLICATION_EXPORTS, StorageError, StorageErrorKind,
    StoredApplicationExportOperation as Head, StoredApplicationExportOperationV2,
    decode_application_export_head, decode_application_export_page_commitment_v1,
    encode_application_export_head, encode_application_export_page_commitment_v1,
};
use riffdb_types::{ApplicationExportOperationId, ApplicationExportPageHash};

use crate::{
    error::{codec_error, precommit_storage_error, storage_error, table_error},
    hooks::RedbTestOperation,
    layout::{
        APPLICATION_EXPORT_OPERATIONS as HEADS, APPLICATION_EXPORT_PAGE_COMMITMENTS as PAGES,
    },
    store::RedbOperationalPorts,
};

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn invalid() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn decode_head(key: &[u8], value: &[u8]) -> Result<Head, StorageError> {
    let head = decode_application_export_head(value).map_err(codec_error)?;
    if key != head.operation_id().as_bytes() {
        return Err(corrupt());
    }
    Ok(head)
}

fn operation_end(operation: ApplicationExportOperationId) -> Result<[u8; 16], StorageError> {
    let mut end = operation.into_bytes();
    for byte in end.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Ok(end);
        }
        *byte = 0;
    }
    // A validated UUID carries fixed version/variant bits, so it cannot be all FF.
    Err(invalid())
}

fn verify_prefix(
    head: &StoredApplicationExportOperationV2,
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<Vec<ApplicationExportPageHash>, StorageError> {
    let operation = head.operation_id();
    let end = operation_end(operation)?;
    let mut entries = Vec::with_capacity(usize::from(head.prefix().pages()));
    let mut charge = 0_usize;
    for row in table
        .range(operation.as_bytes().as_slice()..end.as_slice())
        .map_err(precommit_storage_error)?
    {
        if entries.len() == usize::from(head.prefix().pages()) {
            return Err(corrupt());
        }
        let (key, value) = row.map_err(precommit_storage_error)?;
        let entry = decode_application_export_page_commitment_v1(value.value())
            .map_err(codec_error)?
            .into_parts()
            .0;
        entry.validate_key(key.value()).map_err(|_| corrupt())?;
        let bytes = key
            .value()
            .len()
            .checked_add(value.value().len())
            .ok_or_else(corrupt)?;
        charge = charge.checked_add(bytes).ok_or_else(corrupt)?;
        if charge > head.prefix().retained_bytes() {
            return Err(corrupt());
        }
        entries.push((entry, bytes));
    }
    head.prefix()
        .verify(head.genesis().map_err(|_| corrupt())?, entries)
        .map_err(|_| corrupt())
}

fn require_empty(
    operation: ApplicationExportOperationId,
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let end = operation_end(operation)?;
    if table
        .range(operation.as_bytes().as_slice()..end.as_slice())
        .map_err(precommit_storage_error)?
        .next()
        .transpose()
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    Ok(())
}

/// Full head-to-ledger proof under startup's one immutable root. Each head is
/// verified once; the later ledger walk checks orphans without rehashing prefixes.
pub(crate) fn inspect_head(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<(), StorageError> {
    let head = decode_head(key, value)?;
    let pages = transaction.open_table(PAGES).map_err(table_error)?;
    match head {
        Head::Legacy(head) => require_empty(head.operation_id(), &pages),
        Head::Compact(head) => {
            head.prefix()
                .check_budget(key.len().checked_add(value.len()).ok_or_else(corrupt)?, 0)
                .map_err(|_| corrupt())?;
            verify_prefix(&head, &pages).map(|_| ())
        }
    }
}

/// Reciprocal orphan check. Startup already validates every head and its whole
/// prefix; it need not decode a potentially large terminal body for every page.
pub(crate) fn inspect_page(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<(), StorageError> {
    let entry = decode_application_export_page_commitment_v1(value)
        .map_err(codec_error)?
        .into_parts()
        .0;
    entry.validate_key(key).map_err(|_| corrupt())?;
    let heads = transaction.open_table(HEADS).map_err(table_error)?;
    if heads
        .get(entry.operation().as_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .is_none()
    {
        return Err(corrupt());
    }
    Ok(())
}

impl ApplicationExportLedgerRepository for RedbOperationalPorts {
    fn read_application_export_head(
        &self,
        operation: ApplicationExportOperationId,
    ) -> Result<Option<Head>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction.open_table(HEADS).map_err(table_error)?;
        table
            .get(operation.as_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_head(operation.as_bytes(), value.value()))
            .transpose()
    }

    fn compare_and_swap_application_export_head(
        &mut self,
        expected: Option<&Head>,
        replacement: &Head,
        append: Option<&ApplicationExportPageCommitmentV1>,
        terminal_reserve: usize,
    ) -> Result<WriteResult, StorageError> {
        if let Head::Legacy(replacement) = replacement {
            let expected = match expected {
                Some(Head::Legacy(expected)) => Some(expected),
                None => None,
                Some(Head::Compact(_)) => return Err(invalid()),
            };
            if append.is_some() || terminal_reserve != 0 {
                return Err(invalid());
            }
            return self.compare_and_swap_application_export_operation(expected, replacement);
        }
        let Head::Compact(compact) = replacement else {
            return Err(invalid());
        };
        let prior = match expected {
            Some(Head::Compact(expected)) => Some(expected),
            None => None,
            Some(Head::Legacy(_)) => return Err(invalid()),
        };
        let encoded = encode_application_export_head(replacement).map_err(codec_error)?;
        let page = append
            .map(|entry| {
                encode_application_export_page_commitment_v1(entry)
                    .map(|value| (entry.canonical_key(), value))
                    .map_err(codec_error)
            })
            .transpose()?;
        let page_charge = page
            .as_ref()
            .map(|(key, value)| key.len() + value.as_bytes().len());
        compact
            .validate_transition(
                prior,
                append.zip(page_charge),
                compact.operation_id().as_bytes().len() + encoded.as_bytes().len(),
                terminal_reserve,
            )
            .map_err(|error| match error {
                riffdb_storage_api::StorageValueError::LimitExceeded
                | riffdb_storage_api::StorageValueError::SizeOverflow => {
                    storage_error(StorageErrorKind::LimitExceeded)
                }
                _ => invalid(),
            })?;

        let access =
            self.begin_attributed_write(ChangelogAttributionV3::ApplicationExportOperation)?;
        let mut heads = access
            .transaction()?
            .open_table(HEADS)
            .map_err(table_error)?;
        let mut pages = access
            .transaction()?
            .open_table(PAGES)
            .map_err(table_error)?;
        let key = compact.operation_id().into_bytes();
        let current = heads
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_head(&key, value.value()))
            .transpose()?;
        let retained_page = if let Some((key, _)) = &page {
            pages
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .map(|value| value.value().to_vec())
        } else {
            None
        };
        if current.as_ref() == Some(replacement) {
            if page
                .as_ref()
                .is_some_and(|(_, encoded)| retained_page.as_deref() != Some(encoded.as_bytes()))
            {
                return Err(corrupt());
            }
            if compact.prefix().pages() == 0 {
                require_empty(compact.operation_id(), &pages)?;
            }
            drop(pages);
            drop(heads);
            access.abort()?;
            return Ok(WriteResult::Unchanged);
        }
        if current.as_ref() != expected {
            drop(pages);
            drop(heads);
            access.abort()?;
            return Ok(WriteResult::CompareMismatch);
        }
        if retained_page.is_some() {
            return Err(corrupt());
        }
        if prior.is_none() {
            require_empty(compact.operation_id(), &pages)?;
        }
        heads
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        if let Some((key, value)) = &page {
            pages
                .insert(key.as_slice(), value.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        drop(pages);
        drop(heads);
        access.commit_for(RedbTestOperation::ApplicationExportOperation)?;
        Ok(WriteResult::Applied)
    }

    fn verify_application_export_ledger(
        &self,
        expected: &StoredApplicationExportOperationV2,
    ) -> Result<Vec<ApplicationExportPageHash>, StorageError> {
        let transaction = self.begin_read()?;
        let heads = transaction.open_table(HEADS).map_err(table_error)?;
        let Some(value) = heads
            .get(expected.operation_id().as_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Err(corrupt());
        };
        let retained = decode_head(expected.operation_id().as_bytes(), value.value())?;
        if retained != Head::Compact(expected.clone()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let pages = transaction.open_table(PAGES).map_err(table_error)?;
        verify_prefix(expected, &pages)
    }

    fn list_application_export_heads(&self, maximum: usize) -> Result<Vec<Head>, StorageError> {
        if maximum == 0 || maximum > MAX_ACTIVE_APPLICATION_EXPORTS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_read()?;
        let table = transaction.open_table(HEADS).map_err(table_error)?;
        let mut heads = Vec::with_capacity(maximum);
        for row in table.iter().map_err(precommit_storage_error)? {
            if heads.len() == maximum {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let (key, value) = row.map_err(precommit_storage_error)?;
            heads.push(decode_head(key.value(), value.value())?);
        }
        Ok(heads)
    }
}

#[cfg(test)]
mod tests;
