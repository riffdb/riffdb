//! Redb catalog, audit, and capability administration ports.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use redb::{ReadableTable, ReadableTableMetadata};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, AdministrationAuditReader,
    AdministrationAuditScan, AdministrationAuditScanRequest, AdministrationSequenceAllocator,
    AuditPrincipalV1, AuditedAdmissionRepository, AuditedAdmissionRequestV1,
    AuditedAdmissionResultV1, CapabilityAdministrationOperationV1,
    CapabilityAdministrationTransactionPort, CapabilityBootstrapAdministrationRepository,
    CapabilityBootstrapIntentV1, CapabilityBootstrapMarkerV1, CapabilityBootstrapResult,
    CapabilityCreateAwaitingDecision, CapabilityCreateCandidateTransaction,
    CapabilityCreateCandidateV1, CapabilityCreateIntentV1, CapabilityCreateResult,
    CapabilityInventoryPageV1, CapabilityInventoryReader, CapabilityLifecycleV1,
    CapabilityLookupResult, CapabilityMutationCurrentStateV1, CapabilityReader,
    CapabilityRevokeAwaitingDecision, CapabilityRevokeCandidateTransaction,
    CapabilityRevokeCandidateV1, CapabilityRevokeIntentV1, CapabilityRevokeResult,
    CapabilityTokenLookupV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, CatalogRepository, CommandServiceAuditTransitionV1,
    EncodedPageItem, MAX_READABLE_DIGEST_KEYS, MAX_RETAINED_QUERY_MODULES,
    MAX_RETAINED_REACTIVE_MODULES, MAX_SCAN_PAGE_BYTES, QueryModuleActivationIntentV1,
    QueryModuleActivationResult, QueryModuleActiveExpectationV1,
    QueryModuleAdministrationRepository, QueryModuleRepository,
    ReactiveModuleAdministrationRepository, ReactiveModulePublicationIntentV1,
    ReactiveModulePublicationResult, ReactiveModuleRepository, SequenceAllocationError,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    StagedCommandAuditLinkEvidenceV1, StorageError, StorageErrorKind, StorageScanLimit,
    StoredAdministrationAuditRecordV1, StoredCapabilityAdministrationV1, StoredCapabilityRecordV1,
    StoredCatalogAdministrationV1, StoredContractBundleV1, StoredContractMigrationEdgeV1,
    StoredContractMigrationRecordV1, StoredQueryModuleAdministrationV1, StoredQueryModuleV1,
    StoredReactiveModuleAdministrationV1, StoredReactiveModuleV1, StoredServiceAuditRecordV1,
    TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, CapabilityTokenDigest, ContractBundleHash,
    ContractLineage, ContractVersion, QueryModuleHash, ReactiveModuleHash, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceOperationV1,
};

use crate::application::stage_admission_group;
use crate::codec::{
    decode_active_catalog_pointer_v1, decode_administration_audit_record_v1,
    decode_administration_audit_with_command_tables, decode_administration_sequence_allocator_v1,
    decode_capability_bootstrap_marker_v1, decode_capability_record_v1,
    decode_capability_token_lookup_v1, decode_commit_with_event_table, decode_contract_bundle_v1,
    decode_database_identity_v1, decode_provenance_record_v1,
    decode_query_module_administration_v1, decode_query_module_v1, decode_reactive_module_v1,
    encode_active_catalog_pointer_v1, encode_administration_audit_record_v1,
    encode_administration_sequence_allocator_v1, encode_capability_bootstrap_marker_v1,
    encode_capability_record_v1, encode_capability_token_lookup_v1, encode_contract_bundle_v1,
    encode_query_module_administration_v1, encode_query_module_v1, encode_reactive_module_v1,
};
use crate::command_authority::command_member_at_access;
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::journal::{JournalMutation, JournalTable};
use crate::keys::{
    decode_audit_key, decode_capability_key, decode_contract_migration_operation_key,
    encode_active_query_module_key, encode_application_sequence_key, encode_audit_key,
    encode_capability_key, encode_capability_token_key, encode_contract_bundle_key,
    encode_contract_migration_operation_key, encode_contract_write_retirement_key,
    encode_provenance_key, encode_query_module_key, encode_reactive_module_key,
};
use crate::layout::{
    AUDIT, CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS,
    CONTRACT_BUNDLES, CONTRACT_MIGRATIONS, CONTRACT_WRITE_RETIREMENTS, EVENTS, META,
    META_ADMINISTRATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_DATABASE_ID, PROVENANCE,
    QUERY_MODULE_ACTIVE, QUERY_MODULES, REACTIVE_MODULES,
};
use crate::store::{RedbOperationalPorts, RedbReadAccess, RedbWriteAccess};

fn decoded_value<T>(item: EncodedPageItem<T>) -> T {
    item.into_parts().0
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn sequence_error(error: SequenceAllocationError) -> StorageError {
    match error {
        SequenceAllocationError::Exhausted => storage_error(StorageErrorKind::SequenceExhausted),
        SequenceAllocationError::ZeroCount | SequenceAllocationError::TooMany => invariant(),
    }
}

fn read_database_id(
    transaction: &redb::WriteTransaction,
) -> Result<riffdb_types::DatabaseId, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let value = table
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(value.value())?.value())
}

fn read_database_id_readonly(
    transaction: &redb::ReadTransaction,
) -> Result<riffdb_types::DatabaseId, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let value = table
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(value.value())?.value())
}

pub(crate) fn read_administration_allocator(
    transaction: &redb::WriteTransaction,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let value = table
        .get(META_ADMINISTRATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    decode_administration_sequence_allocator_v1(value.value()).map(decoded_value)
}

fn read_administration_allocator_readonly(
    transaction: &redb::ReadTransaction,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let value = table
        .get(META_ADMINISTRATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    decode_administration_sequence_allocator_v1(value.value()).map(decoded_value)
}

fn read_administration_allocator_access(
    access: &RedbReadAccess,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let encoded = access
        .read_value(JournalTable::Meta, META_ADMINISTRATION_SEQUENCE.as_bytes())?
        .ok_or_else(corrupt)?;
    decode_administration_sequence_allocator_v1(&encoded).map(decoded_value)
}

fn read_administration_record_access(
    ports: &RedbOperationalPorts,
    access: &RedbReadAccess,
    sequence: AdministrationSequence,
) -> Result<StoredAdministrationAuditRecordV1, StorageError> {
    let key = encode_audit_key(sequence);
    let physical = access
        .read_value(JournalTable::Audit, key.as_slice())?
        .map(
            |encoded| match riffdb_storage_api::decode_command_audit_locator_v1(&encoded) {
                Ok(locator) => {
                    let locator = locator.into_parts().0;
                    let command = command_member_at_access(access, locator.commit_sequence())?
                        .ok_or_else(corrupt)?;
                    let base = command.base();
                    let audit = match locator.member() {
                        riffdb_storage_api::StoredCommandAuditMemberV1::Started => {
                            base.started_audit()
                        }
                        riffdb_storage_api::StoredCommandAuditMemberV1::Terminal => {
                            base.terminal_audit()
                        }
                    };
                    Ok(StoredAdministrationAuditRecordV1::Service(audit.clone()))
                }
                Err(_) => Ok(decoded_value(decode_administration_audit_record_v1(
                    &encoded,
                )?)),
            },
        )
        .transpose()?;
    let derived = ports
        .indexed_command_audit_at_access(access, sequence)?
        .map(StoredAdministrationAuditRecordV1::Service);
    let record = match (physical, derived) {
        (Some(physical), Some(derived)) if physical == derived => physical,
        (Some(record), None) | (None, Some(record)) => record,
        (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
    };
    if record.administration_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(record)
}

fn validate_administration_stream_access(
    ports: &RedbOperationalPorts,
    access: &RedbReadAccess,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let allocator = read_administration_allocator_access(access)?;
    let mut current = AdministrationSequence::first();
    loop {
        let present = match allocator {
            AdministrationSequenceAllocator::Next(next) => current < next,
            AdministrationSequenceAllocator::Exhausted => true,
        };
        if !present {
            return Ok(allocator);
        }
        read_administration_record_access(ports, access, current)?;
        let Some(next) = current.checked_next() else {
            return if allocator == AdministrationSequenceAllocator::Exhausted {
                Ok(allocator)
            } else {
                Err(corrupt())
            };
        };
        current = next;
    }
}

fn read_administration_record_readonly(
    ports: &RedbOperationalPorts,
    transaction: &redb::ReadTransaction,
    sequence: AdministrationSequence,
) -> Result<StoredAdministrationAuditRecordV1, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let key = encode_audit_key(sequence);
    let physical = table
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|value| {
            decode_administration_audit_with_command_tables(value.value(), &commits, &events)
                .map(decoded_value)
        })
        .transpose()?;
    let derived = ports
        .indexed_command_audit(sequence)?
        .map(StoredAdministrationAuditRecordV1::Service);
    let record = match (physical, derived) {
        (Some(physical), Some(derived)) if physical == derived => physical,
        (Some(record), None) | (None, Some(record)) => record,
        (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
    };
    if record.administration_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(record)
}

fn validate_administration_tail(
    access: &crate::store::RedbWriteAccess,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let allocator_bytes = access
        .read_command_value(JournalTable::Meta, META_ADMINISTRATION_SEQUENCE.as_bytes())?
        .ok_or_else(corrupt)?;
    let allocator = decoded_value(decode_administration_sequence_allocator_v1(
        &allocator_bytes,
    )?);
    let expected_last = match allocator {
        AdministrationSequenceAllocator::Next(next) => {
            AdministrationSequence::new(next.get().saturating_sub(1))
        }
        AdministrationSequenceAllocator::Exhausted => AdministrationSequence::new(u64::MAX),
    };
    let Some(expected_last) = expected_last else {
        if !access
            .read_command_range(JournalTable::Audit, &[0], &[u8::MAX; 9], 1)?
            .is_empty()
        {
            return Err(corrupt());
        }
        return Ok(allocator);
    };
    let key = encode_audit_key(expected_last);
    let derived = access
        .command_audit_record(expected_last)?
        .map(StoredAdministrationAuditRecordV1::Service);
    let physical = access
        .read_command_value(JournalTable::Audit, key.as_slice())?
        .map(
            |value| match decode_administration_audit_record_v1(&value) {
                Ok(record) => Ok(Some(decoded_value(record))),
                Err(_) if riffdb_storage_api::decode_command_audit_locator_v1(&value).is_ok() => {
                    Ok(None)
                }
                Err(error) => Err(error),
            },
        )
        .transpose()?
        .flatten();
    let exact = match (physical, derived) {
        (Some(physical), Some(derived)) if physical == derived => physical,
        (Some(physical), None) | (None, Some(physical)) => physical,
        (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
    };
    if exact.administration_sequence() != expected_last {
        return Err(corrupt());
    }
    let next_key = expected_last.checked_next().map(encode_audit_key);
    if let Some(next_key) = next_key
        && !access
            .read_command_range(JournalTable::Audit, next_key.as_slice(), &[u8::MAX; 9], 1)?
            .is_empty()
    {
        return Err(corrupt());
    }
    Ok(allocator)
}

/// Resolves only the newest command-owned audit member from redb's current
/// root. This covers a sealed standard-durability epoch that is visible to the
/// next writer but deliberately absent from the published read indexes until
/// its journal fence completes. The normal path is the O(1) transient lookup
/// above; this fallback decodes at most the final command segment.
fn command_audit_at_transaction_tail<T>(
    commits: &T,
    sequence: AdministrationSequence,
) -> Result<Option<StoredServiceAuditRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some((_, value)) = commits.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let Ok(segment) = riffdb_storage_api::decode_command_segment_v1(value.value()) else {
        return Ok(None);
    };
    let Some(command) = segment.value().commands().last() else {
        return Err(corrupt());
    };
    for record in [
        command.base().started_audit(),
        command.base().terminal_audit(),
    ] {
        if record.administration_sequence() == sequence {
            return Ok(Some(record.clone()));
        }
    }
    Ok(None)
}

/// Proves the audit tail agrees with the allocator in O(1) table probes.
///
/// Startup and every public read validate the complete contiguous stream. Once
/// that proof holds, typed writes preserve it inductively: the only audit
/// mutation appends exactly the allocator-owned next sequence and atomically
/// advances the allocator. Checking count plus the exact decoded tail therefore
/// rejects a lost, duplicated, reordered, or allocator-skewed transition
/// without rescanning all retained history for every append.
///
/// Shared verbatim by the write path and by the read-transaction republish
/// probe so the two can never disagree about which streams are admissible.
fn validate_administration_tail_readonly(
    ports: &RedbOperationalPorts,
    transaction: &redb::ReadTransaction,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    let allocator = read_administration_allocator_readonly(transaction)?;
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let expected_last = match allocator {
        AdministrationSequenceAllocator::Next(next) => {
            AdministrationSequence::new(next.get().saturating_sub(1))
        }
        AdministrationSequenceAllocator::Exhausted => AdministrationSequence::new(u64::MAX),
    };
    let Some(expected_last) = expected_last else {
        if audit.last().map_err(precommit_storage_error)?.is_some() {
            return Err(corrupt());
        }
        return Ok(allocator);
    };
    let key = encode_audit_key(expected_last);
    let physical = match audit.get(key.as_slice()).map_err(precommit_storage_error)? {
        Some(value) => {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            Some(decoded_value(
                decode_administration_audit_with_command_tables(value.value(), &commits, &events)?,
            ))
        }
        None => None,
    };
    let derived = match ports.indexed_command_audit(expected_last)? {
        Some(indexed) => Some(indexed),
        None => {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            command_audit_at_transaction_tail(&commits, expected_last)?
        }
    }
    .map(StoredAdministrationAuditRecordV1::Service);
    let exact = match (physical, derived) {
        (Some(physical), Some(derived)) if physical == derived => physical,
        (Some(record), None) | (None, Some(record)) => record,
        (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
    };
    if exact.administration_sequence() != expected_last {
        return Err(corrupt());
    }
    if let Some((last_key, _)) = audit.last().map_err(precommit_storage_error)?
        && decode_audit_key(last_key.value()).map_err(|_| corrupt())? > expected_last
    {
        return Err(corrupt());
    }
    Ok(allocator)
}

fn validate_administration_stream_readonly(
    ports: &RedbOperationalPorts,
    transaction: &redb::ReadTransaction,
) -> Result<(), StorageError> {
    let allocator = read_administration_allocator_readonly(transaction)?;
    let mut current = AdministrationSequence::first();
    loop {
        let present = match allocator {
            AdministrationSequenceAllocator::Next(next) => current < next,
            AdministrationSequenceAllocator::Exhausted => true,
        };
        if !present {
            return Ok(());
        }
        read_administration_record_readonly(ports, transaction, current)?;
        let Some(next) = current.checked_next() else {
            return if allocator == AdministrationSequenceAllocator::Exhausted {
                Ok(())
            } else {
                Err(corrupt())
            };
        };
        current = next;
    }
}

fn allocate_sequences(
    allocator: AdministrationSequenceAllocator,
    count: u16,
) -> Result<(Vec<AdministrationSequence>, AdministrationSequenceAllocator), StorageError> {
    let allocation = allocator
        .allocate_consecutive(count)
        .map_err(sequence_error)?;
    Ok((allocation.assigned().to_vec(), allocation.next()))
}

pub(crate) fn write_administration_allocator(
    transaction: &redb::WriteTransaction,
    expected: AdministrationSequenceAllocator,
    next: AdministrationSequenceAllocator,
) -> Result<(), StorageError> {
    let encoded = encode_administration_sequence_allocator_v1(next)?;
    let mut table = transaction.open_table(META).map_err(table_error)?;
    let current = table
        .get(META_ADMINISTRATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let decoded = decoded_value(decode_administration_sequence_allocator_v1(
        current.value(),
    )?);
    drop(current);
    if decoded != expected {
        return Err(invariant());
    }
    table
        .insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
        .map_err(precommit_storage_error)?;
    Ok(())
}

fn write_administration_allocator_in_access(
    access: &RedbWriteAccess,
    expected: AdministrationSequenceAllocator,
    next: AdministrationSequenceAllocator,
) -> Result<(), StorageError> {
    let current = access
        .read_command_value(JournalTable::Meta, META_ADMINISTRATION_SEQUENCE.as_bytes())?
        .ok_or_else(corrupt)?;
    if decoded_value(decode_administration_sequence_allocator_v1(&current)?) != expected {
        return Err(invariant());
    }
    let encoded = encode_administration_sequence_allocator_v1(next)?;
    let replaced = access.put_command_value(
        JournalTable::Meta,
        META_ADMINISTRATION_SEQUENCE.as_bytes().to_vec(),
        encoded.into_bytes(),
    )?;
    if replaced.as_deref() != Some(current.as_slice()) {
        return Err(invariant());
    }
    Ok(())
}

fn write_service_audit_request_index(
    transaction: &redb::WriteTransaction,
    request_id: riffdb_types::RequestId,
    sequence: AdministrationSequence,
) -> Result<(), StorageError> {
    let index_key = crate::keys::encode_audit_by_request_key(request_id, sequence);
    let index_value = crate::codec::encode_service_audit_request_index_v1(
        riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(request_id, sequence),
    )?;
    let mut index = transaction
        .open_table(crate::layout::AUDIT_BY_REQUEST)
        .map_err(table_error)?;
    if index
        .insert(index_key.as_slice(), index_value.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    Ok(())
}

pub(crate) fn append_audit_record(
    transaction: &redb::WriteTransaction,
    record: &StoredAdministrationAuditRecordV1,
) -> Result<(), StorageError> {
    let key = encode_audit_key(record.administration_sequence());
    let encoded = encode_administration_audit_record_v1(record)?;
    let mut table = transaction.open_table(AUDIT).map_err(table_error)?;
    if table
        .insert(key.as_slice(), encoded.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    drop(table);
    if let StoredAdministrationAuditRecordV1::Service(service) = record {
        write_service_audit_request_index(
            transaction,
            service.request_id(),
            service.administration_sequence(),
        )?;
    }
    Ok(())
}

fn read_audit_record<T, C, E>(
    table: &T,
    commits: &C,
    events: &E,
    sequence: AdministrationSequence,
) -> Result<StoredAdministrationAuditRecordV1, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_audit_key(sequence);
    let value = table
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let record = decoded_value(decode_administration_audit_with_command_tables(
        value.value(),
        commits,
        events,
    )?);
    if record.administration_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(record)
}

/// Returns the shared administration sequence of the one publication that
/// installed this exact immutable reactive module, if the stream retains it.
///
/// Reactive-module rows carry no administration sequence and the frozen durable
/// layout has no module-hash reverse index, so the authoritative stream is the
/// only source of the original publication's identity. Scanning it is bounded by
/// the stream itself, which is append-only, contiguous, and never pruned
/// (`validate_administration_tail`), and this runs only on the rare idempotent
/// republish transition, never on a publication that writes. Two records naming
/// one module hash is corruption: publication is immutable and single-writer.
fn published_reactive_module_sequence<T, C, E>(
    audit: &T,
    commits: &C,
    events: &E,
    module_hash: ReactiveModuleHash,
) -> Result<Option<AdministrationSequence>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut found = None;
    for entry in audit.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let record = decoded_value(decode_administration_audit_with_command_tables(
            value.value(),
            commits,
            events,
        )?);
        if let StoredAdministrationAuditRecordV1::ReactiveModule(published) = record
            && published.module_hash() == module_hash
            && found.replace(published.administration_sequence()).is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(found)
}

fn find_audit_record<T, C, E>(
    table: &T,
    commits: &C,
    events: &E,
    sequence: AdministrationSequence,
) -> Result<Option<StoredAdministrationAuditRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_audit_key(sequence);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let record = decoded_value(decode_administration_audit_with_command_tables(
        value.value(),
        commits,
        events,
    )?);
    if record.administration_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(Some(record))
}

fn active_catalog_from_table<T>(table: &T) -> Result<Option<ActiveCatalogPointerV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut active = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if key.value() != CATALOG_ACTIVE_KEY {
            return Err(corrupt());
        }
        if active.is_some() {
            return Err(corrupt());
        }
        active = Some(decoded_value(decode_active_catalog_pointer_v1(
            value.value(),
        )?));
    }
    Ok(active)
}

fn read_active_catalog_write(
    transaction: &redb::WriteTransaction,
) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
    let table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    active_catalog_from_table(&table)
}

fn read_contract_bundle_from_table<T>(
    table: &T,
    lineage: &ContractLineage,
    version: ContractVersion,
) -> Result<Option<StoredContractBundleV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let bundle = decoded_value(decode_contract_bundle_v1(value.value())?);
    if bundle.lineage() != lineage || bundle.contract_version() != version {
        return Err(corrupt());
    }
    Ok(Some(bundle))
}

fn last_catalog_activation<T, C, E>(
    table: &T,
    commits: &C,
    events: &E,
) -> Result<Option<StoredCatalogAdministrationV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut last = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = decode_audit_key(key.value()).map_err(|_| corrupt())?;
        let record = decoded_value(decode_administration_audit_with_command_tables(
            value.value(),
            commits,
            events,
        )?);
        if record.administration_sequence() != sequence {
            return Err(corrupt());
        }
        if let StoredAdministrationAuditRecordV1::Catalog(record) = record {
            last = Some(record);
        }
    }
    Ok(last)
}

fn last_contract_migration<T>(
    table: &T,
) -> Result<Option<StoredContractMigrationRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut last = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let operation =
            decode_contract_migration_operation_key(key.value()).map_err(|_| corrupt())?;
        let record =
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value.value())
                .map_err(|_| corrupt())?
                .into_parts()
                .0;
        if record.operation_id() != operation {
            return Err(corrupt());
        }
        if last
            .as_ref()
            .is_none_or(|prior: &StoredContractMigrationRecordV1| {
                prior.administration_sequence() < record.administration_sequence()
            })
        {
            last = Some(record);
        }
    }
    Ok(last)
}

fn active_authority_sequence(
    pointer: &ActiveCatalogPointerV1,
    catalog: Option<&StoredCatalogAdministrationV1>,
    migration: Option<&StoredContractMigrationRecordV1>,
) -> Result<AdministrationSequence, StorageError> {
    match (catalog, migration) {
        (Some(catalog), Some(migration))
            if catalog.administration_sequence() > migration.administration_sequence() =>
        {
            (catalog.activated() == pointer)
                .then_some(catalog.administration_sequence())
                .ok_or_else(corrupt)
        }
        (Some(catalog), Some(migration))
            if migration.administration_sequence() > catalog.administration_sequence() =>
        {
            (migration.artifacts().candidate() == pointer.bundle_hash())
                .then_some(migration.administration_sequence())
                .ok_or_else(corrupt)
        }
        (Some(catalog), None) => (catalog.activated() == pointer)
            .then_some(catalog.administration_sequence())
            .ok_or_else(corrupt),
        (None, Some(migration)) => (migration.artifacts().candidate() == pointer.bundle_hash())
            .then_some(migration.administration_sequence())
            .ok_or_else(corrupt),
        (Some(_), Some(_)) | (None, None) => Err(corrupt()),
    }
}

impl CatalogRepository for RedbOperationalPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        let transaction = self.begin_read()?;
        validate_administration_stream_readonly(self, &transaction)?;
        let active_table = transaction
            .open_table(CATALOG_ACTIVE)
            .map_err(table_error)?;
        let active = active_catalog_from_table(&active_table)?;
        drop(active_table);
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let last = last_catalog_activation(&audit, &commits, &events)?;
        drop(audit);
        let migrations = transaction
            .open_table(CONTRACT_MIGRATIONS)
            .map_err(table_error)?;
        let last_migration = last_contract_migration(&migrations)?;
        drop(migrations);
        match (&active, last.as_ref(), last_migration.as_ref()) {
            (None, None, None) => Ok(None),
            (Some(pointer), catalog, migration) => {
                active_authority_sequence(pointer, catalog, migration)?;
                let bundles = transaction
                    .open_table(CONTRACT_BUNDLES)
                    .map_err(table_error)?;
                let bundle = read_contract_bundle_from_table(
                    &bundles,
                    pointer.lineage(),
                    pointer.contract_version(),
                )?
                .ok_or_else(corrupt)?;
                if !pointer.matches_bundle(&bundle) {
                    return Err(corrupt());
                }
                Ok(active)
            }
            _ => Err(corrupt()),
        }
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        let transaction = self.begin_read()?;
        read_database_id_readonly(&transaction)?;
        let table = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(table_error)?;
        read_contract_bundle_from_table(&table, lineage, contract_version)
    }

    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        let transaction = self.begin_read()?;
        read_database_id_readonly(&transaction)?;
        let retirements = transaction
            .open_table(CONTRACT_WRITE_RETIREMENTS)
            .map_err(table_error)?;
        let retirement_key = encode_contract_write_retirement_key(predecessor);
        let Some(retirement) = retirements
            .get(retirement_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let retirement = decoded_value(
            riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(
                retirement.value(),
            )
            .map_err(|_| corrupt())?,
        );
        if retirement.artifacts().parent() != predecessor {
            return Err(corrupt());
        }
        let migrations = transaction
            .open_table(CONTRACT_MIGRATIONS)
            .map_err(table_error)?;
        let operation_key = encode_contract_migration_operation_key(retirement.operation_id());
        let migration = migrations
            .get(operation_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let migration = decoded_value(
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(migration.value())
                .map_err(|_| corrupt())?,
        );
        StoredContractMigrationEdgeV1::new(retirement, migration)
            .map(Some)
            .map_err(|_| corrupt())
    }
}

impl AdministrationAuditReader for RedbOperationalPorts {
    fn scan_administration_audit(
        &self,
        request: AdministrationAuditScanRequest,
    ) -> Result<AdministrationAuditScan, StorageError> {
        let transaction = self.begin_composite_read()?;
        let allocator = validate_administration_stream_access(self, &transaction)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut has_more = false;
        let mut next = match request.after() {
            Some(after) => after.checked_next(),
            None => Some(AdministrationSequence::first()),
        };
        while let Some(sequence) = next {
            let present = match allocator {
                AdministrationSequenceAllocator::Next(frontier) => sequence < frontier,
                AdministrationSequenceAllocator::Exhausted => true,
            };
            if !present {
                break;
            }
            let record = read_administration_record_access(self, &transaction, sequence)?;
            let encoded = encode_administration_audit_record_v1(&record)?;
            let item = EncodedPageItem::new(record, encoded.encoded_content_charge());
            let next_bytes = bytes
                .checked_add(item.encoded_content_charge().get())
                .ok_or_else(corrupt)?;
            if records.len() == usize::from(request.limit().get())
                || next_bytes > MAX_SCAN_PAGE_BYTES
            {
                has_more = true;
                break;
            }
            bytes = next_bytes;
            records.push(item);
            next = sequence.checked_next();
        }
        if records.is_empty() && has_more {
            return Err(corrupt());
        }
        AdministrationAuditScan::page(request, records, has_more).map_err(|_| corrupt())
    }
}

impl CatalogAdministrationRepository for RedbOperationalPorts {
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;
        let bundles = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(table_error)?;
        let existing = read_contract_bundle_from_table(
            &bundles,
            intent.bundle().lineage(),
            intent.bundle().contract_version(),
        )?;
        drop(bundles);
        if existing
            .as_ref()
            .is_some_and(|bundle| bundle != intent.bundle())
        {
            access.abort()?;
            return Ok(CatalogActivationResult::BundleConflict);
        }

        let active = read_active_catalog_write(transaction)?;
        let requested = intent.requested_active();
        if active.as_ref() == Some(&requested) {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let last = last_catalog_activation(&audit, &commits, &events)?.ok_or_else(corrupt)?;
            drop(audit);
            let migrations = transaction
                .open_table(CONTRACT_MIGRATIONS)
                .map_err(table_error)?;
            let last_migration = last_contract_migration(&migrations)?;
            drop(migrations);
            let sequence =
                active_authority_sequence(&requested, Some(&last), last_migration.as_ref())?;
            drop(events);
            drop(commits);
            access.abort()?;
            return Ok(CatalogActivationResult::AlreadyActive {
                active: requested,
                administration_sequence: sequence,
            });
        }

        let actual = active
            .as_ref()
            .map(ActiveCatalogPointerV1::contract_version);
        if actual != intent.expected_active_version() {
            access.abort()?;
            return Ok(CatalogActivationResult::ExpectedActiveVersionMismatch { actual });
        }

        let (assigned, next) = allocate_sequences(allocator, 1)?;
        let sequence = assigned[0];
        let record =
            StoredCatalogAdministrationV1::from_committed_intent(sequence, intent, active.clone())
                .map_err(|_| corrupt())?;
        let encoded_bundle = encode_contract_bundle_v1(intent.bundle())?;
        let encoded_active = encode_active_catalog_pointer_v1(&requested)?;
        let audit_record = StoredAdministrationAuditRecordV1::Catalog(record);

        if existing.is_none() {
            let key = encode_contract_bundle_key(
                intent.bundle().lineage(),
                intent.bundle().contract_version(),
            )
            .map_err(|_| invariant())?;
            let mut table = transaction
                .open_table(CONTRACT_BUNDLES)
                .map_err(table_error)?;
            if table
                .insert(key.as_slice(), encoded_bundle.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(invariant());
            }
        }
        {
            let mut table = transaction
                .open_table(CATALOG_ACTIVE)
                .map_err(table_error)?;
            let prior = table
                .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
                .map_err(precommit_storage_error)?;
            let prior = prior
                .map(|value| decode_active_catalog_pointer_v1(value.value()).map(decoded_value))
                .transpose()?;
            if prior != active {
                return Err(invariant());
            }
        }
        append_audit_record(transaction, &audit_record)?;
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::CatalogAdministration)?;
        Ok(CatalogActivationResult::Activated {
            active: requested,
            administration_sequence: sequence,
        })
    }
}

/// Loads every active query-module pointer together with its stored module body.
pub(crate) fn load_active_query_modules(
    ports: &RedbOperationalPorts,
) -> Result<Vec<(ActiveQueryModulePointerV1, StoredQueryModuleV1)>, StorageError> {
    let transaction = ports.begin_read()?;
    let active_table = transaction
        .open_table(QUERY_MODULE_ACTIVE)
        .map_err(table_error)?;
    let modules_table = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
    let mut loaded = Vec::new();
    let mut count = 0usize;
    for entry in active_table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        count = count.checked_add(1).ok_or_else(corrupt)?;
        if count > MAX_RETAINED_QUERY_MODULES {
            return Err(corrupt());
        }
        let record = decoded_value(decode_query_module_administration_v1(value.value())?);
        let pointer = record.activated().clone();
        // Self-check key identity.
        let expected = encode_active_query_module_key(
            pointer.contract_lineage(),
            pointer.contract_version(),
            pointer.contract_bundle_hash(),
        )
        .map_err(|_| corrupt())?;
        if key.value() != expected.as_slice() {
            return Err(corrupt());
        }
        let module =
            query_module_from_table(&modules_table, pointer.module_hash())?.ok_or_else(corrupt)?;
        if !pointer.matches_module(&module) {
            return Err(corrupt());
        }
        loaded.push((pointer, module));
    }
    Ok(loaded)
}

fn query_module_from_table<T>(
    table: &T,
    module_hash: QueryModuleHash,
) -> Result<Option<StoredQueryModuleV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_query_module_key(module_hash);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let module = decoded_value(decode_query_module_v1(value.value())?);
    if module.module_hash() != module_hash {
        return Err(corrupt());
    }
    Ok(Some(module))
}

fn active_query_module_from_table<T>(
    table: &T,
    lineage: &ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
) -> Result<Option<StoredQueryModuleAdministrationV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key =
        encode_active_query_module_key(lineage, version, bundle_hash).map_err(|_| invariant())?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let record = decoded_value(decode_query_module_administration_v1(value.value())?);
    let active = record.activated();
    if active.contract_lineage() != lineage
        || active.contract_version() != version
        || active.contract_bundle_hash() != bundle_hash
    {
        return Err(corrupt());
    }
    Ok(Some(record))
}

fn module_version_conflicts<T>(
    table: &T,
    candidate: &StoredQueryModuleV1,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut count = 0usize;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        count = count.checked_add(1).ok_or_else(corrupt)?;
        if count > MAX_RETAINED_QUERY_MODULES {
            return Err(corrupt());
        }
        let module = decoded_value(decode_query_module_v1(value.value())?);
        if key.value() != encode_query_module_key(module.module_hash()) {
            return Err(corrupt());
        }
        if module.contract_lineage() == candidate.contract_lineage()
            && module.contract_version() == candidate.contract_version()
            && module.contract_bundle_hash() == candidate.contract_bundle_hash()
            && module.module_name() == candidate.module_name()
            && module.module_version() == candidate.module_version()
            && module.module_hash() != candidate.module_hash()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

impl QueryModuleRepository for RedbOperationalPorts {
    fn read_query_module(
        &self,
        module_hash: QueryModuleHash,
    ) -> Result<Option<StoredQueryModuleV1>, StorageError> {
        let transaction = self.begin_read()?;
        read_database_id_readonly(&transaction)?;
        let modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
        query_module_from_table(&modules, module_hash)
    }

    fn read_active_query_module(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Result<Option<ActiveQueryModulePointerV1>, StorageError> {
        let transaction = self.begin_read()?;
        validate_administration_stream_readonly(self, &transaction)?;
        let active_table = transaction
            .open_table(QUERY_MODULE_ACTIVE)
            .map_err(table_error)?;
        let active = active_query_module_from_table(
            &active_table,
            lineage,
            contract_version,
            contract_bundle_hash,
        )?;
        let Some(record) = active else {
            return Ok(None);
        };
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        let key = encode_audit_key(record.administration_sequence());
        let durable = audit
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let durable = decoded_value(decode_administration_audit_with_command_tables(
            durable.value(),
            &commits,
            &events,
        )?);
        if durable != StoredAdministrationAuditRecordV1::QueryModule(record.clone()) {
            return Err(corrupt());
        }
        let modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
        let module = query_module_from_table(&modules, record.activated().module_hash())?
            .ok_or_else(corrupt)?;
        if !record.activated().matches_module(&module) {
            return Err(corrupt());
        }
        Ok(Some(record.activated().clone()))
    }
}

impl QueryModuleAdministrationRepository for RedbOperationalPorts {
    fn activate_query_module(
        &mut self,
        intent: &QueryModuleActivationIntentV1,
    ) -> Result<QueryModuleActivationResult, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;

        let bundles = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(table_error)?;
        let contract = read_contract_bundle_from_table(
            &bundles,
            intent.module().contract_lineage(),
            intent.module().contract_version(),
        )?;
        drop(bundles);
        if !contract
            .is_some_and(|bundle| bundle.bundle_hash() == intent.module().contract_bundle_hash())
        {
            access.abort()?;
            return Ok(QueryModuleActivationResult::ContractUnavailable);
        }

        let modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
        let existing = query_module_from_table(&modules, intent.module().module_hash())?;
        if existing
            .as_ref()
            .is_some_and(|module| module != intent.module())
        {
            return Err(corrupt());
        }
        if module_version_conflicts(&modules, intent.module())? {
            drop(modules);
            access.abort()?;
            return Ok(QueryModuleActivationResult::ModuleVersionConflict);
        }
        drop(modules);

        let active_table = transaction
            .open_table(QUERY_MODULE_ACTIVE)
            .map_err(table_error)?;
        let active_record = active_query_module_from_table(
            &active_table,
            intent.module().contract_lineage(),
            intent.module().contract_version(),
            intent.module().contract_bundle_hash(),
        )?;
        drop(active_table);
        let active = active_record
            .as_ref()
            .map(|record| record.activated().clone());
        let requested = intent.requested_active();
        if active.as_ref() == Some(&requested) {
            let sequence = active_record
                .as_ref()
                .map(StoredQueryModuleAdministrationV1::administration_sequence)
                .ok_or_else(corrupt)?;
            access.abort()?;
            return Ok(QueryModuleActivationResult::AlreadyActive {
                active: requested,
                administration_sequence: sequence,
            });
        }

        let expectation_matches = match intent.expectation() {
            QueryModuleActiveExpectationV1::Any => true,
            QueryModuleActiveExpectationV1::Absent => active.is_none(),
            QueryModuleActiveExpectationV1::Exact(expected) => {
                active.as_ref().map(ActiveQueryModulePointerV1::module_hash) == Some(expected)
            }
        };
        if !expectation_matches {
            let actual = active.as_ref().map(ActiveQueryModulePointerV1::module_hash);
            access.abort()?;
            return Ok(QueryModuleActivationResult::ExpectedActiveMismatch { actual });
        }

        let (assigned, next) = allocate_sequences(allocator, 1)?;
        let sequence = assigned[0];
        let record =
            StoredQueryModuleAdministrationV1::from_committed_intent(sequence, intent, active);
        let encoded_module = encode_query_module_v1(intent.module())?;
        let encoded_active = encode_query_module_administration_v1(&record)?;
        let audit_record = StoredAdministrationAuditRecordV1::QueryModule(record.clone());
        if existing.is_none() {
            let key = encode_query_module_key(intent.module().module_hash());
            let mut table = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
            if table
                .insert(key.as_slice(), encoded_module.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(invariant());
            }
        }
        {
            let key = encode_active_query_module_key(
                intent.module().contract_lineage(),
                intent.module().contract_version(),
                intent.module().contract_bundle_hash(),
            )
            .map_err(|_| invariant())?;
            let mut table = transaction
                .open_table(QUERY_MODULE_ACTIVE)
                .map_err(table_error)?;
            let prior = table
                .insert(key.as_slice(), encoded_active.as_bytes())
                .map_err(precommit_storage_error)?
                .map(|value| {
                    decode_query_module_administration_v1(value.value()).map(decoded_value)
                })
                .transpose()?;
            if prior != active_record {
                return Err(invariant());
            }
        }
        append_audit_record(transaction, &audit_record)?;
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::ReactiveModuleAdministration)?;
        Ok(QueryModuleActivationResult::Activated {
            active: requested,
            administration_sequence: sequence,
        })
    }
}

fn reactive_module_from_table<T>(
    table: &T,
    module_hash: ReactiveModuleHash,
) -> Result<Option<StoredReactiveModuleV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_reactive_module_key(module_hash);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let module = decoded_value(decode_reactive_module_v1(value.value())?);
    if module.module_hash() != module_hash {
        return Err(corrupt());
    }
    Ok(Some(module))
}

/// True when the exact contract artifact a reactive module compiled against is
/// still retained. Shared by the write path and the read-transaction probe.
fn reactive_contract_is_retained<T>(
    bundles: &T,
    candidate: &StoredReactiveModuleV1,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    Ok(read_contract_bundle_from_table(
        bundles,
        candidate.contract_lineage(),
        candidate.contract_version(),
    )?
    .is_some_and(|bundle| bundle.bundle_hash() == candidate.contract_bundle_hash()))
}

/// True when one exact query-module dependency is retained against the same
/// contract artifact. Shared by the write path and the read-transaction probe.
fn reactive_dependency_is_retained<T>(
    query_modules: &T,
    candidate: &StoredReactiveModuleV1,
    dependency: QueryModuleHash,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    Ok(
        query_module_from_table(query_modules, dependency)?.is_some_and(|module| {
            module.contract_lineage() == candidate.contract_lineage()
                && module.contract_version() == candidate.contract_version()
                && module.contract_bundle_hash() == candidate.contract_bundle_hash()
        }),
    )
}

impl ReactiveModuleRepository for RedbOperationalPorts {
    /// Returns the retained row without re-checking that the stream still holds
    /// its publication record, unlike the memory backend's equivalent. Ruled
    /// asymmetry, not an oversight.
    ///
    /// `inspect_reactive_module_row` proves row-presence implies
    /// publication-record for EVERY retained row at open, unconditionally: the
    /// reactive-module count is an additive structural count, which the
    /// validated-prefix checkpoint guard does not exempt from inspection. The
    /// sole writer then commits the row, its audit record, and the allocator
    /// advance in one transaction, and nothing anywhere removes either
    /// (`retention.rs` never touches `AUDIT`), so the property is preserved
    /// inductively. A per-read check is therefore entailed by its own
    /// precondition — zero detection power in any reachable state — while
    /// costing an unbounded `AUDIT` decode scan on the hottest consumer path
    /// (every consume, acknowledge, negative-acknowledge, seek, retire, and
    /// status RPC, with no cache), over a stream those same RPCs lengthen.
    fn read_reactive_module(
        &self,
        module_hash: ReactiveModuleHash,
    ) -> Result<Option<StoredReactiveModuleV1>, StorageError> {
        let transaction = self.begin_read()?;
        read_database_id_readonly(&transaction)?;
        let table = transaction
            .open_table(REACTIVE_MODULES)
            .map_err(table_error)?;
        reactive_module_from_table(&table, module_hash)
    }
}

impl RedbOperationalPorts {
    /// Answers an idempotent reactive-module republish without the write lock.
    ///
    /// The republish outcome writes nothing durable, so the entire answer is a
    /// pure read: the allocator/tail proof, the preconditions the write path
    /// evaluates ahead of the presence probe, the presence probe itself, and
    /// the original publication's sequence. Running it under `begin_read` keeps
    /// the unbounded `AUDIT` walk off the redb write lock, which one writer
    /// holds against every other writer — commands included — for its duration.
    ///
    /// `Ok(None)` means "not answerable here", never "publish is admissible":
    /// the module is absent, a precondition the write path reports as its own
    /// outcome is unmet, or the retained row disagrees with the candidate.
    /// Every such case falls through to the write path, which re-evaluates the
    /// identical checks in the identical order under the write transaction and
    /// owns the whole result taxonomy. Answering only the one no-write outcome
    /// is what makes this probe observably invisible.
    fn republished_reactive_module_sequence(
        &self,
        candidate: &StoredReactiveModuleV1,
    ) -> Result<Option<AdministrationSequence>, StorageError> {
        let transaction = self.begin_read()?;
        validate_administration_tail_readonly(self, &transaction)?;
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;

        let bundles = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(table_error)?;
        let contract_retained = reactive_contract_is_retained(&bundles, candidate)?;
        drop(bundles);
        if !contract_retained {
            return Ok(None);
        }

        let query_modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
        for dependency in candidate.query_module_hashes() {
            if !reactive_dependency_is_retained(&query_modules, candidate, *dependency)? {
                drop(query_modules);
                return Ok(None);
            }
        }
        drop(query_modules);

        let modules = transaction
            .open_table(REACTIVE_MODULES)
            .map_err(table_error)?;
        if modules.len().map_err(precommit_storage_error)?
            > u64::try_from(MAX_RETAINED_REACTIVE_MODULES).map_err(|_| invariant())?
        {
            return Ok(None);
        }
        let existing = reactive_module_from_table(&modules, candidate.module_hash())?;
        drop(modules);
        if existing.as_ref() != Some(candidate) {
            return Ok(None);
        }

        // Fail closed exactly as the write path does: a retained module with no
        // publication record in the stream is corruption, not a republish.
        Ok(Some(
            published_reactive_module_sequence(&audit, &commits, &events, candidate.module_hash())?
                .ok_or_else(corrupt)?,
        ))
    }

    /// Publishes under the exclusive mutation gate.
    ///
    /// Reached only when the read-transaction probe declined to answer. It
    /// re-probes presence under the write transaction because a racing publish
    /// can land between that read and `begin_write`; on a lost race it serves
    /// the republish answer from here rather than retaking the read path, so
    /// the caller sees one consistent outcome. That rare branch does walk
    /// `AUDIT` under the lock — the bounded-probability exception the fast path
    /// exists to avoid on the common republish.
    fn publish_reactive_module_locked(
        &mut self,
        intent: &ReactiveModulePublicationIntentV1,
    ) -> Result<ReactiveModulePublicationResult, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;
        let candidate = intent.module();

        let bundles = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(table_error)?;
        let contract_retained = reactive_contract_is_retained(&bundles, candidate)?;
        drop(bundles);
        if !contract_retained {
            access.abort()?;
            return Ok(ReactiveModulePublicationResult::ContractUnavailable);
        }

        let query_modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
        for dependency in candidate.query_module_hashes() {
            if !reactive_dependency_is_retained(&query_modules, candidate, *dependency)? {
                drop(query_modules);
                access.abort()?;
                return Ok(ReactiveModulePublicationResult::QueryModuleUnavailable {
                    module_hash: *dependency,
                });
            }
        }
        drop(query_modules);

        let modules = transaction
            .open_table(REACTIVE_MODULES)
            .map_err(table_error)?;
        if modules.len().map_err(precommit_storage_error)?
            > u64::try_from(MAX_RETAINED_REACTIVE_MODULES).map_err(|_| invariant())?
        {
            return Err(corrupt());
        }
        if let Some(existing) = reactive_module_from_table(&modules, candidate.module_hash())? {
            drop(modules);
            if existing != *candidate {
                access.abort()?;
                return Err(corrupt());
            }
            // Read the original publication's own sequence before releasing the
            // transaction: an idempotent republish must record a success linked
            // to that publication, not a linkless failure.
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let administration_sequence = published_reactive_module_sequence(
                &audit,
                &commits,
                &events,
                candidate.module_hash(),
            )?
            .ok_or_else(corrupt)?;
            drop(audit);
            drop(events);
            drop(commits);
            access.abort()?;
            return Ok(ReactiveModulePublicationResult::AlreadyPublished {
                module_hash: candidate.module_hash(),
                administration_sequence,
            });
        }
        let mut conflict = false;
        for entry in modules.iter().map_err(precommit_storage_error)? {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let module = decoded_value(decode_reactive_module_v1(value.value())?);
            if key.value() != encode_reactive_module_key(module.module_hash()) {
                return Err(corrupt());
            }
            if module.contract_lineage() == candidate.contract_lineage()
                && module.contract_version() == candidate.contract_version()
                && module.contract_bundle_hash() == candidate.contract_bundle_hash()
                && module.module_name() == candidate.module_name()
                && module.module_version() == candidate.module_version()
            {
                conflict = true;
                break;
            }
        }
        if conflict {
            drop(modules);
            access.abort()?;
            return Ok(ReactiveModulePublicationResult::ModuleVersionConflict);
        }
        if modules.len().map_err(precommit_storage_error)?
            == u64::try_from(MAX_RETAINED_REACTIVE_MODULES).map_err(|_| invariant())?
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        drop(modules);

        let (assigned, next) = allocate_sequences(allocator, 1)?;
        let sequence = assigned[0];
        let record = StoredReactiveModuleAdministrationV1::from_committed_intent(sequence, intent);
        let encoded = encode_reactive_module_v1(candidate)?;
        let key = encode_reactive_module_key(candidate.module_hash());
        let mut modules = transaction
            .open_table(REACTIVE_MODULES)
            .map_err(table_error)?;
        if modules
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(invariant());
        }
        drop(modules);
        append_audit_record(
            transaction,
            &StoredAdministrationAuditRecordV1::ReactiveModule(record),
        )?;
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::QueryModuleAdministration)?;
        Ok(ReactiveModulePublicationResult::Published {
            module_hash: candidate.module_hash(),
            administration_sequence: sequence,
        })
    }
}

impl ReactiveModuleAdministrationRepository for RedbOperationalPorts {
    fn publish_reactive_module(
        &mut self,
        intent: &ReactiveModulePublicationIntentV1,
    ) -> Result<ReactiveModulePublicationResult, StorageError> {
        let candidate = intent.module();
        if let Some(administration_sequence) =
            self.republished_reactive_module_sequence(candidate)?
        {
            return Ok(ReactiveModulePublicationResult::AlreadyPublished {
                module_hash: candidate.module_hash(),
                administration_sequence,
            });
        }
        self.publish_reactive_module_locked(intent)
    }
}

fn capability_from_tables<C, L>(
    capabilities: &C,
    lookups: &L,
    database_id: riffdb_types::DatabaseId,
    capability_id: CapabilityId,
) -> Result<Option<StoredCapabilityRecordV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    L: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_capability_key(capability_id);
    let Some(value) = capabilities
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let record = decoded_value(decode_capability_record_v1(value.value())?);
    if record.capability_id() != capability_id || record.database_id() != database_id {
        return Err(corrupt());
    }
    let lookup_key = encode_capability_token_key(record.token_digest());
    let lookup = lookups
        .get(lookup_key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let lookup = decoded_value(decode_capability_token_lookup_v1(lookup.value())?);
    if lookup.capability_id() != capability_id {
        return Err(corrupt());
    }
    Ok(Some(record))
}

fn capability_from_write(
    transaction: &redb::WriteTransaction,
    capability_id: CapabilityId,
) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
    let database_id = read_database_id(transaction)?;
    let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
    let lookups = transaction
        .open_table(CAPABILITY_TOKENS)
        .map_err(table_error)?;
    capability_from_tables(&capabilities, &lookups, database_id, capability_id)
}

fn capability_observation(
    transaction: &redb::WriteTransaction,
    capability_id: CapabilityId,
) -> Result<Option<TransactionCurrentCapabilityObservationV1>, StorageError> {
    Ok(capability_from_write(transaction, capability_id)?
        .as_ref()
        .map(TransactionCurrentCapabilityObservationV1::from_record))
}

fn resolve_capability_digests_write(
    transaction: &redb::WriteTransaction,
    candidates: &[CapabilityTokenDigest],
) -> Result<CapabilityLookupResult, StorageError> {
    let database_id = read_database_id(transaction)?;
    let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
    let lookups = transaction
        .open_table(CAPABILITY_TOKENS)
        .map_err(table_error)?;
    resolve_capability_digests_from_tables(&capabilities, &lookups, database_id, candidates)
}

fn resolve_capability_digests_from_tables<C, L>(
    capabilities: &C,
    lookups: &L,
    database_id: riffdb_types::DatabaseId,
    candidates: &[CapabilityTokenDigest],
) -> Result<CapabilityLookupResult, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    L: ReadableTable<&'static [u8], &'static [u8]>,
{
    if candidates.is_empty() {
        return Err(invariant());
    }
    if candidates.len() > MAX_READABLE_DIGEST_KEYS {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let mut ordered = candidates.to_vec();
    ordered.sort_unstable();
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invariant());
    }

    let mut matched = None;
    for digest in candidates {
        let key = encode_capability_token_key(*digest);
        let Some(value) = lookups
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            continue;
        };
        let lookup = decoded_value(decode_capability_token_lookup_v1(value.value())?);
        let record =
            capability_from_tables(capabilities, lookups, database_id, lookup.capability_id())?
                .ok_or_else(corrupt)?;
        if record.token_digest() != *digest {
            return Err(corrupt());
        }
        if matched.is_some() {
            return Ok(CapabilityLookupResult::MultipleMatches);
        }
        matched = Some(record);
    }
    Ok(matched.map_or(CapabilityLookupResult::NotFound, |record| {
        CapabilityLookupResult::Found(Box::new(record))
    }))
}

impl CapabilityReader for RedbOperationalPorts {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_readonly(&transaction)?;
        let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        let lookups = transaction
            .open_table(CAPABILITY_TOKENS)
            .map_err(table_error)?;
        capability_from_tables(&capabilities, &lookups, database_id, capability_id)
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_readonly(&transaction)?;
        let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        let lookups = transaction
            .open_table(CAPABILITY_TOKENS)
            .map_err(table_error)?;
        resolve_capability_digests_from_tables(&capabilities, &lookups, database_id, candidates)
    }
}

impl CapabilityInventoryReader for RedbOperationalPorts {
    fn scan_capabilities(
        &self,
        after: Option<CapabilityId>,
        limit: StorageScanLimit,
    ) -> Result<CapabilityInventoryPageV1, StorageError> {
        let transaction = self.begin_read()?;
        read_database_id_readonly(&transaction)?;
        let table = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut has_more = false;
        for entry in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let capability_id = decode_capability_key(key.value()).map_err(|_| corrupt())?;
            if after.is_some_and(|after| capability_id <= after) {
                continue;
            }
            let record = decoded_value(decode_capability_record_v1(value.value())?);
            if record.capability_id() != capability_id {
                return Err(corrupt());
            }
            let next_bytes = bytes
                .checked_add(record.semantic_bytes().map_err(|_| corrupt())?)
                .ok_or_else(corrupt)?;
            if records.len() == usize::from(limit.get()) || next_bytes > MAX_SCAN_PAGE_BYTES {
                has_more = true;
                break;
            }
            bytes = next_bytes;
            records.push(record);
        }
        CapabilityInventoryPageV1::new(after, limit, records, has_more).map_err(|_| corrupt())
    }
}

#[derive(Clone)]
enum ServiceLifecycle {
    Standalone,
    Started {
        record: StoredServiceAuditRecordV1,
        terminal: bool,
    },
}

fn service_common_matches(
    started: &StoredServiceAuditRecordV1,
    terminal: &StoredServiceAuditRecordV1,
) -> bool {
    started.request_id() == terminal.request_id()
        && started.operation() == terminal.operation()
        && started.principal() == terminal.principal()
        && started.ingress() == terminal.ingress()
        && started.targets() == terminal.targets()
        && started.approval_id() == terminal.approval_id()
        && (started.principal().is_some()
            || (terminal.phase() == ServiceAuditPhaseV1::Succeeded
                && terminal.link() == started.link()))
}

fn service_lifecycle<T, C, E>(
    table: &T,
    commits: &C,
    events: &E,
    request_id: RequestId,
    sequences: &[AdministrationSequence],
) -> Result<Option<ServiceLifecycle>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut lifecycle = None;
    for sequence in sequences {
        let StoredAdministrationAuditRecordV1::Service(record) =
            read_audit_record(table, commits, events, *sequence)?
        else {
            return Err(corrupt());
        };
        if record.request_id() != request_id {
            return Err(corrupt());
        }
        lifecycle = Some(match lifecycle {
            None if record.phase() == ServiceAuditPhaseV1::Started => ServiceLifecycle::Started {
                record,
                terminal: false,
            },
            None if record.principal().is_some()
                && record.link() == ServiceAuditLinkV1::None
                && matches!(
                    record.phase(),
                    ServiceAuditPhaseV1::Denied
                        | ServiceAuditPhaseV1::Cancelled
                        | ServiceAuditPhaseV1::Failed
                ) =>
            {
                ServiceLifecycle::Standalone
            }
            Some(ServiceLifecycle::Started {
                record: started,
                terminal: false,
            }) if record.phase() != ServiceAuditPhaseV1::Started
                && service_common_matches(&started, &record) =>
            {
                ServiceLifecycle::Started {
                    record: started,
                    terminal: true,
                }
            }
            _ => return Err(corrupt()),
        });
    }
    Ok(lifecycle)
}

fn service_lifecycle_in_write<T>(
    access: &RedbWriteAccess,
    table: &T,
    request_id: RequestId,
    sequences: &[AdministrationSequence],
) -> Result<Option<ServiceLifecycle>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    if sequences.is_empty() {
        return Ok(None);
    }
    let transaction = access.transaction()?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let mut lifecycle = None;
    for sequence in sequences {
        let key = encode_audit_key(*sequence);
        let physical = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| {
                decode_administration_audit_with_command_tables(value.value(), &commits, &events)
                    .map(decoded_value)
            })
            .transpose()?;
        let derived = access
            .command_audit_record(*sequence)?
            .map(StoredAdministrationAuditRecordV1::Service);
        let record = match (physical, derived) {
            (Some(physical), Some(derived)) if physical == derived => physical,
            (Some(record), None) | (None, Some(record)) => record,
            (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
        };
        let StoredAdministrationAuditRecordV1::Service(record) = record else {
            return Err(corrupt());
        };
        if record.request_id() != request_id {
            return Err(corrupt());
        }
        lifecycle = Some(match lifecycle {
            None if record.phase() == ServiceAuditPhaseV1::Started => ServiceLifecycle::Started {
                record,
                terminal: false,
            },
            None if record.principal().is_some()
                && record.link() == ServiceAuditLinkV1::None
                && matches!(
                    record.phase(),
                    ServiceAuditPhaseV1::Denied
                        | ServiceAuditPhaseV1::Cancelled
                        | ServiceAuditPhaseV1::Failed
                ) =>
            {
                ServiceLifecycle::Standalone
            }
            Some(ServiceLifecycle::Started {
                record: started,
                terminal: false,
            }) if record.phase() != ServiceAuditPhaseV1::Started
                && service_common_matches(&started, &record) =>
            {
                ServiceLifecycle::Started {
                    record: started,
                    terminal: true,
                }
            }
            _ => return Err(corrupt()),
        });
    }
    Ok(lifecycle)
}

fn service_lifecycle_in_access(
    access: &RedbWriteAccess,
    request_id: RequestId,
    sequences: &[AdministrationSequence],
) -> Result<Option<ServiceLifecycle>, StorageError> {
    let mut lifecycle = None;
    for sequence in sequences {
        let key = encode_audit_key(*sequence);
        let derived = access
            .command_audit_record(*sequence)?
            .map(StoredAdministrationAuditRecordV1::Service);
        let physical = access
            .read_command_value(JournalTable::Audit, key.as_slice())?
            .map(
                |value| match decode_administration_audit_record_v1(&value) {
                    Ok(record) => Ok(Some(decoded_value(record))),
                    Err(_)
                        if riffdb_storage_api::decode_command_audit_locator_v1(&value).is_ok() =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(error),
                },
            )
            .transpose()?
            .flatten();
        let record = match (physical, derived) {
            (Some(physical), Some(derived)) if physical == derived => physical,
            (Some(record), None) | (None, Some(record)) => record,
            (None, None) | (Some(_), Some(_)) => return Err(corrupt()),
        };
        let StoredAdministrationAuditRecordV1::Service(record) = record else {
            return Err(corrupt());
        };
        if record.request_id() != request_id {
            return Err(corrupt());
        }
        lifecycle = Some(match lifecycle {
            None if record.phase() == ServiceAuditPhaseV1::Started => ServiceLifecycle::Started {
                record,
                terminal: false,
            },
            None if record.principal().is_some()
                && record.link() == ServiceAuditLinkV1::None
                && matches!(
                    record.phase(),
                    ServiceAuditPhaseV1::Denied
                        | ServiceAuditPhaseV1::Cancelled
                        | ServiceAuditPhaseV1::Failed
                ) =>
            {
                ServiceLifecycle::Standalone
            }
            Some(ServiceLifecycle::Started {
                record: started,
                terminal: false,
            }) if record.phase() != ServiceAuditPhaseV1::Started
                && service_common_matches(&started, &record) =>
            {
                ServiceLifecycle::Started {
                    record: started,
                    terminal: true,
                }
            }
            _ => return Err(corrupt()),
        });
    }
    Ok(lifecycle)
}

fn service_link_is_valid(
    transaction: &redb::WriteTransaction,
    intent: &ServiceAuditAppendIntentV1,
) -> Result<bool, StorageError> {
    match intent.link() {
        ServiceAuditLinkV1::None => Ok(true),
        ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
            command_service_link_is_valid(
                &commits,
                &events,
                &provenance,
                commit_sequence,
                provenance_id,
            )
        }
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let Some(target) =
                find_audit_record(&audit, &commits, &events, administration_sequence)?
            else {
                return Ok(false);
            };
            Ok(match (intent.operation(), target) {
                (
                    ServiceOperationV1::DeployContract,
                    StoredAdministrationAuditRecordV1::Catalog(_),
                ) => true,
                (
                    ServiceOperationV1::DeployQueryModule,
                    StoredAdministrationAuditRecordV1::QueryModule(_),
                ) => true,
                (
                    ServiceOperationV1::DeployReactiveModule,
                    StoredAdministrationAuditRecordV1::ReactiveModule(_),
                ) => true,
                (
                    ServiceOperationV1::CreateCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    matches!(
                        record.operation(),
                        CapabilityAdministrationOperationV1::Bootstrap
                            | CapabilityAdministrationOperationV1::Create
                    ) && intent
                        .targets()
                        .as_slice()
                        .contains(&ServiceAuditTargetV1::Capability(
                            record.target_capability_id(),
                        ))
                }
                (
                    ServiceOperationV1::RevokeCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    record.operation() == CapabilityAdministrationOperationV1::Revoke
                        && intent
                            .targets()
                            .as_slice()
                            .contains(&ServiceAuditTargetV1::Capability(
                                record.target_capability_id(),
                            ))
                }
                _ => false,
            })
        }
    }
}

fn service_link_is_valid_access(
    access: &RedbWriteAccess,
    intent: &ServiceAuditAppendIntentV1,
) -> Result<bool, StorageError> {
    match intent.link() {
        ServiceAuditLinkV1::None => Ok(true),
        ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => {
            let provenance_key = encode_provenance_key(provenance_id);
            if let Some((segment, locator)) = access.command_derived_member(
                riffdb_storage_api::CommandDerivedIndexKindV1::Provenance,
                provenance_key.as_slice(),
            )? {
                if locator.member != riffdb_storage_api::CommandDerivedMemberV1::Command
                    || locator.member_ordinal != 0
                {
                    return Err(corrupt());
                }
                let command = segment
                    .commands()
                    .get(usize::from(locator.command_ordinal))
                    .ok_or_else(corrupt)?;
                return Ok(command.commit_sequence() == commit_sequence
                    && command.base().provenance().provenance_id() == provenance_id
                    && command.base().provenance().commit_sequence() == commit_sequence);
            }
            let Some(encoded_provenance) =
                access.read_command_value(JournalTable::Provenance, provenance_key.as_slice())?
            else {
                return Ok(false);
            };
            let provenance = decoded_value(decode_provenance_record_v1(&encoded_provenance)?);
            if provenance.provenance_id() != provenance_id
                || provenance.commit_sequence() != commit_sequence
            {
                return Ok(false);
            }
            let commit_key = encode_application_sequence_key(commit_sequence);
            Ok(access
                .read_command_value(JournalTable::Commits, commit_key.as_slice())?
                .is_some())
        }
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let key = encode_audit_key(administration_sequence);
            let Some(encoded) = access.read_command_value(JournalTable::Audit, key.as_slice())?
            else {
                return Ok(false);
            };
            let target = decoded_value(decode_administration_audit_record_v1(&encoded)?);
            Ok(match (intent.operation(), target) {
                (
                    ServiceOperationV1::DeployContract,
                    StoredAdministrationAuditRecordV1::Catalog(_),
                )
                | (
                    ServiceOperationV1::DeployQueryModule,
                    StoredAdministrationAuditRecordV1::QueryModule(_),
                )
                | (
                    ServiceOperationV1::DeployReactiveModule,
                    StoredAdministrationAuditRecordV1::ReactiveModule(_),
                ) => true,
                (
                    ServiceOperationV1::CreateCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    matches!(
                        record.operation(),
                        CapabilityAdministrationOperationV1::Bootstrap
                            | CapabilityAdministrationOperationV1::Create
                    ) && intent
                        .targets()
                        .as_slice()
                        .contains(&ServiceAuditTargetV1::Capability(
                            record.target_capability_id(),
                        ))
                }
                (
                    ServiceOperationV1::RevokeCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    record.operation() == CapabilityAdministrationOperationV1::Revoke
                        && intent
                            .targets()
                            .as_slice()
                            .contains(&ServiceAuditTargetV1::Capability(
                                record.target_capability_id(),
                            ))
                }
                _ => false,
            })
        }
    }
}

fn command_service_link_is_valid<C, E, P>(
    commits: &C,
    events: &E,
    provenance: &P,
    commit_sequence: riffdb_types::CommitSequence,
    provenance_id: riffdb_types::ProvenanceId,
) -> Result<bool, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
    P: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_application_sequence_key(commit_sequence);
    let Some(commit_guard) = commits
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let commit = decoded_value(decode_commit_with_event_table(
        commit_guard.value(),
        events,
    )?);
    drop(commit_guard);
    if commit.commit_sequence() != commit_sequence || commit.provenance_id() != provenance_id {
        return Ok(false);
    }
    let key = encode_provenance_key(provenance_id);
    let Some(provenance) = provenance
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let provenance = decoded_value(decode_provenance_record_v1(provenance.value())?);
    Ok(provenance.provenance_id() == provenance_id
        && provenance.commit_sequence() == commit_sequence)
}

impl ServiceAuditAppendRepository for RedbOperationalPorts {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        let mut results = self.append_service_audit_group(std::slice::from_ref(intent))?;
        results
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn append_service_audit_fused_pair(
        &mut self,
        started: &ServiceAuditAppendIntentV1,
        terminal: &ServiceAuditAppendIntentV1,
    ) -> Result<(), StorageError> {
        let access = self.begin_write()?;
        let _records =
            stage_service_audit_group_in_write(&access, &[started.clone(), terminal.clone()])?;
        access.commit_for(RedbTestOperation::ServiceAudit)?;
        Ok(())
    }

    fn append_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<Vec<ServiceAuditAppendResult>, StorageError> {
        if intents.is_empty()
            || intents.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
            || intents
                .iter()
                .map(ServiceAuditAppendIntentV1::request_id)
                .collect::<BTreeSet<_>>()
                .len()
                != intents.len()
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;
        let distinct_requests = intents
            .iter()
            .map(ServiceAuditAppendIntentV1::request_id)
            .collect::<BTreeSet<_>>();
        let sequences_by_request = access.service_audit_sequences_for(&distinct_requests)?;
        let mut allowed = Vec::with_capacity(intents.len());
        for intent in intents {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let sequences = sequences_by_request
                .get(&intent.request_id())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let lifecycle =
                service_lifecycle_in_write(&access, &audit, intent.request_id(), sequences)?;
            drop(audit);
            let phase_allowed = match lifecycle {
                None => match intent.phase() {
                    ServiceAuditPhaseV1::Started => {
                        intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None
                    }
                    ServiceAuditPhaseV1::Denied
                    | ServiceAuditPhaseV1::Cancelled
                    | ServiceAuditPhaseV1::Failed => {
                        intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None
                    }
                    _ => false,
                },
                Some(ServiceLifecycle::Standalone)
                | Some(ServiceLifecycle::Started { terminal: true, .. }) => false,
                Some(ServiceLifecycle::Started {
                    record: started,
                    terminal: false,
                }) => {
                    intent.phase() != ServiceAuditPhaseV1::Started
                        && started.request_id() == intent.request_id()
                        && started.operation() == intent.operation()
                        && started.principal() == intent.principal()
                        && started.ingress() == intent.ingress()
                        && started.targets() == intent.targets()
                        && started.approval_id() == intent.approval_id()
                        && (started.principal().is_some()
                            || (intent.phase() == ServiceAuditPhaseV1::Succeeded
                                && intent.link() == started.link()))
                }
            };
            allowed.push(phase_allowed && service_link_is_valid(transaction, intent)?);
        }

        let append_count = u16::try_from(allowed.iter().filter(|allowed| **allowed).count())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        if append_count == 0 {
            access.abort()?;
            return Ok(vec![ServiceAuditAppendResult::PhaseConflict; intents.len()]);
        }
        let (assigned, next) = allocate_sequences(allocator, append_count)?;
        let mut assigned = assigned.into_iter();
        let mut results = Vec::with_capacity(intents.len());
        for (intent, allowed) in intents.iter().zip(allowed) {
            if !allowed {
                results.push(ServiceAuditAppendResult::PhaseConflict);
                continue;
            }
            let sequence = assigned
                .next()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let record = StoredServiceAuditRecordV1::from_intent(sequence, intent);
            append_audit_record(
                transaction,
                &StoredAdministrationAuditRecordV1::Service(record.clone()),
            )?;
            results.push(ServiceAuditAppendResult::Appended(record));
        }
        if assigned.next().is_some() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::ServiceAudit)?;
        Ok(results)
    }

    fn submit_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<riffdb_storage_api::ServiceAuditGroupAppend, StorageError> {
        if !self.standard_writer_journal_enabled() {
            return self
                .append_service_audit_group(intents)
                .map(riffdb_storage_api::ServiceAuditGroupAppend::Complete);
        }
        if intents.is_empty()
            || intents.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
            || intents
                .iter()
                .map(ServiceAuditAppendIntentV1::request_id)
                .collect::<BTreeSet<_>>()
                .len()
                != intents.len()
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let access = self.begin_deferred_service_audit_write()?;
        validate_administration_tail(&access)?;
        let distinct_requests = intents
            .iter()
            .map(ServiceAuditAppendIntentV1::request_id)
            .collect::<BTreeSet<_>>();
        let sequences_by_request = access.service_audit_sequences_for(&distinct_requests)?;
        let mut allowed = Vec::with_capacity(intents.len());
        for intent in intents {
            let sequences = sequences_by_request
                .get(&intent.request_id())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let lifecycle = service_lifecycle_in_access(&access, intent.request_id(), sequences)?;
            let phase_allowed = match lifecycle {
                None => match intent.phase() {
                    ServiceAuditPhaseV1::Started => {
                        intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None
                    }
                    ServiceAuditPhaseV1::Denied
                    | ServiceAuditPhaseV1::Cancelled
                    | ServiceAuditPhaseV1::Failed => {
                        intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None
                    }
                    _ => false,
                },
                Some(ServiceLifecycle::Standalone)
                | Some(ServiceLifecycle::Started { terminal: true, .. }) => false,
                Some(ServiceLifecycle::Started {
                    record: started,
                    terminal: false,
                }) => {
                    intent.phase() != ServiceAuditPhaseV1::Started
                        && started.request_id() == intent.request_id()
                        && started.operation() == intent.operation()
                        && started.principal() == intent.principal()
                        && started.ingress() == intent.ingress()
                        && started.targets() == intent.targets()
                        && started.approval_id() == intent.approval_id()
                        && (started.principal().is_some()
                            || (intent.phase() == ServiceAuditPhaseV1::Succeeded
                                && intent.link() == started.link()))
                }
            };
            allowed.push(phase_allowed && service_link_is_valid_access(&access, intent)?);
        }
        let selected = intents
            .iter()
            .zip(&allowed)
            .filter(|(_, allowed)| **allowed)
            .map(|(intent, _)| intent.clone())
            .collect::<Vec<_>>();
        if selected.is_empty() {
            access.abort()?;
            return Ok(riffdb_storage_api::ServiceAuditGroupAppend::Complete(
                vec![ServiceAuditAppendResult::PhaseConflict; intents.len()],
            ));
        }
        let records = stage_checked_standalone_service_audit_group_in_write(&access, &selected)?;
        if records.len() != selected.len() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let mut records = records.into_iter();
        let mut results = Vec::with_capacity(intents.len());
        for allowed in allowed {
            if allowed {
                let record = records
                    .next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                results.push(ServiceAuditAppendResult::Appended(record));
            } else {
                results.push(ServiceAuditAppendResult::PhaseConflict);
            }
        }
        if records.next().is_some() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        access
            .submit_service_audit(results)
            .map(|fence| riffdb_storage_api::ServiceAuditGroupAppend::Submitted(Box::new(fence)))
    }
}

impl AuditedAdmissionRepository for RedbOperationalPorts {
    fn admit_or_resolve_audited_group(
        &self,
        requests: Vec<AuditedAdmissionRequestV1>,
    ) -> Result<Vec<AuditedAdmissionResultV1>, StorageError> {
        let request_ids = requests
            .iter()
            .map(|request| request.started().request_id())
            .collect::<BTreeSet<_>>();
        if requests.is_empty()
            || requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
            || request_ids.len() != requests.len()
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }

        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;
        let sequences_by_request = access.service_audit_sequences_for(&request_ids)?;
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;

        for request in &requests {
            let sequences = sequences_by_request
                .get(&request.started().request_id())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let lifecycle = service_lifecycle_in_write(
                &access,
                &audit,
                request.started().request_id(),
                sequences,
            )?;
            if lifecycle.is_some()
                || request.started().phase() != ServiceAuditPhaseV1::Started
                || request.started().principal().is_none()
                || request.started().link() != ServiceAuditLinkV1::None
                || !service_link_is_valid(transaction, request.started())?
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        drop(audit);

        let count = u16::try_from(requests.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let (assigned, next) = allocate_sequences(allocator, count)?;
        let admissions =
            stage_admission_group(&access, requests.iter().map(|request| request.admission()))?;
        let mut outputs = Vec::with_capacity(requests.len());
        for ((request, sequence), (admission, created)) in
            requests.iter().zip(assigned).zip(admissions)
        {
            if created
                && request.started().request_id()
                    != request
                        .admission()
                        .proposed_pending()
                        .admission_request_id()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let started = StoredServiceAuditRecordV1::from_intent(sequence, request.started());
            append_audit_record(
                transaction,
                &StoredAdministrationAuditRecordV1::Service(started.clone()),
            )?;
            outputs.push(
                AuditedAdmissionResultV1::new(admission, started)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?,
            );
        }
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::Admission)?;
        Ok(outputs)
    }
}

/// Stages one all-or-nothing terminal-audit group in an existing authoritative
/// command transaction.
///
/// The administration stream tail and allocator are validated once for the
/// complete bounded group. Each lifecycle and authoritative result link is
/// still checked independently before any audit row is appended.
pub(crate) fn stage_service_audit_group_in_write(
    access: &crate::store::RedbWriteAccess,
    intents: &[ServiceAuditAppendIntentV1],
) -> Result<Vec<StoredServiceAuditRecordV1>, StorageError> {
    stage_service_audit_group_with_command_evidence(access, intents.to_vec(), None)
}

/// Writes a nonempty subset whose independent lifecycle and link checks were
/// just completed against this same write transaction by the standalone group
/// adapter. Unlike the command helper, a lone `Started` is a complete
/// standalone transition and is not required to have a terminal sibling.
fn stage_checked_standalone_service_audit_group_in_write(
    access: &crate::store::RedbWriteAccess,
    intents: &[ServiceAuditAppendIntentV1],
) -> Result<Vec<StoredServiceAuditRecordV1>, StorageError> {
    if intents.is_empty()
        || intents.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        || intents
            .iter()
            .map(ServiceAuditAppendIntentV1::request_id)
            .collect::<BTreeSet<_>>()
            .len()
            != intents.len()
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let allocator = validate_administration_tail(access)?;
    let count =
        u16::try_from(intents.len()).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let (assigned, next) = allocate_sequences(allocator, count)?;
    let mut records = Vec::with_capacity(intents.len());
    for (intent, sequence) in intents.iter().zip(assigned) {
        let record = StoredServiceAuditRecordV1::from_intent(sequence, intent);
        let encoded = encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(record.clone()),
        )?;
        let key = encode_audit_key(sequence);
        if access
            .put_command_value(
                JournalTable::Audit,
                key.to_vec(),
                encoded.as_bytes().to_vec(),
            )?
            .is_some()
        {
            return Err(corrupt());
        }
        records.push(record);
    }
    for record in &records {
        let key = crate::keys::encode_audit_by_request_key(
            record.request_id(),
            record.administration_sequence(),
        );
        let value = crate::codec::encode_service_audit_request_index_v1(
            riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(
                record.request_id(),
                record.administration_sequence(),
            ),
        )?;
        if access
            .put_command_value(
                JournalTable::AuditByRequest,
                key.to_vec(),
                value.as_bytes().to_vec(),
            )?
            .is_some()
        {
            return Err(corrupt());
        }
    }
    let prior_allocator = encode_administration_sequence_allocator_v1(allocator)?;
    let encoded_allocator = encode_administration_sequence_allocator_v1(next)?;
    let prior = access.put_command_value(
        JournalTable::Meta,
        META_ADMINISTRATION_SEQUENCE.as_bytes().to_vec(),
        encoded_allocator.as_bytes().to_vec(),
    )?;
    if prior.as_deref() != Some(prior_allocator.as_bytes()) {
        return Err(corrupt());
    }
    Ok(records)
}

/// Stages linked command audit rows using evidence that can only be produced
/// by consuming complete command graphs already written to this transaction.
pub(crate) struct StagedCommandAuditRecordsV1 {
    started: StoredServiceAuditRecordV1,
    terminal: StoredServiceAuditRecordV1,
}

impl StagedCommandAuditRecordsV1 {
    pub(crate) fn into_parts(self) -> (StoredServiceAuditRecordV1, StoredServiceAuditRecordV1) {
        (self.started, self.terminal)
    }
}

pub(crate) fn stage_command_service_audit_group_in_write(
    access: &crate::store::RedbWriteAccess,
    transitions: Vec<CommandServiceAuditTransitionV1>,
    staged_commands: &[StagedCommandAuditLinkEvidenceV1],
) -> Result<Vec<StagedCommandAuditRecordsV1>, StorageError> {
    if transitions.is_empty()
        || transitions.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        || transitions.len() != staged_commands.len()
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }

    // Command transitions are already a closed typed lifecycle. Retain the
    // durable request lookup here so a fresh fused start cannot reuse an audit
    // identity, but avoid flattening the transitions and rebuilding a generic
    // lifecycle state machine for every command in the group.
    let request_ids = transitions
        .iter()
        .map(|transition| transition.terminal().request_id())
        .collect::<BTreeSet<_>>();
    if request_ids.len() != transitions.len() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let sequences_by_request = access.service_audit_sequences_for(&request_ids)?;
    let allocator = validate_administration_tail(access)?;
    let mut prior_starts = Vec::with_capacity(transitions.len());
    for (transition, evidence) in transitions.iter().zip(staged_commands) {
        let terminal = transition.terminal();
        let ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } = terminal.link()
        else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        if !evidence.matches(commit_sequence, provenance_id) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let sequences = sequences_by_request
            .get(&terminal.request_id())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let lifecycle = if sequences.is_empty() {
            None
        } else {
            service_lifecycle_in_access(access, terminal.request_id(), sequences)?
        };
        match (transition, lifecycle) {
            (CommandServiceAuditTransitionV1::StartedAndTerminal { started, terminal }, None) => {
                if started.timestamp() > terminal.timestamp() {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                prior_starts.push(None);
            }
            (
                CommandServiceAuditTransitionV1::TerminalOnly(terminal),
                Some(ServiceLifecycle::Started {
                    record: started,
                    terminal: false,
                }),
            ) if started.request_id() == terminal.request_id()
                && started.operation() == terminal.operation()
                && started.principal() == terminal.principal()
                && started.ingress() == terminal.ingress()
                && started.targets() == terminal.targets()
                && started.approval_id() == terminal.approval_id()
                && started.principal().is_some()
                && started.timestamp() <= terminal.timestamp() =>
            {
                prior_starts.push(Some(started));
            }
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    let row_count = transitions
        .iter()
        .try_fold(0_usize, |count, transition| {
            count.checked_add(match transition {
                CommandServiceAuditTransitionV1::TerminalOnly(_) => 1,
                CommandServiceAuditTransitionV1::StartedAndTerminal { .. } => 2,
            })
        })
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let count =
        u16::try_from(row_count).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let (assigned, next) = allocate_sequences(allocator, count)?;
    let mut assigned = assigned.into_iter();
    let mut records = Vec::with_capacity(transitions.len());
    for (transition, prior_start) in transitions.into_iter().zip(prior_starts) {
        match transition {
            CommandServiceAuditTransitionV1::TerminalOnly(terminal) => {
                let sequence = assigned
                    .next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                let terminal = StoredServiceAuditRecordV1::from_owned_intent(sequence, terminal);
                records.push(StagedCommandAuditRecordsV1 {
                    started: prior_start
                        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
                    terminal,
                });
            }
            CommandServiceAuditTransitionV1::StartedAndTerminal { started, terminal } => {
                if prior_start.is_some() {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                let started_sequence = assigned
                    .next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                let terminal_sequence = assigned
                    .next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                records.push(StagedCommandAuditRecordsV1 {
                    started: StoredServiceAuditRecordV1::from_owned_intent(
                        started_sequence,
                        started,
                    ),
                    terminal: StoredServiceAuditRecordV1::from_owned_intent(
                        terminal_sequence,
                        terminal,
                    ),
                });
            }
        }
    }
    if assigned.next().is_some() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }

    write_administration_allocator_in_access(access, allocator, next)?;
    Ok(records)
}

fn stage_service_audit_group_with_command_evidence(
    access: &crate::store::RedbWriteAccess,
    intents: Vec<ServiceAuditAppendIntentV1>,
    staged_commands: Option<&[StagedCommandAuditLinkEvidenceV1]>,
) -> Result<Vec<StoredServiceAuditRecordV1>, StorageError> {
    let command_owned_audits = staged_commands.is_some();
    let maximum_rows = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        .checked_mul(2)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let distinct_requests = intents
        .iter()
        .map(ServiceAuditAppendIntentV1::request_id)
        .collect::<BTreeSet<_>>();
    if intents.is_empty()
        || intents.len() > maximum_rows
        || distinct_requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let sequences_by_request = access.service_audit_sequences_for(&distinct_requests)?;
    let transaction = access.transaction()?;
    let allocator = validate_administration_tail(access)?;
    let mut audit = transaction.open_table(AUDIT).map_err(table_error)?;
    // Independently submitted links retain the complete table decode. The
    // command path instead supplies move-only evidence produced only after the
    // complete graph was checked, encoded, and inserted in this transaction.
    let authoritative_command_tables = if staged_commands.is_none()
        && intents
            .iter()
            .any(|intent| matches!(intent.link(), ServiceAuditLinkV1::Command { .. }))
    {
        Some((
            transaction.open_table(COMMITS).map_err(table_error)?,
            transaction.open_table(EVENTS).map_err(table_error)?,
            transaction.open_table(PROVENANCE).map_err(table_error)?,
        ))
    } else {
        None
    };
    let mut staged_commands = staged_commands.unwrap_or(&[]).iter();
    let mut fused_starts: BTreeMap<riffdb_types::RequestId, usize> = BTreeMap::new();
    for (intent_index, intent) in intents.iter().enumerate() {
        let sequences = sequences_by_request
            .get(&intent.request_id())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let lifecycle = service_lifecycle_in_write(access, &audit, intent.request_id(), sequences)?;
        let had_fused_start = fused_starts.contains_key(&intent.request_id());
        let allowed = if let Some(started_index) = fused_starts.get(&intent.request_id()) {
            let started = &intents[*started_index];
            intent.phase() != ServiceAuditPhaseV1::Started
                && started.request_id() == intent.request_id()
                && started.operation() == intent.operation()
                && started.principal() == intent.principal()
                && started.ingress() == intent.ingress()
                && started.targets() == intent.targets()
                && started.approval_id() == intent.approval_id()
                && started.timestamp() <= intent.timestamp()
        } else {
            match lifecycle {
                Some(ServiceLifecycle::Started {
                    record: started,
                    terminal: false,
                }) => {
                    intent.phase() != ServiceAuditPhaseV1::Started
                        && started.request_id() == intent.request_id()
                        && started.operation() == intent.operation()
                        && started.principal() == intent.principal()
                        && started.ingress() == intent.ingress()
                        && started.targets() == intent.targets()
                        && started.approval_id() == intent.approval_id()
                        && started.principal().is_some()
                }
                None if intent.phase() == ServiceAuditPhaseV1::Started => {
                    fused_starts.insert(intent.request_id(), intent_index);
                    true
                }
                None
                | Some(ServiceLifecycle::Standalone)
                | Some(ServiceLifecycle::Started { .. }) => false,
            }
        };
        if had_fused_start && allowed {
            fused_starts.remove(&intent.request_id());
        }
        let link_is_valid = match intent.link() {
            ServiceAuditLinkV1::Command {
                commit_sequence,
                provenance_id,
            } => {
                if let Some(evidence) = staged_commands.next() {
                    evidence.matches(commit_sequence, provenance_id)
                } else if let Some((commits, events, provenance)) =
                    authoritative_command_tables.as_ref()
                {
                    command_service_link_is_valid(
                        commits,
                        events,
                        provenance,
                        commit_sequence,
                        provenance_id,
                    )?
                } else {
                    false
                }
            }
            ServiceAuditLinkV1::None | ServiceAuditLinkV1::ControlPlane { .. } => {
                service_link_is_valid(transaction, intent)?
            }
        };
        if !allowed || !link_is_valid {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    if !fused_starts.is_empty() || staged_commands.next().is_some() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let count =
        u16::try_from(intents.len()).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let (assigned, next) = allocate_sequences(allocator, count)?;
    let mut records = Vec::with_capacity(intents.len());
    let mut journal_mutations =
        Vec::with_capacity(intents.len().saturating_mul(2).saturating_add(1));
    for (intent, sequence) in intents.into_iter().zip(assigned) {
        let record = StoredServiceAuditRecordV1::from_owned_intent(sequence, intent);
        if !command_owned_audits {
            let encoded = encode_administration_audit_record_v1(
                &StoredAdministrationAuditRecordV1::Service(record.clone()),
            )?;
            let key = encode_audit_key(sequence);
            if audit
                .insert(key.as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(corrupt());
            }
            journal_mutations.push(
                JournalMutation::put(
                    JournalTable::Audit,
                    key.to_vec(),
                    encoded.as_bytes().to_vec(),
                )
                .map_err(|_| invariant())?,
            );
        }
        records.push(record);
    }
    drop(audit);
    drop(authoritative_command_tables);
    // Successful command request locators are rebuilt exactly from the owning
    // segment manifest. Standalone service-audit rows retain their durable
    // request index because no command segment owns them.
    if !command_owned_audits {
        let mut request_index = transaction
            .open_table(crate::layout::AUDIT_BY_REQUEST)
            .map_err(table_error)?;
        for record in &records {
            let index_key = crate::keys::encode_audit_by_request_key(
                record.request_id(),
                record.administration_sequence(),
            );
            let index_value = crate::codec::encode_service_audit_request_index_v1(
                riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(
                    record.request_id(),
                    record.administration_sequence(),
                ),
            )?;
            if request_index
                .insert(index_key.as_slice(), index_value.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(corrupt());
            }
            journal_mutations.push(
                JournalMutation::put(
                    JournalTable::AuditByRequest,
                    index_key.to_vec(),
                    index_value.as_bytes().to_vec(),
                )
                .map_err(|_| invariant())?,
            );
        }
        drop(request_index);
    }
    let prior_allocator = encode_administration_sequence_allocator_v1(allocator)?;
    let encoded_allocator = encode_administration_sequence_allocator_v1(next)?;
    write_administration_allocator(transaction, allocator, next)?;
    journal_mutations.push(
        JournalMutation::replace(
            JournalTable::Meta,
            META_ADMINISTRATION_SEQUENCE.as_bytes().to_vec(),
            prior_allocator.as_bytes(),
            encoded_allocator.as_bytes().to_vec(),
        )
        .map_err(|_| invariant())?,
    );
    access.record_journal_mutations(journal_mutations)?;
    Ok(records)
}

fn principal_matches_observation(
    principal: &AuditPrincipalV1,
    observation: Option<&TransactionCurrentCapabilityObservationV1>,
) -> bool {
    observation.is_some_and(|current| {
        current.capability_id() == principal.capability_id()
            && current.revision() == principal.capability_revision()
            && current.principal_id() == principal.principal_id()
            && current.actor_kind() == principal.actor_kind()
    })
}

fn current_create_state(
    transaction: &redb::WriteTransaction,
    candidate: &CapabilityCreateCandidateV1,
) -> Result<CapabilityMutationCurrentStateV1, StorageError> {
    CapabilityMutationCurrentStateV1::for_create(
        candidate,
        capability_observation(transaction, candidate.initiator().capability_id())?,
        capability_observation(transaction, candidate.capability_id())?,
    )
    .map_err(|_| corrupt())
}

fn current_revoke_state(
    transaction: &redb::WriteTransaction,
    candidate: &CapabilityRevokeCandidateV1,
) -> Result<CapabilityMutationCurrentStateV1, StorageError> {
    CapabilityMutationCurrentStateV1::for_revoke(
        candidate,
        capability_observation(transaction, candidate.initiator().capability_id())?,
        capability_observation(transaction, candidate.capability_id())?,
    )
    .map_err(|_| corrupt())
}

/// Redb create candidate holding the exclusive write transaction.
pub struct RedbCapabilityCreateCandidate {
    access: RedbWriteAccess,
    candidate: CapabilityCreateCandidateV1,
}

/// Redb create candidate after transaction-current facts have been frozen.
pub struct RedbCapabilityCreateAwaiting {
    access: RedbWriteAccess,
    candidate: CapabilityCreateCandidateV1,
    current: CapabilityMutationCurrentStateV1,
}

/// Redb revoke candidate holding the exclusive write transaction.
pub struct RedbCapabilityRevokeCandidate {
    access: RedbWriteAccess,
    candidate: CapabilityRevokeCandidateV1,
}

/// Redb revoke candidate after transaction-current facts have been frozen.
pub struct RedbCapabilityRevokeAwaiting {
    access: RedbWriteAccess,
    candidate: CapabilityRevokeCandidateV1,
    current: CapabilityMutationCurrentStateV1,
}

impl CapabilityAdministrationTransactionPort for RedbOperationalPorts {
    type CreateCandidate = RedbCapabilityCreateCandidate;
    type RevokeCandidate = RedbCapabilityRevokeCandidate;

    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError> {
        Ok(RedbCapabilityCreateCandidate {
            access: self.begin_write()?,
            candidate,
        })
    }

    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError> {
        Ok(RedbCapabilityRevokeCandidate {
            access: self.begin_write()?,
            candidate,
        })
    }
}

impl CapabilityCreateCandidateTransaction for RedbCapabilityCreateCandidate {
    type AwaitingDecision = RedbCapabilityCreateAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError> {
        let current = current_create_state(self.access.transaction()?, &self.candidate)?;
        Ok((
            RedbCapabilityCreateAwaiting {
                access: self.access,
                candidate: self.candidate,
                current: current.clone(),
            },
            current,
        ))
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.candidate
    }
}

impl CapabilityCreateAwaitingDecision for RedbCapabilityCreateAwaiting {
    fn commit_create(
        self,
        intent: CapabilityCreateIntentV1,
    ) -> Result<CapabilityCreateResult, StorageError> {
        if !intent.matches_candidate(&self.candidate)
            || !principal_matches_observation(intent.initiator(), self.current.authorizing())
        {
            return Err(invariant());
        }
        let observed = current_create_state(self.access.transaction()?, &self.candidate)?;
        if observed != self.current {
            return Err(invariant());
        }
        commit_capability_create(self.access, &intent)
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.candidate
    }
}

fn commit_capability_create(
    access: RedbWriteAccess,
    intent: &CapabilityCreateIntentV1,
) -> Result<CapabilityCreateResult, StorageError> {
    let transaction = access.transaction()?;
    let allocator = validate_administration_tail(&access)?;
    if intent.requested().database_id() != read_database_id(transaction)? {
        return Err(invariant());
    }
    if let Some(record) = capability_from_write(transaction, intent.capability_id())? {
        let result = if record.matches_requested(intent.requested()) {
            CapabilityCreateResult::AlreadyCreated {
                capability_id: record.capability_id(),
                revision: record.revision(),
                administration_sequence: record.creation_sequence(),
            }
        } else {
            CapabilityCreateResult::CapabilityIdConflict
        };
        access.abort()?;
        return Ok(result);
    }
    match resolve_capability_digests_write(transaction, &[intent.token_digest()])? {
        CapabilityLookupResult::NotFound => {}
        CapabilityLookupResult::Found(_) => {
            access.abort()?;
            return Ok(CapabilityCreateResult::TokenDigestCollision);
        }
        CapabilityLookupResult::MultipleMatches => return Err(corrupt()),
    }

    let (assigned, next) = allocate_sequences(allocator, 1)?;
    let sequence = assigned[0];
    let capability = StoredCapabilityRecordV1::active(
        intent.capability_id(),
        intent.token_digest(),
        intent.requested().clone(),
        intent.issued_at(),
        intent.expires_at(),
        sequence,
        intent.request_id(),
    )
    .map_err(|_| invariant())?;
    let audit = StoredCapabilityAdministrationV1::new(
        sequence,
        intent.request_id(),
        CapabilityAdministrationOperationV1::Create,
        intent.issued_at(),
        Some(intent.initiator().clone()),
        intent.capability_id(),
        NonZeroU64::MIN,
        intent.approval_id().cloned(),
        None,
    )
    .map_err(|_| invariant())?;
    let encoded_capability = encode_capability_record_v1(&capability)?;
    let lookup = CapabilityTokenLookupV1::new(intent.capability_id());
    let encoded_lookup = encode_capability_token_lookup_v1(lookup)?;

    {
        let key = encode_capability_key(intent.capability_id());
        let mut table = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        if table
            .insert(key.as_slice(), encoded_capability.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(invariant());
        }
    }
    {
        let key = encode_capability_token_key(intent.token_digest());
        let mut table = transaction
            .open_table(CAPABILITY_TOKENS)
            .map_err(table_error)?;
        if table
            .insert(key.as_slice(), encoded_lookup.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(invariant());
        }
    }
    append_audit_record(
        transaction,
        &StoredAdministrationAuditRecordV1::Capability(audit),
    )?;
    write_administration_allocator(transaction, allocator, next)?;
    access.commit_for(RedbTestOperation::CapabilityAdministration)?;
    Ok(CapabilityCreateResult::Created {
        capability_id: intent.capability_id(),
        revision: NonZeroU64::MIN,
        administration_sequence: sequence,
    })
}

impl CapabilityRevokeCandidateTransaction for RedbCapabilityRevokeCandidate {
    type AwaitingDecision = RedbCapabilityRevokeAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError> {
        let current = current_revoke_state(self.access.transaction()?, &self.candidate)?;
        Ok((
            RedbCapabilityRevokeAwaiting {
                access: self.access,
                candidate: self.candidate,
                current: current.clone(),
            },
            current,
        ))
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.candidate
    }
}

impl CapabilityRevokeAwaitingDecision for RedbCapabilityRevokeAwaiting {
    fn commit_revoke(
        self,
        intent: CapabilityRevokeIntentV1,
    ) -> Result<CapabilityRevokeResult, StorageError> {
        if !intent.matches_candidate(&self.candidate)
            || !principal_matches_observation(intent.initiator(), self.current.authorizing())
            || self
                .current
                .target()
                .is_some_and(|target| target.revision() != intent.expected_revision())
        {
            return Err(invariant());
        }
        let observed = current_revoke_state(self.access.transaction()?, &self.candidate)?;
        if observed != self.current {
            return Err(invariant());
        }
        commit_capability_revoke(self.access, &intent)
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.candidate
    }
}

fn commit_capability_revoke(
    access: RedbWriteAccess,
    intent: &CapabilityRevokeIntentV1,
) -> Result<CapabilityRevokeResult, StorageError> {
    let transaction = access.transaction()?;
    let allocator = validate_administration_tail(&access)?;
    let Some(record) = capability_from_write(transaction, intent.capability_id())? else {
        access.abort()?;
        return Ok(CapabilityRevokeResult::CapabilityNotFound);
    };
    if record.revision() != intent.expected_revision() {
        return Err(invariant());
    }
    if let CapabilityLifecycleV1::Revoked {
        administration_sequence,
        ..
    } = record.lifecycle()
    {
        let result = CapabilityRevokeResult::AlreadyRevoked {
            capability_id: record.capability_id(),
            revision: record.revision(),
            administration_sequence: *administration_sequence,
        };
        access.abort()?;
        return Ok(result);
    }

    let (assigned, next) = allocate_sequences(allocator, 1)?;
    let sequence = assigned[0];
    let revoked = record
        .revoked(
            intent.expected_revision(),
            intent.revoked_at(),
            sequence,
            intent.reason(),
        )
        .map_err(|_| invariant())?;
    let audit = StoredCapabilityAdministrationV1::new(
        sequence,
        intent.request_id(),
        CapabilityAdministrationOperationV1::Revoke,
        intent.revoked_at(),
        Some(intent.initiator().clone()),
        intent.capability_id(),
        revoked.revision(),
        intent.approval_id().cloned(),
        Some(intent.reason()),
    )
    .map_err(|_| invariant())?;
    let encoded = encode_capability_record_v1(&revoked)?;
    {
        let key = encode_capability_key(intent.capability_id());
        let mut table = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        let prior = table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?
            .ok_or_else(invariant)?;
        let prior = decoded_value(decode_capability_record_v1(prior.value())?);
        if prior != record {
            return Err(invariant());
        }
    }
    append_audit_record(
        transaction,
        &StoredAdministrationAuditRecordV1::Capability(audit),
    )?;
    write_administration_allocator(transaction, allocator, next)?;
    access.commit_for(RedbTestOperation::CapabilityAdministration)?;
    Ok(CapabilityRevokeResult::Revoked {
        capability_id: intent.capability_id(),
        revision: revoked.revision(),
        administration_sequence: sequence,
    })
}

fn read_bootstrap_marker(
    transaction: &redb::WriteTransaction,
) -> Result<Option<CapabilityBootstrapMarkerV1>, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    table
        .get(META_CAPABILITY_BOOTSTRAP)
        .map_err(precommit_storage_error)?
        .map(|value| decode_capability_bootstrap_marker_v1(value.value()).map(decoded_value))
        .transpose()
}

fn table_is_empty<T>(table: &T) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    Ok(table.len().map_err(precommit_storage_error)? == 0)
}

fn validate_bootstrap_graph(
    access: &RedbWriteAccess,
    marker: CapabilityBootstrapMarkerV1,
) -> Result<StoredCapabilityRecordV1, StorageError> {
    let transaction = access.transaction()?;
    if marker.database_id() != read_database_id(transaction)? {
        return Err(corrupt());
    }
    let capability =
        capability_from_write(transaction, marker.capability_id())?.ok_or_else(corrupt)?;
    if capability.creation_sequence() != marker.administration_sequence() {
        return Err(corrupt());
    }
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let StoredAdministrationAuditRecordV1::Capability(transition) =
        read_audit_record(&audit, &commits, &events, marker.administration_sequence())?
    else {
        return Err(corrupt());
    };
    if transition.operation() != CapabilityAdministrationOperationV1::Bootstrap
        || transition.request_id() != capability.creation_request_id()
        || transition.timestamp() != capability.issued_at()
        || transition.target_capability_id() != capability.capability_id()
        || transition.resulting_revision().get() != 1
    {
        return Err(corrupt());
    }
    let started_sequence = marker
        .administration_sequence()
        .get()
        .checked_sub(1)
        .and_then(AdministrationSequence::new)
        .ok_or_else(corrupt)?;
    let StoredAdministrationAuditRecordV1::Service(started) =
        read_audit_record(&audit, &commits, &events, started_sequence)?
    else {
        return Err(corrupt());
    };
    let sequences = access.service_audit_sequences(started.request_id())?;
    let lifecycle_valid = matches!(
        service_lifecycle(
            &audit,
            &commits,
            &events,
            started.request_id(),
            &sequences,
        )?,
        Some(ServiceLifecycle::Started { record, .. }) if record == started
    );
    if started.request_id() != transition.request_id()
        || started.timestamp() != transition.timestamp()
        || started.operation() != ServiceOperationV1::CreateCapability
        || started.phase() != ServiceAuditPhaseV1::Started
        || started.principal().is_some()
        || transition.initiator().is_some()
        || started.approval_id() != transition.approval_id()
        || started.link()
            != (ServiceAuditLinkV1::ControlPlane {
                administration_sequence: marker.administration_sequence(),
            })
        || !started
            .targets()
            .as_slice()
            .contains(&ServiceAuditTargetV1::Capability(marker.capability_id()))
        || !lifecycle_valid
    {
        return Err(corrupt());
    }
    Ok(capability)
}

impl CapabilityBootstrapAdministrationRepository for RedbOperationalPorts {
    fn bootstrap_capability(
        &mut self,
        intent: &CapabilityBootstrapIntentV1,
    ) -> Result<CapabilityBootstrapResult, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(&access)?;
        let database_id = read_database_id(transaction)?;
        if intent.requested().database_id() != database_id
            || !intent
                .start()
                .targets()
                .as_slice()
                .contains(&ServiceAuditTargetV1::Capability(intent.capability_id()))
        {
            access.abort()?;
            return Ok(CapabilityBootstrapResult::BootstrapConflict);
        }

        if let Some(marker) = read_bootstrap_marker(transaction)? {
            return bootstrap_replay(access, allocator, intent, marker);
        }

        let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
        let capabilities_empty = table_is_empty(&capabilities)?;
        drop(capabilities);
        let lookups = transaction
            .open_table(CAPABILITY_TOKENS)
            .map_err(table_error)?;
        let lookups_empty = table_is_empty(&lookups)?;
        drop(lookups);
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        let audit_empty = table_is_empty(&audit)?;
        drop(audit);
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let commits_empty = table_is_empty(&commits)?;
        drop(commits);
        let active_empty = read_active_catalog_write(transaction)?.is_none();
        if !(capabilities_empty && lookups_empty && audit_empty && commits_empty && active_empty) {
            access.abort()?;
            return Ok(CapabilityBootstrapResult::BootstrapConflict);
        }

        let (assigned, next) = allocate_sequences(allocator, 2)?;
        let started_sequence = assigned[0];
        let transition_sequence = assigned[1];
        let started = StoredServiceAuditRecordV1::from_bootstrap_start(
            started_sequence,
            intent.start(),
            transition_sequence,
        )
        .map_err(|_| invariant())?;
        let capability = StoredCapabilityRecordV1::active(
            intent.capability_id(),
            intent.digests().current_write(),
            intent.requested().clone(),
            intent.issued_at(),
            intent.expires_at(),
            transition_sequence,
            intent.start().request_id(),
        )
        .map_err(|_| invariant())?;
        let capability_audit = StoredCapabilityAdministrationV1::new(
            transition_sequence,
            intent.start().request_id(),
            CapabilityAdministrationOperationV1::Bootstrap,
            intent.issued_at(),
            None,
            intent.capability_id(),
            NonZeroU64::MIN,
            intent.start().approval_id().cloned(),
            None,
        )
        .map_err(|_| invariant())?;
        let marker = CapabilityBootstrapMarkerV1::new(
            database_id,
            intent.capability_id(),
            transition_sequence,
        );
        let encoded_capability = encode_capability_record_v1(&capability)?;
        let encoded_lookup = encode_capability_token_lookup_v1(CapabilityTokenLookupV1::new(
            intent.capability_id(),
        ))?;
        let encoded_marker = encode_capability_bootstrap_marker_v1(marker)?;

        {
            let key = encode_capability_key(intent.capability_id());
            let mut table = transaction.open_table(CAPABILITIES).map_err(table_error)?;
            if table
                .insert(key.as_slice(), encoded_capability.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(invariant());
            }
        }
        {
            let key = encode_capability_token_key(intent.digests().current_write());
            let mut table = transaction
                .open_table(CAPABILITY_TOKENS)
                .map_err(table_error)?;
            if table
                .insert(key.as_slice(), encoded_lookup.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(invariant());
            }
        }
        {
            let mut table = transaction.open_table(META).map_err(table_error)?;
            if table
                .insert(META_CAPABILITY_BOOTSTRAP, encoded_marker.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(invariant());
            }
        }
        append_audit_record(
            transaction,
            &StoredAdministrationAuditRecordV1::Service(started),
        )?;
        append_audit_record(
            transaction,
            &StoredAdministrationAuditRecordV1::Capability(capability_audit),
        )?;
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for(RedbTestOperation::CapabilityBootstrap)?;
        Ok(CapabilityBootstrapResult::BootstrapCreated {
            capability_id: intent.capability_id(),
            revision: NonZeroU64::MIN,
            administration_sequence: transition_sequence,
            invocation_started_sequence: started_sequence,
        })
    }
}

fn bootstrap_replay(
    access: RedbWriteAccess,
    allocator: AdministrationSequenceAllocator,
    intent: &CapabilityBootstrapIntentV1,
    marker: CapabilityBootstrapMarkerV1,
) -> Result<CapabilityBootstrapResult, StorageError> {
    let transaction = access.transaction()?;
    let capability = validate_bootstrap_graph(&access, marker)?;
    let digest_matches =
        match resolve_capability_digests_write(transaction, intent.digests().candidates())? {
            CapabilityLookupResult::Found(record) => {
                record.capability_id() == marker.capability_id()
            }
            CapabilityLookupResult::NotFound | CapabilityLookupResult::MultipleMatches => false,
        };
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let sequences = access.service_audit_sequences(intent.start().request_id())?;
    let request_is_unused =
        service_lifecycle_in_write(&access, &audit, intent.start().request_id(), &sequences)?
            .is_none();
    drop(audit);
    if marker.capability_id() != intent.capability_id()
        || !capability.matches_requested(intent.requested())
        || !digest_matches
        || !request_is_unused
    {
        access.abort()?;
        return Ok(CapabilityBootstrapResult::BootstrapConflict);
    }

    let (assigned, next) = allocate_sequences(allocator, 1)?;
    let started_sequence = assigned[0];
    let started = StoredServiceAuditRecordV1::from_bootstrap_replay_start(
        started_sequence,
        intent.start(),
        marker.administration_sequence(),
    )
    .map_err(|_| corrupt())?;
    append_audit_record(
        transaction,
        &StoredAdministrationAuditRecordV1::Service(started),
    )?;
    write_administration_allocator(transaction, allocator, next)?;
    access.commit_for(RedbTestOperation::CapabilityBootstrap)?;
    Ok(CapabilityBootstrapResult::BootstrapReplayed {
        capability_id: capability.capability_id(),
        revision: capability.revision(),
        administration_sequence: marker.administration_sequence(),
        invocation_started_sequence: started_sequence,
    })
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use redb::ReadableDatabase;
    use riffdb_storage_api::{
        BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1,
        CapabilityCreateAwaitingDecision, CapabilityCreateCandidateTransaction, CapabilityGrantV1,
        CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, CapabilityRevokeAwaitingDecision,
        CapabilityRevokeCandidateTransaction, DatabaseInitializationPort,
        DatabaseInitializationResult, PartitionScopeV1, RevocationReasonCodeV1, StorageScanLimit,
    };
    use riffdb_types::{
        ActorId, ActorKind, Audience, ContractBundleHash, DatabaseId, DigestKeyId, Environment,
        QueryModuleHash, QueryModuleName, QueryModuleVersion, ServiceAuditTargetsV1,
        ServiceIngressKindV1, TenantScope, Timestamp, hash_reactive_module, hash_reactive_source,
    };

    use super::*;
    use crate::store::RedbStore;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(label: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-administration-{label}-{}-{}.redb",
                std::process::id(),
                NEXT_PATH.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let journal = crate::journal::journal_path(&self.0);
            let _ = std::fs::remove_file(&journal);
            let _ = std::fs::remove_file(crate::journal::checkpoint_journal_path(&self.0));
            let _ = std::fs::remove_file(crate::journal::spare_journal_path(&self.0));
            let mut rewrite = journal.into_os_string();
            rewrite.push(".rewrite");
            let _ = std::fs::remove_file(PathBuf::from(rewrite));
        }
    }

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(1)).expect("database ID")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(seed)).expect("capability ID")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
    }

    fn digest(seed: u8) -> CapabilityTokenDigest {
        CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(u32::from(seed)).expect("digest key ID"),
            [seed; 32],
        )
    }

    fn principal(capability_id: CapabilityId) -> AuditPrincipalV1 {
        AuditPrincipalV1::new(
            ActorId::new("operator").expect("actor ID"),
            ActorKind::Human,
            capability_id,
            NonZeroU64::MIN,
        )
    }

    fn requested_record(database_id: DatabaseId, actor: &str) -> CapabilityRequestedRecordV1 {
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        CapabilityRequestedRecordV1::new(
            database_id,
            Environment::new("test").expect("environment"),
            ActorId::new(actor).expect("actor"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested record")
    }

    fn initialized_ports(label: &str) -> (TestPath, RedbOperationalPorts) {
        let path = TestPath::new(label);
        let mut store = RedbStore::open(&path.0).expect("open test database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize test database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate test ports");
        (path, ports)
    }

    fn bundle(lineage: &str, version: u64, hash: u8) -> StoredContractBundleV1 {
        StoredContractBundleV1::new(
            ContractLineage::new(lineage).expect("lineage"),
            ContractVersion::new(version).expect("version"),
            ContractBundleHash::from_bytes([hash; 32]),
            vec![hash],
        )
        .expect("bundle")
    }

    fn catalog_intent(
        expected: Option<ContractVersion>,
        bundle: StoredContractBundleV1,
        request: u8,
    ) -> CatalogActivationIntentV1 {
        CatalogActivationIntentV1::new(
            expected,
            bundle,
            request_id(request),
            principal(capability_id(2)),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            None,
        )
    }

    fn query_module(
        contract: &StoredContractBundleV1,
        version: u64,
        hash: u8,
    ) -> StoredQueryModuleV1 {
        StoredQueryModuleV1::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(version).expect("module version"),
            QueryModuleHash::from_bytes([hash; 32]),
            contract.lineage().clone(),
            contract.contract_version(),
            contract.bundle_hash(),
            vec![hash, 1, 2, 3],
        )
        .expect("stored module")
    }

    fn query_module_intent(
        expectation: QueryModuleActiveExpectationV1,
        module: StoredQueryModuleV1,
        request: u8,
    ) -> QueryModuleActivationIntentV1 {
        QueryModuleActivationIntentV1::new(
            expectation,
            module,
            request_id(request),
            principal(capability_id(2)),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            None,
        )
    }

    fn reactive_module(
        contract: &StoredContractBundleV1,
        source: &[u8],
        artifact: &[u8],
    ) -> StoredReactiveModuleV1 {
        StoredReactiveModuleV1::new(
            "streamdesk".to_owned(),
            1,
            hash_reactive_module(artifact),
            contract.lineage().clone(),
            contract.contract_version(),
            contract.bundle_hash(),
            hash_reactive_source(source),
            Vec::new(),
            source.to_vec(),
            artifact.to_vec(),
        )
        .expect("stored reactive module")
    }

    fn reactive_intent(
        module: StoredReactiveModuleV1,
        request: u8,
    ) -> ReactiveModulePublicationIntentV1 {
        ReactiveModulePublicationIntentV1::new(
            module,
            request_id(request),
            principal(capability_id(2)),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            None,
        )
    }

    /// Publishes one reactive module against a freshly activated contract and
    /// returns the ports, the module, and the publication's own sequence.
    fn published_reactive_module(
        label: &str,
    ) -> (
        TestPath,
        RedbOperationalPorts,
        StoredReactiveModuleV1,
        AdministrationSequence,
    ) {
        let (path, mut ports) = initialized_ports(label);
        let contract = bundle("reactive-hygiene", 1, 0x51);
        assert!(matches!(
            ports
                .activate_catalog(&catalog_intent(None, contract.clone(), 70))
                .expect("activate the contract the module compiles against"),
            CatalogActivationResult::Activated { .. }
        ));
        let module = reactive_module(&contract, b"republish source", b"republish artifact");
        let publication = ports
            .publish_reactive_module(&reactive_intent(module.clone(), 71))
            .expect("publish the immutable module");
        let ReactiveModulePublicationResult::Published {
            administration_sequence,
            ..
        } = publication
        else {
            panic!("a first publication must publish, got {publication:?}");
        };
        (path, ports, module, administration_sequence)
    }

    fn bootstrap_intent(
        request: u8,
        capability_id: CapabilityId,
        token_digest: CapabilityTokenDigest,
        issued_seconds: i64,
    ) -> CapabilityBootstrapIntentV1 {
        let issued_at = Timestamp::new(issued_seconds, 0).expect("issued at");
        let start = BootstrapServiceAuditStartV1::new(
            request_id(request),
            issued_at,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id)])
                .expect("targets"),
            None,
        )
        .expect("bootstrap start");
        CapabilityBootstrapIntentV1::new(
            capability_id,
            requested_record(database_id(), "operator"),
            BootstrapDigestCandidatesV1::new(vec![token_digest], token_digest)
                .expect("digest candidates"),
            issued_at,
            Timestamp::new(issued_seconds + 60, 0).expect("expiry"),
            start,
        )
        .expect("bootstrap intent")
    }

    fn audit_count(ports: &RedbOperationalPorts) -> usize {
        let scan = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(
                None,
                StorageScanLimit::new(64).expect("scan limit"),
            ))
            .expect("scan audit");
        let AdministrationAuditScan::ExactEnd { records } = scan else {
            panic!("small test stream must reach exact end");
        };
        assert!(
            records
                .iter()
                .all(|item| item.encoded_content_charge().get() > 0)
        );
        records.len()
    }

    fn denied_audit(request: u8) -> ServiceAuditAppendIntentV1 {
        ServiceAuditAppendIntentV1::new(
            request_id(request),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            principal(capability_id(2)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("audit intent")
    }

    fn command_audit(
        request: u16,
        phase: ServiceAuditPhaseV1,
        seconds: i64,
    ) -> ServiceAuditAppendIntentV1 {
        let seed = u8::try_from(request % 256).expect("bounded request seed");
        let mut request_bytes = uuid_bytes(seed);
        request_bytes[..2].copy_from_slice(&request.to_be_bytes());
        ServiceAuditAppendIntentV1::new(
            RequestId::from_bytes(request_bytes).expect("request ID"),
            Timestamp::new(seconds, 0).expect("timestamp"),
            ServiceOperationV1::ExecuteCommand,
            phase,
            principal(capability_id(2)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("command audit intent")
    }

    #[test]
    fn service_audit_group_assigns_fifo_sequences_in_one_transition() {
        let (_path, mut ports) = initialized_ports("audit-group");
        let intents = [denied_audit(10), denied_audit(11), denied_audit(12)];

        let results = ports
            .append_service_audit_group(&intents)
            .expect("append audit group");

        let sequences = results
            .into_iter()
            .zip(&intents)
            .map(|(result, intent)| match result {
                ServiceAuditAppendResult::Appended(record) => {
                    assert_eq!(record.request_id(), intent.request_id());
                    record.administration_sequence().get()
                }
                ServiceAuditAppendResult::PhaseConflict => panic!("fresh request must append"),
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2, 3]);
        assert_eq!(audit_count(&ports), 3);
    }

    #[test]
    fn deferred_service_audit_group_is_invisible_until_fenced_and_reopens_exactly() {
        let (path, mut ports) = initialized_ports("deferred-audit-group");
        let intents = [denied_audit(20), denied_audit(21)];
        let submitted = ports
            .submit_service_audit_group(&intents)
            .expect("submit deferred audit group");
        let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence) = submitted else {
            panic!("standard redb must submit a journal fence");
        };
        assert_eq!(audit_count(&ports), 0, "unfenced roots remain private");
        let results = fence.wait().expect("fence audit group");
        assert_eq!(results.len(), intents.len());
        assert!(
            results
                .iter()
                .all(|result| { matches!(result, ServiceAuditAppendResult::Appended(_)) })
        );
        assert_eq!(audit_count(&ports), 2);
        drop(ports);

        let store = RedbStore::open(&path.0).expect("recover journal suffix");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let reopened = dormant
            .into_operational_after_catalog_validation()
            .expect("activate recovered ports");
        assert_eq!(audit_count(&reopened), 2);
    }

    #[test]
    fn staged_audit_group_uses_exactly_one_begin_read_for_sequence_lookup() {
        let path = TestPath::new("audit-group-one-begin-read");
        let controller = crate::hooks::RedbTestController::count_audit_sequence_begin_reads();
        let mut store = RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open controlled database");
        store
            .initialize_database(database_id())
            .expect("initialize");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate");
        let intents = [
            command_audit(1, ServiceAuditPhaseV1::Started, 1),
            command_audit(1, ServiceAuditPhaseV1::Failed, 2),
            command_audit(2, ServiceAuditPhaseV1::Started, 3),
            command_audit(2, ServiceAuditPhaseV1::Failed, 4),
            command_audit(3, ServiceAuditPhaseV1::Started, 5),
            command_audit(3, ServiceAuditPhaseV1::Failed, 6),
        ];
        let before = controller.audit_sequence_begin_reads();
        let access = ports.begin_write().expect("begin write");
        let records =
            stage_service_audit_group_in_write(&access, &intents).expect("stage fused group");
        assert_eq!(records.len(), 6);
        assert_eq!(
            controller
                .audit_sequence_begin_reads()
                .saturating_sub(before),
            1,
            "grouped sequence lookup must open exactly one read snapshot"
        );
        access.abort().expect("abort uncommitted staging");
    }

    #[test]
    fn service_audit_group_uses_exactly_one_begin_read_for_sequence_lookup() {
        let path = TestPath::new("audit-group-append-one-begin-read");
        let controller = crate::hooks::RedbTestController::count_audit_sequence_begin_reads();
        let mut store = RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open controlled database");
        store
            .initialize_database(database_id())
            .expect("initialize");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate");
        let intents = [denied_audit(20), denied_audit(21), denied_audit(22)];
        let before = controller.audit_sequence_begin_reads();
        ports
            .append_service_audit_group(&intents)
            .expect("append group");
        assert_eq!(
            controller
                .audit_sequence_begin_reads()
                .saturating_sub(before),
            1,
            "group append must open exactly one sequence-lookup snapshot"
        );
    }

    #[test]
    fn fused_pair_appends_both_rows_in_one_transaction() {
        let (_path, mut ports) = initialized_ports("fused-pair-happy");
        let started = command_audit(1, ServiceAuditPhaseV1::Started, 1);
        let terminal = command_audit(1, ServiceAuditPhaseV1::Failed, 2);
        ports
            .append_service_audit_fused_pair(&started, &terminal)
            .expect("fused pair");
        assert_eq!(audit_count(&ports), 2);
    }

    #[test]
    fn fused_pair_against_an_existing_started_fails_closed_and_writes_nothing() {
        let (_path, mut ports) = initialized_ports("fused-pair-conflict");
        let started = command_audit(2, ServiceAuditPhaseV1::Started, 1);
        ports
            .append_service_audit(&started)
            .expect("standalone started");
        let before = audit_count(&ports);
        let again_started = command_audit(2, ServiceAuditPhaseV1::Started, 3);
        let terminal = command_audit(2, ServiceAuditPhaseV1::Failed, 4);
        let err = ports
            .append_service_audit_fused_pair(&again_started, &terminal)
            .expect_err("pair against existing Started must fail closed");
        assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
        assert_eq!(audit_count(&ports), before);
    }

    #[test]
    fn fused_maximum_command_group_allocates_two_audit_rows_per_command() {
        let (_path, ports) = initialized_ports("fused-audit-maximum");
        let group_bound = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
        let mut intents = Vec::with_capacity(group_bound * 2);
        for request in 1..=u16::try_from(group_bound).expect("group bound") {
            intents.push(command_audit(
                request,
                ServiceAuditPhaseV1::Started,
                i64::from(request) * 2,
            ));
            intents.push(command_audit(
                request,
                ServiceAuditPhaseV1::Failed,
                i64::from(request) * 2 + 1,
            ));
        }

        let access = ports
            .begin_write()
            .expect("begin fused command transaction");
        let records =
            stage_service_audit_group_in_write(&access, &intents).expect("stage fused audit rows");

        assert_eq!(records.len(), group_bound * 2);
        assert_eq!(
            records
                .first()
                .expect("first record")
                .administration_sequence()
                .get(),
            1
        );
        assert_eq!(
            records
                .last()
                .expect("last record")
                .administration_sequence()
                .get(),
            u64::try_from(group_bound * 2).expect("bounded audit sequence")
        );
        access.abort().expect("abort uncommitted test transaction");
    }

    #[test]
    fn administration_write_rejects_allocator_tail_skew_before_append() {
        let (_path, mut ports) = initialized_ports("audit-tail-skew");
        ports
            .append_service_audit(&denied_audit(10))
            .expect("append first audit");

        let transaction = ports
            .shared
            .database
            .begin_write()
            .expect("begin raw corruption transaction");
        let allocator = AdministrationSequenceAllocator::next(
            AdministrationSequence::new(3).expect("sequence three"),
        );
        let encoded =
            encode_administration_sequence_allocator_v1(allocator).expect("encode allocator");
        transaction
            .open_table(META)
            .expect("open metadata")
            .insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
            .expect("skew allocator");
        transaction.commit().expect("commit test corruption");

        let error = ports
            .append_service_audit(&denied_audit(11))
            .expect_err("allocator/tail skew must fail closed");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
        let transaction = ports
            .shared
            .database
            .begin_read()
            .expect("begin verification read");
        assert_eq!(
            transaction
                .open_table(AUDIT)
                .expect("open audit")
                .len()
                .expect("audit length"),
            1
        );
    }

    #[test]
    fn full_read_validation_still_rejects_corrupt_retained_history() {
        let (_path, mut ports) = initialized_ports("audit-retained-corruption");
        for request in 20..23 {
            ports
                .append_service_audit(&denied_audit(request))
                .expect("append audit");
        }

        let transaction = ports
            .shared
            .database
            .begin_write()
            .expect("begin raw corruption transaction");
        let mut audit = transaction.open_table(AUDIT).expect("open audit");
        let first = audit
            .get(encode_audit_key(AdministrationSequence::first()).as_slice())
            .expect("read first")
            .expect("first audit")
            .value()
            .to_vec();
        audit
            .insert(
                encode_audit_key(AdministrationSequence::new(2).expect("sequence two")).as_slice(),
                first.as_slice(),
            )
            .expect("corrupt middle record");
        drop(audit);
        transaction.commit().expect("commit test corruption");

        let error = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(
                None,
                StorageScanLimit::new(8).expect("scan limit"),
            ))
            .expect_err("full public read validation must reject corrupt history");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    }

    #[test]
    fn catalog_and_service_audit_share_one_contiguous_stream() {
        let (_path, mut ports) = initialized_ports("catalog-audit");
        let deployed = bundle("budget", 1, 7);
        let activated = ports
            .activate_catalog(&catalog_intent(None, deployed.clone(), 10))
            .expect("activate catalog");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::first()
        ));
        assert_eq!(
            ports
                .read_contract_bundle(deployed.lineage(), deployed.contract_version())
                .expect("read bundle"),
            Some(deployed.clone())
        );
        assert_eq!(
            ports.read_active_catalog().expect("read active catalog"),
            Some(ActiveCatalogPointerV1::from_bundle(&deployed))
        );

        let denied = ServiceAuditAppendIntentV1::new(
            request_id(11),
            Timestamp::new(11, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            principal(capability_id(2)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("audit intent");
        assert!(matches!(
            ports
                .append_service_audit(&denied)
                .expect("append denied audit"),
            ServiceAuditAppendResult::Appended(record)
                if record.administration_sequence()
                    == AdministrationSequence::new(2).expect("sequence two")
        ));

        let replay = ports
            .activate_catalog(&catalog_intent(None, deployed.clone(), 12))
            .expect("replay catalog activation");
        assert!(matches!(
            replay,
            CatalogActivationResult::AlreadyActive {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::first()
        ));
        let conflict = ports
            .activate_catalog(&catalog_intent(None, bundle("budget", 1, 9), 13))
            .expect("detect bundle conflict");
        assert_eq!(conflict, CatalogActivationResult::BundleConflict);
        assert_eq!(audit_count(&ports), 2);
    }

    #[test]
    fn query_module_activation_is_atomic_cas_audited_and_restart_safe() {
        let (path, mut ports) = initialized_ports("query-module");
        let contract = bundle("ticketdesk", 1, 7);
        ports
            .activate_catalog(&catalog_intent(None, contract.clone(), 30))
            .expect("activate exact contract");
        let module = query_module(&contract, 1, 8);
        let activated = ports
            .activate_query_module(&query_module_intent(
                QueryModuleActiveExpectationV1::Absent,
                module.clone(),
                31,
            ))
            .expect("activate query module");
        assert!(matches!(
            activated,
            QueryModuleActivationResult::Activated {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::new(2).expect("sequence two")
        ));
        assert_eq!(
            ports
                .read_query_module(module.module_hash())
                .expect("read module"),
            Some(module.clone())
        );
        assert_eq!(
            ports
                .read_active_query_module(
                    contract.lineage(),
                    contract.contract_version(),
                    contract.bundle_hash(),
                )
                .expect("read active module"),
            Some(ActiveQueryModulePointerV1::from_module(&module))
        );
        assert!(matches!(
            ports
                .activate_query_module(&query_module_intent(
                    QueryModuleActiveExpectationV1::Exact(QueryModuleHash::from_bytes([1; 32])),
                    module.clone(),
                    32,
                ))
                .expect("idempotent exact replay"),
            QueryModuleActivationResult::AlreadyActive {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::new(2).expect("sequence two")
        ));
        assert_eq!(audit_count(&ports), 2);

        drop(ports);
        let store = RedbStore::open(&path.0).expect("reopen database");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let reopened = dormant
            .into_operational_after_catalog_validation()
            .expect("reactivate ports");
        assert_eq!(
            reopened
                .read_active_query_module(
                    contract.lineage(),
                    contract.contract_version(),
                    contract.bundle_hash(),
                )
                .expect("read active after restart"),
            Some(ActiveQueryModulePointerV1::from_module(&module))
        );
    }

    #[test]
    fn bootstrap_replay_appends_only_a_new_linked_start() {
        let (_path, mut ports) = initialized_ports("bootstrap");
        let root = capability_id(3);
        let root_digest = digest(1);
        let first = bootstrap_intent(20, root, root_digest, 10);
        assert_eq!(
            ports
                .bootstrap_capability(&first)
                .expect("create bootstrap capability"),
            CapabilityBootstrapResult::BootstrapCreated {
                capability_id: root,
                revision: NonZeroU64::MIN,
                administration_sequence: AdministrationSequence::new(2).expect("sequence two"),
                invocation_started_sequence: AdministrationSequence::first(),
            }
        );
        assert_eq!(
            ports
                .read_capability(root)
                .expect("read bootstrap capability")
                .map(|record| record.capability_id()),
            Some(root)
        );
        assert!(matches!(
            ports
                .resolve_capability_digests(&[root_digest])
                .expect("resolve bootstrap digest"),
            CapabilityLookupResult::Found(record) if record.capability_id() == root
        ));
        let inventory = CapabilityInventoryReader::scan_capabilities(
            &ports,
            None,
            StorageScanLimit::new(1).expect("inventory limit"),
        )
        .expect("scan capability inventory");
        assert_eq!(inventory.records().len(), 1);
        assert_eq!(inventory.records()[0].capability_id(), root);
        assert!(!inventory.has_more());

        let replay = bootstrap_intent(21, root, root_digest, 20);
        assert_eq!(
            ports
                .bootstrap_capability(&replay)
                .expect("replay bootstrap capability"),
            CapabilityBootstrapResult::BootstrapReplayed {
                capability_id: root,
                revision: NonZeroU64::MIN,
                administration_sequence: AdministrationSequence::new(2).expect("sequence two"),
                invocation_started_sequence: AdministrationSequence::new(3)
                    .expect("sequence three"),
            }
        );
        let conflict = bootstrap_intent(22, root, digest(2), 30);
        assert_eq!(
            ports
                .bootstrap_capability(&conflict)
                .expect("reject bootstrap conflict"),
            CapabilityBootstrapResult::BootstrapConflict
        );
        assert_eq!(audit_count(&ports), 3);

        let terminal = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
            replay.start(),
            Timestamp::new(21, 0).expect("terminal timestamp"),
            AdministrationSequence::new(2).expect("bootstrap transition"),
        )
        .expect("bootstrap terminal");
        assert!(matches!(
            ports
                .append_service_audit(&terminal)
                .expect("append bootstrap terminal"),
            ServiceAuditAppendResult::Appended(_)
        ));
        assert_eq!(audit_count(&ports), 4);
    }

    #[test]
    fn consuming_create_and_revoke_transactions_are_atomic_and_replay_safe() {
        let (_path, mut ports) = initialized_ports("capability-transactions");
        let root = capability_id(4);
        ports
            .bootstrap_capability(&bootstrap_intent(30, root, digest(3), 10))
            .expect("bootstrap root");
        let child = capability_id(5);
        let requested = requested_record(database_id(), "delegate");
        let authorizer = principal(root);
        let candidate = CapabilityCreateCandidateV1::new(
            child,
            request_id(31),
            requested.clone(),
            digest(4),
            authorizer.clone(),
            None,
        );
        let candidate = ports
            .begin_capability_create(candidate.clone())
            .expect("begin create");
        let (awaiting, current) = candidate
            .read_transaction_current()
            .expect("read create current state");
        assert!(current.authorizing().is_some());
        assert!(current.target().is_none());
        let intent = CapabilityCreateIntentV1::new(
            child,
            request_id(31),
            requested.clone(),
            digest(4),
            Timestamp::new(20, 0).expect("issued"),
            Timestamp::new(80, 0).expect("expires"),
            authorizer.clone(),
            None,
        )
        .expect("create intent");
        assert!(matches!(
            awaiting.commit_create(intent).expect("commit create"),
            CapabilityCreateResult::Created {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::new(3).expect("sequence three")
        ));
        assert!(matches!(
            ports
                .resolve_capability_digests(&[digest(4)])
                .expect("resolve child"),
            CapabilityLookupResult::Found(record) if record.capability_id() == child
        ));

        let revoke_candidate = CapabilityRevokeCandidateV1::new(
            child,
            request_id(32),
            authorizer.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        );
        let candidate = ports
            .begin_capability_revoke(revoke_candidate.clone())
            .expect("begin revoke");
        let (awaiting, current) = candidate
            .read_transaction_current()
            .expect("read revoke current state");
        let revision = current.target().expect("target capability").revision();
        let revoke = CapabilityRevokeIntentV1::new(
            child,
            revision,
            request_id(32),
            Timestamp::new(30, 0).expect("revoked at"),
            authorizer.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        );
        assert!(matches!(
            awaiting.commit_revoke(revoke).expect("commit revoke"),
            CapabilityRevokeResult::Revoked {
                revision,
                administration_sequence,
                ..
            } if revision.get() == 2
                && administration_sequence
                    == AdministrationSequence::new(4).expect("sequence four")
        ));

        let replay = ports
            .begin_capability_revoke(CapabilityRevokeCandidateV1::new(
                child,
                request_id(33),
                authorizer,
                None,
                RevocationReasonCodeV1::Requested,
            ))
            .expect("begin revoke replay");
        let (awaiting, current) = replay
            .read_transaction_current()
            .expect("read revoke replay state");
        let replay_intent = CapabilityRevokeIntentV1::new(
            child,
            current.target().expect("revoked target").revision(),
            request_id(33),
            Timestamp::new(40, 0).expect("replay time"),
            principal(root),
            None,
            RevocationReasonCodeV1::Requested,
        );
        assert!(matches!(
            awaiting
                .commit_revoke(replay_intent)
                .expect("replay revoke"),
            CapabilityRevokeResult::AlreadyRevoked {
                administration_sequence,
                ..
            } if administration_sequence == AdministrationSequence::new(4).expect("sequence four")
        ));
        assert_eq!(audit_count(&ports), 4);
    }

    #[test]
    fn catalog_commit_failpoint_reports_uncertainty_after_atomic_commit() {
        let path = TestPath::new("catalog-unknown");
        let controller = crate::hooks::RedbTestController::return_unknown_after_commit(
            RedbTestOperation::CatalogAdministration,
        );
        let mut store = RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open controlled database");
        store
            .initialize_database(database_id())
            .expect("initialize controlled database");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate controlled ports");
        let deployed = bundle("uncertain", 1, 8);
        let error = ports
            .activate_catalog(&catalog_intent(None, deployed.clone(), 50))
            .expect_err("post-commit failpoint must return uncertainty");
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        assert_eq!(
            ports
                .read_active_catalog()
                .expect("resolve committed state"),
            Some(ActiveCatalogPointerV1::from_bundle(&deployed))
        );
        assert_eq!(audit_count(&ports), 1);
        assert!(controller.events().iter().any(|event| {
            event.operation() == RedbTestOperation::CatalogAdministration
                && event.phase() == crate::hooks::RedbTestPhase::AfterEngineCommit
        }));
        let unavailable = ports
            .append_service_audit(
                &ServiceAuditAppendIntentV1::new(
                    request_id(51),
                    Timestamp::new(51, 0).expect("timestamp"),
                    ServiceOperationV1::GetHealth,
                    ServiceAuditPhaseV1::Denied,
                    principal(capability_id(2)),
                    ServiceIngressKindV1::Grpc,
                    ServiceAuditTargetsV1::empty(),
                    None,
                    ServiceAuditLinkV1::None,
                )
                .expect("service audit intent"),
            )
            .expect_err("uncertain commit must fence later writes");
        assert_eq!(unavailable.kind(), StorageErrorKind::Unavailable);
    }

    #[test]
    fn fused_service_audit_writes_request_index_and_rejects_reuse() {
        let (_path, mut ports) = initialized_ports("fused-audit-index-gate");
        let request = request_id(0x61);
        let started = ServiceAuditAppendIntentV1::new(
            request,
            Timestamp::new(10, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Started,
            principal(capability_id(1)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("started");
        let failed = ServiceAuditAppendIntentV1::new(
            request,
            Timestamp::new(11, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Failed,
            principal(capability_id(1)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("failed");
        let access = ports.begin_write().expect("begin write");
        let staged =
            stage_service_audit_group_in_write(&access, &[started.clone(), failed.clone()])
                .expect("stage fused");
        assert_eq!(staged.len(), 2);
        access
            .commit_for(RedbTestOperation::ServiceAudit)
            .expect("commit fused");

        // Index rows exist for both sequences under the durable secondary index.
        let read = ports.shared.database.begin_read().expect("read");
        let index = read
            .open_table(crate::layout::AUDIT_BY_REQUEST)
            .expect("index table");
        let prefix = crate::keys::encode_audit_by_request_prefix(request);
        let count = index
            .range::<&[u8]>((
                std::ops::Bound::Included(prefix.as_slice()),
                std::ops::Bound::Unbounded,
            ))
            .expect("range")
            .filter(|entry| {
                entry
                    .as_ref()
                    .ok()
                    .is_some_and(|(key, _)| key.value().starts_with(prefix.as_slice()))
            })
            .count();
        assert_eq!(count, 2);

        // Same RequestId cannot be re-admitted as a new Started lifecycle.
        let reuse = ports
            .append_service_audit(&started)
            .expect("reuse attempt returns phase conflict, not success");
        assert!(matches!(reuse, ServiceAuditAppendResult::PhaseConflict));
    }
    #[test]
    fn redb_reactive_republish_answers_without_taking_the_write_lock() {
        let (_path, mut ports, module, administration_sequence) =
            published_reactive_module("reactive-republish-read");
        let records_before = audit_count(&ports);

        // The exclusive mutation gate is the operationally significant cost:
        // redb admits one writer at a time, so an unbounded AUDIT walk held
        // under it stalls every other writer, commands included. `begin_write`
        // and `acquire_indexed_read_lease` each take exactly one ticket; an
        // unchanged ticket count proves this republish took neither.
        let tickets_before = ports.mutation_gate_tickets();
        let republished = ports
            .publish_reactive_module(&reactive_intent(module.clone(), 72))
            .expect("republish the identical module");
        let tickets_after = ports.mutation_gate_tickets();

        assert_eq!(
            republished,
            ReactiveModulePublicationResult::AlreadyPublished {
                module_hash: module.module_hash(),
                administration_sequence,
            },
            "a republish must name the original publication's transition"
        );
        assert_eq!(
            tickets_after, tickets_before,
            "an idempotent republish must answer from a read transaction, never under the write lock"
        );
        assert_eq!(
            audit_count(&ports),
            records_before,
            "a republish writes nothing durable"
        );
    }

    #[test]
    fn redb_reactive_republish_under_the_write_lock_serves_the_original_sequence() {
        let (_path, mut ports, module, administration_sequence) =
            published_reactive_module("reactive-republish-locked");
        let records_before = audit_count(&ports);

        // Simulates the lost race at the API level: a racing publish landed
        // between the read probe's "absent" answer and `begin_write`, so the
        // locked path is entered for an already-present module. Driving that
        // path directly is the deterministic construction of the interleave —
        // the write path must re-probe presence and serve the same republish
        // answer rather than publishing a duplicate.
        let republished = ports
            .publish_reactive_module_locked(&reactive_intent(module.clone(), 73))
            .expect("serve the republish answer from under the write transaction");

        assert_eq!(
            republished,
            ReactiveModulePublicationResult::AlreadyPublished {
                module_hash: module.module_hash(),
                administration_sequence,
            },
            "the write-path re-check must serve the original publication's transition"
        );
        assert_eq!(
            audit_count(&ports),
            records_before,
            "losing the race still writes nothing durable"
        );
    }

    #[test]
    fn redb_reactive_republish_fails_closed_on_an_allocator_skewed_stream() {
        let (_path, mut ports, module, _sequence) =
            published_reactive_module("reactive-republish-skew");

        // Skew the allocator one ahead of the stream it owns. The write path has
        // always refused this through `validate_administration_tail` before
        // reaching the presence probe, so the read-transaction fast path must
        // refuse it too — otherwise moving the republish answer off the write
        // lock would have quietly weakened fail-closed behaviour instead of
        // merely relocating a scan.
        let access = ports.begin_write().expect("write access");
        {
            let transaction = access.transaction().expect("transaction");
            let AdministrationSequenceAllocator::Next(next) =
                read_administration_allocator(transaction).expect("read the allocator")
            else {
                panic!("a small test stream is never exhausted");
            };
            let skewed = AdministrationSequenceAllocator::next(
                next.checked_next().expect("skewed successor"),
            );
            let encoded =
                encode_administration_sequence_allocator_v1(skewed).expect("encode the allocator");
            let mut meta = transaction.open_table(META).expect("meta table");
            meta.insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
                .expect("skew the administration allocator");
        }
        access
            .commit_for(RedbTestOperation::QueryModuleAdministration)
            .expect("commit the skewed allocator");

        let refused = ports
            .publish_reactive_module(&reactive_intent(module, 76))
            .expect_err("an allocator-skewed stream must be refused, not republished");
        assert_eq!(refused.kind(), StorageErrorKind::CorruptData);
    }

    #[test]
    fn redb_reactive_republish_fails_closed_without_a_publication_record() {
        let (_path, mut ports) = initialized_ports("reactive-republish-orphan");
        let contract = bundle("reactive-hygiene", 1, 0x52);
        assert!(matches!(
            ports
                .activate_catalog(&catalog_intent(None, contract.clone(), 74))
                .expect("activate the contract"),
            CatalogActivationResult::Activated { .. }
        ));
        let module = reactive_module(&contract, b"orphan source", b"orphan artifact");

        // Install the retained row with NO publication record in the stream.
        let access = ports.begin_write().expect("write access");
        {
            let transaction = access.transaction().expect("transaction");
            let mut modules = transaction
                .open_table(REACTIVE_MODULES)
                .expect("reactive module table");
            let encoded = encode_reactive_module_v1(&module).expect("encode module");
            modules
                .insert(
                    encode_reactive_module_key(module.module_hash()).as_slice(),
                    encoded.as_bytes(),
                )
                .expect("install orphan module row");
        }
        access
            .commit_for(RedbTestOperation::QueryModuleAdministration)
            .expect("commit orphan module row");

        // The read-transaction fast path must fail closed exactly as the write
        // path did: a retained module with no publication record is corruption.
        let refused = ports
            .publish_reactive_module(&reactive_intent(module, 75))
            .expect_err("a module with no publication record must be refused");
        assert_eq!(refused.kind(), StorageErrorKind::CorruptData);
    }
}
