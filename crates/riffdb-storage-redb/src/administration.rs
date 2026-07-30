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
    CatalogAdministrationRepository, CatalogRepository, EncodedPageItem, MAX_READABLE_DIGEST_KEYS,
    MAX_RETAINED_QUERY_MODULES, MAX_SCAN_PAGE_BYTES, QueryModuleActivationIntentV1,
    QueryModuleActivationResult, QueryModuleActiveExpectationV1,
    QueryModuleAdministrationRepository, QueryModuleRepository, SequenceAllocationError,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    StorageError, StorageErrorKind, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StoredCapabilityAdministrationV1, StoredCapabilityRecordV1, StoredCatalogAdministrationV1,
    StoredContractBundleV1, StoredQueryModuleAdministrationV1, StoredQueryModuleV1,
    StoredServiceAuditRecordV1, TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, CapabilityTokenDigest, ContractBundleHash,
    ContractLineage, ContractVersion, QueryModuleHash, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceOperationV1,
};

use crate::application::stage_admission;
use crate::codec::{
    decode_active_catalog_pointer_v1, decode_administration_audit_record_v1,
    decode_administration_sequence_allocator_v1, decode_capability_bootstrap_marker_v1,
    decode_capability_record_v1, decode_capability_token_lookup_v1, decode_commit_with_event_table,
    decode_contract_bundle_v1, decode_database_identity_v1, decode_provenance_record_v1,
    decode_query_module_administration_v1, decode_query_module_v1,
    encode_active_catalog_pointer_v1, encode_administration_audit_record_v1,
    encode_administration_sequence_allocator_v1, encode_capability_bootstrap_marker_v1,
    encode_capability_record_v1, encode_capability_token_lookup_v1, encode_contract_bundle_v1,
    encode_query_module_administration_v1, encode_query_module_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{
    decode_audit_key, decode_capability_key, encode_active_query_module_key,
    encode_application_sequence_key, encode_audit_key, encode_capability_key,
    encode_capability_token_key, encode_contract_bundle_key, encode_provenance_key,
    encode_query_module_key,
};
use crate::layout::{
    AUDIT, CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS,
    CONTRACT_BUNDLES, EVENTS, META, META_ADMINISTRATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP,
    META_DATABASE_ID, PROVENANCE, QUERY_MODULE_ACTIVE, QUERY_MODULES,
};
use crate::store::{RedbOperationalPorts, RedbWriteAccess};
use crate::transient::TransientIndexDelta;

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

fn read_administration_allocator(
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

fn validate_administration_table<T>(
    table: &T,
    allocator: AdministrationSequenceAllocator,
) -> Result<(), StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut expected = Some(AdministrationSequence::first());
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let key = decode_audit_key(key.value()).map_err(|_| corrupt())?;
        let record = decode_administration_audit_record_v1(value.value())?;
        if expected != Some(key) || record.value().administration_sequence() != key {
            return Err(corrupt());
        }
        expected = key.checked_next();
    }
    let matches = match expected {
        Some(next) => allocator == AdministrationSequenceAllocator::next(next),
        None => allocator == AdministrationSequenceAllocator::Exhausted,
    };
    if !matches {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_administration_tail(
    transaction: &redb::WriteTransaction,
) -> Result<AdministrationSequenceAllocator, StorageError> {
    // Startup and every public read validate the complete contiguous stream.
    // Once that proof holds, typed writes preserve it inductively: the only
    // audit mutation appends exactly the allocator-owned next sequence and
    // atomically advances the allocator. Checking count plus the exact decoded
    // tail therefore rejects a lost, duplicated, reordered, or allocator-skewed
    // transition without rescanning all retained history for every append.
    let allocator = read_administration_allocator(transaction)?;
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let (expected_count, expected_last) = match allocator {
        AdministrationSequenceAllocator::Next(next) => {
            let count = next.get().checked_sub(1).ok_or_else(corrupt)?;
            (count, AdministrationSequence::new(count))
        }
        AdministrationSequenceAllocator::Exhausted => {
            (u64::MAX, AdministrationSequence::new(u64::MAX))
        }
    };
    if table.len().map_err(precommit_storage_error)? != expected_count {
        return Err(corrupt());
    }
    let last = table.last().map_err(precommit_storage_error)?;
    match (expected_last, last) {
        (None, None) => Ok(allocator),
        (Some(expected), Some((key, value))) => {
            let key = decode_audit_key(key.value()).map_err(|_| corrupt())?;
            let record = decode_administration_audit_record_v1(value.value())?;
            if key != expected || record.value().administration_sequence() != expected {
                return Err(corrupt());
            }
            Ok(allocator)
        }
        _ => Err(corrupt()),
    }
}

fn validate_administration_stream_readonly(
    transaction: &redb::ReadTransaction,
) -> Result<(), StorageError> {
    let allocator = read_administration_allocator_readonly(transaction)?;
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    validate_administration_table(&table, allocator)
}

fn allocate_sequences(
    allocator: AdministrationSequenceAllocator,
    count: u8,
) -> Result<(Vec<AdministrationSequence>, AdministrationSequenceAllocator), StorageError> {
    let allocation = allocator
        .allocate_consecutive(count)
        .map_err(sequence_error)?;
    Ok((allocation.assigned().to_vec(), allocation.next()))
}

fn write_administration_allocator(
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

fn append_audit_record(
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
    Ok(())
}

fn read_audit_record<T>(
    table: &T,
    sequence: AdministrationSequence,
) -> Result<StoredAdministrationAuditRecordV1, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_audit_key(sequence);
    let value = table
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let record = decoded_value(decode_administration_audit_record_v1(value.value())?);
    if record.administration_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(record)
}

fn find_audit_record<T>(
    table: &T,
    sequence: AdministrationSequence,
) -> Result<Option<StoredAdministrationAuditRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_audit_key(sequence);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let record = decoded_value(decode_administration_audit_record_v1(value.value())?);
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

fn last_catalog_activation<T>(
    table: &T,
) -> Result<Option<StoredCatalogAdministrationV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut last = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = decode_audit_key(key.value()).map_err(|_| corrupt())?;
        let record = decoded_value(decode_administration_audit_record_v1(value.value())?);
        if record.administration_sequence() != sequence {
            return Err(corrupt());
        }
        if let StoredAdministrationAuditRecordV1::Catalog(record) = record {
            last = Some(record);
        }
    }
    Ok(last)
}

impl CatalogRepository for RedbOperationalPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        let transaction = self.begin_read()?;
        validate_administration_stream_readonly(&transaction)?;
        let active_table = transaction
            .open_table(CATALOG_ACTIVE)
            .map_err(table_error)?;
        let active = active_catalog_from_table(&active_table)?;
        drop(active_table);
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        let last = last_catalog_activation(&audit)?;
        match (&active, last) {
            (None, None) => Ok(None),
            (Some(pointer), Some(record)) if record.activated() == pointer => {
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
}

impl AdministrationAuditReader for RedbOperationalPorts {
    fn scan_administration_audit(
        &self,
        request: AdministrationAuditScanRequest,
    ) -> Result<AdministrationAuditScan, StorageError> {
        let transaction = self.begin_read()?;
        validate_administration_stream_readonly(&transaction)?;
        let table = transaction.open_table(AUDIT).map_err(table_error)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut has_more = false;
        for entry in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let sequence = decode_audit_key(key.value()).map_err(|_| corrupt())?;
            if request.after().is_some_and(|after| sequence <= after) {
                continue;
            }
            let item = decode_administration_audit_record_v1(value.value())?;
            if item.value().administration_sequence() != sequence {
                return Err(corrupt());
            }
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
        let allocator = validate_administration_tail(transaction)?;
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
            let last = last_catalog_activation(&audit)?.ok_or_else(corrupt)?;
            if last.activated() != &requested {
                return Err(corrupt());
            }
            let sequence = last.administration_sequence();
            drop(audit);
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
        validate_administration_stream_readonly(&transaction)?;
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
        let durable = decoded_value(decode_administration_audit_record_v1(durable.value())?);
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
        let allocator = validate_administration_tail(transaction)?;

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
        access.commit_for(RedbTestOperation::QueryModuleAdministration)?;
        Ok(QueryModuleActivationResult::Activated {
            active: requested,
            administration_sequence: sequence,
        })
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

fn service_lifecycle<T>(
    table: &T,
    request_id: RequestId,
    sequences: &[AdministrationSequence],
) -> Result<Option<ServiceLifecycle>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut lifecycle = None;
    for sequence in sequences {
        let StoredAdministrationAuditRecordV1::Service(record) =
            read_audit_record(table, *sequence)?
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
            let key = encode_application_sequence_key(commit_sequence);
            let Some(commit_guard) = commits
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
            else {
                return Ok(false);
            };
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let commit = decoded_value(decode_commit_with_event_table(
                commit_guard.value(),
                &events,
            )?);
            drop(commit_guard);
            if commit.commit_sequence() != commit_sequence
                || commit.provenance_id() != provenance_id
            {
                return Ok(false);
            }
            drop(commits);
            let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
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
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let Some(target) = find_audit_record(&audit, administration_sequence)? else {
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
        let allocator = validate_administration_tail(transaction)?;
        let mut allowed = Vec::with_capacity(intents.len());
        for intent in intents {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let sequences = access.service_audit_sequences(intent.request_id())?;
            let lifecycle = service_lifecycle(&audit, intent.request_id(), &sequences)?;
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

        let append_count = u8::try_from(allowed.iter().filter(|allowed| **allowed).count())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        if append_count == 0 {
            access.abort()?;
            return Ok(vec![ServiceAuditAppendResult::PhaseConflict; intents.len()]);
        }
        let (assigned, next) = allocate_sequences(allocator, append_count)?;
        let mut assigned = assigned.into_iter();
        let mut results = Vec::with_capacity(intents.len());
        let mut index_delta = Vec::with_capacity(usize::from(append_count));
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
            index_delta.push((intent.request_id(), sequence));
            results.push(ServiceAuditAppendResult::Appended(record));
        }
        if assigned.next().is_some() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for_with_delta(
            RedbTestOperation::ServiceAudit,
            Some(TransientIndexDelta::ServiceAuditGroupAppended(index_delta)),
        )?;
        Ok(results)
    }
}

impl AuditedAdmissionRepository for RedbOperationalPorts {
    fn admit_or_resolve_audited_group(
        &self,
        requests: Vec<AuditedAdmissionRequestV1>,
    ) -> Result<Vec<AuditedAdmissionResultV1>, StorageError> {
        if requests.is_empty()
            || requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
            || requests
                .iter()
                .map(|request| request.started().request_id())
                .collect::<BTreeSet<_>>()
                .len()
                != requests.len()
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }

        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let allocator = validate_administration_tail(transaction)?;

        for request in &requests {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let sequences = access.service_audit_sequences(request.started().request_id())?;
            let lifecycle = service_lifecycle(&audit, request.started().request_id(), &sequences)?;
            drop(audit);
            if lifecycle.is_some()
                || request.started().phase() != ServiceAuditPhaseV1::Started
                || request.started().principal().is_none()
                || request.started().link() != ServiceAuditLinkV1::None
                || !service_link_is_valid(transaction, request.started())?
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }

        let count = u8::try_from(requests.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let (assigned, next) = allocate_sequences(allocator, count)?;
        let mut outputs = Vec::with_capacity(requests.len());
        let mut audit_delta = Vec::with_capacity(requests.len());
        for (request, sequence) in requests.iter().zip(assigned) {
            let (admission, created) = stage_admission(transaction, request.admission())?;
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
            audit_delta.push((request.started().request_id(), sequence));
            outputs.push(
                AuditedAdmissionResultV1::new(admission, started)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?,
            );
        }
        write_administration_allocator(transaction, allocator, next)?;
        access.commit_for_with_delta(
            RedbTestOperation::Admission,
            Some(TransientIndexDelta::ServiceAuditGroupAppended(audit_delta)),
        )?;
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
    let maximum_rows = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        .checked_mul(2)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    if intents.is_empty()
        || intents.len() > maximum_rows
        || intents
            .iter()
            .map(ServiceAuditAppendIntentV1::request_id)
            .collect::<BTreeSet<_>>()
            .len()
            > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let transaction = access.transaction()?;
    let allocator = validate_administration_tail(transaction)?;
    let mut audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut fused_starts: BTreeMap<riffdb_types::RequestId, ServiceAuditAppendIntentV1> =
        BTreeMap::new();
    for intent in intents {
        let sequences = access.service_audit_sequences(intent.request_id())?;
        let lifecycle = service_lifecycle(&audit, intent.request_id(), &sequences)?;
        let had_fused_start = fused_starts.contains_key(&intent.request_id());
        let allowed = if let Some(started) = fused_starts.get(&intent.request_id()) {
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
                    fused_starts.insert(intent.request_id(), intent.clone());
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
        if !allowed || !service_link_is_valid(transaction, intent)? {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    if !fused_starts.is_empty() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }

    let count =
        u8::try_from(intents.len()).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let (assigned, next) = allocate_sequences(allocator, count)?;
    let mut records = Vec::with_capacity(intents.len());
    for (intent, sequence) in intents.iter().zip(assigned) {
        let record = StoredServiceAuditRecordV1::from_intent(sequence, intent);
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
        records.push(record);
    }
    drop(audit);
    write_administration_allocator(transaction, allocator, next)?;
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
    let allocator = validate_administration_tail(transaction)?;
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
    let allocator = validate_administration_tail(transaction)?;
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
    let StoredAdministrationAuditRecordV1::Capability(transition) =
        read_audit_record(&audit, marker.administration_sequence())?
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
        read_audit_record(&audit, started_sequence)?
    else {
        return Err(corrupt());
    };
    let sequences = access.service_audit_sequences(started.request_id())?;
    let lifecycle_valid = matches!(
        service_lifecycle(&audit, started.request_id(), &sequences)?,
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
        let allocator = validate_administration_tail(transaction)?;
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
        access.commit_for_with_delta(
            RedbTestOperation::CapabilityBootstrap,
            Some(TransientIndexDelta::ServiceAuditAppended {
                request_id: intent.start().request_id(),
                sequence: started_sequence,
            }),
        )?;
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
        service_lifecycle(&audit, intent.start().request_id(), &sequences)?.is_none();
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
    access.commit_for_with_delta(
        RedbTestOperation::CapabilityBootstrap,
        Some(TransientIndexDelta::ServiceAuditAppended {
            request_id: intent.start().request_id(),
            sequence: started_sequence,
        }),
    )?;
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
        ServiceIngressKindV1, TenantScope, Timestamp,
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
        request: u8,
        phase: ServiceAuditPhaseV1,
        seconds: i64,
    ) -> ServiceAuditAppendIntentV1 {
        ServiceAuditAppendIntentV1::new(
            request_id(request),
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
    fn fused_maximum_command_group_allocates_two_audit_rows_per_command() {
        let (_path, ports) = initialized_ports("fused-audit-maximum");
        let group_bound = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
        let mut intents = Vec::with_capacity(group_bound * 2);
        for request in 1..=u8::try_from(group_bound).expect("group bound") {
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
            128
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
}
