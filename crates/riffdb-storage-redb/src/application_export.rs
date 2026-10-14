//! Durable exact application-export operation repository and immutable source.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use redb::ReadableTable;
use riffdb_storage_api::{
    ApplicationExportEventRecordV1, ApplicationExportOperationRepository,
    ApplicationExportOperationWriteResultV1, ApplicationExportSnapshotPort,
    ApplicationExportSnapshotReader, ApplicationExportSourcePageV1,
    ApplicationExportSourceRecordV1, DurableCodecErrorKind, MAX_ACTIVE_APPLICATION_EXPORTS,
    MAX_APPLICATION_EXPORT_CONTINUATION_BYTES, MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES,
    MAX_RETAINED_REACTIVE_MODULES, StorageError, StorageErrorKind, StorageScanLimit,
    StoredApplicationExportOperationV1,
};
use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportOperationId, ApplicationExportSnapshotBindingV1,
    ContractLineage, EntityKeyBuilder, EntityTypeId, QueryModuleHash, ReactiveModuleHash,
};

use crate::codec::{
    decode_active_catalog_pointer_v1, decode_application_export_operation_v1,
    decode_database_identity_v1, decode_entity_record_v1, decode_history_incarnation_v1,
    decode_index_entry_v2, decode_provenance_record_v1, decode_query_module_administration_v1,
    decode_reactive_module_v1, encode_application_export_operation_v1,
};
use crate::error::{codec_error, precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::journal::JournalTable;
use crate::keys::{
    decode_audit_key, decode_entity_key, decode_event_key, decode_index_entry_key,
    decode_provenance_key, encode_active_query_module_key, encode_application_sequence_key,
    encode_contract_bundle_key, encode_entity_key, encode_provenance_key,
};
use crate::layout::{
    APPLICATION_EXPORT_OPERATIONS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, CONTRACT_BUNDLES,
    META_DATABASE_ID, META_HISTORY_INCARNATION, QUERY_MODULE_ACTIVE, REACTIVE_MODULES,
};
use crate::store::{RedbOperationalPorts, RedbReadAccess};

const EXPORT_CONTINUATION_MAGIC: &[u8; 4] = b"RDX1";
const EXPORT_CONTINUATION_FIXED_BYTES: usize = 14;
const CONTINUATION_PHASE_TABLE: u8 = 1;
const CONTINUATION_PHASE_COMMAND_EVENTS: u8 = 2;
const CONTINUATION_PHASE_LEGACY_PROVENANCE: u8 = 3;
const CONTINUATION_PHASE_COMMAND_PROVENANCE: u8 = 4;

#[derive(Clone, Eq, PartialEq)]
struct ExportSourceContinuationV1 {
    phase: u8,
    key: Vec<u8>,
    command_ordinal: u16,
    member_ordinal: u32,
}

impl ExportSourceContinuationV1 {
    fn table(key: Vec<u8>) -> Self {
        Self {
            phase: CONTINUATION_PHASE_TABLE,
            key,
            command_ordinal: 0,
            member_ordinal: 0,
        }
    }

    fn command(
        class: ApplicationExportClassV1,
        key: Vec<u8>,
        command_ordinal: usize,
        member_ordinal: usize,
    ) -> Result<Self, StorageError> {
        let phase = match class {
            ApplicationExportClassV1::Event => CONTINUATION_PHASE_COMMAND_EVENTS,
            ApplicationExportClassV1::Provenance => CONTINUATION_PHASE_COMMAND_PROVENANCE,
            ApplicationExportClassV1::Entity | ApplicationExportClassV1::PublicAudit => {
                return Err(corrupt());
            }
        };
        Ok(Self {
            phase,
            key,
            command_ordinal: u16::try_from(command_ordinal).map_err(|_| corrupt())?,
            member_ordinal: u32::try_from(member_ordinal).map_err(|_| corrupt())?,
        })
    }

    fn legacy_provenance(key: Vec<u8>) -> Self {
        Self {
            phase: CONTINUATION_PHASE_LEGACY_PROVENANCE,
            key,
            command_ordinal: 0,
            member_ordinal: 0,
        }
    }

    fn encode(&self, class: ApplicationExportClassV1) -> Result<Box<[u8]>, StorageError> {
        let key_len = u16::try_from(self.key.len()).map_err(|_| corrupt())?;
        let total = EXPORT_CONTINUATION_FIXED_BYTES
            .checked_add(self.key.len())
            .ok_or_else(corrupt)?;
        if self.key.is_empty() || total > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES {
            return Err(corrupt());
        }
        let mut encoded = Vec::with_capacity(total);
        encoded.extend_from_slice(EXPORT_CONTINUATION_MAGIC);
        encoded.push(class.tag());
        encoded.push(self.phase);
        encoded.extend_from_slice(&key_len.to_be_bytes());
        encoded.extend_from_slice(&self.key);
        encoded.extend_from_slice(&self.command_ordinal.to_be_bytes());
        encoded.extend_from_slice(&self.member_ordinal.to_be_bytes());
        Ok(encoded.into_boxed_slice())
    }

    fn decode(class: ApplicationExportClassV1, encoded: &[u8]) -> Result<Self, StorageError> {
        if encoded.len() < EXPORT_CONTINUATION_FIXED_BYTES
            || encoded.len() > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
            || &encoded[..4] != EXPORT_CONTINUATION_MAGIC
            || encoded[4] != class.tag()
        {
            return Err(corrupt());
        }
        let phase = encoded[5];
        let key_len = usize::from(u16::from_be_bytes([encoded[6], encoded[7]]));
        let expected = EXPORT_CONTINUATION_FIXED_BYTES
            .checked_add(key_len)
            .ok_or_else(corrupt)?;
        if key_len == 0 || encoded.len() != expected {
            return Err(corrupt());
        }
        let command_start = 8 + key_len;
        let command_ordinal =
            u16::from_be_bytes([encoded[command_start], encoded[command_start + 1]]);
        let member_start = command_start + 2;
        let member_ordinal = u32::from_be_bytes([
            encoded[member_start],
            encoded[member_start + 1],
            encoded[member_start + 2],
            encoded[member_start + 3],
        ]);
        let valid_phase = match class {
            ApplicationExportClassV1::Entity | ApplicationExportClassV1::PublicAudit => {
                phase == CONTINUATION_PHASE_TABLE && command_ordinal == 0 && member_ordinal == 0
            }
            ApplicationExportClassV1::Event => phase == CONTINUATION_PHASE_COMMAND_EVENTS,
            ApplicationExportClassV1::Provenance => {
                (phase == CONTINUATION_PHASE_LEGACY_PROVENANCE
                    && command_ordinal == 0
                    && member_ordinal == 0)
                    || phase == CONTINUATION_PHASE_COMMAND_PROVENANCE
            }
        };
        if !valid_phase {
            return Err(corrupt());
        }
        Ok(Self {
            phase,
            key: encoded[8..command_start].to_vec(),
            command_ordinal,
            member_ordinal,
        })
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

/// One pinned redb/composite publication root. The mutex protects redb's
/// transaction value without serializing unrelated database readers; each
/// export operation owns its own root.
struct RedbApplicationExportSnapshot {
    ports: RedbOperationalPorts,
    access: Mutex<RedbReadAccess>,
    binding: ApplicationExportSnapshotBindingV1,
    contract_bundle_bytes: Arc<[u8]>,
    lineage: ContractLineage,
}

impl RedbApplicationExportSnapshot {
    fn decode_source_record(
        &self,
        access: &RedbReadAccess,
        class: ApplicationExportClassV1,
        key: &[u8],
        encoded: &[u8],
    ) -> Result<ApplicationExportSourceRecordV1, StorageError> {
        match class {
            ApplicationExportClassV1::Entity => {
                decode_entity_source_record(&self.lineage, key, encoded)
            }
            ApplicationExportClassV1::Event => {
                let physical = decode_event_key(key).map_err(|_| corrupt())?;
                let record = match riffdb_storage_api::decode_durable_event_v2(encoded) {
                    Ok(record) => {
                        let record = record.into_parts().0;
                        if record.event_id() != physical
                            || record.policy_anchor().contract().lineage() != &self.lineage
                        {
                            return Err(corrupt());
                        }
                        ApplicationExportEventRecordV1::V2(record)
                    }
                    Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                        let record = riffdb_storage_api::decode_durable_event_v1(encoded)
                            .map_err(codec_error)?
                            .into_parts()
                            .0;
                        if record.event_id() != physical {
                            return Err(corrupt());
                        }
                        ApplicationExportEventRecordV1::V1(record)
                    }
                    Err(error) => return Err(codec_error(error)),
                };
                Ok(ApplicationExportSourceRecordV1::Event(Box::new(record)))
            }
            ApplicationExportClassV1::Provenance => {
                let physical = decode_provenance_key(key).map_err(|_| corrupt())?;
                let record = decode_provenance_record_v1(encoded)?.into_parts().0;
                if record.provenance_id() != physical
                    || record.identity().contract_lineage() != &self.lineage
                {
                    return Err(corrupt());
                }
                Ok(ApplicationExportSourceRecordV1::Provenance(Box::new(
                    record,
                )))
            }
            ApplicationExportClassV1::PublicAudit => {
                let physical = decode_audit_key(key).map_err(|_| corrupt())?;
                let record = crate::administration::read_administration_record_access(
                    &self.ports,
                    access,
                    physical,
                )?;
                Ok(ApplicationExportSourceRecordV1::PublicAudit(Box::new(
                    record,
                )))
            }
        }
    }

    fn read_source_page_range(
        &self,
        class: ApplicationExportClassV1,
        start: Vec<u8>,
        end_exclusive: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let requested = usize::from(limit.get());
        let inspected = requested
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let table = match class {
            ApplicationExportClassV1::Entity => JournalTable::Entities,
            ApplicationExportClassV1::Event => JournalTable::Events,
            ApplicationExportClassV1::Provenance => JournalTable::Provenance,
            ApplicationExportClassV1::PublicAudit => JournalTable::Audit,
        };
        let rows = access.read_range_to(table, &start, end_exclusive, inspected)?;
        let mut records = Vec::with_capacity(requested.min(rows.len()));
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        let mut stopped_early = false;
        for (key, encoded) in rows.iter().take(requested) {
            let next_bytes = encoded_bytes
                .checked_add(encoded.len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
                stopped_early = true;
                break;
            }
            records.push(self.decode_source_record(&access, class, key, encoded)?);
            encoded_bytes = next_bytes;
            continuation = Some(ExportSourceContinuationV1::table(key.to_vec()).encode(class)?);
        }
        let exact_end = !stopped_early && rows.len() <= records.len();
        if exact_end {
            continuation = None;
        } else if continuation.is_none() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        ApplicationExportSourcePageV1::new_with_inspected_work(
            class,
            records,
            continuation,
            exact_end,
            encoded_bytes,
            rows.len(),
        )
        .map_err(|error| match error {
            riffdb_storage_api::StorageValueError::LimitExceeded => {
                storage_error(StorageErrorKind::LimitExceeded)
            }
            _ => storage_error(StorageErrorKind::InvariantViolation),
        })
    }

    fn read_command_event_page(
        &self,
        continuation: Option<ExportSourceContinuationV1>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let requested = usize::from(limit.get());
        let start = continuation
            .as_ref()
            .map_or_else(Vec::new, |cursor| cursor.key.clone());
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let rows = access.read_range_to(JournalTable::Commits, &start, None, 1)?;
        let Some((physical_key, encoded)) = rows.first() else {
            return if continuation.is_none() && self.binding.application_frontier().is_none() {
                source_page(
                    ApplicationExportClassV1::Event,
                    Vec::new(),
                    None,
                    true,
                    0,
                    0,
                )
            } else {
                Err(corrupt())
            };
        };
        if continuation
            .as_ref()
            .is_some_and(|cursor| cursor.key.as_slice() != physical_key.as_ref())
        {
            return Err(corrupt());
        }
        let physical_sequence =
            crate::keys::decode_application_sequence_key(physical_key).map_err(|_| corrupt())?;
        if continuation.is_none() && physical_sequence != riffdb_types::CommitSequence::first() {
            return Err(corrupt());
        }
        let members = crate::command_authority::application_export_evidence_in_physical_row_access(
            &access,
            encoded,
            physical_sequence,
        )?;
        let mut command_ordinal = continuation
            .as_ref()
            .map_or(0, |cursor| usize::from(cursor.command_ordinal));
        let mut event_ordinal = continuation.as_ref().map_or(0, |cursor| {
            usize::try_from(cursor.member_ordinal).unwrap_or(usize::MAX)
        });
        if command_ordinal >= members.len() {
            return Err(corrupt());
        }
        let mut records = Vec::with_capacity(requested);
        let mut encoded_bytes = 0usize;
        let mut inspected_work = 1usize; // physical command row
        while command_ordinal < members.len() {
            let member = &members[command_ordinal];
            if member.commit().plan().contract_lineage() != &self.lineage {
                return Err(corrupt());
            }
            let events = member.commit().events();
            if event_ordinal > events.len() || (event_ordinal == events.len() && !events.is_empty())
            {
                return Err(corrupt());
            }
            inspected_work = inspected_work
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            while event_ordinal < events.len() {
                if records.len() == requested {
                    let cursor = ExportSourceContinuationV1::command(
                        ApplicationExportClassV1::Event,
                        physical_key.to_vec(),
                        command_ordinal,
                        event_ordinal,
                    )?
                    .encode(ApplicationExportClassV1::Event)?;
                    return source_page(
                        ApplicationExportClassV1::Event,
                        records,
                        Some(cursor),
                        false,
                        encoded_bytes,
                        inspected_work,
                    );
                }
                let event = &events[event_ordinal];
                let expected_ordinal = u32::try_from(event_ordinal).map_err(|_| corrupt())?;
                if event.event_id().commit_sequence() != member.commit().commit_sequence()
                    || event.event_id().event_ordinal() != expected_ordinal
                    || event
                        .policy_anchor()
                        .is_some_and(|anchor| anchor.contract().lineage() != &self.lineage)
                {
                    return Err(corrupt());
                }
                let (event, charge) = application_export_event(event)?;
                inspected_work = inspected_work
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                let next_bytes = encoded_bytes
                    .checked_add(charge)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
                    if records.is_empty() {
                        return Err(storage_error(StorageErrorKind::LimitExceeded));
                    }
                    let cursor = ExportSourceContinuationV1::command(
                        ApplicationExportClassV1::Event,
                        physical_key.to_vec(),
                        command_ordinal,
                        event_ordinal,
                    )?
                    .encode(ApplicationExportClassV1::Event)?;
                    return source_page(
                        ApplicationExportClassV1::Event,
                        records,
                        Some(cursor),
                        false,
                        encoded_bytes,
                        inspected_work,
                    );
                }
                records.push(ApplicationExportSourceRecordV1::Event(Box::new(event)));
                encoded_bytes = next_bytes;
                event_ordinal += 1;
            }
            command_ordinal += 1;
            event_ordinal = 0;
        }
        let last = members
            .last()
            .ok_or_else(corrupt)?
            .commit()
            .commit_sequence();
        self.finish_command_page(
            ApplicationExportClassV1::Event,
            last,
            records,
            encoded_bytes,
            inspected_work,
        )
    }

    fn finish_command_page(
        &self,
        class: ApplicationExportClassV1,
        last: riffdb_types::CommitSequence,
        records: Vec<ApplicationExportSourceRecordV1>,
        encoded_bytes: usize,
        inspected_work: usize,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let frontier = self.binding.application_frontier().ok_or_else(corrupt)?;
        if last > frontier {
            return Err(corrupt());
        }
        if last == frontier {
            return source_page(class, records, None, true, encoded_bytes, inspected_work);
        }
        let next = last.checked_next().ok_or_else(corrupt)?;
        let cursor = ExportSourceContinuationV1::command(
            class,
            encode_application_sequence_key(next).to_vec(),
            0,
            0,
        )?
        .encode(class)?;
        source_page(
            class,
            records,
            Some(cursor),
            false,
            encoded_bytes,
            inspected_work,
        )
    }

    fn read_provenance_page(
        &self,
        continuation: Option<ExportSourceContinuationV1>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let requested = usize::from(limit.get());
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut records = Vec::with_capacity(requested);
        let mut encoded_bytes = 0usize;
        let mut inspected_work = 0usize;
        let mut command_cursor = None;
        if continuation
            .as_ref()
            .is_none_or(|cursor| cursor.phase == CONTINUATION_PHASE_LEGACY_PROVENANCE)
        {
            let mut after_key = continuation.as_ref().map(|cursor| cursor.key.clone());
            for _ in 0..riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES {
                let mut start = after_key.clone().unwrap_or_default();
                if after_key.is_some() {
                    start.push(0);
                }
                let rows = access.read_range_to(JournalTable::Provenance, &start, None, 1)?;
                let Some((key, encoded)) = rows.first() else {
                    command_cursor = None;
                    break;
                };
                inspected_work = inspected_work
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                let physical = decode_provenance_key(key).map_err(|_| corrupt())?;
                match riffdb_storage_api::decode_provenance_record_v1(encoded) {
                    Ok(record) => {
                        let record = record.into_parts().0;
                        if record.provenance_id() != physical
                            || record.identity().contract_lineage() != &self.lineage
                        {
                            return Err(corrupt());
                        }
                        let charge = riffdb_storage_api::encode_provenance_record_v1(&record)
                            .map_err(codec_error)?
                            .encoded_content_charge();
                        let next_bytes = encoded_bytes
                            .checked_add(charge.get())
                            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                        if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
                            if records.is_empty() {
                                return Err(storage_error(StorageErrorKind::LimitExceeded));
                            }
                            let cursor = ExportSourceContinuationV1::legacy_provenance(
                                after_key.ok_or_else(corrupt)?,
                            )
                            .encode(ApplicationExportClassV1::Provenance)?;
                            return source_page(
                                ApplicationExportClassV1::Provenance,
                                records,
                                Some(cursor),
                                false,
                                encoded_bytes,
                                inspected_work,
                            );
                        }
                        records.push(ApplicationExportSourceRecordV1::Provenance(Box::new(
                            record,
                        )));
                        encoded_bytes = next_bytes;
                    }
                    Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                        let locator = riffdb_storage_api::decode_command_locator_v1(encoded)
                            .map_err(codec_error)?
                            .into_parts()
                            .0;
                        let command = crate::command_authority::command_member_at_access(
                            &access,
                            locator.commit_sequence(),
                        )?
                        .ok_or_else(corrupt)?;
                        if command.base().provenance().provenance_id() != physical {
                            return Err(corrupt());
                        }
                    }
                    Err(error) => return Err(codec_error(error)),
                }
                after_key = Some(key.to_vec());
                if records.len() == requested {
                    let cursor = ExportSourceContinuationV1::legacy_provenance(
                        after_key.ok_or_else(corrupt)?,
                    )
                    .encode(ApplicationExportClassV1::Provenance)?;
                    return source_page(
                        ApplicationExportClassV1::Provenance,
                        records,
                        Some(cursor),
                        false,
                        encoded_bytes,
                        inspected_work,
                    );
                }
            }
            if command_cursor.is_none()
                && inspected_work == riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES
            {
                let cursor =
                    ExportSourceContinuationV1::legacy_provenance(after_key.ok_or_else(corrupt)?)
                        .encode(ApplicationExportClassV1::Provenance)?;
                return source_page(
                    ApplicationExportClassV1::Provenance,
                    records,
                    Some(cursor),
                    false,
                    encoded_bytes,
                    inspected_work,
                );
            }
        } else {
            command_cursor = continuation;
        }
        self.read_command_provenance_page(
            &access,
            command_cursor,
            requested,
            records,
            encoded_bytes,
            inspected_work,
        )
    }

    fn read_command_provenance_page(
        &self,
        access: &RedbReadAccess,
        continuation: Option<ExportSourceContinuationV1>,
        requested: usize,
        mut records: Vec<ApplicationExportSourceRecordV1>,
        mut encoded_bytes: usize,
        mut inspected_work: usize,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let start = continuation
            .as_ref()
            .map_or_else(Vec::new, |cursor| cursor.key.clone());
        let rows = access.read_range_to(JournalTable::Commits, &start, None, 1)?;
        let Some((physical_key, encoded)) = rows.first() else {
            return if continuation.is_none() && self.binding.application_frontier().is_none() {
                source_page(
                    ApplicationExportClassV1::Provenance,
                    records,
                    None,
                    true,
                    encoded_bytes,
                    inspected_work,
                )
            } else {
                Err(corrupt())
            };
        };
        if continuation
            .as_ref()
            .is_some_and(|cursor| cursor.key.as_slice() != physical_key.as_ref())
        {
            return Err(corrupt());
        }
        let physical_sequence =
            crate::keys::decode_application_sequence_key(physical_key).map_err(|_| corrupt())?;
        if continuation.is_none() && physical_sequence != riffdb_types::CommitSequence::first() {
            return Err(corrupt());
        }
        let members = crate::command_authority::application_export_evidence_in_physical_row_access(
            access,
            encoded,
            physical_sequence,
        )?;
        inspected_work = inspected_work
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let mut command_ordinal = continuation
            .as_ref()
            .map_or(0, |cursor| usize::from(cursor.command_ordinal));
        if continuation
            .as_ref()
            .is_some_and(|cursor| cursor.member_ordinal != 0)
            || command_ordinal >= members.len()
        {
            return Err(corrupt());
        }
        while command_ordinal < members.len() {
            if records.len() == requested {
                let cursor = ExportSourceContinuationV1::command(
                    ApplicationExportClassV1::Provenance,
                    physical_key.to_vec(),
                    command_ordinal,
                    0,
                )?
                .encode(ApplicationExportClassV1::Provenance)?;
                return source_page(
                    ApplicationExportClassV1::Provenance,
                    records,
                    Some(cursor),
                    false,
                    encoded_bytes,
                    inspected_work,
                );
            }
            let member = &members[command_ordinal];
            if member.commit().plan().contract_lineage() != &self.lineage {
                return Err(corrupt());
            }
            inspected_work = inspected_work
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if let Some(provenance) = member.provenance() {
                if provenance.identity().contract_lineage() != &self.lineage
                    || provenance.commit_sequence() != member.commit().commit_sequence()
                {
                    return Err(corrupt());
                }
                let (duplicate, probes) = self.command_provenance_duplicate(access, provenance)?;
                inspected_work = inspected_work
                    .checked_add(probes)
                    .and_then(|work| work.checked_add(1))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if !duplicate {
                    let charge = riffdb_storage_api::encode_provenance_record_v1(provenance)
                        .map_err(codec_error)?
                        .encoded_content_charge();
                    let next_bytes = encoded_bytes
                        .checked_add(charge.get())
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                    if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
                        if records.is_empty() {
                            return Err(storage_error(StorageErrorKind::LimitExceeded));
                        }
                        let cursor = ExportSourceContinuationV1::command(
                            ApplicationExportClassV1::Provenance,
                            physical_key.to_vec(),
                            command_ordinal,
                            0,
                        )?
                        .encode(ApplicationExportClassV1::Provenance)?;
                        return source_page(
                            ApplicationExportClassV1::Provenance,
                            records,
                            Some(cursor),
                            false,
                            encoded_bytes,
                            inspected_work,
                        );
                    }
                    records.push(ApplicationExportSourceRecordV1::Provenance(Box::new(
                        provenance.clone(),
                    )));
                    encoded_bytes = next_bytes;
                }
            }
            command_ordinal += 1;
        }
        let last = members
            .last()
            .ok_or_else(corrupt)?
            .commit()
            .commit_sequence();
        self.finish_command_page(
            ApplicationExportClassV1::Provenance,
            last,
            records,
            encoded_bytes,
            inspected_work,
        )
    }

    fn command_provenance_duplicate(
        &self,
        access: &RedbReadAccess,
        provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
    ) -> Result<(bool, usize), StorageError> {
        let key = encode_provenance_key(provenance.provenance_id());
        let mut duplicate = false;
        let probes = 1usize;
        if let Some(encoded) = access.read_value(JournalTable::Provenance, &key)? {
            match riffdb_storage_api::decode_provenance_record_v1(&encoded) {
                Ok(record) => {
                    let record = record.into_parts().0;
                    if record.provenance_id() != provenance.provenance_id()
                        || record.identity().contract_lineage() != &self.lineage
                        || &record != provenance
                    {
                        return Err(corrupt());
                    }
                    duplicate = true;
                }
                Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                    let locator = riffdb_storage_api::decode_command_locator_v1(&encoded)
                        .map_err(codec_error)?
                        .into_parts()
                        .0;
                    if locator.commit_sequence() != provenance.commit_sequence() {
                        return Err(corrupt());
                    }
                }
                Err(error) => return Err(codec_error(error)),
            }
        }
        Ok((duplicate, probes))
    }
}

fn application_export_event(
    event: &riffdb_storage_api::StoredDurableEventV1,
) -> Result<(ApplicationExportEventRecordV1, usize), StorageError> {
    if let Some(anchor) = event.policy_anchor() {
        let event = riffdb_storage_api::StoredDurableEventV2::new(
            event.event_id(),
            event.event_type_id(),
            event.payload().clone(),
            event.event_hash(),
            anchor.clone(),
        )
        .map_err(|_| corrupt())?;
        let charge = riffdb_storage_api::encode_durable_event_v2(&event)
            .map_err(codec_error)?
            .encoded_content_charge()
            .get();
        Ok((ApplicationExportEventRecordV1::V2(event), charge))
    } else {
        let charge = riffdb_storage_api::encode_durable_event_v1(event)
            .map_err(codec_error)?
            .encoded_content_charge()
            .get();
        Ok((ApplicationExportEventRecordV1::V1(event.clone()), charge))
    }
}

fn source_page(
    class: ApplicationExportClassV1,
    records: Vec<ApplicationExportSourceRecordV1>,
    continuation: Option<Box<[u8]>>,
    exact_end: bool,
    encoded_bytes: usize,
    inspected_work: usize,
) -> Result<ApplicationExportSourcePageV1, StorageError> {
    ApplicationExportSourcePageV1::new_with_inspected_work(
        class,
        records,
        continuation,
        exact_end,
        encoded_bytes,
        inspected_work,
    )
    .map_err(|error| match error {
        riffdb_storage_api::StorageValueError::LimitExceeded => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        _ => storage_error(StorageErrorKind::InvariantViolation),
    })
}

fn decode_entity_source_record(
    lineage: &ContractLineage,
    key: &[u8],
    encoded: &[u8],
) -> Result<ApplicationExportSourceRecordV1, StorageError> {
    let physical = decode_entity_key(key).map_err(|_| corrupt())?;
    let record = decode_entity_record_v1(encoded)?.into_parts().0;
    if record.target().key() != &physical || record.schema_binding().lineage() != lineage {
        return Err(corrupt());
    }
    Ok(ApplicationExportSourceRecordV1::Entity(Box::new(record)))
}

pub(crate) fn read_entity_type_page_at(
    access: &RedbReadAccess,
    lineage: &ContractLineage,
    entity_type: EntityTypeId,
    after: Option<&[u8]>,
    limit: StorageScanLimit,
) -> Result<ApplicationExportSourcePageV1, StorageError> {
    let prefix = EntityKeyBuilder::new(entity_type).as_bytes().to_vec();
    if after.is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
            || !value.starts_with(&prefix)
    }) {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let mut start = after.map_or_else(|| prefix.clone(), ToOwned::to_owned);
    if after.is_some() {
        start.push(0);
    }
    let end = exclusive_prefix_end(&prefix)
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    read_source_page_range_at(
        access,
        ApplicationExportClassV1::Entity,
        start,
        Some(&end),
        limit,
        |key, encoded| decode_entity_source_record(lineage, key, encoded),
    )
}

fn read_source_page_range_at(
    access: &RedbReadAccess,
    class: ApplicationExportClassV1,
    start: Vec<u8>,
    end_exclusive: Option<&[u8]>,
    limit: StorageScanLimit,
    decode: impl Fn(&[u8], &[u8]) -> Result<ApplicationExportSourceRecordV1, StorageError>,
) -> Result<ApplicationExportSourcePageV1, StorageError> {
    let requested = usize::from(limit.get());
    let inspected = requested
        .checked_add(1)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let table = match class {
        ApplicationExportClassV1::Entity => JournalTable::Entities,
        ApplicationExportClassV1::Event => JournalTable::Events,
        ApplicationExportClassV1::Provenance => JournalTable::Provenance,
        ApplicationExportClassV1::PublicAudit => JournalTable::Audit,
    };
    let rows = access.read_range_to(table, &start, end_exclusive, inspected)?;
    let mut records = Vec::with_capacity(requested.min(rows.len()));
    let mut encoded_bytes = 0usize;
    let mut continuation = None;
    let mut stopped_early = false;
    for (key, encoded) in rows.iter().take(requested) {
        let next_bytes = encoded_bytes
            .checked_add(encoded.len())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
            stopped_early = true;
            break;
        }
        records.push(decode(key, encoded)?);
        encoded_bytes = next_bytes;
        continuation = Some(key.clone());
    }
    let exact_end = !stopped_early && rows.len() <= records.len();
    if exact_end {
        continuation = None;
    } else if continuation.is_none() {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    ApplicationExportSourcePageV1::new(class, records, continuation, exact_end, encoded_bytes)
        .map_err(|error| match error {
            riffdb_storage_api::StorageValueError::LimitExceeded => {
                storage_error(StorageErrorKind::LimitExceeded)
            }
            _ => storage_error(StorageErrorKind::InvariantViolation),
        })
}

impl ApplicationExportSnapshotReader for RedbApplicationExportSnapshot {
    fn binding(&self) -> &ApplicationExportSnapshotBindingV1 {
        &self.binding
    }

    fn contract_bundle_bytes(&self) -> &[u8] {
        &self.contract_bundle_bytes
    }

    fn read_application_export_source_page(
        &self,
        class: ApplicationExportClassV1,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let continuation = after
            .map(|encoded| ExportSourceContinuationV1::decode(class, encoded))
            .transpose()?;
        match class {
            ApplicationExportClassV1::Event => self.read_command_event_page(continuation, limit),
            ApplicationExportClassV1::Provenance => self.read_provenance_page(continuation, limit),
            ApplicationExportClassV1::Entity | ApplicationExportClassV1::PublicAudit => {
                let mut start = continuation
                    .as_ref()
                    .map_or_else(Vec::new, |cursor| cursor.key.clone());
                if continuation.is_some() {
                    start.push(0);
                }
                self.read_source_page_range(class, start, None, limit)
            }
        }
    }

    fn read_application_export_entity_page(
        &self,
        entity_type: EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        read_entity_type_page_at(&access, &self.lineage, entity_type, after, limit)
    }

    fn application_export_indexed_relationship_exists(
        &self,
        index_prefix: &[u8],
        partition: &riffdb_types::PartitionKey,
    ) -> Result<bool, StorageError> {
        if index_prefix.is_empty() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let upper = exclusive_prefix_end(index_prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let rows = access.read_range(
            JournalTable::SecondaryIndexes,
            index_prefix,
            &upper,
            riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES,
        )?;
        if rows.len() == riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        for (key, encoded) in rows {
            let key = decode_index_entry_key(&key).map_err(|_| corrupt())?;
            let entry = decode_index_entry_v2(&encoded)?.into_parts().0;
            if entry.key() != &key {
                return Err(corrupt());
            }
            if entry.partition_key() == partition {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn read_application_export_policy_anchor(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let Some(encoded) =
            access.read_value(JournalTable::Entities, encode_entity_key(target.key()))?
        else {
            return Ok(None);
        };
        let record = decode_entity_record_v1(&encoded)?.into_parts().0;
        if record.target() != target || record.schema_binding().lineage() != &self.lineage {
            return Err(corrupt());
        }
        Ok(Some(record))
    }
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].checked_add(1)?;
    upper.truncate(position + 1);
    Some(upper)
}

impl ApplicationExportSnapshotPort for RedbOperationalPorts {
    fn capture_application_export_snapshot(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Arc<dyn ApplicationExportSnapshotReader>, StorageError> {
        let access = self.begin_composite_read()?;
        let database_id = access
            .read_value(JournalTable::Meta, META_DATABASE_ID.as_bytes())?
            .ok_or_else(corrupt)
            .and_then(|value| decode_database_identity_v1(&value).map(|item| *item.value()))?;
        let history_incarnation = access
            .read_value(JournalTable::Meta, META_HISTORY_INCARNATION.as_bytes())?
            .ok_or_else(corrupt)
            .and_then(|value| decode_history_incarnation_v1(&value).map(|item| *item.value()))?;
        let history_incarnation = NonZeroU64::new(history_incarnation).ok_or_else(corrupt)?;

        let catalog = access.open_table(CATALOG_ACTIVE).map_err(table_error)?;
        let active = catalog
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let active = decode_active_catalog_pointer_v1(active.value())?
            .into_parts()
            .0;
        if active.lineage() != lineage {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        drop(catalog);

        let bundle_key = encode_contract_bundle_key(lineage, active.contract_version())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let bundles = access.open_table(CONTRACT_BUNDLES).map_err(table_error)?;
        let bundle = bundles
            .get(bundle_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let bundle = crate::codec::decode_contract_bundle_v1(bundle.value())?
            .into_parts()
            .0;
        if !active.matches_bundle(&bundle) {
            return Err(corrupt());
        }
        drop(bundles);

        let active_query_key = encode_active_query_module_key(
            lineage,
            active.contract_version(),
            active.bundle_hash(),
        )
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let active_queries = access
            .open_table(QUERY_MODULE_ACTIVE)
            .map_err(table_error)?;
        let query_modules = active_queries
            .get(active_query_key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|encoded| {
                decode_query_module_administration_v1(encoded.value()).and_then(|item| {
                    let record = item.into_parts().0;
                    let pointer = record.activated();
                    if pointer.contract_lineage() != lineage
                        || pointer.contract_version() != active.contract_version()
                        || pointer.contract_bundle_hash() != active.bundle_hash()
                    {
                        return Err(corrupt());
                    }
                    Ok(pointer.module_hash())
                })
            })
            .transpose()?
            .into_iter()
            .collect::<Vec<QueryModuleHash>>();
        drop(active_queries);

        let reactive = access.open_table(REACTIVE_MODULES).map_err(table_error)?;
        let mut reactive_modules = Vec::<ReactiveModuleHash>::new();
        for row in reactive.iter().map_err(precommit_storage_error)? {
            if reactive_modules.len() == MAX_RETAINED_REACTIVE_MODULES {
                return Err(corrupt());
            }
            let (key, encoded) = row.map_err(precommit_storage_error)?;
            let module = decode_reactive_module_v1(encoded.value())?.into_parts().0;
            if key.value() != module.module_hash().as_bytes() {
                return Err(corrupt());
            }
            if module.contract_lineage() == lineage
                && module.contract_version() == active.contract_version()
                && module.contract_bundle_hash() == active.bundle_hash()
            {
                reactive_modules.push(module.module_hash());
            }
        }
        reactive_modules.sort_unstable();
        if reactive_modules.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corrupt());
        }

        let binding = ApplicationExportSnapshotBindingV1::new(
            database_id,
            history_incarnation,
            access.application_frontier()?,
            access.administration_frontier()?,
            active.contract_version(),
            active.bundle_hash(),
            query_modules,
            reactive_modules,
        )
        .map_err(|_| corrupt())?;
        Ok(Arc::new(RedbApplicationExportSnapshot {
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            access: Mutex::new(access),
            binding,
            contract_bundle_bytes: Arc::from(bundle.canonical_bytes()),
            lineage: lineage.clone(),
        }))
    }
}

fn decode_row(
    key: &[u8],
    value: &[u8],
) -> Result<StoredApplicationExportOperationV1, StorageError> {
    let operation = decode_application_export_operation_v1(value)?
        .into_parts()
        .0;
    if key != operation.operation_id().as_bytes() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(operation)
}

impl ApplicationExportOperationRepository for RedbOperationalPorts {
    fn read_application_export_operation(
        &self,
        operation_id: ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperationV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let Some(value) = table
            .get(operation_id.as_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        decode_row(operation_id.as_bytes(), value.value()).map(Some)
    }

    fn compare_and_swap_application_export_operation(
        &mut self,
        expected: Option<&StoredApplicationExportOperationV1>,
        replacement: &StoredApplicationExportOperationV1,
    ) -> Result<ApplicationExportOperationWriteResultV1, StorageError> {
        if expected.is_some_and(|expected| {
            expected.operation_id() != replacement.operation_id()
                || expected.lineage() != replacement.lineage()
        }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_attributed_write(
            riffdb_storage_api::ChangelogAttributionV3::ApplicationExportOperation,
        )?;
        let mut table = access
            .transaction()?
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let operation_id = replacement.operation_id();
        let key = operation_id.as_bytes();
        let current = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_row(key, value.value()))
            .transpose()?;
        if current.as_ref() == Some(replacement) {
            drop(table);
            access.abort()?;
            return Ok(ApplicationExportOperationWriteResultV1::Unchanged);
        }
        if current.as_ref() != expected {
            drop(table);
            access.abort()?;
            return Ok(ApplicationExportOperationWriteResultV1::CompareMismatch);
        }
        let encoded = encode_application_export_operation_v1(replacement)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ApplicationExportOperation)?;
        Ok(ApplicationExportOperationWriteResultV1::Applied)
    }

    fn list_application_export_operations(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperationV1>, StorageError> {
        if maximum == 0 || maximum > MAX_ACTIVE_APPLICATION_EXPORTS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let mut operations = Vec::new();
        for row in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            if operations.len() == maximum {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            operations.push(decode_row(key.value(), value.value())?);
        }
        Ok(operations)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::PathBuf;

    use riffdb_storage_api::{
        ApplicationExportEventRecordV1, ApplicationExportOperationRepository,
        ApplicationExportOperationWriteResultV1, ApplicationExportSnapshotPort,
        ApplicationExportSourceRecordV1, AuditPrincipalV1, CatalogActivationIntentV1,
        CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
        DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyIdentity,
        IdempotencyKeyDigest, StorageErrorKind, StorageScanLimit, StoredAdmittedProvenanceClaimsV1,
        StoredApplicationExportOperationV1, StoredCommandCapsuleV1, StoredCommandCapsuleV2,
        StoredCommitRecordV1, StoredContractBundleV1, StoredDurableEventV1, StoredOutcomeV1,
        StoredProvenanceRecordV1, StoredReadDependenciesV1, StoredServiceAuditRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AdmittedActorContext, AggregateTypeId,
        ApplicationExportClassV1, ApplicationExportOperationId, CanonicalInputHash,
        CanonicalRecord, CapabilityId, CommandId, CommitSequence, ContractBundleHash,
        ContractLineage, ContractVersion, DatabaseId, DigestKeyId, Environment, EventId,
        EventTypeId, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId,
        RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
        ServiceIngressKindV1, ServiceOperationV1, TenantId, TenantScope, Timestamp,
        hash_partition_key,
    };

    use super::ExportSourceContinuationV1;
    use crate::journal::JournalTable;
    use crate::keys::{encode_application_sequence_key, encode_provenance_key};
    use crate::store::{RedbDormantPorts, RedbStore};

    struct TestPath(
        PathBuf,
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new() -> Self {
            let scope = crate::test_path::ScopedDirectory::new("application-export");
            Self(scope.join("db.redb"), scope)
        }
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn record(seed: u8, state: &[u8]) -> StoredApplicationExportOperationV1 {
        StoredApplicationExportOperationV1::new(
            ApplicationExportOperationId::from_bytes(uuid(seed)).expect("operation"),
            ContractLineage::new("TicketDesk").expect("lineage"),
            state.to_vec(),
        )
        .expect("record")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid(seed)).expect("request")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid(seed)).expect("capability")
    }

    fn activate_bundle(
        ports: &mut crate::store::RedbOperationalPorts,
        version: u64,
        seed: u8,
    ) -> StoredContractBundleV1 {
        let bundle = StoredContractBundleV1::new(
            ContractLineage::new("TicketDesk").expect("lineage"),
            ContractVersion::new(version).expect("version"),
            ContractBundleHash::from_bytes([seed; 32]),
            vec![seed, 1, 2, 3],
        )
        .expect("bundle");
        let result = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                (version > 1).then(|| ContractVersion::new(version - 1).expect("prior version")),
                bundle.clone(),
                request_id(seed),
                AuditPrincipalV1::new(
                    ActorId::new("operator").expect("actor"),
                    ActorKind::Human,
                    capability_id(seed),
                    NonZeroU64::MIN,
                ),
                Timestamp::new(i64::from(seed), 0).expect("timestamp"),
                None,
            ))
            .expect("activate");
        assert!(matches!(result, CatalogActivationResult::Activated { .. }));
        bundle
    }

    // req: REP-004, PRJ-004, PRJ-005, PRJ-009
    #[test]
    fn entity_only_snapshot_pages_preserve_exact_root_bounds_and_type() {
        use crate::hooks::RedbTestOperation;
        use riffdb_storage_api::{
            ApplicationExportSourceRecordV1, AuthoritativeEntitySnapshotReader,
            OwnedSnapshotReader, StorageScanLimit,
        };
        use riffdb_types::EntityKeyBuilder;
        use riffdb_types::{CanonicalRecord, CanonicalValue, EntityTypeId, EntityVersion, FieldId};
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).unwrap();
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).unwrap())
            .unwrap();
        let mut ports = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .unwrap();
        let bundle = activate_bundle(&mut ports, 1, 5);
        let entity = EntityTypeId::new(1).unwrap();
        let other = EntityTypeId::new(2).unwrap();
        let record = |entity, key, version| {
            let mut builder = EntityKeyBuilder::new(entity);
            builder.push_u64(key).unwrap();
            riffdb_storage_api::StoredEntityRecordV1::new(
                riffdb_storage_api::EntityTarget::new(entity, builder.finish().unwrap()).unwrap(),
                EntityVersion::new(version).unwrap(),
                bundle.contract_version(),
                riffdb_storage_api::DurableKeySchemaBindingV1::new(
                    bundle.lineage().clone(),
                    bundle.contract_version(),
                    bundle.bundle_hash(),
                ),
                CanonicalRecord::new(vec![(
                    FieldId::new(1).unwrap(),
                    CanonicalValue::U64(version),
                )])
                .unwrap(),
            )
            .unwrap()
        };
        let write = |records: Vec<riffdb_storage_api::StoredEntityRecordV1>| {
            let access = ports.begin_write().unwrap();
            let mut table = access
                .transaction()
                .unwrap()
                .open_table(crate::layout::ENTITIES)
                .unwrap();
            for record in records {
                let bytes = crate::codec::encode_entity_record_v1(&record).unwrap();
                table
                    .insert(record.target().key().as_bytes(), bytes.as_bytes())
                    .unwrap();
            }
            drop(table);
            access
                .commit_for(RedbTestOperation::Initialization)
                .unwrap();
        };
        write(vec![
            record(entity, 1, 1),
            record(entity, 2, 1),
            record(other, 1, 1),
        ]);
        let old = ports.open_owned_snapshot().unwrap();
        let export = ports
            .capture_application_export_snapshot(bundle.lineage())
            .unwrap();
        write(vec![record(entity, 1, 2), record(entity, 3, 1)]);
        let limit = StorageScanLimit::new(1).unwrap();
        let tickets = ports.mutation_gate_tickets();
        let first = old.read_entity_type_page(entity, None, limit).unwrap();
        assert!(
            first
                == export
                    .read_application_export_entity_page(entity, None, limit)
                    .unwrap()
        );
        assert!(
            first.records()
                == [ApplicationExportSourceRecordV1::Entity(Box::new(record(
                    entity, 1, 1
                )))]
        );
        assert!(!first.exact_end());
        let second = old
            .read_entity_type_page(entity, first.continuation(), limit)
            .unwrap();
        assert!(
            second.records()
                == [ApplicationExportSourceRecordV1::Entity(Box::new(record(
                    entity, 2, 1
                )))]
        );
        assert!(second.exact_end());
        assert!(second.continuation().is_none());
        assert_eq!(ports.mutation_gate_tickets(), tickets);
        let current = ports.open_owned_snapshot().unwrap();
        let current = current
            .read_entity_type_page(entity, None, StorageScanLimit::new(500).unwrap())
            .unwrap();
        assert_eq!(current.records().len(), 3);
        assert!(
            current.records()[0]
                == ApplicationExportSourceRecordV1::Entity(Box::new(record(entity, 1, 2)))
        );
        assert!(current.exact_end());
        let wrong_type = record(other, 1, 1);
        for after in [
            b"".as_slice(),
            wrong_type.target().key().as_bytes(),
            &[0; 4097],
        ] {
            assert!(
                old.read_entity_type_page(entity, Some(after), limit)
                    .is_err()
            );
        }
        // A page stops at the existing byte ceiling even when its row limit is 500.
        let large_entity = EntityTypeId::new(3).unwrap();
        let payload = "x".repeat(900_000);
        let large = (1..=6)
            .map(|key| {
                let initial = record(large_entity, key, 1);
                riffdb_storage_api::StoredEntityRecordV1::new(
                    initial.target().clone(),
                    EntityVersion::first(),
                    bundle.contract_version(),
                    initial.schema_binding().clone(),
                    CanonicalRecord::new(vec![(
                        FieldId::new(1).unwrap(),
                        CanonicalValue::string(&payload).unwrap(),
                    )])
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();
        write(large);
        let large_pin = ports.open_owned_snapshot().unwrap();
        let limit = StorageScanLimit::new(500).unwrap();
        let page = large_pin
            .read_entity_type_page(large_entity, None, limit)
            .unwrap();
        assert_eq!(page.records().len(), 4);
        assert!(
            page.encoded_bytes() <= riffdb_storage_api::MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES
        );
        assert!(!page.exact_end());
        let tail = large_pin
            .read_entity_type_page(large_entity, page.continuation(), limit)
            .unwrap();
        assert_eq!(tail.records().len(), 2);
        assert!(tail.exact_end());
    }

    fn provenance_id(seed: u8) -> ProvenanceId {
        ProvenanceId::from_bytes(uuid(seed)).expect("provenance")
    }

    fn command_capsule(
        database_id: DatabaseId,
        lineage: &ContractLineage,
        sequence: CommitSequence,
        provenance_id: ProvenanceId,
        seed: u8,
        event_count: u32,
    ) -> StoredCommandCapsuleV2 {
        let request = request_id(seed);
        let plan = ExecutablePlanRef::new(
            lineage.clone(),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([5; 32]),
            CommandId::first(),
            PlanHash::from_bytes([0x20_u8.wrapping_add(seed); 32]),
        );
        let actor = AdmittedActorContext::new(
            ActorId::new("export-principal").expect("principal"),
            ActorKind::Human,
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id,
            Environment::new("test").expect("environment"),
            actor.tenant_scope().clone(),
            actor.principal_id().clone(),
            lineage.clone(),
            CommandId::first(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(u32::from(seed).max(1)).expect("digest key"),
                [seed; 32],
            ),
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_str("export").expect("partition component");
        let partition = partition.finish().expect("partition");
        let partition_hash = hash_partition_key(partition.as_bytes());
        let logical_time = LogicalTime::new(Timestamp::new(i64::from(seed), 0).expect("time"));
        let input_hash = CanonicalInputHash::from_bytes([0x40_u8.wrapping_add(seed); 32]);
        let events = (0..event_count)
            .map(|ordinal| {
                let payload = CanonicalRecord::new(Vec::new()).expect("payload");
                let event_id = EventId::new(sequence, ordinal);
                let event_hash = riffdb_storage_api::derive_event_hash_v1(
                    event_id,
                    EventTypeId::first(),
                    &payload,
                )
                .expect("event hash");
                StoredDurableEventV1::new(event_id, EventTypeId::first(), payload, event_hash)
                    .expect("event")
            })
            .collect::<Vec<_>>();
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let declared = DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome payload"),
        )
        .expect("declared outcome");
        let claims = StoredAdmittedProvenanceClaimsV1::default();
        let outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request,
            plan.clone(),
            input_hash,
            actor.clone(),
            logical_time,
            partition.clone(),
            partition_hash,
            Vec::new(),
            declared.clone(),
            claims.clone(),
            provenance_id,
            DurabilityMode::Memory,
        )
        .expect("stored outcome");
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity,
            request,
            plan.clone(),
            input_hash,
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            declared.outcome_id(),
            Vec::new(),
            event_ids.clone(),
            claims,
        )
        .expect("provenance");
        let commit = StoredCommitRecordV1::new(
            sequence,
            request,
            plan,
            input_hash,
            outcome.actor().clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            StoredReadDependenciesV1::new(Vec::new()).expect("dependencies"),
            Vec::new(),
            events.clone(),
            declared,
            provenance_id,
            event_ids,
            DurabilityMode::Memory,
        )
        .expect("commit");
        let principal = AuditPrincipalV1::new(
            outcome.actor().principal_id().clone(),
            ActorKind::Human,
            capability_id(seed),
            NonZeroU64::MIN,
        );
        let started_sequence =
            AdministrationSequence::new(u64::from(seed) * 2 - 1).expect("started sequence");
        let started = StoredServiceAuditRecordV1::from_stored_parts(
            started_sequence,
            request,
            Timestamp::new(i64::from(seed), 0).expect("time"),
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Started,
            Some(principal.clone()),
            ServiceIngressKindV1::InProcessTestComparison,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("started audit");
        let terminal = StoredServiceAuditRecordV1::from_stored_parts(
            started_sequence.checked_next().expect("terminal sequence"),
            request,
            Timestamp::new(i64::from(seed), 0).expect("time"),
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Succeeded,
            Some(principal),
            ServiceIngressKindV1::InProcessTestComparison,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::Command {
                commit_sequence: sequence,
                provenance_id,
            },
        )
        .expect("terminal audit");
        let base = StoredCommandCapsuleV1::new(outcome, provenance, commit, started, terminal)
            .expect("capsule");
        StoredCommandCapsuleV2::new(base, events, Vec::new()).expect("current capsule")
    }

    fn install_capsules(
        ports: &crate::store::RedbOperationalPorts,
        database_id: DatabaseId,
        capsules: Vec<StoredCommandCapsuleV2>,
    ) {
        let first = capsules.first().expect("capsules").commit_sequence();
        let manifest =
            crate::application::build_command_segment_manifest(&capsules, first).expect("manifest");
        let segment = riffdb_storage_api::StoredCommandSegmentV1::new(
            database_id,
            riffdb_storage_api::HISTORY_INCARNATION_INITIAL,
            None,
            capsules.clone(),
            manifest,
            riffdb_storage_api::CommandSegmentDigestV1::from_bytes([0; 32]),
        )
        .expect("segment");
        let (_, encoded) = riffdb_storage_api::seal_and_encode_command_segment_v1(segment)
            .expect("encoded segment");
        let access = ports.begin_write().expect("write");
        access
            .put_command_value(
                JournalTable::Commits,
                encode_application_sequence_key(first).to_vec(),
                encoded.into_bytes(),
            )
            .expect("command row");
        let next = riffdb_storage_api::ApplicationSequenceAllocator::Next(
            capsules
                .last()
                .expect("capsules")
                .commit_sequence()
                .checked_next()
                .expect("next allocator"),
        );
        access
            .put_command_value(
                JournalTable::Meta,
                crate::layout::META_APPLICATION_SEQUENCE.as_bytes().to_vec(),
                riffdb_storage_api::encode_application_sequence_allocator_v1(next)
                    .expect("encoded allocator")
                    .into_bytes(),
            )
            .expect("application allocator");
        access.commit().expect("commit evidence");
    }

    // req: AFC-006, EXP-002, EXP-003, EXP-004, EXP-007, EXP-010
    #[test]
    fn application_export_sparse_evidence_pages_charge_bounded_progress() {
        let maximum_key = vec![
            0x61;
            riffdb_storage_api::MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
                - super::EXPORT_CONTINUATION_FIXED_BYTES
        ];
        let encoded = ExportSourceContinuationV1::table(maximum_key)
            .encode(ApplicationExportClassV1::Entity)
            .expect("maximum continuation");
        assert_eq!(
            encoded.len(),
            riffdb_storage_api::MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
        );
        let mut noncanonical = encoded.to_vec();
        noncanonical.push(0);
        assert!(
            ExportSourceContinuationV1::decode(ApplicationExportClassV1::Entity, &noncanonical,)
                .is_err()
        );
        assert_eq!(
            crate::command_authority::MAX_APPLICATION_EXPORT_COMMAND_AUTHORITY_BYTES,
            16 * 1024 * 1024
        );
        assert_eq!(riffdb_storage_api::MAX_STAGED_COMMANDS, 256);
        assert_eq!(riffdb_storage_api::MAX_EVENT_INTENTS, 4_096);
    }

    // req: OUT-001, OUT-002, TXN-042
    #[test]
    fn operation_compare_and_swap_is_durable_ordered_and_retry_safe() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let dormant = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate");
        let first = record(3, b"state-one\n");
        let next = record(3, b"state-two\n");
        let earlier = record(2, b"state-earlier\n");

        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &first)
                .expect("insert"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &first)
                .expect("retry"),
            ApplicationExportOperationWriteResultV1::Unchanged
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &next)
                .expect("mismatch"),
            ApplicationExportOperationWriteResultV1::CompareMismatch
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(Some(&first), &next)
                .expect("advance"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &earlier)
                .expect("earlier"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports.list_application_export_operations(2).expect("list"),
            vec![earlier.clone(), next.clone()]
        );
        drop(ports);

        let mut reopened = RedbStore::open(&path.0).expect("reopen");
        reopened
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("observe initialized database");
        let reopened = RedbDormantPorts {
            pending_v3_activation: None,
            shared: reopened.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("reactivate");
        assert_eq!(
            reopened
                .read_application_export_operation(next.operation_id())
                .expect("read after reopen"),
            Some(next)
        );
        assert_eq!(
            reopened
                .list_application_export_operations(2)
                .expect("list after reopen"),
            vec![earlier, record(3, b"state-two\n")]
        );
    }

    #[test]
    fn captured_snapshot_keeps_one_exact_publication_root_and_bounded_empty_pages() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let dormant = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports");
        let first = activate_bundle(&mut ports, 1, 5);
        let snapshot = ports
            .capture_application_export_snapshot(first.lineage())
            .expect("capture first");
        assert_eq!(
            snapshot.binding().contract_version(),
            first.contract_version()
        );
        assert_eq!(snapshot.contract_bundle_bytes(), first.canonical_bytes());

        let second = activate_bundle(&mut ports, 2, 6);
        let current = ports
            .capture_application_export_snapshot(second.lineage())
            .expect("capture second");
        assert_eq!(
            current.binding().contract_version(),
            second.contract_version()
        );
        assert_eq!(
            snapshot.binding().contract_version(),
            first.contract_version()
        );
        assert_eq!(snapshot.contract_bundle_bytes(), first.canonical_bytes());

        for class in [
            ApplicationExportClassV1::Entity,
            ApplicationExportClassV1::Event,
            ApplicationExportClassV1::Provenance,
        ] {
            let page = snapshot
                .read_application_export_source_page(
                    class,
                    None,
                    riffdb_storage_api::StorageScanLimit::new(1).expect("limit"),
                )
                .expect("empty page");
            assert!(page.exact_end());
            assert!(page.records().is_empty());
            assert!(page.continuation().is_none());
        }
        let audit = snapshot
            .read_application_export_source_page(
                ApplicationExportClassV1::PublicAudit,
                None,
                riffdb_storage_api::StorageScanLimit::new(10).expect("limit"),
            )
            .expect("audit page");
        assert!(audit.exact_end());
        assert_eq!(audit.records().len(), 1);
    }

    // req: AFC-006, EXP-002, EXP-003, EXP-004, EXP-007, EXP-010
    #[test]
    fn application_export_reads_segment_owned_events_and_provenance_from_one_snapshot() {
        let path = TestPath::new();
        let database_id = DatabaseId::from_bytes(uuid(1)).expect("database");
        let mut store = RedbStore::open(&path.0).expect("open");
        store.initialize_database(database_id).expect("initialize");
        let dormant = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports");
        let bundle = activate_bundle(&mut ports, 1, 5);
        let first = command_capsule(
            database_id,
            bundle.lineage(),
            CommitSequence::first(),
            provenance_id(31),
            11,
            2,
        );
        let second_sequence = CommitSequence::new(2).expect("second sequence");
        let second = command_capsule(
            database_id,
            bundle.lineage(),
            second_sequence,
            provenance_id(32),
            12,
            1,
        );
        install_capsules(&ports, database_id, vec![first.clone(), second.clone()]);

        let physical = ports.begin_composite_read().expect("captured read");
        assert!(
            physical
                .read_range_to(JournalTable::Events, &[], None, 1)
                .expect("event table")
                .is_empty()
        );
        assert!(
            physical
                .read_range_to(JournalTable::Provenance, &[], None, 1)
                .expect("provenance table")
                .is_empty()
        );
        drop(physical);

        let snapshot = ports
            .capture_application_export_snapshot(bundle.lineage())
            .expect("snapshot");
        let one = StorageScanLimit::new(1).expect("one");
        let expected_events = [
            first.events()[0].event_id(),
            first.events()[1].event_id(),
            second.events()[0].event_id(),
        ];
        let mut continuation = None;
        let mut observed_events = Vec::new();
        for page_index in 0..expected_events.len() {
            let page = snapshot
                .read_application_export_source_page(
                    ApplicationExportClassV1::Event,
                    continuation.as_deref(),
                    one,
                )
                .expect("event page");
            assert_eq!(page.exact_end(), page_index + 1 == expected_events.len());
            assert!(page.inspected_work() >= 3);
            observed_events.push(match &page.records()[0] {
                ApplicationExportSourceRecordV1::Event(event) => match event.as_ref() {
                    ApplicationExportEventRecordV1::V1(event) => event.event_id(),
                    ApplicationExportEventRecordV1::V2(_) => {
                        panic!("unanchored segment event changed revision")
                    }
                },
                _ => panic!("wrong export class"),
            });
            continuation = page.continuation().map(<[u8]>::to_vec);
        }
        assert_eq!(observed_events, expected_events);
        assert!(continuation.is_none());

        let impossible_event_cursor = ExportSourceContinuationV1::command(
            ApplicationExportClassV1::Event,
            encode_application_sequence_key(CommitSequence::first()).to_vec(),
            0,
            first.events().len(),
        )
        .expect("shape cursor")
        .encode(ApplicationExportClassV1::Event)
        .expect("encode cursor");
        let error = match snapshot.read_application_export_source_page(
            ApplicationExportClassV1::Event,
            Some(&impossible_event_cursor),
            one,
        ) {
            Ok(_) => panic!("noncanonical end-of-command ordinal must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);

        let provenance_page_one = snapshot
            .read_application_export_source_page(ApplicationExportClassV1::Provenance, None, one)
            .expect("first provenance page");
        assert!(!provenance_page_one.exact_end());
        let provenance_page_two = snapshot
            .read_application_export_source_page(
                ApplicationExportClassV1::Provenance,
                provenance_page_one.continuation(),
                one,
            )
            .expect("second provenance page");
        assert!(provenance_page_two.exact_end());
        let observed = [provenance_page_one, provenance_page_two]
            .into_iter()
            .map(|page| match &page.records()[0] {
                ApplicationExportSourceRecordV1::Provenance(record) => record.provenance_id(),
                _ => panic!("wrong export class"),
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, [provenance_id(31), provenance_id(32)]);

        let third = command_capsule(
            database_id,
            bundle.lineage(),
            CommitSequence::new(3).expect("third sequence"),
            provenance_id(33),
            13,
            1,
        );
        install_capsules(&ports, database_id, vec![third]);
        let old_snapshot = snapshot
            .read_application_export_source_page(
                ApplicationExportClassV1::Event,
                None,
                StorageScanLimit::new(10).expect("limit"),
            )
            .expect("old snapshot remains readable");
        assert_eq!(old_snapshot.records().len(), 3);
        assert!(old_snapshot.exact_end());
    }

    // req: AFC-006, EXP-002, EXP-003, EXP-004, EXP-007, EXP-010
    #[test]
    fn application_export_canonically_deduplicates_mixed_legacy_and_segment_evidence() {
        let path = TestPath::new();
        let database_id = DatabaseId::from_bytes(uuid(2)).expect("database");
        let mut store = RedbStore::open(&path.0).expect("open");
        store.initialize_database(database_id).expect("initialize");
        let dormant = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports");
        let bundle = activate_bundle(&mut ports, 1, 5);
        let provenance_id = provenance_id(41);
        let capsule = command_capsule(
            database_id,
            bundle.lineage(),
            CommitSequence::first(),
            provenance_id,
            21,
            1,
        );
        install_capsules(&ports, database_id, vec![capsule.clone()]);
        let exact_legacy = capsule.base().provenance().clone();
        let access = ports.begin_write().expect("legacy write");
        access
            .put_command_value(
                JournalTable::Provenance,
                encode_provenance_key(provenance_id).to_vec(),
                riffdb_storage_api::encode_provenance_record_v1(&exact_legacy)
                    .expect("legacy provenance")
                    .into_bytes(),
            )
            .expect("legacy row");
        access.commit().expect("commit legacy row");
        let snapshot = ports
            .capture_application_export_snapshot(bundle.lineage())
            .expect("snapshot");
        let page = snapshot
            .read_application_export_source_page(
                ApplicationExportClassV1::Provenance,
                None,
                StorageScanLimit::new(10).expect("limit"),
            )
            .expect("mixed provenance page");
        assert!(page.exact_end());
        assert_eq!(page.records().len(), 1, "exact duplicate emitted twice");
        assert!(page.inspected_work() > page.records().len());
        match &page.records()[0] {
            ApplicationExportSourceRecordV1::Provenance(record) => {
                assert_eq!(record.as_ref(), &exact_legacy);
            }
            _ => panic!("wrong export class"),
        }

        let unequal = command_capsule(
            database_id,
            bundle.lineage(),
            CommitSequence::new(2).expect("second sequence"),
            provenance_id,
            22,
            1,
        );
        let access = ports.begin_write().expect("mismatch write");
        access
            .put_command_value(
                JournalTable::Provenance,
                encode_provenance_key(provenance_id).to_vec(),
                riffdb_storage_api::encode_provenance_record_v1(unequal.base().provenance())
                    .expect("unequal provenance")
                    .into_bytes(),
            )
            .expect("replace legacy row");
        access.commit().expect("commit mismatch");
        let corrupt_snapshot = ports
            .capture_application_export_snapshot(bundle.lineage())
            .expect("corrupt snapshot capture remains structural");
        let error = match corrupt_snapshot.read_application_export_source_page(
            ApplicationExportClassV1::Provenance,
            None,
            StorageScanLimit::new(10).expect("limit"),
        ) {
            Ok(_) => panic!("unequal duplicate must refuse the complete page"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);

        let event_cursor = ExportSourceContinuationV1::command(
            ApplicationExportClassV1::Event,
            encode_application_sequence_key(CommitSequence::first()).to_vec(),
            0,
            0,
        )
        .expect("event cursor")
        .encode(ApplicationExportClassV1::Event)
        .expect("encode cursor");
        let error = match corrupt_snapshot.read_application_export_source_page(
            ApplicationExportClassV1::Provenance,
            Some(&event_cursor),
            StorageScanLimit::new(1).expect("limit"),
        ) {
            Ok(_) => panic!("cross-class cursor must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    }
}
