//! Durable redb implementation of [`SagaCoordinationRepository`] (`ADR-0334`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use redb::ReadableTable;
use riffdb_storage_api::ChangelogAttributionV3;
use riffdb_storage_api::{
    SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1, SagaCompareExpectedV1,
    SagaCoordinationRepository, SagaId, SagaLeaseStateV1, SagaLifecycleStatusV1, StorageError,
    StorageErrorKind, StoredApplicationInstallationCampaignV1, StoredSagaInstanceV1,
    is_encoded_saga_instance_v1,
};
use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationInstallationPlanHash, ContractBundleHash,
    ContractLineage, DatabaseId, HashDomain, IdempotencyKey, TenantScope, Timestamp, hash,
};

use crate::codec::{
    decode_application_installation_campaign_v1, encode_application_installation_campaign_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::layout::APPLICATION_INSTALLATION_CAMPAIGNS;
use crate::store::RedbOperationalPorts;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SagaAdmissionIndexKey {
    database_id: DatabaseId,
    tenant_scope: TenantScope,
    contract_lineage: ContractLineage,
    saga_name: String,
    caller_idempotency_key: IdempotencyKey,
}

impl SagaAdmissionIndexKey {
    fn from_instance(instance: &StoredSagaInstanceV1) -> Self {
        Self {
            database_id: instance.database_id(),
            tenant_scope: instance.tenant_scope().clone(),
            contract_lineage: instance.contract_lineage().clone(),
            saga_name: instance.saga_name().to_owned(),
            caller_idempotency_key: instance.caller_idempotency_key().clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SagaScheduleEntry {
    partition_hash: u64,
    lifecycle_status: SagaLifecycleStatusV1,
    next_wakeup_at: Timestamp,
    saga_id: SagaId,
}

impl SagaScheduleEntry {
    const fn from_instance(instance: &StoredSagaInstanceV1) -> Self {
        Self {
            partition_hash: instance.partition_hash(),
            lifecycle_status: instance.lifecycle_status(),
            next_wakeup_at: instance.next_wakeup_at(),
            saga_id: instance.saga_id(),
        }
    }
}

pub(crate) struct SagaIndexState {
    by_admission_key: BTreeMap<SagaAdmissionIndexKey, SagaId>,
    active_schedule: BTreeSet<SagaScheduleEntry>,
    active_by_id: BTreeMap<SagaId, (SagaScheduleEntry, Option<Timestamp>)>,
}

impl SagaIndexState {
    fn empty() -> Self {
        Self {
            by_admission_key: BTreeMap::new(),
            active_schedule: BTreeSet::new(),
            active_by_id: BTreeMap::new(),
        }
    }

    fn upsert(&mut self, instance: &StoredSagaInstanceV1) {
        let saga_id = instance.saga_id();
        self.by_admission_key
            .insert(SagaAdmissionIndexKey::from_instance(instance), saga_id);
        if let Some((prior_entry, _)) = self.active_by_id.remove(&saga_id) {
            self.active_schedule.remove(&prior_entry);
        }
        if !instance.lifecycle_status().is_terminal() {
            let entry = SagaScheduleEntry::from_instance(instance);
            let lease_expires_at = instance.lease().map(SagaLeaseStateV1::expires_at);
            self.active_schedule.insert(entry);
            self.active_by_id.insert(saga_id, (entry, lease_expires_at));
        }
    }

    fn has_due_candidate(&self, now: Timestamp) -> bool {
        self.active_by_id.values().any(|(entry, lease_expires_at)| {
            entry.lifecycle_status.is_claimable()
                && entry.next_wakeup_at <= now
                && lease_expires_at.is_none_or(|expires| expires <= now)
        })
    }

    fn has_claimable_saga(&self) -> bool {
        self.active_by_id
            .values()
            .any(|(entry, _)| entry.lifecycle_status.is_claimable())
    }

    fn next_claimable_wakeup_at(&self) -> Option<Timestamp> {
        self.active_by_id
            .values()
            .filter(|(entry, _)| entry.lifecycle_status.is_claimable())
            .map(|(entry, lease_expires_at)| match lease_expires_at {
                Some(expires) if *expires > entry.next_wakeup_at => *expires,
                _ => entry.next_wakeup_at,
            })
            .min()
    }

    fn select_due_candidate_ids(
        &self,
        now: Timestamp,
        max_claims: usize,
        cursor_partition_hash: u64,
    ) -> Vec<SagaId> {
        if max_claims == 0 || self.active_schedule.is_empty() {
            return Vec::new();
        }
        let mut selected = Vec::with_capacity(max_claims);
        let is_due = |entry: &SagaScheduleEntry| -> bool {
            if !entry.lifecycle_status.is_claimable() || entry.next_wakeup_at > now {
                return false;
            }
            self.active_by_id
                .get(&entry.saga_id)
                .is_some_and(|(_, lease_expires_at)| {
                    lease_expires_at.is_none_or(|expires| expires <= now)
                })
        };
        for entry in self
            .active_schedule
            .iter()
            .filter(|entry| entry.partition_hash >= cursor_partition_hash)
            .chain(
                self.active_schedule
                    .iter()
                    .filter(|entry| entry.partition_hash < cursor_partition_hash),
            )
        {
            if is_due(entry) {
                selected.push(entry.saga_id);
                if selected.len() == max_claims {
                    break;
                }
            }
        }
        selected
    }
}

fn encode_saga_envelope(
    instance: &StoredSagaInstanceV1,
) -> Result<StoredApplicationInstallationCampaignV1, StorageError> {
    let campaign_id =
        ApplicationInstallationCampaignId::from_bytes(instance.saga_id().into_bytes())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let plan_hash =
        ApplicationInstallationPlanHash::from_bytes(instance.saga_plan_hash().into_bytes());
    StoredApplicationInstallationCampaignV1::new(
        campaign_id,
        instance.contract_lineage().clone(),
        plan_hash,
        instance.encode_canonical_bytes(),
    )
    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))
}

pub(crate) fn decode_saga_row_if_present(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StoredSagaInstanceV1>, StorageError> {
    let campaign = decode_application_installation_campaign_v1(value)?
        .into_parts()
        .0;
    if key != campaign.campaign_id().as_bytes() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    if !is_encoded_saga_instance_v1(campaign.canonical_state()) {
        return Ok(None);
    }
    let saga = StoredSagaInstanceV1::decode_canonical_bytes(campaign.canonical_state())
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    if saga.saga_id().as_bytes() != key
        || saga.contract_lineage() != campaign.contract_lineage()
        || saga.saga_plan_hash().as_bytes() != campaign.plan_hash().as_bytes()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(Some(saga))
}

pub(crate) fn has_non_terminal_saga_for_contract_bundle<T>(
    table: &T,
    bundle_hash: ContractBundleHash,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    if let Some(registry) = SAGA_COORDINATORS.get() {
        let guard = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (weak, coord) in guard.iter() {
            if weak.strong_count() == 0 {
                continue;
            }
            if let Ok(state_guard) = coord.state.lock()
                && let Some(ref mem) = state_guard.cached_state
                && mem.sagas_by_id.values().any(|saga| {
                    saga.contract_bundle_hash() == bundle_hash
                        && !saga.lifecycle_status().is_terminal()
                })
            {
                return Ok(true);
            }
        }
    }
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if let Some(saga) = decode_saga_row_if_present(key.value(), value.value())?
            && saga.contract_bundle_hash() == bundle_hash
            && !saga.lifecycle_status().is_terminal()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

const SAGA_WAL_MAGIC: [u8; 8] = *b"RDBSAGA1";
const SAGA_WAL_HEADER_BYTES: usize = 8 + 4 + 32;
const MAX_SAGA_WAL_FRAME_PAYLOAD_BYTES: usize = 1024 * 1024;

fn saga_wal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".sagalog");
    PathBuf::from(path)
}

struct SagaMemoryState {
    sagas_by_id: BTreeMap<SagaId, StoredSagaInstanceV1>,
    index: SagaIndexState,
}

impl SagaMemoryState {
    fn empty() -> Self {
        Self {
            sagas_by_id: BTreeMap::new(),
            index: SagaIndexState::empty(),
        }
    }

    fn upsert(&mut self, instance: StoredSagaInstanceV1) {
        self.index.upsert(&instance);
        self.sagas_by_id.insert(instance.saga_id(), instance);
    }
}

fn build_saga_memory_state_from_table_and_wal<T>(
    table: &T,
    database_path: &Path,
) -> Result<SagaMemoryState, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut state = SagaMemoryState::empty();
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if let Some(saga) = decode_saga_row_if_present(key.value(), value.value())? {
            state.upsert(saga);
        }
    }
    let wal_path = saga_wal_path(database_path);
    match std::fs::read(&wal_path) {
        Ok(bytes) => {
            let mut offset = 0usize;
            while offset + SAGA_WAL_HEADER_BYTES <= bytes.len() {
                if bytes[offset..offset + 8] != SAGA_WAL_MAGIC {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let len = u32::from_le_bytes([
                    bytes[offset + 8],
                    bytes[offset + 9],
                    bytes[offset + 10],
                    bytes[offset + 11],
                ]) as usize;
                if len > MAX_SAGA_WAL_FRAME_PAYLOAD_BYTES {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let frame_end = offset + SAGA_WAL_HEADER_BYTES + len;
                if frame_end > bytes.len() {
                    // Torn trailing write at EOF after an unclean crash.
                    break;
                }
                let expected_hash = &bytes[offset + 12..offset + SAGA_WAL_HEADER_BYTES];
                let payload = &bytes[offset + SAGA_WAL_HEADER_BYTES..frame_end];
                let actual_hash = hash(HashDomain::CanonicalValue, payload);
                if actual_hash.as_bytes() != expected_hash {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let campaign = decode_application_installation_campaign_v1(payload)?
                    .into_parts()
                    .0;
                let Some(saga) =
                    decode_saga_row_if_present(campaign.campaign_id().as_bytes(), payload)?
                else {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                };
                state.upsert(saga);
                offset = frame_end;
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
    }
    Ok(state)
}

enum SagaWriteRequest {
    Admit(Box<StoredSagaInstanceV1>),
    Cas {
        expected: SagaCompareExpectedV1,
        next: Box<StoredSagaInstanceV1>,
    },
    ClaimDue {
        owner_id: [u8; 16],
        now: Timestamp,
        lease_expires_at: Timestamp,
        max_claims: usize,
        cursor_partition_hash: u64,
    },
}

enum SagaWriteResponse {
    Admit(Result<SagaAdmissionStorageOutcomeV1, StorageError>),
    Cas(Result<SagaCasOutcomeV1, StorageError>),
    ClaimDue(Result<Vec<StoredSagaInstanceV1>, StorageError>),
}

struct SagaBatchItem {
    request: SagaWriteRequest,
    response: Mutex<Option<SagaWriteResponse>>,
}

impl SagaBatchItem {
    fn new(request: SagaWriteRequest) -> Self {
        Self {
            request,
            response: Mutex::new(None),
        }
    }

    fn set_response(&self, resp: SagaWriteResponse) {
        if let Ok(mut guard) = self.response.lock() {
            *guard = Some(resp);
        }
    }

    fn is_ready(&self) -> bool {
        self.response.lock().is_ok_and(|guard| guard.is_some())
    }

    fn take_response(&self) -> SagaWriteResponse {
        self.response
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
            .unwrap_or_else(|| {
                SagaWriteResponse::Admit(Err(storage_error(StorageErrorKind::Unavailable)))
            })
    }
}

struct SagaCoordinatorState {
    writer_active: bool,
    pending: Vec<Arc<SagaBatchItem>>,
    cached_state: Option<SagaMemoryState>,
}

struct SagaStoreCoordinator {
    state: Mutex<SagaCoordinatorState>,
    done_cv: Condvar,
}

impl SagaStoreCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(SagaCoordinatorState {
                writer_active: false,
                pending: Vec::new(),
                cached_state: None,
            }),
            done_cv: Condvar::new(),
        }
    }
}

type SagaCoordinatorEntry = (Weak<crate::store::SharedRedb>, Arc<SagaStoreCoordinator>);

static SAGA_COORDINATORS: OnceLock<Mutex<Vec<SagaCoordinatorEntry>>> = OnceLock::new();

pub(crate) fn invalidate_all_saga_coordinators() {
    if let Some(registry) = SAGA_COORDINATORS.get() {
        let guard = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (weak, coord) in guard.iter() {
            if weak.strong_count() == 0 {
                continue;
            }
            if let Ok(mut state_guard) = coord.state.lock() {
                state_guard.cached_state = None;
            }
        }
    }
}

impl RedbOperationalPorts {
    fn saga_coordinator(&self) -> Arc<SagaStoreCoordinator> {
        let registry = SAGA_COORDINATORS.get_or_init(|| Mutex::new(Vec::new()));
        let mut guard = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.retain(|(weak, _)| weak.strong_count() > 0);
        for (weak, coord) in guard.iter() {
            if let Some(upgraded) = weak.upgrade()
                && Arc::ptr_eq(&upgraded, &self.shared)
            {
                return Arc::clone(coord);
            }
        }
        let coord = Arc::new(SagaStoreCoordinator::new());
        guard.push((Arc::downgrade(&self.shared), Arc::clone(&coord)));
        coord
    }

    pub(crate) fn has_saga_id_bytes(&self, raw_id: &[u8; 16]) -> Result<bool, StorageError> {
        let Ok(saga_id) = SagaId::from_bytes(*raw_id) else {
            return Ok(false);
        };
        Ok(self.read_saga(saga_id)?.is_some())
    }

    fn load_saga_memory_state(&self) -> Result<SagaMemoryState, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
            .map_err(table_error)?;
        build_saga_memory_state_from_table_and_wal(&table, self.database_path())
    }

    fn submit_saga_write(&self, request: SagaWriteRequest) -> SagaWriteResponse {
        let coord = self.saga_coordinator();
        let item = Arc::new(SagaBatchItem::new(request));
        let mut guard = coord
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.pending.push(Arc::clone(&item));

        if guard.writer_active {
            while !item.is_ready() {
                guard = coord
                    .done_cv
                    .wait(guard)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            return item.take_response();
        }

        guard.writer_active = true;
        while !guard.pending.is_empty() {
            let mut batch = std::mem::take(&mut guard.pending);
            let mut cached_state = guard.cached_state.take();
            drop(guard);

            let restored_state =
                self.execute_saga_write_batch(&coord, &mut batch, &mut cached_state);

            guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if guard.cached_state.is_none() {
                guard.cached_state = restored_state;
            }
            coord.done_cv.notify_all();
        }
        guard.writer_active = false;
        coord.done_cv.notify_all();
        drop(guard);
        item.take_response()
    }

    fn execute_saga_write_batch(
        &self,
        coord: &SagaStoreCoordinator,
        batch: &mut Vec<Arc<SagaBatchItem>>,
        cached_state: &mut Option<SagaMemoryState>,
    ) -> Option<SagaMemoryState> {
        match self.execute_saga_write_batch_inner(coord, batch, cached_state) {
            Ok(mem_state) => Some(mem_state),
            Err(err) => {
                Self::fail_batch(batch, &err);
                None
            }
        }
    }

    fn execute_saga_write_batch_inner(
        &self,
        coord: &SagaStoreCoordinator,
        batch: &mut Vec<Arc<SagaBatchItem>>,
        cached_state: &mut Option<SagaMemoryState>,
    ) -> Result<SagaMemoryState, StorageError> {
        let access =
            self.begin_attributed_write(ChangelogAttributionV3::ApplicationInstallationCampaign)?;

        {
            let mut guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !guard.pending.is_empty() {
                batch.append(&mut guard.pending);
            }
        }

        let mut table = access
            .transaction()?
            .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
            .map_err(table_error)?;

        let mut mem_state = match cached_state.take() {
            Some(state) => state,
            None => build_saga_memory_state_from_table_and_wal(&table, self.database_path())?,
        };

        let mut any_mutated = false;
        let mut staged_responses = Vec::with_capacity(batch.len());

        for item in batch.iter() {
            let outcome = match &item.request {
                SagaWriteRequest::Admit(instance) => {
                    Self::apply_admit_in_tx(&mut table, &mut mem_state, instance, &mut any_mutated)?
                        .map(|v| SagaWriteResponse::Admit(Ok(v)))
                }
                SagaWriteRequest::Cas { expected, next } => Self::apply_cas_in_tx(
                    &mut table,
                    &mut mem_state,
                    *expected,
                    next,
                    &mut any_mutated,
                )?
                .map(|v| SagaWriteResponse::Cas(Ok(v))),
                SagaWriteRequest::ClaimDue {
                    owner_id,
                    now,
                    lease_expires_at,
                    max_claims,
                    cursor_partition_hash,
                } => Self::apply_claim_due_in_tx(
                    &mut table,
                    &mut mem_state,
                    *owner_id,
                    *now,
                    *lease_expires_at,
                    *max_claims,
                    *cursor_partition_hash,
                    &mut any_mutated,
                )?
                .map(|v| SagaWriteResponse::ClaimDue(Ok(v))),
            };
            match outcome {
                Ok(resp) => staged_responses.push(resp),
                Err(err) => {
                    drop(table);
                    access.abort()?;
                    return Err(err);
                }
            }
        }

        drop(table);
        if any_mutated {
            access.commit_for(RedbTestOperation::ApplicationInstallationCampaign)?;
        } else {
            access.abort()?;
        }

        for (item, resp) in batch.iter().zip(staged_responses) {
            item.set_response(resp);
        }
        Ok(mem_state)
    }

    fn fail_batch(batch: &[Arc<SagaBatchItem>], err: &StorageError) {
        for item in batch {
            let cloned_err = storage_error(err.kind());
            match &item.request {
                SagaWriteRequest::Admit(_) => {
                    item.set_response(SagaWriteResponse::Admit(Err(cloned_err)));
                }
                SagaWriteRequest::Cas { .. } => {
                    item.set_response(SagaWriteResponse::Cas(Err(cloned_err)));
                }
                SagaWriteRequest::ClaimDue { .. } => {
                    item.set_response(SagaWriteResponse::ClaimDue(Err(cloned_err)));
                }
            }
        }
    }

    fn apply_admit_in_tx(
        table: &mut crate::changelog_v3_capture::CapturedTable<'_, '_, &'static [u8]>,
        mem_state: &mut SagaMemoryState,
        instance: &StoredSagaInstanceV1,
        any_mutated: &mut bool,
    ) -> Result<Result<SagaAdmissionStorageOutcomeV1, StorageError>, StorageError> {
        let admission_key = SagaAdmissionIndexKey::from_instance(instance);
        let existing_id = mem_state
            .index
            .by_admission_key
            .get(&admission_key)
            .copied()
            .or_else(|| Some(instance.saga_id()));
        let existing_instance = if let Some(existing_id) = existing_id {
            if let Some(cached) = mem_state.sagas_by_id.get(&existing_id).cloned() {
                Some(cached)
            } else {
                let key_bytes = existing_id.as_bytes();
                table
                    .get(key_bytes.as_slice())
                    .map_err(precommit_storage_error)?
                    .map(|raw| decode_saga_row_if_present(key_bytes, raw.value()))
                    .transpose()?
                    .flatten()
            }
        } else {
            None
        };
        if let Some(existing) = existing_instance {
            if existing.database_id() == instance.database_id()
                && existing.tenant_scope() == instance.tenant_scope()
                && existing.contract_lineage() == instance.contract_lineage()
                && existing.saga_name() == instance.saga_name()
                && existing.caller_idempotency_key() == instance.caller_idempotency_key()
            {
                if existing.saga_plan_hash() == instance.saga_plan_hash()
                    && saga_inputs_match_for_idempotent_replay(&existing, instance)
                {
                    return Ok(Ok(SagaAdmissionStorageOutcomeV1::Replayed(Box::new(
                        existing,
                    ))));
                }
                return Ok(Ok(
                    SagaAdmissionStorageOutcomeV1::ConflictingIdempotencyKey(Box::new(existing)),
                ));
            }
            return Ok(Err(storage_error(StorageErrorKind::InvariantViolation)));
        }

        let envelope = encode_saga_envelope(instance)?;
        let encoded = encode_application_installation_campaign_v1(&envelope)?;
        table
            .insert(instance.saga_id().as_bytes().as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        mem_state.upsert(instance.clone());
        *any_mutated = true;
        Ok(Ok(SagaAdmissionStorageOutcomeV1::Admitted(Box::new(
            instance.clone(),
        ))))
    }

    fn apply_cas_in_tx(
        table: &mut crate::changelog_v3_capture::CapturedTable<'_, '_, &'static [u8]>,
        mem_state: &mut SagaMemoryState,
        expected: SagaCompareExpectedV1,
        next: &StoredSagaInstanceV1,
        any_mutated: &mut bool,
    ) -> Result<Result<SagaCasOutcomeV1, StorageError>, StorageError> {
        let saga_id = next.saga_id();
        let key = saga_id.as_bytes();
        let current = if let Some(cached) = mem_state.sagas_by_id.get(&saga_id).cloned() {
            Some(cached)
        } else {
            table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .map(|value| decode_saga_row_if_present(key, value.value()))
                .transpose()?
                .flatten()
        };
        let Some(current) = current else {
            return Ok(Ok(SagaCasOutcomeV1::Conflict(None)));
        };
        let fencing_matches = expected
            .fencing_token()
            .is_none_or(|token| current.fencing_token() == token);
        if current.revision() != expected.revision()
            || !fencing_matches
            || next.fencing_token() < current.fencing_token()
            || current.contract_lineage() != next.contract_lineage()
            || current.saga_plan_hash() != next.saga_plan_hash()
        {
            return Ok(Ok(SagaCasOutcomeV1::Conflict(Some(Box::new(current)))));
        }

        let envelope = encode_saga_envelope(next)?;
        let encoded = encode_application_installation_campaign_v1(&envelope)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        mem_state.upsert(next.clone());
        *any_mutated = true;
        Ok(Ok(SagaCasOutcomeV1::Applied(Box::new(next.clone()))))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_claim_due_in_tx(
        table: &mut crate::changelog_v3_capture::CapturedTable<'_, '_, &'static [u8]>,
        mem_state: &mut SagaMemoryState,
        owner_id: [u8; 16],
        now: Timestamp,
        lease_expires_at: Timestamp,
        max_claims: usize,
        cursor_partition_hash: u64,
        any_mutated: &mut bool,
    ) -> Result<Result<Vec<StoredSagaInstanceV1>, StorageError>, StorageError> {
        let candidate_ids =
            mem_state
                .index
                .select_due_candidate_ids(now, max_claims, cursor_partition_hash);
        if candidate_ids.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        let mut claimed = Vec::with_capacity(candidate_ids.len());
        for saga_id in candidate_ids {
            let key = saga_id.as_bytes();
            let current = if let Some(cached) = mem_state.sagas_by_id.get(&saga_id).cloned() {
                Some(cached)
            } else {
                table
                    .get(key.as_slice())
                    .map_err(precommit_storage_error)?
                    .map(|value| decode_saga_row_if_present(key, value.value()))
                    .transpose()?
                    .flatten()
            };
            let Some(current) = current else {
                continue;
            };
            if !current.lifecycle_status().is_claimable()
                || current.next_wakeup_at() > now
                || current
                    .lease()
                    .is_some_and(|lease| lease.expires_at() > now)
            {
                continue;
            }
            let updated =
                Self::build_claimed_saga_instance(&current, owner_id, now, lease_expires_at)?;
            let envelope = encode_saga_envelope(&updated)?;
            let encoded = encode_application_installation_campaign_v1(&envelope)?;
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            mem_state.upsert(updated.clone());
            *any_mutated = true;
            claimed.push(updated);
        }
        Ok(Ok(claimed))
    }

    fn build_claimed_saga_instance(
        current: &StoredSagaInstanceV1,
        owner_id: [u8; 16],
        now: Timestamp,
        lease_expires_at: Timestamp,
    ) -> Result<StoredSagaInstanceV1, StorageError> {
        let next_revision = current
            .revision()
            .checked_next()
            .ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))?;
        let next_fencing = current
            .fencing_token()
            .checked_next()
            .ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))?;
        let next_status = match current.lifecycle_status() {
            SagaLifecycleStatusV1::BlockedRetryable => {
                if current
                    .pending_invocation()
                    .is_some_and(|p| p.direction() == riffdb_types::SagaStepDirectionV1::Compensate)
                    || current.cancel_requested()
                {
                    SagaLifecycleStatusV1::Compensating
                } else {
                    SagaLifecycleStatusV1::RunningForward
                }
            }
            other => other,
        };
        let lease = SagaLeaseStateV1::new(owner_id, next_fencing, lease_expires_at);
        StoredSagaInstanceV1::new(
            current.saga_id(),
            current.database_id(),
            current.environment().clone(),
            current.tenant_scope().clone(),
            current.contract_lineage().clone(),
            current.contract_version(),
            current.contract_bundle_hash(),
            current.saga_plan_hash(),
            current.saga_name().to_owned(),
            current.execution_role().to_owned(),
            current.delegated_actor_id().clone(),
            current.partition_hash(),
            current.caller_idempotency_key().clone(),
            current.retained_authority().clone(),
            next_revision,
            next_fencing,
            next_status,
            current.current_step_index(),
            current.cancel_requested(),
            current.retry_count(),
            current.next_wakeup_at(),
            Some(lease),
            current.saga_input_bytes().to_vec(),
            current.pending_invocation().cloned(),
            current.step_ledger().to_vec(),
            current.terminal_outcome().cloned(),
            current.diagnostic_reason().map(str::to_owned),
            current.created_at(),
            now,
        )
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn has_due_saga_candidate(&self, now: Timestamp) -> Result<bool, StorageError> {
        let coord = self.saga_coordinator();
        {
            let guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(ref mem) = guard.cached_state {
                return Ok(mem.index.has_due_candidate(now));
            }
        }
        let built = self.load_saga_memory_state()?;
        let has_due = built.index.has_due_candidate(now);
        let mut guard = coord
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.cached_state.is_none() && !guard.writer_active {
            guard.cached_state = Some(built);
        }
        Ok(has_due)
    }

    /// Returns whether any active saga is in a claimable lifecycle state
    /// (`RunningForward`, `Compensating`, or `BlockedRetryable`), including
    /// instances waiting for a future retry deadline or lease expiration.
    pub fn has_claimable_saga(&self) -> Result<bool, StorageError> {
        let coord = self.saga_coordinator();
        {
            let guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(ref mem) = guard.cached_state {
                return Ok(mem.index.has_claimable_saga());
            }
        }
        let built = self.load_saga_memory_state()?;
        let has_claimable = built.index.has_claimable_saga();
        let mut guard = coord
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.cached_state.is_none() && !guard.writer_active {
            guard.cached_state = Some(built);
        }
        Ok(has_claimable)
    }

    /// Returns the earliest timestamp at which any active claimable saga is
    /// eligible to be claimed (taking into account both `next_wakeup_at` and
    /// any active `lease.expires_at`).
    pub fn next_saga_wakeup_at(&self) -> Result<Option<Timestamp>, StorageError> {
        let coord = self.saga_coordinator();
        {
            let guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(ref mem) = guard.cached_state {
                return Ok(mem.index.next_claimable_wakeup_at());
            }
        }
        let built = self.load_saga_memory_state()?;
        let next_wakeup = built.index.next_claimable_wakeup_at();
        let mut guard = coord
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.cached_state.is_none() && !guard.writer_active {
            guard.cached_state = Some(built);
        }
        Ok(next_wakeup)
    }
}

fn saga_inputs_match_for_idempotent_replay(
    existing: &StoredSagaInstanceV1,
    candidate: &StoredSagaInstanceV1,
) -> bool {
    if existing.saga_input_bytes() == candidate.saga_input_bytes() {
        return true;
    }
    let (
        Ok(riffdb_types::CanonicalValue::Record(existing_rec)),
        Ok(riffdb_types::CanonicalValue::Record(candidate_rec)),
    ) = (
        riffdb_types::decode_canonical_value(existing.saga_input_bytes()),
        riffdb_types::decode_canonical_value(candidate.saga_input_bytes()),
    )
    else {
        return false;
    };
    if existing_rec.fields().len() != candidate_rec.fields().len() {
        return false;
    }
    let existing_uuid = riffdb_types::CanonicalValue::Uuid(existing.saga_id().into_bytes());
    let candidate_uuid = riffdb_types::CanonicalValue::Uuid(candidate.saga_id().into_bytes());
    existing_rec
        .fields()
        .iter()
        .zip(candidate_rec.fields().iter())
        .all(|((ef, ev), (cf, cv))| {
            ef == cf && (ev == cv || (*ev == existing_uuid && *cv == candidate_uuid))
        })
}

impl SagaCoordinationRepository for RedbOperationalPorts {
    fn admit_or_get_saga(
        &self,
        instance: &StoredSagaInstanceV1,
    ) -> Result<SagaAdmissionStorageOutcomeV1, StorageError> {
        if instance.revision() != riffdb_types::SagaRevision::first()
            || instance.fencing_token() != riffdb_types::SagaFencingToken::first()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        match self.submit_saga_write(SagaWriteRequest::Admit(Box::new(instance.clone()))) {
            SagaWriteResponse::Admit(res) => res,
            SagaWriteResponse::Cas(_) | SagaWriteResponse::ClaimDue(_) => {
                Err(storage_error(StorageErrorKind::InvariantViolation))
            }
        }
    }

    fn read_saga(&self, saga_id: SagaId) -> Result<Option<StoredSagaInstanceV1>, StorageError> {
        let coord = self.saga_coordinator();
        {
            let guard = coord
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(ref mem) = guard.cached_state {
                return Ok(mem.sagas_by_id.get(&saga_id).cloned());
            }
        }
        let built = self.load_saga_memory_state()?;
        let found = built.sagas_by_id.get(&saga_id).cloned();
        let mut guard = coord
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.cached_state.is_none() && !guard.writer_active {
            guard.cached_state = Some(built);
        }
        Ok(found)
    }

    fn compare_and_swap_saga(
        &self,
        expected: SagaCompareExpectedV1,
        next: &StoredSagaInstanceV1,
    ) -> Result<SagaCasOutcomeV1, StorageError> {
        if next.revision() <= expected.revision() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        match self.submit_saga_write(SagaWriteRequest::Cas {
            expected,
            next: Box::new(next.clone()),
        }) {
            SagaWriteResponse::Cas(res) => res,
            SagaWriteResponse::Admit(_) | SagaWriteResponse::ClaimDue(_) => {
                Err(storage_error(StorageErrorKind::InvariantViolation))
            }
        }
    }

    fn claim_due_sagas(
        &self,
        owner_id: [u8; 16],
        now: Timestamp,
        lease_expires_at: Timestamp,
        max_claims: usize,
        cursor_partition_hash: u64,
    ) -> Result<Vec<StoredSagaInstanceV1>, StorageError> {
        if max_claims == 0 {
            return Ok(Vec::new());
        }
        if !self.has_due_saga_candidate(now)? {
            return Ok(Vec::new());
        }
        match self.submit_saga_write(SagaWriteRequest::ClaimDue {
            owner_id,
            now,
            lease_expires_at,
            max_claims,
            cursor_partition_hash,
        }) {
            SagaWriteResponse::ClaimDue(res) => res,
            SagaWriteResponse::Admit(_) | SagaWriteResponse::Cas(_) => {
                Err(storage_error(StorageErrorKind::InvariantViolation))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::PathBuf;

    use riffdb_storage_api::{
        ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
        DatabaseInitializationPort, SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1,
        SagaCompareExpectedV1, SagaCoordinationRepository, StoredApplicationInstallationCampaignV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, ApplicationInstallationCampaignId, ApplicationInstallationPlanHash,
        Audience, CapabilityId, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
        Environment, IdempotencyKey, PlanHash, SagaFencingToken, SagaId, SagaLifecycleStatusV1,
        SagaRetainedAuthorityV1, SagaRevision, StoredSagaInstanceV1, TenantScope, Timestamp,
        format_saga_delegated_actor_id,
    };

    use crate::store::{RedbDormantPorts, RedbStore};

    struct TestPath(
        PathBuf,
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new() -> Self {
            let scope = crate::test_path::ScopedDirectory::new("saga-storage");
            Self(scope.join("db.redb"), scope)
        }
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn sample_saga(
        seed: u8,
        idempotency_key: &str,
        partition_hash: u64,
        input_bytes: Vec<u8>,
    ) -> StoredSagaInstanceV1 {
        let lineage = ContractLineage::new("OrderDesk").expect("lineage");
        let delegated = format_saga_delegated_actor_id(&lineage, "PlaceOrder", "CheckoutService")
            .expect("delegated");
        let now = Timestamp::new(1_700_000_000, 0).expect("ts");
        let authority = SagaRetainedAuthorityV1::new(
            CapabilityId::from_bytes(uuid(0x21)).expect("cap"),
            NonZeroU64::MIN,
            ActorId::new("user:alice").expect("actor"),
            ActorKind::Human,
            Audience::new("riffdb:OrderDesk").expect("audience"),
            TenantScope::Global,
            now,
        );
        StoredSagaInstanceV1::new(
            SagaId::from_bytes(uuid(seed)).expect("saga_id"),
            DatabaseId::from_bytes(uuid(1)).expect("db"),
            Environment::new("prod").expect("env"),
            TenantScope::Global,
            lineage,
            ContractVersion::new(1).expect("v1"),
            ContractBundleHash::from_bytes([0x11; 32]),
            PlanHash::from_bytes([0x22; 32]),
            "PlaceOrder",
            "CheckoutService",
            delegated,
            partition_hash,
            IdempotencyKey::new(idempotency_key).expect("idem"),
            authority,
            SagaRevision::first(),
            SagaFencingToken::first(),
            SagaLifecycleStatusV1::RunningForward,
            0,
            false,
            0,
            now,
            None,
            input_bytes,
            None,
            Vec::new(),
            None,
            None,
            now,
            now,
        )
        .expect("saga")
    }

    // req: WF-002, WF-005, ID-001, ID-002
    #[test]
    fn saga_admit_fenced_cas_claim_and_domain_separation_round_trip() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let mut ports = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("activate");

        let s1 = sample_saga(2, "order-1", 10, vec![1, 2, 3]);
        assert_eq!(
            ports.admit_or_get_saga(&s1).expect("admit"),
            SagaAdmissionStorageOutcomeV1::Admitted(Box::new(s1.clone()))
        );

        // Idempotent replay with different SagaId but same caller_idempotency_key returns existing s1.
        let s1_retry = sample_saga(3, "order-1", 10, vec![1, 2, 3]);
        assert_eq!(
            ports.admit_or_get_saga(&s1_retry).expect("replay"),
            SagaAdmissionStorageOutcomeV1::Replayed(Box::new(s1.clone()))
        );

        // Conflicting input bytes on the same caller_idempotency_key is detected.
        let s1_conflict = sample_saga(4, "order-1", 10, vec![9, 9, 9]);
        assert_eq!(
            ports.admit_or_get_saga(&s1_conflict).expect("conflict"),
            SagaAdmissionStorageOutcomeV1::ConflictingIdempotencyKey(Box::new(s1.clone()))
        );

        // Domain separation: ApplicationInstallationCampaignRepository does not see saga rows
        // and refuses overwriting a saga row.
        let campaign_id = ApplicationInstallationCampaignId::from_bytes(s1.saga_id().into_bytes())
            .expect("campaign id");
        assert_eq!(
            ports
                .read_application_installation_campaign(campaign_id)
                .expect("read campaign"),
            None
        );
        let colliding_campaign = StoredApplicationInstallationCampaignV1::new(
            campaign_id,
            ContractLineage::new("OrderDesk").expect("lineage"),
            ApplicationInstallationPlanHash::from_bytes([7; 32]),
            b"campaign-state".to_vec(),
        )
        .expect("campaign");
        assert_eq!(
            ports
                .compare_and_swap_application_installation_campaign(None, &colliding_campaign)
                .expect("refuse overwrite"),
            ApplicationInstallationCampaignWriteResultV1::CompareMismatch
        );

        // Claim due sagas in partition_hash order.
        let s2 = sample_saga(5, "order-2", 20, vec![4, 5, 6]);
        ports.admit_or_get_saga(&s2).expect("admit s2");
        let now = Timestamp::new(1_700_000_005, 0).expect("now");
        let expires = Timestamp::new(1_700_000_035, 0).expect("expires");
        let claimed = ports
            .claim_due_sagas([0xAA; 16], now, expires, 1, 15)
            .expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].saga_id(), s2.saga_id());
        assert_eq!(claimed[0].revision().get(), 2);
        assert_eq!(claimed[0].fencing_token().get(), 2);

        // Stale fencing token CAS fails closed.
        let stale_expected =
            SagaCompareExpectedV1::fenced(claimed[0].revision(), SagaFencingToken::first());
        let mut next = claimed[0].clone();
        next = StoredSagaInstanceV1::new(
            next.saga_id(),
            next.database_id(),
            next.environment().clone(),
            next.tenant_scope().clone(),
            next.contract_lineage().clone(),
            next.contract_version(),
            next.contract_bundle_hash(),
            next.saga_plan_hash(),
            next.saga_name().to_owned(),
            next.execution_role().to_owned(),
            next.delegated_actor_id().clone(),
            next.partition_hash(),
            next.caller_idempotency_key().clone(),
            next.retained_authority().clone(),
            next.revision().checked_next().expect("rev 3"),
            next.fencing_token(),
            SagaLifecycleStatusV1::RunningForward,
            0,
            false,
            0,
            now,
            next.lease().copied(),
            next.saga_input_bytes().to_vec(),
            None,
            Vec::new(),
            None,
            None,
            next.created_at(),
            now,
        )
        .expect("next");
        assert!(matches!(
            ports
                .compare_and_swap_saga(stale_expected, &next)
                .expect("stale cas"),
            SagaCasOutcomeV1::Conflict(Some(_))
        ));

        // Matching revision and fencing token succeeds.
        let valid_expected =
            SagaCompareExpectedV1::fenced(claimed[0].revision(), claimed[0].fencing_token());
        assert_eq!(
            ports
                .compare_and_swap_saga(valid_expected, &next)
                .expect("valid cas"),
            SagaCasOutcomeV1::Applied(Box::new(next))
        );
    }

    // req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001
    #[test]
    fn active_and_terminal_sagas_round_trip_across_backup_restore_and_startup_validation() {
        use riffdb_types::{
            CommandId, CommittedSagaStepRecordV1, OutcomeId, PendingStepInvocationV1,
            SagaStepDirectionV1, SagaStepEffectDispositionV1, SagaTerminalOutcomeRecordV1,
        };

        let path = TestPath::new();
        let backup_scope = crate::test_path::ScopedDirectory::new("saga-storage-restored");
        let db_id = DatabaseId::from_bytes(uuid(1)).expect("database");
        let now = Timestamp::new(1_700_000_010, 0).expect("now");

        let (active_saga_id, terminal_saga_id, expected_active, expected_terminal) = {
            let mut store = RedbStore::open(&path.0).expect("open");
            store.initialize_database(db_id).expect("initialize");
            let ports = RedbDormantPorts {
                pending_v3_activation: None,
                shared: store.shared,
            }
            .into_operational_after_catalog_validation()
            .expect("activate");

            let base_active = sample_saga(10, "backup-active-1", 101, vec![10, 20, 30]);
            ports
                .admit_or_get_saga(&base_active)
                .expect("admit active saga");
            let claimed = ports
                .claim_due_sagas(
                    [0xBB; 16],
                    now,
                    Timestamp::new(1_700_000_040, 0).unwrap(),
                    1,
                    15,
                )
                .expect("claim active");
            assert_eq!(claimed.len(), 1);

            let pending = PendingStepInvocationV1::new(
                0,
                SagaStepDirectionV1::Forward,
                CommandId::new(1).unwrap(),
                claimed[0].contract_lineage().clone(),
                claimed[0].contract_version(),
                claimed[0].contract_bundle_hash(),
                PlanHash::from_bytes([0x33; 32]),
                vec![0xAA, 0xBB],
                claimed[0].tenant_scope().clone(),
                claimed[0].delegated_actor_id().clone(),
                IdempotencyKey::new("saga-step:018f:0:forward").unwrap(),
            )
            .unwrap();
            let active_prepared = StoredSagaInstanceV1::new(
                claimed[0].saga_id(),
                claimed[0].database_id(),
                claimed[0].environment().clone(),
                claimed[0].tenant_scope().clone(),
                claimed[0].contract_lineage().clone(),
                claimed[0].contract_version(),
                claimed[0].contract_bundle_hash(),
                claimed[0].saga_plan_hash(),
                claimed[0].saga_name().to_owned(),
                claimed[0].execution_role().to_owned(),
                claimed[0].delegated_actor_id().clone(),
                claimed[0].partition_hash(),
                claimed[0].caller_idempotency_key().clone(),
                claimed[0].retained_authority().clone(),
                claimed[0].revision().checked_next().unwrap(),
                claimed[0].fencing_token(),
                SagaLifecycleStatusV1::RunningForward,
                0,
                false,
                0,
                now,
                claimed[0].lease().copied(),
                claimed[0].saga_input_bytes().to_vec(),
                Some(pending),
                Vec::new(),
                None,
                None,
                claimed[0].created_at(),
                now,
            )
            .unwrap();
            ports
                .compare_and_swap_saga(
                    SagaCompareExpectedV1::fenced(
                        claimed[0].revision(),
                        claimed[0].fencing_token(),
                    ),
                    &active_prepared,
                )
                .expect("prepare active step");

            let base_terminal = sample_saga(11, "backup-terminal-1", 202, vec![40, 50, 60]);
            ports
                .admit_or_get_saga(&base_terminal)
                .expect("admit terminal saga");
            let committed_step = CommittedSagaStepRecordV1::new(
                0,
                SagaStepDirectionV1::Forward,
                OutcomeId::new(1).unwrap(),
                SagaStepEffectDispositionV1::Effectful,
                vec![0xCC, 0xDD],
            )
            .unwrap();
            let terminal_outcome = SagaTerminalOutcomeRecordV1::new(
                OutcomeId::new(10).unwrap(),
                "OrderCompleted",
                vec![1, 2],
            )
            .unwrap();
            let terminal_completed = StoredSagaInstanceV1::new(
                base_terminal.saga_id(),
                base_terminal.database_id(),
                base_terminal.environment().clone(),
                base_terminal.tenant_scope().clone(),
                base_terminal.contract_lineage().clone(),
                base_terminal.contract_version(),
                base_terminal.contract_bundle_hash(),
                base_terminal.saga_plan_hash(),
                base_terminal.saga_name().to_owned(),
                base_terminal.execution_role().to_owned(),
                base_terminal.delegated_actor_id().clone(),
                base_terminal.partition_hash(),
                base_terminal.caller_idempotency_key().clone(),
                base_terminal.retained_authority().clone(),
                base_terminal.revision().checked_next().unwrap(),
                base_terminal.fencing_token(),
                SagaLifecycleStatusV1::Completed,
                1,
                false,
                0,
                now,
                None,
                base_terminal.saga_input_bytes().to_vec(),
                None,
                vec![committed_step],
                Some(terminal_outcome),
                None,
                base_terminal.created_at(),
                now,
            )
            .unwrap();
            ports
                .compare_and_swap_saga(
                    SagaCompareExpectedV1::fenced(
                        base_terminal.revision(),
                        base_terminal.fencing_token(),
                    ),
                    &terminal_completed,
                )
                .expect("complete terminal saga");

            let read = ports.begin_read().expect("begin_read for changelog check");
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .expect("validate_retained_history")
                .expect("v3 history present");
            assert!(
                history.tail().sequence().get() >= 5,
                "expected saga transitions to advance V3 changelog history, got {:?}",
                history.tail().sequence()
            );

            (
                active_prepared.saga_id(),
                terminal_completed.saga_id(),
                active_prepared,
                terminal_completed,
            )
        };

        assert!(
            !super::saga_wal_path(&path.0).exists(),
            "saga transitions must commit through redb and V3 changelog, not sidecar .sagalog"
        );

        use riffdb_storage_api::{
            BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
            OfflineRestorePersistencePort,
        };
        let backup_dir = backup_scope.join("backup");
        let restored_dir = backup_scope.join("restored");
        let build =
            BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, Vec::new())
                .expect("build metadata");
        crate::backup::RedbOfflineBackup::bind(&path.0, &backup_dir)
            .create_offline_backup(&build)
            .expect("create supported offline backup");
        crate::backup::RedbOfflineRestore::bind(&backup_dir, &restored_dir)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect("restore supported offline backup");

        let restored_db_path = restored_dir.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
        let restored_store = RedbStore::open(&restored_db_path).expect("open restored backup");
        let restored_ports = RedbDormantPorts {
            pending_v3_activation: None,
            shared: restored_store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("activate restored ports");

        let restored_active = restored_ports
            .read_saga(active_saga_id)
            .expect("read restored active")
            .expect("active saga present");
        let restored_terminal = restored_ports
            .read_saga(terminal_saga_id)
            .expect("read restored terminal")
            .expect("terminal saga present");

        assert_eq!(restored_active, expected_active);
        assert_eq!(
            restored_active.saga_plan_hash(),
            expected_active.saga_plan_hash()
        );
        assert_eq!(
            restored_active.fencing_token(),
            expected_active.fencing_token()
        );
        assert_eq!(
            restored_active
                .pending_invocation()
                .unwrap()
                .caller_idempotency_key(),
            expected_active
                .pending_invocation()
                .unwrap()
                .caller_idempotency_key()
        );

        assert_eq!(restored_terminal, expected_terminal);
        assert_eq!(
            restored_terminal.step_ledger()[0].outcome_payload_bytes(),
            expected_terminal.step_ledger()[0].outcome_payload_bytes()
        );
    }
}
