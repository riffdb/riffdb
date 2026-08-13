//! Production coordinator for bounded, snapshot-consistent symbolic export.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use riffdb_application::{ApplicationPortabilityManifest, PortableRecordClass};
use riffdb_catalog::ValidatedContractBundle;
use riffdb_contract_ir::{RecordSchema, RecordTypeRef, ValueType};
use riffdb_policy::{
    AuthorizedApplicationExportV1, AuthorizedQueryRowPolicyContextV1, EventPolicyCandidateV1,
    resolve_authorized_application_export_row_policy_context,
};
use riffdb_service::{
    ApplicationExportCancelPermitV1, ApplicationExportCoordinatorPort, ApplicationExportCursor,
    ApplicationExportFailureV1, ApplicationExportMutationPortErrorV1,
    ApplicationExportObservationPermitV1, ApplicationExportObservationPortErrorV1,
    ApplicationExportOperationV1, ApplicationExportPagePermitV1, ApplicationExportPageV1,
    ApplicationExportPhaseV1, ApplicationExportStartDispositionV1, ApplicationExportStartPermitV1,
    ApplicationExportStartResultV1, AuthorizedApplicationExportOperationV1,
    AuthorizedApplicationExportPageV1, AuthorizedApplicationExportStartV1,
    CanonicalApplicationExportJsonDocument, CanonicalApplicationExportJsonLine, PortAdmissionError,
    PortFuture, RequestControl,
};
use riffdb_storage_api::{
    ApplicationExportEventRecordV1, ApplicationExportOperationRepository,
    ApplicationExportOperationWriteResultV1, ApplicationExportSnapshotPort,
    ApplicationExportSnapshotReader, ApplicationExportSourceRecordV1,
    MAX_ACTIVE_APPLICATION_EXPORTS, StorageError, StorageErrorKind, StorageScanLimit,
    StoredApplicationExportOperationV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApplicationExportAuthorityV1,
    ApplicationExportClassV1, ApplicationExportOperationId, ApplicationExportPageHash,
    ApplicationExportSelectionV1, ApplicationExportSnapshotBindingV1,
    ApplicationPortabilityManifestHash, ApplicationRoleHash, CanonicalValue,
    CapabilityApplicationExportScopeV1, CapabilityId, CommitSequence, ContractBundleHash,
    ContractLineage, ContractVersion, DatabaseId, HashDomain, QueryModuleHash, ReactiveModuleHash,
    Timestamp, hash,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::clocks::ServerApplicationExportClock;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

const STATE_SCHEMA: &str = "riffdb.application-export-operation/v1";
const PORTABILITY_STATE_SCHEMA: &str = "riffdb.application-export-operation/v2";
const MANIFEST_SCHEMA: &str = "riffdb.application-export-manifest/v1";
const RECEIPT_SCHEMA: &str = "riffdb.application-export-receipt/v1";
const PORTABILITY_MANIFEST_SCHEMA: &str = "riffdb.application-export-manifest/v2";
const PORTABILITY_RECEIPT_SCHEMA: &str = "riffdb.application-export-receipt/v2";
const CURSOR_VERSION: u8 = 1;
const MAX_RETAINED_PAGE_HASHES: usize = 4_096;
const MAX_EXPORT_LINES_PER_STORAGE_CHUNK: u16 = 64;
const MAX_POLICY_SOURCE_ROWS_PER_PUBLIC_PAGE: usize = 100_000;
const MAX_APPLICATION_EXPORT_REPLAY_BYTES: usize = 64 * 1024 * 1024;
const APPLICATION_EXPORT_REPLAY_FIXED_CHARGE: usize = 512;

type SnapshotMap =
    Arc<Mutex<BTreeMap<ApplicationExportOperationId, Arc<dyn ApplicationExportSnapshotReader>>>>;
type PageReplayMap = Arc<Mutex<PageReplayCache>>;

#[derive(Default)]
struct PageReplayCache {
    retained_bytes: usize,
    entries: BTreeMap<ApplicationExportOperationId, PageReplayEntry>,
}

#[derive(Clone)]
struct PageReplayEntry {
    request_cursor: ApplicationExportCursor,
    successor_state: Vec<u8>,
    page: ApplicationExportPageV1,
    retained_bytes: usize,
}

/// Bounded blocking adapter owning snapshots and durable export checkpoints.
pub(crate) struct ServerApplicationExportCoordinator {
    selection: BlockingPortExecutor<
        ApplicationExportOperationId,
        Option<ApplicationExportSelectionV1>,
        ApplicationExportObservationPortErrorV1,
    >,
    start: BlockingPortExecutor<
        AuthorizedApplicationExportStartV1,
        ApplicationExportStartResultV1,
        ApplicationExportMutationPortErrorV1,
    >,
    page: BlockingPortExecutor<
        AuthorizedApplicationExportPageV1,
        ApplicationExportPageV1,
        ApplicationExportMutationPortErrorV1,
    >,
    observation: BlockingPortExecutor<
        AuthorizedApplicationExportOperationV1,
        Option<ApplicationExportOperationV1>,
        ApplicationExportObservationPortErrorV1,
    >,
    cancel: BlockingPortExecutor<
        AuthorizedApplicationExportOperationV1,
        Option<ApplicationExportOperationV1>,
        ApplicationExportMutationPortErrorV1,
    >,
}

impl ServerApplicationExportCoordinator {
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        driver: &BlockingPortDriver,
        clock: ServerApplicationExportClock,
    ) -> Self {
        let snapshots: SnapshotMap = Arc::new(Mutex::new(BTreeMap::new()));
        let replays: PageReplayMap = Arc::new(Mutex::new(PageReplayCache::default()));
        let selection_storage = storage.clone();
        let start_storage = storage.clone();
        let page_storage = storage.clone();
        let observation_storage = storage.clone();
        let start_snapshots = Arc::clone(&snapshots);
        let page_snapshots = Arc::clone(&snapshots);
        let observation_snapshots = Arc::clone(&snapshots);
        let cancel_snapshots = Arc::clone(&snapshots);
        let start_replays = Arc::clone(&replays);
        let page_replays = Arc::clone(&replays);
        let observation_replays = Arc::clone(&replays);
        let cancel_replays = Arc::clone(&replays);
        let start_clock = clock.clone();
        let page_clock = clock.clone();
        let observation_clock = clock;
        Self {
            selection: driver
                .executor(move |operation_id| resolve_selection(&selection_storage, operation_id)),
            start: driver.executor(move |request| {
                start_or_replay(
                    start_storage.clone(),
                    &start_snapshots,
                    &start_replays,
                    &start_clock,
                    request,
                )
            }),
            page: driver.executor(move |request| {
                release_page(
                    page_storage.clone(),
                    &page_snapshots,
                    &page_replays,
                    &page_clock,
                    request,
                )
            }),
            observation: driver.executor(move |request| {
                observe(
                    observation_storage.clone(),
                    &observation_snapshots,
                    &observation_replays,
                    &observation_clock,
                    request,
                )
            }),
            cancel: driver.executor(move |request| {
                cancel(storage.clone(), &cancel_snapshots, &cancel_replays, request)
            }),
        }
    }
}

impl ApplicationExportCoordinatorPort for ServerApplicationExportCoordinator {
    fn resolve_application_export_selection(
        &self,
        operation_id: ApplicationExportOperationId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ApplicationExportSelectionV1>, ApplicationExportObservationPortErrorV1>
    {
        let permit = self.selection.reserve(control);
        Box::pin(async move {
            permit
                .map_err(|_| ApplicationExportObservationPortErrorV1::Unavailable)?
                .submit(operation_id)
                .map_err(|_| ApplicationExportObservationPortErrorV1::Unavailable)?
                .completion()
                .await
                .map_err(|_| ApplicationExportObservationPortErrorV1::Unavailable)?
        })
    }

    fn reserve_application_export_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportStartPermitV1, PortAdmissionError> {
        let reservation = self.start.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_application_export_page(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportPagePermitV1, PortAdmissionError> {
        let reservation = self.page.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_application_export_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportObservationPermitV1, PortAdmissionError> {
        let reservation = self.observation.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_application_export_cancel(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportCancelPermitV1, PortAdmissionError> {
        let reservation = self.cancel.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerApplicationExportCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerApplicationExportCoordinator([EXACT_DURABLE_STATE])")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExportStateWireV1 {
    schema: String,
    operation_id: [u8; 16],
    lineage: String,
    scope: u8,
    entities: bool,
    events: bool,
    provenance: bool,
    public_audit: bool,
    database_id: [u8; 16],
    history_incarnation: u64,
    application_frontier: Option<u64>,
    administration_frontier: Option<u64>,
    contract_version: u64,
    contract_bundle_hash: [u8; 32],
    query_modules: Vec<[u8; 32]>,
    reactive_modules: Vec<[u8; 32]>,
    capability_id: [u8; 16],
    capability_revision: u64,
    principal_id: String,
    actor_kind: u8,
    row_policy_role_hash: Option<[u8; 32]>,
    row_policy_names: Vec<String>,
    lease_seconds: i64,
    lease_nanos: u32,
    phase: u8,
    failure: Option<u8>,
    current_class: Option<u8>,
    continuation: Option<Vec<u8>>,
    pages_released: u64,
    rows_released: u64,
    bytes_released: u64,
    class_pages: [u64; 4],
    class_rows: [u64; 4],
    class_bytes: [u64; 4],
    page_hashes: Vec<[u8; 32]>,
    manifest: Option<Vec<u8>>,
    receipt: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    portability_manifest_hash: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    workflow_quiescence: Vec<WorkflowQuiescenceWireV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    portability_entity_schedule: Vec<u32>,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    portability_entity_schedule_index: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkflowQuiescenceWireV1 {
    workflow: String,
    entity_type_id: u32,
    owner_field_id: u32,
    expiry_field_id: u32,
    checked_rows: u64,
    quiescent_rows: u64,
    non_quiescent_rows: u64,
}

#[derive(Clone)]
struct ExportState {
    operation_id: ApplicationExportOperationId,
    selection: ApplicationExportSelectionV1,
    snapshot: ApplicationExportSnapshotBindingV1,
    authority: ApplicationExportAuthorityV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    row_policy_role_hash: Option<ApplicationRoleHash>,
    row_policy_names: Vec<String>,
    lease_expires_at: Timestamp,
    phase: ApplicationExportPhaseV1,
    failure: Option<ApplicationExportFailureV1>,
    current_class: Option<ApplicationExportClassV1>,
    continuation: Option<Vec<u8>>,
    pages_released: u64,
    rows_released: u64,
    bytes_released: u64,
    class_pages: [u64; 4],
    class_rows: [u64; 4],
    class_bytes: [u64; 4],
    page_hashes: Vec<ApplicationExportPageHash>,
    manifest: Option<Vec<u8>>,
    receipt: Option<Vec<u8>>,
    portability_manifest_hash: Option<ApplicationPortabilityManifestHash>,
    workflow_quiescence: Vec<WorkflowQuiescenceWireV1>,
    portability_entity_schedule: Vec<riffdb_types::EntityTypeId>,
    portability_entity_schedule_index: u16,
}

fn resolve_selection(
    storage: &SharedRedbOperationalPorts,
    operation_id: ApplicationExportOperationId,
) -> Result<Option<ApplicationExportSelectionV1>, ApplicationExportObservationPortErrorV1> {
    storage
        .read_application_export_operation(operation_id)
        .map_err(map_observation_storage)?
        .map(|record| decode_state(&record).map(|state| state.selection))
        .transpose()
        .map_err(|()| ApplicationExportObservationPortErrorV1::Integrity)
}

fn start_or_replay(
    mut storage: SharedRedbOperationalPorts,
    snapshots: &SnapshotMap,
    replays: &PageReplayMap,
    clock: &ServerApplicationExportClock,
    request: AuthorizedApplicationExportStartV1,
) -> Result<ApplicationExportStartResultV1, ApplicationExportMutationPortErrorV1> {
    let (_request_id, _ingress, request, authorization) = request.into_parts();
    validate_proof(&authorization, request.operation_id(), request.selection())?;
    if let Some(record) = storage
        .read_application_export_operation(request.operation_id())
        .map_err(map_mutation_storage)?
    {
        let mut state =
            decode_state(&record).map_err(|()| ApplicationExportMutationPortErrorV1::Integrity)?;
        let requested_portability_hash = request
            .intent()
            .portability_manifest()
            .map(ApplicationPortabilityManifest::identity);
        if state.selection != *request.selection()
            || state.portability_manifest_hash != requested_portability_hash
        {
            return Err(ApplicationExportMutationPortErrorV1::InputMismatch);
        }
        if !same_authority(&state, &authorization) {
            terminalize(
                &mut storage,
                Some(&record),
                &mut state,
                ApplicationExportFailureV1::AuthorityChanged,
            )?;
            remove_snapshot(snapshots, state.operation_id)?;
            remove_replay(replays, state.operation_id)?;
        }
        let disposition = if state.phase.is_terminal() {
            ApplicationExportStartDispositionV1::Terminal
        } else {
            ApplicationExportStartDispositionV1::AlreadyAccepted
        };
        return start_result(state, disposition);
    }

    let snapshot = storage
        .capture_application_export_snapshot(request.selection().lineage())
        .map_err(map_mutation_storage)?;
    let bundle = ValidatedContractBundle::decode(snapshot.contract_bundle_bytes())
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let (portability_manifest_hash, workflow_quiescence, portability_entity_schedule) =
        match request.intent().portability_manifest() {
            Some(manifest) => {
                manifest
                    .validate_compiled_contract(bundle.bundle())
                    .map_err(|_| ApplicationExportMutationPortErrorV1::InputMismatch)?;
                (
                    Some(manifest.identity()),
                    portability_workflows(manifest, &bundle)?,
                    manifest
                        .compiled_reimport_entity_schedule(bundle.bundle())
                        .map_err(|_| ApplicationExportMutationPortErrorV1::InputMismatch)?,
                )
            }
            None => (None, Vec::new(), Vec::new()),
        };
    let now = clock
        .now()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Unavailable)?;
    let lease_expires_at = add_seconds(now, u64::from(request.lease_seconds()))
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    let (row_policy_role_hash, mut row_policy_names) =
        match authorization.internal_row_policy_authority() {
            Some(authority) => (
                Some(authority.internal_grant().application_role_hash()),
                authority
                    .internal_grant()
                    .bindings()
                    .iter()
                    .map(|binding| binding.policy_name().as_str().to_owned())
                    .collect(),
            ),
            None => (None, Vec::new()),
        };
    row_policy_names.sort();
    row_policy_names.dedup();
    let state = ExportState {
        operation_id: request.operation_id(),
        selection: request.selection().clone(),
        snapshot: snapshot.binding().clone(),
        authority: authorization.authority(),
        principal_id: authorization.principal_id().clone(),
        actor_kind: authorization.actor_kind(),
        row_policy_role_hash,
        row_policy_names,
        lease_expires_at,
        phase: ApplicationExportPhaseV1::Accepted,
        failure: None,
        current_class: first_selected_class(request.selection()),
        continuation: None,
        pages_released: 0,
        rows_released: 0,
        bytes_released: 0,
        class_pages: [0; 4],
        class_rows: [0; 4],
        class_bytes: [0; 4],
        page_hashes: Vec::new(),
        manifest: None,
        receipt: None,
        portability_manifest_hash,
        workflow_quiescence,
        portability_entity_schedule,
        portability_entity_schedule_index: 0,
    };
    let replacement = stored_state(&state)?;
    insert_snapshot_candidate(snapshots, state.operation_id, Arc::clone(&snapshot))?;
    let write = storage.compare_and_swap_application_export_operation(None, &replacement);
    match write {
        Err(error) => {
            remove_snapshot(snapshots, state.operation_id)?;
            Err(map_mutation_storage(error))
        }
        Ok(
            ApplicationExportOperationWriteResultV1::Applied
            | ApplicationExportOperationWriteResultV1::Unchanged,
        ) => start_result(state, ApplicationExportStartDispositionV1::Accepted),
        Ok(ApplicationExportOperationWriteResultV1::CompareMismatch) => {
            remove_snapshot(snapshots, state.operation_id)?;
            Err(ApplicationExportMutationPortErrorV1::OutcomeUnknown)
        }
    }
}

fn release_page(
    mut storage: SharedRedbOperationalPorts,
    snapshots: &SnapshotMap,
    replays: &PageReplayMap,
    clock: &ServerApplicationExportClock,
    request: AuthorizedApplicationExportPageV1,
) -> Result<ApplicationExportPageV1, ApplicationExportMutationPortErrorV1> {
    let (request, authorization) = request.into_parts();
    let retained = storage
        .read_application_export_operation(request.operation_id())
        .map_err(map_mutation_storage)?
        .ok_or(ApplicationExportMutationPortErrorV1::InputMismatch)?;
    let mut state =
        decode_state(&retained).map_err(|()| ApplicationExportMutationPortErrorV1::Integrity)?;
    validate_proof(&authorization, state.operation_id, &state.selection)?;
    if !same_authority(&state, &authorization) {
        if !state.phase.is_terminal() {
            terminalize(
                &mut storage,
                Some(&retained),
                &mut state,
                ApplicationExportFailureV1::AuthorityChanged,
            )?;
        }
        remove_snapshot(snapshots, state.operation_id)?;
        remove_replay(replays, state.operation_id)?;
        return Err(ApplicationExportMutationPortErrorV1::InputMismatch);
    }
    if clock
        .now()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Unavailable)?
        >= state.lease_expires_at
    {
        if !state.phase.is_terminal() {
            terminalize(
                &mut storage,
                Some(&retained),
                &mut state,
                ApplicationExportFailureV1::LeaseExpired,
            )?;
        }
        remove_snapshot(snapshots, state.operation_id)?;
        remove_replay(replays, state.operation_id)?;
        return Err(ApplicationExportMutationPortErrorV1::Unavailable);
    }
    if let Some(page) = replayed_page(
        replays,
        state.operation_id,
        request.cursor(),
        retained.canonical_state(),
    )? {
        return Ok(page);
    }
    if state.phase.is_terminal() {
        return Err(ApplicationExportMutationPortErrorV1::InputMismatch);
    }
    if cursor_for(&retained)? != *request.cursor() {
        return Err(ApplicationExportMutationPortErrorV1::InputMismatch);
    }
    let snapshot = snapshots
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?
        .get(&state.operation_id)
        .cloned();
    let Some(snapshot) = snapshot else {
        terminalize(
            &mut storage,
            Some(&retained),
            &mut state,
            ApplicationExportFailureV1::SnapshotUnavailable,
        )?;
        remove_replay(replays, state.operation_id)?;
        return Err(ApplicationExportMutationPortErrorV1::Unavailable);
    };
    if snapshot.binding() != &state.snapshot {
        return Err(ApplicationExportMutationPortErrorV1::Integrity);
    }
    let class = state
        .current_class
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    let bundle = ValidatedContractBundle::decode(snapshot.contract_bundle_bytes())
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let row_policy =
        resolve_authorized_application_export_row_policy_context(&authorization, bundle.bundle())
            .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let target_rows = request
        .max_rows()
        .min(authorization.internal_max_scan_rows().get())
        .min(MAX_EXPORT_LINES_PER_STORAGE_CHUNK);
    let field_visibility = (state.selection.scope()
        == CapabilityApplicationExportScopeV1::PrincipalFiltered)
        .then_some(authorization.internal_field_visibility());
    let mut lines = Vec::with_capacity(usize::from(target_rows));
    let mut continuation = state.continuation.clone();
    let mut class_complete = false;
    let mut source_rows = 0usize;
    while lines.len() < usize::from(target_rows)
        && source_rows < MAX_POLICY_SOURCE_ROWS_PER_PUBLIC_PAGE
        && !class_complete
    {
        let remaining = usize::from(target_rows) - lines.len();
        let chunk = u16::try_from(remaining)
            .unwrap_or(MAX_EXPORT_LINES_PER_STORAGE_CHUNK)
            .min(MAX_EXPORT_LINES_PER_STORAGE_CHUNK);
        let limit =
            StorageScanLimit::new(chunk).ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
        let source = if class == ApplicationExportClassV1::Entity
            && !state.portability_entity_schedule.is_empty()
        {
            let entity_type = *state
                .portability_entity_schedule
                .get(usize::from(state.portability_entity_schedule_index))
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            snapshot
                .read_application_export_entity_page(entity_type, continuation.as_deref(), limit)
                .map_err(map_mutation_storage)?
        } else {
            snapshot
                .read_application_export_source_page(class, continuation.as_deref(), limit)
                .map_err(map_mutation_storage)?
        };
        source_rows = source_rows
            .checked_add(source.records().len())
            .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
        for record in source.records() {
            if record_visible(record, row_policy.as_ref(), snapshot.as_ref())? {
                if !record_workflow_quiescence(record, &mut state.workflow_quiescence)? {
                    terminalize(
                        &mut storage,
                        Some(&retained),
                        &mut state,
                        ApplicationExportFailureV1::WorkflowNotQuiescent,
                    )?;
                    remove_snapshot(snapshots, state.operation_id)?;
                    remove_replay(replays, state.operation_id)?;
                    return Err(ApplicationExportMutationPortErrorV1::WorkflowNotQuiescent);
                }
                lines.push(serialize_record(record, &bundle, field_visibility)?);
            }
        }
        class_complete = source.exact_end();
        continuation = source.continuation().map(<[u8]>::to_vec);
    }
    let released_bytes = lines
        .iter()
        .try_fold(0_u64, |total, line| {
            total.checked_add(u64::try_from(line.as_bytes().len() + 1).ok()?)
        })
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    let next_class = if class_complete
        && class == ApplicationExportClassV1::Entity
        && !state.portability_entity_schedule.is_empty()
        && usize::from(state.portability_entity_schedule_index) + 1
            < state.portability_entity_schedule.len()
    {
        state.portability_entity_schedule_index = state
            .portability_entity_schedule_index
            .checked_add(1)
            .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
        Some(ApplicationExportClassV1::Entity)
    } else if class_complete {
        next_selected_class(&state.selection, class)
    } else {
        Some(class)
    };
    state.current_class = next_class;
    state.continuation = if class_complete { None } else { continuation };
    state.phase = ApplicationExportPhaseV1::Exporting;
    state.pages_released = state
        .pages_released
        .checked_add(1)
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.rows_released = state
        .rows_released
        .checked_add(
            u64::try_from(lines.len())
                .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?,
        )
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.bytes_released = state
        .bytes_released
        .checked_add(released_bytes)
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    let class_index = usize::from(class.tag() - 1);
    state.class_pages[class_index] = state.class_pages[class_index]
        .checked_add(1)
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.class_rows[class_index] = state.class_rows[class_index]
        .checked_add(
            u64::try_from(lines.len())
                .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?,
        )
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.class_bytes[class_index] = state.class_bytes[class_index]
        .checked_add(released_bytes)
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    if state.page_hashes.len() >= MAX_RETAINED_PAGE_HASHES {
        terminalize(
            &mut storage,
            Some(&retained),
            &mut state,
            ApplicationExportFailureV1::LimitExceeded,
        )?;
        remove_snapshot(snapshots, state.operation_id)?;
        remove_replay(replays, state.operation_id)?;
        return Err(ApplicationExportMutationPortErrorV1::LimitExceeded);
    }

    let operation_complete = next_class.is_none();
    let page_number = NonZeroU64::new(state.pages_released)
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    let provisional = ApplicationExportPageV1::new(
        state.operation_id,
        page_number,
        class,
        lines.clone(),
        if operation_complete {
            None
        } else {
            Some(
                ApplicationExportCursor::new(vec![1])
                    .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?,
            )
        },
        class_complete,
        operation_complete,
    )
    .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.page_hashes.push(provisional.page_hash());
    if operation_complete {
        complete_state(&mut state)?;
    }
    let replacement = stored_state(&state)?;
    let next_cursor = if operation_complete {
        None
    } else {
        Some(cursor_for(&replacement)?)
    };
    let page = ApplicationExportPageV1::new(
        state.operation_id,
        page_number,
        class,
        lines,
        next_cursor,
        class_complete,
        operation_complete,
    )
    .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    if page.page_hash() != provisional.page_hash() {
        return Err(ApplicationExportMutationPortErrorV1::Integrity);
    }
    let replay = PageReplayEntry::new(request.cursor().clone(), &replacement, page.clone())?;
    install_replay(replays, state.operation_id, replay)?;
    match storage
        .compare_and_swap_application_export_operation(Some(&retained), &replacement)
        .map_err(map_mutation_storage)?
    {
        ApplicationExportOperationWriteResultV1::Applied
        | ApplicationExportOperationWriteResultV1::Unchanged => Ok(page),
        ApplicationExportOperationWriteResultV1::CompareMismatch => {
            remove_replay_if_state(replays, state.operation_id, replacement.canonical_state())?;
            Err(ApplicationExportMutationPortErrorV1::OutcomeUnknown)
        }
    }
}

fn observe(
    mut storage: SharedRedbOperationalPorts,
    snapshots: &SnapshotMap,
    replays: &PageReplayMap,
    clock: &ServerApplicationExportClock,
    request: AuthorizedApplicationExportOperationV1,
) -> Result<Option<ApplicationExportOperationV1>, ApplicationExportObservationPortErrorV1> {
    let (request, authorization) = request.into_parts();
    let Some(retained) = storage
        .read_application_export_operation(request.operation_id())
        .map_err(map_observation_storage)?
    else {
        return Ok(None);
    };
    let mut state =
        decode_state(&retained).map_err(|()| ApplicationExportObservationPortErrorV1::Integrity)?;
    validate_proof(&authorization, state.operation_id, &state.selection)
        .map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)?;
    if !state.phase.is_terminal() {
        let failure = if !same_authority(&state, &authorization) {
            Some(ApplicationExportFailureV1::AuthorityChanged)
        } else if clock
            .now()
            .map_err(|_| ApplicationExportObservationPortErrorV1::Unavailable)?
            >= state.lease_expires_at
        {
            Some(ApplicationExportFailureV1::LeaseExpired)
        } else {
            None
        };
        if let Some(failure) = failure {
            terminalize_observation(&mut storage, &retained, &mut state, failure)?;
            remove_snapshot_observation(snapshots, state.operation_id)?;
            remove_replay_observation(replays, state.operation_id)?;
        }
    } else if clock
        .now()
        .map_err(|_| ApplicationExportObservationPortErrorV1::Unavailable)?
        >= state.lease_expires_at
    {
        remove_snapshot_observation(snapshots, state.operation_id)?;
        remove_replay_observation(replays, state.operation_id)?;
    }
    operation(&state)
        .map(Some)
        .map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)
}

fn cancel(
    mut storage: SharedRedbOperationalPorts,
    snapshots: &SnapshotMap,
    replays: &PageReplayMap,
    request: AuthorizedApplicationExportOperationV1,
) -> Result<Option<ApplicationExportOperationV1>, ApplicationExportMutationPortErrorV1> {
    let (request, authorization) = request.into_parts();
    let Some(retained) = storage
        .read_application_export_operation(request.operation_id())
        .map_err(map_mutation_storage)?
    else {
        return Ok(None);
    };
    let mut state =
        decode_state(&retained).map_err(|()| ApplicationExportMutationPortErrorV1::Integrity)?;
    validate_proof(&authorization, state.operation_id, &state.selection)?;
    if !state.phase.is_terminal() {
        let reason = if same_authority(&state, &authorization) {
            ApplicationExportFailureV1::Cancelled
        } else {
            ApplicationExportFailureV1::AuthorityChanged
        };
        terminalize(&mut storage, Some(&retained), &mut state, reason)?;
        remove_snapshot(snapshots, state.operation_id)?;
        remove_replay(replays, state.operation_id)?;
    }
    operation(&state)
        .map(Some)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn validate_proof(
    authorization: &AuthorizedApplicationExportV1,
    operation_id: ApplicationExportOperationId,
    selection: &ApplicationExportSelectionV1,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    if authorization.request().operation_id() != operation_id
        || authorization.request().selection() != selection
    {
        return Err(ApplicationExportMutationPortErrorV1::InputMismatch);
    }
    Ok(())
}

fn same_authority(state: &ExportState, authorization: &AuthorizedApplicationExportV1) -> bool {
    state.authority == authorization.authority()
        && state.principal_id == *authorization.principal_id()
        && state.actor_kind == authorization.actor_kind()
}

fn first_selected_class(
    selection: &ApplicationExportSelectionV1,
) -> Option<ApplicationExportClassV1> {
    [
        ApplicationExportClassV1::Entity,
        ApplicationExportClassV1::Event,
        ApplicationExportClassV1::Provenance,
        ApplicationExportClassV1::PublicAudit,
    ]
    .into_iter()
    .find(|class| selection.includes(*class))
}

fn next_selected_class(
    selection: &ApplicationExportSelectionV1,
    current: ApplicationExportClassV1,
) -> Option<ApplicationExportClassV1> {
    [
        ApplicationExportClassV1::Entity,
        ApplicationExportClassV1::Event,
        ApplicationExportClassV1::Provenance,
        ApplicationExportClassV1::PublicAudit,
    ]
    .into_iter()
    .skip_while(|class| *class != current)
    .skip(1)
    .find(|class| selection.includes(*class))
}

fn portability_workflows(
    manifest: &ApplicationPortabilityManifest,
    bundle: &ValidatedContractBundle,
) -> Result<Vec<WorkflowQuiescenceWireV1>, ApplicationExportMutationPortErrorV1> {
    let mut workflows = BTreeMap::new();
    for mapping in &manifest.input().mappings {
        if mapping.class() != PortableRecordClass::Entity {
            continue;
        }
        let entity = bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == mapping.symbol().as_str())
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
        let Some(workflow) = bundle.bundle().workflows().for_entity(entity.id()) else {
            continue;
        };
        let Some(lease) = workflow.lease() else {
            continue;
        };
        let evidence = WorkflowQuiescenceWireV1 {
            workflow: workflow.name().to_owned(),
            entity_type_id: entity.id().get(),
            owner_field_id: lease.owner_field().get(),
            expiry_field_id: lease.expiry_field().get(),
            checked_rows: 0,
            quiescent_rows: 0,
            non_quiescent_rows: 0,
        };
        if workflows
            .insert(evidence.workflow.clone(), evidence)
            .is_some()
        {
            return Err(ApplicationExportMutationPortErrorV1::Integrity);
        }
    }
    Ok(workflows.into_values().collect())
}

fn record_workflow_quiescence(
    record: &ApplicationExportSourceRecordV1,
    evidence: &mut [WorkflowQuiescenceWireV1],
) -> Result<bool, ApplicationExportMutationPortErrorV1> {
    let ApplicationExportSourceRecordV1::Entity(record) = record else {
        return Ok(true);
    };
    let Some(evidence) = evidence
        .iter_mut()
        .find(|entry| entry.entity_type_id == record.target().entity_type_id().get())
    else {
        return Ok(true);
    };
    let owner_field = riffdb_types::FieldId::new(evidence.owner_field_id)
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    let expiry_field = riffdb_types::FieldId::new(evidence.expiry_field_id)
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    let field = |field_id| {
        record
            .fields()
            .fields()
            .binary_search_by_key(&field_id, |(id, _)| *id)
            .ok()
            .map(|index| &record.fields().fields()[index].1)
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)
    };
    record_workflow_lease_values(field(owner_field)?, field(expiry_field)?, evidence)
}

fn record_workflow_lease_values(
    owner: &CanonicalValue,
    expiry: &CanonicalValue,
    evidence: &mut WorkflowQuiescenceWireV1,
) -> Result<bool, ApplicationExportMutationPortErrorV1> {
    let quiescent = matches!(owner, CanonicalValue::Null) && matches!(expiry, CanonicalValue::Null);
    evidence.checked_rows = evidence
        .checked_rows
        .checked_add(1)
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    if quiescent {
        evidence.quiescent_rows = evidence
            .quiescent_rows
            .checked_add(1)
            .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    } else {
        evidence.non_quiescent_rows = evidence
            .non_quiescent_rows
            .checked_add(1)
            .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    }
    Ok(quiescent)
}

fn start_result(
    state: ExportState,
    disposition: ApplicationExportStartDispositionV1,
) -> Result<ApplicationExportStartResultV1, ApplicationExportMutationPortErrorV1> {
    let cursor = if state.phase.is_terminal() {
        None
    } else {
        Some(cursor_for(&stored_state(&state)?)?)
    };
    ApplicationExportStartResultV1::new(disposition, operation(&state)?, cursor)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn operation(
    state: &ExportState,
) -> Result<ApplicationExportOperationV1, ApplicationExportMutationPortErrorV1> {
    ApplicationExportOperationV1::new(
        state.operation_id,
        state.selection.clone(),
        state.snapshot.clone(),
        state.phase,
        state.lease_expires_at,
        state.pages_released,
        state.rows_released,
        state.bytes_released,
        state.failure,
        state
            .manifest
            .clone()
            .map(CanonicalApplicationExportJsonDocument::new)
            .transpose()
            .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?,
        state
            .receipt
            .clone()
            .map(CanonicalApplicationExportJsonDocument::new)
            .transpose()
            .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?,
    )
    .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn terminalize(
    storage: &mut SharedRedbOperationalPorts,
    expected: Option<&StoredApplicationExportOperationV1>,
    state: &mut ExportState,
    failure: ApplicationExportFailureV1,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    fail_state(state, failure)?;
    let replacement = stored_state(state)?;
    match storage
        .compare_and_swap_application_export_operation(expected, &replacement)
        .map_err(map_mutation_storage)?
    {
        ApplicationExportOperationWriteResultV1::Applied
        | ApplicationExportOperationWriteResultV1::Unchanged => Ok(()),
        ApplicationExportOperationWriteResultV1::CompareMismatch => {
            Err(ApplicationExportMutationPortErrorV1::OutcomeUnknown)
        }
    }
}

fn terminalize_observation(
    storage: &mut SharedRedbOperationalPorts,
    expected: &StoredApplicationExportOperationV1,
    state: &mut ExportState,
    failure: ApplicationExportFailureV1,
) -> Result<(), ApplicationExportObservationPortErrorV1> {
    fail_state(state, failure).map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)?;
    let replacement =
        stored_state(state).map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)?;
    match storage
        .compare_and_swap_application_export_operation(Some(expected), &replacement)
        .map_err(map_observation_storage)?
    {
        ApplicationExportOperationWriteResultV1::Applied
        | ApplicationExportOperationWriteResultV1::Unchanged => Ok(()),
        ApplicationExportOperationWriteResultV1::CompareMismatch => {
            Err(ApplicationExportObservationPortErrorV1::Unavailable)
        }
    }
}

fn fail_state(
    state: &mut ExportState,
    failure: ApplicationExportFailureV1,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    state.phase = match failure {
        ApplicationExportFailureV1::Cancelled => ApplicationExportPhaseV1::Cancelled,
        ApplicationExportFailureV1::LeaseExpired => ApplicationExportPhaseV1::Expired,
        _ => ApplicationExportPhaseV1::FailedClosed,
    };
    state.failure = Some(failure);
    state.current_class = None;
    state.continuation = None;
    install_terminal_documents(state)
}

fn complete_state(state: &mut ExportState) -> Result<(), ApplicationExportMutationPortErrorV1> {
    state.phase = ApplicationExportPhaseV1::Completed;
    state.failure = None;
    state.current_class = None;
    state.continuation = None;
    install_terminal_documents(state)
}

fn install_terminal_documents(
    state: &mut ExportState,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    let page_hashes = state
        .page_hashes
        .iter()
        .map(|value| lower_hex(value.as_bytes()))
        .collect::<Vec<_>>();
    let selected_classes = [
        ApplicationExportClassV1::Entity,
        ApplicationExportClassV1::Event,
        ApplicationExportClassV1::Provenance,
        ApplicationExportClassV1::PublicAudit,
    ]
    .into_iter()
    .filter(|class| state.selection.includes(*class))
    .map(export_class_name)
    .collect::<Vec<_>>();
    let class_totals = [
        ApplicationExportClassV1::Entity,
        ApplicationExportClassV1::Event,
        ApplicationExportClassV1::Provenance,
        ApplicationExportClassV1::PublicAudit,
    ]
    .into_iter()
    .enumerate()
    .filter(|(_, class)| state.selection.includes(*class))
    .map(|(index, class)| {
        (
            export_class_name(class).to_owned(),
            json!({
                "bytes": state.class_bytes[index].to_string(),
                "pages": state.class_pages[index].to_string(),
                "rows": state.class_rows[index].to_string(),
            }),
        )
    })
    .collect::<Map<String, Value>>();
    let query_modules = state
        .snapshot
        .query_modules()
        .iter()
        .map(|hash| lower_hex(hash.as_bytes()))
        .collect::<Vec<_>>();
    let reactive_modules = state
        .snapshot
        .reactive_modules()
        .iter()
        .map(|hash| lower_hex(hash.as_bytes()))
        .collect::<Vec<_>>();
    let row_policy = state.row_policy_role_hash.map(|role_hash| {
        json!({
            "application_role_hash": lower_hex(role_hash.as_bytes()),
            "policies": &state.row_policy_names,
        })
    });
    let omissions =
        if state.selection.scope() == CapabilityApplicationExportScopeV1::PrincipalFiltered {
            vec![
                "fields_hidden_by_current_role",
                "rows_denied_by_current_policy",
                "unanchored_or_deleted_policy_events",
                "provenance_without_complete_visible_current_rows",
            ]
        } else {
            Vec::new()
        };
    let manifest_schema = if state.portability_manifest_hash.is_some() {
        PORTABILITY_MANIFEST_SCHEMA
    } else {
        MANIFEST_SCHEMA
    };
    let receipt_schema = if state.portability_manifest_hash.is_some() {
        PORTABILITY_RECEIPT_SCHEMA
    } else {
        RECEIPT_SCHEMA
    };
    let workflow_quiescence = state
        .workflow_quiescence
        .iter()
        .map(|evidence| {
            json!({
                "checked_rows": evidence.checked_rows.to_string(),
                "non_quiescent_rows": evidence.non_quiescent_rows.to_string(),
                "quiescent_rows": evidence.quiescent_rows.to_string(),
                "workflow": evidence.workflow,
            })
        })
        .collect::<Vec<_>>();
    let mut manifest = json!({
        "administration_frontier": state.snapshot.administration_frontier().map(|value| value.get().to_string()),
        "application_frontier": state.snapshot.application_frontier().map(|value| value.get().to_string()),
        "bytes": state.bytes_released.to_string(),
        "class_totals": class_totals,
        "contract_bundle_hash": lower_hex(state.snapshot.contract_bundle_hash().as_bytes()),
        "contract_lineage": state.selection.lineage().as_str(),
        "contract_version": state.snapshot.contract_version().get().to_string(),
        "database_id": state.snapshot.database_id().to_string(),
        "history_incarnation": state.snapshot.history_incarnation().get().to_string(),
        "operation_id": state.operation_id.to_string(),
        "omissions": omissions,
        "page_hashes": page_hashes,
        "pages": state.pages_released.to_string(),
        "phase": phase_name(state.phase),
        "query_module_hashes": query_modules,
        "reactive_module_hashes": reactive_modules,
        "row_policy": row_policy,
        "rows": state.rows_released.to_string(),
        "schema": manifest_schema,
        "scope": export_scope_name(state.selection.scope()),
        "selected_classes": selected_classes,
    });
    if let Some(portability_manifest_hash) = state.portability_manifest_hash {
        let object = manifest
            .as_object_mut()
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
        object.insert(
            "portability_manifest_hash".to_owned(),
            Value::String(lower_hex(portability_manifest_hash.as_bytes())),
        );
        object.insert(
            "workflow_lease_quiescence".to_owned(),
            Value::Array(workflow_quiescence),
        );
    }
    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let manifest_document = CanonicalApplicationExportJsonDocument::new(manifest_bytes.clone())
        .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    let manifest_hash =
        riffdb_types::hash_application_export_manifest(manifest_document.as_bytes());
    let mut receipt = json!({
        "capability_id": state.authority.capability_id().to_string(),
        "capability_revision": state.authority.capability_revision().get().to_string(),
        "complete": state.phase == ApplicationExportPhaseV1::Completed,
        "failure": state.failure.map(failure_name),
        "manifest_hash": lower_hex(manifest_hash.as_bytes()),
        "operation_id": state.operation_id.to_string(),
        "phase": phase_name(state.phase),
        "principal_actor_kind": actor_kind_name(state.actor_kind),
        "principal_id": state.principal_id.as_str(),
        "schema": receipt_schema,
    });
    if let Some(portability_manifest_hash) = state.portability_manifest_hash {
        let object = receipt
            .as_object_mut()
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
        object.insert("portability_intent".to_owned(), Value::Bool(true));
        object.insert(
            "portability_manifest_hash".to_owned(),
            Value::String(lower_hex(portability_manifest_hash.as_bytes())),
        );
    }
    let receipt_bytes = serde_json::to_vec(&receipt)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    CanonicalApplicationExportJsonDocument::new(receipt_bytes.clone())
        .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.manifest = Some(manifest_bytes);
    state.receipt = Some(receipt_bytes);
    Ok(())
}

fn export_class_name(class: ApplicationExportClassV1) -> &'static str {
    match class {
        ApplicationExportClassV1::Entity => "entity",
        ApplicationExportClassV1::Event => "event",
        ApplicationExportClassV1::Provenance => "provenance",
        ApplicationExportClassV1::PublicAudit => "public_audit",
    }
}

fn export_scope_name(scope: CapabilityApplicationExportScopeV1) -> &'static str {
    match scope {
        CapabilityApplicationExportScopeV1::PrincipalFiltered => "principal_filtered",
        CapabilityApplicationExportScopeV1::WholeApplication => "whole_application",
    }
}

fn phase_name(phase: ApplicationExportPhaseV1) -> &'static str {
    match phase {
        ApplicationExportPhaseV1::Accepted => "accepted",
        ApplicationExportPhaseV1::Exporting => "exporting",
        ApplicationExportPhaseV1::Completed => "completed",
        ApplicationExportPhaseV1::Cancelled => "cancelled",
        ApplicationExportPhaseV1::Expired => "expired",
        ApplicationExportPhaseV1::FailedClosed => "failed_closed",
    }
}

fn actor_kind_name(actor: ActorKind) -> &'static str {
    match actor {
        ActorKind::Human => "human",
        ActorKind::Agent => "agent",
        ActorKind::Service => "service",
    }
}

fn cursor_for(
    record: &StoredApplicationExportOperationV1,
) -> Result<ApplicationExportCursor, ApplicationExportMutationPortErrorV1> {
    let mut preimage = Vec::with_capacity(16 + record.canonical_state().len());
    preimage.extend_from_slice(record.operation_id().as_bytes());
    preimage.extend_from_slice(record.canonical_state());
    let digest = hash(HashDomain::ApplicationExportCursor, &preimage);
    let mut bytes = Vec::with_capacity(1 + 16 + 32);
    bytes.push(CURSOR_VERSION);
    bytes.extend_from_slice(record.operation_id().as_bytes());
    bytes.extend_from_slice(digest.as_bytes());
    ApplicationExportCursor::new(bytes).map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn stored_state(
    state: &ExportState,
) -> Result<StoredApplicationExportOperationV1, ApplicationExportMutationPortErrorV1> {
    StoredApplicationExportOperationV1::new(
        state.operation_id,
        state.selection.lineage().clone(),
        encode_state(state)?,
    )
    .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)
}

fn encode_state(state: &ExportState) -> Result<Vec<u8>, ApplicationExportMutationPortErrorV1> {
    let wire = state_to_wire(state);
    serde_json::to_vec(&wire).map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn decode_state(record: &StoredApplicationExportOperationV1) -> Result<ExportState, ()> {
    let wire: ExportStateWireV1 =
        serde_json::from_slice(record.canonical_state()).map_err(|_| ())?;
    let state = wire_to_state(wire)?;
    if state.operation_id != record.operation_id()
        || state.selection.lineage() != record.lineage()
        || encode_state(&state).map_err(|_| ())? != record.canonical_state()
    {
        return Err(());
    }
    Ok(state)
}

fn state_to_wire(state: &ExportState) -> ExportStateWireV1 {
    ExportStateWireV1 {
        schema: if state.portability_manifest_hash.is_some() {
            PORTABILITY_STATE_SCHEMA.to_owned()
        } else {
            STATE_SCHEMA.to_owned()
        },
        operation_id: state.operation_id.into_bytes(),
        lineage: state.selection.lineage().as_str().to_owned(),
        scope: state.selection.scope().tag(),
        entities: state.selection.entities(),
        events: state.selection.events(),
        provenance: state.selection.provenance(),
        public_audit: state.selection.public_audit(),
        database_id: state.snapshot.database_id().into_bytes(),
        history_incarnation: state.snapshot.history_incarnation().get(),
        application_frontier: state
            .snapshot
            .application_frontier()
            .map(CommitSequence::get),
        administration_frontier: state
            .snapshot
            .administration_frontier()
            .map(AdministrationSequence::get),
        contract_version: state.snapshot.contract_version().get(),
        contract_bundle_hash: state.snapshot.contract_bundle_hash().into_bytes(),
        query_modules: state
            .snapshot
            .query_modules()
            .iter()
            .map(|value| value.into_bytes())
            .collect(),
        reactive_modules: state
            .snapshot
            .reactive_modules()
            .iter()
            .map(|value| value.into_bytes())
            .collect(),
        capability_id: state.authority.capability_id().into_bytes(),
        capability_revision: state.authority.capability_revision().get(),
        principal_id: state.principal_id.as_str().to_owned(),
        actor_kind: state.actor_kind.tag(),
        row_policy_role_hash: state
            .row_policy_role_hash
            .map(ApplicationRoleHash::into_bytes),
        row_policy_names: state.row_policy_names.clone(),
        lease_seconds: state.lease_expires_at.seconds(),
        lease_nanos: state.lease_expires_at.nanoseconds(),
        phase: phase_tag(state.phase),
        failure: state.failure.map(failure_tag),
        current_class: state.current_class.map(ApplicationExportClassV1::tag),
        continuation: state.continuation.clone(),
        pages_released: state.pages_released,
        rows_released: state.rows_released,
        bytes_released: state.bytes_released,
        class_pages: state.class_pages,
        class_rows: state.class_rows,
        class_bytes: state.class_bytes,
        page_hashes: state
            .page_hashes
            .iter()
            .map(|value| value.into_bytes())
            .collect(),
        manifest: state.manifest.clone(),
        receipt: state.receipt.clone(),
        portability_manifest_hash: state
            .portability_manifest_hash
            .map(ApplicationPortabilityManifestHash::into_bytes),
        workflow_quiescence: state.workflow_quiescence.clone(),
        portability_entity_schedule: state
            .portability_entity_schedule
            .iter()
            .map(|entity| entity.get())
            .collect(),
        portability_entity_schedule_index: state.portability_entity_schedule_index,
    }
}

fn wire_to_state(wire: ExportStateWireV1) -> Result<ExportState, ()> {
    let portability = match wire.schema.as_str() {
        STATE_SCHEMA => false,
        PORTABILITY_STATE_SCHEMA => true,
        _ => return Err(()),
    };
    if wire.page_hashes.len() > MAX_RETAINED_PAGE_HASHES
        || portability != wire.portability_manifest_hash.is_some()
        || (!portability && !wire.workflow_quiescence.is_empty())
        || wire
            .workflow_quiescence
            .windows(2)
            .any(|pair| pair[0].workflow >= pair[1].workflow)
        || wire.workflow_quiescence.iter().any(|evidence| {
            let Some(classified_rows) = evidence
                .quiescent_rows
                .checked_add(evidence.non_quiescent_rows)
            else {
                return true;
            };
            evidence.checked_rows != classified_rows || evidence.non_quiescent_rows > 1
        })
        || wire.portability_entity_schedule.len()
            > riffdb_application::MAX_APPLICATION_PORTABLE_MAPPINGS
        || wire
            .portability_entity_schedule
            .iter()
            .any(|entity| riffdb_types::EntityTypeId::new(*entity).is_none())
        || (!wire.portability_entity_schedule.is_empty()
            && usize::from(wire.portability_entity_schedule_index)
                >= wire.portability_entity_schedule.len())
        || (!portability
            && (!wire.portability_entity_schedule.is_empty()
                || wire.portability_entity_schedule_index != 0))
    {
        return Err(());
    }
    let total_pages = wire
        .class_pages
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(*value))
        .ok_or(())?;
    let total_rows = wire
        .class_rows
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(*value))
        .ok_or(())?;
    let total_bytes = wire
        .class_bytes
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(*value))
        .ok_or(())?;
    if total_pages != wire.pages_released
        || total_rows != wire.rows_released
        || total_bytes != wire.bytes_released
        || usize::try_from(total_pages).map_err(|_| ())? != wire.page_hashes.len()
    {
        return Err(());
    }
    let operation_id =
        ApplicationExportOperationId::from_bytes(wire.operation_id).map_err(|_| ())?;
    let selection = ApplicationExportSelectionV1::new(
        ContractLineage::new(wire.lineage).map_err(|_| ())?,
        CapabilityApplicationExportScopeV1::from_tag(wire.scope).ok_or(())?,
        wire.entities,
        wire.events,
        wire.provenance,
        wire.public_audit,
    )
    .map_err(|_| ())?;
    let snapshot = ApplicationExportSnapshotBindingV1::new(
        DatabaseId::from_bytes(wire.database_id).map_err(|_| ())?,
        NonZeroU64::new(wire.history_incarnation).ok_or(())?,
        match wire.application_frontier {
            Some(value) => Some(CommitSequence::new(value).ok_or(())?),
            None => None,
        },
        match wire.administration_frontier {
            Some(value) => Some(AdministrationSequence::new(value).ok_or(())?),
            None => None,
        },
        ContractVersion::new(wire.contract_version).ok_or(())?,
        ContractBundleHash::from_bytes(wire.contract_bundle_hash),
        wire.query_modules
            .into_iter()
            .map(QueryModuleHash::from_bytes)
            .collect(),
        wire.reactive_modules
            .into_iter()
            .map(ReactiveModuleHash::from_bytes)
            .collect(),
    )
    .map_err(|_| ())?;
    let principal_scope =
        selection.scope() == CapabilityApplicationExportScopeV1::PrincipalFiltered;
    if principal_scope != wire.row_policy_role_hash.is_some()
        || principal_scope != !wire.row_policy_names.is_empty()
        || wire
            .row_policy_names
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(());
    }
    let state = ExportState {
        operation_id,
        selection,
        snapshot,
        authority: ApplicationExportAuthorityV1::new(
            CapabilityId::from_bytes(wire.capability_id).map_err(|_| ())?,
            NonZeroU64::new(wire.capability_revision).ok_or(())?,
        ),
        principal_id: ActorId::new(wire.principal_id).map_err(|_| ())?,
        actor_kind: ActorKind::from_tag(wire.actor_kind).ok_or(())?,
        row_policy_role_hash: wire
            .row_policy_role_hash
            .map(ApplicationRoleHash::from_bytes),
        row_policy_names: wire.row_policy_names,
        lease_expires_at: Timestamp::new(wire.lease_seconds, wire.lease_nanos).map_err(|_| ())?,
        phase: phase_from_tag(wire.phase).ok_or(())?,
        failure: wire.failure.map(failure_from_tag).transpose()?.flatten(),
        current_class: match wire.current_class {
            Some(value) => Some(ApplicationExportClassV1::from_tag(value).ok_or(())?),
            None => None,
        },
        continuation: wire.continuation,
        pages_released: wire.pages_released,
        rows_released: wire.rows_released,
        bytes_released: wire.bytes_released,
        class_pages: wire.class_pages,
        class_rows: wire.class_rows,
        class_bytes: wire.class_bytes,
        page_hashes: wire
            .page_hashes
            .into_iter()
            .map(ApplicationExportPageHash::from_bytes)
            .collect(),
        manifest: wire.manifest,
        receipt: wire.receipt,
        portability_manifest_hash: wire
            .portability_manifest_hash
            .map(ApplicationPortabilityManifestHash::from_bytes),
        workflow_quiescence: wire.workflow_quiescence,
        portability_entity_schedule: wire
            .portability_entity_schedule
            .into_iter()
            .map(|entity| riffdb_types::EntityTypeId::new(entity).ok_or(()))
            .collect::<Result<Vec<_>, _>>()?,
        portability_entity_schedule_index: wire.portability_entity_schedule_index,
    };
    operation(&state).map_err(|_| ())?;
    Ok(state)
}

const fn is_zero_u16(value: &u16) -> bool {
    *value == 0
}

fn phase_tag(value: ApplicationExportPhaseV1) -> u8 {
    match value {
        ApplicationExportPhaseV1::Accepted => 1,
        ApplicationExportPhaseV1::Exporting => 2,
        ApplicationExportPhaseV1::Completed => 3,
        ApplicationExportPhaseV1::Cancelled => 4,
        ApplicationExportPhaseV1::Expired => 5,
        ApplicationExportPhaseV1::FailedClosed => 6,
    }
}
fn phase_from_tag(value: u8) -> Option<ApplicationExportPhaseV1> {
    match value {
        1 => Some(ApplicationExportPhaseV1::Accepted),
        2 => Some(ApplicationExportPhaseV1::Exporting),
        3 => Some(ApplicationExportPhaseV1::Completed),
        4 => Some(ApplicationExportPhaseV1::Cancelled),
        5 => Some(ApplicationExportPhaseV1::Expired),
        6 => Some(ApplicationExportPhaseV1::FailedClosed),
        _ => None,
    }
}
fn failure_tag(value: ApplicationExportFailureV1) -> u8 {
    match value {
        ApplicationExportFailureV1::AuthorityChanged => 1,
        ApplicationExportFailureV1::SnapshotUnavailable => 2,
        ApplicationExportFailureV1::SourceInvalid => 3,
        ApplicationExportFailureV1::LeaseExpired => 4,
        ApplicationExportFailureV1::Cancelled => 5,
        ApplicationExportFailureV1::LimitExceeded => 6,
        ApplicationExportFailureV1::Internal => 7,
        ApplicationExportFailureV1::WorkflowNotQuiescent => 8,
    }
}
fn failure_from_tag(value: u8) -> Result<Option<ApplicationExportFailureV1>, ()> {
    Ok(Some(match value {
        1 => ApplicationExportFailureV1::AuthorityChanged,
        2 => ApplicationExportFailureV1::SnapshotUnavailable,
        3 => ApplicationExportFailureV1::SourceInvalid,
        4 => ApplicationExportFailureV1::LeaseExpired,
        5 => ApplicationExportFailureV1::Cancelled,
        6 => ApplicationExportFailureV1::LimitExceeded,
        7 => ApplicationExportFailureV1::Internal,
        8 => ApplicationExportFailureV1::WorkflowNotQuiescent,
        _ => return Err(()),
    }))
}
fn failure_name(value: ApplicationExportFailureV1) -> &'static str {
    match value {
        ApplicationExportFailureV1::AuthorityChanged => "authority_changed",
        ApplicationExportFailureV1::SnapshotUnavailable => "snapshot_unavailable",
        ApplicationExportFailureV1::SourceInvalid => "source_invalid",
        ApplicationExportFailureV1::LeaseExpired => "lease_expired",
        ApplicationExportFailureV1::Cancelled => "cancelled",
        ApplicationExportFailureV1::LimitExceeded => "limit_exceeded",
        ApplicationExportFailureV1::Internal => "internal",
        ApplicationExportFailureV1::WorkflowNotQuiescent => "workflow_not_quiescent",
    }
}

fn add_seconds(value: Timestamp, seconds: u64) -> Option<Timestamp> {
    let seconds = i64::try_from(seconds).ok()?;
    Timestamp::new(value.seconds().checked_add(seconds)?, value.nanoseconds()).ok()
}

fn remove_snapshot(
    snapshots: &SnapshotMap,
    operation_id: ApplicationExportOperationId,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    snapshots
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?
        .remove(&operation_id);
    Ok(())
}

fn insert_snapshot_candidate(
    snapshots: &SnapshotMap,
    operation_id: ApplicationExportOperationId,
    snapshot: Arc<dyn ApplicationExportSnapshotReader>,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    let mut snapshots = snapshots
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    if snapshots.contains_key(&operation_id) {
        return Err(ApplicationExportMutationPortErrorV1::OutcomeUnknown);
    }
    if snapshots.len() >= MAX_ACTIVE_APPLICATION_EXPORTS {
        return Err(ApplicationExportMutationPortErrorV1::LimitExceeded);
    }
    snapshots.insert(operation_id, snapshot);
    Ok(())
}

fn remove_snapshot_observation(
    snapshots: &SnapshotMap,
    operation_id: ApplicationExportOperationId,
) -> Result<(), ApplicationExportObservationPortErrorV1> {
    snapshots
        .lock()
        .map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)?
        .remove(&operation_id);
    Ok(())
}

impl PageReplayEntry {
    fn new(
        request_cursor: ApplicationExportCursor,
        successor: &StoredApplicationExportOperationV1,
        page: ApplicationExportPageV1,
    ) -> Result<Self, ApplicationExportMutationPortErrorV1> {
        if successor.operation_id() != page.operation_id() {
            return Err(ApplicationExportMutationPortErrorV1::Integrity);
        }
        let line_bytes = page.lines().iter().try_fold(0usize, |total, line| {
            total.checked_add(line.as_bytes().len().checked_add(1)?)
        });
        let retained_bytes = APPLICATION_EXPORT_REPLAY_FIXED_CHARGE
            .checked_add(request_cursor.as_bytes().len())
            .and_then(|total| total.checked_add(successor.canonical_state().len()))
            .and_then(|total| total.checked_add(line_bytes?))
            .and_then(|total| {
                total.checked_add(
                    page.next_cursor()
                        .map_or(0, |cursor| cursor.as_bytes().len()),
                )
            })
            .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
        Ok(Self {
            request_cursor,
            successor_state: successor.canonical_state().to_vec(),
            page,
            retained_bytes,
        })
    }
}

fn install_replay(
    replays: &PageReplayMap,
    operation_id: ApplicationExportOperationId,
    entry: PageReplayEntry,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    let mut cache = replays
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let replaced_bytes = cache
        .entries
        .get(&operation_id)
        .map_or(0, |retained| retained.retained_bytes);
    let retained_bytes = cache
        .retained_bytes
        .checked_sub(replaced_bytes)
        .and_then(|total| total.checked_add(entry.retained_bytes))
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    if retained_bytes > MAX_APPLICATION_EXPORT_REPLAY_BYTES {
        return Err(ApplicationExportMutationPortErrorV1::LimitExceeded);
    }
    cache.entries.insert(operation_id, entry);
    cache.retained_bytes = retained_bytes;
    Ok(())
}

fn replayed_page(
    replays: &PageReplayMap,
    operation_id: ApplicationExportOperationId,
    request_cursor: &ApplicationExportCursor,
    retained_state: &[u8],
) -> Result<Option<ApplicationExportPageV1>, ApplicationExportMutationPortErrorV1> {
    let cache = replays
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    Ok(cache.entries.get(&operation_id).and_then(|entry| {
        (entry.request_cursor == *request_cursor && entry.successor_state == retained_state)
            .then(|| entry.page.clone())
    }))
}

fn remove_replay(
    replays: &PageReplayMap,
    operation_id: ApplicationExportOperationId,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    let mut cache = replays
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    if let Some(entry) = cache.entries.remove(&operation_id) {
        cache.retained_bytes = cache
            .retained_bytes
            .checked_sub(entry.retained_bytes)
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    }
    Ok(())
}

fn remove_replay_if_state(
    replays: &PageReplayMap,
    operation_id: ApplicationExportOperationId,
    successor_state: &[u8],
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    let mut cache = replays
        .lock()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let should_remove = cache
        .entries
        .get(&operation_id)
        .is_some_and(|entry| entry.successor_state == successor_state);
    if should_remove {
        let entry = cache
            .entries
            .remove(&operation_id)
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
        cache.retained_bytes = cache
            .retained_bytes
            .checked_sub(entry.retained_bytes)
            .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    }
    Ok(())
}

fn remove_replay_observation(
    replays: &PageReplayMap,
    operation_id: ApplicationExportOperationId,
) -> Result<(), ApplicationExportObservationPortErrorV1> {
    let mut cache = replays
        .lock()
        .map_err(|_| ApplicationExportObservationPortErrorV1::Integrity)?;
    if let Some(entry) = cache.entries.remove(&operation_id) {
        cache.retained_bytes = cache
            .retained_bytes
            .checked_sub(entry.retained_bytes)
            .ok_or(ApplicationExportObservationPortErrorV1::Integrity)?;
    }
    Ok(())
}

fn serialize_record(
    record: &ApplicationExportSourceRecordV1,
    bundle: &ValidatedContractBundle,
    field_visibility: Option<&[riffdb_types::EntityFieldVisibilityV1]>,
) -> Result<CanonicalApplicationExportJsonLine, ApplicationExportMutationPortErrorV1> {
    let value = match record {
        ApplicationExportSourceRecordV1::Entity(record) => {
            let entity = bundle
                .bundle()
                .schema()
                .entity(record.target().entity_type_id())
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            let visible = field_visibility.map(|entries| {
                entries
                    .iter()
                    .find(|entry| entry.entity_type() == entity.id())
                    .map_or(&[][..], riffdb_types::EntityFieldVisibilityV1::fields)
            });
            let fields = symbolic_entity_record(record.fields(), entity, bundle, visible)?;
            let key = entity
                .primary_key_fields()
                .iter()
                .map(|field_id| {
                    let field = entity
                        .record()
                        .field(*field_id)
                        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
                    let value = record
                        .fields()
                        .fields()
                        .binary_search_by_key(field_id, |(id, _)| *id)
                        .ok()
                        .map(|index| &record.fields().fields()[index].1)
                        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
                    Ok((
                        field.name().to_owned(),
                        natural_value(value, field.value_type(), bundle)?,
                    ))
                })
                .collect::<Result<Map<String, Value>, ApplicationExportMutationPortErrorV1>>()?;
            json!({"class":"entity","entity":entity.name(),"fields":fields,"key":key,"version":record.entity_version().get().to_string(),"written_by_contract_version":record.written_by_contract().get().to_string()})
        }
        ApplicationExportSourceRecordV1::Event(event) => {
            let (event_id, event_type_id, payload) = match event.as_ref() {
                ApplicationExportEventRecordV1::V1(record) => {
                    (record.event_id(), record.event_type_id(), record.payload())
                }
                ApplicationExportEventRecordV1::V2(record) => {
                    (record.event_id(), record.event_type_id(), record.payload())
                }
            };
            let event_schema = bundle
                .bundle()
                .schema()
                .event(event_type_id)
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            json!({"class":"event","commit_sequence":event_id.commit_sequence().get().to_string(),"event":event_schema.name(),"event_id":format!("{}:{}", event_id.commit_sequence().get(), event_id.event_ordinal()),"event_ordinal":event_id.event_ordinal(),"payload":symbolic_record(payload, event_schema.payload(), bundle)?})
        }
        ApplicationExportSourceRecordV1::Provenance(record) => {
            let command = bundle
                .bundle()
                .command(record.plan().command_id())
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            let outcome = command
                .outcomes()
                .iter()
                .find(|outcome| outcome.id() == record.outcome_id())
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            json!({"class":"provenance","command":command.name(),"commit_sequence":record.commit_sequence().get().to_string(),"logical_time":{"nanos":record.logical_time().timestamp().nanoseconds(),"seconds":record.logical_time().timestamp().seconds().to_string()},"outcome":outcome.name(),"provenance_id":record.provenance_id().to_string()})
        }
        ApplicationExportSourceRecordV1::PublicAudit(record) => {
            let kind = match record.as_ref() {
                riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(_) => "contract",
                riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(_) => {
                    "query_module"
                }
                riffdb_storage_api::StoredAdministrationAuditRecordV1::ReactiveModule(_) => {
                    "reactive_module"
                }
                riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(_) => {
                    "capability"
                }
                riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(_) => "service",
                riffdb_storage_api::StoredAdministrationAuditRecordV1::Retention(_) => "retention",
            };
            json!({"administration_sequence":record.administration_sequence().get().to_string(),"class":"public_audit","kind":kind})
        }
    };
    let bytes =
        serde_json::to_vec(&value).map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    CanonicalApplicationExportJsonLine::new(bytes)
        .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)
}

fn record_visible(
    record: &ApplicationExportSourceRecordV1,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    snapshot: &dyn ApplicationExportSnapshotReader,
) -> Result<bool, ApplicationExportMutationPortErrorV1> {
    let Some(policy) = policy else {
        return Ok(true);
    };
    match record {
        ApplicationExportSourceRecordV1::Entity(record) => policy_allows_row(
            policy,
            record.target().entity_type_id(),
            record.fields(),
            snapshot,
        ),
        ApplicationExportSourceRecordV1::Event(event) => {
            let ApplicationExportEventRecordV1::V2(event) = event.as_ref() else {
                // Frozen V1 events have no policy anchor and are therefore
                // indistinguishable absence under principal-filtered export.
                return Ok(false);
            };
            let Some(row) = snapshot
                .read_application_export_policy_anchor(event.policy_anchor().source())
                .map_err(map_mutation_storage)?
            else {
                return Ok(false);
            };
            let lookups = policy
                .relationship_lookups(row.target().entity_type_id(), row.fields())
                .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
            let evidence = relationship_evidence(snapshot, &lookups)?;
            let candidate = EventPolicyCandidateV1::new(
                event.event_id(),
                event.policy_anchor().source().key().clone(),
                event.policy_anchor().read_policy().clone(),
            );
            Ok(policy
                .authorize_event_release(&candidate, row.fields(), &evidence)
                .is_allowed())
        }
        ApplicationExportSourceRecordV1::Provenance(provenance) => {
            if provenance.affected_entities().is_empty() {
                return Ok(false);
            }
            for affected in provenance.affected_entities() {
                let Some(row) = snapshot
                    .read_application_export_policy_anchor(affected.target())
                    .map_err(map_mutation_storage)?
                else {
                    return Ok(false);
                };
                if !policy_allows_row(
                    policy,
                    row.target().entity_type_id(),
                    row.fields(),
                    snapshot,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        // This is already the storage-owned public-safe administration
        // projection. Its independent V5 bit is the authority boundary; it
        // carries no application row values or hidden-row counts.
        ApplicationExportSourceRecordV1::PublicAudit(_) => Ok(true),
    }
}

fn policy_allows_row(
    policy: &AuthorizedQueryRowPolicyContextV1,
    entity: riffdb_types::EntityTypeId,
    row: &riffdb_types::CanonicalRecord,
    snapshot: &dyn ApplicationExportSnapshotReader,
) -> Result<bool, ApplicationExportMutationPortErrorV1> {
    let lookups = policy
        .relationship_lookups(entity, row)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let evidence = relationship_evidence(snapshot, &lookups)?;
    Ok(policy.allows(entity, row, &evidence))
}

fn relationship_evidence(
    snapshot: &dyn ApplicationExportSnapshotReader,
    lookups: &[riffdb_policy::AuthorizedIndexedRelationshipLookupV1],
) -> Result<Vec<bool>, ApplicationExportMutationPortErrorV1> {
    lookups
        .iter()
        .map(|lookup| {
            snapshot
                .application_export_indexed_relationship_exists(
                    lookup.index_prefix(),
                    lookup.partition(),
                )
                .map_err(map_mutation_storage)
        })
        .collect()
}

fn symbolic_entity_record(
    record: &riffdb_types::CanonicalRecord,
    entity: &riffdb_contract_ir::EntitySchema,
    bundle: &ValidatedContractBundle,
    visible_non_key_fields: Option<&[riffdb_types::FieldId]>,
) -> Result<Map<String, Value>, ApplicationExportMutationPortErrorV1> {
    record
        .fields()
        .iter()
        .filter(|(field_id, _)| {
            visible_non_key_fields.is_none_or(|visible| {
                entity.primary_key_fields().contains(field_id)
                    || visible.binary_search(field_id).is_ok()
            })
        })
        .map(|(field_id, value)| {
            let field = entity
                .record()
                .field(*field_id)
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            Ok((
                field.name().to_owned(),
                natural_value(value, field.value_type(), bundle)?,
            ))
        })
        .collect()
}

fn symbolic_record(
    record: &riffdb_types::CanonicalRecord,
    schema: &riffdb_contract_ir::RecordSchema,
    bundle: &ValidatedContractBundle,
) -> Result<Map<String, Value>, ApplicationExportMutationPortErrorV1> {
    record
        .fields()
        .iter()
        .map(|(field_id, value)| {
            let field = schema
                .field(*field_id)
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            Ok((
                field.name().to_owned(),
                natural_value(value, field.value_type(), bundle)?,
            ))
        })
        .collect()
}

fn natural_value(
    value: &CanonicalValue,
    value_type: &ValueType,
    bundle: &ValidatedContractBundle,
) -> Result<Value, ApplicationExportMutationPortErrorV1> {
    value_type
        .validate_value(value)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let concrete_type = value_type.optional_inner().unwrap_or(value_type);
    Ok(match value {
        CanonicalValue::Null => Value::Null,
        CanonicalValue::Bool(value) => Value::Bool(*value),
        CanonicalValue::I64(value) => Value::String(value.to_string()),
        CanonicalValue::U64(value) => Value::String(value.to_string()),
        CanonicalValue::Decimal(value) => {
            json!({"coefficient":value.coefficient().to_string(),"precision":value.spec().precision(),"scale":value.spec().scale()})
        }
        CanonicalValue::Money(value) => {
            json!({"coefficient":value.amount().coefficient().to_string(),"currency":value.currency().to_string(),"precision":value.amount().spec().precision(),"scale":value.amount().spec().scale()})
        }
        CanonicalValue::String(value) => Value::String(value.as_str().to_owned()),
        CanonicalValue::Bytes(value) => Value::String(base64_standard(value.as_bytes())),
        CanonicalValue::Timestamp(value) => {
            json!({"nanos":value.nanoseconds(),"seconds":value.seconds().to_string()})
        }
        CanonicalValue::Date(value) => {
            json!({"days_since_unix_epoch":value.days_since_unix_epoch()})
        }
        CanonicalValue::Uuid(value) => Value::String(format_uuid(*value)),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => Value::String(
            bundle
                .enum_variant_names()
                .get(&(type_id.get(), variant_id.get()))
                .cloned()
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?,
        ),
        CanonicalValue::List(values) => Value::Array(
            values
                .values()
                .iter()
                .map(|value| {
                    concrete_type
                        .list_parts()
                        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)
                        .and_then(|(element, _)| natural_value(value, element, bundle))
                })
                .collect::<Result<_, _>>()?,
        ),
        CanonicalValue::Record(record) => {
            let owner = concrete_type
                .record_ref()
                .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
            Value::Object(symbolic_record(
                record,
                resolve_record_schema(owner, bundle)?,
                bundle,
            )?)
        }
        CanonicalValue::Vector(values) => Value::Array(
            values
                .components()
                .iter()
                .map(|value| {
                    serde_json::Number::from_f64(f64::from(*value))
                        .map(Value::Number)
                        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)
                })
                .collect::<Result<_, _>>()?,
        ),
    })
}

fn resolve_record_schema<'a>(
    owner: &RecordTypeRef,
    bundle: &'a ValidatedContractBundle,
) -> Result<&'a RecordSchema, ApplicationExportMutationPortErrorV1> {
    let bundle = bundle.bundle();
    match owner {
        RecordTypeRef::Entity(entity) => bundle
            .schema()
            .entity(*entity)
            .map(riffdb_contract_ir::EntitySchema::record),
        RecordTypeRef::Event(event) => bundle
            .schema()
            .event(*event)
            .map(riffdb_contract_ir::EventSchema::payload),
        RecordTypeRef::CommandInput(command) => {
            bundle.command(*command).map(|plan| plan.input().record())
        }
        RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        } => bundle.command(*command_id).and_then(|plan| {
            plan.outcomes()
                .iter()
                .find(|outcome| outcome.id() == *outcome_id)
                .map(riffdb_contract_ir::OutcomeSchema::payload)
        }),
        RecordTypeRef::ProjectionResult(projection) => bundle
            .projection(*projection)
            .map(|plan| plan.group_schema().measures()),
    }
    .ok_or(ApplicationExportMutationPortErrorV1::Integrity)
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let hex = lower_hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(ALPHABET[usize::from(first >> 2)]));
        output.push(char::from(
            ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))],
        ));
        if chunk.len() > 1 {
            output.push(char::from(
                ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))],
            ));
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(char::from(ALPHABET[usize::from(third & 0x3f)]));
        } else {
            output.push('=');
        }
    }
    output
}

fn map_mutation_storage(error: StorageError) -> ApplicationExportMutationPortErrorV1 {
    match error.kind() {
        StorageErrorKind::LimitExceeded => ApplicationExportMutationPortErrorV1::LimitExceeded,
        StorageErrorKind::InvariantViolation | StorageErrorKind::CorruptData => {
            ApplicationExportMutationPortErrorV1::Integrity
        }
        _ => ApplicationExportMutationPortErrorV1::Unavailable,
    }
}

fn map_observation_storage(error: StorageError) -> ApplicationExportObservationPortErrorV1 {
    match error.kind() {
        StorageErrorKind::InvariantViolation | StorageErrorKind::CorruptData => {
            ApplicationExportObservationPortErrorV1::Integrity
        }
        _ => ApplicationExportObservationPortErrorV1::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ExportState {
        let operation_id =
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [1; 10])
                .expect("operation id");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(2, [2; 10]).expect("database id");
        let capability_id =
            CapabilityId::from_unix_milliseconds_and_random(3, [3; 10]).expect("capability id");
        let selection = ApplicationExportSelectionV1::new(
            ContractLineage::new("TicketDesk").expect("lineage"),
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            true,
            true,
            true,
        )
        .expect("selection");
        ExportState {
            operation_id,
            selection,
            snapshot: ApplicationExportSnapshotBindingV1::new(
                database_id,
                NonZeroU64::new(7).expect("incarnation"),
                Some(CommitSequence::new(11).expect("frontier")),
                Some(AdministrationSequence::new(13).expect("frontier")),
                ContractVersion::new(2).expect("version"),
                ContractBundleHash::from_bytes([4; 32]),
                vec![QueryModuleHash::from_bytes([5; 32])],
                vec![ReactiveModuleHash::from_bytes([6; 32])],
            )
            .expect("snapshot"),
            authority: ApplicationExportAuthorityV1::new(
                capability_id,
                NonZeroU64::new(9).expect("revision"),
            ),
            principal_id: ActorId::new("operator").expect("principal"),
            actor_kind: ActorKind::Human,
            row_policy_role_hash: None,
            row_policy_names: Vec::new(),
            lease_expires_at: Timestamp::new(1_700_000_000, 42).expect("time"),
            phase: ApplicationExportPhaseV1::Accepted,
            failure: None,
            current_class: Some(ApplicationExportClassV1::Entity),
            continuation: None,
            pages_released: 0,
            rows_released: 0,
            bytes_released: 0,
            class_pages: [0; 4],
            class_rows: [0; 4],
            class_bytes: [0; 4],
            page_hashes: Vec::new(),
            manifest: None,
            receipt: None,
            portability_manifest_hash: None,
            workflow_quiescence: Vec::new(),
            portability_entity_schedule: Vec::new(),
            portability_entity_schedule_index: 0,
        }
    }

    #[test]
    fn durable_state_is_canonical_identity_bound_and_strict() {
        let state = state();
        let stored = stored_state(&state).expect("stored");
        let decoded = decode_state(&stored).expect("decoded");
        assert_eq!(decoded.operation_id, state.operation_id);
        assert_eq!(decoded.selection, state.selection);
        assert_eq!(decoded.snapshot, state.snapshot);
        assert_eq!(decoded.authority, state.authority);
        assert_eq!(
            encode_state(&decoded).expect("encode"),
            stored.canonical_state()
        );

        let mut noncanonical = stored.canonical_state().to_vec();
        noncanonical.push(b' ');
        let noncanonical = StoredApplicationExportOperationV1::new(
            state.operation_id,
            state.selection.lineage().clone(),
            noncanonical,
        )
        .expect("bounded state");
        assert!(decode_state(&noncanonical).is_err());

        let wrong_lineage = StoredApplicationExportOperationV1::new(
            state.operation_id,
            ContractLineage::new("Other").expect("lineage"),
            stored.canonical_state().to_vec(),
        )
        .expect("bounded state");
        assert!(decode_state(&wrong_lineage).is_err());
        assert!(
            !stored
                .canonical_state()
                .windows("portability_manifest_hash".len())
                .any(|window| window == b"portability_manifest_hash")
        );
    }

    #[test]
    fn portability_state_binds_manifest_and_canonical_workflow_evidence() {
        let mut portable = state();
        portable.portability_manifest_hash =
            Some(ApplicationPortabilityManifestHash::from_bytes([0x55; 32]));
        portable.workflow_quiescence = vec![WorkflowQuiescenceWireV1 {
            workflow: "WorkLifecycle".to_owned(),
            entity_type_id: 7,
            owner_field_id: 11,
            expiry_field_id: 12,
            checked_rows: 2,
            quiescent_rows: 2,
            non_quiescent_rows: 0,
        }];
        let stored = stored_state(&portable).expect("portable state");
        let decoded = decode_state(&stored).expect("portable state decodes");
        assert_eq!(
            decoded.portability_manifest_hash,
            portable.portability_manifest_hash
        );
        assert_eq!(decoded.workflow_quiescence, portable.workflow_quiescence);
        assert!(
            std::str::from_utf8(stored.canonical_state())
                .expect("json")
                .contains(PORTABILITY_STATE_SCHEMA)
        );
    }

    #[test]
    fn workflow_quiescence_requires_both_owner_and_expiry_to_be_null() {
        let mut evidence = WorkflowQuiescenceWireV1 {
            workflow: "WorkLifecycle".to_owned(),
            entity_type_id: 7,
            owner_field_id: 11,
            expiry_field_id: 12,
            checked_rows: 0,
            quiescent_rows: 0,
            non_quiescent_rows: 0,
        };
        assert!(
            record_workflow_lease_values(
                &CanonicalValue::Null,
                &CanonicalValue::Null,
                &mut evidence,
            )
            .expect("quiescent")
        );
        assert!(
            !record_workflow_lease_values(
                &CanonicalValue::Uuid([1; 16]),
                &CanonicalValue::Null,
                &mut evidence,
            )
            .expect("partial lease")
        );
        assert_eq!(evidence.checked_rows, 2);
        assert_eq!(evidence.quiescent_rows, 1);
        assert_eq!(evidence.non_quiescent_rows, 1);
    }

    #[test]
    fn cursor_binds_the_complete_durable_checkpoint_without_exposing_progress() {
        let mut state = state();
        let first = cursor_for(&stored_state(&state).expect("stored")).expect("cursor");
        assert_eq!(first.as_bytes().len(), 49);
        assert_eq!(first.as_bytes()[0], CURSOR_VERSION);

        state.continuation = Some(vec![0xde, 0xad]);
        state.pages_released = 1;
        let second = cursor_for(&stored_state(&state).expect("stored")).expect("cursor");
        assert_ne!(first, second);
        assert!(
            !second
                .as_bytes()
                .windows(2)
                .any(|window| window == [0xde, 0xad])
        );
    }

    #[test]
    fn terminal_documents_bind_authority_manifest_and_completion_class() {
        let mut completed = state();
        completed.pages_released = 1;
        completed.rows_released = 2;
        completed.bytes_released = 100;
        completed.class_pages[0] = 1;
        completed.class_rows[0] = 2;
        completed.class_bytes[0] = 100;
        completed
            .page_hashes
            .push(ApplicationExportPageHash::from_bytes([8; 32]));
        complete_state(&mut completed).expect("complete");
        let observed = operation(&completed).expect("operation");
        assert_eq!(observed.phase(), ApplicationExportPhaseV1::Completed);
        assert!(observed.failure().is_none());
        assert!(observed.manifest_hash().is_some());
        assert!(observed.receipt_hash().is_some());

        let receipt: Value =
            serde_json::from_slice(observed.receipt().expect("receipt").as_bytes())
                .expect("receipt json");
        assert_eq!(receipt["complete"], true);
        assert_eq!(receipt["principal_id"], "operator");
        let manifest: Value =
            serde_json::from_slice(observed.manifest().expect("manifest").as_bytes())
                .expect("manifest json");
        assert_eq!(manifest["scope"], "whole_application");
        assert_eq!(manifest["class_totals"]["entity"]["rows"], "2");
        assert_eq!(
            manifest["query_module_hashes"].as_array().map(Vec::len),
            Some(1)
        );

        let mut cancelled = state();
        fail_state(&mut cancelled, ApplicationExportFailureV1::Cancelled).expect("cancel");
        assert_eq!(cancelled.phase, ApplicationExportPhaseV1::Cancelled);
        assert_eq!(
            cancelled.failure,
            Some(ApplicationExportFailureV1::Cancelled)
        );
        assert!(cancelled.manifest.is_some());
        assert!(cancelled.receipt.is_some());

        let mut portability = state();
        portability.portability_manifest_hash =
            Some(ApplicationPortabilityManifestHash::from_bytes([0x77; 32]));
        portability.workflow_quiescence = vec![WorkflowQuiescenceWireV1 {
            workflow: "WorkLifecycle".to_owned(),
            entity_type_id: 7,
            owner_field_id: 11,
            expiry_field_id: 12,
            checked_rows: 3,
            quiescent_rows: 3,
            non_quiescent_rows: 0,
        }];
        complete_state(&mut portability).expect("portable completion");
        let portable_manifest: Value =
            serde_json::from_slice(portability.manifest.as_deref().expect("portable manifest"))
                .expect("portable manifest json");
        let portable_receipt: Value =
            serde_json::from_slice(portability.receipt.as_deref().expect("portable receipt"))
                .expect("portable receipt json");
        assert_eq!(portable_manifest["schema"], PORTABILITY_MANIFEST_SCHEMA);
        assert_eq!(
            portable_manifest["workflow_lease_quiescence"][0]["workflow"],
            "WorkLifecycle"
        );
        assert_eq!(portable_receipt["portability_intent"], true);
    }

    #[test]
    fn selected_classes_advance_in_one_closed_order() {
        let state = state();
        assert_eq!(
            first_selected_class(&state.selection),
            Some(ApplicationExportClassV1::Entity)
        );
        assert_eq!(
            next_selected_class(&state.selection, ApplicationExportClassV1::Entity),
            Some(ApplicationExportClassV1::Event)
        );
        assert_eq!(
            next_selected_class(&state.selection, ApplicationExportClassV1::PublicAudit),
            None
        );
    }

    #[test]
    fn replay_page_is_released_only_for_the_exact_durable_successor() {
        let initial = state();
        let request_cursor = cursor_for(&stored_state(&initial).expect("initial")).expect("cursor");
        let line = CanonicalApplicationExportJsonLine::new(b"{\"class\":\"entity\"}".to_vec())
            .expect("line");
        let page = ApplicationExportPageV1::new(
            initial.operation_id,
            NonZeroU64::new(1).expect("page"),
            ApplicationExportClassV1::Entity,
            vec![line.clone()],
            None,
            true,
            true,
        )
        .expect("page");
        let mut successor = initial;
        successor.pages_released = 1;
        successor.rows_released = 1;
        successor.bytes_released = u64::try_from(line.as_bytes().len() + 1).expect("bytes");
        successor.class_pages[0] = 1;
        successor.class_rows[0] = 1;
        successor.class_bytes[0] = successor.bytes_released;
        successor.page_hashes.push(page.page_hash());
        complete_state(&mut successor).expect("complete");
        let retained = stored_state(&successor).expect("successor");
        let replays = Arc::new(Mutex::new(PageReplayCache::default()));
        install_replay(
            &replays,
            successor.operation_id,
            PageReplayEntry::new(request_cursor.clone(), &retained, page.clone()).expect("entry"),
        )
        .expect("install");

        assert_eq!(
            replayed_page(
                &replays,
                successor.operation_id,
                &request_cursor,
                retained.canonical_state(),
            )
            .expect("replay"),
            Some(page)
        );
        assert!(
            replayed_page(
                &replays,
                successor.operation_id,
                &request_cursor,
                b"different durable state",
            )
            .expect("mismatch")
            .is_none()
        );
    }
}
