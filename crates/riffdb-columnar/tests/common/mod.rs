//! Shared history harness for columnar engine tests.
//!
//! Builds a minimal in-memory authoritative reader (not production apply code)
//! and an independent oracle that never imports merge/supersession helpers.

#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, CommittedEntityReferenceV2,
    DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1, EncodedContentCharge,
    EncodedPageItem, EntityTarget, ExecutablePlanRef, IdempotencyIdentity, StorageError,
    StorageErrorKind, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredOutcomeV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CommandId, CommitSequence, ContractBundleHash, ContractLineage, ContractVersion,
    EntityKeyBuilder, EntityTypeId, EntityVersion, EventId, FieldId, FrontierPosition, LogicalTime,
    OutcomeId, PartitionKeyHash, PlanHash, ProvenanceId, RequestId, TenantId, TenantScope,
    Timestamp,
};

use riffdb_columnar::{
    ColumnPredicate, ColumnarEngine, ColumnarProjectionDefinition, ColumnarQueryRequest,
    OpenOptions, QueryBudget, QueryResult, RegisteredDefinition,
};

pub(crate) const CONTRACT: &str = r#"
contract ColumnarHarness version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: u64)
    field status: u64
    field title: string<200>
    field priority: i64
  }

  entity Note {
    key (organization_id: uuid, note_id: u64)
    field body: string<200>
  }

  event TicketCreated {
    organization_id: uuid
    ticket_id: u64
  }

  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }

  aggregate Notes {
    root Note
    partition_by organization_id
    conflict_key (organization_id, note_id)
  }

  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: u64
    input status: u64
    input title: string<200>
    input priority: i64

    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else AlreadyExists { ticket_id: ticket_id }

    set ticket.status = status
    set ticket.title = title
    set ticket.priority = priority

    emit TicketCreated { organization_id: organization_id, ticket_id: ticket_id }
    return Created { ticket: ticket }
  }

  command CreateNote {
    input idempotency_key: string<128>
    input organization_id: uuid
    input note_id: u64
    input body: string<200>

    idempotency_key idempotency_key
    create Note(organization_id, note_id) as note
      else NoteExists { note_id: note_id }

    set note.body = body
    return NoteCreated { note: note }
  }
}
"#;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

pub(crate) fn temp_dir(label: &str) -> PathBuf {
    let ordinal = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let base = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let path = base.join(format!(
        "riffdb-columnar-{label}-{}-{ordinal}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

pub(crate) fn compile_bundle() -> ContractBundle {
    compile_contract_source(CONTRACT).expect("harness contract compiles")
}

pub(crate) fn field_id(bundle: &ContractBundle, entity: &str, name: &str) -> FieldId {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|e| e.name() == entity)
        .expect("entity");
    entity
        .record()
        .fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.id())
        .unwrap_or_else(|| panic!("field {name} on {}", entity.name()))
}

pub(crate) fn entity_type_id(bundle: &ContractBundle, name: &str) -> EntityTypeId {
    bundle
        .schema()
        .entities()
        .iter()
        .find(|e| e.name() == name)
        .map(|e| e.id())
        .expect("entity type")
}

pub(crate) fn register_ticket_board(bundle: &ContractBundle) -> RegisteredDefinition {
    let org = field_id(bundle, "Ticket", "organization_id");
    let status = field_id(bundle, "Ticket", "status");
    let title = field_id(bundle, "Ticket", "title");
    let priority = field_id(bundle, "Ticket", "priority");
    RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "ticket_board".into(),
            entity_name: "Ticket".into(),
            projected_fields: vec![status, title, priority],
            org_scope_field: org,
        },
        bundle,
    )
    .expect("register board")
}

pub(crate) fn open_engine(definition: RegisteredDefinition, label: &str) -> ColumnarEngine {
    ColumnarEngine::open(
        definition,
        OpenOptions {
            directory: temp_dir(label),
        },
    )
    .expect("open engine")
}

pub(crate) fn uuid(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

pub(crate) fn org_value(fill: u8) -> CanonicalValue {
    CanonicalValue::Uuid(uuid(fill))
}

fn plan_ref() -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("columnar-harness").expect("lineage"),
        ContractVersion::new(1).expect("version"),
        ContractBundleHash::from_bytes([0x21; 32]),
        CommandId::new(1).expect("command"),
        PlanHash::from_bytes([0x22; 32]),
    )
}

fn actor() -> AdmittedActorContext {
    AdmittedActorContext::new(
        ActorId::new("columnar-test").expect("actor"),
        ActorKind::Human,
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        None,
    )
}

fn empty_record() -> CanonicalRecord {
    CanonicalRecord::new(Vec::new()).expect("empty")
}

fn charge() -> EncodedContentCharge {
    EncodedContentCharge::new(64).expect("charge")
}

fn storage_err() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

/// One live entity image at a version (current authoritative state).
#[derive(Clone, Debug)]
pub(crate) struct EntityImage {
    pub target: EntityTarget,
    pub version: EntityVersion,
    pub fields: CanonicalRecord,
}

/// Independent oracle: BTreeMap of projected live rows, keyed by org then PK.
#[derive(Clone, Debug)]
pub(crate) struct Oracle {
    /// org_encoded → pk → (version, cells)
    pub rows: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, (u64, Vec<CanonicalValue>)>>,
    pub frontier: FrontierPosition,
}

impl Default for Oracle {
    fn default() -> Self {
        Self {
            rows: BTreeMap::new(),
            frontier: FrontierPosition::BeforeFirst,
        }
    }
}

impl Oracle {
    pub(crate) fn apply_exact(
        &mut self,
        org: &CanonicalValue,
        pk: &[u8],
        version: u64,
        cells: Vec<CanonicalValue>,
        sequence: CommitSequence,
    ) {
        let org_key = riffdb_types::encode_canonical_value(org).expect("org encode");
        let entry = self.rows.entry(org_key).or_default();
        match entry.get(pk) {
            Some((existing, _)) if *existing >= version => {}
            _ => {
                entry.insert(pk.to_vec(), (version, cells));
            }
        }
        self.frontier = FrontierPosition::AppliedThrough(sequence);
    }

    pub(crate) fn query_rows(
        &self,
        org: &CanonicalValue,
        status_field_idx: usize,
        eq_status: Option<u64>,
    ) -> Vec<Vec<CanonicalValue>> {
        let org_key = riffdb_types::encode_canonical_value(org).expect("org encode");
        let Some(partition) = self.rows.get(&org_key) else {
            return Vec::new();
        };
        let mut out: Vec<_> = partition
            .iter()
            .filter(|(_, (_, cells))| match eq_status {
                Some(want) => {
                    matches!(cells.get(status_field_idx), Some(CanonicalValue::U64(v)) if *v == want)
                }
                None => true,
            })
            .map(|(pk, (_, cells))| (pk.clone(), cells.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out.into_iter().map(|(_, cells)| cells).collect()
    }
}

/// In-memory authoritative source for tests.
#[derive(Clone, Default)]
pub(crate) struct HistorySource {
    /// Current entity state (latest image only).
    pub entities: BTreeMap<Vec<u8>, StoredEntityRecordV1>,
    /// Ordered commits.
    pub commits: Vec<StoredCommitRecordV1>,
}

impl HistorySource {
    pub(crate) fn target_key(target: &EntityTarget) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&target.entity_type_id().to_be_bytes());
        out.extend_from_slice(target.key().as_bytes());
        out
    }

    pub(crate) fn ticket_target(
        entity_type: EntityTypeId,
        org: [u8; 16],
        ticket_id: u64,
    ) -> EntityTarget {
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_uuid(&org).expect("uuid");
        key.push_u64(ticket_id).expect("u64");
        EntityTarget::new(entity_type, key.finish().expect("key")).expect("target")
    }

    pub(crate) fn note_target(
        entity_type: EntityTypeId,
        org: [u8; 16],
        note_id: u64,
    ) -> EntityTarget {
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_uuid(&org).expect("uuid");
        key.push_u64(note_id).expect("u64");
        EntityTarget::new(entity_type, key.finish().expect("key")).expect("target")
    }

    pub(crate) fn put_entity(&mut self, record: StoredEntityRecordV1) {
        let key = Self::target_key(record.target());
        self.entities.insert(key, record);
    }

    /// Appends a commit that references the given post-images (must already be current state
    /// for match cases, or prior versions for race cases where current state is newer).
    pub(crate) fn append_commit(
        &mut self,
        sequence: CommitSequence,
        references: Vec<CommittedEntityReferenceV2>,
        expected_priors: Vec<(EntityTarget, riffdb_storage_api::ExpectedEntityState)>,
    ) {
        let plan = plan_ref();
        let request_id =
            RequestId::from_bytes(uuid((sequence.get() % 200) as u8 + 1)).expect("req");
        let provenance_id =
            ProvenanceId::from_bytes(uuid((sequence.get() % 200) as u8 + 40)).expect("prov");
        let logical_time = LogicalTime::new(Timestamp::new(1_700_000_000, 0).expect("ts"));
        let deps = build_read_deps(expected_priors);
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan,
            CanonicalInputHash::from_bytes([0x32; 32]),
            actor(),
            logical_time,
            PartitionKeyHash::from_bytes([0x41; 32]),
            Vec::new(),
            deps,
            references,
            Vec::new(),
            DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), empty_record())
                .expect("outcome"),
            provenance_id,
            Vec::new(),
            DurabilityMode::Memory,
        )
        .expect("commit");
        self.commits.push(commit);
    }

    pub(crate) fn make_entity(
        target: EntityTarget,
        version: EntityVersion,
        fields: CanonicalRecord,
    ) -> StoredEntityRecordV1 {
        let plan = plan_ref();
        StoredEntityRecordV1::new(
            target,
            version,
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            fields,
        )
        .expect("entity")
    }
}

fn build_read_deps(
    expected: Vec<(EntityTarget, riffdb_storage_api::ExpectedEntityState)>,
) -> StoredReadDependenciesV1 {
    use riffdb_storage_api::{ReadDependencies, ReadDependency};
    let deps = ReadDependencies::new(
        expected
            .into_iter()
            .map(|(target, expected)| ReadDependency::EntityObservation { target, expected }),
    )
    .expect("read deps");
    StoredReadDependenciesV1::from_live(&deps).expect("stored deps")
}

impl AuthoritativePointReader for HistorySource {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        Ok(self.entities.get(&Self::target_key(target)).cloned())
    }

    fn read_stored_outcome(
        &self,
        _identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        Ok(None)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        Ok(self
            .commits
            .iter()
            .find(|c| c.commit_sequence() == sequence)
            .cloned())
    }

    fn read_provenance(
        &self,
        _provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        Ok(None)
    }

    fn read_durable_event(
        &self,
        _event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        Ok(None)
    }
}

impl AuthoritativeScanReader for HistorySource {
    fn scan_index(
        &self,
        _request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        Err(storage_err())
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let inclusive_upper = request.inclusive_upper().map_or_else(
            || {
                self.commits
                    .last()
                    .map_or(FrontierPosition::BeforeFirst, |record| {
                        FrontierPosition::AppliedThrough(record.commit_sequence())
                    })
            },
            FrontierPosition::AppliedThrough,
        );
        let start = request.after().map_or(0, |after| {
            self.commits
                .partition_point(|record| record.commit_sequence() <= after)
        });
        let upper = match inclusive_upper {
            FrontierPosition::BeforeFirst => None,
            FrontierPosition::AppliedThrough(sequence) => Some(sequence),
        };
        let wanted = usize::from(request.limit().get());
        let mut rows: Vec<_> = self.commits[start..]
            .iter()
            .take_while(|record| upper.is_some_and(|u| record.commit_sequence() <= u))
            .take(wanted.saturating_add(1))
            .cloned()
            .map(|record| EncodedPageItem::new(record, charge()))
            .collect();
        let has_more = rows.len() > wanted;
        rows.truncate(wanted);
        if has_more {
            let next_after = rows
                .last()
                .ok_or_else(storage_err)?
                .value()
                .commit_sequence();
            CommitScanPageV1::page(request, inclusive_upper, rows, next_after)
                .map_err(|_| storage_err())
        } else {
            CommitScanPageV1::exact_end(request, inclusive_upper, rows).map_err(|_| storage_err())
        }
    }
}

/// Ticket field builders aligned with projected order [status, title, priority].
pub(crate) fn ticket_fields(
    bundle: &ContractBundle,
    org: [u8; 16],
    ticket_id: u64,
    status: u64,
    title: &str,
    priority: i64,
) -> CanonicalRecord {
    let organization_id = field_id(bundle, "Ticket", "organization_id");
    let ticket_id_field = field_id(bundle, "Ticket", "ticket_id");
    let status_f = field_id(bundle, "Ticket", "status");
    let title_f = field_id(bundle, "Ticket", "title");
    let priority_f = field_id(bundle, "Ticket", "priority");
    CanonicalRecord::new(vec![
        (organization_id, CanonicalValue::Uuid(org)),
        (ticket_id_field, CanonicalValue::U64(ticket_id)),
        (status_f, CanonicalValue::U64(status)),
        (
            title_f,
            CanonicalValue::string(title.to_string()).expect("title"),
        ),
        (priority_f, CanonicalValue::I64(priority)),
    ])
    .expect("record")
}

pub(crate) fn projected_cells(status: u64, title: &str, priority: i64) -> Vec<CanonicalValue> {
    vec![
        CanonicalValue::U64(status),
        CanonicalValue::string(title.to_string()).expect("title"),
        CanonicalValue::I64(priority),
    ]
}

pub(crate) fn board_query(org: CanonicalValue) -> ColumnarQueryRequest {
    ColumnarQueryRequest {
        org_scope: org,
        predicates: Vec::new(),
        order: Vec::new(),
        limit: None,
        group_by: None,
        aggregate: None,
        budget: QueryBudget::default(),
    }
}

pub(crate) fn eq_status_query(
    org: CanonicalValue,
    status_field: FieldId,
    status: u64,
) -> ColumnarQueryRequest {
    ColumnarQueryRequest {
        org_scope: org,
        predicates: vec![ColumnPredicate::Eq {
            field: status_field,
            value: CanonicalValue::U64(status),
        }],
        order: Vec::new(),
        limit: None,
        group_by: None,
        aggregate: None,
        budget: QueryBudget::default(),
    }
}

pub(crate) fn rows_of(result: QueryResult) -> Vec<Vec<CanonicalValue>> {
    match result {
        QueryResult::Rows(rows) => rows.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Push a create of ticket V1 into source + oracle (exact match path).
pub(crate) fn push_ticket_create(
    source: &mut HistorySource,
    oracle: &mut Oracle,
    bundle: &ContractBundle,
    sequence: u64,
    org: [u8; 16],
    ticket_id: u64,
    status: u64,
    title: &str,
    priority: i64,
) {
    let ticket_type = entity_type_id(bundle, "Ticket");
    let seq = CommitSequence::new(sequence).expect("seq");
    let version = EntityVersion::first();
    let target = HistorySource::ticket_target(ticket_type, org, ticket_id);
    let fields = ticket_fields(bundle, org, ticket_id, status, title, priority);
    let entity = HistorySource::make_entity(target.clone(), version, fields);
    let reference = CommittedEntityReferenceV2::from_post_image(&entity).expect("ref");
    source.put_entity(entity);
    source.append_commit(
        seq,
        vec![reference],
        vec![(
            target.clone(),
            riffdb_storage_api::ExpectedEntityState::Absent,
        )],
    );
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        target.key().as_bytes(),
        version.get(),
        projected_cells(status, title, priority),
        seq,
    );
}

/// Replace ticket to a new version (exact match); updates current entity state then commits.
pub(crate) fn push_ticket_replace(
    source: &mut HistorySource,
    oracle: &mut Oracle,
    bundle: &ContractBundle,
    sequence: u64,
    org: [u8; 16],
    ticket_id: u64,
    new_version: u64,
    status: u64,
    title: &str,
    priority: i64,
) {
    let ticket_type = entity_type_id(bundle, "Ticket");
    let seq = CommitSequence::new(sequence).expect("seq");
    let version = EntityVersion::new(new_version).expect("ver");
    let target = HistorySource::ticket_target(ticket_type, org, ticket_id);
    let fields = ticket_fields(bundle, org, ticket_id, status, title, priority);
    let entity = HistorySource::make_entity(target.clone(), version, fields);
    let reference = CommittedEntityReferenceV2::from_post_image(&entity).expect("ref");
    let prior = EntityVersion::new(new_version - 1).expect("prior");
    source.put_entity(entity);
    source.append_commit(
        seq,
        vec![reference],
        vec![(
            target.clone(),
            riffdb_storage_api::ExpectedEntityState::Present(prior),
        )],
    );
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        target.key().as_bytes(),
        version.get(),
        projected_cells(status, title, priority),
        seq,
    );
}

/// Forced supersession race: commit C references V1 while storage already has V2.
/// Call after putting V2 entity and after committing C with V1 reference (without updating oracle for V1).
pub(crate) fn push_race_pair(
    source: &mut HistorySource,
    oracle: &mut Oracle,
    bundle: &ContractBundle,
    seq_v1: u64,
    seq_v2: u64,
    org: [u8; 16],
    ticket_id: u64,
) {
    let ticket_type = entity_type_id(bundle, "Ticket");
    let target = HistorySource::ticket_target(ticket_type, org, ticket_id);
    let v1 = EntityVersion::first();
    let v2 = EntityVersion::new(2).expect("v2");
    let fields_v1 = ticket_fields(bundle, org, ticket_id, 1, "v1", 1);
    let fields_v2 = ticket_fields(bundle, org, ticket_id, 2, "v2", 2);
    let entity_v1 = HistorySource::make_entity(target.clone(), v1, fields_v1);
    let entity_v2 = HistorySource::make_entity(target.clone(), v2, fields_v2.clone());
    let ref_v1 = CommittedEntityReferenceV2::from_post_image(&entity_v1).expect("ref v1");
    let ref_v2 = CommittedEntityReferenceV2::from_post_image(&entity_v2).expect("ref v2");

    // Storage already at V2 before either commit is applied by the projection.
    source.put_entity(entity_v2);
    source.append_commit(
        CommitSequence::new(seq_v1).expect("s1"),
        vec![ref_v1],
        vec![(
            target.clone(),
            riffdb_storage_api::ExpectedEntityState::Absent,
        )],
    );
    source.append_commit(
        CommitSequence::new(seq_v2).expect("s2"),
        vec![ref_v2],
        vec![(
            target.clone(),
            riffdb_storage_api::ExpectedEntityState::Present(v1),
        )],
    );
    // Oracle only sees V2 once seq_v2 is "published" — we model final state at v2.
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        target.key().as_bytes(),
        2,
        projected_cells(2, "v2", 2),
        CommitSequence::new(seq_v2).expect("s2"),
    );
}

pub(crate) fn push_irrelevant_note(
    source: &mut HistorySource,
    bundle: &ContractBundle,
    sequence: u64,
    org: [u8; 16],
    note_id: u64,
) {
    let note_type = entity_type_id(bundle, "Note");
    let seq = CommitSequence::new(sequence).expect("seq");
    let target = HistorySource::note_target(note_type, org, note_id);
    let organization_id = field_id(bundle, "Note", "organization_id");
    let note_id_f = field_id(bundle, "Note", "note_id");
    let body_f = field_id(bundle, "Note", "body");
    let fields = CanonicalRecord::new(vec![
        (organization_id, CanonicalValue::Uuid(org)),
        (note_id_f, CanonicalValue::U64(note_id)),
        (body_f, CanonicalValue::string("note").expect("body")),
    ])
    .expect("fields");
    let entity = HistorySource::make_entity(target.clone(), EntityVersion::first(), fields);
    let reference = CommittedEntityReferenceV2::from_post_image(&entity).expect("ref");
    source.put_entity(entity);
    source.append_commit(
        seq,
        vec![reference],
        vec![(target, riffdb_storage_api::ExpectedEntityState::Absent)],
    );
}
