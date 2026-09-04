#![forbid(unsafe_code)]
// req: DEP-005

//! Gate-A contract migration reference-model and generated-history evidence.

use riffdb_catalog::ValidatedMigrationPlan;
use riffdb_commit::{
    MigrationCoordinator, MigrationProjectionBuildObservation, MigrationProjectionBuildPort,
    MigrationProjectionError,
};
use riffdb_contract_compiler::{
    compile_contract_source, compile_contract_successor, compile_migration_source,
};
use riffdb_contract_ir::{ContractBundle, MigrationBundleV1};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityTarget, MigrationBatch, MigrationCutover,
    MigrationCutoverApplied, MigrationJournalState, MigrationRowEvidence, MigrationRowMutation,
    MigrationScanCursor, MigrationScanPage, MigrationStageError, MigrationStagePort,
    StoredEntityRecordV1,
};
use riffdb_storage_redb::RedbMigrationStageFixture;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, EntityKeyBuilder, EntityTypeId, EntityVersion, FieldId,
    ProjectionGeneration, ProjectionId,
};

const PARENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/parent.contract.bundle"
));
const CANDIDATE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/candidate.contract.bundle"
));
const MIGRATION: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/required-field.migration.bundle"
));
const MODEL_BOUNDARIES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/model/gate-a/required-field-boundaries.txt"
));

#[test]
fn required_field_matches_reference_model_across_page_and_batch_boundaries() {
    let (plan, parent) = plan();
    let rows = (0..300).map(|value| row(&parent, value)).collect();
    let mut stage = redb_stage(&plan, rows);
    let before = *stage.history_witness();

    let preflight = MigrationCoordinator::check(&plan, &stage).expect("complete preflight");
    assert_eq!(preflight.checked_rows(), 300);
    assert_eq!(preflight.changed_rows(), 300);
    assert_eq!(preflight.scan_pages(), 2);
    assert_eq!(stage.history_witness(), &before);
    assert!(stage.journal().is_none());

    let applied = MigrationCoordinator::apply(&plan, &mut stage).expect("bounded migration");
    assert_eq!(applied.checked_rows(), 300);
    assert_eq!(applied.changed_rows(), 300);
    assert_eq!(applied.scan_pages(), 2);
    assert_eq!(applied.batch_count(), 5);
    assert_eq!(stage.history_witness(), &before);
    assert_eq!(stage.active_bundle_hash(), plan.candidate_bundle_hash());
    assert!(stage.predecessor_writes_retired());
    assert_eq!(stage.retained_archive().len(), 300);
    for predecessor in stage.retained_archive() {
        assert_eq!(predecessor.entity_version(), EntityVersion::first());
        assert_eq!(
            predecessor.schema_binding().bundle_hash(),
            plan.parent_bundle_hash()
        );
        assert!(
            predecessor
                .fields()
                .fields()
                .iter()
                .all(|(field, _)| *field != FieldId::new(3).unwrap())
        );
    }
    assert!(stage.journal().is_some_and(|journal| journal.is_complete()));
    let record = stage
        .migration_record()
        .expect("permanent migration record");
    assert_eq!(record.artifacts().parent(), plan.parent_bundle_hash());
    assert_eq!(record.artifacts().candidate(), plan.candidate_bundle_hash());
    assert_eq!(record.artifacts().migration(), plan.migration_bundle_hash());
    assert_eq!(record.administration_sequence().get(), 1);

    for migrated in stage.entities() {
        assert_eq!(migrated.entity_version(), EntityVersion::new(2).unwrap());
        assert_eq!(
            migrated.schema_binding().bundle_hash(),
            plan.candidate_bundle_hash()
        );
        let fields = migrated.fields().fields();
        let value = match &fields[1].1 {
            CanonicalValue::I64(value) => value,
            _ => panic!("fixture value field changed type"),
        };
        assert_eq!(
            fields[2],
            (FieldId::new(3).unwrap(), CanonicalValue::I64(value + 1))
        );
    }
}

#[test]
fn coordinator_resumes_from_the_atomically_committed_journal_cursor() {
    let (plan, parent) = plan();
    let rows = (0..130).map(|value| row(&parent, value)).collect();
    let mut stage = redb_stage(&plan, rows);
    let mut interrupted = InterruptAfterFirstCommittedBatch {
        inner: &mut stage,
        fired: false,
    };
    assert!(MigrationCoordinator::apply(&plan, &mut interrupted).is_err());
    assert_eq!(
        stage.journal().expect("durable progress").checked_rows(),
        64
    );

    let resumed = MigrationCoordinator::apply(&plan, &mut stage).expect("resume exact cursor");
    assert_eq!(resumed.checked_rows(), 130);
    assert_eq!(resumed.changed_rows(), 130);
    assert_eq!(resumed.batch_count(), 3);
    assert!(
        stage
            .journal()
            .is_some_and(MigrationJournalState::is_complete)
    );
}

#[test]
fn generated_histories_match_the_pure_row_reference_model() {
    for line in MODEL_BOUNDARIES.lines().skip(4) {
        let columns = line
            .split(',')
            .map(|column| column.parse::<u64>().expect("numeric model fixture"))
            .collect::<Vec<_>>();
        assert_eq!(columns.len(), 5);
        let row_count = columns[0] as usize;
        let (plan, parent) = plan();
        let rows = (0..row_count)
            .map(|offset| generated_value(row_count, offset))
            .map(|value| row(&parent, value))
            .collect::<Vec<_>>();
        let mut stage = redb_stage(&plan, rows);
        let history_before = *stage.history_witness();

        let applied = MigrationCoordinator::apply(&plan, &mut stage).expect("generated history");
        assert_eq!(applied.checked_rows(), row_count as u64);
        assert_eq!(applied.changed_rows(), row_count as u64);
        assert_eq!(applied.scan_pages(), columns[3]);
        assert_eq!(applied.batch_count(), columns[4]);
        assert_eq!(stage.history_witness(), &history_before);
        for migrated in stage.entities() {
            let fields = migrated.fields().fields();
            let CanonicalValue::I64(old_value) = fields[1].1 else {
                panic!("generated value changed type");
            };
            assert_eq!(
                fields[2],
                (FieldId::new(3).unwrap(), CanonicalValue::I64(old_value + 1))
            );
        }
    }
}

#[test]
fn unresolved_predecessor_admission_fails_before_any_mutation() {
    let (plan, parent) = plan();
    let mut stage = redb_stage(&plan, vec![row(&parent, 7)]);
    stage
        .add_unresolved_retiring_admission()
        .expect("pending admission fixture");
    let before = stage.snapshot();

    let finding = MigrationCoordinator::apply(&plan, &mut stage)
        .expect_err("pending predecessor admission must block cutover");
    assert_eq!(finding.code(), "RDB-M112");
    assert_eq!(stage.snapshot(), before);
}

#[test]
fn atomic_batch_rechecks_exact_source_version_hash_and_binding() {
    let (plan, parent) = plan();
    let original = row(&parent, 41);
    let prepared = plan
        .prepare_row(original.clone())
        .expect("reference transform");
    let mutation = MigrationRowMutation::new(
        MigrationRowEvidence::from_source(original),
        Some(prepared.post_image().expect("changed row").clone()),
        prepared.rebuilt_indexes().to_vec(),
    )
    .expect("checked mutation");
    let checked_through = MigrationScanCursor::after(mutation.expected().source().target().clone());
    let mut stage = redb_stage(&plan, vec![row_with_id_value(&parent, 41, 42)]);
    let before = stage.snapshot();
    let batch = MigrationBatch::new(
        plan.migration_bundle_hash(),
        vec![mutation],
        checked_through,
        1,
        1,
    )
    .expect("bounded batch");

    assert_eq!(
        stage.apply_migration_batch(batch),
        Err(MigrationStageError::RowChanged)
    );
    assert_eq!(stage.snapshot(), before);
}

#[test]
fn combined_gate_a_constraints_indexes_and_projection_pass_together() {
    let (plan, parent) = combined_gate_a_plan();
    let group_id = uuid_for(900);
    let rows = vec![
        named_row(
            &parent,
            "Group",
            vec![("group_id", CanonicalValue::Uuid(group_id))],
        ),
        named_row(
            &parent,
            "Item",
            vec![
                ("group_id", CanonicalValue::Uuid(group_id)),
                ("item_id", CanonicalValue::Uuid(uuid_for(1))),
                ("amount", CanonicalValue::I64(5)),
            ],
        ),
        named_row(
            &parent,
            "Item",
            vec![
                ("group_id", CanonicalValue::Uuid(group_id)),
                ("item_id", CanonicalValue::Uuid(uuid_for(2))),
                ("amount", CanonicalValue::I64(7)),
            ],
        ),
    ];
    let mut stage = redb_stage(&plan, rows);

    let checked = MigrationCoordinator::check(&plan, &stage).expect("combined preflight");
    assert_eq!(checked.checked_rows(), 3);
    assert_eq!(checked.changed_rows(), 0);
    let mut projection_builder = RecordingProjectionBuilder::default();
    let applied = MigrationCoordinator::apply_with_projection_builder(
        &plan,
        &mut stage,
        &mut projection_builder,
    )
    .expect("combined apply");
    assert_eq!(applied.changed_rows(), 0);
    assert_eq!(stage.index_entries().len(), 4);
    assert!(
        plan.rebuilt_projections()
            .iter()
            .copied()
            .eq(projection_builder.requested.iter().copied())
    );
    assert!(
        stage
            .entities()
            .all(|row| row.entity_version() == EntityVersion::first())
    );
}

#[derive(Default)]
struct RecordingProjectionBuilder {
    requested: Vec<ProjectionId>,
}

impl MigrationProjectionBuildPort for RecordingProjectionBuilder {
    fn rebuild_projection(
        &mut self,
        projection: ProjectionId,
        frontier: Option<riffdb_types::CommitSequence>,
    ) -> Result<MigrationProjectionBuildObservation, MigrationProjectionError> {
        self.requested.push(projection);
        Ok(MigrationProjectionBuildObservation::ready(
            projection,
            ProjectionGeneration::first(),
            frontier,
        ))
    }
}

struct InterruptAfterFirstCommittedBatch<'a> {
    inner: &'a mut RedbMigrationStageFixture,
    fired: bool,
}

impl MigrationStagePort for InterruptAfterFirstCommittedBatch<'_> {
    fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
        self.inner.active_bundle_hash()
    }

    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError> {
        self.inner.scan_migration_rows(cursor)
    }

    fn migration_target_exists(&self, target: &EntityTarget) -> Result<bool, MigrationStageError> {
        self.inner.migration_target_exists(target)
    }

    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
        self.inner.has_unresolved_retiring_admissions()
    }

    fn migration_journal_state(
        &self,
        migration: riffdb_types::MigrationBundleHash,
    ) -> Result<Option<MigrationJournalState>, MigrationStageError> {
        self.inner.migration_journal_state(migration)
    }

    fn apply_migration_batch(&mut self, batch: MigrationBatch) -> Result<(), MigrationStageError> {
        self.inner.apply_migration_batch(batch)?;
        if !self.fired {
            self.fired = true;
            return Err(MigrationStageError::Integrity);
        }
        Ok(())
    }

    fn build_migration_projection_candidates(
        &mut self,
        projections: &[ProjectionId],
    ) -> Result<(), MigrationStageError> {
        self.inner
            .build_migration_projection_candidates(projections)
    }

    fn validate_migration_stage(
        &self,
        candidate: riffdb_types::ContractBundleHash,
        retained_parent_lineage: &[riffdb_types::ContractBundleHash],
    ) -> Result<(), MigrationStageError> {
        self.inner
            .validate_migration_stage(candidate, retained_parent_lineage)
    }

    fn validate_migration_stage_structure(&self) -> Result<(), MigrationStageError> {
        self.inner.validate_migration_stage_structure()
    }

    fn finalize_migration(
        &mut self,
        cutover: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError> {
        self.inner.finalize_migration(cutover)
    }
}

#[test]
fn gate_a_relationship_unique_and_invariant_fail_value_free_before_mutation() {
    let (plan, parent) = combined_gate_a_plan();
    let group_id = uuid_for(900);
    let cases = [
        (
            vec![named_row(
                &parent,
                "Item",
                vec![
                    ("group_id", CanonicalValue::Uuid(group_id)),
                    ("item_id", CanonicalValue::Uuid(uuid_for(1))),
                    ("amount", CanonicalValue::I64(5)),
                ],
            )],
            "RDB-M107",
        ),
        (
            vec![
                named_row(
                    &parent,
                    "Group",
                    vec![("group_id", CanonicalValue::Uuid(group_id))],
                ),
                named_row(
                    &parent,
                    "Item",
                    vec![
                        ("group_id", CanonicalValue::Uuid(group_id)),
                        ("item_id", CanonicalValue::Uuid(uuid_for(1))),
                        ("amount", CanonicalValue::I64(5)),
                    ],
                ),
                named_row(
                    &parent,
                    "Item",
                    vec![
                        ("group_id", CanonicalValue::Uuid(group_id)),
                        ("item_id", CanonicalValue::Uuid(uuid_for(2))),
                        ("amount", CanonicalValue::I64(5)),
                    ],
                ),
            ],
            "RDB-M108",
        ),
        (
            vec![
                named_row(
                    &parent,
                    "Group",
                    vec![("group_id", CanonicalValue::Uuid(group_id))],
                ),
                named_row(
                    &parent,
                    "Item",
                    vec![
                        ("group_id", CanonicalValue::Uuid(group_id)),
                        ("item_id", CanonicalValue::Uuid(uuid_for(1))),
                        ("amount", CanonicalValue::I64(-1)),
                    ],
                ),
            ],
            "RDB-M105",
        ),
    ];

    for (rows, expected_code) in cases {
        let mut stage = redb_stage(&plan, rows);
        let before = stage.snapshot();
        let finding = MigrationCoordinator::apply(&plan, &mut stage)
            .expect_err("constraint must fail complete preflight");
        assert_eq!(finding.code(), expected_code);
        assert_eq!(stage.snapshot(), before);
    }
}

#[test]
fn ancestor_written_rows_are_normalized_through_the_exact_parent_lineage() {
    const V1: &str = r#"contract AncestorRows version 1 {
  entity Row { key (id: uuid) field value: i64 }
  entity Stable { key (stable_id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  aggregate StableRows { root Stable partition_by stable_id conflict_key (stable_id) }
}
"#;
    const V2: &str = r#"contract AncestorRows version 2 {
  entity Row {
    key (id: uuid)
    field value: i64
    field note: optional<i64>
  }
  entity Stable { key (stable_id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  aggregate StableRows { root Stable partition_by stable_id conflict_key (stable_id) }
}
"#;
    const V3: &str = r#"contract AncestorRows version 3 {
  entity Row {
    key (id: uuid)
    field value: i64
    field note: optional<i64>
    field doubled: i64
  }
  entity Stable { key (stable_id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  aggregate StableRows { root Stable partition_by stable_id conflict_key (stable_id) }
}
"#;
    const MIGRATION_SOURCE: &str = r#"migration AncestorRows from 2 to 3 {
  transform Row { set doubled = old.value + 1 }
}
"#;

    let v1 = compile_contract_source(V1).expect("v1");
    let v2 = compile_contract_successor(V2, &v1).expect("v2");
    let v3 = compile_contract_successor(V3, &v2).expect("v3");
    let migration = compile_migration_source(MIGRATION_SOURCE, &v2, &v3).expect("migration");
    let v1 = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(v1).expect("v1");
    let v2 = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(v2).expect("v2");
    let v3 = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(v3).expect("v3");
    let plan =
        ValidatedMigrationPlan::from_lineage_artifacts(vec![v1.clone(), v2.clone()], v3, migration)
            .expect("lineage-aware plan");
    let ancestor_row = named_row(
        &v1,
        "Row",
        vec![
            ("id", CanonicalValue::Uuid(uuid_for(77))),
            ("value", CanonicalValue::I64(9)),
        ],
    );
    let unchanged_ancestor = named_row(
        &v1,
        "Stable",
        vec![("stable_id", CanonicalValue::Uuid(uuid_for(88)))],
    );
    let mut stage = redb_stage(&plan, vec![ancestor_row, unchanged_ancestor]);

    MigrationCoordinator::apply(&plan, &mut stage).expect("ancestor row migration");
    let row_entity_id = plan
        .candidate()
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Row")
        .expect("candidate row")
        .id();
    let migrated = stage
        .entities()
        .find(|row| row.target().entity_type_id() == row_entity_id)
        .expect("migrated row");
    assert_eq!(migrated.entity_version(), EntityVersion::new(2).unwrap());
    let candidate_entity = plan
        .candidate()
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Row")
        .expect("candidate row");
    let field = |name: &str| {
        candidate_entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("candidate field")
            .id()
    };
    assert_eq!(
        record_value(migrated.fields(), field("note")),
        CanonicalValue::Null
    );
    assert_eq!(
        record_value(migrated.fields(), field("doubled")),
        CanonicalValue::I64(10)
    );
    let unchanged = stage
        .entities()
        .find(|row| row.target().entity_type_id() != row_entity_id)
        .expect("unchanged ancestor row");
    assert_eq!(unchanged.entity_version(), EntityVersion::first());
    assert_eq!(unchanged.schema_binding().bundle_hash(), v1.bundle_hash());
}

#[test]
fn arithmetic_failure_is_value_free_and_leaves_stage_byte_identical() {
    let (plan, parent) = plan();
    let mut stage = redb_stage(&plan, vec![row(&parent, i64::MAX)]);
    let before = stage.snapshot();

    let finding = MigrationCoordinator::apply(&plan, &mut stage)
        .expect_err("overflow must abort complete preflight");
    assert_eq!(finding.code(), "RDB-M104");
    assert_eq!(finding.entity_type(), Some(EntityTypeId::new(1).unwrap()));
    assert_eq!(finding.field(), Some(FieldId::new(3).unwrap()));
    assert_eq!(stage.snapshot(), before);
    assert!(!format!("{finding:?}").contains(&i64::MAX.to_string()));
}

fn redb_stage(
    plan: &ValidatedMigrationPlan,
    rows: Vec<StoredEntityRecordV1>,
) -> RedbMigrationStageFixture {
    RedbMigrationStageFixture::create(
        &plan.parent().to_stored().expect("stored parent"),
        plan.candidate_bundle_hash(),
        plan.migration_bundle_hash(),
        rows,
    )
    .expect("redb migration stage")
}

fn plan() -> (
    ValidatedMigrationPlan,
    riffdb_catalog::ValidatedContractBundle,
) {
    let parent = ContractBundle::decode(PARENT).expect("parent fixture");
    let candidate = ContractBundle::decode(CANDIDATE).expect("candidate fixture");
    let migration = MigrationBundleV1::decode(MIGRATION).expect("migration fixture");
    let parent = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(parent)
        .expect("catalog parent");
    let candidate = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(candidate)
        .expect("catalog candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed migration plan");
    (plan, parent)
}

fn combined_gate_a_plan() -> (
    ValidatedMigrationPlan,
    riffdb_catalog::ValidatedContractBundle,
) {
    const PARENT_SOURCE: &str = r#"contract CombinedGateA version 1 {
  entity Group { key (group_id: uuid) }
  entity Item {
    key (group_id: uuid, item_id: uuid)
    field amount: i64
  }
  event Changed { group_id: uuid amount: i64 }
  aggregate Groups { root Group partition_by group_id conflict_key (group_id) }
  aggregate Items { root Item partition_by group_id conflict_key (group_id, item_id) }
}
"#;
    const CANDIDATE_SOURCE: &str = r#"contract CombinedGateA version 2 {
  entity Group { key (group_id: uuid) }
  entity Item {
    key (group_id: uuid, item_id: uuid)
    field amount: i64
    index by_amount (amount, group_id, item_id)
    unique group_amount (group_id, amount)
    reference group (group_id) -> Group(group_id)
    invariant nonnegative: amount >= 0
  }
  event Changed { group_id: uuid amount: i64 }
  aggregate Groups { root Group partition_by group_id conflict_key (group_id) }
  aggregate Items { root Item partition_by group_id conflict_key (group_id, item_id) }
  projection ByGroup {
    source event Changed
    key (group_id)
    measure total = sum(amount)
    frontier transactionally_ordered
  }
}
"#;
    const MIGRATION_SOURCE: &str = "migration CombinedGateA from 1 to 2 {}\n";

    let parent = compile_contract_source(PARENT_SOURCE).expect("combined parent");
    let candidate =
        compile_contract_successor(CANDIDATE_SOURCE, &parent).expect("combined candidate");
    let migration = compile_migration_source(MIGRATION_SOURCE, &parent, &candidate)
        .expect("combined Gate-A proof");
    let parent = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(parent)
        .expect("catalog parent");
    let candidate = riffdb_catalog::ValidatedContractBundle::from_compiler_bundle(candidate)
        .expect("catalog candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed combined plan");
    (plan, parent)
}

fn named_row(
    parent: &riffdb_catalog::ValidatedContractBundle,
    entity_name: &str,
    values: Vec<(&str, CanonicalValue)>,
) -> StoredEntityRecordV1 {
    let entity = parent
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == entity_name)
        .expect("fixture entity");
    let fields = entity
        .record()
        .fields()
        .iter()
        .map(|field| {
            let value = values
                .iter()
                .find_map(|(name, value)| (*name == field.name()).then(|| value.clone()))
                .expect("fixture field value");
            (field.id(), value)
        })
        .collect::<Vec<_>>();
    let record = CanonicalRecord::new(fields).expect("canonical named row");
    let key_values = entity
        .primary_key_fields()
        .iter()
        .map(|field_id| {
            record
                .fields()
                .iter()
                .find_map(|(candidate, value)| (candidate == field_id).then(|| value.clone()))
                .expect("primary key value")
        })
        .collect::<Vec<_>>();
    let key = entity
        .primary_key()
        .encode_entity(&key_values)
        .expect("canonical entity key");
    let target = EntityTarget::new(entity.id(), key).expect("named target");
    StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        parent.contract_version(),
        DurableKeySchemaBindingV1::new(
            parent.lineage().clone(),
            parent.contract_version(),
            parent.bundle_hash(),
        ),
        record,
    )
    .expect("stored named row")
}

fn row(parent: &riffdb_catalog::ValidatedContractBundle, value: i64) -> StoredEntityRecordV1 {
    row_with_id_value(parent, value, value)
}

fn row_with_id_value(
    parent: &riffdb_catalog::ValidatedContractBundle,
    id_seed: i64,
    value: i64,
) -> StoredEntityRecordV1 {
    let entity = EntityTypeId::new(1).unwrap();
    let id = uuid_for(id_seed);
    let mut key = EntityKeyBuilder::new(entity);
    key.push_uuid(&id).expect("UUID key component");
    let target = EntityTarget::new(entity, key.finish().expect("entity key")).expect("target");
    let fields = CanonicalRecord::new(vec![
        (FieldId::new(1).unwrap(), CanonicalValue::Uuid(id)),
        (FieldId::new(2).unwrap(), CanonicalValue::I64(value)),
    ])
    .expect("canonical row");
    StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        parent.contract_version(),
        DurableKeySchemaBindingV1::new(
            parent.lineage().clone(),
            parent.contract_version(),
            parent.bundle_hash(),
        ),
        fields,
    )
    .expect("stored row")
}

fn uuid_for(value: i64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&value.to_be_bytes());
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}

fn generated_value(row_count: usize, offset: usize) -> i64 {
    let mixed = (row_count as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add((offset as u64).wrapping_mul(1_442_695_040_888_963_407));
    (mixed % 1_000_000) as i64
}

fn record_value(record: &CanonicalRecord, field: FieldId) -> CanonicalValue {
    record
        .fields()
        .iter()
        .find_map(|(candidate, value)| (*candidate == field).then(|| value.clone()))
        .expect("record field")
}
