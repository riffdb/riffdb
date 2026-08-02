#![forbid(unsafe_code)]

//! Gate-C key, ownership, reference, and crash-resume acceptance evidence.

use riffdb_catalog::{ValidatedContractBundle, ValidatedMigrationPlan};
use riffdb_commit::MigrationCoordinator;
use riffdb_contract_compiler::{
    compile_contract_migration_successor, compile_contract_source, compile_contract_successor,
    compile_migration_source,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DurableKeySchemaBindingV1, EntityTarget, MigrationBatch,
    MigrationCutover, MigrationCutoverApplied, MigrationJournalState, MigrationScanCursor,
    MigrationScanPage, MigrationStageError, MigrationStagePort, StoredEntityRecordV1,
};
use riffdb_storage_memory::{MemoryMigrationHistoryWitness, MemoryMigrationStage};
use riffdb_types::{CanonicalRecord, CanonicalValue, EntityVersion, ProjectionId};

const PARENT_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/parent.riff"
));
const SUCCESSOR_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/successor.riff"
));
const MIGRATION_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/v1-to-v2.riffm"
));
const PARENT_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/parent.contract.bundle"
));
const SUCCESSOR_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/successor.contract.bundle"
));
const MIGRATION_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/v1-to-v2.migration.bundle"
));

#[test]
fn combined_rekey_repartition_reference_and_index_change_is_atomic() {
    let parent_bundle = compile_contract_source(PARENT_SOURCE).expect("parent");
    let (candidate_bundle, migration) =
        compile_contract_migration_successor(SUCCESSOR_SOURCE, MIGRATION_SOURCE, &parent_bundle)
            .expect("Gate C artifacts");
    assert_eq!(parent_bundle.canonical_bytes(), PARENT_BUNDLE);
    assert_eq!(candidate_bundle.canonical_bytes(), SUCCESSOR_BUNDLE);
    assert_eq!(migration.canonical_bytes(), MIGRATION_BUNDLE);

    let parent = validated(parent_bundle);
    let candidate = validated(candidate_bundle);
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed Gate C plan");
    let organization = named_row(
        &parent,
        "Organization",
        vec![
            ("tenant_id", uuid(1)),
            ("organization_id", uuid(10)),
            ("name", string("Acme")),
        ],
    );
    let ticket = named_row(
        &parent,
        "Ticket",
        vec![
            ("tenant_id", uuid(1)),
            ("ticket_id", uuid(20)),
            ("organization_id", uuid(10)),
            ("title", string("Migration")),
        ],
    );
    let old_targets = [organization.target().clone(), ticket.target().clone()];
    let mut stage = memory_stage(&parent, vec![organization, ticket]);
    let history = stage.history_witness().clone();

    let report = MigrationCoordinator::apply(&plan, &mut stage).expect("Gate C apply");

    assert_eq!(report.checked_rows(), 2);
    assert_eq!(report.changed_rows(), 2);
    assert_eq!(stage.retained_archive().len(), 2);
    assert_eq!(stage.entities().len(), 2);
    assert_eq!(stage.index_entries().len(), 2);
    assert_eq!(stage.history_witness(), &history);
    assert!(stage.predecessor_writes_retired());
    for row in stage.entities() {
        assert_eq!(row.entity_version(), EntityVersion::new(2).expect("v2"));
        assert!(old_targets.iter().all(|target| target != row.target()));
        assert!(row.fields().fields().iter().any(|(_, value)| {
            matches!(value, CanonicalValue::String(value) if value.as_str() == "global")
        }));
    }
}

#[test]
fn complete_preflight_rejects_occupied_and_duplicate_successor_targets() {
    let (parent, occupied_plan) = redirect_plan();
    let occupied_rows = vec![
        named_row(&parent, "Row", vec![("id", uuid(1)), ("next_id", uuid(2))]),
        named_row(&parent, "Row", vec![("id", uuid(2)), ("next_id", uuid(3))]),
    ];
    let mut occupied = memory_stage(&parent, occupied_rows);
    let occupied_before = occupied.snapshot();
    let finding = MigrationCoordinator::check(&occupied_plan, &occupied).unwrap_err();
    assert_eq!(finding.code(), "RDB-M114");
    assert!(MigrationCoordinator::apply(&occupied_plan, &mut occupied).is_err());
    assert_eq!(occupied.snapshot(), occupied_before);

    let duplicate_rows = vec![
        named_row(&parent, "Row", vec![("id", uuid(4)), ("next_id", uuid(9))]),
        named_row(&parent, "Row", vec![("id", uuid(5)), ("next_id", uuid(9))]),
    ];
    let mut duplicate = memory_stage(&parent, duplicate_rows);
    let duplicate_before = duplicate.snapshot();
    let finding = MigrationCoordinator::check(&occupied_plan, &duplicate).unwrap_err();
    assert_eq!(finding.code(), "RDB-M113");
    assert!(MigrationCoordinator::apply(&occupied_plan, &mut duplicate).is_err());
    assert_eq!(duplicate.snapshot(), duplicate_before);
}

#[test]
fn aggregate_membership_change_rebinds_every_affected_row_once() {
    let parent_source = r#"
contract OwnershipMove version 1 {
  entity Account { key (tenant_id: uuid, owner_id: uuid) }
  entity Other { key (tenant_id: uuid, owner_id: uuid) }
  entity Note { key (tenant_id: uuid, owner_id: uuid, note_id: uuid) }
  aggregate Accounts { root Account child Note partition_by tenant_id conflict_key (tenant_id, owner_id) }
  aggregate Others { root Other partition_by tenant_id conflict_key (tenant_id, owner_id) }
}
"#;
    let candidate_source = r#"
contract OwnershipMove version 2 {
  entity Account { key (tenant_id: uuid, owner_id: uuid) }
  entity Other { key (tenant_id: uuid, owner_id: uuid) }
  entity Note { key (tenant_id: uuid, owner_id: uuid, note_id: uuid) }
  aggregate Accounts { root Account partition_by tenant_id conflict_key (tenant_id, owner_id) }
  aggregate Others { root Other child Note partition_by tenant_id conflict_key (tenant_id, owner_id) }
}
"#;
    let migration_source = r#"
migration OwnershipMove from 1 to 2 {
  acknowledge aggregate Accounts
  acknowledge aggregate Others
}
"#;
    let parent_bundle = compile_contract_source(parent_source).expect("ownership parent");
    let candidate_bundle =
        compile_contract_successor(candidate_source, &parent_bundle).expect("ownership candidate");
    let migration = compile_migration_source(migration_source, &parent_bundle, &candidate_bundle)
        .expect("ownership migration");
    let parent = validated(parent_bundle);
    let candidate = validated(candidate_bundle);
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("ownership plan");
    let rows = vec![
        named_row(
            &parent,
            "Account",
            vec![("tenant_id", uuid(1)), ("owner_id", uuid(2))],
        ),
        named_row(
            &parent,
            "Other",
            vec![("tenant_id", uuid(1)), ("owner_id", uuid(3))],
        ),
        named_row(
            &parent,
            "Note",
            vec![
                ("tenant_id", uuid(1)),
                ("owner_id", uuid(3)),
                ("note_id", uuid(4)),
            ],
        ),
    ];
    let original_targets = rows
        .iter()
        .map(|row| row.target().clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut stage = memory_stage(&parent, rows);

    let report = MigrationCoordinator::apply(&plan, &mut stage).expect("ownership apply");

    assert_eq!(report.checked_rows(), 3);
    assert_eq!(report.changed_rows(), 3);
    assert_eq!(stage.retained_archive().len(), 3);
    assert_eq!(
        stage
            .entities()
            .map(|row| row.target().clone())
            .collect::<std::collections::BTreeSet<_>>(),
        original_targets
    );
    assert!(
        stage
            .entities()
            .all(|row| row.entity_version() == EntityVersion::new(2).expect("version 2"))
    );
}

#[test]
fn interrupted_rekey_resumes_without_old_new_duplicate_or_partial_domain() {
    let parent_bundle = compile_contract_source(PARENT_SOURCE).expect("parent");
    let (candidate_bundle, migration) =
        compile_contract_migration_successor(SUCCESSOR_SOURCE, MIGRATION_SOURCE, &parent_bundle)
            .expect("Gate C artifacts");
    let parent = validated(parent_bundle);
    let candidate = validated(candidate_bundle);
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed Gate C plan");
    let rows = (1..=65)
        .map(|value| {
            named_row(
                &parent,
                "Organization",
                vec![
                    ("tenant_id", uuid(1)),
                    ("organization_id", uuid(value)),
                    ("name", string(&format!("org-{value}"))),
                ],
            )
        })
        .collect();
    let mut stage = memory_stage(&parent, rows);
    let history = stage.history_witness().clone();
    let mut interrupted = InterruptAfterFirstCommittedBatch {
        inner: &mut stage,
        fired: false,
    };

    assert!(MigrationCoordinator::apply(&plan, &mut interrupted).is_err());
    assert_eq!(stage.journal().expect("durable cursor").checked_rows(), 64);
    assert_eq!(stage.retained_archive().len(), 64);
    assert_eq!(stage.entities().len(), 65);

    let report = MigrationCoordinator::apply(&plan, &mut stage).expect("resume Gate C");
    assert_eq!(report.checked_rows(), 65);
    assert_eq!(report.changed_rows(), 65);
    assert_eq!(stage.retained_archive().len(), 65);
    assert_eq!(stage.entities().len(), 65);
    assert_eq!(stage.index_entries().len(), 130);
    assert_eq!(stage.history_witness(), &history);
    let targets = stage
        .entities()
        .map(|row| row.target().clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(targets.len(), 65);
}

fn redirect_plan() -> (ValidatedContractBundle, ValidatedMigrationPlan) {
    let parent_source = r#"
contract RekeyCollision version 1 {
  entity Row { key (id: uuid) field next_id: uuid }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let candidate_source = r#"
contract RekeyCollision version 2 {
  entity Row { key (next_id: uuid) field id: uuid }
  aggregate Rows { root Row partition_by next_id conflict_key (next_id) }
}
"#;
    let migration_source = r#"
migration RekeyCollision from 1 to 2 {
  transform Row { rekey (old.next_id) }
  acknowledge repartition Rows
  acknowledge conflict Rows
}
"#;
    let parent_bundle = compile_contract_source(parent_source).expect("redirect parent");
    let candidate_bundle =
        compile_contract_successor(candidate_source, &parent_bundle).expect("redirect candidate");
    let migration = compile_migration_source(migration_source, &parent_bundle, &candidate_bundle)
        .expect("redirect migration");
    let parent = validated(parent_bundle);
    let candidate = validated(candidate_bundle);
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("redirect plan");
    (parent, plan)
}

struct InterruptAfterFirstCommittedBatch<'a> {
    inner: &'a mut MemoryMigrationStage,
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

    fn retained_migration_entity_count(
        &self,
        migration: riffdb_types::MigrationBundleHash,
        entity_types: &[riffdb_types::EntityTypeId],
    ) -> Result<u64, MigrationStageError> {
        self.inner
            .retained_migration_entity_count(migration, entity_types)
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

fn validated(bundle: riffdb_contract_ir::ContractBundle) -> ValidatedContractBundle {
    ValidatedContractBundle::from_compiler_bundle(bundle).expect("validated bundle")
}

fn memory_stage(
    parent: &ValidatedContractBundle,
    rows: Vec<StoredEntityRecordV1>,
) -> MemoryMigrationStage {
    let history = MemoryMigrationHistoryWitness::new(
        ApplicationSequenceAllocator::initial(),
        b"immutable-gate-c-history".to_vec(),
    )
    .expect("history");
    MemoryMigrationStage::new(parent.to_stored().expect("stored parent"), rows, history)
        .expect("memory stage")
}

fn named_row(
    parent: &ValidatedContractBundle,
    entity_name: &str,
    values: Vec<(&str, CanonicalValue)>,
) -> StoredEntityRecordV1 {
    let entity = parent
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == entity_name)
        .expect("entity");
    let fields = entity
        .record()
        .fields()
        .iter()
        .map(|field| {
            let value = values
                .iter()
                .find_map(|(name, value)| (*name == field.name()).then(|| value.clone()))
                .expect("field value");
            (field.id(), value)
        })
        .collect();
    let record = CanonicalRecord::new(fields).expect("record");
    let key_values = entity
        .primary_key_fields()
        .iter()
        .map(|field| {
            record
                .fields()
                .iter()
                .find_map(|(candidate, value)| (candidate == field).then(|| value.clone()))
                .expect("key field")
        })
        .collect::<Vec<_>>();
    let key = entity
        .primary_key()
        .encode_entity(&key_values)
        .expect("key");
    let target = EntityTarget::new(entity.id(), key).expect("target");
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
    .expect("stored row")
}

fn uuid(value: u64) -> CanonicalValue {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    CanonicalValue::Uuid(bytes)
}

fn string(value: &str) -> CanonicalValue {
    CanonicalValue::string(value).expect("bounded string")
}
