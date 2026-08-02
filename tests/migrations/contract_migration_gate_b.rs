#![forbid(unsafe_code)]

//! Gate-B structural migration acceptance evidence.

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

const STRUCTURAL_PARENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/parent.riff"
));
const STRUCTURAL_SUCCESSOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/successor.riff"
));
const STRUCTURAL_MIGRATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/v1-to-v2.riffm"
));
const STRUCTURAL_PARENT_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/parent.contract.bundle"
));
const STRUCTURAL_SUCCESSOR_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/successor.contract.bundle"
));
const STRUCTURAL_MIGRATION_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/v1-to-v2.migration.bundle"
));

#[test]
fn rename_replacement_and_enum_map_preserve_identity_and_history() {
    let parent_bundle = compile_contract_source(STRUCTURAL_PARENT).expect("parent");
    let parent_entity = parent_bundle.schema().entities()[0].id();
    let old_value = parent_bundle.schema().entities()[0]
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "value")
        .expect("old value")
        .id();
    let (candidate_bundle, migration) = compile_contract_migration_successor(
        STRUCTURAL_SUCCESSOR,
        STRUCTURAL_MIGRATION,
        &parent_bundle,
    )
    .expect("structural successor");
    assert_eq!(parent_bundle.canonical_bytes(), STRUCTURAL_PARENT_BUNDLE);
    assert_eq!(
        candidate_bundle.canonical_bytes(),
        STRUCTURAL_SUCCESSOR_BUNDLE
    );
    assert_eq!(migration.canonical_bytes(), STRUCTURAL_MIGRATION_BUNDLE);
    let candidate_entity = &candidate_bundle.schema().entities()[0];
    assert_eq!(candidate_entity.id(), parent_entity);
    assert_eq!(candidate_entity.name(), "Record");
    assert!(
        candidate_bundle
            .ledger()
            .aliases()
            .iter()
            .any(|alias| { alias.identity().name() == "Row" && alias.id() == parent_entity.get() })
    );
    let amount = candidate_entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "amount")
        .expect("replacement field")
        .id();
    assert_ne!(amount, old_value);

    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed Gate-B plan");
    let row = named_row(
        &parent,
        "Row",
        vec![
            ("id", CanonicalValue::Uuid(uuid_for(1))),
            ("value", CanonicalValue::I64(7)),
            ("status", enum_value(&parent, "WorkflowStatus", "Closed")),
        ],
    );
    let mut stage = memory_stage(&parent, vec![row]);
    let history = stage.history_witness().clone();
    let applied = MigrationCoordinator::apply(&plan, &mut stage).expect("Gate-B apply");

    assert_eq!(applied.checked_rows(), 1);
    assert_eq!(applied.changed_rows(), 1);
    assert_eq!(stage.retained_archive().len(), 1);
    assert_eq!(stage.history_witness(), &history);
    let migrated = stage.entities().next().expect("migrated row");
    assert_eq!(
        migrated.entity_version(),
        EntityVersion::new(2).expect("version 2")
    );
    assert_eq!(
        migrated
            .fields()
            .fields()
            .iter()
            .find(|(field, _)| *field == amount)
            .map(|(_, value)| value),
        Some(&CanonicalValue::U64(7))
    );
    assert!(
        migrated
            .fields()
            .fields()
            .iter()
            .all(|(field, _)| *field != old_value)
    );
    let archived = plan.candidate().bundle().schema().enums()[0]
        .variants()
        .iter()
        .find(|variant| variant.name() == "Archived")
        .expect("archived variant")
        .id();
    assert!(migrated.fields().fields().iter().any(|(_, value)| matches!(
        value,
        CanonicalValue::Enum { variant_id, .. } if *variant_id == archived
    )));
}

#[test]
fn entity_retirement_archives_data_without_reinterpreting_history() {
    let parent_bundle = compile_contract_source(
        "contract RetiredRows version 1 { entity Row { key (id: uuid) } aggregate Rows { root Row partition_by id conflict_key (id) } }",
    )
    .expect("retirement parent");
    let candidate_bundle =
        compile_contract_successor("contract RetiredRows version 2 {}", &parent_bundle)
            .expect("retirement candidate");
    let migration = compile_migration_source(
        "migration RetiredRows from 1 to 2 { retire entity Row }",
        &parent_bundle,
        &candidate_bundle,
    )
    .expect("retirement proof");
    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("retirement plan");
    let row = named_row(
        &parent,
        "Row",
        vec![("id", CanonicalValue::Uuid(uuid_for(2)))],
    );
    let mut stage = memory_stage(&parent, vec![row.clone()]);
    let history = stage.history_witness().clone();

    MigrationCoordinator::apply(&plan, &mut stage).expect("retirement apply");
    assert_eq!(stage.entities().len(), 0);
    assert_eq!(stage.retained_archive(), &[row]);
    assert_eq!(stage.history_witness(), &history);
}

#[test]
fn failed_checked_conversion_leaves_the_complete_stage_unchanged() {
    let parent_bundle = compile_contract_source(STRUCTURAL_PARENT).expect("parent");
    let (candidate_bundle, migration) = compile_contract_migration_successor(
        STRUCTURAL_SUCCESSOR,
        STRUCTURAL_MIGRATION,
        &parent_bundle,
    )
    .expect("structural successor");
    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed Gate-B plan");
    let invalid = named_row(
        &parent,
        "Row",
        vec![
            ("id", CanonicalValue::Uuid(uuid_for(3))),
            ("value", CanonicalValue::I64(-1)),
            ("status", enum_value(&parent, "WorkflowStatus", "Open")),
        ],
    );
    let mut stage = memory_stage(&parent, vec![invalid.clone()]);
    let history = stage.history_witness().clone();

    assert!(MigrationCoordinator::check(&plan, &stage).is_err());
    assert!(MigrationCoordinator::apply(&plan, &mut stage).is_err());
    assert_eq!(stage.entities().collect::<Vec<_>>(), vec![&invalid]);
    assert!(stage.retained_archive().is_empty());
    assert_eq!(stage.history_witness(), &history);
}

#[test]
fn interrupted_entity_retirement_resumes_without_skip_or_duplicate() {
    let parent_bundle = compile_contract_source(
        "contract RetiredRows version 1 { entity Row { key (id: uuid) } aggregate Rows { root Row partition_by id conflict_key (id) } }",
    )
    .expect("retirement parent");
    let candidate_bundle =
        compile_contract_successor("contract RetiredRows version 2 {}", &parent_bundle)
            .expect("retirement candidate");
    let migration = compile_migration_source(
        "migration RetiredRows from 1 to 2 { retire entity Row }",
        &parent_bundle,
        &candidate_bundle,
    )
    .expect("retirement proof");
    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("retirement plan");
    let rows = (10..75)
        .map(|value| {
            named_row(
                &parent,
                "Row",
                vec![("id", CanonicalValue::Uuid(uuid_for(value)))],
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
    assert_eq!(stage.entities().len(), 1);

    let resumed = MigrationCoordinator::apply(&plan, &mut stage).expect("resume retirement");
    assert_eq!(resumed.checked_rows(), 65);
    assert_eq!(resumed.changed_rows(), 65);
    assert_eq!(resumed.batch_count(), 2);
    assert_eq!(stage.retained_archive().len(), 65);
    assert_eq!(stage.entities().len(), 0);
    assert_eq!(stage.history_witness(), &history);
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

fn memory_stage(
    parent: &ValidatedContractBundle,
    rows: Vec<StoredEntityRecordV1>,
) -> MemoryMigrationStage {
    let history = MemoryMigrationHistoryWitness::new(
        ApplicationSequenceAllocator::initial(),
        b"immutable-gate-b-history".to_vec(),
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

fn enum_value(
    parent: &ValidatedContractBundle,
    enum_name: &str,
    variant_name: &str,
) -> CanonicalValue {
    let enumeration = parent
        .bundle()
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == enum_name)
        .expect("enum");
    let variant = enumeration
        .variants()
        .iter()
        .find(|variant| variant.name() == variant_name)
        .expect("variant");
    CanonicalValue::Enum {
        type_id: enumeration.id(),
        variant_id: variant.id(),
    }
}

fn uuid_for(value: u64) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}
