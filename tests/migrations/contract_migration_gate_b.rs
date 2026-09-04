#![forbid(unsafe_code)]
// req: DEP-005

//! Gate-B structural migration acceptance evidence.

use riffdb_catalog::{ValidatedContractBundle, ValidatedMigrationPlan};
use riffdb_commit::MigrationCoordinator;
use riffdb_contract_compiler::{
    compile_contract_migration_successor, compile_contract_source, compile_contract_successor,
    compile_migration_source,
};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityTarget, MigrationBatch, MigrationCutover,
    MigrationCutoverApplied, MigrationJournalState, MigrationScanCursor, MigrationScanPage,
    MigrationStageError, MigrationStagePort, StoredEntityRecordV1,
};
use riffdb_storage_redb::RedbMigrationStageFixture;
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
    let mut stage = redb_stage(&plan, vec![row]);
    let history = *stage.history_witness();
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
    let mut stage = redb_stage(&plan, vec![row.clone()]);
    let history = *stage.history_witness();

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
    let mut stage = redb_stage(&plan, vec![invalid.clone()]);
    let history = *stage.history_witness();

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
    let mut stage = redb_stage(&plan, rows);
    let history = *stage.history_witness();
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

/// Gate B's successor declares a covering index, so its migration is the one
/// standing gate in which a rebuild derives a cover over real rows.
///
/// SPEC.md requires the coordinator to derive the exact canonical covered
/// record from the transaction-current entity post-image, and states that
/// missing coverage is corruption that MUST fail closed without entity-read
/// fallback. A rebuilt entry with an empty or partial cover is therefore not a
/// degraded row — it is a row the covered read rejects outright, on a database
/// that reports healthy at startup.
///
/// The cover is asserted against the successor schema's own declaration rather
/// than a literal, so it stays correct if the fixture's cover list changes.
#[test]
fn the_rebuilt_covering_index_carries_the_complete_cover_from_the_post_image() {
    let parent_bundle = compile_contract_source(STRUCTURAL_PARENT).expect("parent");
    let (candidate_bundle, migration) = compile_contract_migration_successor(
        STRUCTURAL_SUCCESSOR,
        STRUCTURAL_MIGRATION,
        &parent_bundle,
    )
    .expect("structural successor");

    let candidate_entity = &candidate_bundle.schema().entities()[0];
    let covering = candidate_entity
        .indexes()
        .iter()
        .find(|index| !index.cover_fields().is_empty())
        .expect(
            "the Gate-B successor must declare a covering index; without one this gate \
             exercises no cover derivation at all",
        );
    let covering_id = covering.id();
    let mut declared_cover = covering.cover_fields().to_vec();
    declared_cover.sort_unstable();
    let amount = candidate_entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "amount")
        .expect("replacement field")
        .id();
    assert!(
        declared_cover.contains(&amount),
        "the covering index must cover the field the transform rewrites, so a cover \
         derived from the pre-image rather than the post-image is also caught"
    );
    assert!(
        !parent_bundle.schema().entities()[0]
            .indexes()
            .iter()
            .any(|index| index.id() == covering_id),
        "the covering index must be a new identity introduced by the successor"
    );

    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate.clone(), migration)
        .expect("sealed covering-index plan");
    let row = named_row(
        &parent,
        "Row",
        vec![
            ("id", CanonicalValue::Uuid(uuid_for(1))),
            ("value", CanonicalValue::I64(7)),
            ("status", enum_value(&parent, "WorkflowStatus", "Closed")),
        ],
    );
    let mut stage = redb_stage(&plan, vec![row]);
    MigrationCoordinator::apply(&plan, &mut stage).expect("covering-index migration applies");

    let rebuilt = stage
        .index_entries()
        .iter()
        .find(|entry| entry.key().index_id() == covering_id)
        .expect("the migration must rebuild the newly declared covering index");
    let stored_cover = rebuilt
        .covered_values()
        .fields()
        .iter()
        .map(|(field, _)| *field)
        .collect::<Vec<_>>();
    assert_eq!(
        stored_cover, declared_cover,
        "a rebuilt covering-index entry must carry the exact declared cover; an empty \
         or partial cover passes every startup structural check and is then read as \
         corruption by the covered plan"
    );

    // The cover must come from the post-image the index key came from: the
    // transform replaced i64 `value` with u64 `amount`, so a cover taken from
    // the pre-image would carry the retired field or the wrong value.
    let migrated = stage.entities().next().expect("migrated row");
    for (field, value) in rebuilt.covered_values().fields() {
        let post_image = migrated
            .fields()
            .fields()
            .iter()
            .find_map(|(candidate, value)| (candidate == field).then_some(value))
            .expect("every covered field is a direct field of the migrated post-image");
        assert_eq!(
            value, post_image,
            "covered values must be derived from the successor post-image"
        );
    }
    assert_eq!(
        rebuilt
            .covered_values()
            .fields()
            .iter()
            .find_map(|(field, value)| (*field == amount).then_some(value)),
        Some(&CanonicalValue::U64(7)),
        "the covered value must be the converted successor value, not the retired one"
    );
    assert_eq!(
        rebuilt.schema_binding().bundle_hash(),
        candidate.bundle_hash(),
        "the rebuilt covering entry binds to the successor contract"
    );
}
