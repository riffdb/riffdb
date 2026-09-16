// req: MIG-013
use super::*;
use riffdb_catalog::{ValidatedContractBundle, ValidatedMigrationPlan};
use riffdb_contract_compiler::{
    compile_contract_source, compile_contract_successor, compile_migration_source,
};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityTarget, MigrationRowEvidence, MigrationRowMutation,
    MigrationUniqueValidation,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, EntityVersion};
use std::cell::Cell;

fn fixture(count: usize, duplicate: bool) -> (ValidatedMigrationPlan, RedbMigrationStageFixture) {
    let source = "contract UniqueStage version 1 { entity Row { key (tenant: i64, id: i64) field amount: i64 } aggregate Rows { root Row partition_by tenant conflict_key (tenant, id) } }";
    let parent = compile_contract_source(source).unwrap();
    let next = source.replace("version 1", "version 2").replace(
        "field amount: i64",
        "field amount: i64 unique amount_key (tenant, amount)",
    );
    let candidate = compile_contract_successor(&next, &parent).unwrap();
    let migration =
        compile_migration_source("migration UniqueStage from 1 to 2 {}", &parent, &candidate)
            .unwrap();
    let parent = ValidatedContractBundle::from_compiler_bundle(parent).unwrap();
    let candidate = ValidatedContractBundle::from_compiler_bundle(candidate).unwrap();
    let plan =
        ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration).unwrap();
    let entity = &parent.bundle().schema().entities()[0];
    let rows = (0..count)
        .map(|n| {
            let id = i64::try_from(n).unwrap();
            let values = entity
                .record()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        CanonicalValue::I64(if field.name() == "tenant" {
                            0
                        } else if field.name() == "id" || !duplicate {
                            id
                        } else {
                            0
                        }),
                    )
                })
                .collect();
            StoredEntityRecordV1::new(
                EntityTarget::new(
                    entity.id(),
                    entity
                        .primary_key()
                        .encode_entity(&[CanonicalValue::I64(0), CanonicalValue::I64(id)])
                        .unwrap(),
                )
                .unwrap(),
                EntityVersion::first(),
                parent.contract_version(),
                DurableKeySchemaBindingV1::new(
                    parent.lineage().clone(),
                    parent.contract_version(),
                    parent.bundle_hash(),
                ),
                CanonicalRecord::new(values).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut fixture = RedbMigrationStageFixture::create(
        &parent.to_stored().unwrap(),
        plan.candidate_bundle_hash(),
        plan.migration_bundle_hash(),
        rows.clone(),
    )
    .unwrap();
    let mut checked = 0;
    for chunk in rows.chunks(64) {
        let mutations = chunk
            .iter()
            .map(|row| {
                let prepared = plan.prepare_row(row.clone()).unwrap();
                MigrationRowMutation::new(
                    MigrationRowEvidence::from_source(row.clone()),
                    prepared.post_image().cloned(),
                    prepared.rebuilt_indexes().to_vec(),
                )
                .unwrap()
            })
            .collect();
        checked += chunk.len() as u64;
        fixture
            .stage
            .apply_migration_batch(
                MigrationBatch::new(
                    plan.migration_bundle_hash(),
                    mutations,
                    MigrationScanCursor::after(chunk.last().unwrap().target().clone()),
                    checked,
                    0,
                )
                .unwrap(),
            )
            .unwrap();
    }
    if rows.is_empty() {
        fixture
            .stage
            .apply_migration_batch(
                MigrationBatch::new(
                    plan.migration_bundle_hash(),
                    vec![],
                    MigrationScanCursor::start(),
                    0,
                    0,
                )
                .unwrap(),
            )
            .unwrap();
    }
    fixture
        .stage
        .checkpoint_migration_step(ContractMigrationJournalStepV1::RebuildingProjections, &[])
        .unwrap();
    fixture
        .stage
        .checkpoint_migration_step(ContractMigrationJournalStepV1::Validating, &[])
        .unwrap();
    (plan, fixture)
}

struct Counting<'a> {
    stage: &'a RedbContractMigrationStage,
    rows: Cell<usize>,
    observations: Cell<usize>,
    bounded: bool,
}
struct CountingReader<'a> {
    reader: Box<dyn MigrationUniqueValidation + 'a>,
    count: &'a Cell<usize>,
}
impl MigrationUniqueValidation for CountingReader<'_> {
    fn validate_unique_owner(
        &self,
        entity: riffdb_types::EntityTypeId,
        prefix: &riffdb_storage_api::StructurallyDecodedIndexRangePrefixV1,
        expected: &StoredIndexEntryV2,
    ) -> Result<(), MigrationStageError> {
        self.count.set(self.count.get() + 1);
        self.reader.validate_unique_owner(entity, prefix, expected)
    }
}
impl MigrationStagePort for Counting<'_> {
    fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
        self.stage.active_bundle_hash()
    }
    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError> {
        let page = self.stage.scan_migration_rows(cursor)?;
        self.rows.set(self.rows.get() + page.rows().len());
        Ok(page)
    }
    fn migration_unique_validation(
        &self,
        artifacts: ContractMigrationArtifactsV1,
    ) -> Result<Option<Box<dyn MigrationUniqueValidation + '_>>, MigrationStageError> {
        if !self.bounded {
            return Ok(None);
        }
        Ok(self
            .stage
            .migration_unique_validation(artifacts)?
            .map(|reader| {
                Box::new(CountingReader {
                    reader,
                    count: &self.observations,
                }) as Box<dyn MigrationUniqueValidation>
            }))
    }
    fn migration_target_exists(&self, target: &EntityTarget) -> Result<bool, MigrationStageError> {
        self.stage.migration_target_exists(target)
    }
    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
        self.stage.has_unresolved_retiring_admissions()
    }
    fn apply_migration_batch(&mut self, _: MigrationBatch) -> Result<(), MigrationStageError> {
        unreachable!()
    }
    fn build_migration_projection_candidates(
        &mut self,
        _: &[ProjectionId],
    ) -> Result<(), MigrationStageError> {
        unreachable!()
    }
    fn validate_migration_stage(
        &self,
        candidate: riffdb_types::ContractBundleHash,
        lineage: &[riffdb_types::ContractBundleHash],
    ) -> Result<(), MigrationStageError> {
        self.stage.validate_migration_stage(candidate, lineage)
    }
    fn finalize_migration(
        &mut self,
        _: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError> {
        unreachable!()
    }
}

#[test]
fn bounded_unique_validation_scans_each_row_once_at_scale() {
    for n in [0, 1_000, 10_000] {
        let (plan, fixture) = fixture(n, false);
        let stage = Counting {
            stage: &fixture.stage,
            rows: Cell::new(0),
            observations: Cell::new(0),
            bounded: true,
        };
        let proof = plan.validate_successor_stage(&stage, n as u64).unwrap();
        assert_eq!(proof.checked_rows(), n as u64);
        assert_eq!(stage.rows.get(), n);
        assert_eq!(stage.observations.get(), n);
    }
}

#[test]
fn bounded_unique_validation_matches_independent_oracle() {
    for duplicate in [false, true] {
        let (plan, fixture) = fixture(3, duplicate);
        let stage = Counting {
            stage: &fixture.stage,
            rows: Cell::new(0),
            observations: Cell::new(0),
            bounded: false,
        };
        let old = plan.validate_successor_stage(&stage, 3);
        let new = plan.validate_successor_stage(&fixture.stage, 3);
        match (old, new) {
            (Ok(old), Ok(new)) => assert_eq!(old.validation_digest(), new.validation_digest()),
            (Err(old), Err(new)) => assert_eq!(old.code(), new.code()),
            _ => panic!("bounded validation differs from independent oracle"),
        }
    }
}

#[test]
fn missing_duplicate_owner_cannot_hide_duplicate_and_wrong_binding_is_rejected() {
    for delete_first in [false, true] {
        let (plan, fixture) = fixture(2, true);
        let transaction = fixture.stage.ports.shared.database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(SECONDARY_INDEXES).unwrap();
            let keys = table
                .iter()
                .unwrap()
                .map(|row| row.unwrap().0.value().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(keys.len(), 2);
            table
                .remove(keys[usize::from(delete_first)].as_slice())
                .unwrap();
        }
        transaction.commit().unwrap();
        assert!(plan.validate_successor_stage(&fixture.stage, 2).is_err());
        let bad = ContractMigrationArtifactsV1::new(
            plan.parent_bundle_hash(),
            plan.parent_bundle_hash(),
            plan.migration_bundle_hash(),
        );
        assert!(fixture.stage.migration_unique_validation(bad).is_err());
    }
}

#[test]
fn malformed_cold_index_and_wrong_cover_fail_final_validation() {
    for malformed in [false, true] {
        let (plan, fixture) = fixture(1, false);
        let transaction = fixture.stage.ports.shared.database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(SECONDARY_INDEXES).unwrap();
            let (key, bytes) = {
                let row = table.first().unwrap().unwrap();
                (row.0.value().to_vec(), row.1.value().to_vec())
            };
            let replacement = if malformed {
                vec![0xff]
            } else {
                let decoded = crate::codec::decode_index_entry_v2(&bytes).unwrap();
                let entry = decoded.value();
                let wrong = StoredIndexEntryV2::new(
                    entry.key().clone(),
                    entry.schema_binding().clone(),
                    CanonicalRecord::new(vec![(
                        riffdb_types::FieldId::new(999).unwrap(),
                        CanonicalValue::I64(8),
                    )])
                    .unwrap(),
                    entry.partition_key().clone(),
                )
                .unwrap();
                encode_index_entry_v2(&wrong).unwrap().as_bytes().to_vec()
            };
            table
                .insert(key.as_slice(), replacement.as_slice())
                .unwrap();
        }
        transaction.commit().unwrap();
        assert!(plan.validate_successor_stage(&fixture.stage, 1).is_err());
    }
}

#[test]
fn restart_revalidates_missing_index_even_after_a_successful_final_proof() {
    for ready_for_cutover in [false, true] {
        let (plan, mut fixture) = fixture(2, false);
        let original = plan.validate_successor_stage(&fixture.stage, 2).unwrap();
        assert_eq!(original.checked_rows(), 2);
        if ready_for_cutover {
            fixture
                .stage
                .checkpoint_migration_step(ContractMigrationJournalStepV1::ReadyForCutover, &[])
                .unwrap();
        }
        let transaction = fixture.stage.ports.shared.database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(SECONDARY_INDEXES).unwrap();
            let key = table.first().unwrap().unwrap().0.value().to_vec();
            table.remove(key.as_slice()).unwrap();
        }
        transaction.commit().unwrap();
        let RedbContractMigrationStage {
            ports,
            context,
            immutable_history_digest,
            immutable_v3_history,
        } = fixture.stage;
        let witness = RedbContractMigrationImmutableWitness {
            database_id: context.backup_manifest.database_id(),
            parent: context.artifacts.parent(),
            digest: immutable_history_digest,
            v3_history: immutable_v3_history,
        };
        drop(ports);
        let reopened = RedbStore::open(fixture._scope.path().join("db.redb")).unwrap();
        let stage = RedbContractMigrationStage::resume(reopened, witness, context).unwrap();
        assert!(
            plan.validate_successor_stage(&stage, 2).is_err(),
            "an earlier proof cannot survive restart and conceal a missing owner"
        );
    }
}
