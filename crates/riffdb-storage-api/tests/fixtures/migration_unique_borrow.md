```compile_fail
use riffdb_storage_api::{MigrationStagePort, MigrationBatch, ContractMigrationArtifactsV1};
fn mutate_while_observing(stage: &mut impl MigrationStagePort, artifacts: ContractMigrationArtifactsV1, batch: MigrationBatch) {
    let reader = stage.migration_unique_validation(artifacts).unwrap();
    stage.apply_migration_batch(batch).unwrap();
    drop(reader); // the observation must end before the exclusive mutation
}
```
