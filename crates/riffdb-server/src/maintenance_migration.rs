//! Private offline contract-migration lifecycle and restart recovery.

use std::fmt;

use riffdb_catalog::{
    MigrationFinding, ValidatedContractBundle, ValidatedMigrationPlan, migration_finding_code,
};
use riffdb_commit::MigrationCoordinator;
use riffdb_contract_ir::MigrationBundleV1;
use riffdb_storage_api::{
    BackupBuildMetadataV1, ContractMigrationOperationKindV1, ContractMigrationReceiptFailureV1,
    ContractMigrationReceiptPhaseV1, ContractMigrationReceiptV1, StartupValidationInputs,
    StorageErrorKind,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbContractMigrationContext, RedbContractMigrationPreflight,
    RedbContractMigrationStage, RedbMaintenanceStorage, RedbStore,
};

use crate::identifiers::DatabaseIdCandidateSource;
use crate::projection_worker::MigrationProjectionBuilder;
use crate::startup::{CheckedRedbStartup, open_redb_startup_with_commit_profile};

/// Inputs already fixed by server startup before an offline migration is driven.
pub(crate) struct MigrationDriverInputs<'a> {
    pub(crate) startup: StartupValidationInputs,
    pub(crate) database_ids: &'a DatabaseIdCandidateSource,
    pub(crate) commit_profile: RedbCommitProfile,
    pub(crate) build: &'a BackupBuildMetadataV1,
}

/// Completes or resumes one accepted migration while the selected database is offline.
pub(crate) fn drive_contract_migration(
    storage: &RedbMaintenanceStorage,
    mut receipt: ContractMigrationReceiptV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    if receipt.current_phase().is_terminal() {
        return Err(MigrationDriverError);
    }
    if receipt.operation_kind() == ContractMigrationOperationKindV1::Check {
        return drive_contract_migration_check(storage, receipt, inputs);
    }
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::RollingBack {
        return finish_rollback(storage, receipt, inputs);
    }
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::ValidatingPublished {
        return finish_publication(storage, receipt, inputs);
    }

    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Publishing {
        match active_bundle(storage, inputs) {
            Ok(active) if active == receipt.artifacts().candidate() => {
                receipt = persist_advance(
                    storage,
                    &receipt,
                    ContractMigrationReceiptPhaseV1::ValidatingPublished,
                )?;
                return finish_publication(storage, receipt, inputs);
            }
            Ok(active) if active == receipt.artifacts().parent() => {}
            _ => return begin_rollback(storage, receipt, inputs),
        }
    }

    let (candidate_bytes, migration_bytes) = storage
        .read_contract_migration_artifacts(receipt.operation_id())
        .map_err(|_| MigrationDriverError)?;

    let parent = open_database(storage, inputs)?;
    let plan = match reconstruct_plan(&parent, &candidate_bytes, &migration_bytes, &receipt) {
        Ok(plan) => plan,
        Err(error)
            if matches!(
                receipt.current_phase(),
                ContractMigrationReceiptPhaseV1::Accepted
                    | ContractMigrationReceiptPhaseV1::Draining
                    | ContractMigrationReceiptPhaseV1::Preflight
            ) =>
        {
            let _ = error;
            return finish_failed_closed(
                storage,
                receipt,
                ContractMigrationReceiptFailureV1::ArtifactMismatch,
                parent,
            );
        }
        Err(error) => return Err(error),
    };

    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Accepted {
        receipt = persist_advance(storage, &receipt, ContractMigrationReceiptPhaseV1::Draining)?;
    }
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Draining {
        receipt = persist_advance(
            storage,
            &receipt,
            ContractMigrationReceiptPhaseV1::Preflight,
        )?;
    }

    let (_, _, history, _, _, ports) = parent.into_parts();
    let active = history.active().ok_or(MigrationDriverError)?.bundle_hash();
    let mut preflight =
        Some(RedbContractMigrationPreflight::new(ports, active).map_err(|_| MigrationDriverError)?);
    let immutable_witness = preflight
        .as_ref()
        .ok_or(MigrationDriverError)?
        .immutable_witness()
        .map_err(|_| MigrationDriverError)?;
    let mut reservation = None;
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Preflight {
        let report = match MigrationCoordinator::check(
            &plan,
            preflight.as_ref().ok_or(MigrationDriverError)?,
        ) {
            Ok(report) => report,
            Err(finding) => {
                drop(preflight.take().ok_or(MigrationDriverError)?.into_ports());
                return fail_closed_and_reopen(
                    storage,
                    receipt,
                    classify_preflight_finding(finding),
                    inputs,
                );
            }
        };
        drop(preflight.take().ok_or(MigrationDriverError)?.into_ports());
        let disk = match storage
            .reserve_contract_migration_disk(receipt.operation_id(), report.semantic_write_bytes())
        {
            Ok(disk) => disk,
            Err(error)
                if matches!(
                    error.kind(),
                    StorageErrorKind::LimitExceeded
                        | StorageErrorKind::SequenceExhausted
                        | StorageErrorKind::HistoryPruned
                ) =>
            {
                return fail_closed_and_reopen(
                    storage,
                    receipt,
                    ContractMigrationReceiptFailureV1::CapacityExhausted,
                    inputs,
                );
            }
            Err(error) if error.kind() == StorageErrorKind::Unavailable => {
                return fail_closed_and_reopen(
                    storage,
                    receipt,
                    ContractMigrationReceiptFailureV1::DiskUnavailable,
                    inputs,
                );
            }
            Err(_) => return Err(MigrationDriverError),
        };
        let (name, _manifest, identity) = storage
            .create_contract_migration_backup(receipt.operation_id(), inputs.build)
            .map_err(|_| MigrationDriverError)?;
        let next = receipt
            .publish_backup(name, identity)
            .map_err(|_| MigrationDriverError)?;
        storage
            .replace_contract_migration_receipt(&receipt, &next)
            .map_err(|_| MigrationDriverError)?;
        receipt = next;
        reservation = Some(disk);
    }
    if let Some(preflight) = preflight {
        drop(preflight.into_ports());
    }

    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::BackupPublished {
        receipt = persist_advance(storage, &receipt, ContractMigrationReceiptPhaseV1::Staging)?;
    }
    let (stage_path, stage_identity) = storage
        .materialize_contract_migration_stage(receipt.operation_id())
        .map_err(|_| MigrationDriverError)?;
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Staging {
        let next = receipt
            .begin_transforming(stage_identity)
            .map_err(|_| MigrationDriverError)?;
        storage
            .replace_contract_migration_receipt(&receipt, &next)
            .map_err(|_| MigrationDriverError)?;
        receipt = next;
    } else if receipt.stage_identity() != Some(stage_identity) {
        return Err(MigrationDriverError);
    }
    if let Some(reservation) = reservation {
        reservation.release().map_err(|_| MigrationDriverError)?;
    }

    let checked_stage = open_redb_startup_with_commit_profile(
        &stage_path,
        inputs.startup.clone(),
        inputs.database_ids,
        inputs.commit_profile,
    );
    match checked_stage {
        Ok(checked_stage) => {
            let (_, _, stage_history, _, _, stage_ports) = checked_stage.into_parts();
            let stage_active = stage_history
                .active()
                .ok_or(MigrationDriverError)?
                .bundle_hash();
            if stage_active == plan.parent_bundle_hash() {
                let context = RedbContractMigrationContext::from_receipt(&receipt)
                    .map_err(|_| MigrationDriverError)?;
                let stage = RedbContractMigrationStage::new(stage_ports, context)
                    .map_err(|_| MigrationDriverError)?;
                apply_stage(&plan, stage)?;
            } else if stage_active == plan.candidate_bundle_hash() {
                drop(stage_ports);
            } else {
                return Err(MigrationDriverError);
            }
        }
        Err(_) => {
            let store = RedbStore::open_with_commit_profile(&stage_path, inputs.commit_profile)
                .map_err(|_| MigrationDriverError)?;
            let context = RedbContractMigrationContext::from_receipt(&receipt)
                .map_err(|_| MigrationDriverError)?;
            let stage = RedbContractMigrationStage::resume(store, immutable_witness, context)
                .map_err(|_| MigrationDriverError)?;
            apply_stage(&plan, stage)?;
        }
    }

    // Cutover changes the stage's active catalog and durable migration graph.
    // A fresh complete startup pass is the publication permit required by
    // MIG-013; it also recognizes a cutover committed before a process crash.
    let validated_stage = open_redb_startup_with_commit_profile(
        &stage_path,
        inputs.startup.clone(),
        inputs.database_ids,
        inputs.commit_profile,
    )
    .map_err(|_| MigrationDriverError)?;
    if validated_stage
        .catalog_history()
        .active()
        .is_none_or(|active| active.bundle_hash() != plan.candidate_bundle_hash())
    {
        return Err(MigrationDriverError);
    }
    drop(validated_stage);

    for phase in [
        ContractMigrationReceiptPhaseV1::RebuildingProjections,
        ContractMigrationReceiptPhaseV1::ValidatingStage,
        ContractMigrationReceiptPhaseV1::Publishing,
    ] {
        if phase_precedes(receipt.current_phase(), phase) {
            receipt = persist_advance(storage, &receipt, phase)?;
        }
    }
    finish_publication(storage, receipt, inputs)
}

fn drive_contract_migration_check(
    storage: &RedbMaintenanceStorage,
    mut receipt: ContractMigrationReceiptV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    let (candidate_bytes, migration_bytes) = storage
        .read_contract_migration_artifacts(receipt.operation_id())
        .map_err(|_| MigrationDriverError)?;
    let parent = open_database(storage, inputs)?;
    let plan = match reconstruct_plan(&parent, &candidate_bytes, &migration_bytes, &receipt) {
        Ok(plan) => plan,
        Err(_) => {
            return finish_failed_closed(
                storage,
                receipt,
                ContractMigrationReceiptFailureV1::ArtifactMismatch,
                parent,
            );
        }
    };
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Accepted {
        receipt = persist_advance(
            storage,
            &receipt,
            ContractMigrationReceiptPhaseV1::Preflight,
        )?;
    }
    if receipt.current_phase() != ContractMigrationReceiptPhaseV1::Preflight {
        return Err(MigrationDriverError);
    }
    let (_, _, history, _, _, ports) = parent.into_parts();
    let active = history.active().ok_or(MigrationDriverError)?.bundle_hash();
    let preflight =
        RedbContractMigrationPreflight::new(ports, active).map_err(|_| MigrationDriverError)?;
    let result = MigrationCoordinator::check(&plan, &preflight);
    drop(preflight.into_ports());
    match result {
        Ok(_) => {
            let succeeded = persist_advance(
                storage,
                &receipt,
                ContractMigrationReceiptPhaseV1::Succeeded,
            )?;
            Ok((succeeded, open_database(storage, inputs)?))
        }
        Err(finding) => fail_closed_and_reopen(
            storage,
            receipt,
            classify_preflight_finding(finding),
            inputs,
        ),
    }
}

fn apply_stage(
    plan: &ValidatedMigrationPlan,
    mut stage: RedbContractMigrationStage,
) -> Result<(), MigrationDriverError> {
    let projection_ports = stage.projection_ports();
    let mut projections = MigrationProjectionBuilder::new(plan, projection_ports);
    MigrationCoordinator::apply_with_projection_builder(plan, &mut stage, &mut projections)
        .map_err(|_| MigrationDriverError)?;
    drop(stage.into_ports());
    Ok(())
}

fn finish_publication(
    storage: &RedbMaintenanceStorage,
    mut receipt: ContractMigrationReceiptV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    if receipt.current_phase() == ContractMigrationReceiptPhaseV1::Publishing {
        let already_published = open_database(storage, inputs).is_ok_and(|startup| {
            startup
                .catalog_history()
                .active()
                .is_some_and(|active| active.bundle_hash() == receipt.artifacts().candidate())
        });
        if !already_published
            && storage
                .publish_contract_migration_stage(receipt.operation_id())
                .is_err()
        {
            match active_bundle(storage, inputs) {
                Ok(active) if active == receipt.artifacts().candidate() => {}
                Ok(active) if active == receipt.artifacts().parent() => {
                    return Err(MigrationDriverError);
                }
                _ => return begin_rollback(storage, receipt, inputs),
            }
        }
        receipt = persist_advance(
            storage,
            &receipt,
            ContractMigrationReceiptPhaseV1::ValidatingPublished,
        )?;
    }
    let published = match open_database(storage, inputs) {
        Ok(startup)
            if startup
                .catalog_history()
                .active()
                .is_some_and(|active| active.bundle_hash() == receipt.artifacts().candidate()) =>
        {
            startup
        }
        _ => {
            return begin_rollback(storage, receipt, inputs);
        }
    };
    let succeeded = persist_advance(
        storage,
        &receipt,
        ContractMigrationReceiptPhaseV1::Succeeded,
    )?;
    Ok((succeeded, published))
}

fn begin_rollback(
    storage: &RedbMaintenanceStorage,
    receipt: ContractMigrationReceiptV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    let rolling = receipt
        .advance(ContractMigrationReceiptPhaseV1::RollingBack)
        .map_err(|_| MigrationDriverError)?;
    storage
        .replace_contract_migration_receipt(&receipt, &rolling)
        .map_err(|_| MigrationDriverError)?;
    finish_rollback(storage, rolling, inputs)
}

fn finish_rollback(
    storage: &RedbMaintenanceStorage,
    receipt: ContractMigrationReceiptV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    storage
        .rollback_contract_migration(receipt.operation_id())
        .map_err(|_| MigrationDriverError)?;
    let restored = open_database(storage, inputs)?;
    if restored
        .catalog_history()
        .active()
        .is_none_or(|active| active.bundle_hash() != receipt.artifacts().parent())
    {
        return Err(MigrationDriverError);
    }
    let failed = receipt
        .fail(
            ContractMigrationReceiptPhaseV1::FailedRolledBack,
            ContractMigrationReceiptFailureV1::PublishedValidationFailed,
        )
        .map_err(|_| MigrationDriverError)?;
    storage
        .replace_contract_migration_receipt(&receipt, &failed)
        .map_err(|_| MigrationDriverError)?;
    Ok((failed, restored))
}

fn fail_closed_and_reopen(
    storage: &RedbMaintenanceStorage,
    receipt: ContractMigrationReceiptV1,
    failure: ContractMigrationReceiptFailureV1,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    let predecessor = open_database(storage, inputs)?;
    finish_failed_closed(storage, receipt, failure, predecessor)
}

fn finish_failed_closed(
    storage: &RedbMaintenanceStorage,
    receipt: ContractMigrationReceiptV1,
    failure: ContractMigrationReceiptFailureV1,
    predecessor: CheckedRedbStartup,
) -> Result<(ContractMigrationReceiptV1, CheckedRedbStartup), MigrationDriverError> {
    if predecessor
        .catalog_history()
        .active()
        .is_none_or(|active| active.bundle_hash() != receipt.artifacts().parent())
    {
        return Err(MigrationDriverError);
    }
    let failed = receipt
        .fail(ContractMigrationReceiptPhaseV1::FailedClosed, failure)
        .map_err(|_| MigrationDriverError)?;
    storage
        .replace_contract_migration_receipt(&receipt, &failed)
        .map_err(|_| MigrationDriverError)?;
    Ok((failed, predecessor))
}

pub(crate) fn classify_preflight_finding(
    finding: MigrationFinding,
) -> ContractMigrationReceiptFailureV1 {
    match finding.code() {
        migration_finding_code::ARTIFACT_MISMATCH | migration_finding_code::UNSUPPORTED_STEP => {
            ContractMigrationReceiptFailureV1::ArtifactMismatch
        }
        migration_finding_code::ENTITY_VERSION_EXHAUSTED
        | migration_finding_code::RESOURCE_LIMIT => {
            ContractMigrationReceiptFailureV1::CapacityExhausted
        }
        migration_finding_code::PENDING_ADMISSION => {
            ContractMigrationReceiptFailureV1::PendingAdmission
        }
        migration_finding_code::INVALID_ROW
        | migration_finding_code::TRANSFORM_ARITHMETIC
        | migration_finding_code::INVARIANT_REJECTED
        | migration_finding_code::INDEX_INVALID
        | migration_finding_code::RELATIONSHIP_MISSING
        | migration_finding_code::UNIQUE_CONFLICT
        | migration_finding_code::ROW_CHANGED
        | migration_finding_code::INTEGRITY => {
            ContractMigrationReceiptFailureV1::InvalidPredecessor
        }
        _ => ContractMigrationReceiptFailureV1::InvalidPredecessor,
    }
}

fn reconstruct_plan(
    parent: &CheckedRedbStartup,
    candidate_bytes: &[u8],
    migration_bytes: &[u8],
    receipt: &ContractMigrationReceiptV1,
) -> Result<ValidatedMigrationPlan, MigrationDriverError> {
    let candidate =
        ValidatedContractBundle::decode(candidate_bytes).map_err(|_| MigrationDriverError)?;
    let migration = MigrationBundleV1::decode(migration_bytes).map_err(|_| MigrationDriverError)?;
    let lineage = parent
        .catalog_history()
        .active_lineage_bundles()
        .ok_or(MigrationDriverError)?;
    let plan = ValidatedMigrationPlan::from_lineage_artifacts(lineage, candidate, migration)
        .map_err(|_| MigrationDriverError)?;
    if plan.parent_bundle_hash() != receipt.artifacts().parent()
        || plan.candidate_bundle_hash() != receipt.artifacts().candidate()
        || plan.migration_bundle_hash() != receipt.artifacts().migration()
    {
        return Err(MigrationDriverError);
    }
    Ok(plan)
}

fn open_database(
    storage: &RedbMaintenanceStorage,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<CheckedRedbStartup, MigrationDriverError> {
    open_redb_startup_with_commit_profile(
        storage.configured_database_file(),
        inputs.startup.clone(),
        inputs.database_ids,
        inputs.commit_profile,
    )
    .map_err(|_| MigrationDriverError)
}

fn active_bundle(
    storage: &RedbMaintenanceStorage,
    inputs: &MigrationDriverInputs<'_>,
) -> Result<riffdb_types::ContractBundleHash, MigrationDriverError> {
    open_database(storage, inputs)?
        .catalog_history()
        .active()
        .map(|active| active.bundle_hash())
        .ok_or(MigrationDriverError)
}

fn persist_advance(
    storage: &RedbMaintenanceStorage,
    current: &ContractMigrationReceiptV1,
    phase: ContractMigrationReceiptPhaseV1,
) -> Result<ContractMigrationReceiptV1, MigrationDriverError> {
    let next = current.advance(phase).map_err(|_| MigrationDriverError)?;
    storage
        .replace_contract_migration_receipt(current, &next)
        .map_err(|_| MigrationDriverError)?;
    Ok(next)
}

fn phase_precedes(
    current: ContractMigrationReceiptPhaseV1,
    candidate: ContractMigrationReceiptPhaseV1,
) -> bool {
    phase_ordinal(current) < phase_ordinal(candidate)
}

const fn phase_ordinal(phase: ContractMigrationReceiptPhaseV1) -> u8 {
    use ContractMigrationReceiptPhaseV1 as P;
    match phase {
        P::Accepted => 1,
        P::Draining => 2,
        P::Preflight => 3,
        P::BackupPublished => 4,
        P::Staging => 5,
        P::Transforming => 6,
        P::RebuildingProjections => 7,
        P::ValidatingStage => 8,
        P::Publishing => 9,
        P::ValidatingPublished => 10,
        P::RollingBack => 11,
        P::Succeeded | P::FailedClosed | P::FailedRolledBack => u8::MAX,
    }
}

/// Closed internal driver failure; public mapping is owned by WP-409.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MigrationDriverError;

impl fmt::Display for MigrationDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("contract migration lifecycle failed")
    }
}

impl std::error::Error for MigrationDriverError {}
