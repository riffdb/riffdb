//! Sole authoritative orchestration for checked contract migrations.

use riffdb_catalog::{
    MigrationFinding, MigrationUniqueFact, PreparedMigrationRow, ValidatedMigrationPlan,
};
use riffdb_projection::MigrationProjectionCandidates;
pub use riffdb_projection::{
    MigrationProjectionBuildObservation, MigrationProjectionBuildPort, MigrationProjectionError,
};
use riffdb_storage_api::{
    ContractMigrationJournalStepV1, MAX_MIGRATION_BATCH_MUTATIONS, MAX_MIGRATION_BATCH_WRITE_BYTES,
    MigrationBatch, MigrationCutover, MigrationRowEvidence, MigrationRowMutation,
    MigrationScanCursor, MigrationStageError, MigrationStagePort,
};

/// Complete read-only migration preflight counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCheckReport {
    checked_rows: u64,
    changed_rows: u64,
    scan_pages: u64,
    semantic_write_bytes: u64,
}

impl MigrationCheckReport {
    /// Returns all authoritative rows inspected.
    #[must_use]
    pub const fn checked_rows(self) -> u64 {
        self.checked_rows
    }

    /// Returns rows whose authoritative image will change.
    #[must_use]
    pub const fn changed_rows(self) -> u64 {
        self.changed_rows
    }

    /// Returns bounded scan pages consumed.
    #[must_use]
    pub const fn scan_pages(self) -> u64 {
        self.scan_pages
    }

    /// Returns conservative semantic bytes for all row and index post-images.
    #[must_use]
    pub const fn semantic_write_bytes(self) -> u64 {
        self.semantic_write_bytes
    }
}

/// Successful memory-model migration evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationApplyReport {
    check: MigrationCheckReport,
    batch_count: u64,
    administration_sequence: riffdb_types::AdministrationSequence,
}

impl MigrationApplyReport {
    /// Returns all rows inspected by the mandatory repeated preflight.
    #[must_use]
    pub const fn checked_rows(self) -> u64 {
        self.check.checked_rows
    }

    /// Returns all entity rows changed exactly once.
    #[must_use]
    pub const fn changed_rows(self) -> u64 {
        self.check.changed_rows
    }

    /// Returns bounded pages consumed by preflight.
    #[must_use]
    pub const fn scan_pages(self) -> u64 {
        self.check.scan_pages
    }

    /// Returns atomic journaled row batches applied to the private stage.
    #[must_use]
    pub const fn batch_count(self) -> u64 {
        self.batch_count
    }

    /// Returns the sole sequence assigned by successful final cutover.
    #[must_use]
    pub const fn administration_sequence(self) -> riffdb_types::AdministrationSequence {
        self.administration_sequence
    }
}

/// Commit-owned migration authority.
pub struct MigrationCoordinator;

impl MigrationCoordinator {
    /// Performs complete bounded read-only preflight without stage mutation.
    pub fn check<S: MigrationStagePort>(
        plan: &ValidatedMigrationPlan,
        stage: &S,
    ) -> Result<MigrationCheckReport, MigrationFinding> {
        validate_stage_identity(plan, stage)?;
        if stage
            .has_unresolved_retiring_admissions()
            .map_err(MigrationFinding::from_stage_error)?
        {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::PendingAdmission,
            ));
        }

        let mut cursor = MigrationScanCursor::start();
        let mut checked_rows = 0_u64;
        let mut changed_rows = 0_u64;
        let mut scan_pages = 0_u64;
        let mut semantic_write_bytes = 0_u64;
        loop {
            let page = stage
                .scan_migration_rows(&cursor)
                .map_err(MigrationFinding::from_stage_error)?;
            validate_page_progress(&cursor, &page)?;
            if !page.rows().is_empty() {
                scan_pages = checked_add(scan_pages, 1)?;
            }
            for row in page.rows() {
                let prepared = plan.prepare_row(row.clone())?;
                checked_rows = checked_add(checked_rows, 1)?;
                if prepared.post_image().is_some() || prepared.is_retired() {
                    changed_rows = checked_add(changed_rows, 1)?;
                }
                validate_single_row_write(row, &prepared)?;
                let mutation_bytes = prepare_mutation(row, &prepared)?
                    .map_or(Ok(0), |mutation| mutation.semantic_write_bytes())
                    .map_err(MigrationFinding::from_stage_error)?;
                semantic_write_bytes = checked_add(
                    semantic_write_bytes,
                    u64::try_from(mutation_bytes).map_err(|_| {
                        MigrationFinding::from_stage_error(MigrationStageError::LimitExceeded)
                    })?,
                )?;
                validate_cross_row_facts(plan, stage, &prepared)?;
            }
            let Some(next) = page.next() else {
                break;
            };
            cursor = next.clone();
        }
        Ok(MigrationCheckReport {
            checked_rows,
            changed_rows,
            scan_pages,
            semantic_write_bytes,
        })
    }

    /// Repeats preflight, applies bounded batches, validates, and cuts over atomically.
    pub fn apply<S: MigrationStagePort>(
        plan: &ValidatedMigrationPlan,
        stage: &mut S,
    ) -> Result<MigrationApplyReport, MigrationFinding> {
        Self::apply_with_projection_builder(plan, stage, &mut NoProjectionBuilder)
    }

    /// Applies with the projection-owned builder required by successor plans.
    pub fn apply_with_projection_builder<S, P>(
        plan: &ValidatedMigrationPlan,
        stage: &mut S,
        projection_builder: &mut P,
    ) -> Result<MigrationApplyReport, MigrationFinding>
    where
        S: MigrationStagePort,
        P: MigrationProjectionBuildPort,
    {
        let resumed = stage
            .migration_journal_state(plan.migration_bundle_hash())
            .map_err(MigrationFinding::from_stage_error)?;
        let current_step = stage
            .migration_journal_step(plan.migration_bundle_hash())
            .map_err(MigrationFinding::from_stage_error)?
            .unwrap_or(ContractMigrationJournalStepV1::Transforming);
        if (resumed.is_none() && current_step != ContractMigrationJournalStepV1::Transforming)
            || current_step == ContractMigrationJournalStepV1::Complete
        {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::Integrity,
            ));
        }
        let fresh_check = if resumed.is_none() {
            Some(Self::check(plan, stage)?)
        } else {
            None
        };
        let (mut cursor, mut checked_rows, mut changed_rows, mut batch_count) =
            resumed.map_or((MigrationScanCursor::start(), 0, 0, 0), |journal| {
                (
                    journal.checked_through().clone(),
                    journal.checked_rows(),
                    journal.changed_rows(),
                    journal.batch_count(),
                )
            });
        let mut scan_pages = 0_u64;

        if current_step == ContractMigrationJournalStepV1::Transforming {
            loop {
                let page = stage
                    .scan_migration_rows(&cursor)
                    .map_err(MigrationFinding::from_stage_error)?;
                validate_page_progress(&cursor, &page)?;
                if !page.rows().is_empty() {
                    scan_pages = checked_add(scan_pages, 1)?;
                }
                let mut mutations = Vec::new();
                let mut write_bytes = 0_usize;
                let mut last_checked = None;
                for row in page.rows() {
                    if plan.is_candidate_bound(row) {
                        last_checked = Some(row.target().clone());
                        continue;
                    }
                    let prepared = plan.prepare_row(row.clone())?;
                    let mutation = prepare_mutation(row, &prepared)?;
                    let mutation_bytes = mutation
                        .as_ref()
                        .map_or(Ok(0), MigrationRowMutation::semantic_write_bytes)
                        .map_err(MigrationFinding::from_stage_error)?;
                    let exceeds_batch = mutation.is_some()
                        && (mutations.len() == MAX_MIGRATION_BATCH_MUTATIONS
                            || write_bytes
                                .checked_add(mutation_bytes)
                                .is_none_or(|bytes| bytes > MAX_MIGRATION_BATCH_WRITE_BYTES));
                    if exceeds_batch {
                        apply_batch(
                            plan,
                            stage,
                            std::mem::take(&mut mutations),
                            last_checked
                                .take()
                                .expect("a full batch checked a prior row"),
                            checked_rows,
                            changed_rows,
                        )?;
                        batch_count = checked_add(batch_count, 1)?;
                        write_bytes = 0;
                    }
                    checked_rows = checked_add(checked_rows, 1)?;
                    if prepared.post_image().is_some() || prepared.is_retired() {
                        changed_rows = checked_add(changed_rows, 1)?;
                    }
                    if let Some(mutation) = mutation {
                        write_bytes = write_bytes.checked_add(mutation_bytes).ok_or_else(|| {
                            MigrationFinding::from_stage_error(MigrationStageError::LimitExceeded)
                        })?;
                        mutations.push(mutation);
                    }
                    last_checked = Some(row.target().clone());
                }
                if let Some(last_checked) = last_checked {
                    apply_batch(
                        plan,
                        stage,
                        mutations,
                        last_checked,
                        checked_rows,
                        changed_rows,
                    )?;
                    batch_count = checked_add(batch_count, 1)?;
                }
                let Some(next) = page.next() else {
                    break;
                };
                cursor = next.clone();
            }
        }

        if current_step == ContractMigrationJournalStepV1::Transforming && batch_count == 0 {
            let batch = MigrationBatch::new(
                plan.migration_bundle_hash(),
                Vec::new(),
                MigrationScanCursor::start(),
                0,
                0,
            )
            .map_err(MigrationFinding::from_stage_error)?;
            stage
                .apply_migration_batch(batch)
                .map_err(MigrationFinding::from_stage_error)?;
            batch_count = 1;
        }
        if let Some(check) = fresh_check
            && (checked_rows != check.checked_rows || changed_rows != check.changed_rows)
        {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::RowChanged,
            ));
        }

        let catalog_projection_ids = plan
            .rebuilt_projections()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let projections = MigrationProjectionCandidates::from_catalog_set(
            plan.rebuilt_projections(),
        )
        .map_err(|_| MigrationFinding::from_stage_error(MigrationStageError::LimitExceeded))?;
        if current_step < ContractMigrationJournalStepV1::RebuildingProjections {
            stage
                .checkpoint_migration_step(
                    ContractMigrationJournalStepV1::RebuildingProjections,
                    &catalog_projection_ids,
                )
                .map_err(MigrationFinding::from_stage_error)?;
        }
        let frozen_frontier = stage.migration_frozen_frontier();
        let projection_readiness = projections
            .rebuild(frozen_frontier, projection_builder)
            .map_err(|_| MigrationFinding::from_stage_error(MigrationStageError::Integrity))?;
        if current_step < ContractMigrationJournalStepV1::Validating {
            stage
                .checkpoint_migration_step(
                    ContractMigrationJournalStepV1::Validating,
                    &projection_readiness.projection_ids(),
                )
                .map_err(MigrationFinding::from_stage_error)?;
        }
        stage
            .validate_migration_stage_structure()
            .map_err(MigrationFinding::from_stage_error)?;
        let semantic_validation = plan.validate_successor_stage(stage, checked_rows)?;
        let required_projections = projection_readiness.projection_ids();
        let validation_digest = final_validation_digest(
            semantic_validation.validation_digest(),
            &projection_readiness,
        );
        if current_step < ContractMigrationJournalStepV1::ReadyForCutover {
            stage
                .checkpoint_migration_step(
                    ContractMigrationJournalStepV1::ReadyForCutover,
                    &required_projections,
                )
                .map_err(MigrationFinding::from_stage_error)?;
        }
        let candidate = plan
            .candidate()
            .to_stored()
            .map_err(|_| MigrationFinding::from_stage_error(MigrationStageError::Integrity))?;
        let applied = stage
            .finalize_migration(MigrationCutover::new(
                plan.parent_bundle_hash(),
                candidate,
                plan.migration_bundle_hash(),
                required_projections,
                plan.parent_lineage_hashes().to_vec(),
                validation_digest,
            ))
            .map_err(MigrationFinding::from_stage_error)?;
        Ok(MigrationApplyReport {
            check: MigrationCheckReport {
                checked_rows,
                changed_rows,
                scan_pages: fresh_check.map_or(scan_pages, |check| check.scan_pages),
                semantic_write_bytes: fresh_check.map_or(0, |check| check.semantic_write_bytes),
            },
            batch_count,
            administration_sequence: applied.administration_sequence(),
        })
    }
}

struct NoProjectionBuilder;

impl MigrationProjectionBuildPort for NoProjectionBuilder {
    fn rebuild_projection(
        &mut self,
        _projection: riffdb_types::ProjectionId,
        _frontier: Option<riffdb_types::CommitSequence>,
    ) -> Result<
        riffdb_projection::MigrationProjectionBuildObservation,
        riffdb_projection::MigrationProjectionError,
    > {
        Err(riffdb_projection::MigrationProjectionError::BuildFailed)
    }
}

fn final_validation_digest(
    semantic: riffdb_types::ContractMigrationValidationDigest,
    projections: &riffdb_projection::MigrationProjectionReadiness,
) -> riffdb_types::ContractMigrationValidationDigest {
    let mut bytes = Vec::with_capacity(32 + projections.observations().len() * 24 + 9);
    bytes.extend_from_slice(semantic.as_bytes());
    match projections.frontier() {
        None => bytes.push(0),
        Some(frontier) => {
            bytes.push(1);
            bytes.extend_from_slice(&frontier.get().to_be_bytes());
        }
    }
    for observation in projections.observations() {
        bytes.extend_from_slice(&observation.projection().get().to_be_bytes());
        bytes.extend_from_slice(&observation.generation().get().to_be_bytes());
    }
    riffdb_types::hash_contract_migration_validation(&bytes)
}

fn validate_stage_identity<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &S,
) -> Result<(), MigrationFinding> {
    if stage.active_bundle_hash() != plan.parent_bundle_hash() {
        return Err(MigrationFinding::from_stage_error(
            MigrationStageError::Integrity,
        ));
    }
    Ok(())
}

fn validate_cross_row_facts<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &S,
    prepared: &PreparedMigrationRow,
) -> Result<(), MigrationFinding> {
    if prepared.is_rekeyed() {
        if stage
            .migration_target_exists(prepared.effective_target())
            .map_err(MigrationFinding::from_stage_error)?
        {
            return Err(MigrationFinding::rekey_target_occupied(
                prepared.effective_target().entity_type_id(),
            ));
        }
        validate_target_claim(plan, stage, prepared)?;
    }
    for relationship in prepared.relationships() {
        if !effective_target_exists(plan, stage, relationship.target())? {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::RelationshipMissing(relationship.source().entity_type_id()),
            ));
        }
    }
    for unique in prepared.unique_keys() {
        validate_unique_fact(plan, stage, unique)?;
    }
    Ok(())
}

fn validate_target_claim<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &S,
    expected: &PreparedMigrationRow,
) -> Result<(), MigrationFinding> {
    let mut cursor = MigrationScanCursor::start();
    loop {
        let page = stage
            .scan_migration_rows(&cursor)
            .map_err(MigrationFinding::from_stage_error)?;
        validate_page_progress(&cursor, &page)?;
        for row in page.rows() {
            if row.target() == expected.source().target() {
                continue;
            }
            let candidate = plan.prepare_row(row.clone())?;
            if !candidate.is_retired()
                && candidate.effective_target() == expected.effective_target()
            {
                return Err(MigrationFinding::target_collision(
                    expected.effective_target().entity_type_id(),
                ));
            }
        }
        let Some(next) = page.next() else {
            break;
        };
        cursor = next.clone();
    }
    Ok(())
}

fn effective_target_exists<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &S,
    expected: &riffdb_storage_api::EntityTarget,
) -> Result<bool, MigrationFinding> {
    let mut cursor = MigrationScanCursor::start();
    loop {
        let page = stage
            .scan_migration_rows(&cursor)
            .map_err(MigrationFinding::from_stage_error)?;
        validate_page_progress(&cursor, &page)?;
        for row in page.rows() {
            let candidate = plan.prepare_row(row.clone())?;
            if !candidate.is_retired() && candidate.effective_target() == expected {
                return Ok(true);
            }
        }
        let Some(next) = page.next() else {
            return Ok(false);
        };
        cursor = next.clone();
    }
}

fn validate_single_row_write(
    row: &riffdb_storage_api::StoredEntityRecordV1,
    prepared: &PreparedMigrationRow,
) -> Result<(), MigrationFinding> {
    if let Some(mutation) = prepare_mutation(row, prepared)? {
        mutation
            .semantic_write_bytes()
            .map_err(MigrationFinding::from_stage_error)?;
    }
    Ok(())
}

fn prepare_mutation(
    row: &riffdb_storage_api::StoredEntityRecordV1,
    prepared: &PreparedMigrationRow,
) -> Result<Option<MigrationRowMutation>, MigrationFinding> {
    if prepared.is_retired() {
        return Ok(Some(MigrationRowMutation::retire(
            MigrationRowEvidence::from_source(row.clone()),
        )));
    }
    let post_image = prepared.post_image().cloned();
    if post_image.is_none() && prepared.rebuilt_indexes().is_empty() {
        return Ok(None);
    }
    MigrationRowMutation::new(
        MigrationRowEvidence::from_source(row.clone()),
        post_image,
        prepared.rebuilt_indexes().to_vec(),
    )
    .map(Some)
    .map_err(MigrationFinding::from_stage_error)
}

fn apply_batch<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &mut S,
    mutations: Vec<MigrationRowMutation>,
    last_checked: riffdb_storage_api::EntityTarget,
    checked_rows: u64,
    changed_rows: u64,
) -> Result<(), MigrationFinding> {
    let batch = MigrationBatch::new(
        plan.migration_bundle_hash(),
        mutations,
        MigrationScanCursor::after(last_checked),
        checked_rows,
        changed_rows,
    )
    .map_err(MigrationFinding::from_stage_error)?;
    stage
        .apply_migration_batch(batch)
        .map_err(MigrationFinding::from_stage_error)
}

fn validate_unique_fact<S: MigrationStagePort>(
    plan: &ValidatedMigrationPlan,
    stage: &S,
    expected: &MigrationUniqueFact,
) -> Result<(), MigrationFinding> {
    let mut cursor = MigrationScanCursor::start();
    loop {
        let page = stage
            .scan_migration_rows(&cursor)
            .map_err(MigrationFinding::from_stage_error)?;
        validate_page_progress(&cursor, &page)?;
        for candidate in page.rows() {
            if candidate.target() == expected.source() {
                continue;
            }
            let candidate = plan.prepare_row(candidate.clone())?;
            if candidate.unique_keys().iter().any(|fact| {
                fact.entity_type() == expected.entity_type()
                    && fact.index() == expected.index()
                    && fact.prefix() == expected.prefix()
            }) {
                return Err(MigrationFinding::from_stage_error(
                    MigrationStageError::UniqueConflict {
                        entity: expected.entity_type(),
                        index: expected.index(),
                    },
                ));
            }
        }
        let Some(next) = page.next() else {
            break;
        };
        cursor = next.clone();
    }
    Ok(())
}

fn checked_add(value: u64, amount: u64) -> Result<u64, MigrationFinding> {
    value
        .checked_add(amount)
        .ok_or_else(|| MigrationFinding::from_stage_error(MigrationStageError::LimitExceeded))
}

fn validate_page_progress(
    request: &MigrationScanCursor,
    page: &riffdb_storage_api::MigrationScanPage,
) -> Result<(), MigrationFinding> {
    if page.rows().first().is_some_and(|row| {
        request
            .exclusive_lower_bound()
            .is_some_and(|lower| row.target() <= lower)
    }) || page.next().is_some_and(|next| next <= request)
    {
        return Err(MigrationFinding::from_stage_error(
            MigrationStageError::Integrity,
        ));
    }
    Ok(())
}
