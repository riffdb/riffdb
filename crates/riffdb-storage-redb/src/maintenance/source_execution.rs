//! Closed configured numerical binding and actual offline custody producer.
//! No Source drive/cut/Accepted completion entry is implemented here.
#![allow(clippy::result_large_err)]
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use crate::source_execution_domain::{
    SourceExecutionBootstrapV1, SourceOperationClaimV1, SourceOperationClaimsV1,
    SourceOperationTaskRetirementV1,
};
use std::sync::Arc;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[path = "source_execution/configured_pair.rs"]
mod configured_pair;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) use configured_pair::{ConfiguredDerivedBindingV1, ConfiguredDerivedContinuationV1};

/// Opaque consumed native preparation. There is deliberately no execute method,
/// raw backend/brand/predecessor getter, Clone or detach-to-ID conversion.
/// The complete composition above Native must consume it with the fixed Core
/// commit port; Native must never acquire a dependency on riffdb-commit.
pub struct SourceOwnedOperationV1 {
    // No decoder, backup reader or native engine exists in this root product.
    owner: RedbMaintenanceStorage,
    backup: PathBuf,
    inputs: Option<StartupValidationInputs>,
    cancellation: Arc<std::sync::atomic::AtomicBool>,
    source_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    claim: SourceOperationClaimV1,
    task_retirement: Option<SourceOperationTaskRetirementV1>,
    // Covers both owned configured paths and their later artifact copies.
    // All semantic/path/native contents die before this reservation refunds.
    _intent_work: riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkReservationV1,
}
// This private phase product is not a Source backend, input or completion. Its
// maintenance prelude remains consuming; the later fixed capsule must call it
// inside its actual authenticated, preadmitted no-yield worker before Source work.
// There is no public execute entry or raw predecessor getter.
pub(crate) struct SourceValidatedOperationV1 {
    predecessor: crate::startup::MaintenanceValidatedPredecessorV1,
    // Retain the real original path/claim/control owner on every success/fail
    // route; no destructure-to-locals can refund its last charge prematurely.
    intent: SourceOwnedOperationV1,
}
impl SourceOwnedOperationV1 {
    /// Original numerical ledger only; no semantic Source authority.
    pub fn numerical_budget(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.claim.domain().budget()
    }

    // Called only by the future concrete fixed capsule. Keeping this private is
    // deliberate until the real scope/backend/Commit composition is implemented.
    fn finish_maintenance_preparation(
        self,
    ) -> Result<SourceValidatedOperationV1, SourceWorkerPreparationFailureV1> {
        let mut intent = self;
        let result = (|| -> Result<_, StorageError> {
            intent.claim.require_current()?;
            // The unchanged whole owner performs every path/promotion check
            // inside the worker, never while the root storage mutex is held.
            intent.owner.verify_path_ownership()?;
            let source_domain = intent.source_domain.take().ok_or_else(unavailable)?;
            let inputs = intent.inputs.take().ok_or_else(unavailable)?;
            let maintenance = crate::startup::OfflinePredecessorMaintenanceV1::acquire_configured(
                &intent.owner.database_file,
                &intent.backup,
                inputs,
                Arc::clone(&intent.cancellation),
                source_domain,
            )?;
            let predecessor = maintenance.validate_complete_maintenance()?;
            intent.owner.verify_path_ownership()?;
            intent.claim.require_current()?;
            if !predecessor
                .numerical_source_budget()
                .shares_owner_with(intent.claim.domain().budget())
            {
                return Err(unavailable());
            }
            Ok(predecessor)
        })();
        match result {
            Ok(predecessor) => Ok(SourceValidatedOperationV1 {
                predecessor,
                intent,
            }),
            // All partial native locals have dropped before this failure owner.
            // Preserve the actual maintenance loan for closed retirement/return.
            Err(error) => Err(SourceWorkerPreparationFailureV1 { intent, error }),
        }
    }
    pub(crate) fn take_task_retirement(
        &mut self,
    ) -> Result<SourceOperationTaskRetirementV1, StorageError> {
        self.claim.require_current()?;
        self.task_retirement.take().ok_or_else(unavailable)
    }
}
impl SourceValidatedOperationV1 {
    #[cfg(test)]
    pub(crate) fn numerical_budget(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.predecessor.numerical_source_budget()
    }
}

// The root intent needs only two configured byte paths. The worker's existing
// artifact constructor clones them once more; reserve BOTH live copies now.
// No manifest/catalog scan, encoding or semantic parser runs during this quote.
fn intent_paths_bound(
    source: &Path,
    backup_root: &Path,
    name: &BackupNameV1,
) -> Result<(u64, usize), StorageError> {
    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        return Err(unavailable());
    }
    let source_len = source.as_os_str().len();
    let backup_len = backup_root
        .as_os_str()
        .len()
        .checked_add(1)
        .and_then(|n| n.checked_add(name.as_str().len()))
        .ok_or_else(unavailable)?;
    if source_len > 4096 || backup_len > 4096 {
        return Err(unavailable());
    }
    let one_pair = riffdb_types::owned_allocation_capacity_bound_v1(source_len, 1)
        .ok()
        .and_then(|source| {
            riffdb_types::owned_allocation_capacity_bound_v1(backup_len, 1)
                .ok()
                .and_then(|backup| source.checked_add(backup))
        })
        .ok_or_else(unavailable)?;
    // Conservatively cover two copies of both paths, including the original
    // source's transferred baseline capacity; no exclusion is needed here.
    Ok((one_pair.checked_mul(2).ok_or_else(unavailable)?, backup_len))
}

impl RedbMaintenanceStorage {
    /// Ordinary configured maintenance ownership, consuming its fixed startup
    /// numerical seed. No native/Source handle or arbitrary ledger pair enters.
    pub fn open_for_promotion_recovery_with_source_bootstrap(
        database_file: impl AsRef<Path>,
        backup_root: impl AsRef<Path>,
        bootstrap: SourceExecutionBootstrapV1,
    ) -> Result<Self, StorageError> {
        let mut owner = Self::open_for_promotion_recovery(database_file, backup_root)?;
        let domain = bootstrap.into_configured();
        domain.require_idle()?;
        owner.source_execution = Some(domain);
        Ok(owner)
    }
    /// Opens only this actual configured primary with its original startup
    /// ledger. The normal format/repair/journal/startup-validation path remains
    /// unchanged; an active or retained Source task refuses before native open.
    pub fn open_configured_current_store(
        &mut self,
        profile: crate::RedbCommitProfile,
    ) -> Result<crate::RedbStore, StorageError> {
        self.verify_path_ownership()?;
        let execution = self.source_execution.as_ref().ok_or_else(unavailable)?;
        let domain =
            crate::source_working_domain::SourceWorkingDomain::from_configured(execution, None)?;
        let store = crate::store::open_configured_current(&self.database_file, profile, domain)?;
        self.verify_path_ownership()?;
        Ok(store)
    }

    // Root phase only: this function consumes the actual whole offline owner.
    // It performs NO filesystem/path/promotion inventory verification or native
    // construction. The closed slot retains identity on failure; the actual
    // worker must run every unchanged owner check before using the source.
    fn into_source_operation(
        self,
        backup_name: &BackupNameV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<SourceOwnedOperationV1, SourceOperationLoanFailureV1> {
        // Borrow the whole actual owner throughout fallible preadmission;
        // a refusal returns that SAME owner rather than unlocking/reopening it.
        let mut owner = self;
        let result = (|| -> Result<_, StorageError> {
            let storage = &mut owner;
            let domain = storage
                .source_execution
                .as_ref()
                .ok_or_else(unavailable)?
                .clone();
            domain.require_idle()?;
            let (intent_bytes, backup_capacity) =
                intent_paths_bound(&storage.database_file, &storage.backup_root, backup_name)?;
            let intent_work = domain
                .budget()
                .reserve(intent_bytes)
                .map_err(|_| unavailable())?;
            let maintenance_lock = storage.ownership_lock.retain_for_source(&domain)?;
            let SourceOperationClaimsV1 {
                semantic: claim,
                task,
            } = domain.begin_operation(maintenance_lock)?;
            let source_domain = crate::source_working_domain::SourceWorkingDomain::from_configured(
                &domain,
                Some(&claim),
            )?;
            // Clone no source path. The actual whole owner supplies it later.
            let mut backup = PathBuf::with_capacity(backup_capacity);
            backup.push(&storage.backup_root);
            backup.push(backup_name.as_str());
            claim.require_current()?;
            Ok((backup, source_domain, claim, task, intent_work))
        })();
        match result {
            Ok((backup, source_domain, claim, task, intent_work)) => Ok(SourceOwnedOperationV1 {
                owner,
                backup,
                inputs: Some(inputs),
                cancellation,
                source_domain: Some(source_domain),
                claim,
                task_retirement: Some(task),
                _intent_work: intent_work,
            }),
            Err(error) => Err(SourceOperationLoanFailureV1 { owner, error }),
        }
    }
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

#[cfg(test)]
impl SourceOwnedOperationV1 {
    pub(crate) fn finish_maintenance_preparation_for_test(
        self,
    ) -> Result<SourceValidatedOperationV1, SourceWorkerPreparationFailureV1> {
        self.finish_maintenance_preparation()
    }
}

struct SourceOperationLoanFailureV1 {
    owner: RedbMaintenanceStorage,
    error: StorageError,
}
/// Whole configured maintenance owner, installed or exclusively loaned.
/// No method detaches an installed owner or constructs a returned owner.
#[doc(hidden)]
pub struct SourceOfflineStorageSlotV1 {
    owner: Option<RedbMaintenanceStorage>,
    original_domain: Option<crate::source_execution_domain::ConfiguredSourceExecutionDomainV1>,
}
impl SourceOfflineStorageSlotV1 {
    /// Wraps the existing ordinary owner without reading or decoding artifacts.
    /// An ordinary unconfigured owner remains usable, but cannot produce Source work.
    pub fn new(owner: RedbMaintenanceStorage) -> Self {
        let original_domain = owner.source_execution.clone();
        Self {
            owner: Some(owner),
            original_domain,
        }
    }
    /// Borrows only the currently installed ordinary adapter. A loan refuses
    /// immediately; it neither waits nor reopens/replaces the actual owner.
    pub fn ordinary_mut(&mut self) -> Result<SourceOrdinaryMaintenanceV1<'_>, StorageError> {
        let owner = self.owner.as_mut().ok_or_else(unavailable)?;
        Ok(SourceOrdinaryMaintenanceV1::new(owner))
    }
    /// Shared ordinary authority cannot move the non-Clone owner from this slot.
    pub fn ordinary_ref(&self) -> Result<&RedbMaintenanceStorage, StorageError> {
        self.owner.as_ref().ok_or_else(unavailable)
    }
    /// Moves the SAME configured owner into an opaque, unvalidated intent.
    /// Refusal reinstalls it before returning; no native reads occur here.
    pub fn loan(
        &mut self,
        backup_name: &BackupNameV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<SourceOwnedOperationV1, StorageError> {
        let same_domain = self.owner.as_ref().is_some_and(|owner| {
            self.original_domain.as_ref().is_some_and(|original| {
                owner
                    .source_execution
                    .as_ref()
                    .is_some_and(|actual| original.same_domain_as(actual))
            })
        });
        if !same_domain {
            return Err(unavailable());
        }
        let owner = self.owner.take().ok_or_else(unavailable)?;
        match owner.into_source_operation(backup_name, inputs, cancellation) {
            Ok(operation) => Ok(operation),
            Err(SourceOperationLoanFailureV1 { owner, error }) => {
                self.owner = Some(owner);
                Err(error)
            }
        }
    }
    // No owned take/replacement, reopen or arbitrary callback.
    // This exact returned owner can reenter only its original empty slot, after
    // the semantic claim AND final task tail have physically retired.
    /// Returns only an opaque actual loan after the original domain is idle.
    /// A refusal retains the returned owner, including its maintenance lock.
    pub fn restore(
        &mut self,
        returned: SourceReturnedStorageV1,
    ) -> Result<(), SourceStorageReturnFailureV1> {
        let valid = self.owner.is_none()
            && self.original_domain.as_ref().is_some_and(|original| {
                returned
                    .owner
                    .source_execution
                    .as_ref()
                    .is_some_and(|actual| {
                        original.same_domain_as(actual) && actual.require_idle().is_ok()
                    })
            });
        if !valid {
            return Err(SourceStorageReturnFailureV1 { returned });
        }
        self.owner = Some(returned.owner);
        Ok(())
    }
}
// Not a Source completion; only this worker phase can return its actual loan.
/// An actual retired loan; only native consuming preparation can construct it.
#[doc(hidden)]
pub struct SourceReturnedStorageV1 {
    owner: RedbMaintenanceStorage,
}
/// Retains the actual return when its original slot/domain is not yet idle.
#[doc(hidden)]
pub struct SourceStorageReturnFailureV1 {
    returned: SourceReturnedStorageV1,
}
pub(crate) struct SourceWorkerPreparationFailureV1 {
    intent: SourceOwnedOperationV1,
    error: StorageError,
}
impl std::fmt::Debug for SourceWorkerPreparationFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceWorkerPreparationFailureV1")
            .field("kind", &self.error.kind())
            .finish_non_exhaustive()
    }
}
impl SourceOwnedOperationV1 {
    fn retire_intent(self) -> SourceReturnedStorageV1 {
        let Self {
            owner,
            backup,
            inputs,
            cancellation,
            source_domain,
            claim,
            task_retirement,
            _intent_work,
        } = self;
        drop(backup);
        drop(inputs);
        drop(cancellation);
        drop(source_domain);
        drop(claim);
        drop(task_retirement);
        drop(_intent_work);
        SourceReturnedStorageV1 { owner }
    }
}
impl SourceWorkerPreparationFailureV1 {
    fn return_after_refusal(self) -> (SourceReturnedStorageV1, StorageError) {
        let Self { intent, error } = self;
        (intent.retire_intent(), error)
    }
}
#[cfg(test)]
impl SourceValidatedOperationV1 {
    pub(crate) fn return_storage_after_native_drop_for_test(self) -> SourceReturnedStorageV1 {
        let Self {
            predecessor,
            intent,
        } = self;
        // Native destruction occurs while the entire intent is intact. Typed
        // refusal uses the same private retirement once partial locals are gone.
        drop(predecessor);
        intent.retire_intent()
    }
}
#[cfg(test)]
impl SourceOfflineStorageSlotV1 {
    pub(crate) fn restore_for_test(
        &mut self,
        returned: SourceReturnedStorageV1,
    ) -> Result<(), SourceStorageReturnFailureV1> {
        self.restore(returned)
    }
    pub(crate) fn original_budget_for_test(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.owner
            .as_ref()
            .expect("unloaned or refused slot retains its exact owner")
            .source_execution
            .as_ref()
            .expect("configured owner keeps original domain")
            .budget()
    }
}

impl SourceStorageReturnFailureV1 {
    /// Retries this exact retained return; foreign slots still refuse.
    pub fn retry(self, slot: &mut SourceOfflineStorageSlotV1) -> Result<(), Self> {
        slot.restore(self.returned)
    }
    #[cfg(test)]
    pub(crate) fn retry_for_test(self, slot: &mut SourceOfflineStorageSlotV1) -> Result<(), Self> {
        self.retry(slot)
    }
}

#[cfg(test)]
impl SourceWorkerPreparationFailureV1 {
    pub(crate) fn return_after_refusal_for_test(self) -> (SourceReturnedStorageV1, StorageError) {
        self.return_after_refusal()
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
impl SourceOwnedOperationV1 {
    pub(crate) fn idle_probe_for_test(
        &self,
    ) -> crate::source_execution_domain::SourceOperationIdleProbeV1 {
        self.claim.idle_probe_for_test()
    }
}
#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
impl SourceValidatedOperationV1 {
    // Actual existing D4 handoff, diagnostic test only. No production getter.
    pub(crate) fn split_facade_for_test(
        self,
    ) -> (
        crate::store::SourceOwnedOfflineReadV1,
        SourceOwnedOperationV1,
    ) {
        let Self {
            predecessor,
            intent,
        } = self;
        let opened = crate::engine_owner::source_reopen::OpenedPredecessorSourceReadV1::from_validated_predecessor(predecessor).unwrap();
        (
            crate::store::SourceOwnedOfflineReadV1::from_opened(opened),
            intent,
        )
    }
}
