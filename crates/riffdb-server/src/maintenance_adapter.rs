//! Receipt-backed admission for the private offline-maintenance driver.

use std::fmt;
use std::sync::mpsc as sync_mpsc;
use std::sync::{Arc, Mutex};

use riffdb_policy::{AuthorizedOfflineMaintenance, OfflineMaintenanceAuthorizationRequest};
use riffdb_service::{
    AuthorizedOfflineMaintenanceObservation, AuthorizedOfflineMaintenanceStart,
    AuthorizedRestoreRetryStart, CreateOfflineBackupRequest, OfflineMaintenanceCoordinatorPort,
    OfflineMaintenanceObservationFailure, OfflineMaintenanceObservationPermit,
    OfflineMaintenanceObservationPhase, OfflineMaintenanceObservationPortError,
    OfflineMaintenanceOperationObservation, OfflineMaintenanceStartDisposition,
    OfflineMaintenanceStartPermit, OfflineMaintenanceStartPortError, OfflineMaintenanceStartResult,
    PortAdmissionError, PortFuture, RecoveryOfflineMaintenanceCoordinatorPort,
    RecoveryOfflineMaintenancePortError, RecoveryOfflineMaintenanceRestore,
    RecoveryOfflineMaintenanceRestorePermit, RequestControl, RestoreOfflineBackupRequest,
    RestoreRetryOfflineMaintenanceCoordinatorPort, RestoreRetryOfflineMaintenancePermit,
};
use riffdb_storage_api::{
    OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptCreateResultV1,
    OfflineMaintenanceReceiptFailureV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, StorageError, StorageErrorKind,
};
use riffdb_storage_redb::RedbMaintenanceStorage;
use riffdb_types::{
    OfflineMaintenanceInputHash, OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation,
};
use tokio::sync::{mpsc, oneshot};

use crate::maintenance_lifecycle::{MaintenanceLifecycle, MaintenanceReceiptClaim};
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};

/// The single slot between receipt admission and the exclusive process driver.
///
/// Lifecycle exclusion permits only one nonterminal operation, so additional
/// queue capacity would retain no useful work and would obscure admission
/// defects.
pub(crate) const MAINTENANCE_TRIGGER_BUFFER: usize = 1;

/// Process-wide ownership of the external receipt and artifact adapter.
pub(crate) type SharedMaintenanceStorage = Arc<Mutex<RedbMaintenanceStorage>>;

/// Creates the exact bounded handoff retained outside production graph generations.
pub(crate) fn maintenance_trigger_channel() -> (
    mpsc::Sender<MaintenanceTrigger>,
    mpsc::Receiver<MaintenanceTrigger>,
) {
    mpsc::channel(MAINTENANCE_TRIGGER_BUFFER)
}

/// Wraps the one concrete maintenance adapter for process-wide sharing.
pub(crate) fn shared_maintenance_storage(
    storage: RedbMaintenanceStorage,
) -> SharedMaintenanceStorage {
    Arc::new(Mutex::new(storage))
}

/// Move-only work owned by the daemon after durable receipt admission.
pub(crate) enum MaintenanceTrigger {
    CreateBackup {
        request: CreateOfflineBackupRequest,
        start_ready: oneshot::Receiver<()>,
    },
    RestoreBackup {
        request: RestoreOfflineBackupRequest,
        credential: riffdb_auth::RetainedOpaqueCredential,
        start_ready: oneshot::Receiver<()>,
    },
    RecoveryRestore {
        restore: RecoveryOfflineMaintenanceRestore,
        completion: RecoveryMaintenanceCompletion,
    },
}

impl MaintenanceTrigger {
    /// Returns the caller-stable operation identity.
    pub(crate) const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        match self {
            Self::CreateBackup { request, .. } => request.operation_id(),
            Self::RestoreBackup { request, .. } => request.operation_id(),
            Self::RecoveryRestore { restore, .. } => restore.request().operation_id(),
        }
    }

    /// Waits until the durable start adapter has released its receipt lock and
    /// returned the start result to the service job.
    pub(crate) async fn wait_for_start_ready(&mut self) -> Result<(), ()> {
        match self {
            Self::CreateBackup { start_ready, .. } | Self::RestoreBackup { start_ready, .. } => {
                start_ready.await.map_err(|_| ())
            }
            Self::RecoveryRestore { .. } => Ok(()),
        }
    }
}

impl fmt::Debug for MaintenanceTrigger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CreateBackup { .. } => "MaintenanceTrigger::CreateBackup([REDACTED])",
            Self::RestoreBackup { .. } => "MaintenanceTrigger::RestoreBackup([REDACTED])",
            Self::RecoveryRestore { .. } => "MaintenanceTrigger::RecoveryRestore([REDACTED])",
        })
    }
}

type RecoveryMaintenanceResult =
    Result<OfflineMaintenanceStartResult, RecoveryOfflineMaintenancePortError>;

/// Move-only result capability returned to the recovery coordinator worker.
pub(crate) struct RecoveryMaintenanceCompletion {
    sender: sync_mpsc::SyncSender<RecoveryMaintenanceResult>,
}

impl RecoveryMaintenanceCompletion {
    /// Publishes the receipt-derived result or exact pre-receipt driver failure.
    pub(crate) fn complete(
        self,
        result: RecoveryMaintenanceResult,
    ) -> Result<(), RecoveryMaintenanceCompletionError> {
        self.sender
            .send(result)
            .map_err(|_| RecoveryMaintenanceCompletionError)
    }
}

impl fmt::Debug for RecoveryMaintenanceCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryMaintenanceCompletion([MOVE_ONLY])")
    }
}

/// The recovery coordinator stopped before accepting its driver result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryMaintenanceCompletionError;

/// Process-wide maintenance authority reused across fresh graph generations.
#[derive(Clone)]
pub(crate) struct MaintenanceController {
    storage: SharedMaintenanceStorage,
    lifecycle: Arc<MaintenanceLifecycle>,
    triggers: mpsc::Sender<MaintenanceTrigger>,
    recovery_admission: Arc<Mutex<()>>,
}

impl MaintenanceController {
    pub(crate) fn new(
        storage: SharedMaintenanceStorage,
        lifecycle: Arc<MaintenanceLifecycle>,
        triggers: mpsc::Sender<MaintenanceTrigger>,
    ) -> Self {
        Self {
            storage,
            lifecycle,
            triggers,
            recovery_admission: Arc::new(Mutex::new(())),
        }
    }

    /// Creates a normal service port on one graph generation's blocking driver.
    pub(crate) fn coordinator(
        &self,
        driver: &BlockingPortDriver,
    ) -> Arc<dyn OfflineMaintenanceCoordinatorPort> {
        Arc::new(ServerOfflineMaintenanceCoordinator::new(
            self.clone(),
            driver,
        ))
    }

    /// Creates the staged-only recovery capability on one blocking driver.
    pub(crate) fn recovery_coordinator(
        &self,
        driver: &BlockingPortDriver,
    ) -> Arc<dyn RecoveryOfflineMaintenanceCoordinatorPort> {
        Arc::new(ServerRecoveryOfflineMaintenanceCoordinator::new(
            self.clone(),
            driver,
        ))
    }

    /// Creates one exact current-database restore retry capability.
    pub(crate) fn restore_retry_coordinator(
        &self,
        driver: &BlockingPortDriver,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Arc<dyn RestoreRetryOfflineMaintenanceCoordinatorPort> {
        Arc::new(ServerRestoreRetryOfflineMaintenanceCoordinator::new(
            self.clone(),
            driver,
            operation_id,
            input_hash,
        ))
    }

    /// Clones the external maintenance-storage authority for the daemon driver.
    pub(crate) fn storage(&self) -> SharedMaintenanceStorage {
        Arc::clone(&self.storage)
    }
}

impl fmt::Debug for MaintenanceController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MaintenanceController([PRIVATE_AUTHORITY])")
    }
}

struct ServerOfflineMaintenanceCoordinator {
    start: BlockingPortExecutor<
        AuthorizedOfflineMaintenanceStart,
        OfflineMaintenanceStartResult,
        OfflineMaintenanceStartPortError,
    >,
    observation: BlockingPortExecutor<
        AuthorizedOfflineMaintenanceObservation,
        Option<OfflineMaintenanceOperationObservation>,
        OfflineMaintenanceObservationPortError,
    >,
}

struct ServerRecoveryOfflineMaintenanceCoordinator {
    restore: BlockingPortExecutor<
        RecoveryOfflineMaintenanceRestore,
        OfflineMaintenanceStartResult,
        RecoveryOfflineMaintenancePortError,
    >,
}

struct ServerRestoreRetryOfflineMaintenanceCoordinator {
    restore: BlockingPortExecutor<
        AuthorizedRestoreRetryStart,
        OfflineMaintenanceStartResult,
        OfflineMaintenanceStartPortError,
    >,
}

impl ServerRestoreRetryOfflineMaintenanceCoordinator {
    fn new(
        controller: MaintenanceController,
        driver: &BlockingPortDriver,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Self {
        let restore = driver.executor(move |request| {
            admit_restore_retry(&controller, operation_id, input_hash, request)
        });
        Self { restore }
    }
}

impl RestoreRetryOfflineMaintenanceCoordinatorPort
    for ServerRestoreRetryOfflineMaintenanceCoordinator
{
    fn reserve_restore(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, RestoreRetryOfflineMaintenancePermit, PortAdmissionError> {
        let reservation = self.restore.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerRestoreRetryOfflineMaintenanceCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerRestoreRetryOfflineMaintenanceCoordinator([EXACT_RESTORE])")
    }
}

impl ServerRecoveryOfflineMaintenanceCoordinator {
    fn new(controller: MaintenanceController, driver: &BlockingPortDriver) -> Self {
        let restore = driver.executor(move |request| admit_recovery_restore(&controller, request));
        Self { restore }
    }
}

impl RecoveryOfflineMaintenanceCoordinatorPort for ServerRecoveryOfflineMaintenanceCoordinator {
    fn reserve_restore(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, RecoveryOfflineMaintenanceRestorePermit, PortAdmissionError> {
        let reservation = self.restore.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerRecoveryOfflineMaintenanceCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerRecoveryOfflineMaintenanceCoordinator([STAGED_ONLY])")
    }
}

impl ServerOfflineMaintenanceCoordinator {
    fn new(controller: MaintenanceController, driver: &BlockingPortDriver) -> Self {
        let observation_controller = controller.clone();
        let start = driver.executor(move |request| admit_start(&controller, request));
        let observation =
            driver.executor(move |request| observe_receipt(&observation_controller, request));
        Self { start, observation }
    }
}

impl OfflineMaintenanceCoordinatorPort for ServerOfflineMaintenanceCoordinator {
    fn reserve_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, OfflineMaintenanceStartPermit, PortAdmissionError> {
        let reservation = self.start.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, OfflineMaintenanceObservationPermit, PortAdmissionError> {
        let reservation = self.observation.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerOfflineMaintenanceCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerOfflineMaintenanceCoordinator([RECEIPT_BACKED])")
    }
}

struct PreparedStart {
    receipt: OfflineMaintenanceReceiptV1,
    trigger: MaintenanceTrigger,
    start_ready: Option<oneshot::Sender<()>>,
}

fn admit_restore_retry(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
    input_hash: OfflineMaintenanceInputHash,
    retry: AuthorizedRestoreRetryStart,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    if retry.request().operation_id() != operation_id || retry.request().input_hash() != input_hash
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    if !controller
        .lifecycle
        .credential_retry_restore_matches(operation_id, input_hash)
    {
        return Err(OfflineMaintenanceStartPortError::Unavailable);
    }
    let (request, authorization, credential) = retry.into_parts();
    admit_start(
        controller,
        AuthorizedOfflineMaintenanceStart::RestoreBackup {
            request,
            authorization,
            credential,
        },
    )
}

fn admit_recovery_restore(
    controller: &MaintenanceController,
    restore: RecoveryOfflineMaintenanceRestore,
) -> RecoveryMaintenanceResult {
    let operation_id = restore.request().operation_id();
    claim_recovery_lifecycle(controller, operation_id)?;
    let (sender, receiver) = sync_mpsc::sync_channel(1);
    let trigger = MaintenanceTrigger::RecoveryRestore {
        restore,
        completion: RecoveryMaintenanceCompletion { sender },
    };
    if let Err(error) = controller.triggers.try_send(trigger) {
        drop(error.into_inner());
        controller.lifecycle.fail_closed(operation_id);
        return Err(RecoveryOfflineMaintenancePortError::Unavailable);
    }
    receiver
        .recv()
        .unwrap_or(Err(RecoveryOfflineMaintenancePortError::OutcomeUnknown))
}

fn claim_recovery_lifecycle(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<(), RecoveryOfflineMaintenancePortError> {
    let admission = match controller.recovery_admission.lock() {
        Ok(admission) => admission,
        Err(poisoned) => {
            drop(poisoned.into_inner());
            controller.lifecycle.fail_closed(operation_id);
            return Err(RecoveryOfflineMaintenancePortError::Integrity);
        }
    };
    if !controller.lifecycle.recovery_restore_available() {
        return Err(RecoveryOfflineMaintenancePortError::Unavailable);
    }
    let result = controller
        .lifecycle
        .begin_recovery_restore(operation_id)
        .map_err(|_| RecoveryOfflineMaintenancePortError::Integrity);
    drop(admission);
    result
}

fn admit_start(
    controller: &MaintenanceController,
    request: AuthorizedOfflineMaintenanceStart,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let mut prepared = prepare_start(request)?;
    let operation_id = prepared.receipt.operation_id();
    let mut storage = lock_start_storage(controller, operation_id)?;

    let existing = storage
        .read_receipt(operation_id)
        .map_err(|error| map_start_read_error(controller, operation_id, error))?;
    let start_ready = prepared
        .start_ready
        .take()
        .expect("a prepared maintenance start owns one readiness sender");
    let result = if let Some(existing) = existing {
        resolve_existing(controller, &mut storage, prepared, existing)
    } else {
        // A previously admitted operation may have closed routing after this
        // request reserved its blocking permit. Only exact duplicates above may
        // resolve while ordinary admission is frozen.
        if !controller.lifecycle.ordinary_admission_available() {
            return Err(OfflineMaintenanceStartPortError::Unavailable);
        }

        match storage
            .create_or_read_receipt(&prepared.receipt)
            .map_err(|error| map_receipt_create_error(controller, operation_id, error))?
        {
            OfflineMaintenanceReceiptCreateResultV1::Existing(existing) => {
                resolve_existing(controller, &mut storage, prepared, *existing)
            }
            OfflineMaintenanceReceiptCreateResultV1::Created => {
                admit_created(controller, &mut storage, prepared)
            }
        }
    };
    drop(storage);
    let _ = start_ready.send(());
    result
}

fn prepare_start(
    request: AuthorizedOfflineMaintenanceStart,
) -> Result<PreparedStart, OfflineMaintenanceStartPortError> {
    match request {
        AuthorizedOfflineMaintenanceStart::CreateBackup {
            request,
            authorization,
        } => {
            let (start_ready, ready) = oneshot::channel();
            let policy_request = OfflineMaintenanceAuthorizationRequest::create_backup(
                request.operation_id(),
                request.input_hash(),
            );
            ensure_exact_proof(&authorization, &policy_request)?;
            let admission = admission_from_proof(&authorization);
            let mut receipt = OfflineMaintenanceReceiptV1::accepted(
                request.operation_id(),
                OfflineMaintenanceOperationKind::CreateBackup,
                request.backup_name().clone(),
                request.input_hash(),
                OfflineMaintenanceReplacementConfirmation::NotProvided,
                admission,
            )
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
            receipt
                .record_source_database_id(authorization.database_id())
                .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
            Ok(PreparedStart {
                receipt,
                trigger: MaintenanceTrigger::CreateBackup {
                    request,
                    start_ready: ready,
                },
                start_ready: Some(start_ready),
            })
        }
        AuthorizedOfflineMaintenanceStart::RestoreBackup {
            request,
            authorization,
            credential,
        } => {
            let (start_ready, ready) = oneshot::channel();
            let policy_request = OfflineMaintenanceAuthorizationRequest::restore_backup(
                request.operation_id(),
                request.input_hash(),
            );
            ensure_exact_proof(&authorization, &policy_request)?;
            let admission = admission_from_proof(&authorization);
            let mut receipt = OfflineMaintenanceReceiptV1::accepted(
                request.operation_id(),
                OfflineMaintenanceOperationKind::RestoreBackup,
                request.backup_name().clone(),
                request.input_hash(),
                request.confirmation(),
                admission,
            )
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
            receipt
                .record_source_database_id(authorization.database_id())
                .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
            Ok(PreparedStart {
                receipt,
                trigger: MaintenanceTrigger::RestoreBackup {
                    request,
                    credential,
                    start_ready: ready,
                },
                start_ready: Some(start_ready),
            })
        }
    }
}

fn ensure_exact_proof(
    authorization: &AuthorizedOfflineMaintenance,
    expected: &OfflineMaintenanceAuthorizationRequest,
) -> Result<(), OfflineMaintenanceStartPortError> {
    if authorization.request() == expected {
        Ok(())
    } else {
        Err(OfflineMaintenanceStartPortError::Integrity)
    }
}

fn admission_from_proof(
    authorization: &AuthorizedOfflineMaintenance,
) -> OfflineMaintenanceAdmissionV1 {
    OfflineMaintenanceAdmissionV1::new(
        authorization.principal_id().clone(),
        authorization.actor_kind(),
        authorization.authorizing_capability_id(),
        authorization.obligations().validated_approval().cloned(),
    )
}

fn admit_created(
    controller: &MaintenanceController,
    storage: &mut RedbMaintenanceStorage,
    prepared: PreparedStart,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let PreparedStart {
        receipt, trigger, ..
    } = prepared;
    let operation_id = receipt.operation_id();
    if controller.lifecycle.begin(operation_id).is_err() {
        // A restore bearer must be gone before a terminal receipt is persisted.
        drop(trigger);
        return fail_without_driver(storage, receipt);
    }

    enqueue_claimed(
        controller,
        storage,
        receipt,
        trigger,
        OfflineMaintenanceStartDisposition::Accepted,
    )
}

fn enqueue_claimed(
    controller: &MaintenanceController,
    storage: &mut RedbMaintenanceStorage,
    receipt: OfflineMaintenanceReceiptV1,
    trigger: MaintenanceTrigger,
    disposition: OfflineMaintenanceStartDisposition,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let operation_id = receipt.operation_id();
    let receipt = if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Accepted {
        match persist_draining(storage, receipt) {
            Ok(DrainingPersistence::Durable(receipt)) => receipt,
            Ok(DrainingPersistence::NotAdvanced(receipt)) => {
                drop(trigger);
                controller.lifecycle.fail_closed(operation_id);
                return fail_without_driver(storage, receipt);
            }
            Err(error) => {
                drop(trigger);
                controller.lifecycle.fail_closed(operation_id);
                return Err(error);
            }
        }
    } else {
        receipt
    };

    match controller.triggers.try_send(trigger) {
        Ok(()) => start_result(disposition, &receipt),
        Err(error) => {
            drop(error.into_inner());
            controller.lifecycle.fail_closed(operation_id);
            fail_without_driver(storage, receipt)
        }
    }
}

enum DrainingPersistence {
    Durable(OfflineMaintenanceReceiptV1),
    NotAdvanced(OfflineMaintenanceReceiptV1),
}

fn persist_draining(
    storage: &mut RedbMaintenanceStorage,
    receipt: OfflineMaintenanceReceiptV1,
) -> Result<DrainingPersistence, OfflineMaintenanceStartPortError> {
    let mut draining = receipt.clone();
    draining
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Draining,
        ))
        .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
    if storage.replace_receipt(&draining).is_ok() {
        return Ok(DrainingPersistence::Durable(draining));
    }

    // A failed replace can include an uncertain parent-directory sync. Resolve
    // only from a complete checksum-validated reread of this exact receipt.
    match storage.read_receipt(receipt.operation_id()) {
        Ok(Some(observed)) if observed == draining => Ok(DrainingPersistence::Durable(observed)),
        Ok(Some(observed)) if observed == receipt => Ok(DrainingPersistence::NotAdvanced(observed)),
        Ok(Some(_)) => Err(OfflineMaintenanceStartPortError::Integrity),
        Ok(None) | Err(_) => Err(OfflineMaintenanceStartPortError::OutcomeUnknown),
    }
}

fn fail_without_driver(
    storage: &mut RedbMaintenanceStorage,
    mut receipt: OfflineMaintenanceReceiptV1,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::failed(
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ))
        .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
    storage
        .replace_receipt(&receipt)
        .map_err(|_| OfflineMaintenanceStartPortError::OutcomeUnknown)?;
    start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt)
}

fn resolve_existing(
    controller: &MaintenanceController,
    storage: &mut RedbMaintenanceStorage,
    prepared: PreparedStart,
    existing: OfflineMaintenanceReceiptV1,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let PreparedStart {
        receipt, trigger, ..
    } = prepared;
    if existing.operation_id() != receipt.operation_id() {
        return Err(OfflineMaintenanceStartPortError::Integrity);
    }
    if existing.operation_kind() != receipt.operation_kind()
        || existing.backup_name() != receipt.backup_name()
        || existing.input_hash() != receipt.input_hash()
        || existing.replacement_confirmation() != receipt.replacement_confirmation()
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    match (
        existing.operation_kind(),
        existing.source_database_id(),
        receipt.source_database_id(),
    ) {
        (
            OfflineMaintenanceOperationKind::CreateBackup,
            Some(existing_source),
            Some(current_source),
        )
        | (
            OfflineMaintenanceOperationKind::RestoreBackup,
            Some(existing_source),
            Some(current_source),
        ) if existing_source == current_source => {}
        // Recovery-mode restore has no trustworthy current database. Once it
        // has restored readiness, an exact normal retry must still resolve its
        // receipt rather than invent a source identity.
        (OfflineMaintenanceOperationKind::RestoreBackup, None, Some(_)) => {}
        _ => return Err(OfflineMaintenanceStartPortError::Integrity),
    }
    if existing.current_phase().is_terminal() {
        drop(trigger);
        return start_result(OfflineMaintenanceStartDisposition::Terminal, &existing);
    }

    match controller
        .lifecycle
        .claim_nonterminal_receipt(existing.operation_id())
        .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?
    {
        MaintenanceReceiptClaim::AlreadyActive => {
            drop(trigger);
            start_result(
                OfflineMaintenanceStartDisposition::AlreadyAccepted,
                &existing,
            )
        }
        MaintenanceReceiptClaim::Reacquired => enqueue_claimed(
            controller,
            storage,
            existing,
            trigger,
            OfflineMaintenanceStartDisposition::AlreadyAccepted,
        ),
    }
}

fn observe_receipt(
    controller: &MaintenanceController,
    observation: AuthorizedOfflineMaintenanceObservation,
) -> Result<Option<OfflineMaintenanceOperationObservation>, OfflineMaintenanceObservationPortError>
{
    let (request, authorization) = observation.into_parts();
    let operation_id = request.operation_id();
    if authorization.request()
        != &OfflineMaintenanceAuthorizationRequest::get_operation(operation_id)
    {
        return Err(OfflineMaintenanceObservationPortError::Integrity);
    }

    let mut storage = match controller.storage.lock() {
        Ok(storage) => storage,
        Err(poisoned) => {
            drop(poisoned.into_inner());
            controller.lifecycle.fail_closed(operation_id);
            return Err(OfflineMaintenanceObservationPortError::Integrity);
        }
    };
    if !controller.lifecycle.ordinary_admission_available() {
        return Err(OfflineMaintenanceObservationPortError::Unavailable);
    }
    storage
        .read_receipt(operation_id)
        .map_err(|error| map_observation_error(controller, operation_id, error))?
        .as_ref()
        .map(receipt_observation)
        .transpose()
}

pub(crate) fn start_result(
    disposition: OfflineMaintenanceStartDisposition,
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let observation =
        receipt_observation(receipt).map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
    OfflineMaintenanceStartResult::new(disposition, observation)
        .map_err(|_| OfflineMaintenanceStartPortError::Integrity)
}

fn receipt_observation(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<OfflineMaintenanceOperationObservation, OfflineMaintenanceObservationPortError> {
    let transition = receipt
        .transitions()
        .last()
        .copied()
        .ok_or(OfflineMaintenanceObservationPortError::Integrity)?;
    OfflineMaintenanceOperationObservation::new(
        receipt.operation_id(),
        receipt.operation_kind(),
        receipt.backup_name().clone(),
        receipt.input_hash(),
        map_phase(transition.receipt_phase()),
        transition.failure().map(map_failure),
    )
    .map_err(|_| OfflineMaintenanceObservationPortError::Integrity)
}

const fn map_phase(phase: OfflineMaintenanceReceiptPhaseV1) -> OfflineMaintenanceObservationPhase {
    match phase {
        OfflineMaintenanceReceiptPhaseV1::Accepted => OfflineMaintenanceObservationPhase::Accepted,
        OfflineMaintenanceReceiptPhaseV1::Draining => OfflineMaintenanceObservationPhase::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline => OfflineMaintenanceObservationPhase::Offline,
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished => {
            OfflineMaintenanceObservationPhase::ArtifactPublished
        }
        OfflineMaintenanceReceiptPhaseV1::Validating => {
            OfflineMaintenanceObservationPhase::Validating
        }
        OfflineMaintenanceReceiptPhaseV1::Succeeded => {
            OfflineMaintenanceObservationPhase::Succeeded
        }
        OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            OfflineMaintenanceObservationPhase::FailedClosed
        }
    }
}

const fn map_failure(
    failure: OfflineMaintenanceReceiptFailureV1,
) -> OfflineMaintenanceObservationFailure {
    match failure {
        OfflineMaintenanceReceiptFailureV1::QuiescenceFailed => {
            OfflineMaintenanceObservationFailure::QuiescenceFailed
        }
        OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable => {
            OfflineMaintenanceObservationFailure::ArtifactUnavailable
        }
        OfflineMaintenanceReceiptFailureV1::ArtifactInvalid => {
            OfflineMaintenanceObservationFailure::ArtifactInvalid
        }
        OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed => {
            OfflineMaintenanceObservationFailure::StagedAuthorizationFailed
        }
        OfflineMaintenanceReceiptFailureV1::StorageUnavailable => {
            OfflineMaintenanceObservationFailure::StorageUnavailable
        }
        OfflineMaintenanceReceiptFailureV1::ValidationFailed => {
            OfflineMaintenanceObservationFailure::ValidationFailed
        }
        OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable => {
            OfflineMaintenanceObservationFailure::ReceiptUnavailable
        }
        OfflineMaintenanceReceiptFailureV1::InternalFailure => {
            OfflineMaintenanceObservationFailure::InternalFailure
        }
    }
}

fn lock_start_storage(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<std::sync::MutexGuard<'_, RedbMaintenanceStorage>, OfflineMaintenanceStartPortError> {
    match controller.storage.lock() {
        Ok(storage) => Ok(storage),
        Err(poisoned) => {
            drop(poisoned.into_inner());
            controller.lifecycle.fail_closed(operation_id);
            Err(OfflineMaintenanceStartPortError::Integrity)
        }
    }
}

fn map_start_read_error(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
    error: StorageError,
) -> OfflineMaintenanceStartPortError {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            OfflineMaintenanceStartPortError::Unavailable
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted => {
            controller.lifecycle.fail_closed(operation_id);
            OfflineMaintenanceStartPortError::Integrity
        }
    }
}

fn map_receipt_create_error(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
    error: StorageError,
) -> OfflineMaintenanceStartPortError {
    match error.kind() {
        StorageErrorKind::Unavailable => OfflineMaintenanceStartPortError::Unavailable,
        StorageErrorKind::CommitStatusUnknown => {
            controller.lifecycle.fail_closed(operation_id);
            OfflineMaintenanceStartPortError::OutcomeUnknown
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted => {
            controller.lifecycle.fail_closed(operation_id);
            OfflineMaintenanceStartPortError::Integrity
        }
    }
}

fn map_observation_error(
    controller: &MaintenanceController,
    operation_id: OfflineMaintenanceOperationId,
    error: StorageError,
) -> OfflineMaintenanceObservationPortError {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            OfflineMaintenanceObservationPortError::Unavailable
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted => {
            controller.lifecycle.fail_closed(operation_id);
            OfflineMaintenanceObservationPortError::Integrity
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::OfflineMaintenanceReceiptV1;
    use riffdb_types::{ActorId, BackupNameV1, CapabilityId, DatabaseId};

    use super::*;

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-maintenance-adapter-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn operation_id(seed: u8) -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
            .expect("operation ID")
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(2, [seed; 10]).expect("database ID")
    }

    fn prepared_create(seed: u8) -> PreparedStart {
        let operation_id = operation_id(seed);
        let backup_name = BackupNameV1::new("snapshot").expect("backup name");
        let request =
            CreateOfflineBackupRequest::new(operation_id, backup_name.clone()).expect("request");
        let admission = OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").expect("actor ID"),
            riffdb_types::ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(3, [seed; 10]).expect("capability ID"),
            None,
        );
        let mut receipt = OfflineMaintenanceReceiptV1::accepted(
            operation_id,
            OfflineMaintenanceOperationKind::CreateBackup,
            backup_name,
            request.input_hash(),
            OfflineMaintenanceReplacementConfirmation::NotProvided,
            admission,
        )
        .expect("accepted receipt");
        receipt
            .record_source_database_id(database_id(seed))
            .expect("source database");
        let (start_ready, ready) = oneshot::channel();
        PreparedStart {
            receipt,
            trigger: MaintenanceTrigger::CreateBackup {
                request,
                start_ready: ready,
            },
            start_ready: Some(start_ready),
        }
    }

    fn controller(
        lifecycle: Arc<MaintenanceLifecycle>,
    ) -> (
        TestDirectory,
        MaintenanceController,
        mpsc::Receiver<MaintenanceTrigger>,
    ) {
        let directory = TestDirectory::new();
        let database = directory.0.join("riffdb.redb");
        let backup_root = directory.0.join("backups");
        let (storage, _) =
            RedbMaintenanceStorage::open(database, backup_root).expect("maintenance storage");
        let storage = shared_maintenance_storage(storage);
        let (sender, receiver) = maintenance_trigger_channel();
        (
            directory,
            MaintenanceController::new(storage, lifecycle, sender),
            receiver,
        )
    }

    #[test]
    fn durable_phase_mapping_is_exhaustive() {
        let cases = [
            (
                OfflineMaintenanceReceiptPhaseV1::Accepted,
                OfflineMaintenanceObservationPhase::Accepted,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::Draining,
                OfflineMaintenanceObservationPhase::Draining,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::Offline,
                OfflineMaintenanceObservationPhase::Offline,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
                OfflineMaintenanceObservationPhase::ArtifactPublished,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::Validating,
                OfflineMaintenanceObservationPhase::Validating,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::Succeeded,
                OfflineMaintenanceObservationPhase::Succeeded,
            ),
            (
                OfflineMaintenanceReceiptPhaseV1::FailedClosed,
                OfflineMaintenanceObservationPhase::FailedClosed,
            ),
        ];
        for (durable, public) in cases {
            assert_eq!(map_phase(durable), public);
        }
    }

    #[test]
    fn durable_failure_mapping_is_exhaustive() {
        let cases = [
            (
                OfflineMaintenanceReceiptFailureV1::QuiescenceFailed,
                OfflineMaintenanceObservationFailure::QuiescenceFailed,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
                OfflineMaintenanceObservationFailure::ArtifactUnavailable,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::ArtifactInvalid,
                OfflineMaintenanceObservationFailure::ArtifactInvalid,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed,
                OfflineMaintenanceObservationFailure::StagedAuthorizationFailed,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::StorageUnavailable,
                OfflineMaintenanceObservationFailure::StorageUnavailable,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::ValidationFailed,
                OfflineMaintenanceObservationFailure::ValidationFailed,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
                OfflineMaintenanceObservationFailure::ReceiptUnavailable,
            ),
            (
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
                OfflineMaintenanceObservationFailure::InternalFailure,
            ),
        ];
        for (durable, public) in cases {
            assert_eq!(map_failure(durable), public);
        }
    }

    #[test]
    fn orphaned_exact_receipt_is_reacquired_and_enqueued_once() {
        let lifecycle = Arc::new(MaintenanceLifecycle::ready());
        let (_directory, controller, mut receiver) = controller(Arc::clone(&lifecycle));
        let prepared = prepared_create(7);
        let existing = prepared.receipt.clone();
        let operation_id = existing.operation_id();
        let storage = controller.storage();
        let mut storage = storage.lock().expect("storage lock");
        assert_eq!(
            storage
                .create_or_read_receipt(&existing)
                .expect("persist accepted receipt"),
            OfflineMaintenanceReceiptCreateResultV1::Created
        );
        let result = resolve_existing(&controller, &mut storage, prepared, existing)
            .expect("resolve existing");

        assert_eq!(
            result.disposition(),
            OfflineMaintenanceStartDisposition::AlreadyAccepted
        );
        assert_eq!(
            result.operation().phase(),
            OfflineMaintenanceObservationPhase::Draining
        );
        assert_eq!(
            lifecycle.stage(),
            crate::maintenance_lifecycle::MaintenanceLifecycleStage::Draining
        );
        assert_eq!(
            receiver
                .try_recv()
                .expect("reacquired trigger")
                .operation_id(),
            operation_id
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn live_exact_duplicate_does_not_enqueue_or_poison() {
        let lifecycle = Arc::new(MaintenanceLifecycle::ready());
        let prepared = prepared_create(8);
        let existing = prepared.receipt.clone();
        let operation_id = existing.operation_id();
        lifecycle.begin(operation_id).expect("live operation");
        let (_directory, controller, mut receiver) = controller(Arc::clone(&lifecycle));
        let storage = controller.storage();
        let result = resolve_existing(
            &controller,
            &mut storage.lock().expect("storage lock"),
            prepared,
            existing,
        )
        .expect("resolve duplicate");

        assert_eq!(
            result.disposition(),
            OfflineMaintenanceStartDisposition::AlreadyAccepted
        );
        assert_eq!(
            lifecycle.stage(),
            crate::maintenance_lifecycle::MaintenanceLifecycleStage::Draining
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn recovery_claim_is_exclusive_without_poisoning_a_second_request() {
        let lifecycle = Arc::new(MaintenanceLifecycle::failed_closed());
        let (_directory, controller, _receiver) = controller(Arc::clone(&lifecycle));
        let operation = operation_id(9);

        assert_eq!(claim_recovery_lifecycle(&controller, operation), Ok(()));
        assert_eq!(
            lifecycle.stage(),
            crate::maintenance_lifecycle::MaintenanceLifecycleStage::Offline
        );
        assert_eq!(
            claim_recovery_lifecycle(&controller, operation_id(10)),
            Err(RecoveryOfflineMaintenancePortError::Unavailable)
        );
        assert_eq!(
            lifecycle.stage(),
            crate::maintenance_lifecycle::MaintenanceLifecycleStage::Offline
        );
    }

    #[test]
    fn recovery_completion_is_move_only_and_exact() {
        let (sender, receiver) = sync_mpsc::sync_channel(1);
        RecoveryMaintenanceCompletion { sender }
            .complete(Err(
                RecoveryOfflineMaintenancePortError::AuthorizationDenied,
            ))
            .expect("completion receiver");
        assert_eq!(
            receiver.recv().expect("driver result"),
            Err(RecoveryOfflineMaintenancePortError::AuthorizationDenied)
        );
    }
}
