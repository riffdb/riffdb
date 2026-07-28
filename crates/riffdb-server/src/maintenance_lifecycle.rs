//! Private exclusive lifecycle for offline maintenance.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use riffdb_types::{OfflineMaintenanceInputHash, OfflineMaintenanceOperationId};

/// Closed server-private maintenance routing stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceLifecycleStage {
    Ready,
    AwaitingRestoreRetry,
    AwaitingRecoveryRetry,
    Draining,
    Offline,
    Validating,
    FailedClosed,
}

/// Result of claiming a matching nonterminal receipt after exact-input resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceReceiptClaim {
    /// This process had no driver and now owns the receipt in `Draining`.
    Reacquired,
    /// This process already has the one driver for this operation.
    AlreadyActive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MaintenanceLifecycleState {
    stage: MaintenanceLifecycleStage,
    operation_id: Option<OfflineMaintenanceOperationId>,
    input_hash: Option<OfflineMaintenanceInputHash>,
    exact_recovery_retry: bool,
}

/// The one process authority for maintenance admission and phase movement.
pub(crate) struct MaintenanceLifecycle {
    state: Mutex<MaintenanceLifecycleState>,
}

impl MaintenanceLifecycle {
    pub(crate) const fn ready() -> Self {
        Self {
            state: Mutex::new(MaintenanceLifecycleState {
                stage: MaintenanceLifecycleStage::Ready,
                operation_id: None,
                input_hash: None,
                exact_recovery_retry: false,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) const fn failed_closed() -> Self {
        Self {
            state: Mutex::new(MaintenanceLifecycleState {
                stage: MaintenanceLifecycleStage::FailedClosed,
                operation_id: None,
                input_hash: None,
                exact_recovery_retry: false,
            }),
        }
    }

    pub(crate) fn stage(&self) -> MaintenanceLifecycleStage {
        self.lock().stage
    }

    pub(crate) fn ordinary_admission_available(&self) -> bool {
        self.stage() == MaintenanceLifecycleStage::Ready
    }

    pub(crate) fn credential_retry_restore_available(&self) -> bool {
        self.stage() == MaintenanceLifecycleStage::AwaitingRestoreRetry
    }

    pub(crate) fn credential_retry_restore_matches(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> bool {
        let state = self.lock();
        state.stage == MaintenanceLifecycleStage::AwaitingRestoreRetry
            && state.operation_id == Some(operation_id)
            && state.input_hash == Some(input_hash)
    }

    pub(crate) fn recovery_restore_available(&self) -> bool {
        let state = self.lock();
        matches!(
            (state.stage, state.operation_id),
            (MaintenanceLifecycleStage::FailedClosed, None)
                | (MaintenanceLifecycleStage::AwaitingRecoveryRetry, Some(_))
        )
    }

    pub(crate) fn recovery_restore_matches(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> bool {
        let state = self.lock();
        matches!(
            (state.stage, state.operation_id, state.input_hash),
            (MaintenanceLifecycleStage::FailedClosed, None, None)
        ) || matches!(
            (state.stage, state.operation_id, state.input_hash),
            (
                MaintenanceLifecycleStage::AwaitingRecoveryRetry,
                Some(expected_operation),
                Some(expected_input)
            ) if expected_operation == operation_id && expected_input == input_hash
        )
    }

    /// Enters generic staged-only recovery after current startup cannot be trusted.
    pub(crate) fn enter_recovery_mode(&self) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        if state.stage != MaintenanceLifecycleStage::Ready || state.operation_id.is_some() {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.input_hash = None;
            state.exact_recovery_retry = false;
            return Err(MaintenanceLifecycleError);
        }
        state.stage = MaintenanceLifecycleStage::FailedClosed;
        state.input_hash = None;
        state.exact_recovery_retry = false;
        Ok(())
    }

    /// Closes ordinary readiness until the exact retained restore is retried.
    pub(crate) fn await_restore_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Result<(), MaintenanceLifecycleError> {
        self.await_exact_retry(
            operation_id,
            input_hash,
            MaintenanceLifecycleStage::AwaitingRestoreRetry,
            false,
        )
    }

    /// Closes ordinary readiness until the exact source-less restore is retried.
    pub(crate) fn await_recovery_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Result<(), MaintenanceLifecycleError> {
        self.await_exact_retry(
            operation_id,
            input_hash,
            MaintenanceLifecycleStage::AwaitingRecoveryRetry,
            true,
        )
    }

    pub(crate) fn begin(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        self.transition(
            operation_id,
            MaintenanceLifecycleStage::Ready,
            MaintenanceLifecycleStage::Draining,
            true,
        )
    }

    /// Claims an exact nonterminal receipt or observes its current local owner.
    ///
    /// A resumable receipt is normally claimed from `Ready`. A restore awaiting
    /// a replacement credential is claimed from `AwaitingRestoreRetry`. This
    /// operation atomically installs either as the current driver. An exact
    /// retry against an already active operation does not move or poison state.
    pub(crate) fn claim_nonterminal_receipt(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<MaintenanceReceiptClaim, MaintenanceLifecycleError> {
        let mut state = self.lock();
        match (state.stage, state.operation_id) {
            (MaintenanceLifecycleStage::Ready, None) => {
                state.stage = MaintenanceLifecycleStage::Draining;
                state.operation_id = Some(operation_id);
                Ok(MaintenanceReceiptClaim::Reacquired)
            }
            (MaintenanceLifecycleStage::AwaitingRestoreRetry, Some(awaited))
                if awaited == operation_id =>
            {
                state.stage = MaintenanceLifecycleStage::Draining;
                Ok(MaintenanceReceiptClaim::Reacquired)
            }
            (
                MaintenanceLifecycleStage::Draining
                | MaintenanceLifecycleStage::Offline
                | MaintenanceLifecycleStage::Validating,
                Some(active),
            ) if active == operation_id => Ok(MaintenanceReceiptClaim::AlreadyActive),
            _ => {
                state.stage = MaintenanceLifecycleStage::FailedClosed;
                state.input_hash = None;
                Err(MaintenanceLifecycleError)
            }
        }
    }

    pub(crate) fn begin_recovery_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        match (state.stage, state.operation_id) {
            (MaintenanceLifecycleStage::FailedClosed, None) => {
                state.stage = MaintenanceLifecycleStage::Offline;
                state.operation_id = Some(operation_id);
                state.exact_recovery_retry = false;
                Ok(())
            }
            (MaintenanceLifecycleStage::AwaitingRecoveryRetry, Some(expected))
                if expected == operation_id =>
            {
                state.stage = MaintenanceLifecycleStage::Offline;
                state.exact_recovery_retry = true;
                Ok(())
            }
            _ => Err(MaintenanceLifecycleError),
        }
    }

    /// Releases a recovery attempt that provably created no receipt or publication.
    pub(crate) fn release_recovery_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        if state.stage != MaintenanceLifecycleStage::Offline
            || state.operation_id != Some(operation_id)
        {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.input_hash = None;
            state.exact_recovery_retry = false;
            return Err(MaintenanceLifecycleError);
        }
        if state.exact_recovery_retry {
            state.stage = MaintenanceLifecycleStage::AwaitingRecoveryRetry;
        } else {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.operation_id = None;
            state.input_hash = None;
        }
        Ok(())
    }

    pub(crate) fn mark_offline(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        self.transition(
            operation_id,
            MaintenanceLifecycleStage::Draining,
            MaintenanceLifecycleStage::Offline,
            false,
        )
    }

    pub(crate) fn mark_validating(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        self.transition(
            operation_id,
            MaintenanceLifecycleStage::Offline,
            MaintenanceLifecycleStage::Validating,
            false,
        )
    }

    pub(crate) fn finish_ready(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        if state.stage != MaintenanceLifecycleStage::Validating
            || state.operation_id != Some(operation_id)
        {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.input_hash = None;
            return Err(MaintenanceLifecycleError);
        }
        state.stage = MaintenanceLifecycleStage::Ready;
        state.operation_id = None;
        state.input_hash = None;
        state.exact_recovery_retry = false;
        Ok(())
    }

    pub(crate) fn fail_closed(&self, operation_id: OfflineMaintenanceOperationId) {
        let mut state = self.lock();
        state.stage = MaintenanceLifecycleStage::FailedClosed;
        state.operation_id = Some(operation_id);
        state.input_hash = None;
        state.exact_recovery_retry = false;
    }

    fn await_exact_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
        next: MaintenanceLifecycleStage,
        exact_recovery_retry: bool,
    ) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        if state.stage != MaintenanceLifecycleStage::Ready
            || state.operation_id.is_some()
            || state.input_hash.is_some()
        {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.input_hash = None;
            state.exact_recovery_retry = false;
            return Err(MaintenanceLifecycleError);
        }
        state.stage = next;
        state.operation_id = Some(operation_id);
        state.input_hash = Some(input_hash);
        state.exact_recovery_retry = exact_recovery_retry;
        Ok(())
    }

    fn transition(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        expected: MaintenanceLifecycleStage,
        next: MaintenanceLifecycleStage,
        install_operation: bool,
    ) -> Result<(), MaintenanceLifecycleError> {
        let mut state = self.lock();
        let operation_matches = if install_operation {
            state.operation_id.is_none()
        } else {
            state.operation_id == Some(operation_id)
        };
        if state.stage != expected || !operation_matches {
            state.stage = MaintenanceLifecycleStage::FailedClosed;
            state.input_hash = None;
            state.exact_recovery_retry = false;
            return Err(MaintenanceLifecycleError);
        }
        state.stage = next;
        if install_operation {
            state.operation_id = Some(operation_id);
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, MaintenanceLifecycleState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.stage = MaintenanceLifecycleStage::FailedClosed;
                state.input_hash = None;
                state.exact_recovery_retry = false;
                state
            }
        }
    }
}

impl fmt::Debug for MaintenanceLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MaintenanceLifecycle([PRIVATE_STATE])")
    }
}

/// A phase transition did not match the retained operation and exact stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceLifecycleError;

impl fmt::Display for MaintenanceLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("offline maintenance lifecycle transition failed")
    }
}

impl std::error::Error for MaintenanceLifecycleError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation_id(byte: u8) -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [byte; 10])
            .expect("valid operation ID")
    }

    fn input_hash(byte: u8) -> OfflineMaintenanceInputHash {
        OfflineMaintenanceInputHash::from_bytes([byte; 32])
    }

    #[test]
    fn ordinary_lifecycle_is_exact_and_exclusive() {
        let lifecycle = MaintenanceLifecycle::ready();
        let operation = operation_id(1);
        assert!(lifecycle.ordinary_admission_available());
        assert!(!lifecycle.recovery_restore_available());

        lifecycle.begin(operation).expect("begin");
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Draining);
        assert!(!lifecycle.ordinary_admission_available());
        lifecycle.mark_offline(operation).expect("offline");
        lifecycle.mark_validating(operation).expect("validating");
        lifecycle.finish_ready(operation).expect("ready");

        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Ready);
        assert!(lifecycle.ordinary_admission_available());
    }

    #[test]
    fn mismatched_operation_fails_closed_without_reopening() {
        let lifecycle = MaintenanceLifecycle::ready();
        lifecycle.begin(operation_id(1)).expect("begin");
        assert_eq!(
            lifecycle.mark_offline(operation_id(2)),
            Err(MaintenanceLifecycleError)
        );
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::FailedClosed);
        assert!(!lifecycle.ordinary_admission_available());
        assert!(!lifecycle.recovery_restore_available());
    }

    #[test]
    fn recovery_route_admits_only_restore_ownership_from_failed_closed() {
        let lifecycle = MaintenanceLifecycle::failed_closed();
        let operation = operation_id(3);
        assert!(!lifecycle.ordinary_admission_available());
        assert!(lifecycle.recovery_restore_available());
        lifecycle
            .begin_recovery_restore(operation)
            .expect("recovery restore");
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Offline);
        assert!(!lifecycle.recovery_restore_available());
        lifecycle.mark_validating(operation).expect("validating");
        lifecycle.finish_ready(operation).expect("ready");
        assert!(lifecycle.ordinary_admission_available());
    }

    #[test]
    fn generic_startup_failure_enters_recovery_without_claiming_an_operation() {
        let lifecycle = MaintenanceLifecycle::ready();
        lifecycle.enter_recovery_mode().expect("recovery mode");
        assert!(lifecycle.recovery_restore_available());
        assert!(!lifecycle.ordinary_admission_available());
    }

    #[test]
    fn nonterminal_receipt_is_reacquired_once_without_poisoning_exact_retry() {
        let lifecycle = MaintenanceLifecycle::ready();
        let operation = operation_id(4);
        assert_eq!(
            lifecycle.claim_nonterminal_receipt(operation),
            Ok(MaintenanceReceiptClaim::Reacquired)
        );
        assert_eq!(
            lifecycle.claim_nonterminal_receipt(operation),
            Ok(MaintenanceReceiptClaim::AlreadyActive)
        );
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Draining);
    }

    #[test]
    fn credential_retry_is_reacquired_by_the_exact_operation() {
        let lifecycle = MaintenanceLifecycle::ready();
        let operation = operation_id(5);
        let expected_input = input_hash(5);
        lifecycle
            .await_restore_retry(operation, expected_input)
            .expect("install exact retry");

        assert!(!lifecycle.ordinary_admission_available());
        assert!(lifecycle.credential_retry_restore_available());
        assert!(lifecycle.credential_retry_restore_matches(operation, expected_input));
        assert!(!lifecycle.credential_retry_restore_matches(operation, input_hash(6)));
        assert!(!lifecycle.recovery_restore_available());
        assert_eq!(
            lifecycle.claim_nonterminal_receipt(operation),
            Ok(MaintenanceReceiptClaim::Reacquired)
        );
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Draining);
        assert!(!lifecycle.credential_retry_restore_available());
        assert_eq!(
            lifecycle.claim_nonterminal_receipt(operation),
            Ok(MaintenanceReceiptClaim::AlreadyActive)
        );
    }

    #[test]
    fn credential_retry_rejects_a_different_operation_fail_closed() {
        let lifecycle = MaintenanceLifecycle::ready();
        lifecycle
            .await_restore_retry(operation_id(6), input_hash(6))
            .expect("install exact retry");

        assert_eq!(
            lifecycle.claim_nonterminal_receipt(operation_id(7)),
            Err(MaintenanceLifecycleError)
        );
        assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::FailedClosed);
        assert!(!lifecycle.credential_retry_restore_available());
        assert!(!lifecycle.recovery_restore_available());
    }

    #[test]
    fn exact_recovery_retry_rejects_a_different_operation_without_losing_constraint() {
        let lifecycle = MaintenanceLifecycle::ready();
        let expected = operation_id(8);
        let expected_input = input_hash(8);
        lifecycle
            .await_recovery_retry(expected, expected_input)
            .expect("install exact recovery retry");

        assert!(lifecycle.recovery_restore_available());
        assert!(lifecycle.recovery_restore_matches(expected, expected_input));
        assert!(!lifecycle.recovery_restore_matches(operation_id(9), expected_input));
        assert!(!lifecycle.recovery_restore_matches(expected, input_hash(9)));
        assert_eq!(
            lifecycle.begin_recovery_restore(operation_id(9)),
            Err(MaintenanceLifecycleError)
        );
        assert!(lifecycle.recovery_restore_matches(expected, expected_input));

        lifecycle
            .begin_recovery_restore(expected)
            .expect("claim exact recovery retry");
        lifecycle
            .release_recovery_restore(expected)
            .expect("release pre-receipt attempt");
        assert!(lifecycle.recovery_restore_matches(expected, expected_input));
    }

    #[test]
    fn generic_recovery_attempt_can_be_retried_after_pre_receipt_denial() {
        let lifecycle = MaintenanceLifecycle::failed_closed();
        let first = operation_id(10);
        lifecycle
            .begin_recovery_restore(first)
            .expect("claim generic recovery");
        lifecycle
            .release_recovery_restore(first)
            .expect("release generic recovery");
        assert!(lifecycle.recovery_restore_matches(operation_id(11), input_hash(11)));
    }
}
