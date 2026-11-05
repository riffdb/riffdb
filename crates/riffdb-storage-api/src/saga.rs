//! Authoritative saga coordination storage contract (`ADR-0334`).

use riffdb_types::Timestamp;
pub use riffdb_types::{
    CommittedSagaStepRecordV1, MAX_SAGA_INPUT_BYTES_V1, MAX_SAGA_RETAINED_OUTCOME_BYTES_V1,
    MAX_SAGA_RETRY_COUNT_V1, MAX_SAGA_STEP_LEDGER_ENTRIES_V1, MAX_SAGA_STEPS_V1,
    MAX_SAGA_SYMBOL_BYTES_V1, MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1, PendingStepInvocationV1,
    SAGA_INSTANCE_MAGIC, SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1, SagaCompareExpectedV1,
    SagaFencingToken, SagaId, SagaInstanceErrorV1, SagaLeaseStateV1, SagaLifecycleStatusV1,
    SagaRetainedAuthorityV1, SagaRevision, SagaStepDirectionV1, SagaStepEffectDispositionV1,
    SagaTerminalOutcomeRecordV1, StoredSagaInstanceV1, compute_saga_partition_hash,
    derive_saga_step_idempotency_key, derive_saga_step_idempotency_uuid,
    format_saga_delegated_actor_id, is_encoded_saga_instance_v1,
};

use crate::StorageError;

/// Authoritative storage port for durable multi-step saga coordination (`ADR-0334`).
pub trait SagaCoordinationRepository {
    /// Atomically admits a new saga instance or replays/rejects an existing instance
    /// matching `(database_id, tenant_scope, contract_lineage, saga_name, caller_idempotency_key)`.
    fn admit_or_get_saga(
        &self,
        instance: &StoredSagaInstanceV1,
    ) -> Result<SagaAdmissionStorageOutcomeV1, StorageError>;

    /// Reads the current durable state of one saga instance by its UUIDv7 identifier.
    fn read_saga(&self, saga_id: SagaId) -> Result<Option<StoredSagaInstanceV1>, StorageError>;

    /// Atomically transitions a saga instance when its stored revision and optional fencing token match `expected`.
    fn compare_and_swap_saga(
        &self,
        expected: SagaCompareExpectedV1,
        next: &StoredSagaInstanceV1,
    ) -> Result<SagaCasOutcomeV1, StorageError>;

    /// Claims up to `max_claims` due sagas in fair partition-hash order starting at `cursor_partition_hash`,
    /// atomically bumping each claimed instance's `revision` and `fencing_token` and assigning a worker lease.
    fn claim_due_sagas(
        &self,
        owner_id: [u8; 16],
        now: Timestamp,
        lease_expires_at: Timestamp,
        max_claims: usize,
        cursor_partition_hash: u64,
    ) -> Result<Vec<StoredSagaInstanceV1>, StorageError>;
}
