//! Runtime output, durable admission, and pre-commit command intent values.

use std::fmt;
use std::sync::Arc;

use riffdb_types::{
    AdmittedActorContext, ApprovalId, CanonicalInputHash, CanonicalRecord, ConflictKeyHash,
    ContractVersion, EntityVersion, EventTypeId, ExecutionFailureCode, IndexEntryKey, LogicalTime,
    MAX_COMMIT_INTENT_SEMANTIC_BYTES, MAX_EVALUATED_COMMAND_SEMANTIC_BYTES, OutcomeId,
    PartitionKey, PartitionKeyHash, ProvenanceId, ProvenanceReason, RequestId, SourceCommit,
    SourceRepository, encode_canonical_record, hash_partition_key,
};

/// Server-validated causal context retained across admission and recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredCommandCausationV1 {
    causing_event_id: riffdb_types::EventId,
    root_request_id: RequestId,
}

impl StoredCommandCausationV1 {
    /// Joins the exact causing event and inherited root request.
    #[must_use]
    pub const fn new(causing_event_id: riffdb_types::EventId, root_request_id: RequestId) -> Self {
        Self {
            causing_event_id,
            root_request_id,
        }
    }
    /// Exact causing durable event.
    #[must_use]
    pub const fn causing_event_id(self) -> riffdb_types::EventId {
        self.causing_event_id
    }
    /// Inherited root request correlation.
    #[must_use]
    pub const fn root_request_id(self) -> RequestId {
        self.root_request_id
    }
}

use crate::{
    EntityTarget, ExecutablePlanRef, IdempotencyIdentity, IdempotencyLookupCandidatesV1,
    IndexRangeTarget, MAX_AFFECTED_INDEX_EPOCH_TARGETS, MAX_COMMAND_READ_TARGETS,
    MAX_ENTITY_MUTATIONS, MAX_EVENT_INTENTS, MAX_READ_DEPENDENCIES, MAX_READ_SNAPSHOT_BYTES,
    MAX_VALIDATION_TARGETS, PartitionIndexTarget, ReadDependencies, ReadSnapshot,
    StorageValueError, ValidationReadRequest, canonical_codec_storage_error,
};

pub(crate) const STORED_COMMAND_CAUSATION_SEMANTIC_BYTES: usize = 28;

/// Fixed provenance ID, partition hash, conflict-count framing, and empty conflict set.
pub const COMMIT_INTENT_FIXED_NON_RUNTIME_SEMANTIC_BYTES: usize = 16 + 32 + 4;

/// Tightest possible conflict-hash count under the exact 64 KiB coordinator reserve.
pub const MAX_COMMIT_CONFLICT_HASHES: usize = (riffdb_types::COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES
    - COMMIT_INTENT_FIXED_NON_RUNTIME_SEMANTIC_BYTES)
    / 32;

/// The immutable v1 runtime-owned limits for one evaluated command.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EvaluationBudget {
    maximum_read_targets: usize,
    maximum_read_dependencies: usize,
    maximum_mutations: usize,
    maximum_events: usize,
    maximum_semantic_bytes: usize,
}

impl EvaluationBudget {
    /// Returns the exact accepted v1 registry budget.
    #[must_use]
    pub const fn v1() -> Self {
        Self {
            maximum_read_targets: MAX_COMMAND_READ_TARGETS,
            maximum_read_dependencies: MAX_READ_DEPENDENCIES,
            maximum_mutations: MAX_ENTITY_MUTATIONS,
            maximum_events: MAX_EVENT_INTENTS,
            maximum_semantic_bytes: MAX_EVALUATED_COMMAND_SEMANTIC_BYTES,
        }
    }

    /// Returns the maximum combined source, root, and range targets.
    #[must_use]
    pub const fn maximum_read_targets(self) -> usize {
        self.maximum_read_targets
    }

    /// Returns the maximum canonical dependency count.
    #[must_use]
    pub const fn maximum_read_dependencies(self) -> usize {
        self.maximum_read_dependencies
    }

    /// Returns the maximum mutation count.
    #[must_use]
    pub const fn maximum_mutations(self) -> usize {
        self.maximum_mutations
    }

    /// Returns the maximum ordered event count.
    #[must_use]
    pub const fn maximum_events(self) -> usize {
        self.maximum_events
    }

    /// Returns the maximum runtime-owned semantic bytes.
    #[must_use]
    pub const fn maximum_semantic_bytes(self) -> usize {
        self.maximum_semantic_bytes
    }
}

/// Policy-approved provenance claims frozen with one pending admission.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct StoredAdmittedProvenanceClaimsV1 {
    source_repository: Option<SourceRepository>,
    source_commit: Option<SourceCommit>,
    reason: Option<ProvenanceReason>,
    approval_id: Option<ApprovalId>,
}

impl StoredAdmittedProvenanceClaimsV1 {
    /// Constructs the exact bounded admitted snapshot.
    ///
    /// Repository and commit identify one source together and therefore must be
    /// both present or both absent.
    pub fn new(
        source_repository: Option<SourceRepository>,
        source_commit: Option<SourceCommit>,
        reason: Option<ProvenanceReason>,
        approval_id: Option<ApprovalId>,
    ) -> Result<Self, StorageValueError> {
        if source_repository.is_some() != source_commit.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            source_repository,
            source_commit,
            reason,
            approval_id,
        })
    }

    /// Borrows the admitted source repository.
    #[must_use]
    pub const fn source_repository(&self) -> Option<&SourceRepository> {
        self.source_repository.as_ref()
    }

    /// Borrows the admitted source commit.
    #[must_use]
    pub const fn source_commit(&self) -> Option<&SourceCommit> {
        self.source_commit.as_ref()
    }

    /// Borrows the admitted provenance reason.
    #[must_use]
    pub const fn reason(&self) -> Option<&ProvenanceReason> {
        self.reason.as_ref()
    }

    /// Borrows the policy-validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        [
            self.source_repository
                .as_ref()
                .map(SourceRepository::as_bytes),
            self.source_commit.as_ref().map(SourceCommit::as_bytes),
            self.reason.as_ref().map(ProvenanceReason::as_bytes),
            self.approval_id.as_ref().map(ApprovalId::as_bytes),
        ]
        .into_iter()
        .flatten()
        .try_fold(4usize, |total, value| {
            total
                .checked_add(4)
                .and_then(|value_total| value_total.checked_add(value.len()))
                .ok_or(StorageValueError::SizeOverflow)
        })
    }
}

/// The exact durable pre-evaluation admission state for one mutating command.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredPendingAdmissionV1 {
    identity: IdempotencyIdentity,
    canonical_input_hash: CanonicalInputHash,
    admission_request_id: RequestId,
    plan: ExecutablePlanRef,
    logical_time: LogicalTime,
    actor: AdmittedActorContext,
    partition_key: PartitionKey,
    provenance_claims: StoredAdmittedProvenanceClaimsV1,
    causation: Option<StoredCommandCausationV1>,
}

impl StoredPendingAdmissionV1 {
    /// Freezes one checked admission without assigning an application sequence.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: IdempotencyIdentity,
        canonical_input_hash: CanonicalInputHash,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        logical_time: LogicalTime,
        actor: AdmittedActorContext,
        partition_key: PartitionKey,
        provenance_claims: StoredAdmittedProvenanceClaimsV1,
    ) -> Result<Self, StorageValueError> {
        Self::new_with_causation(
            identity,
            canonical_input_hash,
            admission_request_id,
            plan,
            logical_time,
            actor,
            partition_key,
            provenance_claims,
            None,
        )
    }

    /// Freezes one checked admission with optional server-validated causation.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_causation(
        identity: IdempotencyIdentity,
        canonical_input_hash: CanonicalInputHash,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        logical_time: LogicalTime,
        actor: AdmittedActorContext,
        partition_key: PartitionKey,
        provenance_claims: StoredAdmittedProvenanceClaimsV1,
        causation: Option<StoredCommandCausationV1>,
    ) -> Result<Self, StorageValueError> {
        if identity.contract_lineage() != plan.contract_lineage()
            || identity.command_id() != plan.command_id()
            || identity.principal_id() != actor.principal_id()
            || identity.tenant_scope() != actor.tenant_scope()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let value = Self {
            identity,
            canonical_input_hash,
            admission_request_id,
            plan,
            logical_time,
            actor,
            partition_key,
            provenance_claims,
            causation,
        };
        if value
            .semantic_bytes()?
            .checked_add(COMMIT_INTENT_FIXED_NON_RUNTIME_SEMANTIC_BYTES)
            .ok_or(StorageValueError::SizeOverflow)?
            > riffdb_types::COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(value)
    }

    /// Borrows the complete idempotency identity.
    #[must_use]
    pub const fn identity(&self) -> &IdempotencyIdentity {
        &self.identity
    }

    /// Returns the canonical command-input hash.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.canonical_input_hash
    }

    /// Returns the original request identity frozen at admission.
    #[must_use]
    pub const fn admission_request_id(&self) -> RequestId {
        self.admission_request_id
    }

    /// Borrows the exact historical executable-plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the deterministic logical time frozen at admission.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Borrows the stable actor context frozen at admission.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Borrows the exact validated logical partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Borrows the exact admitted provenance-claim snapshot.
    #[must_use]
    pub const fn provenance_claims(&self) -> &StoredAdmittedProvenanceClaimsV1 {
        &self.provenance_claims
    }

    /// Returns server-validated causation when this is a contextual reaction.
    #[must_use]
    pub const fn causation(&self) -> Option<StoredCommandCausationV1> {
        self.causation
    }

    /// Upgrades one decoded V1 base into its V2 causal successor.
    pub fn with_causation(
        mut self,
        causation: StoredCommandCausationV1,
    ) -> Result<Self, StorageValueError> {
        if self.causation.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        self.causation = Some(causation);
        if self
            .semantic_bytes()?
            .checked_add(COMMIT_INTENT_FIXED_NON_RUNTIME_SEMANTIC_BYTES)
            .ok_or(StorageValueError::SizeOverflow)?
            > riffdb_types::COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(self)
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let identity = self
            .identity
            .storage_key()
            .map_err(|_| StorageValueError::InvalidShape)?;
        let plan_bytes = self
            .plan
            .semantic_bytes()
            .ok_or(StorageValueError::SizeOverflow)?;
        let actor_bytes = actor_semantic_bytes(&self.actor)?;
        let partition_bytes = framed_bytes(self.partition_key.as_bytes().len())?;
        let mut total = framed_bytes(identity.as_bytes().len())?
            .checked_add(32 + 16)
            .and_then(|value| value.checked_add(plan_bytes))
            .and_then(|value| value.checked_add(12))
            .and_then(|value| value.checked_add(actor_bytes))
            .and_then(|value| value.checked_add(partition_bytes))
            .ok_or(StorageValueError::SizeOverflow)?;
        total = total
            .checked_add(self.provenance_claims.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?
            .checked_add(if self.causation.is_some() {
                STORED_COMMAND_CAUSATION_SEMANTIC_BYTES
            } else {
                0
            })
            .ok_or(StorageValueError::SizeOverflow)?;
        Ok(total)
    }
}

/// Exact coordinator-owned capacity reserved before deterministic evaluation.
///
/// Construction freezes every variable-size non-runtime field. The provenance
/// identifier is sourced later, but its fixed 16-byte width is charged here.
#[derive(Clone, Eq, PartialEq)]
pub struct PreEvaluationCommitContext {
    pending: StoredPendingAdmissionV1,
    partition_hash: PartitionKeyHash,
    conflict_hashes: Vec<ConflictKeyHash>,
    non_runtime_semantic_bytes: usize,
}

impl PreEvaluationCommitContext {
    /// Checks the complete exact 64 KiB non-runtime reserve before evaluation.
    pub fn new(
        pending: StoredPendingAdmissionV1,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> Result<Self, StorageValueError> {
        if hash_partition_key(pending.partition_key().as_bytes()) != partition_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        if conflict_hashes.len() > MAX_COMMIT_CONFLICT_HASHES {
            return Err(StorageValueError::LimitExceeded);
        }
        if conflict_hashes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        let non_runtime_semantic_bytes = pending
            .semantic_bytes()?
            .checked_add(COMMIT_INTENT_FIXED_NON_RUNTIME_SEMANTIC_BYTES)
            .and_then(|value| value.checked_add(conflict_hashes.len().checked_mul(32)?))
            .ok_or(StorageValueError::SizeOverflow)?;
        if non_runtime_semantic_bytes > riffdb_types::COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            pending,
            partition_hash,
            conflict_hashes,
            non_runtime_semantic_bytes,
        })
    }

    /// Borrows the exact durable pending admission.
    #[must_use]
    pub const fn pending(&self) -> &StoredPendingAdmissionV1 {
        &self.pending
    }

    /// Returns the checked partition hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Borrows conflict hashes in strict canonical order.
    #[must_use]
    pub fn conflict_hashes(&self) -> &[ConflictKeyHash] {
        &self.conflict_hashes
    }

    /// Returns the immutable v1 runtime budget.
    #[must_use]
    pub const fn evaluation_budget(&self) -> EvaluationBudget {
        EvaluationBudget::v1()
    }

    /// Returns the exact reserved non-runtime semantic charge.
    #[must_use]
    pub const fn non_runtime_semantic_bytes(&self) -> usize {
        self.non_runtime_semantic_bytes
    }
}

/// A durable, terminal, non-commit resolution of one pending admission.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredExecutionFailedV1 {
    pending: StoredPendingAdmissionV1,
    code: ExecutionFailureCode,
}

impl StoredExecutionFailedV1 {
    /// Retains the exact pending admission with the closed deterministic fault.
    #[must_use]
    pub const fn new(pending: StoredPendingAdmissionV1, code: ExecutionFailureCode) -> Self {
        Self { pending, code }
    }

    /// Borrows the exact admission consumed by this terminal state.
    #[must_use]
    pub const fn pending(&self) -> &StoredPendingAdmissionV1 {
        &self.pending
    }

    /// Returns the closed deterministic execution failure.
    #[must_use]
    pub const fn code(&self) -> ExecutionFailureCode {
        self.code
    }
}

/// Complete canonical post-image before its commit-time entity version exists.
#[derive(Clone, Eq, PartialEq)]
pub struct EntityPostImage {
    target: EntityTarget,
    written_by_contract: ContractVersion,
    fields: CanonicalRecord,
    fields_encoded_len: usize,
}

impl EntityPostImage {
    /// Constructs a complete bounded entity post-image.
    pub fn new(
        target: EntityTarget,
        written_by_contract: ContractVersion,
        fields: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        let fields_encoded_len = canonical_record_bytes(&fields)?;
        Ok(Self {
            target,
            written_by_contract,
            fields,
            fields_encoded_len,
        })
    }

    /// Borrows the complete entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the contract version writing this post-image.
    #[must_use]
    pub const fn written_by_contract(&self) -> ContractVersion {
        self.written_by_contract
    }

    /// Borrows every canonical entity field, including preserved unknowns.
    #[must_use]
    pub const fn fields(&self) -> &CanonicalRecord {
        &self.fields
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub const fn fields_encoded_len(&self) -> usize {
        self.fields_encoded_len
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.target
            .semantic_bytes()?
            .checked_add(8)
            .and_then(|value| value.checked_add(framed_bytes(self.fields_encoded_len).ok()?))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One create or replace mutation with an explicit expected observation.
#[derive(Clone, Eq, PartialEq)]
pub enum EntityMutation {
    /// Create only while the target remains absent.
    Create(EntityPostImage),
    /// Replace only while the target retains the expected version.
    Replace {
        /// Exact current entity version required for the replacement.
        expected_version: EntityVersion,
        /// Complete canonical post-image.
        post_image: EntityPostImage,
    },
}

impl EntityMutation {
    /// Borrows the complete mutation target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        match self {
            Self::Create(post_image) | Self::Replace { post_image, .. } => post_image.target(),
        }
    }

    /// Borrows the complete mutation post-image.
    #[must_use]
    pub const fn post_image(&self) -> &EntityPostImage {
        match self {
            Self::Create(post_image) | Self::Replace { post_image, .. } => post_image,
        }
    }

    /// Returns the expected pre-mutation observation.
    #[must_use]
    pub const fn expected_version(&self) -> Option<EntityVersion> {
        match self {
            Self::Create(_) => None,
            Self::Replace {
                expected_version, ..
            } => Some(*expected_version),
        }
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let variant_bytes = match self {
            Self::Create(_) => 1,
            Self::Replace { .. } => 1 + 8,
        };
        self.post_image()
            .semantic_bytes()?
            .checked_add(variant_bytes)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One deterministic pre-commit event value in instruction-occurrence order.
#[derive(Clone, Eq, PartialEq)]
pub struct EventIntent {
    event_type_id: EventTypeId,
    payload: CanonicalRecord,
    payload_encoded_len: usize,
}

impl EventIntent {
    /// Constructs a bounded event intent before its stable event ID exists.
    pub fn new(
        event_type_id: EventTypeId,
        payload: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        let payload_encoded_len = canonical_record_bytes(&payload)?;
        Ok(Self {
            event_type_id,
            payload,
            payload_encoded_len,
        })
    }

    /// Returns the stable event type.
    #[must_use]
    pub const fn event_type_id(&self) -> EventTypeId {
        self.event_type_id
    }

    /// Borrows the complete canonical event payload.
    #[must_use]
    pub const fn payload(&self) -> &CanonicalRecord {
        &self.payload
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub const fn payload_encoded_len(&self) -> usize {
        self.payload_encoded_len
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        framed_bytes(self.payload_encoded_len)?
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One declared typed business outcome before persistence.
#[derive(Clone, Eq, PartialEq)]
pub struct DeclaredOutcome {
    outcome_id: OutcomeId,
    value: CanonicalRecord,
    value_encoded: Arc<[u8]>,
}

impl DeclaredOutcome {
    /// Constructs a bounded declared business outcome.
    pub fn new(outcome_id: OutcomeId, value: CanonicalRecord) -> Result<Self, StorageValueError> {
        let value_encoded = encode_canonical_record(&value)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        Ok(Self {
            outcome_id,
            value,
            value_encoded: Arc::from(value_encoded),
        })
    }

    /// Returns the stable declared outcome identity.
    #[must_use]
    pub const fn outcome_id(&self) -> OutcomeId {
        self.outcome_id
    }

    /// Borrows the complete canonical outcome value.
    #[must_use]
    pub const fn value(&self) -> &CanonicalRecord {
        &self.value
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub fn value_encoded_len(&self) -> usize {
        self.value_encoded.len()
    }

    /// Borrows the canonical bytes sealed with the declared value.
    #[must_use]
    pub fn value_encoded(&self) -> &[u8] {
        &self.value_encoded
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        framed_bytes(self.value_encoded.len())?
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// The exact deterministic runtime output for one mutating command.
#[derive(Clone, Eq, PartialEq)]
pub struct EvaluatedCommand {
    plan: ExecutablePlanRef,
    validation_request: ValidationReadRequest,
    read_dependencies: ReadDependencies,
    mutations: Vec<EntityMutation>,
    event_intents: Vec<EventIntent>,
    outcome: DeclaredOutcome,
    semantic_bytes: usize,
}

/// Incremental runtime-facing construction of one bounded evaluated command.
///
/// Each mutation, event, and outcome is charged against the exact aggregate
/// budget before the builder retains it. This prevents a runtime from first
/// assembling an over-limit aggregate and discovering the failure only during
/// final construction.
pub struct EvaluatedCommandBuilder<'snapshot> {
    snapshot: &'snapshot ReadSnapshot,
    budget: EvaluationBudget,
    mutations: Vec<EntityMutation>,
    event_intents: Vec<EventIntent>,
    outcome: Option<DeclaredOutcome>,
    prior_mutation_key: Option<Vec<u8>>,
    semantic_bytes: usize,
}

impl<'snapshot> EvaluatedCommandBuilder<'snapshot> {
    /// Starts a builder from the snapshot's exact plan, targets, and dependencies.
    pub fn new(
        snapshot: &'snapshot ReadSnapshot,
        budget: EvaluationBudget,
    ) -> Result<Self, StorageValueError> {
        let validation_request = snapshot.validation_request();
        validate_evaluated_snapshot_budget(snapshot, &validation_request, budget)?;
        let semantic_bytes = evaluated_fixed_semantic_bytes(
            snapshot.plan(),
            &validation_request,
            snapshot.read_dependencies(),
        )?;
        if semantic_bytes > budget.maximum_semantic_bytes {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            snapshot,
            budget,
            mutations: Vec::new(),
            event_intents: Vec::new(),
            outcome: None,
            prior_mutation_key: None,
            semantic_bytes,
        })
    }

    /// Charges and retains the next canonically ordered mutation.
    pub fn push_mutation(&mut self, mutation: EntityMutation) -> Result<(), StorageValueError> {
        if self.mutations.len() >= self.budget.maximum_mutations {
            return Err(StorageValueError::LimitExceeded);
        }
        let key = validate_evaluated_mutation(
            self.snapshot,
            &mutation,
            self.prior_mutation_key.as_deref(),
        )?;
        let next = self
            .semantic_bytes
            .checked_add(mutation.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > self.budget.maximum_semantic_bytes {
            return Err(StorageValueError::LimitExceeded);
        }
        self.mutations.push(mutation);
        self.prior_mutation_key = Some(key);
        self.semantic_bytes = next;
        Ok(())
    }

    /// Charges and retains the next event in instruction-occurrence order.
    pub fn push_event(&mut self, event: EventIntent) -> Result<(), StorageValueError> {
        if self.event_intents.len() >= self.budget.maximum_events {
            return Err(StorageValueError::LimitExceeded);
        }
        let next = self
            .semantic_bytes
            .checked_add(event.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > self.budget.maximum_semantic_bytes {
            return Err(StorageValueError::LimitExceeded);
        }
        self.event_intents.push(event);
        self.semantic_bytes = next;
        Ok(())
    }

    /// Charges and retains the command's one terminal declared outcome.
    pub fn set_outcome(&mut self, outcome: DeclaredOutcome) -> Result<(), StorageValueError> {
        if self.outcome.is_some() {
            return Err(StorageValueError::Duplicate);
        }
        let next = self
            .semantic_bytes
            .checked_add(outcome.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > self.budget.maximum_semantic_bytes {
            return Err(StorageValueError::LimitExceeded);
        }
        self.outcome = Some(outcome);
        self.semantic_bytes = next;
        Ok(())
    }

    /// Finishes through the existing checked `EvaluatedCommand` constructor.
    pub fn finish(self) -> Result<EvaluatedCommand, StorageValueError> {
        let outcome = self.outcome.ok_or(StorageValueError::InvalidShape)?;
        let evaluated = EvaluatedCommand::new(
            self.snapshot,
            self.mutations,
            self.event_intents,
            outcome,
            self.budget,
        )?;
        debug_assert_eq!(evaluated.semantic_bytes(), self.semantic_bytes);
        Ok(evaluated)
    }
}

impl EvaluatedCommand {
    /// Copies the snapshot's exact targets and dependencies and validates all
    /// runtime-owned output bounds. Event order is deliberately preserved.
    pub fn new(
        snapshot: &ReadSnapshot,
        mutations: Vec<EntityMutation>,
        event_intents: Vec<EventIntent>,
        outcome: DeclaredOutcome,
        budget: EvaluationBudget,
    ) -> Result<Self, StorageValueError> {
        let validation_request = snapshot.validation_request();
        validate_evaluated_snapshot_budget(snapshot, &validation_request, budget)?;
        if mutations.len() > budget.maximum_mutations || event_intents.len() > budget.maximum_events
        {
            return Err(StorageValueError::LimitExceeded);
        }

        let mut prior: Option<Vec<u8>> = None;
        for mutation in &mutations {
            let key = validate_evaluated_mutation(snapshot, mutation, prior.as_deref())?;
            prior = Some(key);
        }

        let semantic_bytes = evaluated_semantic_bytes(
            snapshot.plan(),
            &validation_request,
            snapshot.read_dependencies(),
            &mutations,
            &event_intents,
            &outcome,
        )?;
        if semantic_bytes > budget.maximum_semantic_bytes {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            plan: snapshot.plan().clone(),
            validation_request,
            read_dependencies: snapshot.read_dependencies().clone(),
            mutations,
            event_intents,
            outcome,
            semantic_bytes,
        })
    }

    /// Borrows the exact historical executable-plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Borrows the structurally checked transaction-current read request.
    #[must_use]
    pub const fn validation_request(&self) -> &ValidationReadRequest {
        &self.validation_request
    }

    /// Borrows the complete canonical dependency evidence.
    #[must_use]
    pub const fn read_dependencies(&self) -> &ReadDependencies {
        &self.read_dependencies
    }

    /// Borrows mutations in canonical target-byte order.
    #[must_use]
    pub fn mutations(&self) -> &[EntityMutation] {
        &self.mutations
    }

    /// Borrows event intents in deterministic occurrence order.
    #[must_use]
    pub fn event_intents(&self) -> &[EventIntent] {
        &self.event_intents
    }

    /// Borrows the declared business outcome.
    #[must_use]
    pub const fn outcome(&self) -> &DeclaredOutcome {
        &self.outcome
    }

    /// Returns checked runtime-owned semantic byte accounting.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

/// Transaction-current idempotency state required before a command may stage.
///
/// New synchronous commands retain every identity inspected as vacant and let
/// the authoritative writer prove that vacancy again in the terminal
/// transaction. Recovery of pre-WP-372 work retains the exact durable
/// `Pending` admission instead.
#[derive(Clone, Eq, PartialEq)]
pub enum CommandAdmissionExpectationV1 {
    /// A legacy durable admission must still equal the retained record.
    ExistingPending,
    /// Every inspected compatible identity must still be vacant.
    Vacant(IdempotencyLookupCandidatesV1),
}

impl fmt::Debug for CommandAdmissionExpectationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ExistingPending => "CommandAdmissionExpectationV1::ExistingPending",
            Self::Vacant(_) => "CommandAdmissionExpectationV1::Vacant([REDACTED])",
        })
    }
}

/// A complete pre-sequence command candidate assembled only after evaluation.
#[derive(Clone, Eq, PartialEq)]
pub struct CommitIntent {
    pending: StoredPendingAdmissionV1,
    admission_expectation: CommandAdmissionExpectationV1,
    evaluated: Arc<EvaluatedCommand>,
    provenance_id: ProvenanceId,
    partition_hash: PartitionKeyHash,
    conflict_hashes: Vec<ConflictKeyHash>,
    semantic_bytes: usize,
}

impl CommitIntent {
    /// Consumes the pre-evaluation reserve and combines it with runtime output.
    pub fn new(
        context: PreEvaluationCommitContext,
        evaluated: impl Into<Arc<EvaluatedCommand>>,
        provenance_id: ProvenanceId,
    ) -> Result<Self, StorageValueError> {
        let evaluated = evaluated.into();
        if context.pending.plan() != evaluated.plan() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let semantic_bytes = context
            .non_runtime_semantic_bytes
            .checked_add(evaluated.semantic_bytes())
            .ok_or(StorageValueError::SizeOverflow)?;
        if semantic_bytes > MAX_COMMIT_INTENT_SEMANTIC_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            pending: context.pending,
            admission_expectation: CommandAdmissionExpectationV1::ExistingPending,
            evaluated,
            provenance_id,
            partition_hash: context.partition_hash,
            conflict_hashes: context.conflict_hashes,
            semantic_bytes,
        })
    }

    /// Combines a speculative synchronous evaluation with the complete set of
    /// identities that the writer must prove vacant before staging anything.
    pub fn new_for_vacant_terminal_admission(
        context: PreEvaluationCommitContext,
        lookup_candidates: IdempotencyLookupCandidatesV1,
        evaluated: impl Into<Arc<EvaluatedCommand>>,
        provenance_id: ProvenanceId,
    ) -> Result<Self, StorageValueError> {
        if lookup_candidates.as_slice().first() != Some(context.pending.identity()) {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut intent = Self::new(context, evaluated, provenance_id)?;
        intent.admission_expectation = CommandAdmissionExpectationV1::Vacant(lookup_candidates);
        Ok(intent)
    }

    /// Borrows the exact stored pending admission.
    #[must_use]
    pub const fn pending(&self) -> &StoredPendingAdmissionV1 {
        &self.pending
    }

    /// Borrows the exact transaction-current idempotency expectation.
    #[must_use]
    pub const fn admission_expectation(&self) -> &CommandAdmissionExpectationV1 {
        &self.admission_expectation
    }

    /// Borrows the unchanged deterministic runtime result.
    #[must_use]
    pub fn evaluated(&self) -> &EvaluatedCommand {
        &self.evaluated
    }

    /// Returns the newly sourced checked provenance identity.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Returns the canonical partition-key hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Borrows conflict-key hashes in canonical byte order.
    #[must_use]
    pub fn conflict_hashes(&self) -> &[ConflictKeyHash] {
        &self.conflict_hashes
    }

    /// Returns checked complete pre-commit semantic byte accounting.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

/// Canonical affected epoch buckets derived from validated mutation bookkeeping.
///
/// These targets are distinct from write-influencing read dependencies. They are
/// read only so the coordinator can advance every affected durable epoch exactly
/// once after private mutation validation.
#[derive(Clone, Eq, PartialEq)]
pub struct AffectedIndexEpochTargets {
    targets: Vec<PartitionIndexTarget>,
    unique_targets: Vec<UniqueIndexTarget>,
    semantic_bytes: usize,
}

/// One exact unique-prefix occupancy check and its permitted owner entry.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct UniqueIndexTarget {
    prefix: IndexRangeTarget,
    expected_entry: IndexEntryKey,
}

impl UniqueIndexTarget {
    /// Constructs a checked target whose expected entry lies under the exact prefix.
    pub fn new(
        prefix: IndexRangeTarget,
        expected_entry: IndexEntryKey,
    ) -> Result<Self, StorageValueError> {
        if prefix.prefix().index_id() != expected_entry.index_id()
            || !expected_entry
                .as_bytes()
                .starts_with(prefix.prefix().as_bytes())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            prefix,
            expected_entry,
        })
    }
    /// Exact complete-component unique prefix.
    #[must_use]
    pub const fn prefix(&self) -> &IndexRangeTarget {
        &self.prefix
    }
    /// Sole entry allowed to occupy this prefix.
    #[must_use]
    pub const fn expected_entry(&self) -> &IndexEntryKey {
        &self.expected_entry
    }
    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.prefix
            .semantic_bytes()?
            .checked_add(self.expected_entry.as_bytes().len())
            .and_then(|bytes| bytes.checked_add(4))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl AffectedIndexEpochTargets {
    /// Canonicalizes and bounds the complete affected bucket set.
    pub fn new(targets: Vec<PartitionIndexTarget>) -> Result<Self, StorageValueError> {
        Self::with_unique(targets, Vec::new())
    }

    /// Canonicalizes affected epoch buckets and exact unique occupancy targets.
    pub fn with_unique(
        mut targets: Vec<PartitionIndexTarget>,
        mut unique_targets: Vec<UniqueIndexTarget>,
    ) -> Result<Self, StorageValueError> {
        if targets.len() > MAX_AFFECTED_INDEX_EPOCH_TARGETS
            || unique_targets.len() > MAX_AFFECTED_INDEX_EPOCH_TARGETS
            || targets
                .len()
                .checked_add(unique_targets.len())
                .is_none_or(|count| count > MAX_VALIDATION_TARGETS)
        {
            return Err(StorageValueError::LimitExceeded);
        }
        targets.sort_unstable();
        if targets.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        unique_targets.sort_unstable();
        if unique_targets.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        let semantic_bytes = targets.iter().try_fold(8usize, |total, target| {
            total
                .checked_add(target.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
        let semantic_bytes = unique_targets
            .iter()
            .try_fold(semantic_bytes, |total, target| {
                total
                    .checked_add(target.semantic_bytes()?)
                    .ok_or(StorageValueError::SizeOverflow)
            })?;
        if semantic_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            targets,
            unique_targets,
            semantic_bytes,
        })
    }

    /// Borrows targets in exact canonical prefix-byte order.
    #[must_use]
    pub fn as_slice(&self) -> &[PartitionIndexTarget] {
        &self.targets
    }
    /// Borrows exact unique targets in canonical prefix/owner order.
    #[must_use]
    pub fn unique_targets(&self) -> &[UniqueIndexTarget] {
        &self.unique_targets
    }

    /// Returns the bounded aggregate transient target charge.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

pub(crate) fn canonical_record_bytes(record: &CanonicalRecord) -> Result<usize, StorageValueError> {
    encode_canonical_record(record)
        .map(|bytes| bytes.len())
        .map_err(|error| canonical_codec_storage_error(&error))
}

fn evaluated_semantic_bytes(
    plan: &ExecutablePlanRef,
    request: &ValidationReadRequest,
    dependencies: &ReadDependencies,
    mutations: &[EntityMutation],
    events: &[EventIntent],
    outcome: &DeclaredOutcome,
) -> Result<usize, StorageValueError> {
    let mut total = evaluated_fixed_semantic_bytes(plan, request, dependencies)?;
    for mutation in mutations {
        total = total
            .checked_add(mutation.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    for event in events {
        total = total
            .checked_add(event.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    total
        .checked_add(outcome.semantic_bytes()?)
        .ok_or(StorageValueError::SizeOverflow)
}

fn evaluated_fixed_semantic_bytes(
    plan: &ExecutablePlanRef,
    request: &ValidationReadRequest,
    dependencies: &ReadDependencies,
) -> Result<usize, StorageValueError> {
    plan.semantic_bytes()
        .and_then(|value| value.checked_add(request.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(dependencies.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(4 + 4))
        .ok_or(StorageValueError::SizeOverflow)
}

fn validate_evaluated_snapshot_budget(
    snapshot: &ReadSnapshot,
    validation_request: &ValidationReadRequest,
    budget: EvaluationBudget,
) -> Result<(), StorageValueError> {
    let target_count = validation_request
        .binding_targets()
        .len()
        .checked_add(validation_request.root_validation_targets().len())
        .and_then(|value| value.checked_add(validation_request.range_targets().len()))
        .ok_or(StorageValueError::SizeOverflow)?;
    if target_count > budget.maximum_read_targets
        || snapshot.read_dependencies().as_slice().len() > budget.maximum_read_dependencies
    {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(())
}

fn validate_evaluated_mutation(
    snapshot: &ReadSnapshot,
    mutation: &EntityMutation,
    prior_key: Option<&[u8]>,
) -> Result<Vec<u8>, StorageValueError> {
    if mutation.post_image().written_by_contract() != snapshot.plan().contract_version() {
        return Err(StorageValueError::IdentityMismatch);
    }
    let required = match mutation {
        EntityMutation::Create(_) => crate::ExpectedEntityState::Absent,
        EntityMutation::Replace {
            expected_version, ..
        } => crate::ExpectedEntityState::Present(*expected_version),
    };
    if snapshot
        .read_dependencies()
        .expected_entity_state(mutation.target())
        != Some(required)
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    let key = mutation.target().canonical_target_key();
    if prior_key.is_some_and(|prior_key| prior_key >= key.as_slice()) {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(key)
}

pub(crate) fn framed_bytes(length: usize) -> Result<usize, StorageValueError> {
    length.checked_add(4).ok_or(StorageValueError::SizeOverflow)
}

pub(crate) fn actor_semantic_bytes(
    actor: &AdmittedActorContext,
) -> Result<usize, StorageValueError> {
    framed_bytes(actor.principal_id().as_str().len())?
        .checked_add(1)
        .and_then(|value| {
            value.checked_add(framed_bytes(actor.tenant_scope().to_canonical_bytes().len()).ok()?)
        })
        .and_then(|value| {
            value.checked_add(if actor.agent_session_id().is_some() {
                1 + 16
            } else {
                1
            })
        })
        .ok_or(StorageValueError::SizeOverflow)
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {
        $(
            impl fmt::Debug for $type {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
                }
            }
        )+
    };
}

redacted_debug!(
    StoredAdmittedProvenanceClaimsV1,
    StoredPendingAdmissionV1,
    StoredExecutionFailedV1,
    PreEvaluationCommitContext,
    EntityPostImage,
    EntityMutation,
    EventIntent,
    DeclaredOutcome,
    EvaluatedCommand,
    CommitIntent,
    AffectedIndexEpochTargets,
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityObservation, IndexRangePrefixBuilder, SnapshotRequest};
    use riffdb_types::{
        AggregateTypeId, CanonicalValue, CommandId, ContractBundleHash, ContractLineage,
        EntityKeyBuilder, EntityTypeId, FieldId, IndexEntryKeyBuilder, IndexId,
        PartitionKeyBuilder, PlanHash,
    };

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("budget").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x11; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn target(value: u64) -> EntityTarget {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(value).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("matching target")
    }

    fn generation_target(index: IndexId) -> PartitionIndexTarget {
        PartitionIndexTarget::new(
            PartitionKeyBuilder::new(AggregateTypeId::first())
                .finish()
                .expect("partition key"),
            index,
        )
    }

    fn absent_snapshot(plan: &ExecutablePlanRef, targets: &[EntityTarget]) -> ReadSnapshot {
        let request = SnapshotRequest::new(plan.clone(), targets.to_vec(), Vec::new(), Vec::new())
            .expect("snapshot request");
        ReadSnapshot::new(
            &request,
            None,
            targets
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            Vec::new(),
            Vec::new(),
        )
        .expect("snapshot")
    }

    fn payload_record(length: usize) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field"),
            CanonicalValue::bytes(vec![0xa5; length]).expect("bounded bytes"),
        )])
        .expect("record")
    }

    fn outcome(length: usize) -> DeclaredOutcome {
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), payload_record(length))
            .expect("declared outcome")
    }

    fn budget_with_semantic_limit(maximum_semantic_bytes: usize) -> EvaluationBudget {
        EvaluationBudget {
            maximum_semantic_bytes,
            ..EvaluationBudget::v1()
        }
    }

    #[test]
    fn affected_index_epoch_targets_use_the_distinct_shared_prefix_limit() {
        let targets = |count: usize| {
            (1..=count)
                .map(|value| {
                    let index = IndexId::new(u32::try_from(value).expect("test index fits u32"))
                        .expect("nonzero index");
                    generation_target(index)
                })
                .collect::<Vec<_>>()
        };

        let exact = AffectedIndexEpochTargets::new(targets(MAX_AFFECTED_INDEX_EPOCH_TARGETS))
            .expect("exact affected-prefix limit");
        assert_eq!(
            exact.as_slice().len(),
            riffdb_types::MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1
        );
        assert_eq!(
            AffectedIndexEpochTargets::new(targets(MAX_AFFECTED_INDEX_EPOCH_TARGETS + 1)),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn unique_targets_accept_the_exact_shared_limit_and_reject_the_next_position() {
        let unique_targets = |count: usize| {
            (1..=count)
                .map(|value| {
                    let index = IndexId::new(u32::try_from(value).expect("test index fits u32"))
                        .expect("nonzero index");
                    let mut prefix = IndexRangePrefixBuilder::new(index);
                    prefix.push_u64(7).expect("unique component");
                    let mut entry = IndexEntryKeyBuilder::new(index);
                    entry.push_u64(7).expect("unique component");
                    UniqueIndexTarget::new(
                        IndexRangeTarget::new(
                            generation_target(index).partition_key().clone(),
                            prefix.finish(),
                        ),
                        entry
                            .finish(
                                target(u64::try_from(value).expect("test entity"))
                                    .key()
                                    .clone(),
                            )
                            .expect("unique entry"),
                    )
                    .expect("unique target")
                })
                .collect::<Vec<_>>()
        };

        let exact = AffectedIndexEpochTargets::with_unique(
            Vec::new(),
            unique_targets(MAX_VALIDATION_TARGETS),
        )
        .expect("exact unique validation-position limit");
        assert_eq!(exact.unique_targets().len(), MAX_VALIDATION_TARGETS);
        assert_eq!(
            AffectedIndexEpochTargets::with_unique(
                Vec::new(),
                unique_targets(MAX_VALIDATION_TARGETS + 1)
            ),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(
            AffectedIndexEpochTargets::with_unique(
                vec![generation_target(IndexId::first())],
                unique_targets(MAX_VALIDATION_TARGETS)
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn evaluated_builder_accepts_exact_byte_limit_and_rejects_next_unit_before_retaining() {
        let plan = plan();
        let snapshot = absent_snapshot(&plan, &[]);
        let validation_request = snapshot.validation_request();
        let baseline = evaluated_fixed_semantic_bytes(
            snapshot.plan(),
            &validation_request,
            snapshot.read_dependencies(),
        )
        .expect("baseline charge");
        let exact_outcome = outcome(0);
        let next_outcome = outcome(1);
        assert_eq!(
            next_outcome.semantic_bytes(),
            exact_outcome
                .semantic_bytes()
                .and_then(|value| value.checked_add(1).ok_or(StorageValueError::SizeOverflow))
        );
        let limit = baseline + exact_outcome.semantic_bytes().expect("outcome charge");
        let budget = budget_with_semantic_limit(limit);

        let mut exact = EvaluatedCommandBuilder::new(&snapshot, budget).expect("builder");
        exact.set_outcome(exact_outcome).expect("exact outcome");
        let evaluated = exact.finish().expect("exact evaluated command");
        assert_eq!(evaluated.semantic_bytes(), limit);

        let mut over = EvaluatedCommandBuilder::new(&snapshot, budget).expect("builder");
        let retained_charge = over.semantic_bytes;
        assert_eq!(
            over.set_outcome(next_outcome),
            Err(StorageValueError::LimitExceeded)
        );
        assert!(over.outcome.is_none());
        assert_eq!(over.semantic_bytes, retained_charge);
    }

    #[test]
    fn evaluated_builder_rejects_noncanonical_mutation_before_retaining() {
        let plan = plan();
        let high_target = target(2);
        let low_target = target(1);
        let snapshot = absent_snapshot(&plan, &[high_target.clone(), low_target.clone()]);
        let empty = CanonicalRecord::new(Vec::new()).expect("empty record");
        let high = EntityMutation::Create(
            EntityPostImage::new(high_target, plan.contract_version(), empty.clone())
                .expect("post image"),
        );
        let low = EntityMutation::Create(
            EntityPostImage::new(low_target, plan.contract_version(), empty.clone())
                .expect("post image"),
        );
        let mut builder =
            EvaluatedCommandBuilder::new(&snapshot, EvaluationBudget::v1()).expect("builder");
        builder.push_mutation(high).expect("first mutation");
        assert_eq!(
            builder.push_mutation(low),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(builder.mutations.len(), 1);
        builder
            .set_outcome(
                DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), empty).expect("outcome"),
            )
            .expect("terminal outcome");
        let evaluated = builder.finish().expect("evaluated command");
        assert_eq!(evaluated.mutations().len(), 1);
    }

    #[test]
    fn evaluated_builder_enforces_mutation_byte_limit_before_retaining() {
        let plan = plan();
        let mutation_target = target(1);
        let snapshot = absent_snapshot(&plan, std::slice::from_ref(&mutation_target));
        let validation_request = snapshot.validation_request();
        let baseline = evaluated_fixed_semantic_bytes(
            snapshot.plan(),
            &validation_request,
            snapshot.read_dependencies(),
        )
        .expect("baseline charge");
        let exact = EntityMutation::Create(
            EntityPostImage::new(
                mutation_target.clone(),
                plan.contract_version(),
                payload_record(0),
            )
            .expect("exact post image"),
        );
        let over = EntityMutation::Create(
            EntityPostImage::new(mutation_target, plan.contract_version(), payload_record(1))
                .expect("over post image"),
        );
        assert_eq!(
            over.semantic_bytes(),
            exact
                .semantic_bytes()
                .and_then(|value| value.checked_add(1).ok_or(StorageValueError::SizeOverflow))
        );
        let limit = baseline + exact.semantic_bytes().expect("exact mutation charge");
        let budget = budget_with_semantic_limit(limit);

        let mut exact_builder =
            EvaluatedCommandBuilder::new(&snapshot, budget).expect("exact builder");
        exact_builder
            .push_mutation(exact)
            .expect("exact mutation limit");
        assert_eq!(exact_builder.semantic_bytes, limit);

        let mut over_builder =
            EvaluatedCommandBuilder::new(&snapshot, budget).expect("over builder");
        let retained_charge = over_builder.semantic_bytes;
        assert_eq!(
            over_builder.push_mutation(over),
            Err(StorageValueError::LimitExceeded)
        );
        assert!(over_builder.mutations.is_empty());
        assert!(over_builder.prior_mutation_key.is_none());
        assert_eq!(over_builder.semantic_bytes, retained_charge);
    }

    #[test]
    fn evaluated_builder_enforces_mutation_count_before_retaining() {
        let plan = plan();
        let first_target = target(1);
        let second_target = target(2);
        let snapshot = absent_snapshot(&plan, &[first_target.clone(), second_target.clone()]);
        let empty = CanonicalRecord::new(Vec::new()).expect("empty record");
        let first = EntityMutation::Create(
            EntityPostImage::new(first_target, plan.contract_version(), empty.clone())
                .expect("first post image"),
        );
        let second = EntityMutation::Create(
            EntityPostImage::new(second_target, plan.contract_version(), empty)
                .expect("second post image"),
        );
        let budget = EvaluationBudget {
            maximum_mutations: 1,
            ..EvaluationBudget::v1()
        };
        let mut builder = EvaluatedCommandBuilder::new(&snapshot, budget).expect("builder");
        builder.push_mutation(first).expect("first mutation");
        let retained_charge = builder.semantic_bytes;
        assert_eq!(
            builder.push_mutation(second),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(builder.mutations.len(), 1);
        assert_eq!(builder.semantic_bytes, retained_charge);
    }

    #[test]
    fn evaluated_builder_enforces_event_byte_limit_before_retaining() {
        let plan = plan();
        let snapshot = absent_snapshot(&plan, &[]);
        let validation_request = snapshot.validation_request();
        let baseline = evaluated_fixed_semantic_bytes(
            snapshot.plan(),
            &validation_request,
            snapshot.read_dependencies(),
        )
        .expect("baseline charge");
        let event_type = EventTypeId::new(1).expect("event type");
        let exact = EventIntent::new(event_type, payload_record(0)).expect("exact event");
        let over = EventIntent::new(event_type, payload_record(1)).expect("over event");
        assert_eq!(
            over.semantic_bytes(),
            exact
                .semantic_bytes()
                .and_then(|value| value.checked_add(1).ok_or(StorageValueError::SizeOverflow))
        );
        let limit = baseline + exact.semantic_bytes().expect("exact event charge");
        let budget = budget_with_semantic_limit(limit);

        let mut exact_builder =
            EvaluatedCommandBuilder::new(&snapshot, budget).expect("exact builder");
        exact_builder.push_event(exact).expect("exact event limit");
        assert_eq!(exact_builder.semantic_bytes, limit);

        let mut over_builder =
            EvaluatedCommandBuilder::new(&snapshot, budget).expect("over builder");
        let retained_charge = over_builder.semantic_bytes;
        assert_eq!(
            over_builder.push_event(over),
            Err(StorageValueError::LimitExceeded)
        );
        assert!(over_builder.event_intents.is_empty());
        assert_eq!(over_builder.semantic_bytes, retained_charge);
    }

    #[test]
    fn evaluated_builder_enforces_event_count_before_retaining() {
        let plan = plan();
        let snapshot = absent_snapshot(&plan, &[]);
        let event = EventIntent::new(
            EventTypeId::new(1).expect("event type"),
            CanonicalRecord::new(Vec::new()).expect("empty record"),
        )
        .expect("event");
        let budget = EvaluationBudget {
            maximum_events: 1,
            ..EvaluationBudget::v1()
        };
        let mut builder = EvaluatedCommandBuilder::new(&snapshot, budget).expect("builder");
        builder.push_event(event.clone()).expect("first event");
        let retained_charge = builder.semantic_bytes;
        assert_eq!(
            builder.push_event(event),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(builder.event_intents.len(), 1);
        assert_eq!(builder.semantic_bytes, retained_charge);
    }

    #[test]
    fn evaluated_builder_cannot_finish_without_an_outcome() {
        let plan = plan();
        let snapshot = absent_snapshot(&plan, &[]);
        let builder =
            EvaluatedCommandBuilder::new(&snapshot, EvaluationBudget::v1()).expect("builder");
        assert_eq!(builder.finish(), Err(StorageValueError::InvalidShape));
    }
}
