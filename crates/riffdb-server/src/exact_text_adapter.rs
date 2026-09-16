#![expect(
    clippy::expect_used,
    reason = "validated exact-text batches retain the indexed entity selected for provider work"
)]

//! Background-owned exact text result-set provider for generated RiffQL.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_policy::{AuthorizedQueryRowPolicyContextV1, MAX_PROJECTED_POLICY_CANDIDATES_V1};
use riffdb_projection::{
    ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5,
    ExactPredicateProviderBindingV1, ExactPredicateProviderBindingV2, ExactPredicateProviderRowV1,
    ExactTextPartitionIndexV2, ExactTextPartitionIndexV3, LongPatternPartitionV1,
    LongPatternProviderErrorV1, MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4,
    MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1, ProviderEpochObservationV1, ProviderLifecycleV1,
    ResultSetEpochContextV1, ResultSetEpochRequirementV1, TokenizedTextConfigV1,
    TokenizedTextFieldV1, TokenizedTextMutationV1, TokenizedTextPartitionIndexV1,
    negotiate_result_set_epoch_v1,
};
use riffdb_query_executor::{
    ExactTextResultSetV1, LongPatternCandidateBatch, QueryExecutionError, QueryExecutionPort,
    QueryRow, execute_exact_predicate_result_set_v1, execute_exact_text_filtered_result_set_v1,
    execute_exact_text_result_set_v1, execute_nullable_exact_predicate_result_set_v1,
    execute_tokenized_text_v1,
};
use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactPredicateNodeV1, ExactReferenceCellV1, ExactScalarV1,
    QueryAccessKind, QueryAccessStep,
};
use riffdb_service::{
    ExactPredicateProjectionPort, ExactPredicateProjectionRequest, ExactPredicateProjectionResult,
    ExactTextProjectionPort, ExactTextProjectionPortError, ExactTextProjectionRequest,
    ExactTextProjectionResult, ExactTextProjectionRow, LongPatternProjectionPort,
    LongPatternProjectionRequest, NullableExactPredicateProjectionRequest,
    TokenizedTextProjectionPort, TokenizedTextProjectionRequest,
};
use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanRequest, AuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest, AuthoritativePointReader, AuthoritativeScanReader,
    CatalogRepository, EntityTarget, IndexRangePrefixBuilder, IndexRangeTarget,
    SnapshotFenceReader, StorageScanLimit,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CapabilityId, CommitSequence,
    EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5, EntityKey, ExactTextProfileV1, FieldId,
    FrontierPosition, HashDomain, PartitionKey, PartitionKeyHash, ProjectionGeneration,
    ProjectionProviderPolicyModeV1, decode_canonical_value, encode_canonical_value, hash,
    hash_entity_key, hash_partition_key,
};

use crate::columnar_adapter::read_application_head;
use crate::projection_read_source::ProjectionReadSource;

#[path = "exact_text_catch_up.rs"]
mod catch_up;
#[cfg(feature = "test-fixtures")]
use crate::exact_text_probe::{ExactProviderTestPoint as TestPoint, observe as observe_test_point};
use catch_up::{CapturedSource, Selected};

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_REGISTERED_EXACT_PARTITIONS: usize = 256;
const REBUILD_PAGE_ROWS: u16 = 500;
const CHECKPOINT_MAGIC: &[u8; 4] = b"RXAC";
const CHECKPOINT_FORMAT_V1: u16 = 1;
const SLOT_KEY_BYTES: usize = 32 + 32 + 32 + 1 + 16 + 8;
const CHECKPOINT_SLOT_OFFSET: usize = 16;
const CHECKPOINT_LENGTH_OFFSET: usize = CHECKPOINT_SLOT_OFFSET + SLOT_KEY_BYTES;
const CHECKPOINT_HEADER_BYTES: usize = CHECKPOINT_LENGTH_OFFSET + 4;
const CHECKPOINT_DIGEST_BYTES: usize = 32;
const MAX_ACTIVATION_CHECKPOINT_BYTES: usize =
    CHECKPOINT_HEADER_BYTES + MAX_PROVIDER_CHECKPOINT_BYTES + CHECKPOINT_DIGEST_BYTES;
const MAX_PROVIDER_CHECKPOINT_BYTES: usize =
    if MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 > MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1 {
        MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4
    } else {
        MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1
    };

type SlotKey = Vec<u8>;
type ExactTextSourceRow = (String, Option<CanonicalValue>, CanonicalRecord);
type ExactTextSourceRows = BTreeMap<riffdb_types::EntityKey, ExactTextSourceRow>;
type ExactPredicateSourceRows = Vec<ExactPredicateProviderRowV1>;

struct ExactTextRegistration {
    query: Arc<riffdb_query_module::CompiledExactTextResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: riffdb_types::ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
}

struct TokenizedTextRegistration {
    query: Arc<riffdb_query_module::CompiledTokenizedTextResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: riffdb_types::ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
}

struct ExactPredicateRegistration {
    query: Arc<riffdb_query_module::CompiledExactPredicateResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: riffdb_types::ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
}

struct NullableExactPredicateRegistration {
    query: Arc<riffdb_query_module::CompiledNullableExactPredicateResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: riffdb_types::ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
}

struct LongPatternRegistration {
    program: Arc<riffdb_query_ir::QueryAccessProgramV1>,
    step: QueryAccessStep,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: riffdb_types::ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
}

impl LongPatternRegistration {
    fn from_request(request: &LongPatternProjectionRequest) -> Self {
        Self {
            program: Arc::clone(request.program()),
            step: request.step().clone(),
            partition_key: request.partition_key().clone(),
            partition_value: request.partition_value().clone(),
            policy_shape: request.policy_shape(),
            row_policy: request.row_policy().cloned(),
        }
    }

    fn row_policy_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.row_policy
            .as_deref()
            .and_then(AuthorizedQueryRowPolicyContextV1::internal_capability_identity)
    }

    fn synthetic_plan_identity(&self) -> riffdb_types::QueryPlanHash {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(self.program.identity().hash().as_bytes());
        if let QueryAccessKind::LongPatternCandidate { pattern, .. } = self.step.access() {
            bytes.extend_from_slice(pattern.descriptor().digest().as_bytes());
        }
        riffdb_types::QueryPlanHash::from_bytes(*hash(HashDomain::QueryPlan, &bytes).as_bytes())
    }

    fn key(&self) -> SlotKey {
        slot_key(
            self.synthetic_plan_identity(),
            hash_partition_key(self.partition_key.as_bytes()),
            self.policy_shape,
            self.row_policy_identity(),
        )
    }

    fn matches(&self, request: &LongPatternProjectionRequest) -> bool {
        self.program.identity() == request.program().identity()
            && self.step == *request.step()
            && self.partition_key == *request.partition_key()
            && self.partition_value == *request.partition_value()
            && self.policy_shape == request.policy_shape()
            && self.row_policy_identity()
                == request
                    .row_policy()
                    .and_then(|policy| policy.internal_capability_identity())
    }
}

impl NullableExactPredicateRegistration {
    fn from_request(request: &NullableExactPredicateProjectionRequest) -> Self {
        Self {
            query: Arc::clone(request.query()),
            partition_key: request.partition_key().clone(),
            partition_value: request.partition_value().clone(),
            policy_shape: request.policy_shape(),
            row_policy: request.row_policy().cloned(),
        }
    }

    fn key(&self) -> SlotKey {
        slot_key(
            self.query.identity(),
            hash_partition_key(self.partition_key.as_bytes()),
            self.policy_shape,
            self.row_policy_identity(),
        )
    }

    fn row_policy_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.row_policy
            .as_deref()
            .and_then(AuthorizedQueryRowPolicyContextV1::internal_capability_identity)
    }

    fn matches(&self, request: &NullableExactPredicateProjectionRequest) -> bool {
        self.query.identity() == request.query().identity()
            && self.partition_key == *request.partition_key()
            && self.partition_value == *request.partition_value()
            && self.policy_shape == request.policy_shape()
            && self.row_policy_identity()
                == request
                    .row_policy()
                    .and_then(|policy| policy.internal_capability_identity())
    }
}

trait ExactPredicateRegistrationView {
    fn identity(&self) -> riffdb_types::QueryPlanHash;
    fn policy_shape(&self) -> riffdb_types::ApplicationRoleHash;
    fn access_program(&self) -> &riffdb_query_ir::QueryAccessProgramV1;
    fn partition_key(&self) -> &PartitionKey;
    fn partition_value(&self) -> &CanonicalValue;
    fn row_policy(&self) -> Option<&AuthorizedQueryRowPolicyContextV1>;
    fn referenced_profiles(
        &self,
    ) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, RebuildFailure>;
    fn max_candidates(&self) -> u32;
}

impl ExactPredicateRegistrationView for ExactPredicateRegistration {
    fn identity(&self) -> riffdb_types::QueryPlanHash {
        self.query.identity()
    }
    fn policy_shape(&self) -> riffdb_types::ApplicationRoleHash {
        self.policy_shape
    }

    fn access_program(&self) -> &riffdb_query_ir::QueryAccessProgramV1 {
        self.query.representative_program()
    }

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }

    fn row_policy(&self) -> Option<&AuthorizedQueryRowPolicyContextV1> {
        self.row_policy.as_deref()
    }

    fn referenced_profiles(
        &self,
    ) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, RebuildFailure> {
        predicate_referenced_profiles(self.query.program())
    }

    fn max_candidates(&self) -> u32 {
        self.query.program().provider_requirement().max_candidates()
    }
}

impl ExactPredicateRegistrationView for NullableExactPredicateRegistration {
    fn identity(&self) -> riffdb_types::QueryPlanHash {
        self.query.identity()
    }
    fn policy_shape(&self) -> riffdb_types::ApplicationRoleHash {
        self.policy_shape
    }

    fn access_program(&self) -> &riffdb_query_ir::QueryAccessProgramV1 {
        self.query.representative_program()
    }

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }

    fn row_policy(&self) -> Option<&AuthorizedQueryRowPolicyContextV1> {
        self.row_policy.as_deref()
    }

    fn referenced_profiles(
        &self,
    ) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, RebuildFailure> {
        nullable_predicate_referenced_profiles(self.query.program())
    }

    fn max_candidates(&self) -> u32 {
        self.query.program().provider_requirement().max_candidates()
    }
}

impl ExactPredicateRegistration {
    fn from_request(request: &ExactPredicateProjectionRequest) -> Self {
        Self {
            query: Arc::clone(request.query()),
            partition_key: request.partition_key().clone(),
            partition_value: request.partition_value().clone(),
            policy_shape: request.policy_shape(),
            row_policy: request.row_policy().cloned(),
        }
    }

    fn key(&self) -> SlotKey {
        slot_key(
            self.query.identity(),
            hash_partition_key(self.partition_key.as_bytes()),
            self.policy_shape,
            self.row_policy_identity(),
        )
    }

    fn row_policy_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.row_policy
            .as_deref()
            .and_then(AuthorizedQueryRowPolicyContextV1::internal_capability_identity)
    }

    fn matches(&self, request: &ExactPredicateProjectionRequest) -> bool {
        self.query.identity() == request.query().identity()
            && self.partition_key == *request.partition_key()
            && self.partition_value == *request.partition_value()
            && self.policy_shape == request.policy_shape()
            && self.row_policy_identity()
                == request
                    .row_policy()
                    .and_then(|policy| policy.internal_capability_identity())
    }
}

impl ExactTextRegistration {
    fn from_request(request: &ExactTextProjectionRequest) -> Self {
        Self {
            query: Arc::clone(request.query()),
            partition_key: request.partition_key().clone(),
            partition_value: request.partition_value().clone(),
            policy_shape: request.policy_shape(),
            row_policy: request.row_policy().cloned(),
        }
    }

    fn key(&self) -> SlotKey {
        slot_key(
            self.query.identity(),
            hash_partition_key(self.partition_key.as_bytes()),
            self.policy_shape,
            self.row_policy_identity(),
        )
    }

    fn row_policy_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.row_policy
            .as_deref()
            .and_then(AuthorizedQueryRowPolicyContextV1::internal_capability_identity)
    }

    fn matches(&self, request: &ExactTextProjectionRequest) -> bool {
        self.query.identity() == request.query().identity()
            && self.partition_key == *request.partition_key()
            && self.partition_value == *request.partition_value()
            && self.policy_shape == request.policy_shape()
            && self.row_policy_identity()
                == request
                    .row_policy()
                    .and_then(|policy| policy.internal_capability_identity())
    }
}

impl TokenizedTextRegistration {
    fn from_request(request: &TokenizedTextProjectionRequest) -> Self {
        Self {
            query: Arc::clone(request.query()),
            partition_key: request.partition_key().clone(),
            partition_value: request.partition_value().clone(),
            policy_shape: request.policy_shape(),
            row_policy: request.row_policy().cloned(),
        }
    }

    fn key(&self) -> SlotKey {
        slot_key(
            self.query.identity(),
            hash_partition_key(self.partition_key.as_bytes()),
            self.policy_shape,
            self.row_policy_identity(),
        )
    }

    fn row_policy_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.row_policy
            .as_deref()
            .and_then(AuthorizedQueryRowPolicyContextV1::internal_capability_identity)
    }

    fn matches(&self, request: &TokenizedTextProjectionRequest) -> bool {
        self.query.identity() == request.query().identity()
            && self.partition_key == *request.partition_key()
            && self.partition_value == *request.partition_value()
            && self.policy_shape == request.policy_shape()
            && self.row_policy_identity()
                == request
                    .row_policy()
                    .and_then(|policy| policy.internal_capability_identity())
    }
}

enum ExactTextSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(Arc<Selected<ExactTextProviderState>>),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

#[derive(Clone, Eq, PartialEq)]
enum ExactTextProviderState {
    V2(Box<ExactTextPartitionIndexV2>),
    V3(Box<ExactTextPartitionIndexV3>),
}

impl ExactTextProviderState {
    const fn partition(&self) -> PartitionKeyHash {
        match self {
            Self::V2(provider) => provider.partition(),
            Self::V3(provider) => provider.partition(),
        }
    }

    const fn generation(&self) -> ProjectionGeneration {
        match self {
            Self::V2(provider) => provider.generation(),
            Self::V3(provider) => provider.generation(),
        }
    }

    const fn frontier(&self) -> Option<CommitSequence> {
        match self {
            Self::V2(provider) => provider.frontier(),
            Self::V3(provider) => provider.frontier(),
        }
    }

    fn checkpoint_bytes(&self) -> Result<Vec<u8>, riffdb_projection::ExactTextProviderErrorV1> {
        match self {
            Self::V2(provider) => provider.to_checkpoint_bytes(),
            Self::V3(provider) => provider.to_checkpoint_bytes(),
        }
    }
}

struct ExactTextSlot {
    registration: ExactTextRegistration,
    checkpoint: PathBuf,
    state: Mutex<ExactTextSlotState>,
}

enum TokenizedTextSlotState {
    Building,
    Rebuilding(TokenizedTextEpochs),
    Ready(TokenizedTextEpochs),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

struct TokenizedTextEpochs {
    providers: VecDeque<Box<TokenizedTextPartitionIndexV1>>,
}

impl TokenizedTextEpochs {
    fn one(provider: TokenizedTextPartitionIndexV1) -> Self {
        Self {
            providers: VecDeque::from([Box::new(provider)]),
        }
    }

    fn current(&self) -> Option<&TokenizedTextPartitionIndexV1> {
        self.providers.back().map(Box::as_ref)
    }

    fn select(
        &self,
        pinned: Option<(CommitSequence, ProjectionGeneration)>,
    ) -> Option<&TokenizedTextPartitionIndexV1> {
        match pinned {
            None => self.current(),
            Some((epoch, generation)) => self.providers.iter().find_map(|provider| {
                (provider.frontier() == Some(epoch) && provider.generation() == generation)
                    .then_some(provider.as_ref())
            }),
        }
    }

    fn push(&mut self, provider: TokenizedTextPartitionIndexV1, retained: usize) {
        self.providers.push_back(Box::new(provider));
        while self.providers.len() > retained.max(1) {
            self.providers.pop_front();
        }
    }
}

struct TokenizedTextSlot {
    registration: TokenizedTextRegistration,
    checkpoint: PathBuf,
    state: Mutex<TokenizedTextSlotState>,
}

enum ExactPredicateSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(Arc<Selected<ExactPredicatePartitionIndexV4>>),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

enum NullableExactPredicateSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(Arc<Selected<ExactPredicatePartitionIndexV5>>),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

struct ExactPredicateSlot {
    registration: ExactPredicateRegistration,
    checkpoint: PathBuf,
    state: Mutex<ExactPredicateSlotState>,
}

struct NullableExactPredicateSlot {
    registration: NullableExactPredicateRegistration,
    checkpoint: PathBuf,
    state: Mutex<NullableExactPredicateSlotState>,
}

struct LongPatternProviderState {
    provider: LongPatternPartitionV1,
    generation: ProjectionGeneration,
    frontier: CommitSequence,
}

struct LongPatternEpochs {
    providers: VecDeque<Box<LongPatternProviderState>>,
}

impl LongPatternEpochs {
    fn one(provider: LongPatternProviderState) -> Self {
        Self {
            providers: VecDeque::from([Box::new(provider)]),
        }
    }

    fn current(&self) -> Option<&LongPatternProviderState> {
        self.providers.back().map(Box::as_ref)
    }

    fn select(&self, pinned: Option<CommitSequence>) -> Option<&LongPatternProviderState> {
        pinned.map_or_else(
            || self.current(),
            |epoch| {
                self.providers
                    .iter()
                    .find(|provider| provider.frontier == epoch)
                    .map(Box::as_ref)
            },
        )
    }

    fn push(&mut self, provider: LongPatternProviderState, retained: usize) {
        self.providers.push_back(Box::new(provider));
        while self.providers.len() > retained.max(1) {
            self.providers.pop_front();
        }
    }
}

enum LongPatternSlotState {
    Building,
    Rebuilding(LongPatternEpochs),
    Ready(LongPatternEpochs),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

struct LongPatternSlot {
    registration: LongPatternRegistration,
    checkpoint: PathBuf,
    state: Mutex<LongPatternSlotState>,
}

impl LongPatternSlot {
    fn new(registration: LongPatternRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(LongPatternSlotState::Building),
        }
    }
}

impl ExactTextSlot {
    fn new(registration: ExactTextRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(ExactTextSlotState::Building),
        }
    }
}

impl TokenizedTextSlot {
    fn new(registration: TokenizedTextRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(TokenizedTextSlotState::Building),
        }
    }
}

impl ExactPredicateSlot {
    fn new(registration: ExactPredicateRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(ExactPredicateSlotState::Building),
        }
    }
}

impl NullableExactPredicateSlot {
    fn new(registration: NullableExactPredicateRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(NullableExactPredicateSlotState::Building),
        }
    }
}

/// Dynamic exact providers registered only from immutable compiled named plans.
pub(crate) struct ExactTextRuntime {
    storage: ProjectionReadSource,
    root: PathBuf,
    predicate_root: PathBuf,
    history_incarnation: u64,
    initial_generation: ProjectionGeneration,
    slots: Mutex<BTreeMap<SlotKey, Arc<ExactTextSlot>>>,
    predicate_slots: Mutex<BTreeMap<SlotKey, Arc<ExactPredicateSlot>>>,
    nullable_predicate_slots: Mutex<BTreeMap<SlotKey, Arc<NullableExactPredicateSlot>>>,
    tokenized_root: PathBuf,
    tokenized_slots: Mutex<BTreeMap<SlotKey, Arc<TokenizedTextSlot>>>,
    long_pattern_root: PathBuf,
    long_pattern_slots: Mutex<BTreeMap<SlotKey, Arc<LongPatternSlot>>>,
}

impl ExactTextRuntime {
    pub(crate) fn open(
        storage: ProjectionReadSource,
        projections_root: &Path,
        history_incarnation: u64,
        initial_generation: ProjectionGeneration,
    ) -> Result<Arc<Self>, ExactTextRuntimeOpenError> {
        let root = projections_root.join("exact-text-v2");
        fs::create_dir_all(&root).map_err(|_| ExactTextRuntimeOpenError)?;
        let predicate_root = projections_root.join("exact-predicate-v4");
        fs::create_dir_all(&predicate_root).map_err(|_| ExactTextRuntimeOpenError)?;
        let tokenized_root = projections_root.join("tokenized-text-v1");
        fs::create_dir_all(&tokenized_root).map_err(|_| ExactTextRuntimeOpenError)?;
        let long_pattern_root = projections_root.join("long-pattern-v1");
        fs::create_dir_all(&long_pattern_root).map_err(|_| ExactTextRuntimeOpenError)?;
        Ok(Arc::new(Self {
            storage,
            root,
            predicate_root,
            history_incarnation,
            initial_generation,
            slots: Mutex::new(BTreeMap::new()),
            predicate_slots: Mutex::new(BTreeMap::new()),
            nullable_predicate_slots: Mutex::new(BTreeMap::new()),
            tokenized_root,
            tokenized_slots: Mutex::new(BTreeMap::new()),
            long_pattern_root,
            long_pattern_slots: Mutex::new(BTreeMap::new()),
        }))
    }

    fn registered_slots(&self) -> Result<Vec<Arc<ExactTextSlot>>, ExactTextProjectionPortError> {
        self.slots
            .lock()
            .map(|slots| slots.values().cloned().collect())
            .map_err(|_| ExactTextProjectionPortError::Integrity)
    }

    fn slot_for(
        &self,
        request: &ExactTextProjectionRequest,
    ) -> Result<Arc<ExactTextSlot>, ExactTextProjectionPortError> {
        let registration = ExactTextRegistration::from_request(request);
        let key = registration.key();
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        if let Some(slot) = slots.get(&key) {
            if !slot.registration.matches(request) {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            return Ok(Arc::clone(slot));
        }
        if slots.len() >= MAX_REGISTERED_EXACT_PARTITIONS {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        let checkpoint = self.root.join(format!("{}.rxts", hex(&key)));
        let slot = Arc::new(ExactTextSlot::new(registration, checkpoint));
        slots.insert(key, Arc::clone(&slot));
        Ok(slot)
    }

    fn registered_predicate_slots(
        &self,
    ) -> Result<Vec<Arc<ExactPredicateSlot>>, ExactTextProjectionPortError> {
        self.predicate_slots
            .lock()
            .map(|slots| slots.values().cloned().collect())
            .map_err(|_| ExactTextProjectionPortError::Integrity)
    }

    fn predicate_slot_for(
        &self,
        request: &ExactPredicateProjectionRequest,
    ) -> Result<Arc<ExactPredicateSlot>, ExactTextProjectionPortError> {
        let registration = ExactPredicateRegistration::from_request(request);
        let key = registration.key();
        let mut slots = self
            .predicate_slots
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        if let Some(slot) = slots.get(&key) {
            if !slot.registration.matches(request) {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            return Ok(Arc::clone(slot));
        }
        if slots.len() >= MAX_REGISTERED_EXACT_PARTITIONS {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        let checkpoint = self.predicate_root.join(format!("{}.rxps", hex(&key)));
        let slot = Arc::new(ExactPredicateSlot::new(registration, checkpoint));
        slots.insert(key, Arc::clone(&slot));
        Ok(slot)
    }

    fn registered_nullable_predicate_slots(
        &self,
    ) -> Result<Vec<Arc<NullableExactPredicateSlot>>, ExactTextProjectionPortError> {
        self.nullable_predicate_slots
            .lock()
            .map(|slots| slots.values().cloned().collect())
            .map_err(|_| ExactTextProjectionPortError::Integrity)
    }

    fn nullable_predicate_slot_for(
        &self,
        request: &NullableExactPredicateProjectionRequest,
    ) -> Result<Arc<NullableExactPredicateSlot>, ExactTextProjectionPortError> {
        let registration = NullableExactPredicateRegistration::from_request(request);
        let key = registration.key();
        let mut slots = self
            .nullable_predicate_slots
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        if let Some(slot) = slots.get(&key) {
            if !slot.registration.matches(request) {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            return Ok(Arc::clone(slot));
        }
        if slots.len() >= MAX_REGISTERED_EXACT_PARTITIONS {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        let checkpoint = self.predicate_root.join(format!("{}.rxp5", hex(&key)));
        let slot = Arc::new(NullableExactPredicateSlot::new(registration, checkpoint));
        slots.insert(key, Arc::clone(&slot));
        Ok(slot)
    }

    fn registered_tokenized_slots(
        &self,
    ) -> Result<Vec<Arc<TokenizedTextSlot>>, ExactTextProjectionPortError> {
        self.tokenized_slots
            .lock()
            .map(|slots| slots.values().cloned().collect())
            .map_err(|_| ExactTextProjectionPortError::Integrity)
    }

    fn registered_long_pattern_slots(
        &self,
    ) -> Result<Vec<Arc<LongPatternSlot>>, ExactTextProjectionPortError> {
        self.long_pattern_slots
            .lock()
            .map(|slots| slots.values().cloned().collect())
            .map_err(|_| ExactTextProjectionPortError::Integrity)
    }

    fn long_pattern_slot_for(
        &self,
        request: &LongPatternProjectionRequest,
    ) -> Result<Arc<LongPatternSlot>, ExactTextProjectionPortError> {
        let registration = LongPatternRegistration::from_request(request);
        let key = registration.key();
        let mut slots = self
            .long_pattern_slots
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        if let Some(slot) = slots.get(&key) {
            if !slot.registration.matches(request) {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            return Ok(Arc::clone(slot));
        }
        if slots.len() >= MAX_REGISTERED_EXACT_PARTITIONS {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        let checkpoint = self.long_pattern_root.join(format!("{}.rlpv", hex(&key)));
        let slot = Arc::new(LongPatternSlot::new(registration, checkpoint));
        slots.insert(key, Arc::clone(&slot));
        Ok(slot)
    }

    fn tokenized_slot_for(
        &self,
        request: &TokenizedTextProjectionRequest,
    ) -> Result<Arc<TokenizedTextSlot>, ExactTextProjectionPortError> {
        let registration = TokenizedTextRegistration::from_request(request);
        let key = registration.key();
        let mut slots = self
            .tokenized_slots
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        if let Some(slot) = slots.get(&key) {
            if !slot.registration.matches(request) {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            return Ok(Arc::clone(slot));
        }
        if slots.len() >= MAX_REGISTERED_EXACT_PARTITIONS {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        let checkpoint = self.tokenized_root.join(format!("{}.rttx", hex(&key)));
        let slot = Arc::new(TokenizedTextSlot::new(registration, checkpoint));
        slots.insert(key, Arc::clone(&slot));
        Ok(slot)
    }
}

const fn provider_policy_binding_is_exact(
    policy_mode: ProjectionProviderPolicyModeV1,
    has_row_policy: bool,
) -> bool {
    match policy_mode {
        ProjectionProviderPolicyModeV1::PartitionAligned => !has_row_policy,
        ProjectionProviderPolicyModeV1::BoundedRowAdmission => has_row_policy,
        ProjectionProviderPolicyModeV1::PolicySubpartition => false,
    }
}

impl ExactTextProjectionPort for ExactTextRuntime {
    fn execute(
        &self,
        request: ExactTextProjectionRequest,
    ) -> Result<ExactTextProjectionResult, ExactTextProjectionPortError> {
        let policy_binding_is_exact = provider_policy_binding_is_exact(
            request.query().binding().plan().provider().policy_mode(),
            request.row_policy().is_some(),
        );
        if !policy_binding_is_exact {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        let head = read_application_head(&self.storage)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let FrontierPosition::AppliedThrough(head) = head else {
            return Err(ExactTextProjectionPortError::Building);
        };
        let slot = self.slot_for(&request)?;
        let state = slot
            .state
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let provider = match &*state {
            ExactTextSlotState::Building => return Err(ExactTextProjectionPortError::Building),
            ExactTextSlotState::Rebuilding(_) => {
                return Err(ExactTextProjectionPortError::Rebuilding);
            }
            ExactTextSlotState::IntegrityFailure => {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            ExactTextSlotState::Unavailable { .. } => {
                return Err(ExactTextProjectionPortError::Unavailable);
            }
            ExactTextSlotState::Ready(provider) => provider,
        };
        if provider.frontier() != Some(head) {
            return Err(ExactTextProjectionPortError::FreshnessUnsatisfied);
        }
        let descriptor = request.query().binding().plan().provider();
        let participant = ProviderEpochObservationV1::new(
            descriptor.digest(),
            descriptor.state_identity().schema_hash(),
            self.history_incarnation,
            provider.generation(),
            head,
            head,
            ProviderLifecycleV1::Ready,
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let proof = negotiate_result_set_epoch_v1(
            ResultSetEpochContextV1::new(
                request.query().binding().plan().identity(),
                request.policy_shape(),
            ),
            &[participant],
            request.minimum_epoch().map_or(
                ResultSetEpochRequirementV1::Latest,
                ResultSetEpochRequirementV1::AtLeast,
            ),
        )
        .map_err(|error| match error {
            riffdb_projection::ResultSetEpochError::EpochExpired => {
                ExactTextProjectionPortError::SnapshotRetired
            }
            riffdb_projection::ResultSetEpochError::FreshnessUnavailable => {
                ExactTextProjectionPortError::FreshnessUnsatisfied
            }
            riffdb_projection::ResultSetEpochError::Diverged
            | riffdb_projection::ResultSetEpochError::IncarnationMismatch => {
                ExactTextProjectionPortError::Diverged
            }
            riffdb_projection::ResultSetEpochError::Rebuilding => {
                ExactTextProjectionPortError::Rebuilding
            }
            riffdb_projection::ResultSetEpochError::Unavailable
            | riffdb_projection::ResultSetEpochError::Retired => {
                ExactTextProjectionPortError::Unavailable
            }
            riffdb_projection::ResultSetEpochError::EmptyParticipants
            | riffdb_projection::ResultSetEpochError::TooManyParticipants
            | riffdb_projection::ResultSetEpochError::InvalidInterval => {
                ExactTextProjectionPortError::Integrity
            }
        })?;
        let result: ExactTextResultSetV1 =
            match (request.query().filter(), provider.provider.as_ref()) {
                (None, ExactTextProviderState::V2(provider))
                    if request.filter_value().is_none() =>
                {
                    execute_exact_text_result_set_v1(
                        request.query().binding().plan(),
                        request.query().binding().family(),
                        &proof,
                        provider,
                        request.query().operator(),
                        request.query().order(),
                        request.needle(),
                        request.offset(),
                        request.limit(),
                    )
                }
                (Some(filter), ExactTextProviderState::V3(provider))
                    if provider.filter_field() == filter.internal_field() =>
                {
                    execute_exact_text_filtered_result_set_v1(
                        request.query().binding().plan(),
                        request.query().binding().family(),
                        &proof,
                        provider,
                        request.query().operator(),
                        request.query().order(),
                        request.needle(),
                        request.filter_value(),
                        request.offset(),
                        request.limit(),
                    )
                }
                _ => return Err(ExactTextProjectionPortError::Integrity),
            }
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let rows = result
            .rows()
            .iter()
            .map(|row| ExactTextProjectionRow::new(row.key().clone(), row.output().clone()))
            .collect();
        Ok(ExactTextProjectionResult::new(
            rows,
            result.exact_total(),
            result.epoch(),
            result.generation(),
            result.provider(),
            result.history_incarnation(),
        ))
    }
}

impl LongPatternProjectionPort for ExactTextRuntime {
    fn execute(
        &self,
        request: LongPatternProjectionRequest,
    ) -> Result<LongPatternCandidateBatch, ExactTextProjectionPortError> {
        let QueryAccessKind::LongPatternCandidate { pattern, .. } = request.step().access() else {
            return Err(ExactTextProjectionPortError::Integrity);
        };
        if !provider_policy_binding_is_exact(
            pattern.descriptor().policy_mode(),
            request.row_policy().is_some(),
        ) || request.plan() != request.program().identity().hash()
        {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        let head = read_application_head(&self.storage)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let FrontierPosition::AppliedThrough(head) = head else {
            return Err(ExactTextProjectionPortError::Building);
        };
        if request.pinned_epoch().is_some_and(|epoch| epoch != head) {
            return Err(ExactTextProjectionPortError::SnapshotRetired);
        }
        let slot = self.long_pattern_slot_for(&request)?;
        let state = slot
            .state
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let epochs = match &*state {
            LongPatternSlotState::Building => {
                return Err(ExactTextProjectionPortError::Building);
            }
            LongPatternSlotState::Rebuilding(_) => {
                return Err(ExactTextProjectionPortError::Rebuilding);
            }
            LongPatternSlotState::Ready(epochs) => epochs,
            LongPatternSlotState::Unavailable { .. } => {
                return Err(ExactTextProjectionPortError::Unavailable);
            }
            LongPatternSlotState::IntegrityFailure => {
                return Err(ExactTextProjectionPortError::Integrity);
            }
        };
        let provider = epochs
            .select(request.pinned_epoch())
            .ok_or(ExactTextProjectionPortError::SnapshotRetired)?;
        if provider.frontier != head
            || request
                .minimum_epoch()
                .is_some_and(|minimum| provider.frontier < minimum)
        {
            return Err(ExactTextProjectionPortError::FreshnessUnsatisfied);
        }
        let observed = provider
            .provider
            .query_observed(request.pattern(), None)
            .map_err(map_long_pattern_error)?;
        let rows = observed
            .results()
            .iter()
            .map(|result| {
                decode_long_pattern_release(&slot.registration, result.key(), result.release())
            })
            .collect::<Result<Vec<_>, _>>()?;
        LongPatternCandidateBatch::checked(
            request.step().binding().to_owned(),
            rows,
            pattern.descriptor().digest(),
            pattern.descriptor().state_identity().schema_hash(),
            self.history_incarnation,
            provider.generation,
            provider.frontier,
            provider.frontier,
            observed.scanned_rows(),
            observed.verification_bytes(),
        )
        .ok_or(ExactTextProjectionPortError::Integrity)
    }
}

const fn map_long_pattern_error(error: LongPatternProviderErrorV1) -> ExactTextProjectionPortError {
    match error {
        LongPatternProviderErrorV1::CandidateBound
        | LongPatternProviderErrorV1::VerificationBound
        | LongPatternProviderErrorV1::ResultBound
        | LongPatternProviderErrorV1::PartitionBound
        | LongPatternProviderErrorV1::RowBound
        | LongPatternProviderErrorV1::CheckpointBound => {
            ExactTextProjectionPortError::ResponseTooLarge
        }
        LongPatternProviderErrorV1::ProfileMismatch
        | LongPatternProviderErrorV1::UniverseRequired
        | LongPatternProviderErrorV1::UniverseInvalid
        | LongPatternProviderErrorV1::InvalidBounds
        | LongPatternProviderErrorV1::CheckpointInvalid => ExactTextProjectionPortError::Integrity,
    }
}

impl TokenizedTextProjectionPort for ExactTextRuntime {
    fn execute(
        &self,
        request: TokenizedTextProjectionRequest,
    ) -> Result<ExactTextProjectionResult, ExactTextProjectionPortError> {
        let plan = request.query().tokenized_plan();
        let policy_binding_is_exact = provider_policy_binding_is_exact(
            plan.descriptor().policy_mode(),
            request.row_policy().is_some(),
        );
        if !policy_binding_is_exact {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        let head = read_application_head(&self.storage)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let FrontierPosition::AppliedThrough(head) = head else {
            return Err(ExactTextProjectionPortError::Building);
        };
        let slot = self.tokenized_slot_for(&request)?;
        let state = slot
            .state
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let provider = match &*state {
            TokenizedTextSlotState::Building => {
                return Err(ExactTextProjectionPortError::Building);
            }
            TokenizedTextSlotState::Rebuilding(epochs) => match request.pinned_snapshot() {
                Some(pinned) => epochs
                    .select(Some(pinned))
                    .ok_or(ExactTextProjectionPortError::SnapshotRetired)?,
                None => return Err(ExactTextProjectionPortError::Rebuilding),
            },
            TokenizedTextSlotState::Unavailable { .. } => {
                return Err(ExactTextProjectionPortError::Unavailable);
            }
            TokenizedTextSlotState::IntegrityFailure => {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            TokenizedTextSlotState::Ready(epochs) => epochs
                .select(request.pinned_snapshot())
                .ok_or(ExactTextProjectionPortError::SnapshotRetired)?,
        };
        if request.pinned_snapshot().is_none() && provider.frontier() != Some(head) {
            return Err(ExactTextProjectionPortError::FreshnessUnsatisfied);
        }
        let descriptor = plan.descriptor();
        let provider_epoch = provider
            .frontier()
            .ok_or(ExactTextProjectionPortError::Integrity)?;
        let participant = ProviderEpochObservationV1::new(
            descriptor.digest(),
            descriptor.state_identity().schema_hash(),
            self.history_incarnation,
            provider.generation(),
            provider_epoch,
            provider_epoch,
            ProviderLifecycleV1::Ready,
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        negotiate_result_set_epoch_v1(
            ResultSetEpochContextV1::new(request.query().identity(), request.policy_shape()),
            &[participant],
            request.pinned_snapshot().map_or_else(
                || {
                    request.minimum_epoch().map_or(
                        ResultSetEpochRequirementV1::Latest,
                        ResultSetEpochRequirementV1::AtLeast,
                    )
                },
                |(epoch, _)| ResultSetEpochRequirementV1::Exact(epoch),
            ),
        )
        .map_err(map_epoch_error)?;
        let page = execute_tokenized_text_v1(
            plan,
            provider,
            provider.generation(),
            provider_epoch,
            request.query_text(),
            request.offset(),
            u32::from(request.limit().get()),
        )
        .map_err(|error| match error {
            riffdb_query_executor::TokenizedTextExecutionErrorV1::EpochMismatch
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::PlanMismatch
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::ProviderCorrupt => {
                ExactTextProjectionPortError::Integrity
            }
            riffdb_query_executor::TokenizedTextExecutionErrorV1::InputLimit
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::EmptyQuery
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::TermLimit => {
                ExactTextProjectionPortError::InputInvalid
            }
            riffdb_query_executor::TokenizedTextExecutionErrorV1::CandidateLimit
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::ResultLimit
            | riffdb_query_executor::TokenizedTextExecutionErrorV1::ScoreOverflow => {
                ExactTextProjectionPortError::ResponseTooLarge
            }
        })?;
        let rows = page
            .rows()
            .iter()
            .map(|row| ExactTextProjectionRow::new(row.key().clone(), row.output().clone()))
            .collect();
        Ok(ExactTextProjectionResult::new_tokenized(
            rows,
            u64::from(page.exact_total()),
            page.epoch(),
            page.generation(),
            descriptor.digest(),
            self.history_incarnation,
            page.statistics_identity(),
        ))
    }
}

impl ExactPredicateProjectionPort for ExactTextRuntime {
    fn execute(
        &self,
        request: ExactPredicateProjectionRequest,
    ) -> Result<ExactPredicateProjectionResult, ExactTextProjectionPortError> {
        let policy_mode = request
            .query()
            .program()
            .provider_requirement()
            .policy_mode();
        let policy_binding_is_exact =
            provider_policy_binding_is_exact(policy_mode, request.row_policy().is_some());
        if !policy_binding_is_exact {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        let head = read_application_head(&self.storage)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let FrontierPosition::AppliedThrough(head) = head else {
            return Err(ExactTextProjectionPortError::Building);
        };
        let slot = self.predicate_slot_for(&request)?;
        let state = slot
            .state
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let provider = match &*state {
            ExactPredicateSlotState::Building => {
                return Err(ExactTextProjectionPortError::Building);
            }
            ExactPredicateSlotState::Rebuilding(_) => {
                return Err(ExactTextProjectionPortError::Rebuilding);
            }
            ExactPredicateSlotState::IntegrityFailure => {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            ExactPredicateSlotState::Unavailable { .. } => {
                return Err(ExactTextProjectionPortError::Unavailable);
            }
            ExactPredicateSlotState::Ready(provider) => provider,
        };
        if provider.binding().frontier() != head {
            return Err(ExactTextProjectionPortError::FreshnessUnsatisfied);
        }
        let descriptor = request
            .query()
            .program()
            .provider_descriptor()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let participant = ProviderEpochObservationV1::new(
            descriptor.digest(),
            descriptor.state_identity().schema_hash(),
            self.history_incarnation,
            provider.binding().generation(),
            head,
            head,
            ProviderLifecycleV1::Ready,
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let proof = negotiate_result_set_epoch_v1(
            ResultSetEpochContextV1::new(request.query().identity(), request.policy_shape()),
            &[participant],
            request.minimum_epoch().map_or(
                ResultSetEpochRequirementV1::Latest,
                ResultSetEpochRequirementV1::AtLeast,
            ),
        )
        .map_err(map_epoch_error)?;
        let result = execute_exact_predicate_result_set_v1(
            request.query().identity(),
            request.query().program(),
            &proof,
            provider,
            request.parameters(),
            request.member(),
            request.offset(),
            request.limit(),
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let binding = result.binding();
        let rows = result
            .rows()
            .iter()
            .map(|row| ExactTextProjectionRow::new(row.key().clone(), row.output().clone()))
            .collect();
        Ok(ExactPredicateProjectionResult::new(
            rows,
            result.exact_total(),
            binding.frontier(),
            binding.generation(),
            binding.descriptor(),
            binding.history_incarnation(),
        ))
    }

    fn execute_nullable(
        &self,
        request: NullableExactPredicateProjectionRequest,
    ) -> Result<ExactPredicateProjectionResult, ExactTextProjectionPortError> {
        let policy_binding_is_exact = provider_policy_binding_is_exact(
            request
                .query()
                .program()
                .provider_requirement()
                .policy_mode(),
            request.row_policy().is_some(),
        );
        if !policy_binding_is_exact {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        let head = read_application_head(&self.storage)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let FrontierPosition::AppliedThrough(head) = head else {
            return Err(ExactTextProjectionPortError::Building);
        };
        let slot = self.nullable_predicate_slot_for(&request)?;
        let state = slot
            .state
            .lock()
            .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let provider = match &*state {
            NullableExactPredicateSlotState::Building => {
                return Err(ExactTextProjectionPortError::Building);
            }
            NullableExactPredicateSlotState::Rebuilding(_) => {
                return Err(ExactTextProjectionPortError::Rebuilding);
            }
            NullableExactPredicateSlotState::IntegrityFailure => {
                return Err(ExactTextProjectionPortError::Integrity);
            }
            NullableExactPredicateSlotState::Unavailable { .. } => {
                return Err(ExactTextProjectionPortError::Unavailable);
            }
            NullableExactPredicateSlotState::Ready(provider) => provider,
        };
        if provider.binding().frontier() != head {
            return Err(ExactTextProjectionPortError::FreshnessUnsatisfied);
        }
        let participant = ProviderEpochObservationV1::new(
            provider.binding().descriptor(),
            EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5,
            self.history_incarnation,
            provider.binding().generation(),
            head,
            head,
            ProviderLifecycleV1::Ready,
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let proof = negotiate_result_set_epoch_v1(
            ResultSetEpochContextV1::new(request.query().identity(), request.policy_shape()),
            &[participant],
            request.minimum_epoch().map_or(
                ResultSetEpochRequirementV1::Latest,
                ResultSetEpochRequirementV1::AtLeast,
            ),
        )
        .map_err(map_epoch_error)?;
        let result = execute_nullable_exact_predicate_result_set_v1(
            request.query().identity(),
            request.query().program(),
            &proof,
            provider,
            request.parameters(),
            request.member(),
            request.offset(),
            request.limit(),
        )
        .map_err(|_| ExactTextProjectionPortError::Integrity)?;
        let binding = result.binding();
        let rows = result
            .rows()
            .iter()
            .map(|row| ExactTextProjectionRow::new(row.key().clone(), row.output().clone()))
            .collect();
        Ok(ExactPredicateProjectionResult::new(
            rows,
            result.exact_total(),
            binding.frontier(),
            binding.generation(),
            binding.descriptor(),
            binding.history_incarnation(),
        ))
    }
}

fn map_epoch_error(error: riffdb_projection::ResultSetEpochError) -> ExactTextProjectionPortError {
    match error {
        riffdb_projection::ResultSetEpochError::EpochExpired => {
            ExactTextProjectionPortError::SnapshotRetired
        }
        riffdb_projection::ResultSetEpochError::FreshnessUnavailable => {
            ExactTextProjectionPortError::FreshnessUnsatisfied
        }
        riffdb_projection::ResultSetEpochError::Diverged
        | riffdb_projection::ResultSetEpochError::IncarnationMismatch => {
            ExactTextProjectionPortError::Diverged
        }
        riffdb_projection::ResultSetEpochError::Rebuilding => {
            ExactTextProjectionPortError::Rebuilding
        }
        riffdb_projection::ResultSetEpochError::Unavailable
        | riffdb_projection::ResultSetEpochError::Retired => {
            ExactTextProjectionPortError::Unavailable
        }
        riffdb_projection::ResultSetEpochError::EmptyParticipants
        | riffdb_projection::ResultSetEpochError::TooManyParticipants
        | riffdb_projection::ResultSetEpochError::InvalidInterval => {
            ExactTextProjectionPortError::Integrity
        }
    }
}

impl fmt::Debug for ExactTextRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactTextRuntime([DERIVED_AUTHORITY])")
    }
}

struct StopState {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Owning guard for the bounded exact-provider rebuild/catch-up worker.
#[must_use = "the exact text worker must be explicitly stopped and joined"]
pub(crate) struct RunningExactTextWorker {
    stop: Arc<StopState>,
    task: Option<JoinHandle<()>>,
}

impl RunningExactTextWorker {
    pub(crate) fn start(runtime: Arc<ExactTextRuntime>) -> Result<Self, ExactTextWorkerStartError> {
        let stop = Arc::new(StopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let worker_stop = Arc::clone(&stop);
        let task = thread::Builder::new()
            .name("riffdb-exact-text".to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                while !stop_requested(&worker_stop) {
                    let _ = refresh_registered_slots(&runtime);
                    if wait_for_stop(&worker_stop, POLL_INTERVAL) {
                        break;
                    }
                }
            })
            .map_err(|_| ExactTextWorkerStartError)?;
        Ok(Self {
            stop,
            task: Some(task),
        })
    }

    pub(crate) fn shutdown(mut self) -> Result<(), ExactTextWorkerShutdownError> {
        {
            let mut requested = self
                .stop
                .requested
                .lock()
                .map_err(|_| ExactTextWorkerShutdownError)?;
            *requested = true;
            self.stop.changed.notify_all();
        }
        self.task
            .take()
            .expect("running exact worker retains its task")
            .join()
            .map_err(|_| ExactTextWorkerShutdownError)
    }
}

fn refresh_registered_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime.registered_slots().map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let mut expected_ready = None;
        let (prior_generation, prior_frontier) = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                ExactTextSlotState::Ready(provider)
                    if provider.frontier() == Some(head) && !provider.has_source_pin() =>
                {
                    continue;
                }
                ExactTextSlotState::Ready(provider) => {
                    expected_ready = Some(Arc::clone(provider));
                    (Some(provider.generation()), provider.frontier())
                }
                ExactTextSlotState::Rebuilding(generation) => (Some(*generation), None),
                ExactTextSlotState::Building => (None, None),
                ExactTextSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = ExactTextSlotState::Rebuilding(generation);
                    (Some(generation), None)
                }
                ExactTextSlotState::IntegrityFailure => continue,
            }
        };
        let result = rebuild_slot(runtime, &slot, head, prior_generation);
        let mut state = slot.state.lock().map_err(|_| ())?;
        // This worker is the sole checkpoint writer. Still compare
        // the exact expected selection after sync/reopen, so retirement
        // or a replaced selection cannot admit a stale private result.
        let expected_selection = match &*state {
            ExactTextSlotState::Ready(previous) => {
                expected_ready
                    .as_ref()
                    .is_some_and(|expected| Arc::ptr_eq(expected, previous))
                    && Some(previous.generation()) == prior_generation
                    && previous.frontier() == prior_frontier
            }
            ExactTextSlotState::Building => prior_generation.is_none() && prior_frontier.is_none(),
            ExactTextSlotState::Rebuilding(generation) => {
                Some(*generation) == prior_generation && prior_frontier.is_none()
            }
            _ => false,
        };
        if !expected_selection {
            continue;
        }
        match result {
            Ok(Some(provider)) => {
                if provider.frontier() != Some(provider.source_frontier())
                    || provider.source_frontier() > head
                    || !provider.validates_frontier(provider.source_frontier())
                {
                    *state = ExactTextSlotState::IntegrityFailure;
                    continue;
                }
                #[cfg(feature = "test-fixtures")]
                observe_test_point(
                    TestPoint::BeforeSelection,
                    &slot.checkpoint,
                    Some(provider.source_frontier()),
                );
                *state = ExactTextSlotState::Ready(Arc::new(provider));
                #[cfg(feature = "test-fixtures")]
                observe_test_point(TestPoint::AfterSelection, &slot.checkpoint, Some(head));
            }
            Ok(None) => {}
            Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                *state = ExactTextSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                *state = ExactTextSlotState::IntegrityFailure;
            }
        }
    }
    refresh_registered_long_pattern_slots(runtime)?;
    refresh_registered_predicate_slots(runtime)?;
    refresh_registered_nullable_predicate_slots(runtime)?;
    refresh_registered_tokenized_slots(runtime)?;
    Ok(())
}

fn refresh_registered_long_pattern_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime.registered_long_pattern_slots().map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let prior_generation = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                LongPatternSlotState::Ready(epochs)
                    if epochs
                        .current()
                        .is_some_and(|provider| provider.frontier == head) =>
                {
                    continue;
                }
                LongPatternSlotState::Ready(_) => {
                    let previous =
                        std::mem::replace(&mut *state, LongPatternSlotState::IntegrityFailure);
                    let LongPatternSlotState::Ready(epochs) = previous else {
                        return Err(());
                    };
                    let generation = epochs
                        .current()
                        .map(|provider| provider.generation)
                        .ok_or(())?;
                    *state = LongPatternSlotState::Rebuilding(epochs);
                    Some(generation)
                }
                LongPatternSlotState::Rebuilding(epochs) => Some(
                    epochs
                        .current()
                        .map(|provider| provider.generation)
                        .ok_or(())?,
                ),
                LongPatternSlotState::Building => None,
                LongPatternSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = LongPatternSlotState::Building;
                    Some(generation)
                }
                LongPatternSlotState::IntegrityFailure => continue,
            }
        };
        match rebuild_long_pattern_slot(runtime, &slot, head, prior_generation) {
            Ok(Some(provider)) => {
                let QueryAccessKind::LongPatternCandidate { pattern, .. } =
                    slot.registration.step.access()
                else {
                    return Err(());
                };
                let retained =
                    usize::try_from(pattern.descriptor().static_bounds().retained_epochs)
                        .map_err(|_| ())?;
                let mut state = slot.state.lock().map_err(|_| ())?;
                let previous =
                    std::mem::replace(&mut *state, LongPatternSlotState::IntegrityFailure);
                *state = match previous {
                    LongPatternSlotState::Rebuilding(mut epochs) => {
                        if epochs
                            .current()
                            .is_none_or(|current| current.frontier != head)
                        {
                            epochs.push(provider, retained);
                        }
                        LongPatternSlotState::Ready(epochs)
                    }
                    LongPatternSlotState::Building => {
                        LongPatternSlotState::Ready(LongPatternEpochs::one(provider))
                    }
                    _ => return Err(()),
                };
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = LongPatternSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = LongPatternSlotState::IntegrityFailure;
            }
        }
    }
    Ok(())
}

fn rebuild_long_pattern_slot(
    runtime: &ExactTextRuntime,
    slot: &LongPatternSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<LongPatternProviderState>, RebuildFailure> {
    if prior_generation.is_none()
        && let Some(bytes) = read_checkpoint(&slot.checkpoint)?
        && let Ok(provider_bytes) = decode_activation_checkpoint(
            &bytes,
            &slot.registration.key(),
            runtime.history_incarnation,
        )
        && let Ok(recovered) = decode_long_pattern_checkpoint(provider_bytes)
    {
        if recovered.frontier == head {
            return Ok(Some(recovered));
        }
        return rebuild_long_pattern_slot(runtime, slot, head, Some(recovered.generation));
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let provider = rebuild_long_pattern_partition(runtime, &slot.registration, head, generation)?;
    let provider_bytes = encode_long_pattern_checkpoint(&provider)?;
    persist_predicate_checkpoint_bytes(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider_bytes,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    Ok(Some(provider))
}

fn rebuild_long_pattern_partition(
    runtime: &ExactTextRuntime,
    registration: &LongPatternRegistration,
    expected_head: CommitSequence,
    generation: ProjectionGeneration,
) -> Result<LongPatternProviderState, RebuildFailure> {
    let QueryAccessKind::LongPatternCandidate { pattern, .. } = registration.step.access() else {
        return Err(RebuildFailure::Integrity);
    };
    let prefix = registration
        .step
        .internal_entity_key_schema()
        .encode_entity_prefix(std::slice::from_ref(&registration.partition_value))
        .map_err(|_| RebuildFailure::Integrity)?;
    let limit = StorageScanLimit::new(REBUILD_PAGE_ROWS).ok_or(RebuildFailure::Integrity)?;
    let mut records = BTreeMap::<EntityKey, riffdb_storage_api::StoredEntityRecordV1>::new();
    let mut after = None;
    loop {
        let request = AuthoritativeEntityPartitionScanRequest::new(
            registration.step.internal_entity_id(),
            prefix.clone(),
            after,
            limit,
        )
        .map_err(|_| RebuildFailure::Integrity)?;
        let page = AuthoritativeScanReader::scan_entity_partition(&runtime.storage, request)
            .map_err(|_| RebuildFailure::Transient)?;
        if page.application_head() != FrontierPosition::AppliedThrough(expected_head) {
            return Err(RebuildFailure::Transient);
        }
        for item in page.records() {
            let record = item.value();
            if records.len() >= pattern.bounds().rows() as usize
                || records
                    .insert(record.target().key().clone(), record.clone())
                    .is_some()
            {
                return Err(RebuildFailure::Capacity(generation));
            }
        }
        let Some(next) = page.next_after().cloned() else {
            break;
        };
        after = Some(next);
    }
    if let Some(policy) = registration.row_policy.as_deref() {
        let candidates = records.keys().cloned().collect::<Vec<_>>();
        let candidate_set = records.keys().cloned().collect::<BTreeSet<_>>();
        let query_executor = runtime.storage.query_executor();
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &query_executor,
            registration.step.internal_entity_id(),
            &candidates,
            policy,
        )
        .map_err(|error| map_policy_admission_error(error, generation))?;
        if !admission.covers(registration.step.internal_entity_id(), &candidate_set) {
            return Err(RebuildFailure::Integrity);
        }
        records.retain(|key, _| admission.admits(key));
    }
    let access = registration
        .program
        .internal_entity_access(registration.step.entity())
        .ok_or(RebuildFailure::Integrity)?;
    let field_ids = access.internal_fields().collect::<BTreeMap<_, _>>();
    let text_field = *field_ids
        .get(pattern.field())
        .ok_or(RebuildFailure::Integrity)?;
    let mut provider = LongPatternPartitionV1::new(pattern.profile(), pattern.bounds());
    for (key, record) in records {
        let source = record
            .fields()
            .fields()
            .binary_search_by_key(&text_field, |(field, _)| *field)
            .ok()
            .and_then(|index| match &record.fields().fields()[index].1 {
                CanonicalValue::String(value) => Some(value.as_str()),
                _ => None,
            })
            .ok_or(RebuildFailure::Integrity)?;
        let release = encode_long_pattern_release(&key, record.fields())?;
        provider
            .upsert(hash_entity_key(key.as_bytes()), source, release)
            .map_err(|error| match error {
                LongPatternProviderErrorV1::RowBound
                | LongPatternProviderErrorV1::PartitionBound => {
                    RebuildFailure::Capacity(generation)
                }
                _ => RebuildFailure::Integrity,
            })?;
    }
    Ok(LongPatternProviderState {
        provider,
        generation,
        frontier: expected_head,
    })
}

fn refresh_registered_tokenized_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime.registered_tokenized_slots().map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let prior_generation = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                TokenizedTextSlotState::Ready(epochs)
                    if epochs
                        .current()
                        .and_then(TokenizedTextPartitionIndexV1::frontier)
                        == Some(head) =>
                {
                    continue;
                }
                TokenizedTextSlotState::Ready(_) => {
                    let previous =
                        std::mem::replace(&mut *state, TokenizedTextSlotState::IntegrityFailure);
                    let TokenizedTextSlotState::Ready(epochs) = previous else {
                        return Err(());
                    };
                    let generation = epochs
                        .current()
                        .map(TokenizedTextPartitionIndexV1::generation)
                        .ok_or(())?;
                    *state = TokenizedTextSlotState::Rebuilding(epochs);
                    Some(generation)
                }
                TokenizedTextSlotState::Rebuilding(epochs) => Some(
                    epochs
                        .current()
                        .map(TokenizedTextPartitionIndexV1::generation)
                        .ok_or(())?,
                ),
                TokenizedTextSlotState::Building => None,
                TokenizedTextSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = TokenizedTextSlotState::Building;
                    Some(generation)
                }
                TokenizedTextSlotState::IntegrityFailure => continue,
            }
        };
        match rebuild_tokenized_slot(runtime, &slot, head, prior_generation) {
            Ok(Some(provider)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                let retained = usize::try_from(
                    slot.registration
                        .query
                        .tokenized_plan()
                        .descriptor()
                        .static_bounds()
                        .retained_epochs,
                )
                .map_err(|_| ())?;
                let previous =
                    std::mem::replace(&mut *state, TokenizedTextSlotState::IntegrityFailure);
                let mut epochs = match previous {
                    TokenizedTextSlotState::Rebuilding(epochs) => epochs,
                    TokenizedTextSlotState::Building => TokenizedTextEpochs::one(provider.clone()),
                    _ => return Err(()),
                };
                if epochs
                    .current()
                    .is_none_or(|current| current.frontier() != provider.frontier())
                {
                    epochs.push(provider, retained);
                }
                *state = TokenizedTextSlotState::Ready(epochs);
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = TokenizedTextSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = TokenizedTextSlotState::IntegrityFailure;
            }
        }
    }
    Ok(())
}

fn refresh_registered_predicate_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime.registered_predicate_slots().map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let mut expected_ready = None;
        let (prior_generation, prior_frontier) = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                ExactPredicateSlotState::Ready(provider)
                    if provider.binding().frontier() == head && !provider.has_source_pin() =>
                {
                    continue;
                }
                ExactPredicateSlotState::Ready(provider) => {
                    expected_ready = Some(Arc::clone(provider));
                    (
                        Some(provider.binding().generation()),
                        Some(provider.binding().frontier()),
                    )
                }
                ExactPredicateSlotState::Rebuilding(generation) => (Some(*generation), None),
                ExactPredicateSlotState::Building => (None, None),
                ExactPredicateSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = ExactPredicateSlotState::Rebuilding(generation);
                    (Some(generation), None)
                }
                ExactPredicateSlotState::IntegrityFailure => continue,
            }
        };
        let result = rebuild_predicate_slot(runtime, &slot, head, prior_generation);
        let mut state = slot.state.lock().map_err(|_| ())?;
        // This worker is the sole checkpoint writer. Still compare
        // the exact expected selection after sync/reopen, so retirement
        // or a replaced selection cannot admit a stale private result.
        let expected_selection = match &*state {
            ExactPredicateSlotState::Ready(previous) => {
                expected_ready
                    .as_ref()
                    .is_some_and(|expected| Arc::ptr_eq(expected, previous))
                    && Some(previous.binding().generation()) == prior_generation
                    && Some(previous.binding().frontier()) == prior_frontier
            }
            ExactPredicateSlotState::Building => {
                prior_generation.is_none() && prior_frontier.is_none()
            }
            ExactPredicateSlotState::Rebuilding(generation) => {
                Some(*generation) == prior_generation && prior_frontier.is_none()
            }
            _ => false,
        };
        if !expected_selection {
            continue;
        }
        match result {
            Ok(Some(provider)) => {
                if provider.binding().frontier() != provider.source_frontier()
                    || provider.source_frontier() > head
                    || !provider.validates_frontier(provider.source_frontier())
                {
                    *state = ExactPredicateSlotState::IntegrityFailure;
                    continue;
                }
                #[cfg(feature = "test-fixtures")]
                observe_test_point(
                    TestPoint::BeforeSelection,
                    &slot.checkpoint,
                    Some(provider.source_frontier()),
                );
                *state = ExactPredicateSlotState::Ready(Arc::new(provider));
                #[cfg(feature = "test-fixtures")]
                observe_test_point(TestPoint::AfterSelection, &slot.checkpoint, Some(head));
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                *state = ExactPredicateSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                *state = ExactPredicateSlotState::IntegrityFailure;
            }
        }
    }
    Ok(())
}

fn refresh_registered_nullable_predicate_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime
        .registered_nullable_predicate_slots()
        .map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let mut expected_ready = None;
        let (prior_generation, prior_frontier) = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                NullableExactPredicateSlotState::Ready(provider)
                    if provider.binding().frontier() == head && !provider.has_source_pin() =>
                {
                    continue;
                }
                NullableExactPredicateSlotState::Ready(provider) => {
                    expected_ready = Some(Arc::clone(provider));
                    (
                        Some(provider.binding().generation()),
                        Some(provider.binding().frontier()),
                    )
                }
                NullableExactPredicateSlotState::Rebuilding(generation) => {
                    (Some(*generation), None)
                }
                NullableExactPredicateSlotState::Building => (None, None),
                NullableExactPredicateSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = NullableExactPredicateSlotState::Rebuilding(generation);
                    (Some(generation), None)
                }
                NullableExactPredicateSlotState::IntegrityFailure => continue,
            }
        };
        let result = rebuild_nullable_predicate_slot(runtime, &slot, head, prior_generation);
        let mut state = slot.state.lock().map_err(|_| ())?;
        // This worker is the sole checkpoint writer. Still compare
        // the exact expected selection after sync/reopen, so retirement
        // or a replaced selection cannot admit a stale private result.
        let expected_selection = match &*state {
            NullableExactPredicateSlotState::Ready(previous) => {
                expected_ready
                    .as_ref()
                    .is_some_and(|expected| Arc::ptr_eq(expected, previous))
                    && Some(previous.binding().generation()) == prior_generation
                    && Some(previous.binding().frontier()) == prior_frontier
            }
            NullableExactPredicateSlotState::Building => {
                prior_generation.is_none() && prior_frontier.is_none()
            }
            NullableExactPredicateSlotState::Rebuilding(generation) => {
                Some(*generation) == prior_generation && prior_frontier.is_none()
            }
            _ => false,
        };
        if !expected_selection {
            continue;
        }
        match result {
            Ok(Some(provider)) => {
                if provider.binding().frontier() != provider.source_frontier()
                    || provider.source_frontier() > head
                    || !provider.validates_frontier(provider.source_frontier())
                {
                    *state = NullableExactPredicateSlotState::IntegrityFailure;
                    continue;
                }
                #[cfg(feature = "test-fixtures")]
                observe_test_point(
                    TestPoint::BeforeSelection,
                    &slot.checkpoint,
                    Some(provider.source_frontier()),
                );
                *state = NullableExactPredicateSlotState::Ready(Arc::new(provider));
                #[cfg(feature = "test-fixtures")]
                observe_test_point(TestPoint::AfterSelection, &slot.checkpoint, Some(head));
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                *state = NullableExactPredicateSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                *state = NullableExactPredicateSlotState::IntegrityFailure;
            }
        }
    }
    Ok(())
}

fn rebuild_slot(
    runtime: &ExactTextRuntime,
    slot: &ExactTextSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<Selected<ExactTextProviderState>>, RebuildFailure> {
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::Preparing, &slot.checkpoint, Some(head));
    let previous = {
        let state = slot.state.lock().map_err(|_| RebuildFailure::Integrity)?;
        match &*state {
            ExactTextSlotState::Ready(previous) => Some(Arc::clone(previous)),
            _ => None,
        }
    };
    if let Some(previous) = previous {
        if Some(previous.generation()) != prior_generation {
            return Err(RebuildFailure::Transient);
        }
        let Some(captured) =
            CapturedSource::capture_successor(runtime, head, previous.generation(), &previous)?
        else {
            return Ok(None);
        };
        if let Some(next) = catch_up::prepare_text(&captured, &previous, &slot.registration)? {
            if next.source_frontier() != previous.source_frontier() {
                persist_checked_slot(runtime, slot, &next.provider)?;
            }
            return Ok(Some(next));
        }
    }
    let mut prior_generation = prior_generation;
    if prior_generation.is_none()
        && let Some(bytes) = read_checkpoint(&slot.checkpoint)?
        && let Ok(provider_bytes) = decode_activation_checkpoint(
            &bytes,
            &slot.registration.key(),
            runtime.history_incarnation,
        )
        && let Ok(recovered) = recover_provider(&slot.registration, provider_bytes)
        && recovered.partition() == hash_partition_key(slot.registration.partition_key.as_bytes())
    {
        prior_generation = Some(recovered.generation());
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let captured = CapturedSource::capture(runtime, head, generation)?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::FullPartitionRead, &slot.checkpoint, Some(head));
    let (rows, candidates) = read_complete_partition(&captured, &slot.registration, generation)?;
    let provider = match slot.registration.query.filter() {
        None => {
            let rows = rows
                .into_iter()
                .map(|(key, (text, _, output))| (key, (text, output)))
                .collect::<BTreeMap<_, _>>();
            ExactTextProviderState::V2(Box::new(
                ExactTextPartitionIndexV2::rebuild(
                    hash_partition_key(slot.registration.partition_key.as_bytes()),
                    generation,
                    head,
                    ExactTextProfileV1::BinaryUtf8V1,
                    &rows,
                )
                .map_err(|_| RebuildFailure::Integrity)?,
            ))
        }
        Some(filter) => {
            let rows = rows
                .into_iter()
                .map(|(key, (text, filter, output))| {
                    (key, (text, filter.unwrap_or(CanonicalValue::Null), output))
                })
                .collect::<BTreeMap<_, _>>();
            ExactTextProviderState::V3(Box::new(
                ExactTextPartitionIndexV3::rebuild(
                    hash_partition_key(slot.registration.partition_key.as_bytes()),
                    generation,
                    head,
                    ExactTextProfileV1::BinaryUtf8V1,
                    filter.internal_field(),
                    &rows,
                )
                .map_err(|_| RebuildFailure::Integrity)?,
            ))
        }
    };
    persist_checked_slot(runtime, slot, &provider)?;
    Ok(Some(Selected::new(provider, captured, candidates)))
}

fn persist_checked_slot(
    runtime: &ExactTextRuntime,
    slot: &ExactTextSlot,
    provider: &ExactTextProviderState,
) -> Result<(), RebuildFailure> {
    persist_checkpoint(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        provider,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    let reopened = read_checkpoint(&slot.checkpoint)?.ok_or(RebuildFailure::Integrity)?;
    let bytes = decode_activation_checkpoint(
        &reopened,
        &slot.registration.key(),
        runtime.history_incarnation,
    )?;
    if recover_provider(&slot.registration, bytes).map_err(|_| RebuildFailure::Integrity)?
        != *provider
    {
        return Err(RebuildFailure::Integrity);
    }
    Ok(())
}

fn rebuild_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &ExactPredicateSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<Selected<ExactPredicatePartitionIndexV4>>, RebuildFailure> {
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::Preparing, &slot.checkpoint, Some(head));
    let previous = {
        let state = slot.state.lock().map_err(|_| RebuildFailure::Integrity)?;
        match &*state {
            ExactPredicateSlotState::Ready(previous) => Some(Arc::clone(previous)),
            _ => None,
        }
    };
    if let Some(previous) = previous {
        if Some(previous.binding().generation()) != prior_generation {
            return Err(RebuildFailure::Transient);
        }
        let Some(captured) = CapturedSource::capture_successor(
            runtime,
            head,
            previous.binding().generation(),
            &previous,
        )?
        else {
            return Ok(None);
        };
        if let Some(next) = catch_up::prepare_predicate(&captured, &previous, &slot.registration)? {
            if next.source_frontier() != previous.source_frontier() {
                persist_checked_predicate_slot(runtime, slot, &next.provider)?;
            }
            return Ok(Some(next));
        }
    }
    let mut prior_generation = prior_generation;
    if prior_generation.is_none()
        && let Some(bytes) = read_checkpoint(&slot.checkpoint)?
        && let Ok(provider_bytes) = decode_activation_checkpoint(
            &bytes,
            &slot.registration.key(),
            runtime.history_incarnation,
        )
        && let Ok(recovered) = ExactPredicatePartitionIndexV4::from_checkpoint_bytes(provider_bytes)
        && recovered.binding().plan() == slot.registration.query.identity()
        && recovered.binding().policy_shape() == slot.registration.policy_shape
        && recovered.binding().partition()
            == hash_partition_key(slot.registration.partition_key.as_bytes())
    {
        prior_generation = Some(recovered.binding().generation());
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let captured = CapturedSource::capture(runtime, head, generation)?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::FullPartitionRead, &slot.checkpoint, Some(head));
    let (rows, candidates) =
        read_complete_predicate_partition(&captured, &slot.registration, generation)?;
    let binding = ExactPredicateProviderBindingV1::new(
        slot.registration.query.identity(),
        slot.registration.query.program(),
        slot.registration.policy_shape,
        hash_partition_key(slot.registration.partition_key.as_bytes()),
        runtime.history_incarnation,
        generation,
        head,
    )
    .map_err(|_| RebuildFailure::Integrity)?;
    let provider = ExactPredicatePartitionIndexV4::rebuild(
        binding,
        slot.registration.query.program().clone(),
        rows,
    )
    .map_err(|error| match error {
        riffdb_projection::ExactPredicateProviderErrorV1::BoundExceeded
        | riffdb_projection::ExactPredicateProviderErrorV1::StateAmplification
        | riffdb_projection::ExactPredicateProviderErrorV1::FuelExhausted => {
            RebuildFailure::Capacity(generation)
        }
        _ => RebuildFailure::Integrity,
    })?;
    persist_checked_predicate_slot(runtime, slot, &provider)?;
    Ok(Some(Selected::new(provider, captured, candidates)))
}

fn persist_checked_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &ExactPredicateSlot,
    provider: &ExactPredicatePartitionIndexV4,
) -> Result<(), RebuildFailure> {
    persist_predicate_checkpoint(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        provider,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    let reopened = read_checkpoint(&slot.checkpoint)?.ok_or(RebuildFailure::Integrity)?;
    let bytes = decode_activation_checkpoint(
        &reopened,
        &slot.registration.key(),
        runtime.history_incarnation,
    )?;
    if ExactPredicatePartitionIndexV4::from_checkpoint_bytes(bytes)
        .map_err(|_| RebuildFailure::Integrity)?
        != *provider
    {
        return Err(RebuildFailure::Integrity);
    }
    Ok(())
}

fn rebuild_tokenized_slot(
    runtime: &ExactTextRuntime,
    slot: &TokenizedTextSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<TokenizedTextPartitionIndexV1>, RebuildFailure> {
    if prior_generation.is_none()
        && let Some(bytes) = read_checkpoint(&slot.checkpoint)?
        && let Ok(provider_bytes) = decode_activation_checkpoint(
            &bytes,
            &slot.registration.key(),
            runtime.history_incarnation,
        )
        && let Ok(recovered) = TokenizedTextPartitionIndexV1::from_checkpoint_bytes(provider_bytes)
        && recovered.config().index_identity()
            == slot.registration.query.tokenized_plan().index_identity()
        && recovered.partition() == hash_partition_key(slot.registration.partition_key.as_bytes())
    {
        if recovered.frontier() == Some(head) {
            return Ok(Some(recovered));
        }
        return rebuild_tokenized_slot(runtime, slot, head, Some(recovered.generation()));
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let plan = slot.registration.query.tokenized_plan();
    let config = TokenizedTextConfigV1::new(
        plan.index_identity(),
        plan.analyzer(),
        plan.fields()
            .iter()
            .map(|field| TokenizedTextFieldV1::new(field.field(), field.weight()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RebuildFailure::Integrity)?,
    )
    .map_err(|_| RebuildFailure::Integrity)?;
    let rows = read_complete_tokenized_partition(runtime, &slot.registration, head, generation)?;
    let provider = TokenizedTextPartitionIndexV1::rebuild(
        config,
        hash_partition_key(slot.registration.partition_key.as_bytes()),
        generation,
        head,
        &rows,
    )
    .map_err(|error| match error {
        riffdb_projection::TokenizedTextErrorV1::PartitionLimit
        | riffdb_projection::TokenizedTextErrorV1::DocumentLimit
        | riffdb_projection::TokenizedTextErrorV1::OutputLimit
        | riffdb_projection::TokenizedTextErrorV1::CheckpointLimit => {
            RebuildFailure::Capacity(generation)
        }
        _ => RebuildFailure::Integrity,
    })?;
    persist_tokenized_checkpoint(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    Ok(Some(provider))
}

fn read_complete_tokenized_partition(
    runtime: &ExactTextRuntime,
    registration: &TokenizedTextRegistration,
    expected_head: CommitSequence,
    generation: ProjectionGeneration,
) -> Result<Vec<TokenizedTextMutationV1>, RebuildFailure> {
    let program = registration.query.representative_program();
    let step = program.steps().first().ok_or(RebuildFailure::Integrity)?;
    if program.steps().len() != 1 {
        return Err(RebuildFailure::Integrity);
    }
    let access = program
        .internal_entity_access(step.entity())
        .ok_or(RebuildFailure::Integrity)?;
    let partition_field_name = step
        .predicates()
        .iter()
        .find(|predicate| {
            predicate.operator() == riffdb_query_ir::QueryPredicateOperator::Equal
                && matches!(
                    predicate.value(),
                    riffdb_query_ir::QueryPredicateValue::Parameter(name)
                        if name == program.partition_parameter()
                )
        })
        .map(riffdb_query_ir::QueryPredicate::field)
        .ok_or(RebuildFailure::Integrity)?;
    let partition_field = access
        .internal_field_id(partition_field_name)
        .ok_or(RebuildFailure::Integrity)?;
    let output_fields = step
        .selected_fields()
        .iter()
        .map(|name| {
            access
                .internal_field_id(name)
                .ok_or(RebuildFailure::Integrity)
        })
        .collect::<Result<BTreeSet<FieldId>, _>>()?;
    let text_fields = registration
        .query
        .tokenized_plan()
        .fields()
        .iter()
        .map(|field| field.field())
        .collect::<BTreeSet<_>>();
    let snapshot = runtime
        .storage
        .pin()
        .map_err(|_| RebuildFailure::Transient)?;
    let catalog = snapshot
        .read_active_catalog()
        .map_err(|_| RebuildFailure::Transient)?
        .ok_or(RebuildFailure::Transient)?;
    if snapshot
        .application_frontier()
        .map_err(|_| RebuildFailure::Transient)?
        != Some(expected_head)
        || catalog.lineage() != program.contract().lineage()
        || catalog.contract_version() != program.contract().version()
        || catalog.bundle_hash() != program.contract().bundle_hash()
    {
        return Err(RebuildFailure::Transient);
    }
    let prefix = step
        .internal_entity_key_schema()
        .encode_entity_prefix(std::slice::from_ref(&registration.partition_value))
        .map_err(|_| RebuildFailure::Integrity)?;
    let limit = StorageScanLimit::new(REBUILD_PAGE_ROWS).ok_or(RebuildFailure::Integrity)?;
    let mut after = None;
    let mut candidates = BTreeSet::new();
    let mut rows =
        BTreeMap::<riffdb_types::EntityKey, (Vec<(FieldId, String)>, CanonicalRecord)>::new();
    loop {
        let request = AuthoritativeEntityPartitionScanRequest::new(
            step.internal_entity_id(),
            prefix.clone(),
            after,
            limit,
        )
        .map_err(|_| RebuildFailure::Integrity)?;
        let page = AuthoritativeScanReader::scan_entity_partition(&snapshot, request)
            .map_err(|_| RebuildFailure::Transient)?;
        for source in page.records() {
            let record = source.value();
            let values = record.fields().fields();
            if values
                .iter()
                .find(|(field, _)| *field == partition_field)
                .map(|(_, value)| value)
                != Some(&registration.partition_value)
            {
                return Err(RebuildFailure::Integrity);
            }
            let key = record.target().key().clone();
            if !candidates.insert(key.clone()) {
                return Err(RebuildFailure::Integrity);
            }
            if candidates.len() > MAX_PROJECTED_POLICY_CANDIDATES_V1 {
                return Err(RebuildFailure::Capacity(generation));
            }
            let fields = values
                .iter()
                .filter(|(field, _)| text_fields.contains(field))
                .filter_map(|(field, value)| match value {
                    CanonicalValue::String(value) => Some(Ok((*field, value.as_str().to_owned()))),
                    CanonicalValue::Null => None,
                    _ => Some(Err(RebuildFailure::Integrity)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let output = CanonicalRecord::new(
                values
                    .iter()
                    .filter(|(field, _)| output_fields.contains(field))
                    .cloned()
                    .collect(),
            )
            .map_err(|_| RebuildFailure::Integrity)?;
            if output.len() != output_fields.len() || rows.insert(key, (fields, output)).is_some() {
                return Err(RebuildFailure::Integrity);
            }
        }
        let Some(next) = page.next_after().cloned() else {
            break;
        };
        after = Some(next);
    }
    if let Some(policy) = registration.row_policy.as_deref() {
        let ordered_candidates = candidates.iter().cloned().collect::<Vec<_>>();
        let query_executor = runtime.storage.query_executor();
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &query_executor,
            step.internal_entity_id(),
            &ordered_candidates,
            policy,
        )
        .map_err(|error| map_policy_admission_error(error, generation))?;
        if !admission.covers(step.internal_entity_id(), &candidates) {
            return Err(RebuildFailure::Integrity);
        }
        rows.retain(|key, _| admission.admits(key));
    }
    rows.into_iter()
        .map(|(key, (fields, output))| {
            TokenizedTextMutationV1::upsert(key, fields, output).map_err(|error| match error {
                riffdb_projection::TokenizedTextErrorV1::DocumentLimit
                | riffdb_projection::TokenizedTextErrorV1::OutputLimit => {
                    RebuildFailure::Capacity(generation)
                }
                _ => RebuildFailure::Integrity,
            })
        })
        .collect()
}

fn rebuild_nullable_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &NullableExactPredicateSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<Selected<ExactPredicatePartitionIndexV5>>, RebuildFailure> {
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::Preparing, &slot.checkpoint, Some(head));
    let previous = {
        let state = slot.state.lock().map_err(|_| RebuildFailure::Integrity)?;
        match &*state {
            NullableExactPredicateSlotState::Ready(previous) => Some(Arc::clone(previous)),
            _ => None,
        }
    };
    if let Some(previous) = previous {
        if Some(previous.binding().generation()) != prior_generation {
            return Err(RebuildFailure::Transient);
        }
        let Some(captured) = CapturedSource::capture_successor(
            runtime,
            head,
            previous.binding().generation(),
            &previous,
        )?
        else {
            return Ok(None);
        };
        if let Some(next) = catch_up::prepare_predicate(&captured, &previous, &slot.registration)? {
            if next.source_frontier() != previous.source_frontier() {
                persist_checked_nullable_predicate_slot(runtime, slot, &next.provider)?;
            }
            return Ok(Some(next));
        }
    }
    let mut prior_generation = prior_generation;
    if prior_generation.is_none()
        && let Some(bytes) = read_checkpoint(&slot.checkpoint)?
        && let Ok(provider_bytes) = decode_activation_checkpoint(
            &bytes,
            &slot.registration.key(),
            runtime.history_incarnation,
        )
        && let Ok(recovered) = ExactPredicatePartitionIndexV5::from_checkpoint_bytes(provider_bytes)
        && recovered.binding().plan() == slot.registration.query.identity()
        && recovered.binding().policy_shape() == slot.registration.policy_shape
        && recovered.binding().partition()
            == hash_partition_key(slot.registration.partition_key.as_bytes())
    {
        prior_generation = Some(recovered.binding().generation());
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let captured = CapturedSource::capture(runtime, head, generation)?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::FullPartitionRead, &slot.checkpoint, Some(head));
    let (rows, candidates) =
        read_complete_predicate_partition(&captured, &slot.registration, generation)?;
    let binding = ExactPredicateProviderBindingV2::new(
        slot.registration.query.identity(),
        slot.registration.query.program(),
        slot.registration.policy_shape,
        hash_partition_key(slot.registration.partition_key.as_bytes()),
        runtime.history_incarnation,
        generation,
        head,
    )
    .map_err(|_| RebuildFailure::Integrity)?;
    let provider = ExactPredicatePartitionIndexV5::rebuild(
        binding,
        slot.registration.query.program().clone(),
        rows,
    )
    .map_err(|error| match error {
        riffdb_projection::ExactPredicateProviderErrorV1::BoundExceeded
        | riffdb_projection::ExactPredicateProviderErrorV1::StateAmplification
        | riffdb_projection::ExactPredicateProviderErrorV1::FuelExhausted => {
            RebuildFailure::Capacity(generation)
        }
        _ => RebuildFailure::Integrity,
    })?;
    persist_checked_nullable_predicate_slot(runtime, slot, &provider)?;
    Ok(Some(Selected::new(provider, captured, candidates)))
}

fn persist_checked_nullable_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &NullableExactPredicateSlot,
    provider: &ExactPredicatePartitionIndexV5,
) -> Result<(), RebuildFailure> {
    persist_predicate_checkpoint_bytes(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider
            .to_checkpoint_bytes()
            .map_err(|_| RebuildFailure::Integrity)?,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    let reopened = read_checkpoint(&slot.checkpoint)?.ok_or(RebuildFailure::Integrity)?;
    let bytes = decode_activation_checkpoint(
        &reopened,
        &slot.registration.key(),
        runtime.history_incarnation,
    )?;
    if ExactPredicatePartitionIndexV5::from_checkpoint_bytes(bytes)
        .map_err(|_| RebuildFailure::Integrity)?
        != *provider
    {
        return Err(RebuildFailure::Integrity);
    }
    Ok(())
}

fn read_complete_predicate_partition<R: ExactPredicateRegistrationView>(
    captured: &CapturedSource,
    registration: &R,
    generation: ProjectionGeneration,
) -> Result<(ExactPredicateSourceRows, BTreeSet<EntityKey>), RebuildFailure> {
    let access_program = registration.access_program();
    let step = access_program
        .steps()
        .first()
        .ok_or(RebuildFailure::Integrity)?;
    if access_program.steps().len() != 1 {
        return Err(RebuildFailure::Integrity);
    }
    let index_id = step.internal_index_id().ok_or(RebuildFailure::Integrity)?;
    let key_schema = step
        .internal_index_key_schema()
        .ok_or(RebuildFailure::Integrity)?;
    let mut prefix = IndexRangePrefixBuilder::new(index_id);
    push_index_component(&mut prefix, registration.partition_value())?;
    let target = IndexRangeTarget::new(registration.partition_key().clone(), prefix.finish());
    let limit = StorageScanLimit::new(REBUILD_PAGE_ROWS).ok_or(RebuildFailure::Integrity)?;
    let access = access_program
        .internal_entity_access(step.entity())
        .ok_or(RebuildFailure::Integrity)?;
    let output_fields = step
        .selected_fields()
        .iter()
        .map(|name| {
            access
                .internal_field_id(name)
                .ok_or(RebuildFailure::Integrity)
        })
        .collect::<Result<BTreeSet<FieldId>, _>>()?;
    let referenced_profiles = registration.referenced_profiles()?;
    let maximum_candidates = usize::try_from(registration.max_candidates())
        .map_err(|_| RebuildFailure::Integrity)?
        .min(MAX_PROJECTED_POLICY_CANDIDATES_V1);
    let mut rows = BTreeMap::new();
    let mut candidates = BTreeSet::new();
    let mut observed_entries = 0_usize;
    let mut after = None;
    loop {
        let request = AuthoritativeIndexScanRequest::new(target.clone(), after, limit)
            .map_err(|_| RebuildFailure::Integrity)?;
        let page = AuthoritativeScanReader::scan_index(&captured.storage, request)
            .map_err(|_| RebuildFailure::Transient)?;
        for item in page.entries() {
            observed_entries = observed_entries
                .checked_add(1)
                .ok_or(RebuildFailure::Capacity(generation))?;
            if observed_entries > maximum_candidates {
                return Err(RebuildFailure::Capacity(generation));
            }
            let decoded = key_schema
                .decode_index(item.value().key())
                .map_err(|_| RebuildFailure::Integrity)?;
            let entity_key = decoded.entity_key().clone();
            if !candidates.insert(entity_key.clone()) {
                return Err(RebuildFailure::Integrity);
            }
            let target = EntityTarget::new(step.internal_entity_id(), entity_key.clone())
                .map_err(|_| RebuildFailure::Integrity)?;
            let record = AuthoritativePointReader::read_entity(&captured.storage, &target)
                .map_err(|_| RebuildFailure::Transient)?
                .ok_or(RebuildFailure::Transient)?;
            let record = captured.materialize(access_program, record)?;
            let values = record
                .fields()
                .fields()
                .iter()
                .cloned()
                .collect::<BTreeMap<_, _>>();
            let fields = referenced_profiles
                .iter()
                .map(|(field, profile)| {
                    exact_reference_cell(values.get(field), *profile).map(|cell| (*field, cell))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let output = CanonicalRecord::new(
                record
                    .fields()
                    .fields()
                    .iter()
                    .filter(|(field, _)| output_fields.contains(field))
                    .cloned()
                    .collect(),
            )
            .map_err(|_| RebuildFailure::Integrity)?;
            if output.len() != output_fields.len()
                || rows
                    .insert(
                        entity_key.clone(),
                        ExactPredicateProviderRowV1::new(entity_key, fields, output),
                    )
                    .is_some()
            {
                return Err(RebuildFailure::Integrity);
            }
        }
        match page {
            AuthoritativeIndexScanPage::Page { next_after, .. } => after = Some(next_after),
            AuthoritativeIndexScanPage::ExactEnd { .. } => break,
        }
    }
    if let Some(policy) = registration.row_policy() {
        let ordered_candidates = candidates.iter().cloned().collect::<Vec<_>>();
        let query_executor = captured.storage.query_executor();
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &query_executor,
            step.internal_entity_id(),
            &ordered_candidates,
            policy,
        )
        .map_err(|error| map_policy_admission_error(error, generation))?;
        if !admission.covers(step.internal_entity_id(), &candidates) {
            return Err(RebuildFailure::Integrity);
        }
        rows.retain(|key, _| admission.admits(key));
    }
    Ok((rows.into_values().collect(), candidates))
}

fn recover_provider(
    registration: &ExactTextRegistration,
    bytes: &[u8],
) -> Result<ExactTextProviderState, riffdb_projection::ExactTextProviderErrorV1> {
    match registration.query.filter() {
        None => ExactTextPartitionIndexV2::from_checkpoint_bytes(bytes)
            .map(Box::new)
            .map(ExactTextProviderState::V2),
        Some(filter) => {
            let provider = ExactTextPartitionIndexV3::from_checkpoint_bytes(bytes)?;
            if provider.filter_field() != filter.internal_field() {
                return Err(riffdb_projection::ExactTextProviderErrorV1::InvalidCheckpoint);
            }
            Ok(ExactTextProviderState::V3(Box::new(provider)))
        }
    }
}

fn read_complete_partition(
    captured: &CapturedSource,
    registration: &ExactTextRegistration,
    generation: ProjectionGeneration,
) -> Result<(ExactTextSourceRows, BTreeSet<EntityKey>), RebuildFailure> {
    let program = registration.query.representative_program();
    let step = program.steps().first().ok_or(RebuildFailure::Integrity)?;
    if program.steps().len() != 1 {
        return Err(RebuildFailure::Integrity);
    }
    let index_id = step.internal_index_id().ok_or(RebuildFailure::Integrity)?;
    let key_schema = step
        .internal_index_key_schema()
        .ok_or(RebuildFailure::Integrity)?;
    let mut prefix = IndexRangePrefixBuilder::new(index_id);
    push_index_component(&mut prefix, &registration.partition_value)?;
    let target = IndexRangeTarget::new(registration.partition_key.clone(), prefix.finish());
    let limit = StorageScanLimit::new(REBUILD_PAGE_ROWS).ok_or(RebuildFailure::Integrity)?;
    let access = program
        .internal_entity_access(step.entity())
        .ok_or(RebuildFailure::Integrity)?;
    let output_fields = step
        .selected_fields()
        .iter()
        .map(|name| {
            access
                .internal_field_id(name)
                .ok_or(RebuildFailure::Integrity)
        })
        .collect::<Result<BTreeSet<FieldId>, _>>()?;
    let text_field = registration.query.binding().family().field();
    let filter_field = registration
        .query
        .filter()
        .map(|filter| filter.internal_field());
    let maximum_candidates =
        usize::try_from(registration.query.binding().family().max_candidates())
            .map_err(|_| RebuildFailure::Integrity)?
            .min(MAX_PROJECTED_POLICY_CANDIDATES_V1);
    let mut rows = BTreeMap::new();
    let mut candidates = BTreeSet::new();
    let mut observed_entries = 0_usize;
    let mut after = None;
    loop {
        let request = AuthoritativeIndexScanRequest::new(target.clone(), after, limit)
            .map_err(|_| RebuildFailure::Integrity)?;
        let page = AuthoritativeScanReader::scan_index(&captured.storage, request)
            .map_err(|_| RebuildFailure::Transient)?;
        for item in page.entries() {
            observed_entries = observed_entries
                .checked_add(1)
                .ok_or(RebuildFailure::Capacity(generation))?;
            if observed_entries > maximum_candidates {
                return Err(RebuildFailure::Capacity(generation));
            }
            let decoded = key_schema
                .decode_index(item.value().key())
                .map_err(|_| RebuildFailure::Integrity)?;
            let entity_key = decoded.entity_key().clone();
            if !candidates.insert(entity_key.clone()) {
                return Err(RebuildFailure::Integrity);
            }
            let target = EntityTarget::new(step.internal_entity_id(), entity_key.clone())
                .map_err(|_| RebuildFailure::Integrity)?;
            let record = AuthoritativePointReader::read_entity(&captured.storage, &target)
                .map_err(|_| RebuildFailure::Transient)?
                .ok_or(RebuildFailure::Transient)?;
            let record = captured.materialize(program, record)?;
            let text = match record
                .fields()
                .fields()
                .iter()
                .find(|(field, _)| *field == text_field)
                .map(|(_, value)| value)
            {
                None | Some(CanonicalValue::Null) => continue,
                Some(CanonicalValue::String(value)) => value.as_str().to_owned(),
                Some(_) => return Err(RebuildFailure::Integrity),
            };
            let output = CanonicalRecord::new(
                record
                    .fields()
                    .fields()
                    .iter()
                    .filter(|(field, _)| output_fields.contains(field))
                    .cloned()
                    .collect(),
            )
            .map_err(|_| RebuildFailure::Integrity)?;
            let filter = filter_field.and_then(|filter_field| {
                record
                    .fields()
                    .fields()
                    .iter()
                    .find(|(field, _)| *field == filter_field)
                    .map(|(_, value)| value.clone())
            });
            if output.len() != output_fields.len()
                || rows.insert(entity_key, (text, filter, output)).is_some()
            {
                return Err(RebuildFailure::Integrity);
            }
        }
        match page {
            AuthoritativeIndexScanPage::Page { next_after, .. } => after = Some(next_after),
            AuthoritativeIndexScanPage::ExactEnd { .. } => break,
        }
    }
    if let Some(policy) = registration.row_policy.as_deref() {
        let ordered_candidates = candidates.iter().cloned().collect::<Vec<_>>();
        let query_executor = captured.storage.query_executor();
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &query_executor,
            step.internal_entity_id(),
            &ordered_candidates,
            policy,
        )
        .map_err(|error| map_policy_admission_error(error, generation))?;
        if !admission.covers(step.internal_entity_id(), &candidates) {
            return Err(RebuildFailure::Integrity);
        }
        rows.retain(|key, _| admission.admits(key));
    }
    Ok((rows, candidates))
}

fn map_policy_admission_error(
    error: QueryExecutionError,
    generation: ProjectionGeneration,
) -> RebuildFailure {
    match error {
        QueryExecutionError::BackendUnavailable => RebuildFailure::Transient,
        QueryExecutionError::BackendLimitExceeded
        | QueryExecutionError::BoundExceeded
        | QueryExecutionError::FuelExhausted => RebuildFailure::Capacity(generation),
        QueryExecutionError::MissingParameter { .. }
        | QueryExecutionError::InvalidParameter { .. }
        | QueryExecutionError::MissingField { .. }
        | QueryExecutionError::InvalidProgram
        | QueryExecutionError::BackendIntegrity
        | QueryExecutionError::AggregateOverflow
        | QueryExecutionError::UnexpectedCardinality { .. }
        | QueryExecutionError::UnsupportedPredicate
        | QueryExecutionError::InvalidDependentKey { .. }
        | QueryExecutionError::StaleCursor
        | QueryExecutionError::InvalidContinuation => RebuildFailure::Integrity,
    }
}

fn predicate_referenced_profiles(
    program: &riffdb_query_ir::ExactPredicateProgramV1,
) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, RebuildFailure> {
    fn insert(
        profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
        field: FieldId,
        profile: ExactComparisonProfileV1,
    ) -> Result<(), RebuildFailure> {
        if profiles
            .insert(field, profile)
            .is_some_and(|prior| prior != profile)
        {
            return Err(RebuildFailure::Integrity);
        }
        Ok(())
    }
    fn visit(
        node: &ExactPredicateNodeV1,
        profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
    ) -> Result<(), RebuildFailure> {
        match node {
            ExactPredicateNodeV1::Leaf(leaf) => insert(profiles, leaf.field(), leaf.profile()),
            ExactPredicateNodeV1::When { child, .. } => visit(child, profiles),
            ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
                for child in children {
                    visit(child, profiles)?;
                }
                Ok(())
            }
        }
    }
    let mut profiles = BTreeMap::new();
    visit(program.predicate(), &mut profiles)?;
    for order in program.orders() {
        for term in order.terms() {
            insert(&mut profiles, term.field(), term.profile())?;
        }
    }
    Ok(profiles)
}

fn nullable_predicate_referenced_profiles(
    program: &riffdb_query_ir::ExactPredicateProgramV2,
) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, RebuildFailure> {
    fn insert(
        profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
        field: FieldId,
        profile: ExactComparisonProfileV1,
    ) -> Result<(), RebuildFailure> {
        if profiles
            .insert(field, profile)
            .is_some_and(|prior| prior != profile)
        {
            return Err(RebuildFailure::Integrity);
        }
        Ok(())
    }
    fn visit(
        node: &ExactPredicateNodeV1,
        profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
    ) -> Result<(), RebuildFailure> {
        match node {
            ExactPredicateNodeV1::Leaf(leaf) => insert(profiles, leaf.field(), leaf.profile()),
            ExactPredicateNodeV1::When { child, .. } => visit(child, profiles),
            ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
                for child in children {
                    visit(child, profiles)?;
                }
                Ok(())
            }
        }
    }
    let mut profiles = BTreeMap::new();
    visit(program.predicate(), &mut profiles)?;
    for order in program.orders() {
        for term in order.terms() {
            insert(&mut profiles, term.field(), term.profile())?;
        }
    }
    Ok(profiles)
}

fn exact_reference_cell(
    value: Option<&CanonicalValue>,
    profile: ExactComparisonProfileV1,
) -> Result<ExactReferenceCellV1, RebuildFailure> {
    let Some(value) = value else {
        return Ok(ExactReferenceCellV1::Missing);
    };
    if matches!(value, CanonicalValue::Null) {
        return Ok(ExactReferenceCellV1::Null);
    }
    let scalar = match value {
        CanonicalValue::Bool(value) => ExactScalarV1::Bool(*value),
        CanonicalValue::I64(value) => ExactScalarV1::I64(*value),
        CanonicalValue::U64(value) => ExactScalarV1::U64(*value),
        CanonicalValue::Decimal(value) => ExactScalarV1::Decimal {
            coefficient: value.coefficient(),
            scale: value.spec().scale(),
        },
        CanonicalValue::Money(value) => ExactScalarV1::Money {
            currency: *value.currency().as_bytes(),
            coefficient: value.amount().coefficient(),
            scale: value.amount().spec().scale(),
        },
        CanonicalValue::String(value) => ExactScalarV1::String(value.as_str().to_owned()),
        CanonicalValue::Bytes(value) => ExactScalarV1::Bytes(value.as_bytes().to_vec()),
        CanonicalValue::Timestamp(value) => {
            let nanos = i128::from(value.seconds())
                .checked_mul(1_000_000_000)
                .and_then(|seconds| seconds.checked_add(i128::from(value.nanoseconds())))
                .ok_or(RebuildFailure::Integrity)?;
            ExactScalarV1::Timestamp(nanos)
        }
        CanonicalValue::Date(value) => ExactScalarV1::Date(value.days_since_unix_epoch()),
        CanonicalValue::Uuid(value) => ExactScalarV1::Uuid(*value),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => ExactScalarV1::Enum {
            type_id: type_id.get(),
            variant_id: variant_id.get(),
        },
        CanonicalValue::Null
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_)
        | CanonicalValue::Vector(_) => return Err(RebuildFailure::Integrity),
    };
    if scalar.profile() != profile {
        return Err(RebuildFailure::Integrity);
    }
    Ok(ExactReferenceCellV1::Value(scalar))
}

fn push_index_component(
    builder: &mut IndexRangePrefixBuilder,
    value: &CanonicalValue,
) -> Result<(), RebuildFailure> {
    let result = match value {
        CanonicalValue::Bool(value) => builder.push_bool(*value),
        CanonicalValue::I64(value) => builder.push_i64(*value),
        CanonicalValue::U64(value) => builder.push_u64(*value),
        CanonicalValue::String(value) => builder.push_str(value.as_str()),
        CanonicalValue::Bytes(value) => builder.push_bytes(value.as_bytes()),
        CanonicalValue::Timestamp(value) => builder.push_timestamp(*value),
        CanonicalValue::Date(value) => builder.push_date(*value),
        CanonicalValue::Uuid(value) => builder.push_uuid(value),
        CanonicalValue::Enum { variant_id, .. } => builder.push_enum_variant(*variant_id),
        CanonicalValue::Null
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_)
        | CanonicalValue::Vector(_) => return Err(RebuildFailure::Integrity),
    };
    result.map(|_| ()).map_err(|_| RebuildFailure::Integrity)
}

fn persist_checkpoint(
    path: &Path,
    slot_key: &[u8],
    history_incarnation: u64,
    provider: &ExactTextProviderState,
) -> Result<(), std::io::Error> {
    let pending = path.with_extension("pending");
    let provider_bytes = provider
        .checkpoint_bytes()
        .map_err(|_| std::io::Error::other("exact checkpoint integrity"))?;
    let bytes = encode_activation_checkpoint(slot_key, history_incarnation, &provider_bytes)
        .map_err(|_| std::io::Error::other("exact checkpoint integrity"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)?;
    file.write_all(&bytes)?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::BeforeFileSync, path, None);
    file.sync_all()?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::AfterFileSync, path, None);
    fs::rename(&pending, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("checkpoint parent"))?,
    )?
    .sync_all()
}

fn encode_long_pattern_release(
    key: &EntityKey,
    fields: &CanonicalRecord,
) -> Result<Vec<u8>, RebuildFailure> {
    let encoded = encode_canonical_value(&CanonicalValue::Record(fields.clone()))
        .map_err(|_| RebuildFailure::Integrity)?;
    let key_len = u32::try_from(key.as_bytes().len()).map_err(|_| RebuildFailure::Integrity)?;
    let value_len = u32::try_from(encoded.len()).map_err(|_| RebuildFailure::Integrity)?;
    let mut output = Vec::with_capacity(8 + key.as_bytes().len() + encoded.len());
    output.extend_from_slice(&key_len.to_be_bytes());
    output.extend_from_slice(key.as_bytes());
    output.extend_from_slice(&value_len.to_be_bytes());
    output.extend_from_slice(&encoded);
    Ok(output)
}

fn decode_long_pattern_release(
    registration: &LongPatternRegistration,
    expected_hash: riffdb_types::EntityKeyHash,
    release: &[u8],
) -> Result<QueryRow, ExactTextProjectionPortError> {
    let key_len = release
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_be_bytes)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    let key_end = 4_usize
        .checked_add(key_len)
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    let key = EntityKey::from_bytes(
        release
            .get(4..key_end)
            .ok_or(ExactTextProjectionPortError::Integrity)?
            .to_vec(),
    )
    .map_err(|_| ExactTextProjectionPortError::Integrity)?;
    if key.entity_type_id() != registration.step.internal_entity_id()
        || hash_entity_key(key.as_bytes()) != expected_hash
    {
        return Err(ExactTextProjectionPortError::Integrity);
    }
    let length_end = key_end
        .checked_add(4)
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    let value_len = release
        .get(key_end..length_end)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_be_bytes)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    let value_end = length_end
        .checked_add(value_len)
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    if value_end != release.len() {
        return Err(ExactTextProjectionPortError::Integrity);
    }
    let CanonicalValue::Record(record) = decode_canonical_value(
        release
            .get(length_end..value_end)
            .ok_or(ExactTextProjectionPortError::Integrity)?,
    )
    .map_err(|_| ExactTextProjectionPortError::Integrity)?
    else {
        return Err(ExactTextProjectionPortError::Integrity);
    };
    let access = registration
        .program
        .internal_entity_access(registration.step.entity())
        .ok_or(ExactTextProjectionPortError::Integrity)?;
    let mut fields = BTreeMap::new();
    for (name, field) in access.internal_fields() {
        if let Ok(index) = record
            .fields()
            .binary_search_by_key(&field, |(field, _)| *field)
        {
            fields.insert(name.to_owned(), record.fields()[index].1.clone());
        }
    }
    QueryRow::checked(registration.step.entity().to_owned(), fields)
        .ok_or(ExactTextProjectionPortError::Integrity)
}

const LONG_PATTERN_EPOCH_MAGIC: &[u8; 4] = b"RLPE";

fn encode_long_pattern_checkpoint(
    provider: &LongPatternProviderState,
) -> Result<Vec<u8>, RebuildFailure> {
    let state = provider
        .provider
        .checkpoint_bytes()
        .map_err(|_| RebuildFailure::Integrity)?;
    let state_len = u32::try_from(state.len()).map_err(|_| RebuildFailure::Integrity)?;
    let mut output = Vec::with_capacity(24 + state.len());
    output.extend_from_slice(LONG_PATTERN_EPOCH_MAGIC);
    output.extend_from_slice(&1_u16.to_be_bytes());
    output.extend_from_slice(&0_u16.to_be_bytes());
    output.extend_from_slice(&provider.generation.to_be_bytes());
    output.extend_from_slice(&provider.frontier.to_be_bytes());
    output.extend_from_slice(&state_len.to_be_bytes());
    output.extend_from_slice(&state);
    Ok(output)
}

fn decode_long_pattern_checkpoint(
    bytes: &[u8],
) -> Result<LongPatternProviderState, RebuildFailure> {
    if bytes.len() < 28
        || &bytes[..4] != LONG_PATTERN_EPOCH_MAGIC
        || bytes[4..6] != 1_u16.to_be_bytes()
        || bytes[6..8] != [0, 0]
    {
        return Err(RebuildFailure::Integrity);
    }
    let generation = ProjectionGeneration::new(u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| RebuildFailure::Integrity)?,
    ))
    .ok_or(RebuildFailure::Integrity)?;
    let frontier = CommitSequence::new(u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| RebuildFailure::Integrity)?,
    ))
    .ok_or(RebuildFailure::Integrity)?;
    let state_len = usize::try_from(u32::from_be_bytes(
        bytes[24..28]
            .try_into()
            .map_err(|_| RebuildFailure::Integrity)?,
    ))
    .map_err(|_| RebuildFailure::Integrity)?;
    if 28_usize.checked_add(state_len) != Some(bytes.len()) {
        return Err(RebuildFailure::Integrity);
    }
    let provider = LongPatternPartitionV1::from_checkpoint_bytes(&bytes[28..])
        .map_err(|_| RebuildFailure::Integrity)?;
    Ok(LongPatternProviderState {
        provider,
        generation,
        frontier,
    })
}

fn persist_predicate_checkpoint(
    path: &Path,
    slot_key: &[u8],
    history_incarnation: u64,
    provider: &ExactPredicatePartitionIndexV4,
) -> Result<(), std::io::Error> {
    let provider_bytes = provider
        .to_checkpoint_bytes()
        .map_err(|_| std::io::Error::other("exact predicate checkpoint integrity"))?;
    persist_predicate_checkpoint_bytes(path, slot_key, history_incarnation, &provider_bytes)
}

fn persist_predicate_checkpoint_bytes(
    path: &Path,
    slot_key: &[u8],
    history_incarnation: u64,
    provider_bytes: &[u8],
) -> Result<(), std::io::Error> {
    let pending = path.with_extension("pending");
    if provider_bytes.len() > MAX_PROVIDER_CHECKPOINT_BYTES {
        return Err(std::io::Error::other("exact predicate checkpoint capacity"));
    }
    let bytes = encode_activation_checkpoint(slot_key, history_incarnation, provider_bytes)
        .map_err(|_| std::io::Error::other("exact predicate checkpoint integrity"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)?;
    file.write_all(&bytes)?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::BeforeFileSync, path, None);
    file.sync_all()?;
    #[cfg(feature = "test-fixtures")]
    observe_test_point(TestPoint::AfterFileSync, path, None);
    fs::rename(&pending, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("checkpoint parent"))?,
    )?
    .sync_all()
}

fn persist_tokenized_checkpoint(
    path: &Path,
    slot_key: &[u8],
    history_incarnation: u64,
    provider: &TokenizedTextPartitionIndexV1,
) -> Result<(), std::io::Error> {
    let provider_bytes = provider
        .to_checkpoint_bytes()
        .map_err(|_| std::io::Error::other("tokenized checkpoint integrity"))?;
    persist_predicate_checkpoint_bytes(path, slot_key, history_incarnation, &provider_bytes)
}

fn read_checkpoint(path: &Path) -> Result<Option<Vec<u8>>, RebuildFailure> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RebuildFailure::Transient),
    };
    let maximum =
        u64::try_from(MAX_ACTIVATION_CHECKPOINT_BYTES).map_err(|_| RebuildFailure::Integrity)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| RebuildFailure::Transient)?;
    if bytes.len() > MAX_ACTIVATION_CHECKPOINT_BYTES {
        return Err(RebuildFailure::Integrity);
    }
    Ok(Some(bytes))
}

fn encode_activation_checkpoint(
    slot_key: &[u8],
    history_incarnation: u64,
    provider: &[u8],
) -> Result<Vec<u8>, RebuildFailure> {
    if slot_key.len() != SLOT_KEY_BYTES || provider.len() > MAX_PROVIDER_CHECKPOINT_BYTES {
        return Err(RebuildFailure::Integrity);
    }
    let provider_length = u32::try_from(provider.len()).map_err(|_| RebuildFailure::Integrity)?;
    let mut bytes =
        Vec::with_capacity(CHECKPOINT_HEADER_BYTES + provider.len() + CHECKPOINT_DIGEST_BYTES);
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&CHECKPOINT_FORMAT_V1.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&history_incarnation.to_be_bytes());
    bytes.extend_from_slice(slot_key);
    bytes.extend_from_slice(&provider_length.to_be_bytes());
    bytes.extend_from_slice(provider);
    let digest = hash(HashDomain::ExactResultCheckpoint, &bytes);
    bytes.extend_from_slice(digest.as_bytes());
    Ok(bytes)
}

fn decode_activation_checkpoint<'a>(
    bytes: &'a [u8],
    expected_slot_key: &[u8],
    expected_history_incarnation: u64,
) -> Result<&'a [u8], RebuildFailure> {
    if expected_slot_key.len() != SLOT_KEY_BYTES
        || bytes.len() < CHECKPOINT_HEADER_BYTES + CHECKPOINT_DIGEST_BYTES
        || bytes.len() > MAX_ACTIVATION_CHECKPOINT_BYTES
        || &bytes[..4] != CHECKPOINT_MAGIC
        || u16::from_be_bytes([bytes[4], bytes[5]]) != CHECKPOINT_FORMAT_V1
        || bytes[6..8] != [0, 0]
        || u64::from_be_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| RebuildFailure::Integrity)?,
        ) != expected_history_incarnation
        || &bytes[CHECKPOINT_SLOT_OFFSET..CHECKPOINT_LENGTH_OFFSET] != expected_slot_key
    {
        return Err(RebuildFailure::Integrity);
    }
    let provider_length = usize::try_from(u32::from_be_bytes(
        bytes[CHECKPOINT_LENGTH_OFFSET..CHECKPOINT_HEADER_BYTES]
            .try_into()
            .map_err(|_| RebuildFailure::Integrity)?,
    ))
    .map_err(|_| RebuildFailure::Integrity)?;
    let provider_end = CHECKPOINT_HEADER_BYTES
        .checked_add(provider_length)
        .ok_or(RebuildFailure::Integrity)?;
    let digest_end = provider_end
        .checked_add(CHECKPOINT_DIGEST_BYTES)
        .ok_or(RebuildFailure::Integrity)?;
    if provider_length > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 || digest_end != bytes.len() {
        return Err(RebuildFailure::Integrity);
    }
    let expected = hash(HashDomain::ExactResultCheckpoint, &bytes[..provider_end]);
    if bytes[provider_end..] != expected.as_bytes()[..] {
        return Err(RebuildFailure::Integrity);
    }
    Ok(&bytes[CHECKPOINT_HEADER_BYTES..provider_end])
}

fn slot_key(
    plan: riffdb_types::QueryPlanHash,
    partition: PartitionKeyHash,
    policy: riffdb_types::ApplicationRoleHash,
    row_policy: Option<(CapabilityId, NonZeroU64)>,
) -> SlotKey {
    let mut key = Vec::with_capacity(SLOT_KEY_BYTES);
    key.extend_from_slice(plan.as_bytes());
    key.extend_from_slice(partition.as_bytes());
    key.extend_from_slice(policy.as_bytes());
    match row_policy {
        Some((capability, revision)) => {
            key.push(1);
            key.extend_from_slice(capability.as_bytes());
            key.extend_from_slice(&revision.get().to_be_bytes());
        }
        None => {
            key.push(0);
            key.extend_from_slice(&[0; 16]);
            key.extend_from_slice(&[0; 8]);
        }
    }
    debug_assert_eq!(key.len(), SLOT_KEY_BYTES);
    key
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn stop_requested(stop: &StopState) -> bool {
    stop.requested.lock().map_or(true, |requested| *requested)
}

fn wait_for_stop(stop: &StopState, timeout: Duration) -> bool {
    let Ok(requested) = stop.requested.lock() else {
        return true;
    };
    if *requested {
        return true;
    }
    stop.changed
        .wait_timeout(requested, timeout)
        .map_or(true, |(requested, _)| *requested)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebuildFailure {
    Transient,
    Capacity(ProjectionGeneration),
    Integrity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactTextRuntimeOpenError;

impl fmt::Display for ExactTextRuntimeOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exact text runtime could not open")
    }
}

impl Error for ExactTextRuntimeOpenError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactTextWorkerStartError;

impl fmt::Display for ExactTextWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exact text worker could not start")
    }
}

impl Error for ExactTextWorkerStartError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactTextWorkerShutdownError;

impl fmt::Display for ExactTextWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exact text worker did not shut down cleanly")
    }
}

impl Error for ExactTextWorkerShutdownError {}

#[cfg(test)]
mod tests {
    use super::provider_policy_binding_is_exact;
    use riffdb_types::ProjectionProviderPolicyModeV1;

    #[test]
    fn provider_policy_binding_requires_the_compiled_mode() {
        assert!(provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            false
        ));
        assert!(!provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            true
        ));
        assert!(provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            true
        ));
        assert!(!provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            false
        ));
        assert!(!provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::PolicySubpartition,
            false
        ));
        assert!(!provider_policy_binding_is_exact(
            ProjectionProviderPolicyModeV1::PolicySubpartition,
            true
        ));
    }

    use super::*;
    use riffdb_types::{ApplicationRoleHash, PartitionKeyBuilder, QueryPlanHash};

    const SOURCE: &str = include_str!("exact_text_adapter.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]\nmod tests")
            .expect("test boundary")
            .0
    }

    #[test]
    fn slot_identity_binds_plan_partition_policy_shape_and_capability_revision() {
        let mut partition = PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
        partition.push_uuid(&[0x22; 16]).expect("partition");
        let partition = partition.finish().expect("partition key");
        let plan = QueryPlanHash::from_bytes([0x11; 32]);
        let policy = ApplicationRoleHash::from_bytes([0x33; 32]);
        let key = slot_key(plan, hash_partition_key(partition.as_bytes()), policy, None);

        assert_eq!(key.len(), SLOT_KEY_BYTES);
        assert_eq!(&key[..32], plan.as_bytes());
        assert_eq!(
            &key[32..64],
            hash_partition_key(partition.as_bytes()).as_bytes()
        );
        assert_eq!(&key[64..96], policy.as_bytes());
        assert_eq!(key[96], 0);

        let capability =
            CapabilityId::from_unix_milliseconds_and_random(7, [0x44; 10]).expect("capability");
        let protected = slot_key(
            plan,
            hash_partition_key(partition.as_bytes()),
            policy,
            Some((capability, NonZeroU64::new(3).expect("revision"))),
        );
        let revised = slot_key(
            plan,
            hash_partition_key(partition.as_bytes()),
            policy,
            Some((capability, NonZeroU64::new(4).expect("revision"))),
        );
        assert_ne!(key, protected);
        assert_ne!(protected, revised);
        assert_eq!(protected[96], 1);
        assert_eq!(&protected[97..113], capability.as_bytes());
    }

    #[test]
    fn public_execute_path_has_no_authoritative_scan_or_point_hydration() {
        let execute = production_source()
            .split_once("impl ExactTextProjectionPort for ExactTextRuntime")
            .expect("port implementation")
            .1
            .split_once("impl fmt::Debug for ExactTextRuntime")
            .expect("execute boundary")
            .0;
        for forbidden in ["scan_index", "read_entity", "read_complete_partition"] {
            assert!(
                !execute.contains(forbidden),
                "query execution must not contain {forbidden}"
            );
        }
        assert!(execute.contains("execute_exact_text_result_set_v1"));
        assert!(execute.contains("execute_exact_predicate_result_set_v1"));
        assert!(execute.contains("execute_nullable_exact_predicate_result_set_v1"));
        assert!(execute.contains("policy_binding_is_exact"));
        assert!(execute.contains("request.row_policy().is_some()"));
    }

    #[test]
    fn tokenized_execute_path_uses_only_retained_posting_epochs() {
        let execute = production_source()
            .split_once("impl TokenizedTextProjectionPort for ExactTextRuntime")
            .expect("tokenized port implementation")
            .1
            .split_once("impl ExactPredicateProjectionPort for ExactTextRuntime")
            .expect("tokenized execute boundary")
            .0;
        for forbidden in [
            "capture_application_export_snapshot",
            "read_application_export_entity_page",
            "scan_index",
            "read_entity",
            "read_complete_tokenized_partition",
        ] {
            assert!(
                !execute.contains(forbidden),
                "tokenized request execution must not contain {forbidden}"
            );
        }
        assert!(execute.contains("execute_tokenized_text_v1"));
        assert!(execute.contains("request.pinned_snapshot()"));
        assert!(execute.contains("SnapshotRetired"));
        assert!(execute.contains("policy_binding_is_exact"));
    }

    #[test]
    fn long_pattern_execute_path_uses_only_retained_provider_epochs() {
        let execute = production_source()
            .split_once("impl LongPatternProjectionPort for ExactTextRuntime")
            .expect("long-pattern port implementation")
            .1
            .split_once("const fn map_long_pattern_error")
            .expect("long-pattern execute boundary")
            .0;
        for forbidden in [
            "scan_entity_partition",
            "scan_index",
            "read_entity",
            "read_complete_partition",
        ] {
            assert!(
                !execute.contains(forbidden),
                "long-pattern request execution must not contain {forbidden}"
            );
        }
        assert!(execute.contains("query_observed"));
        assert!(execute.contains("request.pinned_epoch()"));
        assert!(execute.contains("SnapshotRetired"));
        assert!(execute.contains("provider_policy_binding_is_exact"));
    }

    #[test]
    // req: REP-002, REP-003
    fn exact_provider_runtime_requires_only_snapshot_authority() {
        let source = production_source();
        assert!(source.contains("storage: ProjectionReadSource"));
        for forbidden in [
            "SharedRedbOperationalPorts",
            "ApplicationExportSnapshotPort",
            "read_application_export_entity_page",
        ] {
            assert!(!source.contains(forbidden), "provider holds {forbidden}");
        }
        let rebuild = source
            .split_once("fn read_complete_tokenized_partition")
            .expect("tokenized rebuild")
            .1
            .split_once("fn rebuild_nullable_predicate_slot")
            .expect("next rebuild")
            .0;
        let compact = rebuild.split_whitespace().collect::<String>();
        assert!(compact.contains("runtime.storage.pin()"));
        assert!(rebuild.contains("scan_entity_partition(&snapshot"));
    }

    #[test]
    fn worker_is_the_only_owner_of_rebuild_scans() {
        let worker = production_source()
            .split_once("fn refresh_registered_slots")
            .expect("worker")
            .1;
        assert_eq!(
            worker
                .matches("AuthoritativeScanReader::scan_index")
                .count(),
            2
        );
        assert_eq!(
            production_source()
                .matches("AuthoritativeScanReader::scan_index")
                .count(),
            2
        );
        assert_eq!(
            production_source()
                .matches("AuthoritativeScanReader::scan_entity_partition")
                .count(),
            2
        );
    }

    #[test]
    fn bounded_policy_admission_precedes_exact_measure_and_ordinal_state() {
        let read_partition = production_source()
            .split_once("fn read_complete_partition")
            .expect("partition reader")
            .1
            .split_once("fn push_index_component")
            .expect("partition reader boundary")
            .0;
        let admission = read_partition
            .find("authorize_projected_candidates")
            .expect("opaque policy admission");
        let retain = read_partition
            .find("rows.retain")
            .expect("deny rows before provider state");
        assert!(admission < retain);
        assert!(read_partition.contains("observed_entries > maximum_candidates"));

        let rebuild = production_source()
            .split_once("fn rebuild_slot")
            .expect("rebuild")
            .1
            .split_once("fn read_complete_partition")
            .expect("rebuild boundary")
            .0;
        assert!(
            rebuild.find("read_complete_partition").expect("admission")
                < rebuild
                    .find("ExactTextPartitionIndexV2::rebuild")
                    .expect("provider build")
        );

        let predicate_reader = production_source()
            .split_once("fn read_complete_predicate_partition")
            .expect("predicate partition reader")
            .1
            .split_once("fn recover_provider")
            .expect("predicate reader boundary")
            .0;
        let admission = predicate_reader
            .find("authorize_projected_candidates")
            .expect("predicate policy admission");
        let retain = predicate_reader
            .find("rows.retain")
            .expect("predicate deny before provider build");
        assert!(admission < retain);
        assert!(predicate_reader.contains("observed_entries > maximum_candidates"));

        let predicate_rebuild = production_source()
            .split_once("fn rebuild_predicate_slot")
            .expect("predicate rebuild")
            .1
            .split_once("fn read_complete_predicate_partition")
            .expect("predicate rebuild boundary")
            .0;
        assert!(
            predicate_rebuild
                .find("read_complete_predicate_partition")
                .expect("predicate admission")
                < predicate_rebuild
                    .find("ExactPredicatePartitionIndexV4::rebuild")
                    .expect("predicate provider build")
        );
        assert!(
            predicate_rebuild
                .find("read_complete_predicate_partition")
                .expect("nullable predicate admission")
                < predicate_rebuild
                    .find("ExactPredicatePartitionIndexV5::rebuild")
                    .expect("nullable predicate provider build")
        );
    }

    #[test]
    fn activation_checkpoint_binds_slot_history_and_every_provider_byte() {
        let slot = vec![0x41; SLOT_KEY_BYTES];
        let provider = b"canonical provider state";
        let bytes = encode_activation_checkpoint(&slot, 7, provider).expect("checkpoint");
        assert_eq!(
            decode_activation_checkpoint(&bytes, &slot, 7).expect("decode"),
            provider
        );

        let mut wrong_slot = slot.clone();
        wrong_slot[0] ^= 1;
        assert_eq!(
            decode_activation_checkpoint(&bytes, &wrong_slot, 7),
            Err(RebuildFailure::Integrity)
        );
        assert_eq!(
            decode_activation_checkpoint(&bytes, &slot, 8),
            Err(RebuildFailure::Integrity)
        );

        let mut corrupt = bytes;
        corrupt[CHECKPOINT_HEADER_BYTES] ^= 1;
        assert_eq!(
            decode_activation_checkpoint(&corrupt, &slot, 7),
            Err(RebuildFailure::Integrity)
        );
    }

    #[test]
    fn activation_checkpoint_rejects_truncation_trailing_bytes_and_wrong_format() {
        let slot = vec![0x52; SLOT_KEY_BYTES];
        let bytes = encode_activation_checkpoint(&slot, 11, b"state").expect("checkpoint");
        assert_eq!(
            decode_activation_checkpoint(&bytes[..bytes.len() - 1], &slot, 11),
            Err(RebuildFailure::Integrity)
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            decode_activation_checkpoint(&trailing, &slot, 11),
            Err(RebuildFailure::Integrity)
        );
        let mut unknown = bytes;
        unknown[5] = 2;
        assert_eq!(
            decode_activation_checkpoint(&unknown, &slot, 11),
            Err(RebuildFailure::Integrity)
        );
    }
}
