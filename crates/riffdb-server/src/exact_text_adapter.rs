//! Background-owned exact text result-set provider for generated RiffQL.

use std::collections::{BTreeMap, BTreeSet};
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
    ExactTextPartitionIndexV2, ExactTextPartitionIndexV3, MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4,
    ProviderEpochObservationV1, ProviderLifecycleV1, ResultSetEpochContextV1,
    ResultSetEpochRequirementV1, negotiate_result_set_epoch_v1,
};
use riffdb_query_executor::{
    ExactTextResultSetV1, QueryExecutionError, QueryExecutionPort,
    execute_exact_predicate_result_set_v1, execute_exact_text_filtered_result_set_v1,
    execute_exact_text_result_set_v1, execute_nullable_exact_predicate_result_set_v1,
};
use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactPredicateNodeV1, ExactReferenceCellV1, ExactScalarV1,
};
use riffdb_service::{
    ExactPredicateProjectionPort, ExactPredicateProjectionRequest, ExactPredicateProjectionResult,
    ExactTextProjectionPort, ExactTextProjectionPortError, ExactTextProjectionRequest,
    ExactTextProjectionResult, ExactTextProjectionRow, NullableExactPredicateProjectionRequest,
};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, EntityTarget, IndexRangePrefixBuilder, IndexRangeTarget,
    StorageScanLimit,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CapabilityId, CommitSequence,
    EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5, ExactTextProfileV1, FieldId, FrontierPosition,
    HashDomain, PartitionKey, PartitionKeyHash, ProjectionGeneration,
    ProjectionProviderPolicyModeV1, hash, hash_partition_key,
};

use crate::columnar_adapter::read_application_head;
use crate::storage::SharedRedbOperationalPorts;

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
    CHECKPOINT_HEADER_BYTES + MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 + CHECKPOINT_DIGEST_BYTES;

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

enum ExactTextSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(ExactTextProviderState),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

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

enum ExactPredicateSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(Box<ExactPredicatePartitionIndexV4>),
    Unavailable {
        observed_head: CommitSequence,
        prior_generation: ProjectionGeneration,
    },
    IntegrityFailure,
}

enum NullableExactPredicateSlotState {
    Building,
    Rebuilding(ProjectionGeneration),
    Ready(Box<ExactPredicatePartitionIndexV5>),
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

impl ExactTextSlot {
    fn new(registration: ExactTextRegistration, checkpoint: PathBuf) -> Self {
        Self {
            registration,
            checkpoint,
            state: Mutex::new(ExactTextSlotState::Building),
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
    storage: SharedRedbOperationalPorts,
    root: PathBuf,
    predicate_root: PathBuf,
    history_incarnation: u64,
    initial_generation: ProjectionGeneration,
    slots: Mutex<BTreeMap<SlotKey, Arc<ExactTextSlot>>>,
    predicate_slots: Mutex<BTreeMap<SlotKey, Arc<ExactPredicateSlot>>>,
    nullable_predicate_slots: Mutex<BTreeMap<SlotKey, Arc<NullableExactPredicateSlot>>>,
}

impl ExactTextRuntime {
    pub(crate) fn open(
        storage: SharedRedbOperationalPorts,
        projections_root: &Path,
        history_incarnation: u64,
        initial_generation: ProjectionGeneration,
    ) -> Result<Arc<Self>, ExactTextRuntimeOpenError> {
        let root = projections_root.join("exact-text-v2");
        fs::create_dir_all(&root).map_err(|_| ExactTextRuntimeOpenError)?;
        let predicate_root = projections_root.join("exact-predicate-v4");
        fs::create_dir_all(&predicate_root).map_err(|_| ExactTextRuntimeOpenError)?;
        Ok(Arc::new(Self {
            storage,
            root,
            predicate_root,
            history_incarnation,
            initial_generation,
            slots: Mutex::new(BTreeMap::new()),
            predicate_slots: Mutex::new(BTreeMap::new()),
            nullable_predicate_slots: Mutex::new(BTreeMap::new()),
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
}

impl ExactTextProjectionPort for ExactTextRuntime {
    fn execute(
        &self,
        request: ExactTextProjectionRequest,
    ) -> Result<ExactTextProjectionResult, ExactTextProjectionPortError> {
        let policy_binding_is_exact =
            match request.query().binding().plan().provider().policy_mode() {
                ProjectionProviderPolicyModeV1::PartitionAligned => request.row_policy().is_none(),
                ProjectionProviderPolicyModeV1::BoundedRowAdmission => {
                    request.row_policy().is_some()
                }
                ProjectionProviderPolicyModeV1::PolicySubpartition => false,
            };
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
        let result: ExactTextResultSetV1 = match (request.query().filter(), provider) {
            (None, ExactTextProviderState::V2(provider)) if request.filter_value().is_none() => {
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
        let policy_binding_is_exact = match policy_mode {
            ProjectionProviderPolicyModeV1::PartitionAligned => request.row_policy().is_none(),
            ProjectionProviderPolicyModeV1::BoundedRowAdmission => request.row_policy().is_some(),
            ProjectionProviderPolicyModeV1::PolicySubpartition => false,
        };
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
        let policy_binding_is_exact = match request
            .query()
            .program()
            .provider_requirement()
            .policy_mode()
        {
            ProjectionProviderPolicyModeV1::PartitionAligned => request.row_policy().is_none(),
            ProjectionProviderPolicyModeV1::BoundedRowAdmission => request.row_policy().is_some(),
            ProjectionProviderPolicyModeV1::PolicySubpartition => false,
        };
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
        let prior_generation = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                ExactTextSlotState::Ready(provider) if provider.frontier() == Some(head) => {
                    continue;
                }
                ExactTextSlotState::Ready(provider) => {
                    let generation = provider.generation();
                    *state = ExactTextSlotState::Rebuilding(generation);
                    Some(generation)
                }
                ExactTextSlotState::Rebuilding(generation) => Some(*generation),
                ExactTextSlotState::Building => None,
                ExactTextSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = ExactTextSlotState::Rebuilding(generation);
                    Some(generation)
                }
                ExactTextSlotState::IntegrityFailure => continue,
            }
        };
        match rebuild_slot(runtime, &slot, head, prior_generation) {
            Ok(Some(provider)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = ExactTextSlotState::Ready(provider);
            }
            Ok(None) => {}
            Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = ExactTextSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = ExactTextSlotState::IntegrityFailure;
            }
        }
    }
    refresh_registered_predicate_slots(runtime)?;
    refresh_registered_nullable_predicate_slots(runtime)?;
    Ok(())
}

fn refresh_registered_predicate_slots(runtime: &ExactTextRuntime) -> Result<(), ()> {
    let slots = runtime.registered_predicate_slots().map_err(|_| ())?;
    for slot in slots {
        let head = read_application_head(&runtime.storage).map_err(|_| ())?;
        let FrontierPosition::AppliedThrough(head) = head else {
            continue;
        };
        let prior_generation = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                ExactPredicateSlotState::Ready(provider)
                    if provider.binding().frontier() == head =>
                {
                    continue;
                }
                ExactPredicateSlotState::Ready(provider) => {
                    let generation = provider.binding().generation();
                    *state = ExactPredicateSlotState::Rebuilding(generation);
                    Some(generation)
                }
                ExactPredicateSlotState::Rebuilding(generation) => Some(*generation),
                ExactPredicateSlotState::Building => None,
                ExactPredicateSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = ExactPredicateSlotState::Rebuilding(generation);
                    Some(generation)
                }
                ExactPredicateSlotState::IntegrityFailure => continue,
            }
        };
        match rebuild_predicate_slot(runtime, &slot, head, prior_generation) {
            Ok(Some(provider)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = ExactPredicateSlotState::Ready(Box::new(provider));
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = ExactPredicateSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
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
        let prior_generation = {
            let mut state = slot.state.lock().map_err(|_| ())?;
            match &*state {
                NullableExactPredicateSlotState::Ready(provider)
                    if provider.binding().frontier() == head =>
                {
                    continue;
                }
                NullableExactPredicateSlotState::Ready(provider) => {
                    let generation = provider.binding().generation();
                    *state = NullableExactPredicateSlotState::Rebuilding(generation);
                    Some(generation)
                }
                NullableExactPredicateSlotState::Rebuilding(generation) => Some(*generation),
                NullableExactPredicateSlotState::Building => None,
                NullableExactPredicateSlotState::Unavailable {
                    observed_head,
                    prior_generation,
                } => {
                    if *observed_head == head {
                        continue;
                    }
                    let generation = *prior_generation;
                    *state = NullableExactPredicateSlotState::Rebuilding(generation);
                    Some(generation)
                }
                NullableExactPredicateSlotState::IntegrityFailure => continue,
            }
        };
        match rebuild_nullable_predicate_slot(runtime, &slot, head, prior_generation) {
            Ok(Some(provider)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = NullableExactPredicateSlotState::Ready(Box::new(provider));
            }
            Ok(None) | Err(RebuildFailure::Transient) => {}
            Err(RebuildFailure::Capacity(prior_generation)) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
                *state = NullableExactPredicateSlotState::Unavailable {
                    observed_head: head,
                    prior_generation,
                };
            }
            Err(RebuildFailure::Integrity) => {
                let mut state = slot.state.lock().map_err(|_| ())?;
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
) -> Result<Option<ExactTextProviderState>, RebuildFailure> {
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
        if recovered.frontier() == Some(head) {
            return Ok(Some(recovered));
        }
        return rebuild_slot(runtime, slot, head, Some(recovered.generation()));
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let rows = read_complete_partition(runtime, &slot.registration, head, generation)?;
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
    persist_checkpoint(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    Ok(Some(provider))
}

fn rebuild_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &ExactPredicateSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<ExactPredicatePartitionIndexV4>, RebuildFailure> {
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
        if recovered.binding().frontier() == head {
            return Ok(Some(recovered));
        }
        return rebuild_predicate_slot(runtime, slot, head, Some(recovered.binding().generation()));
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let rows = read_complete_predicate_partition(runtime, &slot.registration, head, generation)?;
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
    persist_predicate_checkpoint(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    Ok(Some(provider))
}

fn rebuild_nullable_predicate_slot(
    runtime: &ExactTextRuntime,
    slot: &NullableExactPredicateSlot,
    head: CommitSequence,
    prior_generation: Option<ProjectionGeneration>,
) -> Result<Option<ExactPredicatePartitionIndexV5>, RebuildFailure> {
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
        if recovered.binding().frontier() == head {
            return Ok(Some(recovered));
        }
        return rebuild_nullable_predicate_slot(
            runtime,
            slot,
            head,
            Some(recovered.binding().generation()),
        );
    }
    let generation = prior_generation
        .map_or(
            Some(runtime.initial_generation),
            ProjectionGeneration::checked_next,
        )
        .ok_or(RebuildFailure::Integrity)?;
    let rows = read_complete_predicate_partition(runtime, &slot.registration, head, generation)?;
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
    persist_predicate_checkpoint_bytes(
        &slot.checkpoint,
        &slot.registration.key(),
        runtime.history_incarnation,
        &provider
            .to_checkpoint_bytes()
            .map_err(|_| RebuildFailure::Integrity)?,
    )
    .map_err(|_| RebuildFailure::Transient)?;
    Ok(Some(provider))
}

fn read_complete_predicate_partition<R: ExactPredicateRegistrationView>(
    runtime: &ExactTextRuntime,
    registration: &R,
    expected_head: CommitSequence,
    generation: ProjectionGeneration,
) -> Result<ExactPredicateSourceRows, RebuildFailure> {
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
        let page = AuthoritativeScanReader::scan_index(&runtime.storage, request)
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
            let record = AuthoritativePointReader::read_entity(&runtime.storage, &target)
                .map_err(|_| RebuildFailure::Transient)?
                .ok_or(RebuildFailure::Transient)?;
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
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &runtime.storage,
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
    if read_application_head(&runtime.storage).map_err(|_| RebuildFailure::Transient)?
        != FrontierPosition::AppliedThrough(expected_head)
    {
        return Err(RebuildFailure::Transient);
    }
    Ok(rows.into_values().collect())
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
    runtime: &ExactTextRuntime,
    registration: &ExactTextRegistration,
    expected_head: CommitSequence,
    generation: ProjectionGeneration,
) -> Result<ExactTextSourceRows, RebuildFailure> {
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
        let page = AuthoritativeScanReader::scan_index(&runtime.storage, request)
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
            let record = AuthoritativePointReader::read_entity(&runtime.storage, &target)
                .map_err(|_| RebuildFailure::Transient)?
                .ok_or(RebuildFailure::Transient)?;
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
        let admission = QueryExecutionPort::authorize_projected_candidates(
            &runtime.storage,
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
    if read_application_head(&runtime.storage).map_err(|_| RebuildFailure::Transient)?
        != FrontierPosition::AppliedThrough(expected_head)
    {
        return Err(RebuildFailure::Transient);
    }
    Ok(rows)
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
    file.sync_all()?;
    fs::rename(&pending, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("checkpoint parent"))?,
    )?
    .sync_all()
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
    if provider_bytes.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 {
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
    file.sync_all()?;
    fs::rename(&pending, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("checkpoint parent"))?,
    )?
    .sync_all()
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
    if slot_key.len() != SLOT_KEY_BYTES || provider.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4
    {
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
