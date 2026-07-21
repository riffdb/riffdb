//! Catalog-proved, process-local command record materialization.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{
    EntitySchema, KeySchema, MAX_DECLARATIONS_PER_KIND, RecordSchema, SchemaIr, ValueType,
};
use riffdb_storage_api::{
    EntityObservation, EntityObservationPosition, ReadDependencies, ReadDependency, ReadSnapshot,
    StorageValueError, StoredEntityRecordV1, TransactionCurrentState,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, MAX_CANONICAL_DOCUMENT_BYTES, MAX_RECORD_FIELDS,
    encode_canonical_record,
};

use crate::ResolvedExecutablePlan;
use crate::lineage::{RecordOwnerV1, WriterRelation};

const CANONICAL_NULL_FIELD_BYTES_V1: usize = 4 + 1 + 1;
const MAX_MATERIALIZATION_MASK_BYTES_V1: usize =
    riffdb_storage_api::MAX_COMMAND_READ_TARGETS * MAX_DECLARATIONS_PER_KIND.div_ceil(8);

const _: () = assert!(CANONICAL_NULL_FIELD_BYTES_V1 == 6);
const _: () = assert!(MAX_MATERIALIZATION_MASK_BYTES_V1 == 2_097_152);

/// Stable classification for catalog-owned command materialization failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandSnapshotMaterializationErrorKind {
    /// Snapshot structure, lineage authority, or retained proof state was invalid.
    Integrity,
}

impl CommandSnapshotMaterializationErrorKind {
    /// Returns fixed safe text without record, key, contract, or proof details.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::Integrity => "command snapshot materialization integrity failure",
        }
    }
}

/// A typed redaction-safe command materialization failure.
#[derive(Clone, Eq, PartialEq)]
pub struct CommandSnapshotMaterializationError {
    kind: CommandSnapshotMaterializationErrorKind,
}

impl CommandSnapshotMaterializationError {
    const fn integrity() -> Self {
        Self {
            kind: CommandSnapshotMaterializationErrorKind::Integrity,
        }
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> CommandSnapshotMaterializationErrorKind {
        self.kind
    }
}

impl From<StorageValueError> for CommandSnapshotMaterializationError {
    fn from(_: StorageValueError) -> Self {
        Self::integrity()
    }
}

impl fmt::Debug for CommandSnapshotMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandSnapshotMaterializationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for CommandSnapshotMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for CommandSnapshotMaterializationError {}

/// Catalog's closed result after validating and charging one raw command snapshot.
pub enum CommandSnapshotMaterialization {
    /// A complete lineage-normalized snapshot is ready for deterministic evaluation.
    Ready(MaterializedCommandSnapshot),
    /// Valid null expansion exceeded a command materialization limit.
    ResourceLimit(CommandSnapshotResourceLimitEvidence),
}

impl fmt::Debug for CommandSnapshotMaterialization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready(_) => {
                formatter.write_str("CommandSnapshotMaterialization::Ready([REDACTED])")
            }
            Self::ResourceLimit(_) => {
                formatter.write_str("CommandSnapshotMaterialization::ResourceLimit([REDACTED])")
            }
        }
    }
}

/// A complete normalized snapshot and the sole masks authorized for its recheck.
pub struct MaterializedCommandSnapshot {
    resolved_plan: ResolvedExecutablePlan,
    snapshot: ReadSnapshot,
    masks: SnapshotMasks,
}

impl MaterializedCommandSnapshot {
    /// Borrows the exact checked plan and its shared lineage proof.
    #[must_use]
    pub const fn resolved_plan(&self) -> &ResolvedExecutablePlan {
        &self.resolved_plan
    }

    /// Borrows the complete normalized snapshot accepted by the runtime.
    #[must_use]
    pub const fn snapshot(&self) -> &ReadSnapshot {
        &self.snapshot
    }

    /// Compares dependencies first, proves raw equality, and reapplies retained masks.
    pub fn materialize_transaction_current(
        &self,
        raw: TransactionCurrentState,
    ) -> Result<TransactionCurrentMaterialization, CommandSnapshotMaterializationError> {
        if compare_current_dependencies(&self.snapshot, &raw)? {
            return Ok(TransactionCurrentMaterialization::DependencyChanged);
        }
        verify_current_against_normalized(&self.resolved_plan, &self.snapshot, &self.masks, &raw)?;
        let state = normalize_transaction_current(&self.resolved_plan, &self.masks, raw)?;
        if state.bindings() != self.snapshot.bindings()
            || state.root_validations() != self.snapshot.root_validations()
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        Ok(TransactionCurrentMaterialization::Ready(
            MaterializedTransactionCurrentState { state },
        ))
    }
}

impl fmt::Debug for MaterializedCommandSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MaterializedCommandSnapshot([REDACTED])")
    }
}

/// Result of dependency-first transaction-current materialization.
pub enum TransactionCurrentMaterialization {
    /// At least one canonical absence, version, or range epoch changed.
    DependencyChanged,
    /// Dependencies and raw physical records were exact and masks were reapplied.
    Ready(MaterializedTransactionCurrentState),
}

impl fmt::Debug for TransactionCurrentMaterialization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DependencyChanged => {
                formatter.write_str("TransactionCurrentMaterialization::DependencyChanged")
            }
            Self::Ready(_) => {
                formatter.write_str("TransactionCurrentMaterialization::Ready([REDACTED])")
            }
        }
    }
}

/// Opaque normalized transaction-current values for commit-time semantic use.
pub struct MaterializedTransactionCurrentState {
    state: TransactionCurrentState,
}

impl MaterializedTransactionCurrentState {
    /// Borrows the complete normalized current state.
    #[must_use]
    pub const fn state(&self) -> &TransactionCurrentState {
        &self.state
    }
}

impl fmt::Debug for MaterializedTransactionCurrentState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MaterializedTransactionCurrentState([REDACTED])")
    }
}

/// Minimal evidence retained after valid proof-authorized expansion overflow.
pub struct CommandSnapshotResourceLimitEvidence {
    resolved_plan: ResolvedExecutablePlan,
    raw_snapshot: ReadSnapshot,
}

impl CommandSnapshotResourceLimitEvidence {
    /// Borrows the exact checked plan and proof used for the failed expansion.
    #[must_use]
    pub const fn resolved_plan(&self) -> &ResolvedExecutablePlan {
        &self.resolved_plan
    }

    /// Borrows the original bounded raw snapshot; no normalized value is retained.
    #[must_use]
    pub const fn raw_snapshot(&self) -> &ReadSnapshot {
        &self.raw_snapshot
    }

    /// Rechecks dependencies, exact raw observations, and deterministic overflow.
    pub fn recheck_transaction_current(
        &self,
        raw: TransactionCurrentState,
    ) -> Result<ResourceLimitRecheck, CommandSnapshotMaterializationError> {
        if compare_current_dependencies(&self.raw_snapshot, &raw)? {
            return Ok(ResourceLimitRecheck::DependencyChanged);
        }
        verify_current_against_raw(&self.raw_snapshot, &raw)?;
        validate_snapshot_shape(&self.resolved_plan, &self.raw_snapshot)?;
        match analyze_snapshot(&self.resolved_plan, &self.raw_snapshot)? {
            SnapshotAnalysis::Ready(_) => Err(CommandSnapshotMaterializationError::integrity()),
            SnapshotAnalysis::ResourceLimit => Ok(ResourceLimitRecheck::Confirmed),
        }
    }
}

impl fmt::Debug for CommandSnapshotResourceLimitEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandSnapshotResourceLimitEvidence([REDACTED])")
    }
}

/// Closed result of transaction-current resource-limit evidence revalidation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ResourceLimitRecheck {
    /// At least one canonical absence, version, or range epoch changed.
    DependencyChanged,
    /// Exact raw evidence reproduced the same valid expansion overflow.
    Confirmed,
}

impl ResolvedExecutablePlan {
    /// Validates, charges, and normalizes one storage-owned raw command snapshot.
    pub fn materialize_command_snapshot(
        self,
        raw: ReadSnapshot,
    ) -> Result<CommandSnapshotMaterialization, CommandSnapshotMaterializationError> {
        validate_snapshot_shape(&self, &raw)?;
        match analyze_snapshot(&self, &raw)? {
            SnapshotAnalysis::Ready(masks) => {
                let snapshot = normalize_snapshot(&self, &masks, raw)?;
                Ok(CommandSnapshotMaterialization::Ready(
                    MaterializedCommandSnapshot {
                        resolved_plan: self,
                        snapshot,
                        masks,
                    },
                ))
            }
            SnapshotAnalysis::ResourceLimit => Ok(CommandSnapshotMaterialization::ResourceLimit(
                CommandSnapshotResourceLimitEvidence {
                    resolved_plan: self,
                    raw_snapshot: raw,
                },
            )),
        }
    }
}

struct InsertedNullMask {
    canonical_bits: Box<[u8]>,
}

impl InsertedNullMask {
    fn from_positions(
        field_count: usize,
        positions: &[usize],
    ) -> Result<Self, CommandSnapshotMaterializationError> {
        if positions.is_empty() {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        let mut canonical_bits = vec![0u8; field_count.div_ceil(8)];
        for position in positions {
            let byte = canonical_bits
                .get_mut(position / 8)
                .ok_or_else(CommandSnapshotMaterializationError::integrity)?;
            *byte |= 1 << (position % 8);
        }
        let mask = Self {
            canonical_bits: canonical_bits.into_boxed_slice(),
        };
        if !mask.has_canonical_shape(field_count) {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        Ok(mask)
    }

    fn contains(&self, position: usize) -> bool {
        self.canonical_bits
            .get(position / 8)
            .is_some_and(|byte| byte & (1 << (position % 8)) != 0)
    }

    fn semantic_bytes(&self) -> usize {
        self.canonical_bits.len()
    }

    fn has_canonical_shape(&self, field_count: usize) -> bool {
        if self.canonical_bits.is_empty()
            || self.canonical_bits.len() != field_count.div_ceil(8)
            || !self.canonical_bits.iter().any(|byte| *byte != 0)
        {
            return false;
        }
        let used_bits = field_count % 8;
        used_bits == 0
            || self
                .canonical_bits
                .last()
                .is_some_and(|last| last & !((1u8 << used_bits) - 1) == 0)
    }
}

impl fmt::Debug for InsertedNullMask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InsertedNullMask([REDACTED])")
    }
}

struct SnapshotMasks {
    bindings: Box<[Option<InsertedNullMask>]>,
    root_validations: Box<[Option<InsertedNullMask>]>,
}

enum SnapshotAnalysis {
    Ready(SnapshotMasks),
    ResourceLimit,
}

struct RecordAnalysis {
    mask: Option<InsertedNullMask>,
    inserted_fields: usize,
    record_limit_exceeded: bool,
}

fn validate_snapshot_shape(
    resolved: &ResolvedExecutablePlan,
    snapshot: &ReadSnapshot,
) -> Result<(), CommandSnapshotMaterializationError> {
    if snapshot.plan() != resolved.reference()
        || snapshot.bindings().len() != resolved.plan().bindings().len()
        || snapshot.root_validations().len() != resolved.plan().root_validation_reads().len()
        || !snapshot.ranges().is_empty()
    {
        return Err(CommandSnapshotMaterializationError::integrity());
    }

    for (observation, binding) in snapshot.bindings().iter().zip(resolved.plan().bindings()) {
        if observation.target().entity_type_id() != binding.entity_type()
            || executing_schema(resolved, binding.entity_type()).is_err()
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }
    for (observation, root) in snapshot
        .root_validations()
        .iter()
        .zip(resolved.plan().root_validation_reads())
    {
        if observation.target().entity_type_id() != root.entity_type()
            || executing_schema(resolved, root.entity_type()).is_err()
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }

    let reconstructed = ReadDependencies::new(
        snapshot
            .bindings()
            .iter()
            .chain(snapshot.root_validations())
            .map(ReadDependency::from_entity)
            .chain(snapshot.ranges().iter().map(ReadDependency::from_range)),
    )?;
    if &reconstructed != snapshot.read_dependencies() {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    let mut physical_by_target = BTreeMap::new();
    for observation in snapshot
        .bindings()
        .iter()
        .chain(snapshot.root_validations())
    {
        if physical_by_target
            .insert(observation.target(), observation)
            .is_some_and(|prior| prior != observation)
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }
    Ok(())
}

fn analyze_snapshot(
    resolved: &ResolvedExecutablePlan,
    snapshot: &ReadSnapshot,
) -> Result<SnapshotAnalysis, CommandSnapshotMaterializationError> {
    let mut binding_masks = Vec::with_capacity(snapshot.bindings().len());
    let mut root_masks = Vec::with_capacity(snapshot.root_validations().len());
    let mut combined_bytes = snapshot.semantic_bytes();
    let mut structural_mask_bytes = 0usize;
    let mut resource_limit = false;

    for (position, observation) in snapshot.bindings().iter().enumerate() {
        let analysis = analyze_observation(
            resolved,
            EntityObservationPosition::Binding(position),
            observation,
        )?;
        retain_analysis(
            analysis,
            &mut binding_masks,
            &mut root_masks,
            &mut combined_bytes,
            &mut structural_mask_bytes,
            &mut resource_limit,
        )?;
    }
    for (position, observation) in snapshot.root_validations().iter().enumerate() {
        let analysis = analyze_observation(
            resolved,
            EntityObservationPosition::RootValidation(position),
            observation,
        )?;
        retain_analysis(
            analysis,
            &mut root_masks,
            &mut binding_masks,
            &mut combined_bytes,
            &mut structural_mask_bytes,
            &mut resource_limit,
        )?;
    }

    if structural_mask_bytes > MAX_MATERIALIZATION_MASK_BYTES_V1 {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    if resource_limit {
        Ok(SnapshotAnalysis::ResourceLimit)
    } else if binding_masks.len() == snapshot.bindings().len()
        && root_masks.len() == snapshot.root_validations().len()
    {
        Ok(SnapshotAnalysis::Ready(SnapshotMasks {
            bindings: binding_masks.into_boxed_slice(),
            root_validations: root_masks.into_boxed_slice(),
        }))
    } else {
        Err(CommandSnapshotMaterializationError::integrity())
    }
}

#[allow(clippy::too_many_arguments)]
fn retain_analysis(
    analysis: RecordAnalysis,
    destination: &mut Vec<Option<InsertedNullMask>>,
    other_destination: &mut Vec<Option<InsertedNullMask>>,
    combined_bytes: &mut usize,
    structural_mask_bytes: &mut usize,
    resource_limit: &mut bool,
) -> Result<(), CommandSnapshotMaterializationError> {
    let mask_bytes = analysis
        .mask
        .as_ref()
        .map_or(0, InsertedNullMask::semantic_bytes);
    *structural_mask_bytes = structural_mask_bytes
        .checked_add(mask_bytes)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;
    let inserted_bytes = analysis
        .inserted_fields
        .checked_mul(CANONICAL_NULL_FIELD_BYTES_V1)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;
    *combined_bytes = combined_bytes
        .checked_add(inserted_bytes)
        .and_then(|total| total.checked_add(mask_bytes))
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;

    if analysis.record_limit_exceeded
        || *combined_bytes > riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES
    {
        *resource_limit = true;
        destination.clear();
        other_destination.clear();
    } else if !*resource_limit {
        destination.push(analysis.mask);
    }
    Ok(())
}

fn analyze_observation(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
    observation: &EntityObservation,
) -> Result<RecordAnalysis, CommandSnapshotMaterializationError> {
    let EntityObservation::Present(record) = observation else {
        if matches!(position, EntityObservationPosition::RootValidation(_)) {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        return Ok(RecordAnalysis {
            mask: None,
            inserted_fields: 0,
            record_limit_exceeded: false,
        });
    };
    let entity = entity_at_position(resolved, position)?;
    analyze_record(resolved, position, entity, record)
}

fn analyze_record(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
    entity: &EntitySchema,
    record: &StoredEntityRecordV1,
) -> Result<RecordAnalysis, CommandSnapshotMaterializationError> {
    let schema = entity.record();
    let owner = RecordOwnerV1::Entity(record.target().entity_type_id());
    let (relation, eligibility) = resolved
        .lineage_proof()
        .writer_materialization(owner, record.schema_binding(), resolved.executing_ordinal())
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    if eligibility
        .as_ref()
        .is_some_and(|mask| !mask.has_canonical_shape(schema.fields().len()))
    {
        return Err(CommandSnapshotMaterializationError::integrity());
    }

    let mut missing = Vec::new();
    for (position, field) in schema.fields().iter().enumerate() {
        let value = record
            .fields()
            .fields()
            .binary_search_by_key(&field.id(), |(id, _)| *id)
            .ok()
            .map(|index| &record.fields().fields()[index].1);
        let eligible = eligibility
            .as_ref()
            .is_some_and(|mask| mask.allows(position));
        if eligible && (relation != WriterRelation::Ancestor || !field.value_type().is_optional()) {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        if let Some(value) = value {
            validate_static_value(
                resolved.bundle().bundle().schema(),
                field.value_type(),
                value,
            )?;
        } else {
            if relation != WriterRelation::Ancestor
                || !eligible
                || !field.value_type().is_optional()
            {
                return Err(CommandSnapshotMaterializationError::integrity());
            }
            missing.push(position);
            validate_static_value(
                resolved.bundle().bundle().schema(),
                field.value_type(),
                &CanonicalValue::Null,
            )?;
        }
    }
    validate_record_key(resolved, position, entity, record)?;

    let mask = if missing.is_empty() {
        None
    } else {
        Some(InsertedNullMask::from_positions(
            schema.fields().len(),
            &missing,
        )?)
    };
    let inserted_bytes = missing
        .len()
        .checked_mul(CANONICAL_NULL_FIELD_BYTES_V1)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;
    let canonical_bytes = encode_canonical_record(record.fields())
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    let expanded_bytes = canonical_bytes
        .len()
        .checked_add(inserted_bytes)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;
    let expanded_fields = record
        .fields()
        .len()
        .checked_add(missing.len())
        .ok_or_else(CommandSnapshotMaterializationError::integrity)?;

    Ok(RecordAnalysis {
        mask,
        inserted_fields: missing.len(),
        record_limit_exceeded: expanded_bytes > MAX_CANONICAL_DOCUMENT_BYTES
            || expanded_fields > MAX_RECORD_FIELDS,
    })
}

fn validate_static_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &CanonicalValue,
) -> Result<(), CommandSnapshotMaterializationError> {
    value_type
        .validate_value(value)
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    if matches!(value, CanonicalValue::Null) {
        return Ok(());
    }
    if let Some(inner) = value_type.optional_inner() {
        return validate_static_value(schema, inner, value);
    }
    if let Some(enum_id) = value_type.enum_type_id() {
        let CanonicalValue::Enum {
            type_id,
            variant_id,
        } = value
        else {
            return Err(CommandSnapshotMaterializationError::integrity());
        };
        if *type_id != enum_id
            || schema
                .enumeration(enum_id)
                .is_none_or(|enumeration| !enumeration.contains_variant(*variant_id))
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }
    if let Some((element, _)) = value_type.list_parts() {
        let CanonicalValue::List(values) = value else {
            return Err(CommandSnapshotMaterializationError::integrity());
        };
        for value in values.values() {
            validate_static_value(schema, element, value)?;
        }
    }
    Ok(())
}

fn validate_record_key(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
    entity: &EntitySchema,
    record: &StoredEntityRecordV1,
) -> Result<(), CommandSnapshotMaterializationError> {
    let key_schema = key_schema_at_position(resolved, position)?;
    if key_schema != entity.primary_key() {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    let values = key_schema
        .decode_entity(record.target().key())
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    if values.len() != entity.primary_key_fields().len() {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    for (field_id, expected) in entity.primary_key_fields().iter().zip(values) {
        let actual = record
            .fields()
            .fields()
            .binary_search_by_key(field_id, |(id, _)| *id)
            .ok()
            .map(|index| &record.fields().fields()[index].1);
        if actual != Some(&expected) {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }
    Ok(())
}

fn normalize_snapshot(
    resolved: &ResolvedExecutablePlan,
    masks: &SnapshotMasks,
    raw: ReadSnapshot,
) -> Result<ReadSnapshot, CommandSnapshotMaterializationError> {
    raw.try_map_present_records(|position, record| {
        let schema = schema_at_position(resolved, position)?;
        normalize_record(record, schema, mask_at(masks, position)?)
    })
}

fn normalize_transaction_current(
    resolved: &ResolvedExecutablePlan,
    masks: &SnapshotMasks,
    raw: TransactionCurrentState,
) -> Result<TransactionCurrentState, CommandSnapshotMaterializationError> {
    raw.try_map_present_records(|position, record| {
        let schema = schema_at_position(resolved, position)?;
        normalize_record(record, schema, mask_at(masks, position)?)
    })
}

fn normalize_record(
    record: StoredEntityRecordV1,
    schema: &RecordSchema,
    mask: Option<&InsertedNullMask>,
) -> Result<StoredEntityRecordV1, CommandSnapshotMaterializationError> {
    if mask.is_some_and(|mask| !mask.has_canonical_shape(schema.fields().len())) {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    let mut fields = record.fields().fields().to_vec();
    for (position, field) in schema.fields().iter().enumerate() {
        let found = record
            .fields()
            .fields()
            .binary_search_by_key(&field.id(), |(id, _)| *id)
            .is_ok();
        let insert = mask.is_some_and(|mask| mask.contains(position));
        if found == insert || (!found && !field.value_type().is_optional()) {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
        if insert {
            fields.push((field.id(), CanonicalValue::Null));
        }
    }
    let fields = CanonicalRecord::new(fields)
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    StoredEntityRecordV1::new(
        record.target().clone(),
        record.entity_version(),
        record.written_by_contract(),
        record.schema_binding().clone(),
        fields,
    )
    .map_err(Into::into)
}

fn reconstruct_raw_record(
    record: &StoredEntityRecordV1,
    schema: &RecordSchema,
    mask: Option<&InsertedNullMask>,
) -> Result<StoredEntityRecordV1, CommandSnapshotMaterializationError> {
    if mask.is_some_and(|mask| !mask.has_canonical_shape(schema.fields().len())) {
        return Err(CommandSnapshotMaterializationError::integrity());
    }
    let mut fields = Vec::with_capacity(record.fields().len());
    for (field_id, value) in record.fields().fields() {
        let schema_position = schema
            .fields()
            .binary_search_by_key(field_id, |field| field.id())
            .ok();
        let remove = schema_position
            .is_some_and(|position| mask.is_some_and(|mask| mask.contains(position)));
        if remove {
            if value != &CanonicalValue::Null {
                return Err(CommandSnapshotMaterializationError::integrity());
            }
        } else {
            fields.push((*field_id, value.clone()));
        }
    }
    for (position, field) in schema.fields().iter().enumerate() {
        let present = record
            .fields()
            .fields()
            .binary_search_by_key(&field.id(), |(id, _)| *id)
            .is_ok();
        if !present
            || (mask.is_some_and(|mask| mask.contains(position))
                && !field.value_type().is_optional())
        {
            return Err(CommandSnapshotMaterializationError::integrity());
        }
    }
    let fields = CanonicalRecord::new(fields)
        .map_err(|_| CommandSnapshotMaterializationError::integrity())?;
    StoredEntityRecordV1::new(
        record.target().clone(),
        record.entity_version(),
        record.written_by_contract(),
        record.schema_binding().clone(),
        fields,
    )
    .map_err(Into::into)
}

fn compare_current_dependencies(
    snapshot: &ReadSnapshot,
    current: &TransactionCurrentState,
) -> Result<bool, CommandSnapshotMaterializationError> {
    validate_current_shape(snapshot, current)?;
    let current_dependencies = ReadDependencies::new(
        current
            .bindings()
            .iter()
            .chain(current.root_validations())
            .map(ReadDependency::from_entity)
            .chain(
                current
                    .ranges()
                    .iter()
                    .map(|range| ReadDependency::IndexRangeEpoch {
                        target: range.target().clone(),
                        expected: range.epoch(),
                    }),
            ),
    )?;
    Ok(&current_dependencies != snapshot.read_dependencies())
}

fn validate_current_shape(
    snapshot: &ReadSnapshot,
    current: &TransactionCurrentState,
) -> Result<(), CommandSnapshotMaterializationError> {
    let entity_shape_matches = snapshot.bindings().len() == current.bindings().len()
        && snapshot.root_validations().len() == current.root_validations().len()
        && snapshot
            .bindings()
            .iter()
            .zip(current.bindings())
            .all(|(before, now)| before.target() == now.target())
        && snapshot
            .root_validations()
            .iter()
            .zip(current.root_validations())
            .all(|(before, now)| before.target() == now.target());
    let range_shape_matches = snapshot.ranges().len() == current.ranges().len()
        && snapshot
            .ranges()
            .iter()
            .zip(current.ranges())
            .all(|(before, now)| before.target() == now.target());
    if entity_shape_matches && range_shape_matches {
        Ok(())
    } else {
        Err(CommandSnapshotMaterializationError::integrity())
    }
}

fn verify_current_against_normalized(
    resolved: &ResolvedExecutablePlan,
    snapshot: &ReadSnapshot,
    masks: &SnapshotMasks,
    current: &TransactionCurrentState,
) -> Result<(), CommandSnapshotMaterializationError> {
    for (position, (retained, raw)) in snapshot
        .bindings()
        .iter()
        .zip(current.bindings())
        .enumerate()
    {
        verify_raw_observation(
            resolved,
            EntityObservationPosition::Binding(position),
            retained,
            mask_at(masks, EntityObservationPosition::Binding(position))?,
            raw,
        )?;
    }
    for (position, (retained, raw)) in snapshot
        .root_validations()
        .iter()
        .zip(current.root_validations())
        .enumerate()
    {
        verify_raw_observation(
            resolved,
            EntityObservationPosition::RootValidation(position),
            retained,
            mask_at(masks, EntityObservationPosition::RootValidation(position))?,
            raw,
        )?;
    }
    Ok(())
}

fn verify_raw_observation(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
    retained: &EntityObservation,
    mask: Option<&InsertedNullMask>,
    raw: &EntityObservation,
) -> Result<(), CommandSnapshotMaterializationError> {
    match retained {
        EntityObservation::Absent(_) if mask.is_none() && retained == raw => Ok(()),
        EntityObservation::Present(record) => {
            let reconstructed =
                reconstruct_raw_record(record, schema_at_position(resolved, position)?, mask)?;
            if raw == &EntityObservation::Present(reconstructed) {
                Ok(())
            } else {
                Err(CommandSnapshotMaterializationError::integrity())
            }
        }
        EntityObservation::Absent(_) => Err(CommandSnapshotMaterializationError::integrity()),
    }
}

fn verify_current_against_raw(
    snapshot: &ReadSnapshot,
    current: &TransactionCurrentState,
) -> Result<(), CommandSnapshotMaterializationError> {
    if snapshot.bindings() == current.bindings()
        && snapshot.root_validations() == current.root_validations()
    {
        Ok(())
    } else {
        Err(CommandSnapshotMaterializationError::integrity())
    }
}

fn mask_at(
    masks: &SnapshotMasks,
    position: EntityObservationPosition,
) -> Result<Option<&InsertedNullMask>, CommandSnapshotMaterializationError> {
    match position {
        EntityObservationPosition::Binding(index) => masks.bindings.get(index),
        EntityObservationPosition::RootValidation(index) => masks.root_validations.get(index),
    }
    .map(Option::as_ref)
    .ok_or_else(CommandSnapshotMaterializationError::integrity)
}

fn schema_at_position(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
) -> Result<&RecordSchema, CommandSnapshotMaterializationError> {
    Ok(entity_at_position(resolved, position)?.record())
}

fn entity_at_position(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
) -> Result<&EntitySchema, CommandSnapshotMaterializationError> {
    let entity_type = entity_type_at_position(resolved, position)?;
    resolved
        .bundle()
        .bundle()
        .schema()
        .entity(entity_type)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)
}

fn entity_type_at_position(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
) -> Result<riffdb_types::EntityTypeId, CommandSnapshotMaterializationError> {
    match position {
        EntityObservationPosition::Binding(index) => resolved
            .plan()
            .bindings()
            .get(index)
            .map(riffdb_contract_ir::BindingPlan::entity_type),
        EntityObservationPosition::RootValidation(index) => resolved
            .plan()
            .root_validation_reads()
            .get(index)
            .map(riffdb_contract_ir::RootValidationReadPlan::entity_type),
    }
    .ok_or_else(CommandSnapshotMaterializationError::integrity)
}

fn key_schema_at_position(
    resolved: &ResolvedExecutablePlan,
    position: EntityObservationPosition,
) -> Result<&KeySchema, CommandSnapshotMaterializationError> {
    match position {
        EntityObservationPosition::Binding(index) => resolved
            .plan()
            .bindings()
            .get(index)
            .map(riffdb_contract_ir::BindingPlan::key_schema),
        EntityObservationPosition::RootValidation(index) => resolved
            .plan()
            .root_validation_reads()
            .get(index)
            .map(riffdb_contract_ir::RootValidationReadPlan::key_schema),
    }
    .ok_or_else(CommandSnapshotMaterializationError::integrity)
}

fn executing_schema(
    resolved: &ResolvedExecutablePlan,
    entity_type: riffdb_types::EntityTypeId,
) -> Result<&RecordSchema, CommandSnapshotMaterializationError> {
    resolved
        .bundle()
        .bundle()
        .schema()
        .entity(entity_type)
        .map(riffdb_contract_ir::EntitySchema::record)
        .ok_or_else(CommandSnapshotMaterializationError::integrity)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, EntityObservation, EntityTarget, ExecutablePlanRef,
        ReadSnapshot, SnapshotRequest, StoredEntityRecordV1, TransactionCurrentState,
    };
    use riffdb_types::{
        CanonicalRecord, CanonicalValue, EntityKeyBuilder, EntityTypeId, EntityVersion, FieldId,
        MAX_CANONICAL_DOCUMENT_BYTES, encode_canonical_record,
    };

    use super::*;
    use crate::ValidatedContractBundle;
    use crate::lineage::LineageMaterializationProof;

    const UNKNOWN_FIELD: u32 = u32::MAX;

    fn source(version: u64, optional_note: bool, binding_count: usize) -> String {
        assert!(binding_count > 0);
        let note = if optional_note {
            "field note: optional<string<8>>"
        } else {
            ""
        };
        let inputs = (0..binding_count)
            .map(|index| format!("    input id_{index}: uuid\n"))
            .collect::<String>();
        let reads = (0..binding_count)
            .map(|index| {
                format!(
                    "    read Row(tenant, id_{index}) as row_{index} else Missing {{ id: id_{index} }}\n"
                )
            })
            .collect::<String>();
        format!(
            r#"
contract SnapshotMaterialization version {version} {{
  entity Row {{ key (tenant: uuid, id: uuid) field value: i64 {note} }}
  aggregate Rows {{ root Row partition_by tenant conflict_key (tenant) }}
  command Observe {{
    input tenant: uuid
{inputs}{reads}    return Found {{ value: row_0.value }}
  }}
}}
"#
        )
    }

    fn lineage(binding_count: usize) -> Vec<ValidatedContractBundle> {
        let genesis_source = source(1, false, binding_count);
        let genesis = compile_contract_source(&genesis_source).expect("genesis compiles");
        let successor = compile_contract_successor(&source(2, true, binding_count), &genesis)
            .expect("optional successor compiles");
        [genesis, successor]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect()
    }

    fn resolved_at(
        bundles: Vec<ValidatedContractBundle>,
        executing_index: usize,
    ) -> ResolvedExecutablePlan {
        let executing = bundles[executing_index].clone();
        let command = executing.bundle().commands().first().expect("command");
        let reference = ExecutablePlanRef::new(
            executing.lineage().clone(),
            executing.contract_version(),
            executing.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let proof =
            LineageMaterializationProof::from_forward_bundles(bundles).expect("lineage proof");
        executing
            .resolve_plan_with_proof(
                &reference,
                Arc::clone(&proof),
                u16::try_from(executing_index).expect("test ordinal"),
            )
            .expect("resolved plan")
    }

    fn binding(bundle: &ValidatedContractBundle) -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        )
    }

    fn entity_type(bundle: &ValidatedContractBundle) -> EntityTypeId {
        bundle.bundle().schema().entities()[0].id()
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut value = [0u8; 16];
        value[15] = seed;
        value
    }

    fn target(entity_type: EntityTypeId, seed: u8) -> EntityTarget {
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_uuid(&uuid(0xaa)).expect("bounded tenant UUID");
        key.push_uuid(&uuid(seed)).expect("bounded UUID key");
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target")
    }

    fn fields(
        schema_bundle: &ValidatedContractBundle,
        entity_type: EntityTypeId,
        seed: u8,
        include_optional: bool,
        value: i64,
        unknown_payload: Option<usize>,
    ) -> CanonicalRecord {
        let schema = schema_bundle
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record();
        let mut fields = Vec::new();
        for field in schema.fields() {
            match field.name() {
                "tenant" => fields.push((field.id(), CanonicalValue::Uuid(uuid(0xaa)))),
                "id" => fields.push((field.id(), CanonicalValue::Uuid(uuid(seed)))),
                "value" => fields.push((field.id(), CanonicalValue::I64(value))),
                "note" if include_optional => fields.push((field.id(), CanonicalValue::Null)),
                "note" => {}
                unexpected => panic!("unexpected fixture field {unexpected}"),
            }
        }
        if let Some(payload) = unknown_payload {
            fields.push((
                FieldId::new(UNKNOWN_FIELD).expect("nonzero unknown field"),
                CanonicalValue::bytes(vec![0; payload]).expect("bounded bytes"),
            ));
        }
        CanonicalRecord::new(fields).expect("canonical fixture record")
    }

    fn record(
        writer: &ValidatedContractBundle,
        entity_type: EntityTypeId,
        seed: u8,
        entity_version: u64,
        include_optional: bool,
        value: i64,
        unknown_payload: Option<usize>,
    ) -> StoredEntityRecordV1 {
        StoredEntityRecordV1::new(
            target(entity_type, seed),
            EntityVersion::new(entity_version).expect("entity version"),
            writer.contract_version(),
            binding(writer),
            fields(
                writer,
                entity_type,
                seed,
                include_optional,
                value,
                unknown_payload,
            ),
        )
        .expect("stored fixture record")
    }

    fn snapshot(
        resolved: &ResolvedExecutablePlan,
        observations: Vec<EntityObservation>,
    ) -> ReadSnapshot {
        let targets = observations
            .iter()
            .map(|observation| observation.target().clone())
            .collect();
        let request = SnapshotRequest::new(
            resolved.reference().clone(),
            targets,
            Vec::new(),
            Vec::new(),
        )
        .expect("snapshot request");
        ReadSnapshot::new(&request, None, observations, Vec::new(), Vec::new())
            .expect("bounded snapshot")
    }

    fn current(
        snapshot: &ReadSnapshot,
        observations: Vec<EntityObservation>,
    ) -> TransactionCurrentState {
        let request = snapshot.validation_request();
        TransactionCurrentState::new(&request, observations, Vec::new(), Vec::new())
            .expect("bounded current state")
    }

    fn payload_for_record_size(
        bundle: &ValidatedContractBundle,
        entity_type: EntityTypeId,
        seed: u8,
        desired_size: usize,
    ) -> usize {
        let empty = fields(bundle, entity_type, seed, false, 7, Some(0));
        let empty_size = encode_canonical_record(&empty).expect("encoding").len();
        let payload = desired_size
            .checked_sub(empty_size)
            .expect("desired fixture size has room for framing");
        let exact = fields(bundle, entity_type, seed, false, 7, Some(payload));
        assert_eq!(
            encode_canonical_record(&exact).expect("encoding").len(),
            desired_size
        );
        payload
    }

    #[test]
    fn ancestor_omission_materializes_once_and_reapplies_after_exact_raw_equality() {
        let bundles = lineage(1);
        let writer = bundles[0].clone();
        let entity_type = entity_type(&writer);
        let raw_record = record(&writer, entity_type, 1, 9, false, 41, Some(3));
        let resolved = resolved_at(bundles, 1);
        let raw_snapshot = snapshot(
            &resolved,
            vec![EntityObservation::Present(raw_record.clone())],
        );
        let raw_dependencies = raw_snapshot.read_dependencies().clone();

        let CommandSnapshotMaterialization::Ready(materialized) = resolved
            .materialize_command_snapshot(raw_snapshot)
            .expect("authorized ancestor normalization")
        else {
            panic!("small expansion must fit");
        };
        assert_eq!(
            materialized.resolved_plan().reference(),
            materialized.snapshot().plan()
        );
        assert_eq!(
            materialized.snapshot().read_dependencies(),
            &raw_dependencies
        );
        let normalized = match &materialized.snapshot().bindings()[0] {
            EntityObservation::Present(record) => record,
            EntityObservation::Absent(_) => panic!("present fixture"),
        };
        let note = materialized
            .resolved_plan()
            .bundle()
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "note")
            .expect("added note");
        assert_eq!(
            normalized
                .fields()
                .fields()
                .binary_search_by_key(&note.id(), |(id, _)| *id)
                .ok()
                .map(|index| &normalized.fields().fields()[index].1),
            Some(&CanonicalValue::Null)
        );
        let unknown = FieldId::new(UNKNOWN_FIELD).expect("unknown field");
        let raw_unknown = raw_record
            .fields()
            .fields()
            .binary_search_by_key(&unknown, |(id, _)| *id)
            .ok()
            .map(|index| &raw_record.fields().fields()[index].1);
        let normalized_unknown = normalized
            .fields()
            .fields()
            .binary_search_by_key(&unknown, |(id, _)| *id)
            .ok()
            .map(|index| &normalized.fields().fields()[index].1);
        assert_eq!(normalized_unknown, raw_unknown);

        let raw_current = current(
            materialized.snapshot(),
            vec![EntityObservation::Present(raw_record.clone())],
        );
        let TransactionCurrentMaterialization::Ready(normalized_current) = materialized
            .materialize_transaction_current(raw_current)
            .expect("exact raw current")
        else {
            panic!("dependency remained equal");
        };
        assert_eq!(
            normalized_current.state().bindings(),
            materialized.snapshot().bindings()
        );

        let changed_version = record(&writer, entity_type, 1, 10, false, 99, None);
        let changed = current(
            materialized.snapshot(),
            vec![EntityObservation::Present(changed_version)],
        );
        assert!(matches!(
            materialized
                .materialize_transaction_current(changed)
                .expect("changed dependency is not an integrity fault"),
            TransactionCurrentMaterialization::DependencyChanged
        ));

        let foreign_changed = StoredEntityRecordV1::new(
            target(entity_type, 1),
            EntityVersion::new(10).expect("changed version"),
            writer.contract_version(),
            DurableKeySchemaBindingV1::new(
                riffdb_types::ContractLineage::new("ForeignCurrent").expect("lineage"),
                writer.contract_version(),
                writer.bundle_hash(),
            ),
            fields(&writer, entity_type, 1, false, 99, None),
        )
        .expect("structural changed record");
        let changed = current(
            materialized.snapshot(),
            vec![EntityObservation::Present(foreign_changed)],
        );
        assert!(matches!(
            materialized
                .materialize_transaction_current(changed)
                .expect("dependency change must precede writer normalization"),
            TransactionCurrentMaterialization::DependencyChanged
        ));

        let drift = record(&writer, entity_type, 1, 9, false, 42, None);
        let drift = current(
            materialized.snapshot(),
            vec![EntityObservation::Present(drift)],
        );
        assert_eq!(
            materialized
                .materialize_transaction_current(drift)
                .expect_err("same-version physical drift is integrity")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );

        let wrong_hash = StoredEntityRecordV1::new(
            raw_record.target().clone(),
            raw_record.entity_version(),
            raw_record.written_by_contract(),
            DurableKeySchemaBindingV1::new(
                writer.lineage().clone(),
                writer.contract_version(),
                riffdb_types::ContractBundleHash::from_bytes([0xd1; 32]),
            ),
            raw_record.fields().clone(),
        )
        .expect("wrong-hash structural record");
        let successor = materialized.resolved_plan().bundle().clone();
        let wrong_writer = StoredEntityRecordV1::new(
            raw_record.target().clone(),
            raw_record.entity_version(),
            successor.contract_version(),
            binding(&successor),
            raw_record.fields().clone(),
        )
        .expect("wrong-writer structural record");
        let mut unknown_drift_fields = raw_record.fields().fields().to_vec();
        let unknown = FieldId::new(UNKNOWN_FIELD).expect("unknown field");
        let unknown_index = unknown_drift_fields
            .binary_search_by_key(&unknown, |(id, _)| *id)
            .expect("unknown field position");
        unknown_drift_fields[unknown_index].1 =
            CanonicalValue::bytes(vec![0, 0, 1]).expect("unknown bytes");
        let unknown_drift = StoredEntityRecordV1::new(
            raw_record.target().clone(),
            raw_record.entity_version(),
            raw_record.written_by_contract(),
            raw_record.schema_binding().clone(),
            CanonicalRecord::new(unknown_drift_fields).expect("unknown drift fields"),
        )
        .expect("unknown-drift structural record");
        for (case, drift) in [
            ("schema binding hash", wrong_hash),
            ("writer version and schema binding", wrong_writer),
            ("unknown field bytes", unknown_drift),
        ] {
            let drift = current(
                materialized.snapshot(),
                vec![EntityObservation::Present(drift)],
            );
            assert_eq!(
                materialized
                    .materialize_transaction_current(drift)
                    .expect_err(case)
                    .kind(),
                CommandSnapshotMaterializationErrorKind::Integrity,
                "equal-version {case} drift must be integrity"
            );
        }
        assert_eq!(
            format!("{materialized:?}"),
            "MaterializedCommandSnapshot([REDACTED])"
        );
        assert_eq!(
            format!("{normalized_current:?}"),
            "MaterializedTransactionCurrentState([REDACTED])"
        );
    }

    #[test]
    fn exact_descendant_foreign_and_required_omissions_fail_closed_or_preserve_unknowns() {
        let bundles = lineage(1);
        let genesis = bundles[0].clone();
        let successor = bundles[1].clone();
        let entity_type = entity_type(&genesis);

        let exact_missing = record(&successor, entity_type, 2, 1, false, 7, None);
        let exact_plan = resolved_at(bundles.clone(), 1);
        let error = exact_plan
            .materialize_command_snapshot(snapshot(
                &resolved_at(bundles.clone(), 1),
                vec![EntityObservation::Present(exact_missing)],
            ))
            .expect_err("same-version omission has no null-fill authority");
        assert_eq!(
            error.kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );

        let descendant = record(&successor, entity_type, 3, 1, true, 8, None);
        let old_plan_for_snapshot = resolved_at(bundles.clone(), 0);
        let old_snapshot = snapshot(
            &old_plan_for_snapshot,
            vec![EntityObservation::Present(descendant)],
        );
        let CommandSnapshotMaterialization::Ready(old_view) = old_plan_for_snapshot
            .materialize_command_snapshot(old_snapshot)
            .expect("complete descendant record is allowed")
        else {
            panic!("no expansion needed");
        };
        let retained = match &old_view.snapshot().bindings()[0] {
            EntityObservation::Present(record) => record,
            EntityObservation::Absent(_) => panic!("present descendant"),
        };
        let note_id = successor
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "note")
            .expect("note")
            .id();
        assert!(
            retained
                .fields()
                .fields()
                .binary_search_by_key(&note_id, |(id, _)| *id)
                .is_ok(),
            "unknown descendant field must remain present"
        );

        let successor_schema = successor
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("successor entity")
            .record();
        let descendant_missing_old_field = CanonicalRecord::new(
            successor_schema
                .fields()
                .iter()
                .filter_map(|field| match field.name() {
                    "tenant" => Some((field.id(), CanonicalValue::Uuid(uuid(0xaa)))),
                    "id" => Some((field.id(), CanonicalValue::Uuid(uuid(12)))),
                    "note" => Some((field.id(), CanonicalValue::Null)),
                    "value" => None,
                    unexpected => panic!("unexpected descendant field {unexpected}"),
                })
                .collect(),
        )
        .expect("descendant partial fields");
        let descendant_missing_old_field = StoredEntityRecordV1::new(
            target(entity_type, 12),
            EntityVersion::new(1).expect("version"),
            successor.contract_version(),
            binding(&successor),
            descendant_missing_old_field,
        )
        .expect("structural descendant record");
        let old_plan = resolved_at(bundles.clone(), 0);
        let raw = snapshot(
            &old_plan,
            vec![EntityObservation::Present(descendant_missing_old_field)],
        );
        assert_eq!(
            old_plan
                .materialize_command_snapshot(raw)
                .expect_err("descendant omission of an executing field is integrity")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );

        let schema = genesis
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record();
        let id_field = schema
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("id")
            .id();
        let missing_required_fields =
            CanonicalRecord::new(vec![(id_field, CanonicalValue::Uuid(uuid(4)))])
                .expect("partial record");
        let missing_required = StoredEntityRecordV1::new(
            target(entity_type, 4),
            EntityVersion::new(1).expect("version"),
            genesis.contract_version(),
            binding(&genesis),
            missing_required_fields,
        )
        .expect("structural stored record");
        let plan = resolved_at(bundles.clone(), 1);
        let raw = snapshot(&plan, vec![EntityObservation::Present(missing_required)]);
        assert_eq!(
            plan.materialize_command_snapshot(raw)
                .expect_err("required genesis field cannot be synthesized")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );

        let optional_genesis_compiled =
            compile_contract_source(&source(1, true, 1)).expect("optional genesis");
        let optional_genesis =
            ValidatedContractBundle::from_compiler_bundle(optional_genesis_compiled)
                .expect("catalog optional genesis");
        let optional_entity = self::entity_type(&optional_genesis);
        let missing_genesis_optional =
            record(&optional_genesis, optional_entity, 13, 1, false, 7, None);
        let optional_genesis_plan = resolved_at(vec![optional_genesis], 0);
        let raw = snapshot(
            &optional_genesis_plan,
            vec![EntityObservation::Present(missing_genesis_optional)],
        );
        assert_eq!(
            optional_genesis_plan
                .materialize_command_snapshot(raw)
                .expect_err("genesis optional omission has no ancestry authority")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );

        let foreign_binding = DurableKeySchemaBindingV1::new(
            riffdb_types::ContractLineage::new("Foreign").expect("lineage"),
            genesis.contract_version(),
            genesis.bundle_hash(),
        );
        let foreign = StoredEntityRecordV1::new(
            target(entity_type, 5),
            EntityVersion::new(1).expect("version"),
            genesis.contract_version(),
            foreign_binding,
            fields(&genesis, entity_type, 5, false, 7, None),
        )
        .expect("structural foreign record");
        let plan = resolved_at(bundles, 1);
        let raw = snapshot(&plan, vec![EntityObservation::Present(foreign)]);
        assert_eq!(
            plan.materialize_command_snapshot(raw)
                .expect_err("foreign writer is integrity")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );
    }

    #[test]
    fn eligible_but_present_field_retains_no_actual_mask_charge() {
        let bundles = lineage(1);
        let genesis = bundles[0].clone();
        let successor = bundles[1].clone();
        let entity_type = entity_type(&genesis);
        let note_id = successor
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "note")
            .expect("note")
            .id();
        let mut physical = fields(&genesis, entity_type, 6, false, 7, None)
            .fields()
            .to_vec();
        physical.push((note_id, CanonicalValue::Null));
        let physical = CanonicalRecord::new(physical).expect("canonical physical fields");
        let record = StoredEntityRecordV1::new(
            target(entity_type, 6),
            EntityVersion::new(1).expect("version"),
            genesis.contract_version(),
            binding(&genesis),
            physical,
        )
        .expect("stored record");
        let plan = resolved_at(bundles, 1);
        let raw = snapshot(&plan, vec![EntityObservation::Present(record)]);
        let SnapshotAnalysis::Ready(masks) = analyze_snapshot(&plan, &raw).expect("analysis")
        else {
            panic!("no insertion means no expansion limit");
        };
        assert!(masks.bindings[0].is_none());
    }

    #[test]
    fn multiple_insertions_add_exact_six_byte_fields_in_canonical_order() {
        let genesis_source = source(1, false, 1);
        let genesis_compiled = compile_contract_source(&genesis_source).expect("genesis");
        let successor_source = source(2, false, 1).replace(
            "field value: i64",
            "field value: i64 field note: optional<string<8>> field tag: optional<i64>",
        );
        let successor_compiled = compile_contract_successor(&successor_source, &genesis_compiled)
            .expect("two-field successor");
        let bundles = [genesis_compiled, successor_compiled]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect::<Vec<_>>();
        let writer = bundles[0].clone();
        let entity_type = entity_type(&writer);
        let raw_record = record(&writer, entity_type, 11, 1, false, 7, Some(0));
        let raw_record_bytes = encode_canonical_record(raw_record.fields())
            .expect("raw encoding")
            .len();
        let plan = resolved_at(bundles, 1);
        let raw = snapshot(&plan, vec![EntityObservation::Present(raw_record)]);
        let SnapshotAnalysis::Ready(masks) = analyze_snapshot(&plan, &raw).expect("analysis")
        else {
            panic!("small expansion fits");
        };
        assert_eq!(
            masks.bindings[0]
                .as_ref()
                .expect("two exact insertions")
                .semantic_bytes(),
            1
        );
        let CommandSnapshotMaterialization::Ready(materialized) = plan
            .materialize_command_snapshot(raw)
            .expect("normalization")
        else {
            panic!("small expansion fits");
        };
        let EntityObservation::Present(record) = &materialized.snapshot().bindings()[0] else {
            panic!("present record");
        };
        assert_eq!(
            encode_canonical_record(record.fields())
                .expect("normalized encoding")
                .len(),
            raw_record_bytes + 2 * CANONICAL_NULL_FIELD_BYTES_V1
        );
        assert!(
            record
                .fields()
                .fields()
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0)
        );
        let optional_ids = materialized
            .resolved_plan()
            .bundle()
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .filter(|field| field.value_type().is_optional())
            .map(|field| field.id())
            .collect::<Vec<_>>();
        assert_eq!(optional_ids.len(), 2);
        for field_id in optional_ids {
            let index = record
                .fields()
                .fields()
                .binary_search_by_key(&field_id, |(id, _)| *id)
                .expect("inserted field");
            assert_eq!(record.fields().fields()[index].1, CanonicalValue::Null);
        }
    }

    #[test]
    fn actual_masks_require_exact_length_nonempty_bits_and_zero_high_bits() {
        assert!(
            !InsertedNullMask {
                canonical_bits: Box::from([0u8]),
            }
            .has_canonical_shape(4)
        );
        assert!(
            !InsertedNullMask {
                canonical_bits: Box::from([1u8, 0]),
            }
            .has_canonical_shape(4)
        );
        assert!(
            !InsertedNullMask {
                canonical_bits: Box::from([0b1000_0000u8]),
            }
            .has_canonical_shape(4)
        );
        assert!(
            InsertedNullMask::from_positions(9, &[0, 8])
                .expect("canonical mask")
                .has_canonical_shape(9)
        );
    }

    #[test]
    fn one_actual_insertion_in_a_4096_field_schema_charges_exactly_518_bytes() {
        let mask = InsertedNullMask::from_positions(MAX_DECLARATIONS_PER_KIND, &[4095])
            .expect("maximum-width one-bit mask");
        assert_eq!(mask.semantic_bytes(), 512);
        let analysis = RecordAnalysis {
            mask: Some(mask),
            inserted_fields: 1,
            record_limit_exceeded: false,
        };
        let mut destination = Vec::new();
        let mut other = Vec::new();
        let mut combined_bytes = 0;
        let mut structural_mask_bytes = 0;
        let mut resource_limit = false;
        retain_analysis(
            analysis,
            &mut destination,
            &mut other,
            &mut combined_bytes,
            &mut structural_mask_bytes,
            &mut resource_limit,
        )
        .expect("checked maximum-width charge");
        assert_eq!(structural_mask_bytes, 512);
        assert_eq!(combined_bytes, CANONICAL_NULL_FIELD_BYTES_V1 + 512);
        assert_eq!(combined_bytes, 518);
        assert!(!resource_limit);
        assert_eq!(destination.len(), 1);
    }

    fn root_source(version: u64, optional_note: bool) -> String {
        let note = if optional_note {
            "field note: optional<string<8>>"
        } else {
            ""
        };
        format!(
            r#"
contract RootMaterialization version {version} {{
  entity Root {{
    key (tenant: uuid, root_id: uuid)
    field total: i64
    {note}
  }}
  entity Child {{
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }}
  aggregate Family {{
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant non_negative: total >= 0
  }}
  command Change {{
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, child_id) as child_row else Missing {{}}
    set child_row.amount = amount
    return Changed {{ amount: child_row.amount }}
  }}
}}
"#
        )
    }

    fn target_with_components(entity_type: EntityTypeId, values: &[[u8; 16]]) -> EntityTarget {
        let mut key = EntityKeyBuilder::new(entity_type);
        for value in values {
            key.push_uuid(value).expect("bounded UUID key");
        }
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target")
    }

    #[test]
    fn binding_and_root_masks_remain_positionally_aligned_across_both_applications() {
        let genesis_compiled =
            compile_contract_source(&root_source(1, false)).expect("root genesis");
        let successor_compiled =
            compile_contract_successor(&root_source(2, true), &genesis_compiled)
                .expect("root optional successor");
        let bundles = [genesis_compiled, successor_compiled]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect::<Vec<_>>();
        let genesis = bundles[0].clone();
        let successor = bundles[1].clone();
        let root_schema = genesis
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Root")
            .expect("root");
        let child_schema = genesis
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Child")
            .expect("child");
        let tenant = uuid(0xa1);
        let root_id = uuid(0xa2);
        let child_id = uuid(0xa3);
        let child_fields = CanonicalRecord::new(
            child_schema
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "tenant" => CanonicalValue::Uuid(tenant),
                        "root_id" => CanonicalValue::Uuid(root_id),
                        "child_id" => CanonicalValue::Uuid(child_id),
                        "amount" => CanonicalValue::I64(4),
                        unexpected => panic!("unexpected child field {unexpected}"),
                    };
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("child fields");
        let child_record = StoredEntityRecordV1::new(
            target_with_components(child_schema.id(), &[tenant, root_id, child_id]),
            EntityVersion::new(3).expect("version"),
            genesis.contract_version(),
            binding(&genesis),
            child_fields,
        )
        .expect("child record");
        let root_fields = CanonicalRecord::new(
            root_schema
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "tenant" => CanonicalValue::Uuid(tenant),
                        "root_id" => CanonicalValue::Uuid(root_id),
                        "total" => CanonicalValue::I64(4),
                        unexpected => panic!("unexpected root field {unexpected}"),
                    };
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("root fields");
        let root_record = StoredEntityRecordV1::new(
            target_with_components(root_schema.id(), &[tenant, root_id]),
            EntityVersion::new(5).expect("version"),
            genesis.contract_version(),
            binding(&genesis),
            root_fields,
        )
        .expect("root record");
        let plan = resolved_at(bundles, 1);
        assert_eq!(plan.plan().bindings().len(), 1);
        assert_eq!(plan.plan().root_validation_reads().len(), 1);
        let request = SnapshotRequest::new(
            plan.reference().clone(),
            vec![child_record.target().clone()],
            vec![root_record.target().clone()],
            Vec::new(),
        )
        .expect("snapshot request");
        let raw = ReadSnapshot::new(
            &request,
            None,
            vec![EntityObservation::Present(child_record.clone())],
            vec![EntityObservation::Present(root_record.clone())],
            Vec::new(),
        )
        .expect("raw snapshot");
        let SnapshotAnalysis::Ready(masks) = analyze_snapshot(&plan, &raw).expect("analysis")
        else {
            panic!("small root expansion fits");
        };
        assert!(masks.bindings[0].is_none());
        assert!(masks.root_validations[0].is_some());

        let CommandSnapshotMaterialization::Ready(materialized) = plan
            .materialize_command_snapshot(raw)
            .expect("root normalization")
        else {
            panic!("small root expansion fits");
        };
        let note_id = successor
            .bundle()
            .schema()
            .entity(root_schema.id())
            .expect("successor root")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "note")
            .expect("note")
            .id();
        let EntityObservation::Present(normalized_root) =
            &materialized.snapshot().root_validations()[0]
        else {
            panic!("present root");
        };
        assert!(
            normalized_root
                .fields()
                .fields()
                .binary_search_by_key(&note_id, |(id, _)| *id)
                .is_ok()
        );
        let validation_request = materialized.snapshot().validation_request();
        let raw_current = TransactionCurrentState::new(
            &validation_request,
            vec![EntityObservation::Present(child_record)],
            vec![EntityObservation::Present(root_record)],
            Vec::new(),
        )
        .expect("raw current");
        let TransactionCurrentMaterialization::Ready(current) = materialized
            .materialize_transaction_current(raw_current)
            .expect("root reapplication")
        else {
            panic!("dependencies equal");
        };
        assert_eq!(
            current.state().root_validations(),
            materialized.snapshot().root_validations()
        );
    }

    #[test]
    fn absent_root_integrity_dominates_an_earlier_binding_overflow() {
        let genesis_compiled =
            compile_contract_source(&root_source(1, false)).expect("root genesis");
        let successor_source = root_source(2, true).replace(
            "field amount: i64",
            "field amount: i64 field child_note: optional<string<8>>",
        );
        let successor_compiled = compile_contract_successor(&successor_source, &genesis_compiled)
            .expect("root and child optional successor");
        let bundles = [genesis_compiled, successor_compiled]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect::<Vec<_>>();
        let writer = bundles[0].clone();
        let child = writer
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Child")
            .expect("child");
        let root = writer
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Root")
            .expect("root");
        let tenant = uuid(0xb1);
        let root_id = uuid(0xb2);
        let child_id = uuid(0xb3);
        let physical_child_fields = |payload: usize| {
            let mut values = child
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "tenant" => CanonicalValue::Uuid(tenant),
                        "root_id" => CanonicalValue::Uuid(root_id),
                        "child_id" => CanonicalValue::Uuid(child_id),
                        "amount" => CanonicalValue::I64(4),
                        unexpected => panic!("unexpected child field {unexpected}"),
                    };
                    (field.id(), value)
                })
                .collect::<Vec<_>>();
            values.push((
                FieldId::new(UNKNOWN_FIELD).expect("unknown field"),
                CanonicalValue::bytes(vec![0; payload]).expect("bounded payload"),
            ));
            CanonicalRecord::new(values).expect("physical child fields")
        };
        let empty_size = encode_canonical_record(&physical_child_fields(0))
            .expect("empty payload encoding")
            .len();
        let desired_raw = MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1 + 1;
        let payload = desired_raw
            .checked_sub(empty_size)
            .expect("record framing leaves payload room");
        let child_fields = physical_child_fields(payload);
        assert_eq!(
            encode_canonical_record(&child_fields)
                .expect("child encoding")
                .len(),
            desired_raw
        );
        let child_record = StoredEntityRecordV1::new(
            target_with_components(child.id(), &[tenant, root_id, child_id]),
            EntityVersion::new(1).expect("version"),
            writer.contract_version(),
            binding(&writer),
            child_fields,
        )
        .expect("overflowing child source record");
        let root_target = target_with_components(root.id(), &[tenant, root_id]);
        let plan = resolved_at(bundles, 1);
        let request = SnapshotRequest::new(
            plan.reference().clone(),
            vec![child_record.target().clone()],
            vec![root_target.clone()],
            Vec::new(),
        )
        .expect("snapshot request");
        let raw = ReadSnapshot::new(
            &request,
            None,
            vec![EntityObservation::Present(child_record)],
            vec![EntityObservation::Absent(root_target)],
            Vec::new(),
        )
        .expect("structural raw snapshot");
        assert_eq!(
            plan.materialize_command_snapshot(raw)
                .expect_err("root absence must dominate earlier valid overflow")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );
    }

    #[test]
    fn later_integrity_failure_dominates_an_earlier_valid_overflow() {
        let bundles = lineage(2);
        let writer = bundles[0].clone();
        let entity_type = entity_type(&writer);
        let payload = payload_for_record_size(
            &writer,
            entity_type,
            9,
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1 + 1,
        );
        let overflow = record(&writer, entity_type, 9, 1, false, 7, Some(payload));
        let foreign_binding = DurableKeySchemaBindingV1::new(
            riffdb_types::ContractLineage::new("Foreign").expect("lineage"),
            writer.contract_version(),
            writer.bundle_hash(),
        );
        let foreign = StoredEntityRecordV1::new(
            target(entity_type, 10),
            EntityVersion::new(1).expect("version"),
            writer.contract_version(),
            foreign_binding,
            fields(&writer, entity_type, 10, false, 7, None),
        )
        .expect("foreign structural record");
        let schema = writer
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record();
        let value_id = schema
            .fields()
            .iter()
            .find(|field| field.name() == "value")
            .expect("value field")
            .id();
        let mut wrong_type_fields = fields(&writer, entity_type, 11, false, 7, None)
            .fields()
            .to_vec();
        let value_index = wrong_type_fields
            .binary_search_by_key(&value_id, |(id, _)| *id)
            .expect("value position");
        wrong_type_fields[value_index].1 = CanonicalValue::Bool(true);
        let wrong_type = StoredEntityRecordV1::new(
            target(entity_type, 11),
            EntityVersion::new(1).expect("version"),
            writer.contract_version(),
            binding(&writer),
            CanonicalRecord::new(wrong_type_fields).expect("wrong-type physical record"),
        )
        .expect("structural wrong-type record");

        let id_field = schema
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("id field")
            .id();
        let mut mismatched_key_fields = fields(&writer, entity_type, 12, false, 7, None)
            .fields()
            .to_vec();
        let id_index = mismatched_key_fields
            .binary_search_by_key(&id_field, |(id, _)| *id)
            .expect("id position");
        mismatched_key_fields[id_index].1 = CanonicalValue::Uuid(uuid(13));
        let mismatched_key = StoredEntityRecordV1::new(
            target(entity_type, 12),
            EntityVersion::new(1).expect("version"),
            writer.contract_version(),
            binding(&writer),
            CanonicalRecord::new(mismatched_key_fields).expect("mismatched key fields"),
        )
        .expect("structural key-mismatch record");

        for (case, invalid) in [
            ("foreign writer", foreign),
            ("wrong known-field type", wrong_type),
            ("primary-key field mismatch", mismatched_key),
        ] {
            let plan = resolved_at(bundles.clone(), 1);
            let raw = snapshot(
                &plan,
                vec![
                    EntityObservation::Present(overflow.clone()),
                    EntityObservation::Present(invalid),
                ],
            );
            assert_eq!(
                plan.materialize_command_snapshot(raw)
                    .expect_err(case)
                    .kind(),
                CommandSnapshotMaterializationErrorKind::Integrity,
                "{case} must dominate an earlier valid overflow"
            );
        }
    }

    fn enum_source(version: u64, optional_note: bool) -> String {
        let note = if optional_note {
            "field note: optional<string<8>>"
        } else {
            ""
        };
        format!(
            r#"
contract EnumMaterialization version {version} {{
  enum State {{ Open, Closed }}
  entity Row {{
    key (tenant: uuid, id: uuid)
    field value: i64
    field status: State
    field history: list<State, 4>
    {note}
  }}
  aggregate Rows {{ root Row partition_by tenant conflict_key (tenant) }}
  command Observe {{
    input tenant: uuid
    input first_id: uuid
    input second_id: uuid
    read Row(tenant, first_id) as first else MissingFirst {{}}
    read Row(tenant, second_id) as second else MissingSecond {{}}
    return Found {{ value: first.value }}
  }}
}}
"#
        )
    }

    #[test]
    fn enum_membership_and_nested_list_validation_dominate_earlier_overflow() {
        let genesis_compiled =
            compile_contract_source(&enum_source(1, false)).expect("enum genesis");
        let successor_compiled =
            compile_contract_successor(&enum_source(2, true), &genesis_compiled)
                .expect("enum optional successor");
        let bundles = [genesis_compiled, successor_compiled]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect::<Vec<_>>();
        let writer = bundles[0].clone();
        let row = writer.bundle().schema().entities().first().expect("row");
        let state = writer
            .bundle()
            .schema()
            .enums()
            .first()
            .expect("state enum");
        let valid_variant = state.variants().first().expect("Open").id();
        let invalid_variant = riffdb_types::EnumVariantId::new(u32::MAX).expect("invalid variant");
        assert!(!state.contains_variant(invalid_variant));
        let physical_fields = |seed: u8,
                               direct_variant: riffdb_types::EnumVariantId,
                               nested_variant: riffdb_types::EnumVariantId,
                               payload: Option<usize>| {
            let mut values = row
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "tenant" => CanonicalValue::Uuid(uuid(0xaa)),
                        "id" => CanonicalValue::Uuid(uuid(seed)),
                        "value" => CanonicalValue::I64(7),
                        "status" => CanonicalValue::Enum {
                            type_id: state.id(),
                            variant_id: direct_variant,
                        },
                        "history" => CanonicalValue::list(vec![CanonicalValue::Enum {
                            type_id: state.id(),
                            variant_id: nested_variant,
                        }])
                        .expect("bounded history"),
                        unexpected => panic!("unexpected enum fixture field {unexpected}"),
                    };
                    (field.id(), value)
                })
                .collect::<Vec<_>>();
            if let Some(payload) = payload {
                values.push((
                    FieldId::new(UNKNOWN_FIELD).expect("unknown field"),
                    CanonicalValue::bytes(vec![0; payload]).expect("bounded payload"),
                ));
            }
            CanonicalRecord::new(values).expect("enum physical fields")
        };
        let empty = physical_fields(21, valid_variant, valid_variant, Some(0));
        let empty_size = encode_canonical_record(&empty).expect("encoding").len();
        let desired_raw = MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1 + 1;
        let payload = desired_raw.checked_sub(empty_size).expect("payload room");
        let overflow_fields = physical_fields(21, valid_variant, valid_variant, Some(payload));
        let overflow = StoredEntityRecordV1::new(
            target(row.id(), 21),
            EntityVersion::new(1).expect("version"),
            writer.contract_version(),
            binding(&writer),
            overflow_fields,
        )
        .expect("overflow record");

        for (case, direct_variant, nested_variant) in [
            ("direct invalid enum", invalid_variant, valid_variant),
            ("nested invalid enum", valid_variant, invalid_variant),
        ] {
            let invalid = StoredEntityRecordV1::new(
                target(row.id(), 22),
                EntityVersion::new(1).expect("version"),
                writer.contract_version(),
                binding(&writer),
                physical_fields(22, direct_variant, nested_variant, None),
            )
            .expect("structural invalid-enum record");
            let plan = resolved_at(bundles.clone(), 1);
            let raw = snapshot(
                &plan,
                vec![
                    EntityObservation::Present(overflow.clone()),
                    EntityObservation::Present(invalid),
                ],
            );
            assert_eq!(
                plan.materialize_command_snapshot(raw)
                    .expect_err(case)
                    .kind(),
                CommandSnapshotMaterializationErrorKind::Integrity,
                "{case} must dominate an earlier valid overflow"
            );
        }
    }

    #[test]
    fn per_record_exact_limit_succeeds_and_equal_plus_one_retains_only_raw_evidence() {
        let bundles = lineage(1);
        let writer = bundles[0].clone();
        let entity_type = entity_type(&writer);
        let exact_payload = payload_for_record_size(
            &writer,
            entity_type,
            7,
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1,
        );
        let exact_record = record(&writer, entity_type, 7, 1, false, 7, Some(exact_payload));
        let exact_plan = resolved_at(bundles.clone(), 1);
        let exact_snapshot = snapshot(&exact_plan, vec![EntityObservation::Present(exact_record)]);
        let CommandSnapshotMaterialization::Ready(exact) = exact_plan
            .materialize_command_snapshot(exact_snapshot)
            .expect("exact record limit")
        else {
            panic!("exact record limit must be accepted");
        };
        let EntityObservation::Present(exact_record) = &exact.snapshot().bindings()[0] else {
            panic!("present record");
        };
        assert_eq!(
            encode_canonical_record(exact_record.fields())
                .expect("normalized encoding")
                .len(),
            MAX_CANONICAL_DOCUMENT_BYTES
        );

        let overflow_payload = payload_for_record_size(
            &writer,
            entity_type,
            8,
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1 + 1,
        );
        let overflow_record = record(&writer, entity_type, 8, 1, false, 7, Some(overflow_payload));
        let overflow_plan = resolved_at(bundles, 1);
        let raw_snapshot = snapshot(
            &overflow_plan,
            vec![EntityObservation::Present(overflow_record.clone())],
        );
        let raw_bytes = raw_snapshot.semantic_bytes();
        let CommandSnapshotMaterialization::ResourceLimit(evidence) = overflow_plan
            .materialize_command_snapshot(raw_snapshot)
            .expect("valid over-limit expansion")
        else {
            panic!("one byte over record limit must retain resource evidence");
        };
        assert_eq!(evidence.raw_snapshot().semantic_bytes(), raw_bytes);
        let raw_current = current(
            evidence.raw_snapshot(),
            vec![EntityObservation::Present(overflow_record.clone())],
        );
        assert_eq!(
            evidence
                .recheck_transaction_current(raw_current)
                .expect("same raw evidence"),
            ResourceLimitRecheck::Confirmed
        );

        let changed = record(&writer, entity_type, 8, 2, false, 7, Some(overflow_payload));
        let changed = current(
            evidence.raw_snapshot(),
            vec![EntityObservation::Present(changed)],
        );
        assert_eq!(
            evidence
                .recheck_transaction_current(changed)
                .expect("dependency change"),
            ResourceLimitRecheck::DependencyChanged
        );

        let drift = record(&writer, entity_type, 8, 1, false, 8, Some(overflow_payload));
        let drift = current(
            evidence.raw_snapshot(),
            vec![EntityObservation::Present(drift)],
        );
        assert_eq!(
            evidence
                .recheck_transaction_current(drift)
                .expect_err("same-version raw resource evidence drift is integrity")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );
        let wrong_binding = StoredEntityRecordV1::new(
            overflow_record.target().clone(),
            overflow_record.entity_version(),
            overflow_record.written_by_contract(),
            DurableKeySchemaBindingV1::new(
                writer.lineage().clone(),
                writer.contract_version(),
                riffdb_types::ContractBundleHash::from_bytes([0xe1; 32]),
            ),
            overflow_record.fields().clone(),
        )
        .expect("wrong-binding resource record");
        let wrong_binding = current(
            evidence.raw_snapshot(),
            vec![EntityObservation::Present(wrong_binding)],
        );
        assert_eq!(
            evidence
                .recheck_transaction_current(wrong_binding)
                .expect_err("equal-version resource schema drift is integrity")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );
        assert_eq!(
            format!("{evidence:?}"),
            "CommandSnapshotResourceLimitEvidence([REDACTED])"
        );
    }

    fn aggregate_snapshot(
        bundles: &[ValidatedContractBundle],
        last_payload_adjustment: usize,
    ) -> (ResolvedExecutablePlan, ReadSnapshot, Vec<EntityObservation>) {
        const POSITIONS: usize = 16;
        let writer = &bundles[0];
        let entity_type = entity_type(writer);
        let large_payload = payload_for_record_size(
            writer,
            entity_type,
            20,
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_NULL_FIELD_BYTES_V1,
        );
        let plan = resolved_at(bundles.to_vec(), 1);
        let mut observations = (0..POSITIONS - 1)
            .map(|index| {
                EntityObservation::Present(record(
                    writer,
                    entity_type,
                    20 + u8::try_from(index).expect("seed"),
                    1,
                    false,
                    7,
                    Some(large_payload),
                ))
            })
            .collect::<Vec<_>>();
        observations.push(EntityObservation::Present(record(
            writer,
            entity_type,
            40,
            1,
            false,
            7,
            Some(0),
        )));
        let baseline = snapshot(&plan, observations.clone());
        let mask_bytes = POSITIONS;
        let inserted_bytes = POSITIONS * CANONICAL_NULL_FIELD_BYTES_V1;
        let desired_raw = riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES - mask_bytes - inserted_bytes
            + last_payload_adjustment;
        let final_payload = desired_raw
            .checked_sub(baseline.semantic_bytes())
            .expect("baseline leaves room for the final payload");
        observations[POSITIONS - 1] = EntityObservation::Present(record(
            writer,
            entity_type,
            40,
            1,
            false,
            7,
            Some(final_payload),
        ));
        let snapshot = snapshot(&plan, observations.clone());
        assert_eq!(snapshot.semantic_bytes(), desired_raw);
        (plan, snapshot, observations)
    }

    #[test]
    fn combined_snapshot_and_exact_actual_masks_freeze_sixteen_mib_boundary() {
        let bundles = lineage(16);
        let (exact_plan, exact_snapshot, _) = aggregate_snapshot(&bundles, 0);
        let CommandSnapshotMaterialization::Ready(exact) = exact_plan
            .materialize_command_snapshot(exact_snapshot)
            .expect("exact combined limit")
        else {
            panic!("exactly 16 MiB must be accepted");
        };
        assert_eq!(
            exact.snapshot().semantic_bytes() + 16,
            riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES
        );

        let (overflow_plan, overflow_snapshot, observations) = aggregate_snapshot(&bundles, 1);
        let CommandSnapshotMaterialization::ResourceLimit(evidence) = overflow_plan
            .materialize_command_snapshot(overflow_snapshot)
            .expect("valid aggregate overflow")
        else {
            panic!("combined limit plus one must reject");
        };
        let current = current(evidence.raw_snapshot(), observations);
        assert_eq!(
            evidence
                .recheck_transaction_current(current)
                .expect("deterministic overflow reproduction"),
            ResourceLimitRecheck::Confirmed
        );

        let (malformed_plan, _, mut malformed_observations) =
            aggregate_snapshot(&bundles, 16 * 7 - 1);
        let writer = &bundles[0];
        let entity_type = entity_type(writer);
        let id_field = writer
            .bundle()
            .schema()
            .entity(entity_type)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("id field")
            .id();
        let EntityObservation::Present(last) = &malformed_observations[15] else {
            panic!("present last observation");
        };
        let mut mismatched_fields = last.fields().fields().to_vec();
        let id_index = mismatched_fields
            .binary_search_by_key(&id_field, |(id, _)| *id)
            .expect("id position");
        mismatched_fields[id_index].1 = CanonicalValue::Uuid(uuid(41));
        let mismatched = StoredEntityRecordV1::new(
            last.target().clone(),
            last.entity_version(),
            last.written_by_contract(),
            last.schema_binding().clone(),
            CanonicalRecord::new(mismatched_fields).expect("mismatched fields"),
        )
        .expect("structural mismatch");
        malformed_observations[15] = EntityObservation::Present(mismatched);
        let malformed_snapshot = snapshot(&malformed_plan, malformed_observations);
        assert_eq!(
            malformed_snapshot.semantic_bytes(),
            riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES - 1
        );
        assert_eq!(
            malformed_plan
                .materialize_command_snapshot(malformed_snapshot)
                .expect_err("later key mismatch must dominate aggregate overflow")
                .kind(),
            CommandSnapshotMaterializationErrorKind::Integrity
        );
    }
}
