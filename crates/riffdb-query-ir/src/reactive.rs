//! Canonical typed plans for reactive application operations.

use riffdb_types::{
    CommandId, ContractBundleHash, ContractLineage, ContractVersion, EventTypeId, FieldId,
    QueryCostVectorV1, QueryModuleHash, QueryPlanHash, ReactiveModuleHash, ReactiveOperationHash,
    ReactiveOperationName, ReactiveSourceHash, hash_reactive_module, hash_reactive_operation,
};

/// Reactive typed-IR version.
pub const REACTIVE_IR_VERSION_V1: u32 = 1;
/// Canonical reactive-module format version.
pub const REACTIVE_MODULE_FORMAT_VERSION_V1: u32 = 1;
/// Maximum canonical module bytes.
pub const MAX_REACTIVE_MODULE_BYTES: usize = 16 * 1_024 * 1_024;

const MODULE_MAGIC: &[u8] = b"RIFFDB-REACTIVE-MODULE\0";
const OPERATION_MAGIC: &[u8] = b"RIFFDB-REACTIVE-OPERATION\0";

/// One compiler-resolved operation parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveParameterV1 {
    name: String,
    type_name: String,
}

impl ReactiveParameterV1 {
    /// Constructs one checked parameter.
    #[doc(hidden)]
    pub fn checked(name: String, type_name: String) -> Option<Self> {
        (valid_name(&name) && !type_name.is_empty() && type_name.len() <= 512)
            .then_some(Self { name, type_name })
    }
    /// Parameter name without `$`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Canonical resolved type name.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

/// One event payload field resolved against an exact contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveEventFieldV1 {
    name: String,
    field_id: FieldId,
    type_name: String,
}

impl ReactiveEventFieldV1 {
    /// Constructs one checked selected field.
    #[doc(hidden)]
    pub fn checked(name: String, field_id: FieldId, type_name: String) -> Option<Self> {
        (valid_name(&name) && !type_name.is_empty()).then_some(Self {
            name,
            field_id,
            type_name,
        })
    }
    /// Field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable compiler identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> FieldId {
        self.field_id
    }
    /// Canonical type.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

/// One selected event type and its explicit field projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveEventV1 {
    name: String,
    event_type_id: EventTypeId,
    fields: Vec<ReactiveEventFieldV1>,
}

impl ReactiveEventV1 {
    /// Constructs one checked event selection.
    #[doc(hidden)]
    pub fn checked(
        name: String,
        event_type_id: EventTypeId,
        mut fields: Vec<ReactiveEventFieldV1>,
    ) -> Option<Self> {
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        if !valid_name(&name)
            || fields.is_empty()
            || fields.len() > 256
            || fields.windows(2).any(|pair| pair[0].name >= pair[1].name)
        {
            return None;
        }
        Some(Self {
            name,
            event_type_id,
            fields,
        })
    }
    /// Event name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable event identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> EventTypeId {
        self.event_type_id
    }
    /// Fields in canonical name order.
    #[must_use]
    pub fn fields(&self) -> &[ReactiveEventFieldV1] {
        &self.fields
    }
}

/// One ordered event-partition binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactivePartitionBindingV1 {
    field: String,
    parameter: String,
    type_name: String,
}

impl ReactivePartitionBindingV1 {
    /// Constructs one compiler-proved partition binding.
    #[doc(hidden)]
    pub fn checked(field: String, parameter: String, type_name: String) -> Option<Self> {
        (valid_name(&field) && valid_name(&parameter) && !type_name.is_empty()).then_some(Self {
            field,
            parameter,
            type_name,
        })
    }
    /// Event partition field.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
    /// Bound input parameter.
    #[must_use]
    pub fn parameter(&self) -> &str {
        &self.parameter
    }
    /// Canonical type.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

/// Closed typed predicate opcode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactivePredicateNodeV1 {
    /// Pushes one event field and its canonical type.
    EventField(String, String),
    /// Pushes one parameter and its canonical type.
    Parameter(String, String),
    /// Pushes a canonical literal encoding and its canonical type.
    Literal(String, Vec<u8>),
    /// Equality.
    Equal,
    /// Inequality.
    NotEqual,
    /// Less than.
    Less,
    /// Less than or equal.
    LessEqual,
    /// Greater than.
    Greater,
    /// Greater than or equal.
    GreaterEqual,
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
}

/// One exact named-query dependency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveQueryDependencyV1 {
    name: String,
    module_hash: QueryModuleHash,
    query_name: String,
    plan_hash: QueryPlanHash,
    arguments: Vec<(String, String)>,
    cost: QueryCostVectorV1,
}

impl ReactiveQueryDependencyV1 {
    /// Constructs one compiler-checked query dependency.
    #[doc(hidden)]
    pub fn checked(
        name: String,
        module_hash: QueryModuleHash,
        query_name: String,
        plan_hash: QueryPlanHash,
        mut arguments: Vec<(String, String)>,
        cost: QueryCostVectorV1,
    ) -> Option<Self> {
        arguments.sort();
        if !valid_name(&name)
            || !valid_name(&query_name)
            || arguments.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        {
            return None;
        }
        Some(Self {
            name,
            module_hash,
            query_name,
            plan_hash,
            arguments,
            cost,
        })
    }
    /// Dependency-local name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Exact module identity.
    #[must_use]
    pub const fn module_hash(&self) -> QueryModuleHash {
        self.module_hash
    }
    /// Exact query name.
    #[must_use]
    pub fn query_name(&self) -> &str {
        &self.query_name
    }
    /// Exact query plan.
    #[must_use]
    pub const fn plan_hash(&self) -> QueryPlanHash {
        self.plan_hash
    }
    /// Canonical argument source descriptions.
    #[must_use]
    pub fn arguments(&self) -> &[(String, String)] {
        &self.arguments
    }
    /// Whole-query upper bound.
    #[must_use]
    pub const fn cost(&self) -> QueryCostVectorV1 {
        self.cost
    }
}

/// One named command reaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCommandDependencyV1 {
    reaction_name: String,
    command_name: String,
    command_id: CommandId,
}

impl ReactiveCommandDependencyV1 {
    /// Constructs one compiler-resolved reaction.
    #[doc(hidden)]
    pub fn checked(
        reaction_name: String,
        command_name: String,
        command_id: CommandId,
    ) -> Option<Self> {
        (valid_name(&reaction_name) && valid_name(&command_name)).then_some(Self {
            reaction_name,
            command_name,
            command_id,
        })
    }
    /// Retry-stable reaction name.
    #[must_use]
    pub fn reaction_name(&self) -> &str {
        &self.reaction_name
    }
    /// Target command.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }
    /// Stable command identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_command_id(&self) -> CommandId {
        self.command_id
    }
}

/// Exact contextual delivery bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReactiveDeliveryLimitsV1 {
    batch: u8,
    in_flight: u8,
    lease_seconds: u16,
}
impl ReactiveDeliveryLimitsV1 {
    /// Constructs accepted P8 bounds.
    #[must_use]
    pub const fn new(batch: u8, in_flight: u8, lease_seconds: u16) -> Option<Self> {
        if batch == 0
            || batch > 8
            || in_flight == 0
            || in_flight > 8
            || lease_seconds < 5
            || lease_seconds > 900
        {
            return None;
        }
        Some(Self {
            batch,
            in_flight,
            lease_seconds,
        })
    }
    /// Batch bound.
    #[must_use]
    pub const fn batch(self) -> u8 {
        self.batch
    }
    /// In-flight bound.
    #[must_use]
    pub const fn in_flight(self) -> u8 {
        self.in_flight
    }
    /// Lease seconds.
    #[must_use]
    pub const fn lease_seconds(self) -> u16 {
        self.lease_seconds
    }
}

/// Public update behavior requested by one watch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReactiveUpdateModeV1 {
    /// Complete-key patches.
    Patch,
    /// Complete resets.
    Reset,
}

/// Closed typed operation plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveOperationPlanV1 {
    /// Partition-local event stream.
    Stream {
        /// Typed parameters.
        parameters: Vec<ReactiveParameterV1>,
        /// Complete ordered partition.
        partition: Vec<ReactivePartitionBindingV1>,
        /// Explicit events and fields.
        events: Vec<ReactiveEventV1>,
        /// Optional typed postfix predicate.
        predicate: Vec<ReactivePredicateNodeV1>,
    },
    /// Exact named query watch.
    Watch {
        /// Typed parameters.
        parameters: Vec<ReactiveParameterV1>,
        /// Exact query dependency.
        query: ReactiveQueryDependencyV1,
        /// Public update mode.
        update_mode: ReactiveUpdateModeV1,
        /// Complete public key in patch mode.
        patch_key: Vec<String>,
    },
    /// Contextual agent subscription.
    Subscription {
        /// Typed parameters.
        parameters: Vec<ReactiveParameterV1>,
        /// Exact stream operation name.
        stream_name: ReactiveOperationName,
        /// Exact stream plan identity.
        stream_hash: ReactiveOperationHash,
        /// Canonical stream argument sources.
        stream_arguments: Vec<(String, String)>,
        /// Bounded hydration queries.
        hydrations: Vec<ReactiveQueryDependencyV1>,
        /// Explicit command reactions.
        reactions: Vec<ReactiveCommandDependencyV1>,
        /// Delivery limits.
        limits: ReactiveDeliveryLimitsV1,
    },
}

/// One canonical reactive operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledReactiveOperationV1 {
    name: ReactiveOperationName,
    plan: ReactiveOperationPlanV1,
    canonical_bytes: Vec<u8>,
    identity: ReactiveOperationHash,
}

impl CompiledReactiveOperationV1 {
    /// Encodes and hashes one compiler-checked plan.
    #[doc(hidden)]
    pub fn checked(name: ReactiveOperationName, plan: ReactiveOperationPlanV1) -> Option<Self> {
        let canonical_bytes = encode_operation(&name, &plan)?;
        let identity = hash_reactive_operation(&canonical_bytes);
        Some(Self {
            name,
            plan,
            canonical_bytes,
            identity,
        })
    }
    /// Operation name.
    #[must_use]
    pub const fn name(&self) -> &ReactiveOperationName {
        &self.name
    }
    /// Typed plan.
    #[must_use]
    pub const fn plan(&self) -> &ReactiveOperationPlanV1 {
        &self.plan
    }
    /// Canonical bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    /// Exact operation identity.
    #[must_use]
    pub const fn identity(&self) -> ReactiveOperationHash {
        self.identity
    }
}

/// One exact immutable reactive module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveModulePlanV1 {
    name: String,
    version: u64,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    source_hash: ReactiveSourceHash,
    operations: Vec<CompiledReactiveOperationV1>,
    canonical_bytes: Vec<u8>,
    identity: ReactiveModuleHash,
}

impl ReactiveModulePlanV1 {
    /// Constructs one exact compiler-owned module.
    #[doc(hidden)]
    pub fn checked(
        name: String,
        version: u64,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_hash: ContractBundleHash,
        source_hash: ReactiveSourceHash,
        mut operations: Vec<CompiledReactiveOperationV1>,
    ) -> Option<Self> {
        if !valid_name(&name) || version == 0 || operations.is_empty() {
            return None;
        }
        operations.sort_by(|left, right| left.name.cmp(&right.name));
        if operations
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
        {
            return None;
        }
        let canonical_bytes = encode_module(
            &name,
            version,
            &contract_lineage,
            contract_version,
            contract_hash,
            source_hash,
            &operations,
        )?;
        let identity = hash_reactive_module(&canonical_bytes);
        Some(Self {
            name,
            version,
            contract_lineage,
            contract_version,
            contract_hash,
            source_hash,
            operations,
            canonical_bytes,
            identity,
        })
    }
    /// Module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Positive module version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }
    /// Exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }
    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }
    /// Exact contract hash.
    #[must_use]
    pub const fn contract_hash(&self) -> ContractBundleHash {
        self.contract_hash
    }
    /// Exact source hash.
    #[must_use]
    pub const fn source_hash(&self) -> ReactiveSourceHash {
        self.source_hash
    }
    /// Operations in canonical name order.
    #[must_use]
    pub fn operations(&self) -> &[CompiledReactiveOperationV1] {
        &self.operations
    }
    /// Exact operation lookup.
    #[must_use]
    pub fn operation(&self, name: &str) -> Option<&CompiledReactiveOperationV1> {
        self.operations
            .binary_search_by(|operation| operation.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.operations[index])
    }
    /// Canonical module bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    /// Exact module identity.
    #[must_use]
    pub const fn identity(&self) -> ReactiveModuleHash {
        self.identity
    }
}

fn encode_operation(
    name: &ReactiveOperationName,
    plan: &ReactiveOperationPlanV1,
) -> Option<Vec<u8>> {
    let mut bytes = OPERATION_MAGIC.to_vec();
    bytes.extend_from_slice(&REACTIVE_IR_VERSION_V1.to_be_bytes());
    text(&mut bytes, name.as_str())?;
    match plan {
        ReactiveOperationPlanV1::Stream {
            parameters,
            partition,
            events,
            predicate,
        } => {
            bytes.push(1);
            parameters_into(&mut bytes, parameters)?;
            count(&mut bytes, partition.len())?;
            for value in partition {
                text(&mut bytes, &value.field)?;
                text(&mut bytes, &value.parameter)?;
                text(&mut bytes, &value.type_name)?;
            }
            count(&mut bytes, events.len())?;
            for event in events {
                text(&mut bytes, &event.name)?;
                bytes.extend_from_slice(&event.event_type_id.to_be_bytes());
                count(&mut bytes, event.fields.len())?;
                for field in &event.fields {
                    text(&mut bytes, &field.name)?;
                    bytes.extend_from_slice(&field.field_id.to_be_bytes());
                    text(&mut bytes, &field.type_name)?;
                }
            }
            count(&mut bytes, predicate.len())?;
            for node in predicate {
                predicate_node(&mut bytes, node)?;
            }
        }
        ReactiveOperationPlanV1::Watch {
            parameters,
            query,
            update_mode,
            patch_key,
        } => {
            bytes.push(2);
            parameters_into(&mut bytes, parameters)?;
            query_into(&mut bytes, query)?;
            bytes.push(match update_mode {
                ReactiveUpdateModeV1::Patch => 1,
                ReactiveUpdateModeV1::Reset => 2,
            });
            strings(&mut bytes, patch_key)?;
        }
        ReactiveOperationPlanV1::Subscription {
            parameters,
            stream_name,
            stream_hash,
            stream_arguments,
            hydrations,
            reactions,
            limits,
        } => {
            bytes.push(3);
            parameters_into(&mut bytes, parameters)?;
            text(&mut bytes, stream_name.as_str())?;
            bytes.extend_from_slice(stream_hash.as_bytes());
            pairs(&mut bytes, stream_arguments)?;
            count(&mut bytes, hydrations.len())?;
            for query in hydrations {
                query_into(&mut bytes, query)?;
            }
            count(&mut bytes, reactions.len())?;
            for value in reactions {
                text(&mut bytes, &value.reaction_name)?;
                text(&mut bytes, &value.command_name)?;
                bytes.extend_from_slice(&value.command_id.to_be_bytes());
            }
            bytes.push(limits.batch);
            bytes.push(limits.in_flight);
            bytes.extend_from_slice(&limits.lease_seconds.to_be_bytes());
        }
    }
    (bytes.len() <= MAX_REACTIVE_MODULE_BYTES).then_some(bytes)
}

fn encode_module(
    name: &str,
    version: u64,
    lineage: &ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    source_hash: ReactiveSourceHash,
    operations: &[CompiledReactiveOperationV1],
) -> Option<Vec<u8>> {
    let mut bytes = MODULE_MAGIC.to_vec();
    bytes.extend_from_slice(&REACTIVE_MODULE_FORMAT_VERSION_V1.to_be_bytes());
    bytes.extend_from_slice(&REACTIVE_IR_VERSION_V1.to_be_bytes());
    text(&mut bytes, name)?;
    bytes.extend_from_slice(&version.to_be_bytes());
    text(&mut bytes, lineage.as_str())?;
    bytes.extend_from_slice(&contract_version.get().to_be_bytes());
    bytes.extend_from_slice(contract_hash.as_bytes());
    bytes.extend_from_slice(source_hash.as_bytes());
    count(&mut bytes, operations.len())?;
    for operation in operations {
        blob(&mut bytes, operation.canonical_bytes())?;
        bytes.extend_from_slice(operation.identity.as_bytes());
    }
    (bytes.len() <= MAX_REACTIVE_MODULE_BYTES).then_some(bytes)
}
fn parameters_into(bytes: &mut Vec<u8>, values: &[ReactiveParameterV1]) -> Option<()> {
    count(bytes, values.len())?;
    for value in values {
        text(bytes, &value.name)?;
        text(bytes, &value.type_name)?;
    }
    Some(())
}
fn query_into(bytes: &mut Vec<u8>, value: &ReactiveQueryDependencyV1) -> Option<()> {
    text(bytes, &value.name)?;
    bytes.extend_from_slice(value.module_hash.as_bytes());
    text(bytes, &value.query_name)?;
    bytes.extend_from_slice(value.plan_hash.as_bytes());
    pairs(bytes, &value.arguments)?;
    cost(bytes, value.cost);
    Some(())
}
fn predicate_node(bytes: &mut Vec<u8>, value: &ReactivePredicateNodeV1) -> Option<()> {
    match value {
        ReactivePredicateNodeV1::EventField(name, ty) => {
            bytes.push(1);
            text(bytes, name)?;
            text(bytes, ty)?;
        }
        ReactivePredicateNodeV1::Parameter(name, ty) => {
            bytes.push(2);
            text(bytes, name)?;
            text(bytes, ty)?;
        }
        ReactivePredicateNodeV1::Literal(ty, value) => {
            bytes.push(3);
            text(bytes, ty)?;
            blob(bytes, value)?;
        }
        ReactivePredicateNodeV1::Equal => bytes.push(4),
        ReactivePredicateNodeV1::NotEqual => bytes.push(5),
        ReactivePredicateNodeV1::Less => bytes.push(6),
        ReactivePredicateNodeV1::LessEqual => bytes.push(7),
        ReactivePredicateNodeV1::Greater => bytes.push(8),
        ReactivePredicateNodeV1::GreaterEqual => bytes.push(9),
        ReactivePredicateNodeV1::And => bytes.push(10),
        ReactivePredicateNodeV1::Or => bytes.push(11),
    }
    Some(())
}
fn cost(bytes: &mut Vec<u8>, value: QueryCostVectorV1) {
    for part in [
        value.access_steps(),
        value.scanned_index_rows(),
        value.point_reads(),
        value.dependent_keys(),
        value.intermediate_rows(),
        value.projected_values(),
        value.encoded_result_bytes(),
    ] {
        bytes.extend_from_slice(&part.to_be_bytes());
    }
}
fn strings(bytes: &mut Vec<u8>, values: &[String]) -> Option<()> {
    count(bytes, values.len())?;
    for value in values {
        text(bytes, value)?;
    }
    Some(())
}
fn pairs(bytes: &mut Vec<u8>, values: &[(String, String)]) -> Option<()> {
    count(bytes, values.len())?;
    for (left, right) in values {
        text(bytes, left)?;
        text(bytes, right)?;
    }
    Some(())
}
fn text(bytes: &mut Vec<u8>, value: &str) -> Option<()> {
    blob(bytes, value.as_bytes())
}
fn blob(bytes: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    count(bytes, value.len())?;
    bytes.extend_from_slice(value);
    (bytes.len() <= MAX_REACTIVE_MODULE_BYTES).then_some(())
}
fn count(bytes: &mut Vec<u8>, value: usize) -> Option<()> {
    bytes.extend_from_slice(&u32::try_from(value).ok()?.to_be_bytes());
    Some(())
}
fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || index > 0 && byte.is_ascii_digit()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_delivery_bounds_are_closed() {
        assert!(ReactiveDeliveryLimitsV1::new(1, 1, 5).is_some());
        assert!(ReactiveDeliveryLimitsV1::new(8, 8, 900).is_some());
        assert!(ReactiveDeliveryLimitsV1::new(9, 8, 900).is_none());
        assert!(ReactiveDeliveryLimitsV1::new(8, 9, 900).is_none());
        assert!(ReactiveDeliveryLimitsV1::new(8, 8, 901).is_none());
    }
}
