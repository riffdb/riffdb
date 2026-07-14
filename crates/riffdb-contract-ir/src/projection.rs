//! Checked projection grouping and executable incremental plans.

use riffdb_types::{
    CanonicalValue, ContractLineage, EnumVariantId, EventTypeId, MAX_KEY_BYTES,
    MAX_PROJECTION_GROUP_COMPONENTS, MAX_PROJECTION_STATE_SEMANTIC_BYTES, ProjectionGeneration,
    ProjectionGroupKey, ProjectionGroupKeyBuilder, ProjectionGroupPrefix,
    ProjectionGroupPrefixBuilder, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
};

use crate::{
    ExprId, ExpressionArena, ExpressionKind, FieldSchema, IrValidationError, RecordSchema,
    RecordTypeRef, SchemaIr, ValueType, ValueTypeTag, checked_len,
};

/// Immutable projection group codec version.
pub const PROJECTION_GROUP_CODEC_VERSION_V1: u32 = 1;

/// One exact projection group component type and enum registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionGroupComponentSchema {
    value_type: ValueType,
    enum_variants: Vec<EnumVariantId>,
    maximum_framed_bytes: usize,
}

impl ProjectionGroupComponentSchema {
    /// Creates a checked group component.
    pub fn new(
        value_type: ValueType,
        mut enum_variants: Vec<EnumVariantId>,
    ) -> Result<Self, IrValidationError> {
        if !value_type.is_projection_group_scalar() {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection group component is not a nonoptional scalar",
            });
        }
        enum_variants.sort_unstable();
        if enum_variants.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "projection enum variants",
            });
        }
        if value_type.tag() == ValueTypeTag::Enum {
            if enum_variants.is_empty() {
                return Err(IrValidationError::Empty {
                    kind: "projection enum variants",
                });
            }
        } else if !enum_variants.is_empty() {
            return Err(IrValidationError::InvalidProjection {
                reason: "non-enum projection component carries variants",
            });
        }
        let canonical =
            value_type
                .maximum_canonical_bytes()?
                .ok_or(IrValidationError::InvalidProjection {
                    reason: "projection component maximum is not statically known",
                })?;
        let maximum_framed_bytes =
            canonical
                .checked_add(4)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "projection group component",
                })?;
        Ok(Self {
            value_type,
            enum_variants,
            maximum_framed_bytes,
        })
    }

    /// Complete static type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Canonical allowed enum variants.
    #[must_use]
    pub fn enum_variants(&self) -> &[EnumVariantId] {
        &self.enum_variants
    }

    /// Four-byte length plus maximum complete canonical value bytes.
    #[must_use]
    pub const fn maximum_framed_bytes(&self) -> usize {
        self.maximum_framed_bytes
    }

    fn validate(&self, value: &CanonicalValue) -> Result<(), IrValidationError> {
        self.value_type.validate_value(value)?;
        if let CanonicalValue::Enum { variant_id, .. } = value
            && self.enum_variants.binary_search(variant_id).is_err()
        {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection enum component has an undeclared variant",
            });
        }
        Ok(())
    }
}

/// Immutable projection group and measure schema before plan hashing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionGroupSchema {
    projection_id: ProjectionId,
    codec_version: u32,
    group_components: Vec<ProjectionGroupComponentSchema>,
    measures: RecordSchema,
    maximum_complete_key_bytes: usize,
    maximum_stored_state_bytes: usize,
}

impl ProjectionGroupSchema {
    /// Creates a checked schema and performs complete key/state maximum analysis.
    pub fn new(
        projection_id: ProjectionId,
        group_components: Vec<ProjectionGroupComponentSchema>,
        measures: RecordSchema,
    ) -> Result<Self, IrValidationError> {
        if group_components.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "projection group components",
            });
        }
        checked_len(
            "projection group components",
            group_components.len(),
            MAX_PROJECTION_GROUP_COMPONENTS,
        )?;
        if measures.owner() != &RecordTypeRef::ProjectionResult(projection_id) {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection measure record has the wrong owner",
            });
        }
        if measures.fields().is_empty() {
            return Err(IrValidationError::Empty {
                kind: "projection measures",
            });
        }
        let identity_maximum = 4usize + 256 + 4 + 32;
        let components_maximum = group_components.iter().try_fold(0usize, |sum, component| {
            sum.checked_add(component.maximum_framed_bytes)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "projection group key",
                })
        })?;
        let maximum_complete_key_bytes = 2usize
            .checked_add(identity_maximum)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(components_maximum))
            .ok_or(IrValidationError::SizeOverflow {
                kind: "projection group key",
            })?;
        checked_len(
            "maximum projection group key",
            maximum_complete_key_bytes,
            MAX_KEY_BYTES,
        )?;

        let maximum_measure_record = maximum_record_bytes(&measures)?;
        // Semantic payload: identity, generation, length-framed repeated group
        // values, canonical measure record, last-changed sequence, and fixed
        // field framing. The 32-byte allowance freezes the v1 semantic framing.
        let maximum_stored_state_bytes = identity_maximum
            .checked_add(8)
            .and_then(|value| value.checked_add(components_maximum))
            .and_then(|value| value.checked_add(4 + maximum_measure_record))
            .and_then(|value| value.checked_add(8 + 32))
            .ok_or(IrValidationError::SizeOverflow {
                kind: "projection stored state",
            })?;
        checked_len(
            "maximum projection stored state",
            maximum_stored_state_bytes,
            MAX_PROJECTION_STATE_SEMANTIC_BYTES,
        )?;
        Ok(Self {
            projection_id,
            codec_version: PROJECTION_GROUP_CODEC_VERSION_V1,
            group_components,
            measures,
            maximum_complete_key_bytes,
            maximum_stored_state_bytes,
        })
    }

    /// Stable projection ID.
    #[must_use]
    pub const fn projection_id(&self) -> ProjectionId {
        self.projection_id
    }

    /// Immutable group codec version.
    #[must_use]
    pub const fn codec_version(&self) -> u32 {
        self.codec_version
    }

    /// Ordered grouping components.
    #[must_use]
    pub fn group_components(&self) -> &[ProjectionGroupComponentSchema] {
        &self.group_components
    }

    /// Projection measure record schema.
    #[must_use]
    pub const fn measures(&self) -> &RecordSchema {
        &self.measures
    }

    /// Checked maximum complete durable group-key length.
    #[must_use]
    pub const fn maximum_complete_key_bytes(&self) -> usize {
        self.maximum_complete_key_bytes
    }

    /// Checked maximum stored semantic state payload length.
    #[must_use]
    pub const fn maximum_stored_state_bytes(&self) -> usize {
        self.maximum_stored_state_bytes
    }

    /// Binds this pre-hash schema to its exact lineage/ID/plan identity.
    pub(crate) fn bind_checked(
        self,
        lineage: ContractLineage,
        plan_hash: ProjectionPlanHash,
    ) -> BoundProjectionGroupSchema {
        let identity = ProjectionIdentity::new(lineage, self.projection_id, plan_hash);
        BoundProjectionGroupSchema {
            schema: self,
            identity,
        }
    }
}

/// The exact checked projection group schema paired with its computed identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundProjectionGroupSchema {
    schema: ProjectionGroupSchema,
    identity: ProjectionIdentity,
}

impl BoundProjectionGroupSchema {
    /// Exact lineage, stable ID, and plan hash.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Immutable pre-hash schema.
    #[must_use]
    pub const fn schema(&self) -> &ProjectionGroupSchema {
        &self.schema
    }

    /// Constructs a complete schema-validated group key.
    pub fn group_key(
        &self,
        generation: ProjectionGeneration,
        values: &[CanonicalValue],
    ) -> Result<ProjectionGroupKey, IrValidationError> {
        if values.len() != self.schema.group_components.len() {
            return Err(IrValidationError::InvalidProjection {
                reason: "wrong projection group component count",
            });
        }
        let mut builder = ProjectionGroupKeyBuilder::new(self.identity.clone(), generation);
        for (component, value) in self.schema.group_components.iter().zip(values) {
            component.validate(value)?;
            builder.push_component(value.clone()).map_err(|_| {
                IrValidationError::InvalidProjection {
                    reason: "projection group key exceeds its checked bound",
                }
            })?;
        }
        builder
            .finish()
            .map_err(|_| IrValidationError::InvalidProjection {
                reason: "empty projection group key",
            })
    }

    /// Constructs a zero-or-more-component validated leading prefix.
    pub fn group_prefix(
        &self,
        generation: ProjectionGeneration,
        values: &[CanonicalValue],
    ) -> Result<ProjectionGroupPrefix, IrValidationError> {
        if values.len() > self.schema.group_components.len() {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection group prefix has too many components",
            });
        }
        let mut builder = ProjectionGroupPrefixBuilder::new(self.identity.clone(), generation);
        for (component, value) in self.schema.group_components.iter().zip(values) {
            component.validate(value)?;
            builder.push_component(value.clone()).map_err(|_| {
                IrValidationError::InvalidProjection {
                    reason: "projection group prefix exceeds its checked bound",
                }
            })?;
        }
        Ok(builder.finish())
    }

    /// Revalidates a syntactically decoded complete group key.
    pub fn validate_group_key(&self, key: &ProjectionGroupKey) -> Result<(), IrValidationError> {
        if key.identity() != &self.identity
            || key.components().len() != self.schema.group_components.len()
        {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection group key identity or arity mismatch",
            });
        }
        for (component, value) in self.schema.group_components.iter().zip(key.components()) {
            component.validate(value)?;
        }
        Ok(())
    }
}

/// Projection aggregation operators.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionAggregation {
    /// Checked `u64` count.
    Count = crate::format_registry::projection_aggregation::COUNT,
    /// Checked sum retaining the exact operand type.
    Sum = crate::format_registry::projection_aggregation::SUM,
}

/// One declared projection measure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionMeasurePlan {
    field: FieldSchema,
    aggregation: ProjectionAggregation,
    expression: Option<ExprId>,
}

impl ProjectionMeasurePlan {
    /// Creates a count measure with `u64` result.
    pub fn count(field: FieldSchema) -> Result<Self, IrValidationError> {
        if field.value_type().tag() != ValueTypeTag::U64 {
            return Err(IrValidationError::TypeMismatch {
                context: "projection count measure",
            });
        }
        Ok(Self {
            field,
            aggregation: ProjectionAggregation::Count,
            expression: None,
        })
    }

    /// Creates a sum measure retaining its operand's exact type.
    pub fn sum(field: FieldSchema, expression: ExprId) -> Result<Self, IrValidationError> {
        if !matches!(
            field.value_type().tag(),
            ValueTypeTag::I64 | ValueTypeTag::U64 | ValueTypeTag::Decimal | ValueTypeTag::Money
        ) {
            return Err(IrValidationError::TypeMismatch {
                context: "projection sum measure",
            });
        }
        Ok(Self {
            field,
            aggregation: ProjectionAggregation::Sum,
            expression: Some(expression),
        })
    }

    /// Stable result field.
    #[must_use]
    pub const fn field(&self) -> &FieldSchema {
        &self.field
    }

    /// Aggregation operation.
    #[must_use]
    pub const fn aggregation(&self) -> ProjectionAggregation {
        self.aggregation
    }

    /// Sum operand, absent for count.
    #[must_use]
    pub const fn expression(&self) -> Option<ExprId> {
        self.expression
    }
}

/// The only v1 projection frontier policy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionFrontierPolicy {
    /// Consume the authoritative log in contiguous transaction order.
    TransactionallyOrdered = crate::format_registry::projection_frontier::TRANSACTIONALLY_ORDERED,
}

/// One immutable executable projection plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionPlan {
    projection_id: ProjectionId,
    name: String,
    source_event: EventTypeId,
    expressions: ExpressionArena,
    filter: Option<ExprId>,
    key_expressions: Vec<ExprId>,
    measures: Vec<ProjectionMeasurePlan>,
    frontier: ProjectionFrontierPolicy,
    group_schema: ProjectionGroupSchema,
    plan_hash: ProjectionPlanHash,
}

impl ProjectionPlan {
    /// Creates and hashes a complete checked incremental projection plan.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        projection_id: ProjectionId,
        name: impl Into<String>,
        source_event: EventTypeId,
        expressions: ExpressionArena,
        filter: Option<ExprId>,
        key_expressions: Vec<ExprId>,
        measures: Vec<ProjectionMeasurePlan>,
        frontier: ProjectionFrontierPolicy,
        group_schema: ProjectionGroupSchema,
        contract_schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        crate::validate_source_name(&name, "projection")?;
        let source_schema =
            contract_schema
                .event(source_event)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "projection source event",
                })?;
        if group_schema.projection_id != projection_id
            || key_expressions.len() != group_schema.group_components.len()
            || measures.len() != group_schema.measures.fields().len()
        {
            return Err(IrValidationError::InvalidProjection {
                reason: "projection plan/schema shape mismatch",
            });
        }
        for component in &group_schema.group_components {
            if let Some(enum_id) = component.value_type().enum_type_id() {
                contract_schema.validate_enum_variant_registry(
                    enum_id,
                    component.enum_variants(),
                    "projection",
                )?;
            }
        }
        contract_schema.validate_expression_enum_constants(&expressions)?;
        if let Some(filter) = filter {
            if expressions
                .get(filter)
                .is_none_or(|node| node.result_type().tag() != ValueTypeTag::Bool)
            {
                return Err(IrValidationError::TypeMismatch {
                    context: "projection filter",
                });
            }
            validate_projection_filter(&expressions, filter)?;
        }
        for (expression, component) in key_expressions.iter().zip(&group_schema.group_components) {
            if expressions
                .get(*expression)
                .is_none_or(|node| node.result_type() != component.value_type())
            {
                return Err(IrValidationError::TypeMismatch {
                    context: "projection group expression",
                });
            }
            validate_projection_expression_context(&expressions, *expression)?;
        }
        for (measure, schema_field) in measures.iter().zip(group_schema.measures.fields()) {
            crate::schema::validate_declared_field_type(
                schema_field.value_type(),
                contract_schema,
            )?;
            if measure.field != *schema_field {
                return Err(IrValidationError::InvalidProjection {
                    reason: "projection measure field/schema mismatch",
                });
            }
            if let Some(expression) = measure.expression {
                if expressions
                    .get(expression)
                    .is_none_or(|node| node.result_type() != measure.field.value_type())
                {
                    return Err(IrValidationError::TypeMismatch {
                        context: "projection measure expression",
                    });
                }
                validate_projection_expression_context(&expressions, expression)?;
            }
        }
        validate_projection_arena(
            &expressions,
            source_schema.payload(),
            filter,
            &key_expressions,
            &measures,
        )?;
        let mut plan = Self {
            projection_id,
            name,
            source_event,
            expressions,
            filter,
            key_expressions,
            measures,
            frontier,
            group_schema,
            plan_hash: ProjectionPlanHash::from_bytes([0; 32]),
        };
        plan.plan_hash = crate::bundle::compute_projection_plan_hash(&plan, contract_schema)?;
        Ok(plan)
    }

    /// Stable projection ID.
    #[must_use]
    pub const fn projection_id(&self) -> ProjectionId {
        self.projection_id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable source event type.
    #[must_use]
    pub const fn source_event(&self) -> EventTypeId {
        self.source_event
    }
    /// Typed expression arena.
    #[must_use]
    pub const fn expressions(&self) -> &ExpressionArena {
        &self.expressions
    }
    /// Optional Boolean filter.
    #[must_use]
    pub const fn filter(&self) -> Option<ExprId> {
        self.filter
    }
    /// Ordered grouping expressions.
    #[must_use]
    pub fn key_expressions(&self) -> &[ExprId] {
        &self.key_expressions
    }
    /// Measures in stable result-field order.
    #[must_use]
    pub fn measures(&self) -> &[ProjectionMeasurePlan] {
        &self.measures
    }
    /// Frontier policy.
    #[must_use]
    pub const fn frontier(&self) -> ProjectionFrontierPolicy {
        self.frontier
    }
    /// Pre-hash group schema.
    #[must_use]
    pub const fn group_schema(&self) -> &ProjectionGroupSchema {
        &self.group_schema
    }
    /// Typed plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> ProjectionPlanHash {
        self.plan_hash
    }
}

fn maximum_record_bytes(record: &RecordSchema) -> Result<usize, IrValidationError> {
    let mut total = 6usize;
    for field in record.fields() {
        let maximum = field.value_type().maximum_canonical_bytes()?.ok_or(
            IrValidationError::InvalidProjection {
                reason: "projection measure record cannot contain a record",
            },
        )?;
        total = total
            .checked_add(4)
            .and_then(|value| value.checked_add(maximum))
            .ok_or(IrValidationError::SizeOverflow {
                kind: "projection measure record",
            })?;
    }
    Ok(total)
}

fn validate_projection_expression_context(
    arena: &ExpressionArena,
    expression: ExprId,
) -> Result<(), IrValidationError> {
    let dependencies = arena.dependencies(expression)?;
    if !dependencies.input_fields().is_empty()
        || !dependencies.bindings().is_empty()
        || !dependencies.schema_fields().is_empty()
        || dependencies.uses_transaction_time()
    {
        return Err(IrValidationError::InvalidProjection {
            reason: "projection expression references a command or schema context",
        });
    }
    Ok(())
}

fn validate_projection_filter(
    arena: &ExpressionArena,
    expression: ExprId,
) -> Result<(), IrValidationError> {
    validate_projection_expression_context(arena, expression)?;
    let node = arena
        .get(expression)
        .ok_or(IrValidationError::InvalidReference {
            kind: "projection filter",
        })?;
    match node.kind() {
        ExpressionKind::Binary {
            operator: crate::BinaryOperator::And,
            left,
            right,
        } => {
            validate_projection_filter(arena, *left)?;
            validate_projection_filter(arena, *right)
        }
        ExpressionKind::Binary {
            operator: crate::BinaryOperator::Equal,
            left,
            right,
        } if arena
            .get(*left)
            .is_some_and(|node| is_projection_filter_atom(node.kind()))
            && arena
                .get(*right)
                .is_some_and(|node| is_projection_filter_atom(node.kind())) =>
        {
            Ok(())
        }
        _ => Err(IrValidationError::InvalidProjection {
            reason: "projection filter is not equality/enum equality/Boolean conjunction",
        }),
    }
}

fn is_projection_filter_atom(kind: &ExpressionKind) -> bool {
    matches!(
        kind,
        ExpressionKind::Constant(_)
            | ExpressionKind::SourceEventField(_)
            | ExpressionKind::TransactionDate
    )
}

fn validate_projection_arena(
    arena: &ExpressionArena,
    source: &RecordSchema,
    filter: Option<ExprId>,
    keys: &[ExprId],
    measures: &[ProjectionMeasurePlan],
) -> Result<(), IrValidationError> {
    let mut reachable = vec![false; arena.len()];
    let mut pending = filter
        .into_iter()
        .chain(keys.iter().copied())
        .chain(
            measures
                .iter()
                .filter_map(ProjectionMeasurePlan::expression),
        )
        .collect::<Vec<_>>();
    while let Some(expression) = pending.pop() {
        let index = expression.get() as usize;
        if index >= arena.len() {
            return Err(IrValidationError::InvalidReference {
                kind: "projection expression root",
            });
        }
        if std::mem::replace(&mut reachable[index], true) {
            continue;
        }
        match arena.nodes()[index].kind() {
            ExpressionKind::Unary { operand, .. } => pending.push(*operand),
            ExpressionKind::Binary { left, right, .. } => {
                pending.push(*left);
                pending.push(*right);
            }
            _ => {}
        }
    }
    if reachable.iter().any(|value| !value) {
        return Err(IrValidationError::InvalidProjection {
            reason: "projection arena contains an unreachable expression",
        });
    }
    for node in arena.nodes() {
        match node.kind() {
            ExpressionKind::Constant(_) | ExpressionKind::TransactionDate => {}
            ExpressionKind::SourceEventField(field_id) => {
                let field = source
                    .field(*field_id)
                    .ok_or(IrValidationError::InvalidReference {
                        kind: "projection source-event field",
                    })?;
                if field.value_type() != node.result_type() {
                    return Err(IrValidationError::TypeMismatch {
                        context: "projection source-event field",
                    });
                }
            }
            ExpressionKind::Unary { .. } | ExpressionKind::Binary { .. } => {}
            _ => {
                return Err(IrValidationError::InvalidProjection {
                    reason: "projection arena contains a command or schema expression",
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::FieldId;

    #[test]
    fn group_schema_rejects_optional_and_key_overflow() {
        let optional = ValueType::optional(ValueType::i64()).expect("optional");
        assert!(ProjectionGroupComponentSchema::new(optional, vec![]).is_err());

        let component =
            ProjectionGroupComponentSchema::new(ValueType::string(4_000).expect("type"), vec![])
                .expect("component");
        let measures = RecordSchema::new(
            RecordTypeRef::ProjectionResult(ProjectionId::first()),
            vec![FieldSchema::new(FieldId::first(), "count", ValueType::u64()).expect("field")],
        )
        .expect("record");
        assert!(
            ProjectionGroupSchema::new(ProjectionId::first(), vec![component], measures).is_err()
        );
    }

    #[test]
    fn group_schema_enforces_component_and_complete_key_boundaries() {
        let measures = || {
            RecordSchema::new(
                RecordTypeRef::ProjectionResult(ProjectionId::first()),
                vec![FieldSchema::new(FieldId::first(), "count", ValueType::u64()).expect("field")],
            )
            .expect("record")
        };
        let bool_component =
            ProjectionGroupComponentSchema::new(ValueType::bool(), vec![]).expect("component");
        assert!(matches!(
            ProjectionGroupSchema::new(
                ProjectionId::first(),
                vec![bool_component.clone(); MAX_PROJECTION_GROUP_COMPONENTS],
                measures(),
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "maximum projection group key",
                ..
            })
        ));
        assert!(matches!(
            ProjectionGroupSchema::new(
                ProjectionId::first(),
                vec![bool_component; MAX_PROJECTION_GROUP_COMPONENTS + 1],
                measures(),
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "projection group components",
                actual: 1_025,
                maximum: 1_024,
            })
        ));

        // Identity/generation framing is 306 bytes and a string component is
        // four-byte framed around six bytes of canonical scalar overhead.
        let exact_string_bound = MAX_KEY_BYTES - 306 - 4 - 6;
        let exact = ProjectionGroupComponentSchema::new(
            ValueType::string(exact_string_bound).expect("type"),
            vec![],
        )
        .expect("component");
        let exact = ProjectionGroupSchema::new(ProjectionId::first(), vec![exact], measures())
            .expect("exact key bound");
        assert_eq!(exact.maximum_complete_key_bytes(), MAX_KEY_BYTES);

        let too_large = ProjectionGroupComponentSchema::new(
            ValueType::string(exact_string_bound + 1).expect("type"),
            vec![],
        )
        .expect("component");
        assert!(matches!(
            ProjectionGroupSchema::new(ProjectionId::first(), vec![too_large], measures(),),
            Err(IrValidationError::LimitExceeded {
                kind: "maximum projection group key",
                actual: 4_097,
                maximum: 4_096,
            })
        ));
    }

    #[test]
    fn bound_schema_constructs_and_checks_exact_keys() {
        let component =
            ProjectionGroupComponentSchema::new(ValueType::i64(), vec![]).expect("component");
        let measures = RecordSchema::new(
            RecordTypeRef::ProjectionResult(ProjectionId::first()),
            vec![FieldSchema::new(FieldId::first(), "count", ValueType::u64()).expect("field")],
        )
        .expect("record");
        let schema = ProjectionGroupSchema::new(ProjectionId::first(), vec![component], measures)
            .expect("schema")
            .bind_checked(
                ContractLineage::new("LegalSpend").expect("lineage"),
                ProjectionPlanHash::from_bytes([7; 32]),
            );
        let key = schema
            .group_key(ProjectionGeneration::first(), &[CanonicalValue::I64(2026)])
            .expect("key");
        schema.validate_group_key(&key).expect("validated key");
    }

    #[test]
    fn filter_accepts_atomic_equality_and_rejects_nested_operators() {
        let field_equality = ExpressionArena::new(vec![
            (
                ExpressionKind::SourceEventField(FieldId::first()),
                ValueType::i64(),
            ),
            (
                ExpressionKind::SourceEventField(FieldId::new(2).expect("field")),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::Equal,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                ValueType::bool(),
            ),
        ])
        .expect("field equality");
        validate_projection_filter(&field_equality, ExprId::new(2)).expect("field equality");

        let event_date_equality = ExpressionArena::new(vec![
            (
                ExpressionKind::SourceEventField(FieldId::first()),
                ValueType::date(),
            ),
            (ExpressionKind::TransactionDate, ValueType::date()),
            (
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::Equal,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                ValueType::bool(),
            ),
        ])
        .expect("date equality");
        validate_projection_filter(&event_date_equality, ExprId::new(2))
            .expect("event date equals tx.date");

        let nested_arithmetic = ExpressionArena::new(vec![
            (
                ExpressionKind::Constant(CanonicalValue::I64(1)),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::I64(2)),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::Add,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                ValueType::i64(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::I64(3)),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::Equal,
                    left: ExprId::new(2),
                    right: ExprId::new(3),
                },
                ValueType::bool(),
            ),
        ])
        .expect("arena");
        assert!(validate_projection_filter(&nested_arithmetic, ExprId::new(4)).is_err());
    }

    #[test]
    fn projection_arena_rejects_unknown_source_fields_and_unreachable_nodes() {
        let event_id = EventTypeId::first();
        let source = RecordSchema::new(
            RecordTypeRef::Event(event_id),
            vec![FieldSchema::new(FieldId::first(), "value", ValueType::i64()).expect("field")],
        )
        .expect("source");
        let unknown = ExpressionArena::new(vec![(
            ExpressionKind::SourceEventField(FieldId::new(2).expect("id")),
            ValueType::i64(),
        )])
        .expect("arena");
        assert!(
            validate_projection_arena(&unknown, &source, None, &[ExprId::new(0)], &[],).is_err()
        );

        let unreachable = ExpressionArena::new(vec![
            (
                ExpressionKind::SourceEventField(FieldId::first()),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::I64(1)),
                ValueType::i64(),
            ),
        ])
        .expect("arena");
        assert!(
            validate_projection_arena(&unreachable, &source, None, &[ExprId::new(0)], &[],)
                .is_err()
        );
    }
}
