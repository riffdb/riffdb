//! Typed, topologically ordered expression plans.

use std::collections::BTreeSet;

use riffdb_types::{CanonicalValue, EntityTypeId, FieldId};

use crate::{IrValidationError, RecordTypeRef, ValueType, ValueTypeTag, checked_len};

/// Maximum total expression nodes in one v1 bundle.
pub const MAX_EXPRESSION_NODES: usize = 131_072;
/// Maximum root-to-leaf node depth in one v1 expression DAG.
pub const MAX_EXPRESSION_NESTING: usize = 32;

/// A dense zero-based expression position scoped to one arena.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExprId(u32);

impl ExprId {
    /// Constructs a plan-local expression position.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric zero-based position.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A dense zero-based binding position scoped to one command plan.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BindingId(u32);

impl BindingId {
    /// Constructs a plan-local binding position.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric zero-based position.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A dense zero-based internal aggregate-root read position scoped to one command plan.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RootValidationReadId(u32);

impl RootValidationReadId {
    /// Constructs a plan-local root-validation read position.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric zero-based position.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Immutable v1 unary operators.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum UnaryOperator {
    /// Boolean negation.
    Not = crate::format_registry::unary_operator::NOT,
    /// Checked arithmetic negation.
    Negate = crate::format_registry::unary_operator::NEGATE,
}

/// Immutable v1 binary operators in source-precedence registry order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum BinaryOperator {
    /// Checked multiplication.
    Multiply = crate::format_registry::binary_operator::MULTIPLY,
    /// Checked division.
    Divide = crate::format_registry::binary_operator::DIVIDE,
    /// Checked addition.
    Add = crate::format_registry::binary_operator::ADD,
    /// Checked subtraction.
    Subtract = crate::format_registry::binary_operator::SUBTRACT,
    /// Equality.
    Equal = crate::format_registry::binary_operator::EQUAL,
    /// Inequality.
    NotEqual = crate::format_registry::binary_operator::NOT_EQUAL,
    /// Less than.
    Less = crate::format_registry::binary_operator::LESS,
    /// Less than or equal.
    LessEqual = crate::format_registry::binary_operator::LESS_EQUAL,
    /// Greater than.
    Greater = crate::format_registry::binary_operator::GREATER,
    /// Greater than or equal.
    GreaterEqual = crate::format_registry::binary_operator::GREATER_EQUAL,
    /// Left-to-right short-circuit conjunction.
    And = crate::format_registry::binary_operator::AND,
    /// Left-to-right short-circuit disjunction.
    Or = crate::format_registry::binary_operator::OR,
}

/// One expression operation. The containing node stores its checked result type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpressionKind {
    /// Exact canonical constant.
    Constant(CanonicalValue),
    /// Stable field in the command input record.
    InputField(FieldId),
    /// Compiler-declared service-owned command value.
    ServiceValue(FieldId),
    /// Current submitted element in the one compiler-owned collection expansion.
    CollectionElement,
    /// Stable field of the current record-valued collection element.
    CollectionElementField(FieldId),
    /// Complete bound entity record.
    CompleteBinding(BindingId),
    /// Stable field of one bound entity record.
    BoundField {
        /// Bound entity record.
        binding: BindingId,
        /// Stable field within that record.
        field: FieldId,
    },
    /// Stable schema field in an invariant or aggregate template.
    SchemaField {
        /// Entity whose schema owns the field.
        entity_type: EntityTypeId,
        /// Stable schema field.
        field: FieldId,
    },
    /// Stable field of one compiler-declared internal aggregate-root read.
    RootValidationField {
        /// Internal root observation.
        read: RootValidationReadId,
        /// Stable field within the aggregate-root record.
        field: FieldId,
    },
    /// Stable field of the projection source event.
    SourceEventField(FieldId),
    /// Fixed admitted command logical time.
    TransactionTime,
    /// UTC date derived from the originating event's logical time.
    TransactionDate,
    /// Unary operation over an earlier node.
    Unary {
        /// Closed unary operation.
        operator: UnaryOperator,
        /// Earlier operand node.
        operand: ExprId,
    },
    /// Binary operation over earlier nodes.
    Binary {
        /// Closed binary operation.
        operator: BinaryOperator,
        /// Earlier left operand.
        left: ExprId,
        /// Earlier right operand.
        right: ExprId,
    },
}

impl ExpressionKind {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::Constant(_) => crate::format_registry::expression::CONSTANT,
            Self::InputField(_) => crate::format_registry::expression::INPUT_FIELD,
            Self::ServiceValue(_) => crate::format_registry::expression::SERVICE_VALUE,
            Self::CollectionElement => crate::format_registry::expression::COLLECTION_ELEMENT,
            Self::CollectionElementField(_) => {
                crate::format_registry::expression::COLLECTION_ELEMENT_FIELD
            }
            Self::CompleteBinding(_) => crate::format_registry::expression::COMPLETE_BINDING,
            Self::BoundField { .. } => crate::format_registry::expression::BOUND_FIELD,
            Self::SchemaField { .. } => crate::format_registry::expression::SCHEMA_FIELD,
            Self::SourceEventField(_) => crate::format_registry::expression::SOURCE_EVENT_FIELD,
            Self::TransactionTime => crate::format_registry::expression::TRANSACTION_TIME,
            Self::TransactionDate => crate::format_registry::expression::TRANSACTION_DATE,
            Self::Unary { .. } => crate::format_registry::expression::UNARY,
            Self::Binary { .. } => crate::format_registry::expression::BINARY,
            Self::RootValidationField { .. } => {
                crate::format_registry::expression::ROOT_VALIDATION_FIELD
            }
        }
    }
}

/// One typed expression arena node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedExpression {
    kind: ExpressionKind,
    result_type: ValueType,
}

impl TypedExpression {
    /// Expression operation.
    #[must_use]
    pub const fn kind(&self) -> &ExpressionKind {
        &self.kind
    }

    /// Checked result type.
    #[must_use]
    pub const fn result_type(&self) -> &ValueType {
        &self.result_type
    }
}

/// A complete immutable topologically ordered expression arena.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionArena {
    nodes: Vec<TypedExpression>,
}

impl ExpressionArena {
    /// Validates a complete arena. Empty arenas are permitted for schema-only plans.
    pub fn new(nodes: Vec<(ExpressionKind, ValueType)>) -> Result<Self, IrValidationError> {
        checked_len("expression arena", nodes.len(), MAX_EXPRESSION_NODES)?;
        let mut arena = Self {
            nodes: Vec::with_capacity(nodes.len()),
        };
        let mut depths = Vec::with_capacity(nodes.len());
        for (kind, result_type) in nodes {
            let depth = match &kind {
                ExpressionKind::Unary { operand, .. } => depths
                    .get(operand.get() as usize)
                    .copied()
                    .and_then(|depth: usize| depth.checked_add(1))
                    .ok_or(IrValidationError::NonForwardExpression)?,
                ExpressionKind::Binary { left, right, .. } => {
                    let left = depths
                        .get(left.get() as usize)
                        .copied()
                        .ok_or(IrValidationError::NonForwardExpression)?;
                    let right = depths
                        .get(right.get() as usize)
                        .copied()
                        .ok_or(IrValidationError::NonForwardExpression)?;
                    left.max(right)
                        .checked_add(1)
                        .ok_or(IrValidationError::SizeOverflow {
                            kind: "expression nesting",
                        })?
                }
                _ => 1,
            };
            checked_len("expression nesting", depth, MAX_EXPRESSION_NESTING)?;
            arena.push_checked(kind, result_type)?;
            depths.push(depth);
        }
        Ok(arena)
    }

    /// Empty expression arena.
    #[must_use]
    pub const fn empty() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Checked nodes in dense `ExprId` order.
    #[must_use]
    pub fn nodes(&self) -> &[TypedExpression] {
        &self.nodes
    }

    /// Resolves one expression position.
    #[must_use]
    pub fn get(&self, id: ExprId) -> Option<&TypedExpression> {
        self.nodes.get(id.get() as usize)
    }

    /// Number of nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether no expressions are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn validate_reachable_from(
        &self,
        roots: &[ExprId],
        context: &'static str,
    ) -> Result<(), IrValidationError> {
        let mut reachable = vec![false; self.nodes.len()];
        let mut pending = roots.to_vec();
        while let Some(expression) = pending.pop() {
            let index = expression.get() as usize;
            let node = self
                .nodes
                .get(index)
                .ok_or(IrValidationError::InvalidReference { kind: context })?;
            if std::mem::replace(&mut reachable[index], true) {
                continue;
            }
            match node.kind() {
                ExpressionKind::Unary { operand, .. } => pending.push(*operand),
                ExpressionKind::Binary { left, right, .. } => {
                    pending.push(*left);
                    pending.push(*right);
                }
                _ => {}
            }
        }
        if reachable.iter().any(|value| !value) {
            return Err(IrValidationError::InvalidDependency {
                reason: "expression arena contains an unreachable node",
            });
        }
        Ok(())
    }

    /// Computes the transitive dependency categories of one expression.
    pub fn dependencies(&self, id: ExprId) -> Result<ExpressionDependencies, IrValidationError> {
        let target = id.get() as usize;
        if target >= self.nodes.len() {
            return Err(IrValidationError::InvalidReference { kind: "expression" });
        }
        let mut result = ExpressionDependencies::default();
        let mut needed = vec![false; target + 1];
        needed[target] = true;
        for index in (0..=target).rev() {
            if !needed[index] {
                continue;
            }
            match &self.nodes[index].kind {
                ExpressionKind::Constant(_) => {}
                ExpressionKind::InputField(field) => {
                    result.input_fields.insert(*field);
                }
                ExpressionKind::ServiceValue(field) => {
                    result.service_values.insert(*field);
                }
                ExpressionKind::CollectionElement => result.collection_element = true,
                ExpressionKind::CollectionElementField(field) => {
                    result.collection_element_fields.insert(*field);
                }
                ExpressionKind::CompleteBinding(binding) => {
                    result.bindings.insert(*binding);
                    result.complete_bindings.insert(*binding);
                }
                ExpressionKind::BoundField { binding, field } => {
                    result.bindings.insert(*binding);
                    result.bound_fields.insert((*binding, *field));
                }
                ExpressionKind::SchemaField { entity_type, field } => {
                    result.schema_fields.insert((*entity_type, *field));
                }
                ExpressionKind::RootValidationField { read, field } => {
                    result.root_validation_reads.insert(*read);
                    result.root_validation_fields.insert((*read, *field));
                }
                ExpressionKind::SourceEventField(field) => {
                    result.source_event_fields.insert(*field);
                }
                ExpressionKind::TransactionTime => result.transaction_time = true,
                ExpressionKind::TransactionDate => result.transaction_date = true,
                ExpressionKind::Unary { operand, .. } => needed[operand.get() as usize] = true,
                ExpressionKind::Binary { left, right, .. } => {
                    needed[left.get() as usize] = true;
                    needed[right.get() as usize] = true;
                }
            }
        }
        Ok(result)
    }

    fn push_checked(
        &mut self,
        kind: ExpressionKind,
        result_type: ValueType,
    ) -> Result<(), IrValidationError> {
        let position = self.nodes.len();
        match &kind {
            ExpressionKind::Constant(value) => {
                validate_ir_constant_shape(value)?;
                result_type.validate_value(value)?;
            }
            ExpressionKind::TransactionTime if result_type.tag() != ValueTypeTag::Timestamp => {
                return Err(IrValidationError::TypeMismatch { context: "tx.time" });
            }
            ExpressionKind::TransactionDate if result_type.tag() != ValueTypeTag::Date => {
                return Err(IrValidationError::TypeMismatch { context: "tx.date" });
            }
            ExpressionKind::CompleteBinding(_) => {
                if !matches!(result_type.record_ref(), Some(RecordTypeRef::Entity(_))) {
                    return Err(IrValidationError::TypeMismatch {
                        context: "complete binding",
                    });
                }
            }
            ExpressionKind::Unary { operator, operand } => {
                let operand = self.forward_node(*operand, position)?;
                validate_unary(*operator, operand.result_type(), &result_type)?;
            }
            ExpressionKind::Binary {
                operator,
                left,
                right,
            } => {
                let left = self.forward_node(*left, position)?;
                let right = self.forward_node(*right, position)?;
                validate_binary(
                    *operator,
                    left.result_type(),
                    right.result_type(),
                    &result_type,
                )?;
            }
            _ => {}
        }
        self.nodes.push(TypedExpression { kind, result_type });
        Ok(())
    }

    fn forward_node(
        &self,
        id: ExprId,
        position: usize,
    ) -> Result<&TypedExpression, IrValidationError> {
        let index = id.get() as usize;
        if index >= position {
            return Err(IrValidationError::NonForwardExpression);
        }
        self.nodes
            .get(index)
            .ok_or(IrValidationError::NonForwardExpression)
    }
}

fn validate_ir_constant_shape(value: &CanonicalValue) -> Result<(), IrValidationError> {
    match value {
        CanonicalValue::List(values) => {
            checked_len(
                "IR constant list entries",
                values.len(),
                crate::MAX_OBJECT_FIELDS,
            )?;
            for value in values.values() {
                validate_ir_constant_shape(value)?;
            }
        }
        CanonicalValue::Record(record) => {
            checked_len(
                "IR constant record fields",
                record.len(),
                crate::MAX_OBJECT_FIELDS,
            )?;
            for (_, value) in record.fields() {
                validate_ir_constant_shape(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Transitive dependency categories for one expression.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExpressionDependencies {
    input_fields: BTreeSet<FieldId>,
    service_values: BTreeSet<FieldId>,
    bindings: BTreeSet<BindingId>,
    complete_bindings: BTreeSet<BindingId>,
    bound_fields: BTreeSet<(BindingId, FieldId)>,
    schema_fields: BTreeSet<(EntityTypeId, FieldId)>,
    root_validation_reads: BTreeSet<RootValidationReadId>,
    root_validation_fields: BTreeSet<(RootValidationReadId, FieldId)>,
    source_event_fields: BTreeSet<FieldId>,
    collection_element: bool,
    collection_element_fields: BTreeSet<FieldId>,
    transaction_time: bool,
    transaction_date: bool,
}

impl ExpressionDependencies {
    /// Referenced command input fields.
    #[must_use]
    pub const fn input_fields(&self) -> &BTreeSet<FieldId> {
        &self.input_fields
    }

    /// Referenced service-owned command values.
    #[must_use]
    pub const fn service_values(&self) -> &BTreeSet<FieldId> {
        &self.service_values
    }

    /// Referenced command bindings.
    #[must_use]
    pub const fn bindings(&self) -> &BTreeSet<BindingId> {
        &self.bindings
    }

    /// Bindings whose complete record is influential.
    #[must_use]
    pub const fn complete_bindings(&self) -> &BTreeSet<BindingId> {
        &self.complete_bindings
    }

    /// Exact influential bound fields.
    #[must_use]
    pub const fn bound_fields(&self) -> &BTreeSet<(BindingId, FieldId)> {
        &self.bound_fields
    }

    /// Referenced schema-template fields.
    #[must_use]
    pub const fn schema_fields(&self) -> &BTreeSet<(EntityTypeId, FieldId)> {
        &self.schema_fields
    }

    /// Referenced internal aggregate-root reads.
    #[must_use]
    pub const fn root_validation_reads(&self) -> &BTreeSet<RootValidationReadId> {
        &self.root_validation_reads
    }

    /// Exact influential fields of internal aggregate-root reads.
    #[must_use]
    pub const fn root_validation_fields(&self) -> &BTreeSet<(RootValidationReadId, FieldId)> {
        &self.root_validation_fields
    }

    /// Referenced source-event fields.
    #[must_use]
    pub const fn source_event_fields(&self) -> &BTreeSet<FieldId> {
        &self.source_event_fields
    }

    /// Whether the complete current collection element is referenced.
    #[must_use]
    pub const fn uses_collection_element(&self) -> bool {
        self.collection_element
    }

    /// Stable fields referenced from the current record-valued element.
    #[must_use]
    pub const fn collection_element_fields(&self) -> &BTreeSet<FieldId> {
        &self.collection_element_fields
    }

    /// Whether `tx.time` is referenced.
    #[must_use]
    pub const fn uses_transaction_time(&self) -> bool {
        self.transaction_time
    }

    /// Whether `tx.date` is referenced.
    #[must_use]
    pub const fn uses_transaction_date(&self) -> bool {
        self.transaction_date
    }

    /// True only when the expression can be computed before entity reads.
    #[must_use]
    pub fn is_input_computable(&self) -> bool {
        self.service_values.is_empty()
            && self.bindings.is_empty()
            && self.schema_fields.is_empty()
            && self.root_validation_reads.is_empty()
            && self.source_event_fields.is_empty()
            && !self.transaction_time
            && !self.transaction_date
    }
}

fn validate_unary(
    operator: UnaryOperator,
    operand: &ValueType,
    result: &ValueType,
) -> Result<(), IrValidationError> {
    let valid = match operator {
        UnaryOperator::Not => operand.tag() == ValueTypeTag::Bool && result == operand,
        UnaryOperator::Negate => {
            matches!(
                operand.tag(),
                ValueTypeTag::I64 | ValueTypeTag::Decimal | ValueTypeTag::Money
            ) && result == operand
        }
    };
    if valid {
        Ok(())
    } else {
        Err(IrValidationError::TypeMismatch {
            context: "unary expression",
        })
    }
}

fn validate_binary(
    operator: BinaryOperator,
    left: &ValueType,
    right: &ValueType,
    result: &ValueType,
) -> Result<(), IrValidationError> {
    let same = left == right;
    let bool_type = result.tag() == ValueTypeTag::Bool;
    let valid = match operator {
        BinaryOperator::Multiply | BinaryOperator::Divide => {
            same && matches!(left.tag(), ValueTypeTag::I64 | ValueTypeTag::U64) && result == left
        }
        BinaryOperator::Add | BinaryOperator::Subtract => {
            same && matches!(
                left.tag(),
                ValueTypeTag::I64 | ValueTypeTag::U64 | ValueTypeTag::Decimal | ValueTypeTag::Money
            ) && result == left
        }
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            same && left.supports_equality() && bool_type
        }
        BinaryOperator::Less
        | BinaryOperator::LessEqual
        | BinaryOperator::Greater
        | BinaryOperator::GreaterEqual => {
            same && matches!(
                left.tag(),
                ValueTypeTag::I64
                    | ValueTypeTag::U64
                    | ValueTypeTag::Decimal
                    | ValueTypeTag::Money
                    | ValueTypeTag::Timestamp
                    | ValueTypeTag::Date
            ) && bool_type
        }
        BinaryOperator::And | BinaryOperator::Or => {
            same && left.tag() == ValueTypeTag::Bool && bool_type
        }
    };
    if valid {
        Ok(())
    } else {
        Err(IrValidationError::TypeMismatch {
            context: "binary expression",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_forward_and_type_invalid_nodes() {
        assert!(
            ExpressionArena::new(vec![(
                ExpressionKind::Unary {
                    operator: UnaryOperator::Not,
                    operand: ExprId::new(0),
                },
                ValueType::bool(),
            )])
            .is_err()
        );

        assert!(
            ExpressionArena::new(vec![
                (
                    ExpressionKind::Constant(CanonicalValue::I64(1)),
                    ValueType::i64()
                ),
                (
                    ExpressionKind::Unary {
                        operator: UnaryOperator::Not,
                        operand: ExprId::new(0),
                    },
                    ValueType::bool(),
                ),
            ])
            .is_err()
        );
    }

    #[test]
    fn expression_dag_enforces_32_node_depth_without_expanding_shared_subtrees() {
        let build = |count: usize| {
            let mut nodes = vec![(
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                ValueType::bool(),
            )];
            for index in 1..count {
                nodes.push((
                    ExpressionKind::Binary {
                        operator: BinaryOperator::And,
                        left: ExprId::new((index - 1) as u32),
                        right: ExprId::new((index - 1) as u32),
                    },
                    ValueType::bool(),
                ));
            }
            ExpressionArena::new(nodes)
        };
        assert!(build(MAX_EXPRESSION_NESTING).is_ok());
        assert!(build(MAX_EXPRESSION_NESTING + 1).is_err());
    }

    #[test]
    fn computes_transitive_input_dependencies() {
        let field = FieldId::first();
        let arena = ExpressionArena::new(vec![
            (ExpressionKind::InputField(field), ValueType::i64()),
            (
                ExpressionKind::Constant(CanonicalValue::I64(2)),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                ValueType::i64(),
            ),
        ])
        .expect("arena");
        let deps = arena.dependencies(ExprId::new(2)).expect("dependencies");
        assert_eq!(
            deps.input_fields().iter().copied().collect::<Vec<_>>(),
            vec![field]
        );
        assert!(deps.is_input_computable());
    }

    #[test]
    fn optional_equality_requires_an_equality_capable_inner_type() {
        let optional_list =
            ValueType::optional(ValueType::list(ValueType::i64(), 2).expect("list"))
                .expect("optional list");
        assert!(
            ExpressionArena::new(vec![
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_list.clone()
                ),
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_list
                ),
                (
                    ExpressionKind::Binary {
                        operator: BinaryOperator::Equal,
                        left: ExprId::new(0),
                        right: ExprId::new(1),
                    },
                    ValueType::bool(),
                ),
            ])
            .is_err()
        );

        let optional_record = ValueType::optional(ValueType::record(RecordTypeRef::Entity(
            EntityTypeId::first(),
        )))
        .expect("optional record");
        assert!(
            ExpressionArena::new(vec![
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_record.clone()
                ),
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_record
                ),
                (
                    ExpressionKind::Binary {
                        operator: BinaryOperator::NotEqual,
                        left: ExprId::new(0),
                        right: ExprId::new(1),
                    },
                    ValueType::bool(),
                ),
            ])
            .is_err()
        );

        let optional_scalar = ValueType::optional(ValueType::i64()).expect("optional scalar");
        assert!(
            ExpressionArena::new(vec![
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_scalar.clone()
                ),
                (
                    ExpressionKind::Constant(CanonicalValue::Null),
                    optional_scalar
                ),
                (
                    ExpressionKind::Binary {
                        operator: BinaryOperator::Equal,
                        left: ExprId::new(0),
                        right: ExprId::new(1),
                    },
                    ValueType::bool(),
                ),
            ])
            .is_ok()
        );
    }

    #[test]
    fn operators_never_apply_contextual_optional_injection() {
        let optional = ValueType::optional(ValueType::i64()).expect("optional");
        assert!(
            ExpressionArena::new(vec![
                (
                    ExpressionKind::Constant(CanonicalValue::I64(1)),
                    ValueType::i64()
                ),
                (ExpressionKind::Constant(CanonicalValue::Null), optional),
                (
                    ExpressionKind::Binary {
                        operator: BinaryOperator::Equal,
                        left: ExprId::new(0),
                        right: ExprId::new(1),
                    },
                    ValueType::bool(),
                ),
            ])
            .is_err()
        );
    }

    #[test]
    fn constants_enforce_recursive_1024_entry_ir_bound() {
        let list =
            |count| CanonicalValue::list(vec![CanonicalValue::Bool(true); count]).expect("list");
        let list_type = |maximum| ValueType::list(ValueType::bool(), maximum).expect("list type");
        assert!(
            ExpressionArena::new(vec![(
                ExpressionKind::Constant(list(crate::MAX_OBJECT_FIELDS)),
                list_type(crate::MAX_OBJECT_FIELDS),
            )])
            .is_ok()
        );
        assert!(
            ExpressionArena::new(vec![(
                ExpressionKind::Constant(list(crate::MAX_OBJECT_FIELDS + 1)),
                list_type(crate::MAX_OBJECT_FIELDS + 1),
            )])
            .is_err()
        );

        let nested =
            CanonicalValue::list(vec![list(crate::MAX_OBJECT_FIELDS + 1)]).expect("nested list");
        let nested_type =
            ValueType::list(list_type(crate::MAX_OBJECT_FIELDS + 1), 1).expect("nested type");
        assert!(
            ExpressionArena::new(vec![(ExpressionKind::Constant(nested), nested_type)]).is_err()
        );

        let record = |count| {
            CanonicalValue::record(
                (1..=count)
                    .map(|id| {
                        (
                            FieldId::new(id as u32).expect("field ID"),
                            CanonicalValue::Bool(true),
                        )
                    })
                    .collect(),
            )
            .expect("record")
        };
        let record_type = ValueType::record(RecordTypeRef::Entity(EntityTypeId::first()));
        assert!(
            ExpressionArena::new(vec![(
                ExpressionKind::Constant(record(crate::MAX_OBJECT_FIELDS)),
                record_type.clone(),
            )])
            .is_ok()
        );
        assert!(
            ExpressionArena::new(vec![(
                ExpressionKind::Constant(record(crate::MAX_OBJECT_FIELDS + 1)),
                record_type,
            )])
            .is_err()
        );
    }
}
