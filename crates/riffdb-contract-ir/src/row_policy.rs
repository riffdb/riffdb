//! Closed, bounded principal-aware row-policy executable IR.

use std::collections::BTreeMap;

use riffdb_types::{
    CanonicalValue, EntityTypeId, FieldId, IndexId, decode_canonical_value, encode_canonical_value,
};

use crate::bundle::{decode_value_type, encode_value_type};
use crate::codec::{Reader, Writer};
use crate::{
    IrValidationError, SchemaIr, ValueType, ValueTypeTag, checked_len, validate_source_name,
};

/// Canonical row-policy catalog encoding version.
pub const ROW_POLICY_CATALOG_VERSION_V1: u32 = 1;
/// Maximum compiler-visible fact schemas in one contract.
pub const MAX_PRINCIPAL_FACT_SCHEMAS_V1: usize = riffdb_types::MAX_PRINCIPAL_FACTS_V1;
/// Maximum values carried by one list-valued principal fact.
pub const MAX_PRINCIPAL_FACT_VALUES_V1: usize = riffdb_types::MAX_PRINCIPAL_FACT_VALUES_V1;
/// Maximum canonical bytes across one principal's complete fact set.
pub const MAX_PRINCIPAL_FACT_BYTES_V1: usize = riffdb_types::MAX_PRINCIPAL_FACT_BYTES_V1;
/// Maximum named row policies in one contract.
pub const MAX_ROW_POLICIES_V1: usize = 1_024;
/// Maximum operation rules in one row policy.
pub const MAX_ROW_POLICY_RULES_V1: usize = 4;
/// Maximum expression nodes in one operation rule.
pub const MAX_ROW_POLICY_NODES_V1: usize = 128;
/// Maximum disjunctive leaves in one operation rule.
pub const MAX_ROW_POLICY_DISJUNCTION_V1: usize = 16;
/// Maximum indexed relationship probes in one operation rule.
pub const MAX_ROW_POLICY_RELATIONSHIP_PROBES_V1: usize = 1;
/// Maximum complete index arguments in one relationship probe.
pub const MAX_ROW_POLICY_INDEX_ARGUMENTS_V1: usize = 16;
/// Maximum canonical bytes for one complete policy catalog.
pub const MAX_ROW_POLICY_CATALOG_BYTES_V1: usize = 1024 * 1024;

/// One compiler-visible bounded principal-fact schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalFactSchemaV1 {
    name: String,
    value_type: ValueType,
}

impl PrincipalFactSchemaV1 {
    /// Constructs one scalar or bounded-list fact schema.
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "principal fact")?;
        let valid = value_type.is_projection_group_scalar()
            || value_type.list_parts().is_some_and(|(element, maximum)| {
                element.is_projection_group_scalar() && maximum <= MAX_PRINCIPAL_FACT_VALUES_V1
            });
        if !valid {
            return Err(IrValidationError::TypeMismatch {
                context: "principal fact schema",
            });
        }
        Ok(Self { name, value_type })
    }

    /// Symbolic fact name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Public fact value type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Renders the exact public symbolic type without stable numeric IDs.
    pub fn public_type_name(&self, schema: &SchemaIr) -> Result<String, IrValidationError> {
        render_public_type(&self.value_type, schema)
    }
}

fn render_public_type(
    value_type: &ValueType,
    schema: &SchemaIr,
) -> Result<String, IrValidationError> {
    let value = match value_type.tag() {
        ValueTypeTag::Bool => "Bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value_type
                .decimal_spec()
                .ok_or(IrValidationError::TypeMismatch {
                    context: "principal fact type",
                })?;
            format!("Decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => format!(
            "Money<{}>",
            value_type
                .currency()
                .ok_or(IrValidationError::TypeMismatch {
                    context: "principal fact type",
                })?
        ),
        ValueTypeTag::String => format!(
            "String<{}>",
            value_type
                .byte_bound()
                .ok_or(IrValidationError::TypeMismatch {
                    context: "principal fact type",
                })?
        ),
        ValueTypeTag::Bytes => format!(
            "Bytes<{}>",
            value_type
                .byte_bound()
                .ok_or(IrValidationError::TypeMismatch {
                    context: "principal fact type",
                })?
        ),
        ValueTypeTag::Timestamp => "Timestamp".to_owned(),
        ValueTypeTag::Date => "Date".to_owned(),
        ValueTypeTag::Uuid => "Uuid".to_owned(),
        ValueTypeTag::Enum => schema
            .enumeration(
                value_type
                    .enum_type_id()
                    .ok_or(IrValidationError::TypeMismatch {
                        context: "principal fact type",
                    })?,
            )
            .map(|enumeration| enumeration.name().to_owned())
            .ok_or(IrValidationError::InvalidReference {
                kind: "principal fact enum",
            })?,
        ValueTypeTag::List => {
            let (element, maximum) =
                value_type
                    .list_parts()
                    .ok_or(IrValidationError::TypeMismatch {
                        context: "principal fact type",
                    })?;
            format!("List<{},{}>", render_public_type(element, schema)?, maximum)
        }
        ValueTypeTag::Optional | ValueTypeTag::Record | ValueTypeTag::Vector => {
            return Err(IrValidationError::TypeMismatch {
                context: "principal fact type",
            });
        }
    };
    Ok(value)
}

/// Closed row-policy operation classes. A missing rule denies the operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RowPolicyOperationV1 {
    /// Read one current row.
    Read,
    /// Create one proposed row.
    Create,
    /// Update a current row to a proposed successor.
    Update,
    /// Delete one current row.
    Delete,
}

impl RowPolicyOperationV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::Read => 1,
            Self::Create => 2,
            Self::Update => 3,
            Self::Delete => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, IrValidationError> {
        match tag {
            1 => Ok(Self::Read),
            2 => Ok(Self::Create),
            3 => Ok(Self::Update),
            4 => Ok(Self::Delete),
            _ => Err(IrValidationError::UnknownTag {
                kind: "row policy operation",
                tag,
            }),
        }
    }
}

/// Closed source of one typed policy operand.
#[derive(Clone, Eq, PartialEq)]
pub enum RowPolicyValueSourceV1 {
    /// Field of the policy's protected row.
    RowField(FieldId),
    /// Stable authenticated principal ID, represented as UUID in policy expressions.
    PrincipalId,
    /// Closed authenticated actor-kind spelling.
    PrincipalKind,
    /// Current capability fact by compiler-visible name.
    PrincipalFact(String),
    /// Redacted canonical constant.
    Constant(CanonicalValue),
}

impl std::fmt::Debug for RowPolicyValueSourceV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowField(field) => formatter.debug_tuple("RowField").field(field).finish(),
            Self::PrincipalId => formatter.write_str("PrincipalId"),
            Self::PrincipalKind => formatter.write_str("PrincipalKind"),
            Self::PrincipalFact(name) => {
                formatter.debug_tuple("PrincipalFact").field(name).finish()
            }
            Self::Constant(_) => formatter.write_str("Constant([REDACTED])"),
        }
    }
}

/// One exact typed operand retained in a policy expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyOperandV1 {
    source: RowPolicyValueSourceV1,
    value_type: ValueType,
}

impl RowPolicyOperandV1 {
    /// Constructs an operand. Catalog construction validates its source binding.
    #[must_use]
    pub const fn new(source: RowPolicyValueSourceV1, value_type: ValueType) -> Self {
        Self { source, value_type }
    }

    /// Operand source.
    #[must_use]
    pub const fn source(&self) -> &RowPolicyValueSourceV1 {
        &self.source
    }

    /// Exact operand type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }
}

/// One topologically ordered policy-expression node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPolicyExpressionNodeV1 {
    /// Typed leaf operand.
    Operand(RowPolicyOperandV1),
    /// Exact equality.
    Equal {
        /// Left operand node.
        left: u16,
        /// Right operand node.
        right: u16,
    },
    /// Exact inequality.
    NotEqual {
        /// Left operand node.
        left: u16,
        /// Right operand node.
        right: u16,
    },
    /// Boolean negation.
    Not {
        /// Boolean operand node.
        value: u16,
    },
    /// Boolean conjunction.
    And {
        /// Left boolean node.
        left: u16,
        /// Right boolean node.
        right: u16,
    },
    /// Boolean disjunction.
    Or {
        /// Left boolean node.
        left: u16,
        /// Right boolean node.
        right: u16,
    },
    /// Scalar membership in a compiler-declared bounded list fact.
    In {
        /// Scalar candidate node.
        needle: u16,
        /// Bounded list node.
        haystack: u16,
    },
    /// Explicit optional-null test.
    IsNull {
        /// Optional operand node.
        value: u16,
        /// Whether this is `is not null`.
        negated: bool,
    },
    /// One exact declared index probe in the same logical partition.
    IndexedExists {
        /// Target entity.
        target_entity: EntityTypeId,
        /// Exact target index.
        index_id: IndexId,
        /// Complete typed index-key operands.
        arguments: Vec<RowPolicyOperandV1>,
    },
}

/// One operation-specific allow rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyRuleV1 {
    operation: RowPolicyOperationV1,
    nodes: Vec<RowPolicyExpressionNodeV1>,
    root: u16,
}

impl RowPolicyRuleV1 {
    /// Constructs and validates one finite topologically ordered boolean rule.
    pub fn new(
        operation: RowPolicyOperationV1,
        nodes: Vec<RowPolicyExpressionNodeV1>,
        root: u16,
        entity: EntityTypeId,
        schema: &SchemaIr,
        facts: &BTreeMap<String, ValueType>,
    ) -> Result<Self, IrValidationError> {
        if nodes.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "row policy nodes",
            });
        }
        checked_len("row policy nodes", nodes.len(), MAX_ROW_POLICY_NODES_V1)?;
        if usize::from(root) >= nodes.len() {
            return Err(IrValidationError::InvalidReference {
                kind: "row policy root",
            });
        }
        let mut types: Vec<ValueType> = Vec::with_capacity(nodes.len());
        let mut disjunctions: Vec<usize> = Vec::with_capacity(nodes.len());
        let mut relationship_probes = 0_usize;
        for (position, node) in nodes.iter().enumerate() {
            let prior = |id: u16| -> Result<usize, IrValidationError> {
                let id = usize::from(id);
                if id >= position {
                    Err(IrValidationError::NonForwardExpression)
                } else {
                    Ok(id)
                }
            };
            let (value_type, disjunction_width) = match node {
                RowPolicyExpressionNodeV1::Operand(operand) => {
                    validate_operand(operand, entity, schema, facts)?;
                    (operand.value_type.clone(), 1)
                }
                RowPolicyExpressionNodeV1::Equal { left, right }
                | RowPolicyExpressionNodeV1::NotEqual { left, right } => {
                    let left = prior(*left)?;
                    let right = prior(*right)?;
                    if !types[left].accepts_contextual(&types[right])
                        && !types[right].accepts_contextual(&types[left])
                    {
                        return Err(IrValidationError::TypeMismatch {
                            context: "row policy equality",
                        });
                    }
                    (ValueType::bool(), 1)
                }
                RowPolicyExpressionNodeV1::Not { value } => {
                    let value = prior(*value)?;
                    require_bool(&types[value])?;
                    (ValueType::bool(), disjunctions[value])
                }
                RowPolicyExpressionNodeV1::And { left, right } => {
                    let left = prior(*left)?;
                    let right = prior(*right)?;
                    require_bool(&types[left])?;
                    require_bool(&types[right])?;
                    (
                        ValueType::bool(),
                        disjunctions[left].max(disjunctions[right]),
                    )
                }
                RowPolicyExpressionNodeV1::Or { left, right } => {
                    let left = prior(*left)?;
                    let right = prior(*right)?;
                    require_bool(&types[left])?;
                    require_bool(&types[right])?;
                    let width = disjunctions[left].checked_add(disjunctions[right]).ok_or(
                        IrValidationError::SizeOverflow {
                            kind: "row policy disjunction",
                        },
                    )?;
                    if width > MAX_ROW_POLICY_DISJUNCTION_V1 {
                        return Err(IrValidationError::LimitExceeded {
                            kind: "row policy disjunction",
                            actual: width,
                            maximum: MAX_ROW_POLICY_DISJUNCTION_V1,
                        });
                    }
                    (ValueType::bool(), width)
                }
                RowPolicyExpressionNodeV1::In { needle, haystack } => {
                    let needle = prior(*needle)?;
                    let haystack = prior(*haystack)?;
                    let Some((element, maximum)) = types[haystack].list_parts() else {
                        return Err(IrValidationError::TypeMismatch {
                            context: "row policy membership",
                        });
                    };
                    let needle_matches = element.accepts_contextual(&types[needle])
                        || types[needle]
                            .optional_inner()
                            .is_some_and(|inner| inner == element);
                    if maximum > MAX_PRINCIPAL_FACT_VALUES_V1 || !needle_matches {
                        return Err(IrValidationError::TypeMismatch {
                            context: "row policy membership",
                        });
                    }
                    (ValueType::bool(), 1)
                }
                RowPolicyExpressionNodeV1::IsNull { value, .. } => {
                    let value = prior(*value)?;
                    if !types[value].is_optional() {
                        return Err(IrValidationError::TypeMismatch {
                            context: "row policy null test",
                        });
                    }
                    (ValueType::bool(), 1)
                }
                RowPolicyExpressionNodeV1::IndexedExists {
                    target_entity,
                    index_id,
                    arguments,
                } => {
                    relationship_probes += 1;
                    if relationship_probes > MAX_ROW_POLICY_RELATIONSHIP_PROBES_V1 {
                        return Err(IrValidationError::LimitExceeded {
                            kind: "row policy relationship probes",
                            actual: relationship_probes,
                            maximum: MAX_ROW_POLICY_RELATIONSHIP_PROBES_V1,
                        });
                    }
                    checked_len(
                        "row policy index arguments",
                        arguments.len(),
                        MAX_ROW_POLICY_INDEX_ARGUMENTS_V1,
                    )?;
                    let target = schema.entity(*target_entity).ok_or(
                        IrValidationError::InvalidReference {
                            kind: "row policy target entity",
                        },
                    )?;
                    let source =
                        schema
                            .entity(entity)
                            .ok_or(IrValidationError::InvalidReference {
                                kind: "row policy entity",
                            })?;
                    let source_owner = schema.aggregate_for_entity(entity).ok_or(
                        IrValidationError::InvalidReference {
                            kind: "row policy aggregate owner",
                        },
                    )?;
                    let target_owner = schema.aggregate_for_entity(*target_entity).ok_or(
                        IrValidationError::InvalidReference {
                            kind: "row policy target aggregate owner",
                        },
                    )?;
                    if source_owner.id() != target_owner.id() {
                        return Err(IrValidationError::InvalidDependency {
                            reason: "row policy relationship crosses aggregate",
                        });
                    }
                    let index = target
                        .indexes()
                        .iter()
                        .find(|index| index.id() == *index_id)
                        .ok_or(IrValidationError::InvalidReference {
                            kind: "row policy target index",
                        })?;
                    if index.fields().len() != arguments.len() {
                        return Err(IrValidationError::TypeMismatch {
                            context: "row policy target index arguments",
                        });
                    }
                    let route_width = schema
                        .entity(target_owner.root())
                        .ok_or(IrValidationError::InvalidReference {
                            kind: "row policy aggregate root",
                        })?
                        .primary_key_fields()
                        .len();
                    if index.fields().len() < route_width
                        || target.primary_key_fields().len() < route_width
                        || index.fields()[..route_width]
                            != target.primary_key_fields()[..route_width]
                    {
                        return Err(IrValidationError::InvalidDependency {
                            reason: "row policy relationship lacks partition route",
                        });
                    }
                    for (argument, field_id) in arguments.iter().zip(index.fields()) {
                        validate_operand(argument, entity, schema, facts)?;
                        let expected = target.record().field(*field_id).ok_or(
                            IrValidationError::InvalidReference {
                                kind: "row policy target index field",
                            },
                        )?;
                        if !expected
                            .value_type()
                            .accepts_contextual(argument.value_type())
                        {
                            return Err(IrValidationError::TypeMismatch {
                                context: "row policy target index argument",
                            });
                        }
                    }
                    for (argument, target_field_id) in
                        arguments.iter().zip(index.fields()).take(route_width)
                    {
                        let RowPolicyValueSourceV1::RowField(source_field_id) = argument.source()
                        else {
                            return Err(IrValidationError::InvalidDependency {
                                reason: "row policy relationship partition argument",
                            });
                        };
                        let source_field = source.record().field(*source_field_id).ok_or(
                            IrValidationError::InvalidReference {
                                kind: "row policy source partition field",
                            },
                        )?;
                        let target_field = target.record().field(*target_field_id).ok_or(
                            IrValidationError::InvalidReference {
                                kind: "row policy target partition field",
                            },
                        )?;
                        if source_field.name() != target_field.name()
                            || source_field.value_type() != target_field.value_type()
                        {
                            return Err(IrValidationError::InvalidDependency {
                                reason: "row policy relationship partition mismatch",
                            });
                        }
                    }
                    (ValueType::bool(), 1)
                }
            };
            types.push(value_type);
            disjunctions.push(disjunction_width);
        }
        require_bool(&types[usize::from(root)])?;
        Ok(Self {
            operation,
            nodes,
            root,
        })
    }

    /// Protected operation.
    #[must_use]
    pub const fn operation(&self) -> RowPolicyOperationV1 {
        self.operation
    }

    /// Topologically ordered expression nodes.
    #[must_use]
    pub fn nodes(&self) -> &[RowPolicyExpressionNodeV1] {
        &self.nodes
    }

    /// Root expression node.
    #[must_use]
    pub const fn root(&self) -> u16 {
        self.root
    }
}

/// One named policy over one entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyPlanV1 {
    name: String,
    entity: EntityTypeId,
    rules: Vec<RowPolicyRuleV1>,
}

impl RowPolicyPlanV1 {
    /// Constructs a canonical policy with at most one rule per operation.
    pub fn new(
        name: impl Into<String>,
        entity: EntityTypeId,
        mut rules: Vec<RowPolicyRuleV1>,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "row policy")?;
        if schema.entity(entity).is_none() {
            return Err(IrValidationError::InvalidReference {
                kind: "row policy entity",
            });
        }
        if rules.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "row policy rules",
            });
        }
        checked_len("row policy rules", rules.len(), MAX_ROW_POLICY_RULES_V1)?;
        rules.sort_unstable_by_key(RowPolicyRuleV1::operation);
        if rules
            .windows(2)
            .any(|pair| pair[0].operation == pair[1].operation)
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "row policy operations",
            });
        }
        Ok(Self {
            name,
            entity,
            rules,
        })
    }

    /// Symbolic policy name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Protected entity.
    #[must_use]
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }

    /// Operation rules in canonical operation order.
    #[must_use]
    pub fn rules(&self) -> &[RowPolicyRuleV1] {
        &self.rules
    }
}

/// Complete compiler-owned principal-fact and row-policy catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyCatalogV1 {
    facts: Vec<PrincipalFactSchemaV1>,
    policies: Vec<RowPolicyPlanV1>,
}

impl RowPolicyCatalogV1 {
    /// Constructs a canonical checked catalog.
    pub fn new(
        mut facts: Vec<PrincipalFactSchemaV1>,
        policies: Vec<RowPolicyPlanV1>,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        checked_len(
            "principal fact schemas",
            facts.len(),
            MAX_PRINCIPAL_FACT_SCHEMAS_V1,
        )?;
        checked_len("row policies", policies.len(), MAX_ROW_POLICIES_V1)?;
        facts.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if facts.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "principal fact schemas",
            });
        }
        let fact_types = facts
            .iter()
            .map(|fact| (fact.name.clone(), fact.value_type.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut checked_policies = Vec::with_capacity(policies.len());
        for policy in policies {
            let mut checked_rules = Vec::with_capacity(policy.rules.len());
            for rule in policy.rules {
                checked_rules.push(RowPolicyRuleV1::new(
                    rule.operation,
                    rule.nodes,
                    rule.root,
                    policy.entity,
                    schema,
                    &fact_types,
                )?);
            }
            checked_policies.push(RowPolicyPlanV1::new(
                policy.name,
                policy.entity,
                checked_rules,
                schema,
            )?);
        }
        checked_policies.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if checked_policies
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "row policies",
            });
        }
        Ok(Self {
            facts,
            policies: checked_policies,
        })
    }

    /// Empty catalog used by bundle versions before row policies.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            facts: Vec::new(),
            policies: Vec::new(),
        }
    }

    /// Fact schemas in canonical name order.
    #[must_use]
    pub fn facts(&self) -> &[PrincipalFactSchemaV1] {
        &self.facts
    }

    /// Policies in canonical name order.
    #[must_use]
    pub fn policies(&self) -> &[RowPolicyPlanV1] {
        &self.policies
    }

    /// Whether the catalog carries no policy semantics.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.facts.is_empty() && self.policies.is_empty()
    }

    /// Canonical standalone bytes used by bundle, role, and module identities.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, IrValidationError> {
        let mut writer = Writer::new(MAX_ROW_POLICY_CATALOG_BYTES_V1);
        encode_catalog(&mut writer, self)?;
        Ok(writer.finish())
    }

    /// Decodes and revalidates one catalog against its exact structural schema.
    pub fn decode(bytes: &[u8], schema: &SchemaIr) -> Result<Self, IrValidationError> {
        checked_len(
            "row policy catalog",
            bytes.len(),
            MAX_ROW_POLICY_CATALOG_BYTES_V1,
        )?;
        let mut reader = Reader::new(bytes);
        let catalog = decode_catalog(&mut reader, schema)?;
        reader.finish()?;
        Ok(catalog)
    }
}

fn require_bool(value_type: &ValueType) -> Result<(), IrValidationError> {
    if value_type == &ValueType::bool() {
        Ok(())
    } else {
        Err(IrValidationError::TypeMismatch {
            context: "row policy boolean expression",
        })
    }
}

fn validate_operand(
    operand: &RowPolicyOperandV1,
    entity: EntityTypeId,
    schema: &SchemaIr,
    facts: &BTreeMap<String, ValueType>,
) -> Result<(), IrValidationError> {
    match &operand.source {
        RowPolicyValueSourceV1::RowField(field) => {
            let expected = schema
                .entity(entity)
                .and_then(|entity| entity.record().field(*field))
                .ok_or(IrValidationError::InvalidReference {
                    kind: "row policy field",
                })?;
            if expected.value_type() != operand.value_type() {
                return Err(IrValidationError::TypeMismatch {
                    context: "row policy field",
                });
            }
        }
        RowPolicyValueSourceV1::PrincipalId => {
            if operand.value_type != ValueType::uuid() {
                return Err(IrValidationError::TypeMismatch {
                    context: "row policy principal ID",
                });
            }
        }
        RowPolicyValueSourceV1::PrincipalKind => {
            if operand.value_type != ValueType::string(16)? {
                return Err(IrValidationError::TypeMismatch {
                    context: "row policy principal kind",
                });
            }
        }
        RowPolicyValueSourceV1::PrincipalFact(name) => {
            validate_source_name(name, "principal fact reference")?;
            if facts.get(name) != Some(operand.value_type()) {
                return Err(IrValidationError::InvalidReference {
                    kind: "row policy principal fact",
                });
            }
        }
        RowPolicyValueSourceV1::Constant(value) => {
            operand.value_type.validate_value(value)?;
        }
    }
    Ok(())
}

pub(crate) fn encode_catalog(
    writer: &mut Writer,
    catalog: &RowPolicyCatalogV1,
) -> Result<(), IrValidationError> {
    writer.u32(ROW_POLICY_CATALOG_VERSION_V1)?;
    writer.u32(catalog.facts.len() as u32)?;
    for fact in &catalog.facts {
        writer.string(&fact.name)?;
        encode_value_type(writer, &fact.value_type)?;
    }
    writer.u32(catalog.policies.len() as u32)?;
    for policy in &catalog.policies {
        writer.string(&policy.name)?;
        writer.u32(policy.entity.get())?;
        writer.u32(policy.rules.len() as u32)?;
        for rule in &policy.rules {
            writer.u8(rule.operation.tag())?;
            writer.u32(rule.root.into())?;
            writer.u32(rule.nodes.len() as u32)?;
            for node in &rule.nodes {
                encode_node(writer, node)?;
            }
        }
    }
    Ok(())
}

fn encode_node(
    writer: &mut Writer,
    node: &RowPolicyExpressionNodeV1,
) -> Result<(), IrValidationError> {
    match node {
        RowPolicyExpressionNodeV1::Operand(operand) => {
            writer.u8(1)?;
            encode_operand(writer, operand)?;
        }
        RowPolicyExpressionNodeV1::Equal { left, right } => {
            encode_binary(writer, 2, *left, *right)?
        }
        RowPolicyExpressionNodeV1::NotEqual { left, right } => {
            encode_binary(writer, 3, *left, *right)?
        }
        RowPolicyExpressionNodeV1::Not { value } => {
            writer.u8(4)?;
            writer.u32((*value).into())?;
        }
        RowPolicyExpressionNodeV1::And { left, right } => encode_binary(writer, 5, *left, *right)?,
        RowPolicyExpressionNodeV1::Or { left, right } => encode_binary(writer, 6, *left, *right)?,
        RowPolicyExpressionNodeV1::In { needle, haystack } => {
            encode_binary(writer, 7, *needle, *haystack)?
        }
        RowPolicyExpressionNodeV1::IsNull { value, negated } => {
            writer.u8(8)?;
            writer.u32((*value).into())?;
            writer.bool(*negated)?;
        }
        RowPolicyExpressionNodeV1::IndexedExists {
            target_entity,
            index_id,
            arguments,
        } => {
            writer.u8(9)?;
            writer.u32(target_entity.get())?;
            writer.u32(index_id.get())?;
            writer.u32(arguments.len() as u32)?;
            for argument in arguments {
                encode_operand(writer, argument)?;
            }
        }
    }
    Ok(())
}

fn encode_binary(
    writer: &mut Writer,
    tag: u8,
    left: u16,
    right: u16,
) -> Result<(), IrValidationError> {
    writer.u8(tag)?;
    writer.u32(left.into())?;
    writer.u32(right.into())
}

fn encode_operand(
    writer: &mut Writer,
    operand: &RowPolicyOperandV1,
) -> Result<(), IrValidationError> {
    match &operand.source {
        RowPolicyValueSourceV1::RowField(field) => {
            writer.u8(1)?;
            writer.u32(field.get())?;
        }
        RowPolicyValueSourceV1::PrincipalId => writer.u8(2)?,
        RowPolicyValueSourceV1::PrincipalKind => writer.u8(3)?,
        RowPolicyValueSourceV1::PrincipalFact(name) => {
            writer.u8(4)?;
            writer.string(name)?;
        }
        RowPolicyValueSourceV1::Constant(value) => {
            writer.u8(5)?;
            writer.bytes(&encode_canonical_value(value).map_err(|_| {
                IrValidationError::InvalidText {
                    kind: "row policy constant",
                }
            })?)?;
        }
    }
    encode_value_type(writer, &operand.value_type)
}

pub(crate) fn decode_catalog(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
) -> Result<RowPolicyCatalogV1, IrValidationError> {
    let version = reader.u32()?;
    if version != ROW_POLICY_CATALOG_VERSION_V1 {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "row policy catalog",
            value: version,
        });
    }
    let fact_count = reader.u32()? as usize;
    checked_len(
        "principal fact schemas",
        fact_count,
        MAX_PRINCIPAL_FACT_SCHEMAS_V1,
    )?;
    let mut facts = Vec::with_capacity(fact_count);
    for _ in 0..fact_count {
        facts.push(PrincipalFactSchemaV1::new(
            reader.string(256)?,
            decode_value_type(reader, 0)?,
        )?);
    }
    let fact_map = facts
        .iter()
        .map(|fact| (fact.name.clone(), fact.value_type.clone()))
        .collect::<BTreeMap<_, _>>();
    let policy_count = reader.u32()? as usize;
    checked_len("row policies", policy_count, MAX_ROW_POLICIES_V1)?;
    let mut policies = Vec::with_capacity(policy_count);
    for _ in 0..policy_count {
        let name = reader.string(256)?;
        let entity =
            EntityTypeId::new(reader.u32()?).ok_or(IrValidationError::InvalidReference {
                kind: "row policy entity",
            })?;
        let rule_count = reader.u32()? as usize;
        checked_len("row policy rules", rule_count, MAX_ROW_POLICY_RULES_V1)?;
        let mut rules = Vec::with_capacity(rule_count);
        for _ in 0..rule_count {
            let operation = RowPolicyOperationV1::from_tag(reader.u8()?)?;
            let root =
                u16::try_from(reader.u32()?).map_err(|_| IrValidationError::InvalidReference {
                    kind: "row policy root",
                })?;
            let node_count = reader.u32()? as usize;
            checked_len("row policy nodes", node_count, MAX_ROW_POLICY_NODES_V1)?;
            let mut nodes = Vec::with_capacity(node_count);
            for _ in 0..node_count {
                nodes.push(decode_node(reader, schema)?);
            }
            rules.push(RowPolicyRuleV1::new(
                operation, nodes, root, entity, schema, &fact_map,
            )?);
        }
        policies.push(RowPolicyPlanV1::new(name, entity, rules, schema)?);
    }
    RowPolicyCatalogV1::new(facts, policies, schema)
}

fn decode_node(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
) -> Result<RowPolicyExpressionNodeV1, IrValidationError> {
    let index = |reader: &mut Reader<'_>| -> Result<u16, IrValidationError> {
        u16::try_from(reader.u32()?).map_err(|_| IrValidationError::InvalidReference {
            kind: "row policy node",
        })
    };
    match reader.u8()? {
        1 => Ok(RowPolicyExpressionNodeV1::Operand(decode_operand(
            reader, schema,
        )?)),
        2 => Ok(RowPolicyExpressionNodeV1::Equal {
            left: index(reader)?,
            right: index(reader)?,
        }),
        3 => Ok(RowPolicyExpressionNodeV1::NotEqual {
            left: index(reader)?,
            right: index(reader)?,
        }),
        4 => Ok(RowPolicyExpressionNodeV1::Not {
            value: index(reader)?,
        }),
        5 => Ok(RowPolicyExpressionNodeV1::And {
            left: index(reader)?,
            right: index(reader)?,
        }),
        6 => Ok(RowPolicyExpressionNodeV1::Or {
            left: index(reader)?,
            right: index(reader)?,
        }),
        7 => Ok(RowPolicyExpressionNodeV1::In {
            needle: index(reader)?,
            haystack: index(reader)?,
        }),
        8 => Ok(RowPolicyExpressionNodeV1::IsNull {
            value: index(reader)?,
            negated: reader.bool()?,
        }),
        9 => {
            let target_entity =
                EntityTypeId::new(reader.u32()?).ok_or(IrValidationError::InvalidReference {
                    kind: "row policy target entity",
                })?;
            let index_id =
                IndexId::new(reader.u32()?).ok_or(IrValidationError::InvalidReference {
                    kind: "row policy target index",
                })?;
            let count = reader.u32()? as usize;
            checked_len(
                "row policy index arguments",
                count,
                MAX_ROW_POLICY_INDEX_ARGUMENTS_V1,
            )?;
            let mut arguments = Vec::with_capacity(count);
            for _ in 0..count {
                arguments.push(decode_operand(reader, schema)?);
            }
            Ok(RowPolicyExpressionNodeV1::IndexedExists {
                target_entity,
                index_id,
                arguments,
            })
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "row policy node",
            tag,
        }),
    }
}

fn decode_operand(
    reader: &mut Reader<'_>,
    _schema: &SchemaIr,
) -> Result<RowPolicyOperandV1, IrValidationError> {
    let source = match reader.u8()? {
        1 => RowPolicyValueSourceV1::RowField(FieldId::new(reader.u32()?).ok_or(
            IrValidationError::InvalidReference {
                kind: "row policy field",
            },
        )?),
        2 => RowPolicyValueSourceV1::PrincipalId,
        3 => RowPolicyValueSourceV1::PrincipalKind,
        4 => RowPolicyValueSourceV1::PrincipalFact(reader.string(256)?),
        5 => RowPolicyValueSourceV1::Constant(
            decode_canonical_value(reader.bytes(MAX_PRINCIPAL_FACT_BYTES_V1)?).map_err(|_| {
                IrValidationError::InvalidText {
                    kind: "row policy constant",
                }
            })?,
        ),
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "row policy operand",
                tag,
            });
        }
    };
    let value_type = decode_value_type(reader, 0)?;
    Ok(RowPolicyOperandV1::new(source, value_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EntitySchema, FieldSchema, KeyComponentSchema, KeyPurpose, KeySchema, RecordSchema,
        RecordTypeRef,
    };

    fn schema() -> SchemaIr {
        let entity = EntityTypeId::new(1).expect("entity ID");
        let field = FieldId::new(1).expect("field ID");
        let key = KeySchema::new(
            KeyPurpose::Entity(entity),
            vec![KeyComponentSchema::new(ValueType::uuid(), vec![]).expect("component")],
        )
        .expect("key");
        SchemaIr::new(
            vec![
                EntitySchema::new(
                    entity,
                    "Document",
                    RecordSchema::new(
                        RecordTypeRef::Entity(entity),
                        vec![
                            FieldSchema::new(field, "owner_id", ValueType::uuid()).expect("field"),
                        ],
                    )
                    .expect("record"),
                    vec![field],
                    key,
                    vec![],
                    vec![],
                )
                .expect("entity"),
            ],
            vec![],
            vec![],
            vec![],
        )
        .expect("schema")
    }

    #[test]
    fn catalog_round_trips_and_redacts_constants() {
        let schema = schema();
        let entity = EntityTypeId::new(1).expect("entity ID");
        let field = FieldId::new(1).expect("field ID");
        let facts = BTreeMap::new();
        let rule = RowPolicyRuleV1::new(
            RowPolicyOperationV1::Read,
            vec![
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::RowField(field),
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::PrincipalId,
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Equal { left: 0, right: 1 },
            ],
            2,
            entity,
            &schema,
            &facts,
        )
        .expect("rule");
        let catalog = RowPolicyCatalogV1::new(
            vec![],
            vec![
                RowPolicyPlanV1::new("DocumentAccess", entity, vec![rule], &schema)
                    .expect("policy"),
            ],
            &schema,
        )
        .expect("catalog");
        let encoded = catalog.canonical_bytes().expect("encode");
        assert_eq!(RowPolicyCatalogV1::decode(&encoded, &schema), Ok(catalog));
    }

    #[test]
    fn disjunction_and_relationship_bounds_fail_closed() {
        let schema = schema();
        let entity = EntityTypeId::new(1).expect("entity ID");
        let mut nodes = vec![RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
            RowPolicyValueSourceV1::Constant(CanonicalValue::Bool(true)),
            ValueType::bool(),
        ))];
        let mut root = 0_u16;
        for _ in 1..=MAX_ROW_POLICY_DISJUNCTION_V1 {
            nodes.push(RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::Constant(CanonicalValue::Bool(true)),
                ValueType::bool(),
            )));
            let right = u16::try_from(nodes.len() - 1).expect("bounded");
            nodes.push(RowPolicyExpressionNodeV1::Or { left: root, right });
            root = u16::try_from(nodes.len() - 1).expect("bounded");
        }
        assert!(
            RowPolicyRuleV1::new(
                RowPolicyOperationV1::Read,
                nodes,
                root,
                entity,
                &schema,
                &BTreeMap::new(),
            )
            .is_err()
        );
    }
}
