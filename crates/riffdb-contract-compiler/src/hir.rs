//! Compiler-private, source-spanned, resolved typed high-level IR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    BindingId, BindingMode, CommandInvocationClass, ExprId, ExpressionArena, ExpressionKind,
    ValueType, ValueTypeTag,
};
use riffdb_contract_syntax::Span;
use riffdb_contract_syntax::ast::{
    AggregateItem, Aggregation, Binding, BulkIteration, CommandDeclaration, CommandKind,
    Declaration, DeletePolicyDeclaration, Effect, EntityBinding, EntityItem,
    EventPolicyAnchorDeclaration, Expression, ObjectLiteral, OutcomeExpression, Path,
    RowPolicyOperation, ServiceValueKind, SetEffect, TypeExpression,
    WorkflowLeaseOperation as SyntaxWorkflowLeaseOperation,
};
use riffdb_contract_syntax::{ContractDocument, Spanned};
use riffdb_types::{
    AggregateTypeId, CanonicalValue, CommandId, ContractVersion, EntityTypeId, EnumTypeId,
    EnumVariantId, EventTypeId, FieldId, IndexId, InvariantId, OutcomeId, ProjectionId,
};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::expression_lowering::{
    BindingExpressionScope, CollectionElementExpressionScope, ExpressionLowerer, ExpressionScope,
};
use crate::symbols::GenesisSymbols;
use crate::typecheck::ResolvedTypes;

#[derive(Clone, Debug)]
pub(crate) struct HirExpressionNode {
    pub(crate) kind: ExpressionKind,
    pub(crate) value_type: ValueType,
    pub(crate) span: Span,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HirExpressionArena {
    pub(crate) nodes: Vec<HirExpressionNode>,
}

impl HirExpressionArena {
    pub(crate) fn to_ir(
        &self,
        enclosing_span: Span,
    ) -> Result<ExpressionArena, CompilerDiagnostic> {
        ExpressionArena::new(
            self.nodes
                .iter()
                .map(|node| (node.kind.clone(), node.value_type.clone()))
                .collect(),
        )
        .map_err(|_| CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidIr, enclosing_span))
    }

    pub(crate) fn node(&self, id: ExprId) -> Option<&HirExpressionNode> {
        self.nodes.get(id.get() as usize)
    }

    pub(crate) fn dependencies(
        &self,
        root: ExprId,
        enclosing_span: Span,
    ) -> Result<riffdb_contract_ir::ExpressionDependencies, CompilerDiagnostic> {
        self.to_ir(enclosing_span)?
            .dependencies(root)
            .map_err(|_| CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidIr, enclosing_span))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HirExpressionRoot {
    pub(crate) id: ExprId,
    pub(crate) value_type: ValueType,
    pub(crate) span: Span,
}

#[derive(Clone, Debug)]
pub(crate) struct HirTypedExpression {
    pub(crate) root: HirExpressionRoot,
    pub(crate) expressions: HirExpressionArena,
}

#[derive(Clone, Debug)]
pub(crate) struct HirField {
    pub(crate) id: FieldId,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) type_span: Span,
    pub(crate) value_type: ValueType,
    /// Span of the `secret` classification modifier (ADR-0118). Only stored
    /// entity fields can carry it; every other position lowers `None`.
    pub(crate) secret_span: Option<Span>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirEnumVariant {
    pub(crate) id: EnumVariantId,
    pub(crate) name: String,
    pub(crate) span: Span,
}

#[derive(Clone, Debug)]
pub(crate) struct HirEnum {
    pub(crate) id: EnumTypeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) variants: Vec<HirEnumVariant>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirInvariant {
    pub(crate) id: InvariantId,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) expression: HirExpressionRoot,
    pub(crate) expressions: HirExpressionArena,
}

#[derive(Clone, Debug)]
pub(crate) struct HirIndex {
    pub(crate) id: IndexId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) fields: Vec<(FieldId, Span)>,
    pub(crate) encodings: Vec<riffdb_contract_ir::IndexFieldEncodingV1>,
    pub(crate) unique: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct HirRelationship {
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) source_fields: Vec<(FieldId, Span)>,
    pub(crate) target_entity: EntityTypeId,
    pub(crate) target_entity_span: Span,
    pub(crate) target_fields: Vec<(FieldId, Span)>,
}

#[derive(Clone, Debug)]
pub(crate) enum HirDeletePolicy {
    NoInbound {
        span: Span,
    },
    Restrict {
        span: Span,
        source_entity: EntityTypeId,
        index_id: IndexId,
    },
    Cascade {
        span: Span,
        relationships: Vec<HirCascadeRelationship>,
    },
}

impl HirDeletePolicy {
    pub(crate) const fn span(&self) -> Span {
        match self {
            Self::NoInbound { span } | Self::Restrict { span, .. } | Self::Cascade { span, .. } => {
                *span
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HirCascadeRelationship {
    pub(crate) source_entity: EntityTypeId,
    pub(crate) relationship_name: String,
    pub(crate) index_id: IndexId,
    pub(crate) maximum: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct HirEntity {
    pub(crate) id: EntityTypeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) fields: Vec<HirField>,
    pub(crate) key_fields: Vec<FieldId>,
    pub(crate) invariants: Vec<HirInvariant>,
    pub(crate) indexes: Vec<HirIndex>,
    pub(crate) relationships: Vec<HirRelationship>,
    pub(crate) delete_policy: Option<HirDeletePolicy>,
    pub(crate) vector_fields: Vec<HirVectorField>,
}

/// One validated vector-field search configuration (ADR-0091): the resolved
/// metric, source fields, and staleness SLO that previously were validated
/// here and then discarded.
#[derive(Clone, Debug)]
pub(crate) struct HirVectorField {
    pub(crate) field_id: FieldId,
    pub(crate) metric: riffdb_types::DistanceMetric,
    pub(crate) source_fields: Vec<FieldId>,
    pub(crate) stale_entity_count_threshold: u64,
    pub(crate) ann_row_threshold: Option<u32>,
    pub(crate) recall_target_bps: Option<u32>,
    pub(crate) span: Span,
}

impl HirEntity {
    pub(crate) fn fields_by_name(&self) -> BTreeMap<String, (FieldId, ValueType)> {
        self.fields
            .iter()
            .map(|field| (field.name.clone(), (field.id, field.value_type.clone())))
            .collect()
    }

    pub(crate) fn key_field_set(&self) -> BTreeSet<FieldId> {
        self.key_fields.iter().copied().collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HirEvent {
    pub(crate) id: EventTypeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) partition_fields: Vec<(FieldId, Span)>,
    pub(crate) partition_span: Option<Span>,
    pub(crate) policy_anchor: Option<HirEventPolicyAnchor>,
    pub(crate) fields: Vec<HirField>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirEventPolicyAnchor {
    pub(crate) source_entity: EntityTypeId,
    pub(crate) key_fields: Vec<(FieldId, FieldId)>,
    pub(crate) read_policy: String,
    pub(crate) span: Span,
}

impl HirEvent {
    pub(crate) fn fields_by_name(&self) -> BTreeMap<String, (FieldId, ValueType)> {
        self.fields
            .iter()
            .map(|field| (field.name.clone(), (field.id, field.value_type.clone())))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HirAggregateKeys {
    pub(crate) expressions: HirExpressionArena,
    pub(crate) partition: HirExpressionRoot,
    pub(crate) conflicts: Vec<HirExpressionRoot>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirAggregate {
    pub(crate) id: AggregateTypeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) root: EntityTypeId,
    pub(crate) root_span: Span,
    pub(crate) children: Vec<(EntityTypeId, Span)>,
    pub(crate) keys: HirAggregateKeys,
    pub(crate) invariants: Vec<HirInvariant>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirInput {
    pub(crate) field: HirField,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HirServiceValueKind {
    UuidV7,
    TransactionTime,
}

#[derive(Clone, Debug)]
pub(crate) struct HirServiceValue {
    pub(crate) field: HirField,
    pub(crate) kind: HirServiceValueKind,
}

#[derive(Clone, Debug)]
pub(crate) struct HirWorkflowTransition {
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) source_states: Vec<EnumVariantId>,
    pub(crate) destination: EnumVariantId,
}

#[derive(Clone, Debug)]
pub(crate) struct HirWorkflowLease {
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) owner_field: FieldId,
    pub(crate) expiry_field: FieldId,
    pub(crate) fencing_token_field: FieldId,
    pub(crate) attempt_field: Option<FieldId>,
    pub(crate) minimum_duration_seconds: u64,
    pub(crate) maximum_duration_seconds: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct HirWorkflow {
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) entity_id: EntityTypeId,
    pub(crate) state_field: FieldId,
    pub(crate) state_enum: EnumTypeId,
    pub(crate) initial_state: Option<EnumVariantId>,
    pub(crate) transitions: Vec<HirWorkflowTransition>,
    pub(crate) lease: Option<HirWorkflowLease>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirObjectField {
    pub(crate) id: FieldId,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) value: HirExpressionRoot,
    pub(crate) reveals: Vec<HirSecretReveal>,
}

/// One statically declared disclosure of a secret-classified bound field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HirSecretReveal {
    pub(crate) binding: BindingId,
    pub(crate) entity: EntityTypeId,
    pub(crate) field: FieldId,
    pub(crate) annotation_span: Span,
    pub(crate) source_span: Span,
}

#[derive(Clone, Debug)]
struct SecretRevealScopeEntry {
    binding: BindingId,
    entity: EntityTypeId,
    fields: BTreeMap<String, (FieldId, Span, bool)>,
}

type SecretRevealScope = BTreeMap<String, SecretRevealScopeEntry>;

#[derive(Clone, Debug)]
pub(crate) struct HirOutcome {
    pub(crate) id: OutcomeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) fields: Vec<HirObjectField>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirBinding {
    pub(crate) id: BindingId,
    pub(crate) mode: BindingMode,
    pub(crate) entity_id: EntityTypeId,
    pub(crate) entity_span: Span,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) span: Span,
    pub(crate) arguments: Vec<HirExpressionRoot>,
    pub(crate) failure: HirOutcome,
    pub(crate) restriction_failure: Option<HirOutcome>,
    pub(crate) cascade_failure: Option<HirOutcome>,
    pub(crate) collection_local: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct HirRequirement {
    pub(crate) name_span: Span,
    pub(crate) condition: HirExpressionRoot,
    pub(crate) rejection: HirOutcome,
}

#[derive(Clone, Debug)]
pub(crate) enum HirEffect {
    Set {
        target_span: Span,
        binding: BindingId,
        field: FieldId,
        value: HirExpressionRoot,
        reveals: Vec<HirSecretReveal>,
    },
    Emit {
        event_id: EventTypeId,
        event_span: Span,
        fields: Vec<HirObjectField>,
    },
    WorkflowTransition {
        transition_span: Span,
        binding: BindingId,
        state_field: FieldId,
        source_states: Vec<EnumVariantId>,
        destination: EnumVariantId,
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        illegal: HirOutcome,
    },
    WorkflowLease {
        lease_span: Span,
        binding: BindingId,
        owner_field: FieldId,
        expiry_field: FieldId,
        fencing_token_field: FieldId,
        attempt_field: Option<FieldId>,
        minimum_duration_seconds: u64,
        maximum_duration_seconds: u64,
        operation: Box<HirWorkflowLeaseOperation>,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum HirWorkflowLeaseOperation {
    Claim {
        owner: HirExpressionRoot,
        duration_seconds: HirExpressionRoot,
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        unavailable: HirOutcome,
        invalid: HirOutcome,
        exhausted: HirOutcome,
    },
    Renew {
        owner: HirExpressionRoot,
        fencing_token: HirExpressionRoot,
        duration_seconds: HirExpressionRoot,
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        invalid: HirOutcome,
        expired: HirOutcome,
        exhausted: HirOutcome,
    },
    Release {
        owner: HirExpressionRoot,
        fencing_token: HirExpressionRoot,
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        invalid: HirOutcome,
    },
    Expire {
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        active: HirOutcome,
    },
    Fence {
        owner: HirExpressionRoot,
        fencing_token: HirExpressionRoot,
        expected_revision: HirExpressionRoot,
        stale: HirOutcome,
        invalid: HirOutcome,
        expired: HirOutcome,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct HirCommand {
    pub(crate) id: CommandId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) inputs: Vec<HirInput>,
    pub(crate) service_values: Vec<HirServiceValue>,
    pub(crate) idempotency: Option<HirTypedExpression>,
    pub(crate) bindings: Vec<HirBinding>,
    pub(crate) requirements: Vec<HirRequirement>,
    pub(crate) effects: Vec<HirEffect>,
    pub(crate) success: HirOutcome,
    pub(crate) expressions: HirExpressionArena,
    pub(crate) collection_expansion: Option<HirCollectionExpansion>,
    pub(crate) invocation_class: CommandInvocationClass,
}

#[derive(Clone, Debug)]
pub(crate) struct HirCollectionExpansion {
    pub(crate) input_field: FieldId,
    pub(crate) minimum_elements: usize,
    pub(crate) maximum_elements: usize,
    pub(crate) element_type: ValueType,
    pub(crate) first_binding: BindingId,
    pub(crate) binding_count: usize,
    pub(crate) repeated_requirement_count: usize,
    pub(crate) repeated_effect_count: usize,
    pub(crate) span: Span,
}

#[derive(Clone, Debug)]
pub(crate) enum HirMeasureKind {
    Count,
    Sum(HirExpressionRoot),
}

#[derive(Clone, Debug)]
pub(crate) struct HirMeasure {
    pub(crate) id: FieldId,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) kind: HirMeasureKind,
}

#[derive(Clone, Debug)]
pub(crate) struct HirProjection {
    pub(crate) id: ProjectionId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) source_event: EventTypeId,
    pub(crate) filter: Option<HirExpressionRoot>,
    pub(crate) key: Vec<HirExpressionRoot>,
    pub(crate) measures: Vec<HirMeasure>,
    pub(crate) frontier: HirFrontier,
    pub(crate) expressions: HirExpressionArena,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HirFrontier {
    TransactionallyOrdered,
}

#[derive(Clone, Debug)]
pub(crate) struct TypedContractHir {
    pub(crate) span: Span,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) contract_version: ContractVersion,
    pub(crate) enums: Vec<HirEnum>,
    pub(crate) entities: Vec<HirEntity>,
    pub(crate) events: Vec<HirEvent>,
    pub(crate) aggregates: Vec<HirAggregate>,
    pub(crate) workflows: Vec<HirWorkflow>,
    pub(crate) commands: Vec<HirCommand>,
    pub(crate) projections: Vec<HirProjection>,
}

impl TypedContractHir {
    pub(crate) fn entity(&self, id: EntityTypeId) -> Option<&HirEntity> {
        self.entities.iter().find(|entity| entity.id == id)
    }

    pub(crate) fn aggregate(&self, id: AggregateTypeId) -> Option<&HirAggregate> {
        self.aggregates.iter().find(|aggregate| aggregate.id == id)
    }

    pub(crate) fn aggregate_for_entity(&self, id: EntityTypeId) -> Option<&HirAggregate> {
        self.aggregates.iter().find(|aggregate| {
            aggregate.root == id || aggregate.children.iter().any(|child| child.0 == id)
        })
    }

    pub(crate) fn command_names(
        &self,
    ) -> Vec<(CommandId, riffdb_contract_syntax::Spanned<String>)> {
        self.commands
            .iter()
            .filter(|command| command.invocation_class == CommandInvocationClass::Application)
            .map(|command| {
                (
                    command.id,
                    riffdb_contract_syntax::Spanned::new(command.name.clone(), command.span),
                )
            })
            .collect()
    }
}

/// Resolves the parsed source tree into the only representation consumed by
/// semantic analyses and executable lowering.
pub(crate) fn lower_contract_hir(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
) -> Result<TypedContractHir, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let enums = lower_enums(document, symbols);
    let entities = lower_entities(document, symbols, types, &mut diagnostics);
    let events = lower_events(document, symbols, types, &entities, &mut diagnostics);
    let aggregates = lower_aggregates(document, symbols, types, &mut diagnostics);
    let workflows = lower_workflows(document, symbols, &entities, &aggregates, &mut diagnostics);
    let commands = lower_commands(
        document,
        symbols,
        types,
        &entities,
        &events,
        &workflows,
        &mut diagnostics,
    );
    let projections = lower_projections(document, symbols, &events, &mut diagnostics);
    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }
    Ok(TypedContractHir {
        span: document.contract.span,
        name: document.contract.value.name.value.clone(),
        name_span: document.contract.value.name.span,
        contract_version: symbols.contract_version,
        enums,
        entities,
        events,
        aggregates,
        workflows,
        commands,
        projections,
    })
}

fn lower_enums(document: &ContractDocument, symbols: &GenesisSymbols) -> Vec<HirEnum> {
    document
        .contract
        .value
        .declarations
        .iter()
        .filter_map(|declaration| {
            let Declaration::Enum(source) = &declaration.value else {
                return None;
            };
            let id = symbols.enums.get(&source.name.value).copied()?;
            let variants = source
                .variants
                .iter()
                .filter_map(|variant| {
                    symbols
                        .enum_variants
                        .get(&(id, variant.value.clone()))
                        .copied()
                        .map(|variant_id| HirEnumVariant {
                            id: variant_id,
                            name: variant.value.clone(),
                            span: variant.span,
                        })
                })
                .collect();
            Some(HirEnum {
                id,
                name: source.name.value.clone(),
                span: source.name.span,
                variants,
            })
        })
        .collect()
}

fn lower_entities(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirEntity> {
    let mut result = Vec::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::Entity(source) = &declaration.value else {
            continue;
        };
        let Some(id) = symbols.entities.get(&source.name.value).copied() else {
            continue;
        };
        let mut fields = Vec::new();
        let mut key_fields = Vec::new();
        for item in &source.items {
            match &item.value {
                EntityItem::Key(key) => {
                    for field in &key.fields {
                        let field_id = symbols
                            .entity_fields
                            .get(&(id, field.value.name.value.clone()))
                            .copied();
                        if let Some(lowered) = lower_field(
                            &field.value.name,
                            field.value.ty.span,
                            field_id,
                            field_id.and_then(|field_id| types.entity_fields.get(&(id, field_id))),
                        ) {
                            key_fields.push(lowered.id);
                            fields.push(lowered);
                        }
                    }
                }
                EntityItem::Field(field) => {
                    let field_id = symbols
                        .entity_fields
                        .get(&(id, field.name.value.clone()))
                        .copied();
                    if let Some(lowered) = lower_classified_field(
                        &field.name,
                        field.ty.span,
                        field_id,
                        field_id.and_then(|field_id| types.entity_fields.get(&(id, field_id))),
                        field.secret,
                    ) {
                        fields.push(lowered);
                    }
                }
                EntityItem::Invariant(_)
                | EntityItem::Index(_)
                | EntityItem::Unique(_)
                | EntityItem::Reference(_)
                | EntityItem::DeletePolicy(_) => {}
                EntityItem::VectorField(vector_field) => {
                    let field_id = symbols
                        .entity_fields
                        .get(&(id, vector_field.name.value.clone()))
                        .copied();
                    if let Some(lowered) = lower_field(
                        &vector_field.name,
                        vector_field.dimension.span,
                        field_id,
                        field_id.and_then(|field_id| types.entity_fields.get(&(id, field_id))),
                    ) {
                        fields.push(lowered);
                    }
                }
            }
        }
        let field_scope = fields_by_name(&fields);
        let mut invariants = Vec::new();
        let mut indexes = Vec::new();
        let mut relationships = Vec::new();
        let mut delete_policy: Option<HirDeletePolicy> = None;
        for item in &source.items {
            match &item.value {
                EntityItem::Invariant(invariant) => {
                    let Some(invariant_id) = symbols
                        .entity_invariants
                        .get(&(id, invariant.name.value.clone()))
                        .copied()
                    else {
                        continue;
                    };
                    if let Some(lowered) = lower_invariant(
                        symbols,
                        id,
                        invariant_id,
                        &invariant.name,
                        &invariant.expression,
                        field_scope.clone(),
                        diagnostics,
                    ) {
                        invariants.push(lowered);
                    }
                }
                EntityItem::Index(index) => {
                    let Some(index_id) = symbols
                        .indexes
                        .get(&(id, index.name.value.clone()))
                        .copied()
                    else {
                        continue;
                    };
                    let mut index_fields = Vec::new();
                    for field in &index.fields {
                        match field_scope.get(&field.value) {
                            Some((field_id, _)) => index_fields.push((*field_id, field.span)),
                            None => diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                field.span,
                            )),
                        }
                    }
                    let mut encodings = vec![
                        riffdb_contract_ir::IndexFieldEncodingV1::Canonical;
                        index_fields.len()
                    ];
                    for option in &index.options {
                        let (field, encoding) = match &option.value {
                            riffdb_contract_syntax::ast::IndexOption::Presence { field } => (
                                field,
                                riffdb_contract_ir::IndexFieldEncodingV1::Presence,
                            ),
                            riffdb_contract_syntax::ast::IndexOption::TextKey {
                                field,
                                profile,
                            } => (
                                field,
                                riffdb_contract_ir::IndexFieldEncodingV1::TextKey(
                                    match profile.value {
                                        riffdb_contract_syntax::ast::TextKeyProfile::BinaryUtf8V1 => {
                                            riffdb_contract_ir::TextKeyProfileV1::BinaryUtf8
                                        }
                                        riffdb_contract_syntax::ast::TextKeyProfile::UnicodeFoldV1 => {
                                            riffdb_contract_ir::TextKeyProfileV1::UnicodeFold
                                        }
                                    },
                                ),
                            ),
                        };
                        let Some(position) = index
                            .fields
                            .iter()
                            .position(|candidate| candidate.value == field.value)
                        else {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                field.span,
                            ));
                            continue;
                        };
                        if encodings[position]
                            != riffdb_contract_ir::IndexFieldEncodingV1::Canonical
                        {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::DuplicateName,
                                field.span,
                            ));
                            continue;
                        }
                        encodings[position] = encoding;
                    }
                    indexes.push(HirIndex {
                        id: index_id,
                        name: index.name.value.clone(),
                        span: index.name.span,
                        fields: index_fields,
                        encodings,
                        unique: false,
                    });
                }
                EntityItem::Unique(unique) => {
                    let Some(index_id) = symbols
                        .indexes
                        .get(&(id, unique.name.value.clone()))
                        .copied()
                    else {
                        continue;
                    };
                    let mut index_fields = Vec::new();
                    for field in &unique.fields {
                        match field_scope.get(&field.value) {
                            Some((field_id, _)) => index_fields.push((*field_id, field.span)),
                            None => diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                field.span,
                            )),
                        }
                    }
                    indexes.push(HirIndex {
                        id: index_id,
                        name: unique.name.value.clone(),
                        span: unique.name.span,
                        encodings: vec![
                            riffdb_contract_ir::IndexFieldEncodingV1::Canonical;
                            index_fields.len()
                        ],
                        fields: index_fields,
                        unique: true,
                    });
                }
                EntityItem::Reference(reference) => {
                    let mut source_fields = Vec::new();
                    for field in &reference.source_fields {
                        match field_scope.get(&field.value) {
                            Some((field_id, _)) => source_fields.push((*field_id, field.span)),
                            None => diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                field.span,
                            )),
                        }
                    }
                    let Some(target_entity) = symbols
                        .entities
                        .get(&reference.target_entity.value)
                        .copied()
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            reference.target_entity.span,
                        ));
                        continue;
                    };
                    let mut target_fields = Vec::new();
                    for field in &reference.target_fields {
                        match symbols
                            .entity_fields
                            .get(&(target_entity, field.value.clone()))
                            .copied()
                        {
                            Some(field_id) => target_fields.push((field_id, field.span)),
                            None => diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                field.span,
                            )),
                        }
                    }
                    relationships.push(HirRelationship {
                        name: reference.name.value.clone(),
                        name_span: reference.name.span,
                        source_fields,
                        target_entity,
                        target_entity_span: reference.target_entity.span,
                        target_fields,
                    });
                }
                EntityItem::DeletePolicy(policy) => {
                    if let Some(ref previous) = delete_policy {
                        diagnostics.push(
                            CompilerDiagnostic::new(
                                CompilerDiagnosticCode::DuplicateName,
                                item.span,
                            )
                            .with_related_span(previous.span()),
                        );
                        continue;
                    }
                    delete_policy = match policy {
                        DeletePolicyDeclaration::NoInbound => {
                            Some(HirDeletePolicy::NoInbound { span: item.span })
                        }
                        DeletePolicyDeclaration::Restrict {
                            source_entity,
                            index,
                        } => {
                            let Some(source_entity_id) =
                                symbols.entities.get(&source_entity.value).copied()
                            else {
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::UnknownName,
                                    source_entity.span,
                                ));
                                continue;
                            };
                            let Some(index_id) = symbols
                                .indexes
                                .get(&(source_entity_id, index.value.clone()))
                                .copied()
                            else {
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::UnknownName,
                                    index.span,
                                ));
                                continue;
                            };
                            Some(HirDeletePolicy::Restrict {
                                span: item.span,
                                source_entity: source_entity_id,
                                index_id,
                            })
                        }
                        DeletePolicyDeclaration::Cascade { relationships } => {
                            if relationships.is_empty() || relationships.len() > 32 {
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::BoundExceeded,
                                    item.span,
                                ));
                                continue;
                            }
                            let mut lowered = Vec::with_capacity(relationships.len());
                            for relationship in relationships {
                                let source = &relationship.value.source_entity;
                                let index_entity = &relationship.value.index_entity;
                                if source.value != index_entity.value {
                                    diagnostics.push(CompilerDiagnostic::new(
                                        CompilerDiagnosticCode::InvalidDeletePolicy,
                                        index_entity.span,
                                    ));
                                    continue;
                                }
                                let Some(source_entity) =
                                    symbols.entities.get(&source.value).copied()
                                else {
                                    diagnostics.push(CompilerDiagnostic::new(
                                        CompilerDiagnosticCode::UnknownName,
                                        source.span,
                                    ));
                                    continue;
                                };
                                let Some(index_id) = symbols
                                    .indexes
                                    .get(&(source_entity, relationship.value.index.value.clone()))
                                    .copied()
                                else {
                                    diagnostics.push(CompilerDiagnostic::new(
                                        CompilerDiagnosticCode::UnknownName,
                                        relationship.value.index.span,
                                    ));
                                    continue;
                                };
                                let Some(maximum) = relationship
                                    .value
                                    .maximum
                                    .value
                                    .parse::<usize>()
                                    .ok()
                                    .filter(|maximum| *maximum > 0 && *maximum <= 255)
                                else {
                                    diagnostics.push(CompilerDiagnostic::new(
                                        CompilerDiagnosticCode::BoundExceeded,
                                        relationship.value.maximum.span,
                                    ));
                                    continue;
                                };
                                lowered.push(HirCascadeRelationship {
                                    source_entity,
                                    relationship_name: relationship
                                        .value
                                        .relationship
                                        .value
                                        .clone(),
                                    index_id,
                                    maximum,
                                });
                            }
                            Some(HirDeletePolicy::Cascade {
                                span: item.span,
                                relationships: lowered,
                            })
                        }
                    };
                }
                EntityItem::Key(_) | EntityItem::Field(_) | EntityItem::VectorField(_) => {}
            }
        }
        // Validate vector field declarations and retain the resolved search
        // configuration (metric, source fields, staleness SLO) — previously
        // validated here and then discarded, so only the dimension survived
        // into the IR.
        let mut vector_fields = Vec::new();
        for item in &source.items {
            if let EntityItem::VectorField(vector_field) = &item.value {
                let mut valid = true;
                // Dimension must be a parseable positive integer within bound.
                match vector_field.dimension.value.parse::<u32>() {
                    Ok(dim) if riffdb_types::VectorDimension::new(dim).is_some() => {}
                    _ => {
                        valid = false;
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::BoundExceeded,
                            vector_field.dimension.span,
                        ));
                    }
                }
                // The v1 staleness SLO is a positive stale-entity count
                // threshold. Duration semantics are reserved for a future
                // amendment and require no clock machinery here.
                let stale_entity_count_threshold =
                    match vector_field.staleness_slo.value.parse::<u32>() {
                        Ok(count)
                            if riffdb_types::StaleEntityCountThreshold::new(count).is_some() =>
                        {
                            u64::from(count)
                        }
                        _ => {
                            valid = false;
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::BoundExceeded,
                                vector_field.staleness_slo.span,
                            ));
                            0
                        }
                    };
                let (ann_row_threshold, recall_target_bps) = match &vector_field.ann {
                    None => (None, None),
                    Some(ann) => {
                        let threshold = match ann.row_threshold.value.parse::<u32>() {
                            Ok(value)
                                if (1
                                    ..=riffdb_contract_ir::MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG)
                                    .contains(&value) =>
                            {
                                Some(value)
                            }
                            _ => {
                                valid = false;
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::BoundExceeded,
                                    ann.row_threshold.span,
                                ));
                                None
                            }
                        };
                        let recall = match ann.recall_target_bps.value.parse::<u32>() {
                            Ok(value)
                                if (1..=riffdb_contract_ir::VECTOR_RECALL_BASIS_POINTS)
                                    .contains(&value) =>
                            {
                                Some(value)
                            }
                            _ => {
                                valid = false;
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::BoundExceeded,
                                    ann.recall_target_bps.span,
                                ));
                                None
                            }
                        };
                        (threshold, recall)
                    }
                };
                // Source fields must be non-empty and each must resolve to an entity field.
                if vector_field.source_fields.is_empty() {
                    valid = false;
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::MissingDeclaration,
                        item.span,
                    ));
                }
                let mut source_fields = Vec::new();
                for source_field in &vector_field.source_fields {
                    match field_scope.get(&source_field.value) {
                        Some((field_id, _)) => {
                            // A repeated source field is a diagnostic, never a
                            // silent dedup: `(title, title)` previously became
                            // `(title)` with no report, and the dedup made
                            // `VectorFieldSpecV1::new`'s sorted-unique check
                            // unreachable from the compiler.
                            if source_fields.contains(field_id) {
                                valid = false;
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::DuplicateName,
                                    source_field.span,
                                ));
                            } else {
                                source_fields.push(*field_id);
                            }
                        }
                        None => {
                            valid = false;
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::UnknownName,
                                source_field.span,
                            ));
                        }
                    }
                }
                source_fields.sort_unstable();
                let field_id = symbols
                    .entity_fields
                    .get(&(id, vector_field.name.value.clone()))
                    .copied();
                // An unresolvable vector-field name with no other diagnostic
                // previously DROPPED the whole spec silently: the contract
                // compiled and its metric, source fields, and SLO vanished
                // from the bundle (the S8 defect's failure mode re-entering
                // through a different door). Report it instead.
                if valid && field_id.is_none() {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        vector_field.name.span,
                    ));
                }
                if let (true, Some(field_id)) = (valid, field_id) {
                    vector_fields.push(HirVectorField {
                        field_id,
                        metric: match vector_field.metric.value {
                            riffdb_contract_syntax::ast::VectorMetricKeyword::Cosine => {
                                riffdb_types::DistanceMetric::Cosine
                            }
                            riffdb_contract_syntax::ast::VectorMetricKeyword::Euclidean => {
                                riffdb_types::DistanceMetric::Euclidean
                            }
                            riffdb_contract_syntax::ast::VectorMetricKeyword::DotProduct => {
                                riffdb_types::DistanceMetric::DotProduct
                            }
                        },
                        source_fields,
                        stale_entity_count_threshold,
                        ann_row_threshold,
                        recall_target_bps,
                        span: item.span,
                    });
                }
            }
        }
        result.push(HirEntity {
            id,
            name: source.name.value.clone(),
            span: source.name.span,
            fields,
            key_fields,
            invariants,
            indexes,
            relationships,
            delete_policy,
            vector_fields,
        });
    }
    result
}

fn lower_events(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
    entities: &[HirEntity],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirEvent> {
    document
        .contract
        .value
        .declarations
        .iter()
        .filter_map(|declaration| {
            let Declaration::Event(source) = &declaration.value else {
                return None;
            };
            let id = symbols.events.get(&source.name.value).copied()?;
            let fields = source
                .fields
                .iter()
                .filter_map(|field| {
                    let field_id = symbols
                        .event_fields
                        .get(&(id, field.value.name.value.clone()))
                        .copied();
                    lower_field(
                        &field.value.name,
                        field.value.ty.span,
                        field_id,
                        field_id.and_then(|field_id| types.event_fields.get(&(id, field_id))),
                    )
                })
                .collect::<Vec<_>>();
            let partition_fields = source
                .partition_by
                .as_ref()
                .map(|partition| {
                    partition
                        .value
                        .iter()
                        .filter_map(|name| {
                            let Some(field_id) =
                                symbols.event_fields.get(&(id, name.value.clone())).copied()
                            else {
                                diagnostics.push(CompilerDiagnostic::new(
                                    CompilerDiagnosticCode::UnknownName,
                                    name.span,
                                ));
                                return None;
                            };
                            Some((field_id, name.span))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let policy_anchor = source.policy_anchor.as_ref().and_then(|anchor| {
                lower_event_policy_anchor(
                    document,
                    symbols,
                    entities,
                    id,
                    &fields,
                    &partition_fields,
                    anchor,
                    diagnostics,
                )
            });
            Some(HirEvent {
                id,
                name: source.name.value.clone(),
                span: source.name.span,
                partition_fields,
                partition_span: source.partition_by.as_ref().map(|partition| partition.span),
                policy_anchor,
                fields,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn lower_event_policy_anchor(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    entities: &[HirEntity],
    event_id: EventTypeId,
    event_fields: &[HirField],
    partition_fields: &[(FieldId, Span)],
    anchor: &Spanned<EventPolicyAnchorDeclaration>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<HirEventPolicyAnchor> {
    let diagnostic_start = diagnostics.len();
    let Some(entity_id) = symbols.entities.get(&anchor.value.entity.value).copied() else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            anchor.value.entity.span,
        ));
        return None;
    };
    let Some(entity) = entities.iter().find(|candidate| candidate.id == entity_id) else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidEvent,
            anchor.value.entity.span,
        ));
        return None;
    };

    let mut mapped_entity_fields = Vec::with_capacity(anchor.value.fields.len());
    let mut mapped_payload_fields = Vec::with_capacity(anchor.value.fields.len());
    for mapping in &anchor.value.fields {
        let entity_field_id = symbols
            .entity_fields
            .get(&(entity_id, mapping.value.entity_field.value.clone()))
            .copied();
        let payload_field_id = symbols
            .event_fields
            .get(&(event_id, mapping.value.payload_field.value.clone()))
            .copied();
        let Some(entity_field_id) = entity_field_id else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                mapping.value.entity_field.span,
            ));
            continue;
        };
        let Some(payload_field_id) = payload_field_id else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                mapping.value.payload_field.span,
            ));
            continue;
        };
        let entity_field = entity
            .fields
            .iter()
            .find(|field| field.id == entity_field_id);
        let payload_field = event_fields
            .iter()
            .find(|field| field.id == payload_field_id);
        if entity_field
            .zip(payload_field)
            .is_none_or(|(entity_field, payload_field)| {
                payload_field.value_type.is_optional()
                    || entity_field.value_type != payload_field.value_type
            })
        {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidEvent,
                mapping.value.payload_field.span,
            ));
        }
        mapped_entity_fields.push(entity_field_id);
        mapped_payload_fields.push(payload_field_id);
    }

    if mapped_entity_fields != entity.key_fields
        || mapped_payload_fields
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != mapped_payload_fields.len()
    {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidEvent,
            anchor.span,
        ));
    }
    let partition_ids = partition_fields
        .iter()
        .map(|(field, _)| *field)
        .collect::<Vec<_>>();
    if partition_ids.is_empty()
        || mapped_payload_fields.get(..partition_ids.len()) != Some(partition_ids.as_slice())
    {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidEvent,
            anchor.span,
        ));
    }

    let policies = document
        .contract
        .value
        .declarations
        .iter()
        .filter_map(|declaration| {
            let Declaration::RowPolicy(policy) = &declaration.value else {
                return None;
            };
            (symbols.entities.get(&policy.entity.value).copied() == Some(entity_id))
                .then_some(policy)
        })
        .collect::<Vec<_>>();
    if policies.len() != 1
        || !policies[0]
            .rules
            .iter()
            .any(|rule| rule.value.operation.value == RowPolicyOperation::Read)
    {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidEvent,
            anchor.value.entity.span,
        ));
    }
    (diagnostics.len() == diagnostic_start).then(|| HirEventPolicyAnchor {
        source_entity: entity_id,
        key_fields: mapped_entity_fields
            .into_iter()
            .zip(mapped_payload_fields)
            .collect(),
        read_policy: policies[0].name.value.clone(),
        span: anchor.span,
    })
}

fn lower_aggregates(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirAggregate> {
    let mut result = Vec::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::Aggregate(source) = &declaration.value else {
            continue;
        };
        let Some(id) = symbols.aggregates.get(&source.name.value).copied() else {
            continue;
        };
        let root = source.items.iter().find_map(|item| match &item.value {
            AggregateItem::Root(root) => Some(root),
            _ => None,
        });
        let partition = source.items.iter().find_map(|item| match &item.value {
            AggregateItem::PartitionBy(expression) => Some(expression),
            _ => None,
        });
        let conflict = source.items.iter().find_map(|item| match &item.value {
            AggregateItem::ConflictKey(expressions) => Some(expressions),
            _ => None,
        });
        let (Some(root), Some(partition), Some(conflict)) = (root, partition, conflict) else {
            continue;
        };
        let Some(root_id) = symbols.entities.get(&root.value).copied() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                root.span,
            ));
            continue;
        };
        let fields = entity_field_map(root_id, symbols, types);
        let mut resolver = ExpressionLowerer::new(
            symbols,
            ExpressionScope::Schema {
                entity_id: root_id,
                fields: fields.clone(),
            },
        );
        let Some(partition_root) = lower_root(&mut resolver, partition, None, false, diagnostics)
        else {
            continue;
        };
        let conflicts = conflict
            .iter()
            .filter_map(|expression| {
                lower_root(&mut resolver, expression, None, false, diagnostics)
            })
            .collect();
        let expressions = match resolver.finish_hir(source.name.span) {
            Ok(expressions) => expressions,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        let mut children = Vec::new();
        for item in &source.items {
            let AggregateItem::Child(child) = &item.value else {
                continue;
            };
            match symbols.entities.get(&child.value).copied() {
                Some(id) => children.push((id, child.span)),
                None => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    child.span,
                )),
            }
        }
        let mut invariants = Vec::new();
        for item in &source.items {
            let AggregateItem::Invariant(invariant) = &item.value else {
                continue;
            };
            let Some(invariant_id) = symbols
                .aggregate_invariants
                .get(&(id, invariant.name.value.clone()))
                .copied()
            else {
                continue;
            };
            if let Some(lowered) = lower_invariant(
                symbols,
                root_id,
                invariant_id,
                &invariant.name,
                &invariant.expression,
                fields.clone(),
                diagnostics,
            ) {
                invariants.push(lowered);
            }
        }
        result.push(HirAggregate {
            id,
            name: source.name.value.clone(),
            span: source.name.span,
            root: root_id,
            root_span: root.span,
            children,
            keys: HirAggregateKeys {
                expressions,
                partition: partition_root,
                conflicts,
            },
            invariants,
        });
    }
    result
}

const MAX_WORKFLOW_LEASE_DURATION_SECONDS: u64 = 86_400;

fn lower_workflows(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    entities: &[HirEntity],
    aggregates: &[HirAggregate],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirWorkflow> {
    let mut result = Vec::new();
    let mut entities_with_workflows = BTreeMap::<EntityTypeId, Span>::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::Workflow(source) = &declaration.value else {
            continue;
        };
        let Some(entity_id) = symbols.entities.get(&source.entity.value).copied() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                source.entity.span,
            ));
            continue;
        };
        let Some(entity) = entities.iter().find(|entity| entity.id == entity_id) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflow,
                source.entity.span,
            ));
            continue;
        };
        if !aggregates.iter().any(|aggregate| {
            aggregate.root == entity_id
                || aggregate.children.iter().any(|child| child.0 == entity_id)
        }) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflow,
                source.entity.span,
            ));
        }
        if let Some(first_span) = entities_with_workflows.insert(entity_id, source.entity.span) {
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidWorkflow,
                    source.entity.span,
                )
                .with_related_span(first_span),
            );
        }
        let Some(state_field) = entity
            .fields
            .iter()
            .find(|field| field.name == source.state_field.value)
        else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                source.state_field.span,
            ));
            continue;
        };
        let Some(state_enum) = state_field.value_type.enum_type_id() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflow,
                source.state_field.span,
            ));
            continue;
        };
        if entity.key_field_set().contains(&state_field.id) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflow,
                source.state_field.span,
            ));
        }

        let initial_state = source.initial_state.as_ref().and_then(|initial| {
            symbols
                .enum_variants
                .get(&(state_enum, initial.value.clone()))
                .copied()
                .or_else(|| {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflowTransition,
                        initial.span,
                    ));
                    None
                })
        });
        let mut transition_names = BTreeMap::<String, Span>::new();
        let mut transition_edges = BTreeMap::<(EnumVariantId, EnumVariantId), Span>::new();
        let mut transitions = Vec::new();
        for transition in &source.transitions {
            if let Some(first_span) = transition_names.insert(
                transition.value.name.value.clone(),
                transition.value.name.span,
            ) {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflowTransition,
                        transition.value.name.span,
                    )
                    .with_related_span(first_span),
                );
            }
            let Some(destination) = symbols
                .enum_variants
                .get(&(state_enum, transition.value.destination.value.clone()))
                .copied()
            else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidWorkflowTransition,
                    transition.value.destination.span,
                ));
                continue;
            };
            let mut seen_sources = BTreeMap::<EnumVariantId, Span>::new();
            let mut source_states = Vec::new();
            for source_state in &transition.value.source_states {
                let Some(source_id) = symbols
                    .enum_variants
                    .get(&(state_enum, source_state.value.clone()))
                    .copied()
                else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflowTransition,
                        source_state.span,
                    ));
                    continue;
                };
                if let Some(first_span) = seen_sources.insert(source_id, source_state.span) {
                    diagnostics.push(
                        CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            source_state.span,
                        )
                        .with_related_span(first_span),
                    );
                    continue;
                }
                if let Some(first_span) =
                    transition_edges.insert((source_id, destination), transition.span)
                {
                    diagnostics.push(
                        CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.span,
                        )
                        .with_related_span(first_span),
                    );
                }
                source_states.push(source_id);
            }
            source_states.sort_unstable();
            transitions.push(HirWorkflowTransition {
                name: transition.value.name.value.clone(),
                span: transition.span,
                source_states,
                destination,
            });
        }

        let lease = source
            .lease
            .as_ref()
            .and_then(|lease| lower_workflow_lease(entity, &lease.value, lease.span, diagnostics));
        result.push(HirWorkflow {
            name: source.name.value.clone(),
            span: source.name.span,
            entity_id,
            state_field: state_field.id,
            state_enum,
            initial_state,
            transitions,
            lease,
        });
    }
    result
}

fn lower_workflow_lease(
    entity: &HirEntity,
    source: &riffdb_contract_syntax::ast::WorkflowLeaseDeclaration,
    span: Span,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<HirWorkflowLease> {
    let field = |name: &Spanned<String>| {
        entity
            .fields
            .iter()
            .find(|field| field.name == name.value)
            .map(|field| (field.id, &field.value_type))
    };
    let owner = field(&source.owner_field);
    let expiry = field(&source.expiry_field);
    let fence = field(&source.fencing_token_field);
    let attempts = source
        .attempt_field
        .as_ref()
        .and_then(|name| field(name).map(|field| (name, field)));
    for (name, value) in [
        (&source.owner_field, owner),
        (&source.expiry_field, expiry),
        (&source.fencing_token_field, fence),
    ] {
        if value.is_none() {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                name.span,
            ));
        }
    }
    if let Some(attempt_field) = &source.attempt_field
        && attempts.is_none()
    {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            attempt_field.span,
        ));
    }
    let (
        Some((owner_id, owner_type)),
        Some((expiry_id, expiry_type)),
        Some((fence_id, fence_type)),
    ) = (owner, expiry, fence)
    else {
        return None;
    };
    let owner_valid = owner_type
        .optional_inner()
        .is_some_and(|inner| inner.tag() == ValueTypeTag::Uuid);
    let expiry_valid = expiry_type
        .optional_inner()
        .is_some_and(|inner| inner.tag() == ValueTypeTag::Timestamp);
    let fence_valid = fence_type.tag() == ValueTypeTag::U64;
    let attempts_valid = attempts
        .as_ref()
        .is_none_or(|(_, (_, value_type))| value_type.tag() == ValueTypeTag::U64);
    let mut fields = vec![owner_id, expiry_id, fence_id];
    if let Some((_, (attempt_id, _))) = attempts {
        fields.push(attempt_id);
    }
    fields.sort_unstable();
    let distinct = fields.windows(2).all(|pair| pair[0] != pair[1]);
    let key_fields = entity.key_field_set();
    let non_key = fields.iter().all(|field| !key_fields.contains(field));
    let minimum = source.minimum_duration_seconds.value.parse::<u64>().ok();
    let maximum = source.maximum_duration_seconds.value.parse::<u64>().ok();
    let duration_valid = minimum.zip(maximum).is_some_and(|(minimum, maximum)| {
        minimum > 0 && minimum <= maximum && maximum <= MAX_WORKFLOW_LEASE_DURATION_SECONDS
    });
    if !owner_valid
        || !expiry_valid
        || !fence_valid
        || !attempts_valid
        || !distinct
        || !non_key
        || !duration_valid
    {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidWorkflowLease,
            span,
        ));
        return None;
    }
    Some(HirWorkflowLease {
        name: source.name.value.clone(),
        span,
        owner_field: owner_id,
        expiry_field: expiry_id,
        fencing_token_field: fence_id,
        attempt_field: attempts.map(|(_, (field, _))| field),
        minimum_duration_seconds: minimum.expect("validated duration"),
        maximum_duration_seconds: maximum.expect("validated duration"),
    })
}

fn lower_commands(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
    entities: &[HirEntity],
    events: &[HirEvent],
    workflows: &[HirWorkflow],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirCommand> {
    let mut result = Vec::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::Command(source_command) = &declaration.value else {
            continue;
        };
        let invocation_class = if source_command.kind == CommandKind::Reimport {
            CommandInvocationClass::Reimport
        } else {
            CommandInvocationClass::Application
        };
        let normalized_source = if invocation_class == CommandInvocationClass::Reimport {
            let Some(normalized) =
                normalize_reimport_command(source_command, symbols, entities, diagnostics)
            else {
                continue;
            };
            Some(normalized)
        } else {
            None
        };
        let source = normalized_source.as_ref().unwrap_or(source_command);
        let Some(command_id) = symbols.commands.get(&source.name.value).copied() else {
            continue;
        };
        let inputs = source
            .inputs
            .iter()
            .filter_map(|input| {
                let field_id = symbols
                    .command_inputs
                    .get(&(command_id, input.value.field.name.value.clone()))
                    .copied();
                lower_field(
                    &input.value.field.name,
                    input.value.field.ty.span,
                    field_id,
                    field_id.and_then(|field_id| types.command_inputs.get(&(command_id, field_id))),
                )
                .map(|field| HirInput { field })
            })
            .collect::<Vec<_>>();
        let service_values = source
            .service_values
            .iter()
            .filter_map(|value| {
                let field_id = symbols
                    .command_service_values
                    .get(&(command_id, value.value.name.value.clone()))
                    .copied();
                let value_type = field_id
                    .and_then(|field_id| types.command_service_values.get(&(command_id, field_id)));
                lower_field(
                    &value.value.name,
                    value.value.kind.span,
                    field_id,
                    value_type,
                )
                .map(|field| HirServiceValue {
                    field,
                    kind: match value.value.kind.value {
                        ServiceValueKind::UuidV7 => HirServiceValueKind::UuidV7,
                        ServiceValueKind::TransactionTime => HirServiceValueKind::TransactionTime,
                    },
                })
            })
            .collect::<Vec<_>>();
        let input_scope = inputs
            .iter()
            .map(|input| {
                (
                    input.field.name.clone(),
                    (input.field.id, input.field.value_type.clone()),
                )
            })
            .collect();
        let service_value_scope = service_values
            .iter()
            .map(|value| {
                (
                    value.field.name.clone(),
                    (value.field.id, value.field.value_type.clone()),
                )
            })
            .collect();
        let mut collection_element_scope = None;
        let mut collection_bounds = None;
        match (source.kind, source.bulk_iteration.as_ref()) {
            (CommandKind::Ordinary, None) => {}
            (CommandKind::Ordinary, Some(iteration)) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidExpression,
                iteration.span,
            )),
            (CommandKind::Bulk, None) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::MissingDeclaration,
                source.name.span,
            )),
            (CommandKind::Bulk, Some(iteration)) => {
                let matching_source = source
                    .inputs
                    .iter()
                    .find(|input| input.value.field.name.value == iteration.value.collection.value);
                let matching_hir = inputs
                    .iter()
                    .find(|input| input.field.name == iteration.value.collection.value);
                match (matching_source, matching_hir) {
                    (Some(source_input), Some(input)) => {
                        let TypeExpression::List {
                            element: _,
                            minimum,
                            maximum: source_maximum,
                        } = &source_input.value.field.ty.value
                        else {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidType,
                                source_input.value.field.ty.span,
                            ));
                            continue;
                        };
                        let Some((element_type, maximum)) = input.field.value_type.list_parts()
                        else {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidType,
                                source_input.value.field.ty.span,
                            ));
                            continue;
                        };
                        let parsed_minimum = minimum
                            .as_ref()
                            .and_then(|minimum| minimum.value.parse::<usize>().ok());
                        let source_maximum = source_maximum.value.parse::<usize>().ok();
                        let Some(minimum) = parsed_minimum else {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidType,
                                source_input.value.field.ty.span,
                            ));
                            continue;
                        };
                        if source_maximum != Some(maximum)
                            || minimum == 0
                            || minimum > maximum
                            || maximum > riffdb_contract_ir::MAX_COLLECTION_COMMAND_ELEMENTS_V1
                        {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::BoundExceeded,
                                source_input.value.field.ty.span,
                            ));
                            continue;
                        }
                        if inputs
                            .iter()
                            .filter(|candidate| candidate.field.value_type.list_parts().is_some())
                            .count()
                            != 1
                        {
                            diagnostics.push(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidType,
                                source.name.span,
                            ));
                            continue;
                        }
                        let fields = element_type
                            .record_ref()
                            .and_then(|record| match record {
                                riffdb_contract_ir::RecordTypeRef::Entity(entity_id) => entities
                                    .iter()
                                    .find(|entity| entity.id == *entity_id)
                                    .map(HirEntity::fields_by_name),
                                _ => None,
                            })
                            .unwrap_or_default();
                        collection_element_scope = Some(CollectionElementExpressionScope {
                            name: iteration.value.element.value.clone(),
                            value_type: element_type.clone(),
                            fields,
                        });
                        collection_bounds = Some((
                            input.field.id,
                            minimum,
                            maximum,
                            element_type.clone(),
                            iteration.span,
                        ));
                    }
                    _ => diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        iteration.value.collection.span,
                    )),
                }
            }
            (CommandKind::Reimport, _) => unreachable!("reimport syntax is normalized above"),
        }
        let mut binding_descriptors = Vec::new();
        let top_level_binding_count = source.bindings.len();
        let collection_bindings = source
            .bulk_iteration
            .as_ref()
            .map_or(&[][..], |iteration| iteration.value.bindings.as_slice());
        for (index, (binding, collection_local)) in source
            .bindings
            .iter()
            .map(|binding| (binding, false))
            .chain(collection_bindings.iter().map(|binding| (binding, true)))
            .enumerate()
        {
            let (binding, mode) = match &binding.value {
                Binding::Read(binding) => (binding, BindingMode::Read),
                Binding::Mutate(binding) => (binding, BindingMode::Mutate),
                Binding::Create(binding) => (binding, BindingMode::Create),
                Binding::Delete(binding) => (binding, BindingMode::Delete),
            };
            let Some(entity_id) = symbols.entities.get(&binding.entity.value).copied() else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    binding.entity.span,
                ));
                continue;
            };
            let Some(entity) = entities.iter().find(|entity| entity.id == entity_id) else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidIr,
                    binding.entity.span,
                ));
                continue;
            };
            let Ok(index) = u32::try_from(index) else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::BoundExceeded,
                    binding.binding.span,
                ));
                continue;
            };
            binding_descriptors.push((
                binding,
                BindingId::new(index),
                mode,
                entity,
                collection_local,
            ));
        }
        let binding_scope = binding_descriptors
            .iter()
            .map(|(source, id, _, entity, collection_local)| {
                (
                    source.binding.value.clone(),
                    BindingExpressionScope {
                        id: *id,
                        entity_id: entity.id,
                        fields: entity.fields_by_name(),
                        collection_local: *collection_local,
                    },
                )
            })
            .collect();
        let reveal_scope = binding_descriptors
            .iter()
            .map(|(source, id, _, entity, _)| {
                (
                    source.binding.value.clone(),
                    SecretRevealScopeEntry {
                        binding: *id,
                        entity: entity.id,
                        fields: entity
                            .fields
                            .iter()
                            .map(|field| {
                                (
                                    field.name.clone(),
                                    (field.id, field.name_span, field.secret_span.is_some()),
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect::<SecretRevealScope>();
        let scope = ExpressionScope::Command {
            command_id,
            inputs: input_scope,
            service_values: service_value_scope,
            bindings: binding_scope,
            collection_element: collection_element_scope,
        };
        let idempotency = source.idempotency.as_ref().and_then(|idempotency| {
            let mut resolver = ExpressionLowerer::new(symbols, scope.clone());
            let root = lower_root(
                &mut resolver,
                &idempotency.value.expression,
                None,
                false,
                diagnostics,
            )?;
            let expressions = match resolver.finish_hir(idempotency.value.expression.span) {
                Ok(expressions) => expressions,
                Err(diagnostic) => {
                    diagnostics.push(diagnostic);
                    return None;
                }
            };
            Some(HirTypedExpression { root, expressions })
        });
        let mut resolver = ExpressionLowerer::new(symbols, scope);
        let mut bindings = Vec::new();
        for (binding, binding_id, mode, entity, collection_local) in binding_descriptors {
            resolver.set_collection_context(collection_local);
            if binding.arguments.len() != entity.key_fields.len() {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidBinding,
                    binding.entity.span,
                ));
                continue;
            }
            let arguments = binding
                .arguments
                .iter()
                .zip(&entity.key_fields)
                .filter_map(|(argument, field_id)| {
                    let expected = entity
                        .fields
                        .iter()
                        .find(|field| field.id == *field_id)
                        .map(|field| &field.value_type);
                    lower_root(&mut resolver, argument, expected, false, diagnostics)
                })
                .collect();
            let Some(failure) = lower_outcome(
                command_id,
                &binding.failure,
                symbols,
                &mut resolver,
                &reveal_scope,
                true,
                diagnostics,
            ) else {
                continue;
            };
            let requires_restriction_failure = mode == BindingMode::Delete
                && matches!(
                    entity.delete_policy.as_ref(),
                    Some(HirDeletePolicy::Restrict { .. })
                );
            if requires_restriction_failure != binding.restriction_failure.is_some() {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidDeletePolicy,
                    binding
                        .restriction_failure
                        .as_ref()
                        .map_or(binding.entity.span, |outcome| outcome.span),
                ));
                continue;
            }
            let restriction_failure = match &binding.restriction_failure {
                Some(source) => {
                    let Some(outcome) = lower_outcome(
                        command_id,
                        source,
                        symbols,
                        &mut resolver,
                        &reveal_scope,
                        true,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    Some(outcome)
                }
                None => None,
            };
            let requires_cascade_failure = mode == BindingMode::Delete
                && matches!(
                    entity.delete_policy.as_ref(),
                    Some(HirDeletePolicy::Cascade { .. })
                );
            if requires_cascade_failure != binding.cascade_failure.is_some() {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidDeletePolicy,
                    binding
                        .cascade_failure
                        .as_ref()
                        .map_or(binding.entity.span, |outcome| outcome.span),
                ));
                continue;
            }
            let cascade_failure = match &binding.cascade_failure {
                Some(source) => {
                    let Some(outcome) = lower_outcome(
                        command_id,
                        source,
                        symbols,
                        &mut resolver,
                        &reveal_scope,
                        true,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    Some(outcome)
                }
                None => None,
            };
            bindings.push(HirBinding {
                id: binding_id,
                mode,
                entity_id: entity.id,
                entity_span: binding.entity.span,
                name: binding.binding.value.clone(),
                name_span: binding.binding.span,
                span: binding
                    .cascade_failure
                    .as_ref()
                    .or(binding.restriction_failure.as_ref())
                    .as_ref()
                    .map_or(binding.failure.span, |outcome| outcome.span)
                    .cover(binding.entity.span),
                arguments,
                failure,
                restriction_failure,
                cascade_failure,
                collection_local,
            });
        }
        let mut requirements = Vec::new();
        let collection_requirements = source
            .bulk_iteration
            .as_ref()
            .map_or(&[][..], |iteration| iteration.value.requirements.as_slice());
        let repeated_requirement_count = collection_requirements.len();
        for (requirement, collection_local) in collection_requirements
            .iter()
            .map(|requirement| (requirement, true))
            .chain(
                source
                    .requirements
                    .iter()
                    .map(|requirement| (requirement, false)),
            )
        {
            resolver.set_collection_context(collection_local);
            let Some(condition) = lower_root(
                &mut resolver,
                &requirement.value.condition,
                Some(&ValueType::bool()),
                false,
                diagnostics,
            ) else {
                continue;
            };
            let Some(rejection) = lower_outcome(
                command_id,
                &requirement.value.rejection,
                symbols,
                &mut resolver,
                &reveal_scope,
                false,
                diagnostics,
            ) else {
                continue;
            };
            requirements.push(HirRequirement {
                name_span: requirement.value.name.span,
                condition,
                rejection,
            });
        }
        let binding_by_name = bindings
            .iter()
            .map(|binding| (binding.name.as_str(), binding))
            .collect::<BTreeMap<_, _>>();
        let mut effects = Vec::new();
        let mut transitioned_bindings = BTreeSet::new();
        let mut leased_bindings = BTreeSet::new();
        let collection_effects = source
            .bulk_iteration
            .as_ref()
            .map_or(&[][..], |iteration| iteration.value.effects.as_slice());
        let mut lowered_collection_effect_count = 0usize;
        for (effect, collection_local) in collection_effects
            .iter()
            .map(|effect| (effect, true))
            .chain(source.effects.iter().map(|effect| (effect, false)))
        {
            let effects_before = effects.len();
            resolver.set_collection_context(collection_local);
            match &effect.value {
                Effect::Set(set) => {
                    let Some((binding_name, field_name)) = path_pair(&set.target.value) else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidMutation,
                            set.target.span,
                        ));
                        continue;
                    };
                    let Some(binding) = binding_by_name.get(binding_name).copied() else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            set.target.span,
                        ));
                        continue;
                    };
                    if binding.collection_local && !collection_local {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidMutation,
                            set.target.span,
                        ));
                        continue;
                    }
                    let Some(entity) = entities
                        .iter()
                        .find(|entity| entity.id == binding.entity_id)
                    else {
                        continue;
                    };
                    let Some(field) = entity.fields.iter().find(|field| field.name == field_name)
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            set.target.span,
                        ));
                        continue;
                    };
                    if invocation_class != CommandInvocationClass::Reimport
                        && workflows.iter().any(|workflow| {
                            workflow.entity_id == binding.entity_id
                                && workflow.state_field == field.id
                        })
                    {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            set.target.span,
                        ));
                        continue;
                    }
                    let Some(value) = lower_root(
                        &mut resolver,
                        &set.value,
                        Some(&field.value_type),
                        false,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    let reveals = lower_secret_reveals(&set.reveals, &reveal_scope, diagnostics);
                    effects.push(HirEffect::Set {
                        target_span: set.target.span,
                        binding: binding.id,
                        field: field.id,
                        value,
                        reveals,
                    });
                }
                Effect::Emit(emit) => {
                    let Some(event_id) = symbols.events.get(&emit.event.value).copied() else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            emit.event.span,
                        ));
                        continue;
                    };
                    let Some(event) = events.iter().find(|event| event.id == event_id) else {
                        continue;
                    };
                    let fields = lower_declared_object(
                        &emit.payload,
                        &event.fields,
                        &mut resolver,
                        &reveal_scope,
                        CompilerDiagnosticCode::InvalidEvent,
                        diagnostics,
                    );
                    effects.push(HirEffect::Emit {
                        event_id,
                        event_span: emit.event.span,
                        fields,
                    });
                }
                Effect::WorkflowTransition(transition) => {
                    let Some(binding) = binding_by_name
                        .get(transition.binding.value.as_str())
                        .copied()
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            transition.binding.span,
                        ));
                        continue;
                    };
                    if binding.collection_local && !collection_local {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.binding.span,
                        ));
                        continue;
                    }
                    if binding.mode != BindingMode::Mutate {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.binding.span,
                        ));
                        continue;
                    }
                    let Some(workflow) = workflows
                        .iter()
                        .find(|workflow| workflow.entity_id == binding.entity_id)
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.transition.span,
                        ));
                        continue;
                    };
                    let Some(declared) = workflow
                        .transitions
                        .iter()
                        .find(|declared| declared.name == transition.transition.value)
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.transition.span,
                        ));
                        continue;
                    };
                    if !transitioned_bindings.insert(binding.id) {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            effect.span,
                        ));
                        continue;
                    }
                    let Some(expected_revision) = lower_root(
                        &mut resolver,
                        &transition.expected_revision,
                        Some(&ValueType::u64()),
                        false,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    let Some(stale) = lower_outcome(
                        command_id,
                        &transition.stale,
                        symbols,
                        &mut resolver,
                        &reveal_scope,
                        false,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    let Some(illegal) = lower_outcome(
                        command_id,
                        &transition.illegal,
                        symbols,
                        &mut resolver,
                        &reveal_scope,
                        false,
                        diagnostics,
                    ) else {
                        continue;
                    };
                    effects.push(HirEffect::WorkflowTransition {
                        transition_span: transition.transition.span,
                        binding: binding.id,
                        state_field: workflow.state_field,
                        source_states: declared.source_states.clone(),
                        destination: declared.destination,
                        expected_revision,
                        stale,
                        illegal,
                    });
                }
                Effect::WorkflowLease(effect_source) => {
                    let Some(binding) = binding_by_name
                        .get(effect_source.binding.value.as_str())
                        .copied()
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::UnknownName,
                            effect_source.binding.span,
                        ));
                        continue;
                    };
                    if binding.collection_local && !collection_local {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowLease,
                            effect_source.binding.span,
                        ));
                        continue;
                    }
                    let Some(lease) = workflows
                        .iter()
                        .find(|workflow| workflow.entity_id == binding.entity_id)
                        .and_then(|workflow| workflow.lease.as_ref())
                        .filter(|lease| lease.name == effect_source.lease.value)
                    else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowLease,
                            effect_source.lease.span,
                        ));
                        continue;
                    };
                    if binding.mode != BindingMode::Mutate || !leased_bindings.insert(binding.id) {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowLease,
                            effect.span,
                        ));
                        continue;
                    }

                    macro_rules! lease_root {
                        ($source:expr, $expected:expr) => {{
                            let Some(value) = lower_root(
                                &mut resolver,
                                $source,
                                Some($expected),
                                false,
                                diagnostics,
                            ) else {
                                continue;
                            };
                            value
                        }};
                    }
                    macro_rules! lease_outcome {
                        ($source:expr) => {{
                            let Some(value) = lower_outcome(
                                command_id,
                                $source,
                                symbols,
                                &mut resolver,
                                &reveal_scope,
                                false,
                                diagnostics,
                            ) else {
                                continue;
                            };
                            value
                        }};
                    }
                    let operation = match &effect_source.operation {
                        SyntaxWorkflowLeaseOperation::Claim {
                            owner,
                            duration_seconds,
                            expected_revision,
                            stale,
                            unavailable,
                            invalid,
                            exhausted,
                        } => HirWorkflowLeaseOperation::Claim {
                            owner: lease_root!(owner, &ValueType::uuid()),
                            duration_seconds: lease_root!(duration_seconds, &ValueType::u64()),
                            expected_revision: lease_root!(expected_revision, &ValueType::u64()),
                            stale: lease_outcome!(stale),
                            unavailable: lease_outcome!(unavailable),
                            invalid: lease_outcome!(invalid),
                            exhausted: lease_outcome!(exhausted),
                        },
                        SyntaxWorkflowLeaseOperation::Renew {
                            owner,
                            fencing_token,
                            duration_seconds,
                            expected_revision,
                            stale,
                            invalid,
                            expired,
                            exhausted,
                        } => HirWorkflowLeaseOperation::Renew {
                            owner: lease_root!(owner, &ValueType::uuid()),
                            fencing_token: lease_root!(fencing_token, &ValueType::u64()),
                            duration_seconds: lease_root!(duration_seconds, &ValueType::u64()),
                            expected_revision: lease_root!(expected_revision, &ValueType::u64()),
                            stale: lease_outcome!(stale),
                            invalid: lease_outcome!(invalid),
                            expired: lease_outcome!(expired),
                            exhausted: lease_outcome!(exhausted),
                        },
                        SyntaxWorkflowLeaseOperation::Release {
                            owner,
                            fencing_token,
                            expected_revision,
                            stale,
                            invalid,
                        } => HirWorkflowLeaseOperation::Release {
                            owner: lease_root!(owner, &ValueType::uuid()),
                            fencing_token: lease_root!(fencing_token, &ValueType::u64()),
                            expected_revision: lease_root!(expected_revision, &ValueType::u64()),
                            stale: lease_outcome!(stale),
                            invalid: lease_outcome!(invalid),
                        },
                        SyntaxWorkflowLeaseOperation::Expire {
                            expected_revision,
                            stale,
                            active,
                        } => HirWorkflowLeaseOperation::Expire {
                            expected_revision: lease_root!(expected_revision, &ValueType::u64()),
                            stale: lease_outcome!(stale),
                            active: lease_outcome!(active),
                        },
                        SyntaxWorkflowLeaseOperation::Fence {
                            owner,
                            fencing_token,
                            expected_revision,
                            stale,
                            invalid,
                            expired,
                        } => HirWorkflowLeaseOperation::Fence {
                            owner: lease_root!(owner, &ValueType::uuid()),
                            fencing_token: lease_root!(fencing_token, &ValueType::u64()),
                            expected_revision: lease_root!(expected_revision, &ValueType::u64()),
                            stale: lease_outcome!(stale),
                            invalid: lease_outcome!(invalid),
                            expired: lease_outcome!(expired),
                        },
                    };
                    effects.push(HirEffect::WorkflowLease {
                        lease_span: effect_source.lease.span,
                        binding: binding.id,
                        owner_field: lease.owner_field,
                        expiry_field: lease.expiry_field,
                        fencing_token_field: lease.fencing_token_field,
                        attempt_field: lease.attempt_field,
                        minimum_duration_seconds: lease.minimum_duration_seconds,
                        maximum_duration_seconds: lease.maximum_duration_seconds,
                        operation: Box::new(operation),
                    });
                }
            }
            if collection_local {
                lowered_collection_effect_count += effects.len() - effects_before;
            }
        }
        let mut collection_initializers = Vec::new();
        let mut top_level_initializers = Vec::new();
        for binding in bindings
            .iter()
            .filter(|binding| binding.mode == BindingMode::Create)
        {
            if invocation_class == CommandInvocationClass::Reimport {
                continue;
            }
            let Some(workflow) = workflows
                .iter()
                .find(|workflow| workflow.entity_id == binding.entity_id)
            else {
                continue;
            };
            let Some(initial_state) = workflow.initial_state else {
                continue;
            };
            let value_type = ValueType::enumeration(workflow.state_enum);
            let value = CanonicalValue::Enum {
                type_id: workflow.state_enum,
                variant_id: initial_state,
            };
            let value_id = match resolver.push_constant(value, value_type.clone(), binding.span) {
                Ok(value_id) => value_id,
                Err(diagnostic) => {
                    diagnostics.push(diagnostic);
                    continue;
                }
            };
            let initializer = HirEffect::Set {
                target_span: binding.span,
                binding: binding.id,
                field: workflow.state_field,
                value: HirExpressionRoot {
                    id: value_id,
                    value_type,
                    span: binding.span,
                },
                reveals: Vec::new(),
            };
            if binding.collection_local {
                collection_initializers.push(initializer);
            } else {
                top_level_initializers.push(initializer);
            }
        }
        let repeated_effect_count = lowered_collection_effect_count + collection_initializers.len();
        effects.splice(
            lowered_collection_effect_count..lowered_collection_effect_count,
            collection_initializers,
        );
        effects.extend(top_level_initializers);
        resolver.set_collection_context(false);
        let Some(success) = lower_outcome(
            command_id,
            &source.return_clause.value.outcome,
            symbols,
            &mut resolver,
            &reveal_scope,
            false,
            diagnostics,
        ) else {
            continue;
        };
        let expressions = match resolver.finish_hir(source.name.span) {
            Ok(expressions) => expressions,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        for effect in &effects {
            let (code, span, roots): (CompilerDiagnosticCode, Span, Vec<&HirExpressionRoot>) =
                match effect {
                    HirEffect::WorkflowTransition {
                        expected_revision,
                        transition_span,
                        ..
                    } => (
                        CompilerDiagnosticCode::MissingWorkflowRevision,
                        *transition_span,
                        vec![expected_revision],
                    ),
                    HirEffect::WorkflowLease {
                        lease_span,
                        operation,
                        ..
                    } => {
                        let roots = match operation.as_ref() {
                            HirWorkflowLeaseOperation::Claim {
                                owner,
                                duration_seconds,
                                expected_revision,
                                ..
                            } => vec![owner, duration_seconds, expected_revision],
                            HirWorkflowLeaseOperation::Renew {
                                owner,
                                fencing_token,
                                duration_seconds,
                                expected_revision,
                                ..
                            } => vec![owner, fencing_token, duration_seconds, expected_revision],
                            HirWorkflowLeaseOperation::Release {
                                owner,
                                fencing_token,
                                expected_revision,
                                ..
                            }
                            | HirWorkflowLeaseOperation::Fence {
                                owner,
                                fencing_token,
                                expected_revision,
                                ..
                            } => vec![owner, fencing_token, expected_revision],
                            HirWorkflowLeaseOperation::Expire {
                                expected_revision, ..
                            } => vec![expected_revision],
                        };
                        (
                            CompilerDiagnosticCode::MissingWorkflowLeaseInput,
                            *lease_span,
                            roots,
                        )
                    }
                    HirEffect::Set { .. } | HirEffect::Emit { .. } => continue,
                };
            if roots.iter().any(|root| {
                !matches!(
                    expressions.node(root.id).map(|node| &node.kind),
                    Some(ExpressionKind::InputField(_))
                )
            }) {
                diagnostics.push(CompilerDiagnostic::new(code, span));
            }
        }
        let collection_expansion = collection_bounds.and_then(
            |(input_field, minimum_elements, maximum_elements, element_type, span)| {
                let binding_count = bindings
                    .iter()
                    .filter(|binding| binding.collection_local)
                    .count();
                if binding_count == 0 || binding_count != collection_bindings.len() {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidBinding,
                        span,
                    ));
                    return None;
                }
                let first_binding = u32::try_from(top_level_binding_count).ok();
                let Some(first_binding) = first_binding else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::BoundExceeded,
                        span,
                    ));
                    return None;
                };
                Some(HirCollectionExpansion {
                    input_field,
                    minimum_elements,
                    maximum_elements,
                    element_type,
                    first_binding: BindingId::new(first_binding),
                    binding_count,
                    repeated_requirement_count,
                    repeated_effect_count,
                    span,
                })
            },
        );
        result.push(HirCommand {
            id: command_id,
            name: source.name.value.clone(),
            span: source.name.span,
            inputs,
            service_values,
            idempotency,
            bindings,
            requirements,
            effects,
            success,
            expressions,
            collection_expansion,
            invocation_class,
        });
    }
    result
}

fn normalize_reimport_command(
    source: &CommandDeclaration,
    symbols: &GenesisSymbols,
    entities: &[HirEntity],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<CommandDeclaration> {
    let clause = source.reconstitution.as_ref()?;
    if source.inputs.len() != 1 {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidType,
            source.name.span,
        ));
        return None;
    }
    let input = &source.inputs[0];
    if input.value.field.name.value != clause.value.source.value {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            clause.value.source.span,
        ));
        return None;
    }
    let TypeExpression::List { element, .. } = &input.value.field.ty.value else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidType,
            input.value.field.ty.span,
        ));
        return None;
    };
    let TypeExpression::Named(record_name) = &element.value else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidType,
            element.span,
        ));
        return None;
    };
    if record_name.value != clause.value.entity.value {
        diagnostics.push(
            CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidType,
                clause.value.entity.span,
            )
            .with_related_span(record_name.span),
        );
        return None;
    }
    let Some(entity_id) = symbols.entities.get(&clause.value.entity.value).copied() else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            clause.value.entity.span,
        ));
        return None;
    };
    let Some(entity) = entities.iter().find(|entity| entity.id == entity_id) else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIr,
            clause.value.entity.span,
        ));
        return None;
    };

    let record_name = "__riffdb_reimport_record".to_owned();
    let binding_name = "__riffdb_reimport_target".to_owned();
    let record_span = clause.value.source.span;
    let binding_span = clause.value.entity.span;
    let path_expression = |head: &str, head_span: Span, field: &HirField| {
        let path = Path {
            segments: vec![
                Spanned::new(head.to_owned(), head_span),
                Spanned::new(field.name.clone(), field.name_span),
            ],
        };
        let span = head_span.cover(field.name_span);
        Spanned::new(Expression::Path(Spanned::new(path, span)), span)
    };

    let arguments = entity
        .key_fields
        .iter()
        .filter_map(|field_id| entity.fields.iter().find(|field| field.id == *field_id))
        .map(|field| path_expression(&record_name, record_span, field))
        .collect();
    let create_binding = Spanned::new(
        Binding::Create(EntityBinding {
            entity: clause.value.entity.clone(),
            arguments,
            binding: Spanned::new(binding_name.clone(), binding_span),
            failure: clause.value.failure.clone(),
            restriction_failure: None,
            cascade_failure: None,
        }),
        clause.span,
    );
    let mut bindings = Vec::with_capacity(entity.relationships.len() + 1);
    for (index, relationship) in entity.relationships.iter().enumerate() {
        let Some(target) = entities
            .iter()
            .find(|candidate| candidate.id == relationship.target_entity)
        else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidRelationship,
                relationship.target_entity_span,
            ));
            return None;
        };
        let arguments = relationship
            .source_fields
            .iter()
            .filter_map(|(field_id, _)| entity.fields.iter().find(|field| field.id == *field_id))
            .map(|field| path_expression(&record_name, record_span, field))
            .collect();
        bindings.push(Spanned::new(
            Binding::Read(EntityBinding {
                entity: Spanned::new(target.name.clone(), relationship.target_entity_span),
                arguments,
                binding: Spanned::new(
                    format!("__riffdb_reimport_dependency_{index}"),
                    relationship.name_span,
                ),
                failure: clause.value.failure.clone(),
                restriction_failure: None,
                cascade_failure: None,
            }),
            relationship.name_span,
        ));
    }
    bindings.push(create_binding);
    let key_fields = entity.key_field_set();
    let effects = entity
        .fields
        .iter()
        .filter(|field| !key_fields.contains(&field.id))
        .map(|field| {
            let target = Path {
                segments: vec![
                    Spanned::new(binding_name.clone(), binding_span),
                    Spanned::new(field.name.clone(), field.name_span),
                ],
            };
            let target_span = binding_span.cover(field.name_span);
            Spanned::new(
                Effect::Set(SetEffect {
                    target: Spanned::new(target, target_span),
                    value: path_expression(&record_name, record_span, field),
                    reveals: Vec::new(),
                }),
                clause.span,
            )
        })
        .collect();

    Some(CommandDeclaration {
        kind: CommandKind::Bulk,
        name: source.name.clone(),
        inputs: source.inputs.clone(),
        service_values: Vec::new(),
        idempotency: None,
        bindings: Vec::new(),
        bulk_iteration: Some(Spanned::new(
            BulkIteration {
                element: Spanned::new(record_name, record_span),
                collection: clause.value.source.clone(),
                bindings,
                requirements: Vec::new(),
                effects,
            },
            clause.span,
        )),
        reconstitution: source.reconstitution.clone(),
        requirements: Vec::new(),
        effects: Vec::new(),
        return_clause: source.return_clause.clone(),
    })
}

fn lower_projections(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    events: &[HirEvent],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirProjection> {
    let mut result = Vec::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::Projection(source) = &declaration.value else {
            continue;
        };
        let Some(id) = symbols.projections.get(&source.name.value).copied() else {
            continue;
        };
        let Some(event_id) = symbols.events.get(&source.source_event.value).copied() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                source.source_event.span,
            ));
            continue;
        };
        let Some(event) = events.iter().find(|event| event.id == event_id) else {
            continue;
        };
        let mut resolver = ExpressionLowerer::new(
            symbols,
            ExpressionScope::Projection {
                event_id,
                fields: event.fields_by_name(),
            },
        );
        let filter = source.filter.as_ref().and_then(|filter| {
            lower_root(
                &mut resolver,
                filter,
                Some(&ValueType::bool()),
                false,
                diagnostics,
            )
        });
        let key = source
            .key
            .iter()
            .filter_map(|expression| {
                lower_root(&mut resolver, expression, None, false, diagnostics)
            })
            .collect();
        let measures = source
            .measures
            .iter()
            .filter_map(|measure| {
                let measure_id = symbols
                    .projection_measures
                    .get(&(id, measure.value.name.value.clone()))
                    .copied()?;
                let kind = match &measure.value.aggregation.value {
                    Aggregation::Count => HirMeasureKind::Count,
                    Aggregation::Sum(expression) => HirMeasureKind::Sum(lower_root(
                        &mut resolver,
                        expression,
                        None,
                        false,
                        diagnostics,
                    )?),
                };
                Some(HirMeasure {
                    id: measure_id,
                    name: measure.value.name.value.clone(),
                    name_span: measure.value.name.span,
                    kind,
                })
            })
            .collect();
        let expressions = match resolver.finish_hir(source.name.span) {
            Ok(expressions) => expressions,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        result.push(HirProjection {
            id,
            name: source.name.value.clone(),
            span: source.name.span,
            source_event: event_id,
            filter,
            key,
            measures,
            frontier: HirFrontier::TransactionallyOrdered,
            expressions,
        });
    }
    result
}

fn lower_field(
    name: &Spanned<String>,
    type_span: Span,
    id: Option<FieldId>,
    value_type: Option<&ValueType>,
) -> Option<HirField> {
    lower_classified_field(name, type_span, id, value_type, None)
}

fn lower_classified_field(
    name: &Spanned<String>,
    type_span: Span,
    id: Option<FieldId>,
    value_type: Option<&ValueType>,
    secret_span: Option<Span>,
) -> Option<HirField> {
    Some(HirField {
        id: id?,
        name: name.value.clone(),
        name_span: name.span,
        type_span,
        value_type: value_type?.clone(),
        secret_span,
    })
}

fn lower_invariant(
    symbols: &GenesisSymbols,
    entity_id: EntityTypeId,
    invariant_id: InvariantId,
    name: &Spanned<String>,
    expression: &Spanned<Expression>,
    fields: BTreeMap<String, (FieldId, ValueType)>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<HirInvariant> {
    let mut resolver =
        ExpressionLowerer::new(symbols, ExpressionScope::Schema { entity_id, fields });
    let root = lower_root(
        &mut resolver,
        expression,
        Some(&ValueType::bool()),
        false,
        diagnostics,
    )?;
    let expressions = match resolver.finish_hir(expression.span) {
        Ok(expressions) => expressions,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return None;
        }
    };
    Some(HirInvariant {
        id: invariant_id,
        name: name.value.clone(),
        name_span: name.span,
        expression: root,
        expressions,
    })
}

fn lower_outcome(
    command_id: CommandId,
    source: &Spanned<OutcomeExpression>,
    symbols: &GenesisSymbols,
    resolver: &mut ExpressionLowerer<'_>,
    reveal_scope: &SecretRevealScope,
    input_only: bool,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<HirOutcome> {
    reject_duplicate_fields(
        &source.value.payload.value,
        CompilerDiagnosticCode::InvalidOutcome,
        diagnostics,
    );
    let outcome_id = symbols
        .outcomes
        .get(&(command_id, source.value.name.value.clone()))
        .copied()?;
    let fields = source
        .value
        .payload
        .value
        .fields
        .iter()
        .filter_map(|field| {
            let field_id = symbols
                .outcome_fields
                .get(&(command_id, outcome_id, field.value.name.value.clone()))
                .copied()?;
            let value = lower_root(resolver, &field.value.value, None, input_only, diagnostics)?;
            let reveals = lower_secret_reveals(&field.value.reveals, reveal_scope, diagnostics);
            Some(HirObjectField {
                id: field_id,
                name: field.value.name.value.clone(),
                name_span: field.value.name.span,
                value,
                reveals,
            })
        })
        .collect();
    Some(HirOutcome {
        id: outcome_id,
        name: source.value.name.value.clone(),
        span: source.span,
        fields,
    })
}

fn lower_declared_object(
    source: &Spanned<ObjectLiteral>,
    declared: &[HirField],
    resolver: &mut ExpressionLowerer<'_>,
    reveal_scope: &SecretRevealScope,
    error_code: CompilerDiagnosticCode,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirObjectField> {
    reject_duplicate_fields(&source.value, error_code, diagnostics);
    let supplied = source
        .value
        .fields
        .iter()
        .map(|field| (field.value.name.value.as_str(), field))
        .collect::<BTreeMap<_, _>>();
    let mut result = Vec::new();
    for field in declared {
        let value = if let Some(supplied) = supplied.get(field.name.as_str()) {
            lower_root(
                resolver,
                &supplied.value.value,
                Some(&field.value_type),
                false,
                diagnostics,
            )
        } else if field.value_type.is_optional() {
            match resolver.push_constant(
                CanonicalValue::Null,
                field.value_type.clone(),
                source.span,
            ) {
                Ok(id) => Some(HirExpressionRoot {
                    id,
                    value_type: field.value_type.clone(),
                    span: source.span,
                }),
                Err(diagnostic) => {
                    diagnostics.push(diagnostic);
                    None
                }
            }
        } else {
            diagnostics.push(CompilerDiagnostic::new(error_code, source.span));
            None
        };
        if let Some(value) = value {
            let reveals = supplied
                .get(field.name.as_str())
                .map_or_else(Vec::new, |field| {
                    lower_secret_reveals(&field.value.reveals, reveal_scope, diagnostics)
                });
            result.push(HirObjectField {
                id: field.id,
                name: field.name.clone(),
                name_span: supplied
                    .get(field.name.as_str())
                    .map_or(source.span, |field| field.value.name.span),
                value,
                reveals,
            });
        }
    }
    for (name, supplied) in supplied {
        if !declared.iter().any(|field| field.name == name) {
            diagnostics.push(CompilerDiagnostic::new(
                error_code,
                supplied.value.name.span,
            ));
        }
    }
    result
}

fn lower_secret_reveals(
    sources: &[Spanned<Path>],
    scope: &SecretRevealScope,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<HirSecretReveal> {
    let mut result = Vec::new();
    let mut seen = BTreeSet::new();
    for source in sources {
        let Some((binding_name, field_name)) = path_pair(&source.value) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidSecretReveal,
                source.span,
            ));
            continue;
        };
        let Some(binding) = scope.get(binding_name) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                source.span,
            ));
            continue;
        };
        let Some((field, source_span, secret)) = binding.fields.get(field_name).copied() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                source.span,
            ));
            continue;
        };
        if !secret || !seen.insert((binding.binding, field)) {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidSecretReveal, source.span)
                    .with_related_span(source_span),
            );
            continue;
        }
        result.push(HirSecretReveal {
            binding: binding.binding,
            entity: binding.entity,
            field,
            annotation_span: source.span,
            source_span,
        });
    }
    result.sort_by_key(|reveal| (reveal.binding, reveal.field));
    result
}

fn lower_root(
    resolver: &mut ExpressionLowerer<'_>,
    expression: &Spanned<Expression>,
    expected: Option<&ValueType>,
    input_only: bool,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<HirExpressionRoot> {
    let lowered = if input_only {
        resolver.lower_input_only(expression, expected)
    } else {
        resolver.lower(expression, expected)
    };
    match lowered {
        Ok((id, value_type)) => Some(HirExpressionRoot {
            id,
            value_type,
            span: expression.span,
        }),
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            None
        }
    }
}

fn reject_duplicate_fields(
    object: &ObjectLiteral,
    code: CompilerDiagnosticCode,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let mut first = BTreeMap::new();
    for field in &object.fields {
        if let Some(first_span) = first.get(&field.value.name.value) {
            diagnostics.push(
                CompilerDiagnostic::new(code, field.value.name.span).with_related_span(*first_span),
            );
        } else {
            first.insert(field.value.name.value.clone(), field.value.name.span);
        }
    }
}

fn fields_by_name(fields: &[HirField]) -> BTreeMap<String, (FieldId, ValueType)> {
    fields
        .iter()
        .map(|field| (field.name.clone(), (field.id, field.value_type.clone())))
        .collect()
}

fn entity_field_map(
    entity_id: EntityTypeId,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
) -> BTreeMap<String, (FieldId, ValueType)> {
    symbols
        .entity_fields
        .iter()
        .filter_map(|((owner, name), field_id)| {
            (*owner == entity_id).then(|| {
                types
                    .entity_fields
                    .get(&(entity_id, *field_id))
                    .cloned()
                    .map(|value_type| (name.clone(), (*field_id, value_type)))
            })?
        })
        .collect()
}

fn path_pair(path: &Path) -> Option<(&str, &str)> {
    (path.segments.len() == 2).then(|| {
        (
            path.segments[0].value.as_str(),
            path.segments[1].value.as_str(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_synthetic_hir_reports_invalid_ir_at_enclosing_span() {
        let enclosing = Span::new(20, 31).expect("span");
        let node_span = Span::new(23, 27).expect("span");
        let forward_reference = HirExpressionArena {
            nodes: vec![HirExpressionNode {
                kind: ExpressionKind::Unary {
                    operator: riffdb_contract_ir::UnaryOperator::Not,
                    operand: ExprId::new(1),
                },
                value_type: ValueType::bool(),
                span: node_span,
            }],
        };
        let diagnostic = forward_reference
            .to_ir(enclosing)
            .expect_err("forward reference");
        assert_eq!(diagnostic.code(), CompilerDiagnosticCode::InvalidIr);
        assert_eq!(diagnostic.primary_span(), enclosing);

        let valid_arena = HirExpressionArena {
            nodes: vec![HirExpressionNode {
                kind: ExpressionKind::Constant(CanonicalValue::Bool(true)),
                value_type: ValueType::bool(),
                span: node_span,
            }],
        };
        let diagnostic = valid_arena
            .dependencies(ExprId::new(1), enclosing)
            .expect_err("dangling root");
        assert_eq!(diagnostic.code(), CompilerDiagnosticCode::InvalidIr);
        assert_eq!(diagnostic.primary_span(), enclosing);
    }
}
