//! Compiler-private, source-spanned, resolved typed high-level IR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    BindingId, BindingMode, ExprId, ExpressionArena, ExpressionKind, ValueType, ValueTypeTag,
};
use riffdb_contract_syntax::Span;
use riffdb_contract_syntax::ast::{
    AggregateItem, Aggregation, Binding, Declaration, Effect, EntityItem, Expression,
    ObjectLiteral, OutcomeExpression, Path, ServiceValueKind,
};
use riffdb_contract_syntax::{ContractDocument, Spanned};
use riffdb_types::{
    AggregateTypeId, CanonicalValue, CommandId, ContractVersion, EntityTypeId, EnumTypeId,
    EnumVariantId, EventTypeId, FieldId, IndexId, InvariantId, OutcomeId, ProjectionId,
};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::expression_lowering::{BindingExpressionScope, ExpressionLowerer, ExpressionScope};
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
pub(crate) struct HirEntity {
    pub(crate) id: EntityTypeId,
    pub(crate) name: String,
    pub(crate) span: Span,
    pub(crate) fields: Vec<HirField>,
    pub(crate) key_fields: Vec<FieldId>,
    pub(crate) invariants: Vec<HirInvariant>,
    pub(crate) indexes: Vec<HirIndex>,
    pub(crate) relationships: Vec<HirRelationship>,
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
    pub(crate) fields: Vec<HirField>,
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
    pub(crate) transitions: Vec<HirWorkflowTransition>,
    pub(crate) lease: Option<HirWorkflowLease>,
}

#[derive(Clone, Debug)]
pub(crate) struct HirObjectField {
    pub(crate) id: FieldId,
    pub(crate) name: String,
    pub(crate) name_span: Span,
    pub(crate) value: HirExpressionRoot,
}

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
    let events = lower_events(document, symbols, types, &mut diagnostics);
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
                    if let Some(lowered) = lower_field(
                        &field.name,
                        field.ty.span,
                        field_id,
                        field_id.and_then(|field_id| types.entity_fields.get(&(id, field_id))),
                    ) {
                        fields.push(lowered);
                    }
                }
                EntityItem::Invariant(_)
                | EntityItem::Index(_)
                | EntityItem::Unique(_)
                | EntityItem::Reference(_) => {}
            }
        }
        let field_scope = fields_by_name(&fields);
        let mut invariants = Vec::new();
        let mut indexes = Vec::new();
        let mut relationships = Vec::new();
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
                    indexes.push(HirIndex {
                        id: index_id,
                        name: index.name.value.clone(),
                        span: index.name.span,
                        fields: index_fields,
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
                EntityItem::Key(_) | EntityItem::Field(_) => {}
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
        });
    }
    result
}

fn lower_events(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
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
                .collect();
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
            Some(HirEvent {
                id,
                name: source.name.value.clone(),
                span: source.name.span,
                partition_fields,
                partition_span: source.partition_by.as_ref().map(|partition| partition.span),
                fields,
            })
        })
        .collect()
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
    if source.attempt_field.is_some() && attempts.is_none() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            source.attempt_field.as_ref().expect("checked present").span,
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
        let Declaration::Command(source) = &declaration.value else {
            continue;
        };
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
        let mut binding_descriptors = Vec::new();
        for (index, binding) in source.bindings.iter().enumerate() {
            let (binding, mode) = match &binding.value {
                Binding::Read(binding) => (binding, BindingMode::Read),
                Binding::Mutate(binding) => (binding, BindingMode::Mutate),
                Binding::Create(binding) => (binding, BindingMode::Create),
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
            binding_descriptors.push((binding, BindingId::new(index), mode, entity));
        }
        let binding_scope = binding_descriptors
            .iter()
            .map(|(source, id, _, entity)| {
                (
                    source.binding.value.clone(),
                    BindingExpressionScope {
                        id: *id,
                        entity_id: entity.id,
                        fields: entity.fields_by_name(),
                    },
                )
            })
            .collect();
        let scope = ExpressionScope::Command {
            command_id,
            inputs: input_scope,
            service_values: service_value_scope,
            bindings: binding_scope,
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
        for (binding, binding_id, mode, entity) in binding_descriptors {
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
                true,
                diagnostics,
            ) else {
                continue;
            };
            bindings.push(HirBinding {
                id: binding_id,
                mode,
                entity_id: entity.id,
                entity_span: binding.entity.span,
                name: binding.binding.value.clone(),
                name_span: binding.binding.span,
                span: binding.failure.span.cover(binding.entity.span),
                arguments,
                failure,
            });
        }
        let mut requirements = Vec::new();
        for requirement in &source.requirements {
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
        for effect in &source.effects {
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
                    if workflows.iter().any(|workflow| {
                        workflow.entity_id == binding.entity_id && workflow.state_field == field.id
                    }) {
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
                    effects.push(HirEffect::Set {
                        target_span: set.target.span,
                        binding: binding.id,
                        field: field.id,
                        value,
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
            }
        }
        let Some(success) = lower_outcome(
            command_id,
            &source.return_clause.value.outcome,
            symbols,
            &mut resolver,
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
            let HirEffect::WorkflowTransition {
                expected_revision,
                transition_span,
                ..
            } = effect
            else {
                continue;
            };
            if !matches!(
                expressions
                    .node(expected_revision.id)
                    .map(|node| &node.kind),
                Some(ExpressionKind::InputField(_))
            ) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::MissingWorkflowRevision,
                    *transition_span,
                ));
            }
        }
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
        });
    }
    result
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
    Some(HirField {
        id: id?,
        name: name.value.clone(),
        name_span: name.span,
        type_span,
        value_type: value_type?.clone(),
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
            Some(HirObjectField {
                id: field_id,
                name: field.value.name.value.clone(),
                name_span: field.value.name.span,
                value,
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
            result.push(HirObjectField {
                id: field.id,
                name: field.name.clone(),
                name_span: supplied
                    .get(field.name.as_str())
                    .map_or(source.span, |field| field.value.name.span),
                value,
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
