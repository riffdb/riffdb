//! Static aggregate ownership, dependency visibility, and locality analysis over typed HIR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{BinaryOperator, BindingMode, ExpressionKind, UnaryOperator};
use riffdb_contract_syntax::Span;
use riffdb_types::{AggregateTypeId, EntityTypeId, FieldId, encode_canonical_value};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{HirBinding, HirExpressionArena, HirExpressionRoot, TypedContractHir};

/// Proven aggregate owner for each grammar-v1 command-bindable entity.
#[derive(Clone, Debug)]
pub(crate) struct LocalityAnalysis {
    pub(crate) entity_owner: BTreeMap<EntityTypeId, AggregateTypeId>,
}

/// Proves the closed ADR-0016 aggregate ownership and single-partition convention.
pub(crate) fn analyze_locality(
    hir: &TypedContractHir,
) -> Result<LocalityAnalysis, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let mut owners = BTreeMap::<EntityTypeId, (AggregateTypeId, Span)>::new();
    for aggregate in &hir.aggregates {
        register_owner(
            aggregate.root,
            aggregate.id,
            aggregate.root_span,
            &mut owners,
            &mut diagnostics,
        );
        let Some(root) = hir.entity(aggregate.root) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                aggregate.root_span,
            ));
            continue;
        };
        validate_aggregate_key_expression(
            &aggregate.keys.expressions,
            &aggregate.keys.partition,
            &root.key_field_set(),
            &mut diagnostics,
        );
        for conflict in &aggregate.keys.conflicts {
            validate_aggregate_key_expression(
                &aggregate.keys.expressions,
                conflict,
                &root.key_field_set(),
                &mut diagnostics,
            );
        }
        for (child_id, span) in &aggregate.children {
            register_owner(
                *child_id,
                aggregate.id,
                *span,
                &mut owners,
                &mut diagnostics,
            );
            let Some(child) = hir.entity(*child_id) else {
                continue;
            };
            if !has_exact_root_key_prefix(root, child) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidAggregate,
                    *span,
                ));
            }
        }
    }

    for command in &hir.commands {
        let mut mutation_aggregate = None;
        let mut partition_fingerprint = None;
        for binding in &command.bindings {
            let Some((aggregate_id, _)) = owners.get(&binding.entity_id).copied() else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidBinding,
                    binding.entity_span,
                ));
                continue;
            };
            if matches!(binding.mode, BindingMode::Create | BindingMode::Mutate) {
                if let Some(expected) = mutation_aggregate {
                    if expected != aggregate_id {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::CrossPartitionMutation,
                            binding.entity_span,
                        ));
                    }
                } else {
                    mutation_aggregate = Some(aggregate_id);
                }
            }
            validate_input_only_binding(command, binding, &mut diagnostics);
            let Some(aggregate) = hir.aggregate(aggregate_id) else {
                continue;
            };
            let Some(root) = hir.entity(aggregate.root) else {
                continue;
            };
            let substitutions = root
                .key_fields
                .iter()
                .copied()
                .zip(binding.arguments.iter())
                .collect::<BTreeMap<_, _>>();
            let fingerprint = expression_fingerprint(
                &aggregate.keys.expressions,
                aggregate.keys.partition.id,
                &substitutions,
                &command.expressions,
            );
            if let Some(expected) = &partition_fingerprint {
                if expected != &fingerprint {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::CrossPartitionMutation,
                        binding.entity_span,
                    ));
                }
            } else {
                partition_fingerprint = Some(fingerprint);
            }
        }
    }

    if diagnostics.is_empty() {
        Ok(LocalityAnalysis {
            entity_owner: owners
                .into_iter()
                .map(|(entity, (aggregate, _))| (entity, aggregate))
                .collect(),
        })
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn register_owner(
    entity_id: EntityTypeId,
    aggregate_id: AggregateTypeId,
    span: Span,
    owners: &mut BTreeMap<EntityTypeId, (AggregateTypeId, Span)>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    if let Some((_, first_span)) = owners.get(&entity_id) {
        diagnostics.push(
            CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidAggregate, span)
                .with_related_span(*first_span),
        );
    } else {
        owners.insert(entity_id, (aggregate_id, span));
    }
}

fn has_exact_root_key_prefix(root: &crate::hir::HirEntity, child: &crate::hir::HirEntity) -> bool {
    child.key_fields.len() >= root.key_fields.len()
        && root
            .key_fields
            .iter()
            .zip(&child.key_fields)
            .all(|(root_id, child_id)| {
                let root_field = root.fields.iter().find(|field| field.id == *root_id);
                let child_field = child.fields.iter().find(|field| field.id == *child_id);
                root_field.zip(child_field).is_some_and(|(root, child)| {
                    root.name == child.name && root.value_type == child.value_type
                })
            })
}

fn validate_aggregate_key_expression(
    arena: &HirExpressionArena,
    root: &HirExpressionRoot,
    root_key_fields: &BTreeSet<FieldId>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    visit_expression(arena, root.id, &mut |kind, span| {
        if let ExpressionKind::SchemaField { field, .. } = kind
            && !root_key_fields.contains(field)
        {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::ConflictNotInputComputable,
                span,
            ));
        }
    });
}

fn validate_input_only_binding(
    command: &crate::hir::HirCommand,
    binding: &HirBinding,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    for argument in &binding.arguments {
        visit_expression(&command.expressions, argument.id, &mut |kind, span| {
            if matches!(
                kind,
                ExpressionKind::BoundField { .. }
                    | ExpressionKind::CompleteBinding(_)
                    | ExpressionKind::TransactionTime
                    | ExpressionKind::TransactionDate
                    | ExpressionKind::SchemaField { .. }
                    | ExpressionKind::SourceEventField(_)
            ) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidBinding,
                    span,
                ));
            }
        });
    }
}

fn visit_expression(
    arena: &HirExpressionArena,
    root: riffdb_contract_ir::ExprId,
    visitor: &mut impl FnMut(&ExpressionKind, Span),
) {
    let mut pending = vec![root];
    let mut visited = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let Some(node) = arena.node(id) else {
            continue;
        };
        visitor(&node.kind, node.span);
        match &node.kind {
            ExpressionKind::Unary { operand, .. } => pending.push(*operand),
            ExpressionKind::Binary { left, right, .. } => {
                pending.push(*right);
                pending.push(*left);
            }
            _ => {}
        }
    }
}

fn expression_fingerprint(
    aggregate_arena: &HirExpressionArena,
    root: riffdb_contract_ir::ExprId,
    substitutions: &BTreeMap<FieldId, &HirExpressionRoot>,
    command_arena: &HirExpressionArena,
) -> Vec<u8> {
    let mut builder = FingerprintBuilder::default();
    let root = builder.intern_primary(aggregate_arena, root, substitutions, command_arena);
    builder.finish(root)
}

/// Structural key for comparing checked command expressions independently of
/// their dense arena positions.
pub(crate) fn command_expression_fingerprint(
    arena: &HirExpressionArena,
    root: riffdb_contract_ir::ExprId,
) -> Vec<u8> {
    expression_fingerprint(arena, root, &BTreeMap::new(), arena)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FingerprintNode {
    Invalid,
    Constant(Vec<u8>),
    InputField(u32),
    ServiceValue(u32),
    Unary(u8, u32),
    Binary(u8, u32, u32),
    SchemaField(u32, u32),
    BoundField(u32, u32),
    CompleteBinding(u32),
    SourceEventField(u32),
    TransactionTime,
    TransactionDate,
    RootValidationField(u32, u32),
}

#[derive(Default)]
struct FingerprintBuilder {
    nodes: Vec<FingerprintNode>,
    interned: BTreeMap<FingerprintNode, u32>,
    primary_memo: BTreeMap<riffdb_contract_ir::ExprId, u32>,
    command_memo: BTreeMap<riffdb_contract_ir::ExprId, u32>,
}

impl FingerprintBuilder {
    fn intern_primary(
        &mut self,
        arena: &HirExpressionArena,
        id: riffdb_contract_ir::ExprId,
        substitutions: &BTreeMap<FieldId, &HirExpressionRoot>,
        command_arena: &HirExpressionArena,
    ) -> u32 {
        if let Some(existing) = self.primary_memo.get(&id).copied() {
            return existing;
        }
        let canonical = match arena.node(id) {
            Some(node) => {
                if let ExpressionKind::SchemaField { field, .. } = &node.kind
                    && let Some(replacement) = substitutions.get(field)
                {
                    let canonical = self.intern_command(command_arena, replacement.id);
                    self.primary_memo.insert(id, canonical);
                    return canonical;
                }
                self.canonical_primary_node(arena, node, substitutions, command_arena)
            }
            None => FingerprintNode::Invalid,
        };
        let canonical = self.intern(canonical);
        self.primary_memo.insert(id, canonical);
        canonical
    }

    fn canonical_primary_node(
        &mut self,
        arena: &HirExpressionArena,
        node: &crate::hir::HirExpressionNode,
        substitutions: &BTreeMap<FieldId, &HirExpressionRoot>,
        command_arena: &HirExpressionArena,
    ) -> FingerprintNode {
        match &node.kind {
            ExpressionKind::Unary { operator, operand } => FingerprintNode::Unary(
                unary_tag(*operator),
                self.intern_primary(arena, *operand, substitutions, command_arena),
            ),
            ExpressionKind::Binary {
                operator,
                left,
                right,
            } => FingerprintNode::Binary(
                binary_tag(*operator),
                self.intern_primary(arena, *left, substitutions, command_arena),
                self.intern_primary(arena, *right, substitutions, command_arena),
            ),
            kind => fingerprint_leaf(kind),
        }
    }

    fn intern_command(
        &mut self,
        arena: &HirExpressionArena,
        id: riffdb_contract_ir::ExprId,
    ) -> u32 {
        if let Some(existing) = self.command_memo.get(&id).copied() {
            return existing;
        }
        let canonical = match arena.node(id) {
            Some(node) => match &node.kind {
                ExpressionKind::Unary { operator, operand } => FingerprintNode::Unary(
                    unary_tag(*operator),
                    self.intern_command(arena, *operand),
                ),
                ExpressionKind::Binary {
                    operator,
                    left,
                    right,
                } => FingerprintNode::Binary(
                    binary_tag(*operator),
                    self.intern_command(arena, *left),
                    self.intern_command(arena, *right),
                ),
                kind => fingerprint_leaf(kind),
            },
            None => FingerprintNode::Invalid,
        };
        let canonical = self.intern(canonical);
        self.command_memo.insert(id, canonical);
        canonical
    }

    fn intern(&mut self, node: FingerprintNode) -> u32 {
        if let Some(existing) = self.interned.get(&node).copied() {
            return existing;
        }
        let id = u32::try_from(self.nodes.len()).expect("HIR expression count is bounded");
        self.nodes.push(node.clone());
        self.interned.insert(node, id);
        id
    }

    fn finish(self, root: u32) -> Vec<u8> {
        let mut output = Vec::new();
        output.push(0x01);
        output.extend_from_slice(
            &u32::try_from(self.nodes.len())
                .expect("HIR expression count is bounded")
                .to_be_bytes(),
        );
        for node in self.nodes {
            encode_fingerprint_node(&node, &mut output);
        }
        output.extend_from_slice(&root.to_be_bytes());
        output
    }
}

fn fingerprint_leaf(kind: &ExpressionKind) -> FingerprintNode {
    match kind {
        ExpressionKind::Constant(value) => FingerprintNode::Constant(
            encode_canonical_value(value).expect("HIR constants are canonical"),
        ),
        ExpressionKind::InputField(field) => FingerprintNode::InputField(field.get()),
        ExpressionKind::ServiceValue(field) => FingerprintNode::ServiceValue(field.get()),
        ExpressionKind::SchemaField { entity_type, field } => {
            FingerprintNode::SchemaField(entity_type.get(), field.get())
        }
        ExpressionKind::BoundField { binding, field } => {
            FingerprintNode::BoundField(binding.get(), field.get())
        }
        ExpressionKind::CompleteBinding(binding) => FingerprintNode::CompleteBinding(binding.get()),
        ExpressionKind::SourceEventField(field) => FingerprintNode::SourceEventField(field.get()),
        ExpressionKind::TransactionTime => FingerprintNode::TransactionTime,
        ExpressionKind::TransactionDate => FingerprintNode::TransactionDate,
        ExpressionKind::RootValidationField { read, field } => {
            FingerprintNode::RootValidationField(read.get(), field.get())
        }
        ExpressionKind::Unary { .. } | ExpressionKind::Binary { .. } => {
            unreachable!("operator nodes are handled before leaves")
        }
    }
}

fn encode_fingerprint_node(node: &FingerprintNode, output: &mut Vec<u8>) {
    match node {
        FingerprintNode::Invalid => output.push(0xff),
        FingerprintNode::Constant(bytes) => {
            output.push(0x01);
            output.extend_from_slice(
                &u32::try_from(bytes.len())
                    .expect("bounded canonical value")
                    .to_be_bytes(),
            );
            output.extend_from_slice(bytes);
        }
        FingerprintNode::InputField(field) => append_tagged_u32(output, 0x02, *field),
        FingerprintNode::Unary(operator, operand) => {
            output.extend_from_slice(&[0x03, *operator]);
            output.extend_from_slice(&operand.to_be_bytes());
        }
        FingerprintNode::Binary(operator, left, right) => {
            output.extend_from_slice(&[0x04, *operator]);
            output.extend_from_slice(&left.to_be_bytes());
            output.extend_from_slice(&right.to_be_bytes());
        }
        FingerprintNode::SchemaField(entity, field) => {
            output.push(0x05);
            output.extend_from_slice(&entity.to_be_bytes());
            output.extend_from_slice(&field.to_be_bytes());
        }
        FingerprintNode::BoundField(binding, field) => {
            output.push(0x06);
            output.extend_from_slice(&binding.to_be_bytes());
            output.extend_from_slice(&field.to_be_bytes());
        }
        FingerprintNode::CompleteBinding(binding) => append_tagged_u32(output, 0x07, *binding),
        FingerprintNode::SourceEventField(field) => append_tagged_u32(output, 0x08, *field),
        FingerprintNode::TransactionTime => output.push(0x09),
        FingerprintNode::TransactionDate => output.push(0x0a),
        FingerprintNode::RootValidationField(read, field) => {
            output.push(0x0b);
            output.extend_from_slice(&read.to_be_bytes());
            output.extend_from_slice(&field.to_be_bytes());
        }
        FingerprintNode::ServiceValue(field) => append_tagged_u32(output, 0x0c, *field),
    }
}

fn append_tagged_u32(output: &mut Vec<u8>, tag: u8, value: u32) {
    output.push(tag);
    output.extend_from_slice(&value.to_be_bytes());
}

fn unary_tag(operator: UnaryOperator) -> u8 {
    match operator {
        UnaryOperator::Not => 0x01,
        UnaryOperator::Negate => 0x02,
    }
}

fn binary_tag(operator: BinaryOperator) -> u8 {
    match operator {
        BinaryOperator::Multiply => 0x01,
        BinaryOperator::Divide => 0x02,
        BinaryOperator::Add => 0x03,
        BinaryOperator::Subtract => 0x04,
        BinaryOperator::Equal => 0x05,
        BinaryOperator::NotEqual => 0x06,
        BinaryOperator::Less => 0x07,
        BinaryOperator::LessEqual => 0x08,
        BinaryOperator::Greater => 0x09,
        BinaryOperator::GreaterEqual => 0x0a,
        BinaryOperator::And => 0x0b,
        BinaryOperator::Or => 0x0c,
    }
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn analyze(source: &str) -> Result<LocalityAnalysis, CompilerDiagnostics> {
        let document = parse_contract(source).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document).expect("valid symbols");
        let types = resolve_declared_types(&document, &symbols).expect("valid types");
        let hir = lower_contract_hir(&document, &symbols, &types).expect("valid HIR");
        analyze_locality(&hir)
    }

    #[test]
    fn child_root_prefix_and_structurally_equal_partitions_pass() {
        let source = r#"
contract Example version 1 {
  entity Root { key (tenant: uuid, root_id: uuid) }
  entity Child { key (tenant: uuid, root_id: uuid, child_id: uuid) }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
  }
  command ReadBoth {
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    read Root(tenant, root_id) as root_row else MissingRoot { root_id: root_id }
    read Child(tenant, root_id, child_id) as child_row else MissingChild { child_id: child_id }
    return Found { root_row: root_row, child_row: child_row }
  }
}
"#;
        analyze(source).expect("single partition is provable");
    }

    #[test]
    fn same_partition_external_read_and_one_mutation_aggregate_pass() {
        let source = r#"
contract Example version 1 {
  entity Account { key (tenant: uuid, account_id: uuid) }
  entity Entry { key (tenant: uuid, entry_id: uuid) }
  aggregate Accounts {
    root Account
    partition_by tenant
    conflict_key (tenant, account_id)
  }
  aggregate Entries {
    root Entry
    partition_by tenant
    conflict_key (tenant, entry_id)
  }
  command CreateEntry {
    input request_key: string<16>
    input tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    idempotency_key request_key
    read Account(tenant, account_id) as account else MissingAccount {}
    create Entry(tenant, entry_id) as entry else EntryExists {}
    return Created { entry: entry }
  }
}
"#;
        analyze(source).expect("external read is an exact same-partition dependency");
    }

    #[test]
    fn cross_partition_external_read_rejects() {
        let source = r#"
contract Example version 1 {
  entity Account { key (tenant: uuid, account_id: uuid) }
  entity Entry { key (tenant: uuid, entry_id: uuid) }
  aggregate Accounts {
    root Account
    partition_by tenant
    conflict_key (tenant, account_id)
  }
  aggregate Entries {
    root Entry
    partition_by tenant
    conflict_key (tenant, entry_id)
  }
  command CreateEntry {
    input request_key: string<16>
    input account_tenant: uuid
    input entry_tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    idempotency_key request_key
    read Account(account_tenant, account_id) as account else MissingAccount {}
    create Entry(entry_tenant, entry_id) as entry else EntryExists {}
    return Created { entry: entry }
  }
}
"#;
        let diagnostics = analyze(source).expect_err("cross-partition external read rejects");
        assert!(diagnostics.as_slice().iter().any(|diagnostic| {
            diagnostic.code() == CompilerDiagnosticCode::CrossPartitionMutation
        }));
    }

    #[test]
    fn same_partition_writes_to_two_aggregates_reject() {
        let source = r#"
contract Example version 1 {
  entity Account { key (tenant: uuid, account_id: uuid) }
  entity Entry { key (tenant: uuid, entry_id: uuid) }
  aggregate Accounts {
    root Account
    partition_by tenant
    conflict_key (tenant, account_id)
  }
  aggregate Entries {
    root Entry
    partition_by tenant
    conflict_key (tenant, entry_id)
  }
  command ChangeBoth {
    input request_key: string<16>
    input tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    idempotency_key request_key
    mutate Account(tenant, account_id) as account else MissingAccount {}
    mutate Entry(tenant, entry_id) as entry else MissingEntry {}
    return Changed { account: account, entry: entry }
  }
}
"#;
        let diagnostics = analyze(source).expect_err("two mutation aggregates reject");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::CrossPartitionMutation)
            .expect("mutation aggregate diagnostic");
        let start = source.find("Entry(tenant").expect("second mutation");
        assert_eq!(
            diagnostic.primary_span(),
            Span::new(start, start + 5).expect("span")
        );
    }

    #[test]
    fn cross_partition_binding_arguments_reject_at_second_entity() {
        let source = r#"
contract Example version 1 {
  entity Root { key (tenant: uuid, root_id: uuid) }
  aggregate Family { root Root partition_by tenant conflict_key (tenant, root_id) }
  command ChangeBoth {
    input request_key: string<16>
    input first_tenant: uuid
    input second_tenant: uuid
    input root_id: uuid
    idempotency_key request_key
    mutate Root(first_tenant, root_id) as first else MissingFirst { root_id: root_id }
    mutate Root(second_tenant, root_id) as second else MissingSecond { root_id: root_id }
    return Found { first: first, second: second }
  }
}
"#;
        let diagnostics = analyze(source).expect_err("cross-partition shape rejects");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::CrossPartitionMutation)
            .expect("cross-partition diagnostic");
        let start = source.find("Root(second_tenant").expect("second entity");
        assert_eq!(
            diagnostic.primary_span(),
            Span::new(start, start + 4).expect("span")
        );
    }

    #[test]
    fn dynamic_binding_key_rejects_at_non_input_expression() {
        let source = r#"
contract Example version 1 {
  entity Root { key (tenant: uuid, root_id: uuid) }
  aggregate Family { root Root partition_by tenant conflict_key (tenant, root_id) }
  command Change {
    input request_key: string<16>
    input tenant: uuid
    input root_id: uuid
    idempotency_key request_key
    read Root(tenant, root_id) as observed else MissingObserved {}
    mutate Root(observed.tenant, root_id) as target else MissingTarget {}
    return Found { target: target }
  }
}
"#;
        let diagnostics = analyze(source).expect_err("dynamic acquisition rejects");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidBinding)
            .expect("input-computability diagnostic");
        let start = source.find("observed.tenant").expect("dynamic expression");
        assert_eq!(
            diagnostic.primary_span(),
            Span::new(start, start + 15).expect("span")
        );
    }

    #[test]
    fn child_without_exact_root_key_prefix_rejects() {
        let source = r#"
contract Example version 1 {
  entity Root { key (tenant: uuid, root_id: uuid) }
  entity Child { key (root_id: uuid, tenant: uuid, child_id: uuid) }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
  }
}
"#;
        let diagnostics = analyze(source).expect_err("bad child prefix rejects");
        assert!(
            diagnostics
                .as_slice()
                .iter()
                .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidAggregate)
        );
    }

    #[test]
    fn structural_fingerprint_interns_shared_and_duplicated_dags_linearly() {
        fn node(kind: ExpressionKind) -> crate::hir::HirExpressionNode {
            crate::hir::HirExpressionNode {
                kind,
                value_type: riffdb_contract_ir::ValueType::i64(),
                span: Span::new(0, 1).expect("span"),
            }
        }

        let input = FieldId::first();
        let shared = HirExpressionArena {
            nodes: vec![
                node(ExpressionKind::InputField(input)),
                node(ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: riffdb_contract_ir::ExprId::new(0),
                    right: riffdb_contract_ir::ExprId::new(0),
                }),
                node(ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: riffdb_contract_ir::ExprId::new(1),
                    right: riffdb_contract_ir::ExprId::new(1),
                }),
            ],
        };
        let duplicated = HirExpressionArena {
            nodes: vec![
                node(ExpressionKind::InputField(input)),
                node(ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: riffdb_contract_ir::ExprId::new(0),
                    right: riffdb_contract_ir::ExprId::new(0),
                }),
                node(ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: riffdb_contract_ir::ExprId::new(0),
                    right: riffdb_contract_ir::ExprId::new(0),
                }),
                node(ExpressionKind::Binary {
                    operator: BinaryOperator::Add,
                    left: riffdb_contract_ir::ExprId::new(1),
                    right: riffdb_contract_ir::ExprId::new(2),
                }),
            ],
        };
        assert_eq!(
            command_expression_fingerprint(&shared, riffdb_contract_ir::ExprId::new(2)),
            command_expression_fingerprint(&duplicated, riffdb_contract_ir::ExprId::new(3))
        );

        let mut repeated = HirExpressionArena {
            nodes: vec![node(ExpressionKind::InputField(input))],
        };
        for index in 1_u32..32 {
            repeated.nodes.push(node(ExpressionKind::Binary {
                operator: BinaryOperator::Add,
                left: riffdb_contract_ir::ExprId::new(index - 1),
                right: riffdb_contract_ir::ExprId::new(index - 1),
            }));
        }
        let fingerprint =
            command_expression_fingerprint(&repeated, riffdb_contract_ir::ExprId::new(31));
        assert!(
            fingerprint.len() < 512,
            "fingerprint must not expand the DAG"
        );
    }
}
