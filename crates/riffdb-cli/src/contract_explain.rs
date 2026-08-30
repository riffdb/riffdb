//! Local, read-only explanation of one contract's aggregate structure.
//!
//! The aggregate model is the most consequential rule in the language and the
//! one authors currently discover through rejection: `RDB-C017` refuses a
//! command that writes two aggregate roots, `RDB-C008` refuses a child whose
//! key is not prefixed by its root's, and `RDB-C020` refuses a cascade whose
//! discovered graph exceeds its fixed bound. Each of those is a consequence of
//! structure the compiler already knows and never shows.
//!
//! This renders that structure: which aggregate owns each entity, the conflict
//! key writers contend on, how each entity proves its deletions, and what the
//! cascade arithmetic actually comes to.

use riffdb_contract_ir::{
    BinaryOperator, BindingId, ContractBundle, DeletePolicyModeV1, ExpressionArena, ExpressionKind,
    Instruction, ValueTypeTag,
};
use riffdb_types::{EntityTypeId, FieldId};

/// SPEC DEL-004 bounds one cascaded delete's whole discovered graph.
const MAX_CASCADE_GRAPH: u32 = 256;

/// Renders one compiled contract's aggregate structure as human text.
#[must_use]
pub(crate) fn render_contract_explain(bundle: &ContractBundle) -> String {
    let schema = bundle.schema();
    let name = |id: EntityTypeId| -> String {
        schema.entity(id).map_or_else(
            || format!("entity#{id:?}"),
            |entity| entity.name().to_owned(),
        )
    };

    let mut output = String::new();
    output.push_str(&format!(
        "contract {} version {}\n",
        bundle.lineage().as_str(),
        bundle.contract_version().get()
    ));

    output.push_str("\naggregates\n");
    for aggregate in schema.aggregates() {
        let children = aggregate.children();
        output.push_str(&format!(
            "  {} — root {}\n",
            aggregate.name(),
            name(aggregate.root())
        ));
        if children.is_empty() {
            output.push_str("    children: none\n");
        } else {
            output.push_str(&format!(
                "    children: {}\n",
                children
                    .iter()
                    .map(|id| name(*id))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        // Writers contend at this granularity, so it is the number that decides
        // whether two concurrent commands serialise against each other.
        output.push_str(&format!(
            "    conflict key: {} component(s); partition: {} component(s)\n",
            aggregate.keys().conflict_schema().components().len(),
            aggregate.keys().partition_schema().components().len()
        ));
    }

    output.push_str("\ndeletion proofs\n");
    for entity in schema.entities() {
        let Some(policy) = schema.delete_policy(entity.id()) else {
            output.push_str(&format!("  {}: none declared\n", entity.name()));
            continue;
        };
        match policy.mode() {
            DeletePolicyModeV1::NoInbound => {
                output.push_str(&format!(
                    "  {}: no_inbound — nothing references it\n",
                    entity.name()
                ));
            }
            DeletePolicyModeV1::Restrict { source_entity, .. } => {
                output.push_str(&format!(
                    "  {}: restrict — refuses while {} rows reference it\n",
                    entity.name(),
                    name(source_entity)
                ));
            }
            DeletePolicyModeV1::Cascade { relationships } => {
                let total: u32 = relationships
                    .iter()
                    .map(|relationship| u32::from(relationship.maximum()))
                    .sum();
                let per_root = total.saturating_add(1);
                let roots = MAX_CASCADE_GRAPH / per_root.max(1);
                output.push_str(&format!(
                    "  {}: cascade over {} relationship(s)\n",
                    entity.name(),
                    relationships.len()
                ));
                for relationship in relationships {
                    output.push_str(&format!(
                        "      {} up to {}\n",
                        name(relationship.source_entity()),
                        relationship.maximum()
                    ));
                }
                // DEL-004: roots * (1 + sum(maxima)) <= 256. Authors meet this
                // as a rejected list bound; showing the arithmetic makes the
                // available bound obvious instead of a search.
                output.push_str(&format!(
                    "      deleting one root discovers up to {per_root} rows, \
                     so one bulk delete admits at most {roots} root(s)\n"
                ));
            }
        }
    }

    output.push_str("\ncommand conflict ownership\n");
    for command in bundle.commands() {
        let owners = command
            .locality()
            .conflict_keys()
            .iter()
            .filter_map(|key| match key.schema().purpose() {
                riffdb_contract_ir::KeyPurpose::Conflict(owner) => Some(owner),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let named = owners
            .iter()
            .map(|owner| {
                schema
                    .aggregates()
                    .iter()
                    .find(|aggregate| aggregate.id() == *owner)
                    .map_or_else(
                        || format!("{owner:?}"),
                        |aggregate| aggregate.name().to_owned(),
                    )
            })
            .collect::<Vec<_>>();
        // Writers contend on this set, so a command owning two aggregates is
        // the thing an author most needs to see about it (ADR-0170).
        output.push_str(&format!(
            "  {}: {}{}\n",
            command.name(),
            named.join(", "),
            if command.locality().spans_aggregates() {
                " (spans aggregates; one atomic write over one partition route)"
            } else {
                ""
            }
        ));
    }

    output.push_str(&unguarded_subtractions(bundle));
    output
}

/// Reports unsigned subtractions no `require` in the same command guards.
///
/// Deleting a `require x.stock >= input.quantity` still compiles: the checked
/// subtraction that follows becomes a runtime execution failure instead of the
/// declared outcome the caller can render. That is safe but it is not the
/// compile-time guarantee the surrounding language leads authors to expect, so
/// it is reported here rather than left to be discovered in production.
///
/// This is advisory and deliberately conservative: it looks only for a
/// `require` mentioning the same bound field, so a guard expressed another way
/// reads as guarded rather than producing a false alarm.
fn unguarded_subtractions(bundle: &ContractBundle) -> String {
    let mut findings = Vec::new();
    for command in bundle.commands() {
        let arena = command.expressions();
        let guarded = guarded_fields(command.instructions(), arena);
        for instruction in command.instructions() {
            let Instruction::SetField {
                binding,
                field,
                value,
            } = instruction
            else {
                continue;
            };
            if !subtracts_from_self(arena, *value, *binding, *field) {
                continue;
            }
            if guarded.contains(&(*binding, *field)) {
                continue;
            }
            findings.push(command.name().to_owned());
            break;
        }
    }
    if findings.is_empty() {
        return String::new();
    }
    let mut output = String::from("\nunguarded unsigned subtraction\n");
    for command in findings {
        output.push_str(&format!(
            "  {command}: subtracts from an unsigned field with no `require` naming it; \
             underflow fails the transaction at runtime instead of returning a declared outcome\n"
        ));
    }
    output
}

/// Bound fields any `require` in this command mentions.
fn guarded_fields(
    instructions: &[Instruction],
    arena: &ExpressionArena,
) -> Vec<(BindingId, FieldId)> {
    let mut guarded = Vec::new();
    for instruction in instructions {
        let Instruction::Require { predicate, .. } = instruction else {
            continue;
        };
        collect_bound_fields(arena, *predicate, &mut guarded);
    }
    guarded
}

fn collect_bound_fields(
    arena: &ExpressionArena,
    root: riffdb_contract_ir::ExprId,
    found: &mut Vec<(BindingId, FieldId)>,
) {
    let Some(node) = arena.get(root) else { return };
    match node.kind() {
        ExpressionKind::BoundField { binding, field } => found.push((*binding, *field)),
        ExpressionKind::Binary { left, right, .. } => {
            collect_bound_fields(arena, *left, found);
            collect_bound_fields(arena, *right, found);
        }
        _ => {}
    }
}

/// Whether `value` is `binding.field - <anything>` over an unsigned field.
fn subtracts_from_self(
    arena: &ExpressionArena,
    value: riffdb_contract_ir::ExprId,
    binding: BindingId,
    field: FieldId,
) -> bool {
    let Some(node) = arena.get(value) else {
        return false;
    };
    if node.result_type().tag() != ValueTypeTag::U64 {
        return false;
    }
    let ExpressionKind::Binary {
        operator: BinaryOperator::Subtract,
        left,
        ..
    } = node.kind()
    else {
        return false;
    };
    matches!(
        arena.get(*left).map(riffdb_contract_ir::TypedExpression::kind),
        Some(ExpressionKind::BoundField {
            binding: left_binding,
            field: left_field,
        }) if *left_binding == binding && *left_field == field
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn explain(source: &str) -> String {
        let bundle =
            riffdb_contract_compiler::compile_contract_source(source).expect("contract compiles");
        render_contract_explain(&bundle)
    }

    const CASCADE_CONTRACT: &str = r"
contract ExplainCascade version 1 {
  entity Root {
    key (tenant_id: uuid, root_id: uuid)
    field label: string<32>
    delete_policy cascade {
      relationship Leaf.leaf_parent using Leaf.by_parent maximum 32
    }
  }
  entity Leaf {
    key (tenant_id: uuid, root_id: uuid, leaf_id: uuid)
    field quantity: u64
    index by_parent (tenant_id, root_id, leaf_id)
    reference leaf_parent (tenant_id, root_id) -> Root(tenant_id, root_id)
    delete_policy no_inbound
  }
  aggregate RootData {
    root Root
    child Leaf
    partition_by tenant_id
    conflict_key (tenant_id, root_id)
  }
}
";

    #[test]
    fn it_reports_which_aggregate_owns_each_entity() {
        let explained = explain(CASCADE_CONTRACT);
        assert!(explained.contains("RootData — root Root"), "{explained}");
        assert!(explained.contains("children: Leaf"), "{explained}");
    }

    #[test]
    fn it_does_the_cascade_arithmetic_that_del_004_bounds() {
        // One relationship at 32 discovers 33 rows per root, and 256 / 33 is 7.
        // Authors currently meet this bound only as a rejected list maximum.
        let explained = explain(CASCADE_CONTRACT);
        assert!(
            explained
                .contains("discovers up to 33 rows, so one bulk delete admits at most 7 root(s)"),
            "{explained}"
        );
    }

    const SUBTRACTION_CONTRACT: &str = r"
contract ExplainSubtract version 1 {
  entity Stock {
    key (tenant_id: uuid, stock_id: uuid)
    field on_hand: u64
    delete_policy no_inbound
  }
  aggregate StockData {
    root Stock
    partition_by tenant_id
    conflict_key (tenant_id, stock_id)
  }
  command Take {
    input request_id: uuid
    input tenant_id: uuid
    input stock_id: uuid
    input quantity: u64
    idempotency_key request_id
    mutate Stock(tenant_id, stock_id) as stock else StockMissing {}
    GUARD
    set stock.on_hand = stock.on_hand - quantity
    return Taken {}
  }
}
";

    #[test]
    fn a_guarded_subtraction_is_not_reported() {
        let guarded = SUBTRACTION_CONTRACT.replace(
            "    GUARD",
            "    require enough: stock.on_hand >= quantity else Insufficient {}",
        );
        assert!(
            !explain(&guarded).contains("unguarded unsigned subtraction"),
            "a declared guard must not be reported"
        );
    }

    #[test]
    fn an_unguarded_unsigned_subtraction_is_reported() {
        // Deleting the guard still compiles; underflow becomes a runtime
        // execution failure rather than a declared outcome, which is the one
        // place the compile-time safety story degrades quietly.
        let unguarded = SUBTRACTION_CONTRACT.replace("    GUARD\n", "");
        let explained = explain(&unguarded);
        assert!(
            explained.contains("unguarded unsigned subtraction"),
            "{explained}"
        );
        assert!(explained.contains("Take:"), "{explained}");
    }

    #[test]
    fn it_names_the_source_that_a_restrict_policy_refuses_against() {
        let source = CASCADE_CONTRACT.replace(
            "    delete_policy cascade {\n      relationship Leaf.leaf_parent using Leaf.by_parent maximum 32\n    }",
            "    delete_policy restrict Leaf.by_parent",
        );
        let explained = explain(&source);
        assert!(
            explained.contains("Root: restrict — refuses while Leaf rows reference it"),
            "{explained}"
        );
    }
}
