//! Coverage proofs for what a command enforces at commit.
//!
//! Both proofs are derived from the aggregates a command writes (ADR-0252). An
//! earlier form derived them from the locality anchor -- the first mutating
//! binding's aggregate -- which made them blind to exactly the omission they
//! exist to catch.

use super::*;

pub(super) fn validate_root_validation_reads(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    reads: &[RootValidationReadPlan],
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    if bindings
        .len()
        .checked_add(reads.len())
        .is_none_or(|count| count > MAX_COMMAND_ITEMS)
    {
        return Err(IrValidationError::LimitExceeded {
            kind: "source-binding and root-validation targets",
            actual: bindings.len().saturating_add(reads.len()),
            maximum: MAX_COMMAND_ITEMS,
        });
    }
    // One command may write more than one aggregate (ADR-0170). Every aggregate
    // it writes owes its own root-validation reads, in the same canonical
    // aggregate order the compiler lowers them in.
    let mut written_aggregates = BTreeSet::new();
    for binding in bindings.iter().filter(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate
                | BindingMode::Create
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
                | BindingMode::Delete
        )
    }) {
        if let Some(aggregate) = schema.aggregate_for_entity(binding.entity_type) {
            written_aggregates.insert(aggregate.id());
        }
    }

    let mut required = Vec::<(BindingId, Vec<ExprId>, EntityTypeId)>::new();
    for aggregate_id in written_aggregates {
        let aggregate =
            schema
                .aggregate(aggregate_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "root-validation aggregate",
                })?;
        if aggregate.invariants().is_empty() {
            continue;
        }
        let root = schema
            .entity(aggregate.root())
            .ok_or(IrValidationError::InvalidReference {
                kind: "root-validation entity",
            })?;
        for binding in bindings.iter().filter(|binding| {
            matches!(
                binding.mode,
                BindingMode::Mutate
                    | BindingMode::Create
                    | BindingMode::InitOrMutate
                    | BindingMode::ObserveOrInitialize
                    | BindingMode::Delete
            ) && aggregate.owns(binding.entity_type)
                && binding.entity_type != root.id()
        }) {
            let prefix = binding
                .key_expressions
                .iter()
                .take(root.primary_key_fields().len())
                .copied()
                .collect::<Vec<_>>();
            if prefix.len() != root.primary_key_fields().len() {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child binding does not contain the complete aggregate-root key prefix",
                });
            }
            let mut supplied_by_source = false;
            for candidate in bindings
                .iter()
                .filter(|candidate| candidate.entity_type == root.id())
            {
                if expression_tuples_equal(arena, &candidate.key_expressions, arena, &prefix)? {
                    supplied_by_source = true;
                    break;
                }
            }
            if supplied_by_source {
                continue;
            }
            let mut grouped = false;
            for (_, expressions, grouped_root) in &required {
                if *grouped_root == root.id()
                    && expression_tuples_equal(arena, expressions, arena, &prefix)?
                {
                    grouped = true;
                    break;
                }
            }
            if !grouped {
                required.push((binding.id, prefix, root.id()));
            }
        }
    }

    if reads.len() != required.len() {
        return Err(IrValidationError::InvalidDependency {
            reason: "root-validation reads do not exactly cover mutable child root requirements",
        });
    }
    for (read, (expected_source, expected_expressions, expected_root)) in
        reads.iter().zip(&required)
    {
        let root = schema
            .entity(*expected_root)
            .ok_or(IrValidationError::InvalidReference {
                kind: "root-validation entity",
            })?;
        let source = bindings.get(read.source_binding.get() as usize).ok_or(
            IrValidationError::InvalidReference {
                kind: "root-validation source binding",
            },
        )?;
        if !matches!(
            source.mode,
            BindingMode::Mutate
                | BindingMode::Create
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
                | BindingMode::Delete
        ) || source.entity_type == root.id()
            || read.entity_type != root.id()
            || read.key_schema != *root.primary_key()
            || read.key_expressions.len() != root.primary_key_fields().len()
            || read
                .accessed_fields
                .iter()
                .any(|field| root.record().field(*field).is_none())
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "root-validation read does not identify the exact aggregate root",
            });
        }
        for (expression, component) in read
            .key_expressions
            .iter()
            .zip(read.key_schema.components())
        {
            if arena
                .get(*expression)
                .is_none_or(|node| node.result_type() != component.value_type())
                || (!arena.dependencies(*expression)?.is_input_computable()
                    && !source
                        .indexed_selection()
                        .is_some_and(|_| source.key_expressions().contains(expression)))
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "root-validation key is not input/constant-computable with the declared type",
                });
            }
        }
        if read.source_binding != *expected_source
            || !expression_tuples_equal(arena, &read.key_expressions, arena, expected_expressions)?
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "root-validation reads are not canonically ordered by their lowest source binding or use a different checked root-key derivation",
            });
        }
    }
    Ok(())
}

pub(super) fn validate_commit_checks(
    arena: &ExpressionArena,
    checks: &[CommitCheckPlan],
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    for check in checks {
        if arena
            .get(check.predicate)
            .is_none_or(|node| node.result_type().tag() != ValueTypeTag::Bool)
            || check
                .source_bindings
                .iter()
                .any(|binding| binding.get() as usize >= bindings.len())
            || check
                .root_validation_reads
                .iter()
                .any(|read| read.get() as usize >= root_validation_reads.len())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "commit check",
            });
        }
        let dependencies = arena.dependencies(check.predicate)?;
        if dependencies
            .bindings()
            .iter()
            .any(|binding| check.source_bindings.binary_search(binding).is_err())
            || dependencies
                .root_validation_reads()
                .iter()
                .any(|read| check.root_validation_reads.binary_search(read).is_err())
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "commit-check predicate references a record outside its application subjects",
            });
        }
    }

    let postimage_bindings = bindings
        .iter()
        .filter(|binding| {
            matches!(
                binding.mode,
                BindingMode::Mutate
                    | BindingMode::Create
                    | BindingMode::InitOrMutate
                    | BindingMode::ObserveOrInitialize
            )
        })
        .collect::<Vec<_>>();
    let mut expected = Vec::new();
    for binding in &postimage_bindings {
        let entity =
            schema
                .entity(binding.entity_type)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "commit-check entity",
                })?;
        for invariant in entity.invariants() {
            expected.push((
                invariant.id(),
                vec![binding.id],
                Vec::new(),
                invariant,
                binding.entity_type,
            ));
        }
    }

    let mut written_aggregates = BTreeSet::new();
    for binding in bindings.iter().filter(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate
                | BindingMode::Create
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
                | BindingMode::Delete
        )
    }) {
        if let Some(aggregate) = schema.aggregate_for_entity(binding.entity_type) {
            written_aggregates.insert(aggregate.id());
        }
    }
    for aggregate_id in written_aggregates {
        let aggregate =
            schema
                .aggregate(aggregate_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "commit-check aggregate",
                })?;
        if aggregate.invariants().is_empty() {
            continue;
        }
        let root = schema
            .entity(aggregate.root())
            .ok_or(IrValidationError::InvalidReference {
                kind: "aggregate root entity",
            })?;
        let mut root_subjects = BTreeSet::new();
        for binding in bindings.iter().filter(|binding| {
            matches!(
                binding.mode,
                BindingMode::Mutate
                    | BindingMode::Create
                    | BindingMode::InitOrMutate
                    | BindingMode::ObserveOrInitialize
                    | BindingMode::Delete
            )
        }) {
            // Only this aggregate's own writes reach its root.
            if !aggregate.owns(binding.entity_type) {
                continue;
            }
            if binding.mode == BindingMode::Delete && binding.entity_type == aggregate.root() {
                continue;
            }
            if binding.entity_type == aggregate.root() {
                root_subjects.insert((Some(binding.id), None));
                continue;
            }
            let prefix = binding
                .key_expressions
                .iter()
                .take(root.primary_key_fields().len())
                .copied()
                .collect::<Vec<_>>();
            let mut matching = Vec::new();
            for candidate in bindings
                .iter()
                .filter(|candidate| candidate.entity_type == aggregate.root())
            {
                if expression_tuples_equal(arena, &candidate.key_expressions, arena, &prefix)? {
                    matching.push(candidate.id());
                }
            }
            if matching.len() > 1 {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child mutation has multiple exact source aggregate-root bindings",
                });
            }
            if let Some(binding) = matching.first() {
                root_subjects.insert((Some(*binding), None));
                continue;
            }
            let mut matching_reads = Vec::new();
            for read in root_validation_reads
                .iter()
                .filter(|read| read.entity_type == aggregate.root())
            {
                if expression_tuples_equal(arena, &read.key_expressions, arena, &prefix)? {
                    matching_reads.push(read.id());
                }
            }
            if matching_reads.len() != 1 {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child mutation requires one exact aggregate-root validation read",
                });
            }
            root_subjects.insert((None, Some(matching_reads[0])));
        }
        for (binding_id, read_id) in root_subjects {
            for invariant in aggregate.invariants() {
                expected.push((
                    invariant.id(),
                    binding_id.into_iter().collect::<Vec<_>>(),
                    read_id.into_iter().collect::<Vec<_>>(),
                    invariant,
                    aggregate.root(),
                ));
            }
        }
    }
    expected.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    if checks.len() != expected.len() {
        return Err(IrValidationError::InvalidDependency {
            reason: "commit checks do not exactly cover applicable invariants",
        });
    }
    for (check, (invariant_id, invariant_bindings, invariant_reads, invariant, expected_entity)) in
        checks.iter().zip(expected)
    {
        let exact_template = if let Some(binding) = invariant_bindings.first() {
            matches_invariant_template(
                invariant.expressions(),
                invariant.predicate(),
                arena,
                check.predicate,
                *binding,
                expected_entity,
            )?
        } else if let Some(read) = invariant_reads.first() {
            matches_root_invariant_template(
                invariant.expressions(),
                invariant.predicate(),
                arena,
                check.predicate,
                *read,
                expected_entity,
            )?
        } else {
            false
        };
        if check.invariant_id != invariant_id
            || check.source_bindings != invariant_bindings
            || check.root_validation_reads != invariant_reads
            || !exact_template
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "commit check is not the exact instantiated invariant template",
            });
        }
    }
    Ok(())
}
