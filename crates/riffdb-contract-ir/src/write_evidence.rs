//! Per-assignment structural evidence. Business relevance is an author claim.

mod capacity;
pub(crate) mod codec;

use std::collections::BTreeMap;

use riffdb_types::{CanonicalValue, FieldId};

use crate::{
    BinaryOperator, BindingId, BindingMode, CommandPlan, ExprId, ExpressionArena, ExpressionKind,
    Instruction, IrValidationError, MAX_COMMAND_ITEMS, SchemaIr, ValueTypeTag, WriteGuard,
};

/// Stable location of one instruction, including conditional occurrences.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WriteLocation {
    /// Index in the ordinary instruction stream.
    Instruction(u32),
    /// Decision index, arm index (the final index is else), and instruction index.
    Decision {
        /// Decision index.
        decision: u32,
        /// Arm index.
        arm: u32,
        /// Instruction index within the arm.
        instruction: u32,
    },
}

/// What the compiler structurally establishes for an assignment.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WriteEvidence {
    /// Creation has no previous value.
    Initialization,
    /// A same-field checked arithmetic delta; delta freshness is not established.
    StateDerived,
    /// Explicit author association to a dominating requirement; relevance is not inferred.
    BusinessCheck {
        /// Dense requirement index.
        requirement: u32,
    },
    /// Exact root version check and advancement; other writers must maintain the protocol.
    CallerVersionFence {
        /// Dense requirement index.
        requirement: u32,
        /// Index of the unconditional advancement instruction.
        advancement: u32,
    },
    /// Explicit replacement without other qualifying evidence.
    Overwrite,
}

/// All evidence for one assignment occurrence, in canonical order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteProof {
    pub(crate) location: WriteLocation,
    pub(crate) evidence: Vec<WriteEvidence>,
}
impl WriteProof {
    /// Assignment occurrence in the executable plan.
    #[must_use]
    pub const fn location(&self) -> WriteLocation {
        self.location
    }
    /// Canonically ordered evidence, preserving all applicable kinds.
    #[must_use]
    pub fn evidence(&self) -> &[WriteEvidence] {
        &self.evidence
    }
}

/// Recognizes only the closed same-field arithmetic delta shape of ADR-0262.
/// `is_key` distinguishes key fields from stored state in bound-field expressions.
pub fn is_state_derived_write(
    arena: &ExpressionArena,
    value: ExprId,
    binding: BindingId,
    field: FieldId,
    is_key: impl Fn(BindingId, FieldId) -> bool,
) -> Result<bool, IrValidationError> {
    if !arena.get(value).is_some_and(|node| {
        matches!(
            node.result_type().tag(),
            ValueTypeTag::I64 | ValueTypeTag::U64 | ValueTypeTag::Decimal | ValueTypeTag::Money
        )
    }) {
        return Ok(false);
    }
    let Some(ExpressionKind::Binary {
        operator,
        left,
        right,
    }) = arena.get(value).map(|n| n.kind())
    else {
        return Ok(false);
    };
    if !matches!(operator, BinaryOperator::Add | BinaryOperator::Subtract) {
        return Ok(false);
    }
    let pair = |prior: ExprId, delta: ExprId| -> Result<bool, IrValidationError> {
        if !matches!(arena.get(prior).map(|n| n.kind()), Some(ExpressionKind::BoundField {binding: b, field: f}) if *b == binding && *f == field)
        {
            return Ok(false);
        }
        let deps = arena.dependencies(delta)?;
        Ok(deps.complete_bindings().is_empty()
            && deps.root_validation_reads().is_empty()
            && deps.schema_fields().is_empty()
            && deps.source_event_fields().is_empty()
            && deps.bound_fields().iter().all(|(b, f)| is_key(*b, *f)))
    };
    Ok(pair(*left, *right)? || (*operator == BinaryOperator::Add && pair(*right, *left)?))
}

pub(crate) fn assignments(plan: &CommandPlan) -> Vec<(WriteLocation, &Instruction)> {
    let mut writes = Vec::new();
    for (i, instruction) in plan.instructions().iter().enumerate() {
        if matches!(instruction, Instruction::SetField { .. }) {
            writes.push((WriteLocation::Instruction(i as u32), instruction));
        }
    }
    for (d, decision) in plan.decisions().iter().enumerate() {
        for (a, action) in decision
            .when_arms()
            .iter()
            .map(|a| a.action())
            .chain(std::iter::once(decision.else_action()))
            .enumerate()
        {
            for (i, instruction) in action.instructions().iter().enumerate() {
                if matches!(instruction, Instruction::SetField { .. }) {
                    writes.push((
                        WriteLocation::Decision {
                            decision: d as u32,
                            arm: a as u32,
                            instruction: i as u32,
                        },
                        instruction,
                    ));
                }
            }
        }
    }
    writes
}

fn invalid() -> IrValidationError {
    IrValidationError::InvalidDependency {
        reason: "invalid per-write evidence or business association",
    }
}

fn repeated_instruction(plan: &CommandPlan, index: u32) -> bool {
    plan.collection_expansion().is_some_and(|e| {
        index >= e.first_instruction()
            && (index - e.first_instruction()) < e.instruction_count() as u32
    })
}

fn repeated_binding(plan: &CommandPlan, binding: BindingId) -> bool {
    plan.collection_expansion().is_some_and(|e| {
        binding >= e.first_binding()
            && (binding.get() - e.first_binding().get()) < e.binding_count() as u32
    })
}

fn stored_read(
    plan: &CommandPlan,
    schema: &SchemaIr,
    predicate: ExprId,
    binding: BindingId,
) -> Result<bool, IrValidationError> {
    let bound = plan
        .bindings()
        .get(binding.get() as usize)
        .ok_or_else(invalid)?;
    let entity = schema.entity(bound.entity_type()).ok_or_else(invalid)?;
    Ok(plan
        .expressions()
        .dependencies(predicate)?
        .bound_fields()
        .iter()
        .any(|(b, f)| *b == binding && !entity.primary_key_fields().contains(f)))
}

pub(crate) fn derive_write_proofs(
    plan: &CommandPlan,
    schema: &SchemaIr,
    associations: &BTreeMap<WriteLocation, u32>,
) -> Result<Vec<WriteProof>, IrValidationError> {
    let writes = assignments(plan);
    crate::error::checked_len("write proofs", writes.len(), MAX_COMMAND_ITEMS)?;
    if associations
        .keys()
        .any(|location| !writes.iter().any(|(l, _)| l == location))
    {
        return Err(invalid());
    }
    let mut fence_cache = BTreeMap::new();
    let mut proofs = Vec::new();
    let mut evidence_count = 0;
    for (location, instruction) in writes {
        let Instruction::SetField {
            binding,
            field,
            value,
            guard,
        } = instruction
        else {
            return Err(invalid());
        };
        let bound = plan
            .bindings()
            .get(binding.get() as usize)
            .ok_or_else(invalid)?;
        let mut evidence = Vec::new();
        let association = associations.get(&location);
        if bound.mode() == BindingMode::Create {
            if association.is_some() {
                return Err(invalid());
            }
            evidence.push(WriteEvidence::Initialization);
        } else {
            if is_state_derived_write(plan.expressions(), *value, *binding, *field, |b, f| {
                plan.bindings()
                    .get(b.get() as usize)
                    .and_then(|b| schema.entity(b.entity_type()))
                    .is_some_and(|e| e.primary_key_fields().contains(&f))
            })? {
                evidence.push(WriteEvidence::StateDerived);
            }
            if let Some(index) = association {
                if *guard == WriteGuard::Unguarded {
                    return Err(invalid());
                }
                // Decision requirements execute in the common suffix, after arm writes.
                let WriteLocation::Instruction(write_index) = location else {
                    return Err(invalid());
                };
                let (position, predicate) = plan
                    .instructions()
                    .iter()
                    .enumerate()
                    .find_map(|(i, ins)| match ins {
                        Instruction::Require {
                            requirement_index,
                            predicate,
                            ..
                        } if requirement_index == index => Some((i as u32, *predicate)),
                        _ => None,
                    })
                    .ok_or_else(invalid)?;
                if position >= write_index
                    || repeated_instruction(plan, position)
                        != repeated_instruction(plan, write_index)
                    || !stored_read(plan, schema, predicate, *binding)?
                {
                    return Err(invalid());
                }
                evidence.push(WriteEvidence::BusinessCheck {
                    requirement: *index,
                });
            }
            if !fence_cache.contains_key(binding) {
                fence_cache.insert(*binding, fences(plan, schema, *binding)?);
            }
            for &(requirement, advancement) in &fence_cache[binding] {
                evidence.push(WriteEvidence::CallerVersionFence {
                    requirement,
                    advancement,
                });
            }
        }
        if evidence.is_empty() {
            if *guard != WriteGuard::Unguarded {
                return Err(invalid());
            }
            evidence.push(WriteEvidence::Overwrite);
        } else if *guard != WriteGuard::Guarded {
            return Err(invalid());
        }
        evidence.sort_unstable();
        evidence.dedup();
        evidence_count += evidence.len();
        crate::error::checked_len("write evidence", evidence_count, MAX_COMMAND_ITEMS)?;
        proofs.push(WriteProof { location, evidence });
    }
    Ok(proofs)
}

fn fences(
    plan: &CommandPlan,
    schema: &SchemaIr,
    target: BindingId,
) -> Result<Vec<(u32, u32)>, IrValidationError> {
    let arena = plan.expressions();
    let target = plan
        .bindings()
        .get(target.get() as usize)
        .ok_or_else(invalid)?;
    let mut fences = Vec::new();
    for (position, instruction) in plan.instructions().iter().enumerate() {
        let Instruction::Require {
            requirement_index,
            predicate,
            ..
        } = instruction
        else {
            continue;
        };
        if repeated_instruction(plan, position as u32) {
            continue;
        }
        let Some(ExpressionKind::Binary {
            operator: BinaryOperator::Equal,
            left,
            right,
        }) = arena.get(*predicate).map(|n| n.kind())
        else {
            continue;
        };
        let pair = |stored, input| {
            let s = arena.get(stored)?;
            let i = arena.get(input)?;
            match (s.kind(), i.kind()) {
                (ExpressionKind::BoundField { binding, field }, ExpressionKind::InputField(_))
                    if matches!(s.result_type().tag(), ValueTypeTag::I64 | ValueTypeTag::U64)
                        && s.result_type() == i.result_type() =>
                {
                    Some((*binding, *field))
                }
                _ => None,
            }
        };
        let Some((binding, field)) = pair(*left, *right).or_else(|| pair(*right, *left)) else {
            continue;
        };
        let root = plan
            .bindings()
            .get(binding.get() as usize)
            .ok_or_else(invalid)?;
        if root.mode() != BindingMode::Mutate || repeated_binding(plan, binding) {
            continue;
        }
        let Some(aggregate) = schema
            .aggregates()
            .iter()
            .find(|a| a.root() == root.entity_type())
        else {
            continue;
        };
        if target.entity_type() != aggregate.root()
            && !aggregate.children().contains(&target.entity_type())
        {
            continue;
        }
        if target.key_expressions().len() < root.key_expressions().len() {
            continue;
        }
        let mut same = true;
        for (left, right) in root.key_expressions().iter().zip(target.key_expressions()) {
            same &= crate::plan::expression_trees_equal(arena, *left, arena, *right)?;
        }
        if !same {
            continue;
        }
        for (i, instruction) in plan.instructions().iter().enumerate() {
            let Instruction::SetField {
                binding: b,
                field: f,
                value,
                ..
            } = instruction
            else {
                continue;
            };
            if *b != binding || *f != field || repeated_instruction(plan, i as u32) {
                continue;
            }
            let Some(ExpressionKind::Binary {
                operator: BinaryOperator::Add,
                left,
                right,
            }) = arena.get(*value).map(|n| n.kind())
            else {
                continue;
            };
            let advance = |prior, one| {
                matches!(arena.get(prior).map(|n| n.kind()), Some(ExpressionKind::BoundField {binding: b, field: f}) if *b == binding && *f == field)
                    && matches!(
                        arena.get(one).map(|n| n.kind()),
                        Some(ExpressionKind::Constant(
                            CanonicalValue::I64(1) | CanonicalValue::U64(1)
                        ))
                    )
            };
            if advance(*left, *right) || advance(*right, *left) {
                fences.push((*requirement_index, i as u32));
                crate::error::checked_len("revision fences", fences.len(), MAX_COMMAND_ITEMS)?;
            }
        }
    }
    Ok(fences)
}
