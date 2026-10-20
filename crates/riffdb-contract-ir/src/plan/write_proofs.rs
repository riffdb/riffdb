//! Proof sealing and inspection for assignment-bearing command plans.
use super::*;

impl CommandPlan {
    /// Derives and seals ADR-0262 evidence from explicit requirement associations.
    /// Invalid spellings or associations are rejected; evidence cannot be supplied unchecked.
    pub fn with_write_proofs(
        self,
        associations: &BTreeMap<crate::WriteLocation, u32>,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        self.with_write_proofs_admitted(
            associations,
            schema,
            &mut crate::bundle::source_decode_sink::OrdinaryBundleDecodeAdmissionV1,
        )
    }

    pub(crate) fn source_write_proof_construction_bound(
        &self,
    ) -> Result<(u64, u64), riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
        write_proof_construction_bound(self)
    }

    // Crate-private sealed numerical admission, never an application callback
    // or a public Source opt-out. All ordinary constructors keep the wrapper.
    pub(crate) fn with_write_proofs_admitted<
        A: crate::bundle::source_decode_sink::BundleDecodeAdmissionV1,
    >(
        mut self,
        associations: &BTreeMap<crate::WriteLocation, u32>,
        schema: &SchemaIr,
        admission: &mut A,
    ) -> Result<Self, A::Error> {
        let proofs = {
            let _work = admission.write_proof_construction(&self)?;
            crate::write_evidence::derive_write_proofs(&self, schema, associations)?
        };
        if !proofs.is_empty() {
            self.write_proofs = Some(proofs);
            self.plan_hash = admission.command_hash(&self, schema)?;
        }
        Ok(self)
    }

    /// Per-assignment evidence; None identifies historical dependency classification.
    #[must_use]
    pub fn write_proofs(&self) -> Option<&[crate::WriteProof]> {
        self.write_proofs.as_deref()
    }

    /// Whether this command carries the new per-assignment proof format.
    #[must_use]
    pub const fn requires_ir_v25(&self) -> bool {
        self.write_proofs.is_some()
    }

    /// Whether the historical instruction stream needs the explicit-overwrite encoding.
    /// Proof-bearing commands use V25; this retains the predecessor identity rule.
    #[must_use]
    pub fn requires_ir_v24(&self) -> bool {
        self.instructions.iter().any(|instruction| {
            matches!(
                instruction,
                Instruction::SetField {
                    guard: WriteGuard::Unguarded,
                    ..
                }
            )
        })
    }
}

// Source-only numerical bound over an already owned, validated CommandPlan.
// No identities, expression DTOs or semantic values are constructed here.
fn write_proof_construction_bound(
    plan: &CommandPlan,
) -> Result<(u64, u64), riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_growing_vector_bound, source_tree_layout_bound,
    };
    use crate::{Instruction, WriteEvidence, WriteLocation, WriteProof};
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mul = |left: usize, right: usize| left.checked_mul(right).ok_or(BudgetUnavailable);
    let writes = plan
        .instructions()
        .iter()
        .chain(plan.decisions().iter().flat_map(|decision| {
            decision
                .when_arms()
                .iter()
                .map(crate::CommandDecisionArmV1::action)
                .chain(std::iter::once(decision.else_action()))
                .flat_map(crate::CommandDecisionActionV1::instructions)
        }))
        .filter(|instruction| matches!(instruction, Instruction::SetField { .. }))
        .count();
    let requirements = plan
        .instructions()
        .iter()
        .filter(|instruction| matches!(instruction, Instruction::Require { .. }))
        .count();
    let common_writes = plan
        .instructions()
        .iter()
        .filter(|instruction| matches!(instruction, Instruction::SetField { .. }))
        .count();
    // fences() pushes at most one pair for each actual Require/SetField pair.
    // Its existing checked_len runs immediately after push, so refusal can
    // retain MAX_COMMAND_ITEMS+1 pairs, never the unconstrained product.
    let fences = mul(requirements, common_writes)?.min(crate::MAX_COMMAND_ITEMS + 1);
    let evidence = fences.checked_add(2).ok_or(BudgetUnavailable)?;
    let retained = add(
        source_growing_vector_bound::<WriteProof>(writes)?,
        u64::try_from(writes)
            .map_err(|_| BudgetUnavailable)?
            .checked_mul(source_growing_vector_bound::<WriteEvidence>(evidence)?)
            .ok_or(BudgetUnavailable)?,
    )?;
    // assignments Vec and cached fence Vecs overlap the output proof graph.
    let mut temporary = source_growing_vector_bound::<(WriteLocation, &Instruction)>(writes)?;
    let cache_entries = writes.min(plan.bindings().len());
    temporary = add(
        temporary,
        source_tree_layout_bound::<crate::BindingId, Vec<(u32, u32)>>(cache_entries)?,
    )?;
    temporary = add(
        temporary,
        u64::try_from(cache_entries)
            .map_err(|_| BudgetUnavailable)?
            .checked_mul(source_growing_vector_bound::<(u32, u32)>(fences)?)
            .ok_or(BudgetUnavailable)?,
    )?;
    // Shared actual dependency/category and comparison-pair kernel bounds.
    let nodes = plan.expressions().nodes().len();
    let dependency =
        crate::bundle::source_decode_sink::source_expression_dependency_work_bound(nodes)?;
    let comparison =
        crate::bundle::source_decode_sink::source_expression_comparison_work_bound(nodes, nodes)?;
    temporary = add(temporary, dependency.max(comparison))?;
    Ok((retained, temporary))
}
