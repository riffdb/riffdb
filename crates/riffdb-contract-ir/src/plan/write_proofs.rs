//! Proof sealing and inspection for assignment-bearing command plans.
use super::*;

impl CommandPlan {
    /// Derives and seals ADR-0262 evidence from explicit requirement associations.
    /// Invalid spellings or associations are rejected; evidence cannot be supplied unchecked.
    pub fn with_write_proofs(
        mut self,
        associations: &BTreeMap<crate::WriteLocation, u32>,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        let proofs = crate::write_evidence::derive_write_proofs(&self, schema, associations)?;
        if !proofs.is_empty() {
            self.write_proofs = Some(proofs);
            self.plan_hash = crate::bundle::compute_command_plan_hash(&self, schema)?;
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
