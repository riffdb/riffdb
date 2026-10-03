//! Regenerate canonical artifacts with admission in the unchanged registry order.
// req: OQ-040, PRJ-004
use super::*;
use crate::ProviderSchemaArtifactErrorV1;
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

impl ContractBundle {
    /// Revalidate already constructed first-party IR against its artifact registry.
    /// Each temporary artifact is admitted before generation and released before
    /// the next. This does not admit IR parsing or the borrowed bundle itself.
    #[doc(hidden)]
    pub fn validate_provider_schema_artifacts_admitted_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<(), ProviderSchemaArtifactErrorV1> {
        let invalid = || IrValidationError::HashMismatch {
            kind: "generated schema artifact registry",
        };
        if self.schema_artifacts.len()
            != expected_schema_artifact_count(&self.schema, &self.commands, &self.projections)?
        {
            return Err(invalid().into());
        }
        let mut actual = self.schema_artifacts.iter();
        visit_schema_artifact_sources(&self.schema, &self.commands, &self.projections, |source| {
            let expected = source.generate_admitted(&self.schema, budget)?;
            if actual.next() != Some(&**expected) {
                return Err(invalid().into());
            }
            Ok::<_, ProviderSchemaArtifactErrorV1>(())
        })?;
        if actual.next().is_some() {
            return Err(invalid().into());
        }
        Ok(())
    }
}
