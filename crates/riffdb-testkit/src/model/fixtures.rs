//! Checked compiler fixtures exposed as storage-neutral test values.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::CheckedProjectionSchema;
use riffdb_types::ProjectionId;

const BUDGET_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/examples/budget.riff"
));

/// Compiles the canonical budget contract and returns its checked projection schema.
///
/// Backend tests consume only the returned storage wrapper; production storage
/// crates therefore do not acquire a contract-IR dependency to build fixtures.
#[must_use]
pub fn budget_projection_schema() -> CheckedProjectionSchema {
    let bundle =
        compile_contract_source(BUDGET_SOURCE).expect("canonical budget contract compiles");
    let bound = bundle
        .bound_projection_group_schema(ProjectionId::first())
        .expect("canonical budget contract has projection one");
    CheckedProjectionSchema::new(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_budget_projection_fixture_is_checked() {
        let schema = budget_projection_schema();
        assert_eq!(schema.identity().projection_id(), ProjectionId::first());
        assert_eq!(schema.group_component_count(), 3);
    }
}
