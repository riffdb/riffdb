//! Borrowed canonical sizing and admitted bundle output; parsing remains separate.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use std::{
    mem::{align_of, size_of},
    sync::Arc,
};

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderBundleEncodingErrorV1 {
    Unavailable,
    Invalid(IrValidationError),
}
impl From<BudgetUnavailable> for ProviderBundleEncodingErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::Unavailable
    }
}
impl From<IrValidationError> for ProviderBundleEncodingErrorV1 {
    fn from(error: IrValidationError) -> Self {
        Self::Invalid(error)
    }
}
impl std::fmt::Display for ProviderBundleEncodingErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("provider bundle encoding unavailable"),
            Self::Invalid(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ProviderBundleEncodingErrorV1 {}

fn output_work(bytes: usize) -> Result<u64, BudgetUnavailable> {
    // One exact Vec allocation, the ownership inspector's doubled capacity and
    // allocation metadata, the eventual immutable wrapper and census scratch.
    // Canonical constants append directly into the preallocated output buffer.
    bytes
        .checked_mul(2)
        .and_then(|bytes| {
            bytes.checked_add(
                2 * size_of::<ProviderAllocation<Vec<u8>>>()
                    + 2 * align_of::<ProviderAllocation<Vec<u8>>>()
                    + 4096,
            )
        })
        .and_then(|bytes| u64::try_from(bytes).ok())
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .ok_or(BudgetUnavailable)
}

impl ContractBundle {
    /// Re-encode validated first-party IR after admitting its exact output buffer.
    /// This does not admit parsing, validation, or ownership of the borrowed IR.
    #[doc(hidden)]
    pub fn encode_provider_admitted_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Vec<u8>>>, ProviderBundleEncodingErrorV1> {
        let sizing = budget.reserve(32 * 1024)?;
        let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
        encode_bundle_to(&mut counter, self)?;
        let expected = counter.encoded_len();
        let work = budget.reserve(output_work(expected)?)?;
        drop(sizing);
        let mut writer = Writer::with_capacity(expected, expected);
        encode_bundle_to(&mut writer, self)?;
        if writer.encoded_len() != expected {
            return Err(IrValidationError::HashMismatch {
                kind: "canonical bundle encoding length",
            }
            .into());
        }
        work.settle_owned_value(writer.finish()).map_err(Into::into)
    }
}

pub(super) fn encode_root_to(
    writer: &mut Writer,
    structural_hash: SchemaHash,
    workflows: &WorkflowCatalog,
    row_policies: &RowPolicyCatalogV1,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
    ir_version: u32,
) -> Result<(), IrValidationError> {
    writer.raw(ROOT_PLAN_MAGIC)?;
    writer.u32(ir_version)?;
    writer.raw(structural_hash.as_bytes())?;
    if ir_version >= EXECUTABLE_IR_VERSION_V2 {
        encode_workflows(writer, workflows, ir_version)?;
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V4 {
        // Keep the standalone row-policy limit before appending its identical
        // framed encoding, without a second row-policy output allocation.
        let mut rows = Writer::counting(crate::MAX_ROW_POLICY_CATALOG_BYTES_V1);
        encode_row_policy_catalog(&mut rows, row_policies)?;
        writer.u32(rows.encoded_len() as u32)?;
        encode_row_policy_catalog(writer, row_policies)?;
    }
    writer.u32(commands.len() as u32)?;
    for command in commands {
        writer.u32(command.command_id().get())?;
        writer.raw(command.plan_hash().as_bytes())?;
    }
    writer.u32(projections.len() as u32)?;
    for projection in projections {
        writer.u32(projection.projection_id().get())?;
        writer.raw(projection.plan_hash().as_bytes())?;
    }
    Ok(())
}

struct RootWorkspace {
    schema: usize,
    preimage: usize,
    root: usize,
}
impl RootWorkspace {
    fn sizing(bundle: &ContractBundle) -> Result<Self, IrValidationError> {
        let mut schema = Writer::counting(MAX_BUNDLE_BYTES);
        encode_schema(&mut schema, &bundle.schema)?;
        let mut preimage = Writer::counting(MAX_BUNDLE_BYTES);
        preimage.raw(SCHEMA_IR_MAGIC)?;
        preimage.u32(EXECUTABLE_IR_VERSION_V1)?;
        // checked_append proves the same structural-preimage ceiling without
        // needing a slice backed by an allocated structural encoding.
        preimage.count_bytes(schema.encoded_len())?;
        let mut root = Writer::counting(MAX_BUNDLE_BYTES);
        encode_root_to(
            &mut root,
            SchemaHash::from_bytes([0; 32]),
            &bundle.workflows,
            &bundle.row_policies,
            &bundle.commands,
            &bundle.projections,
            bundle.ir_version,
        )?;
        Ok(Self {
            schema: schema.encoded_len(),
            preimage: preimage.encoded_len(),
            root: root.encoded_len(),
        })
    }

    fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        // The structural bytes outlive both phases. The structural preimage
        // is destroyed before the root buffer is allocated, so only the larger
        // phase overlaps. Each buffer is allocated once at its exact length.
        self.schema
            .checked_add(self.preimage.max(self.root))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(4096))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)
    }
}

impl ContractBundle {
    /// Recompute the existing plan-root hash with admitted encoding workspace.
    /// This does not admit IR parsing or the command/projection hash validators.
    #[doc(hidden)]
    pub fn compute_provider_plan_root_hash_admitted_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<ContractPlanRootHash, ProviderBundleEncodingErrorV1> {
        let sizing = budget.reserve(32 * 1024)?;
        let sizes = RootWorkspace::sizing(self)?;
        let _work = budget.reserve(sizes.maximum()?)?;
        drop(sizing);
        let mut schema = Writer::with_capacity(sizes.schema, sizes.schema);
        encode_schema(&mut schema, &self.schema)?;
        let schema_bytes = schema.finish();
        let structural_hash = {
            let mut preimage = Writer::with_capacity(sizes.preimage, sizes.preimage);
            preimage.raw(SCHEMA_IR_MAGIC)?;
            preimage.u32(EXECUTABLE_IR_VERSION_V1)?;
            preimage.raw(&schema_bytes)?;
            hash_schema(&preimage.finish())
        };
        let mut root = Writer::with_capacity(sizes.root, sizes.root);
        encode_root_to(
            &mut root,
            structural_hash,
            &self.workflows,
            &self.row_policies,
            &self.commands,
            &self.projections,
            self.ir_version,
        )?;
        Ok(hash_contract_plan_root(&root.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provider_bundle_encoding_workspace_refuses_overflow() {
        assert_eq!(output_work(usize::MAX), Err(BudgetUnavailable));
    }
    #[test]
    fn provider_root_workspace_covers_native_buffers_and_preserves_legacy_preimage() {
        for bytes in [
            include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
            include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
            include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        ] {
            let bundle = ContractBundle::decode(bytes).unwrap();
            let budget = ProviderBudget::new();
            let sizing = budget.reserve(32 * 1024).unwrap();
            let sizes = RootWorkspace::sizing(&bundle).unwrap();
            let maximum = sizes.maximum().unwrap();
            let work = budget.reserve(maximum).unwrap();
            drop(sizing);
            let mut schema = Writer::with_capacity(sizes.schema, sizes.schema);
            encode_schema(&mut schema, &bundle.schema).unwrap();
            let schema_bytes = schema.finish();
            let structural_hash = {
                let mut preimage = Writer::with_capacity(sizes.preimage, sizes.preimage);
                preimage.raw(SCHEMA_IR_MAGIC).unwrap();
                preimage.u32(EXECUTABLE_IR_VERSION_V1).unwrap();
                preimage.raw(&schema_bytes).unwrap();
                let preimage = preimage.finish();
                assert!(
                    2 * (schema_bytes.capacity() + preimage.capacity()) as u64 + 4096 <= maximum
                );
                hash_schema(&preimage)
            };
            let mut root = Writer::with_capacity(sizes.root, sizes.root);
            encode_root_to(
                &mut root,
                structural_hash,
                &bundle.workflows,
                &bundle.row_policies,
                &bundle.commands,
                &bundle.projections,
                bundle.ir_version,
            )
            .unwrap();
            let root_bytes = root.finish();
            assert!(2 * (schema_bytes.capacity() + root_bytes.capacity()) as u64 + 4096 <= maximum);
            assert_eq!(
                hash_contract_plan_root(&root_bytes),
                bundle.plan_root_hash()
            );
            drop(schema_bytes);
            drop(root_bytes);
            drop(work);
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            // Independent original framing, including the standalone row-policy
            // encoding: sharing the new traversal cannot hide a changed preimage.
            let mut legacy = Writer::new(MAX_BUNDLE_BYTES);
            legacy.raw(ROOT_PLAN_MAGIC).unwrap();
            legacy.u32(bundle.ir_version).unwrap();
            legacy.raw(structural_hash.as_bytes()).unwrap();
            if bundle.ir_version >= EXECUTABLE_IR_VERSION_V2 {
                encode_workflows(&mut legacy, &bundle.workflows, bundle.ir_version).unwrap();
            }
            if bundle.ir_version >= EXECUTABLE_IR_VERSION_V4 {
                legacy
                    .bytes(&bundle.row_policies.canonical_bytes().unwrap())
                    .unwrap();
            }
            legacy.u32(bundle.commands.len() as u32).unwrap();
            for command in &bundle.commands {
                legacy.u32(command.command_id().get()).unwrap();
                legacy.raw(command.plan_hash().as_bytes()).unwrap();
            }
            legacy.u32(bundle.projections.len() as u32).unwrap();
            for projection in &bundle.projections {
                legacy.u32(projection.projection_id().get()).unwrap();
                legacy.raw(projection.plan_hash().as_bytes()).unwrap();
            }
            assert_eq!(
                hash_contract_plan_root(&legacy.finish()),
                bundle
                    .compute_provider_plan_root_hash_admitted_v1(&budget)
                    .unwrap()
            );
        }
        assert_eq!(
            RootWorkspace {
                schema: usize::MAX,
                preimage: 1,
                root: 0
            }
            .maximum(),
            Err(BudgetUnavailable)
        );
    }
}
