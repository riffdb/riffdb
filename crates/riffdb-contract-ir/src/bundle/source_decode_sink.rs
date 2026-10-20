//! Private numerical sink for the sole bundle decoder's Source construction.
//! This is an unintegrated draft, not a second parser or a Source capability.
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{
        BudgetUnavailable, ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
    },
};

/// Counts emitted by the existing grammar before a family's construction.
/// No IDs, schema, plan or decoded semantic representation is retained here.
#[derive(Clone, Copy, Default)]
pub(super) struct SourceFamilyMeasureV1 {
    retained: u64,
    workspace: u64,
}
impl SourceFamilyMeasureV1 {
    pub(super) fn bounded(retained: u64, workspace: u64) -> Self {
        Self {
            retained,
            workspace,
        }
    }
}

/// Source construction never has an ordinary/unchecked/custom mode. The
/// public source entry creates this before invoking the shared decoder kernel.
/// Its original caller's ledger is borrowed; no budget/worker factory exists.
pub(super) struct SourceDecodeSinkV1<'source> {
    budget: &'source ProjectionSourceWorkingBudgetV1,
    #[cfg(test)]
    binding_sort_observation: Option<(usize, u64, u64, Option<u64>)>,
    retained: ProjectionSourceWorkReservationV1,
}

/// Actual constructor scratch dies separately from retained data allowance.
/// It contains no decoded graph/engine/callback or authority.
pub(crate) struct SourceConstructorWorkV1 {
    _work: Option<ProjectionSourceWorkReservationV1>,
}

impl<'source> SourceDecodeSinkV1<'source> {
    pub(super) fn new(
        budget: &'source ProjectionSourceWorkingBudgetV1,
        owner_controls: u64,
    ) -> Result<Self, BudgetUnavailable> {
        let controls = u64::try_from(std::mem::size_of::<Self>())
            .map_err(|_| BudgetUnavailable)?
            .checked_add(owner_controls)
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)?;
        // This admission occurs before Reader/family metadata or semantic
        // construction. The caller additionally preprices its actual outer
        // success/failure owners in owner_controls using their compiled types.
        let retained = budget.reserve(controls)?;
        Ok(Self {
            budget,
            #[cfg(test)]
            binding_sort_observation: None,
            retained,
        })
    }

    #[cfg(test)]
    pub(super) fn binding_sort_observation_for_test(
        &self,
    ) -> Option<(usize, u64, u64, Option<u64>)> {
        self.binding_sort_observation
    }

    // An empty owning clone/collection constructs no heap allocation. Its
    // inline controls were already admitted by this sink's original owner.
    // Preserve the budget's zero-reservation refusal instead of fabricating
    // a nonzero numerical allowance for a genuinely zero heap quote.
    fn admit_retained(&mut self, bytes: u64) -> Result<(), BudgetUnavailable> {
        if bytes == 0 {
            return Ok(());
        }
        let extra = self.budget.reserve(bytes)?;
        self.retained.absorb(extra)
    }

    pub(super) fn admit_family(
        &mut self,
        measured: SourceFamilyMeasureV1,
    ) -> Result<SourceConstructorWorkV1, BudgetUnavailable> {
        self.admit_retained(measured.retained)?;
        // Refusal here leaves the original, already admitted data allowance
        // intact. The caller has not constructed this family yet.
        let work = if measured.workspace == 0 {
            None
        } else {
            Some(self.budget.reserve(measured.workspace)?)
        };
        Ok(SourceConstructorWorkV1 { _work: work })
    }

    // Sole existing ownership census, prepaid by this sink before any walk.
    // This is only a conservative numerical clone bound, never an attachment
    // report, identity proof or authority. The preview dies before new admission.
    fn complete_clone_capacity<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        value: &T,
    ) -> Result<u64, BudgetUnavailable> {
        let scope = self.retained.inspection_scope()?;
        let preview = riffdb_types::OwnedCapacityInspectionV1::preview(value, scope)
            .map_err(|_| BudgetUnavailable)?;
        let mut bytes = preview.unshared_bytes();
        for shared in preview.shared_allocations() {
            bytes = bytes.checked_add(shared.bytes()).ok_or(BudgetUnavailable)?;
        }
        Ok(bytes)
    }

    /// Prior family bounds already paid construction and census scratch.
    /// Settlement attaches the same family's actual shared-lifecycle ownership;
    /// a cloned typed identity cannot outlive its originating numerical charge.
    pub(super) fn settle<T: riffdb_types::InspectOwnedCapacityV1>(
        self,
        value: T,
    ) -> Result<riffdb_types::ProjectionSourceWorkAllocationV1<T>, BudgetUnavailable> {
        self.retained.settle_shared_value(value)
    }

    /// Failure owners retain their bounded error FIRST and this charge LAST.
    /// No partial decoded value or native/caller owner escapes through this seam.
    pub(super) fn into_retained(self) -> ProjectionSourceWorkReservationV1 {
        self.retained
    }
}

// Only the two owning implementations below may admit the shared grammar.
// An ordinary caller never evaluates Source-only allocation arithmetic.
mod sealed {
    pub(crate) trait Sealed {}
}
pub(crate) trait BundleDecodeAdmissionV1: sealed::Sealed {
    type Error: From<super::IrValidationError>;
    type FamilyConstructorWork;
    fn workflow_catalog_work(
        &mut self,
        workflows: &[crate::WorkflowSchema],
        schema: &crate::SchemaIr,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn ledger_validation_work(
        &mut self,
        ledger: &super::LineageLedgerV1,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn artifact_validation_work(
        &mut self,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    #[allow(clippy::too_many_arguments)]
    fn plan_root_hash(
        &mut self,
        schema: &crate::SchemaIr,
        workflows: &crate::WorkflowCatalog,
        row_policies: &crate::RowPolicyCatalogV1,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
        version: u32,
    ) -> Result<super::ContractPlanRootHash, Self::Error>;
    fn canonical_bundle(&mut self, bundle: &super::ContractBundle) -> Result<Vec<u8>, Self::Error>;
    fn projection_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn projection_hash(
        &mut self,
        plan: &crate::ProjectionPlan,
        schema: &crate::SchemaIr,
    ) -> Result<riffdb_types::ProjectionPlanHash, Self::Error>;
    fn schema_registry_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn compatibility_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        has_parent: bool,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn mcp_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn row_policy_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn workflow_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
        version: u32,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn schema_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn ledger_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn command_schema_closure_work(
        &mut self,
        bindings: &[crate::BindingPlan],
        reads: &[crate::RootValidationReadPlan],
        common: &[crate::Instruction],
        decisions: &[crate::CommandDecisionPlanV1],
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn entity_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn event_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn aggregate_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn write_association_work(
        &mut self,
        proofs: &[crate::WriteProof],
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn temporary_clone_work<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        value: &T,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn retained_clone<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error>;
    fn record_schema_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    fn expression_arena_work(
        &mut self,
        reader: &super::Reader<'_>,
        version: u32,
    ) -> Result<Self::FamilyConstructorWork, Self::Error>;
    type BindingSortWork;
    fn binding_initializer_sort_work(
        &mut self,
        count: usize,
    ) -> Result<Self::BindingSortWork, Self::Error>;
    type CommandConstructorWork;
    #[allow(clippy::too_many_arguments)]
    fn command_constructor_work(
        &mut self,
        arena: &crate::ExpressionArena,
        bindings: &[crate::BindingPlan],
        outcomes: &[crate::OutcomeSchema],
        reads: &[crate::RootValidationReadPlan],
        locality: &crate::LocalityPlan,
        checks: &[crate::CommitCheckPlan],
        common: &[crate::Instruction],
        decisions: &[crate::CommandDecisionPlanV1],
        schema: &crate::SchemaIr,
    ) -> Result<Self::CommandConstructorWork, Self::Error>;
    type WriteProofWork;
    fn write_proof_construction(
        &mut self,
        plan: &super::CommandPlan,
    ) -> Result<Self::WriteProofWork, Self::Error>;
    fn command_hash(
        &mut self,
        plan: &super::CommandPlan,
        schema: &super::SchemaIr,
    ) -> Result<super::PlanHash, Self::Error>;
    fn generated_schema_hash(
        &mut self,
        source: crate::ProviderSchemaArtifactSourceV1<'_>,
        schema: &super::SchemaIr,
    ) -> Result<super::SchemaHash, Self::Error>;
    fn retained_text(&mut self, bytes: usize) -> Result<(), Self::Error>;
    fn retained_growing_vector<T>(&mut self, count: usize) -> Result<(), Self::Error>;
    fn retained_vector<T>(&mut self, count: usize) -> Result<(), Self::Error>;
    fn retained_box<T>(&mut self) -> Result<(), Self::Error>;
    fn string(
        &mut self,
        reader: &mut super::Reader<'_>,
        maximum: usize,
    ) -> Result<String, Self::Error>;
}

pub(crate) struct OrdinaryBundleDecodeAdmissionV1;
impl sealed::Sealed for OrdinaryBundleDecodeAdmissionV1 {}
impl BundleDecodeAdmissionV1 for OrdinaryBundleDecodeAdmissionV1 {
    type Error = super::IrValidationError;
    type FamilyConstructorWork = ();
    fn workflow_catalog_work(
        &mut self,
        _workflows: &[crate::WorkflowSchema],
        _schema: &crate::SchemaIr,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn ledger_validation_work(
        &mut self,
        _ledger: &super::LineageLedgerV1,
        _schema: &crate::SchemaIr,
        _commands: &[crate::CommandPlan],
        _projections: &[crate::ProjectionPlan],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn artifact_validation_work(
        &mut self,
        _schema: &crate::SchemaIr,
        _commands: &[crate::CommandPlan],
        _projections: &[crate::ProjectionPlan],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn plan_root_hash(
        &mut self,
        schema: &crate::SchemaIr,
        workflows: &crate::WorkflowCatalog,
        row_policies: &crate::RowPolicyCatalogV1,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
        version: u32,
    ) -> Result<super::ContractPlanRootHash, Self::Error> {
        super::compute_plan_root_hash_versioned(
            schema,
            workflows,
            row_policies,
            commands,
            projections,
            version,
        )
    }
    fn canonical_bundle(&mut self, bundle: &super::ContractBundle) -> Result<Vec<u8>, Self::Error> {
        super::encode_bundle(bundle)
    }
    fn projection_section_work(
        &mut self,
        _reader: &super::Reader<'_>,
        _schema: &crate::SchemaIr,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn projection_hash(
        &mut self,
        plan: &crate::ProjectionPlan,
        schema: &crate::SchemaIr,
    ) -> Result<riffdb_types::ProjectionPlanHash, Self::Error> {
        super::compute_projection_plan_hash(plan, schema)
    }
    fn schema_registry_section_work(
        &mut self,
        _reader: &super::Reader<'_>,
        _schema: &crate::SchemaIr,
        _commands: &[crate::CommandPlan],
        _projections: &[crate::ProjectionPlan],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn compatibility_section_work(
        &mut self,
        _reader: &super::Reader<'_>,
        _has_parent: bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn mcp_section_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn row_policy_section_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn workflow_section_work(
        &mut self,
        _reader: &super::Reader<'_>,
        _schema: &crate::SchemaIr,
        _version: u32,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn schema_section_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn ledger_section_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn command_schema_closure_work(
        &mut self,
        _bindings: &[crate::BindingPlan],
        _reads: &[crate::RootValidationReadPlan],
        _common: &[crate::Instruction],
        _decisions: &[crate::CommandDecisionPlanV1],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        Ok(())
    }
    fn entity_fragment_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn event_fragment_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn aggregate_fragment_work(&mut self, _reader: &super::Reader<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn write_association_work(&mut self, _proofs: &[crate::WriteProof]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn temporary_clone_work<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retained_clone<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn record_schema_work(
        &mut self,
        _reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        Ok(())
    }
    fn expression_arena_work(
        &mut self,
        _reader: &super::Reader<'_>,
        _version: u32,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        Ok(())
    }
    type BindingSortWork = ();
    fn binding_initializer_sort_work(
        &mut self,
        _count: usize,
    ) -> Result<Self::BindingSortWork, Self::Error> {
        // Preserve ordinary constructor/error/ordering; no Source arithmetic.
        Ok(())
    }
    type CommandConstructorWork = ();
    #[allow(clippy::too_many_arguments)]
    fn command_constructor_work(
        &mut self,
        _arena: &crate::ExpressionArena,
        _bindings: &[crate::BindingPlan],
        _outcomes: &[crate::OutcomeSchema],
        _reads: &[crate::RootValidationReadPlan],
        _locality: &crate::LocalityPlan,
        _checks: &[crate::CommitCheckPlan],
        _common: &[crate::Instruction],
        _decisions: &[crate::CommandDecisionPlanV1],
        _schema: &crate::SchemaIr,
    ) -> Result<Self::CommandConstructorWork, Self::Error> {
        Ok(())
    }
    type WriteProofWork = ();
    fn write_proof_construction(&mut self, _plan: &super::CommandPlan) -> Result<(), Self::Error> {
        Ok(())
    }
    fn command_hash(
        &mut self,
        plan: &super::CommandPlan,
        schema: &super::SchemaIr,
    ) -> Result<super::PlanHash, Self::Error> {
        super::compute_command_plan_hash(plan, schema)
    }
    fn generated_schema_hash(
        &mut self,
        source: crate::ProviderSchemaArtifactSourceV1<'_>,
        schema: &super::SchemaIr,
    ) -> Result<super::SchemaHash, Self::Error> {
        // Same ordinary parts/from_node path, no sizing/admission arithmetic.
        source
            .generate_ordinary(schema)
            .map(|artifact| artifact.hash())
    }
    fn retained_text(&mut self, _bytes: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retained_growing_vector<T>(&mut self, _count: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retained_vector<T>(&mut self, _count: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retained_box<T>(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn string(
        &mut self,
        reader: &mut super::Reader<'_>,
        maximum: usize,
    ) -> Result<String, Self::Error> {
        reader.string(maximum)
    }
}
impl sealed::Sealed for SourceDecodeSinkV1<'_> {}
impl BundleDecodeAdmissionV1 for SourceDecodeSinkV1<'_> {
    type Error = super::ProviderBundleEncodingErrorV1;
    type FamilyConstructorWork = SourceConstructorWorkV1;
    fn workflow_catalog_work(
        &mut self,
        workflows: &[crate::WorkflowSchema],
        schema: &crate::SchemaIr,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut largest_variants = 0u64;
        for workflow in workflows {
            let variants = schema
                .enumeration(workflow.state_enum())
                .map_or(0, |value| value.variants().len());
            largest_variants = largest_variants
                .max(source_tree_layout_bound::<riffdb_types::EnumVariantId, ()>(
                    variants,
                )?);
        }
        let workspace =
            source_tree_layout_bound::<riffdb_types::EntityTypeId, ()>(workflows.len())?
                .checked_add(largest_variants)
                .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn ledger_validation_work(
        &mut self,
        ledger: &super::LineageLedgerV1,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let bound = super::ledger_validation_admission::LedgerValidationBound::inspect_parts(
            ledger,
            schema,
            commands,
            projections,
        )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, bound.maximum()?))
            .map_err(Into::into)
    }
    fn artifact_validation_work(
        &mut self,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let workspace =
            super::schema_registry_decode_admission::SchemaRegistrySectionBound::generation_work(
                schema,
                commands,
                projections,
            )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn plan_root_hash(
        &mut self,
        schema: &crate::SchemaIr,
        workflows: &crate::WorkflowCatalog,
        row_policies: &crate::RowPolicyCatalogV1,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
        version: u32,
    ) -> Result<super::ContractPlanRootHash, Self::Error> {
        super::encoding_admission::source_root_hash(
            schema,
            workflows,
            row_policies,
            commands,
            projections,
            version,
            self.budget,
        )
    }
    fn canonical_bundle(&mut self, bundle: &super::ContractBundle) -> Result<Vec<u8>, Self::Error> {
        // First size all encoder-owned JSON/closure work under prepaid scalar
        // sizing. The canonical length pass itself regenerates JSON, so its
        // semantic workspace must already be held before that first pass.
        let sizing = self.budget.reserve(32 * 1024)?;
        let maximum = super::plan_hash_admission::bundle_encoding_work_bound(bundle)?;
        let _semantic_work = if maximum == 0 {
            None
        } else {
            Some(self.budget.reserve(maximum)?)
        };
        drop(sizing);
        let mut counter = super::Writer::counting(super::MAX_BUNDLE_BYTES);
        super::encode_bundle_to(&mut counter, bundle)?;
        let length = counter.encoded_len();
        self.retained_vector::<u8>(length)?;
        let mut writer = super::Writer::with_capacity(length, length);
        super::encode_bundle_to(&mut writer, bundle)?;
        if writer.encoded_len() != length {
            return Err(super::IrValidationError::HashMismatch {
                kind: "canonical bundle encoding length",
            }
            .into());
        }
        Ok(writer.finish())
    }
    fn projection_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::projection_decode_admission::ProjectionSectionBound::inspect_source_from(
                &mut measured,
                schema,
            )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn projection_hash(
        &mut self,
        plan: &crate::ProjectionPlan,
        schema: &crate::SchemaIr,
    ) -> Result<riffdb_types::ProjectionPlanHash, Self::Error> {
        super::plan_hash_admission::source_projection_hash(plan, schema, self.budget)
    }
    fn schema_registry_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
        commands: &[crate::CommandPlan],
        projections: &[crate::ProjectionPlan],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::schema_registry_decode_admission::SchemaRegistrySectionBound::inspect_from(
                &mut measured,
                schema,
                commands,
                projections,
            )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.retained,
            bound.generation,
        ))
        .map_err(Into::into)
    }
    fn compatibility_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        has_parent: bool,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound = super::compatibility_admission::CompatibilitySectionBound::inspect_from(
            &mut measured,
            has_parent,
        )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn mcp_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound = super::mcp_admission::McpSectionBound::inspect_from(&mut measured)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound
                .heap
                .checked_add(bound.shared)
                .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn row_policy_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::row_policy_admission::RowPolicySectionBound::inspect_from(&mut measured)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors()?,
        ))
        .map_err(Into::into)
    }
    fn workflow_section_work(
        &mut self,
        reader: &super::Reader<'_>,
        schema: &crate::SchemaIr,
        version: u32,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound = super::workflow_admission::WorkflowSectionBound::inspect_from(
            &mut measured,
            schema,
            version,
        )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn schema_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::schema_decode_admission::SchemaSectionBound::inspect_from(&mut measured)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn ledger_section_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound = super::ledger_admission::LedgerSectionBound::inspect_from(&mut measured)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(Into::into)
    }
    fn command_schema_closure_work(
        &mut self,
        bindings: &[crate::BindingPlan],
        reads: &[crate::RootValidationReadPlan],
        common: &[crate::Instruction],
        decisions: &[crate::CommandDecisionPlanV1],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let unavailable = super::ProviderBundleEncodingErrorV1::Unavailable;
        let entities = bindings
            .len()
            .checked_add(reads.len())
            .ok_or_else(|| unavailable.clone())?;
        let mut events = common.len();
        for decision in decisions {
            for action in decision
                .when_arms()
                .iter()
                .map(crate::CommandDecisionArmV1::action)
                .chain(std::iter::once(decision.else_action()))
            {
                events = events
                    .checked_add(action.instructions().len())
                    .ok_or_else(|| unavailable.clone())?;
            }
        }
        let workspace = source_growing_vector_bound::<riffdb_types::EntityTypeId>(entities)?
            .checked_add(source_growing_vector_bound::<riffdb_types::EventTypeId>(
                events,
            )?)
            .ok_or(unavailable)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn entity_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::schema_decode_admission::SchemaSectionBound::inspect_entity_from(&mut measured)?;
        let workspace = bound
            .heap
            .checked_add(bound.constructors)
            .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn event_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound =
            super::schema_decode_admission::SchemaSectionBound::inspect_event_from(&mut measured)?;
        let workspace = bound
            .heap
            .checked_add(bound.constructors)
            .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn aggregate_fragment_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut measured = reader.fork();
        let bound = super::schema_decode_admission::SchemaSectionBound::inspect_aggregate_from(
            &mut measured,
        )?;
        let workspace = bound
            .heap
            .checked_add(bound.constructors)
            .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn write_association_work(
        &mut self,
        proofs: &[crate::WriteProof],
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let mut count = 0usize;
        for proof in proofs {
            for evidence in proof.evidence() {
                if matches!(evidence, crate::WriteEvidence::BusinessCheck { .. }) {
                    count = count
                        .checked_add(1)
                        .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
                }
            }
        }
        let workspace = source_tree_layout_bound::<crate::WriteLocation, u32>(count)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))
            .map_err(Into::into)
    }
    fn temporary_clone_work<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        value: &T,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        let bytes = self.complete_clone_capacity(value)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(0, bytes))
            .map_err(Into::into)
    }
    fn retained_clone<T: riffdb_types::InspectOwnedCapacityV1>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        let bytes = self.complete_clone_capacity(value)?;
        self.admit_retained(bytes)?;
        Ok(())
    }
    fn record_schema_work(
        &mut self,
        reader: &super::Reader<'_>,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        // Same fixed borrowed-walk scratch as the existing structural decoder,
        // admitted on the ORIGINAL Source ledger before the fork/walk begins.
        let _sizing = self
            .budget
            .reserve(32 * 1024)
            .map_err(|_| super::ProviderBundleEncodingErrorV1::Unavailable)?;
        let mut measured = reader.fork();
        let bound =
            super::schema_decode_admission::SchemaSectionBound::inspect_record_from(&mut measured)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(|_| super::ProviderBundleEncodingErrorV1::Unavailable)
    }
    fn expression_arena_work(
        &mut self,
        reader: &super::Reader<'_>,
        version: u32,
    ) -> Result<Self::FamilyConstructorWork, Self::Error> {
        // Canonical constants/type recursion/native constructor/depth arrays
        // use the sole existing term admission walk and semantic decoder.
        let _sizing = self
            .budget
            .reserve(32 * 1024)
            .map_err(|_| super::ProviderBundleEncodingErrorV1::Unavailable)?;
        let mut measured = reader.fork();
        let bound = super::term_admission::expression_bound(&mut measured, version)?;
        self.admit_family(SourceFamilyMeasureV1::bounded(
            bound.heap,
            bound.constructors,
        ))
        .map_err(|_| super::ProviderBundleEncodingErrorV1::Unavailable)
    }
    type BindingSortWork = SourceConstructorWorkV1;
    fn binding_initializer_sort_work(
        &mut self,
        count: usize,
    ) -> Result<Self::BindingSortWork, Self::Error> {
        // Pinned Rust1.98.1 stable::driftsort_main uses one Vec buffer of
        // max(n - n/2, min(n, 8_000_000 / size_of<T>), 48) elements. Its
        // capacity is <=max(n,48); Vec allocation uses the sole existing
        // typed allocation bound. The original initializer stays retained.
        // AlignedStorage<T,4096> is simultaneously alive even on the heap
        // branch. These exact source/profile pins are required by the
        // encompassing Source constructor, not ordinary decoding.
        let buffer =
            super::workflow_admission::allocation::<super::FieldExpression>(count.max(48))?;
        let workspace = buffer
            .checked_add(4096)
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<Vec<super::FieldExpression>>() as u64)
            })
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<SourceConstructorWorkV1>() as u64)
            })
            .ok_or(super::ProviderBundleEncodingErrorV1::Unavailable)?;
        #[cfg(test)]
        {
            let before = self.budget.observation()?.reserved_bytes;
            self.binding_sort_observation = Some((count, workspace, before, None));
        }
        let work = self.admit_family(SourceFamilyMeasureV1::bounded(0, workspace))?;
        #[cfg(test)]
        {
            if let Some(observation) = &mut self.binding_sort_observation {
                observation.3 = Some(self.budget.observation()?.reserved_bytes);
            }
        }
        Ok(work)
    }
    type CommandConstructorWork = SourceConstructorWorkV1;
    #[allow(clippy::too_many_arguments)]
    fn command_constructor_work(
        &mut self,
        arena: &crate::ExpressionArena,
        bindings: &[crate::BindingPlan],
        outcomes: &[crate::OutcomeSchema],
        reads: &[crate::RootValidationReadPlan],
        locality: &crate::LocalityPlan,
        checks: &[crate::CommitCheckPlan],
        common: &[crate::Instruction],
        decisions: &[crate::CommandDecisionPlanV1],
        schema: &crate::SchemaIr,
    ) -> Result<Self::CommandConstructorWork, Self::Error> {
        let (retained, workspace) = crate::CommandPlan::source_constructor_heap_bound(
            arena, bindings, outcomes, reads, locality, checks, common, decisions, schema,
        )?;
        self.admit_family(SourceFamilyMeasureV1::bounded(retained, workspace))
            .map_err(Into::into)
    }
    type WriteProofWork = SourceConstructorWorkV1;
    fn write_proof_construction(
        &mut self,
        plan: &super::CommandPlan,
    ) -> Result<Self::WriteProofWork, Self::Error> {
        let (retained, workspace) = plan.source_write_proof_construction_bound()?;
        self.admit_family(SourceFamilyMeasureV1::bounded(retained, workspace))
            .map_err(Into::into)
    }
    fn command_hash(
        &mut self,
        plan: &super::CommandPlan,
        schema: &super::SchemaIr,
    ) -> Result<super::PlanHash, Self::Error> {
        super::plan_hash_admission::source_command_hash(plan, schema, self.budget)
    }
    fn generated_schema_hash(
        &mut self,
        source: crate::ProviderSchemaArtifactSourceV1<'_>,
        schema: &super::SchemaIr,
    ) -> Result<super::SchemaHash, Self::Error> {
        source
            .generate_source_hash_admitted(schema, self.budget)
            .map_err(|error| match error {
                crate::ProviderSchemaArtifactErrorV1::Unavailable => {
                    super::ProviderBundleEncodingErrorV1::Unavailable
                }
                crate::ProviderSchemaArtifactErrorV1::Invalid(error) => {
                    super::ProviderBundleEncodingErrorV1::Invalid(error)
                }
            })
    }
    fn retained_text(&mut self, bytes: usize) -> Result<(), Self::Error> {
        let bytes = riffdb_types::OwnedCapacityTextV1::retained_allocation_upper_bound(bytes)
            .map_err(|_| super::ProviderBundleEncodingErrorV1::Unavailable)?;
        self.admit_retained(bytes)?;
        Ok(())
    }
    fn retained_growing_vector<T>(&mut self, count: usize) -> Result<(), Self::Error> {
        let bytes = source_growing_vector_bound::<T>(count)?;
        self.admit_retained(bytes)?;
        Ok(())
    }
    fn retained_vector<T>(&mut self, count: usize) -> Result<(), Self::Error> {
        let bytes = super::workflow_admission::allocation::<T>(count)?;
        self.admit_retained(bytes)?;
        Ok(())
    }
    fn retained_box<T>(&mut self) -> Result<(), Self::Error> {
        self.retained_vector::<T>(1)
    }
    fn string(
        &mut self,
        reader: &mut super::Reader<'_>,
        maximum: usize,
    ) -> Result<String, Self::Error> {
        // Identical framing/UTF8 checks precede admission and to_owned.
        let value = reader.string_ref(maximum)?;
        self.retained_vector::<u8>(value.len())?;
        Ok(value.to_owned())
    }
}

// Exact-capacity clone/with_capacity buffers use the same owning allocation
// bound as the existing family measurement. No new formula or admission grant.
pub(crate) fn source_fixed_vector_bound<T>(count: usize) -> Result<u64, BudgetUnavailable> {
    super::workflow_admission::allocation::<T>(count)
}
// ExpressionArena::dependencies creates one needed Vec and these ten exact
// category B-trees. One actual arena node can insert at most once per category.
// This is shared by command-constructor and write-proof workspace planning.
pub(crate) fn source_expression_dependency_work_bound(
    nodes: usize,
) -> Result<u64, BudgetUnavailable> {
    use crate::{BindingId, RootValidationReadId};
    use riffdb_types::{EntityTypeId, FieldId};
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut bytes = source_growing_vector_bound::<bool>(nodes)?;
    bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(nodes)?)?; // input
    bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(nodes)?)?; // service
    bytes = add(bytes, source_tree_layout_bound::<BindingId, ()>(nodes)?)?;
    bytes = add(bytes, source_tree_layout_bound::<BindingId, ()>(nodes)?)?; // complete
    bytes = add(
        bytes,
        source_tree_layout_bound::<(BindingId, FieldId), ()>(nodes)?,
    )?;
    bytes = add(
        bytes,
        source_tree_layout_bound::<(EntityTypeId, FieldId), ()>(nodes)?,
    )?;
    bytes = add(
        bytes,
        source_tree_layout_bound::<RootValidationReadId, ()>(nodes)?,
    )?;
    bytes = add(
        bytes,
        source_tree_layout_bound::<(RootValidationReadId, FieldId), ()>(nodes)?,
    )?;
    bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(nodes)?)?; // event
    add(bytes, source_tree_layout_bound::<FieldId, ()>(nodes)?) // collection
}

// All four existing comparison kernels insert, check MAX_EXPRESSION_NODES,
// then resolve the pair and push at most two children. One out-of-range pair
// may be retained before the original lookup refuses a malformed root/child.
// The valid pair product plus one covers that terminal error without trusting
// the not-yet-validated command's references or weakening the original cap.
pub(crate) fn source_expression_comparison_work_bound(
    left_nodes: usize,
    right_nodes: usize,
) -> Result<u64, BudgetUnavailable> {
    use crate::ExprId;
    let pairs = left_nodes
        .checked_mul(right_nodes)
        .and_then(|n| n.checked_add(1))
        .ok_or(BudgetUnavailable)?
        .min(crate::MAX_EXPRESSION_NODES + 1);
    let pending = pairs
        .checked_mul(2)
        .and_then(|n| n.checked_add(1))
        .ok_or(BudgetUnavailable)?;
    source_tree_layout_bound::<(ExprId, ExprId), ()>(pairs)?
        .checked_add(source_growing_vector_bound::<(ExprId, ExprId)>(pending)?)
        .ok_or(BudgetUnavailable)
}

// One owning layout formula: no copied B-tree node geometry in plan helpers.
pub(crate) fn source_tree_layout_bound<K, V>(count: usize) -> Result<u64, BudgetUnavailable> {
    super::plan_hash_admission::tree_layout_bound::<K, V>(count)
}
// Qualified Rust/standard-library profile is an encompassing Source constructor
// prerequisite. For a growable Vec, capacity is at most twice the required
// count or the pinned RawVec minimum (8 one-byte slots, 4 medium slots, 1 large).
// Existing allocation() then covers both simultaneous old/new allocation
// capacity plus metadata; this is a container-growth derivation, not an IR
// expansion multiplier. Empty Vec::new makes no allocation.
pub(crate) fn source_growing_vector_bound<T>(count: usize) -> Result<u64, BudgetUnavailable> {
    if count == 0 {
        return Ok(0);
    }
    let minimum = if std::mem::size_of::<T>() == 1 {
        8
    } else if std::mem::size_of::<T>() <= 1024 {
        4
    } else {
        1
    };
    let slots = count.checked_mul(2).ok_or(BudgetUnavailable)?.max(minimum);
    super::workflow_admission::allocation::<T>(slots)
}
