//! Private, inert checking for one pre-generation logical V2 batch draft.

use riffdb_types::{
    ColumnarDefinitionSemanticsHashV1, FieldId, MAX_AGGREGATE_STATE_BYTES_V1,
    MAX_APPLICATION_QUERY_PAGE_ROWS, MAX_APPLICATION_QUERY_RESULT_BYTES,
    ProjectionProviderDescriptorHash, QueryPlanHash,
};

use super::{CLOSED_BATCH_WIDTHS, ColumnarBatchWidth};
use crate::segment_v2::{MAX_LANE_BYTES, MAX_SEGMENT_V2_COLUMNS, SegmentV2LogicalType};

const MAX_PROGRAM_SELECTION_BYTES: usize = CLOSED_BATCH_WIDTHS[CLOSED_BATCH_WIDTHS.len() - 1] / 8;
const MAX_PROGRAM_OPTIONAL_STATE_BYTES: usize =
    MAX_SEGMENT_V2_COLUMNS * MAX_PROGRAM_SELECTION_BYTES * 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckedLogicalProgramIdentity {
    definition: ColumnarDefinitionSemanticsHashV1,
    provider: ProjectionProviderDescriptorHash,
    plan: QueryPlanHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProgramLane {
    field: FieldId,
    logical: SegmentV2LogicalType,
    optional: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PhaseLaneSets {
    // These are disjoint first-materialization owners. A later phase may use an
    // already materialized lane without claiming it a second time.
    eligibility: Vec<usize>,
    policy: Vec<usize>,
    predicate: Vec<usize>,
    aggregate: Vec<usize>,
    order: Vec<usize>,
    output: Vec<usize>,
}

impl PhaseLaneSets {
    fn in_execution_order(&self) -> [&[usize]; 6] {
        [
            &self.eligibility,
            &self.policy,
            &self.predicate,
            &self.aggregate,
            &self.order,
            &self.output,
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProgramResourcePlan {
    // All values originate in the checked compiler/provider lowering. This
    // module has no request or configuration constructor.
    rows: usize,
    // At this pre-generation stage MAX_LANE_BYTES bounds only the encoded-lane
    // declaration. Post-WP-757 Active integration must independently bind the
    // actual decoded/borrowed footprint before this name can become runtime
    // evidence.
    decoded_lane_bytes: usize,
    optional_state_bytes: usize,
    selection_bytes: usize,
    partial_count: usize,
    partial_bytes_each: usize,
    top_n_kind: TopNPlanKind,
    result_maximum: usize,
    heap_entries: usize,
    output_rows: usize,
    output_bytes_per_row: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopNPlanKind {
    Absent,
    OrderedRows,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProgramCharges {
    rows: usize,
    lanes: usize,
    row_lane_evaluations: usize,
    decoded_lane_bytes: usize,
    optional_state_bytes: usize,
    selection_bytes: usize,
    partial_bytes: usize,
    result_maximum: usize,
    heap_entries: usize,
    output_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramIdentityFact {
    Definition,
    Provider,
    Plan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramLaneFact {
    Field,
    LogicalType,
    OptionalState,
    Phase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramResource {
    Rows,
    Lanes,
    DecodedLanes,
    OptionalState,
    Selection,
    Partial,
    ResultMaximum,
    Heap,
    Output,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramSealError {
    IdentitySubstitution(ProgramIdentityFact),
    LaneSubstitution(ProgramLaneFact),
    BoundExceeded(ProgramResource),
    ArithmeticOverflow(ProgramResource),
    ChargeMismatch(ProgramResource),
    NonCanonicalLaneCatalog,
    NonCanonicalLaneSet,
    OverlappingLane,
    MissingLane,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CheckedLogicalProgramFacts {
    identity: CheckedLogicalProgramIdentity,
    lanes: Vec<ProgramLane>,
    phases: PhaseLaneSets,
}

/// Compiler/provider facts before WP-757 attaches an Active root and generation.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PreGenerationBatchProgramDraft {
    facts: CheckedLogicalProgramFacts,
    width: ColumnarBatchWidth,
    resources: ProgramResourcePlan,
}

impl PreGenerationBatchProgramDraft {
    fn check(
        &self,
        checked: &CheckedLogicalProgramFacts,
    ) -> Result<CheckedPreGenerationBatchProgram, ProgramSealError> {
        validate_identity(self.facts.identity, checked.identity)?;
        validate_lane_catalog(&self.facts.lanes)?;
        validate_lane_catalog(&checked.lanes)?;
        validate_logical_facts(&self.facts, checked)?;

        if self.resources.rows == 0 || self.resources.rows > self.width.get() {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Rows));
        }
        let lane_count = self.facts.lanes.len();
        if lane_count == 0 || lane_count > MAX_SEGMENT_V2_COLUMNS {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Lanes));
        }
        let row_lane_evaluations =
            checked_charge_product(self.resources.rows, lane_count, ProgramResource::Lanes)?;

        if self.resources.decoded_lane_bytes == 0
            || self.resources.decoded_lane_bytes > MAX_LANE_BYTES
        {
            return Err(ProgramSealError::BoundExceeded(
                ProgramResource::DecodedLanes,
            ));
        }

        let state_map_bytes =
            self.resources
                .rows
                .checked_add(7)
                .ok_or(ProgramSealError::ArithmeticOverflow(
                    ProgramResource::OptionalState,
                ))?
                / 8;
        let optional_lanes = self.facts.lanes.iter().filter(|lane| lane.optional).count();
        // Optional lanes carry two independent state maps: Missing and Null.
        let required_optional_state_bytes = checked_charge_product(
            checked_charge_product(
                state_map_bytes,
                optional_lanes,
                ProgramResource::OptionalState,
            )?,
            2,
            ProgramResource::OptionalState,
        )?;
        check_maximum(
            self.resources.optional_state_bytes,
            MAX_PROGRAM_OPTIONAL_STATE_BYTES,
            ProgramResource::OptionalState,
        )?;
        if self.resources.optional_state_bytes != required_optional_state_bytes {
            return Err(ProgramSealError::ChargeMismatch(
                ProgramResource::OptionalState,
            ));
        }
        check_maximum(
            self.resources.selection_bytes,
            MAX_PROGRAM_SELECTION_BYTES,
            ProgramResource::Selection,
        )?;
        if self.resources.selection_bytes != state_map_bytes {
            return Err(ProgramSealError::ChargeMismatch(ProgramResource::Selection));
        }

        let max_partial_bytes = usize::try_from(MAX_AGGREGATE_STATE_BYTES_V1)
            .expect("aggregate-state maximum fits usize");
        check_maximum(
            self.resources.partial_count,
            self.resources.rows,
            ProgramResource::Partial,
        )?;
        check_maximum(
            self.resources.partial_bytes_each,
            max_partial_bytes,
            ProgramResource::Partial,
        )?;
        if (self.resources.partial_count == 0) != (self.resources.partial_bytes_each == 0) {
            return Err(ProgramSealError::ChargeMismatch(ProgramResource::Partial));
        }
        let partial_bytes = checked_charge_product(
            self.resources.partial_count,
            self.resources.partial_bytes_each,
            ProgramResource::Partial,
        )?;
        check_maximum(partial_bytes, max_partial_bytes, ProgramResource::Partial)?;

        let max_result_rows = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS)
            .expect("query page-row maximum fits usize");
        check_maximum(
            self.resources.result_maximum,
            max_result_rows,
            ProgramResource::ResultMaximum,
        )?;
        match self.resources.top_n_kind {
            TopNPlanKind::Absent => {
                if self.resources.result_maximum != 0 || self.resources.heap_entries != 0 {
                    return Err(ProgramSealError::ChargeMismatch(ProgramResource::Heap));
                }
            }
            TopNPlanKind::OrderedRows => {
                if self.resources.result_maximum == 0 {
                    return Err(ProgramSealError::ChargeMismatch(
                        ProgramResource::ResultMaximum,
                    ));
                }
                let required_heap_entries = self
                    .resources
                    .result_maximum
                    .checked_add(1)
                    .ok_or(ProgramSealError::ArithmeticOverflow(ProgramResource::Heap))?;
                if self.resources.heap_entries != required_heap_entries {
                    return Err(ProgramSealError::ChargeMismatch(ProgramResource::Heap));
                }
            }
        }

        let max_result_bytes = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES)
            .expect("query result-byte maximum fits usize");
        check_maximum(
            self.resources.output_rows,
            max_result_rows,
            ProgramResource::Output,
        )?;
        check_maximum(
            self.resources.output_bytes_per_row,
            max_result_bytes,
            ProgramResource::Output,
        )?;
        if (self.resources.output_rows == 0) != (self.resources.output_bytes_per_row == 0) {
            return Err(ProgramSealError::ChargeMismatch(ProgramResource::Output));
        }
        if self.resources.top_n_kind == TopNPlanKind::OrderedRows
            && self.resources.output_rows > self.resources.result_maximum
        {
            return Err(ProgramSealError::ChargeMismatch(ProgramResource::Output));
        }
        let output_bytes = checked_charge_product(
            self.resources.output_rows,
            self.resources.output_bytes_per_row,
            ProgramResource::Output,
        )?;
        check_maximum(output_bytes, max_result_bytes, ProgramResource::Output)?;

        validate_phase_lanes(&self.facts.phases, lane_count)?;

        Ok(CheckedPreGenerationBatchProgram {
            facts: self.facts.clone(),
            width: self.width,
            charges: ProgramCharges {
                rows: self.resources.rows,
                lanes: lane_count,
                row_lane_evaluations,
                decoded_lane_bytes: self.resources.decoded_lane_bytes,
                optional_state_bytes: self.resources.optional_state_bytes,
                selection_bytes: self.resources.selection_bytes,
                partial_bytes,
                result_maximum: self.resources.result_maximum,
                heap_entries: self.resources.heap_entries,
                output_bytes,
            },
        })
    }
}

fn checked_charge_product(
    count: usize,
    bytes_each: usize,
    resource: ProgramResource,
) -> Result<usize, ProgramSealError> {
    count
        .checked_mul(bytes_each)
        .ok_or(ProgramSealError::ArithmeticOverflow(resource))
}

fn validate_identity(
    expected: CheckedLogicalProgramIdentity,
    checked: CheckedLogicalProgramIdentity,
) -> Result<(), ProgramSealError> {
    if expected.definition != checked.definition {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Definition,
        ));
    }
    if expected.provider != checked.provider {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Provider,
        ));
    }
    if expected.plan != checked.plan {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Plan,
        ));
    }
    Ok(())
}

fn validate_lane_catalog(lanes: &[ProgramLane]) -> Result<(), ProgramSealError> {
    if lanes.windows(2).any(|pair| pair[0].field >= pair[1].field) {
        return Err(ProgramSealError::NonCanonicalLaneCatalog);
    }
    Ok(())
}

fn validate_logical_facts(
    expected: &CheckedLogicalProgramFacts,
    checked: &CheckedLogicalProgramFacts,
) -> Result<(), ProgramSealError> {
    if expected.lanes.len() != checked.lanes.len() {
        return Err(ProgramSealError::LaneSubstitution(ProgramLaneFact::Field));
    }
    for (expected, checked) in expected.lanes.iter().zip(&checked.lanes) {
        if expected.field != checked.field {
            return Err(ProgramSealError::LaneSubstitution(ProgramLaneFact::Field));
        }
        if expected.logical != checked.logical {
            return Err(ProgramSealError::LaneSubstitution(
                ProgramLaneFact::LogicalType,
            ));
        }
        if expected.optional != checked.optional {
            return Err(ProgramSealError::LaneSubstitution(
                ProgramLaneFact::OptionalState,
            ));
        }
    }
    if expected.phases != checked.phases {
        return Err(ProgramSealError::LaneSubstitution(ProgramLaneFact::Phase));
    }
    Ok(())
}

fn check_maximum(
    actual: usize,
    maximum: usize,
    resource: ProgramResource,
) -> Result<(), ProgramSealError> {
    if actual > maximum {
        return Err(ProgramSealError::BoundExceeded(resource));
    }
    Ok(())
}

fn validate_phase_lanes(phases: &PhaseLaneSets, lane_count: usize) -> Result<(), ProgramSealError> {
    let mut assigned = vec![false; lane_count];
    for lanes in phases.in_execution_order() {
        if lanes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ProgramSealError::NonCanonicalLaneSet);
        }
        for lane in lanes {
            let assignment = assigned
                .get_mut(*lane)
                .ok_or(ProgramSealError::MissingLane)?;
            if *assignment {
                return Err(ProgramSealError::OverlappingLane);
            }
            *assignment = true;
        }
    }
    if assigned.iter().any(|assigned| !assigned) {
        return Err(ProgramSealError::MissingLane);
    }
    Ok(())
}

struct CheckedPreGenerationBatchProgram {
    // Active root/generation binding is deliberately deferred to WP-757.
    facts: CheckedLogicalProgramFacts,
    width: ColumnarBatchWidth,
    charges: ProgramCharges,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::ColumnarBatchBoundError;

    fn hash<const BYTE: u8>() -> [u8; 32] {
        [BYTE; 32]
    }

    fn identity() -> CheckedLogicalProgramIdentity {
        CheckedLogicalProgramIdentity {
            definition: ColumnarDefinitionSemanticsHashV1::from_bytes(hash::<1>()),
            provider: ProjectionProviderDescriptorHash::from_bytes(hash::<2>()),
            plan: QueryPlanHash::from_bytes(hash::<3>()),
        }
    }

    fn width() -> ColumnarBatchWidth {
        ColumnarBatchWidth::choose(64, 1, 64).expect("closed width")
    }

    fn lane(field: u32, logical: SegmentV2LogicalType, optional: bool) -> ProgramLane {
        ProgramLane {
            field: FieldId::new(field).expect("nonzero field"),
            logical,
            optional,
        }
    }

    fn checked_facts() -> CheckedLogicalProgramFacts {
        CheckedLogicalProgramFacts {
            identity: identity(),
            lanes: vec![
                lane(1, SegmentV2LogicalType::U64, false),
                lane(2, SegmentV2LogicalType::String, true),
                lane(3, SegmentV2LogicalType::Bool, false),
                lane(4, SegmentV2LogicalType::I64, true),
                lane(5, SegmentV2LogicalType::Bytes, false),
                lane(6, SegmentV2LogicalType::Timestamp, false),
            ],
            phases: PhaseLaneSets {
                eligibility: vec![0],
                policy: vec![1],
                predicate: vec![2],
                aggregate: vec![3],
                order: vec![4],
                output: vec![5],
            },
        }
    }

    fn valid_draft() -> PreGenerationBatchProgramDraft {
        PreGenerationBatchProgramDraft {
            facts: checked_facts(),
            width: width(),
            resources: ProgramResourcePlan {
                rows: 64,
                decoded_lane_bytes: 4_096,
                optional_state_bytes: 32,
                selection_bytes: 8,
                partial_count: 16,
                partial_bytes_each: 64,
                top_n_kind: TopNPlanKind::OrderedRows,
                result_maximum: 63,
                heap_entries: 64,
                output_rows: 16,
                output_bytes_per_row: 256,
            },
        }
    }

    #[test]
    fn seals_exact_identities_types_phases_and_checked_charges() {
        let draft = valid_draft();
        let program = draft
            .check(&checked_facts())
            .expect("valid logical draft checks");

        assert_eq!(program.facts, checked_facts());
        assert_eq!(program.width.get(), 64);
        assert_eq!(program.charges.rows, 64);
        assert_eq!(program.charges.lanes, 6);
        assert_eq!(program.charges.row_lane_evaluations, 384);
        assert_eq!(program.charges.decoded_lane_bytes, 4_096);
        assert_eq!(program.charges.optional_state_bytes, 32);
        assert_eq!(program.charges.selection_bytes, 8);
        assert_eq!(program.charges.partial_bytes, 1024);
        assert_eq!(program.charges.result_maximum, 63);
        assert_eq!(program.charges.heap_entries, 64);
        assert_eq!(program.charges.output_bytes, 4096);
    }

    #[test]
    fn rejects_identity_substitution_one_fact_at_a_time() {
        let draft = valid_draft();
        let cases = [
            (
                CheckedLogicalProgramIdentity {
                    definition: ColumnarDefinitionSemanticsHashV1::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Definition,
            ),
            (
                CheckedLogicalProgramIdentity {
                    provider: ProjectionProviderDescriptorHash::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Provider,
            ),
            (
                CheckedLogicalProgramIdentity {
                    plan: QueryPlanHash::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Plan,
            ),
        ];
        for (identity, fact) in cases {
            let mut checked = checked_facts();
            checked.identity = identity;
            assert_eq!(
                draft.check(&checked).err(),
                Some(ProgramSealError::IdentitySubstitution(fact))
            );
        }
    }

    #[test]
    fn rejects_field_type_optional_and_phase_substitution() {
        let draft = valid_draft();
        let mut cases = Vec::new();

        let mut field = checked_facts();
        field.lanes[5].field = FieldId::new(7).expect("field");
        cases.push((field, ProgramLaneFact::Field));

        let mut logical = checked_facts();
        logical.lanes[0].logical = SegmentV2LogicalType::I64;
        cases.push((logical, ProgramLaneFact::LogicalType));

        let mut optional = checked_facts();
        optional.lanes[0].optional = true;
        cases.push((optional, ProgramLaneFact::OptionalState));

        let mut phase = checked_facts();
        phase.phases.eligibility.clear();
        phase.phases.policy.insert(0, 0);
        cases.push((phase, ProgramLaneFact::Phase));

        for (checked, fact) in cases {
            assert_eq!(
                draft.check(&checked).err(),
                Some(ProgramSealError::LaneSubstitution(fact))
            );
        }
    }

    #[test]
    fn rejects_duplicate_noncanonical_overlapping_and_missing_lanes() {
        let mut noncanonical_catalog = valid_draft();
        noncanonical_catalog.facts.lanes.swap(0, 1);
        let noncanonical_catalog_facts = noncanonical_catalog.facts.clone();
        assert_eq!(
            noncanonical_catalog
                .check(&noncanonical_catalog_facts)
                .err(),
            Some(ProgramSealError::NonCanonicalLaneCatalog)
        );

        let mut duplicate_field = valid_draft();
        duplicate_field.facts.lanes[1].field = duplicate_field.facts.lanes[0].field;
        let duplicate_field_facts = duplicate_field.facts.clone();
        assert_eq!(
            duplicate_field.check(&duplicate_field_facts).err(),
            Some(ProgramSealError::NonCanonicalLaneCatalog)
        );

        let mut duplicate = valid_draft();
        duplicate.facts.phases.policy = vec![1, 1];
        let duplicate_facts = duplicate.facts.clone();
        assert_eq!(
            duplicate.check(&duplicate_facts).err(),
            Some(ProgramSealError::NonCanonicalLaneSet)
        );

        let mut noncanonical = valid_draft();
        noncanonical.facts.phases.predicate = vec![3, 2];
        let noncanonical_facts = noncanonical.facts.clone();
        assert_eq!(
            noncanonical.check(&noncanonical_facts).err(),
            Some(ProgramSealError::NonCanonicalLaneSet)
        );

        let mut overlap = valid_draft();
        overlap.facts.phases.output.insert(0, 4);
        let overlap_facts = overlap.facts.clone();
        assert_eq!(
            overlap.check(&overlap_facts).err(),
            Some(ProgramSealError::OverlappingLane)
        );

        let mut missing = valid_draft();
        missing.facts.phases.output.clear();
        let missing_facts = missing.facts.clone();
        assert_eq!(
            missing.check(&missing_facts).err(),
            Some(ProgramSealError::MissingLane)
        );
    }

    #[test]
    fn accepts_exact_independent_byte_bounds_and_rejects_plus_one() {
        let max_partial = usize::try_from(MAX_AGGREGATE_STATE_BYTES_V1).expect("usize");
        let max_rows = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS).expect("usize");
        let max_output = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES).expect("usize");

        let mut decoded = valid_draft();
        decoded.resources.decoded_lane_bytes = MAX_LANE_BYTES;
        assert_eq!(
            decoded
                .check(&decoded.facts.clone())
                .expect("exact")
                .charges
                .decoded_lane_bytes,
            MAX_LANE_BYTES
        );
        decoded.resources.decoded_lane_bytes += 1;
        assert_eq!(
            decoded.check(&decoded.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(
                ProgramResource::DecodedLanes
            ))
        );
        decoded.resources.decoded_lane_bytes = 0;
        assert_eq!(
            decoded.check(&decoded.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(
                ProgramResource::DecodedLanes
            )),
            "zero is not decoded-footprint evidence; this draft only checks the encoded declaration"
        );

        let mut partial = valid_draft();
        partial.resources.partial_count = 1;
        partial.resources.partial_bytes_each = max_partial;
        assert_eq!(
            partial
                .check(&partial.facts.clone())
                .expect("exact")
                .charges
                .partial_bytes,
            max_partial
        );
        partial.resources.partial_bytes_each += 1;
        assert_eq!(
            partial.check(&partial.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Partial))
        );

        let mut partial_count = valid_draft();
        partial_count.resources.partial_count = partial_count.resources.rows;
        partial_count.resources.partial_bytes_each = 1;
        assert_eq!(
            partial_count
                .check(&partial_count.facts.clone())
                .expect("exact partial count")
                .charges
                .partial_bytes,
            partial_count.resources.rows
        );
        partial_count.resources.partial_count += 1;
        assert_eq!(
            partial_count.check(&partial_count.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Partial))
        );

        let mut output = valid_draft();
        output.resources.output_rows = 1;
        output.resources.output_bytes_per_row = max_output;
        assert_eq!(
            output
                .check(&output.facts.clone())
                .expect("exact")
                .charges
                .output_bytes,
            max_output
        );
        output.resources.output_bytes_per_row += 1;
        assert_eq!(
            output.check(&output.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Output))
        );

        let mut output_rows = valid_draft();
        output_rows.resources.result_maximum = max_rows;
        output_rows.resources.heap_entries = max_rows + 1;
        output_rows.resources.output_rows = max_rows;
        output_rows.resources.output_bytes_per_row = 1;
        assert_eq!(
            output_rows
                .check(&output_rows.facts.clone())
                .expect("exact output row count")
                .charges
                .output_bytes,
            max_rows
        );
        output_rows.resources.output_rows += 1;
        assert_eq!(
            output_rows.check(&output_rows.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Output))
        );
    }

    #[test]
    fn total_byte_ceilings_are_enforced_after_legal_operands() {
        let max_partial = usize::try_from(MAX_AGGREGATE_STATE_BYTES_V1).expect("usize");
        let max_output = usize::try_from(MAX_APPLICATION_QUERY_RESULT_BYTES).expect("usize");

        let mut partial = valid_draft();
        partial.resources.partial_count = 2;
        partial.resources.partial_bytes_each = max_partial / 2;
        assert_eq!(
            partial
                .check(&partial.facts.clone())
                .expect("exact aggregate-state product")
                .charges
                .partial_bytes,
            max_partial
        );
        partial.resources.partial_bytes_each += 1;
        assert_eq!(
            partial.check(&partial.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Partial)),
            "both operands are legal, so only the total-product ceiling refuses"
        );

        let mut output = valid_draft();
        output.resources.output_rows = 2;
        output.resources.output_bytes_per_row = max_output / 2;
        assert_eq!(
            output
                .check(&output.facts.clone())
                .expect("exact result-byte product")
                .charges
                .output_bytes,
            max_output
        );
        output.resources.output_bytes_per_row += 1;
        assert_eq!(
            output.check(&output.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Output)),
            "both operands are legal, so only the total-product ceiling refuses"
        );
    }

    #[test]
    fn row_lane_selection_and_optional_maps_have_exact_bounds() {
        let mut rows = valid_draft();
        rows.resources.rows = 0;
        assert_eq!(
            rows.check(&rows.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Rows))
        );
        rows.resources.rows = 65;
        assert_eq!(
            rows.check(&rows.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Rows))
        );

        let mut lanes = valid_draft();
        install_lane_count(&mut lanes, MAX_SEGMENT_V2_COLUMNS);
        lanes.resources.optional_state_bytes = 0;
        assert_eq!(
            lanes
                .check(&lanes.facts.clone())
                .expect("exact lane count")
                .charges
                .lanes,
            MAX_SEGMENT_V2_COLUMNS
        );
        install_lane_count(&mut lanes, MAX_SEGMENT_V2_COLUMNS + 1);
        assert_eq!(
            lanes.check(&lanes.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Lanes))
        );

        let mut maps = valid_draft();
        maps.width = ColumnarBatchWidth::choose(1_024, 1, 1_024).expect("closed width");
        maps.resources.rows = 1_024;
        maps.resources.selection_bytes = MAX_PROGRAM_SELECTION_BYTES;
        install_lane_count_with_optionality(&mut maps, MAX_SEGMENT_V2_COLUMNS, true);
        maps.resources.optional_state_bytes = MAX_PROGRAM_OPTIONAL_STATE_BYTES;
        let checked = maps.check(&maps.facts.clone()).expect("exact maps");
        assert_eq!(checked.charges.selection_bytes, MAX_PROGRAM_SELECTION_BYTES);
        assert_eq!(
            checked.charges.optional_state_bytes,
            MAX_PROGRAM_OPTIONAL_STATE_BYTES
        );

        let mut extra_selection = maps.clone();
        extra_selection.resources.selection_bytes += 1;
        assert_eq!(
            extra_selection.check(&extra_selection.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(ProgramResource::Selection))
        );
        let mut extra_optional = maps;
        extra_optional.resources.optional_state_bytes += 1;
        assert_eq!(
            extra_optional.check(&extra_optional.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(
                ProgramResource::OptionalState
            ))
        );
    }

    #[test]
    fn optional_state_charge_is_exactly_two_maps() {
        let exact = valid_draft();
        assert_eq!(
            exact
                .check(&checked_facts())
                .expect("two exact state maps")
                .charges
                .optional_state_bytes,
            32
        );
        for bytes in [31, 33] {
            let mut mismatch = exact.clone();
            mismatch.resources.optional_state_bytes = bytes;
            assert_eq!(
                mismatch.check(&checked_facts()).err(),
                Some(ProgramSealError::ChargeMismatch(
                    ProgramResource::OptionalState
                ))
            );
        }
    }

    #[test]
    fn top_n_heap_is_result_maximum_plus_one_probe() {
        let max_rows = usize::try_from(MAX_APPLICATION_QUERY_PAGE_ROWS).expect("usize");
        let mut exact = valid_draft();
        exact.resources.result_maximum = max_rows;
        exact.resources.heap_entries = max_rows + 1;
        let checked = exact
            .check(&exact.facts.clone())
            .expect("exact result and probe");
        assert_eq!(checked.charges.result_maximum, max_rows);
        assert_eq!(checked.charges.heap_entries, max_rows + 1);

        let mut excessive = exact.clone();
        excessive.resources.result_maximum += 1;
        excessive.resources.heap_entries += 1;
        assert_eq!(
            excessive.check(&excessive.facts.clone()).err(),
            Some(ProgramSealError::BoundExceeded(
                ProgramResource::ResultMaximum
            ))
        );

        let cases = [
            (TopNPlanKind::Absent, 0, 0, true),
            (TopNPlanKind::Absent, 1, 0, false),
            (TopNPlanKind::Absent, 0, 1, false),
            (TopNPlanKind::OrderedRows, 0, 1, false),
            (TopNPlanKind::OrderedRows, 1, 1, false),
            (TopNPlanKind::OrderedRows, 1, 2, true),
            (TopNPlanKind::OrderedRows, 1, 3, false),
        ];
        for (kind, result_maximum, heap_entries, accepted) in cases {
            let mut draft = valid_draft();
            draft.resources.top_n_kind = kind;
            draft.resources.result_maximum = result_maximum;
            draft.resources.heap_entries = heap_entries;
            draft.resources.output_rows = usize::from(result_maximum != 0);
            draft.resources.output_bytes_per_row = usize::from(result_maximum != 0);
            assert_eq!(draft.check(&draft.facts.clone()).is_ok(), accepted);
        }

        let mut exact_output = valid_draft();
        exact_output.resources.result_maximum = 16;
        exact_output.resources.heap_entries = 17;
        exact_output.resources.output_rows = 16;
        assert!(exact_output.check(&exact_output.facts.clone()).is_ok());

        let mut excessive_output = exact_output;
        excessive_output.resources.output_rows = 17;
        assert_eq!(
            excessive_output
                .check(&excessive_output.facts.clone())
                .err(),
            Some(ProgramSealError::ChargeMismatch(ProgramResource::Output)),
            "an OrderedRows result cannot emit past its sealed result maximum"
        );
    }

    #[test]
    fn operands_are_bounded_before_products_and_failures_are_atomic() {
        let cases = [
            (ProgramResource::Partial, true),
            (ProgramResource::Output, false),
        ];
        for (resource, partial) in cases {
            let mut draft = valid_draft();
            if partial {
                draft.resources.partial_count = 0;
                draft.resources.partial_bytes_each = usize::MAX;
            } else {
                draft.resources.output_rows = 0;
                draft.resources.output_bytes_per_row = usize::MAX;
            }
            let before = draft.clone();
            assert_eq!(
                draft.check(&checked_facts()).err(),
                Some(ProgramSealError::BoundExceeded(resource))
            );
            assert_eq!(draft, before);
        }
        assert_eq!(
            checked_charge_product(usize::MAX, 2, ProgramResource::Output),
            Err(ProgramSealError::ArithmeticOverflow(
                ProgramResource::Output
            ))
        );
    }

    fn install_lane_count(draft: &mut PreGenerationBatchProgramDraft, count: usize) {
        install_lane_count_with_optionality(draft, count, false);
    }

    fn install_lane_count_with_optionality(
        draft: &mut PreGenerationBatchProgramDraft,
        count: usize,
        optional: bool,
    ) {
        draft.facts.lanes = (0..count)
            .map(|index| {
                lane(
                    u32::try_from(index + 1).expect("bounded field"),
                    SegmentV2LogicalType::U64,
                    optional,
                )
            })
            .collect();
        draft.facts.phases = PhaseLaneSets {
            eligibility: (0..count).collect(),
            ..PhaseLaneSets::default()
        };
    }

    #[test]
    fn fixture_width_is_the_expected_closed_width() {
        assert_eq!(width().get(), 64);
        assert_eq!(
            ColumnarBatchWidth::choose(65, 1, 65),
            Err(ColumnarBatchBoundError::InvalidCompilerMaximum)
        );
    }
}
