//! Private, inert checking for one pre-generation logical V2 batch draft.

use riffdb_types::{
    ColumnarDefinitionSemanticsHashV1, FieldId, ProjectionProviderDescriptorHash, QueryPlanHash,
};

use super::ColumnarBatchWidth;
use crate::segment_v2::{MAX_SEGMENT_V2_COLUMNS, SegmentV2LogicalType};

const MAX_PROGRAM_VALIDITY_BYTES: usize = 128 * 1024;
const MAX_PROGRAM_SELECTION_BYTES: usize = 128;
const MAX_PROGRAM_PARTIAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROGRAM_HEAP_ENTRIES: usize = 500;
const MAX_PROGRAM_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

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
    validity_bytes: usize,
    selection_bytes: usize,
    partial_rows: usize,
    partial_bytes_per_row: usize,
    heap_entries: usize,
    output_rows: usize,
    output_bytes_per_row: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProgramCharges {
    rows: usize,
    lanes: usize,
    row_lane_evaluations: usize,
    validity_bytes: usize,
    selection_bytes: usize,
    partial_bytes: usize,
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
    Validity,
    Selection,
    Partial,
    Heap,
    Output,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramSealError {
    IdentitySubstitution(ProgramIdentityFact),
    LaneSubstitution(ProgramLaneFact),
    BoundExceeded(ProgramResource),
    ArithmeticOverflow(ProgramResource),
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

        let lane_count = self.facts.lanes.len();
        let row_lane_evaluations = self
            .resources
            .rows
            .checked_mul(lane_count)
            .ok_or(ProgramSealError::ArithmeticOverflow(ProgramResource::Lanes))?;
        let partial_bytes = self
            .resources
            .partial_rows
            .checked_mul(self.resources.partial_bytes_per_row)
            .ok_or(ProgramSealError::ArithmeticOverflow(
                ProgramResource::Partial,
            ))?;
        let output_bytes = self
            .resources
            .output_rows
            .checked_mul(self.resources.output_bytes_per_row)
            .ok_or(ProgramSealError::ArithmeticOverflow(
                ProgramResource::Output,
            ))?;

        if self.resources.rows == 0 || self.resources.rows > self.width.get() {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Rows));
        }
        if lane_count == 0 || lane_count > MAX_SEGMENT_V2_COLUMNS {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Lanes));
        }

        let validity_map_bytes =
            self.resources
                .rows
                .checked_add(7)
                .ok_or(ProgramSealError::ArithmeticOverflow(
                    ProgramResource::Validity,
                ))?
                / 8;
        let optional_lanes = self.facts.lanes.iter().filter(|lane| lane.optional).count();
        let required_validity_bytes = validity_map_bytes.checked_mul(optional_lanes).ok_or(
            ProgramSealError::ArithmeticOverflow(ProgramResource::Validity),
        )?;
        if self.resources.validity_bytes < required_validity_bytes
            || self.resources.validity_bytes > MAX_PROGRAM_VALIDITY_BYTES
        {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Validity));
        }
        if self.resources.selection_bytes < validity_map_bytes
            || self.resources.selection_bytes > MAX_PROGRAM_SELECTION_BYTES
        {
            return Err(ProgramSealError::BoundExceeded(ProgramResource::Selection));
        }
        check_maximum(
            partial_bytes,
            MAX_PROGRAM_PARTIAL_BYTES,
            ProgramResource::Partial,
        )?;
        check_maximum(
            self.resources.heap_entries,
            MAX_PROGRAM_HEAP_ENTRIES,
            ProgramResource::Heap,
        )?;
        check_maximum(
            output_bytes,
            MAX_PROGRAM_OUTPUT_BYTES,
            ProgramResource::Output,
        )?;

        validate_phase_lanes(&self.facts.phases, lane_count)?;

        Ok(CheckedPreGenerationBatchProgram {
            facts: self.facts.clone(),
            width: self.width,
            charges: ProgramCharges {
                rows: self.resources.rows,
                lanes: lane_count,
                row_lane_evaluations,
                validity_bytes: self.resources.validity_bytes,
                selection_bytes: self.resources.selection_bytes,
                partial_bytes,
                heap_entries: self.resources.heap_entries,
                output_bytes,
            },
        })
    }
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
                validity_bytes: 16,
                selection_bytes: 8,
                partial_rows: 16,
                partial_bytes_per_row: 64,
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
        assert_eq!(program.charges.validity_bytes, 16);
        assert_eq!(program.charges.selection_bytes, 8);
        assert_eq!(program.charges.partial_bytes, 1024);
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
    fn accepts_each_exact_resource_bound_and_rejects_plus_one_independently() {
        let cases = [
            (ProgramResource::Rows, 64, 65),
            (
                ProgramResource::Lanes,
                MAX_SEGMENT_V2_COLUMNS,
                MAX_SEGMENT_V2_COLUMNS + 1,
            ),
            (
                ProgramResource::Validity,
                MAX_PROGRAM_VALIDITY_BYTES,
                MAX_PROGRAM_VALIDITY_BYTES + 1,
            ),
            (
                ProgramResource::Selection,
                MAX_PROGRAM_SELECTION_BYTES,
                MAX_PROGRAM_SELECTION_BYTES + 1,
            ),
            (
                ProgramResource::Partial,
                MAX_PROGRAM_PARTIAL_BYTES,
                MAX_PROGRAM_PARTIAL_BYTES + 1,
            ),
            (
                ProgramResource::Heap,
                MAX_PROGRAM_HEAP_ENTRIES,
                MAX_PROGRAM_HEAP_ENTRIES + 1,
            ),
            (
                ProgramResource::Output,
                MAX_PROGRAM_OUTPUT_BYTES,
                MAX_PROGRAM_OUTPUT_BYTES + 1,
            ),
        ];

        for (resource, exact, excessive) in cases {
            let mut accepted = valid_draft();
            set_resource(&mut accepted, resource, exact);
            if resource == ProgramResource::Lanes {
                install_lane_count(&mut accepted, exact);
            }
            let program = accepted
                .check(&accepted.facts.clone())
                .expect("exact bound is accepted");
            assert_eq!(charged_resource(&program, resource), exact);

            let mut rejected = valid_draft();
            set_resource(&mut rejected, resource, excessive);
            if resource == ProgramResource::Lanes {
                install_lane_count(&mut rejected, excessive);
            }
            assert_eq!(
                rejected.check(&rejected.facts.clone()).err(),
                Some(ProgramSealError::BoundExceeded(resource))
            );
        }
    }

    #[test]
    fn arithmetic_overflow_and_other_failures_leave_the_draft_unchanged() {
        let cases = [ProgramResource::Partial, ProgramResource::Output];
        for resource in cases {
            let mut draft = valid_draft();
            match resource {
                ProgramResource::Partial => {
                    draft.resources.partial_rows = usize::MAX;
                    draft.resources.partial_bytes_per_row = 2;
                }
                ProgramResource::Output => {
                    draft.resources.output_rows = usize::MAX;
                    draft.resources.output_bytes_per_row = 2;
                }
                _ => unreachable!("overflow fixture uses a multiplied byte charge"),
            }
            let before = draft.clone();

            assert_eq!(
                draft.check(&checked_facts()).err(),
                Some(ProgramSealError::ArithmeticOverflow(resource))
            );
            assert_eq!(draft, before);
        }
    }

    fn set_resource(
        draft: &mut PreGenerationBatchProgramDraft,
        resource: ProgramResource,
        value: usize,
    ) {
        match resource {
            ProgramResource::Rows => draft.resources.rows = value,
            ProgramResource::Lanes => {}
            ProgramResource::Validity => draft.resources.validity_bytes = value,
            ProgramResource::Selection => draft.resources.selection_bytes = value,
            ProgramResource::Partial => {
                draft.resources.partial_rows = 1;
                draft.resources.partial_bytes_per_row = value;
            }
            ProgramResource::Heap => draft.resources.heap_entries = value,
            ProgramResource::Output => {
                draft.resources.output_rows = 1;
                draft.resources.output_bytes_per_row = value;
            }
        }
    }

    fn install_lane_count(draft: &mut PreGenerationBatchProgramDraft, count: usize) {
        draft.facts.lanes = (0..count)
            .map(|index| {
                lane(
                    u32::try_from(index + 1).expect("bounded field"),
                    SegmentV2LogicalType::U64,
                    false,
                )
            })
            .collect();
        draft.facts.phases = PhaseLaneSets {
            eligibility: (0..count).collect(),
            ..PhaseLaneSets::default()
        };
    }

    fn charged_resource(
        program: &CheckedPreGenerationBatchProgram,
        resource: ProgramResource,
    ) -> usize {
        match resource {
            ProgramResource::Rows => program.charges.rows,
            ProgramResource::Lanes => program.charges.lanes,
            ProgramResource::Validity => program.charges.validity_bytes,
            ProgramResource::Selection => program.charges.selection_bytes,
            ProgramResource::Partial => program.charges.partial_bytes,
            ProgramResource::Heap => program.charges.heap_entries,
            ProgramResource::Output => program.charges.output_bytes,
        }
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
