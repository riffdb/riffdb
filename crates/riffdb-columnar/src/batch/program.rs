//! Private, inert sealing for one compiler-owned V2 batch program.

use riffdb_types::{
    ColumnarDefinitionSemanticsHashV1, ProjectionProviderDescriptorHash, QueryPlanHash,
};

use super::ColumnarBatchWidth;
use crate::segment_v2::{MAX_SEGMENT_V2_COLUMNS, SegmentV2LogicalType};

const MAX_PROGRAM_VALIDITY_BYTES: usize = 128 * 1024;
const MAX_PROGRAM_SELECTION_BYTES: usize = 128;
const MAX_PROGRAM_PARTIAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROGRAM_HEAP_ENTRIES: usize = 500;
const MAX_PROGRAM_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckedProgramIdentity {
    definition: ColumnarDefinitionSemanticsHashV1,
    provider: ProjectionProviderDescriptorHash,
    plan: QueryPlanHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProgramLaneType {
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
    BoundExceeded(ProgramResource),
    ArithmeticOverflow(ProgramResource),
    NonCanonicalLaneSet,
    OverlappingLane,
    MissingLane,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BatchProgramDraft {
    identity: CheckedProgramIdentity,
    width: ColumnarBatchWidth,
    lane_types: Vec<ProgramLaneType>,
    phases: PhaseLaneSets,
    resources: ProgramResourcePlan,
}

impl BatchProgramDraft {
    fn seal(&self, active: CheckedProgramIdentity) -> Result<SealedBatchProgram, ProgramSealError> {
        validate_identity(self.identity, active)?;

        let lane_count = self.lane_types.len();
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
        let optional_lanes = self.lane_types.iter().filter(|lane| lane.optional).count();
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

        validate_phase_lanes(&self.phases, lane_count)?;

        Ok(SealedBatchProgram {
            identity: self.identity,
            width: self.width,
            lane_types: self.lane_types.clone(),
            phases: self.phases.clone(),
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
    expected: CheckedProgramIdentity,
    active: CheckedProgramIdentity,
) -> Result<(), ProgramSealError> {
    if expected.definition != active.definition {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Definition,
        ));
    }
    if expected.provider != active.provider {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Provider,
        ));
    }
    if expected.plan != active.plan {
        return Err(ProgramSealError::IdentitySubstitution(
            ProgramIdentityFact::Plan,
        ));
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

struct SealedBatchProgram {
    identity: CheckedProgramIdentity,
    width: ColumnarBatchWidth,
    lane_types: Vec<ProgramLaneType>,
    phases: PhaseLaneSets,
    charges: ProgramCharges,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::ColumnarBatchBoundError;

    fn hash<const BYTE: u8>() -> [u8; 32] {
        [BYTE; 32]
    }

    fn identity() -> CheckedProgramIdentity {
        CheckedProgramIdentity {
            definition: ColumnarDefinitionSemanticsHashV1::from_bytes(hash::<1>()),
            provider: ProjectionProviderDescriptorHash::from_bytes(hash::<2>()),
            plan: QueryPlanHash::from_bytes(hash::<3>()),
        }
    }

    fn width() -> ColumnarBatchWidth {
        ColumnarBatchWidth::choose(64, 1, 64).expect("closed width")
    }

    fn lane(logical: SegmentV2LogicalType, optional: bool) -> ProgramLaneType {
        ProgramLaneType { logical, optional }
    }

    fn valid_draft() -> BatchProgramDraft {
        BatchProgramDraft {
            identity: identity(),
            width: width(),
            lane_types: vec![
                lane(SegmentV2LogicalType::U64, false),
                lane(SegmentV2LogicalType::String, true),
                lane(SegmentV2LogicalType::Bool, false),
                lane(SegmentV2LogicalType::I64, true),
                lane(SegmentV2LogicalType::Bytes, false),
                lane(SegmentV2LogicalType::Timestamp, false),
            ],
            phases: PhaseLaneSets {
                eligibility: vec![0],
                policy: vec![1],
                predicate: vec![2],
                aggregate: vec![3],
                order: vec![4],
                output: vec![5],
            },
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
        let program = draft.seal(identity()).expect("valid program seals");

        assert_eq!(program.identity, identity());
        assert_eq!(program.width.get(), 64);
        assert_eq!(program.lane_types, draft.lane_types);
        assert_eq!(program.phases, draft.phases);
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
                CheckedProgramIdentity {
                    definition: ColumnarDefinitionSemanticsHashV1::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Definition,
            ),
            (
                CheckedProgramIdentity {
                    provider: ProjectionProviderDescriptorHash::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Provider,
            ),
            (
                CheckedProgramIdentity {
                    plan: QueryPlanHash::from_bytes(hash::<9>()),
                    ..identity()
                },
                ProgramIdentityFact::Plan,
            ),
        ];
        for (active, fact) in cases {
            assert_eq!(
                draft.seal(active).err(),
                Some(ProgramSealError::IdentitySubstitution(fact))
            );
        }
    }

    #[test]
    fn rejects_duplicate_noncanonical_overlapping_and_missing_lanes() {
        let mut duplicate = valid_draft();
        duplicate.phases.policy = vec![1, 1];
        assert_eq!(
            duplicate.seal(identity()).err(),
            Some(ProgramSealError::NonCanonicalLaneSet)
        );

        let mut noncanonical = valid_draft();
        noncanonical.phases.predicate = vec![3, 2];
        assert_eq!(
            noncanonical.seal(identity()).err(),
            Some(ProgramSealError::NonCanonicalLaneSet)
        );

        let mut overlap = valid_draft();
        overlap.phases.output.insert(0, 4);
        assert_eq!(
            overlap.seal(identity()).err(),
            Some(ProgramSealError::OverlappingLane)
        );

        let mut missing = valid_draft();
        missing.phases.output.clear();
        assert_eq!(
            missing.seal(identity()).err(),
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
            let program = accepted.seal(identity()).expect("exact bound is accepted");
            assert_eq!(charged_resource(&program, resource), exact);

            let mut rejected = valid_draft();
            set_resource(&mut rejected, resource, excessive);
            if resource == ProgramResource::Lanes {
                install_lane_count(&mut rejected, excessive);
            }
            assert_eq!(
                rejected.seal(identity()).err(),
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
                draft.seal(identity()).err(),
                Some(ProgramSealError::ArithmeticOverflow(resource))
            );
            assert_eq!(draft, before);
        }
    }

    fn set_resource(draft: &mut BatchProgramDraft, resource: ProgramResource, value: usize) {
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

    fn install_lane_count(draft: &mut BatchProgramDraft, count: usize) {
        draft.lane_types = vec![lane(SegmentV2LogicalType::U64, false); count];
        draft.phases = PhaseLaneSets {
            eligibility: (0..count).collect(),
            ..PhaseLaneSets::default()
        };
    }

    fn charged_resource(program: &SealedBatchProgram, resource: ProgramResource) -> usize {
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
