//! One closed inspection/parser/mapper path for caller-owned durable decode work.
//! Numeric admission is supplied before scratch/output allocation by the caller.
use super::*;

/// Borrowed, allocation-free plan for the complete temporary/output overlap.
/// This is structural numerical evidence, never source or publication authority.
#[derive(Clone, Copy, Debug)]
pub struct ProjectionSourceCallerDecodeWorkV1 {
    maximum_work_bytes: u64,
    scratch_capacity: usize,
}
impl ProjectionSourceCallerDecodeWorkV1 {
    /// Complete generated/canonical/semantic/inspection overlap to admit first.
    #[must_use]
    pub const fn maximum_work_bytes(self) -> u64 {
        self.maximum_work_bytes
    }
    /// Canonical encoding buffer capacity, admitted before construction.
    #[must_use]
    pub const fn scratch_capacity(self) -> usize {
        self.scratch_capacity
    }
}

fn inspect_work<M: ProjectionSourceMessageV1>(
    encoded: &[u8],
) -> Result<ProjectionSourceCallerDecodeWorkV1, ProjectionSourceRecordDecodeErrorV1> {
    let input = inspect_projection_source_decode_v1::<M>(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    Ok(ProjectionSourceCallerDecodeWorkV1 {
        maximum_work_bytes: semantic_work_bound(&input)?,
        scratch_capacity: encoded.len(),
    })
}

fn reconstruct<M, T>(
    encoded: &[u8],
    scratch: &mut Vec<u8>,
    from_wire: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<EncodedPageItem<T>, ProjectionSourceRecordDecodeErrorV1>
where
    M: ProjectionSourceMessageV1,
{
    let input = inspect_projection_source_decode_v1::<M>(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let wire = input
        .decode_with_caller_scratch(scratch)
        .map_err(|error| match error {
            riffdb_proto::envelope::CallerScratchDecodeErrorV1::Envelope(error) => {
                ProjectionSourceRecordDecodeErrorV1::Codec(DurableCodecError::from_decode_envelope(
                    error,
                ))
            }
            riffdb_proto::envelope::CallerScratchDecodeErrorV1::CapacityUnavailable => {
                ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
            }
        })?;
    let value = from_wire(wire)?;
    let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

macro_rules! caller_role {
    ($inspect:ident, $decode:ident, $wire:ty, $dto:ty, $mapper:path) => {
        /// Computes the existing closed codec's full work bound without allocation.
        pub fn $inspect(
            encoded: &[u8],
        ) -> Result<ProjectionSourceCallerDecodeWorkV1, ProjectionSourceRecordDecodeErrorV1> {
            inspect_work::<$wire>(encoded)
        }
        /// Reuses the sole ordinary mapper and canonical parser. The caller must
        /// already own the inspected work allowance, scratch and final graph.
        /// This structural value grants no database or source authority.
        pub fn $decode(
            encoded: &[u8],
            scratch: &mut Vec<u8>,
        ) -> Result<EncodedPageItem<$dto>, ProjectionSourceRecordDecodeErrorV1> {
            reconstruct::<$wire, _>(encoded, scratch, $mapper)
        }
    };
}
caller_role!(
    inspect_projection_source_row_caller_decode_v1,
    decode_projection_source_row_with_caller_scratch_v1,
    wire::StoredProjectionSourceRowV1,
    ProjectionSourceRowV1,
    row::row_from_wire
);
caller_role!(
    inspect_projection_source_object_caller_decode_v1,
    decode_projection_source_object_with_caller_scratch_v1,
    wire::StoredProjectionSourceObjectV1,
    ProjectionSourceObjectV1,
    metadata::object_from_wire
);
caller_role!(
    inspect_projection_source_root_caller_decode_v1,
    decode_projection_source_root_with_caller_scratch_v1,
    wire::StoredProjectionSourceRootV1,
    ProjectionSourceRootV1,
    metadata::root_from_wire
);
caller_role!(
    inspect_projection_source_maintenance_caller_decode_v1,
    decode_projection_source_maintenance_with_caller_scratch_v1,
    wire::StoredProjectionSourceMaintenanceV1,
    ProjectionSourceMaintenanceV1,
    metadata::maintenance_from_wire
);
