//! Row conversion stays generation-neutral and retains ordinary semantic checks.
use super::super::canonical_record_from_bytes;
use super::*;
use riffdb_types::{decode_canonical_value, encode_canonical_record, encode_canonical_value};

pub(super) fn row_to_wire(
    value: &ProjectionSourceRowV1,
) -> Result<wire::StoredProjectionSourceRowV1, DurableCodecError> {
    Ok(wire::StoredProjectionSourceRowV1 {
        identity: Some(projection_identity_to_proto(value.identity())),
        locator: Some(locator_to_wire(value.locator())),
        canonical_group_values: {
            let mut groups = Vec::with_capacity(value.group_values().len());
            for group in value.group_values() {
                groups.push(
                    encode_canonical_value(group).map_err(|_| DurableCodecError::invariant())?,
                );
            }
            groups
        },
        canonical_measures: encode_canonical_record(value.measures())
            .map_err(|_| DurableCodecError::invariant())?,
        last_changed_sequence: value.last_changed_sequence().get(),
    })
}
pub(super) fn row_from_wire(
    value: wire::StoredProjectionSourceRowV1,
) -> Result<ProjectionSourceRowV1, DurableCodecError> {
    let groups = value
        .canonical_group_values
        .into_iter()
        .map(|bytes| {
            let value = decode_canonical_value(&bytes).map_err(|_| DurableCodecError::corrupt())?;
            if encode_canonical_value(&value).map_err(|_| DurableCodecError::corrupt())? != bytes {
                return Err(DurableCodecError::corrupt());
            }
            Ok(value)
        })
        .collect::<Result<_, _>>()?;
    storage_result(ProjectionSourceRowV1::from_stored_parts(
        projection_identity_from_proto(require(value.identity)?)?,
        locator_from_wire(require(value.locator)?)?,
        groups,
        canonical_record_from_bytes(&value.canonical_measures)?,
        CommitSequence::new(value.last_changed_sequence).ok_or_else(DurableCodecError::corrupt)?,
    ))
}
/// Encodes only a bounded row representation, never evaluation/publication proof.
/// Caller admits generated message and encoded output capacities before this call.
pub fn encode_projection_source_row_v1(
    value: &ProjectionSourceRowV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(ROW, &row_to_wire(value)?)
}
/// Allocation-free exact complete-envelope charge. Encoding still requires the
/// caller's construction allowance; native key charge remains separate.
pub fn projection_source_row_encoded_charge_v1(
    value: &ProjectionSourceRowV1,
) -> Result<EncodedContentCharge, DurableCodecError> {
    super::row_encode_bound::projection_source_row_encoded_charge_v1(value)
}
/// Structural syntax only; output owns its actual source-ledger allocation graph.
pub fn decode_projection_source_row_structural_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionSourceRowV1, _>(encoded, budget, row_from_wire)
}
/// Validates the exact plan/locator/F context under the same prior work admission.
pub fn decode_projection_source_row_admitted_v1(
    encoded: &[u8],
    schema: &crate::CheckedProjectionSchema,
    expected_locator: ProjectionSourceLocatorV1,
    applied_through: CommitSequence,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionSourceRowV1, _>(encoded, budget, |value| {
        storage_result(row_from_wire(value)?.into_checked(
            schema,
            expected_locator,
            applied_through,
        ))
    })
}
