//! One closed origin/cut semantic mapping; historical registry bytes stay exact.
use super::super::projection_source::metadata::{allocator_from_wire, allocator_to_wire};
use super::*;

pub(super) fn catalog_to_wire(
    value: &NativeCatalogTransitionV1,
) -> Result<wire::StoredAuthoritativeCatalogTransitionV1, DurableCodecError> {
    use wire::stored_authoritative_catalog_transition_v1::{Origin, V2ToV3};
    validate_catalog_originals(value)?;
    let parts = value.parts();
    let origin = match &parts.origin {
        NativeCatalogTransitionOriginV1::FreshV3 => Origin::FreshV3(wire::UnitV1 {}),
        NativeCatalogTransitionOriginV1::V2ToV3 {
            predecessor_history,
            original_registry,
            original_active_admission,
            legacy_controls,
        } => Origin::V2ToV3(Box::new(V2ToV3 {
            predecessor_history: Some(history_to_wire(**predecessor_history)),
            original_registry_envelope: original_registry.as_bytes().to_vec(),
            original_active_admission_envelope: original_active_admission.as_bytes().to_vec(),
            legacy_controls_commitment: legacy_controls.as_bytes().to_vec(),
        })),
    };
    Ok(wire::StoredAuthoritativeCatalogTransitionV1 {
        database_id: parts.database_id.as_bytes().to_vec(),
        administration_sequence: parts.administration_sequence.get(),
        publication_binding: Some(publication_to_wire(parts.publication_binding)),
        target_catalog_digest: parts.target_catalog_digest.to_vec(),
        target_registry_digest: parts.target_registry_digest.as_bytes().to_vec(),
        initial_source_allocator: Some(allocator_to_wire(parts.initial_source_allocator)),
        origin: Some(origin),
    })
}

fn catalog_from_wire(
    value: wire::StoredAuthoritativeCatalogTransitionV1,
) -> Result<NativeCatalogTransitionV1, DurableCodecError> {
    use wire::stored_authoritative_catalog_transition_v1::Origin;
    let origin = match require(value.origin)? {
        Origin::FreshV3(_) => NativeCatalogTransitionOriginV1::FreshV3,
        Origin::V2ToV3(value) => NativeCatalogTransitionOriginV1::V2ToV3 {
            predecessor_history: Box::new(history_from_wire(require(value.predecessor_history)?)?),
            original_registry: original_bytes(value.original_registry_envelope)?,
            original_active_admission: original_bytes(value.original_active_admission_envelope)?,
            legacy_controls: NativeLegacyControlsCommitmentV1::from_bytes(fixed(
                value.legacy_controls_commitment,
            )?),
        },
    };
    let value = storage_result(NativeCatalogTransitionV1::new(
        NativeCatalogTransitionPartsV1 {
            database_id: DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            administration_sequence: AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            publication_binding: publication_from_wire(require(value.publication_binding)?)?,
            target_catalog_digest: fixed(value.target_catalog_digest)?,
            target_registry_digest: SchemaHash::from_bytes(fixed(value.target_registry_digest)?),
            initial_source_allocator: allocator_from_wire(require(
                value.initial_source_allocator,
            )?)?,
            origin,
        },
    ))?;
    validate_catalog_originals(&value)?;
    Ok(value)
}

/// Admits all transient codec/output capacity before copying any origin fields.
/// The actual engine must already own this DTO under the same source ledger.
pub fn encode_native_catalog_transition_admitted_v1(
    value: &ProjectionSourceWorkAllocationV1<NativeCatalogTransitionV1>,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    if !value.belongs_to(budget) {
        return Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable);
    }
    encode_admitted(
        CATALOG,
        CATALOG_CAP,
        super::encode_bound::catalog_encode_work_bound(value)?,
        budget,
        || catalog_to_wire(value),
    )
}

/// Fully validates canonical framing and exact retained original-role bytes.
/// Decoding never supplies an Empty/Active proof or a native cut permit.
pub fn decode_native_catalog_transition_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<NativeCatalogTransitionV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    super::super::projection_source::decode_admitted_with_additional_work::<
        wire::StoredAuthoritativeCatalogTransitionV1,
        _,
    >(
        encoded,
        budget,
        super::encode_bound::catalog_semantic_history_box_work_bound()?,
        catalog_from_wire,
    )
}

/// Produces the origin-reference artifact digest after full canonical decoding,
/// hashing the exact complete envelope, including its original framing.
pub fn native_catalog_origin_envelope_digest_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<NativeCatalogOriginEnvelopeDigestV1, ProjectionSourceRecordDecodeErrorV1> {
    let validated = decode_native_catalog_transition_admitted_v1(encoded, budget)?;
    let digest = NativeCatalogOriginEnvelopeDigestV1::from_bytes(Sha256::digest(encoded).into());
    drop(validated);
    Ok(digest)
}

/// Revalidates the exact origin artifact; semantic reserialization is never used.
pub fn verify_native_catalog_origin_envelope_digest_admitted_v1(
    encoded: &[u8],
    expected: NativeCatalogOriginEnvelopeDigestV1,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<(), ProjectionSourceRecordDecodeErrorV1> {
    if native_catalog_origin_envelope_digest_admitted_v1(encoded, budget)? != expected {
        return Err(DurableCodecError::corrupt().into());
    }
    Ok(())
}
