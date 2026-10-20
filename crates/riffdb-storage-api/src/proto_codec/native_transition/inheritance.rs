//! Bounded exact source witnesses and original scalar states, without authority.
use super::*;
use wire::stored_source_inheritance_administration_v1 as inherited;

fn scalar_to_wire<const MAX: usize>(
    value: &Option<NativeTransitionEnvelopeBytesV1<MAX>>,
) -> inherited::PredecessorScalar {
    use inherited::predecessor_scalar::State;
    inherited::PredecessorScalar {
        state: Some(match value {
            None => State::Absent(wire::UnitV1 {}),
            Some(bytes) => State::PresentEnvelope(bytes.as_bytes().to_vec()),
        }),
    }
}
fn scalar_from_wire<const MAX: usize>(
    value: Option<inherited::PredecessorScalar>,
) -> Result<Option<NativeTransitionEnvelopeBytesV1<MAX>>, DurableCodecError> {
    use inherited::predecessor_scalar::State;
    match require(require(value)?.state)? {
        State::Absent(_) => Ok(None),
        State::PresentEnvelope(bytes) => original_bytes(bytes).map(Some),
    }
}

fn witness_from_wire(bytes: Vec<u8>) -> Result<SourceInheritanceWitnessV1, DurableCodecError> {
    let bytes: [u8; 96] = fixed(bytes)?;
    let database = bytes[..16]
        .try_into()
        .map_err(|_| DurableCodecError::corrupt())?;
    let incarnation = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| DurableCodecError::corrupt())?,
    );
    let ordinal = u64::from_be_bytes(
        bytes[24..32]
            .try_into()
            .map_err(|_| DurableCodecError::corrupt())?,
    );
    let locator = ProjectionSourceLocatorV1::new(
        DatabaseId::from_bytes(database).map_err(|_| DurableCodecError::corrupt())?,
        incarnation,
        ProjectionCheckpointOrdinal::new(ordinal).ok_or_else(DurableCodecError::corrupt)?,
    )
    .ok_or_else(DurableCodecError::corrupt)?;
    let descriptor = ContentDigest::from_stored_parts(
        DIGEST_SCHEME_V1,
        HashDomain::ProjectionSourceDescriptor,
        bytes[32..64]
            .try_into()
            .map_err(|_| DurableCodecError::corrupt())?,
    )
    .ok_or_else(DurableCodecError::corrupt)?;
    let origin = ContentDigest::from_stored_parts(
        DIGEST_SCHEME_V1,
        HashDomain::ProjectionSourceOrigin,
        bytes[64..]
            .try_into()
            .map_err(|_| DurableCodecError::corrupt())?,
    )
    .ok_or_else(DurableCodecError::corrupt)?;
    storage_result(SourceInheritanceWitnessV1::new(locator, descriptor, origin))
}

pub(super) fn inheritance_to_wire(
    value: &SourceInheritanceAdministrationV1,
) -> Result<wire::StoredSourceInheritanceAdministrationV1, DurableCodecError> {
    validate_inherited_originals(value)?;
    let parts = value.parts();
    let lineage = parts.successor_lineage;
    let scalars = &parts.predecessor_scalars;
    let mut source_witnesses = Vec::with_capacity(parts.witnesses.len());
    for witness in &parts.witnesses {
        source_witnesses.push(witness.canonical_witness_bytes().to_vec());
    }
    Ok(wire::StoredSourceInheritanceAdministrationV1 {
        administration_sequence: parts.administration_sequence.get(),
        cause: match parts.cause {
            SourceInheritanceCauseV1::Restore => 1,
            SourceInheritanceCauseV1::Promotion => 2,
        },
        predecessor_history: Some(history_to_wire(parts.predecessor_history)),
        successor_lineage: Some(inherited::SuccessorLineage {
            database_id: lineage.database_id().as_bytes().to_vec(),
            history_incarnation: lineage.history_incarnation(),
            leadership_epoch: lineage.leadership_epoch().get(),
            catalog_digest: lineage.catalog_digest().to_vec(),
        }),
        anchor_binding: Some(publication_to_wire(parts.anchor_binding)),
        original_origin: Some(inherited::OriginReference {
            administration_sequence: parts.original_origin.administration_sequence().get(),
            canonical_envelope_digest: parts.original_origin.digest().as_bytes().to_vec(),
        }),
        authorized_selection_commitment: parts.authorized_selection.as_bytes().to_vec(),
        source_witnesses,
        source_allocator: Some(scalar_to_wire(&scalars.source_allocator)),
        incarnation: Some(scalar_to_wire(&scalars.incarnation)),
        registry: Some(scalar_to_wire(&scalars.registry)),
        watermark: Some(scalar_to_wire(&scalars.watermark)),
    })
}

fn inheritance_from_wire(
    value: wire::StoredSourceInheritanceAdministrationV1,
) -> Result<SourceInheritanceAdministrationV1, DurableCodecError> {
    let lineage = require(value.successor_lineage)?;
    let origin = require(value.original_origin)?;
    let witnesses = value
        .source_witnesses
        .into_iter()
        .map(witness_from_wire)
        .collect::<Result<Vec<_>, _>>()?;
    let value = storage_result(SourceInheritanceAdministrationV1::new(
        SourceInheritanceAdministrationPartsV1 {
            administration_sequence: AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            cause: match value.cause {
                1 => SourceInheritanceCauseV1::Restore,
                2 => SourceInheritanceCauseV1::Promotion,
                _ => return Err(DurableCodecError::corrupt()),
            },
            predecessor_history: history_from_wire(require(value.predecessor_history)?)?,
            successor_lineage: crate::ChangelogLineageV3::new_with_catalog(
                DatabaseId::from_bytes(fixed(lineage.database_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                lineage.history_incarnation,
                LeadershipEpochV1::new(lineage.leadership_epoch)
                    .ok_or_else(DurableCodecError::corrupt)?,
                fixed(lineage.catalog_digest)?,
            )
            .map_err(|_| DurableCodecError::corrupt())?,
            anchor_binding: publication_from_wire(require(value.anchor_binding)?)?,
            original_origin: NativeCatalogOriginReferenceV1::new(
                AdministrationSequence::new(origin.administration_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?,
                NativeCatalogOriginEnvelopeDigestV1::from_bytes(fixed(
                    origin.canonical_envelope_digest,
                )?),
            ),
            authorized_selection: SourceInheritanceSelectionCommitmentV1::from_bytes(fixed(
                value.authorized_selection_commitment,
            )?),
            witnesses: witnesses.into_boxed_slice(),
            predecessor_scalars: SourceInheritancePredecessorScalarsV1 {
                source_allocator: scalar_from_wire(value.source_allocator)?,
                incarnation: scalar_from_wire(value.incarnation)?,
                registry: scalar_from_wire(value.registry)?,
                watermark: scalar_from_wire(value.watermark)?,
            },
        },
    ))?;
    validate_inherited_originals(&value)?;
    Ok(value)
}

/// Admits transient wire, exact original checks and retained encoded output
/// before copying witnesses; the DTO must belong to this actual source ledger.
pub fn encode_source_inheritance_admitted_v1(
    value: &ProjectionSourceWorkAllocationV1<SourceInheritanceAdministrationV1>,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    if !value.belongs_to(budget) {
        return Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable);
    }
    encode_admitted(
        INHERITANCE,
        INHERITANCE_CAP,
        super::encode_bound::inheritance_encode_work_bound(value)?,
        budget,
        || inheritance_to_wire(value),
    )
}

/// Fully validates both framing versions, fixed witnesses and exact original
/// scalar roles. Native closure, selected-recipe absence and preimages remain
/// mandatory separate proofs; a decoded fact grants no acquisition authority.
pub fn decode_source_inheritance_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<SourceInheritanceAdministrationV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredSourceInheritanceAdministrationV1, _>(
        encoded,
        budget,
        inheritance_from_wire,
    )
}
