//! Exact original-role checks using the qualified caller-owned canonical decoder.
use super::*;

fn original_registry(
    bytes: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<super::super::metadata::StoredRecordRegistry, DurableCodecError> {
    match inspect_readable_record_role_v1(bytes).map_err(DurableCodecError::from_decode_envelope)? {
        "riffdb.storage.v1.StoredRecordRegistryV2" => {
            super::super::metadata::registry_v2_from_wire(caller_decode::<
                wire::StoredRecordRegistryV2,
            >(bytes, scratch)?)
        }
        "riffdb.storage.v1.StoredRecordRegistryV3" => {
            super::super::metadata::registry_v3_from_wire(caller_decode::<
                wire::StoredRecordRegistryV3,
            >(bytes, scratch)?)
        }
        _ => Err(DurableCodecError::corrupt()),
    }
}

fn registry_context(
    bytes: &[u8],
    history: crate::ChangelogHistoryStateV3,
    scratch: &mut Vec<u8>,
) -> Result<(), DurableCodecError> {
    let registry = original_registry(bytes, scratch)?;
    if let super::super::metadata::RecordRegistryValidation::Completed {
        database_id,
        history_incarnation,
    } = registry.validation()
        && (database_id != history.lineage().database_id()
            || history_incarnation != history.lineage().history_incarnation())
    {
        return Err(DurableCodecError::corrupt());
    }
    Ok(())
}

pub(super) fn validate_catalog_originals(
    value: &NativeCatalogTransitionV1,
) -> Result<(), DurableCodecError> {
    if let NativeCatalogTransitionOriginV1::V2ToV3 {
        predecessor_history,
        original_registry,
        original_active_admission,
        ..
    } = &value.parts().origin
    {
        let mut scratch = Vec::with_capacity(INNER_ENVELOPE_CAP);
        registry_context(
            original_registry.as_bytes(),
            **predecessor_history,
            &mut scratch,
        )?;
        let original = caller_decode::<wire::ReplicationPrimaryAdmissionV1>(
            original_active_admission.as_bytes(),
            &mut scratch,
        )?;
        if !matches!(
            &original.state,
            Some(wire::replication_primary_admission_v1::State::Active(_))
        ) {
            return Err(DurableCodecError::corrupt());
        }
        let active = admission_from_wire_v1(original)?;
        storage_result(value.validate_original_active(&active))?;
    }
    Ok(())
}

pub(super) fn validate_inherited_originals(
    value: &SourceInheritanceAdministrationV1,
) -> Result<(), DurableCodecError> {
    let parts = value.parts();
    let scalars = &parts.predecessor_scalars;
    let before = parts.predecessor_history;
    let incarnation = before.lineage().history_incarnation();
    let mut scratch = Vec::with_capacity(INNER_ENVELOPE_CAP);
    if let Some(bytes) = &scalars.source_allocator {
        let allocator = super::super::projection_source::decode_projection_checkpoint_ordinal_allocator_inline_v1(bytes.as_bytes())?;
        if allocator.value().allocation_history_incarnation() != incarnation {
            return Err(DurableCodecError::corrupt());
        }
    }
    if let Some(bytes) = &scalars.incarnation {
        let stored = super::super::metadata::incarnation_from_wire(caller_decode::<
            wire::StoredHistoryIncarnationV1,
        >(
            bytes.as_bytes(),
            &mut scratch,
        )?)?;
        if stored != incarnation {
            return Err(DurableCodecError::corrupt());
        }
    }
    if let Some(bytes) = &scalars.registry {
        registry_context(bytes.as_bytes(), before, &mut scratch)?;
    }
    if let Some(bytes) = &scalars.watermark {
        let watermark = super::super::retention::watermark_from_wire(caller_decode::<
            wire::StoredRetentionWatermarkV1,
        >(
            bytes.as_bytes(),
            &mut scratch,
        )?)?;
        if watermark.history_incarnation() != incarnation {
            return Err(DurableCodecError::corrupt());
        }
    }
    Ok(())
}

pub(super) fn original_bytes<const MAX: usize>(
    bytes: Vec<u8>,
) -> Result<NativeTransitionEnvelopeBytesV1<MAX>, DurableCodecError> {
    storage_result(NativeTransitionEnvelopeBytesV1::new(
        bytes.into_boxed_slice(),
    ))
}
