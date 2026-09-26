//! Bounded V4 external receipt codec for the ADR-0203 storage scrub
//! (Amendment 1). V1, V2, and V3 encoders and bytes are independent.
use super::*;
use riffdb_storage_api::{OFFLINE_MAINTENANCE_RECEIPT_VERSION_V4, OfflineMaintenanceReceiptV4};

/// The closed storage-scrub kind byte, matching the public wire value.
const STORAGE_SCRUB_KIND_TAG: u8 = 0x04;

pub(in crate::maintenance) fn encode_scrub_receipt(
    receipt: &OfflineMaintenanceReceiptV4,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(OFFLINE_MAINTENANCE_RECEIPT_VERSION_V4)?;
    output.bytes(receipt.operation_id().as_bytes())?;
    output.u8(STORAGE_SCRUB_KIND_TAG)?;
    output.bytes(receipt.input_hash().as_bytes())?;
    let admission = receipt.admission();
    output.u8(admission.actor_kind().tag())?;
    output.framed_u16(admission.principal_id().as_str().as_bytes())?;
    output.bytes(admission.capability_id().as_bytes())?;
    match admission.approval_id() {
        None => output.u8(0)?,
        Some(approval_id) => {
            output.u8(1)?;
            output.framed_u16(approval_id.as_bytes())?;
        }
    }
    output.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for transition in receipt.transitions() {
        output.u8(transition.receipt_phase().tag())?;
        output.u8(transition.failure().map_or(0, |failure| failure.tag()))?;
    }
    let mut bytes = output.finish();
    let checksum = Sha256::digest(&bytes);
    reserve_bytes(&bytes, checksum.len())?;
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(in crate::maintenance) fn decode_scrub_receipt(
    encoded: &[u8],
) -> Result<OfflineMaintenanceReceiptV4, StorageError> {
    if encoded.len() > MAX_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    let body_length = encoded
        .len()
        .checked_sub(SHA256_BYTES)
        .ok_or_else(corrupt)?;
    let (body, stored_checksum) = encoded.split_at(body_length);
    if Sha256::digest(body).as_slice() != stored_checksum {
        return Err(corrupt());
    }
    let mut input = Decoder::new(body);
    if input.bytes(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC
        || input.u32()? != OFFLINE_MAINTENANCE_RECEIPT_VERSION_V4
    {
        return Err(incompatible());
    }
    let operation_id =
        OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    if input.u8()? != STORAGE_SCRUB_KIND_TAG {
        return Err(corrupt());
    }
    let input_hash = OfflineMaintenanceInputHash::from_bytes(input.array()?);
    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let principal_id = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability_id = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let approval_id = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let admission =
        OfflineMaintenanceAdmissionV1::new(principal_id, actor_kind, capability_id, approval_id);
    let transition_count = usize::from(input.u8()?);
    if transition_count == 0 || transition_count > MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
        return Err(corrupt());
    }
    let mut transitions = Vec::with_capacity(transition_count);
    for _ in 0..transition_count {
        let phase = OfflineMaintenanceReceiptPhaseV1::from_tag(input.u8()?).ok_or_else(corrupt)?;
        let failure_tag = input.u8()?;
        transitions.push(match (phase, failure_tag) {
            (OfflineMaintenanceReceiptPhaseV1::FailedClosed, tag) => {
                OfflineMaintenanceReceiptTransitionV1::failed(
                    OfflineMaintenanceReceiptFailureV1::from_tag(tag).ok_or_else(corrupt)?,
                )
            }
            (_, 0) => OfflineMaintenanceReceiptTransitionV1::phase(phase),
            _ => return Err(corrupt()),
        });
    }
    input.finish()?;
    OfflineMaintenanceReceiptV4::from_canonical_parts(
        operation_id,
        input_hash,
        admission,
        transitions,
    )
    .map_err(|_| corrupt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid_v7(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn validating_scrub_receipt() -> OfflineMaintenanceReceiptV4 {
        let mut receipt = OfflineMaintenanceReceiptV4::accepted_storage_scrub(
            OfflineMaintenanceOperationId::from_bytes(uuid_v7(21)).expect("maintenance operation"),
            OfflineMaintenanceAdmissionV1::new(
                ActorId::new("operator").expect("actor"),
                ActorKind::Human,
                CapabilityId::from_bytes(uuid_v7(22)).expect("capability"),
                Some(ApprovalId::new("approval").expect("approval")),
            ),
        )
        .expect("scrub receipt");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
            OfflineMaintenanceReceiptPhaseV1::Validating,
        ] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("legal scrub phase");
        }
        receipt
    }

    // req: REC-008
    #[test]
    fn scrub_receipt_v4_round_trips_canonically_and_refuses_damage() {
        let receipt = validating_scrub_receipt();
        let encoded = encode_scrub_receipt(&receipt).expect("encode");
        let canonical_hex = encoded
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            canonical_hex,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/compatibility/offline-maintenance-scrub-receipt-v4.hex"
            ))
            .trim(),
            "scrub receipt V4 exact bytes are a compatibility boundary"
        );
        assert_eq!(decode_scrub_receipt(&encoded).expect("decode"), receipt);

        let mut corrupt = encoded.clone();
        corrupt[40] ^= 0x01;
        assert!(decode_scrub_receipt(&corrupt).is_err());

        // A V3 version word under a valid checksum is an unknown version here,
        // never silently read as a scrub.
        let mut body = encoded[..encoded.len() - SHA256_BYTES].to_vec();
        let version = RECEIPT_MAGIC.len();
        body[version..version + 4].copy_from_slice(&3_u32.to_be_bytes());
        let checksum = Sha256::digest(&body);
        body.extend_from_slice(&checksum);
        assert_eq!(
            decode_scrub_receipt(&body).expect_err("V3 word").kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }

    // req: REC-008
    #[test]
    fn scrub_receipt_v4_never_publishes_an_artifact() {
        let mut receipt = validating_scrub_receipt();
        assert!(
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                    OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
                ))
                .is_err()
        );
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Succeeded,
            ))
            .expect("validating succeeds");
        assert!(
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::failed(
                    OfflineMaintenanceReceiptFailureV1::InternalFailure,
                ))
                .is_err(),
            "a terminal scrub receipt does not transition"
        );
    }
}
