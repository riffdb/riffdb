//! Durable codec for opaque exact symbolic application-export state.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{ApplicationExportOperationId, ContractLineage};

use crate::{
    ApplicationExportLedgerPrefixV1, ApplicationExportPageCommitmentV1, EncodedPageItem,
    StoredApplicationExportOperation, StoredApplicationExportOperationV1,
    StoredApplicationExportOperationV2,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed,
    storage_result,
};

const APPLICATION_EXPORT_OPERATION: &str = "riffdb.storage.v1.StoredApplicationExportOperationV1";
const COMPACT_APPLICATION_EXPORT_OPERATION: &str =
    "riffdb.storage.v1.StoredApplicationExportOperationV2";
const APPLICATION_EXPORT_PAGE: &str = "riffdb.storage.v1.StoredApplicationExportPageCommitmentV1";

/// Encodes the retained operation's original version without upgrading it.
pub fn encode_application_export_head(
    value: &StoredApplicationExportOperation,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    match value {
        StoredApplicationExportOperation::Legacy(value) => {
            encode_application_export_operation_v1(value)
        }
        StoredApplicationExportOperation::Compact(value) => {
            encode_application_export_operation_v2(value)
        }
    }
}

/// Selects only a registered export-operation role; malformed or foreign
/// envelopes cannot become absence or silently downgrade to the legacy path.
pub fn decode_application_export_head(
    encoded: &[u8],
) -> Result<StoredApplicationExportOperation, DurableCodecError> {
    if super::decode_record_variant(
        encoded,
        COMPACT_APPLICATION_EXPORT_OPERATION,
        APPLICATION_EXPORT_OPERATION,
    )? {
        Ok(StoredApplicationExportOperation::Compact(
            decode_application_export_operation_v2(encoded)?
                .into_parts()
                .0,
        ))
    } else {
        Ok(StoredApplicationExportOperation::Legacy(
            decode_application_export_operation_v1(encoded)?
                .into_parts()
                .0,
        ))
    }
}

/// Encodes the versioned compact head without reading or encoding earlier pages.
pub fn encode_application_export_operation_v2(
    value: &StoredApplicationExportOperationV2,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        COMPACT_APPLICATION_EXPORT_OPERATION,
        &wire::StoredApplicationExportOperationV2 {
            operation_id: value.operation_id().as_bytes().to_vec(),
            contract_lineage: value.lineage().as_str().to_owned(),
            immutable_binding: value.immutable_binding().to_vec(),
            canonical_state: value.canonical_state().to_vec(),
            canonical_prefix: storage_result(value.prefix().canonical_bytes())?.to_vec(),
        },
    )
}

/// Decodes the complete canonical head and proves its immutable genesis binding.
/// Recovery additionally verifies the retained ledger against the claimed prefix.
pub fn decode_application_export_operation_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredApplicationExportOperationV2>, DurableCodecError> {
    decode_message::<wire::StoredApplicationExportOperationV2, _, _>(
        COMPACT_APPLICATION_EXPORT_OPERATION,
        encoded,
        |value| {
            storage_result(StoredApplicationExportOperationV2::new(
                ApplicationExportOperationId::from_bytes(fixed(value.operation_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                ContractLineage::new(value.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                value.immutable_binding,
                value.canonical_state,
                storage_result(ApplicationExportLedgerPrefixV1::from_canonical_bytes(
                    &value.canonical_prefix,
                ))?,
            ))
        },
    )
}

/// Encodes one append-only page member under its separately registered identity.
pub fn encode_application_export_page_commitment_v1(
    value: &ApplicationExportPageCommitmentV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        APPLICATION_EXPORT_PAGE,
        &wire::StoredApplicationExportPageCommitmentV1 {
            canonical_commitment: value.canonical_bytes().to_vec(),
        },
    )
}

/// Decodes complete bounded page evidence. The repository must also compare
/// `canonical_key` with the physical operation/ordinal key before using it.
pub fn decode_application_export_page_commitment_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ApplicationExportPageCommitmentV1>, DurableCodecError> {
    decode_message::<wire::StoredApplicationExportPageCommitmentV1, _, _>(
        APPLICATION_EXPORT_PAGE,
        encoded,
        |value| {
            storage_result(ApplicationExportPageCommitmentV1::from_canonical_bytes(
                &value.canonical_commitment,
            ))
        },
    )
}

/// Encodes one exact opaque application-export checkpoint or receipt.
pub fn encode_application_export_operation_v1(
    value: &StoredApplicationExportOperationV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        APPLICATION_EXPORT_OPERATION,
        &wire::StoredApplicationExportOperationV1 {
            operation_id: value.operation_id().as_bytes().to_vec(),
            contract_lineage: value.lineage().as_str().to_owned(),
            canonical_state: value.canonical_state().to_vec(),
        },
    )
}

/// Decodes one exact opaque application-export checkpoint or receipt.
pub fn decode_application_export_operation_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredApplicationExportOperationV1>, DurableCodecError> {
    decode_message::<wire::StoredApplicationExportOperationV1, _, _>(
        APPLICATION_EXPORT_OPERATION,
        encoded,
        |value| {
            storage_result(StoredApplicationExportOperationV1::new(
                ApplicationExportOperationId::from_bytes(fixed(value.operation_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                ContractLineage::new(value.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                value.canonical_state,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApplicationExportPageOrdinalV1;
    use riffdb_types::{ApplicationExportClassV1, ApplicationExportPageHash};

    fn operation() -> ApplicationExportOperationId {
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x61; 10]).unwrap()
    }

    fn head(prefix: ApplicationExportLedgerPrefixV1) -> StoredApplicationExportOperationV2 {
        StoredApplicationExportOperationV2::new(
            operation(),
            ContractLineage::new("ExportLedger").unwrap(),
            b"exact-immutable-binding".to_vec(),
            b"exact-canonical-state".to_vec(),
            prefix,
        )
        .unwrap()
    }

    // req: EXP-006, EXP-007, EXP-008, EXP-009
    #[test]
    fn export_ledger_codecs_keep_each_append_charge_constant_and_verify_full_prefix() {
        let genesis =
            ApplicationExportLedgerPrefixV1::genesis(operation(), b"exact-immutable-binding")
                .unwrap();
        let mut prefix = genesis;
        let mut entries = Vec::new();
        let mut page_charge = None;
        let mut head_charge = None;
        for ordinal in 1..=1024 {
            let entry = ApplicationExportPageCommitmentV1::new(
                operation(),
                ApplicationExportPageOrdinalV1::new(ordinal).unwrap(),
                ApplicationExportClassV1::Entity,
                ApplicationExportPageHash::from_bytes([0x45; 32]),
                2,
                40,
            )
            .unwrap();
            let encoded = encode_application_export_page_commitment_v1(&entry).unwrap();
            let decoded = decode_application_export_page_commitment_v1(encoded.as_bytes()).unwrap();
            assert_eq!(decoded.into_parts().0, entry);
            let charge = entry.canonical_key().len() + encoded.as_bytes().len();
            assert_eq!(*page_charge.get_or_insert(charge), charge);
            let previous = head(prefix);
            prefix = prefix.advance(&entry, charge).unwrap();
            let replacement = head(prefix);
            let encoded_head = encode_application_export_operation_v2(&replacement).unwrap();
            let charge_head = operation().as_bytes().len() + encoded_head.as_bytes().len();
            assert_eq!(*head_charge.get_or_insert(charge_head), charge_head);
            replacement
                .validate_transition(
                    Some(&previous),
                    Some((&entry, charge)),
                    charge_head,
                    ordinal as usize * 70 + 2048,
                )
                .unwrap();
            assert_eq!(
                decode_application_export_operation_v2(encoded_head.as_bytes())
                    .unwrap()
                    .into_parts()
                    .0,
                replacement
            );
            entries.push((entry, charge));
            if matches!(ordinal, 16 | 128 | 1024) {
                assert_eq!(prefix.retained_bytes(), ordinal as usize * charge);
                assert_eq!(
                    prefix
                        .verify(genesis, entries.iter().copied())
                        .unwrap()
                        .len(),
                    ordinal as usize
                );
            }
        }
    }

    // req: EXP-006, EXP-007, EXP-009
    #[test]
    fn export_ledger_codec_refuses_foreign_record_roles_and_tampered_binding() {
        let genesis =
            ApplicationExportLedgerPrefixV1::genesis(operation(), b"exact-immutable-binding")
                .unwrap();
        let original = head(genesis);
        let encoded = encode_application_export_operation_v2(&original).unwrap();
        assert!(decode_application_export_operation_v1(encoded.as_bytes()).is_err());
        assert!(decode_application_export_page_commitment_v1(encoded.as_bytes()).is_err());
        let old = StoredApplicationExportOperationV1::new(
            operation(),
            original.lineage().clone(),
            b"legacy-canonical-state".to_vec(),
        )
        .unwrap();
        let old = encode_application_export_operation_v1(&old).unwrap();
        assert!(decode_application_export_operation_v1(old.as_bytes()).is_ok());
        assert!(decode_application_export_operation_v2(old.as_bytes()).is_err());

        let wire = wire::StoredApplicationExportOperationV2 {
            operation_id: operation().as_bytes().to_vec(),
            contract_lineage: original.lineage().as_str().to_owned(),
            immutable_binding: b"foreign-authority".to_vec(),
            canonical_state: original.canonical_state().to_vec(),
            canonical_prefix: genesis.canonical_bytes().unwrap().to_vec(),
        };
        // A valid outer envelope does not make a substituted genesis valid.
        let tampered = encode_message(COMPACT_APPLICATION_EXPORT_OPERATION, &wire).unwrap();
        assert!(decode_application_export_operation_v2(tampered.as_bytes()).is_err());
        for length in 0..encoded.as_bytes().len() {
            assert!(decode_application_export_operation_v2(&encoded.as_bytes()[..length]).is_err());
        }
        let mut bad = encoded.into_bytes();
        bad.push(0);
        assert!(decode_application_export_operation_v2(&bad).is_err());
    }
}
