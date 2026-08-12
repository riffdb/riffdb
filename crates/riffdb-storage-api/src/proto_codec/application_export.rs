//! Durable codec for opaque exact symbolic application-export state.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{ApplicationExportOperationId, ContractLineage};

use crate::{EncodedPageItem, StoredApplicationExportOperationV1};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed,
    storage_result,
};

const APPLICATION_EXPORT_OPERATION: &str = "riffdb.storage.v1.StoredApplicationExportOperationV1";

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
