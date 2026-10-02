//! Sealed destination-only schema; never a primary database record role.

use prost::Message;
use riffdb_types::SchemaHash;

use crate::durable_wire::DurablePreflightError;
use crate::envelope::{PayloadValidationError, RecordRegistry, RecordRegistryError, RecordSchema};
use crate::storage::v1::LocalOutboxArchiveRecordV1;

const BOUNDS: [u8; 8] =
    *include_bytes!("../fixtures/durable-local-outbox-archive-v1-record-bound.bin");
const SCHEMAS: [RecordSchema<'static>; 1] = [RecordSchema::new_current(
    "riffdb.storage.v1.LocalOutboxArchiveRecordV1",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-local-outbox-archive-v1-schema-hash.bin"
    )),
    u32::from_be_bytes([BOUNDS[0], BOUNDS[1], BOUNDS[2], BOUNDS[3]]) as usize,
    u32::from_be_bytes([BOUNDS[4], BOUNDS[5], BOUNDS[6], BOUNDS[7]]) as usize,
    preflight,
    validate,
)
.with_compact_identity(255, 1)];

/// The generated archive schema in its separate destination namespace.
#[must_use]
pub const fn local_outbox_archive_record_schema_v1() -> &'static RecordSchema<'static> {
    &SCHEMAS[0]
}

/// Closed registry for this archive format; primary readers never use it.
pub fn local_outbox_archive_record_registry_v1()
-> Result<RecordRegistry<'static>, RecordRegistryError> {
    RecordRegistry::new(&SCHEMAS)
}

fn preflight(payload: &[u8]) -> Result<(), PayloadValidationError> {
    crate::durable_wire::local_outbox_archive(payload).map_err(|error| match error {
        DurablePreflightError::Malformed => PayloadValidationError::Malformed,
        DurablePreflightError::NonCanonical => PayloadValidationError::NonCanonical,
        DurablePreflightError::LimitExceeded => PayloadValidationError::LimitExceeded,
    })
}

fn validate(payload: &[u8]) -> Result<(), PayloadValidationError> {
    preflight(payload)?;
    let value = LocalOutboxArchiveRecordV1::decode(payload)
        .map_err(|_| PayloadValidationError::Malformed)?;
    if !crate::envelope::is_canonical_encoding(&value, payload) {
        return Err(PayloadValidationError::NonCanonical);
    }
    Ok(())
}
