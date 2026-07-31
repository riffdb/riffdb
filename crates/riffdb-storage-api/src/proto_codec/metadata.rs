use riffdb_proto::storage::v1 as wire;
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, SchemaHash};

use crate::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, EncodedPageItem,
    StorageFormatVersion,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed, require,
};

const FORMAT: &str = "riffdb.storage.v1.StoredStorageFormatVersionV1";
const DATABASE: &str = "riffdb.storage.v1.StoredDatabaseIdentityV1";
pub(super) const APPLICATION: &str = "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1";
const ADMINISTRATION: &str = "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1";
const RECORD_REGISTRY: &str = "riffdb.storage.v1.StoredRecordRegistryV2";
const HISTORY_INCARNATION: &str = "riffdb.storage.v1.StoredHistoryIncarnationV1";

/// Returns the exact registry digest required by current storage-format V2.
#[must_use]
pub fn current_record_registry_digest() -> SchemaHash {
    riffdb_proto::durable::record_registry_digest()
}

/// Encodes the supported durable storage-format metadata record.
pub fn encode_storage_format_version_v1(
    value: StorageFormatVersion,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        FORMAT,
        &wire::StoredStorageFormatVersionV1 {
            storage_format_version: value.get(),
        },
    )
}

/// Decodes the supported durable storage-format metadata record.
pub fn decode_storage_format_version_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StorageFormatVersion>, DurableCodecError> {
    decode_message::<wire::StoredStorageFormatVersionV1, _, _>(FORMAT, encoded, |value| {
        StorageFormatVersion::from_supported(value.storage_format_version)
            .ok_or_else(DurableCodecError::corrupt)
    })
}

/// Encodes the exact compact durable-record registry digest.
pub fn encode_record_registry_v2(
    value: SchemaHash,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        RECORD_REGISTRY,
        &wire::StoredRecordRegistryV2 {
            registry_digest: value.as_bytes().to_vec(),
        },
    )
}

/// Decodes the exact compact durable-record registry digest.
pub fn decode_record_registry_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<SchemaHash>, DurableCodecError> {
    decode_message::<wire::StoredRecordRegistryV2, _, _>(RECORD_REGISTRY, encoded, |value| {
        Ok(SchemaHash::from_bytes(fixed(value.registry_digest)?))
    })
}

/// Encodes the permanent database identity metadata record.
pub fn encode_database_identity_v1(
    value: DatabaseId,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        DATABASE,
        &wire::StoredDatabaseIdentityV1 {
            database_id: value.as_bytes().to_vec(),
        },
    )
}

/// Decodes the permanent database identity metadata record.
pub fn decode_database_identity_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<DatabaseId>, DurableCodecError> {
    decode_message::<wire::StoredDatabaseIdentityV1, _, _>(DATABASE, encoded, |value| {
        DatabaseId::from_bytes(fixed(value.database_id)?).map_err(|_| DurableCodecError::corrupt())
    })
}

/// Encodes the application-sequence allocator metadata record.
pub fn encode_application_sequence_allocator_v1(
    value: ApplicationSequenceAllocator,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_application_sequence_allocator_v1::State;
    let state = match value {
        ApplicationSequenceAllocator::Next(sequence) => State::NextCommitSequence(sequence.get()),
        ApplicationSequenceAllocator::Exhausted => State::Exhausted(wire::UnitV1 {}),
    };
    encode_message(
        APPLICATION,
        &wire::StoredApplicationSequenceAllocatorV1 { state: Some(state) },
    )
}

/// Decodes the application-sequence allocator metadata record.
pub fn decode_application_sequence_allocator_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ApplicationSequenceAllocator>, DurableCodecError> {
    use wire::stored_application_sequence_allocator_v1::State;
    decode_message::<wire::StoredApplicationSequenceAllocatorV1, _, _>(
        APPLICATION,
        encoded,
        |value| match require(value.state)? {
            State::NextCommitSequence(value) => CommitSequence::new(value)
                .map(ApplicationSequenceAllocator::next)
                .ok_or_else(DurableCodecError::corrupt),
            State::Exhausted(_) => Ok(ApplicationSequenceAllocator::Exhausted),
        },
    )
}

/// Encodes the administration-sequence allocator metadata record.
pub fn encode_administration_sequence_allocator_v1(
    value: AdministrationSequenceAllocator,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_administration_sequence_allocator_v1::State;
    let state = match value {
        AdministrationSequenceAllocator::Next(sequence) => {
            State::NextAdministrationSequence(sequence.get())
        }
        AdministrationSequenceAllocator::Exhausted => State::Exhausted(wire::UnitV1 {}),
    };
    encode_message(
        ADMINISTRATION,
        &wire::StoredAdministrationSequenceAllocatorV1 { state: Some(state) },
    )
}

/// Decodes the administration-sequence allocator metadata record.
pub fn decode_administration_sequence_allocator_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<AdministrationSequenceAllocator>, DurableCodecError> {
    use wire::stored_administration_sequence_allocator_v1::State;
    decode_message::<wire::StoredAdministrationSequenceAllocatorV1, _, _>(
        ADMINISTRATION,
        encoded,
        |value| match require(value.state)? {
            State::NextAdministrationSequence(value) => AdministrationSequence::new(value)
                .map(AdministrationSequenceAllocator::next)
                .ok_or_else(DurableCodecError::corrupt),
            State::Exhausted(_) => Ok(AdministrationSequenceAllocator::Exhausted),
        },
    )
}

/// Encodes the durable history-incarnation metadata record.
pub fn encode_history_incarnation_v1(
    incarnation: u64,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if incarnation < crate::HISTORY_INCARNATION_INITIAL {
        return Err(DurableCodecError::corrupt());
    }
    encode_message(
        HISTORY_INCARNATION,
        &wire::StoredHistoryIncarnationV1 { incarnation },
    )
}

/// Decodes the durable history-incarnation metadata record.
pub fn decode_history_incarnation_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<u64>, DurableCodecError> {
    decode_message::<wire::StoredHistoryIncarnationV1, _, _>(
        HISTORY_INCARNATION,
        encoded,
        |value| {
            if value.incarnation < crate::HISTORY_INCARNATION_INITIAL {
                return Err(DurableCodecError::corrupt());
            }
            Ok(value.incarnation)
        },
    )
}
