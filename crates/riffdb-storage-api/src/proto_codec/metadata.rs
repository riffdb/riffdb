use riffdb_proto::storage::v1 as wire;
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId};

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
