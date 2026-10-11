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
const RECORD_REGISTRY_V3: &str = "riffdb.storage.v1.StoredRecordRegistryV3";
const HISTORY_INCARNATION: &str = "riffdb.storage.v1.StoredHistoryIncarnationV1";
const CHANGELOG_ALLOCATOR: &str = "riffdb.storage.v1.StoredChangelogTransactionAllocatorV3";

/// Encodes physical transaction allocation with its own closed durable identity.
/// The returned complete envelope, not a bare counter, is the journal mutation value.
pub fn encode_changelog_transaction_allocator_v3(
    value: crate::ChangelogTransactionAllocator,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_changelog_transaction_allocator_v3::{Exhausted, State};
    let state = match value {
        crate::ChangelogTransactionAllocator::Next(sequence) => {
            State::NextTransactionSequence(sequence.get())
        }
        crate::ChangelogTransactionAllocator::Exhausted => State::Exhausted(Exhausted {}),
    };
    encode_message(
        CHANGELOG_ALLOCATOR,
        &wire::StoredChangelogTransactionAllocatorV3 { state: Some(state) },
    )
}

/// Decodes nonzero Next or explicit Exhausted, refusing missing/foreign states.
pub fn decode_changelog_transaction_allocator_v3(
    encoded: &[u8],
) -> Result<EncodedPageItem<crate::ChangelogTransactionAllocator>, DurableCodecError> {
    use wire::stored_changelog_transaction_allocator_v3::State;
    decode_message::<wire::StoredChangelogTransactionAllocatorV3, _, _>(
        CHANGELOG_ALLOCATOR,
        encoded,
        |value| match require(value.state)? {
            State::NextTransactionSequence(value) => {
                crate::ChangelogTransactionSequence::new(value)
                    .map(crate::ChangelogTransactionAllocator::Next)
                    .ok_or_else(DurableCodecError::corrupt)
            }
            State::Exhausted(_) => Ok(crate::ChangelogTransactionAllocator::Exhausted),
        },
    )
}

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

/// Decoded registry metadata; completion is a durable binding, never a writer
/// capability. Only the backend's complete-validation owner may publish it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredRecordRegistry {
    digest: SchemaHash,
    validation: RecordRegistryValidation,
}

/// Validation state preserved exactly by the versioned registry codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordRegistryValidation {
    /// Frozen V2 bytes contain no independent completion witness.
    Legacy,
    /// Current-format validation has not completed.
    Pending,
    /// Complete native validation for this exact database and incarnation.
    Completed {
        /// Permanent database identity.
        database_id: DatabaseId,
        /// Nonzero history incarnation.
        history_incarnation: u64,
    },
}

impl StoredRecordRegistry {
    /// Constructs pending successor metadata without granting any shortcut.
    #[must_use]
    pub const fn pending(digest: SchemaHash) -> Self {
        Self {
            digest,
            validation: RecordRegistryValidation::Pending,
        }
    }

    /// Constructs completion metadata for the native proof owner's receipt.
    /// This value provides no capability to mutate storage or activate ports.
    pub fn completed(
        digest: SchemaHash,
        database_id: DatabaseId,
        history_incarnation: u64,
    ) -> Result<Self, DurableCodecError> {
        if history_incarnation < crate::HISTORY_INCARNATION_INITIAL {
            return Err(DurableCodecError::corrupt());
        }
        Ok(Self {
            digest,
            validation: RecordRegistryValidation::Completed {
                database_id,
                history_incarnation,
            },
        })
    }

    /// Exact readable registry identity.
    #[must_use]
    pub const fn digest(self) -> SchemaHash {
        self.digest
    }

    /// Explicit durable validation state.
    #[must_use]
    pub const fn validation(self) -> RecordRegistryValidation {
        self.validation
    }

    /// Requires every completion binding; legacy and pending never match.
    #[must_use]
    pub fn completed_for(
        self,
        database_id: DatabaseId,
        incarnation: u64,
        digest: SchemaHash,
    ) -> bool {
        self.digest == digest
            && self.validation
                == RecordRegistryValidation::Completed {
                    database_id,
                    history_incarnation: incarnation,
                }
    }
}

/// Encodes successor metadata within its compiled 256-byte envelope ceiling.
pub fn encode_record_registry_v3(
    value: StoredRecordRegistry,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_record_registry_v3::{Completed, Pending, State};
    let state = match value.validation {
        RecordRegistryValidation::Legacy => return Err(DurableCodecError::invariant()),
        RecordRegistryValidation::Pending => State::Pending(Pending {}),
        RecordRegistryValidation::Completed {
            database_id,
            history_incarnation,
        } => State::Completed(Completed {
            database_id: database_id.as_bytes().to_vec(),
            history_incarnation,
            registry_digest: value.digest.as_bytes().to_vec(),
        }),
    };
    encode_message(
        RECORD_REGISTRY_V3,
        &wire::StoredRecordRegistryV3 {
            registry_digest: value.digest.as_bytes().to_vec(),
            state: Some(state),
        },
    )
}

/// Decodes both registry versions into the single semantic metadata type.
pub fn decode_record_registry(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRecordRegistry>, DurableCodecError> {
    use prost::Message;
    use wire::stored_record_registry_v3::State;
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let value = match decoded.record_type() {
        RECORD_REGISTRY => {
            let value = wire::StoredRecordRegistryV2::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            StoredRecordRegistry {
                digest: SchemaHash::from_bytes(fixed(value.registry_digest)?),
                validation: RecordRegistryValidation::Legacy,
            }
        }
        RECORD_REGISTRY_V3 => {
            let value = wire::StoredRecordRegistryV3::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            let digest = SchemaHash::from_bytes(fixed(value.registry_digest)?);
            match require(value.state)? {
                State::Pending(_) => StoredRecordRegistry::pending(digest),
                State::Completed(binding) => {
                    if SchemaHash::from_bytes(fixed(binding.registry_digest)?) != digest {
                        return Err(DurableCodecError::corrupt());
                    }
                    StoredRecordRegistry::completed(
                        digest,
                        DatabaseId::from_bytes(fixed(binding.database_id)?)
                            .map_err(|_| DurableCodecError::corrupt())?,
                        binding.history_incarnation,
                    )?
                }
            }
        }
        _ => return Err(DurableCodecError::corrupt()),
    };
    let charge =
        crate::EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

/// Reads only the digest through the shared versioned semantic decoder.
pub fn decode_record_registry_digest(
    encoded: &[u8],
) -> Result<EncodedPageItem<SchemaHash>, DurableCodecError> {
    let (registry, charge) = decode_record_registry(encoded)?.into_parts();
    Ok(EncodedPageItem::new(registry.digest(), charge))
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

#[cfg(test)]
mod registry_completion_tests {
    use super::*;
    use crate::transcode_durable_record_to_v1;
    use riffdb_proto::{durable::*, envelope::*};

    // req: REC-005, REC-006, REC-007, STO-032
    #[test]
    fn registry_completion_codec_preserves_legacy_and_exact_successor_bindings() {
        let digest = current_record_registry_digest();
        let mut bytes = [0x31; 16];
        bytes[6] = 0x71;
        bytes[8] = 0x81;
        let database = DatabaseId::from_bytes(bytes).unwrap();
        bytes[15] = 0x32;
        let other = DatabaseId::from_bytes(bytes).unwrap();
        let legacy = encode_record_registry_v2(digest).unwrap();
        let parsed = *decode_record_registry(legacy.as_bytes()).unwrap().value();
        assert_eq!(parsed.validation(), RecordRegistryValidation::Legacy);
        assert!(!parsed.completed_for(database, 1, digest));
        assert_eq!(encode_record_registry_v2(parsed.digest()).unwrap(), legacy);

        for registry in [
            StoredRecordRegistry::pending(digest),
            StoredRecordRegistry::completed(digest, database, 1).unwrap(),
            StoredRecordRegistry::completed(digest, database, u64::MAX).unwrap(),
        ] {
            let encoded = encode_record_registry_v3(registry).unwrap();
            assert!(encoded.as_bytes().len() <= 256);
            assert_eq!(
                *decode_record_registry(encoded.as_bytes()).unwrap().value(),
                registry
            );
            assert_eq!(
                decode_record_registry_digest(encoded.as_bytes())
                    .unwrap()
                    .value(),
                &digest
            );
            assert!(!registry.completed_for(other, 1, digest));
            assert!(!registry.completed_for(database, 1, SchemaHash::from_bytes([0; 32])));
            let predecessor = RecordRegistry::new(&READABLE_RECORD_SCHEMAS[..115]).unwrap();
            assert!(predecessor.decode(encoded.as_bytes()).is_err());
            let legacy_framed = transcode_durable_record_to_v1(encoded.as_bytes()).unwrap();
            assert!(legacy_framed.as_bytes().len() <= 256);
            assert_eq!(
                *decode_record_registry(legacy_framed.as_bytes())
                    .unwrap()
                    .value(),
                registry
            );
        }
        assert!(StoredRecordRegistry::completed(digest, database, 0).is_err());
        assert_eq!(
            current_record_schema(RECORD_REGISTRY_V3)
                .unwrap()
                .max_envelope_bytes(),
            256
        );
    }

    // req: REC-005, REC-006, REC-007, STO-032
    #[test]
    fn registry_completion_codec_refuses_missing_and_inconsistent_state() {
        use wire::stored_record_registry_v3::{Completed, State};
        let digest = current_record_registry_digest();
        let mut database_bytes = vec![0x31; 16];
        database_bytes[6] = 0x71;
        database_bytes[8] = 0x81;
        let completed = Completed {
            database_id: database_bytes,
            history_incarnation: 1,
            registry_digest: digest.as_bytes().to_vec(),
        };
        let mut states = vec![None];
        let mut wrong_digest = completed.clone();
        wrong_digest.registry_digest = vec![0; 32];
        states.push(Some(State::Completed(wrong_digest)));
        let mut zero_incarnation = completed.clone();
        zero_incarnation.history_incarnation = 0;
        states.push(Some(State::Completed(zero_incarnation)));
        let mut nil_database = completed;
        nil_database.database_id = vec![0; 16];
        states.push(Some(State::Completed(nil_database)));
        for state in states {
            let message = wire::StoredRecordRegistryV3 {
                registry_digest: digest.as_bytes().to_vec(),
                state,
            };
            let bytes = encode_current_message(&message).unwrap();
            assert!(decode_record_registry(&bytes).is_err());
        }
        let valid = encode_record_registry_v3(StoredRecordRegistry::pending(digest)).unwrap();
        let mut corrupt = valid.as_bytes().to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_record_registry(&corrupt).is_err());
        assert!(decode_record_registry(&[0; 257]).is_err());
    }

    // req: REC-005, REC-006, REC-007, STO-032
    #[test]
    fn registry_completion_successor_wire_vectors_are_exact() {
        use std::fmt::Write as _;
        let digest = SchemaHash::from_bytes([0x42; 32]);
        let mut bytes = [0x31; 16];
        bytes[6] = 0x71;
        bytes[8] = 0x81;
        let database = DatabaseId::from_bytes(bytes).unwrap();
        let records = [
            ("legacy-v2", encode_record_registry_v2(digest).unwrap()),
            (
                "pending-v3",
                encode_record_registry_v3(StoredRecordRegistry::pending(digest)).unwrap(),
            ),
            (
                "completed-v3",
                encode_record_registry_v3(
                    StoredRecordRegistry::completed(digest, database, 1).unwrap(),
                )
                .unwrap(),
            ),
        ];
        let mut output = String::from("riffdb-registry-completion-v3-wire-vectors\nrecords\t3\n");
        for (name, record) in records {
            write!(output, "{name}\t").unwrap();
            for byte in record.as_bytes() {
                write!(output, "{byte:02x}").unwrap();
            }
            output.push('\n');
        }
        if let Some(path) = std::env::var_os("RIFFDB_REGISTRY_COMPLETION_V3_VECTOR_OUTPUT") {
            std::fs::write(path, output).unwrap();
        } else {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/proto/durable-registry-v3-wire-vectors.txt");
            assert_eq!(std::fs::read_to_string(path).unwrap(), output);
        }
    }
}
