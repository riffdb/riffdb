//! Canonical purpose-specific durable key builders.

use std::fmt;

use crate::{
    AggregateTypeId, ConflictKey, Date, EntityKey, EntityTypeId, EnumVariantId, IndexEntryKey,
    IndexId, PartitionKey, Timestamp,
    limits::MAX_KEY_BYTES,
    {
        CONFLICT_KEY_V1_PREFIX, ENTITY_KEY_V1_PREFIX, INDEX_ENTRY_KEY_V1_PREFIX,
        PARTITION_KEY_V1_PREFIX,
    },
};

macro_rules! key_component_methods {
    () => {
        /// Appends a canonical Boolean component.
        pub fn push_bool(&mut self, value: bool) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_fixed(&[u8::from(value)])?;
            Ok(self)
        }

        /// Appends an unsigned 32-bit ordered component.
        pub fn push_u32(&mut self, value: u32) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_fixed(&value.to_be_bytes())?;
            Ok(self)
        }

        /// Appends an unsigned 64-bit ordered component.
        pub fn push_u64(&mut self, value: u64) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_fixed(&value.to_be_bytes())?;
            Ok(self)
        }

        /// Appends a signed 32-bit component whose bytes preserve numeric order.
        pub fn push_i32(&mut self, value: i32) -> Result<&mut Self, KeyEncodingError> {
            let mut bytes = value.to_be_bytes();
            bytes[0] ^= 0x80;
            self.0.push_fixed(&bytes)?;
            Ok(self)
        }

        /// Appends a signed 64-bit component whose bytes preserve numeric order.
        pub fn push_i64(&mut self, value: i64) -> Result<&mut Self, KeyEncodingError> {
            let mut bytes = value.to_be_bytes();
            bytes[0] ^= 0x80;
            self.0.push_fixed(&bytes)?;
            Ok(self)
        }

        /// Appends a canonical timestamp component.
        pub fn push_timestamp(&mut self, value: Timestamp) -> Result<&mut Self, KeyEncodingError> {
            let mut seconds = value.seconds().to_be_bytes();
            seconds[0] ^= 0x80;
            let nanoseconds = value.nanoseconds().to_be_bytes();
            let mut bytes = [0; 12];
            bytes[..8].copy_from_slice(&seconds);
            bytes[8..].copy_from_slice(&nanoseconds);
            self.0.push_fixed(&bytes)?;
            Ok(self)
        }

        /// Appends a canonical date component.
        pub fn push_date(&mut self, value: Date) -> Result<&mut Self, KeyEncodingError> {
            let mut bytes = value.days_since_unix_epoch().to_be_bytes();
            bytes[0] ^= 0x80;
            self.0.push_fixed(&bytes)?;
            Ok(self)
        }

        /// Appends a canonical declared-enum variant component.
        pub fn push_enum_variant(
            &mut self,
            value: EnumVariantId,
        ) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_fixed(&value.to_be_bytes())?;
            Ok(self)
        }

        /// Appends fixed-width UUID network-order bytes.
        pub fn push_uuid(&mut self, value: &[u8; 16]) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_fixed(value)?;
            Ok(self)
        }

        /// Appends a length-prefixed exact byte component.
        pub fn push_bytes(&mut self, value: &[u8]) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_bytes(value)?;
            Ok(self)
        }

        /// Appends a length-prefixed exact UTF-8 component without normalization.
        pub fn push_str(&mut self, value: &str) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_bytes(value.as_bytes())?;
            Ok(self)
        }

        /// Appends zero-escaped bytes whose encoded order preserves byte order.
        pub fn push_ordered_bytes(&mut self, value: &[u8]) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_ordered_bytes(value, true)?;
            Ok(self)
        }

        /// Appends the non-terminated prefix of one ordered-byte component.
        pub fn push_ordered_bytes_prefix(
            &mut self,
            value: &[u8],
        ) -> Result<&mut Self, KeyEncodingError> {
            self.0.push_ordered_bytes(value, false)?;
            Ok(self)
        }

        /// Returns the currently encoded prefix and components.
        pub fn as_bytes(&self) -> &[u8] {
            &self.0.bytes
        }
    };
}

/// Builder for a v1 canonical entity key.
#[derive(Clone)]
pub struct EntityKeyBuilder(KeyBuilder);

impl EntityKeyBuilder {
    /// Starts an entity key with its purpose namespace, version, and entity type.
    pub fn new(entity_type: EntityTypeId) -> Self {
        Self(KeyBuilder::new(
            &ENTITY_KEY_V1_PREFIX,
            entity_type.get().to_be_bytes(),
        ))
    }

    key_component_methods!();

    /// Finishes the bounded canonical entity key.
    pub fn finish(self) -> Result<EntityKey, KeyEncodingError> {
        Ok(EntityKey::from_validated_bytes(self.0.bytes))
    }
}

impl fmt::Debug for EntityKeyBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntityKeyBuilder")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.bytes.len())
            .finish()
    }
}

/// Builder for a v1 canonical conflict key.
#[derive(Clone)]
pub struct ConflictKeyBuilder(KeyBuilder);

impl ConflictKeyBuilder {
    /// Starts a conflict key with its purpose namespace, version, and aggregate type.
    pub fn new(aggregate_type: AggregateTypeId) -> Self {
        Self(KeyBuilder::new(
            &CONFLICT_KEY_V1_PREFIX,
            aggregate_type.get().to_be_bytes(),
        ))
    }

    key_component_methods!();

    /// Finishes the bounded canonical conflict key.
    pub fn finish(self) -> Result<ConflictKey, KeyEncodingError> {
        Ok(ConflictKey::from_validated_bytes(self.0.bytes))
    }
}

impl fmt::Debug for ConflictKeyBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConflictKeyBuilder")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.bytes.len())
            .finish()
    }
}

/// Builder for a v1 canonical logical partition key.
#[derive(Clone)]
pub struct PartitionKeyBuilder(KeyBuilder);

impl PartitionKeyBuilder {
    /// Starts a partition key with its purpose namespace, version, and aggregate type.
    pub fn new(aggregate_type: AggregateTypeId) -> Self {
        Self(KeyBuilder::new(
            &PARTITION_KEY_V1_PREFIX,
            aggregate_type.get().to_be_bytes(),
        ))
    }

    key_component_methods!();

    /// Finishes the bounded canonical logical partition key.
    pub fn finish(self) -> Result<PartitionKey, KeyEncodingError> {
        Ok(PartitionKey::from_validated_bytes(self.0.bytes))
    }
}

impl fmt::Debug for PartitionKeyBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionKeyBuilder")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.bytes.len())
            .finish()
    }
}

/// Builder for a complete v1 canonical local-index entry key.
#[derive(Clone)]
pub struct IndexEntryKeyBuilder(KeyBuilder);

impl IndexEntryKeyBuilder {
    /// Starts an index entry key with its purpose namespace, version, and index ID.
    pub fn new(index: IndexId) -> Self {
        Self(KeyBuilder::new(
            &INDEX_ENTRY_KEY_V1_PREFIX,
            index.get().to_be_bytes(),
        ))
    }

    key_component_methods!();

    /// Appends the length-delimited complete entity key and finishes the index key.
    pub fn finish(mut self, entity_key: EntityKey) -> Result<IndexEntryKey, KeyEncodingError> {
        self.0.push_bytes(entity_key.as_bytes())?;
        Ok(IndexEntryKey::from_validated_bytes(self.0.bytes))
    }
}

impl fmt::Debug for IndexEntryKeyBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexEntryKeyBuilder")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.bytes.len())
            .finish()
    }
}

#[derive(Clone)]
struct KeyBuilder {
    bytes: Vec<u8>,
}

impl KeyBuilder {
    fn new(prefix: &[u8], type_id: [u8; 4]) -> Self {
        let mut bytes = Vec::with_capacity(prefix.len() + type_id.len());
        bytes.extend_from_slice(prefix);
        bytes.extend_from_slice(&type_id);
        Self { bytes }
    }

    fn push_fixed(&mut self, bytes: &[u8]) -> Result<(), KeyEncodingError> {
        self.ensure_additional(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), KeyEncodingError> {
        let length =
            u32::try_from(bytes.len()).map_err(|_| KeyEncodingError::ComponentTooLong {
                actual: bytes.len(),
                maximum: u32::MAX as usize,
            })?;
        let additional = 4usize
            .checked_add(bytes.len())
            .ok_or(KeyEncodingError::TooLong {
                actual: usize::MAX,
                maximum: MAX_KEY_BYTES,
            })?;
        self.ensure_additional(additional)?;
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn push_ordered_bytes(
        &mut self,
        bytes: &[u8],
        terminated: bool,
    ) -> Result<(), KeyEncodingError> {
        let zeros = bytes.iter().filter(|byte| **byte == 0).count();
        let additional = bytes
            .len()
            .checked_add(zeros)
            .and_then(|value| value.checked_add(if terminated { 2 } else { 0 }))
            .ok_or(KeyEncodingError::TooLong {
                actual: usize::MAX,
                maximum: MAX_KEY_BYTES,
            })?;
        self.ensure_additional(additional)?;
        for byte in bytes {
            if *byte == 0 {
                self.bytes.extend_from_slice(&[0, 0xff]);
            } else {
                self.bytes.push(*byte);
            }
        }
        if terminated {
            self.bytes.extend_from_slice(&[0, 0]);
        }
        Ok(())
    }

    fn ensure_additional(&self, additional: usize) -> Result<(), KeyEncodingError> {
        let actual = self
            .bytes
            .len()
            .checked_add(additional)
            .ok_or(KeyEncodingError::TooLong {
                actual: usize::MAX,
                maximum: MAX_KEY_BYTES,
            })?;
        if actual > MAX_KEY_BYTES {
            return Err(KeyEncodingError::TooLong {
                actual,
                maximum: MAX_KEY_BYTES,
            });
        }
        Ok(())
    }
}

/// A bounded key-encoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyEncodingError {
    /// The complete key would exceed the 4 KiB hard limit.
    TooLong {
        /// Attempted encoded key size.
        actual: usize,
        /// Maximum encoded key size.
        maximum: usize,
    },
    /// A variable component cannot be represented by its u32 length prefix.
    ComponentTooLong {
        /// Component byte length.
        actual: usize,
        /// Maximum representable component byte length.
        maximum: usize,
    },
}

impl fmt::Display for KeyEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { actual, maximum } => {
                write!(
                    formatter,
                    "key would have {actual} bytes; maximum is {maximum}"
                )
            }
            Self::ComponentTooLong { actual, maximum } => write!(
                formatter,
                "key component has {actual} bytes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for KeyEncodingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_identity_is_part_of_the_immutable_prefix() {
        assert_eq!(
            EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304).expect("nonzero")).as_bytes(),
            &[0x45, 0x01, 0x01, 0x02, 0x03, 0x04]
        );
        assert_eq!(
            ConflictKeyBuilder::new(AggregateTypeId::new(0x0506_0708).expect("nonzero")).as_bytes(),
            &[0x43, 0x01, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(
            PartitionKeyBuilder::new(AggregateTypeId::new(0x090a_0b0c).expect("nonzero"))
                .as_bytes(),
            &[0x50, 0x01, 0x09, 0x0a, 0x0b, 0x0c]
        );
        assert_eq!(
            IndexEntryKeyBuilder::new(IndexId::new(0x0d0e_0f10).expect("nonzero")).as_bytes(),
            &[0x49, 0x01, 0x0d, 0x0e, 0x0f, 0x10]
        );
    }

    #[test]
    fn failed_append_does_not_mutate_builder() {
        let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
        let before = builder.as_bytes().to_vec();
        assert!(builder.push_bytes(&vec![0; MAX_KEY_BYTES]).is_err());
        assert_eq!(builder.as_bytes(), before);
    }
}
