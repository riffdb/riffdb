//! Canonical purpose-specific durable key builders.

use std::fmt;

use crate::{
    AggregateTypeId, ConflictKey, EntityKey, EntityTypeId,
    limits::MAX_KEY_BYTES,
    {CONFLICT_KEY_V1_PREFIX, ENTITY_KEY_V1_PREFIX},
};

macro_rules! key_component_methods {
    () => {
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
            EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304)).as_bytes(),
            &[0x45, 0x01, 0x01, 0x02, 0x03, 0x04]
        );
        assert_eq!(
            ConflictKeyBuilder::new(AggregateTypeId::new(0x0506_0708)).as_bytes(),
            &[0x43, 0x01, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn failed_append_does_not_mutate_builder() {
        let mut builder = EntityKeyBuilder::new(EntityTypeId::new(1));
        let before = builder.as_bytes().to_vec();
        assert!(builder.push_bytes(&vec![0; MAX_KEY_BYTES]).is_err());
        assert_eq!(builder.as_bytes(), before);
    }
}
