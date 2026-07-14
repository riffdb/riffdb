//! Schema-directed canonical entity, partition, conflict, and index keys.

use std::fmt;

use riffdb_types::{
    AggregateTypeId, CanonicalBytes, CanonicalString, CanonicalValue, ConflictKey,
    ConflictKeyBuilder, Date, EntityKey, EntityKeyBuilder, EntityTypeId, EnumVariantId,
    IndexEntryKey, IndexEntryKeyBuilder, IndexId, MAX_KEY_BYTES, PartitionKey, PartitionKeyBuilder,
    Timestamp,
};

use crate::{IrValidationError, ValueType, ValueTypeTag, checked_len};

/// Immutable v1 key codec version.
pub const KEY_CODEC_VERSION_V1: u32 = 1;

/// A closed authoritative key purpose and stable owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyPurpose {
    /// Entity primary key.
    Entity(EntityTypeId),
    /// One aggregate's logical partition.
    Partition(AggregateTypeId),
    /// One aggregate's exclusive conflict domain.
    Conflict(AggregateTypeId),
    /// Local secondary index entry.
    Index {
        /// Stable index ID.
        index_id: IndexId,
        /// Owning entity ID.
        entity_type: EntityTypeId,
    },
}

impl KeyPurpose {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Entity(_) => crate::format_registry::key_purpose::ENTITY,
            Self::Partition(_) => crate::format_registry::key_purpose::PARTITION,
            Self::Conflict(_) => crate::format_registry::key_purpose::CONFLICT,
            Self::Index { .. } => crate::format_registry::key_purpose::INDEX,
        }
    }
}

/// One exact closed-registry key component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyComponentSchema {
    value_type: ValueType,
    enum_variants: Vec<EnumVariantId>,
    maximum_payload_bytes: usize,
}

impl KeyComponentSchema {
    /// Creates a checked component. Enum variants are required only for enum types.
    pub fn new(
        value_type: ValueType,
        mut enum_variants: Vec<EnumVariantId>,
    ) -> Result<Self, IrValidationError> {
        if !value_type.is_authoritative_key_scalar() {
            return Err(IrValidationError::InvalidKey {
                reason: "type is not a v1 authoritative key scalar",
            });
        }
        enum_variants.sort_unstable();
        if enum_variants.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "key enum variants",
            });
        }
        if value_type.tag() == ValueTypeTag::Enum {
            if enum_variants.is_empty() {
                return Err(IrValidationError::Empty {
                    kind: "key enum variants",
                });
            }
        } else if !enum_variants.is_empty() {
            return Err(IrValidationError::InvalidKey {
                reason: "non-enum key component carries enum variants",
            });
        }
        let maximum_payload_bytes = authoritative_component_maximum(&value_type)?;
        Ok(Self {
            value_type,
            enum_variants,
            maximum_payload_bytes,
        })
    }

    /// Component type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Canonically ordered allowed variants for an enum component.
    #[must_use]
    pub fn enum_variants(&self) -> &[EnumVariantId] {
        &self.enum_variants
    }

    /// Maximum encoded component payload length, excluding the six-byte envelope.
    #[must_use]
    pub const fn maximum_payload_bytes(&self) -> usize {
        self.maximum_payload_bytes
    }
}

/// A fully checked schema-directed authoritative key codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeySchema {
    purpose: KeyPurpose,
    components: Vec<KeyComponentSchema>,
    entity_key_schema: Option<Box<KeySchema>>,
    maximum_encoded_bytes: usize,
}

impl KeySchema {
    /// Creates an entity, partition, or conflict schema.
    pub fn new(
        purpose: KeyPurpose,
        components: Vec<KeyComponentSchema>,
    ) -> Result<Self, IrValidationError> {
        if matches!(purpose, KeyPurpose::Index { .. }) {
            return Err(IrValidationError::InvalidKey {
                reason: "index key schema requires its owning entity key schema",
            });
        }
        if components.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "key components",
            });
        }
        if matches!(purpose, KeyPurpose::Partition(_)) && components.len() != 1 {
            return Err(IrValidationError::InvalidKey {
                reason: "partition key schema must contain exactly one component",
            });
        }
        checked_len("key components", components.len(), 1_024)?;
        let maximum_encoded_bytes = complete_key_maximum(&components, None)?;
        Ok(Self {
            purpose,
            components,
            entity_key_schema: None,
            maximum_encoded_bytes,
        })
    }

    /// Creates a complete local-index entry schema.
    pub fn index(
        index_id: IndexId,
        entity_type: EntityTypeId,
        components: Vec<KeyComponentSchema>,
        entity_key_schema: KeySchema,
    ) -> Result<Self, IrValidationError> {
        if entity_key_schema.purpose != KeyPurpose::Entity(entity_type) {
            return Err(IrValidationError::InvalidKey {
                reason: "index entity key schema has the wrong owner",
            });
        }
        if components.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "index components",
            });
        }
        checked_len("index components", components.len(), 1_024)?;
        let maximum_encoded_bytes =
            complete_key_maximum(&components, Some(entity_key_schema.maximum_encoded_bytes))?;
        Ok(Self {
            purpose: KeyPurpose::Index {
                index_id,
                entity_type,
            },
            components,
            entity_key_schema: Some(Box::new(entity_key_schema)),
            maximum_encoded_bytes,
        })
    }

    /// Purpose and stable owner.
    #[must_use]
    pub const fn purpose(&self) -> KeyPurpose {
        self.purpose
    }

    /// Ordered component schemas.
    #[must_use]
    pub fn components(&self) -> &[KeyComponentSchema] {
        &self.components
    }

    /// Maximum complete encoded key length.
    #[must_use]
    pub const fn maximum_encoded_bytes(&self) -> usize {
        self.maximum_encoded_bytes
    }

    /// Referenced complete entity-key schema for an index entry.
    #[must_use]
    pub fn entity_key_schema(&self) -> Option<&Self> {
        self.entity_key_schema.as_deref()
    }

    /// Encodes a complete entity primary key.
    pub fn encode_entity(&self, values: &[CanonicalValue]) -> Result<EntityKey, IrValidationError> {
        let KeyPurpose::Entity(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not an entity key schema",
            });
        };
        let mut builder = EntityKeyBuilder::new(owner);
        append_components(&mut builder, &self.components, values)?;
        builder.finish().map_err(|_| IrValidationError::InvalidKey {
            reason: "entity key exceeds its bound",
        })
    }

    /// Decodes and validates a complete entity primary key.
    pub fn decode_entity(&self, key: &EntityKey) -> Result<Vec<CanonicalValue>, IrValidationError> {
        let KeyPurpose::Entity(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not an entity key schema",
            });
        };
        decode_complete_components(key.as_bytes(), &[0x45, 0x01], owner.get(), &self.components)
    }

    /// Encodes a complete logical partition key.
    pub fn encode_partition(
        &self,
        values: &[CanonicalValue],
    ) -> Result<PartitionKey, IrValidationError> {
        let KeyPurpose::Partition(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not a partition key schema",
            });
        };
        let mut builder = PartitionKeyBuilder::new(owner);
        append_components(&mut builder, &self.components, values)?;
        builder.finish().map_err(|_| IrValidationError::InvalidKey {
            reason: "partition key exceeds its bound",
        })
    }

    /// Decodes and validates a complete logical partition key.
    pub fn decode_partition(
        &self,
        key: &PartitionKey,
    ) -> Result<Vec<CanonicalValue>, IrValidationError> {
        let KeyPurpose::Partition(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not a partition key schema",
            });
        };
        decode_complete_components(key.as_bytes(), &[0x50, 0x01], owner.get(), &self.components)
    }

    /// Encodes a complete logical conflict key.
    pub fn encode_conflict(
        &self,
        values: &[CanonicalValue],
    ) -> Result<ConflictKey, IrValidationError> {
        let KeyPurpose::Conflict(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not a conflict key schema",
            });
        };
        let mut builder = ConflictKeyBuilder::new(owner);
        append_components(&mut builder, &self.components, values)?;
        builder.finish().map_err(|_| IrValidationError::InvalidKey {
            reason: "conflict key exceeds its bound",
        })
    }

    /// Decodes and validates a complete logical conflict key.
    pub fn decode_conflict(
        &self,
        key: &ConflictKey,
    ) -> Result<Vec<CanonicalValue>, IrValidationError> {
        let KeyPurpose::Conflict(owner) = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not a conflict key schema",
            });
        };
        decode_complete_components(key.as_bytes(), &[0x43, 0x01], owner.get(), &self.components)
    }

    /// Encodes a complete local-index entry key.
    pub fn encode_index(
        &self,
        values: &[CanonicalValue],
        entity_key: EntityKey,
    ) -> Result<IndexEntryKey, IrValidationError> {
        let KeyPurpose::Index { index_id, .. } = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not an index key schema",
            });
        };
        self.entity_key_schema
            .as_ref()
            .ok_or(IrValidationError::InvalidKey {
                reason: "missing entity key schema",
            })?
            .decode_entity(&entity_key)?;
        let mut builder = IndexEntryKeyBuilder::new(index_id);
        append_components(&mut builder, &self.components, values)?;
        builder
            .finish(entity_key)
            .map_err(|_| IrValidationError::InvalidKey {
                reason: "index entry key exceeds its bound",
            })
    }

    /// Decodes and validates a complete local-index entry key.
    pub fn decode_index(&self, key: &IndexEntryKey) -> Result<DecodedIndexKey, IrValidationError> {
        let KeyPurpose::Index { index_id, .. } = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not an index key schema",
            });
        };
        let mut cursor = KeyCursor::new(key.as_bytes());
        cursor.expect_prefix(&[0x49, 0x01], index_id.get())?;
        let mut values = Vec::with_capacity(self.components.len());
        for component in &self.components {
            values.push(cursor.read_component(component)?);
        }
        let length = cursor.read_u32()? as usize;
        let bytes = cursor.read(length)?.to_vec();
        if !cursor.is_empty() {
            return Err(IrValidationError::TrailingBytes);
        }
        let entity_key =
            EntityKey::from_bytes(bytes).map_err(|_| IrValidationError::InvalidKey {
                reason: "invalid nested entity key envelope",
            })?;
        self.entity_key_schema
            .as_ref()
            .ok_or(IrValidationError::InvalidKey {
                reason: "missing entity key schema",
            })?
            .decode_entity(&entity_key)?;
        Ok(DecodedIndexKey { values, entity_key })
    }

    /// Encodes a validated component-complete transient index scan prefix.
    pub fn encode_index_prefix(
        &self,
        values: &[CanonicalValue],
    ) -> Result<IndexScanPrefix, IrValidationError> {
        let KeyPurpose::Index { index_id, .. } = self.purpose else {
            return Err(IrValidationError::InvalidKey {
                reason: "not an index key schema",
            });
        };
        if values.len() > self.components.len() {
            return Err(IrValidationError::InvalidKey {
                reason: "index prefix has too many components",
            });
        }
        let mut builder = IndexEntryKeyBuilder::new(index_id);
        append_components(&mut builder, &self.components[..values.len()], values)?;
        Ok(IndexScanPrefix {
            index_id,
            component_count: values.len(),
            bytes: builder.as_bytes().to_vec(),
        })
    }
}

/// A decoded complete local-index entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedIndexKey {
    values: Vec<CanonicalValue>,
    entity_key: EntityKey,
}

impl DecodedIndexKey {
    /// Ordered index component values.
    #[must_use]
    pub fn values(&self) -> &[CanonicalValue] {
        &self.values
    }

    /// Complete owning entity key.
    #[must_use]
    pub const fn entity_key(&self) -> &EntityKey {
        &self.entity_key
    }
}

/// A checked component-complete transient index scan prefix.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexScanPrefix {
    index_id: IndexId,
    component_count: usize,
    bytes: Vec<u8>,
}

impl IndexScanPrefix {
    /// Stable index ID embedded in the prefix.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Number of complete leading components.
    #[must_use]
    pub const fn component_count(&self) -> usize {
        self.component_count
    }

    /// Canonical transient prefix bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for IndexScanPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexScanPrefix")
            .field("index_id", &self.index_id)
            .field("component_count", &self.component_count)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

fn complete_key_maximum(
    components: &[KeyComponentSchema],
    nested_entity_maximum: Option<usize>,
) -> Result<usize, IrValidationError> {
    let mut total = 6usize;
    for component in components {
        total = total
            .checked_add(component.maximum_payload_bytes)
            .ok_or(IrValidationError::SizeOverflow { kind: "key schema" })?;
    }
    if let Some(maximum) = nested_entity_maximum {
        total = total
            .checked_add(4)
            .and_then(|value| value.checked_add(maximum))
            .ok_or(IrValidationError::SizeOverflow {
                kind: "index key schema",
            })?;
    }
    checked_len("maximum encoded key", total, MAX_KEY_BYTES)?;
    Ok(total)
}

fn authoritative_component_maximum(value_type: &ValueType) -> Result<usize, IrValidationError> {
    let value = match value_type.tag() {
        ValueTypeTag::Bool => 1,
        ValueTypeTag::I64 | ValueTypeTag::U64 => 8,
        ValueTypeTag::Timestamp => 12,
        ValueTypeTag::Date | ValueTypeTag::Enum => 4,
        ValueTypeTag::Uuid => 16,
        ValueTypeTag::String | ValueTypeTag::Bytes => value_type
            .byte_bound()
            .and_then(|bound| bound.checked_add(4))
            .ok_or(IrValidationError::SizeOverflow {
                kind: "key component",
            })?,
        _ => {
            return Err(IrValidationError::InvalidKey {
                reason: "type is not a v1 authoritative key scalar",
            });
        }
    };
    Ok(value)
}

trait ComponentBuilder {
    fn push_value(&mut self, value: &CanonicalValue) -> Result<(), IrValidationError>;
}

macro_rules! component_builder {
    ($builder:ty) => {
        impl ComponentBuilder for $builder {
            fn push_value(&mut self, value: &CanonicalValue) -> Result<(), IrValidationError> {
                let result = match value {
                    CanonicalValue::Bool(value) => self.push_bool(*value).map(|_| ()),
                    CanonicalValue::I64(value) => self.push_i64(*value).map(|_| ()),
                    CanonicalValue::U64(value) => self.push_u64(*value).map(|_| ()),
                    CanonicalValue::String(value) => self.push_str(value.as_str()).map(|_| ()),
                    CanonicalValue::Bytes(value) => self.push_bytes(value.as_bytes()).map(|_| ()),
                    CanonicalValue::Timestamp(value) => self.push_timestamp(*value).map(|_| ()),
                    CanonicalValue::Date(value) => self.push_date(*value).map(|_| ()),
                    CanonicalValue::Uuid(value) => self.push_uuid(value).map(|_| ()),
                    CanonicalValue::Enum { variant_id, .. } => {
                        self.push_enum_variant(*variant_id).map(|_| ())
                    }
                    _ => {
                        return Err(IrValidationError::InvalidKey {
                            reason: "value is not a v1 authoritative key scalar",
                        });
                    }
                };
                result.map_err(|_| IrValidationError::InvalidKey {
                    reason: "key exceeds its checked bound",
                })
            }
        }
    };
}

component_builder!(EntityKeyBuilder);
component_builder!(PartitionKeyBuilder);
component_builder!(ConflictKeyBuilder);
component_builder!(IndexEntryKeyBuilder);

fn append_components<B: ComponentBuilder>(
    builder: &mut B,
    schemas: &[KeyComponentSchema],
    values: &[CanonicalValue],
) -> Result<(), IrValidationError> {
    if values.len() != schemas.len() {
        return Err(IrValidationError::InvalidKey {
            reason: "wrong key component count",
        });
    }
    for (schema, value) in schemas.iter().zip(values) {
        validate_key_value(schema, value)?;
        builder.push_value(value)?;
    }
    Ok(())
}

fn validate_key_value(
    schema: &KeyComponentSchema,
    value: &CanonicalValue,
) -> Result<(), IrValidationError> {
    schema.value_type.validate_value(value)?;
    if let CanonicalValue::Enum { variant_id, .. } = value
        && schema.enum_variants.binary_search(variant_id).is_err()
    {
        return Err(IrValidationError::InvalidKey {
            reason: "enum key component has an undeclared variant",
        });
    }
    Ok(())
}

fn decode_complete_components(
    bytes: &[u8],
    prefix: &[u8; 2],
    owner: u32,
    schemas: &[KeyComponentSchema],
) -> Result<Vec<CanonicalValue>, IrValidationError> {
    let mut cursor = KeyCursor::new(bytes);
    cursor.expect_prefix(prefix, owner)?;
    let mut result = Vec::with_capacity(schemas.len());
    for schema in schemas {
        result.push(cursor.read_component(schema)?);
    }
    if !cursor.is_empty() {
        return Err(IrValidationError::TrailingBytes);
    }
    Ok(result)
}

struct KeyCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> KeyCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn expect_prefix(&mut self, prefix: &[u8; 2], owner: u32) -> Result<(), IrValidationError> {
        if self.read(2)? != prefix || self.read_u32()? != owner {
            return Err(IrValidationError::InvalidKey {
                reason: "key envelope mismatch",
            });
        }
        Ok(())
    }

    fn read_component(
        &mut self,
        schema: &KeyComponentSchema,
    ) -> Result<CanonicalValue, IrValidationError> {
        let value = match schema.value_type.tag() {
            ValueTypeTag::Bool => match self.read(1)?[0] {
                0 => CanonicalValue::Bool(false),
                1 => CanonicalValue::Bool(true),
                _ => {
                    return Err(IrValidationError::InvalidKey {
                        reason: "invalid Boolean key",
                    });
                }
            },
            ValueTypeTag::I64 => {
                let mut bytes: [u8; 8] = self.read_array()?;
                bytes[0] ^= 0x80;
                CanonicalValue::I64(i64::from_be_bytes(bytes))
            }
            ValueTypeTag::U64 => CanonicalValue::U64(u64::from_be_bytes(self.read_array()?)),
            ValueTypeTag::Timestamp => {
                let mut seconds: [u8; 8] = self.read_array()?;
                seconds[0] ^= 0x80;
                let nanos = u32::from_be_bytes(self.read_array()?);
                CanonicalValue::Timestamp(
                    Timestamp::new(i64::from_be_bytes(seconds), nanos).map_err(|_| {
                        IrValidationError::InvalidKey {
                            reason: "invalid timestamp key",
                        }
                    })?,
                )
            }
            ValueTypeTag::Date => {
                let mut bytes: [u8; 4] = self.read_array()?;
                bytes[0] ^= 0x80;
                CanonicalValue::Date(Date::new(i32::from_be_bytes(bytes)))
            }
            ValueTypeTag::Uuid => CanonicalValue::Uuid(self.read_array()?),
            ValueTypeTag::Enum => CanonicalValue::Enum {
                type_id: schema
                    .value_type
                    .enum_type_id()
                    .ok_or(IrValidationError::InvalidKey {
                        reason: "missing enum type",
                    })?,
                variant_id: EnumVariantId::new(u32::from_be_bytes(self.read_array()?)).ok_or(
                    IrValidationError::InvalidKey {
                        reason: "zero enum variant",
                    },
                )?,
            },
            ValueTypeTag::String => {
                let length = self.read_u32()? as usize;
                let bytes = self.read(length)?;
                CanonicalValue::String(
                    CanonicalString::new(
                        std::str::from_utf8(bytes)
                            .map_err(|_| IrValidationError::InvalidText { kind: "key" })?
                            .to_owned(),
                    )
                    .map_err(|_| IrValidationError::InvalidKey {
                        reason: "oversized string key",
                    })?,
                )
            }
            ValueTypeTag::Bytes => {
                let length = self.read_u32()? as usize;
                CanonicalValue::Bytes(CanonicalBytes::new(self.read(length)?.to_vec()).map_err(
                    |_| IrValidationError::InvalidKey {
                        reason: "oversized bytes key",
                    },
                )?)
            }
            _ => {
                return Err(IrValidationError::InvalidKey {
                    reason: "invalid authoritative key component type",
                });
            }
        };
        validate_key_value(schema, &value)?;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, IrValidationError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], IrValidationError> {
        self.read(N)?
            .try_into()
            .map_err(|_| IrValidationError::UnexpectedEnd)
    }

    fn read(&mut self, length: usize) -> Result<&'a [u8], IrValidationError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        self.position = end;
        Ok(result)
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_keys_round_trip_and_preserve_signed_order() {
        let schema = KeySchema::new(
            KeyPurpose::Entity(EntityTypeId::first()),
            vec![KeyComponentSchema::new(ValueType::i64(), vec![]).expect("component")],
        )
        .expect("schema");
        let negative = schema
            .encode_entity(&[CanonicalValue::I64(-1)])
            .expect("key");
        let positive = schema
            .encode_entity(&[CanonicalValue::I64(1)])
            .expect("key");
        assert!(negative.as_bytes() < positive.as_bytes());
        assert_eq!(
            schema.decode_entity(&negative).expect("decode"),
            vec![CanonicalValue::I64(-1)]
        );
    }

    #[test]
    fn rejects_schema_whose_declared_maximum_is_too_large() {
        let component = KeyComponentSchema::new(ValueType::string(4_091).expect("type"), vec![])
            .expect("component");
        assert!(
            KeySchema::new(KeyPurpose::Entity(EntityTypeId::first()), vec![component]).is_err()
        );
    }

    #[test]
    fn index_prefix_ends_only_after_complete_components() {
        let entity = KeySchema::new(
            KeyPurpose::Entity(EntityTypeId::first()),
            vec![KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component")],
        )
        .expect("entity");
        let index = KeySchema::index(
            IndexId::first(),
            EntityTypeId::first(),
            vec![
                KeyComponentSchema::new(ValueType::string(8).expect("type"), vec![])
                    .expect("component"),
            ],
            entity,
        )
        .expect("index");
        let prefix = index
            .encode_index_prefix(&[CanonicalValue::string("abc").expect("value")])
            .expect("prefix");
        assert_eq!(prefix.component_count(), 1);
        assert_eq!(&prefix.as_bytes()[..6], &[0x49, 0x01, 0, 0, 0, 1]);
    }
}
