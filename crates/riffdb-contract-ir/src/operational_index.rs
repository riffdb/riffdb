//! Compiler-sealed physical values for operational secondary indexes.

use riffdb_types::{CanonicalRecord, CanonicalValue, Date, FieldId, Timestamp};

use crate::{IndexFieldEncodingV1, IndexSchema, IrValidationError, TextKeyProfileV1, ValueTypeTag};

/// Missing-field discriminator in a presence-aware index.
pub const PRESENCE_MISSING_V1: u64 = 0;
/// Explicit-null discriminator in a presence-aware index.
pub const PRESENCE_NULL_V1: u64 = 1;
/// Present non-null discriminator in a presence-aware index.
pub const PRESENCE_VALUE_V1: u64 = 2;

/// Lowers one logical entity record to the exact physical index components.
pub fn encode_operational_index_values_v1(
    index: &IndexSchema,
    record: &CanonicalRecord,
) -> Result<Vec<CanonicalValue>, IrValidationError> {
    let mut values = Vec::with_capacity(index.key_schema().components().len());
    let mut physical_position = 0usize;
    for (field, encoding) in index.fields().iter().zip(index.encodings()) {
        let value = record_value(record, *field);
        match encoding {
            IndexFieldEncodingV1::Canonical => {
                values.push(value.cloned().ok_or(IrValidationError::InvalidKey {
                    reason: "canonical index field is missing",
                })?);
                physical_position += 1;
            }
            IndexFieldEncodingV1::Presence => match value {
                None => {
                    values.push(CanonicalValue::U64(PRESENCE_MISSING_V1));
                    values.push(presence_placeholder_v1(
                        index
                            .key_schema()
                            .components()
                            .get(physical_position + 1)
                            .ok_or(IrValidationError::InvalidKey {
                                reason: "presence payload component is missing",
                            })?,
                    )?);
                    physical_position += 2;
                }
                Some(CanonicalValue::Null) => {
                    values.push(CanonicalValue::U64(PRESENCE_NULL_V1));
                    values.push(presence_placeholder_v1(
                        index
                            .key_schema()
                            .components()
                            .get(physical_position + 1)
                            .ok_or(IrValidationError::InvalidKey {
                                reason: "presence payload component is missing",
                            })?,
                    )?);
                    physical_position += 2;
                }
                Some(value) => {
                    values.push(CanonicalValue::U64(PRESENCE_VALUE_V1));
                    values.push(value.clone());
                    physical_position += 2;
                }
            },
            IndexFieldEncodingV1::TextKey(profile) => {
                let Some(CanonicalValue::String(value)) = value else {
                    return Err(IrValidationError::InvalidKey {
                        reason: "text-key index field is absent or not a string",
                    });
                };
                let transformed = match profile {
                    TextKeyProfileV1::BinaryUtf8 => value.as_str().as_bytes().to_vec(),
                    // ADR-0172. The same transform the exact-text provider
                    // applies to a needle, so a stored value and a submitted
                    // one compare as equal text.
                    TextKeyProfileV1::UnicodeFold => {
                        riffdb_types::unicode_fold_v1(value.as_str()).into_bytes()
                    }
                };
                values.push(CanonicalValue::bytes(transformed).map_err(|_| {
                    IrValidationError::InvalidKey {
                        reason: "text-key output exceeds its bound",
                    }
                })?);
                physical_position += 1;
            }
        }
    }
    if values.len() != index.key_schema().components().len()
        || index
            .key_schema()
            .components()
            .iter()
            .zip(&values)
            .any(|(component, value)| {
                component.value_type().tag() == ValueTypeTag::Bytes
                    && !matches!(value, CanonicalValue::Bytes(_))
            })
    {
        return Err(IrValidationError::InvalidKey {
            reason: "operational index physical arity mismatch",
        });
    }
    Ok(values)
}

/// Lowers one logical entity record to the exact canonical covered record for
/// a covering index.
///
/// The cover is derived from the same post-image the index key is derived
/// from, so a covered read observes one consistent row. A cover field absent
/// from the record is an error rather than an omission: a short cover is
/// indistinguishable from durable corruption at read time and fails the query
/// closed, so it must never be written.
pub fn encode_operational_index_cover_v1(
    index: &IndexSchema,
    record: &CanonicalRecord,
) -> Result<CanonicalRecord, IrValidationError> {
    let mut covered = Vec::with_capacity(index.cover_fields().len());
    for field in index.cover_fields() {
        let value = record_value(record, *field).ok_or(IrValidationError::InvalidKey {
            reason: "covered index field is missing",
        })?;
        covered.push((*field, value.clone()));
    }
    // `CanonicalRecord::new` sorts by stable ID, so the declared cover order
    // does not have to be ascending; the stored record is canonical either way.
    CanonicalRecord::new(covered).map_err(|_| IrValidationError::InvalidKey {
        reason: "covered index fields are not a canonical record",
    })
}

/// Returns the unique ignored payload stored beside a missing/null discriminator.
pub fn presence_placeholder_v1(
    component: &crate::KeyComponentSchema,
) -> Result<CanonicalValue, IrValidationError> {
    Ok(match component.value_type().tag() {
        ValueTypeTag::Bool => CanonicalValue::Bool(false),
        ValueTypeTag::I64 => CanonicalValue::I64(0),
        ValueTypeTag::U64 => CanonicalValue::U64(0),
        ValueTypeTag::String => {
            CanonicalValue::string(String::new()).map_err(|_| IrValidationError::InvalidKey {
                reason: "presence string placeholder is invalid",
            })?
        }
        ValueTypeTag::Bytes => {
            CanonicalValue::bytes(Vec::new()).map_err(|_| IrValidationError::InvalidKey {
                reason: "presence bytes placeholder is invalid",
            })?
        }
        ValueTypeTag::Timestamp => {
            CanonicalValue::Timestamp(Timestamp::new(0, 0).map_err(|_| {
                IrValidationError::InvalidKey {
                    reason: "presence timestamp placeholder is invalid",
                }
            })?)
        }
        ValueTypeTag::Date => CanonicalValue::Date(Date::new(0)),
        ValueTypeTag::Uuid => CanonicalValue::Uuid([0; 16]),
        ValueTypeTag::Enum => CanonicalValue::Enum {
            type_id: component.value_type().enum_type_id().ok_or(
                IrValidationError::InvalidKey {
                    reason: "presence enum placeholder has no type",
                },
            )?,
            variant_id: *component.enum_variants().first().ok_or(
                IrValidationError::InvalidKey {
                    reason: "presence enum placeholder has no variant",
                },
            )?,
        },
        _ => {
            return Err(IrValidationError::InvalidKey {
                reason: "presence payload is not an authoritative key scalar",
            });
        }
    })
}

fn record_value(record: &CanonicalRecord, field: FieldId) -> Option<&CanonicalValue> {
    record
        .fields()
        .binary_search_by_key(&field, |(candidate, _)| *candidate)
        .ok()
        .map(|position| &record.fields()[position].1)
}
