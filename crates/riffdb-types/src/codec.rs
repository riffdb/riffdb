//! Versioned canonical encoding for [`CanonicalValue`](crate::CanonicalValue).

use std::fmt;

use crate::{
    CanonicalBytes, CanonicalList, CanonicalRecord, CanonicalString, CanonicalValue, CurrencyCode,
    Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId, FieldId, Money, Timestamp,
    limits::{
        MAX_BYTES_VALUE_BYTES, MAX_CANONICAL_DOCUMENT_BYTES, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH,
        MAX_RECORD_FIELDS, MAX_STRING_BYTES,
    },
};

/// Canonical value encoding version defined by ADR-0011.
pub const CANONICAL_VALUE_VERSION: u8 = 0x01;

const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_I64: u8 = 0x02;
const TAG_U64: u8 = 0x03;
const TAG_DECIMAL: u8 = 0x04;
const TAG_MONEY: u8 = 0x05;
const TAG_STRING: u8 = 0x06;
const TAG_BYTES: u8 = 0x07;
const TAG_TIMESTAMP: u8 = 0x08;
const TAG_DATE: u8 = 0x09;
const TAG_UUID: u8 = 0x0a;
const TAG_ENUM: u8 = 0x0b;
const TAG_LIST: u8 = 0x0c;
const TAG_RECORD: u8 = 0x0d;

/// Encodes one value using canonical value encoding v1.
pub fn encode_canonical_value(value: &CanonicalValue) -> Result<Vec<u8>, CanonicalCodecError> {
    let mut encoder = Encoder::default();
    encoder.encode_value(value, 0)?;
    Ok(encoder.output)
}

/// Decodes exactly one canonical value encoding v1 document.
pub fn decode_canonical_value(input: &[u8]) -> Result<CanonicalValue, CanonicalCodecError> {
    if input.len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(CanonicalCodecError::DocumentTooLarge {
            actual: input.len(),
            maximum: MAX_CANONICAL_DOCUMENT_BYTES,
        });
    }

    let mut decoder = Decoder { input, position: 0 };
    let value = decoder.decode_value(0)?;
    if decoder.position != input.len() {
        return Err(CanonicalCodecError::TrailingBytes {
            count: input.len() - decoder.position,
        });
    }
    Ok(value)
}

#[derive(Default)]
struct Encoder {
    output: Vec<u8>,
}

impl Encoder {
    fn encode_value(
        &mut self,
        value: &CanonicalValue,
        depth: usize,
    ) -> Result<(), CanonicalCodecError> {
        if depth > MAX_NESTING_DEPTH {
            return Err(CanonicalCodecError::NestingTooDeep {
                depth,
                maximum: MAX_NESTING_DEPTH,
            });
        }

        self.write(&[CANONICAL_VALUE_VERSION])?;
        match value {
            CanonicalValue::Null => self.write(&[TAG_NULL]),
            CanonicalValue::Bool(value) => self.write(&[TAG_BOOL, u8::from(*value)]),
            CanonicalValue::I64(value) => {
                self.write(&[TAG_I64])?;
                self.write(&value.to_be_bytes())
            }
            CanonicalValue::U64(value) => {
                self.write(&[TAG_U64])?;
                self.write(&value.to_be_bytes())
            }
            CanonicalValue::Decimal(value) => {
                self.write(&[TAG_DECIMAL])?;
                self.encode_decimal(value)
            }
            CanonicalValue::Money(value) => {
                self.write(&[TAG_MONEY])?;
                self.write(value.currency().as_bytes())?;
                self.encode_decimal(&value.amount())
            }
            CanonicalValue::String(value) => {
                self.write(&[TAG_STRING])?;
                self.encode_bounded_bytes(value.as_str().as_bytes(), BoundedKind::String)
            }
            CanonicalValue::Bytes(value) => {
                self.write(&[TAG_BYTES])?;
                self.encode_bounded_bytes(value.as_bytes(), BoundedKind::Bytes)
            }
            CanonicalValue::Timestamp(value) => {
                self.write(&[TAG_TIMESTAMP])?;
                self.write(&value.seconds().to_be_bytes())?;
                self.write(&value.nanoseconds().to_be_bytes())
            }
            CanonicalValue::Date(value) => {
                self.write(&[TAG_DATE])?;
                self.write(&value.days_since_unix_epoch().to_be_bytes())
            }
            CanonicalValue::Uuid(value) => {
                self.write(&[TAG_UUID])?;
                self.write(value)
            }
            CanonicalValue::Enum {
                type_id,
                variant_id,
            } => {
                self.write(&[TAG_ENUM])?;
                self.write(&type_id.get().to_be_bytes())?;
                self.write(&variant_id.get().to_be_bytes())
            }
            CanonicalValue::List(values) => {
                if values.len() > MAX_LIST_ENTRIES {
                    return Err(CanonicalCodecError::TooManyEntries {
                        kind: CollectionKind::List,
                        actual: values.len(),
                        maximum: MAX_LIST_ENTRIES,
                    });
                }
                self.write(&[TAG_LIST])?;
                self.write(&(values.len() as u32).to_be_bytes())?;
                for value in values.values() {
                    self.encode_value(value, depth + 1)?;
                }
                Ok(())
            }
            CanonicalValue::Record(record) => {
                if record.len() > MAX_RECORD_FIELDS {
                    return Err(CanonicalCodecError::TooManyEntries {
                        kind: CollectionKind::Record,
                        actual: record.len(),
                        maximum: MAX_RECORD_FIELDS,
                    });
                }
                self.write(&[TAG_RECORD])?;
                self.write(&(record.len() as u32).to_be_bytes())?;
                let mut previous = None;
                for (field_id, value) in record.fields() {
                    if previous.is_some_and(|id| id >= field_id.get()) {
                        return Err(CanonicalCodecError::NonCanonicalRecordOrder);
                    }
                    previous = Some(field_id.get());
                    self.write(&field_id.get().to_be_bytes())?;
                    self.encode_value(value, depth + 1)?;
                }
                Ok(())
            }
        }
    }

    fn encode_decimal(&mut self, value: &Decimal) -> Result<(), CanonicalCodecError> {
        self.write(&[value.spec().precision(), value.spec().scale()])?;
        self.write(&value.coefficient().to_be_bytes())
    }

    fn encode_bounded_bytes(
        &mut self,
        value: &[u8],
        kind: BoundedKind,
    ) -> Result<(), CanonicalCodecError> {
        let maximum = match kind {
            BoundedKind::String => MAX_STRING_BYTES,
            BoundedKind::Bytes => MAX_BYTES_VALUE_BYTES,
        };
        if value.len() > maximum {
            return Err(match kind {
                BoundedKind::String => CanonicalCodecError::StringTooLarge {
                    actual: value.len(),
                    maximum,
                },
                BoundedKind::Bytes => CanonicalCodecError::BytesTooLarge {
                    actual: value.len(),
                    maximum,
                },
            });
        }
        self.write(&(value.len() as u32).to_be_bytes())?;
        self.write(value)
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), CanonicalCodecError> {
        let new_length = self.output.len().checked_add(bytes.len()).ok_or(
            CanonicalCodecError::DocumentTooLarge {
                actual: usize::MAX,
                maximum: MAX_CANONICAL_DOCUMENT_BYTES,
            },
        )?;
        if new_length > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(CanonicalCodecError::DocumentTooLarge {
                actual: new_length,
                maximum: MAX_CANONICAL_DOCUMENT_BYTES,
            });
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl Decoder<'_> {
    fn decode_value(&mut self, depth: usize) -> Result<CanonicalValue, CanonicalCodecError> {
        if depth > MAX_NESTING_DEPTH {
            return Err(CanonicalCodecError::NestingTooDeep {
                depth,
                maximum: MAX_NESTING_DEPTH,
            });
        }

        let version = self.read_u8()?;
        if version != CANONICAL_VALUE_VERSION {
            return Err(CanonicalCodecError::UnsupportedVersion { version });
        }
        let tag = self.read_u8()?;
        match tag {
            TAG_NULL => Ok(CanonicalValue::Null),
            TAG_BOOL => match self.read_u8()? {
                0 => Ok(CanonicalValue::Bool(false)),
                1 => Ok(CanonicalValue::Bool(true)),
                value => Err(CanonicalCodecError::InvalidBoolean { value }),
            },
            TAG_I64 => Ok(CanonicalValue::I64(i64::from_be_bytes(self.read_array()?))),
            TAG_U64 => Ok(CanonicalValue::U64(u64::from_be_bytes(self.read_array()?))),
            TAG_DECIMAL => Ok(CanonicalValue::Decimal(self.decode_decimal()?)),
            TAG_MONEY => {
                let currency_bytes: [u8; 3] = self.read_array()?;
                let currency_text = std::str::from_utf8(&currency_bytes)
                    .map_err(|_| CanonicalCodecError::InvalidCurrency)?;
                let currency = CurrencyCode::new(currency_text)
                    .map_err(|_| CanonicalCodecError::InvalidCurrency)?;
                let amount = self.decode_decimal()?;
                Ok(CanonicalValue::Money(Money::new(currency, amount)))
            }
            TAG_STRING => {
                let bytes = self.decode_bounded_bytes(BoundedKind::String)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| CanonicalCodecError::InvalidUtf8)?
                    .to_owned();
                Ok(CanonicalValue::String(
                    CanonicalString::new(value).map_err(|_| {
                        CanonicalCodecError::StringTooLarge {
                            actual: bytes.len(),
                            maximum: MAX_STRING_BYTES,
                        }
                    })?,
                ))
            }
            TAG_BYTES => {
                let bytes = self.decode_bounded_bytes(BoundedKind::Bytes)?;
                Ok(CanonicalValue::Bytes(
                    CanonicalBytes::new(bytes.to_vec()).map_err(|_| {
                        CanonicalCodecError::BytesTooLarge {
                            actual: bytes.len(),
                            maximum: MAX_BYTES_VALUE_BYTES,
                        }
                    })?,
                ))
            }
            TAG_TIMESTAMP => {
                let seconds = i64::from_be_bytes(self.read_array()?);
                let nanos = u32::from_be_bytes(self.read_array()?);
                Timestamp::new(seconds, nanos)
                    .map(CanonicalValue::Timestamp)
                    .map_err(|_| CanonicalCodecError::InvalidTimestamp)
            }
            TAG_DATE => Ok(CanonicalValue::Date(Date::from_days_since_unix_epoch(
                i32::from_be_bytes(self.read_array()?),
            ))),
            TAG_UUID => Ok(CanonicalValue::Uuid(self.read_array()?)),
            TAG_ENUM => {
                let type_id = EnumTypeId::new(u32::from_be_bytes(self.read_array()?))
                    .ok_or(CanonicalCodecError::ZeroEnumTypeId)?;
                let variant_id = EnumVariantId::new(u32::from_be_bytes(self.read_array()?))
                    .ok_or(CanonicalCodecError::ZeroEnumVariantId)?;
                Ok(CanonicalValue::Enum {
                    type_id,
                    variant_id,
                })
            }
            TAG_LIST => {
                let count = self.read_collection_count(CollectionKind::List)?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.decode_value(depth + 1)?);
                }
                Ok(CanonicalValue::List(CanonicalList::new(values).map_err(
                    |_| CanonicalCodecError::TooManyEntries {
                        kind: CollectionKind::List,
                        actual: count,
                        maximum: MAX_LIST_ENTRIES,
                    },
                )?))
            }
            TAG_RECORD => {
                let count = self.read_collection_count(CollectionKind::Record)?;
                let mut fields = Vec::with_capacity(count);
                let mut previous = None;
                for _ in 0..count {
                    let raw_field_id = u32::from_be_bytes(self.read_array()?);
                    if previous.is_some_and(|id| id >= raw_field_id) {
                        return Err(CanonicalCodecError::NonCanonicalRecordOrder);
                    }
                    previous = Some(raw_field_id);
                    let field_id =
                        FieldId::new(raw_field_id).ok_or(CanonicalCodecError::ZeroFieldId)?;
                    fields.push((field_id, self.decode_value(depth + 1)?));
                }
                Ok(CanonicalValue::Record(
                    CanonicalRecord::from_canonical_fields(fields),
                ))
            }
            tag => Err(CanonicalCodecError::UnknownTag { tag }),
        }
    }

    fn decode_decimal(&mut self) -> Result<Decimal, CanonicalCodecError> {
        let precision = self.read_u8()?;
        let scale = self.read_u8()?;
        let coefficient = i128::from_be_bytes(self.read_array()?);
        let spec =
            DecimalSpec::new(precision, scale).map_err(|_| CanonicalCodecError::InvalidDecimal)?;
        Decimal::new(spec, coefficient).map_err(|_| CanonicalCodecError::InvalidDecimal)
    }

    fn decode_bounded_bytes(&mut self, kind: BoundedKind) -> Result<&[u8], CanonicalCodecError> {
        let length = u32::from_be_bytes(self.read_array()?) as usize;
        let maximum = match kind {
            BoundedKind::String => MAX_STRING_BYTES,
            BoundedKind::Bytes => MAX_BYTES_VALUE_BYTES,
        };
        if length > maximum {
            return Err(match kind {
                BoundedKind::String => CanonicalCodecError::StringTooLarge {
                    actual: length,
                    maximum,
                },
                BoundedKind::Bytes => CanonicalCodecError::BytesTooLarge {
                    actual: length,
                    maximum,
                },
            });
        }
        self.read(length)
    }

    fn read_collection_count(
        &mut self,
        kind: CollectionKind,
    ) -> Result<usize, CanonicalCodecError> {
        let count = u32::from_be_bytes(self.read_array()?) as usize;
        let maximum = match kind {
            CollectionKind::List => MAX_LIST_ENTRIES,
            CollectionKind::Record => MAX_RECORD_FIELDS,
        };
        if count > maximum {
            return Err(CanonicalCodecError::TooManyEntries {
                kind,
                actual: count,
                maximum,
            });
        }
        Ok(count)
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalCodecError> {
        Ok(self.read(1)?[0])
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CanonicalCodecError> {
        self.read(N)?
            .try_into()
            .map_err(|_| CanonicalCodecError::UnexpectedEnd)
    }

    fn read(&mut self, length: usize) -> Result<&[u8], CanonicalCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CanonicalCodecError::UnexpectedEnd)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(CanonicalCodecError::UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedKind {
    String,
    Bytes,
}

/// A collection kind used in bounded decoder errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionKind {
    /// Canonical list.
    List,
    /// Canonical record.
    Record,
}

/// A safe canonical encoding or decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalCodecError {
    /// The document exceeds the hard encoded-size limit.
    DocumentTooLarge {
        /// Observed or attempted encoded length.
        actual: usize,
        /// Maximum encoded length.
        maximum: usize,
    },
    /// A string exceeds its hard byte-length limit.
    StringTooLarge {
        /// Declared or observed UTF-8 byte length.
        actual: usize,
        /// Maximum UTF-8 byte length.
        maximum: usize,
    },
    /// A byte value exceeds its hard length limit.
    BytesTooLarge {
        /// Declared or observed byte length.
        actual: usize,
        /// Maximum byte length.
        maximum: usize,
    },
    /// A list or record exceeds its hard entry limit.
    TooManyEntries {
        /// Collection kind.
        kind: CollectionKind,
        /// Declared or observed entry count.
        actual: usize,
        /// Maximum entry count.
        maximum: usize,
    },
    /// The value graph exceeds the hard nesting-depth limit.
    NestingTooDeep {
        /// Observed depth below the root value.
        depth: usize,
        /// Maximum depth below the root value.
        maximum: usize,
    },
    /// The first byte is not a supported encoding version.
    UnsupportedVersion {
        /// Unsupported format byte.
        version: u8,
    },
    /// The type tag is not registered in encoding v1.
    UnknownTag {
        /// Unknown tag byte.
        tag: u8,
    },
    /// A Boolean payload is neither zero nor one.
    InvalidBoolean {
        /// Invalid payload byte.
        value: u8,
    },
    /// A decimal specification or coefficient is invalid.
    InvalidDecimal,
    /// A money currency is not three uppercase ASCII bytes.
    InvalidCurrency,
    /// Timestamp nanoseconds are outside the accepted range.
    InvalidTimestamp,
    /// A string payload is not valid UTF-8.
    InvalidUtf8,
    /// Record fields are duplicated or not in strictly increasing ID order.
    NonCanonicalRecordOrder,
    /// An enum payload contains the reserved zero enum-type ID.
    ZeroEnumTypeId,
    /// An enum payload contains the reserved zero variant ID.
    ZeroEnumVariantId,
    /// A record payload contains the reserved zero field ID.
    ZeroFieldId,
    /// The document ended before the declared value was complete.
    UnexpectedEnd,
    /// Bytes remain after the first complete value.
    TrailingBytes {
        /// Number of unused bytes.
        count: usize,
    },
}

impl fmt::Display for CanonicalCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { actual, maximum } => write!(
                formatter,
                "canonical document has {actual} bytes; maximum is {maximum}"
            ),
            Self::StringTooLarge { actual, maximum } => {
                write!(formatter, "string has {actual} bytes; maximum is {maximum}")
            }
            Self::BytesTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "byte value has {actual} bytes; maximum is {maximum}"
                )
            }
            Self::TooManyEntries {
                kind,
                actual,
                maximum,
            } => write!(
                formatter,
                "{kind:?} has {actual} entries; maximum is {maximum}"
            ),
            Self::NestingTooDeep { depth, maximum } => {
                write!(formatter, "nesting depth is {depth}; maximum is {maximum}")
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported canonical value version {version}")
            }
            Self::UnknownTag { tag } => write!(formatter, "unknown canonical value tag {tag}"),
            Self::InvalidBoolean { value } => {
                write!(formatter, "invalid Boolean payload {value}")
            }
            Self::InvalidDecimal => formatter.write_str("invalid canonical decimal"),
            Self::InvalidCurrency => formatter.write_str("invalid canonical currency"),
            Self::InvalidTimestamp => formatter.write_str("invalid canonical timestamp"),
            Self::InvalidUtf8 => formatter.write_str("invalid canonical UTF-8"),
            Self::NonCanonicalRecordOrder => {
                formatter.write_str("record fields are not in canonical order")
            }
            Self::ZeroEnumTypeId => formatter.write_str("enum type ID must be nonzero"),
            Self::ZeroEnumVariantId => formatter.write_str("enum variant ID must be nonzero"),
            Self::ZeroFieldId => formatter.write_str("record field ID must be nonzero"),
            Self::UnexpectedEnd => formatter.write_str("canonical document ended unexpectedly"),
            Self::TrailingBytes { count } => {
                write!(formatter, "canonical document has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for CanonicalCodecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_documents_without_panicking() {
        let cases: &[&[u8]] = &[
            &[],
            &[2, TAG_NULL],
            &[1, 0xff],
            &[1, TAG_BOOL, 2],
            &[1, TAG_U64],
            &[1, TAG_STRING, 0, 0, 0, 1, 0xff],
            &[1, TAG_NULL, 0],
        ];
        for case in cases {
            assert!(decode_canonical_value(case).is_err(), "accepted {case:?}");
        }
    }
}
