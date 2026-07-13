//! The closed canonical value algebra shared by semantic boundaries.

use std::fmt;

use crate::limits::{
    MAX_BYTES_VALUE_BYTES, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH, MAX_RECORD_FIELDS, MAX_STRING_BYTES,
};
use crate::{Date, Decimal, EnumTypeId, EnumVariantId, FieldId, Money, Timestamp};

/// A value in RiffDB's closed, deterministic transactional value algebra.
///
/// Constructors for variable-sized variants enforce process hard limits. The
/// canonical encoder validates again so direct enum construction cannot bypass
/// a durable-boundary check.
#[derive(Clone, Eq, PartialEq)]
pub enum CanonicalValue {
    /// Explicit optional absence.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// Checked fixed-scale decimal.
    Decimal(Decimal),
    /// Currency-qualified checked decimal.
    Money(Money),
    /// Exact UTF-8 text without implicit normalization.
    String(CanonicalString),
    /// Exact opaque bytes.
    Bytes(CanonicalBytes),
    /// UTC timestamp supplied by an input or deterministic transaction context.
    Timestamp(Timestamp),
    /// Calendar date represented as days since the Unix epoch.
    Date(Date),
    /// UUID network-order bytes.
    Uuid([u8; 16]),
    /// Stable enum type and variant IDs. Display names are not canonical.
    Enum {
        /// Stable enum type ID.
        type_id: EnumTypeId,
        /// Stable enum variant ID.
        variant_id: EnumVariantId,
    },
    /// Bounded ordered values.
    List(CanonicalList),
    /// Fields in strictly increasing stable-ID order.
    Record(CanonicalRecord),
}

impl fmt::Debug for CanonicalValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("CanonicalValue::Null"),
            Self::Bool(_) => formatter.write_str("CanonicalValue::Bool([REDACTED])"),
            Self::I64(_) => formatter.write_str("CanonicalValue::I64([REDACTED])"),
            Self::U64(_) => formatter.write_str("CanonicalValue::U64([REDACTED])"),
            Self::Decimal(_) => formatter.write_str("CanonicalValue::Decimal([REDACTED])"),
            Self::Money(_) => formatter.write_str("CanonicalValue::Money([REDACTED])"),
            Self::String(value) => formatter
                .debug_tuple("CanonicalValue::String")
                .field(value)
                .finish(),
            Self::Bytes(value) => formatter
                .debug_tuple("CanonicalValue::Bytes")
                .field(value)
                .finish(),
            Self::Timestamp(_) => formatter.write_str("CanonicalValue::Timestamp([REDACTED])"),
            Self::Date(_) => formatter.write_str("CanonicalValue::Date([REDACTED])"),
            Self::Uuid(_) => formatter.write_str("CanonicalValue::Uuid([REDACTED])"),
            Self::Enum { .. } => formatter.write_str("CanonicalValue::Enum([REDACTED])"),
            Self::List(values) => formatter
                .debug_struct("CanonicalValue::List")
                .field("length", &values.len())
                .finish(),
            Self::Record(record) => formatter
                .debug_struct("CanonicalValue::Record")
                .field("field_count", &record.len())
                .finish(),
        }
    }
}

impl CanonicalValue {
    /// Constructs bounded exact UTF-8 text.
    pub fn string(value: impl Into<String>) -> Result<Self, ValueError> {
        let value = value.into();
        Ok(Self::String(CanonicalString::new(value)?))
    }

    /// Constructs bounded exact bytes.
    pub fn bytes(value: impl Into<Vec<u8>>) -> Result<Self, ValueError> {
        let value = value.into();
        Ok(Self::Bytes(CanonicalBytes::new(value)?))
    }

    /// Constructs a bounded list without changing its order.
    pub fn list(values: Vec<Self>) -> Result<Self, ValueError> {
        Ok(Self::List(CanonicalList::new(values)?))
    }

    /// Constructs a record, sorting fields by stable ID and rejecting duplicates.
    pub fn record(fields: Vec<(FieldId, Self)>) -> Result<Self, ValueError> {
        Ok(Self::Record(CanonicalRecord::new(fields)?))
    }

    fn nesting_depth(&self) -> usize {
        match self {
            Self::List(list) => list
                .values()
                .iter()
                .map(Self::nesting_depth)
                .max()
                .map_or(0, |depth| depth + 1),
            Self::Record(record) => record
                .fields()
                .iter()
                .map(|(_, value)| value.nesting_depth())
                .max()
                .map_or(0, |depth| depth + 1),
            _ => 0,
        }
    }
}

/// Exact bounded UTF-8 text for a canonical value.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalString(String);

impl fmt::Debug for CanonicalString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalString")
            .field("value", &"[REDACTED]")
            .field("length", &self.len())
            .finish()
    }
}

impl CanonicalString {
    /// Validates and constructs exact UTF-8 text without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, ValueError> {
        let value = value.into();
        if value.len() > MAX_STRING_BYTES {
            return Err(ValueError::StringTooLong {
                actual: value.len(),
                maximum: MAX_STRING_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Borrows the exact UTF-8 text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the UTF-8 byte length.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the text is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Exact bounded opaque bytes for a canonical value.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalBytes(Vec<u8>);

impl fmt::Debug for CanonicalBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalBytes")
            .field("value", &"[REDACTED]")
            .field("length", &self.len())
            .finish()
    }
}

impl CanonicalBytes {
    /// Validates and constructs an exact byte value.
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, ValueError> {
        let value = value.into();
        if value.len() > MAX_BYTES_VALUE_BYTES {
            return Err(ValueError::BytesTooLong {
                actual: value.len(),
                maximum: MAX_BYTES_VALUE_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Borrows the exact bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the byte length.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A bounded ordered list of canonical values.
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalList(Vec<CanonicalValue>);

impl fmt::Debug for CanonicalList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalList")
            .field("length", &self.len())
            .finish()
    }
}

impl CanonicalList {
    /// Validates and constructs a bounded list without changing its order.
    pub fn new(values: Vec<CanonicalValue>) -> Result<Self, ValueError> {
        if values.len() > MAX_LIST_ENTRIES {
            return Err(ValueError::TooManyListEntries {
                actual: values.len(),
                maximum: MAX_LIST_ENTRIES,
            });
        }
        validate_child_depth(values.iter())?;
        Ok(Self(values))
    }

    /// Borrows the ordered values.
    pub fn values(&self) -> &[CanonicalValue] {
        &self.0
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the list contains no values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A record whose fields are canonicalized by stable field ID.
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalRecord {
    fields: Vec<(FieldId, CanonicalValue)>,
}

impl fmt::Debug for CanonicalRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalRecord")
            .field("field_count", &self.len())
            .finish()
    }
}

impl CanonicalRecord {
    /// Sorts fields by stable ID and rejects duplicate IDs.
    pub fn new(mut fields: Vec<(FieldId, CanonicalValue)>) -> Result<Self, ValueError> {
        if fields.len() > MAX_RECORD_FIELDS {
            return Err(ValueError::TooManyRecordFields {
                actual: fields.len(),
                maximum: MAX_RECORD_FIELDS,
            });
        }

        fields.sort_by_key(|(field_id, _)| field_id.get());
        if let Some(field_id) = fields
            .windows(2)
            .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
        {
            return Err(ValueError::DuplicateRecordField { field_id });
        }
        validate_child_depth(fields.iter().map(|(_, value)| value))?;

        Ok(Self { fields })
    }

    pub(crate) fn from_canonical_fields(fields: Vec<(FieldId, CanonicalValue)>) -> Self {
        debug_assert!(fields.windows(2).all(|pair| pair[0].0 < pair[1].0));
        Self { fields }
    }

    /// Returns fields in strictly increasing stable-ID order.
    pub fn fields(&self) -> &[(FieldId, CanonicalValue)] {
        &self.fields
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns whether the record contains no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// A bounded-value construction failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueError {
    /// A string exceeds the process hard limit.
    StringTooLong {
        /// Observed UTF-8 byte length.
        actual: usize,
        /// Maximum permitted UTF-8 byte length.
        maximum: usize,
    },
    /// A byte value exceeds the process hard limit.
    BytesTooLong {
        /// Observed byte length.
        actual: usize,
        /// Maximum permitted byte length.
        maximum: usize,
    },
    /// A list exceeds the process hard entry limit.
    TooManyListEntries {
        /// Observed entry count.
        actual: usize,
        /// Maximum permitted entry count.
        maximum: usize,
    },
    /// A record exceeds the process hard field limit.
    TooManyRecordFields {
        /// Observed field count.
        actual: usize,
        /// Maximum permitted field count.
        maximum: usize,
    },
    /// Two record fields use the same stable field ID.
    DuplicateRecordField {
        /// Duplicated stable field ID.
        field_id: FieldId,
    },
    /// A list or record would exceed the canonical nesting limit.
    NestingTooDeep {
        /// Attempted depth below the root value.
        depth: usize,
        /// Maximum depth below the root value.
        maximum: usize,
    },
}

impl fmt::Display for ValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StringTooLong { actual, maximum } => {
                write!(formatter, "string has {actual} bytes; maximum is {maximum}")
            }
            Self::BytesTooLong { actual, maximum } => {
                write!(
                    formatter,
                    "byte value has {actual} bytes; maximum is {maximum}"
                )
            }
            Self::TooManyListEntries { actual, maximum } => {
                write!(formatter, "list has {actual} entries; maximum is {maximum}")
            }
            Self::TooManyRecordFields { actual, maximum } => {
                write!(
                    formatter,
                    "record has {actual} fields; maximum is {maximum}"
                )
            }
            Self::DuplicateRecordField { field_id } => {
                write!(
                    formatter,
                    "record contains duplicate field ID {}",
                    field_id.get()
                )
            }
            Self::NestingTooDeep { depth, maximum } => {
                write!(formatter, "nesting depth is {depth}; maximum is {maximum}")
            }
        }
    }
}

impl std::error::Error for ValueError {}

fn validate_child_depth<'a>(
    children: impl IntoIterator<Item = &'a CanonicalValue>,
) -> Result<(), ValueError> {
    let depth = children
        .into_iter()
        .map(CanonicalValue::nesting_depth)
        .max()
        .map_or(0, |depth| depth + 1);
    if depth > MAX_NESTING_DEPTH {
        return Err(ValueError::NestingTooDeep {
            depth,
            maximum: MAX_NESTING_DEPTH,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_constructor_sorts_and_rejects_duplicates() {
        let record = CanonicalRecord::new(vec![
            (FieldId::new(9).expect("nonzero"), CanonicalValue::Null),
            (
                FieldId::new(2).expect("nonzero"),
                CanonicalValue::Bool(true),
            ),
        ])
        .expect("valid record");
        assert_eq!(
            record
                .fields()
                .iter()
                .map(|(id, _)| id.get())
                .collect::<Vec<_>>(),
            [2, 9]
        );

        assert_eq!(
            CanonicalRecord::new(vec![
                (FieldId::new(2).expect("nonzero"), CanonicalValue::Null),
                (FieldId::new(2).expect("nonzero"), CanonicalValue::Null),
            ]),
            Err(ValueError::DuplicateRecordField {
                field_id: FieldId::new(2).expect("nonzero")
            })
        );
    }

    #[test]
    fn canonical_debug_output_redacts_exact_values() {
        let secret = "raw-idempotency-key-canary";
        let values = [
            CanonicalValue::string(secret).expect("bounded string"),
            CanonicalValue::bytes(secret.as_bytes().to_vec()).expect("bounded bytes"),
            CanonicalValue::I64(4_242_424_242),
        ];

        for value in values {
            let output = format!("{value:?}");
            assert!(!output.contains(secret));
            assert!(!output.contains("4242424242"));
        }
    }
}
