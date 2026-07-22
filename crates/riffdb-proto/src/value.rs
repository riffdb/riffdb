//! Checked adapters for the exact public Protobuf value family.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_types::{
    CanonicalValue, CurrencyCode, Decimal as CanonicalDecimal, DecimalSpec, EnumTypeId,
    EnumVariantId, FieldId, MAX_BYTES_VALUE_BYTES, MAX_CANONICAL_DOCUMENT_BYTES, MAX_LIST_ENTRIES,
    MAX_NESTING_DEPTH, MAX_RECORD_FIELDS, MAX_STRING_BYTES, Money as CanonicalMoney, Timestamp,
};

use crate::v1;
use crate::wire::{self, PreflightError};

/// Maximum byte length of a protocol or schema-resolved display name.
pub const MAX_PROTOCOL_NAME_BYTES: usize = 256;

/// Decodes and structurally validates one public `Value` message.
///
/// This check is deliberately schema-independent. Decimal precision, record
/// name resolution, and enum display-name agreement require compiled contract
/// context and are not claimed by this function.
pub fn decode_value(input: &[u8]) -> Result<v1::Value, ValueValidationError> {
    if input.len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(ValueValidationError::DocumentTooLarge);
    }
    match wire::value(input) {
        Ok(()) => {}
        Err(PreflightError::Malformed) => return Err(ValueValidationError::MalformedEncoding),
        Err(PreflightError::LimitExceeded) => {
            return Err(ValueValidationError::PreflightLimitExceeded);
        }
    }
    let value = v1::Value::decode(input).map_err(|_| ValueValidationError::MalformedEncoding)?;
    validate_value(&value)?;
    Ok(value)
}

/// Applies schema-independent bounds and canonical representation checks.
pub fn validate_value(value: &v1::Value) -> Result<(), ValueValidationError> {
    validate_value_at_depth(value, 0)?;
    if value.encoded_len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(ValueValidationError::DocumentTooLarge);
    }
    Ok(())
}

/// Converts a canonical semantic value to its exact public wire form.
///
/// Record fields are emitted by stable field ID and enum display names are
/// omitted because they are not part of canonical identity.
pub fn canonical_value_to_proto(value: &CanonicalValue) -> Result<v1::Value, ValueValidationError> {
    let wire = canonical_value_to_proto_unchecked(value);
    validate_value(&wire)?;
    Ok(wire)
}

fn canonical_value_to_proto_unchecked(value: &CanonicalValue) -> v1::Value {
    use v1::value::Kind;

    let kind = match value {
        CanonicalValue::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        CanonicalValue::Bool(value) => Kind::BoolValue(*value),
        CanonicalValue::I64(value) => Kind::I64Value(*value),
        CanonicalValue::U64(value) => Kind::U64Value(*value),
        CanonicalValue::Decimal(value) => Kind::DecimalValue(decimal_to_proto(*value)),
        CanonicalValue::Money(value) => Kind::MoneyValue(money_to_proto(*value)),
        CanonicalValue::String(value) => Kind::StringValue(value.as_str().to_owned()),
        CanonicalValue::Bytes(value) => Kind::BytesValue(value.as_bytes().to_vec()),
        CanonicalValue::Uuid(value) => Kind::UuidValue(value.to_vec()),
        CanonicalValue::Date(value) => Kind::DateValue(v1::Date {
            days_since_unix_epoch: value.days_since_unix_epoch(),
        }),
        CanonicalValue::Timestamp(value) => Kind::TimestampValue(v1::Timestamp {
            seconds: value.seconds(),
            nanos: value.nanoseconds(),
        }),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => Kind::EnumValue(v1::EnumValue {
            type_id: type_id.get(),
            variant_id: variant_id.get(),
            name: String::new(),
        }),
        CanonicalValue::List(values) => Kind::ListValue(v1::ValueList {
            values: values
                .values()
                .iter()
                .map(canonical_value_to_proto_unchecked)
                .collect(),
        }),
        CanonicalValue::Record(record) => Kind::RecordValue(v1::ValueRecord {
            fields: record
                .fields()
                .iter()
                .map(|(field_id, value)| v1::ValueField {
                    field_id: Some(field_id.get()),
                    name: String::new(),
                    value: Some(canonical_value_to_proto_unchecked(value)),
                })
                .collect(),
        }),
    };
    v1::Value { kind: Some(kind) }
}

/// Converts a structurally valid wire decimal using its compiled decimal type.
pub fn decimal_from_proto(
    value: &v1::Decimal,
    expected: DecimalSpec,
) -> Result<CanonicalDecimal, ValueValidationError> {
    validate_decimal(value)?;
    if value.scale != u32::from(expected.scale()) {
        return Err(ValueValidationError::DecimalTypeMismatch);
    }
    let coefficient = decode_minimal_i128(&value.coefficient_twos_complement)?;
    CanonicalDecimal::new(expected, coefficient)
        .map_err(|_| ValueValidationError::DecimalOutOfRange)
}

/// Converts structurally valid wire money using its compiled decimal type.
pub fn money_from_proto(
    value: &v1::Money,
    expected: DecimalSpec,
    expected_currency: CurrencyCode,
) -> Result<CanonicalMoney, ValueValidationError> {
    let currency = CurrencyCode::new(value.currency.as_bytes())
        .map_err(|_| ValueValidationError::InvalidCurrency)?;
    if currency != expected_currency {
        return Err(ValueValidationError::MoneyTypeMismatch);
    }
    let amount = value
        .amount
        .as_ref()
        .ok_or(ValueValidationError::MissingNestedValue)?;
    Ok(CanonicalMoney::new(
        currency,
        decimal_from_proto(amount, expected)?,
    ))
}

fn validate_value_at_depth(value: &v1::Value, depth: usize) -> Result<(), ValueValidationError> {
    use v1::value::Kind;

    if depth > MAX_NESTING_DEPTH {
        return Err(ValueValidationError::NestingTooDeep);
    }
    match value
        .kind
        .as_ref()
        .ok_or(ValueValidationError::MissingKind)?
    {
        Kind::NullValue(value) if *value != v1::NullValue::NullValue as i32 => {
            Err(ValueValidationError::InvalidNull)
        }
        Kind::NullValue(_) | Kind::BoolValue(_) | Kind::I64Value(_) | Kind::U64Value(_) => Ok(()),
        Kind::DecimalValue(value) => validate_decimal(value),
        Kind::MoneyValue(value) => validate_money(value),
        Kind::StringValue(value) if value.len() > MAX_STRING_BYTES => {
            Err(ValueValidationError::StringTooLong)
        }
        Kind::StringValue(_) => Ok(()),
        Kind::BytesValue(value) if value.len() > MAX_BYTES_VALUE_BYTES => {
            Err(ValueValidationError::BytesTooLong)
        }
        Kind::BytesValue(_) => Ok(()),
        Kind::UuidValue(value) if value.len() != 16 => Err(ValueValidationError::InvalidUuid),
        Kind::UuidValue(_) | Kind::DateValue(_) => Ok(()),
        Kind::TimestampValue(value) => Timestamp::new(value.seconds, value.nanos)
            .map(|_| ())
            .map_err(|_| ValueValidationError::InvalidTimestamp),
        Kind::EnumValue(value) if value.name.len() > MAX_PROTOCOL_NAME_BYTES => {
            Err(ValueValidationError::NameTooLong)
        }
        Kind::EnumValue(value)
            if EnumTypeId::new(value.type_id).is_none()
                || EnumVariantId::new(value.variant_id).is_none() =>
        {
            Err(ValueValidationError::InvalidEnumIdentity)
        }
        Kind::EnumValue(_) => Ok(()),
        Kind::ListValue(value) => {
            if value.values.len() > MAX_LIST_ENTRIES {
                return Err(ValueValidationError::TooManyItems);
            }
            for child in &value.values {
                validate_value_at_depth(child, depth + 1)?;
            }
            Ok(())
        }
        Kind::RecordValue(value) => validate_record(value, depth),
    }
}

fn validate_decimal(value: &v1::Decimal) -> Result<(), ValueValidationError> {
    if value.scale > 38 {
        return Err(ValueValidationError::DecimalScaleOutOfRange);
    }
    decode_minimal_i128(&value.coefficient_twos_complement).map(|_| ())
}

fn validate_money(value: &v1::Money) -> Result<(), ValueValidationError> {
    CurrencyCode::new(value.currency.as_bytes())
        .map_err(|_| ValueValidationError::InvalidCurrency)?;
    validate_decimal(
        value
            .amount
            .as_ref()
            .ok_or(ValueValidationError::MissingNestedValue)?,
    )
}

pub(crate) fn validate_value_record(value: &v1::ValueRecord) -> Result<(), ValueValidationError> {
    validate_record(value, 0)
}

fn validate_record(value: &v1::ValueRecord, depth: usize) -> Result<(), ValueValidationError> {
    if value.fields.len() > MAX_RECORD_FIELDS {
        return Err(ValueValidationError::TooManyItems);
    }

    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut previous_id = None;
    let mut previous_name: Option<&str> = None;
    let mut saw_name_only = false;

    for field in &value.fields {
        if field.name.len() > MAX_PROTOCOL_NAME_BYTES {
            return Err(ValueValidationError::NameTooLong);
        }
        match field.field_id {
            Some(field_id) => {
                if FieldId::new(field_id).is_none() {
                    return Err(ValueValidationError::InvalidFieldIdentity);
                }
                if saw_name_only || previous_id.is_some_and(|previous| previous >= field_id) {
                    return Err(ValueValidationError::NonCanonicalRecordOrder);
                }
                if !ids.insert(field_id) {
                    return Err(ValueValidationError::DuplicateField);
                }
                previous_id = Some(field_id);
            }
            None => {
                saw_name_only = true;
                if field.name.is_empty() {
                    return Err(ValueValidationError::MissingFieldIdentity);
                }
                if previous_name.is_some_and(|previous| previous >= field.name.as_str()) {
                    return Err(ValueValidationError::NonCanonicalRecordOrder);
                }
                previous_name = Some(field.name.as_str());
            }
        }
        if !field.name.is_empty() && !names.insert(field.name.as_str()) {
            return Err(ValueValidationError::DuplicateField);
        }
        validate_value_at_depth(
            field
                .value
                .as_ref()
                .ok_or(ValueValidationError::MissingNestedValue)?,
            depth + 1,
        )?;
    }
    Ok(())
}

fn decimal_to_proto(value: CanonicalDecimal) -> v1::Decimal {
    v1::Decimal {
        coefficient_twos_complement: encode_minimal_i128(value.coefficient()),
        scale: u32::from(value.spec().scale()),
    }
}

fn money_to_proto(value: CanonicalMoney) -> v1::Money {
    v1::Money {
        currency: value.currency().to_string(),
        amount: Some(decimal_to_proto(value.amount())),
    }
}

fn encode_minimal_i128(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let removable_positive = bytes[start] == 0 && bytes[start + 1] & 0x80 == 0;
        let removable_negative = bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0;
        if !removable_positive && !removable_negative {
            break;
        }
        start += 1;
    }
    bytes[start..].to_vec()
}

fn decode_minimal_i128(bytes: &[u8]) -> Result<i128, ValueValidationError> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(ValueValidationError::InvalidDecimalCoefficient);
    }
    if bytes.len() > 1 {
        let removable_positive = bytes[0] == 0 && bytes[1] & 0x80 == 0;
        let removable_negative = bytes[0] == 0xff && bytes[1] & 0x80 != 0;
        if removable_positive || removable_negative {
            return Err(ValueValidationError::NonCanonicalDecimalCoefficient);
        }
    }
    let mut padded = if bytes[0] & 0x80 == 0 {
        [0; 16]
    } else {
        [0xff; 16]
    };
    padded[16 - bytes.len()..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(padded))
}

/// A bounded, non-secret failure while checking a public value message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueValidationError {
    /// The encoded document exceeds the public value ceiling.
    DocumentTooLarge,
    /// Protobuf decoding failed.
    MalformedEncoding,
    /// A nested wire length, item count, or depth exceeds its pre-allocation limit.
    PreflightLimitExceeded,
    /// The `Value` oneof is absent.
    MissingKind,
    /// The null enum contains an unknown value.
    InvalidNull,
    /// A string exceeds the hard byte limit.
    StringTooLong,
    /// A byte value exceeds the hard byte limit.
    BytesTooLong,
    /// A list or record exceeds the hard item limit.
    TooManyItems,
    /// A list or record exceeds the hard nesting limit.
    NestingTooDeep,
    /// A protocol name exceeds its hard byte limit.
    NameTooLong,
    /// A record field provides neither a stable ID nor a name.
    MissingFieldIdentity,
    /// A present record field ID is the unassigned zero sentinel.
    InvalidFieldIdentity,
    /// A record field occurs more than once.
    DuplicateField,
    /// Record fields are not in canonical structural order.
    NonCanonicalRecordOrder,
    /// A nested message required by its enclosing value is absent.
    MissingNestedValue,
    /// A decimal coefficient is empty or wider than `i128`.
    InvalidDecimalCoefficient,
    /// A decimal coefficient contains redundant sign-extension bytes.
    NonCanonicalDecimalCoefficient,
    /// A wire decimal scale exceeds the structural maximum.
    DecimalScaleOutOfRange,
    /// A wire decimal does not have the expected compiled scale.
    DecimalTypeMismatch,
    /// A decimal coefficient exceeds the compiled precision.
    DecimalOutOfRange,
    /// A currency is not three uppercase ASCII letters.
    InvalidCurrency,
    /// A wire currency does not match the compiled money type.
    MoneyTypeMismatch,
    /// A UUID is not exactly 16 bytes.
    InvalidUuid,
    /// An enum type or variant ID is the unassigned zero sentinel.
    InvalidEnumIdentity,
    /// Timestamp nanoseconds are not canonical.
    InvalidTimestamp,
}

impl fmt::Display for ValueValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("public value failed canonical structural validation")
    }
}

impl Error for ValueValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{Date, EnumTypeId, EnumVariantId, FieldId};

    #[test]
    fn signed_coefficients_use_minimal_twos_complement() {
        let cases = [
            (0, vec![0]),
            (127, vec![0x7f]),
            (128, vec![0, 0x80]),
            (-128, vec![0x80]),
            (-129, vec![0xff, 0x7f]),
            (i128::MIN, i128::MIN.to_be_bytes().to_vec()),
            (i128::MAX, i128::MAX.to_be_bytes().to_vec()),
        ];
        for (value, encoded) in cases {
            assert_eq!(encode_minimal_i128(value), encoded);
            assert_eq!(decode_minimal_i128(&encoded), Ok(value));
        }
    }

    #[test]
    fn redundant_sign_extension_is_rejected() {
        assert_eq!(
            decode_minimal_i128(&[0, 0x7f]),
            Err(ValueValidationError::NonCanonicalDecimalCoefficient)
        );
        assert_eq!(
            decode_minimal_i128(&[0xff, 0x80]),
            Err(ValueValidationError::NonCanonicalDecimalCoefficient)
        );
    }

    #[test]
    fn context_supplies_decimal_precision() {
        let spec = DecimalSpec::new(3, 2).expect("valid decimal type");
        let wire = v1::Decimal {
            coefficient_twos_complement: vec![0x7b],
            scale: 2,
        };
        let value = decimal_from_proto(&wire, spec).expect("valid typed decimal");
        assert_eq!(value.coefficient(), 123);
        assert_eq!(decimal_to_proto(value), wire);
    }

    #[test]
    fn context_supplies_the_money_currency() {
        let spec = DecimalSpec::new(6, 2).expect("valid decimal type");
        let usd = CurrencyCode::new("USD").expect("valid currency");
        let eur = CurrencyCode::new("EUR").expect("valid currency");
        let wire = v1::Money {
            currency: "USD".to_owned(),
            amount: Some(v1::Decimal {
                coefficient_twos_complement: vec![1],
                scale: 2,
            }),
        };
        assert!(money_from_proto(&wire, spec, usd).is_ok());
        assert_eq!(
            money_from_proto(&wire, spec, eur),
            Err(ValueValidationError::MoneyTypeMismatch)
        );
    }

    #[test]
    fn canonical_record_output_uses_only_sorted_ids() {
        let value = CanonicalValue::record(vec![
            (
                FieldId::new(9).expect("field ID is nonzero"),
                CanonicalValue::I64(2),
            ),
            (
                FieldId::new(3).expect("field ID is nonzero"),
                CanonicalValue::I64(1),
            ),
        ])
        .expect("canonical record");
        let wire = canonical_value_to_proto(&value).expect("outbound value must validate");
        let v1::value::Kind::RecordValue(record) = wire.kind.expect("kind") else {
            panic!("expected record")
        };
        assert_eq!(
            record
                .fields
                .iter()
                .map(|field| field.field_id)
                .collect::<Vec<_>>(),
            vec![Some(3), Some(9)]
        );
        assert!(record.fields.iter().all(|field| field.name.is_empty()));
    }

    #[test]
    fn schema_identity_types_remain_exact() {
        let value = CanonicalValue::Enum {
            type_id: EnumTypeId::new(7).expect("enum type ID is nonzero"),
            variant_id: EnumVariantId::new(11).expect("enum variant ID is nonzero"),
        };
        let wire = canonical_value_to_proto(&value).expect("valid outbound value");
        let v1::value::Kind::EnumValue(value) = wire.kind.expect("kind") else {
            panic!("expected enum")
        };
        assert_eq!((value.type_id, value.variant_id), (7, 11));
        assert!(value.name.is_empty());
    }

    #[test]
    fn assigned_schema_id_zero_sentinels_are_rejected() {
        let enum_value = v1::Value {
            kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
                type_id: 0,
                variant_id: 1,
                name: String::new(),
            })),
        };
        assert_eq!(
            validate_value(&enum_value),
            Err(ValueValidationError::InvalidEnumIdentity)
        );
        assert_eq!(
            decode_value(&enum_value.encode_to_vec()),
            Err(ValueValidationError::InvalidEnumIdentity)
        );

        let enum_variant = v1::Value {
            kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
                type_id: 1,
                variant_id: 0,
                name: String::new(),
            })),
        };
        assert_eq!(
            validate_value(&enum_variant),
            Err(ValueValidationError::InvalidEnumIdentity)
        );
        assert_eq!(
            decode_value(&enum_variant.encode_to_vec()),
            Err(ValueValidationError::InvalidEnumIdentity)
        );

        let record = v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: vec![v1::ValueField {
                    field_id: Some(0),
                    name: String::new(),
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
                    }),
                }],
            })),
        };
        assert_eq!(
            validate_value(&record),
            Err(ValueValidationError::InvalidFieldIdentity)
        );
        assert_eq!(
            decode_value(&record.encode_to_vec()),
            Err(ValueValidationError::InvalidFieldIdentity)
        );
    }

    #[test]
    fn temporal_and_date_output_remains_exact() {
        let timestamp = Timestamp::new(-1, 999_999_999).expect("valid timestamp");
        canonical_value_to_proto(&CanonicalValue::Timestamp(timestamp))
            .expect("timestamp validates");
        canonical_value_to_proto(&CanonicalValue::Date(Date::new(i32::MIN)))
            .expect("date validates");
    }

    #[test]
    fn aggregate_outbound_document_limit_is_checked() {
        let part = CanonicalValue::string("x".repeat(600_000)).expect("bounded field");
        let value = CanonicalValue::list(vec![part.clone(), part]).expect("bounded list");
        assert_eq!(
            canonical_value_to_proto(&value),
            Err(ValueValidationError::DocumentTooLarge)
        );
    }
}
