use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use riffdb_client_rust::v1;
use serde::de::Error as _;
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum InputValue {
    Null {},
    Bool {
        value: bool,
    },
    I64 {
        #[serde(deserialize_with = "deserialize_i64")]
        value: i64,
    },
    U64 {
        #[serde(deserialize_with = "deserialize_u64")]
        value: u64,
    },
    Decimal {
        #[serde(deserialize_with = "deserialize_base64")]
        coefficient_twos_complement: Vec<u8>,
        scale: u32,
    },
    Money {
        currency: String,
        amount: InputDecimal,
    },
    String {
        value: String,
    },
    Bytes {
        #[serde(deserialize_with = "deserialize_base64")]
        value: Vec<u8>,
    },
    Uuid {
        #[serde(deserialize_with = "deserialize_uuid")]
        value: [u8; 16],
    },
    Date {
        days_since_unix_epoch: i32,
    },
    Timestamp {
        #[serde(deserialize_with = "deserialize_i64")]
        seconds: i64,
        nanos: u32,
    },
    Enum {
        type_id: u32,
        variant_id: u32,
        #[serde(default, deserialize_with = "deserialize_optional_string")]
        name: Option<String>,
    },
    List {
        values: Vec<InputValue>,
    },
    Record {
        fields: Vec<InputField>,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputDecimal {
    #[serde(deserialize_with = "deserialize_base64")]
    coefficient_twos_complement: Vec<u8>,
    scale: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputField {
    #[serde(default, deserialize_with = "deserialize_optional_u32")]
    field_id: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    name: Option<String>,
    value: InputValue,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordInput {
    #[serde(rename = "type", deserialize_with = "deserialize_record_tag")]
    _type: (),
    fields: Vec<InputField>,
}

impl RecordInput {
    pub(crate) fn into_proto(self) -> Result<v1::Value, ValueError> {
        let fields = self
            .fields
            .into_iter()
            .map(InputField::into_proto)
            .collect::<Result<_, _>>()?;
        Ok(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
        })
    }
}

impl InputValue {
    pub(crate) fn into_proto(self) -> Result<v1::Value, ValueError> {
        let kind = match self {
            Self::Null {} => v1::value::Kind::NullValue(v1::NullValue::NullValue as i32),
            Self::Bool { value } => v1::value::Kind::BoolValue(value),
            Self::I64 { value } => v1::value::Kind::I64Value(value),
            Self::U64 { value } => v1::value::Kind::U64Value(value),
            Self::Decimal {
                coefficient_twos_complement,
                scale,
            } => v1::value::Kind::DecimalValue(v1::Decimal {
                coefficient_twos_complement,
                scale,
                precision: None,
            }),
            Self::Money { currency, amount } => v1::value::Kind::MoneyValue(v1::Money {
                currency,
                amount: Some(v1::Decimal {
                    coefficient_twos_complement: amount.coefficient_twos_complement,
                    scale: amount.scale,
                    precision: None,
                }),
            }),
            Self::String { value } => v1::value::Kind::StringValue(value),
            Self::Bytes { value } => v1::value::Kind::BytesValue(value),
            Self::Uuid { value } => v1::value::Kind::UuidValue(value.to_vec()),
            Self::Date {
                days_since_unix_epoch,
            } => v1::value::Kind::DateValue(v1::Date {
                days_since_unix_epoch,
            }),
            Self::Timestamp { seconds, nanos } => {
                v1::value::Kind::TimestampValue(v1::Timestamp { seconds, nanos })
            }
            Self::Enum {
                type_id,
                variant_id,
                name,
            } => {
                if type_id == 0 || variant_id == 0 {
                    return Err(ValueError);
                }
                v1::value::Kind::EnumValue(v1::EnumValue {
                    type_id,
                    variant_id,
                    name: name.unwrap_or_default(),
                })
            }
            Self::List { values } => v1::value::Kind::ListValue(v1::ValueList {
                values: values
                    .into_iter()
                    .map(Self::into_proto)
                    .collect::<Result<_, _>>()?,
            }),
            Self::Record { fields } => v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: fields
                    .into_iter()
                    .map(InputField::into_proto)
                    .collect::<Result<_, _>>()?,
            }),
        };
        Ok(v1::Value { kind: Some(kind) })
    }
}

impl InputField {
    fn into_proto(self) -> Result<v1::ValueField, ValueError> {
        if self.field_id == Some(0)
            || self.name.as_ref().is_some_and(|name| name.is_empty())
            || (self.field_id.is_none() && self.name.is_none())
        {
            return Err(ValueError);
        }
        Ok(v1::ValueField {
            field_id: self.field_id,
            name: self.name.unwrap_or_default(),
            value: Some(self.value.into_proto()?),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValueError;

pub(crate) struct OutputValue<'a>(pub(crate) &'a v1::Value);
pub(crate) struct OutputRecord<'a>(pub(crate) &'a v1::ValueRecord);
pub(crate) struct PaddedBytes<'a>(pub(crate) &'a [u8]);
pub(crate) struct LowerHex<'a>(pub(crate) &'a [u8]);
pub(crate) struct UuidText<'a>(pub(crate) &'a [u8]);

impl Serialize for OutputValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use v1::value::Kind;
        let kind = self
            .0
            .kind
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing value kind"))?;
        let mut map = serializer.serialize_map(None)?;
        match kind {
            Kind::NullValue(value) if *value == v1::NullValue::NullValue as i32 => {
                map.serialize_entry("type", "null")?;
            }
            Kind::BoolValue(value) => {
                map.serialize_entry("type", "bool")?;
                map.serialize_entry("value", value)?;
            }
            Kind::I64Value(value) => {
                map.serialize_entry("type", "i64")?;
                map.serialize_entry("value", &value.to_string())?;
            }
            Kind::U64Value(value) => {
                map.serialize_entry("type", "u64")?;
                map.serialize_entry("value", &value.to_string())?;
            }
            Kind::DecimalValue(value) => {
                map.serialize_entry("type", "decimal")?;
                map.serialize_entry(
                    "coefficient_twos_complement",
                    &PaddedBytes(&value.coefficient_twos_complement),
                )?;
                map.serialize_entry("scale", &value.scale)?;
            }
            Kind::MoneyValue(value) => {
                let amount = value
                    .amount
                    .as_ref()
                    .ok_or_else(|| S::Error::custom("missing money amount"))?;
                map.serialize_entry("type", "money")?;
                map.serialize_entry("currency", &value.currency)?;
                map.serialize_entry("amount", &OutputDecimal(amount))?;
            }
            Kind::StringValue(value) => {
                map.serialize_entry("type", "string")?;
                map.serialize_entry("value", value)?;
            }
            Kind::BytesValue(value) => {
                map.serialize_entry("type", "bytes")?;
                map.serialize_entry("value", &PaddedBytes(value))?;
            }
            Kind::UuidValue(value) => {
                map.serialize_entry("type", "uuid")?;
                map.serialize_entry("value", &UuidText(value))?;
            }
            Kind::DateValue(value) => {
                map.serialize_entry("type", "date")?;
                map.serialize_entry("days_since_unix_epoch", &value.days_since_unix_epoch)?;
            }
            Kind::TimestampValue(value) => {
                map.serialize_entry("type", "timestamp")?;
                map.serialize_entry("seconds", &value.seconds.to_string())?;
                map.serialize_entry("nanos", &value.nanos)?;
            }
            Kind::EnumValue(value) => {
                if value.type_id == 0 || value.variant_id == 0 {
                    return Err(S::Error::custom("zero enum identifier"));
                }
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("type_id", &value.type_id)?;
                map.serialize_entry("variant_id", &value.variant_id)?;
                if !value.name.is_empty() {
                    map.serialize_entry("name", &value.name)?;
                }
            }
            Kind::ListValue(value) => {
                map.serialize_entry("type", "list")?;
                map.serialize_entry("values", &OutputValues(&value.values))?;
            }
            Kind::RecordValue(value) => {
                map.serialize_entry("type", "record")?;
                map.serialize_entry("fields", &OutputFields(&value.fields))?;
            }
            Kind::NullValue(_) => return Err(S::Error::custom("invalid null enum")),
        }
        map.end()
    }
}

struct OutputDecimal<'a>(&'a v1::Decimal);

impl Serialize for OutputDecimal<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(
            "coefficient_twos_complement",
            &PaddedBytes(&self.0.coefficient_twos_complement),
        )?;
        map.serialize_entry("scale", &self.0.scale)?;
        map.end()
    }
}

struct OutputValues<'a>(&'a [v1::Value]);

impl Serialize for OutputValues<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&OutputValue(value))?;
        }
        sequence.end()
    }
}

struct OutputFields<'a>(&'a [v1::ValueField]);

impl Serialize for OutputFields<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut previous = 0_u32;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for field in self.0 {
            let field_id = field
                .field_id
                .filter(|field_id| *field_id > previous)
                .ok_or_else(|| S::Error::custom("invalid output field ordering"))?;
            previous = field_id;
            sequence.serialize_element(&OutputField(field))?;
        }
        sequence.end()
    }
}

struct OutputField<'a>(&'a v1::ValueField);

impl Serialize for OutputField<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(
            "field_id",
            &self
                .0
                .field_id
                .filter(|value| *value != 0)
                .ok_or_else(|| S::Error::custom("missing output field ID"))?,
        )?;
        map.serialize_entry(
            "value",
            &OutputValue(
                self.0
                    .value
                    .as_ref()
                    .ok_or_else(|| S::Error::custom("missing output field value"))?,
            ),
        )?;
        map.end()
    }
}

impl Serialize for OutputRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("fields", &OutputFields(&self.0.fields))?;
        map.end()
    }
}

impl Serialize for PaddedBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(self.0))
    }
}

impl Serialize for LowerHex<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.len() != 32 {
            return Err(S::Error::custom("hash is not 32 bytes"));
        }
        serializer.serialize_str(&lower_hex(self.0))
    }
}

impl Serialize for UuidText<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_uuid(self.0).ok_or_else(|| S::Error::custom("UUID"))?)
    }
}

pub(crate) fn format_uuid(bytes: &[u8]) -> Option<String> {
    let bytes: &[u8; 16] = bytes.try_into().ok()?;
    let mut output = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        write!(&mut output, "{byte:02x}").ok()?;
    }
    Some(output)
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn deserialize_record_tag<'de, D: Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
    let value = String::deserialize(deserializer)?;
    if value == "record" {
        Ok(())
    } else {
        Err(D::Error::custom("expected record"))
    }
}

fn deserialize_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let value = String::deserialize(deserializer)?;
    if !canonical_signed(&value) {
        return Err(D::Error::custom("noncanonical i64"));
    }
    value.parse().map_err(D::Error::custom)
}

fn deserialize_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = String::deserialize(deserializer)?;
    if !canonical_unsigned(&value) {
        return Err(D::Error::custom("noncanonical u64"));
    }
    value.parse().map_err(D::Error::custom)
}

fn deserialize_base64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    let value = String::deserialize(deserializer)?;
    let decoded = STANDARD
        .decode(value.as_bytes())
        .map_err(D::Error::custom)?;
    if STANDARD.encode(&decoded) != value {
        return Err(D::Error::custom("noncanonical base64"));
    }
    Ok(decoded)
}

fn deserialize_uuid<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 16], D::Error> {
    let value = String::deserialize(deserializer)?;
    parse_uuid(&value).ok_or_else(|| D::Error::custom("invalid canonical UUID"))
}

fn deserialize_optional_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

fn deserialize_optional_u32<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u32>, D::Error> {
    u32::deserialize(deserializer).map(Some)
}

pub(crate) fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    if value.len() != 36
        || !matches!(
            value.as_bytes(),
            [
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                b'-',
                _,
                _,
                _,
                _,
                b'-',
                _,
                _,
                _,
                _,
                b'-',
                _,
                _,
                _,
                _,
                b'-',
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _
            ]
        )
    {
        return None;
    }
    let mut output = [0_u8; 16];
    let mut digits = value.bytes().filter(|byte| *byte != b'-');
    for byte in &mut output {
        *byte = hex(digits.next()?)?.checked_mul(16)? + hex(digits.next()?)?;
    }
    digits.next().is_none().then_some(output)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn canonical_unsigned(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

fn canonical_signed(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    let digits = value.strip_prefix('-').unwrap_or(value);
    !digits.is_empty()
        && !digits.starts_with('0')
        && digits.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use riffdb_client_rust::IdempotentCommand;

    use super::*;

    #[test]
    fn exact_scalar_forms_round_trip_without_json_number_loss() {
        let input: InputValue =
            serde_json::from_str(r#"{"type":"u64","value":"18446744073709551615"}"#).expect("u64");
        let value = input.into_proto().expect("proto");
        assert_eq!(
            serde_json::to_string(&OutputValue(&value)).expect("render"),
            r#"{"type":"u64","value":"18446744073709551615"}"#
        );

        let input: InputValue =
            serde_json::from_str(r#"{"type":"i64","value":"-9223372036854775808"}"#).expect("i64");
        let value = input.into_proto().expect("proto");
        assert_eq!(
            serde_json::to_string(&OutputValue(&value)).expect("render"),
            r#"{"type":"i64","value":"-9223372036854775808"}"#
        );
    }

    #[test]
    fn alternate_integer_base64_uuid_and_unknown_keys_reject() {
        for input in [
            r#"{"type":"u64","value":"01"}"#,
            r#"{"type":"i64","value":"-0"}"#,
            r#"{"type":"bytes","value":"AQI"}"#,
            r#"{"type":"uuid","value":"01234567-89AB-7def-8123-456789abcdef"}"#,
            r#"{"type":"enum","type_id":1,"variant_id":1,"name":null}"#,
            r#"{"type":"null","extra":true}"#,
        ] {
            assert!(
                serde_json::from_str::<InputValue>(input).is_err(),
                "{input}"
            );
        }
        assert!(
            serde_json::from_str::<RecordInput>(
                r#"{"type":"record","fields":[{"field_id":null,"name":"x","value":{"type":"null"}}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn output_record_requires_ids_in_strict_order_and_omits_names() {
        let record = v1::ValueRecord {
            fields: vec![v1::ValueField {
                field_id: Some(1),
                name: "schema_bound_name".to_owned(),
                value: Some(v1::Value {
                    kind: Some(v1::value::Kind::NullValue(0)),
                }),
            }],
        };
        assert_eq!(
            serde_json::to_string(&OutputRecord(&record)).expect("record"),
            r#"{"fields":[{"field_id":1,"value":{"type":"null"}}]}"#
        );
    }

    #[test]
    fn scalar_and_optional_forms_cover_the_closed_value_family() {
        let values = [
            r#"{"type":"null"}"#,
            r#"{"type":"bool","value":true}"#,
            r#"{"type":"i64","value":"0"}"#,
            r#"{"type":"i64","value":"-9223372036854775808"}"#,
            r#"{"type":"u64","value":"18446744073709551615"}"#,
            r#"{"type":"decimal","coefficient_twos_complement":"AA==","scale":0}"#,
            r#"{"type":"money","currency":"USD","amount":{"coefficient_twos_complement":"AQ==","scale":2}}"#,
            r#"{"type":"string","value":""}"#,
            r#"{"type":"bytes","value":""}"#,
            r#"{"type":"uuid","value":"01234567-89ab-7def-8123-456789abcdef"}"#,
            r#"{"type":"date","days_since_unix_epoch":-1}"#,
            r#"{"type":"timestamp","seconds":"-1","nanos":999999999}"#,
            r#"{"type":"enum","type_id":1,"variant_id":1}"#,
            r#"{"type":"list","values":[]}"#,
            r#"{"type":"record","fields":[]}"#,
        ];
        for input in values {
            let value = serde_json::from_str::<InputValue>(input)
                .expect(input)
                .into_proto()
                .expect("proto");
            assert!(checked_as_command_value(value).is_ok(), "{input}");
        }

        for input in [
            r#"{"type":"decimal","coefficient_twos_complement":"","scale":0}"#,
            r#"{"type":"decimal","coefficient_twos_complement":"AAA=","scale":0}"#,
            r#"{"type":"money","currency":"usd","amount":{"coefficient_twos_complement":"AQ==","scale":2}}"#,
            r#"{"type":"timestamp","seconds":"0","nanos":1000000000}"#,
            r#"{"type":"enum","type_id":0,"variant_id":1}"#,
        ] {
            let value = serde_json::from_str::<InputValue>(input)
                .expect("structural JSON")
                .into_proto();
            assert!(
                value.is_err() || checked_as_command_value(value.expect("value")).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn sdk_depth_and_collection_limits_are_preserved_without_truncation() {
        let accepted_depth = nested_lists(31);
        assert!(checked_as_command_value(accepted_depth).is_ok());
        let excessive_depth = nested_lists(32);
        assert!(checked_as_command_value(excessive_depth).is_err());

        let accepted_list = v1::Value {
            kind: Some(v1::value::Kind::ListValue(v1::ValueList {
                values: vec![null_value(); 65_535],
            })),
        };
        assert!(checked_as_command_value(accepted_list).is_ok());
        let excessive_list = v1::Value {
            kind: Some(v1::value::Kind::ListValue(v1::ValueList {
                values: vec![null_value(); 65_536],
            })),
        };
        assert!(checked_as_command_value(excessive_list).is_err());

        let accepted_record = (1..=65_535)
            .map(|field_id| v1::ValueField {
                field_id: Some(field_id),
                name: String::new(),
                value: Some(null_value()),
            })
            .collect();
        assert!(
            IdempotentCommand::new(
                "Bounded",
                None,
                v1::Value {
                    kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                        fields: accepted_record,
                    })),
                },
            )
            .is_ok()
        );
        let excessive_record = (1..=65_536)
            .map(|field_id| v1::ValueField {
                field_id: Some(field_id),
                name: String::new(),
                value: Some(null_value()),
            })
            .collect();
        assert!(
            IdempotentCommand::new(
                "Bounded",
                None,
                v1::Value {
                    kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                        fields: excessive_record,
                    })),
                },
            )
            .is_err()
        );
    }

    fn checked_as_command_value(value: v1::Value) -> Result<IdempotentCommand, ()> {
        IdempotentCommand::new(
            "Bounded",
            None,
            v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                    fields: vec![v1::ValueField {
                        field_id: Some(1),
                        name: String::new(),
                        value: Some(value),
                    }],
                })),
            },
        )
        .map_err(|_| ())
    }

    fn nested_lists(depth: usize) -> v1::Value {
        let mut value = null_value();
        for _ in 0..depth {
            value = v1::Value {
                kind: Some(v1::value::Kind::ListValue(v1::ValueList {
                    values: vec![value],
                })),
            };
        }
        value
    }

    fn null_value() -> v1::Value {
        v1::Value {
            kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
        }
    }
}
