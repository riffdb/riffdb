//! Private bounded JSON serialization and duplicate-safe parsing.

use std::{fmt, io};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundedJsonError;

pub(crate) fn to_vec<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: usize,
) -> Result<Vec<u8>, BoundedJsonError> {
    let mut writer = BoundedBuffer::new(maximum_bytes);
    serde_json::to_writer(&mut writer, value).map_err(|_| BoundedJsonError)?;
    Ok(writer.into_bytes())
}

pub(crate) fn to_string<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: usize,
) -> Result<String, BoundedJsonError> {
    String::from_utf8(to_vec(value, maximum_bytes)?).map_err(|_| BoundedJsonError)
}

pub(crate) fn encoded_len<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: usize,
) -> Result<usize, BoundedJsonError> {
    let mut writer = BoundedCounter::new(maximum_bytes);
    serde_json::to_writer(&mut writer, value).map_err(|_| BoundedJsonError)?;
    Ok(writer.length)
}

pub(crate) fn parse_unique(bytes: &[u8], maximum_bytes: usize) -> Result<Value, BoundedJsonError> {
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(BoundedJsonError);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueJsonValue::deserialize(&mut deserializer)
        .map_err(|_| BoundedJsonError)?
        .0;
    deserializer.end().map_err(|_| BoundedJsonError)?;
    Ok(value)
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedBuffer {
    const fn new(maximum_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum_bytes,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next_length = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(limit_error)?;
        if next_length > self.maximum_bytes {
            return Err(limit_error());
        }
        self.bytes
            .try_reserve_exact(buffer.len())
            .map_err(|_| limit_error())?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BoundedCounter {
    length: usize,
    maximum_bytes: usize,
}

impl BoundedCounter {
    const fn new(maximum_bytes: usize) -> Self {
        Self {
            length: 0,
            maximum_bytes,
        }
    }
}

impl io::Write for BoundedCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next_length = self
            .length
            .checked_add(buffer.len())
            .ok_or_else(limit_error)?;
        if next_length > self.maximum_bytes {
            return Err(limit_error());
        }
        self.length = next_length;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn limit_error() -> io::Error {
    io::Error::other("bounded JSON unavailable")
}

struct UniqueJsonValue(Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("one JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJsonValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value.to_owned())))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueJsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1_024));
        while let Some(value) = sequence.next_element::<UniqueJsonValue>()? {
            values.push(value.0);
        }
        Ok(UniqueJsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Map::new();
        while let Some(key) = entries.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(serde::de::Error::custom("duplicate JSON object key"));
            }
            let value = entries.next_value::<UniqueJsonValue>()?;
            object.insert(key, value.0);
        }
        Ok(UniqueJsonValue(Value::Object(object)))
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::SerializeMap;

    use super::*;

    struct DuplicateMap;

    impl Serialize for DuplicateMap {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("same", &1)?;
            map.serialize_entry("same", &2)?;
            map.end()
        }
    }

    #[test]
    fn serialization_checks_before_each_write_and_count_matches() {
        assert_eq!(to_vec(&"12345", 7).expect("exact"), br#""12345""#);
        assert_eq!(encoded_len(&"12345", 7), Ok(7));
        assert_eq!(to_vec(&"12345", 6), Err(BoundedJsonError));
        assert_eq!(encoded_len(&"12345", 6), Err(BoundedJsonError));
    }

    #[test]
    fn parser_rejects_duplicates_recursively_and_trailing_input() {
        assert_eq!(
            parse_unique(br#"{"outer":{"same":1,"same":2}}"#, 64),
            Err(BoundedJsonError)
        );
        assert_eq!(
            parse_unique(br#"{"value":1} {"value":2}"#, 64),
            Err(BoundedJsonError)
        );
        let duplicate = to_vec(&DuplicateMap, 64).expect("serializer can emit duplicate keys");
        assert_eq!(parse_unique(&duplicate, 64), Err(BoundedJsonError));
    }
}
