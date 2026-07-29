//! Protobuf value helpers for TicketDesk command and query payloads.

use riffdb_proto::v1;
use riffdb_types::{EntityKeyBuilder, EntityTypeId};

use crate::RiffDbError;

pub(crate) fn uuid_value(bytes: [u8; 16]) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::UuidValue(bytes.to_vec())),
    }
}

pub(crate) fn string_value(text: impl Into<String>) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::StringValue(text.into())),
    }
}

pub(crate) fn enum_value(type_id: u32, variant_id: u32, name: &str) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
            type_id,
            variant_id,
            name: name.to_owned(),
        })),
    }
}

pub(crate) fn field(field_id: u32, value: v1::Value) -> v1::ValueField {
    v1::ValueField {
        field_id: Some(field_id),
        name: String::new(),
        value: Some(value),
    }
}

pub(crate) fn record(mut fields: Vec<v1::ValueField>) -> v1::Value {
    fields.sort_by_key(|field| field.field_id.unwrap_or(0));
    v1::Value {
        kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
    }
}

pub(crate) fn entity_key(
    entity_type_id: u32,
    components: &[[u8; 16]],
) -> Result<Vec<u8>, RiffDbError> {
    let entity_type = EntityTypeId::new(entity_type_id).ok_or(RiffDbError::InvalidSchema)?;
    let mut builder = EntityKeyBuilder::new(entity_type);
    for component in components {
        builder
            .push_uuid(component)
            .map_err(|_| RiffDbError::InvalidSchema)?;
    }
    builder
        .finish()
        .map(|key| key.into_bytes())
        .map_err(|_| RiffDbError::InvalidSchema)
}

pub(crate) fn require_uuid(value: &v1::Value) -> Result<[u8; 16], RiffDbError> {
    match value.kind.as_ref() {
        Some(v1::value::Kind::UuidValue(bytes)) if bytes.len() == 16 => {
            let mut out = [0_u8; 16];
            out.copy_from_slice(bytes);
            Ok(out)
        }
        _ => Err(RiffDbError::Decode),
    }
}

pub(crate) fn require_string(value: &v1::Value) -> Result<String, RiffDbError> {
    match value.kind.as_ref() {
        Some(v1::value::Kind::StringValue(text)) => Ok(text.clone()),
        _ => Err(RiffDbError::Decode),
    }
}

pub(crate) fn field_map(
    record: &v1::ValueRecord,
) -> Result<std::collections::BTreeMap<u32, &v1::Value>, RiffDbError> {
    let mut map = std::collections::BTreeMap::new();
    for field in &record.fields {
        let Some(field_id) = field.field_id else {
            continue;
        };
        let value = field.value.as_ref().ok_or(RiffDbError::Decode)?;
        if map.insert(field_id, value).is_some() {
            return Err(RiffDbError::Decode);
        }
    }
    Ok(map)
}
