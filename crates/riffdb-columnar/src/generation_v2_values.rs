//! Schema-bound lowering into the unchanged Segment V2 physical registry.

use super::{CanonicalValue, ColumnarV2GenerationError, SegmentV2Cell, ValueType, ValueTypeTag};
use riffdb_types::{decode_canonical_value, encode_canonical_value};

pub(super) fn is_vector(value_type: &ValueType) -> bool {
    value_type.optional_inner().unwrap_or(value_type).tag() == ValueTypeTag::Vector
}

pub(super) fn lower(
    value: &CanonicalValue,
    value_type: &ValueType,
) -> Result<SegmentV2Cell, ColumnarV2GenerationError> {
    value_type
        .validate_value(value)
        .map_err(|_| ColumnarV2GenerationError::LogicalMismatch)?;
    if value == &CanonicalValue::Null {
        return Ok(SegmentV2Cell::Null);
    }
    let physical = if is_vector(value_type) {
        let bytes =
            encode_canonical_value(value).map_err(|_| ColumnarV2GenerationError::Invalid)?;
        CanonicalValue::bytes(bytes).map_err(|_| ColumnarV2GenerationError::BoundExceeded)?
    } else {
        value.clone()
    };
    Ok(SegmentV2Cell::Value(physical))
}

pub(super) fn restore(
    cell: &SegmentV2Cell,
    value_type: &ValueType,
) -> Result<CanonicalValue, ColumnarV2GenerationError> {
    let invalid = ColumnarV2GenerationError::Invalid;
    let value = match cell {
        SegmentV2Cell::Null => CanonicalValue::Null,
        SegmentV2Cell::Missing => return Err(invalid),
        SegmentV2Cell::Value(value) if is_vector(value_type) => {
            let CanonicalValue::Bytes(bytes) = value else {
                return Err(invalid);
            };
            let dimension = value_type
                .optional_inner()
                .unwrap_or(value_type)
                .vector_dimension()
                .ok_or(invalid)?
                .get();
            // The existing canonical document is version + tag + dimension + f32s.
            // Refuse other payload shapes before decoding or allocating a value.
            if bytes.len() != 6 + dimension as usize * 4 {
                return Err(invalid);
            }
            let decoded = decode_canonical_value(bytes.as_bytes()).map_err(|_| invalid)?;
            // Canonical decode rejects negative zero, non-finite components,
            // bad dimensions and trailing bytes. Re-encoding adds no evidence.
            if !matches!(decoded, CanonicalValue::Vector(_)) {
                return Err(invalid);
            }
            decoded
        }
        SegmentV2Cell::Value(value) => value.clone(),
    };
    value_type.validate_value(&value).map_err(|_| invalid)?;
    Ok(value)
}
