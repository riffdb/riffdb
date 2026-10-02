//! Borrowed canonical decode sizing; no decoded collection is constructed.
// req: OQ-040

use super::*;
use crate::OwnedCapacityInspectionErrorV1;
use crate::owned_capacity::allocation_upper_bound;
use std::mem::{align_of, size_of};

/// Complete borrowed sizing result; not a decoded or authenticated value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalDecodeCapacityBoundV1 {
    heap_bytes: u64,
}
impl CanonicalDecodeCapacityBoundV1 {
    /// Conservative decoded heap capacity. The containing root, transport
    /// buffers, verification copies, and derived indexes require separate admission.
    pub const fn owned_heap_bytes(&self) -> u64 {
        self.heap_bytes
    }
}

/// Compute decoded heap admission without allocating decoded data or a census.
/// The normal semantic decoder remains required before using the value.
#[doc(hidden)]
pub fn inspect_canonical_decode_capacity_v1(
    input: &[u8],
) -> Result<CanonicalDecodeCapacityBoundV1, OwnedCapacityInspectionErrorV1> {
    if input.len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    let mut decoder = Decoder { input, position: 0 };
    let mut heap_bytes = 0;
    inspect_value(&mut decoder, 0, &mut heap_bytes).map_err(|_| OwnedCapacityInspectionErrorV1)?;
    if decoder.position != input.len() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    Ok(CanonicalDecodeCapacityBoundV1 { heap_bytes })
}

fn charge<T>(count: usize, heap_bytes: &mut u64) -> Result<(), CanonicalCodecError> {
    let bytes = count
        .checked_mul(size_of::<T>())
        .ok_or(CanonicalCodecError::UnexpectedEnd)?;
    let bound = allocation_upper_bound(bytes, align_of::<T>())
        .map_err(|_| CanonicalCodecError::UnexpectedEnd)?;
    *heap_bytes = heap_bytes
        .checked_add(bound)
        .ok_or(CanonicalCodecError::UnexpectedEnd)?;
    Ok(())
}

fn inspect_value(
    decoder: &mut Decoder<'_>,
    depth: usize,
    heap_bytes: &mut u64,
) -> Result<(), CanonicalCodecError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(CanonicalCodecError::NestingTooDeep {
            depth,
            maximum: MAX_NESTING_DEPTH,
        });
    }
    let version = decoder.read_u8()?;
    if version != CANONICAL_VALUE_VERSION {
        return Err(CanonicalCodecError::UnsupportedVersion { version });
    }
    match decoder.read_u8()? {
        TAG_NULL => {}
        TAG_BOOL => match decoder.read_u8()? {
            0 | 1 => {}
            value => return Err(CanonicalCodecError::InvalidBoolean { value }),
        },
        TAG_I64 | TAG_U64 => {
            decoder.read_array::<8>()?;
        }
        TAG_DECIMAL => {
            decoder.decode_decimal()?;
        }
        TAG_MONEY => {
            let bytes = decoder.read_array::<3>()?;
            let currency =
                std::str::from_utf8(&bytes).map_err(|_| CanonicalCodecError::InvalidCurrency)?;
            CurrencyCode::new(currency).map_err(|_| CanonicalCodecError::InvalidCurrency)?;
            decoder.decode_decimal()?;
        }
        TAG_STRING => {
            let bytes = decoder.decode_bounded_bytes(BoundedKind::String)?;
            std::str::from_utf8(bytes).map_err(|_| CanonicalCodecError::InvalidUtf8)?;
            charge::<u8>(bytes.len(), heap_bytes)?;
        }
        TAG_BYTES => {
            let bytes = decoder.decode_bounded_bytes(BoundedKind::Bytes)?;
            charge::<u8>(bytes.len(), heap_bytes)?;
        }
        TAG_TIMESTAMP => {
            let seconds = i64::from_be_bytes(decoder.read_array()?);
            let nanos = u32::from_be_bytes(decoder.read_array()?);
            Timestamp::new(seconds, nanos).map_err(|_| CanonicalCodecError::InvalidTimestamp)?;
        }
        TAG_DATE => {
            decoder.read_array::<4>()?;
        }
        TAG_UUID => {
            decoder.read_array::<16>()?;
        }
        TAG_ENUM => {
            EnumTypeId::new(u32::from_be_bytes(decoder.read_array()?))
                .ok_or(CanonicalCodecError::ZeroEnumTypeId)?;
            EnumVariantId::new(u32::from_be_bytes(decoder.read_array()?))
                .ok_or(CanonicalCodecError::ZeroEnumVariantId)?;
        }
        TAG_LIST => {
            let count = decoder.read_collection_count(CollectionKind::List)?;
            charge::<CanonicalValue>(count, heap_bytes)?;
            for _ in 0..count {
                inspect_value(decoder, depth + 1, heap_bytes)?;
            }
        }
        TAG_RECORD => {
            let count = decoder.read_collection_count(CollectionKind::Record)?;
            charge::<(FieldId, CanonicalValue)>(count, heap_bytes)?;
            let mut previous = None;
            for _ in 0..count {
                let field = u32::from_be_bytes(decoder.read_array()?);
                if previous.is_some_and(|prior| prior >= field) {
                    return Err(CanonicalCodecError::NonCanonicalRecordOrder);
                }
                FieldId::new(field).ok_or(CanonicalCodecError::ZeroFieldId)?;
                previous = Some(field);
                inspect_value(decoder, depth + 1, heap_bytes)?;
            }
        }
        TAG_VECTOR => {
            let dimension = u32::from_be_bytes(decoder.read_array()?);
            if dimension == 0 || dimension > crate::MAX_VECTOR_DIMENSION {
                return Err(CanonicalCodecError::VectorDimensionOutOfRange {
                    actual: dimension,
                    maximum: crate::MAX_VECTOR_DIMENSION,
                });
            }
            charge::<f32>(dimension as usize, heap_bytes)?;
            for index in 0..dimension as usize {
                let bits = u32::from_be_bytes(decoder.read_array()?);
                if !f32::from_bits(bits).is_finite() || bits == (-0.0_f32).to_bits() {
                    return Err(CanonicalCodecError::NonCanonicalVectorComponent { index });
                }
            }
        }
        tag => return Err(CanonicalCodecError::UnknownTag { tag }),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CanonicalList, CanonicalValue, OwnedCapacityInspectionV1, encode_canonical_value};

    #[test]
    fn every_canonical_tag_bounds_the_real_decoded_capacity() {
        let decimal = crate::Decimal::new(crate::DecimalSpec::new(12, 2).unwrap(), 123).unwrap();
        let values = vec![
            CanonicalValue::Null,
            CanonicalValue::Bool(true),
            CanonicalValue::I64(-10),
            CanonicalValue::U64(10),
            CanonicalValue::Decimal(decimal),
            CanonicalValue::Money(crate::Money::new(
                crate::CurrencyCode::new("USD").unwrap(),
                decimal,
            )),
            CanonicalValue::String(crate::CanonicalString::new("ticket").unwrap()),
            CanonicalValue::Bytes(crate::CanonicalBytes::new(vec![1, 2, 3]).unwrap()),
            CanonicalValue::Timestamp(crate::Timestamp::new(-1, 123).unwrap()),
            CanonicalValue::Date(crate::Date::from_days_since_unix_epoch(-10)),
            CanonicalValue::Uuid([7; 16]),
            CanonicalValue::Enum {
                type_id: crate::EnumTypeId::first(),
                variant_id: crate::EnumVariantId::first(),
            },
            CanonicalValue::List(
                CanonicalList::new(vec![CanonicalValue::Null, CanonicalValue::Bool(false)])
                    .unwrap(),
            ),
            CanonicalValue::Record(
                crate::CanonicalRecord::new(vec![(
                    crate::FieldId::first(),
                    CanonicalValue::U64(1),
                )])
                .unwrap(),
            ),
            CanonicalValue::Vector(crate::CanonicalVector::new(vec![0.0, 1.25]).unwrap()),
        ];
        for value in values {
            let bytes = encode_canonical_value(&value).unwrap();
            let bound = inspect_canonical_decode_capacity_v1(&bytes).unwrap();
            let decoded = crate::decode_canonical_value(&bytes).unwrap();
            let observed = OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
            assert!(observed.shared_allocations().is_empty());
            assert!(bound.owned_heap_bytes() >= observed.unshared_bytes());
            assert_eq!(decoded, value);
            assert_eq!(encode_canonical_value(&decoded).unwrap(), bytes);
        }
    }

    #[test]
    fn truncated_or_noncanonical_documents_cannot_produce_an_admission_bound() {
        let list =
            CanonicalValue::List(CanonicalList::new(vec![CanonicalValue::Null; 32]).unwrap());
        let bytes = encode_canonical_value(&list).unwrap();
        for end in 0..bytes.len() {
            assert!(inspect_canonical_decode_capacity_v1(&bytes[..end]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(inspect_canonical_decode_capacity_v1(&trailing).is_err());
        for bytes in [
            vec![1, 255],
            vec![2, 0],
            vec![1, 1, 2],
            vec![1, 6, 0, 0, 0, 1, 255],
            vec![1, 12, 255, 255, 255, 255],
        ] {
            assert!(crate::decode_canonical_value(&bytes).is_err());
            assert!(inspect_canonical_decode_capacity_v1(&bytes).is_err());
        }
        assert!(
            inspect_canonical_decode_capacity_v1(&vec![0; MAX_CANONICAL_DOCUMENT_BYTES + 1])
                .is_err()
        );
    }

    #[test]
    fn duplicate_record_fields_and_invalid_vector_bits_are_refused() {
        let mut record = vec![1, TAG_RECORD];
        record.extend_from_slice(&2_u32.to_be_bytes());
        for _ in 0..2 {
            record.extend_from_slice(&1_u32.to_be_bytes());
            record.extend_from_slice(&[1, TAG_NULL]);
        }
        assert!(inspect_canonical_decode_capacity_v1(&record).is_err());
        for bits in [
            f32::NAN.to_bits(),
            f32::INFINITY.to_bits(),
            (-0.0_f32).to_bits(),
        ] {
            let mut vector = vec![1, TAG_VECTOR];
            vector.extend_from_slice(&1_u32.to_be_bytes());
            vector.extend_from_slice(&bits.to_be_bytes());
            assert!(inspect_canonical_decode_capacity_v1(&vector).is_err());
        }
    }

    #[test]
    fn excessive_depth_is_refused_without_constructing_nested_lists() {
        let mut bytes = Vec::new();
        for _ in 0..=MAX_NESTING_DEPTH {
            bytes.extend_from_slice(&[1, TAG_LIST]);
            bytes.extend_from_slice(&1_u32.to_be_bytes());
        }
        bytes.extend_from_slice(&[1, TAG_NULL]);
        assert!(inspect_canonical_decode_capacity_v1(&bytes).is_err());
        assert!(crate::decode_canonical_value(&bytes).is_err());
    }

    #[test]
    fn compact_null_lists_admit_the_decoded_slots_before_materialization() {
        let value =
            CanonicalValue::List(CanonicalList::new(vec![CanonicalValue::Null; 4096]).unwrap());
        let bytes = encode_canonical_value(&value).unwrap();
        let bound = inspect_canonical_decode_capacity_v1(&bytes).unwrap();
        assert!(bound.owned_heap_bytes() > u64::try_from(bytes.len()).unwrap() * 10);
        let decoded = crate::decode_canonical_value(&bytes).unwrap();
        let observed = OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
        assert!(bound.owned_heap_bytes() >= observed.unshared_bytes());
        assert_eq!(decoded, value);
    }
}
