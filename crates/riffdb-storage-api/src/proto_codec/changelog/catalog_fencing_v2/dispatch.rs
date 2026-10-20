//! Exact known-version dispatch; malformed known evidence never falls back.
use super::{
    Admission, AuthoritativeStateCatalogV2, AuthoritativeStateCatalogV3, CanonicalStoredEnvelopeV1,
    DurableCodecError, DurableCodecErrorKind, EncodedPageItem, Fence, Promotion,
    decode_primary_fence_administration_v2, decode_promotion_administration_v2,
    decode_replication_primary_admission_v2, encode_primary_fence_administration_v2,
    encode_promotion_administration_v2, encode_replication_primary_admission_v2,
};

/// Selects the exact catalog-owned fence writer, never a caller-supplied version.
pub fn encode_primary_fence_administration(
    value: &Fence,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    match value.lineage().catalog_digest() {
        digest if digest == AuthoritativeStateCatalogV2.digest() => {
            super::super::encode_primary_fence_administration_v1(value)
        }
        digest if digest == AuthoritativeStateCatalogV3.digest() => {
            encode_primary_fence_administration_v2(value)
        }
        _ => Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        )),
    }
}
/// Dispatches only on exact outer record identity; corrupt evidence never falls back.
pub fn decode_primary_fence_administration(
    encoded: &[u8],
) -> Result<EncodedPageItem<Fence>, DurableCodecError> {
    match super::super::decode_primary_fence_administration_v1(encoded) {
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
            decode_primary_fence_administration_v2(encoded)
        }
        result => result,
    }
}
/// Selects admission encoding from its exact known lineage.
pub fn encode_replication_primary_admission(
    value: &Admission,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    match value.lineage().catalog_digest() {
        digest if digest == AuthoritativeStateCatalogV2.digest() => {
            super::super::encode_replication_primary_admission_v1(value)
        }
        digest if digest == AuthoritativeStateCatalogV3.digest() => {
            encode_replication_primary_admission_v2(value)
        }
        _ => Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        )),
    }
}
/// Reconstructs known admission versions without interpreting absent state as Active.
pub fn decode_replication_primary_admission(
    encoded: &[u8],
) -> Result<EncodedPageItem<Admission>, DurableCodecError> {
    match super::super::decode_replication_primary_admission_v1(encoded) {
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
            decode_replication_primary_admission_v2(encoded)
        }
        result => result,
    }
}
/// Selects promotion encoding from its immutable, checked published lineage.
pub fn encode_promotion_administration(
    value: &Promotion,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let selection = value
        .attempt()
        .selection()
        .ok_or_else(DurableCodecError::corrupt)?;
    match selection.published_lineage().catalog_digest() {
        digest if digest == AuthoritativeStateCatalogV2.digest() => {
            super::super::encode_promotion_administration_v1(value)
        }
        digest if digest == AuthoritativeStateCatalogV3.digest() => {
            encode_promotion_administration_v2(value)
        }
        _ => Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        )),
    }
}
/// Reconstructs exact promotion versions; schema/checksum/semantic errors refuse.
pub fn decode_promotion_administration(
    encoded: &[u8],
) -> Result<EncodedPageItem<Promotion>, DurableCodecError> {
    match super::super::decode_promotion_administration_v1(encoded) {
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
            decode_promotion_administration_v2(encoded)
        }
        result => result,
    }
}
