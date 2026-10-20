//! Independent structural frame checks, not a publication or fixture authority.
use super::*;
use crate::NativeCatalogOriginEnvelopeDigestV1;
use riffdb_types::AdministrationSequence;
use sha2::{Digest, Sha256};
fn reference(sequence: u64, byte: u8) -> NativeCatalogOriginReferenceV1 {
    NativeCatalogOriginReferenceV1::new(
        AdministrationSequence::new(sequence).unwrap(),
        NativeCatalogOriginEnvelopeDigestV1::from_bytes([byte; 32]),
    )
}
// req: PRJ-004, STO-012
#[test]
fn source_origin_component_matches_independent_complete_frame_and_all_original_fields() {
    let database = DatabaseId::from_unix_milliseconds_and_random(42, [0x31; 10]).unwrap();
    let origin = reference(9, 0x41);
    let mut preimage = [0; 68];
    preimage[..4].copy_from_slice(&1_u32.to_be_bytes());
    preimage[4..20].copy_from_slice(database.as_bytes());
    preimage[20..28].copy_from_slice(&7_u64.to_be_bytes());
    preimage[28..36].copy_from_slice(&9_u64.to_be_bytes());
    preimage[36..].copy_from_slice(&[0x41; 32]);
    assert_eq!(
        projection_source_origin_preimage_v1(database, 7, origin).unwrap(),
        preimage
    );
    let domain = b"riffdb.projection-source-origin/v1";
    let mut frame = Sha256::new();
    frame.update(b"RIFFDB-HASH\0");
    frame.update([1]);
    frame.update(u16::try_from(domain.len()).unwrap().to_be_bytes());
    frame.update(domain);
    frame.update(68_u64.to_be_bytes());
    frame.update(preimage);
    let expected: [u8; 32] = frame.finalize().into();
    let actual = projection_source_origin_commitment_v1(database, 7, origin).unwrap();
    assert_eq!(actual.as_bytes(), &expected);
    assert_eq!(actual.domain(), HashDomain::ProjectionSourceOrigin);
    assert_ne!(actual.as_bytes(), origin.digest().as_bytes());
    for changed in [
        projection_source_origin_commitment_v1(
            DatabaseId::from_unix_milliseconds_and_random(43, [0x31; 10]).unwrap(),
            7,
            origin,
        )
        .unwrap(),
        projection_source_origin_commitment_v1(database, 8, origin).unwrap(),
        projection_source_origin_commitment_v1(database, 7, reference(10, 0x41)).unwrap(),
        projection_source_origin_commitment_v1(database, 7, reference(9, 0x42)).unwrap(),
    ] {
        assert_ne!(changed, actual);
    }
    assert!(projection_source_origin_commitment_v1(database, 0, origin).is_err());
}
