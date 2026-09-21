#![forbid(unsafe_code)]
//! Versioned catalog evidence never relabels old roots or grants source admission.
// req: REP-005, STO-012, REC-001

use riffdb_proto::{durable::encode_current_message, storage::v1 as wire};
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2, DurableCodecErrorKind,
    proto_codec::{
        decode_authoritative_state_catalog_v1 as decode_old,
        decode_authoritative_state_catalog_v2 as decode_new,
        encode_authoritative_state_catalog_v1 as encode_old,
        encode_authoritative_state_catalog_v2 as encode_new, encode_record_registry_v2,
    },
};

#[test]
fn catalog_generations_round_trip_without_cross_decoding_or_relabeling() {
    let old = encode_old(AuthoritativeStateCatalogV1).unwrap();
    let new = encode_new(AuthoritativeStateCatalogV2).unwrap();
    assert_eq!(
        decode_old(old.as_bytes()).unwrap().value(),
        &AuthoritativeStateCatalogV1
    );
    assert_eq!(
        decode_new(new.as_bytes()).unwrap().value(),
        &AuthoritativeStateCatalogV2
    );
    assert!(decode_old(new.as_bytes()).is_err());
    assert!(decode_new(old.as_bytes()).is_err());
    assert_eq!(
        new.encoded_content_charge(),
        decode_new(new.as_bytes()).unwrap().encoded_content_charge()
    );
    let old_hex = old
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(
        old_hex,
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v1.hex").trim()
    );
    let new_hex = new
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(
        new_hex,
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v2.hex").trim()
    );
}

#[test]
fn successor_catalog_refuses_foreign_old_missing_and_wrong_role_digests() {
    for digest in [AuthoritativeStateCatalogV1.digest(), [0x7e; 32]] {
        let forged = encode_current_message(&wire::StoredAuthoritativeStateCatalogV2 {
            catalog_digest: digest.to_vec(),
        })
        .unwrap();
        let error = decode_new(&forged).unwrap_err();
        assert_eq!(error.kind(), DurableCodecErrorKind::IncompatibleFormat);
        assert!(!format!("{error:?}").contains("7e7e"));
    }
    let missing = encode_current_message(&wire::StoredAuthoritativeStateCatalogV2 {
        catalog_digest: vec![],
    })
    .unwrap();
    assert_eq!(
        decode_new(&missing).unwrap_err().kind(),
        DurableCodecErrorKind::CorruptData
    );
    let registry = encode_record_registry_v2(riffdb_types::SchemaHash::from_bytes(
        AuthoritativeStateCatalogV2.digest(),
    ))
    .unwrap();
    assert!(decode_new(registry.as_bytes()).is_err());
    for size in [31, 33] {
        assert!(
            encode_current_message(&wire::StoredAuthoritativeStateCatalogV2 {
                catalog_digest: vec![0; size]
            })
            .is_err()
        );
    }
}

#[test]
fn successor_catalog_refuses_every_truncation_and_corruption() {
    let encoded = encode_new(AuthoritativeStateCatalogV2).unwrap();
    for length in 0..encoded.as_bytes().len() {
        assert!(decode_new(&encoded.as_bytes()[..length]).is_err());
    }
    for index in 0..encoded.as_bytes().len() {
        let mut corrupt = encoded.as_bytes().to_vec();
        corrupt[index] ^= 0x80;
        assert!(decode_new(&corrupt).is_err());
    }
    let mut trailing = encoded.as_bytes().to_vec();
    trailing.push(0);
    assert!(decode_new(&trailing).is_err());
}
