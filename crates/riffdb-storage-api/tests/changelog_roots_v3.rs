#![forbid(unsafe_code)]
//! Closed catalog and monotonic leadership metadata cannot masquerade as other roots.
// req: REP-003, STO-012, REC-001

use riffdb_proto::{durable::encode_current_message, storage::v1 as wire};
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, DurableCodecErrorKind, LeadershipEpochV1,
    proto_codec::{
        decode_authoritative_state_catalog_v1 as decode_catalog,
        decode_leadership_epoch_v1 as decode_epoch,
        encode_authoritative_state_catalog_v1 as encode_catalog, encode_history_incarnation_v1,
        encode_leadership_epoch_v1 as encode_epoch, encode_record_registry_v2,
    },
};

#[test]
fn catalog_root_accepts_only_the_single_owned_inventory_not_another_digest_role() {
    let encoded = encode_catalog(AuthoritativeStateCatalogV1).unwrap();
    let decoded = decode_catalog(encoded.as_bytes()).unwrap();
    assert_eq!(
        decoded.value().digest(),
        AuthoritativeStateCatalogV1.digest()
    );
    assert_eq!(
        encoded.encoded_content_charge(),
        decoded.encoded_content_charge()
    );
    let foreign = encode_current_message(&wire::StoredAuthoritativeStateCatalogV1 {
        catalog_digest: vec![0x7e; 32],
    })
    .unwrap();
    let error = decode_catalog(&foreign).unwrap_err();
    assert_eq!(error.kind(), DurableCodecErrorKind::IncompatibleFormat);
    assert!(!format!("{error:?}").contains("7e7e"));
    let other_role = encode_record_registry_v2(riffdb_types::SchemaHash::from_bytes(
        AuthoritativeStateCatalogV1.digest(),
    ))
    .unwrap();
    assert!(decode_catalog(other_role.as_bytes()).is_err());
    // Protobuf permits omission of a bytes field; the semantic root does not.
    let missing = encode_current_message(&wire::StoredAuthoritativeStateCatalogV1 {
        catalog_digest: vec![],
    })
    .unwrap();
    assert_eq!(
        decode_catalog(&missing).unwrap_err().kind(),
        DurableCodecErrorKind::CorruptData
    );
    for payload in [vec![0; 31], vec![0; 33]] {
        assert!(
            encode_current_message(&wire::StoredAuthoritativeStateCatalogV1 {
                catalog_digest: payload,
            })
            .is_err()
        );
    }
}

#[test]
fn leadership_is_nonzero_checked_and_not_a_history_incarnation() {
    assert_eq!(LeadershipEpochV1::new(0), None);
    assert_eq!(LeadershipEpochV1::initial().get(), 1);
    assert_eq!(
        LeadershipEpochV1::initial().checked_next().unwrap().get(),
        2
    );
    for value in [1, 2, u64::MAX] {
        let epoch = LeadershipEpochV1::new(value).unwrap();
        let encoded = encode_epoch(epoch).unwrap();
        let decoded = decode_epoch(encoded.as_bytes()).unwrap();
        assert_eq!(*decoded.value(), epoch);
        assert_eq!(
            decoded.encoded_content_charge(),
            encoded.encoded_content_charge()
        );
        let other_role = encode_history_incarnation_v1(value).unwrap();
        assert!(decode_epoch(other_role.as_bytes()).is_err());
    }
    let maximum = LeadershipEpochV1::new(u64::MAX).unwrap();
    assert_eq!(maximum.checked_next(), None);
    assert_eq!(
        decode_epoch(encode_epoch(maximum).unwrap().as_bytes())
            .unwrap()
            .value()
            .checked_next(),
        None
    );
    let zero = encode_current_message(&wire::StoredLeadershipEpochV1 { epoch: 0 }).unwrap();
    assert!(decode_epoch(&zero).is_err());
}

#[test]
fn every_catalog_and_epoch_root_byte_is_checked_before_semantic_use() {
    for (encoded, catalog) in [
        (encode_catalog(AuthoritativeStateCatalogV1).unwrap(), true),
        (encode_epoch(LeadershipEpochV1::initial()).unwrap(), false),
    ] {
        let refuses = |bytes: &[u8]| {
            if catalog {
                decode_catalog(bytes).is_err()
            } else {
                decode_epoch(bytes).is_err()
            }
        };
        for offset in 0..encoded.as_bytes().len() {
            assert!(refuses(&encoded.as_bytes()[..offset]));
            let mut corrupt = encoded.as_bytes().to_vec();
            corrupt[offset] ^= 0x80;
            assert!(refuses(&corrupt));
        }
        let mut trailing = encoded.into_bytes();
        trailing.push(0);
        assert!(refuses(&trailing));
    }
}

#[test]
fn catalog_and_leadership_complete_envelope_vectors_are_frozen() {
    for (encoded, fixture, frozen) in [
        (
            encode_catalog(AuthoritativeStateCatalogV1).unwrap(),
            include_str!("../../../fixtures/replication/authoritative-state-catalog-v1.hex"),
            "5244423202450001000000220ddd4efe0a204ab9f21e9446ab94671415b4e705efdc78e848d589b59d5f4b8d2a268645eb81\n",
        ),
        (
            encode_epoch(LeadershipEpochV1::initial()).unwrap(),
            include_str!("../../../fixtures/replication/leadership-epoch-v1-first.hex"),
            "5244423202460001000000029e1e37690801\n",
        ),
        (
            encode_epoch(LeadershipEpochV1::new(u64::MAX).unwrap()).unwrap(),
            include_str!("../../../fixtures/replication/leadership-epoch-v1-last.hex"),
            "52444232024600010000000be390f85508ffffffffffffffffff01\n",
        ),
    ] {
        assert_eq!(fixture, frozen);
        let actual = encoded
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            + "\n";
        assert_eq!(actual, frozen);
    }
}
