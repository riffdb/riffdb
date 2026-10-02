// req: OQ-040
use super::*;
use crate::storage::v1 as wire;
use prost::Message;
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

fn entity() -> wire::StoredEntityRecordV1 {
    wire::StoredEntityRecordV1 {
        target: Some(wire::EntityTargetV1 {
            entity_type_id: 1,
            entity_key: vec![1; 16],
        }),
        entity_version: 1,
        written_by_contract: 1,
        schema_binding: Some(wire::DurableKeySchemaBindingV1 {
            contract_lineage: "example".to_owned(),
            contract_version: 1,
            contract_bundle_hash: vec![1; 32],
        }),
        canonical_fields: riffdb_types::encode_canonical_record(
            &riffdb_types::CanonicalRecord::new(vec![]).unwrap(),
        )
        .unwrap(),
    }
}
#[test]
fn admitted_entity_decode_preserves_both_framings_and_does_not_retain_tls_scratch() {
    let message = entity();
    let schema =
        crate::durable::readable_record_schema("riffdb.storage.v1.StoredEntityRecordV1").unwrap();
    let payload = message.encode_to_vec();
    for bytes in [
        super::super::encode(schema, &payload).unwrap(),
        super::super::encode_v1(schema, &payload).unwrap(),
    ] {
        let ordinary =
            crate::durable::decode_readable_message::<wire::StoredEntityRecordV1>(&bytes).unwrap();
        let retained = super::super::CANONICAL_SCRATCH.with(|scratch| scratch.borrow().capacity());
        let budget = ProviderBudget::new();
        let inspection =
            inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(&bytes).unwrap();
        assert_eq!(inspection.canonical_payload(), message.canonical_fields);
        let mut scratch = ProviderDecodeScratchV1::new(bytes.len(), &budget).unwrap();
        let decoded = inspection.decode_with_scratch(&mut scratch).unwrap();
        assert_eq!(&**decoded, &ordinary);
        assert_eq!(
            retained,
            super::super::CANONICAL_SCRATCH.with(|scratch| scratch.borrow().capacity())
        );
        drop(scratch);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(decoded);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}

// req: OQ-040
#[test]
fn all_closed_provider_source_roles_preserve_v1_and_v2_decoding() {
    fn check<M: ProviderSourceMessageV1 + PartialEq + std::fmt::Debug>(message: M) {
        let schema = M::record_schema();
        let payload = message.encode_to_vec();
        for bytes in [
            super::super::encode(schema, &payload).unwrap(),
            super::super::encode_v1(schema, &payload).unwrap(),
        ] {
            let ordinary = crate::durable::decode_readable_message::<M>(&bytes).unwrap();
            let budget = ProviderBudget::new();
            let mut scratch = ProviderDecodeScratchV1::new(bytes.len(), &budget).unwrap();
            let decoded = inspect_provider_source_decode_v1::<M>(&bytes)
                .unwrap()
                .decode_with_scratch(&mut scratch)
                .unwrap();
            assert_eq!(&**decoded, &ordinary);
            let captured = decoded.clone();
            drop(decoded);
            drop(scratch);
            assert!(budget.observation().unwrap().owned_bytes > 0);
            drop(captured);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        }
    }
    check(entity());
    check(wire::StoredContractBundleV1 {
        contract_lineage: "example".to_owned(),
        contract_version: 1,
        contract_bundle_hash: vec![1; 32],
        canonical_bundle: vec![7; 32768],
    });
    check(wire::ActiveCatalogPointerV1 {
        contract_lineage: "example".to_owned(),
        contract_version: 1,
        contract_bundle_hash: vec![1; 32],
    });
    check(wire::StoredIndexGenerationV2 {
        partition_key: vec![1; 16],
        index_id: 1,
        schema_binding: entity().schema_binding,
        generation: 1,
    });

    let canonical = entity().canonical_fields;
    check(wire::StoredIndexEntryV1 {
        index_entry_key: vec![1; 16],
        schema_binding: entity().schema_binding,
        canonical_covered_values: canonical.clone(),
    });
    check(wire::StoredIndexEntryV2 {
        index_entry_key: vec![1; 16],
        schema_binding: entity().schema_binding,
        canonical_covered_values: canonical,
        partition_key: vec![1; 16],
    });
}

// req: OQ-040
#[test]
fn failed_provider_wire_decode_releases_only_its_own_admission() {
    let message = entity();
    let schema =
        crate::durable::readable_record_schema("riffdb.storage.v1.StoredEntityRecordV1").unwrap();
    let payload = message.encode_to_vec();
    let mut bytes = super::super::encode_v1(schema, &payload).unwrap();
    // An unknown envelope field passes borrowed bounds but fails the unchanged
    // byte-for-byte canonical envelope proof after generated envelope decoding.
    bytes.extend_from_slice(&[0x30, 0x01]);
    let ordinary =
        crate::durable::decode_readable_message::<wire::StoredEntityRecordV1>(&bytes).unwrap_err();
    assert_eq!(ordinary, EnvelopeError::NonCanonicalEnvelope);
    let budget = ProviderBudget::new();
    let mut scratch = ProviderDecodeScratchV1::new(bytes.len(), &budget).unwrap();
    let inspected =
        inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(&bytes).unwrap();
    assert!(matches!(
        inspected.decode_with_scratch(&mut scratch),
        Err(ProviderSourceDecodeErrorV1::Envelope(
            EnvelopeError::NonCanonicalEnvelope
        ))
    ));
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reservations, 1);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: OQ-040
#[test]
fn pressure_and_wrong_source_roles_refuse_before_message_publication() {
    let message = entity();
    let schema =
        crate::durable::readable_record_schema("riffdb.storage.v1.StoredEntityRecordV1").unwrap();
    let bytes = super::super::encode(schema, &message.encode_to_vec()).unwrap();
    assert!(matches!(
        inspect_provider_source_decode_v1::<wire::StoredIndexEntryV2>(&bytes),
        Err(EnvelopeError::UnknownRecordType)
    ));
    let budget = ProviderBudget::new();
    let mut scratch = ProviderDecodeScratchV1::new(bytes.len(), &budget).unwrap();
    let observed = budget.observation().unwrap();
    let occupied = budget
        .reserve(
            riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
                - observed.fixed_owner_bytes
                - observed.reserved_bytes,
        )
        .unwrap();
    assert!(matches!(
        inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(&bytes)
            .unwrap()
            .decode_with_scratch(&mut scratch),
        Err(ProviderSourceDecodeErrorV1::CapacityUnavailable)
    ));
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().released_allocations, 0);
    drop(occupied);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: OQ-040
#[test]
fn admitted_entity_wire_decoder_agrees_with_ordinary_decoder_on_byte_mutations() {
    let message = entity();
    let schema =
        crate::durable::readable_record_schema("riffdb.storage.v1.StoredEntityRecordV1").unwrap();
    for original in [
        super::super::encode(schema, &message.encode_to_vec()).unwrap(),
        super::super::encode_v1(schema, &message.encode_to_vec()).unwrap(),
    ] {
        for position in 0..original.len() {
            for mask in [1, 128] {
                let mut bytes = original.clone();
                bytes[position] ^= mask;
                let ordinary =
                    crate::durable::decode_readable_message::<wire::StoredEntityRecordV1>(&bytes);
                let budget = ProviderBudget::new();
                let mut scratch = ProviderDecodeScratchV1::new(bytes.len(), &budget).unwrap();
                let admitted =
                    inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(&bytes)
                        .map_err(ProviderSourceDecodeErrorV1::from)
                        .and_then(|input| input.decode_with_scratch(&mut scratch));
                assert_eq!(
                    ordinary.is_ok(),
                    admitted.is_ok(),
                    "decoder disagreement at position {position}"
                );
                if let (Ok(expected), Ok(actual)) = (&ordinary, &admitted) {
                    assert_eq!(expected, &***actual);
                }
                drop(admitted);
                drop(scratch);
                assert_eq!(budget.observation().unwrap().owned_bytes, 0);
                assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            }
        }
    }
}
