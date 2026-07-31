use riffdb_proto::{
    durable::READABLE_RECORD_SCHEMAS,
    envelope::{EnvelopeError, maximum_encoded_envelope_bytes},
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, FieldId, IndexEntryKeyBuilder, IndexId,
    MAX_CANONICAL_DOCUMENT_BYTES,
};

use crate::{
    AffectedEpochCurrentState, AffectedIndexEpochTargets, CommandWriteClassBreakdownV1,
    CommandWriteSetPlanV1, EncodedWriteSetUpperBound, IndexEntryMutationV1, StoredIndexEntryV2,
};

use super::super::*;
use super::sample;

#[test]
fn every_semantic_fixture_reports_its_exact_complete_envelope_charge() {
    let vectors = super::semantic_wire_vectors();
    assert_eq!(vectors.len(), 26);
    for ((name, envelope), schema) in vectors.iter().zip(READABLE_RECORD_SCHEMAS.iter()) {
        assert_eq!(*name, schema.record_type());
        assert_eq!(
            envelope.encoded_content_charge().get(),
            envelope.as_bytes().len(),
            "{}",
            schema.record_type()
        );
        assert!(
            envelope.as_bytes().len() <= schema.max_envelope_bytes(),
            "{}",
            schema.record_type()
        );
    }
}

#[test]
fn every_generated_record_bound_accepts_equal_and_rejects_one_over() {
    for schema in &READABLE_RECORD_SCHEMAS {
        assert_eq!(
            maximum_encoded_envelope_bytes(schema, schema.max_payload_bytes())
                .expect("generated payload ceiling fits"),
            schema.max_envelope_bytes(),
            "{}",
            schema.record_type()
        );
        assert_eq!(
            maximum_encoded_envelope_bytes(schema, schema.max_payload_bytes() + 1),
            Err(EnvelopeError::PayloadTooLarge),
            "{}",
            schema.record_type()
        );
    }
}

#[test]
fn encoded_atomic_graph_charges_exact_bytes_and_fits_its_reservation() {
    let records = sample::atomic_record_set();
    let encoded = encode_atomic_command_record_set_v1(&records).expect("atomic graph encodes");
    let actual = encoded.actual_charge();

    assert_eq!(actual.allocator(), encoded.allocator().as_bytes().len());
    assert_eq!(actual.pending_resolution(), 0);
    assert_eq!(
        actual.entities(),
        encoded
            .entities()
            .iter()
            .map(|value| value.as_bytes().len())
            .sum::<usize>()
    );
    assert_eq!(
        actual.index_entries(),
        encoded
            .index_entries()
            .iter()
            .flatten()
            .map(|value| value.as_bytes().len())
            .sum::<usize>()
    );
    assert_eq!(
        actual.index_epochs(),
        encoded
            .index_epochs()
            .iter()
            .map(|value| value.as_bytes().len())
            .sum::<usize>()
    );
    assert_eq!(actual.outcome(), encoded.outcome().as_bytes().len());
    assert_eq!(
        actual.events(),
        encoded
            .events()
            .iter()
            .map(|value| value.as_bytes().len())
            .chain(
                encoded
                    .event_routes()
                    .iter()
                    .map(|value| value.as_bytes().len()),
            )
            .sum::<usize>()
    );
    assert_eq!(
        actual.outbox_intents(),
        encoded
            .outbox_intents()
            .iter()
            .map(|value| value.as_bytes().len())
            .sum::<usize>()
    );
    assert_eq!(actual.provenance(), encoded.provenance().as_bytes().len());
    assert_eq!(actual.commit(), encoded.commit().as_bytes().len());

    let reserved = records.presequence_charge().encoded_upper_bound();
    verify_actual_write_set_charge_v1(actual, reserved).expect("actual graph fits reservation");
    assert!(actual.total().expect("actual total") <= reserved.total());
}

#[test]
fn exact_write_set_reservation_accepts_equal_and_rejects_one_byte_over() {
    let actual = encode_atomic_command_record_set_v1(&sample::atomic_record_set())
        .expect("atomic graph encodes")
        .actual_charge();
    let exact = EncodedWriteSetUpperBound::new(actual).expect("nonzero exact reservation");
    verify_actual_write_set_charge_v1(actual, exact).expect("equal reservation succeeds");

    let one_over = CommandWriteClassBreakdownV1::new(
        actual.allocator() + 1,
        actual.pending_resolution(),
        actual.entities(),
        actual.index_entries(),
        actual.index_epochs(),
        actual.outcome(),
        actual.events(),
        actual.outbox_intents(),
        actual.provenance(),
        actual.commit(),
    )
    .expect("fixture remains below aggregate hard limit");
    assert_eq!(
        verify_actual_write_set_charge_v1(one_over, exact)
            .expect_err("one byte above a class reservation fails")
            .kind(),
        DurableCodecErrorKind::ReservationExceeded
    );
}

#[test]
fn pending_and_index_deletes_have_zero_encoded_but_nonzero_semantic_charge() {
    let intent = sample::commit_intent();
    let (put, _) = sample::index_records();

    let mut delete_key = IndexEntryKeyBuilder::new(IndexId::first());
    delete_key
        .push_str("delete-a")
        .expect("delete key component");
    let delete_key = delete_key
        .finish(sample::entity_target().key().clone())
        .expect("delete key");

    let mut mutations = vec![
        IndexEntryMutationV1::Delete(delete_key),
        IndexEntryMutationV1::Put(put.clone()),
    ];
    mutations.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));

    let reservation = match command_write_set_upper_bound_v1(&intent, &mutations, &[])
        .expect("mixed index reservation")
    {
        EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
        EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
            panic!("small mixed fixture must fit")
        }
    };
    let put_charge = encode_index_entry_v2(&put)
        .expect("put post-image encodes")
        .encoded_content_charge()
        .get();
    assert_eq!(reservation.classes().pending_resolution(), 0);
    assert_eq!(reservation.classes().index_entries(), put_charge);

    let affected = AffectedIndexEpochTargets::new(Vec::new()).expect("empty targets");
    let current =
        AffectedEpochCurrentState::new(&affected, Vec::new()).expect("empty current state");
    let plan = CommandWriteSetPlanV1::new(
        &intent,
        affected,
        current,
        mutations,
        Vec::new(),
        reservation,
    )
    .expect("mixed index plan");
    let semantic = plan.charge().semantic_classes();
    assert!(semantic.pending_resolution() > 0);
    assert!(semantic.index_entries() > 0);
}

#[test]
fn complete_valid_records_can_exceed_only_the_final_aggregate_cap() {
    const CANONICAL_RECORD_OVERHEAD: usize = 16;

    let intent = sample::commit_intent();
    let (template, _) = sample::index_records();
    let covered_values = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::bytes(vec![
            0x5a;
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD
        ])
        .expect("maximum bytes value"),
    )])
    .expect("maximum canonical record");
    let entries = (0_u64..17)
        .map(|ordinal| {
            let mut key = IndexEntryKeyBuilder::new(IndexId::first());
            key.push_u64(ordinal).expect("unique index component");
            let key = key
                .finish(sample::entity_target().key().clone())
                .expect("complete index key");
            StoredIndexEntryV2::new(
                key,
                template.schema_binding().clone(),
                covered_values.clone(),
                template.partition_key().clone(),
            )
            .map(IndexEntryMutationV1::Put)
            .expect("each maximum row is individually valid")
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        command_write_set_upper_bound_v1(&intent, &entries, &[]),
        Ok(EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_))
    ));
}
