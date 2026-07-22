//! Durable semantic codec contract tests.

mod bounds;
mod malformed_semantic;
mod migration;
mod relationships;
mod sample;
mod variants;

use std::fmt::Debug;

use riffdb_types::{AdministrationSequence, CommitSequence, ExecutionFailureCode};

use crate::EncodedPageItem;

use super::*;

fn assert_round_trip<T>(
    value: T,
    encode: impl FnOnce(&T) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError>,
    decode: impl FnOnce(&[u8]) -> Result<EncodedPageItem<T>, DurableCodecError>,
) -> CanonicalStoredEnvelopeV1
where
    T: Debug + Eq,
{
    let encoded = encode(&value).expect("checked sample encodes");
    let decoded = decode(encoded.as_bytes()).expect("canonical sample decodes");
    assert_eq!(decoded.value(), &value);
    assert_eq!(
        decoded.encoded_content_charge(),
        encoded.encoded_content_charge()
    );
    encoded
}

fn semantic_wire_vectors() -> Vec<(&'static str, CanonicalStoredEnvelopeV1)> {
    let mut vectors = Vec::with_capacity(26);
    vectors.push((
        "riffdb.storage.v1.StoredStorageFormatVersionV1",
        assert_round_trip(
            crate::StorageFormatVersion::V1,
            |value| encode_storage_format_version_v1(*value),
            decode_storage_format_version_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredDatabaseIdentityV1",
        assert_round_trip(
            sample::database_id(),
            |value| encode_database_identity_v1(*value),
            decode_database_identity_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1",
        assert_round_trip(
            crate::ApplicationSequenceAllocator::next(CommitSequence::first()),
            |value| encode_application_sequence_allocator_v1(*value),
            decode_application_sequence_allocator_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1",
        assert_round_trip(
            crate::AdministrationSequenceAllocator::next(AdministrationSequence::first()),
            |value| encode_administration_sequence_allocator_v1(*value),
            decode_administration_sequence_allocator_v1,
        ),
    ));

    let (bundle, active, catalog_administration) = sample::catalog_records();
    vectors.push((
        "riffdb.storage.v1.StoredContractBundleV1",
        assert_round_trip(bundle, encode_contract_bundle_v1, decode_contract_bundle_v1),
    ));
    vectors.push((
        "riffdb.storage.v1.ActiveCatalogPointerV1",
        assert_round_trip(
            active,
            encode_active_catalog_pointer_v1,
            decode_active_catalog_pointer_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredCatalogAdministrationV1",
        assert_round_trip(
            catalog_administration,
            encode_catalog_administration_v1,
            decode_catalog_administration_v1,
        ),
    ));

    let atomic = sample::atomic_record_set();
    let (_, index_epoch) = sample::index_records();
    vectors.push((
        "riffdb.storage.v1.StoredEntityRecordV1",
        assert_round_trip(
            atomic.entities()[0].post_image().clone(),
            encode_entity_record_v1,
            decode_entity_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredIndexEntryV1",
        assert_round_trip(
            sample::legacy_index_record(),
            encode_index_entry_v1,
            decode_index_entry_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredIndexEpochV1",
        assert_round_trip(index_epoch, encode_index_epoch_v1, decode_index_epoch_v1),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredPendingAdmissionV1",
        assert_round_trip(
            atomic.expected_pending().clone(),
            encode_pending_admission_v1,
            decode_pending_admission_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredExecutionFailedV1",
        assert_round_trip(
            crate::StoredExecutionFailedV1::new(
                atomic.expected_pending().clone(),
                ExecutionFailureCode::ArithmeticFault,
            ),
            encode_execution_failed_v1,
            decode_execution_failed_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutcomeV1",
        assert_round_trip(
            atomic.stored_outcome().clone(),
            encode_stored_outcome_v1,
            decode_stored_outcome_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredDurableEventV1",
        assert_round_trip(
            atomic.events()[0].clone(),
            encode_durable_event_v1,
            decode_durable_event_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutboxIntentV1",
        assert_round_trip(
            atomic.outbox_intents()[0].clone(),
            encode_outbox_intent_v1,
            decode_outbox_intent_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProvenanceRecordV1",
        assert_round_trip(
            atomic.provenance().clone(),
            encode_provenance_record_v1,
            decode_provenance_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredCommitRecordV1",
        assert_round_trip(
            atomic.commit().clone(),
            encode_commit_record_v1,
            decode_commit_record_v1,
        ),
    ));

    let (capability, lookup, marker, capability_administration) = sample::capability_records();
    vectors.push((
        "riffdb.storage.v1.CapabilityRecordV1",
        assert_round_trip(
            capability,
            encode_capability_record_v1,
            decode_capability_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityTokenLookupV1",
        assert_round_trip(
            lookup,
            |value| encode_capability_token_lookup_v1(*value),
            decode_capability_token_lookup_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityBootstrapMarkerV1",
        assert_round_trip(
            marker,
            |value| encode_capability_bootstrap_marker_v1(*value),
            decode_capability_bootstrap_marker_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityAdministrationAuditV1",
        assert_round_trip(
            capability_administration,
            encode_capability_administration_v1,
            decode_capability_administration_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.ServiceAuditRecordV1",
        assert_round_trip(
            sample::service_audit_record(),
            encode_service_audit_record_v1,
            decode_service_audit_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutboxStatusV1",
        assert_round_trip(
            sample::outbox_status(),
            encode_outbox_status_v1,
            decode_outbox_status_v1,
        ),
    ));

    let (schema, state, apply, control) = sample::projection_records();
    vectors.push((
        "riffdb.storage.v1.StoredProjectionStateV1",
        assert_round_trip(state, encode_projection_state_v1, |bytes| {
            decode_projection_state_v1(bytes, &schema)
        }),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProjectionApplyV1",
        assert_round_trip(
            apply,
            encode_projection_apply_v1,
            decode_projection_apply_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProjectionControlV1",
        assert_round_trip(
            control,
            encode_projection_control_v1,
            decode_projection_control_v1,
        ),
    ));
    vectors
}

#[test]
fn every_registered_semantic_record_round_trips_in_registry_order() {
    let vectors = semantic_wire_vectors();
    assert_eq!(vectors.len(), 26);
    for ((name, _), schema) in vectors
        .iter()
        .zip(riffdb_proto::durable::READABLE_RECORD_SCHEMAS.iter())
    {
        assert_eq!(*name, schema.record_type());
    }

    let (current, _) = sample::index_records();
    assert_round_trip(current, encode_index_entry_v2, decode_index_entry_v2);
}

#[test]
fn emit_semantic_wire_vectors_for_fixture_regeneration() {
    let fixture = semantic_wire_fixture();
    print!("{fixture}");
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create vector parent directory");
        }
        std::fs::write(path, fixture).expect("write semantic wire fixture");
    }
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_INDEX_V2_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create V2 vector parent directory");
        }
        std::fs::write(path, index_v2_wire_fixture()).expect("write V2 semantic wire fixture");
    }
}

#[test]
fn checked_in_semantic_wire_vectors_are_current() {
    assert_eq!(
        semantic_wire_fixture(),
        include_str!("../../../../../fixtures/proto/durable-wire-vectors.txt")
    );
    assert_eq!(
        index_v2_wire_fixture(),
        include_str!("../../../../../fixtures/proto/durable-index-v2-wire-vector.txt")
    );
}

fn semantic_wire_fixture() -> String {
    let mut fixture = String::from("riffdb-durable-wire-vectors-v1\nrecords\t26\n");
    for (name, envelope) in semantic_wire_vectors() {
        let decoded = riffdb_proto::durable::readable_record_registry()
            .decode(envelope.as_bytes())
            .expect("sample envelope decodes");
        let line = format!(
            "{name}\t{}\t{}\n",
            hex(decoded.payload()),
            hex(envelope.as_bytes())
        );
        fixture.push_str(&line);
    }
    fixture
}

fn index_v2_wire_fixture() -> String {
    let (value, _) = sample::index_records();
    let envelope = assert_round_trip(value, encode_index_entry_v2, decode_index_entry_v2);
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(envelope.as_bytes())
        .expect("V2 sample envelope decodes");
    format!(
        "riffdb-durable-index-v2-wire-vector-v1\n{}\t{}\t{}\n",
        decoded.record_type(),
        hex(decoded.payload()),
        hex(envelope.as_bytes())
    )
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("String writes are infallible");
    }
    output
}
