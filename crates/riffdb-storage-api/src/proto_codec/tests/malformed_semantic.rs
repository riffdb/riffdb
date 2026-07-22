use prost::Message;
use riffdb_proto::{
    durable::{readable_record_registry, readable_record_schema},
    envelope::{self, STORAGE_FORMAT_VERSION_V1},
    storage::v1 as wire,
};
use riffdb_types::ExecutionFailureCode;

use super::super::*;
use super::sample;

fn raw_envelope(record_type: &'static str, payload: Vec<u8>) -> Vec<u8> {
    let schema = readable_record_schema(record_type).expect("registered durable record");
    wire::StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: record_type.to_owned(),
        payload_crc32c: envelope::payload_crc32c(&payload),
        payload,
        schema_hash: schema.schema_hash().as_bytes().to_vec(),
    }
    .encode_to_vec()
}

fn checked_envelope<M: Message>(record_type: &'static str, message: &M) -> Vec<u8> {
    let schema = readable_record_schema(record_type).expect("registered durable record");
    envelope::encode(schema, &message.encode_to_vec()).expect("fixture is structurally canonical")
}

fn payload_message<M: Message + Default>(encoded: &[u8]) -> M {
    let decoded = readable_record_registry()
        .decode(encoded)
        .expect("checked sample envelope");
    M::decode(decoded.payload()).expect("checked sample payload")
}

fn assert_corrupt<T>(result: Result<crate::EncodedPageItem<T>, DurableCodecError>) {
    assert_error_kind(result, DurableCodecErrorKind::CorruptData);
}

fn assert_error_kind<T>(
    result: Result<crate::EncodedPageItem<T>, DurableCodecError>,
    expected: DurableCodecErrorKind,
) {
    match result {
        Err(error) => assert_eq!(error.kind(), expected),
        Ok(_) => panic!("semantically malformed durable record decoded"),
    }
}

#[test]
fn required_fields_uuidv7_and_timestamp_fail_closed() {
    const DATABASE: &str = "riffdb.storage.v1.StoredDatabaseIdentityV1";
    assert_corrupt(decode_database_identity_v1(&raw_envelope(
        DATABASE,
        Vec::new(),
    )));

    let invalid_uuid = wire::StoredDatabaseIdentityV1 {
        database_id: [0x11; 16].to_vec(),
    };
    assert_corrupt(decode_database_identity_v1(&checked_envelope(
        DATABASE,
        &invalid_uuid,
    )));

    const PENDING: &str = "riffdb.storage.v1.StoredPendingAdmissionV1";
    let canonical = encode_pending_admission_v1(&sample::pending()).expect("pending encodes");
    let mut pending = payload_message::<wire::StoredPendingAdmissionV1>(canonical.as_bytes());
    pending
        .logical_time
        .as_mut()
        .expect("sample logical time")
        .nanos = 1_000_000_000;
    assert_corrupt(decode_pending_admission_v1(&checked_envelope(
        PENDING, &pending,
    )));
}

#[test]
fn unknown_enum_and_multiple_oneof_members_fail_closed() {
    const FAILED: &str = "riffdb.storage.v1.StoredExecutionFailedV1";
    let failed = crate::StoredExecutionFailedV1::new(
        sample::pending(),
        ExecutionFailureCode::ArithmeticFault,
    );
    let canonical = encode_execution_failed_v1(&failed).expect("execution failure encodes");
    let mut message = payload_message::<wire::StoredExecutionFailedV1>(canonical.as_bytes());
    message.code = 99;
    assert_corrupt(decode_execution_failed_v1(&checked_envelope(
        FAILED, &message,
    )));

    const ALLOCATOR: &str = "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1";
    let both_oneof_members = vec![0x08, 0x01, 0x12, 0x00];
    assert_corrupt(decode_application_sequence_allocator_v1(&raw_envelope(
        ALLOCATOR,
        both_oneof_members,
    )));
}

#[test]
fn capability_parameter_presence_and_canonical_order_fail_closed() {
    const CAPABILITY: &str = "riffdb.storage.v1.CapabilityRecordV1";
    let (value, _, _, _) = sample::capability_records();
    let canonical = encode_capability_record_v1(&value).expect("capability encodes");
    let message = payload_message::<wire::CapabilityRecordV1>(canonical.as_bytes());

    let mut missing_wrapper = message.clone();
    missing_wrapper
        .grant
        .as_mut()
        .expect("sample grant")
        .permissions = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY,
        &missing_wrapper,
    )));

    let mut incomplete_parameter = message.clone();
    let permission = incomplete_parameter
        .grant
        .as_mut()
        .and_then(|grant| grant.permissions.as_mut())
        .and_then(|permissions| {
            permissions
                .values
                .iter_mut()
                .find(|permission| permission.contract_lineage.is_some())
        })
        .expect("sample parameterized permission");
    permission.contract_lineage = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY,
        &incomplete_parameter,
    )));

    let mut reversed = message;
    reversed
        .grant
        .as_mut()
        .and_then(|grant| grant.permissions.as_mut())
        .expect("sample permission wrapper")
        .values
        .reverse();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY, &reversed,
    )));
}

#[test]
fn canonical_keys_records_and_event_hashes_are_revalidated() {
    const INDEX_ENTRY: &str = "riffdb.storage.v1.StoredIndexEntryV1";
    let entry = sample::legacy_index_record();
    let canonical = encode_index_entry_v1(&entry).expect("index entry encodes");
    let mut invalid_key = payload_message::<wire::StoredIndexEntryV1>(canonical.as_bytes());
    invalid_key.index_entry_key = vec![0xff];
    assert_corrupt(decode_index_entry_v1(&checked_envelope(
        INDEX_ENTRY,
        &invalid_key,
    )));

    const EVENT: &str = "riffdb.storage.v1.StoredDurableEventV1";
    let event = sample::atomic_record_set().events()[0].clone();
    let canonical = encode_durable_event_v1(&event).expect("event encodes");
    let message = payload_message::<wire::StoredDurableEventV1>(canonical.as_bytes());

    let mut invalid_record = message.clone();
    invalid_record.canonical_payload = vec![0x00];
    assert_corrupt(decode_durable_event_v1(&checked_envelope(
        EVENT,
        &invalid_record,
    )));

    let mut invalid_hash = message;
    invalid_hash.event_hash[0] ^= 1;
    assert_corrupt(decode_durable_event_v1(&checked_envelope(
        EVENT,
        &invalid_hash,
    )));
}

#[test]
fn audit_target_shape_order_and_cardinality_fail_closed() {
    use wire::service_audit_target_v1::Target;

    const AUDIT: &str = "riffdb.storage.v1.ServiceAuditRecordV1";
    let canonical =
        encode_service_audit_record_v1(&sample::service_audit_record()).expect("audit encodes");
    let message = payload_message::<wire::ServiceAuditRecordV1>(canonical.as_bytes());
    assert_eq!(message.targets.len(), 9, "sample carries every target kind");

    let mut reversed = message.clone();
    reversed.targets.reverse();
    assert_corrupt(decode_service_audit_record_v1(&checked_envelope(
        AUDIT, &reversed,
    )));

    let mut duplicate = message.clone();
    duplicate.targets.push(duplicate.targets[0].clone());
    assert_corrupt(decode_service_audit_record_v1(&checked_envelope(
        AUDIT, &duplicate,
    )));

    let mut missing_kind = message.clone();
    missing_kind.targets[0].target = None;
    assert_corrupt(decode_service_audit_record_v1(&checked_envelope(
        AUDIT,
        &missing_kind,
    )));

    let mut zero_commit = message.clone();
    let commit = zero_commit
        .targets
        .iter_mut()
        .find_map(|target| match target.target.as_mut() {
            Some(Target::CommitSequence(sequence)) => Some(sequence),
            _ => None,
        })
        .expect("sample commit target");
    *commit = 0;
    assert_corrupt(decode_service_audit_record_v1(&checked_envelope(
        AUDIT,
        &zero_commit,
    )));

    let mut wrong_lineage = message.clone();
    let lineage = wrong_lineage
        .targets
        .iter_mut()
        .find_map(|target| match target.target.as_mut() {
            Some(Target::ContractVersion(value)) => Some(&mut value.contract_lineage),
            _ => None,
        })
        .expect("sample lineage-scoped target");
    lineage.clear();
    assert_corrupt(decode_service_audit_record_v1(&checked_envelope(
        AUDIT,
        &wrong_lineage,
    )));

    let mut over_limit = message;
    over_limit.targets = vec![over_limit.targets[0].clone(); 17];
    assert_error_kind(
        decode_service_audit_record_v1(&raw_envelope(AUDIT, over_limit.encode_to_vec())),
        DurableCodecErrorKind::LimitExceeded,
    );
}

#[test]
fn outcome_provenance_and_commit_canonical_lists_fail_closed() {
    let records = sample::atomic_record_set();

    const OUTCOME: &str = "riffdb.storage.v1.StoredOutcomeV1";
    let outcome = encode_stored_outcome_v1(records.stored_outcome()).expect("outcome encodes");
    let outcome = payload_message::<wire::StoredOutcomeV1>(outcome.as_bytes());

    let mut missing_partition_key = outcome.clone();
    missing_partition_key.partition_key.clear();
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &missing_partition_key,
    )));

    let mut mismatched_partition_hash = outcome.clone();
    *mismatched_partition_hash
        .partition_key
        .last_mut()
        .expect("sample partition key") ^= 1;
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &mismatched_partition_hash,
    )));

    let mut noncanonical_conflicts = outcome;
    noncanonical_conflicts.conflict_hashes = vec![[0x82; 32].to_vec(), [0x81; 32].to_vec()];
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &noncanonical_conflicts,
    )));

    const PROVENANCE: &str = "riffdb.storage.v1.StoredProvenanceRecordV1";
    let provenance = encode_provenance_record_v1(records.provenance()).expect("provenance encodes");
    let mut provenance = payload_message::<wire::StoredProvenanceRecordV1>(provenance.as_bytes());
    provenance
        .affected_entities
        .push(provenance.affected_entities[0].clone());
    assert_corrupt(decode_provenance_record_v1(&checked_envelope(
        PROVENANCE,
        &provenance,
    )));

    const COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV1";
    let commit = encode_commit_record_v1(records.commit()).expect("commit encodes");
    let commit = payload_message::<wire::StoredCommitRecordV1>(commit.as_bytes());

    let mut duplicate_dependency = commit.clone();
    let dependency = duplicate_dependency
        .read_dependencies
        .as_ref()
        .expect("sample dependencies")
        .dependencies[0]
        .clone();
    duplicate_dependency
        .read_dependencies
        .as_mut()
        .expect("sample dependencies")
        .dependencies
        .push(dependency);
    assert_corrupt(decode_commit_record_v1(&checked_envelope(
        COMMIT,
        &duplicate_dependency,
    )));

    let mut duplicate_outbox_id = commit;
    duplicate_outbox_id
        .outbox_event_ids
        .push(duplicate_outbox_id.outbox_event_ids[0]);
    assert_corrupt(decode_commit_record_v1(&checked_envelope(
        COMMIT,
        &duplicate_outbox_id,
    )));
}

#[test]
fn absent_outbox_state_and_projection_schema_mismatch_fail_closed() {
    const OUTBOX: &str = "riffdb.storage.v1.StoredOutboxStatusV1";
    let status = encode_outbox_status_v1(&sample::outbox_status()).expect("status encodes");
    let mut status = payload_message::<wire::StoredOutboxStatusV1>(status.as_bytes());
    status.state = None;
    assert_corrupt(decode_outbox_status_v1(&checked_envelope(OUTBOX, &status)));

    const PROJECTION: &str = "riffdb.storage.v1.StoredProjectionStateV1";
    let (schema, state, _, _) = sample::projection_records();
    let state = encode_projection_state_v1(&state).expect("projection state encodes");
    let mut state = payload_message::<wire::StoredProjectionStateV1>(state.as_bytes());
    state
        .identity
        .as_mut()
        .expect("sample projection identity")
        .projection_plan_hash[0] ^= 1;
    assert_corrupt(decode_projection_state_v1(
        &checked_envelope(PROJECTION, &state),
        &schema,
    ));
}
