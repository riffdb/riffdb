use prost::Message;
use riffdb_proto::{
    durable::{readable_record_registry, readable_record_schema},
    envelope::{self, STORAGE_FORMAT_VERSION_V1},
    storage::v1 as wire,
};
use riffdb_types::{CommitSequence, EventId, EventTypeId, ExecutionFailureCode, RowPolicyName};

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

fn canonical_anchored_event() -> CanonicalStoredEnvelopeV1 {
    let event_id = EventId::new(CommitSequence::first(), 0);
    let event_type = EventTypeId::first();
    let payload = sample::canonical_record(0x43);
    let anchor = crate::StoredEventPolicyAnchorV1::new(
        crate::DurableKeySchemaBindingV1::from_plan(&sample::plan()),
        event_type,
        sample::entity_target(),
        RowPolicyName::new("TicketAccess").expect("policy name"),
    );
    let event = crate::StoredDurableEventV2::new(
        event_id,
        event_type,
        payload.clone(),
        crate::derive_event_hash_v2(event_id, event_type, &payload, &anchor).expect("event hash"),
        anchor,
    )
    .expect("anchored event");
    encode_durable_event_v2(&event).expect("anchored event encodes")
}

#[test]
fn anchored_event_missing_or_inconsistent_authority_fails_closed() {
    const EVENT_V2: &str = "riffdb.storage.v1.StoredDurableEventV2";
    let canonical = canonical_anchored_event();
    let message = payload_message::<wire::StoredDurableEventV2>(canonical.as_bytes());

    let mut missing_anchor = message.clone();
    missing_anchor.policy_anchor = None;
    assert_corrupt(decode_durable_event_v2(&checked_envelope(
        EVENT_V2,
        &missing_anchor,
    )));

    let mut mismatched_event_type = message.clone();
    mismatched_event_type
        .policy_anchor
        .as_mut()
        .expect("sample policy anchor")
        .event_type_id = 2;
    assert_corrupt(decode_durable_event_v2(&checked_envelope(
        EVENT_V2,
        &mismatched_event_type,
    )));

    let mut invalid_policy = message.clone();
    invalid_policy
        .policy_anchor
        .as_mut()
        .expect("sample policy anchor")
        .read_policy
        .clear();
    assert_corrupt(decode_durable_event_v2(&checked_envelope(
        EVENT_V2,
        &invalid_policy,
    )));

    let mut mismatched_hash = message;
    mismatched_hash.event_hash[0] ^= 0x01;
    assert_corrupt(decode_durable_event_v2(&checked_envelope(
        EVENT_V2,
        &mismatched_hash,
    )));
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

    const PENDING: &str = "riffdb.storage.v1.StoredPendingAdmissionV3";
    let canonical = encode_pending_admission_v1(&sample::pending()).expect("pending encodes");
    let mut pending = payload_message::<wire::StoredPendingAdmissionV3>(canonical.as_bytes());
    pending
        .base
        .as_mut()
        .expect("sample pending base")
        .base
        .as_mut()
        .expect("sample causal pending base")
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
    const FAILED: &str = "riffdb.storage.v1.StoredExecutionFailedV3";
    let failed = crate::StoredExecutionFailedV1::new(
        sample::pending(),
        ExecutionFailureCode::ArithmeticFault,
    );
    let canonical = encode_execution_failed_v1(&failed).expect("execution failure encodes");
    let mut message = payload_message::<wire::StoredExecutionFailedV3>(canonical.as_bytes());
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
fn capability_v2_migration_extension_is_required_and_canonical() {
    const CAPABILITY_V2: &str = "riffdb.storage.v1.CapabilityRecordV2";
    let value = sample::capability_record_with_migration_authority();
    let canonical = encode_capability_record_v1(&value).expect("migration capability encodes");
    let message = payload_message::<wire::CapabilityRecordV2>(canonical.as_bytes());

    let mut missing = message.clone();
    missing.migration = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V2,
        &missing,
    )));

    let mut empty = message.clone();
    empty
        .migration
        .as_mut()
        .expect("migration extension")
        .contract_lineages
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V2,
        &empty,
    )));

    let mut duplicate = message.clone();
    duplicate
        .migration
        .as_mut()
        .expect("migration extension")
        .contract_lineages[1] = "accounts".to_owned();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V2,
        &duplicate,
    )));

    let mut unordered = message;
    unordered
        .migration
        .as_mut()
        .expect("migration extension")
        .contract_lineages
        .reverse();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V2,
        &unordered,
    )));
}

#[test]
fn capability_v3_installation_extension_is_required_and_canonical() {
    const CAPABILITY_V3: &str = "riffdb.storage.v1.CapabilityRecordV3";
    let value = sample::capability_record_with_installation_authority();
    let canonical = encode_capability_record_v1(&value).expect("installation capability encodes");
    let message = payload_message::<wire::CapabilityRecordV3>(canonical.as_bytes());

    let mut missing = message.clone();
    missing.installation = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V3,
        &missing,
    )));

    let mut empty = message.clone();
    empty
        .installation
        .as_mut()
        .expect("installation extension")
        .contract_lineages
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V3,
        &empty,
    )));

    let mut invalid = message;
    invalid
        .installation
        .as_mut()
        .expect("installation extension")
        .contract_lineages[0] = String::new();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V3,
        &invalid,
    )));
}

#[test]
fn capability_v4_row_policy_extension_is_required_canonical_and_role_bound() {
    const CAPABILITY_V4: &str = "riffdb.storage.v1.CapabilityRecordV4";
    let value = sample::capability_record_with_row_policy_authority();
    let canonical = encode_capability_record_v1(&value).expect("row-policy capability encodes");
    let message = payload_message::<wire::CapabilityRecordV4>(canonical.as_bytes());

    let mut missing = message.clone();
    missing.row_policy = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &missing,
    )));

    let mut empty_bindings = message.clone();
    empty_bindings
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .policies
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &empty_bindings,
    )));

    let mut empty_facts = message.clone();
    empty_facts
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .canonical_principal_facts
        .clear();
    assert_corrupt(decode_capability_record_v1(&raw_envelope(
        CAPABILITY_V4,
        empty_facts.encode_to_vec(),
    )));

    let mut wrong_role = message.clone();
    wrong_role
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .application_role_hash = vec![0x77; 32];
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &wrong_role,
    )));

    let mut duplicate = message.clone();
    let binding = duplicate
        .row_policy
        .as_ref()
        .expect("row-policy extension")
        .policies[0]
        .clone();
    duplicate
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .policies
        .push(binding);
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &duplicate,
    )));

    let mut unordered_operations = message.clone();
    unordered_operations
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .policies[0]
        .operations
        .reverse();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &unordered_operations,
    )));

    let mut unknown_operation = message;
    unknown_operation
        .row_policy
        .as_mut()
        .expect("row-policy extension")
        .policies[0]
        .operations[0] = 99;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V4,
        &unknown_operation,
    )));
}

#[test]
fn capability_v5_export_extension_is_required_canonical_and_scope_checked() {
    const CAPABILITY_V5: &str = "riffdb.storage.v1.CapabilityRecordV5";
    let value = sample::capability_record_with_export_authority();
    let canonical = encode_capability_record_v1(&value).expect("export capability encodes");
    let message = payload_message::<wire::CapabilityRecordV5>(canonical.as_bytes());

    let mut missing = message.clone();
    missing.export = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V5,
        &missing,
    )));

    let mut empty = message.clone();
    empty
        .export
        .as_mut()
        .expect("export extension")
        .applications
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V5,
        &empty,
    )));

    let mut false_only = message.clone();
    let application = &mut false_only
        .export
        .as_mut()
        .expect("export extension")
        .applications[0];
    application.entities = false;
    application.events = false;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V5,
        &false_only,
    )));

    let mut unknown_scope = message.clone();
    unknown_scope
        .export
        .as_mut()
        .expect("export extension")
        .applications[0]
        .scope = 99;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V5,
        &unknown_scope,
    )));

    let mut missing_policy = message;
    missing_policy.row_policy = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V5,
        &missing_policy,
    )));
}

#[test]
fn interim_installation_payload_under_v2_compact_identity_is_recovered_exactly() {
    const CAPABILITY_V2: &str = "riffdb.storage.v1.CapabilityRecordV2";
    let expected = sample::capability_record_with_installation_authority();
    let canonical = encode_capability_record_v1(&expected).expect("V3 capability encodes");
    let message = payload_message::<wire::CapabilityRecordV3>(canonical.as_bytes());

    // Commit 49b66da briefly emitted this V3 payload under V2's already-frozen
    // compact tag/revision. Preserve authority while decoding affected
    // pre-alpha databases; every new write uses the distinct V3 identity.
    let interim = checked_envelope(CAPABILITY_V2, &message);
    let decoded = decode_capability_record_v1(&interim).expect("interim record is recoverable");
    assert_eq!(decoded.value(), &expected);
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
    use wire::service_audit_target_v2::Target;

    const AUDIT: &str = "riffdb.storage.v1.ServiceAuditRecordV2";
    let canonical =
        encode_service_audit_record_v2(&sample::service_audit_record()).expect("audit encodes");
    let message = payload_message::<wire::ServiceAuditRecordV2>(canonical.as_bytes());
    assert_eq!(message.targets.len(), 9, "sample carries every target kind");

    let mut reversed = message.clone();
    reversed.targets.reverse();
    assert_corrupt(decode_service_audit_record_v2(&checked_envelope(
        AUDIT, &reversed,
    )));

    let mut duplicate = message.clone();
    duplicate.targets.push(duplicate.targets[0].clone());
    assert_corrupt(decode_service_audit_record_v2(&checked_envelope(
        AUDIT, &duplicate,
    )));

    let mut missing_kind = message.clone();
    missing_kind.targets[0].target = None;
    assert_corrupt(decode_service_audit_record_v2(&checked_envelope(
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
    assert_corrupt(decode_service_audit_record_v2(&checked_envelope(
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
    assert_corrupt(decode_service_audit_record_v2(&checked_envelope(
        AUDIT,
        &wrong_lineage,
    )));

    let mut over_limit = message;
    over_limit.targets = vec![over_limit.targets[0].clone(); 17];
    assert_error_kind(
        decode_service_audit_record_v2(&raw_envelope(AUDIT, over_limit.encode_to_vec())),
        DurableCodecErrorKind::LimitExceeded,
    );
}

#[test]
fn outcome_provenance_and_commit_canonical_lists_fail_closed() {
    let records = sample::atomic_record_set();

    const OUTCOME: &str = "riffdb.storage.v1.StoredOutcomeV3";
    let outcome = encode_stored_outcome_v1(records.stored_outcome()).expect("outcome encodes");
    let outcome = payload_message::<wire::StoredOutcomeV3>(outcome.as_bytes());

    let mut missing_partition_key = outcome.clone();
    missing_partition_key
        .base
        .as_mut()
        .expect("sample outcome base")
        .base
        .as_mut()
        .expect("sample causal outcome base")
        .partition_key
        .clear();
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &missing_partition_key,
    )));

    let mut mismatched_partition_hash = outcome.clone();
    *mismatched_partition_hash
        .base
        .as_mut()
        .expect("sample outcome base")
        .base
        .as_mut()
        .expect("sample causal outcome base")
        .partition_key
        .last_mut()
        .expect("sample partition key") ^= 1;
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &mismatched_partition_hash,
    )));

    let mut noncanonical_conflicts = outcome;
    noncanonical_conflicts
        .base
        .as_mut()
        .expect("sample outcome base")
        .base
        .as_mut()
        .expect("sample causal outcome base")
        .conflict_hashes = vec![[0x82; 32].to_vec(), [0x81; 32].to_vec()];
    assert_corrupt(decode_stored_outcome_v1(&checked_envelope(
        OUTCOME,
        &noncanonical_conflicts,
    )));

    const PROVENANCE: &str = "riffdb.storage.v1.StoredProvenanceRecordV2";
    let provenance = encode_provenance_record_v1(records.provenance()).expect("provenance encodes");
    let mut provenance = payload_message::<wire::StoredProvenanceRecordV2>(provenance.as_bytes());
    let duplicate_affected_entity = provenance
        .base
        .as_ref()
        .expect("sample provenance base")
        .affected_entities[0]
        .clone();
    provenance
        .base
        .as_mut()
        .expect("sample provenance base")
        .affected_entities
        .push(duplicate_affected_entity);
    assert_corrupt(decode_provenance_record_v1(&checked_envelope(
        PROVENANCE,
        &provenance,
    )));

    const COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV1";
    let commit = encode_commit_record_legacy_v1(records.commit(), records.entities())
        .expect("legacy commit encodes");
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

/// ADR-0118: every advertised fail-closed rejection of the V6 secret
/// extension is exercised against real wire bytes — a missing extension,
/// an empty extension, an empty per-entry naming, an entry matching no
/// visibility row, a duplicate entry for one row, and a non-canonical
/// (unsorted/duplicated) id list all refuse to decode.
#[test]
fn capability_v6_secret_extension_is_required_canonical_and_matched() {
    const CAPABILITY_V6: &str = "riffdb.storage.v1.CapabilityRecordV6";
    let value = sample::capability_record_with_secret_naming();
    let canonical = encode_capability_record_v1(&value).expect("secret capability encodes");
    let message = payload_message::<wire::CapabilityRecordV6>(canonical.as_bytes());

    // 1. V6 without its distinguishing extension is not a valid V6.
    let mut missing = message.clone();
    missing.secret = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &missing,
    )));

    // 2. An extension with zero entries names nothing and must refuse.
    let mut empty = message.clone();
    empty
        .secret
        .as_mut()
        .expect("secret extension")
        .entries
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &empty,
    )));

    // 3. An entry with an empty id list reveals nothing and must refuse.
    let mut empty_ids = message.clone();
    empty_ids.secret.as_mut().expect("secret extension").entries[0]
        .secret_field_ids
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &empty_ids,
    )));

    // 4. An entry that matches no visibility row cannot attach.
    let mut unmatched = message.clone();
    unmatched.secret.as_mut().expect("secret extension").entries[0].entity_type_id = 999;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &unmatched,
    )));

    // 5. Two entries for the same visibility row are a duplicate naming.
    let mut duplicated = message.clone();
    {
        let entries = &mut duplicated
            .secret
            .as_mut()
            .expect("secret extension")
            .entries;
        let copy = entries[0].clone();
        entries.push(copy);
    }
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &duplicated,
    )));

    // 6. A non-canonical id list (duplicate ids) fails the exact
    //    sorted-identity re-check.
    let mut noncanonical = message.clone();
    {
        let ids = &mut noncanonical
            .secret
            .as_mut()
            .expect("secret extension")
            .entries[0]
            .secret_field_ids;
        let first = ids[0];
        ids.push(first);
    }
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V6,
        &noncanonical,
    )));

    // Control (non-empty triggering set): the untouched message decodes.
    let decoded = decode_capability_record_v1(&checked_envelope(CAPABILITY_V6, &message))
        .expect("the canonical V6 record decodes");
    assert_eq!(decoded.value(), &value);
}

/// ADR-0136: the V8 vector-inspection successor is useful only when its
/// compiler-owned role, permission, visible field, and canonical target set
/// agree. Durable decode must reject wire records that try to separate any of
/// those facts.
#[test]
fn capability_v8_vector_inspection_is_required_canonical_and_role_bound() {
    const CAPABILITY_V8: &str = "riffdb.storage.v1.CapabilityRecordV8";
    let value = sample::capability_record_with_vector_inspection();
    let canonical = encode_capability_record_v1(&value).expect("inspection capability encodes");
    let message = payload_message::<wire::CapabilityRecordV8>(canonical.as_bytes());

    let mut missing = message.clone();
    missing.vector_inspection = None;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V8,
        &missing,
    )));

    let mut empty = message.clone();
    empty
        .vector_inspection
        .as_mut()
        .expect("inspection extension")
        .targets
        .clear();
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V8,
        &empty,
    )));

    let mut duplicate = message.clone();
    {
        let targets = &mut duplicate
            .vector_inspection
            .as_mut()
            .expect("inspection extension")
            .targets;
        targets.push(targets[0].clone());
    }
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V8,
        &duplicate,
    )));

    let mut wrong_role = message.clone();
    wrong_role
        .vector_inspection
        .as_mut()
        .expect("inspection extension")
        .application_role_hash[0] ^= 1;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V8,
        &wrong_role,
    )));

    let mut invisible = message.clone();
    invisible
        .vector_inspection
        .as_mut()
        .expect("inspection extension")
        .targets[0]
        .field_id = 999;
    assert_corrupt(decode_capability_record_v1(&checked_envelope(
        CAPABILITY_V8,
        &invisible,
    )));

    let decoded = decode_capability_record_v1(&checked_envelope(CAPABILITY_V8, &message))
        .expect("canonical V8 decodes");
    assert_eq!(decoded.value(), &value);
}

/// The stored canonical bytes are re-proved on every entity and index-entry
/// decode, and the row keeps exactly the bytes that proof accepted.
///
/// Rows retain the buffer produced by the canonicality re-encode rather than
/// encoding the same record a second time. Both halves are asserted here: the
/// retained encoding equals the stored bytes, and every stored byte string that
/// is not an exact canonical record document still fails closed.
#[test]
fn canonical_record_bytes_are_reproved_and_retained_exactly() {
    use crate::{DurableKeySchemaBindingV1, StoredEntityRecordV1};
    use riffdb_types::{CanonicalValue, encode_canonical_value};

    const ENTITY: &str = "riffdb.storage.v1.StoredEntityRecordV1";
    const INDEX_ENTRY: &str = "riffdb.storage.v1.StoredIndexEntryV2";

    let plan = sample::plan();
    let entity = StoredEntityRecordV1::new(
        sample::entity_target(),
        riffdb_types::EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        sample::canonical_record(0x42),
    )
    .expect("entity record");
    let canonical = encode_entity_record_v1(&entity).expect("entity encodes");
    let message = payload_message::<wire::StoredEntityRecordV1>(canonical.as_bytes());

    let decoded = decode_entity_record_v1(&checked_envelope(ENTITY, &message))
        .expect("canonical entity decodes")
        .into_parts()
        .0;
    assert_eq!(
        decoded.fields_encoded(),
        message.canonical_fields.as_slice(),
        "the retained encoding is the exact stored canonical document"
    );
    assert_eq!(decoded.fields(), entity.fields());

    let mut trailing = message.clone();
    trailing.canonical_fields.push(0x00);
    assert_corrupt(decode_entity_record_v1(&checked_envelope(
        ENTITY, &trailing,
    )));

    let mut truncated = message.clone();
    truncated.canonical_fields.pop();
    assert_corrupt(decode_entity_record_v1(&checked_envelope(
        ENTITY, &truncated,
    )));

    let mut not_a_record = message.clone();
    not_a_record.canonical_fields =
        encode_canonical_value(&CanonicalValue::U64(7)).expect("scalar document encodes");
    assert_corrupt(decode_entity_record_v1(&checked_envelope(
        ENTITY,
        &not_a_record,
    )));

    let mut empty = message;
    empty.canonical_fields.clear();
    assert_corrupt(decode_entity_record_v1(&checked_envelope(ENTITY, &empty)));

    let (entry, _) = sample::index_records();
    let canonical = encode_index_entry_v2(&entry).expect("index entry encodes");
    let message = payload_message::<wire::StoredIndexEntryV2>(canonical.as_bytes());

    let decoded = decode_index_entry_v2(&checked_envelope(INDEX_ENTRY, &message))
        .expect("canonical index entry decodes")
        .into_parts()
        .0;
    assert_eq!(
        decoded.covered_values_encoded(),
        message.canonical_covered_values.as_slice(),
        "the retained encoding is the exact stored canonical document"
    );
    assert_eq!(decoded.covered_values(), entry.covered_values());

    let mut trailing = message.clone();
    trailing.canonical_covered_values.push(0x00);
    assert_corrupt(decode_index_entry_v2(&checked_envelope(
        INDEX_ENTRY,
        &trailing,
    )));

    let mut not_a_record = message;
    not_a_record.canonical_covered_values =
        encode_canonical_value(&CanonicalValue::Null).expect("null document encodes");
    assert_corrupt(decode_index_entry_v2(&checked_envelope(
        INDEX_ENTRY,
        &not_a_record,
    )));
}

#[test]
// req: REP-006, STO-012
fn frozen_audit_v2_refuses_a_follower_target_even_with_a_valid_envelope_checksum() {
    const AUDIT: &str = "riffdb.storage.v1.ServiceAuditRecordV2";
    let canonical =
        encode_service_audit_record_v2(&sample::service_audit_record()).expect("audit encodes");
    assert!(decode_service_audit_record_v2(canonical.as_bytes()).is_ok());
    let message = payload_message::<wire::ServiceAuditRecordV2>(canonical.as_bytes());
    let last = message
        .targets
        .last()
        .expect("sample targets")
        .encode_to_vec();
    let mut last_field = vec![0x42, u8::try_from(last.len()).expect("short sample target")];
    last_field.extend(last);
    let mut payload = message.encode_to_vec();
    let offsets = payload
        .windows(last_field.len())
        .enumerate()
        .filter_map(|(offset, value)| (value == last_field).then_some(offset))
        .collect::<Vec<_>>();
    assert_eq!(offsets.len(), 1, "exact final target occurs once");
    let insertion = offsets[0] + last_field.len();

    // A bounded, canonical candidate follower payload: source database, history
    // incarnation, epoch and opaque hold ID. Field 11 is absent from frozen V2.
    let mut follower = vec![0x0a, 16];
    follower.extend_from_slice(sample::database_id().as_bytes());
    follower.extend_from_slice(&[0x10, 1, 0x18, 1, 0x22, 16]);
    follower.extend_from_slice(&[0x81; 16]);
    let mut target = vec![
        0x5a,
        u8::try_from(follower.len()).expect("bounded follower"),
    ];
    target.extend(follower);
    let mut field = vec![0x42, u8::try_from(target.len()).expect("bounded target")];
    field.extend(target);
    payload.splice(insertion..insertion, field);
    // The outer checksum is recomputed. Target order, all original fields and
    // the sixteen-target bound remain intact; this new kind still must refuse.
    assert_corrupt(decode_service_audit_record_v2(&raw_envelope(
        AUDIT, payload,
    )));
}
