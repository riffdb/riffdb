#![forbid(unsafe_code)]

//! Cross-record compatibility fixture validation for future durable engines.

use std::collections::BTreeMap;

use prost::Message;
use riffdb_proto::{durable::readable_record_registry, storage::v1 as wire};
use riffdb_types::hash_event;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-relationship-vectors.txt"
));

#[test]
fn relationship_fixture_is_structural_canonical_and_self_consistent() {
    let lines = FIXTURE.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 36);
    assert_eq!(lines[0], "riffdb-durable-relationship-vectors-v1");
    assert_eq!(lines[1], "bootstrap-cases\t5");
    validate_bootstrap_cases(&lines[2..7]);
    assert_eq!(lines[7], "compound-bootstrap-records\t5");
    validate_compound_bootstrap(&lines[8..14]);
    assert_eq!(lines[14], "reciprocity-cases\t4");
    assert_eq!(lines[15], "reciprocity-records\t12");
    validate_reciprocity_cases(&lines[16..28]);
    assert_eq!(lines[28], "event-copies\t3");
    let event_id = validate_event_relationship(&lines[29..34]);
    assert_eq!(lines[34], "outbox-initial-status-cases\t1");
    validate_absent_initial_outbox_status(lines[35], &event_id);
}

fn validate_bootstrap_cases(lines: &[&str]) {
    let expected = [
        ("first-two", "ok", "next:1", "2", "1,2", "next:3"),
        (
            "last-two",
            "ok",
            "next:18446744073709551614",
            "2",
            "18446744073709551614,18446744073709551615",
            "exhausted",
        ),
        (
            "maximum-one",
            "ok",
            "next:18446744073709551615",
            "1",
            "18446744073709551615",
            "exhausted",
        ),
        (
            "maximum-two",
            "error-exhausted",
            "next:18446744073709551615",
            "2",
            "-",
            "no-write",
        ),
        (
            "exhausted-two",
            "error-exhausted",
            "exhausted",
            "2",
            "-",
            "no-write",
        ),
    ];

    for (line, expected) in lines.iter().zip(expected) {
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 11);
        assert_eq!(fields[0], "bootstrap");
        assert_eq!(
            (
                &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6]
            ),
            (
                &expected.0,
                &expected.1,
                &expected.2,
                &expected.3,
                &expected.4,
                &expected.5
            ),
        );

        let before = decode_hex(fields[8]);
        assert_eq!(
            fields[7].parse::<usize>().expect("before byte count"),
            before.len()
        );
        assert_eq!(allocator_label(&before), fields[3]);
        if fields[2] == "ok" {
            let after = decode_hex(fields[10]);
            assert_eq!(
                fields[9].parse::<usize>().expect("after byte count"),
                after.len()
            );
            assert_eq!(allocator_label(&after), fields[6]);
        } else {
            assert_eq!(fields[9], "0");
            assert_eq!(fields[10], "-");
        }
    }
}

fn allocator_label(envelope: &[u8]) -> String {
    use wire::stored_administration_sequence_allocator_v1::State;

    let decoded = readable_record_registry()
        .decode(envelope)
        .expect("allocator envelope is registered and canonical");
    assert_eq!(
        decoded.record_type(),
        "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1"
    );
    let allocator = wire::StoredAdministrationSequenceAllocatorV1::decode(decoded.payload())
        .expect("allocator payload");
    match allocator.state.expect("allocator state presence") {
        State::NextAdministrationSequence(value) => format!("next:{value}"),
        State::Exhausted(_) => "exhausted".to_owned(),
    }
}

fn validate_compound_bootstrap(lines: &[&str]) {
    use wire::capability_lifecycle_v1::State as CapabilityState;
    use wire::service_audit_link_v1::Link;
    use wire::service_audit_target_v2::Target;

    let expected = [
        ("service-start", "riffdb.storage.v1.ServiceAuditRecordV2"),
        (
            "capability-administration",
            "riffdb.storage.v1.CapabilityAdministrationAuditV1",
        ),
        ("capability-record", "riffdb.storage.v1.CapabilityRecordV1"),
        ("token-lookup", "riffdb.storage.v1.CapabilityTokenLookupV1"),
        (
            "bootstrap-marker",
            "riffdb.storage.v1.CapabilityBootstrapMarkerV1",
        ),
    ];
    let mut service_start = None;
    let mut administration = None;
    let mut capability = None;
    let mut token_lookup = None;
    let mut marker = None;

    for (line, (expected_role, expected_type)) in lines[..5].iter().zip(expected) {
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0], "compound-bootstrap");
        assert_eq!(fields[1], expected_role);
        assert_eq!(fields[2], expected_type);
        let envelope = decode_hex(fields[3]);
        let decoded = readable_record_registry()
            .decode(&envelope)
            .expect("compound-bootstrap member is registered and canonical");
        assert_eq!(decoded.record_type(), expected_type);
        match expected_role {
            "service-start" => {
                service_start = Some(
                    wire::ServiceAuditRecordV2::decode(decoded.payload())
                        .expect("bootstrap service-start payload"),
                );
            }
            "capability-administration" => {
                administration = Some(
                    wire::CapabilityAdministrationAuditV1::decode(decoded.payload())
                        .expect("bootstrap administration payload"),
                );
            }
            "capability-record" => {
                capability = Some(
                    wire::CapabilityRecordV1::decode(decoded.payload())
                        .expect("bootstrap capability payload"),
                );
            }
            "token-lookup" => {
                token_lookup = Some(
                    wire::CapabilityTokenLookupV1::decode(decoded.payload())
                        .expect("bootstrap token-lookup payload"),
                );
            }
            "bootstrap-marker" => {
                marker = Some(
                    wire::CapabilityBootstrapMarkerV1::decode(decoded.payload())
                        .expect("bootstrap marker payload"),
                );
            }
            _ => unreachable!("expected role inventory is closed"),
        }
    }

    let service_start = service_start.expect("service-start fixture");
    let administration = administration.expect("capability-administration fixture");
    let capability = capability.expect("capability-record fixture");
    let token_lookup = token_lookup.expect("token-lookup fixture");
    let marker = marker.expect("bootstrap-marker fixture");
    assert_eq!(
        service_start.administration_sequence.checked_add(1),
        Some(administration.administration_sequence)
    );
    assert_eq!(service_start.request_id, administration.request_id);
    assert_eq!(service_start.timestamp, administration.timestamp);
    assert_eq!(service_start.timestamp, capability.issued_at);
    assert_eq!(
        service_start.operation,
        wire::ServiceOperationV1::ServiceOperationCreateCapability as i32
    );
    assert_eq!(
        service_start.phase,
        wire::ServiceAuditPhaseV1::ServiceAuditPhaseStarted as i32
    );
    assert!(service_start.principal.is_none());
    assert_eq!(
        service_start.ingress,
        wire::ServiceIngressKindV1::ServiceIngressKindGrpc as i32
    );
    assert!(service_start.approval_id.is_none());
    let link = service_start
        .link
        .expect("bootstrap service-start link")
        .link
        .expect("bootstrap service-start link variant");
    assert!(matches!(
        link,
        Link::ControlPlane(value)
            if value.administration_sequence == administration.administration_sequence
    ));
    assert_eq!(service_start.targets.len(), 1);
    assert!(matches!(
        service_start.targets[0].target.as_ref(),
        Some(Target::CapabilityId(value)) if *value == capability.capability_id
    ));

    assert_eq!(
        administration.operation,
        wire::CapabilityAdministrationOperationV1::CapabilityAdministrationOperationBootstrap
            as i32
    );
    assert!(administration.initiator.is_none());
    assert_eq!(
        administration.target_capability_id,
        capability.capability_id
    );
    assert_eq!(administration.resulting_revision, capability.revision);
    assert_eq!(
        administration.administration_sequence,
        capability.creation_sequence
    );
    assert_eq!(administration.request_id, capability.creation_request_id);
    assert!(administration.approval_id.is_none());
    assert!(administration.revocation_reason.is_none());
    assert_eq!(capability.revision, 1);
    assert_eq!(
        capability.actor_kind,
        wire::ActorKindV1::ActorKindHuman as i32
    );
    assert!(matches!(
        capability.lifecycle.and_then(|value| value.state),
        Some(CapabilityState::Active(_))
    ));

    assert_eq!(token_lookup.capability_id, capability.capability_id);
    assert_eq!(marker.database_id, capability.database_id);
    assert_eq!(marker.capability_id, capability.capability_id);
    assert_eq!(
        marker.administration_sequence,
        administration.administration_sequence
    );

    let key_fields = lines[5].split('\t').collect::<Vec<_>>();
    assert_eq!(key_fields.len(), 4);
    assert_eq!(key_fields[0], "compound-bootstrap-token-lookup-key");
    let token_digest = capability.token_digest.expect("capability token digest");
    assert_eq!(
        key_fields[1].parse::<u32>().expect("digest scheme"),
        token_digest.digest_scheme
    );
    assert_eq!(
        key_fields[2].parse::<u32>().expect("digest key ID"),
        token_digest.digest_key_id
    );
    assert_eq!(decode_hex(key_fields[3]), token_digest.digest);
}

fn validate_reciprocity_cases(lines: &[&str]) {
    let mut envelopes = BTreeMap::new();
    let mut requests = BTreeMap::new();
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0], "reciprocity");
        let case = fields[1];
        let expectation = fields[2];
        let role = fields[3];
        assert_eq!(
            expectation,
            if case == "valid" { "valid" } else { "invalid" }
        );

        let envelope = decode_hex(fields[5]);
        let decoded = readable_record_registry()
            .decode(&envelope)
            .expect("relationship member is individually canonical");
        assert_eq!(decoded.record_type(), fields[4]);
        let request_id = match role {
            "outcome" => {
                assert_eq!(fields[4], "riffdb.storage.v1.StoredOutcomeV1");
                wire::StoredOutcomeV1::decode(decoded.payload())
                    .expect("outcome payload")
                    .admission_request_id
            }
            "commit" => {
                assert_eq!(fields[4], "riffdb.storage.v1.StoredCommitRecordV1");
                wire::StoredCommitRecordV1::decode(decoded.payload())
                    .expect("commit payload")
                    .admission_request_id
            }
            "provenance" => {
                assert_eq!(fields[4], "riffdb.storage.v1.StoredProvenanceRecordV1");
                wire::StoredProvenanceRecordV1::decode(decoded.payload())
                    .expect("provenance payload")
                    .admission_request_id
            }
            _ => panic!("unknown relationship role"),
        };
        assert!(envelopes.insert((case, role), envelope).is_none());
        assert!(requests.insert((case, role), request_id).is_none());
    }

    for case in [
        "outcome-request-id-mismatch",
        "commit-request-id-mismatch",
        "provenance-request-id-mismatch",
    ] {
        let changed_role = case
            .strip_suffix("-request-id-mismatch")
            .expect("mismatch case suffix");
        for role in ["outcome", "commit", "provenance"] {
            let baseline = envelopes.get(&("valid", role)).expect("baseline role");
            let candidate = envelopes.get(&(case, role)).expect("mismatch role");
            let baseline_request = requests.get(&("valid", role)).expect("baseline request");
            let candidate_request = requests.get(&(case, role)).expect("mismatch request");
            if role == changed_role {
                assert_ne!(candidate, baseline);
                assert_ne!(candidate_request, baseline_request);
            } else {
                assert_eq!(candidate, baseline);
                assert_eq!(candidate_request, baseline_request);
            }
        }
    }
}

fn validate_event_relationship(lines: &[&str]) -> Vec<u8> {
    let preimage_fields = lines[0].split('\t').collect::<Vec<_>>();
    let hash_fields = lines[1].split('\t').collect::<Vec<_>>();
    assert_eq!(preimage_fields[0], "event-preimage");
    assert_eq!(hash_fields[0], "event-hash");
    let preimage = decode_hex(preimage_fields[1]);
    let event_hash = decode_hex(hash_fields[1]);
    assert_eq!(event_hash.len(), 32);
    assert_eq!(hash_event(&preimage).as_bytes(), event_hash.as_slice());

    let mut copies = BTreeMap::new();
    for line in &lines[2..] {
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], "event-copy");
        assert!(copies.insert(fields[1], decode_hex(fields[2])).is_none());
    }
    assert_eq!(copies.len(), 3);
    let standalone = copies.get("standalone").expect("standalone event");
    assert_eq!(copies.get("commit"), Some(standalone));
    assert_eq!(copies.get("outbox"), Some(standalone));

    let event =
        wire::StoredDurableEventV1::decode(standalone.as_slice()).expect("canonical event message");
    assert_eq!(event.event_hash, event_hash);
    let id = event.event_id.expect("event ID");
    let mut expected_preimage = Vec::new();
    expected_preimage.extend_from_slice(&id.commit_sequence.to_be_bytes());
    expected_preimage.extend_from_slice(&id.event_ordinal.to_be_bytes());
    expected_preimage.extend_from_slice(&event.event_type_id.to_be_bytes());
    expected_preimage.extend_from_slice(
        &u32::try_from(event.canonical_payload.len())
            .expect("bounded event payload")
            .to_be_bytes(),
    );
    expected_preimage.extend_from_slice(&event.canonical_payload);
    assert_eq!(preimage, expected_preimage);

    let mut event_id = Vec::with_capacity(12);
    event_id.extend_from_slice(&id.commit_sequence.to_be_bytes());
    event_id.extend_from_slice(&id.event_ordinal.to_be_bytes());
    event_id
}

fn validate_absent_initial_outbox_status(line: &str, expected_event_id: &[u8]) {
    let fields = line.split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 8);
    assert_eq!(fields[0], "outbox-initial-status");
    assert_eq!(decode_hex(fields[1]), expected_event_id);
    assert_eq!(fields[2], "absent");
    assert_eq!(fields[3], "0");
    assert_eq!(fields[4], "-");
    assert_eq!(fields[5], "absent-initial-pending");
    assert_eq!(fields[6], "pending");
    assert_eq!(fields[7], "0");
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex must contain complete bytes");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex is ASCII");
            u8::from_str_radix(text, 16).expect("lowercase hexadecimal byte")
        })
        .collect()
}
