use prost::Message;
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, CapabilityTokenDigest, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, encode_canonical_record,
};

use crate::{
    AdministrationSequenceAllocator, AffectedEntityV1, BootstrapServiceAuditStartV1,
    CapabilityAdministrationOperationV1, CapabilityBootstrapMarkerV1, CapabilityTokenLookupV1,
    OutboxStatusObservationV1, SequenceAllocationError, StoredCapabilityAdministrationV1,
    StoredCapabilityRecordV1, StoredCommitRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    StoredServiceAuditRecordV1, derive_event_hash_v1,
};

use super::super::*;
use super::{hex, sample};

const OUTCOME: &str = "riffdb.storage.v1.StoredOutcomeV1";
const COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV1";
const PROVENANCE: &str = "riffdb.storage.v1.StoredProvenanceRecordV1";
const SERVICE_AUDIT: &str = "riffdb.storage.v1.ServiceAuditRecordV1";
const CAPABILITY_ADMINISTRATION: &str = "riffdb.storage.v1.CapabilityAdministrationAuditV1";
const CAPABILITY_RECORD: &str = "riffdb.storage.v1.CapabilityRecordV1";
const CAPABILITY_LOOKUP: &str = "riffdb.storage.v1.CapabilityTokenLookupV1";
const BOOTSTRAP_MARKER: &str = "riffdb.storage.v1.CapabilityBootstrapMarkerV1";

#[derive(Clone)]
struct RelationshipGraph {
    outcome: StoredOutcomeV1,
    commit: StoredCommitRecordV1,
    provenance: StoredProvenanceRecordV1,
}

struct EventRelationshipParts {
    event_id: Vec<u8>,
    preimage: Vec<u8>,
    event_hash: Vec<u8>,
    copies: Vec<(&'static str, Vec<u8>)>,
}

struct CompoundBootstrapRelationship {
    service_start: StoredServiceAuditRecordV1,
    administration: StoredCapabilityAdministrationV1,
    capability: StoredCapabilityRecordV1,
    token_lookup: CapabilityTokenLookupV1,
    marker: CapabilityBootstrapMarkerV1,
    token_lookup_key: CapabilityTokenDigest,
}

#[test]
fn compound_bootstrap_allocator_preflights_the_complete_range_before_encoding() {
    let first = AdministrationSequenceAllocator::initial()
        .allocate_consecutive(2)
        .expect("first bootstrap range");
    assert_eq!(
        first
            .assigned()
            .iter()
            .map(|value| value.get())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        first.next(),
        AdministrationSequenceAllocator::next(
            AdministrationSequence::new(3).expect("sequence three")
        )
    );

    let penultimate = AdministrationSequence::new(u64::MAX - 1).expect("penultimate sequence");
    let last = AdministrationSequenceAllocator::next(penultimate)
        .allocate_consecutive(2)
        .expect("last complete bootstrap range");
    assert_eq!(
        last.assigned()
            .iter()
            .map(|value| value.get())
            .collect::<Vec<_>>(),
        [u64::MAX - 1, u64::MAX]
    );
    assert_eq!(last.next(), AdministrationSequenceAllocator::Exhausted);

    let maximum = AdministrationSequenceAllocator::next(
        AdministrationSequence::new(u64::MAX).expect("maximum sequence"),
    );
    assert_eq!(
        maximum.allocate_consecutive(2),
        Err(SequenceAllocationError::Exhausted)
    );
    assert_eq!(
        AdministrationSequenceAllocator::Exhausted.allocate_consecutive(2),
        Err(SequenceAllocationError::Exhausted)
    );

    for state in [
        AdministrationSequenceAllocator::initial(),
        AdministrationSequenceAllocator::next(penultimate),
        maximum,
        AdministrationSequenceAllocator::Exhausted,
        first.next(),
        last.next(),
    ] {
        let encoded =
            encode_administration_sequence_allocator_v1(state).expect("allocator state encodes");
        assert_eq!(
            decode_administration_sequence_allocator_v1(encoded.as_bytes())
                .expect("allocator state decodes")
                .value(),
            &state
        );
        assert_eq!(
            encoded.encoded_content_charge().get(),
            encoded.as_bytes().len()
        );
    }
}

#[test]
fn compound_bootstrap_records_freeze_the_complete_linked_graph() {
    let graph = compound_bootstrap_relationship();
    assert_compound_bootstrap_graph(&graph);

    let service_start =
        encode_service_audit_record_v1(&graph.service_start).expect("service start encodes");
    assert_eq!(
        decode_service_audit_record_v1(service_start.as_bytes())
            .expect("service start decodes")
            .value(),
        &graph.service_start
    );
    let administration = encode_capability_administration_v1(&graph.administration)
        .expect("bootstrap administration encodes");
    assert_eq!(
        decode_capability_administration_v1(administration.as_bytes())
            .expect("bootstrap administration decodes")
            .value(),
        &graph.administration
    );
    let capability =
        encode_capability_record_v1(&graph.capability).expect("bootstrap capability encodes");
    assert_eq!(
        decode_capability_record_v1(capability.as_bytes())
            .expect("bootstrap capability decodes")
            .value(),
        &graph.capability
    );
    let token_lookup = encode_capability_token_lookup_v1(graph.token_lookup)
        .expect("bootstrap token lookup encodes");
    assert_eq!(
        decode_capability_token_lookup_v1(token_lookup.as_bytes())
            .expect("bootstrap token lookup decodes")
            .value(),
        &graph.token_lookup
    );
    let marker =
        encode_capability_bootstrap_marker_v1(graph.marker).expect("bootstrap marker encodes");
    assert_eq!(
        decode_capability_bootstrap_marker_v1(marker.as_bytes())
            .expect("bootstrap marker decodes")
            .value(),
        &graph.marker
    );
}

#[test]
fn reciprocity_vectors_are_individually_valid_but_graph_mismatches_fail_the_oracle() {
    let cases = relationship_cases();
    assert!(graph_agrees(&cases[0].1));
    for (_, graph, expected_valid) in &cases {
        assert_eq!(graph_agrees(graph), *expected_valid);
        assert_codec_round_trip(graph);
    }
}

#[test]
fn standalone_commit_and_outbox_event_messages_are_identical_and_hash_the_exact_preimage() {
    let EventRelationshipParts {
        event_id: _,
        preimage,
        event_hash,
        copies,
    } = event_relationship_parts();
    assert_eq!(copies[0].1, copies[1].1);
    assert_eq!(copies[0].1, copies[2].1);

    let records = sample::atomic_record_set();
    let event = &records.events()[0];
    assert_eq!(event.event_hash().as_bytes(), event_hash.as_slice());
    assert_eq!(
        derive_event_hash_v1(event.event_id(), event.event_type_id(), event.payload())
            .expect("event hash derives")
            .as_bytes(),
        event_hash.as_slice()
    );

    let payload = encode_canonical_record(event.payload()).expect("canonical event payload");
    let mut expected = Vec::new();
    expected.extend_from_slice(&event.event_id().to_be_bytes());
    expected.extend_from_slice(&event.event_type_id().to_be_bytes());
    expected.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("bounded payload length")
            .to_be_bytes(),
    );
    expected.extend_from_slice(&payload);
    assert_eq!(preimage, expected);
}

#[test]
fn absent_initial_outbox_status_is_no_row_and_zero_attempt_pending() {
    let observation = OutboxStatusObservationV1::AbsentInitialPending;
    let status_envelope: Option<CanonicalStoredEnvelopeV1> = None;

    assert!(observation.is_pending());
    assert_eq!(observation.attempts(), 0);
    assert!(status_envelope.is_none());
    assert_eq!(
        status_envelope
            .as_ref()
            .map_or(0, |envelope| envelope.as_bytes().len()),
        0
    );
}

#[test]
fn checked_in_durable_relationship_vectors_are_current() {
    assert_eq!(
        relationship_wire_fixture(),
        include_str!("../../../../../fixtures/proto/durable-relationship-vectors.txt")
    );
}

#[test]
fn emit_relationship_vectors_for_fixture_regeneration() {
    let fixture = relationship_wire_fixture();
    print!("{fixture}");
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_RELATIONSHIP_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create relationship-vector parent directory");
        }
        std::fs::write(path, fixture).expect("write durable relationship fixture");
    }
}

pub(super) fn relationship_wire_fixture() -> String {
    let mut fixture = String::from("riffdb-durable-relationship-vectors-v1\nbootstrap-cases\t5\n");
    append_bootstrap_cases(&mut fixture);
    append_compound_bootstrap_relationship(&mut fixture);

    let cases = relationship_cases();
    fixture.push_str("reciprocity-cases\t4\nreciprocity-records\t12\n");
    for (case, graph, expected_valid) in cases {
        let expectation = if expected_valid { "valid" } else { "invalid" };
        append_relationship_record(
            &mut fixture,
            case,
            expectation,
            "outcome",
            OUTCOME,
            encode_stored_outcome_v1(&graph.outcome).expect("outcome encodes"),
        );
        append_relationship_record(
            &mut fixture,
            case,
            expectation,
            "commit",
            COMMIT,
            encode_commit_record_v1(&graph.commit).expect("commit encodes"),
        );
        append_relationship_record(
            &mut fixture,
            case,
            expectation,
            "provenance",
            PROVENANCE,
            encode_provenance_record_v1(&graph.provenance).expect("provenance encodes"),
        );
    }

    let EventRelationshipParts {
        event_id,
        preimage,
        event_hash,
        copies,
    } = event_relationship_parts();
    fixture.push_str("event-copies\t3\n");
    fixture.push_str(&format!("event-preimage\t{}\n", hex(&preimage)));
    fixture.push_str(&format!("event-hash\t{}\n", hex(&event_hash)));
    for (location, bytes) in copies {
        fixture.push_str(&format!("event-copy\t{location}\t{}\n", hex(&bytes)));
    }
    append_absent_initial_outbox_status(&mut fixture, &event_id);
    fixture
}

fn append_bootstrap_cases(fixture: &mut String) {
    let first = AdministrationSequenceAllocator::initial();
    let penultimate = AdministrationSequenceAllocator::next(
        AdministrationSequence::new(u64::MAX - 1).expect("penultimate sequence"),
    );
    let maximum = AdministrationSequenceAllocator::next(
        AdministrationSequence::new(u64::MAX).expect("maximum sequence"),
    );
    let exhausted = AdministrationSequenceAllocator::Exhausted;

    append_bootstrap_success(fixture, "first-two", first, 2);
    append_bootstrap_success(fixture, "last-two", penultimate, 2);
    append_bootstrap_success(fixture, "maximum-one", maximum, 1);
    append_bootstrap_failure(fixture, "maximum-two", maximum, 2);
    append_bootstrap_failure(fixture, "exhausted-two", exhausted, 2);
}

fn append_bootstrap_success(
    fixture: &mut String,
    name: &str,
    before: AdministrationSequenceAllocator,
    count: u8,
) {
    let allocated = before
        .allocate_consecutive(count)
        .expect("successful fixture allocation");
    let assigned = allocated
        .assigned()
        .iter()
        .map(|value| value.get().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let before_envelope =
        encode_administration_sequence_allocator_v1(before).expect("before allocator encodes");
    let after_envelope = encode_administration_sequence_allocator_v1(allocated.next())
        .expect("after allocator encodes");
    fixture.push_str(&format!(
        "bootstrap\t{name}\tok\t{}\t{count}\t{assigned}\t{}\t{}\t{}\t{}\t{}\n",
        allocator_label(before),
        allocator_label(allocated.next()),
        before_envelope.as_bytes().len(),
        hex(before_envelope.as_bytes()),
        after_envelope.as_bytes().len(),
        hex(after_envelope.as_bytes()),
    ));
}

fn append_bootstrap_failure(
    fixture: &mut String,
    name: &str,
    before: AdministrationSequenceAllocator,
    count: u8,
) {
    assert_eq!(
        before.allocate_consecutive(count),
        Err(SequenceAllocationError::Exhausted)
    );
    let before_envelope = encode_administration_sequence_allocator_v1(before)
        .expect("failed preflight input encodes");
    fixture.push_str(&format!(
        "bootstrap\t{name}\terror-exhausted\t{}\t{count}\t-\tno-write\t{}\t{}\t0\t-\n",
        allocator_label(before),
        before_envelope.as_bytes().len(),
        hex(before_envelope.as_bytes()),
    ));
}

fn allocator_label(value: AdministrationSequenceAllocator) -> String {
    match value {
        AdministrationSequenceAllocator::Next(sequence) => format!("next:{}", sequence.get()),
        AdministrationSequenceAllocator::Exhausted => "exhausted".to_owned(),
    }
}

fn compound_bootstrap_relationship() -> CompoundBootstrapRelationship {
    let (seed_capability, _, _, _) = sample::capability_records();
    let service_sequence = AdministrationSequence::first();
    let administration_sequence = service_sequence
        .checked_next()
        .expect("bootstrap transition sequence");
    let request_id = sample::request_id();
    let timestamp = seed_capability.issued_at();
    let targets = ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(
        seed_capability.capability_id(),
    )])
    .expect("bootstrap service target");
    let start = BootstrapServiceAuditStartV1::new(
        request_id,
        timestamp,
        ServiceIngressKindV1::Grpc,
        targets,
        None,
    )
    .expect("bootstrap service start");
    let service_start = StoredServiceAuditRecordV1::from_bootstrap_start(
        service_sequence,
        &start,
        administration_sequence,
    )
    .expect("linked bootstrap service start");
    let capability = StoredCapabilityRecordV1::from_stored_parts(
        seed_capability.capability_id(),
        seed_capability.revision(),
        seed_capability.token_digest(),
        seed_capability.database_id(),
        seed_capability.environment().clone(),
        seed_capability.principal_id().clone(),
        seed_capability.actor_kind(),
        seed_capability.audiences().to_vec(),
        seed_capability.issued_at(),
        seed_capability.expires_at(),
        administration_sequence,
        request_id,
        seed_capability.grant().clone(),
        seed_capability.lifecycle().clone(),
    )
    .expect("bootstrap capability record");
    let token_lookup = CapabilityTokenLookupV1::new(capability.capability_id());
    let marker = CapabilityBootstrapMarkerV1::new(
        capability.database_id(),
        capability.capability_id(),
        administration_sequence,
    );
    let administration = StoredCapabilityAdministrationV1::new(
        administration_sequence,
        request_id,
        CapabilityAdministrationOperationV1::Bootstrap,
        timestamp,
        None,
        capability.capability_id(),
        capability.revision(),
        None,
        None,
    )
    .expect("bootstrap administration record");

    CompoundBootstrapRelationship {
        token_lookup_key: capability.token_digest(),
        service_start,
        administration,
        capability,
        token_lookup,
        marker,
    }
}

fn assert_compound_bootstrap_graph(graph: &CompoundBootstrapRelationship) {
    let transition_sequence = graph.administration.administration_sequence();
    assert_eq!(
        graph.service_start.administration_sequence().checked_next(),
        Some(transition_sequence)
    );
    assert_eq!(
        graph.service_start.request_id(),
        graph.administration.request_id()
    );
    assert_eq!(
        graph.service_start.timestamp(),
        graph.administration.timestamp()
    );
    assert_eq!(
        graph.service_start.operation(),
        ServiceOperationV1::CreateCapability
    );
    assert_eq!(graph.service_start.phase(), ServiceAuditPhaseV1::Started);
    assert!(graph.service_start.principal().is_none());
    assert_eq!(
        graph.service_start.link(),
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence: transition_sequence,
        }
    );
    assert_eq!(
        graph.service_start.targets().as_slice(),
        [ServiceAuditTargetV1::Capability(
            graph.capability.capability_id()
        )]
    );

    assert_eq!(
        graph.administration.operation(),
        CapabilityAdministrationOperationV1::Bootstrap
    );
    assert!(graph.administration.initiator().is_none());
    assert_eq!(
        graph.administration.target_capability_id(),
        graph.capability.capability_id()
    );
    assert_eq!(
        graph.administration.resulting_revision(),
        graph.capability.revision()
    );
    assert_eq!(graph.capability.creation_sequence(), transition_sequence);
    assert_eq!(
        graph.capability.creation_request_id(),
        graph.administration.request_id()
    );
    assert_eq!(
        graph.capability.issued_at(),
        graph.administration.timestamp()
    );
    assert_eq!(graph.token_lookup_key, graph.capability.token_digest());
    assert_eq!(
        graph.token_lookup.capability_id(),
        graph.capability.capability_id()
    );
    assert_eq!(graph.marker.database_id(), graph.capability.database_id());
    assert_eq!(
        graph.marker.capability_id(),
        graph.capability.capability_id()
    );
    assert_eq!(graph.marker.administration_sequence(), transition_sequence);
}

fn append_compound_bootstrap_relationship(fixture: &mut String) {
    let graph = compound_bootstrap_relationship();
    assert_compound_bootstrap_graph(&graph);
    fixture.push_str("compound-bootstrap-records\t5\n");
    for (role, record_type, envelope) in [
        (
            "service-start",
            SERVICE_AUDIT,
            encode_service_audit_record_v1(&graph.service_start)
                .expect("bootstrap service start encodes"),
        ),
        (
            "capability-administration",
            CAPABILITY_ADMINISTRATION,
            encode_capability_administration_v1(&graph.administration)
                .expect("bootstrap administration encodes"),
        ),
        (
            "capability-record",
            CAPABILITY_RECORD,
            encode_capability_record_v1(&graph.capability).expect("bootstrap capability encodes"),
        ),
        (
            "token-lookup",
            CAPABILITY_LOOKUP,
            encode_capability_token_lookup_v1(graph.token_lookup)
                .expect("bootstrap token lookup encodes"),
        ),
        (
            "bootstrap-marker",
            BOOTSTRAP_MARKER,
            encode_capability_bootstrap_marker_v1(graph.marker).expect("bootstrap marker encodes"),
        ),
    ] {
        fixture.push_str(&format!(
            "compound-bootstrap\t{role}\t{record_type}\t{}\n",
            hex(envelope.as_bytes())
        ));
    }
    fixture.push_str(&format!(
        "compound-bootstrap-token-lookup-key\t{}\t{}\t{}\n",
        graph.token_lookup_key.scheme(),
        graph.token_lookup_key.key_id().get(),
        hex(graph.token_lookup_key.as_bytes()),
    ));
}

fn append_absent_initial_outbox_status(fixture: &mut String, event_id: &[u8]) {
    let observation = OutboxStatusObservationV1::AbsentInitialPending;
    let status_envelope: Option<CanonicalStoredEnvelopeV1> = None;
    assert!(observation.is_pending());
    assert_eq!(observation.attempts(), 0);
    assert!(status_envelope.is_none());

    fixture.push_str("outbox-initial-status-cases\t1\n");
    fixture.push_str(&format!(
        "outbox-initial-status\t{}\tabsent\t0\t-\tabsent-initial-pending\tpending\t{}\n",
        hex(event_id),
        observation.attempts(),
    ));
}

fn relationship_cases() -> Vec<(&'static str, RelationshipGraph, bool)> {
    let records = sample::atomic_record_set();
    let baseline = RelationshipGraph {
        outcome: records.stored_outcome().clone(),
        commit: records.commit().clone(),
        provenance: records.provenance().clone(),
    };
    let alternate_request =
        RequestId::from_bytes(sample::uuid_v7(0x15)).expect("alternate request ID");

    let mut outcome_mismatch = baseline.clone();
    outcome_mismatch.outcome = outcome_with_request(&baseline.outcome, alternate_request);
    let mut commit_mismatch = baseline.clone();
    commit_mismatch.commit = commit_with_request(&baseline.commit, alternate_request);
    let mut provenance_mismatch = baseline.clone();
    provenance_mismatch.provenance =
        provenance_with_request(&baseline.provenance, alternate_request);

    vec![
        ("valid", baseline, true),
        ("outcome-request-id-mismatch", outcome_mismatch, false),
        ("commit-request-id-mismatch", commit_mismatch, false),
        ("provenance-request-id-mismatch", provenance_mismatch, false),
    ]
}

fn outcome_with_request(value: &StoredOutcomeV1, request_id: RequestId) -> StoredOutcomeV1 {
    StoredOutcomeV1::new(
        value.identity().clone(),
        value.commit_sequence(),
        request_id,
        value.plan().clone(),
        value.canonical_input_hash(),
        value.actor().clone(),
        value.logical_time(),
        value.partition_hash(),
        value.conflict_hashes().to_vec(),
        value.declared_outcome().clone(),
        value.admitted_claims().clone(),
        value.provenance_id(),
        value.durability_mode(),
    )
    .expect("mismatched outcome remains individually valid")
}

fn commit_with_request(
    value: &StoredCommitRecordV1,
    request_id: RequestId,
) -> StoredCommitRecordV1 {
    StoredCommitRecordV1::new(
        value.commit_sequence(),
        request_id,
        value.plan().clone(),
        value.canonical_input_hash(),
        value.actor().clone(),
        value.logical_time(),
        value.partition_hash(),
        value.conflict_hashes().to_vec(),
        value.read_dependencies().clone(),
        value.mutations().to_vec(),
        value.events().to_vec(),
        value.declared_outcome().clone(),
        value.provenance_id(),
        value.outbox_event_ids().to_vec(),
        value.durability_mode(),
    )
    .expect("mismatched commit remains individually valid")
}

fn provenance_with_request(
    value: &StoredProvenanceRecordV1,
    request_id: RequestId,
) -> StoredProvenanceRecordV1 {
    StoredProvenanceRecordV1::new(
        value.provenance_id(),
        value.commit_sequence(),
        value.identity().clone(),
        request_id,
        value.plan().clone(),
        value.canonical_input_hash(),
        value.actor().clone(),
        value.logical_time(),
        value.partition_hash(),
        value.conflict_hashes().to_vec(),
        value.outcome_id(),
        value.affected_entities().to_vec(),
        value.event_ids().to_vec(),
        value.admitted_claims().clone(),
    )
    .expect("mismatched provenance remains individually valid")
}

fn graph_agrees(graph: &RelationshipGraph) -> bool {
    let outcome = &graph.outcome;
    let commit = &graph.commit;
    let provenance = &graph.provenance;
    let affected = commit
        .mutations()
        .iter()
        .map(|mutation| AffectedEntityV1::from_record(mutation.post_image()))
        .collect::<Vec<_>>();

    outcome.commit_sequence() == commit.commit_sequence()
        && outcome.admission_request_id() == commit.admission_request_id()
        && outcome.plan() == commit.plan()
        && outcome.canonical_input_hash() == commit.canonical_input_hash()
        && outcome.actor() == commit.actor()
        && outcome.logical_time() == commit.logical_time()
        && outcome.partition_hash() == commit.partition_hash()
        && outcome.conflict_hashes() == commit.conflict_hashes()
        && outcome.declared_outcome() == commit.declared_outcome()
        && outcome.provenance_id() == commit.provenance_id()
        && outcome.durability_mode() == commit.durability_mode()
        && provenance.provenance_id() == commit.provenance_id()
        && provenance.commit_sequence() == commit.commit_sequence()
        && provenance.identity() == outcome.identity()
        && provenance.admission_request_id() == commit.admission_request_id()
        && provenance.plan() == commit.plan()
        && provenance.canonical_input_hash() == commit.canonical_input_hash()
        && provenance.actor() == commit.actor()
        && provenance.logical_time() == commit.logical_time()
        && provenance.partition_hash() == commit.partition_hash()
        && provenance.conflict_hashes() == commit.conflict_hashes()
        && provenance.outcome_id() == commit.declared_outcome().outcome_id()
        && provenance.affected_entities() == affected
        && provenance.event_ids() == commit.event_ids()
        && provenance.admitted_claims() == outcome.admitted_claims()
}

fn assert_codec_round_trip(graph: &RelationshipGraph) {
    let outcome = encode_stored_outcome_v1(&graph.outcome).expect("outcome encodes");
    assert_eq!(
        decode_stored_outcome_v1(outcome.as_bytes())
            .expect("outcome individually decodes")
            .value(),
        &graph.outcome
    );
    let commit = encode_commit_record_v1(&graph.commit).expect("commit encodes");
    assert_eq!(
        decode_commit_record_v1(commit.as_bytes())
            .expect("commit individually decodes")
            .value(),
        &graph.commit
    );
    let provenance = encode_provenance_record_v1(&graph.provenance).expect("provenance encodes");
    assert_eq!(
        decode_provenance_record_v1(provenance.as_bytes())
            .expect("provenance individually decodes")
            .value(),
        &graph.provenance
    );
}

fn append_relationship_record(
    fixture: &mut String,
    case: &str,
    expectation: &str,
    role: &str,
    record_type: &str,
    envelope: CanonicalStoredEnvelopeV1,
) {
    fixture.push_str(&format!(
        "reciprocity\t{case}\t{expectation}\t{role}\t{record_type}\t{}\n",
        hex(envelope.as_bytes())
    ));
}

fn event_relationship_parts() -> EventRelationshipParts {
    let records = sample::atomic_record_set();
    let event = &records.events()[0];
    let payload = encode_canonical_record(event.payload()).expect("canonical event payload");
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&event.event_id().to_be_bytes());
    preimage.extend_from_slice(&event.event_type_id().to_be_bytes());
    preimage.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("bounded payload length")
            .to_be_bytes(),
    );
    preimage.extend_from_slice(&payload);

    let standalone_envelope = encode_durable_event_v1(event).expect("standalone event encodes");
    let standalone = riffdb_proto::durable::current_record_registry()
        .decode(standalone_envelope.as_bytes())
        .expect("standalone event envelope")
        .payload()
        .to_vec();

    let commit_envelope = encode_commit_record_v1(records.commit()).expect("commit encodes");
    let commit_payload = riffdb_proto::durable::current_record_registry()
        .decode(commit_envelope.as_bytes())
        .expect("commit envelope");
    let commit = wire::StoredCommitRecordV1::decode(commit_payload.payload())
        .expect("commit payload decodes");
    let nested_commit = commit.events[0].encode_to_vec();

    let outbox_envelope =
        encode_outbox_intent_v1(&records.outbox_intents()[0]).expect("outbox intent encodes");
    let outbox_payload = riffdb_proto::durable::current_record_registry()
        .decode(outbox_envelope.as_bytes())
        .expect("outbox envelope");
    let outbox = wire::StoredOutboxIntentV1::decode(outbox_payload.payload())
        .expect("outbox payload decodes");
    let nested_outbox = outbox.event.expect("nested outbox event").encode_to_vec();

    EventRelationshipParts {
        event_id: event.event_id().to_be_bytes().to_vec(),
        preimage,
        event_hash: event.event_hash().as_bytes().to_vec(),
        copies: vec![
            ("standalone", standalone),
            ("commit", nested_commit),
            ("outbox", nested_outbox),
        ],
    }
}
