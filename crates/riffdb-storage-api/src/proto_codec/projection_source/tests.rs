//! New source-codec proofs. No constructed fixture grants source authority.

use super::metadata::{maintenance_to_wire, object_to_wire, root_to_wire};
use super::row::row_to_wire;
use super::*;
use crate::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogTransactionSequence, ProjectionSourceDescriptorPartsV1,
};
use prost::Message;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, DatabaseId, DualFrontier,
    FieldId, HashDomain, LeadershipEpochV1, ProjectionCheckpointOrdinal, ProjectionGeneration,
    ProjectionId, ProjectionIdentity, ProjectionPlanHash, ProjectionSourceLocatorV1, hash,
};

fn seq(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}
fn locator(ordinal: u64) -> ProjectionSourceLocatorV1 {
    ProjectionSourceLocatorV1::new(
        DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap(),
        2,
        ProjectionCheckpointOrdinal::new(ordinal).unwrap(),
    )
    .unwrap()
}
fn identity() -> ProjectionIdentity {
    ProjectionIdentity::new(
        ContractLineage::new("source_codec_proof").unwrap(),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([9; 32]),
    )
}
fn descriptor() -> ProjectionSourceDescriptorV1 {
    let point = |value| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(value).unwrap(),
            [value as u8; 32],
            DualFrontier::new(Some(seq(value)), None),
        )
    };
    ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity: identity(),
        locator: locator(1),
        applied_through: seq(3),
        captured_source: ChangelogHistoryStateV3::new(
            ChangelogLineageV3::new(locator(1).database_id(), 2, LeadershipEpochV1::initial())
                .unwrap(),
            point(1),
            point(3),
            point(2),
        )
        .unwrap(),
        origin: hash(
            HashDomain::ProjectionSourceOrigin,
            b"synthetic independent origin",
        ),
        prior_source_digest: None,
        row_count: 1,
        canonical_row_bytes: 900,
        row_root: hash(
            HashDomain::ProjectionSourceRows,
            b"synthetic independent semantic rows",
        ),
    })
    .unwrap()
}
fn row() -> ProjectionSourceRowV1 {
    ProjectionSourceRowV1::from_stored_parts(
        identity(),
        locator(1),
        vec![CanonicalValue::U64(7)],
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(8))]).unwrap(),
        seq(2),
    )
    .unwrap()
}
fn root() -> ProjectionSourceRootV1 {
    let descriptor = descriptor();
    let tail = descriptor.captured_source().tail();
    ProjectionSourceRootV1::new(
        descriptor.reference().clone(),
        AuthoritativeTransactionBindingV3 {
            database_id: locator(1).database_id(),
            history_incarnation: 2,
            predecessor: Some(tail.sequence()),
            sequence: tail.sequence().checked_next().unwrap(),
            predecessor_frontier: tail.frontier(),
            covered_frontier: tail.frontier(),
            prior_history_hash: tail.history_hash(),
        },
        None,
    )
    .unwrap()
}
fn raw_payload<M: riffdb_proto::durable::ReadableRecordMessage>(message: &M) -> Vec<u8> {
    let payload = message.encode_to_vec();
    let schema = M::record_schema();
    wire::StoredEnvelope {
        storage_format_version: 1,
        record_type: schema.record_type().to_owned(),
        payload_crc32c: riffdb_proto::envelope::payload_crc32c(&payload),
        payload,
        schema_hash: schema.schema_hash().as_bytes().to_vec(),
    }
    .encode_to_vec()
}
type AdmittedSourceResult<T> = Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<T>>,
    ProjectionSourceRecordDecodeErrorV1,
>;

fn roundtrip<T: riffdb_types::InspectOwnedCapacityV1 + std::fmt::Debug + Eq>(
    value: &T,
    encode: impl FnOnce(&T) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError>,
    decode: impl FnOnce(&[u8], &ProjectionSourceWorkingBudgetV1) -> AdmittedSourceResult<T>,
) {
    let encoded = encode(value).unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let initial = budget.observation().unwrap().owned_bytes;
    let decoded = decode(encoded.as_bytes(), &budget).unwrap();
    assert_eq!(&**decoded.value(), value);
    assert_eq!(
        decoded.encoded_content_charge().get(),
        encoded.as_bytes().len()
    );
    assert!(decoded.value().belongs_to(&budget));
    let retained = decoded.value().clone();
    drop(decoded);
    assert!(budget.observation().unwrap().owned_bytes > initial);
    drop(retained);
    assert_eq!(budget.observation().unwrap().owned_bytes, initial);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: STO-012, PRJ-004, REC-003
#[test]
fn source_six_envelope_roles_roundtrip_complete_context_and_release_final_ownership() {
    roundtrip(
        &row(),
        encode_projection_source_row_v1,
        decode_projection_source_row_structural_admitted_v1,
    );
    roundtrip(
        &ProjectionSourceObjectV1::new(descriptor(), ProjectionSourceLifecycleV1::Copying),
        encode_projection_source_object_v1,
        decode_projection_source_object_admitted_v1,
    );
    roundtrip(
        &root(),
        encode_projection_source_root_v1,
        decode_projection_source_root_admitted_v1,
    );
    roundtrip(
        &ProjectionSourceMaintenanceV1::initial(descriptor(), None).unwrap(),
        encode_projection_source_maintenance_v1,
        decode_projection_source_maintenance_admitted_v1,
    );
    roundtrip(
        &GenerationSourceBaseV1::new(
            ProjectionGeneration::new(7).unwrap(),
            descriptor().reference().clone(),
            seq(3),
        ),
        encode_generation_source_base_v1,
        decode_generation_source_base_admitted_v1,
    );
    for position in [
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(u64::MAX).unwrap(),
        ),
        ProjectionCheckpointOrdinalPositionV1::Exhausted,
    ] {
        roundtrip(
            &ProjectionCheckpointOrdinalAllocatorV1::new(2, position).unwrap(),
            encode_projection_checkpoint_ordinal_allocator_v1,
            decode_projection_checkpoint_ordinal_allocator_admitted_v1,
        );
    }
}

// req: STO-012, PRJ-004, REC-003
#[test]
fn source_codec_refuses_unknown_fields_wrong_family_noncanonical_values_and_context() {
    let encoded = encode_projection_source_row_v1(&row()).unwrap();
    let mut message = row_to_wire(&row()).unwrap();
    message.canonical_group_values[0].push(0);
    assert!(
        decode_projection_source_row_structural_admitted_v1(
            &raw_payload(&message),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    let mut payload = row_to_wire(&row()).unwrap().encode_to_vec();
    payload.extend_from_slice(&[0x98, 0x06, 1]);
    let schema=<wire::StoredProjectionSourceRowV1 as riffdb_proto::durable::ReadableRecordMessage>::record_schema();
    let unknown = wire::StoredEnvelope {
        storage_format_version: 1,
        record_type: schema.record_type().to_owned(),
        payload_crc32c: riffdb_proto::envelope::payload_crc32c(&payload),
        payload,
        schema_hash: schema.schema_hash().as_bytes().to_vec(),
    }
    .encode_to_vec();
    assert!(
        decode_projection_source_row_structural_admitted_v1(
            &unknown,
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    assert!(
        decode_projection_source_root_admitted_v1(
            encoded.as_bytes(),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    let bundle=riffdb_contract_compiler::compile_contract_source("contract SourceContext version 1 { event Input { group: u64 } projection Totals { source event Input key (group) measure total = count() frontier transactionally_ordered } }").unwrap();
    let schema = crate::CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(ProjectionId::first())
            .unwrap(),
    );
    assert!(
        decode_projection_source_row_admitted_v1(
            encoded.as_bytes(),
            &schema,
            locator(1),
            seq(3),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err(),
        "the synthetic foreign plan is refused independently"
    );
    let key = schema
        .group_key(ProjectionGeneration::first(), &[CanonicalValue::U64(7)])
        .unwrap();
    let matching = ProjectionSourceRowV1::new(
        &schema,
        locator(1),
        seq(3),
        key,
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(8))]).unwrap(),
        seq(2),
    )
    .unwrap();
    let matching = encode_projection_source_row_v1(&matching).unwrap();
    let exact = decode_projection_source_row_admitted_v1(
        matching.as_bytes(),
        &schema,
        locator(1),
        seq(3),
        &ProjectionSourceWorkingBudgetV1::new(),
    )
    .unwrap();
    assert_eq!(exact.value().last_changed_sequence(), seq(2));
    assert!(
        decode_projection_source_row_admitted_v1(
            matching.as_bytes(),
            &schema,
            locator(2),
            seq(3),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err(),
        "only the expected locator differs"
    );
    assert!(
        decode_projection_source_row_admitted_v1(
            matching.as_bytes(),
            &schema,
            locator(1),
            seq(1),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err(),
        "only the captured frontier differs"
    );
}

// req: STO-012, PRJ-004
#[test]
fn source_codec_descriptor_digest_and_hash_scheme_cannot_be_substituted() {
    let object =
        ProjectionSourceObjectV1::new(descriptor(), ProjectionSourceLifecycleV1::Published);
    let mut message = object_to_wire(&object);
    message.descriptor.as_mut().unwrap().row_count = 2;
    assert!(
        decode_projection_source_object_admitted_v1(
            &raw_payload(&message),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    let mut message = object_to_wire(&object);
    message
        .descriptor
        .as_mut()
        .unwrap()
        .origin_commitment
        .as_mut()
        .unwrap()
        .scheme = 2;
    assert!(
        decode_projection_source_object_admitted_v1(
            &raw_payload(&message),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    let mut message = object_to_wire(&object);
    message
        .descriptor
        .as_mut()
        .unwrap()
        .canonical_row_root
        .as_mut()
        .unwrap()
        .digest
        .pop();
    assert!(
        decode_projection_source_object_admitted_v1(
            &raw_payload(&message),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
    let mut message = object_to_wire(&object);
    message.lifecycle = 255;
    assert!(
        decode_projection_source_object_admitted_v1(
            &raw_payload(&message),
            &ProjectionSourceWorkingBudgetV1::new()
        )
        .is_err()
    );
}

// req: STO-012, REC-001, REC-003
#[test]
fn source_codec_publication_binding_is_nonrecursive_and_allocators_refuse_zero_or_absence() {
    let expected = root();
    let encoded = encode_projection_source_root_v1(&expected).unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let decoded = decode_projection_source_root_admitted_v1(encoded.as_bytes(), &budget).unwrap();
    assert_eq!(
        decoded.value().publication_binding(),
        expected.publication_binding()
    );
    for field in 0..3 {
        let mut wire = root_to_wire(&expected);
        let binding = wire.publication_binding.as_mut().unwrap();
        match field {
            0 => binding.history_incarnation = 3,
            1 => binding.sequence += 1,
            _ => {
                binding
                    .covered_frontier
                    .as_mut()
                    .unwrap()
                    .application_sequence = Some(4)
            }
        }
        assert!(decode_projection_source_root_admitted_v1(&raw_payload(&wire), &budget).is_err());
    }
    for position in [
        None,
        Some(wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(0)),
    ] {
        let message = wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
            allocation_history_incarnation: 2,
            position,
        };
        assert!(
            decode_projection_checkpoint_ordinal_allocator_admitted_v1(
                &raw_payload(&message),
                &budget
            )
            .is_err()
        );
    }
}

// req: STO-020, PRJ-004
#[test]
fn source_control_ceiling_and_pressure_refuse_before_decode_without_leaking_allowance() {
    let initial = ProjectionSourceMaintenanceV1::initial(descriptor(), None).unwrap();
    let complete = ProjectionSourceMaintenanceV1::from_stored_parts(
        descriptor(),
        None,
        1,
        900,
        Some(vec![7; 4096]),
        hash(
            HashDomain::ProjectionSourceCopyProgress,
            b"synthetic complete copy progress",
        ),
    )
    .unwrap();
    assert_eq!(
        encode_projection_source_maintenance_v1(&complete)
            .unwrap_err()
            .kind(),
        super::super::DurableCodecErrorKind::LimitExceeded
    );
    let encoded = raw_payload(&maintenance_to_wire(&complete));
    assert!(encoded.len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    assert!(decode_projection_source_maintenance_admitted_v1(&encoded, &budget).is_err());
    assert_eq!(budget.observation().unwrap(), before);
    let blocked = budget
        .reserve(
            riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes,
        )
        .unwrap();
    let row = encode_projection_source_row_v1(&row()).unwrap();
    assert!(matches!(
        decode_projection_source_row_structural_admitted_v1(row.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    drop(blocked);
    assert_eq!(budget.observation().unwrap(), before);
    assert!(!initial.copy_complete());
}

// req: STO-012, PRJ-004, REC-003
#[test]
fn source_codec_canonical_wire_vectors_are_exact() {
    let cases = vec![
        ("row", encode_projection_source_row_v1(&row()).unwrap()),
        (
            "object-copying",
            encode_projection_source_object_v1(&ProjectionSourceObjectV1::new(
                descriptor(),
                ProjectionSourceLifecycleV1::Copying,
            ))
            .unwrap(),
        ),
        ("root", encode_projection_source_root_v1(&root()).unwrap()),
        (
            "maintenance-initial",
            encode_projection_source_maintenance_v1(
                &ProjectionSourceMaintenanceV1::initial(descriptor(), None).unwrap(),
            )
            .unwrap(),
        ),
        (
            "generation-base",
            encode_generation_source_base_v1(&GenerationSourceBaseV1::new(
                ProjectionGeneration::new(7).unwrap(),
                descriptor().reference().clone(),
                seq(3),
            ))
            .unwrap(),
        ),
        (
            "allocator-next",
            encode_projection_checkpoint_ordinal_allocator_v1(
                &ProjectionCheckpointOrdinalAllocatorV1::initial(2).unwrap(),
            )
            .unwrap(),
        ),
        (
            "allocator-exhausted",
            encode_projection_checkpoint_ordinal_allocator_v1(
                &ProjectionCheckpointOrdinalAllocatorV1::new(
                    2,
                    ProjectionCheckpointOrdinalPositionV1::Exhausted,
                )
                .unwrap(),
            )
            .unwrap(),
        ),
    ];
    fn hex(bytes: &[u8]) -> String {
        let mut result = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut result, "{byte:02x}").unwrap();
        }
        result
    }
    let mut fixture = format!(
        "riffdb-projection-source-wire-vectors-v1\nrecords {}\n",
        cases.len()
    );
    for (name, compact) in cases {
        let decoded = riffdb_proto::durable::readable_record_registry()
            .decode(compact.as_bytes())
            .unwrap();
        let schema = riffdb_proto::durable::readable_record_schema(decoded.record_type()).unwrap();
        let legacy = riffdb_proto::envelope::encode_v1(schema, decoded.payload()).unwrap();
        fixture.push_str(&format!(
            "{name}\t{}\t{}\t{}\t{}\n",
            decoded.record_type(),
            hex(decoded.payload()),
            hex(compact.as_bytes()),
            hex(&legacy)
        ));
    }
    // Generation mode produces concrete review bytes on the designated host.
    // Ordinary verification requires the human-reviewed canonical fixture.
    if let Some(path) = std::env::var_os("RIFFDB_PROJECTION_SOURCE_WIRE_VECTOR_OUTPUT") {
        std::fs::write(path, fixture).unwrap();
        return;
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/proto/durable-projection-source-v1-wire-vectors.txt");
    assert_eq!(fixture, std::fs::read_to_string(path).unwrap());
}

// req: STO-012, STO-020, PRJ-004
#[test]
fn source_allocator_inline_validation_is_independent_of_exhausted_worker_admission() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let held = budget
        .reserve(riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES - before.fixed_owner_bytes)
        .unwrap();
    let exhausted = budget.observation().unwrap();
    for position in [
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(u64::MAX).unwrap(),
        ),
        ProjectionCheckpointOrdinalPositionV1::Exhausted,
    ] {
        let allocator = ProjectionCheckpointOrdinalAllocatorV1::new(u64::MAX, position).unwrap();
        let compact = encode_projection_checkpoint_ordinal_allocator_v1(&allocator).unwrap();
        let legacy = raw_payload(&super::metadata::allocator_to_wire(allocator));
        for encoded in [compact.as_bytes(), legacy.as_slice()] {
            assert!(matches!(
                decode_projection_checkpoint_ordinal_allocator_admitted_v1(encoded, &budget),
                Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
            ));
            let decoded =
                decode_projection_checkpoint_ordinal_allocator_inline_v1(encoded).unwrap();
            assert_eq!(*decoded.value(), allocator);
            assert_eq!(decoded.encoded_content_charge().get(), encoded.len());
            assert_eq!(budget.observation().unwrap(), exhausted);
        }
    }
    for message in [
        wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
            allocation_history_incarnation: 0,
            position: Some(
                wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(1),
            ),
        },
        wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
            allocation_history_incarnation: 2,
            position: None,
        },
        wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
            allocation_history_incarnation: 2,
            position: Some(
                wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(0),
            ),
        },
    ] {
        assert!(
            decode_projection_checkpoint_ordinal_allocator_inline_v1(&raw_payload(&message))
                .is_err()
        );
    }
    assert_eq!(budget.observation().unwrap(), exhausted);
    drop(held);
    assert_eq!(budget.observation().unwrap(), before);
}

#[path = "tests/caller_admission.rs"]
mod caller_admission_tests;

#[path = "tests/control_encode.rs"]
mod control_encode;

#[path = "maintenance_capacity_tests.rs"]
mod maintenance_capacity;
