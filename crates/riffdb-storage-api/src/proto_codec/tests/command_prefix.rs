//! Successor capsule/segment codec proof; storage capture and replay are separate.
// req: REP-007, AFC-007

use super::*;
use crate::{CommandPrefixEvidenceV1, StoredCommandCapsuleV2, StoredCommandSegmentV1};
use riffdb_types::DualFrontier;

fn capsule(sequence: u64) -> StoredCommandCapsuleV2 {
    let atomic = sample::atomic_record_set_at(
        CommitSequence::new(sequence).unwrap(),
        riffdb_types::RequestId::from_bytes(sample::uuid_v7(sequence as u8 + 40)).unwrap(),
        riffdb_types::ProvenanceId::from_bytes(sample::uuid_v7(sequence as u8 + 80)).unwrap(),
    );
    StoredCommandCapsuleV2::from_base(
        sample_command_capsule_v1_from_atomic(
            &atomic,
            AdministrationSequence::new(sequence * 2 - 1).unwrap(),
        ),
        atomic.index_epochs().to_vec(),
    )
    .unwrap()
}

fn evidence(sequence: u64, value: &[u8]) -> CommandPrefixEvidenceV1 {
    CommandPrefixEvidenceV1::new(
        DualFrontier::new(
            CommitSequence::new(sequence - 1),
            AdministrationSequence::new((sequence - 1) * 2),
        ),
        DualFrontier::new(
            CommitSequence::new(sequence),
            AdministrationSequence::new(sequence * 2),
        ),
        // Opaque bytes exercise the codec only. Production must join decoded
        // entity/index records and the original predecessor before restoration.
        vec![
            crate::AuthoritativeMutationV3::put(
                crate::AuthoritativeNamespaceV1::Entities,
                b"key",
                None,
                value,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn draft(
    commands: Vec<StoredCommandCapsuleV2>,
) -> Result<StoredCommandSegmentV1, crate::StorageValueError> {
    let first = commands[0].commit_sequence();
    let manifest = crate::CommandSegmentManifestV1::new(vec![
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::EventRoute,
            crate::CommandDerivedMemberV1::Event,
            vec![0x31],
            0,
            0,
            first,
        )
        .unwrap(),
    ])
    .unwrap();
    StoredCommandSegmentV1::new(
        sample::database_id(),
        1,
        None,
        commands,
        manifest,
        crate::CommandSegmentDigestV1::from_bytes([0; 32]),
    )
}

#[test]
fn successor_capsule_requires_evidence_and_never_downgrades_it() {
    let legacy = capsule(1);
    assert!(legacy.prefix_evidence().is_none());
    let legacy_bytes = encode_command_capsule_v2(&legacy).unwrap();
    let current = legacy
        .clone()
        .with_prefix_evidence(evidence(1, b"intermediate"))
        .unwrap();
    let encoded = assert_round_trip(
        current.clone(),
        encode_command_capsule_v2,
        decode_command_capsule_v2,
    );
    assert_eq!(
        riffdb_proto::durable::readable_record_registry()
            .decode(encoded.as_bytes())
            .unwrap()
            .record_type(),
        "riffdb.storage.v1.StoredCommandCapsuleV7"
    );
    assert_eq!(encode_command_capsule_v2(&legacy).unwrap(), legacy_bytes);
    assert!(
        decode_command_capsule_v2(legacy_bytes.as_bytes())
            .unwrap()
            .value()
            .prefix_evidence()
            .is_none()
    );
    for version in [
        CommandCapsuleWireVersionV1::V4,
        CommandCapsuleWireVersionV1::V5,
        CommandCapsuleWireVersionV1::V6,
    ] {
        assert!(prepare_command_segment_capsule_v1(&current, version).is_err());
    }
    assert!(prepare_command_segment_capsule_v1(&legacy, CommandCapsuleWireVersionV1::V7).is_err());
    assert!(
        legacy
            .with_prefix_evidence(evidence(2, b"wrong-command"))
            .is_err()
    );
}

#[test]
fn successor_segment_covers_evidence_in_both_sealing_paths_and_digest() {
    let command = capsule(1)
        .with_prefix_evidence(evidence(1, b"intermediate"))
        .unwrap();
    let draft = draft(vec![command]).unwrap();
    let prepared = draft
        .commands()
        .iter()
        .map(|c| prepare_command_segment_capsule_v1(c, CommandCapsuleWireVersionV1::V7).unwrap())
        .collect();
    let (sealed, encoded) = seal_and_encode_command_segment_v1(draft.clone()).unwrap();
    let (prepared_sealed, prepared_bytes, _) =
        seal_and_encode_command_segment_with_prepared_capsules_v1(draft, prepared).unwrap();
    assert_eq!(sealed, prepared_sealed);
    assert_eq!(encoded, prepared_bytes);
    assert_eq!(encode_command_segment_v1(&sealed).unwrap(), encoded);
    assert_eq!(
        decode_command_segment_v1(encoded.as_bytes())
            .unwrap()
            .value(),
        &sealed
    );
    assert_eq!(
        riffdb_proto::durable::readable_record_registry()
            .decode(encoded.as_bytes())
            .unwrap()
            .record_type(),
        "riffdb.storage.v1.StoredCommandSegmentV6"
    );
    let altered = capsule(1)
        .with_prefix_evidence(evidence(1, b"different"))
        .unwrap();
    let prepared = vec![
        prepare_command_segment_capsule_v1(&altered, CommandCapsuleWireVersionV1::V7).unwrap(),
    ];
    assert!(seal_and_encode_command_segment_with_prepared_capsules_v1(sealed, prepared).is_err());
}

#[test]
fn successor_segment_refuses_mixed_missing_or_discontinuous_evidence() {
    let first = capsule(1)
        .with_prefix_evidence(evidence(1, b"one"))
        .unwrap();
    let second = capsule(2)
        .with_prefix_evidence(evidence(2, b"two"))
        .unwrap();
    draft(vec![first.clone(), second.clone()]).unwrap();
    assert!(draft(vec![first.clone(), capsule(2)]).is_err());
    assert!(draft(vec![capsule(1), second]).is_err());
    // A terminal-only step can individually join command 2's audit facts, but
    // cannot skip administration sequence 3 between members of this segment.
    let gap = CommandPrefixEvidenceV1::new(
        DualFrontier::new(CommitSequence::new(1), AdministrationSequence::new(3)),
        DualFrontier::new(CommitSequence::new(2), AdministrationSequence::new(4)),
        Vec::new(),
    )
    .unwrap();
    let second = capsule(2).with_prefix_evidence(gap).unwrap();
    assert!(draft(vec![first, second]).is_err());
}

#[test]
fn valid_outer_envelopes_cannot_hide_missing_or_wrong_command_prefixes() {
    use prost::Message;
    let capsule = capsule(1)
        .with_prefix_evidence(evidence(1, b"value"))
        .unwrap();
    let encoded = encode_command_capsule_v2(&capsule).unwrap();
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    let wire =
        riffdb_proto::storage::v1::StoredCommandCapsuleV7::decode(decoded.payload()).unwrap();
    for payload in [
        Vec::new(),
        vec![0; 40],
        evidence(2, b"foreign").encode_capsule_payload().unwrap(),
    ] {
        let altered = riffdb_proto::storage::v1::StoredCommandCapsuleV7 {
            command_prefix_evidence: payload,
            ..wire.clone()
        };
        let envelope =
            encode_message("riffdb.storage.v1.StoredCommandCapsuleV7", &altered).unwrap();
        assert!(decode_command_capsule_v2(envelope.as_bytes()).is_err());
    }
}

#[test]
fn successor_evidence_and_complete_graph_share_the_original_envelope_ceiling() {
    let oversized = capsule(1)
        .with_prefix_evidence(evidence(
            1,
            &vec![0x5a; crate::MAX_STAGED_WRITE_BYTES - 40 - 44 - 3],
        ))
        .unwrap();
    assert!(encode_command_capsule_v2(&oversized).is_err());
    assert!(seal_and_encode_command_segment_v1(draft(vec![oversized]).unwrap()).is_err());
    let half = vec![0x5a; crate::MAX_STAGED_WRITE_BYTES / 2];
    let first = capsule(1).with_prefix_evidence(evidence(1, &half)).unwrap();
    let second = capsule(2).with_prefix_evidence(evidence(2, &half)).unwrap();
    assert!(draft(vec![first, second]).is_err());
}

#[test]
fn command_prefix_successor_wire_vectors_are_exact() {
    let capsule = capsule(1)
        .with_prefix_evidence(evidence(1, b"intermediate"))
        .unwrap();
    let capsule_bytes = encode_command_capsule_v2(&capsule).unwrap();
    let (_, segment_bytes) =
        seal_and_encode_command_segment_v1(draft(vec![capsule]).unwrap()).unwrap();
    let mut output = String::from("riffdb-command-prefix-authority-v7-wire-vectors\nrecords\t2\n");
    for (name, bytes) in [
        ("riffdb.storage.v1.StoredCommandCapsuleV7", capsule_bytes),
        ("riffdb.storage.v1.StoredCommandSegmentV6", segment_bytes),
    ] {
        use std::fmt::Write as _;
        write!(output, "{name}\t").unwrap();
        for byte in bytes.as_bytes() {
            write!(output, "{byte:02x}").unwrap();
        }
        output.push('\n');
    }
    if let Some(path) = std::env::var_os("RIFFDB_COMMAND_PREFIX_V7_VECTOR_OUTPUT") {
        std::fs::write(path, output).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt");
        assert_eq!(std::fs::read_to_string(path).unwrap(), output);
    }
}
