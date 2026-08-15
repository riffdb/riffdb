//! ADR-0029 stage-one key-envelope fixtures.

use riffdb_proto::{v1, validate_public_message};
use riffdb_types::{EntityKey, IndexEntryKey, PartitionKey};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/public-key-envelope-vectors.txt"
));

fn uuid_v7() -> Vec<u8> {
    vec![
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ]
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                .expect("valid hex")
        })
        .collect()
}

fn active() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
    }
}

fn empty_record() -> v1::ValueRecord {
    v1::ValueRecord { fields: Vec::new() }
}

fn commit_with_entity_key(entity_key: Vec<u8>) -> v1::Commit {
    v1::Commit {
        commit_sequence: 1,
        admission_request_id: uuid_v7(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        plan_hash: vec![0x11; 32],
        canonical_input_hash: vec![0x22; 32],
        actor: Some(v1::AdmittedActor {
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            agent_session_id: None,
        }),
        logical_time: Some(v1::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        partition_hash: vec![0x33; 32],
        conflict_hashes: Vec::new(),
        affected_entities: vec![v1::AffectedEntity {
            entity_key,
            entity_version: 1,
        }],
        events: Vec::new(),
        outcome: Some(v1::DeclaredOutcome {
            outcome_id: 1,
            outcome_name: "Reserved".to_owned(),
            value: Some(empty_record()),
        }),
        provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
        durability: v1::CommandDurability::Synchronous as i32,
    }
}

fn entity_result_positions(bytes: &[u8]) -> [bool; 4] {
    let entity = v1::GetEntityResponse {
        result: Some(v1::get_entity_response::Result::Found(v1::Entity {
            entity_key: bytes.to_vec(),
            entity_version: 1,
            written_by_contract_version: 1,
            fields: Some(empty_record()),
        })),
    };
    let commit = commit_with_entity_key(bytes.to_vec());
    let get_commit = v1::GetCommitResponse {
        result: Some(v1::get_commit_response::Result::Found(commit.clone())),

        history_incarnation: 1,
    };
    let scan_commits = v1::ScanCommitsResponse {
        page: Some(v1::CommitPage {
            items: vec![commit.clone()],
            next_cursor: None,
            observed_fence: Some(v1::FrontierPosition {
                position: Some(v1::frontier_position::Position::AppliedThrough(1)),
            }),

            history_incarnation: 1,
        }),
    };
    let notification = v1::CommitNotification {
        notification: Some(v1::commit_notification::Notification::Commit(commit)),

        history_incarnation: 1,
    };
    [
        validate_public_message(&entity).is_ok(),
        validate_public_message(&get_commit).is_ok(),
        validate_public_message(&scan_commits).is_ok(),
        validate_public_message(&notification).is_ok(),
    ]
}

fn contextual_result(kind: &str, bytes: Vec<u8>, owner: u32) -> bool {
    match kind {
        "entity" => validate_public_message(&v1::GetEntityRequest {
            request_id: uuid_v7(),
            contract: Some(active()),
            entity_type_id: owner,
            entity_key: bytes,
            fields: Some(v1::FieldSelection {
                field_ids: Vec::new(),
            }),
        })
        .is_ok(),
        "index" => validate_public_message(&v1::ScanIndexResponse {
            page: Some(v1::IndexPage {
                items: vec![v1::IndexRow {
                    index_entry_key: bytes,
                    values: Some(v1::ValueRecord { fields: Vec::new() }),
                }],
                next_cursor: None,
                observed_fence: Some(v1::IndexScanFence {
                    position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
                }),
            }),
        })
        .is_ok(),
        "partition" => validate_public_message(&v1::CreateCapabilityRequest {
            request_id: uuid_v7(),
            mode: v1::CapabilityCreateMode::Normal as i32,
            capability_id: uuid_v7(),
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            requested_lifetime_seconds: 60,
            audiences: vec!["riffdb-cli".to_owned()],
            grant: Some(v1::CapabilityGrant {
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                }),
                partition_scope: Some(v1::PartitionScope {
                    scope: Some(v1::partition_scope::Scope::Explicit(
                        v1::ExplicitPartitionScope {
                            partitions: vec![v1::ScopedPartition {
                                contract_lineage: "budget".to_owned(),
                                partition_key: bytes,
                            }],
                        },
                    )),
                }),
                permissions: Vec::new(),
                field_visibility: Vec::new(),
                max_scan_rows: 1,
                approval_required: Vec::new(),
                row_policy: None,
                export: None,
                reimport: None,
            }),
        })
        .is_ok(),
        other => panic!("unknown key kind: {other}"),
    }
}

#[test]
fn envelope_and_contextual_expectations_are_distinct_and_frozen() {
    let mut lines = VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-public-key-envelope-vectors-v1"));
    assert!(lines.next().expect("legend").starts_with('#'));
    let mut count = 0;
    for line in lines {
        let mut fields = line.split_whitespace();
        let kind = fields.next().expect("kind");
        let _case = fields.next().expect("case");
        let envelope = fields.next().expect("envelope") == "pass";
        let contextual = fields.next().expect("contextual") == "pass";
        let owner = fields.next().expect("owner").parse().expect("u32 owner");
        let bytes = decode_hex(fields.next().expect("hex bytes"));
        assert!(fields.next().is_none());

        let constructed = match kind {
            "entity" => EntityKey::from_bytes(bytes.clone()).is_ok(),
            "index" => IndexEntryKey::from_bytes(bytes.clone()).is_ok(),
            "partition" => PartitionKey::from_bytes(bytes.clone()).is_ok(),
            other => panic!("unknown key kind: {other}"),
        };
        assert_eq!(constructed, envelope, "{kind} envelope fixture");
        assert_eq!(
            contextual_result(kind, bytes.clone(), owner),
            contextual,
            "{kind} contextual fixture"
        );
        if kind == "entity" {
            for accepted in entity_result_positions(&bytes) {
                assert_eq!(accepted, envelope, "entity result key position fixture");
            }
        }
        count += 1;
    }
    assert_eq!(count, 25);
}
