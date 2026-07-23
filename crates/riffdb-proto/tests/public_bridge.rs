//! Focused semantic and compatibility coverage for the WP-137 public bridge.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use riffdb_proto::{
    ExecuteWireError, PublicWireError, decode_public_message, v1,
    validate_discover_command_tools_exchange, validate_discover_resources_exchange,
    validate_get_contract_version_exchange, validate_get_outcome_exchange,
    validate_list_pending_outbox_deliveries_exchange, validate_outcome_resource_locator,
    validate_public_message, validate_trace_provenance_exchange,
};
use riffdb_types::hash_schema;

const FULL_CATALOG: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/operation-schema-catalog-full-v1.bin"
));
const IDENTITY_CATALOG: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/operation-schema-catalog-identity-v1.bin"
));

fn request_id() -> Vec<u8> {
    vec![
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ]
}

fn full_catalog() -> v1::OperationSchemaCatalog {
    v1::OperationSchemaCatalog::decode(FULL_CATALOG).expect("accepted full catalog")
}

fn schema_identity() -> v1::OperationSchemaCatalogIdentity {
    v1::OperationSchemaCatalogIdentity::decode(IDENTITY_CATALOG).expect("accepted identity catalog")
}

fn fence(active: bool, generation: u8) -> v1::DiscoveryCatalogFence {
    v1::DiscoveryCatalogFence {
        state: Some(if active {
            v1::discovery_catalog_fence::State::ActiveContract(v1::ActiveDiscoveryCatalogFence {
                contract_lineage: "budget".to_owned(),
                contract_version: 1,
                bundle_hash: vec![0x44; 32],
            })
        } else {
            v1::discovery_catalog_fence::State::NoActiveContract(v1::Unit {})
        }),
        server_generation: vec![generation; 16],
        operation_schemas: Some(schema_identity()),
    }
}

fn page() -> v1::PageRequest {
    v1::PageRequest {
        limit: Some(50),
        cursor: None,
    }
}

fn schema_artifact(artifact: v1::schema_artifact_key::Artifact) -> v1::GeneratedSchemaArtifact {
    let canonical_json = "{}".to_owned();
    v1::GeneratedSchemaArtifact {
        key: Some(v1::SchemaArtifactKey {
            artifact: Some(artifact),
        }),
        dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
        schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
        canonical_json,
    }
}

fn resize_schema_artifact(schema: &mut v1::GeneratedSchemaArtifact, length: usize) {
    assert!(length >= 8);
    schema.canonical_json = format!("{{\"x\":\"{}\"}}", "a".repeat(length - 8));
    assert_eq!(schema.canonical_json.len(), length);
    schema.schema_hash = hash_schema(schema.canonical_json.as_bytes())
        .as_bytes()
        .to_vec();
}

fn generated_schema_identity(
    artifact: v1::schema_artifact_key::Artifact,
) -> v1::GeneratedSchemaIdentity {
    let artifact = schema_artifact(artifact);
    v1::GeneratedSchemaIdentity {
        key: artifact.key,
        schema_hash: artifact.schema_hash,
    }
}

fn command_tool_descriptor(
    lineage: &str,
    source_command: &str,
    tool_name: &str,
) -> v1::CommandToolDescriptor {
    v1::CommandToolDescriptor {
        tool_name: tool_name.to_owned(),
        source_command: source_command.to_owned(),
        contract_lineage: lineage.to_owned(),
        contract_version: 1,
        command_id: 1,
        input_schema: Some(schema_artifact(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
        )),
        outcome_schema: Some(schema_artifact(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
        )),
    }
}

fn compact_command_tool_descriptor(
    lineage: &str,
    source_command: &str,
    tool_name: &str,
) -> v1::CompactCommandToolDescriptor {
    v1::CompactCommandToolDescriptor {
        tool_name: tool_name.to_owned(),
        source_command: source_command.to_owned(),
        contract_lineage: lineage.to_owned(),
        contract_version: 1,
        command_id: 1,
        input_schema: Some(generated_schema_identity(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
        )),
        outcome_schema: Some(generated_schema_identity(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
        )),
    }
}

fn full_command_discovery(
    descriptor: v1::CommandToolDescriptor,
) -> v1::DiscoverCommandToolsResponse {
    v1::DiscoverCommandToolsResponse {
        result: Some(v1::discover_command_tools_response::Result::Page(
            v1::CommandToolDiscoveryPage {
                items: vec![v1::CommandToolDiscoveryItem {
                    item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                        descriptor,
                    )),
                }],
                next_cursor: None,
                observed_fence: Some(fence(true, 0x31)),
                operation_schemas: Some(full_catalog()),
            },
        )),
    }
}

fn compact_command_discovery(
    descriptor: v1::CompactCommandToolDescriptor,
) -> v1::DiscoverCommandToolsResponse {
    v1::DiscoverCommandToolsResponse {
        result: Some(v1::discover_command_tools_response::Result::CompactPage(
            v1::CompactCommandToolDiscoveryPage {
                items: vec![v1::CompactCommandToolDiscoveryItem {
                    item: Some(v1::compact_command_tool_discovery_item::Item::CommandTool(
                        descriptor,
                    )),
                }],
                next_cursor: None,
                observed_fence: Some(fence(true, 0x32)),
            },
        )),
    }
}

fn outcome_uri(tuple: &[u8]) -> String {
    format!(
        "riffdb://outcome/principal/budget/1/riffdb.cmd.budget.reserve/{}",
        URL_SAFE_NO_PAD.encode(tuple)
    )
}

fn digest_tuple(scheme: u8, key_id: u32) -> Vec<u8> {
    let mut tuple = vec![scheme];
    tuple.extend_from_slice(&key_id.to_be_bytes());
    tuple.extend_from_slice(&[0x55; 32]);
    tuple
}

fn append_length_delimited(output: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    output.push(tag);
    let mut length = bytes.len();
    while length >= 0x80 {
        output.push(u8::try_from(length & 0x7f).expect("seven-bit chunk") | 0x80);
        length >>= 7;
    }
    output.push(u8::try_from(length).expect("terminal seven-bit chunk"));
    output.extend_from_slice(bytes);
}

fn execute(status: v1::execute_command_response::CompletionStatus) -> v1::ExecuteCommandResponse {
    let read_only = status == v1::execute_command_response::CompletionStatus::ExecutedReadOnly;
    v1::ExecuteCommandResponse {
        status: status as i32,
        commit_sequence: if read_only { 0 } else { 1 },
        contract_version: 1,
        plan_hash: vec![0x66; 32],
        outcome_type: "Reserved".to_owned(),
        outcome: Some(v1::Value {
            kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
        }),
        provenance_uri: if read_only {
            String::new()
        } else {
            "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned()
        },
        durability_mode: if read_only {
            String::new()
        } else {
            "sync".to_owned()
        },
        outcome_uri: (!read_only).then(|| outcome_uri(&digest_tuple(1, 1))),
    }
}

#[test]
fn operation_catalog_checkpoint_is_encoded_by_the_public_types() {
    let full = full_catalog();
    let identity = schema_identity();
    assert_eq!(full.encode_to_vec(), FULL_CATALOG);
    assert_eq!(identity.encode_to_vec(), IDENTITY_CATALOG);
    assert_eq!(FULL_CATALOG.len(), 7_522);
    assert_eq!(IDENTITY_CATALOG.len(), 148);
}

#[test]
fn outcome_locator_parser_is_canonical_and_closed() {
    let valid = outcome_uri(&digest_tuple(1, 1));
    validate_outcome_resource_locator(&valid).expect("canonical locator");
    let encoded_utf8 = valid.replacen("principal", "pr%C3%ADncipal", 1);
    validate_outcome_resource_locator(&encoded_utf8).expect("canonical UTF-8 bytes");
    let mixed_case_lineage = valid.replacen("/budget/", "/Budget/", 1);
    validate_outcome_resource_locator(&mixed_case_lineage)
        .expect("ASCII-lower lineage matches the tool contract segment");
    let historical_command_name = valid.replacen(
        "riffdb.cmd.budget.reserve",
        "riffdb.cmd.budget.retired_name",
        1,
    );
    validate_outcome_resource_locator(&historical_command_name)
        .expect("the proto boundary does not resolve historical command names");

    for invalid in [
        valid.replacen("principal", "pr%69ncipal", 1),
        valid.replacen("principal", "pr%c3%adncipal", 1),
        valid.replacen("/1/riffdb", "/01/riffdb", 1),
        valid.replacen("riffdb.cmd.budget.reserve", "riffdb.cmd.Budget.reserve", 1),
        valid.replacen("riffdb.cmd.budget.reserve", "riffdb.cmd.ledger.reserve", 1),
        valid.replacen("/budget/", "/budg%C3%A9t/", 1),
        format!("{valid}?query=1"),
        outcome_uri(&digest_tuple(2, 1)),
        outcome_uri(&digest_tuple(1, 0)),
    ] {
        assert_eq!(
            validate_outcome_resource_locator(&invalid),
            Err(ExecuteWireError::InvalidOutcomeUri),
            "{invalid}"
        );
    }
}

#[test]
fn locator_presence_is_status_consistent_and_legacy_compatible() {
    let mut committed = execute(v1::execute_command_response::CompletionStatus::Committed);
    validate_public_message(&committed).expect("new durable result");
    committed.outcome_uri = None;
    validate_public_message(&committed).expect("legacy durable result");

    let mut read_only = execute(v1::execute_command_response::CompletionStatus::ExecutedReadOnly);
    read_only.outcome_uri = Some(outcome_uri(&digest_tuple(1, 1)));
    assert_eq!(
        validate_public_message(&read_only),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn get_outcome_selector_is_exclusive_and_locator_result_echoes() {
    let locator = outcome_uri(&digest_tuple(1, 1));
    let request = v1::GetOutcomeRequest {
        request_id: request_id(),
        contract_lineage: String::new(),
        command_name: String::new(),
        idempotency_key: String::new(),
        outcome_uri: Some(locator.clone()),
    };
    let response = v1::GetOutcomeResponse {
        result: Some(v1::get_outcome_response::Result::Found(execute(
            v1::execute_command_response::CompletionStatus::Replayed,
        ))),
    };
    validate_get_outcome_exchange(&request, &response).expect("equal locator");

    let mut mixed = request.clone();
    mixed.command_name = "reserve".to_owned();
    assert_eq!(
        validate_public_message(&mixed),
        Err(PublicWireError::InconsistentFields)
    );

    let mut changed = response;
    let Some(v1::get_outcome_response::Result::Found(found)) = changed.result.as_mut() else {
        unreachable!()
    };
    found.outcome_uri = Some(outcome_uri(&digest_tuple(1, 2)));
    assert_eq!(
        validate_get_outcome_exchange(&request, &changed),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn full_command_discovery_binds_the_compiler_owned_tool_name() {
    let valid = full_command_discovery(command_tool_descriptor(
        "Budget",
        "ReserveFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    validate_public_message(&valid).expect("exact ASCII-lower derivation");

    let wrong_contract = full_command_discovery(command_tool_descriptor(
        "Ledger",
        "ReserveFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    assert_eq!(
        validate_public_message(&wrong_contract),
        Err(PublicWireError::InconsistentFields)
    );

    let wrong_command = full_command_discovery(command_tool_descriptor(
        "Budget",
        "ReleaseFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    assert_eq!(
        validate_public_message(&wrong_command),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn compact_command_discovery_binds_the_compiler_owned_tool_name() {
    let valid = compact_command_discovery(compact_command_tool_descriptor(
        "Budget",
        "ReserveFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    validate_public_message(&valid).expect("exact ASCII-lower derivation");

    let wrong_contract = compact_command_discovery(compact_command_tool_descriptor(
        "Ledger",
        "ReserveFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    assert_eq!(
        validate_public_message(&wrong_contract),
        Err(PublicWireError::InconsistentFields)
    );

    let wrong_command = compact_command_discovery(compact_command_tool_descriptor(
        "Budget",
        "ReleaseFunds",
        "riffdb.cmd.budget.reservefunds",
    ));
    assert_eq!(
        validate_public_message(&wrong_command),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn conditional_command_discovery_binds_representation_and_fence() {
    let observed = fence(true, 0x33);
    let full_request = v1::DiscoverCommandToolsRequest {
        request_id: request_id(),
        page: Some(page()),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
    };
    let full_response = v1::DiscoverCommandToolsResponse {
        result: Some(v1::discover_command_tools_response::Result::Page(
            v1::CommandToolDiscoveryPage {
                items: Vec::new(),
                next_cursor: None,
                observed_fence: Some(observed.clone()),
                operation_schemas: Some(full_catalog()),
            },
        )),
    };
    validate_discover_command_tools_exchange(&full_request, &full_response)
        .expect("full first page");

    let compact_request = v1::DiscoverCommandToolsRequest {
        request_id: request_id(),
        page: Some(page()),
        prior_fence: Some(observed.clone()),
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
    };
    let unchanged = v1::DiscoverCommandToolsResponse {
        result: Some(
            v1::discover_command_tools_response::Result::CatalogUnchanged(observed.clone()),
        ),
    };
    validate_discover_command_tools_exchange(&compact_request, &unchanged)
        .expect("equal conditional fence");

    let mut wrong = unchanged;
    let Some(v1::discover_command_tools_response::Result::CatalogUnchanged(fence)) =
        wrong.result.as_mut()
    else {
        unreachable!()
    };
    fence.server_generation[0] ^= 1;
    assert_eq!(
        validate_discover_command_tools_exchange(&compact_request, &wrong),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn oversized_full_discovery_uses_one_dedicated_error_class() {
    let mut commands = Vec::new();
    for (command_id, source_command) in [(1, "ReserveA"), (2, "ReserveB")] {
        let mut descriptor = command_tool_descriptor(
            "Budget",
            source_command,
            &format!("riffdb.cmd.budget.{}", source_command.to_ascii_lowercase()),
        );
        descriptor.command_id = command_id;
        let input = descriptor.input_schema.as_mut().expect("input schema");
        input.key = Some(v1::SchemaArtifactKey {
            artifact: Some(v1::schema_artifact_key::Artifact::CommandInputId(
                command_id,
            )),
        });
        resize_schema_artifact(input, 700_000);
        let outcome = descriptor.outcome_schema.as_mut().expect("outcome schema");
        outcome.key = Some(v1::SchemaArtifactKey {
            artifact: Some(v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(
                command_id,
            )),
        });
        resize_schema_artifact(outcome, 700_000);
        commands.push(v1::CommandToolDiscoveryItem {
            item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                descriptor,
            )),
        });
    }
    let command_response = v1::DiscoverCommandToolsResponse {
        result: Some(v1::discover_command_tools_response::Result::Page(
            v1::CommandToolDiscoveryPage {
                items: commands,
                next_cursor: None,
                observed_fence: Some(fence(true, 0x71)),
                operation_schemas: Some(full_catalog()),
            },
        )),
    };
    assert!(command_response.encoded_len() < 4_194_304);
    assert_eq!(
        validate_public_message(&command_response),
        Err(PublicWireError::MessageTooLarge)
    );

    let resources = (1..=3)
        .map(|entity_type_id| {
            let mut schema =
                schema_artifact(v1::schema_artifact_key::Artifact::EntityId(entity_type_id));
            resize_schema_artifact(&mut schema, 900_000);
            v1::ResourceDescriptor {
                resource: Some(v1::resource_descriptor::Resource::EntitySchema(
                    v1::EntitySchemaResource {
                        contract_lineage: "budget".to_owned(),
                        entity_type_id,
                        schema: Some(schema),
                    },
                )),
            }
        })
        .collect();
    let resource_response = v1::DiscoverResourcesResponse {
        result: Some(v1::discover_resources_response::Result::Page(
            v1::ResourceDiscoveryPage {
                items: resources,
                next_cursor: None,
                observed_fence: Some(fence(true, 0x72)),
            },
        )),
    };
    assert!(resource_response.encoded_len() < 4_194_304);
    assert_eq!(
        validate_public_message(&resource_response),
        Err(PublicWireError::MessageTooLarge)
    );
}

#[test]
fn resource_kind_filter_is_checked_before_public_release() {
    let observed = fence(true, 0x22);
    let request = |kind| v1::DiscoverResourcesRequest {
        request_id: request_id(),
        page: Some(page()),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
        kind: kind as i32,
    };
    let response = v1::DiscoverResourcesResponse {
        result: Some(v1::discover_resources_response::Result::Page(
            v1::ResourceDiscoveryPage {
                items: vec![v1::ResourceDescriptor {
                    resource: Some(v1::resource_descriptor::Resource::CommandOutcome(
                        v1::CommandOutcomeResource {
                            contract_lineage: "budget".to_owned(),
                            command_id: 1,
                            tool_name: "riffdb.cmd.budget.reserve".to_owned(),
                        },
                    )),
                }],
                next_cursor: None,
                observed_fence: Some(observed),
            },
        )),
    };
    validate_discover_resources_exchange(&request(v1::ResourceDiscoveryKind::Template), &response)
        .expect("template member");
    assert_eq!(
        validate_discover_resources_exchange(
            &request(v1::ResourceDiscoveryKind::Concrete),
            &response
        ),
        Err(PublicWireError::InconsistentFields)
    );

    let mut wrong_contract = response;
    let Some(v1::discover_resources_response::Result::Page(page)) = wrong_contract.result.as_mut()
    else {
        unreachable!()
    };
    let Some(v1::resource_descriptor::Resource::CommandOutcome(resource)) =
        page.items[0].resource.as_mut()
    else {
        unreachable!()
    };
    resource.tool_name = "riffdb.cmd.ledger.reserve".to_owned();
    assert_eq!(
        validate_discover_resources_exchange(
            &request(v1::ResourceDiscoveryKind::Template),
            &wrong_contract,
        ),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn exact_contract_and_outbox_exchange_relations_are_closed() {
    let request = v1::GetContractVersionRequest {
        request_id: request_id(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
    };
    let mut descriptor = v1::ContractDescriptor {
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        bundle_hash: vec![1; 32],
        source_hash: vec![2; 32],
        plan_root_hash: vec![3; 32],
    };
    let response = |descriptor| v1::GetContractVersionResponse {
        result: Some(v1::get_contract_version_response::Result::Found(descriptor)),
    };
    validate_get_contract_version_exchange(&request, &response(descriptor.clone()))
        .expect("exact identity");
    descriptor.contract_version = 2;
    assert_eq!(
        validate_get_contract_version_exchange(&request, &response(descriptor)),
        Err(PublicWireError::InconsistentFields)
    );

    let page_request = v1::ListPendingOutboxDeliveriesRequest {
        request_id: request_id(),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: None,
        }),
    };
    let page_response = v1::ListPendingOutboxDeliveriesResponse {
        page: Some(v1::OutboxDeliveryPage {
            items: vec![
                v1::OutboxDeliverySummary {
                    event_id: Some(v1::EventId {
                        commit_sequence: 1,
                        event_ordinal: 0,
                    }),
                    state: v1::OutboxDeliveryState::Pending as i32,
                    attempts: 0,
                    next_attempt_at: None,
                },
                v1::OutboxDeliverySummary {
                    event_id: Some(v1::EventId {
                        commit_sequence: 1,
                        event_ordinal: 1,
                    }),
                    state: v1::OutboxDeliveryState::Pending as i32,
                    attempts: 0,
                    next_attempt_at: None,
                },
            ],
            next_cursor: None,
        }),
    };
    assert_eq!(
        validate_list_pending_outbox_deliveries_exchange(&page_request, &page_response),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn nested_duplicate_fence_fields_reject_before_prost_merge() {
    let fence = fence(false, 0x11).encode_to_vec();
    let mut request = v1::DiscoverCommandToolsRequest {
        request_id: request_id(),
        page: Some(page()),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
    }
    .encode_to_vec();
    append_length_delimited(&mut request, 0x1a, &fence);
    append_length_delimited(&mut request, 0x1a, &fence);
    assert_eq!(
        decode_public_message::<v1::DiscoverCommandToolsRequest>(&request),
        Err(PublicWireError::MalformedEncoding)
    );
}

#[test]
fn trace_selector_must_match_the_found_provenance() {
    let request = v1::TraceProvenanceRequest {
        request_id: request_id(),
        selector: Some(v1::ProvenanceSelection {
            selection: Some(v1::provenance_selection::Selection::CommitSequence(2)),
        }),
    };
    let fixture_line = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/proto/public-client-vectors.txt"
    ))
    .lines()
    .find(|line| line.starts_with("CommitService.TraceProvenance response found "))
    .expect("provenance fixture");
    let hex = fixture_line
        .split_whitespace()
        .last()
        .expect("fixture bytes");
    let bytes = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII"), 16).expect("hex"))
        .collect::<Vec<_>>();
    let response = decode_public_message::<v1::TraceProvenanceResponse>(&bytes)
        .expect("valid provenance response");
    assert_eq!(
        validate_trace_provenance_exchange(&request, &response),
        Err(PublicWireError::InconsistentFields)
    );
}
