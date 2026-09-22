//! Real daemon catalogs for the bounded R-06 model-selection experiment.

use super::*;
use serde_json::{Value, json};

const LINEAGE: &str = "WorkflowDesk";
const NOUNS: [&str; 8] = [
    "Ticket", "Invoice", "Shipment", "Request", "Report", "Expense", "Order", "Case",
];
const ACTIONS: [&str; 8] = [
    "Submit", "Approve", "Reject", "Cancel", "Archive", "Reopen", "Escalate", "Defer",
];

fn experiment_source(changed: bool) -> String {
    let mut source = format!(
        "contract {LINEAGE} version {} {{\n",
        if changed { 2 } else { 1 }
    );
    for noun in NOUNS {
        source.push_str(&format!(
            "entity {noun} {{ key (organization_id: uuid, item_id: uuid) field revision: u64 field state: string<32> field reason: string<128> }}\naggregate {noun}Root {{ root {noun} partition_by organization_id conflict_key (organization_id, item_id) }}\n"
        ));
    }
    for noun in NOUNS {
        for action in ACTIONS {
            if changed && noun == "Ticket" && action == "Cancel" {
                continue;
            }
            let name = if changed && noun == "Ticket" && action == "Approve" {
                "Authorize"
            } else {
                action
            };
            let extra = if changed && noun == "Ticket" && action == "Submit" {
                "input review_reference: string<64>"
            } else {
                ""
            };
            source.push_str(&format!(
                "command {name}{noun} {{ input idempotency_key: string<128> input organization_id: uuid input item_id: uuid input expected_revision: u64 input reason: string<128> {extra}\nidempotency_key idempotency_key\nmutate {noun}(organization_id, item_id) as item else NotFound {{ item_id: item_id }}\nrequire revision_matches: item.revision == expected_revision else StaleRevision {{ current: item.revision }}\nset item.revision = item.revision + 1\nset item.state = \"{name}\"\nset item.reason = reason\nreturn Updated {{ revision: item.revision }} }}\n"
            ));
        }
    }
    source.push_str("}\n");
    source
}

fn invocation_permission(stable_id: u32) -> v1::CapabilityPermission {
    v1::CapabilityPermission {
        permission: Some(v1::capability_permission::Permission::InvokeCommand(
            v1::LineageScopedStableId {
                contract_lineage: LINEAGE.to_owned(),
                stable_id,
            },
        )),
    }
}

fn request_json(
    address: SocketAddr,
    token: &str,
    session: &str,
    request: &Value,
) -> TestResult<Value> {
    let response = mcp_post(
        address,
        Some(ALLOWED_ORIGIN),
        Some(token),
        Some(session),
        &serde_json::to_vec(request)?,
    )?;
    assert_eq!(response.status, 200);
    Ok(serde_json::from_str(single_mcp_json_payload(
        response.body_text()?,
    )?)?)
}

// req: MCP-020, MCP-023, MCP-025, MCP-026, MCP-040, MCP-041
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_catalog_risk_snapshots_are_filtered_stable_and_schema_complete() -> TestResult<()> {
    for changed in [false, true] {
        capture_catalog(changed).await?;
    }
    Ok(())
}

async fn capture_catalog(changed: bool) -> TestResult<()> {
    let source = experiment_source(changed);
    let bundle = compile_contract_source(&source)?;
    let expected_count = if changed { 63 } else { 64 };
    assert_eq!(bundle.commands().len(), expected_count);
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");
    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(&bootstrap_path, generated.render_document().expose_secret())?;
    drop(generated);
    let retained = load_bootstrap_credential_file(&bootstrap_path)?;
    let reservation = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let audience = format!("http://{address}{MCP_ROUTE}");
    let mut process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
        address,
    )?;
    let grpc = process.wait_for_ready_address()?;
    process.close_stdin()?;
    let mut client = connect(grpc).await?;
    let bootstrap = bootstrap_request(&retained, &audience)?;
    let created = bounded_rpc(
        "catalog bootstrap",
        client.create_bootstrap_capability(bootstrap, &bootstrap_metadata(&retained)?),
    )
    .await?;
    assert_created_bootstrap(created)?;
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained)?);
    let deployed = bounded_rpc(
        "catalog deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: source.clone(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(active)) = deployed.result else {
        return Err(test_failure("experiment contract was not activated"));
    };
    assert_eq!(active.contract_lineage, LINEAGE);
    for small in [true, false] {
        let names = bundle
            .commands()
            .iter()
            .filter_map(|command| {
                let name = bundle
                    .mcp_command_names()
                    .get(command.command_id())
                    .expect("compiled name");
                (!small || name.tool_name().as_str().ends_with("ticket")).then_some((
                    command.command_id().get(),
                    name.tool_name().as_str().to_owned(),
                ))
            })
            .collect::<Vec<_>>();
        let mut capability = catalog_capability_request(&audience)?;
        capability.principal_id = "catalog-risk-subject".to_owned();
        capability.grant.as_mut().expect("normal grant").permissions = names
            .iter()
            .map(|(id, _)| invocation_permission(*id))
            .collect();
        let created = bounded_rpc(
            "catalog capability",
            client.create_capability(capability, &authenticated),
        )
        .await?;
        let token = normal_capability_token(created)?;
        let initialized = mcp_post(
            address,
            Some(ALLOWED_ORIGIN),
            Some(&token),
            None,
            initialize_request().as_bytes(),
        )?;
        assert_eq!(initialized.status, 200);
        let session = initialized
            .single_header("mcp-session-id")?
            .ok_or_else(|| test_failure("session missing"))?
            .to_owned();
        let notified = mcp_post(
            address,
            Some(ALLOWED_ORIGIN),
            Some(&token),
            Some(&session),
            br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )?;
        assert!((200..300).contains(&notified.status));
        let request = json!({"jsonrpc":"2.0", "id":2, "method":"tools/list"});
        let response = request_json(address, &token, &session, &request)?;
        for _ in 0..2 {
            assert_eq!(
                response,
                request_json(address, &token, &session, &request)?,
                "unchanged discovery must repeat exactly"
            );
        }
        assert!(response["result"].get("nextCursor").is_none());
        let tools = response["result"]["tools"].as_array().expect("tool array");
        let dynamic = tools
            .iter()
            .filter(|tool| {
                tool["name"]
                    .as_str()
                    .expect("name")
                    .starts_with("riffdb_cmd_")
            })
            .collect::<Vec<_>>();
        assert_eq!(dynamic.len(), names.len());
        let mut actual_names = dynamic
            .iter()
            .map(|tool| tool["name"].as_str().expect("name").to_owned())
            .collect::<Vec<_>>();
        let mut expected_names = names
            .iter()
            .map(|(_, name)| name.clone())
            .collect::<Vec<_>>();
        actual_names.sort();
        expected_names.sort();
        assert_eq!(actual_names, expected_names);
        for descriptor in &dynamic {
            assert!(descriptor["inputSchema"].is_object());
            assert!(descriptor["outputSchema"].is_object());
            let (id, _) = names
                .iter()
                .find(|(_, name)| descriptor["name"] == *name)
                .expect("authorized command");
            let command = bundle
                .commands()
                .iter()
                .find(|command| command.command_id().get() == *id)
                .expect("compiled command");
            let input = bundle
                .schema_artifacts()
                .iter()
                .find(|artifact| {
                    artifact.key() == SchemaArtifactKey::CommandInput(command.command_id())
                })
                .expect("compiled input");
            let outcome = bundle
                .schema_artifacts()
                .iter()
                .find(|artifact| {
                    artifact.key() == SchemaArtifactKey::CommandOutcomeUnion(command.command_id())
                })
                .expect("compiled outcome");
            let expected = McpDynamicToolDefinition::from_discovered_command(
                descriptor["name"].as_str().expect("name"),
                SchemaDocument::from_public_generated(
                    McpGeneratedSchemaKind::CommandInput,
                    *id,
                    input.hash().as_bytes(),
                    input.canonical_json(),
                )?,
                SchemaDocument::from_public_generated(
                    McpGeneratedSchemaKind::CommandOutcomeUnion,
                    *id,
                    outcome.hash().as_bytes(),
                    outcome.canonical_json(),
                )?,
            )?
            .descriptor_json()?;
            assert_eq!(
                **descriptor, expected,
                "hosted descriptor exactly matches compiler-owned schemas"
            );
        }
        let refusal = if changed {
            let refused = request_json(
                address,
                &token,
                &session,
                &json!({"jsonrpc":"2.0", "id":3,"method":"tools/call","params":{"name":"riffdb_cmd_workflowdesk_approveticket","arguments":{}}}),
            )?;
            assert_eq!(refused["error"]["code"], -32601);
            Some(refused)
        } else {
            None
        };
        let missing_new_input = if changed {
            let refused = request_json(
                address,
                &token,
                &session,
                &json!({"jsonrpc":"2.0", "id":4,"method":"tools/call","params":{"name":"riffdb_cmd_workflowdesk_submitticket","arguments":{"idempotency_key":"schema-refusal", "organization_id":"11111111-1111-4111-8111-111111111111", "item_id":"22222222-2222-4222-8222-222222222222", "expected_revision":7, "reason":"Ready for review"}}}),
            )?;
            assert_eq!(refused["error"]["code"], -32602);
            assert_eq!(
                refused["error"]["data"]["code"],
                "required_property_missing"
            );
            assert_eq!(refused["error"]["data"]["path"], "/review_reference");
            Some(refused)
        } else {
            None
        };
        let hidden_name_refusal = if small {
            let refused = request_json(
                address,
                &token,
                &session,
                &json!({"jsonrpc":"2.0", "id":5,"method":"tools/call","params":{"name":"riffdb_cmd_workflowdesk_approveinvoice","arguments":{}}}),
            )?;
            assert_eq!(refused["error"]["code"], -32601);
            Some(refused)
        } else {
            None
        };
        if let Some(directory) = std::env::var_os("RIFFDB_CATALOG_RISK_OUTPUT") {
            let directory = std::path::PathBuf::from(directory);
            std::fs::create_dir_all(&directory)?;
            let condition = format!(
                "{}-{}",
                if small { "small" } else { "large" },
                if changed { "changed" } else { "stable" }
            );
            std::fs::write(
                directory.join(format!("{condition}.json")),
                serde_json::to_vec(&response["result"])?,
            )?;
            std::fs::write(
                directory.join(format!("{condition}-metadata.json")),
                serde_json::to_vec_pretty(&json!({
                    "condition": condition, "dynamic_tools": dynamic.len(), "fixed_tools": tools.len()-dynamic.len(), "total_tools": tools.len(),
                    "json_bytes": serde_json::to_vec(&response["result"])?.len(), "pages":1, "equal_observations":3,
                    "capability_visible_names_exact":true, "real_daemon":true, "changed_version_deployed_in_fresh_database":changed,
                    "stale_name_refusal":refusal, "missing_new_input_refusal":missing_new_input, "hidden_name_refusal":hidden_name_refusal
                }))?,
            )?;
            std::fs::write(
                directory.join(format!(
                    "{}.riff",
                    if changed { "changed" } else { "stable" }
                )),
                &source,
            )?;
        }
    }
    drop(client);
    process.signal_sigterm()?;
    process.wait_for_successful_exit(PROCESS_STOP_TIMEOUT)?;
    Ok(())
}
