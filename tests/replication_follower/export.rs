//! Production service export path over TLS, with real authorization and restart.
use super::{support::*, workload};
use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient, v1};
use riffdb_storage_api::{ApplicationExportLedgerRepository, StoredApplicationExportOperation};
use riffdb_types::{ApplicationExportOperationId, ApplicationExportPageHash};

fn next_request() -> Vec<u8> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let mut random = [0; 10];
    random[2..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    riffdb_types::RequestId::from_unix_milliseconds_and_random(9, random)
        .unwrap()
        .into_bytes()
        .to_vec()
}

async fn status(
    client: &mut RiffDbClient,
    operation: &[u8],
    metadata: &CallMetadata,
) -> v1::ApplicationExportOperation {
    let result = client
        .get_application_export(
            v1::GetApplicationExportRequest {
                request_id: next_request(),
                operation_id: operation.to_vec(),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::get_application_export_response::Result::Found(value)) = result.result else {
        panic!("retained export");
    };
    value
}

// req: EXP-002, EXP-006, EXP-007, EXP-008, EXP-009, EXP-010
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_export_service_replays_pages_and_retains_terminal_evidence_after_restart() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (_, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    workload::deploy(&mut client, &admin).await;
    let created = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: next_request(),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(0x21).as_bytes().to_vec(),
                principal_id: "export-operator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec!["replication-process-test".into()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions: vec![v1::CapabilityPermission {
                        permission: Some(v1::capability_permission::Permission::ReadHealth(
                            v1::Unit {},
                        )),
                    }],
                    max_scan_rows: 100,
                    export: Some(v1::CapabilityExportGrant {
                        applications: vec![v1::CapabilityApplicationExportGrant {
                            contract_lineage: "TicketDesk".into(),
                            scope: v1::CapabilityApplicationExportScope::WholeApplication as i32,
                            entities: true,
                            events: true,
                            provenance: true,
                            public_audit: true,
                        }],
                    }),
                    ..Default::default()
                }),
            },
            &admin,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(normal)) = created.result else {
        panic!("normal capability");
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        panic!("created capability");
    };
    let metadata = CallMetadata::authenticated(BearerCredential::new(&created.token).unwrap());
    let operation =
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x31; 10]).unwrap();
    let selection = v1::ApplicationExportSelection {
        contract_lineage: "TicketDesk".into(),
        scope: v1::CapabilityApplicationExportScope::WholeApplication as i32,
        entities: true,
        events: true,
        provenance: true,
        public_audit: true,
    };
    let start = client
        .start_application_export(
            v1::StartApplicationExportRequest {
                request_id: next_request(),
                operation_id: operation.as_bytes().to_vec(),
                selection: Some(selection.clone()),
                lease_seconds: 3600,
                canonical_portability_manifest_json: Vec::new(),
            },
            &metadata,
        )
        .await
        .unwrap();
    assert_eq!(start.cursor[0], 2);
    let mut cursor = start.cursor;
    let mut hashes = Vec::new();
    for ordinal in 1..=64 {
        let request = v1::GetApplicationExportPageRequest {
            request_id: next_request(),
            operation_id: operation.as_bytes().to_vec(),
            cursor: cursor.clone(),
            max_rows: 1,
        };
        let response = client
            .get_application_export_page(request.clone(), &metadata)
            .await
            .unwrap_or_else(|error| panic!("page {ordinal}: {error:?}"));
        let page = response.page.unwrap();
        assert_eq!(page.page_number, ordinal);
        assert!(page.canonical_json_lines.len() <= 1);
        let mut retry = request;
        retry.request_id = next_request();
        assert_eq!(
            client
                .get_application_export_page(retry, &metadata)
                .await
                .unwrap()
                .page
                .as_ref(),
            Some(&page)
        );
        hashes.push(ApplicationExportPageHash::from_bytes(
            page.page_hash.clone().try_into().unwrap(),
        ));
        cursor = page.next_cursor;
        if page.operation_complete {
            break;
        }
    }
    assert!(
        cursor.is_empty(),
        "bounded fixture completed every selected class"
    );
    assert!(hashes.len() >= 4, "empty classes also release bound pages");
    let completed = status(&mut client, operation.as_bytes(), &metadata).await;
    assert_eq!(
        completed.phase,
        v1::ApplicationExportPhase::Completed as i32
    );
    assert_eq!(completed.pages_released as usize, hashes.len());
    let manifest: serde_json::Value =
        serde_json::from_slice(&completed.canonical_manifest_json).unwrap();
    assert_eq!(
        manifest["page_hashes"].as_array().unwrap().len(),
        hashes.len()
    );
    let abandoned =
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x41; 10]).unwrap();
    let start = client
        .start_application_export(
            v1::StartApplicationExportRequest {
                request_id: next_request(),
                operation_id: abandoned.as_bytes().to_vec(),
                selection: Some(selection),
                lease_seconds: 3600,
                canonical_portability_manifest_json: Vec::new(),
            },
            &metadata,
        )
        .await
        .unwrap();
    stop(&mut primary);
    {
        let ports = open_primary(&fixture.database("primary"));
        let head = ports
            .read_application_export_head(operation)
            .unwrap()
            .unwrap();
        let StoredApplicationExportOperation::Compact(head) = head else {
            panic!("new compact operation");
        };
        assert_eq!(
            ports.verify_application_export_ledger(&head).unwrap(),
            hashes
        );
    }
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    assert_eq!(
        status(&mut client, operation.as_bytes(), &metadata).await,
        completed
    );
    assert!(
        client
            .get_application_export_page(
                v1::GetApplicationExportPageRequest {
                    request_id: next_request(),
                    operation_id: abandoned.as_bytes().to_vec(),
                    cursor: start.cursor,
                    max_rows: 1
                },
                &metadata
            )
            .await
            .is_err(),
        "ledger does not recreate a lost snapshot"
    );
    let abandoned = status(&mut client, abandoned.as_bytes(), &metadata).await;
    assert_eq!(
        abandoned.failure,
        v1::ApplicationExportFailure::SnapshotUnavailable as i32
    );
    assert_eq!(abandoned.pages_released, 0);
    assert!(!abandoned.canonical_receipt_json.is_empty());
    stop(&mut primary);
}
