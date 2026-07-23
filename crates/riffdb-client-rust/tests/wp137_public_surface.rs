#![forbid(unsafe_code)]

//! Compile-time fixture for the exact WP-137 Rust-client surface.

use std::path::Path;

use riffdb_client_rust::{
    AttemptBudget, BearerCredential, BearerCredentialFileError, BootstrapCallMetadata,
    BootstrapCapabilityCreateTemplate, CallMetadata, CapabilityCreateTemplateError, ClientError,
    IdempotentCommand, NormalCapabilityCreateTemplate, PublicError, RiffDbClient,
    load_protected_bearer_credential, v1,
};

#[test]
fn six_checked_unary_operations_are_public() {
    let _ = RiffDbClient::get_contract_version;
    let _ = RiffDbClient::discover_command_tools;
    let _ = RiffDbClient::discover_resources;
    let _ = RiffDbClient::get_projection_status;
    let _ = RiffDbClient::trace_provenance;
    let _ = RiffDbClient::list_pending_outbox_deliveries;
    let _ = checked_unary_signatures;
}

#[test]
fn capability_retry_and_protected_credential_types_are_public() {
    let _: fn(
        v1::CreateCapabilityRequest,
    ) -> Result<NormalCapabilityCreateTemplate, CapabilityCreateTemplateError> =
        NormalCapabilityCreateTemplate::new;
    let _: fn(
        v1::CreateCapabilityRequest,
    ) -> Result<BootstrapCapabilityCreateTemplate, CapabilityCreateTemplateError> =
        BootstrapCapabilityCreateTemplate::new;
    let _: fn(&Path) -> Result<BearerCredential, BearerCredentialFileError> =
        load_protected_bearer_credential;
    let _: fn(&BearerCredential, &BearerCredential) -> bool =
        BearerCredential::has_same_presentation;
    let _: Option<PublicError> = None;

    let _ = RiffDbClient::execute_with_retry;
    let _ = RiffDbClient::create_capability_with_retry;
    let _ = RiffDbClient::create_bootstrap_capability_with_retry;
    let _ = retry_signatures;
}

async fn checked_unary_signatures(client: &mut RiffDbClient, metadata: &CallMetadata) {
    let _: Result<v1::GetContractVersionResponse, ClientError> = client
        .get_contract_version(v1::GetContractVersionRequest::default(), metadata)
        .await;
    let _: Result<v1::DiscoverCommandToolsResponse, ClientError> = client
        .discover_command_tools(v1::DiscoverCommandToolsRequest::default(), metadata)
        .await;
    let _: Result<v1::DiscoverResourcesResponse, ClientError> = client
        .discover_resources(v1::DiscoverResourcesRequest::default(), metadata)
        .await;
    let _: Result<v1::GetProjectionStatusResponse, ClientError> = client
        .get_projection_status(v1::GetProjectionStatusRequest::default(), metadata)
        .await;
    let _: Result<v1::TraceProvenanceResponse, ClientError> = client
        .trace_provenance(v1::TraceProvenanceRequest::default(), metadata)
        .await;
    let _: Result<v1::ListPendingOutboxDeliveriesResponse, ClientError> = client
        .list_pending_outbox_deliveries(v1::ListPendingOutboxDeliveriesRequest::default(), metadata)
        .await;
}

async fn retry_signatures(
    client: &mut RiffDbClient,
    command: &IdempotentCommand,
    normal: &NormalCapabilityCreateTemplate,
    bootstrap: &BootstrapCapabilityCreateTemplate,
    attempts: AttemptBudget,
    metadata: &CallMetadata,
    bootstrap_metadata: &BootstrapCallMetadata,
) {
    let _: Result<v1::ExecuteCommandResponse, ClientError> =
        client.execute_with_retry(command, attempts, metadata).await;
    let _: Result<v1::CreateCapabilityResponse, ClientError> = client
        .create_capability_with_retry(normal, attempts, metadata)
        .await;
    let _: Result<v1::CreateCapabilityResponse, ClientError> = client
        .create_bootstrap_capability_with_retry(bootstrap, attempts, bootstrap_metadata)
        .await;
}
