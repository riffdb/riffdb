use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use riffdb_client_rust::{
    ApplicationInstallationCampaignId, AttemptBudget, CallMetadata, ClientError, RiffDbClient,
    StartApplicationReimport, v1,
};
use tokio::sync::Mutex;

use crate::{
    DRIVER_IDENTITY, OPERATOR_DRIVER_PROTOCOL_VERSION, OperatorDriverRequest,
    OperatorDriverResponse, OperatorReimportOperation,
};

/// Rust-owned operator transport authority for one exact reimport campaign.
pub struct OperatorDriverHost {
    database: String,
    campaign_id: ApplicationInstallationCampaignId,
    campaign_text: String,
    manifest_hash: String,
    start: StartApplicationReimport,
    client: Mutex<RiffDbClient>,
    metadata: CallMetadata,
}

impl OperatorDriverHost {
    /// Binds one protected client and credential to one immutable campaign.
    pub fn new(
        database: String,
        start: StartApplicationReimport,
        client: RiffDbClient,
        metadata: CallMetadata,
    ) -> Self {
        let campaign_id = start.campaign_id();
        Self {
            database,
            campaign_text: campaign_id.to_string(),
            campaign_id,
            manifest_hash: hex(start.portability_manifest().identity().as_bytes()),
            start,
            client: Mutex::new(client),
            metadata,
        }
    }

    /// Checks the exact local binding before any operator request is accepted.
    #[must_use]
    pub fn handshake(&self, request: &OperatorDriverRequest) -> OperatorDriverResponse {
        let OperatorDriverRequest::Handshake {
            request_id,
            protocol_version,
            database,
            campaign_id,
            portability_manifest_hash,
        } = request
        else {
            return error(None, "RDB-OPERATOR-0001", "handshake_required", false);
        };
        if *protocol_version != OPERATOR_DRIVER_PROTOCOL_VERSION
            || database != &self.database
            || campaign_id != &self.campaign_text
            || portability_manifest_hash != &self.manifest_hash
        {
            return error(
                Some(request_id.clone()),
                "RDB-OPERATOR-0002",
                "operator_identity_mismatch",
                false,
            );
        }
        OperatorDriverResponse::Handshake {
            request_id: request_id.clone(),
            protocol_version: OPERATOR_DRIVER_PROTOCOL_VERSION,
            driver_identity: DRIVER_IDENTITY.to_owned(),
            database: self.database.clone(),
            campaign_id: self.campaign_text.clone(),
            portability_manifest_hash: self.manifest_hash.clone(),
        }
    }

    /// Executes one already structurally checked operator request.
    pub async fn invoke(&self, request: OperatorDriverRequest) -> OperatorDriverResponse {
        match request {
            OperatorDriverRequest::Start {
                request_id,
                canonical_export_manifest_json,
                canonical_export_receipt_json,
                maximum_attempts,
            } => {
                // Source terminal documents are immutable parts of the start identity.
                // A configured host therefore refuses substitution rather than rebuilding
                // a new StartApplicationReimport from local request bytes.
                let expected = self.start.clone();
                let Ok(submitted) = StartApplicationReimport::new(
                    expected.campaign_id(),
                    expected.lineage().clone(),
                    expected.scope(),
                    expected.portability_manifest().clone(),
                    canonical_export_manifest_json.into_bytes(),
                    canonical_export_receipt_json.into_bytes(),
                ) else {
                    return error(
                        Some(request_id),
                        "RDB-OPERATOR-0003",
                        "invalid_source",
                        false,
                    );
                };
                if submitted != expected {
                    return error(
                        Some(request_id),
                        "RDB-OPERATOR-0002",
                        "source_identity_mismatch",
                        false,
                    );
                }
                let budget = AttemptBudget::new(maximum_attempts).expect("protocol bound");
                let result = self
                    .client
                    .lock()
                    .await
                    .start_application_reimport_with_retry(&self.start, budget, &self.metadata)
                    .await;
                response(request_id, result.map(|value| value.operation))
            }
            OperatorDriverRequest::ApplyPage {
                request_id,
                export_operation_id,
                page_number,
                canonical_json_lines,
                next_cursor_base64,
                class_complete,
                operation_complete,
                page_hash_hex,
                maximum_attempts,
            } => {
                let Some(operation_id) = uuid_bytes(&export_operation_id) else {
                    return error(Some(request_id), "RDB-OPERATOR-0003", "invalid_page", false);
                };
                let Some(page_hash) = hex_bytes(&page_hash_hex) else {
                    return error(Some(request_id), "RDB-OPERATOR-0003", "invalid_page", false);
                };
                let next_cursor = match next_cursor_base64 {
                    Some(value) => match BASE64.decode(value) {
                        Ok(value) => value,
                        Err(_) => {
                            return error(
                                Some(request_id),
                                "RDB-OPERATOR-0003",
                                "invalid_page",
                                false,
                            );
                        }
                    },
                    None => Vec::new(),
                };
                let page = v1::ApplicationExportPage {
                    operation_id: operation_id.to_vec(),
                    page_number,
                    record_class: v1::ApplicationExportRecordClass::Entity as i32,
                    canonical_json_lines: canonical_json_lines
                        .into_iter()
                        .map(String::into_bytes)
                        .collect(),
                    next_cursor,
                    class_complete,
                    operation_complete,
                    page_hash: page_hash.to_vec(),
                };
                let budget = AttemptBudget::new(maximum_attempts).expect("protocol bound");
                let result = self
                    .client
                    .lock()
                    .await
                    .apply_application_reimport_page_with_retry(
                        self.campaign_id,
                        &page,
                        budget,
                        &self.metadata,
                    )
                    .await;
                response(request_id, result.map(|value| value.operation))
            }
            OperatorDriverRequest::Status { request_id } => {
                match self
                    .client
                    .lock()
                    .await
                    .get_application_reimport_operation(self.campaign_id, &self.metadata)
                    .await
                {
                    Ok(value) => match value.result {
                        Some(v1::get_application_reimport_response::Result::Found(value)) => {
                            operation_response(request_id, value)
                        }
                        Some(v1::get_application_reimport_response::Result::NotFound(_)) => {
                            OperatorDriverResponse::NotFound { request_id }
                        }
                        None => error(
                            Some(request_id),
                            "RDB-OPERATOR-0004",
                            "invalid_response",
                            false,
                        ),
                    },
                    Err(value) => client_error(Some(request_id), value),
                }
            }
            OperatorDriverRequest::Cancel {
                request_id,
                maximum_attempts,
            } => {
                let budget = AttemptBudget::new(maximum_attempts).expect("protocol bound");
                match self
                    .client
                    .lock()
                    .await
                    .cancel_application_reimport_with_retry(
                        self.campaign_id,
                        budget,
                        &self.metadata,
                    )
                    .await
                {
                    Ok(value) => match value.result {
                        Some(v1::cancel_application_reimport_response::Result::Found(value)) => {
                            operation_response(request_id, value)
                        }
                        Some(v1::cancel_application_reimport_response::Result::NotFound(_)) => {
                            OperatorDriverResponse::NotFound { request_id }
                        }
                        None => error(
                            Some(request_id),
                            "RDB-OPERATOR-0004",
                            "invalid_response",
                            false,
                        ),
                    },
                    Err(value) => client_error(Some(request_id), value),
                }
            }
            OperatorDriverRequest::Handshake { request_id, .. } => error(
                Some(request_id),
                "RDB-OPERATOR-0001",
                "handshake_already_completed",
                false,
            ),
        }
    }
}

fn response(
    request_id: String,
    result: Result<Option<v1::ApplicationReimportOperation>, ClientError>,
) -> OperatorDriverResponse {
    match result {
        Ok(Some(value)) => operation_response(request_id, value),
        Ok(None) => error(
            Some(request_id),
            "RDB-OPERATOR-0004",
            "invalid_response",
            false,
        ),
        Err(value) => client_error(Some(request_id), value),
    }
}

fn operation_response(
    request_id: String,
    value: v1::ApplicationReimportOperation,
) -> OperatorDriverResponse {
    match operation(value) {
        Some(operation) => OperatorDriverResponse::Operation {
            request_id,
            operation: Box::new(operation),
        },
        None => error(
            Some(request_id),
            "RDB-OPERATOR-0004",
            "invalid_response",
            false,
        ),
    }
}

fn operation(value: v1::ApplicationReimportOperation) -> Option<OperatorReimportOperation> {
    let phase = v1::ApplicationReimportPhase::try_from(value.phase).ok()?;
    let failure = v1::ApplicationReimportFailure::try_from(value.failure).ok()?;
    Some(OperatorReimportOperation {
        campaign_id: format_uuid(&value.campaign_id)?,
        contract_lineage: value.contract_lineage,
        scope: match v1::CapabilityApplicationReimportScope::try_from(value.scope).ok()? {
            v1::CapabilityApplicationReimportScope::PrincipalFiltered => "principal_filtered",
            v1::CapabilityApplicationReimportScope::WholeApplication => "whole_application",
            _ => return None,
        }
        .to_owned(),
        portability_manifest_hash: hex(&value.portability_manifest_hash),
        export_manifest_hash: hex(&value.export_manifest_hash),
        export_receipt_hash: hex(&value.export_receipt_hash),
        source_database_id: format_uuid(&value.source_database_id)?,
        target_database_id: format_uuid(&value.target_database_id)?,
        source_rows: value.source_rows.to_string(),
        source_pages: value.source_pages.to_string(),
        next_page: value.next_page.to_string(),
        rows_applied: value.rows_applied.to_string(),
        phase: format!("{phase:?}").to_ascii_lowercase(),
        failure: (failure != v1::ApplicationReimportFailure::Unspecified)
            .then(|| format!("{failure:?}").to_ascii_lowercase()),
        canonical_reimport_receipt_json: (!value.canonical_reimport_receipt_json.is_empty())
            .then(|| String::from_utf8(value.canonical_reimport_receipt_json).ok())
            .flatten(),
        reimport_receipt_hash: (!value.reimport_receipt_hash.is_empty())
            .then(|| hex(&value.reimport_receipt_hash)),
    })
}

fn client_error(request_id: Option<String>, value: ClientError) -> OperatorDriverResponse {
    if matches!(value, ClientError::OutcomeUnknown(_)) {
        return error(request_id, "RDB-OUTCOME-0101", "outcome_unknown", true);
    }
    if let Some(public) = value.public_error() {
        return OperatorDriverResponse::Error {
            request_id,
            code: public.code().to_owned(),
            category: "public".to_owned(),
            message: public.safe_message().to_owned(),
            retryability: "checked".to_owned(),
            recovery_action: format!("{:?}", public.recovery_action()).to_ascii_lowercase(),
            outcome_uncertain: false,
        };
    }
    error(
        request_id,
        "RDB-OPERATOR-0101",
        "operator_request_failed",
        false,
    )
}
fn error(
    request_id: Option<String>,
    code: &str,
    message: &str,
    uncertain: bool,
) -> OperatorDriverResponse {
    OperatorDriverResponse::Error {
        request_id,
        code: code.to_owned(),
        category: "operator".to_owned(),
        message: message.to_owned(),
        retryability: if uncertain {
            "resolve"
        } else {
            "not_retryable"
        }
        .to_owned(),
        recovery_action: if uncertain {
            "observe_same_campaign"
        } else {
            "correct_request"
        }
        .to_owned(),
        outcome_uncertain: uncertain,
    }
}
fn hex(value: &[u8]) -> String {
    value
        .iter()
        .flat_map(|byte| {
            [
                char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'),
                char::from_digit(u32::from(byte & 15), 16).unwrap_or('0'),
            ]
        })
        .collect()
}
fn hex_bytes(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut out = [0; 32];
    for (slot, pair) in out.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(out)
}
fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
fn uuid_bytes(value: &str) -> Option<[u8; 16]> {
    let compact = value.replace('-', "");
    if compact.len() != 32 {
        return None;
    }
    let mut out = [0; 16];
    for (slot, pair) in out.iter_mut().zip(compact.as_bytes().chunks_exact(2)) {
        *slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(out)
}
fn format_uuid(value: &[u8]) -> Option<String> {
    let value = <[u8; 16]>::try_from(value).ok()?;
    Some(format!(
        "{}-{}-{}-{}-{}",
        hex(&value[0..4]),
        hex(&value[4..6]),
        hex(&value[6..8]),
        hex(&value[8..10]),
        hex(&value[10..16])
    ))
}

#[cfg(test)]
mod tests {
    use riffdb_application::ApplicationPortabilityManifest;
    use riffdb_client_rust::{
        CapabilityApplicationReimportScopeV1, RiffDbClient, StartApplicationReimport,
    };
    use riffdb_types::ApplicationInstallationCampaignId;
    use tonic::transport::Endpoint;

    use super::*;

    fn host() -> OperatorDriverHost {
        let manifest = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("manifest");
        let campaign =
            ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [7; 10])
                .expect("campaign");
        let start = StartApplicationReimport::new(
            campaign,
            manifest.input().contract_lineage.clone(),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            manifest,
            b"{\"complete\":true}".to_vec(),
            b"{\"complete\":true}".to_vec(),
        )
        .expect("start");
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        OperatorDriverHost::new(
            "restored".to_owned(),
            start,
            RiffDbClient::from_channel(channel),
            CallMetadata::default(),
        )
    }

    #[tokio::test]
    async fn handshake_is_exactly_campaign_manifest_and_database_bound() {
        let host = host();
        let request = OperatorDriverRequest::Handshake {
            request_id: "handshake-1".to_owned(),
            protocol_version: OPERATOR_DRIVER_PROTOCOL_VERSION,
            database: host.database.clone(),
            campaign_id: host.campaign_text.clone(),
            portability_manifest_hash: host.manifest_hash.clone(),
        };
        assert!(matches!(
            host.handshake(&request),
            OperatorDriverResponse::Handshake { .. }
        ));
        let OperatorDriverRequest::Handshake {
            request_id,
            protocol_version,
            campaign_id,
            portability_manifest_hash,
            ..
        } = request
        else {
            unreachable!()
        };
        assert!(matches!(
            host.handshake(&OperatorDriverRequest::Handshake {
                request_id,
                protocol_version,
                database: "wrong".to_owned(),
                campaign_id,
                portability_manifest_hash,
            }),
            OperatorDriverResponse::Error { .. }
        ));
    }
}
