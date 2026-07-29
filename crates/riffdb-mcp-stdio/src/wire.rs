//! Checked lowering from common MCP request DTOs to the public Protobuf surface.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use riffdb_api_mcp::{
    McpContractSelection, McpFixedToolRequest, McpPageRequest, McpSubmittedField,
    McpSubmittedFieldIdentity, McpSubmittedValue,
};
use riffdb_client_rust::{app_v1, v1};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WireConversionError;

pub(crate) enum FixedGrpcRequest {
    ValidateContract(v1::ValidateContractRequest),
    GetActiveContract(v1::GetActiveContractRequest),
    ExplainCommand(v1::ExplainCommandRequest),
    DeployContract(v1::DeployContractRequest),
    GetOutcome(v1::GetOutcomeRequest),
    GetEntity(v1::GetEntityRequest),
    ScanIndex(v1::ScanIndexRequest),
    GetCommit(v1::GetCommitRequest),
    ScanCommits(v1::ScanCommitsRequest),
    TraceProvenance(v1::TraceProvenanceRequest),
    QueryProjection(v1::QueryProjectionRequest),
    GetProjectionStatus(v1::GetProjectionStatusRequest),
    ListPendingOutboxDeliveries(v1::ListPendingOutboxDeliveriesRequest),
    Health(v1::HealthRequest),
    DescribeContract(app_v1::DescribeContractRequest),
    CheckQuery(app_v1::CheckQueryRequest),
    ExplainQuery(app_v1::ExplainQueryRequest),
    ExecuteQuery(app_v1::ExecuteQueryRequest),
    RunCommand(v1::ExecuteCommandRequest),
}

pub(crate) fn fixed_request_to_proto(
    request_id: [u8; 16],
    request: McpFixedToolRequest,
) -> Result<FixedGrpcRequest, WireConversionError> {
    let request_id = request_id.to_vec();
    Ok(match request {
        McpFixedToolRequest::ValidateContract { source } => {
            FixedGrpcRequest::ValidateContract(v1::ValidateContractRequest { request_id, source })
        }
        McpFixedToolRequest::GetActiveContract => {
            FixedGrpcRequest::GetActiveContract(v1::GetActiveContractRequest { request_id })
        }
        McpFixedToolRequest::ExplainCommand {
            contract,
            command_name,
        } => FixedGrpcRequest::ExplainCommand(v1::ExplainCommandRequest {
            request_id,
            contract: Some(contract_selection_to_proto(contract)),
            command_name,
        }),
        McpFixedToolRequest::DeployContract {
            source,
            expected_active_version,
        } => FixedGrpcRequest::DeployContract(v1::DeployContractRequest {
            request_id,
            source,
            expected_active_version,
        }),
        McpFixedToolRequest::GetOutcomeIdentity {
            contract_lineage,
            command_name,
            idempotency_key,
        } => FixedGrpcRequest::GetOutcome(v1::GetOutcomeRequest {
            request_id,
            contract_lineage,
            command_name,
            idempotency_key,
            outcome_uri: None,
        }),
        McpFixedToolRequest::GetOutcomeLocator { outcome_uri } => {
            FixedGrpcRequest::GetOutcome(v1::GetOutcomeRequest {
                request_id,
                contract_lineage: String::new(),
                command_name: String::new(),
                idempotency_key: String::new(),
                outcome_uri: Some(outcome_uri),
            })
        }
        McpFixedToolRequest::GetEntity {
            contract,
            entity_type_id,
            entity_key,
            fields,
        } => FixedGrpcRequest::GetEntity(v1::GetEntityRequest {
            request_id,
            contract: Some(contract_selection_to_proto(contract)),
            entity_type_id,
            entity_key,
            fields: Some(v1::FieldSelection { field_ids: fields }),
        }),
        McpFixedToolRequest::ScanIndex {
            contract,
            index_id,
            leading_components,
            fields,
            page,
        } => FixedGrpcRequest::ScanIndex(v1::ScanIndexRequest {
            request_id,
            contract: Some(contract_selection_to_proto(contract)),
            index_id,
            leading_components: leading_components
                .into_iter()
                .map(submitted_value_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
            fields: Some(v1::FieldSelection { field_ids: fields }),
            page: Some(page_to_proto(page)),
        }),
        McpFixedToolRequest::GetCommit { commit_sequence } => {
            FixedGrpcRequest::GetCommit(v1::GetCommitRequest {
                request_id,
                commit_sequence,
            })
        }
        McpFixedToolRequest::ScanCommits { page } => {
            FixedGrpcRequest::ScanCommits(v1::ScanCommitsRequest {
                request_id,
                page: Some(page_to_proto(page)),
            })
        }
        McpFixedToolRequest::TraceProvenance { selector } => {
            let selection = match selector {
                riffdb_api_mcp::McpProvenanceSelector::CommitSequence(sequence) => {
                    v1::provenance_selection::Selection::CommitSequence(sequence)
                }
                riffdb_api_mcp::McpProvenanceSelector::ProvenanceId(id) => {
                    v1::provenance_selection::Selection::ProvenanceId(id.to_vec())
                }
            };
            FixedGrpcRequest::TraceProvenance(v1::TraceProvenanceRequest {
                request_id,
                selector: Some(v1::ProvenanceSelection {
                    selection: Some(selection),
                }),
            })
        }
        McpFixedToolRequest::QueryProjection {
            contract,
            projection_id,
            leading_components,
            required_sequence,
            wait_nanos,
            page,
        } => FixedGrpcRequest::QueryProjection(v1::QueryProjectionRequest {
            request_id,
            contract: Some(contract_selection_to_proto(contract)),
            projection_id,
            leading_components: leading_components
                .into_iter()
                .map(submitted_value_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
            required_sequence,
            wait_nanos,
            page: Some(page_to_proto(page)),
        }),
        McpFixedToolRequest::GetProjectionStatus {
            contract,
            projection_id,
        } => FixedGrpcRequest::GetProjectionStatus(v1::GetProjectionStatusRequest {
            request_id,
            contract: Some(contract_selection_to_proto(contract)),
            projection_id,
        }),
        McpFixedToolRequest::ListPendingOutboxDeliveries { page } => {
            FixedGrpcRequest::ListPendingOutboxDeliveries(v1::ListPendingOutboxDeliveriesRequest {
                request_id,
                page: Some(page_to_proto(page)),
            })
        }
        McpFixedToolRequest::Health => FixedGrpcRequest::Health(v1::HealthRequest {
            request_id: Some(request_id),
        }),
        McpFixedToolRequest::DescribeContract { contract } => {
            FixedGrpcRequest::DescribeContract(app_v1::DescribeContractRequest {
                contract: contract.and_then(symbolic_contract_selection_to_proto),
                request_id,
            })
        }
        McpFixedToolRequest::CheckQuery { contract, source } => {
            FixedGrpcRequest::CheckQuery(app_v1::CheckQueryRequest {
                contract: contract.and_then(symbolic_contract_selection_to_proto),
                source,
                request_id,
            })
        }
        McpFixedToolRequest::ExplainQuery { contract, source } => {
            FixedGrpcRequest::ExplainQuery(app_v1::ExplainQueryRequest {
                contract: contract.and_then(symbolic_contract_selection_to_proto),
                query: Some(app_v1::explain_query_request::Query::Source(source)),
                module_hash: None,
                request_id,
            })
        }
        McpFixedToolRequest::ExecuteQuery {
            contract,
            source,
            parameters,
            cursor,
        } => {
            let mut parameters = parameters
                .into_iter()
                .map(|(name, value)| {
                    Ok(app_v1::Parameter {
                        name,
                        value: Some(natural_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>, WireConversionError>>()?;
            parameters.sort_by(|left, right| left.name.cmp(&right.name));
            FixedGrpcRequest::ExecuteQuery(app_v1::ExecuteQueryRequest {
                contract: contract.and_then(symbolic_contract_selection_to_proto),
                query: Some(app_v1::execute_query_request::Query::Source(source)),
                module_hash: None,
                parameters,
                cursor: cursor
                    .map(|cursor| {
                        riffdb_api_mcp::decode_mcp_cursor(&cursor)
                            .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                            .map_err(|_| WireConversionError)
                    })
                    .transpose()?,
                request_id,
            })
        }
        McpFixedToolRequest::RunCommand {
            command_name,
            input,
            expected_contract_version,
        } => {
            let mut fields = input
                .into_iter()
                .map(|(name, value)| {
                    Ok(v1::ValueField {
                        field_id: None,
                        name,
                        value: Some(natural_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>, WireConversionError>>()?;
            fields.sort_by(|left, right| left.name.cmp(&right.name));
            FixedGrpcRequest::RunCommand(v1::ExecuteCommandRequest {
                request_id,
                command_name,
                expected_contract_version,
                input: Some(v1::Value {
                    kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
                }),
            })
        }
    })
}

fn symbolic_contract_selection_to_proto(
    selection: McpContractSelection,
) -> Option<app_v1::ContractSelector> {
    match selection {
        McpContractSelection::Active => None,
        McpContractSelection::Exact {
            contract_lineage,
            contract_version,
        } => Some(app_v1::ContractSelector {
            lineage: contract_lineage,
            version: contract_version,
            bundle_hash: Vec::new(),
        }),
    }
}

fn natural_value_to_proto(value: serde_json::Value) -> Result<v1::Value, WireConversionError> {
    use v1::value::Kind;
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        serde_json::Value::Bool(value) => Kind::BoolValue(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Kind::U64Value(value)
            } else {
                Kind::I64Value(value.as_i64().ok_or(WireConversionError)?)
            }
        }
        serde_json::Value::String(value) => Kind::StringValue(value),
        serde_json::Value::Array(values) => Kind::ListValue(v1::ValueList {
            values: values
                .into_iter()
                .map(natural_value_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        serde_json::Value::Object(_) => return Err(WireConversionError),
    };
    Ok(v1::Value { kind: Some(kind) })
}

pub(crate) fn submitted_record_to_proto(
    fields: Vec<McpSubmittedField>,
) -> Result<v1::Value, WireConversionError> {
    submitted_value_to_proto(McpSubmittedValue::Record(fields))
}

pub(crate) fn contract_selection_to_proto(
    selection: McpContractSelection,
) -> v1::ContractSelection {
    let selection = match selection {
        McpContractSelection::Active => v1::contract_selection::Selection::Active(v1::Unit {}),
        McpContractSelection::Exact {
            contract_lineage,
            contract_version,
        } => v1::contract_selection::Selection::Exact(v1::ExactContractSelection {
            contract_lineage,
            contract_version,
        }),
    };
    v1::ContractSelection {
        selection: Some(selection),
    }
}

pub(crate) fn page_to_proto(page: McpPageRequest) -> v1::PageRequest {
    v1::PageRequest {
        limit: Some(u32::from(page.limit())),
        cursor: page.cursor().map(|cursor| cursor.to_vec()),
    }
}

fn submitted_value_to_proto(value: McpSubmittedValue) -> Result<v1::Value, WireConversionError> {
    use v1::value::Kind;

    let kind = match value {
        McpSubmittedValue::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        McpSubmittedValue::Bool(value) => Kind::BoolValue(value),
        McpSubmittedValue::I64(value) => Kind::I64Value(value),
        McpSubmittedValue::U64(value) => Kind::U64Value(value),
        McpSubmittedValue::Decimal {
            coefficient,
            precision,
            scale,
        } => Kind::DecimalValue(v1::Decimal {
            coefficient_twos_complement: encode_minimal_i128(coefficient),
            scale: u32::from(scale),
            precision: Some(u32::from(precision)),
        }),
        McpSubmittedValue::Money {
            currency,
            coefficient,
            precision,
            scale,
        } => Kind::MoneyValue(v1::Money {
            currency,
            amount: Some(v1::Decimal {
                coefficient_twos_complement: encode_minimal_i128(coefficient),
                scale: u32::from(scale),
                precision: Some(u32::from(precision)),
            }),
        }),
        McpSubmittedValue::String(value) => Kind::StringValue(value),
        McpSubmittedValue::Bytes(value) => Kind::BytesValue(value),
        McpSubmittedValue::Timestamp { seconds, nanos } => {
            Kind::TimestampValue(v1::Timestamp { seconds, nanos })
        }
        McpSubmittedValue::Date(days_since_unix_epoch) => Kind::DateValue(v1::Date {
            days_since_unix_epoch,
        }),
        McpSubmittedValue::Uuid(value) => Kind::UuidValue(value.to_vec()),
        McpSubmittedValue::EnumIdentity {
            type_id,
            variant_id,
        } => Kind::EnumValue(v1::EnumValue {
            type_id,
            variant_id,
            name: String::new(),
        }),
        McpSubmittedValue::EnumName(name) => Kind::EnumValue(v1::EnumValue {
            type_id: 0,
            variant_id: 0,
            name,
        }),
        McpSubmittedValue::List(values) => Kind::ListValue(v1::ValueList {
            values: values
                .into_iter()
                .map(submitted_value_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        McpSubmittedValue::Record(fields) => Kind::RecordValue(v1::ValueRecord {
            fields: fields
                .into_iter()
                .map(submitted_field_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
    };
    Ok(v1::Value { kind: Some(kind) })
}

fn submitted_field_to_proto(
    field: McpSubmittedField,
) -> Result<v1::ValueField, WireConversionError> {
    let (field_id, name) = match field.identity {
        McpSubmittedFieldIdentity::Id(id) => (Some(id), String::new()),
        McpSubmittedFieldIdentity::Name(name) => (None, name),
    };
    Ok(v1::ValueField {
        field_id,
        name,
        value: Some(submitted_value_to_proto(field.value)?),
    })
}

fn encode_minimal_i128(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let removable_positive = bytes[start] == 0 && bytes[start + 1] & 0x80 == 0;
        let removable_negative = bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0;
        if !removable_positive && !removable_negative {
            break;
        }
        start += 1;
    }
    bytes[start..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_coefficients_use_minimal_twos_complement() {
        assert_eq!(encode_minimal_i128(0), [0]);
        assert_eq!(encode_minimal_i128(127), [0x7f]);
        assert_eq!(encode_minimal_i128(128), [0, 0x80]);
        assert_eq!(encode_minimal_i128(-1), [0xff]);
        assert_eq!(encode_minimal_i128(-128), [0x80]);
        assert_eq!(encode_minimal_i128(-129), [0xff, 0x7f]);
    }

    #[test]
    fn enum_names_use_the_schema_resolved_public_form() {
        let value = submitted_value_to_proto(McpSubmittedValue::EnumName("Open".to_owned()))
            .expect("name-only enum lowers");
        let Some(v1::value::Kind::EnumValue(value)) = value.kind else {
            panic!("expected enum value");
        };
        assert_eq!(value.type_id, 0);
        assert_eq!(value.variant_id, 0);
        assert_eq!(value.name, "Open");
    }

    #[test]
    fn decimal_input_preserves_declared_precision() {
        let value = submitted_value_to_proto(McpSubmittedValue::Decimal {
            coefficient: 123,
            precision: 4,
            scale: 2,
        })
        .expect("decimal lowers");
        let Some(v1::value::Kind::DecimalValue(value)) = value.kind else {
            panic!("expected decimal value");
        };
        assert_eq!(value.coefficient_twos_complement, [0x7b]);
        assert_eq!(value.scale, 2);
        assert_eq!(value.precision, Some(4));
    }

    #[test]
    fn active_and_exact_selections_preserve_identity() {
        assert!(matches!(
            contract_selection_to_proto(McpContractSelection::Active).selection,
            Some(v1::contract_selection::Selection::Active(_))
        ));
        let exact = contract_selection_to_proto(McpContractSelection::Exact {
            contract_lineage: "Example".to_owned(),
            contract_version: 7,
        });
        let Some(v1::contract_selection::Selection::Exact(exact)) = exact.selection else {
            panic!("expected exact selection");
        };
        assert_eq!(exact.contract_lineage, "Example");
        assert_eq!(exact.contract_version, 7);
    }
}
