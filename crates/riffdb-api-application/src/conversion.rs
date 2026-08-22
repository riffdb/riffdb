//! Closed generated-operation conversion shared by every public transport.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use riffdb_proto::{app::v1 as app_v1, v1};
use riffdb_service::{
    ApplicationCatalogRequest, CommandDurability, ContractSelection, CursorToken,
    DeclaredOutcomeView, ExecuteCommandRequest, ExecuteCommandResult, ExecuteSymbolicQueryRequest,
    ExecuteSymbolicQueryResult, JournaledCommandResult, JournaledCompletion,
    NamedSymbolicQueryRequest, SchemaBoundOutcomeRecord, SchemaBoundOutcomeValue, SourceName,
    SubmittedDecimal, SubmittedEnum, SubmittedField, SubmittedFieldIdentity, SubmittedMoney,
    SubmittedRecord, SubmittedValue, SymbolicContractSelector, SymbolicQueryParameters,
    SymbolicQuerySource, SymbolicResultField, SymbolicResultRecord,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, CurrencyCode, Date, EnumTypeId,
    EnumVariantId, FieldId, QueryModuleHash, RequestId, Timestamp,
};
use std::num::NonZeroU16;

/// Closed failure at the transport-independent wire/domain boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConversionError {
    /// Caller bytes are structurally valid Protobuf but not one valid operation.
    InvalidRequest,
    /// An API-neutral service result cannot be represented by the frozen public shape.
    InvalidResponse,
}

/// Exact execute operation selected by the public oneof.
pub enum ExecuteSymbolicQueryInvocation {
    /// Ad-hoc source. Retained for unary compatibility; framed sessions reject it.
    AdHoc(ExecuteSymbolicQueryRequest),
    /// One exact named immutable operation.
    Named(NamedSymbolicQueryRequest),
}

/// Converts the exact identity probe used to establish an application session.
pub fn application_session_catalog_request_from_proto(
    request: &v1::ApplicationSessionOpen,
) -> Result<(RequestId, ApplicationCatalogRequest), ConversionError> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = symbolic_contract_selector_from_proto(request.contract.clone())?;
    let limit = NonZeroU16::new(1).expect("one is nonzero");
    let request =
        ApplicationCatalogRequest::new(contract, limit, None).map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts Execute input mechanically and leaves schema resolution to the service.
pub fn execute_command_request_from_proto(
    request: v1::ExecuteCommandRequest,
) -> Result<(RequestId, ExecuteCommandRequest), ConversionError> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let command = SourceName::new(request.command_name).map_err(|_| invalid_request())?;
    let expected_contract_version = request
        .expected_contract_version
        .map(|version| ContractVersion::new(version).ok_or_else(invalid_request))
        .transpose()?;
    let input = submitted_value_from_proto(request.input.ok_or_else(invalid_request)?)?;
    let SubmittedValue::Record(input) = input else {
        return Err(invalid_request());
    };
    let request = ExecuteCommandRequest::new(command, expected_contract_version, input)
        .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts an ad-hoc or named execute request into canonical parameters.
pub fn execute_symbolic_query_request_from_proto(
    request: app_v1::ExecuteQueryRequest,
) -> Result<(RequestId, ExecuteSymbolicQueryInvocation), ConversionError> {
    let (accepts_compact_result_v1, accepts_packed_result_v1) =
        accepted_named_result_encodings(&request.accepted_result_encodings)?;
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = symbolic_contract_selector_from_proto(request.contract)?;
    let module_hash = optional_query_module_hash(request.module_hash)?;
    let parameters = symbolic_parameters_from_proto(&request.parameters)?;
    let cursor = request
        .cursor
        .map(|cursor| cursor_token_from_text(&cursor))
        .transpose()?;
    let minimum_application_head = request.minimum_application_head;
    if minimum_application_head == Some(0) {
        return Err(invalid_request());
    }
    let invocation = match request.query {
        Some(app_v1::execute_query_request::Query::Source(source)) if module_hash.is_none() => {
            let mut request = ExecuteSymbolicQueryRequest::new(
                contract,
                SymbolicQuerySource::new(source).map_err(|_| invalid_request())?,
                parameters,
            );
            if let Some(cursor) = cursor {
                request = request.with_cursor(cursor);
            }
            if let Some(minimum) = minimum_application_head {
                request = request.with_minimum_application_head(minimum);
            }
            ExecuteSymbolicQueryInvocation::AdHoc(request)
        }
        Some(app_v1::execute_query_request::Query::QueryName(name)) => {
            let mut request =
                NamedSymbolicQueryRequest::new(contract, name, module_hash, parameters)
                    .map_err(|_| invalid_request())?;
            if let Some(cursor) = cursor {
                request = request.with_cursor(cursor);
            }
            if let Some(minimum) = minimum_application_head {
                request = request.with_minimum_application_head(minimum);
            }
            if accepts_compact_result_v1 {
                request = request.accepting_compact_result_v1();
            }
            if accepts_packed_result_v1 {
                request = request.accepting_packed_result_v1();
            }
            ExecuteSymbolicQueryInvocation::Named(request)
        }
        _ => return Err(invalid_request()),
    };
    Ok((request_id, invocation))
}

/// Converts a checked command result, including exact read-only sentinels.
pub fn execute_command_result_to_proto(
    result: &ExecuteCommandResult,
    history_incarnation: u64,
) -> Result<v1::ExecuteCommandResponse, ConversionError> {
    match result {
        ExecuteCommandResult::Journaled(result) => {
            journaled_command_result_to_proto(result, history_incarnation)
        }
        ExecuteCommandResult::ReadOnlyExecuted(result) => {
            let outcome = result.outcome();
            Ok(v1::ExecuteCommandResponse {
                status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
                commit_sequence: 0,
                contract_version: result.contract_version().get(),
                plan_hash: result.plan_hash().as_bytes().to_vec(),
                outcome_type: outcome.outcome_name().as_str().to_owned(),
                outcome: Some(schema_bound_outcome_as_public_value(outcome)?),
                provenance_uri: String::new(),
                durability_mode: String::new(),
                outcome_uri: None,
                history_incarnation,
            })
        }
    }
}

fn journaled_command_result_to_proto(
    result: &JournaledCommandResult,
    history_incarnation: u64,
) -> Result<v1::ExecuteCommandResponse, ConversionError> {
    let status = match result.completion() {
        JournaledCompletion::Committed => v1::execute_command_response::CompletionStatus::Committed,
        JournaledCompletion::Replayed => v1::execute_command_response::CompletionStatus::Replayed,
    };
    let durability_mode = match result.durability() {
        CommandDurability::Synchronous => "sync",
        CommandDurability::Group => "group",
    };
    let outcome = result.outcome();
    Ok(v1::ExecuteCommandResponse {
        status: status as i32,
        commit_sequence: result.commit_sequence().get(),
        contract_version: result.contract_version().get(),
        plan_hash: result.plan_hash().as_bytes().to_vec(),
        outcome_type: outcome.outcome_name().as_str().to_owned(),
        outcome: Some(schema_bound_outcome_as_public_value(outcome)?),
        provenance_uri: format!("riffdb://provenance/{}", result.provenance_id()),
        durability_mode: durability_mode.to_owned(),
        outcome_uri: Some(result.outcome_locator().canonical_uri().to_owned()),
        history_incarnation,
    })
}

/// Converts one symbolic snapshot result by consuming owned rows.
pub fn execute_symbolic_query_result_to_proto(
    result: ExecuteSymbolicQueryResult,
) -> Result<app_v1::ExecuteQueryResponse, ConversionError> {
    let (
        identity,
        outcome,
        application_head,
        fields,
        compact_result,
        packed_result,
        enum_names,
        next_cursor,
    ) = result.into_response_parts();
    if !fields.is_empty() && compact_result.is_some() {
        return Err(invalid_response());
    }
    let fields = fields
        .into_iter()
        .map(|(name, field)| symbolic_field_into_proto(&enum_names, name, field))
        .collect::<Result<Vec<_>, _>>()?;
    let (selected_result_encoding, compact_result, packed_result) = match compact_result {
        Some(compact) if packed_result => (
            app_v1::NamedResultEncoding::PackedV1,
            None,
            Some(packed_result_into_proto(compact)?),
        ),
        Some(compact) => (
            app_v1::NamedResultEncoding::CompactV1,
            Some(compact_result_into_proto(&enum_names, compact)?),
            None,
        ),
        None => (app_v1::NamedResultEncoding::LegacyRecords, None, None),
    };
    Ok(app_v1::ExecuteQueryResponse {
        identity: Some(symbolic_identity_to_proto(&identity)),
        outcome,
        application_head,
        fields,
        next_cursor: next_cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor.as_bytes())),
        selected_result_encoding: selected_result_encoding as i32,
        compact_result,
        packed_result,
    })
}

fn request_id_from_bytes(bytes: &[u8]) -> Result<RequestId, ConversionError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_request())?;
    RequestId::from_bytes(bytes).map_err(|_| invalid_request())
}

fn symbolic_contract_selector_from_proto(
    selection: Option<app_v1::ContractSelector>,
) -> Result<SymbolicContractSelector, ConversionError> {
    let Some(selection) = selection else {
        return Ok(SymbolicContractSelector::active());
    };
    let lineage = ContractLineage::new(selection.lineage).map_err(|_| invalid_request())?;
    let version = ContractVersion::new(selection.version).ok_or_else(invalid_request)?;
    if selection.bundle_hash.is_empty() {
        return Ok(SymbolicContractSelector::from_selection(
            ContractSelection::Exact { lineage, version },
        ));
    }
    let hash: [u8; 32] = selection
        .bundle_hash
        .try_into()
        .map_err(|_| invalid_request())?;
    Ok(SymbolicContractSelector::exact(
        lineage,
        version,
        ContractBundleHash::from_bytes(hash),
    ))
}

fn accepted_named_result_encodings(values: &[i32]) -> Result<(bool, bool), ConversionError> {
    match values {
        [] => Ok((false, false)),
        [legacy] if *legacy == app_v1::NamedResultEncoding::LegacyRecords as i32 => {
            Ok((false, false))
        }
        [legacy, compact]
            if *legacy == app_v1::NamedResultEncoding::LegacyRecords as i32
                && *compact == app_v1::NamedResultEncoding::CompactV1 as i32 =>
        {
            Ok((true, false))
        }
        [legacy, compact, packed]
            if *legacy == app_v1::NamedResultEncoding::LegacyRecords as i32
                && *compact == app_v1::NamedResultEncoding::CompactV1 as i32
                && *packed == app_v1::NamedResultEncoding::PackedV1 as i32 =>
        {
            Ok((true, true))
        }
        _ => Err(invalid_request()),
    }
}

fn symbolic_parameters_from_proto(
    values: &[app_v1::Parameter],
) -> Result<SymbolicQueryParameters, ConversionError> {
    let mut prior: Option<&str> = None;
    let mut parameters = std::collections::BTreeMap::new();
    for parameter in values {
        if parameter.name.is_empty() || prior.is_some_and(|name| name >= parameter.name.as_str()) {
            return Err(invalid_request());
        }
        let submitted =
            submitted_value_from_proto(parameter.value.clone().ok_or_else(invalid_request)?)?;
        parameters.insert(parameter.name.clone(), submitted);
        prior = Some(&parameter.name);
    }
    SymbolicQueryParameters::new(parameters).map_err(|_| invalid_request())
}

fn cursor_token_from_text(cursor: &str) -> Result<CursorToken, ConversionError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|_| invalid_request())?;
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_request())?;
    Ok(CursorToken::from_bytes(bytes))
}

fn optional_query_module_hash(
    value: Option<Vec<u8>>,
) -> Result<Option<QueryModuleHash>, ConversionError> {
    value
        .map(|bytes| {
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| invalid_request())?;
            Ok(QueryModuleHash::from_bytes(bytes))
        })
        .transpose()
}

fn submitted_value_from_proto(value: v1::Value) -> Result<SubmittedValue, ConversionError> {
    use v1::value::Kind;
    match value.kind.ok_or_else(invalid_request)? {
        Kind::NullValue(value) if value == v1::NullValue::NullValue as i32 => {
            Ok(SubmittedValue::Null)
        }
        Kind::NullValue(_) => Err(invalid_request()),
        Kind::BoolValue(value) => Ok(SubmittedValue::Bool(value)),
        Kind::I64Value(value) => Ok(SubmittedValue::I64(value)),
        Kind::U64Value(value) => Ok(SubmittedValue::U64(value)),
        Kind::DecimalValue(value) => submitted_decimal(&value).map(SubmittedValue::Decimal),
        Kind::MoneyValue(value) => {
            let currency =
                CurrencyCode::new(value.currency.as_bytes()).map_err(|_| invalid_request())?;
            let amount = value.amount.as_ref().ok_or_else(invalid_request)?;
            Ok(SubmittedValue::Money(SubmittedMoney::new(
                currency,
                submitted_decimal(amount)?,
            )))
        }
        Kind::StringValue(value) => SubmittedValue::string(value).map_err(|_| invalid_request()),
        Kind::BytesValue(value) => SubmittedValue::bytes(value).map_err(|_| invalid_request()),
        Kind::UuidValue(value) => Ok(SubmittedValue::Uuid(
            value.try_into().map_err(|_| invalid_request())?,
        )),
        Kind::DateValue(value) => Ok(SubmittedValue::Date(Date::new(value.days_since_unix_epoch))),
        Kind::TimestampValue(value) => Ok(SubmittedValue::Timestamp(
            Timestamp::new(value.seconds, value.nanos).map_err(|_| invalid_request())?,
        )),
        Kind::EnumValue(value) => {
            let name = (!value.name.is_empty())
                .then(|| SourceName::new(value.name))
                .transpose()
                .map_err(|_| invalid_request())?;
            let submitted = match (
                EnumTypeId::new(value.type_id),
                EnumVariantId::new(value.variant_id),
                name,
            ) {
                (Some(type_id), Some(variant_id), name) => {
                    SubmittedEnum::new(type_id, variant_id, name)
                }
                (None, None, Some(name)) => SubmittedEnum::name_only(name),
                _ => return Err(invalid_request()),
            };
            Ok(SubmittedValue::Enum(submitted))
        }
        Kind::ListValue(value) => SubmittedValue::list(
            value
                .values
                .into_iter()
                .map(submitted_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|_| invalid_request()),
        Kind::RecordValue(value) => submitted_record_from_proto(value).map(SubmittedValue::Record),
        Kind::VectorValue(value) => riffdb_types::CanonicalVector::new(value.components)
            .map(SubmittedValue::Vector)
            .map_err(|_| invalid_request()),
    }
}

fn submitted_record_from_proto(
    record: v1::ValueRecord,
) -> Result<SubmittedRecord, ConversionError> {
    let fields = record
        .fields
        .into_iter()
        .map(|field| {
            let id = field
                .field_id
                .map(|id| FieldId::new(id).ok_or_else(invalid_request))
                .transpose()?;
            let name = (!field.name.is_empty())
                .then(|| SourceName::new(field.name))
                .transpose()
                .map_err(|_| invalid_request())?;
            let identity = match (id, name) {
                (Some(id), Some(name)) => SubmittedFieldIdentity::IdAndName { id, name },
                (Some(id), None) => SubmittedFieldIdentity::Id(id),
                (None, Some(name)) => SubmittedFieldIdentity::Name(name),
                (None, None) => return Err(invalid_request()),
            };
            let value = submitted_value_from_proto(field.value.ok_or_else(invalid_request)?)?;
            Ok(SubmittedField::new(identity, value))
        })
        .collect::<Result<Vec<_>, ConversionError>>()?;
    SubmittedRecord::new(fields).map_err(|_| invalid_request())
}

fn submitted_decimal(value: &v1::Decimal) -> Result<SubmittedDecimal, ConversionError> {
    let scale = u8::try_from(value.scale).map_err(|_| invalid_request())?;
    let precision = value
        .precision
        .map(u8::try_from)
        .transpose()
        .map_err(|_| invalid_request())?;
    SubmittedDecimal::from_minimal_twos_complement_with_precision(
        &value.coefficient_twos_complement,
        scale,
        precision,
    )
    .map_err(|_| invalid_request())
}

fn schema_bound_outcome_as_public_value(
    outcome: &DeclaredOutcomeView,
) -> Result<v1::Value, ConversionError> {
    Ok(v1::Value {
        kind: Some(v1::value::Kind::RecordValue(schema_bound_record_to_public(
            outcome.schema_bound_value(),
        )?)),
    })
}

fn schema_bound_record_to_public(
    record: SchemaBoundOutcomeRecord<'_>,
) -> Result<v1::ValueRecord, ConversionError> {
    let mut fields = Vec::with_capacity(record.len());
    for index in 0..record.len() {
        let field = record.field(index).ok_or_else(invalid_response)?;
        fields.push(v1::ValueField {
            field_id: Some(field.field_id().get()),
            name: field.field_name().as_str().to_owned(),
            value: Some(schema_bound_value_to_public(
                field.value().ok_or_else(invalid_response)?,
            )?),
        });
    }
    Ok(v1::ValueRecord { fields })
}

fn schema_bound_value_to_public(
    value: SchemaBoundOutcomeValue<'_>,
) -> Result<v1::Value, ConversionError> {
    let kind = match value {
        SchemaBoundOutcomeValue::Null => {
            v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)
        }
        SchemaBoundOutcomeValue::Scalar(value) => {
            return riffdb_proto::canonical_value_to_proto(value).map_err(|_| invalid_response());
        }
        SchemaBoundOutcomeValue::Enum {
            type_id,
            variant_id,
            variant_name,
        } => v1::value::Kind::EnumValue(v1::EnumValue {
            type_id: type_id.get(),
            variant_id: variant_id.get(),
            name: variant_name.as_str().to_owned(),
        }),
        SchemaBoundOutcomeValue::List(values) => {
            let mut output = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                output.push(schema_bound_value_to_public(
                    values.value(index).ok_or_else(invalid_response)?,
                )?);
            }
            v1::value::Kind::ListValue(v1::ValueList { values: output })
        }
        SchemaBoundOutcomeValue::Record(record) => {
            v1::value::Kind::RecordValue(schema_bound_record_to_public(record)?)
        }
    };
    Ok(v1::Value { kind: Some(kind) })
}

fn symbolic_identity_to_proto(
    identity: &riffdb_service::SymbolicQueryIdentity,
) -> app_v1::QueryIdentity {
    app_v1::QueryIdentity {
        contract_lineage: identity.lineage().as_str().to_owned(),
        contract_version: identity.version().get(),
        contract_bundle_hash: identity.bundle_hash().as_bytes().to_vec(),
        query_name: identity.name().map(str::to_owned),
        plan_hash: identity.plan_hash().as_bytes().to_vec(),
        module_hash: identity.module_hash().map(|hash| hash.as_bytes().to_vec()),
    }
}

fn symbolic_record_into_proto(
    enum_names: &riffdb_service::SharedEnumVariantNames,
    record: SymbolicResultRecord,
) -> Result<app_v1::ResultRecord, ConversionError> {
    let (entity, fields, exact_decimals) = record.into_parts();
    let mut fields = fields
        .into_iter()
        .map(|(name, value)| {
            let mut value = canonical_value_into_public(value)?;
            name_symbolic_enum_values(enum_names, &mut value)?;
            Ok(app_v1::Parameter {
                name: name.to_string(),
                value: Some(value),
            })
        })
        .collect::<Result<Vec<_>, ConversionError>>()?;
    fields.extend(
        exact_decimals
            .into_iter()
            .map(|(name, value)| app_v1::Parameter {
                name: name.to_string(),
                value: Some(v1::Value {
                    kind: Some(v1::value::Kind::DecimalValue(
                        riffdb_proto::aggregate_decimal_sum_to_proto(
                            value.coefficient(),
                            value.scale(),
                        ),
                    )),
                }),
            }),
    );
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(app_v1::ResultRecord {
        fields,
        entity: entity.to_string(),
    })
}

fn symbolic_field_into_proto(
    enum_names: &riffdb_service::SharedEnumVariantNames,
    name: String,
    field: SymbolicResultField,
) -> Result<app_v1::ResultField, ConversionError> {
    let (cardinality, records) = match field {
        SymbolicResultField::One(record) => (
            app_v1::ResultCardinality::One,
            vec![symbolic_record_into_proto(enum_names, record)?],
        ),
        SymbolicResultField::Maybe(record) => (
            app_v1::ResultCardinality::Maybe,
            record
                .map(|record| symbolic_record_into_proto(enum_names, record))
                .transpose()?
                .into_iter()
                .collect(),
        ),
        SymbolicResultField::Many(records) => (
            app_v1::ResultCardinality::Many,
            records
                .into_iter()
                .map(|record| symbolic_record_into_proto(enum_names, record))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };
    Ok(app_v1::ResultField {
        name,
        cardinality: cardinality as i32,
        records,
    })
}

fn name_symbolic_enum_values(
    enum_names: &riffdb_service::SharedEnumVariantNames,
    value: &mut v1::Value,
) -> Result<(), ConversionError> {
    use v1::value::Kind;
    match value.kind.as_mut().ok_or_else(invalid_response)? {
        Kind::EnumValue(enumeration) => {
            enumeration.name = enum_names
                .get(&(enumeration.type_id, enumeration.variant_id))
                .ok_or_else(invalid_response)?
                .clone();
        }
        Kind::ListValue(values) => {
            for value in &mut values.values {
                name_symbolic_enum_values(enum_names, value)?;
            }
        }
        Kind::RecordValue(record) => {
            for field in &mut record.fields {
                name_symbolic_enum_values(
                    enum_names,
                    field.value.as_mut().ok_or_else(invalid_response)?,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn canonical_value_into_public(
    value: riffdb_types::CanonicalValue,
) -> Result<v1::Value, ConversionError> {
    let wire = canonical_value_into_public_unchecked(value);
    riffdb_proto::validate_value(&wire).map_err(|_| invalid_response())?;
    Ok(wire)
}

fn canonical_value_into_public_unchecked(value: riffdb_types::CanonicalValue) -> v1::Value {
    use riffdb_types::CanonicalValue;
    use v1::value::Kind;
    let kind = match value {
        CanonicalValue::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        CanonicalValue::Bool(value) => Kind::BoolValue(value),
        CanonicalValue::I64(value) => Kind::I64Value(value),
        CanonicalValue::U64(value) => Kind::U64Value(value),
        CanonicalValue::Decimal(value) => Kind::DecimalValue(riffdb_proto::decimal_to_proto(value)),
        CanonicalValue::Money(value) => Kind::MoneyValue(riffdb_proto::money_to_proto(value)),
        CanonicalValue::String(value) => Kind::StringValue(value.into_string()),
        CanonicalValue::Bytes(value) => Kind::BytesValue(value.into_vec()),
        CanonicalValue::Uuid(value) => Kind::UuidValue(value.to_vec()),
        CanonicalValue::Date(value) => Kind::DateValue(v1::Date {
            days_since_unix_epoch: value.days_since_unix_epoch(),
        }),
        CanonicalValue::Timestamp(value) => Kind::TimestampValue(v1::Timestamp {
            seconds: value.seconds(),
            nanos: value.nanoseconds(),
        }),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => Kind::EnumValue(v1::EnumValue {
            type_id: type_id.get(),
            variant_id: variant_id.get(),
            name: String::new(),
        }),
        CanonicalValue::List(values) => Kind::ListValue(v1::ValueList {
            values: values
                .into_values()
                .into_iter()
                .map(canonical_value_into_public_unchecked)
                .collect(),
        }),
        CanonicalValue::Record(record) => Kind::RecordValue(v1::ValueRecord {
            fields: record
                .into_fields()
                .into_iter()
                .map(|(field_id, child)| v1::ValueField {
                    field_id: Some(field_id.get()),
                    name: String::new(),
                    value: Some(canonical_value_into_public_unchecked(child)),
                })
                .collect(),
        }),
        CanonicalValue::Vector(vector) => Kind::VectorValue(v1::VectorValue {
            components: vector.into_components(),
        }),
    };
    v1::Value { kind: Some(kind) }
}

fn packed_result_into_proto(
    compact: riffdb_service::CoveredQueryResultV1,
) -> Result<app_v1::PackedResultField, ConversionError> {
    let (name, entity, fields, rows) = compact.into_parts();
    let width = fields.len();
    if rows.iter().any(|row| row.len() != width) {
        return Err(invalid_response());
    }
    let row_count = u32::try_from(rows.len()).map_err(|_| invalid_response())?;
    let columns = (0..width)
        .map(|column| {
            pack_column(
                rows.iter()
                    .map(|row| row.get(column).ok_or_else(invalid_response)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(app_v1::PackedResultField {
        name,
        cardinality: app_v1::ResultCardinality::Many as i32,
        entity: entity.to_string(),
        fields: fields.into_iter().map(|field| field.to_string()).collect(),
        row_count,
        columns,
    })
}

fn pack_column<'a, I>(cells: I) -> Result<app_v1::PackedColumn, ConversionError>
where
    I: Iterator<Item = Result<&'a riffdb_types::CanonicalValue, ConversionError>>,
{
    let mut data = Vec::new();
    let mut offsets = vec![0_u32];
    for cell in cells {
        let value = cell?;
        let needed =
            riffdb_types::canonical_value_encoded_len(value).map_err(|_| invalid_response())?;
        data.reserve(needed);
        riffdb_types::encode_canonical_value_into(&mut data, value)
            .map_err(|_| invalid_response())?;
        offsets.push(u32::try_from(data.len()).map_err(|_| invalid_response())?);
    }
    Ok(app_v1::PackedColumn { data, offsets })
}

fn compact_result_into_proto(
    enum_names: &riffdb_service::SharedEnumVariantNames,
    compact: riffdb_service::CoveredQueryResultV1,
) -> Result<app_v1::CompactResultField, ConversionError> {
    let (name, entity, fields, rows) = compact.into_parts();
    let width = fields.len();
    let rows = rows
        .into_iter()
        .map(|values| {
            if values.len() != width {
                return Err(invalid_response());
            }
            let values = values
                .into_iter()
                .map(|value| {
                    let mut value = canonical_value_into_public(value)?;
                    name_symbolic_enum_values(enum_names, &mut value)?;
                    Ok(value)
                })
                .collect::<Result<Vec<_>, ConversionError>>()?;
            Ok(app_v1::CompactResultRow { values })
        })
        .collect::<Result<Vec<_>, ConversionError>>()?;
    Ok(app_v1::CompactResultField {
        name,
        cardinality: app_v1::ResultCardinality::Many as i32,
        entity: entity.to_string(),
        fields: fields.into_iter().map(|field| field.to_string()).collect(),
        rows,
    })
}

const fn invalid_request() -> ConversionError {
    ConversionError::InvalidRequest
}

const fn invalid_response() -> ConversionError {
    ConversionError::InvalidResponse
}
