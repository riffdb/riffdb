//! Structural validation for the additive symbolic application API.

use std::collections::BTreeSet;

use riffdb_types::MAX_CONTRACT_LINEAGE_BYTES;

use crate::app::v1 as app_v1;
use crate::public_message::{
    MAX_PUBLIC_REQUEST_BYTES, MAX_PUBLIC_RESPONSE_BYTES, PublicMessage, PublicWireError,
};
use crate::value::{MAX_PROTOCOL_NAME_BYTES, validate_value};
use crate::wire::{self, Cursor, PreflightError};

const MAX_QUERY_SOURCE_BYTES: usize = 262_144;
const MAX_SYMBOLIC_CATALOG_BYTES: usize = 262_144;
const MAX_QUERY_ITEMS: usize = 1_024;
const MAX_QUERY_ROWS: usize = 500;
const MAX_DIAGNOSTICS: usize = 32;
const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 1_024;
const MAX_CURSOR_BYTES: usize = 4_096;

fn preflight(
    input: &[u8],
    maximum: usize,
    maximum_known_field: u32,
    repeated: &[u32],
    oneof: &[u32],
) -> Result<(), PublicWireError> {
    match wire::bounded_message(input, maximum) {
        Ok(()) => {}
        Err(PreflightError::Malformed) => return Err(PublicWireError::MalformedEncoding),
        Err(PreflightError::LimitExceeded) => {
            return Err(PublicWireError::PreflightLimitExceeded);
        }
    }
    let mut seen = BTreeSet::new();
    let mut saw_oneof = false;
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        if field.number <= maximum_known_field
            && !repeated.contains(&field.number)
            && !seen.insert(field.number)
        {
            return Err(PublicWireError::MalformedEncoding);
        }
        if oneof.contains(&field.number) {
            if saw_oneof {
                return Err(PublicWireError::MalformedEncoding);
            }
            saw_oneof = true;
        }
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_PROTOCOL_NAME_BYTES
}

fn valid_hash(value: &[u8]) -> bool {
    value.len() == 32
}

fn validate_request_id(value: &[u8]) -> Result<(), PublicWireError> {
    if value.len() == 16 {
        Ok(())
    } else {
        Err(PublicWireError::InvalidIdentity)
    }
}

fn validate_selector(value: &app_v1::ContractSelector) -> Result<(), PublicWireError> {
    if value.lineage.is_empty()
        || value.lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
        || value.version == 0
        || (!value.bundle_hash.is_empty() && !valid_hash(&value.bundle_hash))
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

fn validate_parameters(values: &[app_v1::Parameter]) -> Result<(), PublicWireError> {
    if values.len() > MAX_QUERY_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior: Option<&str> = None;
    for value in values {
        if !valid_name(&value.name) || prior.is_some_and(|name| name >= value.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            value
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        prior = Some(&value.name);
    }
    Ok(())
}

fn validate_identity(value: &app_v1::QueryIdentity) -> Result<(), PublicWireError> {
    if value.contract_lineage.is_empty()
        || value.contract_lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
        || value.contract_version == 0
        || !valid_hash(&value.contract_bundle_hash)
        || !valid_hash(&value.plan_hash)
        || value
            .query_name
            .as_deref()
            .is_some_and(|name| !valid_name(name))
        || value
            .module_hash
            .as_deref()
            .is_some_and(|hash| !valid_hash(hash))
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

fn validate_schema(value: &app_v1::QuerySchema) -> Result<(), PublicWireError> {
    for names in [
        value.parameters.as_slice(),
        value.outcomes.as_slice(),
        value.result_fields.as_slice(),
    ] {
        if names.len() > MAX_QUERY_ITEMS
            || names.iter().any(|name| !valid_name(name))
            || names.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(PublicWireError::NonCanonical);
        }
    }
    Ok(())
}

fn validate_diagnostics(values: &[app_v1::Diagnostic]) -> Result<(), PublicWireError> {
    if values.len() > MAX_DIAGNOSTICS {
        return Err(PublicWireError::TooManyItems);
    }
    for value in values {
        let span = value
            .span
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        if !valid_name(&value.code)
            || value.summary.is_empty()
            || value.summary.len() > MAX_DIAGNOSTIC_TEXT_BYTES
            || span.start > span.end
            || value.symbols.len() > MAX_QUERY_ITEMS
            || value.symbols.iter().any(|symbol| !valid_name(symbol))
            || value
                .suggestion
                .as_deref()
                .is_some_and(|text| text.is_empty() || text.len() > MAX_DIAGNOSTIC_TEXT_BYTES)
        {
            return Err(PublicWireError::InvalidBytes);
        }
    }
    Ok(())
}

macro_rules! app_message {
    ($type:ty, $maximum:expr, $last:expr, $repeated:expr, $oneof:expr, $validate:expr) => {
        impl PublicMessage for $type {
            const MAX_ENCODED_BYTES: usize = $maximum;

            fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
                preflight(input, Self::MAX_ENCODED_BYTES, $last, $repeated, $oneof)
            }

            fn validate_structure(&self) -> Result<(), PublicWireError> {
                ($validate)(self)
            }
        }
    };
}

app_message!(
    app_v1::DescribeContractRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::DescribeContractRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)
    }
);
app_message!(
    app_v1::DescribeContractResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    4,
    &[],
    &[],
    |value: &app_v1::DescribeContractResponse| {
        if value.contract_lineage.is_empty()
            || value.contract_lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
            || value.contract_version == 0
            || !valid_hash(&value.contract_bundle_hash)
            || value.symbolic_catalog.is_empty()
            || value.symbolic_catalog.len() > MAX_SYMBOLIC_CATALOG_BYTES
        {
            Err(PublicWireError::InvalidBytes)
        } else {
            Ok(())
        }
    }
);
app_message!(
    app_v1::CheckQueryRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::CheckQueryRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)?;
        if value.source.is_empty() || value.source.len() > MAX_QUERY_SOURCE_BYTES {
            Err(PublicWireError::InvalidBytes)
        } else {
            Ok(())
        }
    }
);
app_message!(
    app_v1::CheckQueryResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[3],
    &[],
    |value: &app_v1::CheckQueryResponse| {
        validate_diagnostics(&value.diagnostics)?;
        match (&value.identity, &value.schema, value.diagnostics.is_empty()) {
            (Some(identity), Some(schema), true) => {
                validate_identity(identity)?;
                validate_schema(schema)
            }
            (None, None, false) => Ok(()),
            _ => Err(PublicWireError::InconsistentFields),
        }
    }
);
app_message!(
    app_v1::ExplainQueryRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[2, 3],
    |value: &app_v1::ExplainQueryRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)?;
        match value.query.as_ref() {
            Some(app_v1::explain_query_request::Query::Source(source))
                if !source.is_empty() && source.len() <= MAX_QUERY_SOURCE_BYTES => {}
            Some(app_v1::explain_query_request::Query::QueryName(name)) if valid_name(name) => {}
            _ => return Err(PublicWireError::InvalidBytes),
        }
        if value
            .module_hash
            .as_deref()
            .is_some_and(|hash| !valid_hash(hash))
        {
            return Err(PublicWireError::InvalidIdentity);
        }
        Ok(())
    }
);
app_message!(
    app_v1::ExplainQueryResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    4,
    &[3, 4],
    &[],
    |value: &app_v1::ExplainQueryResponse| {
        validate_diagnostics(&value.diagnostics)?;
        if value.diagnostics.is_empty() {
            validate_identity(
                value
                    .identity
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?,
            )?;
            validate_schema(
                value
                    .schema
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?,
            )?;
            if value.plan_lines.is_empty()
                || value.plan_lines.len() > MAX_QUERY_ITEMS
                || value
                    .plan_lines
                    .iter()
                    .any(|line| line.is_empty() || line.len() > MAX_DIAGNOSTIC_TEXT_BYTES)
            {
                return Err(PublicWireError::InvalidBytes);
            }
        } else if value.identity.is_some() || value.schema.is_some() || !value.plan_lines.is_empty()
        {
            return Err(PublicWireError::InconsistentFields);
        }
        Ok(())
    }
);
app_message!(
    app_v1::ExecuteQueryRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[5],
    &[2, 3],
    |value: &app_v1::ExecuteQueryRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)?;
        match value.query.as_ref() {
            Some(app_v1::execute_query_request::Query::Source(source))
                if !source.is_empty() && source.len() <= MAX_QUERY_SOURCE_BYTES => {}
            Some(app_v1::execute_query_request::Query::QueryName(name)) if valid_name(name) => {}
            _ => return Err(PublicWireError::InvalidBytes),
        }
        if value
            .module_hash
            .as_deref()
            .is_some_and(|hash| !valid_hash(hash))
            || value
                .cursor
                .as_deref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
            || value.minimum_application_head == Some(0)
        {
            return Err(PublicWireError::InvalidBytes);
        }
        validate_parameters(&value.parameters)
    }
);
app_message!(
    app_v1::ExecuteQueryResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    5,
    &[4],
    &[],
    |value: &app_v1::ExecuteQueryResponse| {
        validate_identity(
            value
                .identity
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?;
        if !valid_name(&value.outcome)
            || value.fields.len() > MAX_QUERY_ITEMS
            || value
                .next_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
            || value
                .fields
                .windows(2)
                .any(|pair| pair[0].name >= pair[1].name)
        {
            return Err(PublicWireError::NonCanonical);
        }
        let mut rows = 0usize;
        for field in &value.fields {
            if !valid_name(&field.name)
                || app_v1::ResultCardinality::try_from(field.cardinality)
                    .ok()
                    .is_none_or(|kind| kind == app_v1::ResultCardinality::Unspecified)
            {
                return Err(PublicWireError::InvalidEnum);
            }
            rows = rows
                .checked_add(field.records.len())
                .ok_or(PublicWireError::TooManyItems)?;
            for record in &field.records {
                if !valid_name(&record.entity) {
                    return Err(PublicWireError::InvalidBytes);
                }
                validate_parameters(&record.fields)?;
            }
        }
        if rows > MAX_QUERY_ROWS {
            return Err(PublicWireError::TooManyItems);
        }
        Ok(())
    }
);

fn validate_named_sources(values: &[app_v1::NamedQuerySource]) -> Result<(), PublicWireError> {
    if values.is_empty() || values.len() > MAX_QUERY_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    for value in values {
        if !valid_name(&value.name)
            || value.source.is_empty()
            || value.source.len() > MAX_QUERY_SOURCE_BYTES
        {
            return Err(PublicWireError::InvalidBytes);
        }
    }
    if values.windows(2).any(|pair| pair[0].name >= pair[1].name) {
        return Err(PublicWireError::NonCanonical);
    }
    Ok(())
}

fn validate_module_descriptor(
    value: &app_v1::QueryModuleDescriptor,
) -> Result<(), PublicWireError> {
    if !valid_name(&value.module_name)
        || value.module_version == 0
        || !valid_hash(&value.module_hash)
        || value.contract_lineage.is_empty()
        || value.contract_lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
        || value.contract_version == 0
        || !valid_hash(&value.contract_bundle_hash)
        || value.query_names.is_empty()
        || value.query_names.len() > MAX_QUERY_ITEMS
        || value.query_names.iter().any(|name| !valid_name(name))
        || value.query_names.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

app_message!(
    app_v1::DeployQueryModuleRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[4],
    &[5, 6, 7],
    |value: &app_v1::DeployQueryModuleRequest| {
        validate_request_id(&value.request_id)?;
        validate_selector(
            value
                .contract
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?;
        if !valid_name(&value.module_name) || value.module_version == 0 {
            return Err(PublicWireError::InvalidIdentity);
        }
        validate_named_sources(&value.queries)?;
        match value.expected_active.as_ref() {
            Some(app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true))
            | Some(app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true)) => {
                Ok(())
            }
            Some(app_v1::deploy_query_module_request::ExpectedActive::ModuleHash(hash))
                if valid_hash(hash) =>
            {
                Ok(())
            }
            _ => Err(PublicWireError::InvalidValue),
        }
    }
);
app_message!(
    app_v1::DeployQueryModuleResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[],
    |value: &app_v1::DeployQueryModuleResponse| {
        let outcome = app_v1::QueryModuleDeploymentOutcome::try_from(value.outcome)
            .map_err(|_| PublicWireError::InvalidEnum)?;
        if outcome == app_v1::QueryModuleDeploymentOutcome::Unspecified {
            return Err(PublicWireError::InvalidEnum);
        }
        validate_module_descriptor(
            value
                .module
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?;
        let mismatch = outcome == app_v1::QueryModuleDeploymentOutcome::ExpectedActiveMismatch;
        if mismatch
            != value
                .actual_active_module_hash
                .as_deref()
                .is_some_and(valid_hash)
            && value.actual_active_module_hash.is_some()
        {
            return Err(PublicWireError::InconsistentFields);
        }
        Ok(())
    }
);
app_message!(
    app_v1::GetQueryModuleRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::GetQueryModuleRequest| {
        validate_request_id(&value.request_id)?;
        validate_selector(
            value
                .contract
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?;
        if value
            .module_hash
            .as_deref()
            .is_some_and(|hash| !valid_hash(hash))
        {
            return Err(PublicWireError::InvalidIdentity);
        }
        Ok(())
    }
);
app_message!(
    app_v1::GetQueryModuleResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[2],
    &[],
    |value: &app_v1::GetQueryModuleResponse| {
        match value.module.as_ref() {
            Some(module) => {
                validate_module_descriptor(module)?;
                validate_named_sources(&value.queries)
            }
            None if value.queries.is_empty() => Ok(()),
            None => Err(PublicWireError::InconsistentFields),
        }
    }
);
