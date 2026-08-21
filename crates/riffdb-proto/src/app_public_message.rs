//! Structural validation for the additive symbolic application API.

use std::collections::BTreeSet;

use riffdb_types::{EmbeddingMetadata, EntityKey, MAX_CONTRACT_LINEAGE_BYTES};

use crate::app::v1 as app_v1;
use crate::public_message::{
    MAX_PUBLIC_REQUEST_BYTES, MAX_PUBLIC_RESPONSE_BYTES, PublicMessage, PublicWireError,
};
use crate::value::{MAX_PROTOCOL_NAME_BYTES, validate_value};
use crate::wire::{self, Cursor, PreflightError};

const MAX_QUERY_SOURCE_BYTES: usize = 262_144;
const MAX_REACTIVE_SOURCE_BYTES: usize = 1_048_576;
const MAX_REACTIVE_QUERY_MODULES: usize = 32;
const MAX_SYMBOLIC_CATALOG_BYTES: usize = 262_144;
const MAX_QUERY_ITEMS: usize = 1_024;
const MAX_QUERY_ROWS: usize = 500;
const MAX_DIAGNOSTICS: usize = 32;
const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 1_024;
const MAX_CURSOR_BYTES: usize = 4_096;
const MAX_APPLICATION_CATALOG_PAGE_ITEMS: usize = 100;
const MAX_APPLICATION_CATALOG_PATH_COMPONENTS: usize = 8;
const MAX_APPLICATION_CATALOG_TEXT_BYTES: usize = 256;
const MAX_APPLICATION_CATALOG_FEATURES: usize = 6;
const VECTOR_CURSOR_BYTES: usize = 16;

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

fn validate_vector_page_request(value: &crate::v1::PageRequest) -> Result<(), PublicWireError> {
    if value.limit.is_some_and(|limit| limit == 0 || limit > 500)
        || value
            .cursor
            .as_deref()
            .is_some_and(|cursor| cursor.len() != VECTOR_CURSOR_BYTES)
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_vector_staleness_page(
    value: &app_v1::VectorStalenessPage,
) -> Result<(), PublicWireError> {
    if value.items.len() > MAX_QUERY_ROWS
        || value
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.len() != VECTOR_CURSOR_BYTES)
        || value.observed_frontier == Some(0)
    {
        return Err(PublicWireError::InvalidValue);
    }
    for item in &value.items {
        let key = EntityKey::from_bytes(item.entity_key.clone())
            .map_err(|_| PublicWireError::InvalidIdentity)?;
        if key.as_bytes().is_empty()
            || item.newest_source_write == 0
            || item.embedding_write == Some(0)
            || item
                .embedding_write
                .is_some_and(|embedding| item.newest_source_write <= embedding)
        {
            return Err(PublicWireError::InvalidValue);
        }
    }
    Ok(())
}

fn validate_vector_model_page(
    value: &app_v1::VectorModelVersionPage,
) -> Result<(), PublicWireError> {
    if value.items.len() > MAX_QUERY_ROWS
        || value
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.len() != VECTOR_CURSOR_BYTES)
        || value.observed_frontier == Some(0)
    {
        return Err(PublicWireError::InvalidValue);
    }
    for item in &value.items {
        EntityKey::from_bytes(item.entity_key.clone())
            .map_err(|_| PublicWireError::InvalidIdentity)?;
        if item.model.is_empty()
            || item.model.len() > EmbeddingMetadata::MAX_MODEL_STRING_LEN
            || item.model_version.is_empty()
            || item.model_version.len() > EmbeddingMetadata::MAX_MODEL_STRING_LEN
            || item.embedding_write == 0
        {
            return Err(PublicWireError::InvalidValue);
        }
    }
    Ok(())
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
    ($type:ty, $operation:expr, $maximum:expr, $last:expr, $repeated:expr, $oneof:expr, $validate:expr) => {
        impl PublicMessage for $type {
            const MAX_ENCODED_BYTES: usize = $maximum;
            const APPLICATION_OPERATION: Option<riffdb_errors::ApplicationOperation> = $operation;

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
    Some(riffdb_errors::ApplicationOperation::DescribeContract),
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
    None,
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
    app_v1::GetApplicationCatalogRequest,
    Some(riffdb_errors::ApplicationOperation::DescribeContract),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::GetApplicationCatalogRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)?;
        if value.limit == 0 || value.limit as usize > MAX_APPLICATION_CATALOG_PAGE_ITEMS {
            return Err(PublicWireError::TooManyItems);
        }
        if value
            .cursor
            .as_deref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
        {
            return Err(PublicWireError::InvalidBytes);
        }
        Ok(())
    }
);
app_message!(
    app_v1::GetApplicationCatalogResponse,
    None,
    MAX_PUBLIC_RESPONSE_BYTES,
    9,
    &[5, 6, 7],
    &[],
    |value: &app_v1::GetApplicationCatalogResponse| {
        if value.schema != "riffdb.application-catalog/v1"
            || value.contract_lineage.is_empty()
            || value.contract_lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
            || value.contract_version == 0
            || !valid_hash(&value.contract_bundle_hash)
            || value.query_module_hashes.len() > 1
            || value
                .query_module_hashes
                .iter()
                .any(|hash| !valid_hash(hash))
            || value.symbols.len() > MAX_APPLICATION_CATALOG_PAGE_ITEMS
            || value.features.len() > MAX_APPLICATION_CATALOG_FEATURES
            || value
                .next_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
            || value.has_more != value.next_cursor.is_some()
        {
            return Err(PublicWireError::InvalidBytes);
        }

        let mut prior_symbol: Option<(i32, &[String])> = None;
        for symbol in &value.symbols {
            if symbol.kind == 0
                || app_v1::ApplicationCatalogSymbolKind::try_from(symbol.kind).is_err()
                || symbol.path.is_empty()
                || symbol.path.len() > MAX_APPLICATION_CATALOG_PATH_COMPONENTS
                || symbol.path.iter().any(|part| !valid_name(part))
                || symbol.public_type.as_deref().is_some_and(|public_type| {
                    public_type.is_empty() || public_type.len() > MAX_APPLICATION_CATALOG_TEXT_BYTES
                })
                || symbol
                    .source_span
                    .is_some_and(|span| span.start >= span.end)
            {
                return Err(PublicWireError::InvalidValue);
            }
            if prior_symbol
                .as_ref()
                .is_some_and(|prior| prior >= &(symbol.kind, symbol.path.as_slice()))
            {
                return Err(PublicWireError::NonCanonical);
            }
            prior_symbol = Some((symbol.kind, symbol.path.as_slice()));
        }

        let mut prior_feature = None;
        for feature in &value.features {
            if feature.feature == 0
                || feature.state == 0
                || app_v1::ApplicationCatalogFeature::try_from(feature.feature).is_err()
                || app_v1::ApplicationCatalogFeatureState::try_from(feature.state).is_err()
                || prior_feature.is_some_and(|prior| prior >= feature.feature)
            {
                return Err(PublicWireError::NonCanonical);
            }
            prior_feature = Some(feature.feature);
        }
        Ok(())
    }
);
app_message!(
    app_v1::CheckQueryRequest,
    Some(riffdb_errors::ApplicationOperation::CheckQuery),
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
    None,
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
    Some(riffdb_errors::ApplicationOperation::ExplainQuery),
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
    None,
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
    Some(riffdb_errors::ApplicationOperation::ExecuteQuery),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[5, 8],
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
        validate_parameters(&value.parameters)?;
        match value.accepted_result_encodings.as_slice() {
            [] => {}
            [legacy] if *legacy == app_v1::NamedResultEncoding::LegacyRecords as i32 => {}
            [legacy, compact]
                if *legacy == app_v1::NamedResultEncoding::LegacyRecords as i32
                    && *compact == app_v1::NamedResultEncoding::CompactV1 as i32 => {}
            _ => return Err(PublicWireError::InvalidEnum),
        }
        Ok(())
    }
);
app_message!(
    app_v1::ExecuteQueryResponse,
    None,
    MAX_PUBLIC_RESPONSE_BYTES,
    7,
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
            || value
                .next_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
        {
            return Err(PublicWireError::NonCanonical);
        }
        match app_v1::NamedResultEncoding::try_from(value.selected_result_encoding) {
            Ok(app_v1::NamedResultEncoding::Unspecified)
            | Ok(app_v1::NamedResultEncoding::LegacyRecords)
                if value.compact_result.is_none() =>
            {
                validate_legacy_result_fields(&value.fields)
            }
            Ok(app_v1::NamedResultEncoding::CompactV1) if value.fields.is_empty() => {
                validate_compact_result_field(
                    value
                        .compact_result
                        .as_ref()
                        .ok_or(PublicWireError::MissingRequiredField)?,
                )
            }
            Ok(_) => Err(PublicWireError::InconsistentFields),
            Err(_) => Err(PublicWireError::InvalidEnum),
        }
    }
);

fn validate_legacy_result_fields(fields: &[app_v1::ResultField]) -> Result<(), PublicWireError> {
    if fields.len() > MAX_QUERY_ITEMS || fields.windows(2).any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(PublicWireError::NonCanonical);
    }
    let mut rows = 0usize;
    for field in fields {
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

fn validate_compact_result_field(
    field: &app_v1::CompactResultField,
) -> Result<(), PublicWireError> {
    let cardinality = app_v1::ResultCardinality::try_from(field.cardinality)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if cardinality == app_v1::ResultCardinality::Unspecified
        || !valid_name(&field.name)
        || !valid_name(&field.entity)
        || field.fields.is_empty()
        || field.fields.len() > MAX_QUERY_ITEMS
        || field.fields.iter().any(|name| !valid_name(name))
        || field.rows.len() > MAX_QUERY_ROWS
        || (cardinality == app_v1::ResultCardinality::One && field.rows.len() != 1)
        || (cardinality == app_v1::ResultCardinality::Maybe && field.rows.len() > 1)
    {
        return Err(PublicWireError::InvalidBytes);
    }
    let mut names = BTreeSet::new();
    if field.fields.iter().any(|name| !names.insert(name)) {
        return Err(PublicWireError::NonCanonical);
    }
    for row in &field.rows {
        if row.values.len() != field.fields.len() {
            return Err(PublicWireError::InconsistentFields);
        }
        for value in &row.values {
            validate_value(value).map_err(|_| PublicWireError::InvalidValue)?;
        }
    }
    Ok(())
}

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
    Some(riffdb_errors::ApplicationOperation::DeployQueryModule),
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
    None,
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

fn validate_reactive_module_descriptor(
    value: &app_v1::ReactiveModuleDescriptor,
) -> Result<(), PublicWireError> {
    if !valid_name(&value.module_name)
        || value.module_version == 0
        || !valid_hash(&value.module_hash)
        || value.contract_lineage.is_empty()
        || value.contract_lineage.len() > MAX_CONTRACT_LINEAGE_BYTES
        || value.contract_version == 0
        || !valid_hash(&value.contract_bundle_hash)
        || value.query_module_hashes.len() > MAX_REACTIVE_QUERY_MODULES
        || value
            .query_module_hashes
            .iter()
            .any(|hash| !valid_hash(hash))
        || value
            .query_module_hashes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || value.operation_names.is_empty()
        || value.operation_names.len() > MAX_QUERY_ITEMS
        || value.operation_names.iter().any(|name| !valid_name(name))
        || value
            .operation_names
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

app_message!(
    app_v1::DeployReactiveModuleRequest,
    Some(riffdb_errors::ApplicationOperation::DeployReactiveModule),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[3],
    &[],
    |value: &app_v1::DeployReactiveModuleRequest| {
        validate_request_id(&value.request_id)?;
        if let Some(contract) = value.contract.as_ref() {
            validate_selector(contract)?;
        }
        if value.source.is_empty() || value.source.len() > MAX_REACTIVE_SOURCE_BYTES {
            return Err(PublicWireError::InvalidBytes);
        }
        if value.query_module_hashes.len() > MAX_REACTIVE_QUERY_MODULES
            || value
                .query_module_hashes
                .iter()
                .any(|hash| !valid_hash(hash))
            || value
                .query_module_hashes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(PublicWireError::NonCanonical);
        }
        Ok(())
    }
);
app_message!(
    app_v1::DeployReactiveModuleResponse,
    None,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[],
    |value: &app_v1::DeployReactiveModuleResponse| {
        let outcome = app_v1::ReactiveModuleDeploymentOutcome::try_from(value.outcome)
            .map_err(|_| PublicWireError::InvalidEnum)?;
        if outcome == app_v1::ReactiveModuleDeploymentOutcome::Unspecified {
            return Err(PublicWireError::InvalidEnum);
        }
        validate_reactive_module_descriptor(
            value
                .module
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?;
        let unavailable =
            outcome == app_v1::ReactiveModuleDeploymentOutcome::QueryModuleUnavailable;
        match value.unavailable_query_module_hash.as_deref() {
            Some(hash) if unavailable && valid_hash(hash) => Ok(()),
            None if !unavailable => Ok(()),
            _ => Err(PublicWireError::InconsistentFields),
        }
    }
);
app_message!(
    app_v1::GetQueryModuleRequest,
    Some(riffdb_errors::ApplicationOperation::GetQueryModule),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::GetQueryModuleRequest| {
        validate_request_id(&value.request_id)?;
        if let Some(contract) = value.contract.as_ref() {
            validate_selector(contract)?;
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
    app_v1::GetQueryModuleResponse,
    None,
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
app_message!(
    app_v1::ExecuteProjectedQueryRequest,
    Some(riffdb_errors::ApplicationOperation::ExecuteProjectedQuery),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::ExecuteProjectedQueryRequest| {
        validate_request_id(&value.request_id)?;
        value.contract.as_ref().map_or(Ok(()), validate_selector)?;
        if !valid_name(&value.projection_name) {
            return Err(PublicWireError::InvalidBytes);
        }
        if value.request.is_none() {
            return Err(PublicWireError::MissingRequiredField);
        }
        Ok(())
    }
);
app_message!(
    app_v1::ExecuteProjectedQueryResponse,
    None,
    MAX_PUBLIC_RESPONSE_BYTES,
    8,
    &[],
    &[1, 2, 3, 4, 5, 6, 7, 8],
    |value: &app_v1::ExecuteProjectedQueryResponse| {
        if value.outcome.is_none() {
            return Err(PublicWireError::MissingRequiredField);
        }
        Ok(())
    }
);
app_message!(
    app_v1::InspectVectorStateRequest,
    Some(riffdb_errors::ApplicationOperation::InspectVectorState),
    MAX_PUBLIC_REQUEST_BYTES,
    100,
    &[],
    &[],
    |value: &app_v1::InspectVectorStateRequest| {
        validate_request_id(&value.request_id)?;
        if let Some(contract) = value.contract.as_ref() {
            validate_selector(contract)?;
        }
        if !valid_name(&value.entity) || !valid_name(&value.field) {
            return Err(PublicWireError::InvalidBytes);
        }
        validate_value(
            value
                .partition
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        match app_v1::VectorStateInspectionKind::try_from(value.kind) {
            Ok(app_v1::VectorStateInspectionKind::StaleEntities)
            | Ok(app_v1::VectorStateInspectionKind::OutdatedModelEntities) => {}
            Ok(app_v1::VectorStateInspectionKind::Unspecified) | Err(_) => {
                return Err(PublicWireError::InvalidValue);
            }
        }
        validate_vector_page_request(
            value
                .page
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
    }
);
app_message!(
    app_v1::InspectVectorStateResponse,
    None,
    MAX_PUBLIC_RESPONSE_BYTES,
    4,
    &[],
    &[1, 2, 3, 4],
    |value: &app_v1::InspectVectorStateResponse| {
        match value
            .result
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
        {
            app_v1::inspect_vector_state_response::Result::StalenessSummary(report) => {
                if report.stale_count > report.total_entities
                    || report.stale_entity_count_threshold == 0
                    || report.slo_breached
                        != (report.stale_count > report.stale_entity_count_threshold)
                {
                    return Err(PublicWireError::InconsistentFields);
                }
                Ok(())
            }
            app_v1::inspect_vector_state_response::Result::StaleEntities(page) => {
                validate_vector_staleness_page(page)
            }
            app_v1::inspect_vector_state_response::Result::ModelVersionSummary(_) => Ok(()),
            app_v1::inspect_vector_state_response::Result::OutdatedModelEntities(page) => {
                validate_vector_model_page(page)
            }
        }
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    fn selector() -> app_v1::ContractSelector {
        app_v1::ContractSelector {
            lineage: "ReactiveBoundary".to_owned(),
            version: 1,
            bundle_hash: vec![0x11; 32],
        }
    }

    fn descriptor() -> app_v1::ReactiveModuleDescriptor {
        app_v1::ReactiveModuleDescriptor {
            module_name: "Activity".to_owned(),
            module_version: 1,
            module_hash: vec![0x44; 32],
            contract_lineage: "ReactiveBoundary".to_owned(),
            contract_version: 1,
            contract_bundle_hash: vec![0x11; 32],
            query_module_hashes: vec![vec![0x22; 32], vec![0x33; 32]],
            operation_names: vec!["ActivityStream".to_owned()],
        }
    }

    #[test]
    fn reactive_publication_requires_canonical_exact_dependencies() {
        let request = app_v1::DeployReactiveModuleRequest {
            contract: Some(selector()),
            source: "reactive Activity version 1 {}".to_owned(),
            query_module_hashes: vec![vec![0x22; 32], vec![0x33; 32]],
            request_id: vec![0x77; 16],
        };
        assert_eq!(request.validate_structure(), Ok(()));

        let mut reversed = request.clone();
        reversed.query_module_hashes.reverse();
        assert_eq!(
            reversed.validate_structure(),
            Err(PublicWireError::NonCanonical)
        );
        let mut duplicate = request;
        duplicate.query_module_hashes[1] = duplicate.query_module_hashes[0].clone();
        assert_eq!(
            duplicate.validate_structure(),
            Err(PublicWireError::NonCanonical)
        );
    }

    #[test]
    fn reactive_publication_outcome_binds_unavailable_dependency_evidence() {
        let published = app_v1::DeployReactiveModuleResponse {
            outcome: app_v1::ReactiveModuleDeploymentOutcome::Published as i32,
            module: Some(descriptor()),
            unavailable_query_module_hash: None,
        };
        assert_eq!(published.validate_structure(), Ok(()));

        let mut missing = published.clone();
        missing.outcome = app_v1::ReactiveModuleDeploymentOutcome::QueryModuleUnavailable as i32;
        assert_eq!(
            missing.validate_structure(),
            Err(PublicWireError::InconsistentFields)
        );
        missing.unavailable_query_module_hash = Some(vec![0x22; 32]);
        assert_eq!(missing.validate_structure(), Ok(()));

        let mut impossible = published;
        impossible.unavailable_query_module_hash = Some(vec![0x22; 32]);
        assert_eq!(
            impossible.validate_structure(),
            Err(PublicWireError::InconsistentFields)
        );
    }

    #[test]
    fn application_catalog_wire_shape_is_bounded_closed_and_canonical() {
        let request = app_v1::GetApplicationCatalogRequest {
            contract: Some(selector()),
            limit: 100,
            cursor: None,
            request_id: vec![0x77; 16],
        };
        assert_eq!(request.validate_structure(), Ok(()));
        let mut zero_limit = request;
        zero_limit.limit = 0;
        assert_eq!(
            zero_limit.validate_structure(),
            Err(PublicWireError::TooManyItems)
        );

        let response = app_v1::GetApplicationCatalogResponse {
            schema: "riffdb.application-catalog/v1".to_owned(),
            contract_lineage: "ReactiveBoundary".to_owned(),
            contract_version: 1,
            contract_bundle_hash: vec![0x11; 32],
            query_module_hashes: Vec::new(),
            symbols: vec![app_v1::ApplicationCatalogSymbol {
                kind: app_v1::ApplicationCatalogSymbolKind::Contract as i32,
                path: vec!["ReactiveBoundary".to_owned()],
                public_type: None,
                source_span: Some(app_v1::ApplicationCatalogSourceSpan { start: 1, end: 2 }),
            }],
            features: vec![app_v1::ApplicationCatalogFeatureView {
                feature: app_v1::ApplicationCatalogFeature::StableCursorPages as i32,
                state: app_v1::ApplicationCatalogFeatureState::Available as i32,
            }],
            has_more: false,
            next_cursor: None,
        };
        assert_eq!(response.validate_structure(), Ok(()));

        let mut unspecified = response.clone();
        unspecified.symbols[0].kind = 0;
        assert_eq!(
            unspecified.validate_structure(),
            Err(PublicWireError::InvalidValue)
        );
        let mut empty_span = response;
        empty_span.symbols[0].source_span =
            Some(app_v1::ApplicationCatalogSourceSpan { start: 2, end: 2 });
        assert_eq!(
            empty_span.validate_structure(),
            Err(PublicWireError::InvalidValue)
        );
    }

    #[test]
    fn compact_named_result_negotiation_is_closed_and_width_checked() {
        let mut request = app_v1::ExecuteQueryRequest {
            contract: Some(selector()),
            module_hash: None,
            parameters: Vec::new(),
            cursor: None,
            minimum_application_head: None,
            accepted_result_encodings: vec![
                app_v1::NamedResultEncoding::LegacyRecords as i32,
                app_v1::NamedResultEncoding::CompactV1 as i32,
            ],
            request_id: vec![0x77; 16],
            query: Some(app_v1::execute_query_request::Query::QueryName(
                "BoardPage".to_owned(),
            )),
        };
        assert_eq!(request.validate_structure(), Ok(()));
        request.accepted_result_encodings.reverse();
        assert_eq!(
            request.validate_structure(),
            Err(PublicWireError::InvalidEnum)
        );

        let identity = app_v1::QueryIdentity {
            contract_lineage: "ReactiveBoundary".to_owned(),
            contract_version: 1,
            contract_bundle_hash: vec![0x11; 32],
            query_name: Some("BoardPage".to_owned()),
            plan_hash: vec![0x22; 32],
            module_hash: Some(vec![0x33; 32]),
        };
        let mut response = app_v1::ExecuteQueryResponse {
            identity: Some(identity),
            outcome: "Found".to_owned(),
            application_head: 9,
            fields: Vec::new(),
            next_cursor: None,
            selected_result_encoding: app_v1::NamedResultEncoding::CompactV1 as i32,
            compact_result: Some(app_v1::CompactResultField {
                name: "tickets".to_owned(),
                cardinality: app_v1::ResultCardinality::Many as i32,
                entity: "Ticket".to_owned(),
                fields: vec!["ticket_id".to_owned(), "title".to_owned()],
                rows: vec![app_v1::CompactResultRow {
                    values: vec![
                        crate::v1::Value {
                            kind: Some(crate::v1::value::Kind::UuidValue(vec![0x44; 16])),
                        },
                        crate::v1::Value {
                            kind: Some(crate::v1::value::Kind::StringValue("covered".to_owned())),
                        },
                    ],
                }],
            }),
        };
        assert_eq!(response.validate_structure(), Ok(()));
        response.fields.push(app_v1::ResultField {
            name: "tickets".to_owned(),
            cardinality: app_v1::ResultCardinality::Many as i32,
            records: Vec::new(),
        });
        assert_eq!(
            response.validate_structure(),
            Err(PublicWireError::InconsistentFields)
        );
        response.fields.clear();
        response.compact_result.as_mut().expect("compact").rows[0]
            .values
            .pop();
        assert_eq!(
            response.validate_structure(),
            Err(PublicWireError::InconsistentFields)
        );
    }

    #[test]
    fn vector_inspection_wire_shape_is_symbolic_bounded_and_closed() {
        let mut request = app_v1::InspectVectorStateRequest {
            contract: Some(selector()),
            entity: "Document".to_owned(),
            field: "embedding".to_owned(),
            partition: Some(crate::v1::Value {
                kind: Some(crate::v1::value::Kind::StringValue("org-a".to_owned())),
            }),
            kind: app_v1::VectorStateInspectionKind::StaleEntities as i32,
            page: Some(crate::v1::PageRequest {
                limit: Some(20),
                cursor: None,
            }),
            request_id: vec![0x77; 16],
        };
        assert_eq!(request.validate_structure(), Ok(()));
        request.contract = None;
        assert_eq!(
            request.validate_structure(),
            Ok(()),
            "an absent selector means the active contract"
        );
        request.page.as_mut().expect("page").limit = Some(501);
        assert_eq!(
            request.validate_structure(),
            Err(PublicWireError::InvalidValue)
        );

        let response = app_v1::InspectVectorStateResponse {
            result: Some(
                app_v1::inspect_vector_state_response::Result::StalenessSummary(
                    app_v1::VectorStalenessReport {
                        total_entities: 8,
                        stale_count: 3,
                        stale_entity_count_threshold: 2,
                        slo_breached: true,
                    },
                ),
            ),
        };
        assert_eq!(response.validate_structure(), Ok(()));
        let mut inconsistent = response;
        let Some(app_v1::inspect_vector_state_response::Result::StalenessSummary(report)) =
            inconsistent.result.as_mut()
        else {
            panic!("summary");
        };
        report.slo_breached = false;
        assert_eq!(
            inconsistent.validate_structure(),
            Err(PublicWireError::InconsistentFields)
        );
    }
}

#[cfg(test)]
mod ready_packed_preflight_pins {
    use super::*;
    use crate::public_message::decode_public_message;

    /// Review pin: the hand-registered oneof arm 7 (ready_packed) must keep
    /// duplicate-arm rejection — two arm-7 occurrences are one arm too many.
    #[test]
    fn duplicate_ready_packed_arm_is_rejected() {
        // field 7, wire type 2 (LEN), empty payload — twice.
        let bytes = [0x3a, 0x00, 0x3a, 0x00];
        assert!(
            decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes).is_err(),
            "duplicate ready_packed arms must fail preflight"
        );
    }

    /// Review pin: ready (field 1) plus ready_packed (field 7) is two oneof
    /// arms in one message — exclusivity must hold across the hand-edit.
    #[test]
    fn ready_and_ready_packed_together_are_rejected() {
        let bytes = [0x0a, 0x00, 0x3a, 0x00];
        assert!(
            decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes).is_err(),
            "two outcome arms must fail preflight"
        );
    }

    /// Review pin mirroring `duplicate_ready_packed_arm_is_rejected` for the
    /// hand-registered aggregate arm 8: two arm-8 occurrences are one too many.
    /// This only holds while `maximum_known_field` covers field 8.
    #[test]
    fn duplicate_ready_aggregates_arm_is_rejected() {
        // field 8, wire type 2 (LEN), empty payload — twice.
        let bytes = [0x42, 0x00, 0x42, 0x00];
        assert!(
            decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes).is_err(),
            "duplicate ready_aggregates arms must fail preflight"
        );
    }

    /// Review pin: ready_aggregates (field 8) is exclusive with every other
    /// outcome arm, including the two Ready-shaped ones.
    #[test]
    fn ready_aggregates_with_any_other_arm_is_rejected() {
        for other in [0x0a_u8, 0x12, 0x1a, 0x22, 0x2a, 0x32, 0x3a] {
            let bytes = [other, 0x00, 0x42, 0x00];
            assert!(
                decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes).is_err(),
                "arm {other:#04x} together with ready_aggregates must fail preflight"
            );
        }
    }

    /// A lone aggregate arm still decodes: the exclusivity pins above must not
    /// be passing because field 8 is rejected outright.
    #[test]
    fn lone_ready_aggregates_arm_decodes() {
        let bytes = [0x42, 0x00];
        assert!(
            decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes).is_ok(),
            "a single ready_aggregates arm must decode"
        );
    }
}

#[cfg(test)]
mod vector_inspection_preflight_pins {
    use super::*;
    use crate::public_message::decode_public_message;

    #[test]
    fn duplicate_vector_result_arms_are_rejected() {
        // staleness_summary (field 1) plus model_version_summary (field 3).
        let bytes = [0x0a, 0x00, 0x1a, 0x00];
        assert!(
            decode_public_message::<app_v1::InspectVectorStateResponse>(&bytes).is_err(),
            "multiple result arms must fail before prost merge"
        );
    }
}
