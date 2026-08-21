//! Exact mapping for the bounded application-semantic error boundary.

use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_errors::{
    APPLICATION_ERROR_ENVELOPE_VERSION, ApplicationError as DomainApplicationError,
    ApplicationErrorCategory as DomainCategory, ApplicationErrorCode as DomainCode,
    ApplicationErrorContext as DomainContext, ApplicationFixCode as DomainFix,
    ApplicationOperation as DomainOperation, ApplicationRecoveryAction as DomainRecovery,
    ApplicationSourceSpan as DomainSpan, MAX_APPLICATION_ERROR_BYTES, MAX_APPLICATION_FIXES,
    MAX_APPLICATION_SYMBOL_PATH_SEGMENTS,
};
use riffdb_types::{ContractLineage, ContractVersion, IncidentId, RequestId};

use crate::app::v1 as app_v1;
use crate::wire::{self, Cursor, PreflightError};

/// Encodes one checked application error.
#[must_use]
pub fn application_error_to_proto(error: &DomainApplicationError) -> app_v1::ApplicationError {
    let contract = error.context().contract();
    app_v1::ApplicationError {
        envelope_version: APPLICATION_ERROR_ENVELOPE_VERSION,
        code: proto_code(error.code()) as i32,
        category: proto_category(error.category()) as i32,
        recovery_action: proto_recovery(error.recovery_action()) as i32,
        operation: proto_operation(error.operation()) as i32,
        contract_lineage: contract.map(|(lineage, _)| lineage.as_str().to_owned()),
        contract_version: contract.map(|(_, version)| version.get()),
        operation_symbol: error.context().operation_symbol().map(str::to_owned),
        symbol_path: error.context().symbol_path().to_vec(),
        source_span: error
            .context()
            .source_span()
            .map(|span| app_v1::SourceSpan {
                start: span.start(),
                end: span.end(),
            }),
        fixes: error
            .fixes()
            .iter()
            .copied()
            .map(|fix| proto_fix(fix) as i32)
            .collect(),
        trace_id: error
            .context()
            .trace_id()
            .map(|trace_id| trace_id.as_bytes().to_vec()),
        incident_id: error
            .incident_id()
            .map(|incident_id| incident_id.as_bytes().to_vec()),
    }
}

/// Encodes the canonical direct gRPC details payload.
#[must_use]
pub fn encode_application_error(error: &DomainApplicationError) -> Vec<u8> {
    application_error_to_proto(error).encode_to_vec()
}

/// Decodes a complete direct gRPC details payload.
pub fn decode_application_error(
    input: &[u8],
) -> Result<DomainApplicationError, ApplicationErrorWireError> {
    if input.len() > MAX_APPLICATION_ERROR_BYTES {
        return Err(ApplicationErrorWireError::MessageTooLarge);
    }
    preflight_application_error(input)?;
    let wire = app_v1::ApplicationError::decode(input)
        .map_err(|_| ApplicationErrorWireError::MalformedEncoding)?;
    application_error_from_proto(&wire)
}

/// Allocation-free structural preflight for one application-error wire payload.
pub fn preflight_application_error(input: &[u8]) -> Result<(), ApplicationErrorWireError> {
    preflight(input)
}

/// Validates redundant registry fields and reconstructs one domain error.
pub fn application_error_from_proto(
    wire: &app_v1::ApplicationError,
) -> Result<DomainApplicationError, ApplicationErrorWireError> {
    if wire.encoded_len() > MAX_APPLICATION_ERROR_BYTES {
        return Err(ApplicationErrorWireError::MessageTooLarge);
    }
    let code = domain_code(wire.code)?;
    if wire.envelope_version != APPLICATION_ERROR_ENVELOPE_VERSION
        || wire.category != proto_category(code.category()) as i32
        || wire.recovery_action != proto_recovery(code.recovery_action()) as i32
    {
        return Err(ApplicationErrorWireError::InconsistentRegistry);
    }
    let operation = domain_operation(wire.operation)?;
    if wire.fixes.len() > MAX_APPLICATION_FIXES
        || wire
            .fixes
            .iter()
            .copied()
            .map(domain_fix)
            .collect::<Result<Vec<_>, _>>()?
            != code.fixes()
    {
        return Err(ApplicationErrorWireError::InconsistentRegistry);
    }

    let mut context = DomainContext::empty();
    match (&wire.contract_lineage, wire.contract_version) {
        (Some(lineage), Some(version)) => {
            context = context.with_contract(
                ContractLineage::new(lineage.clone())
                    .map_err(|_| ApplicationErrorWireError::InvalidContext)?,
                ContractVersion::new(version).ok_or(ApplicationErrorWireError::InvalidContext)?,
            );
        }
        (None, None) => {}
        _ => return Err(ApplicationErrorWireError::InvalidContext),
    }
    if let Some(symbol) = &wire.operation_symbol {
        context = context
            .with_operation_symbol(symbol.clone())
            .map_err(|_| ApplicationErrorWireError::InvalidContext)?;
    }
    context = context
        .with_symbol_path(wire.symbol_path.clone())
        .map_err(|_| ApplicationErrorWireError::InvalidContext)?;
    if let Some(span) = &wire.source_span {
        context = context.with_source_span(
            DomainSpan::new(span.start, span.end)
                .ok_or(ApplicationErrorWireError::InvalidContext)?,
        );
    }
    if let Some(trace_id) = wire.trace_id.as_deref() {
        context = context.with_trace_id(parse_request_id(trace_id)?);
    }
    let incident_id = wire
        .incident_id
        .as_deref()
        .map(parse_incident_id)
        .transpose()?;
    if code == DomainCode::InternalDefect && incident_id.is_none() {
        return Err(ApplicationErrorWireError::MissingInternalIncident);
    }
    Ok(DomainApplicationError::new(
        code,
        operation,
        context,
        incident_id,
    ))
}

fn preflight(input: &[u8]) -> Result<(), ApplicationErrorWireError> {
    match wire::bounded_message(input, MAX_APPLICATION_ERROR_BYTES) {
        Ok(()) => {}
        Err(PreflightError::Malformed) => {
            return Err(ApplicationErrorWireError::MalformedEncoding);
        }
        Err(PreflightError::LimitExceeded) => {
            return Err(ApplicationErrorWireError::PreflightLimitExceeded);
        }
    }
    let mut cursor = Cursor::new(input);
    let mut seen = [false; 14];
    let mut symbol_count = 0usize;
    let mut fix_count = 0usize;
    while let Some(field) = cursor
        .next()
        .map_err(|_| ApplicationErrorWireError::MalformedEncoding)?
    {
        let index =
            usize::try_from(field.number).map_err(|_| ApplicationErrorWireError::UnknownField)?;
        if !(1..=13).contains(&index) {
            return Err(ApplicationErrorWireError::UnknownField);
        }
        if field.number == 9 {
            symbol_count = symbol_count.saturating_add(1);
            if symbol_count > MAX_APPLICATION_SYMBOL_PATH_SEGMENTS {
                return Err(ApplicationErrorWireError::PreflightLimitExceeded);
            }
        } else if field.number == 11 {
            fix_count = fix_count.saturating_add(1);
            if fix_count > MAX_APPLICATION_FIXES {
                return Err(ApplicationErrorWireError::PreflightLimitExceeded);
            }
        } else if std::mem::replace(&mut seen[index], true) {
            return Err(ApplicationErrorWireError::DuplicateField);
        }
    }
    Ok(())
}

fn parse_request_id(bytes: &[u8]) -> Result<RequestId, ApplicationErrorWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ApplicationErrorWireError::InvalidContext)?;
    RequestId::from_bytes(bytes).map_err(|_| ApplicationErrorWireError::InvalidContext)
}

fn parse_incident_id(bytes: &[u8]) -> Result<IncidentId, ApplicationErrorWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ApplicationErrorWireError::InvalidContext)?;
    IncidentId::from_bytes(bytes).map_err(|_| ApplicationErrorWireError::InvalidContext)
}

const fn proto_operation(value: DomainOperation) -> app_v1::ApplicationOperation {
    match value {
        DomainOperation::DescribeContract => app_v1::ApplicationOperation::DescribeContract,
        DomainOperation::CheckQuery => app_v1::ApplicationOperation::CheckQuery,
        DomainOperation::ExplainQuery => app_v1::ApplicationOperation::ExplainQuery,
        DomainOperation::ExecuteQuery => app_v1::ApplicationOperation::ExecuteQuery,
        DomainOperation::DeployQueryModule => app_v1::ApplicationOperation::DeployQueryModule,
        DomainOperation::DeployReactiveModule => app_v1::ApplicationOperation::DeployReactiveModule,
        DomainOperation::GetQueryModule => app_v1::ApplicationOperation::GetQueryModule,
        DomainOperation::ExecuteCommand => app_v1::ApplicationOperation::ExecuteCommand,
        DomainOperation::BatchCommand => app_v1::ApplicationOperation::BatchCommand,
        DomainOperation::ExecuteProjectedQuery => {
            app_v1::ApplicationOperation::ExecuteProjectedQuery
        }
        DomainOperation::InspectVectorState => app_v1::ApplicationOperation::InspectVectorState,
    }
}

fn domain_operation(value: i32) -> Result<DomainOperation, ApplicationErrorWireError> {
    match app_v1::ApplicationOperation::try_from(value) {
        Ok(app_v1::ApplicationOperation::DescribeContract) => Ok(DomainOperation::DescribeContract),
        Ok(app_v1::ApplicationOperation::CheckQuery) => Ok(DomainOperation::CheckQuery),
        Ok(app_v1::ApplicationOperation::ExplainQuery) => Ok(DomainOperation::ExplainQuery),
        Ok(app_v1::ApplicationOperation::ExecuteQuery) => Ok(DomainOperation::ExecuteQuery),
        Ok(app_v1::ApplicationOperation::DeployQueryModule) => {
            Ok(DomainOperation::DeployQueryModule)
        }
        Ok(app_v1::ApplicationOperation::DeployReactiveModule) => {
            Ok(DomainOperation::DeployReactiveModule)
        }
        Ok(app_v1::ApplicationOperation::GetQueryModule) => Ok(DomainOperation::GetQueryModule),
        Ok(app_v1::ApplicationOperation::ExecuteCommand) => Ok(DomainOperation::ExecuteCommand),
        Ok(app_v1::ApplicationOperation::BatchCommand) => Ok(DomainOperation::BatchCommand),
        Ok(app_v1::ApplicationOperation::ExecuteProjectedQuery) => {
            Ok(DomainOperation::ExecuteProjectedQuery)
        }
        Ok(app_v1::ApplicationOperation::InspectVectorState) => {
            Ok(DomainOperation::InspectVectorState)
        }
        Ok(app_v1::ApplicationOperation::Unspecified) | Err(_) => {
            Err(ApplicationErrorWireError::UnknownOperation)
        }
    }
}

const fn proto_code(value: DomainCode) -> app_v1::ApplicationErrorCode {
    match value {
        DomainCode::InvalidRequest => app_v1::ApplicationErrorCode::InvalidRequest,
        DomainCode::InputInvalid => app_v1::ApplicationErrorCode::InputInvalid,
        DomainCode::AuthorizationDenied => app_v1::ApplicationErrorCode::AuthorizationDenied,
        DomainCode::ContractMismatch => app_v1::ApplicationErrorCode::ContractMismatch,
        DomainCode::QueryInvalid => app_v1::ApplicationErrorCode::QueryInvalid,
        DomainCode::QueryUnavailable => app_v1::ApplicationErrorCode::QueryUnavailable,
        DomainCode::ModuleUnavailable => app_v1::ApplicationErrorCode::ModuleUnavailable,
        DomainCode::CursorInvalid => app_v1::ApplicationErrorCode::CursorInvalid,
        DomainCode::ResponseTooLarge => app_v1::ApplicationErrorCode::ResponseTooLarge,
        DomainCode::StorageUnavailable => app_v1::ApplicationErrorCode::StorageUnavailable,
        DomainCode::OutcomeUnknown => app_v1::ApplicationErrorCode::OutcomeUnknown,
        DomainCode::OperationCancelled => app_v1::ApplicationErrorCode::OperationCancelled,
        DomainCode::DeadlineExceeded => app_v1::ApplicationErrorCode::DeadlineExceeded,
        DomainCode::InternalDefect => app_v1::ApplicationErrorCode::InternalDefect,
        DomainCode::IdempotencyKeyReuse => app_v1::ApplicationErrorCode::IdempotencyKeyReuse,
        DomainCode::CommandExecutionFailed => app_v1::ApplicationErrorCode::CommandExecutionFailed,
        DomainCode::CapabilityRevoked => app_v1::ApplicationErrorCode::CapabilityRevoked,
        DomainCode::ProtocolInvalid => app_v1::ApplicationErrorCode::ProtocolInvalid,
        DomainCode::HistoryIncarnationMismatch => {
            app_v1::ApplicationErrorCode::HistoryIncarnationMismatch
        }
        DomainCode::HistoryPruned => app_v1::ApplicationErrorCode::HistoryPruned,
        DomainCode::Overloaded => app_v1::ApplicationErrorCode::Overloaded,
        DomainCode::ProjectionDiverged => app_v1::ApplicationErrorCode::ProjectionDiverged,
        DomainCode::SnapshotRetired => app_v1::ApplicationErrorCode::SnapshotRetired,
        DomainCode::FreshnessUnsatisfied => app_v1::ApplicationErrorCode::FreshnessUnsatisfied,
        DomainCode::ProjectedSourceRequired => {
            app_v1::ApplicationErrorCode::ProjectedSourceRequired
        }
    }
}

fn domain_code(value: i32) -> Result<DomainCode, ApplicationErrorWireError> {
    use app_v1::ApplicationErrorCode as Wire;
    match Wire::try_from(value) {
        Ok(Wire::InvalidRequest) => Ok(DomainCode::InvalidRequest),
        Ok(Wire::InputInvalid) => Ok(DomainCode::InputInvalid),
        Ok(Wire::AuthorizationDenied) => Ok(DomainCode::AuthorizationDenied),
        Ok(Wire::ContractMismatch) => Ok(DomainCode::ContractMismatch),
        Ok(Wire::QueryInvalid) => Ok(DomainCode::QueryInvalid),
        Ok(Wire::QueryUnavailable) => Ok(DomainCode::QueryUnavailable),
        Ok(Wire::ModuleUnavailable) => Ok(DomainCode::ModuleUnavailable),
        Ok(Wire::CursorInvalid) => Ok(DomainCode::CursorInvalid),
        Ok(Wire::ResponseTooLarge) => Ok(DomainCode::ResponseTooLarge),
        Ok(Wire::StorageUnavailable) => Ok(DomainCode::StorageUnavailable),
        Ok(Wire::OutcomeUnknown) => Ok(DomainCode::OutcomeUnknown),
        Ok(Wire::OperationCancelled) => Ok(DomainCode::OperationCancelled),
        Ok(Wire::DeadlineExceeded) => Ok(DomainCode::DeadlineExceeded),
        Ok(Wire::InternalDefect) => Ok(DomainCode::InternalDefect),
        Ok(Wire::IdempotencyKeyReuse) => Ok(DomainCode::IdempotencyKeyReuse),
        Ok(Wire::CommandExecutionFailed) => Ok(DomainCode::CommandExecutionFailed),
        Ok(Wire::CapabilityRevoked) => Ok(DomainCode::CapabilityRevoked),
        Ok(Wire::ProtocolInvalid) => Ok(DomainCode::ProtocolInvalid),
        Ok(Wire::HistoryIncarnationMismatch) => Ok(DomainCode::HistoryIncarnationMismatch),
        Ok(Wire::HistoryPruned) => Ok(DomainCode::HistoryPruned),
        Ok(Wire::Overloaded) => Ok(DomainCode::Overloaded),
        Ok(Wire::ProjectionDiverged) => Ok(DomainCode::ProjectionDiverged),
        Ok(Wire::SnapshotRetired) => Ok(DomainCode::SnapshotRetired),
        Ok(Wire::FreshnessUnsatisfied) => Ok(DomainCode::FreshnessUnsatisfied),
        Ok(Wire::ProjectedSourceRequired) => Ok(DomainCode::ProjectedSourceRequired),
        Ok(Wire::Unspecified) | Err(_) => Err(ApplicationErrorWireError::UnknownCode),
    }
}

const fn proto_category(value: DomainCategory) -> app_v1::ApplicationErrorCategory {
    match value {
        DomainCategory::Input => app_v1::ApplicationErrorCategory::Input,
        DomainCategory::Authorization => app_v1::ApplicationErrorCategory::Authorization,
        DomainCategory::Contract => app_v1::ApplicationErrorCategory::Contract,
        DomainCategory::Query => app_v1::ApplicationErrorCategory::Query,
        DomainCategory::Module => app_v1::ApplicationErrorCategory::Module,
        DomainCategory::Cursor => app_v1::ApplicationErrorCategory::Cursor,
        DomainCategory::Resource => app_v1::ApplicationErrorCategory::Resource,
        DomainCategory::Storage => app_v1::ApplicationErrorCategory::Storage,
        DomainCategory::Uncertainty => app_v1::ApplicationErrorCategory::Uncertainty,
        DomainCategory::Internal => app_v1::ApplicationErrorCategory::Internal,
        DomainCategory::Protocol => app_v1::ApplicationErrorCategory::Protocol,
        DomainCategory::Command => app_v1::ApplicationErrorCategory::Command,
        DomainCategory::Control => app_v1::ApplicationErrorCategory::Control,
        DomainCategory::History => app_v1::ApplicationErrorCategory::History,
        DomainCategory::Capacity => app_v1::ApplicationErrorCategory::Capacity,
    }
}

const fn proto_recovery(value: DomainRecovery) -> app_v1::ApplicationRecoveryAction {
    match value {
        DomainRecovery::CorrectRequest => app_v1::ApplicationRecoveryAction::CorrectRequest,
        DomainRecovery::Retry => app_v1::ApplicationRecoveryAction::Retry,
        DomainRecovery::ResolveWithSameIdempotencyKey => {
            app_v1::ApplicationRecoveryAction::ResolveWithSameIdempotencyKey
        }
        DomainRecovery::ObtainPermission => app_v1::ApplicationRecoveryAction::ObtainPermission,
        DomainRecovery::RefreshContract => app_v1::ApplicationRecoveryAction::RefreshContract,
        DomainRecovery::ContactOperator => app_v1::ApplicationRecoveryAction::ContactOperator,
        DomainRecovery::None => app_v1::ApplicationRecoveryAction::None,
    }
}

const fn proto_fix(value: DomainFix) -> app_v1::ApplicationFixCode {
    match value {
        DomainFix::CorrectInput => app_v1::ApplicationFixCode::CorrectInput,
        DomainFix::RemoveForbiddenOutput => app_v1::ApplicationFixCode::RemoveForbiddenOutput,
        DomainFix::BindApplicationRole => app_v1::ApplicationFixCode::BindApplicationRole,
        DomainFix::RefreshContract => app_v1::ApplicationFixCode::RefreshContract,
        DomainFix::PinActiveModule => app_v1::ApplicationFixCode::PinActiveModule,
        DomainFix::AddBoundedIndex => app_v1::ApplicationFixCode::AddBoundedIndex,
        DomainFix::RestartFromFirstPage => app_v1::ApplicationFixCode::RestartFromFirstPage,
        DomainFix::RetryLater => app_v1::ApplicationFixCode::RetryLater,
        DomainFix::ResolveWithSameIdempotencyKey => {
            app_v1::ApplicationFixCode::ResolveWithSameIdempotencyKey
        }
        DomainFix::ContactOperatorWithIncident => {
            app_v1::ApplicationFixCode::ContactOperatorWithIncident
        }
    }
}

fn domain_fix(value: i32) -> Result<DomainFix, ApplicationErrorWireError> {
    use app_v1::ApplicationFixCode as Wire;
    match Wire::try_from(value) {
        Ok(Wire::CorrectInput) => Ok(DomainFix::CorrectInput),
        Ok(Wire::RemoveForbiddenOutput) => Ok(DomainFix::RemoveForbiddenOutput),
        Ok(Wire::BindApplicationRole) => Ok(DomainFix::BindApplicationRole),
        Ok(Wire::RefreshContract) => Ok(DomainFix::RefreshContract),
        Ok(Wire::PinActiveModule) => Ok(DomainFix::PinActiveModule),
        Ok(Wire::AddBoundedIndex) => Ok(DomainFix::AddBoundedIndex),
        Ok(Wire::RestartFromFirstPage) => Ok(DomainFix::RestartFromFirstPage),
        Ok(Wire::RetryLater) => Ok(DomainFix::RetryLater),
        Ok(Wire::ResolveWithSameIdempotencyKey) => Ok(DomainFix::ResolveWithSameIdempotencyKey),
        Ok(Wire::ContactOperatorWithIncident) => Ok(DomainFix::ContactOperatorWithIncident),
        Ok(Wire::Unspecified) | Err(_) => Err(ApplicationErrorWireError::UnknownFix),
    }
}

/// Closed, non-secret rejection reason for application-error wire bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationErrorWireError {
    /// The encoded message exceeds 16 KiB.
    MessageTooLarge,
    /// Protobuf encoding is malformed.
    MalformedEncoding,
    /// A nested length or collection preflight bound was exceeded.
    PreflightLimitExceeded,
    /// An unrecognized field tag was present.
    UnknownField,
    /// A singular field appeared more than once.
    DuplicateField,
    /// The stable code is absent or unknown.
    UnknownCode,
    /// The operation is absent or unknown.
    UnknownOperation,
    /// One fix code is absent or unknown.
    UnknownFix,
    /// Redundant code/category/recovery/fix values disagree.
    InconsistentRegistry,
    /// A symbolic, span, contract, trace, or incident value is invalid.
    InvalidContext,
    /// An internal error omitted its required incident identity.
    MissingInternalIncident,
}

impl fmt::Display for ApplicationErrorWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application error wire message is invalid")
    }
}

impl Error for ApplicationErrorWireError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_id() -> RequestId {
        RequestId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("valid UUIDv7")
    }

    fn incident_id() -> IncidentId {
        IncidentId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x24,
        ])
        .expect("valid UUIDv7")
    }

    #[test]
    fn symbolic_application_error_round_trips_exactly() {
        let context = DomainContext::empty()
            .with_contract(
                ContractLineage::new("ticketdesk").expect("lineage"),
                ContractVersion::new(18).expect("version"),
            )
            .with_operation_symbol("TicketPage".to_owned())
            .expect("symbol")
            .with_symbol_path(vec![
                "Ticket".to_owned(),
                "requester".to_owned(),
                "User.email".to_owned(),
            ])
            .expect("path")
            .with_source_span(DomainSpan::new(20, 44).expect("span"))
            .with_trace_id(request_id());
        let error = DomainApplicationError::new(
            DomainCode::AuthorizationDenied,
            DomainOperation::ExecuteQuery,
            context,
            None,
        );
        let wire = application_error_to_proto(&error);
        assert_eq!(application_error_from_proto(&wire), Ok(error.clone()));
        assert_eq!(decode_application_error(&wire.encode_to_vec()), Ok(error));
    }

    #[test]
    fn complete_application_error_registry_round_trips_without_fallback() {
        for code in riffdb_errors::APPLICATION_ERROR_CODES {
            let incident = (code == DomainCode::InternalDefect).then(incident_id);
            let error = DomainApplicationError::new(
                code,
                DomainOperation::ExecuteQuery,
                DomainContext::empty(),
                incident,
            );
            let encoded = encode_application_error(&error);
            assert_eq!(
                decode_application_error(&encoded),
                Ok(error),
                "application error {}",
                code.as_str()
            );
        }
    }

    #[test]
    fn arbitrary_and_oversized_symbol_context_is_impossible() {
        assert!(
            DomainContext::empty()
                .with_operation_symbol("secret value with spaces".to_owned())
                .is_err()
        );
        assert!(
            DomainContext::empty()
                .with_symbol_path(vec![
                    "x".repeat(riffdb_errors::MAX_APPLICATION_SYMBOL_BYTES + 1)
                ])
                .is_err()
        );
    }

    #[test]
    fn redundant_registry_fields_fail_closed() {
        let error = DomainApplicationError::new(
            DomainCode::AuthorizationDenied,
            DomainOperation::ExecuteQuery,
            DomainContext::empty(),
            None,
        );
        let mut wire = application_error_to_proto(&error);
        wire.category = app_v1::ApplicationErrorCategory::Storage as i32;
        assert_eq!(
            application_error_from_proto(&wire),
            Err(ApplicationErrorWireError::InconsistentRegistry)
        );
    }

    #[test]
    fn unknown_tags_fail_closed() {
        let error = DomainApplicationError::new(
            DomainCode::InputInvalid,
            DomainOperation::CheckQuery,
            DomainContext::empty(),
            None,
        );
        let mut bytes = application_error_to_proto(&error).encode_to_vec();
        bytes.extend_from_slice(&[0xa0, 0x06, 0x01]);
        assert_eq!(
            decode_application_error(&bytes),
            Err(ApplicationErrorWireError::UnknownField)
        );
    }

    #[test]
    fn source_bounds_are_enforced() {
        assert!(DomainSpan::new(4, 3).is_none());
        assert!(DomainSpan::new(0, riffdb_errors::MAX_APPLICATION_SOURCE_OFFSET + 1).is_none());
    }
}
