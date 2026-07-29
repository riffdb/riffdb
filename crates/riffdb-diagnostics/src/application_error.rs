//! Bounded value-free operator fields for application failures.

use std::fmt::{self, Write as _};

use riffdb_errors::{APPLICATION_ERROR_ENVELOPE_VERSION, ApplicationError};

/// Maximum bytes in one operator-side application error field record.
pub const MAX_APPLICATION_ERROR_DIAGNOSTIC_BYTES: usize = 8 * 1024;

/// Renders deterministic log/trace fields from the same checked error object.
///
/// The result contains no submitted value, principal, credential, numeric
/// schema ID, arbitrary message, or internal error source. Consumers may attach
/// the complete string as one structured tracing field or split on ASCII spaces
/// because every dynamic component is checked visible ASCII without spaces.
pub fn render_application_error_diagnostic(
    error: &ApplicationError,
) -> Result<String, ApplicationErrorDiagnosticError> {
    let mut output = String::new();
    write!(
        output,
        "application_error version={} code={} category={} recovery_action={} operation={}",
        APPLICATION_ERROR_ENVELOPE_VERSION,
        error.code().as_str(),
        error.category().as_str(),
        error.recovery_action().as_str(),
        error.operation().as_str(),
    )
    .map_err(|_| ApplicationErrorDiagnosticError)?;
    if let Some((lineage, version)) = error.context().contract() {
        write!(
            output,
            " contract_lineage={lineage} contract_version={version}"
        )
        .map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    if let Some(symbol) = error.context().operation_symbol() {
        write!(output, " operation_symbol={symbol}")
            .map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    if !error.context().symbol_path().is_empty() {
        write!(
            output,
            " symbol_path={}",
            error.context().symbol_path().join(".")
        )
        .map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    if let Some(span) = error.context().source_span() {
        write!(output, " source_span={}:{}", span.start(), span.end())
            .map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    write!(
        output,
        " fixes={}",
        error
            .fixes()
            .iter()
            .map(|fix| fix.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
    .map_err(|_| ApplicationErrorDiagnosticError)?;
    if let Some(trace_id) = error.context().trace_id() {
        write!(output, " trace_id={trace_id}").map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    if let Some(incident_id) = error.incident_id() {
        write!(output, " incident_id={incident_id}")
            .map_err(|_| ApplicationErrorDiagnosticError)?;
    }
    if output.len() > MAX_APPLICATION_ERROR_DIAGNOSTIC_BYTES {
        return Err(ApplicationErrorDiagnosticError);
    }
    Ok(output)
}

/// A checked application error could not fit the operator field bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationErrorDiagnosticError;

impl fmt::Display for ApplicationErrorDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application error diagnostic exceeds its fixed bound")
    }
}

impl std::error::Error for ApplicationErrorDiagnosticError {}

#[cfg(test)]
mod tests {
    use riffdb_errors::{
        ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation, ApplicationSourceSpan,
    };
    use riffdb_types::{ContractLineage, ContractVersion, RequestId};

    use super::*;

    #[test]
    fn checked_error_renders_the_frozen_value_free_trace_fields() {
        let trace_id = RequestId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("trace ID");
        let context = ApplicationErrorContext::empty()
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
            .with_source_span(ApplicationSourceSpan::new(20, 44).expect("span"))
            .with_trace_id(trace_id);
        let error = riffdb_errors::ApplicationError::new(
            ApplicationErrorCode::AuthorizationDenied,
            ApplicationOperation::ExecuteQuery,
            context,
            None,
        );
        assert_eq!(
            render_application_error_diagnostic(&error).expect("diagnostic"),
            include_str!("../../../fixtures/application-errors/authorization-ticket-page-v1.trace")
                .trim_end()
        );
    }
}
