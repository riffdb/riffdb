//! Service-owned construction of redaction-safe application error context.

use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
};
use riffdb_types::{ContractLineage, ContractVersion, RequestId};

use crate::ServiceFailure;

/// Builds one application error exclusively from already public symbolic facts.
///
/// This builder deliberately has no principal, capability, raw value, numeric
/// schema ID, source text, storage error, or arbitrary-message input. A symbol
/// may be attached only when the caller submitted it or authorization already
/// released it.
#[derive(Clone, Debug)]
pub struct ApplicationErrorContextBuilder {
    operation: ApplicationOperation,
    context: ApplicationErrorContext,
}

impl ApplicationErrorContextBuilder {
    /// Starts context with the application operation and request/trace ID.
    #[must_use]
    pub fn new(operation: ApplicationOperation, trace_id: RequestId) -> Self {
        Self {
            operation,
            context: ApplicationErrorContext::empty().with_trace_id(trace_id),
        }
    }

    /// Starts context before a valid request identity can be established.
    #[must_use]
    pub const fn without_trace(operation: ApplicationOperation) -> Self {
        Self {
            operation,
            context: ApplicationErrorContext::empty(),
        }
    }

    /// Attaches exact caller-visible contract identity.
    #[must_use]
    pub fn with_contract(mut self, lineage: ContractLineage, version: ContractVersion) -> Self {
        self.context = self.context.with_contract(lineage, version);
        self
    }

    /// Attaches one caller-visible query, module, command, or role name.
    ///
    /// Invalid symbols are omitted rather than copied or normalized.
    #[must_use]
    pub fn with_operation_symbol(mut self, symbol: String) -> Self {
        if let Ok(context) = self.context.clone().with_operation_symbol(symbol) {
            self.context = context;
        }
        self
    }

    /// Converts the shared service failure after authorization/redaction.
    ///
    /// The emergency path returns `None` because it has no real incident
    /// identity and must retain ADR-0006's details-free containment behavior.
    #[must_use]
    pub fn build(&self, failure: &ServiceFailure) -> Option<ApplicationError> {
        let (code, incident_id) = match failure {
            ServiceFailure::Public(error) => {
                return Some(ApplicationError::from_public_error(
                    error,
                    self.operation,
                    self.context.clone(),
                ));
            }
            ServiceFailure::Cancelled => (ApplicationErrorCode::OperationCancelled, None),
            ServiceFailure::DeadlineExceeded => (ApplicationErrorCode::DeadlineExceeded, None),
            ServiceFailure::ResponseTooLarge => (ApplicationErrorCode::ResponseTooLarge, None),
            ServiceFailure::EmergencyInternal(_) => return None,
        };
        Some(ApplicationError::new(
            code,
            self.operation,
            self.context.clone(),
            incident_id,
        ))
    }

    /// Builds a structural rejection before the service operation exists.
    #[must_use]
    pub fn invalid_request(&self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            self.operation,
            self.context.clone(),
            None,
        )
    }

    /// Builds a pre-service authentication or authorization rejection.
    #[must_use]
    pub fn authorization_denied(&self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::AuthorizationDenied,
            self.operation,
            self.context.clone(),
            None,
        )
    }

    /// Builds a rejection for a reciprocally matched but revoked capability.
    #[must_use]
    pub fn capability_revoked(&self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::CapabilityRevoked,
            self.operation,
            self.context.clone(),
            None,
        )
    }

    /// Builds a pre-service readiness rejection.
    #[must_use]
    pub fn unavailable(&self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::StorageUnavailable,
            self.operation,
            self.context.clone(),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use riffdb_errors::{
        ApplicationFixCode, PublicError, ValidationCode, ValidationIssue, ValidationIssues,
        ValidationPath,
    };
    use riffdb_types::IncidentId;

    use super::*;

    fn request_id() -> RequestId {
        RequestId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("request ID")
    }

    #[test]
    fn authorization_context_contains_names_but_no_authority_encoding() {
        let builder =
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id())
                .with_contract(
                    ContractLineage::new("ticketdesk").expect("lineage"),
                    ContractVersion::new(18).expect("version"),
                )
                .with_operation_symbol("TicketPage".to_owned());
        let error = builder
            .build(&ServiceFailure::from(PublicError::authorization_denied()))
            .expect("application error");
        assert_eq!(error.code(), ApplicationErrorCode::AuthorizationDenied);
        assert_eq!(error.context().operation_symbol(), Some("TicketPage"));
        assert_eq!(error.fixes(), &[ApplicationFixCode::BindApplicationRole]);
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("field_id"));
        assert!(!rendered.contains("capability"));
        assert!(!rendered.contains("principal"));
    }

    #[test]
    fn matched_revocation_retains_symbolic_context_and_closed_fix() {
        let builder =
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id())
                .with_operation_symbol("TicketPage".to_owned());
        let error = builder.capability_revoked();
        assert_eq!(error.code(), ApplicationErrorCode::CapabilityRevoked);
        assert_eq!(error.context().operation_symbol(), Some("TicketPage"));
        assert_eq!(error.fixes(), &[ApplicationFixCode::BindApplicationRole]);
    }

    #[test]
    fn emergency_without_incident_remains_details_free() {
        let builder =
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id());
        let emergency =
            riffdb_errors::EmergencyInternalFailure::from(riffdb_errors::IncidentIdSourceError);
        assert!(
            builder
                .build(&ServiceFailure::EmergencyInternal(emergency))
                .is_none()
        );
    }

    #[test]
    fn internal_failure_preserves_only_opaque_incident() {
        let incident = IncidentId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x24,
        ])
        .expect("incident ID");
        let builder =
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id());
        let error = builder
            .build(&ServiceFailure::from(PublicError::internal_defect(
                incident,
            )))
            .expect("application error");
        assert_eq!(error.incident_id(), Some(&incident));
    }

    #[test]
    fn service_preserves_stricter_cursor_classification() {
        let public = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
            ValidationCode::InvalidValue,
            ValidationPath::root(),
        )))
        .with_application_code_hint(ApplicationErrorCode::CursorInvalid)
        .expect("compatible hint");
        let builder =
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id());
        let error = builder
            .build(&ServiceFailure::from(public))
            .expect("application error");
        assert_eq!(error.code(), ApplicationErrorCode::CursorInvalid);
        assert_eq!(error.fixes(), &[ApplicationFixCode::RestartFromFirstPage]);
    }
}
