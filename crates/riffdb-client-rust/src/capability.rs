//! Immutable capability-create templates for bounded transport retry.

use std::error::Error;
use std::fmt;

use riffdb_proto::v1;
use riffdb_proto::validate_create_capability_template;
use riffdb_types::RequestId;

/// A closed local failure while constructing a capability-create template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityCreateTemplateError {
    /// The caller supplied an outer request ID instead of the empty sentinel.
    NonemptyRequestId,
    /// The request mode does not match the selected template type.
    WrongMode,
    /// The retained semantic body violates the public protocol.
    InvalidBody,
}

impl fmt::Display for CapabilityCreateTemplateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NonemptyRequestId => "capability-create template request ID is not empty",
            Self::WrongMode => "capability-create template mode is invalid",
            Self::InvalidBody => "capability-create template body is invalid",
        })
    }
}

impl Error for CapabilityCreateTemplateError {}

/// A checked immutable normal capability-create operation.
pub struct NormalCapabilityCreateTemplate {
    request: v1::CreateCapabilityRequest,
}

impl NormalCapabilityCreateTemplate {
    /// Validates and retains one exact normal capability-create body.
    pub fn new(
        request: v1::CreateCapabilityRequest,
    ) -> Result<Self, CapabilityCreateTemplateError> {
        validate_template_mode(&request, v1::CapabilityCreateMode::Normal)?;
        Ok(Self { request })
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::CreateCapabilityRequest {
        request_with_id(&self.request, request_id)
    }
}

impl fmt::Debug for NormalCapabilityCreateTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NormalCapabilityCreateTemplate([REDACTED])")
    }
}

/// A checked immutable bootstrap capability-create operation.
pub struct BootstrapCapabilityCreateTemplate {
    request: v1::CreateCapabilityRequest,
}

impl BootstrapCapabilityCreateTemplate {
    /// Validates and retains one exact bootstrap capability-create body.
    pub fn new(
        request: v1::CreateCapabilityRequest,
    ) -> Result<Self, CapabilityCreateTemplateError> {
        validate_template_mode(&request, v1::CapabilityCreateMode::Bootstrap)?;
        Ok(Self { request })
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::CreateCapabilityRequest {
        request_with_id(&self.request, request_id)
    }
}

impl fmt::Debug for BootstrapCapabilityCreateTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapCapabilityCreateTemplate([REDACTED])")
    }
}

fn validate_template_mode(
    request: &v1::CreateCapabilityRequest,
    expected_mode: v1::CapabilityCreateMode,
) -> Result<(), CapabilityCreateTemplateError> {
    if !request.request_id.is_empty() {
        return Err(CapabilityCreateTemplateError::NonemptyRequestId);
    }
    if request.mode != expected_mode as i32 {
        return Err(CapabilityCreateTemplateError::WrongMode);
    }
    validate_create_capability_template(request)
        .map_err(|_| CapabilityCreateTemplateError::InvalidBody)
}

fn request_with_id(
    template: &v1::CreateCapabilityRequest,
    request_id: RequestId,
) -> v1::CreateCapabilityRequest {
    let mut request = template.clone();
    request.request_id = request_id.into_bytes().to_vec();
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(mode: v1::CapabilityCreateMode) -> v1::CreateCapabilityRequest {
        let permissions = if mode == v1::CapabilityCreateMode::Bootstrap {
            vec![v1::CapabilityPermission {
                permission: Some(
                    v1::capability_permission::Permission::AdministerCapabilities(v1::Unit {}),
                ),
            }]
        } else {
            Vec::new()
        };
        v1::CreateCapabilityRequest {
            request_id: Vec::new(),
            mode: mode as i32,
            capability_id: RequestId::from_unix_milliseconds_and_random(1, [1; 10])
                .expect("UUIDv7")
                .into_bytes()
                .to_vec(),
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            requested_lifetime_seconds: 60,
            audiences: vec!["riffdb-cli".to_owned()],
            grant: Some(v1::CapabilityGrant {
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                }),
                partition_scope: Some(v1::PartitionScope {
                    scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                }),
                permissions,
                field_visibility: Vec::new(),
                max_scan_rows: 1,
                approval_required: Vec::new(),
                row_policy: None,
                export: None,
                reimport: None,
                vector_inspection: None,
            }),
        }
    }

    #[test]
    fn constructor_enforces_empty_id_exact_mode_and_complete_body() {
        assert!(
            NormalCapabilityCreateTemplate::new(request(v1::CapabilityCreateMode::Normal)).is_ok()
        );
        assert!(
            BootstrapCapabilityCreateTemplate::new(request(v1::CapabilityCreateMode::Bootstrap))
                .is_ok()
        );

        let mut nonempty = request(v1::CapabilityCreateMode::Normal);
        nonempty.request_id = vec![1; 16];
        assert!(matches!(
            NormalCapabilityCreateTemplate::new(nonempty),
            Err(CapabilityCreateTemplateError::NonemptyRequestId)
        ));
        assert!(matches!(
            NormalCapabilityCreateTemplate::new(request(v1::CapabilityCreateMode::Bootstrap)),
            Err(CapabilityCreateTemplateError::WrongMode)
        ));
        let mut invalid = request(v1::CapabilityCreateMode::Normal);
        invalid.principal_id.clear();
        assert!(matches!(
            NormalCapabilityCreateTemplate::new(invalid),
            Err(CapabilityCreateTemplateError::InvalidBody)
        ));
    }

    #[test]
    fn every_submission_changes_only_the_outer_request_id() {
        let template =
            NormalCapabilityCreateTemplate::new(request(v1::CapabilityCreateMode::Normal))
                .expect("template");
        let first_id = RequestId::from_unix_milliseconds_and_random(2, [2; 10]).expect("ID");
        let second_id = RequestId::from_unix_milliseconds_and_random(3, [3; 10]).expect("ID");
        let first = template.request(first_id);
        let second = template.request(second_id);

        assert_ne!(first.request_id, second.request_id);
        let mut first_without_id = first;
        let mut second_without_id = second;
        first_without_id.request_id.clear();
        second_without_id.request_id.clear();
        assert_eq!(first_without_id, second_without_id);
        assert_eq!(
            format!("{template:?}"),
            "NormalCapabilityCreateTemplate([REDACTED])"
        );
    }
}
