//! Immutable symbolic export submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_proto::v1;
use riffdb_types::{
    ApplicationExportOperationId, ApplicationExportSelectionV1, CapabilityApplicationExportScopeV1,
    RequestId,
};

/// One immutable exact application-export start or replay submission.
#[derive(Clone, Eq, PartialEq)]
pub struct StartApplicationExport {
    operation_id: ApplicationExportOperationId,
    selection: ApplicationExportSelectionV1,
    lease_seconds: u32,
}

impl StartApplicationExport {
    /// Retains one closed selection under a caller-stable operation identity.
    #[must_use]
    pub const fn new(
        operation_id: ApplicationExportOperationId,
        selection: ApplicationExportSelectionV1,
        lease_seconds: u32,
    ) -> Self {
        Self {
            operation_id,
            selection,
            lease_seconds,
        }
    }

    /// Caller-stable identity reused across uncertain transport retries.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Exact symbolic lineage, scope, and record classes.
    #[must_use]
    pub const fn selection(&self) -> &ApplicationExportSelectionV1 {
        &self.selection
    }

    /// Requested bounded server-owned lease in seconds.
    #[must_use]
    pub const fn lease_seconds(&self) -> u32 {
        self.lease_seconds
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::StartApplicationExportRequest {
        let scope = match self.selection.scope() {
            CapabilityApplicationExportScopeV1::PrincipalFiltered => {
                v1::CapabilityApplicationExportScope::PrincipalFiltered
            }
            CapabilityApplicationExportScopeV1::WholeApplication => {
                v1::CapabilityApplicationExportScope::WholeApplication
            }
        };
        v1::StartApplicationExportRequest {
            request_id: request_id.into_bytes().to_vec(),
            operation_id: self.operation_id.into_bytes().to_vec(),
            selection: Some(v1::ApplicationExportSelection {
                contract_lineage: self.selection.lineage().as_str().to_owned(),
                scope: scope as i32,
                entities: self.selection.entities(),
                events: self.selection.events(),
                provenance: self.selection.provenance(),
                public_audit: self.selection.public_audit(),
            }),
            lease_seconds: self.lease_seconds,
        }
    }
}

impl fmt::Debug for StartApplicationExport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartApplicationExport")
            .field("operation_id", &self.operation_id)
            .field("selection", &self.selection)
            .field("lease_seconds", &self.lease_seconds)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::ContractLineage;

    use super::*;

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    #[test]
    fn retry_changes_only_outer_request_identity() {
        let start = StartApplicationExport::new(
            ApplicationExportOperationId::from_bytes(uuid(1)).expect("operation"),
            ApplicationExportSelectionV1::new(
                ContractLineage::new("TicketDesk").expect("lineage"),
                CapabilityApplicationExportScopeV1::WholeApplication,
                true,
                true,
                true,
                false,
            )
            .expect("selection"),
            900,
        );
        let first = start.request(RequestId::from_bytes(uuid(2)).expect("request"));
        let second = start.request(RequestId::from_bytes(uuid(3)).expect("request"));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.selection, second.selection);
        assert_eq!(first.lease_seconds, second.lease_seconds);
    }
}
