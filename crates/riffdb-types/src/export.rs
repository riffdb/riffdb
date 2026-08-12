//! Closed identities, scope, and snapshot bindings for symbolic export.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

use crate::{
    AdministrationSequence, ApplicationExportOperationId, CapabilityApplicationExportScopeV1,
    CapabilityId, CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    QueryModuleHash, ReactiveModuleHash,
};

/// Maximum immutable module identities bound into one export snapshot.
pub const MAX_APPLICATION_EXPORT_MODULES: usize = 256;

/// Safe construction failure for one symbolic export identity or selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportValueError {
    /// A closed combination was invalid.
    InvalidShape,
    /// A hard count bound was exceeded.
    LimitExceeded,
    /// A canonical identity collection was duplicate or unordered.
    NonCanonical,
}

impl fmt::Display for ApplicationExportValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidShape => "application export has an invalid closed shape",
            Self::LimitExceeded => "application export exceeds a hard limit",
            Self::NonCanonical => "application export identities are not canonical",
        })
    }
}

impl Error for ApplicationExportValueError {}

/// Closed portable record classes. No table, index, or raw key selector exists.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ApplicationExportClassV1 {
    /// Current symbolic entity state.
    Entity = 1,
    /// Retained typed domain events.
    Event = 2,
    /// Separately authorized command provenance.
    Provenance = 3,
    /// Separately authorized public-safe administration audit.
    PublicAudit = 4,
}

impl ApplicationExportClassV1 {
    /// Stable closed semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Strictly decodes a stable semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Entity),
            2 => Some(Self::Event),
            3 => Some(Self::Provenance),
            4 => Some(Self::PublicAudit),
            _ => None,
        }
    }
}

/// Canonical hash preimage for one bounded symbolic export page.
///
/// Cursor bytes are deliberately excluded: a cursor is a sealed continuation
/// capability, while the page hash identifies released application content,
/// its position, and terminal shape.
pub fn canonical_application_export_page_preimage(
    operation_id: ApplicationExportOperationId,
    page_number: NonZeroU64,
    class: ApplicationExportClassV1,
    lines: &[&[u8]],
    class_complete: bool,
    operation_complete: bool,
) -> Result<Vec<u8>, ApplicationExportValueError> {
    let line_count =
        u32::try_from(lines.len()).map_err(|_| ApplicationExportValueError::LimitExceeded)?;
    let mut output = Vec::new();
    output.extend_from_slice(operation_id.as_bytes());
    output.extend_from_slice(&page_number.get().to_be_bytes());
    output.push(class.tag());
    output.push(u8::from(class_complete));
    output.push(u8::from(operation_complete));
    output.extend_from_slice(&line_count.to_be_bytes());
    for line in lines {
        let line_length =
            u32::try_from(line.len()).map_err(|_| ApplicationExportValueError::LimitExceeded)?;
        output.extend_from_slice(&line_length.to_be_bytes());
        output.extend_from_slice(line);
    }
    Ok(output)
}

/// Exact caller-selected subset of one current V5 lineage grant.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportSelectionV1 {
    lineage: ContractLineage,
    scope: CapabilityApplicationExportScopeV1,
    entities: bool,
    events: bool,
    provenance: bool,
    public_audit: bool,
}

impl ApplicationExportSelectionV1 {
    /// Constructs one closed selection. Supporting records cannot stand alone.
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn new(
        lineage: ContractLineage,
        scope: CapabilityApplicationExportScopeV1,
        entities: bool,
        events: bool,
        provenance: bool,
        public_audit: bool,
    ) -> Result<Self, ApplicationExportValueError> {
        if !entities && !events {
            return Err(ApplicationExportValueError::InvalidShape);
        }
        Ok(Self {
            lineage,
            scope,
            entities,
            events,
            provenance,
            public_audit,
        })
    }

    /// Selected exact application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Selected policy-filtered or whole-application scope.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationExportScopeV1 {
        self.scope
    }

    /// Whether current entities are selected.
    #[must_use]
    pub const fn entities(&self) -> bool {
        self.entities
    }

    /// Whether retained events are selected.
    #[must_use]
    pub const fn events(&self) -> bool {
        self.events
    }

    /// Whether separately authorized provenance is selected.
    #[must_use]
    pub const fn provenance(&self) -> bool {
        self.provenance
    }

    /// Whether separately authorized public-safe audit is selected.
    #[must_use]
    pub const fn public_audit(&self) -> bool {
        self.public_audit
    }

    /// True only when this operation selected the class explicitly.
    #[must_use]
    pub const fn includes(&self, class: ApplicationExportClassV1) -> bool {
        match class {
            ApplicationExportClassV1::Entity => self.entities,
            ApplicationExportClassV1::Event => self.events,
            ApplicationExportClassV1::Provenance => self.provenance,
            ApplicationExportClassV1::PublicAudit => self.public_audit,
        }
    }
}

impl fmt::Debug for ApplicationExportSelectionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationExportSelectionV1")
            .field("lineage", &self.lineage)
            .field("scope", &self.scope)
            .field("entities", &self.entities)
            .field("events", &self.events)
            .field("provenance", &self.provenance)
            .field("public_audit", &self.public_audit)
            .finish()
    }
}

/// Exact current capability revision bound into one export operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationExportAuthorityV1 {
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
}

impl ApplicationExportAuthorityV1 {
    /// Binds one current durable capability revision.
    #[must_use]
    pub const fn new(capability_id: CapabilityId, capability_revision: NonZeroU64) -> Self {
        Self {
            capability_id,
            capability_revision,
        }
    }

    /// Durable capability identity.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }

    /// Exact revision revalidated at every release safe point.
    #[must_use]
    pub const fn capability_revision(self) -> NonZeroU64 {
        self.capability_revision
    }
}

/// Immutable published source identity captured atomically at export start.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportSnapshotBindingV1 {
    database_id: DatabaseId,
    history_incarnation: NonZeroU64,
    application_frontier: Option<CommitSequence>,
    administration_frontier: Option<AdministrationSequence>,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    query_modules: Vec<QueryModuleHash>,
    reactive_modules: Vec<ReactiveModuleHash>,
}

impl ApplicationExportSnapshotBindingV1 {
    /// Constructs one canonical source identity with sorted unique modules.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: NonZeroU64,
        application_frontier: Option<CommitSequence>,
        administration_frontier: Option<AdministrationSequence>,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        query_modules: Vec<QueryModuleHash>,
        reactive_modules: Vec<ReactiveModuleHash>,
    ) -> Result<Self, ApplicationExportValueError> {
        if query_modules.len() > MAX_APPLICATION_EXPORT_MODULES
            || reactive_modules.len() > MAX_APPLICATION_EXPORT_MODULES
        {
            return Err(ApplicationExportValueError::LimitExceeded);
        }
        if query_modules.windows(2).any(|pair| pair[0] >= pair[1])
            || reactive_modules.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ApplicationExportValueError::NonCanonical);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            application_frontier,
            administration_frontier,
            contract_version,
            contract_bundle_hash,
            query_modules,
            reactive_modules,
        })
    }

    /// Durable database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Positive history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> NonZeroU64 {
        self.history_incarnation
    }

    /// Published application frontier captured with the snapshot.
    #[must_use]
    pub const fn application_frontier(&self) -> Option<CommitSequence> {
        self.application_frontier
    }

    /// Published administration frontier captured with the snapshot.
    #[must_use]
    pub const fn administration_frontier(&self) -> Option<AdministrationSequence> {
        self.administration_frontier
    }

    /// Exact active contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Exact active contract bundle identity.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Canonical active query-module identities.
    #[must_use]
    pub fn query_modules(&self) -> &[QueryModuleHash] {
        &self.query_modules
    }

    /// Canonical active reactive-module identities.
    #[must_use]
    pub fn reactive_modules(&self) -> &[ReactiveModuleHash] {
        &self.reactive_modules
    }
}

impl fmt::Debug for ApplicationExportSnapshotBindingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationExportSnapshotBindingV1")
            .field("database_id", &self.database_id)
            .field("history_incarnation", &self.history_incarnation)
            .field("application_frontier", &self.application_frontier)
            .field("administration_frontier", &self.administration_frontier)
            .field("contract_version", &self.contract_version)
            .field("contract_bundle_hash", &self.contract_bundle_hash)
            .field("query_module_count", &self.query_modules.len())
            .field("reactive_module_count", &self.reactive_modules.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_has_no_supporting_only_or_implicit_class_shape() {
        let lineage = ContractLineage::new("TicketDesk").expect("lineage");
        assert_eq!(
            ApplicationExportSelectionV1::new(
                lineage.clone(),
                CapabilityApplicationExportScopeV1::WholeApplication,
                false,
                false,
                true,
                true,
            ),
            Err(ApplicationExportValueError::InvalidShape)
        );
        let selection = ApplicationExportSelectionV1::new(
            lineage,
            CapabilityApplicationExportScopeV1::PrincipalFiltered,
            true,
            false,
            false,
            false,
        )
        .expect("selection");
        assert!(selection.includes(ApplicationExportClassV1::Entity));
        assert!(!selection.includes(ApplicationExportClassV1::Event));
        assert!(!selection.includes(ApplicationExportClassV1::Provenance));
        assert!(!selection.includes(ApplicationExportClassV1::PublicAudit));
    }
}
