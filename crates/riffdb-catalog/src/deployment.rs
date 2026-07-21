//! Pure catalog activation preparation for coordinator-owned persistence.

use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{
    CompatibilityClass, ContractBundle, ContractCandidateV1, compare_successor,
};
use riffdb_storage_api::{ActiveCatalogPointerV1, AuditPrincipalV1, CatalogActivationIntentV1};
use riffdb_types::{ApprovalId, ContractVersion, RequestId, Timestamp};

use crate::lineage::LineageMaterializationProof;
use crate::{ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, ValidatedContractBundle};

/// Whether preparation represents a new pointer or an exact idempotent replay.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogActivationMode {
    /// Candidate is not currently active and requires a durable CAS.
    NewActivation,
    /// The exact immutable pointer is already active; storage resolves its sequence.
    ExactReplay,
}

/// Catalog-checked input to the coordinator-owned administrative transition.
#[derive(Clone)]
pub struct PreparedCatalogActivation {
    expected_active_version: Option<ContractVersion>,
    bundle: ValidatedContractBundle,
    mode: CatalogActivationMode,
    projected_lineage_proof: Arc<LineageMaterializationProof>,
}

impl PreparedCatalogActivation {
    /// Caller-supplied expected active version, including expected absence.
    #[must_use]
    pub const fn expected_active_version(&self) -> Option<ContractVersion> {
        self.expected_active_version
    }

    /// Fully revalidated immutable candidate.
    #[must_use]
    pub const fn bundle(&self) -> &ValidatedContractBundle {
        &self.bundle
    }

    /// Whether this is a new activation or an exact replay.
    #[must_use]
    pub const fn mode(&self) -> CatalogActivationMode {
        self.mode
    }

    /// Lowers checked semantics plus coordinator-supplied operational metadata.
    ///
    /// This creates no mutation authority. Only `riffdb-commit` may submit the
    /// resulting typed intent through the coordinator-only storage authority.
    pub fn into_storage_intent(
        self,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Result<CatalogActivationIntentV1, CatalogError> {
        Ok(CatalogActivationIntentV1::new(
            self.expected_active_version,
            self.bundle.to_stored()?,
            request_id,
            principal,
            timestamp,
            approval_id,
        ))
    }
}

impl fmt::Debug for PreparedCatalogActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedCatalogActivation")
            .field("expected_active_version", &self.expected_active_version)
            .field("bundle", &self.bundle)
            .field("mode", &self.mode)
            .field("projected_lineage_proof", &"[CHECKED]")
            .field(
                "projected_lineage_bundle_count",
                &self.projected_lineage_proof.bundle_count(),
            )
            .finish()
    }
}

/// Closed pure preparation result before the coordinator opens a transaction.
#[derive(Clone, Debug)]
pub enum CatalogPreparationResult {
    /// Candidate is valid and may be submitted through the coordinator.
    Prepared(PreparedCatalogActivation),
    /// The observed active version/absence differed from the requested CAS.
    ExpectedActiveVersionMismatch {
        /// Observed active application version, or absence before first deployment.
        actual: Option<ContractVersion>,
    },
}

/// Revalidates one compiler bundle and prepares a typed expected-version operation.
///
/// The exact already-active pointer takes precedence over a stale expected version,
/// matching the durable repository's retry semantics. Every new successor report
/// is recomputed from parent and candidate; the stored report is never trusted as
/// an activation verdict.
pub fn prepare_catalog_activation(
    candidate: ContractBundle,
    expected_active_version: Option<ContractVersion>,
    active: Option<&ActiveCatalogSnapshot>,
) -> Result<CatalogPreparationResult, CatalogError> {
    let candidate = ValidatedContractBundle::from_compiler_bundle(candidate)?;
    let requested = ActiveCatalogPointerV1::new(
        candidate.lineage().clone(),
        candidate.contract_version(),
        candidate.bundle_hash(),
    );

    if let Some(active) = active {
        if active.pointer() == &requested {
            return Ok(CatalogPreparationResult::Prepared(
                PreparedCatalogActivation {
                    expected_active_version,
                    bundle: candidate,
                    mode: CatalogActivationMode::ExactReplay,
                    projected_lineage_proof: Arc::clone(active.lineage_proof()),
                },
            ));
        }
        if active.pointer().lineage() == requested.lineage()
            && active.pointer().contract_version() == requested.contract_version()
        {
            return Err(CatalogError::new(CatalogErrorKind::BundleIdentityConflict));
        }
    }

    let actual = active.map(|snapshot| snapshot.pointer().contract_version());
    if actual != expected_active_version {
        return Ok(CatalogPreparationResult::ExpectedActiveVersionMismatch { actual });
    }

    validate_activation_compatibility(&candidate, active)?;
    let projected_lineage_proof = match active {
        Some(active) => active.lineage_proof().extend(candidate.clone())?,
        None => LineageMaterializationProof::from_forward_bundles(vec![candidate.clone()])?,
    };
    Ok(CatalogPreparationResult::Prepared(
        PreparedCatalogActivation {
            expected_active_version,
            bundle: candidate,
            mode: CatalogActivationMode::NewActivation,
            projected_lineage_proof,
        },
    ))
}

pub(crate) fn validate_activation_compatibility(
    candidate: &ValidatedContractBundle,
    active: Option<&ActiveCatalogSnapshot>,
) -> Result<(), CatalogError> {
    let bundle = candidate.bundle();
    let Some(active) = active else {
        if bundle.parent().is_some()
            || bundle.compatibility().overall() != CompatibilityClass::Compatible
            || !bundle.compatibility().entries().is_empty()
        {
            return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
        }
        return Ok(());
    };

    let parent_reference = bundle
        .parent()
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))?;
    if bundle.lineage() != active.bundle().lineage()
        || parent_reference.contract_version() != active.bundle().contract_version()
        || parent_reference.bundle_hash() != active.bundle().bundle_hash()
    {
        return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
    }

    validate_successor_compatibility(candidate, active.bundle())
}

pub(crate) fn validate_successor_compatibility(
    candidate: &ValidatedContractBundle,
    parent: &ValidatedContractBundle,
) -> Result<(), CatalogError> {
    let bundle = candidate.bundle();
    let checked_candidate = ContractCandidateV1::new(
        bundle.schema(),
        bundle.commands(),
        bundle.projections(),
        bundle.mcp_command_names(),
    )
    .map_err(|error| CatalogError::from_ir(&error))?;
    let recomputed = compare_successor(parent.bundle(), checked_candidate)
        .map_err(|error| CatalogError::from_ir(&error))?;
    if &recomputed != bundle.compatibility()
        || recomputed.overall() != CompatibilityClass::Compatible
    {
        return Err(CatalogError::new(CatalogErrorKind::IncompatibleContract));
    }
    Ok(())
}
