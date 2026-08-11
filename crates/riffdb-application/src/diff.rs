use riffdb_types::{ApplicationRoleHash, CapabilityId};

use crate::{
    ApplicationInstallationPlan, CredentialDestination, InstallationArtifact, InstallationFeature,
    InstallationPlanError, InstallationPlanErrorKind, InstallationRole, InstallationSymbol,
    InstallationTarget, RoleOperation,
};

/// Exact lifecycle observation required before any installation mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationLifecycleState {
    /// Selected database is authenticated, serving, and ready.
    Ready,
    /// Selected database is not ready; planning may describe but execution must stop.
    NotReady,
}

/// Existing role state exposed symbolically for a bounded authority diff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedInstallationRole {
    name: InstallationSymbol,
    role_hash: ApplicationRoleHash,
    operations: Vec<RoleOperation>,
}

impl ObservedInstallationRole {
    /// Creates one sorted exact role observation.
    pub fn new(
        name: InstallationSymbol,
        role_hash: ApplicationRoleHash,
        mut operations: Vec<RoleOperation>,
    ) -> Result<Self, InstallationPlanError> {
        operations.sort();
        if operations.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(InstallationPlanError::from_kind_for_diff(
                InstallationPlanErrorKind::Duplicate,
            ));
        }
        Ok(Self {
            name,
            role_hash,
            operations,
        })
    }

    /// Symbolic role name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact current role identity.
    #[must_use]
    pub const fn role_hash(&self) -> ApplicationRoleHash {
        self.role_hash
    }

    /// Current symbolic operation surface.
    #[must_use]
    pub fn operations(&self) -> &[RoleOperation] {
        &self.operations
    }
}

/// Existing external credential slot identity without token material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedCredentialDestination {
    name: InstallationSymbol,
    capability_id: CapabilityId,
}

impl ObservedCredentialDestination {
    /// Creates one exact non-secret slot observation.
    #[must_use]
    pub const fn new(name: InstallationSymbol, capability_id: CapabilityId) -> Self {
        Self {
            name,
            capability_id,
        }
    }

    /// Symbolic slot name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Current capability identity, never bearer material.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }
}

/// Bounded exact remote state used only for read-only planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallationRemoteState {
    target: InstallationTarget,
    lifecycle: InstallationLifecycleState,
    contract: Option<crate::InstallationContract>,
    artifacts: Vec<InstallationArtifact>,
    roles: Vec<ObservedInstallationRole>,
    credentials: Vec<ObservedCredentialDestination>,
    supported_features: Vec<InstallationFeature>,
}

impl InstallationRemoteState {
    /// Validates and canonicalizes one bounded remote observation.
    pub fn new(
        target: InstallationTarget,
        lifecycle: InstallationLifecycleState,
        contract: Option<crate::InstallationContract>,
        mut artifacts: Vec<InstallationArtifact>,
        mut roles: Vec<ObservedInstallationRole>,
        mut credentials: Vec<ObservedCredentialDestination>,
        mut supported_features: Vec<InstallationFeature>,
    ) -> Result<Self, InstallationPlanError> {
        if artifacts.len() > crate::MAX_INSTALLATION_ARTIFACTS
            || roles.len() > crate::MAX_INSTALLATION_ROLES
            || credentials.len() > crate::MAX_CREDENTIAL_DESTINATIONS
        {
            return Err(InstallationPlanError::from_kind_for_diff(
                InstallationPlanErrorKind::LimitExceeded,
            ));
        }
        artifacts.sort();
        reject_duplicate_keys(
            artifacts
                .windows(2)
                .any(|pair| (pair[0].kind(), pair[0].name()) == (pair[1].kind(), pair[1].name())),
        )?;
        roles.sort_by(|left, right| left.name.cmp(&right.name));
        reject_duplicate_keys(roles.windows(2).any(|pair| pair[0].name == pair[1].name))?;
        credentials.sort_by(|left, right| left.name.cmp(&right.name));
        reject_duplicate_keys(
            credentials
                .windows(2)
                .any(|pair| pair[0].name == pair[1].name),
        )?;
        supported_features.sort();
        reject_duplicate_keys(supported_features.windows(2).any(|pair| pair[0] == pair[1]))?;
        Ok(Self {
            target,
            lifecycle,
            contract,
            artifacts,
            roles,
            credentials,
            supported_features,
        })
    }

    /// Exact selected target.
    #[must_use]
    pub const fn target(&self) -> &InstallationTarget {
        &self.target
    }

    /// Current lifecycle/readiness state.
    #[must_use]
    pub const fn lifecycle(&self) -> InstallationLifecycleState {
        self.lifecycle
    }
}

/// Exact contract action required by a plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationContractAction {
    /// Candidate is already active exactly.
    AlreadyExact,
    /// No contract is active; deploy the exact candidate.
    DeployGenesis,
    /// A different predecessor is active and the supplied exact migration must run.
    ApplyMigration,
    /// A different predecessor is active without an applicable exact migration.
    StopForMigration,
}

/// Exact generated-artifact change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstallationArtifactAction {
    /// Artifact already exists at the exact identity.
    AlreadyExact(InstallationArtifact),
    /// Artifact is absent and must be published.
    Publish(InstallationArtifact),
    /// Same symbolic artifact exists at another immutable identity.
    IdentityConflict {
        /// Desired artifact.
        desired: InstallationArtifact,
        /// Observed inexact artifact.
        observed: InstallationArtifact,
    },
}

/// Bounded symbolic authority change for one role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallationRoleDiff {
    name: InstallationSymbol,
    current_hash: Option<ApplicationRoleHash>,
    desired_hash: ApplicationRoleHash,
    additions: Vec<RoleOperation>,
    removals: Vec<RoleOperation>,
    widening_approved: bool,
}

impl InstallationRoleDiff {
    /// Symbolic role name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Current exact role identity, when one exists.
    #[must_use]
    pub const fn current_hash(&self) -> Option<ApplicationRoleHash> {
        self.current_hash
    }

    /// Desired exact role identity.
    #[must_use]
    pub const fn desired_hash(&self) -> ApplicationRoleHash {
        self.desired_hash
    }

    /// New symbolic authority.
    #[must_use]
    pub fn additions(&self) -> &[RoleOperation] {
        &self.additions
    }

    /// Removed symbolic authority.
    #[must_use]
    pub fn removals(&self) -> &[RoleOperation] {
        &self.removals
    }

    /// Whether the exact additions carry an accepted approval.
    #[must_use]
    pub const fn widening_approved(&self) -> bool {
        self.widening_approved
    }
}

/// Exact external credential action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstallationCredentialAction {
    /// Destination already contains the exact successor identity.
    AlreadyExact(InstallationSymbol),
    /// Empty destination may receive the successor.
    Create(InstallationSymbol),
    /// Exact predecessor may be replaced after successor proof.
    Rotate(InstallationSymbol),
    /// Destination contains an unapproved identity and must not be overwritten.
    Occupied(InstallationSymbol),
}

/// Complete deterministic read-only diff against one selected database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationInstallationDiff {
    lifecycle_ready: bool,
    contract: InstallationContractAction,
    artifacts: Vec<InstallationArtifactAction>,
    roles: Vec<InstallationRoleDiff>,
    credentials: Vec<InstallationCredentialAction>,
    missing_features: Vec<InstallationFeature>,
}

impl ApplicationInstallationDiff {
    /// Compiles one bounded diff without remote mutation.
    pub fn compile(
        plan: &ApplicationInstallationPlan,
        remote: &InstallationRemoteState,
    ) -> Result<Self, InstallationPlanError> {
        let input = plan.input();
        if &input.target != remote.target() {
            return Err(InstallationPlanError::from_kind_for_diff(
                InstallationPlanErrorKind::IdentityMismatch,
            ));
        }
        let contract = match remote.contract {
            None => InstallationContractAction::DeployGenesis,
            Some(active) if active == input.contract => InstallationContractAction::AlreadyExact,
            Some(active)
                if input.migration.is_some_and(|migration| {
                    migration.parent_version() == active.version()
                        && migration.parent_bundle_hash() == active.bundle_hash()
                }) =>
            {
                InstallationContractAction::ApplyMigration
            }
            Some(_) => InstallationContractAction::StopForMigration,
        };
        let artifacts = input
            .artifacts
            .iter()
            .map(|desired| {
                remote
                    .artifacts
                    .iter()
                    .find(|observed| {
                        observed.kind() == desired.kind() && observed.name() == desired.name()
                    })
                    .map_or_else(
                        || InstallationArtifactAction::Publish(desired.clone()),
                        |observed| {
                            if observed == desired {
                                InstallationArtifactAction::AlreadyExact(desired.clone())
                            } else {
                                InstallationArtifactAction::IdentityConflict {
                                    desired: desired.clone(),
                                    observed: observed.clone(),
                                }
                            }
                        },
                    )
            })
            .collect();
        let roles = input
            .roles
            .iter()
            .map(|desired| role_diff(desired, &remote.roles))
            .collect();
        let credentials = input
            .credential_destinations
            .iter()
            .map(|desired| credential_diff(desired, &remote.credentials))
            .collect();
        let missing_features = input
            .required_features
            .iter()
            .filter(|feature| remote.supported_features.binary_search(feature).is_err())
            .copied()
            .collect();
        Ok(Self {
            lifecycle_ready: remote.lifecycle == InstallationLifecycleState::Ready,
            contract,
            artifacts,
            roles,
            credentials,
            missing_features,
        })
    }

    /// Whether the selected database is ready.
    #[must_use]
    pub const fn lifecycle_ready(&self) -> bool {
        self.lifecycle_ready
    }

    /// Required contract action.
    #[must_use]
    pub const fn contract(&self) -> InstallationContractAction {
        self.contract
    }

    /// Exact artifact actions.
    #[must_use]
    pub fn artifacts(&self) -> &[InstallationArtifactAction] {
        &self.artifacts
    }

    /// Symbolic role authority diffs.
    #[must_use]
    pub fn roles(&self) -> &[InstallationRoleDiff] {
        &self.roles
    }

    /// External credential actions.
    #[must_use]
    pub fn credentials(&self) -> &[InstallationCredentialAction] {
        &self.credentials
    }

    /// Required features absent from the exact server/driver observation.
    #[must_use]
    pub fn missing_features(&self) -> &[InstallationFeature] {
        &self.missing_features
    }

    /// Whether execution may start without operator repair or new approval.
    #[must_use]
    pub fn executable(&self) -> bool {
        self.lifecycle_ready
            && self.contract != InstallationContractAction::StopForMigration
            && self.missing_features.is_empty()
            && self.roles.iter().all(|role| {
                role.additions.is_empty() || role.current_hash.is_none() || role.widening_approved
            })
            && self
                .credentials
                .iter()
                .all(|action| !matches!(action, InstallationCredentialAction::Occupied(_)))
            && self.artifacts.iter().all(|action| {
                !matches!(action, InstallationArtifactAction::IdentityConflict { .. })
            })
    }
}

fn role_diff(
    desired: &InstallationRole,
    observed_roles: &[ObservedInstallationRole],
) -> InstallationRoleDiff {
    let observed = observed_roles
        .iter()
        .find(|observed| observed.name == *desired.name());
    let existing = observed.map_or(&[][..], |role| role.operations.as_slice());
    let additions = desired
        .desired_operations()
        .iter()
        .filter(|operation| existing.binary_search(operation).is_err())
        .cloned()
        .collect::<Vec<_>>();
    let removals = existing
        .iter()
        .filter(|operation| {
            desired
                .desired_operations()
                .binary_search(operation)
                .is_err()
        })
        .cloned()
        .collect::<Vec<_>>();
    let widening_approved = additions.is_empty()
        || observed.is_none()
        || desired.widening_approval().is_some_and(|approval| {
            observed.is_some_and(|role| approval.expected_previous_role() == role.role_hash)
                && approval.additions() == additions
        });
    InstallationRoleDiff {
        name: desired.name().clone(),
        current_hash: observed.map(ObservedInstallationRole::role_hash),
        desired_hash: desired.role_hash(),
        additions,
        removals,
        widening_approved,
    }
}

fn credential_diff(
    desired: &CredentialDestination,
    observed_credentials: &[ObservedCredentialDestination],
) -> InstallationCredentialAction {
    let observed = observed_credentials
        .iter()
        .find(|observed| observed.name == *desired.name());
    match observed {
        Some(observed) if observed.capability_id == desired.successor() => {
            InstallationCredentialAction::AlreadyExact(desired.name().clone())
        }
        None if desired.expected_current().is_none() => {
            InstallationCredentialAction::Create(desired.name().clone())
        }
        Some(observed) if Some(observed.capability_id) == desired.expected_current() => {
            InstallationCredentialAction::Rotate(desired.name().clone())
        }
        _ => InstallationCredentialAction::Occupied(desired.name().clone()),
    }
}

fn reject_duplicate_keys(duplicate: bool) -> Result<(), InstallationPlanError> {
    if duplicate {
        Err(InstallationPlanError::from_kind_for_diff(
            InstallationPlanErrorKind::Duplicate,
        ))
    } else {
        Ok(())
    }
}
