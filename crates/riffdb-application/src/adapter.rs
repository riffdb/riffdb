use std::error::Error;
use std::fmt;

use riffdb_types::{
    AdapterConformanceManifestHash, ApplicationLockHash, ApplicationManifestHash,
    ApplicationRoleHash, ContractBundleHash, ContractLineage, ContractVersion,
    GeneratedArtifactHash, MigrationBundleHash, hash_adapter_conformance_manifest,
};
use serde::{Deserialize, Serialize};

use crate::{
    ApplicationInstallationPlan, InstallationArtifact, InstallationArtifactKind,
    InstallationContract, InstallationDriver, InstallationFeature, InstallationSymbol,
    RoleOperation, RoleOperationKind,
};

/// Canonical schema for adapter-owned conformance manifests.
pub const ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V1: &str = "riffdb.adapter-conformance-manifest/v1";
/// Maximum canonical manifest size.
pub const MAX_ADAPTER_CONFORMANCE_MANIFEST_BYTES: usize = 1_048_576;
/// Maximum feature claims in one manifest.
pub const MAX_ADAPTER_FEATURE_CLAIMS: usize = 32;
/// Maximum exact artifacts in one manifest.
pub const MAX_ADAPTER_ARTIFACTS: usize = 256;
/// Maximum exact roles in one manifest.
pub const MAX_ADAPTER_ROLES: usize = 128;
/// Maximum driver/runtime requirements in one manifest.
pub const MAX_ADAPTER_DRIVERS: usize = 16;
/// Maximum supported platforms per driver requirement.
pub const MAX_ADAPTER_PLATFORMS: usize = 16;
/// Maximum conformance probes in one manifest.
pub const MAX_ADAPTER_CONFORMANCE_PROBES: usize = 256;
/// Maximum evolution cases in one manifest.
pub const MAX_ADAPTER_EVOLUTIONS: usize = 32;
/// Maximum bounded rows/items observed by one conformance probe.
pub const MAX_ADAPTER_PROBE_ITEMS: u32 = 4_096;

/// Stable, value-free adapter manifest failure class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterConformanceErrorKind {
    /// A fixed byte or collection ceiling was exceeded.
    LimitExceeded,
    /// JSON or a closed registry value was invalid.
    InvalidEncoding,
    /// Valid bytes were not the one canonical encoding.
    NonCanonical,
    /// A required shape or disposition was inconsistent.
    InvalidShape,
    /// A symbolic or exact identity was duplicated.
    Duplicate,
    /// Manifest and application-plan identities disagree.
    IdentityMismatch,
    /// A required closed product feature is unavailable.
    UnsupportedFeature,
    /// A required first-party driver identity is unavailable.
    UnsupportedDriver,
}

/// Bounded adapter manifest error without paths, values, or engine prose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdapterConformanceError {
    kind: AdapterConformanceErrorKind,
}

impl AdapterConformanceError {
    const fn new(kind: AdapterConformanceErrorKind) -> Self {
        Self { kind }
    }

    /// Stable failure class.
    #[must_use]
    pub const fn kind(self) -> AdapterConformanceErrorKind {
        self.kind
    }
}

impl fmt::Display for AdapterConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            AdapterConformanceErrorKind::LimitExceeded => {
                "adapter conformance manifest exceeds a fixed limit"
            }
            AdapterConformanceErrorKind::InvalidEncoding => {
                "adapter conformance manifest encoding is invalid"
            }
            AdapterConformanceErrorKind::NonCanonical => {
                "adapter conformance manifest encoding is not canonical"
            }
            AdapterConformanceErrorKind::InvalidShape => {
                "adapter conformance manifest shape is invalid"
            }
            AdapterConformanceErrorKind::Duplicate => {
                "adapter conformance manifest contains a duplicate identity"
            }
            AdapterConformanceErrorKind::IdentityMismatch => {
                "adapter conformance manifest identity does not match the application plan"
            }
            AdapterConformanceErrorKind::UnsupportedFeature => {
                "adapter requires an unavailable RiffDB feature"
            }
            AdapterConformanceErrorKind::UnsupportedDriver => {
                "adapter requires an unavailable driver or platform"
            }
        })
    }
}

impl Error for AdapterConformanceError {}

/// Explicit support posture for one closed RiffDB feature.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdapterFeatureDisposition {
    /// Installation must stop when the feature is unavailable.
    Required,
    /// The adapter does not require or emulate this feature.
    Optional,
    /// The feature is used with a declared bounded limitation.
    Degraded,
    /// The feature is knowingly unavailable and is not emulated.
    Unavailable,
}

impl AdapterFeatureDisposition {
    const fn tag(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Optional => "optional",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "required" => Self::Required,
            "optional" => Self::Optional,
            "degraded" => Self::Degraded,
            "unavailable" => Self::Unavailable,
            _ => return None,
        })
    }
}

/// Closed reason attached only to degraded or unavailable claims.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdapterLimitation {
    /// The product feature is not implemented in this exact release.
    ProductUnavailable,
    /// The exact released driver platform matrix is narrower than requested.
    PlatformLimited,
    /// The supported operation exists with a documented bounded performance limit.
    PerformanceLimited,
}

impl AdapterLimitation {
    const fn tag(self) -> &'static str {
        match self {
            Self::ProductUnavailable => "product_unavailable",
            Self::PlatformLimited => "platform_limited",
            Self::PerformanceLimited => "performance_limited",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "product_unavailable" => Self::ProductUnavailable,
            "platform_limited" => Self::PlatformLimited,
            "performance_limited" => Self::PerformanceLimited,
            _ => return None,
        })
    }
}

/// One explicit feature claim. There is no fallback or emulation field.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdapterFeatureClaim {
    feature: InstallationFeature,
    disposition: AdapterFeatureDisposition,
    limitation: Option<AdapterLimitation>,
}

impl AdapterFeatureClaim {
    /// Validates one closed support claim.
    pub fn new(
        feature: InstallationFeature,
        disposition: AdapterFeatureDisposition,
        limitation: Option<AdapterLimitation>,
    ) -> Result<Self, AdapterConformanceError> {
        if matches!(
            disposition,
            AdapterFeatureDisposition::Degraded | AdapterFeatureDisposition::Unavailable
        ) != limitation.is_some()
        {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::InvalidShape,
            ));
        }
        Ok(Self {
            feature,
            disposition,
            limitation,
        })
    }

    /// Required claim with no silent fallback.
    #[must_use]
    pub const fn required(feature: InstallationFeature) -> Self {
        Self {
            feature,
            disposition: AdapterFeatureDisposition::Required,
            limitation: None,
        }
    }

    /// Explicit unavailable claim with a closed reason.
    #[must_use]
    pub const fn unavailable(feature: InstallationFeature, limitation: AdapterLimitation) -> Self {
        Self {
            feature,
            disposition: AdapterFeatureDisposition::Unavailable,
            limitation: Some(limitation),
        }
    }

    /// Closed feature identity.
    #[must_use]
    pub const fn feature(self) -> InstallationFeature {
        self.feature
    }

    /// Explicit support disposition.
    #[must_use]
    pub const fn disposition(self) -> AdapterFeatureDisposition {
        self.disposition
    }
}

/// Closed platform triples supported by the alpha driver matrix.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdapterPlatform {
    /// GNU Linux on x86-64.
    LinuxX86_64Gnu,
    /// musl Linux on x86-64.
    LinuxX86_64Musl,
    /// GNU Linux on AArch64.
    LinuxAarch64Gnu,
    /// macOS on x86-64.
    MacosX86_64,
    /// macOS on Apple silicon.
    MacosAarch64,
    /// Windows on x86-64.
    WindowsX86_64,
}

impl AdapterPlatform {
    const fn tag(self) -> &'static str {
        match self {
            Self::LinuxX86_64Gnu => "linux-x86_64-gnu",
            Self::LinuxX86_64Musl => "linux-x86_64-musl",
            Self::LinuxAarch64Gnu => "linux-aarch64-gnu",
            Self::MacosX86_64 => "macos-x86_64",
            Self::MacosAarch64 => "macos-aarch64",
            Self::WindowsX86_64 => "windows-x86_64",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "linux-x86_64-gnu" => Self::LinuxX86_64Gnu,
            "linux-x86_64-musl" => Self::LinuxX86_64Musl,
            "linux-aarch64-gnu" => Self::LinuxAarch64Gnu,
            "macos-x86_64" => Self::MacosX86_64,
            "macos-aarch64" => Self::MacosAarch64,
            "windows-x86_64" => Self::WindowsX86_64,
            _ => return None,
        })
    }
}

/// One exact first-party driver/runtime/platform claim.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdapterDriverRequirement {
    driver: InstallationDriver,
    runtime_version: InstallationSymbol,
    platforms: Vec<AdapterPlatform>,
    conformance_hash: GeneratedArtifactHash,
}

impl AdapterDriverRequirement {
    /// Creates one exact driver requirement; ranges and empty platforms are impossible.
    pub fn new(
        driver: InstallationDriver,
        runtime_version: InstallationSymbol,
        mut platforms: Vec<AdapterPlatform>,
        conformance_hash: GeneratedArtifactHash,
    ) -> Result<Self, AdapterConformanceError> {
        platforms.sort();
        if platforms.is_empty()
            || platforms.len() > MAX_ADAPTER_PLATFORMS
            || platforms.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(AdapterConformanceError::new(if platforms.is_empty() {
                AdapterConformanceErrorKind::InvalidShape
            } else if platforms.len() > MAX_ADAPTER_PLATFORMS {
                AdapterConformanceErrorKind::LimitExceeded
            } else {
                AdapterConformanceErrorKind::Duplicate
            }));
        }
        Ok(Self {
            driver,
            runtime_version,
            platforms,
            conformance_hash,
        })
    }

    /// First-party transport owner selected by this requirement.
    #[must_use]
    pub const fn driver(&self) -> InstallationDriver {
        self.driver
    }

    /// Exact runtime/toolchain version symbol, never a version range.
    #[must_use]
    pub const fn runtime_version(&self) -> &InstallationSymbol {
        &self.runtime_version
    }

    /// Closed supported platforms.
    #[must_use]
    pub fn platforms(&self) -> &[AdapterPlatform] {
        &self.platforms
    }
}

/// Exact symbolic role required by an adapter.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdapterRoleRequirement {
    name: InstallationSymbol,
    role_hash: ApplicationRoleHash,
    operations: Vec<RoleOperation>,
}

impl AdapterRoleRequirement {
    /// Creates one exact, nonempty, name-only role surface.
    pub fn new(
        name: InstallationSymbol,
        role_hash: ApplicationRoleHash,
        mut operations: Vec<RoleOperation>,
    ) -> Result<Self, AdapterConformanceError> {
        operations.sort();
        if operations.is_empty()
            || operations.len() > crate::MAX_ROLE_OPERATIONS
            || operations.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(AdapterConformanceError::new(if operations.is_empty() {
                AdapterConformanceErrorKind::InvalidShape
            } else if operations.len() > crate::MAX_ROLE_OPERATIONS {
                AdapterConformanceErrorKind::LimitExceeded
            } else {
                AdapterConformanceErrorKind::Duplicate
            }));
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

    /// Exact compiled role identity.
    #[must_use]
    pub const fn role_hash(&self) -> ApplicationRoleHash {
        self.role_hash
    }

    /// Closed named application operations.
    #[must_use]
    pub fn operations(&self) -> &[RoleOperation] {
        &self.operations
    }
}

/// One public operation and bounded golden observation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdapterConformanceProbe {
    name: InstallationSymbol,
    role: InstallationSymbol,
    operation: RoleOperation,
    expected_observation_hash: GeneratedArtifactHash,
    maximum_items: u32,
}

impl AdapterConformanceProbe {
    /// Creates one bounded named-operation probe. It cannot carry code or a method path.
    pub fn new(
        name: InstallationSymbol,
        role: InstallationSymbol,
        operation: RoleOperation,
        expected_observation_hash: GeneratedArtifactHash,
        maximum_items: u32,
    ) -> Result<Self, AdapterConformanceError> {
        if maximum_items == 0 || maximum_items > MAX_ADAPTER_PROBE_ITEMS {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            name,
            role,
            operation,
            expected_observation_hash,
            maximum_items,
        })
    }
}

/// Closed application evolution class. No open version range exists.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdapterEvolutionClass {
    /// Install into an empty selected database.
    EmptyInstall,
    /// Upgrade one exact predecessor through ordinary compatible deployment.
    CompatibleUpgrade,
    /// Upgrade one exact predecessor through one exact reviewed migration.
    ExplicitMigration,
}

impl AdapterEvolutionClass {
    const fn tag(self) -> &'static str {
        match self {
            Self::EmptyInstall => "empty_install",
            Self::CompatibleUpgrade => "compatible_upgrade",
            Self::ExplicitMigration => "explicit_migration",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "empty_install" => Self::EmptyInstall,
            "compatible_upgrade" => Self::CompatibleUpgrade,
            "explicit_migration" => Self::ExplicitMigration,
            _ => return None,
        })
    }
}

/// One exact install/upgrade case and its golden observation identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdapterEvolutionRequirement {
    class: AdapterEvolutionClass,
    predecessor: Option<InstallationContract>,
    successor: InstallationContract,
    migration_hash: Option<MigrationBundleHash>,
    expected_observation_hash: GeneratedArtifactHash,
}

impl AdapterEvolutionRequirement {
    /// Creates a closed exact evolution case.
    pub fn new(
        class: AdapterEvolutionClass,
        predecessor: Option<InstallationContract>,
        successor: InstallationContract,
        migration_hash: Option<MigrationBundleHash>,
        expected_observation_hash: GeneratedArtifactHash,
    ) -> Result<Self, AdapterConformanceError> {
        let shape_valid = match class {
            AdapterEvolutionClass::EmptyInstall => {
                predecessor.is_none() && migration_hash.is_none()
            }
            AdapterEvolutionClass::CompatibleUpgrade => {
                predecessor.is_some_and(|value| value.version() < successor.version())
                    && migration_hash.is_none()
            }
            AdapterEvolutionClass::ExplicitMigration => {
                predecessor.is_some_and(|value| value.version() < successor.version())
                    && migration_hash.is_some()
            }
        };
        if !shape_valid {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::InvalidShape,
            ));
        }
        Ok(Self {
            class,
            predecessor,
            successor,
            migration_hash,
            expected_observation_hash,
        })
    }

    /// Exact successor contract installed by this case.
    #[must_use]
    pub const fn successor(&self) -> InstallationContract {
        self.successor
    }
}

/// Complete bounded adapter-owned input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterConformanceManifestInput {
    /// Stable symbolic adapter name.
    pub adapter: InstallationSymbol,
    /// Exact adapter version; ranges are not representable.
    pub adapter_version: InstallationSymbol,
    /// Exact generated application manifest identity.
    pub application_manifest_hash: ApplicationManifestHash,
    /// Exact compiler-owned application lock identity.
    pub application_lock_hash: ApplicationLockHash,
    /// Exact application lineage.
    pub contract_lineage: ContractLineage,
    /// Closed feature claims, including explicit unavailable/degraded states.
    pub feature_claims: Vec<AdapterFeatureClaim>,
    /// Exact required generated artifacts.
    pub artifacts: Vec<InstallationArtifact>,
    /// Exact least-authority symbolic roles.
    pub roles: Vec<AdapterRoleRequirement>,
    /// Exact first-party driver/runtime/platform claims.
    pub drivers: Vec<AdapterDriverRequirement>,
    /// Bounded public named-operation probes and golden observations.
    pub conformance: Vec<AdapterConformanceProbe>,
    /// Exact empty-install and evolution cases.
    pub evolution: Vec<AdapterEvolutionRequirement>,
}

/// One immutable content-addressed adapter conformance manifest.
#[derive(Clone, Eq, PartialEq)]
pub struct AdapterConformanceManifest {
    input: AdapterConformanceManifestInput,
    identity: AdapterConformanceManifestHash,
    canonical_bytes: Vec<u8>,
}

impl AdapterConformanceManifest {
    /// Compiles one canonical manifest without filesystem, server, or application execution.
    pub fn compile(
        mut input: AdapterConformanceManifestInput,
    ) -> Result<Self, AdapterConformanceError> {
        validate_and_sort(&mut input)?;
        let dto = ManifestDto::from_input(&input);
        let mut canonical_bytes = serde_json::to_vec(&dto).map_err(|_| {
            AdapterConformanceError::new(AdapterConformanceErrorKind::InvalidEncoding)
        })?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_ADAPTER_CONFORMANCE_MANIFEST_BYTES {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::LimitExceeded,
            ));
        }
        let identity = hash_adapter_conformance_manifest(&canonical_bytes);
        Ok(Self {
            input,
            identity,
            canonical_bytes,
        })
    }

    /// Strictly decodes and revalidates one canonical manifest.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, AdapterConformanceError> {
        if bytes.is_empty() || bytes.len() > MAX_ADAPTER_CONFORMANCE_MANIFEST_BYTES {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::LimitExceeded,
            ));
        }
        let dto: ManifestDto = serde_json::from_slice(bytes).map_err(|_| {
            AdapterConformanceError::new(AdapterConformanceErrorKind::InvalidEncoding)
        })?;
        let manifest = Self::compile(dto.into_input()?)?;
        if manifest.canonical_bytes != bytes {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::NonCanonical,
            ));
        }
        Ok(manifest)
    }

    /// Content identity recorded in installation plans and receipts.
    #[must_use]
    pub const fn identity(&self) -> AdapterConformanceManifestHash {
        self.identity
    }

    /// Validated manifest contents.
    #[must_use]
    pub const fn input(&self) -> &AdapterConformanceManifestInput {
        &self.input
    }

    /// Canonical compatibility bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Verifies one installation plan is exactly the adapter-reviewed plan.
    pub fn validate_installation_plan(
        &self,
        plan: &ApplicationInstallationPlan,
    ) -> Result<(), AdapterConformanceError> {
        let planned = plan.input();
        if planned.adapter_manifest_hash != Some(self.identity)
            || planned.manifest_hash != self.input.application_manifest_hash
            || planned.lock_hash != self.input.application_lock_hash
            || planned.target.lineage() != &self.input.contract_lineage
            || self
                .input
                .evolution
                .iter()
                .any(|evolution| evolution.successor != planned.contract)
        {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::IdentityMismatch,
            ));
        }
        let self_artifact_hash = GeneratedArtifactHash::from_bytes(*self.identity.as_bytes());
        if !planned.artifacts.iter().any(|artifact| {
            artifact.kind() == InstallationArtifactKind::AdapterManifest
                && artifact.name() == &self.input.adapter
                && artifact.content_hash() == self_artifact_hash
        }) || self
            .input
            .artifacts
            .iter()
            .any(|required| planned.artifacts.binary_search(required).is_err())
        {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::IdentityMismatch,
            ));
        }
        for required in &self.input.roles {
            let Some(role) = planned
                .roles
                .iter()
                .find(|role| role.name() == required.name())
            else {
                return Err(AdapterConformanceError::new(
                    AdapterConformanceErrorKind::IdentityMismatch,
                ));
            };
            if role.role_hash() != required.role_hash
                || role.desired_operations() != required.operations
            {
                return Err(AdapterConformanceError::new(
                    AdapterConformanceErrorKind::IdentityMismatch,
                ));
            }
        }
        if self
            .input
            .drivers
            .iter()
            .any(|required| planned.drivers.binary_search(&required.driver).is_err())
            || self.input.feature_claims.iter().any(|claim| {
                claim.disposition == AdapterFeatureDisposition::Required
                    && planned
                        .required_features
                        .binary_search(&claim.feature)
                        .is_err()
            })
        {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::IdentityMismatch,
            ));
        }
        Ok(())
    }

    /// Verifies required features and exact driver conformance identities against a closed catalog.
    pub fn validate_catalog(
        &self,
        supported_features: &[InstallationFeature],
        supported_drivers: &[AdapterDriverRequirement],
    ) -> Result<(), AdapterConformanceError> {
        if self.input.feature_claims.iter().any(|claim| {
            claim.disposition == AdapterFeatureDisposition::Required
                && !supported_features.contains(&claim.feature)
        }) {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::UnsupportedFeature,
            ));
        }
        if self
            .input
            .drivers
            .iter()
            .any(|required| !supported_drivers.contains(required))
        {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::UnsupportedDriver,
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for AdapterConformanceManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterConformanceManifest")
            .field("adapter", &self.input.adapter)
            .field("adapter_version", &self.input.adapter_version)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

fn validate_and_sort(
    input: &mut AdapterConformanceManifestInput,
) -> Result<(), AdapterConformanceError> {
    if input.feature_claims.is_empty()
        || input.feature_claims.len() > MAX_ADAPTER_FEATURE_CLAIMS
        || input.artifacts.is_empty()
        || input.artifacts.len() > MAX_ADAPTER_ARTIFACTS
        || input.roles.is_empty()
        || input.roles.len() > MAX_ADAPTER_ROLES
        || input.drivers.is_empty()
        || input.drivers.len() > MAX_ADAPTER_DRIVERS
        || input.conformance.is_empty()
        || input.conformance.len() > MAX_ADAPTER_CONFORMANCE_PROBES
        || input.evolution.is_empty()
        || input.evolution.len() > MAX_ADAPTER_EVOLUTIONS
    {
        return Err(AdapterConformanceError::new(
            AdapterConformanceErrorKind::LimitExceeded,
        ));
    }
    input.feature_claims.sort();
    reject_duplicate(
        input
            .feature_claims
            .windows(2)
            .any(|pair| pair[0].feature == pair[1].feature),
    )?;
    if !input.feature_claims.iter().any(|claim| {
        claim.feature == InstallationFeature::InstallationCampaigns
            && claim.disposition == AdapterFeatureDisposition::Required
    }) {
        return Err(AdapterConformanceError::new(
            AdapterConformanceErrorKind::InvalidShape,
        ));
    }
    input.artifacts.sort();
    reject_duplicate(
        input
            .artifacts
            .windows(2)
            .any(|pair| (pair[0].kind(), pair[0].name()) == (pair[1].kind(), pair[1].name())),
    )?;
    input.roles.sort();
    reject_duplicate(
        input
            .roles
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name),
    )?;
    input.drivers.sort();
    reject_duplicate(
        input
            .drivers
            .windows(2)
            .any(|pair| pair[0].driver == pair[1].driver),
    )?;
    input.conformance.sort();
    reject_duplicate(
        input
            .conformance
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name),
    )?;
    for probe in &input.conformance {
        let Some(role) = input.roles.iter().find(|role| role.name == probe.role) else {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::IdentityMismatch,
            ));
        };
        if role.operations.binary_search(&probe.operation).is_err() {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::IdentityMismatch,
            ));
        }
    }
    input.evolution.sort();
    reject_duplicate(input.evolution.windows(2).any(|pair| pair[0] == pair[1]))?;
    let genesis = input
        .evolution
        .iter()
        .filter(|item| item.class == AdapterEvolutionClass::EmptyInstall)
        .collect::<Vec<_>>();
    if genesis.len() != 1
        || input
            .evolution
            .iter()
            .any(|item| item.successor != genesis[0].successor)
    {
        return Err(AdapterConformanceError::new(
            AdapterConformanceErrorKind::InvalidShape,
        ));
    }
    Ok(())
}

fn reject_duplicate(value: bool) -> Result<(), AdapterConformanceError> {
    if value {
        Err(AdapterConformanceError::new(
            AdapterConformanceErrorKind::Duplicate,
        ))
    } else {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDto {
    schema: String,
    adapter: String,
    adapter_version: String,
    application_manifest_hash: String,
    application_lock_hash: String,
    contract_lineage: String,
    feature_claims: Vec<FeatureClaimDto>,
    artifacts: Vec<ArtifactDto>,
    roles: Vec<RoleDto>,
    drivers: Vec<DriverDto>,
    conformance: Vec<ProbeDto>,
    evolution: Vec<EvolutionDto>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureClaimDto {
    feature: String,
    disposition: String,
    limitation: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDto {
    kind: String,
    name: String,
    content_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleDto {
    name: String,
    role_hash: String,
    operations: Vec<OperationDto>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationDto {
    kind: String,
    name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverDto {
    driver: String,
    runtime_version: String,
    platforms: Vec<String>,
    conformance_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeDto {
    name: String,
    role: String,
    operation: OperationDto,
    expected_observation_hash: String,
    maximum_items: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionDto {
    class: String,
    predecessor: Option<ContractDto>,
    successor: ContractDto,
    migration_hash: Option<String>,
    expected_observation_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractDto {
    version: u64,
    bundle_hash: String,
}

impl ManifestDto {
    fn from_input(input: &AdapterConformanceManifestInput) -> Self {
        Self {
            schema: ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V1.to_owned(),
            adapter: input.adapter.as_str().to_owned(),
            adapter_version: input.adapter_version.as_str().to_owned(),
            application_manifest_hash: hex(input.application_manifest_hash.as_bytes()),
            application_lock_hash: hex(input.application_lock_hash.as_bytes()),
            contract_lineage: input.contract_lineage.as_str().to_owned(),
            feature_claims: input
                .feature_claims
                .iter()
                .map(FeatureClaimDto::from_claim)
                .collect(),
            artifacts: input
                .artifacts
                .iter()
                .map(ArtifactDto::from_artifact)
                .collect(),
            roles: input.roles.iter().map(RoleDto::from_role).collect(),
            drivers: input.drivers.iter().map(DriverDto::from_driver).collect(),
            conformance: input.conformance.iter().map(ProbeDto::from_probe).collect(),
            evolution: input
                .evolution
                .iter()
                .map(EvolutionDto::from_evolution)
                .collect(),
        }
    }

    fn into_input(self) -> Result<AdapterConformanceManifestInput, AdapterConformanceError> {
        if self.schema != ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V1 {
            return Err(AdapterConformanceError::new(
                AdapterConformanceErrorKind::InvalidEncoding,
            ));
        }
        Ok(AdapterConformanceManifestInput {
            adapter: symbol(self.adapter)?,
            adapter_version: symbol(self.adapter_version)?,
            application_manifest_hash: ApplicationManifestHash::from_bytes(parse_hash(
                &self.application_manifest_hash,
            )?),
            application_lock_hash: ApplicationLockHash::from_bytes(parse_hash(
                &self.application_lock_hash,
            )?),
            contract_lineage: ContractLineage::new(self.contract_lineage)
                .map_err(|_| invalid_encoding())?,
            feature_claims: self
                .feature_claims
                .into_iter()
                .map(FeatureClaimDto::into_claim)
                .collect::<Result<_, _>>()?,
            artifacts: self
                .artifacts
                .into_iter()
                .map(ArtifactDto::into_artifact)
                .collect::<Result<_, _>>()?,
            roles: self
                .roles
                .into_iter()
                .map(RoleDto::into_role)
                .collect::<Result<_, _>>()?,
            drivers: self
                .drivers
                .into_iter()
                .map(DriverDto::into_driver)
                .collect::<Result<_, _>>()?,
            conformance: self
                .conformance
                .into_iter()
                .map(ProbeDto::into_probe)
                .collect::<Result<_, _>>()?,
            evolution: self
                .evolution
                .into_iter()
                .map(EvolutionDto::into_evolution)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl FeatureClaimDto {
    fn from_claim(claim: &AdapterFeatureClaim) -> Self {
        Self {
            feature: claim.feature.tag().to_owned(),
            disposition: claim.disposition.tag().to_owned(),
            limitation: claim.limitation.map(|value| value.tag().to_owned()),
        }
    }

    fn into_claim(self) -> Result<AdapterFeatureClaim, AdapterConformanceError> {
        AdapterFeatureClaim::new(
            InstallationFeature::parse(&self.feature).ok_or_else(invalid_encoding)?,
            AdapterFeatureDisposition::parse(&self.disposition).ok_or_else(invalid_encoding)?,
            self.limitation
                .map(|value| AdapterLimitation::parse(&value).ok_or_else(invalid_encoding))
                .transpose()?,
        )
    }
}

impl ArtifactDto {
    fn from_artifact(value: &InstallationArtifact) -> Self {
        Self {
            kind: value.kind().tag().to_owned(),
            name: value.name().as_str().to_owned(),
            content_hash: hex(value.content_hash().as_bytes()),
        }
    }

    fn into_artifact(self) -> Result<InstallationArtifact, AdapterConformanceError> {
        Ok(InstallationArtifact::new(
            InstallationArtifactKind::parse(&self.kind).ok_or_else(invalid_encoding)?,
            symbol(self.name)?,
            GeneratedArtifactHash::from_bytes(parse_hash(&self.content_hash)?),
        ))
    }
}

impl RoleDto {
    fn from_role(value: &AdapterRoleRequirement) -> Self {
        Self {
            name: value.name.as_str().to_owned(),
            role_hash: hex(value.role_hash.as_bytes()),
            operations: value
                .operations
                .iter()
                .map(OperationDto::from_operation)
                .collect(),
        }
    }

    fn into_role(self) -> Result<AdapterRoleRequirement, AdapterConformanceError> {
        AdapterRoleRequirement::new(
            symbol(self.name)?,
            ApplicationRoleHash::from_bytes(parse_hash(&self.role_hash)?),
            self.operations
                .into_iter()
                .map(OperationDto::into_operation)
                .collect::<Result<_, _>>()?,
        )
    }
}

impl OperationDto {
    fn from_operation(value: &RoleOperation) -> Self {
        Self {
            kind: value.kind().tag().to_owned(),
            name: value.name().as_str().to_owned(),
        }
    }

    fn into_operation(self) -> Result<RoleOperation, AdapterConformanceError> {
        Ok(RoleOperation::new(
            RoleOperationKind::parse(&self.kind).ok_or_else(invalid_encoding)?,
            symbol(self.name)?,
        ))
    }
}

impl DriverDto {
    fn from_driver(value: &AdapterDriverRequirement) -> Self {
        Self {
            driver: value.driver.tag().to_owned(),
            runtime_version: value.runtime_version.as_str().to_owned(),
            platforms: value
                .platforms
                .iter()
                .map(|value| value.tag().to_owned())
                .collect(),
            conformance_hash: hex(value.conformance_hash.as_bytes()),
        }
    }

    fn into_driver(self) -> Result<AdapterDriverRequirement, AdapterConformanceError> {
        AdapterDriverRequirement::new(
            InstallationDriver::parse(&self.driver).ok_or_else(invalid_encoding)?,
            symbol(self.runtime_version)?,
            self.platforms
                .into_iter()
                .map(|value| AdapterPlatform::parse(&value).ok_or_else(invalid_encoding))
                .collect::<Result<_, _>>()?,
            GeneratedArtifactHash::from_bytes(parse_hash(&self.conformance_hash)?),
        )
    }
}

impl ProbeDto {
    fn from_probe(value: &AdapterConformanceProbe) -> Self {
        Self {
            name: value.name.as_str().to_owned(),
            role: value.role.as_str().to_owned(),
            operation: OperationDto::from_operation(&value.operation),
            expected_observation_hash: hex(value.expected_observation_hash.as_bytes()),
            maximum_items: value.maximum_items,
        }
    }

    fn into_probe(self) -> Result<AdapterConformanceProbe, AdapterConformanceError> {
        AdapterConformanceProbe::new(
            symbol(self.name)?,
            symbol(self.role)?,
            self.operation.into_operation()?,
            GeneratedArtifactHash::from_bytes(parse_hash(&self.expected_observation_hash)?),
            self.maximum_items,
        )
    }
}

impl EvolutionDto {
    fn from_evolution(value: &AdapterEvolutionRequirement) -> Self {
        Self {
            class: value.class.tag().to_owned(),
            predecessor: value.predecessor.map(ContractDto::from_contract),
            successor: ContractDto::from_contract(value.successor),
            migration_hash: value.migration_hash.map(|value| hex(value.as_bytes())),
            expected_observation_hash: hex(value.expected_observation_hash.as_bytes()),
        }
    }

    fn into_evolution(self) -> Result<AdapterEvolutionRequirement, AdapterConformanceError> {
        AdapterEvolutionRequirement::new(
            AdapterEvolutionClass::parse(&self.class).ok_or_else(invalid_encoding)?,
            self.predecessor
                .map(ContractDto::into_contract)
                .transpose()?,
            self.successor.into_contract()?,
            self.migration_hash
                .map(|value| parse_hash(&value).map(MigrationBundleHash::from_bytes))
                .transpose()?,
            GeneratedArtifactHash::from_bytes(parse_hash(&self.expected_observation_hash)?),
        )
    }
}

impl ContractDto {
    fn from_contract(value: InstallationContract) -> Self {
        Self {
            version: value.version().get(),
            bundle_hash: hex(value.bundle_hash().as_bytes()),
        }
    }

    fn into_contract(self) -> Result<InstallationContract, AdapterConformanceError> {
        Ok(InstallationContract::new(
            ContractVersion::new(self.version).ok_or_else(invalid_encoding)?,
            ContractBundleHash::from_bytes(parse_hash(&self.bundle_hash)?),
        ))
    }
}

fn symbol(value: String) -> Result<InstallationSymbol, AdapterConformanceError> {
    InstallationSymbol::new(value).map_err(|_| invalid_encoding())
}

fn invalid_encoding() -> AdapterConformanceError {
    AdapterConformanceError::new(AdapterConformanceErrorKind::InvalidEncoding)
}

fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_hash(value: &str) -> Result<[u8; 32], AdapterConformanceError> {
    if value.len() != 64 {
        return Err(invalid_encoding());
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (nibble(pair[0]).ok_or_else(invalid_encoding)? << 4)
            | nibble(pair[1]).ok_or_else(invalid_encoding)?;
    }
    Ok(bytes)
}

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
