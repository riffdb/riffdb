//! Adapter-owned, compiler-closed export/reimport mappings and receipts.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{ContractBundle, RecordSchema};
use riffdb_types::{
    AdapterConformanceManifestHash, ApplicationExportManifestHash,
    ApplicationPortabilityManifestHash, ApplicationReimportReceiptHash, CanonicalValue,
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, EntityTypeId,
    GeneratedArtifactHash, MigrationBundleHash, QueryModuleHash, decode_canonical_value,
    encode_canonical_value, hash_application_portability_manifest,
    hash_application_reimport_receipt,
};
use serde::{Deserialize, Serialize};

use crate::{AdapterConformanceManifest, InstallationSymbol, RoleOperationKind};

/// Canonical adapter-owned reimport mapping schema.
pub const APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V1: &str =
    "riffdb.application-portability-manifest/v1";
/// Canonical adapter-owned reimport mapping schema with compiler-owned commands.
pub const APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V2: &str =
    "riffdb.application-portability-manifest/v2";
/// Canonical mapping manifest with compiler-owned commands and typed observations.
pub const APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V3: &str =
    "riffdb.application-portability-manifest/v3";
/// Canonical terminal reconciliation receipt schema.
pub const APPLICATION_REIMPORT_RECEIPT_SCHEMA_V1: &str = "riffdb.application-reimport-receipt/v1";
/// Canonical terminal reconciliation receipt for a compiler-owned v2 mapping.
pub const APPLICATION_REIMPORT_RECEIPT_SCHEMA_V2: &str = "riffdb.application-reimport-receipt/v2";
/// Maximum canonical bytes for either portability document.
pub const MAX_APPLICATION_PORTABILITY_DOCUMENT_BYTES: usize = 1_048_576;
/// Maximum portable record mappings.
pub const MAX_APPLICATION_PORTABLE_MAPPINGS: usize = 512;
/// Maximum symbolic field bindings in one command mapping.
pub const MAX_APPLICATION_PORTABLE_FIELD_BINDINGS: usize = 256;
/// Maximum explicit omissions.
pub const MAX_APPLICATION_PORTABLE_OMISSIONS: usize = 512;
/// Maximum reconciliation observations.
pub const MAX_APPLICATION_REIMPORT_OBSERVATIONS: usize = 256;
/// Maximum exact parameters carried by one reconciliation observation.
pub const MAX_APPLICATION_REIMPORT_OBSERVATION_PARAMETERS: usize = 256;

/// Public-safe portability-manifest failure class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationPortabilityErrorKind {
    /// A fixed byte or collection ceiling was exceeded.
    LimitExceeded,
    /// JSON, hash, or a closed symbolic value was invalid.
    InvalidEncoding,
    /// The declared schema is unsupported.
    UnsupportedVersion,
    /// Valid input bytes were not canonical.
    NonCanonical,
    /// Required relationships between fields were not satisfied.
    InvalidShape,
    /// One symbolic identity was duplicated.
    Duplicate,
    /// Terminal reconciliation did not match the declared manifest.
    ReconciliationMismatch,
}

/// Bounded error without exported application values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationPortabilityError(ApplicationPortabilityErrorKind);

impl ApplicationPortabilityError {
    const fn new(kind: ApplicationPortabilityErrorKind) -> Self {
        Self(kind)
    }

    /// Stable public-safe failure class.
    #[must_use]
    pub const fn kind(self) -> ApplicationPortabilityErrorKind {
        self.0
    }
}

impl fmt::Display for ApplicationPortabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.0 {
            ApplicationPortabilityErrorKind::LimitExceeded => {
                "application portability document exceeds a fixed limit"
            }
            ApplicationPortabilityErrorKind::InvalidEncoding => {
                "application portability document encoding is invalid"
            }
            ApplicationPortabilityErrorKind::UnsupportedVersion => {
                "application portability document version is unsupported"
            }
            ApplicationPortabilityErrorKind::NonCanonical => {
                "application portability document is not canonical"
            }
            ApplicationPortabilityErrorKind::InvalidShape => {
                "application portability document shape is invalid"
            }
            ApplicationPortabilityErrorKind::Duplicate => {
                "application portability document contains a duplicate identity"
            }
            ApplicationPortabilityErrorKind::ReconciliationMismatch => {
                "application reimport reconciliation does not match its manifest"
            }
        })
    }
}

impl Error for ApplicationPortabilityError {}

/// Portable application record classes accepted by compiled reimport mappings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PortableRecordClass {
    /// Current symbolic entity state.
    Entity,
    /// Retained typed domain event.
    Event,
}

impl PortableRecordClass {
    const fn tag(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Event => "event",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "entity" => Some(Self::Entity),
            "event" => Some(Self::Event),
            _ => None,
        }
    }
}

/// One compiler-visible field-to-input binding.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PortableFieldBinding {
    source_field: InstallationSymbol,
    command_input: InstallationSymbol,
}

impl PortableFieldBinding {
    /// Binds one symbolic exported field to one symbolic command input.
    #[must_use]
    pub const fn new(source_field: InstallationSymbol, command_input: InstallationSymbol) -> Self {
        Self {
            source_field,
            command_input,
        }
    }

    /// Exported symbolic field name.
    #[must_use]
    pub const fn source_field(&self) -> &InstallationSymbol {
        &self.source_field
    }

    /// Declared symbolic command-input name.
    #[must_use]
    pub const fn command_input(&self) -> &InstallationSymbol {
        &self.command_input
    }
}

/// Closed reimport implementation. There is no callback, method path, or transaction escape.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PortableReimportStrategy {
    /// Invoke one operator-only, compiler-owned reimport command.
    ReimportCommand {
        /// Symbolic compiled reimport-command name.
        command: InstallationSymbol,
    },
    /// Frozen v1 mapping retained only so old manifests remain inspectable.
    #[doc(hidden)]
    LegacyApplicationCommand {
        command: InstallationSymbol,
        idempotency_input: InstallationSymbol,
        record_input: Option<InstallationSymbol>,
        fields: Vec<PortableFieldBinding>,
    },
    /// Delegate this class to one exact accepted application migration.
    Migration {
        /// Exact reviewed migration identity.
        migration_hash: MigrationBundleHash,
    },
}

impl PortableReimportStrategy {
    /// Constructs an exact compiler-owned reimport-command mapping.
    #[must_use]
    pub const fn reimport_command(command: InstallationSymbol) -> Self {
        Self::ReimportCommand { command }
    }

    fn legacy_command(
        command: InstallationSymbol,
        idempotency_input: InstallationSymbol,
        mut fields: Vec<PortableFieldBinding>,
    ) -> Result<Self, ApplicationPortabilityError> {
        fields.sort();
        if fields.is_empty() || fields.len() > MAX_APPLICATION_PORTABLE_FIELD_BINDINGS {
            return Err(ApplicationPortabilityError::new(if fields.is_empty() {
                ApplicationPortabilityErrorKind::InvalidShape
            } else {
                ApplicationPortabilityErrorKind::LimitExceeded
            }));
        }
        let unique_sources = fields
            .iter()
            .map(|field| field.source_field.clone())
            .collect::<BTreeSet<_>>();
        let unique_inputs = fields
            .iter()
            .map(|field| field.command_input.clone())
            .collect::<BTreeSet<_>>();
        if unique_sources.len() != fields.len()
            || unique_inputs.len() != fields.len()
            || unique_inputs.contains(&idempotency_input)
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::Duplicate,
            ));
        }
        Ok(Self::LegacyApplicationCommand {
            command,
            idempotency_input,
            record_input: None,
            fields,
        })
    }

    const fn legacy_bounded_collection_command(
        command: InstallationSymbol,
        idempotency_input: InstallationSymbol,
        record_input: InstallationSymbol,
    ) -> Self {
        Self::LegacyApplicationCommand {
            command,
            idempotency_input,
            record_input: Some(record_input),
            fields: Vec::new(),
        }
    }

    /// Selects one exact accepted migration with no record callback.
    #[must_use]
    pub const fn migration(migration_hash: MigrationBundleHash) -> Self {
        Self::Migration { migration_hash }
    }
}

/// One symbolic exported record mapping.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PortableRecordMapping {
    class: PortableRecordClass,
    symbol: InstallationSymbol,
    strategy: PortableReimportStrategy,
}

impl PortableRecordMapping {
    /// Creates one closed symbolic mapping.
    #[must_use]
    pub const fn new(
        class: PortableRecordClass,
        symbol: InstallationSymbol,
        strategy: PortableReimportStrategy,
    ) -> Self {
        Self {
            class,
            symbol,
            strategy,
        }
    }

    /// Portable record class.
    #[must_use]
    pub const fn class(&self) -> PortableRecordClass {
        self.class
    }

    /// Stable contract symbol.
    #[must_use]
    pub const fn symbol(&self) -> &InstallationSymbol {
        &self.symbol
    }

    /// Compiler-closed reimport strategy.
    #[must_use]
    pub const fn strategy(&self) -> &PortableReimportStrategy {
        &self.strategy
    }
}

/// Supporting export classes whose history may be explicitly omitted on reimport.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PortableOmissionClass {
    /// One named retained event type is not regenerated.
    Event,
    /// Provenance records are not regenerated.
    Provenance,
    /// Public audit records are not regenerated.
    PublicAudit,
}

impl PortableOmissionClass {
    const fn tag(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Provenance => "provenance",
            Self::PublicAudit => "public_audit",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "event" => Some(Self::Event),
            "provenance" => Some(Self::Provenance),
            "public_audit" => Some(Self::PublicAudit),
            _ => None,
        }
    }
}

/// Closed reason a historical/supporting class is not regenerated.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PortableOmissionReason {
    /// The destination reconstructs current state but cannot regenerate historical facts.
    HistoricalRecordsNotRegenerable,
    /// The adapter deliberately excludes the class from its portability claim.
    NotPortable,
}

impl PortableOmissionReason {
    const fn tag(self) -> &'static str {
        match self {
            Self::HistoricalRecordsNotRegenerable => "historical_records_not_regenerable",
            Self::NotPortable => "not_portable",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "historical_records_not_regenerable" => Some(Self::HistoricalRecordsNotRegenerable),
            "not_portable" => Some(Self::NotPortable),
            _ => None,
        }
    }
}

/// One exact declared omission.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PortableOmission {
    class: PortableOmissionClass,
    symbol: Option<InstallationSymbol>,
    reason: PortableOmissionReason,
}

impl PortableOmission {
    /// Constructs one omission; only event omissions carry a symbol.
    pub fn new(
        class: PortableOmissionClass,
        symbol: Option<InstallationSymbol>,
        reason: PortableOmissionReason,
    ) -> Result<Self, ApplicationPortabilityError> {
        if (class == PortableOmissionClass::Event) != symbol.is_some() {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::InvalidShape,
            ));
        }
        Ok(Self {
            class,
            symbol,
            reason,
        })
    }

    /// Omitted record class.
    #[must_use]
    pub const fn class(&self) -> PortableOmissionClass {
        self.class
    }
}

/// One named query observation used to reconcile the empty-database reimport.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportObservationParameter {
    name: InstallationSymbol,
    canonical_value: Vec<u8>,
}

impl ReimportObservationParameter {
    /// Creates one exact scalar parameter. Nested, vector, and ID-addressed enum values are absent.
    pub fn new(
        name: InstallationSymbol,
        value: CanonicalValue,
    ) -> Result<Self, ApplicationPortabilityError> {
        if matches!(
            value,
            CanonicalValue::Enum { .. }
                | CanonicalValue::List(_)
                | CanonicalValue::Record(_)
                | CanonicalValue::Vector(_)
        ) {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::InvalidShape,
            ));
        }
        let canonical_value = encode_canonical_value(&value).map_err(|_| {
            ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
        })?;
        Ok(Self {
            name,
            canonical_value,
        })
    }

    /// Symbolic query parameter name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Decodes the exact typed value retained by the manifest.
    pub fn value(&self) -> Result<CanonicalValue, ApplicationPortabilityError> {
        decode_canonical_value(&self.canonical_value).map_err(|_| {
            ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
        })
    }
}

/// One named query observation used to reconcile the empty-database reimport.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportObservation {
    name: InstallationSymbol,
    query: InstallationSymbol,
    module_hash: Option<QueryModuleHash>,
    parameters: Vec<ReimportObservationParameter>,
    expected_hash: GeneratedArtifactHash,
    maximum_items: u32,
}

impl ReimportObservation {
    /// Creates one bounded symbolic observation.
    pub fn new(
        name: InstallationSymbol,
        query: InstallationSymbol,
        expected_hash: GeneratedArtifactHash,
        maximum_items: u32,
    ) -> Result<Self, ApplicationPortabilityError> {
        Self::new_checked(name, query, None, Vec::new(), expected_hash, maximum_items)
    }

    /// Creates one bounded symbolic observation with exact typed scalar parameters.
    pub fn new_with_parameters(
        name: InstallationSymbol,
        query: InstallationSymbol,
        module_hash: QueryModuleHash,
        parameters: Vec<ReimportObservationParameter>,
        expected_hash: GeneratedArtifactHash,
        maximum_items: u32,
    ) -> Result<Self, ApplicationPortabilityError> {
        Self::new_checked(
            name,
            query,
            Some(module_hash),
            parameters,
            expected_hash,
            maximum_items,
        )
    }

    fn new_checked(
        name: InstallationSymbol,
        query: InstallationSymbol,
        module_hash: Option<QueryModuleHash>,
        mut parameters: Vec<ReimportObservationParameter>,
        expected_hash: GeneratedArtifactHash,
        maximum_items: u32,
    ) -> Result<Self, ApplicationPortabilityError> {
        if maximum_items == 0 || maximum_items > crate::MAX_ADAPTER_PROBE_ITEMS {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::LimitExceeded,
            ));
        }
        if parameters.len() > MAX_APPLICATION_REIMPORT_OBSERVATION_PARAMETERS {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::LimitExceeded,
            ));
        }
        parameters.sort();
        if parameters
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::Duplicate,
            ));
        }
        Ok(Self {
            name,
            query,
            module_hash,
            parameters,
            expected_hash,
            maximum_items,
        })
    }

    /// Stable observation name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Named query used for reconciliation.
    #[must_use]
    pub const fn query(&self) -> &InstallationSymbol {
        &self.query
    }

    /// Exact query module identity for V3 observations.
    #[must_use]
    pub const fn module_hash(&self) -> Option<QueryModuleHash> {
        self.module_hash
    }

    /// Exact typed parameters in canonical name order.
    #[must_use]
    pub fn parameters(&self) -> &[ReimportObservationParameter] {
        &self.parameters
    }

    /// Expected application-level observation digest.
    #[must_use]
    pub const fn expected_hash(&self) -> GeneratedArtifactHash {
        self.expected_hash
    }

    /// Maximum total result records accepted from this bounded observation.
    #[must_use]
    pub const fn maximum_items(&self) -> u32 {
        self.maximum_items
    }
}

/// Complete adapter-owned portability input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationPortabilityManifestInput {
    /// Exact adapter conformance manifest that owns these mappings.
    pub adapter_manifest_hash: AdapterConformanceManifestHash,
    /// Exported/destination contract lineage.
    pub contract_lineage: ContractLineage,
    /// Exact destination contract version.
    pub contract_version: ContractVersion,
    /// Exact destination bundle identity.
    pub contract_bundle_hash: ContractBundleHash,
    /// Entity/event mappings to commands or one accepted migration.
    pub mappings: Vec<PortableRecordMapping>,
    /// Explicit unsupported historical/supporting classes.
    pub omissions: Vec<PortableOmission>,
    /// Bounded application-level reconciliation observations.
    pub observations: Vec<ReimportObservation>,
}

/// Immutable content-addressed adapter portability manifest.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationPortabilityManifest {
    schema: ApplicationPortabilityManifestSchema,
    input: ApplicationPortabilityManifestInput,
    identity: ApplicationPortabilityManifestHash,
    canonical_bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplicationPortabilityManifestSchema {
    V1,
    V2,
    V3,
}

impl ApplicationPortabilityManifestSchema {
    const fn name(self) -> &'static str {
        match self {
            Self::V1 => APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V1,
            Self::V2 => APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V2,
            Self::V3 => APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V3,
        }
    }
}

impl ApplicationPortabilityManifest {
    /// Compiles one bounded mapping manifest without executing application code.
    pub fn compile(
        input: ApplicationPortabilityManifestInput,
    ) -> Result<Self, ApplicationPortabilityError> {
        Self::compile_version(input, ApplicationPortabilityManifestSchema::V3)
    }

    fn compile_version(
        mut input: ApplicationPortabilityManifestInput,
        schema: ApplicationPortabilityManifestSchema,
    ) -> Result<Self, ApplicationPortabilityError> {
        validate_manifest(&mut input, schema)?;
        let dto = PortabilityManifestDto::from_input(&input, schema);
        let canonical_bytes = canonical_bytes(&dto)?;
        let identity = hash_application_portability_manifest(&canonical_bytes);
        Ok(Self {
            schema,
            input,
            identity,
            canonical_bytes,
        })
    }

    /// Strictly decodes one canonical manifest, retaining frozen v1 readability.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, ApplicationPortabilityError> {
        checked_size(bytes)?;
        let dto: PortabilityManifestDto = serde_json::from_slice(bytes).map_err(|_| {
            ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
        })?;
        let schema = match dto.schema.as_str() {
            APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V1 => ApplicationPortabilityManifestSchema::V1,
            APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V2 => ApplicationPortabilityManifestSchema::V2,
            APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V3 => ApplicationPortabilityManifestSchema::V3,
            _ => {
                return Err(ApplicationPortabilityError::new(
                    ApplicationPortabilityErrorKind::UnsupportedVersion,
                ));
            }
        };
        let compiled = Self::compile_version(dto.into_input(schema)?, schema)?;
        if compiled.canonical_bytes != bytes {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::NonCanonical,
            ));
        }
        Ok(compiled)
    }

    /// Exact manifest content identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationPortabilityManifestHash {
        self.identity
    }

    /// Validated mappings and reconciliation contract.
    #[must_use]
    pub const fn input(&self) -> &ApplicationPortabilityManifestInput {
        &self.input
    }

    /// Exact canonical JSON bytes with one trailing newline.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Verifies every mapping against one exact compiler-owned contract bundle.
    ///
    /// This is the required compilation boundary before a reimport runner may
    /// accept the manifest. It resolves symbolic records, commands, source
    /// fields, command inputs, and exact value types; runtime code never
    /// interprets a caller-provided callback or field identifier.
    pub fn validate_compiled_contract(
        &self,
        bundle: &ContractBundle,
    ) -> Result<(), ApplicationPortabilityError> {
        if bundle.lineage() != &self.input.contract_lineage
            || bundle.contract_version() != self.input.contract_version
            || bundle.bundle_hash() != self.input.contract_bundle_hash
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        for mapping in &self.input.mappings {
            let source = match mapping.class {
                PortableRecordClass::Entity => bundle
                    .schema()
                    .entities()
                    .iter()
                    .find(|entity| entity.name() == mapping.symbol.as_str())
                    .map(|entity| entity.record()),
                PortableRecordClass::Event => bundle
                    .schema()
                    .events()
                    .iter()
                    .find(|event| event.name() == mapping.symbol.as_str())
                    .map(|event| event.payload()),
            }
            .ok_or_else(|| {
                ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidShape)
            })?;
            match &mapping.strategy {
                PortableReimportStrategy::ReimportCommand { command } => {
                    if mapping.class != PortableRecordClass::Entity {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                    let command = bundle
                        .commands()
                        .iter()
                        .find(|plan| plan.name() == command.as_str())
                        .ok_or_else(|| {
                            ApplicationPortabilityError::new(
                                ApplicationPortabilityErrorKind::InvalidShape,
                            )
                        })?;
                    let target = command.input().record();
                    let [target_field] = target.fields() else {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    };
                    let Some((element, maximum)) = target_field.value_type().list_parts() else {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    };
                    if !command.is_reimport()
                        || command.idempotency_input().is_some()
                        || maximum == 0
                        || element.record_ref() != Some(source.owner())
                    {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                }
                PortableReimportStrategy::LegacyApplicationCommand {
                    command,
                    idempotency_input,
                    record_input,
                    fields,
                } => {
                    let command = bundle
                        .commands()
                        .iter()
                        .find(|plan| plan.name() == command.as_str())
                        .ok_or_else(|| {
                            ApplicationPortabilityError::new(
                                ApplicationPortabilityErrorKind::InvalidShape,
                            )
                        })?;
                    let target = command.input().record();
                    if let Some(record_input) = record_input {
                        let target_field = field_by_name(target, record_input.as_str())
                            .ok_or_else(|| {
                                ApplicationPortabilityError::new(
                                    ApplicationPortabilityErrorKind::InvalidShape,
                                )
                            })?;
                        let Some((element, maximum)) = target_field.value_type().list_parts()
                        else {
                            return Err(ApplicationPortabilityError::new(
                                ApplicationPortabilityErrorKind::InvalidShape,
                            ));
                        };
                        if maximum == 0 || element.record_ref() != Some(source.owner()) {
                            return Err(ApplicationPortabilityError::new(
                                ApplicationPortabilityErrorKind::InvalidShape,
                            ));
                        }
                    } else {
                        for binding in fields {
                            let source_field = field_by_name(source, binding.source_field.as_str())
                                .ok_or_else(|| {
                                    ApplicationPortabilityError::new(
                                        ApplicationPortabilityErrorKind::InvalidShape,
                                    )
                                })?;
                            let target_field =
                                field_by_name(target, binding.command_input.as_str()).ok_or_else(
                                    || {
                                        ApplicationPortabilityError::new(
                                            ApplicationPortabilityErrorKind::InvalidShape,
                                        )
                                    },
                                )?;
                            if source_field.value_type() != target_field.value_type() {
                                return Err(ApplicationPortabilityError::new(
                                    ApplicationPortabilityErrorKind::InvalidShape,
                                ));
                            }
                        }
                    }
                    let idempotency_field = field_by_name(target, idempotency_input.as_str());
                    if idempotency_field.is_none()
                        || command.idempotency_input() != idempotency_field.map(|field| field.id())
                        || target.fields().iter().any(|target_field| {
                            target_field.name() != idempotency_input.as_str()
                                && record_input.as_ref().is_none_or(|record_input| {
                                    record_input.as_str() != target_field.name()
                                })
                                && !fields.iter().any(|binding| {
                                    binding.command_input.as_str() == target_field.name()
                                })
                        })
                    {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                }
                PortableReimportStrategy::Migration { .. } => {}
            }
        }
        Ok(())
    }

    /// Derives the only legal entity reconstitution order from required relationships.
    ///
    /// The returned stable IDs are internal compiler evidence. They are used to
    /// constrain symbolic portability export and reimport; they never cross an
    /// application-facing boundary. Every required target must itself be mapped,
    /// parents precede children, and cycles fail closed.
    pub fn compiled_reimport_entity_schedule(
        &self,
        bundle: &ContractBundle,
    ) -> Result<Vec<EntityTypeId>, ApplicationPortabilityError> {
        self.validate_compiled_contract(bundle)?;
        let selected = self
            .input
            .mappings
            .iter()
            .filter(|mapping| mapping.class == PortableRecordClass::Entity)
            .map(|mapping| {
                if !matches!(
                    mapping.strategy,
                    PortableReimportStrategy::ReimportCommand { .. }
                ) {
                    return Err(ApplicationPortabilityError::new(
                        ApplicationPortabilityErrorKind::InvalidShape,
                    ));
                }
                let entity = bundle
                    .schema()
                    .entities()
                    .iter()
                    .find(|entity| entity.name() == mapping.symbol.as_str())
                    .ok_or_else(|| {
                        ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        )
                    })?;
                Ok((entity.id(), entity.name()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if selected.is_empty() {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::InvalidShape,
            ));
        }
        let selected_ids = selected
            .iter()
            .map(|(entity, _)| *entity)
            .collect::<BTreeSet<_>>();
        let mut indegree = selected_ids
            .iter()
            .map(|entity| (*entity, 0_usize))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut dependents = selected_ids
            .iter()
            .map(|entity| (*entity, BTreeSet::new()))
            .collect::<std::collections::BTreeMap<_, _>>();
        for relationship in bundle.schema().relationships() {
            if !selected_ids.contains(&relationship.source_entity()) {
                continue;
            }
            if !selected_ids.contains(&relationship.target_entity()) {
                return Err(ApplicationPortabilityError::new(
                    ApplicationPortabilityErrorKind::InvalidShape,
                ));
            }
            if dependents
                .get_mut(&relationship.target_entity())
                .ok_or_else(|| {
                    ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidShape)
                })?
                .insert(relationship.source_entity())
            {
                let count = indegree
                    .get_mut(&relationship.source_entity())
                    .ok_or_else(|| {
                        ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        )
                    })?;
                *count = count.checked_add(1).ok_or_else(|| {
                    ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::LimitExceeded)
                })?;
            }
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(entity, count)| (*count == 0).then_some(*entity))
            .collect::<BTreeSet<_>>();
        let mut schedule = Vec::with_capacity(selected_ids.len());
        while let Some(entity) = ready.pop_first() {
            schedule.push(entity);
            for dependent in dependents.get(&entity).into_iter().flatten() {
                let count = indegree.get_mut(dependent).ok_or_else(|| {
                    ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidShape)
                })?;
                *count = count.checked_sub(1).ok_or_else(|| {
                    ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidShape)
                })?;
                if *count == 0 {
                    ready.insert(*dependent);
                }
            }
        }
        if schedule.len() != selected_ids.len() {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::InvalidShape,
            ));
        }
        Ok(schedule)
    }

    /// Binds mappings and observations to one exact adapter-owned public surface.
    pub fn validate_adapter_conformance(
        &self,
        adapter: &AdapterConformanceManifest,
    ) -> Result<(), ApplicationPortabilityError> {
        if adapter.identity() != self.input.adapter_manifest_hash
            || adapter.input().contract_lineage != self.input.contract_lineage
            || !adapter.input().evolution.iter().any(|evolution| {
                evolution.successor().version() == self.input.contract_version
                    && evolution.successor().bundle_hash() == self.input.contract_bundle_hash
            })
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        for mapping in &self.input.mappings {
            match &mapping.strategy {
                PortableReimportStrategy::ReimportCommand { command } => {
                    if adapter.input().roles.iter().any(|role| {
                        role.operations().iter().any(|operation| {
                            operation.kind() == RoleOperationKind::Command
                                && operation.name().as_str() == command.as_str()
                        })
                    }) {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                }
                PortableReimportStrategy::LegacyApplicationCommand { command, .. } => {
                    if !adapter.input().roles.iter().any(|role| {
                        role.operations().iter().any(|operation| {
                            operation.kind() == RoleOperationKind::Command
                                && operation.name().as_str() == command.as_str()
                        })
                    }) {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                }
                PortableReimportStrategy::Migration { migration_hash } => {
                    if !adapter
                        .input()
                        .evolution
                        .iter()
                        .any(|evolution| evolution.migration_hash() == Some(*migration_hash))
                    {
                        return Err(ApplicationPortabilityError::new(
                            ApplicationPortabilityErrorKind::InvalidShape,
                        ));
                    }
                }
            }
        }
        if self.input.observations.iter().any(|observation| {
            !adapter.input().roles.iter().any(|role| {
                role.operations().iter().any(|operation| {
                    operation.kind() == RoleOperationKind::Query
                        && operation.name().as_str() == observation.query.as_str()
                })
            })
        }) {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::InvalidShape,
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ApplicationPortabilityManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationPortabilityManifest")
            .field("contract_lineage", &self.input.contract_lineage)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

/// One per-mapping terminal import result without source values.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportMappingResult {
    class: PortableRecordClass,
    symbol: InstallationSymbol,
    records: u64,
    succeeded: u64,
    replayed: u64,
    outcome_hash: GeneratedArtifactHash,
}

impl ReimportMappingResult {
    /// Records one exact completed mapping result.
    pub fn new(
        class: PortableRecordClass,
        symbol: InstallationSymbol,
        records: u64,
        succeeded: u64,
        replayed: u64,
        outcome_hash: GeneratedArtifactHash,
    ) -> Result<Self, ApplicationPortabilityError> {
        if succeeded.checked_add(replayed) != Some(records) {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        Ok(Self {
            class,
            symbol,
            records,
            succeeded,
            replayed,
            outcome_hash,
        })
    }

    /// Portable record class reconciled by this result.
    #[must_use]
    pub const fn class(&self) -> PortableRecordClass {
        self.class
    }

    /// Contract symbol reconciled by this result.
    #[must_use]
    pub const fn symbol(&self) -> &InstallationSymbol {
        &self.symbol
    }

    /// Total source records accounted for.
    #[must_use]
    pub const fn records(&self) -> u64 {
        self.records
    }

    /// Stable digest of the persisted command outcomes.
    #[must_use]
    pub const fn outcome_hash(&self) -> GeneratedArtifactHash {
        self.outcome_hash
    }
}

/// One observed named-query digest after reimport.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportObservationResult {
    name: InstallationSymbol,
    actual_hash: GeneratedArtifactHash,
}

impl ReimportObservationResult {
    /// Retains one exact observed digest.
    #[must_use]
    pub const fn new(name: InstallationSymbol, actual_hash: GeneratedArtifactHash) -> Self {
        Self { name, actual_hash }
    }
}

/// Immutable successful reconciliation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationReimportReceiptInput {
    /// Exact mapping manifest used by the importer.
    pub portability_manifest_hash: ApplicationPortabilityManifestHash,
    /// Exact completed source export manifest.
    pub export_manifest_hash: ApplicationExportManifestHash,
    /// New target database identity; it need not equal the source identity.
    pub target_database_id: DatabaseId,
    /// Per-mapping item/outcome evidence.
    pub mappings: Vec<ReimportMappingResult>,
    /// Application-level observation digests.
    pub observations: Vec<ReimportObservationResult>,
}

/// Canonical success receipt produced only after exact reconciliation.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationReimportReceipt {
    schema: ApplicationReimportReceiptSchema,
    input: ApplicationReimportReceiptInput,
    identity: ApplicationReimportReceiptHash,
    canonical_bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplicationReimportReceiptSchema {
    V1,
    V2,
}

impl ApplicationReimportReceiptSchema {
    const fn for_manifest(manifest: &ApplicationPortabilityManifest) -> Self {
        match manifest.schema {
            ApplicationPortabilityManifestSchema::V1 => Self::V1,
            ApplicationPortabilityManifestSchema::V2 | ApplicationPortabilityManifestSchema::V3 => {
                Self::V2
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::V1 => APPLICATION_REIMPORT_RECEIPT_SCHEMA_V1,
            Self::V2 => APPLICATION_REIMPORT_RECEIPT_SCHEMA_V2,
        }
    }
}

impl ApplicationReimportReceipt {
    /// Seals a receipt only when all mappings and observations match the manifest.
    pub fn reconcile(
        manifest: &ApplicationPortabilityManifest,
        export_manifest_hash: ApplicationExportManifestHash,
        target_database_id: DatabaseId,
        mut mappings: Vec<ReimportMappingResult>,
        mut observations: Vec<ReimportObservationResult>,
    ) -> Result<Self, ApplicationPortabilityError> {
        mappings.sort();
        observations.sort();
        if mappings.len() != manifest.input.mappings.len()
            || mappings
                .iter()
                .zip(&manifest.input.mappings)
                .any(|(result, mapping)| {
                    result.class != mapping.class || result.symbol != mapping.symbol
                })
            || observations.len() != manifest.input.observations.len()
            || observations
                .iter()
                .zip(&manifest.input.observations)
                .any(|(actual, expected)| {
                    actual.name != expected.name || actual.actual_hash != expected.expected_hash
                })
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        let input = ApplicationReimportReceiptInput {
            portability_manifest_hash: manifest.identity,
            export_manifest_hash,
            target_database_id,
            mappings,
            observations,
        };
        let schema = ApplicationReimportReceiptSchema::for_manifest(manifest);
        let canonical_bytes = canonical_bytes(&ReimportReceiptDto::from_input(&input, schema))?;
        let identity = hash_application_reimport_receipt(&canonical_bytes);
        Ok(Self {
            schema,
            input,
            identity,
            canonical_bytes,
        })
    }

    /// Strictly decodes and revalidates one canonical receipt against its manifest.
    pub fn decode_canonical(
        bytes: &[u8],
        manifest: &ApplicationPortabilityManifest,
    ) -> Result<Self, ApplicationPortabilityError> {
        checked_size(bytes)?;
        let dto: ReimportReceiptDto = serde_json::from_slice(bytes).map_err(|_| {
            ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
        })?;
        let schema = match dto.schema.as_str() {
            APPLICATION_REIMPORT_RECEIPT_SCHEMA_V1 => ApplicationReimportReceiptSchema::V1,
            APPLICATION_REIMPORT_RECEIPT_SCHEMA_V2 => ApplicationReimportReceiptSchema::V2,
            _ => {
                return Err(ApplicationPortabilityError::new(
                    ApplicationPortabilityErrorKind::UnsupportedVersion,
                ));
            }
        };
        if schema != ApplicationReimportReceiptSchema::for_manifest(manifest) {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::UnsupportedVersion,
            ));
        }
        let (export, database, mappings, observations) = dto.into_parts(manifest.identity)?;
        let receipt = Self::reconcile(manifest, export, database, mappings, observations)?;
        if receipt.schema != schema || receipt.canonical_bytes != bytes {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::NonCanonical,
            ));
        }
        Ok(receipt)
    }

    /// Receipt identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationReimportReceiptHash {
        self.identity
    }

    /// Exact reconciled evidence.
    #[must_use]
    pub const fn input(&self) -> &ApplicationReimportReceiptInput {
        &self.input
    }

    /// Exact canonical receipt bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for ApplicationReimportReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationReimportReceipt")
            .field("identity", &self.identity)
            .field(
                "portability_manifest_hash",
                &self.input.portability_manifest_hash,
            )
            .finish_non_exhaustive()
    }
}

fn validate_manifest(
    input: &mut ApplicationPortabilityManifestInput,
    schema: ApplicationPortabilityManifestSchema,
) -> Result<(), ApplicationPortabilityError> {
    if input.mappings.is_empty()
        || input.mappings.len() > MAX_APPLICATION_PORTABLE_MAPPINGS
        || input.omissions.len() > MAX_APPLICATION_PORTABLE_OMISSIONS
        || input.observations.is_empty()
        || input.observations.len() > MAX_APPLICATION_REIMPORT_OBSERVATIONS
    {
        return Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::LimitExceeded,
        ));
    }
    input.mappings.sort();
    input.omissions.sort();
    input.observations.sort();
    if input
        .mappings
        .windows(2)
        .any(|pair| (pair[0].class, &pair[0].symbol) == (pair[1].class, &pair[1].symbol))
        || input.omissions.windows(2).any(|pair| pair[0] == pair[1])
        || input
            .observations
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
    {
        return Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::Duplicate,
        ));
    }
    if input.mappings.iter().any(|mapping| {
        input.omissions.iter().any(|omission| {
            omission.class == PortableOmissionClass::Event
                && mapping.class == PortableRecordClass::Event
                && omission.symbol.as_ref() == Some(&mapping.symbol)
        })
    }) {
        return Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::InvalidShape,
        ));
    }
    let wrong_strategy_version = input.mappings.iter().any(|mapping| match mapping.strategy {
        PortableReimportStrategy::ReimportCommand { .. } => !matches!(
            schema,
            ApplicationPortabilityManifestSchema::V2 | ApplicationPortabilityManifestSchema::V3
        ),
        PortableReimportStrategy::LegacyApplicationCommand { .. } => {
            schema != ApplicationPortabilityManifestSchema::V1
        }
        PortableReimportStrategy::Migration { .. } => false,
    });
    if wrong_strategy_version {
        return Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::InvalidShape,
        ));
    }
    let wrong_observation_version = input.observations.iter().any(|observation| match schema {
        ApplicationPortabilityManifestSchema::V1 | ApplicationPortabilityManifestSchema::V2 => {
            observation.module_hash.is_some() || !observation.parameters.is_empty()
        }
        ApplicationPortabilityManifestSchema::V3 => observation.module_hash.is_none(),
    });
    if wrong_observation_version {
        return Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::InvalidShape,
        ));
    }
    Ok(())
}

fn field_by_name<'a>(
    record: &'a RecordSchema,
    name: &str,
) -> Option<&'a riffdb_contract_ir::FieldSchema> {
    record.fields().iter().find(|field| field.name() == name)
}

fn checked_size(bytes: &[u8]) -> Result<(), ApplicationPortabilityError> {
    if bytes.is_empty() || bytes.len() > MAX_APPLICATION_PORTABILITY_DOCUMENT_BYTES {
        Err(ApplicationPortabilityError::new(
            ApplicationPortabilityErrorKind::LimitExceeded,
        ))
    } else {
        Ok(())
    }
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ApplicationPortabilityError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| {
        ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
    })?;
    bytes.push(b'\n');
    checked_size(&bytes)?;
    Ok(bytes)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PortabilityManifestDto {
    schema: String,
    adapter_manifest_hash: String,
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    mappings: Vec<MappingDto>,
    omissions: Vec<OmissionDto>,
    observations: Vec<ObservationDto>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MappingDto {
    class: String,
    symbol: String,
    strategy: StrategyDto,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrategyDto {
    kind: String,
    command: Option<String>,
    idempotency_input: Option<String>,
    record_input: Option<String>,
    fields: Vec<FieldBindingDto>,
    migration_hash: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FieldBindingDto {
    source_field: String,
    command_input: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OmissionDto {
    class: String,
    symbol: Option<String>,
    reason: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObservationDto {
    name: String,
    query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    module_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parameters: Vec<ObservationParameterDto>,
    expected_hash: String,
    maximum_items: u32,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObservationParameterDto {
    name: String,
    canonical_value_hex: String,
}

impl PortabilityManifestDto {
    fn from_input(
        input: &ApplicationPortabilityManifestInput,
        schema: ApplicationPortabilityManifestSchema,
    ) -> Self {
        Self {
            schema: schema.name().to_owned(),
            adapter_manifest_hash: hex(input.adapter_manifest_hash.as_bytes()),
            contract_lineage: input.contract_lineage.as_str().to_owned(),
            contract_version: input.contract_version.get(),
            contract_bundle_hash: hex(input.contract_bundle_hash.as_bytes()),
            mappings: input
                .mappings
                .iter()
                .map(MappingDto::from_mapping)
                .collect(),
            omissions: input
                .omissions
                .iter()
                .map(|omission| OmissionDto {
                    class: omission.class.tag().to_owned(),
                    symbol: omission
                        .symbol
                        .as_ref()
                        .map(|symbol| symbol.as_str().to_owned()),
                    reason: omission.reason.tag().to_owned(),
                })
                .collect(),
            observations: input
                .observations
                .iter()
                .map(|observation| ObservationDto {
                    name: observation.name.as_str().to_owned(),
                    query: observation.query.as_str().to_owned(),
                    module_hash: observation.module_hash.map(|hash| hex(hash.as_bytes())),
                    parameters: observation
                        .parameters
                        .iter()
                        .map(|parameter| ObservationParameterDto {
                            name: parameter.name.as_str().to_owned(),
                            canonical_value_hex: hex(&parameter.canonical_value),
                        })
                        .collect(),
                    expected_hash: hex(observation.expected_hash.as_bytes()),
                    maximum_items: observation.maximum_items,
                })
                .collect(),
        }
    }

    fn into_input(
        self,
        schema: ApplicationPortabilityManifestSchema,
    ) -> Result<ApplicationPortabilityManifestInput, ApplicationPortabilityError> {
        Ok(ApplicationPortabilityManifestInput {
            adapter_manifest_hash: AdapterConformanceManifestHash::from_bytes(parse_hash(
                &self.adapter_manifest_hash,
            )?),
            contract_lineage: ContractLineage::new(self.contract_lineage).map_err(|_| invalid())?,
            contract_version: ContractVersion::new(self.contract_version).ok_or_else(invalid)?,
            contract_bundle_hash: ContractBundleHash::from_bytes(parse_hash(
                &self.contract_bundle_hash,
            )?),
            mappings: self
                .mappings
                .into_iter()
                .map(|mapping| mapping.into_mapping(schema))
                .collect::<Result<Vec<_>, _>>()?,
            omissions: self
                .omissions
                .into_iter()
                .map(|omission| {
                    PortableOmission::new(
                        PortableOmissionClass::parse(&omission.class).ok_or_else(invalid)?,
                        omission
                            .symbol
                            .map(InstallationSymbol::new)
                            .transpose()
                            .map_err(|_| invalid())?,
                        PortableOmissionReason::parse(&omission.reason).ok_or_else(invalid)?,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            observations: self
                .observations
                .into_iter()
                .map(|observation| {
                    ReimportObservation::new_checked(
                        InstallationSymbol::new(observation.name).map_err(|_| invalid())?,
                        InstallationSymbol::new(observation.query).map_err(|_| invalid())?,
                        observation
                            .module_hash
                            .map(|hash| parse_hash(&hash).map(QueryModuleHash::from_bytes))
                            .transpose()?,
                        observation
                            .parameters
                            .into_iter()
                            .map(|parameter| {
                                let bytes = parse_hex_vec(&parameter.canonical_value_hex)?;
                                let value =
                                    decode_canonical_value(&bytes).map_err(|_| invalid())?;
                                ReimportObservationParameter::new(
                                    InstallationSymbol::new(parameter.name)
                                        .map_err(|_| invalid())?,
                                    value,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        GeneratedArtifactHash::from_bytes(parse_hash(&observation.expected_hash)?),
                        observation.maximum_items,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl MappingDto {
    fn from_mapping(mapping: &PortableRecordMapping) -> Self {
        let strategy = match &mapping.strategy {
            PortableReimportStrategy::ReimportCommand { command } => StrategyDto {
                kind: "reimport_command".to_owned(),
                command: Some(command.as_str().to_owned()),
                idempotency_input: None,
                record_input: None,
                fields: Vec::new(),
                migration_hash: None,
            },
            PortableReimportStrategy::LegacyApplicationCommand {
                command,
                idempotency_input,
                record_input,
                fields,
            } => StrategyDto {
                kind: "command".to_owned(),
                command: Some(command.as_str().to_owned()),
                idempotency_input: Some(idempotency_input.as_str().to_owned()),
                record_input: record_input
                    .as_ref()
                    .map(|record_input| record_input.as_str().to_owned()),
                fields: fields
                    .iter()
                    .map(|field| FieldBindingDto {
                        source_field: field.source_field.as_str().to_owned(),
                        command_input: field.command_input.as_str().to_owned(),
                    })
                    .collect(),
                migration_hash: None,
            },
            PortableReimportStrategy::Migration { migration_hash } => StrategyDto {
                kind: "migration".to_owned(),
                command: None,
                idempotency_input: None,
                record_input: None,
                fields: Vec::new(),
                migration_hash: Some(hex(migration_hash.as_bytes())),
            },
        };
        Self {
            class: mapping.class.tag().to_owned(),
            symbol: mapping.symbol.as_str().to_owned(),
            strategy,
        }
    }

    fn into_mapping(
        self,
        schema: ApplicationPortabilityManifestSchema,
    ) -> Result<PortableRecordMapping, ApplicationPortabilityError> {
        let class = PortableRecordClass::parse(&self.class).ok_or_else(invalid)?;
        let symbol = InstallationSymbol::new(self.symbol).map_err(|_| invalid())?;
        let StrategyDto {
            kind,
            command,
            idempotency_input,
            record_input,
            fields,
            migration_hash,
        } = self.strategy;
        let strategy = match kind.as_str() {
            "reimport_command"
                if matches!(
                    schema,
                    ApplicationPortabilityManifestSchema::V2
                        | ApplicationPortabilityManifestSchema::V3
                ) && migration_hash.is_none()
                    && command.is_some()
                    && idempotency_input.is_none()
                    && record_input.is_none()
                    && fields.is_empty() =>
            {
                PortableReimportStrategy::reimport_command(
                    InstallationSymbol::new(command.ok_or_else(invalid)?).map_err(|_| invalid())?,
                )
            }
            "command"
                if schema == ApplicationPortabilityManifestSchema::V1
                    && migration_hash.is_none()
                    && command.is_some()
                    && idempotency_input.is_some() =>
            {
                let command =
                    InstallationSymbol::new(command.ok_or_else(invalid)?).map_err(|_| invalid())?;
                let idempotency_input =
                    InstallationSymbol::new(idempotency_input.ok_or_else(invalid)?)
                        .map_err(|_| invalid())?;
                match record_input {
                    Some(record_input) if fields.is_empty() => {
                        PortableReimportStrategy::legacy_bounded_collection_command(
                            command,
                            idempotency_input,
                            InstallationSymbol::new(record_input).map_err(|_| invalid())?,
                        )
                    }
                    None => PortableReimportStrategy::legacy_command(
                        command,
                        idempotency_input,
                        fields
                            .into_iter()
                            .map(|field| {
                                Ok(PortableFieldBinding::new(
                                    InstallationSymbol::new(field.source_field)
                                        .map_err(|_| invalid())?,
                                    InstallationSymbol::new(field.command_input)
                                        .map_err(|_| invalid())?,
                                ))
                            })
                            .collect::<Result<Vec<_>, ApplicationPortabilityError>>()?,
                    )?,
                    Some(_) => return Err(invalid()),
                }
            }
            "migration"
                if command.is_none()
                    && idempotency_input.is_none()
                    && record_input.is_none()
                    && fields.is_empty()
                    && migration_hash.is_some() =>
            {
                PortableReimportStrategy::migration(MigrationBundleHash::from_bytes(parse_hash(
                    migration_hash.as_deref().ok_or_else(invalid)?,
                )?))
            }
            _ => return Err(invalid()),
        };
        Ok(PortableRecordMapping::new(class, symbol, strategy))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReimportReceiptDto {
    schema: String,
    portability_manifest_hash: String,
    export_manifest_hash: String,
    target_database_id: String,
    mappings: Vec<MappingResultDto>,
    observations: Vec<ObservationResultDto>,
    terminal_state: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MappingResultDto {
    class: String,
    symbol: String,
    records: u64,
    succeeded: u64,
    replayed: u64,
    outcome_hash: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObservationResultDto {
    name: String,
    actual_hash: String,
}

impl ReimportReceiptDto {
    fn from_input(
        input: &ApplicationReimportReceiptInput,
        schema: ApplicationReimportReceiptSchema,
    ) -> Self {
        Self {
            schema: schema.name().to_owned(),
            portability_manifest_hash: hex(input.portability_manifest_hash.as_bytes()),
            export_manifest_hash: hex(input.export_manifest_hash.as_bytes()),
            target_database_id: hex(input.target_database_id.as_bytes()),
            mappings: input
                .mappings
                .iter()
                .map(|result| MappingResultDto {
                    class: result.class.tag().to_owned(),
                    symbol: result.symbol.as_str().to_owned(),
                    records: result.records,
                    succeeded: result.succeeded,
                    replayed: result.replayed,
                    outcome_hash: hex(result.outcome_hash.as_bytes()),
                })
                .collect(),
            observations: input
                .observations
                .iter()
                .map(|result| ObservationResultDto {
                    name: result.name.as_str().to_owned(),
                    actual_hash: hex(result.actual_hash.as_bytes()),
                })
                .collect(),
            terminal_state: "reconciled".to_owned(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn into_parts(
        self,
        expected_manifest: ApplicationPortabilityManifestHash,
    ) -> Result<
        (
            ApplicationExportManifestHash,
            DatabaseId,
            Vec<ReimportMappingResult>,
            Vec<ReimportObservationResult>,
        ),
        ApplicationPortabilityError,
    > {
        if self.terminal_state != "reconciled"
            || ApplicationPortabilityManifestHash::from_bytes(parse_hash(
                &self.portability_manifest_hash,
            )?) != expected_manifest
        {
            return Err(ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch,
            ));
        }
        Ok((
            ApplicationExportManifestHash::from_bytes(parse_hash(&self.export_manifest_hash)?),
            DatabaseId::from_bytes(parse_uuid(&self.target_database_id)?).map_err(|_| invalid())?,
            self.mappings
                .into_iter()
                .map(|result| {
                    ReimportMappingResult::new(
                        PortableRecordClass::parse(&result.class).ok_or_else(invalid)?,
                        InstallationSymbol::new(result.symbol).map_err(|_| invalid())?,
                        result.records,
                        result.succeeded,
                        result.replayed,
                        GeneratedArtifactHash::from_bytes(parse_hash(&result.outcome_hash)?),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            self.observations
                .into_iter()
                .map(|result| {
                    Ok(ReimportObservationResult::new(
                        InstallationSymbol::new(result.name).map_err(|_| invalid())?,
                        GeneratedArtifactHash::from_bytes(parse_hash(&result.actual_hash)?),
                    ))
                })
                .collect::<Result<Vec<_>, ApplicationPortabilityError>>()?,
        ))
    }
}

fn parse_hash(value: &str) -> Result<[u8; 32], ApplicationPortabilityError> {
    parse_hex::<32>(value)
}

fn parse_uuid(value: &str) -> Result<[u8; 16], ApplicationPortabilityError> {
    parse_hex::<16>(value)
}

fn parse_hex_vec(value: &str) -> Result<Vec<u8>, ApplicationPortabilityError> {
    if value.is_empty()
        || value.len() % 2 != 0
        || value.len() > riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = nibble(pair[0]).ok_or_else(invalid)?;
            let low = nibble(pair[1]).ok_or_else(invalid)?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], ApplicationPortabilityError> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    let mut output = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] =
            (nibble(pair[0]).ok_or_else(invalid)? << 4) | nibble(pair[1]).ok_or_else(invalid)?;
    }
    Ok(output)
}

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

const fn invalid() -> ApplicationPortabilityError {
    ApplicationPortabilityError::new(ApplicationPortabilityErrorKind::InvalidEncoding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_contract_compiler::compile_contract_source;

    fn symbol(value: &str) -> InstallationSymbol {
        InstallationSymbol::new(value).expect("symbol")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes([0, 0, 0, 0, 0, 1, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 6])
            .expect("database ID")
    }

    fn manifest() -> ApplicationPortabilityManifest {
        let command = PortableReimportStrategy::reimport_command(symbol("ReconstituteTickets"));
        ApplicationPortabilityManifest::compile(ApplicationPortabilityManifestInput {
            adapter_manifest_hash: AdapterConformanceManifestHash::from_bytes([1; 32]),
            contract_lineage: ContractLineage::new("TicketDesk").expect("lineage"),
            contract_version: ContractVersion::new(2).expect("version"),
            contract_bundle_hash: ContractBundleHash::from_bytes([2; 32]),
            mappings: vec![PortableRecordMapping::new(
                PortableRecordClass::Entity,
                symbol("Ticket"),
                command,
            )],
            omissions: vec![
                PortableOmission::new(
                    PortableOmissionClass::Provenance,
                    None,
                    PortableOmissionReason::HistoricalRecordsNotRegenerable,
                )
                .expect("omission"),
            ],
            observations: vec![
                ReimportObservation::new_with_parameters(
                    symbol("ticket_count"),
                    symbol("ListTickets"),
                    QueryModuleHash::from_bytes([4; 32]),
                    Vec::new(),
                    GeneratedArtifactHash::from_bytes([3; 32]),
                    500,
                )
                .expect("observation"),
            ],
        })
        .expect("manifest")
    }

    fn scheduled_manifest(
        bundle: &ContractBundle,
        include_parent: bool,
    ) -> ApplicationPortabilityManifest {
        let mut mappings = vec![PortableRecordMapping::new(
            PortableRecordClass::Entity,
            symbol("Child"),
            PortableReimportStrategy::reimport_command(symbol("ReconstituteChildren")),
        )];
        if include_parent {
            mappings.push(PortableRecordMapping::new(
                PortableRecordClass::Entity,
                symbol("Parent"),
                PortableReimportStrategy::reimport_command(symbol("ReconstituteParents")),
            ));
        }
        ApplicationPortabilityManifest::compile(ApplicationPortabilityManifestInput {
            adapter_manifest_hash: AdapterConformanceManifestHash::from_bytes([9; 32]),
            contract_lineage: bundle.lineage().clone(),
            contract_version: bundle.contract_version(),
            contract_bundle_hash: bundle.bundle_hash(),
            mappings,
            omissions: Vec::new(),
            observations: vec![
                ReimportObservation::new_with_parameters(
                    symbol("family_count"),
                    symbol("ListFamilies"),
                    QueryModuleHash::from_bytes([9; 32]),
                    Vec::new(),
                    GeneratedArtifactHash::from_bytes([8; 32]),
                    64,
                )
                .expect("observation"),
            ],
        })
        .expect("scheduled manifest")
    }

    fn relationship_bundle() -> ContractBundle {
        compile_contract_source(
            r#"
contract PortableFamilies version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
  }
  entity Child {
    key (tenant_id: uuid, parent_id: uuid, child_id: uuid)
    field label: string<64>
    reference parent (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }
  aggregate Families {
    root Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id, parent_id)
  }
  reimport command ReconstituteParents {
    input records: list<Parent, 1..64>
    reconstitute Parent from records else ParentExists {}
    return ParentsReconstituted {}
  }
  reimport command ReconstituteChildren {
    input records: list<Child, 1..64>
    reconstitute Child from records else DependencyUnavailable {}
    return ChildrenReconstituted {}
  }
}
"#,
        )
        .expect("relationship bundle")
    }

    #[test]
    fn canonical_manifest_round_trips_and_has_no_execution_escape() {
        let manifest = manifest();
        let decoded = ApplicationPortabilityManifest::decode_canonical(manifest.canonical_bytes())
            .expect("decode");
        assert_eq!(decoded, manifest);
        let text = std::str::from_utf8(manifest.canonical_bytes()).expect("UTF-8");
        for forbidden in [
            "entity_type_id",
            "field_id",
            "storage",
            "method_path",
            "callback",
            "transaction",
            "script",
        ] {
            assert!(!text.contains(forbidden));
        }
        let mut v2_input = manifest.input().clone();
        v2_input.observations = vec![
            ReimportObservation::new(
                symbol("ticket_count"),
                symbol("ListTickets"),
                GeneratedArtifactHash::from_bytes([3; 32]),
                500,
            )
            .expect("legacy observation"),
        ];
        let v2 = ApplicationPortabilityManifest::compile_version(
            v2_input,
            ApplicationPortabilityManifestSchema::V2,
        )
        .expect("frozen v2");
        assert!(
            std::str::from_utf8(v2.canonical_bytes())
                .expect("UTF-8")
                .contains(APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V2)
        );
        assert_eq!(
            ApplicationPortabilityManifest::decode_canonical(v2.canonical_bytes())
                .expect("decode v2"),
            v2
        );
    }

    #[test]
    fn v3_observations_carry_only_bounded_exact_scalar_parameters() {
        let mut input = manifest().input().clone();
        input.observations = vec![
            ReimportObservation::new_with_parameters(
                symbol("ticket"),
                symbol("GetTicket"),
                QueryModuleHash::from_bytes([4; 32]),
                vec![
                    ReimportObservationParameter::new(
                        symbol("ticket_id"),
                        CanonicalValue::Uuid([7; 16]),
                    )
                    .expect("UUID parameter"),
                    ReimportObservationParameter::new(symbol("after"), CanonicalValue::Null)
                        .expect("null parameter"),
                ],
                GeneratedArtifactHash::from_bytes([3; 32]),
                1,
            )
            .expect("observation"),
        ];
        let manifest = ApplicationPortabilityManifest::compile(input).expect("v3 manifest");
        let decoded = ApplicationPortabilityManifest::decode_canonical(manifest.canonical_bytes())
            .expect("decode v3");
        assert_eq!(decoded, manifest);
        assert_eq!(
            decoded.input().observations[0].parameters()[1]
                .value()
                .expect("value"),
            CanonicalValue::Uuid([7; 16])
        );
        assert!(
            ReimportObservationParameter::new(
                symbol("unsafe"),
                CanonicalValue::record(Vec::new()).expect("record")
            )
            .is_err()
        );
    }

    #[test]
    fn compiler_schedule_places_required_parent_before_child() {
        let bundle = relationship_bundle();
        let manifest = scheduled_manifest(&bundle, true);
        let schedule = manifest
            .compiled_reimport_entity_schedule(&bundle)
            .expect("acyclic complete schedule");
        let names = schedule
            .iter()
            .map(|id| bundle.schema().entity(*id).expect("entity").name())
            .collect::<Vec<_>>();
        assert_eq!(names, ["Parent", "Child"]);
    }

    #[test]
    fn compiler_schedule_rejects_an_unmapped_required_parent() {
        let bundle = relationship_bundle();
        let manifest = scheduled_manifest(&bundle, false);
        let error = manifest
            .compiled_reimport_entity_schedule(&bundle)
            .expect_err("required parent cannot be omitted");
        assert_eq!(error.kind(), ApplicationPortabilityErrorKind::InvalidShape);
    }

    #[test]
    fn v3_command_mapping_has_no_caller_selected_execution_inputs() {
        let manifest = manifest();
        let text = std::str::from_utf8(manifest.canonical_bytes()).expect("UTF-8");
        assert!(text.contains(APPLICATION_PORTABILITY_MANIFEST_SCHEMA_V3));
        assert!(text.contains("\"kind\":\"reimport_command\""));
        assert!(text.contains("\"idempotency_input\":null"));
        assert!(text.contains("\"record_input\":null"));
        assert!(text.contains("\"fields\":[]"));
    }

    #[test]
    fn receipt_seals_only_exact_mapping_and_observation_reconciliation() {
        let manifest = manifest();
        let mappings = vec![
            ReimportMappingResult::new(
                PortableRecordClass::Entity,
                symbol("Ticket"),
                3,
                2,
                1,
                GeneratedArtifactHash::from_bytes([4; 32]),
            )
            .expect("mapping result"),
        ];
        let observations = vec![ReimportObservationResult::new(
            symbol("ticket_count"),
            GeneratedArtifactHash::from_bytes([3; 32]),
        )];
        let receipt = ApplicationReimportReceipt::reconcile(
            &manifest,
            ApplicationExportManifestHash::from_bytes([5; 32]),
            database_id(),
            mappings.clone(),
            observations,
        )
        .expect("receipt");
        assert_eq!(
            ApplicationReimportReceipt::decode_canonical(receipt.canonical_bytes(), &manifest)
                .expect("decode"),
            receipt
        );
        assert_eq!(
            ApplicationReimportReceipt::reconcile(
                &manifest,
                ApplicationExportManifestHash::from_bytes([5; 32]),
                database_id(),
                mappings,
                vec![ReimportObservationResult::new(
                    symbol("ticket_count"),
                    GeneratedArtifactHash::from_bytes([9; 32]),
                )],
            )
            .expect_err("mismatched observation"),
            ApplicationPortabilityError::new(
                ApplicationPortabilityErrorKind::ReconciliationMismatch
            )
        );
    }

    #[test]
    fn reimport_mapping_resolves_only_against_exact_compiler_owned_command() {
        let bundle = ContractBundle::decode(include_bytes!(
            "../../../fixtures/compiler/reimport/bundle.bin"
        ))
        .expect("contract bundle");
        let manifest =
            ApplicationPortabilityManifest::compile(ApplicationPortabilityManifestInput {
                adapter_manifest_hash: AdapterConformanceManifestHash::from_bytes([1; 32]),
                contract_lineage: bundle.lineage().clone(),
                contract_version: bundle.contract_version(),
                contract_bundle_hash: bundle.bundle_hash(),
                mappings: vec![PortableRecordMapping::new(
                    PortableRecordClass::Entity,
                    symbol("Session"),
                    PortableReimportStrategy::reimport_command(symbol("ReconstituteSessions")),
                )],
                omissions: vec![],
                observations: vec![
                    ReimportObservation::new_with_parameters(
                        symbol("list_tuples"),
                        symbol("ListFgaTuples"),
                        QueryModuleHash::from_bytes([4; 32]),
                        Vec::new(),
                        GeneratedArtifactHash::from_bytes([3; 32]),
                        500,
                    )
                    .expect("observation"),
                ],
            })
            .expect("manifest");
        manifest
            .validate_compiled_contract(&bundle)
            .expect("compiled mapping");

        let mut wrong = manifest.input().clone();
        wrong.mappings[0] = PortableRecordMapping::new(
            PortableRecordClass::Entity,
            symbol("Session"),
            PortableReimportStrategy::reimport_command(symbol("MissingCommand")),
        );
        ApplicationPortabilityManifest::compile(wrong)
            .expect("symbolically valid")
            .validate_compiled_contract(&bundle)
            .expect_err("wrong compiled collection input");
    }

    #[test]
    fn frozen_v1_manifest_remains_readable_but_cannot_be_compiled_as_v2() {
        let legacy = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v1.json"
        ))
        .expect("frozen v1 manifest");
        assert_eq!(legacy.schema, ApplicationPortabilityManifestSchema::V1);
        assert!(matches!(
            legacy.input().mappings[0].strategy(),
            PortableReimportStrategy::LegacyApplicationCommand { .. }
        ));
        assert_eq!(
            ApplicationPortabilityManifest::compile(legacy.input().clone())
                .expect_err("legacy application command must not enter v2")
                .kind(),
            ApplicationPortabilityErrorKind::InvalidShape
        );
    }
}
