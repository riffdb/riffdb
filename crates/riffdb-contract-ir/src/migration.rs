//! Canonical, span-free migration artifacts.

use riffdb_types::{
    AggregateTypeId, ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId,
    EnumTypeId, EnumVariantId, FieldId, IndexId, InvariantId, MigrationBundleHash,
    MigrationSourceHash, ProjectionId, hash_migration_bundle,
};

use crate::bundle::{decode_expression_arena, encode_expression_arena};
use crate::codec::{Reader, Writer};
use crate::format_registry::{migration_conversion as conversion_tag, migration_step as step_tag};
use crate::{
    ExprId, ExpressionArena, ExpressionKind, IrValidationError, StableIdNamespaceTag, checked_len,
    validate_source_name,
};

/// Canonical migration-bundle format version.
pub const MIGRATION_BUNDLE_FORMAT_VERSION_V1: u32 = 1;
/// Migration source grammar version represented by V1 bundles.
pub const MIGRATION_GRAMMAR_VERSION_V1: u32 = 1;
/// Executable migration IR version.
pub const MIGRATION_IR_VERSION_V1: u32 = 1;
/// Maximum number of typed steps in one bundle.
pub const MAX_MIGRATION_STEPS_V1: usize = 4_096;
/// Maximum canonical bytes in one migration bundle.
pub const MAX_MIGRATION_BUNDLE_BYTES_V1: usize = 16 * 1_024 * 1_024;
/// Maximum exact migration-source bytes.
pub const MAX_MIGRATION_SOURCE_BYTES_V1: usize = 1_048_576;

const MIGRATION_MAGIC: &[u8] = b"RIFFDB-MIGRATION\0";

/// A positive, bundle-local migration step identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MigrationStepId(std::num::NonZeroU32);

impl MigrationStepId {
    /// Creates a positive step identifier.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match std::num::NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Numeric one-based position.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Fixed hard ceilings recorded in every V1 migration artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationResourceBoundsV1 {
    maximum_source_bytes: u32,
    maximum_steps: u32,
    maximum_expression_nodes: u32,
    maximum_bundle_bytes: u32,
}

impl MigrationResourceBoundsV1 {
    /// Returns the only executable V1 bounds.
    #[must_use]
    pub const fn fixed() -> Self {
        Self {
            maximum_source_bytes: MAX_MIGRATION_SOURCE_BYTES_V1 as u32,
            maximum_steps: MAX_MIGRATION_STEPS_V1 as u32,
            maximum_expression_nodes: crate::MAX_EXPRESSION_NODES as u32,
            maximum_bundle_bytes: MAX_MIGRATION_BUNDLE_BYTES_V1 as u32,
        }
    }

    /// Maximum source bytes.
    #[must_use]
    pub const fn maximum_source_bytes(self) -> u32 {
        self.maximum_source_bytes
    }

    /// Maximum step count.
    #[must_use]
    pub const fn maximum_steps(self) -> u32 {
        self.maximum_steps
    }

    /// Maximum expression nodes across the artifact.
    #[must_use]
    pub const fn maximum_expression_nodes(self) -> u32 {
        self.maximum_expression_nodes
    }

    /// Maximum canonical bundle bytes.
    #[must_use]
    pub const fn maximum_bundle_bytes(self) -> u32 {
        self.maximum_bundle_bytes
    }
}

/// One checked row-local expression and its result node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationExpressionV1 {
    arena: ExpressionArena,
    result: ExprId,
}

impl MigrationExpressionV1 {
    /// Creates a migration expression restricted to constants, old-row fields,
    /// unary operations, and binary operations.
    pub fn new(arena: ExpressionArena, result: ExprId) -> Result<Self, IrValidationError> {
        arena.validate_reachable_from(&[result], "migration expression result")?;
        if arena.nodes().iter().any(|node| {
            !matches!(
                node.kind(),
                ExpressionKind::Constant(_)
                    | ExpressionKind::SchemaField { .. }
                    | ExpressionKind::Unary { .. }
                    | ExpressionKind::Binary { .. }
            )
        }) {
            return Err(IrValidationError::InvalidDependency {
                reason: "migration expressions may inspect only constants and the old row",
            });
        }
        Ok(Self { arena, result })
    }

    /// Checked expression arena.
    #[must_use]
    pub const fn arena(&self) -> &ExpressionArena {
        &self.arena
    }

    /// Result expression.
    #[must_use]
    pub const fn result(&self) -> ExprId {
        self.result
    }
}

/// Closed checked field-conversion registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum MigrationConversionV1 {
    /// Exact identity conversion.
    Identity = conversion_tag::IDENTITY,
    /// Wraps a required value in an optional value.
    WrapOptional = conversion_tag::WRAP_OPTIONAL,
    /// Asserts presence before optional unwrapping.
    AssertUnwrapOptional = conversion_tag::ASSERT_UNWRAP_OPTIONAL,
    /// Checked signed-to-unsigned integer conversion.
    CheckedI64ToU64 = conversion_tag::CHECKED_I64_TO_U64,
    /// Checked unsigned-to-signed integer conversion.
    CheckedU64ToI64 = conversion_tag::CHECKED_U64_TO_I64,
    /// Exact fixed-decimal precision/scale conversion.
    ExactDecimal = conversion_tag::EXACT_DECIMAL,
    /// Asserted string, bytes, or list narrowing.
    AssertBoundedNarrow = conversion_tag::ASSERT_BOUNDED_NARROW,
    /// Bounded element-wise list conversion.
    ListElements = conversion_tag::LIST_ELEMENTS,
    /// Canonical UUID-to-string conversion.
    UuidToString = conversion_tag::UUID_TO_STRING,
    /// Checked lowercase-hyphenated string-to-UUID conversion.
    StringToUuid = conversion_tag::STRING_TO_UUID,
}

impl MigrationConversionV1 {
    /// Whether this closed conversion accepts the exact predecessor and successor types.
    #[must_use]
    pub fn accepts(self, old: &crate::ValueType, new: &crate::ValueType) -> bool {
        use crate::ValueTypeTag as Tag;
        match self {
            Self::Identity => old == new,
            Self::WrapOptional => new.optional_inner() == Some(old),
            Self::AssertUnwrapOptional => old.optional_inner() == Some(new),
            Self::CheckedI64ToU64 => old.tag() == Tag::I64 && new.tag() == Tag::U64,
            Self::CheckedU64ToI64 => old.tag() == Tag::U64 && new.tag() == Tag::I64,
            Self::ExactDecimal => old.decimal_spec().is_some() && new.decimal_spec().is_some(),
            Self::AssertBoundedNarrow => match (old.tag(), new.tag()) {
                (Tag::String, Tag::String) | (Tag::Bytes, Tag::Bytes) => old
                    .byte_bound()
                    .zip(new.byte_bound())
                    .is_some_and(|(old, new)| new <= old),
                (Tag::List, Tag::List) => old.list_parts().zip(new.list_parts()).is_some_and(
                    |((old_element, old_max), (new_element, new_max))| {
                        old_element == new_element && new_max <= old_max
                    },
                ),
                _ => false,
            },
            Self::ListElements => old.list_parts().zip(new.list_parts()).is_some_and(
                |((old_element, old_max), (new_element, new_max))| {
                    old_max <= new_max && inferred_element_conversion(old_element, new_element)
                },
            ),
            Self::UuidToString => {
                old.tag() == Tag::Uuid
                    && new.tag() == Tag::String
                    && new.byte_bound().is_some_and(|bound| bound >= 36)
            }
            Self::StringToUuid => old.tag() == Tag::String && new.tag() == Tag::Uuid,
        }
    }
}

fn inferred_element_conversion(old: &crate::ValueType, new: &crate::ValueType) -> bool {
    old == new
        || MigrationConversionV1::WrapOptional.accepts(old, new)
        || MigrationConversionV1::AssertUnwrapOptional.accepts(old, new)
        || MigrationConversionV1::CheckedI64ToU64.accepts(old, new)
        || MigrationConversionV1::CheckedU64ToI64.accepts(old, new)
        || MigrationConversionV1::ExactDecimal.accepts(old, new)
        || MigrationConversionV1::UuidToString.accepts(old, new)
        || MigrationConversionV1::StringToUuid.accepts(old, new)
}

/// One enum-variant mapping in stable predecessor-ID order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationEnumMappingV1 {
    from: EnumVariantId,
    to: EnumVariantId,
}

impl MigrationEnumMappingV1 {
    /// Creates one mapping.
    #[must_use]
    pub const fn new(from: EnumVariantId, to: EnumVariantId) -> Self {
        Self { from, to }
    }

    /// Predecessor variant.
    #[must_use]
    pub const fn from(self) -> EnumVariantId {
        self.from
    }

    /// Successor variant.
    #[must_use]
    pub const fn to(self) -> EnumVariantId {
        self.to
    }
}

/// Frozen V1 typed migration step payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationStepKindV1 {
    /// Semantic-preserving stable-identity rename.
    RenameIdentity {
        /// Stable namespace.
        namespace: StableIdNamespaceTag,
        /// Closed identity owner-kind tag.
        owner_kind: u8,
        /// Exact stable owner path.
        owner_ids: Vec<u32>,
        /// Stable numeric ID.
        stable_id: u32,
        /// Successor display name.
        new_name: String,
    },
    /// Logical retirement of a stable identity.
    RetireIdentity {
        /// Stable namespace.
        namespace: StableIdNamespaceTag,
        /// Closed identity owner-kind tag.
        owner_kind: u8,
        /// Exact stable owner path.
        owner_ids: Vec<u32>,
        /// Stable numeric ID.
        stable_id: u32,
    },
    /// Sets one successor field from a checked row-local expression.
    SetField {
        /// Entity being transformed.
        entity: EntityTypeId,
        /// Successor field.
        field: FieldId,
        /// Checked expression.
        expression: MigrationExpressionV1,
    },
    /// Replaces one field through a closed conversion.
    ReplaceField {
        /// Entity being transformed.
        entity: EntityTypeId,
        /// Predecessor field.
        old_field: FieldId,
        /// Successor field.
        new_field: FieldId,
        /// Checked conversion.
        conversion: MigrationConversionV1,
    },
    /// Validates one entity-local assertion over every row.
    RequireEntity {
        /// Entity being validated.
        entity: EntityTypeId,
        /// Checked Boolean expression.
        predicate: MigrationExpressionV1,
    },
    /// Replaces an entity primary key through checked expressions.
    RekeyEntity {
        /// Entity being rekeyed.
        entity: EntityTypeId,
        /// Complete successor key components.
        components: Vec<MigrationExpressionV1>,
    },
    /// Exhaustively remaps one enumeration.
    MapEnum {
        /// Enumeration being mapped.
        enumeration: EnumTypeId,
        /// Complete predecessor mapping.
        mappings: Vec<MigrationEnumMappingV1>,
    },
    /// Builds one successor index over existing state.
    RebuildIndex {
        /// Owning entity.
        entity: EntityTypeId,
        /// Successor index.
        index: IndexId,
    },
    /// Validates one named relationship over existing state.
    ValidateRelationship {
        /// Source entity.
        source_entity: EntityTypeId,
        /// Relationship name.
        name: String,
    },
    /// Validates one unique index over existing state.
    ValidateUnique {
        /// Owning entity.
        entity: EntityTypeId,
        /// Backing index.
        index: IndexId,
    },
    /// Validates one entity- or aggregate-owned invariant.
    ValidateInvariant {
        /// Owning entity or aggregate namespace.
        owner_namespace: StableIdNamespaceTag,
        /// Owner stable ID.
        owner_id: u32,
        /// Invariant stable ID.
        invariant: InvariantId,
    },
    /// Rebuilds one projection generation through the frozen frontier.
    RebuildProjection {
        /// Successor projection.
        projection: ProjectionId,
    },
    /// Acknowledges compiler-derived repartition work.
    AcknowledgeRepartition {
        /// Affected aggregate.
        aggregate: AggregateTypeId,
    },
    /// Acknowledges compiler-derived aggregate-membership work.
    AcknowledgeAggregate {
        /// Affected aggregate.
        aggregate: AggregateTypeId,
    },
    /// Acknowledges compiler-derived conflict-domain work.
    AcknowledgeConflict {
        /// Affected aggregate.
        aggregate: AggregateTypeId,
    },
}

impl MigrationStepKindV1 {
    /// Encoded tag for projection rebuild, exposed for compatibility fixtures.
    pub const REBUILD_PROJECTION_TAG: u8 = step_tag::REBUILD_PROJECTION;

    const fn tag(&self) -> u8 {
        match self {
            Self::RenameIdentity { .. } => step_tag::RENAME_IDENTITY,
            Self::RetireIdentity { .. } => step_tag::RETIRE_IDENTITY,
            Self::SetField { .. } => step_tag::SET_FIELD,
            Self::ReplaceField { .. } => step_tag::REPLACE_FIELD,
            Self::RequireEntity { .. } => step_tag::REQUIRE_ENTITY,
            Self::RekeyEntity { .. } => step_tag::REKEY_ENTITY,
            Self::MapEnum { .. } => step_tag::MAP_ENUM,
            Self::RebuildIndex { .. } => step_tag::REBUILD_INDEX,
            Self::ValidateRelationship { .. } => step_tag::VALIDATE_RELATIONSHIP,
            Self::ValidateUnique { .. } => step_tag::VALIDATE_UNIQUE,
            Self::ValidateInvariant { .. } => step_tag::VALIDATE_INVARIANT,
            Self::RebuildProjection { .. } => step_tag::REBUILD_PROJECTION,
            Self::AcknowledgeRepartition { .. } => step_tag::ACKNOWLEDGE_REPARTITION,
            Self::AcknowledgeAggregate { .. } => step_tag::ACKNOWLEDGE_AGGREGATE,
            Self::AcknowledgeConflict { .. } => step_tag::ACKNOWLEDGE_CONFLICT,
        }
    }
}

/// One canonically ordered migration DAG step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationStepV1 {
    id: MigrationStepId,
    dependencies: Vec<MigrationStepId>,
    kind: MigrationStepKindV1,
}

impl MigrationStepV1 {
    /// Creates a step whose dependencies are sorted, unique, and earlier.
    pub fn new(
        id: MigrationStepId,
        dependencies: Vec<MigrationStepId>,
        kind: MigrationStepKindV1,
    ) -> Result<Self, IrValidationError> {
        if dependencies.windows(2).any(|pair| pair[0] >= pair[1])
            || dependencies.iter().any(|dependency| *dependency >= id)
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "migration step dependencies",
            });
        }
        checked_len(
            "migration step dependencies",
            dependencies.len(),
            MAX_MIGRATION_STEPS_V1,
        )?;
        validate_step_kind(&kind)?;
        Ok(Self {
            id,
            dependencies,
            kind,
        })
    }

    /// Step ID.
    #[must_use]
    pub const fn id(&self) -> MigrationStepId {
        self.id
    }

    /// Earlier dependency IDs.
    #[must_use]
    pub fn dependencies(&self) -> &[MigrationStepId] {
        &self.dependencies
    }

    /// Typed step payload.
    #[must_use]
    pub const fn kind(&self) -> &MigrationStepKindV1 {
        &self.kind
    }
}

/// One exact, canonical parent-specific migration artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationBundleV1 {
    compiler_version: String,
    lineage: ContractLineage,
    parent_version: ContractVersion,
    parent_bundle_hash: ContractBundleHash,
    candidate_version: ContractVersion,
    candidate_bundle_hash: ContractBundleHash,
    source_hash: MigrationSourceHash,
    steps: Vec<MigrationStepV1>,
    resource_bounds: MigrationResourceBoundsV1,
    canonical_bytes: Vec<u8>,
    bundle_hash: MigrationBundleHash,
}

impl MigrationBundleV1 {
    /// Validates and canonically encodes one exact migration artifact.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compiler_version: impl Into<String>,
        lineage: ContractLineage,
        parent_version: ContractVersion,
        parent_bundle_hash: ContractBundleHash,
        candidate_version: ContractVersion,
        candidate_bundle_hash: ContractBundleHash,
        source_hash: MigrationSourceHash,
        steps: Vec<MigrationStepV1>,
        resource_bounds: MigrationResourceBoundsV1,
    ) -> Result<Self, IrValidationError> {
        let compiler_version = compiler_version.into();
        if compiler_version.is_empty()
            || compiler_version.len() > 64
            || !compiler_version.is_ascii()
            || parent_version >= candidate_version
            || resource_bounds != MigrationResourceBoundsV1::fixed()
        {
            return Err(IrValidationError::InvalidReference {
                kind: "migration bundle header",
            });
        }
        if steps.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "migration steps",
            });
        }
        checked_len("migration steps", steps.len(), MAX_MIGRATION_STEPS_V1)?;
        for (index, step) in steps.iter().enumerate() {
            let expected =
                u32::try_from(index + 1).map_err(|_| IrValidationError::SizeOverflow {
                    kind: "migration step ID",
                })?;
            if step.id.get() != expected {
                return Err(IrValidationError::NonCanonicalOrder {
                    kind: "migration steps",
                });
            }
        }
        let mut bundle = Self {
            compiler_version,
            lineage,
            parent_version,
            parent_bundle_hash,
            candidate_version,
            candidate_bundle_hash,
            source_hash,
            steps,
            resource_bounds,
            canonical_bytes: Vec::new(),
            bundle_hash: MigrationBundleHash::from_bytes([0; 32]),
        };
        bundle.canonical_bytes = encode_bundle(&bundle)?;
        bundle.bundle_hash = hash_migration_bundle(&bundle.canonical_bytes);
        Ok(bundle)
    }

    /// Strictly decodes and revalidates one canonical V1 bundle.
    pub fn decode(bytes: &[u8]) -> Result<Self, IrValidationError> {
        if bytes.len() > MAX_MIGRATION_BUNDLE_BYTES_V1 {
            return Err(IrValidationError::LimitExceeded {
                kind: "migration bundle",
                actual: bytes.len(),
                maximum: MAX_MIGRATION_BUNDLE_BYTES_V1,
            });
        }
        let mut reader = Reader::new(bytes);
        if reader.read(MIGRATION_MAGIC.len())? != MIGRATION_MAGIC {
            return Err(IrValidationError::InvalidText {
                kind: "migration bundle magic",
            });
        }
        require_version(
            reader.u32()?,
            MIGRATION_BUNDLE_FORMAT_VERSION_V1,
            "migration bundle",
        )?;
        require_version(
            reader.u32()?,
            MIGRATION_GRAMMAR_VERSION_V1,
            "migration grammar",
        )?;
        require_version(reader.u32()?, MIGRATION_IR_VERSION_V1, "migration IR")?;
        let compiler_version = reader.string(64)?;
        let lineage = ContractLineage::new(reader.string(256)?).map_err(|_| {
            IrValidationError::InvalidText {
                kind: "migration lineage",
            }
        })?;
        let parent_version = decode_contract_version(&mut reader)?;
        let parent_bundle_hash = ContractBundleHash::from_bytes(reader.array()?);
        let candidate_version = decode_contract_version(&mut reader)?;
        let candidate_bundle_hash = ContractBundleHash::from_bytes(reader.array()?);
        let source_hash = MigrationSourceHash::from_bytes(reader.array()?);
        let resource_bounds = MigrationResourceBoundsV1 {
            maximum_source_bytes: reader.u32()?,
            maximum_steps: reader.u32()?,
            maximum_expression_nodes: reader.u32()?,
            maximum_bundle_bytes: reader.u32()?,
        };
        let count = reader.u32()? as usize;
        checked_len("migration steps", count, MAX_MIGRATION_STEPS_V1)?;
        let mut steps = Vec::with_capacity(count);
        for _ in 0..count {
            steps.push(decode_step(&mut reader)?);
        }
        reader.finish()?;
        let bundle = Self::new(
            compiler_version,
            lineage,
            parent_version,
            parent_bundle_hash,
            candidate_version,
            candidate_bundle_hash,
            source_hash,
            steps,
            resource_bounds,
        )?;
        if bundle.canonical_bytes != bytes {
            return Err(IrValidationError::HashMismatch {
                kind: "migration canonical bytes",
            });
        }
        Ok(bundle)
    }

    /// Compiler semantic version.
    #[must_use]
    pub fn compiler_version(&self) -> &str {
        &self.compiler_version
    }

    /// Contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact parent contract version.
    #[must_use]
    pub const fn parent_version(&self) -> ContractVersion {
        self.parent_version
    }

    /// Exact parent bundle hash.
    #[must_use]
    pub const fn parent_bundle_hash(&self) -> ContractBundleHash {
        self.parent_bundle_hash
    }

    /// Exact candidate contract version.
    #[must_use]
    pub const fn candidate_version(&self) -> ContractVersion {
        self.candidate_version
    }

    /// Exact candidate bundle hash.
    #[must_use]
    pub const fn candidate_bundle_hash(&self) -> ContractBundleHash {
        self.candidate_bundle_hash
    }

    /// Exact source identity.
    #[must_use]
    pub const fn source_hash(&self) -> MigrationSourceHash {
        self.source_hash
    }

    /// Canonically ordered typed step DAG.
    #[must_use]
    pub fn steps(&self) -> &[MigrationStepV1] {
        &self.steps
    }

    /// Frozen execution bounds.
    #[must_use]
    pub const fn resource_bounds(&self) -> MigrationResourceBoundsV1 {
        self.resource_bounds
    }

    /// Canonical bundle bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated bundle identity.
    #[must_use]
    pub const fn bundle_hash(&self) -> MigrationBundleHash {
        self.bundle_hash
    }
}

fn validate_step_kind(kind: &MigrationStepKindV1) -> Result<(), IrValidationError> {
    match kind {
        MigrationStepKindV1::RenameIdentity {
            namespace,
            owner_kind,
            owner_ids,
            stable_id,
            new_name,
        } => {
            if *stable_id == 0
                || crate::StableIdNamespace::new(*namespace, *owner_kind, owner_ids.clone())
                    .is_err()
            {
                return Err(IrValidationError::InvalidReference {
                    kind: "migration stable identity",
                });
            }
            validate_source_name(new_name, "migration rename")?;
        }
        MigrationStepKindV1::RetireIdentity {
            namespace,
            owner_kind,
            owner_ids,
            stable_id,
        } if *stable_id == 0
            || crate::StableIdNamespace::new(*namespace, *owner_kind, owner_ids.clone())
                .is_err() =>
        {
            return Err(IrValidationError::InvalidReference {
                kind: "migration stable identity",
            });
        }
        MigrationStepKindV1::RekeyEntity { components, .. } if components.is_empty() => {
            return Err(IrValidationError::Empty {
                kind: "migration rekey components",
            });
        }
        MigrationStepKindV1::MapEnum { mappings, .. } => {
            if mappings.is_empty() || mappings.windows(2).any(|pair| pair[0].from >= pair[1].from) {
                return Err(IrValidationError::NonCanonicalOrder {
                    kind: "migration enum mappings",
                });
            }
        }
        MigrationStepKindV1::ValidateRelationship { name, .. } => {
            validate_source_name(name, "migration relationship")?;
        }
        MigrationStepKindV1::ValidateInvariant {
            owner_namespace, ..
        } if !matches!(
            owner_namespace,
            StableIdNamespaceTag::Entity | StableIdNamespaceTag::Aggregate
        ) =>
        {
            return Err(IrValidationError::InvalidReference {
                kind: "migration invariant owner",
            });
        }
        _ => {}
    }
    Ok(())
}

fn encode_bundle(bundle: &MigrationBundleV1) -> Result<Vec<u8>, IrValidationError> {
    let mut writer = Writer::new(MAX_MIGRATION_BUNDLE_BYTES_V1);
    writer.raw(MIGRATION_MAGIC)?;
    writer.u32(MIGRATION_BUNDLE_FORMAT_VERSION_V1)?;
    writer.u32(MIGRATION_GRAMMAR_VERSION_V1)?;
    writer.u32(MIGRATION_IR_VERSION_V1)?;
    writer.string(&bundle.compiler_version)?;
    writer.string(bundle.lineage.as_str())?;
    writer.u64(bundle.parent_version.get())?;
    writer.raw(bundle.parent_bundle_hash.as_bytes())?;
    writer.u64(bundle.candidate_version.get())?;
    writer.raw(bundle.candidate_bundle_hash.as_bytes())?;
    writer.raw(bundle.source_hash.as_bytes())?;
    writer.u32(bundle.resource_bounds.maximum_source_bytes)?;
    writer.u32(bundle.resource_bounds.maximum_steps)?;
    writer.u32(bundle.resource_bounds.maximum_expression_nodes)?;
    writer.u32(bundle.resource_bounds.maximum_bundle_bytes)?;
    writer.u32(bundle.steps.len() as u32)?;
    for step in &bundle.steps {
        encode_step(&mut writer, step)?;
    }
    Ok(writer.finish())
}

fn encode_step(writer: &mut Writer, step: &MigrationStepV1) -> Result<(), IrValidationError> {
    writer.u32(step.id.get())?;
    writer.u32(step.dependencies.len() as u32)?;
    for dependency in &step.dependencies {
        writer.u32(dependency.get())?;
    }
    writer.u8(step.kind.tag())?;
    match &step.kind {
        MigrationStepKindV1::RenameIdentity {
            namespace,
            owner_kind,
            owner_ids,
            stable_id,
            new_name,
        } => {
            writer.u8(*namespace as u8)?;
            writer.u8(*owner_kind)?;
            writer.u8(owner_ids.len() as u8)?;
            for owner in owner_ids {
                writer.u32(*owner)?;
            }
            writer.u32(*stable_id)?;
            writer.string(new_name)?;
        }
        MigrationStepKindV1::RetireIdentity {
            namespace,
            owner_kind,
            owner_ids,
            stable_id,
        } => {
            writer.u8(*namespace as u8)?;
            writer.u8(*owner_kind)?;
            writer.u8(owner_ids.len() as u8)?;
            for owner in owner_ids {
                writer.u32(*owner)?;
            }
            writer.u32(*stable_id)?;
        }
        MigrationStepKindV1::SetField {
            entity,
            field,
            expression,
        } => {
            writer.u32(entity.get())?;
            writer.u32(field.get())?;
            encode_expression(writer, expression)?;
        }
        MigrationStepKindV1::ReplaceField {
            entity,
            old_field,
            new_field,
            conversion,
        } => {
            writer.u32(entity.get())?;
            writer.u32(old_field.get())?;
            writer.u32(new_field.get())?;
            writer.u8(*conversion as u8)?;
        }
        MigrationStepKindV1::RequireEntity { entity, predicate } => {
            writer.u32(entity.get())?;
            encode_expression(writer, predicate)?;
        }
        MigrationStepKindV1::RekeyEntity { entity, components } => {
            writer.u32(entity.get())?;
            writer.u32(components.len() as u32)?;
            for expression in components {
                encode_expression(writer, expression)?;
            }
        }
        MigrationStepKindV1::MapEnum {
            enumeration,
            mappings,
        } => {
            writer.u32(enumeration.get())?;
            writer.u32(mappings.len() as u32)?;
            for mapping in mappings {
                writer.u32(mapping.from.get())?;
                writer.u32(mapping.to.get())?;
            }
        }
        MigrationStepKindV1::RebuildIndex { entity, index }
        | MigrationStepKindV1::ValidateUnique { entity, index } => {
            writer.u32(entity.get())?;
            writer.u32(index.get())?;
        }
        MigrationStepKindV1::ValidateRelationship {
            source_entity,
            name,
        } => {
            writer.u32(source_entity.get())?;
            writer.string(name)?;
        }
        MigrationStepKindV1::ValidateInvariant {
            owner_namespace,
            owner_id,
            invariant,
        } => {
            writer.u8(*owner_namespace as u8)?;
            writer.u32(*owner_id)?;
            writer.u32(invariant.get())?;
        }
        MigrationStepKindV1::RebuildProjection { projection } => {
            writer.u32(projection.get())?;
        }
        MigrationStepKindV1::AcknowledgeRepartition { aggregate }
        | MigrationStepKindV1::AcknowledgeAggregate { aggregate }
        | MigrationStepKindV1::AcknowledgeConflict { aggregate } => {
            writer.u32(aggregate.get())?;
        }
    }
    Ok(())
}

fn encode_expression(
    writer: &mut Writer,
    expression: &MigrationExpressionV1,
) -> Result<(), IrValidationError> {
    encode_expression_arena(writer, &expression.arena)?;
    writer.u32(expression.result.get())
}

fn decode_step(reader: &mut Reader<'_>) -> Result<MigrationStepV1, IrValidationError> {
    let id = MigrationStepId::new(reader.u32()?).ok_or(IrValidationError::InvalidReference {
        kind: "migration step ID",
    })?;
    let dependency_count = reader.u32()? as usize;
    checked_len(
        "migration step dependencies",
        dependency_count,
        MAX_MIGRATION_STEPS_V1,
    )?;
    let mut dependencies = Vec::with_capacity(dependency_count);
    for _ in 0..dependency_count {
        dependencies.push(MigrationStepId::new(reader.u32()?).ok_or(
            IrValidationError::InvalidReference {
                kind: "migration step dependency",
            },
        )?);
    }
    let kind = match reader.u8()? {
        step_tag::RENAME_IDENTITY => MigrationStepKindV1::RenameIdentity {
            namespace: decode_namespace(reader.u8()?)?,
            owner_kind: reader.u8()?,
            owner_ids: decode_owner_ids(reader)?,
            stable_id: reader.u32()?,
            new_name: reader.string(256)?,
        },
        step_tag::RETIRE_IDENTITY => MigrationStepKindV1::RetireIdentity {
            namespace: decode_namespace(reader.u8()?)?,
            owner_kind: reader.u8()?,
            owner_ids: decode_owner_ids(reader)?,
            stable_id: reader.u32()?,
        },
        step_tag::SET_FIELD => MigrationStepKindV1::SetField {
            entity: entity_id(reader.u32()?)?,
            field: field_id(reader.u32()?)?,
            expression: decode_expression(reader)?,
        },
        step_tag::REPLACE_FIELD => MigrationStepKindV1::ReplaceField {
            entity: entity_id(reader.u32()?)?,
            old_field: field_id(reader.u32()?)?,
            new_field: field_id(reader.u32()?)?,
            conversion: decode_conversion(reader.u8()?)?,
        },
        step_tag::REQUIRE_ENTITY => MigrationStepKindV1::RequireEntity {
            entity: entity_id(reader.u32()?)?,
            predicate: decode_expression(reader)?,
        },
        step_tag::REKEY_ENTITY => {
            let entity = entity_id(reader.u32()?)?;
            let count = reader.u32()? as usize;
            checked_len("migration rekey components", count, MAX_MIGRATION_STEPS_V1)?;
            let mut components = Vec::with_capacity(count);
            for _ in 0..count {
                components.push(decode_expression(reader)?);
            }
            MigrationStepKindV1::RekeyEntity { entity, components }
        }
        step_tag::MAP_ENUM => {
            let enumeration = enum_id(reader.u32()?)?;
            let count = reader.u32()? as usize;
            checked_len("migration enum mappings", count, MAX_MIGRATION_STEPS_V1)?;
            let mut mappings = Vec::with_capacity(count);
            for _ in 0..count {
                mappings.push(MigrationEnumMappingV1::new(
                    variant_id(reader.u32()?)?,
                    variant_id(reader.u32()?)?,
                ));
            }
            MigrationStepKindV1::MapEnum {
                enumeration,
                mappings,
            }
        }
        step_tag::REBUILD_INDEX => MigrationStepKindV1::RebuildIndex {
            entity: entity_id(reader.u32()?)?,
            index: index_id(reader.u32()?)?,
        },
        step_tag::VALIDATE_RELATIONSHIP => MigrationStepKindV1::ValidateRelationship {
            source_entity: entity_id(reader.u32()?)?,
            name: reader.string(256)?,
        },
        step_tag::VALIDATE_UNIQUE => MigrationStepKindV1::ValidateUnique {
            entity: entity_id(reader.u32()?)?,
            index: index_id(reader.u32()?)?,
        },
        step_tag::VALIDATE_INVARIANT => MigrationStepKindV1::ValidateInvariant {
            owner_namespace: decode_namespace(reader.u8()?)?,
            owner_id: reader.u32()?,
            invariant: invariant_id(reader.u32()?)?,
        },
        step_tag::REBUILD_PROJECTION => MigrationStepKindV1::RebuildProjection {
            projection: projection_id(reader.u32()?)?,
        },
        step_tag::ACKNOWLEDGE_REPARTITION => MigrationStepKindV1::AcknowledgeRepartition {
            aggregate: aggregate_id(reader.u32()?)?,
        },
        step_tag::ACKNOWLEDGE_AGGREGATE => MigrationStepKindV1::AcknowledgeAggregate {
            aggregate: aggregate_id(reader.u32()?)?,
        },
        step_tag::ACKNOWLEDGE_CONFLICT => MigrationStepKindV1::AcknowledgeConflict {
            aggregate: aggregate_id(reader.u32()?)?,
        },
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "migration step",
                tag,
            });
        }
    };
    MigrationStepV1::new(id, dependencies, kind)
}

fn decode_expression(reader: &mut Reader<'_>) -> Result<MigrationExpressionV1, IrValidationError> {
    let arena = decode_expression_arena(reader)?;
    MigrationExpressionV1::new(arena, ExprId::new(reader.u32()?))
}

fn decode_owner_ids(reader: &mut Reader<'_>) -> Result<Vec<u32>, IrValidationError> {
    let count = reader.u8()? as usize;
    if count > 2 {
        return Err(IrValidationError::LimitExceeded {
            kind: "migration identity owner path",
            maximum: 2,
            actual: count,
        });
    }
    (0..count).map(|_| reader.u32()).collect()
}

const fn decode_conversion(tag: u8) -> Result<MigrationConversionV1, IrValidationError> {
    match tag {
        conversion_tag::IDENTITY => Ok(MigrationConversionV1::Identity),
        conversion_tag::WRAP_OPTIONAL => Ok(MigrationConversionV1::WrapOptional),
        conversion_tag::ASSERT_UNWRAP_OPTIONAL => Ok(MigrationConversionV1::AssertUnwrapOptional),
        conversion_tag::CHECKED_I64_TO_U64 => Ok(MigrationConversionV1::CheckedI64ToU64),
        conversion_tag::CHECKED_U64_TO_I64 => Ok(MigrationConversionV1::CheckedU64ToI64),
        conversion_tag::EXACT_DECIMAL => Ok(MigrationConversionV1::ExactDecimal),
        conversion_tag::ASSERT_BOUNDED_NARROW => Ok(MigrationConversionV1::AssertBoundedNarrow),
        conversion_tag::LIST_ELEMENTS => Ok(MigrationConversionV1::ListElements),
        conversion_tag::UUID_TO_STRING => Ok(MigrationConversionV1::UuidToString),
        conversion_tag::STRING_TO_UUID => Ok(MigrationConversionV1::StringToUuid),
        tag => Err(IrValidationError::UnknownTag {
            kind: "migration conversion",
            tag,
        }),
    }
}

const fn decode_namespace(tag: u8) -> Result<StableIdNamespaceTag, IrValidationError> {
    use crate::format_registry::stable_id_namespace as namespace;
    match tag {
        namespace::ENTITY => Ok(StableIdNamespaceTag::Entity),
        namespace::EVENT => Ok(StableIdNamespaceTag::Event),
        namespace::ENUM => Ok(StableIdNamespaceTag::Enum),
        namespace::AGGREGATE => Ok(StableIdNamespaceTag::Aggregate),
        namespace::COMMAND => Ok(StableIdNamespaceTag::Command),
        namespace::PROJECTION => Ok(StableIdNamespaceTag::Projection),
        namespace::INDEX => Ok(StableIdNamespaceTag::Index),
        namespace::INVARIANT => Ok(StableIdNamespaceTag::Invariant),
        namespace::FIELD => Ok(StableIdNamespaceTag::Field),
        namespace::OUTCOME => Ok(StableIdNamespaceTag::Outcome),
        namespace::ENUM_VARIANT => Ok(StableIdNamespaceTag::EnumVariant),
        tag => Err(IrValidationError::UnknownTag {
            kind: "stable ID namespace",
            tag,
        }),
    }
}

fn decode_contract_version(reader: &mut Reader<'_>) -> Result<ContractVersion, IrValidationError> {
    ContractVersion::new(reader.u64()?).ok_or(IrValidationError::InvalidReference {
        kind: "migration contract version",
    })
}

macro_rules! decode_id {
    ($name:ident, $type:ty, $kind:literal) => {
        fn $name(value: u32) -> Result<$type, IrValidationError> {
            <$type>::new(value).ok_or(IrValidationError::InvalidReference { kind: $kind })
        }
    };
}

decode_id!(entity_id, EntityTypeId, "migration entity ID");
decode_id!(field_id, FieldId, "migration field ID");
decode_id!(enum_id, EnumTypeId, "migration enum ID");
decode_id!(variant_id, EnumVariantId, "migration enum variant ID");
decode_id!(index_id, IndexId, "migration index ID");
decode_id!(invariant_id, InvariantId, "migration invariant ID");
decode_id!(projection_id, ProjectionId, "migration projection ID");
decode_id!(aggregate_id, AggregateTypeId, "migration aggregate ID");

fn require_version(
    actual: u32,
    expected: u32,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    if actual == expected {
        Ok(())
    } else {
        Err(IrValidationError::UnsupportedVersion {
            kind,
            value: actual,
        })
    }
}
