//! Catalog-sealed contract migration plans and pure Gate-A/Gate-B row evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{
    CompatibilityClass, CompatibilityCode, ContractCandidateV1, EntitySchema, LineageEntryState,
    MigrationBundleV1, MigrationConversionV1, MigrationExpressionV1, MigrationStepKindV1,
    StableIdNamespace, StableIdNamespaceTag, StableIdentity, ValueType, compare_successor,
};
use riffdb_invariant::{
    EvaluationError, ExpressionValueSource, evaluate_expression, evaluate_predicate,
};
use riffdb_storage_api::{
    CatalogRepository, DurableKeySchemaBindingV1, EntityTarget, MigrationScanCursor,
    MigrationStageError, MigrationStagePort, StoredEntityRecordV1, StoredIndexEntryV2,
};
use riffdb_types::{
    CanonicalList, CanonicalRecord, CanonicalValue, ContractBundleHash,
    ContractMigrationValidationDigest, EntityTypeId, EnumTypeId, EnumVariantId, FieldId, IndexId,
    MigrationBundleHash, hash_contract_migration_validation,
};

use crate::ValidatedContractBundle;
use crate::lineage::LineageMaterializationProof;
use crate::materialization::validate_static_value;

/// Value-free migration diagnostic codes owned by the semantic catalog boundary.
pub mod migration_finding_code {
    /// Artifact identities or exact proof coverage do not match.
    pub const ARTIFACT_MISMATCH: &str = "RDB-M100";
    /// The artifact contains a valid but not-yet-executable migration step.
    pub const UNSUPPORTED_STEP: &str = "RDB-M101";
    /// A predecessor row is structurally invalid for its exact schema.
    pub const INVALID_ROW: &str = "RDB-M102";
    /// A changed entity cannot advance its authoritative version.
    pub const ENTITY_VERSION_EXHAUSTED: &str = "RDB-M103";
    /// A deterministic row expression failed checked arithmetic.
    pub const TRANSFORM_ARITHMETIC: &str = "RDB-M104";
    /// A successor invariant rejected a transformed row.
    pub const INVARIANT_REJECTED: &str = "RDB-M105";
    /// A successor index value could not be derived exactly.
    pub const INDEX_INVALID: &str = "RDB-M106";
    /// A required relationship target is absent.
    pub const RELATIONSHIP_MISSING: &str = "RDB-M107";
    /// A successor unique key collides.
    pub const UNIQUE_CONFLICT: &str = "RDB-M108";
    /// A fixed migration resource bound was exceeded.
    pub const RESOURCE_LIMIT: &str = "RDB-M109";
    /// Transaction-current row evidence changed before a bounded batch applied.
    pub const ROW_CHANGED: &str = "RDB-M110";
    /// Migration storage or journal state is internally inconsistent.
    pub const INTEGRITY: &str = "RDB-M111";
    /// An unresolved predecessor admission would be stranded by cutover.
    pub const PENDING_ADMISSION: &str = "RDB-M112";
}

/// One bounded, value-free semantic migration finding.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MigrationFinding {
    code: &'static str,
    entity_type: Option<EntityTypeId>,
    field: Option<FieldId>,
    index: Option<IndexId>,
}

impl MigrationFinding {
    const fn new(code: &'static str) -> Self {
        Self {
            code,
            entity_type: None,
            field: None,
            index: None,
        }
    }

    const fn entity(mut self, entity_type: EntityTypeId) -> Self {
        self.entity_type = Some(entity_type);
        self
    }

    const fn with_field(mut self, field: FieldId) -> Self {
        self.field = Some(field);
        self
    }

    const fn with_index(mut self, index: IndexId) -> Self {
        self.index = Some(index);
        self
    }

    /// Stable closed diagnostic code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.code
    }

    /// Optional affected entity identity.
    #[must_use]
    pub const fn entity_type(self) -> Option<EntityTypeId> {
        self.entity_type
    }

    /// Optional affected field identity.
    #[must_use]
    pub const fn field(self) -> Option<FieldId> {
        self.field
    }

    /// Optional affected index identity.
    #[must_use]
    pub const fn index(self) -> Option<IndexId> {
        self.index
    }

    /// Maps a value-free stage failure into the catalog-owned diagnostic set.
    #[must_use]
    pub const fn from_stage_error(error: riffdb_storage_api::MigrationStageError) -> Self {
        match error {
            riffdb_storage_api::MigrationStageError::LimitExceeded
            | riffdb_storage_api::MigrationStageError::SequenceExhausted => {
                Self::new(migration_finding_code::RESOURCE_LIMIT)
            }
            riffdb_storage_api::MigrationStageError::RowChanged => {
                Self::new(migration_finding_code::ROW_CHANGED)
            }
            riffdb_storage_api::MigrationStageError::Integrity => {
                Self::new(migration_finding_code::INTEGRITY)
            }
            riffdb_storage_api::MigrationStageError::RelationshipMissing(entity) => {
                Self::new(migration_finding_code::RELATIONSHIP_MISSING).entity(entity)
            }
            riffdb_storage_api::MigrationStageError::UniqueConflict { entity, index } => {
                Self::new(migration_finding_code::UNIQUE_CONFLICT)
                    .entity(entity)
                    .with_index(index)
            }
            riffdb_storage_api::MigrationStageError::PendingAdmission => {
                Self::new(migration_finding_code::PENDING_ADMISSION)
            }
        }
    }
}

impl fmt::Debug for MigrationFinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MigrationFinding")
            .field("code", &self.code)
            .field("entity_type", &self.entity_type)
            .field("field", &self.field)
            .field("index", &self.index)
            .finish()
    }
}

/// One relationship target derived from a transformed source row.
#[derive(Clone, Eq, PartialEq)]
pub struct MigrationRelationshipFact {
    name: String,
    source: EntityTarget,
    target: EntityTarget,
}

impl MigrationRelationshipFact {
    /// Relationship name from the checked successor.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Complete referencing row target.
    #[must_use]
    pub const fn source(&self) -> &EntityTarget {
        &self.source
    }

    /// Complete required target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }
}

/// One complete successor unique-prefix fact.
#[derive(Clone, Eq, PartialEq)]
pub struct MigrationUniqueFact {
    entity_type: EntityTypeId,
    index: IndexId,
    prefix: Vec<u8>,
    source: EntityTarget,
}

impl MigrationUniqueFact {
    /// Owning entity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Backing successor index.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        self.index
    }

    /// Canonical complete-component prefix bytes.
    #[must_use]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Source row whose uniqueness is being proved.
    #[must_use]
    pub const fn source(&self) -> &EntityTarget {
        &self.source
    }
}

/// Pure checked result for one predecessor row.
#[must_use = "prepared migration rows must be checked or applied"]
pub struct PreparedMigrationRow {
    source: StoredEntityRecordV1,
    post_image: Option<StoredEntityRecordV1>,
    rebuilt_indexes: Vec<StoredIndexEntryV2>,
    relationships: Vec<MigrationRelationshipFact>,
    unique_keys: Vec<MigrationUniqueFact>,
    retired: bool,
}

/// Move-only catalog proof that every staged row has valid successor meaning.
pub struct ValidatedMigrationStage {
    validation_digest: ContractMigrationValidationDigest,
    checked_rows: u64,
}

impl ValidatedMigrationStage {
    /// Returns the canonical semantic validation digest.
    #[must_use]
    pub const fn validation_digest(&self) -> ContractMigrationValidationDigest {
        self.validation_digest
    }

    /// Returns the complete authoritative row count validated.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }
}

impl fmt::Debug for ValidatedMigrationStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedMigrationStage")
            .field("validation_digest", &self.validation_digest)
            .field("checked_rows", &self.checked_rows)
            .finish()
    }
}

impl PreparedMigrationRow {
    /// Exact source evidence used by preflight and transaction-current recheck.
    #[must_use]
    pub const fn source(&self) -> &StoredEntityRecordV1 {
        &self.source
    }

    /// Successor-bound post-image, absent when the entity row need not change.
    #[must_use]
    pub const fn post_image(&self) -> Option<&StoredEntityRecordV1> {
        self.post_image.as_ref()
    }

    /// Newly derived successor index rows.
    #[must_use]
    pub fn rebuilt_indexes(&self) -> &[StoredIndexEntryV2] {
        &self.rebuilt_indexes
    }

    /// Required relationship targets derived from this row.
    #[must_use]
    pub fn relationships(&self) -> &[MigrationRelationshipFact] {
        &self.relationships
    }

    /// Complete unique-key prefixes derived from this row.
    #[must_use]
    pub fn unique_keys(&self) -> &[MigrationUniqueFact] {
        &self.unique_keys
    }

    /// Whether this authoritative row is logically retired from current state.
    #[must_use]
    pub const fn is_retired(&self) -> bool {
        self.retired
    }
}

#[derive(Clone)]
struct FieldReplacement {
    old: FieldId,
    new: FieldId,
    conversion: MigrationConversionV1,
    old_type: ValueType,
    new_type: ValueType,
}

/// Catalog-owned exact Gate-A migration proof.
///
/// Fields are private and artifacts are re-decoded at construction, so callers
/// cannot assemble a migration authority from independently checked values.
pub struct ValidatedMigrationPlan {
    parent: ValidatedContractBundle,
    parent_lineage: Option<Arc<LineageMaterializationProof>>,
    parent_lineage_hashes: Vec<ContractBundleHash>,
    candidate: ValidatedContractBundle,
    migration: MigrationBundleV1,
    set_fields: BTreeMap<EntityTypeId, Vec<(FieldId, MigrationExpressionV1)>>,
    replacements: BTreeMap<EntityTypeId, Vec<FieldReplacement>>,
    requirements: BTreeMap<EntityTypeId, Vec<MigrationExpressionV1>>,
    enum_maps: BTreeMap<EnumTypeId, BTreeMap<EnumVariantId, EnumVariantId>>,
    retired_entities: BTreeSet<EntityTypeId>,
    rebuilt_indexes: BTreeMap<EntityTypeId, Vec<IndexId>>,
    added_invariants: BTreeMap<EntityTypeId, BTreeSet<riffdb_types::InvariantId>>,
    added_relationships: BTreeSet<(EntityTypeId, String)>,
    added_unique: BTreeSet<(EntityTypeId, IndexId)>,
    rebuilt_projections: BTreeSet<riffdb_types::ProjectionId>,
}

impl ValidatedMigrationPlan {
    /// Rebuilds the bounded current lineage from repository evidence before
    /// sealing one executable migration plan.
    pub fn from_current_catalog_artifacts<R: CatalogRepository>(
        repository: &R,
        candidate: ValidatedContractBundle,
        migration: MigrationBundleV1,
    ) -> Result<Self, MigrationFinding> {
        let active = repository
            .read_active_catalog()
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?
            .ok_or_else(|| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        let proof = LineageMaterializationProof::load_active(repository, &active)
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        let parent = proof.terminal().clone();
        Self::from_artifacts_with_lineage(parent, Some(proof), candidate, migration)
    }

    /// Revalidates exact artifacts and seals one complete executable Gate-A plan.
    pub fn from_artifacts(
        parent: ValidatedContractBundle,
        candidate: ValidatedContractBundle,
        migration: MigrationBundleV1,
    ) -> Result<Self, MigrationFinding> {
        Self::from_artifacts_with_lineage(parent, None, candidate, migration)
    }

    /// Revalidates an exact active parent lineage and seals ancestor-row normalization.
    pub fn from_lineage_artifacts(
        parent_lineage: Vec<ValidatedContractBundle>,
        candidate: ValidatedContractBundle,
        migration: MigrationBundleV1,
    ) -> Result<Self, MigrationFinding> {
        let proof = LineageMaterializationProof::from_forward_bundles(parent_lineage)
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        let parent = proof.terminal().clone();
        Self::from_artifacts_with_lineage(parent, Some(proof), candidate, migration)
    }

    fn from_artifacts_with_lineage(
        parent: ValidatedContractBundle,
        parent_lineage: Option<Arc<LineageMaterializationProof>>,
        candidate: ValidatedContractBundle,
        migration: MigrationBundleV1,
    ) -> Result<Self, MigrationFinding> {
        let parent_lineage_hashes = parent_lineage.as_ref().map_or_else(
            || vec![parent.bundle_hash()],
            |proof| {
                proof
                    .bundles()
                    .iter()
                    .map(ValidatedContractBundle::bundle_hash)
                    .collect()
            },
        );
        let migration = MigrationBundleV1::decode(migration.canonical_bytes())
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        if parent.lineage() != candidate.lineage()
            || migration.lineage() != parent.lineage()
            || migration.parent_version() != parent.contract_version()
            || migration.parent_bundle_hash() != parent.bundle_hash()
            || migration.candidate_version() != candidate.contract_version()
            || migration.candidate_bundle_hash() != candidate.bundle_hash()
        {
            return Err(MigrationFinding::new(
                migration_finding_code::ARTIFACT_MISMATCH,
            ));
        }
        let direct = ContractCandidateV1::new(
            candidate.bundle().schema(),
            candidate.bundle().commands(),
            candidate.bundle().projections(),
            candidate.bundle().mcp_command_names(),
        )
        .and_then(|candidate| compare_successor(parent.bundle(), candidate))
        .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        if direct.entries().is_empty()
            || direct.entries().iter().any(|entry| {
                entry.class() == CompatibilityClass::Incompatible
                    && entry.code() != CompatibilityCode::RemovedIdentity
            })
        {
            return Err(MigrationFinding::new(
                migration_finding_code::ARTIFACT_MISMATCH,
            ));
        }

        let mut expected = expected_gate_a_steps(&parent, &candidate)?;
        let mut actual = BTreeSet::new();
        let mut set_fields = BTreeMap::<EntityTypeId, Vec<(FieldId, MigrationExpressionV1)>>::new();
        let mut replacements = BTreeMap::<EntityTypeId, Vec<FieldReplacement>>::new();
        let mut requirements = BTreeMap::<EntityTypeId, Vec<MigrationExpressionV1>>::new();
        let mut enum_maps = BTreeMap::<EnumTypeId, BTreeMap<EnumVariantId, EnumVariantId>>::new();
        let mut retired_entities = BTreeSet::new();
        let mut covered_removed = BTreeSet::new();
        let mut rebuilt_indexes = BTreeMap::<EntityTypeId, Vec<IndexId>>::new();
        let mut added_invariants =
            BTreeMap::<EntityTypeId, BTreeSet<riffdb_types::InvariantId>>::new();
        let mut added_relationships = BTreeSet::new();
        let mut added_unique = BTreeSet::new();
        let mut rebuilt_projections = BTreeSet::new();
        for step in migration.steps() {
            let key = match step.kind() {
                MigrationStepKindV1::RenameIdentity {
                    namespace,
                    owner_kind,
                    owner_ids,
                    stable_id,
                    new_name,
                } => {
                    let identity = migration_identity(
                        &parent,
                        *namespace,
                        *owner_kind,
                        owner_ids,
                        *stable_id,
                    )?;
                    validate_rename_step(&parent, &candidate, &identity, new_name)?;
                    covered_removed.insert(identity.clone());
                    let key = GateAStepKey::Rename(identity);
                    expected.insert(key.clone());
                    key
                }
                MigrationStepKindV1::RetireIdentity {
                    namespace,
                    owner_kind,
                    owner_ids,
                    stable_id,
                } => {
                    let identity = migration_identity(
                        &parent,
                        *namespace,
                        *owner_kind,
                        owner_ids,
                        *stable_id,
                    )?;
                    validate_retire_step(&parent, &candidate, &identity)?;
                    covered_removed.extend(retirement_identity_keys(&parent, &identity));
                    if *namespace == StableIdNamespaceTag::Entity {
                        retired_entities
                            .insert(EntityTypeId::new(*stable_id).ok_or_else(artifact_mismatch)?);
                    }
                    let key = GateAStepKey::Retire(identity);
                    expected.insert(key.clone());
                    key
                }
                MigrationStepKindV1::SetField {
                    entity,
                    field,
                    expression,
                } => {
                    let schema = candidate
                        .bundle()
                        .schema()
                        .entity(*entity)
                        .and_then(|entity| entity.record().field(*field))
                        .ok_or_else(|| {
                            MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH)
                        })?;
                    if expression
                        .arena()
                        .get(expression.result())
                        .is_none_or(|node| node.result_type() != schema.value_type())
                    {
                        return Err(MigrationFinding::new(
                            migration_finding_code::ARTIFACT_MISMATCH,
                        ));
                    }
                    set_fields
                        .entry(*entity)
                        .or_default()
                        .push((*field, expression.clone()));
                    GateAStepKey::SetField(*entity, *field)
                }
                MigrationStepKindV1::ReplaceField {
                    entity,
                    old_field,
                    new_field,
                    conversion,
                } => {
                    let old = parent
                        .bundle()
                        .schema()
                        .entity(*entity)
                        .and_then(|schema| schema.record().field(*old_field))
                        .ok_or_else(artifact_mismatch)?;
                    let new = candidate
                        .bundle()
                        .schema()
                        .entity(*entity)
                        .and_then(|schema| schema.record().field(*new_field))
                        .ok_or_else(artifact_mismatch)?;
                    if old_field == new_field
                        || !conversion.accepts(old.value_type(), new.value_type())
                    {
                        return Err(artifact_mismatch());
                    }
                    replacements
                        .entry(*entity)
                        .or_default()
                        .push(FieldReplacement {
                            old: *old_field,
                            new: *new_field,
                            conversion: *conversion,
                            old_type: old.value_type().clone(),
                            new_type: new.value_type().clone(),
                        });
                    expected.remove(&GateAStepKey::SetField(*entity, *new_field));
                    covered_removed.insert(field_identity_key(*entity, *old_field, old.name())?);
                    let key = GateAStepKey::Replace(*entity, *old_field, *new_field);
                    expected.insert(key.clone());
                    key
                }
                MigrationStepKindV1::RequireEntity { entity, predicate } => {
                    let valid = parent.bundle().schema().entity(*entity).is_some()
                        && predicate
                            .arena()
                            .get(predicate.result())
                            .is_some_and(|node| {
                                node.result_type().tag() == riffdb_contract_ir::ValueTypeTag::Bool
                            });
                    if !valid {
                        return Err(artifact_mismatch());
                    }
                    requirements
                        .entry(*entity)
                        .or_default()
                        .push(predicate.clone());
                    let key = GateAStepKey::Require(*entity, step.id().get());
                    expected.insert(key.clone());
                    key
                }
                MigrationStepKindV1::MapEnum {
                    enumeration,
                    mappings,
                } => {
                    let map = validate_enum_map(&parent, &candidate, *enumeration, mappings)?;
                    covered_removed.extend(removed_enum_variant_keys(
                        &parent,
                        &candidate,
                        *enumeration,
                    )?);
                    enum_maps.insert(*enumeration, map);
                    let key = GateAStepKey::EnumMap(*enumeration);
                    expected.insert(key.clone());
                    key
                }
                MigrationStepKindV1::RebuildIndex { entity, index } => {
                    rebuilt_indexes.entry(*entity).or_default().push(*index);
                    GateAStepKey::RebuildIndex(*entity, *index)
                }
                MigrationStepKindV1::ValidateRelationship {
                    source_entity,
                    name,
                } => {
                    added_relationships.insert((*source_entity, name.clone()));
                    GateAStepKey::Relationship(*source_entity, name.clone())
                }
                MigrationStepKindV1::ValidateUnique { entity, index } => {
                    added_unique.insert((*entity, *index));
                    GateAStepKey::Unique(*entity, *index)
                }
                MigrationStepKindV1::ValidateInvariant {
                    owner_namespace: StableIdNamespaceTag::Entity,
                    owner_id,
                    invariant,
                } => {
                    let entity = EntityTypeId::new(*owner_id).ok_or_else(|| {
                        MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH)
                    })?;
                    added_invariants
                        .entry(entity)
                        .or_default()
                        .insert(*invariant);
                    GateAStepKey::EntityInvariant(entity, *invariant)
                }
                MigrationStepKindV1::ValidateInvariant {
                    owner_namespace: StableIdNamespaceTag::Aggregate,
                    ..
                } => {
                    return Err(MigrationFinding::new(
                        migration_finding_code::UNSUPPORTED_STEP,
                    ));
                }
                MigrationStepKindV1::RebuildProjection { projection } => {
                    rebuilt_projections.insert(*projection);
                    GateAStepKey::Projection(*projection)
                }
                _ => {
                    return Err(MigrationFinding::new(
                        migration_finding_code::UNSUPPORTED_STEP,
                    ));
                }
            };
            if !actual.insert(key) {
                return Err(MigrationFinding::new(
                    migration_finding_code::ARTIFACT_MISMATCH,
                ));
            }
        }
        let removed = removed_identity_keys(&parent, &candidate)?;
        if !removed.is_subset(&covered_removed) {
            return Err(artifact_mismatch());
        }
        if actual != expected {
            return Err(MigrationFinding::new(
                migration_finding_code::ARTIFACT_MISMATCH,
            ));
        }
        for fields in set_fields.values_mut() {
            fields.sort_unstable_by_key(|(field, _)| *field);
        }
        for indexes in rebuilt_indexes.values_mut() {
            indexes.sort_unstable();
        }
        Ok(Self {
            parent,
            parent_lineage,
            parent_lineage_hashes,
            candidate,
            migration,
            set_fields,
            replacements,
            requirements,
            enum_maps,
            retired_entities,
            rebuilt_indexes,
            added_invariants,
            added_relationships,
            added_unique,
            rebuilt_projections,
        })
    }

    /// Exact canonical migration identity.
    #[must_use]
    pub const fn migration_bundle_hash(&self) -> MigrationBundleHash {
        self.migration.bundle_hash()
    }

    /// Exact parent bundle identity.
    #[must_use]
    pub fn parent_bundle_hash(&self) -> ContractBundleHash {
        self.parent.bundle_hash()
    }

    /// Exact successor bundle identity.
    #[must_use]
    pub fn candidate_bundle_hash(&self) -> ContractBundleHash {
        self.candidate.bundle_hash()
    }

    /// Borrows exact active-lineage bundle hashes accepted on unchanged rows.
    #[must_use]
    pub fn parent_lineage_hashes(&self) -> &[ContractBundleHash] {
        &self.parent_lineage_hashes
    }

    /// Exact checked parent bundle.
    #[must_use]
    pub const fn parent(&self) -> &ValidatedContractBundle {
        &self.parent
    }

    /// Exact checked successor bundle.
    #[must_use]
    pub const fn candidate(&self) -> &ValidatedContractBundle {
        &self.candidate
    }

    /// Successor projections that require a fresh candidate generation.
    #[must_use]
    pub fn rebuilt_projections(&self) -> &BTreeSet<riffdb_types::ProjectionId> {
        &self.rebuilt_projections
    }

    pub(crate) fn candidate_lineage_proof(
        &self,
    ) -> Result<Arc<LineageMaterializationProof>, MigrationFinding> {
        let parent = self
            .parent_lineage
            .clone()
            .map_or_else(
                || LineageMaterializationProof::from_forward_bundles(vec![self.parent.clone()]),
                Ok,
            )
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
        parent
            .extend_migrated(self.candidate.clone())
            .map_err(|_| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))
    }

    /// Purely validates and prepares one predecessor row.
    pub fn prepare_row(
        &self,
        row: StoredEntityRecordV1,
    ) -> Result<PreparedMigrationRow, MigrationFinding> {
        let entity_id = row.target().entity_type_id();
        let parent_entity = self
            .parent
            .bundle()
            .schema()
            .entity(entity_id)
            .ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity_id)
            })?;
        let parent_fields = self.materialize_parent_fields(&row, parent_entity)?;
        if let Some(requirements) = self.requirements.get(&entity_id) {
            let values = SchemaRowValues {
                row: &parent_fields,
            };
            for predicate in requirements {
                if !evaluate_predicate(predicate.arena(), predicate.result(), &values).map_err(
                    |error| expression_finding(error, entity_id, FieldId::new(1).expect("nonzero")),
                )? {
                    return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW)
                        .entity(entity_id));
                }
            }
        }
        if self.retired_entities.contains(&entity_id) {
            return Ok(PreparedMigrationRow {
                source: row,
                post_image: None,
                rebuilt_indexes: Vec::new(),
                relationships: Vec::new(),
                unique_keys: Vec::new(),
                retired: true,
            });
        }
        let candidate_entity = self
            .candidate
            .bundle()
            .schema()
            .entity(entity_id)
            .ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity_id)
            })?;

        let mut output = parent_fields
            .fields()
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>();
        let mut changed = false;
        if let Some(replacements) = self.replacements.get(&entity_id) {
            for replacement in replacements {
                let value = output.remove(&replacement.old).ok_or_else(|| {
                    MigrationFinding::new(migration_finding_code::INVALID_ROW)
                        .entity(entity_id)
                        .with_field(replacement.old)
                })?;
                let value = convert_value(
                    replacement.conversion,
                    value,
                    &replacement.old_type,
                    &replacement.new_type,
                )
                .ok_or_else(|| {
                    MigrationFinding::new(migration_finding_code::INVALID_ROW)
                        .entity(entity_id)
                        .with_field(replacement.old)
                })?;
                output.insert(replacement.new, value);
                changed = true;
            }
        }
        if let Some(fields) = self.set_fields.get(&entity_id) {
            let values = SchemaRowValues {
                row: &parent_fields,
            };
            for (field, expression) in fields {
                let value = evaluate_expression(expression.arena(), expression.result(), &values)
                    .map_err(|error| expression_finding(error, entity_id, *field))?;
                output.insert(*field, value);
                changed = true;
            }
        }
        for value in output.values_mut() {
            if apply_enum_maps(value, &self.enum_maps)? {
                changed = true;
            }
        }
        if changed {
            for field in candidate_entity.record().fields() {
                if !output.contains_key(&field.id()) && field.value_type().is_optional() {
                    output.insert(field.id(), CanonicalValue::Null);
                }
            }
        }
        let effective = if changed {
            CanonicalRecord::new(output.into_iter().collect()).map_err(|_| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity_id)
            })?
        } else {
            parent_fields
        };
        validate_candidate_record(
            self.candidate.bundle().schema(),
            candidate_entity,
            &effective,
        )?;
        validate_added_invariants(self, candidate_entity, &effective)?;

        let post_image = if changed {
            let version = row.entity_version().checked_next().ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::ENTITY_VERSION_EXHAUSTED)
                    .entity(entity_id)
            })?;
            Some(
                StoredEntityRecordV1::new(
                    row.target().clone(),
                    version,
                    self.candidate.contract_version(),
                    DurableKeySchemaBindingV1::new(
                        self.candidate.lineage().clone(),
                        self.candidate.contract_version(),
                        self.candidate.bundle_hash(),
                    ),
                    effective.clone(),
                )
                .map_err(|_| {
                    MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity_id)
                })?,
            )
        } else {
            None
        };
        let partition = derive_partition(self, entity_id, &effective)?;
        let rebuilt_indexes = derive_indexes(self, candidate_entity, &row, &effective, &partition)?;
        let relationships = derive_relationships(self, entity_id, &row, &effective)?;
        let unique_keys = derive_unique(self, candidate_entity, &row, &effective)?;
        Ok(PreparedMigrationRow {
            source: row,
            post_image,
            rebuilt_indexes,
            relationships,
            unique_keys,
            retired: false,
        })
    }

    /// Completely revalidates staged rows under successor catalog meaning.
    pub fn validate_successor_stage<S: MigrationStagePort>(
        &self,
        stage: &S,
        expected_rows: u64,
    ) -> Result<ValidatedMigrationStage, MigrationFinding> {
        if stage.active_bundle_hash() != self.parent_bundle_hash() {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::Integrity,
            ));
        }
        let mut cursor = MigrationScanCursor::start();
        let mut checked_rows = 0_u64;
        loop {
            let page = stage
                .scan_migration_rows(&cursor)
                .map_err(MigrationFinding::from_stage_error)?;
            if page.rows().first().is_some_and(|row| {
                cursor
                    .exclusive_lower_bound()
                    .is_some_and(|lower| row.target() <= lower)
            }) || page.next().is_some_and(|next| next <= &cursor)
            {
                return Err(MigrationFinding::from_stage_error(
                    MigrationStageError::Integrity,
                ));
            }
            for row in page.rows() {
                let prepared = self.successor_stage_facts(row)?;
                for relationship in prepared.relationships() {
                    if !stage
                        .migration_target_exists(relationship.target())
                        .map_err(MigrationFinding::from_stage_error)?
                    {
                        return Err(MigrationFinding::from_stage_error(
                            MigrationStageError::RelationshipMissing(
                                relationship.source().entity_type_id(),
                            ),
                        ));
                    }
                }
                for unique in prepared.unique_keys() {
                    self.validate_successor_unique(stage, unique)?;
                }
                checked_rows = checked_rows.checked_add(1).ok_or_else(|| {
                    MigrationFinding::from_stage_error(MigrationStageError::LimitExceeded)
                })?;
            }
            let Some(next) = page.next() else {
                break;
            };
            cursor = next.clone();
        }
        let retired_rows = stage
            .retained_migration_entity_count(
                self.migration_bundle_hash(),
                &self.retired_entities.iter().copied().collect::<Vec<_>>(),
            )
            .map_err(MigrationFinding::from_stage_error)?;
        if checked_rows.checked_add(retired_rows) != Some(expected_rows) {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::RowChanged,
            ));
        }
        let mut bytes = Vec::with_capacity(32 * 3 + 8);
        bytes.extend_from_slice(self.parent_bundle_hash().as_bytes());
        bytes.extend_from_slice(self.candidate_bundle_hash().as_bytes());
        bytes.extend_from_slice(self.migration_bundle_hash().as_bytes());
        bytes.extend_from_slice(&checked_rows.to_be_bytes());
        Ok(ValidatedMigrationStage {
            validation_digest: hash_contract_migration_validation(&bytes),
            checked_rows,
        })
    }

    fn successor_stage_facts(
        &self,
        row: &StoredEntityRecordV1,
    ) -> Result<PreparedMigrationRow, MigrationFinding> {
        if row.schema_binding().bundle_hash() != self.candidate_bundle_hash() {
            let prepared = self.prepare_row(row.clone())?;
            if prepared.post_image().is_some() {
                return Err(MigrationFinding::from_stage_error(
                    MigrationStageError::RowChanged,
                ));
            }
            return Ok(prepared);
        }
        if row.schema_binding().lineage() != self.candidate.lineage()
            || row.schema_binding().contract_version() != self.candidate.contract_version()
            || row.written_by_contract() != self.candidate.contract_version()
        {
            return Err(MigrationFinding::from_stage_error(
                MigrationStageError::Integrity,
            ));
        }
        let entity_id = row.target().entity_type_id();
        let entity = self
            .candidate
            .bundle()
            .schema()
            .entity(entity_id)
            .ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity_id)
            })?;
        validate_record(self.candidate.bundle().schema(), entity, row)?;
        validate_all_invariants(entity, row.fields())?;
        let relationships = derive_relationships(self, entity_id, row, row.fields())?;
        let unique_keys = derive_unique(self, entity, row, row.fields())?;
        Ok(PreparedMigrationRow {
            source: row.clone(),
            post_image: None,
            rebuilt_indexes: Vec::new(),
            relationships,
            unique_keys,
            retired: false,
        })
    }

    fn validate_successor_unique<S: MigrationStagePort>(
        &self,
        stage: &S,
        expected: &MigrationUniqueFact,
    ) -> Result<(), MigrationFinding> {
        let mut cursor = MigrationScanCursor::start();
        loop {
            let page = stage
                .scan_migration_rows(&cursor)
                .map_err(MigrationFinding::from_stage_error)?;
            for candidate in page.rows() {
                if candidate.target() == expected.source() {
                    continue;
                }
                let candidate = self.successor_stage_facts(candidate)?;
                if candidate.unique_keys().iter().any(|fact| {
                    fact.entity_type() == expected.entity_type()
                        && fact.index() == expected.index()
                        && fact.prefix() == expected.prefix()
                }) {
                    return Err(MigrationFinding::from_stage_error(
                        MigrationStageError::UniqueConflict {
                            entity: expected.entity_type(),
                            index: expected.index(),
                        },
                    ));
                }
            }
            let Some(next) = page.next() else {
                break;
            };
            cursor = next.clone();
        }
        Ok(())
    }

    fn materialize_parent_fields(
        &self,
        row: &StoredEntityRecordV1,
        parent_entity: &EntitySchema,
    ) -> Result<CanonicalRecord, MigrationFinding> {
        if row.schema_binding().lineage() != self.parent.lineage() {
            return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW)
                .entity(parent_entity.id()));
        }
        if row.schema_binding().contract_version() == self.parent.contract_version()
            && row.schema_binding().bundle_hash() == self.parent.bundle_hash()
            && row.written_by_contract() == self.parent.contract_version()
        {
            validate_record(self.parent.bundle().schema(), parent_entity, row)?;
            return Ok(row.fields().clone());
        }

        let proof = self.parent_lineage.as_ref().ok_or_else(|| {
            MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(parent_entity.id())
        })?;
        let (_, writer) = proof
            .exact_binding_member(row.schema_binding())
            .ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW)
                    .entity(parent_entity.id())
            })?;
        if row.written_by_contract() != writer.contract_version() {
            return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW)
                .entity(parent_entity.id()));
        }
        let writer_entity = writer
            .bundle()
            .schema()
            .entity(parent_entity.id())
            .ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW)
                    .entity(parent_entity.id())
            })?;
        validate_record(writer.bundle().schema(), writer_entity, row)?;

        let fields = parent_entity
            .record()
            .fields()
            .iter()
            .map(|field| {
                if let Some(value) = field_value(row.fields(), field.id()) {
                    return Ok((field.id(), value.clone()));
                }
                if field.value_type().is_optional() {
                    return Ok((field.id(), CanonicalValue::Null));
                }
                Err(MigrationFinding::new(migration_finding_code::INVALID_ROW)
                    .entity(parent_entity.id())
                    .with_field(field.id()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let materialized = CanonicalRecord::new(fields).map_err(|_| {
            MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(parent_entity.id())
        })?;
        validate_candidate_record(self.parent.bundle().schema(), parent_entity, &materialized)?;
        Ok(materialized)
    }
}

impl fmt::Debug for ValidatedMigrationPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedMigrationPlan")
            .field("parent", &"[CHECKED]")
            .field("candidate", &"[CHECKED]")
            .field("migration", &"[CHECKED]")
            .finish()
    }
}

fn convert_value(
    conversion: MigrationConversionV1,
    value: CanonicalValue,
    old_type: &ValueType,
    new_type: &ValueType,
) -> Option<CanonicalValue> {
    if !conversion.accepts(old_type, new_type) {
        return None;
    }
    match conversion {
        MigrationConversionV1::Identity
        | MigrationConversionV1::WrapOptional
        | MigrationConversionV1::AssertBoundedNarrow => Some(value),
        MigrationConversionV1::AssertUnwrapOptional => {
            (!matches!(value, CanonicalValue::Null)).then_some(value)
        }
        MigrationConversionV1::CheckedI64ToU64 => match value {
            CanonicalValue::I64(value) => u64::try_from(value).ok().map(CanonicalValue::U64),
            _ => None,
        },
        MigrationConversionV1::CheckedU64ToI64 => match value {
            CanonicalValue::U64(value) => i64::try_from(value).ok().map(CanonicalValue::I64),
            _ => None,
        },
        MigrationConversionV1::ExactDecimal => match value {
            CanonicalValue::Decimal(value) => value
                .rescale(new_type.decimal_spec()?)
                .ok()
                .map(CanonicalValue::Decimal),
            _ => None,
        },
        MigrationConversionV1::ListElements => {
            let CanonicalValue::List(values) = value else {
                return None;
            };
            let (old_element, _) = old_type.list_parts()?;
            let (new_element, _) = new_type.list_parts()?;
            let element_conversion = inferred_conversion(old_element, new_element)?;
            values
                .into_values()
                .into_iter()
                .map(|value| convert_value(element_conversion, value, old_element, new_element))
                .collect::<Option<Vec<_>>>()
                .and_then(|values| CanonicalList::new(values).ok())
                .map(CanonicalValue::List)
        }
        MigrationConversionV1::UuidToString => match value {
            CanonicalValue::Uuid(value) => CanonicalValue::string(format_uuid(value)).ok(),
            _ => None,
        },
        MigrationConversionV1::StringToUuid => match value {
            CanonicalValue::String(value) => parse_uuid(value.as_str()).map(CanonicalValue::Uuid),
            _ => None,
        },
    }
}

fn inferred_conversion(old: &ValueType, new: &ValueType) -> Option<MigrationConversionV1> {
    [
        MigrationConversionV1::Identity,
        MigrationConversionV1::WrapOptional,
        MigrationConversionV1::AssertUnwrapOptional,
        MigrationConversionV1::CheckedI64ToU64,
        MigrationConversionV1::CheckedU64ToI64,
        MigrationConversionV1::ExactDecimal,
        MigrationConversionV1::UuidToString,
        MigrationConversionV1::StringToUuid,
    ]
    .into_iter()
    .find(|conversion| conversion.accepts(old, new))
}

fn format_uuid(value: [u8; 16]) -> String {
    let mut output = String::with_capacity(36);
    for (index, byte) in value.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    if value.len() != 36
        || value
            .bytes()
            .enumerate()
            .any(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) != (byte == b'-'))
    {
        return None;
    }
    let hex = value
        .bytes()
        .filter(|byte| *byte != b'-')
        .collect::<Vec<_>>();
    let mut output = [0_u8; 16];
    for (slot, pair) in output.iter_mut().zip(hex.chunks_exact(2)) {
        *slot = hex_nibble(pair[0])?
            .checked_mul(16)?
            .checked_add(hex_nibble(pair[1])?)?;
    }
    Some(output)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn apply_enum_maps(
    value: &mut CanonicalValue,
    maps: &BTreeMap<EnumTypeId, BTreeMap<EnumVariantId, EnumVariantId>>,
) -> Result<bool, MigrationFinding> {
    match value {
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => {
            let Some(map) = maps.get(type_id) else {
                return Ok(false);
            };
            let target = map.get(variant_id).copied().ok_or_else(artifact_mismatch)?;
            let changed = target != *variant_id;
            *variant_id = target;
            Ok(changed)
        }
        CanonicalValue::List(values) => {
            let mut changed = false;
            let mut output = values.values().to_vec();
            for value in &mut output {
                changed |= apply_enum_maps(value, maps)?;
            }
            if changed {
                *values = CanonicalList::new(output).map_err(|_| artifact_mismatch())?;
            }
            Ok(changed)
        }
        CanonicalValue::Record(record) => {
            let mut changed = false;
            let mut output = record.fields().to_vec();
            for (_, value) in &mut output {
                changed |= apply_enum_maps(value, maps)?;
            }
            if changed {
                *record = CanonicalRecord::new(output).map_err(|_| artifact_mismatch())?;
            }
            Ok(changed)
        }
        _ => Ok(false),
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum GateAStepKey {
    Rename(StableIdentity),
    Retire(StableIdentity),
    SetField(EntityTypeId, FieldId),
    Replace(EntityTypeId, FieldId, FieldId),
    Require(EntityTypeId, u32),
    EnumMap(EnumTypeId),
    RebuildIndex(EntityTypeId, IndexId),
    Relationship(EntityTypeId, String),
    Unique(EntityTypeId, IndexId),
    EntityInvariant(EntityTypeId, riffdb_types::InvariantId),
    AggregateInvariant(riffdb_types::AggregateTypeId, riffdb_types::InvariantId),
    Projection(riffdb_types::ProjectionId),
}

fn artifact_mismatch() -> MigrationFinding {
    MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH)
}

fn migration_identity(
    parent: &ValidatedContractBundle,
    namespace: StableIdNamespaceTag,
    owner_kind: u8,
    owner_ids: &[u32],
    stable_id: u32,
) -> Result<StableIdentity, MigrationFinding> {
    let namespace = StableIdNamespace::new(namespace, owner_kind, owner_ids.to_vec())
        .map_err(|_| artifact_mismatch())?;
    parent
        .bundle()
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .find(|entry| entry.id() == stable_id && entry.identity().namespace() == &namespace)
        .filter(|entry| entry.state() == LineageEntryState::Active)
        .map(|entry| entry.identity().clone())
        .filter(|identity| identity.namespace() == &namespace)
        .ok_or_else(artifact_mismatch)
}

fn validate_rename_step(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
    old: &StableIdentity,
    new_name: &str,
) -> Result<(), MigrationFinding> {
    let id = parent
        .bundle()
        .ledger()
        .active_id(old)
        .ok_or_else(artifact_mismatch)?;
    let new =
        StableIdentity::new(old.namespace().clone(), new_name).map_err(|_| artifact_mismatch())?;
    if candidate.bundle().ledger().active_id(&new) != Some(id)
        || !candidate
            .bundle()
            .ledger()
            .aliases()
            .iter()
            .any(|alias| alias.identity() == old && alias.id() == id)
    {
        return Err(artifact_mismatch());
    }
    Ok(())
}

fn validate_retire_step(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
    identity: &StableIdentity,
) -> Result<(), MigrationFinding> {
    let id = parent
        .bundle()
        .ledger()
        .active_id(identity)
        .ok_or_else(artifact_mismatch)?;
    let retired = candidate
        .bundle()
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .any(|entry| {
            entry.id() == id
                && entry.identity() == identity
                && entry.state() == LineageEntryState::Tombstone
        });
    if !retired {
        return Err(artifact_mismatch());
    }
    Ok(())
}

fn field_identity_key(
    entity: EntityTypeId,
    field: FieldId,
    name: &str,
) -> Result<StableIdentity, MigrationFinding> {
    let _ = field;
    StableIdentity::new(
        StableIdNamespace::new(StableIdNamespaceTag::Field, 0x01, vec![entity.get()])
            .map_err(|_| artifact_mismatch())?,
        name,
    )
    .map_err(|_| artifact_mismatch())
}

fn removed_identity_keys(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
) -> Result<BTreeSet<StableIdentity>, MigrationFinding> {
    Ok(parent
        .bundle()
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .filter(|entry| entry.state() == LineageEntryState::Active)
        .filter(|entry| {
            candidate
                .bundle()
                .ledger()
                .active_id(entry.identity())
                .is_none()
        })
        .map(|entry| entry.identity().clone())
        .collect())
}

fn retirement_identity_keys(
    parent: &ValidatedContractBundle,
    identity: &StableIdentity,
) -> BTreeSet<StableIdentity> {
    let id = parent.bundle().ledger().active_id(identity).unwrap_or(0);
    let retired_aggregates = if identity.namespace().tag() == StableIdNamespaceTag::Entity {
        parent
            .bundle()
            .schema()
            .aggregates()
            .iter()
            .filter(|aggregate| {
                aggregate.root().get() == id
                    || aggregate.children().iter().any(|child| child.get() == id)
            })
            .map(|aggregate| aggregate.id().get())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    parent
        .bundle()
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .filter(|entry| entry.state() == LineageEntryState::Active)
        .filter(|entry| {
            entry.identity() == identity
                || entry.identity().namespace().tag() == StableIdNamespaceTag::Aggregate
                    && retired_aggregates.contains(&entry.id())
                || entry.identity().namespace().tag() == StableIdNamespaceTag::Invariant
                    && entry.identity().namespace().owner_kind() == 0x02
                    && entry
                        .identity()
                        .namespace()
                        .owner_ids()
                        .first()
                        .is_some_and(|owner| retired_aggregates.contains(owner))
                || identity_descendant(identity.namespace().tag(), id, entry.identity())
        })
        .map(|entry| entry.identity().clone())
        .collect()
}

fn identity_descendant(kind: StableIdNamespaceTag, id: u32, child: &StableIdentity) -> bool {
    let namespace = child.namespace();
    let owner_matches = namespace.owner_ids().first() == Some(&id);
    owner_matches
        && match kind {
            StableIdNamespaceTag::Entity => matches!(
                (namespace.tag(), namespace.owner_kind()),
                (StableIdNamespaceTag::Field, 0x01)
                    | (StableIdNamespaceTag::Index, 0x01)
                    | (StableIdNamespaceTag::Invariant, 0x01)
            ),
            StableIdNamespaceTag::Event => {
                namespace.tag() == StableIdNamespaceTag::Field && namespace.owner_kind() == 0x02
            }
            StableIdNamespaceTag::Enum => {
                namespace.tag() == StableIdNamespaceTag::EnumVariant
                    && namespace.owner_kind() == 0x01
            }
            StableIdNamespaceTag::Command => matches!(
                (namespace.tag(), namespace.owner_kind()),
                (StableIdNamespaceTag::Field, 0x03 | 0x04) | (StableIdNamespaceTag::Outcome, 0x01)
            ),
            StableIdNamespaceTag::Projection => {
                namespace.tag() == StableIdNamespaceTag::Field && namespace.owner_kind() == 0x05
            }
            _ => false,
        }
}

fn validate_enum_map(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
    enumeration: EnumTypeId,
    mappings: &[riffdb_contract_ir::MigrationEnumMappingV1],
) -> Result<BTreeMap<EnumVariantId, EnumVariantId>, MigrationFinding> {
    let old = parent
        .bundle()
        .schema()
        .enumeration(enumeration)
        .ok_or_else(artifact_mismatch)?;
    let new = candidate
        .bundle()
        .schema()
        .enumeration(enumeration)
        .ok_or_else(artifact_mismatch)?;
    let map = mappings
        .iter()
        .map(|mapping| (mapping.from(), mapping.to()))
        .collect::<BTreeMap<_, _>>();
    if map.len() != old.variants().len()
        || old
            .variants()
            .iter()
            .any(|variant| !map.contains_key(&variant.id()))
        || map
            .values()
            .any(|target| new.variants().iter().all(|variant| variant.id() != *target))
    {
        return Err(artifact_mismatch());
    }
    Ok(map)
}

fn removed_enum_variant_keys(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
    enumeration: EnumTypeId,
) -> Result<BTreeSet<StableIdentity>, MigrationFinding> {
    let old = parent
        .bundle()
        .schema()
        .enumeration(enumeration)
        .ok_or_else(artifact_mismatch)?;
    let new = candidate
        .bundle()
        .schema()
        .enumeration(enumeration)
        .ok_or_else(artifact_mismatch)?;
    old.variants()
        .iter()
        .filter(|variant| new.variants().iter().all(|next| next.id() != variant.id()))
        .map(|variant| {
            StableIdentity::new(
                StableIdNamespace::new(
                    StableIdNamespaceTag::EnumVariant,
                    0x01,
                    vec![enumeration.get()],
                )
                .map_err(|_| artifact_mismatch())?,
                variant.name(),
            )
            .map_err(|_| artifact_mismatch())
        })
        .collect()
}

fn expected_gate_a_steps(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
) -> Result<BTreeSet<GateAStepKey>, MigrationFinding> {
    let mut expected = BTreeSet::new();
    for entity in candidate.bundle().schema().entities() {
        let Some(old) = parent.bundle().schema().entity(entity.id()) else {
            continue;
        };
        for field in entity.record().fields() {
            if old.record().field(field.id()).is_none() && !field.value_type().is_optional() {
                expected.insert(GateAStepKey::SetField(entity.id(), field.id()));
            }
        }
        for index in entity.indexes() {
            if old.indexes().iter().all(|old| old.id() != index.id()) {
                expected.insert(GateAStepKey::RebuildIndex(entity.id(), index.id()));
            }
        }
        for invariant in entity.invariants() {
            if old
                .invariants()
                .iter()
                .all(|old| old.id() != invariant.id())
            {
                expected.insert(GateAStepKey::EntityInvariant(entity.id(), invariant.id()));
            }
        }
    }
    for relationship in candidate.bundle().schema().relationships() {
        if parent
            .bundle()
            .schema()
            .entity(relationship.source_entity())
            .is_some()
            && parent.bundle().schema().relationships().iter().all(|old| {
                old.source_entity() != relationship.source_entity()
                    || old.name() != relationship.name()
            })
        {
            expected.insert(GateAStepKey::Relationship(
                relationship.source_entity(),
                relationship.name().to_owned(),
            ));
        }
    }
    for unique in candidate.bundle().schema().unique_keys() {
        if parent
            .bundle()
            .schema()
            .entity(unique.source_entity())
            .is_some()
            && parent.bundle().schema().unique_keys().iter().all(|old| {
                old.source_entity() != unique.source_entity() || old.name() != unique.name()
            })
        {
            expected.insert(GateAStepKey::Unique(
                unique.source_entity(),
                unique.index_id(),
            ));
        }
    }
    for aggregate in candidate.bundle().schema().aggregates() {
        let Some(old) = parent.bundle().schema().aggregate(aggregate.id()) else {
            continue;
        };
        for invariant in aggregate.invariants() {
            if old
                .invariants()
                .iter()
                .all(|old| old.id() != invariant.id())
            {
                expected.insert(GateAStepKey::AggregateInvariant(
                    aggregate.id(),
                    invariant.id(),
                ));
            }
        }
    }
    for projection in candidate.bundle().projections() {
        if parent
            .bundle()
            .projections()
            .iter()
            .all(|old| old.projection_id() != projection.projection_id())
            && parent
                .bundle()
                .schema()
                .event(projection.source_event())
                .is_some()
        {
            expected.insert(GateAStepKey::Projection(projection.projection_id()));
        }
    }
    if expected
        .iter()
        .any(|key| matches!(key, GateAStepKey::AggregateInvariant(..)))
    {
        return Err(MigrationFinding::new(
            migration_finding_code::UNSUPPORTED_STEP,
        ));
    }
    Ok(expected)
}

fn validate_record(
    schema: &riffdb_contract_ir::SchemaIr,
    entity: &EntitySchema,
    row: &StoredEntityRecordV1,
) -> Result<(), MigrationFinding> {
    validate_candidate_record(schema, entity, row.fields())?;
    let values = entity
        .primary_key_fields()
        .iter()
        .map(|field| {
            field_value(row.fields(), *field).cloned().ok_or_else(|| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity.id())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !matches!(
        entity.primary_key().encode_entity(&values),
        Ok(key) if &key == row.target().key()
    ) {
        return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity.id()));
    }
    Ok(())
}

fn validate_candidate_record(
    schema: &riffdb_contract_ir::SchemaIr,
    entity: &EntitySchema,
    record: &CanonicalRecord,
) -> Result<(), MigrationFinding> {
    if record.fields().len() != entity.record().fields().len() {
        return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity.id()));
    }
    for (actual, expected) in record.fields().iter().zip(entity.record().fields()) {
        if actual.0 != expected.id()
            || validate_static_value(schema, expected.value_type(), &actual.1).is_err()
        {
            return Err(MigrationFinding::new(migration_finding_code::INVALID_ROW)
                .entity(entity.id())
                .with_field(expected.id()));
        }
    }
    Ok(())
}

fn validate_added_invariants(
    plan: &ValidatedMigrationPlan,
    entity: &EntitySchema,
    record: &CanonicalRecord,
) -> Result<(), MigrationFinding> {
    let Some(required) = plan.added_invariants.get(&entity.id()) else {
        return Ok(());
    };
    let values = SchemaRowValues { row: record };
    for invariant in entity.invariants() {
        if required.contains(&invariant.id())
            && !evaluate_predicate(invariant.expressions(), invariant.predicate(), &values)
                .map_err(|_| {
                    MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity.id())
                })?
        {
            return Err(
                MigrationFinding::new(migration_finding_code::INVARIANT_REJECTED)
                    .entity(entity.id()),
            );
        }
    }
    Ok(())
}

fn validate_all_invariants(
    entity: &EntitySchema,
    record: &CanonicalRecord,
) -> Result<(), MigrationFinding> {
    let values = SchemaRowValues { row: record };
    for invariant in entity.invariants() {
        if !evaluate_predicate(invariant.expressions(), invariant.predicate(), &values).map_err(
            |_| MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity.id()),
        )? {
            return Err(
                MigrationFinding::new(migration_finding_code::INVARIANT_REJECTED)
                    .entity(entity.id()),
            );
        }
    }
    Ok(())
}

fn derive_partition(
    plan: &ValidatedMigrationPlan,
    entity: EntityTypeId,
    record: &CanonicalRecord,
) -> Result<riffdb_types::PartitionKey, MigrationFinding> {
    let aggregate = plan
        .candidate
        .bundle()
        .schema()
        .aggregates()
        .iter()
        .find(|aggregate| aggregate.owns(entity))
        .ok_or_else(|| MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity))?;
    let values = SchemaRowValues { row: record };
    let value = evaluate_expression(
        aggregate.keys().expressions(),
        aggregate.keys().partition_expression(),
        &values,
    )
    .map_err(|_| MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity))?;
    aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[value])
        .map_err(|_| MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity))
}

fn derive_indexes(
    plan: &ValidatedMigrationPlan,
    entity: &EntitySchema,
    source: &StoredEntityRecordV1,
    record: &CanonicalRecord,
    partition: &riffdb_types::PartitionKey,
) -> Result<Vec<StoredIndexEntryV2>, MigrationFinding> {
    let Some(required) = plan.rebuilt_indexes.get(&entity.id()) else {
        return Ok(Vec::new());
    };
    let binding = DurableKeySchemaBindingV1::new(
        plan.candidate.lineage().clone(),
        plan.candidate.contract_version(),
        plan.candidate.bundle_hash(),
    );
    required
        .iter()
        .map(|index_id| {
            let index = entity
                .indexes()
                .iter()
                .find(|index| index.id() == *index_id)
                .ok_or_else(|| {
                    MigrationFinding::new(migration_finding_code::INDEX_INVALID)
                        .entity(entity.id())
                        .with_index(*index_id)
                })?;
            let values = index
                .fields()
                .iter()
                .map(|field| {
                    field_value(record, *field).cloned().ok_or_else(|| {
                        MigrationFinding::new(migration_finding_code::INDEX_INVALID)
                            .entity(entity.id())
                            .with_field(*field)
                            .with_index(*index_id)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key = index
                .key_schema()
                .encode_index(&values, source.target().key().clone())
                .map_err(|_| {
                    MigrationFinding::new(migration_finding_code::INDEX_INVALID)
                        .entity(entity.id())
                        .with_index(*index_id)
                })?;
            StoredIndexEntryV2::new(
                key,
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("empty record"),
                partition.clone(),
            )
            .map_err(|_| {
                MigrationFinding::new(migration_finding_code::INDEX_INVALID)
                    .entity(entity.id())
                    .with_index(*index_id)
            })
        })
        .collect()
}

fn derive_relationships(
    plan: &ValidatedMigrationPlan,
    entity: EntityTypeId,
    source: &StoredEntityRecordV1,
    record: &CanonicalRecord,
) -> Result<Vec<MigrationRelationshipFact>, MigrationFinding> {
    plan.candidate
        .bundle()
        .schema()
        .relationships()
        .iter()
        .filter(|relationship| {
            relationship.source_entity() == entity
                && plan
                    .added_relationships
                    .contains(&(entity, relationship.name().to_owned()))
        })
        .map(|relationship| {
            let target_schema = plan
                .candidate
                .bundle()
                .schema()
                .entity(relationship.target_entity())
                .ok_or_else(|| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
            let values = relationship
                .source_fields()
                .iter()
                .map(|field| {
                    field_value(record, *field).cloned().ok_or_else(|| {
                        MigrationFinding::new(migration_finding_code::INVALID_ROW)
                            .entity(entity)
                            .with_field(*field)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key = target_schema
                .primary_key()
                .encode_entity(&values)
                .map_err(|_| {
                    MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity)
                })?;
            let target = EntityTarget::new(relationship.target_entity(), key).map_err(|_| {
                MigrationFinding::new(migration_finding_code::INVALID_ROW).entity(entity)
            })?;
            Ok(MigrationRelationshipFact {
                name: relationship.name().to_owned(),
                source: source.target().clone(),
                target,
            })
        })
        .collect()
}

fn derive_unique(
    plan: &ValidatedMigrationPlan,
    entity: &EntitySchema,
    source: &StoredEntityRecordV1,
    record: &CanonicalRecord,
) -> Result<Vec<MigrationUniqueFact>, MigrationFinding> {
    plan.candidate
        .bundle()
        .schema()
        .unique_keys()
        .iter()
        .filter(|unique| {
            unique.source_entity() == entity.id()
                && plan
                    .added_unique
                    .contains(&(entity.id(), unique.index_id()))
        })
        .map(|unique| {
            let index = entity
                .indexes()
                .iter()
                .find(|index| index.id() == unique.index_id())
                .ok_or_else(|| MigrationFinding::new(migration_finding_code::ARTIFACT_MISMATCH))?;
            let values = unique
                .fields()
                .iter()
                .map(|field| {
                    field_value(record, *field).cloned().ok_or_else(|| {
                        MigrationFinding::new(migration_finding_code::INVALID_ROW)
                            .entity(entity.id())
                            .with_field(*field)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let prefix = index
                .key_schema()
                .encode_index_prefix(&values)
                .map_err(|_| {
                    MigrationFinding::new(migration_finding_code::INDEX_INVALID)
                        .entity(entity.id())
                        .with_index(index.id())
                })?;
            Ok(MigrationUniqueFact {
                entity_type: entity.id(),
                index: index.id(),
                prefix: prefix.as_bytes().to_vec(),
                source: source.target().clone(),
            })
        })
        .collect()
}

fn expression_finding(
    error: EvaluationError,
    entity: EntityTypeId,
    field: FieldId,
) -> MigrationFinding {
    let code = match error {
        EvaluationError::Arithmetic => migration_finding_code::TRANSFORM_ARITHMETIC,
        EvaluationError::Integrity => migration_finding_code::INVALID_ROW,
    };
    MigrationFinding::new(code).entity(entity).with_field(field)
}

fn field_value(record: &CanonicalRecord, field: FieldId) -> Option<&CanonicalValue> {
    record
        .fields()
        .binary_search_by_key(&field, |(candidate, _)| *candidate)
        .ok()
        .map(|index| &record.fields()[index].1)
}

struct SchemaRowValues<'a> {
    row: &'a CanonicalRecord,
}

impl ExpressionValueSource for SchemaRowValues<'_> {
    fn schema_field(&self, _entity_type: EntityTypeId, field: FieldId) -> Option<CanonicalValue> {
        field_value(self.row, field).cloned()
    }
}

#[cfg(test)]
mod conversion_tests {
    use super::*;
    use riffdb_types::{Decimal, DecimalSpec};

    #[test]
    fn checked_integer_conversions_cover_boundaries_without_fallback() {
        for value in [0_i64, 1, i64::MAX] {
            assert_eq!(
                convert_value(
                    MigrationConversionV1::CheckedI64ToU64,
                    CanonicalValue::I64(value),
                    &ValueType::i64(),
                    &ValueType::u64(),
                ),
                Some(CanonicalValue::U64(value as u64))
            );
        }
        assert!(
            convert_value(
                MigrationConversionV1::CheckedI64ToU64,
                CanonicalValue::I64(-1),
                &ValueType::i64(),
                &ValueType::u64(),
            )
            .is_none()
        );
        for value in [0_u64, 1, i64::MAX as u64] {
            assert_eq!(
                convert_value(
                    MigrationConversionV1::CheckedU64ToI64,
                    CanonicalValue::U64(value),
                    &ValueType::u64(),
                    &ValueType::i64(),
                ),
                Some(CanonicalValue::I64(value as i64))
            );
        }
        assert!(
            convert_value(
                MigrationConversionV1::CheckedU64ToI64,
                CanonicalValue::U64(i64::MAX as u64 + 1),
                &ValueType::u64(),
                &ValueType::i64(),
            )
            .is_none()
        );
    }

    #[test]
    fn optional_decimal_and_list_assertions_are_exact() {
        let optional_i64 = ValueType::optional(ValueType::i64()).expect("optional type");
        assert!(
            convert_value(
                MigrationConversionV1::AssertUnwrapOptional,
                CanonicalValue::Null,
                &optional_i64,
                &ValueType::i64(),
            )
            .is_none()
        );
        assert_eq!(
            convert_value(
                MigrationConversionV1::AssertUnwrapOptional,
                CanonicalValue::I64(5),
                &optional_i64,
                &ValueType::i64(),
            ),
            Some(CanonicalValue::I64(5))
        );

        let old_spec = DecimalSpec::new(6, 2).expect("old decimal");
        let new_spec = DecimalSpec::new(5, 1).expect("new decimal");
        let exact = Decimal::new(old_spec, 1_230).expect("exact decimal");
        let inexact = Decimal::new(old_spec, 1_231).expect("inexact decimal");
        assert_eq!(
            convert_value(
                MigrationConversionV1::ExactDecimal,
                CanonicalValue::Decimal(exact),
                &ValueType::decimal(old_spec),
                &ValueType::decimal(new_spec),
            ),
            Some(CanonicalValue::Decimal(
                Decimal::new(new_spec, 123).expect("rescaled")
            ))
        );
        assert!(
            convert_value(
                MigrationConversionV1::ExactDecimal,
                CanonicalValue::Decimal(inexact),
                &ValueType::decimal(old_spec),
                &ValueType::decimal(new_spec),
            )
            .is_none()
        );

        let old_list = ValueType::list(ValueType::i64(), 2).expect("old list");
        let new_list = ValueType::list(ValueType::u64(), 2).expect("new list");
        let values = CanonicalList::new(vec![CanonicalValue::I64(0), CanonicalValue::I64(9)])
            .expect("values");
        assert!(matches!(
            convert_value(
                MigrationConversionV1::ListElements,
                CanonicalValue::List(values),
                &old_list,
                &new_list,
            ),
            Some(CanonicalValue::List(values))
                if values.values() == [CanonicalValue::U64(0), CanonicalValue::U64(9)]
        ));
    }

    #[test]
    fn canonical_uuid_text_round_trips_and_rejects_alternate_spellings() {
        for seed in [0_u8, 1, 0x7f, 0xff] {
            let mut uuid = [0_u8; 16];
            for (index, byte) in uuid.iter_mut().enumerate() {
                *byte = seed.wrapping_add(index as u8);
            }
            let text = convert_value(
                MigrationConversionV1::UuidToString,
                CanonicalValue::Uuid(uuid),
                &ValueType::uuid(),
                &ValueType::string(36).expect("UUID text type"),
            )
            .expect("format UUID");
            assert_eq!(
                convert_value(
                    MigrationConversionV1::StringToUuid,
                    text,
                    &ValueType::string(36).expect("UUID text type"),
                    &ValueType::uuid(),
                ),
                Some(CanonicalValue::Uuid(uuid))
            );
        }
        assert!(
            convert_value(
                MigrationConversionV1::StringToUuid,
                CanonicalValue::string("00112233-4455-6677-8899-AABBCCDDEEFF")
                    .expect("bounded text"),
                &ValueType::string(36).expect("UUID text type"),
                &ValueType::uuid(),
            )
            .is_none()
        );
    }
}
