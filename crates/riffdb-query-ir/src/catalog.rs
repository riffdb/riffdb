use std::collections::BTreeMap;

use riffdb_contract_ir::{
    ContractBundle, ExpressionKind, IndexFieldEncodingV1, KeyComponentSchema, KeyPurpose,
    KeySchema, LineageEntryState, RowPolicyOperationV1, StableIdNamespaceTag, TextKeyProfileV1,
    UNICODE_FOLD_V1_MAXIMUM_EXPANSION, ValueType,
};
use riffdb_types::{EntityTypeId, EnumTypeId, EnumVariantId, FieldId, HashDomain, IndexId, hash};

use crate::{
    QueryDiagnostic, QueryDiagnosticCode, QueryDiagnosticStage, QueryDiagnostics,
    resolver::ExactContractIdentity,
};

/// One exact contract field symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldSymbol {
    id: FieldId,
    name: String,
    value_type: ValueType,
    key: bool,
    secret: bool,
    production_vector: bool,
    present_and_non_null_across_lineage: bool,
}

impl FieldSymbol {
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Complete contract value type.
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Whether this field is a primary-key component.
    #[must_use]
    pub const fn is_key(&self) -> bool {
        self.key
    }

    /// Whether the exact contract classifies this stored field as secret.
    #[must_use]
    pub const fn is_secret(&self) -> bool {
        self.secret
    }

    /// Whether the exact contract declares this vector field production-capable.
    #[must_use]
    pub const fn is_production_vector(&self) -> bool {
        self.production_vector
    }

    /// Whether the supplied complete lineage proves this field always has a value.
    #[must_use]
    pub const fn is_present_and_non_null_across_lineage(&self) -> bool {
        self.present_and_non_null_across_lineage
    }

    /// Compiler-internal stable identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> FieldId {
        self.id
    }
}

/// One exact declared index symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSymbol {
    id: IndexId,
    name: String,
    field_ids: Vec<FieldId>,
    fields: Vec<String>,
    cover_fields: Vec<String>,
    encodings: Vec<IndexFieldEncodingV1>,
    key_schema: KeySchema,
}

/// One compiler-sealed logical-to-physical component mapping.
///
/// This is process-local operational evidence. It is deliberately absent from
/// canonical query encoding and has no public constructor.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalIndexComponentV1 {
    field_id: FieldId,
    field: String,
    encoding: IndexFieldEncodingV1,
    physical_start: usize,
    physical: Vec<KeyComponentSchema>,
}

impl OperationalIndexComponentV1 {
    /// Stable field identity selected from the exact bundle.
    #[doc(hidden)]
    #[must_use]
    pub const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Exact bundle-declared physical encoding for one logical field.
    #[doc(hidden)]
    #[must_use]
    pub const fn encoding(&self) -> IndexFieldEncodingV1 {
        self.encoding
    }

    /// First physical component occupied by this logical field.
    #[doc(hidden)]
    #[must_use]
    pub const fn physical_start(&self) -> usize {
        self.physical_start
    }

    /// Complete checked physical components occupied by this logical field.
    #[doc(hidden)]
    #[must_use]
    pub fn physical(&self) -> &[KeyComponentSchema] {
        &self.physical
    }
}

/// Bounded private reconstruction of one exact ordinary index declaration.
///
/// Stable IDs select the declaration. Names, field order, and all retained
/// schemas are agreement checks. The type is never canonically serialized.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalIndexDescriptorV1 {
    contract: ExactContractIdentity,
    entity_id: EntityTypeId,
    entity: String,
    index_id: IndexId,
    index: String,
    components: Vec<OperationalIndexComponentV1>,
    partition_key_schema: KeySchema,
    entity_key_schema: KeySchema,
    index_key_schema: KeySchema,
}

impl OperationalIndexDescriptorV1 {
    /// Exact contract identity from which this descriptor was reconstructed.
    #[doc(hidden)]
    #[must_use]
    pub const fn contract(&self) -> &ExactContractIdentity {
        &self.contract
    }

    /// Returns one exact logical component only when its symbolic field agrees.
    #[doc(hidden)]
    #[must_use]
    pub fn component(
        &self,
        logical_position: usize,
        field: &str,
    ) -> Option<&OperationalIndexComponentV1> {
        self.components
            .get(logical_position)
            .filter(|component| component.field == field)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn matches(
        &self,
        contract: &ExactContractIdentity,
        entity_id: EntityTypeId,
        entity: &str,
        index_id: IndexId,
        index: &str,
        fields: &[String],
        partition_key_schema: &KeySchema,
        entity_key_schema: &KeySchema,
        index_key_schema: &KeySchema,
    ) -> bool {
        let mut next_physical = 0usize;
        let components_match = self.components.len() == fields.len()
            && self
                .components
                .iter()
                .zip(fields)
                .all(|(component, field)| {
                    let end = next_physical.checked_add(component.physical.len());
                    let matches = !component.physical.is_empty()
                        && component.field == *field
                        && component.physical_start == next_physical
                        && end
                            .and_then(|end| index_key_schema.components().get(next_physical..end))
                            == Some(component.physical.as_slice());
                    if let Some(end) = end {
                        next_physical = end;
                    }
                    matches
                })
            && next_physical == index_key_schema.components().len();
        self.contract == *contract
            && self.entity_id == entity_id
            && self.entity == entity
            && self.index_id == index_id
            && self.index == index
            && self.partition_key_schema == *partition_key_schema
            && self.entity_key_schema == *entity_key_schema
            && self.index_key_schema == *index_key_schema
            && components_match
    }
}

/// One exact required relationship symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipSymbol {
    name: String,
    source_fields: Vec<String>,
    target_entity: String,
    target_fields: Vec<String>,
}

impl RelationshipSymbol {
    /// Exact relationship source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stored source fields in target-key order.
    #[must_use]
    pub fn source_fields(&self) -> &[String] {
        &self.source_fields
    }
    /// Exact target entity source name.
    #[must_use]
    pub fn target_entity(&self) -> &str {
        &self.target_entity
    }
    /// Complete target primary-key field names.
    #[must_use]
    pub fn target_fields(&self) -> &[String] {
        &self.target_fields
    }
}

impl IndexSymbol {
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Component field names in declared order.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Compiler-owned covered field names in declared order.
    #[must_use]
    pub fn cover_fields(&self) -> &[String] {
        &self.cover_fields
    }

    /// Compiler-owned encoding for each logical field.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_encodings(&self) -> &[IndexFieldEncodingV1] {
        &self.encodings
    }

    /// Compiler-internal stable identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> IndexId {
        self.id
    }

    /// Compiler-internal complete index key schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_key_schema(&self) -> &KeySchema {
        &self.key_schema
    }
}

/// One exact entity symbol with deterministic name lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntitySymbol {
    id: EntityTypeId,
    name: String,
    fields: BTreeMap<String, FieldSymbol>,
    indexes: BTreeMap<String, IndexSymbol>,
    text_indexes: BTreeMap<String, TextIndexSymbol>,
    long_pattern_indexes: BTreeMap<String, LongPatternSymbol>,
    relationships: BTreeMap<String, RelationshipSymbol>,
    primary_key: Vec<String>,
    partition_field: String,
    partition_key_schema: KeySchema,
    primary_key_schema: KeySchema,
}

impl EntitySymbol {
    /// Exact entity source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Resolves an exact field name.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&FieldSymbol> {
        self.fields.get(name)
    }

    /// Fields in exact name order.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &FieldSymbol> {
        self.fields.values()
    }

    /// Resolves an exact index name.
    #[must_use]
    pub fn index(&self, name: &str) -> Option<&IndexSymbol> {
        self.indexes.get(name)
    }

    /// Indexes in exact name order.
    #[must_use]
    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &IndexSymbol> {
        self.indexes.values()
    }

    /// Resolves one compiler-owned tokenized text index by source name.
    #[must_use]
    pub fn text_index(&self, name: &str) -> Option<&TextIndexSymbol> {
        self.text_indexes.get(name)
    }

    /// Tokenized text indexes in exact source-name order.
    #[must_use]
    pub fn text_indexes(&self) -> impl ExactSizeIterator<Item = &TextIndexSymbol> {
        self.text_indexes.values()
    }

    /// Resolves one compiler-owned exact long-pattern provider by source name.
    #[must_use]
    pub fn long_pattern_index(&self, name: &str) -> Option<&LongPatternSymbol> {
        self.long_pattern_indexes.get(name)
    }

    /// Exact long-pattern providers in source-name order.
    #[must_use]
    pub fn long_pattern_indexes(&self) -> impl ExactSizeIterator<Item = &LongPatternSymbol> {
        self.long_pattern_indexes.values()
    }

    /// Resolves an exact required relationship name.
    #[must_use]
    pub fn relationship(&self, name: &str) -> Option<&RelationshipSymbol> {
        self.relationships.get(name)
    }

    /// Required relationships in exact name order.
    #[must_use]
    pub fn relationships(&self) -> impl ExactSizeIterator<Item = &RelationshipSymbol> {
        self.relationships.values()
    }

    /// Primary-key component names in declared key order.
    #[must_use]
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Field whose exact value derives this entity's aggregate partition.
    #[must_use]
    pub fn partition_field(&self) -> &str {
        &self.partition_field
    }

    /// Compiler-internal aggregate partition-key schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_partition_key_schema(&self) -> &KeySchema {
        &self.partition_key_schema
    }

    /// Compiler-internal complete entity key schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_primary_key_schema(&self) -> &KeySchema {
        &self.primary_key_schema
    }

    /// Compiler-internal stable identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> EntityTypeId {
        self.id
    }
}

/// Safe compiler-visible tokenized text-index declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextIndexSymbol {
    id: riffdb_types::IndexId,
    identity: [u8; 32],
    name: String,
    analyzer: riffdb_types::TextAnalyzerV1,
    source_fields: Vec<(riffdb_types::FieldId, u16)>,
    max_terms: u32,
    max_candidates: u32,
    max_results: u32,
}

impl TextIndexSymbol {
    /// Exact contract source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stable contract identity.
    #[must_use]
    pub const fn id(&self) -> riffdb_types::IndexId {
        self.id
    }

    /// Contract-derived immutable index identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Frozen analyzer.
    #[must_use]
    pub const fn analyzer(&self) -> riffdb_types::TextAnalyzerV1 {
        self.analyzer
    }

    /// Canonically ordered weighted source fields.
    #[must_use]
    pub fn source_fields(&self) -> &[(riffdb_types::FieldId, u16)] {
        &self.source_fields
    }

    /// Maximum analyzed query terms.
    #[must_use]
    pub const fn max_terms(&self) -> u32 {
        self.max_terms
    }

    /// Maximum candidate documents.
    #[must_use]
    pub const fn max_candidates(&self) -> u32 {
        self.max_candidates
    }

    /// Maximum returned documents.
    #[must_use]
    pub const fn max_results(&self) -> u32 {
        self.max_results
    }
}

/// Safe compiler-visible exact long-pattern provider declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternSymbol {
    id: IndexId,
    state_schema_hash: [u8; 32],
    name: String,
    field: String,
    field_id: FieldId,
    profile: riffdb_types::LongPatternProfileV1,
    operators: Vec<riffdb_types::LongPatternOperatorV1>,
    bounds: riffdb_types::LongPatternBoundsV1,
    replay_age_seconds: u64,
    retained_generations: u32,
    staleness_slo: u32,
}

impl LongPatternSymbol {
    /// Exact contract source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable index identity.
    #[must_use]
    pub const fn id(&self) -> IndexId {
        self.id
    }
    /// Exact matched source field.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
    /// Stable matched field identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field_id(&self) -> FieldId {
        self.field_id
    }
    /// Frozen matching profile.
    #[must_use]
    pub const fn profile(&self) -> riffdb_types::LongPatternProfileV1 {
        self.profile
    }
    /// Closed declared operator family.
    #[must_use]
    pub fn operators(&self) -> &[riffdb_types::LongPatternOperatorV1] {
        &self.operators
    }
    /// Complete provider bounds.
    #[must_use]
    pub const fn bounds(&self) -> riffdb_types::LongPatternBoundsV1 {
        self.bounds
    }
    /// Immutable provider-state schema identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn state_schema_hash(&self) -> [u8; 32] {
        self.state_schema_hash
    }
    /// Minimum retained provider generations.
    #[must_use]
    pub const fn retained_generations(&self) -> u32 {
        self.retained_generations
    }
    /// Maximum deterministic retained epoch lease.
    #[must_use]
    pub const fn replay_age_seconds(&self) -> u64 {
        self.replay_age_seconds
    }
    /// Compiler-declared staleness ceiling.
    #[must_use]
    pub const fn staleness_slo(&self) -> u32 {
        self.staleness_slo
    }
}

/// One exact enum symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumSymbol {
    id: EnumTypeId,
    name: String,
    variants: BTreeMap<String, EnumVariantId>,
}

/// Safe symbolic row-policy description. Executable bytecode remains private.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicySymbol {
    name: String,
    entity: String,
    operations: Vec<RowPolicyOperationV1>,
}

impl RowPolicySymbol {
    /// Symbolic policy name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Symbolic protected entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Closed operation classes defined by this policy.
    #[must_use]
    pub fn operations(&self) -> &[RowPolicyOperationV1] {
        &self.operations
    }
}

/// Safe compiler-visible principal-fact schema without a capability value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalFactSymbol {
    name: String,
    value_type: String,
}

impl PrincipalFactSymbol {
    /// Symbolic fact name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Symbolic public type spelling without stable numeric IDs.
    #[must_use]
    pub fn value_type(&self) -> &str {
        &self.value_type
    }
}

impl EnumSymbol {
    /// Exact enum source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Resolves an exact variant source name.
    #[must_use]
    pub fn variant(&self, name: &str) -> Option<EnumVariantId> {
        self.variants.get(name).copied()
    }

    /// Variant names in exact name order.
    #[must_use]
    pub fn variants(&self) -> impl ExactSizeIterator<Item = &str> {
        self.variants.keys().map(String::as_str)
    }

    /// Compiler-internal stable identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_id(&self) -> EnumTypeId {
        self.id
    }
}

/// Immutable name-addressed view of one exact validated contract bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicCatalog {
    identity: ExactContractIdentity,
    entities: BTreeMap<String, EntitySymbol>,
    enums: BTreeMap<String, EnumSymbol>,
    row_policies: BTreeMap<String, RowPolicySymbol>,
    principal_facts: BTreeMap<String, PrincipalFactSymbol>,
}

impl SymbolicCatalog {
    /// Builds a deterministic catalog from an already checked exact bundle.
    pub fn from_bundle(bundle: &ContractBundle) -> Result<Self, QueryDiagnostics> {
        let mut entities = BTreeMap::new();
        for entity in bundle.schema().entities() {
            let aggregate = bundle
                .schema()
                .aggregates()
                .iter()
                .find(|aggregate| aggregate.owns(entity.id()))
                .ok_or_else(|| invariant("entity has no aggregate owner"))?;
            let partition_node = aggregate
                .keys()
                .expressions()
                .get(aggregate.keys().partition_expression())
                .ok_or_else(|| invariant("aggregate partition expression is absent"))?;
            let (partition_entity_id, partition_field_id) = match partition_node.kind() {
                ExpressionKind::SchemaField { entity_type, field } => (*entity_type, *field),
                _ => {
                    return Err(invariant(
                        "RiffQL v1 requires a direct aggregate partition field",
                    ));
                }
            };
            let partition_entity = bundle
                .schema()
                .entity(partition_entity_id)
                .ok_or_else(|| invariant("partition expression entity is absent"))?;
            let partition_name = partition_entity
                .record()
                .field(partition_field_id)
                .map(|field| field.name().to_owned())
                .ok_or_else(|| invariant("partition expression field is absent"))?;
            if !entity
                .record()
                .fields()
                .iter()
                .any(|field| field.name() == partition_name)
            {
                return Err(invariant(
                    "owned entity lacks the aggregate partition field",
                ));
            }
            let key_ids = entity.primary_key_fields();
            let mut fields = BTreeMap::new();
            for field in entity.record().fields() {
                let symbol = FieldSymbol {
                    id: field.id(),
                    name: field.name().to_owned(),
                    value_type: field.value_type().clone(),
                    key: key_ids.contains(&field.id()),
                    secret: bundle.schema().is_secret_field(entity.id(), field.id()),
                    production_vector: bundle
                        .schema()
                        .vector_production_spec(entity.id(), field.id())
                        .is_some(),
                    present_and_non_null_across_lineage: !field.value_type().is_optional()
                        && bundle.contract_version().get() == 1,
                };
                if fields.insert(symbol.name.clone(), symbol).is_some() {
                    return Err(invariant("duplicate exact-contract field"));
                }
            }
            let mut indexes = BTreeMap::new();
            for index in entity.indexes() {
                let component_names = index
                    .fields()
                    .iter()
                    .map(|id| {
                        entity
                            .record()
                            .field(*id)
                            .map(|field| field.name().to_owned())
                            .ok_or_else(|| invariant("index references an absent field"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let cover_names = index
                    .cover_fields()
                    .iter()
                    .map(|id| {
                        entity
                            .record()
                            .field(*id)
                            .map(|field| field.name().to_owned())
                            .ok_or_else(|| invariant("index cover references an absent field"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let symbol = IndexSymbol {
                    id: index.id(),
                    name: index.name().to_owned(),
                    field_ids: index.fields().to_vec(),
                    fields: component_names,
                    cover_fields: cover_names,
                    encodings: index.encodings().to_vec(),
                    key_schema: index.key_schema().clone(),
                };
                if indexes.insert(symbol.name.clone(), symbol).is_some() {
                    return Err(invariant("duplicate exact-contract index"));
                }
            }
            let mut text_indexes = BTreeMap::new();
            for spec in bundle
                .schema()
                .text_index_specs()
                .iter()
                .filter(|spec| spec.entity() == entity.id())
            {
                let name = bundle
                    .ledger()
                    .allocations()
                    .iter()
                    .flat_map(|allocation| allocation.entries())
                    .find(|entry| {
                        entry.identity().namespace().tag() == StableIdNamespaceTag::Index
                            && entry.identity().namespace().owner_ids() == [entity.id().get()]
                            && {
                                entry.id() == spec.index().get()
                                    && entry.state() == LineageEntryState::Active
                            }
                    })
                    .map(|entry| entry.name().to_owned())
                    .ok_or_else(|| invariant("text index has no active lineage name"))?;
                let symbol = TextIndexSymbol {
                    id: spec.index(),
                    identity: {
                        let mut preimage = Vec::with_capacity(44);
                        preimage.extend_from_slice(b"RIFFDB-TEXT-INDEX-V1\0");
                        preimage.extend_from_slice(bundle.bundle_hash().as_bytes());
                        preimage.extend_from_slice(&entity.id().to_be_bytes());
                        preimage.extend_from_slice(&spec.index().to_be_bytes());
                        *hash(HashDomain::ProjectionPlan, &preimage).as_bytes()
                    },
                    name: name.clone(),
                    analyzer: spec.analyzer(),
                    source_fields: spec
                        .source_fields()
                        .iter()
                        .map(|source| (source.field(), source.weight()))
                        .collect(),
                    max_terms: spec.max_terms(),
                    max_candidates: spec.max_candidates(),
                    max_results: spec.max_results(),
                };
                if text_indexes.insert(name, symbol).is_some() {
                    return Err(invariant("duplicate tokenized text index"));
                }
            }
            let mut long_pattern_indexes = BTreeMap::new();
            for spec in bundle
                .schema()
                .long_pattern_specs()
                .iter()
                .filter(|spec| spec.entity() == entity.id())
            {
                let name = spec.name().to_owned();
                let field = entity
                    .record()
                    .field(spec.field())
                    .ok_or_else(|| invariant("long-pattern provider field is absent"))?;
                let state_schema_hash = {
                    let mut preimage = Vec::with_capacity(96);
                    preimage.extend_from_slice(b"RIFFDB-LONG-PATTERN-STATE-V1\0");
                    preimage.extend_from_slice(bundle.bundle_hash().as_bytes());
                    preimage.extend_from_slice(&entity.id().to_be_bytes());
                    preimage.extend_from_slice(&spec.index().to_be_bytes());
                    *hash(HashDomain::ProjectionPlan, &preimage).as_bytes()
                };
                let symbol = LongPatternSymbol {
                    id: spec.index(),
                    state_schema_hash,
                    name: name.clone(),
                    field: field.name().to_owned(),
                    field_id: spec.field(),
                    profile: spec.profile(),
                    operators: spec.operators().to_vec(),
                    bounds: spec.bounds(),
                    replay_age_seconds: spec.replay_age_seconds(),
                    retained_generations: spec.retained_generations(),
                    staleness_slo: spec.stale_entity_count_threshold(),
                };
                if long_pattern_indexes.insert(name, symbol).is_some() {
                    return Err(invariant("duplicate exact long-pattern provider"));
                }
            }
            let mut relationships = BTreeMap::new();
            for relationship in bundle
                .schema()
                .relationships()
                .iter()
                .filter(|relationship| relationship.source_entity() == entity.id())
            {
                let target = bundle
                    .schema()
                    .entity(relationship.target_entity())
                    .ok_or_else(|| invariant("relationship target entity is absent"))?;
                let symbol = RelationshipSymbol {
                    name: relationship.name().to_owned(),
                    source_fields: relationship
                        .source_fields()
                        .iter()
                        .map(|id| {
                            entity
                                .record()
                                .field(*id)
                                .map(|field| field.name().to_owned())
                                .ok_or_else(|| invariant("relationship source field is absent"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    target_entity: target.name().to_owned(),
                    target_fields: relationship
                        .target_fields()
                        .iter()
                        .map(|id| {
                            target
                                .record()
                                .field(*id)
                                .map(|field| field.name().to_owned())
                                .ok_or_else(|| invariant("relationship target field is absent"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                };
                if relationships.insert(symbol.name.clone(), symbol).is_some() {
                    return Err(invariant("duplicate exact-contract relationship"));
                }
            }
            let symbol = EntitySymbol {
                id: entity.id(),
                name: entity.name().to_owned(),
                fields,
                indexes,
                text_indexes,
                long_pattern_indexes,
                relationships,
                primary_key: key_ids
                    .iter()
                    .map(|id| {
                        entity
                            .record()
                            .field(*id)
                            .map(|field| field.name().to_owned())
                            .ok_or_else(|| invariant("primary key references an absent field"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                partition_field: partition_name,
                partition_key_schema: aggregate.keys().partition_schema().clone(),
                primary_key_schema: entity.primary_key().clone(),
            };
            if entities.insert(symbol.name.clone(), symbol).is_some() {
                return Err(invariant("duplicate exact-contract entity"));
            }
        }
        let mut enums = BTreeMap::new();
        for enumeration in bundle.schema().enums() {
            let variants = enumeration
                .variants()
                .iter()
                .map(|variant| (variant.name().to_owned(), variant.id()))
                .collect::<BTreeMap<_, _>>();
            if variants.len() != enumeration.variants().len() {
                return Err(invariant("duplicate exact-contract enum variant"));
            }
            let symbol = EnumSymbol {
                id: enumeration.id(),
                name: enumeration.name().to_owned(),
                variants,
            };
            if enums.insert(symbol.name.clone(), symbol).is_some() {
                return Err(invariant("duplicate exact-contract enum"));
            }
        }
        let mut row_policies = BTreeMap::new();
        for policy in bundle.row_policies().policies() {
            let entity = bundle
                .schema()
                .entity(policy.entity())
                .ok_or_else(|| invariant("row policy entity is absent"))?;
            let symbol = RowPolicySymbol {
                name: policy.name().to_owned(),
                entity: entity.name().to_owned(),
                operations: policy.rules().iter().map(|rule| rule.operation()).collect(),
            };
            if row_policies.insert(symbol.name.clone(), symbol).is_some() {
                return Err(invariant("duplicate exact-contract row policy"));
            }
        }
        let mut principal_facts = BTreeMap::new();
        for fact in bundle.row_policies().facts() {
            let symbol = PrincipalFactSymbol {
                name: fact.name().to_owned(),
                value_type: fact
                    .public_type_name(bundle.schema())
                    .map_err(|_| invariant("principal fact type is invalid"))?,
            };
            if principal_facts
                .insert(symbol.name.clone(), symbol)
                .is_some()
            {
                return Err(invariant("duplicate exact-contract principal fact"));
            }
        }
        Ok(Self {
            identity: ExactContractIdentity::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            entities,
            enums,
            row_policies,
            principal_facts,
        })
    }

    /// Builds a catalog whose field-presence proof covers a complete ordered lineage.
    pub fn from_lineage(bundles: &[ContractBundle]) -> Result<Self, QueryDiagnostics> {
        let current = bundles
            .last()
            .ok_or_else(|| invariant("contract lineage is empty"))?;
        if bundles
            .iter()
            .any(|bundle| bundle.lineage() != current.lineage())
        {
            return Err(invariant("contract lineage contains a foreign member"));
        }
        if bundles
            .first()
            .map(ContractBundle::contract_version)
            .map(|version| version.get())
            != Some(1)
            || bundles.windows(2).any(|pair| {
                pair[1].contract_version().get()
                    != pair[0].contract_version().get().saturating_add(1)
            })
        {
            return Err(invariant(
                "contract lineage must contain every version in ascending order",
            ));
        }
        let mut catalog = Self::from_bundle(current)?;
        for entity in catalog.entities.values_mut() {
            for field in entity.fields.values_mut() {
                field.present_and_non_null_across_lineage = !field.value_type.is_optional()
                    && bundles
                        .iter()
                        .filter_map(|bundle| {
                            bundle
                                .schema()
                                .entities()
                                .iter()
                                .find(|candidate| candidate.name() == entity.name)
                        })
                        .all(|historical_entity| {
                            historical_entity
                                .record()
                                .fields()
                                .iter()
                                .find(|candidate| candidate.name() == field.name)
                                .is_some_and(|historical| !historical.value_type().is_optional())
                        });
            }
        }
        Ok(catalog)
    }

    /// Exact immutable contract identity.
    #[must_use]
    pub const fn identity(&self) -> &ExactContractIdentity {
        &self.identity
    }

    /// Reconstructs one private operational descriptor by stable IDs.
    ///
    /// The source bundle was fully validated before this catalog existed. This
    /// second bounded walk recovers its already-sealed component profile and
    /// verifies the complete logical/physical mapping before a query step can
    /// be admitted.
    #[doc(hidden)]
    pub fn internal_operational_index_descriptor(
        &self,
        entity_id: EntityTypeId,
        index_id: IndexId,
    ) -> Option<OperationalIndexDescriptorV1> {
        let entity = self
            .entities
            .values()
            .find(|candidate| candidate.id == entity_id)?;
        let index = entity
            .indexes
            .values()
            .find(|candidate| candidate.id == index_id)?;
        if index.field_ids.len() != index.fields.len()
            || index.encodings.len() != index.fields.len()
            || index.key_schema.purpose()
                != (KeyPurpose::Index {
                    index_id,
                    entity_type: entity_id,
                })
            || index.key_schema.entity_key_schema() != Some(&entity.primary_key_schema)
        {
            return None;
        }

        let mut physical_start = 0usize;
        let mut components = Vec::with_capacity(index.fields.len());
        for ((field_id, field_name), encoding) in index
            .field_ids
            .iter()
            .zip(&index.fields)
            .zip(&index.encodings)
        {
            let field = entity.fields.get(field_name)?;
            if field.id != *field_id {
                return None;
            }
            let expected = expected_operational_components(self, &field.value_type, *encoding)?;
            let physical_end = physical_start.checked_add(expected.len())?;
            let physical = index
                .key_schema
                .components()
                .get(physical_start..physical_end)?;
            if physical != expected.as_slice() {
                return None;
            }
            components.push(OperationalIndexComponentV1 {
                field_id: *field_id,
                field: field_name.clone(),
                encoding: *encoding,
                physical_start,
                physical: physical.to_vec(),
            });
            physical_start = physical_end;
        }
        if physical_start != index.key_schema.components().len() {
            return None;
        }

        Some(OperationalIndexDescriptorV1 {
            contract: self.identity.clone(),
            entity_id,
            entity: entity.name.clone(),
            index_id,
            index: index.name.clone(),
            components,
            partition_key_schema: entity.partition_key_schema.clone(),
            entity_key_schema: entity.primary_key_schema.clone(),
            index_key_schema: index.key_schema.clone(),
        })
    }

    /// Creates a compiler-private catalog view in which one tokenized index
    /// supplies only the partition-plus-primary-key metadata access witness.
    ///
    /// The synthetic ordinary entry is never returned by `from_bundle` and is
    /// never an executable fallback. It lets a provider-owned named operation
    /// reuse the shared schema, authorization, partition, output, and cost
    /// lowering without making tokenized indexes available to ordinary RiffQL.
    #[doc(hidden)]
    pub fn with_tokenized_metadata_index(
        &self,
        entity_name: &str,
        index_name: &str,
    ) -> Option<Self> {
        let mut catalog = self.clone();
        let entity = catalog.entities.get_mut(entity_name)?;
        let text = entity.text_indexes.get(index_name)?;
        if entity.indexes.contains_key(index_name) {
            return None;
        }
        let fields = entity.primary_key.clone();
        let symbol = IndexSymbol {
            id: text.id,
            name: text.name.clone(),
            field_ids: fields
                .iter()
                .map(|name| entity.fields.get(name).map(|field| field.id))
                .collect::<Option<Vec<_>>>()?,
            encodings: vec![IndexFieldEncodingV1::Canonical; fields.len()],
            fields,
            cover_fields: Vec::new(),
            key_schema: entity.primary_key_schema.clone(),
        };
        entity.indexes.insert(index_name.to_owned(), symbol);
        Some(catalog)
    }

    /// Resolves an exact entity source name.
    #[must_use]
    pub fn entity(&self, name: &str) -> Option<&EntitySymbol> {
        self.entities.get(name)
    }

    /// Entities in exact source-name order.
    #[must_use]
    pub fn entities(&self) -> impl ExactSizeIterator<Item = &EntitySymbol> {
        self.entities.values()
    }

    /// Resolves an exact enum source name.
    #[must_use]
    pub fn enumeration(&self, name: &str) -> Option<&EnumSymbol> {
        self.enums.get(name)
    }

    /// Enums in exact source-name order.
    #[must_use]
    pub fn enums(&self) -> impl ExactSizeIterator<Item = &EnumSymbol> {
        self.enums.values()
    }

    /// Resolves one visible symbolic row-policy name.
    #[must_use]
    pub fn row_policy(&self, name: &str) -> Option<&RowPolicySymbol> {
        self.row_policies.get(name)
    }

    /// Safe row-policy descriptions in exact source-name order.
    #[must_use]
    pub fn row_policies(&self) -> impl ExactSizeIterator<Item = &RowPolicySymbol> {
        self.row_policies.values()
    }

    /// Whether an exact entity has a compiled read policy.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_has_read_policy(&self, entity: &str) -> bool {
        self.row_policies.values().any(|policy| {
            policy.entity == entity && policy.operations.contains(&RowPolicyOperationV1::Read)
        })
    }

    /// Resolves one compiler-visible principal-fact schema.
    #[must_use]
    pub fn principal_fact(&self, name: &str) -> Option<&PrincipalFactSymbol> {
        self.principal_facts.get(name)
    }

    /// Principal-fact schemas in exact source-name order. Values never enter the catalog.
    #[must_use]
    pub fn principal_facts(&self) -> impl ExactSizeIterator<Item = &PrincipalFactSymbol> {
        self.principal_facts.values()
    }

    pub(crate) fn enum_name(&self, id: EnumTypeId) -> Option<&str> {
        self.enums
            .values()
            .find(|enumeration| enumeration.id == id)
            .map(EnumSymbol::name)
    }
}

fn expected_operational_components(
    catalog: &SymbolicCatalog,
    logical: &ValueType,
    encoding: IndexFieldEncodingV1,
) -> Option<Vec<KeyComponentSchema>> {
    let canonical = |value_type: ValueType| {
        let enum_variants = if let Some(id) = value_type.enum_type_id() {
            catalog
                .enums
                .values()
                .find(|enumeration| enumeration.id == id)?
                .variants
                .values()
                .copied()
                .collect()
        } else {
            Vec::new()
        };
        KeyComponentSchema::new(value_type, enum_variants).ok()
    };
    match encoding {
        IndexFieldEncodingV1::Canonical => canonical(logical.clone()).map(|value| vec![value]),
        IndexFieldEncodingV1::Presence => {
            let inner = logical
                .optional_inner()
                .filter(|inner| inner.is_authoritative_key_scalar())?;
            Some(vec![
                canonical(ValueType::u64())?,
                canonical(inner.clone())?,
            ])
        }
        IndexFieldEncodingV1::TextKey(profile) => {
            let source_maximum = logical
                .byte_bound()
                .filter(|_| logical.tag() == riffdb_contract_ir::ValueTypeTag::String)?;
            let physical_maximum = match profile {
                TextKeyProfileV1::BinaryUtf8 => source_maximum,
                TextKeyProfileV1::UnicodeFold => {
                    source_maximum.checked_mul(UNICODE_FOLD_V1_MAXIMUM_EXPANSION)?
                }
            };
            KeyComponentSchema::ordered_bytes(physical_maximum)
                .ok()
                .map(|value| vec![value])
        }
    }
}

fn invariant(summary: &'static str) -> QueryDiagnostics {
    QueryDiagnostics::one(QueryDiagnostic::new(
        QueryDiagnosticCode::ArtifactLimit,
        QueryDiagnosticStage::Schema,
        riffdb_riffql_syntax::Span { start: 0, end: 0 },
        Vec::new(),
        summary,
        None,
    ))
}

#[cfg(test)]
mod operational_descriptor_tests {
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{
        IndexFieldEncodingV1, KeyComponentSchema, KeySchema, TextKeyProfileV1, ValueType,
    };
    use riffdb_types::{EntityTypeId, IndexId};

    use super::{OperationalIndexDescriptorV1, SymbolicCatalog};

    const CONTRACT: &str = r#"
contract OperationalDescriptorContract version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<8>
    index by_title (organization_id, title, document_id) text_key(title, unicode_fold_v1)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

    fn catalog_from(source: &str) -> SymbolicCatalog {
        let bundle = compile_contract_source(source).expect("contract");
        SymbolicCatalog::from_bundle(&bundle).expect("catalog")
    }

    fn descriptor_matches(
        catalog: &SymbolicCatalog,
        descriptor: &OperationalIndexDescriptorV1,
    ) -> bool {
        let entity = catalog.entity("Document").expect("entity");
        let index = entity.index("by_title").expect("index");
        descriptor.matches(
            catalog.identity(),
            entity.internal_id(),
            entity.name(),
            index.internal_id(),
            index.name(),
            index.fields(),
            entity.internal_partition_key_schema(),
            entity.internal_primary_key_schema(),
            index.internal_key_schema(),
        )
    }

    // req: OQ-032, OQ-033, OQ-034, OQ-037, OQ-038, OQ-043
    #[test]
    fn operational_descriptor_refuses_mismatched_bundle_index_or_schema_before_storage() {
        let catalog = catalog_from(CONTRACT);
        let entity = catalog.entity("Document").expect("entity");
        let index = entity.index("by_title").expect("index");
        let descriptor = catalog
            .internal_operational_index_descriptor(entity.internal_id(), index.internal_id())
            .expect("descriptor");
        assert!(descriptor_matches(&catalog, &descriptor));

        let foreign = catalog_from(&CONTRACT.replace(
            "OperationalDescriptorContract",
            "ForeignOperationalDescriptorContract",
        ));
        assert!(!descriptor.matches(
            foreign.identity(),
            entity.internal_id(),
            entity.name(),
            index.internal_id(),
            index.name(),
            index.fields(),
            entity.internal_partition_key_schema(),
            entity.internal_primary_key_schema(),
            index.internal_key_schema(),
        ));
        assert!(!descriptor.matches(
            catalog.identity(),
            EntityTypeId::new(entity.internal_id().get() + 1).expect("foreign entity ID"),
            entity.name(),
            index.internal_id(),
            index.name(),
            index.fields(),
            entity.internal_partition_key_schema(),
            entity.internal_primary_key_schema(),
            index.internal_key_schema(),
        ));
        assert!(!descriptor.matches(
            catalog.identity(),
            entity.internal_id(),
            "WrongDocument",
            IndexId::new(index.internal_id().get() + 1).expect("foreign index ID"),
            "wrong_index",
            &[
                "organization_id".to_owned(),
                "document_id".to_owned(),
                "title".to_owned(),
            ],
            entity.internal_primary_key_schema(),
            entity.internal_partition_key_schema(),
            entity.internal_primary_key_schema(),
        ));

        let entity_id = entity.internal_id();
        let index_id = index.internal_id();
        let entity_key = entity.internal_primary_key_schema().clone();
        assert!(
            catalog
                .internal_operational_index_descriptor(
                    EntityTypeId::new(entity_id.get() + 1).expect("missing entity"),
                    index_id,
                )
                .is_none()
        );
        assert!(
            catalog
                .internal_operational_index_descriptor(
                    entity_id,
                    IndexId::new(index_id.get() + 1).expect("missing index"),
                )
                .is_none()
        );

        let mut wrong_encoding = catalog.clone();
        wrong_encoding
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index")
            .encodings[1] = IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8);
        assert!(
            wrong_encoding
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_order = catalog.clone();
        let wrong_order_index = wrong_order
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index");
        wrong_order_index.fields.swap(1, 2);
        assert!(
            wrong_order
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_field_identity = catalog.clone();
        wrong_field_identity
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index")
            .field_ids
            .swap(1, 2);
        assert!(
            wrong_field_identity
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_arity = catalog.clone();
        let wrong_arity_index = wrong_arity
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index");
        let mut too_many = wrong_arity_index.key_schema.components().to_vec();
        too_many.push(KeyComponentSchema::new(ValueType::u64(), Vec::new()).expect("component"));
        wrong_arity_index.key_schema =
            KeySchema::index(index_id, entity_id, too_many, entity_key.clone())
                .expect("wrong-arity schema remains structurally valid");
        assert!(
            wrong_arity
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_codec = catalog.clone();
        let wrong_codec_index = wrong_codec
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index");
        let mut wrong_components = wrong_codec_index.key_schema.components().to_vec();
        wrong_components[1] =
            KeyComponentSchema::new(ValueType::bytes(8).expect("bytes"), Vec::new())
                .expect("canonical bytes component");
        wrong_codec_index.key_schema =
            KeySchema::index(index_id, entity_id, wrong_components, entity_key.clone())
                .expect("wrong-codec schema remains structurally valid");
        assert!(
            wrong_codec
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_bound = catalog.clone();
        let wrong_bound_index = wrong_bound
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index");
        let mut wrong_components = wrong_bound_index.key_schema.components().to_vec();
        wrong_components[1] = KeyComponentSchema::ordered_bytes(8).expect("narrow ordered bytes");
        wrong_bound_index.key_schema =
            KeySchema::index(index_id, entity_id, wrong_components, entity_key.clone())
                .expect("wrong-bound schema remains structurally valid");
        assert!(
            wrong_bound
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );

        let mut wrong_owner = catalog.clone();
        let wrong_owner_index = wrong_owner
            .entities
            .get_mut("Document")
            .expect("entity")
            .indexes
            .get_mut("by_title")
            .expect("index");
        wrong_owner_index.key_schema = KeySchema::index(
            IndexId::new(index_id.get() + 1).expect("wrong owner"),
            entity_id,
            wrong_owner_index.key_schema.components().to_vec(),
            entity_key,
        )
        .expect("wrong-owner schema remains structurally valid");
        assert!(
            wrong_owner
                .internal_operational_index_descriptor(entity_id, index_id)
                .is_none()
        );
    }
}
