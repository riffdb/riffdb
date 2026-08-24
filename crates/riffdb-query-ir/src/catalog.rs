use std::collections::BTreeMap;

use riffdb_contract_ir::{
    ContractBundle, ExpressionKind, IndexFieldEncodingV1, KeySchema, RowPolicyOperationV1,
    ValueType,
};
use riffdb_types::{EntityTypeId, EnumTypeId, EnumVariantId, FieldId, IndexId};

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
    fields: Vec<String>,
    cover_fields: Vec<String>,
    encodings: Vec<IndexFieldEncodingV1>,
    key_schema: KeySchema,
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
                    fields: component_names,
                    cover_fields: cover_names,
                    encodings: index.encodings().to_vec(),
                    key_schema: index.key_schema().clone(),
                };
                if indexes.insert(symbol.name.clone(), symbol).is_some() {
                    return Err(invariant("duplicate exact-contract index"));
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
