use std::collections::BTreeMap;

use riffdb_contract_ir::{ContractBundle, ExpressionKind, KeySchema, ValueType};
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
                let symbol = IndexSymbol {
                    id: index.id(),
                    name: index.name().to_owned(),
                    fields: component_names,
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
        Ok(Self {
            identity: ExactContractIdentity::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            entities,
            enums,
        })
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
