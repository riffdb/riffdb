//! Stable-ID structural contract schema IR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{
    AggregateTypeId, CanonicalValue, CommandId, EntityTypeId, EnumTypeId, EnumVariantId,
    EventTypeId, FieldId, IndexId, InvariantId,
};

use crate::{
    ExprId, ExpressionArena, ExpressionKind, FieldSchema, IrValidationError, KeyPurpose, KeySchema,
    RecordTypeRef, ValueType, ValueTypeTag, checked_len, validate_source_name,
};

/// Maximum declarations of any one top-level kind in IR v1.
pub const MAX_DECLARATIONS_PER_KIND: usize = 4_096;

/// One complete stable record schema with canonical `FieldId` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSchema {
    owner: RecordTypeRef,
    fields: Vec<FieldSchema>,
}

impl RecordSchema {
    /// Creates a checked canonical record schema.
    pub fn new(
        owner: RecordTypeRef,
        mut fields: Vec<FieldSchema>,
    ) -> Result<Self, IrValidationError> {
        checked_len("record fields", fields.len(), MAX_DECLARATIONS_PER_KIND)?;
        fields.sort_unstable_by_key(FieldSchema::id);
        if fields.windows(2).any(|pair| pair[0].id() == pair[1].id()) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "record fields",
            });
        }
        let mut names = BTreeSet::new();
        if fields.iter().any(|field| !names.insert(field.name())) {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate field",
            });
        }
        Ok(Self { owner, fields })
    }

    /// Stable record owner.
    #[must_use]
    pub const fn owner(&self) -> &RecordTypeRef {
        &self.owner
    }

    /// Fields in increasing stable-ID order.
    #[must_use]
    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }

    /// Looks up one stable field.
    #[must_use]
    pub fn field(&self, id: FieldId) -> Option<&FieldSchema> {
        self.fields
            .binary_search_by_key(&id, FieldSchema::id)
            .ok()
            .map(|index| &self.fields[index])
    }
}

/// One named declared enum variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumVariantSchema {
    id: EnumVariantId,
    name: String,
}

impl EnumVariantSchema {
    /// Creates a checked enum variant.
    pub fn new(id: EnumVariantId, name: impl Into<String>) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "enum variant")?;
        Ok(Self { id, name })
    }

    /// Stable variant ID.
    #[must_use]
    pub const fn id(&self) -> EnumVariantId {
        self.id
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One declared enum and its canonical stable variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumSchema {
    id: EnumTypeId,
    name: String,
    variants: Vec<EnumVariantSchema>,
}

impl EnumSchema {
    /// Creates a checked declared enum.
    pub fn new(
        id: EnumTypeId,
        name: impl Into<String>,
        mut variants: Vec<EnumVariantSchema>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "enum")?;
        if variants.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "enum variants",
            });
        }
        checked_len("enum variants", variants.len(), MAX_DECLARATIONS_PER_KIND)?;
        variants.sort_unstable_by_key(EnumVariantSchema::id);
        if variants.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "enum variants",
            });
        }
        let mut names = BTreeSet::new();
        if variants
            .iter()
            .any(|variant| !names.insert(variant.name.as_str()))
        {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate enum variant",
            });
        }
        Ok(Self { id, name, variants })
    }

    /// Stable enum ID.
    #[must_use]
    pub const fn id(&self) -> EnumTypeId {
        self.id
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Variants in stable-ID order.
    #[must_use]
    pub fn variants(&self) -> &[EnumVariantSchema] {
        &self.variants
    }

    /// Whether a stable variant is declared.
    #[must_use]
    pub fn contains_variant(&self, id: EnumVariantId) -> bool {
        self.variants
            .binary_search_by_key(&id, EnumVariantSchema::id)
            .is_ok()
    }
}

/// A checked invariant template over schema fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantPlan {
    id: InvariantId,
    name: String,
    expressions: ExpressionArena,
    predicate: ExprId,
}

impl InvariantPlan {
    /// Creates a Boolean invariant template.
    pub fn new(
        id: InvariantId,
        name: impl Into<String>,
        expressions: ExpressionArena,
        predicate: ExprId,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "invariant")?;
        if expressions
            .get(predicate)
            .is_none_or(|node| node.result_type().tag() != ValueTypeTag::Bool)
        {
            return Err(IrValidationError::TypeMismatch {
                context: "invariant predicate",
            });
        }
        expressions.validate_reachable_from(&[predicate], "invariant predicate")?;
        Ok(Self {
            id,
            name,
            expressions,
            predicate,
        })
    }

    /// Stable invariant ID.
    #[must_use]
    pub const fn id(&self) -> InvariantId {
        self.id
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Typed expression arena.
    #[must_use]
    pub const fn expressions(&self) -> &ExpressionArena {
        &self.expressions
    }

    /// Terminal Boolean predicate expression.
    #[must_use]
    pub const fn predicate(&self) -> ExprId {
        self.predicate
    }
}

/// One local entity index declaration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TextKeyProfileV1 {
    /// Exact UTF-8 source bytes.
    BinaryUtf8,
    /// Unicode 17.0.0 NFKC followed by full non-Turkic case folding.
    UnicodeFold,
}

/// Compiler-sealed physical encoding for one logical index field.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IndexFieldEncodingV1 {
    /// Existing canonical scalar component.
    Canonical,
    /// Missing, explicit null, and non-null use distinct byte discriminators.
    Presence,
    /// Versioned canonical text key.
    TextKey(TextKeyProfileV1),
}

/// Maximum byte expansion charged for the frozen Unicode-fold profile.
pub const UNICODE_FOLD_V1_MAXIMUM_EXPANSION: usize = 18;

/// One local entity index declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSchema {
    id: IndexId,
    name: String,
    fields: Vec<FieldId>,
    encodings: Vec<IndexFieldEncodingV1>,
    key_schema: KeySchema,
}

impl IndexSchema {
    /// Creates a checked local index.
    pub fn new(
        id: IndexId,
        name: impl Into<String>,
        fields: Vec<FieldId>,
        key_schema: KeySchema,
    ) -> Result<Self, IrValidationError> {
        let encodings = vec![IndexFieldEncodingV1::Canonical; fields.len()];
        Self::with_encodings(id, name, fields, encodings, key_schema)
    }

    /// Creates a checked local index with explicit physical field encodings.
    pub fn with_encodings(
        id: IndexId,
        name: impl Into<String>,
        fields: Vec<FieldId>,
        encodings: Vec<IndexFieldEncodingV1>,
        key_schema: KeySchema,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "index")?;
        if fields.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "index fields",
            });
        }
        if fields.len() != encodings.len()
            || physical_index_component_count(&encodings) != key_schema.components().len()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "index field/schema arity mismatch",
            });
        }
        if !matches!(key_schema.purpose(), KeyPurpose::Index { index_id, .. } if index_id == id) {
            return Err(IrValidationError::InvalidKey {
                reason: "index key schema owner mismatch",
            });
        }
        let mut unique = BTreeSet::new();
        if fields.iter().any(|field| !unique.insert(*field)) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "index fields",
            });
        }
        Ok(Self {
            id,
            name,
            fields,
            encodings,
            key_schema,
        })
    }

    /// Stable index ID.
    #[must_use]
    pub const fn id(&self) -> IndexId {
        self.id
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Selected field IDs in declared component order.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }

    /// Physical encoding selected for each logical field in declared order.
    #[must_use]
    pub fn encodings(&self) -> &[IndexFieldEncodingV1] {
        &self.encodings
    }

    /// Complete checked index-entry schema.
    #[must_use]
    pub const fn key_schema(&self) -> &KeySchema {
        &self.key_schema
    }
}

/// One authoritative entity schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntitySchema {
    id: EntityTypeId,
    name: String,
    record: RecordSchema,
    primary_key_fields: Vec<FieldId>,
    primary_key: KeySchema,
    invariants: Vec<InvariantPlan>,
    indexes: Vec<IndexSchema>,
}

impl EntitySchema {
    /// Creates a checked entity schema.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: EntityTypeId,
        name: impl Into<String>,
        record: RecordSchema,
        primary_key_fields: Vec<FieldId>,
        primary_key: KeySchema,
        mut invariants: Vec<InvariantPlan>,
        mut indexes: Vec<IndexSchema>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "entity")?;
        if record.owner() != &RecordTypeRef::Entity(id) {
            return Err(IrValidationError::InvalidReference {
                kind: "entity record owner",
            });
        }
        if primary_key_fields.is_empty()
            || primary_key_fields.len() != primary_key.components().len()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "entity primary-key arity mismatch",
            });
        }
        if primary_key.purpose() != KeyPurpose::Entity(id) {
            return Err(IrValidationError::InvalidKey {
                reason: "entity primary-key owner mismatch",
            });
        }
        let mut unique = BTreeSet::new();
        for (field_id, component) in primary_key_fields.iter().zip(primary_key.components()) {
            let field = record
                .field(*field_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "entity primary-key field",
                })?;
            if !unique.insert(*field_id) || field.value_type() != component.value_type() {
                return Err(IrValidationError::InvalidKey {
                    reason: "entity primary-key field/type mismatch",
                });
            }
        }
        invariants.sort_unstable_by_key(InvariantPlan::id);
        indexes.sort_unstable_by_key(IndexSchema::id);
        reject_adjacent_id(&invariants, InvariantPlan::id, "entity invariants")?;
        reject_adjacent_id(&indexes, IndexSchema::id, "entity indexes")?;
        reject_duplicate_names(
            invariants.iter().map(InvariantPlan::name),
            "entity invariants",
        )?;
        reject_duplicate_names(indexes.iter().map(IndexSchema::name), "entity indexes")?;
        for invariant in &invariants {
            validate_schema_expression_arena(
                invariant.expressions(),
                &record,
                id,
                None,
                "entity invariant",
            )?;
        }
        for index in &indexes {
            let KeyPurpose::Index { entity_type, .. } = index.key_schema.purpose() else {
                return Err(IrValidationError::InvalidKey {
                    reason: "invalid index purpose",
                });
            };
            if entity_type != id || index.key_schema.entity_key_schema() != Some(&primary_key) {
                return Err(IrValidationError::InvalidKey {
                    reason: "index does not embed the exact entity primary-key schema",
                });
            }
            let mut physical = index.key_schema.components().iter();
            let invalid =
                index
                    .fields
                    .iter()
                    .zip(index.encodings.iter())
                    .any(|(field_id, encoding)| {
                        record.field(*field_id).is_none_or(|field| {
                            !index_components_match(field.value_type(), *encoding, &mut physical)
                        })
                    })
                    || physical.next().is_some();
            if invalid {
                return Err(IrValidationError::InvalidReference {
                    kind: "entity index field",
                });
            }
        }
        Ok(Self {
            id,
            name,
            record,
            primary_key_fields,
            primary_key,
            invariants,
            indexes,
        })
    }

    /// Stable entity ID.
    #[must_use]
    pub const fn id(&self) -> EntityTypeId {
        self.id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Complete record schema.
    #[must_use]
    pub const fn record(&self) -> &RecordSchema {
        &self.record
    }
    /// Primary-key field IDs in declared order.
    #[must_use]
    pub fn primary_key_fields(&self) -> &[FieldId] {
        &self.primary_key_fields
    }
    /// Complete primary-key schema.
    #[must_use]
    pub const fn primary_key(&self) -> &KeySchema {
        &self.primary_key
    }
    /// Entity invariants in stable-ID order.
    #[must_use]
    pub fn invariants(&self) -> &[InvariantPlan] {
        &self.invariants
    }
    /// Local indexes in stable-ID order.
    #[must_use]
    pub fn indexes(&self) -> &[IndexSchema] {
        &self.indexes
    }
}

fn physical_index_component_count(encodings: &[IndexFieldEncodingV1]) -> usize {
    encodings
        .iter()
        .map(|encoding| match encoding {
            IndexFieldEncodingV1::Presence => 2,
            IndexFieldEncodingV1::Canonical | IndexFieldEncodingV1::TextKey(_) => 1,
        })
        .sum()
}

fn index_components_match<'a>(
    logical: &ValueType,
    encoding: IndexFieldEncodingV1,
    physical: &mut impl Iterator<Item = &'a crate::KeyComponentSchema>,
) -> bool {
    match encoding {
        IndexFieldEncodingV1::Canonical => physical
            .next()
            .is_some_and(|component| logical == component.value_type()),
        IndexFieldEncodingV1::Presence => {
            logical
                .optional_inner()
                .is_some_and(ValueType::is_authoritative_key_scalar)
                && physical.next().is_some_and(|component| {
                    component.value_type().tag() == crate::ValueTypeTag::U64
                })
                && physical.next().is_some_and(|component| {
                    logical
                        .optional_inner()
                        .is_some_and(|inner| inner == component.value_type())
                })
        }
        IndexFieldEncodingV1::TextKey(_) => {
            logical.tag() == crate::ValueTypeTag::String
                && physical.next().is_some_and(|component| {
                    component.value_type().tag() == crate::ValueTypeTag::Bytes
                })
        }
    }
}

/// One durable event payload schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPartitionSchema {
    fields: Vec<FieldId>,
    key_schema: KeySchema,
}

impl EventPartitionSchema {
    /// Creates an exact event-payload derivation of one command partition key.
    pub fn new(
        fields: Vec<FieldId>,
        key_schema: KeySchema,
        payload: &RecordSchema,
    ) -> Result<Self, IrValidationError> {
        if fields.is_empty() || fields.len() != key_schema.components().len() {
            return Err(IrValidationError::InvalidKey {
                reason: "event partition field/key arity mismatch",
            });
        }
        if !matches!(key_schema.purpose(), KeyPurpose::Partition(_)) {
            return Err(IrValidationError::InvalidKey {
                reason: "event partition does not use a partition key schema",
            });
        }
        let mut seen = BTreeSet::new();
        for (field_id, component) in fields.iter().zip(key_schema.components()) {
            let field = payload
                .field(*field_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "event partition field",
                })?;
            if !seen.insert(*field_id) || field.value_type() != component.value_type() {
                return Err(IrValidationError::InvalidKey {
                    reason: "event partition field/type mismatch",
                });
            }
        }
        Ok(Self { fields, key_schema })
    }

    /// Event payload fields in canonical partition-component order.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }

    /// Exact aggregate-namespaced canonical partition key schema.
    #[must_use]
    pub const fn key_schema(&self) -> &KeySchema {
        &self.key_schema
    }
}

/// One durable event payload schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSchema {
    id: EventTypeId,
    name: String,
    payload: RecordSchema,
    partition: Option<EventPartitionSchema>,
}

impl EventSchema {
    /// Creates a checked durable event schema.
    pub fn new(
        id: EventTypeId,
        name: impl Into<String>,
        payload: RecordSchema,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "event")?;
        if payload.owner() != &RecordTypeRef::Event(id) {
            return Err(IrValidationError::InvalidReference {
                kind: "event payload owner",
            });
        }
        Ok(Self {
            id,
            name,
            payload,
            partition: None,
        })
    }

    /// Creates an application-streamable event with an exact partition derivation.
    pub fn partitioned(
        id: EventTypeId,
        name: impl Into<String>,
        payload: RecordSchema,
        partition: EventPartitionSchema,
    ) -> Result<Self, IrValidationError> {
        let mut event = Self::new(id, name, payload)?;
        if partition
            .fields()
            .iter()
            .any(|field| event.payload.field(*field).is_none())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "event partition field",
            });
        }
        event.partition = Some(partition);
        Ok(event)
    }
    /// Stable event ID.
    #[must_use]
    pub const fn id(&self) -> EventTypeId {
        self.id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Complete payload schema.
    #[must_use]
    pub const fn payload(&self) -> &RecordSchema {
        &self.payload
    }
    /// Exact application-stream partition derivation, when declared.
    #[must_use]
    pub const fn partition(&self) -> Option<&EventPartitionSchema> {
        self.partition.as_ref()
    }
}

/// One checked aggregate partition/conflict derivation template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateKeyPlan {
    expressions: ExpressionArena,
    partition_expression: ExprId,
    conflict_expressions: Vec<ExprId>,
    partition_schema: KeySchema,
    conflict_schema: KeySchema,
}

impl AggregateKeyPlan {
    /// Creates a checked aggregate key template.
    pub fn new(
        expressions: ExpressionArena,
        partition_expression: ExprId,
        conflict_expressions: Vec<ExprId>,
        partition_schema: KeySchema,
        conflict_schema: KeySchema,
    ) -> Result<Self, IrValidationError> {
        if conflict_expressions.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "conflict expressions",
            });
        }
        let partition =
            expressions
                .get(partition_expression)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "partition expression",
                })?;
        if partition_schema.components().len() != 1
            || partition.result_type() != partition_schema.components()[0].value_type()
            || conflict_expressions.len() != conflict_schema.components().len()
        {
            return Err(IrValidationError::TypeMismatch {
                context: "aggregate key plan",
            });
        }
        for (expression, component) in conflict_expressions
            .iter()
            .zip(conflict_schema.components())
        {
            if expressions
                .get(*expression)
                .is_none_or(|node| node.result_type() != component.value_type())
            {
                return Err(IrValidationError::TypeMismatch {
                    context: "aggregate conflict plan",
                });
            }
        }
        let roots = std::iter::once(partition_expression)
            .chain(conflict_expressions.iter().copied())
            .collect::<Vec<_>>();
        expressions.validate_reachable_from(&roots, "aggregate key expression")?;
        Ok(Self {
            expressions,
            partition_expression,
            conflict_expressions,
            partition_schema,
            conflict_schema,
        })
    }
    /// Expression arena.
    #[must_use]
    pub const fn expressions(&self) -> &ExpressionArena {
        &self.expressions
    }
    /// Single partition expression.
    #[must_use]
    pub const fn partition_expression(&self) -> ExprId {
        self.partition_expression
    }
    /// Ordered conflict expressions.
    #[must_use]
    pub fn conflict_expressions(&self) -> &[ExprId] {
        &self.conflict_expressions
    }
    /// Partition key schema.
    #[must_use]
    pub const fn partition_schema(&self) -> &KeySchema {
        &self.partition_schema
    }
    /// Conflict key schema.
    #[must_use]
    pub const fn conflict_schema(&self) -> &KeySchema {
        &self.conflict_schema
    }
}

/// One aggregate ownership and locality schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateSchema {
    id: AggregateTypeId,
    name: String,
    root: EntityTypeId,
    children: Vec<EntityTypeId>,
    keys: AggregateKeyPlan,
    invariants: Vec<InvariantPlan>,
}

impl AggregateSchema {
    /// Creates a checked aggregate schema.
    pub fn new(
        id: AggregateTypeId,
        name: impl Into<String>,
        root: EntityTypeId,
        mut children: Vec<EntityTypeId>,
        keys: AggregateKeyPlan,
        mut invariants: Vec<InvariantPlan>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "aggregate")?;
        children.sort_unstable();
        if children.binary_search(&root).is_ok()
            || children.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "aggregate children",
            });
        }
        if keys.partition_schema.purpose() != KeyPurpose::Partition(id)
            || keys.conflict_schema.purpose() != KeyPurpose::Conflict(id)
        {
            return Err(IrValidationError::InvalidKey {
                reason: "aggregate key owner mismatch",
            });
        }
        invariants.sort_unstable_by_key(InvariantPlan::id);
        reject_adjacent_id(&invariants, InvariantPlan::id, "aggregate invariants")?;
        reject_duplicate_names(
            invariants.iter().map(InvariantPlan::name),
            "aggregate invariants",
        )?;
        Ok(Self {
            id,
            name,
            root,
            children,
            keys,
            invariants,
        })
    }
    /// Stable aggregate ID.
    #[must_use]
    pub const fn id(&self) -> AggregateTypeId {
        self.id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Root entity.
    #[must_use]
    pub const fn root(&self) -> EntityTypeId {
        self.root
    }
    /// Child entities in stable-ID order.
    #[must_use]
    pub fn children(&self) -> &[EntityTypeId] {
        &self.children
    }
    /// Partition and conflict template.
    #[must_use]
    pub const fn keys(&self) -> &AggregateKeyPlan {
        &self.keys
    }
    /// Aggregate invariants.
    #[must_use]
    pub fn invariants(&self) -> &[InvariantPlan] {
        &self.invariants
    }
    /// Whether this aggregate owns an entity.
    #[must_use]
    pub fn owns(&self, entity: EntityTypeId) -> bool {
        entity == self.root || self.children.binary_search(&entity).is_ok()
    }
}

/// One required relationship between stored source fields and a complete target key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipSchema {
    name: String,
    source_entity: EntityTypeId,
    source_fields: Vec<FieldId>,
    target_entity: EntityTypeId,
    target_fields: Vec<FieldId>,
}

impl RelationshipSchema {
    /// Creates a bounded relationship. `SchemaIr` validates ownership, key
    /// completeness, component types, and the shared partition route.
    pub fn new(
        name: impl Into<String>,
        source_entity: EntityTypeId,
        source_fields: Vec<FieldId>,
        target_entity: EntityTypeId,
        target_fields: Vec<FieldId>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "relationship")?;
        if source_fields.is_empty()
            || source_fields.len() != target_fields.len()
            || source_fields.len() > 1_024
        {
            return Err(IrValidationError::InvalidReference {
                kind: "relationship component arity",
            });
        }
        let has_duplicate = |fields: &[FieldId]| {
            let mut seen = BTreeSet::new();
            fields.iter().any(|field| !seen.insert(*field))
        };
        if has_duplicate(&source_fields) || has_duplicate(&target_fields) {
            return Err(IrValidationError::InvalidReference {
                kind: "relationship duplicate component",
            });
        }
        Ok(Self {
            name,
            source_entity,
            source_fields,
            target_entity,
            target_fields,
        })
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Referencing entity.
    #[must_use]
    pub const fn source_entity(&self) -> EntityTypeId {
        self.source_entity
    }
    /// Stored source components in target-key order.
    #[must_use]
    pub fn source_fields(&self) -> &[FieldId] {
        &self.source_fields
    }
    /// Referenced entity.
    #[must_use]
    pub const fn target_entity(&self) -> EntityTypeId {
        self.target_entity
    }
    /// Complete target key in canonical order.
    #[must_use]
    pub fn target_fields(&self) -> &[FieldId] {
        &self.target_fields
    }
}

/// One required same-partition unique key backed by an authoritative index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniqueKeySchema {
    name: String,
    source_entity: EntityTypeId,
    index_id: IndexId,
    fields: Vec<FieldId>,
}

impl UniqueKeySchema {
    /// Creates a bounded unique-key declaration. `SchemaIr` validates the
    /// backing index, field types, ownership, and complete partition prefix.
    pub fn new(
        name: impl Into<String>,
        source_entity: EntityTypeId,
        index_id: IndexId,
        fields: Vec<FieldId>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "unique key")?;
        if fields.is_empty() || fields.len() > 1_024 {
            return Err(IrValidationError::InvalidKey {
                reason: "unique key component arity",
            });
        }
        let mut seen = BTreeSet::new();
        if fields.iter().any(|field| !seen.insert(*field)) {
            return Err(IrValidationError::InvalidKey {
                reason: "duplicate unique key component",
            });
        }
        Ok(Self {
            name,
            source_entity,
            index_id,
            fields,
        })
    }

    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Owning entity.
    #[must_use]
    pub const fn source_entity(&self) -> EntityTypeId {
        self.source_entity
    }
    /// Authoritative backing index identity.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }
    /// Complete unique components, beginning with the partition route.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }
}

/// Complete structural schema for one contract version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaIr {
    entities: Vec<EntitySchema>,
    events: Vec<EventSchema>,
    enums: Vec<EnumSchema>,
    aggregates: Vec<AggregateSchema>,
    relationships: Vec<RelationshipSchema>,
    unique_keys: Vec<UniqueKeySchema>,
}

impl SchemaIr {
    /// Creates a checked canonical schema and verifies aggregate ownership.
    pub fn new(
        entities: Vec<EntitySchema>,
        events: Vec<EventSchema>,
        enums: Vec<EnumSchema>,
        aggregates: Vec<AggregateSchema>,
    ) -> Result<Self, IrValidationError> {
        Self::with_integrity(entities, events, enums, aggregates, vec![], vec![])
    }

    /// Creates a checked canonical schema including required relationships.
    pub fn with_relationships(
        entities: Vec<EntitySchema>,
        events: Vec<EventSchema>,
        enums: Vec<EnumSchema>,
        aggregates: Vec<AggregateSchema>,
        relationships: Vec<RelationshipSchema>,
    ) -> Result<Self, IrValidationError> {
        Self::with_integrity(entities, events, enums, aggregates, relationships, vec![])
    }

    /// Creates a checked canonical schema including every declared integrity key.
    pub fn with_integrity(
        mut entities: Vec<EntitySchema>,
        mut events: Vec<EventSchema>,
        mut enums: Vec<EnumSchema>,
        mut aggregates: Vec<AggregateSchema>,
        mut relationships: Vec<RelationshipSchema>,
        mut unique_keys: Vec<UniqueKeySchema>,
    ) -> Result<Self, IrValidationError> {
        for (kind, count) in [
            ("entities", entities.len()),
            ("events", events.len()),
            ("enums", enums.len()),
            ("aggregates", aggregates.len()),
            ("relationships", relationships.len()),
            ("unique keys", unique_keys.len()),
        ] {
            checked_len(kind, count, MAX_DECLARATIONS_PER_KIND)?;
        }
        entities.sort_unstable_by_key(EntitySchema::id);
        events.sort_unstable_by_key(EventSchema::id);
        enums.sort_unstable_by_key(EnumSchema::id);
        aggregates.sort_unstable_by_key(AggregateSchema::id);
        relationships.sort_unstable_by(|left, right| {
            left.source_entity
                .cmp(&right.source_entity)
                .then_with(|| left.name.cmp(&right.name))
        });
        unique_keys.sort_unstable_by(|left, right| {
            left.source_entity
                .cmp(&right.source_entity)
                .then_with(|| left.name.cmp(&right.name))
        });
        reject_adjacent_id(&entities, EntitySchema::id, "entities")?;
        reject_adjacent_id(&events, EventSchema::id, "events")?;
        reject_adjacent_id(&enums, EnumSchema::id, "enums")?;
        reject_adjacent_id(&aggregates, AggregateSchema::id, "aggregates")?;
        reject_duplicate_names(entities.iter().map(EntitySchema::name), "entities")?;
        reject_duplicate_names(events.iter().map(EventSchema::name), "events")?;
        reject_duplicate_names(enums.iter().map(EnumSchema::name), "enums")?;
        reject_duplicate_names(aggregates.iter().map(AggregateSchema::name), "aggregates")?;
        if relationships.windows(2).any(|pair| {
            pair[0].source_entity == pair[1].source_entity && pair[0].name == pair[1].name
        }) {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate relationship",
            });
        }
        if unique_keys.windows(2).any(|pair| {
            pair[0].source_entity == pair[1].source_entity && pair[0].name == pair[1].name
        }) {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate unique key",
            });
        }

        let mut global_indexes = BTreeSet::new();
        if entities
            .iter()
            .flat_map(EntitySchema::indexes)
            .any(|index| !global_indexes.insert(index.id()))
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "lineage-global index IDs",
            });
        }
        let mut global_invariants = BTreeSet::new();
        if entities
            .iter()
            .flat_map(EntitySchema::invariants)
            .chain(aggregates.iter().flat_map(AggregateSchema::invariants))
            .any(|invariant| !global_invariants.insert(invariant.id()))
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "lineage-global invariant IDs",
            });
        }

        let entity_map = entities
            .iter()
            .map(|entity| (entity.id, entity))
            .collect::<BTreeMap<_, _>>();
        let mut ownership = BTreeMap::new();
        for aggregate in &aggregates {
            let root =
                entity_map
                    .get(&aggregate.root)
                    .ok_or(IrValidationError::InvalidReference {
                        kind: "aggregate root",
                    })?;
            let primary_fields = root
                .primary_key_fields()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            validate_schema_expression_arena(
                aggregate.keys().expressions(),
                root.record(),
                root.id(),
                Some(&primary_fields),
                "aggregate key template",
            )?;
            for invariant in aggregate.invariants() {
                validate_schema_expression_arena(
                    invariant.expressions(),
                    root.record(),
                    root.id(),
                    None,
                    "aggregate invariant",
                )?;
            }
            for entity_id in std::iter::once(&aggregate.root).chain(&aggregate.children) {
                let entity =
                    entity_map
                        .get(entity_id)
                        .ok_or(IrValidationError::InvalidReference {
                            kind: "aggregate child",
                        })?;
                if ownership.insert(*entity_id, aggregate.id).is_some() {
                    return Err(IrValidationError::InvalidReference {
                        kind: "entity has multiple aggregate owners",
                    });
                }
                if *entity_id != aggregate.root {
                    validate_child_key_prefix(root, entity)?;
                }
            }
        }
        for relationship in &relationships {
            validate_relationship(relationship, &entity_map, &aggregates, &ownership)?;
        }
        for unique in &unique_keys {
            validate_unique_key(unique, &entity_map, &aggregates, &ownership)?;
        }
        for event in &events {
            let Some(partition) = event.partition() else {
                continue;
            };
            let KeyPurpose::Partition(aggregate_id) = partition.key_schema().purpose() else {
                return Err(IrValidationError::InvalidKey {
                    reason: "event partition purpose",
                });
            };
            let aggregate = aggregates
                .binary_search_by_key(&aggregate_id, AggregateSchema::id)
                .ok()
                .map(|index| &aggregates[index])
                .ok_or(IrValidationError::InvalidReference {
                    kind: "event partition aggregate",
                })?;
            if partition.key_schema() != aggregate.keys().partition_schema() {
                return Err(IrValidationError::InvalidKey {
                    reason: "event partition key schema differs from aggregate",
                });
            }
        }
        let result = Self {
            entities,
            events,
            enums,
            aggregates,
            relationships,
            unique_keys,
        };
        result.validate_enum_references()?;
        result.validate_schema_enum_registries_and_constants()?;
        Ok(result)
    }

    /// Entity schemas in stable-ID order.
    #[must_use]
    pub fn entities(&self) -> &[EntitySchema] {
        &self.entities
    }
    /// Event schemas in stable-ID order.
    #[must_use]
    pub fn events(&self) -> &[EventSchema] {
        &self.events
    }
    /// Enum schemas in stable-ID order.
    #[must_use]
    pub fn enums(&self) -> &[EnumSchema] {
        &self.enums
    }
    /// Aggregate schemas in stable-ID order.
    #[must_use]
    pub fn aggregates(&self) -> &[AggregateSchema] {
        &self.aggregates
    }
    /// Required relationships in canonical source-entity/name order.
    #[must_use]
    pub fn relationships(&self) -> &[RelationshipSchema] {
        &self.relationships
    }
    /// Same-partition unique keys in canonical source-entity/name order.
    #[must_use]
    pub fn unique_keys(&self) -> &[UniqueKeySchema] {
        &self.unique_keys
    }
    /// Resolves an entity.
    #[must_use]
    pub fn entity(&self, id: EntityTypeId) -> Option<&EntitySchema> {
        self.entities
            .binary_search_by_key(&id, EntitySchema::id)
            .ok()
            .map(|i| &self.entities[i])
    }
    /// Resolves an event.
    #[must_use]
    pub fn event(&self, id: EventTypeId) -> Option<&EventSchema> {
        self.events
            .binary_search_by_key(&id, EventSchema::id)
            .ok()
            .map(|i| &self.events[i])
    }
    /// Resolves an enum.
    #[must_use]
    pub fn enumeration(&self, id: EnumTypeId) -> Option<&EnumSchema> {
        self.enums
            .binary_search_by_key(&id, EnumSchema::id)
            .ok()
            .map(|i| &self.enums[i])
    }
    /// Resolves an aggregate.
    #[must_use]
    pub fn aggregate(&self, id: AggregateTypeId) -> Option<&AggregateSchema> {
        self.aggregates
            .binary_search_by_key(&id, AggregateSchema::id)
            .ok()
            .map(|i| &self.aggregates[i])
    }
    /// Resolves the one aggregate owning an entity, if any.
    #[must_use]
    pub fn aggregate_for_entity(&self, entity: EntityTypeId) -> Option<&AggregateSchema> {
        self.aggregates
            .iter()
            .find(|aggregate| aggregate.owns(entity))
    }

    fn validate_enum_references(&self) -> Result<(), IrValidationError> {
        for record in self.entities.iter().map(EntitySchema::record) {
            for field in record.fields() {
                validate_declared_field_type(field.value_type(), self)?;
            }
        }
        for record in self.events.iter().map(EventSchema::payload) {
            for field in record.fields() {
                validate_declared_field_type(field.value_type(), self)?;
            }
        }
        Ok(())
    }

    fn validate_schema_enum_registries_and_constants(&self) -> Result<(), IrValidationError> {
        for entity in &self.entities {
            self.validate_key_enum_registry(entity.primary_key())?;
            for index in entity.indexes() {
                self.validate_key_enum_registry(index.key_schema())?;
            }
            for invariant in entity.invariants() {
                self.validate_expression_enum_constants(invariant.expressions())?;
            }
        }
        for aggregate in &self.aggregates {
            self.validate_key_enum_registry(aggregate.keys().partition_schema())?;
            self.validate_key_enum_registry(aggregate.keys().conflict_schema())?;
            self.validate_expression_enum_constants(aggregate.keys().expressions())?;
            for invariant in aggregate.invariants() {
                self.validate_expression_enum_constants(invariant.expressions())?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_key_enum_registry(
        &self,
        key: &KeySchema,
    ) -> Result<(), IrValidationError> {
        for component in key.components() {
            if let Some(enum_id) = component.value_type().enum_type_id() {
                self.validate_enum_variant_registry(enum_id, component.enum_variants(), "key")?;
            }
        }
        if let Some(entity_key) = key.entity_key_schema() {
            self.validate_key_enum_registry(entity_key)?;
        }
        Ok(())
    }

    pub(crate) fn validate_enum_variant_registry(
        &self,
        enum_id: EnumTypeId,
        actual: &[EnumVariantId],
        context: &'static str,
    ) -> Result<(), IrValidationError> {
        let expected = self
            .enumeration(enum_id)
            .ok_or(IrValidationError::InvalidReference { kind: "enum type" })?
            .variants()
            .iter()
            .map(EnumVariantSchema::id)
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(IrValidationError::InvalidDependency {
                reason: match context {
                    "key" => "key enum registry does not exactly match its declared enum",
                    "projection" => {
                        "projection enum registry does not exactly match its declared enum"
                    }
                    _ => "enum registry does not exactly match its declared enum",
                },
            });
        }
        Ok(())
    }

    pub(crate) fn validate_expression_enum_constants(
        &self,
        arena: &ExpressionArena,
    ) -> Result<(), IrValidationError> {
        for node in arena.nodes() {
            if let ExpressionKind::Constant(value) = node.kind() {
                self.validate_canonical_value(node.result_type(), value)?;
            }
        }
        Ok(())
    }

    fn validate_canonical_value(
        &self,
        value_type: &ValueType,
        value: &CanonicalValue,
    ) -> Result<(), IrValidationError> {
        value_type.validate_value(value)?;
        if matches!(value, CanonicalValue::Null) {
            return Ok(());
        }
        if let Some(inner) = value_type.optional_inner() {
            return self.validate_canonical_value(inner, value);
        }
        if let Some(enum_id) = value_type.enum_type_id() {
            let CanonicalValue::Enum {
                type_id,
                variant_id,
            } = value
            else {
                return Err(IrValidationError::TypeMismatch {
                    context: "enum constant",
                });
            };
            if *type_id != enum_id
                || self
                    .enumeration(enum_id)
                    .is_none_or(|enumeration| !enumeration.contains_variant(*variant_id))
            {
                return Err(IrValidationError::InvalidReference {
                    kind: "enum constant variant",
                });
            }
            return Ok(());
        }
        if let Some((element, _)) = value_type.list_parts() {
            let CanonicalValue::List(values) = value else {
                return Err(IrValidationError::TypeMismatch {
                    context: "list constant",
                });
            };
            for value in values.values() {
                self.validate_canonical_value(element, value)?;
            }
            return Ok(());
        }
        if let Some(record_ref) = value_type.record_ref() {
            let CanonicalValue::Record(value) = value else {
                return Err(IrValidationError::TypeMismatch {
                    context: "record constant",
                });
            };
            let record = match record_ref {
                RecordTypeRef::Entity(id) => self.entity(*id).map(EntitySchema::record),
                RecordTypeRef::Event(id) => self.event(*id).map(EventSchema::payload),
                _ => None,
            }
            .ok_or(IrValidationError::InvalidReference {
                kind: "record constant type",
            })?;
            if value.fields().len() != record.fields().len() {
                return Err(IrValidationError::TypeMismatch {
                    context: "record constant fields",
                });
            }
            for ((actual_id, actual), declared) in value.fields().iter().zip(record.fields()) {
                if *actual_id != declared.id() {
                    return Err(IrValidationError::InvalidReference {
                        kind: "record constant field",
                    });
                }
                self.validate_canonical_value(declared.value_type(), actual)?;
            }
        }
        Ok(())
    }
}

fn validate_schema_expression_arena(
    arena: &ExpressionArena,
    record: &RecordSchema,
    entity_type: EntityTypeId,
    allowed_fields: Option<&BTreeSet<FieldId>>,
    context: &'static str,
) -> Result<(), IrValidationError> {
    for node in arena.nodes() {
        match node.kind() {
            crate::ExpressionKind::Constant(_) => {}
            crate::ExpressionKind::SchemaField {
                entity_type: actual_entity,
                field,
            } => {
                let declared = record
                    .field(*field)
                    .ok_or(IrValidationError::InvalidReference { kind: context })?;
                if *actual_entity != entity_type
                    || declared.value_type() != node.result_type()
                    || allowed_fields.is_some_and(|allowed| !allowed.contains(field))
                {
                    return Err(IrValidationError::InvalidDependency {
                        reason: "schema expression references a field outside its exact context",
                    });
                }
            }
            crate::ExpressionKind::Unary { .. } | crate::ExpressionKind::Binary { .. } => {}
            _ => {
                return Err(IrValidationError::InvalidDependency {
                    reason: "schema expression contains a command, event, or transaction dependency",
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_declared_field_type(
    value_type: &crate::ValueType,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    if value_type.tag() == ValueTypeTag::Record {
        return Err(IrValidationError::TypeMismatch {
            context: "declared grammar-v1 field type",
        });
    }
    if let Some(id) = value_type.enum_type_id()
        && schema.enumeration(id).is_none()
    {
        return Err(IrValidationError::InvalidReference { kind: "enum type" });
    }
    if let Some(inner) = value_type.optional_inner() {
        validate_declared_field_type(inner, schema)?;
    }
    if let Some((element, _)) = value_type.list_parts() {
        validate_declared_field_type(element, schema)?;
    }
    Ok(())
}

/// Validates a command input while confining record-valued lists to the one
/// compiler-declared collection expansion input.
pub(crate) fn validate_command_input_field_type(
    value_type: &crate::ValueType,
    schema: &SchemaIr,
    collection_input: bool,
) -> Result<(), IrValidationError> {
    if !collection_input {
        return validate_declared_field_type(value_type, schema);
    }
    let Some((element, maximum)) = value_type.list_parts() else {
        return Err(IrValidationError::TypeMismatch {
            context: "collection command list input",
        });
    };
    if maximum > crate::MAX_COLLECTION_COMMAND_ELEMENTS_V1 {
        return Err(IrValidationError::LimitExceeded {
            kind: "collection command elements",
            actual: maximum,
            maximum: crate::MAX_COLLECTION_COMMAND_ELEMENTS_V1,
        });
    }
    if let Some(record) = element.record_ref() {
        return match record {
            RecordTypeRef::Entity(id) if schema.entity(*id).is_some() => Ok(()),
            _ => Err(IrValidationError::InvalidReference {
                kind: "collection element record type",
            }),
        };
    }
    validate_declared_field_type(element, schema)
}

pub(crate) fn validate_payload_field_type(
    value_type: &crate::ValueType,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    if let Some(record) = value_type.record_ref() {
        return match record {
            RecordTypeRef::Entity(id) if schema.entity(*id).is_some() => Ok(()),
            RecordTypeRef::Event(id) if schema.event(*id).is_some() => Ok(()),
            _ => Err(IrValidationError::InvalidReference {
                kind: "payload record type",
            }),
        };
    }
    if let Some(id) = value_type.enum_type_id()
        && schema.enumeration(id).is_none()
    {
        return Err(IrValidationError::InvalidReference { kind: "enum type" });
    }
    if let Some(inner) = value_type.optional_inner() {
        validate_payload_field_type(inner, schema)?;
    }
    if let Some((element, _)) = value_type.list_parts() {
        validate_payload_field_type(element, schema)?;
    }
    Ok(())
}

fn validate_relationship(
    relationship: &RelationshipSchema,
    entities: &BTreeMap<EntityTypeId, &EntitySchema>,
    aggregates: &[AggregateSchema],
    ownership: &BTreeMap<EntityTypeId, AggregateTypeId>,
) -> Result<(), IrValidationError> {
    let source =
        entities
            .get(&relationship.source_entity)
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship source entity",
            })?;
    let target =
        entities
            .get(&relationship.target_entity)
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship target entity",
            })?;
    if target.primary_key_fields() != relationship.target_fields {
        return Err(IrValidationError::InvalidReference {
            kind: "relationship target must be complete primary key",
        });
    }
    let source_owner =
        ownership
            .get(&source.id())
            .copied()
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship source aggregate owner",
            })?;
    let target_owner =
        ownership
            .get(&target.id())
            .copied()
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship target aggregate owner",
            })?;
    for (source_field, target_field) in relationship
        .source_fields
        .iter()
        .zip(&relationship.target_fields)
    {
        let source_field =
            source
                .record()
                .field(*source_field)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "relationship source field",
                })?;
        let target_field =
            target
                .record()
                .field(*target_field)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "relationship target field",
                })?;
        if source_field.value_type().is_optional()
            || source_field.value_type() != target_field.value_type()
        {
            return Err(IrValidationError::TypeMismatch {
                context: "required relationship component",
            });
        }
    }
    let source_aggregate = aggregates
        .iter()
        .find(|aggregate| aggregate.id() == source_owner)
        .ok_or(IrValidationError::InvalidReference {
            kind: "relationship source aggregate",
        })?;
    let target_aggregate = aggregates
        .iter()
        .find(|aggregate| aggregate.id() == target_owner)
        .ok_or(IrValidationError::InvalidReference {
            kind: "relationship target aggregate",
        })?;
    let source_root =
        entities
            .get(&source_aggregate.root())
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship source aggregate root",
            })?;
    let target_root =
        entities
            .get(&target_aggregate.root())
            .ok_or(IrValidationError::InvalidReference {
                kind: "relationship target aggregate root",
            })?;
    if !relationship_partition_templates_match(
        relationship,
        source,
        target,
        source_aggregate,
        target_aggregate,
        source_root,
        target_root,
    )? {
        return Err(IrValidationError::InvalidReference {
            kind: "relationship partition route",
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn relationship_partition_templates_match(
    relationship: &RelationshipSchema,
    source: &EntitySchema,
    target: &EntitySchema,
    source_aggregate: &AggregateSchema,
    target_aggregate: &AggregateSchema,
    source_root: &EntitySchema,
    target_root: &EntitySchema,
) -> Result<bool, IrValidationError> {
    let source_arena = source_aggregate.keys().expressions();
    let target_arena = target_aggregate.keys().expressions();
    let mut pending = vec![(
        source_aggregate.keys().partition_expression(),
        target_aggregate.keys().partition_expression(),
    )];
    let mut visited = BTreeSet::new();
    while let Some((source_id, target_id)) = pending.pop() {
        if !visited.insert((source_id, target_id)) {
            continue;
        }
        checked_len(
            "relationship partition expression pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let source_node =
            source_arena
                .get(source_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "relationship source partition expression",
                })?;
        let target_node =
            target_arena
                .get(target_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "relationship target partition expression",
                })?;
        if source_node.result_type() != target_node.result_type() {
            return Ok(false);
        }
        match (source_node.kind(), target_node.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (
                ExpressionKind::SchemaField {
                    entity_type: left_entity,
                    field: left_field,
                },
                ExpressionKind::SchemaField {
                    entity_type: right_entity,
                    field: right_field,
                },
            ) => {
                if *left_entity != source_root.id() || *right_entity != target_root.id() {
                    return Ok(false);
                }
                let Some(left_position) = source_root
                    .primary_key_fields()
                    .iter()
                    .position(|candidate| candidate == left_field)
                else {
                    return Ok(false);
                };
                let Some(source_partition_field) =
                    source.primary_key_fields().get(left_position).copied()
                else {
                    return Ok(false);
                };
                let Some(right_position) = target_root
                    .primary_key_fields()
                    .iter()
                    .position(|candidate| candidate == right_field)
                else {
                    return Ok(false);
                };
                let Some(target_partition_field) =
                    target.primary_key_fields().get(right_position).copied()
                else {
                    return Ok(false);
                };
                let Some(mapped_position) = relationship
                    .target_fields
                    .iter()
                    .position(|field| *field == target_partition_field)
                else {
                    return Ok(false);
                };
                if relationship.source_fields[mapped_position] != source_partition_field {
                    return Ok(false);
                }
            }
            (
                ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn validate_unique_key(
    unique: &UniqueKeySchema,
    entities: &BTreeMap<EntityTypeId, &EntitySchema>,
    aggregates: &[AggregateSchema],
    ownership: &BTreeMap<EntityTypeId, AggregateTypeId>,
) -> Result<(), IrValidationError> {
    let source =
        entities
            .get(&unique.source_entity)
            .ok_or(IrValidationError::InvalidReference {
                kind: "unique key source entity",
            })?;
    let index = source
        .indexes()
        .iter()
        .find(|index| index.id() == unique.index_id)
        .ok_or(IrValidationError::InvalidReference {
            kind: "unique key backing index",
        })?;
    if index.name() != unique.name || index.fields() != unique.fields {
        return Err(IrValidationError::InvalidReference {
            kind: "unique key backing index mismatch",
        });
    }
    for field in &unique.fields {
        let field = source
            .record()
            .field(*field)
            .ok_or(IrValidationError::InvalidReference {
                kind: "unique key field",
            })?;
        if field.value_type().is_optional() {
            return Err(IrValidationError::TypeMismatch {
                context: "required unique key component",
            });
        }
    }
    let owner_id =
        ownership
            .get(&source.id())
            .copied()
            .ok_or(IrValidationError::InvalidReference {
                kind: "unique key aggregate owner",
            })?;
    let owner = aggregates
        .iter()
        .find(|aggregate| aggregate.id() == owner_id)
        .ok_or(IrValidationError::InvalidReference {
            kind: "unique key aggregate",
        })?;
    let root = entities
        .get(&owner.root())
        .ok_or(IrValidationError::InvalidReference {
            kind: "unique key aggregate root",
        })?;
    let route = aggregate_partition_route_fields(owner, root, source)?;
    if unique.fields.get(..route.len()) != Some(route.as_slice()) {
        return Err(IrValidationError::InvalidKey {
            reason: "unique key lacks complete canonical partition prefix",
        });
    }
    Ok(())
}

fn aggregate_partition_route_fields(
    aggregate: &AggregateSchema,
    root: &EntitySchema,
    entity: &EntitySchema,
) -> Result<Vec<FieldId>, IrValidationError> {
    let mut dependencies = BTreeSet::new();
    let mut pending = vec![aggregate.keys().partition_expression()];
    let mut visited = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        checked_len(
            "aggregate partition dependency nodes",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let node =
            aggregate
                .keys()
                .expressions()
                .get(id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "aggregate partition expression",
                })?;
        match node.kind() {
            ExpressionKind::SchemaField { entity_type, field } if *entity_type == root.id() => {
                dependencies.insert(*field);
            }
            ExpressionKind::Unary { operand, .. } => pending.push(*operand),
            ExpressionKind::Binary { left, right, .. } => {
                pending.push(*right);
                pending.push(*left);
            }
            ExpressionKind::Constant(_) => {}
            _ => {
                return Err(IrValidationError::InvalidDependency {
                    reason: "aggregate partition expression is not root-key computable",
                });
            }
        }
    }
    root.primary_key_fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| dependencies.contains(field))
        .map(|(position, _)| {
            entity.primary_key_fields().get(position).copied().ok_or(
                IrValidationError::InvalidKey {
                    reason: "entity key lacks aggregate partition dependency prefix",
                },
            )
        })
        .collect()
}

fn validate_child_key_prefix(
    root: &EntitySchema,
    child: &EntitySchema,
) -> Result<(), IrValidationError> {
    if child.primary_key_fields.len() < root.primary_key_fields.len() {
        return Err(IrValidationError::InvalidKey {
            reason: "child key lacks root prefix",
        });
    }
    for (root_field_id, child_field_id) in root
        .primary_key_fields
        .iter()
        .zip(&child.primary_key_fields)
    {
        let root_field = root
            .record
            .field(*root_field_id)
            .expect("validated root key");
        let child_field = child
            .record
            .field(*child_field_id)
            .expect("validated child key");
        if root_field.name() != child_field.name()
            || root_field.value_type() != child_field.value_type()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "child key does not begin with the root key name/type sequence",
            });
        }
    }
    Ok(())
}

fn reject_adjacent_id<T, K: Eq>(
    values: &[T],
    id: impl Fn(&T) -> K,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    if values.windows(2).any(|pair| id(&pair[0]) == id(&pair[1])) {
        Err(IrValidationError::NonCanonicalOrder { kind })
    } else {
        Ok(())
    }
}

fn reject_duplicate_names<'a>(
    values: impl Iterator<Item = &'a str>,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    let mut names = BTreeSet::new();
    if values.into_iter().any(|name| !names.insert(name)) {
        Err(IrValidationError::InvalidName { kind })
    } else {
        Ok(())
    }
}

/// A stable command-input record declaration used by HIR and command plans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandInputSchema {
    command_id: CommandId,
    record: RecordSchema,
}

impl CommandInputSchema {
    /// Creates a checked command input record.
    pub fn new(command_id: CommandId, record: RecordSchema) -> Result<Self, IrValidationError> {
        if record.owner() != &RecordTypeRef::CommandInput(command_id) {
            return Err(IrValidationError::InvalidReference {
                kind: "command input owner",
            });
        }
        Ok(Self { command_id, record })
    }
    /// Owning command.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Complete input record.
    #[must_use]
    pub const fn record(&self) -> &RecordSchema {
        &self.record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyComponentSchema, ValueType};
    use riffdb_types::CanonicalValue;

    #[test]
    fn records_sort_by_stable_field_id() {
        let fields = vec![
            FieldSchema::new(FieldId::new(2).expect("id"), "b", ValueType::i64()).expect("field"),
            FieldSchema::new(FieldId::first(), "a", ValueType::i64()).expect("field"),
        ];
        let record = RecordSchema::new(RecordTypeRef::Entity(EntityTypeId::first()), fields)
            .expect("record");
        assert_eq!(record.fields()[0].id(), FieldId::first());
    }

    #[test]
    fn entity_requires_key_schema_to_match_fields() {
        let record = RecordSchema::new(
            RecordTypeRef::Entity(EntityTypeId::first()),
            vec![FieldSchema::new(FieldId::first(), "id", ValueType::u64()).expect("field")],
        )
        .expect("record");
        let key = KeySchema::new(
            KeyPurpose::Entity(EntityTypeId::first()),
            vec![KeyComponentSchema::new(ValueType::i64(), vec![]).expect("component")],
        )
        .expect("key");
        assert!(
            EntitySchema::new(
                EntityTypeId::first(),
                "Thing",
                record,
                vec![FieldId::first()],
                key,
                vec![],
                vec![],
            )
            .is_err()
        );
    }

    #[test]
    fn enum_registries_and_nested_constants_must_match_the_schema_exactly() {
        let enum_id = EnumTypeId::first();
        let variant_id = EnumVariantId::first();
        let enumeration = EnumSchema::new(
            enum_id,
            "State",
            vec![EnumVariantSchema::new(variant_id, "Open").expect("variant")],
        )
        .expect("enum");
        let enum_type = ValueType::enumeration(enum_id);
        let entity_id = EntityTypeId::first();
        let entity = EntitySchema::new(
            entity_id,
            "Thing",
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![
                    FieldSchema::new(FieldId::first(), "state", enum_type.clone()).expect("field"),
                ],
            )
            .expect("record"),
            vec![FieldId::first()],
            KeySchema::new(
                KeyPurpose::Entity(entity_id),
                vec![
                    KeyComponentSchema::new(
                        enum_type.clone(),
                        vec![EnumVariantId::new(2).expect("variant")],
                    )
                    .expect("component"),
                ],
            )
            .expect("key"),
            vec![],
            vec![],
        )
        .expect("entity");
        assert!(SchemaIr::new(vec![entity], vec![], vec![enumeration.clone()], vec![]).is_err());

        let schema = SchemaIr::new(vec![], vec![], vec![enumeration], vec![]).expect("schema");
        let invalid = CanonicalValue::Enum {
            type_id: enum_id,
            variant_id: EnumVariantId::new(2).expect("variant"),
        };
        assert!(
            schema
                .validate_canonical_value(&enum_type, &invalid)
                .is_err()
        );
        let list_type = ValueType::list(enum_type, 4).expect("list type");
        let list = CanonicalValue::list(vec![invalid]).expect("list");
        assert!(schema.validate_canonical_value(&list_type, &list).is_err());
    }

    #[test]
    fn invariant_rejects_an_unreachable_expression_node() {
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                ValueType::bool(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::Bool(false)),
                ValueType::bool(),
            ),
        ])
        .expect("arena");
        assert!(InvariantPlan::new(InvariantId::first(), "Always", arena, ExprId::new(0)).is_err());
    }

    #[test]
    fn declared_events_reject_self_and_mutual_record_recursion() {
        let first_id = EventTypeId::first();
        let second_id = EventTypeId::new(2).expect("event ID");
        let event = |id, name, referenced| {
            EventSchema::new(
                id,
                name,
                RecordSchema::new(
                    RecordTypeRef::Event(id),
                    vec![
                        FieldSchema::new(
                            FieldId::first(),
                            "nested",
                            ValueType::record(RecordTypeRef::Event(referenced)),
                        )
                        .expect("field"),
                    ],
                )
                .expect("record"),
            )
            .expect("event")
        };

        assert!(
            SchemaIr::new(
                vec![],
                vec![event(first_id, "SelfEvent", first_id)],
                vec![],
                vec![],
            )
            .is_err()
        );
        assert!(
            SchemaIr::new(
                vec![],
                vec![
                    event(first_id, "FirstEvent", second_id),
                    event(second_id, "SecondEvent", first_id),
                ],
                vec![],
                vec![],
            )
            .is_err()
        );
    }

    #[test]
    fn event_partition_requires_an_exact_declared_aggregate_key_schema() {
        let event_id = EventTypeId::first();
        let field_id = FieldId::first();
        let payload = RecordSchema::new(
            RecordTypeRef::Event(event_id),
            vec![FieldSchema::new(field_id, "tenant_id", ValueType::uuid()).expect("field")],
        )
        .expect("payload");
        let key_schema = KeySchema::new(
            KeyPurpose::Partition(AggregateTypeId::first()),
            vec![KeyComponentSchema::new(ValueType::uuid(), vec![]).expect("component")],
        )
        .expect("partition key");
        let partition = EventPartitionSchema::new(vec![field_id], key_schema, &payload)
            .expect("partition schema");
        let event = EventSchema::partitioned(event_id, "Changed", payload, partition)
            .expect("partitioned event");

        assert!(SchemaIr::new(vec![], vec![event], vec![], vec![]).is_err());
    }
}
