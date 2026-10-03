//! Borrowed structural framing and native constructor workspace admission.
// req: OQ-040, PRJ-004
use super::*;
use crate::schema::*;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use std::sync::Arc;
use workflow_admission::allocation;

fn add(total: &mut u64, bytes: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(bytes).ok_or(BudgetUnavailable)?;
    Ok(())
}
fn grow(count: usize) -> Result<usize, BudgetUnavailable> {
    count
        .checked_mul(2)
        .map(|count| count.max(4))
        .ok_or(BudgetUnavailable)
}

#[derive(Default)]
struct Walk {
    heap: u64,
    constructors: u64,
    indexes: usize,
    invariants: usize,
    largest_arena: usize,
}
impl Walk {
    fn name(
        &mut self,
        reader: &mut Reader<'_>,
        maximum: usize,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        let bytes = reader.bytes(maximum)?;
        std::str::from_utf8(bytes).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
        add(&mut self.heap, allocation::<u8>(bytes.len())?)?;
        Ok(())
    }
    fn vector<T>(&mut self, count: usize) -> Result<(), BudgetUnavailable> {
        add(&mut self.heap, allocation::<T>(count)?)
    }
    fn extension<T>(
        &mut self,
        reader: &mut Reader<'_>,
        marker: u32,
        kind: &'static str,
    ) -> Result<Option<usize>, ProviderBundleEncodingErrorV1> {
        if reader.remaining() < 4 || reader.peek_u32()? != marker {
            return Ok(None);
        }
        reader.u32()?;
        let count = decode_len(reader, kind, crate::MAX_DECLARATIONS_PER_KIND)?;
        // Empty Vec::reserve may round a small request up to its native minimum.
        self.vector::<T>(if count == 0 { 0 } else { count.max(4) })?;
        Ok(Some(count))
    }
    fn fields<T>(
        &mut self,
        reader: &mut Reader<'_>,
        kind: &'static str,
        maximum: usize,
        width: usize,
    ) -> Result<usize, ProviderBundleEncodingErrorV1> {
        let count = decode_len(reader, kind, maximum)?;
        self.vector::<T>(count)?;
        reader.read(count.checked_mul(width).ok_or(BudgetUnavailable)?)?;
        Ok(count)
    }
    fn field_set(&mut self, count: usize) -> Result<(), BudgetUnavailable> {
        add(
            &mut self.constructors,
            plan_hash_admission::tree_bound::<FieldId>(count)?,
        )
    }
    fn names(&mut self, count: usize) -> Result<(), BudgetUnavailable> {
        add(
            &mut self.constructors,
            plan_hash_admission::tree_bound::<&str>(count)?,
        )
    }
    pub(super) fn record(
        &mut self,
        reader: &mut Reader<'_>,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        decode_record_ref(reader)?;
        let count = decode_len(reader, "record fields", crate::MAX_DECLARATIONS_PER_KIND)?;
        self.vector::<FieldSchema>(count)?;
        self.names(count)?;
        for _ in 0..count {
            reader.u32()?;
            self.name(reader, 256)?;
            add(&mut self.heap, row_policy_admission::type_heap(reader, 0)?)?;
        }
        Ok(())
    }
    fn key(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        add(&mut self.heap, term_admission::key_bound(reader, 0)?.heap)?;
        Ok(())
    }
    fn arena(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        let count = reader.peek_u32()? as usize;
        let bound = term_admission::expression_bound(reader, EXECUTABLE_IR_VERSION_V1)?;
        self.largest_arena = self.largest_arena.max(count);
        add(&mut self.heap, bound.heap)?;
        add(&mut self.constructors, bound.constructors)?;
        // Reachability validates one arena with at most 1,025 roots. Each
        // visited node pushes at most two children; vector growth is explicit.
        add(&mut self.constructors, allocation::<bool>(count)?)?;
        let pending = count
            .checked_mul(2)
            .and_then(|count| count.checked_add(1025))
            .ok_or(BudgetUnavailable)?;
        add(
            &mut self.constructors,
            allocation::<ExprId>(grow(pending)?)?,
        )?;
        add(&mut self.constructors, allocation::<ExprId>(grow(1025)?)?)?;
        Ok(())
    }
    fn invariants(
        &mut self,
        reader: &mut Reader<'_>,
        kind: &'static str,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        let count = decode_len(reader, kind, crate::MAX_DECLARATIONS_PER_KIND)?;
        self.invariants = self
            .invariants
            .checked_add(count)
            .ok_or(BudgetUnavailable)?;
        self.vector::<InvariantPlan>(count)?;
        self.names(count)?;
        for _ in 0..count {
            reader.u32()?;
            self.name(reader, 256)?;
            self.arena(reader)?;
            reader.u32()?;
        }
        Ok(())
    }
    fn entity(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader, 256)?;
        self.record(reader)?;
        let count = self.fields::<FieldId>(reader, "primary-key fields", 1024, 4)?;
        self.field_set(count)?;
        self.key(reader)?;
        self.invariants(reader, "entity invariants")?;
        let count = decode_len(reader, "entity indexes", crate::MAX_DECLARATIONS_PER_KIND)?;
        self.indexes = self.indexes.checked_add(count).ok_or(BudgetUnavailable)?;
        self.vector::<IndexSchema>(count)?;
        self.names(count)?;
        for _ in 0..count {
            reader.u32()?;
            self.name(reader, 256)?;
            let fields = self.fields::<FieldId>(reader, "index fields", 1024, 4)?;
            self.field_set(fields)?;
            self.key(reader)?;
            if reader.remaining() >= 4 && reader.peek_u32()? == INDEX_FIELD_ENCODING_EXTENSION {
                reader.u32()?;
                let encodings = decode_len(reader, "index field encodings", 1024)?;
                if encodings != fields {
                    return Err(IrValidationError::InvalidKey {
                        reason: "index field encoding arity mismatch",
                    }
                    .into());
                }
                // Result collection can grow; bound its temporary output capacity.
                self.vector::<IndexFieldEncodingV1>(grow(encodings)?)?;
                reader.read(encodings)?;
            } else {
                self.vector::<IndexFieldEncodingV1>(fields)?;
            }
            if reader.remaining() >= 4 && reader.peek_u32()? == INDEX_COVER_FIELDS_EXTENSION {
                reader.u32()?;
                let cover = self.fields::<FieldId>(reader, "index cover fields", 1024, 4)?;
                // The same constructor set keeps index fields while inserting
                // covered fields. Summed node bounds cover their joint tree.
                self.field_set(cover)?;
            }
        }
        Ok(())
    }
    fn event(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader, 256)?;
        self.record(reader)?;
        if reader.remaining() >= 8 && reader.peek_u64()? == EVENT_PARTITION_SCHEMA_EXTENSION {
            reader.u64()?;
            let count = self.fields::<FieldId>(reader, "event partition fields", 1024, 4)?;
            self.field_set(count)?;
            self.key(reader)?;
        }
        if reader.remaining() >= 8 && reader.peek_u64()? == EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION {
            reader.u64()?;
            reader.u32()?;
            self.name(reader, 256)?;
            let count = self.fields::<EventPolicyAnchorFieldV1>(
                reader,
                "event policy anchor fields",
                1024,
                8,
            )?;
            self.field_set(count)?;
        }
        Ok(())
    }
    fn enumeration(
        &mut self,
        reader: &mut Reader<'_>,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader, 256)?;
        let count = decode_len(reader, "enum variants", crate::MAX_DECLARATIONS_PER_KIND)?;
        self.vector::<EnumVariantSchema>(count)?;
        self.names(count)?;
        // Schema validation regenerates one enum's expected variant IDs.
        add(
            &mut self.constructors,
            allocation::<EnumVariantId>(grow(count)?)?,
        )?;
        for _ in 0..count {
            reader.u32()?;
            self.name(reader, 256)?;
        }
        Ok(())
    }
    fn aggregate(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader, 256)?;
        reader.u32()?;
        self.fields::<EntityTypeId>(
            reader,
            "aggregate children",
            crate::MAX_DECLARATIONS_PER_KIND,
            4,
        )?;
        self.arena(reader)?;
        reader.u32()?;
        self.fields::<ExprId>(reader, "aggregate conflict expressions", 1024, 4)?;
        self.key(reader)?;
        self.key(reader)?;
        self.invariants(reader, "aggregate invariants")?;
        Ok(())
    }
}

pub(super) struct SchemaSectionBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
}
impl SchemaSectionBound {
    pub(super) fn inspect(bytes: &[u8]) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        let mut walk = Walk::default();
        let entities = decode_len(&mut reader, "entities", crate::MAX_DECLARATIONS_PER_KIND)?;
        walk.vector::<EntitySchema>(entities)?;
        walk.names(entities)?;
        for _ in 0..entities {
            walk.entity(&mut reader)?;
        }
        let events = decode_len(&mut reader, "events", crate::MAX_DECLARATIONS_PER_KIND)?;
        walk.vector::<EventSchema>(events)?;
        walk.names(events)?;
        for _ in 0..events {
            walk.event(&mut reader)?;
        }
        let enums = decode_len(&mut reader, "enums", crate::MAX_DECLARATIONS_PER_KIND)?;
        walk.vector::<EnumSchema>(enums)?;
        walk.names(enums)?;
        for _ in 0..enums {
            walk.enumeration(&mut reader)?;
        }
        let aggregates = decode_len(&mut reader, "aggregates", crate::MAX_DECLARATIONS_PER_KIND)?;
        walk.vector::<AggregateSchema>(aggregates)?;
        walk.names(aggregates)?;
        for _ in 0..aggregates {
            walk.aggregate(&mut reader)?;
        }
        let relationships = walk
            .extension::<RelationshipSchema>(
                &mut reader,
                RELATIONSHIP_SCHEMA_EXTENSION,
                "relationships",
            )?
            .unwrap_or(0);
        for _ in 0..relationships {
            walk.name(&mut reader, 256)?;
            reader.u32()?;
            let source =
                walk.fields::<FieldId>(&mut reader, "relationship source fields", 1024, 4)?;
            walk.field_set(source)?;
            reader.u32()?;
            let target =
                walk.fields::<FieldId>(&mut reader, "relationship target fields", 1024, 4)?;
            walk.field_set(target)?;
        }
        let unique_keys = walk
            .extension::<UniqueKeySchema>(&mut reader, UNIQUE_KEY_SCHEMA_EXTENSION, "unique keys")?
            .unwrap_or(0);
        for _ in 0..unique_keys {
            walk.name(&mut reader, 256)?;
            reader.read(8)?;
            let count = walk.fields::<FieldId>(&mut reader, "unique key fields", 1024, 4)?;
            walk.field_set(count)?;
        }
        if let Some(count) = walk.extension::<DeletePolicySchemaV1>(
            &mut reader,
            DELETE_POLICY_SCHEMA_EXTENSION,
            "delete policies",
        )? {
            for _ in 0..count {
                reader.u32()?;
                match reader.u8()? {
                    crate::format_registry::delete_policy_mode::NO_INBOUND => {}
                    crate::format_registry::delete_policy_mode::RESTRICT => {
                        reader.read(8)?;
                    }
                    crate::format_registry::delete_policy_mode::CASCADE => {
                        let count = decode_len(&mut reader, "cascade relationships", 32)?;
                        walk.vector::<CascadeRelationshipSpecV1>(count)?;
                        for _ in 0..count {
                            reader.u32()?;
                            walk.name(&mut reader, 256)?;
                            reader.read(8)?;
                        }
                    }
                    tag => {
                        return Err(IrValidationError::UnknownTag {
                            kind: "delete policy mode",
                            tag,
                        }
                        .into());
                    }
                }
            }
        }
        if let Some(count) = walk.extension::<VectorFieldSpecV1>(
            &mut reader,
            VECTOR_FIELD_SPEC_SCHEMA_EXTENSION,
            "vector field specs",
        )? {
            for _ in 0..count {
                reader.read(9)?;
                walk.fields::<FieldId>(
                    &mut reader,
                    "vector spec source fields",
                    crate::schema::MAX_VECTOR_SOURCE_FIELDS,
                    4,
                )?;
                reader.u64()?;
            }
        }
        if let Some(count) = walk.extension::<SecretFieldSpecV1>(
            &mut reader,
            SECRET_FIELD_SPEC_SCHEMA_EXTENSION,
            "secret field specs",
        )? {
            reader.read(count.checked_mul(8).ok_or(BudgetUnavailable)?)?;
        }
        if let Some(count) = walk.extension::<VectorAnnSpecV1>(
            &mut reader,
            VECTOR_ANN_SPEC_SCHEMA_EXTENSION,
            "vector ANN specs",
        )? {
            reader.read(count.checked_mul(16).ok_or(BudgetUnavailable)?)?;
        }
        if let Some(count) = walk.extension::<VectorProductionSpecV1>(
            &mut reader,
            VECTOR_PRODUCTION_SPEC_SCHEMA_EXTENSION,
            "vector production specs",
        )? {
            for _ in 0..count {
                reader.read(8)?;
                walk.name(
                    &mut reader,
                    riffdb_types::EmbeddingMetadata::MAX_MODEL_STRING_LEN,
                )?;
                walk.name(
                    &mut reader,
                    riffdb_types::EmbeddingMetadata::MAX_MODEL_STRING_LEN,
                )?;
                reader.read(24)?;
            }
        }
        if let Some(count) = walk.extension::<TextIndexSpecV1>(
            &mut reader,
            TEXT_INDEX_SPEC_SCHEMA_EXTENSION,
            "text index specs",
        )? {
            for _ in 0..count {
                reader.read(9)?;
                walk.fields::<TextIndexSourceV1>(
                    &mut reader,
                    "text index source fields",
                    crate::MAX_TEXT_INDEX_SOURCE_FIELDS,
                    8,
                )?;
                reader.read(4 + 24 + 1 + 12)?;
            }
        }
        if let Some(count) = walk.extension::<LongPatternSpecV1>(
            &mut reader,
            LONG_PATTERN_SPEC_SCHEMA_EXTENSION,
            "long pattern specs",
        )? {
            for _ in 0..count {
                reader.read(8)?;
                walk.name(&mut reader, 256)?;
                reader.read(5)?;
                walk.fields::<riffdb_types::LongPatternOperatorV1>(
                    &mut reader,
                    "long pattern operators",
                    8,
                    1,
                )?;
                reader.read(10 * 4 + 4 * 8 + 3 * 8 + 2 * 4)?;
            }
        }
        reader.finish()?;

        // The ordinary whole-schema constructor retains these forests while
        // checking ownership, relationships, delete policies and enum registries.
        add(
            &mut walk.constructors,
            plan_hash_admission::tree_bound::<IndexId>(walk.indexes)?,
        )?;
        add(
            &mut walk.constructors,
            plan_hash_admission::tree_bound::<InvariantId>(walk.invariants)?,
        )?;
        add(
            &mut walk.constructors,
            plan_hash_admission::tree_layout_bound::<EntityTypeId, &EntitySchema>(entities)?,
        )?;
        add(
            &mut walk.constructors,
            allocation::<(EntityTypeId, &EntitySchema)>(grow(entities)?)?,
        )?;
        add(
            &mut walk.constructors,
            plan_hash_admission::tree_layout_bound::<EntityTypeId, AggregateTypeId>(entities)?,
        )?;
        // One root's primary-key set (including collection-builder input).
        walk.field_set(1024)?;
        add(&mut walk.constructors, allocation::<FieldId>(grow(1024)?)?)?;
        // Delete validation can retain all inbound references; route validation
        // retains a field set, visited arena and growing pending/route vectors.
        add(
            &mut walk.constructors,
            allocation::<&RelationshipSchema>(grow(relationships)?)?,
        )?;
        if relationships > 0 || unique_keys > 0 {
            walk.field_set(1024)?;
            add(
                &mut walk.constructors,
                plan_hash_admission::tree_bound::<ExprId>(walk.largest_arena)?,
            )?;
            add(
                &mut walk.constructors,
                allocation::<ExprId>(grow(
                    walk.largest_arena
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(1))
                        .ok_or(BudgetUnavailable)?,
                )?)?,
            )?;
            add(&mut walk.constructors, allocation::<FieldId>(grow(1024)?)?)?;
        }
        if relationships > 0 {
            // Pair traversal may visit a product of DAG nodes. The ordinary
            // insert precedes its limit check, so also admit the rejecting node.
            let pairs = walk
                .largest_arena
                .checked_mul(walk.largest_arena)
                .ok_or(BudgetUnavailable)?
                .min(crate::MAX_EXPRESSION_NODES + 1);
            add(
                &mut walk.constructors,
                plan_hash_admission::tree_bound::<(ExprId, ExprId)>(pairs)?,
            )?;
            add(
                &mut walk.constructors,
                allocation::<(ExprId, ExprId)>(grow(
                    pairs
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(1))
                        .ok_or(BudgetUnavailable)?,
                )?)?,
            )?;
        }
        // Text attachment builds a second ordinary-index set after the initial
        // constructor returns; summing both is conservative and layout-derived.
        add(
            &mut walk.constructors,
            plan_hash_admission::tree_bound::<IndexId>(walk.indexes)?,
        )?;
        add(
            &mut walk.constructors,
            allocation::<IndexId>(grow(walk.indexes)?)?,
        )?;
        Ok(Self {
            heap: walk.heap,
            constructors: walk.constructors,
        })
    }
    pub(super) fn maximum(&self, encoded_len: usize) -> Result<u64, BudgetUnavailable> {
        term_admission::TermBound {
            heap: self.heap,
            constructors: self.constructors,
        }
        .maximum(encoded_len)
    }
}

/// First-party structural fragment admission, preserving all ordinary schema
/// checks. Input ownership, executable plans and complete identity are separate.
#[doc(hidden)]
pub fn decode_provider_structural_schema_admitted_v1(
    bytes: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<SchemaIr>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = SchemaSectionBound::inspect(bytes)?;
    let work = budget.reserve(bound.maximum(bytes.len())?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let value = decode_schema(&mut reader)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_schema(&mut writer, &value)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(value).map_err(Into::into)
}
