//! Catalog-owned symbolic materialization of authoritative domain events.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{FieldSchema, Instruction, SchemaIr, ValueType, ValueTypeTag};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, ExecutablePlanRef, StoredDurableEventV1, StoredEventRouteV1,
};
use riffdb_types::{
    CanonicalValue, ContractVersion, EventId, EventTypeId, PartitionKey, PartitionKeyHash,
    PlanHash, hash_partition_key,
};

use crate::lineage::{LineageMaterializationProof, RecordOwnerV1, WriterRelation};
use crate::materialization::validate_static_value;
use crate::{ActiveCatalogSnapshot, ContractEnumVariantNames, ValidatedContractBundle};

/// Maximum explicitly selected payload fields in one application event view.
pub const MAX_EVENT_MATERIALIZATION_FIELDS: usize = 256;

/// One symbolic event field without a stable numeric identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicEventFieldDescriptor {
    name: String,
    value_type: String,
}

impl SymbolicEventFieldDescriptor {
    /// Borrows the exact contract field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrows the canonical contract-language type spelling.
    #[must_use]
    pub fn value_type(&self) -> &str {
        &self.value_type
    }
}

/// One active symbolic event descriptor safe for public service shaping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicEventDescriptor {
    event_name: String,
    partition_fields: Vec<SymbolicEventFieldDescriptor>,
    payload_fields: Vec<SymbolicEventFieldDescriptor>,
}

impl SymbolicEventDescriptor {
    /// Borrows the exact event name.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Returns whether compiler-proved application routing is available.
    #[must_use]
    pub fn application_streamable(&self) -> bool {
        !self.partition_fields.is_empty()
    }

    /// Borrows the ordered compiler-proved partition field tuple.
    #[must_use]
    pub fn partition_fields(&self) -> &[SymbolicEventFieldDescriptor] {
        &self.partition_fields
    }

    /// Borrows every payload field in stable schema order.
    #[must_use]
    pub fn payload_fields(&self) -> &[SymbolicEventFieldDescriptor] {
        &self.payload_fields
    }
}

/// Stable classification for symbolic event-materialization failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventMaterializationErrorKind {
    /// The requested event or payload field is not present in the active contract.
    UnknownSymbol,
    /// The event exists but has no compiler-proved application partition.
    NotStreamable,
    /// Event, route, writer, lineage, schema, or payload evidence was inconsistent.
    Integrity,
    /// The requested selection exceeded a fixed semantic limit.
    HardLimit,
}

impl EventMaterializationErrorKind {
    /// Returns fixed safe text without event, field, plan, or payload details.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::UnknownSymbol => "event materialization symbol is unavailable",
            Self::NotStreamable => "event is not application streamable",
            Self::Integrity => "event materialization integrity failure",
            Self::HardLimit => "event materialization exceeds the hard limit",
        }
    }
}

/// A typed, redaction-safe event-materialization failure.
#[derive(Clone, Eq, PartialEq)]
pub struct EventMaterializationError {
    kind: EventMaterializationErrorKind,
}

impl EventMaterializationError {
    const fn new(kind: EventMaterializationErrorKind) -> Self {
        Self { kind }
    }

    const fn unknown_symbol() -> Self {
        Self::new(EventMaterializationErrorKind::UnknownSymbol)
    }

    const fn integrity() -> Self {
        Self::new(EventMaterializationErrorKind::Integrity)
    }

    const fn not_streamable() -> Self {
        Self::new(EventMaterializationErrorKind::NotStreamable)
    }

    const fn hard_limit() -> Self {
        Self::new(EventMaterializationErrorKind::HardLimit)
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> EventMaterializationErrorKind {
        self.kind
    }
}

impl fmt::Debug for EventMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventMaterializationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for EventMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for EventMaterializationError {}

#[derive(Clone)]
struct SelectedEventField {
    schema: FieldSchema,
}

/// An active-contract symbolic event selection with historical lineage proof.
///
/// Stable numeric identities remain private. Application-facing code supplies
/// and receives contract symbols only.
#[derive(Clone)]
pub struct ResolvedEventMaterializer {
    event_name: String,
    event_type_id: riffdb_types::EventTypeId,
    selected_fields: Vec<SelectedEventField>,
    active_bundle: ValidatedContractBundle,
    lineage_proof: Arc<LineageMaterializationProof>,
    active_ordinal: u16,
}

impl ResolvedEventMaterializer {
    /// Returns the exact active-contract event symbol.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Returns selected payload symbols in the caller-declared order.
    pub fn selected_field_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.selected_fields.iter().map(|field| field.schema.name())
    }

    pub(crate) fn event_type_id(&self) -> riffdb_types::EventTypeId {
        self.event_type_id
    }

    pub(crate) fn policy_anchor_entity(&self) -> Option<riffdb_types::EntityTypeId> {
        self.active_bundle
            .bundle()
            .schema()
            .event(self.event_type_id)
            .and_then(|event| event.policy_anchor())
            .map(|anchor| anchor.source_entity())
    }

    pub(crate) fn derive_partition_hash<'a>(
        &self,
        supplied: impl IntoIterator<Item = (&'a str, CanonicalValue)>,
    ) -> Result<PartitionKeyHash, EventMaterializationError> {
        self.derive_partition_key(supplied)
            .map(|key| hash_partition_key(key.as_bytes()))
    }

    pub(crate) fn derive_partition_key<'a>(
        &self,
        supplied: impl IntoIterator<Item = (&'a str, CanonicalValue)>,
    ) -> Result<PartitionKey, EventMaterializationError> {
        let active_event = self
            .active_bundle
            .bundle()
            .schema()
            .event(self.event_type_id)
            .ok_or_else(EventMaterializationError::integrity)?;
        let partition = active_event
            .partition()
            .ok_or_else(EventMaterializationError::not_streamable)?;
        let supplied = supplied.into_iter().collect::<Vec<_>>();
        if supplied.len() != partition.fields().len()
            || supplied
                .iter()
                .enumerate()
                .any(|(index, (name, _))| supplied[..index].iter().any(|(prior, _)| prior == name))
        {
            return Err(EventMaterializationError::unknown_symbol());
        }
        let values = partition
            .fields()
            .iter()
            .map(|field_id| {
                let field = active_event
                    .payload()
                    .field(*field_id)
                    .ok_or_else(EventMaterializationError::integrity)?;
                let value = supplied
                    .iter()
                    .find(|(name, _)| *name == field.name())
                    .map(|(_, value)| value)
                    .ok_or_else(EventMaterializationError::unknown_symbol)?;
                validate_static_value(
                    self.active_bundle.bundle().schema(),
                    field.value_type(),
                    value,
                )
                .map_err(|_| EventMaterializationError::integrity())?;
                Ok(value.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = partition
            .key_schema()
            .encode_partition(&values)
            .map_err(|_| EventMaterializationError::integrity())?;
        Ok(key)
    }

    /// Validates and symbolically materializes one immutable routed event.
    ///
    /// The returned view owns only selected values. Unknown source fields and
    /// the complete durable payload remain inaccessible.
    pub fn materialize_routed_event(
        &self,
        writer: &ExecutablePlanRef,
        route_partition_hash: PartitionKeyHash,
        route: StoredEventRouteV1,
        event: &StoredDurableEventV1,
    ) -> Result<SymbolicEventView, EventMaterializationError> {
        if route.event_id() != event.event_id()
            || route.event_type_id() != event.event_type_id()
            || route.event_hash() != event.event_hash()
        {
            return Err(EventMaterializationError::integrity());
        }
        let (writer_ordinal, writer_bundle) = self
            .lineage_proof
            .exact_member(writer.contract_version(), writer.contract_bundle_hash())
            .filter(|(_, bundle)| bundle.lineage() == writer.contract_lineage())
            .ok_or_else(EventMaterializationError::integrity)?;
        let writer_plan = writer_bundle
            .resolve_plan_with_proof(writer, Arc::clone(&self.lineage_proof), writer_ordinal)
            .map_err(|_| EventMaterializationError::integrity())?;

        if event.event_type_id() != self.event_type_id
            || !writer_plan.plan().instructions().iter().any(|instruction| {
                matches!(
                    instruction,
                    Instruction::EmitEvent(construction)
                        if construction.event_type() == event.event_type_id()
                )
            })
        {
            return Err(EventMaterializationError::integrity());
        }

        let writer_schema = writer_bundle
            .bundle()
            .schema()
            .event(event.event_type_id())
            .ok_or_else(EventMaterializationError::integrity)?;
        validate_complete_event_payload(writer_bundle, writer_schema.payload(), event)?;

        let writer_binding = DurableKeySchemaBindingV1::from_plan(writer);
        let (relation, null_fill) = self
            .lineage_proof
            .writer_materialization(
                RecordOwnerV1::Event(self.event_type_id),
                &writer_binding,
                self.active_ordinal,
            )
            .map_err(|_| EventMaterializationError::integrity())?;
        let active_schema = self
            .active_bundle
            .bundle()
            .schema()
            .event(self.event_type_id)
            .ok_or_else(EventMaterializationError::integrity)?;
        let partition = active_schema
            .partition()
            .ok_or_else(EventMaterializationError::integrity)?;
        if null_fill
            .as_ref()
            .is_some_and(|mask| !mask.has_canonical_shape(active_schema.payload().fields().len()))
        {
            return Err(EventMaterializationError::integrity());
        }

        let active_value = |field: &FieldSchema| {
            let position = active_schema
                .payload()
                .fields()
                .binary_search_by_key(&field.id(), FieldSchema::id)
                .map_err(|_| EventMaterializationError::integrity())?;
            let source_value = event
                .payload()
                .fields()
                .binary_search_by_key(&field.id(), |(field_id, _)| *field_id)
                .ok()
                .map(|index| &event.payload().fields()[index].1);
            match source_value {
                Some(value) => {
                    validate_static_value(
                        self.active_bundle.bundle().schema(),
                        field.value_type(),
                        value,
                    )
                    .map_err(|_| EventMaterializationError::integrity())?;
                    Ok(value.clone())
                }
                None if relation == WriterRelation::Ancestor
                    && null_fill.as_ref().is_some_and(|mask| mask.allows(position))
                    && field.value_type().is_optional() =>
                {
                    Ok(CanonicalValue::Null)
                }
                None => Err(EventMaterializationError::integrity()),
            }
        };

        let partition_values = partition
            .fields()
            .iter()
            .map(|field_id| {
                let field = active_schema
                    .payload()
                    .field(*field_id)
                    .ok_or_else(EventMaterializationError::integrity)?;
                active_value(field)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoded_partition = partition
            .key_schema()
            .encode_partition(&partition_values)
            .map_err(|_| EventMaterializationError::integrity())?;
        if hash_partition_key(encoded_partition.as_bytes()) != route_partition_hash {
            return Err(EventMaterializationError::integrity());
        }

        let mut fields = Vec::with_capacity(self.selected_fields.len());
        for selected in &self.selected_fields {
            let value = active_value(&selected.schema)?;
            fields.push(SymbolicEventField {
                name: selected.schema.name().to_owned(),
                value,
            });
        }

        Ok(SymbolicEventView {
            event_id: event.event_id(),
            event_type_id: event.event_type_id(),
            event_name: self.event_name.clone(),
            writer_contract_version: writer.contract_version(),
            writer_plan_hash: writer.command_plan_hash(),
            command_name: writer_plan.plan().name().to_owned(),
            fields,
            enum_variant_names: Arc::clone(self.active_bundle.enum_variant_names()),
        })
    }
}

impl fmt::Debug for ResolvedEventMaterializer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedEventMaterializer")
            .field("event", &self.event_name)
            .field("selected_field_count", &self.selected_fields.len())
            .field("lineage", &"[CHECKED]")
            .finish()
    }
}

/// One selected symbolic payload field.
#[derive(Clone, Eq, PartialEq)]
pub struct SymbolicEventField {
    name: String,
    value: CanonicalValue,
}

impl SymbolicEventField {
    /// Returns the exact contract field symbol.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrows the selected, type-checked canonical value.
    #[must_use]
    pub const fn value(&self) -> &CanonicalValue {
        &self.value
    }
}

impl fmt::Debug for SymbolicEventField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SymbolicEventField")
            .field("name", &self.name)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Move-owned safe symbolic projection of one authoritative event.
///
/// It deliberately contains no raw principal/session identity, partition or
/// conflict key, unselected payload, credential, or process-local trace ID.
pub struct SymbolicEventView {
    event_id: EventId,
    event_type_id: EventTypeId,
    event_name: String,
    writer_contract_version: ContractVersion,
    writer_plan_hash: PlanHash,
    command_name: String,
    fields: Vec<SymbolicEventField>,
    enum_variant_names: ContractEnumVariantNames,
}

impl SymbolicEventView {
    /// Returns the unchanged authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Stable event type retained for policy-anchor verification.
    #[doc(hidden)]
    #[must_use]
    pub const fn event_type_id(&self) -> EventTypeId {
        self.event_type_id
    }

    /// Returns the symbolic event type.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Returns the originating contract version; no separate event clock exists.
    #[must_use]
    pub const fn writer_contract_version(&self) -> ContractVersion {
        self.writer_contract_version
    }

    /// Returns the exact originating command-plan identity.
    #[must_use]
    pub const fn writer_plan_hash(&self) -> PlanHash {
        self.writer_plan_hash
    }

    /// Returns the symbolic originating command.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }

    /// Borrows only the explicitly selected symbolic payload fields.
    #[must_use]
    pub fn fields(&self) -> &[SymbolicEventField] {
        &self.fields
    }

    /// Resolves one canonical enum identity through the active contract schema.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.enum_variant_names
            .get(&(type_id, variant_id))
            .map(String::as_str)
    }

    /// Borrows the shared enum display-name table for protocol presentation.
    #[must_use]
    pub const fn enum_variant_names(&self) -> &ContractEnumVariantNames {
        &self.enum_variant_names
    }
}

impl fmt::Debug for SymbolicEventView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SymbolicEventView([REDACTED])")
    }
}

impl ActiveCatalogSnapshot {
    /// Describes one active event using only symbolic names and type spellings.
    #[must_use]
    pub fn describe_event(&self, event_name: &str) -> Option<SymbolicEventDescriptor> {
        let schema = self.bundle().bundle().schema();
        let event = schema
            .events()
            .iter()
            .find(|event| event.name() == event_name)?;
        let payload_fields = event
            .payload()
            .fields()
            .iter()
            .map(|field| symbolic_field_descriptor(schema, field))
            .collect::<Option<Vec<_>>>()?;
        let partition_fields = match event.partition() {
            Some(partition) => partition
                .fields()
                .iter()
                .map(|field_id| {
                    event
                        .payload()
                        .field(*field_id)
                        .and_then(|field| symbolic_field_descriptor(schema, field))
                })
                .collect::<Option<Vec<_>>>()?,
            None => Vec::new(),
        };
        Some(SymbolicEventDescriptor {
            event_name: event.name().to_owned(),
            partition_fields,
            payload_fields,
        })
    }

    /// Resolves one event and an explicit nonempty payload selection by symbol.
    pub fn resolve_event_materializer<'a>(
        &self,
        event_name: &str,
        selected_field_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<ResolvedEventMaterializer, EventMaterializationError> {
        let event = self
            .bundle()
            .bundle()
            .schema()
            .events()
            .iter()
            .find(|event| event.name() == event_name)
            .ok_or_else(EventMaterializationError::unknown_symbol)?;
        if event.partition().is_none() {
            return Err(EventMaterializationError::not_streamable());
        }
        let names = selected_field_names.into_iter().collect::<Vec<_>>();
        if names.is_empty() || names.len() > MAX_EVENT_MATERIALIZATION_FIELDS {
            return Err(EventMaterializationError::hard_limit());
        }
        let mut selected_fields = Vec::with_capacity(names.len());
        for (index, name) in names.iter().enumerate() {
            if names[..index].contains(name) {
                return Err(EventMaterializationError::unknown_symbol());
            }
            let schema = event
                .payload()
                .fields()
                .iter()
                .find(|field| field.name() == *name)
                .cloned()
                .ok_or_else(EventMaterializationError::unknown_symbol)?;
            selected_fields.push(SelectedEventField { schema });
        }
        let active_ordinal = u16::try_from(self.lineage_proof().bundle_count() - 1)
            .map_err(|_| EventMaterializationError::hard_limit())?;
        Ok(ResolvedEventMaterializer {
            event_name: event.name().to_owned(),
            event_type_id: event.id(),
            selected_fields,
            active_bundle: self.bundle().clone(),
            lineage_proof: Arc::clone(self.lineage_proof()),
            active_ordinal,
        })
    }
}

fn symbolic_field_descriptor(
    schema: &SchemaIr,
    field: &FieldSchema,
) -> Option<SymbolicEventFieldDescriptor> {
    Some(SymbolicEventFieldDescriptor {
        name: field.name().to_owned(),
        value_type: render_value_type(schema, field.value_type())?,
    })
}

fn render_value_type(schema: &SchemaIr, value: &ValueType) -> Option<String> {
    Some(match value.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value.decimal_spec()?;
            format!("decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => format!("money<{}>", value.currency()?),
        ValueTypeTag::String => format!("string<{}>", value.byte_bound()?),
        ValueTypeTag::Bytes => format!("bytes<{}>", value.byte_bound()?),
        ValueTypeTag::Timestamp => "timestamp".to_owned(),
        ValueTypeTag::Date => "date".to_owned(),
        ValueTypeTag::Uuid => "uuid".to_owned(),
        ValueTypeTag::Enum => schema.enumeration(value.enum_type_id()?)?.name().to_owned(),
        ValueTypeTag::Optional => {
            format!("{}?", render_value_type(schema, value.optional_inner()?)?)
        }
        ValueTypeTag::List => {
            let (element, maximum) = value.list_parts()?;
            format!("[{}; {maximum}]", render_value_type(schema, element)?)
        }
        ValueTypeTag::Record => "record".to_owned(),
        ValueTypeTag::Vector => format!("vector<{}>", value.vector_dimension()?.get()),
    })
}

fn validate_complete_event_payload(
    writer_bundle: &ValidatedContractBundle,
    record_schema: &riffdb_contract_ir::RecordSchema,
    event: &StoredDurableEventV1,
) -> Result<(), EventMaterializationError> {
    if record_schema.fields().len() != event.payload().fields().len() {
        return Err(EventMaterializationError::integrity());
    }
    for (schema_field, (field_id, value)) in
        record_schema.fields().iter().zip(event.payload().fields())
    {
        if schema_field.id() != *field_id {
            return Err(EventMaterializationError::integrity());
        }
        validate_static_value(
            writer_bundle.bundle().schema(),
            schema_field.value_type(),
            value,
        )
        .map_err(|_| EventMaterializationError::integrity())?;
    }
    Ok(())
}
