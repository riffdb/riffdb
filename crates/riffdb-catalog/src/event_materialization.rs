//! Catalog-owned symbolic materialization of authoritative domain events.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{FieldSchema, Instruction};
use riffdb_storage_api::{DurableKeySchemaBindingV1, ExecutablePlanRef, StoredDurableEventV1};
use riffdb_types::{CanonicalValue, ContractVersion, EventId, PlanHash};

use crate::lineage::{LineageMaterializationProof, RecordOwnerV1, WriterRelation};
use crate::materialization::validate_static_value;
use crate::{ActiveCatalogSnapshot, ValidatedContractBundle};

/// Maximum explicitly selected payload fields in one application event view.
pub const MAX_EVENT_MATERIALIZATION_FIELDS: usize = 256;

/// Stable classification for symbolic event-materialization failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventMaterializationErrorKind {
    /// The requested event or payload field is not present in the active contract.
    UnknownSymbol,
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

    /// Validates and symbolically materializes one immutable historical event.
    ///
    /// The returned view owns only selected values. Unknown source fields and
    /// the complete durable payload remain inaccessible.
    pub fn materialize_event(
        &self,
        writer: &ExecutablePlanRef,
        event: &StoredDurableEventV1,
    ) -> Result<SymbolicEventView, EventMaterializationError> {
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
        if null_fill
            .as_ref()
            .is_some_and(|mask| !mask.has_canonical_shape(active_schema.payload().fields().len()))
        {
            return Err(EventMaterializationError::integrity());
        }

        let mut fields = Vec::with_capacity(self.selected_fields.len());
        for selected in &self.selected_fields {
            let position = active_schema
                .payload()
                .fields()
                .binary_search_by_key(&selected.schema.id(), FieldSchema::id)
                .map_err(|_| EventMaterializationError::integrity())?;
            let source_value = event
                .payload()
                .fields()
                .binary_search_by_key(&selected.schema.id(), |(field_id, _)| *field_id)
                .ok()
                .map(|index| &event.payload().fields()[index].1);
            let value = match source_value {
                Some(value) => {
                    validate_static_value(
                        self.active_bundle.bundle().schema(),
                        selected.schema.value_type(),
                        value,
                    )
                    .map_err(|_| EventMaterializationError::integrity())?;
                    value.clone()
                }
                None if relation == WriterRelation::Ancestor
                    && null_fill.as_ref().is_some_and(|mask| mask.allows(position))
                    && selected.schema.value_type().is_optional() =>
                {
                    CanonicalValue::Null
                }
                None => return Err(EventMaterializationError::integrity()),
            };
            fields.push(SymbolicEventField {
                name: selected.schema.name().to_owned(),
                value,
            });
        }

        Ok(SymbolicEventView {
            event_id: event.event_id(),
            event_name: self.event_name.clone(),
            writer_contract_version: writer.contract_version(),
            writer_plan_hash: writer.command_plan_hash(),
            command_name: writer_plan.plan().name().to_owned(),
            fields,
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
    event_name: String,
    writer_contract_version: ContractVersion,
    writer_plan_hash: PlanHash,
    command_name: String,
    fields: Vec<SymbolicEventField>,
}

impl SymbolicEventView {
    /// Returns the unchanged authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
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
}

impl fmt::Debug for SymbolicEventView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SymbolicEventView([REDACTED])")
    }
}

impl ActiveCatalogSnapshot {
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
