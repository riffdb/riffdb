//! Deterministic contract compatibility metadata and IR-owned comparison.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{EntityTypeId, FieldId};

use crate::format_registry::{
    enum_variant_owner as variant_owner_tag, index_owner as index_owner_tag,
    invariant_owner as invariant_owner_tag, outcome_owner as outcome_owner_tag,
    record_owner as record_owner_tag,
};
use crate::{
    BindingPlan, CommandPlan, ContractBundle, EventConstruction, ExpressionArena, ExpressionKind,
    FieldExpression, Instruction, IrValidationError, LineageEntryState, McpCommandNameRegistryV2,
    OutcomeConstruction, ProjectionPlan, RecordSchema, RecordTypeRef, SchemaIr, StableIdNamespace,
    StableIdNamespaceTag, StableIdentity, ValueType, checked_len,
};

/// Closed compatibility classes from least to most restrictive.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum CompatibilityClass {
    /// Safe under the active POC additive policy.
    Compatible = crate::format_registry::compatibility_class::COMPATIBLE,
    /// Requires every caller to select the new application version explicitly.
    RequiresExplicitVersion =
        crate::format_registry::compatibility_class::REQUIRES_EXPLICIT_VERSION,
    /// Not activatable under the POC policy.
    Incompatible = crate::format_registry::compatibility_class::INCOMPATIBLE,
}

/// Stable compatibility finding codes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum CompatibilityCode {
    /// No semantic change.
    NoSemanticChange,
    /// Added command.
    AddedCommand,
    /// Added event.
    AddedEvent,
    /// Added projection.
    AddedProjection,
    /// Added optional field.
    AddedOptionalField,
    /// Added enum.
    AddedEnum,
    /// Added entity.
    AddedEntity,
    /// Added aggregate containing only newly added entities.
    AddedAggregate,
    /// Added outcome requiring explicit version selection.
    AddedOutcome,
    /// Added optional outcome field requiring explicit version selection.
    AddedOptionalOutcomeField,
    /// Added variant to an existing enum, requiring explicit version selection.
    AddedEnumVariant,
    /// Removed stable identity.
    RemovedIdentity,
    /// Attempted tombstone resurrection.
    TombstoneResurrection,
    /// Reused stable ID.
    IdReuse,
    /// Changed exact type.
    TypeChange,
    /// Changed key layout.
    KeyLayoutChange,
    /// Changed idempotency identity.
    IdempotencyChange,
    /// Changed partition or conflict derivation.
    PartitionConflictChange,
    /// Changed invariant.
    InvariantChange,
    /// Changed outcome.
    OutcomeChange,
    /// Changed event.
    EventChange,
    /// Changed an existing executable plan.
    ExistingPlanChange,
    /// Unsupported semantic addition.
    UnsupportedAddition,
    /// Changed executable IR version.
    IrVersionChange,
}

impl CompatibilityCode {
    pub(crate) const ALL: [Self; 24] = [
        Self::NoSemanticChange,
        Self::AddedCommand,
        Self::AddedEvent,
        Self::AddedProjection,
        Self::AddedOptionalField,
        Self::AddedEnum,
        Self::AddedEntity,
        Self::AddedAggregate,
        Self::AddedOutcome,
        Self::AddedOptionalOutcomeField,
        Self::AddedEnumVariant,
        Self::RemovedIdentity,
        Self::TombstoneResurrection,
        Self::IdReuse,
        Self::TypeChange,
        Self::KeyLayoutChange,
        Self::IdempotencyChange,
        Self::PartitionConflictChange,
        Self::InvariantChange,
        Self::OutcomeChange,
        Self::EventChange,
        Self::ExistingPlanChange,
        Self::UnsupportedAddition,
        Self::IrVersionChange,
    ];

    fn format(self) -> &'static crate::format_registry::CompatibilityCodeFormat {
        &crate::format_registry::COMPATIBILITY_CODES[self as usize]
    }

    pub(crate) fn from_code(code: &str) -> Option<Self> {
        crate::format_registry::COMPATIBILITY_CODES
            .iter()
            .position(|entry| entry.code == code)
            .and_then(|index| Self::ALL.get(index).copied())
    }

    /// Immutable public code.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.format().code
    }

    /// Required class for this code.
    #[must_use]
    pub fn class(self) -> CompatibilityClass {
        match self.format().class_tag {
            crate::format_registry::compatibility_class::COMPATIBLE => {
                CompatibilityClass::Compatible
            }
            crate::format_registry::compatibility_class::REQUIRES_EXPLICIT_VERSION => {
                CompatibilityClass::RequiresExplicitVersion
            }
            crate::format_registry::compatibility_class::INCOMPATIBLE => {
                CompatibilityClass::Incompatible
            }
            _ => unreachable!("closed compatibility registry uses a registered class"),
        }
    }
}

/// One canonical compatibility finding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityEntry {
    code: CompatibilityCode,
    affected_path: String,
    path_key: StableAffectedPath,
}

impl CompatibilityEntry {
    /// Creates a bounded stable-path finding.
    pub fn new(
        code: CompatibilityCode,
        affected_path: impl Into<String>,
    ) -> Result<Self, IrValidationError> {
        let affected_path = affected_path.into();
        if affected_path.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "compatibility path",
            });
        }
        checked_len("compatibility path", affected_path.len(), 1_024)?;
        if !affected_path.is_ascii() {
            return Err(IrValidationError::InvalidText {
                kind: "compatibility path",
            });
        }
        let path_key = StableAffectedPath::parse(&affected_path)?;
        Ok(Self {
            code,
            affected_path,
            path_key,
        })
    }

    /// Stable finding code.
    #[must_use]
    pub const fn code(&self) -> CompatibilityCode {
        self.code
    }
    /// Required class.
    #[must_use]
    pub fn class(&self) -> CompatibilityClass {
        self.code.class()
    }
    /// Stable bounded affected path.
    #[must_use]
    pub fn affected_path(&self) -> &str {
        &self.affected_path
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StableAffectedPath {
    segments: Vec<StablePathSegment>,
}

impl StableAffectedPath {
    fn parse(path: &str) -> Result<Self, IrValidationError> {
        if path == "contract" {
            return Ok(Self {
                segments: vec![StablePathSegment::literal("contract")],
            });
        }
        let parts = path.split('/').collect::<Vec<_>>();
        let invalid = || IrValidationError::InvalidText {
            kind: "compatibility path",
        };
        let Some(root) = parts.first() else {
            return Err(invalid());
        };
        let (root_kind, root_id) = parse_stable_path_id(root).ok_or_else(invalid)?;
        if !matches!(
            root_kind,
            "aggregate" | "command" | "entity" | "enum" | "event" | "projection"
        ) {
            return Err(invalid());
        }

        let mut segments = vec![StablePathSegment::stable_id(root_kind, root_id)];
        match (root_kind, parts.as_slice()) {
            (_, [_]) => {}
            ("aggregate", [_, child]) => {
                push_expected_stable_segment(&mut segments, child, "invariant")?;
            }
            ("entity", [_, child]) => {
                let (kind, id) = parse_stable_path_id(child).ok_or_else(invalid)?;
                if !matches!(kind, "field" | "index" | "invariant") {
                    return Err(invalid());
                }
                segments.push(StablePathSegment::stable_id(kind, id));
            }
            ("enum", [_, child]) => {
                push_expected_stable_segment(&mut segments, child, "variant")?;
            }
            ("event" | "projection", [_, child]) => {
                push_expected_stable_segment(&mut segments, child, "field")?;
            }
            ("command", [_, "input", child]) => {
                segments.push(StablePathSegment::literal("input"));
                push_expected_stable_segment(&mut segments, child, "field")?;
            }
            ("command", [_, outcome]) => {
                push_expected_stable_segment(&mut segments, outcome, "outcome")?;
            }
            ("command", [_, outcome, field]) => {
                push_expected_stable_segment(&mut segments, outcome, "outcome")?;
                push_expected_stable_segment(&mut segments, field, "field")?;
            }
            _ => return Err(invalid()),
        }
        Ok(Self { segments })
    }
}

impl Ord for StableAffectedPath {
    fn cmp(&self, other: &Self) -> Ordering {
        self.segments.cmp(&other.segments)
    }
}

impl PartialOrd for StableAffectedPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StablePathSegment {
    kind: &'static str,
    stable_id: Option<u32>,
}

impl StablePathSegment {
    const fn literal(kind: &'static str) -> Self {
        Self {
            kind,
            stable_id: None,
        }
    }

    const fn stable_id(kind: &'static str, stable_id: u32) -> Self {
        Self {
            kind,
            stable_id: Some(stable_id),
        }
    }
}

impl Ord for StablePathSegment {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind
            .cmp(other.kind)
            .then_with(|| self.stable_id.cmp(&other.stable_id))
    }
}

impl PartialOrd for StablePathSegment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn parse_stable_path_id(segment: &str) -> Option<(&'static str, u32)> {
    let (kind, digits) = segment.split_once(':')?;
    let kind = match kind {
        "aggregate" => "aggregate",
        "command" => "command",
        "entity" => "entity",
        "enum" => "enum",
        "event" => "event",
        "field" => "field",
        "index" => "index",
        "invariant" => "invariant",
        "outcome" => "outcome",
        "projection" => "projection",
        "variant" => "variant",
        _ => return None,
    };
    if digits.is_empty() || digits.starts_with('0') {
        return None;
    }
    let id = digits.parse::<u32>().ok()?;
    (id != 0).then_some((kind, id))
}

fn push_expected_stable_segment(
    segments: &mut Vec<StablePathSegment>,
    segment: &str,
    expected_kind: &'static str,
) -> Result<(), IrValidationError> {
    let (kind, id) = parse_stable_path_id(segment).ok_or(IrValidationError::InvalidText {
        kind: "compatibility path",
    })?;
    if kind != expected_kind {
        return Err(IrValidationError::InvalidText {
            kind: "compatibility path",
        });
    }
    segments.push(StablePathSegment::stable_id(kind, id));
    Ok(())
}

/// A complete canonical compatibility report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityReport {
    overall: CompatibilityClass,
    entries: Vec<CompatibilityEntry>,
}

impl CompatibilityReport {
    /// Defined empty compatible report for a genesis bundle.
    #[must_use]
    pub const fn genesis() -> Self {
        Self {
            overall: CompatibilityClass::Compatible,
            entries: Vec::new(),
        }
    }

    /// Creates a successor report in code/path order and derives its overall class.
    pub fn successor(mut entries: Vec<CompatibilityEntry>) -> Result<Self, IrValidationError> {
        if entries.is_empty() {
            entries.push(CompatibilityEntry::new(
                CompatibilityCode::NoSemanticChange,
                "contract",
            )?);
        }
        checked_len("compatibility entries", entries.len(), 4_096)?;
        entries.sort_unstable_by(|left, right| {
            left.code
                .as_str()
                .cmp(right.code.as_str())
                .then_with(|| left.path_key.cmp(&right.path_key))
        });
        if entries.windows(2).any(|pair| {
            pair[0].code == pair[1].code && pair[0].affected_path == pair[1].affected_path
        }) || (entries.len() > 1
            && entries
                .iter()
                .any(|entry| entry.code == CompatibilityCode::NoSemanticChange))
        {
            return Err(IrValidationError::InvalidCompatibilityReport);
        }
        let overall = entries
            .iter()
            .map(CompatibilityEntry::class)
            .max()
            .expect("successor report is nonempty");
        Ok(Self { overall, entries })
    }

    /// Most restrictive class.
    #[must_use]
    pub const fn overall(&self) -> CompatibilityClass {
        self.overall
    }
    /// Findings in stable code/path order.
    #[must_use]
    pub fn entries(&self) -> &[CompatibilityEntry] {
        &self.entries
    }
}

/// Borrowed checked successor semantics consumed by the IR-owned comparator.
#[derive(Clone, Copy)]
pub struct ContractCandidateV1<'a> {
    schema: &'a SchemaIr,
    commands: &'a [CommandPlan],
    projections: &'a [ProjectionPlan],
    mcp_names: &'a McpCommandNameRegistryV2,
}

impl<'a> ContractCandidateV1<'a> {
    /// Validates canonical candidate ordering, hashes, global bounds, and MCP completeness.
    pub fn new(
        schema: &'a SchemaIr,
        commands: &'a [CommandPlan],
        projections: &'a [ProjectionPlan],
        mcp_names: &'a McpCommandNameRegistryV2,
    ) -> Result<Self, IrValidationError> {
        if commands
            .windows(2)
            .any(|pair| pair[0].command_id() >= pair[1].command_id())
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "compatibility candidate commands",
            });
        }
        if projections
            .windows(2)
            .any(|pair| pair[0].projection_id() >= pair[1].projection_id())
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "compatibility candidate projections",
            });
        }
        if commands
            .windows(2)
            .any(|pair| pair[0].contract_version() != pair[1].contract_version())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "compatibility candidate contract version",
            });
        }
        crate::bundle::validate_bundle_global_bounds(schema, commands, projections)?;
        crate::bundle::validate_mcp_registry(mcp_names.lineage(), commands, mcp_names)?;
        for command in commands {
            if crate::bundle::compute_command_plan_hash(command, schema)? != command.plan_hash() {
                return Err(IrValidationError::HashMismatch {
                    kind: "compatibility candidate command",
                });
            }
        }
        for projection in projections {
            if crate::bundle::compute_projection_plan_hash(projection, schema)?
                != projection.plan_hash()
            {
                return Err(IrValidationError::HashMismatch {
                    kind: "compatibility candidate projection",
                });
            }
        }
        let candidate = Self {
            schema,
            commands,
            projections,
            mcp_names,
        };
        let identities = candidate_identities(candidate)?;
        let mut unique_identities = BTreeSet::new();
        let mut unique_slots = BTreeSet::new();
        for (identity, id) in identities {
            if !unique_identities.insert(identity.clone())
                || !unique_slots.insert((identity.namespace().allocation_namespace(), id))
            {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "compatibility candidate reuses an identity or stable-ID slot",
                });
            }
        }
        Ok(candidate)
    }

    /// Candidate structural schema.
    #[must_use]
    pub const fn schema(self) -> &'a SchemaIr {
        self.schema
    }

    /// Candidate commands in stable-ID order.
    #[must_use]
    pub const fn commands(self) -> &'a [CommandPlan] {
        self.commands
    }

    /// Candidate projections in stable-ID order.
    #[must_use]
    pub const fn projections(self) -> &'a [ProjectionPlan] {
        self.projections
    }

    /// Candidate compiler-owned MCP command registry.
    #[must_use]
    pub const fn mcp_names(self) -> &'a McpCommandNameRegistryV2 {
        self.mcp_names
    }
}

/// Compares one fully checked candidate against its exact parent bundle.
pub fn compare_successor(
    parent: &ContractBundle,
    next: ContractCandidateV1<'_>,
) -> Result<CompatibilityReport, IrValidationError> {
    if parent.lineage() != next.mcp_names.lineage() {
        return Err(IrValidationError::InvalidReference {
            kind: "successor contract lineage",
        });
    }
    let mut findings = BTreeSet::<(CompatibilityCode, String)>::new();
    compare_identities(parent, next, &mut findings)?;
    let compatible_schema = compare_schema(parent.schema(), next.schema, &mut findings);
    compare_commands(
        parent.commands(),
        next.commands,
        next.schema,
        &compatible_schema,
        &mut findings,
    )?;
    compare_projections(parent.projections(), next.projections, &mut findings);
    CompatibilityReport::successor(
        findings
            .into_iter()
            .map(|(code, path)| CompatibilityEntry::new(code, path))
            .collect::<Result<Vec<_>, _>>()?,
    )
}

#[derive(Default)]
struct CompatibleSchemaChanges {
    added_entities: BTreeSet<EntityTypeId>,
    optional_entities: BTreeSet<EntityTypeId>,
    optional_events: BTreeSet<riffdb_types::EventTypeId>,
}

fn compare_schema(
    parent: &SchemaIr,
    next: &SchemaIr,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) -> CompatibleSchemaChanges {
    let mut compatible = CompatibleSchemaChanges::default();
    for entity in next.entities() {
        let path = format!("entity:{}", entity.id().get());
        let Some(old) = parent.entity(entity.id()) else {
            compatible.added_entities.insert(entity.id());
            add(findings, CompatibilityCode::AddedEntity, path);
            continue;
        };
        if compare_optional_record_additions(
            old.record(),
            entity.record(),
            &path,
            CompatibilityCode::AddedOptionalField,
            findings,
        ) {
            compatible.optional_entities.insert(entity.id());
        }
        if old.primary_key_fields() != entity.primary_key_fields()
            || old.primary_key() != entity.primary_key()
        {
            add(findings, CompatibilityCode::KeyLayoutChange, path.clone());
        }
        compare_invariants(old.invariants(), entity.invariants(), &path, findings);
        let old_indexes = old
            .indexes()
            .iter()
            .map(|index| (index.id(), index))
            .collect::<BTreeMap<_, _>>();
        for index in entity.indexes() {
            let index_path = format!("{path}/index:{}", index.id().get());
            match old_indexes.get(&index.id()) {
                None => add(findings, CompatibilityCode::UnsupportedAddition, index_path),
                Some(old) if *old != index => {
                    add(findings, CompatibilityCode::KeyLayoutChange, index_path);
                }
                Some(_) => {}
            }
        }
    }
    for event in next.events() {
        let path = format!("event:{}", event.id().get());
        let Some(old) = parent.event(event.id()) else {
            add(findings, CompatibilityCode::AddedEvent, path);
            continue;
        };
        compare_optional_record_additions(
            old.payload(),
            event.payload(),
            &path,
            CompatibilityCode::AddedOptionalField,
            findings,
        );
        if old != event {
            if old.name() == event.name() && only_optional_additions(old.payload(), event.payload())
            {
                compatible.optional_events.insert(event.id());
            } else {
                add(findings, CompatibilityCode::EventChange, path);
            }
        }
    }
    for enumeration in next.enums() {
        let path = format!("enum:{}", enumeration.id().get());
        let Some(old) = parent.enumeration(enumeration.id()) else {
            add(findings, CompatibilityCode::AddedEnum, path);
            continue;
        };
        if old.name() != enumeration.name() {
            add(findings, CompatibilityCode::IdReuse, path.clone());
        }
        let old_variants = old
            .variants()
            .iter()
            .map(|variant| (variant.id(), variant))
            .collect::<BTreeMap<_, _>>();
        for variant in enumeration.variants() {
            let variant_path = format!("{path}/variant:{}", variant.id().get());
            match old_variants.get(&variant.id()) {
                None => add(findings, CompatibilityCode::AddedEnumVariant, variant_path),
                Some(old) if *old != variant => {
                    add(findings, CompatibilityCode::IdReuse, variant_path);
                }
                Some(_) => {}
            }
        }
    }
    for aggregate in next.aggregates() {
        let path = format!("aggregate:{}", aggregate.id().get());
        let Some(old) = parent.aggregate(aggregate.id()) else {
            let owns_only_added_entities = compatible.added_entities.contains(&aggregate.root())
                && aggregate
                    .children()
                    .iter()
                    .all(|entity| compatible.added_entities.contains(entity));
            add(
                findings,
                if owns_only_added_entities {
                    CompatibilityCode::AddedAggregate
                } else {
                    CompatibilityCode::UnsupportedAddition
                },
                path,
            );
            continue;
        };
        if old.root() != aggregate.root() || old.children() != aggregate.children() {
            add(findings, CompatibilityCode::KeyLayoutChange, path.clone());
        }
        if old.keys() != aggregate.keys() {
            add(
                findings,
                CompatibilityCode::PartitionConflictChange,
                path.clone(),
            );
        }
        compare_invariants(old.invariants(), aggregate.invariants(), &path, findings);
    }
    let parent_relationships = parent
        .relationships()
        .iter()
        .map(|relationship| {
            (
                (relationship.source_entity(), relationship.name().to_owned()),
                relationship,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let next_relationships = next
        .relationships()
        .iter()
        .map(|relationship| {
            (
                (relationship.source_entity(), relationship.name().to_owned()),
                relationship,
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (key, relationship) in &next_relationships {
        match parent_relationships.get(key) {
            None if compatible
                .added_entities
                .contains(&relationship.source_entity())
                && compatible
                    .added_entities
                    .contains(&relationship.target_entity()) => {}
            Some(old) if *old == *relationship => {}
            None | Some(_) => add(
                findings,
                CompatibilityCode::InvariantChange,
                format!("entity:{}", key.0.get()),
            ),
        }
    }
    for key in parent_relationships.keys() {
        if !next_relationships.contains_key(key) {
            add(
                findings,
                CompatibilityCode::InvariantChange,
                format!("entity:{}", key.0.get()),
            );
        }
    }
    let parent_unique = parent
        .unique_keys()
        .iter()
        .map(|unique| ((unique.source_entity(), unique.name().to_owned()), unique))
        .collect::<BTreeMap<_, _>>();
    let next_unique = next
        .unique_keys()
        .iter()
        .map(|unique| ((unique.source_entity(), unique.name().to_owned()), unique))
        .collect::<BTreeMap<_, _>>();
    for (key, unique) in &next_unique {
        match parent_unique.get(key) {
            None if compatible.added_entities.contains(&unique.source_entity()) => {}
            Some(old) if *old == *unique => {}
            None | Some(_) => add(
                findings,
                CompatibilityCode::InvariantChange,
                format!("entity:{}", key.0.get()),
            ),
        }
    }
    for key in parent_unique.keys() {
        if !next_unique.contains_key(key) {
            add(
                findings,
                CompatibilityCode::InvariantChange,
                format!("entity:{}", key.0.get()),
            );
        }
    }
    compatible
}

fn compare_invariants(
    parent: &[crate::InvariantPlan],
    next: &[crate::InvariantPlan],
    owner_path: &str,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) {
    let old = parent
        .iter()
        .map(|invariant| (invariant.id(), invariant))
        .collect::<BTreeMap<_, _>>();
    for invariant in next {
        let path = format!("{owner_path}/invariant:{}", invariant.id().get());
        match old.get(&invariant.id()) {
            None => add(findings, CompatibilityCode::UnsupportedAddition, path),
            Some(old) if *old != invariant => {
                add(findings, CompatibilityCode::InvariantChange, path);
            }
            Some(_) => {}
        }
    }
}

fn compare_optional_record_additions(
    parent: &RecordSchema,
    next: &RecordSchema,
    owner_path: &str,
    optional_code: CompatibilityCode,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) -> bool {
    let old_fields = parent
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect::<BTreeMap<FieldId, _>>();
    let mut optional_added = false;
    for field in next.fields() {
        let path = format!("{owner_path}/field:{}", field.id().get());
        match old_fields.get(&field.id()) {
            None if field.value_type().is_optional() => {
                optional_added = true;
                add(findings, optional_code, path);
            }
            None => add(findings, CompatibilityCode::UnsupportedAddition, path),
            Some(old) if old != &field => {
                let code = if old.value_type() != field.value_type() {
                    CompatibilityCode::TypeChange
                } else {
                    CompatibilityCode::IdReuse
                };
                add(findings, code, path);
            }
            Some(_) => {}
        }
    }
    optional_added
}

fn only_optional_additions(parent: &RecordSchema, next: &RecordSchema) -> bool {
    if parent.owner() != next.owner() {
        return false;
    }
    let old_fields = parent
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect::<BTreeMap<_, _>>();
    parent.fields().len() <= next.fields().len()
        && next
            .fields()
            .iter()
            .all(|field| match old_fields.get(&field.id()) {
                Some(old) => *old == field,
                None => field.value_type().is_optional(),
            })
}

fn compare_commands(
    parent: &[CommandPlan],
    next: &[CommandPlan],
    next_schema: &SchemaIr,
    compatible_schema: &CompatibleSchemaChanges,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) -> Result<(), IrValidationError> {
    let old_commands = parent
        .iter()
        .map(|command| (command.command_id(), command))
        .collect::<BTreeMap<_, _>>();
    for command in next {
        let path = format!("command:{}", command.command_id().get());
        let Some(old) = old_commands.get(&command.command_id()).copied() else {
            add(findings, CompatibilityCode::AddedCommand, path);
            continue;
        };
        compare_optional_record_additions(
            old.input().record(),
            command.input().record(),
            &format!("{path}/input"),
            CompatibilityCode::AddedOptionalField,
            findings,
        );
        if old.idempotency_input() != command.idempotency_input() {
            add(findings, CompatibilityCode::IdempotencyChange, path.clone());
        }
        if !locality_semantics_equal(old, command)? {
            add(
                findings,
                CompatibilityCode::PartitionConflictChange,
                path.clone(),
            );
        }
        compare_outcomes(
            old,
            command,
            &compatible_schema.optional_entities,
            &path,
            findings,
        );
        if !command_semantics_compatible(old, command, next_schema, compatible_schema)? {
            add(findings, CompatibilityCode::ExistingPlanChange, path);
        }
    }
    Ok(())
}

fn compare_outcomes(
    parent: &CommandPlan,
    next: &CommandPlan,
    optional_entities: &BTreeSet<EntityTypeId>,
    command_path: &str,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) {
    let old_outcomes = parent
        .outcomes()
        .iter()
        .map(|outcome| (outcome.id(), outcome))
        .collect::<BTreeMap<_, _>>();
    for outcome in next.outcomes() {
        let path = format!("{command_path}/outcome:{}", outcome.id().get());
        let Some(old) = old_outcomes.get(&outcome.id()).copied() else {
            add(findings, CompatibilityCode::AddedOutcome, path);
            continue;
        };
        compare_optional_record_additions(
            old.payload(),
            outcome.payload(),
            &path,
            CompatibilityCode::AddedOptionalOutcomeField,
            findings,
        );
        if old.name() != outcome.name() {
            add(findings, CompatibilityCode::IdReuse, path.clone());
        }
        if old.payload() != outcome.payload()
            && !only_optional_additions(old.payload(), outcome.payload())
        {
            add(findings, CompatibilityCode::OutcomeChange, path.clone());
        }
        for entity in optional_entities {
            if outcome
                .payload()
                .fields()
                .iter()
                .any(|field| contains_entity_record(field.value_type(), *entity))
            {
                add(
                    findings,
                    CompatibilityCode::AddedOptionalOutcomeField,
                    path.clone(),
                );
            }
        }
    }
    if parent.success_outcome() != next.success_outcome() {
        add(
            findings,
            CompatibilityCode::OutcomeChange,
            command_path.to_owned(),
        );
    }
}

fn command_semantics_compatible(
    old: &CommandPlan,
    next: &CommandPlan,
    next_schema: &SchemaIr,
    compatible_schema: &CompatibleSchemaChanges,
) -> Result<bool, IrValidationError> {
    if old.success_outcome() != next.success_outcome()
        || old.idempotency_input() != next.idempotency_input()
        || old.execution_class() != next.execution_class()
        || old.retry_policy() != next.retry_policy()
        || old.required_capability() != next.required_capability()
        || old.bindings().len() != next.bindings().len()
        || old.root_validation_reads().len() != next.root_validation_reads().len()
        || old.commit_checks().len() != next.commit_checks().len()
    {
        return Ok(false);
    }
    let Some(added_requirements) =
        instructions_compatible(old, next, next_schema, compatible_schema)?
    else {
        return Ok(false);
    };
    let (allowed_fields, allowed_complete) =
        added_requirement_dependencies(next, &added_requirements)?;
    for (left, right) in old.bindings().iter().zip(next.bindings()) {
        let index = right.id().get() as usize;
        if !bindings_compatible(
            old,
            left,
            next,
            right,
            &allowed_fields[index],
            allowed_complete[index],
        )? {
            return Ok(false);
        }
    }
    if !locality_semantics_equal(old, next)? {
        return Ok(false);
    }
    for (left, right) in old
        .root_validation_reads()
        .iter()
        .zip(next.root_validation_reads())
    {
        if left.id() != right.id()
            || left.source_binding() != right.source_binding()
            || left.entity_type() != right.entity_type()
            || left.key_schema() != right.key_schema()
            || left.accessed_fields() != right.accessed_fields()
            || left.key_expressions().len() != right.key_expressions().len()
        {
            return Ok(false);
        }
        for (old_expression, next_expression) in
            left.key_expressions().iter().zip(right.key_expressions())
        {
            if !expression_trees_equal(
                old.expressions(),
                *old_expression,
                next.expressions(),
                *next_expression,
            )? {
                return Ok(false);
            }
        }
    }
    for (left, right) in old.commit_checks().iter().zip(next.commit_checks()) {
        if left.invariant_id() != right.invariant_id()
            || left.source_bindings() != right.source_bindings()
            || left.root_validation_reads() != right.root_validation_reads()
            || !expression_trees_equal(
                old.expressions(),
                left.predicate(),
                next.expressions(),
                right.predicate(),
            )?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn instructions_compatible<'a>(
    old: &CommandPlan,
    next: &'a CommandPlan,
    next_schema: &SchemaIr,
    compatible_schema: &CompatibleSchemaChanges,
) -> Result<Option<Vec<&'a Instruction>>, IrValidationError> {
    let old_outcomes = old
        .outcomes()
        .iter()
        .map(|outcome| outcome.id())
        .collect::<BTreeSet<_>>();
    let mut old_index = 0usize;
    let mut next_index = 0usize;
    let mut added = Vec::new();
    while next_index < next.instructions().len() {
        let next_instruction = &next.instructions()[next_index];
        if matches!(next_instruction, Instruction::Require { reject, .. }
            if !old_outcomes.contains(&reject.outcome_id()))
        {
            added.push(next_instruction);
            next_index += 1;
            continue;
        }
        let Some(old_instruction) = old.instructions().get(old_index) else {
            return Ok(None);
        };
        if !existing_instruction_compatible(
            old,
            old_instruction,
            next,
            next_instruction,
            next_schema,
            compatible_schema,
        )? {
            return Ok(None);
        }
        old_index += 1;
        next_index += 1;
    }
    if old_index != old.instructions().len() {
        return Ok(None);
    }
    Ok(Some(added))
}

fn existing_instruction_compatible(
    old_plan: &CommandPlan,
    old: &Instruction,
    next_plan: &CommandPlan,
    next: &Instruction,
    next_schema: &SchemaIr,
    compatible_schema: &CompatibleSchemaChanges,
) -> Result<bool, IrValidationError> {
    match (old, next) {
        (
            Instruction::Require {
                predicate: left_predicate,
                reject: left_reject,
                ..
            },
            Instruction::Require {
                predicate: right_predicate,
                reject: right_reject,
                ..
            },
        ) => Ok(expression_trees_equal(
            old_plan.expressions(),
            *left_predicate,
            next_plan.expressions(),
            *right_predicate,
        )? && outcome_constructions_compatible(
            old_plan,
            left_reject,
            next_plan,
            right_reject,
        )?),
        (
            Instruction::SetField {
                binding: left_binding,
                field: left_field,
                value: left_value,
            },
            Instruction::SetField {
                binding: right_binding,
                field: right_field,
                value: right_value,
            },
        ) => Ok(left_binding == right_binding
            && left_field == right_field
            && expression_trees_equal(
                old_plan.expressions(),
                *left_value,
                next_plan.expressions(),
                *right_value,
            )?),
        (Instruction::EmitEvent(left), Instruction::EmitEvent(right)) => {
            event_constructions_compatible(
                old_plan.expressions(),
                left,
                next_plan.expressions(),
                right,
                next_schema,
                &compatible_schema.optional_events,
            )
        }
        (Instruction::Return(left), Instruction::Return(right)) => {
            outcome_constructions_compatible(old_plan, left, next_plan, right)
        }
        _ => Ok(false),
    }
}

fn added_requirement_dependencies(
    plan: &CommandPlan,
    added: &[&Instruction],
) -> Result<(Vec<BTreeSet<FieldId>>, Vec<bool>), IrValidationError> {
    let mut fields = vec![BTreeSet::new(); plan.bindings().len()];
    let mut complete = vec![false; plan.bindings().len()];
    let mut include = |expression| -> Result<(), IrValidationError> {
        let dependencies = plan.expressions().dependencies(expression)?;
        for binding in dependencies.complete_bindings() {
            complete[binding.get() as usize] = true;
        }
        for (binding, field) in dependencies.bound_fields() {
            fields[binding.get() as usize].insert(*field);
        }
        Ok(())
    };
    for instruction in added {
        let Instruction::Require {
            predicate, reject, ..
        } = instruction
        else {
            return Err(IrValidationError::InvalidInstructionStream {
                reason: "compatibility-added instruction is not a requirement",
            });
        };
        include(*predicate)?;
        for field in reject.payload().fields() {
            include(field.expression())?;
        }
    }
    Ok((fields, complete))
}

fn bindings_compatible(
    old_plan: &CommandPlan,
    old: &BindingPlan,
    next_plan: &CommandPlan,
    next: &BindingPlan,
    allowed_fields: &BTreeSet<FieldId>,
    allowed_complete: bool,
) -> Result<bool, IrValidationError> {
    if old.id() != next.id()
        || old.mode() != next.mode()
        || old.entity_type() != next.entity_type()
        || old.key_schema() != next.key_schema()
        || old.key_expressions().len() != next.key_expressions().len()
    {
        return Ok(false);
    }
    let expected_fields = old
        .accessed_fields()
        .iter()
        .copied()
        .chain(allowed_fields.iter().copied())
        .collect::<BTreeSet<_>>();
    if next
        .accessed_fields()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        != expected_fields
        || next.complete_record_access() != (old.complete_record_access() || allowed_complete)
    {
        return Ok(false);
    }
    for (left, right) in old.key_expressions().iter().zip(next.key_expressions()) {
        if !expression_trees_equal(
            old_plan.expressions(),
            *left,
            next_plan.expressions(),
            *right,
        )? {
            return Ok(false);
        }
    }
    outcome_constructions_compatible(old_plan, old.failure(), next_plan, next.failure())
}

fn locality_semantics_equal(
    old: &CommandPlan,
    next: &CommandPlan,
) -> Result<bool, IrValidationError> {
    if old.locality().aggregate_id() != next.locality().aggregate_id()
        || old.locality().partition_schema() != next.locality().partition_schema()
        || old.locality().conflict_keys().len() != next.locality().conflict_keys().len()
        || !expression_trees_equal(
            old.expressions(),
            old.locality().partition_expression(),
            next.expressions(),
            next.locality().partition_expression(),
        )?
    {
        return Ok(false);
    }
    for (left, right) in old
        .locality()
        .conflict_keys()
        .iter()
        .zip(next.locality().conflict_keys())
    {
        if left.schema() != right.schema() || left.expressions().len() != right.expressions().len()
        {
            return Ok(false);
        }
        for (left_expression, right_expression) in
            left.expressions().iter().zip(right.expressions())
        {
            if !expression_trees_equal(
                old.expressions(),
                *left_expression,
                next.expressions(),
                *right_expression,
            )? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn outcome_constructions_compatible(
    old_plan: &CommandPlan,
    old: &OutcomeConstruction,
    next_plan: &CommandPlan,
    next: &OutcomeConstruction,
) -> Result<bool, IrValidationError> {
    if old.outcome_id() != next.outcome_id() {
        return Ok(false);
    }
    let next_schema = next_plan
        .outcomes()
        .iter()
        .find(|outcome| outcome.id() == next.outcome_id())
        .ok_or(IrValidationError::InvalidReference {
            kind: "compatibility outcome construction",
        })?
        .payload();
    object_constructions_compatible(
        old_plan.expressions(),
        old.payload().fields(),
        next_plan.expressions(),
        next.payload().fields(),
        next_schema,
    )
}

fn event_constructions_compatible(
    old_arena: &ExpressionArena,
    old: &EventConstruction,
    next_arena: &ExpressionArena,
    next: &EventConstruction,
    next_schema: &SchemaIr,
    optional_events: &BTreeSet<riffdb_types::EventTypeId>,
) -> Result<bool, IrValidationError> {
    if old.event_type() != next.event_type() {
        return Ok(false);
    }
    if !optional_events.contains(&next.event_type()) {
        return object_constructions_exact(
            old_arena,
            old.payload().fields(),
            next_arena,
            next.payload().fields(),
        );
    }
    let schema = next_schema
        .event(next.event_type())
        .ok_or(IrValidationError::InvalidReference {
            kind: "compatibility event construction",
        })?
        .payload();
    object_constructions_compatible(
        old_arena,
        old.payload().fields(),
        next_arena,
        next.payload().fields(),
        schema,
    )
}

fn object_constructions_exact(
    old_arena: &ExpressionArena,
    old: &[FieldExpression],
    next_arena: &ExpressionArena,
    next: &[FieldExpression],
) -> Result<bool, IrValidationError> {
    if old.len() != next.len() {
        return Ok(false);
    }
    for (left, right) in old.iter().zip(next) {
        if left.field_id() != right.field_id()
            || !expression_trees_equal(
                old_arena,
                left.expression(),
                next_arena,
                right.expression(),
            )?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn object_constructions_compatible(
    old_arena: &ExpressionArena,
    old: &[FieldExpression],
    next_arena: &ExpressionArena,
    next: &[FieldExpression],
    next_schema: &RecordSchema,
) -> Result<bool, IrValidationError> {
    let old_fields = old
        .iter()
        .map(|field| (field.field_id(), field))
        .collect::<BTreeMap<_, _>>();
    if old_fields
        .keys()
        .any(|field_id| next.iter().all(|field| field.field_id() != *field_id))
    {
        return Ok(false);
    }
    for field in next {
        match old_fields.get(&field.field_id()) {
            Some(old) => {
                if !expression_trees_equal(
                    old_arena,
                    old.expression(),
                    next_arena,
                    field.expression(),
                )? {
                    return Ok(false);
                }
            }
            None => {
                let declared = next_schema.field(field.field_id()).ok_or(
                    IrValidationError::InvalidReference {
                        kind: "compatibility constructed field",
                    },
                )?;
                if !declared.value_type().is_optional()
                    || !matches!(
                        next_arena.get(field.expression()).map(|node| node.kind()),
                        Some(ExpressionKind::Constant(riffdb_types::CanonicalValue::Null))
                    )
                {
                    return Ok(false);
                }
            }
        }
    }
    Ok(old.len() <= next.len())
}

fn expression_trees_equal(
    left_arena: &ExpressionArena,
    left_root: crate::ExprId,
    right_arena: &ExpressionArena,
    right_root: crate::ExprId,
) -> Result<bool, IrValidationError> {
    let mut pending = vec![(left_root, right_root)];
    let mut visited = BTreeSet::new();
    while let Some((left_id, right_id)) = pending.pop() {
        if !visited.insert((left_id, right_id)) {
            continue;
        }
        checked_len(
            "compatibility expression comparison pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let left = left_arena
            .get(left_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "compatibility expression",
            })?;
        let right = right_arena
            .get(right_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "compatibility expression",
            })?;
        if left.result_type() != right.result_type() {
            return Ok(false);
        }
        match (left.kind(), right.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (ExpressionKind::InputField(left), ExpressionKind::InputField(right))
                if left == right => {}
            (ExpressionKind::CompleteBinding(left), ExpressionKind::CompleteBinding(right))
                if left == right => {}
            (
                ExpressionKind::BoundField {
                    binding: left_binding,
                    field: left_field,
                },
                ExpressionKind::BoundField {
                    binding: right_binding,
                    field: right_field,
                },
            ) if left_binding == right_binding && left_field == right_field => {}
            (
                ExpressionKind::SchemaField {
                    entity_type: left_entity,
                    field: left_field,
                },
                ExpressionKind::SchemaField {
                    entity_type: right_entity,
                    field: right_field,
                },
            ) if left_entity == right_entity && left_field == right_field => {}
            (
                ExpressionKind::RootValidationField {
                    read: left_read,
                    field: left_field,
                },
                ExpressionKind::RootValidationField {
                    read: right_read,
                    field: right_field,
                },
            ) if left_read == right_read && left_field == right_field => {}
            (ExpressionKind::SourceEventField(left), ExpressionKind::SourceEventField(right))
                if left == right => {}
            (ExpressionKind::TransactionTime, ExpressionKind::TransactionTime)
            | (ExpressionKind::TransactionDate, ExpressionKind::TransactionDate) => {}
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

fn compare_projections(
    parent: &[ProjectionPlan],
    next: &[ProjectionPlan],
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) {
    let old = parent
        .iter()
        .map(|projection| (projection.projection_id(), projection))
        .collect::<BTreeMap<_, _>>();
    for projection in next {
        let path = format!("projection:{}", projection.projection_id().get());
        let Some(parent) = old.get(&projection.projection_id()).copied() else {
            add(findings, CompatibilityCode::AddedProjection, path);
            continue;
        };
        if parent.source_event() != projection.source_event()
            || parent.expressions() != projection.expressions()
            || parent.filter() != projection.filter()
            || parent.key_expressions() != projection.key_expressions()
            || parent.measures() != projection.measures()
            || parent.frontier() != projection.frontier()
            || parent.group_schema() != projection.group_schema()
        {
            add(findings, CompatibilityCode::ExistingPlanChange, path);
        }
    }
}

fn contains_entity_record(value_type: &ValueType, entity: EntityTypeId) -> bool {
    if value_type.record_ref() == Some(&RecordTypeRef::Entity(entity)) {
        return true;
    }
    value_type
        .optional_inner()
        .is_some_and(|inner| contains_entity_record(inner, entity))
        || value_type
            .list_parts()
            .is_some_and(|(element, _)| contains_entity_record(element, entity))
}

fn compare_identities(
    parent: &ContractBundle,
    next: ContractCandidateV1<'_>,
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
) -> Result<(), IrValidationError> {
    let desired = candidate_identities(next)?;
    let desired_by_identity = desired
        .iter()
        .cloned()
        .collect::<BTreeMap<StableIdentity, u32>>();
    let desired_by_slot = desired
        .iter()
        .map(|(identity, id)| ((identity.namespace().allocation_namespace(), *id), identity))
        .collect::<BTreeMap<_, _>>();
    for allocation in parent.ledger().allocations() {
        for entry in allocation.entries() {
            let path = identity_path(
                entry.identity().namespace().tag(),
                entry.identity().namespace().owner_kind(),
                entry.identity().namespace().owner_ids(),
                entry.id(),
            );
            match entry.state() {
                LineageEntryState::Active => {
                    match desired_by_identity.get(entry.identity()) {
                        None => add(findings, CompatibilityCode::RemovedIdentity, path.clone()),
                        Some(id) if *id != entry.id() => {
                            add(findings, CompatibilityCode::IdReuse, path.clone());
                        }
                        Some(_) => {}
                    }
                    if desired_by_slot
                        .get(&(allocation.namespace().clone(), entry.id()))
                        .is_some_and(|identity| *identity != entry.identity())
                    {
                        add(findings, CompatibilityCode::IdReuse, path);
                    }
                }
                LineageEntryState::Tombstone => {
                    if desired_by_identity.contains_key(entry.identity()) {
                        add(findings, CompatibilityCode::TombstoneResurrection, path);
                    } else if desired_by_slot
                        .get(&(allocation.namespace().clone(), entry.id()))
                        .is_some_and(|identity| *identity != entry.identity())
                    {
                        add(findings, CompatibilityCode::IdReuse, path);
                    }
                }
            }
        }
    }
    Ok(())
}

fn candidate_identities(
    next: ContractCandidateV1<'_>,
) -> Result<Vec<(StableIdentity, u32)>, IrValidationError> {
    let mut identities = Vec::new();
    let mut push = |tag, owner_kind, owner_ids, name: &str, id| {
        identities.push((
            StableIdentity::new(StableIdNamespace::new(tag, owner_kind, owner_ids)?, name)?,
            id,
        ));
        Ok::<_, IrValidationError>(())
    };
    for entity in next.schema.entities() {
        push(
            StableIdNamespaceTag::Entity,
            0,
            vec![],
            entity.name(),
            entity.id().get(),
        )?;
        for field in entity.record().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::ENTITY,
                vec![entity.id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
        for invariant in entity.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                invariant_owner_tag::ENTITY,
                vec![entity.id().get()],
                invariant.name(),
                invariant.id().get(),
            )?;
        }
        for index in entity.indexes() {
            push(
                StableIdNamespaceTag::Index,
                index_owner_tag::ENTITY,
                vec![entity.id().get()],
                index.name(),
                index.id().get(),
            )?;
        }
    }
    for event in next.schema.events() {
        push(
            StableIdNamespaceTag::Event,
            0,
            vec![],
            event.name(),
            event.id().get(),
        )?;
        for field in event.payload().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::EVENT,
                vec![event.id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
    }
    for enumeration in next.schema.enums() {
        push(
            StableIdNamespaceTag::Enum,
            0,
            vec![],
            enumeration.name(),
            enumeration.id().get(),
        )?;
        for variant in enumeration.variants() {
            push(
                StableIdNamespaceTag::EnumVariant,
                variant_owner_tag::ENUM,
                vec![enumeration.id().get()],
                variant.name(),
                variant.id().get(),
            )?;
        }
    }
    for aggregate in next.schema.aggregates() {
        push(
            StableIdNamespaceTag::Aggregate,
            0,
            vec![],
            aggregate.name(),
            aggregate.id().get(),
        )?;
        for invariant in aggregate.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                invariant_owner_tag::AGGREGATE,
                vec![aggregate.id().get()],
                invariant.name(),
                invariant.id().get(),
            )?;
        }
    }
    for command in next.commands {
        push(
            StableIdNamespaceTag::Command,
            0,
            vec![],
            command.name(),
            command.command_id().get(),
        )?;
        for field in command.input().record().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::COMMAND_INPUT,
                vec![command.command_id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
        for outcome in command.outcomes() {
            push(
                StableIdNamespaceTag::Outcome,
                outcome_owner_tag::COMMAND,
                vec![command.command_id().get()],
                outcome.name(),
                outcome.id().get(),
            )?;
            for field in outcome.payload().fields() {
                push(
                    StableIdNamespaceTag::Field,
                    record_owner_tag::COMMAND_OUTCOME,
                    vec![command.command_id().get(), outcome.id().get()],
                    field.name(),
                    field.id().get(),
                )?;
            }
        }
    }
    for projection in next.projections {
        push(
            StableIdNamespaceTag::Projection,
            0,
            vec![],
            projection.name(),
            projection.projection_id().get(),
        )?;
        for field in projection.group_schema().measures().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::PROJECTION_RESULT,
                vec![projection.projection_id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
    }
    Ok(identities)
}

fn identity_path(tag: StableIdNamespaceTag, owner_kind: u8, owners: &[u32], id: u32) -> String {
    let kind = match tag {
        StableIdNamespaceTag::Entity => "entity",
        StableIdNamespaceTag::Event => "event",
        StableIdNamespaceTag::Enum => "enum",
        StableIdNamespaceTag::Aggregate => "aggregate",
        StableIdNamespaceTag::Command => "command",
        StableIdNamespaceTag::Projection => "projection",
        StableIdNamespaceTag::Index => "index",
        StableIdNamespaceTag::Invariant => "invariant",
        StableIdNamespaceTag::Field => "field",
        StableIdNamespaceTag::Outcome => "outcome",
        StableIdNamespaceTag::EnumVariant => "variant",
    };
    if owners.is_empty() {
        format!("{kind}:{id}")
    } else {
        match (tag, owner_kind, owners) {
            (StableIdNamespaceTag::Field, record_owner_tag::ENTITY, [entity]) => {
                format!("entity:{entity}/field:{id}")
            }
            (StableIdNamespaceTag::Field, record_owner_tag::EVENT, [event]) => {
                format!("event:{event}/field:{id}")
            }
            (StableIdNamespaceTag::Field, record_owner_tag::COMMAND_INPUT, [command]) => {
                format!("command:{command}/input/field:{id}")
            }
            (
                StableIdNamespaceTag::Field,
                record_owner_tag::COMMAND_OUTCOME,
                [command, outcome],
            ) => format!("command:{command}/outcome:{outcome}/field:{id}"),
            (StableIdNamespaceTag::Field, record_owner_tag::PROJECTION_RESULT, [projection]) => {
                format!("projection:{projection}/field:{id}")
            }
            (StableIdNamespaceTag::Outcome, outcome_owner_tag::COMMAND, [command]) => {
                format!("command:{command}/outcome:{id}")
            }
            (StableIdNamespaceTag::EnumVariant, variant_owner_tag::ENUM, [enumeration]) => {
                format!("enum:{enumeration}/variant:{id}")
            }
            (StableIdNamespaceTag::Index, index_owner_tag::ENTITY, [entity]) => {
                format!("entity:{entity}/index:{id}")
            }
            (StableIdNamespaceTag::Invariant, invariant_owner_tag::ENTITY, [entity]) => {
                format!("entity:{entity}/invariant:{id}")
            }
            (StableIdNamespaceTag::Invariant, invariant_owner_tag::AGGREGATE, [aggregate]) => {
                format!("aggregate:{aggregate}/invariant:{id}")
            }
            _ => {
                let owner_ids = owners
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join("/");
                format!("owner-kind:{owner_kind}/owner:{owner_ids}/{kind}:{id}")
            }
        }
    }
}

fn add(
    findings: &mut BTreeSet<(CompatibilityCode, String)>,
    code: CompatibilityCode,
    path: String,
) {
    findings.insert((code, path));
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{ContractLineage, ContractVersion, EntityTypeId, SourceHash};

    fn empty_parent_with_entity_tombstone(name: &str) -> ContractBundle {
        let lineage = ContractLineage::new("Test".to_owned()).expect("lineage");
        let identity = StableIdentity::new(
            StableIdNamespace::new(StableIdNamespaceTag::Entity, 0, vec![]).expect("namespace"),
            name,
        )
        .expect("identity");
        let schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let required =
            crate::required_lineage_allocation_namespaces(&schema, &[], &[]).expect("required");
        let genesis = crate::LineageLedgerV1::genesis_complete(vec![identity], required.clone())
            .expect("genesis ledger");
        let removed = crate::LineageLedgerV1::successor_complete(&genesis, vec![], required)
            .expect("removed ledger");
        let registry =
            McpCommandNameRegistryV2::new(lineage.clone(), "Test", vec![]).expect("registry");
        ContractBundle::new(
            "0.1.0",
            lineage,
            ContractVersion::new(1).expect("version"),
            None,
            SourceHash::from_bytes([0; 32]),
            removed,
            schema,
            vec![],
            vec![],
            vec![],
            registry,
            CompatibilityReport::genesis(),
        )
        .expect("parent")
    }

    fn one_entity_candidate(name: &str) -> (SchemaIr, McpCommandNameRegistryV2) {
        let entity_id = EntityTypeId::first();
        let field_id = FieldId::first();
        let component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let entity = crate::EntitySchema::new(
            entity_id,
            name,
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![
                    crate::FieldSchema::new(field_id, "id", crate::ValueType::u64())
                        .expect("field"),
                ],
            )
            .expect("record"),
            vec![field_id],
            crate::KeySchema::new(crate::KeyPurpose::Entity(entity_id), vec![component])
                .expect("key"),
            vec![],
            vec![],
        )
        .expect("entity");
        let schema = SchemaIr::new(vec![entity], vec![], vec![], vec![]).expect("schema");
        let lineage = ContractLineage::new("Test".to_owned()).expect("lineage");
        let registry = McpCommandNameRegistryV2::new(lineage, "Test", vec![]).expect("registry");
        (schema, registry)
    }

    #[test]
    fn successor_empty_input_has_defined_no_change_identity() {
        let report = CompatibilityReport::successor(vec![]).expect("report");
        assert_eq!(
            report.entries()[0].code(),
            CompatibilityCode::NoSemanticChange
        );
        assert_eq!(report.overall(), CompatibilityClass::Compatible);
    }

    #[test]
    fn overall_is_the_most_restrictive_entry() {
        let report = CompatibilityReport::successor(vec![
            CompatibilityEntry::new(CompatibilityCode::AddedCommand, "command:2").expect("entry"),
            CompatibilityEntry::new(CompatibilityCode::OutcomeChange, "command:1/outcome:2")
                .expect("entry"),
        ])
        .expect("report");
        assert_eq!(report.overall(), CompatibilityClass::Incompatible);
    }

    #[test]
    fn compatibility_paths_order_stable_ids_numerically_at_every_segment()
    -> Result<(), IrValidationError> {
        let report = CompatibilityReport::successor(
            [
                "entity:10/field:2",
                "entity:2/field:11",
                "entity:2/field:10",
                "entity:2/field:2",
                "command:10/outcome:2/field:2",
                "command:2/outcome:10/field:2",
                "command:2/outcome:2/field:11",
                "command:2/outcome:2/field:10",
                "command:2/outcome:2/field:2",
            ]
            .into_iter()
            .map(|path| CompatibilityEntry::new(CompatibilityCode::RemovedIdentity, path))
            .collect::<Result<Vec<_>, _>>()?,
        )
        .expect("report");
        assert_eq!(
            report
                .entries()
                .iter()
                .map(CompatibilityEntry::affected_path)
                .collect::<Vec<_>>(),
            vec![
                "command:2/outcome:2/field:2",
                "command:2/outcome:2/field:10",
                "command:2/outcome:2/field:11",
                "command:2/outcome:10/field:2",
                "command:10/outcome:2/field:2",
                "entity:2/field:2",
                "entity:2/field:10",
                "entity:2/field:11",
                "entity:10/field:2",
            ]
        );
        Ok::<(), IrValidationError>(())
    }

    #[test]
    fn compatibility_paths_reject_noncanonical_or_unknown_grammar() {
        for path in [
            "entity:0",
            "entity:02",
            "entity:4294967296",
            "command:1/input",
            "command:1/input/outcome:2",
            "command:1/outcome:2/index:3",
            "owner-kind:1/owner:2/field:3",
            "unknown:1",
            "contract/field:1",
        ] {
            assert!(
                CompatibilityEntry::new(CompatibilityCode::RemovedIdentity, path).is_err(),
                "accepted {path}"
            );
        }
    }

    #[test]
    fn tombstone_resurrection_and_different_identity_slot_reuse_are_distinct() {
        let parent = empty_parent_with_entity_tombstone("Old");
        let (resurrected_schema, resurrected_registry) = one_entity_candidate("Old");
        let resurrected =
            ContractCandidateV1::new(&resurrected_schema, &[], &[], &resurrected_registry)
                .expect("candidate");
        let report = compare_successor(&parent, resurrected).expect("report");
        assert!(
            report
                .entries()
                .iter()
                .any(|entry| { entry.code() == CompatibilityCode::TombstoneResurrection })
        );

        let (reused_schema, reused_registry) = one_entity_candidate("New");
        let reused = ContractCandidateV1::new(&reused_schema, &[], &[], &reused_registry)
            .expect("candidate");
        let report = compare_successor(&parent, reused).expect("report");
        assert!(
            report
                .entries()
                .iter()
                .any(|entry| entry.code() == CompatibilityCode::IdReuse)
        );
    }

    #[test]
    fn added_outcome_requirement_is_explicit_version_but_unrelated_set_is_incompatible() {
        let (old, schema) = crate::plan::tests::minimal_mutation();
        let added = crate::plan::tests::mutation_with_added_outcome(&old, &schema, false);
        let mut findings = BTreeSet::new();
        compare_commands(
            std::slice::from_ref(&old),
            std::slice::from_ref(&added),
            &schema,
            &CompatibleSchemaChanges::default(),
            &mut findings,
        )
        .expect("compare");
        let report = CompatibilityReport::successor(
            findings
                .into_iter()
                .map(|(code, path)| CompatibilityEntry::new(code, path))
                .collect::<Result<Vec<_>, _>>()
                .expect("entries"),
        )
        .expect("report");
        assert_eq!(
            report.overall(),
            CompatibilityClass::RequiresExplicitVersion
        );
        assert!(
            report
                .entries()
                .iter()
                .all(|entry| entry.code() != CompatibilityCode::ExistingPlanChange)
        );

        let changed = crate::plan::tests::mutation_with_added_outcome(&old, &schema, true);
        let mut findings = BTreeSet::new();
        compare_commands(
            std::slice::from_ref(&old),
            std::slice::from_ref(&changed),
            &schema,
            &CompatibleSchemaChanges::default(),
            &mut findings,
        )
        .expect("compare");
        assert!(
            findings
                .iter()
                .any(|(code, _)| *code == CompatibilityCode::ExistingPlanChange)
        );
    }

    #[test]
    fn unchanged_field_dependent_root_validation_plan_is_semantically_compatible() {
        let (plan, schema) = crate::plan::tests::root_validation_mutation(true);
        assert!(
            command_semantics_compatible(
                &plan,
                &plan,
                &schema,
                &CompatibleSchemaChanges::default(),
            )
            .expect("comparison")
        );
    }

    #[test]
    fn compatibility_expression_comparison_memoizes_shared_dag_pairs() {
        let mut nodes = vec![(
            ExpressionKind::Constant(riffdb_types::CanonicalValue::Bool(true)),
            ValueType::bool(),
        )];
        for index in 1..crate::MAX_EXPRESSION_NESTING {
            nodes.push((
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::And,
                    left: crate::ExprId::new((index - 1) as u32),
                    right: crate::ExprId::new((index - 1) as u32),
                },
                ValueType::bool(),
            ));
        }
        let left = ExpressionArena::new(nodes.clone()).expect("left arena");
        let right = ExpressionArena::new(nodes).expect("right arena");
        assert!(
            expression_trees_equal(
                &left,
                crate::ExprId::new((crate::MAX_EXPRESSION_NESTING - 1) as u32),
                &right,
                crate::ExprId::new((crate::MAX_EXPRESSION_NESTING - 1) as u32),
            )
            .expect("comparison")
        );
    }

    #[test]
    fn identity_paths_interpret_owner_tags_in_namespace_context() {
        assert_eq!(
            identity_path(
                StableIdNamespaceTag::Field,
                record_owner_tag::EVENT,
                &[7],
                3,
            ),
            "event:7/field:3"
        );
        assert_eq!(
            identity_path(
                StableIdNamespaceTag::Field,
                record_owner_tag::COMMAND_OUTCOME,
                &[11, 2],
                5,
            ),
            "command:11/outcome:2/field:5"
        );
        assert_eq!(
            identity_path(
                StableIdNamespaceTag::Outcome,
                outcome_owner_tag::COMMAND,
                &[11],
                2,
            ),
            "command:11/outcome:2"
        );
        assert_eq!(
            identity_path(
                StableIdNamespaceTag::Invariant,
                invariant_owner_tag::AGGREGATE,
                &[13],
                4,
            ),
            "aggregate:13/invariant:4"
        );
    }
}
