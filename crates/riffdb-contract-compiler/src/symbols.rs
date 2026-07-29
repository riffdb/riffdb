//! Source symbol validation and deterministic genesis stable-ID allocation.

use std::collections::BTreeMap;

use riffdb_contract_ir::{
    LineageEntryState, LineageLedgerV1, StableIdAllocationNamespace, StableIdNamespace,
    StableIdNamespaceTag, StableIdentity,
};
use riffdb_contract_syntax::ast::{
    AggregateItem, CommandDeclaration, Declaration, EntityItem, ObjectLiteral,
};
use riffdb_contract_syntax::{ContractDocument, Span, Spanned};
use riffdb_types::{
    AggregateTypeId, CommandId, ContractVersion, EntityTypeId, EnumTypeId, EnumVariantId,
    EventTypeId, FieldId, IndexId, InvariantId, OutcomeId, ProjectionId,
};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};

/// Compiler-private stable-ID side tables for one lineage compilation.
#[derive(Clone, Debug)]
pub(crate) struct GenesisSymbols {
    pub(crate) contract_version: ContractVersion,
    pub(crate) entities: BTreeMap<String, EntityTypeId>,
    pub(crate) events: BTreeMap<String, EventTypeId>,
    pub(crate) enums: BTreeMap<String, EnumTypeId>,
    pub(crate) aggregates: BTreeMap<String, AggregateTypeId>,
    pub(crate) commands: BTreeMap<String, CommandId>,
    pub(crate) projections: BTreeMap<String, ProjectionId>,
    pub(crate) entity_fields: BTreeMap<(EntityTypeId, String), FieldId>,
    pub(crate) event_fields: BTreeMap<(EventTypeId, String), FieldId>,
    pub(crate) command_inputs: BTreeMap<(CommandId, String), FieldId>,
    pub(crate) outcomes: BTreeMap<(CommandId, String), OutcomeId>,
    pub(crate) outcome_fields: BTreeMap<(CommandId, OutcomeId, String), FieldId>,
    pub(crate) enum_variants: BTreeMap<(EnumTypeId, String), EnumVariantId>,
    pub(crate) projection_measures: BTreeMap<(ProjectionId, String), FieldId>,
    pub(crate) indexes: BTreeMap<(EntityTypeId, String), IndexId>,
    pub(crate) entity_invariants: BTreeMap<(EntityTypeId, String), InvariantId>,
    pub(crate) aggregate_invariants: BTreeMap<(AggregateTypeId, String), InvariantId>,
}

/// Validates source namespaces and allocates all genesis stable IDs deterministically.
pub(crate) fn allocate_genesis_symbols(
    document: &ContractDocument,
) -> Result<GenesisSymbols, CompilerDiagnostics> {
    allocate_symbols(document, None)
}

/// Allocates stable IDs from an exact predecessor ledger.
pub(crate) fn allocate_successor_symbols(
    document: &ContractDocument,
    parent: &LineageLedgerV1,
) -> Result<GenesisSymbols, CompilerDiagnostics> {
    allocate_symbols(document, Some(parent))
}

fn allocate_symbols(
    document: &ContractDocument,
    parent: Option<&LineageLedgerV1>,
) -> Result<GenesisSymbols, CompilerDiagnostics> {
    let contract = &document.contract.value;
    let contract_version = match contract
        .version
        .value
        .parse::<u64>()
        .ok()
        .and_then(ContractVersion::new)
    {
        Some(version) => version,
        None => {
            return Err(CompilerDiagnostics::single(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidContractVersion,
                contract.version.span,
            )));
        }
    };

    let mut diagnostics = Vec::new();
    if contract.name.value == "tx" {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::DuplicateName,
            contract.name.span,
        ));
    }

    let mut entity_names = NameCollector::default();
    let mut event_names = NameCollector::default();
    let mut enum_names = NameCollector::default();
    let mut aggregate_names = NameCollector::default();
    let mut command_names = NameCollector::default();
    let mut projection_names = NameCollector::default();

    for declaration in &contract.declarations {
        match &declaration.value {
            Declaration::Entity(entity) => entity_names.insert(&entity.name, &mut diagnostics),
            Declaration::Event(event) => event_names.insert(&event.name, &mut diagnostics),
            Declaration::Enum(enumeration) => {
                enum_names.insert(&enumeration.name, &mut diagnostics)
            }
            Declaration::Aggregate(aggregate) => {
                aggregate_names.insert(&aggregate.name, &mut diagnostics)
            }
            Declaration::Command(command) => command_names.insert(&command.name, &mut diagnostics),
            Declaration::Projection(projection) => {
                projection_names.insert(&projection.name, &mut diagnostics)
            }
        }
    }

    let allocator = StableIdAllocator::new(parent);
    let entities = allocator.allocate_global::<EntityTypeId>(
        StableIdNamespaceTag::Entity,
        &entity_names.names,
        &mut diagnostics,
    );
    let events = allocator.allocate_global::<EventTypeId>(
        StableIdNamespaceTag::Event,
        &event_names.names,
        &mut diagnostics,
    );
    let enums = allocator.allocate_global::<EnumTypeId>(
        StableIdNamespaceTag::Enum,
        &enum_names.names,
        &mut diagnostics,
    );
    let aggregates = allocator.allocate_global::<AggregateTypeId>(
        StableIdNamespaceTag::Aggregate,
        &aggregate_names.names,
        &mut diagnostics,
    );
    let commands = allocator.allocate_global::<CommandId>(
        StableIdNamespaceTag::Command,
        &command_names.names,
        &mut diagnostics,
    );
    let projections = allocator.allocate_global::<ProjectionId>(
        StableIdNamespaceTag::Projection,
        &projection_names.names,
        &mut diagnostics,
    );

    let mut entity_fields = BTreeMap::new();
    let mut event_fields = BTreeMap::new();
    let mut command_inputs = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    let mut outcome_fields = BTreeMap::new();
    let mut enum_variants = BTreeMap::new();
    let mut projection_measures = BTreeMap::new();
    let mut index_identities = Vec::new();
    let mut invariant_identities = Vec::new();

    for declaration in &contract.declarations {
        match &declaration.value {
            Declaration::Entity(entity) => {
                let Some(entity_id) = entities.get(&entity.name.value).copied() else {
                    continue;
                };
                let mut fields = NameCollector::default();
                let mut indexes = NameCollector::default();
                let mut invariants = NameCollector::default();
                let mut key_count = 0_usize;
                for item in &entity.items {
                    match &item.value {
                        EntityItem::Key(key) => {
                            key_count += 1;
                            for field in &key.fields {
                                fields.insert(&field.value.name, &mut diagnostics);
                            }
                        }
                        EntityItem::Field(field) => {
                            fields.insert(&field.name, &mut diagnostics);
                        }
                        EntityItem::Invariant(invariant) => {
                            invariants.insert(&invariant.name, &mut diagnostics);
                        }
                        EntityItem::Index(index) => {
                            indexes.insert(&index.name, &mut diagnostics);
                            validate_unique_spanned_names(&index.fields, &mut diagnostics);
                        }
                        EntityItem::Unique(unique) => {
                            indexes.insert(&unique.name, &mut diagnostics);
                            validate_unique_spanned_names(&unique.fields, &mut diagnostics);
                        }
                        EntityItem::Reference(reference) => {
                            validate_unique_spanned_names(
                                &reference.source_fields,
                                &mut diagnostics,
                            );
                            validate_unique_spanned_names(
                                &reference.target_fields,
                                &mut diagnostics,
                            );
                        }
                    }
                }
                if key_count != 1 {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::MissingDeclaration,
                        entity.name.span,
                    ));
                }
                allocator
                    .allocate_scoped::<FieldId>(
                        StableIdNamespaceTag::Field,
                        0x01,
                        vec![entity_id.get()],
                        &fields.names,
                        &mut diagnostics,
                    )
                    .into_iter()
                    .for_each(|(name, id)| {
                        entity_fields.insert((entity_id, name), id);
                    });
                for name in indexes.names.keys() {
                    index_identities.push((entity_id, name.clone(), indexes.names[name]));
                }
                for name in invariants.names.keys() {
                    invariant_identities.push(InvariantIdentity::Entity(
                        entity_id,
                        name.clone(),
                        invariants.names[name],
                    ));
                }
            }
            Declaration::Event(event) => {
                let Some(event_id) = events.get(&event.name.value).copied() else {
                    continue;
                };
                let mut fields = NameCollector::default();
                for field in &event.fields {
                    fields.insert(&field.value.name, &mut diagnostics);
                }
                allocator
                    .allocate_scoped::<FieldId>(
                        StableIdNamespaceTag::Field,
                        0x02,
                        vec![event_id.get()],
                        &fields.names,
                        &mut diagnostics,
                    )
                    .into_iter()
                    .for_each(|(name, id)| {
                        event_fields.insert((event_id, name), id);
                    });
            }
            Declaration::Enum(enumeration) => {
                let Some(enum_id) = enums.get(&enumeration.name.value).copied() else {
                    continue;
                };
                let mut variants = NameCollector::default();
                for variant in &enumeration.variants {
                    variants.insert(variant, &mut diagnostics);
                }
                allocator
                    .allocate_scoped::<EnumVariantId>(
                        StableIdNamespaceTag::EnumVariant,
                        0x01,
                        vec![enum_id.get()],
                        &variants.names,
                        &mut diagnostics,
                    )
                    .into_iter()
                    .for_each(|(name, id)| {
                        enum_variants.insert((enum_id, name), id);
                    });
            }
            Declaration::Aggregate(aggregate) => {
                let Some(aggregate_id) = aggregates.get(&aggregate.name.value).copied() else {
                    continue;
                };
                let mut invariant_names = NameCollector::default();
                let mut roots = 0_usize;
                let mut partitions = 0_usize;
                let mut conflicts = 0_usize;
                let mut children = NameCollector::default();
                for item in &aggregate.items {
                    match &item.value {
                        AggregateItem::Root(_) => roots += 1,
                        AggregateItem::Child(child) => {
                            children.insert(child, &mut diagnostics);
                        }
                        AggregateItem::PartitionBy(_) => partitions += 1,
                        AggregateItem::ConflictKey(_) => conflicts += 1,
                        AggregateItem::Invariant(invariant) => {
                            invariant_names.insert(&invariant.name, &mut diagnostics);
                        }
                    }
                }
                if roots != 1 || partitions != 1 || conflicts != 1 {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::MissingDeclaration,
                        aggregate.name.span,
                    ));
                }
                for name in invariant_names.names.keys() {
                    invariant_identities.push(InvariantIdentity::Aggregate(
                        aggregate_id,
                        name.clone(),
                        invariant_names.names[name],
                    ));
                }
            }
            Declaration::Command(command) => {
                let Some(command_id) = commands.get(&command.name.value).copied() else {
                    continue;
                };
                allocate_command_symbols(
                    &allocator,
                    command_id,
                    command,
                    &mut command_inputs,
                    &mut outcomes,
                    &mut outcome_fields,
                    &mut diagnostics,
                );
            }
            Declaration::Projection(projection) => {
                let Some(projection_id) = projections.get(&projection.name.value).copied() else {
                    continue;
                };
                let mut measures = NameCollector::default();
                for measure in &projection.measures {
                    measures.insert(&measure.value.name, &mut diagnostics);
                }
                allocator
                    .allocate_scoped::<FieldId>(
                        StableIdNamespaceTag::Field,
                        0x05,
                        vec![projection_id.get()],
                        &measures.names,
                        &mut diagnostics,
                    )
                    .into_iter()
                    .for_each(|(name, id)| {
                        projection_measures.insert((projection_id, name), id);
                    });
            }
        }
    }

    let index_allocation = StableIdAllocationNamespace::global(StableIdNamespaceTag::Index)
        .expect("index global allocation namespace");
    let indexes = allocator.allocate_custom::<_, IndexId>(
        index_allocation,
        index_identities
            .into_iter()
            .map(|(owner, name, span)| {
                (
                    (owner, name.clone()),
                    StableIdNamespace::new(StableIdNamespaceTag::Index, 0x01, vec![owner.get()])
                        .expect("entity index identity namespace"),
                    name,
                    span,
                )
            })
            .collect(),
        &mut diagnostics,
    );

    let mut entity_invariants = BTreeMap::new();
    let mut aggregate_invariants = BTreeMap::new();
    let invariant_allocation = StableIdAllocationNamespace::global(StableIdNamespaceTag::Invariant)
        .expect("invariant global allocation namespace");
    let allocated_invariants = allocator.allocate_custom::<_, InvariantId>(
        invariant_allocation,
        invariant_identities
            .into_iter()
            .map(InvariantIdentity::allocation_entry)
            .collect(),
        &mut diagnostics,
    );
    for (identity, id) in allocated_invariants {
        match identity {
            InvariantIdentityKey::Entity(owner, name) => {
                entity_invariants.insert((owner, name), id);
            }
            InvariantIdentityKey::Aggregate(owner, name) => {
                aggregate_invariants.insert((owner, name), id);
            }
        }
    }

    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }

    Ok(GenesisSymbols {
        contract_version,
        entities,
        events,
        enums,
        aggregates,
        commands,
        projections,
        entity_fields,
        event_fields,
        command_inputs,
        outcomes,
        outcome_fields,
        enum_variants,
        projection_measures,
        indexes,
        entity_invariants,
        aggregate_invariants,
    })
}

fn allocate_command_symbols(
    allocator: &StableIdAllocator<'_>,
    command_id: CommandId,
    command: &CommandDeclaration,
    command_inputs: &mut BTreeMap<(CommandId, String), FieldId>,
    outcomes: &mut BTreeMap<(CommandId, String), OutcomeId>,
    outcome_fields: &mut BTreeMap<(CommandId, OutcomeId, String), FieldId>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let mut inputs = NameCollector::default();
    for input in &command.inputs {
        inputs.insert(&input.value.field.name, diagnostics);
    }
    allocator
        .allocate_scoped::<FieldId>(
            StableIdNamespaceTag::Field,
            0x03,
            vec![command_id.get()],
            &inputs.names,
            diagnostics,
        )
        .into_iter()
        .for_each(|(name, id)| {
            command_inputs.insert((command_id, name), id);
        });

    let mut binding_names = NameCollector::default();
    for binding in &command.bindings {
        let entity_binding = match &binding.value {
            riffdb_contract_syntax::ast::Binding::Read(binding)
            | riffdb_contract_syntax::ast::Binding::Mutate(binding)
            | riffdb_contract_syntax::ast::Binding::Create(binding) => binding,
        };
        binding_names.insert(&entity_binding.binding, diagnostics);
        if let Some(input_span) = inputs.names.get(&entity_binding.binding.value) {
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::DuplicateName,
                    entity_binding.binding.span,
                )
                .with_related_span(*input_span),
            );
        }
    }
    let mut requirement_names = NameCollector::default();
    for requirement in &command.requirements {
        requirement_names.insert(&requirement.value.name, diagnostics);
    }

    let mut rejection_names = BTreeMap::<String, Span>::new();
    let mut outcome_occurrences = Vec::new();
    for binding in &command.bindings {
        let failure = match &binding.value {
            riffdb_contract_syntax::ast::Binding::Read(binding)
            | riffdb_contract_syntax::ast::Binding::Mutate(binding)
            | riffdb_contract_syntax::ast::Binding::Create(binding) => &binding.failure,
        };
        rejection_names
            .entry(failure.value.name.value.clone())
            .or_insert(failure.value.name.span);
        outcome_occurrences.push(&failure.value);
    }
    for requirement in &command.requirements {
        let rejection = &requirement.value.rejection;
        rejection_names
            .entry(rejection.value.name.value.clone())
            .or_insert(rejection.value.name.span);
        outcome_occurrences.push(&rejection.value);
    }
    let success = &command.return_clause.value.outcome.value;
    if let Some(rejection_span) = rejection_names.get(&success.name.value) {
        diagnostics.push(
            CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidOutcome, success.name.span)
                .with_related_span(*rejection_span),
        );
    }
    outcome_occurrences.push(success);

    let outcome_names = outcome_occurrences
        .iter()
        .map(|outcome| (outcome.name.value.clone(), outcome.name.span))
        .collect::<BTreeMap<_, _>>();
    let allocated_outcomes = allocator.allocate_scoped::<OutcomeId>(
        StableIdNamespaceTag::Outcome,
        0x01,
        vec![command_id.get()],
        &outcome_names,
        diagnostics,
    );
    for (name, id) in &allocated_outcomes {
        outcomes.insert((command_id, name.clone()), *id);
    }

    let mut fields_by_outcome = BTreeMap::<String, NameCollector>::new();
    for occurrence in outcome_occurrences {
        validate_object_field_names(&occurrence.payload.value, diagnostics);
        let fields = fields_by_outcome
            .entry(occurrence.name.value.clone())
            .or_default();
        for field in &occurrence.payload.value.fields {
            fields
                .names
                .entry(field.value.name.value.clone())
                .or_insert(field.value.name.span);
        }
    }
    for (outcome_name, mut fields) in fields_by_outcome {
        let Some(outcome_id) = allocated_outcomes.get(&outcome_name).copied() else {
            continue;
        };
        if let Some(span) = fields.names.remove("type") {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidOutcome,
                span,
            ));
        }
        allocator
            .allocate_scoped::<FieldId>(
                StableIdNamespaceTag::Field,
                0x04,
                vec![command_id.get(), outcome_id.get()],
                &fields.names,
                diagnostics,
            )
            .into_iter()
            .for_each(|(field_name, field_id)| {
                outcome_fields.insert((command_id, outcome_id, field_name), field_id);
            });
    }

    let mutating_binding = command.bindings.iter().any(|binding| {
        matches!(
            binding.value,
            riffdb_contract_syntax::ast::Binding::Mutate(_)
                | riffdb_contract_syntax::ast::Binding::Create(_)
        )
    });
    let has_effect = !command.effects.is_empty();
    let mutating = mutating_binding || has_effect;
    if mutating && command.idempotency.is_none() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::MissingIdempotency,
            command.name.span,
        ));
    }
}

fn validate_object_field_names(object: &ObjectLiteral, diagnostics: &mut Vec<CompilerDiagnostic>) {
    let fields = object
        .fields
        .iter()
        .map(|field| field.value.name.clone())
        .collect::<Vec<_>>();
    validate_unique_spanned_names(&fields, diagnostics);
}

fn validate_unique_spanned_names(
    names: &[Spanned<String>],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let mut collector = NameCollector::default();
    for name in names {
        collector.insert(name, diagnostics);
    }
}

#[derive(Default)]
struct NameCollector {
    names: BTreeMap<String, Span>,
}

impl NameCollector {
    fn insert(&mut self, name: &Spanned<String>, diagnostics: &mut Vec<CompilerDiagnostic>) {
        if name.value == "tx" {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::DuplicateName,
                name.span,
            ));
            return;
        }
        if let Some(first_span) = self.names.get(&name.value) {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::DuplicateName, name.span)
                    .with_related_span(*first_span),
            );
        } else {
            self.names.insert(name.value.clone(), name.span);
        }
    }
}

trait AllocatableId: Copy + Ord {
    fn from_nonzero(value: u32) -> Option<Self>;
}

macro_rules! impl_allocatable_id {
    ($($id:ty),+ $(,)?) => {
        $(
            impl AllocatableId for $id {
                fn from_nonzero(value: u32) -> Option<Self> {
                    Self::new(value)
                }
            }
        )+
    };
}

impl_allocatable_id!(
    AggregateTypeId,
    CommandId,
    EntityTypeId,
    EnumTypeId,
    EnumVariantId,
    EventTypeId,
    FieldId,
    IndexId,
    InvariantId,
    OutcomeId,
    ProjectionId,
);

struct StableIdAllocator<'a> {
    parent: Option<&'a LineageLedgerV1>,
}

impl<'a> StableIdAllocator<'a> {
    const fn new(parent: Option<&'a LineageLedgerV1>) -> Self {
        Self { parent }
    }

    fn allocate_global<I: AllocatableId>(
        &self,
        tag: StableIdNamespaceTag,
        names: &BTreeMap<String, Span>,
        diagnostics: &mut Vec<CompilerDiagnostic>,
    ) -> BTreeMap<String, I> {
        let allocation =
            StableIdAllocationNamespace::global(tag).expect("caller passes a global stable-ID tag");
        let namespace = StableIdNamespace::new(tag, 0, vec![]).expect("global identity namespace");
        self.allocate_custom(
            allocation,
            names
                .iter()
                .map(|(name, span)| (name.clone(), namespace.clone(), name.clone(), *span))
                .collect(),
            diagnostics,
        )
    }

    fn allocate_scoped<I: AllocatableId>(
        &self,
        tag: StableIdNamespaceTag,
        owner_kind: u8,
        owner_ids: Vec<u32>,
        names: &BTreeMap<String, Span>,
        diagnostics: &mut Vec<CompilerDiagnostic>,
    ) -> BTreeMap<String, I> {
        let allocation = StableIdAllocationNamespace::scoped(tag, owner_kind, owner_ids.clone())
            .expect("caller passes a scoped stable-ID tag and owner");
        let namespace =
            StableIdNamespace::new(tag, owner_kind, owner_ids).expect("scoped identity namespace");
        self.allocate_custom(
            allocation,
            names
                .iter()
                .map(|(name, span)| (name.clone(), namespace.clone(), name.clone(), *span))
                .collect(),
            diagnostics,
        )
    }

    fn allocate_custom<K: Clone + Ord, I: AllocatableId>(
        &self,
        allocation: StableIdAllocationNamespace,
        desired: Vec<(K, StableIdNamespace, String, Span)>,
        diagnostics: &mut Vec<CompilerDiagnostic>,
    ) -> BTreeMap<K, I> {
        let parent_allocation = self.parent.and_then(|ledger| {
            ledger
                .allocations()
                .iter()
                .find(|candidate| candidate.namespace() == &allocation)
        });
        let mut resolved = BTreeMap::new();
        let mut additions = Vec::new();
        for (key, namespace, name, span) in desired {
            let identity = match StableIdentity::new(namespace, name) {
                Ok(identity) => identity,
                Err(_) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::StableIdAllocation,
                        span,
                    ));
                    continue;
                }
            };
            match parent_allocation.and_then(|parent| {
                parent
                    .entries()
                    .iter()
                    .find(|entry| entry.identity() == &identity)
            }) {
                Some(entry) if entry.state() == LineageEntryState::Active => {
                    if let Some(id) = I::from_nonzero(entry.id()) {
                        resolved.insert(key, id);
                    } else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::StableIdAllocation,
                            span,
                        ));
                    }
                }
                Some(_) => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::StableIdAllocation,
                    span,
                )),
                None => additions.push((key, identity, span)),
            }
        }
        additions.sort_by(|left, right| canonical_identity_cmp(&left.1, &right.1));
        let mut next =
            parent_allocation.map_or(Some(1), |parent| parent.max_allocated().checked_add(1));
        for (key, _, span) in additions {
            let Some(value) = next else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::StableIdAllocation,
                    span,
                ));
                continue;
            };
            let Some(id) = I::from_nonzero(value) else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::StableIdAllocation,
                    span,
                ));
                next = None;
                continue;
            };
            resolved.insert(key, id);
            next = value.checked_add(1);
        }
        resolved
    }
}

fn canonical_identity_cmp(left: &StableIdentity, right: &StableIdentity) -> std::cmp::Ordering {
    left.namespace()
        .tag()
        .cmp(&right.namespace().tag())
        .then_with(|| {
            left.namespace()
                .owner_kind()
                .cmp(&right.namespace().owner_kind())
        })
        .then_with(|| {
            left.namespace()
                .owner_ids()
                .len()
                .cmp(&right.namespace().owner_ids().len())
        })
        .then_with(|| {
            left.namespace()
                .owner_ids()
                .cmp(right.namespace().owner_ids())
        })
        .then_with(|| canonical_name_cmp(left.name(), right.name()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InvariantIdentity {
    Entity(EntityTypeId, String, Span),
    Aggregate(AggregateTypeId, String, Span),
}

impl InvariantIdentity {
    fn allocation_entry(self) -> (InvariantIdentityKey, StableIdNamespace, String, Span) {
        match self {
            Self::Entity(owner, name, span) => (
                InvariantIdentityKey::Entity(owner, name.clone()),
                StableIdNamespace::new(StableIdNamespaceTag::Invariant, 0x01, vec![owner.get()])
                    .expect("entity invariant identity namespace"),
                name,
                span,
            ),
            Self::Aggregate(owner, name, span) => (
                InvariantIdentityKey::Aggregate(owner, name.clone()),
                StableIdNamespace::new(StableIdNamespaceTag::Invariant, 0x02, vec![owner.get()])
                    .expect("aggregate invariant identity namespace"),
                name,
                span,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum InvariantIdentityKey {
    Entity(EntityTypeId, String),
    Aggregate(AggregateTypeId, String),
}

fn canonical_name_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    let left_length = u16::try_from(left.len()).expect("parser name bound fits u16");
    let right_length = u16::try_from(right.len()).expect("parser name bound fits u16");
    left_length
        .to_be_bytes()
        .cmp(&right_length.to_be_bytes())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;

    const MINIMAL: &str = r#"
contract Example version 1 {
  entity Alpha { key (id: uuid) field value: i64 }
  entity Z { key (id: uuid) field value: i64 }
  aggregate Longer { root Alpha partition_by id conflict_key (id) }
  aggregate A { root Z partition_by id conflict_key (id) }
  command AlphaCommand {
    input lookup_id: uuid
    read Alpha(lookup_id) as row else Missing { lookup_id: lookup_id }
    return Found { row: row }
  }
  command Z {
    input lookup_id: uuid
    read Z(lookup_id) as row else Missing { lookup_id: lookup_id }
    return Found { row: row }
  }
}
"#;

    #[test]
    fn genesis_ids_follow_sorted_identity_keys_not_declaration_order() {
        let document = parse_contract(MINIMAL).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document).expect("valid symbols");
        assert_eq!(symbols.entities["Z"].get(), 1);
        assert_eq!(symbols.entities["Alpha"].get(), 2);
        assert_eq!(symbols.commands["Z"].get(), 1);
        assert_eq!(symbols.commands["AlphaCommand"].get(), 2);
        assert_eq!(
            symbols.entity_fields[&(symbols.entities["Alpha"], "id".to_owned())].get(),
            1
        );
        assert_eq!(
            symbols.entity_fields[&(symbols.entities["Alpha"], "value".to_owned())].get(),
            2
        );
    }

    #[test]
    fn duplicate_names_report_both_source_spans() {
        let source = "contract Example version 1 { enum State { Open, Open } }";
        let document = parse_contract(source).expect("valid syntax");
        let diagnostics = allocate_genesis_symbols(&document).expect_err("duplicate rejects");
        let duplicate = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::DuplicateName)
            .expect("duplicate diagnostic");
        assert!(duplicate.related_span().is_some());
        assert!(duplicate.primary_span().start() > duplicate.related_span().unwrap().start());
    }

    #[test]
    fn mutating_command_requires_idempotency() {
        let source = r#"
contract Example version 1 {
  entity Row { key (id: uuid) field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input id: uuid
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#;
        let document = parse_contract(source).expect("valid syntax");
        let diagnostics = allocate_genesis_symbols(&document).expect_err("missing key rejects");
        assert!(
            diagnostics
                .as_slice()
                .iter()
                .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::MissingIdempotency)
        );
    }
}
