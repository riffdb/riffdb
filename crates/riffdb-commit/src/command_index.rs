//! Pure grammar/IR-v1 secondary-index derivation.
//!
//! This module is deliberately sealed until the coordinator carries the exact
//! validated command attempt into derivation by construction.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_contract_ir::{
    BindingMode, EXECUTABLE_IR_VERSION_V1, ExecutionClass, GRAMMAR_VERSION_V1, IndexSchema,
};
use riffdb_storage_api::{
    AffectedIndexEpochTargets, DurableKeySchemaBindingV1, EntityMutation, EntityObservation,
    EntityTarget, EvaluatedCommand, IndexEntryMutationV1, IndexRangePrefixBuilder,
    IndexRangeTarget, MAX_AFFECTED_INDEX_EPOCH_TARGETS, MAX_INDEX_DELTAS, MAX_READ_SNAPSHOT_BYTES,
    MAX_VALIDATION_TARGETS, StoredIndexEntryV1, TransactionCurrentState,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, IndexEntryKey};

const AFFECTED_CURRENT_STATE_FIXED_BYTES_V1: usize = 4;
const INDEX_RANGE_TARGET_FIXED_BYTES_V1: usize = 8;
const MAX_INDEX_EPOCH_POSITION_BYTES_V1: usize = 9;

#[derive(Clone, Copy, Eq, PartialEq)]
struct CommandIndexError {
    _private: (),
}

impl CommandIndexError {
    const fn internal_defect() -> Self {
        Self { _private: () }
    }
}

impl std::fmt::Debug for CommandIndexError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandIndexError([REDACTED])")
    }
}

struct DerivedCommandIndexes {
    entry_mutations: Vec<IndexEntryMutationV1>,
    affected_targets: AffectedIndexEpochTargets,
}

struct IndexDerivationBuilder {
    entry_mutations: Vec<IndexEntryMutationV1>,
    entry_keys: BTreeSet<IndexEntryKey>,
    affected_targets: BTreeSet<IndexRangeTarget>,
    validation_positions: usize,
    affected_current_semantic_bytes: usize,
}

impl IndexDerivationBuilder {
    fn new(binding_count: usize, root_count: usize) -> Result<Self, CommandIndexError> {
        let validation_positions = binding_count
            .checked_add(root_count)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if validation_positions > MAX_VALIDATION_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        Ok(Self {
            entry_mutations: Vec::new(),
            entry_keys: BTreeSet::new(),
            affected_targets: BTreeSet::new(),
            validation_positions,
            affected_current_semantic_bytes: AFFECTED_CURRENT_STATE_FIXED_BYTES_V1,
        })
    }

    fn push_entry(&mut self, mutation: IndexEntryMutationV1) -> Result<(), CommandIndexError> {
        if self.entry_mutations.len() >= MAX_INDEX_DELTAS
            || !self.entry_keys.insert(mutation.key().clone())
        {
            return Err(CommandIndexError::internal_defect());
        }
        self.entry_mutations.push(mutation);
        Ok(())
    }

    fn insert_target(&mut self, target: IndexRangeTarget) -> Result<(), CommandIndexError> {
        if self.affected_targets.contains(&target) {
            return Ok(());
        }
        if self.affected_targets.len() >= MAX_AFFECTED_INDEX_EPOCH_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        let next_positions = self
            .validation_positions
            .checked_add(1)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_positions > MAX_VALIDATION_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        let observation_bytes = target
            .prefix()
            .as_bytes()
            .len()
            .checked_add(INDEX_RANGE_TARGET_FIXED_BYTES_V1)
            .and_then(|bytes| bytes.checked_add(MAX_INDEX_EPOCH_POSITION_BYTES_V1))
            .ok_or_else(CommandIndexError::internal_defect)?;
        let next_bytes = self
            .affected_current_semantic_bytes
            .checked_add(observation_bytes)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(CommandIndexError::internal_defect());
        }
        self.affected_targets.insert(target);
        self.validation_positions = next_positions;
        self.affected_current_semantic_bytes = next_bytes;
        Ok(())
    }

    fn finish(mut self) -> Result<DerivedCommandIndexes, CommandIndexError> {
        self.entry_mutations
            .sort_unstable_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
        if self
            .entry_mutations
            .windows(2)
            .any(|pair| pair[0].key().as_bytes() >= pair[1].key().as_bytes())
        {
            return Err(CommandIndexError::internal_defect());
        }
        let affected_targets =
            AffectedIndexEpochTargets::new(self.affected_targets.into_iter().collect())
                .map_err(|_| CommandIndexError::internal_defect())?;
        Ok(DerivedCommandIndexes {
            entry_mutations: self.entry_mutations,
            affected_targets,
        })
    }
}

fn derive_grammar_v1_indexes(
    resolved: &ResolvedExecutablePlan,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<DerivedCommandIndexes, CommandIndexError> {
    let bundle = resolved.bundle().bundle();
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    if bundle.grammar_version() != GRAMMAR_VERSION_V1
        || bundle.ir_version() != EXECUTABLE_IR_VERSION_V1
        || plan.execution_class() != ExecutionClass::IdempotentMutation
        || resolved.reference() != evaluated.plan()
        || request.plan() != evaluated.plan()
        || evaluated.mutations().is_empty()
        || plan.bindings().len() != request.binding_targets().len()
        || plan.bindings().len() != current.bindings().len()
        || plan.root_validation_reads().len() != request.root_validation_targets().len()
        || plan.root_validation_reads().len() != current.root_validations().len()
        || !request.range_targets().is_empty()
        || !current.ranges().is_empty()
    {
        return Err(CommandIndexError::internal_defect());
    }
    for (position, ((read, target), observation)) in plan
        .root_validation_reads()
        .iter()
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
        .enumerate()
    {
        if read.id().get() as usize != position
            || read.entity_type() != target.entity_type_id()
            || observation.target() != target
        {
            return Err(CommandIndexError::internal_defect());
        }
    }

    let mutation_positions = exact_mutation_positions(resolved, evaluated, current)?;
    let mut builder =
        IndexDerivationBuilder::new(plan.bindings().len(), plan.root_validation_reads().len())?;
    let schema_binding = DurableKeySchemaBindingV1::from_plan(resolved.reference());
    let empty_covered =
        CanonicalRecord::new(Vec::new()).map_err(|_| CommandIndexError::internal_defect())?;

    for (binding_position, binding) in plan.bindings().iter().enumerate() {
        let Some(mutation_position) = mutation_positions[binding_position] else {
            continue;
        };
        let mutation = &evaluated.mutations()[mutation_position];
        let entity = bundle
            .schema()
            .entity(binding.entity_type())
            .ok_or_else(CommandIndexError::internal_defect)?;
        if binding.key_schema() != entity.primary_key() {
            return Err(CommandIndexError::internal_defect());
        }

        let current_record = match (binding.mode(), &current.bindings()[binding_position]) {
            (BindingMode::Create, EntityObservation::Absent(_)) => None,
            (BindingMode::Mutate, EntityObservation::Present(record)) => Some(record.fields()),
            (BindingMode::Read | BindingMode::Create | BindingMode::Mutate, _) => {
                return Err(CommandIndexError::internal_defect());
            }
        };
        for index in entity.indexes() {
            let new_values = index_values(index, mutation.post_image().fields())?;
            let new_key = index
                .key_schema()
                .encode_index(&new_values, mutation.target().key().clone())
                .map_err(|_| CommandIndexError::internal_defect())?;
            match current_record {
                None => {
                    builder.push_entry(IndexEntryMutationV1::Put(
                        StoredIndexEntryV1::new(
                            new_key,
                            schema_binding.clone(),
                            empty_covered.clone(),
                        )
                        .map_err(|_| CommandIndexError::internal_defect())?,
                    ))?;
                    insert_prefixes(index, &new_values, &mut builder)?;
                }
                Some(record) => {
                    let old_values = index_values(index, record)?;
                    let old_key = index
                        .key_schema()
                        .encode_index(&old_values, mutation.target().key().clone())
                        .map_err(|_| CommandIndexError::internal_defect())?;
                    if old_key == new_key {
                        continue;
                    }
                    builder.push_entry(IndexEntryMutationV1::Delete(old_key))?;
                    builder.push_entry(IndexEntryMutationV1::Put(
                        StoredIndexEntryV1::new(
                            new_key,
                            schema_binding.clone(),
                            empty_covered.clone(),
                        )
                        .map_err(|_| CommandIndexError::internal_defect())?,
                    ))?;
                    insert_prefixes(index, &old_values, &mut builder)?;
                    insert_prefixes(index, &new_values, &mut builder)?;
                }
            }
        }
    }
    builder.finish()
}

fn exact_mutation_positions(
    resolved: &ResolvedExecutablePlan,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<Box<[Option<usize>]>, CommandIndexError> {
    let plan = resolved.plan();
    let mutable_count = plan
        .bindings()
        .iter()
        .filter(|binding| binding.mode() != BindingMode::Read)
        .count();
    if mutable_count != evaluated.mutations().len() {
        return Err(CommandIndexError::internal_defect());
    }
    let mut mutations = BTreeMap::<EntityTarget, usize>::new();
    for (position, mutation) in evaluated.mutations().iter().enumerate() {
        if mutations
            .insert(mutation.target().clone(), position)
            .is_some()
        {
            return Err(CommandIndexError::internal_defect());
        }
    }

    let mut positions = vec![None; plan.bindings().len()];
    let mut mutable_targets = BTreeSet::new();
    for (position, ((binding, target), observation)) in plan
        .bindings()
        .iter()
        .zip(evaluated.validation_request().binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        if binding.id().get() as usize != position
            || binding.entity_type() != target.entity_type_id()
            || observation.target() != target
        {
            return Err(CommandIndexError::internal_defect());
        }
        if binding.mode() == BindingMode::Read {
            continue;
        }
        if !mutable_targets.insert(target.clone()) {
            return Err(CommandIndexError::internal_defect());
        }
        let mutation_position = mutations
            .remove(target)
            .ok_or_else(CommandIndexError::internal_defect)?;
        let mutation = &evaluated.mutations()[mutation_position];
        if mutation.post_image().written_by_contract() != plan.contract_version() {
            return Err(CommandIndexError::internal_defect());
        }
        match (binding.mode(), mutation, observation) {
            (BindingMode::Create, EntityMutation::Create(_), EntityObservation::Absent(_)) => {}
            (
                BindingMode::Mutate,
                EntityMutation::Replace {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (BindingMode::Read | BindingMode::Create | BindingMode::Mutate, _, _) => {
                return Err(CommandIndexError::internal_defect());
            }
        }
        positions[position] = Some(mutation_position);
    }
    if !mutations.is_empty() {
        return Err(CommandIndexError::internal_defect());
    }
    Ok(positions.into_boxed_slice())
}

fn index_values(
    index: &IndexSchema,
    record: &CanonicalRecord,
) -> Result<Vec<CanonicalValue>, CommandIndexError> {
    index
        .fields()
        .iter()
        .map(|field| {
            record
                .fields()
                .binary_search_by_key(field, |(candidate, _)| *candidate)
                .ok()
                .map(|position| record.fields()[position].1.clone())
                .ok_or_else(CommandIndexError::internal_defect)
        })
        .collect()
}

fn insert_prefixes(
    index: &IndexSchema,
    values: &[CanonicalValue],
    builder: &mut IndexDerivationBuilder,
) -> Result<(), CommandIndexError> {
    if values.len() != index.fields().len() {
        return Err(CommandIndexError::internal_defect());
    }
    let mut storage_prefix = IndexRangePrefixBuilder::new(index.id());
    for component_count in 0..=values.len() {
        if let Some(value) = component_count
            .checked_sub(1)
            .and_then(|position| values.get(position))
        {
            push_storage_prefix_component(&mut storage_prefix, value)?;
        }
        let ir_prefix = index
            .key_schema()
            .encode_index_prefix(&values[..component_count])
            .map_err(|_| CommandIndexError::internal_defect())?;
        let storage = storage_prefix.clone().finish();
        if storage.index_id() != ir_prefix.index_id() || storage.as_bytes() != ir_prefix.as_bytes()
        {
            return Err(CommandIndexError::internal_defect());
        }
        builder.insert_target(IndexRangeTarget::new(storage))?;
    }
    Ok(())
}

fn push_storage_prefix_component(
    builder: &mut IndexRangePrefixBuilder,
    value: &CanonicalValue,
) -> Result<(), CommandIndexError> {
    let result = match value {
        CanonicalValue::Bool(value) => builder.push_bool(*value),
        CanonicalValue::I64(value) => builder.push_i64(*value),
        CanonicalValue::U64(value) => builder.push_u64(*value),
        CanonicalValue::String(value) => builder.push_str(value.as_str()),
        CanonicalValue::Bytes(value) => builder.push_bytes(value.as_bytes()),
        CanonicalValue::Timestamp(value) => builder.push_timestamp(*value),
        CanonicalValue::Date(value) => builder.push_date(*value),
        CanonicalValue::Uuid(value) => builder.push_uuid(value),
        CanonicalValue::Enum { variant_id, .. } => builder.push_enum_variant(*variant_id),
        CanonicalValue::Null
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_) => return Err(CommandIndexError::internal_defect()),
    };
    result
        .map(|_| ())
        .map_err(|_| CommandIndexError::internal_defect())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{EntitySchema, RecordSchema};
    use riffdb_invariant::derive_input_command_facts;
    use riffdb_runtime::{ExecutionResult, TransactionContext, execute_command};
    use riffdb_storage_api::{
        EvaluationBudget, ExecutablePlanRef, ReadSnapshot, SnapshotRequest, StoredEntityRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, CanonicalValue, Date, EntityVersion, IndexId,
        LogicalTime, RequestId, TenantScope, Timestamp,
    };

    use super::*;

    const INDEXED_SOURCE: &str = r#"
contract IndexedRows version 1 {
  entity Row {
    key (id: uuid)
    field tenant: uuid
    field category: string<32>
    field score: i64
    index by_tenant_category (tenant, category)
    index by_score (score)
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command CreateRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    create Row(id) as row else AlreadyExists {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    return Created { row: row }
  }
  command ChangeRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    return Changed { row: row }
  }
}
"#;

    const SCALAR_SOURCE: &str = r#"
contract ScalarPrefixes version 1 {
  enum State { Ready, Stopped }
  entity Scalar {
    key (id: uuid)
    field flag: bool
    field unsigned: u64
    field signed: i64
    field at: timestamp
    field day: date
    field status: State
    field other_id: uuid
    field blob: bytes<16>
    field text: string<16>
    index by_all (flag, unsigned, signed, at, day, status, other_id, blob, text)
  }
}
"#;

    struct Fixture {
        resolved: ResolvedExecutablePlan,
        evaluated: EvaluatedCommand,
        current: TransactionCurrentState,
    }

    fn fixture(
        command_name: &str,
        request_key: &str,
        next: ([u8; 16], &str, i64),
        old: Option<([u8; 16], &str, i64)>,
    ) -> Fixture {
        let compiled = compile_contract_source(INDEXED_SOURCE).expect("indexed source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("indexed bundle validates");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|candidate| candidate.name() == command_name)
            .expect("fixture command");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let input = named_record(
            plan.input().record(),
            &[
                ("request_key", string(request_key)),
                ("id", CanonicalValue::Uuid([0x11; 16])),
                ("tenant", CanonicalValue::Uuid(next.0)),
                ("category", string(next.1)),
                ("score", CanonicalValue::I64(next.2)),
            ],
        );
        let facts = derive_input_command_facts(plan, input.clone()).expect("input facts");
        let target = EntityTarget::new(
            plan.bindings()[0].entity_type(),
            facts.binding_entity_keys()[0].clone(),
        )
        .expect("binding target");
        let entity = bundle
            .bundle()
            .schema()
            .entity(target.entity_type_id())
            .expect("row schema");
        let observation = old.map_or_else(
            || EntityObservation::Absent(target.clone()),
            |old| {
                EntityObservation::Present(
                    StoredEntityRecordV1::new(
                        target.clone(),
                        EntityVersion::first(),
                        plan.contract_version(),
                        DurableKeySchemaBindingV1::from_plan(&reference),
                        entity_record(
                            entity,
                            &target,
                            &[
                                ("tenant", CanonicalValue::Uuid(old.0)),
                                ("category", string(old.1)),
                                ("score", CanonicalValue::I64(old.2)),
                            ],
                        ),
                    )
                    .expect("stored row"),
                )
            },
        );
        let request = SnapshotRequest::new(reference.clone(), vec![target], Vec::new(), Vec::new())
            .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &request,
            None,
            vec![observation.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("snapshot");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(1, [0x31; 10]).expect("request ID"),
            AdmittedActorContext::new(
                ActorId::new("command-index-test").expect("actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            reference.clone(),
            LogicalTime::new(Timestamp::new(100, 2).expect("logical time")),
            facts.partition_key().clone(),
        );
        let ExecutionResult::CommitRequired(evaluated) = execute_command(
            bundle.bundle(),
            &input,
            &snapshot,
            &context,
            EvaluationBudget::v1(),
        )
        .expect("command evaluates") else {
            panic!("fixture command must commit")
        };
        let current = TransactionCurrentState::new(
            evaluated.validation_request(),
            vec![observation],
            Vec::new(),
            Vec::new(),
        )
        .expect("transaction-current state");
        Fixture {
            resolved: bundle.resolve_plan(&reference).expect("resolved plan"),
            evaluated,
            current,
        }
    }

    fn named_record(schema: &RecordSchema, supplied: &[(&str, CanonicalValue)]) -> CanonicalRecord {
        let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        supplied
                            .get(field.name())
                            .cloned()
                            .unwrap_or_else(|| panic!("missing field {}", field.name())),
                    )
                })
                .collect(),
        )
        .expect("canonical named record")
    }

    fn entity_record(
        entity: &EntitySchema,
        target: &EntityTarget,
        supplied: &[(&str, CanonicalValue)],
    ) -> CanonicalRecord {
        let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
        let key_values = entity
            .primary_key()
            .decode_entity(target.key())
            .expect("entity key");
        CanonicalRecord::new(
            entity
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = entity
                        .primary_key_fields()
                        .iter()
                        .position(|candidate| *candidate == field.id())
                        .map(|position| key_values[position].clone())
                        .or_else(|| supplied.get(field.name()).cloned())
                        .unwrap_or_else(|| panic!("missing entity field {}", field.name()));
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("canonical entity record")
    }

    fn string(value: &str) -> CanonicalValue {
        CanonicalValue::string(value).expect("bounded string")
    }

    fn index<'a>(fixture: &'a Fixture, name: &str) -> &'a IndexSchema {
        fixture.resolved.bundle().bundle().schema().entities()[0]
            .indexes()
            .iter()
            .find(|index| index.name() == name)
            .expect("named index")
    }

    fn expected_prefix_bytes(
        indexes: &[(&IndexSchema, Vec<Vec<CanonicalValue>>)],
    ) -> BTreeSet<Vec<u8>> {
        indexes
            .iter()
            .flat_map(|(index, value_sets)| {
                value_sets.iter().flat_map(|values| {
                    (0..=values.len()).map(|count| {
                        index
                            .key_schema()
                            .encode_index_prefix(&values[..count])
                            .expect("expected prefix")
                            .as_bytes()
                            .to_vec()
                    })
                })
            })
            .collect()
    }

    fn actual_prefix_bytes(derived: &DerivedCommandIndexes) -> BTreeSet<Vec<u8>> {
        derived
            .affected_targets
            .as_slice()
            .iter()
            .map(|target| target.prefix().as_bytes().to_vec())
            .collect()
    }

    fn actual_entry_kinds(derived: &DerivedCommandIndexes) -> BTreeSet<(Vec<u8>, bool)> {
        derived
            .entry_mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key().as_bytes().to_vec(),
                    matches!(mutation, IndexEntryMutationV1::Put(_)),
                )
            })
            .collect()
    }

    #[test]
    fn create_puts_each_index_with_empty_covered_values_and_all_new_prefixes() {
        let fixture = fixture("CreateRow", "create-1", ([0x21; 16], "new", 10), None);
        let derived =
            derive_grammar_v1_indexes(&fixture.resolved, &fixture.evaluated, &fixture.current)
                .expect("create indexes");
        assert_eq!(derived.entry_mutations.len(), 2);
        assert!(
            derived
                .entry_mutations
                .windows(2)
                .all(|pair| { pair[0].key().as_bytes() < pair[1].key().as_bytes() })
        );
        for mutation in &derived.entry_mutations {
            let IndexEntryMutationV1::Put(record) = mutation else {
                panic!("create must only put")
            };
            assert!(record.covered_values().is_empty());
            assert!(
                record
                    .schema_binding()
                    .matches_plan(fixture.resolved.reference())
            );
        }
        let tenant = index(&fixture, "by_tenant_category");
        let score = index(&fixture, "by_score");
        let entity_key = fixture.evaluated.mutations()[0].target().key().clone();
        assert_eq!(
            actual_entry_kinds(&derived),
            BTreeSet::from([
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("new")],
                            entity_key.clone(),
                        )
                        .expect("tenant index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(10)], entity_key)
                        .expect("score index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
            ])
        );
        assert_eq!(
            actual_prefix_bytes(&derived),
            expected_prefix_bytes(&[
                (
                    tenant,
                    vec![vec![CanonicalValue::Uuid([0x21; 16]), string("new")]],
                ),
                (score, vec![vec![CanonicalValue::I64(10)]]),
            ])
        );
        assert_eq!(derived.affected_targets.as_slice().len(), 5);
    }

    #[test]
    fn mutate_skips_unchanged_indexes_and_unions_changed_old_and_new_prefixes() {
        let unchanged = fixture(
            "ChangeRow",
            "unchanged-1",
            ([0x21; 16], "old", 10),
            Some(([0x21; 16], "old", 10)),
        );
        let unchanged = derive_grammar_v1_indexes(
            &unchanged.resolved,
            &unchanged.evaluated,
            &unchanged.current,
        )
        .expect("unchanged indexes");
        assert!(unchanged.entry_mutations.is_empty());
        assert!(unchanged.affected_targets.as_slice().is_empty());

        let changed = fixture(
            "ChangeRow",
            "changed-1",
            ([0x21; 16], "new", 20),
            Some(([0x21; 16], "old", 10)),
        );
        let derived =
            derive_grammar_v1_indexes(&changed.resolved, &changed.evaluated, &changed.current)
                .expect("changed indexes");
        assert_eq!(derived.entry_mutations.len(), 4);
        assert_eq!(
            derived
                .entry_mutations
                .iter()
                .filter(|mutation| matches!(mutation, IndexEntryMutationV1::Delete(_)))
                .count(),
            2
        );
        assert_eq!(
            derived
                .entry_mutations
                .iter()
                .filter(|mutation| matches!(mutation, IndexEntryMutationV1::Put(_)))
                .count(),
            2
        );
        let tenant = index(&changed, "by_tenant_category");
        let score = index(&changed, "by_score");
        let entity_key = changed.evaluated.mutations()[0].target().key().clone();
        assert!(
            derived
                .entry_mutations
                .windows(2)
                .all(|pair| pair[0].key().as_bytes() < pair[1].key().as_bytes())
        );
        for mutation in &derived.entry_mutations {
            if let IndexEntryMutationV1::Put(record) = mutation {
                assert!(record.covered_values().is_empty());
            }
        }
        assert_eq!(
            actual_entry_kinds(&derived),
            BTreeSet::from([
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("old")],
                            entity_key.clone(),
                        )
                        .expect("old tenant index key")
                        .as_bytes()
                        .to_vec(),
                    false,
                ),
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("new")],
                            entity_key.clone(),
                        )
                        .expect("new tenant index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(10)], entity_key.clone())
                        .expect("old score index key")
                        .as_bytes()
                        .to_vec(),
                    false,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(20)], entity_key)
                        .expect("new score index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
            ])
        );
        assert_eq!(
            actual_prefix_bytes(&derived),
            expected_prefix_bytes(&[
                (
                    tenant,
                    vec![
                        vec![CanonicalValue::Uuid([0x21; 16]), string("old")],
                        vec![CanonicalValue::Uuid([0x21; 16]), string("new")],
                    ],
                ),
                (
                    score,
                    vec![vec![CanonicalValue::I64(10)], vec![CanonicalValue::I64(20)]],
                ),
            ])
        );
        assert_eq!(derived.affected_targets.as_slice().len(), 7);
    }

    #[test]
    fn storage_prefix_builder_matches_ir_for_every_v1_key_scalar() {
        let compiled = compile_contract_source(SCALAR_SOURCE).expect("scalar source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("scalar bundle validates");
        let schema = bundle.bundle().schema();
        let index = &schema.entities()[0].indexes()[0];
        let enumeration = &schema.enums()[0];
        let values = vec![
            CanonicalValue::Bool(true),
            CanonicalValue::U64(7),
            CanonicalValue::I64(-8),
            CanonicalValue::Timestamp(Timestamp::new(-9, 10).expect("timestamp")),
            CanonicalValue::Date(Date::new(-11)),
            CanonicalValue::Enum {
                type_id: enumeration.id(),
                variant_id: enumeration.variants()[0].id(),
            },
            CanonicalValue::Uuid([0x12; 16]),
            CanonicalValue::bytes(vec![0, 1, 2]).expect("bytes"),
            string("exact"),
        ];
        let mut builder = IndexDerivationBuilder::new(0, 0).expect("builder");
        insert_prefixes(index, &values, &mut builder).expect("cross-checked prefixes");
        let derived = builder.finish().expect("derived prefixes");
        assert_eq!(derived.affected_targets.as_slice().len(), values.len() + 1);
        assert_eq!(
            actual_prefix_bytes(&derived),
            expected_prefix_bytes(&[(index, vec![values])])
        );
    }

    #[test]
    fn incremental_guards_reject_duplicate_deltas_and_each_exact_plus_one_bound() {
        let fixture = fixture("CreateRow", "bounds-1", ([0x21; 16], "new", 10), None);
        let derived =
            derive_grammar_v1_indexes(&fixture.resolved, &fixture.evaluated, &fixture.current)
                .expect("fixture indexes");
        let entry = derived.entry_mutations[0].clone();
        let mut duplicate = IndexDerivationBuilder::new(0, 0).expect("builder");
        duplicate.push_entry(entry.clone()).expect("first key");
        assert_eq!(
            duplicate.push_entry(entry.clone()),
            Err(CommandIndexError::internal_defect())
        );

        let mut delta_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        delta_limit.entry_mutations = vec![entry; MAX_INDEX_DELTAS - 1];
        delta_limit
            .push_entry(derived.entry_mutations[1].clone())
            .expect("exact delta bound");
        assert_eq!(delta_limit.entry_mutations.len(), MAX_INDEX_DELTAS);
        assert_eq!(
            delta_limit.push_entry(derived.entry_mutations[1].clone()),
            Err(CommandIndexError::internal_defect())
        );

        assert!(IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 0).is_ok());
        assert_eq!(
            IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 1)
                .err()
                .expect("positions over limit"),
            CommandIndexError::internal_defect()
        );
        let mut position_limit =
            IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 0).expect("exact positions");
        assert_eq!(
            position_limit.insert_target(IndexRangeTarget::new(
                IndexRangePrefixBuilder::new(IndexId::first()).finish()
            )),
            Err(CommandIndexError::internal_defect())
        );

        let mut target_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        for raw in 1..=u32::try_from(MAX_AFFECTED_INDEX_EPOCH_TARGETS).expect("u32 bound") {
            target_limit
                .insert_target(IndexRangeTarget::new(
                    IndexRangePrefixBuilder::new(IndexId::new(raw).expect("index ID")).finish(),
                ))
                .expect("exact affected-target bound");
        }
        assert_eq!(
            target_limit.affected_targets.len(),
            MAX_AFFECTED_INDEX_EPOCH_TARGETS
        );
        assert_eq!(
            target_limit.insert_target(IndexRangeTarget::new(
                IndexRangePrefixBuilder::new(
                    IndexId::new(
                        u32::try_from(MAX_AFFECTED_INDEX_EPOCH_TARGETS + 1).expect("u32 bound")
                    )
                    .expect("index ID")
                )
                .finish()
            )),
            Err(CommandIndexError::internal_defect())
        );

        let exact_byte_target =
            IndexRangeTarget::new(IndexRangePrefixBuilder::new(IndexId::first()).finish());
        let observation_bytes = exact_byte_target.prefix().as_bytes().len()
            + INDEX_RANGE_TARGET_FIXED_BYTES_V1
            + MAX_INDEX_EPOCH_POSITION_BYTES_V1;
        let mut byte_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        byte_limit.affected_current_semantic_bytes = MAX_READ_SNAPSHOT_BYTES - observation_bytes;
        byte_limit
            .insert_target(exact_byte_target)
            .expect("exact affected-current byte bound");
        assert_eq!(
            byte_limit.affected_current_semantic_bytes,
            MAX_READ_SNAPSHOT_BYTES
        );
        assert_eq!(
            byte_limit.insert_target(IndexRangeTarget::new(
                IndexRangePrefixBuilder::new(IndexId::new(2).expect("index ID")).finish()
            )),
            Err(CommandIndexError::internal_defect())
        );
    }

    #[test]
    fn zero_mutation_and_missing_index_field_fail_closed() {
        let fixture = fixture("CreateRow", "invalid-1", ([0x21; 16], "new", 10), None);
        let empty = EvaluatedCommand::new(
            &ReadSnapshot::new(
                &SnapshotRequest::new(
                    fixture.resolved.reference().clone(),
                    fixture
                        .evaluated
                        .validation_request()
                        .binding_targets()
                        .to_vec(),
                    Vec::new(),
                    Vec::new(),
                )
                .expect("request"),
                None,
                fixture.current.bindings().to_vec(),
                Vec::new(),
                Vec::new(),
            )
            .expect("snapshot"),
            Vec::new(),
            fixture.evaluated.event_intents().to_vec(),
            fixture.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structural zero-mutation candidate");
        assert_eq!(
            derive_grammar_v1_indexes(&fixture.resolved, &empty, &fixture.current).err(),
            Some(CommandIndexError::internal_defect())
        );

        let index = index(&fixture, "by_score");
        let missing = CanonicalRecord::new(Vec::new()).expect("empty record");
        assert_eq!(
            index_values(index, &missing),
            Err(CommandIndexError::internal_defect())
        );
    }
}
