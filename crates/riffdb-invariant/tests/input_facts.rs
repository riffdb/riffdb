#![forbid(unsafe_code)]

//! Semantic checks for proof-bearing pre-admission command facts.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{
    AggregateKeyPlan, AggregateSchema, BinaryOperator, BindingId, BindingMode, BindingPlan,
    CommandInputSchema, CommandPlan, CommitCheckPlan, ConflictDerivationPlan, ExecutionClass,
    ExprId, ExpressionArena, ExpressionKind, FieldSchema, Instruction, InvariantPlan,
    KeyComponentSchema, KeyPurpose, KeySchema, LocalityPlan, OutcomeConstruction, OutcomeSchema,
    RecordSchema, RecordTypeRef, RootValidationReadId, RootValidationReadPlan, SchemaIr, ValueType,
};
use riffdb_invariant::{EvaluationError, derive_input_command_facts};
use riffdb_types::{
    AggregateTypeId, CanonicalList, CanonicalRecord, CanonicalValue, CommandId, ContractLineage,
    ContractVersion, EntityTypeId, FieldId, InvariantId, OutcomeId,
};

const SECRET_KEY: &str = "idempotency-secret-canary";
const BULK_TUPLE_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/contracts/bulk/openfga-tuples.riff"
));

struct PlanFixture {
    plan: CommandPlan,
    input: CanonicalRecord,
    entity_schema: KeySchema,
    partition_schema: KeySchema,
    conflict_schema: KeySchema,
}

#[test]
fn collection_facts_expand_only_the_compiler_owned_template_in_submitted_order() {
    let bundle = compile_contract_source(BULK_TUPLE_SOURCE).expect("bulk fixture compiles");
    let plan = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "WriteTuples")
        .expect("bulk command");
    let tuple = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Tuple")
        .expect("tuple entity");
    let store = [0x31; 16];
    let tuple_value = |id: u8, object: &str| {
        CanonicalValue::Record(
            CanonicalRecord::new(
                tuple
                    .record()
                    .fields()
                    .iter()
                    .map(|field| {
                        let value = match field.name() {
                            "store_id" => CanonicalValue::Uuid(store),
                            "tuple_id" => CanonicalValue::Uuid([id; 16]),
                            "object" => CanonicalValue::string(object).expect("object"),
                            "relation" => CanonicalValue::string("reader").expect("relation"),
                            "subject" => CanonicalValue::string("user:alice").expect("subject"),
                            other => panic!("unexpected tuple field {other}"),
                        };
                        (field.id(), value)
                    })
                    .collect(),
            )
            .expect("tuple record"),
        )
    };
    let elements = vec![
        tuple_value(0x41, "document:first"),
        tuple_value(0x42, "document:second"),
    ];
    let input = CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "request_id" => CanonicalValue::Uuid([0x21; 16]),
                    "tuples" => CanonicalValue::List(
                        CanonicalList::new(elements.clone()).expect("tuple list"),
                    ),
                    other => panic!("unexpected input field {other}"),
                };
                (field.id(), value)
            })
            .collect(),
    )
    .expect("bulk input");

    let facts = derive_input_command_facts(plan, input.clone()).expect("collection facts");
    assert_eq!(facts.binding_entity_keys().len(), 2);
    assert_eq!(facts.binding_plan_indices(), &[0, 0]);
    assert_eq!(facts.binding_element_ordinals(), &[Some(0), Some(1)]);
    assert_eq!(facts.declared_conflict_keys().len(), 2);
    let expected_partition = plan
        .locality()
        .partition_schema()
        .encode_partition(&[CanonicalValue::Uuid(store)])
        .expect("partition");
    assert_eq!(facts.partition_key(), &expected_partition);

    let duplicate = CanonicalRecord::new(
        input
            .fields()
            .iter()
            .map(|(field, value)| {
                let replacement = if matches!(value, CanonicalValue::List(_)) {
                    CanonicalValue::List(
                        CanonicalList::new(vec![elements[0].clone(), elements[0].clone()])
                            .expect("duplicate list"),
                    )
                } else {
                    value.clone()
                };
                (*field, replacement)
            })
            .collect(),
    )
    .expect("duplicate input");
    assert!(matches!(
        derive_input_command_facts(plan, duplicate),
        Err(EvaluationError::Integrity)
    ));
}

#[test]
fn facts_bind_exact_plan_and_full_input_without_sorting_declared_conflicts() {
    let fixture = plan_fixture(false);
    let alternate = plan_fixture(true);
    let facts = derive_input_command_facts(&fixture.plan, fixture.input.clone())
        .expect("input-only facts derive");

    assert!(facts.matches_command(&fixture.plan, &fixture.input));
    assert!(!facts.matches_command(&alternate.plan, &fixture.input));

    let changed_input = input_record(7, 11, 3);
    assert!(!facts.matches_command(&fixture.plan, &changed_input));

    let expected_partition = fixture
        .partition_schema
        .encode_partition(&[CanonicalValue::U64(7)])
        .expect("expected partition");
    assert_eq!(facts.partition_key(), &expected_partition);

    let first = fixture
        .conflict_schema
        .encode_conflict(&[CanonicalValue::U64(7), CanonicalValue::U64(10)])
        .expect("first conflict");
    let second = fixture
        .conflict_schema
        .encode_conflict(&[CanonicalValue::U64(7), CanonicalValue::U64(3)])
        .expect("second conflict");
    assert!(
        first > second,
        "fixture must distinguish declaration from sort order"
    );
    assert_eq!(facts.declared_conflict_keys(), &[first, second]);

    let first_target = fixture
        .entity_schema
        .encode_entity(&[CanonicalValue::U64(7), CanonicalValue::U64(10)])
        .expect("first entity key");
    let second_target = fixture
        .entity_schema
        .encode_entity(&[CanonicalValue::U64(7), CanonicalValue::U64(3)])
        .expect("second entity key");
    assert!(
        first_target > second_target,
        "fixture must distinguish dense binding order from key order"
    );
    assert_eq!(facts.binding_entity_keys(), &[first_target, second_target]);
    assert!(facts.root_validation_entity_keys().is_empty());

    let duplicate_input = input_record(7, 3, 3);
    let duplicate_facts = derive_input_command_facts(&fixture.plan, duplicate_input)
        .expect("duplicate binding targets are positional");
    assert_eq!(duplicate_facts.binding_entity_keys().len(), 2);
    assert_eq!(
        duplicate_facts.binding_entity_keys()[0],
        duplicate_facts.binding_entity_keys()[1]
    );
}

#[test]
fn input_shape_type_and_arithmetic_fail_closed() {
    let fixture = plan_fixture(false);
    let missing = CanonicalRecord::new(fixture.input.fields()[..3].to_vec())
        .expect("canonical incomplete input");
    assert!(matches!(
        derive_input_command_facts(&fixture.plan, missing),
        Err(EvaluationError::Integrity)
    ));

    let wrong_type = CanonicalRecord::new(vec![
        (
            FieldId::new(1).expect("field"),
            CanonicalValue::string(SECRET_KEY).expect("key"),
        ),
        (FieldId::new(2).expect("field"), CanonicalValue::I64(7)),
        (FieldId::new(3).expect("field"), CanonicalValue::U64(10)),
        (FieldId::new(4).expect("field"), CanonicalValue::U64(3)),
    ])
    .expect("canonical wrong-typed input");
    assert!(matches!(
        derive_input_command_facts(&fixture.plan, wrong_type),
        Err(EvaluationError::Integrity)
    ));

    let extra = CanonicalRecord::new(
        fixture
            .input
            .fields()
            .iter()
            .cloned()
            .chain([(FieldId::new(9).expect("field"), CanonicalValue::Bool(true))])
            .collect(),
    )
    .expect("canonical extended input");
    assert!(matches!(
        derive_input_command_facts(&fixture.plan, extra),
        Err(EvaluationError::Integrity)
    ));

    let arithmetic = plan_fixture(true);
    assert!(matches!(
        derive_input_command_facts(&arithmetic.plan, input_record(u64::MAX, 10, 3),),
        Err(EvaluationError::Arithmetic)
    ));
}

#[test]
fn diagnostics_redact_input_and_derived_keys() {
    let fixture = plan_fixture(false);
    let facts =
        derive_input_command_facts(&fixture.plan, fixture.input).expect("input-only facts derive");
    let diagnostic = format!("{facts:?}");

    assert_eq!(diagnostic, "InputDerivedCommandFacts([REDACTED])");
    assert!(!diagnostic.contains(SECRET_KEY));
    assert!(!diagnostic.contains("10"));
}

#[test]
fn root_validation_keys_are_derived_in_dense_plan_order() {
    let (plan, input, child_key_schema, root_key_schema) = root_validation_fixture(false);
    let facts = derive_input_command_facts(&plan, input).expect("input-only facts derive");

    let expected_child = child_key_schema
        .encode_entity(&[CanonicalValue::U64(17), CanonicalValue::U64(29)])
        .expect("child key");
    let expected_root = root_key_schema
        .encode_entity(&[CanonicalValue::U64(17)])
        .expect("root key");
    assert_eq!(facts.binding_entity_keys(), &[expected_child]);
    assert_eq!(facts.root_validation_entity_keys(), &[expected_root]);
}

#[test]
fn binding_key_arithmetic_runs_after_successful_locality_derivation() {
    let (plan, input, _, _) = root_validation_fixture(true);
    assert!(matches!(
        derive_input_command_facts(&plan, input),
        Err(EvaluationError::Arithmetic)
    ));
}

fn root_validation_fixture(
    overflowing_child_key: bool,
) -> (CommandPlan, CanonicalRecord, KeySchema, KeySchema) {
    let root_id = EntityTypeId::first();
    let child_id = EntityTypeId::new(2).expect("child entity");
    let aggregate_id = AggregateTypeId::first();
    let command_id = CommandId::first();
    let root_id_field = FieldId::first();
    let root_flag_field = FieldId::new(2).expect("root flag");
    let child_id_field = FieldId::new(2).expect("child ID");
    let request_key_input = FieldId::first();
    let root_id_input = FieldId::new(2).expect("root input");
    let child_id_input = FieldId::new(3).expect("child input");
    let invariant_id = InvariantId::first();
    let scalar = KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component");
    let root_key_schema =
        KeySchema::new(KeyPurpose::Entity(root_id), vec![scalar.clone()]).expect("root key");
    let child_key_schema = KeySchema::new(
        KeyPurpose::Entity(child_id),
        vec![scalar.clone(), scalar.clone()],
    )
    .expect("child key");

    let root = riffdb_contract_ir::EntitySchema::new(
        root_id,
        "Root",
        RecordSchema::new(
            RecordTypeRef::Entity(root_id),
            vec![
                FieldSchema::new(root_id_field, "root_id", ValueType::u64()).expect("field"),
                FieldSchema::new(root_flag_field, "flag", ValueType::bool()).expect("field"),
            ],
        )
        .expect("root record"),
        vec![root_id_field],
        root_key_schema.clone(),
        vec![],
        vec![],
    )
    .expect("root entity");
    let child = riffdb_contract_ir::EntitySchema::new(
        child_id,
        "Child",
        RecordSchema::new(
            RecordTypeRef::Entity(child_id),
            vec![
                FieldSchema::new(root_id_field, "root_id", ValueType::u64()).expect("field"),
                FieldSchema::new(child_id_field, "child_id", ValueType::u64()).expect("field"),
            ],
        )
        .expect("child record"),
        vec![root_id_field, child_id_field],
        child_key_schema.clone(),
        vec![],
        vec![],
    )
    .expect("child entity");
    let invariant_expressions = ExpressionArena::new(vec![(
        ExpressionKind::Constant(CanonicalValue::Bool(true)),
        ValueType::bool(),
    )])
    .expect("invariant expressions");
    let invariant = InvariantPlan::new(
        invariant_id,
        "RootPolicy",
        invariant_expressions,
        ExprId::new(0),
    )
    .expect("invariant");
    let aggregate_key_expressions = ExpressionArena::new(vec![(
        ExpressionKind::SchemaField {
            entity_type: root_id,
            field: root_id_field,
        },
        ValueType::u64(),
    )])
    .expect("aggregate key expressions");
    let partition_schema =
        KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![scalar.clone()])
            .expect("partition key");
    let conflict_schema =
        KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![scalar]).expect("conflict key");
    let aggregate = AggregateSchema::new(
        aggregate_id,
        "Aggregate",
        root_id,
        vec![child_id],
        AggregateKeyPlan::new(
            aggregate_key_expressions,
            ExprId::new(0),
            vec![ExprId::new(0)],
            partition_schema.clone(),
            conflict_schema.clone(),
        )
        .expect("aggregate keys"),
        vec![invariant],
    )
    .expect("aggregate");
    let schema = SchemaIr::new(vec![root, child], vec![], vec![], vec![aggregate]).expect("schema");
    let input_schema = CommandInputSchema::new(
        command_id,
        RecordSchema::new(
            RecordTypeRef::CommandInput(command_id),
            vec![
                FieldSchema::new(
                    request_key_input,
                    "request_key",
                    ValueType::string(128).expect("string type"),
                )
                .expect("field"),
                FieldSchema::new(root_id_input, "root_id", ValueType::u64()).expect("field"),
                FieldSchema::new(child_id_input, "child_id", ValueType::u64()).expect("field"),
            ],
        )
        .expect("input record"),
    )
    .expect("input schema");
    let (expressions, child_key_expression, commit_predicate) = if overflowing_child_key {
        (
            ExpressionArena::new(vec![
                (ExpressionKind::InputField(root_id_input), ValueType::u64()),
                (ExpressionKind::InputField(child_id_input), ValueType::u64()),
                (
                    ExpressionKind::Constant(CanonicalValue::U64(1)),
                    ValueType::u64(),
                ),
                (
                    ExpressionKind::Binary {
                        operator: BinaryOperator::Add,
                        left: ExprId::new(1),
                        right: ExprId::new(2),
                    },
                    ValueType::u64(),
                ),
                (
                    ExpressionKind::Constant(CanonicalValue::Bool(true)),
                    ValueType::bool(),
                ),
            ])
            .expect("command expressions"),
            ExprId::new(3),
            ExprId::new(4),
        )
    } else {
        (
            ExpressionArena::new(vec![
                (ExpressionKind::InputField(root_id_input), ValueType::u64()),
                (ExpressionKind::InputField(child_id_input), ValueType::u64()),
                (
                    ExpressionKind::Constant(CanonicalValue::Bool(true)),
                    ValueType::bool(),
                ),
            ])
            .expect("command expressions"),
            ExprId::new(1),
            ExprId::new(2),
        )
    };
    let missing = OutcomeSchema::new(
        command_id,
        OutcomeId::first(),
        "Missing",
        RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: OutcomeId::first(),
            },
            vec![],
        )
        .expect("missing record"),
    )
    .expect("missing outcome");
    let applied_id = OutcomeId::new(2).expect("applied outcome");
    let applied = OutcomeSchema::new(
        command_id,
        applied_id,
        "Applied",
        RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: applied_id,
            },
            vec![],
        )
        .expect("applied record"),
    )
    .expect("applied outcome");
    let binding = BindingPlan::new(
        BindingId::new(0),
        "child",
        BindingMode::Mutate,
        child_id,
        child_key_schema.clone(),
        vec![ExprId::new(0), child_key_expression],
        vec![],
        false,
        OutcomeConstruction::new(&missing, vec![], &expressions).expect("missing construction"),
    )
    .expect("binding");
    let root_read = RootValidationReadPlan::new(
        RootValidationReadId::new(0),
        BindingId::new(0),
        root_id,
        root_key_schema.clone(),
        vec![ExprId::new(0)],
        vec![],
    )
    .expect("root read");
    let locality = LocalityPlan::new(
        aggregate_id,
        partition_schema,
        ExprId::new(0),
        vec![ConflictDerivationPlan::new(conflict_schema, vec![ExprId::new(0)]).expect("conflict")],
    )
    .expect("locality");
    let check = CommitCheckPlan::new(
        invariant_id,
        commit_predicate,
        vec![],
        vec![RootValidationReadId::new(0)],
    )
    .expect("commit check");
    let success =
        OutcomeConstruction::new(&applied, vec![], &expressions).expect("success construction");
    let plan = CommandPlan::new(
        command_id,
        ContractLineage::new("RootValidationFacts").expect("lineage"),
        "ApplyChild",
        ContractVersion::new(1).expect("version"),
        input_schema,
        vec![missing, applied],
        applied_id,
        Some(request_key_input),
        expressions,
        vec![binding],
        vec![root_read],
        locality,
        vec![check],
        vec![Instruction::Return(success)],
        ExecutionClass::IdempotentMutation,
        &schema,
    )
    .expect("checked command plan");
    let input = CanonicalRecord::new(vec![
        (
            request_key_input,
            CanonicalValue::string(SECRET_KEY).expect("request key"),
        ),
        (root_id_input, CanonicalValue::U64(17)),
        (
            child_id_input,
            CanonicalValue::U64(if overflowing_child_key { u64::MAX } else { 29 }),
        ),
    ])
    .expect("normalized input");

    (plan, input, child_key_schema, root_key_schema)
}

fn plan_fixture(arithmetic_partition: bool) -> PlanFixture {
    let aggregate_id = AggregateTypeId::first();
    let entity_id = EntityTypeId::first();
    let command_id = CommandId::first();
    let entity_tenant = FieldId::new(1).expect("field");
    let entity_item = FieldId::new(2).expect("field");
    let idempotency_input = FieldId::new(1).expect("field");
    let tenant_input = FieldId::new(2).expect("field");
    let first_item_input = FieldId::new(3).expect("field");
    let second_item_input = FieldId::new(4).expect("field");
    let scalar = KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component");
    let entity_key = KeySchema::new(
        KeyPurpose::Entity(entity_id),
        vec![scalar.clone(), scalar.clone()],
    )
    .expect("entity key");
    let partition_schema =
        KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![scalar.clone()])
            .expect("partition key");
    let conflict_schema = KeySchema::new(
        KeyPurpose::Conflict(aggregate_id),
        vec![scalar.clone(), scalar],
    )
    .expect("conflict key");

    let entity = riffdb_contract_ir::EntitySchema::new(
        entity_id,
        "Root",
        RecordSchema::new(
            RecordTypeRef::Entity(entity_id),
            vec![
                FieldSchema::new(entity_tenant, "tenant", ValueType::u64()).expect("field"),
                FieldSchema::new(entity_item, "item", ValueType::u64()).expect("field"),
            ],
        )
        .expect("entity record"),
        vec![entity_tenant, entity_item],
        entity_key.clone(),
        vec![],
        vec![],
    )
    .expect("entity");
    let aggregate_expressions = ExpressionArena::new(vec![
        (
            ExpressionKind::SchemaField {
                entity_type: entity_id,
                field: entity_tenant,
            },
            ValueType::u64(),
        ),
        (
            ExpressionKind::SchemaField {
                entity_type: entity_id,
                field: entity_item,
            },
            ValueType::u64(),
        ),
    ])
    .expect("aggregate expressions");
    let aggregate = AggregateSchema::new(
        aggregate_id,
        "Roots",
        entity_id,
        vec![],
        AggregateKeyPlan::new(
            aggregate_expressions,
            ExprId::new(0),
            vec![ExprId::new(0), ExprId::new(1)],
            partition_schema.clone(),
            conflict_schema.clone(),
        )
        .expect("aggregate key plan"),
        vec![],
    )
    .expect("aggregate");
    let schema = SchemaIr::new(vec![entity], vec![], vec![], vec![aggregate]).expect("schema");
    let input_schema = CommandInputSchema::new(
        command_id,
        RecordSchema::new(
            RecordTypeRef::CommandInput(command_id),
            vec![
                FieldSchema::new(
                    idempotency_input,
                    "request_key",
                    ValueType::string(128).expect("string type"),
                )
                .expect("field"),
                FieldSchema::new(tenant_input, "tenant", ValueType::u64()).expect("field"),
                FieldSchema::new(first_item_input, "first_item", ValueType::u64()).expect("field"),
                FieldSchema::new(second_item_input, "second_item", ValueType::u64())
                    .expect("field"),
            ],
        )
        .expect("input record"),
    )
    .expect("input schema");

    let (expressions, tenant_expression, first_expression, second_expression) =
        if arithmetic_partition {
            (
                ExpressionArena::new(vec![
                    (ExpressionKind::InputField(tenant_input), ValueType::u64()),
                    (
                        ExpressionKind::Constant(CanonicalValue::U64(1)),
                        ValueType::u64(),
                    ),
                    (
                        ExpressionKind::Binary {
                            operator: BinaryOperator::Add,
                            left: ExprId::new(0),
                            right: ExprId::new(1),
                        },
                        ValueType::u64(),
                    ),
                    (
                        ExpressionKind::InputField(first_item_input),
                        ValueType::u64(),
                    ),
                    (
                        ExpressionKind::InputField(second_item_input),
                        ValueType::u64(),
                    ),
                ])
                .expect("arithmetic expressions"),
                ExprId::new(2),
                ExprId::new(3),
                ExprId::new(4),
            )
        } else {
            (
                ExpressionArena::new(vec![
                    (ExpressionKind::InputField(tenant_input), ValueType::u64()),
                    (
                        ExpressionKind::InputField(first_item_input),
                        ValueType::u64(),
                    ),
                    (
                        ExpressionKind::InputField(second_item_input),
                        ValueType::u64(),
                    ),
                ])
                .expect("input expressions"),
                ExprId::new(0),
                ExprId::new(1),
                ExprId::new(2),
            )
        };

    let missing = OutcomeSchema::new(
        command_id,
        OutcomeId::first(),
        "Missing",
        RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: OutcomeId::first(),
            },
            vec![],
        )
        .expect("missing record"),
    )
    .expect("missing outcome");
    let applied_id = OutcomeId::new(2).expect("outcome");
    let applied = OutcomeSchema::new(
        command_id,
        applied_id,
        "Applied",
        RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: applied_id,
            },
            vec![],
        )
        .expect("applied record"),
    )
    .expect("applied outcome");
    let binding = |id, name, item_expression| {
        BindingPlan::new(
            BindingId::new(id),
            name,
            BindingMode::Mutate,
            entity_id,
            entity_key.clone(),
            vec![tenant_expression, item_expression],
            vec![],
            false,
            OutcomeConstruction::new(&missing, vec![], &expressions).expect("missing construction"),
        )
        .expect("binding")
    };
    let bindings = vec![
        binding(0, "first", first_expression),
        binding(1, "second", second_expression),
    ];
    let conflicts = [first_expression, second_expression]
        .into_iter()
        .map(|item_expression| {
            ConflictDerivationPlan::new(
                conflict_schema.clone(),
                vec![tenant_expression, item_expression],
            )
            .expect("conflict derivation")
        })
        .collect();
    let locality = LocalityPlan::new(
        aggregate_id,
        partition_schema.clone(),
        tenant_expression,
        conflicts,
    )
    .expect("locality");
    let success =
        OutcomeConstruction::new(&applied, vec![], &expressions).expect("success construction");
    let plan = CommandPlan::new(
        command_id,
        ContractLineage::new(if arithmetic_partition {
            "ArithmeticFacts"
        } else {
            "InputFacts"
        })
        .expect("lineage"),
        "Apply",
        ContractVersion::new(1).expect("version"),
        input_schema,
        vec![missing, applied],
        applied_id,
        Some(idempotency_input),
        expressions,
        bindings,
        vec![],
        locality,
        vec![],
        vec![Instruction::Return(success)],
        ExecutionClass::IdempotentMutation,
        &schema,
    )
    .expect("checked command plan");

    PlanFixture {
        plan,
        input: input_record(7, 10, 3),
        entity_schema: entity_key,
        partition_schema,
        conflict_schema,
    }
}

fn input_record(tenant: u64, first_item: u64, second_item: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![
        (
            FieldId::new(1).expect("field"),
            CanonicalValue::string(SECRET_KEY).expect("key"),
        ),
        (FieldId::new(2).expect("field"), CanonicalValue::U64(tenant)),
        (
            FieldId::new(3).expect("field"),
            CanonicalValue::U64(first_item),
        ),
        (
            FieldId::new(4).expect("field"),
            CanonicalValue::U64(second_item),
        ),
    ])
    .expect("normalized input")
}
