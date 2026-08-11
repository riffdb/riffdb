//! Differential generated command histories for the deterministic runtime.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordSchema};
    use riffdb_invariant::{ExpressionValueSource, evaluate_expression};
    use riffdb_runtime::{ExecutionResult, TransactionContext, execute_command};
    use riffdb_storage_api::{
        DeclaredOutcome, DurableKeySchemaBindingV1, EntityMutation, EntityObservation,
        EntityTarget, EvaluationBudget, ExecutablePlanRef, ReadSnapshot, SnapshotRequest,
        StoredEntityRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, CanonicalRecord, CanonicalValue, Decimal,
        DecimalSpec, EntityVersion, FieldId, LogicalTime, RequestId, TenantScope, Timestamp,
    };

    const BUDGET_SOURCE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../contracts/examples/budget.riff"
    ));

    #[derive(Clone, Copy)]
    enum Operation {
        Create(i128),
        Allocate(i128),
    }

    const OPERATIONS: [Operation; 6] = [
        Operation::Create(0),
        Operation::Create(500),
        Operation::Allocate(-1),
        Operation::Allocate(100),
        Operation::Allocate(400),
        Operation::Allocate(600),
    ];

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ObservedOutcome {
        Created {
            approved: i128,
            allocated: i128,
        },
        AlreadyExists,
        InvalidApproval {
            minimum: i128,
        },
        NotFound,
        InvalidAmount {
            minimum: i128,
        },
        Insufficient {
            approved: i128,
            allocated: i128,
            requested: i128,
        },
        Allocated {
            approved: i128,
            allocated: i128,
            remaining: i128,
        },
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct BudgetState {
        approved: i128,
        allocated: i128,
    }

    #[derive(Default)]
    struct ReferenceModel {
        budget: Option<BudgetState>,
    }

    impl ReferenceModel {
        fn execute(&mut self, operation: Operation) -> ObservedOutcome {
            match operation {
                Operation::Create(approved) => {
                    if self.budget.is_some() {
                        ObservedOutcome::AlreadyExists
                    } else if approved <= 0 {
                        ObservedOutcome::InvalidApproval { minimum: 1 }
                    } else {
                        let state = BudgetState {
                            approved,
                            allocated: 0,
                        };
                        self.budget = Some(state);
                        ObservedOutcome::Created {
                            approved,
                            allocated: 0,
                        }
                    }
                }
                Operation::Allocate(amount) => {
                    let Some(current) = self.budget else {
                        return ObservedOutcome::NotFound;
                    };
                    if amount <= 0 {
                        return ObservedOutcome::InvalidAmount { minimum: 1 };
                    }
                    let allocated = current
                        .allocated
                        .checked_add(amount)
                        .expect("small generated arithmetic");
                    if allocated > current.approved {
                        return ObservedOutcome::Insufficient {
                            approved: current.approved,
                            allocated: current.allocated,
                            requested: amount,
                        };
                    }
                    let state = BudgetState {
                        approved: current.approved,
                        allocated,
                    };
                    self.budget = Some(state);
                    ObservedOutcome::Allocated {
                        approved: state.approved,
                        allocated: state.allocated,
                        remaining: state.approved - state.allocated,
                    }
                }
            }
        }
    }

    #[derive(Default)]
    struct RuntimeState {
        entities: BTreeMap<EntityTarget, (EntityVersion, CanonicalRecord)>,
    }

    #[test]
    fn generated_histories_match_runtime_and_reference_model() {
        let bundle = compile_contract_source(BUDGET_SOURCE).expect("budget contract compiles");
        let mut history_id = 0u64;
        for first in OPERATIONS {
            for second in OPERATIONS {
                for third in OPERATIONS {
                    history_id += 1;
                    run_history(&bundle, history_id, [first, second, third]);
                }
            }
        }
        assert_eq!(history_id, 216);
    }

    fn run_history(bundle: &ContractBundle, history_id: u64, operations: [Operation; 3]) {
        let organization = uuid_for(history_id);
        let fiscal_year = 2_000 + (history_id % 100) as i64;
        let mut reference = ReferenceModel::default();
        let mut runtime = RuntimeState::default();

        for (step, operation) in operations.into_iter().enumerate() {
            let expected = reference.execute(operation);
            let (plan, input) = command_input(
                bundle,
                operation,
                organization,
                fiscal_year,
                history_id,
                step,
            );
            let target = derive_target(plan, &input);
            let observation = match runtime.entities.get(&target) {
                Some((version, fields)) => EntityObservation::Present(
                    StoredEntityRecordV1::new(
                        target.clone(),
                        *version,
                        bundle.contract_version(),
                        DurableKeySchemaBindingV1::from_plan(&plan_ref(bundle, plan)),
                        fields.clone(),
                    )
                    .expect("modeled stored entity"),
                ),
                None => EntityObservation::Absent(target.clone()),
            };
            let request =
                SnapshotRequest::new(plan_ref(bundle, plan), vec![target.clone()], vec![], vec![])
                    .expect("snapshot request");
            let snapshot = ReadSnapshot::new(&request, None, vec![observation], vec![], vec![])
                .expect("owned snapshot");
            let context = context(bundle, plan, &input, history_id, step);

            let first =
                execute_command(bundle, &input, &snapshot, &context, EvaluationBudget::v1())
                    .expect("generated command has no execution fault");
            let repeated =
                execute_command(bundle, &input, &snapshot, &context, EvaluationBudget::v1())
                    .expect("equal evaluation repeats");
            assert_eq!(first, repeated, "history {history_id}, step {step}");

            let ExecutionResult::CommitRequired(evaluated) = first else {
                panic!("canonical budget commands mutate");
            };
            assert_eq!(evaluated.read_dependencies().as_slice().len(), 1);
            let actual = normalize_outcome(bundle, plan, evaluated.outcome());
            assert_eq!(actual, expected, "history {history_id}, step {step}");
            let expected_events = usize::from(matches!(actual, ObservedOutcome::Allocated { .. }));
            assert_eq!(evaluated.event_intents().len(), expected_events);

            for mutation in evaluated.mutations() {
                match mutation {
                    EntityMutation::Create(post_image) => {
                        assert!(!runtime.entities.contains_key(post_image.target()));
                        runtime.entities.insert(
                            post_image.target().clone(),
                            (EntityVersion::first(), post_image.fields().clone()),
                        );
                    }
                    EntityMutation::Replace {
                        expected_version,
                        post_image,
                    } => {
                        let (current_version, _) = runtime
                            .entities
                            .get(post_image.target())
                            .expect("replacement target exists");
                        assert_eq!(current_version, expected_version);
                        runtime.entities.insert(
                            post_image.target().clone(),
                            (
                                expected_version
                                    .checked_next()
                                    .expect("history version bound"),
                                post_image.fields().clone(),
                            ),
                        );
                    }
                    EntityMutation::Delete { .. } => {
                        panic!("budget history commands cannot produce delete effects")
                    }
                }
            }
            assert_runtime_state(bundle, &runtime, reference.budget, &target);
        }
    }

    fn command_input(
        bundle: &ContractBundle,
        operation: Operation,
        organization: [u8; 16],
        fiscal_year: i64,
        history_id: u64,
        step: usize,
    ) -> (&CommandPlan, CanonicalRecord) {
        let key = CanonicalValue::string(format!("history-{history_id}-step-{step}"))
            .expect("bounded idempotency input");
        match operation {
            Operation::Create(approved) => {
                let plan = command(bundle, "CreateBudget");
                (
                    plan,
                    record(
                        plan.input().record(),
                        [
                            ("idempotency_key", key),
                            ("organization_id", CanonicalValue::Uuid(organization)),
                            ("fiscal_year", CanonicalValue::I64(fiscal_year)),
                            ("approved_amount", decimal(approved)),
                        ],
                    ),
                )
            }
            Operation::Allocate(amount) => {
                let plan = command(bundle, "AllocateBudget");
                (
                    plan,
                    record(
                        plan.input().record(),
                        [
                            ("idempotency_key", key),
                            ("organization_id", CanonicalValue::Uuid(organization)),
                            ("fiscal_year", CanonicalValue::I64(fiscal_year)),
                            ("matter_id", CanonicalValue::Uuid(uuid_for(history_id + 1))),
                            ("amount", decimal(amount)),
                        ],
                    ),
                )
            }
        }
    }

    fn normalize_outcome(
        bundle: &ContractBundle,
        plan: &CommandPlan,
        outcome: &DeclaredOutcome,
    ) -> ObservedOutcome {
        let schema = plan
            .outcomes()
            .iter()
            .find(|schema| schema.id() == outcome.outcome_id())
            .expect("declared outcome schema");
        match schema.name() {
            "BudgetCreated" => {
                let budget = nested(outcome.value(), schema.payload(), "budget");
                ObservedOutcome::Created {
                    approved: decimal_named_entity_field(bundle, budget, "approved_amount"),
                    allocated: decimal_named_entity_field(bundle, budget, "allocated_amount"),
                }
            }
            "BudgetAlreadyExists" => ObservedOutcome::AlreadyExists,
            "InvalidApprovedAmount" => ObservedOutcome::InvalidApproval {
                minimum: decimal_payload(outcome.value(), schema.payload(), "minimum"),
            },
            "BudgetNotFound" => ObservedOutcome::NotFound,
            "InvalidAmount" => ObservedOutcome::InvalidAmount {
                minimum: decimal_payload(outcome.value(), schema.payload(), "minimum"),
            },
            "InsufficientBudget" => ObservedOutcome::Insufficient {
                approved: decimal_payload(outcome.value(), schema.payload(), "approved"),
                allocated: decimal_payload(outcome.value(), schema.payload(), "allocated"),
                requested: decimal_payload(outcome.value(), schema.payload(), "requested"),
            },
            "Allocated" => {
                let budget = nested(outcome.value(), schema.payload(), "budget");
                ObservedOutcome::Allocated {
                    approved: decimal_named_entity_field(bundle, budget, "approved_amount"),
                    allocated: decimal_named_entity_field(bundle, budget, "allocated_amount"),
                    remaining: decimal_payload(outcome.value(), schema.payload(), "remaining"),
                }
            }
            other => panic!("unexpected generated outcome {other}"),
        }
    }

    fn assert_runtime_state(
        bundle: &ContractBundle,
        runtime: &RuntimeState,
        expected: Option<BudgetState>,
        target: &EntityTarget,
    ) {
        let actual = runtime.entities.get(target).map(|(_, record)| BudgetState {
            approved: decimal_named_entity_field(bundle, record, "approved_amount"),
            allocated: decimal_named_entity_field(bundle, record, "allocated_amount"),
        });
        assert_eq!(actual, expected);
        if let Some(state) = actual {
            assert!(state.allocated >= 0);
            assert!(state.allocated <= state.approved);
        }
    }

    fn command<'a>(bundle: &'a ContractBundle, name: &str) -> &'a CommandPlan {
        bundle
            .commands()
            .iter()
            .find(|command| command.name() == name)
            .expect("command exists")
    }

    fn plan_ref(bundle: &ContractBundle, plan: &CommandPlan) -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        )
    }

    fn derive_target(plan: &CommandPlan, input: &CanonicalRecord) -> EntityTarget {
        let binding = &plan.bindings()[0];
        let values = Inputs { input };
        let components = binding
            .key_expressions()
            .iter()
            .map(|expression| {
                evaluate_expression(plan.expressions(), *expression, &values)
                    .expect("key expression")
            })
            .collect::<Vec<_>>();
        let key = binding
            .key_schema()
            .encode_entity(&components)
            .expect("entity key");
        EntityTarget::new(binding.entity_type(), key).expect("target")
    }

    fn context(
        bundle: &ContractBundle,
        plan: &CommandPlan,
        input: &CanonicalRecord,
        history_id: u64,
        step: usize,
    ) -> TransactionContext {
        let values = Inputs { input };
        let partition_value = evaluate_expression(
            plan.expressions(),
            plan.locality().partition_expression(),
            &values,
        )
        .expect("partition expression");
        let partition = plan
            .locality()
            .partition_schema()
            .encode_partition(&[partition_value])
            .expect("partition");
        TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(
                history_id * 10 + step as u64,
                [step as u8; 10],
            )
            .expect("request ID"),
            AdmittedActorContext::new(
                ActorId::new("generated-history").expect("actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            plan_ref(bundle, plan),
            LogicalTime::new(
                Timestamp::new(history_id as i64 * 10 + step as i64, step as u32)
                    .expect("logical time"),
            ),
            partition,
        )
    }

    fn record<const N: usize>(
        schema: &RecordSchema,
        values: [(&str, CanonicalValue); N],
    ) -> CanonicalRecord {
        let values = values.into_iter().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        values
                            .get(field.name())
                            .unwrap_or_else(|| panic!("missing field {}", field.name()))
                            .clone(),
                    )
                })
                .collect(),
        )
        .expect("canonical record")
    }

    fn nested<'a>(
        payload: &'a CanonicalRecord,
        schema: &RecordSchema,
        name: &str,
    ) -> &'a CanonicalRecord {
        let id = named_field(schema, name);
        let CanonicalValue::Record(record) = value(payload, id) else {
            panic!("expected nested record");
        };
        record
    }

    fn decimal_payload(payload: &CanonicalRecord, schema: &RecordSchema, name: &str) -> i128 {
        decimal_value(value(payload, named_field(schema, name)))
    }

    fn decimal_named_entity_field(
        bundle: &ContractBundle,
        record: &CanonicalRecord,
        name: &str,
    ) -> i128 {
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Budget")
            .expect("Budget entity");
        decimal_value(value(record, named_field(entity.record(), name)))
    }

    fn named_field(schema: &RecordSchema, name: &str) -> FieldId {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("named field")
            .id()
    }

    fn value(record: &CanonicalRecord, field: FieldId) -> &CanonicalValue {
        record
            .fields()
            .iter()
            .find(|(candidate, _)| *candidate == field)
            .map(|(_, value)| value)
            .expect("record value")
    }

    fn decimal_value(value: &CanonicalValue) -> i128 {
        let CanonicalValue::Decimal(value) = value else {
            panic!("expected decimal");
        };
        value.coefficient()
    }

    fn decimal(coefficient: i128) -> CanonicalValue {
        CanonicalValue::Decimal(
            Decimal::new(DecimalSpec::new(28, 2).expect("decimal spec"), coefficient)
                .expect("decimal"),
        )
    }

    fn uuid_for(value: u64) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[8..].copy_from_slice(&value.to_be_bytes());
        bytes
    }

    struct Inputs<'a> {
        input: &'a CanonicalRecord,
    }

    impl ExpressionValueSource for Inputs<'_> {
        fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
            self.input
                .fields()
                .iter()
                .find(|(candidate, _)| *candidate == field)
                .map(|(_, value)| value.clone())
        }
    }
}
