//! Durable outcomes for distinct write evidence, using the authorized command coordinator.
// req: CMP-015, CMP-013
use super::*;
use riffdb_commit::{CommandExecutionResult, CommittedOutcomeDisposition};

const SOURCE: &str = r"
contract EvidenceBudget version 1 {
  entity Budget {
    key (organization_id: uuid, fiscal_year: i64)
    field approved_amount: decimal<28,2>
    field revision: u64
    field free: bool
    field holder: uuid
  }
  aggregate Budgets { root Budget partition_by organization_id conflict_key (organization_id, fiscal_year) }
  command CreateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input approved_amount: decimal<28,2>
    idempotency_key idempotency_key
    create Budget(organization_id, fiscal_year) as budget else Exists {}
    set budget.approved_amount = approved_amount
    set budget.revision = 0
    set budget.free = true
    set budget.holder = organization_id
    return Created {}
  }
  command ReplaceBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input amount: decimal<28,2>
    input expected_revision: u64
    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget else Missing {}
    require current: budget.revision == expected_revision else Stale {}
    set budget.revision = budget.revision + 1
    set budget.approved_amount = amount
    return Replaced {}
  }
  command ClaimBudgetSlot {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input holder: uuid
    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget else Missing {}
    require is_free: budget.free else Taken {}
    set budget.free = false checked_by is_free
    set budget.holder = holder checked_by is_free
    set budget.revision = budget.revision + 1
    return Claimed {}
  }
  command IncrementBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input amount: decimal<28,2>
    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget else Missing {}
    set budget.approved_amount = budget.approved_amount + amount
    set budget.revision = budget.revision + 1
    return Incremented { amount: budget.approved_amount }
  }
  command OverwriteBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input amount: decimal<28,2>
    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget else Missing {}
    overwrite budget.approved_amount = amount
    set budget.revision = budget.revision + 1
    return Overwritten {}
  }
}
";

fn prepare(
    database: &BudgetDatabase,
    ports: &RedbOperationalPorts,
    name: &str,
    seed: u8,
    attempt: u8,
    amount: i128,
) -> CommandExecutionPreparation {
    let plan = database
        .bundle()
        .bundle()
        .commands()
        .iter()
        .find(|p| p.name() == name)
        .expect("command");
    let key = format!("evidence-{seed}");
    let mut fields = vec![
        (
            "idempotency_key",
            CanonicalValue::string(&key).expect("key"),
        ),
        ("organization_id", CanonicalValue::Uuid(ORGANIZATION_ID)),
        ("fiscal_year", CanonicalValue::I64(FISCAL_YEAR)),
    ];
    if name == "ClaimBudgetSlot" {
        fields.push(("holder", CanonicalValue::Uuid([seed; 16])));
    } else {
        fields.push(("amount", decimal(amount)));
    }
    if name == "ReplaceBudget" {
        fields.push(("expected_revision", CanonicalValue::U64(0)));
    }
    let input = CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    fields
                        .iter()
                        .find(|(name, _)| *name == field.name())
                        .expect("input field")
                        .1
                        .clone(),
                )
            })
            .collect(),
    )
    .expect("input");
    database.prepare_plan(ports, plan, input, &key, seed, attempt)
}

#[test]
fn write_evidence_preserves_stale_refusal_replay_provenance_and_explicit_replacement() {
    let database =
        BudgetDatabase::create_from_source_with_setup("write-evidence", SOURCE, |_, _| {});
    let ports = database.open();
    let initialization = database.prepare(&ports, 12_500, 1);
    // Prepare both callers before either replacement executes, then choose an
    // explicit serialized schedule; synchronization never relies on sleeps.
    let requests = [
        ("ReplaceBudget", 10, 30, 13_000, "Replaced"),
        ("ReplaceBudget", 11, 31, 14_000, "Stale"),
        ("ReplaceBudget", 10, 32, 13_000, "Replaced"),
        ("ClaimBudgetSlot", 12, 33, 0, "Claimed"),
        ("ClaimBudgetSlot", 13, 34, 0, "Taken"),
        ("IncrementBudget", 14, 35, 100, "Incremented"),
        ("IncrementBudget", 15, 36, 200, "Incremented"),
        ("OverwriteBudget", 16, 37, 14_000, "Overwritten"),
        ("OverwriteBudget", 17, 38, 15_000, "Overwritten"),
    ];
    let preparations = requests
        .iter()
        .map(|(name, seed, attempt, amount, outcome)| {
            (
                prepare(&database, &ports, name, *seed, *attempt, *amount),
                *name,
                *outcome,
            )
        })
        .collect::<Vec<_>>();
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(80)),
    );
    let executor = coordinator.command_executor();
    let runtime = runtime();
    let execute = |preparation| {
        runtime.block_on(async {
            let result = executor
                .reserve_capacity()
                .await
                .expect("capacity")
                .submit(preparation)
                .expect("submission")
                .completion()
                .await
                .expect("completion");
            let CommandExecutionResult::Committed(outcome) = result else {
                panic!("durable declared outcome: {result:?}");
            };
            outcome
        })
    };
    let initial = execute(initialization);
    let mut outcomes = vec![initial.into_stored_outcome()];
    for (index, (preparation, name, expected)) in preparations.into_iter().enumerate() {
        let committed = execute(preparation);
        let plan = database
            .bundle()
            .bundle()
            .commands()
            .iter()
            .find(|p| p.name() == name)
            .expect("command");
        let expected = plan
            .outcomes()
            .iter()
            .find(|o| o.name() == expected)
            .expect("declared outcome")
            .id();
        assert_eq!(
            committed.stored_outcome().declared_outcome().outcome_id(),
            expected
        );
        if matches!(index, 5 | 6) {
            let amount = plan
                .outcomes()
                .iter()
                .find(|o| o.name() == "Incremented")
                .expect("outcome")
                .payload()
                .fields()
                .iter()
                .find(|f| f.name() == "amount")
                .expect("amount")
                .id();
            assert_eq!(
                committed
                    .stored_outcome()
                    .declared_outcome()
                    .value()
                    .fields(),
                &[(amount, decimal(if index == 5 { 13_100 } else { 13_300 }))]
            );
        }
        if index == 2 {
            assert_eq!(committed.disposition(), CommittedOutcomeDisposition::Replay);
            assert_eq!(committed.stored_outcome(), &outcomes[1]);
        }
        outcomes.push(committed.into_stored_outcome());
    }
    drop(executor);
    coordinator.shutdown().expect("drain");
    let ports = database.open();
    let entity = ports
        .read_entity(&database.entity_target())
        .expect("entity read")
        .expect("budget");
    assert_eq!(entity.entity_version().get(), 7);
    let schema = database
        .bundle()
        .bundle()
        .schema()
        .entity(database.target_entity_type)
        .expect("schema");
    let value = |name| {
        let id = schema
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == name)
            .expect("field")
            .id();
        entity
            .fields()
            .fields()
            .iter()
            .find(|(f, _)| *f == id)
            .expect("stored field")
            .1
            .clone()
    };
    assert_eq!(value("revision"), CanonicalValue::U64(6));
    assert_eq!(value("approved_amount"), decimal(15_000));
    assert_eq!(value("holder"), CanonicalValue::Uuid([12; 16]));
    assert_eq!(value("free"), CanonicalValue::Bool(false));
    for outcome in outcomes {
        assert_eq!(
            ports
                .read_stored_outcome(outcome.identity())
                .expect("outcome"),
            Some(outcome.clone())
        );
        let provenance = ports
            .read_provenance(outcome.provenance_id())
            .expect("provenance")
            .expect("present");
        assert_eq!(provenance.commit_sequence(), outcome.commit_sequence());
        let commit = ports
            .read_commit(outcome.commit_sequence())
            .expect("commit")
            .expect("present");
        assert_eq!(commit.provenance_id(), outcome.provenance_id());
    }
}
