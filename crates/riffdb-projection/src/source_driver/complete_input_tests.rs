// req: PRJ-004, STO-020, REC-003
//! Protocol-only backends cannot manufacture concrete native cut authority.
use super::super::SourceCheckpointBackend;
use super::*;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_storage_api::{
    CatalogRepository, ChangelogHistoryPointV3, ChangelogLineageV3, ChangelogTransactionSequence,
    LeadershipEpochV1, StorageError, StoredContractBundleV1,
};
use riffdb_types::{DatabaseId, DualFrontier};

struct Backend;
struct Brand(u8);
struct Input {
    rewinds: usize,
    reads: usize,
    foreign: Option<Brand>,
    commands: Vec<riffdb_storage_api::StoredCommitRecordV1>,
    cursor: usize,
}
impl SourceCheckpointBackend for Backend {
    type Brand = Brand;
    type StageBrand = Brand;
}
impl SourceReplayInputBackend for Backend {
    type Input = Input;
    fn rewind<'session>(
        input: &mut Input,
        actual: &Brand,
        request: SourceRewindInstruction<'session, Self>,
    ) -> Result<super::super::SourceRewindEvidence<'session, Self>, SourceDriverError> {
        request.check_before_rewind(actual)?;
        input.rewinds += 1;
        input.cursor = 0;
        request.rewound(actual)
    }
    fn read_command<'command, 'session>(
        input: &'command mut Input,
        actual: &Brand,
        request: SourceReadInstruction<'session, Self>,
    ) -> Result<SourceCommandResponse<'command, 'session, Self>, SourceDriverError> {
        request.check_before_read(actual)?;
        input.reads += 1;
        if let Some(foreign) = input.foreign.as_ref() {
            return request.exact_end(foreign);
        }
        if let Some(command) = input.commands.get(input.cursor) {
            input.cursor += 1;
            request.command(actual, command)
        } else {
            request.exact_end(actual)
        }
    }
}
struct Repository(StoredContractBundleV1);
impl CatalogRepository for Repository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(ActiveCatalogPointerV1::from_bundle(&self.0)))
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (lineage == self.0.lineage() && version == self.0.contract_version())
                .then(|| self.0.clone()),
        )
    }
}
fn catalog() -> ActiveCatalogSnapshot {
    let compiled = riffdb_contract_compiler::compile_contract_source(
        "contract CompleteInput version 1 { event Added { group: string<128> } projection First { source event Added key (group) measure n = count() frontier transactionally_ordered } projection Last { source event Added key (group) measure n = count() frontier transactionally_ordered } }",
    ).unwrap();
    let validated = ValidatedContractBundle::from_compiler_bundle(compiled).unwrap();
    ActiveCatalogSnapshot::read(&Repository(validated.to_stored().unwrap()))
        .unwrap()
        .unwrap()
}
fn history(application: Option<CommitSequence>) -> ChangelogHistoryStateV3 {
    let database = DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap();
    let lineage = ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [9; 32],
        DualFrontier::new(application, None),
    );
    ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap()
}
fn input() -> Input {
    Input {
        rewinds: 0,
        reads: 0,
        foreign: None,
        commands: Vec::new(),
        cursor: 0,
    }
}

#[test]
fn complete_active_input_preserves_absent_catalog_and_optional_application_head() {
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand, &mut input, None, &history, &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    assert_eq!(complete.checked_plan_count(), 0);
    assert_eq!(complete.checked_command_count(), 0);
    complete.consume_for_cut(&brand, &history, None).unwrap();
    assert_eq!((input.rewinds, input.reads), (0, 0));
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn complete_active_input_visits_every_actual_active_plan_over_genuine_empty_end() {
    let catalog = catalog();
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    assert_eq!(complete.checked_plan_count(), 2);
    assert_eq!(complete.checked_command_count(), 0);
    complete
        .consume_for_cut(&brand, &history, Some(catalog.pointer()))
        .unwrap();
    assert_eq!((input.rewinds, input.reads), (2, 2));
}

#[test]
fn complete_active_input_refuses_missing_required_command_before_cut_completion() {
    let catalog = catalog();
    let brand = Brand(1);
    let mut input = input();
    let history = history(Some(CommitSequence::first()));
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    assert!(matches!(
        validate_complete_active_plan_input(session),
        Err(SourceDriverError::IncompleteInput)
    ));
    assert_eq!((input.rewinds, input.reads), (1, 1));
}

#[test]
fn complete_active_input_refuses_foreign_instance_even_when_scalar_brand_matches() {
    let catalog = catalog();
    let brand = Brand(1);
    let mut input = input();
    input.foreign = Some(Brand(brand.0));
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    assert!(matches!(
        validate_complete_active_plan_input(session),
        Err(SourceDriverError::ForeignCustody)
    ));
}

#[test]
fn complete_active_input_preadmits_fixed_controls_before_backend_operations() {
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(
            riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.reserved_bytes
                - before.owned_bytes,
        )
        .unwrap();
    let pressured = budget.observation().unwrap();
    assert!(matches!(
        SourceActivePlanInputSession::<Backend>::attach(
            &brand, &mut input, None, &history, &budget,
        ),
        Err(SourceDriverError::SourceUnavailable)
    ));
    assert_eq!((input.rewinds, input.reads), (0, 0));
    assert_eq!(budget.observation().unwrap(), pressured);
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), before);
}

// Reuse the ordinary evaluator's existing typed command/event construction.
// These are protocol-only controls; they are not native artifact/cut authority.
fn two_plan_fixture() -> crate::source_evaluator::tests::EvaluatorFixture {
    use crate::source_evaluator::tests::{EVALUATOR_CONTRACT, evaluator_fixture_from_source};
    evaluator_fixture_from_source(&EVALUATOR_CONTRACT.replace(
        "  projection Totals {",
        r#"
  projection First {
    source event Added
    key (group)
    measure item_count = count()
    frontier transactionally_ordered
  }
  projection Totals {"#,
    ))
}
fn actual_catalog(
    fixture: &crate::source_evaluator::tests::EvaluatorFixture,
) -> ActiveCatalogSnapshot {
    ActiveCatalogSnapshot::read(&Repository(fixture.bundle.to_stored().unwrap()))
        .unwrap()
        .unwrap()
}

#[test]
fn complete_active_input_interprets_complete_commands_and_empty_progress_for_all_plans() {
    use crate::source_evaluator::tests::evaluator_commit;
    let fixture = two_plan_fixture();
    let catalog = actual_catalog(&fixture);
    let brand = Brand(1);
    let mut input = input();
    input.commands.push(evaluator_commit(
        &fixture,
        CommitSequence::first(),
        &[(10, 4), (10, -1)],
    ));
    input.commands.push(evaluator_commit(
        &fixture,
        CommitSequence::new(2).unwrap(),
        &[],
    ));
    let history = history(Some(CommitSequence::new(2).unwrap()));
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    assert_eq!(complete.checked_plan_count(), 2);
    assert_eq!(complete.checked_command_count(), 4);
    assert!(budget.observation().unwrap().reserved_bytes > before.reserved_bytes);
    complete
        .consume_for_cut(&brand, &history, Some(catalog.pointer()))
        .unwrap();
    assert_eq!((input.rewinds, input.reads), (2, 6));
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn complete_active_input_late_plan_refusal_releases_every_graph_before_cut_completion() {
    use crate::source_evaluator::tests::evaluator_commit;
    let fixture = two_plan_fixture();
    let catalog = actual_catalog(&fixture);
    assert!(
        catalog
            .bundle()
            .bundle()
            .projections()
            .first()
            .unwrap()
            .measures()
            .iter()
            .all(|measure| measure.expression().is_none())
    );
    let brand = Brand(1);
    let mut input = input();
    input.commands.push(evaluator_commit(
        &fixture,
        CommitSequence::first(),
        &[(10, 4)],
    ));
    input.commands.push(evaluator_commit(
        &fixture,
        CommitSequence::new(2).unwrap(),
        &[(10, i64::MAX), (10, 1), (10, -1)],
    ));
    let history = history(Some(CommitSequence::new(2).unwrap()));
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    assert!(matches!(validate_complete_active_plan_input(session),
        Err(SourceDriverError::Evaluation(error))
        if error.kind() == crate::ProjectionEvaluationErrorKind::ArithmeticOverflow));
    assert_eq!((input.rewinds, input.reads), (2, 5));
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn complete_active_input_consumption_refuses_changed_full_history_and_refunds() {
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let changed = super::tests::history(Some(CommitSequence::first()));
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand, &mut input, None, &history, &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    assert!(matches!(
        complete.consume_for_cut(&brand, &changed, None),
        Err(SourceDriverError::InputChanged)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!((input.rewinds, input.reads), (0, 0));
}

#[test]
fn complete_active_input_consumption_refuses_changed_catalog_pointer_and_refunds() {
    let catalog = catalog();
    let different = riffdb_contract_compiler::compile_contract_source(
        "contract DifferentInput version 1 { event Added { group: string<128> } projection First { source event Added key (group) measure n = count() frontier transactionally_ordered } }",
    ).unwrap();
    let different = ValidatedContractBundle::from_compiler_bundle(different)
        .unwrap()
        .to_stored()
        .unwrap();
    let pointer = ActiveCatalogPointerV1::from_bundle(&different);
    assert_ne!(catalog.pointer(), &pointer);
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    assert!(matches!(
        complete.consume_for_cut(&brand, &history, Some(&pointer)),
        Err(SourceDriverError::InputChanged)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!((input.rewinds, input.reads), (2, 2));
}

#[test]
fn complete_active_input_refuses_command_at_absent_application_head_and_refunds() {
    use crate::source_evaluator::tests::evaluator_commit;
    let fixture = two_plan_fixture();
    let catalog = actual_catalog(&fixture);
    let brand = Brand(1);
    let mut input = input();
    input
        .commands
        .push(evaluator_commit(&fixture, CommitSequence::first(), &[]));
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    assert!(matches!(
        validate_complete_active_plan_input(session),
        Err(SourceDriverError::InputChanged)
    ));
    assert_eq!((input.rewinds, input.reads), (1, 1));
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn complete_active_input_refuses_sequence_gap_before_evaluation_and_refunds() {
    use crate::source_evaluator::tests::evaluator_commit;
    let fixture = two_plan_fixture();
    let catalog = actual_catalog(&fixture);
    let brand = Brand(1);
    let mut input = input();
    input.commands.push(evaluator_commit(
        &fixture,
        CommitSequence::new(2).unwrap(),
        &[],
    ));
    let history = history(Some(CommitSequence::new(2).unwrap()));
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    assert!(matches!(
        validate_complete_active_plan_input(session),
        Err(SourceDriverError::InputChanged)
    ));
    assert_eq!((input.rewinds, input.reads), (1, 1));
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn complete_active_input_unused_completion_keeps_admission_until_physical_drop() {
    let catalog = catalog();
    let brand = Brand(1);
    let mut input = input();
    let history = history(None);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let session = SourceActivePlanInputSession::<Backend>::attach(
        &brand,
        &mut input,
        Some(&catalog),
        &history,
        &budget,
    )
    .unwrap();
    let complete = validate_complete_active_plan_input(session).unwrap();
    // No accessor or consuming handoff is called on this proof.
    assert!(budget.observation().unwrap().reserved_bytes > before.reserved_bytes);
    drop(complete);
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!((input.rewinds, input.reads), (2, 2));
}
