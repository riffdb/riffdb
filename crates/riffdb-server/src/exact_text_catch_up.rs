//! Private source custody for binary-text and exact-predicate workers.

#[cfg(test)]
use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use riffdb_catalog::{ActiveCatalogSnapshot, CatalogErrorKind};
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_storage_api::{
    ChangelogCursorErrorV3, DurableKeySchemaBindingV1, StorageErrorKind, StoredEntityRecordV1,
};
use riffdb_storage_redb::RedbDerivedSourcePin;
use riffdb_types::{CommitSequence, FrontierPosition, ProjectionGeneration};

use super::{ExactTextRuntime, MAX_PROJECTED_POLICY_CANDIDATES_V1, RebuildFailure};
use crate::columnar_adapter::read_application_head;
use crate::exact_provider_owner::ProviderPin;
use crate::projection_read_source::ProjectionReadSource;

#[path = "exact_text_delta.rs"]
mod delta;
#[path = "exact_source_population.rs"]
mod population;
pub(super) use population::{AdmittedCandidatePopulation, CandidatePopulation};
#[cfg(test)]
#[path = "exact_source_population_tests.rs"]
mod population_tests;
use delta::{Delta, DeltaPlan};
#[path = "exact_text_delta_provider.rs"]
mod provider;
pub(super) use provider::{prepare_predicate, prepare_text};

pub(super) struct CapturedSource {
    pub(super) storage: ProjectionReadSource,
    pub(super) budget: riffdb_types::exact_provider_budget_v1::ProviderBudget,
    catalog: ActiveCatalogSnapshot,
    history_incarnation: u64,
    pin: Option<RedbDerivedSourcePin>,
    head: CommitSequence,
    // Last: all private source data drops before releasing the existing work pin.
    work: ProviderPin,
}

impl CapturedSource {
    pub(super) fn capture(
        runtime: &ExactTextRuntime,
        head: CommitSequence,
        generation: ProjectionGeneration,
        work: &ProviderPin,
    ) -> Result<Self, RebuildFailure> {
        Self::capture_changed(runtime, head, generation, None, work)?
            .ok_or(RebuildFailure::Integrity)
    }

    pub(super) fn capture_successor<T>(
        runtime: &ExactTextRuntime,
        head: CommitSequence,
        generation: ProjectionGeneration,
        previous: &Selected<T>,
        work: &ProviderPin,
    ) -> Result<Option<Self>, RebuildFailure> {
        Self::capture_changed(runtime, head, generation, Some(&previous.source), work)
    }

    fn capture_changed(
        runtime: &ExactTextRuntime,
        head: CommitSequence,
        generation: ProjectionGeneration,
        previous: Option<&SelectedSource>,
        work: &ProviderPin,
    ) -> Result<Option<Self>, RebuildFailure> {
        work.ensure_active()
            .map_err(|_| RebuildFailure::Transient)?;
        let snapshot = runtime
            .storage
            .pin()
            .map_err(|_| RebuildFailure::Transient)?;
        if read_application_head(&snapshot).map_err(|_| RebuildFailure::Transient)?
            != FrontierPosition::AppliedThrough(head)
        {
            return Err(RebuildFailure::Transient);
        }
        let pin = match snapshot.pin_derived_source_v3() {
            Ok(pin) => {
                if pin.history().lineage().history_incarnation() != runtime.history_incarnation
                    || pin.position().frontier().application() != Some(head)
                {
                    return Err(RebuildFailure::Integrity);
                }
                Some(pin)
            }
            // A legacy/offline source or history retired before registration
            // still permits a complete immutable rebuild, never delta replay.
            Err(ChangelogCursorErrorV3::Storage(error))
                if matches!(
                    error.kind(),
                    StorageErrorKind::Unavailable | StorageErrorKind::HistoryPruned
                ) =>
            {
                None
            }
            Err(ChangelogCursorErrorV3::Storage(error))
                if error.kind() == StorageErrorKind::LimitExceeded =>
            {
                return Err(RebuildFailure::Capacity(generation));
            }
            Err(_) => return Err(RebuildFailure::Integrity),
        };
        if let Some(previous) = previous.filter(|previous| previous.head == head) {
            let unchanged = match (&pin, &previous.pin) {
                (Some(next), Some(prior)) if next.position() == prior.position() => {
                    // Equality alone cannot substitute a different store with
                    // copied counters. Opening proves the original owner too.
                    drop(
                        next.receipts_after(prior)
                            .map_err(|_| RebuildFailure::Integrity)?,
                    );
                    true
                }
                (None, None) => true,
                _ => false,
            };
            if unchanged {
                return Ok(None);
            }
        }
        let catalog = ActiveCatalogSnapshot::read(&snapshot)
            .map_err(|error| match error.kind() {
                CatalogErrorKind::Storage => RebuildFailure::Transient,
                _ => RebuildFailure::Integrity,
            })?
            .ok_or(RebuildFailure::Integrity)?;
        Ok(Some(Self {
            storage: ProjectionReadSource::from_snapshot(snapshot),
            budget: runtime.ownership_graph.budget.clone(),
            catalog,
            history_incarnation: runtime.history_incarnation,
            pin,
            head,
            work: work.clone(),
        }))
    }

    pub(super) fn ensure_active(&self) -> Result<(), RebuildFailure> {
        self.work
            .ensure_active()
            .map_err(|_| RebuildFailure::Transient)
    }

    pub(super) fn materialize(
        &self,
        program: &QueryAccessProgramV1,
        record: StoredEntityRecordV1,
    ) -> Result<StoredEntityRecordV1, RebuildFailure> {
        self.ensure_active()?;
        let contract = program.contract();
        self.catalog
            .materialize_derived_entity(
                &DurableKeySchemaBindingV1::new(
                    contract.lineage().clone(),
                    contract.version(),
                    contract.bundle_hash(),
                ),
                record,
            )
            .map_err(|_| RebuildFailure::Integrity)
    }
}

/// Provider and its exact source proof are installed and retired together under
/// the slot mutex. The source pin outlives private successor construction.
pub(super) struct Selected<T> {
    pub(super) provider: Arc<T>,
    source: SelectedSource,
}

#[derive(Clone)]
struct SelectedSource {
    pin: Option<RedbDerivedSourcePin>,
    head: CommitSequence,
    // Includes policy-denied and null-text rows: admission does not reduce the
    // population against which a future insert must check the existing bound.
    candidates: AdmittedCandidatePopulation,
}

impl<T> Selected<T> {
    pub(super) fn new(
        provider: T,
        captured: CapturedSource,
        candidates: CandidatePopulation,
    ) -> Result<Self, RebuildFailure> {
        Self::from_shared(Arc::new(provider), captured, candidates)
    }

    pub(super) fn from_shared(
        provider: Arc<T>,
        captured: CapturedSource,
        candidates: CandidatePopulation,
    ) -> Result<Self, RebuildFailure> {
        let candidates = candidates.finish()?;
        Ok(Self {
            provider,
            source: SelectedSource {
                pin: captured.pin,
                head: captured.head,
                candidates,
            },
        })
    }

    pub(super) fn has_source_pin(&self) -> bool {
        self.source.pin.is_some()
    }

    pub(super) const fn source_frontier(&self) -> CommitSequence {
        self.source.head
    }

    pub(super) fn validates_frontier(&self, frontier: CommitSequence) -> bool {
        self.source.head == frontier
            && self.source.candidates.len() <= MAX_PROJECTED_POLICY_CANDIDATES_V1
            && self
                .source
                .pin
                .as_ref()
                .is_none_or(|pin| pin.position().frontier().application() == Some(frontier))
    }
}

impl<T> Deref for Selected<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.provider
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::AuthoritativePointReader;
    use riffdb_types::EntityKey;

    fn selected_for_test<T>(
        provider: T,
        captured: CapturedSource,
        keys: BTreeSet<EntityKey>,
    ) -> Selected<T> {
        let population = CandidatePopulation::copy_from(&captured.budget, &keys).unwrap();
        Selected::new(provider, captured, population).unwrap()
    }

    use super::*;
    use crate::columnar_adapter::tests::{append_board_ticket_for_worker_at, board_runtime};

    fn test_work(runtime: &ExactTextRuntime) -> ProviderPin {
        let budget = runtime.ownership_graph.budget.clone();
        let owner = crate::exact_provider_owner::ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        owner.pin().unwrap()
    }

    fn board_program() -> riffdb_query_module::QueryModule {
        use riffdb_query_module::{NamedQuerySource, QueryModule, QueryModuleCandidate};
        use riffdb_types::{QueryModuleName, QueryModuleVersion};
        let bundle = riffdb_contract_compiler::compile_contract_source(
            crate::columnar_adapter::tests::ADAPTER_BOARD_CONTRACT,
        )
        .unwrap();
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("delta_board").unwrap(), QueryModuleVersion::new(1).unwrap(),
            vec![NamedQuerySource::new("Ticket", r#"
query Ticket($organization_id: Ticket.organization_id, $ticket_id: Ticket.ticket_id) {
  one ticket from Ticket where organization_id == $organization_id && ticket_id == $ticket_id else Missing
  return Found { ticket: ticket { organization_id ticket_id status title } }
  outcomes Found | Missing
}"#).unwrap()],
        ).unwrap();
        QueryModule::compile(candidate, &bundle).unwrap()
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut value = [seed; 16];
        value[6] = 0x70 | (seed & 0x0f);
        value[8] = 0x80 | (seed & 0x3f);
        value
    }

    #[cfg(feature = "test-fixtures")]
    fn deny_policy() -> riffdb_policy::AuthorizedQueryRowPolicyContextV1 {
        use riffdb_auth::PrincipalFactBindingV1;
        use riffdb_contract_ir::{
            RowPolicyExpressionNodeV1, RowPolicyOperandV1, RowPolicyOperationV1, RowPolicyPlanV1,
            RowPolicyRuleV1, RowPolicyValueSourceV1, ValueType,
        };
        use riffdb_types::{
            ActorId, ActorKind, Audience, CanonicalValue, CapabilityId, CapabilityPrincipalFactsV1,
            DatabaseId, Environment, TenantScope, Timestamp,
        };
        let bundle = riffdb_contract_compiler::compile_contract_source(
            crate::columnar_adapter::tests::ADAPTER_BOARD_CONTRACT,
        )
        .unwrap();
        let entity = bundle.schema().entities()[0].id();
        let rule = RowPolicyRuleV1::new(
            RowPolicyOperationV1::Read,
            vec![RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::Constant(CanonicalValue::Bool(false)),
                ValueType::bool(),
            ))],
            0,
            entity,
            bundle.schema(),
            &Default::default(),
        )
        .unwrap();
        let policy = RowPolicyPlanV1::new("deny", entity, vec![rule], bundle.schema()).unwrap();
        let principal = PrincipalFactBindingV1::new(
            CapabilityId::from_bytes(uuid(1)).unwrap(),
            std::num::NonZeroU64::MIN,
            DatabaseId::from_bytes(uuid(2)).unwrap(),
            Environment::new("test").unwrap(),
            ActorId::new("reader").unwrap(),
            ActorKind::Human,
            vec![Audience::new("test").unwrap()],
            TenantScope::Global,
            Timestamp::new(1, 0).unwrap(),
            Timestamp::new(100, 0).unwrap(),
            CapabilityPrincipalFactsV1::empty(),
        )
        .unwrap();
        riffdb_policy::AuthorizedQueryRowPolicyContextV1::test_fixture(
            principal,
            vec![policy],
            bundle.schema(),
        )
        .unwrap()
    }

    // req: OQ-040, PRJ-004
    #[test]
    fn source_delta_and_materialization_stop_after_owner_retirement_without_dropping_captures() {
        use riffdb_types::{CanonicalValue, EntityKeyBuilder};
        let (board, scope) = board_runtime("retired-exact-source");
        let module = board_program();
        let program = module.queries()[0].ordinary_program().unwrap();
        let entity = program.steps()[0].internal_entity_id();
        append_board_ticket_for_worker_at(&board, CommitSequence::first(), 0x20);
        let runtime = ExactTextRuntime::open(
            board.storage().projection_reads(),
            scope.path(),
            1,
            ProjectionGeneration::first(),
            Arc::new(crate::cursor::ProductionCursorMonotonicClock::new()),
        )
        .unwrap();
        let budget = runtime.ownership_graph.budget.clone();
        let owner = crate::exact_provider_owner::ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        let work = owner.pin().unwrap();
        let initial = CapturedSource::capture(
            &runtime,
            CommitSequence::first(),
            ProjectionGeneration::first(),
            &work,
        )
        .unwrap();
        let previous = selected_for_test((), initial, BTreeSet::new());
        let head = CommitSequence::new(2).unwrap();
        append_board_ticket_for_worker_at(&board, head, 0x21);
        let captured =
            CapturedSource::capture(&runtime, head, ProjectionGeneration::first(), &work).unwrap();
        let mut key = EntityKeyBuilder::new(entity);
        key.push_uuid(&uuid(0x41)).unwrap();
        key.push_uuid(&uuid(0x21)).unwrap();
        let target = riffdb_storage_api::EntityTarget::new(entity, key.finish().unwrap()).unwrap();
        let record = captured.storage.read_entity(&target).unwrap().unwrap();
        let before = budget.observation().unwrap();
        owner.retire_superseded().unwrap();
        let partition = CanonicalValue::Uuid(uuid(0x41));
        assert!(matches!(
            Delta::read(
                &captured,
                &previous.source,
                DeltaPlan {
                    program,
                    partition_value: &partition,
                    max_candidates: 500,
                    policy: None,
                    generation: ProjectionGeneration::first()
                }
            ),
            Err(RebuildFailure::Transient)
        ));
        assert!(matches!(
            captured.materialize(program, record),
            Err(RebuildFailure::Transient)
        ));
        assert_eq!(
            captured
                .pin
                .as_ref()
                .unwrap()
                .position()
                .frontier()
                .application(),
            Some(head)
        );
        assert_eq!(owner.retirement_candidate().unwrap(), None);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            before.owned_bytes
        );
        drop(work);
        assert_eq!(owner.retirement_candidate().unwrap(), None);
        drop(captured);
        assert_eq!(owner.retirement_candidate().unwrap(), Some(true));
        drop(owner);
        assert_eq!(
            budget.observation().unwrap().released_owners,
            before.released_owners + 1
        );
    }

    // req: OQ-040, PRJ-004
    #[test]
    fn captured_provider_point_uses_shared_budget_and_original_snapshot() {
        // Saturate the actual process graph in a dedicated process, preserving
        // production accounting without refusing unrelated parallel unit tests.
        const CHILD_MARKER: &str = "RIFFDB_PROVIDER_ADMISSION_TEST_CHILD";
        if std::env::var_os(CHILD_MARKER).is_none() {
            use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
            let item_name = std::any::type_name_of_val(
                &captured_provider_point_uses_shared_budget_and_original_snapshot,
            );
            let test_name = item_name.split_once("::").unwrap().1;
            let specification = ChildProcessSpec::new(std::env::current_exe().unwrap())
                .unwrap()
                .arg(test_name)
                .unwrap()
                .arg("--exact")
                .unwrap()
                .env(CHILD_MARKER, "1")
                .unwrap();
            let mut child = ChildProcessController::spawn(&specification).unwrap();
            let exit = child
                .wait_for_exit(std::time::Duration::from_secs(60))
                .unwrap();
            assert!(
                exit.status.success(),
                "isolated production-budget assertions failed"
            );
            return;
        }
        use riffdb_storage_api::EntityTarget;
        use riffdb_types::{
            EntityKeyBuilder, exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES,
        };
        let (board, scope) = board_runtime("provider-admitted-captured-point");
        let module = board_program();
        let program = module.queries()[0].ordinary_program().unwrap();
        let entity = program.steps()[0].internal_entity_id();
        append_board_ticket_for_worker_at(&board, CommitSequence::first(), 0x20);
        let runtime = ExactTextRuntime::open(
            board.storage().projection_reads(),
            scope.path(),
            1,
            ProjectionGeneration::first(),
            Arc::new(crate::cursor::ProductionCursorMonotonicClock::new()),
        )
        .unwrap();
        let captured = CapturedSource::capture(
            &runtime,
            CommitSequence::first(),
            ProjectionGeneration::first(),
            &test_work(&runtime),
        )
        .unwrap();
        let mut key = EntityKeyBuilder::new(entity);
        key.push_uuid(&uuid(0x41)).unwrap();
        key.push_uuid(&uuid(0x20)).unwrap();
        let target = EntityTarget::new(entity, key.finish().unwrap()).unwrap();
        let expected = captured.storage.read_entity(&target).unwrap().unwrap();
        append_board_ticket_for_worker_at(&board, CommitSequence::new(2).unwrap(), 0x21);
        let before = runtime.ownership_graph.budget.observation().unwrap();
        let occupied = runtime
            .ownership_graph
            .budget
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - before.fixed_owner_bytes
                    - before.owned_bytes
                    - before.reserved_bytes,
            )
            .unwrap();
        assert!(matches!(
            captured.storage.read_provider_entity_v1(&target, &captured.budget),
            Err(error) if error.kind() == StorageErrorKind::Unavailable
        ));
        assert_eq!(
            runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap()
                .owned_bytes,
            before.owned_bytes
        );
        drop(occupied);
        let record = captured
            .storage
            .read_provider_entity_v1(&target, &captured.budget)
            .unwrap()
            .unwrap();
        assert!(**record == expected);
        assert!(
            runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap()
                .owned_bytes
                > before.owned_bytes
        );
        drop(record);
        assert_eq!(
            runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap()
                .owned_bytes,
            before.owned_bytes
        );
        assert_eq!(
            runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap()
                .reserved_bytes,
            before.reserved_bytes
        );
    }

    // req: PRJ-001, PRJ-002, PRJ-004
    #[test]
    fn exact_delta_processes_at_most_64_commits_and_resumes_the_exact_receipt() {
        use riffdb_storage_api::EntityTarget;
        use riffdb_types::{CanonicalValue, EntityKeyBuilder};
        let (board, scope) = board_runtime("exact-delta-prefix");
        let module = board_program();
        let program = module.queries()[0].ordinary_program().unwrap();
        let entity = program.steps()[0].internal_entity_id();
        let generation = ProjectionGeneration::first();
        append_board_ticket_for_worker_at(&board, CommitSequence::first(), 0x20);
        let runtime = ExactTextRuntime::open(
            board.storage().projection_reads(),
            scope.path(),
            1,
            generation,
            Arc::new(crate::cursor::ProductionCursorMonotonicClock::new()),
        )
        .unwrap();
        let initial = CapturedSource::capture(
            &runtime,
            CommitSequence::first(),
            generation,
            &test_work(&runtime),
        )
        .unwrap();
        let mut key = EntityKeyBuilder::new(entity);
        key.push_uuid(&uuid(0x41)).unwrap();
        key.push_uuid(&uuid(0x20)).unwrap();
        let previous = selected_for_test((), initial, BTreeSet::from([key.finish().unwrap()]));
        for sequence in 2..=66 {
            append_board_ticket_for_worker_at(
                &board,
                CommitSequence::new(sequence).unwrap(),
                u8::try_from(sequence + 100).unwrap(),
            );
        }
        let head = CommitSequence::new(66).unwrap();
        let captured =
            CapturedSource::capture(&runtime, head, generation, &test_work(&runtime)).unwrap();
        let partition = CanonicalValue::Uuid(uuid(0x41));
        let plan = || DeltaPlan {
            program,
            partition_value: &partition,
            max_candidates: 500,
            policy: None,
            generation,
        };
        let first = Delta::read(&captured, &previous.source, plan())
            .unwrap()
            .unwrap();
        assert_eq!(first.source.head, CommitSequence::new(65).unwrap());
        assert_eq!(first.changes.len(), 64);
        assert_eq!(first.source.candidates.len(), 65);
        for (key, record) in &first.changes {
            let current = captured
                .storage
                .read_entity(&EntityTarget::new(entity, key.clone()).unwrap())
                .unwrap();
            assert_eq!(&current, record);
        }
        let second = Delta::read(&captured, &first.source, plan())
            .unwrap()
            .unwrap();
        assert_eq!(second.source.head, head);
        assert_eq!(second.changes.len(), 1);
        assert_eq!(second.source.candidates.len(), 66);
        let selected = Selected {
            provider: Arc::new(()),
            source: second.source.clone(),
        };
        assert!(
            CapturedSource::capture_successor(
                &runtime,
                head,
                generation,
                &selected,
                &test_work(&runtime)
            )
            .unwrap()
            .is_none()
        );
        #[cfg(feature = "test-fixtures")]
        {
            let policy = deny_policy();
            let denied = Delta::read(
                &captured,
                &previous.source,
                DeltaPlan {
                    policy: Some(&policy),
                    ..plan()
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(denied.source.candidates.len(), 65);
            assert!(denied.changes.values().all(Option::is_none));
            assert!(
                matches!(
                    Delta::read(
                        &captured,
                        &denied.source,
                        DeltaPlan {
                            policy: Some(&policy),
                            max_candidates: 65,
                            ..plan()
                        }
                    ),
                    Err(RebuildFailure::Capacity(_))
                ),
                "denied rows still consume the bounded candidate population"
            );
        }
        let end = Delta::read(&captured, &second.source, plan())
            .unwrap()
            .unwrap();
        assert!(end.changes.is_empty());
        assert_eq!(
            end.source.pin.unwrap().position(),
            second.source.pin.as_ref().unwrap().position()
        );
        let mut substituted = previous.source.clone();
        substituted.head = head;
        assert!(matches!(
            Delta::read(&captured, &substituted, plan()),
            Err(RebuildFailure::Integrity)
        ));
    }

    // req: PRJ-001, PRJ-002, PRJ-004
    #[test]
    fn exact_provider_capture_keeps_rows_and_source_at_one_frontier_during_writes() {
        let (board, scope) = board_runtime("exact-capture");
        let first = CommitSequence::first();
        let second = first.checked_next().unwrap();
        append_board_ticket_for_worker_at(&board, first, 0x42);
        let runtime = ExactTextRuntime::open(
            board.storage().projection_reads(),
            scope.path(),
            1,
            ProjectionGeneration::first(),
            Arc::new(crate::cursor::ProductionCursorMonotonicClock::new()),
        )
        .unwrap();
        let captured = CapturedSource::capture(
            &runtime,
            first,
            ProjectionGeneration::first(),
            &test_work(&runtime),
        )
        .unwrap();
        assert!(captured.pin.is_some());
        append_board_ticket_for_worker_at(&board, second, 0x43);

        // Explicitly ordered writes, with no sleeps or timing assumptions: the
        // captured row/policy source cannot observe the later commit.
        assert_eq!(
            read_application_head(&captured.storage).unwrap(),
            FrontierPosition::AppliedThrough(first)
        );
        assert!(captured.storage.read_commit(first).unwrap().is_some());
        assert!(captured.storage.read_commit(second).unwrap().is_none());
        assert_eq!(
            read_application_head(&runtime.storage).unwrap(),
            FrontierPosition::AppliedThrough(second)
        );

        let selected = selected_for_test((), captured, BTreeSet::new());
        assert!(selected.validates_frontier(first));
        assert!(!selected.validates_frontier(second));
        let next = CapturedSource::capture(
            &runtime,
            second,
            ProjectionGeneration::first(),
            &test_work(&runtime),
        )
        .unwrap();
        let mut receipts = next
            .pin
            .as_ref()
            .unwrap()
            .receipts_after(selected.source.pin.as_ref().unwrap())
            .unwrap();
        let mut reached = first;
        while let Some(receipt) = receipts.next_receipt().unwrap() {
            reached = receipt.binding().covered_frontier.application().unwrap();
        }
        assert_eq!(reached, second);
    }
}
