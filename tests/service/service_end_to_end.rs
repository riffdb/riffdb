#![forbid(unsafe_code)]

//! Concrete in-process application-service integration evidence.

mod support;

use std::time::{Duration, Instant};

use riffdb_errors::{ApplicationErrorCode, PublicErrorKind};
use riffdb_policy::PartitionConstraint;
use riffdb_service::{
    AdministrationApplication, AuthoritativeOutcomeSelectorRef, AuthoritativeReadinessFailure,
    CapacityRejectionStage, CommandApplication, CommandDurability, CommitApplication,
    CompactResourceDescriptorRef, ContractApplication, ContractValidationResult,
    CreateCapabilityInvocation, DiscoverCommandToolsRequest, DiscoverCommandToolsResultRef,
    DiscoverResourcesRequest, DiscoverResourcesResultRef, DiscoveryApplication,
    DiscoveryCatalogStateRef, DiscoveryRepresentation, ExecuteCommandResult, ExplainCommandResult,
    GetActiveContractRequest, GetActiveContractResult, GetCommitRequest, GetCommitResult,
    GetContractVersionResult, GetEntityResult, GetProjectionStatusResult, HealthContext,
    HealthRequest, HealthResult, HealthStatus, JournaledCompletion, PageLimit, PageRequest,
    PreBootstrapLifecycle, QueryApplication, ResolveCommandOutcomeRequest,
    ResolveCommandOutcomeResult, ResourceDescriptorRef, ResourceDiscoveryKind,
    ServiceTelemetryEvent, StatisticsRequest, TraceProvenanceResult,
};
use riffdb_types::{
    PartitionScopeV1, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
};

use support::{ReadCommitMode, ServiceHarness, run_async, sequence};

#[test]
fn pre_bootstrap_health_is_restricted_and_bypasses_policy_storage_and_audit() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);

        let result = harness
            .service
            .health(
                harness.pre_bootstrap_health(PreBootstrapLifecycle::InitializingValidation),
                HealthRequest,
            )
            .await
            .expect("restricted pre-bootstrap health succeeds");

        let HealthResult::PreBootstrap(report) = result else {
            panic!("pre-bootstrap context cannot produce authenticated health");
        };
        assert_eq!(
            report.lifecycle(),
            PreBootstrapLifecycle::InitializingValidation
        );
        assert!(report.liveness());
        assert!(!report.readiness());
        assert_eq!(harness.policy.calls(), 0);
        assert_eq!(harness.ports.read_reservations(), 0);
        assert_eq!(harness.ports.read_submissions(), 0);

        harness.stop_coordinator();
        assert!(
            harness.all_audit_phases().is_empty(),
            "restricted pre-bootstrap health must not enter the service-audit path"
        );
    });
}

#[test]
fn closed_or_foreign_pre_bootstrap_health_contexts_fail_without_semantic_access() {
    run_async(async move {
        let mut first = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let mut second = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);

        let already_issued =
            first.pre_bootstrap_health(PreBootstrapLifecycle::InitializingBootstrap);
        first.close_pre_bootstrap_health();
        let closed_failure = first
            .service
            .health(already_issued, HealthRequest)
            .await
            .expect_err("closing the service gate revokes already-issued contexts");
        assert_eq!(
            closed_failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );

        let foreign = second.pre_bootstrap_health(PreBootstrapLifecycle::InitializingValidation);
        let foreign_failure = first
            .service
            .health(foreign, HealthRequest)
            .await
            .expect_err("a context from another service cannot cross the admission gate");
        assert_eq!(
            foreign_failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );

        assert_eq!(first.policy.calls(), 0);
        assert_eq!(first.ports.read_reservations(), 0);
        assert_eq!(first.ports.read_submissions(), 0);
        first.stop_coordinator();
        second.stop_coordinator();
        assert!(first.all_audit_phases().is_empty());
        assert!(second.all_audit_phases().is_empty());
    });
}

#[test]
fn concrete_service_reads_through_policy_port_and_real_audit_coordinator() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x31;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let result = harness
            .service
            .get_commit(context, GetCommitRequest::new(sequence(7)))
            .await
            .expect("authorized commit read succeeds");

        assert_eq!(result, GetCommitResult::NotFound);
        assert_eq!(
            harness.policy.calls(),
            3,
            "policy is reloaded after capacity and after the protected read completes"
        );
        assert_eq!(harness.ports.read_reservations(), 1);
        assert_eq!(harness.ports.read_submissions(), 1);

        let expected_principal = harness.policy.principal();
        harness.stop_coordinator();
        let records = harness.audit_records(REQUEST_SEED);
        assert_eq!(
            records
                .iter()
                .map(|record| record.phase())
                .collect::<Vec<_>>(),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
        let expected_targets =
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Commit(sequence(7))])
                .expect("one canonical commit target");
        for record in records {
            assert_eq!(record.operation(), ServiceOperationV1::GetCommit);
            assert_eq!(record.ingress(), ServiceIngressKindV1::Grpc);
            assert_eq!(record.targets(), &expected_targets);
            let principal = record
                .principal()
                .expect("authenticated read audit carries its exact principal");
            assert_eq!(principal.principal_id(), expected_principal.principal_id());
            assert_eq!(principal.actor_kind(), expected_principal.actor_kind());
            assert_eq!(
                principal.capability_id(),
                expected_principal.capability_id()
            );
            assert_eq!(
                principal.capability_revision(),
                expected_principal.capability_revision()
            );
            assert_eq!(record.approval_id(), None);
            assert_eq!(record.link(), ServiceAuditLinkV1::None);
        }
    });
}

#[test]
fn saturated_command_capacity_rejects_before_reauthorization_with_typed_overload() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_one();
        // Hold the sole workload slot outside the service path.
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "capacity-one harness must report full after one hold"
        );

        let (context, _cancellation) = harness.context(0xA1);
        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("second concurrent command must be capacity-rejected");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Overloaded),
            "got {failure:?}"
        );
        assert_eq!(
            failure
                .public_error()
                .map(|error| ApplicationErrorCode::from_public_kind(error.kind()).as_str()),
            Some("RDB-CAPACITY-0101")
        );
        // begin_compound performs one initial authorize; reauthorization after
        // capacity is skipped when admission rejects. A full committed command
        // records several post-admission policy safe points.
        assert_eq!(
            harness.policy.calls(),
            1,
            "overload must reject after initial authorize and before reauthorization"
        );
        assert!(
            harness.telemetry.events().iter().any(|event| matches!(
                event,
                ServiceTelemetryEvent::CapacityRejected {
                    operation: ServiceOperationV1::ExecuteCommand,
                    ingress: ServiceIngressKindV1::Grpc,
                    stage: CapacityRejectionStage::QueueDepth,
                }
            )),
            "CapacityRejected QueueDepth must be recorded"
        );

        drop(held);
        harness.stop_coordinator();
    });
}

#[test]
fn admission_deadline_while_queued_is_overloaded_not_deadline_exceeded() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_one();
        let held = harness.hold_command_capacity();

        // Remaining budget below COMMAND_ADMISSION_MIN_REMAINING (25ms) rejects
        // immediately as overload without burning the client's deadline class.
        let (context, _cancellation) =
            harness.context_with_deadline(0xA2, Instant::now() + Duration::from_millis(10));
        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("queued-unadmitted deadline maps to overload");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Overloaded),
            "must not surface details-free DeadlineExceeded while unadmitted"
        );
        assert_ne!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::InternalDefect)
        );

        drop(held);
        harness.stop_coordinator();
    });
}

#[test]
fn cancellation_during_admission_remains_cancelled() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_one();
        let held = harness.hold_command_capacity();

        let (context, cancellation) =
            harness.context_with_deadline(0xA3, Instant::now() + Duration::from_secs(5));
        // Cancel before the bounded admission wait parks so the first poll sees
        // cancellation while still unadmitted.
        cancellation.cancel();

        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("cancellation during admission is Cancelled");

        assert!(
            matches!(failure, riffdb_service::ServiceFailure::Cancelled),
            "cancellation during admission must remain ServiceFailure::Cancelled, got {failure:?}"
        );

        drop(held);
        harness.stop_coordinator();
    });
}

#[test]
fn real_coordinator_command_and_outcome_resolution_share_the_api_neutral_service() {
    run_async(async move {
        const COMMAND_REQUEST: u8 = 0x32;
        const RESOLVE_REQUEST: u8 = 0x33;
        const LOCATOR_REQUEST: u8 = 0x34;
        let mut harness = ServiceHarness::command();
        let (command_context, _command_cancellation) = harness.context(COMMAND_REQUEST);

        let execution = harness
            .service
            .execute_command(command_context, harness.execute_command_request())
            .await
            .expect("CreateBudget commits through the concrete service");
        let ExecuteCommandResult::Journaled(committed) = execution else {
            panic!("CreateBudget is a journaled mutation");
        };
        assert_eq!(committed.completion(), JournaledCompletion::Committed);
        assert_eq!(committed.durability(), CommandDurability::Synchronous);
        assert_eq!(committed.outcome().outcome_name().as_str(), "BudgetCreated");

        harness.set_actual_outcome(&committed);
        let (resolve_context, _resolve_cancellation) = harness.context(RESOLVE_REQUEST);
        let resolution = harness
            .service
            .resolve_command_outcome(resolve_context, harness.resolve_command_outcome_request())
            .await
            .expect("the committed outcome is currently authorized");
        let ResolveCommandOutcomeResult::Found(replayed) = resolution else {
            panic!("the exact committed outcome must resolve");
        };
        assert_eq!(replayed.completion(), JournaledCompletion::Replayed);
        assert_eq!(replayed.commit_sequence(), committed.commit_sequence());
        assert_eq!(replayed.lineage(), committed.lineage());
        assert_eq!(replayed.contract_version(), committed.contract_version());
        assert_eq!(replayed.command_id(), committed.command_id());
        assert_eq!(replayed.plan_hash(), committed.plan_hash());
        assert_eq!(replayed.outcome(), committed.outcome());
        assert_eq!(replayed.outcome().outcome_name().as_str(), "BudgetCreated");
        assert_eq!(replayed.provenance_id(), committed.provenance_id());
        assert_eq!(replayed.durability(), committed.durability());
        assert_eq!(replayed.outcome_locator(), committed.outcome_locator());
        let lower_request = harness
            .ports
            .last_outcome_request()
            .expect("outcome lookup was synchronously submitted");
        let AuthoritativeOutcomeSelectorRef::RawKey(lower_request) = lower_request.selector()
        else {
            panic!("legacy uncertainty recovery must use the raw-key selector");
        };
        assert_eq!(lower_request.lineage(), committed.lineage());
        assert_eq!(lower_request.command_id(), committed.command_id());
        assert_eq!(
            lower_request.tenant_scope(),
            &riffdb_types::TenantScope::Global
        );

        let requested_locator = committed.outcome_locator().clone();
        harness.set_actual_outcome(&committed);
        let (locator_context, _locator_cancellation) = harness.context(LOCATOR_REQUEST);
        let locator_resolution = harness
            .service
            .resolve_command_outcome(
                locator_context,
                ResolveCommandOutcomeRequest::locator(requested_locator.clone()),
            )
            .await
            .expect("the canonical locator resolves through the shared service");
        let ResolveCommandOutcomeResult::Found(locator_replay) = locator_resolution else {
            panic!("the exact locator must resolve");
        };
        assert_eq!(
            locator_replay.outcome_locator().canonical_uri(),
            requested_locator.canonical_uri(),
            "locator-form recovery echoes the exact canonical request URI"
        );
        let lower_request = harness
            .ports
            .last_outcome_request()
            .expect("locator lookup was synchronously submitted");
        let AuthoritativeOutcomeSelectorRef::Digested(lower_request) = lower_request.selector()
        else {
            panic!("locator recovery must use one digested point selector");
        };
        assert_eq!(lower_request.lineage(), committed.lineage());
        assert_eq!(lower_request.command_id(), committed.command_id());
        assert_eq!(
            harness.policy.calls(),
            9,
            "the mutation adds one post-evaluation current-policy safe point"
        );
        assert_eq!(harness.ports.outcome_reservations(), 2);
        assert_eq!(harness.ports.outcome_submissions(), 2);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(COMMAND_REQUEST),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
        assert_eq!(
            harness.audit_phases(RESOLVE_REQUEST),
            [],
            "an allowed standard read emits no durable audit without a policy audit obligation"
        );
        assert!(harness.audit_phases(LOCATOR_REQUEST).is_empty());
        let expected_link = ServiceAuditLinkV1::Command {
            commit_sequence: committed.commit_sequence(),
            provenance_id: committed.provenance_id(),
        };
        assert_eq!(
            harness
                .audit_records(COMMAND_REQUEST)
                .last()
                .expect("command terminal audit")
                .link(),
            expected_link
        );
    });
}

#[test]
fn command_revocation_at_the_post_evaluation_safe_point_writes_no_terminal_state() {
    run_async(async move {
        const COMMAND_REQUEST: u8 = 0x35;
        let mut harness = ServiceHarness::command();
        harness.deny_after_next_policy_allows(2);
        let (context, _cancellation) = harness.context(COMMAND_REQUEST);

        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("post-evaluation revocation must deny before terminal commit");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(COMMAND_REQUEST),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed],
            "the denied attempt retains one complete non-command audit lifecycle"
        );
    });
}

#[test]
fn distinct_command_identities_preserve_the_exact_business_rejection_name() {
    run_async(async move {
        let mut harness = ServiceHarness::command();
        let (first_context, _first_cancellation) = harness.context(0x34);
        let first = harness
            .service
            .execute_command(
                first_context,
                harness.execute_command_request_with_key("create-budget-first-key"),
            )
            .await
            .expect("first CreateBudget commits");
        let ExecuteCommandResult::Journaled(first) = first else {
            panic!("CreateBudget is a journaled mutation");
        };
        assert_eq!(first.completion(), JournaledCompletion::Committed);
        assert_eq!(first.outcome().outcome_name().as_str(), "BudgetCreated");

        let (second_context, _second_cancellation) = harness.context(0x35);
        let second = harness
            .service
            .execute_command(
                second_context,
                harness.execute_command_request_with_key("create-budget-second-key"),
            )
            .await
            .expect("second CreateBudget reaches its declared business rejection");
        let ExecuteCommandResult::Journaled(second) = second else {
            panic!("CreateBudget rejection remains a journaled result");
        };
        assert_eq!(second.completion(), JournaledCompletion::Committed);
        assert_eq!(
            second.outcome().outcome_name().as_str(),
            "BudgetAlreadyExists"
        );
        assert_ne!(first.outcome().outcome_id(), second.outcome().outcome_id());

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0x34),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
        assert_eq!(
            harness.audit_phases(0x35),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn unknown_authoritative_outcome_id_fails_closed() {
    run_async(async move {
        let mut harness = ServiceHarness::command();
        let (command_context, _command_cancellation) = harness.context(0x36);
        let execution = harness
            .service
            .execute_command(command_context, harness.execute_command_request())
            .await
            .expect("CreateBudget commits before recovery corruption is injected");
        let ExecuteCommandResult::Journaled(committed) = execution else {
            panic!("CreateBudget is a journaled mutation");
        };
        harness.set_outcome_with_unknown_id(&committed);

        let (resolve_context, _resolve_cancellation) = harness.context(0x37);
        let failure = harness
            .service
            .resolve_command_outcome(resolve_context, harness.resolve_command_outcome_request())
            .await
            .expect_err("an undeclared durable outcome cannot cross the service boundary");
        let public = failure
            .public_error()
            .expect("internal containment obtains a caller-safe incident");
        assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
        assert!(public.incident_id().is_some());
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );

        harness.stop_coordinator();
        assert!(harness.audit_phases(0x37).is_empty());
    });
}

#[test]
fn contract_and_discovery_operations_share_current_policy_and_checked_catalog_metadata() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();

        let (validate_context, _cancellation) = harness.context(0x70);
        assert_eq!(
            harness
                .service
                .validate_contract(validate_context, harness.validate_request())
                .await
                .expect("authorized contract validation"),
            ContractValidationResult::Valid
        );

        let (explain_context, _cancellation) = harness.context(0x71);
        let ExplainCommandResult::Found(explained) = harness
            .service
            .explain_command(explain_context, harness.explain_request())
            .await
            .expect("authorized active command explanation")
        else {
            panic!("CreateBudget must be explained from the active checked bundle");
        };
        assert_eq!(explained.command_id(), explained.explanation().command_id());

        let (active_context, _cancellation) = harness.context(0x72);
        let GetActiveContractResult::Present(active) = harness
            .service
            .get_active_contract(active_context, GetActiveContractRequest)
            .await
            .expect("authorized active contract read")
        else {
            panic!("the harness has one active contract");
        };

        let (version_context, _cancellation) = harness.context(0x73);
        let GetContractVersionResult::Found(version) = harness
            .service
            .get_contract_version(version_context, harness.contract_version_request())
            .await
            .expect("authorized immutable contract read")
        else {
            panic!("the active immutable bundle must also be addressable by version");
        };
        assert_eq!(version.lineage(), active.lineage());
        assert_eq!(version.version(), active.version());
        assert_eq!(version.bundle_hash(), active.bundle_hash());

        let (tools_context, _cancellation) = harness.context(0x74);
        let tools = harness
            .service
            .discover_command_tools(tools_context, DiscoverCommandToolsRequest::default())
            .await
            .expect("policy-filtered command discovery");
        let DiscoverCommandToolsResultRef::Page { page: tools, .. } = tools.result() else {
            panic!("default command discovery must return a full page");
        };
        assert!(tools.items().iter().any(|item| {
            matches!(
                item,
                riffdb_service::CommandToolDiscoveryItem::Command(command)
                    if command.source_command().as_str() == "CreateBudget"
            )
        }));

        let (resources_context, _cancellation) = harness.context(0x75);
        let resources = harness
            .service
            .discover_resources(resources_context, DiscoverResourcesRequest::default())
            .await
            .expect("policy-filtered semantic-resource discovery");
        let DiscoverResourcesResultRef::Page(resources) = resources.result() else {
            panic!("default resource discovery must return a full page");
        };
        assert!(!resources.items().is_empty());

        assert!(harness.policy.calls() >= 12);
        assert_eq!(
            harness.ports.operation_calls(),
            [
                "active_catalog",
                "active_catalog",
                "contract_version",
                "active_catalog",
                "active_catalog"
            ],
            "contract reads and both discovery operations use checked catalog paths"
        );
        harness.stop_coordinator();
        for request_seed in 0x70..=0x75 {
            assert!(
                harness.audit_phases(request_seed).is_empty(),
                "allowed standard reads are unaudited without an audit obligation"
            );
        }
    });
}

#[test]
fn unchanged_command_discovery_observations_repeat_the_authorized_current_catalog_safe_point() {
    run_async(async move {
        const INITIAL_REQUEST: u8 = 0xff;
        const OBSERVATIONS: usize = 180;
        let mut harness = ServiceHarness::operations();
        let compact_page = PageRequest::new(PageLimit::default(), None);

        let (initial_context, _cancellation) = harness.context(INITIAL_REQUEST);
        let initial = harness
            .service
            .discover_command_tools(
                initial_context,
                DiscoverCommandToolsRequest::with_options(
                    compact_page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid initial compact discovery request"),
            )
            .await
            .expect("initial compact discovery returns the current fence");
        let DiscoverCommandToolsResultRef::CompactPage(initial_page) = initial.result() else {
            panic!("an initial compact discovery must return a page");
        };
        let prior_fence = initial_page.observed_fence().clone();

        let policy_calls = harness.policy.calls();
        let prepare_calls = harness.ports.prepare_active_calls();
        let reservations = harness.ports.active_catalog_reservations();
        let submissions = harness.ports.active_catalog_submissions();
        let cursor_tokens = harness.cursor_token_calls();
        harness.clear_discovery_order();

        for request_seed in 0..OBSERVATIONS as u8 {
            let (unchanged_context, _cancellation) = harness.context(request_seed);
            let unchanged = harness
                .service
                .discover_command_tools(
                    unchanged_context,
                    DiscoverCommandToolsRequest::with_options(
                        compact_page,
                        DiscoveryRepresentation::CompactObservation,
                        Some(prior_fence.clone()),
                    )
                    .expect("valid conditional compact discovery request"),
                )
                .await
                .expect("an equal current fence returns CatalogUnchanged");
            let DiscoverCommandToolsResultRef::CatalogUnchanged(observed_fence) =
                unchanged.result()
            else {
                panic!("an equal compact prior fence must not materialize a page");
            };
            assert_eq!(observed_fence, &prior_fence);
        }

        assert_eq!(harness.policy.calls() - policy_calls, 2 * OBSERVATIONS);
        assert_eq!(harness.ports.prepare_active_calls(), prepare_calls);
        assert_eq!(
            harness.ports.active_catalog_reservations() - reservations,
            OBSERVATIONS
        );
        assert_eq!(
            harness.ports.active_catalog_submissions() - submissions,
            OBSERVATIONS
        );
        let discovery_order = harness.discovery_order();
        assert_eq!(discovery_order.len(), 4 * OBSERVATIONS);
        assert!(
            discovery_order
                .chunks_exact(4)
                .all(|events| events == ["policy", "catalog_reserve", "policy", "catalog_submit"])
        );
        assert_eq!(
            harness.cursor_token_calls(),
            cursor_tokens,
            "CatalogUnchanged publishes no continuation cursor"
        );

        harness.stop_coordinator();
        for request_seed in 0..OBSERVATIONS as u8 {
            assert!(
                harness.audit_phases(request_seed).is_empty(),
                "unaudited standard discovery must not fabricate Started or Succeeded"
            );
        }
    });
}

#[test]
fn stale_resource_discovery_fence_returns_the_catalog_activated_at_reservation() {
    run_async(async move {
        const INITIAL_REQUEST: u8 = 0x92;
        const STALE_REQUEST: u8 = 0x93;
        let mut harness = ServiceHarness::operations();
        let compact_page = PageRequest::new(PageLimit::default(), None);

        let (initial_context, _cancellation) = harness.context(INITIAL_REQUEST);
        let initial = harness
            .service
            .discover_resources(
                initial_context,
                DiscoverResourcesRequest::with_options(
                    compact_page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                    ResourceDiscoveryKind::All,
                )
                .expect("valid initial compact resource discovery request"),
            )
            .await
            .expect("initial compact resource discovery returns the current fence");
        let DiscoverResourcesResultRef::CompactPage(initial_page) = initial.result() else {
            panic!("an initial compact resource discovery must return a page");
        };
        let stale_fence = initial_page.observed_fence().clone();

        harness.activate_compatible_successor_on_active_catalog_reservation();
        let policy_calls = harness.policy.calls();
        let prepare_calls = harness.ports.prepare_active_calls();
        let reservations = harness.ports.active_catalog_reservations();
        let submissions = harness.ports.active_catalog_submissions();
        harness.clear_discovery_order();

        let (stale_context, _cancellation) = harness.context(STALE_REQUEST);
        let changed = harness
            .service
            .discover_resources(
                stale_context,
                DiscoverResourcesRequest::with_options(
                    compact_page,
                    DiscoveryRepresentation::CompactObservation,
                    Some(stale_fence.clone()),
                    ResourceDiscoveryKind::All,
                )
                .expect("valid stale conditional resource discovery request"),
            )
            .await
            .expect("a stale fence returns the current first page");
        let DiscoverResourcesResultRef::CompactPage(current_page) = changed.result() else {
            panic!("a stale prior fence must return a compact page");
        };
        let DiscoveryCatalogStateRef::ActiveContract { version, .. } =
            current_page.observed_fence().state()
        else {
            panic!("the compatible successor remains active");
        };

        assert_ne!(current_page.observed_fence(), &stale_fence);
        assert_eq!(version, harness.prepared_active_catalog_version());
        assert_eq!(harness.policy.calls() - policy_calls, 2);
        assert_eq!(harness.ports.prepare_active_calls(), prepare_calls);
        assert_eq!(
            harness.ports.active_catalog_reservations() - reservations,
            1
        );
        assert_eq!(harness.ports.active_catalog_submissions() - submissions, 1);
        assert_eq!(
            harness.discovery_order(),
            ["policy", "catalog_reserve", "policy", "catalog_submit"]
        );

        harness.stop_coordinator();
        assert!(harness.audit_phases(STALE_REQUEST).is_empty());
    });
}

#[test]
fn large_discovery_catalogs_batch_authorization_and_page_before_the_mcp_limit() {
    run_async(async move {
        const ADDITIONAL_COMMANDS: usize = 499;
        const COMMAND_CANDIDATES: usize = 501;
        const COMMAND_ITEMS: usize = 19 + COMMAND_CANDIDATES;
        const RESOURCE_ITEMS: usize = 4 + 1 + 1 + (3 * COMMAND_CANDIDATES) + 1;
        let limit = PageLimit::new(500).expect("maximum discovery page limit");
        let mut harness = ServiceHarness::discovery_inventory(ADDITIONAL_COMMANDS);

        let policy_calls = harness.policy.calls();
        let (first_tools_context, _cancellation) = harness.context(0xa0);
        let first_tools = harness
            .service
            .discover_command_tools(
                first_tools_context,
                DiscoverCommandToolsRequest::with_options(
                    PageRequest::new(limit, None),
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid compact command discovery request"),
            )
            .await
            .expect("more than 500 command candidates filter in bounded batches");
        let DiscoverCommandToolsResultRef::CompactPage(first_tools) = first_tools.result() else {
            panic!("compact command discovery returns a compact page");
        };
        assert_eq!(first_tools.items().len(), 500);
        let command_cursor = first_tools
            .next_cursor()
            .expect("the first 500 visible tools require continuation");
        assert_eq!(
            harness.policy.calls() - policy_calls,
            3,
            "initial authorization, acceptance authorization, and one second-batch authorization"
        );

        let (final_tools_context, _cancellation) = harness.context(0xa1);
        let final_tools = harness
            .service
            .discover_command_tools(
                final_tools_context,
                DiscoverCommandToolsRequest::with_options(
                    PageRequest::new(limit, Some(command_cursor)),
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid compact command continuation"),
            )
            .await
            .expect("command continuation reaches exact end");
        let DiscoverCommandToolsResultRef::CompactPage(final_tools) = final_tools.result() else {
            panic!("compact command continuation returns a compact page");
        };
        assert_eq!(
            first_tools.items().len() + final_tools.items().len(),
            COMMAND_ITEMS
        );
        assert!(final_tools.next_cursor().is_none());

        let mut request_seed = 0xa2_u8;
        for (kind, expected_pages, expected_policy_calls, expected_items) in [
            (
                ResourceDiscoveryKind::All,
                vec![500, 500, 500, 10],
                5,
                RESOURCE_ITEMS,
            ),
            (ResourceDiscoveryKind::Concrete, vec![500, 500, 7], 4, 1_007),
            (ResourceDiscoveryKind::Template, vec![500, 3], 3, 503),
        ] {
            let policy_calls = harness.policy.calls();
            let mut cursor = None;
            let mut page_counts = Vec::new();
            let mut observed = 0;
            let mut membership = [0_usize; 10];
            loop {
                let (context, _cancellation) = harness.context(request_seed);
                request_seed = request_seed.wrapping_add(1);
                let result = harness
                    .service
                    .discover_resources(
                        context,
                        DiscoverResourcesRequest::with_options(
                            PageRequest::new(limit, cursor),
                            DiscoveryRepresentation::CompactObservation,
                            None,
                            kind,
                        )
                        .expect("valid compact resource discovery request"),
                    )
                    .await
                    .expect("large resource discovery remains bounded");
                let DiscoverResourcesResultRef::CompactPage(page) = result.result() else {
                    panic!("compact resource discovery returns a compact page");
                };
                for item in page.items() {
                    let template = matches!(
                        item.resource(),
                        CompactResourceDescriptorRef::CommandOutcome { .. }
                            | CompactResourceDescriptorRef::Commit { sequence: None }
                            | CompactResourceDescriptorRef::Provenance {
                                provenance_id: None
                            }
                    );
                    assert!(match kind {
                        ResourceDiscoveryKind::All => true,
                        ResourceDiscoveryKind::Concrete => !template,
                        ResourceDiscoveryKind::Template => template,
                    });
                    let member = match item.resource() {
                        CompactResourceDescriptorRef::ActiveContract => 0,
                        CompactResourceDescriptorRef::ContractVersion { .. } => 1,
                        CompactResourceDescriptorRef::EntitySchema { .. } => 2,
                        CompactResourceDescriptorRef::CommandPlan { .. } => 3,
                        CompactResourceDescriptorRef::CommandDocumentation { .. } => 4,
                        CompactResourceDescriptorRef::CommandOutcome { .. } => 5,
                        CompactResourceDescriptorRef::Commit { .. } => 6,
                        CompactResourceDescriptorRef::Provenance { .. } => 7,
                        CompactResourceDescriptorRef::ProjectionStatus { .. } => 8,
                        CompactResourceDescriptorRef::ServerHealth => 9,
                    };
                    membership[member] += 1;
                }
                observed += page.items().len();
                page_counts.push(page.items().len());
                cursor = page.next_cursor();
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(page_counts, expected_pages);
            assert_eq!(observed, expected_items);
            assert_eq!(
                membership,
                match kind {
                    ResourceDiscoveryKind::All => [1, 1, 1, 501, 501, 501, 1, 1, 1, 1],
                    ResourceDiscoveryKind::Concrete => [1, 1, 1, 501, 501, 0, 0, 0, 1, 1],
                    ResourceDiscoveryKind::Template => [0, 0, 0, 0, 0, 501, 1, 1, 0, 0],
                },
                "resource kind membership is exact"
            );
            assert_eq!(
                harness.policy.calls() - policy_calls,
                expected_policy_calls * page_counts.len(),
                "kind selection happens before bounded policy batching and pagination"
            );
        }

        let mut cursor = None;
        let mut full_items = 0;
        let mut full_pages = 0;
        loop {
            let (context, _cancellation) = harness.context(request_seed);
            request_seed = request_seed.wrapping_add(1);
            let result = harness
                .service
                .discover_resources(
                    context,
                    DiscoverResourcesRequest::with_options(
                        PageRequest::new(limit, cursor),
                        DiscoveryRepresentation::Full,
                        None,
                        ResourceDiscoveryKind::All,
                    )
                    .expect("valid full resource discovery request"),
                )
                .await
                .expect("full discovery walks the complete large inventory");
            let DiscoverResourcesResultRef::Page(page) = result.result() else {
                panic!("full resource discovery returns a full page");
            };
            assert!(page.items().iter().any(|item| {
                matches!(
                    item.resource(),
                    ResourceDescriptorRef::CommandPlan { .. }
                        | ResourceDescriptorRef::CommandDocumentation { .. }
                        | ResourceDescriptorRef::CommandOutcome { .. }
                )
            }));
            full_items += page.items().len();
            full_pages += 1;
            cursor = page.next_cursor();
            if cursor.is_none() {
                break;
            }
            assert!(full_pages < 20, "full discovery must make bounded progress");
        }
        assert_eq!(full_items, RESOURCE_ITEMS);
        assert!(full_items > 1_024);
        assert!(full_pages >= 4);

        let cursor_calls = harness.cursor_token_calls();
        harness.deny_after_next_policy_allows(2);
        let (denied_context, _cancellation) = harness.context(0xf0);
        let denied = harness
            .service
            .discover_command_tools(
                denied_context,
                DiscoverCommandToolsRequest::with_options(
                    PageRequest::new(limit, None),
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid command discovery request"),
            )
            .await
            .expect_err("revocation before the second candidate batch denies the whole result");
        assert_eq!(
            denied.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(
            harness.cursor_token_calls(),
            cursor_calls,
            "later-batch denial cannot publish a partial result cursor"
        );

        harness.stop_coordinator();
    });
}

#[test]
fn compact_discovery_of_exactly_1024_items_returns_500_500_24_and_stops() {
    run_async(async move {
        const ADDITIONAL_COMMANDS: usize = 337;
        let limit = PageLimit::new(500).expect("maximum discovery page limit");
        let mut harness = ServiceHarness::discovery_inventory(ADDITIONAL_COMMANDS);
        let mut cursor = None;
        let mut page_counts = Vec::new();

        for request_seed in 0xd0_u8..=0xd2 {
            let (context, _cancellation) = harness.context(request_seed);
            let result = harness
                .service
                .discover_resources(
                    context,
                    DiscoverResourcesRequest::with_options(
                        PageRequest::new(limit, cursor),
                        DiscoveryRepresentation::CompactObservation,
                        None,
                        ResourceDiscoveryKind::All,
                    )
                    .expect("valid compact discovery request"),
                )
                .await
                .expect("the maximum MCP-accepted inventory remains service-readable");
            let DiscoverResourcesResultRef::CompactPage(page) = result.result() else {
                panic!("compact resource discovery returns a compact page");
            };
            page_counts.push(page.items().len());
            cursor = page.next_cursor();
        }

        assert_eq!(page_counts, [500, 500, 24]);
        assert!(cursor.is_none(), "the exact 1,024th item is the end");
        harness.stop_coordinator();
    });
}

#[test]
fn authoritative_projection_and_operational_reads_use_typed_ports_and_audit_by_scope() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();

        let (entity_context, _cancellation) = harness.context(0x76);
        assert_eq!(
            harness
                .service
                .get_entity(entity_context, harness.entity_request())
                .await
                .expect("authorized entity lookup"),
            GetEntityResult::NotFound
        );

        let (index_context, _cancellation) = harness.context(0x77);
        let index = harness
            .service
            .scan_index(index_context, harness.index_request())
            .await
            .expect("authorized checked index scan");
        assert!(index.page().items().is_empty());

        let (projection_context, _cancellation) = harness.context(0x78);
        let GetProjectionStatusResult::Found(status) = harness
            .service
            .get_projection_status(projection_context, harness.projection_status_request())
            .await
            .expect("authorized projection-status read")
        else {
            panic!("known checked projection has an uninitialized status");
        };
        assert_eq!(status.identity(), &harness.projection_identity());

        let (scan_context, _cancellation) = harness.context(0x79);
        let commits = harness
            .service
            .scan_commits(scan_context, harness.scan_commits_request())
            .await
            .expect("authorized commit scan");
        assert!(commits.page().items().is_empty());

        let (provenance_context, _cancellation) = harness.context(0x7a);
        assert!(matches!(
            harness
                .service
                .trace_provenance(provenance_context, harness.trace_request())
                .await
                .expect("authorized provenance lookup"),
            TraceProvenanceResult::NotFound
        ));

        let (health_context, _cancellation) = harness.context(0x7b);
        let HealthResult::Authenticated(health) = harness
            .service
            .health(HealthContext::authenticated(health_context), HealthRequest)
            .await
            .expect("authenticated health uses current policy and lower ports")
        else {
            panic!("authenticated context cannot produce pre-bootstrap health");
        };
        assert_eq!(health.status(), HealthStatus::Ready);

        let (statistics_context, _cancellation) = harness.context(0x7c);
        let statistics = harness
            .service
            .statistics(statistics_context, StatisticsRequest)
            .await
            .expect("authorized process statistics");
        assert_eq!(statistics.active_cursors(), 0);
        assert_eq!(statistics.active_commit_subscribers(), 0);

        let (outbox_context, _cancellation) = harness.context(0x7d);
        let outbox = harness
            .service
            .list_pending_outbox_deliveries(outbox_context, harness.outbox_request())
            .await
            .expect("authorized payload-free outbox status");
        assert!(outbox.page().items().is_empty());

        assert_eq!(
            harness.ports.operation_calls(),
            [
                "entity",
                "index",
                "projection_status",
                "commit_scan",
                "provenance",
                "active_catalog",
                "health",
                "statistics",
                "statistics",
                "outbox",
            ]
        );
        harness.stop_coordinator();
        for request_seed in [0x76, 0x77, 0x78, 0x7b] {
            assert!(harness.audit_phases(request_seed).is_empty());
        }
        for request_seed in [0x79, 0x7a, 0x7c, 0x7d] {
            assert_eq!(
                harness.audit_phases(request_seed),
                [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
                "intrinsic administrative reads always retain their durable lifecycle"
            );
        }
    });
}

#[test]
fn index_rows_share_one_exact_historical_bundle_lookup() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();
        let mut rows = vec![harness.primary_index_row(), harness.alternate_index_row()];
        rows.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
        harness.configure_index_pages(vec![(rows, None)]);
        harness.use_compatible_successor_as_active();
        let (context, _cancellation) = harness.context(0x80);

        let result = harness
            .service
            .scan_index(context, harness.index_request())
            .await
            .expect("compatible historical index rows validate under their exact bundle");

        assert_eq!(result.page().items().len(), 2);
        assert_eq!(
            harness.ports.prepare_contract_version_calls(),
            1,
            "both rows share one exact historical binding"
        );
        assert_eq!(harness.ports.index_submissions(), 1);
        harness.stop_coordinator();
    });
}

#[test]
fn return_time_narrowing_emits_empty_progress_and_resumes_without_refill() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();
        let primary = harness.primary_index_row();
        let alternate = harness.alternate_index_row();
        assert!(primary.key().as_bytes() < alternate.key().as_bytes());
        harness.configure_index_pages(vec![
            (vec![primary.clone()], Some(primary.key().clone())),
            (vec![alternate.clone()], None),
        ]);
        harness.narrow_policy_after_next_index_submission();

        let (first_context, _cancellation) = harness.context(0x81);
        let first = harness
            .service
            .scan_index(first_context, harness.index_request())
            .await
            .expect("narrowed return policy omits the first lower row");
        assert!(first.page().items().is_empty());
        let cursor = first
            .page()
            .next_cursor()
            .expect("empty non-final page retains opaque physical progress");
        assert_eq!(harness.ports.index_submissions(), 1);
        let first_requests = harness.ports.index_requests();
        assert_eq!(first_requests.len(), 1);
        assert!(matches!(
            first_requests[0].partition_constraint(),
            PartitionConstraint::Filter(PartitionScopeV1::All)
        ));
        assert!(first_requests[0].after().is_none());

        let (second_context, _cancellation) = harness.context(0x82);
        let second = harness
            .service
            .scan_index(
                second_context,
                harness.index_request_with_cursor(Some(cursor)),
            )
            .await
            .expect("cursor resumes after the inspected physical candidate");
        assert_eq!(second.page().items().len(), 1);
        assert_eq!(second.page().items()[0].key(), alternate.key());
        assert!(second.page().next_cursor().is_none());
        assert_eq!(
            harness.ports.index_submissions(),
            2,
            "each RPC performs exactly one lower scan"
        );
        let requests = harness.ports.index_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].after(), Some(primary.key()));
        assert!(matches!(
            requests[1].partition_constraint(),
            PartitionConstraint::Filter(PartitionScopeV1::Explicit(entries))
                if entries.len() == 1
                    && entries[0].lineage() == requests[1].lineage()
                    && entries[0].partition_key() == alternate.stored_partition()
        ));
        harness.stop_coordinator();
    });
}

#[test]
fn authoritative_index_partition_mismatch_releases_no_result_or_cursor() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();
        harness.configure_index_pages(vec![(
            vec![harness.primary_index_row_with_wrong_partition()],
            None,
        )]);
        let (context, _cancellation) = harness.context(0x83);

        let failure = harness
            .service
            .scan_index(context, harness.index_request())
            .await
            .expect_err("stored and historically derived partitions must match exactly");

        let public = failure
            .public_error()
            .expect("integrity containment obtains a caller-safe incident");
        assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
        assert!(public.incident_id().is_some());
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );
        assert_eq!(harness.ports.index_submissions(), 1);
        assert_eq!(harness.cursor_token_calls(), 0);
        harness.stop_coordinator();
    });
}

#[test]
fn authoritative_index_row_outside_read_filter_is_an_integrity_failure() {
    run_async(async move {
        let mut harness = ServiceHarness::restricted_operations();
        let expected_partition = harness.primary_index_row().stored_partition().clone();
        harness.configure_index_pages(vec![(vec![harness.alternate_index_row()], None)]);
        let (context, _cancellation) = harness.context(0x84);

        let failure = harness
            .service
            .scan_index(context, harness.index_request())
            .await
            .expect_err("the lower port must not return a row outside its read-time filter");

        let public = failure
            .public_error()
            .expect("integrity containment obtains a caller-safe incident");
        assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
        assert!(public.incident_id().is_some());
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );
        assert_eq!(harness.ports.index_submissions(), 1);
        let requests = harness.ports.index_requests();
        assert!(matches!(
            requests.as_slice(),
            [request]
                if matches!(
                    request.partition_constraint(),
                    PartitionConstraint::Filter(PartitionScopeV1::Explicit(entries))
                        if entries.len() == 1
                            && entries[0].lineage() == request.lineage()
                            && entries[0].partition_key() == &expected_partition
                )
        ));
        assert_eq!(harness.cursor_token_calls(), 0);
        harness.stop_coordinator();
    });
}

#[test]
fn capability_create_and_revoke_fail_closed_at_their_owned_dependency_boundaries() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();

        let (create_context, _cancellation) = harness.context(0x7e);
        let create_failure = harness
            .service
            .create_capability(CreateCapabilityInvocation::Normal {
                context: create_context,
                request: harness.create_capability_request(),
            })
            .await
            .expect_err("unavailable token issuance fails closed after authorization");
        assert_eq!(
            create_failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );

        let (revoke_context, _cancellation) = harness.context(0x7f);
        let revoke_failure = harness
            .service
            .revoke_capability(revoke_context, harness.revoke_request())
            .await
            .expect_err(
                "the fake policy principal is intentionally absent from authoritative redb state",
            );
        assert_eq!(
            revoke_failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0x7e),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );
        assert_eq!(
            harness.audit_phases(0x7f),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied],
            "transaction-current capability absence cannot be bypassed by the fake policy port"
        );
    });
}
