#![forbid(unsafe_code)]

//! Concrete in-process application-service integration evidence.

mod support;

use riffdb_errors::PublicErrorKind;
use riffdb_service::{
    AdministrationApplication, AuthoritativeReadinessFailure, CommandApplication,
    CommandDurability, CommitApplication, ContractApplication, ContractValidationResult,
    CreateCapabilityInvocation, DiscoverCommandToolsRequest, DiscoverResourcesRequest,
    DiscoveryApplication, ExecuteCommandResult, ExplainCommandResult, GetActiveContractRequest,
    GetActiveContractResult, GetCommitRequest, GetCommitResult, GetContractVersionResult,
    GetEntityResult, GetProjectionStatusResult, HealthContext, HealthRequest, HealthResult,
    HealthStatus, JournaledCompletion, PreBootstrapLifecycle, QueryApplication,
    ResolveCommandOutcomeResult, StatisticsRequest, TraceProvenanceResult,
};
use riffdb_types::{
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1,
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
fn real_coordinator_command_and_outcome_resolution_share_the_api_neutral_service() {
    run_async(async move {
        const COMMAND_REQUEST: u8 = 0x32;
        const RESOLVE_REQUEST: u8 = 0x33;
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
        assert_eq!(harness.policy.calls(), 5);
        assert_eq!(harness.ports.outcome_reservations(), 1);
        assert_eq!(harness.ports.outcome_submissions(), 1);
        let lower_request = harness
            .ports
            .last_outcome_request()
            .expect("outcome lookup was synchronously submitted");
        assert_eq!(lower_request.lineage(), committed.lineage());
        assert_eq!(lower_request.command_id(), committed.command_id());
        assert_eq!(
            lower_request.tenant_scope(),
            &riffdb_types::TenantScope::Global
        );

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
        assert!(tools.page().items().iter().any(|item| {
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
        assert!(!resources.page().items().is_empty());

        assert!(harness.policy.calls() >= 12);
        assert_eq!(
            harness.ports.operation_calls(),
            ["active_catalog", "active_catalog", "contract_version"],
            "explain, active lookup, and historical lookup use checked catalog paths"
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
