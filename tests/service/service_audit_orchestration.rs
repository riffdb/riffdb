#![forbid(unsafe_code)]

//! Fail-closed application-service audit orchestration evidence.

mod support;

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use riffdb_errors::PublicErrorKind;
use riffdb_service::{
    AdministrationApplication, AuthoritativeCommitNotification, AuthoritativeReadinessFailure,
    BootstrapCapabilityResult, CommandApplication, CommitApplication, CommitSubscriptionEndReason,
    CommitSubscriptionEvent, ContractApplication, CreateCapabilityInvocation,
    CreateCapabilityResult, DeployContractResult, DiscoverCommandToolsRequest,
    DiscoverCommandToolsResultRef, DiscoverResourcesRequest, DiscoverResourcesResultRef,
    DiscoveryApplication, DiscoveryRepresentation, ExecuteCommandResult, GetCommitRequest,
    JournaledCommandResult, JournaledCompletion, MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS,
    MAX_LIVE_COMMIT_SUBSCRIBERS, NormalCreateCapabilityRequest, OutcomeResourceLocator, PageLimit,
    PageRequest, ProjectionPageFence, QueryApplication, QueryProjectionResult,
    ResolveCommandOutcomeRequest, ResolveCommandOutcomeResult, ResourceDiscoveryKind,
    ServiceFailure, ServiceTelemetryEvent, StatisticsRequest, SubscribeToCommitsRequest,
};
use riffdb_types::{
    FrontierPosition, ProjectionGeneration, ProjectionIdentity, ProjectionPlanHash,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceOperationV1,
};

use support::{
    CommitContinuationPanic, ProjectionObservation, ReadCommitMode, ServiceHarness, run_async,
    sequence,
};

async fn execute_journaled(harness: &ServiceHarness, request_seed: u8) -> JournaledCommandResult {
    let (context, _cancellation) = harness.context(request_seed);
    let result = harness
        .service
        .execute_command(context, harness.execute_command_request())
        .await
        .expect("command commits before outcome test");
    let ExecuteCommandResult::Journaled(result) = result else {
        panic!("CreateBudget is a journaled mutation");
    };
    result
}

fn locator_with_segment(
    locator: &OutcomeResourceLocator,
    segment_index: usize,
    replacement: String,
) -> OutcomeResourceLocator {
    let path = locator
        .canonical_uri()
        .strip_prefix("riffdb://outcome/")
        .expect("service locator namespace");
    let mut segments = path.split('/').map(str::to_owned).collect::<Vec<_>>();
    segments[segment_index] = replacement;
    OutcomeResourceLocator::parse(format!("riffdb://outcome/{}", segments.join("/")))
        .expect("canonical modified locator")
}

async fn active_commit_subscribers(harness: &ServiceHarness, request_seed: u8) -> u16 {
    let (context, _cancellation) = harness.context(request_seed);
    harness
        .service
        .statistics(context, StatisticsRequest)
        .await
        .unwrap_or_else(|failure| {
            panic!(
                "statistics remain available: {:?}",
                failure.public_error().map(|error| error.kind())
            )
        })
        .active_commit_subscribers()
}

#[test]
fn audited_discovery_full_compact_and_unchanged_results_close_exactly_once() {
    run_async(async move {
        const COMMAND_FULL: u8 = 0xa0;
        const COMMAND_COMPACT: u8 = 0xa1;
        const COMMAND_UNCHANGED: u8 = 0xa2;
        const RESOURCE_FULL: u8 = 0xa3;
        const RESOURCE_COMPACT: u8 = 0xa4;
        const RESOURCE_UNCHANGED: u8 = 0xa5;

        let mut harness = ServiceHarness::operations();
        harness.audit_discovery_operations();
        let page = PageRequest::new(PageLimit::default(), None);

        let (context, _cancellation) = harness.context(COMMAND_FULL);
        let full_commands = harness
            .service
            .discover_command_tools(context, DiscoverCommandToolsRequest::default())
            .await
            .expect("audited full command discovery");
        assert!(matches!(
            full_commands.result(),
            DiscoverCommandToolsResultRef::Page { .. }
        ));

        let (context, _cancellation) = harness.context(COMMAND_COMPACT);
        let compact_commands = harness
            .service
            .discover_command_tools(
                context,
                DiscoverCommandToolsRequest::with_options(
                    page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid compact command-discovery request"),
            )
            .await
            .expect("audited compact command discovery");
        let DiscoverCommandToolsResultRef::CompactPage(compact_commands) =
            compact_commands.result()
        else {
            panic!("compact command discovery returns a compact page");
        };
        let command_fence = compact_commands.observed_fence().clone();

        let (context, _cancellation) = harness.context(COMMAND_UNCHANGED);
        let unchanged_commands = harness
            .service
            .discover_command_tools(
                context,
                DiscoverCommandToolsRequest::with_options(
                    page,
                    DiscoveryRepresentation::CompactObservation,
                    Some(command_fence.clone()),
                )
                .expect("valid conditional command-discovery request"),
            )
            .await
            .expect("audited unchanged command discovery");
        let DiscoverCommandToolsResultRef::CatalogUnchanged(observed) = unchanged_commands.result()
        else {
            panic!("equal command fence returns CatalogUnchanged");
        };
        assert_eq!(observed, &command_fence);

        let (context, _cancellation) = harness.context(RESOURCE_FULL);
        let full_resources = harness
            .service
            .discover_resources(context, DiscoverResourcesRequest::default())
            .await
            .expect("audited full resource discovery");
        assert!(matches!(
            full_resources.result(),
            DiscoverResourcesResultRef::Page(_)
        ));

        let (context, _cancellation) = harness.context(RESOURCE_COMPACT);
        let compact_resources = harness
            .service
            .discover_resources(
                context,
                DiscoverResourcesRequest::with_options(
                    page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                    ResourceDiscoveryKind::All,
                )
                .expect("valid compact resource-discovery request"),
            )
            .await
            .expect("audited compact resource discovery");
        let DiscoverResourcesResultRef::CompactPage(compact_resources) = compact_resources.result()
        else {
            panic!("compact resource discovery returns a compact page");
        };
        let resource_fence = compact_resources.observed_fence().clone();

        let (context, _cancellation) = harness.context(RESOURCE_UNCHANGED);
        let unchanged_resources = harness
            .service
            .discover_resources(
                context,
                DiscoverResourcesRequest::with_options(
                    page,
                    DiscoveryRepresentation::CompactObservation,
                    Some(resource_fence.clone()),
                    ResourceDiscoveryKind::All,
                )
                .expect("valid conditional resource-discovery request"),
            )
            .await
            .expect("audited unchanged resource discovery");
        let DiscoverResourcesResultRef::CatalogUnchanged(observed) = unchanged_resources.result()
        else {
            panic!("equal resource fence returns CatalogUnchanged");
        };
        assert_eq!(observed, &resource_fence);

        harness.stop_coordinator();
        for request_seed in [
            COMMAND_FULL,
            COMMAND_COMPACT,
            COMMAND_UNCHANGED,
            RESOURCE_FULL,
            RESOURCE_COMPACT,
            RESOURCE_UNCHANGED,
        ] {
            assert_eq!(
                harness.audit_phases(request_seed),
                [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
                "each audited discovery result closes one exact lifecycle"
            );
        }
    });
}

async fn assert_explicit_scope_validation_rejection(
    mut harness: ServiceHarness,
    request_seed: u8,
    request: NormalCreateCapabilityRequest,
) {
    let (context, _cancellation) = harness.context(request_seed);
    let failure = harness
        .service
        .create_capability(CreateCapabilityInvocation::Normal { context, request })
        .await
        .expect_err("an invalid explicit capability scope must fail closed");
    assert_eq!(
        failure.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::Validation)
    );
    assert_eq!(harness.ports.prepare_active_calls(), 1);
    assert_eq!(harness.policy.calls(), 0);
    assert_eq!(harness.ports.token_issue_calls(), 0);
    assert_eq!(harness.ports.capability_order(), ["catalog"]);
    harness.stop_coordinator();
    assert_eq!(
        harness.audit_phases(request_seed),
        [ServiceAuditPhaseV1::Failed],
        "pre-policy explicit-scope rejection appends one standalone terminal"
    );
}

#[test]
fn explicit_capability_partition_validation_precedes_policy_and_token_issuance() {
    run_async(async move {
        let mut valid = ServiceHarness::operations();
        let request = valid.valid_explicit_capability_request();
        let (context, _cancellation) = valid.context(0x81);
        let failure = valid
            .service
            .create_capability(CreateCapabilityInvocation::Normal { context, request })
            .await
            .expect_err("the checked explicit scope reaches the unavailable token boundary");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(valid.ports.prepare_active_calls(), 1);
        assert_eq!(valid.policy.calls(), 1);
        assert_eq!(valid.ports.token_issue_calls(), 1);
        assert_eq!(
            valid.ports.capability_order(),
            ["catalog", "policy", "token"]
        );
        valid.stop_coordinator();
        assert_eq!(
            valid.audit_phases(0x81),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );

        let malformed = ServiceHarness::operations();
        let request = malformed.malformed_explicit_capability_request();
        assert_explicit_scope_validation_rejection(malformed, 0x82, request).await;

        let mixed = ServiceHarness::operations();
        let request = mixed.mixed_valid_invalid_capability_request();
        assert_explicit_scope_validation_rejection(mixed, 0x8d, request).await;

        let absent_aggregate = ServiceHarness::operations();
        let request = absent_aggregate.absent_aggregate_capability_request();
        assert_explicit_scope_validation_rejection(absent_aggregate, 0x83, request).await;

        let wrong_lineage = ServiceHarness::operations();
        let request = wrong_lineage.wrong_lineage_capability_request();
        assert_explicit_scope_validation_rejection(wrong_lineage, 0x84, request).await;

        let absent_catalog = ServiceHarness::operations();
        absent_catalog.remove_active_catalog();
        let request = absent_catalog.valid_explicit_capability_request();
        assert_explicit_scope_validation_rejection(absent_catalog, 0x85, request).await;
    });
}

#[test]
fn all_partition_capability_scope_bypasses_catalog_validation() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();
        harness.corrupt_active_catalog_preparation();
        let (context, _cancellation) = harness.context(0x86);
        let failure = harness
            .service
            .create_capability(CreateCapabilityInvocation::Normal {
                context,
                request: harness.create_capability_request(),
            })
            .await
            .expect_err("All bypasses catalog preparation and reaches token issuance");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(harness.ports.prepare_active_calls(), 0);
        assert_eq!(harness.policy.calls(), 1);
        assert_eq!(harness.ports.token_issue_calls(), 1);
        assert_eq!(harness.ports.capability_order(), ["policy", "token"]);
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0x86),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );
    });
}

#[test]
fn compatible_activation_after_catalog_observation_preserves_partition_validation() {
    run_async(async move {
        let mut harness = ServiceHarness::operations();
        let observed_version = harness.prepared_active_catalog_version();
        harness.configure_compatible_activation_race();
        let request = harness.valid_explicit_capability_request();
        let (context, _cancellation) = harness.context(0x8e);
        let service = harness.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .create_capability(CreateCapabilityInvocation::Normal { context, request })
                .await
        });

        harness.wait_for_active_catalog_preparation().await;
        assert_eq!(
            harness.prepared_active_catalog_version(),
            observed_version,
            "the preparatory read captured the original compatible schema"
        );
        harness.complete_compatible_activation_race();
        assert_eq!(
            harness.prepared_active_catalog_version().get(),
            observed_version.get() + 1
        );

        let failure = invocation
            .await
            .expect("service task remains contained")
            .expect_err("the validated request reaches the unavailable token boundary");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(harness.ports.prepare_active_calls(), 1);
        assert_eq!(harness.policy.calls(), 1);
        assert_eq!(harness.ports.token_issue_calls(), 1);
        assert_eq!(
            harness.ports.capability_order(),
            ["catalog", "policy", "token"]
        );
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0x8e),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );
    });
}

#[test]
fn explicit_capability_catalog_failures_preserve_intrinsic_audit_mapping() {
    run_async(async move {
        let mut storage = ServiceHarness::operations();
        storage.fail_active_catalog_preparation();
        let request = storage.valid_explicit_capability_request();
        let (context, _cancellation) = storage.context(0x87);
        let failure = storage
            .service
            .create_capability(CreateCapabilityInvocation::Normal { context, request })
            .await
            .expect_err("catalog storage failure fails closed");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(storage.ports.prepare_active_calls(), 1);
        assert_eq!(storage.policy.calls(), 0);
        assert_eq!(storage.ports.token_issue_calls(), 0);
        assert_eq!(storage.ports.capability_order(), ["catalog"]);
        storage.stop_coordinator();
        assert_eq!(storage.audit_phases(0x87), [ServiceAuditPhaseV1::Failed]);

        let mut integrity = ServiceHarness::operations();
        integrity.corrupt_active_catalog_preparation();
        let request = integrity.valid_explicit_capability_request();
        let (context, _cancellation) = integrity.context(0x88);
        let failure = integrity
            .service
            .create_capability(CreateCapabilityInvocation::Normal { context, request })
            .await
            .expect_err("catalog integrity failure is contained");
        assert_internal_with_incident(&failure);
        assert_eq!(
            integrity.health.failures(),
            [AuthoritativeReadinessFailure::Integrity]
        );
        assert_eq!(integrity.ports.prepare_active_calls(), 1);
        assert_eq!(integrity.policy.calls(), 0);
        assert_eq!(integrity.ports.token_issue_calls(), 0);
        assert_eq!(integrity.ports.capability_order(), ["catalog"]);
        integrity.stop_coordinator();
        assert_eq!(integrity.audit_phases(0x88), [ServiceAuditPhaseV1::Failed]);

        let mut cancellation = ServiceHarness::operations();
        cancellation.block_active_catalog_preparation();
        let request = cancellation.valid_explicit_capability_request();
        let (context, cancel) = cancellation.context(0x89);
        let service = cancellation.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .create_capability(CreateCapabilityInvocation::Normal { context, request })
                .await
        });
        cancellation.wait_for_active_catalog_preparation().await;
        assert_eq!(cancellation.ports.prepare_active_calls(), 1);
        assert_eq!(cancellation.policy.calls(), 0);
        assert_eq!(cancellation.ports.token_issue_calls(), 0);
        cancel.cancel();
        assert!(matches!(
            invocation.await.expect("service task remains contained"),
            Err(ServiceFailure::Cancelled)
        ));
        assert_eq!(cancellation.ports.capability_order(), ["catalog"]);
        cancellation.stop_coordinator();
        assert_eq!(
            cancellation.audit_phases(0x89),
            [ServiceAuditPhaseV1::Cancelled]
        );

        let mut deadline = ServiceHarness::operations();
        deadline.block_active_catalog_preparation();
        deadline.control_request_deadline();
        let request = deadline.valid_explicit_capability_request();
        let (context, _cancellation) = deadline.context(0x8c);
        let service = deadline.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .create_capability(CreateCapabilityInvocation::Normal { context, request })
                .await
        });
        deadline.wait_for_active_catalog_preparation().await;
        deadline.wait_for_request_deadline().await;
        assert_eq!(deadline.ports.prepare_active_calls(), 1);
        assert_eq!(deadline.policy.calls(), 0);
        assert_eq!(deadline.ports.token_issue_calls(), 0);
        deadline.elapse_request_deadline();
        assert!(matches!(
            invocation.await.expect("service task remains contained"),
            Err(ServiceFailure::DeadlineExceeded)
        ));
        assert_eq!(deadline.ports.capability_order(), ["catalog"]);
        deadline.stop_coordinator();
        assert_eq!(
            deadline.audit_phases(0x8c),
            [ServiceAuditPhaseV1::Cancelled]
        );
    });
}

#[test]
fn explicit_bootstrap_rejection_is_unaudited_and_consumes_no_bootstrap_state() {
    run_async(async move {
        let mut harness = ServiceHarness::pre_bootstrap();
        let explicit = harness
            .service
            .create_capability(CreateCapabilityInvocation::Bootstrap {
                context: harness.bootstrap_context(0x8a),
                request: harness.explicit_bootstrap_request(),
            })
            .await
            .expect_err("pre-active bootstrap rejects every explicit partition scope");
        assert_eq!(
            explicit.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Validation)
        );
        assert_eq!(harness.ports.prepare_active_calls(), 0);
        assert_eq!(harness.policy.calls(), 0);
        assert_eq!(harness.ports.token_issue_calls(), 0);
        assert!(harness.ports.capability_order().is_empty());

        let result = harness
            .service
            .create_capability(CreateCapabilityInvocation::Bootstrap {
                context: harness.bootstrap_context(0x8b),
                request: harness.all_partitions_bootstrap_request(),
            })
            .await
            .expect("All remains a viable pre-active bootstrap scope");
        assert!(
            matches!(
                &result,
                CreateCapabilityResult::Bootstrap(BootstrapCapabilityResult::Created(_))
            ),
            "unexpected bootstrap result: {result:?}"
        );
        assert_eq!(harness.ports.prepare_active_calls(), 0);
        assert_eq!(harness.policy.calls(), 0);
        assert_eq!(harness.ports.token_issue_calls(), 0);
        assert!(harness.ports.capability_order().is_empty());
        harness.stop_coordinator();
        assert!(harness.audit_phases(0x8a).is_empty());
        assert_eq!(
            harness.audit_phases(0x8b),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
            "Created proves the rejected same-ID explicit attempt consumed no bootstrap state"
        );
    });
}

#[test]
fn known_command_and_control_plane_replays_keep_the_exact_succeeded_link() {
    run_async(async move {
        let mut command = ServiceHarness::command();
        let committed = execute_journaled(&command, 0xd0).await;
        let replayed = execute_journaled(&command, 0xd1).await;
        assert_eq!(committed.completion(), JournaledCompletion::Committed);
        assert_eq!(replayed.completion(), JournaledCompletion::Replayed);
        assert_eq!(replayed.commit_sequence(), committed.commit_sequence());
        assert_eq!(replayed.provenance_id(), committed.provenance_id());
        command.stop_coordinator();

        let expected_command_link = ServiceAuditLinkV1::Command {
            commit_sequence: committed.commit_sequence(),
            provenance_id: committed.provenance_id(),
        };
        for request_seed in [0xd0, 0xd1] {
            let records = command.audit_records(request_seed);
            assert_eq!(records.len(), 2);
            assert_eq!(records[1].phase(), ServiceAuditPhaseV1::Succeeded);
            assert_eq!(records[1].link(), expected_command_link);
        }

        let mut deployment = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        for request_seed in [0xd2, 0xd3] {
            let (context, _cancellation) = deployment.context(request_seed);
            let result = deployment
                .service
                .deploy_contract(context, deployment.deploy_request())
                .await
                .expect("the exact active contract is a known replay");
            assert!(matches!(result, DeployContractResult::AlreadyActive(_)));
        }
        deployment.stop_coordinator();
        let first = deployment.audit_records(0xd2);
        let second = deployment.audit_records(0xd3);
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_eq!(first[1].phase(), ServiceAuditPhaseV1::Succeeded);
        assert_eq!(second[1].phase(), ServiceAuditPhaseV1::Succeeded);
        assert!(matches!(
            first[1].link(),
            ServiceAuditLinkV1::ControlPlane { .. }
        ));
        assert_eq!(
            second[1].link(),
            first[1].link(),
            "an already-active replay retains the original control-plane sequence"
        );
    });
}

#[test]
fn lagged_commit_stream_preserves_resume_frontier_and_releases_resources() {
    run_async(async move {
        assert_eq!(MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS, 256);
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Lagged {
                resume_after: FrontierPosition::BeforeFirst,
            }],
            false,
        );
        let (context, _cancellation) = harness.context(0xb0);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        assert_eq!(active_commit_subscribers(&harness, 0xb1).await, 1);

        let event = subscription.next().await.expect("typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("lagged source cannot emit a commit");
        };
        assert_eq!(terminal.reason(), CommitSubscriptionEndReason::Lagged);
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xb2).await, 0);

        let repeated = subscription.next().await.expect("terminal remains stable");
        assert_eq!(repeated, CommitSubscriptionEvent::Terminal(terminal));
        harness.stop_coordinator();
    });
}

#[test]
fn post_establishment_cancellation_ends_the_stream_without_another_audit() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xd0;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), false);
        harness.stall_next_commit_notification();
        let (context, cancellation) = harness.context(REQUEST_SEED);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();

        let mut next = subscription.next();
        {
            let mut task_context = Context::from_waker(Waker::noop());
            assert!(matches!(
                next.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
        }
        assert!(harness.commit_notification_is_stalled());
        cancellation.cancel();
        let event = next.await.expect("cancellation is a typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("cancellation cannot expose a commit");
        };
        assert_eq!(terminal.reason(), CommitSubscriptionEndReason::Cancelled);
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xd1).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn post_establishment_request_deadline_ends_the_stream_deterministically() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xd2;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), false);
        harness.stall_next_commit_notification();
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        harness.control_request_deadline();

        let mut next = subscription.next();
        {
            let mut task_context = Context::from_waker(Waker::noop());
            assert!(matches!(
                next.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
        }
        assert!(harness.commit_notification_is_stalled());
        assert!(harness.request_deadline_is_waiting());
        harness.elapse_request_deadline();
        let event = next.await.expect("deadline is a typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("deadline cannot expose a commit");
        };
        assert_eq!(
            terminal.reason(),
            CommitSubscriptionEndReason::DeadlineExceeded
        );
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xd3).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn maximum_lifetime_ends_the_stream_on_an_explicit_scheduler_tick() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xd4;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), false);
        harness.stall_next_commit_notification();
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(5))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        harness.control_stream_lifetime();

        let mut next = subscription.next();
        {
            let mut task_context = Context::from_waker(Waker::noop());
            assert!(matches!(
                next.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
        }
        assert!(harness.commit_notification_is_stalled());
        assert!(harness.stream_lifetime_is_waiting());
        harness.elapse_stream_lifetime();
        let event = next.await.expect("lifetime is a typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("lifetime cannot expose a commit");
        };
        assert_eq!(
            terminal.reason(),
            CommitSubscriptionEndReason::LifetimeElapsed
        );
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xd5).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn post_establishment_policy_denial_withholds_the_next_commit() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xd6;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let first = sequence(1);
        harness.set_read_commit_snapshot(harness.commit_snapshot(first));
        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            false,
        );
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        harness.revoke_policy();

        let event = subscription
            .next()
            .await
            .expect("policy denial is a typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("revocation cannot expose the unauthorized commit");
        };
        assert_eq!(terminal.reason(), CommitSubscriptionEndReason::PolicyDenied);
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert_eq!(harness.ports.read_submissions(), 0);
        assert!(harness.commit_subscription_source_dropped());
        assert!(
            harness
                .telemetry
                .events()
                .contains(&ServiceTelemetryEvent::StreamClosedByPolicy)
        );

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn scan_gap_and_source_shutdown_return_exact_resume_terminals() {
    run_async(async move {
        let scenarios = [
            (
                0xd7,
                AuthoritativeCommitNotification::Gap {
                    resume_after: FrontierPosition::BeforeFirst,
                },
                CommitSubscriptionEndReason::ScanGap,
            ),
            (
                0xd8,
                AuthoritativeCommitNotification::Closed,
                CommitSubscriptionEndReason::ServiceShutdown,
            ),
        ];

        for (request_seed, notification, expected_reason) in scenarios {
            let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
            harness.configure_commit_subscription(vec![notification], false);
            let (context, _cancellation) = harness.context(request_seed);
            let established = harness
                .service
                .subscribe_to_commits(
                    context,
                    SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                        .expect("bounded subscription"),
                )
                .await
                .expect("subscription establishes");
            let mut subscription = established.into_subscription();

            let event = subscription.next().await.expect("typed stream terminal");
            let CommitSubscriptionEvent::Terminal(terminal) = event else {
                panic!("terminal source notification cannot expose a commit");
            };
            assert_eq!(terminal.reason(), expected_reason);
            assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
            assert!(harness.commit_subscription_source_dropped());
            assert_eq!(
                active_commit_subscribers(&harness, request_seed + 0x10).await,
                0
            );

            harness.stop_coordinator();
            assert_eq!(
                harness.audit_phases(request_seed),
                [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
            );
        }
    });
}

#[test]
fn stale_wake_and_coalesced_frontier_deliver_every_contiguous_commit_once() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xd9;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let first = sequence(1);
        let second = sequence(2);
        let third = sequence(3);
        let fourth = sequence(4);
        harness.set_read_commit_snapshot(harness.commit_snapshot(second));
        harness.configure_commit_subscription(
            vec![
                AuthoritativeCommitNotification::Advanced(first),
                AuthoritativeCommitNotification::Advanced(third),
                AuthoritativeCommitNotification::Advanced(fourth),
                AuthoritativeCommitNotification::Closed,
            ],
            false,
        );
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(Some(first), Duration::from_secs(30))
                    .expect("bounded resumed subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        let policy_calls_after_establishment = harness.policy.calls();

        for expected in [second, third, fourth] {
            harness.set_read_commit_snapshot(harness.commit_snapshot(expected));
            let event = subscription
                .next()
                .await
                .expect("contiguous commit is delivered");
            let CommitSubscriptionEvent::Commit(commit) = event else {
                panic!("a contiguous frontier cannot terminate early");
            };
            assert_eq!(commit.as_snapshot().sequence(), expected);
        }
        assert_eq!(harness.ports.read_submissions(), 3);
        assert_eq!(
            harness.commit_subscription_acknowledgements(),
            [second, third, fourth],
            "only response-budgeted contiguous commits advance the safe resume position"
        );
        assert_eq!(
            harness.policy.calls() - policy_calls_after_establishment,
            9,
            "each visible commit has all three current-policy safe points"
        );

        let terminal = subscription
            .next()
            .await
            .expect("closed source reports a stable resume point");
        let CommitSubscriptionEvent::Terminal(terminal) = terminal else {
            panic!("source closure cannot duplicate the last commit");
        };
        assert_eq!(
            terminal.reason(),
            CommitSubscriptionEndReason::ServiceShutdown
        );
        assert_eq!(
            terminal.resume_after(),
            FrontierPosition::AppliedThrough(fourth)
        );
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xda).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn subscriber_limit_is_exact_and_released_capacity_can_be_reused() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let mut subscriptions = Vec::with_capacity(usize::from(MAX_LIVE_COMMIT_SUBSCRIBERS));
        for ordinal in 0..MAX_LIVE_COMMIT_SUBSCRIBERS {
            harness.configure_commit_subscription(Vec::new(), false);
            let request_seed = u8::try_from(ordinal + 1).expect("128 subscriber seeds fit u8");
            let (context, _cancellation) = harness.context(request_seed);
            let established = harness
                .service
                .subscribe_to_commits(
                    context,
                    SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                        .expect("bounded subscription"),
                )
                .await
                .expect("subscriber within the hard limit establishes");
            subscriptions.push(established.into_subscription());
        }

        harness.configure_commit_subscription(Vec::new(), false);
        let (context, _cancellation) = harness.context(0x81);
        let failure = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect_err("subscriber 129 must be rejected");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );

        drop(subscriptions.pop().expect("one live subscriber"));
        harness.configure_commit_subscription(Vec::new(), false);
        let (context, _cancellation) = harness.context(0x82);
        let replacement = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("released capacity is reusable");
        subscriptions.push(replacement.into_subscription());

        drop(subscriptions);
        assert_eq!(active_commit_subscribers(&harness, 0x83).await, 0);
        harness.stop_coordinator();
    });
}

#[test]
fn historical_affected_keys_fail_closed_before_output_or_frontier_publication() {
    run_async(async move {
        let first = sequence(1);
        let second = sequence(2);

        let mut get = ServiceHarness::operations();
        get.set_read_commit_snapshot(get.commit_snapshot_with_incomplete_affected_key(first));
        let (context, _cancellation) = get.context(0xe0);
        let failure = get
            .service
            .get_commit(context, GetCommitRequest::new(first))
            .await
            .expect_err("an incomplete historical entity key cannot be released");
        assert_internal_with_incident(&failure);
        assert_eq!(get.ports.prepare_contract_version_calls(), 1);
        assert_eq!(
            get.policy.calls(),
            2,
            "catalog validation precedes the final policy check"
        );
        assert_eq!(
            get.health.failures(),
            [AuthoritativeReadinessFailure::Integrity]
        );
        get.stop_coordinator();
        assert_eq!(
            get.audit_phases(0xe0),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );

        let mut scan = ServiceHarness::operations();
        scan.set_commit_scan_snapshots(vec![
            scan.commit_snapshot_with_valid_affected_key(first, 0),
            scan.commit_snapshot_with_trailing_affected_key(second),
        ]);
        let (context, _cancellation) = scan.context(0xe1);
        let failure = scan
            .service
            .scan_commits(context, scan.scan_commits_request())
            .await
            .expect_err("one trailing key invalidates the complete protected page");
        assert_internal_with_incident(&failure);
        assert_eq!(
            scan.ports.prepare_contract_version_calls(),
            1,
            "one historical bundle lookup serves every page item at the same lineage and version"
        );
        assert_eq!(
            scan.cursor_token_calls(),
            0,
            "no continuation cursor is published"
        );
        assert_eq!(
            scan.policy.calls(),
            2,
            "catalog validation precedes the final policy check"
        );
        assert_eq!(
            scan.health.failures(),
            [AuthoritativeReadinessFailure::Integrity]
        );
        scan.stop_coordinator();
        assert_eq!(
            scan.audit_phases(0xe1),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );

        let mut provenance = ServiceHarness::operations();
        provenance.set_provenance_snapshot(
            provenance.provenance_snapshot_with_incomplete_affected_key(first),
        );
        let (context, _cancellation) = provenance.context(0xe2);
        let failure = provenance
            .service
            .trace_provenance(context, provenance.trace_request())
            .await
            .expect_err("incomplete provenance entity keys remain private");
        assert_internal_with_incident(&failure);
        assert_eq!(provenance.ports.prepare_contract_version_calls(), 1);
        assert_eq!(
            provenance.policy.calls(),
            2,
            "catalog validation precedes the final policy check"
        );
        assert_eq!(
            provenance.health.failures(),
            [AuthoritativeReadinessFailure::Integrity]
        );
        provenance.stop_coordinator();
        assert_eq!(
            provenance.audit_phases(0xe2),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );

        let mut stream = ServiceHarness::operations();
        stream.set_read_commit_snapshot(stream.commit_snapshot_with_trailing_affected_key(first));
        stream.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            false,
        );
        let (context, _cancellation) = stream.context(0xe3);
        let established = stream
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes before per-item validation");
        let policy_calls_after_establishment = stream.policy.calls();
        let mut subscription = established.into_subscription();
        let event = subscription
            .next()
            .await
            .expect("key-integrity failure is a typed stream terminal");
        let CommitSubscriptionEvent::Terminal(terminal) = event else {
            panic!("a trailing entity key cannot become a stream item");
        };
        assert_eq!(terminal.reason(), CommitSubscriptionEndReason::Unavailable);
        assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
        assert_eq!(stream.ports.prepare_contract_version_calls(), 1);
        assert_eq!(
            stream.policy.calls() - policy_calls_after_establishment,
            2,
            "the catalog wait occurs before the final per-item policy check"
        );
        assert_eq!(
            stream.health.failures(),
            [AuthoritativeReadinessFailure::Integrity]
        );
        assert!(stream.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&stream, 0xe4).await, 0);
        stream.stop_coordinator();
        assert_eq!(
            stream.audit_phases(0xe3),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
            "post-establishment integrity failure appends no second terminal audit"
        );
    });
}

#[test]
fn oversized_commit_stream_item_is_withheld_and_releases_resources() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let first = sequence(1);
        harness.set_read_commit_snapshot(harness.oversized_commit_snapshot(first));
        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            false,
        );
        let (context, _cancellation) = harness.context(0xb3);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();
        assert_eq!(active_commit_subscribers(&harness, 0xb4).await, 1);

        assert!(matches!(
            subscription.next().await,
            Err(ServiceFailure::ResponseTooLarge)
        ));
        assert!(
            harness.commit_subscription_acknowledgements().is_empty(),
            "a withheld item cannot advance the hub's safe resume position"
        );
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xb5).await, 0);
        assert!(matches!(
            subscription.next().await,
            Err(ServiceFailure::ResponseTooLarge)
        ));
        harness.stop_coordinator();
    });
}

#[test]
fn post_establishment_stream_panics_are_contained_without_a_second_audit() {
    run_async(async move {
        #[derive(Clone, Copy)]
        enum Scenario {
            Policy,
            Continuation(CommitContinuationPanic),
        }

        let scenarios = [
            Scenario::Policy,
            Scenario::Continuation(CommitContinuationPanic::SourceNext),
            Scenario::Continuation(CommitContinuationPanic::SourcePoll),
            Scenario::Continuation(CommitContinuationPanic::ReadSubmit),
            Scenario::Continuation(CommitContinuationPanic::SourceDrop),
        ];

        for (ordinal, scenario) in scenarios.into_iter().enumerate() {
            let request_seed = 0xc0_u8 + u8::try_from(ordinal).expect("bounded scenario ordinal");
            let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
            let notification = match scenario {
                Scenario::Continuation(CommitContinuationPanic::SourceDrop) => {
                    AuthoritativeCommitNotification::Closed
                }
                Scenario::Policy | Scenario::Continuation(_) => {
                    AuthoritativeCommitNotification::Advanced(sequence(1))
                }
            };
            harness.configure_commit_subscription(vec![notification], false);
            let (context, _cancellation) = harness.context(request_seed);
            let established = harness
                .service
                .subscribe_to_commits(
                    context,
                    SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                        .expect("bounded subscription"),
                )
                .await
                .expect("subscription establishes before the injected panic");
            let mut subscription = established.into_subscription();

            match scenario {
                Scenario::Policy => harness.panic_next_policy_check(),
                Scenario::Continuation(point) => harness.panic_commit_continuation_at(point),
            }

            let failure = subscription
                .next()
                .await
                .expect_err("a post-establishment panic is contained");
            assert_internal_with_incident(&failure);
            assert!(harness.commit_subscription_source_dropped());
            assert_eq!(
                active_commit_subscribers(&harness, request_seed + 0x10).await,
                0
            );

            let lower_calls = (
                harness.ports.read_reservations(),
                harness.ports.read_submissions(),
            );
            let terminal = subscription
                .next()
                .await
                .expect("the contained continuation remains terminal");
            let CommitSubscriptionEvent::Terminal(terminal) = terminal else {
                panic!("a contained continuation cannot emit another commit");
            };
            assert_eq!(terminal.reason(), CommitSubscriptionEndReason::Unavailable);
            assert_eq!(terminal.resume_after(), FrontierPosition::BeforeFirst);
            assert_eq!(
                subscription
                    .next()
                    .await
                    .expect("the unavailable terminal remains stable"),
                CommitSubscriptionEvent::Terminal(terminal)
            );
            assert_eq!(
                (
                    harness.ports.read_reservations(),
                    harness.ports.read_submissions(),
                ),
                lower_calls,
                "a stable terminal never retries a lower provider"
            );
            assert_eq!(
                harness
                    .telemetry
                    .events()
                    .into_iter()
                    .filter(|event| {
                        *event
                            == ServiceTelemetryEvent::InternalIntegrity {
                                operation: ServiceOperationV1::SubscribeToCommits,
                            }
                    })
                    .count(),
                1,
                "one contained panic records one incident event"
            );

            harness.stop_coordinator();
            assert_eq!(
                harness.audit_phases(request_seed),
                [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
                "post-establishment containment cannot append a second terminal audit"
            );
        }
    });
}

#[test]
fn contained_stream_panic_restores_the_last_delivered_frontier() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let first = sequence(1);
        let second = sequence(2);
        harness.configure_commit_subscription(
            vec![
                AuthoritativeCommitNotification::Advanced(first),
                AuthoritativeCommitNotification::Advanced(second),
            ],
            false,
        );
        harness.set_read_commit_snapshot(harness.commit_snapshot(first));
        let (context, _cancellation) = harness.context(0xc8);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let mut subscription = established.into_subscription();

        let first_event = subscription
            .next()
            .await
            .expect("the first commit is delivered safely");
        let CommitSubscriptionEvent::Commit(first_commit) = first_event else {
            panic!("the first notification has a matching commit");
        };
        assert_eq!(first_commit.as_snapshot().sequence(), first);

        harness.set_read_commit_snapshot(harness.commit_snapshot(second));
        harness.panic_next_policy_check();
        let failure = subscription
            .next()
            .await
            .expect_err("the second item policy panic is contained");
        assert_internal_with_incident(&failure);
        let terminal = subscription
            .next()
            .await
            .expect("the contained stream exposes its stable resume point");
        let CommitSubscriptionEvent::Terminal(terminal) = terminal else {
            panic!("no second commit can be emitted after containment");
        };
        assert_eq!(terminal.reason(), CommitSubscriptionEndReason::Unavailable);
        assert_eq!(
            terminal.resume_after(),
            FrontierPosition::AppliedThrough(first),
            "the failed second item cannot advance the safely delivered frontier"
        );
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xc9).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0xc8),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn cancelled_subscription_establishment_does_not_leak_its_lease() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), true);
        let (context, cancellation) = harness.context(0xb6);
        let service = harness.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .subscribe_to_commits(
                    context,
                    SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                        .expect("bounded subscription"),
                )
                .await
        });
        harness.wait_for_commit_subscription_submission().await;
        assert_eq!(active_commit_subscribers(&harness, 0xb7).await, 1);
        cancellation.cancel();
        assert!(matches!(
            invocation.await.expect("service task does not panic"),
            Err(ServiceFailure::Cancelled)
        ));
        assert_eq!(active_commit_subscribers(&harness, 0xb8).await, 0);
        harness.release_commit_subscription_source();
        assert!(harness.commit_subscription_source_dropped());
        harness.stop_coordinator();
    });
}

#[test]
fn dropping_an_established_subscription_releases_its_source_and_subscriber_lease() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), false);
        let (context, _cancellation) = harness.context(0xbb);
        let established = harness
            .service
            .subscribe_to_commits(
                context,
                SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                    .expect("bounded subscription"),
            )
            .await
            .expect("subscription establishes");
        let subscription = established.into_subscription();

        assert_eq!(active_commit_subscribers(&harness, 0xbc).await, 1);
        assert!(!harness.commit_subscription_source_dropped());
        drop(subscription);
        assert!(harness.commit_subscription_source_dropped());
        assert_eq!(active_commit_subscribers(&harness, 0xbd).await, 0);

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(0xbb),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
    });
}

#[test]
fn subscription_terminal_audit_failure_drops_the_source_and_acquired_lease() {
    run_async(async move {
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.configure_commit_subscription(Vec::new(), true);
        let (context, _cancellation) = harness.context(0xb9);
        let service = harness.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .subscribe_to_commits(
                    context,
                    SubscribeToCommitsRequest::new(None, Duration::from_secs(30))
                        .expect("bounded subscription"),
                )
                .await
        });
        harness.wait_for_commit_subscription_submission().await;
        assert_eq!(active_commit_subscribers(&harness, 0xba).await, 1);
        harness.stop_coordinator();
        harness.release_commit_subscription_source();

        let failure = invocation
            .await
            .expect("service task does not panic")
            .expect_err("terminal audit failure withholds the stream handle");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert!(harness.commit_subscription_source_dropped());
    });
}

#[test]
fn projection_pending_then_revocation_denies_without_a_second_observation() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x31;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        let required = sequence(5);
        harness.set_projection_observations(vec![ProjectionObservation::Pending(
            harness.projection_fence(ProjectionGeneration::first(), FrontierPosition::BeforeFirst),
        )]);
        harness.revoke_after_next_projection_observation();
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let failure = harness
            .service
            .query_projection(context, harness.query_projection_request(required))
            .await
            .expect_err("the post-wake policy check must withhold projection output");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(harness.policy.calls(), 3);
        assert_eq!(harness.ports.projection_reservations(), 1);
        assert_eq!(harness.ports.projection_submissions(), 1);
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Denied],
            "an unaudited allowed read emits exactly one standalone denial when revoked"
        );
    });
}

#[test]
fn index_policy_intersection_denial_is_a_denied_audit_not_a_failed_operation() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x39;
        let mut harness = ServiceHarness::restricted_operations();
        harness.use_narrowed_policy_for_next_safe_point();
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let failure = harness
            .service
            .scan_index(context, harness.index_request())
            .await
            .expect_err("disjoint return-time authority must withhold the observed index page");
        let constraints = harness.policy.partition_constraints();

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(harness.policy.calls(), 3);
        assert_eq!(
            harness.policy.allowed_calls(),
            harness.policy.calls(),
            "each point-in-time policy decision allows; only monotonic intersection denies"
        );
        assert_eq!(constraints.len(), 3);
        assert_ne!(constraints[0], constraints[1]);
        assert_eq!(constraints[0], constraints[2]);
        assert_eq!(harness.ports.operation_calls(), ["index"]);
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Denied],
            "post-start derived authorization loss is a denial, never an execution failure"
        );
    });
}

#[test]
fn projection_pending_then_ready_reauthorizes_without_restarting_wait_or_audit() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x32;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        let required = sequence(5);
        let generation = ProjectionGeneration::first();
        harness.set_projection_observations(vec![
            ProjectionObservation::Pending(
                harness.projection_fence(generation, FrontierPosition::BeforeFirst),
            ),
            ProjectionObservation::ReadyEmpty(
                harness.projection_fence(generation, FrontierPosition::AppliedThrough(required)),
            ),
        ]);
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let result = harness
            .service
            .query_projection(context, harness.query_projection_request(required))
            .await
            .expect("fresh policy remains authorized through the ready observation");

        let QueryProjectionResult::Ready(ready) = result else {
            panic!("the second atomic observation is ready");
        };
        assert!(ready.data().items().is_empty());
        assert_eq!(ready.frontier(), FrontierPosition::AppliedThrough(required));
        assert_eq!(harness.policy.calls(), 5);
        assert_eq!(harness.ports.projection_reservations(), 2);
        assert_eq!(harness.ports.projection_submissions(), 2);
        let requests = harness.ports.projection_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0], requests[1],
            "a wake resubmits the same normalized request and absolute wait deadline"
        );
        harness.stop_coordinator();
        assert!(
            harness.audit_phases(REQUEST_SEED).is_empty(),
            "one ordinary allowed standard-read invocation remains unaudited across wakes"
        );
    });
}

#[test]
fn stalled_projection_receipt_uses_the_original_wait_deadline_without_fabricating_a_frontier() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x3a;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        harness.set_projection_observations(vec![ProjectionObservation::Stalled]);
        harness.control_projection_deadline();
        let required = sequence(5);
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let outer_deadline = context.control().deadline();
        let service = harness.service.clone();
        let request = harness.query_projection_request(required);
        let invocation =
            tokio::spawn(async move { service.query_projection(context, request).await });

        harness.wait_for_stalled_projection().await;
        harness.elapse_projection_deadline();
        let failure = invocation
            .await
            .expect("service task does not panic")
            .expect_err("a stalled provider cannot invent a typed projection timeout frontier");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(harness.policy.calls(), 2);
        assert_eq!(harness.ports.projection_reservations(), 1);
        assert_eq!(harness.ports.projection_submissions(), 1);
        let submitted = harness.ports.projection_requests();
        assert_eq!(submitted.len(), 1);
        assert!(
            submitted[0].deadline() < outer_deadline,
            "the lower request retains its original projection-specific absolute deadline"
        );
        harness.stop_coordinator();
        assert!(harness.audit_phases(REQUEST_SEED).is_empty());
    });
}

#[test]
fn projection_pending_observation_rejects_wrong_identity_and_satisfied_frontier() {
    run_async(async move {
        let mut identity_harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        let required = sequence(5);
        let expected = identity_harness.projection_identity();
        let wrong_identity = ProjectionIdentity::new(
            expected.contract_lineage().clone(),
            expected.projection_id(),
            ProjectionPlanHash::from_bytes([0xee; 32]),
        );
        identity_harness.set_projection_observations(vec![ProjectionObservation::Pending(
            ProjectionPageFence::new(
                wrong_identity,
                ProjectionGeneration::first(),
                FrontierPosition::BeforeFirst,
            ),
        )]);
        let (context, _cancellation) = identity_harness.context(0x34);

        let identity_failure = identity_harness
            .service
            .query_projection(context, identity_harness.query_projection_request(required))
            .await
            .expect_err("a lower provider cannot substitute a projection identity");

        assert_internal_with_incident(&identity_failure);
        assert_eq!(identity_harness.policy.calls(), 2);
        assert!(
            identity_harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );
        identity_harness.stop_coordinator();
        assert!(identity_harness.audit_phases(0x34).is_empty());

        let mut frontier_harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        frontier_harness.set_projection_observations(vec![ProjectionObservation::Pending(
            frontier_harness.projection_fence(
                ProjectionGeneration::first(),
                FrontierPosition::AppliedThrough(required),
            ),
        )]);
        let (context, _cancellation) = frontier_harness.context(0x35);

        let frontier_failure = frontier_harness
            .service
            .query_projection(context, frontier_harness.query_projection_request(required))
            .await
            .expect_err("a satisfied frontier is not a pending observation");

        assert_internal_with_incident(&frontier_failure);
        assert_eq!(frontier_harness.policy.calls(), 2);
        assert!(
            frontier_harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );
        frontier_harness.stop_coordinator();
        assert!(frontier_harness.audit_phases(0x35).is_empty());
    });
}

#[test]
fn projection_pending_observation_ceiling_fails_closed() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x33;
        const OBSERVATION_CEILING: usize = 64;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        let required = sequence(5);
        let pending = ProjectionObservation::Pending(
            harness.projection_fence(ProjectionGeneration::first(), FrontierPosition::BeforeFirst),
        );
        harness.set_projection_observations(vec![pending; OBSERVATION_CEILING]);
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let failure = harness
            .service
            .query_projection(context, harness.query_projection_request(required))
            .await
            .expect_err("an immediate-wake provider cannot drive unbounded work");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(harness.ports.projection_reservations(), OBSERVATION_CEILING);
        assert_eq!(harness.ports.projection_submissions(), OBSERVATION_CEILING);
        assert_eq!(harness.policy.calls(), 1 + (2 * OBSERVATION_CEILING));
        harness.stop_coordinator();
        assert!(harness.audit_phases(REQUEST_SEED).is_empty());
    });
}

#[test]
fn revocation_at_the_post_capacity_safe_point_denies_without_submission() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x41;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        harness.revoke_on_read_reservation();
        let (context, _cancellation) = harness.context(REQUEST_SEED);

        let failure = harness
            .service
            .get_commit(context, GetCommitRequest::new(sequence(8)))
            .await
            .expect_err("current revocation must deny at the second safe point");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(harness.policy.calls(), 2);
        assert_eq!(harness.ports.read_reservations(), 1);
        assert_eq!(
            harness.ports.read_submissions(),
            0,
            "denial occurs before synchronous lower-port admission"
        );

        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied]
        );
    });
}

#[test]
fn cancellation_while_the_lower_read_is_pending_appends_cancelled() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x42;
        let mut harness = ServiceHarness::new(ReadCommitMode::Pending, true);
        let (context, cancellation) = harness.context(REQUEST_SEED);
        let service = harness.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .get_commit(context, GetCommitRequest::new(sequence(9)))
                .await
        });

        harness.ports.wait_for_read_submission().await;
        cancellation.cancel();
        let failure = invocation
            .await
            .expect("service task does not panic")
            .expect_err("cancelled invocation cannot release a read result");

        assert!(matches!(failure, ServiceFailure::Cancelled));
        assert_eq!(harness.policy.calls(), 2);
        harness.ports.abandon_pending_read();
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Cancelled]
        );
    });
}

#[test]
fn terminal_audit_unavailability_suppresses_an_already_ready_result() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x43;
        let mut harness = ServiceHarness::new(ReadCommitMode::Pending, true);
        let (context, _cancellation) = harness.context(REQUEST_SEED);
        let service = harness.service.clone();
        let invocation = tokio::spawn(async move {
            service
                .get_commit(context, GetCommitRequest::new(sequence(10)))
                .await
        });

        harness.ports.wait_for_read_submission().await;
        harness.stop_coordinator();
        harness.ports.release_read_not_found();
        let failure = invocation
            .await
            .expect("service task does not panic")
            .expect_err("output remains private when terminal audit cannot append");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(
            harness.audit_phases(REQUEST_SEED),
            [ServiceAuditPhaseV1::Started],
            "the read output existed, but no success was released without a durable terminal"
        );
        assert!(
            harness
                .telemetry
                .events()
                .contains(&ServiceTelemetryEvent::AuditUnavailable {
                    operation: ServiceOperationV1::GetCommit,
                })
        );
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::AuditUnavailable)
        );
    });
}

#[test]
fn deploy_prestart_failures_append_one_standalone_phase_without_policy_or_preparation() {
    run_async(async move {
        let mut catalog_failure = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        catalog_failure.fail_active_catalog_preparation();
        let (context, _cancellation) = catalog_failure.context(0x61);
        let failure = catalog_failure
            .service
            .deploy_contract(context, catalog_failure.deploy_request())
            .await
            .expect_err("active-catalog storage failure cannot reach deployment policy");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(catalog_failure.policy.calls(), 0);
        catalog_failure.stop_coordinator();
        let records = catalog_failure.audit_records(0x61);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Failed);
        assert_eq!(records[0].targets(), &ServiceAuditTargetsV1::empty());

        let mut compile_failure = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let (context, _cancellation) = compile_failure.context(0x62);
        let failure = compile_failure
            .service
            .deploy_contract(context, compile_failure.invalid_deploy_request())
            .await
            .expect_err("invalid contract compilation remains a failed deployment attempt");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Validation)
        );
        assert_eq!(compile_failure.policy.calls(), 0);
        compile_failure.stop_coordinator();
        let records = compile_failure.audit_records(0x62);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Failed);
        assert_eq!(records[0].targets(), &ServiceAuditTargetsV1::empty());

        let mut cancelled = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        cancelled.block_active_catalog_preparation();
        let (context, cancel) = cancelled.context(0x6a);
        let service = cancelled.service.clone();
        let request = cancelled.deploy_request();
        let invocation =
            tokio::spawn(async move { service.deploy_contract(context, request).await });
        cancelled.wait_for_active_catalog_preparation().await;
        cancel.cancel();
        let failure = invocation
            .await
            .expect("service task remains contained")
            .expect_err("active-catalog cancellation remains prestart");
        assert!(matches!(failure, ServiceFailure::Cancelled));
        assert_eq!(cancelled.policy.calls(), 0);
        cancelled.stop_coordinator();
        let records = cancelled.audit_records(0x6a);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Cancelled);
        assert_eq!(records[0].targets(), &ServiceAuditTargetsV1::empty());
    });
}

#[test]
fn explain_preparation_obeys_cancellation_before_policy_or_audit() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0x6b;
        let mut harness = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, false);
        harness.block_active_catalog_preparation();
        let (context, cancellation) = harness.context(REQUEST_SEED);
        let service = harness.service.clone();
        let request = harness.explain_request();
        let invocation =
            tokio::spawn(async move { service.explain_command(context, request).await });

        harness.wait_for_active_catalog_preparation().await;
        cancellation.cancel();
        let failure = invocation
            .await
            .expect("service task remains contained")
            .expect_err("pre-policy catalog preparation must observe request cancellation");

        assert!(matches!(failure, ServiceFailure::Cancelled));
        assert_eq!(harness.policy.calls(), 0);
        harness.stop_coordinator();
        assert!(harness.audit_records(REQUEST_SEED).is_empty());
    });
}

#[test]
fn deploy_policy_and_preparation_failures_preserve_the_prestart_start_boundary() {
    run_async(async move {
        let mut policy_panic = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let expected_targets = policy_panic.deploy_targets();
        policy_panic.panic_next_policy_check();
        policy_panic.fail_deployment_preparation();
        let (context, _cancellation) = policy_panic.context(0x63);
        let failure = policy_panic
            .service
            .deploy_contract(context, policy_panic.deploy_request())
            .await
            .expect_err("a contained initial-policy panic cannot escape intrinsic audit");
        assert_internal_with_incident(&failure);
        assert_eq!(policy_panic.policy.calls(), 1);
        policy_panic.stop_coordinator();
        let records = policy_panic.audit_records(0x63);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Failed);
        assert_eq!(records[0].targets(), &expected_targets);

        let mut preparation_error = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        let expected_targets = preparation_error.deploy_targets();
        preparation_error.fail_deployment_preparation();
        let (context, _cancellation) = preparation_error.context(0x64);
        let failure = preparation_error
            .service
            .deploy_contract(context, preparation_error.deploy_request())
            .await
            .expect_err("catalog preparation failure follows durable start");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(
            preparation_error.policy.calls(),
            2,
            "preparation result is reauthorized before its typed failure is released"
        );
        preparation_error.stop_coordinator();
        let records = preparation_error.audit_records(0x64);
        assert_eq!(
            records
                .iter()
                .map(|record| record.phase())
                .collect::<Vec<_>>(),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );
        assert!(
            records
                .iter()
                .all(|record| record.targets() == &expected_targets)
        );
    });
}

#[test]
fn deploy_preparation_cancellation_and_panic_each_select_one_terminal() {
    run_async(async move {
        let mut cancellation = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        cancellation.block_deployment_preparation();
        let (context, cancel) = cancellation.context(0x65);
        let service = cancellation.service.clone();
        let request = cancellation.deploy_request();
        let invocation =
            tokio::spawn(async move { service.deploy_contract(context, request).await });
        cancellation.wait_for_deployment_preparation().await;
        cancel.cancel();
        let failure = invocation
            .await
            .expect("service task remains contained")
            .expect_err("cancelled protected preparation cannot proceed");
        assert!(matches!(failure, ServiceFailure::Cancelled));
        cancellation.stop_coordinator();
        assert_eq!(
            cancellation.audit_phases(0x65),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Cancelled]
        );

        let mut panic = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        panic.panic_deployment_preparation();
        let (context, _cancellation) = panic.context(0x66);
        let failure = panic
            .service
            .deploy_contract(context, panic.deploy_request())
            .await
            .expect_err("catalog preparation panic is contained after Started");
        assert_internal_with_incident(&failure);
        panic.stop_coordinator();
        assert_eq!(
            panic.audit_phases(0x66),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Failed]
        );
    });
}

#[test]
fn active_execute_uses_its_exact_snapshot_while_revoke_panics_remain_contained() {
    run_async(async move {
        let mut command = ServiceHarness::command();
        command.panic_executable_plan_resolution();
        let committed = execute_journaled(&command, 0x67).await;
        assert_eq!(committed.completion(), JournaledCompletion::Committed);
        command.stop_coordinator();
        assert_eq!(
            command.audit_phases(0x67),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
            "the exact active snapshot carries the plan without a second lower lookup"
        );

        let mut revoke_read = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        revoke_read.panic_revoke_target_read();
        let (context, _cancellation) = revoke_read.context(0x68);
        let failure = revoke_read
            .service
            .revoke_capability(context, revoke_read.revoke_request())
            .await
            .expect_err("revoke target-read panic after classification is contained");
        assert_internal_with_incident(&failure);
        assert_eq!(revoke_read.policy.calls(), 0);
        revoke_read.stop_coordinator();
        assert_eq!(
            revoke_read.audit_phases(0x68),
            [ServiceAuditPhaseV1::Failed]
        );

        let mut revoke_policy = ServiceHarness::new(ReadCommitMode::ImmediateNotFound, true);
        revoke_policy.panic_next_policy_check();
        let (context, _cancellation) = revoke_policy.context(0x69);
        let failure = revoke_policy
            .service
            .revoke_capability(context, revoke_policy.revoke_request())
            .await
            .expect_err("capability-mutation policy panic is contained before Started");
        assert_internal_with_incident(&failure);
        assert_eq!(revoke_policy.policy.calls(), 1);
        revoke_policy.stop_coordinator();
        assert_eq!(
            revoke_policy.audit_phases(0x69),
            [ServiceAuditPhaseV1::Failed]
        );
    });
}

#[test]
fn missing_outcome_returns_bounded_absence_after_pre_lookup_authorization() {
    run_async(async move {
        let mut harness = ServiceHarness::command();
        let (context, _cancellation) = harness.context(0x51);

        let result = harness
            .service
            .resolve_command_outcome(context, harness.resolve_command_outcome_request())
            .await
            .expect("authorized missing outcome is a successful bounded absence");

        assert_eq!(result, ResolveCommandOutcomeResult::NotFound);
        assert_eq!(harness.policy.calls(), 2);
        assert_eq!(harness.ports.outcome_reservations(), 1);
        assert_eq!(harness.ports.outcome_submissions(), 1);
        harness.stop_coordinator();
        assert!(harness.audit_phases(0x51).is_empty());
    });
}

#[test]
fn locator_owner_digest_and_tool_mismatches_are_nondisclosing_absence() {
    run_async(async move {
        let mut owner_harness = ServiceHarness::command();
        let owner_result = execute_journaled(&owner_harness, 0x71).await;
        let wrong_owner = locator_with_segment(
            owner_result.outcome_locator(),
            0,
            "different-outcome-owner".to_owned(),
        );
        let owner_calls = owner_harness.policy.calls();
        let (context, _cancellation) = owner_harness.context(0x72);
        let owner_resolution = owner_harness
            .service
            .resolve_command_outcome(context, ResolveCommandOutcomeRequest::locator(wrong_owner))
            .await
            .expect("a locator-owner mismatch is existence-blind absence");
        assert_eq!(owner_resolution, ResolveCommandOutcomeResult::NotFound);
        assert_eq!(owner_harness.policy.calls() - owner_calls, 2);
        assert_eq!(owner_harness.ports.outcome_reservations(), 1);
        assert_eq!(owner_harness.ports.outcome_submissions(), 0);
        owner_harness.stop_coordinator();

        let mut digest_harness = ServiceHarness::command();
        let digest_result = execute_journaled(&digest_harness, 0x73).await;
        digest_harness.set_actual_outcome(&digest_result);
        let digest_text = digest_result
            .outcome_locator()
            .canonical_uri()
            .rsplit('/')
            .next()
            .expect("digest segment");
        let mut wrong_digest_text = digest_text.to_owned();
        let replacement = if wrong_digest_text.ends_with('A') {
            'Q'
        } else {
            'A'
        };
        wrong_digest_text.pop();
        wrong_digest_text.push(replacement);
        let wrong_digest =
            locator_with_segment(digest_result.outcome_locator(), 4, wrong_digest_text);
        let digest_calls = digest_harness.policy.calls();
        let (context, _cancellation) = digest_harness.context(0x74);
        let digest_resolution = digest_harness
            .service
            .resolve_command_outcome(context, ResolveCommandOutcomeRequest::locator(wrong_digest))
            .await
            .expect("a locator-digest mismatch is existence-blind absence");
        assert_eq!(digest_resolution, ResolveCommandOutcomeResult::NotFound);
        assert_eq!(digest_harness.policy.calls() - digest_calls, 2);
        assert_eq!(digest_harness.ports.outcome_submissions(), 1);
        digest_harness.stop_coordinator();

        let mut tool_harness = ServiceHarness::command();
        let tool_result = execute_journaled(&tool_harness, 0x75).await;
        tool_harness.set_actual_outcome(&tool_result);
        let tool_name = tool_result
            .outcome_locator()
            .tool_name()
            .rsplit_once('_')
            .map(|(prefix, _)| format!("{prefix}_other"))
            .expect("compiler tool-name segments");
        let wrong_tool = locator_with_segment(tool_result.outcome_locator(), 3, tool_name);
        let tool_calls = tool_harness.policy.calls();
        let (context, _cancellation) = tool_harness.context(0x76);
        let tool_resolution = tool_harness
            .service
            .resolve_command_outcome(context, ResolveCommandOutcomeRequest::locator(wrong_tool))
            .await
            .expect("a locator-tool mismatch is existence-blind absence");
        assert_eq!(tool_resolution, ResolveCommandOutcomeResult::NotFound);
        assert_eq!(tool_harness.policy.calls() - tool_calls, 2);
        assert_eq!(tool_harness.ports.outcome_submissions(), 1);
        tool_harness.stop_coordinator();
    });
}

#[test]
fn outcome_storage_failure_fails_closed_without_a_disclosure_check() {
    run_async(async move {
        let mut harness = ServiceHarness::command();
        harness.set_outcome_storage_failure();
        let (context, _cancellation) = harness.context(0x52);

        let failure = harness
            .service
            .resolve_command_outcome(context, harness.resolve_command_outcome_request())
            .await
            .expect_err("storage failure cannot be reported as absence");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        assert_eq!(harness.policy.calls(), 2);
        assert_eq!(harness.ports.outcome_submissions(), 1);
        harness.stop_coordinator();
        assert!(harness.audit_phases(0x52).is_empty());
    });
}

#[test]
fn policy_revocation_between_outcome_checks_withholds_the_present_result() {
    run_async(async move {
        const COMMAND_REQUEST: u8 = 0x53;
        const RESOLVE_REQUEST: u8 = 0x54;
        let mut harness = ServiceHarness::command();
        let committed = execute_journaled(&harness, COMMAND_REQUEST).await;
        let policy_calls_before_resolution = harness.policy.calls();
        harness.set_actual_outcome(&committed);
        harness.revoke_on_outcome_submission();
        let (context, _cancellation) = harness.context(RESOLVE_REQUEST);

        let failure = harness
            .service
            .resolve_command_outcome(context, harness.resolve_command_outcome_request())
            .await
            .expect_err("revoked current authority cannot disclose a stored outcome");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(
            harness.policy.calls() - policy_calls_before_resolution,
            3,
            "initial, post-capacity pre-lookup, and post-lookup checks are distinct"
        );
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(RESOLVE_REQUEST),
            [ServiceAuditPhaseV1::Denied]
        );
    });
}

#[test]
fn wrong_stored_owner_and_tenant_are_internal_integrity_failures() {
    run_async(async move {
        let mut owner_harness = ServiceHarness::command();
        let owner_result = execute_journaled(&owner_harness, 0x55).await;
        owner_harness.set_outcome_with_wrong_owner(&owner_result);
        let owner_calls = owner_harness.policy.calls();
        let (owner_context, _cancellation) = owner_harness.context(0x56);
        let owner_failure = owner_harness
            .service
            .resolve_command_outcome(
                owner_context,
                owner_harness.resolve_command_outcome_request(),
            )
            .await
            .expect_err("a lower outcome cannot substitute its owner");
        assert_internal_with_incident(&owner_failure);
        assert_eq!(owner_harness.policy.calls() - owner_calls, 2);
        owner_harness.stop_coordinator();
        assert!(owner_harness.audit_phases(0x56).is_empty());

        let mut tenant_harness = ServiceHarness::command();
        let tenant_result = execute_journaled(&tenant_harness, 0x57).await;
        tenant_harness.set_outcome_with_wrong_tenant(&tenant_result);
        let tenant_calls = tenant_harness.policy.calls();
        let (tenant_context, _cancellation) = tenant_harness.context(0x58);
        let tenant_failure = tenant_harness
            .service
            .resolve_command_outcome(
                tenant_context,
                tenant_harness.resolve_command_outcome_request(),
            )
            .await
            .expect_err("a lower outcome cannot substitute its tenant");
        assert_internal_with_incident(&tenant_failure);
        assert_eq!(tenant_harness.policy.calls() - tenant_calls, 2);
        tenant_harness.stop_coordinator();
        assert!(tenant_harness.audit_phases(0x58).is_empty());
    });
}

#[test]
fn wrong_stored_partition_is_denied_by_the_full_disclosure_check() {
    run_async(async move {
        const RESOLVE_REQUEST: u8 = 0x5a;
        let mut harness = ServiceHarness::command();
        let committed = execute_journaled(&harness, 0x59).await;
        harness.set_outcome_with_wrong_partition(&committed);
        let policy_calls = harness.policy.calls();
        let (context, _cancellation) = harness.context(RESOLVE_REQUEST);

        let failure = harness
            .service
            .resolve_command_outcome(context, harness.resolve_command_outcome_request())
            .await
            .expect_err("partition authority must cover the exact stored partition");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert_eq!(harness.policy.calls() - policy_calls, 3);
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(RESOLVE_REQUEST),
            [ServiceAuditPhaseV1::Denied]
        );
    });
}

#[test]
fn incident_source_failure_withholds_a_wrong_owner_outcome_without_fabrication() {
    run_async(async move {
        const RESOLVE_REQUEST: u8 = 0x5c;
        let mut harness = ServiceHarness::command_with_failing_incident_source();
        let committed = execute_journaled(&harness, 0x5b).await;
        harness.set_outcome_with_wrong_owner(&committed);
        let (context, _cancellation) = harness.context(RESOLVE_REQUEST);

        let failure = harness
            .service
            .resolve_command_outcome(context, harness.resolve_command_outcome_request())
            .await
            .expect_err("protected outcome remains withheld without an incident ID");

        assert!(matches!(failure, ServiceFailure::EmergencyInternal(_)));
        assert!(failure.public_error().is_none());
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::Integrity)
        );
        harness.stop_coordinator();
        assert!(harness.audit_phases(RESOLVE_REQUEST).is_empty());
    });
}

fn assert_internal_with_incident(failure: &ServiceFailure) {
    let public = failure
        .public_error()
        .expect("ordinary internal containment has a public error");
    assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
    assert!(public.incident_id().is_some());
}
