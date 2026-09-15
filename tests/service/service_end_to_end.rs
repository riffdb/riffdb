#![forbid(unsafe_code)]

//! Concrete in-process application-service integration evidence.

mod follower_refusals;
mod support;

use riffdb_errors::{ApplicationErrorCode, PublicErrorKind};
use riffdb_policy::PartitionConstraint;
use riffdb_service::{
    AdministrationApplication, AuthoritativeOutcomeSelectorRef, AuthoritativeReadinessFailure,
    CapacityRejectionStage, CommandApplication, CommandDurability, CommitApplication,
    CompactResourceDescriptorRef, ContractApplication, ContractValidationResult,
    CreateCapabilityInvocation, DescribeEventRequest, DescribeEventResult,
    DiscoverCommandToolsRequest, DiscoverCommandToolsResultRef, DiscoverResourcesRequest,
    DiscoverResourcesResultRef, DiscoveryApplication, DiscoveryCatalogStateRef,
    DiscoveryRepresentation, EventPartitionComponent, EventSelection, EventServiceApplication,
    ExecuteCommandResult, ExplainCommandResult, GetActiveContractRequest, GetActiveContractResult,
    GetCommitRequest, GetCommitResult, GetContractVersionResult, GetEntityResult,
    GetProjectionStatusResult, HealthContext, HealthRequest, HealthResult, HealthStatus,
    JournaledCompletion, PageLimit, PageRequest, PreBootstrapLifecycle, QueryApplication,
    ReplayEventsRequest, ResolveCommandOutcomeRequest, ResolveCommandOutcomeResult,
    ResourceDescriptorRef, ResourceDiscoveryKind, ServiceTelemetryEvent, StatisticsRequest,
    TraceProvenanceResult,
};
use riffdb_types::{
    CanonicalValue, PartitionScopeV1, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
};

use support::{ReadCommitMode, ServiceHarness, run_async, sequence};

// Covers: EVT-006, EVT-007, EVT-008.
#[test]
fn event_catalog_and_replay_use_current_operator_authority_and_symbolic_resolution() {
    run_async(async move {
        const DESCRIBE_SEED: u8 = 0x21;
        const REPLAY_SEED: u8 = 0x22;
        let mut harness = ServiceHarness::operations();

        let (context, _cancellation) = harness.context(DESCRIBE_SEED);
        let described = harness
            .service
            .describe_event(
                context,
                DescribeEventRequest::new("BudgetAllocated".to_owned())
                    .expect("bounded event symbol"),
            )
            .await
            .expect("ReadContract permits active event inspection");
        let DescribeEventResult::Found(descriptor) = described else {
            panic!("the active budget event is described symbolically");
        };
        assert_eq!(descriptor.event_name(), "BudgetAllocated");
        assert!(!descriptor.application_streamable());
        assert!(descriptor.partition_fields().is_empty());
        assert!(
            descriptor
                .payload_fields()
                .iter()
                .any(|field| field.name() == "organization_id")
        );

        let selection = EventSelection::new(
            "BudgetAllocated".to_owned(),
            vec![
                EventPartitionComponent::new(
                    "organization_id".to_owned(),
                    CanonicalValue::Uuid([0x21; 16]),
                )
                .expect("bounded partition component"),
            ],
            vec!["matter_id".to_owned()],
        )
        .expect("bounded symbolic event selection");
        let request = ReplayEventsRequest::new(
            selection,
            None,
            PageRequest::new(PageLimit::default(), None),
        )
        .expect("bounded replay request");
        let (context, _cancellation) = harness.context(REPLAY_SEED);
        let failure = harness
            .service
            .replay_events(context, request)
            .await
            .expect_err("an unpartitioned event has no application replay fallback");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Validation)
        );

        harness.stop_coordinator();
        assert!(
            harness.audit_records(DESCRIBE_SEED).is_empty(),
            "ReadContract metadata inspection retains its existing unaudited posture"
        );
        let records = harness.audit_records(REPLAY_SEED);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Started);
        assert_eq!(records[1].phase(), ServiceAuditPhaseV1::Failed);
        assert!(
            records
                .iter()
                .all(|record| record.operation() == ServiceOperationV1::ReplayEvents)
        );
    });
}

// Covers: EVT-007.
#[test]
fn event_description_without_read_contract_fails_before_catalog_disclosure() {
    run_async(async move {
        let mut harness = ServiceHarness::command();
        let (context, _cancellation) = harness.context(0x23);
        let failure = harness
            .service
            .describe_event(
                context,
                DescribeEventRequest::new("BudgetAllocated".to_owned())
                    .expect("bounded event symbol"),
            )
            .await
            .expect_err("command-only authority cannot inspect the event catalog");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        harness.stop_coordinator();
        let records = harness.audit_records(0x23);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation(), ServiceOperationV1::DescribeEvent);
        assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Denied);
    });
}

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

/// Constructed saturation: hold the sole workload permit outside the service
/// path, prove the channel is full, then probe with a generous request deadline.
/// Rejection is state-driven (try_reserve fails → bounded admission wait caps as
/// Overloaded), never a wall-clock race against catalog prep.
#[test]
fn saturated_command_capacity_rejects_before_reauthorization_with_typed_overload() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_one();
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "capacity-one harness must report full after one hold"
        );

        // Generous deadline: catalog prep must not race; overload comes from
        // the held-full channel, not from Instant::now() + few-ms budgets.
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

/// Primary ADR-0071 saturation evidence: capacity N with 4N concurrent in-flight
/// commands against a constructed full channel. Every failure is typed Overloaded;
/// no probe uses a tight wall-clock deadline that can expire during catalog prep.
#[test]
fn capacity_n_concurrent_commands_yield_only_typed_overload_or_commit() {
    run_async(async move {
        const N: u16 = 2;
        const IN_FLIGHT: usize = 8; // 4N
        let mut harness = ServiceHarness::command_capacity_n(N);
        let held = harness.hold_command_capacity_n(usize::from(N));
        assert!(
            harness.try_command_capacity_is_full(),
            "all N workload slots must be held before probes"
        );

        let service = harness.service.clone();
        let mut joins = Vec::with_capacity(IN_FLIGHT);
        for i in 0..IN_FLIGHT {
            // Shared generous deadline budget so prep never races admission.
            let (context, _cancel) = harness.context(0xB0_u8.wrapping_add(i as u8));
            let request = harness.execute_command_request_with_key(&format!("sat-{i}"));
            let service = service.clone();
            joins.push(tokio::spawn(async move {
                service.execute_command(context, request).await
            }));
        }

        let mut overloaded = 0u32;
        let mut success = 0u32;
        let mut other = 0u32;
        for join in joins {
            match join.await.expect("join") {
                Ok(ExecuteCommandResult::Journaled(result)) => {
                    assert_eq!(result.completion(), JournaledCompletion::Committed);
                    success = success.saturating_add(1);
                }
                Ok(_) => other = other.saturating_add(1),
                Err(failure) => {
                    if failure.public_error().map(|e| e.kind()) == Some(PublicErrorKind::Overloaded)
                    {
                        overloaded = overloaded.saturating_add(1);
                    } else {
                        other = other.saturating_add(1);
                    }
                }
            }
        }
        assert_eq!(success, 0, "held-full channel admits no commits");
        assert_eq!(other, 0, "no unclassified outcomes: other={other}");
        assert_eq!(overloaded, IN_FLIGHT as u32);

        drop(held);
        // After release, a command commits.
        let (context, _c) = harness.context(0xC0);
        let ok = harness
            .service
            .execute_command(context, harness.execute_command_request_with_key("sat-ok"))
            .await
            .expect("released capacity commits");
        assert!(matches!(ok, ExecuteCommandResult::Journaled(_)));
        harness.stop_coordinator();
    });
}

/// Constructed retained-byte exhaustion: hold the entire independent byte budget,
/// then probe with a generous deadline. Rejection is state-driven (bytes full),
/// not a short Instant budget racing prep.
#[test]
fn retained_bytes_exhaustion_rejects_with_capacity_stage() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_n(4);
        let _bytes = harness.hold_all_retained_bytes();

        let (context, _c) = harness.context(0xD1);
        let failure = harness
            .service
            .execute_command(
                context,
                harness.execute_command_request_with_key("bytes-full"),
            )
            .await
            .expect_err("retained-byte exhaustion is overload");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::Overloaded)
        );
        assert!(
            harness.telemetry.events().iter().any(|event| matches!(
                event,
                ServiceTelemetryEvent::CapacityRejected {
                    stage: CapacityRejectionStage::RetainedBytes,
                    ..
                }
            )),
            "CapacityRejected RetainedBytes must be recorded"
        );
        harness.stop_coordinator();
    });
}

/// I4 / M6: read-only capacity rejection is typed overload and writes no audit
/// (admission precedes begin_invocation). Saturation is constructed by holding
/// the sole workload permit before the probe.
#[test]
fn read_only_capacity_rejects_before_audit_start_with_typed_overload() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xE1;
        let mut harness = ServiceHarness::command_capacity_n_with_observe(1);
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "observe harness must be full before the read-only probe"
        );
        let (context, _c) = harness.context(REQUEST_SEED);
        let failure = harness
            .service
            .execute_command(context, harness.observe_budget_request())
            .await
            .expect_err("read-only capacity rejection");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::Overloaded)
        );
        assert_eq!(
            failure
                .public_error()
                .map(|error| ApplicationErrorCode::from_public_kind(error.kind()).as_str()),
            Some("RDB-CAPACITY-0101")
        );
        assert!(
            harness.telemetry.events().iter().any(|event| matches!(
                event,
                ServiceTelemetryEvent::CapacityRejected {
                    stage: CapacityRejectionStage::QueueDepth,
                    ..
                }
            )),
            "read-only capacity must record CapacityRejected QueueDepth"
        );
        drop(held);
        harness.stop_coordinator();
        assert!(
            harness.audit_records(REQUEST_SEED).is_empty(),
            "pre-audit capacity rejection must leave no Started/Failed pair"
        );
    });
}

/// Queue-delay shed under constructed saturation: hold the sole slot so
/// try_reserve fails, then force an EWMA estimate larger than any remaining
/// client budget under a generous request deadline. Shed is state-driven
/// (estimate > remaining − MIN_REMAINING), not a few-ms Instant race.
#[test]
fn queue_delay_shed_rejects_before_admission_with_zero_durable_audit_rows() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xE5;
        // Larger than any remaining-after-floor for harness.context() (30s).
        const FORCED_QUEUE_DELAY_MICROS: u64 = 60_000_000;
        let mut harness = ServiceHarness::command_capacity_one();
        harness.force_queue_delay_estimate_micros(FORCED_QUEUE_DELAY_MICROS);
        assert_eq!(
            harness.queue_delay_estimate_micros(),
            FORCED_QUEUE_DELAY_MICROS
        );
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "queue-delay shed requires a full channel so try_reserve fails first"
        );
        let (context, _c) = harness.context(REQUEST_SEED);
        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("queue-delay shed must reject");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::Overloaded)
        );
        assert!(
            harness.telemetry.events().iter().any(|event| matches!(
                event,
                ServiceTelemetryEvent::CapacityRejected {
                    stage: CapacityRejectionStage::QueueDepth,
                    ..
                }
            )),
            "queue-delay shed must record CapacityRejected QueueDepth"
        );
        // Shed-distinguishing observable. Overloaded + QueueDepth + zero audit
        // rows are identical whether the shed fires or the request falls through
        // to the bounded admission wait and its cap expires. Only the shed
        // returns before any admission wait is ever registered.
        assert_eq!(
            harness.admission_wait_registrations(),
            0,
            "the pre-admission shed must reject before registering an admission wait"
        );
        drop(held);
        harness.stop_coordinator();
        assert!(
            harness.audit_records(REQUEST_SEED).is_empty(),
            "pre-admission queue-delay shed must leave zero durable audit rows"
        );
    });
}

#[test]
fn stale_zero_queue_delay_estimate_never_sheds() {
    run_async(async move {
        // With a stale-zero estimate the shed branch is skipped. Capacity is
        // free and the request deadline is generous, so the command commits.
        let mut harness = ServiceHarness::command();
        assert_eq!(
            harness.queue_delay_estimate_micros(),
            0,
            "writer must start with a stale-zero estimate"
        );
        let (context, _c) = harness.context(0xE6);
        let ok = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect("stale-zero estimate must not pre-shed an otherwise-admissible command");
        assert!(matches!(ok, ExecuteCommandResult::Journaled(_)));
        harness.stop_coordinator();
    });
}

/// I1: capacity overload settles without audit; a later success still audits.
/// Non-capacity admission failures must not use the capacity settle path
/// (`is_capacity_overload` is Overloaded-only — covered in unit tests).
/// Saturation is constructed by holding the sole workload permit.
#[test]
fn capacity_overload_mutation_leaves_no_terminal_audit_row() {
    run_async(async move {
        const REQUEST_SEED: u8 = 0xE2;
        let mut harness = ServiceHarness::command_capacity_one();
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "mutation overload probe requires a full channel"
        );
        let (context, _c) = harness.context(REQUEST_SEED);
        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("capacity rejection");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::Overloaded)
        );
        drop(held);
        harness.stop_coordinator();
        assert!(
            harness.audit_records(REQUEST_SEED).is_empty(),
            "capacity settle must not append Started/Failed (ADR-0071 capacity-only skip)"
        );
    });
}

/// While the channel is held full, the probe parks on admission until the
/// absolute admission cap elapses. That unadmitted cap is typed Overloaded
/// (RDB-CAPACITY-0101), never details-free DeadlineExceeded — even though the
/// client request deadline remains far in the future.
///
/// The sibling sub-MIN_REMAINING immediate-reject path (a client budget too
/// small to host the bounded wait at all) is covered at unit level in
/// riffdb-service's `admission_budget_*` tests, because reaching it end to end
/// requires a near-expired deadline racing catalog preparation.
#[test]
fn admission_deadline_while_queued_is_overloaded_not_deadline_exceeded() {
    run_async(async move {
        let mut harness = ServiceHarness::command_capacity_one();
        let held = harness.hold_command_capacity();
        assert!(
            harness.try_command_capacity_is_full(),
            "admission-cap overload requires a constructed full channel"
        );

        // Generous client deadline so catalog prep cannot surface DeadlineExceeded
        // before admission. Overload is the admission wait cap against a held
        // permit, not Instant::now() + 10ms racing the prep path.
        let (context, _cancellation) = harness.context(0xA2);
        let failure = harness
            .service
            .execute_command(context, harness.execute_command_request())
            .await
            .expect_err("queued-unadmitted admission cap maps to overload");

        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::Overloaded),
            "must not surface details-free DeadlineExceeded while unadmitted"
        );
        assert_ne!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::InternalDefect)
        );
        assert!(
            harness.telemetry.events().iter().any(|event| matches!(
                event,
                ServiceTelemetryEvent::CapacityRejected {
                    stage: CapacityRejectionStage::QueueDepth,
                    ..
                }
            )),
            "admission-cap overload must record CapacityRejected QueueDepth"
        );
        // Positive control for the shed test's zero-registration assertion: this
        // probe does park on the bounded admission wait, so the observable is
        // not vacuously zero.
        assert!(
            harness.admission_wait_registrations() >= 1,
            "parking on the admission cap must register a bounded admission wait"
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
        assert!(
            harness.try_command_capacity_is_full(),
            "cancellation-during-admission requires a full channel so the probe parks"
        );

        let (context, cancellation) = harness.context(0xA3);
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

// req: OQ-113
#[test]
fn mcp_tool_discovery_omits_reimport_commands_without_hiding_application_commands() {
    run_async(async move {
        let mut harness = ServiceHarness::reimport_discovery();

        let (tools_context, _cancellation) = harness.context(0x76);
        let tools = harness
            .service
            .discover_command_tools(tools_context, DiscoverCommandToolsRequest::default())
            .await
            .expect("a deliberately non-MCP reimport command cannot corrupt tool discovery");
        let DiscoverCommandToolsResultRef::Page { page: tools, .. } = tools.result() else {
            panic!("default command discovery must return a full page");
        };
        let mut command_names = tools
            .items()
            .iter()
            .filter_map(|item| match item {
                riffdb_service::CommandToolDiscoveryItem::Command(command) => {
                    Some(command.source_command().as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        command_names.sort_unstable();
        assert_eq!(command_names, ["CreateBudget"]);

        harness.stop_coordinator();
    });
}

// req: OQ-113
#[test]
fn mcp_resource_discovery_omits_reimport_commands_without_hiding_application_commands() {
    run_async(async move {
        let mut harness = ServiceHarness::reimport_discovery();

        let (resources_context, _cancellation) = harness.context(0x77);
        let resources = harness
            .service
            .discover_resources(resources_context, DiscoverResourcesRequest::default())
            .await
            .expect("a deliberately non-MCP reimport command cannot corrupt resource discovery");
        let DiscoverResourcesResultRef::Page(resources) = resources.result() else {
            panic!("default resource discovery must return a full page");
        };
        let mut source_command_resources = resources
            .items()
            .iter()
            .filter_map(|item| match item.resource() {
                ResourceDescriptorRef::CommandPlan { source_command, .. }
                | ResourceDescriptorRef::CommandDocumentation { source_command, .. } => {
                    Some(source_command.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        source_command_resources.sort_unstable();
        assert_eq!(
            source_command_resources,
            [
                "AllocateBudget",
                "AllocateBudget",
                "CreateBudget",
                "CreateBudget"
            ],
            "only ordinary application commands own plans and documentation"
        );
        assert_eq!(
            resources
                .items()
                .iter()
                .filter(|item| matches!(
                    item.resource(),
                    ResourceDescriptorRef::CommandOutcome { .. }
                ))
                .count(),
            1,
            "policy exposes only the authorized application's outcome template"
        );

        harness.stop_coordinator();
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
fn compact_tool_and_resource_discovery_share_the_active_query_module_fence() {
    run_async(async move {
        let (mut harness, _module_hash) = named_query_harness(None);
        let compact_page = PageRequest::new(PageLimit::default(), None);

        let (tools_context, _cancellation) = harness.context(0x8e);
        let tools = harness
            .service
            .discover_command_tools(
                tools_context,
                DiscoverCommandToolsRequest::with_options(
                    compact_page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                )
                .expect("valid compact tool discovery"),
            )
            .await
            .expect("compact tools");
        let DiscoverCommandToolsResultRef::CompactPage(tools) = tools.result() else {
            panic!("initial compact tool discovery returns a page");
        };

        let (resources_context, _cancellation) = harness.context(0x8f);
        let resources = harness
            .service
            .discover_resources(
                resources_context,
                DiscoverResourcesRequest::with_options(
                    compact_page,
                    DiscoveryRepresentation::CompactObservation,
                    None,
                    ResourceDiscoveryKind::All,
                )
                .expect("valid compact resource discovery"),
            )
            .await
            .expect("compact resources");
        let DiscoverResourcesResultRef::CompactPage(resources) = resources.result() else {
            panic!("initial compact resource discovery returns a page");
        };

        assert_eq!(
            tools.observed_fence(),
            resources.observed_fence(),
            "one active catalog must have one observer fence across inventories"
        );
        harness.stop_coordinator();
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
            let mut membership = [0_usize; 11];
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
                        CompactResourceDescriptorRef::ReactiveWakeup => 10,
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
                    ResourceDiscoveryKind::All => [1, 1, 1, 501, 501, 501, 1, 1, 1, 1, 0],
                    ResourceDiscoveryKind::Concrete => [1, 1, 1, 501, 501, 0, 0, 0, 1, 1, 0],
                    ResourceDiscoveryKind::Template => [0, 0, 0, 0, 0, 501, 1, 1, 0, 0, 0],
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

#[test]
fn operational_named_query_selects_presence_members_and_records_read_stages() {
    use std::sync::Arc;

    use riffdb_errors::IncidentIdSource;
    use riffdb_observability::Observability;
    use riffdb_query_module::{
        NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_service::{
        NamedSymbolicQueryRequest, QueryModuleReadPort, ReadPipelineStage, ServiceTelemetry,
        SymbolicContractSelector, SymbolicQueryApplication, SymbolicQueryParameters,
    };
    use riffdb_types::{CapabilityPermissionV1, QueryOperationName};
    use support::{EmptyQueryExecutor, FixedQueryModulePort, ServiceHarness};

    const GET_BUDGET: &str = r#"
query GetBudget(
    $organization_id: Budget.organization_id,
    $fiscal_year: Budget.fiscal_year,
    $minimum_fiscal_year: Budget.fiscal_year?,
) {
    one budget from Budget
        where organization_id == $organization_id
            && fiscal_year == $fiscal_year
            && when $minimum_fiscal_year { fiscal_year >= $minimum_fiscal_year }
        else NotFound

    return Found {
        budget: budget {
            organization_id
            fiscal_year
            approved_amount
            allocated_amount
            updated_at
        }
    }

    outcomes Found | NotFound
}
"#;

    const BUDGET_SUMMARY: &str = r#"
query BudgetSummary(
    $organization_id: Budget.organization_id,
    $fiscal_year: Budget.fiscal_year,
) {
    many budgets from Budget
        where organization_id == $organization_id
          && fiscal_year == $fiscal_year
        order by fiscal_year asc
        take 5

    aggregate summary from budgets {
        count() as budget_count
        sum(approved_amount) as approved_total
    }

    return Found { summary: summary { budget_count approved_total } }
    outcomes Found
}
"#;

    run_async(async move {
        // Seed harness supplies the exact active catalog the named query will bind to.
        let seed = ServiceHarness::operations();
        let validated = seed.active_validated_bundle();
        let aggregate_document =
            riffdb_riffql_syntax::parse_query(BUDGET_SUMMARY).expect("aggregate query syntax");
        let aggregate_catalog = riffdb_query_ir::SymbolicCatalog::from_bundle(validated.bundle())
            .expect("aggregate catalog");
        riffdb_query_compiler::compile_operational_query_family(
            &aggregate_document,
            &aggregate_catalog,
        )
        .expect("aggregate query plan");
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("budget_reads").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![
                NamedQuerySource::new("BudgetSummary", BUDGET_SUMMARY).expect("aggregate source"),
                NamedQuerySource::new("GetBudget", GET_BUDGET).expect("query source"),
            ],
        )
        .expect("module candidate");
        let module =
            riffdb_catalog::ValidatedQueryModule::compile(candidate, &validated).expect("module");
        let module_hash = module.identity();
        let family_hash = module
            .module()
            .query("GetBudget")
            .expect("query")
            .plan()
            .identity();
        let query_name = QueryOperationName::new("GetBudget").expect("query name");
        let named_permission = CapabilityPermissionV1::ExecuteNamedQuery(
            validated.lineage().clone(),
            module_hash,
            query_name,
        );
        let summary_permission = CapabilityPermissionV1::ExecuteNamedQuery(
            validated.lineage().clone(),
            module_hash,
            QueryOperationName::new("BudgetSummary").expect("summary name"),
        );

        struct FixedIncidents;
        impl IncidentIdSource for FixedIncidents {
            fn next_incident_id(
                &self,
            ) -> Result<riffdb_types::IncidentId, riffdb_errors::IncidentIdSourceError>
            {
                riffdb_types::IncidentId::from_bytes([0xab; 16])
                    .map_err(|_| riffdb_errors::IncidentIdSourceError)
            }
        }
        let observability = Arc::new(
            Observability::new(Arc::new(FixedIncidents), 32).expect("bounded observability"),
        );
        let modules = Arc::new(FixedQueryModulePort::new(module)) as Arc<dyn QueryModuleReadPort>;
        let executor = Arc::new(EmptyQueryExecutor);
        let harness = ServiceHarness::operations_with_read_stage_telemetry(
            Arc::clone(&observability) as Arc<dyn ServiceTelemetry>,
            executor,
            modules,
            vec![named_permission, summary_permission],
        );

        let (context, _cancellation) = harness.context(0x91);
        let mut values = std::collections::BTreeMap::new();
        values.insert(
            "organization_id".to_owned(),
            riffdb_service::SubmittedValue::Uuid([0x31; 16]),
        );
        values.insert(
            "fiscal_year".to_owned(),
            riffdb_service::SubmittedValue::I64(2026),
        );
        let request = NamedSymbolicQueryRequest::new(
            SymbolicContractSelector::active(),
            "GetBudget".to_owned(),
            Some(module_hash),
            SymbolicQueryParameters::new(values.clone()).expect("parameters"),
        )
        .expect("named request");

        let result = harness
            .service
            .execute_named_symbolic_query(context, request)
            .await
            .expect("absent optional member succeeds through empty executor");
        assert_eq!(result.outcome(), "NotFound");
        assert_eq!(result.identity().plan_hash(), family_hash);

        values.insert(
            "minimum_fiscal_year".to_owned(),
            riffdb_service::SubmittedValue::I64(2020),
        );
        let request = NamedSymbolicQueryRequest::new(
            SymbolicContractSelector::active(),
            "GetBudget".to_owned(),
            Some(module_hash),
            SymbolicQueryParameters::new(values).expect("parameters"),
        )
        .expect("named request");
        let (present_context, _present_cancellation) = harness.context(0x92);
        let result = harness
            .service
            .execute_named_symbolic_query(present_context, request)
            .await
            .expect("present optional member succeeds through empty executor");
        assert_eq!(result.outcome(), "NotFound");
        assert_eq!(result.identity().plan_hash(), family_hash);

        let summary_request = NamedSymbolicQueryRequest::new(
            SymbolicContractSelector::active(),
            "BudgetSummary".to_owned(),
            Some(module_hash),
            SymbolicQueryParameters::new(std::collections::BTreeMap::from([
                (
                    "organization_id".to_owned(),
                    riffdb_service::SubmittedValue::Uuid([0x31; 16]),
                ),
                (
                    "fiscal_year".to_owned(),
                    riffdb_service::SubmittedValue::I64(2026),
                ),
            ]))
            .expect("summary parameters"),
        )
        .expect("summary request");
        let (summary_context, _summary_cancellation) = harness.context(0x93);
        let summary = harness
            .service
            .execute_named_symbolic_query(summary_context, summary_request)
            .await
            .expect("deployed aggregate executes through the application service");
        let Some(riffdb_service::SymbolicResultField::One(record)) =
            summary.fields().get("summary")
        else {
            panic!("whole-set aggregate record missing: {:?}", summary.fields());
        };
        assert_eq!(
            record.fields().get("budget_count"),
            Some(&riffdb_types::CanonicalValue::U64(0))
        );
        let approved_total = record
            .exact_decimals()
            .get("approved_total")
            .expect("full-width sum carriage");
        assert_eq!(approved_total.coefficient(), 0);
        assert_eq!(approved_total.scale(), 2);

        let queries = 3_u64;
        for stage in [
            ReadPipelineStage::SpawnDispatch,
            ReadPipelineStage::PlanLookup,
            ReadPipelineStage::Execute,
            ReadPipelineStage::ResponseBuild,
        ] {
            let snapshot = observability.metrics().read_stage_duration(stage);
            assert!(
                snapshot.count >= 1,
                "expected {stage:?} count >= 1, got {}",
                snapshot.count
            );
        }
        let authorize_total = [
            ReadPipelineStage::AuthorizeBegin,
            ReadPipelineStage::AuthorizePre,
            ReadPipelineStage::AuthorizePost,
        ]
        .into_iter()
        .map(|stage| observability.metrics().read_stage_duration(stage).count)
        .sum::<u64>();
        assert_eq!(
            authorize_total,
            3 * queries,
            "authorize stages should fire once each per successful query"
        );
        for stage in [
            ReadPipelineStage::ParamMaterialize,
            ReadPipelineStage::AuthorizeBegin,
            ReadPipelineStage::AuthorizePre,
            ReadPipelineStage::AuthorizePost,
            // The audit finish that closes the invocation used to sit outside
            // every stage, so its cost landed in the unattributed remainder.
            ReadPipelineStage::AuditFinish,
        ] {
            assert_eq!(
                observability.metrics().read_stage_duration(stage).count,
                queries,
                "{stage:?} should observe exactly once per query"
            );
        }
    });
}

#[test]
fn live_named_query_closes_snapshot_catchup_reconnect_and_revocation() {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use riffdb_catalog::{ValidatedQueryModule, ValidatedReactiveModule};
    use riffdb_query_module::{
        NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_service::{
        AuthoritativeCommitNotification, LiveNamedQueryApplication, LiveNamedQuerySelection,
        LiveQueryUpdate, QueryModuleReadPort, QueryParameters, ReactiveModuleReadPort,
        WatchLiveNamedQueryRequest,
    };
    use riffdb_types::{
        CanonicalValue, CapabilityPermissionV1, CommitSequence, ReactiveOperationName,
    };
    use support::{
        EmptyQueryExecutor, FixedLiveQueryClock, FixedQueryModulePort, FixedReactiveModulePort,
        ServiceHarness,
    };

    const REACTIVE: &str = r#"
reactive BudgetLive version 1 {
  watch BudgetWatch($fiscal_year: Budget.fiscal_year, $organization_id: Budget.organization_id) query GetBudget updates reset;
}
"#;

    run_async(async move {
        let seed = ServiceHarness::operations();
        let contract = seed.active_validated_bundle();
        let query_module = ValidatedQueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("budget_live").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("GetBudget", GET_BUDGET_QUERY).expect("query source")],
            )
            .expect("module candidate"),
            &contract,
        )
        .expect("validated query module");
        let reactive_module = ValidatedReactiveModule::compile(
            REACTIVE,
            &contract,
            std::slice::from_ref(&query_module),
        )
        .expect("validated reactive module");
        let reactive_hash = reactive_module.identity();
        let operation_name = ReactiveOperationName::new("BudgetWatch").expect("operation name");
        let permission = CapabilityPermissionV1::WatchNamedQuery(
            contract.lineage().clone(),
            reactive_hash,
            operation_name.clone(),
        );
        drop(seed);

        let harness = ServiceHarness::live_named_queries(
            Arc::new(EmptyQueryExecutor),
            Arc::new(FixedQueryModulePort::new(query_module)) as Arc<dyn QueryModuleReadPort>,
            Arc::new(FixedReactiveModulePort::new(reactive_module))
                as Arc<dyn ReactiveModuleReadPort>,
            Arc::new(FixedLiveQueryClock),
            vec![permission],
        );
        let first = CommitSequence::first();
        harness.set_read_commit_snapshot(harness.commit_snapshot(first));
        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            true,
        );
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("fiscal_year".to_owned(), CanonicalValue::I64(2026)),
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid([0x31; 16]),
            ),
        ]))
        .expect("canonical parameters");
        let selection = LiveNamedQuerySelection::new(reactive_hash, operation_name, parameters);

        let (context, _cancellation) = harness.context(0xb1);
        let service = harness.service.clone();
        let first_selection = selection.clone();
        let establishing = tokio::spawn(async move {
            service
                .watch_live_named_query(context, WatchLiveNamedQueryRequest::new(first_selection))
                .await
        });
        harness.wait_for_commit_subscription_submission().await;
        harness.release_commit_subscription_source();
        let result = establishing
            .await
            .expect("establishment task")
            .expect("watch establishes");
        let mut subscription = result.into_subscription();
        let snapshot_cursor = match subscription.next().await.expect("initial snapshot") {
            LiveQueryUpdate::Snapshot(snapshot) => snapshot.cursor().clone(),
            other => panic!("expected snapshot, got {other:?}"),
        };
        match subscription.next().await.expect("racing commit update") {
            LiveQueryUpdate::Checkpoint(checkpoint) => {
                assert_eq!(checkpoint.frontier().application_head(), first.get());
            }
            other => panic!("expected checkpoint, got {other:?}"),
        }
        assert_eq!(harness.commit_subscription_acknowledgements(), [first]);
        drop(subscription);

        harness.configure_commit_subscription(Vec::new(), false);
        let (context, _cancellation) = harness.context(0xb2);
        let resumed = harness
            .service
            .watch_live_named_query(
                context,
                WatchLiveNamedQueryRequest::new(selection.clone()).with_cursor(snapshot_cursor),
            )
            .await
            .expect("resume establishes");
        let mut resumed = resumed.into_subscription();
        assert!(matches!(
            resumed.next().await.expect("resumed snapshot"),
            LiveQueryUpdate::Snapshot(_)
        ));
        drop(resumed);

        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            false,
        );
        let (context, _cancellation) = harness.context(0xb3);
        let revoked = harness
            .service
            .watch_live_named_query(context, WatchLiveNamedQueryRequest::new(selection))
            .await
            .expect("revocation watch establishes");
        let mut revoked = revoked.into_subscription();
        assert!(matches!(
            revoked.next().await.expect("initial before revocation"),
            LiveQueryUpdate::Snapshot(_)
        ));
        harness.deny_after_next_policy_allows(1);
        assert!(matches!(
            revoked.next().await.expect("typed revocation terminal"),
            LiveQueryUpdate::Terminal(terminal)
                if terminal.reason() == riffdb_service::LiveQueryTerminalReason::AuthorizationChanged
        ));
        assert!(harness.commit_subscription_source_dropped());
    });
}

#[test]
fn live_named_query_accepts_a_bounded_top_n_engine_continuation() {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use riffdb_catalog::{ValidatedQueryModule, ValidatedReactiveModule};
    use riffdb_query_module::{
        NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_service::{
        AuthoritativeCommitNotification, LiveNamedQueryApplication, LiveNamedQuerySelection,
        LiveQueryUpdate, QueryModuleReadPort, QueryParameters, ReactiveModuleReadPort,
        WatchLiveNamedQueryRequest,
    };
    use riffdb_types::{
        CanonicalValue, CapabilityPermissionV1, CommitSequence, ReactiveOperationName,
    };
    use support::{
        ContinuedEmptyQueryExecutor, FixedLiveQueryClock, FixedQueryModulePort,
        FixedReactiveModulePort, ServiceHarness,
    };

    const LIST_BUDGETS: &str = r#"
query ListBudgets(
    $organization_id: Budget.organization_id,
    $fiscal_year: Budget.fiscal_year,
) {
    many budgets from Budget
        where organization_id == $organization_id
            && fiscal_year == $fiscal_year
        order by fiscal_year asc
        take 5

    return Found {
        budgets: budgets {
            organization_id
            fiscal_year
            approved_amount
        }
    }

    outcomes Found
}
"#;
    const REACTIVE: &str = r#"
reactive ContinuedBudgetLive version 1 {
  watch ContinuedBudgetWatch($fiscal_year: Budget.fiscal_year, $organization_id: Budget.organization_id) query ListBudgets updates reset;
}
"#;

    run_async(async move {
        let seed = ServiceHarness::operations();
        let contract = seed.active_validated_bundle();
        let query_module = ValidatedQueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("continued_budget_live").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("ListBudgets", LIST_BUDGETS).expect("query source")],
            )
            .expect("module candidate"),
            &contract,
        )
        .expect("validated query module");
        let reactive_module = ValidatedReactiveModule::compile(
            REACTIVE,
            &contract,
            std::slice::from_ref(&query_module),
        )
        .expect("validated reactive module");
        let reactive_hash = reactive_module.identity();
        let operation_name =
            ReactiveOperationName::new("ContinuedBudgetWatch").expect("operation name");
        let permission = CapabilityPermissionV1::WatchNamedQuery(
            contract.lineage().clone(),
            reactive_hash,
            operation_name.clone(),
        );
        drop(seed);

        let harness = ServiceHarness::live_named_queries(
            Arc::new(ContinuedEmptyQueryExecutor::default()),
            Arc::new(FixedQueryModulePort::new(query_module)) as Arc<dyn QueryModuleReadPort>,
            Arc::new(FixedReactiveModulePort::new(reactive_module))
                as Arc<dyn ReactiveModuleReadPort>,
            Arc::new(FixedLiveQueryClock),
            vec![permission],
        );
        let first = CommitSequence::first();
        harness.set_read_commit_snapshot(harness.commit_snapshot(first));
        harness.configure_commit_subscription(
            vec![AuthoritativeCommitNotification::Advanced(first)],
            false,
        );
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("fiscal_year".to_owned(), CanonicalValue::I64(2026)),
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid([0x31; 16]),
            ),
        ]))
        .expect("canonical parameters");
        let selection = LiveNamedQuerySelection::new(reactive_hash, operation_name, parameters);
        let (context, _cancellation) = harness.context(0xb4);
        let result = harness
            .service
            .watch_live_named_query(context, WatchLiveNamedQueryRequest::new(selection))
            .await
            .expect("bounded top-N watch establishes");
        let mut subscription = result.into_subscription();
        assert!(matches!(
            subscription.next().await.expect("initial top-N snapshot"),
            LiveQueryUpdate::Snapshot(_)
        ));
        assert!(matches!(
            subscription
                .next()
                .await
                .expect("continued top-N re-execution"),
            LiveQueryUpdate::Checkpoint(_)
        ));
    });
}

/// Source and permission fixture shared by the read-path safe-point tests.
const GET_BUDGET_QUERY: &str = r#"
query GetBudget(
    $organization_id: Budget.organization_id,
    $fiscal_year: Budget.fiscal_year,
) {
    one budget from Budget
        where organization_id == $organization_id
            && fiscal_year == $fiscal_year
        else NotFound

    return Found {
        budget: budget {
            organization_id
            fiscal_year
            approved_amount
            allocated_amount
            updated_at
        }
    }

    outcomes Found | NotFound
}
"#;

/// Builds an operations harness that can execute the named `GetBudget` query.
fn named_query_harness(
    telemetry: Option<std::sync::Arc<dyn riffdb_service::ServiceTelemetry>>,
) -> (support::ServiceHarness, riffdb_types::QueryModuleHash) {
    use std::sync::Arc;

    use riffdb_query_module::{
        NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_service::{NoopServiceTelemetry, QueryModuleReadPort, ServiceTelemetry};
    use riffdb_types::{CapabilityPermissionV1, QueryOperationName};
    use support::{EmptyQueryExecutor, FixedQueryModulePort, ServiceHarness};

    let seed = ServiceHarness::operations();
    let validated = seed.active_validated_bundle();
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("budget_reads").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("GetBudget", GET_BUDGET_QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = riffdb_catalog::ValidatedQueryModule::compile(candidate, &validated)
        .expect("named query module compiles");
    let module_hash = module.identity();
    let named_permission = CapabilityPermissionV1::ExecuteNamedQuery(
        validated.lineage().clone(),
        module_hash,
        QueryOperationName::new("GetBudget").expect("query name"),
    );
    let harness = ServiceHarness::operations_with_read_stage_telemetry(
        telemetry.unwrap_or_else(|| Arc::new(NoopServiceTelemetry) as Arc<dyn ServiceTelemetry>),
        Arc::new(EmptyQueryExecutor),
        Arc::new(FixedQueryModulePort::new(module)) as Arc<dyn QueryModuleReadPort>,
        vec![named_permission],
    );
    (harness, module_hash)
}

fn named_budget_request(
    module_hash: riffdb_types::QueryModuleHash,
) -> riffdb_service::NamedSymbolicQueryRequest {
    use riffdb_service::{
        NamedSymbolicQueryRequest, SymbolicContractSelector, SymbolicQueryParameters,
    };

    let mut values = std::collections::BTreeMap::new();
    values.insert(
        "organization_id".to_owned(),
        riffdb_service::SubmittedValue::Uuid([0x31; 16]),
    );
    values.insert(
        "fiscal_year".to_owned(),
        riffdb_service::SubmittedValue::I64(2026),
    );
    NamedSymbolicQueryRequest::new(
        SymbolicContractSelector::active(),
        "GetBudget".to_owned(),
        Some(module_hash),
        SymbolicQueryParameters::new(values).expect("parameters"),
    )
    .expect("named request")
}

// req: DEP-001
#[test]
fn admission_head_is_captured_after_authorization_only_for_stronger_first_pages() {
    use riffdb_errors::ApplicationErrorCode;
    use riffdb_service::SymbolicQueryApplication;
    use riffdb_types::CommitSequence;

    run_async(async move {
        let (harness, module_hash) = named_query_harness(None);

        let (default_context, _default_cancellation) = harness.context(0xc1);
        harness
            .service
            .execute_named_symbolic_query(default_context, named_budget_request(module_hash))
            .await
            .expect("legacy query behavior is unchanged");
        assert_eq!(harness.application_head_observations(), 0);

        let order_start = harness.ports.capability_order().len();
        let (fenced_context, _fenced_cancellation) = harness.context(0xc2);
        harness
            .service
            .execute_named_symbolic_query(
                fenced_context,
                named_budget_request(module_hash).with_admission_head_consistency(),
            )
            .await
            .expect("empty authoritative head is already satisfied");
        assert_eq!(harness.application_head_observations(), 1);
        let order = harness.ports.capability_order();
        let invocation_order = &order[order_start..];
        let head = invocation_order
            .iter()
            .position(|stage| *stage == "application_head")
            .expect("head observation recorded");
        let authorization = invocation_order
            .iter()
            .rposition(|stage| *stage == "policy")
            .expect("initial authorization recorded");
        assert!(
            authorization < head,
            "authorization must precede head capture"
        );

        harness.set_application_head(Some(CommitSequence::first()));
        let (behind_context, _behind_cancellation) = harness.context(0xc3);
        let failure = harness
            .service
            .execute_named_symbolic_query(
                behind_context,
                named_budget_request(module_hash).with_admission_head_consistency(),
            )
            .await
            .expect_err("a lower snapshot must never be served");
        assert_eq!(
            failure
                .public_error()
                .and_then(|error| error.application_code_hint()),
            Some(ApplicationErrorCode::FreshnessUnsatisfied)
        );
        assert_eq!(harness.application_head_observations(), 2);
        if let Some(root) = std::env::var_os("RIFFDB_WP754_FIXTURE_OUTPUT") {
            let code = failure
                .public_error()
                .and_then(|error| error.application_code_hint())
                .expect("typed freshness refusal")
                .as_str();
            let mut bytes = b"riffdb.query-pre-inversion/admission-head-v1\0".to_vec();
            bytes.extend_from_slice(&2_u64.to_be_bytes());
            bytes.push(u8::from(authorization < head));
            bytes.extend_from_slice(
                &u32::try_from(code.len())
                    .expect("bounded application code")
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(code.as_bytes());
            let path = std::path::PathBuf::from(root).join("admission-head-page-and-refusal.bin");
            std::fs::create_dir_all(path.parent().expect("fixture parent"))
                .expect("fixture directory");
            std::fs::write(path, bytes).expect("write admission-head fixture");
        }
    });
}

/// Revoking between begin and recheck must fail the read closed.
///
/// The harness never hand-sets a generation. `revoke()` applies the real
/// active-to-revoked record transition to the capability the production
/// `CurrentAuthorizer` resolves, and the capability-view generation this
/// policy reports is derived from that transition. Read safe point 2 therefore
/// sees a moved generation, declines the revision-checked reissue, and the
/// mandatory full re-evaluation denies.
///
/// Falsifiability: neuter the generation comparison in
/// `AuthorizedOperation::reissue_for_unchanged_view` (drop the
/// `observed.generation() != baseline_generation` disjunct) and this test fails
/// — the revoked read succeeds.
#[test]
fn named_read_fails_closed_when_the_capability_is_revoked_between_begin_and_recheck() {
    use riffdb_service::SymbolicQueryApplication;

    run_async(async move {
        let (harness, module_hash) = named_query_harness(None);
        // Revoke immediately after the begin safe point's allow.
        harness.deny_after_next_policy_allows(1);

        let (context, _cancellation) = harness.context(0x92);
        let failure = harness
            .service
            .execute_named_symbolic_query(context, named_budget_request(module_hash))
            .await
            .expect_err("a revoked capability must fail closed at a read safe point");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied),
            "post-revoke recheck must deny, got {failure:?}"
        );
        assert_eq!(
            harness.policy.calls(),
            2,
            "the recheck must fall through to a full evaluation after the revoke"
        );
    });
}

/// Expiry between begin and recheck must fail the read closed.
///
/// Nothing is published, so the capability-view generation does not move. Only
/// the clock advances past the capability's `expires_at`. The retained validity
/// window must reject the reissue and the full re-evaluation must deny.
///
/// Falsifiability: neuter the time comparison in
/// `AuthorizedOperation::reissue_for_unchanged_view` (drop the
/// `!self.identity.validity.admits(observed.now())` disjunct) and this test
/// fails — the expired read succeeds.
#[test]
fn named_read_fails_closed_when_the_capability_expires_between_begin_and_recheck() {
    use riffdb_service::SymbolicQueryApplication;

    run_async(async move {
        let (harness, module_hash) = named_query_harness(None);
        let generation_at_begin = {
            use riffdb_service::CurrentPolicyPort;
            harness
                .policy
                .capability_view_generation()
                .expect("harness publishes a generation")
        };
        // The harness capability is valid for BASE_SECONDS..BASE_SECONDS+1_000.
        // Step past that boundary right after the begin safe point's allow.
        harness.advance_policy_clock_after_next_allows(1, support::BASE_SECONDS + 5_000);

        let (context, _cancellation) = harness.context(0x94);
        let failure = harness
            .service
            .execute_named_symbolic_query(context, named_budget_request(module_hash))
            .await
            .expect_err("an expired capability must fail closed at a read safe point");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied),
            "post-expiry recheck must deny, got {failure:?}"
        );
        assert_eq!(
            harness.policy.calls(),
            2,
            "the recheck must fall through to a full evaluation once the window closed"
        );
        let generation_after = {
            use riffdb_service::CurrentPolicyPort;
            harness
                .policy
                .capability_view_generation()
                .expect("harness publishes a generation")
        };
        assert_eq!(
            generation_at_begin, generation_after,
            "expiry is a clock event, not a view mutation: the generation must not move"
        );
    });
}

/// An unchanged world reissues at both read safe points.
///
/// One full evaluation at begin, then safe points 2 and 3 reissue. The outcome
/// is identical to pre-R2 semantics; only the cost changes.
#[test]
fn unchanged_view_reissues_the_begin_proof_at_both_read_safe_points() {
    use riffdb_service::SymbolicQueryApplication;

    run_async(async move {
        let (harness, module_hash) = named_query_harness(None);
        let (context, _cancellation) = harness.context(0x95);
        let result = harness
            .service
            .execute_named_symbolic_query(context, named_budget_request(module_hash))
            .await
            .expect("named query succeeds through the empty executor");
        assert_eq!(result.outcome(), "NotFound");
        assert_eq!(
            harness.policy.calls(),
            1,
            "an unchanged view must cost exactly one full evaluation for three safe points"
        );
    });
}

/// The revision-checked shortcut is scoped to the read pipeline.
///
/// A contract read reauthorizes through the shared entry point, which always
/// re-evaluates in full even though the capability view never moved. This is
/// the assertion that fails if the shortcut is ever hoisted back into
/// `BegunInvocation::reauthorize`, where commits and administration would
/// inherit it — including the mandatory recheck after a capacity wait.
#[test]
fn non_read_reauthorization_always_evaluates_in_full_on_an_unchanged_view() {
    use riffdb_service::GetActiveContractRequest;

    run_async(async move {
        let harness = support::ServiceHarness::operations();
        let (context, _cancellation) = harness.context(0x96);
        let _active = harness
            .service
            .get_active_contract(context, GetActiveContractRequest)
            .await
            .expect("active contract read succeeds");
        assert_eq!(
            harness.policy.calls(),
            3,
            "every safe point outside the read pipeline stays a full evaluation"
        );
    });
}

/// A divergent reauthorization target never reuses the begin proof.
#[test]
fn a_divergent_request_is_never_reissued_from_an_unchanged_view() {
    use riffdb_policy::{CapabilityViewCheckpoint, Decision, OperationRequest};
    use riffdb_service::CurrentPolicyPort;

    let harness = support::ServiceHarness::operations();
    let baseline = harness
        .policy
        .capability_view_generation()
        .expect("harness publishes a generation");
    let authorized = OperationRequest::get_active_contract();
    let Ok(Decision::Allow(proof)) = harness
        .policy
        .authorize(&harness.policy.principal(), authorized.clone())
    else {
        panic!("the harness capability may read the active contract");
    };
    let observed = CapabilityViewCheckpoint::new(baseline, harness.policy.now());

    assert!(
        proof
            .reissue_for_unchanged_view(baseline, observed, &authorized)
            .is_some(),
        "the authorized request reissues on an unchanged view"
    );
    let divergent = OperationRequest::get_contract_version(
        riffdb_types::ContractLineage::new("other_lineage").expect("lineage"),
        riffdb_types::ContractVersion::new(7).expect("version"),
    );
    assert!(
        proof
            .reissue_for_unchanged_view(baseline, observed, &divergent)
            .is_none(),
        "a different target must force a full evaluation even on an unchanged view"
    );
}

/// Perf smoke (non-gating, report-only): mean plan_lookup and authorize stage times.
#[test]
fn read_stage_perf_smoke_reports_plan_lookup_and_authorize_means() {
    use std::sync::Arc;

    use riffdb_service::{ReadPipelineStage, ServiceTelemetry, SymbolicQueryApplication};

    run_async(async move {
        struct FixedIncidents;
        impl riffdb_errors::IncidentIdSource for FixedIncidents {
            fn next_incident_id(
                &self,
            ) -> Result<riffdb_types::IncidentId, riffdb_errors::IncidentIdSourceError>
            {
                riffdb_types::IncidentId::from_bytes([0xcd; 16])
                    .map_err(|_| riffdb_errors::IncidentIdSourceError)
            }
        }
        let observability = Arc::new(
            riffdb_observability::Observability::new(Arc::new(FixedIncidents), 32)
                .expect("bounded observability"),
        );
        let (harness, module_hash) =
            named_query_harness(Some(Arc::clone(&observability) as Arc<dyn ServiceTelemetry>));

        const ITERATIONS: u64 = 32;
        for index in 0..ITERATIONS {
            let (context, _cancellation) = harness.context(0xa0 + (index as u8));
            let _result = harness
                .service
                .execute_named_symbolic_query(context, named_budget_request(module_hash))
                .await
                .expect("named query succeeds");
        }

        let mean = |snapshot: riffdb_observability::HistogramSnapshot| -> f64 {
            if snapshot.count == 0 {
                0.0
            } else {
                snapshot.sum as f64 / snapshot.count as f64
            }
        };
        let stage = |stage| observability.metrics().read_stage_duration(stage);
        eprintln!(
            "perf-smoke plan_lookup mean_us={:.2} (n={}); authorize_begin mean_us={:.2}; authorize_pre mean_us={:.2}; authorize_post mean_us={:.2}",
            mean(stage(ReadPipelineStage::PlanLookup)),
            stage(ReadPipelineStage::PlanLookup).count,
            mean(stage(ReadPipelineStage::AuthorizeBegin)),
            mean(stage(ReadPipelineStage::AuthorizePre)),
            mean(stage(ReadPipelineStage::AuthorizePost)),
        );
        for recorded in [
            ReadPipelineStage::PlanLookup,
            ReadPipelineStage::AuthorizeBegin,
            ReadPipelineStage::AuthorizePre,
            ReadPipelineStage::AuthorizePost,
        ] {
            assert!(
                stage(recorded).count >= ITERATIONS,
                "every read safe point must still record its stage"
            );
        }
        assert_eq!(
            harness.policy.calls() as u64,
            ITERATIONS,
            "each unchanged-view read costs exactly one full evaluation"
        );
    });
}
