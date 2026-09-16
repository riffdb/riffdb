//! Follower service composition has no source writer capabilities.
// req: REP-002, PERF-007
use super::support::{ServiceHarness, run_async};
use riffdb_errors::PublicErrorKind;
use riffdb_service::{
    AdministrationApplication, CommandApplication, ContractApplication, CreateCapabilityInvocation,
    QueryApplication, ServiceFailure,
};

fn refused<T>(result: Result<T, ServiceFailure>) {
    let Err(failure) = result else {
        panic!("follower request unexpectedly succeeded");
    };
    assert_eq!(
        failure.public_error().unwrap().kind(),
        PublicErrorKind::FollowerMode
    );
}

#[test]
fn follower_service_refuses_commands_deployment_and_capabilities_without_writer_admission() {
    run_async(async {
        let mut harness = ServiceHarness::operations();
        let follower = harness.follower_service();
        for seed in 0x30..0x33 {
            let (context, _) = harness.context(seed);
            refused(
                follower
                    .execute_command(context, harness.execute_command_request())
                    .await,
            );
            let (context, _) = harness.context(seed);
            refused(
                follower
                    .deploy_contract(context, harness.deploy_request())
                    .await,
            );
            let (context, _) = harness.context(seed);
            refused(
                follower
                    .create_capability(CreateCapabilityInvocation::Normal {
                        context,
                        request: harness.create_capability_request(),
                    })
                    .await,
            );
            let (context, _) = harness.context(seed);
            refused(
                follower
                    .revoke_capability(context, harness.revoke_request())
                    .await,
            );
        }
        assert_eq!(
            harness.policy.calls(),
            0,
            "role refusal precedes authorization and audit admission"
        );
        assert_eq!(harness.audit_submission_count(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_service_keeps_current_policy_for_an_allowed_standard_read() {
    run_async(async {
        let mut harness = ServiceHarness::operations();
        let follower = harness.follower_service();
        let (context, _) = harness.context(0x35);
        follower
            .get_entity(context, harness.entity_request())
            .await
            .unwrap();
        assert!(harness.policy.calls() > 0);
        assert_eq!(harness.audit_submission_count(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_service_refuses_maintenance_migration_installation_export_and_reimport_surfaces() {
    use riffdb_service::*;
    use riffdb_types::*;
    run_async(async {
        let mut harness = ServiceHarness::operations();
        let follower = harness.follower_service();
        let maintenance =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [3; 10]).unwrap();
        let name = BackupNameV1::new("bounded-backup").unwrap();
        let (context, _) = harness.context(0x41);
        refused(
            follower
                .create_offline_backup(
                    context,
                    CreateOfflineBackupRequest::new(maintenance, name.clone()).unwrap(),
                )
                .await,
        );
        let (context, _) = harness.context(0x40);
        refused(
            follower
                .restore_archived_backup(RestoreArchivedBackupInvocation::new(
                    context,
                    RestoreArchivedBackupRequest::new(
                        maintenance,
                        name.clone(),
                        ArchiveNameV1::new("daily").unwrap(),
                        ArchiveRestoreStopV1::LastArchived,
                        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
                    )
                    .unwrap(),
                    riffdb_auth::RetainedOpaqueCredential::new(b"private-presentation").unwrap(),
                ))
                .await,
        );
        let (context, _) = harness.context(0x42);
        refused(
            follower
                .retire_offline_backup(
                    context,
                    RetireOfflineBackupRequest::new(maintenance, name).unwrap(),
                )
                .await,
        );
        let (context, _) = harness.context(0x43);
        refused(
            follower
                .get_offline_maintenance_operation(
                    context,
                    GetOfflineMaintenanceOperationRequest::new(maintenance).unwrap(),
                )
                .await,
        );
        let migration =
            ContractMigrationOperationId::from_unix_milliseconds_and_random(1, [4; 10]).unwrap();
        let (context, _) = harness.context(0x44);
        refused(
            follower
                .get_contract_migration_operation(
                    context,
                    GetContractMigrationOperationRequest::new(migration),
                )
                .await,
        );
        let campaign =
            ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [5; 10])
                .unwrap();
        let (context, _) = harness.context(0x45);
        refused(
            follower
                .get_application_installation(
                    context,
                    GetApplicationInstallationRequest::new(campaign),
                )
                .await,
        );
        let export =
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [6; 10]).unwrap();
        let (context, _) = harness.context(0x46);
        let request_id = context.request_id();
        let selection = ApplicationExportSelectionV1::new(
            ContractLineage::new("example").unwrap(),
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            false,
            false,
            false,
        )
        .unwrap();
        refused(
            follower
                .start_application_export(
                    context,
                    request_id,
                    StartApplicationExportRequest::new(
                        export,
                        selection,
                        MIN_APPLICATION_EXPORT_LEASE_SECONDS,
                    )
                    .unwrap(),
                )
                .await,
        );
        let (context, _) = harness.context(0x47);
        let request_id = context.request_id();
        refused(
            follower
                .get_application_export(
                    context,
                    request_id,
                    ApplicationExportOperationRequest::new(export),
                )
                .await,
        );
        let (context, _) = harness.context(0x48);
        let request_id = context.request_id();
        refused(
            follower
                .cancel_application_export(
                    context,
                    request_id,
                    ApplicationExportOperationRequest::new(export),
                )
                .await,
        );
        let (context, _) = harness.context(0x49);
        let request_id = context.request_id();
        refused(
            follower
                .get_application_reimport(
                    context,
                    request_id,
                    ApplicationReimportOperationRequestV1::new(campaign),
                )
                .await,
        );
        let (context, _) = harness.context(0x50);
        let request_id = context.request_id();
        refused(
            follower
                .cancel_application_reimport(
                    context,
                    request_id,
                    ApplicationReimportOperationRequestV1::new(campaign),
                )
                .await,
        );
        assert_eq!(harness.policy.calls(), 0);
        assert_eq!(harness.audit_submission_count(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_service_refuses_a_policy_required_audit_without_releasing_the_read() {
    use riffdb_service::{DiscoverResourcesRequest, DiscoveryApplication, PageLimit, PageRequest};
    run_async(async {
        let mut harness = ServiceHarness::operations();
        harness.audit_discovery_operations();
        let follower = harness.follower_service();
        let (context, _) = harness.context(0x51);
        refused(
            follower
                .discover_resources(
                    context,
                    DiscoverResourcesRequest::new(PageRequest::new(PageLimit::default(), None)),
                )
                .await,
        );
        assert!(
            harness.policy.calls() > 0,
            "current policy selected the required audit"
        );
        assert_eq!(harness.audit_submission_count(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_denials_remain_authorization_denials_without_audit_or_readiness_failure() {
    use riffdb_service::{
        HealthContext, HealthRequest, ServiceTelemetryEvent, ServiceTerminalClass,
        StatisticsRequest,
    };
    run_async(async {
        let mut harness = ServiceHarness::operations();
        harness.revoke_policy();
        let follower = harness.follower_service();
        for seed in 0x61..0x65 {
            let (context, _) = harness.context(seed);
            let error = follower
                .get_entity(context, harness.entity_request())
                .await
                .unwrap_err();
            assert_eq!(
                error.public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
            let (context, _) = harness.context(seed);
            let error = follower
                .statistics(context, StatisticsRequest)
                .await
                .unwrap_err();
            assert_eq!(
                error.public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
            let (context, _) = harness.context(seed);
            let error = follower
                .health(HealthContext::authenticated(context), HealthRequest)
                .await
                .unwrap_err();
            assert_eq!(
                error.public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
        }
        assert!(harness.ports.operation_calls().is_empty());
        assert_eq!(harness.audit_submission_count(), 0);
        assert!(harness.health.failures().is_empty());
        assert_eq!(
            harness
                .telemetry
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    ServiceTelemetryEvent::OperationTerminal {
                        terminal: ServiceTerminalClass::AuthorizationDenied,
                        ..
                    }
                ))
                .count(),
            12
        );
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_read_revocation_at_reauthorization_does_not_fence_the_applier() {
    for statistics in [false, true] {
        run_async(async move {
            let mut harness = ServiceHarness::operations();
            harness.deny_after_next_policy_allows(1);
            let follower = harness.follower_service();
            let (context, _) = harness.context(0x66);
            let error = if statistics {
                follower
                    .statistics(context, riffdb_service::StatisticsRequest)
                    .await
                    .unwrap_err()
            } else {
                follower
                    .get_entity(context, harness.entity_request())
                    .await
                    .unwrap_err()
            };
            assert_eq!(
                error.public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
            assert!(harness.policy.calls() >= 2);
            assert!(harness.health.failures().is_empty());
            assert_eq!(harness.audit_submission_count(), 0);
            drop(follower);
            harness.stop_coordinator();
            assert!(harness.all_audit_phases().is_empty());
        });
    }
}

#[test]
fn follower_authorized_operational_reads_use_telemetry_without_durable_audit() {
    use riffdb_service::{
        HealthContext, HealthRequest, HealthResult, ServiceTelemetryEvent, StatisticsRequest,
    };
    run_async(async {
        let mut harness = ServiceHarness::operations();
        let follower = harness.follower_service();
        let (context, _) = harness.context(0x67);
        let health = follower
            .health(HealthContext::authenticated(context), HealthRequest)
            .await
            .unwrap();
        assert!(matches!(health, HealthResult::Authenticated(_)));
        let (context, _) = harness.context(0x68);
        let statistics = follower
            .statistics(context, StatisticsRequest)
            .await
            .unwrap();
        assert!(harness.policy.calls() >= 4, "both reads reauthorize");
        assert_eq!(harness.audit_submission_count(), 0);
        assert!(harness.health.failures().is_empty());
        assert_eq!(
            harness
                .telemetry
                .events()
                .iter()
                .filter(|event| matches!(event, ServiceTelemetryEvent::OperationTerminal { .. }))
                .count(),
            2
        );
        assert_eq!(statistics.active_cursors(), 0);
        assert_eq!(statistics.active_commit_subscribers(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}

#[test]
fn follower_refuses_a_new_durable_audit_obligation_at_the_read_safe_point() {
    use riffdb_service::{DiscoverResourcesRequest, DiscoveryApplication, PageLimit, PageRequest};
    run_async(async {
        let mut harness = ServiceHarness::operations();
        harness.audit_discovery_on_catalog_reservation();
        let follower = harness.follower_service();
        let (context, _) = harness.context(0x69);
        refused(
            follower
                .discover_resources(
                    context,
                    DiscoverResourcesRequest::new(PageRequest::new(PageLimit::default(), None)),
                )
                .await,
        );
        assert!(
            harness.policy.calls() >= 2,
            "audit obligation changed after initial allow"
        );
        assert_eq!(harness.audit_submission_count(), 0);
        assert!(harness.health.failures().is_empty());
        assert_eq!(
            harness.ports.operation_calls(),
            Vec::<&str>::new(),
            "no provider was submitted"
        );
        drop(follower);
        harness.stop_coordinator();
        assert!(harness.all_audit_phases().is_empty());
    });
}
