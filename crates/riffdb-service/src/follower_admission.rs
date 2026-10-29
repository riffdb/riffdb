//! Closed admission at the shared service boundary, before any operation future
//! or durable audit starts. New operation variants require an explicit decision.
use super::*;

impl ServiceExecutors {
    pub(super) fn admit_operation(&self, operation: ServiceOperationV1) -> ServiceResult<()> {
        if self.is_follower() && requires_primary(operation) {
            return Err(PublicError::follower_mode().into());
        }
        Ok(())
    }
}

impl RiffDbServiceInner {
    pub(super) fn admit_operation(
        &self,
        operation: ServiceOperationV1,
        ingress: riffdb_types::ServiceIngressKindV1,
    ) -> ServiceResult<()> {
        self.executors.admit_operation(operation)?;
        if !operation_is_exempt_from_resource_limits(operation)
            && let Some(resource_admission) = &self.providers.resource_admission
            && let Err(error) = resource_admission.admit_operation(operation)
        {
            let failure = ServiceFailure::from(error);
            let result: ServiceResult<()> = Err(failure);
            self.providers
                .telemetry
                .record(ServiceTelemetryEvent::OperationTerminal {
                    operation,
                    ingress,
                    terminal: super::service_terminal_class(&result),
                    elapsed: std::time::Duration::ZERO,
                });
            return result;
        }
        Ok(())
    }
}

/// Returns whether an operation is exempt from tenant rate and storage ceilings.
///
/// Health and statistics probes remain available under tenant saturation so
/// operator supervision reflects process readiness rather than tenant traffic.
#[must_use]
pub const fn operation_is_exempt_from_resource_limits(operation: ServiceOperationV1) -> bool {
    matches!(
        operation,
        ServiceOperationV1::GetHealth | ServiceOperationV1::GetStatistics
    )
}

/// Returns whether an operation appends new authoritative application or schema state.
///
/// Reads, uncertainty recovery (`ResolveCommandOutcome`), application data
/// exports (`StartApplicationExport`, `GetApplicationExportPage`,
/// `GetApplicationExport`, `CancelApplicationExport`), capability revocation,
/// and consumer retirement never grow authoritative storage and must remain
/// admissible when a database reaches its `max_storage_bytes` ceiling.
#[must_use]
pub const fn operation_grows_authoritative_storage(operation: ServiceOperationV1) -> bool {
    use ServiceOperationV1::*;
    match operation {
        ExecuteCommand
        | DeployContract
        | DeployQueryModule
        | DeployReactiveModule
        | StartApplicationInstallation
        | ApplyContractMigration
        | StartApplicationReimport
        | ApplyApplicationReimportPage => true,
        ValidateContract
        | ExplainCommand
        | GetActiveContract
        | GetContractVersion
        | GetEntity
        | ScanIndex
        | QueryProjection
        | GetProjectionStatus
        | GetHealth
        | GetStatistics
        | DiscoverCommandTools
        | DiscoverResources
        | DescribeContract
        | CheckQuery
        | ExplainQuery
        | ExecuteQuery
        | DescribeEvent
        | ExecuteProjectedQuery
        | WatchNamedQuery
        | InspectVectorState
        | ResolveCommandOutcome
        | GetCommit
        | ScanCommits
        | SubscribeToCommits
        | TraceProvenance
        | CreateCapability
        | RevokeCapability
        | ListPendingOutboxDeliveries
        | ReplayEvents
        | TailEvents
        | ConsumeEventStream
        | AcknowledgeEventStream
        | NegativeAcknowledgeEventStream
        | SeekEventStreamConsumer
        | RetireEventStreamConsumer
        | GetEventStreamConsumerStatus
        | ConsumeContextualSubscription
        | AcknowledgeContextualSubscription
        | NegativeAcknowledgeContextualSubscription
        | GetContextualSubscriptionStatus
        | ExecuteContextualReaction
        | GetReactiveWakeup
        | GetApplicationInstallation
        | StartApplicationExport
        | GetApplicationExportPage
        | GetApplicationExport
        | CancelApplicationExport
        | GetApplicationReimport
        | CancelApplicationReimport
        | RegisterFollower
        | RetireFollower
        | FenceReplicationPrimary
        | StreamChangelog
        | PromoteFollower => false,
    }
}

/// Returns whether an operation charges the per-database write rate bucket
/// rather than the read/query rate bucket.
#[must_use]
pub const fn operation_uses_write_rate_bucket(operation: ServiceOperationV1) -> bool {
    use ServiceOperationV1::*;
    match operation {
        ExecuteCommand
        | DeployContract
        | DeployQueryModule
        | DeployReactiveModule
        | StartApplicationInstallation
        | ApplyContractMigration
        | StartApplicationReimport
        | ApplyApplicationReimportPage
        | CreateCapability
        | RevokeCapability
        | ConsumeEventStream
        | AcknowledgeEventStream
        | NegativeAcknowledgeEventStream
        | SeekEventStreamConsumer
        | RetireEventStreamConsumer
        | ConsumeContextualSubscription
        | AcknowledgeContextualSubscription
        | NegativeAcknowledgeContextualSubscription
        | ExecuteContextualReaction
        | CancelApplicationExport
        | CancelApplicationReimport
        | RegisterFollower
        | RetireFollower
        | FenceReplicationPrimary
        | PromoteFollower => true,
        ValidateContract
        | ExplainCommand
        | GetActiveContract
        | GetContractVersion
        | GetEntity
        | ScanIndex
        | QueryProjection
        | GetProjectionStatus
        | GetHealth
        | GetStatistics
        | DiscoverCommandTools
        | DiscoverResources
        | DescribeContract
        | CheckQuery
        | ExplainQuery
        | ExecuteQuery
        | DescribeEvent
        | ExecuteProjectedQuery
        | WatchNamedQuery
        | InspectVectorState
        | ResolveCommandOutcome
        | GetCommit
        | ScanCommits
        | SubscribeToCommits
        | TraceProvenance
        | ListPendingOutboxDeliveries
        | ReplayEvents
        | TailEvents
        | GetEventStreamConsumerStatus
        | GetContextualSubscriptionStatus
        | GetReactiveWakeup
        | GetApplicationInstallation
        | StartApplicationExport
        | GetApplicationExportPage
        | GetApplicationExport
        | GetApplicationReimport
        | StreamChangelog => false,
    }
}

fn requires_primary(operation: ServiceOperationV1) -> bool {
    use ServiceOperationV1::*;
    match operation {
        ValidateContract
        | ExplainCommand
        | GetActiveContract
        | GetContractVersion
        | GetEntity
        | ScanIndex
        | QueryProjection
        | GetProjectionStatus
        | GetHealth
        | GetStatistics
        | DiscoverCommandTools
        | DiscoverResources
        | DescribeContract
        | CheckQuery
        | ExplainQuery
        | ExecuteQuery
        | DescribeEvent
        | ExecuteProjectedQuery
        | WatchNamedQuery
        | InspectVectorState => false,
        // Commands include grammar read-only invocation and outcome resolution.
        // Administrative reads other than Health/Statistics require durable audit.
        DeployContract
        | ExecuteCommand
        | ResolveCommandOutcome
        | GetCommit
        | ScanCommits
        | SubscribeToCommits
        | TraceProvenance
        | CreateCapability
        | RevokeCapability
        | ListPendingOutboxDeliveries
        | DeployQueryModule
        | ApplyContractMigration
        | ReplayEvents
        | TailEvents
        | DeployReactiveModule
        | ConsumeEventStream
        | AcknowledgeEventStream
        | NegativeAcknowledgeEventStream
        | SeekEventStreamConsumer
        | RetireEventStreamConsumer
        | GetEventStreamConsumerStatus
        | ConsumeContextualSubscription
        | AcknowledgeContextualSubscription
        | NegativeAcknowledgeContextualSubscription
        | GetContextualSubscriptionStatus
        | ExecuteContextualReaction
        | GetReactiveWakeup
        | StartApplicationInstallation
        | GetApplicationInstallation
        | StartApplicationExport
        | GetApplicationExportPage
        | GetApplicationExport
        | CancelApplicationExport
        | StartApplicationReimport
        | ApplyApplicationReimportPage
        | GetApplicationReimport
        | CancelApplicationReimport
        | RegisterFollower
        | RetireFollower
        | FenceReplicationPrimary
        | StreamChangelog
        | PromoteFollower => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: REP-002, PERF-007
    #[test]
    fn follower_has_no_writer_and_rejects_every_intrinsically_mutating_or_audited_family() {
        use ServiceOperationV1::*;
        let follower = ServiceExecutors::follower();
        assert_eq!(
            follower
                .writer()
                .err()
                .unwrap()
                .public_error()
                .unwrap()
                .kind(),
            PublicErrorKind::FollowerMode
        );
        for operation in [
            ExecuteCommand,
            ResolveCommandOutcome,
            DeployContract,
            DeployQueryModule,
            DeployReactiveModule,
            CreateCapability,
            RevokeCapability,
            ApplyContractMigration,
            ConsumeEventStream,
            AcknowledgeEventStream,
            NegativeAcknowledgeEventStream,
            SeekEventStreamConsumer,
            RetireEventStreamConsumer,
            ConsumeContextualSubscription,
            AcknowledgeContextualSubscription,
            NegativeAcknowledgeContextualSubscription,
            ExecuteContextualReaction,
            StartApplicationInstallation,
            GetApplicationInstallation,
            StartApplicationExport,
            GetApplicationExportPage,
            GetApplicationExport,
            CancelApplicationExport,
            StartApplicationReimport,
            ApplyApplicationReimportPage,
            GetApplicationReimport,
            CancelApplicationReimport,
            RegisterFollower,
            RetireFollower,
            FenceReplicationPrimary,
            StreamChangelog,
            PromoteFollower,
        ] {
            assert_eq!(
                follower
                    .admit_operation(operation)
                    .unwrap_err()
                    .public_error()
                    .unwrap()
                    .kind(),
                PublicErrorKind::FollowerMode,
                "{operation:?}"
            );
        }
    }

    // req: NET-005, PERF-007
    #[test]
    fn storage_growth_and_rate_classification_preserve_reads_uncertainty_recovery_and_exports() {
        use ServiceOperationV1::*;
        assert!(operation_is_exempt_from_resource_limits(GetHealth));
        assert!(operation_is_exempt_from_resource_limits(GetStatistics));
        assert!(!operation_is_exempt_from_resource_limits(ExecuteCommand));
        assert!(!operation_is_exempt_from_resource_limits(ExecuteQuery));

        for growing in [
            ExecuteCommand,
            DeployContract,
            DeployQueryModule,
            DeployReactiveModule,
            StartApplicationInstallation,
            ApplyContractMigration,
            StartApplicationReimport,
            ApplyApplicationReimportPage,
        ] {
            assert!(
                operation_grows_authoritative_storage(growing),
                "{growing:?}"
            );
            assert!(operation_uses_write_rate_bucket(growing), "{growing:?}");
        }

        for non_growing_read_or_recovery in [
            ResolveCommandOutcome,
            GetEntity,
            ScanIndex,
            QueryProjection,
            ExecuteQuery,
            ExecuteProjectedQuery,
            StartApplicationExport,
            GetApplicationExportPage,
            GetApplicationExport,
            GetCommit,
            ScanCommits,
            ReplayEvents,
            TailEvents,
        ] {
            assert!(
                !operation_grows_authoritative_storage(non_growing_read_or_recovery),
                "{non_growing_read_or_recovery:?}"
            );
            assert!(
                !operation_uses_write_rate_bucket(non_growing_read_or_recovery),
                "{non_growing_read_or_recovery:?}"
            );
        }

        for non_growing_control in [
            RevokeCapability,
            RetireEventStreamConsumer,
            CancelApplicationExport,
            CancelApplicationReimport,
        ] {
            assert!(
                !operation_grows_authoritative_storage(non_growing_control),
                "{non_growing_control:?}"
            );
            assert!(
                operation_uses_write_rate_bucket(non_growing_control),
                "{non_growing_control:?}"
            );
        }
    }
}
