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
        | CancelApplicationReimport => true,
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
}
