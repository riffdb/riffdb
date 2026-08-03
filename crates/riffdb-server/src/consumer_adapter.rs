//! Production adapter for the service-owned durable consumer coordinator.

use riffdb_service::{
    BoxPortCapacityPermit, EventConsumerCheckpoint, EventConsumerLeaseValidation,
    EventConsumerMutationResult, EventConsumerPort, EventConsumerPortError,
    EventConsumerPortIdentity, EventConsumerPortLease, EventConsumerPortRequest,
    EventConsumerPortResponse, EventConsumerStatus, PortAdmissionError, PortFuture, RequestControl,
};
use riffdb_storage_api::{
    ConsumerCheckpointV1, CoordinateConsumerAcknowledgementV1, CoordinateConsumerLeaseV1,
    CoordinateConsumerNegativeAcknowledgementV1, CoordinatedConsumerLeaseValidationV1,
    CoordinatedConsumerStatusV1, EventConsumerIdentityV1, EventConsumerTransitionResultV1,
    StorageErrorKind, coordinate_consumer_acknowledgement, coordinate_consumer_lease,
    coordinate_consumer_lease_validation, coordinate_consumer_negative_acknowledgement,
    coordinate_consumer_retire, coordinate_consumer_seek, coordinate_consumer_status,
};
use riffdb_types::DatabaseId;

use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

/// Bounded blocking adapter over the one activated storage bundle.
pub(crate) struct ServerEventConsumerPort {
    executor: BlockingPortExecutor<
        EventConsumerPortRequest,
        EventConsumerPortResponse,
        EventConsumerPortError,
    >,
}

impl ServerEventConsumerPort {
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        database_id: DatabaseId,
        driver: &BlockingPortDriver,
    ) -> Self {
        let executor = driver
            .executor(move |request| coordinate_request(storage.clone(), database_id, request));
        Self { executor }
    }
}

impl EventConsumerPort for ServerEventConsumerPort {
    fn reserve_event_consumer<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            EventConsumerPortRequest,
            EventConsumerPortResponse,
            EventConsumerPortError,
        >,
        PortAdmissionError,
    > {
        Box::pin(self.executor.reserve_async(control))
    }
}

impl std::fmt::Debug for ServerEventConsumerPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ServerEventConsumerPort([REDACTED])")
    }
}

fn coordinate_request(
    mut storage: SharedRedbOperationalPorts,
    database_id: DatabaseId,
    request: EventConsumerPortRequest,
) -> Result<EventConsumerPortResponse, EventConsumerPortError> {
    match request {
        EventConsumerPortRequest::Inspect { identity } => {
            let identity = storage_identity(database_id, identity);
            coordinate_consumer_status(&storage, &identity)
                .map(|status| EventConsumerPortResponse::Status(status.map(service_status)))
                .map_err(map_storage)
        }
        EventConsumerPortRequest::ValidateLease {
            identity,
            partition_hash,
            event_id,
            attempt,
            token,
            history_incarnation,
            observed_at,
        } => coordinate_consumer_lease_validation(
            &storage,
            &storage_identity(database_id, identity),
            partition_hash,
            event_id,
            attempt,
            token,
            history_incarnation,
            observed_at,
        )
        .map(|result| {
            EventConsumerPortResponse::LeaseValidation(match result {
                CoordinatedConsumerLeaseValidationV1::Live => EventConsumerLeaseValidation::Live,
                CoordinatedConsumerLeaseValidationV1::NotFound => {
                    EventConsumerLeaseValidation::NotFound
                }
                CoordinatedConsumerLeaseValidationV1::Stale => EventConsumerLeaseValidation::Stale,
                CoordinatedConsumerLeaseValidationV1::Expired => {
                    EventConsumerLeaseValidation::Expired
                }
            })
        })
        .map_err(map_storage),
        EventConsumerPortRequest::Lease {
            identity,
            partition_hash,
            history_incarnation,
            observed_at,
            expires_at,
            selected_events,
            tokens,
            batch_limit,
            in_flight_limit,
        } => {
            let result = coordinate_consumer_lease(
                &mut storage,
                CoordinateConsumerLeaseV1 {
                    identity: storage_identity(database_id, identity),
                    partition_hash,
                    history_incarnation,
                    observed_at,
                    expires_at,
                    selected_events,
                    tokens,
                    batch_limit,
                    in_flight_limit,
                },
            )
            .map_err(map_storage)?;
            Ok(EventConsumerPortResponse::Leased {
                result: mutation_result(result.transition),
                leases: result
                    .leases
                    .into_iter()
                    .map(|lease| EventConsumerPortLease {
                        event_id: lease.event_id,
                        attempt: lease.attempt,
                        token: lease.token,
                        expires_at: lease.expires_at,
                    })
                    .collect(),
                status: result.status.map(service_status),
            })
        }
        EventConsumerPortRequest::Acknowledge {
            identity,
            event_id,
            token,
            history_incarnation,
            observed_at,
            selected_prefix,
        } => coordinate_consumer_acknowledgement(
            &mut storage,
            CoordinateConsumerAcknowledgementV1 {
                identity: storage_identity(database_id, identity),
                event_id,
                token,
                history_incarnation,
                observed_at,
                selected_prefix,
            },
        )
        .map(|result| EventConsumerPortResponse::Mutated(mutation_result(result)))
        .map_err(map_storage),
        EventConsumerPortRequest::NegativeAcknowledge {
            identity,
            event_id,
            token,
            observed_at,
            eligible_at,
            selected_prefix,
        } => coordinate_consumer_negative_acknowledgement(
            &mut storage,
            CoordinateConsumerNegativeAcknowledgementV1 {
                identity: storage_identity(database_id, identity),
                event_id,
                token,
                observed_at,
                eligible_at,
                selected_prefix,
            },
        )
        .map(|result| EventConsumerPortResponse::Mutated(mutation_result(result)))
        .map_err(map_storage),
        EventConsumerPortRequest::Seek {
            identity,
            checkpoint,
        } => coordinate_consumer_seek(
            &mut storage,
            &storage_identity(database_id, identity),
            storage_checkpoint(checkpoint),
        )
        .map(|result| EventConsumerPortResponse::Mutated(mutation_result(result)))
        .map_err(map_storage),
        EventConsumerPortRequest::Retire { identity } => {
            coordinate_consumer_retire(&mut storage, &storage_identity(database_id, identity))
                .map(|result| EventConsumerPortResponse::Mutated(mutation_result(result)))
                .map_err(map_storage)
        }
    }
}

fn storage_identity(
    database_id: DatabaseId,
    identity: EventConsumerPortIdentity,
) -> EventConsumerIdentityV1 {
    EventConsumerIdentityV1::new(
        database_id,
        identity.module_hash,
        identity.operation_name,
        identity.parameter_hash,
        identity.consumer_name,
    )
}

const fn storage_checkpoint(checkpoint: EventConsumerCheckpoint) -> ConsumerCheckpointV1 {
    match checkpoint {
        EventConsumerCheckpoint::BeforeFirst => ConsumerCheckpointV1::BeforeFirst,
        EventConsumerCheckpoint::After(event_id) => ConsumerCheckpointV1::After(event_id),
    }
}

fn service_status(status: CoordinatedConsumerStatusV1) -> EventConsumerStatus {
    EventConsumerStatus::new(
        status.revision,
        match status.checkpoint {
            ConsumerCheckpointV1::BeforeFirst => EventConsumerCheckpoint::BeforeFirst,
            ConsumerCheckpointV1::After(event_id) => EventConsumerCheckpoint::After(event_id),
        },
        status.history_incarnation,
        status.live_leases,
        status.retries,
        status.dead_letters,
    )
}

const fn mutation_result(result: EventConsumerTransitionResultV1) -> EventConsumerMutationResult {
    match result {
        EventConsumerTransitionResultV1::Applied => EventConsumerMutationResult::Applied,
        EventConsumerTransitionResultV1::StateChanged => EventConsumerMutationResult::StateChanged,
        EventConsumerTransitionResultV1::NotFound => EventConsumerMutationResult::NotFound,
        EventConsumerTransitionResultV1::OutstandingLease => {
            EventConsumerMutationResult::OutstandingLease
        }
        EventConsumerTransitionResultV1::StaleLease => EventConsumerMutationResult::StaleLease,
        EventConsumerTransitionResultV1::LeaseExpired => EventConsumerMutationResult::LeaseExpired,
    }
}

fn map_storage(error: riffdb_storage_api::StorageError) -> EventConsumerPortError {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            EventConsumerPortError::Unavailable
        }
        _ => EventConsumerPortError::Integrity,
    }
}
