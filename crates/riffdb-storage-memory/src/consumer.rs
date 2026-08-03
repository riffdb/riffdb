//! Atomic reference persistence for durable event-consumer transitions.

use riffdb_storage_api::{
    ConsumerStateError, EvaluatedEventConsumerTransitionV1, EventConsumerRepository,
    EventConsumerSnapshotV1, EventConsumerTransitionResultV1, EventConsumerTransitionV1,
    MAX_CONSUMER_DELIVERY_RECORDS, StorageError, StorageErrorKind, StoredEventConsumerDeliveryV1,
    evaluate_event_consumer_transition,
};
use riffdb_types::{EventConsumerIdentityHash, EventId};

use crate::state::MemoryState;
use crate::store::{MemoryOperationalPorts, storage_error};

impl EventConsumerRepository for MemoryOperationalPorts {
    fn inspect_event_consumer(
        &self,
        identity_hash: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
        self.read(|state| inspect(state, identity_hash))
    }

    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        self.apply_exclusive_mut(|state| {
            validate_all(state)?;
            let identity = transition.consumer_identity_hash();
            let evaluated =
                evaluate_event_consumer_transition(inspect(state, identity)?, transition)
                    .map_err(state_error)?;
            match evaluated {
                EvaluatedEventConsumerTransitionV1::NoChange(result) => Ok(result),
                EvaluatedEventConsumerTransitionV1::Replace(snapshot) => {
                    replace_snapshot(state, *snapshot)?;
                    Ok(EventConsumerTransitionResultV1::Applied)
                }
                EvaluatedEventConsumerTransitionV1::Retire(identity) => {
                    remove_snapshot(state, identity);
                    Ok(EventConsumerTransitionResultV1::Applied)
                }
            }
        })
    }

    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
        self.read(|state| {
            validate_all(state)?;
            (0..state.event_consumers.len())
                .map(|index| snapshot(state, index))
                .collect()
        })
    }

    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
        self.read(|state| {
            validate_all(state)?;
            Ok(state
                .event_consumers
                .iter()
                .map(|consumer| consumer.checkpoint().retention_frontier())
                .min())
        })
    }
}

fn inspect(
    state: &MemoryState,
    identity: EventConsumerIdentityHash,
) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
    let Ok(index) = consumer_position(state, identity) else {
        return Ok(None);
    };
    snapshot(state, index).map(Some)
}

fn snapshot(state: &MemoryState, index: usize) -> Result<EventConsumerSnapshotV1, StorageError> {
    let consumer = state.event_consumers[index].clone();
    let deliveries = state
        .event_consumer_deliveries
        .iter()
        .filter(|row| row.consumer_identity_hash() == consumer.identity().identity_hash())
        .cloned()
        .collect();
    EventConsumerSnapshotV1::new(consumer, deliveries).map_err(state_error)
}

fn replace_snapshot(
    state: &mut MemoryState,
    snapshot: EventConsumerSnapshotV1,
) -> Result<(), StorageError> {
    let identity = snapshot.consumer().identity().identity_hash();
    remove_snapshot(state, identity);
    let consumer_index = consumer_position(state, identity).unwrap_err();
    state
        .event_consumers
        .insert(consumer_index, snapshot.consumer().clone());
    for delivery in snapshot.deliveries().iter().cloned() {
        let delivery_index = match delivery_position(
            &state.event_consumer_deliveries,
            delivery.consumer_identity_hash(),
            delivery.event_id(),
        ) {
            Ok(_) => return Err(corrupt()),
            Err(index) => index,
        };
        state
            .event_consumer_deliveries
            .insert(delivery_index, delivery);
    }
    validate_all(state)
}

fn remove_snapshot(state: &mut MemoryState, identity: EventConsumerIdentityHash) {
    if let Ok(index) = consumer_position(state, identity) {
        state.event_consumers.remove(index);
    }
    state
        .event_consumer_deliveries
        .retain(|row| row.consumer_identity_hash() != identity);
}

fn validate_all(state: &MemoryState) -> Result<(), StorageError> {
    if state.event_consumers.len() > MAX_CONSUMER_DELIVERY_RECORDS
        || state
            .event_consumers
            .windows(2)
            .any(|pair| pair[0].identity().identity_hash() >= pair[1].identity().identity_hash())
        || state.event_consumer_deliveries.len()
            > MAX_CONSUMER_DELIVERY_RECORDS.saturating_mul(state.event_consumers.len().max(1))
    {
        return Err(corrupt());
    }
    for index in 0..state.event_consumers.len() {
        snapshot(state, index)?;
    }
    Ok(())
}

fn consumer_position(
    state: &MemoryState,
    identity: EventConsumerIdentityHash,
) -> Result<usize, usize> {
    state
        .event_consumers
        .binary_search_by_key(&identity, |row| row.identity().identity_hash())
}

fn delivery_position(
    rows: &[StoredEventConsumerDeliveryV1],
    identity: EventConsumerIdentityHash,
    event_id: EventId,
) -> Result<usize, usize> {
    rows.binary_search_by_key(&(identity, event_id), |row| {
        (row.consumer_identity_hash(), row.event_id())
    })
}

fn state_error(error: ConsumerStateError) -> StorageError {
    let kind = match error {
        ConsumerStateError::LimitExceeded => StorageErrorKind::LimitExceeded,
        ConsumerStateError::InvalidShape | ConsumerStateError::RevisionExhausted => {
            StorageErrorKind::CorruptData
        }
    };
    storage_error(kind)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
