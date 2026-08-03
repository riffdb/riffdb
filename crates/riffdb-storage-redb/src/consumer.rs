//! Durable redb persistence for backend-neutral event-consumer transitions.

use redb::ReadableTable;
use riffdb_storage_api::{
    ConsumerStateError, EvaluatedEventConsumerTransitionV1, EventConsumerRepository,
    EventConsumerSnapshotV1, EventConsumerTransitionResultV1, EventConsumerTransitionV1,
    MAX_CONSUMER_DELIVERY_RECORDS, MAX_EVENT_CONSUMERS, StorageError, StorageErrorKind,
    StoredEventConsumerDeliveryV1, StoredEventConsumerV1, evaluate_event_consumer_transition,
};
use riffdb_types::{DatabaseId, EventConsumerIdentityHash};

use crate::codec::{
    decode_database_identity_v1, decode_event_consumer_delivery_v1, decode_event_consumer_v1,
    encode_event_consumer_delivery_v1, encode_event_consumer_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{
    decode_event_consumer_delivery_key, decode_event_consumer_key,
    encode_event_consumer_delivery_key, encode_event_consumer_key,
};
use crate::layout::{EVENT_CONSUMER_DELIVERIES, EVENT_CONSUMERS, META, META_DATABASE_ID};
use crate::store::RedbOperationalPorts;

impl EventConsumerRepository for RedbOperationalPorts {
    fn inspect_event_consumer(
        &self,
        identity: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        let deliveries = transaction
            .open_table(EVENT_CONSUMER_DELIVERIES)
            .map_err(table_error)?;
        read_snapshot_from_tables(&consumers, &deliveries, database_id, identity)
    }

    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        let identity = transition.consumer_identity_hash();
        let current = {
            let consumers = transaction
                .open_table(EVENT_CONSUMERS)
                .map_err(table_error)?;
            let deliveries = transaction
                .open_table(EVENT_CONSUMER_DELIVERIES)
                .map_err(table_error)?;
            read_snapshot_from_tables(&consumers, &deliveries, database_id, identity)?
        };
        let evaluated =
            evaluate_event_consumer_transition(current.clone(), transition).map_err(state_error)?;
        match evaluated {
            EvaluatedEventConsumerTransitionV1::NoChange(result) => {
                access.abort()?;
                Ok(result)
            }
            EvaluatedEventConsumerTransitionV1::Replace(replacement) => {
                replace_snapshot(transaction, current.as_ref(), replacement.as_ref())?;
                access.commit_for(RedbTestOperation::EventConsumerTransition)?;
                Ok(EventConsumerTransitionResultV1::Applied)
            }
            EvaluatedEventConsumerTransitionV1::Retire(identity) => {
                remove_snapshot(transaction, current.as_ref(), identity)?;
                access.commit_for(RedbTestOperation::EventConsumerTransition)?;
                Ok(EventConsumerTransitionResultV1::Applied)
            }
        }
    }

    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        let deliveries = transaction
            .open_table(EVENT_CONSUMER_DELIVERIES)
            .map_err(table_error)?;
        let mut inventory = Vec::new();
        for entry in consumers.iter().map_err(precommit_storage_error)? {
            if inventory.len() == MAX_EVENT_CONSUMERS {
                return Err(corrupt());
            }
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let consumer = decode_event_consumer_v1(value.value())?.into_parts().0;
            validate_consumer_key(database_id, key.value(), &consumer)?;
            let rows = read_deliveries(&deliveries, consumer.identity().identity_hash())?;
            inventory.push(EventConsumerSnapshotV1::new(consumer, rows).map_err(state_error)?);
        }
        Ok(inventory)
    }

    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        retention_low_water_from_table(&consumers, database_id)
    }
}

fn read_database_id_from_read(
    transaction: &redb::ReadTransaction,
) -> Result<DatabaseId, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let row = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(row.value())?.value())
}

fn read_database_id_from_write(
    transaction: &redb::WriteTransaction,
) -> Result<DatabaseId, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let row = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(row.value())?.value())
}

pub(crate) fn read_snapshot_from_tables<C, D>(
    consumers: &C,
    deliveries: &D,
    database_id: DatabaseId,
    identity: EventConsumerIdentityHash,
) -> Result<Option<EventConsumerSnapshotV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    D: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_event_consumer_key(identity);
    let Some(row) = consumers
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let consumer = decode_event_consumer_v1(row.value())?.into_parts().0;
    validate_consumer_key(database_id, key.as_slice(), &consumer)?;
    let delivery_rows = read_deliveries(deliveries, identity)?;
    EventConsumerSnapshotV1::new(consumer, delivery_rows)
        .map(Some)
        .map_err(state_error)
}

fn read_deliveries<D>(
    table: &D,
    identity: EventConsumerIdentityHash,
) -> Result<Vec<StoredEventConsumerDeliveryV1>, StorageError>
where
    D: ReadableTable<&'static [u8], &'static [u8]>,
{
    let (lower, upper) = delivery_bounds(identity);
    let mut rows = Vec::new();
    for entry in table
        .range(lower.as_slice()..=upper.as_slice())
        .map_err(precommit_storage_error)?
    {
        if rows.len() == MAX_CONSUMER_DELIVERY_RECORDS {
            return Err(corrupt());
        }
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let (key_identity, event_id) =
            decode_event_consumer_delivery_key(key.value()).map_err(|_| corrupt())?;
        let delivery = decode_event_consumer_delivery_v1(value.value())?
            .into_parts()
            .0;
        if key_identity != identity
            || delivery.consumer_identity_hash() != identity
            || delivery.event_id() != event_id
        {
            return Err(corrupt());
        }
        rows.push(delivery);
    }
    Ok(rows)
}

fn replace_snapshot(
    transaction: &redb::WriteTransaction,
    current: Option<&EventConsumerSnapshotV1>,
    replacement: &EventConsumerSnapshotV1,
) -> Result<(), StorageError> {
    let identity = replacement.consumer().identity().identity_hash();
    remove_snapshot(transaction, current, identity)?;
    let consumer_key = encode_event_consumer_key(identity);
    let consumer_value = encode_event_consumer_v1(replacement.consumer())?;
    let mut consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    if consumers
        .insert(consumer_key.as_slice(), consumer_value.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    drop(consumers);
    let mut deliveries = transaction
        .open_table(EVENT_CONSUMER_DELIVERIES)
        .map_err(table_error)?;
    for delivery in replacement.deliveries() {
        let key = encode_event_consumer_delivery_key(identity, delivery.event_id());
        let value = encode_event_consumer_delivery_v1(delivery)?;
        if deliveries
            .insert(key.as_slice(), value.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

fn remove_snapshot(
    transaction: &redb::WriteTransaction,
    current: Option<&EventConsumerSnapshotV1>,
    identity: EventConsumerIdentityHash,
) -> Result<(), StorageError> {
    let consumer_key = encode_event_consumer_key(identity);
    let mut consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    let removed = consumers
        .remove(consumer_key.as_slice())
        .map_err(precommit_storage_error)?;
    if removed.is_some() != current.is_some() {
        return Err(corrupt());
    }
    drop(removed);
    drop(consumers);
    let mut deliveries = transaction
        .open_table(EVENT_CONSUMER_DELIVERIES)
        .map_err(table_error)?;
    if let Some(current) = current {
        for delivery in current.deliveries() {
            let key = encode_event_consumer_delivery_key(identity, delivery.event_id());
            if deliveries
                .remove(key.as_slice())
                .map_err(precommit_storage_error)?
                .is_none()
            {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}

pub(crate) fn retention_low_water_from_table<T>(
    consumers: &T,
    database_id: DatabaseId,
) -> Result<Option<u64>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut minimum = None;
    for (count, entry) in consumers
        .iter()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count == MAX_EVENT_CONSUMERS {
            return Err(corrupt());
        }
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let consumer = decode_event_consumer_v1(value.value())?.into_parts().0;
        validate_consumer_key(database_id, key.value(), &consumer)?;
        minimum = Some(minimum.map_or_else(
            || consumer.checkpoint().retention_frontier(),
            |current: u64| current.min(consumer.checkpoint().retention_frontier()),
        ));
    }
    Ok(minimum)
}

fn validate_consumer_key(
    database_id: DatabaseId,
    key: &[u8],
    consumer: &StoredEventConsumerV1,
) -> Result<(), StorageError> {
    let identity = decode_event_consumer_key(key).map_err(|_| corrupt())?;
    if consumer.identity().database_id() != database_id
        || consumer.identity().identity_hash() != identity
    {
        return Err(corrupt());
    }
    Ok(())
}

fn delivery_bounds(identity: EventConsumerIdentityHash) -> ([u8; 44], [u8; 44]) {
    let mut lower = [0_u8; 44];
    lower[..32].copy_from_slice(identity.as_bytes());
    let mut upper = lower;
    upper[32..].fill(u8::MAX);
    (lower, upper)
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
