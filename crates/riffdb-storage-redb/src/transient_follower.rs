//! Bounded follower maintenance from a fully decoded receipt's final write view.
//! Never scan retained tables or clone the full cache. Changed old/new segments
//! are retained by Arc, so deletion does not copy potentially large old rows.
use super::*;
use redb::WriteTransaction;
use riffdb_storage_api::{AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N};

pub(crate) fn affects_indexes(namespace: N) -> bool {
    matches!(
        namespace,
        N::Commits | N::EventRoutes | N::Outbox | N::OutboxStatus
    )
}

impl TransientIndexes {
    pub(crate) fn apply_follower_receipt(
        &mut self,
        write: &WriteTransaction,
        mutations: &[AuthoritativeMutationV3],
    ) -> Result<(), StorageError> {
        let previous = self.locator_generation()?;
        let next =
            previous.prepare_follower_receipt(mutations, LocatorAllocationClass::Maintenance)?;
        self.install_locator_generation(next);
        self.apply_follower_disposable_receipt(write, mutations, &previous)
    }

    pub(crate) fn apply_follower_disposable_receipt(
        &mut self,
        write: &WriteTransaction,
        mutations: &[AuthoritativeMutationV3],
        previous: &LocatorGeneration,
    ) -> Result<(), StorageError> {
        // The checked frame bounds changed keys. Borrow each old/new metadata
        // entry without allocating a collection of all affected ownership.
        let current = self.locator_generation()?;
        for mutation in mutations.iter().filter(|m| m.namespace() == N::Commits) {
            let key = decode_application_sequence_key(mutation.key()).map_err(|_| corrupt())?;
            for generation in [previous, &current] {
                if let Some(segment) = generation.segments.get(&key) {
                    for (id, (partition, _)) in &segment.events {
                        self.refresh_route(write, &encode_event_route_key(*partition, *id))?;
                        self.refresh_outbox(write, *id)?;
                    }
                }
            }
            if let Some(indexes) = self.command_derived.as_mut() {
                indexes.payloads.remove(key)?;
            }
        }
        // All physical mutations were applied before this call. Union lookups
        // therefore never observe temporary orphan statuses or route copies.
        for mutation in mutations {
            match mutation.namespace() {
                N::EventRoutes => self.refresh_route(write, mutation.key())?,
                N::Outbox | N::OutboxStatus => self.refresh_outbox(
                    write,
                    decode_event_key(mutation.key()).map_err(|_| corrupt())?,
                )?,
                _ => {}
            }
        }
        Ok(())
    }

    fn refresh_route(&mut self, write: &WriteTransaction, key: &[u8]) -> Result<(), StorageError> {
        let (partition, event_id) =
            crate::keys::decode_event_route_key(key).map_err(|_| corrupt())?;
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let derived = indexes
            .event_at(event_id)
            .filter(|(scope, _)| *scope == partition)
            .map(|(_, event)| *event);
        let table = write.open_table(EVENT_ROUTES).map_err(table_error)?;
        let physical = table
            .get(key)
            .map_err(precommit_storage_error)?
            .map(|row| decode_event_route_v1(row.value()).map(|value| value.into_parts().0))
            .transpose()?;
        if derived.is_some() && physical.is_some() && derived != physical {
            return Err(corrupt());
        }
        let routes = self.event_routes.as_mut().ok_or_else(corrupt)?;
        if let Some(route) = derived.or(physical) {
            if route.event_id() != event_id {
                return Err(corrupt());
            }
            routes.insert(key.to_vec(), route);
        } else {
            routes.remove(key);
        }
        Ok(())
    }

    fn refresh_outbox(
        &mut self,
        write: &WriteTransaction,
        event_id: EventId,
    ) -> Result<(), StorageError> {
        let key = encode_event_key(event_id);
        let derived = self
            .command_derived
            .as_ref()
            .ok_or_else(corrupt)?
            .event_at(event_id)
            .is_some();
        let intents = write.open_table(OUTBOX).map_err(table_error)?;
        let present = derived
            || intents
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .is_some();
        let statuses = write.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let status = statuses
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|row| decode_outbox_status_v1(row.value()).map(|value| value.into_parts().0))
            .transpose()?;
        if status
            .as_ref()
            .is_some_and(|status| !present || status.event_id() != event_id)
        {
            return Err(corrupt());
        }
        let pending = present
            && status
                .as_ref()
                .is_none_or(|status| status.state().is_pending());
        let undelivered = present
            && status.as_ref().is_none_or(|status| {
                !matches!(status.state(), OutboxDeliveryStateV1::Delivered { .. })
            });
        for (index, contains) in [
            (&mut self.outbox_intents, present),
            (&mut self.pending_outbox, pending),
            (&mut self.undelivered_outbox, undelivered),
        ] {
            let index = index.as_mut().ok_or_else(corrupt)?;
            if contains {
                index.insert(event_id);
            } else {
                index.remove(&event_id);
            }
        }
        Ok(())
    }
}

impl LocatorGeneration {
    pub(crate) fn prepare_follower_receipt(
        &self,
        mutations: &[AuthoritativeMutationV3],
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let mut next = self.successor(class)?;
        // Remove all old manifests first: retention may split a segment and
        // replace its suffix at a different key in one authoritative receipt.
        for mutation in mutations.iter().filter(|m| m.namespace() == N::Commits) {
            let key = decode_application_sequence_key(mutation.key()).map_err(|_| corrupt())?;
            next.remove_segment(key)?;
        }
        for mutation in mutations.iter().filter(|m| m.namespace() == N::Commits) {
            let Some(value) = mutation.value() else {
                continue;
            };
            // Live follower apply has its existing frame limits, rather than
            // startup's recovery window. Its additional directory decode is
            // nevertheless admitted in the aggregate native construction pool.
            if let Some(segment) =
                decode_native_locator_construction_segment(value, None, class, None)?
            {
                if crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                    != mutation.key()
                {
                    return Err(corrupt());
                }
                next.insert_segment(&segment)?;
            }
        }
        Ok(next)
    }
}

impl CommandDerivedIndexes {
    // Immutable event metadata remains complete even if a manifest intentionally
    // omits a standalone locator during the pre-alpha representation transition.
    fn event_at(&self, id: EventId) -> Option<&(PartitionKeyHash, StoredEventRouteV1)> {
        let (_, metadata) = self.segments.predecessor(&id.commit_sequence())?;
        metadata.events.get(&id)
    }
}
