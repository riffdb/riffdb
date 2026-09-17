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
        // The checked frame bounds mutations. At most two Arc references per
        // changed commit key are retained, regardless of the history size.
        let mut affected = Vec::new();
        let indexes = self.command_derived.as_mut().ok_or_else(corrupt)?;
        // Remove all old manifests first: a retained segment may be split or
        // replaced at another key in the same authoritative transaction.
        for mutation in mutations.iter().filter(|m| m.namespace() == N::Commits) {
            let key = decode_application_sequence_key(mutation.key()).map_err(|_| corrupt())?;
            if let Some(segment) = indexes.remove_segment(key)? {
                affected.push(segment);
            }
        }
        for mutation in mutations.iter().filter(|m| m.namespace() == N::Commits) {
            let Some(value) = mutation.value() else {
                continue;
            };
            match riffdb_storage_api::decode_command_segment_v1(value) {
                Ok(decoded) => {
                    let segment = Arc::new(decoded.into_parts().0);
                    if crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                        != mutation.key()
                    {
                        return Err(corrupt());
                    }
                    indexes.apply_segment_arc(Arc::clone(&segment))?;
                    affected.push(SegmentMetadata::new(&segment)?);
                }
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
                Err(_) => return Err(corrupt()),
            }
        }
        for segment in affected {
            for (id, (partition, _)) in &segment.events {
                self.refresh_route(write, &encode_event_route_key(*partition, *id))?;
                self.refresh_outbox(write, *id)?;
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

impl CommandDerivedIndexes {
    // Match startup rebuild's source of truth: the immutable command events,
    // not the presence of a locator entry in the segment manifest. Sequence and
    // ordinal identify one bounded member without walking any retained history.
    fn event_at(&self, id: EventId) -> Option<&(PartitionKeyHash, StoredEventRouteV1)> {
        let (_, metadata) = self.segments.range(..=id.commit_sequence()).next_back()?;
        metadata.events.get(&id)
    }

    fn remove_segment(
        &mut self,
        first: CommitSequence,
    ) -> Result<Option<SegmentMetadata>, StorageError> {
        let Some(segment) = self.segments.remove(&first) else {
            return Ok(None);
        };
        self.payloads.remove(first)?;
        for entry in segment.manifest.entries() {
            let expected = CommandDerivedLocator {
                segment_first: entry.segment_first_commit_sequence(),
                command_ordinal: entry.command_ordinal(),
                member_ordinal: entry.member_ordinal(),
                member: entry.member(),
            };
            if self.index_mut(entry.kind()).remove(entry.exact_key()) != Some(expected) {
                return Err(corrupt());
            }
        }
        Ok(Some(segment))
    }

    pub(super) fn index_mut(
        &mut self,
        kind: CommandDerivedIndexKindV1,
    ) -> &mut BTreeMap<Vec<u8>, CommandDerivedLocator> {
        match kind {
            CommandDerivedIndexKindV1::Idempotency => &mut self.idempotency,
            CommandDerivedIndexKindV1::Provenance => &mut self.provenance,
            CommandDerivedIndexKindV1::AuditSequence => &mut self.audit_sequence,
            CommandDerivedIndexKindV1::AuditRequest => &mut self.audit_request,
            CommandDerivedIndexKindV1::EventRoute => &mut self.event_route,
            CommandDerivedIndexKindV1::PendingOutbox => &mut self.pending_outbox,
        }
    }
}
