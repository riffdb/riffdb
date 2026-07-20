//! Rebuildable operational accelerators derived from authoritative tables.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use redb::{ReadTransaction, ReadableTable};
use riffdb_storage_api::{StorageError, StorageErrorKind, StoredAdministrationAuditRecordV1};
use riffdb_types::{AdministrationSequence, EventId, RequestId};

use crate::codec::{decode_administration_audit_record_v1, decode_outbox_status_v1};
use crate::error::{precommit_storage_error, table_error};
use crate::keys::{decode_audit_key, decode_event_key};
use crate::layout::{AUDIT, OUTBOX, OUTBOX_STATUS};

const MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST: usize = 2;

pub(crate) struct TransientIndexes {
    service_audit_sequences: Option<BTreeMap<RequestId, Vec<AdministrationSequence>>>,
    pending_outbox: Option<BTreeSet<EventId>>,
}

#[derive(Default)]
pub(crate) enum TransientIndexState {
    #[default]
    Dormant,
    Ready(TransientIndexes),
    Invalid,
}

pub(crate) enum TransientIndexDelta {
    ServiceAuditAppended {
        request_id: RequestId,
        sequence: AdministrationSequence,
    },
    PendingOutboxInserted(Vec<EventId>),
    PendingOutboxMembership {
        event_id: EventId,
        pending: bool,
    },
}

impl TransientIndexes {
    pub(crate) fn rebuild(transaction: &ReadTransaction) -> Result<Self, StorageError> {
        Ok(Self {
            service_audit_sequences: Some(rebuild_service_audit(transaction)?),
            pending_outbox: rebuild_pending_outbox(transaction)?,
        })
    }

    pub(crate) fn service_audit_sequences(
        &self,
        request_id: RequestId,
    ) -> Option<&[AdministrationSequence]> {
        let index = self.service_audit_sequences.as_ref()?;
        Some(index.get(&request_id).map_or(&[], Vec::as_slice))
    }

    pub(crate) fn pending_outbox_page(
        &self,
        after: Option<EventId>,
        limit: usize,
    ) -> Option<(Vec<EventId>, bool)> {
        let pending_outbox = self.pending_outbox.as_ref()?;
        let mut events = match after {
            Some(after) => pending_outbox
                .range((Excluded(after), Unbounded))
                .copied()
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
            None => pending_outbox
                .iter()
                .copied()
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
        };
        let has_more = events.len() > limit;
        events.truncate(limit);
        Some((events, has_more))
    }

    pub(crate) fn apply(&mut self, delta: TransientIndexDelta) {
        match delta {
            TransientIndexDelta::ServiceAuditAppended {
                request_id,
                sequence,
            } => {
                let Some(index) = self.service_audit_sequences.as_mut() else {
                    return;
                };
                if insert_service_audit(index, request_id, sequence).is_err() {
                    self.service_audit_sequences = None;
                }
            }
            TransientIndexDelta::PendingOutboxInserted(events) => {
                let Some(index) = self.pending_outbox.as_mut() else {
                    return;
                };
                for event_id in events {
                    if !index.insert(event_id) {
                        self.pending_outbox = None;
                        return;
                    }
                }
            }
            TransientIndexDelta::PendingOutboxMembership { event_id, pending } => {
                let Some(index) = self.pending_outbox.as_mut() else {
                    return;
                };
                if pending {
                    index.insert(event_id);
                } else {
                    index.remove(&event_id);
                }
            }
        }
    }
}

impl Default for TransientIndexes {
    fn default() -> Self {
        Self {
            service_audit_sequences: Some(BTreeMap::new()),
            pending_outbox: Some(BTreeSet::new()),
        }
    }
}

fn rebuild_service_audit(
    transaction: &ReadTransaction,
) -> Result<BTreeMap<RequestId, Vec<AdministrationSequence>>, StorageError> {
    let mut index = BTreeMap::new();
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = decode_audit_key(key.value()).map_err(|_| corrupt())?;
        let record = decode_administration_audit_record_v1(value.value())?
            .into_parts()
            .0;
        if record.administration_sequence() != sequence {
            return Err(corrupt());
        }
        if let StoredAdministrationAuditRecordV1::Service(service) = record {
            insert_service_audit(&mut index, service.request_id(), sequence)
                .map_err(|_| corrupt())?;
        }
    }
    Ok(index)
}

fn rebuild_pending_outbox(
    transaction: &ReadTransaction,
) -> Result<Option<BTreeSet<EventId>>, StorageError> {
    let mut index = BTreeSet::new();
    let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
    let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
    for entry in intents.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value()).map_err(|_| corrupt())?;
        let status = statuses.get(key.value()).map_err(precommit_storage_error)?;
        let Some(status) = status else {
            index.insert(event_id);
            continue;
        };
        let Ok(status) = decode_outbox_status_v1(status.value()) else {
            return Ok(None);
        };
        if status.value().event_id() != event_id {
            return Ok(None);
        }
        if status.value().state().is_pending() {
            index.insert(event_id);
        }
    }
    for entry in statuses.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(event_id) = decode_event_key(key.value()) else {
            return Ok(None);
        };
        if intents
            .get(key.value())
            .map_err(precommit_storage_error)?
            .is_none()
        {
            return Ok(None);
        }
        let Ok(status) = decode_outbox_status_v1(value.value()) else {
            return Ok(None);
        };
        if status.value().event_id() != event_id {
            return Ok(None);
        }
    }
    Ok(Some(index))
}

fn insert_service_audit(
    index: &mut BTreeMap<RequestId, Vec<AdministrationSequence>>,
    request_id: RequestId,
    sequence: AdministrationSequence,
) -> Result<(), StorageError> {
    let sequences = index.entry(request_id).or_default();
    if sequences.len() >= MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST
        || sequences.last().is_some_and(|prior| prior >= &sequence)
    {
        return Err(invariant());
    }
    sequences.push(sequence);
    Ok(())
}

const fn invariant() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}

#[cfg(test)]
mod tests {
    use riffdb_types::CommitSequence;

    use super::*;

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
    }

    #[test]
    fn service_audit_accelerator_enforces_one_bounded_ordered_lifecycle() {
        let mut indexes = TransientIndexes::default();
        let request = request_id(1);
        for value in [1, 2] {
            indexes.apply(TransientIndexDelta::ServiceAuditAppended {
                request_id: request,
                sequence: AdministrationSequence::new(value).expect("sequence"),
            });
        }
        assert_eq!(indexes.service_audit_sequences(request).unwrap().len(), 2);
        indexes.apply(TransientIndexDelta::ServiceAuditAppended {
            request_id: request,
            sequence: AdministrationSequence::new(3).expect("sequence"),
        });
        assert_eq!(indexes.service_audit_sequences(request), None);
    }

    #[test]
    fn pending_outbox_accelerator_pages_in_event_order_and_tracks_membership() {
        let first = EventId::new(CommitSequence::first(), 0);
        let second = EventId::new(CommitSequence::first(), 1);
        let third = EventId::new(CommitSequence::first(), 2);
        let mut indexes = TransientIndexes::default();
        indexes.apply(TransientIndexDelta::PendingOutboxInserted(vec![
            third, first, second,
        ]));

        assert_eq!(
            indexes.pending_outbox_page(None, 2),
            Some((vec![first, second], true))
        );
        indexes.apply(TransientIndexDelta::PendingOutboxMembership {
            event_id: second,
            pending: false,
        });
        assert_eq!(
            indexes.pending_outbox_page(Some(first), 2),
            Some((vec![third], false))
        );
    }
}
