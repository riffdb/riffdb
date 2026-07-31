//! Rebuildable operational accelerators derived from authoritative tables.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use redb::{ReadTransaction, ReadableTable};
use riffdb_storage_api::{OutboxDeliveryStateV1, StorageError, StorageErrorKind};
use riffdb_types::{EventId, RequestId};

use crate::codec::decode_outbox_status_v1;
use crate::error::{precommit_storage_error, table_error};
use crate::keys::decode_event_key;
use crate::layout::{OUTBOX, OUTBOX_STATUS};

pub(crate) struct TransientIndexes {
    pending_outbox: Option<BTreeSet<EventId>>,
    undelivered_outbox: Option<BTreeSet<EventId>>,
}

struct RebuiltOutboxIndexes {
    pending: Option<BTreeSet<EventId>>,
    undelivered: Option<BTreeSet<EventId>>,
}

#[derive(Default)]
pub(crate) enum TransientIndexState {
    #[default]
    Dormant,
    Ready(TransientIndexes),
    Invalid,
}

#[allow(dead_code)]
pub(crate) enum TransientIndexDelta {
    Composite(Vec<TransientIndexDelta>),
    /// Retained for call-site compatibility; service-audit lookup is durable.
    ServiceAuditAppended {
        request_id: RequestId,
        sequence: riffdb_types::AdministrationSequence,
    },
    /// Retained for call-site compatibility; service-audit lookup is durable.
    ServiceAuditGroupAppended(Vec<(RequestId, riffdb_types::AdministrationSequence)>),
    PendingOutboxInserted(Vec<EventId>),
    PendingOutboxMembership {
        event_id: EventId,
        was_pending: bool,
        pending: bool,
        was_undelivered: bool,
        undelivered: bool,
    },
}

impl TransientIndexes {
    pub(crate) fn rebuild(transaction: &ReadTransaction) -> Result<Self, StorageError> {
        let rebuilt_outbox = rebuild_outbox_indexes(transaction)?;
        Ok(Self {
            pending_outbox: rebuilt_outbox.pending,
            undelivered_outbox: rebuilt_outbox.undelivered,
        })
    }

    pub(crate) fn undelivered_outbox_page(
        &self,
        after: Option<EventId>,
        limit: usize,
    ) -> Option<(Vec<EventId>, bool)> {
        page_event_index(self.undelivered_outbox.as_ref()?, after, limit)
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
            TransientIndexDelta::Composite(deltas) => {
                for delta in deltas {
                    self.apply(delta);
                }
            }
            TransientIndexDelta::ServiceAuditAppended { .. }
            | TransientIndexDelta::ServiceAuditGroupAppended(_) => {
                // Durable AUDIT_BY_REQUEST is authoritative; no in-memory map.
            }
            TransientIndexDelta::PendingOutboxInserted(events) => {
                let (Some(pending), Some(undelivered)) = (
                    self.pending_outbox.as_mut(),
                    self.undelivered_outbox.as_mut(),
                ) else {
                    return;
                };
                for event_id in events {
                    if !pending.insert(event_id) || !undelivered.insert(event_id) {
                        self.invalidate_outbox();
                        return;
                    }
                }
            }
            TransientIndexDelta::PendingOutboxMembership {
                event_id,
                was_pending,
                pending,
                was_undelivered,
                undelivered,
            } => {
                let (Some(pending_index), Some(undelivered_index)) = (
                    self.pending_outbox.as_mut(),
                    self.undelivered_outbox.as_mut(),
                ) else {
                    return;
                };
                if update_event_membership(pending_index, event_id, was_pending, pending).is_err()
                    || update_event_membership(
                        undelivered_index,
                        event_id,
                        was_undelivered,
                        undelivered,
                    )
                    .is_err()
                {
                    self.invalidate_outbox();
                }
            }
        }
    }

    fn invalidate_outbox(&mut self) {
        self.pending_outbox = None;
        self.undelivered_outbox = None;
    }
}

impl Default for TransientIndexes {
    fn default() -> Self {
        Self {
            pending_outbox: Some(BTreeSet::new()),
            undelivered_outbox: Some(BTreeSet::new()),
        }
    }
}

fn rebuild_outbox_indexes(
    transaction: &ReadTransaction,
) -> Result<RebuiltOutboxIndexes, StorageError> {
    // Single merge-join forward pass over OUTBOX and OUTBOX_STATUS (both ordered by event key).
    let mut pending = BTreeSet::new();
    let mut undelivered = BTreeSet::new();
    let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
    let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
    let mut intent_iter = intents.iter().map_err(precommit_storage_error)?;
    let mut status_iter = statuses.iter().map_err(precommit_storage_error)?;
    let mut next_intent = intent_iter
        .next()
        .transpose()
        .map_err(precommit_storage_error)?;
    let mut next_status = status_iter
        .next()
        .transpose()
        .map_err(precommit_storage_error)?;

    loop {
        match (next_intent.take(), next_status.take()) {
            (None, None) => break,
            (Some((intent_key, _)), None) => {
                let event_id = decode_event_key(intent_key.value()).map_err(|_| corrupt())?;
                pending.insert(event_id);
                undelivered.insert(event_id);
                next_intent = intent_iter
                    .next()
                    .transpose()
                    .map_err(precommit_storage_error)?;
            }
            (None, Some(_)) => {
                return Ok(RebuiltOutboxIndexes {
                    pending: None,
                    undelivered: None,
                });
            }
            (Some((intent_key, _)), Some((status_key, status_value))) => {
                let intent_bytes = intent_key.value();
                let status_bytes = status_key.value();
                match intent_bytes.cmp(status_bytes) {
                    std::cmp::Ordering::Less => {
                        let event_id = decode_event_key(intent_bytes).map_err(|_| corrupt())?;
                        pending.insert(event_id);
                        undelivered.insert(event_id);
                        next_intent = intent_iter
                            .next()
                            .transpose()
                            .map_err(precommit_storage_error)?;
                        next_status = Some((status_key, status_value));
                    }
                    std::cmp::Ordering::Greater => {
                        return Ok(RebuiltOutboxIndexes {
                            pending: None,
                            undelivered: None,
                        });
                    }
                    std::cmp::Ordering::Equal => {
                        let event_id = decode_event_key(intent_bytes).map_err(|_| corrupt())?;
                        let Ok(status) = decode_outbox_status_v1(status_value.value()) else {
                            return Ok(RebuiltOutboxIndexes {
                                pending: None,
                                undelivered: None,
                            });
                        };
                        if status.value().event_id() != event_id {
                            return Ok(RebuiltOutboxIndexes {
                                pending: None,
                                undelivered: None,
                            });
                        }
                        if status.value().state().is_pending() {
                            pending.insert(event_id);
                        }
                        if !matches!(
                            status.value().state(),
                            OutboxDeliveryStateV1::Delivered { .. }
                        ) {
                            undelivered.insert(event_id);
                        }
                        next_intent = intent_iter
                            .next()
                            .transpose()
                            .map_err(precommit_storage_error)?;
                        next_status = status_iter
                            .next()
                            .transpose()
                            .map_err(precommit_storage_error)?;
                    }
                }
            }
        }
    }
    Ok(RebuiltOutboxIndexes {
        pending: Some(pending),
        undelivered: Some(undelivered),
    })
}

fn page_event_index(
    index: &BTreeSet<EventId>,
    after: Option<EventId>,
    limit: usize,
) -> Option<(Vec<EventId>, bool)> {
    let mut events = match after {
        Some(after) => index
            .range((Excluded(after), Unbounded))
            .copied()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>(),
        None => index
            .iter()
            .copied()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>(),
    };
    let has_more = events.len() > limit;
    events.truncate(limit);
    Some((events, has_more))
}

fn update_event_membership(
    index: &mut BTreeSet<EventId>,
    event_id: EventId,
    expected: bool,
    updated: bool,
) -> Result<(), ()> {
    if index.contains(&event_id) != expected {
        return Err(());
    }
    if updated {
        index.insert(event_id);
    } else {
        index.remove(&event_id);
    }
    Ok(())
}

const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}

#[cfg(test)]
mod tests {
    use riffdb_types::CommitSequence;

    use super::*;

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
            was_pending: true,
            pending: false,
            was_undelivered: true,
            undelivered: true,
        });
        assert_eq!(
            indexes.pending_outbox_page(Some(first), 2),
            Some((vec![third], false))
        );
        assert_eq!(
            indexes.undelivered_outbox_page(Some(first), 2),
            Some((vec![second, third], false))
        );
    }
}
