//! Bounded independent event oracle for the campaign's observed partition.

use riffdb_client_rust::ApplicationEventId;

use super::Result;
use super::application::TicketEventsEvent;

struct EventOracle {
    latest: Vec<Option<(ApplicationEventId, TicketEventsEvent)>>,
}

impl EventOracle {
    fn new() -> Self {
        Self {
            latest: vec![None; 1024],
        }
    }

    fn accept(&mut self, id: ApplicationEventId, event: &TicketEventsEvent) -> Result<bool> {
        let (organization, ticket_id, revision) = match event {
            TicketEventsEvent::TicketCreated(row) => (&row.organization_id, &row.ticket_id, 0),
            TicketEventsEvent::TicketUpdated(row) => {
                (&row.organization_id, &row.ticket_id, row.revision)
            }
        };
        if id.commit_sequence == 0
            || id.event_ordinal != 0
            || organization != &super::uuid(0x8001, 0)
        {
            return Err("event_identity");
        }
        let suffix = ticket_id
            .strip_prefix("01900000-0000-7000-8004-")
            .ok_or("event_identity")?;
        if suffix.len() != 12 {
            return Err("event_identity");
        }
        let ticket = u64::from_str_radix(suffix, 16).map_err(|_| "event_identity")?;
        if ticket >= 1024 || ticket_id != &super::uuid(0x8004, ticket) {
            return Err("event_identity");
        }
        match event {
            TicketEventsEvent::TicketCreated(row) => {
                if row.project_id != super::uuid(0x8003, 0)
                    || row.reporter_id != super::uuid(0x8002, 0)
                    || row.assignee_id != super::uuid(0x8002, 0)
                    || row.title != format!("Lifecycle ticket {ticket}")
                    || row.status != "Open"
                {
                    return Err("event_reconciliation");
                }
            }
            TicketEventsEvent::TicketUpdated(row) => {
                if revision == 0
                    || revision > super::model::MAX_COMMANDS.div_ceil(super::model::POPULATION)
                    || row.increment != 1
                    || row.status != super::model::status(revision)
                {
                    return Err("event_reconciliation");
                }
            }
        }
        let previous = self
            .latest
            .get_mut(ticket as usize)
            .ok_or("event_identity")?;
        if let Some((previous_id, previous_event)) = previous {
            if *previous_id == id {
                return if previous_event == event {
                    Ok(false)
                } else {
                    Err("event_reconciliation")
                };
            }
            let previous_revision = match previous_event {
                TicketEventsEvent::TicketCreated(_) => 0,
                TicketEventsEvent::TicketUpdated(row) => row.revision,
            };
            if id <= *previous_id || revision != previous_revision + 1 {
                return Err("event_gap");
            }
        } else if revision != 0 {
            return Err("event_creation_missing");
        }
        *previous = Some((id, event.clone()));
        Ok(true)
    }
}

pub(super) async fn run() -> Result<()> {
    use super::application::{
        TicketEventsConsumer, TicketEventsParams, TicketQueueResult, TicketQueueWatchParams,
        TicketQueueWatchUpdate,
    };
    use riffdb_client_rust::{
        ApplicationEventCheckpoint, ApplicationEventConsumerPublicStatus,
        ApplicationEventMutationResult, EventConsumerOptions,
    };
    use std::io::{self, Write};
    use std::time::{Duration, Instant};

    let stop = std::env::var_os("RIFFDB_LIFECYCLE_OBSERVER_STOP").ok_or("configuration")?;
    let stop = std::path::PathBuf::from(stop);
    if !stop.is_absolute() || stop.exists() || !stop.parent().is_some_and(std::path::Path::is_dir) {
        return Err("observer_stop_path");
    }
    let mut client = super::connect("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").await?;
    let mut live_client = super::connect("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").await?;
    let consumer = TicketEventsConsumer {
        parameters: TicketEventsParams {
            organization_id: super::uuid(0x8001, 0),
        },
        consumer_name: "ticketdesk-lifecycle-campaign".to_owned(),
    };
    let options = EventConsumerOptions {
        batch_limit: 32,
        in_flight_limit: 32,
        lease_seconds: 60,
        maximum_wait_nanos: 0,
    };
    // Register the retention hold before seeding; a caller must wait for ready.
    let initial = client
        .next_ticket_events(consumer.clone(), options)
        .await
        .map_err(|_| "observer_consumer")?;
    if !initial.events.is_empty() {
        return Err("observer_requires_fresh_consumer");
    }
    let mut stream = live_client
        .watch_ticket_queue_watch(
            TicketQueueWatchParams {
                organization_id: super::uuid(0x8001, 0),
                project_id: super::uuid(0x8003, 0),
            },
            None,
        )
        .await
        .map_err(|_| "observer_live_query")?;
    let first = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .map_err(|_| "observer_live_timeout")?
        .map_err(|_| "observer_live_query")?
        .ok_or("observer_live_ended")?;
    let TicketQueueWatchUpdate::Snapshot(snapshot) = first else {
        return Err("observer_live_snapshot");
    };
    let TicketQueueResult::Found(queue) = snapshot.result;
    if !queue.tickets.is_empty() {
        return Err("observer_requires_fresh_population");
    }
    let mut live_head = snapshot.checkpoint.application_head;
    let incarnation = snapshot.checkpoint.history_incarnation;
    if incarnation == 0 {
        return Err("observer_incarnation");
    }
    let mut oracle = EventOracle::new();
    let mut events = 0_u64;
    let mut duplicates = 0_u64;
    let mut checkpoint = 0_u64;
    let mut live_updates = 1_u64;
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-observer-ready/v1\",\"auxiliary_slots\":2}}"
    );
    io::stdout().flush().map_err(|_| "observation_output")?;
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut published = 0;
    loop {
        tokio::select! {
            update = stream.message() => {
                let update = update.map_err(|_| "observer_live_query")?.ok_or("observer_live_ended")?;
                let next = match update {
                    TicketQueueWatchUpdate::Snapshot(_) => return Err("observer_duplicate_snapshot"),
                    TicketQueueWatchUpdate::Reset(_) => return Err("observer_unplanned_reset"),
                    TicketQueueWatchUpdate::Patch(value) => value.checkpoint,
                    TicketQueueWatchUpdate::Checkpoint(value) => value,
                    TicketQueueWatchUpdate::Terminal(_) => return Err("observer_live_terminal"),
                };
                if next.history_incarnation != incarnation || next.application_head < live_head {
                    return Err("observer_live_frontier");
                }
                live_head = next.application_head;
                live_updates += 1;
            }
            _ = tick.tick() => {
                if stop.exists() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(88_200) {
                    return Err("observer_duration_bound");
                }
                let batch = client.next_ticket_events(consumer.clone(), options).await
                    .map_err(|_| "observer_consumer")?;
                for delivery in &batch.events {
                    let evidence = &delivery.evidence().event;
                    let command = match &delivery.event {
                        TicketEventsEvent::TicketCreated(_) => "CreateTicket",
                        TicketEventsEvent::TicketUpdated(_) => "UpdateTicket",
                    };
                    if evidence.history_incarnation != incarnation || evidence.writer_contract_version != 1
                        || evidence.command_name != command || evidence.actor_kind != "ACTOR_KIND_SERVICE"
                        || evidence.provenance_uri.is_empty() || evidence.provenance_uri.len() > 2048 {
                        return Err("observer_event_provenance");
                    }
                    if oracle.accept(evidence.id, &delivery.event)? {
                        events += 1;
                    } else {
                        duplicates += 1;
                    }
                    if client.ack_ticket_events(&consumer, delivery).await.map_err(|_| "observer_ack")?
                        != ApplicationEventMutationResult::Applied {
                        return Err("observer_ack_transition");
                    }
                }
                let Some(ApplicationEventConsumerPublicStatus::Exact(status)) =
                    client.ticket_events_status(&consumer).await.map_err(|_| "observer_status")? else {
                    return Err("observer_status");
                };
                if status.history_incarnation != incarnation || status.live_leases != 0
                    || status.retries != 0 || status.dead_letters != 0 {
                    return Err("observer_consumer_state");
                }
                let next_checkpoint = match status.checkpoint {
                    ApplicationEventCheckpoint::BeforeFirst => 0,
                    ApplicationEventCheckpoint::After(id) => id.commit_sequence,
                };
                if next_checkpoint < checkpoint {
                    return Err("observer_checkpoint");
                }
                checkpoint = next_checkpoint;
                let elapsed = started.elapsed().as_secs();
                if elapsed > published {
                    println!(concat!("{{\"schema\":\"riffdb.ticketdesk-lifecycle-observation/v1\",",
                        "\"elapsed_s\":{},\"events_checked\":{},\"duplicates\":{},",
                        "\"consumer_checkpoint\":{},\"live_head\":{},\"live_updates\":{},",
                        "\"history_incarnation\":{},\"auxiliary_slots\":2}}"),
                        elapsed, events, duplicates, checkpoint, live_head, live_updates, incarnation);
                    io::stdout().flush().map_err(|_| "observation_output")?;
                    published = elapsed;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{TicketEventsTicketCreated, TicketEventsTicketUpdated};

    fn created() -> TicketEventsEvent {
        TicketEventsEvent::TicketCreated(TicketEventsTicketCreated {
            organization_id: crate::uuid(0x8001, 0),
            ticket_id: crate::uuid(0x8004, 0),
            project_id: crate::uuid(0x8003, 0),
            reporter_id: crate::uuid(0x8002, 0),
            assignee_id: crate::uuid(0x8002, 0),
            title: "Lifecycle ticket 0".to_owned(),
            status: "Open".to_owned(),
        })
    }

    fn updated(revision: u64) -> TicketEventsEvent {
        TicketEventsEvent::TicketUpdated(TicketEventsTicketUpdated {
            organization_id: crate::uuid(0x8001, 0),
            ticket_id: crate::uuid(0x8004, 0),
            revision,
            status: crate::model::status(revision).to_owned(),
            increment: 1,
        })
    }

    fn id(commit_sequence: u64) -> ApplicationEventId {
        ApplicationEventId {
            commit_sequence,
            event_ordinal: 0,
        }
    }

    // req: END-003, END-007, END-008
    #[test]
    fn event_oracle_rejects_missing_creation_gaps_and_substituted_duplicates() {
        let mut oracle = EventOracle::new();
        assert!(oracle.accept(id(2), &updated(1)).is_err());
        assert_eq!(oracle.accept(id(1), &created()), Ok(true));
        assert_eq!(oracle.accept(id(1), &created()), Ok(false));
        assert!(oracle.accept(id(3), &updated(2)).is_err());
        assert_eq!(oracle.accept(id(2), &updated(1)), Ok(true));
        let mut forged = updated(1);
        let TicketEventsEvent::TicketUpdated(event) = &mut forged else {
            panic!("fixture")
        };
        event.increment = 2;
        assert!(oracle.accept(id(2), &forged).is_err());
        assert_eq!(oracle.accept(id(2), &updated(1)), Ok(false));
        assert_eq!(oracle.accept(id(3), &updated(2)), Ok(true));
    }

    // req: END-003, END-007, END-008
    #[test]
    fn event_oracle_rejects_wrong_partition_and_noncanonical_or_unbounded_identity() {
        let mut oracle = EventOracle::new();
        let mut event = created();
        let TicketEventsEvent::TicketCreated(row) = &mut event else {
            panic!("fixture")
        };
        row.organization_id = crate::uuid(0x8001, 1);
        assert!(oracle.accept(id(1), &event).is_err());
        let mut event = created();
        let TicketEventsEvent::TicketCreated(row) = &mut event else {
            panic!("fixture")
        };
        row.ticket_id = crate::uuid(0x8004, 1024);
        assert!(oracle.accept(id(1), &event).is_err());
        assert!(oracle.accept(id(0), &created()).is_err());
        assert!(
            oracle
                .accept(
                    ApplicationEventId {
                        commit_sequence: 1,
                        event_ordinal: 1
                    },
                    &created()
                )
                .is_err()
        );
        assert_eq!(oracle.latest.len(), 1024);
    }
}
