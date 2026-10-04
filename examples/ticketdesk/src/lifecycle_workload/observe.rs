//! Bounded independent event oracle for the campaign's observed partition.

use riffdb_client_rust::ApplicationEventId;

use super::Result;
use super::application::TicketEventsEvent;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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

struct LiveProgress {
    head: AtomicU64,
    updates: AtomicU64,
}

trait LiveInput: Send {
    fn message(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<riffdb_client_rust::LiveQueryCheckpoint>>> + Send;
}

impl LiveInput
    for riffdb_client_rust::TypedLiveQueryStream<super::application::TicketQueueWatchParams>
{
    async fn message(&mut self) -> Result<Option<riffdb_client_rust::LiveQueryCheckpoint>> {
        use super::application::TicketQueueWatchUpdate;
        self.message()
            .await
            .map_err(|_| "observer_live_query")?
            .map(|update| match update {
                TicketQueueWatchUpdate::Snapshot(_) => Err("observer_duplicate_snapshot"),
                TicketQueueWatchUpdate::Reset(_) => Err("observer_unplanned_reset"),
                TicketQueueWatchUpdate::Patch(value) => Ok(value.checkpoint),
                TicketQueueWatchUpdate::Checkpoint(value) => Ok(value),
                TicketQueueWatchUpdate::Terminal(value) => Err(match value.reason.as_str() {
                    "LIVE_QUERY_TERMINAL_REASON_BUFFER_PRESSURE" => "observer_live_buffer_pressure",
                    "LIVE_QUERY_TERMINAL_REASON_LIFETIME_EXPIRED" => "observer_live_lifetime",
                    "LIVE_QUERY_TERMINAL_REASON_DEFINITION_CHANGED" => "observer_live_definition",
                    "LIVE_QUERY_TERMINAL_REASON_AUTHORIZATION_CHANGED" => {
                        "observer_live_authorization"
                    }
                    "LIVE_QUERY_TERMINAL_REASON_SERVICE_UNAVAILABLE" => "observer_live_unavailable",
                    _ => "observer_live_terminal",
                }),
            })
            .transpose()
    }
}

async fn drain_live<S: LiveInput>(
    mut stream: S,
    progress: Arc<LiveProgress>,
    incarnation: u64,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut previous = progress.head.load(Ordering::Acquire);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                changed.map_err(|_| "observer_stop")?;
                if *stop.borrow() { return Ok(()); }
            }
            update = stream.message() => {
                let next = update?.ok_or("observer_live_ended")?;
                if next.history_incarnation != incarnation || next.application_head < previous {
                    return Err("observer_live_frontier");
                }
                previous = next.application_head;
                progress.head.store(previous, Ordering::Release);
                progress.updates.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

fn validate_seeded_queue(queue: &super::application::TicketQueueFound) -> Result<()> {
    if queue.tickets.len() != 50 {
        return Err("observer_seeded_snapshot");
    }
    for (index, row) in queue.tickets.iter().enumerate() {
        let ticket = 1023 - index as u64;
        if row.organization_id != super::uuid(0x8001, 0)
            || row.project_id != super::uuid(0x8003, 0)
            || row.ticket_id != super::uuid(0x8004, ticket)
            || row.reporter_id != super::uuid(0x8002, 0)
            || row.assignee_id != super::uuid(0x8002, 0)
            || row.title != format!("Lifecycle ticket {ticket}")
            || row.status != "Open"
        {
            return Err("observer_seeded_snapshot");
        }
    }
    Ok(())
}

async fn start_live(
    client: &mut super::application::TicketDeskLifecycleClient,
    progress: &Arc<LiveProgress>,
    incarnation: u64,
    populated: bool,
) -> Result<riffdb_client_rust::TypedLiveQueryStream<super::application::TicketQueueWatchParams>> {
    use super::application::{TicketQueueResult, TicketQueueWatchParams, TicketQueueWatchUpdate};
    let mut stream = client
        .watch_ticket_queue_watch(
            TicketQueueWatchParams {
                organization_id: super::uuid(0x8001, 0),
                project_id: super::uuid(0x8003, 0),
            },
            None,
        )
        .await
        .map_err(|_| "observer_live_query")?;
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), stream.message())
        .await
        .map_err(|_| "observer_live_timeout")?
        .map_err(|_| "observer_live_query")?
        .ok_or("observer_live_ended")?;
    let TicketQueueWatchUpdate::Snapshot(snapshot) = first else {
        return Err("observer_live_snapshot");
    };
    let TicketQueueResult::Found(queue) = snapshot.result;
    if snapshot.checkpoint.history_incarnation != incarnation
        || queue.tickets.len() != if populated { 50 } else { 0 }
    {
        return Err("observer_live_snapshot");
    }
    if populated {
        validate_seeded_queue(&queue)?;
    }
    progress
        .head
        .store(snapshot.checkpoint.application_head, Ordering::Release);
    progress.updates.store(1, Ordering::Release);
    Ok(stream)
}

pub(super) async fn run() -> Result<()> {
    use super::application::{TicketEventsConsumer, TicketEventsParams};
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
    let pause = stop
        .parent()
        .ok_or("observer_stop_path")?
        .join("consumer.pause");
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
    let ApplicationEventConsumerPublicStatus::Exact(initial_status) = initial.status else {
        return Err("observer_status");
    };
    let incarnation = initial_status.history_incarnation;
    if incarnation == 0 {
        return Err("observer_incarnation");
    }
    let progress = Arc::new(LiveProgress {
        head: AtomicU64::new(0),
        updates: AtomicU64::new(0),
    });
    let deferred = match std::env::var("RIFFDB_LIFECYCLE_DEFER_LIVE") {
        Ok(value) if value == "1" => true,
        Err(std::env::VarError::NotPresent) => false,
        _ => return Err("configuration"),
    };
    let live_start = stop
        .parent()
        .ok_or("observer_stop_path")?
        .join("live.start");
    let mut oracle = EventOracle::new();
    let mut events = 0_u64;
    let mut duplicates = 0_u64;
    let mut checkpoint = 0_u64;
    let (stop_live, live_stopped) = tokio::sync::watch::channel(false);
    let mut live_workers = tokio::task::JoinSet::new();
    let mut live_stopped = Some(live_stopped);
    if !deferred {
        let stream = start_live(&mut live_client, &progress, incarnation, false).await?;
        live_workers.spawn(drain_live(
            stream,
            Arc::clone(&progress),
            incarnation,
            live_stopped.take().ok_or("observer_live_worker")?,
        ));
    }
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
            result = live_workers.join_next(), if !live_workers.is_empty() => {
                return result.ok_or("observer_live_worker")?.map_err(|_| "observer_live_worker")?
                    .and(Err("observer_live_ended"));
            }
            _ = tick.tick() => {
                if stop.exists() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(88_200) {
                    return Err("observer_duration_bound");
                }
                if live_stopped.is_some() && live_start.exists() {
                    let stream = start_live(&mut live_client, &progress, incarnation, true).await?;
                    live_workers.spawn(drain_live(stream, Arc::clone(&progress), incarnation,
                        live_stopped.take().ok_or("observer_live_worker")?));
                }
                if !pause.exists() {
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
                }
                let elapsed = started.elapsed().as_secs();
                if elapsed > published {
                    println!(concat!("{{\"schema\":\"riffdb.ticketdesk-lifecycle-observation/v1\",",
                        "\"elapsed_s\":{},\"events_checked\":{},\"duplicates\":{},",
                        "\"consumer_checkpoint\":{},\"live_head\":{},\"live_updates\":{},",
                        "\"history_incarnation\":{},\"consumer_paused\":{},\"auxiliary_slots\":2}}"),
                        elapsed, events, duplicates, checkpoint, progress.head.load(Ordering::Acquire),
                        progress.updates.load(Ordering::Acquire), incarnation, pause.exists());
                    io::stdout().flush().map_err(|_| "observation_output")?;
                    published = elapsed;
                }
            }
        }
    }
    if !live_workers.is_empty() {
        stop_live.send(true).map_err(|_| "observer_stop")?;
        tokio::time::timeout(Duration::from_secs(10), live_workers.join_next())
            .await
            .map_err(|_| "observer_live_shutdown")?
            .ok_or("observer_live_worker")?
            .map_err(|_| "observer_live_worker")??;
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
    struct FixtureLiveInput {
        messages: tokio::sync::mpsc::Receiver<riffdb_client_rust::LiveQueryCheckpoint>,
        third_pull: Option<tokio::sync::oneshot::Sender<()>>,
        pulls: u32,
    }

    impl LiveInput for FixtureLiveInput {
        async fn message(&mut self) -> Result<Option<riffdb_client_rust::LiveQueryCheckpoint>> {
            self.pulls += 1;
            if self.pulls == 3 {
                self.third_pull
                    .take()
                    .expect("one signal")
                    .send(())
                    .expect("receiver");
            }
            Ok(self.messages.recv().await)
        }
    }

    fn checkpoint(head: u64, incarnation: u64) -> riffdb_client_rust::LiveQueryCheckpoint {
        riffdb_client_rust::LiveQueryCheckpoint {
            application_head: head,
            history_incarnation: incarnation,
            cursor: riffdb_client_rust::LiveQueryCursor::new(vec![1])
                .expect("bounded opaque fixture"),
        }
    }

    // req: END-003, END-007, END-008
    #[tokio::test]
    async fn live_reader_progresses_and_stops_while_consumer_acknowledgement_is_held() {
        let (resume_consumer, held) = tokio::sync::oneshot::channel();
        let (consumer_done, mut completed) = tokio::sync::oneshot::channel();
        let consumer = tokio::spawn(async move {
            held.await.expect("explicit release");
            consumer_done.send(()).expect("completion receiver");
        });
        let (send, messages) = tokio::sync::mpsc::channel(1);
        let (third_pull, reached) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let progress = Arc::new(LiveProgress {
            head: AtomicU64::new(0),
            updates: AtomicU64::new(0),
        });
        let reader = tokio::spawn(drain_live(
            FixtureLiveInput {
                messages,
                third_pull: Some(third_pull),
                pulls: 0,
            },
            Arc::clone(&progress),
            1,
            stopped,
        ));
        send.send(checkpoint(1, 1)).await.expect("first update");
        send.send(checkpoint(2, 1)).await.expect("second update");
        reached
            .await
            .expect("reader requested another frame after applying both updates");
        assert_eq!(progress.head.load(Ordering::Acquire), 2);
        assert_eq!(progress.updates.load(Ordering::Acquire), 2);
        assert!(matches!(
            completed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        stop.send(true).expect("live stop");
        assert_eq!(reader.await.expect("reader task"), Ok(()));
        resume_consumer.send(()).expect("consumer release");
        consumer.await.expect("consumer task");
        completed.await.expect("consumer completion");
    }

    // req: END-003, END-007, END-008
    #[tokio::test]
    async fn live_reader_refuses_regression_and_incarnation_drift_without_publishing_them() {
        for (head, incarnation) in [(9, 1), (11, 2)] {
            let (send, messages) = tokio::sync::mpsc::channel(1);
            let (_stop, stopped) = tokio::sync::watch::channel(false);
            let progress = Arc::new(LiveProgress {
                head: AtomicU64::new(10),
                updates: AtomicU64::new(1),
            });
            let reader = tokio::spawn(drain_live(
                FixtureLiveInput {
                    messages,
                    third_pull: None,
                    pulls: 0,
                },
                Arc::clone(&progress),
                1,
                stopped,
            ));
            send.send(checkpoint(head, incarnation))
                .await
                .expect("invalid fixture");
            assert_eq!(
                reader.await.expect("reader task"),
                Err("observer_live_frontier")
            );
            assert_eq!(progress.head.load(Ordering::Acquire), 10);
            assert_eq!(progress.updates.load(Ordering::Acquire), 1);
        }
    }
    // req: END-003, END-007, END-008
    #[test]
    fn seeded_live_snapshot_requires_the_complete_expected_top_fifty() {
        use crate::application::{TicketQueueFound, TicketQueueFoundTickets, TimestampValue};
        let mut queue = TicketQueueFound {
            tickets: (0..50)
                .map(|index| {
                    let ticket = 1023 - index;
                    TicketQueueFoundTickets {
                        organization_id: crate::uuid(0x8001, 0),
                        project_id: crate::uuid(0x8003, 0),
                        ticket_id: crate::uuid(0x8004, ticket),
                        reporter_id: crate::uuid(0x8002, 0),
                        assignee_id: crate::uuid(0x8002, 0),
                        title: format!("Lifecycle ticket {ticket}"),
                        status: "Open".to_owned(),
                        updated_at: TimestampValue {
                            seconds: 0,
                            nanos: 0,
                        },
                    }
                })
                .collect(),
        };
        assert_eq!(validate_seeded_queue(&queue), Ok(()));
        queue.tickets[0].organization_id = crate::uuid(0x8001, 1);
        assert!(validate_seeded_queue(&queue).is_err());
        queue.tickets[0].organization_id = crate::uuid(0x8001, 0);
        queue.tickets.swap(0, 1);
        assert!(validate_seeded_queue(&queue).is_err());
        queue.tickets.swap(0, 1);
        queue.tickets.pop();
        assert!(validate_seeded_queue(&queue).is_err());
    }
}
