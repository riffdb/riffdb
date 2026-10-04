//! Private observation assembly across the two exact generated releases.

use super::{application as v1, application_v2 as v2, current::CurrentClient};
use riffdb_client_rust::{
    ApplicationClientError, ApplicationEventConsumerPublicStatus, ApplicationEventDelivery,
    ApplicationEventMutationResult, EventConsumerOptions, LiveQueryCheckpoint, LiveQueryCursor,
    LiveQueryTerminal, TypedLiveQueryStream,
};

pub(super) enum Delivery {
    V1(v1::TicketEventsDelivery),
    V2(v2::TicketEventsDelivery),
}

impl Delivery {
    pub(super) fn evidence(&self) -> &ApplicationEventDelivery {
        match self {
            Self::V1(value) => value.evidence(),
            Self::V2(value) => value.evidence(),
        }
    }

    pub(super) fn event(&self) -> v1::TicketEventsEvent {
        match self {
            Self::V1(value) => value.event.clone(),
            Self::V2(value) => match &value.event {
                v2::TicketEventsEvent::TicketCreated(row) => {
                    v1::TicketEventsEvent::TicketCreated(v1::TicketEventsTicketCreated {
                        organization_id: row.organization_id.clone(),
                        ticket_id: row.ticket_id.clone(),
                        project_id: row.project_id.clone(),
                        reporter_id: row.reporter_id.clone(),
                        assignee_id: row.assignee_id.clone(),
                        title: row.title.clone(),
                        status: row.status.clone(),
                    })
                }
                v2::TicketEventsEvent::TicketUpdated(row) => {
                    v1::TicketEventsEvent::TicketUpdated(v1::TicketEventsTicketUpdated {
                        organization_id: row.organization_id.clone(),
                        ticket_id: row.ticket_id.clone(),
                        revision: row.revision,
                        status: row.status.clone(),
                        increment: row.increment,
                    })
                }
            },
        }
    }
}

pub(super) struct Batch {
    pub(super) events: Vec<Delivery>,
    pub(super) status: ApplicationEventConsumerPublicStatus,
}

fn consumer_v1() -> v1::TicketEventsConsumer {
    v1::TicketEventsConsumer {
        parameters: v1::TicketEventsParams {
            organization_id: super::uuid(0x8001, 0),
        },
        consumer_name: "ticketdesk-lifecycle-campaign".to_owned(),
    }
}

fn consumer_v2() -> v2::TicketEventsConsumer {
    v2::TicketEventsConsumer {
        parameters: v2::TicketEventsParams {
            organization_id: super::uuid(0x8001, 0),
        },
        consumer_name: "ticketdesk-lifecycle-campaign".to_owned(),
    }
}

impl CurrentClient {
    pub(super) fn version(&self) -> u64 {
        match self {
            Self::V1(_) => 1,
            Self::V2(_) => 2,
        }
    }

    pub(super) async fn next_events(
        &mut self,
        options: EventConsumerOptions,
    ) -> Result<Batch, ApplicationClientError> {
        match self {
            Self::V1(client) => {
                let batch = client.next_ticket_events(consumer_v1(), options).await?;
                Ok(Batch {
                    events: batch.events.into_iter().map(Delivery::V1).collect(),
                    status: batch.status,
                })
            }
            Self::V2(client) => {
                let batch = client.next_ticket_events(consumer_v2(), options).await?;
                Ok(Batch {
                    events: batch.events.into_iter().map(Delivery::V2).collect(),
                    status: batch.status,
                })
            }
        }
    }

    pub(super) async fn ack_event(
        &mut self,
        delivery: &Delivery,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        match (self, delivery) {
            (Self::V1(client), Delivery::V1(value)) => {
                client.ack_ticket_events(&consumer_v1(), value).await
            }
            (Self::V2(client), Delivery::V2(value)) => {
                client.ack_ticket_events(&consumer_v2(), value).await
            }
            _ => Err(ApplicationClientError::InvalidInput),
        }
    }

    pub(super) async fn events_status(
        &mut self,
    ) -> Result<Option<ApplicationEventConsumerPublicStatus>, ApplicationClientError> {
        match self {
            Self::V1(client) => client.ticket_events_status(&consumer_v1()).await,
            Self::V2(client) => client.ticket_events_status(&consumer_v2()).await,
        }
    }

    pub(super) async fn watch_queue(
        &mut self,
        cursor: Option<LiveQueryCursor>,
    ) -> Result<LiveStream, ApplicationClientError> {
        match self {
            Self::V1(client) => client
                .watch_ticket_queue_watch(
                    v1::TicketQueueWatchParams {
                        organization_id: super::uuid(0x8001, 0),
                        project_id: super::uuid(0x8003, 0),
                    },
                    cursor,
                )
                .await
                .map(LiveStream::V1),
            Self::V2(client) => client
                .watch_ticket_queue_watch(
                    v2::TicketQueueWatchParams {
                        organization_id: super::uuid(0x8001, 0),
                        project_id: super::uuid(0x8003, 0),
                    },
                    cursor,
                )
                .await
                .map(LiveStream::V2),
        }
    }
}

pub(super) enum LiveStream {
    V1(TypedLiveQueryStream<v1::TicketQueueWatchParams>),
    V2(TypedLiveQueryStream<v2::TicketQueueWatchParams>),
}

pub(super) enum LiveUpdate {
    Snapshot(v1::TicketQueueFound, LiveQueryCheckpoint),
    Progress(LiveQueryCheckpoint),
    Reset,
    Terminal(LiveQueryTerminal),
}

fn queue_v2(value: v2::TicketQueueFound) -> v1::TicketQueueFound {
    v1::TicketQueueFound {
        tickets: value
            .tickets
            .into_iter()
            .map(|row| v1::TicketQueueFoundTickets {
                organization_id: row.organization_id,
                ticket_id: row.ticket_id,
                project_id: row.project_id,
                title: row.title,
                status: row.status,
                reporter_id: row.reporter_id,
                assignee_id: row.assignee_id,
                updated_at: v1::TimestampValue {
                    seconds: row.updated_at.seconds,
                    nanos: row.updated_at.nanos,
                },
            })
            .collect(),
    }
}

impl LiveStream {
    pub(super) async fn message(&mut self) -> Result<Option<LiveUpdate>, ApplicationClientError> {
        match self {
            Self::V1(stream) => Ok(stream.message().await?.map(|update| match update {
                v1::TicketQueueWatchUpdate::Snapshot(value) => {
                    let v1::TicketQueueResult::Found(queue) = value.result;
                    LiveUpdate::Snapshot(*queue, value.checkpoint)
                }
                v1::TicketQueueWatchUpdate::Patch(value) => LiveUpdate::Progress(value.checkpoint),
                v1::TicketQueueWatchUpdate::Checkpoint(value) => LiveUpdate::Progress(value),
                v1::TicketQueueWatchUpdate::Reset(_) => LiveUpdate::Reset,
                v1::TicketQueueWatchUpdate::Terminal(value) => LiveUpdate::Terminal(value),
            })),
            Self::V2(stream) => Ok(stream.message().await?.map(|update| match update {
                v2::TicketQueueWatchUpdate::Snapshot(value) => {
                    let v2::TicketQueueResult::Found(queue) = value.result;
                    LiveUpdate::Snapshot(queue_v2(*queue), value.checkpoint)
                }
                v2::TicketQueueWatchUpdate::Patch(value) => LiveUpdate::Progress(value.checkpoint),
                v2::TicketQueueWatchUpdate::Checkpoint(value) => LiveUpdate::Progress(value),
                v2::TicketQueueWatchUpdate::Reset(_) => LiveUpdate::Reset,
                v2::TicketQueueWatchUpdate::Terminal(value) => LiveUpdate::Terminal(value),
            })),
        }
    }
}
