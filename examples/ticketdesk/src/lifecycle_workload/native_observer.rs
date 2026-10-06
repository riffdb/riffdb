//! Public native observations owned by one inventoried campaign process.

use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::time::Duration;

use riffdb_client_rust::{CallMetadata, RiffDbClient, generate_request_id};
use riffdb_proto::v1;

use super::{Result, application, connection_configuration};

const MAX_REQUEST_BYTES: u64 = 64;
const MAX_SAMPLES: u64 = 86_400;

#[derive(Debug, Eq, PartialEq)]
struct Measurement {
    head: u64,
    statistics_head: u64,
    pending_deliveries: u64,
    outbox_degraded: bool,
    incarnation: u64,
    applied: u64,
    generation: u64,
    projection_backlog: u64,
}

fn applied(frontier: Option<v1::FrontierPosition>) -> Result<u64> {
    match frontier.and_then(|frontier| frontier.position) {
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) if sequence > 0 => {
            Ok(sequence)
        }
        _ => Err("native_observation_frontier"),
    }
}

fn measure(
    health: &v1::HealthResponse,
    stats: &v1::StatsResponse,
    projection: &v1::QueryProjectionResponse,
    expected: &v1::ProjectionIdentity,
) -> Result<Measurement> {
    let Some(v1::health_response::Result::Authenticated(health)) = &health.result else {
        return Err("native_observation_health");
    };
    let head = health
        .last_commit_sequence
        .ok_or("native_observation_head")?;
    let statistics_head = stats
        .last_commit_sequence
        .ok_or("native_observation_head")?;
    if head == 0
        || statistics_head < head
        || health.history_incarnation == 0
        || stats.history_incarnation != health.history_incarnation
    {
        return Err("native_observation_incarnation");
    }
    let mut outbox = health
        .components
        .iter()
        .filter(|component| component.component == v1::HealthComponentKind::Outbox as i32);
    let status = outbox.next().ok_or("native_observation_outbox")?.status;
    if outbox.next().is_some() {
        return Err("native_observation_outbox");
    }
    let outbox_degraded = match v1::HealthComponentStatus::try_from(status) {
        Ok(v1::HealthComponentStatus::Healthy) => false,
        Ok(v1::HealthComponentStatus::Degraded) => true,
        _ => return Err("native_observation_outbox"),
    };
    let Some(v1::query_projection_response::Result::Ready(projection)) = &projection.result else {
        return Err("native_observation_projection");
    };
    let data = projection
        .data
        .as_ref()
        .ok_or("native_observation_projection")?;
    let fence = data
        .observed_fence
        .as_ref()
        .ok_or("native_observation_projection")?;
    if data.items.len() > 1
        || fence.identity.as_ref() != Some(expected)
        || fence.generation == 0
        || fence.frontier != projection.frontier
    {
        return Err("native_observation_projection_identity");
    }
    let applied = applied(projection.frontier)?;
    Ok(Measurement {
        head,
        statistics_head,
        pending_deliveries: stats
            .pending_outbox_deliveries
            .ok_or("native_observation_outbox")?,
        outbox_degraded,
        incarnation: health.history_incarnation,
        applied,
        generation: fence.generation,
        projection_backlog: head.saturating_sub(applied),
    })
}

fn expected_projection() -> Result<v1::ProjectionIdentity> {
    let bundle = riffdb_contract_compiler::compile_contract_source(include_str!(
        "../../lifecycle/riffdb/contract.riff"
    ))
    .map_err(|_| "contract_descriptor")?;
    if bundle.bundle_hash().as_bytes() != &application::CONTRACT_BUNDLE_HASH {
        return Err("contract_descriptor_hash");
    }
    let projection = bundle
        .projections()
        .iter()
        .find(|projection| projection.name() == "TicketUpdates")
        .ok_or("projection_descriptor")?;
    Ok(v1::ProjectionIdentity {
        contract_lineage: "TicketDeskLifecycle".to_owned(),
        projection_id: projection.projection_id().get(),
        projection_plan_hash: projection.plan_hash().as_bytes().to_vec(),
    })
}

fn request_id() -> Result<Vec<u8>> {
    Ok(generate_request_id()
        .map_err(|_| "request_identity")?
        .into_bytes()
        .to_vec())
}

fn parse_request(encoded: &[u8]) -> Result<Option<u64>> {
    match encoded {
        b"sample 1\n" => Ok(Some(1)),
        b"sample 2\n" => Ok(Some(2)),
        b"stop\n" => Ok(None),
        _ => Err("native_observer_request"),
    }
}

async fn sample(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    expected: &v1::ProjectionIdentity,
    version: u64,
) -> Result<Measurement> {
    // One sequential slot, one transport attempt per call. No hidden retry.
    let health = client
        .health(
            v1::HealthRequest {
                request_id: Some(request_id()?),
            },
            metadata,
        )
        .await
        .map_err(|_| "native_observation_health")?;
    let statistics = client
        .stats(
            v1::StatsRequest {
                request_id: request_id()?,
            },
            metadata,
        )
        .await
        .map_err(|_| "native_observation_statistics")?;
    let organization = riffdb_client_rust::ApplicationUuid::from_text(super::uuid(0x8001, 0))
        .map_err(|_| "native_observer_configuration")?;
    let projection = client
        .query_projection(
            v1::QueryProjectionRequest {
                request_id: request_id()?,
                contract: Some(v1::ContractSelection {
                    selection: Some(v1::contract_selection::Selection::Exact(
                        v1::ExactContractSelection {
                            contract_lineage: expected.contract_lineage.clone(),
                            contract_version: version,
                        },
                    )),
                }),
                projection_id: expected.projection_id,
                leading_components: vec![v1::Value {
                    kind: Some(v1::value::Kind::UuidValue(organization.as_bytes().to_vec())),
                }],
                required_sequence: None,
                wait_nanos: 0,
                page: Some(v1::PageRequest {
                    limit: Some(1),
                    cursor: None,
                }),
            },
            metadata,
        )
        .await
        .map_err(|_| "native_observation_projection")?;
    measure(&health, &statistics, &projection, expected)
}

pub(super) async fn run() -> Result<()> {
    let credential = std::env::var_os("RIFFDB_LIFECYCLE_OPERATOR_CREDENTIAL")
        .ok_or("native_observer_configuration")?;
    let (tls, metadata) = connection_configuration(Path::new(&credential))?;
    let mut client = RiffDbClient::connect_verified_tls(&tls)
        .await
        .map_err(|_| "connection")?;
    let expected = expected_projection()?;
    let plan = expected
        .projection_plan_hash
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let stdin = io::stdin();
    let stdout = io::stdout();
    {
        let mut output = stdout.lock();
        writeln!(output, "{{\"schema\":\"riffdb.ticketdesk-lifecycle-native-observer-ready/v1\",\"auxiliary_slots\":1}}")
            .map_err(|_| "native_observer_output")?;
        output.flush().map_err(|_| "native_observer_output")?;
    }
    let mut index = 0_u64;
    loop {
        let mut line = Vec::with_capacity(MAX_REQUEST_BYTES as usize + 1);
        let read = {
            let mut input = stdin.lock();
            Read::take(&mut input, MAX_REQUEST_BYTES + 1)
                .read_until(b'\n', &mut line)
                .map_err(|_| "native_observer_request")?
        };
        if read == 0 {
            return Ok(());
        }
        if read > MAX_REQUEST_BYTES as usize {
            return Err("native_observer_request");
        }
        let Some(version) = parse_request(&line)? else {
            return Ok(());
        };
        if index >= MAX_SAMPLES {
            return Err("native_observer_sample_bound");
        }
        let value = tokio::time::timeout(
            Duration::from_secs(5),
            sample(&mut client, &metadata, &expected, version),
        )
        .await
        .map_err(|_| "native_observer_deadline")??;
        let mut output = stdout.lock();
        writeln!(output,
            "{{\"schema\":\"riffdb.ticketdesk-lifecycle-native-observation/v1\",\"index\":{index},\"contract_version\":{version},\"sampled_head\":{},\"statistics_head\":{},\"outbox_backlog\":{},\"outbox_degraded\":{},\"history_incarnation\":{},\"applied_through\":{},\"projection_generation\":{},\"projection_backlog\":{},\"projection_id\":{},\"projection_plan_hash\":\"{plan}\",\"transport_attempts\":3,\"retries\":0,\"auxiliary_slots\":1}}",
            value.head, value.statistics_head, value.pending_deliveries, value.outbox_degraded,
            value.incarnation, value.applied, value.generation, value.projection_backlog, expected.projection_id,
        ).map_err(|_| "native_observer_output")?;
        output.flush().map_err(|_| "native_observer_output")?;
        index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_proto::v1;

    fn responses() -> (
        v1::HealthResponse,
        v1::StatsResponse,
        v1::QueryProjectionResponse,
        v1::ProjectionIdentity,
    ) {
        let expected = v1::ProjectionIdentity {
            contract_lineage: "TicketDeskLifecycle".to_owned(),
            projection_id: 7,
            projection_plan_hash: vec![9; 32],
        };
        let frontier = v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::AppliedThrough(99)),
        };
        (
            v1::HealthResponse {
                result: Some(v1::health_response::Result::Authenticated(
                    v1::AuthenticatedHealth {
                        history_incarnation: 2,
                        last_commit_sequence: Some(100),
                        components: vec![v1::HealthComponent {
                            component: v1::HealthComponentKind::Outbox as i32,
                            status: v1::HealthComponentStatus::Healthy as i32,
                            replication: None,
                        }],
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
            v1::StatsResponse {
                history_incarnation: 2,
                last_commit_sequence: Some(101),
                pending_outbox_deliveries: Some(3),
                ..Default::default()
            },
            v1::QueryProjectionResponse {
                result: Some(v1::query_projection_response::Result::Ready(
                    v1::QueryProjectionReady {
                        frontier: Some(frontier),
                        data: Some(v1::ProjectionPage {
                            observed_fence: Some(v1::ProjectionPageFence {
                                identity: Some(expected.clone()),
                                generation: 4,
                                frontier: Some(frontier),
                            }),
                            ..Default::default()
                        }),
                    },
                )),
            },
            expected,
        )
    }

    // req: END-003, END-007, END-008
    #[test]
    fn measurements_keep_actual_nonzero_lag_and_pending_delivery_counts() {
        let (health, stats, projection, expected) = responses();
        let sample = measure(&health, &stats, &projection, &expected).expect("actual response");
        assert_eq!(sample.head, 100);
        assert_eq!(sample.pending_deliveries, 3);
        assert_eq!(sample.projection_backlog, 1);
        assert_eq!(sample.applied, 99);
        assert_eq!(sample.incarnation, 2);
        assert_eq!(sample.generation, 4);
    }

    // req: END-003, END-007, END-008
    #[test]
    fn absent_counts_and_incarnation_changes_cannot_be_caught_up_samples() {
        let (health, mut stats, projection, expected) = responses();
        stats.pending_outbox_deliveries = None;
        assert!(measure(&health, &stats, &projection, &expected).is_err());
        stats.pending_outbox_deliveries = Some(0);
        stats.history_incarnation += 1;
        assert!(measure(&health, &stats, &projection, &expected).is_err());
    }

    // req: END-003, END-007, END-008
    #[test]
    fn substituted_plan_or_frontier_and_unavailable_projection_refuse() {
        let (health, stats, mut projection, mut expected) = responses();
        expected.projection_plan_hash[0] ^= 1;
        assert!(measure(&health, &stats, &projection, &expected).is_err());
        expected.projection_plan_hash[0] ^= 1;
        let Some(v1::query_projection_response::Result::Ready(ready)) = &mut projection.result
        else {
            panic!("fixture")
        };
        ready
            .data
            .as_mut()
            .unwrap()
            .observed_fence
            .as_mut()
            .unwrap()
            .frontier = Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::AppliedThrough(98)),
        });
        assert!(measure(&health, &stats, &projection, &expected).is_err());
        projection.result = Some(v1::query_projection_response::Result::WaitTimedOut(
            v1::QueryProjectionWaitTimedOut {
                required_sequence: 100,
                current: None,
            },
        ));
        assert!(measure(&health, &stats, &projection, &expected).is_err());
    }

    // req: END-007, END-008, END-010
    #[test]
    fn relay_accepts_only_bounded_exact_samples_or_stop() {
        assert_eq!(parse_request(b"sample 1\n"), Ok(Some(1)));
        assert_eq!(parse_request(b"sample 2\n"), Ok(Some(2)));
        assert_eq!(parse_request(b"stop\n"), Ok(None));
        for input in [
            b"sample 0\n".as_slice(),
            b"sample 3\n",
            b"sample 1",
            b"sample 01\n",
            b"sample 1\r\n",
            b"sample 1 extra\n",
            b"arbitrary\n",
        ] {
            assert!(parse_request(input).is_err());
        }
    }
}
