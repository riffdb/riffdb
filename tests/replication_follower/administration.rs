//! Public lifecycle retries retain one immutable registration and exact audit links.
// req: REP-006, REC-001, STO-012
use super::support::*;
use riffdb_client_rust::{BearerCredential, CallMetadata, v1};
use riffdb_errors::PublicErrorKind;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    ReplicationAdministrationActionV1, StorageScanLimit, StoredAdministrationAuditRecordV1,
};
use riffdb_types::{
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceOperationV1,
};

fn receipt(response: v1::RegisterFollowerResponse) -> v1::FollowerAdministrationReceipt {
    let Some(v1::register_follower_response::Result::Receipt(value)) = response.result else {
        panic!("registration must return its checked durable receipt")
    };
    value
}
fn retired(response: v1::RetireFollowerResponse) -> v1::FollowerAdministrationReceipt {
    let Some(v1::retire_follower_response::Result::Receipt(value)) = response.result else {
        panic!("retirement must return its checked durable receipt")
    };
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_lifecycle_public_retry_and_restart_preserve_exact_audit_linkage() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let target = v1::ReplicationFollowerTarget {
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        hold_id: vec![0x71; 16],
    };
    let register = |seed, budget| v1::RegisterFollowerRequest {
        request_id: request_id(seed),
        target: Some(target.clone()),
        hold_budget_sequences: budget,
        expires_at_application_sequence: None,
    };
    // This runs before contract deployment: operator custody has no application dependency.
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let first = receipt(
        client
            .register_follower(register(110, 10), &admin)
            .await
            .unwrap(),
    );
    assert!(!first.replayed);
    // Request IDs name one transport submission; retries need a fresh ID.
    let duplicate = client
        .register_follower(register(110, 10), &admin)
        .await
        .unwrap_err();
    assert_eq!(
        duplicate.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::StorageUnavailable)
    );
    // The existing audit failure boundary stops this source; reopening must
    // retain exactly the original registration and permit a fresh-ID retry.
    drop(client);
    primary
        .wait_for_exit(std::time::Duration::from_secs(30))
        .unwrap();
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    assert!(first.administration_sequence > 0 && first.registration_generation > 0);
    let replay = receipt(
        client
            .register_follower(register(111, 10), &admin)
            .await
            .unwrap(),
    );
    assert!(replay.replayed);
    assert_eq!(
        replay.administration_sequence,
        first.administration_sequence
    );
    assert_eq!(
        replay.registration_generation,
        first.registration_generation
    );
    assert_eq!(
        client
            .register_follower(register(112, 11), &admin)
            .await
            .unwrap()
            .result,
        Some(v1::register_follower_response::Result::Refusal(
            v1::FollowerAdministrationRefusal::RegistrationConflict as i32
        ))
    );
    let token = create_replication_capability(&mut client, &admin).await;
    let replication = CallMetadata::authenticated(BearerCredential::new(&token).unwrap());
    let denied = client
        .register_follower(register(113, 10), &replication)
        .await
        .unwrap_err();
    assert_eq!(
        denied.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::AuthorizationDenied)
    );
    let retire = |seed, generation| v1::RetireFollowerRequest {
        request_id: request_id(seed),
        target: Some(target.clone()),
        registration_generation: generation,
    };
    assert_eq!(
        client
            .retire_follower(retire(114, first.registration_generation + 1), &admin)
            .await
            .unwrap()
            .result,
        Some(v1::retire_follower_response::Result::Refusal(
            v1::FollowerAdministrationRefusal::RegistrationMissingOrStale as i32
        ))
    );
    drop(client);
    stop(&mut primary);
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    let replay = receipt(
        client
            .register_follower(register(115, 10), &admin)
            .await
            .unwrap(),
    );
    assert!(replay.replayed);
    assert_eq!(
        replay.administration_sequence,
        first.administration_sequence
    );
    let retirement = retired(
        client
            .retire_follower(retire(116, first.registration_generation), &admin)
            .await
            .unwrap(),
    );
    assert!(!retirement.replayed);
    assert!(retirement.administration_sequence > first.administration_sequence);
    assert_eq!(
        retirement.registration_generation,
        first.registration_generation
    );
    let replay = retired(
        client
            .retire_follower(retire(117, first.registration_generation), &admin)
            .await
            .unwrap(),
    );
    assert!(replay.replayed);
    assert_eq!(
        replay.administration_sequence,
        retirement.administration_sequence
    );
    let historical = receipt(
        client
            .register_follower(register(118, 10), &admin)
            .await
            .unwrap(),
    );
    assert!(historical.replayed);
    assert_eq!(
        historical.administration_sequence,
        first.administration_sequence
    );
    assert_eq!(
        historical.registration_generation,
        first.registration_generation
    );
    drop(client);
    stop(&mut primary);
    // Full startup validation reopens the exact records, not only the response summaries.
    let ports = open_primary(&fixture.database("primary"));
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(128).unwrap(),
        ))
        .unwrap()
    else {
        panic!("bounded lifecycle evidence must end");
    };
    let records: Vec<_> = records
        .into_iter()
        .map(|record| record.into_parts().0)
        .collect();
    let lifecycle: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            StoredAdministrationAuditRecordV1::Replication(value) => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(
        lifecycle.len(),
        2,
        "retries and refusals cannot mint transitions"
    );
    assert_eq!(
        lifecycle[0].action(),
        ReplicationAdministrationActionV1::RegisterFollower
    );
    assert_eq!(
        lifecycle[1].action(),
        ReplicationAdministrationActionV1::RetireFollower
    );
    assert_eq!(
        lifecycle[1].after().phase(),
        riffdb_storage_api::FollowerRegistrationPhaseV1::Retired
    );
    for (seed, operation, terminal, sequence) in [
        (
            110,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(first.administration_sequence),
        ),
        (
            111,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(first.administration_sequence),
        ),
        (
            112,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Failed,
            None,
        ),
        (
            113,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Denied,
            None,
        ),
        (
            114,
            ServiceOperationV1::RetireFollower,
            ServiceAuditPhaseV1::Failed,
            None,
        ),
        (
            115,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(first.administration_sequence),
        ),
        (
            116,
            ServiceOperationV1::RetireFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(retirement.administration_sequence),
        ),
        (
            117,
            ServiceOperationV1::RetireFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(retirement.administration_sequence),
        ),
        (
            118,
            ServiceOperationV1::RegisterFollower,
            ServiceAuditPhaseV1::Succeeded,
            Some(first.administration_sequence),
        ),
    ] {
        let audits: Vec<_> = records
            .iter()
            .filter_map(|record| match record {
                StoredAdministrationAuditRecordV1::Service(value)
                    if value.request_id().as_bytes().as_slice() == request_id(seed) =>
                {
                    Some(value)
                }
                _ => None,
            })
            .collect();
        let phases: Vec<_> = audits.iter().map(|value| value.phase()).collect();
        assert_eq!(
            phases,
            if terminal == ServiceAuditPhaseV1::Denied {
                vec![terminal]
            } else {
                vec![ServiceAuditPhaseV1::Started, terminal]
            }
        );
        for audit in &audits {
            assert_eq!(audit.operation(), operation);
            assert_eq!(
                audit.targets().as_slice(),
                &[ServiceAuditTargetV1::ReplicationFollower(
                    lifecycle[0].target()
                )]
            );
            assert!(audit.principal().is_some());
        }
        assert_eq!(
            audits.last().unwrap().link(),
            sequence.map_or(ServiceAuditLinkV1::None, |sequence| {
                ServiceAuditLinkV1::ControlPlane {
                    administration_sequence: riffdb_types::AdministrationSequence::new(sequence)
                        .unwrap(),
                }
            })
        );
    }
}
