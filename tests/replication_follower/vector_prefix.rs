//! Real command execution, bootstrap and follower ingress with omitted vector work.
// req: REP-007, REP-003, REC-001
use super::{columnar_reads, oracle, support::*, wait_for_commit};
use riffdb_storage_api::AuthoritativeNamespaceV1 as N;
use riffdb_storage_api::*;

fn without_vector_work(receipt: &AuthoritativeTransactionV3) -> AuthoritativeTransactionV3 {
    let graph = receipt
        .mutations()
        .iter()
        .find(|row| row.namespace() == N::Commits)
        .unwrap();
    let decoded = decode_command_segment_v1(graph.value().unwrap()).unwrap();
    let segment = decoded.value();
    let commands = segment
        .commands()
        .iter()
        .map(|command| {
            let prefix = command.prefix_evidence().unwrap();
            assert!(
                prefix
                    .mutations()
                    .iter()
                    .any(|row| row.namespace() == N::VectorEvidence)
            );
            let mutations = prefix
                .mutations()
                .iter()
                .filter(|row| {
                    !matches!(
                        row.namespace(),
                        N::VectorEvidence | N::VectorEvidenceIndex | N::VectorObservations
                    )
                })
                .cloned()
                .collect();
            StoredCommandCapsuleV2::from_base_with_entity_transitions(
                command.base().clone(),
                command.index_generation_transitions().to_vec(),
                command.entity_transitions().to_vec(),
            )
            .unwrap()
            .with_prefix_evidence(
                CommandPrefixEvidenceV1::new(prefix.predecessor(), prefix.covered(), mutations)
                    .unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut fold = AuthoritativeMutationAccumulatorV3::default();
    for command in &commands {
        for row in command.prefix_evidence().unwrap().mutations() {
            fold.record(row.clone()).unwrap();
        }
    }
    let draft = StoredCommandSegmentV1::new(
        segment.database_id(),
        segment.history_incarnation(),
        segment.predecessor_segment_digest(),
        commands,
        segment.manifest().clone(),
        CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .unwrap();
    let (segment, encoded) = seal_and_encode_command_segment_v1(draft).unwrap();
    let mut mutations = fold.finish().unwrap();
    for row in receipt
        .mutations()
        .iter()
        .filter(|row| !CommandPrefixEvidenceV1::supports_namespace(row.namespace()))
    {
        mutations.push(if row.namespace() == N::Commits {
            AuthoritativeMutationV3::put(
                N::Commits,
                row.key(),
                row.expected_hash(),
                encoded.as_bytes(),
            )
            .unwrap()
        } else {
            row.clone()
        });
    }
    mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    let receipt =
        AuthoritativeTransactionV3::new(receipt.binding(), receipt.attribution(), mutations)
            .unwrap();
    let prefixes = segment
        .commands()
        .iter()
        .map(|command| command.prefix_evidence().unwrap())
        .collect::<Vec<_>>();
    validate_command_prefix_mutations_v1(&prefixes, &receipt).unwrap();
    receipt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_refuses_a_resealed_command_with_all_vector_work_omitted() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    let module = columnar_reads::deploy(&mut client, &admin).await;
    let caller = columnar_reads::authority(&mut client, &admin, module).await;
    let first = columnar_reads::write(
        &mut client,
        &caller,
        70,
        0x11,
        "first",
        [1.0, 0.0, 0.0, 0.0],
    )
    .await;
    stop(&mut primary);
    fixture.configure_document_projection("primary");
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    let proxy = super::proxy::Proxy::start(fixture.channel("primary").await).await;
    fixture.configure_follower_via(lineage, &replication, proxy.endpoint());
    fixture.configure_document_projection("follower");
    let mut follower = fixture.start("follower", Some("follower"));
    wait_for_commit(&fixture, &admin, first).await;
    stop(&mut follower);
    let mut applier = oracle::follower(&fixture.database("follower"));
    let baseline = applier.durable_history().unwrap();
    let second = columnar_reads::write(
        &mut client,
        &caller,
        71,
        0x11,
        "second",
        [0.0, 1.0, 0.0, 0.0],
    )
    .await;
    stop(&mut primary);
    let source = open_primary(&fixture.database("primary"));
    let pin = source.published_changelog_snapshot_v3().unwrap();
    let mut cursor = pin
        .changelog_receipts_v3(baseline.lineage(), baseline.tail())
        .unwrap();
    let mut receipts = Vec::new();
    while let Some(receipt) = cursor.next_receipt().unwrap() {
        assert!(receipts.len() < 256);
        receipts.push(receipt);
    }
    let offset = receipts
        .iter()
        .rposition(|receipt| {
            receipt
                .mutations()
                .iter()
                .any(|row| row.namespace() == N::Commits)
        })
        .unwrap();
    receipts.truncate(offset + 1);
    let binding = ChangelogFrameBindingV3::new(
        baseline.lineage().database_id(),
        baseline.lineage().history_incarnation(),
        baseline.lineage().leadership_epoch().get(),
        baseline.lineage().catalog_digest(),
        baseline.tail().history_hash(),
    )
    .unwrap();
    let original = ChangelogFrameV3::new(binding, receipts.clone())
        .unwrap()
        .encode()
        .unwrap();
    receipts[offset] = without_vector_work(&receipts[offset]);
    let forged = ChangelogFrameV3::new(binding, receipts)
        .unwrap()
        .encode()
        .unwrap();
    assert!(
        applier.apply_frame(&forged).is_err(),
        "valid checksums and net reduction cannot hide missing production vector work"
    );
    drop(applier);
    let mut reopened = oracle::follower(&fixture.database("follower"));
    assert_eq!(reopened.durable_history().unwrap(), baseline);
    let applied = reopened.apply_frame(&original).unwrap();
    assert_eq!(applied.frontier().application().unwrap().get(), second);
    drop(reopened);
    assert_eq!(
        oracle::follower(&fixture.database("follower"))
            .durable_history()
            .unwrap()
            .tail(),
        applied
    );
}
