//! Well-sealed codec fixtures are not proof of valid command-owned row images.
// req: REP-007, REP-003

use super::*;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, CommandSegmentDigestV1,
    StoredCommandSegmentV1, encode_administration_sequence_allocator_v1,
    encode_application_sequence_allocator_v1, seal_and_encode_command_segment_v1,
};

fn successor_fixture(history: ChangelogHistoryStateV3) -> Vec<u8> {
    let fixture =
        include_str!("../../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt");
    let encoded = fixture
        .lines()
        .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
        .unwrap();
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&encoded[offset..offset + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let decoded = riffdb_storage_api::decode_command_segment_v1(&bytes).unwrap();
    let segment = decoded.value();
    let draft = StoredCommandSegmentV1::new(
        history.lineage().database_id(),
        history.lineage().history_incarnation(),
        None,
        segment.commands().to_vec(),
        segment.manifest().clone(),
        CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .unwrap();
    seal_and_encode_command_segment_v1(draft)
        .unwrap()
        .1
        .into_bytes()
}

#[test]
fn follower_refuses_opaque_prefix_images_even_when_the_original_net_matches() {
    for valid in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("follower-prefix-net-refusal");
        let path = scope.join("db.redb");
        let history = fixture(&path);
        let segment = successor_fixture(history);
        let application = encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(CommitSequence::new(2).unwrap()),
        )
        .unwrap();
        let administration = encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::Next(AdministrationSequence::new(3).unwrap()),
        )
        .unwrap();
        let mut mutations = vec![
            Mutation::put(
                N::Entities,
                b"key",
                None,
                if valid {
                    b"intermediate"
                } else {
                    b"contradicts-prefix"
                },
            )
            .unwrap(),
            Mutation::put(N::Commits, &1_u64.to_be_bytes(), None, &segment).unwrap(),
        ];
        let read_store = RedbFollowerStore::open(&path).unwrap();
        let read = read_store.0.shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        for (namespace, bytes) in [
            (N::NextApplicationSequence, application),
            (N::NextAdministrationSequence, administration),
        ] {
            let key = namespace.metadata_key().unwrap();
            let before = meta.get(key).unwrap().unwrap();
            mutations.push(
                Mutation::replace(namespace, key.as_bytes(), before.value(), bytes.as_bytes())
                    .unwrap(),
            );
        }
        drop(meta);
        drop(read);
        mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: history.lineage().database_id(),
                history_incarnation: 1,
                predecessor: Some(history.tail().sequence()),
                sequence: history.tail().sequence().checked_next().unwrap(),
                predecessor_frontier: history.tail().frontier(),
                covered_frontier: DualFrontier::new(
                    CommitSequence::new(1),
                    AdministrationSequence::new(2),
                ),
                prior_history_hash: history.tail().history_hash(),
            },
            ChangelogAttributionV3::JournaledApplicationGroup,
            mutations,
        )
        .unwrap();
        let bytes = ChangelogFrameV3::new(
            ChangelogFrameBindingV3::new(
                history.lineage().database_id(),
                1,
                1,
                history.lineage().catalog_digest(),
                history.tail().history_hash(),
            )
            .unwrap(),
            vec![receipt],
        )
        .unwrap()
        .encode()
        .unwrap();
        let mut applier = isolated_applier(read_store);
        assert!(applier.apply_frame(&bytes).is_err());
        assert_eq!(applier.shared.durable_commit_epoch(), 0);
        assert!(applier.durable_history().is_err());
        let read = applier.shared.database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read).unwrap(),
            Some(history)
        );
        assert!(
            read.open_table(ENTITIES)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .is_none()
        );
    }
}
