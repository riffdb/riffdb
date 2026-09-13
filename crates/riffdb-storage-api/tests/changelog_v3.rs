#![forbid(unsafe_code)]
//! Checked V3 physical transaction ordering and canonical transition semantics.
// req: REP-003, STO-012, REC-001

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, ChangelogTransactionAllocator,
    ChangelogTransactionSequence, ChangelogV3Error,
};
use sha2::{Digest, Sha256};

fn binding() -> riffdb_storage_api::AuthoritativeTransactionBindingV3 {
    use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};
    riffdb_storage_api::AuthoritativeTransactionBindingV3 {
        database_id: DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap(),
        history_incarnation: 1,
        predecessor: None,
        sequence: ChangelogTransactionSequence::new(1).unwrap(),
        predecessor_frontier: DualFrontier::INITIAL,
        covered_frontier: DualFrontier::new(CommitSequence::new(1), None),
        prior_history_hash: [0; 32],
    }
}

#[test]
fn v3_receipt_roundtrips_overwrites_deletes_and_rejects_every_truncation() {
    use riffdb_storage_api::{
        AuthoritativeTransactionV3 as Receipt, ChangelogAttributionV3 as Source,
    };
    let old_hash: [u8; 32] = Sha256::digest(b"old").into();
    let mutations = vec![
        AuthoritativeMutationV3::put(N::Entities, b"a", Some(old_hash), b"overwritten").unwrap(),
        AuthoritativeMutationV3::delete(N::Entities, b"b", old_hash).unwrap(),
        AuthoritativeMutationV3::put(N::Entities, b"c", None, b"created").unwrap(),
    ];
    let receipt = Receipt::new(
        binding(),
        Source::JournaledApplicationGroup,
        mutations.clone(),
    )
    .unwrap();
    let encoded = receipt.encode().unwrap();
    assert_eq!(Receipt::decode(&encoded), Ok(receipt.clone()));
    assert_eq!(receipt.mutations(), mutations);
    for end in 0..encoded.len() {
        assert!(
            Receipt::decode(&encoded[..end]).is_err(),
            "accepted truncation at {end}"
        );
    }
    for index in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[index] ^= 0x80;
        assert!(
            Receipt::decode(&corrupt).is_err(),
            "accepted corrupt byte {index}"
        );
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert!(Receipt::decode(&trailing).is_err());
    let mut reversed = mutations.clone();
    reversed.reverse();
    assert!(Receipt::new(binding(), Source::JournaledApplicationGroup, reversed).is_err());
    assert!(
        Receipt::new(
            binding(),
            Source::JournaledApplicationGroup,
            vec![mutations[0].clone(), mutations[0].clone()]
        )
        .is_err()
    );
    assert!(!format!("{receipt:?}").contains("overwritten"));
}

#[test]
fn v3_receipts_require_contiguous_positions_and_attributed_frontiers() {
    use riffdb_storage_api::{
        AuthoritativeTransactionV3 as Receipt, ChangelogAttributionV3 as Source,
    };
    let mut control = binding();
    control.covered_frontier = control.predecessor_frontier;
    assert!(Receipt::new(control, Source::CleanClose, vec![]).is_ok());
    assert!(Receipt::new(control, Source::JournaledApplicationGroup, vec![]).is_err());
    assert!(Receipt::new(control, Source::JournaledServiceAudit, vec![]).is_err());
    let mutation = AuthoritativeMutationV3::put(N::Entities, b"a", None, b"value").unwrap();
    assert!(Receipt::new(binding(), Source::CleanClose, vec![mutation.clone()]).is_err());
    let mut skipped = binding();
    skipped.sequence = ChangelogTransactionSequence::new(2).unwrap();
    assert!(
        Receipt::new(
            skipped,
            Source::JournaledApplicationGroup,
            vec![mutation.clone()]
        )
        .is_err()
    );
    let mut zero_incarnation = binding();
    zero_incarnation.history_incarnation = 0;
    assert!(
        Receipt::new(
            zero_incarnation,
            Source::JournaledApplicationGroup,
            vec![mutation.clone()]
        )
        .is_err()
    );
    let mut exhausted = binding();
    exhausted.predecessor = ChangelogTransactionSequence::new(u64::MAX);
    assert!(Receipt::new(exhausted, Source::JournaledApplicationGroup, vec![mutation]).is_err());
}

#[test]
fn v3_receipt_rejects_noncanonical_payloads_even_with_recomputed_checksums() {
    use riffdb_storage_api::{
        AuthoritativeTransactionV3 as Receipt, ChangelogAttributionV3 as Source,
    };
    let mutation = AuthoritativeMutationV3::put(N::Entities, b"a", None, b"created").unwrap();
    let receipt =
        Receipt::new(binding(), Source::JournaledApplicationGroup, vec![mutation]).unwrap();
    let canonical = receipt.encode().unwrap();
    // Each corruption is structurally parseable and rechecksummed, so a checksum-
    // only reader is insufficient. Offsets independently pin the 128-byte header.
    for (offset, replacement) in [
        (9, 2),     // predecessor version
        (33, 0),    // zero incarnation
        (49, 2),    // skipped transaction
        (87, 255),  // unknown attribution
        (120, 255), // forged allocation-driving count
        (127, 0),   // wrong payload length
        (129, 255), // unknown namespace
        (130, 2),   // unknown operation
        (131, 2),   // unknown expected-state tag
        (140, 1),   // noncanonical absent hash
    ] {
        let mut malformed = canonical.clone();
        assert_ne!(malformed[offset], replacement);
        malformed[offset] = replacement;
        let end = malformed.len() - 32;
        let checksum: [u8; 32] = Sha256::digest(&malformed[..end]).into();
        malformed[end..].copy_from_slice(&checksum);
        assert!(
            Receipt::decode(&malformed).is_err(),
            "accepted changed byte {offset}"
        );
    }
    let oversized = vec![0; riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES + 1];
    assert_eq!(
        Receipt::decode(&oversized),
        Err(ChangelogV3Error::LimitExceeded)
    );
    assert_eq!(
        AuthoritativeMutationV3::put(N::Entities, b"key", None, &oversized),
        Err(ChangelogV3Error::LimitExceeded)
    );
}

#[test]
fn v3_frames_bind_exact_catalog_lineage_and_contiguous_receipt_history() {
    use riffdb_storage_api::{
        AuthoritativeStateCatalogV1, AuthoritativeTransactionV3 as Receipt,
        ChangelogAttributionV3 as Source, ChangelogFrameBindingV3, ChangelogFrameV3,
    };
    let first = Receipt::new(
        binding(),
        Source::JournaledApplicationGroup,
        vec![AuthoritativeMutationV3::put(N::Entities, b"a", None, b"value").unwrap()],
    )
    .unwrap();
    let mut next_binding = binding();
    next_binding.predecessor = Some(next_binding.sequence);
    next_binding.sequence = next_binding.sequence.checked_next().unwrap();
    next_binding.predecessor_frontier = next_binding.covered_frontier;
    next_binding.prior_history_hash = first.history_hash().unwrap();
    let last = Receipt::new(next_binding, Source::CleanClose, vec![]).unwrap();
    let frame_binding = ChangelogFrameBindingV3::new(
        binding().database_id,
        1,
        1,
        AuthoritativeStateCatalogV1.digest(),
        [0; 32],
    )
    .unwrap();
    let frame = ChangelogFrameV3::new(frame_binding, vec![first.clone(), last.clone()]).unwrap();
    let bytes = frame.encode().unwrap();
    assert_eq!(&bytes[..8], b"RDBCLF03");
    assert_eq!(&bytes[bytes.len() - 48..bytes.len() - 40], b"RDBCLE03");
    assert_eq!(ChangelogFrameV3::decode(&bytes), Ok(frame));
    for end in 0..bytes.len() {
        assert!(ChangelogFrameV3::decode(&bytes[..end]).is_err());
    }
    assert!(ChangelogFrameV3::new(frame_binding, vec![]).is_err());
    assert!(ChangelogFrameV3::new(frame_binding, vec![first.clone(), first.clone()]).is_err());
    let mut bad_hash = next_binding;
    bad_hash.prior_history_hash[0] ^= 1;
    let bad_receipt = Receipt::new(bad_hash, Source::CleanClose, vec![]).unwrap();
    assert!(ChangelogFrameV3::new(frame_binding, vec![first, bad_receipt]).is_err());
    assert!(ChangelogFrameBindingV3::new(binding().database_id, 1, 1, [0; 32], [0; 32]).is_err());
    let foreign = ChangelogFrameBindingV3::new(
        binding().database_id,
        2,
        1,
        AuthoritativeStateCatalogV1.digest(),
        [0; 32],
    )
    .unwrap();
    assert!(ChangelogFrameV3::new(foreign, vec![last]).is_err());
}

#[test]
fn v3_frame_bounds_count_grouped_transitions_not_only_receipt_rows() {
    use riffdb_storage_api::{
        AuthoritativeStateCatalogV1, AuthoritativeTransactionV3 as Receipt,
        ChangelogAttributionV3 as Source, ChangelogFrameBindingV3, ChangelogFrameV3,
    };
    let mut group_binding = binding();
    group_binding.covered_frontier =
        riffdb_types::DualFrontier::new(riffdb_types::CommitSequence::new(256), None);
    let group = Receipt::new(
        group_binding,
        Source::JournaledApplicationGroup,
        vec![AuthoritativeMutationV3::put(N::Entities, b"a", None, b"last-post-image").unwrap()],
    )
    .unwrap();
    let binding = ChangelogFrameBindingV3::new(
        group_binding.database_id,
        1,
        1,
        AuthoritativeStateCatalogV1.digest(),
        [0; 32],
    )
    .unwrap();
    assert!(ChangelogFrameV3::new(binding, vec![group.clone()]).is_ok());
    let close = Receipt::new(
        riffdb_storage_api::AuthoritativeTransactionBindingV3 {
            predecessor: Some(group_binding.sequence),
            sequence: group_binding.sequence.checked_next().unwrap(),
            predecessor_frontier: group_binding.covered_frontier,
            prior_history_hash: group.history_hash().unwrap(),
            ..group_binding
        },
        Source::CleanClose,
        vec![],
    )
    .unwrap();
    assert_eq!(
        ChangelogFrameV3::new(binding, vec![group, close]),
        Err(ChangelogV3Error::LimitExceeded)
    );
}

#[test]
fn v3_frame_rejects_forged_counts_and_catalog_with_valid_checksum() {
    use riffdb_storage_api::{
        AuthoritativeStateCatalogV1, AuthoritativeTransactionV3 as Receipt,
        ChangelogAttributionV3 as Source, ChangelogFrameBindingV3, ChangelogFrameV3,
    };
    let receipt = Receipt::new(
        binding(),
        Source::JournaledApplicationGroup,
        vec![AuthoritativeMutationV3::put(N::Entities, b"a", None, b"value").unwrap()],
    )
    .unwrap();
    let frame = ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            binding().database_id,
            1,
            1,
            AuthoritativeStateCatalogV1.digest(),
            [0; 32],
        )
        .unwrap(),
        vec![receipt],
    )
    .unwrap();
    let bytes = frame.encode().unwrap();
    // Version, leadership, catalog, predecessor/covered positions, receipt
    // count, payload length and attribution counts are independently bound.
    for offset in [9, 41, 42, 81, 89, 161, 165, 169] {
        let mut malformed = bytes.clone();
        malformed[offset] ^= 0x80;
        let footer = malformed.len() - 48;
        let checksum: [u8; 32] = Sha256::digest(&malformed[..footer]).into();
        let end = malformed.len();
        malformed[end - 32..].copy_from_slice(&checksum);
        if offset == 41 {
            // Nonzero leadership changes remain structurally valid; the stream
            // handshake must reject this otherwise valid foreign fence.
            assert_ne!(
                ChangelogFrameV3::decode(&malformed).unwrap().binding(),
                frame.binding()
            );
        } else {
            assert!(
                ChangelogFrameV3::decode(&malformed).is_err(),
                "accepted changed byte {offset}"
            );
        }
    }
}

#[test]
fn changelog_allocator_is_nonzero_checked_and_canonically_exhausted() {
    assert_eq!(ChangelogTransactionSequence::new(0), None);
    let initial = ChangelogTransactionAllocator::initial();
    let (assigned, next) = initial.allocate_one().unwrap();
    assert_eq!(assigned.get(), 1);
    assert_eq!(next.allocate_one().unwrap().0.get(), 2);
    let last = ChangelogTransactionSequence::new(u64::MAX).unwrap();
    let (assigned, exhausted) = ChangelogTransactionAllocator::Next(last)
        .allocate_one()
        .unwrap();
    assert_eq!(assigned, last);
    assert_eq!(exhausted, ChangelogTransactionAllocator::Exhausted);
    assert_eq!(
        exhausted.allocate_one(),
        Err(ChangelogV3Error::SequenceExhausted)
    );
    for state in [
        initial,
        next,
        ChangelogTransactionAllocator::Next(last),
        exhausted,
    ] {
        use riffdb_storage_api::proto_codec::{
            decode_changelog_transaction_allocator_v3 as decode,
            encode_changelog_transaction_allocator_v3 as encode,
        };
        let bytes = encode(state).unwrap().into_bytes();
        assert_eq!(*decode(&bytes).unwrap().value(), state);
        for end in 0..bytes.len() {
            assert!(decode(&bytes[..end]).is_err());
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
}

#[test]
fn authoritative_mutations_preserve_exact_expected_states_and_redact_debug() {
    let old = b"old-private-payload";
    let digest: [u8; 32] = Sha256::digest(old).into();
    let insert =
        AuthoritativeMutationV3::put(N::Entities, b"private-key", None, b"new-private-payload")
            .unwrap();
    assert!(insert.matches_prior(None));
    assert!(!insert.matches_prior(Some(old)));
    let replace = AuthoritativeMutationV3::put(
        N::Entities,
        b"private-key",
        Some(digest),
        b"new-private-payload",
    )
    .unwrap();
    let delete = AuthoritativeMutationV3::delete(N::Entities, b"private-key", digest).unwrap();
    for mutation in [&replace, &delete] {
        assert!(mutation.matches_prior(Some(old)));
        assert!(!mutation.matches_prior(None));
        assert!(!mutation.matches_prior(Some(b"different")));
        let diagnostic = format!("{mutation:?}");
        for private in ["private-key", "private-payload", &format!("{digest:?}")] {
            assert!(!diagnostic.contains(private));
        }
    }
    assert_eq!(delete.value(), None);
    assert_eq!(replace.value(), Some(b"new-private-payload".as_slice()));
    for namespace in [
        N::CleanCloseLifecycle,
        N::NextChangelogTransaction,
        N::ChangelogHistory,
        N::ReplicationSourceHolds,
        N::ProjectionState,
    ] {
        assert_eq!(
            AuthoritativeMutationV3::put(namespace, b"key", None, b"value"),
            Err(ChangelogV3Error::InvalidNamespace)
        );
    }
    assert!(AuthoritativeMutationV3::put(N::DatabaseIdentity, b"foreign", None, b"value").is_err());
    assert!(AuthoritativeMutationV3::put(N::Entities, b"", None, b"value").is_err());
}
