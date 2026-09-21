#![forbid(unsafe_code)]
// req: REP-005, REP-003, STO-012, REC-001
//! Successor catalog framing preserves frozen V1-catalog bytes and admission bounds.
use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as Namespace,
    AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2,
    AuthoritativeTransactionBindingV3 as Binding, AuthoritativeTransactionV3 as Receipt,
    ChangelogAttributionV3 as Source, ChangelogFrameBindingV3 as FrameBinding,
    ChangelogFrameV3 as Frame, ChangelogTransactionSequence as Sequence, ChangelogV3Error,
    MAX_CHANGELOG_FRAME_BYTES,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};
use sha2::{Digest, Sha256};

fn binding() -> Binding {
    Binding {
        database_id: DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap(),
        history_incarnation: 2,
        predecessor: Sequence::new(9),
        sequence: Sequence::new(10).unwrap(),
        predecessor_frontier: DualFrontier::new(
            CommitSequence::new(5),
            AdministrationSequence::new(2),
        ),
        covered_frontier: DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(3)),
        prior_history_hash: [0x55; 32],
    }
}
fn frame_binding(catalog: [u8; 32]) -> FrameBinding {
    FrameBinding::new(binding().database_id, 2, 3, catalog, [0x66; 32]).unwrap()
}
fn mutation() -> Mutation {
    Mutation::put(Namespace::Audit, b"key", None, b"receipt").unwrap()
}
fn fence() -> Receipt {
    Receipt::new(binding(), Source::PrimaryFence, vec![mutation()]).unwrap()
}
fn refresh_checksum(bytes: &mut [u8]) {
    let end = bytes.len() - 48;
    let digest: [u8; 32] = Sha256::digest(&bytes[..end]).into();
    let start = bytes.len() - 32;
    bytes[start..].copy_from_slice(&digest);
}
fn unhex(value: &str) -> Vec<u8> {
    value
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn primary_fence_requires_one_control_transition_without_application_advancement() {
    assert_eq!(Source::PrimaryFence as u16, 34);
    assert_eq!(Source::from_tag(34), Some(Source::PrimaryFence));
    for case in 0..5 {
        let mut row = binding();
        let mut mutations = vec![mutation()];
        match case {
            0 => {
                row.covered_frontier =
                    DualFrontier::new(CommitSequence::new(6), AdministrationSequence::new(3))
            }
            1 => row.covered_frontier = row.predecessor_frontier,
            2 => {
                row.covered_frontier =
                    DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(4))
            }
            3 => mutations.clear(),
            4 => {
                row.predecessor_frontier = DualFrontier::INITIAL;
                row.covered_frontier = DualFrontier::new(None, AdministrationSequence::new(1));
            }
            _ => unreachable!(),
        }
        assert!(
            Receipt::new(row, Source::PrimaryFence, mutations).is_err(),
            "case {case}"
        );
    }
    assert_eq!(
        Receipt::decode(&fence().encode().unwrap()).unwrap(),
        fence()
    );
}

#[test]
fn successor_frame_carries_fence_attribution_and_refuses_catalog_relabeling() {
    let old = frame_binding(AuthoritativeStateCatalogV1.digest());
    let new = frame_binding(AuthoritativeStateCatalogV2.digest());
    assert!(Frame::new(old, vec![fence()]).is_err());
    let frame = Frame::new(new, vec![fence()]).unwrap();
    let bytes = frame.encode().unwrap();
    let vector = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    if let Some(path) = std::env::var_os("RIFFDB_PRIMARY_FENCE_FRAME_VECTOR_OUTPUT") {
        std::fs::write(path, vector).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/changelog-frame-v3-primary-fence.hex");
        assert_eq!(std::fs::read_to_string(path).unwrap(), vector);
    }
    assert_eq!(Frame::decode(&bytes).unwrap(), frame);
    assert_eq!(bytes.len(), 302 + 4 + fence().encoded_len().unwrap() + 48);
    assert_eq!(&bytes[298..302], &1_u32.to_be_bytes());
    for catalog in [AuthoritativeStateCatalogV1.digest(), [0x77; 32]] {
        let mut corrupt = bytes.clone();
        corrupt[42..74].copy_from_slice(&catalog);
        refresh_checksum(&mut corrupt);
        assert!(Frame::decode(&corrupt).is_err());
    }
    let mut counts = bytes.clone();
    counts[298..302].copy_from_slice(&2_u32.to_be_bytes());
    refresh_checksum(&mut counts);
    assert!(Frame::decode(&counts).is_err());
    for length in 0..bytes.len() {
        assert!(Frame::decode(&bytes[..length]).is_err());
    }
    let old_bytes = unhex(include_str!(
        "../../../fixtures/replication/changelog-frame-v3.hex"
    ));
    let old_frame = Frame::decode(&old_bytes).unwrap();
    assert_eq!(old_frame.encode().unwrap(), old_bytes);
    let mut relabeled = old_bytes;
    relabeled[42..74].copy_from_slice(&AuthoritativeStateCatalogV2.digest());
    refresh_checksum(&mut relabeled);
    assert!(Frame::decode(&relabeled).is_err());
}

#[test]
fn catalog_specific_admission_preserves_old_ceiling_and_checks_successor_overhead() {
    let old_catalog = AuthoritativeStateCatalogV1.digest();
    let new_catalog = AuthoritativeStateCatalogV2.digest();
    let maximum = MAX_CHANGELOG_FRAME_BYTES - 298 - 48 - 4 - 160 - 44 - 1;
    let mut row = binding();
    row.covered_frontier =
        DualFrontier::new(CommitSequence::new(6), AdministrationSequence::new(2));
    for (catalog, value_size, admitted) in [
        (old_catalog, maximum, true),
        (new_catalog, maximum, false),
        (new_catalog, maximum - 4, true),
        ([0xff; 32], 1, false),
    ] {
        let receipt = Receipt::new_for_catalog(
            row,
            Source::JournaledApplicationGroup,
            vec![Mutation::put(Namespace::Entities, b"a", None, &vec![1; value_size]).unwrap()],
            catalog,
        );
        if admitted {
            let frame = Frame::new(frame_binding(catalog), vec![receipt.unwrap()]).unwrap();
            assert_eq!(frame.encoded_len().unwrap(), MAX_CHANGELOG_FRAME_BYTES);
            assert_eq!(Frame::decode(&frame.encode().unwrap()).unwrap(), frame);
        } else {
            assert!(receipt.is_err());
        }
    }
    assert_eq!(
        Receipt::new_for_catalog(
            binding(),
            Source::PrimaryFence,
            vec![mutation()],
            old_catalog
        ),
        Err(ChangelogV3Error::InvalidEncoding)
    );
}

#[test]
fn original_catalog_history_cannot_admit_or_certify_primary_fence_receipts() {
    use riffdb_storage_api::{
        ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
        ChangelogLineageV3 as Lineage, LeadershipEpochV1,
    };
    let row = binding();
    let lineage = Lineage::new(
        row.database_id,
        row.history_incarnation,
        LeadershipEpochV1::new(3).unwrap(),
    )
    .unwrap();
    let predecessor = Point::new(
        row.predecessor.unwrap(),
        row.prior_history_hash,
        row.predecessor_frontier,
    );
    let before = History::new(lineage, predecessor, predecessor, predecessor).unwrap();
    assert!(before.advance(&fence()).is_err());
    let fabricated = History::new(
        lineage,
        predecessor,
        Point::from_receipt(&fence()).unwrap(),
        predecessor,
    )
    .unwrap();
    assert!(fabricated.validate_terminal_receipt(&fence()).is_err());
}
