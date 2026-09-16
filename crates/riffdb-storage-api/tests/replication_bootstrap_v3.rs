#![forbid(unsafe_code)]
//! Bounded complete-inventory bootstrap framing and integrity checks.
// req: REP-002, REP-003, REC-001
use riffdb_storage_api::*;
use riffdb_types::{DatabaseId, DualFrontier};
use std::collections::VecDeque;

fn fence() -> ReplicationBootstrapFenceV3 {
    let lineage = ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(9).unwrap(),
        [0x25; 32],
        DualFrontier::INITIAL,
    );
    ReplicationBootstrapFenceV3::new(
        ReplicationSourceHoldIdV1::new([0x18; 16]).unwrap(),
        ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap(),
    )
}
fn namespaces() -> impl Iterator<Item = AuthoritativeNamespaceV1> {
    AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
}
struct Cursor {
    fence: ReplicationBootstrapFenceV3,
    items: VecDeque<AuthoritativeStateStepV3>,
}
impl AuthoritativeStateCursorV3 for Cursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.fence.history()
    }
    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        Ok(self.items.pop_front())
    }
}
fn cursor() -> Cursor {
    Cursor {
        fence: fence(),
        items: namespaces()
            .flat_map(|n| {
                [
                    AuthoritativeStateStepV3::Row(
                        AuthoritativeStateRowV3::new(
                            n,
                            n.metadata_key().map(str::as_bytes).unwrap_or(b"key"),
                            b"opaque source bytes",
                        )
                        .unwrap(),
                    ),
                    AuthoritativeStateStepV3::EndNamespace(n),
                ]
            })
            .collect(),
    }
}

#[test]
fn bootstrap_pages_cover_every_authoritative_namespace_and_bind_one_fence() {
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(cursor())).unwrap();
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    assert!(source.manifest().is_err());
    let mut pages = 0;
    while let Some(page) = source.next_page().unwrap() {
        let encoded = page.encode().unwrap();
        let decoded = ReplicationBootstrapPageV3::decode(&encoded).unwrap();
        assert_eq!(decoded.encode().unwrap(), encoded);
        receiver.observe(&decoded).unwrap();
        pages += 1;
    }
    assert_eq!(pages, namespaces().count());
    let manifest = source.manifest().unwrap();
    assert_eq!(manifest.row_count(), pages as u64);
    assert_eq!(manifest.page_count(), pages as u32);
    assert_eq!(
        ReplicationBootstrapManifestV1::decode(&manifest.encode().unwrap()).unwrap(),
        manifest
    );
    receiver.verify_manifest(manifest).unwrap();
    assert!(source.next_page().unwrap().is_none());
}

#[test]
fn bootstrap_incomplete_or_substituted_stream_never_yields_a_manifest() {
    for arm in 0..4 {
        let mut input = cursor();
        match arm {
            0 => {
                input.items.pop_back();
            }
            1 => {
                input.items.pop_front();
                input.items.pop_front();
            }
            2 => {
                let item = input.items[0].clone();
                input.items.insert(1, item);
            }
            _ => input
                .items
                .push_back(AuthoritativeStateStepV3::EndNamespace(
                    namespaces().next().unwrap(),
                )),
        }
        let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(input)).unwrap();
        loop {
            match source.next_page() {
                Ok(Some(_)) => {}
                Err(_) => break,
                Ok(None) => panic!("arm {arm} unexpectedly completed"),
            }
        }
        assert!(source.manifest().is_err());
        assert!(source.next_page().is_err());
    }
}

#[test]
fn bootstrap_receiver_rejects_reordered_repeated_foreign_and_corrupt_pages() {
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(cursor())).unwrap();
    let first = source.next_page().unwrap().unwrap();
    let second = source.next_page().unwrap().unwrap();
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    assert!(receiver.observe(&second).is_err());
    assert!(receiver.observe(&first).is_err());
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    receiver.observe(&first).unwrap();
    assert!(receiver.observe(&first).is_err());
    let foreign = ReplicationBootstrapFenceV3::new(
        ReplicationSourceHoldIdV1::new([0x19; 16]).unwrap(),
        fence().history(),
    );
    assert!(
        ReplicationBootstrapTranscriptV3::new(foreign)
            .observe(&first)
            .is_err()
    );
    let encoded = first.encode().unwrap();
    for at in [0, 8, encoded.len() / 2, encoded.len() - 1] {
        let mut bytes = encoded.clone();
        bytes[at] ^= 1;
        assert!(ReplicationBootstrapPageV3::decode(&bytes).is_err());
    }
    assert!(
        ReplicationBootstrapPageV3::decode(&vec![0; MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES + 1])
            .is_err()
    );
}

#[test]
fn bootstrap_page_count_and_cross_page_order_are_bounded() {
    let first_ns = namespaces().next().unwrap();
    let mut input = cursor();
    input.items.pop_front();
    input.items.pop_front();
    let mut initial = VecDeque::new();
    for index in 0u32..257 {
        initial.push_back(AuthoritativeStateStepV3::Row(
            AuthoritativeStateRowV3::new(first_ns, &index.to_be_bytes(), b"value").unwrap(),
        ));
    }
    initial.push_back(AuthoritativeStateStepV3::EndNamespace(first_ns));
    initial.extend(input.items);
    input.items = initial;
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(input)).unwrap();
    let first = source.next_page().unwrap().unwrap();
    let second = source.next_page().unwrap().unwrap();
    assert_eq!(first.rows().len(), MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS);
    assert!(!first.ends_namespace());
    assert_eq!(second.rows().len(), 1);
    assert!(second.ends_namespace());
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    receiver.observe(&first).unwrap();
    let encoded = first.encode().unwrap();
    let hash = *encoded.last_chunk::<32>().unwrap();
    let backwards = ReplicationBootstrapPageV3::new(
        fence().digest(),
        2,
        first_ns,
        true,
        hash,
        vec![AuthoritativeStateRowV3::new(first_ns, &0u32.to_be_bytes(), b"value").unwrap()],
    )
    .unwrap();
    assert!(receiver.observe(&backwards).is_err());
    assert!(receiver.observe(&second).is_err());
}

#[test]
fn empty_namespaces_still_require_exact_end_pages() {
    let input = Cursor {
        fence: fence(),
        items: namespaces()
            .map(AuthoritativeStateStepV3::EndNamespace)
            .collect(),
    };
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(input)).unwrap();
    while let Some(page) = source.next_page().unwrap() {
        assert!(page.rows().is_empty());
        assert!(page.ends_namespace());
        assert!(ReplicationBootstrapPageV3::decode(&page.encode().unwrap()).is_ok());
    }
    assert_eq!(source.manifest().unwrap().row_count(), 0);
    assert_eq!(
        source.manifest().unwrap().page_count(),
        namespaces().count() as u32
    );
}

#[test]
fn frozen_bootstrap_external_receipt_and_page_v1_bytes_are_exact() {
    fn decode_hex(line: &str) -> Vec<u8> {
        line.as_bytes()
            .chunks_exact(2)
            .map(|digits| u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap())
            .collect()
    }
    let mut transcript = ReplicationBootstrapTranscriptV3::new(fence());
    let fixtures = include_str!("../../../fixtures/replication/bootstrap-pages-v1.hex");
    for line in fixtures.lines() {
        let bytes = decode_hex(line);
        let page = ReplicationBootstrapPageV3::decode(&bytes).unwrap();
        assert_eq!(page.encode().unwrap(), bytes);
        transcript.observe(&page).unwrap();
    }
    let bytes =
        decode_hex(include_str!("../../../fixtures/replication/bootstrap-manifest-v1.hex").trim());
    let manifest = ReplicationBootstrapManifestV1::decode(&bytes).unwrap();
    assert_eq!(manifest.encode().unwrap(), bytes);
    assert_eq!(manifest.page_count(), 53);
    assert_eq!(manifest.row_count(), 53);
    transcript.verify_manifest(manifest).unwrap();
}

#[test]
fn bootstrap_byte_limit_splits_complete_rows_without_truncation() {
    let first_ns = namespaces().next().unwrap();
    let mut input = cursor();
    input.items.pop_front();
    input.items.pop_front();
    let large = vec![0x61; MAX_CHANGELOG_FRAME_BYTES - 100];
    let mut initial = VecDeque::from([
        AuthoritativeStateStepV3::Row(
            AuthoritativeStateRowV3::new(first_ns, b"a", &large).unwrap(),
        ),
        AuthoritativeStateStepV3::Row(
            AuthoritativeStateRowV3::new(first_ns, b"b", &[0x62; 1024]).unwrap(),
        ),
        AuthoritativeStateStepV3::EndNamespace(first_ns),
    ]);
    initial.extend(input.items);
    input.items = initial;
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(input)).unwrap();
    let first = source.next_page().unwrap().unwrap();
    assert_eq!(first.rows().len(), 1);
    assert!(!first.ends_namespace());
    assert_eq!(first.rows()[0].value(), large);
    assert!(first.encoded_len().unwrap() <= MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES);
    let second = source.next_page().unwrap().unwrap();
    assert_eq!(second.rows().len(), 1);
    assert!(second.ends_namespace());
    assert_eq!(second.rows()[0].key(), b"b");
}

#[test]
fn bootstrap_progress_resumes_at_each_exact_durable_page_boundary() {
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(cursor())).unwrap();
    let mut pages = Vec::new();
    while let Some(page) = source.next_page().unwrap() {
        pages.push(page);
    }
    let manifest = source.manifest().unwrap();
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    for page in &pages {
        receiver.observe(page).unwrap();
        let checkpoint = receiver.checkpoint(manifest).unwrap();
        let bytes = checkpoint.encode().unwrap();
        let decoded = ReplicationBootstrapProgressV1::decode(&bytes).unwrap();
        assert_eq!(decoded, checkpoint);
        assert_eq!(decoded.page_count(), page.ordinal());
        receiver = ReplicationBootstrapTranscriptV3::resume(manifest, decoded, Some(page)).unwrap();
    }
    receiver.verify_manifest(manifest).unwrap();
    let checkpoint = receiver.checkpoint(manifest).unwrap();
    assert!(
        ReplicationBootstrapTranscriptV3::resume(manifest, checkpoint, Some(&pages[0])).is_err()
    );
}

#[test]
fn bootstrap_progress_preserves_mid_namespace_order_and_rejects_substitution() {
    let first_ns = namespaces().next().unwrap();
    let mut input = cursor();
    input.items.pop_front();
    input.items.pop_front();
    let mut initial: VecDeque<_> = (0u32..257)
        .map(|i| {
            AuthoritativeStateStepV3::Row(
                AuthoritativeStateRowV3::new(first_ns, &i.to_be_bytes(), b"value").unwrap(),
            )
        })
        .collect();
    initial.push_back(AuthoritativeStateStepV3::EndNamespace(first_ns));
    initial.extend(input.items);
    input.items = initial;
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(input)).unwrap();
    let mut pages = Vec::new();
    while let Some(page) = source.next_page().unwrap() {
        pages.push(page);
    }
    let manifest = source.manifest().unwrap();
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence());
    let empty = receiver.checkpoint(manifest).unwrap();
    assert!(
        ReplicationBootstrapTranscriptV3::resume(manifest, empty.clone(), Some(&pages[0])).is_err()
    );
    assert!(ReplicationBootstrapTranscriptV3::resume(manifest, empty, None).is_ok());
    receiver.observe(&pages[0]).unwrap();
    let progress = receiver.checkpoint(manifest).unwrap();
    let encoded = progress.encode().unwrap();
    for at in [0, 8, 12, encoded.len() - 1] {
        let mut corrupt = encoded.clone();
        corrupt[at] ^= 1;
        assert!(ReplicationBootstrapProgressV1::decode(&corrupt).is_err());
    }
    assert!(
        ReplicationBootstrapProgressV1::decode(&vec![
            0;
            MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES + 1
        ])
        .is_err()
    );
    assert!(ReplicationBootstrapTranscriptV3::resume(manifest, progress.clone(), None).is_err());
    let decoded = ReplicationBootstrapProgressV1::decode(&encoded).unwrap();
    let mut receiver =
        ReplicationBootstrapTranscriptV3::resume(manifest, decoded, Some(&pages[0])).unwrap();
    for page in &pages[1..] {
        receiver.observe(page).unwrap();
    }
    receiver.verify_manifest(manifest).unwrap();
    // Repeating the prior page after restoring its boundary is still an order
    // violation in the transcript; only the durable owner may prove a retry.
    let mut receiver =
        ReplicationBootstrapTranscriptV3::resume(manifest, progress, Some(&pages[0])).unwrap();
    assert!(receiver.observe(&pages[0]).is_err());
    assert!(receiver.checkpoint(manifest).is_err());
}

#[test]
fn frozen_bootstrap_progress_v1_bytes_bind_each_page_boundary() {
    fn unhex(line: &str) -> Vec<u8> {
        line.as_bytes()
            .chunks_exact(2)
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    }
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(cursor())).unwrap();
    let mut pages = vec![None];
    while let Some(page) = source.next_page().unwrap() {
        pages.push(Some(page));
    }
    let manifest = source.manifest().unwrap();
    let fixtures: Vec<_> = include_str!("../../../fixtures/replication/bootstrap-progress-v1.hex")
        .lines()
        .collect();
    assert_eq!(fixtures.len(), pages.len());
    for (page, line) in pages.iter().zip(fixtures) {
        let bytes = unhex(line);
        let progress = ReplicationBootstrapProgressV1::decode(&bytes).unwrap();
        assert_eq!(progress.encode().unwrap(), bytes);
        let resumed =
            ReplicationBootstrapTranscriptV3::resume(manifest, progress.clone(), page.as_ref())
                .unwrap();
        assert_eq!(resumed.checkpoint(manifest).unwrap(), progress);
    }
}

#[test]
fn bootstrap_progress_rejects_impossible_counts_even_with_a_valid_checksum() {
    use sha2::{Digest, Sha256};
    let mut source = ReplicationBootstrapPageCursorV3::new(fence(), Box::new(cursor())).unwrap();
    while source.next_page().unwrap().is_some() {}
    let manifest = source.manifest().unwrap();
    let empty = ReplicationBootstrapTranscriptV3::new(fence())
        .checkpoint(manifest)
        .unwrap();
    let encoded = empty.encode().unwrap();
    let counts = 12 + usize::from(u16::from_be_bytes(encoded[10..12].try_into().unwrap()));
    // pages, rows, bytes, namespace index, and the initial chain seed.
    for offset in [
        counts + 3,
        counts + 11,
        counts + 19,
        counts + 21,
        encoded.len() - 33,
    ] {
        let mut corrupt = encoded.clone();
        corrupt[offset] ^= 1;
        let end = corrupt.len() - 32;
        let digest: [u8; 32] = Sha256::digest(&corrupt[..end]).into();
        corrupt[end..].copy_from_slice(&digest);
        assert!(
            ReplicationBootstrapProgressV1::decode(&corrupt).is_err(),
            "offset {offset}"
        );
    }
}
