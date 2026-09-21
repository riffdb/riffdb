// req: REP-005
use super::*;
use riffdb_storage_api::{
    ChangelogHistoryStateV3 as History, PrimaryFenceSourceEvidenceV1 as Evidence,
};

#[test]
fn source_evidence_rpo_uses_only_checked_application_sequences() {
    for head in [0, 9, u64::MAX - 2] {
        let observed = point(u64::MAX - 1, head, 5);
        let fence = fence(observed, 4, 6, 7).unwrap();
        let anchor = point(1, 0, 0);
        let history =
            History::new(fence.lineage(), anchor, point(u64::MAX, head, 6), anchor).unwrap();
        for applied in [0, head / 2, head] {
            let candidate = point(u64::MAX - 2, applied, 4);
            let evidence = Evidence::new(fence.clone(), candidate, history).unwrap();
            assert_eq!(evidence.application_rpo(), head - applied);
            assert_eq!(evidence.applied(), candidate);
            assert_eq!(evidence.fence(), &fence);
            assert_eq!(
                format!("{evidence:?}"),
                "PrimaryFenceSourceEvidenceV1([redacted])"
            );
        }
        let before_fence = History::new(fence.lineage(), anchor, observed, anchor).unwrap();
        assert!(Evidence::new(fence.clone(), anchor, before_fence).is_err());
        let beyond = point(u64::MAX - 2, head + 1, 4);
        assert!(Evidence::new(fence.clone(), beyond, history).is_err());
        let different_head = History::new(
            fence.lineage(),
            anchor,
            point(u64::MAX, head + 1, 6),
            anchor,
        )
        .unwrap();
        assert!(Evidence::new(fence.clone(), anchor, different_head).is_err());
        let foreign = History::new(lineage(3, 3), anchor, history.tail(), anchor).unwrap();
        assert!(Evidence::new(fence, anchor, foreign).is_err());
    }
}

#[test]
fn source_evidence_carriage_preserves_frozen_fence_bytes_and_checks_component_consistency() {
    fn fixture(name: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/primary-fence-v1.hex");
        let text = std::fs::read_to_string(path).unwrap();
        let hex = text
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .unwrap();
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
    let record = fixture("receipt");
    let applied = point(8, 3, 2);
    let anchor = point(1, 0, 0);
    let tail = point(10, 5, 3);
    let decoded = Evidence::from_fence_record(&record, applied, anchor, tail, anchor).unwrap();
    assert_eq!(decoded.fence(), &fence(point(9, 5, 2), 4, 3, 7).unwrap());
    assert_eq!(decoded.applied(), applied);
    assert_eq!(decoded.application_rpo(), 2);
    assert_eq!(decoded.encode_fence_record().unwrap(), record);
    let mut damaged = record.clone();
    *damaged.last_mut().unwrap() ^= 1;
    let mut trailing = record.clone();
    trailing.push(0);
    for malformed in [fixture("active"), damaged, trailing, vec![0; 4096], vec![]] {
        assert!(Evidence::from_fence_record(&malformed, applied, anchor, tail, anchor).is_err());
    }
    for (candidate, head, minimum) in [
        (point(8, 6, 2), tail, anchor),
        (applied, point(10, 6, 3), anchor),
        (applied, point(9, 5, 2), anchor),
        (applied, tail, point(9, 5, 2)),
    ] {
        assert!(Evidence::from_fence_record(&record, candidate, anchor, head, minimum).is_err());
    }
}
