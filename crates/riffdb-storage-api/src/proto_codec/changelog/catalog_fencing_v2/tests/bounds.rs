// req: REP-005, STO-012, REC-001
use super::*;

#[test]
fn maximum_valid_widths_fit_unchanged_caps_and_payload_one_over_refuses() {
    let (_, _, source) = evidence(u64::MAX - 1, u64::MAX - 1, true);
    let fence = encode_primary_fence_administration_v2(source.fence()).unwrap();
    let admission =
        encode_replication_primary_admission_v2(&Admission::fenced(source.fence().clone()))
            .unwrap();
    let promotion = promotion(true);
    let cutover = encode_promotion_administration_v2(&promotion).unwrap();
    for (name, encoded, cap) in [
        (FENCE_V2, &fence, 1000),
        (ADMISSION_V2, &admission, 1024),
        (PROMOTION_V2, &cutover, 8192),
    ] {
        let outer = readable_record_registry()
            .decode(encoded.as_bytes())
            .unwrap();
        assert!(outer.payload().len() <= cap);
        assert_eq!(
            readable_record_schema(name).unwrap().max_payload_bytes(),
            cap
        );
        let too_large = raw(name, vec![0; cap + 1]);
        assert!(decode_replication_primary_admission(&too_large).is_err());
        assert!(decode_primary_fence_administration(&too_large).is_err());
        assert!(decode_promotion_administration(&too_large).is_err());
    }
    assert!(fence.as_bytes().len() <= 1536);
    assert_eq!(
        decode_primary_fence_administration(fence.as_bytes())
            .unwrap()
            .value(),
        source.fence()
    );
    assert_eq!(
        decode_promotion_administration(cutover.as_bytes())
            .unwrap()
            .value(),
        &promotion
    );
    assert_eq!(
        promotion.next_administration(),
        crate::AdministrationSequenceAllocator::Exhausted
    );
    let selected = promotion.attempt().selection().unwrap();
    assert_eq!(selected.published_lineage().history_incarnation(), u64::MAX);
    assert_eq!(
        selected.published_lineage().leadership_epoch().get(),
        u64::MAX
    );
}
