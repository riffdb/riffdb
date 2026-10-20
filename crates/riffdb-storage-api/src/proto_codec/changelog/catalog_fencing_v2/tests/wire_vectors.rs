// req: REP-005, STO-012, REC-001
use super::*;

#[test]
fn catalog_bound_fencing_canonical_wire_vectors_preserve_both_framings() {
    let (_, _, source) = evidence(2, 7, false);
    let cases = [
        (
            "admission-active",
            encode_replication_primary_admission_v2(
                &Admission::active_catalog_bound(source.source_history().lineage()).unwrap(),
            )
            .unwrap(),
        ),
        (
            "admission-fenced",
            encode_replication_primary_admission_v2(&Admission::fenced(source.fence().clone()))
                .unwrap(),
        ),
        (
            "fence",
            encode_primary_fence_administration_v2(source.fence()).unwrap(),
        ),
        (
            "promotion",
            encode_promotion_administration_v2(&promotion(false)).unwrap(),
        ),
    ];
    fn hex(bytes: &[u8]) -> String {
        let mut result = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut result, "{byte:02x}").unwrap();
        }
        result
    }
    let mut fixture = format!(
        "riffdb-catalog-bound-fencing-wire-vectors-v2\nrecords {}\n",
        cases.len()
    );
    for (name, compact) in cases {
        let decoded = readable_record_registry()
            .decode(compact.as_bytes())
            .unwrap();
        let schema = readable_record_schema(decoded.record_type()).unwrap();
        let legacy = riffdb_proto::envelope::encode_v1(schema, decoded.payload()).unwrap();
        match name {
            "admission-active" | "admission-fenced" => {
                let compact_value =
                    decode_replication_primary_admission(compact.as_bytes()).unwrap();
                assert_eq!(
                    decode_replication_primary_admission(&legacy)
                        .unwrap()
                        .value(),
                    compact_value.value()
                );
            }
            "fence" => assert_eq!(
                decode_primary_fence_administration(&legacy)
                    .unwrap()
                    .value(),
                source.fence()
            ),
            "promotion" => assert_eq!(
                decode_promotion_administration(&legacy).unwrap().value(),
                &promotion(false)
            ),
            _ => unreachable!("closed synthetic case inventory"),
        }
        fixture.push_str(&format!(
            "{name}\t{}\t{}\t{}\t{}\n",
            decoded.record_type(),
            hex(decoded.payload()),
            hex(compact.as_bytes()),
            hex(&legacy)
        ));
    }
    // Only designated-host generation produces concrete bytes for human review.
    if let Some(path) = std::env::var_os("RIFFDB_CATALOG_FENCING_WIRE_VECTOR_OUTPUT") {
        std::fs::write(path, fixture).unwrap();
        return;
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/proto/durable-catalog-bound-fencing-v2-wire-vectors.txt");
    assert_eq!(fixture, std::fs::read_to_string(path).unwrap());
}
