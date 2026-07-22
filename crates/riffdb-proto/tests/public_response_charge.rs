//! ADR-0027 conservative service-charge coverage for exact public encodings.

use std::collections::BTreeMap;

const PUBLIC: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/public-response-charge-v1.tsv"
));
const SERVICE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../riffdb-service/fixtures/response-charge-v1.tsv"
));
const CEILING: usize = 4_194_304;

#[test]
fn every_service_charge_case_covers_its_public_encoding() {
    let service = SERVICE
        .lines()
        .skip(4)
        .map(|line| {
            let columns = line.split('\t').collect::<Vec<_>>();
            assert_eq!(columns.len(), 6);
            (
                columns[0],
                (
                    columns[1],
                    columns[4].parse::<usize>().expect("charge"),
                    columns[5],
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut lines = PUBLIC.lines();
    assert_eq!(
        lines.next(),
        Some("riffdb_public_response_charge_fixture_version\t1")
    );
    assert_eq!(lines.next(), Some("service_response_charge_version\t1"));
    assert_eq!(lines.next(), Some("ceiling_bytes\t4194304"));
    assert_eq!(
        lines.next(),
        Some("case_id\tresponse_family\tservice_charge_bytes\tprotobuf_encoded_bytes\tdisposition")
    );
    let mut actual = BTreeMap::new();
    for line in lines {
        let columns = line.split('\t').collect::<Vec<_>>();
        assert_eq!(columns.len(), 5);
        let charge = columns[2].parse::<usize>().expect("charge");
        let encoded =
            (columns[3] != "-").then(|| columns[3].parse::<usize>().expect("encoded length"));
        if columns[0].starts_with("boundary.") {
            assert_eq!(encoded, Some(charge), "synthetic boundary must be encoded");
        }
        if let Some(encoded) = encoded {
            assert!(encoded <= charge);
            match columns[4] {
                "release" => assert!(encoded <= CEILING),
                "response_too_large" => assert!(encoded > CEILING),
                other => panic!("unknown disposition: {other}"),
            }
        }
        assert!(
            actual
                .insert(columns[0], (columns[1], charge, columns[4]))
                .is_none()
        );
    }
    assert_eq!(actual, service);
    assert_eq!(actual["boundary.exact_ceiling"].1, CEILING);
    assert_eq!(actual["boundary.one_over"].1, CEILING + 1);
}
