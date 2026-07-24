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
const FULL_DISCOVERY_CEILING: usize = 2_621_440;

fn case_ceiling(case_id: &str) -> usize {
    if case_id.starts_with("discover_command_tools.full_")
        || case_id.starts_with("discover_resources.full_")
    {
        FULL_DISCOVERY_CEILING
    } else {
        CEILING
    }
}

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
                    columns[2],
                    columns[3],
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
        Some(
            "case_id\tresponse_family\tresponse_variant\tshape_v1\tservice_charge_bytes\tprotobuf_encoded_bytes\tdisposition"
        )
    );
    let mut actual = BTreeMap::new();
    let mut encoded_lengths = BTreeMap::new();
    for line in lines {
        let columns = line.split('\t').collect::<Vec<_>>();
        assert_eq!(columns.len(), 7);
        let charge = columns[4].parse::<usize>().expect("charge");
        let encoded =
            (columns[5] != "-").then(|| columns[5].parse::<usize>().expect("encoded length"));
        if columns[0].starts_with("boundary.") {
            assert_eq!(encoded, Some(charge), "synthetic boundary must be encoded");
        } else {
            assert!(encoded.is_some(), "public candidate is required");
        }
        if let Some(encoded) = encoded {
            assert!(encoded <= charge);
            assert!(encoded_lengths.insert(columns[0], encoded).is_none());
            let ceiling = case_ceiling(columns[0]);
            match columns[6] {
                "release" => {
                    assert!(charge <= ceiling);
                    assert!(encoded <= ceiling);
                }
                "response_too_large" => assert!(charge > ceiling),
                other => panic!("unknown disposition: {other}"),
            }
        }
        assert!(
            actual
                .insert(
                    columns[0],
                    (columns[1], columns[2], columns[3], charge, columns[6]),
                )
                .is_none()
        );
    }
    assert_eq!(actual, service);

    assert_eq!(actual["boundary.exact_ceiling"].3, CEILING);
    assert_eq!(actual["boundary.one_over"].3, CEILING + 1);
    assert_eq!(
        actual["discover_command_tools.full_exact_ceiling"].3,
        FULL_DISCOVERY_CEILING
    );
    assert_eq!(
        actual["discover_command_tools.full_one_over"].3,
        FULL_DISCOVERY_CEILING + 1
    );
    assert_eq!(
        actual["discover_resources.full_exact_ceiling"].3,
        FULL_DISCOVERY_CEILING
    );
    assert_eq!(
        actual["discover_resources.full_one_over"].3,
        FULL_DISCOVERY_CEILING + 1
    );
    assert_eq!(actual["operation_schema_catalog.accepted"].3, 7_882);
    assert_eq!(actual["discover_command_tools.compact_item_max"].3, 1_369);
    assert_eq!(actual["discover_command_tools.compact_page_max"].3, 335_729);
    assert_eq!(
        actual["discover_command_tools.full_max_dynamic"].3,
        2_106_529
    );
    assert_eq!(actual["discover_resources.compact_item_max"].3, 1_480);
    assert_eq!(actual["discover_resources.compact_page_max"].3, 320_872);
    assert_eq!(actual["discover_resources.full_max_dynamic"].3, 1_049_956);
    assert_eq!(actual["trace_provenance.found_max_claims"].3, 2_476);
    assert_eq!(
        actual["trace_provenance.found_max_claims"].2,
        "actor_id_bytes=13,affected_entity_count=1,approval_id_bytes=256,claim_count=4,entity_key_bytes=24,event_count=1,lineage_bytes=7,reason_bytes=1024,source_commit_bytes=128,source_repository_bytes=512,tenant_scope=global"
    );
    assert_eq!(
        encoded_lengths["discover_command_tools.compact_item_max"],
        702
    );
    assert_eq!(
        encoded_lengths["discover_command_tools.compact_page_max"],
        181_357
    );
    assert_eq!(
        encoded_lengths["discover_command_tools.full_max_dynamic"],
        2_105_539
    );
    assert_eq!(
        encoded_lengths["discover_resources.compact_item_max"],
        1_023
    );
    assert_eq!(
        encoded_lengths["discover_resources.compact_page_max"],
        271_001
    );
    assert_eq!(
        encoded_lengths["discover_resources.full_max_dynamic"],
        1_049_446
    );
    assert_eq!(encoded_lengths["trace_provenance.found_max_claims"], 2_086);

    for case_id in [
        "discover_command_tools.full_one_over",
        "discover_resources.full_one_over",
    ] {
        assert!(
            encoded_lengths[case_id] <= FULL_DISCOVERY_CEILING,
            "typed +1 response must be withheld only by its conservative service charge"
        );
    }

    for case_id in [
        "get_contract_version.found",
        "get_contract_version.not_found",
        "trace_provenance.found",
        "trace_provenance.found_max_claims",
        "trace_provenance.not_found",
        "list_pending_outbox_deliveries.pending",
        "list_pending_outbox_deliveries.retry_scheduled",
        "list_pending_outbox_deliveries.delivering",
        "list_pending_outbox_deliveries.dead_letter",
        "projection_status.uninitialized",
        "projection_status.building",
        "projection_status.catching_up",
        "projection_status.ready",
        "projection_status.rebuilding",
        "projection_status.degraded",
        "projection_status.invalid",
        "projection_status.not_found",
    ] {
        assert!(
            actual.contains_key(case_id),
            "missing fixture case {case_id}"
        );
    }
}
