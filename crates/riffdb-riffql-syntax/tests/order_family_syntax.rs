//! Finite order-family source grammar coverage.

use riffdb_riffql_syntax::{RIFFQL_LANGUAGE_VERSION_ORDER_FAMILY_V1, format_query, parse_query};

const QUERY: &str = r#"query Experiments($scope: Experiment.scope, $order: ExperimentOrder, $limit: Limit<50000>) {
    many experiments from Experiment
        where scope == $scope
        order by $order {
            NameAsc: name asc, experiment_id asc;
            UpdatedDesc: last_update_time desc, experiment_id asc;
        }
        take $limit
        else IntegrityFailure
    return Found { experiments: experiments { experiment_id, name } }
    outcomes Found | IntegrityFailure
}"#;

#[test]
fn finite_order_family_is_canonical_and_structurally_closed() {
    let document = parse_query(QUERY).expect("order family parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_ORDER_FAMILY_V1
    );
    let family = document.body.bindings[0]
        .order_family
        .as_ref()
        .expect("family");
    assert_eq!(family.parameter.value.as_str(), "order");
    assert_eq!(family.variants.len(), 2);
    assert!(document.body.bindings[0].order.is_empty());
    let canonical = format_query(&document);
    assert_eq!(
        format_query(&parse_query(&canonical).expect("reparse")),
        canonical
    );
}
