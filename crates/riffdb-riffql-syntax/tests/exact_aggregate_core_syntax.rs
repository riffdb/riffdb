//! Syntax and identity acceptance for ADR-0152's additive exact core.

use riffdb_riffql_syntax::{
    AggregateFunction, RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1,
    RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1, format_query, parse_query,
};

const CORE_SOURCE: &str = r#"
query AggregateCore($organization_id: Item.organization_id) {
    many items from Item
        where organization_id == $organization_id
        order by item_id asc
        take 50

    aggregate summary from items {
        count_present(optional_label) as present_labels
        count_distinct(optional_label) as distinct_labels
        count_distinct_present(optional_label) as distinct_present_labels
        mean(amount) as amount_mean
        any(enabled) as any_enabled
        all(enabled) as all_enabled
    }

    return Found {
        summary: summary {
            present_labels
            distinct_labels
            distinct_present_labels
            amount_mean
            any_enabled
            all_enabled
        }
    }
    outcomes Found
}
"#;

#[test]
fn exact_core_functions_parse_format_and_select_only_the_additive_identity() {
    let parsed = parse_query(CORE_SOURCE).expect("exact aggregate core parses");
    assert_eq!(
        parsed.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1
    );
    assert_eq!(
        parsed.body.aggregates[0]
            .measures
            .iter()
            .map(|measure| measure.function.value)
            .collect::<Vec<_>>(),
        vec![
            AggregateFunction::CountPresent,
            AggregateFunction::CountDistinct,
            AggregateFunction::CountDistinctPresent,
            AggregateFunction::Mean,
            AggregateFunction::Any,
            AggregateFunction::All,
        ]
    );
    let canonical = format_query(&parsed);
    assert_eq!(
        format_query(&parse_query(&canonical).expect("canonical exact core reparses")),
        canonical
    );

    let old = parse_query(
        CORE_SOURCE
            .replace(
                "        count_present(optional_label) as present_labels\n        count_distinct(optional_label) as distinct_labels\n        count_distinct_present(optional_label) as distinct_present_labels\n        mean(amount) as amount_mean\n        any(enabled) as any_enabled\n        all(enabled) as all_enabled",
                "        count() as present_labels\n        sum(amount) as distinct_labels\n        min(amount) as distinct_present_labels\n        max(amount) as amount_mean",
            )
            .as_str(),
    )
    .expect("existing aggregates still parse");
    assert_eq!(old.language_version, RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1);
}
