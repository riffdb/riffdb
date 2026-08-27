//! Generic application closure for ADR-0150's operational access-path algebra.

use std::fs;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_operational_query_family;
use riffdb_query_ir::{AccessDirection, QueryAccessKind, QueryPredicateOperator, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

#[derive(Clone, Copy)]
struct ExpectedOperation<'a> {
    file: &'a str,
    index: &'a str,
    direction: AccessDirection,
    predicate: QueryPredicateOperator,
    cursor: bool,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn one_generic_application_proves_the_complete_operational_access_matrix() {
    let root = workspace_root().join("fixtures/riffql/operational-access-corpus-v1");
    let contract = fs::read_to_string(root.join("contract.riff")).expect("generic contract");
    let bundle = compile_contract_source(&contract).expect("generic contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");

    let expected = [
        ExpectedOperation {
            file: "items_by_code.riffq",
            index: "by_code",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::Equal,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_by_codes.riffq",
            index: "by_code",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::In,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_by_codes_reverse.riffq",
            index: "by_code",
            direction: AccessDirection::Reverse,
            predicate: QueryPredicateOperator::In,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_by_code_prefix.riffq",
            index: "by_code",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::Prefix,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_in_code_window.riffq",
            index: "by_code",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::Greater,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_outside_code.riffq",
            index: "by_code",
            direction: AccessDirection::Reverse,
            predicate: QueryPredicateOperator::NotEqual,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_by_kinds.riffq",
            index: "by_kind",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::In,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_in_score_window.riffq",
            index: "by_score",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::GreaterEqual,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_outside_score.riffq",
            index: "by_score",
            direction: AccessDirection::Reverse,
            predicate: QueryPredicateOperator::NotEqual,
            cursor: true,
        },
        ExpectedOperation {
            file: "items_by_published_state.riffq",
            index: "by_published",
            direction: AccessDirection::Forward,
            predicate: QueryPredicateOperator::IsNotNull,
            cursor: true,
        },
    ];

    for expected in expected {
        let source = fs::read_to_string(root.join("queries").join(expected.file))
            .expect("generic query source");
        let family = compile_operational_query_family(
            &parse_query(&source).expect("generic query parses"),
            &catalog,
        )
        .unwrap_or_else(|diagnostics| panic!("{}: {diagnostics:?}", expected.file));
        let program = family
            .select(&[])
            .expect("sole compiled family member")
            .program();
        assert_eq!(program.partition_parameter(), "workspace_id");
        let step = &program.steps()[0];
        assert!(matches!(
            step.access(),
            QueryAccessKind::Index { index, direction, .. }
                if index == expected.index && *direction == expected.direction
        ));
        assert!(
            step.predicates()
                .iter()
                .any(|predicate| predicate.operator() == expected.predicate),
            "{} lacks {:?}",
            expected.file,
            expected.predicate
        );
        assert_eq!(step.cursor_parameter().is_some(), expected.cursor);
        assert!(step.maximum_rows() <= 16);
        assert!(
            program
                .authorization()
                .iter()
                .any(|access| access.entity() == "CatalogItem")
        );
    }
}

#[test]
fn generic_relationship_shape_is_one_bounded_dependent_complete_key_batch() {
    let root = workspace_root().join("fixtures/riffql/operational-access-corpus-v1");
    let contract = fs::read_to_string(root.join("contract.riff")).expect("generic contract");
    let bundle = compile_contract_source(&contract).expect("generic contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = fs::read_to_string(root.join("queries/items_with_labels.riffq"))
        .expect("generic relationship query");
    let family = compile_operational_query_family(
        &parse_query(&source).expect("generic relationship query parses"),
        &catalog,
    )
    .expect("generic relationship query compiles");
    let program = family.select(&[]).expect("sole family member").program();
    assert_eq!(program.steps().len(), 2);
    assert!(matches!(
        program.steps()[1].access(),
        QueryAccessKind::DependentPointBatch {
            source_binding,
            source_field,
            ..
        } if source_binding == "items" && source_field == "label_id"
    ));
    assert_eq!(program.steps()[0].maximum_rows(), 16);
    assert_eq!(program.steps()[1].maximum_rows(), 16);
    assert_eq!(program.steps()[1].cursor_parameter(), None);
}

#[test]
fn generic_corpus_contains_no_external_framework_vocabulary() {
    let root = workspace_root().join("fixtures/riffql/operational-access-corpus-v1");
    let mut sources = vec![root.join("contract.riff")];
    sources.extend(
        fs::read_dir(root.join("queries"))
            .expect("generic query directory")
            .map(|entry| entry.expect("generic query entry").path()),
    );
    for path in sources {
        let source = fs::read_to_string(&path).expect("generic source");
        for forbidden in [
            "BetterAuth",
            "OpenFGA",
            "OAuth",
            "TupleState",
            "admin route",
            "adapter",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} contains external vocabulary {forbidden}",
                path.display()
            );
        }
    }
}
