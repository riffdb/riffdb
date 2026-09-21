#![forbid(unsafe_code)]
//! WP-803. What a deployment that adds a field to an existing entity does.
//!
//! ADR-0066's additive set is new enums, new entities, new aggregates, and
//! relationships confined to entities the same successor introduces. Adding a
//! field to an existing entity is not in it. That reads as a wall, and this
//! drives the comparison to find out whether it is one.
//!
//! It lives beside the catalog because deciding whether a successor may deploy
//! is the catalog's job -- `validate_successor_compatibility` runs exactly this
//! comparison and refuses an `Incompatible` report.
use riffdb_contract_ir::{CompatibilityClass, ContractCandidateV1, compare_successor};

const GENESIS: &str = r#"contract Live version 1 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    index by_org (org_id, document_id)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id)
  }
  command CreateDocument {
    input idempotency_key: string<128>
    input org_id: uuid
    input document_id: uuid
    input title: string<200>
    idempotency_key idempotency_key
    create Document(org_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    return Created { document: document }
  }
}"#;

/// The genesis contract with one ordinary field added, and the command
/// initialising it so the create binding stays complete.
const ADDS_A_FIELD: &str = r#"contract Live version 2 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    field body: string<4096>
    index by_org (org_id, document_id)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id)
  }
  command CreateDocument {
    input idempotency_key: string<128>
    input org_id: uuid
    input document_id: uuid
    input title: string<200>
    input body: string<4096>
    idempotency_key idempotency_key
    create Document(org_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    set document.body = body
    return Created { document: document }
  }
}"#;

/// The genesis contract with a field added and set from an input the command
/// already had, so no command surface changes.
const ADDS_A_FIELD_WITHOUT_A_NEW_INPUT: &str = r#"contract Live version 2 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    field subtitle: string<200>
    index by_org (org_id, document_id)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id)
  }
  command CreateDocument {
    input idempotency_key: string<128>
    input org_id: uuid
    input document_id: uuid
    input title: string<200>
    idempotency_key idempotency_key
    create Document(org_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    set document.subtitle = title
    return Created { document: document }
  }
}"#;

/// Compiles `successor` against the genesis contract and reports its overall
/// compatibility class, printing every finding so a failure shows which change
/// drove the class rather than only the class.
fn classify(successor: &str) -> CompatibilityClass {
    let parent =
        riffdb_contract_compiler::compile_contract_source(GENESIS).expect("genesis compiles");
    let candidate = riffdb_contract_compiler::compile_contract_successor(successor, &parent)
        .expect("successor compiles");
    let checked = ContractCandidateV1::new(
        candidate.schema(),
        candidate.commands(),
        candidate.projections(),
        candidate.mcp_command_names(),
    )
    .expect("checked candidate");
    let report = compare_successor(&parent, checked).expect("comparable successor");
    for entry in report.entries() {
        println!(
            "finding code={} class={:?} path={}",
            entry.code().as_str(),
            entry.class(),
            entry.affected_path()
        );
    }
    report.overall()
}

/// Adding a field and the command input to initialise it reports two findings:
/// the field is `RequiresMigration` (RDB-K030) and the input is `Incompatible`
/// (RDB-K111). The overall class is the more restrictive of the two, so what
/// refuses the deployment is the command surface, not the field.
#[test]
fn a_field_needing_a_new_command_input_is_incompatible() {
    let class = classify(ADDS_A_FIELD);
    assert_eq!(
        class,
        CompatibilityClass::Incompatible,
        "the new command input, not the new field, is what refuses"
    );
}

/// A field whose value comes from an input the command already had changes no
/// command surface, and is migratable rather than incompatible. So a field can
/// be added to a live application; what cannot is a field that needs the caller
/// to supply something new.
#[test]
fn a_field_needing_no_new_command_input_is_migratable() {
    assert_eq!(
        classify(ADDS_A_FIELD_WITHOUT_A_NEW_INPUT),
        CompatibilityClass::RequiresMigration,
        "the field addition alone is migratable"
    );
}

/// A vector field cannot even reach that classification, because the create
/// binding must initialise the new field and a vector field's value is embedded
/// rather than supplied. The refusal is a compile error, before compatibility
/// is consulted at all.
#[test]
fn a_vector_field_is_refused_before_compatibility_is_consulted() {
    let with_vector = ADDS_A_FIELD.replace(
        "    field body: string<4096>\n",
        "    field body: string<4096>\n    vector_field embedding(4, cosine, (title, body), \
         staleness_slo 60, model \"m\", current_version \"2026-09-21\", \
         replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000)\n",
    );
    let parent =
        riffdb_contract_compiler::compile_contract_source(GENESIS).expect("genesis compiles");
    let outcome = riffdb_contract_compiler::compile_contract_successor(&with_vector, &parent);
    let error = outcome.expect_err("a vector successor does not compile");
    let rendered = format!("{error:?}");
    assert!(
        rendered.contains("InvalidCreation"),
        "the create binding must initialise the new field, found {rendered}"
    );
}
