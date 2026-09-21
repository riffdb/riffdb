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

/// The genesis contract with an explicitly nullable field added and no command
/// change at all, since an optional field needs no initialisation.
const ADDS_AN_OPTIONAL_FIELD: &str = r#"contract Live version 2 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    field body: optional<string<4096>>
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

/// The field-adding successor with a production vector field alongside.
fn vector_successor_source() -> String {
    ADDS_A_FIELD.replace(
        "    field body: string<4096>\n",
        "    field body: string<4096>\n    vector_field embedding(4, cosine, (title, body), \
         staleness_slo 60, model \"m\", current_version \"2026-09-21\", \
         replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000)\n",
    )
}

/// A vector field added alongside a whole new command that initialises it,
/// leaving the original command untouched. A successor may introduce a command
/// freely, so if anything rescues a vector field on a live application this is
/// it.
const ADDS_A_VECTOR_VIA_A_NEW_COMMAND: &str = r#"contract Live version 2 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    vector_field embedding(4, cosine, (title), staleness_slo 60,
      model "m", current_version "2026-09-21",
      replay_age_seconds 86400, replay_bytes 1073741824,
      replay_backlog 100000)
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
  command CreateDocumentWithEmbedding {
    input idempotency_key: string<128>
    input org_id: uuid
    input document_id: uuid
    input title: string<200>
    input embedding: vector<4>
    input submitted_model: string<256>
    input submitted_version: string<256>
    idempotency_key idempotency_key
    create Document(org_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    embed document.embedding = embedding
      from (submitted_model, submitted_version)
    return Created { document: document }
  }
}"#;

/// An optional field added to an existing command's input record, alongside an
/// optional entity field it sets. Nothing here is required.
const ADDS_AN_OPTIONAL_INPUT: &str = r#"contract Live version 2 {
  entity Document {
    key (org_id: uuid, document_id: uuid)
    field title: string<200>
    field note: optional<string<64>>
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
    input note: optional<string<64>>
    idempotency_key idempotency_key
    create Document(org_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    set document.note = note
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

/// An optional field needs no initialisation, so it needs no command change,
/// so nothing in the successor touches the frozen command surface.
#[test]
fn an_optional_field_needs_no_command_change() {
    let class = classify(ADDS_AN_OPTIONAL_FIELD);
    assert_ne!(
        class,
        CompatibilityClass::Incompatible,
        "an optional field changes no command surface, so nothing refuses it"
    );
}

/// An existing command's input list is not frozen, and the finding that
/// refuses is not about the input at all.
///
/// The optional input reports `RDB-K013`, `Compatible` -- the same code an
/// optional entity field gets. What refuses is `RDB-K110`, an existing plan
/// change, because the command gained the instruction that uses the input.
///
/// This is deliberately asserted at the level of findings rather than the
/// overall class. "Adding an optional input is incompatible" would be the
/// wrong summary of a run whose input finding is Compatible, and the earlier
/// draft of this file said exactly that about required inputs.
#[test]
fn an_optional_input_is_compatible_and_the_plan_change_is_what_refuses() {
    let parent =
        riffdb_contract_compiler::compile_contract_source(GENESIS).expect("genesis compiles");
    let candidate =
        riffdb_contract_compiler::compile_contract_successor(ADDS_AN_OPTIONAL_INPUT, &parent)
            .expect("successor compiles");
    let checked = ContractCandidateV1::new(
        candidate.schema(),
        candidate.commands(),
        candidate.projections(),
        candidate.mcp_command_names(),
    )
    .expect("checked candidate");
    let report = compare_successor(&parent, checked).expect("comparable successor");

    let input_finding = report
        .entries()
        .iter()
        .find(|entry| entry.affected_path().contains("/input/"))
        .expect("a finding about the added input");
    assert_eq!(
        input_finding.class(),
        CompatibilityClass::Compatible,
        "an optional input is a compatible addition"
    );
    assert!(
        report
            .entries()
            .iter()
            .any(|entry| entry.code().as_str() == "RDB-K110"),
        "what refuses is the plan change that uses it"
    );
}

/// A new command does not rescue it, and the reason is the old command.
///
/// A successor may introduce a command freely, so the new one is not what
/// refuses. The original `CreateDocument` still creates a `Document`, and that
/// entity now has a required vector field it does not initialise, so the
/// binding it always had stops compiling. Adding a required field breaks every
/// existing command that creates the entity, whatever else the successor does.
#[test]
fn a_new_command_does_not_rescue_a_vector_field() {
    let parent =
        riffdb_contract_compiler::compile_contract_source(GENESIS).expect("genesis compiles");
    let outcome = riffdb_contract_compiler::compile_contract_successor(
        ADDS_A_VECTOR_VIA_A_NEW_COMMAND,
        &parent,
    );
    let rendered = format!(
        "{:?}",
        outcome.expect_err("a new command does not make the old one compile")
    );
    assert!(
        rendered.contains("InvalidCreation"),
        "the untouched command's create binding is what refuses, found {rendered}"
    );
}

/// The migration path does not relax the create binding either.
///
/// `compile_contract_migration_successor` compiles the candidate through the
/// same path a plain successor takes, with identity renames bound first, and a
/// rename cannot make a required field initialised. So a vector field cannot be
/// introduced by a migration any more than by a deploy -- which makes WP-803's
/// third question moot: a migration cannot bring a columnar source into
/// existence, so any source present after one was admitted at the deploy that
/// first declared it.
#[test]
fn a_migration_cannot_introduce_a_vector_field_either() {
    let with_vector = vector_successor_source();
    let parent =
        riffdb_contract_compiler::compile_contract_source(GENESIS).expect("genesis compiles");
    let migration = r#"
migration Evolution from 1 to 2 {
  transform Document {
    set title = old.title
  }
}
"#;
    let outcome = riffdb_contract_compiler::compile_contract_migration_successor(
        &with_vector,
        migration,
        &parent,
    );
    let error = outcome.err().map(|error| format!("{error:?}"));
    let rendered = error.expect("a vector successor does not compile as a migration either");
    assert!(
        rendered.contains("InvalidCreation"),
        "the create binding still refuses under migration, found {rendered}"
    );
}

/// A vector field cannot even reach that classification, because the create
/// binding must initialise the new field and a vector field's value is embedded
/// rather than supplied. The refusal is a compile error, before compatibility
/// is consulted at all.
#[test]
fn a_vector_field_is_refused_before_compatibility_is_consulted() {
    let with_vector = vector_successor_source();
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
