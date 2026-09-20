#![forbid(unsafe_code)]

//! Contract variants for attributing per-write cost to one mechanism at a time.
//!
//! The ticketdesk benchmark contract declares no projection, no text-key index
//! and no tokenized index, so those subsystems have never been measured. This
//! crate builds one contract per mechanism from a single template, so a
//! measurement difference between two variants is attributable to the one
//! clause that differs rather than to two contracts that differ in many ways.

/// One mechanism whose per-write cost is being attributed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Mechanism {
    /// An event-derived projection maintained on commit.
    Projection,
    /// An exact byte-prefix text index over a bounded string field.
    TextKey,
    /// A declaration-only tokenized text index, which still maintains durable
    /// provider state on every write.
    TokenizedText,
    /// A declared vector field, which is how a contract asks for the columnar
    /// engine. This is the only mechanism here whose runtime is demand
    /// activated: WP-777 keeps the source cold until a projected query asks for
    /// it, so a measurement must wait for activation rather than assume it.
    Vector,
}

impl Mechanism {
    /// Every mechanism, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::Projection,
        Self::TextKey,
        Self::TokenizedText,
        Self::Vector,
    ];

    /// Stable name used in report rows and file names.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Projection => "projection",
            Self::TextKey => "text_key",
            Self::TokenizedText => "tokenized_text",
            Self::Vector => "vector",
        }
    }
}

/// The variants a run measures: a baseline carrying none of the mechanisms,
/// then the baseline plus exactly one of them.
#[must_use]
pub fn variants() -> Vec<(String, Vec<Mechanism>)> {
    let mut all = vec![("base".to_owned(), Vec::new())];
    for mechanism in Mechanism::ALL {
        all.push((mechanism.as_str().to_owned(), vec![mechanism]));
    }
    all.push(("all".to_owned(), Mechanism::ALL.to_vec()));
    all
}

/// Renders the contract carrying exactly the requested mechanisms.
///
/// Every variant keeps the same entities, aggregates, commands and event, so
/// the only difference between two renderings is the mechanism clauses. The
/// command always emits the event, whether or not a projection consumes it,
/// so the baseline pays the emit cost too and the projection variant isolates
/// projection maintenance rather than event emission.
#[must_use]
pub fn contract_source(mechanisms: &[Mechanism]) -> String {
    let has = |m: Mechanism| mechanisms.contains(&m);

    let text_key = if has(Mechanism::TextKey) {
        "\n    index by_title (workspace_id, title, document_id)\n      text_key(title, binary_utf8_v1)"
    } else {
        ""
    };

    let tokenized = if has(Mechanism::TokenizedText) {
        "\n    text_index search((title weight 4, body weight 1),\n      analyzer standard_v1,\n      staleness_slo 60,\n      replay_age_seconds 86400,\n      replay_bytes 1073741824,\n      replay_backlog 100000,\n      result boolean_v1,\n      max_terms 16,\n      max_candidates 10000,\n      max_results 1000)"
    } else {
        ""
    };

    let vector_field = if has(Mechanism::Vector) {
        "\n    vector_field embedding(4, cosine, (title, body), staleness_slo 60,\n      model \"perf-surface-v1\", current_version \"2026-09-20\",\n      replay_age_seconds 86400, replay_bytes 1073741824,\n      replay_backlog 100000)"
    } else {
        ""
    };

    // The embed statement and its inputs only exist when the field does, so the
    // base variant's command stays byte-identical to what it was before this
    // mechanism was added.
    let vector_inputs = if has(Mechanism::Vector) {
        "\n    input embedding: vector<4>\n    input submitted_model: string<256>\n    input submitted_version: string<256>"
    } else {
        ""
    };
    let vector_embed = if has(Mechanism::Vector) {
        "\n    embed document.embedding = embedding\n      from (submitted_model, submitted_version)"
    } else {
        ""
    };

    let projection = if has(Mechanism::Projection) {
        "\n  projection PublishedBytesDaily {\n    source event DocumentPublished\n    key (workspace_id, tx.date)\n    measure published_bytes = sum(size_bytes)\n    frontier transactionally_ordered\n  }\n"
    } else {
        ""
    };

    format!(
        r#"contract PerfSurface version 1 {{
  enum DocumentState {{ Draft, Published, Archived }}

  entity Workspace {{
    key (workspace_id: uuid)
    field name: string<64>
    field created_at: timestamp
  }}

  entity Document {{
    key (workspace_id: uuid, document_id: uuid)
    field title: string<200>
    field body: string<4096>
    field state: DocumentState
    field size_bytes: i64
    field created_at: timestamp

    index by_workspace (workspace_id, document_id){text_key}{tokenized}{vector_field}
  }}

  aggregate Documents {{
    root Document
    partition_by workspace_id
    conflict_key (workspace_id)
  }}

  aggregate Workspaces {{
    root Workspace
    partition_by workspace_id
    conflict_key (workspace_id)
  }}

  event DocumentPublished {{
    partition_by (workspace_id)
    workspace_id: uuid
    document_id: uuid
    state: DocumentState
    size_bytes: i64
  }}

  command CreateWorkspace {{
    input idempotency_key: string<128>
    input workspace_id: uuid
    input name: string<64>
    idempotency_key idempotency_key
    create Workspace(workspace_id) as workspace
      else WorkspaceExists {{ workspace_id: workspace_id }}
    set workspace.name = name
    set workspace.created_at = tx.time
    return Created {{ workspace: workspace }}
  }}

  command PublishDocument {{
    input idempotency_key: string<128>
    input workspace_id: uuid
    input document_id: uuid
    input title: string<200>
    input body: string<4096>
    input size_bytes: i64{vector_inputs}
    idempotency_key idempotency_key
    read Workspace(workspace_id) as workspace
      else WorkspaceMissing {{ workspace_id: workspace_id }}
    create Document(workspace_id, document_id) as document
      else DocumentExists {{ document_id: document_id }}
    set document.title = title
    set document.body = body
    set document.state = DocumentState.Published
    set document.size_bytes = size_bytes
    set document.created_at = tx.time{vector_embed}
    emit DocumentPublished {{
      workspace_id: workspace_id,
      document_id: document_id,
      state: DocumentState.Published,
      size_bytes: size_bytes
    }}
    return Published {{ document: document }}
  }}
{projection}}}
"#
    )
}

pub mod daemon;
pub mod measure;
pub mod session;
