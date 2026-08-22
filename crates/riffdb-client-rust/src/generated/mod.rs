//! Stable boundary for compiler-generated Rust command modules.

use std::error::Error;
use std::fmt;

use riffdb_proto::{app::v1 as app_v1, v1};
use riffdb_types::RequestId;

use crate::{
    ApplicationClientError, ApplicationEvent, ApplicationEventConsumer, ApplicationLiveQueryUpdate,
    ApplicationReactiveOperation, CommandShapeError, IdempotentCommand, NamedQuery,
    NamedQueryResult, QueryOptions,
};

/// One exact generated durable event-stream selection and decoder.
pub trait GeneratedEventConsumer {
    /// Closed generated event union.
    type Event;

    /// Constructs the exact immutable consumer selection.
    fn event_consumer(self) -> Result<ApplicationEventConsumer, ApplicationClientError>;

    /// Decodes one already authorized selected event by symbolic names only.
    fn decode_event(event: ApplicationEvent) -> Result<Self::Event, ApplicationClientError>;
}

/// One exact generated live named-query watch and closed update decoder.
pub trait GeneratedLiveQuery {
    /// Closed generated update union.
    type Update;

    /// Constructs the exact immutable watch operation.
    fn live_operation(self) -> Result<ApplicationReactiveOperation, ApplicationClientError>;

    /// Decodes one structurally checked application-safe update.
    fn decode_update(
        update: ApplicationLiveQueryUpdate,
    ) -> Result<Self::Update, ApplicationClientError>;
}

/// One exact named query shape emitted from an immutable query module.
pub trait GeneratedQuery {
    /// The generated declared-result union.
    type Output;

    /// Constructs the exact module-pinned symbolic request.
    fn named_query(self, options: QueryOptions) -> Result<NamedQuery, ApplicationClientError>;

    /// Decodes one identity-checked name-addressed response.
    fn decode_result(response: NamedQueryResult) -> Result<Self::Output, ApplicationClientError>;

    /// Decodes the compiler-sealed positional arm without constructing generic
    /// per-row maps. Only cover-eligible generated operations override this.
    fn decode_compact_result(
        _outcome: String,
        _response: app_v1::CompactResultField,
    ) -> Result<Self::Output, ApplicationClientError> {
        Err(ApplicationClientError::InvalidResponse)
    }

    /// Decodes the compiler-sealed canonical-column arm directly into the
    /// generated result without constructing generic value or row maps.
    fn decode_packed_result(
        _outcome: String,
        _response: app_v1::PackedResultField,
    ) -> Result<Self::Output, ApplicationClientError> {
        Err(ApplicationClientError::InvalidResponse)
    }
}

/// One command shape emitted from a checked contract bundle.
///
/// Implementations contain only value-shape ergonomics. The server remains
/// authoritative for schema validation and every database guarantee.
pub trait GeneratedCommand {
    /// The generated declared-outcome enum.
    type Outcome;

    /// Constructs the immutable generic transport command.
    fn idempotent_command(&self) -> Result<IdempotentCommand, GeneratedCommandError>;

    /// Constructs same-key outcome recovery from this typed command input.
    ///
    /// The generated implementation owns the schema binding between the
    /// command input's idempotency field and this recovery request.
    fn outcome_request(
        &self,
        request_id: RequestId,
    ) -> Result<v1::GetOutcomeRequest, GeneratedCommandError>;

    /// Decodes a structurally checked Execute response into the generated outcome.
    fn decode_outcome(
        &self,
        response: &v1::ExecuteCommandResponse,
    ) -> Result<Self::Outcome, GeneratedCommandError>;

    /// Derives compiler-owned successor revisions for successful checked
    /// workflow mutations.
    ///
    /// Ordinary commands return no revisions. Generated workflow bindings
    /// override this method and derive each successor from the exact observed
    /// revision that the server accepted. The response outcome gates the
    /// derivation, so stale or illegal business outcomes never manufacture a
    /// revision.
    fn workflow_successor_revisions(
        &self,
        _response: &v1::ExecuteCommandResponse,
    ) -> Result<Vec<WorkflowSuccessorRevision>, GeneratedCommandError> {
        Ok(Vec::new())
    }
}

/// Successor revision produced by one checked workflow binding mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowSuccessorRevision {
    binding: &'static str,
    revision: u64,
}

impl WorkflowSuccessorRevision {
    /// Constructs compiler-generated successor evidence.
    #[must_use]
    pub const fn generated(binding: &'static str, revision: u64) -> Self {
        Self { binding, revision }
    }

    /// Exact source binding whose entity revision advanced.
    #[must_use]
    pub const fn binding(&self) -> &'static str {
        self.binding
    }

    /// Exact successor revision after the successful command.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// A closed failure in generated shape-only code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedCommandError {
    /// Typed input could not form the frozen generic command envelope.
    InvalidInputShape,
    /// A successful response did not match the generated declared-outcome union.
    InvalidOutcomeShape,
}

impl From<CommandShapeError> for GeneratedCommandError {
    fn from(_: CommandShapeError) -> Self {
        Self::InvalidInputShape
    }
}

impl fmt::Display for GeneratedCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInputShape => "generated command input is invalid",
            Self::InvalidOutcomeShape => "generated command outcome is invalid",
        })
    }
}

impl Error for GeneratedCommandError {}

/// Generated ergonomic bindings for the canonical POC contract.
pub mod legal_spend;
