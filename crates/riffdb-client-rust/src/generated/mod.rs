//! Stable boundary for compiler-generated Rust command modules.

use std::error::Error;
use std::fmt;

use riffdb_proto::v1;
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
