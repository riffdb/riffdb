//! Immutable same-input command submissions and bounded retry state.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use riffdb_proto::v1;
use riffdb_proto::{MAX_PROTOCOL_NAME_BYTES, validate_value};
use riffdb_types::RequestId;

use crate::status::{ClientError, OutcomeUnknown, carries_uncertainty, is_retryable};

/// A caller-selected nonzero bound on total transport submissions.
///
/// RiffDB defines no default attempt count or backoff policy in the POC. This
/// value bounds immediate same-input recovery without inventing either.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptBudget(NonZeroU32);

impl AttemptBudget {
    /// Constructs a total submission bound.
    #[must_use]
    pub const fn new(maximum_submissions: u32) -> Option<Self> {
        match NonZeroU32::new(maximum_submissions) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the maximum number of transport submissions.
    #[must_use]
    pub const fn maximum_submissions(self) -> u32 {
        self.0.get()
    }
}

/// A safe local error while constructing an immutable command submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandShapeError {
    /// The command name is empty or exceeds the protocol-name bound.
    InvalidCommandName,
    /// An explicitly selected contract version is zero.
    InvalidContractVersion,
    /// The command input is not a structurally valid record value.
    InvalidCommandInput,
}

impl fmt::Display for CommandShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommandName => "command name is invalid",
            Self::InvalidContractVersion => "contract version is invalid",
            Self::InvalidCommandInput => "command input is invalid",
        })
    }
}

impl Error for CommandShapeError {}

/// One immutable idempotent command used by every automatic retry attempt.
///
/// Generic transport code retains the opaque input byte-for-byte but cannot
/// identify or validate the contract field carrying an idempotency key.
/// Generated command modules bind their typed key to both execution input and
/// explicit outcome recovery. The server remains authoritative for schema and
/// idempotency validation.
#[derive(Clone, PartialEq)]
pub struct IdempotentCommand {
    command_name: String,
    expected_contract_version: Option<u64>,
    input: v1::Value,
}

impl fmt::Debug for IdempotentCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotentCommand([REDACTED])")
    }
}

impl IdempotentCommand {
    /// Constructs one immutable generic command submission.
    pub fn new(
        command_name: impl Into<String>,
        expected_contract_version: Option<u64>,
        input: v1::Value,
    ) -> Result<Self, CommandShapeError> {
        let command_name = command_name.into();
        if command_name.is_empty() || command_name.len() > MAX_PROTOCOL_NAME_BYTES {
            return Err(CommandShapeError::InvalidCommandName);
        }
        if expected_contract_version == Some(0) {
            return Err(CommandShapeError::InvalidContractVersion);
        }
        if validate_value(&input).is_err()
            || !matches!(input.kind.as_ref(), Some(v1::value::Kind::RecordValue(_)))
        {
            return Err(CommandShapeError::InvalidCommandInput);
        }
        Ok(Self {
            command_name,
            expected_contract_version,
            input,
        })
    }

    /// Borrows the exact command name reused by every attempt.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }

    /// Returns the explicit contract version, when one was selected.
    #[must_use]
    pub const fn expected_contract_version(&self) -> Option<u64> {
        self.expected_contract_version
    }

    /// Borrows the exact input cloned for every attempt.
    #[must_use]
    pub const fn input(&self) -> &v1::Value {
        &self.input
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::ExecuteCommandRequest {
        v1::ExecuteCommandRequest {
            request_id: request_id.into_bytes().to_vec(),
            command_name: self.command_name.clone(),
            expected_contract_version: self.expected_contract_version,
            input: Some(self.input.clone()),
        }
    }
}

pub(crate) struct RetryState {
    remaining_submissions: u32,
    unresolved_uncertainty: bool,
}

impl RetryState {
    pub(crate) const fn new(budget: AttemptBudget) -> Self {
        Self {
            remaining_submissions: budget.maximum_submissions(),
            unresolved_uncertainty: false,
        }
    }

    pub(crate) fn begin_submission(&mut self) -> bool {
        let Some(remaining) = self.remaining_submissions.checked_sub(1) else {
            return false;
        };
        self.remaining_submissions = remaining;
        true
    }

    pub(crate) fn request_id_failure(
        &self,
        error: crate::IdentifierGenerationError,
    ) -> ClientError {
        if self.unresolved_uncertainty {
            ClientError::OutcomeUnknown(OutcomeUnknown)
        } else {
            ClientError::IdentifierGeneration(error)
        }
    }

    pub(crate) fn handle_failure(&mut self, error: ClientError) -> RetryDecision {
        self.unresolved_uncertainty |= carries_uncertainty(&error);
        if !is_retryable(&error) {
            return RetryDecision::Return(if self.unresolved_uncertainty {
                ClientError::OutcomeUnknown(OutcomeUnknown)
            } else {
                error
            });
        }
        if self.remaining_submissions > 0 {
            return RetryDecision::Retry;
        }
        if self.unresolved_uncertainty {
            RetryDecision::Return(ClientError::OutcomeUnknown(OutcomeUnknown))
        } else {
            RetryDecision::Return(error)
        }
    }
}

pub(crate) enum RetryDecision {
    Retry,
    Return(ClientError),
}

#[cfg(test)]
mod tests {
    use riffdb_errors::{PublicError, RecoveryAction};

    use super::*;
    use crate::status::DetailsFreeStatus;

    fn record_input() -> v1::Value {
        v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: vec![v1::ValueField {
                    field_id: Some(1),
                    name: String::new(),
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::StringValue("same-key".to_owned())),
                    }),
                }],
            })),
        }
    }

    #[test]
    fn every_request_changes_only_the_outer_request_id() {
        let command =
            IdempotentCommand::new("CreateBudget", Some(7), record_input()).expect("command");
        let first_id = RequestId::from_unix_milliseconds_and_random(1, [1; 10]).expect("ID");
        let second_id = RequestId::from_unix_milliseconds_and_random(2, [2; 10]).expect("ID");
        let first = command.request(first_id);
        let second = command.request(second_id);

        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.command_name, second.command_name);
        assert_eq!(
            first.expected_contract_version,
            second.expected_contract_version
        );
        assert_eq!(first.input, second.input);
    }

    #[test]
    fn transport_uncertainty_exhaustion_is_explicit_outcome_unknown() {
        let mut state = RetryState::new(AttemptBudget::new(2).expect("budget"));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Retry
        ));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Return(ClientError::OutcomeUnknown(_))
        ));
    }

    #[test]
    fn exhausted_known_retry_error_preserves_the_checked_public_error() {
        let error = PublicError::concurrency_deadline_exceeded();
        assert_eq!(error.recovery_action(), RecoveryAction::Retry);
        let mut state = RetryState::new(AttemptBudget::new(1).expect("budget"));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::Public(error)),
            RetryDecision::Return(ClientError::Public(returned))
                if returned == PublicError::concurrency_deadline_exceeded()
        ));
    }

    #[test]
    fn identifier_failure_after_uncertainty_does_not_hide_possible_commit() {
        let mut state = RetryState::new(AttemptBudget::new(2).expect("budget"));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Retry
        ));
        assert!(state.begin_submission());
        assert!(matches!(
            state.request_id_failure(crate::IdentifierGenerationError::EntropyUnavailable),
            ClientError::OutcomeUnknown(_)
        ));
    }

    #[test]
    fn later_terminal_error_does_not_hide_an_earlier_uncertain_submission() {
        let mut state = RetryState::new(AttemptBudget::new(2).expect("budget"));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Retry
        ));
        assert!(state.begin_submission());
        assert!(matches!(
            state.handle_failure(ClientError::Public(PublicError::authorization_denied())),
            RetryDecision::Return(ClientError::OutcomeUnknown(_))
        ));
    }
}
