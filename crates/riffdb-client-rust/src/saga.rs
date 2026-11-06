//! Typed client operations for compiled multi-step durable sagas (`ADR-0334`).

use std::collections::BTreeMap;
use std::time::Duration;

use crate::{
    ApplicationClientError, ApplicationCommand, ApplicationCommandResult, ApplicationValue,
    AttemptBudget, CallMetadata, RiffDbClient, StableApplicationClient,
    application::raise_application_command_result,
};

/// Durable lifecycle status of a compiled saga instance (`ADR-0334`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientSagaStatus {
    /// Forward execution is in progress or ready to advance.
    RunningForward,
    /// Reverse compensation is in progress in strict LIFO order.
    Compensating,
    /// Forward execution reached a terminal success branch.
    Completed,
    /// Forward execution aborted before any effectful step committed.
    AbortedClean,
    /// Forward execution explicitly transitioned to `abort_uncompensated`.
    AbortedUncompensated,
    /// All prior effectful forward steps have been compensated in reverse order.
    Compensated,
    /// Step execution encountered transient contention and is waiting for backoff or manual resume.
    BlockedRetryable,
    /// Step execution failed live capability/policy authorization and requires `resume_saga`.
    BlockedAuthority,
    /// A compensating command returned a non-success outcome and requires operator intervention.
    CompensationFailed,
}

impl ClientSagaStatus {
    /// Parses a canonical status string returned by the service.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running_forward" => Some(Self::RunningForward),
            "compensating" => Some(Self::Compensating),
            "completed" => Some(Self::Completed),
            "aborted_clean" => Some(Self::AbortedClean),
            "aborted_uncompensated" => Some(Self::AbortedUncompensated),
            "compensated" => Some(Self::Compensated),
            "blocked_retryable" => Some(Self::BlockedRetryable),
            "blocked_authority" => Some(Self::BlockedAuthority),
            "compensation_failed" => Some(Self::CompensationFailed),
            _ => None,
        }
    }

    /// Stable wire string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunningForward => "running_forward",
            Self::Compensating => "compensating",
            Self::Completed => "completed",
            Self::AbortedClean => "aborted_clean",
            Self::AbortedUncompensated => "aborted_uncompensated",
            Self::Compensated => "compensated",
            Self::BlockedRetryable => "blocked_retryable",
            Self::BlockedAuthority => "blocked_authority",
            Self::CompensationFailed => "compensation_failed",
        }
    }

    /// Whether this status is an immutable terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::AbortedClean | Self::AbortedUncompensated | Self::Compensated
        )
    }

    /// Whether this status is paused in a blocked or failed state.
    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(
            self,
            Self::BlockedRetryable | Self::BlockedAuthority | Self::CompensationFailed
        )
    }
}

/// Admission or observation disposition of a saga operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientSagaDisposition {
    /// A new saga instance was durably admitted.
    Admitted,
    /// An existing saga instance matched the caller's idempotency key and input.
    Replayed,
    /// An existing saga instance was read, awaited, cancelled, or resumed.
    Observed,
    /// No visible saga instance exists for the requested ID.
    NotFound,
}

impl ClientSagaDisposition {
    /// Parses a canonical disposition string.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "admitted" => Some(Self::Admitted),
            "replayed" => Some(Self::Replayed),
            "observed" => Some(Self::Observed),
            "not_found" => Some(Self::NotFound),
            _ => None,
        }
    }

    /// Stable wire string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Replayed => "replayed",
            Self::Observed => "observed",
            Self::NotFound => "not_found",
        }
    }
}

/// One committed forward or compensating step in a saga's durable step ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientCommittedSagaStep {
    /// Zero-based topological step index.
    pub step_index: u16,
    /// Declared step symbol.
    pub step_name: String,
    /// `"forward"` or `"compensate"`.
    pub direction: String,
    /// Compiled command name executed by the step.
    pub command_name: String,
    /// Numeric outcome variant ID.
    pub outcome_id: u32,
    /// Declared outcome variant name.
    pub outcome_name: String,
    /// `"effectful"` or `"no_op"`.
    pub effect_disposition: String,
    /// Deterministic UUIDv5 step idempotency key.
    pub step_idempotency_key: String,
}

/// Public client view of one durable saga instance (`ADR-0334`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientSagaInstanceView {
    /// Durable UUIDv7 saga identifier.
    pub saga_id: String,
    /// Compiled saga symbol.
    pub saga_name: String,
    /// Declared execution role.
    pub execution_role: String,
    /// Current durable lifecycle status.
    pub status: ClientSagaStatus,
    /// Admission or observation disposition.
    pub disposition: ClientSagaDisposition,
    /// Monotonic saga state revision.
    pub revision: u64,
    /// Monotonic worker fencing token.
    pub fencing_token: u64,
    /// Current step index in the compiled saga plan.
    pub current_step_index: u16,
    /// Whether explicit cancellation has been requested.
    pub cancel_requested: bool,
    /// Current step retry count.
    pub retry_count: u8,
    /// Total committed step count in the durable ledger.
    pub step_count: u64,
    /// Terminal outcome variant name when completed, aborted, or compensated.
    pub terminal_outcome_name: Option<String>,
    /// Diagnostic reason when blocked or failed.
    pub diagnostic_reason: Option<String>,
    /// Committed forward and compensating steps in execution order.
    pub step_ledger: Vec<ClientCommittedSagaStep>,
    /// Raw JSON array of committed step ledger entries.
    pub step_ledger_json: String,
    /// Raw JSON object of terminal outcome fields when present.
    pub terminal_outcome_json: String,
}

/// Result of starting or replaying a compiled saga.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientStartSagaResult {
    /// Whether the saga was newly admitted or replayed.
    pub disposition: ClientSagaDisposition,
    /// Current durable saga instance view.
    pub instance: ClientSagaInstanceView,
}

/// Decodes a [`ClientSagaInstanceView`] from an [`ApplicationCommandResult`] returned by saga dispatch.
pub fn decode_saga_command_result(
    result: &ApplicationCommandResult,
) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
    let Some(ApplicationValue::Record(fields)) = result.outcome_value.as_ref() else {
        return Err(ApplicationClientError::InvalidResponse);
    };
    let get_str = |key: &str| match fields.get(key) {
        Some(ApplicationValue::String(s)) => Ok(s.clone()),
        _ => Err(ApplicationClientError::InvalidResponse),
    };
    let get_opt_str = |key: &str| match fields.get(key) {
        Some(ApplicationValue::String(s)) => Ok(Some(s.clone())),
        Some(ApplicationValue::Null) | None => Ok(None),
        _ => Err(ApplicationClientError::InvalidResponse),
    };
    let get_u64 = |key: &str| match fields.get(key) {
        Some(ApplicationValue::U64(v)) => Ok(*v),
        Some(ApplicationValue::I64(v)) if *v >= 0 => Ok(*v as u64),
        _ => Err(ApplicationClientError::InvalidResponse),
    };
    let get_bool = |key: &str| match fields.get(key) {
        Some(ApplicationValue::Bool(v)) => Ok(*v),
        _ => Err(ApplicationClientError::InvalidResponse),
    };

    let disposition_str = get_str("disposition")?;
    let disposition = ClientSagaDisposition::parse(&disposition_str)
        .ok_or(ApplicationClientError::InvalidResponse)?;
    let status_str = get_str("status")?;
    if disposition == ClientSagaDisposition::NotFound || status_str == "not_found" {
        return Ok(None);
    }
    let status =
        ClientSagaStatus::parse(&status_str).ok_or(ApplicationClientError::InvalidResponse)?;
    let saga_id = get_str("saga_id")?;
    let saga_name = get_str("saga_name")?;
    let execution_role = get_str("execution_role")?;
    let revision = get_u64("revision")?;
    let fencing_token = get_u64("fencing_token")?;
    let current_step_index = u16::try_from(get_u64("current_step_index")?)
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let cancel_requested = get_bool("cancel_requested")?;
    let retry_count = u8::try_from(get_u64("retry_count")?)
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let step_count = get_u64("step_count")?;
    let terminal_outcome_name = get_opt_str("terminal_outcome_name")?;
    let diagnostic_reason = get_opt_str("diagnostic_reason")?;
    let step_ledger_json = get_str("step_ledger_json")?;
    let terminal_outcome_json = get_str("terminal_outcome_json")?;
    let step_ledger = parse_step_ledger_json(&step_ledger_json);

    Ok(Some(ClientSagaInstanceView {
        saga_id,
        saga_name,
        execution_role,
        status,
        disposition,
        revision,
        fencing_token,
        current_step_index,
        cancel_requested,
        retry_count,
        step_count,
        terminal_outcome_name,
        diagnostic_reason,
        step_ledger,
        step_ledger_json,
        terminal_outcome_json,
    }))
}

fn parse_step_ledger_json(json_str: &str) -> Vec<ClientCommittedSagaStep> {
    let trimmed = json_str.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') || trimmed.len() <= 2 {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut entries = Vec::new();
    let mut depth = 0_usize;
    let mut in_quotes = false;
    let mut escaped = false;
    let mut obj_start: Option<usize> = None;
    for (idx, ch) in inner.char_indices() {
        if in_quotes {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_quotes = false;
            }
            continue;
        }
        match ch {
            '"' => in_quotes = true,
            '{' => {
                if depth == 0 {
                    obj_start = Some(idx);
                }
                depth += 1;
            }
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0
                    && let Some(start) = obj_start.take()
                    && let Some(step) = parse_single_step_object(&inner[start..=idx])
                {
                    entries.push(step);
                }
            }
            _ => {}
        }
    }
    entries
}

fn parse_single_step_object(obj_str: &str) -> Option<ClientCommittedSagaStep> {
    let extract_str = |key: &str| -> Option<String> {
        let pattern = format!("\"{key}\":\"");
        let start = obj_str.find(&pattern)? + pattern.len();
        let rest = &obj_str[start..];
        let mut out = String::new();
        let mut escaped = false;
        for ch in rest.chars() {
            if escaped {
                out.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Some(out);
            } else {
                out.push(ch);
            }
        }
        None
    };
    let extract_u64 = |key: &str| -> Option<u64> {
        let pattern = format!("\"{key}\":");
        let start = obj_str.find(&pattern)? + pattern.len();
        let rest = obj_str[start..].trim_start();
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };

    Some(ClientCommittedSagaStep {
        step_index: u16::try_from(extract_u64("step_index")?).ok()?,
        step_name: extract_str("step_name")?,
        direction: extract_str("direction")?,
        command_name: extract_str("command_name")?,
        outcome_id: u32::try_from(extract_u64("outcome_id")?).ok()?,
        outcome_name: extract_str("outcome_name")?,
        effect_disposition: extract_str("effect_disposition")?,
        step_idempotency_key: extract_str("step_idempotency_key")?,
    })
}

impl StableApplicationClient {
    /// Starts or idempotently replays a compiled saga instance (`ADR-0334`).
    pub async fn start_saga(
        &mut self,
        saga_name: &str,
        saga_id: Option<&str>,
        mut input: BTreeMap<String, ApplicationValue>,
        await_window: Option<Duration>,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<ClientStartSagaResult, ApplicationClientError> {
        input.insert(
            "__saga_op".to_owned(),
            ApplicationValue::String("start".to_owned()),
        );
        if let Some(id) = saga_id {
            input.insert(
                "saga_id".to_owned(),
                ApplicationValue::String(id.to_owned()),
            );
        }
        if let Some(window) = await_window {
            let millis = u64::try_from(window.as_millis())
                .map_err(|_| ApplicationClientError::InvalidInput)?;
            input.insert("__await_millis".to_owned(), ApplicationValue::U64(millis));
        }
        let cmd = ApplicationCommand::new(saga_name, None, input)?;
        let res = self.execute_command(cmd, attempts, metadata).await?;
        let view =
            decode_saga_command_result(&res)?.ok_or(ApplicationClientError::InvalidResponse)?;
        Ok(ClientStartSagaResult {
            disposition: view.disposition,
            instance: view,
        })
    }

    /// Reads the current durable status and step ledger of a saga instance.
    pub async fn get_saga(
        &mut self,
        saga_name: Option<&str>,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "__saga_op".to_owned(),
            ApplicationValue::String("get".to_owned()),
        );
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd_name = saga_name.map_or_else(|| "__saga_get__".to_owned(), str::to_owned);
        let cmd = ApplicationCommand::new(cmd_name, None, input)?;
        let res = self.execute_command(cmd, attempts, metadata).await?;
        decode_saga_command_result(&res)
    }

    /// Awaits a saga instance for a bounded duration (`<= 30s`).
    pub async fn await_saga(
        &mut self,
        saga_name: Option<&str>,
        saga_id: &str,
        max_wait: Duration,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let millis = u64::try_from(max_wait.as_millis())
            .map_err(|_| ApplicationClientError::InvalidInput)?;
        let mut input = BTreeMap::new();
        input.insert(
            "__saga_op".to_owned(),
            ApplicationValue::String("await".to_owned()),
        );
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        input.insert("__await_millis".to_owned(), ApplicationValue::U64(millis));
        let cmd_name = saga_name.map_or_else(|| "__saga_await__".to_owned(), str::to_owned);
        let cmd = ApplicationCommand::new(cmd_name, None, input)?;
        let res = self.execute_command(cmd, attempts, metadata).await?;
        decode_saga_command_result(&res)
    }

    /// Requests explicit cancellation and reverse LIFO compensation of a saga instance.
    pub async fn cancel_saga(
        &mut self,
        saga_name: Option<&str>,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "__saga_op".to_owned(),
            ApplicationValue::String("cancel".to_owned()),
        );
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd_name = saga_name.map_or_else(|| "__saga_cancel__".to_owned(), str::to_owned);
        let cmd = ApplicationCommand::new(cmd_name, None, input)?;
        let res = self.execute_command(cmd, attempts, metadata).await?;
        decode_saga_command_result(&res)
    }

    /// Resumes a blocked or failed saga instance with fresh capability authority.
    pub async fn resume_saga(
        &mut self,
        saga_name: Option<&str>,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "__saga_op".to_owned(),
            ApplicationValue::String("resume".to_owned()),
        );
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd_name = saga_name.map_or_else(|| "__saga_resume__".to_owned(), str::to_owned);
        let cmd = ApplicationCommand::new(cmd_name, None, input)?;
        let res = self.execute_command(cmd, attempts, metadata).await?;
        decode_saga_command_result(&res)
    }
}

impl RiffDbClient {
    /// Starts or idempotently replays a compiled saga instance (`ADR-0334`).
    pub async fn start_saga(
        &mut self,
        saga_name: &str,
        saga_id: Option<&str>,
        mut input: BTreeMap<String, ApplicationValue>,
        await_window: Option<Duration>,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<ClientStartSagaResult, ApplicationClientError> {
        if let Some(id) = saga_id {
            input.insert(
                "saga_id".to_owned(),
                ApplicationValue::String(id.to_owned()),
            );
        }
        if let Some(window) = await_window {
            let millis = u64::try_from(window.as_millis())
                .map_err(|_| ApplicationClientError::InvalidInput)?;
            input.insert("__await_millis".to_owned(), ApplicationValue::U64(millis));
        }
        let cmd = ApplicationCommand::new(format!("__saga_start__{saga_name}"), None, input)?;
        let idempotent = cmd.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&idempotent, attempts, metadata)
            .await
            .map_err(ApplicationClientError::Client)?;
        let raised = raise_application_command_result(response)?;
        let view =
            decode_saga_command_result(&raised)?.ok_or(ApplicationClientError::InvalidResponse)?;
        Ok(ClientStartSagaResult {
            disposition: view.disposition,
            instance: view,
        })
    }

    /// Reads the current durable status and step ledger of a saga instance.
    pub async fn get_saga(
        &mut self,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd = ApplicationCommand::new("__saga_get__", None, input)?;
        let idempotent = cmd.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&idempotent, attempts, metadata)
            .await
            .map_err(ApplicationClientError::Client)?;
        let raised = raise_application_command_result(response)?;
        decode_saga_command_result(&raised)
    }

    /// Awaits a saga instance for a bounded duration (`<= 30s`).
    pub async fn await_saga(
        &mut self,
        saga_id: &str,
        max_wait: Duration,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let millis = u64::try_from(max_wait.as_millis())
            .map_err(|_| ApplicationClientError::InvalidInput)?;
        let mut input = BTreeMap::new();
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        input.insert("__await_millis".to_owned(), ApplicationValue::U64(millis));
        let cmd = ApplicationCommand::new("__saga_await__", None, input)?;
        let idempotent = cmd.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&idempotent, attempts, metadata)
            .await
            .map_err(ApplicationClientError::Client)?;
        let raised = raise_application_command_result(response)?;
        decode_saga_command_result(&raised)
    }

    /// Requests explicit cancellation and reverse LIFO compensation of a saga instance.
    pub async fn cancel_saga(
        &mut self,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd = ApplicationCommand::new("__saga_cancel__", None, input)?;
        let idempotent = cmd.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&idempotent, attempts, metadata)
            .await
            .map_err(ApplicationClientError::Client)?;
        let raised = raise_application_command_result(response)?;
        decode_saga_command_result(&raised)
    }

    /// Resumes a blocked or failed saga instance with fresh capability authority.
    pub async fn resume_saga(
        &mut self,
        saga_id: &str,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Option<ClientSagaInstanceView>, ApplicationClientError> {
        let mut input = BTreeMap::new();
        input.insert(
            "saga_id".to_owned(),
            ApplicationValue::String(saga_id.to_owned()),
        );
        let cmd = ApplicationCommand::new("__saga_resume__", None, input)?;
        let idempotent = cmd.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&idempotent, attempts, metadata)
            .await
            .map_err(ApplicationClientError::Client)?;
        let raised = raise_application_command_result(response)?;
        decode_saga_command_result(&raised)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001
    #[test]
    fn decode_saga_command_result_parses_completed_and_compensated_views_and_step_ledger() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "saga_id".to_owned(),
            ApplicationValue::String("018f0000-0000-7000-8000-010203040506".to_owned()),
        );
        fields.insert(
            "saga_name".to_owned(),
            ApplicationValue::String("CheckoutOrder".to_owned()),
        );
        fields.insert(
            "execution_role".to_owned(),
            ApplicationValue::String("OrchestratorRole".to_owned()),
        );
        fields.insert(
            "status".to_owned(),
            ApplicationValue::String("compensated".to_owned()),
        );
        fields.insert(
            "disposition".to_owned(),
            ApplicationValue::String("admitted".to_owned()),
        );
        fields.insert("revision".to_owned(), ApplicationValue::U64(4));
        fields.insert("fencing_token".to_owned(), ApplicationValue::U64(1));
        fields.insert("current_step_index".to_owned(), ApplicationValue::U64(1));
        fields.insert("cancel_requested".to_owned(), ApplicationValue::Bool(false));
        fields.insert("retry_count".to_owned(), ApplicationValue::U64(0));
        fields.insert("step_count".to_owned(), ApplicationValue::U64(2));
        fields.insert(
            "terminal_outcome_name".to_owned(),
            ApplicationValue::String("Compensated".to_owned()),
        );
        fields.insert("diagnostic_reason".to_owned(), ApplicationValue::Null);
        fields.insert(
            "step_ledger_json".to_owned(),
            ApplicationValue::String(
                "[{\"step_index\":0,\"step_name\":\"reserve\",\"direction\":\"forward\",\"command_name\":\"ReserveInventory\",\"outcome_id\":1,\"outcome_name\":\"Reserved\",\"effect_disposition\":\"effectful\",\"step_idempotency_key\":\"saga:018f:f:0:ReserveInventory\"},{\"step_index\":0,\"step_name\":\"reserve\",\"direction\":\"compensating\",\"command_name\":\"ReleaseInventory\",\"outcome_id\":2,\"outcome_name\":\"Released\",\"effect_disposition\":\"effectful\",\"step_idempotency_key\":\"saga:018f:c:0:ReleaseInventory\"}]"
                    .to_owned(),
            ),
        );
        fields.insert(
            "terminal_outcome_json".to_owned(),
            ApplicationValue::String("{}".to_owned()),
        );

        let result = ApplicationCommandResult {
            outcome: Some("SagaStatus".to_owned()),
            outcome_value: Some(ApplicationValue::Record(fields)),
            commit_sequence: None,
            contract_version: 1,
            plan_hash: [0u8; 32],
            replayed: false,
            outcome_uri: None,
        };
        let view = decode_saga_command_result(&result)
            .expect("decode ok")
            .expect("some view");
        assert_eq!(view.saga_name, "CheckoutOrder");
        assert_eq!(view.status, ClientSagaStatus::Compensated);
        assert_eq!(view.disposition, ClientSagaDisposition::Admitted);
        assert!(view.status.is_terminal());
        assert_eq!(view.revision, 4);
        assert_eq!(view.step_count, 2);
        assert_eq!(view.step_ledger.len(), 2);
        assert_eq!(view.step_ledger[0].direction, "forward");
        assert_eq!(view.step_ledger[1].direction, "compensating");
    }
}
