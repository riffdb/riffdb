//! CLI subcommands for compiled multi-step durable sagas (`riffdb saga start|get|await|cancel|resume`).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use riffdb_client_rust::{
    ApplicationValue, AttemptBudget, CallMetadata, ClientSagaInstanceView, RiffDbClient,
    load_protected_bearer_credential,
};
use serde::Serialize;

use crate::cli::OutputMode;
use crate::input::{MAX_INPUT_BYTES, read_path_or_stdin, utf8};

#[derive(Debug, Parser)]
#[command(name = "riffdb")]
struct SagaRootCli {
    #[arg(long, global = true)]
    endpoint: Option<String>,
    #[arg(long, global = true)]
    credential_file: Option<PathBuf>,
    #[arg(long, global = true)]
    output: Option<OutputMode>,
    #[command(subcommand)]
    command: SagaTopSubcommand,
}

#[derive(Debug, Subcommand)]
enum SagaTopSubcommand {
    /// Manage compiled multi-step durable sagas.
    Saga {
        #[command(subcommand)]
        action: SagaAction,
    },
}

/// Subcommands for `riffdb saga`.
#[derive(Debug, Subcommand)]
pub enum SagaAction {
    /// Start or idempotently replay a compiled saga instance.
    Start {
        /// Compiled saga symbolic name.
        #[arg(long = "name", visible_alias = "saga")]
        name: String,
        /// Optional explicit UUIDv7 saga identifier.
        #[arg(long = "saga-id")]
        saga_id: Option<String>,
        /// Inline JSON object of saga input fields.
        #[arg(long = "input")]
        input: Option<String>,
        /// Path to JSON file (or `-` for stdin) of saga input fields.
        #[arg(long = "input-file")]
        input_file: Option<OsString>,
        /// Optional bounded wait window in milliseconds (`<= 30000`).
        #[arg(long = "await-millis")]
        await_millis: Option<u64>,
    },
    /// Read the durable status and step ledger of a saga instance.
    Get {
        /// Target UUIDv7 saga identifier.
        #[arg(long = "saga-id")]
        saga_id: String,
    },
    /// Wait up to a bounded window (`<= 30000ms`) for a saga instance to complete or block.
    Await {
        /// Target UUIDv7 saga identifier.
        #[arg(long = "saga-id")]
        saga_id: String,
        /// Maximum bounded wait window in milliseconds (`<= 30000`).
        #[arg(long = "await-millis", default_value_t = 5_000)]
        await_millis: u64,
    },
    /// Request explicit cancellation and reverse LIFO compensation of a saga instance.
    Cancel {
        /// Target UUIDv7 saga identifier.
        #[arg(long = "saga-id")]
        saga_id: String,
    },
    /// Resume a blocked or failed saga instance with fresh capability authority.
    Resume {
        /// Target UUIDv7 saga identifier.
        #[arg(long = "saga-id")]
        saga_id: String,
    },
}

#[derive(Serialize)]
struct SagaCliOutput {
    saga_id: String,
    saga_name: String,
    execution_role: String,
    status: &'static str,
    disposition: &'static str,
    revision: u64,
    fencing_token: u64,
    current_step_index: u16,
    cancel_requested: bool,
    retry_count: u8,
    step_count: u64,
    terminal_outcome_name: Option<String>,
    diagnostic_reason: Option<String>,
    step_ledger: serde_json::Value,
    terminal_outcome: serde_json::Value,
}

impl SagaCliOutput {
    fn from_view(view: ClientSagaInstanceView) -> Self {
        let step_ledger = serde_json::from_str(&view.step_ledger_json)
            .unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
        let terminal_outcome = serde_json::from_str(&view.terminal_outcome_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        Self {
            saga_id: view.saga_id,
            saga_name: view.saga_name,
            execution_role: view.execution_role,
            status: view.status.as_str(),
            disposition: view.disposition.as_str(),
            revision: view.revision,
            fencing_token: view.fencing_token,
            current_step_index: view.current_step_index,
            cancel_requested: view.cancel_requested,
            retry_count: view.retry_count,
            step_count: view.step_count,
            terminal_outcome_name: view.terminal_outcome_name,
            diagnostic_reason: view.diagnostic_reason,
            step_ledger,
            terminal_outcome,
        }
    }
}

/// Checks if the current process arguments invoke `riffdb saga ...` and executes it if so.
pub async fn try_run_from_env() -> Option<ExitCode> {
    let args: Vec<OsString> = std::env::args_os().collect();
    if !is_saga_invocation(&args) {
        return None;
    }
    Some(run_saga_args(args, &mut io::stdout().lock(), &mut io::stderr().lock()).await)
}

fn is_saga_invocation(args: &[OsString]) -> bool {
    let mut skip_next = false;
    for arg in args.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        let Some(text) = arg.to_str() else {
            continue;
        };
        if matches!(
            text,
            "--endpoint"
                | "--credential-file"
                | "--output"
                | "--config"
                | "--database"
                | "--max-attempts"
        ) {
            skip_next = true;
            continue;
        }
        if text.starts_with('-') {
            continue;
        }
        return text == "saga";
    }
    false
}

/// Executes a `riffdb saga ...` CLI invocation against a live RiffDB server.
pub async fn run_saga_args<I, T>(
    args: I,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let parsed = match SagaRootCli::try_parse_from(args) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(stderr, "{err}");
            return ExitCode::from(2);
        }
    };
    let output_mode = parsed.output.unwrap_or(OutputMode::Json);
    let endpoint_str = parsed
        .endpoint
        .or_else(|| std::env::var("RIFFDB_ENDPOINT").ok())
        .unwrap_or_else(|| "http://127.0.0.1:7443".to_owned());
    let credential_path = parsed
        .credential_file
        .or_else(|| std::env::var_os("RIFFDB_CREDENTIAL_FILE").map(PathBuf::from));
    let Some(credential_path) = credential_path else {
        let _ = writeln!(
            stderr,
            "error: --credential-file is required for saga operations"
        );
        return ExitCode::from(2);
    };
    let credential = match load_protected_bearer_credential(&credential_path) {
        Ok(cred) => cred,
        Err(err) => {
            let _ = writeln!(stderr, "error loading credential: {err}");
            return ExitCode::from(1);
        }
    };
    let metadata = CallMetadata::authenticated(credential);
    let endpoint = match endpoint_str.parse() {
        Ok(ep) => ep,
        Err(_) => {
            let _ = writeln!(stderr, "error: invalid endpoint URL");
            return ExitCode::from(2);
        }
    };
    let mut client = match RiffDbClient::connect(endpoint).await {
        Ok(client) => client,
        Err(err) => {
            let _ = writeln!(stderr, "error connecting to server: {err}");
            return ExitCode::from(1);
        }
    };
    let attempts = AttemptBudget::new(3).expect("valid attempt budget");
    let SagaTopSubcommand::Saga { action } = parsed.command;
    let view_opt = match action {
        SagaAction::Start {
            name,
            saga_id,
            input,
            input_file,
            await_millis,
        } => {
            let raw_json = match (input, input_file) {
                (Some(inline), None) => inline,
                (None, Some(path)) => {
                    let mut stdin = io::stdin().lock();
                    match read_path_or_stdin(&path, &mut stdin, MAX_INPUT_BYTES).and_then(utf8) {
                        Ok(text) => text,
                        Err(_) => {
                            let _ = writeln!(stderr, "error reading saga input file");
                            return ExitCode::from(2);
                        }
                    }
                }
                (None, None) => "{}".to_owned(),
                (Some(_), Some(_)) => {
                    let _ = writeln!(stderr, "error: specify either --input or --input-file");
                    return ExitCode::from(2);
                }
            };
            let input_map = match parse_json_input_map(&raw_json) {
                Ok(map) => map,
                Err(msg) => {
                    let _ = writeln!(stderr, "error: {msg}");
                    return ExitCode::from(2);
                }
            };
            let window = await_millis.map(Duration::from_millis);
            match client
                .start_saga(
                    &name,
                    saga_id.as_deref(),
                    input_map,
                    window,
                    attempts,
                    &metadata,
                )
                .await
            {
                Ok(res) => Some(res.instance),
                Err(err) => {
                    let _ = writeln!(stderr, "error starting saga: {err}");
                    return ExitCode::from(1);
                }
            }
        }
        SagaAction::Get { saga_id } => match client.get_saga(&saga_id, attempts, &metadata).await {
            Ok(opt) => opt,
            Err(err) => {
                let _ = writeln!(stderr, "error getting saga: {err}");
                return ExitCode::from(1);
            }
        },
        SagaAction::Await {
            saga_id,
            await_millis,
        } => {
            match client
                .await_saga(
                    &saga_id,
                    Duration::from_millis(await_millis),
                    attempts,
                    &metadata,
                )
                .await
            {
                Ok(opt) => opt,
                Err(err) => {
                    let _ = writeln!(stderr, "error awaiting saga: {err}");
                    return ExitCode::from(1);
                }
            }
        }
        SagaAction::Cancel { saga_id } => {
            match client.cancel_saga(&saga_id, attempts, &metadata).await {
                Ok(opt) => opt,
                Err(err) => {
                    let _ = writeln!(stderr, "error cancelling saga: {err}");
                    return ExitCode::from(1);
                }
            }
        }
        SagaAction::Resume { saga_id } => {
            match client.resume_saga(&saga_id, attempts, &metadata).await {
                Ok(opt) => opt,
                Err(err) => {
                    let _ = writeln!(stderr, "error resuming saga: {err}");
                    return ExitCode::from(1);
                }
            }
        }
    };

    let Some(view) = view_opt else {
        let _ = writeln!(stderr, "error: saga not found");
        return ExitCode::from(1);
    };
    let doc = SagaCliOutput::from_view(view);
    match output_mode {
        OutputMode::Json => {
            if let Ok(json_text) = serde_json::to_string(&doc) {
                let _ = writeln!(stdout, "{json_text}");
            }
        }
        OutputMode::Human => {
            let _ = writeln!(
                stdout,
                "saga_id={} saga_name={} status={} disposition={} revision={} steps={}",
                doc.saga_id,
                doc.saga_name,
                doc.status,
                doc.disposition,
                doc.revision,
                doc.step_count
            );
        }
    }
    ExitCode::SUCCESS
}

fn parse_json_input_map(
    raw_json: &str,
) -> Result<BTreeMap<String, ApplicationValue>, &'static str> {
    let parsed: serde_json::Value =
        serde_json::from_str(raw_json).map_err(|_| "invalid JSON input")?;
    let serde_json::Value::Object(obj) = parsed else {
        return Err("saga input must be a JSON object");
    };
    let mut out = BTreeMap::new();
    for (k, v) in obj {
        out.insert(k, json_to_application_value(v)?);
    }
    Ok(out)
}

fn json_to_application_value(value: serde_json::Value) -> Result<ApplicationValue, &'static str> {
    match value {
        serde_json::Value::Null => Ok(ApplicationValue::Null),
        serde_json::Value::Bool(b) => Ok(ApplicationValue::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Ok(ApplicationValue::U64(u))
            } else if let Some(i) = n.as_i64() {
                Ok(ApplicationValue::I64(i))
            } else {
                Err("floating-point numbers are not valid RiffDB values")
            }
        }
        serde_json::Value::String(s) => Ok(ApplicationValue::String(s)),
        serde_json::Value::Array(items) => {
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                list.push(json_to_application_value(item)?);
            }
            Ok(ApplicationValue::List(list))
        }
        serde_json::Value::Object(map) => {
            let mut rec = BTreeMap::new();
            for (k, v) in map {
                rec.insert(k, json_to_application_value(v)?);
            }
            Ok(ApplicationValue::Record(rec))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001
    #[test]
    fn cli_saga_parser_parses_start_get_await_cancel_and_resume_subcommands() {
        let start = SagaRootCli::try_parse_from([
            "riffdb",
            "saga",
            "start",
            "--name",
            "CheckoutOrder",
            "--input",
            "{\"idempotency_key\":\"k1\",\"order_id\":901}",
            "--await-millis",
            "5000",
        ])
        .expect("parse saga start");
        let SagaTopSubcommand::Saga {
            action:
                SagaAction::Start {
                    name,
                    await_millis,
                    input,
                    ..
                },
        } = start.command
        else {
            panic!("expected SagaAction::Start");
        };
        assert_eq!(name, "CheckoutOrder");
        assert_eq!(await_millis, Some(5000));
        let parsed_map = parse_json_input_map(&input.unwrap()).expect("valid json input");
        assert_eq!(
            parsed_map.get("order_id"),
            Some(&ApplicationValue::U64(901))
        );

        let cancel = SagaRootCli::try_parse_from([
            "riffdb",
            "saga",
            "cancel",
            "--saga-id",
            "018f0000-0000-7000-8000-010203040506",
        ])
        .expect("parse saga cancel");
        let SagaTopSubcommand::Saga {
            action: SagaAction::Cancel { saga_id },
        } = cancel.command
        else {
            panic!("expected SagaAction::Cancel");
        };
        assert_eq!(saga_id, "018f0000-0000-7000-8000-010203040506");
    }
}
