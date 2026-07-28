use std::ffi::OsString;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "riffdb", disable_help_subcommand = true)]
pub(crate) struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<OsString>,
    #[arg(long, global = true, value_name = "LOOPBACK_HTTP_ENDPOINT")]
    pub(crate) endpoint: Option<String>,
    #[arg(long, global = true, value_enum, value_name = "human|json")]
    pub(crate) output: Option<OutputMode>,
    #[arg(long, global = true, value_name = "1..10")]
    pub(crate) max_attempts: Option<String>,
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) credential_file: Option<OsString>,
    #[command(subcommand)]
    pub(crate) command: TopLevel,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopLevel {
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
    Command {
        #[command(subcommand)]
        command: CommandCommand,
    },
    Entity {
        #[command(subcommand)]
        command: EntityCommand,
    },
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    Projection {
        #[command(subcommand)]
        command: ProjectionCommand,
    },
    Capability {
        #[command(subcommand)]
        command: CapabilityCommand,
    },
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    Demo {
        #[command(subcommand)]
        command: DemoCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ContractCommand {
    Validate {
        #[arg(value_name = "SOURCE")]
        source: OsString,
    },
    Deploy {
        #[arg(value_name = "SOURCE")]
        source: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CommandCommand {
    Execute {
        #[arg(value_name = "COMMAND_NAME")]
        command_name: String,
        #[arg(long, value_name = "JSON_INPUT")]
        input: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
    },
    Outcome {
        #[arg(value_name = "COMMAND_NAME")]
        command_name: Option<String>,
        #[arg(long, value_name = "CONTRACT_LINEAGE")]
        lineage: Option<String>,
        #[arg(long, value_name = "IDEMPOTENCY_KEY")]
        idempotency_key: Option<String>,
        #[arg(long, value_name = "OUTCOME_URI")]
        outcome_uri: Option<String>,
    },
}

#[derive(Debug, Args)]
pub(crate) struct ContractSelectionArgs {
    #[arg(long, requires = "contract_version", value_name = "LINEAGE")]
    pub(crate) contract_lineage: Option<String>,
    #[arg(long, requires = "contract_lineage", value_name = "VERSION")]
    pub(crate) contract_version: Option<String>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum EntityCommand {
    Get {
        #[arg(value_name = "ENTITY_TYPE_ID")]
        entity_type_id: String,
        #[arg(long, value_name = "BASE64_ENTITY_KEY")]
        entity_key: String,
        #[command(flatten)]
        contract: ContractSelectionArgs,
        #[arg(long = "field", value_name = "FIELD_ID")]
        fields: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CommitCommand {
    Show {
        #[arg(value_name = "COMMIT_SEQUENCE")]
        commit_sequence: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProjectionCommand {
    Query {
        #[arg(value_name = "PROJECTION_ID")]
        projection_id: String,
        #[arg(long, value_name = "JSON_INPUT")]
        input: OsString,
        #[command(flatten)]
        contract: ContractSelectionArgs,
        #[arg(long, value_name = "COMMIT_SEQUENCE")]
        after: Option<String>,
        #[arg(long, requires = "after", value_name = "NANOSECONDS")]
        wait_nanos: Option<String>,
        #[arg(long, value_name = "ROWS")]
        limit: Option<String>,
        #[arg(long, value_name = "BASE64_CURSOR")]
        cursor: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CapabilityCommand {
    Bootstrap {
        #[arg(long, value_name = "JSON_REQUEST")]
        request: OsString,
        #[arg(long, group = "bootstrap_source", value_name = "PATH")]
        generate: Option<OsString>,
        #[arg(long, group = "bootstrap_source", value_name = "PATH")]
        bootstrap_file: Option<OsString>,
        #[arg(long, group = "bootstrap_source")]
        bootstrap_stdin: bool,
        #[arg(long, value_name = "PATH")]
        bearer_output: Option<OsString>,
    },
    Create {
        #[arg(long, value_name = "JSON_REQUEST")]
        request: OsString,
        #[arg(long, value_name = "CAPABILITY_UUIDV7")]
        capability_id: Option<String>,
        #[arg(long, value_name = "PATH")]
        credential_output: OsString,
    },
    Revoke {
        #[arg(value_name = "CAPABILITY_UUIDV7")]
        capability_id: String,
        #[arg(long, value_enum, value_name = "REASON")]
        reason: RevocationReason,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ServerCommand {
    Health,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BackupCommand {
    Create {
        #[arg(value_name = "NAME")]
        name: String,
    },
    Restore {
        #[arg(value_name = "NAME")]
        name: String,
        #[arg(long)]
        confirm_replace_current_database: bool,
    },
    Operation {
        #[arg(value_name = "MAINTENANCE_OPERATION_ID")]
        maintenance_operation_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum DemoCommand {
    Budget {
        #[arg(long, value_name = "PATH")]
        runner: OsString,
        #[arg(long, value_enum, value_name = "CASE")]
        case: BudgetCase,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum OutputMode {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub(crate) enum RevocationReason {
    Requested,
    Replaced,
    SuspectedCompromise,
    PolicyChange,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub(crate) enum BudgetCase {
    Sequential,
    Contention,
    SameKeyReplay,
}

impl BudgetCase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Contention => "contention",
            Self::SameKeyReplay => "same_key_replay",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_command_tree_and_snake_case_values_parse() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "--output",
            "json",
            "demo",
            "budget",
            "--runner",
            "/tmp/runner",
            "--case",
            "same_key_replay",
        ])
        .expect("accepted command");
        assert_eq!(cli.output, Some(OutputMode::Json));
        assert!(matches!(
            cli.command,
            TopLevel::Demo {
                command: DemoCommand::Budget {
                    case: BudgetCase::SameKeyReplay,
                    ..
                }
            }
        ));
    }

    #[test]
    fn aliases_and_extra_commands_fail_closed() {
        assert!(Cli::try_parse_from(["riffdb", "backup"]).is_err());
        assert!(
            Cli::try_parse_from(["riffdb", "backup", "create", ".maintenance"]).is_ok(),
            "clap leaves semantic backup-name validation to the checked type"
        );
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "demo",
                "budget",
                "--runner=/tmp/runner",
                "--case",
                "same-key-replay"
            ])
            .is_err()
        );
    }
}
