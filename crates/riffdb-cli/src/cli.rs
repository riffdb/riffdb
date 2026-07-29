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
    /// Creates a deterministic application-first RiffDB repository.
    New {
        #[arg(value_name = "APPLICATION")]
        application: String,
        #[arg(
            long,
            value_enum,
            default_value = "rust",
            value_name = "rust|typescript"
        )]
        language: ApplicationLanguage,
        #[arg(long, value_name = "DIRECTORY")]
        directory: Option<OsString>,
    },
    /// Validates and regenerates an exact application package.
    Application {
        #[command(subcommand)]
        command: ApplicationCommand,
    },
    /// Starts the bounded local symbolic development workflow.
    Dev {
        #[arg(long, default_value = "application", value_name = "ROLE_PRESET")]
        role: String,
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        seed: bool,
        #[arg(long, value_name = "DIRECTORY")]
        seed_dir: Option<OsString>,
        #[arg(long, default_value = "8", value_name = "1..8")]
        seed_concurrency: String,
        #[arg(long, hide = true)]
        acceptance: bool,
    },
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
    Query {
        #[command(subcommand)]
        command: QueryCommand,
    },
    /// Checks, describes, binds, or revokes an exact symbolic application role.
    Role {
        #[command(subcommand)]
        command: RoleCommand,
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
pub(crate) enum ApplicationCommand {
    /// Recompiles pinned sources and regenerates all compiler-owned bindings.
    Generate {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_MANIFEST"
        )]
        manifest: OsString,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum ApplicationLanguage {
    Rust,
    Typescript,
}

#[derive(Debug, Subcommand)]
pub(crate) enum RoleCommand {
    Check {
        #[arg(value_name = "APPLICATION_MANIFEST")]
        manifest: OsString,
        #[arg(long, value_name = "ROLE")]
        role: String,
        #[arg(long, value_name = "TENANT")]
        tenant: Option<String>,
    },
    Describe {
        #[arg(value_name = "APPLICATION_MANIFEST")]
        manifest: OsString,
        #[arg(long, value_name = "ROLE")]
        role: String,
        #[arg(long, value_name = "TENANT")]
        tenant: Option<String>,
    },
    Bind {
        #[arg(value_name = "APPLICATION_MANIFEST")]
        manifest: OsString,
        #[arg(long, value_name = "ROLE")]
        role: String,
        #[arg(long, value_name = "TENANT")]
        tenant: Option<String>,
        #[arg(long, value_name = "PRINCIPAL")]
        principal: String,
        #[arg(long, value_enum, value_name = "human|agent|service")]
        actor_kind: RoleActorKind,
        #[arg(long, default_value = "3600", value_name = "SECONDS")]
        lifetime_seconds: String,
        #[arg(long = "audience", required = true, value_name = "AUDIENCE")]
        audiences: Vec<String>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum RoleActorKind {
    Human,
    Agent,
    Service,
}

#[derive(Debug, Subcommand)]
pub(crate) enum QueryCommand {
    Describe {
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Check {
        #[arg(value_name = "RIFFQL_SOURCE")]
        source: OsString,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Explain {
        #[arg(value_name = "RIFFQL_SOURCE")]
        source: OsString,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Run {
        #[arg(value_name = "RIFFQL_SOURCE")]
        source: OsString,
        #[arg(long, value_name = "JSON_PARAMETERS")]
        parameters: Option<OsString>,
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    RunNamed {
        #[arg(value_name = "QUERY_NAME")]
        query_name: String,
        #[arg(long, value_name = "MODULE_HASH_HEX")]
        module_hash: Option<String>,
        #[arg(long, value_name = "JSON_PARAMETERS")]
        parameters: Option<OsString>,
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Deploy {
        #[arg(value_name = "QUERY_DIRECTORY")]
        directory: OsString,
        #[arg(long, value_name = "MODULE_NAME")]
        module_name: String,
        #[arg(long, value_name = "POSITIVE_VERSION")]
        module_version: String,
        #[arg(long, value_name = "any|absent|MODULE_HASH_HEX", default_value = "any")]
        expected_active: String,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Module {
        #[arg(long, value_name = "MODULE_HASH_HEX")]
        module_hash: Option<String>,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    Repl {
        #[command(flatten)]
        contract: ContractSelectionArgs,
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
    /// Runs or resumes bounded ordinary symbolic commands from JSONL.
    Batch {
        #[arg(value_name = "COMMAND_NAME")]
        command_name: String,
        #[arg(value_name = "JSONL_INPUT")]
        input: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
        #[arg(long, default_value = "8", value_name = "1..32")]
        concurrency: String,
        #[arg(long, default_value = "idempotency_key", value_name = "FIELD")]
        idempotency_field: String,
        #[arg(long, value_name = "PATH")]
        checkpoint: Option<OsString>,
        #[arg(long = "error-outcome", value_name = "OUTCOME")]
        error_outcomes: Vec<String>,
        #[arg(long)]
        progress: bool,
    },
    Run {
        #[arg(value_name = "COMMAND_NAME")]
        command_name: String,
        #[arg(long, value_name = "JSON_INPUT")]
        input: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
    },
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
    fn dev_is_a_first_class_bounded_top_level_command() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "dev",
            "--role",
            "ticketdesk-application",
            "--seed",
            "--seed-concurrency",
            "4",
        ])
        .expect("accepted dev command");
        assert!(matches!(
            cli.command,
            TopLevel::Dev {
                role,
                seed: true,
                seed_concurrency,
                ..
            } if role == "ticketdesk-application" && seed_concurrency == "4"
        ));
    }

    #[test]
    fn new_is_application_first_and_has_no_kernel_mode() {
        let cli = Cli::try_parse_from(["riffdb", "new", "inventory", "--language", "typescript"])
            .expect("accepted scaffold command");
        assert!(matches!(
            cli.command,
            TopLevel::New {
                application,
                language: ApplicationLanguage::Typescript,
                ..
            } if application == "inventory"
        ));
        assert!(Cli::try_parse_from(["riffdb", "new", "inventory", "--kernel"]).is_err());
    }

    #[test]
    fn command_batch_is_symbolic_and_has_no_bulk_write_escape_hatch() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "command",
            "batch",
            "CreateTicket",
            "tickets.jsonl",
            "--concurrency",
            "16",
            "--checkpoint",
            "tickets.checkpoint.json",
            "--error-outcome",
            "InvalidInput",
            "--progress",
        ])
        .expect("symbolic batch");
        assert!(matches!(
            cli.command,
            TopLevel::Command {
                command: CommandCommand::Batch {
                    command_name,
                    concurrency,
                    progress: true,
                    ..
                }
            } if command_name == "CreateTicket" && concurrency == "16"
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "command",
                "batch",
                "CreateTicket",
                "tickets.jsonl",
                "--entity-type-id",
                "2",
            ])
            .is_err()
        );
    }

    #[test]
    fn symbolic_role_commands_never_accept_masks_or_stable_ids() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "role",
            "describe",
            "riffdb.application.json",
            "--role",
            "HelpdeskAgent",
        ])
        .expect("symbolic role command");
        assert!(matches!(
            cli.command,
            TopLevel::Role {
                command: RoleCommand::Describe { role, .. }
            } if role == "HelpdeskAgent"
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "role",
                "bind",
                "riffdb.application.json",
                "--role",
                "HelpdeskAgent",
                "--principal",
                "app:helpdesk",
                "--actor-kind",
                "service",
                "--audience",
                "helpdesk",
                "--credential-output",
                "helpdesk.credential",
                "--field-id",
                "7",
            ])
            .is_err()
        );
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
