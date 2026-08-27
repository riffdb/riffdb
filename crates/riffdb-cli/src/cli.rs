use std::ffi::OsString;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "riffdb", disable_help_subcommand = true)]
pub(crate) struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<OsString>,
    #[arg(long, global = true, value_name = "HTTP_OR_HTTPS_ENDPOINT")]
    pub(crate) endpoint: Option<String>,
    #[arg(long, global = true, value_name = "DATABASE")]
    pub(crate) database: Option<String>,
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
    /// Starts the bounded compiler-backed Language Server Protocol service on stdio.
    Lsp,
    /// Initializes RiffDB schema files in a new or existing project.
    Init {
        #[arg(value_name = "APPLICATION")]
        application: Option<String>,
        #[arg(
            long = "generator",
            value_enum,
            default_value = "rust",
            value_name = "rust|go|typescript|python|mcp"
        )]
        generators: Vec<ApplicationGenerator>,
    },
    /// Checks, locks, and installs the configured schema.
    Push {
        /// Accepts one exact compiler-owned successor lock identity.
        #[arg(long, value_name = "64_LOWERCASE_HEX_HASH")]
        accept_lock: Option<String>,
    },
    /// Regenerates configured SDK targets from the exact project lock.
    Generate,
    /// Installs repository-local generated agent guidance and MCP configuration.
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    /// Reports the configured local and installed schema identities.
    Status,
    /// Reports the bounded installed-versus-local schema difference.
    Diff,
    /// Runs the existing staged migration path for the configured schema.
    Migrate {
        #[command(subcommand)]
        command: ProjectMigrationCommand,
    },
    /// Creates a deterministic application-first RiffDB repository.
    New {
        #[arg(value_name = "APPLICATION")]
        application: String,
        #[arg(
            long,
            value_enum,
            default_value = "rust",
            value_name = "rust|go|typescript|python"
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
    /// Inspects exact local contract migration artifacts.
    Migration {
        #[command(subcommand)]
        command: MigrationCommand,
    },
    /// Starts the bounded local symbolic development workflow.
    Dev {
        #[arg(long, default_value = "application", value_name = "ROLE_PRESET")]
        role: String,
        #[arg(long)]
        watch: bool,
        /// Runs the repository's generated application against the local server.
        #[arg(long, conflicts_with_all = ["watch", "acceptance"])]
        run: bool,
        /// Repository-relative Go main-package directory used by `--run`.
        #[arg(long, value_name = "PATH", requires = "run")]
        go_runner_package: Option<OsString>,
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
    /// Describes and reads partitioned domain events.
    Event {
        #[command(subcommand)]
        command: EventCommand,
    },
    /// Consumes and resolves contextual agent work through generated identities.
    Contextual {
        #[command(subcommand)]
        command: ContextualCommand,
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
    /// Exports one authorized symbolic application snapshot as canonical JSONL pages.
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
    /// Reconstitutes one empty not-ready database from an exact portability export.
    Reimport {
        #[command(subcommand)]
        command: ReimportCommand,
    },
    /// Inspects or upgrades one closed database's durable format.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Offline exclusive retention maintenance on a closed database file.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    Demo {
        #[command(subcommand)]
        command: DemoCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum AgentCommand {
    /// Installs or verifies the exact repository-local agent rails.
    Init,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProjectMigrationCommand {
    /// Prints the exact read-only migration plan for the project lock.
    Plan,
    /// Runs one read-only server preflight for the exact project migration.
    Check {
        #[arg(long, value_name = "UUIDV7")]
        operation_id: String,
        #[arg(long, value_name = "64_LOWERCASE_HEX_HASH")]
        migration_hash: Option<String>,
    },
    /// Applies one exact project migration after hash confirmation.
    Apply {
        #[arg(long, value_name = "UUIDV7")]
        operation_id: String,
        #[arg(long, value_name = "64_LOWERCASE_HEX_HASH")]
        confirm_apply: String,
    },
    /// Observes one caller-stable project migration operation.
    Operation {
        #[arg(value_name = "UUIDV7")]
        operation_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ExportScope {
    /// Apply the current application role's row and field policy.
    Principal,
    /// Use explicit whole-application export authority.
    Whole,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ExportCommand {
    /// Starts or exactly replays one immutable snapshot export.
    Start {
        #[arg(long, value_name = "CONTRACT_LINEAGE")]
        lineage: String,
        #[arg(long, value_enum, value_name = "principal|whole")]
        scope: ExportScope,
        #[arg(long)]
        entities: bool,
        #[arg(long)]
        events: bool,
        #[arg(long)]
        provenance: bool,
        #[arg(long)]
        public_audit: bool,
        #[arg(long, default_value = "3600", value_name = "60..86400")]
        lease_seconds: String,
        #[arg(long, value_name = "UUID_V7")]
        operation_id: Option<String>,
        /// Starts a portability-intent export bound to this exact canonical manifest.
        #[arg(long, value_name = "PORTABILITY_MANIFEST_JSON")]
        portability_manifest: Option<OsString>,
    },
    /// Writes one exact bounded page to a newly created canonical JSONL file.
    Page {
        #[arg(long, value_name = "UUID_V7")]
        operation_id: String,
        #[arg(long, value_name = "OPAQUE_BASE64_CURSOR")]
        cursor: String,
        #[arg(long, default_value = "500", value_name = "1..500")]
        max_rows: String,
        #[arg(long, value_name = "NEW_JSONL_FILE")]
        jsonl: OsString,
    },
    /// Observes one durable export checkpoint or terminal receipt.
    Status {
        #[arg(long, value_name = "UUID_V7")]
        operation_id: String,
    },
    /// Closes one nonterminal export with a durable incomplete receipt.
    Cancel {
        #[arg(long, value_name = "UUID_V7")]
        operation_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ReimportScope {
    /// Reconstitute only the principal-filtered source authorized by the grant.
    Principal,
    /// Reconstitute the explicitly authorized whole-application source.
    Whole,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ReimportCommand {
    /// Starts or exactly replays one immutable reimport campaign.
    Start {
        #[arg(long, value_name = "CONTRACT_LINEAGE")]
        lineage: String,
        #[arg(long, value_enum, value_name = "principal|whole")]
        scope: ReimportScope,
        #[arg(long, value_name = "PORTABILITY_MANIFEST_JSON")]
        portability_manifest: OsString,
        #[arg(long, value_name = "EXPORT_MANIFEST_JSON")]
        export_manifest: OsString,
        #[arg(long, value_name = "EXPORT_RECEIPT_JSON")]
        export_receipt: OsString,
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: Option<String>,
    },
    /// Applies one exact hash-bearing entity page from the source export.
    Page {
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: String,
        #[arg(long, value_name = "UUID_V7")]
        export_operation_id: String,
        #[arg(long, value_name = "POSITIVE_INTEGER")]
        page_number: String,
        #[arg(long, value_name = "CANONICAL_JSONL_FILE")]
        jsonl: OsString,
        #[arg(long, value_name = "LOWER_HEX_SHA256")]
        page_hash: String,
        #[arg(long)]
        class_complete: bool,
        #[arg(long)]
        operation_complete: bool,
        #[arg(
            long,
            value_name = "OPAQUE_BASE64_CURSOR",
            required_unless_present = "operation_complete",
            conflicts_with = "operation_complete"
        )]
        next_cursor: Option<String>,
    },
    /// Observes one durable reimport checkpoint or terminal receipt.
    Status {
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: String,
    },
    /// Cancels one nonterminal reimport campaign.
    Cancel {
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum MigrationCommand {
    /// Prints the exact read-only migration plan for a locked application.
    Plan {
        #[arg(
            long,
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        application: OsString,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
    },
    /// Runs one read-only server preflight for the exact locked migration.
    Check {
        #[arg(
            long,
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        application: OsString,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
        #[arg(long, value_name = "UUIDV7")]
        operation_id: String,
        #[arg(long, value_name = "64_LOWERCASE_HEX_HASH")]
        migration_hash: Option<String>,
    },
    /// Applies one exact locked migration after explicit hash confirmation.
    Apply {
        #[arg(
            long,
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        application: OsString,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
        #[arg(long, value_name = "UUIDV7")]
        operation_id: String,
        #[arg(long, value_name = "64_LOWERCASE_HEX_HASH")]
        confirm_apply: String,
    },
    /// Observes one caller-stable migration operation after the database reopens.
    Operation {
        #[arg(value_name = "UUIDV7")]
        operation_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ApplicationCommand {
    /// Validates one bounded adapter conformance manifest and optional exact installation plan.
    Conformance {
        #[arg(value_name = "ADAPTER_CONFORMANCE_MANIFEST")]
        manifest: OsString,
        #[arg(long, value_name = "CANONICAL_INSTALLATION_PLAN")]
        plan: Option<OsString>,
    },
    /// Starts or resumes one exact, caller-identified installation campaign.
    Install {
        #[arg(long, value_name = "CANONICAL_PLAN")]
        plan: OsString,
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: String,
        /// Attests exact successful first-party driver identity handshakes.
        #[arg(
            long = "driver-proof",
            value_enum,
            value_name = "rust|go|typescript|python",
            conflicts_with = "seed_receipts"
        )]
        driver_proof: Vec<ApplicationLanguage>,
        /// Attests one canonical plan-bound ordinary-command seed receipt set.
        #[arg(
            long,
            value_name = "CANONICAL_SEED_RECEIPTS",
            conflicts_with = "driver_proof"
        )]
        seed_receipts: Option<OsString>,
    },
    /// Observes one retained installation campaign without changing its plan.
    Installation {
        #[arg(long, value_name = "UUID_V7")]
        campaign_id: String,
    },
    /// Previews or atomically writes an explicit local application-format migration.
    Migrate {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
        #[arg(long, value_parser = ["v2"], value_name = "v2")]
        to: String,
        #[arg(long)]
        write: bool,
    },
    /// Read-only symbolic compilation and safety analysis.
    Check {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
        /// Compiles author-owned sources and roles without comparing the exact lock or generated files.
        #[arg(long)]
        source_only: bool,
    },
    /// Compiles and prints the exact proposed lock without writing.
    Preview {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
    },
    /// Writes or verifies the compiler-owned exact lock and generated artifacts.
    Lock {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
        #[arg(long, conflicts_with = "check", required_unless_present = "check")]
        write: bool,
        #[arg(long, conflicts_with = "write", required_unless_present = "write")]
        check: bool,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
    },
    /// Regenerates V1 manifests or exact locked symbolic application bindings.
    Generate {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE_OR_V1_MANIFEST"
        )]
        manifest: OsString,
        #[arg(long)]
        locked: bool,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
    },
    /// Deploys one exact lock, with optional explicit role provisioning and seed.
    Deploy {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
        #[arg(long, value_name = "ROLE")]
        provision_role: Option<String>,
        #[arg(long, requires = "provision_role", value_name = "TENANT")]
        tenant: Option<String>,
        #[arg(long, default_value = "28800", value_name = "SECONDS")]
        lifetime_seconds: String,
        #[arg(long, requires = "provision_role")]
        seed: bool,
        #[arg(long, default_value = "8", value_name = "1..32")]
        seed_concurrency: String,
        #[arg(
            long = "replace-role-credential",
            alias = "replace-expired-credential",
            requires = "provision_role"
        )]
        replace_expired_credential: bool,
        /// Binds deployment to one exact installation plan and campaign.
        #[arg(
            long,
            value_name = "CANONICAL_PLAN",
            requires = "installation_campaign_id"
        )]
        installation_plan: Option<OsString>,
        /// Caller-stable campaign resumed before and after remote deployment.
        #[arg(long, value_name = "UUID_V7", requires = "installation_plan")]
        installation_campaign_id: Option<String>,
    },
    /// Idempotently deploys and binds one explicit short-lived application role.
    BindDevRole {
        #[arg(
            default_value = "riffdb.application.json",
            value_name = "APPLICATION_SOURCE"
        )]
        source: OsString,
        #[arg(
            long,
            default_value = "riffdb.application.lock.json",
            value_name = "APPLICATION_LOCK"
        )]
        lock: OsString,
        #[arg(long, value_name = "ROLE")]
        role: String,
        #[arg(long, value_name = "TENANT")]
        tenant: Option<String>,
        #[arg(long, default_value = "28800", value_name = "SECONDS")]
        lifetime_seconds: String,
        #[arg(long = "replace-role-credential", alias = "replace-expired-credential")]
        replace_expired_credential: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum ApplicationLanguage {
    Rust,
    Go,
    Typescript,
    Python,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum ApplicationGenerator {
    Rust,
    Go,
    Typescript,
    Python,
    Mcp,
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
        /// Operator-owned bounded fact values for a compiler-protected role.
        #[arg(long, value_name = "JSON_OBJECT_PATH")]
        principal_facts: Option<OsString>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum VectorInspectionKind {
    Stale,
    Outdated,
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
        #[arg(long, value_name = "COMMIT_SEQUENCE")]
        read_after_commit: Option<String>,
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
        #[arg(long, value_name = "COMMIT_SEQUENCE")]
        read_after_commit: Option<String>,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    /// Receives one closed update from an exact generated live named query.
    Watch {
        #[arg(long = "module-hash", value_name = "64_HEX_CHARS")]
        module_hash: String,
        #[arg(value_name = "OPERATION")]
        operation: String,
        #[arg(long = "parameter", value_name = "NAME=JSON_VALUE")]
        parameters: Vec<String>,
        #[arg(long, value_name = "BASE64_CURSOR")]
        cursor: Option<String>,
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
    /// Inspects authoritative vector freshness or model-version state.
    InspectVector {
        /// Symbolic entity name that owns the maintained vector field.
        #[arg(value_name = "ENTITY")]
        entity: String,
        /// Symbolic maintained vector field name.
        #[arg(value_name = "FIELD")]
        field: String,
        /// Exact partition value as typed InputValue JSON.
        #[arg(long, value_name = "JSON_VALUE")]
        partition: String,
        /// Whether to inspect stale evidence or outdated model versions.
        #[arg(long, value_enum, default_value = "stale")]
        kind: VectorInspectionKind,
        /// Maximum returned entity items when row policy forbids summary counts.
        #[arg(long, value_name = "ROWS")]
        limit: Option<String>,
        /// Opaque continuation cursor as lower-hex bytes.
        #[arg(long, value_name = "HEX")]
        cursor_hex: Option<String>,
        #[command(flatten)]
        contract: ContractSelectionArgs,
    },
    /// Executes one org-scoped projected columnar query under a freshness policy.
    Projected {
        /// Registered projection name.
        #[arg(value_name = "PROJECTION_NAME")]
        projection_name: String,
        /// Organization scope as typed InputValue JSON (e.g. `{"type":"uuid","value":"..."}`).
        #[arg(long, value_name = "JSON_VALUE")]
        org_scope: String,
        /// Selected field names (repeatable). Empty = all projected fields.
        #[arg(long = "select", value_name = "FIELD")]
        select: Vec<String>,
        /// Equality predicate as `FIELD=JSON_VALUE` (repeatable).
        #[arg(long = "eq", value_name = "FIELD=JSON_VALUE")]
        eq: Vec<String>,
        /// Range predicate as `FIELD=[LOW_JSON,HIGH_JSON]`: a JSON two-element
        /// array whose bounds are typed JSON values or `null` for unbounded
        /// (inclusive low, exclusive high; at least one bound; repeatable).
        #[arg(long = "range", value_name = "FIELD=[LOW,HIGH]")]
        range: Vec<String>,
        /// Order key as `FIELD` or `FIELD:desc` (repeatable).
        #[arg(long = "order", value_name = "FIELD[:desc]")]
        order: Vec<String>,
        /// Aggregate as `OP:FIELD` where OP is `sum`, `min`, or `max`, or the
        /// bare word `count` (repeatable). Any aggregate returns groups
        /// instead of rows; more than one requires `--group-by`.
        #[arg(long = "aggregate", value_name = "OP:FIELD|count")]
        aggregate: Vec<String>,
        /// Group-by key field name (repeatable).
        #[arg(long = "group-by", value_name = "FIELD")]
        group_by: Vec<String>,
        /// Post-sort row limit. With `--group-by` this is the maximum number
        /// of groups instead, and exceeding it is rejected rather than
        /// truncated; it does not apply to an aggregate without `--group-by`.
        #[arg(long, value_name = "ROWS")]
        limit: Option<String>,
        /// Freshness policy: `available`, `bounded:N`, or `causal` (requires token).
        #[arg(long, value_name = "POLICY", default_value = "available")]
        freshness: String,
        /// Opaque commit-token bytes as hex, exactly as `commit_token` is
        /// reported by a ready projected read (causal only).
        #[arg(long, value_name = "HEX", group = "causal_token")]
        token_hex: Option<String>,
        /// Opaque commit-token raw bytes (not hex) from a file (causal only).
        #[arg(long, value_name = "PATH", group = "causal_token")]
        token_file: Option<OsString>,
        /// Opaque commit-token raw bytes (not hex) from stdin (causal only).
        #[arg(long, group = "causal_token")]
        token_stdin: bool,
        /// Maximum wait for causal catch-up (nanoseconds). Default 30s.
        #[arg(long, value_name = "NANOSECONDS")]
        max_wait_nanos: Option<String>,
        /// Request packed column-major Ready encoding.
        #[arg(long)]
        packed: bool,
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
        /// Symbolic idempotency input; derived from --application when omitted.
        #[arg(long, value_name = "FIELD")]
        idempotency_field: Option<String>,
        #[arg(long, value_name = "PATH")]
        checkpoint: Option<OsString>,
        #[arg(long = "error-outcome", value_name = "OUTCOME")]
        error_outcomes: Vec<String>,
        #[arg(long)]
        progress: bool,
        /// Exact application source used to preflight compiled collection bounds.
        #[arg(long, value_name = "RIFFDB.APPLICATION.JSON")]
        application: Option<OsString>,
    },
    Run {
        #[arg(value_name = "COMMAND_NAME")]
        command_name: String,
        #[arg(long, value_name = "JSON_INPUT")]
        input: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
        /// Exact application source used to preflight compiled collection bounds.
        #[arg(long, value_name = "RIFFDB.APPLICATION.JSON")]
        application: Option<OsString>,
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
        /// Observed history incarnation fence (ADR-0072). Stale values fail closed.
        #[arg(long = "observed-history-incarnation", value_name = "INCARNATION")]
        observed_history_incarnation: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum EventCommand {
    /// Describes one event in the active contract.
    Describe {
        #[arg(value_name = "EVENT")]
        event: String,
    },
    /// Replays one exact event partition.
    Replay {
        #[arg(value_name = "EVENT")]
        event: String,
        #[arg(long = "partition", value_name = "FIELD=JSON_VALUE")]
        partition: Vec<String>,
        #[arg(long = "field", value_name = "FIELD")]
        fields: Vec<String>,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        after: Option<String>,
        #[arg(long, value_name = "ROWS")]
        limit: Option<String>,
        #[arg(long, value_name = "BASE64_CURSOR")]
        cursor: Option<String>,
        #[arg(long = "observed-history-incarnation", value_name = "INCARNATION")]
        observed_history_incarnation: Option<String>,
    },
    /// Waits for events after one exact partition position.
    Tail {
        #[arg(value_name = "EVENT")]
        event: String,
        #[arg(long = "partition", value_name = "FIELD=JSON_VALUE")]
        partition: Vec<String>,
        #[arg(long = "field", value_name = "FIELD")]
        fields: Vec<String>,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        after: Option<String>,
        #[arg(long, value_name = "ROWS")]
        limit: Option<String>,
        #[arg(long, default_value = "30000000000", value_name = "NANOSECONDS")]
        wait_nanos: String,
        #[arg(long = "observed-history-incarnation", value_name = "INCARNATION")]
        observed_history_incarnation: Option<String>,
    },
    /// Leases one bounded batch from a named reactive event stream.
    Consume {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, default_value = "1", value_name = "1..64")]
        batch_limit: String,
        #[arg(long, default_value = "16", value_name = "1..64")]
        in_flight_limit: String,
        #[arg(long, default_value = "60", value_name = "5..900")]
        lease_seconds: String,
        #[arg(long, default_value = "0", value_name = "NANOSECONDS")]
        wait_nanos: String,
        #[arg(long, value_name = "BASE64_CURSOR")]
        progress_cursor: Option<String>,
    },
    /// Acknowledges one exact live event lease.
    Ack {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        event_id: String,
        #[arg(long, value_name = "64_HEX_CHARS")]
        lease_token: String,
        #[arg(long, value_name = "INCARNATION")]
        history_incarnation: String,
    },
    /// Negatively acknowledges one exact live event lease.
    Nack {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        event_id: String,
        #[arg(long, value_name = "64_HEX_CHARS")]
        lease_token: String,
        #[arg(long, value_name = "INCARNATION")]
        history_incarnation: String,
        #[arg(long, default_value = "0", value_name = "NANOSECONDS")]
        retry_delay_nanos: String,
    },
    /// Moves a consumer checkpoint after exact seek authorization.
    Seek {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "before-first|COMMIT:ORDINAL")]
        checkpoint: Option<String>,
        /// Opaque protected-consumer cursor returned by status or consume.
        #[arg(long, value_name = "BASE64_CURSOR")]
        progress_cursor: Option<String>,
    },
    /// Retires one consumer and releases its retention fence.
    Retire {
        #[command(flatten)]
        consumer: EventConsumerArgs,
    },
    /// Reads bounded status for one exact consumer.
    Status {
        #[command(flatten)]
        consumer: EventConsumerArgs,
    },
}

#[derive(Debug, Args)]
pub(crate) struct EventConsumerArgs {
    #[arg(long, value_name = "64_HEX_CHARS")]
    pub(crate) module_hash: String,
    #[arg(long, value_name = "OPERATION")]
    pub(crate) operation: String,
    #[arg(long = "parameter", value_name = "NAME=JSON_VALUE")]
    pub(crate) parameters: Vec<String>,
    #[arg(long, value_name = "CONSUMER")]
    pub(crate) consumer_name: String,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ContextualCommand {
    /// Leases at most one event with fresh same-snapshot context.
    Next {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, default_value = "0", value_name = "NANOSECONDS")]
        wait_nanos: String,
        #[arg(long, value_name = "BASE64_CURSOR")]
        progress_cursor: Option<String>,
    },
    /// Acknowledges one exact contextual work-item lease.
    Ack {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        event_id: String,
        #[arg(long, value_name = "64_HEX_CHARS")]
        lease_token: String,
        #[arg(long, value_name = "INCARNATION")]
        history_incarnation: String,
    },
    /// Negatively acknowledges one exact contextual work-item lease.
    Nack {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "COMMIT:ORDINAL")]
        event_id: String,
        #[arg(long, value_name = "64_HEX_CHARS")]
        lease_token: String,
        #[arg(long, value_name = "INCARNATION")]
        history_incarnation: String,
        #[arg(long, default_value = "0", value_name = "NANOSECONDS")]
        retry_delay_nanos: String,
    },
    /// Reads bounded status for one exact contextual consumer.
    Status {
        #[command(flatten)]
        consumer: EventConsumerArgs,
    },
    /// Executes one declared command reaction using its server-issued proof.
    React {
        #[command(flatten)]
        consumer: EventConsumerArgs,
        #[arg(long, value_name = "REACTION")]
        reaction: String,
        #[arg(long, value_name = "OPAQUE_LOWER_HEX")]
        causation_token: String,
        #[arg(long, value_name = "COMMAND")]
        command_name: String,
        #[arg(long, value_name = "JSON_INPUT")]
        input: OsString,
        #[arg(long, value_name = "VERSION")]
        expected_version: Option<String>,
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
    /// Retires one exact immutable backup through the receipted public surface.
    Retire {
        #[arg(value_name = "NAME")]
        name: String,
    },
    Operation {
        #[arg(value_name = "MAINTENANCE_OPERATION_ID")]
        maintenance_operation_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum StorageCommand {
    /// Compares the retained format identity without opening the database.
    Preflight {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
    },
    /// Runs the sole manifest-authorized offline transition from a verified backup.
    Upgrade {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "BACKUP_DIRECTORY")]
        backup: OsString,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum RetentionCommand {
    /// Prints the retention watermark and fencing breakdown for a closed database.
    Status {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
    },
    /// Operator hold management (add / remove).
    Hold {
        #[command(subcommand)]
        command: RetentionHoldCommand,
    },
    /// Detaches a projection identity from the fencing minimum (audited).
    ProjectionDetach {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "ID")]
        projection_id: String,
        #[arg(long, value_name = "TEXT")]
        reason: String,
    },
    /// Reattaches a previously detached projection to the minimum (audited).
    ProjectionReattach {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "ID")]
        projection_id: String,
        #[arg(long, value_name = "TEXT")]
        reason: String,
    },
    /// Offline prune of commits/events/outbox through a target inclusive sequence.
    Prune {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "N")]
        target_sequence: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum RetentionHoldCommand {
    Add {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "ID")]
        hold_id: String,
        #[arg(long, value_name = "N")]
        sequence: String,
        #[arg(long, value_name = "TEXT")]
        reason: String,
    },
    Remove {
        #[arg(long, value_name = "PATH")]
        database_path: OsString,
        #[arg(long, value_name = "ID")]
        hold_id: String,
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
    fn query_projected_parses_and_packed_flag_is_observed() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "query",
            "projected",
            "board",
            "--org-scope",
            r#"{"type":"uuid","value":"11111111-1111-1111-1111-111111111111"}"#,
            "--eq",
            r#"project_id={"type":"uuid","value":"22222222-2222-2222-2222-222222222222"}"#,
            "--range",
            r#"created_at=[{"type":"i64","value":"1"},null]"#,
            "--select",
            "title",
            "--order",
            "ticket_id",
            "--limit",
            "50",
            "--freshness",
            "available",
            "--packed",
            "--contract-lineage",
            "TicketDesk",
            "--contract-version",
            "1",
        ])
        .expect("accepted projected query");
        match cli.command {
            TopLevel::Query {
                command:
                    QueryCommand::Projected {
                        projection_name,
                        packed,
                        limit,
                        freshness,
                        range,
                        ..
                    },
            } => {
                assert_eq!(projection_name, "board");
                assert!(packed, "falsifiability (c): --packed must set packed=true");
                assert_eq!(limit.as_deref(), Some("50"));
                assert_eq!(freshness, "available");
                // JSON-array bounds reach the parser as one unsplit argument.
                assert_eq!(
                    range,
                    vec![r#"created_at=[{"type":"i64","value":"1"},null]"#.to_owned()]
                );
            }
            other => panic!("expected Query::Projected, got {other:?}"),
        }

        let row = Cli::try_parse_from([
            "riffdb",
            "query",
            "projected",
            "board",
            "--org-scope",
            r#"{"type":"uuid","value":"11111111-1111-1111-1111-111111111111"}"#,
            "--freshness",
            "available",
        ])
        .expect("row default");
        match row.command {
            TopLevel::Query {
                command: QueryCommand::Projected { packed: false, .. },
            } => {}
            other => panic!("default must not be packed: {other:?}"),
        }
    }

    #[test]
    fn query_vector_inspection_is_symbolic_and_bounded_at_the_cli_boundary() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "query",
            "inspect-vector",
            "Document",
            "embedding",
            "--partition",
            r#"{"type":"uuid","value":"11111111-1111-1111-1111-111111111111"}"#,
            "--kind",
            "outdated",
            "--limit",
            "20",
            "--cursor-hex",
            "00112233445566778899aabbccddeeff",
        ])
        .expect("accepted symbolic inspection");
        match cli.command {
            TopLevel::Query {
                command:
                    QueryCommand::InspectVector {
                        entity,
                        field,
                        kind,
                        limit,
                        cursor_hex,
                        ..
                    },
            } => {
                assert_eq!(entity, "Document");
                assert_eq!(field, "embedding");
                assert_eq!(kind, VectorInspectionKind::Outdated);
                assert_eq!(limit.as_deref(), Some("20"));
                assert_eq!(
                    cursor_hex.as_deref(),
                    Some("00112233445566778899aabbccddeeff")
                );
            }
            other => panic!("expected Query::InspectVector, got {other:?}"),
        }
    }

    #[test]
    fn query_projected_accepts_repeated_aggregate_and_group_by_flags() {
        let cli = Cli::try_parse_from([
            "riffdb",
            "query",
            "projected",
            "board",
            "--org-scope",
            r#"{"type":"uuid","value":"11111111-1111-1111-1111-111111111111"}"#,
            "--group-by",
            "status",
            "--group-by",
            "project_id",
            "--aggregate",
            "count",
            "--aggregate",
            "sum:story_points",
            "--aggregate",
            "min:title",
            "--freshness",
            "available",
        ])
        .expect("accepted aggregate query");
        match cli.command {
            TopLevel::Query {
                command:
                    QueryCommand::Projected {
                        aggregate,
                        group_by,
                        ..
                    },
            } => {
                assert_eq!(
                    aggregate,
                    vec![
                        "count".to_owned(),
                        "sum:story_points".to_owned(),
                        "min:title".to_owned()
                    ],
                    "aggregates must arrive repeatable and in request order"
                );
                assert_eq!(group_by, vec!["status".to_owned(), "project_id".to_owned()]);
            }
            other => panic!("expected Query::Projected, got {other:?}"),
        }

        // A row query keeps both lists empty.
        let row = Cli::try_parse_from([
            "riffdb",
            "query",
            "projected",
            "board",
            "--org-scope",
            r#"{"type":"uuid","value":"11111111-1111-1111-1111-111111111111"}"#,
            "--freshness",
            "available",
        ])
        .expect("row default");
        match row.command {
            TopLevel::Query {
                command:
                    QueryCommand::Projected {
                        aggregate,
                        group_by,
                        ..
                    },
            } => {
                assert!(aggregate.is_empty());
                assert!(group_by.is_empty());
            }
            other => panic!("expected Query::Projected, got {other:?}"),
        }
    }

    #[test]
    fn retention_offline_verbs_parse() {
        let status = Cli::try_parse_from([
            "riffdb",
            "retention",
            "status",
            "--database-path",
            "/tmp/db.redb",
        ])
        .expect("status");
        assert!(matches!(
            status.command,
            TopLevel::Retention {
                command: RetentionCommand::Status { .. }
            }
        ));

        let hold = Cli::try_parse_from([
            "riffdb",
            "retention",
            "hold",
            "add",
            "--database-path",
            "/tmp/db.redb",
            "--hold-id",
            "cap",
            "--sequence",
            "10",
            "--reason",
            "test",
        ])
        .expect("hold add");
        assert!(matches!(
            hold.command,
            TopLevel::Retention {
                command: RetentionCommand::Hold {
                    command: RetentionHoldCommand::Add { .. }
                }
            }
        ));

        let prune = Cli::try_parse_from([
            "riffdb",
            "retention",
            "prune",
            "--database-path",
            "/tmp/db.redb",
            "--target-sequence",
            "5",
        ])
        .expect("prune");
        assert!(matches!(
            prune.command,
            TopLevel::Retention {
                command: RetentionCommand::Prune { .. }
            }
        ));

        let detach = Cli::try_parse_from([
            "riffdb",
            "retention",
            "projection-detach",
            "--database-path",
            "/tmp/db.redb",
            "--projection-id",
            "7",
            "--reason",
            "replay budget hold",
        ])
        .expect("detach");
        assert!(matches!(
            detach.command,
            TopLevel::Retention {
                command: RetentionCommand::ProjectionDetach { .. }
            }
        ));

        let reattach = Cli::try_parse_from([
            "riffdb",
            "retention",
            "projection-reattach",
            "--database-path",
            "/tmp/db.redb",
            "--projection-id",
            "7",
            "--reason",
            "budget restored",
        ])
        .expect("reattach");
        assert!(matches!(
            reattach.command,
            TopLevel::Retention {
                command: RetentionCommand::ProjectionReattach { .. }
            }
        ));
    }

    #[test]
    fn durable_format_commands_require_explicit_closed_database_paths() {
        let preflight = Cli::try_parse_from([
            "riffdb",
            "storage",
            "preflight",
            "--database-path",
            "/home/operator/data/application.redb",
        ])
        .expect("storage preflight");
        assert!(matches!(
            preflight.command,
            TopLevel::Storage {
                command: StorageCommand::Preflight { .. }
            }
        ));

        let upgrade = Cli::try_parse_from([
            "riffdb",
            "storage",
            "upgrade",
            "--database-path",
            "/home/operator/data/application.redb",
            "--backup",
            "/home/operator/backups/pre-upgrade",
        ])
        .expect("storage upgrade");
        assert!(matches!(
            upgrade.command,
            TopLevel::Storage {
                command: StorageCommand::Upgrade { .. }
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
        let run = Cli::try_parse_from(["riffdb", "dev", "--seed", "--run"])
            .expect("accepted generated application run");
        assert!(matches!(
            run.command,
            TopLevel::Dev {
                run: true,
                seed: true,
                go_runner_package: None,
                ..
            }
        ));
        let nested_go = Cli::try_parse_from([
            "riffdb",
            "dev",
            "--run",
            "--go-runner-package",
            "cmd/server",
        ])
        .expect("accepted nested Go runner package");
        assert!(matches!(
            nested_go.command,
            TopLevel::Dev { go_runner_package, .. }
                if go_runner_package.as_deref() == Some(std::ffi::OsStr::new("cmd/server"))
        ));
        assert!(
            Cli::try_parse_from(["riffdb", "dev", "--go-runner-package", "cmd/server"]).is_err()
        );
        assert!(Cli::try_parse_from(["riffdb", "dev", "--run", "--watch"]).is_err());
    }

    #[test]
    fn contextual_commands_require_exact_generated_identity_and_lease_evidence() {
        let module = "ab".repeat(32);
        let next = Cli::try_parse_from([
            "riffdb",
            "contextual",
            "next",
            "--module-hash",
            &module,
            "--operation",
            "TriageTicket",
            "--consumer-name",
            "triage-worker",
            "--parameter",
            "organization_id={\"type\":\"string\",\"value\":\"acme\"}",
            "--wait-nanos",
            "1000000",
        ])
        .expect("contextual next");
        assert!(matches!(
            next.command,
            TopLevel::Contextual {
                command: ContextualCommand::Next { wait_nanos, .. }
            } if wait_nanos == "1000000"
        ));

        let react = Cli::try_parse_from([
            "riffdb",
            "contextual",
            "react",
            "--module-hash",
            &module,
            "--operation",
            "TriageTicket",
            "--consumer-name",
            "triage-worker",
            "--reaction",
            "comment",
            "--causation-token",
            &"cd".repeat(33),
            "--command-name",
            "CreateComment",
            "--input",
            "comment.json",
            "--expected-version",
            "1",
        ])
        .expect("contextual reaction");
        assert!(matches!(
            react.command,
            TopLevel::Contextual {
                command: ContextualCommand::React { command_name, .. }
            } if command_name == "CreateComment"
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "contextual",
                "ack",
                "--module-hash",
                &module,
                "--operation",
                "TriageTicket",
                "--consumer-name",
                "triage-worker",
            ])
            .is_err()
        );
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
        let python = Cli::try_parse_from(["riffdb", "new", "inventory", "--language", "python"])
            .expect("accepted Python scaffold command");
        assert!(matches!(
            python.command,
            TopLevel::New {
                language: ApplicationLanguage::Python,
                ..
            }
        ));
        let go = Cli::try_parse_from(["riffdb", "new", "inventory", "--language", "go"])
            .expect("accepted Go scaffold command");
        assert!(matches!(
            go.command,
            TopLevel::New {
                language: ApplicationLanguage::Go,
                ..
            }
        ));
        assert!(Cli::try_parse_from(["riffdb", "new", "inventory", "--kernel"]).is_err());
    }

    #[test]
    fn database_shaped_project_verbs_are_additive_and_exact() {
        let init = Cli::try_parse_from([
            "riffdb",
            "init",
            "inventory",
            "--generator",
            "rust",
            "--generator",
            "typescript",
        ])
        .expect("project init");
        assert!(matches!(
            init.command,
            TopLevel::Init { application: Some(application), generators }
                if application == "inventory"
                    && generators == vec![ApplicationGenerator::Rust, ApplicationGenerator::Typescript]
        ));

        let accepted = "ab".repeat(32);
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "push", "--accept-lock", &accepted])
                .expect("accepted push")
                .command,
            TopLevel::Push { accept_lock: Some(hash) } if hash == accepted
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "generate"])
                .expect("generate")
                .command,
            TopLevel::Generate
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "agent", "init"])
                .expect("agent init")
                .command,
            TopLevel::Agent {
                command: AgentCommand::Init
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "status"])
                .expect("status")
                .command,
            TopLevel::Status
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "diff"])
                .expect("diff")
                .command,
            TopLevel::Diff
        ));

        let operation = "018f2f85-3c20-7a31-8f11-112233445566";
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "migrate",
                "apply",
                "--operation-id",
                operation,
                "--confirm-apply",
                &accepted,
            ])
            .expect("project migration apply")
            .command,
            TopLevel::Migrate {
                command: ProjectMigrationCommand::Apply {
                    operation_id,
                    confirm_apply,
                }
            } if operation_id == operation && confirm_apply == accepted
        ));
        assert!(Cli::try_parse_from(["riffdb", "migrate", "apply"]).is_err());
    }

    #[test]
    fn application_source_and_lock_operations_are_explicit_and_closed() {
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "migration",
                "plan",
                "--application",
                "custom.application.json"
            ])
            .expect("migration plan")
            .command,
            TopLevel::Migration {
                command: MigrationCommand::Plan { application, .. }
            } if application == "custom.application.json"
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "conformance",
                "adapter.conformance.json",
                "--plan",
                "installation-plan.json",
            ])
            .expect("adapter conformance check")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::Conformance {
                    manifest,
                    plan: Some(plan),
                }
            } if manifest == "adapter.conformance.json" && plan == "installation-plan.json"
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "application", "migrate", "--to", "v2"])
                .expect("migration preview")
                .command,
            TopLevel::Application {
                command: ApplicationCommand::Migrate { write: false, .. }
            }
        ));
        assert!(Cli::try_parse_from(["riffdb", "application", "migrate", "--to", "v3"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "application", "check"])
                .expect("check")
                .command,
            TopLevel::Application {
                command: ApplicationCommand::Check {
                    source_only: false,
                    ..
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "application", "check", "--source-only"])
                .expect("source-only check")
                .command,
            TopLevel::Application {
                command: ApplicationCommand::Check {
                    source_only: true,
                    ..
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "application", "lock", "--write"])
                .expect("write")
                .command,
            TopLevel::Application {
                command: ApplicationCommand::Lock {
                    write: true,
                    check: false,
                    ..
                }
            }
        ));
        assert!(Cli::try_parse_from(["riffdb", "application", "lock"]).is_err());
        assert!(
            Cli::try_parse_from(["riffdb", "application", "lock", "--write", "--check"]).is_err()
        );
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "application", "generate", "--locked"])
                .expect("locked generation")
                .command,
            TopLevel::Application {
                command: ApplicationCommand::Generate { locked: true, .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "deploy",
                "--provision-role",
                "TicketDeskAgent",
                "--tenant",
                "organization_acme",
                "--seed",
                "--seed-concurrency",
                "16",
                "--replace-role-credential",
                "--installation-plan",
                "installation-plan.json",
                "--installation-campaign-id",
                "018f2f85-3c20-7a31-8f11-112233445566",
            ])
            .expect("locked installed deployment")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::Deploy {
                    provision_role: Some(role),
                    tenant: Some(tenant),
                    seed: true,
                    seed_concurrency,
                    replace_expired_credential: true,
                    installation_plan: Some(installation_plan),
                    installation_campaign_id: Some(installation_campaign_id),
                    ..
                }
            } if role == "TicketDeskAgent"
                && tenant == "organization_acme"
                && seed_concurrency == "16"
                && installation_plan == "installation-plan.json"
                && installation_campaign_id == "018f2f85-3c20-7a31-8f11-112233445566"
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "deploy",
                "--installation-plan",
                "installation-plan.json",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "deploy",
                "--installation-campaign-id",
                "018f2f85-3c20-7a31-8f11-112233445566",
            ])
            .is_err()
        );
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "bind-dev-role",
                "--role",
                "TicketDeskAgent",
                "--replace-expired-credential",
            ])
            .expect("legacy replacement alias")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::BindDevRole {
                    replace_expired_credential: true,
                    ..
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "bind-dev-role",
                "--role",
                "TicketDeskAgent",
                "--tenant",
                "organization_acme",
            ])
            .expect("standalone development role bind")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::BindDevRole { role, tenant: Some(tenant), .. }
            } if role == "TicketDeskAgent" && tenant == "organization_acme"
        ));

        let campaign_id = "018f2f85-3c20-7a31-8f11-112233445566";
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "install",
                "--plan",
                "installation-plan.json",
                "--campaign-id",
                campaign_id,
                "--driver-proof",
                "rust",
                "--driver-proof",
                "typescript",
            ])
            .expect("exact application installation")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::Install {
                    plan,
                    campaign_id: parsed,
                    driver_proof,
                    seed_receipts: None,
                }
            } if plan == "installation-plan.json"
                && parsed == campaign_id
                && driver_proof == [ApplicationLanguage::Rust, ApplicationLanguage::Typescript]
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "install",
                "--plan",
                "installation-plan.json",
                "--campaign-id",
                campaign_id,
                "--seed-receipts",
                "seed-receipts.json",
            ])
            .expect("exact seed completion")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::Install {
                    seed_receipts: Some(path),
                    driver_proof,
                    ..
                }
            } if path == "seed-receipts.json" && driver_proof.is_empty()
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "install",
                "--plan",
                "installation-plan.json",
                "--campaign-id",
                campaign_id,
                "--driver-proof",
                "rust",
                "--seed-receipts",
                "seed-receipts.json",
            ])
            .is_err(),
            "one resume request can complete only one external stage"
        );
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "installation",
                "--campaign-id",
                campaign_id,
            ])
            .expect("application installation observation")
            .command,
            TopLevel::Application {
                command: ApplicationCommand::Installation { campaign_id: parsed }
            } if parsed == campaign_id
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "application",
                "install",
                "--plan",
                "installation-plan.json",
            ])
            .is_err()
        );
    }

    #[test]
    fn migration_administration_commands_require_explicit_identity_and_confirmation() {
        let operation = "018f2f85-3c20-7a31-8f11-112233445566";
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "migration",
                "check",
                "--operation-id",
                operation,
                "--migration-hash",
                &"ab".repeat(32),
            ])
            .expect("migration check")
            .command,
            TopLevel::Migration {
                command: MigrationCommand::Check { operation_id, migration_hash, .. }
            } if operation_id == operation && migration_hash == Some("ab".repeat(32))
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "migration",
                "apply",
                "--operation-id",
                operation,
                "--confirm-apply",
                &"ab".repeat(32),
            ])
            .expect("migration apply")
            .command,
            TopLevel::Migration {
                command: MigrationCommand::Apply { operation_id, confirm_apply, .. }
            } if operation_id == operation && confirm_apply == "ab".repeat(32)
        ));
        assert!(matches!(
            Cli::try_parse_from(["riffdb", "migration", "operation", operation])
                .expect("migration operation")
                .command,
            TopLevel::Migration {
                command: MigrationCommand::Operation { operation_id }
            } if operation_id == operation
        ));
        assert!(Cli::try_parse_from(["riffdb", "migration", "check"]).is_err());
        assert!(
            Cli::try_parse_from(["riffdb", "migration", "apply", "--operation-id", operation,])
                .is_err()
        );
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
            "--application",
            "riffdb.application.json",
        ])
        .expect("symbolic batch");
        assert!(matches!(
            cli.command,
            TopLevel::Command {
                command: CommandCommand::Batch {
                    command_name,
                    concurrency,
                    idempotency_field: None,
                    progress: true,
                    application: Some(application),
                    ..
                }
            } if command_name == "CreateTicket"
                && concurrency == "16"
                && application == "riffdb.application.json"
        ));
        let explicit = Cli::try_parse_from([
            "riffdb",
            "command",
            "batch",
            "CreateTicket",
            "tickets.jsonl",
            "--idempotency-field",
            "request_key",
        ])
        .expect("explicit standalone idempotency field");
        assert!(matches!(
            explicit.command,
            TopLevel::Command {
                command: CommandCommand::Batch {
                    idempotency_field: Some(field),
                    application: None,
                    ..
                }
            } if field == "request_key"
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

        let protected = Cli::try_parse_from([
            "riffdb",
            "role",
            "bind",
            "riffdb.application.json",
            "--role",
            "HelpdeskAgent",
            "--principal",
            "00000000-0000-0000-0000-000000000007",
            "--actor-kind",
            "service",
            "--audience",
            "helpdesk",
            "--credential-output",
            "helpdesk.credential",
            "--principal-facts",
            "principal-facts.json",
        ]);
        assert!(
            protected.is_ok(),
            "protected role binding must accept one operator-owned facts document: {protected:?}"
        );
    }

    #[test]
    fn export_commands_expose_only_closed_symbolic_scope_and_bounded_pages() {
        let operation = "018f2f85-3c20-7a31-8f11-112233445566";
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "export",
                "start",
                "--lineage",
                "TicketDesk",
                "--scope",
                "principal",
                "--entities",
                "--events",
                "--lease-seconds",
                "900",
                "--operation-id",
                operation,
            ])
            .expect("symbolic export start")
            .command,
            TopLevel::Export {
                command: ExportCommand::Start {
                    lineage,
                    scope: ExportScope::Principal,
                    entities: true,
                    events: true,
                    operation_id: Some(parsed),
                    ..
                }
            } if lineage == "TicketDesk" && parsed == operation
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "export",
                "page",
                "--operation-id",
                operation,
                "--cursor",
                "AQID",
                "--jsonl",
                "ticketdesk-0001.jsonl",
            ])
            .expect("bounded export page")
            .command,
            TopLevel::Export {
                command: ExportCommand::Page { max_rows, .. }
            } if max_rows == "500"
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "export",
                "start",
                "--lineage",
                "TicketDesk",
                "--scope",
                "whole",
                "--entity-type-id",
                "2",
            ])
            .is_err()
        );
    }

    #[test]
    fn reimport_commands_require_exact_source_identity_and_never_accept_raw_rows() {
        let campaign = "018f2f85-3c20-7a31-8f11-112233445566";
        let operation = "018f2f85-3c20-7a31-8f11-112233445577";
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "reimport",
                "start",
                "--lineage",
                "TicketDesk",
                "--scope",
                "whole",
                "--portability-manifest",
                "portability.json",
                "--export-manifest",
                "manifest.json",
                "--export-receipt",
                "receipt.json",
                "--campaign-id",
                campaign,
            ])
            .expect("closed reimport start")
            .command,
            TopLevel::Reimport {
                command: ReimportCommand::Start {
                    scope: ReimportScope::Whole,
                    campaign_id: Some(parsed),
                    ..
                }
            } if parsed == campaign
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "riffdb",
                "reimport",
                "page",
                "--campaign-id",
                campaign,
                "--export-operation-id",
                operation,
                "--page-number",
                "1",
                "--jsonl",
                "page.jsonl",
                "--page-hash",
                "0101010101010101010101010101010101010101010101010101010101010101",
                "--operation-complete",
                "--class-complete",
            ])
            .expect("hash-bound terminal page")
            .command,
            TopLevel::Reimport {
                command: ReimportCommand::Page {
                    operation_complete: true,
                    class_complete: true,
                    next_cursor: None,
                    ..
                }
            }
        ));
        assert!(
            Cli::try_parse_from([
                "riffdb",
                "reimport",
                "page",
                "--campaign-id",
                campaign,
                "--jsonl",
                "page.jsonl",
            ])
            .is_err(),
            "a raw JSONL import without source operation/page/hash is unrepresentable"
        );
    }

    #[test]
    fn aliases_and_extra_commands_fail_closed() {
        assert!(Cli::try_parse_from(["riffdb", "backup"]).is_err());
        assert!(
            Cli::try_parse_from(["riffdb", "backup", "create", ".maintenance"]).is_ok(),
            "clap leaves semantic backup-name validation to the checked type"
        );
        assert!(Cli::try_parse_from(["riffdb", "backup", "retire", "older"]).is_ok());
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
