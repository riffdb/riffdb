// req: TEST-001, TEST-003
//! Shared process, transport, and workspace fixtures for `public_guarantees`.

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use riffdb_api_mcp::MCP_PROTOCOL_VERSION;
use riffdb_client_rust::{
    ApplicationClientError, ApplicationCommand, ApplicationCommandResult, ApplicationContract,
    ApplicationValue, AttemptBudget, CallMetadata, DatabaseAlias, NamedQuery, NamedQueryResult,
    QueryOptions, RiffDbClient, StableApplicationClient, load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_driver_host::{
    ApplicationCatalog, BindingError, DriverRequest, DriverResponse, DriverRuntime,
    DriverSocketState, DriverValue, InvokeOptions,
};
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) const SERVER_CHILD_ENV: &str = "RIFFDB_PG_SERVER_CHILD";
pub(crate) const CLI_CHILD_ENV: &str = "RIFFDB_PG_CLI_CHILD";
pub(crate) const GRPC_AUDIENCE: &str = "riffdb-public-guarantees";
pub(crate) const ALLOWED_MCP_ORIGIN: &str = "http://127.0.0.1:3000";
pub(crate) const CONTRACT_LINEAGE: &str = "GuaranteeSuite";
pub(crate) const CONTRACT_VERSION: u64 = 1;
pub(crate) const PRIMARY_ROLE: &str = "GuaranteeRole";
pub(crate) const PK_SET_ONLY_ROLE: &str = "PkSetOnlyRole";

const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_HTTP_LINE_BYTES: usize = 8 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_HTTP_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_CLI_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

pub(crate) type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) const GUARANTEE_CONTRACT: &str = r#"
contract GuaranteeSuite version 1 {
  enum AccountSortOrder {
    ScoreDesc,
    ScoreAsc,
    AccountAsc,
  }

  entity Account {
    key (org_id: uuid, account_id: uuid)
    field credit_limit: i64
    field posted_total: i64
    field title: string<128>
    index by_title (org_id, title, account_id) text_key(title, binary_utf8_v1)
  }

  entity Charge {
    key (org_id: uuid, account_id: uuid, charge_id: uuid)
    field amount: i64
    field memo: string<128>
    reference account (org_id, account_id) -> Account(org_id, account_id)
  }

  aggregate Accounts {
    root Account
    child Charge
    partition_by org_id
    conflict_key (org_id, account_id)
    invariant within_credit_limit: posted_total <= credit_limit
  }

  entity AccountTag {
    key (org_id: uuid, account_id: uuid, tag_key: string<64>)
    field tag_val: string<64>
    field sort_score: i64
    index by_tag_score (org_id, tag_key, tag_val, sort_score, account_id)
      text_key(tag_key, binary_utf8_v1)
      text_key(tag_val, binary_utf8_v1)
    reference account (org_id, account_id) -> Account(org_id, account_id)
  }

  aggregate AccountTags {
    root AccountTag
    partition_by org_id
    conflict_key (org_id, account_id, tag_key)
  }

  entity TraceHeader {
    key (trace_id: string<64>)
    field trace_name: string<256>
    field status_summary: string<1048576>
  }

  aggregate TraceHeaders {
    root TraceHeader
    partition_by trace_id
    conflict_key (trace_id)
  }

  entity WideSpan {
    key (trace_id: string<64>, span_id: string<64>)
    field seq_no: i64
    field payload_text: string<1048576>
    field payload_blob: bytes<1048576>
    index by_seq (trace_id, seq_no, span_id)
  }

  aggregate WideSpans {
    root WideSpan
    partition_by trace_id
    conflict_key (trace_id, span_id)
  }

  entity TraceBucket {
    key (trace_id: string<64>, bucket_id: string<32>)
    field bucket_title: string<128>
    index by_trace_bucket (trace_id, bucket_id)
  }

  aggregate TraceBuckets {
    root TraceBucket
    partition_by trace_id
    conflict_key (trace_id, bucket_id)
  }

  entity TraceBucketNote {
    key (trace_id: string<64>, bucket_id: string<32>, note_id: string<32>)
    field group_label: string<300000>
    index by_bucket_note (trace_id, bucket_id, note_id)
    reference bucket (trace_id, bucket_id) -> TraceBucket(trace_id, bucket_id)
  }

  aggregate TraceBucketNotes {
    root TraceBucketNote
    partition_by trace_id
    conflict_key (trace_id, bucket_id, note_id)
  }

  entity MediumSpan {
    key (trace_id: string<64>, span_id: string<64>)
    field seq_no: i64
    field payload_text: string<100000>
    index by_seq (trace_id, seq_no, span_id)
  }

  aggregate MediumSpans {
    root MediumSpan
    partition_by trace_id
    conflict_key (trace_id, span_id)
  }

  command OpenAccount {
    input req_key: string<128>
    input org_id: uuid
    input account_id: uuid
    input credit_limit: i64
    input title: string<128>
    idempotency_key req_key
    create Account(org_id, account_id) as account
      else AlreadyOpen {}
    set account.credit_limit = credit_limit
    set account.posted_total = 0
    set account.title = title
    return Opened { posted_total: account.posted_total, credit_limit: account.credit_limit }
  }

  command PostCharge {
    input req_key: string<128>
    input org_id: uuid
    input account_id: uuid
    input charge_id: uuid
    input amount: i64
    input memo: string<128>
    idempotency_key req_key
    mutate Account(org_id, account_id) as account else AccountMissing {}
    create Charge(org_id, account_id, charge_id) as charge
      else DuplicateCharge {}
    set charge.amount = amount
    set charge.memo = memo
    set account.posted_total = account.posted_total + amount
    return Charged { new_total: account.posted_total }
  }

  command UpsertAccountTag {
    input req_key: string<128>
    input org_id: uuid
    input account_id: uuid
    input tag_key: string<64>
    input tag_val: string<64>
    input sort_score: i64
    idempotency_key req_key
    read Account(org_id, account_id) as account else AccountMissing {}
    create AccountTag(org_id, account_id, tag_key) as tag
      else TagExists {}
    set tag.tag_val = tag_val
    set tag.sort_score = sort_score
    return Tagged {}
  }

  command CreateTraceHeader {
    input req_key: string<128>
    input trace_id: string<64>
    input trace_name: string<256>
    input status_summary: string<1048576>
    idempotency_key req_key
    create TraceHeader(trace_id) as header
      else HeaderExists {}
    set header.trace_name = trace_name
    set header.status_summary = status_summary
    return HeaderCreated {}
  }

  command RecordWideSpan {
    input req_key: string<128>
    input trace_id: string<64>
    input span_id: string<64>
    input seq_no: i64
    input payload_text: string<1048576>
    input payload_blob: bytes<1048576>
    idempotency_key req_key
    create WideSpan(trace_id, span_id) as span
      else SpanExists {}
    set span.seq_no = seq_no
    set span.payload_text = payload_text
    set span.payload_blob = payload_blob
    return SpanRecorded { seq_no: span.seq_no }
  }

  command CreateTraceBucket {
    input req_key: string<128>
    input trace_id: string<64>
    input bucket_id: string<32>
    input bucket_title: string<128>
    idempotency_key req_key
    create TraceBucket(trace_id, bucket_id) as bucket
      else BucketExists {}
    set bucket.bucket_title = bucket_title
    return BucketCreated {}
  }

  command CreateTraceBucketNote {
    input req_key: string<128>
    input trace_id: string<64>
    input bucket_id: string<32>
    input note_id: string<32>
    input group_label: string<300000>
    idempotency_key req_key
    read TraceBucket(trace_id, bucket_id) as bucket else BucketMissing {}
    create TraceBucketNote(trace_id, bucket_id, note_id) as note
      else NoteExists {}
    set note.group_label = group_label
    return NoteCreated {}
  }

  command RecordMediumSpan {
    input req_key: string<128>
    input trace_id: string<64>
    input span_id: string<64>
    input seq_no: i64
    input payload_text: string<100000>
    idempotency_key req_key
    create MediumSpan(trace_id, span_id) as span
      else SpanExists {}
    set span.seq_no = seq_no
    set span.payload_text = payload_text
    return SpanRecorded { seq_no: span.seq_no }
  }

  command AdminOnlyPing {
    input req_key: string<128>
    input org_id: uuid
    input account_id: uuid
    idempotency_key req_key
    read Account(org_id, account_id) as account else Missing {}
    return Pong { posted_total: account.posted_total }
  }
}
"#;

pub(crate) const QUERY_GET_ACCOUNT: &str = r#"
query GetAccount(
    $org_id: Account.org_id,
    $account_id: Account.account_id,
) {
    one account from Account
        where org_id == $org_id && account_id == $account_id
        else NotFound

    return Found {
        account: account {
            org_id
            account_id
            credit_limit
            posted_total
            title
        }
    }

    outcomes Found | NotFound
}
"#;

pub(crate) const QUERY_GET_ACCOUNTS_BY_SET: &str = r#"
query GetAccountsBySet(
    $org_id: Account.org_id,
    $ids: Set<Account.account_id>,
) {
    many accounts from Account
        where org_id == $org_id && account_id in $ids
        order by account_id asc
        take 25

    return Found {
        accounts: accounts {
            org_id
            account_id
            credit_limit
            posted_total
            title
        }
    }

    outcomes Found
}
"#;

pub(crate) const QUERY_SCAN_ACCOUNTS_BY_TITLE: &str = r#"
query ScanAccountsByTitle(
    $org_id: Account.org_id,
) {
    many accounts from Account
        where org_id == $org_id
        order by title asc, account_id asc
        take 20

    return Found {
        accounts: accounts {
            account_id
            title
            posted_total
        }
    }

    outcomes Found
}
"#;

pub(crate) const QUERY_SEARCH_TAGGED_ACCOUNTS: &str = r#"
query SearchTaggedAccounts(
    $org_id: Account.org_id,
    $k1: AccountTag.tag_key,
    $v1: AccountTag.tag_val?,
    $k2: AccountTag.tag_key?,
    $v2: AccountTag.tag_val?,
    $order: AccountSortOrder,
    $limit: Limit<20>,
    $after: Cursor?,
) {
    candidates matching_ids: Account.account_id from intersect {
        AccountTag.account_id using by_tag_score
            where org_id == $org_id
              && tag_key == $k1
              && when $v1 { tag_val == $v1 },
        AccountTag.account_id using by_tag_score
            where org_id == $org_id
              && when $k2 { tag_key == $k2 }
              && when $v2 { tag_val == $v2 },
    } within 50 else WindowExceeded

    many rank_tags from AccountTag
        where org_id == $org_id
          && tag_key == "rank"
          && tag_val == "score"
        order by sort_score asc, account_id asc
        take 50

    many accounts from Account
        where org_id == $org_id
          && account_id in matching_ids
        order by $order {
            ScoreDesc: rank_tags.sort_score desc nulls last, account_id asc;
            ScoreAsc: rank_tags.sort_score asc nulls last, account_id asc;
            AccountAsc: account_id asc;
        }
        take $limit after $after else WindowExceeded

    return Found {
        accounts: accounts {
            account_id
            title
            posted_total
        }
    }

    outcomes Found | WindowExceeded
}
"#;

pub(crate) const QUERY_PAGE_WIDE_SPANS: &str = r#"
query PageWideSpansWithHeader(
    $trace_id: WideSpan.trace_id,
    $limit: Limit<10>,
    $after: Cursor?,
) {
    one header from TraceHeader
        where trace_id == $trace_id
        else MissingTrace

    many spans from WideSpan
        where trace_id == $trace_id
        order by seq_no asc, span_id asc
        take $limit after $after

    return Found {
        header: header {
            trace_id
            trace_name
            status_summary
        },
        spans: spans {
            trace_id
            span_id
            seq_no
            payload_text
            payload_blob
        }
    }

    outcomes Found | MissingTrace
}
"#;

pub(crate) const QUERY_PAGE_MEDIUM_SPANS_WITH_GROUPED_ROLLUP: &str = r#"
query PageMediumSpansWithGroupedRollup(
    $trace_id: MediumSpan.trace_id,
    $limit: Limit<10>,
    $after: Cursor?,
) {
    many spans from MediumSpan
        where trace_id == $trace_id
        order by seq_no asc, span_id asc
        take $limit after $after

    many buckets from TraceBucket
        where trace_id == $trace_id
        order by bucket_id asc
        take 1

    many notes from TraceBucketNote for each bucket in buckets
        where trace_id == $trace_id
          && bucket_id == bucket.bucket_id
        order by note_id asc
        take 2 per bucket

    aggregate note_groups from notes per buckets {
        group by group_label
        count() as note_count
        take 1
    }

    return Found {
        buckets: buckets {
            bucket_id
            bucket_title
            note_groups {
                group_label
                note_count
            }
        },
        spans: spans {
            trace_id
            span_id
            seq_no
            payload_text
        }
    }

    outcomes Found
}
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitProfile {
    StandardGroup,
    HardenedSync,
}

impl CommitProfile {
    pub(crate) const fn as_toml_str(self) -> &'static str {
        match self {
            Self::StandardGroup => "standard",
            Self::HardenedSync => "hardened",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ServerOptions {
    pub commit_profile: CommitProfile,
    pub environment: &'static str,
    pub authorization_diagnostics: Option<bool>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            commit_profile: CommitProfile::StandardGroup,
            environment: "development",
            authorization_diagnostics: None,
        }
    }
}

pub(crate) struct TestWorkspace {
    scratch: riffdb_testkit::scratch::ScratchDir,
    pub grpc_port: u16,
    pub mcp_port: u16,
    pub ca_cert_path: PathBuf,
    pub server_cert_path: PathBuf,
    pub server_key_path: PathBuf,
    pub capability_keys_path: PathBuf,
    pub idempotency_keys_path: PathBuf,
    pub database_path: PathBuf,
    pub backup_root: PathBuf,
    pub server_config_path: PathBuf,
    pub operator_config_path: PathBuf,
    pub operator_credential_path: PathBuf,
    pub app_dir: PathBuf,
}

impl TestWorkspace {
    pub(crate) fn new(label: &str) -> TestResult<Self> {
        let short_label = &label[..label.len().min(6)];
        let scratch = riffdb_testkit::scratch::ScratchDir::new(short_label)?;
        let root = scratch.path();
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;

        let grpc_port = reserve_loopback_port()?;
        let mcp_port = reserve_loopback_port()?;

        let server_cert_path = write_protected(
            root,
            "server.pem",
            include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-cert.pem"),
            0o444,
        )?;
        let server_key_path = write_protected(
            root,
            "server.key",
            include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-key.pem"),
            0o600,
        )?;
        let ca_cert_path = write_protected(
            root,
            "ca.pem",
            include_bytes!("../../crates/riffdb-server/tests/fixtures/test-ca.pem"),
            0o444,
        )?;
        let capability_keys_path =
            write_protected(root, "capability.keys", CAPABILITY_KEYS, 0o600)?;
        let idempotency_keys_path =
            write_protected(root, "idempotency.keys", IDEMPOTENCY_KEYS, 0o600)?;

        let backup_root = root.join("backups");
        fs::create_dir(&backup_root)?;
        fs::set_permissions(&backup_root, fs::Permissions::from_mode(0o700))?;

        let app_dir = root.join("app");
        fs::create_dir_all(app_dir.join("riffdb/queries"))?;

        Ok(Self {
            database_path: root.join("riffdb.redb"),
            server_config_path: root.join("riffdbd.toml"),
            operator_config_path: root.join("operator-client.toml"),
            operator_credential_path: root.join("operator.credential"),
            scratch,
            grpc_port,
            mcp_port,
            ca_cert_path,
            server_cert_path,
            server_key_path,
            capability_keys_path,
            idempotency_keys_path,
            backup_root,
            app_dir,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        self.scratch.path()
    }

    pub(crate) fn mcp_address(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.mcp_port))
    }

    pub(crate) fn mcp_audience(&self) -> String {
        format!("http://127.0.0.1:{}/mcp", self.mcp_port)
    }

    pub(crate) fn tls_config_for_port(&self, port: u16) -> TestResult<TlsClientConfig> {
        Ok(TlsClientConfig::new(
            CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.1:{port}"))?,
            ProtectedFilePath::new(self.ca_cert_path.clone())?,
            TlsServerIdentity::parse("127.0.0.1")?,
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(4).ok_or("pool bound")?,
            NonZeroU32::new(64).ok_or("stream bound")?,
        )?)
    }

    pub(crate) fn tls_config(&self) -> TestResult<TlsClientConfig> {
        self.tls_config_for_port(self.grpc_port)
    }

    pub(crate) fn write_server_config(&self, options: &ServerOptions) -> TestResult<()> {
        let auth_diag_line = options
            .authorization_diagnostics
            .map_or_else(String::new, |enabled| {
                format!("authorization_diagnostics = {enabled}\n")
            });
        let config = format!(
            concat!(
                "[server]\n",
                "database = {:?}\n",
                "environment = {:?}\n",
                "audience = {:?}\n",
                "redb_commit_profile = {:?}\n",
                "{}",
                "mcp_listen = \"127.0.0.1:{}\"\n",
                "mcp_origins = [{:?}]\n",
                "capability_keys = {:?}\n",
                "idempotency_keys = {:?}\n\n",
                "[server.application_listener]\n",
                "mode = \"direct_tls\"\n",
                "listen = \"127.0.0.1:{}\"\n",
                "public_endpoint = \"https://127.0.0.1:{}\"\n",
                "certificate_chain = {:?}\n",
                "private_key = {:?}\n\n",
                "[server.application_listener.bounds]\n",
                "max_connections = 64\n",
                "max_streams_per_connection = 128\n",
                "handshake_timeout_seconds = 5\n",
                "drain_timeout_seconds = 5\n\n",
                "[maintenance]\n",
                "backup_root = {:?}\n"
            ),
            self.database_path,
            options.environment,
            GRPC_AUDIENCE,
            options.commit_profile.as_toml_str(),
            auth_diag_line,
            self.mcp_port,
            ALLOWED_MCP_ORIGIN,
            self.capability_keys_path,
            self.idempotency_keys_path,
            self.grpc_port,
            self.grpc_port,
            self.server_cert_path,
            self.server_key_path,
            self.backup_root,
        );
        fs::write(&self.server_config_path, config)?;
        fs::set_permissions(&self.server_config_path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    pub(crate) fn spawn_server(&self, options: &ServerOptions) -> TestResult<RunningServer> {
        self.write_server_config(options)?;
        RunningServer::spawn(&self.server_config_path, self.grpc_port)
    }

    pub(crate) fn bootstrap_operator(&self) -> TestResult<()> {
        let bootstrap_req_path = self.root().join("bootstrap.json");
        let mut audiences = vec![GRPC_AUDIENCE.to_owned(), self.mcp_audience()];
        audiences.sort_unstable();
        let request = serde_json::json!({
            "principal_id": "pg-operator",
            "actor_kind": "human",
            "requested_lifetime_seconds": 86400,
            "audiences": audiences,
            "grant": {
                "tenant_scope": {"type": "global"},
                "partition_scope": {"type": "all"},
                "permissions": [
                    {"type": "validate_contract"},
                    {"type": "read_contract"},
                    {"type": "deploy_contract"},
                    {"type": "read_health"},
                    {"type": "create_capability"},
                    {"type": "revoke_capability"},
                    {"type": "administer_capabilities"}
                ],
                "field_visibility": [],
                "max_scan_rows": 5000,
                "approval_required": []
            }
        });
        fs::write(&bootstrap_req_path, serde_json::to_vec(&request)?)?;
        fs::set_permissions(&bootstrap_req_path, fs::Permissions::from_mode(0o600))?;

        let bootstrap_client_toml = self.root().join("bootstrap-client.toml");
        self.write_client_config(&bootstrap_client_toml, None)?;
        let bootstrap_material = self.root().join("bootstrap.material");

        require_cli_success(
            run_cli(
                [
                    "--config".into(),
                    bootstrap_client_toml.into_os_string(),
                    "capability".into(),
                    "bootstrap".into(),
                    "--request".into(),
                    bootstrap_req_path.into_os_string(),
                    "--generate".into(),
                    bootstrap_material.into_os_string(),
                    "--bearer-output".into(),
                    self.operator_credential_path.clone().into_os_string(),
                ],
                None,
            )?,
            "operator bootstrap",
        )?;

        self.write_client_config(
            &self.operator_config_path,
            Some(&self.operator_credential_path),
        )?;
        Ok(())
    }

    pub(crate) fn write_client_config(
        &self,
        path: &Path,
        credential: Option<&Path>,
    ) -> TestResult<()> {
        let credential_line =
            credential.map_or_else(String::new, |cred| format!("credential_file = {cred:?}\n"));
        let body = format!(
            "[client]\nendpoint = \"https://127.0.0.1:{}\"\ndatabase = \"default\"\noutput = \"json\"\nmax_attempts = 3\n{}tls_trust_root = {:?}\ntls_server_name = \"127.0.0.1\"\n",
            self.grpc_port, credential_line, self.ca_cert_path,
        );
        fs::write(path, body)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    pub(crate) fn write_guarantee_application(&self, role_environment: &str) -> TestResult<()> {
        fs::write(
            self.app_dir.join("riffdb/contract.riff"),
            GUARANTEE_CONTRACT,
        )?;
        fs::write(
            self.app_dir.join("riffdb/queries/get_account.riffq"),
            QUERY_GET_ACCOUNT,
        )?;
        fs::write(
            self.app_dir
                .join("riffdb/queries/get_accounts_by_set.riffq"),
            QUERY_GET_ACCOUNTS_BY_SET,
        )?;
        fs::write(
            self.app_dir
                .join("riffdb/queries/scan_accounts_by_title.riffq"),
            QUERY_SCAN_ACCOUNTS_BY_TITLE,
        )?;
        fs::write(
            self.app_dir
                .join("riffdb/queries/search_tagged_accounts.riffq"),
            QUERY_SEARCH_TAGGED_ACCOUNTS,
        )?;
        fs::write(
            self.app_dir.join("riffdb/queries/page_wide_spans.riffq"),
            QUERY_PAGE_WIDE_SPANS,
        )?;
        fs::write(
            self.app_dir
                .join("riffdb/queries/page_medium_spans_grouped.riffq"),
            QUERY_PAGE_MEDIUM_SPANS_WITH_GROUPED_ROLLUP,
        )?;

        let manifest = serde_json::json!({
            "schema": "riffdb.application-source/v6",
            "application": "public-guarantees",
            "contract": {
                "source": "riffdb/contract.riff",
                "lineage": CONTRACT_LINEAGE,
                "version": CONTRACT_VERSION
            },
            "generation": {
                "go": "generated/go/client.go",
                "rust": "generated/rust/client.rs",
                "typescript": "generated/typescript/client.ts",
                "python": "generated/python/client.py",
                "mcp": "generated/mcp/tools.json"
            },
            "migrations": [],
            "query_modules": [
                {
                    "name": "guarantee_queries",
                    "version": 1,
                    "queries": [
                        { "name": "GetAccount", "source": "riffdb/queries/get_account.riffq" },
                        { "name": "GetAccountsBySet", "source": "riffdb/queries/get_accounts_by_set.riffq" },
                        { "name": "ScanAccountsByTitle", "source": "riffdb/queries/scan_accounts_by_title.riffq" },
                        { "name": "SearchTaggedAccounts", "source": "riffdb/queries/search_tagged_accounts.riffq" },
                        { "name": "PageWideSpansWithHeader", "source": "riffdb/queries/page_wide_spans.riffq" },
                        { "name": "PageMediumSpansWithGroupedRollup", "source": "riffdb/queries/page_medium_spans_grouped.riffq" }
                    ]
                }
            ],
            "reactive_modules": [],
            "roles": [
                {
                    "name": PRIMARY_ROLE,
                    "environment": role_environment,
                    "tenant_scope": "global",
                    "queries": [
                        "GetAccount",
                        "GetAccountsBySet",
                        "ScanAccountsByTitle",
                        "SearchTaggedAccounts",
                        "PageWideSpansWithHeader",
                        "PageMediumSpansWithGroupedRollup"
                    ],
                    "commands": [
                        "CreateTraceBucket",
                        "CreateTraceBucketNote",
                        "CreateTraceHeader",
                        "OpenAccount",
                        "PostCharge",
                        "RecordMediumSpan",
                        "RecordWideSpan",
                        "UpsertAccountTag"
                    ],
                    "event_streams": [],
                    "watch_queries": [],
                    "agent_subscriptions": [],
                    "row_policies": []
                },
                {
                    "name": PK_SET_ONLY_ROLE,
                    "environment": role_environment,
                    "tenant_scope": "global",
                    "queries": ["GetAccountsBySet"],
                    "commands": [],
                    "event_streams": [],
                    "watch_queries": [],
                    "agent_subscriptions": [],
                    "row_policies": []
                },
                {
                    "name": "AdminOnlyRole",
                    "environment": role_environment,
                    "tenant_scope": "global",
                    "queries": ["GetAccount"],
                    "commands": ["AdminOnlyPing"],
                    "event_streams": [],
                    "watch_queries": [],
                    "agent_subscriptions": [],
                    "row_policies": []
                }
            ],
            "seed_inputs": []
        });
        fs::write(
            self.app_dir.join("riffdb.application.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        Ok(())
    }

    pub(crate) fn lock_and_deploy(&self, role_environment: &str) -> TestResult<DeployedApp> {
        self.write_guarantee_application(role_environment)?;

        require_cli_success(
            run_cli(
                [
                    "application".into(),
                    "lock".into(),
                    "riffdb.application.json".into(),
                    "--write".into(),
                ],
                Some(&self.app_dir),
            )?,
            "application lock --write",
        )?;

        require_cli_success(
            run_cli(
                [
                    "application".into(),
                    "generate".into(),
                    "riffdb.application.json".into(),
                    "--locked".into(),
                ],
                Some(&self.app_dir),
            )?,
            "application generate --locked",
        )?;

        require_cli_success(
            run_cli(
                [
                    "--config".into(),
                    self.operator_config_path.clone().into_os_string(),
                    "application".into(),
                    "deploy".into(),
                    "riffdb.application.json".into(),
                    "--lock".into(),
                    "riffdb.application.lock.json".into(),
                    "--provision-role".into(),
                    PRIMARY_ROLE.into(),
                    "--lifetime-seconds".into(),
                    "3600".into(),
                ],
                Some(&self.app_dir),
            )?,
            "application deploy",
        )?;

        let exact_manifest_path = self.app_dir.join("generated/riffdb.application.exact.json");
        let role_credential_path = self.root().join("primary-role.credential");
        let mut sorted_audiences = [GRPC_AUDIENCE.to_owned(), self.mcp_audience()];
        sorted_audiences.sort_unstable();
        require_cli_success(
            run_cli(
                [
                    "--config".into(),
                    self.operator_config_path.clone().into_os_string(),
                    "role".into(),
                    "bind".into(),
                    "riffdb.application.json".into(),
                    "--role".into(),
                    PRIMARY_ROLE.into(),
                    "--principal".into(),
                    "pg-primary-agent".into(),
                    "--actor-kind".into(),
                    "agent".into(),
                    "--lifetime-seconds".into(),
                    "3600".into(),
                    "--audience".into(),
                    sorted_audiences[0].clone().into(),
                    "--audience".into(),
                    sorted_audiences[1].clone().into(),
                    "--credential-output".into(),
                    role_credential_path.clone().into_os_string(),
                ],
                Some(&self.app_dir),
            )?,
            "role bind primary with MCP audience",
        )?;

        let role_client_config_path = self.root().join("role-client.toml");
        self.write_client_config(&role_client_config_path, Some(&role_credential_path))?;

        let lock_bytes = fs::read(self.app_dir.join("riffdb.application.lock.json"))?;
        let manifest_bytes = fs::read(&exact_manifest_path)?;
        let tools_bytes = fs::read(self.app_dir.join("generated/mcp/tools.json"))?;
        let catalog = ApplicationCatalog::from_exact_artifacts(
            &lock_bytes,
            &manifest_bytes,
            &tools_bytes,
            "default",
            PRIMARY_ROLE,
        )?;
        let lock_json: Value = serde_json::from_slice(&lock_bytes)?;
        let tools_json: Value = serde_json::from_slice(&tools_bytes)?;
        let contract_bundle_hex = lock_json["contract"]["bundle_hash"]
            .as_str()
            .ok_or("missing contract.bundle_hash in lock")?
            .to_owned();
        let contract_bundle_hash = decode_hex_32(&contract_bundle_hex)?;
        let query_module_hex = lock_json["modules"][0]["module_hash"]
            .as_str()
            .ok_or("missing modules[0].module_hash in lock")?
            .to_owned();
        let query_module_hash = decode_hex_32(&query_module_hex)?;

        Ok(DeployedApp {
            role_credential_path,
            role_client_config_path,
            catalog,
            tools_json,
            contract_bundle_hex,
            contract_bundle_hash,
            query_module_hash,
        })
    }

    pub(crate) fn bind_role_credential(
        &self,
        role_name: &str,
        principal: &str,
        filename: &str,
    ) -> TestResult<PathBuf> {
        let credential_path = self.root().join(filename);
        let mut sorted_audiences = [GRPC_AUDIENCE.to_owned(), self.mcp_audience()];
        sorted_audiences.sort_unstable();
        require_cli_success(
            run_cli(
                [
                    "--config".into(),
                    self.operator_config_path.clone().into_os_string(),
                    "role".into(),
                    "bind".into(),
                    "riffdb.application.json".into(),
                    "--role".into(),
                    role_name.into(),
                    "--principal".into(),
                    principal.into(),
                    "--actor-kind".into(),
                    "agent".into(),
                    "--lifetime-seconds".into(),
                    "3600".into(),
                    "--audience".into(),
                    sorted_audiences[0].clone().into(),
                    "--audience".into(),
                    sorted_audiences[1].clone().into(),
                    "--credential-output".into(),
                    credential_path.clone().into_os_string(),
                ],
                Some(&self.app_dir),
            )?,
            "role bind additional role",
        )?;
        Ok(credential_path)
    }

    pub(crate) async fn spawn_driver_host(
        &self,
        credential_path: &Path,
        role_name: &str,
        socket_name: &str,
    ) -> TestResult<RunningDriverHost> {
        let socket_path = self.root().join(socket_name);
        let driver_toml_path = self.root().join(format!("{socket_name}.toml"));
        let lock_path = self.app_dir.join("riffdb.application.lock.json");
        let manifest_path = self.app_dir.join("generated/riffdb.application.exact.json");
        let tools_path = self.app_dir.join("generated/mcp/tools.json");

        let config_body = format!(
            concat!(
                "[driver]\n",
                "socket = {:?}\n\n",
                "[application]\n",
                "lock_file = {:?}\n",
                "manifest_file = {:?}\n",
                "operation_catalog_file = {:?}\n",
                "database = \"default\"\n",
                "role = {:?}\n\n",
                "[remote]\n",
                "endpoint = \"https://127.0.0.1:{}\"\n",
                "tls_trust_root = {:?}\n",
                "tls_server_name = \"127.0.0.1\"\n",
                "credential_file = {:?}\n",
                "pool_connections = 4\n",
                "streams_per_connection = 64\n"
            ),
            socket_path,
            lock_path,
            manifest_path,
            tools_path,
            role_name,
            self.grpc_port,
            self.ca_cert_path,
            credential_path,
        );
        fs::write(&driver_toml_path, config_body)?;
        fs::set_permissions(&driver_toml_path, fs::Permissions::from_mode(0o600))?;

        RunningDriverHost::spawn(&driver_toml_path, socket_path).await
    }

    pub(crate) async fn connect_app_client_on_port(
        &self,
        port: u16,
        credential_path: &Path,
    ) -> TestResult<(StableApplicationClient, CallMetadata)> {
        let tls = self.tls_config_for_port(port)?;
        let credential = load_protected_bearer_credential(credential_path)?;
        let metadata =
            CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
        let client = StableApplicationClient::connect_verified_tls(&tls).await?;
        Ok((client, metadata))
    }

    pub(crate) async fn connect_app_client(
        &self,
        credential_path: &Path,
    ) -> TestResult<(StableApplicationClient, CallMetadata)> {
        self.connect_app_client_on_port(self.grpc_port, credential_path)
            .await
    }

    pub(crate) async fn connect_operator_grpc(&self) -> TestResult<(RiffDbClient, CallMetadata)> {
        let tls = self.tls_config()?;
        let credential = load_protected_bearer_credential(&self.operator_credential_path)?;
        let metadata =
            CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
        let client = RiffDbClient::connect_verified_tls(&tls).await?;
        Ok((client, metadata))
    }
}

pub(crate) struct DeployedApp {
    pub role_credential_path: PathBuf,
    pub role_client_config_path: PathBuf,
    pub catalog: ApplicationCatalog,
    pub tools_json: Value,
    pub contract_bundle_hex: String,
    pub contract_bundle_hash: [u8; 32],
    pub query_module_hash: [u8; 32],
}

impl DeployedApp {
    pub(crate) fn driver_command_request(
        &self,
        command_symbol: &str,
        input: BTreeMap<String, DriverValue>,
        maximum_attempts: u32,
    ) -> TestResult<DriverRequest> {
        let public_name = self.driver_operation_name(command_symbol)?;
        let spec = self
            .catalog
            .operation(&public_name)
            .ok_or_else(|| format!("missing driver operation {public_name}"))?;
        Ok(DriverRequest::Invoke {
            request_id: "pg-req".to_owned(),
            operation: public_name,
            input_schema_hash: spec.input_schema_hash().to_owned(),
            input,
            options: InvokeOptions {
                deadline_millis: 15_000,
                maximum_attempts,
                read_after_commit: None,
                cursor: None,
                query_consistency: None,
                accept_compact_result: false,
                accept_packed_result: false,
            },
        })
    }

    pub(crate) fn driver_query_request(
        &self,
        query_symbol: &str,
        input: BTreeMap<String, DriverValue>,
        cursor: Option<String>,
        read_after_commit: Option<u64>,
    ) -> TestResult<DriverRequest> {
        let public_name = self.driver_operation_name(query_symbol)?;
        let spec = self
            .catalog
            .operation(&public_name)
            .ok_or_else(|| format!("missing driver query {public_name}"))?;
        Ok(DriverRequest::Invoke {
            request_id: "pg-qry".to_owned(),
            operation: public_name,
            input_schema_hash: spec.input_schema_hash().to_owned(),
            input,
            options: InvokeOptions {
                deadline_millis: 15_000,
                maximum_attempts: 1,
                read_after_commit,
                cursor,
                query_consistency: None,
                accept_compact_result: false,
                accept_packed_result: false,
            },
        })
    }

    pub(crate) fn driver_operation_name(&self, symbol: &str) -> TestResult<String> {
        for section in ["commands", "tools", "sdk_tools"] {
            if let Some(items) = self.tools_json.get(section).and_then(Value::as_array) {
                for item in items {
                    if item.get("operation_name").and_then(Value::as_str) == Some(symbol)
                        && let Some(name) = item.get("name").and_then(Value::as_str)
                    {
                        return Ok(name.to_owned());
                    }
                }
            }
        }
        Err(format!("could not locate driver operation name for {symbol}").into())
    }

    pub(crate) fn mcp_tool_name(&self, symbol: &str) -> TestResult<String> {
        for section in ["commands", "tools", "sdk_tools"] {
            if let Some(items) = self.tools_json.get(section).and_then(Value::as_array) {
                for item in items {
                    if item.get("operation_name").and_then(Value::as_str) == Some(symbol)
                        && let Some(mcp_name) = item
                            .get("mcp_descriptor")
                            .and_then(|d| d.get("name"))
                            .and_then(Value::as_str)
                    {
                        return Ok(mcp_name.to_owned());
                    }
                }
            }
        }
        Err(format!("could not locate MCP descriptor name for {symbol}").into())
    }

    pub(crate) fn build_grpc_command(
        &self,
        command_symbol: &str,
        input: BTreeMap<String, ApplicationValue>,
    ) -> TestResult<ApplicationCommand> {
        Ok(ApplicationCommand::new(
            command_symbol.to_owned(),
            Some(CONTRACT_VERSION),
            input,
        )?)
    }

    pub(crate) fn build_grpc_query(
        &self,
        query_symbol: &str,
        parameters: BTreeMap<String, ApplicationValue>,
        cursor: Option<String>,
        read_after_commit: Option<u64>,
    ) -> TestResult<NamedQuery> {
        let public_name = self.driver_operation_name(query_symbol)?;
        let spec = self
            .catalog
            .operation(&public_name)
            .ok_or_else(|| format!("missing catalog entry for {public_name}"))?;
        let plan_hash = spec
            .plan_hash()
            .ok_or_else(|| format!("missing plan_hash for {public_name}"))?;
        let mut options = QueryOptions::new();
        if let Some(cursor) = cursor {
            options = options.after(cursor);
        }
        if let Some(seq) = read_after_commit {
            options = options.read_after_commit(seq);
        }
        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: CONTRACT_LINEAGE.to_owned(),
                version: CONTRACT_VERSION,
                bundle_hash: Some(self.contract_bundle_hash),
            },
            query_symbol.to_owned(),
            Some(self.query_module_hash),
            parameters,
            None,
        )?
        .expect_plan_hash(plan_hash)
        .with_options(options)?;
        Ok(query)
    }
}

pub(crate) async fn execute_grpc_command(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    command: ApplicationCommand,
    attempts: u32,
) -> Result<ApplicationCommandResult, ApplicationClientError> {
    let budget = AttemptBudget::new(attempts).expect("nonzero attempt budget");
    client.execute_command(command, budget, metadata).await
}

pub(crate) async fn execute_grpc_query(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    query: NamedQuery,
) -> Result<NamedQueryResult, ApplicationClientError> {
    client.execute_named_query(query, metadata).await
}

pub(crate) async fn execute_driver_command(
    socket: &mut DriverSocketState,
    app: &DeployedApp,
    command_symbol: &str,
    input: BTreeMap<String, DriverValue>,
    maximum_attempts: u32,
) -> Result<DriverResponse, BindingError> {
    let req = app
        .driver_command_request(command_symbol, input, maximum_attempts)
        .expect("valid driver command request");
    socket
        .invoke(
            CONTRACT_LINEAGE,
            CONTRACT_VERSION,
            Some(&app.contract_bundle_hex),
            req,
            true,
        )
        .await
        .map(|(resp, _)| resp)
}

pub(crate) async fn execute_driver_query(
    socket: &mut DriverSocketState,
    app: &DeployedApp,
    query_symbol: &str,
    input: BTreeMap<String, DriverValue>,
    cursor: Option<String>,
    read_after_commit: Option<u64>,
) -> Result<DriverResponse, BindingError> {
    let req = app
        .driver_query_request(query_symbol, input, cursor, read_after_commit)
        .expect("valid driver query request");
    socket
        .invoke(
            CONTRACT_LINEAGE,
            CONTRACT_VERSION,
            Some(&app.contract_bundle_hex),
            req,
            false,
        )
        .await
        .map(|(resp, _)| resp)
}

pub(crate) struct RunningServer {
    controller: Option<ChildProcessController>,
}

impl RunningServer {
    pub(crate) fn spawn(server_config: &Path, expected_port: u16) -> TestResult<Self> {
        let spec = ChildProcessSpec::new(std::env::current_exe()?)?
            .clear_environment()
            .arg("--projections-root")?
            .arg(server_config.with_extension("projections").into_os_string())?
            .arg("--config")?
            .arg(server_config.as_os_str().to_owned())?
            .env(SERVER_CHILD_ENV, "1")?;
        let controller = ChildProcessController::spawn(&spec)?;
        let ready = controller.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?;
        if ready != format!("{READY_PREFIX}127.0.0.1:{expected_port}") {
            return Err(format!("unexpected riffdbd readiness line: {ready}").into());
        }
        Ok(Self {
            controller: Some(controller),
        })
    }

    pub(crate) fn shutdown(&mut self) -> TestResult<()> {
        if let Some(mut controller) = self.controller.take() {
            let outcome = controller.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;
            if !outcome.status.success() {
                return Err(
                    format!("riffdbd exited non-zero on shutdown: {}", outcome.status).into(),
                );
            }
        }
        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(mut controller) = self.controller.take() {
            let _ = controller.shutdown_cleanly(b"shutdown\n", Duration::from_secs(5));
        }
    }
}

pub(crate) struct RunningDriverHost {
    pub socket_path: PathBuf,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), riffdb_driver_host::DriverRuntimeError>>>,
}

impl RunningDriverHost {
    pub(crate) async fn spawn(config_path: &Path, socket_path: PathBuf) -> TestResult<Self> {
        let runtime = DriverRuntime::from_config_file(config_path)
            .await
            .map_err(|err| DriverRuntime::describe_config_error(config_path, err))?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            runtime
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Ok(Self {
            socket_path,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    pub(crate) async fn connect(&self, role_name: &str) -> TestResult<DriverSocketState> {
        let path_str = self
            .socket_path
            .to_str()
            .ok_or("non-UTF-8 driver socket path")?;
        DriverSocketState::connect(path_str, Some("default"), Some(role_name))
            .await
            .map_err(|err| format!("driver socket connect failed: {:?}", err.kind).into())
    }

    pub(crate) async fn shutdown(&mut self) -> TestResult<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let res = tokio::time::timeout(STOP_TIMEOUT, task).await??;
            res.map_err(|err| format!("driver host serve_until failed: {err}"))?;
        }
        Ok(())
    }
}

impl Drop for RunningDriverHost {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(crate) struct CliExit {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub(crate) fn run_cli(
    arguments: impl IntoIterator<Item = OsString>,
    working_directory: Option<&Path>,
) -> TestResult<CliExit> {
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command.env_clear().env(CLI_CHILD_ENV, "1").args(arguments);
    if let Some(dir) = working_directory {
        command.current_dir(dir);
    }
    let output = command.output()?;
    if output.stdout.len() > MAX_CLI_OUTPUT_BYTES || output.stderr.len() > MAX_CLI_OUTPUT_BYTES {
        return Err("CLI child output exceeded test bound".into());
    }
    Ok(CliExit {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

pub(crate) fn require_cli_success(exit: CliExit, label: &str) -> TestResult<CliExit> {
    if !exit.status.success() {
        return Err(format!(
            "{label} failed ({}): stdout={} stderr={}",
            exit.status,
            String::from_utf8_lossy(&exit.stdout),
            String::from_utf8_lossy(&exit.stderr)
        )
        .into());
    }
    Ok(exit)
}

#[derive(Clone)]
pub(crate) struct McpSession {
    address: SocketAddr,
    bearer_token: String,
    session_id: String,
}

impl McpSession {
    pub(crate) fn connect(address: SocketAddr, credential_path: &Path) -> TestResult<Self> {
        let raw_token = fs::read_to_string(credential_path)?;
        let bearer_token = raw_token.trim().to_owned();
        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "public-guarantees", "version": "1.0" }
            }
        });
        let init_resp = mcp_http_post(
            address,
            &bearer_token,
            None,
            &serde_json::to_vec(&init_body)?,
        )?;
        if init_resp.status != 200 {
            return Err(format!(
                "MCP initialize failed with status {}: {}",
                init_resp.status,
                String::from_utf8_lossy(&init_resp.body)
            )
            .into());
        }
        let session_id = init_resp
            .header("mcp-session-id")
            .ok_or("MCP initialize omitted mcp-session-id")?
            .to_owned();

        let notify_body = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let notify_resp = mcp_http_post(address, &bearer_token, Some(&session_id), notify_body)?;
        if !(200..300).contains(&notify_resp.status) {
            return Err(format!(
                "MCP notifications/initialized returned status {}",
                notify_resp.status
            )
            .into());
        }

        Ok(Self {
            address,
            bearer_token,
            session_id,
        })
    }

    pub(crate) fn call_tool(&self, name: &str, arguments: Value) -> TestResult<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            }
        });
        let resp = mcp_http_post(
            self.address,
            &self.bearer_token,
            Some(&self.session_id),
            &serde_json::to_vec(&body)?,
        )?;
        if resp.status != 200 {
            return Err(format!(
                "MCP tools/call({name}) returned HTTP {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )
            .into());
        }
        parse_mcp_json_body(&resp)
    }
}

pub(crate) struct McpStdioSession {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl McpStdioSession {
    pub(crate) fn spawn(ws: &TestWorkspace, credential_path: &Path) -> TestResult<Self> {
        let bin = resolve_riffdb_mcp_bin()?;
        let mut child = std::process::Command::new(bin)
            .env_clear()
            .env("RIFFDB_MCP_CREDENTIAL_FILE", credential_path)
            .arg("--endpoint")
            .arg(format!("https://127.0.0.1:{}", ws.grpc_port))
            .arg("--database")
            .arg("default")
            .arg("--tls-trust-root")
            .arg(&ws.ca_cert_path)
            .arg("--tls-server-name")
            .arg("127.0.0.1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().ok_or("missing riffdb-mcp stdin")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("missing riffdb-mcp stdout")?);
        let mut session = Self {
            child,
            stdin: Some(stdin),
            stdout,
            next_id: 1,
        };
        let init = serde_json::json!({
            "jsonrpc": "2.0",
            "id": session.next_id,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "public-guarantees-stdio", "version": "1.0" }
            }
        });
        session.next_id += 1;
        let init_resp = session.roundtrip(&init)?;
        if init_resp.get("error").is_some() {
            return Err(format!("riffdb-mcp initialize failed: {init_resp}").into());
        }
        let stdin_ref = session.stdin.as_mut().ok_or("missing riffdb-mcp stdin")?;
        writeln!(
            stdin_ref,
            "{}",
            serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"})
        )?;
        stdin_ref.flush()?;
        Ok(session)
    }

    pub(crate) fn call_tool(&mut self, name: &str, arguments: Value) -> TestResult<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments,
            }
        });
        self.roundtrip(&req)
    }

    fn roundtrip(&mut self, req: &Value) -> TestResult<Value> {
        let encoded = serde_json::to_vec(req)?;
        let stdin_ref = self.stdin.as_mut().ok_or("missing riffdb-mcp stdin")?;
        stdin_ref.write_all(&encoded)?;
        stdin_ref.write_all(b"\n")?;
        stdin_ref.flush()?;

        let mut buf = Vec::new();
        let mut limited = (&mut self.stdout).take((MAX_HTTP_BODY_BYTES + 1) as u64);
        limited.read_until(b'\n', &mut buf)?;
        if buf.is_empty() {
            return Err("riffdb-mcp closed stdout unexpectedly".into());
        }
        if buf.len() > MAX_HTTP_BODY_BYTES {
            return Err("riffdb-mcp stdout frame exceeded bound".into());
        }
        Ok(serde_json::from_slice(&buf)?)
    }

    pub(crate) fn shutdown(mut self) -> TestResult<()> {
        drop(self.stdin.take());
        let status = self.child.wait()?;
        if !status.success() {
            return Err(format!("riffdb-mcp exited non-zero: {status}").into());
        }
        Ok(())
    }
}

impl Drop for McpStdioSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn resolve_riffdb_mcp_bin() -> TestResult<PathBuf> {
    let exe = std::env::current_exe()?;
    if let Some(deps_dir) = exe.parent()
        && let Some(profile_dir) = deps_dir.parent()
    {
        let candidate = profile_dir.join("riffdb-mcp");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or("missing workspace root")?;
    let status = std::process::Command::new("cargo")
        .current_dir(workspace_root)
        .args([
            "build",
            "--locked",
            "--quiet",
            "-p",
            "riffdb-mcp-stdio",
            "--bin",
            "riffdb-mcp",
        ])
        .status()?;
    if !status.success() {
        return Err("failed to build riffdb-mcp binary".into());
    }
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace_root.join("target"), PathBuf::from);
    let built = target_dir.join("debug/riffdb-mcp");
    if !built.is_file() {
        return Err(format!("missing built riffdb-mcp at {}", built.display()).into());
    }
    Ok(built)
}

fn parse_mcp_json_body(resp: &RawHttpResponse) -> TestResult<Value> {
    let decoded = if resp
        .header("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
    {
        decode_chunked_body(&resp.body)?
    } else {
        resp.body.clone()
    };
    let text = String::from_utf8_lossy(&decoded);
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Ok(serde_json::from_str(trimmed)?);
    }
    for line in trimmed.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            let candidate = rest.trim();
            if candidate.starts_with('{') {
                return Ok(serde_json::from_str(candidate)?);
            }
        }
    }
    Err(format!("unable to parse MCP JSON response body: {trimmed}").into())
}

fn decode_chunked_body(raw: &[u8]) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0_usize;
    while pos < raw.len() {
        let Some(rel_end) = raw[pos..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_str = std::str::from_utf8(&raw[pos..pos + rel_end])?
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let chunk_size = usize::from_str_radix(size_str, 16)?;
        pos += rel_end + 2;
        if chunk_size == 0 {
            break;
        }
        if pos + chunk_size > raw.len() {
            return Err("truncated HTTP chunked body".into());
        }
        out.extend_from_slice(&raw[pos..pos + chunk_size]);
        pos += chunk_size;
        if raw.get(pos..pos + 2) == Some(b"\r\n") {
            pos += 2;
        }
    }
    Ok(out)
}

struct RawHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RawHttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn mcp_http_post(
    address: SocketAddr,
    bearer_token: &str,
    session_id: Option<&str>,
    body: &[u8],
) -> TestResult<RawHttpResponse> {
    let mut stream = TcpStream::connect_timeout(&address, HTTP_TIMEOUT)?;
    stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: {address}\r\nOrigin: {ALLOWED_MCP_ORIGIN}\r\nAuthorization: Bearer {bearer_token}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMcp-Protocol-Version: {MCP_PROTOCOL_VERSION}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )?;
    if let Some(session_id) = session_id {
        write!(stream, "Mcp-Session-Id: {session_id}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let status_line = read_bounded_http_line(&mut reader)?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("missing HTTP status")?
        .parse::<u16>()?;
    let mut headers = Vec::new();
    for _ in 0..MAX_HTTP_HEADERS {
        let line = read_bounded_http_line(&mut reader)?;
        if line.is_empty() {
            break;
        }
        let (name, val) = line.split_once(':').ok_or("malformed HTTP header")?;
        headers.push((name.trim().to_ascii_lowercase(), val.trim().to_owned()));
    }
    let mut resp_body = Vec::new();
    reader
        .take((MAX_HTTP_BODY_BYTES + 1) as u64)
        .read_to_end(&mut resp_body)?;
    if resp_body.len() > MAX_HTTP_BODY_BYTES {
        return Err("MCP HTTP response body exceeded bound".into());
    }
    Ok(RawHttpResponse {
        status,
        headers,
        body: resp_body,
    })
}

fn read_bounded_http_line(reader: &mut BufReader<TcpStream>) -> TestResult<String> {
    let mut buf = Vec::new();
    let mut limited = reader.take((MAX_HTTP_LINE_BYTES + 1) as u64);
    limited.read_until(b'\n', &mut buf)?;
    if buf.len() > MAX_HTTP_LINE_BYTES {
        return Err("HTTP line exceeded bound".into());
    }
    if buf.ends_with(b"\n") {
        buf.pop();
        if buf.ends_with(b"\r") {
            buf.pop();
        }
    }
    Ok(String::from_utf8(buf)?)
}

pub(crate) fn reserve_loopback_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

pub(crate) fn write_protected(
    root: &Path,
    name: &str,
    bytes: &[u8],
    mode: u32,
) -> io::Result<PathBuf> {
    let path = root.join(name);
    fs::write(&path, bytes)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
    Ok(path)
}

pub(crate) fn uuid_str(index: u32) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-{index:012x}")
}

pub(crate) fn app_uuid(index: u32) -> ApplicationValue {
    ApplicationValue::Uuid(
        riffdb_client_rust::ApplicationUuid::from_text(uuid_str(index)).expect("valid UUID"),
    )
}

pub(crate) fn decode_hex_32(hex: &str) -> TestResult<[u8; 32]> {
    if hex.len() != 64 {
        return Err(format!("expected 64-char hex digest, got len {}", hex.len()).into());
    }
    let mut out = [0_u8; 32];
    for (idx, slot) in out.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[idx * 2..idx * 2 + 2], 16)?;
    }
    Ok(out)
}
