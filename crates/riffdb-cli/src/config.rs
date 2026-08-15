use std::env;
use std::ffi::{OsStr, OsString};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_types::DatabaseAlias;
use serde::Deserialize;

use crate::cli::{Cli, OutputMode};
use crate::input::{MAX_CONFIG_BYTES, read_file, validate_path};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7443";
const DEFAULT_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub(crate) struct EffectiveConfig {
    pub(crate) endpoint: String,
    pub(crate) database: DatabaseAlias,
    pub(crate) output: OutputMode,
    pub(crate) max_attempts: u32,
    pub(crate) credential_file: Option<PathBuf>,
    pub(crate) tls: Option<TlsClientConfig>,
    pub(crate) config_file: Option<PathBuf>,
    pub(crate) project_mode: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigError {
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionError {
    output: OutputMode,
}

impl ResolutionError {
    pub(crate) const fn output(self) -> OutputMode {
        self.output
    }
}

pub(crate) trait Environment {
    fn value(&self, name: &str) -> Option<OsString>;
}

pub(crate) struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn value(&self, name: &str) -> Option<OsString> {
        env::var_os(name)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    client: Option<ClientDocument>,
    project: Option<ProjectDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectDocument {
    schema: String,
    generators: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ProjectGenerator {
    Rust,
    Go,
    Typescript,
    Python,
}

impl ProjectGenerator {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Go => "go",
            Self::Typescript => "typescript",
            Self::Python => "python",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "rust" => Some(Self::Rust),
            "go" => Some(Self::Go),
            "typescript" => Some(Self::Typescript),
            "python" => Some(Self::Python),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectConfig {
    root: PathBuf,
    schema: PathBuf,
    generators: Vec<ProjectGenerator>,
    endpoint: String,
    database: String,
}

impl ProjectConfig {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn schema(&self) -> &Path {
        &self.schema
    }

    pub(crate) fn generators(&self) -> &[ProjectGenerator] {
        &self.generators
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientDocument {
    endpoint: Option<String>,
    database: Option<String>,
    output: Option<String>,
    max_attempts: Option<u32>,
    credential_file: Option<String>,
    tls_trust_root: Option<String>,
    tls_server_name: Option<String>,
}

pub(crate) fn resolve(
    cli: &Cli,
    environment: &dyn Environment,
) -> Result<EffectiveConfig, ResolutionError> {
    let environment_output = environment.value("RIFFDB_OUTPUT");
    let explicit_output = explicit_output(cli.output, environment_output.as_ref());
    let config_path = selected_os(
        cli.config.as_ref(),
        environment.value("RIFFDB_CONFIG"),
        None,
    )
    .map_err(|_| resolution_error(explicit_output))?;
    let document = match config_path.as_ref() {
        Some(path) => {
            read_document(Path::new(path)).map_err(|_| resolution_error(explicit_output))?
        }
        None => ConfigDocument::default(),
    };
    let client = document.client.unwrap_or_default();

    let output = match selected_string(
        cli.output.map(output_text),
        environment_output,
        client.output.as_deref(),
        "human",
    )
    .map_err(|_| resolution_error(explicit_output))?
    .as_str()
    {
        "human" => OutputMode::Human,
        "json" => OutputMode::Json,
        _ => return Err(resolution_error(explicit_output)),
    };

    let endpoint = selected_string(
        cli.endpoint.as_deref(),
        environment.value("RIFFDB_ENDPOINT"),
        client.endpoint.as_deref(),
        DEFAULT_ENDPOINT,
    )
    .map_err(|_| resolution_error(output))?;
    validate_endpoint(&endpoint).map_err(|_| resolution_error(output))?;
    let database = selected_string(
        cli.database.as_deref(),
        environment.value("RIFFDB_DATABASE"),
        client.database.as_deref(),
        riffdb_types::DEFAULT_DATABASE_ALIAS,
    )
    .map_err(|_| resolution_error(output))
    .and_then(|value| DatabaseAlias::new(value).map_err(|_| resolution_error(output)))?;

    let attempts = match (&cli.max_attempts, environment.value("RIFFDB_MAX_ATTEMPTS")) {
        (Some(value), _) => parse_attempts(value).map_err(|_| resolution_error(output))?,
        (None, Some(value)) => parse_os_attempts(&value).map_err(|_| resolution_error(output))?,
        (None, None) => client.max_attempts.unwrap_or(DEFAULT_ATTEMPTS),
    };
    if !(1..=10).contains(&attempts) {
        return Err(resolution_error(output));
    }

    let credential_file = selected_os(
        cli.credential_file.as_ref(),
        environment.value("RIFFDB_CREDENTIAL_FILE"),
        client.credential_file.map(OsString::from),
    )
    .map_err(|_| resolution_error(output))?
    .map(PathBuf::from);
    if let Some(path) = &credential_file {
        validate_path(path.as_os_str()).map_err(|_| resolution_error(output))?;
    }

    let tls_trust_root = selected_optional_string(
        environment.value("RIFFDB_TLS_TRUST_ROOT"),
        client.tls_trust_root.as_deref(),
    )
    .map_err(|_| resolution_error(output))?;
    let tls_server_name = selected_optional_string(
        environment.value("RIFFDB_TLS_SERVER_NAME"),
        client.tls_server_name.as_deref(),
    )
    .map_err(|_| resolution_error(output))?;
    let tls = match (
        endpoint.starts_with("https://"),
        tls_trust_root,
        tls_server_name,
    ) {
        (false, None, None) => None,
        (true, Some(trust_root), Some(server_name)) => Some(
            TlsClientConfig::new(
                CanonicalHttpsEndpoint::parse(&endpoint).map_err(|_| resolution_error(output))?,
                ProtectedFilePath::new(PathBuf::from(trust_root))
                    .map_err(|_| resolution_error(output))?,
                TlsServerIdentity::parse(&server_name).map_err(|_| resolution_error(output))?,
                Duration::from_secs(5),
                Duration::from_secs(30),
                NonZeroU32::new(4).expect("fixed nonzero pool bound"),
                NonZeroU32::new(64).expect("fixed nonzero stream bound"),
            )
            .map_err(|_| resolution_error(output))?,
        ),
        (false, Some(_), _) | (false, _, Some(_)) | (true, _, _) => {
            return Err(resolution_error(output));
        }
    };

    Ok(EffectiveConfig {
        endpoint,
        database,
        output,
        max_attempts: attempts,
        credential_file,
        tls,
        config_file: config_path.map(PathBuf::from),
        project_mode: false,
    })
}

const fn resolution_error(output: OutputMode) -> ResolutionError {
    ResolutionError { output }
}

fn explicit_output(flag: Option<OutputMode>, environment: Option<&OsString>) -> OutputMode {
    if let Some(output) = flag {
        return output;
    }
    match environment.and_then(|value| value.to_str()) {
        Some("json") => OutputMode::Json,
        _ => OutputMode::Human,
    }
}

fn read_document(path: &Path) -> Result<ConfigDocument, ConfigError> {
    let bytes = read_file(path, MAX_CONFIG_BYTES).map_err(|_| ConfigError::Invalid)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| ConfigError::Invalid)?;
    toml::from_str(text).map_err(|_| ConfigError::Invalid)
}

pub(crate) fn load_project(path: &Path) -> Result<ProjectConfig, ConfigError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ConfigError::Invalid)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ConfigError::Invalid);
    }
    let document = read_document(path)?;
    let client = document.client.unwrap_or_default();
    let endpoint = client
        .endpoint
        .unwrap_or_else(|| "http://127.0.0.1:7443".to_owned());
    validate_endpoint(&endpoint)?;
    let database = client
        .database
        .unwrap_or_else(|| riffdb_types::DEFAULT_DATABASE_ALIAS.to_owned());
    riffdb_types::DatabaseAlias::new(&database).map_err(|_| ConfigError::Invalid)?;
    let project = document.project.ok_or(ConfigError::Invalid)?;
    let schema = checked_project_path(&project.schema)?;
    if project.generators.is_empty() || project.generators.len() > 4 {
        return Err(ConfigError::Invalid);
    }
    let mut generators = project
        .generators
        .iter()
        .map(|value| ProjectGenerator::parse(value).ok_or(ConfigError::Invalid))
        .collect::<Result<Vec<_>, _>>()?;
    generators.sort_unstable();
    let before = generators.len();
    generators.dedup();
    if generators.len() != before {
        return Err(ConfigError::Invalid);
    }
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let root = std::fs::canonicalize(root).map_err(|_| ConfigError::Invalid)?;
    Ok(ProjectConfig {
        schema: root.join(schema),
        root,
        generators,
        endpoint,
        database,
    })
}

fn checked_project_path(value: &str) -> Result<PathBuf, ConfigError> {
    use std::path::Component;

    if value.is_empty() || value.len() > crate::input::MAX_PATH_BYTES {
        return Err(ConfigError::Invalid);
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ConfigError::Invalid);
    }
    Ok(path.to_path_buf())
}

fn selected_string(
    flag: Option<&str>,
    environment: Option<OsString>,
    document: Option<&str>,
    default: &str,
) -> Result<String, ConfigError> {
    if let Some(value) = flag {
        return nonempty(value);
    }
    if let Some(value) = environment {
        return value
            .into_string()
            .map_err(|_| ConfigError::Invalid)
            .and_then(|value| nonempty(&value));
    }
    document.map_or_else(|| Ok(default.to_owned()), nonempty)
}

fn selected_os(
    flag: Option<&OsString>,
    environment: Option<OsString>,
    document: Option<OsString>,
) -> Result<Option<OsString>, ConfigError> {
    let selected = flag.cloned().or(environment).or(document);
    if let Some(value) = &selected {
        validate_path(value).map_err(|_| ConfigError::Invalid)?;
    }
    Ok(selected)
}

fn selected_optional_string(
    environment: Option<OsString>,
    document: Option<&str>,
) -> Result<Option<String>, ConfigError> {
    match environment {
        Some(value) => value
            .into_string()
            .map_err(|_| ConfigError::Invalid)
            .and_then(|value| nonempty(&value))
            .map(Some),
        None => document.map(nonempty).transpose(),
    }
}

fn nonempty(value: &str) -> Result<String, ConfigError> {
    if value.is_empty() {
        Err(ConfigError::Invalid)
    } else {
        Ok(value.to_owned())
    }
}

fn parse_os_attempts(value: &OsStr) -> Result<u32, ConfigError> {
    value
        .to_str()
        .ok_or(ConfigError::Invalid)
        .and_then(parse_attempts)
}

fn parse_attempts(value: &str) -> Result<u32, ConfigError> {
    if !canonical_unsigned(value) {
        return Err(ConfigError::Invalid);
    }
    value.parse().map_err(|_| ConfigError::Invalid)
}

fn canonical_unsigned(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

fn output_text(value: OutputMode) -> &'static str {
    match value {
        OutputMode::Human => "human",
        OutputMode::Json => "json",
    }
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<(), ConfigError> {
    if endpoint.is_empty()
        || endpoint.len() > 512
        || !endpoint.is_ascii()
        || endpoint.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(ConfigError::Invalid);
    }
    if endpoint.starts_with("https://") {
        return CanonicalHttpsEndpoint::parse(endpoint)
            .map(|_| ())
            .map_err(|_| ConfigError::Invalid);
    }
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or(ConfigError::Invalid)?;
    if authority.contains(['/', '?', '#', '@']) {
        return Err(ConfigError::Invalid);
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or(ConfigError::Invalid)?;
        if host.is_empty() || port.contains(':') {
            return Err(ConfigError::Invalid);
        }
        (host, port)
    } else {
        let (host, port) = authority.rsplit_once(':').ok_or(ConfigError::Invalid)?;
        if host.is_empty() || host.contains(':') {
            return Err(ConfigError::Invalid);
        }
        (host, port)
    };
    let address: IpAddr = host.parse().map_err(|_| ConfigError::Invalid)?;
    if !address.is_loopback() || !canonical_unsigned(port) {
        return Err(ConfigError::Invalid);
    }
    let port: u16 = port.parse().map_err(|_| ConfigError::Invalid)?;
    if port == 0 {
        return Err(ConfigError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use clap::Parser;

    use super::*;

    #[derive(Default)]
    struct TestEnvironment(BTreeMap<String, OsString>);

    impl Environment for TestEnvironment {
        fn value(&self, name: &str) -> Option<OsString> {
            self.0.get(name).cloned()
        }
    }

    fn health_cli(arguments: &[&str]) -> Cli {
        Cli::try_parse_from(
            std::iter::once("riffdb")
                .chain(arguments.iter().copied())
                .chain(["server", "health"]),
        )
        .expect("CLI")
    }

    #[test]
    fn defaults_and_flag_over_environment_precedence_are_exact() {
        let defaults = resolve(&health_cli(&[]), &TestEnvironment::default()).expect("defaults");
        assert_eq!(defaults.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(
            defaults.database.as_str(),
            riffdb_types::DEFAULT_DATABASE_ALIAS
        );
        assert_eq!(defaults.output, OutputMode::Human);
        assert_eq!(defaults.max_attempts, 3);

        let mut environment = TestEnvironment::default();
        environment
            .0
            .insert("RIFFDB_ENDPOINT".into(), "http://127.0.0.1:8000".into());
        let selected = resolve(
            &health_cli(&["--endpoint", "http://[::1]:9000"]),
            &environment,
        )
        .expect("flag");
        assert_eq!(selected.endpoint, "http://[::1]:9000");
    }

    #[test]
    fn database_precedence_and_canonical_validation_are_exact() {
        let (_scratch, path) = temporary_file(
            "database-precedence",
            b"[client]\ndatabase = \"document_db\"\n",
        );
        let mut environment = TestEnvironment::default();
        environment
            .0
            .insert("RIFFDB_CONFIG".into(), path.as_os_str().to_owned());
        assert_eq!(
            resolve(&health_cli(&[]), &environment)
                .expect("document")
                .database
                .as_str(),
            "document_db"
        );
        environment
            .0
            .insert("RIFFDB_DATABASE".into(), "environment_db".into());
        assert_eq!(
            resolve(&health_cli(&[]), &environment)
                .expect("environment")
                .database
                .as_str(),
            "environment_db"
        );
        assert_eq!(
            resolve(&health_cli(&["--database", "argument_db"]), &environment)
                .expect("argument")
                .database
                .as_str(),
            "argument_db"
        );
        assert!(resolve(&health_cli(&["--database", "Invalid"]), &environment).is_err());
    }

    #[test]
    fn every_configuration_source_obeys_fieldwise_precedence() {
        let (_scratch, path) = temporary_file(
            "precedence",
            concat!(
                "[client]\n",
                "endpoint = \"http://127.0.0.1:7001\"\n",
                "output = \"human\"\n",
                "max_attempts = 2\n",
                "credential_file = \"/toml-token\"\n",
            )
            .as_bytes(),
        );
        let path_text = path.to_str().expect("UTF-8 temp path");
        let mut environment = TestEnvironment::default();
        environment
            .0
            .insert("RIFFDB_CONFIG".into(), path_text.into());

        let from_toml = resolve(&health_cli(&[]), &environment).expect("TOML");
        assert_eq!(from_toml.endpoint, "http://127.0.0.1:7001");
        assert_eq!(from_toml.output, OutputMode::Human);
        assert_eq!(from_toml.max_attempts, 2);
        assert_eq!(
            from_toml.credential_file,
            Some(PathBuf::from("/toml-token"))
        );

        environment
            .0
            .insert("RIFFDB_ENDPOINT".into(), "http://127.0.0.1:7002".into());
        environment.0.insert("RIFFDB_OUTPUT".into(), "json".into());
        environment
            .0
            .insert("RIFFDB_MAX_ATTEMPTS".into(), "4".into());
        environment
            .0
            .insert("RIFFDB_CREDENTIAL_FILE".into(), "/env-token".into());
        let from_environment = resolve(&health_cli(&[]), &environment).expect("environment");
        assert_eq!(from_environment.endpoint, "http://127.0.0.1:7002");
        assert_eq!(from_environment.output, OutputMode::Json);
        assert_eq!(from_environment.max_attempts, 4);
        assert_eq!(
            from_environment.credential_file,
            Some(PathBuf::from("/env-token"))
        );

        let from_flags = resolve(
            &health_cli(&[
                "--config",
                path_text,
                "--endpoint",
                "http://[::1]:7003",
                "--output",
                "human",
                "--max-attempts",
                "6",
                "--credential-file",
                "/flag-token",
            ]),
            &environment,
        )
        .expect("flags");
        assert_eq!(from_flags.endpoint, "http://[::1]:7003");
        assert_eq!(from_flags.output, OutputMode::Human);
        assert_eq!(from_flags.max_attempts, 6);
        assert_eq!(
            from_flags.credential_file,
            Some(PathBuf::from("/flag-token"))
        );
    }

    #[test]
    fn config_flag_wins_over_environment_config_without_discovery_or_fallback() {
        let (_environment_scratch, environment_document) = temporary_file(
            "environment-config",
            b"[client]\nendpoint = \"http://127.0.0.1:7001\"\n",
        );
        let (_flag_scratch, flag_document) = temporary_file(
            "flag-config",
            b"[client]\nendpoint = \"http://127.0.0.1:7002\"\n",
        );
        let mut environment = TestEnvironment::default();
        environment.0.insert(
            "RIFFDB_CONFIG".into(),
            environment_document.as_os_str().to_owned(),
        );
        let selected = resolve(
            &health_cli(&["--config", flag_document.to_str().expect("UTF-8 temp path")]),
            &environment,
        )
        .expect("flag-selected config");
        assert_eq!(selected.endpoint, "http://127.0.0.1:7002");

        let missing = health_cli(&["--config", "/definitely/missing/riffdb.toml"]);
        assert!(
            resolve(&missing, &environment).is_err(),
            "a selected invalid flag path must not fall through to RIFFDB_CONFIG"
        );
    }

    #[test]
    fn invalid_higher_precedence_fields_never_fall_through() {
        let (_scratch, document) = temporary_file(
            "valid-lower-precedence",
            concat!(
                "[client]\n",
                "endpoint = \"http://127.0.0.1:7001\"\n",
                "output = \"human\"\n",
                "max_attempts = 2\n",
                "credential_file = \"/toml-token\"\n",
            )
            .as_bytes(),
        );
        let document_text = document.to_str().expect("UTF-8 temp path");

        let mut invalid_environment = TestEnvironment::default();
        invalid_environment
            .0
            .insert("RIFFDB_CONFIG".into(), document_text.into());
        invalid_environment
            .0
            .insert("RIFFDB_ENDPOINT".into(), "not-an-endpoint".into());
        assert!(resolve(&health_cli(&[]), &invalid_environment).is_err());

        invalid_environment.0.remove("RIFFDB_ENDPOINT");
        invalid_environment
            .0
            .insert("RIFFDB_OUTPUT".into(), "JSON".into());
        assert!(resolve(&health_cli(&[]), &invalid_environment).is_err());

        invalid_environment.0.remove("RIFFDB_OUTPUT");
        invalid_environment
            .0
            .insert("RIFFDB_MAX_ATTEMPTS".into(), "03".into());
        assert!(resolve(&health_cli(&[]), &invalid_environment).is_err());

        invalid_environment.0.remove("RIFFDB_MAX_ATTEMPTS");
        invalid_environment
            .0
            .insert("RIFFDB_CREDENTIAL_FILE".into(), OsString::from(""));
        assert!(resolve(&health_cli(&[]), &invalid_environment).is_err());

        let mut valid_environment = TestEnvironment::default();
        valid_environment
            .0
            .insert("RIFFDB_CONFIG".into(), document_text.into());
        valid_environment
            .0
            .insert("RIFFDB_ENDPOINT".into(), "http://127.0.0.1:7003".into());
        valid_environment
            .0
            .insert("RIFFDB_MAX_ATTEMPTS".into(), "4".into());
        valid_environment
            .0
            .insert("RIFFDB_CREDENTIAL_FILE".into(), "/env-token".into());

        assert!(resolve(&health_cli(&["--endpoint", "invalid"]), &valid_environment).is_err());
        assert!(resolve(&health_cli(&["--max-attempts", "04"]), &valid_environment).is_err());
        assert!(resolve(&health_cli(&["--credential-file", ""]), &valid_environment).is_err());

        let mut invalid_lower_output = valid_environment;
        invalid_lower_output
            .0
            .insert("RIFFDB_OUTPUT".into(), "invalid".into());
        let flag_output = resolve(&health_cli(&["--output", "json"]), &invalid_lower_output)
            .expect("valid flag output must ignore lower-precedence invalid output");
        assert_eq!(flag_output.output, OutputMode::Json);
    }

    #[test]
    fn present_empty_and_noncanonical_values_never_fall_through() {
        let mut environment = TestEnvironment::default();
        environment.0.insert("RIFFDB_OUTPUT".into(), "".into());
        assert!(resolve(&health_cli(&[]), &environment).is_err());
        for value in ["0", "01", "11", "+1", " 1"] {
            let cli = health_cli(&["--max-attempts", value]);
            assert!(resolve(&cli, &TestEnvironment::default()).is_err());
        }
    }

    #[test]
    fn valid_output_selection_survives_unrelated_configuration_failures() {
        let mut environment = TestEnvironment::default();
        environment.0.insert("RIFFDB_OUTPUT".into(), "json".into());
        environment.0.insert("RIFFDB_ENDPOINT".into(), "bad".into());
        assert_eq!(
            resolve(&health_cli(&[]), &environment)
                .expect_err("invalid endpoint")
                .output(),
            OutputMode::Json
        );

        let (_scratch, document) = temporary_file(
            "json-invalid-endpoint",
            b"[client]\noutput = \"json\"\nendpoint = \"bad\"\n",
        );
        let mut document_environment = TestEnvironment::default();
        document_environment
            .0
            .insert("RIFFDB_CONFIG".into(), document.as_os_str().to_owned());
        assert_eq!(
            resolve(&health_cli(&[]), &document_environment)
                .expect_err("invalid document endpoint")
                .output(),
            OutputMode::Json
        );

        let overridden = health_cli(&["--output", "human"]);
        assert_eq!(
            resolve(&overridden, &environment)
                .expect_err("invalid endpoint")
                .output(),
            OutputMode::Human
        );
    }

    #[test]
    fn config_document_bounds_and_closed_toml_shape_are_exact() {
        let prefix = b"[client]\n";
        let mut exact = prefix.to_vec();
        exact.extend_from_slice(b"#");
        exact.resize(MAX_CONFIG_BYTES - 1, b'x');
        exact.push(b'\n');
        assert_eq!(exact.len(), MAX_CONFIG_BYTES);
        let (_exact_scratch, exact_path) = temporary_file("exact", &exact);
        assert!(read_document(&exact_path).is_ok());

        exact.push(b'\n');
        let (_excess_scratch, excess_path) = temporary_file("excess", &exact);
        assert_eq!(
            read_document(&excess_path).unwrap_err(),
            ConfigError::Invalid
        );

        for (name, document) in [
            ("unknown-table", "[unknown]\nvalue = 1\n"),
            ("unknown-key", "[client]\nunknown = 1\n"),
            (
                "duplicate-key",
                "[client]\noutput = \"json\"\noutput = \"human\"\n",
            ),
            ("wrong-type", "[client]\nmax_attempts = \"3\"\n"),
            ("trailing", "[client]\noutput = \"json\"\nnot toml"),
        ] {
            let (_scratch, path) = temporary_file(name, document.as_bytes());
            assert_eq!(
                read_document(&path).unwrap_err(),
                ConfigError::Invalid,
                "{name}"
            );
        }

        let (_empty_scratch, empty_path) = temporary_file("empty", b"");
        assert!(read_document(&empty_path).is_ok());
    }

    #[test]
    fn missing_config_and_invalid_selected_values_do_not_fall_through() {
        let mut environment = TestEnvironment::default();
        environment
            .0
            .insert("RIFFDB_CONFIG".into(), "/definitely/missing".into());
        assert!(resolve(&health_cli(&[]), &environment).is_err());

        let (_scratch, valid) = temporary_file("valid", b"[client]\nmax_attempts = 2\n");
        let valid_text = valid.to_str().expect("UTF-8 path");
        environment
            .0
            .insert("RIFFDB_CONFIG".into(), valid_text.into());
        environment
            .0
            .insert("RIFFDB_MAX_ATTEMPTS".into(), "".into());
        assert!(resolve(&health_cli(&[]), &environment).is_err());

        environment.0.remove("RIFFDB_MAX_ATTEMPTS");
        environment
            .0
            .insert("RIFFDB_UNRECOGNIZED".into(), "must-be-ignored".into());
        assert_eq!(
            resolve(&health_cli(&[]), &environment)
                .expect("unknown environment ignored")
                .max_attempts,
            2
        );
    }

    #[test]
    fn endpoint_requires_literal_loopback_http_and_explicit_canonical_port() {
        for accepted in [
            "http://127.0.0.1:1",
            "http://[::1]:65535",
            "https://riffdb.example.test:7443",
            "https://127.0.0.1:7443",
        ] {
            assert!(validate_endpoint(accepted).is_ok(), "{accepted}");
        }
        for rejected in [
            "http://localhost:7443",
            "http://192.0.2.1:7443",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://127.0.0.1:07443",
            "http://127.0.0.1:7443/",
            "http://user@127.0.0.1:7443",
            "http://127.0.0.1:+7443",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:7443?query",
            "http://127.0.0.1:7443#fragment",
            "http://127.0.0.1:7443/path",
            "http://127.0.0.1:7443 ",
            "http://[::1%lo]:7443",
            "http://::1:7443",
            "HTTP://127.0.0.1:7443",
        ] {
            assert_eq!(
                validate_endpoint(rejected),
                Err(ConfigError::Invalid),
                "{rejected}"
            );
        }
        assert_eq!(
            validate_endpoint(&format!("http://{}:1", "1".repeat(513))),
            Err(ConfigError::Invalid)
        );
    }

    #[test]
    fn verified_tls_configuration_is_complete_or_rejected() {
        let (_trust_scratch, trust) = temporary_file("tls-root", b"test trust root");
        let trust_text = trust.to_str().expect("UTF-8 test path");
        let (_complete_scratch, complete) = temporary_file(
            "tls-complete",
            format!(
                "[client]\nendpoint = \"https://127.0.0.1:7443\"\ntls_trust_root = {trust_text:?}\ntls_server_name = \"127.0.0.1\"\n"
            )
            .as_bytes(),
        );
        let mut environment = TestEnvironment::default();
        environment
            .0
            .insert("RIFFDB_CONFIG".into(), complete.as_os_str().to_owned());
        let resolved = resolve(&health_cli(&[]), &environment).expect("complete TLS config");
        let tls = resolved.tls.expect("verified TLS selection");
        assert_eq!(tls.endpoint().as_str(), "https://127.0.0.1:7443");
        assert_eq!(tls.trust_root().as_path(), trust.as_path());
        assert_eq!(tls.expected_server_identity().as_str(), "127.0.0.1");

        for (label, document) in [
            (
                "missing-root",
                "[client]\nendpoint = \"https://127.0.0.1:7443\"\ntls_server_name = \"127.0.0.1\"\n".to_owned(),
            ),
            (
                "missing-name",
                format!(
                    "[client]\nendpoint = \"https://127.0.0.1:7443\"\ntls_trust_root = {trust_text:?}\n"
                ),
            ),
            (
                "wrong-name",
                format!(
                    "[client]\nendpoint = \"https://127.0.0.1:7443\"\ntls_trust_root = {trust_text:?}\ntls_server_name = \"other.example\"\n"
                ),
            ),
            (
                "tls-on-cleartext",
                format!(
                    "[client]\nendpoint = \"http://127.0.0.1:7443\"\ntls_trust_root = {trust_text:?}\ntls_server_name = \"127.0.0.1\"\n"
                ),
            ),
        ] {
            let (_scratch, path) = temporary_file(label, document.as_bytes());
            let mut environment = TestEnvironment::default();
            environment
                .0
                .insert("RIFFDB_CONFIG".into(), path.as_os_str().to_owned());
            assert!(resolve(&health_cli(&[]), &environment).is_err(), "{label}");
        }
    }

    #[test]
    fn configured_path_uses_the_general_platform_byte_boundary() {
        let mut environment = TestEnvironment::default();
        environment.0.insert(
            "RIFFDB_CREDENTIAL_FILE".into(),
            OsString::from("x".repeat(crate::input::MAX_PATH_BYTES)),
        );
        assert!(resolve(&health_cli(&[]), &environment).is_ok());
        environment.0.insert(
            "RIFFDB_CREDENTIAL_FILE".into(),
            OsString::from("x".repeat(crate::input::MAX_PATH_BYTES + 1)),
        );
        assert!(resolve(&health_cli(&[]), &environment).is_err());
    }

    #[test]
    fn project_configuration_is_closed_bounded_and_workspace_relative() {
        let (scratch, path) = temporary_file(
            "riffdb.toml",
            b"[client]\ndatabase = \"inventory\"\n\n[project]\nschema = \"riffdb.application.json\"\ngenerators = [\"rust\", \"typescript\"]\n",
        );
        let project = load_project(&path).expect("project config");
        assert_eq!(project.root(), scratch.path());
        assert_eq!(
            project.schema(),
            scratch.path().join("riffdb.application.json")
        );
        assert_eq!(
            project.generators(),
            &[ProjectGenerator::Rust, ProjectGenerator::Typescript]
        );

        for (label, document) in [
            (
                "escaping.toml",
                "[project]\nschema = \"../schema.json\"\ngenerators = [\"rust\"]\n",
            ),
            (
                "duplicate.toml",
                "[project]\nschema = \"schema.json\"\ngenerators = [\"rust\", \"rust\"]\n",
            ),
            (
                "unknown.toml",
                "[project]\nschema = \"schema.json\"\ngenerators = [\"java\"]\n",
            ),
            (
                "open.toml",
                "[project]\nschema = \"schema.json\"\ngenerators = [\"rust\"]\nextra = true\n",
            ),
        ] {
            let (_scratch, path) = temporary_file(label, document.as_bytes());
            assert_eq!(load_project(&path), Err(ConfigError::Invalid), "{label}");
        }
    }

    fn temporary_file(label: &str, bytes: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-cli-config-").expect("scratch directory");
        let path = scratch.path().join(label);
        fs::write(&path, bytes).expect("temporary config");
        (scratch, path)
    }
}
