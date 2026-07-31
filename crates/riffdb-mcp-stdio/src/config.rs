//! Closed process configuration for the stdio bridge.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::Read;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

use riffdb_client_rust::{
    BearerCredential, BearerCredentialFileError, DEFAULT_DATABASE_ALIAS, DatabaseAlias,
    load_protected_bearer_credential,
};
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7443";
const MAX_CONFIG_BYTES: usize = 65_536;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4_096;

const CONFIG_ENV: &str = "RIFFDB_MCP_CONFIG";
const ENDPOINT_ENV: &str = "RIFFDB_MCP_ENDPOINT";
const TOKEN_ENV: &str = "RIFFDB_MCP_CAPABILITY_TOKEN";
const CREDENTIAL_FILE_ENV: &str = "RIFFDB_MCP_CREDENTIAL_FILE";
const DATABASE_ENV: &str = "RIFFDB_MCP_DATABASE";

/// A closed, redaction-safe startup configuration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StdioConfigError {
    InvalidArguments,
    InvalidConfigPath,
    ConfigReadFailed,
    ConfigTooLarge,
    InvalidConfig,
    InvalidEndpoint,
    InvalidDatabase,
    ConflictingCredentialSources,
    MissingCredential,
    InvalidCredential,
    InvalidCredentialPath,
    CredentialFileRejected,
}

impl std::fmt::Display for StdioConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidArguments => "invalid riffdb-mcp arguments",
            Self::InvalidConfigPath => "the MCP configuration path is invalid",
            Self::ConfigReadFailed => "the MCP configuration file could not be read",
            Self::ConfigTooLarge => "the MCP configuration file exceeds its size limit",
            Self::InvalidConfig => "the MCP configuration file is invalid",
            Self::InvalidEndpoint => "the RiffDB endpoint is invalid",
            Self::InvalidDatabase => "the RiffDB database selector is invalid",
            Self::ConflictingCredentialSources => "multiple MCP credential sources were supplied",
            Self::MissingCredential => "an MCP capability credential is required",
            Self::InvalidCredential => "the MCP capability credential is invalid",
            Self::InvalidCredentialPath => "the MCP credential-file path is invalid",
            Self::CredentialFileRejected => "the protected MCP credential file was rejected",
        })
    }
}

impl std::error::Error for StdioConfigError {}

pub(crate) struct StdioConfig {
    endpoint: String,
    database: DatabaseAlias,
    credential: BearerCredential,
}

impl StdioConfig {
    pub(crate) fn into_parts(self) -> (String, DatabaseAlias, BearerCredential) {
        (self.endpoint, self.database, self.credential)
    }
}

#[derive(Default)]
struct Arguments {
    config: Option<OsString>,
    endpoint: Option<OsString>,
    database: Option<OsString>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    mcp: Option<McpConfig>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpConfig {
    endpoint: Option<String>,
    database: Option<String>,
    credential_file: Option<String>,
}

trait StartupIo {
    fn environment(&self, name: &str) -> Option<OsString>;

    fn read_config(&self, path: &Path) -> Result<Vec<u8>, ConfigReadError>;

    fn load_credential(&self, path: &Path) -> Result<BearerCredential, BearerCredentialFileError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfigReadError {
    Io,
    TooLarge,
}

struct ProcessIo;

impl StartupIo for ProcessIo {
    fn environment(&self, name: &str) -> Option<OsString> {
        std::env::var_os(name)
    }

    fn read_config(&self, path: &Path) -> Result<Vec<u8>, ConfigReadError> {
        read_bounded_config(path)
    }

    fn load_credential(&self, path: &Path) -> Result<BearerCredential, BearerCredentialFileError> {
        load_protected_bearer_credential(path)
    }
}

pub(crate) fn load_process_config() -> Result<StdioConfig, StdioConfigError> {
    load_config(std::env::args_os().skip(1), &ProcessIo)
}

fn load_config(
    arguments: impl IntoIterator<Item = OsString>,
    io: &impl StartupIo,
) -> Result<StdioConfig, StdioConfigError> {
    let arguments = parse_arguments(arguments)?;
    let config_path = arguments
        .config
        .or_else(|| io.environment(CONFIG_ENV))
        .map(PathBuf::from);
    let document = match config_path {
        Some(path) => {
            validate_path(&path).map_err(|_| StdioConfigError::InvalidConfigPath)?;
            let bytes = io.read_config(&path).map_err(|error| match error {
                ConfigReadError::Io => StdioConfigError::ConfigReadFailed,
                ConfigReadError::TooLarge => StdioConfigError::ConfigTooLarge,
            })?;
            parse_document(&bytes)?
        }
        None => ConfigDocument::default(),
    };
    let file_config = document.mcp.unwrap_or_default();

    let endpoint = match arguments.endpoint {
        Some(endpoint) => os_string_to_text(endpoint)?,
        None => match io.environment(ENDPOINT_ENV) {
            Some(endpoint) => os_string_to_text(endpoint)?,
            None => file_config
                .endpoint
                .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned()),
        },
    };
    validate_endpoint(&endpoint)?;
    let database = match arguments.database {
        Some(database) => os_string_to_database(database)?,
        None => match io.environment(DATABASE_ENV) {
            Some(database) => os_string_to_database(database)?,
            None => DatabaseAlias::new(
                file_config
                    .database
                    .unwrap_or_else(|| DEFAULT_DATABASE_ALIAS.to_owned()),
            )
            .map_err(|_| StdioConfigError::InvalidDatabase)?,
        },
    };

    let raw_token = io.environment(TOKEN_ENV);
    let credential_path = io
        .environment(CREDENTIAL_FILE_ENV)
        .map(PathBuf::from)
        .or_else(|| file_config.credential_file.map(PathBuf::from));
    let credential = match (raw_token, credential_path) {
        (Some(_), Some(_)) => return Err(StdioConfigError::ConflictingCredentialSources),
        (None, None) => return Err(StdioConfigError::MissingCredential),
        (Some(token), None) => {
            let token = token.to_str().ok_or(StdioConfigError::InvalidCredential)?;
            BearerCredential::new(token).map_err(|_| StdioConfigError::InvalidCredential)?
        }
        (None, Some(path)) => {
            validate_path(&path).map_err(|_| StdioConfigError::InvalidCredentialPath)?;
            io.load_credential(&path)
                .map_err(|_| StdioConfigError::CredentialFileRejected)?
        }
    };

    Ok(StdioConfig {
        endpoint,
        database,
        credential,
    })
}

fn parse_arguments(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<Arguments, StdioConfigError> {
    let mut parsed = Arguments::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--config") if parsed.config.is_none() => {
                parsed.config = Some(arguments.next().ok_or(StdioConfigError::InvalidArguments)?);
            }
            Some("--endpoint") if parsed.endpoint.is_none() => {
                parsed.endpoint = Some(arguments.next().ok_or(StdioConfigError::InvalidArguments)?);
            }
            Some("--database") if parsed.database.is_none() => {
                parsed.database = Some(arguments.next().ok_or(StdioConfigError::InvalidArguments)?);
            }
            _ => return Err(StdioConfigError::InvalidArguments),
        }
    }
    Ok(parsed)
}

fn os_string_to_database(value: OsString) -> Result<DatabaseAlias, StdioConfigError> {
    value
        .into_string()
        .map_err(|_| StdioConfigError::InvalidDatabase)
        .and_then(|value| DatabaseAlias::new(value).map_err(|_| StdioConfigError::InvalidDatabase))
}

fn os_string_to_text(value: OsString) -> Result<String, StdioConfigError> {
    value
        .into_string()
        .map_err(|_| StdioConfigError::InvalidEndpoint)
}

fn read_bounded_config(path: &Path) -> Result<Vec<u8>, ConfigReadError> {
    let mut file = File::open(path).map_err(|_| ConfigReadError::Io)?;
    let mut bytes = Vec::with_capacity(8 * 1_024);
    file.by_ref()
        .take((MAX_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigReadError::Io)?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(ConfigReadError::TooLarge);
    }
    Ok(bytes)
}

fn parse_document(bytes: &[u8]) -> Result<ConfigDocument, StdioConfigError> {
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(StdioConfigError::ConfigTooLarge);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| StdioConfigError::InvalidConfig)?;
    toml::from_str(text).map_err(|_| StdioConfigError::InvalidConfig)
}

fn validate_endpoint(endpoint: &str) -> Result<(), StdioConfigError> {
    if endpoint.is_empty()
        || endpoint.len() > MAX_ENDPOINT_BYTES
        || !endpoint.is_ascii()
        || endpoint.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(StdioConfigError::InvalidEndpoint);
    }
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or(StdioConfigError::InvalidEndpoint)?;
    if authority.is_empty()
        || authority
            .bytes()
            .any(|byte| matches!(byte, b'/' | b'?' | b'#' | b'@'))
    {
        return Err(StdioConfigError::InvalidEndpoint);
    }

    let (host, port) = if authority.starts_with('[') {
        let close = authority
            .find(']')
            .ok_or(StdioConfigError::InvalidEndpoint)?;
        let host = authority
            .get(1..close)
            .ok_or(StdioConfigError::InvalidEndpoint)?;
        let port = authority
            .get(close + 1..)
            .and_then(|suffix| suffix.strip_prefix(':'))
            .ok_or(StdioConfigError::InvalidEndpoint)?;
        let address = host
            .parse::<Ipv6Addr>()
            .map_err(|_| StdioConfigError::InvalidEndpoint)?;
        if !address.is_loopback() {
            return Err(StdioConfigError::InvalidEndpoint);
        }
        (host, port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or(StdioConfigError::InvalidEndpoint)?;
        if host.contains(':') {
            return Err(StdioConfigError::InvalidEndpoint);
        }
        let address = host
            .parse::<Ipv4Addr>()
            .map_err(|_| StdioConfigError::InvalidEndpoint)?;
        if !address.is_loopback() {
            return Err(StdioConfigError::InvalidEndpoint);
        }
        (host, port)
    };
    if host.is_empty()
        || port.is_empty()
        || !port.bytes().all(|byte| byte.is_ascii_digit())
        || port.parse::<u16>().ok().filter(|port| *port != 0).is_none()
    {
        return Err(StdioConfigError::InvalidEndpoint);
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), ()> {
    validate_platform_path(path.as_os_str())
}

#[cfg(unix)]
fn validate_platform_path(path: &OsStr) -> Result<(), ()> {
    use std::os::unix::ffi::OsStrExt;

    let bytes = path.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_PATH_BYTES || bytes.contains(&0) {
        return Err(());
    }
    Ok(())
}

#[cfg(windows)]
fn validate_platform_path(path: &OsStr) -> Result<(), ()> {
    use std::os::windows::ffi::OsStrExt;

    let mut units = 0_usize;
    for unit in path.encode_wide() {
        if unit == 0 {
            return Err(());
        }
        units = units.checked_add(1).ok_or(())?;
        if units > MAX_PATH_BYTES / 2 {
            return Err(());
        }
    }
    if units == 0 {
        return Err(());
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn validate_platform_path(path: &OsStr) -> Result<(), ()> {
    let text = path.to_str().ok_or(())?;
    if text.is_empty() || text.len() > MAX_PATH_BYTES || text.as_bytes().contains(&0) {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    use super::*;

    const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[derive(Default)]
    struct FakeIo {
        environment: BTreeMap<&'static str, OsString>,
        documents: BTreeMap<PathBuf, Result<Vec<u8>, ConfigReadError>>,
        read_count: Cell<usize>,
        loaded_paths: RefCell<Vec<PathBuf>>,
    }

    impl FakeIo {
        fn with_token() -> Self {
            let mut io = Self::default();
            io.environment.insert(TOKEN_ENV, TOKEN.into());
            io
        }
    }

    impl StartupIo for FakeIo {
        fn environment(&self, name: &str) -> Option<OsString> {
            self.environment.get(name).cloned()
        }

        fn read_config(&self, path: &Path) -> Result<Vec<u8>, ConfigReadError> {
            self.read_count.set(self.read_count.get() + 1);
            self.documents
                .get(path)
                .cloned()
                .unwrap_or(Err(ConfigReadError::Io))
        }

        fn load_credential(
            &self,
            path: &Path,
        ) -> Result<BearerCredential, BearerCredentialFileError> {
            self.loaded_paths.borrow_mut().push(path.to_owned());
            BearerCredential::new(TOKEN).map_err(|_| BearerCredentialFileError::InvalidPresentation)
        }
    }

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    fn endpoint_of(config: StdioConfig) -> String {
        config.into_parts().0
    }

    #[test]
    fn absent_optional_config_performs_no_ambient_discovery() {
        let io = FakeIo::with_token();
        let config = load_config([], &io).expect("default config");
        assert_eq!(endpoint_of(config), DEFAULT_ENDPOINT);
        assert_eq!(io.read_count.get(), 0);
    }

    #[test]
    fn config_selection_and_endpoint_precedence_are_exact() {
        let mut io = FakeIo::with_token();
        io.environment.insert(CONFIG_ENV, "/env/config".into());
        io.environment
            .insert(ENDPOINT_ENV, "http://127.0.0.2:7002".into());
        io.documents.insert(
            "/env/config".into(),
            Ok(b"[mcp]\nendpoint = \"http://127.0.0.3:7003\"\n".to_vec()),
        );
        io.documents.insert(
            "/arg/config".into(),
            Ok(b"[mcp]\nendpoint = \"http://127.0.0.4:7004\"\n".to_vec()),
        );

        let config = load_config(
            args(&[
                "--config",
                "/arg/config",
                "--endpoint",
                "http://127.0.0.5:7005",
            ]),
            &io,
        )
        .expect("argv selections");
        assert_eq!(endpoint_of(config), "http://127.0.0.5:7005");

        let config =
            load_config(args(&["--config", "/env/config"]), &io).expect("environment endpoint");
        assert_eq!(endpoint_of(config), "http://127.0.0.2:7002");
    }

    #[test]
    fn database_selection_uses_argument_environment_document_then_default() {
        let default = load_config([], &FakeIo::with_token()).expect("default");
        assert_eq!(default.into_parts().1.as_str(), DEFAULT_DATABASE_ALIAS);

        let mut io = FakeIo::with_token();
        io.environment.insert(CONFIG_ENV, "/config".into());
        io.documents.insert(
            "/config".into(),
            Ok(b"[mcp]\ndatabase = \"document_db\"\n".to_vec()),
        );
        assert_eq!(
            load_config([], &io)
                .expect("document")
                .into_parts()
                .1
                .as_str(),
            "document_db"
        );

        io.environment.insert(DATABASE_ENV, "environment_db".into());
        assert_eq!(
            load_config([], &io)
                .expect("environment")
                .into_parts()
                .1
                .as_str(),
            "environment_db"
        );
        assert_eq!(
            load_config(args(&["--database", "argument_db"]), &io)
                .expect("argument")
                .into_parts()
                .1
                .as_str(),
            "argument_db"
        );
        assert_eq!(
            load_config(args(&["--database", "Invalid"]), &io).err(),
            Some(StdioConfigError::InvalidDatabase)
        );
    }

    #[test]
    fn present_invalid_higher_precedence_endpoint_never_falls_through() {
        let mut io = FakeIo::with_token();
        io.environment.insert(ENDPOINT_ENV, OsString::new());
        assert_eq!(
            load_config([], &io).err(),
            Some(StdioConfigError::InvalidEndpoint)
        );
    }

    #[test]
    fn invalid_lower_precedence_endpoint_is_not_observed() {
        let mut io = FakeIo::with_token();
        io.environment.insert(ENDPOINT_ENV, OsString::new());
        let config =
            load_config(args(&["--endpoint", DEFAULT_ENDPOINT]), &io).expect("argv endpoint wins");
        assert_eq!(endpoint_of(config), DEFAULT_ENDPOINT);
    }

    #[test]
    fn only_two_pair_form_flags_are_accepted_once() {
        let io = FakeIo::with_token();
        for values in [
            vec!["--config"],
            vec!["--endpoint"],
            vec!["--config=x"],
            vec!["--credential-file", "x"],
            vec![
                "--endpoint",
                DEFAULT_ENDPOINT,
                "--endpoint",
                DEFAULT_ENDPOINT,
            ],
            vec!["positional"],
        ] {
            assert_eq!(
                load_config(args(&values), &io).err(),
                Some(StdioConfigError::InvalidArguments),
                "{values:?}"
            );
        }
    }

    #[test]
    fn endpoint_is_exact_literal_loopback_http_with_explicit_port() {
        for endpoint in [
            "http://127.0.0.1:1",
            "http://127.255.255.254:65535",
            "http://[::1]:7443",
            "http://[0:0:0:0:0:0:0:1]:7443",
        ] {
            assert_eq!(validate_endpoint(endpoint), Ok(()), "{endpoint}");
        }
        for endpoint in [
            "",
            "https://127.0.0.1:7443",
            "HTTP://127.0.0.1:7443",
            "http://localhost:7443",
            "http://0.0.0.0:7443",
            "http://192.0.2.1:7443",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:+1",
            "http://127.0.0.1:7443/",
            "http://user@127.0.0.1:7443",
            "http://127.0.0.1:7443?x",
            "http://127.0.0.1:7443#x",
            "http://127.0.0.1:7443 ",
            "http://::1:7443",
            "http://[::2]:7443",
        ] {
            assert_eq!(
                validate_endpoint(endpoint),
                Err(StdioConfigError::InvalidEndpoint),
                "{endpoint}"
            );
        }
        assert_eq!(
            validate_endpoint(&format!("http://127.0.0.1:{}", "7".repeat(490))),
            Err(StdioConfigError::InvalidEndpoint)
        );
    }

    #[test]
    fn selected_config_is_bounded_utf8_and_closed() {
        let mut io = FakeIo::with_token();
        for (path, bytes, expected) in [
            (
                "/unknown",
                b"[mcp]\nunknown = true\n".to_vec(),
                StdioConfigError::InvalidConfig,
            ),
            (
                "/raw-token",
                b"[mcp]\ncapability_token = \"secret\"\n".to_vec(),
                StdioConfigError::InvalidConfig,
            ),
            (
                "/duplicate",
                b"[mcp]\nendpoint = \"http://127.0.0.1:1\"\nendpoint = \"http://127.0.0.1:2\"\n"
                    .to_vec(),
                StdioConfigError::InvalidConfig,
            ),
            ("/not-utf8", vec![0xff], StdioConfigError::InvalidConfig),
            (
                "/too-large",
                vec![b' '; MAX_CONFIG_BYTES + 1],
                StdioConfigError::ConfigTooLarge,
            ),
        ] {
            io.documents.insert(path.into(), Ok(bytes));
            assert_eq!(
                load_config(args(&["--config", path]), &io).err(),
                Some(expected),
                "{path}"
            );
        }
    }

    #[test]
    fn token_and_credential_file_are_exclusive_and_required() {
        let io = FakeIo::default();
        assert_eq!(
            load_config([], &io).err(),
            Some(StdioConfigError::MissingCredential)
        );

        let mut io = FakeIo::with_token();
        io.environment
            .insert(CREDENTIAL_FILE_ENV, "/protected/token".into());
        assert_eq!(
            load_config([], &io).err(),
            Some(StdioConfigError::ConflictingCredentialSources)
        );
        assert!(io.loaded_paths.borrow().is_empty());
    }

    #[test]
    fn environment_token_uses_exact_public_client_presentation_check() {
        let mut io = FakeIo::default();
        for token in ["", "A", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!"] {
            io.environment.insert(TOKEN_ENV, token.into());
            assert_eq!(
                load_config([], &io).err(),
                Some(StdioConfigError::InvalidCredential),
                "{token:?}"
            );
        }
        io.environment.insert(TOKEN_ENV, TOKEN.into());
        assert!(load_config([], &io).is_ok());
    }

    #[test]
    fn credential_file_selection_uses_environment_then_config_and_loads_once() {
        let mut io = FakeIo::default();
        io.environment
            .insert(CREDENTIAL_FILE_ENV, "/env/token".into());
        io.environment.insert(CONFIG_ENV, "/config".into());
        io.documents.insert(
            "/config".into(),
            Ok(b"[mcp]\ncredential_file = \"/file/token\"\n".to_vec()),
        );
        load_config([], &io).expect("protected credential");
        assert_eq!(
            io.loaded_paths.borrow().as_slice(),
            [PathBuf::from("/env/token")]
        );
    }

    #[test]
    fn paths_are_nonempty_nul_free_and_bounded() {
        let mut io = FakeIo::with_token();
        io.environment.insert(CONFIG_ENV, OsString::new());
        assert_eq!(
            load_config([], &io).err(),
            Some(StdioConfigError::InvalidConfigPath)
        );

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;

            io.environment
                .insert(CONFIG_ENV, OsString::from_vec(b"a\0b".to_vec()));
            assert_eq!(
                load_config([], &io).err(),
                Some(StdioConfigError::InvalidConfigPath)
            );
        }

        io.environment
            .insert(CONFIG_ENV, OsString::from("x".repeat(MAX_PATH_BYTES + 1)));
        assert_eq!(
            load_config([], &io).err(),
            Some(StdioConfigError::InvalidConfigPath)
        );
    }

    #[test]
    fn errors_and_credentials_are_redaction_safe() {
        let error = StdioConfigError::CredentialFileRejected;
        assert_eq!(
            format!("{error:?}"),
            "CredentialFileRejected",
            "closed debug carries no source data"
        );
        assert!(!error.to_string().contains("token"));

        let config = load_config([], &FakeIo::with_token()).expect("config");
        let (_, _, credential) = config.into_parts();
        assert_eq!(format!("{credential:?}"), "BearerCredential([REDACTED])");
    }
}
