//! Closed configuration causes. Supplied values, parser excerpts and paths cannot enter this DTO.
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigSource {
    Flag,
    Environment,
    File,
    Default,
}
impl ConfigSource {
    pub(crate) const fn selected(flag: bool, environment: bool, file: bool) -> Self {
        if flag {
            Self::Flag
        } else if environment {
            Self::Environment
        } else if file {
            Self::File
        } else {
            Self::Default
        }
    }
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Flag => "flag",
            Self::Environment => "environment",
            Self::File => "file",
            Self::Default => "default",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum ConfigSetting {
    #[serde(rename = "config")]
    Config,
    #[serde(rename = "client.output")]
    Output,
    #[serde(rename = "client.endpoint")]
    Endpoint,
    #[serde(rename = "client.database")]
    Database,
    #[serde(rename = "client.max_attempts")]
    MaxAttempts,
    #[serde(rename = "client.credential_file")]
    CredentialFile,
    #[serde(rename = "client.tls_trust_root")]
    TlsTrustRoot,
    #[serde(rename = "client.tls_server_name")]
    TlsServerName,
    #[serde(rename = "client.tls_client_cert")]
    TlsClientCert,
    #[serde(rename = "client.tls_client_key")]
    TlsClientKey,
}
impl ConfigSetting {
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Output => "client.output",
            Self::Endpoint => "client.endpoint",
            Self::Database => "client.database",
            Self::MaxAttempts => "client.max_attempts",
            Self::CredentialFile => "client.credential_file",
            Self::TlsTrustRoot => "client.tls_trust_root",
            Self::TlsServerName => "client.tls_server_name",
            Self::TlsClientCert => "client.tls_client_cert",
            Self::TlsClientKey => "client.tls_client_key",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigCause {
    ValueInvalid,
    EndpointInvalid,
    DatabaseInvalid,
    RetryBounds,
    PathInvalid,
    ConfigUnreadable,
    ConfigTooLarge,
    TomlInvalid,
    UnknownSetting,
    TomlShape,
    TlsRequiresHttps,
    AbsolutePathRequired,
    TlsServerNameInvalid,
    TlsIdentityMismatch,
    TlsPairRequired,
    ProtectedPathsOverlap,
    TrustRootUnavailable,
    TrustRootPermissions,
    TlsMaterialInvalid,
    PeerUnavailableOrUnverified,
}
impl ConfigCause {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::ValueInvalid => "value_invalid",
            Self::EndpointInvalid => "endpoint_invalid",
            Self::DatabaseInvalid => "database_invalid",
            Self::RetryBounds => "retry_bounds",
            Self::PathInvalid => "path_invalid",
            Self::ConfigUnreadable => "config_unreadable",
            Self::ConfigTooLarge => "config_too_large",
            Self::TomlInvalid => "toml_invalid",
            Self::UnknownSetting => "unknown_setting",
            Self::TomlShape => "toml_shape",
            Self::TlsRequiresHttps => "tls_requires_https",
            Self::AbsolutePathRequired => "absolute_path_required",
            Self::TlsServerNameInvalid => "tls_server_name_invalid",
            Self::TlsIdentityMismatch => "tls_identity_mismatch",
            Self::TlsPairRequired => "tls_pair_required",
            Self::ProtectedPathsOverlap => "protected_paths_overlap",
            Self::TrustRootUnavailable => "trust_root_unavailable",
            Self::TrustRootPermissions => "trust_root_permissions",
            Self::TlsMaterialInvalid => "tls_material_invalid",
            Self::PeerUnavailableOrUnverified => "peer_unavailable_or_unverified",
        }
    }
    pub(crate) const fn expected(self) -> &'static str {
        match self {
            Self::EndpointInvalid => {
                "canonical https://host:port or literal-loopback http://address:port; explicit nonzero port"
            }
            Self::DatabaseInvalid => {
                "canonical database alias of 1..=64 permitted ASCII characters"
            }
            Self::RetryBounds => "integer in 1..=10",
            Self::PathInvalid => {
                "nonempty path of at most 4096 bytes without NUL; protected TLS paths are absolute and normal"
            }
            Self::ConfigUnreadable => "readable bounded configuration file",
            Self::ConfigTooLarge => "configuration at most 65536 bytes",
            Self::TomlInvalid => "UTF-8 TOML",
            Self::UnknownSetting | Self::TomlShape => {
                "documented client/project keys with their declared types"
            }
            Self::TlsRequiresHttps => "TLS options only with HTTPS",
            Self::AbsolutePathRequired => "absolute protected path",
            Self::TlsServerNameInvalid => "canonical DNS name or literal IP identity",
            Self::TlsIdentityMismatch => "TLS identity equal to endpoint host",
            Self::TlsPairRequired => "both client certificate and private key",
            Self::ProtectedPathsOverlap => "distinct protected transport paths",
            Self::TrustRootUnavailable => "stable readable bounded regular trust-root file",
            Self::TrustRootPermissions => "protected trust-root ownership and permissions",
            Self::TlsMaterialInvalid => "valid bounded certificate and key material",
            Self::PeerUnavailableOrUnverified => "reachable peer with a verified certificate",
            Self::ValueInvalid => "one documented nonempty value",
        }
    }
    pub(crate) const fn fix(self) -> &'static str {
        match self {
            Self::EndpointInvalid => {
                "use https://host:port with an explicit nonzero port; cleartext requires http://literal-loopback:port"
            }
            Self::DatabaseInvalid => {
                "use the configured database alias with a lowercase initial letter, then lowercase letters, digits, underscores or hyphens"
            }
            Self::RetryBounds => "set max_attempts to an integer from 1 through 10",
            Self::PathInvalid => "correct the selected path without including its value in reports",
            Self::ConfigUnreadable => {
                "restore the selected configuration file and read permissions"
            }
            Self::ConfigTooLarge => "reduce the configuration file to at most 65536 bytes",
            Self::TomlInvalid => "correct TOML syntax and UTF-8 encoding",
            Self::UnknownSetting => "remove unknown keys; consult the configuration reference",
            Self::TomlShape => "use the documented tables and value types",
            Self::TlsRequiresHttps => "TLS options require an https:// endpoint",
            Self::AbsolutePathRequired => {
                "use an absolute protected TLS path without parent components"
            }
            Self::TlsServerNameInvalid => "use a canonical certificate DNS name or literal IP",
            Self::TlsIdentityMismatch => {
                "tls_server_name must match the host in --endpoint; omit --tls-server-name when the endpoint host is already in the certificate SAN"
            }
            Self::TlsPairRequired => "tls_client_cert and tls_client_key must be provided together",
            Self::ProtectedPathsOverlap => {
                "configure distinct trust-root, certificate and private-key paths"
            }
            Self::TrustRootUnavailable => {
                "restore the selected trust-root file with valid bounded PEM and stable protected custody"
            }
            Self::TrustRootPermissions => {
                "remove group/other write permissions and verify the protected file owner"
            }
            Self::TlsMaterialInvalid => {
                "correct the protected TLS certificate, private key and trust-root material"
            }
            Self::PeerUnavailableOrUnverified => {
                "check listener availability, trust chain, certificate validity and endpoint identity"
            }
            Self::ValueInvalid => "set a documented nonempty value for the selected setting",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ConfigurationDiagnostic {
    pub(crate) setting: ConfigSetting,
    pub(crate) source: ConfigSource,
    pub(crate) cause: ConfigCause,
    pub(crate) expected: &'static str,
    pub(crate) fix: &'static str,
}
impl ConfigurationDiagnostic {
    pub(crate) const fn new(
        setting: ConfigSetting,
        source: ConfigSource,
        cause: ConfigCause,
    ) -> Self {
        Self {
            setting,
            source,
            cause,
            expected: cause.expected(),
            fix: cause.fix(),
        }
    }
    pub(crate) fn human(self) -> String {
        format!(
            "setting={} source={} cause={}; expected={}; fix={}",
            self.setting.name(),
            self.source.name(),
            self.cause.name(),
            self.expected,
            self.fix
        )
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum OperatorDiagnostic {
    Configuration(ConfigurationDiagnostic),
    CredentialFile(CredentialFileDiagnostic),
    Readiness(crate::readiness_diagnostics::ReadinessDiagnostic),
    Local(crate::local_diagnostics::LocalDiagnostic),
    Transport {
        component: &'static str,
        cause: ConfigCause,
        expected: &'static str,
        fix: &'static str,
    },
}
impl OperatorDiagnostic {
    pub(crate) fn human(&self) -> String {
        match self {
            Self::Configuration(diagnostic) => diagnostic.human(),
            Self::CredentialFile(diagnostic) => diagnostic.human(),
            Self::Readiness(diagnostic) => diagnostic.human(),
            Self::Local(diagnostic) => diagnostic.human(),
            Self::Transport {
                component,
                cause,
                expected,
                fix,
            } => format!(
                "component={component} cause={}; expected={expected}; fix={fix}",
                cause.name()
            ),
        }
    }
    pub(crate) fn from_tls(failure: riffdb_client_rust::TlsClientFailure) -> Self {
        use riffdb_client_rust::TlsClientFailure;
        let (setting, cause) = match failure {
            TlsClientFailure::TrustRootUnavailable => (
                Some(ConfigSetting::TlsTrustRoot),
                ConfigCause::TrustRootUnavailable,
            ),
            TlsClientFailure::TrustRootPermissions => (
                Some(ConfigSetting::TlsTrustRoot),
                ConfigCause::TrustRootPermissions,
            ),
            TlsClientFailure::InvalidConfiguration => (None, ConfigCause::TlsMaterialInvalid),
            TlsClientFailure::ConnectionOrPeerVerification => (
                Some(ConfigSetting::Endpoint),
                ConfigCause::PeerUnavailableOrUnverified,
            ),
        };
        match setting {
            Some(setting) => Self::Configuration(ConfigurationDiagnostic::new(
                setting,
                ConfigSource::Default,
                cause,
            )),
            None => Self::Transport {
                component: "tls",
                cause,
                expected: cause.expected(),
                fix: cause.fix(),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CredentialFileDiagnostic {
    pub(crate) source: ConfigSource,
    pub(crate) cause: riffdb_client_rust::BearerCredentialFileCause,
}
impl CredentialFileDiagnostic {
    pub(crate) fn human(&self) -> String {
        format!(
            "setting=client.credential_file source={} cause={}; expected={}; fix={}",
            self.source.name(),
            self.cause.as_str(),
            self.cause.expected(),
            self.cause.fix()
        )
    }
}
impl Serialize for CredentialFileDiagnostic {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("setting", "client.credential_file")?;
        map.serialize_entry("source", &self.source)?;
        map.serialize_entry("cause", self.cause.as_str())?;
        map.serialize_entry("expected", self.cause.expected())?;
        map.serialize_entry("fix", self.cause.fix())?;
        map.end()
    }
}
