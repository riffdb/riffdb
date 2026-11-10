//! Restart-applied operator-only durable local and HTTP webhook outbox destination configuration.
use super::*;
use riffdb_storage_api::OutboxDestinationIdV1;
use std::num::{NonZeroU32, NonZeroU64};

const DEFAULT_WEBHOOK_DESTINATION_ID: &str = "server/http-webhook";
const DEFAULT_WEBHOOK_TIMEOUT_SECONDS: u32 = 5;
const MAX_WEBHOOK_TIMEOUT_SECONDS: u32 = 300;
const DEFAULT_WEBHOOK_MAX_ATTEMPTS: u32 = 32;
const MAX_WEBHOOK_MAX_ATTEMPTS: u32 = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutboxDocument {
    kind: String,
    directory: Option<String>,
    quota_encoded_bytes: Option<u64>,
    destination_id: Option<String>,
    endpoint: Option<String>,
    ca_certificate_path: Option<String>,
    bearer_token_path: Option<String>,
    timeout_seconds: Option<u32>,
    max_attempts: Option<u32>,
}

/// Validated HTTP/HTTPS webhook or message-broker push destination (`ADR-0332`).
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ConfiguredWebhookOutbox {
    destination_id: OutboxDestinationIdV1,
    endpoint: TelemetryPushEndpoint,
    bearer_token_path: Option<ProtectedFilePath>,
    timeout_seconds: NonZeroU32,
    max_attempts: NonZeroU32,
}

impl ConfiguredWebhookOutbox {
    pub(crate) const fn destination_id(&self) -> &OutboxDestinationIdV1 {
        &self.destination_id
    }

    pub(crate) const fn endpoint(&self) -> &TelemetryPushEndpoint {
        &self.endpoint
    }

    pub(crate) const fn bearer_token_path(&self) -> Option<&ProtectedFilePath> {
        self.bearer_token_path.as_ref()
    }

    pub(crate) const fn timeout_seconds(&self) -> NonZeroU32 {
        self.timeout_seconds
    }

    pub(crate) const fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum ConfiguredOutbox {
    DurableLocal {
        directory: PathBuf,
        quota: NonZeroU64,
    },
    HttpWebhook(ConfiguredWebhookOutbox),
}

impl ConfiguredOutbox {
    pub(crate) fn directory(&self) -> Option<&Path> {
        match self {
            Self::DurableLocal { directory, .. } => Some(directory.as_path()),
            Self::HttpWebhook(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn quota(&self) -> Option<NonZeroU64> {
        match self {
            Self::DurableLocal { quota, .. } => Some(*quota),
            Self::HttpWebhook(_) => None,
        }
    }
}

impl fmt::Debug for ConfiguredOutbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfiguredOutbox([REDACTED])")
    }
}
impl fmt::Debug for OutboxDocument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OutboxDocument([REDACTED])")
    }
}
pub(super) fn parse_outbox(
    document: Option<OutboxDocument>,
    default_directory: PathBuf,
) -> Result<Option<ConfiguredOutbox>, ServerConfigError> {
    document
        .map(|document| match document.kind.as_str() {
            "durable_local" => {
                if document.destination_id.is_some()
                    || document.endpoint.is_some()
                    || document.ca_certificate_path.is_some()
                    || document.bearer_token_path.is_some()
                    || document.timeout_seconds.is_some()
                    || document.max_attempts.is_some()
                {
                    return Err(ServerConfigError::InvalidOutboxConfiguration);
                }
                let directory = match document.directory {
                    Some(raw_dir) => bounded_absolute_directory(raw_dir.into())?,
                    None => bounded_absolute_directory(default_directory.into_os_string())?,
                };
                let raw_quota = document
                    .quota_encoded_bytes
                    .unwrap_or(DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES);
                Ok(ConfiguredOutbox::DurableLocal {
                    directory,
                    quota: NonZeroU64::new(raw_quota)
                        .ok_or(ServerConfigError::InvalidOutboxConfiguration)?,
                })
            }
            "http_webhook" => {
                if document.directory.is_some() || document.quota_encoded_bytes.is_some() {
                    return Err(ServerConfigError::InvalidOutboxConfiguration);
                }
                let raw_endpoint = document
                    .endpoint
                    .ok_or(ServerConfigError::InvalidOutboxConfiguration)?;
                let ca_certificate_path = document
                    .ca_certificate_path
                    .map(|raw| {
                        ProtectedFilePath::new(PathBuf::from(raw))
                            .map_err(|_| ServerConfigError::InvalidOutboxConfiguration)
                    })
                    .transpose()?;
                let bearer_token_path = document
                    .bearer_token_path
                    .map(|raw| {
                        ProtectedFilePath::new(PathBuf::from(raw))
                            .map_err(|_| ServerConfigError::InvalidOutboxConfiguration)
                    })
                    .transpose()?;
                let endpoint = parse_telemetry_push_endpoint(&raw_endpoint, ca_certificate_path)
                    .map_err(|_| ServerConfigError::InvalidOutboxConfiguration)?;
                let raw_dest_id = document
                    .destination_id
                    .unwrap_or_else(|| DEFAULT_WEBHOOK_DESTINATION_ID.to_owned());
                let destination_id = OutboxDestinationIdV1::new(raw_dest_id)
                    .map_err(|_| ServerConfigError::InvalidOutboxConfiguration)?;
                let raw_timeout = document
                    .timeout_seconds
                    .unwrap_or(DEFAULT_WEBHOOK_TIMEOUT_SECONDS);
                if raw_timeout == 0 || raw_timeout > MAX_WEBHOOK_TIMEOUT_SECONDS {
                    return Err(ServerConfigError::InvalidOutboxConfiguration);
                }
                let timeout_seconds = NonZeroU32::new(raw_timeout)
                    .ok_or(ServerConfigError::InvalidOutboxConfiguration)?;
                let raw_attempts = document
                    .max_attempts
                    .unwrap_or(DEFAULT_WEBHOOK_MAX_ATTEMPTS);
                if raw_attempts == 0 || raw_attempts > MAX_WEBHOOK_MAX_ATTEMPTS {
                    return Err(ServerConfigError::InvalidOutboxConfiguration);
                }
                let max_attempts = NonZeroU32::new(raw_attempts)
                    .ok_or(ServerConfigError::InvalidOutboxConfiguration)?;
                Ok(ConfiguredOutbox::HttpWebhook(ConfiguredWebhookOutbox {
                    destination_id,
                    endpoint,
                    bearer_token_path,
                    timeout_seconds,
                    max_attempts,
                }))
            }
            _ => Err(ServerConfigError::InvalidOutboxConfiguration),
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    // req: EFF-002, EFF-003, STO-020
    use super::*;
    #[test]
    fn named_destination_paths_are_disjoint_across_all_database_owners() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config.toml");
        let database = |alias: &str, destination: &str| {
            format!(
                "[databases.{alias}]\npath = '{0}/{alias}.redb'\nbackup_root = '{0}/{alias}-backups'\nprojections_root = '{0}/{alias}-projections'\nenvironment = 'test'\n[databases.{alias}.outbox]\nkind = 'durable_local'\ndirectory = '{0}/{destination}'\nquota_encoded_bytes = 4096\n",
                root.path().display()
            )
        };
        for (second, accepted) in [
            ("destination-two", true),
            ("destination-one", false),
            ("destination-one/subdir", false),
            ("one-backups", false),
            ("one-projections", false),
        ] {
            std::fs::write(
                &config,
                format!(
                    "{}{}",
                    database("one", "destination-one"),
                    database("two", second)
                ),
            )
            .unwrap();
            assert_eq!(
                ServerConfig::resolve(
                    ["--config".into(), config.clone().into_os_string()],
                    &EmptyEnvironment,
                    root.path()
                )
                .is_ok(),
                accepted
            );
        }
    }
    #[test]
    fn destination_is_explicit_positive_bounded_disjoint_and_redacted() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        let destination = root.path().join("destination");
        let valid = format!(
            "[maintenance.outbox]\nkind = 'durable_local'\ndirectory = '{}'\nquota_encoded_bytes = 1048576\n",
            destination.display()
        );
        for (document, accepted) in [
            (valid.clone(), true),
            (valid.replace("durable_local", "http"), false),
            (valid.replace("1048576", "0"), false),
            (valid.replace("1048576", "-1"), false),
            (
                valid.replace(&destination.display().to_string(), "relative"),
                false,
            ),
            (
                valid.replace(
                    &destination.display().to_string(),
                    &root.path().join("backups/subdir").display().to_string(),
                ),
                false,
            ),
            (format!("{valid}unrecognized = true\n"), false),
        ] {
            std::fs::write(&path, document).unwrap();
            let result = ServerConfig::resolve(
                ["--config".into(), path.clone().into_os_string()],
                &EmptyEnvironment,
                root.path(),
            );
            assert_eq!(result.is_ok(), accepted);
            if let Ok(config) = result {
                let outbox = config.databases()[0].outbox().unwrap();
                assert_eq!(outbox.directory(), Some(destination.as_path()));
                assert!(!format!("{outbox:?}").contains(&destination.display().to_string()));
            }
        }
        // Omitting quota_encoded_bytes on durable_local defaults to 256 MiB (DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES).
        std::fs::write(
            &path,
            format!(
                "[maintenance.outbox]\nkind = 'durable_local'\ndirectory = '{}'\n",
                destination.display()
            ),
        )
        .unwrap();
        let config = ServerConfig::resolve(
            ["--config".into(), path.clone().into_os_string()],
            &EmptyEnvironment,
            root.path(),
        )
        .expect("durable_local without explicit quota uses default 256 MiB quota");
        assert_eq!(
            config.databases()[0]
                .outbox()
                .and_then(|o| o.quota())
                .map(NonZeroU64::get),
            Some(DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES)
        );
        // Omitting both directory and quota_encoded_bytes defaults directory to <database_path>.riffoutbox.
        std::fs::write(&path, "[maintenance.outbox]\nkind = 'durable_local'\n").unwrap();
        let config = ServerConfig::resolve(
            ["--config".into(), path.into_os_string()],
            &EmptyEnvironment,
            root.path(),
        )
        .expect("durable_local without directory defaults to <database_path>.riffoutbox");
        let expected_default_dir = root.path().join("data/riffdb.redb.riffoutbox");
        assert_eq!(
            config.databases()[0].outbox().and_then(|o| o.directory()),
            Some(expected_default_dir.as_path())
        );
    }
}
