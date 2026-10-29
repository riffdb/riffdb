//! Restart-applied operator-only durable local destination configuration.
use super::*;
use std::num::NonZeroU64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutboxDocument {
    kind: String,
    directory: String,
    quota_encoded_bytes: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ConfiguredOutbox {
    directory: PathBuf,
    quota: NonZeroU64,
}
impl ConfiguredOutbox {
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }
    pub(crate) const fn quota(&self) -> NonZeroU64 {
        self.quota
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
) -> Result<Option<ConfiguredOutbox>, ServerConfigError> {
    document
        .map(|document| {
            if document.kind != "durable_local" {
                return Err(ServerConfigError::InvalidOutboxConfiguration);
            }
            Ok(ConfiguredOutbox {
                directory: bounded_absolute_directory(document.directory.into())?,
                quota: NonZeroU64::new(document.quota_encoded_bytes)
                    .ok_or(ServerConfigError::InvalidOutboxConfiguration)?,
            })
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
                assert_eq!(outbox.directory(), destination);
                assert!(!format!("{outbox:?}").contains(&destination.display().to_string()));
            }
        }
    }
}
