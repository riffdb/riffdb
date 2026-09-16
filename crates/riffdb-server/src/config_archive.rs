//! Operator-only named filesystem archive bindings.
use super::*;
use riffdb_storage_api::ArchiveEncryptionPostureV1;
use riffdb_types::ArchiveNameV1;

const MAX_CONFIGURED_ARCHIVES: usize = 16;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArchiveDocument {
    name: String,
    path: String,
    encryption: String,
}

#[derive(Clone)]
pub(crate) struct ConfiguredArchive {
    name: ArchiveNameV1,
    path: PathBuf,
    encryption: ArchiveEncryptionPostureV1,
}
impl ConfiguredArchive {
    pub(crate) fn name(&self) -> &ArchiveNameV1 {
        &self.name
    }
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn encryption(&self) -> ArchiveEncryptionPostureV1 {
        self.encryption
    }
}
impl fmt::Debug for ConfiguredArchive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfiguredArchive([redacted])")
    }
}

pub(super) fn parse_archives(
    documents: Vec<ArchiveDocument>,
) -> Result<Vec<ConfiguredArchive>, ServerConfigError> {
    if documents.len() > MAX_CONFIGURED_ARCHIVES {
        return Err(ServerConfigError::InvalidArchiveConfiguration);
    }
    let mut seen = std::collections::BTreeSet::new();
    documents
        .into_iter()
        .map(|document| {
            let name = ArchiveNameV1::new(document.name)
                .map_err(|_| ServerConfigError::InvalidArchiveConfiguration)?;
            if !seen.insert(name.clone()) {
                return Err(ServerConfigError::InvalidArchiveConfiguration);
            }
            let path = bounded_absolute_directory(document.path.into())?;
            let encryption = match document.encryption.as_str() {
                "unencrypted" => ArchiveEncryptionPostureV1::Unencrypted,
                "operator_managed" => ArchiveEncryptionPostureV1::OperatorManaged,
                _ => return Err(ServerConfigError::InvalidArchiveConfiguration),
            };
            Ok(ConfiguredArchive {
                name,
                path,
                encryption,
            })
        })
        .collect()
}

impl ServerConfig {
    /// Resolve owner paths once when constructing offline driver dependencies.
    /// Request input can choose only an explicitly configured checked name.
    pub(crate) fn archive_bindings(
        &self,
    ) -> Result<Vec<(PathBuf, ConfiguredArchive)>, ServerConfigError> {
        let cwd = std::env::current_dir().map_err(|_| ServerConfigError::InvalidPath)?;
        let mut entries = Vec::new();
        for database in self.databases() {
            let path = lexical_absolute(database.database_path(), &cwd)?;
            entries.extend(
                database
                    .archives()
                    .iter()
                    .cloned()
                    .map(|archive| (path.clone(), archive)),
            );
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    // req: REP-007, AFC-007
    use super::*;
    #[test]
    fn archive_configuration_is_named_bounded_disjoint_and_explicit_about_encryption() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        let archive = root.path().join("archive");
        let valid = format!(
            "[[maintenance.archives]]\nname = 'daily'\npath = '{}'\nencryption = 'operator_managed'\n",
            archive.display()
        );
        for (document, accepted) in [
            (valid.clone(), true),
            (format!("{valid}{valid}"), false),
            (valid.replace("operator_managed", "automatic"), false),
            (valid.replace("name = 'daily'", "name = '../daily'"), false),
            (
                valid.replace(&archive.display().to_string(), "relative"),
                false,
            ),
            (
                valid.replace(
                    &archive.display().to_string(),
                    &root.path().join("backups/archive").display().to_string(),
                ),
                false,
            ),
            (
                (0..17)
                    .map(|i| {
                        valid
                            .replace("daily", &format!("daily-{i}"))
                            .replace("archive'", &format!("archive-{i}'"))
                    })
                    .collect::<String>(),
                false,
            ),
        ] {
            std::fs::write(&path, document).unwrap();
            let parsed = ServerConfig::resolve(
                ["--config".into(), path.clone().into_os_string()],
                &EmptyEnvironment,
                root.path(),
            );
            assert_eq!(parsed.is_ok(), accepted);
            if let Ok(config) = parsed {
                let archives = config.databases()[0].archives();
                assert_eq!(archives.len(), 1);
                assert_eq!(archives[0].name().as_str(), "daily");
                assert_eq!(
                    archives[0].encryption(),
                    ArchiveEncryptionPostureV1::OperatorManaged
                );
                assert!(!format!("{:?}", archives[0]).contains(&archive.display().to_string()));
            }
        }
    }
    #[test]
    fn named_database_archives_reject_cross_database_path_ownership() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config.toml");
        let database = |alias: &str, archive: &str| {
            format!(
                "[databases.{alias}]\npath = '{0}/{alias}.redb'\nbackup_root = '{0}/{alias}-backups'\nenvironment = 'test'\n[[databases.{alias}.archives]]\nname = 'daily'\npath = '{0}/{archive}'\nencryption = 'unencrypted'\n",
                root.path().display()
            )
        };
        let first = database("one", "archive-one");
        for (second, accepted) in [
            (database("two", "archive-two"), true),
            (database("two", "archive-one"), false),
            (database("two", "one-backups/archive"), false),
            (
                database("two", "archive-two").replace("encryption = 'unencrypted'\n", ""),
                false,
            ),
        ] {
            std::fs::write(&config, format!("{first}{second}")).unwrap();
            let result = ServerConfig::resolve(
                ["--config".into(), config.clone().into_os_string()],
                &EmptyEnvironment,
                root.path(),
            );
            assert_eq!(result.is_ok(), accepted);
            if let Ok(config) = result {
                let bindings = config.archive_bindings().unwrap();
                assert_eq!(bindings.len(), 2);
                assert_ne!(bindings[0].0, bindings[1].0);
                assert_eq!(bindings[0].1.name(), bindings[1].1.name());
                assert_ne!(bindings[0].1.path(), bindings[1].1.path());
            }
        }
    }
}
