//! ADR-0182 section 6: the online retention lane's operator configuration.
//!
//! The lane's target is a retained-sequence count or a hold, never a
//! wall-clock age, and its progress budget is sequences per durability epoch
//! (one lane step). An absent section is a hold, so a configuration that never
//! names retention keeps every sequence exactly as before.
use super::*;
use std::num::NonZeroU64;

/// The largest per-epoch budget: one online step's hard bound.
const MAX_SEQUENCES_PER_EPOCH: u64 = riffdb_storage_redb::ONLINE_RETENTION_STEP_SEQUENCES;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetentionDocument {
    retained_sequences: Option<u64>,
    hold: Option<bool>,
    sequences_per_epoch: Option<u64>,
}

/// What the online lane compacts toward.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetentionTarget {
    /// The lane does not run; nothing is pruned online.
    Hold,
    /// Keep this many sequences below the durable application head.
    RetainSequences(NonZeroU64),
}

/// One database's online retention configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetentionConfig {
    target: RetentionTarget,
    sequences_per_epoch: NonZeroU64,
}

impl RetentionConfig {
    /// Test-only constructor bypassing document parsing.
    #[cfg(test)]
    pub(crate) const fn for_test(target: RetentionTarget, sequences_per_epoch: NonZeroU64) -> Self {
        Self {
            target,
            sequences_per_epoch,
        }
    }

    pub(crate) const fn target(&self) -> RetentionTarget {
        self.target
    }

    /// Sequences one lane step may remove (1..=256).
    pub(crate) const fn sequences_per_epoch(&self) -> NonZeroU64 {
        self.sequences_per_epoch
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            target: RetentionTarget::Hold,
            sequences_per_epoch: NonZeroU64::new(MAX_SEQUENCES_PER_EPOCH)
                .unwrap_or(NonZeroU64::MIN),
        }
    }
}

pub(super) fn parse_retention(
    document: Option<RetentionDocument>,
) -> Result<RetentionConfig, ServerConfigError> {
    let Some(document) = document else {
        return Ok(RetentionConfig::default());
    };
    let target = match (document.retained_sequences, document.hold) {
        (Some(count), None) => RetentionTarget::RetainSequences(
            NonZeroU64::new(count).ok_or(ServerConfigError::InvalidRetentionConfiguration)?,
        ),
        (None, Some(true)) => RetentionTarget::Hold,
        _ => return Err(ServerConfigError::InvalidRetentionConfiguration),
    };
    let sequences_per_epoch = match document.sequences_per_epoch {
        None => RetentionConfig::default().sequences_per_epoch,
        Some(budget) if budget <= MAX_SEQUENCES_PER_EPOCH => {
            NonZeroU64::new(budget).ok_or(ServerConfigError::InvalidRetentionConfiguration)?
        }
        Some(_) => return Err(ServerConfigError::InvalidRetentionConfiguration),
    };
    Ok(RetentionConfig {
        target,
        sequences_per_epoch,
    })
}

#[cfg(test)]
mod tests {
    // req: STO-030
    use super::*;

    fn resolve(document: &str) -> Result<ServerConfig, ServerConfigError> {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        std::fs::write(&path, document).unwrap();
        ServerConfig::resolve(
            ["--config".into(), path.into_os_string()],
            &EmptyEnvironment,
            root.path(),
        )
    }

    #[test]
    fn retention_configuration_is_a_sequence_target_or_a_hold_with_a_bounded_budget() {
        let default = resolve("").unwrap();
        assert_eq!(
            default.databases()[0].retention(),
            RetentionConfig::default()
        );
        assert_eq!(
            default.databases()[0].retention().target(),
            RetentionTarget::Hold,
            "an absent section prunes nothing online"
        );
        let retained = resolve(
            "[maintenance.retention]\nretained_sequences = 4096\nsequences_per_epoch = 64\n",
        )
        .unwrap();
        let retention = retained.databases()[0].retention();
        assert_eq!(
            retention.target(),
            RetentionTarget::RetainSequences(NonZeroU64::new(4096).unwrap())
        );
        assert_eq!(retention.sequences_per_epoch().get(), 64);
        assert_eq!(
            resolve("[maintenance.retention]\nhold = true\n")
                .unwrap()
                .databases()[0]
                .retention()
                .target(),
            RetentionTarget::Hold
        );
        for rejected in [
            "[maintenance.retention]\n",
            "[maintenance.retention]\nhold = false\n",
            "[maintenance.retention]\nretained_sequences = 0\n",
            "[maintenance.retention]\nretained_sequences = 8\nhold = true\n",
            "[maintenance.retention]\nretained_sequences = 8\nsequences_per_epoch = 0\n",
            "[maintenance.retention]\nretained_sequences = 8\nsequences_per_epoch = 257\n",
            "[maintenance.retention]\nretained_sequences = 8\nmax_age_seconds = 60\n",
        ] {
            assert!(
                matches!(
                    resolve(rejected),
                    Err(ServerConfigError::InvalidRetentionConfiguration
                        | ServerConfigError::InvalidConfigDocument)
                ),
                "{rejected:?}"
            );
        }
    }

    #[test]
    fn named_databases_carry_their_own_retention() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[databases.one]\npath = '{0}/one.redb'\nbackup_root = '{0}/one-backups'\nenvironment = 'test'\n[databases.one.retention]\nretained_sequences = 10\n[databases.two]\npath = '{0}/two.redb'\nbackup_root = '{0}/two-backups'\nenvironment = 'test'\n",
                root.path().display()
            ),
        )
        .unwrap();
        let resolved = ServerConfig::resolve(
            ["--config".into(), config.into_os_string()],
            &EmptyEnvironment,
            root.path(),
        )
        .unwrap();
        let targets = resolved
            .databases()
            .iter()
            .map(|database| database.retention().target())
            .collect::<Vec<_>>();
        assert_eq!(
            targets,
            vec![
                RetentionTarget::RetainSequences(NonZeroU64::new(10).unwrap()),
                RetentionTarget::Hold
            ]
        );
    }
}
