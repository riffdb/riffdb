//! Complete follower source binding. Values never enter configuration diagnostics.
use super::*;
use riffdb_config::{TlsClientConfig, TlsServerIdentity};
use riffdb_storage_api::{ChangelogLineageV3, LeadershipEpochV1, ReplicationSourceHoldIdV1};
use riffdb_types::{DatabaseId, parse_hex16};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerMode {
    Primary,
    Follower,
}
impl ServerMode {
    pub(super) fn parse(value: OsString) -> Result<Self, ServerConfigError> {
        match value.to_str() {
            Some("primary") => Ok(Self::Primary),
            Some("follower") => Ok(Self::Follower),
            _ => Err(invalid()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FollowerSourceConfig {
    pub(crate) tls: TlsClientConfig,
    pub(crate) credential: ProtectedFilePath,
    pub(crate) database: DatabaseAlias,
    pub(crate) lineage: ChangelogLineageV3,
    pub(crate) hold: ReplicationSourceHoldIdV1,
}
impl fmt::Debug for FollowerSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FollowerSourceConfig([redacted])")
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FollowerSourceDocument {
    endpoint: String,
    trust_root: String,
    server_name: String,
    credential_file: String,
    database: String,
    database_id: String,
    history_incarnation: u64,
    leadership_epoch: u64,
    hold_id: String,
}
impl fmt::Debug for FollowerSourceDocument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FollowerSourceDocument([redacted])")
    }
}
impl FollowerSourceDocument {
    pub(super) fn resolve(self) -> Result<FollowerSourceConfig, ServerConfigError> {
        let endpoint = CanonicalHttpsEndpoint::parse(&self.endpoint).map_err(|_| invalid())?;
        let trust_root = protected_path(self.trust_root)?;
        let credential = protected_path(self.credential_file)?;
        let identity = TlsServerIdentity::parse(&self.server_name).map_err(|_| invalid())?;
        let tls = TlsClientConfig::new(
            endpoint,
            trust_root,
            identity,
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::MIN,
            NonZeroU32::MIN,
        )
        .map_err(|_| invalid())?;
        if self.database_id.len() != 36
            || self
                .database_id
                .bytes()
                .enumerate()
                .any(|(i, b)| matches!(i, 8 | 13 | 18 | 23) && b != b'-')
        {
            return Err(invalid());
        }
        let compact: String = self.database_id.chars().filter(|c| *c != '-').collect();
        let database_id = DatabaseId::from_bytes(parse_hex16(&compact).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
        let lineage = ChangelogLineageV3::new_with_catalog(
            database_id,
            self.history_incarnation,
            LeadershipEpochV1::new(self.leadership_epoch).ok_or_else(invalid)?,
            riffdb_storage_api::AuthoritativeStateCatalogV2.digest(),
        )
        .map_err(|_| invalid())?;
        let hold =
            ReplicationSourceHoldIdV1::new(parse_hex16(&self.hold_id).map_err(|_| invalid())?)
                .ok_or_else(invalid)?;
        Ok(FollowerSourceConfig {
            tls,
            credential,
            lineage,
            hold,
            database: DatabaseAlias::new(self.database).map_err(|_| invalid())?,
        })
    }
}
fn protected_path(value: String) -> Result<ProtectedFilePath, ServerConfigError> {
    ProtectedFilePath::new(bounded_absolute_directory(value.into())?).map_err(|_| invalid())
}
fn invalid() -> ServerConfigError {
    ServerConfigError::InvalidFollowerConfiguration
}

/// Primary command commit durability acknowledgement mode (ADR-0337).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum CommitAcknowledgementMode {
    /// Release outcomes after local durable journal commit (`k = 0`).
    #[default]
    Local,
    /// Release outcomes only after `required_followers` registered voter
    /// followers durably acknowledge the committed sequence within `timeout`.
    Quorum {
        required_followers: NonZeroU32,
        timeout: Duration,
    },
}

/// Deployment-configured replication quorum and automatic failover policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicationQuorumConfig {
    pub(crate) commit_acknowledgement: CommitAcknowledgementMode,
    pub(crate) auto_failover: bool,
    pub(crate) lease_duration: Duration,
    pub(crate) election_timeout: Duration,
    pub(crate) voters: Vec<String>,
}

impl Default for ReplicationQuorumConfig {
    fn default() -> Self {
        Self {
            commit_acknowledgement: CommitAcknowledgementMode::Local,
            auto_failover: false,
            lease_duration: Duration::from_millis(3_000),
            election_timeout: Duration::from_millis(6_000),
            voters: Vec::new(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplicationQuorumDocument {
    commit_acknowledgement: Option<String>,
    quorum_ack_followers: Option<u32>,
    quorum_ack_timeout_ms: Option<u64>,
    auto_failover: Option<bool>,
    lease_duration_ms: Option<u64>,
    election_timeout_ms: Option<u64>,
    voters: Option<Vec<String>>,
}

impl ReplicationQuorumDocument {
    pub(super) fn resolve(self) -> Result<ReplicationQuorumConfig, ServerConfigError> {
        let commit_acknowledgement = match self.commit_acknowledgement.as_deref() {
            None | Some("local") => {
                if self.quorum_ack_followers.is_some() || self.quorum_ack_timeout_ms.is_some() {
                    return Err(invalid());
                }
                CommitAcknowledgementMode::Local
            }
            Some("quorum") => {
                let raw_k = self.quorum_ack_followers.ok_or_else(invalid)?;
                if raw_k == 0 || (raw_k as usize) > riffdb_storage_api::MAX_QUORUM_ACK_FOLLOWERS_V1
                {
                    return Err(invalid());
                }
                let required_followers = NonZeroU32::new(raw_k).ok_or_else(invalid)?;
                let timeout_ms = self.quorum_ack_timeout_ms.unwrap_or(5_000);
                if !(100..=30_000).contains(&timeout_ms) {
                    return Err(invalid());
                }
                CommitAcknowledgementMode::Quorum {
                    required_followers,
                    timeout: Duration::from_millis(timeout_ms),
                }
            }
            Some(_) => return Err(invalid()),
        };
        let auto_failover = self.auto_failover.unwrap_or(false);
        let lease_ms = self.lease_duration_ms.unwrap_or(3_000);
        let election_ms = self.election_timeout_ms.unwrap_or(6_000);
        if !(100..=30_000).contains(&lease_ms)
            || !(200..=60_000).contains(&election_ms)
            || election_ms <= lease_ms
        {
            return Err(invalid());
        }
        let voters = self.voters.unwrap_or_default();
        if voters.len() > riffdb_storage_api::MAX_QUORUM_ACK_FOLLOWERS_V1
            || voters.iter().any(|v| v.is_empty() || v.len() > 256)
        {
            return Err(invalid());
        }
        Ok(ReplicationQuorumConfig {
            commit_acknowledgement,
            auto_failover,
            lease_duration: Duration::from_millis(lease_ms),
            election_timeout: Duration::from_millis(election_ms),
            voters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const SOURCE: &str = r#"
endpoint = "https://primary.example:7443"
trust_root = "/etc/riffdb/source-ca.pem"
server_name = "primary.example"
credential_file = "/etc/riffdb/source.token"
database = "default"
database_id = "018bcfe5-6800-7101-8101-010101010101"
history_incarnation = 1
leadership_epoch = 1
hold_id = "71717171717171717171717171717171"
"#;
    // req: REP-002, REP-003
    #[test]
    fn follower_source_requires_complete_identity_and_verified_transport() {
        let document: FollowerSourceDocument = toml::from_str(SOURCE).unwrap();
        let config = document.resolve().unwrap();
        assert_eq!(config.lineage.history_incarnation(), 1);
        assert_eq!(
            config.lineage.catalog_digest(),
            riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
        );
        assert_eq!(config.hold.as_bytes(), &[0x71; 16]);
        assert!(!format!("{config:?}").contains("primary.example"));
        for (from, to) in [
            ("https://", "http://"),
            (
                "server_name = \"primary.example\"",
                "server_name = \"foreign.example\"",
            ),
            ("history_incarnation = 1", "history_incarnation = 0"),
            ("leadership_epoch = 1", "leadership_epoch = 0"),
            (
                "71717171717171717171717171717171",
                "00000000000000000000000000000000",
            ),
            ("/etc/riffdb/source.token", "source.token"),
        ] {
            let document: FollowerSourceDocument =
                toml::from_str(&SOURCE.replace(from, to)).unwrap();
            assert!(document.resolve().is_err(), "{from}");
        }
        assert!(
            toml::from_str::<FollowerSourceDocument>(&SOURCE.replace("leadership_epoch = 1", ""))
                .is_err()
        );
        assert!(
            toml::from_str::<FollowerSourceDocument>(&format!("{SOURCE}\ninsecure = true"))
                .is_err()
        );
    }
    // req: REP-002, REP-003
    #[test]
    fn follower_mode_never_defaults_to_primary_or_accepts_partial_source_configuration() {
        let scope = tempfile::tempdir().unwrap();
        let path = scope.path().join("config.toml");
        let document =
            format!("[server]\nmode = \"follower\"\n[server.replication_source]\n{SOURCE}");
        std::fs::write(&path, &document).unwrap();
        let arguments = vec![OsString::from("--config"), path.clone().into_os_string()];
        let config = ServerConfig::parse(arguments.clone()).unwrap();
        assert_eq!(config.mode(), ServerMode::Follower);
        assert!(config.databases()[0].follower().is_some());
        let outbox = format!(
            "{document}\n[maintenance.outbox]\nkind = 'durable_local'\ndirectory = '{}/destination'\nquota_encoded_bytes = 4096\n",
            scope.path().display()
        );
        std::fs::write(&path, outbox).unwrap();
        assert!(matches!(
            ServerConfig::parse(arguments.clone()),
            Err(ServerConfigError::InvalidOutboxConfiguration)
        ));
        std::fs::write(&path, &document).unwrap();
        let mut primary = arguments.clone();
        primary.extend([OsString::from("--mode"), OsString::from("primary")]);
        assert!(
            ServerConfig::parse(primary).is_err(),
            "source config cannot be ignored in primary mode"
        );
        std::fs::write(&path, "[server]\nmode = \"follower\"\n").unwrap();
        assert!(ServerConfig::parse(arguments).is_err());
        assert!(
            ServerConfig::parse([OsString::from("--mode"), OsString::from("unknown")]).is_err()
        );
    }
}
