use std::fmt;
use std::fs;
use std::io::Read as _;
use std::num::NonZeroU32;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use riffdb_application::ApplicationPortabilityManifest;
use riffdb_client_rust::{
    ApplicationInstallationCampaignId, CallMetadata, CapabilityApplicationReimportScopeV1,
    RiffDbClient, StartApplicationReimport, load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_types::{ContractLineage, DatabaseAlias};
use serde::Deserialize;

use crate::{OperatorDriverHost, OperatorDriverSocket};

const MAX_CONFIG_BYTES: u64 = 64 * 1_024;
const MAX_MANIFEST_BYTES: u64 = 1_024 * 1_024;
const MAX_TERMINAL_DOCUMENT_BYTES: u64 = 256 * 1_024;

/// Protected runtime for one operator-only reimport campaign.
pub struct OperatorDriverRuntime {
    host: OperatorDriverHost,
    socket: OperatorDriverSocket,
}

impl OperatorDriverRuntime {
    /// Loads exact protected artifacts and establishes verified TLS before binding locally.
    pub async fn from_config_file(path: &Path) -> Result<Self, OperatorRuntimeError> {
        let document: OperatorDocument = toml::from_str(
            std::str::from_utf8(&read_checked(path, MAX_CONFIG_BYTES, true)?)
                .map_err(|_| OperatorRuntimeError)?,
        )
        .map_err(|_| OperatorRuntimeError)?;
        let database = DatabaseAlias::new(document.campaign.database.clone())
            .map_err(|_| OperatorRuntimeError)?;
        let campaign_id = ApplicationInstallationCampaignId::from_bytes(
            parse_uuid_v7(&document.campaign.campaign_id).ok_or(OperatorRuntimeError)?,
        )
        .map_err(|_| OperatorRuntimeError)?;
        let lineage = ContractLineage::new(document.campaign.contract_lineage.clone())
            .map_err(|_| OperatorRuntimeError)?;
        let scope = match document.campaign.scope.as_str() {
            "principal_filtered" => CapabilityApplicationReimportScopeV1::PrincipalFiltered,
            "whole_application" => CapabilityApplicationReimportScopeV1::WholeApplication,
            _ => return Err(OperatorRuntimeError),
        };
        let portability = ApplicationPortabilityManifest::decode_canonical(&read_checked(
            &checked_absolute(&document.campaign.portability_manifest)?,
            MAX_MANIFEST_BYTES,
            false,
        )?)
        .map_err(|_| OperatorRuntimeError)?;
        let export_manifest = read_checked(
            &checked_absolute(&document.campaign.export_manifest)?,
            MAX_TERMINAL_DOCUMENT_BYTES,
            false,
        )?;
        let export_receipt = read_checked(
            &checked_absolute(&document.campaign.export_receipt)?,
            MAX_TERMINAL_DOCUMENT_BYTES,
            false,
        )?;
        let start = StartApplicationReimport::new(
            campaign_id,
            lineage,
            scope,
            portability,
            export_manifest,
            export_receipt,
        )
        .map_err(|_| OperatorRuntimeError)?;
        let tls = tls_config(&document)?;
        let credential =
            load_protected_bearer_credential(&checked_absolute(&document.remote.credential_file)?)
                .map_err(|_| OperatorRuntimeError)?;
        let metadata = CallMetadata::authenticated(credential).with_database(database);
        let client = RiffDbClient::connect_verified_tls(&tls)
            .await
            .map_err(|_| OperatorRuntimeError)?;
        let socket = OperatorDriverSocket::bind(checked_absolute(&document.driver.socket)?)
            .map_err(|_| OperatorRuntimeError)?;
        Ok(Self {
            host: OperatorDriverHost::new(document.campaign.database, start, client, metadata),
            socket,
        })
    }

    /// Serves until shutdown and removes only the owned private socket.
    pub async fn serve_until<F>(self, shutdown: F) -> Result<(), OperatorRuntimeError>
    where
        F: std::future::Future<Output = ()>,
    {
        self.socket
            .serve_until(self.host, shutdown)
            .await
            .map_err(|_| OperatorRuntimeError)
    }
}

fn tls_config(document: &OperatorDocument) -> Result<TlsClientConfig, OperatorRuntimeError> {
    TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&document.remote.endpoint)
            .map_err(|_| OperatorRuntimeError)?,
        ProtectedFilePath::new(checked_absolute(&document.remote.tls_trust_root)?)
            .map_err(|_| OperatorRuntimeError)?,
        TlsServerIdentity::parse(&document.remote.tls_server_name)
            .map_err(|_| OperatorRuntimeError)?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(1).expect("one"),
        NonZeroU32::new(32).expect("positive"),
    )
    .map_err(|_| OperatorRuntimeError)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorDocument {
    driver: OperatorDriverSection,
    campaign: OperatorCampaignSection,
    remote: OperatorRemoteSection,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorDriverSection {
    socket: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorCampaignSection {
    database: String,
    campaign_id: String,
    contract_lineage: String,
    scope: String,
    portability_manifest: String,
    export_manifest: String,
    export_receipt: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorRemoteSection {
    endpoint: String,
    tls_trust_root: String,
    tls_server_name: String,
    credential_file: String,
}

fn checked_absolute(value: &str) -> Result<PathBuf, OperatorRuntimeError> {
    let path = PathBuf::from(value);
    if !path.is_absolute()
        || path.as_os_str().as_encoded_bytes().len() > 4_096
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(OperatorRuntimeError);
    }
    Ok(path)
}

fn read_checked(path: &Path, maximum: u64, private: bool) -> Result<Vec<u8>, OperatorRuntimeError> {
    let link = fs::symlink_metadata(path).map_err(|_| OperatorRuntimeError)?;
    if !link.is_file()
        || link.file_type().is_symlink()
        || link.len() == 0
        || link.len() > maximum
        || link.permissions().mode() & 0o022 != 0
        || (private && link.permissions().mode() & 0o077 != 0)
    {
        return Err(OperatorRuntimeError);
    }
    let file = fs::File::open(path).map_err(|_| OperatorRuntimeError)?;
    let opened = file.metadata().map_err(|_| OperatorRuntimeError)?;
    if opened.dev() != link.dev() || opened.ino() != link.ino() || opened.len() != link.len() {
        return Err(OperatorRuntimeError);
    }
    let mut bytes =
        Vec::with_capacity(usize::try_from(opened.len()).map_err(|_| OperatorRuntimeError)?);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| OperatorRuntimeError)?;
    let after = fs::symlink_metadata(path).map_err(|_| OperatorRuntimeError)?;
    if bytes.is_empty()
        || u64::try_from(bytes.len())
            .ok()
            .is_none_or(|len| len > maximum)
        || after.dev() != opened.dev()
        || after.ino() != opened.ino()
        || after.len() != opened.len()
    {
        return Err(OperatorRuntimeError);
    }
    Ok(bytes)
}

fn parse_uuid_v7(value: &str) -> Option<[u8; 16]> {
    let compact = value.replace('-', "");
    if compact.len() != 32 {
        return None;
    }
    let mut out = [0; 16];
    for (slot, pair) in out.iter_mut().zip(compact.as_bytes().chunks_exact(2)) {
        *slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    if out[6] >> 4 != 7 || out[8] >> 6 != 2 {
        return None;
    }
    Some(out)
}
fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// Fixed safe operator runtime setup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorRuntimeError;
impl fmt::Display for OperatorRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operator driver configuration or runtime failed closed")
    }
}
impl std::error::Error for OperatorRuntimeError {}
