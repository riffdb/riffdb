//! Accepted primary-fencing inventory successor (WP-748).
//!
//! Inventory is descriptive evidence, not permission to open or upgrade storage.
//! Production activation still requires the admission record, registry, startup
//! validation and complete fencing transaction. V1 remains frozen and separate.

use sha2::{Digest, Sha256};

use crate::{
    AuthoritativeNamespaceV1, AuthoritativeStateCatalogV1, ReplicationAuthorityClassV1,
    ReplicationTransferV1,
};

/// Existing domains retain their sole semantic owner; only the new source-only
/// domain is declared here. This is not a replicated mutation namespace selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeNamespaceV2 {
    /// An unchanged V1 domain, with identical tag, key and authority class.
    Existing(AuthoritativeNamespaceV1),
    /// Required source-mode admission state; absent from attached followers.
    ReplicationPrimaryAdmission,
}

impl AuthoritativeNamespaceV2 {
    /// Closed namespace tag, independent of the durable record registry.
    #[must_use]
    pub const fn tag(self) -> u16 {
        match self {
            Self::Existing(namespace) => namespace.tag(),
            Self::ReplicationPrimaryAdmission => 208,
        }
    }

    /// Exact physical table name.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::Existing(namespace) => namespace.table(),
            Self::ReplicationPrimaryAdmission => "meta",
        }
    }

    /// Exact singleton key; unknown mixed-table keys never inherit a class.
    #[must_use]
    pub const fn metadata_key(self) -> Option<&'static str> {
        match self {
            Self::Existing(namespace) => namespace.metadata_key(),
            Self::ReplicationPrimaryAdmission => Some("replication_primary_admission/v1"),
        }
    }

    /// Fixed transfer behavior; admission state cannot enter V3 mutations.
    #[must_use]
    pub const fn class(self) -> ReplicationAuthorityClassV1 {
        match self {
            Self::Existing(namespace) => namespace.class(),
            Self::ReplicationPrimaryAdmission => {
                ReplicationAuthorityClassV1::ReplicationControl(ReplicationTransferV1::SourceOnly)
            }
        }
    }
}

/// Closed successor inventory. Construction neither selects an active catalog
/// nor infers missing admission state in an existing database.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AuthoritativeStateCatalogV2;

impl AuthoritativeStateCatalogV2 {
    /// Distinct binding; old receipts retain their original V1 catalog identity.
    pub const IDENTITY: &'static str = "riffdb.authoritative-state-catalog/v2";

    /// Every domain in ascending tag order, without an alternate caller inventory.
    pub fn namespaces(self) -> impl Iterator<Item = AuthoritativeNamespaceV2> {
        AuthoritativeNamespaceV1::ALL
            .into_iter()
            .map(AuthoritativeNamespaceV2::Existing)
            .chain(std::iter::once(
                AuthoritativeNamespaceV2::ReplicationPrimaryAdmission,
            ))
    }

    /// Resolves only an exact closed tag.
    #[must_use]
    pub fn by_tag(self, tag: u16) -> Option<AuthoritativeNamespaceV2> {
        self.namespaces().find(|namespace| namespace.tag() == tag)
    }

    /// Bounded, allocation-free classification of exact physical domains.
    #[must_use]
    pub fn lookup(self, table: &str, key: &[u8]) -> Option<AuthoritativeNamespaceV2> {
        self.namespaces().find(|namespace| {
            namespace.table() == table
                && namespace
                    .metadata_key()
                    .is_none_or(|expected| expected.as_bytes() == key)
        })
    }

    /// Canonical bounded inventory. Reuse V1's owner for its exact domain lines.
    #[must_use]
    pub fn canonical_fixture(self) -> String {
        let old = AuthoritativeStateCatalogV1.canonical_fixture();
        let mut result = format!("{}\n", Self::IDENTITY);
        for line in old.lines().skip(1) {
            result.push_str(line);
            result.push('\n');
        }
        let admission = AuthoritativeNamespaceV2::ReplicationPrimaryAdmission;
        result.push_str(&format!(
            "{}\t{}\t{}\treplication-control:source-only\tfencing-activation\n",
            admission.tag(),
            admission.table(),
            admission.metadata_key().unwrap_or("-"),
        ));
        result
    }

    /// SHA-256 of this distinct canonical inventory, including its identity.
    #[must_use]
    pub fn digest(self) -> [u8; 32] {
        Sha256::digest(self.canonical_fixture().as_bytes()).into()
    }
}
