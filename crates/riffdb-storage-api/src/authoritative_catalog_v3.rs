//! Native projection replay-source authority inventory (ADR-0316).
//!
//! This closed successor does not rewrite V1 or V2 declarations or interpret
//! an old catalog as permission to install a new namespace. Native activation
//! and the exact offline writer upgrade select the new inventory separately.

use sha2::{Digest, Sha256};
use std::sync::OnceLock;

use crate::{AuthoritativeNamespaceV1, AuthoritativeNamespaceV2, ReplicationAuthorityClassV1};

/// Closed physical domains. Existing domains retain their original semantic
/// owner rather than duplicating its tags, table names or authority classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeNamespaceV3 {
    /// An unchanged predecessor domain, including source-only primary admission.
    Existing(AuthoritativeNamespaceV2),
    /// Canonical independently evaluated replay checkpoint rows.
    ProjectionSourceRows,
    /// Immutable source-object binding and complete row-root metadata.
    ProjectionSourceObjects,
    /// The selected published source object for one exact projection identity.
    ProjectionSourceRoots,
    /// Bounded coordinator-owned source copy, abort and collection progress.
    ProjectionSourceMaintenance,
    /// Rebuildable local generation-to-source binding, excluded from receipts.
    GenerationSourceBases,
    /// Coordinator-only non-reusing ordinal allocator for the current incarnation.
    ProjectionSourceAllocator,
}

impl From<AuthoritativeNamespaceV1> for AuthoritativeNamespaceV3 {
    fn from(value: AuthoritativeNamespaceV1) -> Self {
        Self::Existing(AuthoritativeNamespaceV2::Existing(value))
    }
}

impl From<AuthoritativeNamespaceV2> for AuthoritativeNamespaceV3 {
    fn from(value: AuthoritativeNamespaceV2) -> Self {
        Self::Existing(value)
    }
}

impl PartialOrd for AuthoritativeNamespaceV3 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AuthoritativeNamespaceV3 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tag().cmp(&other.tag())
    }
}

impl AuthoritativeNamespaceV3 {
    /// Closed namespace tag, independent of record-family and journal tags.
    #[must_use]
    pub const fn tag(self) -> u16 {
        match self {
            Self::Existing(namespace) => namespace.tag(),
            Self::ProjectionSourceRows => 44,
            Self::ProjectionSourceObjects => 45,
            Self::ProjectionSourceRoots => 46,
            Self::ProjectionSourceMaintenance => 47,
            Self::GenerationSourceBases => 48,
            Self::ProjectionSourceAllocator => 114,
        }
    }

    /// Exact physical table; an unknown mixed-table key never inherits a class.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::Existing(namespace) => namespace.table(),
            Self::ProjectionSourceRows => "projection_source_rows/v1",
            Self::ProjectionSourceObjects => "projection_source_objects/v1",
            Self::ProjectionSourceRoots => "projection_source_roots/v1",
            Self::ProjectionSourceMaintenance => "projection_source_maintenance/v1",
            Self::GenerationSourceBases => "generation_source_bases/v1",
            Self::ProjectionSourceAllocator => "meta",
        }
    }

    /// Exact singleton metadata key, never a caller-selected prefix or wildcard.
    #[must_use]
    pub const fn metadata_key(self) -> Option<&'static str> {
        match self {
            Self::Existing(namespace) => namespace.metadata_key(),
            Self::ProjectionSourceAllocator => Some("projection_checkpoint_ordinal/v1"),
            Self::ProjectionSourceRows
            | Self::ProjectionSourceObjects
            | Self::ProjectionSourceRoots
            | Self::ProjectionSourceMaintenance
            | Self::GenerationSourceBases => None,
        }
    }

    /// Fixed authority classification. A derived generation base cannot become
    /// a replicated mutation or certify its own checkpoint's authority.
    #[must_use]
    pub const fn class(self) -> ReplicationAuthorityClassV1 {
        match self {
            Self::Existing(namespace) => namespace.class(),
            Self::GenerationSourceBases => ReplicationAuthorityClassV1::RebuildableLocal,
            Self::ProjectionSourceRows
            | Self::ProjectionSourceObjects
            | Self::ProjectionSourceRoots
            | Self::ProjectionSourceMaintenance
            | Self::ProjectionSourceAllocator => {
                ReplicationAuthorityClassV1::ReplicatedAuthoritative
            }
        }
    }
}

/// The distinct closed successor inventory. Construction alone grants no
/// permission to read old storage with a newer writer or to mutate a catalog.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AuthoritativeStateCatalogV3;

impl AuthoritativeStateCatalogV3 {
    /// Distinct inventory binding; frozen V1/V2 receipts retain their digests.
    pub const IDENTITY: &'static str = "riffdb.authoritative-state-catalog/v3";

    /// Every namespace in ascending tag order without a caller inventory.
    pub fn namespaces(self) -> impl Iterator<Item = AuthoritativeNamespaceV3> {
        use AuthoritativeNamespaceV3 as N;
        // V1 is frozen: 43 table domains, 13 exact metadata domains and seven
        // replication-control domains. V2 adds tag 208 without changing them.
        AuthoritativeNamespaceV1::ALL
            .into_iter()
            .take(43)
            .map(|n| N::Existing(AuthoritativeNamespaceV2::Existing(n)))
            .chain([
                N::ProjectionSourceRows,
                N::ProjectionSourceObjects,
                N::ProjectionSourceRoots,
                N::ProjectionSourceMaintenance,
                N::GenerationSourceBases,
            ])
            .chain(
                AuthoritativeNamespaceV1::ALL
                    .into_iter()
                    .skip(43)
                    .take(13)
                    .map(|n| N::Existing(AuthoritativeNamespaceV2::Existing(n))),
            )
            .chain([N::ProjectionSourceAllocator])
            .chain(
                AuthoritativeNamespaceV1::ALL
                    .into_iter()
                    .skip(56)
                    .map(|n| N::Existing(AuthoritativeNamespaceV2::Existing(n))),
            )
            .chain([N::Existing(
                AuthoritativeNamespaceV2::ReplicationPrimaryAdmission,
            )])
    }

    /// Resolve only an exact known namespace tag.
    #[must_use]
    pub fn by_tag(self, tag: u16) -> Option<AuthoritativeNamespaceV3> {
        self.namespaces().find(|n| n.tag() == tag)
    }

    /// Classify only a closed table domain or an exact mixed-table singleton.
    /// This fixed 70-element walk allocates nothing and retains no caller data.
    #[must_use]
    pub fn lookup(self, table: &str, key: &[u8]) -> Option<AuthoritativeNamespaceV3> {
        self.namespaces().find(|n| {
            n.table() == table
                && n.metadata_key()
                    .is_none_or(|expected| expected.as_bytes() == key)
        })
    }

    /// Canonical bounded inventory; old domain lines come from their sole owner.
    #[must_use]
    pub fn canonical_fixture(self) -> String {
        let mut result = format!("{}\n", Self::IDENTITY);
        for namespace in self.namespaces() {
            if let AuthoritativeNamespaceV3::Existing(existing) = namespace {
                existing.append_canonical_fixture_line(&mut result);
            } else {
                let class = if namespace == AuthoritativeNamespaceV3::GenerationSourceBases {
                    "rebuildable-local"
                } else {
                    "replicated-authoritative"
                };
                result.push_str(&format!(
                    "{}\t{}\t{}\t{}\tprojection-source-upgrade\n",
                    namespace.tag(),
                    namespace.table(),
                    namespace.metadata_key().unwrap_or("-"),
                    class,
                ));
            }
        }
        result
    }

    /// SHA-256 over the distinct canonical closed inventory. This caches only
    /// compile-time metadata, never freshly read runtime authority evidence.
    #[must_use]
    pub fn digest(self) -> [u8; 32] {
        static DIGEST: OnceLock<[u8; 32]> = OnceLock::new();
        *DIGEST.get_or_init(|| Sha256::digest(self.canonical_fixture().as_bytes()).into())
    }
}

// Append to the single authoritative_catalog_v3 namespace owner. These are
// exact predecessor-domain delegation, never a fallback for new authority.
impl AuthoritativeNamespaceV3 {
    /// Exact unchanged V1 owner, absent for every new or source-admission domain.
    #[must_use]
    pub const fn as_v1(self) -> Option<AuthoritativeNamespaceV1> {
        match self {
            Self::Existing(AuthoritativeNamespaceV2::Existing(namespace)) => Some(namespace),
            _ => None,
        }
    }

    /// Frozen retired-locator classification; source tables are never locators.
    #[must_use]
    pub const fn is_retired_locator_table(self) -> bool {
        match self.as_v1() {
            Some(namespace) => namespace.is_retired_locator_table(),
            None => false,
        }
    }
}

impl PartialEq<AuthoritativeNamespaceV1> for AuthoritativeNamespaceV3 {
    fn eq(&self, other: &AuthoritativeNamespaceV1) -> bool {
        self.as_v1() == Some(*other)
    }
}

impl PartialEq<AuthoritativeNamespaceV3> for AuthoritativeNamespaceV1 {
    fn eq(&self, other: &AuthoritativeNamespaceV3) -> bool {
        other == self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2};
    use std::collections::BTreeSet;

    // req: STO-012, REP-001, PRJ-004
    #[test]
    fn successor_retains_every_predecessor_domain_without_duplicate_tags() {
        assert_eq!(AuthoritativeNamespaceV1::ALL.len(), 63);
        let all: Vec<_> = AuthoritativeStateCatalogV3.namespaces().collect();
        assert_eq!(all.len(), 70);
        assert!(all.windows(2).all(|pair| pair[0].tag() < pair[1].tag()));
        let mut reversed = all.clone();
        reversed.reverse();
        reversed.sort();
        assert_eq!(
            reversed, all,
            "receipt ordering follows frozen numeric tags"
        );
        assert_eq!(
            all.iter().map(|n| n.tag()).collect::<BTreeSet<_>>().len(),
            70
        );
        for old in AuthoritativeStateCatalogV2.namespaces() {
            let new = AuthoritativeStateCatalogV3.by_tag(old.tag()).unwrap();
            assert_eq!(new, AuthoritativeNamespaceV3::Existing(old));
            assert_eq!(new.table(), old.table());
            assert_eq!(new.metadata_key(), old.metadata_key());
            assert_eq!(new.class(), old.class());
            let key = old.metadata_key().unwrap_or("synthetic-row").as_bytes();
            assert_eq!(
                AuthoritativeStateCatalogV3.lookup(old.table(), key),
                Some(new)
            );
        }
        let canonical = AuthoritativeStateCatalogV3.canonical_fixture();
        for old in AuthoritativeStateCatalogV2
            .canonical_fixture()
            .lines()
            .skip(1)
        {
            assert!(canonical.lines().any(|line| line == old));
        }
        assert_ne!(
            AuthoritativeStateCatalogV3.digest(),
            AuthoritativeStateCatalogV1.digest()
        );
        assert_ne!(
            AuthoritativeStateCatalogV3.digest(),
            AuthoritativeStateCatalogV2.digest()
        );
    }

    // req: STO-012, REP-001, PRJ-004
    #[test]
    fn only_source_authority_transfers_and_unknown_keys_refuse() {
        use AuthoritativeNamespaceV3 as N;
        for namespace in [
            N::ProjectionSourceRows,
            N::ProjectionSourceObjects,
            N::ProjectionSourceRoots,
            N::ProjectionSourceMaintenance,
            N::ProjectionSourceAllocator,
        ] {
            assert_eq!(
                namespace.class(),
                ReplicationAuthorityClassV1::ReplicatedAuthoritative
            );
        }
        assert_eq!(
            N::GenerationSourceBases.class(),
            ReplicationAuthorityClassV1::RebuildableLocal
        );
        assert_eq!(
            AuthoritativeStateCatalogV3.lookup("meta", b"projection_checkpoint_ordinal/v1"),
            Some(N::ProjectionSourceAllocator)
        );
        for key in [
            b"projection_checkpoint_ordinal/v2".as_slice(),
            b"projection_checkpoint_ordinal/v1/suffix",
            b"projection_checkpoint_ordinal/",
        ] {
            assert_eq!(AuthoritativeStateCatalogV3.lookup("meta", key), None);
        }
        assert_eq!(AuthoritativeStateCatalogV3.by_tag(49), None);
        assert_eq!(
            AuthoritativeStateCatalogV3.lookup("projection_source_rows/v2", b"synthetic-row"),
            None
        );
        assert_eq!(
            AuthoritativeStateCatalogV2.lookup(N::ProjectionSourceRows.table(), b"synthetic-row"),
            None
        );
        assert_eq!(AuthoritativeStateCatalogV1.by_tag(44), None);
    }
}
