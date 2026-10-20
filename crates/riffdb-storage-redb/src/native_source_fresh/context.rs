//! Exact engine-owned fixed evidence for an already validated native control head.
//! Only the enclosing actual Fresh/cut/cold owner may prepare or install it.
use std::sync::OnceLock;

use crate::engine_owner::EngineOwner;
use riffdb_storage_api::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    NativeCatalogOriginReferenceV1, StorageError, StorageErrorKind,
};
use riffdb_types::{AdministrationSequence, ProjectionSourceWorkReservationV1};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ControlRoleV1 {
    CatalogOrigin,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct LineageStampsV1 {
    catalog: [u8; 32],
    incarnation: [u8; 32],
    leadership: [u8; 32],
}
impl LineageStampsV1 {
    // The enclosing owner supplies exact checked postimages. These hashes never
    // stand in for the full cold/offline canonical and lineage validation.
    pub(super) fn from_checked_postimages(
        catalog: &[u8],
        incarnation: &[u8],
        leadership: &[u8],
    ) -> Result<Self, StorageError> {
        if [catalog, incarnation, leadership]
            .iter()
            .any(|bytes| bytes.is_empty() || bytes.len() > 512)
        {
            return Err(corrupt());
        }
        Ok(Self {
            catalog: digest(catalog),
            incarnation: digest(incarnation),
            leadership: digest(leadership),
        })
    }
    // Fixed digests come only from the closed same-pin four-visit observer.
    pub(super) fn from_observed_digests(
        catalog: [u8; 32],
        incarnation: [u8; 32],
        leadership: [u8; 32],
    ) -> Self {
        Self {
            catalog,
            incarnation,
            leadership,
        }
    }
}

#[derive(Clone, Copy)]
struct ControlHeadV1 {
    administration: AdministrationSequence,
    key: [u8; 9],
    role: ControlRoleV1,
    canonical_artifact: [u8; 32],
}
#[derive(Clone, Copy)]
struct OriginContextV1 {
    lineage: ChangelogLineageV3,
    original_origin: NativeCatalogOriginReferenceV1,
    original_publication_point: ChangelogHistoryPointV3,
    predecessor_cut: Option<ChangelogHistoryStateV3>,
    stamps: LineageStampsV1,
    head: ControlHeadV1,
}
struct AdmittedOriginContextV1 {
    value: OriginContextV1,
    // Fixed context bytes and accountant alias remain admitted until after data.
    _control: ProjectionSourceWorkReservationV1,
}

/// Inline engine control; callers can check exact evidence but cannot assign it.
pub(crate) struct OriginContextCellV1(OnceLock<AdmittedOriginContextV1>);
impl OriginContextCellV1 {
    pub(crate) const fn new() -> Self {
        Self(OnceLock::new())
    }
    pub(crate) fn anchor_key(&self) -> Option<[u8; 9]> {
        self.0.get().map(|context| context.value.head.key)
    }

    pub(super) fn install<T>(
        &self,
        prepared: PreparedOriginContextV1<'_, T>,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self, prepared.owner.native_origin_context()) {
            return Err(corrupt());
        }
        self.0.set(prepared.admitted).map_err(|_| corrupt())
    }

    /// Ordinary authoritative append performs only bounded fixed comparisons.
    /// Missing proof and any mismatch refuse; this does not reserve Source64.
    pub(crate) fn check_control_head(
        &self,
        key: &[u8],
        encoded: &[u8],
        catalog: &[u8],
        incarnation: &[u8],
        leadership: &[u8],
    ) -> Result<AdministrationSequence, StorageError> {
        let context = self.0.get().ok_or_else(corrupt)?;
        let maximum = match context.value.head.role {
            ControlRoleV1::CatalogOrigin => 2048,
        };
        // Preserve the existing borrowed size refusal before any content hash.
        if encoded.is_empty() || encoded.len() > maximum {
            return Err(corrupt());
        }
        let stamps = LineageStampsV1::from_checked_postimages(catalog, incarnation, leadership)?;
        self.check_control_head_observed(key, encoded.len(), digest(encoded), stamps)
    }
    // Both byte-taking hot/cold callers and the bounded same-pin observer use
    // this one exact immutable control checker. It installs no context.
    fn check_control_head_observed(
        &self,
        key: &[u8],
        encoded_len: usize,
        artifact: [u8; 32],
        stamps: LineageStampsV1,
    ) -> Result<AdministrationSequence, StorageError> {
        let context = self.0.get().ok_or_else(corrupt)?;
        let value = context.value;
        let maximum = match value.head.role {
            ControlRoleV1::CatalogOrigin => 2048,
        };
        if key != value.head.key
            || encoded_len == 0
            || encoded_len > maximum
            || artifact != value.head.canonical_artifact
            || stamps != value.stamps
            || value.lineage.catalog_digest()
                != riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
            || value.original_origin.administration_sequence() != value.head.administration
            || value.original_origin.digest().as_bytes() != &value.head.canonical_artifact
            || value.original_publication_point.frontier().administration()
                != Some(value.head.administration)
            || value.predecessor_cut.is_some()
        {
            return Err(corrupt());
        }
        Ok(value.head.administration)
    }
    pub(super) fn checked_observed_origin(
        &self,
        key: &[u8],
        encoded_len: usize,
        artifact: [u8; 32],
        stamps: LineageStampsV1,
    ) -> Result<(NativeCatalogOriginReferenceV1, ChangelogLineageV3), StorageError> {
        self.check_control_head_observed(key, encoded_len, artifact, stamps)?;
        self.0
            .get()
            .map(|context| (context.value.original_origin, context.value.lineage))
            .ok_or_else(corrupt)
    }
    pub(super) fn checked_origin_reference(
        &self,
        key: &[u8],
        encoded: &[u8],
        catalog: &[u8],
        incarnation: &[u8],
        leadership: &[u8],
    ) -> Result<NativeCatalogOriginReferenceV1, StorageError> {
        self.check_control_head(key, encoded, catalog, incarnation, leadership)?;
        self.0
            .get()
            .map(|context| context.value.original_origin)
            .ok_or_else(corrupt)
    }
}

/// Move-only preparation scoped by a borrow of the exact physical owner.
pub(super) struct PreparedOriginContextV1<'engine, T> {
    owner: &'engine EngineOwner<T>,
    admitted: AdmittedOriginContextV1,
}

// This constructor is private to the actual enclosing native source owner. It
// must only be called after full checked origin/control/receipt postimages.
pub(super) struct CheckedOriginPostimagesV1<'bytes> {
    pub(super) lineage: ChangelogLineageV3,
    pub(super) original_origin: NativeCatalogOriginReferenceV1,
    pub(super) anchor: ChangelogHistoryPointV3,
    pub(super) predecessor_cut: Option<ChangelogHistoryStateV3>,
    pub(super) stamps: LineageStampsV1,
    pub(super) administration: AdministrationSequence,
    pub(super) key: [u8; 9],
    pub(super) role: ControlRoleV1,
    pub(super) encoded: &'bytes [u8],
}
pub(super) fn prepare<'engine, T>(
    owner: &'engine EngineOwner<T>,
    postimages: CheckedOriginPostimagesV1<'_>,
) -> Result<PreparedOriginContextV1<'engine, T>, StorageError> {
    let CheckedOriginPostimagesV1 {
        lineage,
        original_origin,
        anchor,
        predecessor_cut,
        stamps,
        administration,
        key,
        role,
        encoded,
    } = postimages;
    let maximum = match role {
        ControlRoleV1::CatalogOrigin => 2048,
    };
    if encoded.is_empty()
        || encoded.len() > maximum
        || anchor.frontier().administration() != Some(administration)
    {
        return Err(corrupt());
    }
    let bytes =
        u64::try_from(std::mem::size_of::<AdmittedOriginContextV1>()).map_err(|_| unavailable())?;
    let control = owner
        .source_working_budget()
        .reserve(bytes)
        .map_err(|_| unavailable())?;
    let value = OriginContextV1 {
        lineage,
        original_origin,
        original_publication_point: anchor,
        predecessor_cut,
        stamps,
        head: ControlHeadV1 {
            administration,
            key,
            role,
            canonical_artifact: digest(encoded),
        },
    };
    Ok(PreparedOriginContextV1 {
        owner,
        admitted: AdmittedOriginContextV1 {
            value,
            _control: control,
        },
    })
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}

// Only the enclosing real native owner can call this after its durable commit.
pub(super) fn install_prepared<T>(
    prepared: PreparedOriginContextV1<'_, T>,
) -> Result<(), StorageError> {
    let cell = prepared.owner.native_origin_context();
    cell.install(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    // req: REP-002, REC-001, STO-012, PRJ-004
    fn concurrent_duplicate_origin_installation_never_replaces_evidence_or_leaks_admission() {
        let root = crate::test_path::ScopedDirectory::new("native-origin-once-schedule");
        let mut store = crate::RedbStore::open(root.join("database.redb")).unwrap();
        let database = riffdb_types::DatabaseId::from_unix_milliseconds_and_random(
            1_700_000_000_000,
            [0x74; 10],
        )
        .unwrap();
        store
            .initialize_projection_source_fixture(database)
            .unwrap();
        let owner = &store.shared.database;
        let cell = owner.native_origin_context();
        // Reuse only actual fully validated Fresh evidence from this exact
        // owner, never manufacture an origin or transferable writer proof.
        let original = cell.0.get().unwrap().value;
        let baseline = owner.source_working_budget().observation().unwrap();
        let prepare_again = || PreparedOriginContextV1 {
            owner,
            admitted: AdmittedOriginContextV1 {
                value: original,
                _control: owner
                    .source_working_budget()
                    .reserve(u64::try_from(std::mem::size_of::<AdmittedOriginContextV1>()).unwrap())
                    .unwrap(),
            },
        };
        let first = prepare_again();
        let second = prepare_again();
        let barrier = Barrier::new(3);
        std::thread::scope(|scope| {
            let barrier = &barrier;
            let a = scope.spawn(move || {
                barrier.wait();
                assert!(cell.install(first).is_err());
            });
            let b = scope.spawn(move || {
                barrier.wait();
                assert!(cell.install(second).is_err());
            });
            barrier.wait();
            assert_eq!(
                cell.0.get().unwrap().value.head.canonical_artifact,
                original.head.canonical_artifact
            );
            a.join().unwrap();
            b.join().unwrap();
        });
        assert_eq!(
            owner.source_working_budget().observation().unwrap(),
            baseline
        );
        let final_evidence = cell.0.get().unwrap().value;
        assert_eq!(final_evidence.lineage, original.lineage);
        assert_eq!(final_evidence.head.key, original.head.key);
        assert_eq!(
            final_evidence.head.canonical_artifact,
            original.head.canonical_artifact
        );
    }
}
