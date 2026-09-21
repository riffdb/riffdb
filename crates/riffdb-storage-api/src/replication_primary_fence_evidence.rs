//! Source evidence is not authenticated peer proof or promotion authority.
use crate::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, StorageValueError,
    StoredPrimaryFenceAdministrationV1,
};

/// Bounded source observation of an exact fence and candidate position.
/// The storage owner must validate retained receipt ancestry independently.
/// Decoding or constructing this value never authenticates its remote origin.
#[derive(Clone, Eq, PartialEq)]
pub struct PrimaryFenceSourceEvidenceV1 {
    fence: StoredPrimaryFenceAdministrationV1,
    applied: ChangelogHistoryPointV3,
    source_history: ChangelogHistoryStateV3,
    application_rpo: u64,
}
impl PrimaryFenceSourceEvidenceV1 {
    /// Encodes only the existing bounded, versioned fence administration record.
    /// This adds no evidence envelope and does not authenticate a remote source.
    pub fn encode_fence_record(&self) -> Result<Vec<u8>, StorageValueError> {
        crate::proto_codec::encode_primary_fence_administration_v1(&self.fence)
            .map(|encoded| encoded.as_bytes().to_vec())
            .map_err(|_| StorageValueError::InvalidShape)
    }

    /// Checks existing canonical fence bytes and the surrounding history values.
    /// The existing record decoder owns size, role, schema and checksum checks.
    /// This does not prove retained ancestry or the source's authenticated origin.
    pub fn from_fence_record(
        encoded: &[u8],
        applied: ChangelogHistoryPointV3,
        anchor: ChangelogHistoryPointV3,
        tail: ChangelogHistoryPointV3,
        minimum_resume: ChangelogHistoryPointV3,
    ) -> Result<Self, StorageValueError> {
        let decoded = crate::proto_codec::decode_primary_fence_administration_v1(encoded)
            .map_err(|_| StorageValueError::InvalidShape)?;
        let fence = decoded.value().clone();
        let history = ChangelogHistoryStateV3::new(fence.lineage(), anchor, tail, minimum_resume)
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        Self::new(fence, applied, history)
    }

    /// Checks consistency and application-sequence arithmetic only. The caller
    /// must separately prove source custody, exact retained receipts, current
    /// registration and authenticated transport before using this in promotion.
    pub fn new(
        fence: StoredPrimaryFenceAdministrationV1,
        applied: ChangelogHistoryPointV3,
        source_history: ChangelogHistoryStateV3,
    ) -> Result<Self, StorageValueError> {
        if fence.lineage() != source_history.lineage()
            || fence.final_application_head() != source_history.tail().frontier().application()
            || !fence.observed().precedes_or_equals(source_history.tail())
            || fence.observed().sequence() >= source_history.tail().sequence()
            || Some(fence.administration_sequence())
                > source_history.tail().frontier().administration()
            || !source_history.minimum_resume().precedes_or_equals(applied)
            || !applied.precedes_or_equals(source_history.tail())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let application_rpo = fence
            .final_application_head()
            .map_or(0, |s| s.get())
            .checked_sub(applied.frontier().application().map_or(0, |s| s.get()))
            .ok_or(StorageValueError::IdentityMismatch)?;
        Ok(Self {
            fence,
            applied,
            source_history,
            application_rpo,
        })
    }
    /// Exact original administration value, not a bearer or transport credential.
    #[must_use]
    pub const fn fence(&self) -> &StoredPrimaryFenceAdministrationV1 {
        &self.fence
    }
    /// Exact candidate applied receipt, including hash and both frontiers.
    #[must_use]
    pub const fn applied(&self) -> ChangelogHistoryPointV3 {
        self.applied
    }
    /// History of the same immutable source publication used for validation.
    #[must_use]
    pub const fn source_history(&self) -> ChangelogHistoryStateV3 {
        self.source_history
    }
    /// Checked application-sequence delta; BeforeFirst contributes zero.
    #[must_use]
    pub const fn application_rpo(&self) -> u64 {
        self.application_rpo
    }
}
impl std::fmt::Debug for PrimaryFenceSourceEvidenceV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrimaryFenceSourceEvidenceV1([redacted])")
    }
}
