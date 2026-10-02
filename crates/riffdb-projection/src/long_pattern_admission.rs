//! Admission before private long-pattern row mutation and normalization.
// req: OQ-040
use super::{LongPatternPartitionV1, LongPatternProviderErrorV1, LongPatternRowV1};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget, Reservation,
};
use riffdb_types::{
    EntityKeyHash, LongPatternBoundsV1, LongPatternProfileV1,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1, UNICODE_FOLD_V1_MAXIMUM_EXPANSION,
};
use std::collections::BTreeSet;
use std::mem::align_of;
use std::sync::Arc;

/// Closed distinction between aggregate admission and ordinary provider validation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongPatternAdmissionErrorV1 {
    /// Retriable graph capacity or inspection refusal.
    CapacityUnavailable,
    /// Unchanged ordinary row/partition validation failure.
    Provider(LongPatternProviderErrorV1),
}
impl From<BudgetUnavailable> for LongPatternAdmissionErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::CapacityUnavailable
    }
}
impl std::fmt::Display for LongPatternAdmissionErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityUnavailable => {
                f.write_str("long-pattern construction admission unavailable")
            }
            Self::Provider(error) => write!(f, "long-pattern validation unavailable: {error:?}"),
        }
    }
}
impl std::error::Error for LongPatternAdmissionErrorV1 {}

/// Private unpublished construction. Data is destroyed before its admission on
/// cancellation, and all row reservations precede ordinary mutation.
#[doc(hidden)]
pub struct AdmittedLongPatternBuilderV1 {
    provider: LongPatternPartitionV1,
    budget: ProviderBudget,
    // Last: all partial rows and postings drop before work releases.
    work: Reservation,
}
impl AdmittedLongPatternBuilderV1 {
    /// Reserve root and census/settlement scratch before creating private state.
    pub fn new(
        profile: LongPatternProfileV1,
        bounds: LongPatternBoundsV1,
        budget: &ProviderBudget,
    ) -> Result<Self, LongPatternAdmissionErrorV1> {
        let work = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 4096)?;
        Ok(Self {
            provider: LongPatternPartitionV1::new(profile, bounds),
            budget: budget.clone(),
            work,
        })
    }
    /// Admit source transformation, grams, release copy and posting growth before
    /// invoking the ordinary checked private upsert. Failed mutation never frees
    /// admission that still protects an earlier row.
    pub fn upsert(
        &mut self,
        key: EntityKeyHash,
        source: &str,
        release: &[u8],
    ) -> Result<(), LongPatternAdmissionErrorV1> {
        if source.len() > self.provider.bounds().source_bytes() as usize
            || release.len() > super::MAX_LONG_PATTERN_RELEASE_BYTES_PER_ROW_V1
        {
            return Err(LongPatternAdmissionErrorV1::Provider(
                LongPatternProviderErrorV1::RowBound,
            ));
        }
        let maximum = row_maximum(self.provider.profile(), source.len(), release.len())
            .map_err(|_| BudgetUnavailable)?;
        let row = self.budget.reserve(maximum)?;
        self.work.absorb(row)?;
        self.provider
            .upsert(key, source, release.to_vec())
            .map_err(LongPatternAdmissionErrorV1::Provider)
    }
    /// Settle one complete immutable provider; captures retain its actual charge.
    pub fn finish(
        self,
    ) -> Result<Arc<ProviderAllocation<LongPatternPartitionV1>>, LongPatternAdmissionErrorV1> {
        self.work
            .settle_owned_value(self.provider)
            .map_err(Into::into)
    }
}
fn row_maximum(
    profile: LongPatternProfileV1,
    source: usize,
    release: usize,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let matched = match profile {
        LongPatternProfileV1::BinaryUtf8V1 => source,
        LongPatternProfileV1::UnicodeFoldV1 => source
            .checked_mul(UNICODE_FOLD_V1_MAXIMUM_EXPANSION)
            .ok_or(OwnedCapacityInspectionErrorV1)?,
    };
    let grams = matched.saturating_sub(2);
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.prospective_btree::<EntityKeyHash, LongPatternRowV1>(1)?;
        inspection.prospective_btree::<[u8; 3], BTreeSet<EntityKeyHash>>(grams)?;
        // Every gram can own an independent posting-set root, in
        // addition to its member node and allocator metadata.
        inspection.prospective_btree::<EntityKeyHash, ()>(
            grams.checked_mul(2).ok_or(OwnedCapacityInspectionErrorV1)?,
        )?;
        inspection.allocation(
            grams
                .checked_mul(512)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<usize>(),
        )?;
        // Generated grams are retained before deduplication, and normalization
        // owns independent decomposition, composition, sort and String buffers.
        let transform = if profile == LongPatternProfileV1::UnicodeFoldV1 {
            256
        } else {
            2
        };
        inspection.allocation(
            matched
                .checked_mul(transform)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<usize>(),
        )?;
        inspection.allocation(
            grams.checked_mul(6).ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<[u8; 3]>(),
        )?;
        inspection.allocation(
            release
                .checked_mul(2)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<u8>(),
        )?;
        Ok(())
    })?;
    report
        .unshared_bytes()
        .checked_add(4096)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

impl LongPatternPartitionV1 {
    /// Admit borrowed checkpoint capacity before ordinary digest/gram validation
    /// and posting reconstruction. Stored matched bytes are not transformed.
    #[doc(hidden)]
    pub fn from_checkpoint_bytes_admitted_v1(
        bytes: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<Self>>, LongPatternAdmissionErrorV1> {
        if bytes.len() > super::MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1
            || !bytes.starts_with(super::MAGIC)
        {
            return Err(LongPatternAdmissionErrorV1::Provider(
                LongPatternProviderErrorV1::CheckpointInvalid,
            ));
        }
        let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
        let maximum = recovery_maximum(bytes).map_err(|_| BudgetUnavailable)?;
        drop(scratch);
        let work = budget.reserve(maximum)?;
        let provider =
            Self::from_checkpoint_bytes(bytes).map_err(LongPatternAdmissionErrorV1::Provider)?;
        work.settle_owned_value(provider).map_err(Into::into)
    }
}
fn recovery_maximum(bytes: &[u8]) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        let mut reader = super::Reader::new(&bytes[super::MAGIC.len()..]);
        reader.u8().map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let bounds = super::read_bounds(&mut reader).map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let count = usize::try_from(reader.u32().map_err(|_| OwnedCapacityInspectionErrorV1)?)
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        if count > bounds.rows() as usize {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        inspection.prospective_btree::<EntityKeyHash, LongPatternRowV1>(count)?;
        for _ in 0..count {
            reader
                .array::<32>()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let matched = reader.bytes().map_err(|_| OwnedCapacityInspectionErrorV1)?;
            reader
                .array::<32>()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let grams = usize::try_from(reader.u32().map_err(|_| OwnedCapacityInspectionErrorV1)?)
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let gram_bytes = grams.checked_mul(3).ok_or(OwnedCapacityInspectionErrorV1)?;
            let end = reader
                .offset
                .checked_add(gram_bytes)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            reader
                .bytes
                .get(reader.offset..end)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            reader.offset = end;
            let release = reader.bytes().map_err(|_| OwnedCapacityInspectionErrorV1)?;
            if matched.len() > bounds.matched_bytes() as usize
                || release.len() > super::MAX_LONG_PATTERN_RELEASE_BYTES_PER_ROW_V1
            {
                return Err(OwnedCapacityInspectionErrorV1);
            }
            inspection.allocation(
                matched
                    .len()
                    .checked_mul(2)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<u8>(),
            )?;
            inspection.allocation(
                release
                    .len()
                    .checked_mul(2)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<u8>(),
            )?;
            // The stored gram vector and a separately regenerated pre-dedup
            // vector coexist during ordinary validation. Admit both capacities.
            let generated = matched
                .len()
                .saturating_sub(2)
                .checked_mul(6)
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(
                gram_bytes
                    .checked_add(generated)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<[u8; 3]>(),
            )?;
            inspection.prospective_btree::<[u8; 3], BTreeSet<EntityKeyHash>>(grams)?;
            // Every gram can own an independent posting-set root, in
            // addition to its member node and allocator metadata.
            inspection.prospective_btree::<EntityKeyHash, ()>(
                grams.checked_mul(2).ok_or(OwnedCapacityInspectionErrorV1)?,
            )?;
            inspection.allocation(
                grams
                    .checked_mul(512)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<usize>(),
            )?;
        }
        if !reader.done() {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        inspection.allocation(
            4096,
            align_of::<ProviderAllocation<LongPatternPartitionV1>>(),
        )?;
        Ok(())
    })?;
    report
        .unshared_bytes()
        .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    use std::sync::Arc;
    fn bounds() -> LongPatternBoundsV1 {
        LongPatternBoundsV1::new(
            8000, 144000, 8, 1000000, 10000, 50000, 2000000, 10000, 8000, 144000, 8000, 8, 1000000,
            8,
        )
        .unwrap()
    }
    #[test]
    fn admitted_long_pattern_builder_matches_rows_and_final_captures() {
        for profile in [
            LongPatternProfileV1::BinaryUtf8V1,
            LongPatternProfileV1::UnicodeFoldV1,
        ] {
            let budget = ProviderBudget::new();
            let mut expected = LongPatternPartitionV1::new(profile, bounds());
            let mut candidate =
                AdmittedLongPatternBuilderV1::new(profile, bounds(), &budget).unwrap();
            for (id, text) in [
                (1, ""),
                (2, "shared shared"),
                (3, "Straße ﬁle İ Σ"),
                (2, "replacement é日"),
            ] {
                let key = EntityKeyHash::from_bytes([id; 32]);
                expected.upsert(key, text, b"release".to_vec()).unwrap();
                candidate.upsert(key, text, b"release").unwrap();
            }
            let provider = candidate.finish().unwrap();
            assert_eq!(&**provider, &expected);
            assert_eq!(
                provider.checkpoint_bytes().unwrap(),
                expected.checkpoint_bytes().unwrap()
            );
            let encoded = provider.checkpoint_bytes().unwrap();
            let admitted_encoded = provider.to_checkpoint_bytes_admitted_v1(&budget).unwrap();
            assert_eq!(&**admitted_encoded, &encoded);
            drop(admitted_encoded);
            let recovered =
                LongPatternPartitionV1::from_checkpoint_bytes_admitted_v1(&encoded, &budget)
                    .unwrap();
            assert_eq!(&**recovered, &expected);
            drop(recovered);
            let capture = Arc::clone(&provider);
            drop(provider);
            assert!(budget.observation().unwrap().owned_bytes > 0);
            drop(capture);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        }
    }
    #[test]
    fn admitted_long_pattern_partial_refusal_and_cancellation_release_only_work() {
        let budget = ProviderBudget::new();
        let mut candidate = AdmittedLongPatternBuilderV1::new(
            LongPatternProfileV1::UnicodeFoldV1,
            bounds(),
            &budget,
        )
        .unwrap();
        candidate
            .upsert(EntityKeyHash::from_bytes([1; 32]), "retained é", b"release")
            .unwrap();
        let before = candidate.provider.checkpoint_bytes().unwrap();
        let observation = budget.observation().unwrap();
        let available = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
            - observation.fixed_owner_bytes
            - observation.reserved_bytes;
        let occupied = budget.reserve(available).unwrap();
        assert!(matches!(
            candidate.upsert(EntityKeyHash::from_bytes([2; 32]), "refused", b"other"),
            Err(LongPatternAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(candidate.provider.checkpoint_bytes().unwrap(), before);
        assert_eq!(budget.observation().unwrap().reservations, 2);
        drop(occupied);
        let reserved = budget.observation().unwrap().reserved_bytes;
        assert_eq!(
            candidate.upsert(
                EntityKeyHash::from_bytes([3; 32]),
                &"x".repeat(8001),
                b"other"
            ),
            Err(LongPatternAdmissionErrorV1::Provider(
                LongPatternProviderErrorV1::RowBound
            ))
        );
        assert_eq!(candidate.provider.checkpoint_bytes().unwrap(), before);
        assert_eq!(budget.observation().unwrap().reserved_bytes, reserved);
        drop(candidate);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    #[test]
    fn admitted_long_pattern_recovery_refuses_corruption_counts_and_pressure() {
        let budget = ProviderBudget::new();
        let mut original =
            LongPatternPartitionV1::new(LongPatternProfileV1::BinaryUtf8V1, bounds());
        original
            .upsert(
                EntityKeyHash::from_bytes([1; 32]),
                "abcdef",
                b"release".to_vec(),
            )
            .unwrap();
        let encoded = original.checkpoint_bytes().unwrap();
        let mut reader = super::super::Reader::new(&encoded[super::super::MAGIC.len()..]);
        reader.u8().unwrap();
        super::super::read_bounds(&mut reader).unwrap();
        reader.u32().unwrap();
        reader.array::<32>().unwrap();
        reader.bytes().unwrap();
        let digest_offset = super::super::MAGIC.len() + reader.offset;
        let mut digest = encoded.clone();
        digest[digest_offset] ^= 1;
        let mut trailing = encoded.clone();
        trailing.push(0);
        for corrupt in [
            &encoded[..encoded.len() - 1],
            digest.as_slice(),
            trailing.as_slice(),
        ] {
            assert!(LongPatternPartitionV1::from_checkpoint_bytes(corrupt).is_err());
            assert!(
                LongPatternPartitionV1::from_checkpoint_bytes_admitted_v1(corrupt, &budget)
                    .is_err()
            );
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        }
        // The existing decoder allocates its declared gram vector before
        // semantic validation. Borrowed preflight must refuse an impossible
        // declaration before that allocation is attempted.
        let mut oversized = encoded.clone();
        oversized[digest_offset + 32..digest_offset + 36].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            LongPatternPartitionV1::from_checkpoint_bytes_admitted_v1(&oversized, &budget).is_err()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        let available = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
            - budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget
            .reserve(available - riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1)
            .unwrap();
        assert!(matches!(
            LongPatternPartitionV1::from_checkpoint_bytes_admitted_v1(&encoded, &budget),
            Err(LongPatternAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reservations, 1);
        drop(occupied);
        let recovered =
            LongPatternPartitionV1::from_checkpoint_bytes_admitted_v1(&encoded, &budget).unwrap();
        assert_eq!(&**recovered, &original);
        drop(recovered);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}
