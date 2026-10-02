//! Admission includes encoding scratch and the final encoded allocation.
// req: OQ-040, PRJ-004

use crate::{
    ExactPredicateAdmissionErrorV1, ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5,
    ExactTextPartitionIndexV2, ExactTextPartitionIndexV3, ExactTextRebuildAdmissionErrorV1,
    LongPatternAdmissionErrorV1, LongPatternPartitionV1, TokenizedTextAdmissionErrorV1,
    TokenizedTextPartitionIndexV1,
};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1};
use std::sync::Arc;

macro_rules! admitted_encoder {
    ($provider:ty, $encode:ident, $error:ty) => {
        impl $provider {
            /// First-party encoding admission; exact ordinary bytes are preserved.
            /// The outer activation frame requires its own admitted buffer.
            #[doc(hidden)]
            pub fn to_checkpoint_bytes_admitted_v1(
                &self,
                budget: &ProviderBudget,
            ) -> Result<Arc<ProviderAllocation<Vec<u8>>>, $error> {
                let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
                let report =
                    OwnedCapacityInspectionV1::inspect(self).map_err(|_| BudgetUnavailable)?;
                if !report.shared_allocations().is_empty() {
                    return Err(BudgetUnavailable.into());
                }
                // These closed encoders serialize owned rows and postings. The
                // capacity inventory admits copied canonical outputs, temporary
                // scalar/gram buffers and geometrically grown wire output.
                // A fixed allowance covers empty-state headers and ownership.
                let maximum = report
                    .unshared_bytes()
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(32768))
                    .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
                    .ok_or(BudgetUnavailable)?;
                drop(report);
                drop(scratch);
                let work = budget.reserve(maximum)?;
                let encoded = self.$encode().map_err(<$error>::Provider)?;
                work.settle_owned_value(encoded).map_err(Into::into)
            }
        }
    };
}
admitted_encoder!(
    ExactTextPartitionIndexV2,
    to_checkpoint_bytes,
    ExactTextRebuildAdmissionErrorV1
);
admitted_encoder!(
    ExactTextPartitionIndexV3,
    to_checkpoint_bytes,
    ExactTextRebuildAdmissionErrorV1
);
admitted_encoder!(
    ExactPredicatePartitionIndexV4,
    to_checkpoint_bytes,
    ExactPredicateAdmissionErrorV1
);
admitted_encoder!(
    ExactPredicatePartitionIndexV5,
    to_checkpoint_bytes,
    ExactPredicateAdmissionErrorV1
);
admitted_encoder!(
    TokenizedTextPartitionIndexV1,
    to_checkpoint_bytes,
    TokenizedTextAdmissionErrorV1
);
admitted_encoder!(
    LongPatternPartitionV1,
    checkpoint_bytes,
    LongPatternAdmissionErrorV1
);

#[cfg(test)]
mod tests {
    use crate::{
        ExactTextIndexMutationV2, ExactTextPartitionIndexV2, ExactTextRebuildAdmissionErrorV1,
    };
    use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};
    use riffdb_types::{
        CanonicalRecord, CanonicalValue, CommitSequence, EntityKeyBuilder, EntityTypeId,
        ExactTextProfileV1, FieldId, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, PartitionKeyHash,
        ProjectionGeneration,
    };

    #[test]
    fn encoded_provider_bytes_are_admitted_and_charged_until_final_capture() {
        let mut provider = ExactTextPartitionIndexV2::new(
            PartitionKeyHash::from_bytes([1; 32]),
            ProjectionGeneration::new(1).unwrap(),
            ExactTextProfileV1::BinaryUtf8V1,
        );
        let mut key = EntityKeyBuilder::new(EntityTypeId::first());
        key.push_u32(1).unwrap();
        provider
            .apply(
                CommitSequence::new(1).unwrap(),
                &[ExactTextIndexMutationV2::upsert(
                    key.finish().unwrap(),
                    "é repeated example",
                    CanonicalRecord::new(vec![(
                        FieldId::new(1).unwrap(),
                        CanonicalValue::Bytes(
                            riffdb_types::CanonicalBytes::new(vec![3; 16384]).unwrap(),
                        ),
                    )])
                    .unwrap(),
                )
                .unwrap()],
            )
            .unwrap();
        let ordinary = provider.to_checkpoint_bytes().unwrap();
        let budget = ProviderBudget::new();
        let blocker = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - 128 - OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        assert!(matches!(
            provider.to_checkpoint_bytes_admitted_v1(&budget),
            Err(ExactTextRebuildAdmissionErrorV1::CapacityUnavailable)
        ));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        drop(blocker);
        let encoded = provider.to_checkpoint_bytes_admitted_v1(&budget).unwrap();
        assert_eq!(&**encoded, &ordinary);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            encoded.owned_bytes()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let capture = encoded.clone();
        drop(encoded);
        assert!(budget.observation().unwrap().owned_bytes >= ordinary.len() as u64);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(provider.to_checkpoint_bytes().unwrap(), ordinary);
    }
}
