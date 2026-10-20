//! Definite-before reconciliation from the actual raw transaction owner.
//! Receipt-slot absence is insufficient after an ordinary writer can advance;
//! this consuming proof exists only after successful abort under the same lease.
use super::*;
use crate::store::source_usage_domain::NativeSourceNoSubmission;
use riffdb_storage_api::StorageError;

pub(in crate::source_checkpoint_adapter) fn verified_before<'turn, 'work: 'turn>(
    proof: NativeSourceNoSubmission<'turn, 'work>,
) -> Result<NativeVerifiedSourceStep<'turn>, StorageError> {
    proof.require_current()?;
    let actual = proof.actual_ports();
    let lease = proof.source_lease();
    let mutation = proof.mutation();
    let intent = proof.intent().clone();
    // Same actual mutation/Source lease and same admitted intent. Consuming the
    // proof cannot turn abort failure or physical submission into ExactBefore.
    let verified = NativeVerifiedSourceStep {
        actual,
        lease,
        mutation,
        intent,
        image: VerifiedSourceImage::ExactBefore,
    };
    drop(proof);
    Ok(verified)
}
