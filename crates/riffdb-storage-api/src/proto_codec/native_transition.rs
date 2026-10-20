//! Canonical ADR-0318 facts with caller-admitted source-owned work. These codecs
//! preserve original bytes and never supply native custody or closure authority.

use super::changelog::{admission_from_wire_v1, history_from_wire, history_to_wire};
use super::projection_source::{
    ProjectionSourceRecordDecodeErrorV1, decode_admitted, publication_from_wire,
    publication_to_wire,
};
use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind, encode_message,
    encoded_message_charge, fixed, require, storage_result,
};
use crate::{
    EncodedPageItem, NativeCatalogOriginEnvelopeDigestV1, NativeCatalogOriginReferenceV1,
    NativeCatalogTransitionOriginV1, NativeCatalogTransitionPartsV1, NativeCatalogTransitionV1,
    NativeLegacyControlsCommitmentV1, NativeTransitionEnvelopeBytesV1,
    SourceInheritanceAdministrationPartsV1, SourceInheritanceAdministrationV1,
    SourceInheritanceCauseV1, SourceInheritancePredecessorScalarsV1,
    SourceInheritanceSelectionCommitmentV1, SourceInheritanceWitnessV1,
};
use riffdb_proto::{
    durable::{ReadableRecordMessage, WritableRecordMessage},
    envelope::{
        CallerScratchDecodeErrorV1, decode_readable_message_with_caller_scratch_v1,
        inspect_readable_record_role_v1,
    },
    storage::v1 as wire,
};
use riffdb_types::{
    AdministrationSequence, ContentDigest, DIGEST_SCHEME_V1, DatabaseId, HashDomain,
    LeadershipEpochV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, ProjectionCheckpointOrdinal,
    ProjectionSourceLocatorV1, ProjectionSourceWorkAllocationV1, ProjectionSourceWorkingBudgetV1,
    SchemaHash,
};
use sha2::{Digest, Sha256};

mod catalog;
mod encode_bound;
mod inheritance;
mod originals;
pub use catalog::*;
pub use inheritance::*;
use originals::*;

const CATALOG: &str = "riffdb.storage.v1.StoredAuthoritativeCatalogTransitionV1";
const INHERITANCE: &str = "riffdb.storage.v1.StoredSourceInheritanceAdministrationV1";
const CATALOG_CAP: usize = 2048;
const INHERITANCE_CAP: usize = 16384;
const INNER_ENVELOPE_CAP: usize = 1024;

fn caller_decode<M: ReadableRecordMessage + Default>(
    bytes: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<M, DurableCodecError> {
    decode_readable_message_with_caller_scratch_v1(bytes, scratch).map_err(|error| match error {
        CallerScratchDecodeErrorV1::Envelope(error) => {
            DurableCodecError::from_decode_envelope(error)
        }
        CallerScratchDecodeErrorV1::CapacityUnavailable => DurableCodecError::invariant(),
    })
}

// Closed per-field sizing runs before the wire mapper allocates. Input data
// remains separately settled under this same source ledger.
fn encode_admitted<M: WritableRecordMessage>(
    name: &'static str,
    cap: usize,
    maximum: u64,
    budget: &ProjectionSourceWorkingBudgetV1,
    build: impl FnOnce() -> Result<M, DurableCodecError>,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(maximum)?;
    let message = build()?;
    if encoded_message_charge(name, &message)?.get() > cap {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
    }
    let encoded = encode_message(name, &message)?;
    drop(message);
    Ok(work.settle_owned_value(encoded)?)
}

riffdb_types::inspect_owned_capacity_struct_v1!(CanonicalStoredEnvelopeV1 {
    owned: [bytes],
    inline: [charge]
});

/// Decodes only the two closed native control Audit roles under the originating
/// engine's admitted source ledger. Normal roles return no match and still use
/// their unchanged ordinary decoder; a malformed known control always refuses.
pub fn decode_native_control_audit_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    Option<EncodedPageItem<crate::StoredAdministrationAuditRecordV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    use crate::StoredAdministrationAuditRecordV1 as Audit;
    let role = inspect_readable_record_role_v1(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    match role {
        CATALOG => {
            let (value, charge) =
                decode_native_catalog_transition_admitted_v1(encoded, budget)?.into_parts();
            Ok(Some(EncodedPageItem::new(
                Audit::NativeCatalogTransition(
                    crate::NativeCatalogTransitionAuditV1::from_admitted(value, charge),
                ),
                charge,
            )))
        }
        INHERITANCE => {
            let (value, charge) =
                decode_source_inheritance_admitted_v1(encoded, budget)?.into_parts();
            Ok(Some(EncodedPageItem::new(
                Audit::SourceInheritance(crate::SourceInheritanceAuditV1::from_admitted(
                    value, charge,
                )),
                charge,
            )))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests;
