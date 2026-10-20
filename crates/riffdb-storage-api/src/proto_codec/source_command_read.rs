//! Closed structural command reconstruction with prior source-worker admission.
//! The native captured-prefix owner must additionally prove engine identity,
//! physical key, retained canonical source closure, ordering and captured F.
use super::{
    DurableCodecError, ProjectionSourceRecordDecodeErrorV1, application, command_capsule,
    command_segment, fixed, require,
};
use crate::{
    CommandSegmentDigestV1, DecodedCommitEventReferencesV1, EventReferenceV2,
    StoredCommandCapsuleV1, StoredCommandCapsuleV2, StoredCommandSegmentV1, StoredCommitRecordV1,
    StoredDurableEventV1,
};
use riffdb_proto::{durable::ReadableRecordMessage, envelope::*, storage::v1 as wire};
use riffdb_types::{ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1};

type Error = ProjectionSourceRecordDecodeErrorV1;
fn corrupt() -> Error {
    DurableCodecError::corrupt().into()
}
fn unavailable() -> Error {
    Error::AdmissionUnavailable
}

enum Complete {
    Segment(StoredCommandSegmentV1),
    Capsule(StoredCommandCapsuleV2),
    LegacyCapsule(StoredCommandCapsuleV1),
    Commit(StoredCommitRecordV1),
}
enum Prepared {
    Capsule(Box<wire::StoredCommandCapsuleV1>, Vec<EventReferenceV2>),
    Commit(Box<DecodedCommitEventReferencesV1>),
}
// Field order is part of the memory owner: data/scratch drop before admission.
struct EventOwner {
    value: Option<StoredDurableEventV1>,
    _scratch: Vec<u8>,
    _work: ProjectionSourceWorkReservationV1,
}

/// Move-only complete command graph owned by the actual source worker ledger.
/// Borrows cannot escape this owner; no detached graph, event or allocation is
/// returned. Canonical syntax never grants captured-source or writer authority.
pub struct ProjectionSourceCommandReadV1 {
    complete: Option<Complete>,
    prepared: Option<Prepared>,
    event_owners: Vec<EventOwner>,
    _scratch: Vec<u8>,
    _work: ProjectionSourceWorkReservationV1,
    budget: ProjectionSourceWorkingBudgetV1,
    encoded_charge: crate::EncodedContentCharge,
}
impl ProjectionSourceCommandReadV1 {
    /// Ordered event references requiring authoritative event-table joins.
    /// Embedded event graphs are already completely validated by their mapper.
    pub fn required_events(&self) -> &[EventReferenceV2] {
        match &self.prepared {
            Some(Prepared::Capsule(_, references)) => references,
            Some(Prepared::Commit(commit)) => commit.references(),
            None => &[],
        }
    }
    /// Canonically decodes the next exact event under this same retained ledger.
    /// The native reader supplies only bytes read through its originating engine
    /// capture; no caller-selected semantic event value can enter this graph.
    pub fn accept_event_canonical(&mut self, encoded: &[u8]) -> Result<(), Error> {
        let index = self.event_owners.len();
        let expected = *self.required_events().get(index).ok_or_else(corrupt)?;
        let work = self
            .budget
            .reserve(command_work(encoded)?)
            .map_err(|_| unavailable())?;
        let mut scratch = Vec::with_capacity(encoded.len());
        let role = inspect_readable_record_role_v1(encoded)
            .map_err(DurableCodecError::from_decode_envelope)?;
        let value = match role {
            "riffdb.storage.v1.StoredDurableEventV1" => {
                application::event_from_proto(read::<wire::StoredDurableEventV1>(
                    encoded,
                    &mut scratch,
                )?)?
            }
            "riffdb.storage.v1.StoredDurableEventV2" => super::storage_result(
                application::event_v2_from_proto(read::<wire::StoredDurableEventV2>(
                    encoded,
                    &mut scratch,
                )?)?
                .into_common(),
            )?,
            _ => return Err(corrupt()),
        };
        if !expected.matches(&value) {
            return Err(corrupt());
        }
        self.event_owners.push(EventOwner {
            value: Some(value),
            _scratch: scratch,
            _work: work,
        });
        Ok(())
    }
    /// Completes the existing exact event join without releasing any event graph
    /// allowance. An incomplete or unequal join refuses before graph exposure.
    pub fn finish_event_join(&mut self) -> Result<(), Error> {
        if self.complete.is_some() {
            return Ok(());
        }
        if self.event_owners.len() != self.required_events().len() {
            return Err(corrupt());
        }
        let mut events = Vec::with_capacity(self.event_owners.len());
        for owner in &mut self.event_owners {
            events.push(owner.value.take().ok_or_else(corrupt)?);
        }
        self.complete = Some(match self.prepared.take().ok_or_else(corrupt)? {
            Prepared::Capsule(value, _) => Complete::LegacyCapsule(
                command_capsule::capsule_from_canonical_proto(*value, events)?,
            ),
            Prepared::Commit(value) => Complete::Commit(
                (*value)
                    .materialize(events, self.encoded_charge)?
                    .into_parts()
                    .0,
            ),
        });
        Ok(())
    }
    /// Borrows the exact existing semantic commit; the graph remains charged.
    pub fn commit(&self, index: usize) -> Option<&StoredCommitRecordV1> {
        match self.complete.as_ref()? {
            Complete::Segment(value) => value
                .commands()
                .get(index)
                .map(|value| value.base().commit()),
            Complete::Capsule(value) if index == 0 => Some(value.base().commit()),
            Complete::LegacyCapsule(value) if index == 0 => Some(value.commit()),
            Complete::Commit(value) if index == 0 => Some(value),
            _ => None,
        }
    }
    /// Borrows the existing fully validated segment while its complete graph
    /// stays admitted. The native owner additionally checks its exact physical
    /// key, segment summary, originating engine and retained source closure.
    #[doc(hidden)]
    pub fn validated_segment_v1(&self) -> Option<&StoredCommandSegmentV1> {
        match self.complete.as_ref()? {
            Complete::Segment(value) => Some(value),
            _ => None,
        }
    }
    /// A finite physical graph count; every logical command remains individually
    /// ordered and bounded by the native cursor's original captured frontier.
    pub fn commit_count(&self) -> usize {
        match &self.complete {
            Some(Complete::Segment(value)) => value.commands().len(),
            Some(_) => 1,
            None => 0,
        }
    }
}

fn command_work(encoded: &[u8]) -> Result<u64, Error> {
    let mut work =
        crate::projection_source_command_decode_work_bound_v1(encoded)?.ok_or_else(corrupt)?;
    // The two historical preparation variants have different large inline
    // representations. Each box is separately admitted at its exact generated
    // or existing prepared type layout before Box::new can allocate.
    let role = inspect_readable_record_role_v1(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let boxed = match role {
        "riffdb.storage.v1.StoredCommandCapsuleV1" => Some((
            std::mem::size_of::<wire::StoredCommandCapsuleV1>(),
            std::mem::align_of::<wire::StoredCommandCapsuleV1>(),
        )),
        "riffdb.storage.v1.StoredCommitRecordV1"
        | "riffdb.storage.v1.StoredCommitRecordV2"
        | "riffdb.storage.v1.StoredCommitRecordV3" => Some((
            std::mem::size_of::<DecodedCommitEventReferencesV1>(),
            std::mem::align_of::<DecodedCommitEventReferencesV1>(),
        )),
        _ => None,
    };
    if let Some((bytes, alignment)) = boxed {
        let charge = riffdb_types::owned_allocation_capacity_bound_v1(bytes, alignment)
            .map_err(|_| unavailable())?;
        work = work.checked_add(charge).ok_or_else(unavailable)?;
    }
    // External-event wrappers and the mapper's exact-capacity transfer vector
    // coexist with the prepared command and each independently admitted event.
    let shape = inspect_command_source_decode_shape_v1(encoded, |_| Ok(0))
        .map_err(DurableCodecError::from_decode_envelope)?
        .ok_or_else(corrupt)?;
    let count = usize::try_from(shape.semantic_event_references).map_err(|_| unavailable())?;
    for (each, alignment) in [
        (
            std::mem::size_of::<EventOwner>(),
            std::mem::align_of::<EventOwner>(),
        ),
        (
            std::mem::size_of::<StoredDurableEventV1>(),
            std::mem::align_of::<StoredDurableEventV1>(),
        ),
    ] {
        let bytes = count.checked_mul(each).ok_or_else(unavailable)?;
        let extra = riffdb_types::owned_allocation_capacity_bound_v1(bytes, alignment)
            .map_err(|_| unavailable())?;
        work = work.checked_add(extra).ok_or_else(unavailable)?;
    }
    Ok(work)
}
fn read<M: ReadableRecordMessage + Default>(
    encoded: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<M, Error> {
    decode_readable_message_with_caller_scratch_v1(encoded, scratch).map_err(|error| match error {
        CallerScratchDecodeErrorV1::CapacityUnavailable => unavailable(),
        CallerScratchDecodeErrorV1::Envelope(error) => {
            DurableCodecError::from_decode_envelope(error).into()
        }
    })
}

/// Admits the complete temporary/retained decode graph before any canonical
/// scratch, generated message or mapper allocation. A largest readable record
/// may safely refuse when it cannot coexist with the worker's other owners.
pub fn decode_projection_source_command_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<ProjectionSourceCommandReadV1, Error> {
    let encoded_charge = crate::EncodedContentCharge::new(encoded.len()).ok_or_else(corrupt)?;
    let work = budget
        .reserve(command_work(encoded)?)
        .map_err(|_| unavailable())?;
    let mut scratch = Vec::with_capacity(encoded.len());
    let role = inspect_readable_record_role_v1(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let mut complete = None;
    let mut prepared = None;
    macro_rules! capsule {
        ($wire:ty,$mapper:ident) => {{
            complete = Some(Complete::Capsule(command_segment::$mapper(read::<$wire>(
                encoded,
                &mut scratch,
            )?)?));
        }};
    }
    macro_rules! segment {
        ($wire:ty,$mapper:ident) => {{
            let value = read::<$wire>(encoded, &mut scratch)?;
            let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
            complete = Some(Complete::Segment(command_segment::$mapper(
                require(value.body)?,
                digest,
            )?));
        }};
    }
    match role {
        "riffdb.storage.v1.StoredCommandCapsuleV1" => {
            let value = read::<wire::StoredCommandCapsuleV1>(encoded, &mut scratch)?;
            let references = value
                .event_references
                .iter()
                .cloned()
                .map(application::event_reference_from_proto)
                .collect::<Result<_, _>>()?;
            prepared = Some(Prepared::Capsule(Box::new(value), references));
        }
        "riffdb.storage.v1.StoredCommandCapsuleV2" => {
            capsule!(wire::StoredCommandCapsuleV2, capsule_v2_from_proto)
        }
        "riffdb.storage.v1.StoredCommandCapsuleV3" => {
            capsule!(wire::StoredCommandCapsuleV3, capsule_v3_from_proto)
        }
        "riffdb.storage.v1.StoredCommandCapsuleV4" => {
            capsule!(wire::StoredCommandCapsuleV4, capsule_v4_from_proto)
        }
        "riffdb.storage.v1.StoredCommandCapsuleV5" => {
            capsule!(wire::StoredCommandCapsuleV5, capsule_v5_from_proto)
        }
        "riffdb.storage.v1.StoredCommandCapsuleV6" => {
            capsule!(wire::StoredCommandCapsuleV6, capsule_v6_from_proto)
        }
        "riffdb.storage.v1.StoredCommandCapsuleV7" => {
            capsule!(wire::StoredCommandCapsuleV7, capsule_v7_from_proto)
        }
        "riffdb.storage.v1.StoredCommandSegmentV1" => {
            complete = Some(Complete::Segment(
                command_segment::segment_v1_from_canonical_proto(read::<
                    wire::StoredCommandSegmentV1,
                >(
                    encoded, &mut scratch
                )?)?,
            ))
        }
        "riffdb.storage.v1.StoredCommandSegmentV2" => {
            complete = Some(Complete::Segment(
                command_segment::segment_v2_from_canonical_proto(read::<
                    wire::StoredCommandSegmentV2,
                >(
                    encoded, &mut scratch
                )?)?,
            ))
        }
        "riffdb.storage.v1.StoredCommandSegmentV3" => {
            segment!(wire::StoredCommandSegmentV3, segment_from_body_v3)
        }
        "riffdb.storage.v1.StoredCommandSegmentV4" => {
            segment!(wire::StoredCommandSegmentV4, segment_from_body_v4)
        }
        "riffdb.storage.v1.StoredCommandSegmentV5" => {
            segment!(wire::StoredCommandSegmentV5, segment_from_body_v5)
        }
        "riffdb.storage.v1.StoredCommandSegmentV6" => {
            segment!(wire::StoredCommandSegmentV6, segment_from_body_v6)
        }
        "riffdb.storage.v1.StoredCommitRecordV1" => {
            let value =
                application::commit_legacy_from_canonical_proto(
                    read::<wire::StoredCommitRecordV1>(encoded, &mut scratch)?,
                )?;
            prepared = Some(Prepared::Commit(Box::new(
                application::prepared_legacy_commit_from_canonical(value),
            )));
        }
        "riffdb.storage.v1.StoredCommitRecordV2" => {
            prepared = Some(Prepared::Commit(Box::new(
                application::prepared_commit_v2_from_canonical(
                    read::<wire::StoredCommitRecordV2>(encoded, &mut scratch)?,
                )?,
            )))
        }
        "riffdb.storage.v1.StoredCommitRecordV3" => {
            prepared = Some(Prepared::Commit(Box::new(
                application::prepared_commit_v3_from_canonical(
                    read::<wire::StoredCommitRecordV3>(encoded, &mut scratch)?,
                )?,
            )))
        }
        _ => return Err(corrupt()),
    }
    let count = match &prepared {
        Some(Prepared::Capsule(_, refs)) => refs.len(),
        Some(Prepared::Commit(value)) => value.references().len(),
        None => 0,
    };
    Ok(ProjectionSourceCommandReadV1 {
        complete,
        prepared,
        event_owners: Vec::with_capacity(count),
        _scratch: scratch,
        _work: work,
        budget: budget.clone(),
        encoded_charge,
    })
}
