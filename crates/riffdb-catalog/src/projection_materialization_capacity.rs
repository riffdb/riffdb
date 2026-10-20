//! Before-allocation sizing for the unchanged catalog normalization path.
use super::*;
use riffdb_types::{
    FieldId, OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1,
    canonical_value_clone_work_bound_v1, canonical_value_encoded_len,
    owned_allocation_capacity_bound_v1,
};
use std::{
    alloc::Layout,
    mem::{align_of, size_of},
};

impl ResolvedProjectionPlan {
    /// Additional construction allowance for one event's existing normalization.
    /// The caller must first reserve the existing ownership-census scratch, then
    /// reserve the returned allowance before calling materialize_event. Original
    /// plan, catalog and event custody remain separately charged. This numerical
    /// observation does not validate an event or grant source authority.
    #[doc(hidden)]
    pub fn source_event_materialization_work_bound_v1(
        &self,
        writer: &ExecutablePlanRef,
        event: &StoredDurableEventV1,
        mut scope: riffdb_types::OwnedCapacityInspectionScopeV1<'_>,
    ) -> Result<u64, OwnedCapacityInspectionErrorV1> {
        let (writer_ordinal, writer_bundle) = self
            .lineage_proof
            .exact_member(writer.contract_version(), writer.contract_bundle_hash())
            .filter(|(_, bundle)| bundle.lineage() == writer.contract_lineage())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        let command = writer_bundle
            .bundle()
            .command(writer.command_id())
            .filter(|plan| plan.plan_hash() == writer.command_plan_hash())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        if event.event_type_id() != self.plan.source_event() {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let schema = self
            .bundle
            .bundle()
            .schema()
            .event(self.plan.source_event())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        // resolve_plan_with_proof clones this full CommandPlan into a fresh Arc.
        // Count its actual typed graph, including lifecycle construction. Shared
        // fields are conservatively counted in full, even though Arc::clone
        // itself does not allocate their payload a second time.
        let report = OwnedCapacityInspectionV1::preview(command, scope.reborrow())?;
        let mut bytes = report.unshared_bytes();
        for allocation in report.shared_allocations() {
            add(&mut bytes, allocation.bytes())?;
        }
        drop(report);
        // ResolvedExecutablePlan also clones the exact ExecutablePlanRef.
        // Its shared lineage identity can retain physical input ownership; the
        // borrowed input owner remains live, and this allowance counts it
        // conservatively rather than transferring a foreign lifecycle binding.
        let reference = OwnedCapacityInspectionV1::preview(writer, scope.reborrow())?;
        add(&mut bytes, reference.unshared_bytes())?;
        for allocation in reference.shared_allocations() {
            add(&mut bytes, allocation.bytes())?;
        }
        drop(reference);
        let (plan_arc_layout, _) = Layout::new::<[usize; 2]>()
            .extend(Layout::new::<riffdb_contract_ir::CommandPlan>())
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        let plan_arc_layout = plan_arc_layout.pad_to_align();
        add_allocation(&mut bytes, plan_arc_layout.size(), plan_arc_layout.align())?;
        let fields_bytes = schema
            .payload()
            .fields()
            .len()
            .checked_mul(size_of::<(FieldId, CanonicalValue)>())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        // Exact known-field vector plus a whole-vector stable-sort scratch
        // allowance. The latter is deliberately conservative for small records.
        add_allocation(
            &mut bytes,
            fields_bytes,
            align_of::<(FieldId, CanonicalValue)>(),
        )?;
        add_allocation(
            &mut bytes,
            fields_bytes,
            align_of::<(FieldId, CanonicalValue)>(),
        )?;
        if writer_ordinal < self.projection_ordinal {
            add_allocation(
                &mut bytes,
                schema.payload().fields().len().div_ceil(8),
                align_of::<u8>(),
            )?;
        }
        // Canonical record version/tag/count. Missing source fields are sized as
        // Null; the unchanged normalizer must still prove that filling is legal.
        let mut encoded = 6_usize;
        for field in schema.payload().fields() {
            let value = event
                .payload()
                .fields()
                .binary_search_by_key(&field.id(), |(id, _)| *id)
                .ok()
                .map(|index| &event.payload().fields()[index].1);
            let length = match value {
                Some(value) => {
                    add(&mut bytes, canonical_value_clone_work_bound_v1(value)?)?;
                    canonical_value_encoded_len(value)
                        .map_err(|_| OwnedCapacityInspectionErrorV1)?
                }
                None => 2,
            };
            encoded = encoded
                .checked_add(4)
                .and_then(|n| n.checked_add(length))
                .ok_or(OwnedCapacityInspectionErrorV1)?;
        }
        if encoded > riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        add_allocation(&mut bytes, encoded, align_of::<u8>())?;
        Ok(bytes)
    }
}

fn add(total: &mut u64, bytes: u64) -> Result<(), OwnedCapacityInspectionErrorV1> {
    *total = total
        .checked_add(bytes)
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    Ok(())
}
fn add_allocation(
    total: &mut u64,
    bytes: usize,
    alignment: usize,
) -> Result<(), OwnedCapacityInspectionErrorV1> {
    add(total, owned_allocation_capacity_bound_v1(bytes, alignment)?)
}
