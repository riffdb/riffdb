//! Real offline Original adapter for the existing Projection instructions.
//! No catalog interpretation, staged rows, ordinal assignment or publication.
#![allow(clippy::let_and_return)]
use super::source_native_read::SourceOwnedOfflineReadV1;
use super::source_native_read::offline::OfflineSourceSessionCustodyV1;
use super::source_offline_original::OfflineOriginalSourceCursorV1;
use super::*;
use riffdb_projection::source_driver::{
    SourceCheckpointBackend, SourceCommandResponse, SourceDriverError, SourceReadInstruction,
    SourceReplayInputBackend, SourceRewindEvidence, SourceRewindInstruction,
};

pub(crate) struct NativeSourceSessionBrandV1 {
    custody: OfflineSourceSessionCustodyV1,
    // Actual brand header admission stays through its descriptor/data retirement.
    _work: SourceReadWork,
}
/// Uninhabited while the backend implements Original input only. A numerical
/// read component can never fabricate a stage instance or publication route.
pub(crate) enum NativeSourceStageBrandV1 {}
pub(crate) struct NativeSourceOriginalInputV1<'read> {
    cursor: OfflineOriginalSourceCursorV1<'read>,
}
pub(crate) struct NativeOfflineSourceBackendV1<'brand, 'read> {
    #[expect(
        dead_code,
        reason = "staged offline Original replay input RAII drop order for WP-757/WP-760"
    )]
    input: NativeSourceOriginalInputV1<'read>,
    #[expect(
        dead_code,
        reason = "staged offline Original session brand lifetime bound for WP-757/WP-760"
    )]
    brand: &'brand NativeSourceSessionBrandV1,
    // Closed physical Original/descriptors/worker remain in input and brand;
    // same original-ledger control allowance retires after them on every path.
    _work: SourceReadWork,
}
impl Drop for NativeOfflineSourceBackendV1<'_, '_> {
    fn drop(&mut self) {}
}

impl SourceOwnedOfflineReadV1 {
    pub(in crate::store) fn prepare_original_instruction_brand(
        &self,
    ) -> Result<NativeSourceSessionBrandV1, StorageError> {
        let (budget, _) = self.original_history_admission()?;
        let bytes = u64::try_from(std::mem::size_of::<NativeSourceSessionBrandV1>())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let work = SourceReadWork::admit(&budget, bytes)?;
        let custody = self.original_session_custody()?;
        Ok(NativeSourceSessionBrandV1 {
            custody,
            _work: work,
        })
    }
}
impl<'brand, 'read> NativeOfflineSourceBackendV1<'brand, 'read> {
    #[expect(
        dead_code,
        reason = "staged offline Original replay backend constructor for WP-757/WP-760"
    )]
    pub(in crate::store) fn attach(
        facade: &'read mut SourceOwnedOfflineReadV1,
        brand: &'brand NativeSourceSessionBrandV1,
    ) -> Result<Self, StorageError> {
        // Brand is a fixed external worker-stack owner. Preplan catalog evidence
        // can retain its exact reference through later input/stage construction.
        // The private factory rejects a brand minted by any other actual facade.
        if !brand.custody.matches_facade(facade) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let (budget, _) = facade.original_history_admission()?;
        let bytes = std::mem::size_of::<(Self, NativeSourceOriginalInputV1<'read>)>();
        let work = SourceReadWork::admit(
            &budget,
            u64::try_from(bytes).map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        )?;
        let cursor = facade.original_cursor()?;
        if !cursor.belongs_to_session(&brand.custody) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(Self {
            input: NativeSourceOriginalInputV1 { cursor },
            brand,
            _work: work,
        })
    }
}
impl<'brand, 'read> SourceCheckpointBackend for NativeOfflineSourceBackendV1<'brand, 'read> {
    type Brand = NativeSourceSessionBrandV1;
    type StageBrand = NativeSourceStageBrandV1;
}
impl<'brand, 'read> SourceReplayInputBackend for NativeOfflineSourceBackendV1<'brand, 'read> {
    type Input = NativeSourceOriginalInputV1<'read>;
    fn rewind<'session>(
        input: &mut Self::Input,
        actual: &Self::Brand,
        request: SourceRewindInstruction<'session, Self>,
    ) -> Result<SourceRewindEvidence<'session, Self>, SourceDriverError> {
        request.check_before_rewind(actual)?;
        if !input.cursor.belongs_to_session(&actual.custody) {
            return Err(SourceDriverError::ForeignCustody);
        }
        input.cursor.rewind_original_prefix();
        request.rewound(actual)
    }
    fn read_command<'command, 'session>(
        input: &'command mut Self::Input,
        actual: &Self::Brand,
        request: SourceReadInstruction<'session, Self>,
    ) -> Result<SourceCommandResponse<'command, 'session, Self>, SourceDriverError> {
        request.check_before_read(actual)?;
        if !input.cursor.belongs_to_session(&actual.custody) {
            return Err(SourceDriverError::ForeignCustody);
        }
        match input.cursor.next_commit()? {
            Some(command) => request.command(actual, command),
            None => request.exact_end(actual),
        }
    }
}

pub(crate) struct NativeSourceCatalogPointerOwnerV1 {
    pointer: Option<
        riffdb_types::ProjectionSourceWorkAllocationV1<riffdb_storage_api::ActiveCatalogPointerV1>,
    >,
    custody: OfflineSourceSessionCustodyV1,
    _work: SourceReadWork,
}
pub(crate) struct NativeSourceCatalogEnvelopeOwnerV1 {
    // Opaque decoded data retires before actual native owners. The raw
    // transport copy was destroyed before this final owner was constructed.
    record:
        riffdb_types::ProjectionSourceWorkAllocationV1<riffdb_storage_api::StoredContractBundleV1>,
    // Actual pointer alias includes its real native custody, even if outer
    // pointer response is subsequently forgotten or destroyed independently.
    pointer: NativeSourceCatalogPointerOwnerV1,
    _work: SourceReadWork,
}
impl NativeSourceCatalogPointerOwnerV1 {
    fn share_for_envelope(&self) -> Result<Self, StorageError> {
        let budget = self.custody.originating_budget()?;
        let work = SourceReadWork::admit(budget, std::mem::size_of::<Self>() as u64)?;
        Ok(Self {
            pointer: self.pointer.clone(),
            custody: self.custody.share_for_catalog()?,
            _work: work,
        })
    }
}
use riffdb_projection::source_driver::{
    SourceCatalogBundleEvidence, SourceCatalogBundleReadInstruction, SourceCatalogEnvelopeOwner,
    SourceCatalogInputBackend, SourceCatalogPointerEvidence, SourceCatalogPointerOwner,
    SourceCatalogPointerReadInstruction,
};
impl<B: SourceCheckpointBackend<Brand = NativeSourceSessionBrandV1>> SourceCatalogPointerOwner<B>
    for NativeSourceCatalogPointerOwnerV1
{
    fn pointer(&self) -> Option<&riffdb_storage_api::ActiveCatalogPointerV1> {
        self.pointer.as_deref()
    }
    fn belongs_to(&self, budget: &riffdb_types::ProjectionSourceWorkingBudgetV1) -> bool {
        self.custody
            .originating_budget()
            .is_ok_and(|actual| actual.shares_owner_with(budget))
            && self.pointer.as_ref().is_none_or(|p| p.belongs_to(budget))
    }
    fn matches_native(&self, actual: &NativeSourceSessionBrandV1) -> bool {
        self.custody.matches_session(&actual.custody)
    }
}
impl<B: SourceCheckpointBackend<Brand = NativeSourceSessionBrandV1>> SourceCatalogEnvelopeOwner<B>
    for NativeSourceCatalogEnvelopeOwnerV1
{
    fn record(&self) -> &riffdb_storage_api::StoredContractBundleV1 {
        &self.record
    }
    fn belongs_to(&self, budget: &riffdb_types::ProjectionSourceWorkingBudgetV1) -> bool {
        self.record.belongs_to(budget)
            && <NativeSourceCatalogPointerOwnerV1 as SourceCatalogPointerOwner<B>>::belongs_to(
                &self.pointer,
                budget,
            )
    }
    fn matches_native(&self, actual: &NativeSourceSessionBrandV1) -> bool {
        self.pointer.custody.matches_session(&actual.custody)
    }
}
impl<'brand, 'read> SourceCatalogInputBackend for NativeOfflineSourceBackendV1<'brand, 'read> {
    type PointerOwner = NativeSourceCatalogPointerOwnerV1;
    type EnvelopeOwner = NativeSourceCatalogEnvelopeOwnerV1;
    type Reader = SourceOwnedOfflineReadV1;
    fn read_catalog_pointer<'session>(
        reader: &mut Self::Reader,
        actual: &Self::Brand,
        request: SourceCatalogPointerReadInstruction<'session, Self>,
    ) -> Result<SourceCatalogPointerEvidence<'session, Self>, SourceDriverError> {
        use redb::TableHandle;
        request.check_before_read(actual)?;
        if !actual.custody.matches_facade(reader) {
            return Err(SourceDriverError::ForeignCustody);
        }
        let budget = actual.custody.originating_budget()?;
        let work = SourceReadWork::admit(
            budget,
            std::mem::size_of::<NativeSourceCatalogPointerOwnerV1>() as u64,
        )?;
        // Full pointer decoder allowance precedes native table/value lookup.
        let admission =
            riffdb_storage_api::proto_codec::ProjectionSourceActiveCatalogDecodeAdmissionV1::new(
                budget,
            )
            .map_err(source_catalog_decode_error)?;
        let custody = actual.custody.share_for_catalog()?;
        let pointer = {
            let mut read = reader.begin_read()?;
            let mut table = read.open_bytes_table(crate::layout::CATALOG_ACTIVE.name())?;
            let value = table.get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())?;
            let decoded = match value {
                Some(value) => Some(
                    admission
                        .decode(value.value())
                        .map_err(source_catalog_decode_error)?
                        .into_parts()
                        .0,
                ),
                None => {
                    drop(admission);
                    None
                }
            };
            decoded
        };
        request.attach(
            actual,
            NativeSourceCatalogPointerOwnerV1 {
                pointer,
                custody,
                _work: work,
            },
        )
    }
    fn read_catalog_bundle<'pointer, 'session>(
        reader: &mut Self::Reader,
        actual: &Self::Brand,
        request: SourceCatalogBundleReadInstruction<'pointer, 'session, Self>,
    ) -> Result<SourceCatalogBundleEvidence<'pointer, 'session, Self>, SourceDriverError> {
        use redb::TableHandle;
        request.check_before_read(actual)?;
        if !actual.custody.matches_facade(reader) {
            return Err(SourceDriverError::ForeignCustody);
        }
        let budget = actual.custody.originating_budget()?;
        let pointer = request.pointer()?;
        let key_bound = crate::keys::contract_bundle_key_allocation_bound(pointer.lineage())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let controls = u64::try_from(std::mem::size_of::<NativeSourceCatalogEnvelopeOwnerV1>())
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        let work = SourceReadWork::admit(
            budget,
            key_bound
                .checked_add(controls)
                .ok_or(SourceDriverError::SourceUnavailable)?,
        )?;
        let key =
            crate::keys::encode_contract_bundle_key(pointer.lineage(), pointer.contract_version())
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let owned_pointer = request.pointer_owner().share_for_envelope()?;
        let envelope = {
            let mut read = reader.begin_read()?;
            let mut table = read.open_bytes_table(crate::layout::CONTRACT_BUNDLES.name())?;
            let value = table
                .get(&key)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let encoded = value.value();
            if encoded.len() > riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES {
                // Same ordinary EnvelopeTooLarge→codec LimitExceeded policy.
                return Err(SourceDriverError::Storage(storage_error(
                    StorageErrorKind::LimitExceeded,
                )));
            }
            let maximum=riffdb_types::owned_allocation_capacity_bound_v1(encoded.len(),1)
                .map_err(|_|SourceDriverError::SourceUnavailable)?
                .checked_add(riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner::<Vec<u8>>::settlement_root_capacity_upper_bound()
                    .map_err(|_|SourceDriverError::SourceUnavailable)?)
                .and_then(|n|n.checked_add(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
                .ok_or(SourceDriverError::SourceUnavailable)?;
            let construction = budget
                .reserve(maximum)
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let mut copy = Vec::new();
            copy.try_reserve_exact(encoded.len())
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            copy.extend_from_slice(encoded);
            construction
                .settle_owned_value(copy)
                .map_err(|_| SourceDriverError::SourceUnavailable)?
        };
        drop(key);
        // Only the sole StorageAPI opaque envelope decoder runs here. All IR
        // interpretation remains above Native, on the accepted worker capsule.
        // Partial DTO/error data dies inside the mapper while both encoded and
        // actual native owners plus the original work are still physically live.
        let record = riffdb_storage_api::proto_codec::decode_source_catalog_bundle_admitted_v1(
            &envelope, budget,
        )
        .map_err(source_catalog_decode_error)?;
        // StoredContractBundle owns independently admitted canonical bytes and
        // its original shared lineage. No later Catalog/IR reader needs this
        // duplicate transport allocation. Preserve native/pointer/control while
        // retiring it now, before returning the final opaque data owner.
        drop(envelope);
        request.attach(
            actual,
            NativeSourceCatalogEnvelopeOwnerV1 {
                record,
                pointer: owned_pointer,
                _work: work,
            },
        )
    }
}
fn source_catalog_decode_error(
    error: riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1,
) -> SourceDriverError {
    match error {
        riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            SourceDriverError::SourceUnavailable
        }
        riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::Codec(error) => {
            SourceDriverError::Storage(crate::error::codec_error(error))
        }
    }
}

#[cfg(test)]
impl SourceOwnedOfflineReadV1 {
    pub(crate) fn prepare_catalog_instruction_brand_for_test(
        &self,
    ) -> Result<NativeSourceSessionBrandV1, StorageError> {
        self.prepare_original_instruction_brand()
    }
    pub(crate) fn catalog_instruction_input_for_test<'session>(
        &mut self,
        brand: &'session NativeSourceSessionBrandV1,
        budget: &'session riffdb_types::ProjectionSourceWorkingBudgetV1,
    ) -> Result<
        riffdb_projection::source_driver::SourceCatalogInputs<
            'session,
            NativeOfflineSourceBackendV1<'session, 'session>,
        >,
        SourceDriverError,
    > {
        riffdb_projection::source_driver::read_source_catalog_input::<
            NativeOfflineSourceBackendV1<'session, 'session>,
        >(self, brand, budget)
    }
}
