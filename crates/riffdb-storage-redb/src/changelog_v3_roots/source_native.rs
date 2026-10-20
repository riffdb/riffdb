//! Only a fields-private actual Source logical reader can enter this transport.
use super::*;
use crate::store::OriginalLogicalRead;
use redb::TableHandle;
struct NativeSourceRootRead<'a, 'read, 'context, 'ports> {
    read: &'a mut OriginalLogicalRead<'read, 'context, 'ports>,
    history: bool,
    holds: bool,
    audit: bool,
}
pub(crate) fn read_native_source_roots(
    read: &mut OriginalLogicalRead<'_, '_, '_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let history = read.checkpoint_table_present(HISTORY.name())?;
    let holds = read.checkpoint_table_present(SOURCE_HOLDS.name())?;
    let audit = read.checkpoint_table_present(crate::layout::AUDIT.name())?;
    validate_root_evidence(
        &mut NativeSourceRootRead {
            read,
            history,
            holds,
            audit,
        },
        CheckpointRegistryAdmissionV1::current(),
    )
}
impl RootEvidenceRead for NativeSourceRootRead<'_, '_, '_, '_> {
    fn meta_value(&mut self, key: &str) -> Result<Option<RootValue>, StorageError> {
        self.read
            .checkpoint_meta(key)?
            .map(|v| RootValue::copy(v.value()))
            .transpose()
    }
    fn meta_present(&mut self, key: &str) -> Result<bool, StorageError> {
        self.read.checkpoint_meta_present(key)
    }
    fn history_present(&self) -> bool {
        self.history
    }
    fn holds_present(&self) -> bool {
        self.holds
    }
    fn history_empty(&mut self) -> Result<bool, StorageError> {
        self.read.checkpoint_table_empty(HISTORY.name())
    }
    fn holds_empty(&mut self) -> Result<bool, StorageError> {
        self.read.checkpoint_table_empty(SOURCE_HOLDS.name())
    }
    fn validate_source_allocator(
        &mut self,
        catalog: [u8; 32],
        incarnation: u64,
    ) -> Result<(), StorageError> {
        self.read
            .validate_checkpoint_source_allocator(catalog, incarnation)
    }
    fn validate_admission(
        &mut self,
        history: ChangelogHistoryStateV3,
        attached: bool,
    ) -> Result<(), StorageError> {
        crate::primary_admission_roots::validate_unfenced_source_admission_v1(
            self.read.checkpoint_admission()?,
            history,
            attached,
            self.audit,
        )
    }
    fn validate_terminal(&mut self, history: ChangelogHistoryStateV3) -> Result<(), StorageError> {
        self.read.validate_checkpoint_terminal(history)
    }
}
