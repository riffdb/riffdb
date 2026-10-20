//! Source-private FIRST capture; no checkpoint-cache publication or raw read escape.
use super::super::source_original_retention::OriginalSourceReadContext;
use super::*;

pub(in crate::store) struct OriginalNativeBase<'context, 'ports> {
    access: Option<RedbReadAccess>,
    context: &'context OriginalSourceReadContext<'ports>,
    _control: ProjectionSourceWorkReservationV1,
}
impl Drop for OriginalNativeBase<'_, '_> {
    fn drop(&mut self) {}
}
impl<'ports> OriginalSourceReadContext<'ports> {
    pub(in crate::store) fn begin_native_base(
        &self,
    ) -> Result<OriginalNativeBase<'_, 'ports>, StorageError> {
        if !self.ports().shared.source_native_file_backend
            || !self.ports().shared.is_source_mode()
            || self
                .ports()
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let inline = std::mem::size_of::<OriginalNativeBase<'_, '_>>()
            .checked_add(std::mem::size_of::<
                crate::store::OriginalLogicalRead<'_, '_, '_>,
            >())
            .ok_or_else(unavailable)?;
        let control = self
            .budget()
            .reserve(u64::try_from(inline).map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?;
        let access = self.ports().shared.try_clone_published_source_base()?;
        Ok(OriginalNativeBase {
            access,
            context: self,
            _control: control,
        })
    }
}
impl OriginalNativeBase<'_, '_> {
    pub(in crate::store) fn context(&self) -> &OriginalSourceReadContext<'_> {
        self.context
    }
    pub(in crate::store) fn composite(
        &self,
    ) -> Option<&crate::composite_view::RedbCompositeReadView> {
        match self.access.as_ref() {
            Some(RedbReadAccess::Composite(view)) => Some(view),
            _ => None,
        }
    }
    pub(in crate::store) fn open(&self) -> Result<SourceNativeReadTransaction<'_>, StorageError> {
        let capture = if self.access.is_some() {
            redb::ReadTransaction::read_context_plan_capture_bytes()
        } else {
            redb::Database::current_read_plan_capture_bytes()
        }
        .map_err(native_read_error)?;
        let fixed = source_control_bytes()?
            .checked_add(shared_owner_bound::<NumericAdmission>()?)
            .and_then(|n| n.checked_add(shared_owner_bound::<ActualBaseReadData>().ok()?))
            .ok_or_else(unavailable)?;
        let mut reservation = self
            .context
            .budget()
            .reserve(fixed.checked_add(capture).ok_or_else(unavailable)?)
            .map_err(|_| unavailable())?;
        // Plans have no allocation/registration/I/O before this admission.
        match self.access.as_ref() {
            Some(access) => {
                let checkpoint = match access {
                    RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
                        root.transaction()
                    }
                    RedbReadAccess::Composite(view) => view.checkpoint_root().transaction(),
                };
                let plan = checkpoint.plan_read_context().map_err(native_read_error)?;
                absorb_peak(self.context, &mut reservation, capture, plan.peak_bytes())?;
                let numeric = NumericOwner::new(reservation);
                let base = ActualBaseReadCustody::new(access.clone(), numeric.share()?);
                let inner = checkpoint
                    .open_planned_read_context(plan)
                    .map_err(native_read_error)?;
                Ok(SourceNativeReadTransaction {
                    inner,
                    _source: None,
                    _base: Some(base),
                    _numeric: numeric,
                })
            }
            None => {
                let engine = &self.context.ports().shared.database;
                let plan = engine
                    .plan_current_read_context()
                    .map_err(native_read_error)?;
                absorb_peak(self.context, &mut reservation, capture, plan.peak_bytes())?;
                let numeric = NumericOwner::new(reservation);
                let base = ActualBaseReadCustody(Some(Arc::new(ActualBaseReadData {
                    _access: ActualBaseAccess::Current {
                        _engine: engine.clone(),
                    },
                    _numeric: numeric.share()?,
                })));
                let inner = engine
                    .begin_planned_read_context(plan)
                    .map_err(native_read_error)?;
                Ok(SourceNativeReadTransaction {
                    inner,
                    _source: None,
                    _base: Some(base),
                    _numeric: numeric,
                })
            }
        }
    }
}
fn absorb_peak(
    context: &OriginalSourceReadContext<'_>,
    reservation: &mut ProjectionSourceWorkReservationV1,
    capture: u64,
    peak: u64,
) -> Result<(), StorageError> {
    let extra = peak
        .max(capture)
        .checked_sub(capture)
        .ok_or_else(invariant)?;
    if extra != 0 {
        let additional = context.budget().reserve(extra).map_err(|_| unavailable())?;
        reservation.absorb(additional).map_err(|_| unavailable())?;
    }
    Ok(())
}
