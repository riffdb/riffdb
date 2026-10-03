//! Explicit capture and retirement authority, shared across provider families.
// req: OQ-040
#![forbid(unsafe_code)]

use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::exact_provider_checkpoint_custody::CheckpointManifest;
use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, OwnershipCharge, ProviderBudget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Active,
    Retiring,
    Refused,
}

// Use the same conservative allocation allowance as the capacity inspector,
// including each Arc header. These two closed metadata types cannot overflow.
const fn arc_capacity_bound<T>() -> u64 {
    (2 * (std::mem::size_of::<T>() + 2 * std::mem::size_of::<usize>())
        + 2 * std::mem::align_of::<T>()
        + 256) as u64
}
const PIN_ALLOCATION_BYTES: u64 =
    arc_capacity_bound::<PinGuard>() + arc_capacity_bound::<OwnershipCharge>();
const _: () = assert!(PIN_ALLOCATION_BYTES <= 1024);

struct ArtifactState {
    manifest: CheckpointManifest,
    observed: bool,
}

struct State {
    phase: Phase,
    pins: u32,
    artifact: Option<Box<ArtifactState>>,
    // Last: artifact data drops before final lifecycle release.
    accounting: riffdb_types::exact_provider_budget_v1::ProviderOwnerAccounting,
}

pub(super) struct ProviderOwner {
    state: Mutex<State>,
    budget: ProviderBudget,
    // Last field: the registration charge survives all owner state.
    _charge: Arc<OwnershipCharge>,
}

impl ProviderOwner {
    pub(super) fn new(
        budget: ProviderBudget,
        charge: Arc<OwnershipCharge>,
    ) -> Result<Arc<Self>, BudgetUnavailable> {
        let minimum = arc_capacity_bound::<Self>() + arc_capacity_bound::<OwnershipCharge>();
        if !charge.belongs_to(&budget) || charge.bytes() < minimum {
            return Err(BudgetUnavailable);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Active,
                pins: 0,
                artifact: None,
                accounting: budget.register_provider_owner()?,
            }),
            budget,
            _charge: charge,
        }))
    }

    pub(super) fn artifact_binding(&self) -> Result<(CheckpointManifest, bool), BudgetUnavailable> {
        let state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase == Phase::Refused || state.pins == 0 {
            return Err(BudgetUnavailable);
        }
        state
            .artifact
            .as_deref()
            .map(|artifact| (artifact.manifest.clone(), !artifact.observed))
            .ok_or(BudgetUnavailable)
    }
    /// Existing work may record its own partial resources after supersession;
    /// these claims grant no publication or new pin and preserve cleanup custody.
    pub(super) fn record_artifact(
        &self,
        manifest: CheckpointManifest,
    ) -> Result<(), BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase == Phase::Refused || state.pins == 0 {
            return Err(BudgetUnavailable);
        }
        let binding = state.artifact.as_deref_mut().ok_or(BudgetUnavailable)?;
        if !binding.manifest.same_binding(&manifest) {
            let _ = state.accounting.retire();
            state.phase = Phase::Refused;
            return Err(BudgetUnavailable);
        }
        binding.manifest = manifest;
        binding.observed = true;
        Ok(())
    }

    /// Binding is fixed before the first capture, under already admitted root
    /// metadata. Later writes may claim objects only in this exact namespace.
    pub(super) fn bind_artifact(
        &self,
        binding: CheckpointManifest,
    ) -> Result<(), BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        let minimum = arc_capacity_bound::<Self>()
            + arc_capacity_bound::<OwnershipCharge>()
            + (2 * std::mem::size_of::<ArtifactState>()
                + 2 * std::mem::align_of::<ArtifactState>()
                + 256) as u64;
        if state.phase != Phase::Active
            || state.pins != 0
            || state.artifact.is_some()
            || self._charge.bytes() < minimum
        {
            return Err(BudgetUnavailable);
        }
        state.artifact = Some(Box::new(ArtifactState {
            manifest: binding,
            observed: false,
        }));
        Ok(())
    }

    /// Cleanup evidence is available only after discovery admission is closed
    /// and every capture/publication has released its pin.
    pub(super) fn cleanup_manifest(&self) -> Result<Option<CheckpointManifest>, BudgetUnavailable> {
        let state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Retiring || state.pins != 0 {
            return Err(BudgetUnavailable);
        }
        Ok(state
            .artifact
            .as_deref()
            .map(|artifact| artifact.manifest.clone()))
    }

    /// Must precede query/worker capture. Retirement closes new admission.
    pub(super) fn pin(self: &Arc<Self>) -> Result<ProviderPin, BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        let pins = state.pins.checked_add(1).ok_or(BudgetUnavailable)?;
        // Admit both the Arc guard and its immutable accounting owner first.
        let charge = self.budget.reserve(PIN_ALLOCATION_BYTES)?.commit()?;
        state.accounting.pin_admitted()?;
        state.pins = pins;
        Ok(ProviderPin(Arc::new(PinGuard {
            owner: self.clone(),
            _charge: charge,
        })))
    }

    pub(super) fn is_active(&self) -> Result<bool, BudgetUnavailable> {
        self.state
            .lock()
            .map(|state| state.phase == Phase::Active)
            .map_err(|_| BudgetUnavailable)
    }

    /// None protects a capture; Some(true) is already retiring, Some(false)
    /// still needs the registry's minimum-retention proof before transition.
    pub(super) fn retirement_candidate(&self) -> Result<Option<bool>, BudgetUnavailable> {
        let state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase == Phase::Refused {
            return Err(BudgetUnavailable);
        }
        if state.pins != 0 {
            return Ok(None);
        }
        Ok(Some(state.phase == Phase::Retiring))
    }

    /// Pressure eviction additionally proves retention/cursor obligations at
    /// the registry boundary. This closes admission atomically with pin census.
    pub(super) fn retire_if_unpinned(&self) -> Result<bool, BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active || state.pins != 0 {
            return Ok(false);
        }
        state.accounting.retire()?;
        state.phase = Phase::Retiring;
        Ok(true)
    }

    /// Exact supersession removes discovery even while captured work lives.
    /// Existing pins retain ownership; they cannot publish into retired state.
    pub(super) fn retire_superseded(&self) -> Result<(), BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase == Phase::Refused {
            return Err(BudgetUnavailable);
        }
        state.accounting.retire()?;
        state.phase = Phase::Retiring;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct ProviderPin(Arc<PinGuard>);

impl ProviderPin {
    pub(super) fn ensure_active(&self) -> Result<(), BudgetUnavailable> {
        if self.0.owner.is_active()? {
            Ok(())
        } else {
            Err(BudgetUnavailable)
        }
    }
    /// Lock order is registry -> owner -> slot. Keep this guard alive through
    /// the selection change; no registry lock may be acquired while it lives.
    pub(super) fn publication(&self) -> Result<ProviderPublication<'_>, BudgetUnavailable> {
        let state = self.0.owner.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        Ok(ProviderPublication { _state: state })
    }

    #[cfg(test)]
    pub(super) fn publish<T>(&self, select: impl FnOnce() -> T) -> Result<T, BudgetUnavailable> {
        let _publication = self.publication()?;
        Ok(select())
    }
}

pub(super) struct ProviderPublication<'a> {
    _state: MutexGuard<'a, State>,
}

pub(super) trait OwnedProviderSlot {
    fn provider_owner(&self) -> &Arc<ProviderOwner>;
}

/// Capture retains both the actual slot and its admission pin. Data drops
/// before the pin, so pressure cannot observe a released capture too early.
pub(super) struct CapturedProviderSlot<T> {
    slot: Arc<T>,
    pub(super) pin: ProviderPin,
}

impl<T: OwnedProviderSlot> CapturedProviderSlot<T> {
    pub(super) fn capture(slot: &Arc<T>) -> Result<Self, BudgetUnavailable> {
        let pin = slot.provider_owner().pin()?;
        Ok(Self {
            slot: Arc::clone(slot),
            pin,
        })
    }
}

impl<T> Deref for CapturedProviderSlot<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.slot.as_ref()
    }
}

struct PinGuard {
    owner: Arc<ProviderOwner>,
    _charge: Arc<OwnershipCharge>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pins) = state.pins.checked_sub(1) {
            if state.accounting.release_pin().is_err() {
                state.phase = Phase::Refused;
            }
            state.pins = pins;
        } else {
            state.phase = Phase::Refused;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_counts_track_unique_pin_guards_and_final_owner_destruction() {
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        let first = owner.pin().unwrap();
        let alias = first.clone();
        let second = owner.pin().unwrap();
        let counts = budget.observation().unwrap();
        assert_eq!(
            (
                counts.active_owners,
                counts.retiring_owners,
                counts.pinned_owners,
                counts.pins,
                counts.released_owners
            ),
            (1, 0, 1, 2, 0)
        );
        owner.retire_superseded().unwrap();
        assert!(owner.pin().is_err());
        drop(first);
        assert_eq!(budget.observation().unwrap().pins, 2);
        drop(alias);
        assert_eq!(budget.observation().unwrap().pins, 1);
        drop(owner);
        let retained = budget.observation().unwrap();
        assert_eq!(
            (
                retained.active_owners,
                retained.retiring_owners,
                retained.pinned_owners,
                retained.pins,
                retained.released_owners
            ),
            (0, 1, 1, 1, 0)
        );
        assert!(retained.owned_bytes > 0);
        drop(second);
        let released = budget.observation().unwrap();
        assert_eq!(
            (
                released.active_owners,
                released.retiring_owners,
                released.pinned_owners,
                released.pins,
                released.released_owners,
                released.owned_bytes
            ),
            (0, 0, 0, 0, 1, 0)
        );
    }

    #[test]
    fn captured_slot_keeps_data_alive_and_prevents_pressure_retirement() {
        struct Slot {
            data: Vec<u8>,
            owner: Arc<ProviderOwner>,
        }
        impl OwnedProviderSlot for Slot {
            fn provider_owner(&self) -> &Arc<ProviderOwner> {
                &self.owner
            }
        }
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        let slot = Arc::new(Slot {
            data: vec![7; 1024],
            owner: owner.clone(),
        });
        let capture = CapturedProviderSlot::capture(&slot).unwrap();
        drop(slot);
        assert_eq!(capture.data.len(), 1024);
        assert!(!owner.retire_if_unpinned().unwrap());
        owner.retire_superseded().unwrap();
        assert!(CapturedProviderSlot::capture(&capture.slot).is_err());
        assert!(capture.pin.publication().is_err());
        drop(owner);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            8192 + PIN_ALLOCATION_BYTES
        );
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn pin_charge_covers_both_actual_ownership_allocations() {
        use riffdb_types::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1};
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let scratch = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
            inspection.allocation(
                std::mem::size_of::<PinGuard>() + 2 * std::mem::size_of::<usize>(),
                std::mem::align_of::<PinGuard>(),
            )?;
            inspection.allocation(
                std::mem::size_of::<OwnershipCharge>() + 2 * std::mem::size_of::<usize>(),
                std::mem::align_of::<OwnershipCharge>(),
            )
        })
        .unwrap();
        let minimum = report.unshared_bytes();
        drop(report);
        drop(scratch);
        let before = budget.observation().unwrap().owned_bytes;
        let pin = owner.pin().unwrap();
        assert!(budget.observation().unwrap().owned_bytes - before >= minimum);
        drop(pin);
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn ownership_cannot_attach_a_charge_from_another_budget() {
        let budget = ProviderBudget::new();
        let foreign = ProviderBudget::new();
        let charge = foreign.reserve(1024).unwrap().commit().unwrap();
        assert!(ProviderOwner::new(budget.clone(), charge).is_err());
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn captured_owner_survives_discovery_removal_and_cannot_publish_after_retirement() {
        let budget = ProviderBudget::new();
        let charge = budget.reserve(1024).unwrap().commit().unwrap();
        let owner = ProviderOwner::new(budget.clone(), charge).unwrap();
        let captured = owner.pin().unwrap();
        let continuation = captured.clone();
        assert!(!owner.retire_if_unpinned().unwrap());
        owner.retire_superseded().unwrap();
        assert!(owner.pin().is_err());
        let mut published = false;
        assert!(captured.publish(|| published = true).is_err());
        assert!(!published);
        drop(owner);
        drop(captured);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            1024 + PIN_ALLOCATION_BYTES
        );
        drop(continuation);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn pressure_retirement_admits_only_after_last_pin_releases() {
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let pin = owner.pin().unwrap();
        assert_eq!(pin.publish(|| 42), Ok(42));
        assert!(!owner.retire_if_unpinned().unwrap());
        drop(pin);
        assert!(owner.retire_if_unpinned().unwrap());
        assert!(owner.pin().is_err());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn retirement_serializes_with_an_already_admitted_publication() {
        use std::sync::mpsc;
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let pin = owner.pin().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (finish_tx, finish_rx) = mpsc::sync_channel(0);
        let publisher = std::thread::spawn(move || {
            pin.publish(|| {
                entered_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                42
            })
        });
        entered_rx.recv().unwrap();
        let retiring = owner.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let retire = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            retiring.retire_superseded()
        });
        ready_rx.recv().unwrap();
        finish_tx.send(()).unwrap();
        assert_eq!(publisher.join().unwrap(), Ok(42));
        assert_eq!(retire.join().unwrap(), Ok(()));
        assert!(owner.pin().is_err());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod artifact_tests {
    use super::*;
    use crate::exact_provider_checkpoint_custody::{
        CheckpointCustody, CheckpointManifest, create_private_directory,
    };
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn retiring_worker_records_resources_but_cannot_publish_or_clean_while_pinned() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("epoch.rxts");
        let directory = create_private_directory(root.path()).unwrap();
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        owner
            .bind_artifact(CheckpointManifest::empty(directory, path.file_name().unwrap()).unwrap())
            .unwrap();
        let pin = owner.pin().unwrap();
        let alias = pin.clone();
        let (binding, initial) = owner.artifact_binding().unwrap();
        assert!(initial);
        CheckpointCustody::open_bound(&path, &binding, initial)
            .unwrap()
            .publish_tracked(
                b"complete derived bytes",
                || owner.retire_superseded().unwrap(),
                || {},
                |manifest| {
                    owner
                        .record_artifact(manifest)
                        .map_err(|_| std::io::Error::other("fixture custody"))
                },
            )
            .unwrap();
        assert!(!owner.artifact_binding().unwrap().1);
        assert!(pin.publication().is_err());
        assert!(owner.pin().is_err());
        assert!(owner.cleanup_manifest().is_err());
        drop(pin);
        assert!(owner.cleanup_manifest().is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"complete derived bytes");
        drop(alias);
        let manifest = owner.cleanup_manifest().unwrap().unwrap();
        CheckpointCustody::open_for_cleanup(&path, &manifest)
            .unwrap()
            .remove_owned()
            .unwrap();
        assert!(!path.exists());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn cancelled_publication_keeps_its_claimed_partial_object_for_safe_retirement() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("epoch.rxts");
        let directory = create_private_directory(root.path()).unwrap();
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        owner
            .bind_artifact(CheckpointManifest::empty(directory, path.file_name().unwrap()).unwrap())
            .unwrap();
        let pin = owner.pin().unwrap();
        let (binding, initial) = owner.artifact_binding().unwrap();
        let mut records = 0;
        let result = CheckpointCustody::open_bound(&path, &binding, initial)
            .unwrap()
            .publish_tracked(
                b"never written bytes",
                || {},
                || {},
                |manifest| {
                    owner.record_artifact(manifest).unwrap();
                    records += 1;
                    if records == 2 {
                        Err(std::io::Error::other("fixture cancellation"))
                    } else {
                        Ok(())
                    }
                },
            );
        assert!(result.is_err());
        assert!(path.with_extension("pending").exists());
        assert!(!path.exists());
        drop(pin);
        assert!(owner.retire_if_unpinned().unwrap());
        let manifest = owner.cleanup_manifest().unwrap().unwrap();
        CheckpointCustody::open_for_cleanup(&path, &manifest)
            .unwrap()
            .remove_owned()
            .unwrap();
        assert!(!path.with_extension("pending").exists());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
