//! Numerical descriptor traversal under the caller's already admitted scratch.
//! Linux fd-only std/cap-std leaves are covered by their containing inline layout.
//! No descriptor clone, file read, semantic decode or authority is created here.
use riffdb_types::{OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1};
use std::mem::{align_of, size_of};
use std::{
    ffi::OsString,
    fs::File,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

type Result<T> = std::result::Result<T, OwnedCapacityInspectionErrorV1>;
#[repr(C)]
struct ArcLayout<T> {
    counts: [usize; 2],
    value: T,
}

// These borrowed Arc fields are INTERNAL to the fixed whole-descriptor owner.
// Every actual block is charged; repeated aliases may conservatively overlap.
// No Arc, Weak, address or data pointer is returned to a Source consumer.
pub(crate) fn arc_block<T>(
    inspection: &mut OwnedCapacityInspectionV1<'_>,
    _: &Arc<T>,
) -> Result<()> {
    inspection.allocation(size_of::<ArcLayout<T>>(), align_of::<ArcLayout<T>>())
}
pub(crate) fn path(inspection: &mut OwnedCapacityInspectionV1<'_>, value: &PathBuf) -> Result<()> {
    inspection.allocation(value.capacity(), align_of::<u8>())
}
pub(crate) fn name(inspection: &mut OwnedCapacityInspectionV1<'_>, value: &OsString) -> Result<()> {
    inspection.allocation(value.capacity(), align_of::<u8>())
}
pub(crate) fn file(_: &mut OwnedCapacityInspectionV1<'_>, _: &File) -> Result<()> {
    // Qualified Unix std File is fd-only; its inline value is included in its
    // actual parent/Arc allocation. Other resource profiles refuse, not guess.
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        Ok(())
    } else {
        Err(OwnedCapacityInspectionErrorV1)
    }
}
pub(crate) fn directory(_: &mut OwnedCapacityInspectionV1<'_>, _: &cap_std::fs::Dir) -> Result<()> {
    // Pinned cap-std4.0.3 Dir contains only std_file:std::fs::File (audited source).
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        Ok(())
    } else {
        Err(OwnedCapacityInspectionErrorV1)
    }
}
pub(crate) fn atomic(_: &mut OwnedCapacityInspectionV1<'_>, _: &AtomicBool) -> Result<()> {
    Ok(())
}
