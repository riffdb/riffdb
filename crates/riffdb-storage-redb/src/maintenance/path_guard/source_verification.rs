//! Complete parent-directory verification driver shared by ordinary and Source paths.
//! Ordinary verification uses std path metadata and canonicalization. Source
//! verification uses prospectively admitted fixed byte buffers and borrowed CStr
//! syscalls on the reviewed Linux-raw profile without allocating or mutating cwd.

#[cfg(unix)]
use super::UnixFileIdentity;
use super::{Component, Path, PathBuf, PinnedDirectory, StorageError, corrupt, io_unavailable};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use super::{StorageErrorKind, storage_error};
use std::fs;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use std::{
    ffi::{CStr, OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use riffdb_types::exact_provider_budget_v1::{
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ObservedPathMetadata {
    pub(super) is_dir: bool,
    pub(super) is_symlink: bool,
    #[cfg(unix)]
    pub(super) identity: UnixFileIdentity,
}

impl ObservedPathMetadata {
    fn from_std(metadata: &fs::Metadata) -> Self {
        let file_type = metadata.file_type();
        Self {
            is_dir: file_type.is_dir(),
            is_symlink: file_type.is_symlink(),
            #[cfg(unix)]
            identity: UnixFileIdentity::from_std_metadata(metadata),
        }
    }
}

pub(super) trait ParentVerificationBackend {
    fn reset_prefix(&mut self);
    fn push_prefix_component(&mut self, component: Component<'_>) -> Result<(), StorageError>;
    fn lstat_current_prefix(&mut self) -> Result<Option<ObservedPathMetadata>, StorageError>;
    fn lstat_original_path(&mut self, path: &Path) -> Result<ObservedPathMetadata, StorageError>;
    fn verify_canonical_path(&mut self, expected: &PinnedDirectory) -> Result<(), StorageError>;
    fn before_retained_check(&mut self, _expected: &PinnedDirectory) -> Result<(), StorageError> {
        Ok(())
    }
    fn verify_post_retained(
        &mut self,
        _expected: &PinnedDirectory,
        _initial_metadata: ObservedPathMetadata,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct OrdinaryVerificationBackend {
    prefix: PathBuf,
}

impl ParentVerificationBackend for OrdinaryVerificationBackend {
    fn reset_prefix(&mut self) {
        self.prefix.clear();
    }

    fn push_prefix_component(&mut self, component: Component<'_>) -> Result<(), StorageError> {
        self.prefix.push(component.as_os_str());
        Ok(())
    }

    fn lstat_current_prefix(&mut self) -> Result<Option<ObservedPathMetadata>, StorageError> {
        match fs::symlink_metadata(&self.prefix) {
            Ok(metadata) => Ok(Some(ObservedPathMetadata::from_std(&metadata))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
        }
    }

    fn lstat_original_path(&mut self, path: &Path) -> Result<ObservedPathMetadata, StorageError> {
        let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
        Ok(ObservedPathMetadata::from_std(&metadata))
    }

    fn verify_canonical_path(&mut self, expected: &PinnedDirectory) -> Result<(), StorageError> {
        if fs::canonicalize(&expected.path).map_err(io_unavailable)? != expected.canonical_path {
            return Err(corrupt());
        }
        Ok(())
    }
}

pub(super) fn reject_symlink_components_with(
    path: &Path,
    backend: &mut impl ParentVerificationBackend,
) -> Result<(), StorageError> {
    backend.reset_prefix();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {}
            Component::CurDir | Component::ParentDir => return Err(corrupt()),
        }
        backend.push_prefix_component(component)?;
        match backend.lstat_current_prefix()? {
            Some(metadata) if metadata.is_symlink => return Err(corrupt()),
            Some(_) => {}
            None => break,
        }
    }
    Ok(())
}

impl PinnedDirectory {
    pub(super) fn verify_with_backend(
        &self,
        backend: &mut impl ParentVerificationBackend,
    ) -> Result<(), StorageError> {
        reject_symlink_components_with(&self.path, backend)?;
        let metadata = backend.lstat_original_path(&self.path)?;
        if !metadata.is_dir {
            return Err(corrupt());
        }
        backend.verify_canonical_path(self)?;
        backend.before_retained_check(self)?;
        self.verify_retained()?;
        #[cfg(unix)]
        if metadata.identity != self.identity {
            return Err(corrupt());
        }
        backend.verify_post_retained(self, metadata)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[derive(Clone, Copy)]
struct SourceParentVerificationCapacities {
    prefix_capacity: usize,
    cwd_capacity: usize,
    canonical_capacity: usize,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) struct SourceParentVerificationWorkV1 {
    prefix_nul: Vec<u8>,
    cwd_bytes: Vec<u8>,
    canonical_bytes: Vec<u8>,
    observed_cwd: Option<(UnixFileIdentity, usize)>,
    _work: ProjectionSourceWorkReservationV1,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl Drop for SourceParentVerificationWorkV1 {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.prefix_nul));
        drop(std::mem::take(&mut self.cwd_bytes));
        drop(std::mem::take(&mut self.canonical_bytes));
        #[cfg(test)]
        record_verification_drop_for_test(
            self.prefix_nul.capacity(),
            self.cwd_bytes.capacity(),
            self.canonical_bytes.capacity(),
            self._work.bytes(),
        );
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl SourceParentVerificationWorkV1 {
    pub(crate) fn from_reservation(
        parent: &PinnedDirectory,
        work: ProjectionSourceWorkReservationV1,
    ) -> Result<Self, StorageError> {
        let expected_bytes = parent.source_parent_verification_bytes()?;
        if work.bytes() != expected_bytes {
            return Err(unavailable());
        }
        let caps = parent.source_parent_verification_capacities()?;
        let mut owner = Self {
            prefix_nul: Vec::new(),
            cwd_bytes: Vec::new(),
            canonical_bytes: Vec::new(),
            observed_cwd: None,
            _work: work,
        };
        owner
            .prefix_nul
            .try_reserve_exact(caps.prefix_capacity)
            .map_err(|_| unavailable())?;
        owner
            .cwd_bytes
            .try_reserve_exact(caps.cwd_capacity)
            .map_err(|_| unavailable())?;
        owner
            .canonical_bytes
            .try_reserve_exact(caps.canonical_capacity)
            .map_err(|_| unavailable())?;
        if owner.prefix_nul.capacity() != caps.prefix_capacity
            || owner.cwd_bytes.capacity() != caps.cwd_capacity
            || owner.canonical_bytes.capacity() != caps.canonical_capacity
        {
            return Err(unavailable());
        }
        owner.cwd_bytes.resize(caps.cwd_capacity, 0);
        Ok(owner)
    }

    pub(crate) fn reserve_for(
        parent: &PinnedDirectory,
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, StorageError> {
        let bytes = parent.source_parent_verification_bytes()?;
        let work = budget.reserve(bytes).map_err(|_| unavailable())?;
        Self::from_reservation(parent, work)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl ParentVerificationBackend for SourceParentVerificationWorkV1 {
    fn reset_prefix(&mut self) {
        self.prefix_nul.clear();
    }

    fn push_prefix_component(&mut self, component: Component<'_>) -> Result<(), StorageError> {
        let max_prefix_bytes = self
            .prefix_nul
            .capacity()
            .checked_sub(1)
            .ok_or_else(unavailable)?;
        match component {
            Component::RootDir => set_cstr_root(&mut self.prefix_nul, max_prefix_bytes),
            Component::Normal(part) => append_cstr_component(
                &mut self.prefix_nul,
                max_prefix_bytes,
                part.as_bytes(),
                unavailable(),
            ),
            Component::Prefix(_) | Component::CurDir | Component::ParentDir => Err(corrupt()),
        }
    }

    fn lstat_current_prefix(&mut self) -> Result<Option<ObservedPathMetadata>, StorageError> {
        Ok(statat_cstr_optional(&self.prefix_nul)?.map(|stat| observed_from_stat(&stat)))
    }

    fn lstat_original_path(&mut self, path: &Path) -> Result<ObservedPathMetadata, StorageError> {
        let max_prefix_bytes = self
            .prefix_nul
            .capacity()
            .checked_sub(1)
            .ok_or_else(unavailable)?;
        set_cstr_exact(
            &mut self.prefix_nul,
            max_prefix_bytes,
            path.as_os_str().as_bytes(),
        )?;
        let stat = statat_cstr_required(&self.prefix_nul)?;
        Ok(observed_from_stat(&stat))
    }

    fn verify_canonical_path(&mut self, expected: &PinnedDirectory) -> Result<(), StorageError> {
        let caps = expected.source_parent_verification_capacities()?;
        if self.prefix_nul.capacity() != caps.prefix_capacity
            || self.cwd_bytes.capacity() != caps.cwd_capacity
            || self.canonical_bytes.capacity() != caps.canonical_capacity
        {
            return Err(unavailable());
        }
        let max_prefix_bytes = caps
            .prefix_capacity
            .checked_sub(1)
            .ok_or_else(unavailable)?;
        let max_canonical_bytes = caps
            .canonical_capacity
            .checked_sub(1)
            .ok_or_else(unavailable)?;
        self.observed_cwd = None;
        self.canonical_bytes.clear();

        if expected.path.is_relative() {
            self.cwd_bytes.resize(caps.cwd_capacity, 0);
            let cwd_total_len =
                rustix::process::getcwd_raw(&mut self.cwd_bytes[..]).map_err(|_| unavailable())?;
            let cwd_init = self
                .cwd_bytes
                .get(..cwd_total_len)
                .ok_or_else(unavailable)?;
            if cwd_total_len < 2
                || cwd_total_len > caps.cwd_capacity
                || cwd_init.last().copied() != Some(0)
            {
                return Err(unavailable());
            }
            let cwd_without_nul = cwd_init
                .get(..cwd_total_len.checked_sub(1).ok_or_else(unavailable)?)
                .ok_or_else(unavailable)?;
            if cwd_without_nul.contains(&0) || cwd_without_nul.first().copied() != Some(b'/') {
                return Err(unavailable());
            }
            let dot_stat =
                rustix::fs::statat(rustix::fs::CWD, c".", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|_| unavailable())?;
            let dot_meta = observed_from_stat(&dot_stat);
            if !dot_meta.is_dir || dot_stat.st_nlink == 0 {
                return Err(unavailable());
            }
            set_cstr_root(&mut self.canonical_bytes, max_canonical_bytes)?;
            let root_stat = statat_cstr_required(&self.canonical_bytes)?;
            let root_meta = observed_from_stat(&root_stat);
            if root_meta.is_symlink || !root_meta.is_dir || root_stat.st_nlink == 0 {
                return Err(unavailable());
            }
            let mut last_cwd_meta = root_meta;
            for component in Path::new(OsStr::from_bytes(cwd_without_nul)).components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(part) => {
                        append_cstr_component(
                            &mut self.canonical_bytes,
                            max_canonical_bytes,
                            part.as_bytes(),
                            unavailable(),
                        )?;
                        let stat = statat_cstr_required(&self.canonical_bytes)?;
                        let meta = observed_from_stat(&stat);
                        if meta.is_symlink || !meta.is_dir || stat.st_nlink == 0 {
                            return Err(unavailable());
                        }
                        last_cwd_meta = meta;
                    }
                    Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                        return Err(unavailable());
                    }
                }
            }
            if last_cwd_meta.identity != dot_meta.identity
                || borrowed_cstr(&self.canonical_bytes)?.to_bytes() != cwd_without_nul
            {
                return Err(unavailable());
            }
            self.observed_cwd = Some((dot_meta.identity, cwd_total_len));
            #[cfg(test)]
            run_race_hook_for_test(RACE_PHASE_AFTER_INITIAL_CWD_READ);
        }

        #[cfg(test)]
        run_race_hook_for_test(RACE_PHASE_BEFORE_CANONICAL_WALK);

        self.prefix_nul.clear();
        let mut last_target_meta: Option<ObservedPathMetadata> = None;
        for component in expected.path.components() {
            match component {
                Component::RootDir => {
                    set_cstr_root(&mut self.prefix_nul, max_prefix_bytes)?;
                    set_cstr_root(&mut self.canonical_bytes, max_canonical_bytes)?;
                    let stat = statat_cstr_required(&self.prefix_nul)?;
                    let meta = observed_from_stat(&stat);
                    if meta.is_symlink || !meta.is_dir {
                        return Err(corrupt());
                    }
                    last_target_meta = Some(meta);
                }
                Component::Normal(part) => {
                    append_cstr_component(
                        &mut self.prefix_nul,
                        max_prefix_bytes,
                        part.as_bytes(),
                        unavailable(),
                    )?;
                    let prefix_stat = statat_cstr_required(&self.prefix_nul)?;
                    let prefix_meta = observed_from_stat(&prefix_stat);
                    if prefix_meta.is_symlink {
                        return Err(corrupt());
                    }
                    append_cstr_component(
                        &mut self.canonical_bytes,
                        max_canonical_bytes,
                        part.as_bytes(),
                        corrupt(),
                    )?;
                    let canon_stat = statat_cstr_required(&self.canonical_bytes)?;
                    let canon_meta = observed_from_stat(&canon_stat);
                    if canon_meta.is_symlink || canon_meta != prefix_meta {
                        return Err(corrupt());
                    }
                    last_target_meta = Some(canon_meta);
                }
                Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                    return Err(corrupt());
                }
            }
        }
        let final_meta = last_target_meta.ok_or_else(corrupt)?;
        if !final_meta.is_dir || final_meta.identity != expected.identity {
            return Err(corrupt());
        }
        if borrowed_cstr(&self.canonical_bytes)?.to_bytes()
            != expected.canonical_path.as_os_str().as_bytes()
        {
            return Err(corrupt());
        }
        Ok(())
    }

    fn before_retained_check(&mut self, _expected: &PinnedDirectory) -> Result<(), StorageError> {
        #[cfg(test)]
        run_race_hook_for_test(RACE_PHASE_BEFORE_RETAINED_CHECK);
        Ok(())
    }

    fn verify_post_retained(
        &mut self,
        expected: &PinnedDirectory,
        initial_metadata: ObservedPathMetadata,
    ) -> Result<(), StorageError> {
        let after_metadata = self.lstat_original_path(&expected.path)?;
        if !after_metadata.is_dir
            || after_metadata.identity != expected.identity
            || after_metadata != initial_metadata
        {
            return Err(corrupt());
        }
        if let Some((initial_cwd_identity, cwd_total_len)) = self.observed_cwd {
            let dot_after =
                rustix::fs::statat(rustix::fs::CWD, c".", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|_| unavailable())?;
            let dot_after_meta = observed_from_stat(&dot_after);
            if !dot_after_meta.is_dir || dot_after.st_nlink == 0 {
                return Err(unavailable());
            }
            if dot_after_meta.identity != initial_cwd_identity {
                return Err(corrupt());
            }
            let cap = self.canonical_bytes.capacity();
            self.canonical_bytes.resize(cap, 0);
            let after_cwd_len = rustix::process::getcwd_raw(&mut self.canonical_bytes[..])
                .map_err(|_| unavailable())?;
            let after_cwd = self
                .canonical_bytes
                .get(..after_cwd_len)
                .ok_or_else(unavailable)?;
            let expected_cwd = self
                .cwd_bytes
                .get(..cwd_total_len)
                .ok_or_else(unavailable)?;
            if after_cwd != expected_cwd {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl PinnedDirectory {
    fn source_parent_verification_capacities(
        &self,
    ) -> Result<SourceParentVerificationCapacities, StorageError> {
        if !is_getcwd_raw_supported() {
            return Err(unavailable());
        }
        let path_bytes = self.path.as_os_str().as_bytes();
        let canonical_bytes = self.canonical_path.as_os_str().as_bytes();
        if path_bytes.is_empty()
            || path_bytes.len() > 4096
            || path_bytes.contains(&0)
            || canonical_bytes.is_empty()
            || canonical_bytes.len() > 4096
            || canonical_bytes.contains(&0)
            || canonical_bytes.first().copied() != Some(b'/')
        {
            return Err(unavailable());
        }
        let prefix_capacity = path_bytes.len().checked_add(1).ok_or_else(unavailable)?;
        let cwd_capacity = canonical_bytes
            .len()
            .checked_add(1)
            .ok_or_else(unavailable)?;
        let canonical_capacity = canonical_bytes
            .len()
            .checked_add(1)
            .ok_or_else(unavailable)?;
        Ok(SourceParentVerificationCapacities {
            prefix_capacity,
            cwd_capacity,
            canonical_capacity,
        })
    }

    pub(crate) fn source_parent_verification_bytes(&self) -> Result<u64, StorageError> {
        let caps = self.source_parent_verification_capacities()?;
        let mut bytes = u64::try_from(std::mem::size_of::<SourceParentVerificationWorkV1>())
            .map_err(|_| unavailable())?;
        for cap in [
            caps.prefix_capacity,
            caps.cwd_capacity,
            caps.canonical_capacity,
        ] {
            bytes = bytes
                .checked_add(
                    riffdb_types::owned_allocation_capacity_bound_v1(cap, 1)
                        .map_err(|_| unavailable())?,
                )
                .ok_or_else(unavailable)?;
        }
        Ok(bytes)
    }

    /// Duplicates retained capabilities without reopening an ambient path.
    pub(crate) fn source_retained_clone_bytes(&self) -> Result<u64, StorageError> {
        let _ = self.source_parent_verification_capacities()?;
        let paths = [
            self.path.as_os_str().len(),
            self.canonical_path.as_os_str().len(),
        ];
        let mut bytes = u64::try_from(std::mem::size_of::<Self>()).map_err(|_| unavailable())?;
        for len in paths {
            bytes = bytes
                .checked_add(
                    riffdb_types::owned_allocation_capacity_bound_v1(len, 1)
                        .map_err(|_| unavailable())?,
                )
                .ok_or_else(unavailable)?;
        }
        Ok(bytes)
    }

    pub(crate) fn verify_source_with(
        &self,
        work: &mut SourceParentVerificationWorkV1,
    ) -> Result<(), StorageError> {
        self.verify_with_backend(work)
    }

    pub(crate) fn source_retained_clone_with(
        &self,
        work: &mut SourceParentVerificationWorkV1,
    ) -> Result<Self, StorageError> {
        self.verify_source_with(work)?;
        let retained = Self {
            path: clone_path_exact(&self.path)?,
            canonical_path: clone_path_exact(&self.canonical_path)?,
            directory: self.directory.try_clone().map_err(io_unavailable)?,
            sync_handle: self.sync_handle.try_clone().map_err(io_unavailable)?,
            identity: self.identity,
        };
        retained.verify_source_with(work)?;
        self.verify_source_with(work)?;
        Ok(retained)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn is_getcwd_raw_supported() -> bool {
    #[cfg(test)]
    if FORCE_UNSUPPORTED_GETCWD_RAW.with(std::cell::Cell::get) {
        return false;
    }
    rustix::process::GETCWD_RAW_SUPPORTED
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn clone_path_exact(path: &Path) -> Result<PathBuf, StorageError> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.len() > 4096 || bytes.contains(&0) {
        return Err(unavailable());
    }
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| unavailable())?;
    if owned.capacity() != bytes.len() {
        return Err(unavailable());
    }
    owned.extend_from_slice(bytes);
    Ok(PathBuf::from(OsString::from_vec(owned)))
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn set_cstr_root(buf: &mut Vec<u8>, max_path_bytes: usize) -> Result<(), StorageError> {
    if max_path_bytes < 1 || buf.capacity() < 2 {
        return Err(unavailable());
    }
    buf.clear();
    buf.push(b'/');
    buf.push(0);
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn set_cstr_exact(
    buf: &mut Vec<u8>,
    max_path_bytes: usize,
    bytes: &[u8],
) -> Result<(), StorageError> {
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(corrupt());
    }
    let total_len = bytes.len().checked_add(1).ok_or_else(unavailable)?;
    if bytes.len() > max_path_bytes || total_len > buf.capacity() {
        return Err(unavailable());
    }
    buf.clear();
    buf.extend_from_slice(bytes);
    buf.push(0);
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn append_cstr_component(
    buf: &mut Vec<u8>,
    max_path_bytes: usize,
    part: &[u8],
    overflow_error: StorageError,
) -> Result<(), StorageError> {
    if part.is_empty() || part.contains(&0) || part.contains(&b'/') {
        return Err(corrupt());
    }
    if buf.last().copied() == Some(0) {
        buf.pop();
    }
    let sep_len = usize::from(!buf.is_empty() && buf.as_slice() != b"/");
    let new_path_len = match buf
        .len()
        .checked_add(sep_len)
        .and_then(|len| len.checked_add(part.len()))
    {
        Some(len) => len,
        None => return Err(overflow_error),
    };
    let new_total_len = match new_path_len.checked_add(1) {
        Some(len) => len,
        None => return Err(overflow_error),
    };
    if new_path_len > max_path_bytes || new_total_len > buf.capacity() {
        return Err(overflow_error);
    }
    if sep_len == 1 {
        buf.push(b'/');
    }
    buf.extend_from_slice(part);
    buf.push(0);
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn borrowed_cstr(buf: &[u8]) -> Result<&CStr, StorageError> {
    CStr::from_bytes_with_nul(buf).map_err(|_| corrupt())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn statat_cstr_optional(buf: &[u8]) -> Result<Option<rustix::fs::Stat>, StorageError> {
    let cstr = borrowed_cstr(buf)?;
    match rustix::fs::statat(rustix::fs::CWD, cstr, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(_) => Err(unavailable()),
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn statat_cstr_required(buf: &[u8]) -> Result<rustix::fs::Stat, StorageError> {
    statat_cstr_optional(buf)?.ok_or_else(unavailable)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn observed_from_stat(stat: &rustix::fs::Stat) -> ObservedPathMetadata {
    let file_type = rustix::fs::FileType::from_raw_mode(stat.st_mode);
    ObservedPathMetadata {
        is_dir: file_type == rustix::fs::FileType::Directory,
        is_symlink: file_type == rustix::fs::FileType::Symlink,
        identity: UnixFileIdentity {
            device: stat.st_dev,
            inode: stat.st_ino,
        },
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
pub(crate) const RACE_PHASE_AFTER_INITIAL_CWD_READ: u8 = 1;
#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
pub(crate) const RACE_PHASE_BEFORE_CANONICAL_WALK: u8 = 2;
#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
pub(crate) const RACE_PHASE_BEFORE_RETAINED_CHECK: u8 = 3;

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
thread_local! {
    static FORCE_UNSUPPORTED_GETCWD_RAW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LAST_VERIFICATION_DROP_RECORD: std::cell::Cell<Option<(usize, usize, usize, u64)>> =
        const { std::cell::Cell::new(None) };
    #[allow(clippy::type_complexity)]
    static RACE_HOOK: std::cell::RefCell<Option<(u8, Box<dyn FnOnce()>)>> =
        std::cell::RefCell::new(None);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
fn record_verification_drop_for_test(
    prefix_cap: usize,
    cwd_cap: usize,
    canonical_cap: usize,
    work_bytes: u64,
) {
    LAST_VERIFICATION_DROP_RECORD.with(|slot| {
        slot.set(Some((prefix_cap, cwd_cap, canonical_cap, work_bytes)));
    });
}

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
fn run_race_hook_for_test(phase: u8) {
    let hook = RACE_HOOK.with(|slot| {
        let mut borrow = slot.borrow_mut();
        if borrow
            .as_ref()
            .is_some_and(|(expected, _)| *expected == phase)
        {
            borrow.take().map(|(_, callback)| callback)
        } else {
            None
        }
    });
    if let Some(callback) = hook {
        callback();
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
impl PinnedDirectory {
    pub(crate) fn verify_source_for_test(
        &self,
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<(), StorageError> {
        let mut work = SourceParentVerificationWorkV1::reserve_for(self, budget)?;
        self.verify_source_with(&mut work)
    }

    pub(crate) fn verify_source_with_race_hook_for_test(
        &self,
        budget: &ProjectionSourceWorkingBudgetV1,
        phase: u8,
        callback: impl FnOnce() + 'static,
    ) -> Result<(), StorageError> {
        let mut work = SourceParentVerificationWorkV1::reserve_for(self, budget)?;
        RACE_HOOK.with(|slot| {
            *slot.borrow_mut() = Some((phase, Box::new(callback)));
        });
        let result = self.verify_source_with(&mut work);
        RACE_HOOK.with(|slot| {
            *slot.borrow_mut() = None;
        });
        result
    }

    pub(crate) fn source_retained_clone_for_test(
        &self,
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, StorageError> {
        let mut work = SourceParentVerificationWorkV1::reserve_for(self, budget)?;
        self.source_retained_clone_with(&mut work)
    }

    pub(crate) fn with_test_paths_from(&self, path: PathBuf, canonical_path: PathBuf) -> Self {
        Self {
            path,
            canonical_path,
            directory: self.directory.try_clone().unwrap(),
            sync_handle: self.sync_handle.try_clone().unwrap(),
            identity: self.identity,
        }
    }

    pub(crate) fn set_force_unsupported_getcwd_raw_for_test(force: bool) {
        FORCE_UNSUPPORTED_GETCWD_RAW.with(|slot| slot.set(force));
    }

    pub(crate) fn take_last_verification_drop_record_for_test() -> Option<(usize, usize, usize, u64)>
    {
        LAST_VERIFICATION_DROP_RECORD.with(|slot| slot.take())
    }
}
