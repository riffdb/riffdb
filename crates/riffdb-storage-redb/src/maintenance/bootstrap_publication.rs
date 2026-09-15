//! Same-filesystem follower replacement. The private candidate survives retries;
//! the engine lock excludes startup until marker-last publication is durable.
use super::*;
use crate::maintenance::path_guard::check_current_marker;
use std::ffi::OsString;

/// Published file with its engine lock still held. This is not readiness: the
/// server must release it and run unchanged follower startup before tail apply.
pub struct RedbPublishedBootstrapCandidate {
    database: Database,
    candidate: RedbValidatedBootstrapCandidate,
    parent: PinnedDirectory,
    name: OsString,
    marker_name: OsString,
    marker: File,
}

impl RedbPublishedBootstrapCandidate {
    /// Exact source identity, unchanged by physical publication.
    pub fn manifest(&self) -> Manifest {
        self.candidate.manifest()
    }

    /// Checks the durable namespace once more before releasing engine exclusion.
    /// This grants no serving port and cannot skip the subsequent startup proof.
    pub fn release_for_startup(self) -> Result<Manifest, StorageError> {
        self.verify()?;
        self.parent.sync()?;
        self.verify()?;
        let manifest = self.manifest();
        drop(self);
        Ok(manifest)
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.parent.verify()?;
        self.candidate.owner.verify()?;
        if !self
            .parent
            .regular_file_matches(&self.name, &self.candidate.owner.file)?
            || !self
                .parent
                .regular_file_matches(&self.marker_name, &self.marker)?
        {
            return Err(corrupt());
        }
        // Retain and exercise the actual engine owner, not an auxiliary lock
        // that ordinary follower startup would fail to consult.
        drop(self.database.begin_read().map_err(unavailable)?);
        Ok(())
    }
}

impl RedbValidatedBootstrapCandidate {
    /// Publishes one already scrubbed physical candidate without changing its
    /// lineage or adding a source journal. Production server composition must
    /// first finish its sealed semantic projection rebuild. Only absent/empty
    /// targets or a closed, non-regressing follower of this database qualify.
    /// Existing private candidate links remain for crash retry; no full-file copy
    /// or population scan occurs at this boundary. Requires a shared filesystem.
    pub fn publish(self, path: &Path) -> Result<RedbPublishedBootstrapCandidate, StorageError> {
        self.owner.verify()?;
        let parent = PinnedDirectory::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
        let name = path.file_name().ok_or_else(corrupt)?.to_os_string();
        let marker_path = crate::durable_format_marker_path(path);
        let marker_name = marker_path.file_name().ok_or_else(corrupt)?.to_os_string();
        reject_source_sidecars(&parent, path)?;
        let mut marker = self
            .owner
            .directory
            .open_file(
                crate::durable_format_marker_path(&self.owner.path)
                    .file_name()
                    .ok_or_else(corrupt)?,
            )?
            .into_std();
        check_current_marker(&mut marker)?;
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(self.owner.file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        self.owner.verify()?;
        let read = database.begin_read().map_err(unavailable)?;
        if crate::changelog_v3_roots::read_checkpoint_roots(&read)?
            != Some(self.manifest().fence().history())
        {
            return Err(corrupt());
        }
        drop(read);
        // The candidate directory was private, but the configured parent may
        // be readable. Restrict the retained inode before linking it there.
        make_private(&self.owner.file)?;
        self.owner.file.sync_all().map_err(unavailable)?;
        let suffix: String = self
            .manifest()
            .fence()
            .hold_id()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let temporary = OsString::from(format!(".riffdb-bootstrap-{suffix}.redb"));
        let marker_temporary = OsString::from(format!(".riffdb-bootstrap-{suffix}.format"));
        if name == temporary
            || name == marker_temporary
            || marker_name == temporary
            || marker_name == marker_temporary
        {
            return Err(corrupt());
        }
        let recovering = parent.regular_file_matches(&temporary, &self.owner.file)?
            && parent.regular_file_matches(&marker_temporary, &marker)?;
        let same = parent.regular_file_matches(&name, &self.owner.file)?;
        let old = if same {
            None
        } else {
            lock_target(&parent, path, self.manifest(), recovering)?
        };
        let old_marker = if parent.regular_file_length(&marker_name)?.is_some() {
            Some(parent.open_file(&marker_name)?.into_std())
        } else {
            None
        };
        ensure_link(
            &self.owner.directory,
            OsStr::new(FILE),
            &parent,
            &temporary,
            &self.owner.file,
        )?;
        let source_marker = crate::durable_format_marker_path(&self.owner.path);
        ensure_link(
            &self.owner.directory,
            source_marker.file_name().ok_or_else(corrupt)?,
            &parent,
            &marker_temporary,
            &marker,
        )?;
        parent.sync()?;
        publication_edge("publication-prepared");
        parent.verify()?;
        self.owner.verify()?;
        reject_source_sidecars(&parent, path)?;
        let target_matches = if same {
            parent.regular_file_matches(&name, &self.owner.file)?
        } else {
            match &old {
                Some(old) => parent.regular_file_matches(&name, &old.file)?,
                None => parent.regular_file_length(&name)?.is_none(),
            }
        };
        let marker_matches = match &old_marker {
            Some(old) => parent.regular_file_matches(&marker_name, old)?,
            None => parent.regular_file_length(&marker_name)?.is_none(),
        };
        if !target_matches
            || !marker_matches
            || !parent.regular_file_matches(&temporary, &self.owner.file)?
            || !parent.regular_file_matches(&marker_temporary, &marker)?
        {
            return Err(corrupt());
        }
        // Absence is durable before the replacement bytes become visible.
        parent.remove_file_if_present(&marker_name)?;
        parent.sync()?;
        publication_edge("publication-marker-removed");
        if same {
            // POSIX rename between two names of the same inode is a no-op and
            // leaves both names. Remove only our proven temporary alias.
            parent.remove_file_if_present(&temporary)?;
        } else {
            parent.rename(&temporary, &name)?;
        }
        publication_edge("publication-database-renamed");
        parent.rename(&marker_temporary, &marker_name)?;
        publication_edge("publication-marker-renamed");
        parent.sync()?;
        let published = RedbPublishedBootstrapCandidate {
            database,
            candidate: self,
            parent,
            name,
            marker_name,
            marker,
        };
        published.verify()?;
        publication_edge("publication-parent-synced");
        drop(old);
        Ok(published)
    }
}

struct OldTarget {
    // Hold any old follower engine (or the empty-file lock) through publication.
    _database: Option<redb::ReadOnlyDatabase>,
    file: File,
}

fn lock_target(
    parent: &PinnedDirectory,
    path: &Path,
    manifest: Manifest,
    recovering: bool,
) -> Result<Option<OldTarget>, StorageError> {
    let name = path.file_name().ok_or_else(corrupt)?;
    let Some(length) = parent.regular_file_length(name)? else {
        if parent
            .regular_file_length(
                crate::durable_format_marker_path(path)
                    .file_name()
                    .ok_or_else(corrupt)?,
            )?
            .is_some()
        {
            return Err(corrupt());
        }
        return Ok(None);
    };
    let file = parent.open_file_read_write(name)?.into_std();
    if length == 0 {
        file.try_lock().map_err(unavailable)?;
        return Ok(Some(OldTarget {
            _database: None,
            file,
        }));
    }
    if !recovering
        && crate::preflight_durable_format_path(path).map_err(invalid)?
            != crate::RedbDurableFormatPreflight::OpenCurrent
    {
        return Err(corrupt());
    }
    let database = read_only_target(&file)?;
    crate::store::validate_follower_open(&database, path, &crate::media::RealJournalMedia)?;
    let read = database.begin_read().map_err(unavailable)?;
    let old = crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(corrupt)?;
    let new = manifest.fence().history();
    if old.lineage().database_id() != new.lineage().database_id()
        || old.lineage().history_incarnation() > new.lineage().history_incarnation()
        || old.lineage().leadership_epoch() > new.lineage().leadership_epoch()
        || (old.lineage().history_incarnation() == new.lineage().history_incarnation()
            && (old.lineage() != new.lineage() || !old.tail().precedes_or_equals(new.tail())))
    {
        return Err(corrupt());
    }
    drop(read);
    if !parent.regular_file_matches(name, &file)? {
        return Err(corrupt());
    }
    Ok(Some(OldTarget {
        _database: Some(database),
        file,
    }))
}

fn ensure_link(
    source: &PinnedDirectory,
    name: &OsStr,
    target: &PinnedDirectory,
    temporary: &OsStr,
    file: &File,
) -> Result<(), StorageError> {
    if !source.regular_file_matches(name, file)? {
        return Err(corrupt());
    }
    match target.regular_file_length(temporary)? {
        Some(_) if target.regular_file_matches(temporary, file)? => {}
        Some(_) => return Err(corrupt()),
        None => source.hard_link_to(name, target, temporary)?,
    }
    if !target.regular_file_matches(temporary, file)? {
        return Err(corrupt());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_only_target(file: &File) -> Result<redb::ReadOnlyDatabase, StorageError> {
    use std::os::fd::AsRawFd;
    // redb's read-only constructor accepts a path, not a retained File. The
    // kernel descriptor path binds it to this exact capability-opened inode,
    // even if the ambient name is replaced. Its shared engine lock excludes
    // every writer while classification and replacement run. A refused primary
    // therefore receives no redb repair or writable-open bookkeeping changes.
    Database::builder()
        .set_cache_size(CACHE_BYTES)
        .open_read_only(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .map_err(unavailable)
}

#[cfg(not(target_os = "linux"))]
fn read_only_target(_: &File) -> Result<redb::ReadOnlyDatabase, StorageError> {
    Err(storage_error(StorageErrorKind::IncompatibleFormat))
}

fn reject_source_sidecars(parent: &PinnedDirectory, path: &Path) -> Result<(), StorageError> {
    for sidecar in [
        crate::journal::journal_path(path),
        crate::journal::checkpoint_journal_path(path),
        crate::journal::spare_journal_path(path),
    ] {
        if parent
            .regular_file_length(sidecar.file_name().ok_or_else(corrupt)?)?
            .is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn make_private(file: &File) -> Result<(), StorageError> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(unavailable)
}
#[cfg(not(unix))]
fn make_private(_: &File) -> Result<(), StorageError> {
    Err(storage_error(StorageErrorKind::IncompatibleFormat))
}

fn publication_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_BOOTSTRAP_PUBLICATION_EDGE")
        .ok()
        .as_deref()
        == Some(_edge)
    {
        std::process::exit(93);
    }
}
