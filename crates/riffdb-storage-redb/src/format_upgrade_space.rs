//! Metadata-only, fail-closed admission before any offline upgrade mutation.

use super::RedbDurableFormatUpgradeError as Error;
use crate::format_preflight::RedbDurableFormatPreflight;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::path::Path;

const MAX_DIRECTORY_ENTRIES: usize = 16_384;
const MAX_SEALED_EXTENTS: usize = riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS - 3;

#[cfg(test)]
#[path = "format_upgrade_space_tests.rs"]
mod tests;

pub(super) fn admit(
    database: &Path,
    observed: RedbDurableFormatPreflight,
    factor: u8,
    observe: impl FnOnce(&File) -> Result<(u64, u64), Error>,
) -> Result<(), Error> {
    let parent = database
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !fs::symlink_metadata(parent)
        .map_err(|_| Error::Unavailable)?
        .is_dir()
    {
        return Err(Error::Unavailable);
    }
    let directory = File::open(parent).map_err(|_| Error::Unavailable)?;
    let source = source_bytes(database, observed)?;
    let (available_fragments, fragment_bytes) = observe(&directory)?;
    check_floor(source, factor, available_fragments, fragment_bytes)
}

pub(super) fn observe_available(directory: &File) -> Result<(u64, u64), Error> {
    #[cfg(unix)]
    {
        let stats = rustix::fs::fstatvfs(directory).map_err(|_| Error::Unavailable)?;
        Ok((stats.f_bavail, stats.f_frsize))
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
        Err(Error::Unavailable)
    }
}

fn check_floor(source: u64, factor: u8, fragments: u64, fragment_bytes: u64) -> Result<(), Error> {
    if source == 0 || factor == 0 || fragment_bytes == 0 {
        return Err(Error::Unavailable);
    }
    let required = source
        .checked_mul(u64::from(factor))
        .ok_or(Error::Unavailable)?;
    let available = fragments
        .checked_mul(fragment_bytes)
        .ok_or(Error::Unavailable)?;
    if available < required {
        Err(Error::InsufficientSpace)
    } else {
        Ok(())
    }
}

fn source_bytes(database: &Path, observed: RedbDurableFormatPreflight) -> Result<u64, Error> {
    let parent = database
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let entries = fs::read_dir(parent)
        .map_err(|_| Error::Unavailable)?
        .map(|entry| {
            let entry = entry.map_err(|_| Error::Unavailable)?;
            let name = entry.file_name();
            // Unrelated siblings count against discovery, but confer no source size.
            let metadata = if source_name(database, &name)?.is_some() {
                let metadata =
                    fs::symlink_metadata(entry.path()).map_err(|_| Error::Unavailable)?;
                Some((metadata.is_file(), metadata.len()))
            } else {
                None
            };
            Ok((name, metadata))
        });
    inventory_bytes(database, observed, entries)
}

fn inventory_bytes(
    database: &Path,
    observed: RedbDurableFormatPreflight,
    entries: impl IntoIterator<Item = Result<(OsString, Option<(bool, u64)>), Error>>,
) -> Result<u64, Error> {
    let mut count = 0usize;
    let mut sealed = 0usize;
    let mut fixed_seen = 0u16;
    let mut total = 0u64;
    for entry in entries {
        count = count.checked_add(1).ok_or(Error::Unavailable)?;
        if count > MAX_DIRECTORY_ENTRIES {
            return Err(Error::Unavailable);
        }
        let (name, metadata) = entry?;
        let Some(kind) = source_name(database, &name)? else {
            continue;
        };
        let Some((true, bytes)) = metadata else {
            return Err(Error::Unavailable);
        };
        match kind {
            SourceName::Fixed(index) => {
                let bit = 1u16 << index;
                if fixed_seen & bit != 0 {
                    return Err(Error::Unavailable);
                }
                fixed_seen |= bit;
            }
            SourceName::Sealed => {
                sealed = sealed.checked_add(1).ok_or(Error::Unavailable)?;
                if sealed > MAX_SEALED_EXTENTS {
                    return Err(Error::Unavailable);
                }
                // Canonical fixed-width names make the generation unique in
                // a directory. Alternate spellings never enter accounting.
            }
        }
        total = total.checked_add(bytes).ok_or(Error::Unavailable)?;
    }
    let marker_required = match observed {
        RedbDurableFormatPreflight::OfflineUpgradeRequired { current, .. } => {
            current.writer().get() != 0
        }
        RedbDurableFormatPreflight::OpenCurrent => true,
        _ => return Err(Error::UnsupportedAction),
    };
    if fixed_seen & 1 == 0 || (marker_required && fixed_seen & 2 == 0) || total == 0 {
        return Err(Error::Unavailable);
    }
    Ok(total)
}

enum SourceName {
    Fixed(u8),
    Sealed,
}

fn source_name(database: &Path, name: &OsStr) -> Result<Option<SourceName>, Error> {
    let base = database.file_name().ok_or(Error::Unavailable)?;
    if name == base {
        return Ok(Some(SourceName::Fixed(0)));
    }
    let Some(suffix) = name
        .as_encoded_bytes()
        .strip_prefix(base.as_encoded_bytes())
    else {
        return Ok(None);
    };
    // Include all retained lifecycle working files, even non-authoritative
    // spare/recycled and interrupted publication staging files. Their logical
    // lengths consume the same filesystem and must not understate the floor.
    const FIXED: [&[u8]; 10] = [
        b".riffdb-format-v1",
        b".riffdb-format-upgrade-v1",
        b".riffjournal",
        b".riffjournal.checkpoint",
        b".riffjournal.sealing",
        b".riffjournal.next",
        b".riffjournal.next.prep",
        b".riffjournal.recycled",
        b".riffdb-format-v1.staging",
        b".riffdb-format-upgrade-v1.staging",
    ];
    if let Some(index) = FIXED.iter().position(|s| *s == suffix) {
        return Ok(Some(SourceName::Fixed(
            u8::try_from(index + 1).map_err(|_| Error::Unavailable)?,
        )));
    }
    if let Some(digits) = suffix.strip_prefix(b".riffjournal.sealed.") {
        if digits.len() != 20 || !digits.iter().all(u8::is_ascii_digit) {
            return Err(Error::Unavailable);
        }
        let generation = std::str::from_utf8(digits)
            .map_err(|_| Error::Unavailable)?
            .parse::<u64>()
            .map_err(|_| Error::Unavailable)?;
        if generation == 0 {
            return Err(Error::Unavailable);
        }
        return Ok(Some(SourceName::Sealed));
    }
    if suffix.starts_with(b".riffjournal") || suffix.starts_with(b".riffdb-format") {
        return Err(Error::Unavailable);
    }
    Ok(None)
}
