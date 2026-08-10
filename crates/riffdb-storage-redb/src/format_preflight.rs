//! Source-free durable-format comparison before redb may open for mutation.

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use riffdb_storage_api::{
    DURABLE_FORMAT_MARKER_BYTES, DurableFormatAction, DurableFormatIdentity,
    DurableFormatMarkerError, DurableFormatPreflightError, current_durable_format_manifest,
    current_durable_format_marker, decode_durable_format_marker, encode_durable_format_marker,
    preflight_durable_format,
};

const MARKER_SUFFIX: &str = ".riffdb-format-v1";
const MARKER_STAGING_SUFFIX: &str = ".riffdb-format-v1.staging";

/// Closed pre-open state for one redb database path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbDurableFormatPreflight {
    /// Neither database nor marker exists; current initialization is allowed.
    InitializeCurrent,
    /// The exact current marker is present and checked.
    OpenCurrent,
    /// A declared offline transition is required before normal startup.
    OfflineUpgradeRequired {
        /// The retained predecessor identity.
        current: DurableFormatIdentity,
        /// This binary's identity.
        binary: DurableFormatIdentity,
        /// The only manifest-declared transition.
        action: DurableFormatAction,
    },
}

/// Safe pre-open failures that retain no filesystem path or data value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbDurableFormatPreflightError {
    /// Filesystem metadata or bounded reads were unavailable.
    Unavailable,
    /// Database/marker presence was ambiguous and cannot authorize creation.
    AmbiguousInventory,
    /// The marker was malformed, corrupt, or did not match its release.
    Marker(DurableFormatMarkerError),
    /// The retained release has no edge to this binary.
    Unsupported(DurableFormatPreflightError),
}

impl RedbDurableFormatPreflightError {
    /// Returns the retained identity when it was decoded and rejected as an
    /// unsupported release edge. Corrupt or ambiguous bytes deliberately
    /// remain `None` rather than exposing a guessed identity.
    #[must_use]
    pub const fn current_identity(self) -> Option<DurableFormatIdentity> {
        match self {
            Self::Unsupported(error) => Some(error.current()),
            Self::Unavailable | Self::AmbiguousInventory | Self::Marker(_) => None,
        }
    }

    /// Returns this binary's exact immutable format identity.
    #[must_use]
    pub fn binary_identity(self) -> DurableFormatIdentity {
        current_durable_format_manifest().identity()
    }

    /// Returns the sole safe recovery command without admitting force/reset.
    #[must_use]
    pub const fn next_command(self) -> riffdb_storage_api::SafeFormatCommand {
        match self {
            Self::Unsupported(error) => error.next_command(),
            Self::Unavailable | Self::AmbiguousInventory | Self::Marker(_) => {
                riffdb_storage_api::SafeFormatCommand::UseMatchingBinary
            }
        }
    }
}

impl fmt::Display for RedbDurableFormatPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Unavailable => formatter.write_str("durable format preflight is unavailable"),
            Self::AmbiguousInventory => {
                formatter.write_str("database and durable format marker inventory is ambiguous")
            }
            Self::Marker(error) => error.fmt(formatter),
            Self::Unsupported(error) => return error.fmt(formatter),
        }?;
        let binary = self.binary_identity();
        write!(
            formatter,
            "; current=unknown binary=alpha-{}.{}; next: {}",
            binary.epoch().get(),
            binary.writer().get(),
            self.next_command().render()
        )
    }
}

impl Error for RedbDurableFormatPreflightError {}

/// Returns the fixed sibling marker path for backup and startup inventory.
#[must_use]
pub fn durable_format_marker_path(database_path: &Path) -> PathBuf {
    sibling_with_suffix(database_path, MARKER_SUFFIX)
}

/// Compares immutable path/marker identity without opening redb.
pub fn preflight_durable_format_path(
    database_path: &Path,
) -> Result<RedbDurableFormatPreflight, RedbDurableFormatPreflightError> {
    let marker_path = durable_format_marker_path(database_path);
    let database = metadata_state(database_path)?;
    let marker = metadata_state(&marker_path)?;

    match (database, marker) {
        (FileState::Absent, FileState::Absent) => Ok(RedbDurableFormatPreflight::InitializeCurrent),
        (FileState::Empty, FileState::Absent) => Ok(RedbDurableFormatPreflight::InitializeCurrent),
        (FileState::Present, FileState::Absent) => {
            let manifest = current_durable_format_manifest();
            let current = riffdb_storage_api::DurableFormatIdentity::new(
                manifest.epoch(),
                riffdb_storage_api::DurableFormatWriter::new(0),
            );
            let action = preflight_durable_format(current)
                .map_err(RedbDurableFormatPreflightError::Unsupported)?;
            Ok(RedbDurableFormatPreflight::OfflineUpgradeRequired {
                current,
                binary: manifest.identity(),
                action,
            })
        }
        (FileState::Present, FileState::Present) => preflight_existing_marker(&marker_path),
        (FileState::Present, FileState::Empty) => {
            Err(RedbDurableFormatPreflightError::AmbiguousInventory)
        }
        (FileState::Absent | FileState::Empty, FileState::Present) => {
            match preflight_existing_marker(&marker_path)? {
                RedbDurableFormatPreflight::OpenCurrent => {
                    Ok(RedbDurableFormatPreflight::InitializeCurrent)
                }
                RedbDurableFormatPreflight::InitializeCurrent
                | RedbDurableFormatPreflight::OfflineUpgradeRequired { .. } => {
                    Err(RedbDurableFormatPreflightError::AmbiguousInventory)
                }
            }
        }
        (FileState::Absent | FileState::Empty, FileState::Empty) => {
            Err(RedbDurableFormatPreflightError::AmbiguousInventory)
        }
    }
}

/// Publishes current initialization intent before redb may create a container.
///
/// Marker-first ordering closes the crash gap between container creation and
/// format publication. A current marker with an absent/empty container resumes
/// `InitializeCurrent`; the caller must retain the exact pre-open witness, so
/// this function cannot stamp an existing predecessor database. Publication is
/// create-only, file-synced, rename-published, and parent-directory-synced.
pub(crate) fn publish_initialized_current_marker(
    database_path: &Path,
    preflight: RedbDurableFormatPreflight,
) -> Result<(), RedbDurableFormatPreflightError> {
    if preflight != RedbDurableFormatPreflight::InitializeCurrent {
        return Err(RedbDurableFormatPreflightError::AmbiguousInventory);
    }
    publish_current_marker_create_only(database_path)
}

/// Publishes the current marker only for the exact manifest-declared
/// predecessor transition. Backup and durable receipt validation remain the
/// upgrade driver's responsibility.
pub(crate) fn publish_upgraded_current_marker(
    database_path: &Path,
    preflight: RedbDurableFormatPreflight,
) -> Result<(), RedbDurableFormatPreflightError> {
    let RedbDurableFormatPreflight::OfflineUpgradeRequired {
        current,
        binary,
        action,
    } = preflight
    else {
        return Err(RedbDurableFormatPreflightError::AmbiguousInventory);
    };
    let manifest = current_durable_format_manifest();
    if binary != manifest.identity()
        || preflight_durable_format(current)
            .map_err(RedbDurableFormatPreflightError::Unsupported)?
            != action
        || !matches!(action, DurableFormatAction::OfflineInPlace { .. })
    {
        return Err(RedbDurableFormatPreflightError::AmbiguousInventory);
    }
    publish_current_marker_create_only(database_path)
}

fn publish_current_marker_create_only(
    database_path: &Path,
) -> Result<(), RedbDurableFormatPreflightError> {
    let marker_path = durable_format_marker_path(database_path);
    if marker_path
        .try_exists()
        .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?
    {
        return match preflight_existing_marker(&marker_path)? {
            RedbDurableFormatPreflight::OpenCurrent => Ok(()),
            RedbDurableFormatPreflight::InitializeCurrent
            | RedbDurableFormatPreflight::OfflineUpgradeRequired { .. } => {
                Err(RedbDurableFormatPreflightError::AmbiguousInventory)
            }
        };
    }
    let staging_path = sibling_with_suffix(database_path, MARKER_STAGING_SUFFIX);
    let mut staging = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&staging_path)
        .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
    let encoded = encode_durable_format_marker(current_durable_format_marker());
    let result = (|| {
        staging
            .write_all(&encoded)
            .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
        staging
            .sync_all()
            .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
        drop(staging);
        fs::rename(&staging_path, &marker_path)
            .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
        sync_parent(&marker_path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging_path);
    }
    result
}

fn preflight_existing_marker(
    marker_path: &Path,
) -> Result<RedbDurableFormatPreflight, RedbDurableFormatPreflightError> {
    let marker = read_marker(marker_path)?;
    let manifest = current_durable_format_manifest();
    let action = preflight_durable_format(marker.identity())
        .map_err(RedbDurableFormatPreflightError::Unsupported)?;
    if action == DurableFormatAction::OpenCurrent {
        if marker.registry_digest() != manifest.registry_digest()
            || marker.compatibility_fixture_digest() != manifest.compatibility_fixture_digest()
        {
            return Err(RedbDurableFormatPreflightError::Marker(
                DurableFormatMarkerError::ManifestMismatch,
            ));
        }
        return Ok(RedbDurableFormatPreflight::OpenCurrent);
    }
    Ok(RedbDurableFormatPreflight::OfflineUpgradeRequired {
        current: marker.identity(),
        binary: manifest.identity(),
        action,
    })
}

fn read_marker(
    marker_path: &Path,
) -> Result<riffdb_storage_api::DurableFormatMarker, RedbDurableFormatPreflightError> {
    let metadata =
        fs::metadata(marker_path).map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
    if metadata.len() != u64::try_from(DURABLE_FORMAT_MARKER_BYTES).expect("marker length fits u64")
    {
        return Err(RedbDurableFormatPreflightError::Marker(
            DurableFormatMarkerError::Malformed,
        ));
    }
    let mut file =
        File::open(marker_path).map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
    let mut encoded = [0_u8; DURABLE_FORMAT_MARKER_BYTES];
    file.read_exact(&mut encoded)
        .map_err(|_| RedbDurableFormatPreflightError::Unavailable)?;
    decode_durable_format_marker(&encoded).map_err(RedbDurableFormatPreflightError::Marker)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileState {
    Absent,
    Empty,
    Present,
}

fn metadata_state(path: &Path) -> Result<FileState, RedbDurableFormatPreflightError> {
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_file() => {
            Err(RedbDurableFormatPreflightError::AmbiguousInventory)
        }
        Ok(metadata) if metadata.len() == 0 => Ok(FileState::Empty),
        Ok(_) => Ok(FileState::Present),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(FileState::Absent),
        Err(_) => Err(RedbDurableFormatPreflightError::Unavailable),
    }
}

fn sibling_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn sync_parent(path: &Path) -> Result<(), RedbDurableFormatPreflightError> {
    let parent = path
        .parent()
        .ok_or(RedbDurableFormatPreflightError::Unavailable)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RedbDurableFormatPreflightError::Unavailable)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{current_durable_format_marker, encode_durable_format_marker};

    use super::*;

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn root(label: &str) -> PathBuf {
        let ordinal = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/durable-format-preflight")
            .join(format!("{label}-{}-{ordinal}", std::process::id()));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove exact prior test root");
        }
        fs::create_dir_all(&root).expect("create test root");
        root
    }

    #[test]
    fn missing_current_and_exact_current_are_distinct() {
        let root = root("current");
        let database = root.join("app.redb");
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::InitializeCurrent)
        );
        fs::write(&database, b"nonempty database placeholder").expect("write database");
        assert!(matches!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::OfflineUpgradeRequired { .. })
        ));
        fs::write(
            durable_format_marker_path(&database),
            encode_durable_format_marker(current_durable_format_marker()),
        )
        .expect("write exact marker");
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::OpenCurrent)
        );
    }

    #[test]
    fn unsupported_marker_refuses_without_changing_either_file() {
        let root = root("refusal");
        let database = root.join("app.redb");
        let database_bytes = b"do not mutate this database";
        fs::write(&database, database_bytes).expect("write database");
        let marker_path = durable_format_marker_path(&database);
        let current = current_durable_format_marker();
        let unsupported = riffdb_storage_api::DurableFormatMarker::new(
            riffdb_storage_api::DurableFormatIdentity::new(
                riffdb_storage_api::AlphaFormatEpoch::new(2).expect("epoch"),
                riffdb_storage_api::DurableFormatWriter::new(1),
            ),
            current.registry_digest(),
            current.compatibility_fixture_digest(),
        );
        let marker_bytes = encode_durable_format_marker(unsupported);
        fs::write(&marker_path, marker_bytes).expect("write marker");

        let error = preflight_durable_format_path(&database).expect_err("unsupported marker");
        assert_eq!(error.current_identity(), Some(unsupported.identity()));
        assert_eq!(
            error.binary_identity(),
            current_durable_format_manifest().identity()
        );
        assert_eq!(
            error.next_command(),
            riffdb_storage_api::SafeFormatCommand::UseMatchingBinary
        );
        assert_eq!(fs::read(&database).expect("read database"), database_bytes);
        assert_eq!(fs::read(&marker_path).expect("read marker"), marker_bytes);
    }

    #[test]
    fn current_marker_publication_requires_the_new_container_witness() {
        let root = root("publication");
        let database = root.join("app.redb");
        let witness = preflight_durable_format_path(&database).expect("preflight absent path");
        publish_initialized_current_marker(&database, witness).expect("publish current marker");
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::InitializeCurrent)
        );
        fs::write(&database, b"new current container").expect("create current container");
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::OpenCurrent)
        );

        assert_eq!(
            publish_initialized_current_marker(&database, RedbDurableFormatPreflight::OpenCurrent,),
            Err(RedbDurableFormatPreflightError::AmbiguousInventory)
        );
        assert!(!sibling_with_suffix(&database, MARKER_STAGING_SUFFIX).exists());
    }
}
