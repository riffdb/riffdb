//! Protected PostgreSQL URL loading for the non-production evidence runner.

use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::time::Duration;

use postgres::Config;

#[cfg(target_os = "linux")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const MAX_URL_BYTES: usize = 4_096;
const PROC_STATUS_MAX_BYTES: usize = 65_536;
const PROC_STATUS_PATH: &str = "/proc/self/status";
const O_NOFOLLOW: i32 = 0o00400000;
const O_NONBLOCK: i32 = 0o00004000;
const PROTECTED_OPEN_FLAGS: i32 = O_NOFOLLOW | O_NONBLOCK;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A validated PostgreSQL connection URL with redacted debug presentation.
#[derive(Clone, Eq, PartialEq)]
pub struct ProtectedPostgresUrl(String);

impl ProtectedPostgresUrl {
    pub(crate) fn expose_for_connection(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn from_test_url(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl fmt::Debug for ProtectedPostgresUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedPostgresUrl([REDACTED])")
    }
}

/// Loads one PostgreSQL URL through the accepted protected Linux file rule.
pub fn load_protected_postgres_url(
    path: &Path,
) -> Result<ProtectedPostgresUrl, PostgresUrlFileError> {
    #[cfg(target_os = "linux")]
    {
        load_with(&LinuxPlatform, path)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err(PostgresUrlFileError::UnsupportedPlatform)
    }
}

#[derive(Clone, Copy)]
struct FileMetadata {
    is_regular: bool,
    uid: u32,
    mode: u32,
    device: u64,
    inode: u64,
}

trait ProtectedFilePlatform {
    type ProcStatusReader: Read;
    type OpenedFile: Read;

    fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader>;
    fn symlink_metadata(&self, path: &Path) -> io::Result<FileMetadata>;
    fn open_file(&self, path: &Path, flags: i32) -> io::Result<Self::OpenedFile>;
    fn opened_metadata(&self, file: &Self::OpenedFile) -> io::Result<FileMetadata>;
}

#[cfg(target_os = "linux")]
struct LinuxPlatform;

#[cfg(target_os = "linux")]
impl ProtectedFilePlatform for LinuxPlatform {
    type ProcStatusReader = File;
    type OpenedFile = File;

    fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader> {
        File::open(PROC_STATUS_PATH)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<FileMetadata> {
        std::fs::symlink_metadata(path).map(metadata_snapshot)
    }

    fn open_file(&self, path: &Path, flags: i32) -> io::Result<Self::OpenedFile> {
        OpenOptions::new().read(true).custom_flags(flags).open(path)
    }

    fn opened_metadata(&self, file: &Self::OpenedFile) -> io::Result<FileMetadata> {
        file.metadata().map(metadata_snapshot)
    }
}

#[cfg(target_os = "linux")]
fn metadata_snapshot(metadata: std::fs::Metadata) -> FileMetadata {
    FileMetadata {
        is_regular: metadata.is_file(),
        uid: metadata.uid(),
        mode: metadata.mode(),
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn load_with<P: ProtectedFilePlatform>(
    platform: &P,
    path: &Path,
) -> Result<ProtectedPostgresUrl, PostgresUrlFileError> {
    let effective_uid = read_effective_uid(platform)?;
    let before = platform
        .symlink_metadata(path)
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    if !before.is_regular {
        return Err(PostgresUrlFileError::ProtectedFileRejected);
    }

    let mut file = platform
        .open_file(path, PROTECTED_OPEN_FLAGS)
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    let opened = platform
        .opened_metadata(&file)
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    if !opened.is_regular
        || opened.uid != effective_uid
        || opened.mode & 0o077 != 0
        || (opened.device, opened.inode) != (before.device, before.inode)
    {
        return Err(PostgresUrlFileError::ProtectedFileRejected);
    }

    let mut bytes = Vec::with_capacity(MAX_URL_BYTES + 1);
    file.by_ref()
        .take((MAX_URL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    if bytes.is_empty() || bytes.len() > MAX_URL_BYTES {
        return Err(PostgresUrlFileError::ProtectedFileRejected);
    }
    parse_url(bytes)
}

fn parse_url(bytes: Vec<u8>) -> Result<ProtectedPostgresUrl, PostgresUrlFileError> {
    if bytes.contains(&0) || bytes.contains(&b'\n') || bytes.contains(&b'\r') {
        return Err(PostgresUrlFileError::InvalidUrl);
    }
    let value = String::from_utf8(bytes).map_err(|_| PostgresUrlFileError::InvalidUrl)?;
    if !(value.starts_with("postgres://") || value.starts_with("postgresql://")) {
        return Err(PostgresUrlFileError::InvalidUrl);
    }
    let mut config = value
        .parse::<Config>()
        .map_err(|_| PostgresUrlFileError::InvalidUrl)?;
    config.connect_timeout(CONNECT_TIMEOUT);
    Ok(ProtectedPostgresUrl(value))
}

fn read_effective_uid<P: ProtectedFilePlatform>(platform: &P) -> Result<u32, PostgresUrlFileError> {
    let mut status = platform
        .open_proc_status()
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    let mut bytes = Vec::with_capacity(8 * 1024);
    status
        .by_ref()
        .take((PROC_STATUS_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| PostgresUrlFileError::ProtectedFileRejected)?;
    if bytes.len() > PROC_STATUS_MAX_BYTES {
        return Err(PostgresUrlFileError::ProtectedFileRejected);
    }
    parse_effective_uid(&bytes).ok_or(PostgresUrlFileError::ProtectedFileRejected)
}

fn parse_effective_uid(status: &[u8]) -> Option<u32> {
    let mut effective_uid = None;
    for line in status.split(|byte| *byte == b'\n') {
        let Some(fields) = line.strip_prefix(b"Uid:") else {
            continue;
        };
        if effective_uid.is_some() {
            return None;
        }
        effective_uid = Some(parse_uid_fields(fields)?[1]);
    }
    effective_uid
}

fn parse_uid_fields(fields: &[u8]) -> Option<[u32; 4]> {
    let mut cursor = 0;
    require_separator(fields, &mut cursor)?;
    let mut values = [0_u32; 4];
    for (index, value) in values.iter_mut().enumerate() {
        if index != 0 {
            require_separator(fields, &mut cursor)?;
        }
        let start = cursor;
        while fields.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        *value = parse_canonical_u32(&fields[start..cursor])?;
    }
    (cursor == fields.len()).then_some(values)
}

fn require_separator(fields: &[u8], cursor: &mut usize) -> Option<()> {
    let start = *cursor;
    while fields
        .get(*cursor)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        *cursor += 1;
    }
    (*cursor != start).then_some(())
}

fn parse_canonical_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || (bytes.len() > 1 && bytes[0] == b'0') {
        return None;
    }
    bytes.iter().try_fold(0_u32, |value, byte| {
        byte.is_ascii_digit()
            .then_some(*byte - b'0')
            .and_then(|digit| value.checked_mul(10)?.checked_add(u32::from(digit)))
    })
}

/// A closed, redaction-safe PostgreSQL URL loading failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresUrlFileError {
    /// The protected-file procedure is unavailable on this platform.
    UnsupportedPlatform,
    /// File type, ownership, permissions, identity, or size failed closed.
    ProtectedFileRejected,
    /// Contents are not one bounded UTF-8 PostgreSQL URL.
    InvalidUrl,
}

impl fmt::Display for PostgresUrlFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedPlatform => {
                "protected PostgreSQL URL files are unsupported on this platform"
            }
            Self::ProtectedFileRejected => "protected PostgreSQL URL file was rejected",
            Self::InvalidUrl => "PostgreSQL URL file contents are invalid",
        })
    }
}

impl Error for PostgresUrlFileError {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[derive(Clone)]
    struct TestPlatform {
        status: Vec<u8>,
        before: FileMetadata,
        opened: FileMetadata,
        bytes: Vec<u8>,
    }

    impl ProtectedFilePlatform for TestPlatform {
        type ProcStatusReader = Cursor<Vec<u8>>;
        type OpenedFile = Cursor<Vec<u8>>;

        fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader> {
            Ok(Cursor::new(self.status.clone()))
        }

        fn symlink_metadata(&self, _path: &Path) -> io::Result<FileMetadata> {
            Ok(self.before)
        }

        fn open_file(&self, _path: &Path, _flags: i32) -> io::Result<Self::OpenedFile> {
            Ok(Cursor::new(self.bytes.clone()))
        }

        fn opened_metadata(&self, _file: &Self::OpenedFile) -> io::Result<FileMetadata> {
            Ok(self.opened)
        }
    }

    fn valid_platform() -> TestPlatform {
        let metadata = FileMetadata {
            is_regular: true,
            uid: 1000,
            mode: 0o100600,
            device: 4,
            inode: 5,
        };
        TestPlatform {
            status: b"Name:\ttest\nUid:\t1000\t1000\t1000\t1000\n".to_vec(),
            before: metadata,
            opened: metadata,
            bytes: b"postgresql://user:secret-canary@127.0.0.1:5432/example".to_vec(),
        }
    }

    #[test]
    fn loader_accepts_private_stable_file_and_redacts_debug() {
        let loaded = load_with(&valid_platform(), Path::new("ignored")).expect("URL");
        assert!(loaded.expose_for_connection().starts_with("postgresql://"));
        assert_eq!(format!("{loaded:?}"), "ProtectedPostgresUrl([REDACTED])");
        assert!(!format!("{loaded:?}").contains("secret-canary"));
    }

    #[test]
    fn loader_rejects_permissions_identity_symlink_shape_and_bad_contents() {
        let mut world_readable = valid_platform();
        world_readable.opened.mode = 0o100644;
        let mut wrong_owner = valid_platform();
        wrong_owner.opened.uid = 1001;
        let mut changed = valid_platform();
        changed.opened.inode = 6;
        let mut not_regular = valid_platform();
        not_regular.before.is_regular = false;
        let mut newline = valid_platform();
        newline.bytes.push(b'\n');
        let mut nul = valid_platform();
        nul.bytes.push(0);
        let mut non_utf8 = valid_platform();
        non_utf8.bytes = vec![0xff];
        let mut non_url = valid_platform();
        non_url.bytes = b"host=127.0.0.1".to_vec();

        for platform in [world_readable, wrong_owner, changed, not_regular] {
            assert_eq!(
                load_with(&platform, Path::new("ignored")),
                Err(PostgresUrlFileError::ProtectedFileRejected)
            );
        }
        for platform in [newline, nul, non_utf8, non_url] {
            assert_eq!(
                load_with(&platform, Path::new("ignored")),
                Err(PostgresUrlFileError::InvalidUrl)
            );
        }
    }

    #[test]
    fn loader_enforces_empty_exact_and_over_limit_content_boundaries() {
        let prefix = b"postgresql://user@127.0.0.1:5432/";
        let mut exact = valid_platform();
        exact.bytes = prefix
            .iter()
            .copied()
            .chain(std::iter::repeat_n(b'x', MAX_URL_BYTES - prefix.len()))
            .collect();
        assert_eq!(exact.bytes.len(), MAX_URL_BYTES);
        assert!(load_with(&exact, Path::new("ignored")).is_ok());

        let mut empty = valid_platform();
        empty.bytes.clear();
        let mut too_large = valid_platform();
        too_large.bytes = vec![b'x'; MAX_URL_BYTES + 1];
        for platform in [empty, too_large] {
            assert_eq!(
                load_with(&platform, Path::new("ignored")),
                Err(PostgresUrlFileError::ProtectedFileRejected)
            );
        }
    }

    #[test]
    fn effective_uid_parser_requires_one_exact_four_field_row() {
        assert_eq!(
            parse_effective_uid(b"Name:\ttest\nUid:\t7\t8\t9\t10\n"),
            Some(8)
        );
        assert_eq!(parse_effective_uid(b"Uid:\t7\t8\t9\n"), None);
        assert_eq!(
            parse_effective_uid(b"Uid:\t7\t8\t9\t10\nUid:\t7\t8\t9\t10\n"),
            None
        );
    }
}
