//! Linux protected-file loading shared by secret-document owners.

use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;

use zeroize::Zeroizing;

#[cfg(target_os = "linux")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const PROC_STATUS_PATH: &str = "/proc/self/status";
const PROC_STATUS_MAX_BYTES: usize = 65_536;
const O_NOFOLLOW: i32 = 0o00400000;
const O_NONBLOCK: i32 = 0o00004000;
const PROTECTED_OPEN_FLAGS: i32 = O_NOFOLLOW | O_NONBLOCK;

/// Closed failure classes for protected local secret files.
///
/// The error deliberately carries no path, secret bytes, operating-system
/// message, user identifier, or metadata value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProtectedFileError {
    /// The exact protected-file rule is unavailable on this platform.
    #[cfg(not(target_os = "linux"))]
    UnsupportedPlatform,
    /// The caller supplied a limit for which `limit + 1` is not representable.
    InvalidDocumentLimit,
    /// `/proc/self/status` could not be opened.
    ProcStatusUnavailable,
    /// `/proc/self/status` could not be read through EOF.
    ProcStatusReadFailed,
    /// `/proc/self/status` exceeded its fixed bound.
    ProcStatusTooLarge,
    /// `/proc/self/status` did not contain one exact canonical `Uid:` record.
    ProcStatusMalformed,
    /// Final-component metadata could not be obtained before open.
    PathMetadataUnavailable,
    /// The final component was not a regular file before open.
    PathNotRegularFile,
    /// The no-follow, nonblocking read open failed.
    FileOpenFailed,
    /// Metadata could not be obtained from the opened handle.
    OpenedMetadataUnavailable,
    /// The opened handle was not a regular file.
    OpenedFileNotRegular,
    /// The opened file was not owned by the process effective user.
    OwnerMismatch,
    /// The opened file granted any group or other permission.
    InsecureMode,
    /// The pre-open path and opened handle named different filesystem objects.
    FileIdentityChanged,
    /// The opened file could not be read through EOF.
    FileReadFailed,
    /// The opened file exceeded its caller-provided document bound.
    DocumentTooLarge,
}

impl fmt::Display for ProtectedFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            #[cfg(not(target_os = "linux"))]
            Self::UnsupportedPlatform => "protected files are unsupported on this platform",
            Self::InvalidDocumentLimit => "protected file limit is invalid",
            Self::ProcStatusUnavailable => "effective user information is unavailable",
            Self::ProcStatusReadFailed => "effective user information could not be read",
            Self::ProcStatusTooLarge => "effective user information exceeds its size limit",
            Self::ProcStatusMalformed => "effective user information is malformed",
            Self::PathMetadataUnavailable => "protected file metadata is unavailable",
            Self::PathNotRegularFile => "protected file path is not a regular file",
            Self::FileOpenFailed => "protected file could not be opened",
            Self::OpenedMetadataUnavailable => "opened protected file metadata is unavailable",
            Self::OpenedFileNotRegular => "opened protected file is not a regular file",
            Self::OwnerMismatch => "protected file owner does not match the effective user",
            Self::InsecureMode => "protected file permissions are not private",
            Self::FileIdentityChanged => "protected file identity changed during open",
            Self::FileReadFailed => "protected file could not be read",
            Self::DocumentTooLarge => "protected file exceeds its document size limit",
        })
    }
}

impl Error for ProtectedFileError {}

/// Owned bytes read from one validated protected file.
///
/// The value is deliberately non-`Clone`, nonserializable, redacted when
/// formatted, and zeroized on drop. Parsers may only borrow its contents.
pub(crate) struct ProtectedFileDocument(Zeroizing<Vec<u8>>);

impl ProtectedFileDocument {
    /// Explicitly borrows the secret document for its exact parser.
    #[must_use]
    pub(crate) fn expose_secret(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for ProtectedFileDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedFileDocument([REDACTED])")
    }
}

impl fmt::Display for ProtectedFileDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("protected file document [REDACTED]")
    }
}

/// Loads one protected secret document under the exact ADR-0009 Linux rule.
///
/// The returned bytes have passed filesystem and size validation only. The
/// caller remains responsible for exact document parsing and for immediately
/// moving the bytes into its secret-custody wrapper.
pub(crate) fn read_protected_file(
    path: &Path,
    document_limit: usize,
) -> Result<ProtectedFileDocument, ProtectedFileError> {
    #[cfg(target_os = "linux")]
    {
        read_protected_file_with(&LinuxPlatform, path, document_limit)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, document_limit);
        Err(ProtectedFileError::UnsupportedPlatform)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    fn open_file(&self, path: &Path, custom_flags: i32) -> io::Result<Self::OpenedFile>;

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

    fn open_file(&self, path: &Path, custom_flags: i32) -> io::Result<Self::OpenedFile> {
        OpenOptions::new()
            .read(true)
            .custom_flags(custom_flags)
            .open(path)
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

fn read_protected_file_with<P: ProtectedFilePlatform>(
    platform: &P,
    path: &Path,
    document_limit: usize,
) -> Result<ProtectedFileDocument, ProtectedFileError> {
    let mut proc_status = platform
        .open_proc_status()
        .map_err(|_| ProtectedFileError::ProcStatusUnavailable)?;
    let proc_status = read_bounded_to_eof(&mut proc_status, PROC_STATUS_MAX_BYTES).map_err(
        |error| match error {
            BoundedReadError::Read => ProtectedFileError::ProcStatusReadFailed,
            BoundedReadError::TooLarge => ProtectedFileError::ProcStatusTooLarge,
            BoundedReadError::InvalidLimit => ProtectedFileError::ProcStatusMalformed,
        },
    )?;
    let effective_uid = parse_effective_uid(&proc_status)?;

    let before = platform
        .symlink_metadata(path)
        .map_err(|_| ProtectedFileError::PathMetadataUnavailable)?;
    if !before.is_regular {
        return Err(ProtectedFileError::PathNotRegularFile);
    }

    let mut file = platform
        .open_file(path, PROTECTED_OPEN_FLAGS)
        .map_err(|_| ProtectedFileError::FileOpenFailed)?;
    let opened = platform
        .opened_metadata(&file)
        .map_err(|_| ProtectedFileError::OpenedMetadataUnavailable)?;
    if !opened.is_regular {
        return Err(ProtectedFileError::OpenedFileNotRegular);
    }
    if opened.uid != effective_uid {
        return Err(ProtectedFileError::OwnerMismatch);
    }
    if opened.mode & 0o077 != 0 {
        return Err(ProtectedFileError::InsecureMode);
    }
    if (opened.device, opened.inode) != (before.device, before.inode) {
        return Err(ProtectedFileError::FileIdentityChanged);
    }

    read_secret_bounded_to_eof(&mut file, document_limit).map_err(|error| match error {
        BoundedReadError::Read => ProtectedFileError::FileReadFailed,
        BoundedReadError::TooLarge => ProtectedFileError::DocumentTooLarge,
        BoundedReadError::InvalidLimit => ProtectedFileError::InvalidDocumentLimit,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedReadError {
    Read,
    TooLarge,
    InvalidLimit,
}

fn read_bounded_to_eof<R: Read>(reader: &mut R, limit: usize) -> Result<Vec<u8>, BoundedReadError> {
    let limit_plus_one = limit.checked_add(1).ok_or(BoundedReadError::InvalidLimit)?;
    let read_limit = u64::try_from(limit_plus_one).map_err(|_| BoundedReadError::InvalidLimit)?;
    let mut bytes = Vec::with_capacity(limit_plus_one.min(8 * 1024));
    reader
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| BoundedReadError::Read)?;
    if bytes.len() > limit {
        Err(BoundedReadError::TooLarge)
    } else {
        Ok(bytes)
    }
}

fn read_secret_bounded_to_eof<R: Read>(
    reader: &mut R,
    limit: usize,
) -> Result<ProtectedFileDocument, BoundedReadError> {
    let limit_plus_one = limit.checked_add(1).ok_or(BoundedReadError::InvalidLimit)?;
    let read_limit = u64::try_from(limit_plus_one).map_err(|_| BoundedReadError::InvalidLimit)?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(limit_plus_one.min(8 * 1024)));
    reader
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| BoundedReadError::Read)?;
    if bytes.len() > limit {
        Err(BoundedReadError::TooLarge)
    } else {
        Ok(ProtectedFileDocument(bytes))
    }
}

fn parse_effective_uid(proc_status: &[u8]) -> Result<u32, ProtectedFileError> {
    let mut effective_uid = None;
    for line in proc_status.split(|byte| *byte == b'\n') {
        let Some(fields) = line.strip_prefix(b"Uid:") else {
            continue;
        };
        if effective_uid.is_some() {
            return Err(ProtectedFileError::ProcStatusMalformed);
        }
        effective_uid = Some(parse_uid_fields(fields)?[1]);
    }
    effective_uid.ok_or(ProtectedFileError::ProcStatusMalformed)
}

fn parse_uid_fields(fields: &[u8]) -> Result<[u32; 4], ProtectedFileError> {
    let mut cursor = 0;
    require_uid_separator(fields, &mut cursor)?;

    let mut values = [0_u32; 4];
    for (index, value) in values.iter_mut().enumerate() {
        if index != 0 {
            require_uid_separator(fields, &mut cursor)?;
        }
        let start = cursor;
        while fields.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        *value = parse_canonical_u32(&fields[start..cursor])?;
    }
    if cursor != fields.len() {
        return Err(ProtectedFileError::ProcStatusMalformed);
    }
    Ok(values)
}

fn require_uid_separator(fields: &[u8], cursor: &mut usize) -> Result<(), ProtectedFileError> {
    let start = *cursor;
    while fields
        .get(*cursor)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        *cursor += 1;
    }
    if *cursor == start {
        Err(ProtectedFileError::ProcStatusMalformed)
    } else {
        Ok(())
    }
}

fn parse_canonical_u32(bytes: &[u8]) -> Result<u32, ProtectedFileError> {
    if bytes.is_empty() || (bytes.len() > 1 && bytes[0] == b'0') {
        return Err(ProtectedFileError::ProcStatusMalformed);
    }
    bytes.iter().try_fold(0_u32, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err(ProtectedFileError::ProcStatusMalformed);
        }
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(*byte - b'0')))
            .ok_or(ProtectedFileError::ProcStatusMalformed)
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::Cursor;

    use super::*;

    const EFFECTIVE_UID: u32 = 1_234;
    const TEST_PATH: &str = "/not-opened-by-fake";

    struct FakeReader {
        inner: Cursor<Vec<u8>>,
        fails: bool,
    }

    impl FakeReader {
        fn new(bytes: Vec<u8>, fails: bool) -> Self {
            Self {
                inner: Cursor::new(bytes),
                fails,
            }
        }
    }

    impl Read for FakeReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.fails {
                Err(io::Error::other("injected read failure"))
            } else {
                self.inner.read(buffer)
            }
        }
    }

    struct FakePlatform {
        proc_status: Option<Vec<u8>>,
        proc_read_fails: bool,
        before: Option<FileMetadata>,
        file_bytes: Option<Vec<u8>>,
        file_read_fails: bool,
        opened: Option<FileMetadata>,
        seen_flags: Cell<Option<i32>>,
    }

    impl FakePlatform {
        fn valid(document: &[u8]) -> Self {
            let metadata = FileMetadata {
                is_regular: true,
                uid: EFFECTIVE_UID,
                mode: 0o100600,
                device: 10,
                inode: 20,
            };
            Self {
                proc_status: Some(b"Name:\triffdb\nUid:\t1234\t1234  1234\t1234\n".to_vec()),
                proc_read_fails: false,
                before: Some(metadata),
                file_bytes: Some(document.to_vec()),
                file_read_fails: false,
                opened: Some(metadata),
                seen_flags: Cell::new(None),
            }
        }
    }

    impl ProtectedFilePlatform for FakePlatform {
        type ProcStatusReader = FakeReader;
        type OpenedFile = FakeReader;

        fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader> {
            self.proc_status
                .clone()
                .map(|bytes| FakeReader::new(bytes, self.proc_read_fails))
                .ok_or_else(|| io::Error::other("injected proc open failure"))
        }

        fn symlink_metadata(&self, _path: &Path) -> io::Result<FileMetadata> {
            self.before
                .ok_or_else(|| io::Error::other("injected path metadata failure"))
        }

        fn open_file(&self, _path: &Path, custom_flags: i32) -> io::Result<Self::OpenedFile> {
            self.seen_flags.set(Some(custom_flags));
            self.file_bytes
                .clone()
                .map(|bytes| FakeReader::new(bytes, self.file_read_fails))
                .ok_or_else(|| io::Error::other("injected file open failure"))
        }

        fn opened_metadata(&self, _file: &Self::OpenedFile) -> io::Result<FileMetadata> {
            self.opened
                .ok_or_else(|| io::Error::other("injected opened metadata failure"))
        }
    }

    fn load(
        platform: &FakePlatform,
        limit: usize,
    ) -> Result<ProtectedFileDocument, ProtectedFileError> {
        read_protected_file_with(platform, Path::new(TEST_PATH), limit)
    }

    fn assert_load_error(platform: &FakePlatform, limit: usize, expected: ProtectedFileError) {
        assert_eq!(load(platform, limit).err(), Some(expected));
    }

    #[test]
    fn linux_bounds_and_open_flags_are_frozen() {
        assert_eq!(PROC_STATUS_MAX_BYTES, 65_536);
        assert_eq!(O_NOFOLLOW, 0o00400000);
        assert_eq!(O_NONBLOCK, 0o00004000);
        assert_eq!(PROTECTED_OPEN_FLAGS, 0o00404000);
    }

    #[test]
    fn effective_uid_is_the_second_of_four_canonical_fields() {
        assert_eq!(
            parse_effective_uid(b"Name:\triffdb\nUid:\t1 4294967295\t3  4\n"),
            Ok(u32::MAX)
        );
        assert_eq!(
            parse_effective_uid(b"Uid: 0\t0 0\t0"),
            Ok(0),
            "zero is the only canonical value with a leading zero byte"
        );
    }

    #[test]
    fn missing_multiple_and_malformed_uid_records_fail_closed() {
        for malformed in [
            b"Name:\triffdb\n".as_slice(),
            b"Uid:\t1 2 3 4\nUid:\t1 2 3 4\n",
            b"Uid:1 2 3 4\n",
            b"Uid:\t1 2 3\n",
            b"Uid:\t1 2 3 4 5\n",
            b"Uid:\t01 2 3 4\n",
            b"Uid:\t1 +2 3 4\n",
            b"Uid:\t1 4294967296 3 4\n",
            b"Uid:\t1 2 3 4 \n",
            b"Uid:\t1 2 3 4\r\n",
        ] {
            assert_eq!(
                parse_effective_uid(malformed),
                Err(ProtectedFileError::ProcStatusMalformed),
                "malformed proc status must reject: {malformed:?}"
            );
        }
    }

    #[test]
    fn proc_status_open_read_and_bound_failures_are_distinct() {
        let mut platform = FakePlatform::valid(b"secret");
        platform.proc_status = None;
        assert_load_error(&platform, 6, ProtectedFileError::ProcStatusUnavailable);

        let mut platform = FakePlatform::valid(b"secret");
        platform.proc_read_fails = true;
        assert_load_error(&platform, 6, ProtectedFileError::ProcStatusReadFailed);

        let mut platform = FakePlatform::valid(b"secret");
        let mut exact_bound = b"Uid:\t1234\t1234\t1234\t1234\n".to_vec();
        exact_bound.resize(PROC_STATUS_MAX_BYTES, b'x');
        platform.proc_status = Some(exact_bound);
        assert_eq!(
            load(&platform, 6)
                .expect("exact proc bound")
                .expose_secret(),
            b"secret"
        );

        let mut platform = FakePlatform::valid(b"secret");
        platform.proc_status = Some(vec![b'x'; PROC_STATUS_MAX_BYTES + 1]);
        assert_load_error(&platform, 6, ProtectedFileError::ProcStatusTooLarge);
    }

    #[test]
    fn preopen_symlink_or_other_nonregular_path_never_reaches_open() {
        let mut platform = FakePlatform::valid(b"secret");
        platform.before.as_mut().expect("metadata").is_regular = false;
        assert_load_error(&platform, 6, ProtectedFileError::PathNotRegularFile);
        assert_eq!(platform.seen_flags.get(), None);
    }

    #[test]
    fn metadata_and_open_time_races_fail_closed() {
        let mut platform = FakePlatform::valid(b"secret");
        platform.before = None;
        assert_load_error(&platform, 6, ProtectedFileError::PathMetadataUnavailable);

        let mut platform = FakePlatform::valid(b"secret");
        platform.file_bytes = None;
        assert_load_error(&platform, 6, ProtectedFileError::FileOpenFailed);
        assert_eq!(platform.seen_flags.get(), Some(PROTECTED_OPEN_FLAGS));

        let mut platform = FakePlatform::valid(b"secret");
        platform.opened = None;
        assert_load_error(&platform, 6, ProtectedFileError::OpenedMetadataUnavailable);
    }

    #[test]
    fn opened_handle_must_be_regular_owned_private_and_identity_stable() {
        let mut platform = FakePlatform::valid(b"secret");
        platform.opened.as_mut().expect("metadata").is_regular = false;
        assert_load_error(&platform, 6, ProtectedFileError::OpenedFileNotRegular);
        assert_eq!(platform.seen_flags.get(), Some(PROTECTED_OPEN_FLAGS));

        let mut platform = FakePlatform::valid(b"secret");
        platform.opened.as_mut().expect("metadata").uid += 1;
        assert_load_error(&platform, 6, ProtectedFileError::OwnerMismatch);

        let mut platform = FakePlatform::valid(b"secret");
        platform.opened.as_mut().expect("metadata").mode = 0o100640;
        assert_load_error(&platform, 6, ProtectedFileError::InsecureMode);

        let identity_changes: [fn(&mut FileMetadata); 2] = [
            |metadata: &mut FileMetadata| metadata.device += 1,
            |metadata: &mut FileMetadata| metadata.inode += 1,
        ];
        for change_identity in identity_changes {
            let mut platform = FakePlatform::valid(b"secret");
            change_identity(platform.opened.as_mut().expect("metadata"));
            assert_load_error(&platform, 6, ProtectedFileError::FileIdentityChanged);
        }
    }

    #[test]
    fn file_read_requires_eof_at_or_before_the_exact_limit() {
        let platform = FakePlatform::valid(b"secret");
        let document = load(&platform, 6).expect("exact-limit document");
        assert_eq!(document.expose_secret(), b"secret");
        assert_eq!(platform.seen_flags.get(), Some(PROTECTED_OPEN_FLAGS));

        let platform = FakePlatform::valid(b"secret+");
        assert_load_error(&platform, 6, ProtectedFileError::DocumentTooLarge);

        let mut platform = FakePlatform::valid(b"secret");
        platform.file_read_fails = true;
        assert_load_error(&platform, 6, ProtectedFileError::FileReadFailed);

        let platform = FakePlatform::valid(b"");
        assert_load_error(
            &platform,
            usize::MAX,
            ProtectedFileError::InvalidDocumentLimit,
        );

        let canary = FakePlatform::valid(b"protected-file-secret-canary");
        let document = load(&canary, 28).expect("redaction document");
        assert!(!format!("{document:?}").contains("secret-canary"));
        assert!(!document.to_string().contains("secret-canary"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn live_linux_loader_reads_private_file_and_rejects_final_symlink() {
        use std::fs;
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, symlink};
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

        let root = loop {
            let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let candidate = std::env::temp_dir().join(format!(
                "riffdb-protected-file-{}-{suffix}",
                std::process::id()
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("create isolated test directory: {error}"),
            }
        };
        let path = root.join("secret");
        let link = root.join("secret-link");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("create private file");
        file.write_all(b"secret").expect("write private file");
        drop(file);

        let document = read_protected_file(&path, 6).expect("read private file");
        assert_eq!(document.expose_secret(), b"secret");
        symlink(&path, &link).expect("create final-component symlink");
        assert_eq!(
            read_protected_file(&link, 6).err(),
            Some(ProtectedFileError::PathNotRegularFile)
        );

        fs::remove_dir_all(root).expect("remove isolated test directory");
    }
}
