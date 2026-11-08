//! Protected presentation-only bearer credential loading.

use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;

use zeroize::Zeroizing;

use crate::BearerCredential;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::{File, OpenOptions};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const BEARER_PRESENTATION_BYTES: usize = 43;
const PROC_STATUS_MAX_BYTES: usize = 65_536;
#[cfg(target_os = "linux")]
const PROC_STATUS_PATH: &str = "/proc/self/status";
#[cfg(any(test, not(target_os = "macos")))]
const LINUX_O_NOFOLLOW: i32 = 0o00400000;
#[cfg(any(test, not(target_os = "macos")))]
const LINUX_O_NONBLOCK: i32 = 0o00004000;
#[cfg(any(test, target_os = "macos"))]
const MACOS_O_NOFOLLOW: i32 = 0x0100;
#[cfg(any(test, target_os = "macos"))]
const MACOS_O_NONBLOCK: i32 = 0x0004;

#[cfg(target_os = "macos")]
const O_NOFOLLOW: i32 = MACOS_O_NOFOLLOW;
#[cfg(not(target_os = "macos"))]
const O_NOFOLLOW: i32 = LINUX_O_NOFOLLOW;

#[cfg(target_os = "macos")]
const O_NONBLOCK: i32 = MACOS_O_NONBLOCK;
#[cfg(not(target_os = "macos"))]
const O_NONBLOCK: i32 = LINUX_O_NONBLOCK;

const PROTECTED_OPEN_FLAGS: i32 = O_NOFOLLOW | O_NONBLOCK;

/// A closed, redaction-safe failure to load one protected bearer credential.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum BearerCredentialFileError {
    /// The exact protected-file procedure is unavailable on this platform.
    UnsupportedPlatform,
    /// The file, its metadata, or the effective-user source failed closed.
    ProtectedFileRejected,
    /// The exact 43-byte file is not a valid bearer presentation.
    InvalidPresentation,
}

impl fmt::Debug for BearerCredentialFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedPlatform => "UnsupportedPlatform",
            Self::ProtectedFileRejected => "ProtectedFileRejected",
            Self::InvalidPresentation => "InvalidPresentation",
        })
    }
}

impl fmt::Display for BearerCredentialFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedPlatform => {
                "protected bearer credential files are unsupported on this platform"
            }
            Self::ProtectedFileRejected => "protected bearer credential file was rejected",
            Self::InvalidPresentation => "bearer credential presentation is invalid",
        })
    }
}

impl Error for BearerCredentialFileError {}

/// Loads one exact bearer presentation through the protected Unix file rule.
///
/// The loader performs no token decoding, hashing, authentication, or
/// capability lookup. All temporary credential bytes are bounded and zeroized.
pub fn load_protected_bearer_credential(
    path: &Path,
) -> Result<BearerCredential, BearerCredentialFileError> {
    load_protected_bearer_credential_diagnosed(path).map_err(|failure| failure.kind())
}

/// Loads through the same protected procedure and preserves the failing check.
/// The failure contains no submitted path, metadata values or presentation.
pub fn load_protected_bearer_credential_diagnosed(
    path: &Path,
) -> Result<BearerCredential, BearerCredentialFileFailure> {
    #[cfg(target_os = "linux")]
    {
        load_with_diagnostics(&LinuxPlatform, path)
    }
    #[cfg(target_os = "macos")]
    {
        load_with_diagnostics(&MacOsPlatform, path)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = path;
        Err(BearerCredentialFileFailure::new(
            BearerCredentialFileCause::UnsupportedPlatform,
        ))
    }
}

/// Closed cause observed by the original protected-file check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BearerCredentialFileCause {
    /// The platform cannot implement the protected procedure.
    UnsupportedPlatform,
    /// A file or ancestor was absent at an actual metadata/open operation.
    NotFound,
    /// A metadata/open/read operation was refused for permissions.
    ReadDenied,
    /// The opened target was not a regular file (including links).
    NotRegular,
    /// The opened file belonged to another effective user.
    WrongOwner,
    /// Group or other users had file access.
    Permissions,
    /// The opened file identity differed from the original observation.
    FileChanged,
    /// The bounded read did not contain exactly one 43-byte presentation.
    InvalidLength,
    /// The exact presentation failed the existing bearer validator.
    InvalidPresentation,
    /// The effective user could not be established under its bounded rule.
    EffectiveUserUnavailable,
    /// Another local I/O or metadata observation was unavailable.
    Unavailable,
}
impl BearerCredentialFileCause {
    /// Stable value-free diagnostic spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::NotFound => "not_found",
            Self::ReadDenied => "read_denied",
            Self::NotRegular => "not_regular",
            Self::WrongOwner => "wrong_owner",
            Self::Permissions => "permissions",
            Self::FileChanged => "file_changed",
            Self::InvalidLength => "invalid_length",
            Self::InvalidPresentation => "invalid_presentation",
            Self::EffectiveUserUnavailable => "effective_user_unavailable",
            Self::Unavailable => "unavailable",
        }
    }
    /// The checked rule, without file metadata or supplied values.
    #[must_use]
    pub const fn expected(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "supported protected-file platform",
            Self::NotFound => "selected credential file exists",
            Self::ReadDenied => "selected credential file is readable by its owner",
            Self::NotRegular => "regular file without a final symlink",
            Self::WrongOwner => "file owned by the current effective user",
            Self::Permissions => "no group or other file access",
            Self::FileChanged => "unchanged device and inode during protected open",
            Self::InvalidLength => "exact 43-byte capability token without trailing newline",
            Self::InvalidPresentation => "valid canonical bearer presentation",
            Self::EffectiveUserUnavailable => "one bounded canonical effective-user observation",
            Self::Unavailable => "available protected-file metadata and read operations",
        }
    }
    /// Safe repair guidance; never asks to display the credential.
    #[must_use]
    pub const fn fix(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "use a supported Unix protected-file deployment",
            Self::NotFound => "restore the selected protected credential file",
            Self::ReadDenied => "restore owner read access to the selected credential file",
            Self::NotRegular => "select an owned regular credential file without a final symlink",
            Self::WrongOwner => "use a private credential file owned by the current effective user",
            Self::Permissions => "set credential file mode to 0600",
            Self::FileChanged => "retry after restoring stable protected credential-file custody",
            Self::InvalidLength | Self::InvalidPresentation => {
                "retain the exact issued capability token without a trailing newline; never print it"
            }
            Self::EffectiveUserUnavailable => {
                "restore the platform effective-user source; do not bypass ownership checks"
            }
            Self::Unavailable => "restore protected-file availability and retry",
        }
    }
}

/// Diagnostic extension retaining the legacy failure class and exact safe cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BearerCredentialFileFailure {
    kind: BearerCredentialFileError,
    cause: BearerCredentialFileCause,
}
impl BearerCredentialFileFailure {
    const fn new(cause: BearerCredentialFileCause) -> Self {
        let kind = match cause {
            BearerCredentialFileCause::UnsupportedPlatform => {
                BearerCredentialFileError::UnsupportedPlatform
            }
            BearerCredentialFileCause::InvalidPresentation => {
                BearerCredentialFileError::InvalidPresentation
            }
            _ => BearerCredentialFileError::ProtectedFileRejected,
        };
        Self { kind, cause }
    }
    /// Unchanged legacy failure classification.
    #[must_use]
    pub const fn kind(self) -> BearerCredentialFileError {
        self.kind
    }
    /// The original value-free failing check.
    #[must_use]
    pub const fn cause(self) -> BearerCredentialFileCause {
        self.cause
    }
}
impl fmt::Display for BearerCredentialFileFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "protected bearer credential rejected: {}",
            self.cause.as_str()
        )
    }
}
impl Error for BearerCredentialFileFailure {}

fn io_failure(error: io::Error) -> BearerCredentialFileFailure {
    BearerCredentialFileFailure::new(match error.kind() {
        io::ErrorKind::NotFound => BearerCredentialFileCause::NotFound,
        io::ErrorKind::PermissionDenied => BearerCredentialFileCause::ReadDenied,
        _ => BearerCredentialFileCause::Unavailable,
    })
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

#[cfg(target_os = "macos")]
struct MacOsPlatform;

#[cfg(target_os = "macos")]
impl ProtectedFilePlatform for MacOsPlatform {
    type ProcStatusReader = io::Cursor<Vec<u8>>;
    type OpenedFile = File;

    fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader> {
        let uid = macos_effective_uid()?;
        Ok(io::Cursor::new(
            format!("Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n").into_bytes(),
        ))
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

#[cfg(target_os = "macos")]
fn macos_effective_uid() -> io::Result<u32> {
    use std::process::Command;
    use std::sync::OnceLock;

    static CACHED_EUID: OnceLock<u32> = OnceLock::new();
    if let Some(uid) = CACHED_EUID.get() {
        return Ok(*uid);
    }
    let output = Command::new("/usr/bin/id").arg("-u").output()?;
    if !output.status.success() {
        return Err(io::Error::other("id -u failed"));
    }
    let trimmed = std::str::from_utf8(&output.stdout)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "id -u non-utf8"))?
        .trim();
    let uid = trimmed
        .parse::<u32>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "id -u invalid u32"))?;
    let _ = CACHED_EUID.set(uid);
    Ok(uid)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn metadata_snapshot(metadata: std::fs::Metadata) -> FileMetadata {
    FileMetadata {
        is_regular: metadata.is_file(),
        uid: metadata.uid(),
        mode: metadata.mode(),
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(test)]
fn load_with<P: ProtectedFilePlatform>(
    platform: &P,
    path: &Path,
) -> Result<BearerCredential, BearerCredentialFileError> {
    load_with_diagnostics(platform, path).map_err(|failure| failure.kind())
}
fn load_with_diagnostics<P: ProtectedFilePlatform>(
    platform: &P,
    path: &Path,
) -> Result<BearerCredential, BearerCredentialFileFailure> {
    use BearerCredentialFileCause as Cause;
    let effective_uid = read_effective_uid(platform)?;
    let before = platform.symlink_metadata(path).map_err(io_failure)?;
    if !before.is_regular {
        return Err(BearerCredentialFileFailure::new(Cause::NotRegular));
    }
    let mut file = platform
        .open_file(path, PROTECTED_OPEN_FLAGS)
        .map_err(io_failure)?;
    let opened = platform.opened_metadata(&file).map_err(io_failure)?;
    if !opened.is_regular {
        return Err(BearerCredentialFileFailure::new(Cause::NotRegular));
    }
    if opened.uid != effective_uid {
        return Err(BearerCredentialFileFailure::new(Cause::WrongOwner));
    }
    if opened.mode & 0o077 != 0 {
        return Err(BearerCredentialFileFailure::new(Cause::Permissions));
    }
    if (opened.device, opened.inode) != (before.device, before.inode) {
        return Err(BearerCredentialFileFailure::new(Cause::FileChanged));
    }
    let mut presentation = Zeroizing::new(Vec::with_capacity(BEARER_PRESENTATION_BYTES + 1));
    file.by_ref()
        .take((BEARER_PRESENTATION_BYTES + 1) as u64)
        .read_to_end(&mut presentation)
        .map_err(io_failure)?;
    if presentation.len() != BEARER_PRESENTATION_BYTES {
        return Err(BearerCredentialFileFailure::new(Cause::InvalidLength));
    }
    let presentation = std::str::from_utf8(&presentation)
        .map_err(|_| BearerCredentialFileFailure::new(Cause::InvalidPresentation))?;
    BearerCredential::new(presentation)
        .map_err(|_| BearerCredentialFileFailure::new(Cause::InvalidPresentation))
}
fn read_effective_uid<P: ProtectedFilePlatform>(
    platform: &P,
) -> Result<u32, BearerCredentialFileFailure> {
    let unavailable =
        || BearerCredentialFileFailure::new(BearerCredentialFileCause::EffectiveUserUnavailable);
    let mut status = platform.open_proc_status().map_err(|_| unavailable())?;
    let mut bytes = Vec::with_capacity(8 * 1024);
    status
        .by_ref()
        .take((PROC_STATUS_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    if bytes.len() > PROC_STATUS_MAX_BYTES {
        return Err(unavailable());
    }
    parse_effective_uid(&bytes).ok_or_else(unavailable)
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    const TOKEN: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    const TEST_PATH: &str = "/not-opened-by-test-platform";
    const EFFECTIVE_UID: u32 = 1_234;

    struct InjectedReader {
        bytes: Vec<u8>,
        position: usize,
        fail_at: Option<usize>,
    }

    impl InjectedReader {
        fn new(bytes: Vec<u8>, fail_at: Option<usize>) -> Self {
            Self {
                bytes,
                position: 0,
                fail_at,
            }
        }
    }

    impl Read for InjectedReader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.fail_at == Some(self.position) {
                return Err(io::Error::other("injected read failure"));
            }
            if self.position == self.bytes.len() || output.is_empty() {
                return Ok(0);
            }

            let readable_end = self
                .fail_at
                .unwrap_or(self.bytes.len())
                .min(self.bytes.len());
            let count = output.len().min(readable_end.saturating_sub(self.position));
            if count == 0 {
                return Err(io::Error::other("injected read failure"));
            }
            output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
            self.position += count;
            Ok(count)
        }
    }

    struct TestPlatform {
        proc_status: io::Result<Vec<u8>>,
        proc_status_fail_at: Option<usize>,
        before: io::Result<FileMetadata>,
        opened: io::Result<FileMetadata>,
        bytes: io::Result<Vec<u8>>,
        bytes_fail_at: Option<usize>,
        flags: Cell<Option<i32>>,
        metadata_reads: Cell<u32>,
    }

    impl TestPlatform {
        fn valid() -> Self {
            let metadata = FileMetadata {
                is_regular: true,
                uid: EFFECTIVE_UID,
                mode: 0o100600,
                device: 11,
                inode: 17,
            };
            Self {
                proc_status: Ok(b"Name:\triffdb\nUid:\t1234\t1234  1234\t1234\n".to_vec()),
                proc_status_fail_at: None,
                before: Ok(metadata),
                opened: Ok(metadata),
                bytes: Ok(TOKEN.to_vec()),
                bytes_fail_at: None,
                flags: Cell::new(None),
                metadata_reads: Cell::new(0),
            }
        }
    }

    impl ProtectedFilePlatform for TestPlatform {
        type ProcStatusReader = InjectedReader;
        type OpenedFile = InjectedReader;

        fn open_proc_status(&self) -> io::Result<Self::ProcStatusReader> {
            self.proc_status
                .as_ref()
                .map(|bytes| InjectedReader::new(bytes.clone(), self.proc_status_fail_at))
                .map_err(|error| io::Error::new(error.kind(), "injected"))
        }

        fn symlink_metadata(&self, _path: &Path) -> io::Result<FileMetadata> {
            self.metadata_reads.set(self.metadata_reads.get() + 1);
            self.before
                .as_ref()
                .copied()
                .map_err(|error| io::Error::new(error.kind(), "injected"))
        }

        fn open_file(&self, _path: &Path, flags: i32) -> io::Result<Self::OpenedFile> {
            self.flags.set(Some(flags));
            self.bytes
                .as_ref()
                .map(|bytes| InjectedReader::new(bytes.clone(), self.bytes_fail_at))
                .map_err(|error| io::Error::new(error.kind(), "injected"))
        }

        fn opened_metadata(&self, _file: &Self::OpenedFile) -> io::Result<FileMetadata> {
            self.opened
                .as_ref()
                .copied()
                .map_err(|error| io::Error::new(error.kind(), "injected"))
        }
    }

    #[test]
    fn exact_linux_bounds_flags_and_presentation_are_frozen() {
        assert_eq!(PROC_STATUS_MAX_BYTES, 65_536);
        assert_eq!(BEARER_PRESENTATION_BYTES, 43);
        assert_eq!(LINUX_O_NOFOLLOW, 0o00400000);
        assert_eq!(LINUX_O_NONBLOCK, 0o00004000);
        assert_eq!(LINUX_O_NOFOLLOW | LINUX_O_NONBLOCK, 0o00404000);
        assert_eq!(MACOS_O_NOFOLLOW, 0x0100);
        assert_eq!(MACOS_O_NONBLOCK, 0x0004);
        assert_eq!(MACOS_O_NOFOLLOW | MACOS_O_NONBLOCK, 0x0104);
        assert_eq!(PROTECTED_OPEN_FLAGS, O_NOFOLLOW | O_NONBLOCK);

        let platform = TestPlatform::valid();
        let loaded = load_with(&platform, Path::new(TEST_PATH)).expect("credential");
        let expected = BearerCredential::new(std::str::from_utf8(TOKEN).expect("ASCII"))
            .expect("expected credential");
        assert!(loaded.has_same_presentation(&expected));
        assert_eq!(platform.flags.get(), Some(PROTECTED_OPEN_FLAGS));
    }

    #[test]
    fn effective_uid_requires_one_canonical_four_field_line() {
        assert_eq!(
            parse_effective_uid(b"Name:\triffdb\nUid:\t1 4294967295\t3  4\n"),
            Some(u32::MAX)
        );
        assert_eq!(parse_effective_uid(b"Uid: 0\t0 0\t0"), Some(0));
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
            assert_eq!(parse_effective_uid(malformed), None);
        }
    }

    #[test]
    fn proc_status_must_reach_eof_within_the_exact_bound() {
        let mut exact = b"Uid:\t1234\t1234\t1234\t1234\n".to_vec();
        exact.resize(PROC_STATUS_MAX_BYTES, b'x');
        let mut platform = TestPlatform::valid();
        platform.proc_status = Ok(exact);
        assert!(load_with(&platform, Path::new(TEST_PATH)).is_ok());

        let mut over = b"Uid:\t1234\t1234\t1234\t1234\n".to_vec();
        over.resize(PROC_STATUS_MAX_BYTES + 1, b'x');
        let mut platform = TestPlatform::valid();
        platform.proc_status = Ok(over);
        assert_eq!(
            load_with(&platform, Path::new(TEST_PATH)).err(),
            Some(BearerCredentialFileError::ProtectedFileRejected)
        );
    }

    #[test]
    fn proc_status_failures_stop_before_the_credential_open() {
        let canonical = b"Uid:\t1234\t1234\t1234\t1234\n";
        for fail_at in [0, canonical.len() / 2, canonical.len()] {
            let mut platform = TestPlatform::valid();
            platform.proc_status = Ok(canonical.to_vec());
            platform.proc_status_fail_at = Some(fail_at);
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected)
            );
            assert_eq!(platform.flags.get(), None);
        }

        let mut unavailable = TestPlatform::valid();
        unavailable.proc_status = Err(io::Error::new(
            io::ErrorKind::NotFound,
            "secret proc failure canary",
        ));
        assert_eq!(
            load_with(&unavailable, Path::new(TEST_PATH)).err(),
            Some(BearerCredentialFileError::ProtectedFileRejected)
        );
        assert_eq!(unavailable.flags.get(), None);

        for malformed in [
            b"Name:\triffdb\n".as_slice(),
            b"Uid:\t1 2 3 4\nUid:\t1 2 3 4\n",
            b"Uid:\t1 4294967296 3 4\n",
        ] {
            let mut platform = TestPlatform::valid();
            platform.proc_status = Ok(malformed.to_vec());
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected)
            );
            assert_eq!(platform.flags.get(), None);
        }
    }

    #[test]
    fn path_open_handle_owner_mode_and_identity_failures_are_closed() {
        let mutations: [fn(&mut TestPlatform); 8] = [
            |platform| platform.before = Err(io::Error::other("pre-open metadata failure")),
            |platform| platform.before.as_mut().expect("metadata").is_regular = false,
            |platform| platform.bytes = Err(io::Error::other("open failure")),
            |platform| platform.opened = Err(io::Error::other("opened metadata failure")),
            |platform| platform.opened.as_mut().expect("metadata").is_regular = false,
            |platform| platform.opened.as_mut().expect("metadata").uid += 1,
            |platform| platform.opened.as_mut().expect("metadata").device += 1,
            |platform| platform.opened.as_mut().expect("metadata").inode += 1,
        ];
        for mutate in mutations {
            let mut platform = TestPlatform::valid();
            mutate(&mut platform);
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected)
            );
        }

        let mut preopen_nonregular = TestPlatform::valid();
        preopen_nonregular
            .before
            .as_mut()
            .expect("metadata")
            .is_regular = false;
        assert_eq!(
            load_with(&preopen_nonregular, Path::new(TEST_PATH)).err(),
            Some(BearerCredentialFileError::ProtectedFileRejected)
        );
        assert_eq!(preopen_nonregular.flags.get(), None);

        for forbidden_bit in [0o001, 0o002, 0o004, 0o010, 0o020, 0o040] {
            let mut platform = TestPlatform::valid();
            platform.opened.as_mut().expect("metadata").mode |= forbidden_bit;
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected),
                "mode bit {forbidden_bit:#05o} must reject"
            );
        }
    }

    #[test]
    fn symlink_and_replacement_races_fail_before_a_credential_is_returned() {
        let mut raced_to_symlink = TestPlatform::valid();
        raced_to_symlink.bytes = Err(io::Error::other("O_NOFOLLOW raced symlink"));
        assert_eq!(
            load_with(&raced_to_symlink, Path::new(TEST_PATH)).err(),
            Some(BearerCredentialFileError::ProtectedFileRejected)
        );
        assert_eq!(raced_to_symlink.flags.get(), Some(O_NOFOLLOW | O_NONBLOCK));

        for replace in [
            |metadata: &mut FileMetadata| metadata.device += 1,
            |metadata: &mut FileMetadata| metadata.inode += 1,
            |metadata: &mut FileMetadata| metadata.is_regular = false,
        ] {
            let mut platform = TestPlatform::valid();
            replace(platform.opened.as_mut().expect("opened metadata"));
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected)
            );
        }
    }

    #[test]
    fn credential_requires_exactly_43_bytes_and_a_confirmed_final_eof() {
        for bytes in [TOKEN[..42].to_vec(), [TOKEN, b"A"].concat()] {
            let mut platform = TestPlatform::valid();
            platform.bytes = Ok(bytes);
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected)
            );
        }

        for fail_at in [0, BEARER_PRESENTATION_BYTES / 2, BEARER_PRESENTATION_BYTES] {
            let mut platform = TestPlatform::valid();
            platform.bytes_fail_at = Some(fail_at);
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(BearerCredentialFileError::ProtectedFileRejected),
                "read failure at byte {fail_at} must reject"
            );
        }
    }

    #[test]
    fn every_invalid_alphabet_byte_rejects_at_every_presentation_position() {
        for position in 0..BEARER_PRESENTATION_BYTES {
            for byte in u8::MIN..=u8::MAX {
                if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
                    continue;
                }
                let mut bytes = TOKEN.to_vec();
                bytes[position] = byte;
                let mut platform = TestPlatform::valid();
                platform.bytes = Ok(bytes);
                assert_eq!(
                    load_with(&platform, Path::new(TEST_PATH)).err(),
                    Some(BearerCredentialFileError::InvalidPresentation),
                    "byte {byte:#04x} at position {position} must reject"
                );
            }
        }
    }

    #[test]
    fn presentation_valid_noncanonical_token_is_left_for_server_rejection() {
        let noncanonical = vec![b'A'; BEARER_PRESENTATION_BYTES];
        let mut platform = TestPlatform::valid();
        platform.bytes = Ok(noncanonical.clone());
        let loaded = load_with(&platform, Path::new(TEST_PATH))
            .expect("presentation-only validation accepts URL-safe alphabet text");
        let expected =
            BearerCredential::new(std::str::from_utf8(&noncanonical).expect("ASCII presentation"))
                .expect("same presentation is accepted by the metadata boundary");
        assert!(loaded.has_same_presentation(&expected));
    }

    #[test]
    fn public_errors_are_fixed_and_redacted() {
        let canary = "credential-file-secret-canary";
        for error in [
            BearerCredentialFileError::UnsupportedPlatform,
            BearerCredentialFileError::ProtectedFileRejected,
            BearerCredentialFileError::InvalidPresentation,
        ] {
            assert!(!format!("{error:?}").contains(canary));
            assert!(!error.to_string().contains(canary));
            assert!(Error::source(&error).is_none());
        }
    }
    // req: DX-003, DX-005, ID-005
    #[test]
    fn protected_bearer_diagnostics_preserve_original_checks_without_restat() {
        use BearerCredentialFileCause as Cause;
        let mut cases = Vec::new();
        let mut p = TestPlatform::valid();
        p.before = Err(io::ErrorKind::NotFound.into());
        cases.push((p, Cause::NotFound));
        let mut p = TestPlatform::valid();
        p.before.as_mut().unwrap().is_regular = false;
        cases.push((p, Cause::NotRegular));
        let mut p = TestPlatform::valid();
        p.opened.as_mut().unwrap().uid += 1;
        cases.push((p, Cause::WrongOwner));
        let mut p = TestPlatform::valid();
        p.opened.as_mut().unwrap().mode = 0o100644;
        cases.push((p, Cause::Permissions));
        let mut p = TestPlatform::valid();
        p.opened.as_mut().unwrap().inode += 1;
        cases.push((p, Cause::FileChanged));
        let mut p = TestPlatform::valid();
        p.bytes = Ok(vec![b'A'; 44]);
        cases.push((p, Cause::InvalidLength));
        let mut p = TestPlatform::valid();
        p.bytes = Ok(vec![b'!'; 43]);
        cases.push((p, Cause::InvalidPresentation));
        for (platform, cause) in cases {
            let failure = load_with_diagnostics(&platform, Path::new(TEST_PATH))
                .err()
                .unwrap();
            assert_eq!(failure.cause(), cause);
            assert_eq!(
                platform.metadata_reads.get(),
                1,
                "the reported cause is from the original procedure"
            );
            assert_eq!(
                load_with(&platform, Path::new(TEST_PATH)).err(),
                Some(failure.kind()),
                "the legacy classification is unchanged"
            );
            let rendered = format!("{failure:?} {failure}");
            assert!(!rendered.contains(TEST_PATH));
            assert!(!rendered.contains(std::str::from_utf8(TOKEN).unwrap()));
        }
        let mut platform = TestPlatform::valid();
        platform.proc_status = Ok(b"invalid".to_vec());
        assert_eq!(
            load_with_diagnostics(&platform, Path::new(TEST_PATH))
                .err()
                .unwrap()
                .cause(),
            Cause::EffectiveUserUnavailable
        );
        assert_eq!(platform.metadata_reads.get(), 0);
    }
}
