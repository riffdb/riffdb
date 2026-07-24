use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(crate) const MAX_INPUT_BYTES: usize = 1_048_576;
pub(crate) const MAX_CONFIG_BYTES: usize = 65_536;
pub(crate) const MAX_PATH_BYTES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputError {
    PathInvalid,
    ReadFailed,
    TooLarge,
    Invalid,
}

pub(crate) fn validate_path(path: &OsStr) -> Result<(), InputError> {
    let bytes = path.as_encoded_bytes();
    if bytes.is_empty() || bytes.len() > MAX_PATH_BYTES || bytes.contains(&0) {
        return Err(InputError::PathInvalid);
    }
    Ok(())
}

pub(crate) fn read_path_or_stdin(
    path: &OsStr,
    stdin: &mut dyn Read,
    limit: usize,
) -> Result<Vec<u8>, InputError> {
    validate_path(path)?;
    if path.as_encoded_bytes() == b"-" {
        return read_bounded(stdin, limit);
    }
    let mut file = File::open(Path::new(path)).map_err(|_| InputError::ReadFailed)?;
    read_bounded(&mut file, limit)
}

pub(crate) fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>, InputError> {
    validate_path(path.as_os_str())?;
    let mut file = File::open(path).map_err(|_| InputError::ReadFailed)?;
    read_bounded(&mut file, limit)
}

pub(crate) fn read_bounded(reader: &mut dyn Read, limit: usize) -> Result<Vec<u8>, InputError> {
    let probe_limit = u64::try_from(limit)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(InputError::TooLarge)?;
    let mut bytes = Vec::with_capacity(limit.min(8 * 1024));
    reader
        .take(probe_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| InputError::ReadFailed)?;
    if bytes.len() > limit {
        return Err(InputError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn utf8(bytes: Vec<u8>) -> Result<String, InputError> {
    String::from_utf8(bytes).map_err(|_| InputError::Invalid)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn bounded_reader_accepts_limit_and_rejects_one_excess_byte() {
        let mut exact = Cursor::new(vec![b'x'; 16]);
        assert_eq!(read_bounded(&mut exact, 16).expect("exact").len(), 16);

        let mut excess = Cursor::new(vec![b'x'; 17]);
        assert_eq!(read_bounded(&mut excess, 16), Err(InputError::TooLarge));
    }

    #[test]
    fn every_cli_streaming_boundary_probes_exactly_one_excess_byte() {
        for limit in [MAX_INPUT_BYTES, MAX_CONFIG_BYTES, 132] {
            let mut exact = Cursor::new(vec![b'x'; limit]);
            assert_eq!(
                read_bounded(&mut exact, limit)
                    .expect("exact boundary")
                    .len(),
                limit
            );
            let mut excess = Cursor::new(vec![b'x'; limit + 1]);
            assert_eq!(read_bounded(&mut excess, limit), Err(InputError::TooLarge));
        }
        assert_eq!(utf8(vec![0xff]), Err(InputError::Invalid));
    }

    #[test]
    fn paths_are_nonempty_nul_free_and_bounded() {
        assert_eq!(validate_path(OsStr::new("")), Err(InputError::PathInvalid));
        assert_eq!(
            validate_path(OsStr::new("bad\0path")),
            Err(InputError::PathInvalid)
        );
        assert!(validate_path(OsStr::new(&"x".repeat(MAX_PATH_BYTES))).is_ok());
        assert_eq!(
            validate_path(OsStr::new(&"x".repeat(MAX_PATH_BYTES + 1))),
            Err(InputError::PathInvalid)
        );
    }
}
