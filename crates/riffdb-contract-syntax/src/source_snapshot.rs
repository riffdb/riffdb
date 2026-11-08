//! Numeric-only source locations from one exact bounded compiled snapshot.
use crate::limits::MAX_SOURCE_BYTES;
use std::fmt;

/// One-based human source location; tabs count as one Unicode scalar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourcePosition {
    line: u32,
    column: u32,
}
impl SourcePosition {
    /// One-based line.
    #[must_use]
    pub const fn line(self) -> u32 {
        self.line
    }
    /// One-based Unicode-scalar column.
    #[must_use]
    pub const fn column(self) -> u32 {
        self.column
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct Line {
    start: u32,
    crlf: bool,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct UnicodeDelta {
    start: u32,
    end: u32,
    scalar_extra: u32,
    utf16_extra: u32,
}
/// Checked numeric index; it retains no source text, literals or identifiers.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceSnapshotMap {
    bytes: u32,
    lines: Vec<Line>,
    unicode: Vec<UnicodeDelta>,
}
impl fmt::Debug for SourceSnapshotMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceSnapshotMap")
            .field("bytes", &self.bytes)
            .field("lines", &self.lines.len())
            .field("unicode_entries", &self.unicode.len())
            .finish()
    }
}
/// Source exceeds the existing parser source-byte bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceSnapshotBoundsError;
impl fmt::Display for SourceSnapshotBoundsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("source snapshot exceeds the parser mapping bound")
    }
}
impl std::error::Error for SourceSnapshotBoundsError {}
impl SourceSnapshotMap {
    /// Indexes the exact UTF-8 string passed to compilation, without filesystem I/O.
    pub fn new(source: &str) -> Result<Self, SourceSnapshotBoundsError> {
        if source.len() > MAX_SOURCE_BYTES {
            return Err(SourceSnapshotBoundsError);
        }
        let bytes = u32::try_from(source.len()).map_err(|_| SourceSnapshotBoundsError)?;
        let mut lines = vec![Line {
            start: 0,
            crlf: false,
        }];
        let mut unicode = Vec::new();
        let (mut scalar_extra, mut utf16_extra) = (0u32, 0u32);
        for (offset, character) in source.char_indices() {
            let start = offset as u32;
            let end = start + character.len_utf8() as u32;
            if !character.is_ascii() {
                scalar_extra += character.len_utf8() as u32 - 1;
                utf16_extra += (character.len_utf8() - character.len_utf16()) as u32;
                unicode.push(UnicodeDelta {
                    start,
                    end,
                    scalar_extra,
                    utf16_extra,
                });
            }
            if character == '\n' {
                if let Some(line) = lines.last_mut() {
                    line.crlf = offset > 0 && source.as_bytes()[offset - 1] == b'\r';
                }
                lines.push(Line {
                    start: end,
                    crlf: false,
                });
            }
        }
        Ok(Self {
            bytes,
            lines,
            unicode,
        })
    }
    fn extra(&self, byte: u32) -> Option<(u32, u32)> {
        if byte > self.bytes {
            return None;
        }
        let index = self.unicode.partition_point(|value| value.end <= byte);
        if self
            .unicode
            .get(index)
            .is_some_and(|value| value.start < byte)
        {
            return None;
        }
        Some(
            index
                .checked_sub(1)
                .and_then(|index| self.unicode.get(index))
                .map_or((0, 0), |value| (value.scalar_extra, value.utf16_extra)),
        )
    }
    fn line_index(&self, byte: u32) -> Option<usize> {
        self.extra(byte)?;
        self.lines
            .partition_point(|line| line.start <= byte)
            .checked_sub(1)
    }
    fn raw_line_end(&self, index: usize) -> Option<u32> {
        self.lines.get(index)?;
        self.lines
            .get(index + 1)
            .map_or(Some(self.bytes), |next| next.start.checked_sub(1))
    }
    fn units(&self, byte: u32) -> Option<u32> {
        byte.checked_sub(self.extra(byte)?.1)
    }
    /// Maps a valid byte boundary to human line/column. CRLF is one line break.
    #[must_use]
    pub fn position(&self, byte: u32) -> Option<SourcePosition> {
        let index = self.line_index(byte)?;
        let line = self.lines.get(index)?;
        let content_end = self
            .raw_line_end(index)?
            .checked_sub(u32::from(line.crlf))?;
        let byte = byte.min(content_end);
        let scalar = byte.checked_sub(self.extra(byte)?.0)?;
        let start_scalar = line.start.checked_sub(self.extra(line.start)?.0)?;
        Some(SourcePosition {
            line: u32::try_from(index).ok()?.checked_add(1)?,
            column: scalar.checked_sub(start_scalar)?.checked_add(1)?,
        })
    }
    /// Maps to the existing zero-based UTF-16 LSP coordinate convention.
    #[must_use]
    pub fn lsp_position(&self, byte: u32) -> Option<(u32, u32)> {
        let index = self.line_index(byte)?;
        let line = self.lines.get(index)?;
        Some((
            u32::try_from(index).ok()?,
            self.units(byte)?.checked_sub(self.units(line.start)?)?,
        ))
    }
    /// Clamps an internal diagnostic offset to a preceding valid source boundary.
    #[must_use]
    pub fn clamped_lsp_position(&self, byte: usize) -> (u32, u32) {
        let mut byte = byte.min(self.bytes as usize) as u32;
        let index = self.unicode.partition_point(|value| value.end <= byte);
        if let Some(value) = self.unicode.get(index)
            && value.start < byte
        {
            byte = value.start;
        }
        self.lsp_position(byte).unwrap_or((0, 0))
    }
    /// Inverts a zero-based UTF-16 coordinate, refusing a split surrogate pair.
    #[must_use]
    pub fn byte_for_lsp_position(&self, line: u32, character: u32) -> Option<u32> {
        let index = usize::try_from(line).ok()?;
        let start = self.lines.get(index)?.start;
        let units = self.units(start)?.checked_add(character)?;
        if units > self.units(self.raw_line_end(index)?)? {
            return None;
        }
        let index = self
            .unicode
            .partition_point(|value| value.end - value.utf16_extra <= units);
        let extra = index
            .checked_sub(1)
            .and_then(|index| self.unicode.get(index))
            .map_or(0, |value| value.utf16_extra);
        let byte = units.checked_add(extra)?;
        self.extra(byte)?;
        (byte >= start).then_some(byte)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    // req: DX-005, DX-012, ID-005
    #[test]
    fn source_locations_use_compiled_snapshot_and_preserve_utf16_lsp() {
        let mut source = "\tα😀\r\nxy\n".to_owned();
        let map = SourceSnapshotMap::new(&source).unwrap();
        source.clear();
        source.push_str("changed-file-canary");
        for (byte, line, column) in [
            (0, 1, 1),
            (1, 1, 2),
            (3, 1, 3),
            (7, 1, 4),
            (8, 1, 4),
            (9, 2, 1),
            (10, 2, 2),
            (11, 2, 3),
            (12, 3, 1),
        ] {
            let actual = map.position(byte).unwrap();
            assert_eq!((actual.line(), actual.column()), (line, column));
        }
        assert_eq!(map.lsp_position(7), Some((0, 4)));
        assert_eq!(map.lsp_position(8), Some((0, 5)));
        assert_eq!(map.lsp_position(12), Some((2, 0)));
        assert_eq!(map.byte_for_lsp_position(0, 3), None);
        assert_eq!(map.byte_for_lsp_position(0, 4), Some(7));
        assert_eq!(map.byte_for_lsp_position(2, 0), Some(12));
        assert_eq!(map.position(2), None);
        assert_eq!(map.position(13), None);
        assert!(!format!("{map:?}").contains("changed-file-canary"));
    }
    // req: DX-005, DX-012, ID-005
    #[test]
    fn empty_eof_and_every_unicode_boundary_round_trip_without_source_retention() {
        for source in ["", "a", "a\r\n", "\n\n", "\tα😀\r\nxy\n", "a😀z", "éé\né"] {
            let map = SourceSnapshotMap::new(source).unwrap();
            for byte in source
                .char_indices()
                .map(|(byte, _)| byte)
                .chain(std::iter::once(source.len()))
            {
                let (line, column) = map.lsp_position(byte as u32).unwrap();
                assert_eq!(map.byte_for_lsp_position(line, column), Some(byte as u32));
            }
            assert!(map.position(source.len() as u32).is_some());
        }
        assert!(SourceSnapshotMap::new(&"x".repeat(MAX_SOURCE_BYTES + 1)).is_err());
    }
}
