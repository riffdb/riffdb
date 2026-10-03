// req: OQ-040, PRJ-004
use crate::IrValidationError;

pub(crate) struct Writer {
    bytes: Vec<u8>,
    maximum: usize,
    counted: Option<usize>,
}

impl Writer {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            counted: None,
        }
    }

    /// Borrowed sizing uses the same wire traversal without an output allocation.
    pub(crate) fn counting(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            counted: Some(0),
        }
    }

    /// Caller has already admitted this exact output allocation.
    pub(crate) fn with_capacity(maximum: usize, capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            maximum,
            counted: None,
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.counted.unwrap_or(self.bytes.len())
    }

    fn checked_append(&self, length: usize) -> Result<usize, IrValidationError> {
        let actual =
            self.encoded_len()
                .checked_add(length)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "canonical IR",
                })?;
        if actual > self.maximum {
            return Err(IrValidationError::LimitExceeded {
                kind: "canonical IR",
                actual,
                maximum: self.maximum,
            });
        }
        Ok(actual)
    }

    /// Advance only a sizing writer over an already sized borrowed encoding.
    pub(crate) fn count_bytes(&mut self, length: usize) -> Result<(), IrValidationError> {
        let actual = self.checked_append(length)?;
        match &mut self.counted {
            Some(counted) => {
                *counted = actual;
                Ok(())
            }
            None => Err(IrValidationError::InvalidText {
                kind: "IR sizing writer",
            }),
        }
    }

    /// Append the existing canonical value document directly into this writer.
    /// Its exact-length preflight is borrowed and validates the same value bounds.
    /// A counting writer performs neither document nor enclosing-buffer allocation.
    pub(crate) fn canonical_value(
        &mut self,
        value: &riffdb_types::CanonicalValue,
        invalid: IrValidationError,
    ) -> Result<(), IrValidationError> {
        let length =
            riffdb_types::canonical_value_encoded_len(value).map_err(|_| invalid.clone())?;
        self.u32(
            u32::try_from(length).map_err(|_| IrValidationError::SizeOverflow {
                kind: "canonical IR",
            })?,
        )?;
        let actual = self.checked_append(length)?;
        if let Some(counted) = &mut self.counted {
            *counted = actual;
        } else {
            riffdb_types::encode_canonical_value_into(&mut self.bytes, value)
                .map_err(|_| invalid)?;
        }
        Ok(())
    }

    pub(crate) fn u8(&mut self, value: u8) -> Result<(), IrValidationError> {
        self.raw(&[value])
    }

    pub(crate) fn bool(&mut self, value: bool) -> Result<(), IrValidationError> {
        self.u8(u8::from(value))
    }

    pub(crate) fn u32(&mut self, value: u32) -> Result<(), IrValidationError> {
        self.raw(&value.to_be_bytes())
    }

    pub(crate) fn u64(&mut self, value: u64) -> Result<(), IrValidationError> {
        self.raw(&value.to_be_bytes())
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), IrValidationError> {
        let length = u32::try_from(value.len()).map_err(|_| IrValidationError::LimitExceeded {
            kind: "encoded byte string",
            actual: value.len(),
            maximum: u32::MAX as usize,
        })?;
        self.u32(length)?;
        self.raw(value)
    }

    pub(crate) fn string(&mut self, value: &str) -> Result<(), IrValidationError> {
        self.bytes(value.as_bytes())
    }

    pub(crate) fn raw(&mut self, value: &[u8]) -> Result<(), IrValidationError> {
        let actual = self.checked_append(value.len())?;
        if let Some(counted) = &mut self.counted {
            *counted = actual;
        } else {
            self.bytes.extend_from_slice(value);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, IrValidationError> {
        Ok(self.read(1)?[0])
    }

    pub(crate) fn bool(&mut self) -> Result<bool, IrValidationError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(IrValidationError::UnknownTag {
                kind: "Boolean",
                tag,
            }),
        }
    }

    pub(crate) fn u32(&mut self) -> Result<u32, IrValidationError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    pub(crate) fn peek_u32(&self) -> Result<u32, IrValidationError> {
        let end = self
            .position
            .checked_add(4)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        Ok(u32::from_be_bytes(
            self.bytes
                .get(self.position..end)
                .ok_or(IrValidationError::UnexpectedEnd)?
                .try_into()
                .map_err(|_| IrValidationError::UnexpectedEnd)?,
        ))
    }

    pub(crate) fn peek_u64(&self) -> Result<u64, IrValidationError> {
        let end = self
            .position
            .checked_add(8)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        Ok(u64::from_be_bytes(
            self.bytes
                .get(self.position..end)
                .ok_or(IrValidationError::UnexpectedEnd)?
                .try_into()
                .map_err(|_| IrValidationError::UnexpectedEnd)?,
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, IrValidationError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    pub(crate) fn bytes(&mut self, maximum: usize) -> Result<&'a [u8], IrValidationError> {
        let length = self.u32()? as usize;
        if length > maximum {
            return Err(IrValidationError::LimitExceeded {
                kind: "encoded byte string",
                actual: length,
                maximum,
            });
        }
        self.read(length)
    }

    pub(crate) fn string(&mut self, maximum: usize) -> Result<String, IrValidationError> {
        let value = self.bytes(maximum)?;
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| IrValidationError::InvalidText { kind: "IR" })
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], IrValidationError> {
        self.read(N)?
            .try_into()
            .map_err(|_| IrValidationError::UnexpectedEnd)
    }

    pub(crate) fn read(&mut self, length: usize) -> Result<&'a [u8], IrValidationError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(IrValidationError::UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    pub(crate) fn finish(self) -> Result<(), IrValidationError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(IrValidationError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    // req: OQ-040, PRJ-004
    use super::*;
    use riffdb_types::{CanonicalString, CanonicalValue, encode_canonical_value};

    #[test]
    fn canonical_constant_count_and_direct_append_preserve_document_and_error() {
        let invalid = IrValidationError::TypeMismatch {
            context: "expression constant encoding",
        };
        for value in [
            CanonicalValue::Null,
            CanonicalValue::Bool(true),
            CanonicalValue::U64(u64::MAX),
            CanonicalValue::String(CanonicalString::new("hello".repeat(512)).unwrap()),
        ] {
            let document = encode_canonical_value(&value).unwrap();
            let expected = document.len() + 4;
            let mut counter = Writer::counting(expected);
            counter.canonical_value(&value, invalid.clone()).unwrap();
            assert_eq!(counter.encoded_len(), expected);
            assert_eq!(counter.finish().capacity(), 0);
            let mut writer = Writer::with_capacity(expected, expected);
            let capacity = writer.bytes.capacity();
            writer.canonical_value(&value, invalid.clone()).unwrap();
            assert_eq!(writer.bytes.capacity(), capacity);
            let bytes = writer.finish();
            assert_eq!(&bytes[..4], &(document.len() as u32).to_be_bytes());
            assert_eq!(&bytes[4..], document);
            let mut short = Writer::counting(expected - 1);
            assert_eq!(
                short.canonical_value(&value, invalid.clone()),
                Err(IrValidationError::LimitExceeded {
                    kind: "canonical IR",
                    actual: expected,
                    maximum: expected - 1,
                })
            );
            assert_eq!(short.finish().capacity(), 0);
        }
        // The scalar fits its own 1 MiB limit, but its document framing exceeds
        // the ordinary complete-document limit. Keep the original IR mapping.
        let too_large = CanonicalValue::String(
            CanonicalString::new("x".repeat(riffdb_types::MAX_STRING_BYTES)).unwrap(),
        );
        let mut counter = Writer::counting(usize::MAX);
        assert_eq!(
            counter.canonical_value(&too_large, invalid.clone()),
            Err(invalid)
        );
        assert_eq!(counter.encoded_len(), 0);
        assert_eq!(counter.finish().capacity(), 0);
    }

    #[test]
    fn counting_writer_enforces_same_bound_and_overflow_before_append() {
        for counting in [false, true] {
            let mut writer = if counting {
                Writer::counting(5)
            } else {
                Writer::new(5)
            };
            writer.u32(7).unwrap();
            assert_eq!(
                writer.raw(&[1, 2]),
                Err(IrValidationError::LimitExceeded {
                    kind: "canonical IR",
                    actual: 6,
                    maximum: 5,
                })
            );
            assert_eq!(writer.encoded_len(), 4);
        }
        let mut counter = Writer::counting(usize::MAX);
        counter.counted = Some(usize::MAX);
        assert_eq!(
            counter.u8(1),
            Err(IrValidationError::SizeOverflow {
                kind: "canonical IR"
            })
        );
        assert_eq!(counter.finish().capacity(), 0);
    }
}
