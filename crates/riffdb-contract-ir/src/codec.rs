use crate::IrValidationError;

pub(crate) struct Writer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl Writer {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
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
        let actual =
            self.bytes
                .len()
                .checked_add(value.len())
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
        self.bytes.extend_from_slice(value);
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
