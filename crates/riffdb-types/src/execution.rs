//! Closed deterministic execution-failure vocabulary.

use std::error::Error;
use std::fmt;

/// A dependency-validated deterministic command execution failure.
///
/// Zero is reserved for wire-level unspecified values and is never represented
/// by this type.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u32)]
pub enum ExecutionFailureCode {
    /// Checked arithmetic could not produce a valid contract value.
    ArithmeticFault = 1,
    /// A fixed deterministic execution budget was exhausted.
    ResourceLimit = 2,
}

impl ExecutionFailureCode {
    /// Every valid v1 execution-failure code.
    pub const ALL: [Self; 2] = [Self::ArithmeticFault, Self::ResourceLimit];

    /// Returns the stable v1 numeric code.
    #[must_use]
    pub const fn code(self) -> u32 {
        self as u32
    }
}

impl From<ExecutionFailureCode> for u32 {
    fn from(value: ExecutionFailureCode) -> Self {
        value.code()
    }
}

impl TryFrom<u32> for ExecutionFailureCode {
    type Error = ExecutionFailureCodeError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::ArithmeticFault),
            2 => Ok(Self::ResourceLimit),
            _ => Err(ExecutionFailureCodeError { value }),
        }
    }
}

/// A safe failure to decode a closed execution-failure code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionFailureCodeError {
    value: u32,
}

impl ExecutionFailureCodeError {
    /// Returns the rejected numeric value.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.value
    }
}

impl fmt::Display for ExecutionFailureCodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("execution failure code is unspecified or unknown")
    }
}

impl Error for ExecutionFailureCodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_codes_round_trip_and_zero_is_not_representable() {
        for code in ExecutionFailureCode::ALL {
            assert_eq!(ExecutionFailureCode::try_from(code.code()), Ok(code));
        }

        assert_eq!(
            ExecutionFailureCode::try_from(0)
                .expect_err("zero is the wire unspecified value")
                .value(),
            0
        );
        assert_eq!(
            ExecutionFailureCode::try_from(3)
                .expect_err("unknown values fail closed")
                .value(),
            3
        );
        assert_eq!(
            ExecutionFailureCode::try_from(u32::MAX)
                .expect_err("unknown values fail closed")
                .value(),
            u32::MAX
        );
    }
}
