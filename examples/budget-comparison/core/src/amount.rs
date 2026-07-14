//! Exact `decimal<28,2>` values used by the comparison workload.

use riffdb_types::{Decimal, DecimalError, DecimalSpec};
use std::error::Error;
use std::fmt;

/// Decimal precision from the canonical budget contract.
pub const BUDGET_DECIMAL_PRECISION: u8 = 28;
/// Decimal scale from the canonical budget contract.
pub const BUDGET_DECIMAL_SCALE: u8 = 2;

/// An exact `decimal<28,2>` amount represented in hundredths.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Amount(i128);

impl Amount {
    /// The exact zero amount.
    pub const ZERO: Self = Self(0);
    /// The smallest positive contract amount.
    pub const MINIMUM_POSITIVE: Self = Self(1);

    /// Creates an amount from its exact fixed-scale coefficient.
    pub fn from_minor_units(coefficient: i128) -> Result<Self, AmountError> {
        let spec = DecimalSpec::new(BUDGET_DECIMAL_PRECISION, BUDGET_DECIMAL_SCALE)
            .map_err(|_| AmountError::OutOfRange)?;
        Decimal::new(spec, coefficient).map_err(|_| AmountError::OutOfRange)?;
        Ok(Self(coefficient))
    }

    /// Parses the one canonical decimal spelling with exactly two fractional digits.
    pub fn parse(text: &str) -> Result<Self, AmountError> {
        if text.is_empty() || text.len() > 30 {
            return Err(AmountError::InvalidSyntax);
        }

        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) if !rest.is_empty() => (true, rest),
            Some(_) => return Err(AmountError::InvalidSyntax),
            None => (false, text),
        };
        let (whole, fractional) = unsigned.split_once('.').ok_or(AmountError::InvalidSyntax)?;
        if whole.is_empty()
            || fractional.len() != usize::from(BUDGET_DECIMAL_SCALE)
            || (whole.len() > 1 && whole.starts_with('0'))
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fractional.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(AmountError::InvalidSyntax);
        }

        let digit_count = whole
            .len()
            .checked_add(fractional.len())
            .ok_or(AmountError::OutOfRange)?;
        if digit_count > usize::from(BUDGET_DECIMAL_PRECISION) {
            return Err(AmountError::OutOfRange);
        }

        let mut coefficient = 0_i128;
        for byte in whole.bytes().chain(fractional.bytes()) {
            coefficient = coefficient
                .checked_mul(10)
                .and_then(|value| value.checked_add(i128::from(byte - b'0')))
                .ok_or(AmountError::OutOfRange)?;
        }
        if negative {
            coefficient = coefficient.checked_neg().ok_or(AmountError::OutOfRange)?;
        }
        let amount = Self::from_minor_units(coefficient)?;
        if amount.to_string() != text {
            return Err(AmountError::InvalidSyntax);
        }
        Ok(amount)
    }

    /// Returns the exact fixed-scale coefficient.
    pub const fn minor_units(self) -> i128 {
        self.0
    }

    /// Adds two exact amounts with checked contract precision.
    pub fn checked_add(self, other: Self) -> Result<Self, AmountError> {
        let coefficient = self.0.checked_add(other.0).ok_or(AmountError::OutOfRange)?;
        Self::from_minor_units(coefficient)
    }

    /// Subtracts two exact amounts with checked contract precision.
    pub fn checked_sub(self, other: Self) -> Result<Self, AmountError> {
        let coefficient = self.0.checked_sub(other.0).ok_or(AmountError::OutOfRange)?;
        Self::from_minor_units(coefficient)
    }

    /// Converts through the production canonical decimal type without changing scale.
    pub fn canonical_decimal(self) -> Result<Decimal, AmountError> {
        let spec = DecimalSpec::new(BUDGET_DECIMAL_PRECISION, BUDGET_DECIMAL_SCALE)
            .map_err(|_| AmountError::OutOfRange)?;
        Decimal::new(spec, self.0).map_err(|_| AmountError::OutOfRange)
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let negative = self.0 < 0;
        let absolute = if negative { -self.0 } else { self.0 };
        let whole = absolute / 100;
        let fractional = absolute % 100;
        if negative {
            write!(formatter, "-{whole}.{fractional:02}")
        } else {
            write!(formatter, "{whole}.{fractional:02}")
        }
    }
}

/// A safe amount parsing or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AmountError {
    /// The text is not the canonical fixed-scale spelling.
    InvalidSyntax,
    /// The coefficient exceeds `decimal<28,2>` or checked arithmetic overflowed.
    OutOfRange,
}

impl fmt::Display for AmountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSyntax => {
                formatter.write_str("amount is not canonical decimal<28,2> text")
            }
            Self::OutOfRange => formatter.write_str("amount exceeds decimal<28,2> bounds"),
        }
    }
}

impl Error for AmountError {}

impl From<DecimalError> for AmountError {
    fn from(_: DecimalError) -> Self {
        Self::OutOfRange
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amount_round_trips_exact_canonical_text() {
        for text in [
            "0.00",
            "0.01",
            "100.00",
            "-1.00",
            "99999999999999999999999999.99",
        ] {
            let amount = Amount::parse(text).expect("canonical amount");
            assert_eq!(amount.to_string(), text);
            assert_eq!(
                amount
                    .canonical_decimal()
                    .expect("canonical decimal")
                    .coefficient(),
                amount.minor_units()
            );
        }
    }

    #[test]
    fn amount_rejects_noncanonical_or_out_of_range_text() {
        for text in [
            "",
            "1",
            "1.0",
            "+1.00",
            "01.00",
            "-0.00",
            "1e2",
            "100000000000000000000000000.00",
        ] {
            assert!(Amount::parse(text).is_err(), "unexpectedly accepted {text}");
        }
    }
}
