//! Checked fixed-scale decimals and money values.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

/// The smallest supported decimal precision.
pub const MIN_DECIMAL_PRECISION: u8 = 1;

/// The largest supported decimal precision.
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// A checked fixed-scale decimal type declaration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DecimalSpec {
    precision: u8,
    scale: u8,
}

impl DecimalSpec {
    /// Validates and creates a decimal specification.
    pub const fn new(precision: u8, scale: u8) -> Result<Self, DecimalSpecError> {
        if precision < MIN_DECIMAL_PRECISION || precision > MAX_DECIMAL_PRECISION {
            return Err(DecimalSpecError::PrecisionOutOfRange { precision });
        }
        if scale > precision {
            return Err(DecimalSpecError::ScaleExceedsPrecision { precision, scale });
        }
        Ok(Self { precision, scale })
    }

    /// Returns the total decimal precision.
    #[must_use]
    pub const fn precision(self) -> u8 {
        self.precision
    }

    /// Returns the number of fractional decimal digits.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    const fn coefficient_limit(self) -> i128 {
        10_i128.pow(self.precision as u32)
    }
}

/// A safe validation failure for a decimal specification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalSpecError {
    /// Precision is not in `1..=38`.
    PrecisionOutOfRange {
        /// The invalid precision.
        precision: u8,
    },
    /// Scale is greater than precision.
    ScaleExceedsPrecision {
        /// The declared precision.
        precision: u8,
        /// The invalid scale.
        scale: u8,
    },
}

impl fmt::Display for DecimalSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrecisionOutOfRange { precision } => write!(
                formatter,
                "decimal precision {precision} is outside the supported range 1..=38"
            ),
            Self::ScaleExceedsPrecision { precision, scale } => write!(
                formatter,
                "decimal scale {scale} exceeds precision {precision}"
            ),
        }
    }
}

impl Error for DecimalSpecError {}

/// A checked fixed-scale decimal value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Decimal {
    spec: DecimalSpec,
    coefficient: i128,
}

impl Decimal {
    /// Validates and creates a decimal from its type and coefficient.
    pub const fn new(spec: DecimalSpec, coefficient: i128) -> Result<Self, DecimalError> {
        if coefficient <= -spec.coefficient_limit() || coefficient >= spec.coefficient_limit() {
            return Err(DecimalError::CoefficientOutOfRange { spec });
        }
        Ok(Self { spec, coefficient })
    }

    /// Returns the declared decimal type.
    #[must_use]
    pub const fn spec(self) -> DecimalSpec {
        self.spec
    }

    /// Returns the signed fixed-scale coefficient.
    #[must_use]
    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    /// Adds two values of exactly the same decimal type.
    pub fn checked_add(self, other: Self) -> Result<Self, DecimalError> {
        self.require_same_spec(other)?;
        let coefficient = self
            .coefficient
            .checked_add(other.coefficient)
            .ok_or(DecimalError::ArithmeticOverflow)?;
        Self::new(self.spec, coefficient)
    }

    /// Subtracts two values of exactly the same decimal type.
    pub fn checked_sub(self, other: Self) -> Result<Self, DecimalError> {
        self.require_same_spec(other)?;
        let coefficient = self
            .coefficient
            .checked_sub(other.coefficient)
            .ok_or(DecimalError::ArithmeticOverflow)?;
        Self::new(self.spec, coefficient)
    }

    /// Compares two values of exactly the same decimal type.
    pub fn checked_cmp(self, other: Self) -> Result<Ordering, DecimalError> {
        self.require_same_spec(other)?;
        Ok(self.coefficient.cmp(&other.coefficient))
    }

    /// Changes the declared scale without rounding or changing the numeric value.
    pub fn rescale(self, target: DecimalSpec) -> Result<Self, DecimalError> {
        if target.scale == self.spec.scale {
            return Self::new(target, self.coefficient);
        }

        if target.scale > self.spec.scale {
            let factor = decimal_power(target.scale - self.spec.scale);
            let coefficient = self
                .coefficient
                .checked_mul(factor)
                .ok_or(DecimalError::ArithmeticOverflow)?;
            return Self::new(target, coefficient);
        }

        let factor = decimal_power(self.spec.scale - target.scale);
        if self.coefficient % factor != 0 {
            return Err(DecimalError::LossyRescale {
                from_scale: self.spec.scale,
                to_scale: target.scale,
            });
        }
        Self::new(target, self.coefficient / factor)
    }

    fn require_same_spec(self, other: Self) -> Result<(), DecimalError> {
        if self.spec != other.spec {
            return Err(DecimalError::SpecMismatch {
                left: self.spec,
                right: other.spec,
            });
        }
        Ok(())
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.checked_cmp(*other).ok()
    }
}

fn decimal_power(exponent: u8) -> i128 {
    10_i128.pow(exponent as u32)
}

/// A safe decimal construction or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalError {
    /// The coefficient does not fit the declared precision.
    CoefficientOutOfRange {
        /// The decimal type whose precision was exceeded.
        spec: DecimalSpec,
    },
    /// An `i128` arithmetic operation overflowed.
    ArithmeticOverflow,
    /// An operation combined distinct decimal types.
    SpecMismatch {
        /// The left operand type.
        left: DecimalSpec,
        /// The right operand type.
        right: DecimalSpec,
    },
    /// Reducing scale would discard nonzero decimal digits.
    LossyRescale {
        /// The source scale.
        from_scale: u8,
        /// The requested scale.
        to_scale: u8,
    },
}

impl fmt::Display for DecimalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoefficientOutOfRange { spec } => write!(
                formatter,
                "decimal coefficient exceeds precision {}",
                spec.precision()
            ),
            Self::ArithmeticOverflow => formatter.write_str("decimal arithmetic overflow"),
            Self::SpecMismatch { left, right } => write!(
                formatter,
                "decimal type mismatch: <{},{}> and <{},{}>",
                left.precision(),
                left.scale(),
                right.precision(),
                right.scale()
            ),
            Self::LossyRescale {
                from_scale,
                to_scale,
            } => write!(
                formatter,
                "rescaling from scale {from_scale} to {to_scale} would lose precision"
            ),
        }
    }
}

impl Error for DecimalError {}

/// A safe validation failure for a three-letter currency code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrencyCodeError {
    /// The code is not exactly three bytes.
    InvalidLength,
    /// A byte is not an uppercase ASCII letter.
    InvalidCharacter,
}

impl fmt::Display for CurrencyCodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength => {
                formatter.write_str("currency code must contain exactly three ASCII letters")
            }
            Self::InvalidCharacter => {
                formatter.write_str("currency code must contain only uppercase ASCII letters")
            }
        }
    }
}

impl Error for CurrencyCodeError {}

/// An exact three-letter uppercase ASCII currency identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CurrencyCode([u8; 3]);

impl CurrencyCode {
    /// Validates and creates a currency code.
    pub fn new(value: impl AsRef<[u8]>) -> Result<Self, CurrencyCodeError> {
        let bytes: [u8; 3] = value
            .as_ref()
            .try_into()
            .map_err(|_| CurrencyCodeError::InvalidLength)?;
        if !bytes.iter().all(u8::is_ascii_uppercase) {
            return Err(CurrencyCodeError::InvalidCharacter);
        }
        Ok(Self(bytes))
    }

    /// Returns the three canonical ASCII bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 3] {
        &self.0
    }
}

impl fmt::Display for CurrencyCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            formatter.write_fmt(format_args!("{}", char::from(byte)))?;
        }
        Ok(())
    }
}

/// A money value with an explicit currency and checked decimal amount.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Money {
    currency: CurrencyCode,
    amount: Decimal,
}

impl Money {
    /// Creates a money value from an already validated currency and decimal amount.
    #[must_use]
    pub const fn new(currency: CurrencyCode, amount: Decimal) -> Self {
        Self { currency, amount }
    }

    /// Returns the currency identifier.
    #[must_use]
    pub const fn currency(self) -> CurrencyCode {
        self.currency
    }

    /// Returns the fixed-scale amount.
    #[must_use]
    pub const fn amount(self) -> Decimal {
        self.amount
    }

    /// Adds two money values with identical currency and decimal type.
    pub fn checked_add(self, other: Self) -> Result<Self, MoneyError> {
        self.require_same_currency(other)?;
        Ok(Self::new(
            self.currency,
            self.amount.checked_add(other.amount)?,
        ))
    }

    /// Subtracts two money values with identical currency and decimal type.
    pub fn checked_sub(self, other: Self) -> Result<Self, MoneyError> {
        self.require_same_currency(other)?;
        Ok(Self::new(
            self.currency,
            self.amount.checked_sub(other.amount)?,
        ))
    }

    /// Compares two values with identical currency and decimal type.
    pub fn checked_cmp(self, other: Self) -> Result<Ordering, MoneyError> {
        self.require_same_currency(other)?;
        Ok(self.amount.checked_cmp(other.amount)?)
    }

    fn require_same_currency(self, other: Self) -> Result<(), MoneyError> {
        if self.currency != other.currency {
            return Err(MoneyError::CurrencyMismatch);
        }
        Ok(())
    }
}

impl PartialOrd for Money {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.checked_cmp(*other).ok()
    }
}

/// A safe checked-money arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoneyError {
    /// The operands use distinct currency identifiers.
    CurrencyMismatch,
    /// The decimal operation failed.
    Decimal(DecimalError),
}

impl fmt::Display for MoneyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CurrencyMismatch => {
                formatter.write_str("money operands use different currencies")
            }
            Self::Decimal(error) => error.fmt(formatter),
        }
    }
}

impl Error for MoneyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CurrencyMismatch => None,
            Self::Decimal(error) => Some(error),
        }
    }
}

impl From<DecimalError> for MoneyError {
    fn from(error: DecimalError) -> Self {
        Self::Decimal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_spec_enforces_precision_and_scale_bounds() {
        assert_eq!(
            DecimalSpec::new(0, 0),
            Err(DecimalSpecError::PrecisionOutOfRange { precision: 0 })
        );
        assert_eq!(
            DecimalSpec::new(39, 0),
            Err(DecimalSpecError::PrecisionOutOfRange { precision: 39 })
        );
        assert_eq!(
            DecimalSpec::new(2, 3),
            Err(DecimalSpecError::ScaleExceedsPrecision {
                precision: 2,
                scale: 3
            })
        );
    }

    #[test]
    fn decimal_coefficient_must_fit_declared_precision() {
        let spec = DecimalSpec::new(3, 2).expect("valid spec");
        assert!(Decimal::new(spec, 999).is_ok());
        assert!(Decimal::new(spec, -999).is_ok());
        assert_eq!(
            Decimal::new(spec, 1_000),
            Err(DecimalError::CoefficientOutOfRange { spec })
        );
        assert_eq!(
            Decimal::new(spec, -1_000),
            Err(DecimalError::CoefficientOutOfRange { spec })
        );
    }

    #[test]
    fn decimal_arithmetic_checks_type_and_precision() {
        let spec = DecimalSpec::new(3, 2).expect("valid spec");
        let left = Decimal::new(spec, 600).expect("valid decimal");
        let right = Decimal::new(spec, 399).expect("valid decimal");
        assert_eq!(left.checked_add(right).expect("sum").coefficient(), 999);
        assert!(matches!(
            left.checked_add(Decimal::new(spec, 400).expect("valid decimal")),
            Err(DecimalError::CoefficientOutOfRange { .. })
        ));

        let other_spec = DecimalSpec::new(4, 2).expect("valid spec");
        assert!(matches!(
            left.checked_sub(Decimal::new(other_spec, 1).expect("valid decimal")),
            Err(DecimalError::SpecMismatch { .. })
        ));
        assert_eq!(left.checked_cmp(right), Ok(Ordering::Greater));
        assert_eq!(
            left.partial_cmp(&Decimal::new(other_spec, 1).expect("valid decimal")),
            None
        );
    }

    #[test]
    fn rescale_is_exact_or_fails() {
        let source = Decimal::new(DecimalSpec::new(5, 2).expect("valid spec"), 1_200)
            .expect("valid decimal");
        let lower = DecimalSpec::new(4, 1).expect("valid spec");
        assert_eq!(
            source.rescale(lower).expect("exact rescale").coefficient(),
            120
        );

        let lossy = Decimal::new(DecimalSpec::new(5, 2).expect("valid spec"), 1_201)
            .expect("valid decimal");
        assert!(matches!(
            lossy.rescale(lower),
            Err(DecimalError::LossyRescale { .. })
        ));

        let higher = DecimalSpec::new(6, 3).expect("valid spec");
        assert_eq!(
            source.rescale(higher).expect("exact rescale").coefficient(),
            12_000
        );
    }

    #[test]
    fn money_requires_exact_currency_codes_and_matching_operands() {
        assert!(CurrencyCode::new("USD").is_ok());
        assert_eq!(
            CurrencyCode::new("usd"),
            Err(CurrencyCodeError::InvalidCharacter)
        );
        assert_eq!(
            CurrencyCode::new("EURO"),
            Err(CurrencyCodeError::InvalidLength)
        );

        let spec = DecimalSpec::new(6, 2).expect("valid spec");
        let amount = Decimal::new(spec, 100).expect("valid decimal");
        let usd = Money::new(CurrencyCode::new("USD").expect("valid code"), amount);
        let eur = Money::new(CurrencyCode::new("EUR").expect("valid code"), amount);
        assert_eq!(usd.checked_add(eur), Err(MoneyError::CurrencyMismatch));
        assert_eq!(usd.partial_cmp(&eur), None);
        assert_eq!(usd.checked_cmp(usd), Ok(Ordering::Equal));
    }
}
