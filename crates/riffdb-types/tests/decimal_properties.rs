#![forbid(unsafe_code)]

//! Arithmetic properties for the fixed-scale decimal contract.

use proptest::prelude::*;
use riffdb_types::{Decimal, DecimalError, DecimalSpec};

fn decimal(spec: DecimalSpec, coefficient: i64) -> Decimal {
    Decimal::new(spec, i128::from(coefficient)).expect("generated coefficient fits")
}

proptest! {
    #[test]
    fn checked_addition_is_commutative_when_the_sum_fits(
        left in -4_000_000_000_000_000_000i64..=4_000_000_000_000_000_000,
        right in -4_000_000_000_000_000_000i64..=4_000_000_000_000_000_000,
    ) {
        let spec = DecimalSpec::new(19, 2).expect("valid specification");
        let left = decimal(spec, left);
        let right = decimal(spec, right);
        prop_assert_eq!(left.checked_add(right), right.checked_add(left));
    }

    #[test]
    fn zero_is_additive_identity_and_subtraction_is_reflexive(
        coefficient in -9_000_000_000_000_000_000i64..=9_000_000_000_000_000_000,
    ) {
        let spec = DecimalSpec::new(19, 2).expect("valid specification");
        let value = decimal(spec, coefficient);
        let zero = decimal(spec, 0);
        prop_assert_eq!(value.checked_add(zero), Ok(value));
        prop_assert_eq!(value.checked_sub(value), Ok(zero));
    }

    #[test]
    fn exact_rescaling_round_trips(
        coefficient in -9_000_000_000_000_000_000i64..=9_000_000_000_000_000_000,
    ) {
        let source_spec = DecimalSpec::new(19, 2).expect("valid source specification");
        let expanded_spec = DecimalSpec::new(20, 3).expect("valid expanded specification");
        let value = decimal(source_spec, coefficient);
        let expanded = value.rescale(expanded_spec).expect("scale expansion fits");
        prop_assert_eq!(expanded.rescale(source_spec), Ok(value));
    }
}

#[test]
fn precision_overflow_is_a_typed_failure() {
    let spec = DecimalSpec::new(2, 0).expect("valid specification");
    let maximum = Decimal::new(spec, 99).expect("maximum coefficient fits");
    let one = Decimal::new(spec, 1).expect("one fits");

    assert_eq!(
        maximum.checked_add(one),
        Err(DecimalError::CoefficientOutOfRange { spec })
    );
}

#[test]
fn precision_38_boundaries_are_exact() {
    let spec = DecimalSpec::new(38, 38).expect("valid specification");
    let limit = 10_i128.pow(38);

    assert!(Decimal::new(spec, limit - 1).is_ok());
    assert!(Decimal::new(spec, -limit + 1).is_ok());
    assert_eq!(
        Decimal::new(spec, limit),
        Err(DecimalError::CoefficientOutOfRange { spec })
    );
    assert_eq!(
        Decimal::new(spec, -limit),
        Err(DecimalError::CoefficientOutOfRange { spec })
    );
}

#[test]
fn full_width_arithmetic_and_rescale_overflow_are_typed() {
    let spec = DecimalSpec::new(38, 0).expect("valid specification");
    let value = Decimal::new(spec, 10_i128.pow(38) - 1).expect("coefficient fits");

    assert_eq!(
        value.checked_add(value),
        Err(DecimalError::ArithmeticOverflow)
    );

    let scaled = DecimalSpec::new(38, 38).expect("valid specification");
    assert_eq!(value.rescale(scaled), Err(DecimalError::ArithmeticOverflow));
}

#[test]
fn ordering_requires_the_exact_decimal_type() {
    let scale_two = DecimalSpec::new(4, 2).expect("valid specification");
    let scale_three = DecimalSpec::new(4, 3).expect("valid specification");
    let left = Decimal::new(scale_two, 100).expect("coefficient fits");
    let right = Decimal::new(scale_three, 100).expect("coefficient fits");

    assert!(matches!(
        left.checked_cmp(right),
        Err(DecimalError::SpecMismatch { .. })
    ));
    assert_eq!(left.partial_cmp(&right), None);
}
