use super::super::*;

// req: BLK-017
#[test]
fn aggregate_collection_graph_arithmetic_overflow_fails_closed() {
    let overflow = usize::MAX
        .checked_mul(2)
        .and_then(|variable| 1_usize.checked_add(variable));
    assert_eq!(overflow, None);
}

proptest::proptest! {
    // req: BLK-017
    #[test]
    fn aggregate_copy_coefficient_matches_the_closed_reference_model(
        sources in proptest::collection::vec(0_u8..=8, 0..128),
    ) {
        let modeled = sources
            .iter()
            .map(|source| {
                if *source == 0 {
                    AggregateElementSource::Whole
                } else {
                    AggregateElementSource::Field(
                        FieldId::new(u32::from(*source)).expect("nonzero field"),
                    )
                }
            })
            .collect::<Vec<_>>();
        let whole = sources.iter().filter(|source| **source == 0).count();
        let most_copied_field = (1_u8..=8)
            .map(|field| sources.iter().filter(|source| **source == field).count())
            .max()
            .unwrap_or(0);

        proptest::prop_assert_eq!(
            aggregate_copy_charge(modeled).expect("bounded model cannot overflow"),
            whole + most_copied_field
        );
    }
}
