//! Genuine compiled projection command fixture; no manufactured event payload.
use super::*;

pub(super) fn bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        let projection = r#"
  projection Rows {
    source event RowCreated
    key (id)
    measure item_count = count()
    frontier transactionally_ordered
  }
"#;
        let (prefix, suffix) = STORAGE_RECOVERY_CONTRACT.rsplit_once('}').unwrap();
        assert!(suffix.trim().is_empty());
        let source = format!("{prefix}{projection}}}");
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(&source).expect("compile genuine Source projection fixture"),
        )
        .expect("validate genuine Source projection fixture")
    })
}

pub(super) fn plan_for_bundle(bundle: &ValidatedContractBundle) -> ExecutablePlanRef {
    let commands = bundle.bundle().commands();
    assert_eq!(commands.len(), 1);
    let command = &commands[0];
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    )
}

pub(super) fn input(bundle: &ValidatedContractBundle, ordinal: u64, value: u64) -> CanonicalRecord {
    let command = &bundle.bundle().commands()[0];
    let fields = command.input().record().fields();
    assert_eq!(fields.len(), 3);
    let values = fields
        .iter()
        .map(|field| {
            let value = match field.name() {
                "idempotency_key" => {
                    CanonicalValue::string(format!("source-fixture-{ordinal}")).unwrap()
                }
                "id" => CanonicalValue::U64(6 + ordinal),
                "value" => CanonicalValue::U64(value),
                _ => panic!("closed compiled fixture input"),
            };
            (field.id(), value)
        })
        .collect();
    CanonicalRecord::new(values).expect("exact compiled fixture input")
}

pub(super) fn activate(
    ports: &mut RedbOperationalPorts,
) -> Result<ActiveCatalogPointerV1, StorageError> {
    let stored = bundle()
        .to_stored()
        .map_err(|_| crate::error::storage_error(StorageErrorKind::InvariantViolation))?;
    match ports.activate_catalog(&CatalogActivationIntentV1::new(
        None,
        stored,
        RequestId::from_bytes(uuid_bytes(0x62)).unwrap(),
        catalog_principal(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        None,
    ))? {
        CatalogActivationResult::Activated { active, .. } => Ok(active),
        _ => Err(crate::error::storage_error(
            StorageErrorKind::InvariantViolation,
        )),
    }
}
