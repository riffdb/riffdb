//! Shared checked source resolution and follower-only immutable control admission.
use super::*;

pub(super) fn validate_columnar_configuration(
    projections: &[ConfiguredProjection],
) -> Result<(), ColumnarRegistrationError> {
    if projections.len() > 256
        || projections.iter().any(|projection| {
            let name = projection.name();
            name.is_empty()
                || name.len() > 256
                || name == "."
                || name == ".."
                || name.contains('/')
                || name.contains('\\')
        })
    {
        return Err(ColumnarRegistrationError::definition(
            first_projection_name(projections),
        ));
    }
    Ok(())
}

/// Resolves the entire bounded set before any control or artifact I/O.
pub(super) fn resolve_columnar_bindings(
    projections: &[ConfiguredProjection],
    bundle: Option<&ContractBundle>,
) -> Result<Vec<ColumnarControlBinding>, ColumnarRegistrationError> {
    validate_columnar_configuration(projections)?;
    let Some(bundle) = bundle else {
        if projections.is_empty() {
            return Ok(Vec::new());
        }
        return Err(ColumnarRegistrationError::no_active_catalog(
            first_projection_name(projections),
        ));
    };
    let count = projections
        .len()
        .checked_add(bundle.schema().vector_production_specs().len())
        .filter(|count| *count <= 256)
        .ok_or_else(|| ColumnarRegistrationError::definition("columnar-control"))?;
    let mut bindings = Vec::with_capacity(count);
    let mut names = BTreeSet::new();
    let mut sources = BTreeSet::new();
    for configured in projections {
        let name = configured.name().to_owned();
        let definition = resolve_configured_projection(configured, bundle)?;
        let spec = ColumnarProjectionSpecV1::for_scalar(&definition, bundle)
            .map_err(|_| ColumnarRegistrationError::definition(name.clone()))?;
        insert_control_binding(
            &mut bindings,
            &mut names,
            &mut sources,
            ColumnarControlBinding {
                name,
                definition,
                spec,
                is_vector: false,
            },
        )?;
    }
    for production in bundle.schema().vector_production_specs() {
        let entity = bundle
            .schema()
            .entity(production.entity())
            .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
        let vector_field = entity
            .record()
            .field(production.field())
            .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
        let name = format!("{}.{}", entity.name(), vector_field.name());
        let definition = resolve_vector_registration(bundle, entity, &name)?;
        let spec = ColumnarProjectionSpecV1::for_vector(&definition, production.field(), bundle)
            .map_err(|_| ColumnarRegistrationError::definition(name.clone()))?;
        insert_control_binding(
            &mut bindings,
            &mut names,
            &mut sources,
            ColumnarControlBinding {
                name,
                definition,
                spec,
                is_vector: true,
            },
        )?;
    }
    Ok(bindings)
}

fn insert_control_binding(
    bindings: &mut Vec<ColumnarControlBinding>,
    names: &mut BTreeSet<String>,
    sources: &mut BTreeSet<Vec<u8>>,
    binding: ColumnarControlBinding,
) -> Result<(), ColumnarRegistrationError> {
    if !names.insert(binding.name.clone())
        || !sources.insert(binding.spec.source().to_canonical_bytes())
    {
        return Err(ColumnarRegistrationError::duplicate_name(binding.name));
    }
    bindings.push(binding);
    Ok(())
}

/// Startup admission uses one completed follower pin for catalog, controls and
/// history. It grants no artifact selection or durable control capability.
pub(crate) fn validate_follower_columnar_admission(
    view: &crate::replication_bootstrap::FollowerReadView,
    projections: &[ConfiguredProjection],
) -> Result<(), ColumnarRegistrationError> {
    let bindings = resolve_columnar_bindings(
        projections,
        view.catalog().map(|active| active.bundle().bundle()),
    )?;
    let controls = view
        .snapshot()
        .read_columnar_projection_controls()
        .map_err(|error| ColumnarRegistrationError::control_storage(error, "columnar-control"))?;
    validate_follower_columnar_controls(
        &bindings,
        &controls,
        view.history().lineage().history_incarnation(),
        view.history().tail().frontier().application().map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        ),
    )
}

pub(super) fn validate_follower_columnar_controls(
    bindings: &[ColumnarControlBinding],
    controls: &[StoredColumnarProjectionControlV1],
    history_incarnation: u64,
    applied: FrontierPosition,
) -> Result<(), ColumnarRegistrationError> {
    if history_incarnation == 0 || bindings.len() > 256 || controls.len() > 256 {
        return Err(ColumnarRegistrationError::synchronization());
    }
    let mut by_source = BTreeMap::new();
    for control in controls {
        if by_source.insert(control.source(), control).is_some() {
            return Err(ColumnarRegistrationError::synchronization());
        }
    }
    for binding in bindings {
        let control = by_source
            .get(binding.spec.source())
            .ok_or_else(ColumnarRegistrationError::synchronization)?;
        if !common_control_matches(control, &binding.spec)
            || !common_control_history_incarnation_matches(control, history_incarnation)
            || [
                control.published(),
                control.candidate(),
                control.predecessor(),
            ]
            .into_iter()
            .flatten()
            .any(|pointer| {
                pointer.frontier() > applied
                    || pointer
                        .snapshot_frontier()
                        .is_some_and(|frontier| frontier > applied)
                    || (pointer.layout() == ColumnarProjectionLayoutV1::V2
                        && pointer.physical_generation_fingerprint()
                            != Some(
                                *PhysicalGenerationFingerprintV1::compute(
                                    pointer.definition_fingerprint(),
                                )
                                .as_bytes(),
                            ))
            })
        {
            return Err(ColumnarRegistrationError::synchronization());
        }
    }
    // A valid source artifact failure is not a failure of independently built
    // follower material. No source generation, frontier or checksum is selected.
    Ok(())
}
