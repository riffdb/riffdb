//! Exact predecessor artifact matching for the closed offline catalog cut.
//!
//! This validation grants no writer, origin or plan-input authority. Its sole
//! caller must retain actual native file/directory custody across this check and
//! every subsequent pre-mutation proof. In particular, these path arguments are
//! not a substitute for the offline owner's pinned artifact set.
use super::*;

/// Checks the complete source artifact set which a native backup authenticates.
/// The ordinary upgrade helper predates sealed extent custody and deliberately
/// checks only the database and active journal; the catalog cut needs both the
/// marker and every original sealed generation as well.
pub(crate) fn validate_catalog_cut_source_artifacts(
    source_database: &Path,
    backup_directory: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
    validate_backup_inventory(backup_directory)?;
    validate_manifest_inventory(backup_directory, manifest)?;
    let (validated, _) = validate_immutable_backup(backup_directory)?;
    if validated != *manifest {
        return Err(corrupt());
    }
    validate_exact_source_matches_backup(source_database, manifest)?;

    let marker = crate::durable_format_marker_path(source_database);
    let expected_marker = manifest_format_marker_checksum(manifest)?.ok_or_else(corrupt)?;
    if sha256_file(&marker)? != expected_marker {
        return Err(corrupt());
    }
    validate_manifest_format_marker(&marker, manifest)?;

    // The native backup recipe refuses an unfinished seal rather than inventing
    // a correspondence for an intermediate filename. Keep that exact rule.
    if crate::journal::sealing_journal_path(source_database)
        .try_exists()
        .map_err(io_unavailable)?
    {
        return Err(unknown());
    }
    let source_sealed = sealed_extents_beside(source_database)?;
    let backup_sealed = sealed_artifacts_in(backup_directory)?;
    if source_sealed.len() != backup_sealed.len() {
        return Err(corrupt());
    }
    for (index, ((source_generation, source_path), (backup_generation, backup_path))) in
        source_sealed.iter().zip(&backup_sealed).enumerate()
    {
        if source_generation != backup_generation {
            return Err(corrupt());
        }
        let ordinal = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(FIRST_SEALED_ARTIFACT_ORDINAL))
            .and_then(NonZeroU32::new)
            .ok_or_else(corrupt)?;
        let expected = manifest_artifact_checksum(manifest, ordinal)?;
        if sha256_file(source_path)? != expected || sha256_file(backup_path)? != expected {
            return Err(corrupt());
        }
        crate::journal::read_sealed_extent_with_media(
            &crate::media::RealJournalMedia,
            source_path,
            manifest.database_id(),
        )
        .map_err(|_| corrupt())?;
        crate::journal::read_sealed_extent_with_media(
            &crate::media::RealJournalMedia,
            backup_path,
            manifest.database_id(),
        )
        .map_err(|_| corrupt())?;
    }
    Ok(())
}
