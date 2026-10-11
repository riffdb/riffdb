//! Exact native identities retained from materialization through publication.
//! Byte equality cannot substitute for continuity of the validated artifacts.

use super::*;

pub(super) struct StagedArtifactIdentities {
    primary: File,
    journal: Option<File>,
    marker: File,
}

impl StagedArtifactIdentities {
    pub(super) fn capture(
        directory: &PinnedDirectory,
        primary: &Path,
        journal: &Path,
        marker: &Path,
    ) -> Result<Self, StorageError> {
        let retained = Self {
            primary: directory.open_file(name(primary)?)?.into_std(),
            journal: Some(directory.open_file(name(journal)?)?.into_std()),
            marker: directory.open_file(name(marker)?)?.into_std(),
        };
        retained.verify(directory, primary, journal, marker)?;
        Ok(retained)
    }

    pub(super) fn verify(
        &self,
        directory: &PinnedDirectory,
        primary: &Path,
        journal: &Path,
        marker: &Path,
    ) -> Result<(), StorageError> {
        directory.verify()?;
        if !directory.regular_file_matches(name(primary)?, &self.primary)?
            || !directory.regular_file_matches(name(marker)?, &self.marker)?
        {
            return Err(corrupt());
        }
        match &self.journal {
            Some(file) if directory.regular_file_matches(name(journal)?, file)? => {}
            None if directory.regular_file_length(name(journal)?)?.is_none() => {}
            _ => return Err(corrupt()),
        }
        directory.verify()
    }

    /// Only archive conversion removes the already verified empty source journal.
    pub(super) fn remove_archive_source_journal(
        &mut self,
        directory: &PinnedDirectory,
        journal: &Path,
    ) -> Result<(), StorageError> {
        let file = self.journal.as_ref().ok_or_else(corrupt)?;
        if !directory.regular_file_matches(name(journal)?, file)? {
            return Err(corrupt());
        }
        directory.remove_file_if_present(name(journal)?)?;
        if directory.regular_file_length(name(journal)?)?.is_some() {
            return Err(corrupt());
        }
        self.journal = None;
        Ok(())
    }

    /// The durable archive receipt alone authorizes creating the RestoreAnchor's
    /// new journal. This never rebinds a replaced existing artifact.
    pub(super) fn create_archive_anchor_journal(
        &mut self,
        directory: &PinnedDirectory,
        journal: &Path,
        header: &crate::journal::JournalFileHeader,
    ) -> Result<(), StorageError> {
        if self.journal.is_some() || directory.regular_file_length(name(journal)?)?.is_some() {
            return Err(corrupt());
        }
        crate::journal::reset_journal(journal, header)
            .map_err(crate::backup::backup_journal_error)?;
        let file = directory.open_file(name(journal)?)?.into_std();
        if !directory.regular_file_matches(name(journal)?, &file)? {
            return Err(corrupt());
        }
        self.journal = Some(file);
        Ok(())
    }
}

fn name(path: &Path) -> Result<&std::ffi::OsStr, StorageError> {
    path.file_name().ok_or_else(invariant)
}
