//! Source construction never exposes unheld bytes. Each step owns at most one
//! bounded copy or verification; dropping the owner releases its source pin.
use super::*;
use riffdb_storage_api::{
    AuthoritativeStateCursorV3, ReplicationBootstrapFenceV3 as Fence,
    ReplicationBootstrapPageCursorV3 as Cursor, ReplicationSourceHoldIdV1 as HoldId,
};

pub(in crate::maintenance) struct SourceBuild {
    state: Option<State>,
}

enum State {
    Copy(Box<CopySource>),
    Verify(Box<RedbBootstrapVerification>),
    Complete(Box<RedbVerifiedBootstrapTransfer>),
}

struct CopySource {
    database: Database,
    file: File,
    directory: PinnedDirectory,
    source: Cursor,
    transcript: Transcript,
}

impl SourceBuild {
    pub(in crate::maintenance) fn begin(
        path: &Path,
        fence: Fence,
        input: Box<dyn AuthoritativeStateCursorV3>,
    ) -> Result<Self, StorageError> {
        let source = Cursor::new(fence, input).map_err(invalid)?;
        let (database, file, directory) = create_database(path)?;
        Ok(Self {
            state: Some(State::Copy(Box::new(CopySource {
                database,
                file,
                directory,
                source,
                transcript: Transcript::new(fence),
            }))),
        })
    }

    pub(in crate::maintenance) fn resume(path: &Path, id: HoldId) -> Result<Self, StorageError> {
        let stage = RedbBootstrapStage::open_checked(path, None)?;
        if stage.manifest.fence().hold_id() != id {
            return Err(corrupt());
        }
        Ok(Self {
            state: Some(State::Verify(Box::new(stage.begin_verification()?))),
        })
    }

    pub(in crate::maintenance) fn advance(&mut self) -> Result<bool, StorageError> {
        // Take before doing work: a failed step drops all non-durable owners
        // and leaves a fused handle, including when a commit is uncertain.
        let state = self.state.take().ok_or_else(corrupt)?;
        let state = match state {
            State::Copy(mut copy) => {
                copy.verify_identity()?;
                match copy.source.next_page().map_err(invalid)? {
                    Some(page) => {
                        copy.transcript.observe(&page).map_err(invalid)?;
                        let bytes = page.encode().map_err(invalid)?;
                        let mut write = copy.database.begin_write().map_err(unavailable)?;
                        write.set_two_phase_commit(true);
                        write
                            .set_durability(Durability::Immediate)
                            .map_err(unavailable)?;
                        if write
                            .open_table(PAGES)
                            .map_err(unavailable)?
                            .insert(page.ordinal(), bytes.as_slice())
                            .map_err(unavailable)?
                            .is_some()
                        {
                            return Err(corrupt());
                        }
                        copy.verify_identity()?;
                        write.commit().map_err(unavailable)?;
                        crash_edge("source-page-committed");
                        copy.verify_identity()?;
                        State::Copy(copy)
                    }
                    None => State::Verify(Box::new(copy.seal()?.begin_verification()?)),
                }
            }
            State::Verify(mut verification) => {
                if verification.advance()? {
                    State::Complete(Box::new(verification.finish()?))
                } else {
                    State::Verify(verification)
                }
            }
            State::Complete(transfer) => {
                transfer.stage.verify_identity()?;
                State::Complete(transfer)
            }
        };
        let complete = matches!(state, State::Complete(_));
        self.state = Some(state);
        Ok(complete)
    }

    pub(in crate::maintenance) fn finish(
        self,
    ) -> Result<RedbVerifiedBootstrapTransfer, StorageError> {
        let Some(State::Complete(transfer)) = self.state else {
            return Err(corrupt());
        };
        transfer.stage.verify_identity()?;
        Ok(*transfer)
    }
}

impl CopySource {
    fn verify_identity(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if !self
            .directory
            .regular_file_matches(OsStr::new(FILE), &self.file)?
        {
            return Err(corrupt());
        }
        Ok(())
    }

    fn seal(self) -> Result<RedbBootstrapStage, StorageError> {
        let manifest = self.source.manifest().map_err(invalid)?;
        let progress = self.transcript.checkpoint(manifest).map_err(invalid)?;
        let bytes = progress.encode().map_err(invalid)?;
        let mut write = self.database.begin_write().map_err(unavailable)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(unavailable)?;
        write
            .open_table(RECEIPT)
            .map_err(unavailable)?
            .insert(0, bytes.as_slice())
            .map_err(unavailable)?;
        self.verify_identity()?;
        write.commit().map_err(unavailable)?;
        self.directory.sync()?;
        crash_edge("source-manifest-committed");
        self.verify_identity()?;
        // The source snapshot is no longer needed once exact EOF and the
        // manifest are durable. Verification reads only the private artifact.
        drop(self.source);
        Ok(RedbBootstrapStage {
            database: self.database,
            file: self.file,
            directory: self.directory,
            manifest,
            progress,
            transcript: self.transcript,
            failed: false,
            receiver_repository: None,
        })
    }
}
