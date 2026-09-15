//! Bounded framing over exactly one published authoritative cursor.
use super::*;
use crate::{
    AuthoritativeStateCursorV3, AuthoritativeStateStepV3, ChangelogCursorErrorV3, StorageError,
    StorageErrorKind,
};

/// A source holds one immutable cursor, one bounded page and at most one pending
/// row. The caller must establish the durable bootstrap hold before releasing
/// bytes. No live engine read or source-side writer is available here.
pub struct ReplicationBootstrapPageCursorV3 {
    input: Box<dyn AuthoritativeStateCursorV3>,
    pending: Option<AuthoritativeStateStepV3>,
    transcript: ReplicationBootstrapTranscriptV3,
    failed: bool,
    finished: bool,
}
impl ReplicationBootstrapPageCursorV3 {
    /// Refuses a cursor whose immutable snapshot differs from the held fence.
    pub fn new(
        fence: ReplicationBootstrapFenceV3,
        input: Box<dyn AuthoritativeStateCursorV3>,
    ) -> Result<Self, Error> {
        if input.history() != fence.history() {
            return Err(Error::PredecessorMismatch);
        }
        Ok(Self {
            input,
            pending: None,
            transcript: ReplicationBootstrapTranscriptV3::new(fence),
            failed: false,
            finished: false,
        })
    }
    /// Returns one complete bounded page, an exact terminal end, or a fused
    /// refusal. Namespace ends are explicit even for an empty namespace.
    pub fn next_page(
        &mut self,
    ) -> Result<Option<ReplicationBootstrapPageV3>, ChangelogCursorErrorV3> {
        if self.failed {
            return Err(reject(Error::InvalidEncoding));
        }
        if self.finished {
            return Ok(None);
        }
        self.failed = true;
        let result = self.next_inner();
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<ReplicationBootstrapPageV3>, ChangelogCursorErrorV3> {
        if self.input.history() != self.transcript.fence.history() {
            return Err(reject(Error::PredecessorMismatch));
        }
        let Some(namespace) = namespace(self.transcript.namespace_index) else {
            if self.pending.is_some() || self.input.next_item()?.is_some() {
                return Err(reject(Error::InvalidEncoding));
            }
            self.finished = true;
            return Ok(None);
        };
        let mut rows = Vec::new();
        let mut size = PAGE_FIXED_BYTES;
        let end = loop {
            let step = match self.pending.take() {
                Some(step) => Some(step),
                None => self.input.next_item()?,
            };
            match step.ok_or_else(|| reject(Error::InvalidEncoding))? {
                AuthoritativeStateStepV3::Row(row) => {
                    if row.namespace() != namespace {
                        return Err(reject(Error::InvalidNamespace));
                    }
                    let next_size = size
                        .checked_add(8)
                        .and_then(|n| n.checked_add(row.key().len()))
                        .and_then(|n| n.checked_add(row.value().len()))
                        .ok_or_else(|| reject(Error::LimitExceeded))?;
                    if rows.len() == MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS
                        || next_size > MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES
                    {
                        if rows.is_empty() {
                            return Err(reject(Error::LimitExceeded));
                        }
                        self.pending = Some(AuthoritativeStateStepV3::Row(row));
                        break false;
                    }
                    size = next_size;
                    rows.push(row);
                }
                AuthoritativeStateStepV3::EndNamespace(observed) => {
                    if observed != namespace {
                        return Err(reject(Error::InvalidNamespace));
                    }
                    break true;
                }
            }
        };
        if self.input.history() != self.transcript.fence.history() {
            return Err(reject(Error::PredecessorMismatch));
        }
        let page = ReplicationBootstrapPageV3::new(
            self.transcript.fence.digest(),
            self.transcript
                .pages
                .checked_add(1)
                .ok_or_else(|| reject(Error::LimitExceeded))?,
            namespace,
            end,
            self.transcript.hash,
            rows,
        )
        .map_err(reject)?;
        self.transcript.observe(&page).map_err(reject)?;
        Ok(Some(page))
    }
    /// Available only after the cursor itself proves its exact end. A valid
    /// namespace prefix or an error can never become a manifest.
    pub fn manifest(&self) -> Result<ReplicationBootstrapManifestV1, Error> {
        if self.failed || !self.finished {
            return Err(Error::InvalidEncoding);
        }
        self.transcript.manifest()
    }
}
fn reject(error: Error) -> ChangelogCursorErrorV3 {
    StorageError::new(
        if error == Error::LimitExceeded {
            StorageErrorKind::LimitExceeded
        } else {
            StorageErrorKind::CorruptData
        },
        None,
    )
    .into()
}
impl std::fmt::Debug for ReplicationBootstrapPageCursorV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationBootstrapPageCursorV3([redacted])")
    }
}
