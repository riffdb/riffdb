//! Closed, cancellable read-only custody of one exact held source artifact.
use super::RedbHeldBootstrapSource;
use crate::store::ReplicationSourceControl;
use riffdb_storage_api::{
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapPageV3 as Page, StorageError,
    StorageErrorKind,
};
use std::sync::{Arc, Mutex};

/// This owns one bounded read, not another source proof or snapshot history.
/// Drop revokes only its own nondurable preparation. The actual read checks
/// current source evidence after bounded artifact I/O before releasing bytes.
#[doc(hidden)]
pub struct RedbBootstrapCustodyRead {
    source: Option<RedbHeldBootstrapSource>,
    control: Arc<Mutex<ReplicationSourceControl>>,
    owner: Arc<()>,
    failed: bool,
    #[cfg(test)]
    after_page_io: Option<Box<dyn FnOnce() + Send>>,
}

impl RedbBootstrapCustodyRead {
    pub(super) fn new(source: RedbHeldBootstrapSource) -> Result<Self, StorageError> {
        source.verify_paths()?;
        let control = source.custody_control()?;
        Ok(Self {
            source: Some(source),
            control,
            owner: Arc::new(()),
            failed: false,
            #[cfg(test)]
            after_page_io: None,
        })
    }

    /// At most one bounded native validation page. This gains no writer turn
    /// or mutation budget and cannot displace a prepared acknowledgement.
    pub fn advance(&mut self) -> Result<bool, StorageError> {
        if self.failed {
            return Err(unavailable());
        }
        let result = self.advance_step();
        self.fuse_on_failure(&result);
        result
    }

    fn advance_step(&self) -> Result<bool, StorageError> {
        let source = self.source.as_ref().ok_or_else(unavailable)?;
        source.verify_paths()?;
        let mut control = self.control.lock().map_err(|_| unavailable())?;
        let started = std::time::Instant::now();
        let result = control.prepare_bootstrap_custody(&self.owner, source.custody_hold());
        control.observe_bootstrap_custody_work(&self.owner, started, &result);
        result
    }

    /// Manifest release remains conditional on a fresh current proof.
    pub fn manifest(&mut self) -> Result<Option<Manifest>, StorageError> {
        if !self.advance()? {
            return Ok(None);
        }
        Ok(Some(
            self.source.as_ref().ok_or_else(unavailable)?.manifest(),
        ))
    }

    /// A change during page I/O can require more bounded validation; an
    /// incomplete or refused proof returns no page to the caller.
    pub fn read_page(&mut self, ordinal: u32) -> Result<Option<Page>, StorageError> {
        if self.failed {
            return Err(unavailable());
        }
        let result = (|| {
            let source = self.source.as_ref().ok_or_else(unavailable)?;
            source.verify_paths()?;
            let page = source.read_artifact_page(ordinal)?;
            #[cfg(test)]
            if let Some(after_io) = self.after_page_io.take() {
                after_io();
            }
            if !self.advance()? {
                return Ok(None);
            }
            Ok(Some(page))
        })();
        self.fuse_on_failure(&result);
        result
    }

    pub fn into_source(mut self) -> Result<RedbHeldBootstrapSource, StorageError> {
        if self.failed {
            return Err(unavailable());
        }
        self.source.take().ok_or_else(unavailable)
    }

    fn fuse_on_failure<T>(&mut self, result: &Result<T, StorageError>) {
        if result.is_err() {
            self.failed = true;
            if let Ok(mut control) = self.control.lock() {
                control.cancel_bootstrap_custody(&self.owner);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn after_page_io(&mut self, hook: impl FnOnce() + Send + 'static) {
        self.after_page_io = Some(Box::new(hook));
    }
}

impl Drop for RedbBootstrapCustodyRead {
    fn drop(&mut self) {
        let mut control = match self.control.lock() {
            Ok(control) => control,
            Err(poisoned) => poisoned.into_inner(),
        };
        control.cancel_bootstrap_custody(&self.owner);
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
