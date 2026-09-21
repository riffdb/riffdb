//! Fence staging retains the existing source-control transaction and its lease.
use super::*;
use redb::WriteTransaction;

/// Production has one drained owner. Raw database transactions are restricted
/// to isolated physical fixtures and cannot enter the runtime constructor.
pub(super) enum FenceTransaction {
    Drained(Box<crate::store::ControlWrite>),
    #[cfg(test)]
    Isolated(Box<WriteTransaction>),
}

impl FenceTransaction {
    pub(super) fn transaction(&self) -> Result<&WriteTransaction, StorageError> {
        match self {
            Self::Drained(write) => write.transaction(),
            #[cfg(test)]
            Self::Isolated(write) => Ok(write),
        }
    }

    pub(super) fn commit(self) -> Result<(), StorageError> {
        match self {
            Self::Drained(write) => {
                write.validate()?;
                write.commit_primary_fence()?;
                Ok(())
            }
            #[cfg(test)]
            Self::Isolated(write) => write.commit().map_err(crate::error::commit_error),
        }
    }
}
