//! One exact-fence source hold handoff at the existing drained barrier.
//! Authorized composition owns the receiver's durable-attachment evidence.
use super::*;

impl ReplicationSourceControl {
    /// Replaces only this bootstrap hold with a follower hold at the identical
    /// position. This never advances the retention floor or removes its fence.
    /// The caller must have checked an authenticated, durable acknowledgement
    /// at the published bootstrap fence; this internal method does not infer it.
    pub(crate) fn attach_bootstrap(&mut self, bootstrap: Hold) -> Result<bool, Refusal> {
        if bootstrap.kind() != Kind::Bootstrap {
            return Err(Refusal::InvalidPosition);
        }
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        drop(crate::changelog_v3_cursor::open(
            &RedbReadAccess::Durable(Arc::clone(&barrier.root)),
            bootstrap.lineage(),
            bootstrap.fence(),
        )?);
        let follower = Hold::new(
            bootstrap.id(),
            Kind::FollowerAcknowledgement,
            bootstrap.lineage(),
            bootstrap.fence(),
        );
        let table = barrier.root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let read_hold = |expected: Hold| -> Result<Option<Hold>, StorageError> {
            table
                .get(expected.storage_key().as_slice())
                .map_err(precommit_storage_error)?
                .map(|row| {
                    decode_replication_source_hold_v1(row.value())
                        .map(|decoded| *decoded.value())
                        .map_err(crate::error::codec_error)
                })
                .transpose()
        };
        let old_bootstrap = read_hold(bootstrap)?;
        let old_follower = read_hold(follower)?;
        match (old_bootstrap, old_follower) {
            (None, Some(exact)) if exact == follower => return Ok(false),
            (Some(exact), None) if exact == bootstrap => {}
            (Some(exact), Some(attached)) if exact == bootstrap && attached == follower => {}
            _ => return Err(Refusal::InvalidPosition),
        }
        drop(table);
        let encoded =
            encode_replication_source_hold_v1(follower).map_err(crate::error::codec_error)?;
        let write = barrier.begin()?;
        let (receipt, _) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::ReplicationSourceHold,
        )?;
        {
            let mut table = write
                .transaction()?
                .open_table(SOURCE_HOLDS)
                .map_err(table_error)?;
            // Insert before remove inside this one transaction. Every durable
            // outcome contains the old fence or the identical follower fence.
            table
                .insert(follower.storage_key().as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            crash_edge("attachment-follower-staged");
            table
                .remove(bootstrap.storage_key().as_slice())
                .map_err(precommit_storage_error)?;
        }
        crash_edge("attachment-bootstrap-removed");
        receipt.stage(write.transaction()?)?;
        write.validate()?;
        crash_edge("attachment-receipted");
        write.commit()?;
        crash_edge("attachment-committed");
        Ok(true)
    }
}
