//! Pure request selection before source admission or intrinsic audit start.
use super::{ReplicationFailure, ReplicationPhase, ReplicationRequest, ReplicationStreamErrorV3};
use riffdb_auth::ReplicationBootstrapManifestV1;
use riffdb_types::{
    DualFrontier, LeadershipEpochV1, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
    ServiceAuditTargetV1, ServiceAuditTargetsV1,
};

fn invalid() -> ReplicationFailure {
    ReplicationFailure::Source(ReplicationStreamErrorV3::InvalidPosition)
}

impl ReplicationRequest {
    /// Uses only checked caller selection. A decoded manifest proves neither
    /// source origin nor retention, acknowledgement, publication or promotion.
    pub(crate) fn checked_audit_targets(
        &self,
    ) -> Result<ServiceAuditTargetsV1, ReplicationFailure> {
        if self.history_incarnation == 0
            || self.readable_format.is_empty()
            || self.readable_format.len() > 128
        {
            return Err(invalid());
        }
        let epoch = LeadershipEpochV1::new(self.leadership_epoch).ok_or_else(invalid)?;
        let hold = match &self.phase {
            ReplicationPhase::FenceEvidence { request } if self.after_sequence != 0 => {
                let target = request.target();
                if target.database_id() != self.database_id
                    || target.history_incarnation() != self.history_incarnation
                    || target.leadership_epoch() != epoch
                {
                    return Err(invalid());
                }
                Some(target.hold_id())
            }
            ReplicationPhase::Tail if self.after_sequence != 0 => None,
            ReplicationPhase::Follower { hold_id } if self.after_sequence != 0 => {
                Some(ReplicationSourceHoldIdV1::new(*hold_id).ok_or_else(invalid)?)
            }
            ReplicationPhase::Attach { manifest } if self.after_sequence != 0 => {
                Some(self.checked_manifest(manifest)?.fence().hold_id())
            }
            ReplicationPhase::Bootstrap {
                hold_id,
                resume_manifest,
                after_page,
            } if self.after_sequence == 0
                && self.after_hash == [0; 32]
                && self.after_frontier == DualFrontier::INITIAL
                && *after_page <= 1_048_576 =>
            {
                let hold = ReplicationSourceHoldIdV1::new(*hold_id).ok_or_else(invalid)?;
                if resume_manifest.is_empty() {
                    if *after_page != 0 {
                        return Err(invalid());
                    }
                } else {
                    let manifest = self.checked_manifest(resume_manifest)?;
                    if manifest.fence().hold_id() != hold || *after_page > manifest.page_count() {
                        return Err(invalid());
                    }
                }
                Some(hold)
            }
            _ => return Err(invalid()),
        };
        let Some(hold) = hold else {
            return Ok(ServiceAuditTargetsV1::empty());
        };
        let target = ReplicationFollowerAuditTargetV1::new(
            self.database_id,
            self.history_incarnation,
            epoch,
            hold,
        )
        .ok_or_else(invalid)?;
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(target)])
            .map_err(|_| invalid())
    }

    fn checked_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<ReplicationBootstrapManifestV1, ReplicationFailure> {
        // Keep the API-neutral bound independent of the lower decoder's ceiling.
        if bytes.is_empty() || bytes.len() > 512 {
            return Err(invalid());
        }
        let manifest = ReplicationBootstrapManifestV1::decode(bytes).map_err(|_| invalid())?;
        let lineage = manifest.fence().history().lineage();
        if lineage.database_id() != self.database_id
            || lineage.history_incarnation() != self.history_incarnation
            || lineage.leadership_epoch().get() != self.leadership_epoch
            || lineage.catalog_digest() != self.catalog_digest
        {
            return Err(invalid());
        }
        Ok(manifest)
    }
}

#[cfg(test)]
#[path = "replication_request_tests.rs"]
mod tests;
