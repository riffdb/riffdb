//! Closed structural owners for additive family76/77/78 revision2.
use super::*;

shape!(PRIMARY_FENCE_ADMINISTRATION_V2 [
    message(2, &TIMESTAMP), fixed_bytes(3, 16), fixed_bytes(4, 16),
    message(5, &AUDIT_PRINCIPAL), string(6, MAX_TEXT_ID_BYTES),
    message(7, &REPLICATION_FOLLOWER_AUDIT_TARGET), message(9, &CHANGELOG_POSITION_V3),
    fixed_bytes(10, 32),
]);
shape!(PRIMARY_ADMISSION_ACTIVE_V2 [fixed_bytes(1,16),fixed_bytes(4,32)]);
shape!(REPLICATION_PRIMARY_ADMISSION_V2 [message(1,&PRIMARY_ADMISSION_ACTIVE_V2),message(2,&PRIMARY_FENCE_ADMINISTRATION_V2)]);
shape!(PROMOTION_ADMINISTRATION_V2 [
    message(2,&TIMESTAMP), fixed_bytes(3,16), fixed_bytes(4,16),
    message(5,&AUDIT_PRINCIPAL), string(6,MAX_TEXT_ID_BYTES), message(7,&TIMESTAMP),
    message(8,&PRIMARY_FENCE_ADMINISTRATION_V2), message(9,&CHANGELOG_POSITION_V3),
    message(10,&CHANGELOG_HISTORY_STATE_V3), nonempty_bytes(14,16), fixed_bytes(16,32),
]);

pub(super) const ADMISSION: &Shape = &REPLICATION_PRIMARY_ADMISSION_V2;
pub(super) const FENCE: &Shape = &PRIMARY_FENCE_ADMINISTRATION_V2;
pub(super) const PROMOTION: &Shape = &PROMOTION_ADMINISTRATION_V2;
