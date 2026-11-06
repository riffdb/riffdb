//! Checked epoch-vote and quorum-fence certificate values (`ADR-0337` Decision 2).
//!
//! Constructing or decoding these values grants no transport trust or cutover
//! authority; the storage and daemon owners must independently enforce durable
//! epoch promises and voter configuration bounds.

use std::num::NonZeroU64;

use sha2::{Digest, Sha256};

use crate::{
    AdministrationSequence, CommitSequence, DatabaseId, DualFrontier, LeadershipEpochV1,
    ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1, RequestId,
};

/// Maximum number of configured replication voters in a quorum group (`ADR-0337`).
pub const MAX_REPLICATION_VOTERS_V1: usize = 15;

const VOTE_REQUEST_MAGIC_V1: &[u8; 9] = b"RIFF_EVR1";
const VOTE_RESPONSE_MAGIC_V1: &[u8; 9] = b"RIFF_EVV1";
const CERTIFICATE_DOMAIN_V1: &[u8; 9] = b"RIFF_QFC1";

/// Typed validation or wire decoding error for epoch votes and quorum certificates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuorumFenceCertificateError {
    /// Wire payload length, magic, or checksum is invalid.
    InvalidEncoding,
    /// Configured voter set is empty, exceeds `MAX_REPLICATION_VOTERS_V1`, or contains duplicates.
    InvalidVoterConfiguration,
    /// Candidate is not a member of the configured voter set.
    CandidateNotVoter,
    /// Proposed leadership epoch is not the exact successor of the source epoch.
    InvalidProposedEpoch,
    /// A vote targets a different database, incarnation, epoch, or candidate.
    IdentityMismatch,
    /// A voter denied the vote or its durable frontier exceeds the candidate's frontier.
    CandidateFrontierLagging,
    /// Fewer than a strict majority of configured voters granted the vote.
    InsufficientQuorum,
}

/// Exact durable history point summary carried in epoch-vote requests and responses.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ReplicationEpochVotePositionV1 {
    transaction_sequence: NonZeroU64,
    history_hash: [u8; 32],
    frontier: DualFrontier,
}

impl ReplicationEpochVotePositionV1 {
    /// Constructs a checked vote position.
    #[must_use]
    pub const fn new(
        transaction_sequence: NonZeroU64,
        history_hash: [u8; 32],
        frontier: DualFrontier,
    ) -> Self {
        Self {
            transaction_sequence,
            history_hash,
            frontier,
        }
    }

    /// Returns the physical changelog transaction sequence.
    #[must_use]
    pub const fn transaction_sequence(self) -> NonZeroU64 {
        self.transaction_sequence
    }

    /// Returns the cumulative receipt history hash at this sequence.
    #[must_use]
    pub const fn history_hash(self) -> [u8; 32] {
        self.history_hash
    }

    /// Returns the dual application/administration frontier at this sequence.
    #[must_use]
    pub const fn frontier(self) -> DualFrontier {
        self.frontier
    }

    /// Returns `true` iff `self` is at least as up-to-date as `voter` in the same
    /// lineage (`ADR-0337` Decision 2). At an equal transaction sequence, the
    /// history hash and dual frontier must match identically.
    #[must_use]
    pub fn is_at_least_as_up_to_date_as(self, voter: Self) -> bool {
        if self.transaction_sequence < voter.transaction_sequence
            || self.frontier.application() < voter.frontier.application()
            || self.frontier.administration() < voter.frontier.administration()
        {
            return false;
        }
        if self.transaction_sequence == voter.transaction_sequence && self != voter {
            return false;
        }
        true
    }

    fn encode_into(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.transaction_sequence.get().to_be_bytes());
        out.extend_from_slice(&self.history_hash);
        out.extend_from_slice(
            &self
                .frontier
                .application()
                .map_or(0_u64, CommitSequence::get)
                .to_be_bytes(),
        );
        out.extend_from_slice(
            &self
                .frontier
                .administration()
                .map_or(0_u64, AdministrationSequence::get)
                .to_be_bytes(),
        );
    }

    fn decode_from(bytes: &[u8]) -> Result<Self, QuorumFenceCertificateError> {
        if bytes.len() != 56 {
            return Err(QuorumFenceCertificateError::InvalidEncoding);
        }
        let seq = NonZeroU64::new(u64::from_be_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        ))
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let history_hash: [u8; 32] = bytes[8..40]
            .try_into()
            .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?;
        let raw_app = u64::from_be_bytes(
            bytes[40..48]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        );
        let raw_admin = u64::from_be_bytes(
            bytes[48..56]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        );
        let app = if raw_app == 0 {
            None
        } else {
            Some(CommitSequence::new(raw_app).ok_or(QuorumFenceCertificateError::InvalidEncoding)?)
        };
        let admin = if raw_admin == 0 {
            None
        } else {
            Some(
                AdministrationSequence::new(raw_admin)
                    .ok_or(QuorumFenceCertificateError::InvalidEncoding)?,
            )
        };
        Ok(Self::new(seq, history_hash, DualFrontier::new(app, admin)))
    }
}

impl std::fmt::Debug for ReplicationEpochVotePositionV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationEpochVotePositionV1([REDACTED])")
    }
}

/// Candidate request for a voter follower's durable epoch promise (`RequestReplicationEpochVote`).
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationEpochVoteRequestV1 {
    request_id: RequestId,
    candidate_target: ReplicationFollowerAuditTargetV1,
    proposed_epoch: LeadershipEpochV1,
    candidate_applied: ReplicationEpochVotePositionV1,
}

impl ReplicationEpochVoteRequestV1 {
    /// Constructs a checked epoch-vote request where `proposed_epoch` is the
    /// exact successor of `candidate_target.leadership_epoch()`.
    #[must_use]
    pub fn new(
        request_id: RequestId,
        candidate_target: ReplicationFollowerAuditTargetV1,
        proposed_epoch: LeadershipEpochV1,
        candidate_applied: ReplicationEpochVotePositionV1,
    ) -> Option<Self> {
        if candidate_target.leadership_epoch().checked_next() != Some(proposed_epoch) {
            return None;
        }
        Some(Self {
            request_id,
            candidate_target,
            proposed_epoch,
            candidate_applied,
        })
    }

    /// Transport request identity.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Candidate's source lineage and hold identity.
    #[must_use]
    pub const fn candidate_target(self) -> ReplicationFollowerAuditTargetV1 {
        self.candidate_target
    }

    /// Proposed successor leadership epoch `E' = E + 1`.
    #[must_use]
    pub const fn proposed_epoch(self) -> LeadershipEpochV1 {
        self.proposed_epoch
    }

    /// Candidate's drained or latest durable applied history position.
    #[must_use]
    pub const fn candidate_applied(self) -> ReplicationEpochVotePositionV1 {
        self.candidate_applied
    }

    /// Encodes the request into a canonical, checksummed 161-byte payload.
    #[must_use]
    pub fn encode_v1(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(161);
        out.extend_from_slice(VOTE_REQUEST_MAGIC_V1);
        out.extend_from_slice(&self.request_id.into_bytes());
        out.extend_from_slice(&self.candidate_target.database_id().into_bytes());
        out.extend_from_slice(&self.candidate_target.history_incarnation().to_be_bytes());
        out.extend_from_slice(&self.candidate_target.leadership_epoch().get().to_be_bytes());
        out.extend_from_slice(self.candidate_target.hold_id().as_bytes());
        out.extend_from_slice(&self.proposed_epoch.get().to_be_bytes());
        self.candidate_applied.encode_into(&mut out);
        let checksum: [u8; 32] = Sha256::digest(&out).into();
        out.extend_from_slice(&checksum);
        out
    }

    /// Decodes and validates a canonical 161-byte request payload.
    pub fn decode_v1(bytes: &[u8]) -> Result<Self, QuorumFenceCertificateError> {
        if bytes.len() != 161 || &bytes[0..9] != VOTE_REQUEST_MAGIC_V1 {
            return Err(QuorumFenceCertificateError::InvalidEncoding);
        }
        let expected_checksum: [u8; 32] = Sha256::digest(&bytes[0..129]).into();
        if bytes[129..161] != expected_checksum {
            return Err(QuorumFenceCertificateError::InvalidEncoding);
        }
        let request_id = RequestId::from_bytes(
            bytes[9..25]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?;
        let database_id = DatabaseId::from_bytes(
            bytes[25..41]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?;
        let history_incarnation = u64::from_be_bytes(
            bytes[41..49]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        );
        let source_epoch = LeadershipEpochV1::new(u64::from_be_bytes(
            bytes[49..57]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        ))
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let hold_id = ReplicationSourceHoldIdV1::new(
            bytes[57..73]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let proposed_epoch = LeadershipEpochV1::new(u64::from_be_bytes(
            bytes[73..81]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        ))
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let candidate_target = ReplicationFollowerAuditTargetV1::new(
            database_id,
            history_incarnation,
            source_epoch,
            hold_id,
        )
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let candidate_applied = ReplicationEpochVotePositionV1::decode_from(&bytes[81..137])?;
        Self::new(
            request_id,
            candidate_target,
            proposed_epoch,
            candidate_applied,
        )
        .ok_or(QuorumFenceCertificateError::InvalidProposedEpoch)
    }
}

impl std::fmt::Debug for ReplicationEpochVoteRequestV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationEpochVoteRequestV1([REDACTED])")
    }
}

/// Durable epoch-vote response from a configured voter follower.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationEpochVoteV1 {
    voter_hold_id: ReplicationSourceHoldIdV1,
    database_id: DatabaseId,
    history_incarnation: NonZeroU64,
    promised_epoch: LeadershipEpochV1,
    candidate_hold_id: ReplicationSourceHoldIdV1,
    candidate_applied: ReplicationEpochVotePositionV1,
    voter_applied: ReplicationEpochVotePositionV1,
    granted: bool,
}

impl ReplicationEpochVoteV1 {
    /// Constructs a checked epoch vote. A granted vote requires the candidate's
    /// applied position to be at least as up-to-date as the voter's position.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        voter_hold_id: ReplicationSourceHoldIdV1,
        database_id: DatabaseId,
        history_incarnation: u64,
        promised_epoch: LeadershipEpochV1,
        candidate_hold_id: ReplicationSourceHoldIdV1,
        candidate_applied: ReplicationEpochVotePositionV1,
        voter_applied: ReplicationEpochVotePositionV1,
        granted: bool,
    ) -> Option<Self> {
        let history_incarnation = NonZeroU64::new(history_incarnation)?;
        if granted && !candidate_applied.is_at_least_as_up_to_date_as(voter_applied) {
            return None;
        }
        Some(Self {
            voter_hold_id,
            database_id,
            history_incarnation,
            promised_epoch,
            candidate_hold_id,
            candidate_applied,
            voter_applied,
            granted,
        })
    }

    /// Hold identity of the voter that produced this response.
    #[must_use]
    pub const fn voter_hold_id(self) -> ReplicationSourceHoldIdV1 {
        self.voter_hold_id
    }

    /// Database identity bound by the vote.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    /// Source history incarnation bound by the vote.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation.get()
    }

    /// Highest leadership epoch durably promised by the voter.
    #[must_use]
    pub const fn promised_epoch(self) -> LeadershipEpochV1 {
        self.promised_epoch
    }

    /// Candidate hold identity evaluated by the voter.
    #[must_use]
    pub const fn candidate_hold_id(self) -> ReplicationSourceHoldIdV1 {
        self.candidate_hold_id
    }

    /// Candidate applied position evaluated by the voter.
    #[must_use]
    pub const fn candidate_applied(self) -> ReplicationEpochVotePositionV1 {
        self.candidate_applied
    }

    /// Voter's own durable applied position at the time of evaluation.
    #[must_use]
    pub const fn voter_applied(self) -> ReplicationEpochVotePositionV1 {
        self.voter_applied
    }

    /// Whether the voter durably promised `promised_epoch` to `candidate_hold_id`.
    #[must_use]
    pub const fn granted(self) -> bool {
        self.granted
    }

    /// Encodes the vote response into a canonical, checksummed 218-byte payload.
    #[must_use]
    pub fn encode_v1(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(218);
        out.extend_from_slice(VOTE_RESPONSE_MAGIC_V1);
        out.extend_from_slice(self.voter_hold_id.as_bytes());
        out.extend_from_slice(&self.database_id.into_bytes());
        out.extend_from_slice(&self.history_incarnation.get().to_be_bytes());
        out.extend_from_slice(&self.promised_epoch.get().to_be_bytes());
        out.extend_from_slice(self.candidate_hold_id.as_bytes());
        self.candidate_applied.encode_into(&mut out);
        self.voter_applied.encode_into(&mut out);
        out.push(u8::from(self.granted));
        let checksum: [u8; 32] = Sha256::digest(&out).into();
        out.extend_from_slice(&checksum);
        out
    }

    /// Decodes and validates a canonical 218-byte vote response payload.
    pub fn decode_v1(bytes: &[u8]) -> Result<Self, QuorumFenceCertificateError> {
        if bytes.len() != 218 || &bytes[0..9] != VOTE_RESPONSE_MAGIC_V1 {
            return Err(QuorumFenceCertificateError::InvalidEncoding);
        }
        let expected_checksum: [u8; 32] = Sha256::digest(&bytes[0..186]).into();
        if bytes[186..218] != expected_checksum {
            return Err(QuorumFenceCertificateError::InvalidEncoding);
        }
        let voter_hold_id = ReplicationSourceHoldIdV1::new(
            bytes[9..25]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let database_id = DatabaseId::from_bytes(
            bytes[25..41]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?;
        let history_incarnation = u64::from_be_bytes(
            bytes[41..49]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        );
        let promised_epoch = LeadershipEpochV1::new(u64::from_be_bytes(
            bytes[49..57]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        ))
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let candidate_hold_id = ReplicationSourceHoldIdV1::new(
            bytes[57..73]
                .try_into()
                .map_err(|_| QuorumFenceCertificateError::InvalidEncoding)?,
        )
        .ok_or(QuorumFenceCertificateError::InvalidEncoding)?;
        let candidate_applied = ReplicationEpochVotePositionV1::decode_from(&bytes[73..129])?;
        let voter_applied = ReplicationEpochVotePositionV1::decode_from(&bytes[129..185])?;
        let granted = match bytes[185] {
            0 => false,
            1 => true,
            _ => return Err(QuorumFenceCertificateError::InvalidEncoding),
        };
        Self::new(
            voter_hold_id,
            database_id,
            history_incarnation,
            promised_epoch,
            candidate_hold_id,
            candidate_applied,
            voter_applied,
            granted,
        )
        .ok_or(QuorumFenceCertificateError::CandidateFrontierLagging)
    }
}

impl std::fmt::Debug for ReplicationEpochVoteV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationEpochVoteV1([REDACTED])")
    }
}

/// Validated majority epoch-vote certificate fencing out `source_epoch` across a
/// quorum of `configured_voters` and certifying `candidate_applied` (`ADR-0337` Decision 2).
#[derive(Clone, Eq, PartialEq)]
pub struct QuorumFenceCertificateV1 {
    database_id: DatabaseId,
    history_incarnation: NonZeroU64,
    source_epoch: LeadershipEpochV1,
    promised_epoch: LeadershipEpochV1,
    candidate_hold_id: ReplicationSourceHoldIdV1,
    candidate_applied: ReplicationEpochVotePositionV1,
    votes: Vec<ReplicationEpochVoteV1>,
}

impl QuorumFenceCertificateV1 {
    /// Validates that a strict majority of `configured_voters` granted their vote
    /// for `promised_epoch = source_epoch + 1` to `candidate_hold_id` at
    /// `candidate_applied`, and that every granting voter's durable frontier is
    /// `<= candidate_applied`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        source_epoch: LeadershipEpochV1,
        promised_epoch: LeadershipEpochV1,
        candidate_hold_id: ReplicationSourceHoldIdV1,
        candidate_applied: ReplicationEpochVotePositionV1,
        configured_voters: &[ReplicationSourceHoldIdV1],
        mut votes: Vec<ReplicationEpochVoteV1>,
    ) -> Result<Self, QuorumFenceCertificateError> {
        let history_incarnation = NonZeroU64::new(history_incarnation)
            .ok_or(QuorumFenceCertificateError::IdentityMismatch)?;
        if source_epoch.checked_next() != Some(promised_epoch) {
            return Err(QuorumFenceCertificateError::InvalidProposedEpoch);
        }
        if configured_voters.is_empty() || configured_voters.len() > MAX_REPLICATION_VOTERS_V1 {
            return Err(QuorumFenceCertificateError::InvalidVoterConfiguration);
        }
        let mut sorted_voters = configured_voters.to_vec();
        sorted_voters.sort_unstable();
        if sorted_voters.windows(2).any(|w| w[0] == w[1]) {
            return Err(QuorumFenceCertificateError::InvalidVoterConfiguration);
        }
        if sorted_voters.binary_search(&candidate_hold_id).is_err() {
            return Err(QuorumFenceCertificateError::CandidateNotVoter);
        }
        votes.sort_by_key(|vote| vote.voter_hold_id());
        if votes
            .windows(2)
            .any(|w| w[0].voter_hold_id() == w[1].voter_hold_id())
        {
            return Err(QuorumFenceCertificateError::InvalidVoterConfiguration);
        }
        for vote in &votes {
            if sorted_voters.binary_search(&vote.voter_hold_id()).is_err() {
                return Err(QuorumFenceCertificateError::InvalidVoterConfiguration);
            }
            if vote.database_id() != database_id
                || vote.history_incarnation() != history_incarnation.get()
                || vote.promised_epoch() != promised_epoch
                || vote.candidate_hold_id() != candidate_hold_id
                || vote.candidate_applied() != candidate_applied
            {
                return Err(QuorumFenceCertificateError::IdentityMismatch);
            }
            if !vote.granted()
                || !candidate_applied.is_at_least_as_up_to_date_as(vote.voter_applied())
            {
                return Err(QuorumFenceCertificateError::CandidateFrontierLagging);
            }
        }
        let required_majority = (configured_voters.len() / 2) + 1;
        if votes.len() < required_majority {
            return Err(QuorumFenceCertificateError::InsufficientQuorum);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            source_epoch,
            promised_epoch,
            candidate_hold_id,
            candidate_applied,
            votes,
        })
    }

    /// Database identity bound by the certificate.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Source history incarnation bound by the certificate.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation.get()
    }

    /// Fenced source leadership epoch `E`.
    #[must_use]
    pub const fn source_epoch(&self) -> LeadershipEpochV1 {
        self.source_epoch
    }

    /// Promised successor leadership epoch `E' = E + 1`.
    #[must_use]
    pub const fn promised_epoch(&self) -> LeadershipEpochV1 {
        self.promised_epoch
    }

    /// Elected candidate follower hold identity.
    #[must_use]
    pub const fn candidate_hold_id(&self) -> ReplicationSourceHoldIdV1 {
        self.candidate_hold_id
    }

    /// Elected candidate's certified applied position.
    #[must_use]
    pub const fn candidate_applied(&self) -> ReplicationEpochVotePositionV1 {
        self.candidate_applied
    }

    /// Canonically ordered granted votes comprising the majority quorum.
    #[must_use]
    pub fn votes(&self) -> &[ReplicationEpochVoteV1] {
        &self.votes
    }

    /// Deterministic SHA-256 digest over the canonical certificate and its sorted votes.
    #[must_use]
    pub fn certificate_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(CERTIFICATE_DOMAIN_V1);
        hasher.update(self.database_id.into_bytes());
        hasher.update(self.history_incarnation.get().to_be_bytes());
        hasher.update(self.source_epoch.get().to_be_bytes());
        hasher.update(self.promised_epoch.get().to_be_bytes());
        hasher.update(self.candidate_hold_id.as_bytes());
        let mut pos_bytes = Vec::with_capacity(56);
        self.candidate_applied.encode_into(&mut pos_bytes);
        hasher.update(&pos_bytes);
        hasher.update((self.votes.len() as u32).to_be_bytes());
        for vote in &self.votes {
            hasher.update(vote.encode_v1());
        }
        hasher.finalize().into()
    }
}

impl std::fmt::Debug for QuorumFenceCertificateV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuorumFenceCertificateV1([REDACTED])")
    }
}
