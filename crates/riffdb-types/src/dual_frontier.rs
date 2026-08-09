//! The dual durable frontier ADR-0100 addresses changelog frames by.
//!
//! ADR-0101 publishes two coordinated sequences at one durability fence: the
//! application commit sequence and the administration sequence. ADR-0100 §1
//! makes the pair the address of a changelog frame — a frame names its
//! predecessor pair and its covered pair, "encoding an unchanged frontier
//! explicitly".
//!
//! This module owns only the semantic identity of that pair. It deliberately
//! does not touch [`crate::CommitToken`] or [`crate::ProjectionFrontier`],
//! which remain application-sequence-only freshness vocabulary (ADR-0086).

use std::cmp::Ordering;
use std::fmt;

use crate::{AdministrationSequence, CommitSequence};

/// Encoded byte length of a canonical [`DualFrontier`].
pub const DUAL_FRONTIER_BYTES: usize = 18;

/// Explicit tag for an absent (never-advanced) frontier component.
const COMPONENT_ABSENT: u8 = 0x00;
/// Explicit tag for a present nonzero frontier component.
const COMPONENT_PRESENT: u8 = 0x01;

/// One published durable frontier: an application and an administration position.
///
/// `None` in either component means that space has never advanced. The
/// canonical encoding tags presence explicitly so an unchanged frontier is
/// never confused with a zero sequence.
///
/// Comparison is the componentwise (product) order and is deliberately
/// **partial**: a pair whose application component rises while its
/// administration component falls is *incomparable*, not "greater". Every
/// caller that needs "does this advance?" must use [`DualFrontier::advances_from`],
/// which answers `false` for an incomparable pair. This is the same fail-closed
/// discipline [`crate::ProjectionFrontier::satisfies`] applies across
/// incarnations: an unproven relation is never treated as satisfied.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DualFrontier {
    application: Option<CommitSequence>,
    administration: Option<AdministrationSequence>,
}

impl DualFrontier {
    /// The frontier of a database on which nothing has ever been published.
    pub const INITIAL: Self = Self {
        application: None,
        administration: None,
    };

    /// Builds a frontier pair from its two independently optional components.
    #[must_use]
    pub const fn new(
        application: Option<CommitSequence>,
        administration: Option<AdministrationSequence>,
    ) -> Self {
        Self {
            application,
            administration,
        }
    }

    /// Returns the published application commit-sequence component.
    #[must_use]
    pub const fn application(self) -> Option<CommitSequence> {
        self.application
    }

    /// Returns the published administration-sequence component.
    #[must_use]
    pub const fn administration(self) -> Option<AdministrationSequence> {
        self.administration
    }

    /// Returns true when nothing has ever been published.
    #[must_use]
    pub const fn is_initial(self) -> bool {
        self.application.is_none() && self.administration.is_none()
    }

    /// Returns true when `self` is a strict successor of `predecessor`.
    ///
    /// Both components must be greater than or equal to the predecessor's and
    /// at least one must be strictly greater. An incomparable pair — one
    /// component up, the other down — answers `false`.
    #[must_use]
    pub fn advances_from(self, predecessor: Self) -> bool {
        self.partial_cmp(&predecessor) == Some(Ordering::Greater)
    }

    /// Returns the canonical [`DUAL_FRONTIER_BYTES`]-byte encoding.
    ///
    /// Layout: `[app presence][app u64 BE][admin presence][admin u64 BE]`.
    /// An absent component encodes its presence tag plus eight zero bytes, so
    /// "unchanged" is explicit rather than inferred.
    #[must_use]
    pub const fn to_canonical_bytes(self) -> [u8; DUAL_FRONTIER_BYTES] {
        let mut encoded = [0_u8; DUAL_FRONTIER_BYTES];
        if let Some(application) = self.application {
            encoded[0] = COMPONENT_PRESENT;
            let value = application.to_be_bytes();
            let mut index = 0;
            while index < 8 {
                encoded[1 + index] = value[index];
                index += 1;
            }
        } else {
            encoded[0] = COMPONENT_ABSENT;
        }
        if let Some(administration) = self.administration {
            encoded[9] = COMPONENT_PRESENT;
            let value = administration.to_be_bytes();
            let mut index = 0;
            while index < 8 {
                encoded[10 + index] = value[index];
                index += 1;
            }
        } else {
            encoded[9] = COMPONENT_ABSENT;
        }
        encoded
    }

    /// Decodes a canonical encoding, failing closed on any noncanonical shape.
    pub fn from_canonical_bytes(
        bytes: [u8; DUAL_FRONTIER_BYTES],
    ) -> Result<Self, DualFrontierError> {
        let application = decode_component(bytes[0], &bytes[1..9])?
            .map(|value| CommitSequence::new(value).ok_or(DualFrontierError::ZeroSequence))
            .transpose()?;
        let administration = decode_component(bytes[9], &bytes[10..18])?
            .map(|value| AdministrationSequence::new(value).ok_or(DualFrontierError::ZeroSequence))
            .transpose()?;
        Ok(Self::new(application, administration))
    }
}

fn decode_component(presence: u8, value: &[u8]) -> Result<Option<u64>, DualFrontierError> {
    let raw = u64::from_be_bytes(
        value
            .try_into()
            .map_err(|_| DualFrontierError::InvalidShape)?,
    );
    match presence {
        COMPONENT_ABSENT if raw == 0 => Ok(None),
        COMPONENT_ABSENT => Err(DualFrontierError::NonCanonicalAbsence),
        COMPONENT_PRESENT => Ok(Some(raw)),
        _ => Err(DualFrontierError::InvalidShape),
    }
}

impl PartialOrd for DualFrontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let application = component_cmp(
            self.application.map(CommitSequence::get),
            other.application.map(CommitSequence::get),
        );
        let administration = component_cmp(
            self.administration.map(AdministrationSequence::get),
            other.administration.map(AdministrationSequence::get),
        );
        match (application, administration) {
            (Ordering::Equal, value) | (value, Ordering::Equal) => Some(value),
            (Ordering::Less, Ordering::Less) => Some(Ordering::Less),
            (Ordering::Greater, Ordering::Greater) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

fn component_cmp(left: Option<u64>, right: Option<u64>) -> Ordering {
    left.unwrap_or(0).cmp(&right.unwrap_or(0))
}

impl fmt::Display for DualFrontier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.application, self.administration) {
            (Some(application), Some(administration)) => {
                write!(formatter, "(app {application}, admin {administration})")
            }
            (Some(application), None) => write!(formatter, "(app {application}, admin none)"),
            (None, Some(administration)) => write!(formatter, "(app none, admin {administration})"),
            (None, None) => formatter.write_str("(app none, admin none)"),
        }
    }
}

/// A safe failure to decode a canonical [`DualFrontier`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DualFrontierError {
    /// A presence tag is not one of the two closed values.
    InvalidShape,
    /// A component is tagged absent but carries nonzero bytes.
    NonCanonicalAbsence,
    /// A component is tagged present but encodes sequence zero.
    ZeroSequence,
}

impl fmt::Display for DualFrontierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidShape => "dual frontier bytes have an invalid shape",
            Self::NonCanonicalAbsence => {
                "an absent dual frontier component is not canonically zero"
            }
            Self::ZeroSequence => "a present dual frontier component must be nonzero",
        })
    }
}

impl std::error::Error for DualFrontierError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn application(value: u64) -> Option<CommitSequence> {
        CommitSequence::new(value)
    }

    fn administration(value: u64) -> Option<AdministrationSequence> {
        AdministrationSequence::new(value)
    }

    fn pair(app: u64, admin: u64) -> DualFrontier {
        DualFrontier::new(application(app), administration(admin))
    }

    #[test]
    fn initial_is_both_components_absent() {
        assert!(DualFrontier::INITIAL.is_initial());
        assert_eq!(DualFrontier::INITIAL.application(), None);
        assert_eq!(DualFrontier::INITIAL.administration(), None);
        assert!(!pair(1, 0).is_initial());
    }

    #[test]
    fn canonical_bytes_round_trip_over_every_presence_combination() {
        for frontier in [
            DualFrontier::INITIAL,
            pair(1, 0),
            pair(0, 1),
            pair(7, 9),
            pair(u64::MAX, u64::MAX),
        ] {
            let encoded = frontier.to_canonical_bytes();
            assert_eq!(encoded.len(), DUAL_FRONTIER_BYTES);
            assert_eq!(
                DualFrontier::from_canonical_bytes(encoded).expect("canonical round trip"),
                frontier
            );
        }
    }

    #[test]
    fn absence_is_encoded_explicitly_and_never_confused_with_zero() {
        let encoded = DualFrontier::INITIAL.to_canonical_bytes();
        assert_eq!(encoded, [0_u8; DUAL_FRONTIER_BYTES]);
        let present_zero = {
            let mut bytes = encoded;
            bytes[0] = COMPONENT_PRESENT;
            bytes
        };
        assert_eq!(
            DualFrontier::from_canonical_bytes(present_zero),
            Err(DualFrontierError::ZeroSequence)
        );
        let absent_nonzero = {
            let mut bytes = encoded;
            bytes[8] = 1;
            bytes
        };
        assert_eq!(
            DualFrontier::from_canonical_bytes(absent_nonzero),
            Err(DualFrontierError::NonCanonicalAbsence)
        );
        let unknown_tag = {
            let mut bytes = encoded;
            bytes[9] = 0x02;
            bytes
        };
        assert_eq!(
            DualFrontier::from_canonical_bytes(unknown_tag),
            Err(DualFrontierError::InvalidShape)
        );
    }

    #[test]
    fn a_command_frame_advances_only_the_application_component() {
        let predecessor = pair(3, 4);
        let covered = pair(5, 4);
        assert!(covered.advances_from(predecessor));
        assert!(!predecessor.advances_from(covered));
        assert_eq!(covered.partial_cmp(&predecessor), Some(Ordering::Greater));
    }

    #[test]
    fn a_service_audit_frame_advances_only_the_administration_component() {
        let predecessor = pair(5, 4);
        let covered = pair(5, 6);
        assert!(covered.advances_from(predecessor));
        assert!(!predecessor.advances_from(covered));
    }

    #[test]
    fn an_equal_pair_never_advances() {
        assert!(!pair(5, 4).advances_from(pair(5, 4)));
        assert!(!DualFrontier::INITIAL.advances_from(DualFrontier::INITIAL));
        assert_eq!(pair(5, 4).partial_cmp(&pair(5, 4)), Some(Ordering::Equal));
    }

    #[test]
    fn the_first_frame_advances_from_the_initial_frontier() {
        assert!(pair(1, 0).advances_from(DualFrontier::INITIAL));
        assert!(pair(0, 1).advances_from(DualFrontier::INITIAL));
        assert!(pair(1, 1).advances_from(DualFrontier::INITIAL));
    }

    #[test]
    fn an_incomparable_pair_is_never_an_advance_in_either_direction() {
        let left = pair(9, 2);
        let right = pair(3, 7);
        assert_eq!(left.partial_cmp(&right), None);
        assert_eq!(right.partial_cmp(&left), None);
        assert!(!left.advances_from(right));
        assert!(!right.advances_from(left));
    }

    #[test]
    fn a_regressed_component_is_never_an_advance() {
        assert!(!pair(4, 4).advances_from(pair(5, 4)));
        assert!(!pair(5, 3).advances_from(pair(5, 4)));
        assert!(!DualFrontier::INITIAL.advances_from(pair(1, 0)));
    }

    #[test]
    fn display_names_both_components_without_leaking_anything_else() {
        assert_eq!(pair(5, 4).to_string(), "(app 5, admin 4)");
        assert_eq!(pair(5, 0).to_string(), "(app 5, admin none)");
        assert_eq!(pair(0, 4).to_string(), "(app none, admin 4)");
        assert_eq!(DualFrontier::INITIAL.to_string(), "(app none, admin none)");
    }
}
