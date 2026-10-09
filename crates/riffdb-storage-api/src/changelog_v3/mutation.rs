use sha2::{Digest, Sha256};

use crate::{AuthoritativeNamespaceV1, MAX_CHANGELOG_FRAME_BYTES, ReplicationAuthorityClassV1};

use super::ChangelogV3Error;

/// One checked net key transition. Fields are private so a caller cannot
/// inject control state, an invalid metadata key, or an unbounded payload.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeMutationV3 {
    namespace: AuthoritativeNamespaceV1,
    key: Box<[u8]>,
    expected_hash: Option<[u8; 32]>,
    value: Option<Box<[u8]>>,
}

impl AuthoritativeMutationV3 {
    /// Forms an exact delete from the immutable transaction/snapshot pre-image,
    /// using the same expected-state hash owner as replacements. Bounds and
    /// authority class are checked before hashing; absence is not a wildcard.
    pub fn delete_matching(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        before: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, before.len())?;
        Self::delete(namespace, key, Sha256::digest(before).into())
    }

    /// Forms an exact replacement from a transaction-owned pre-image. Hashing
    /// lives with the expected-state codec, not a second adapter implementation.
    pub fn replace(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        before: &[u8],
        after: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, before.len())?;
        Self::validate(namespace, key, after.len())?;
        Self::put(namespace, key, Some(Sha256::digest(before).into()), after)
    }

    /// Builds an exact insert (absent prior) or replacement (SHA-256 prior).
    /// Input lengths are checked before any payload is copied.
    pub fn put(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        expected_hash: Option<[u8; 32]>,
        value: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, value.len())?;
        Ok(Self {
            namespace,
            key: key.into(),
            expected_hash,
            value: Some(value.into()),
        })
    }

    /// Builds a delete with a mandatory exact prior-value hash.
    pub fn delete(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        expected_hash: [u8; 32],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, 0)?;
        Ok(Self {
            namespace,
            key: key.into(),
            expected_hash: Some(expected_hash),
            value: None,
        })
    }

    pub(super) fn validate(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        value_len: usize,
    ) -> Result<(), ChangelogV3Error> {
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative
            || namespace
                .metadata_key()
                .is_some_and(|expected| expected.as_bytes() != key)
        {
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        if key.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let bytes = 44_usize
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value_len))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        Ok(())
    }

    /// Closed authoritative namespace.
    #[must_use]
    pub const fn namespace(&self) -> AuthoritativeNamespaceV1 {
        self.namespace
    }

    /// Exact physical key; only internal storage/replication owners consume it.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Complete post-image for puts; deletes have none.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Expected absent or exact SHA-256 prior state; never public diagnostic data.
    #[must_use]
    pub const fn expected_hash(&self) -> Option<[u8; 32]> {
        self.expected_hash
    }

    /// Validates a predecessor without interpreting absence as a wildcard.
    #[must_use]
    pub fn matches_prior(&self, prior: Option<&[u8]>) -> bool {
        self.expected_hash == prior.map(|value| <[u8; 32]>::from(Sha256::digest(value)))
    }

    /// Encoded mutation bytes, including the fixed namespace/operation/hash header.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        44 + self.key.len() + self.value.as_ref().map_or(0, |value| value.len())
    }
}

impl std::fmt::Debug for AuthoritativeMutationV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeMutationV3([redacted])")
    }
}

/// Bounded sequential-to-net conversion for one authoritative transaction.
/// This does not validate the first observation against storage: the writer or
/// validated journal owner must do so. Every subsequent observation of a key
/// must match the preceding post-image. Ignoring an error cannot yield a receipt.
pub struct AuthoritativeMutationAccumulatorV3 {
    changes: std::collections::BTreeMap<(AuthoritativeNamespaceV1, Box<[u8]>), NetTransition>,
    encoded_bytes: usize,
    failure: Option<ChangelogV3Error>,
}

struct NetTransition {
    expected_hash: Option<[u8; 32]>,
    current: CurrentState,
    restored: bool,
    value: Option<Box<[u8]>>,
}

/// The key's state after the latest observation, as its SHA-256 digest.
///
/// A digest is needed only to validate a later observation of the same key or
/// to detect an exact restoration of an existing predecessor. Nearly every
/// receipt row is observed once, and an insert can never be a restoration, so
/// an insert defers its digest until a second observation asks for it.
#[derive(Clone, Copy)]
enum CurrentState {
    /// Deleted (or never present): the digest is absence.
    Absent,
    /// Present with this exact digest.
    Hashed([u8; 32]),
    /// Present; the digest is that of the retained `value`, not yet computed.
    Unhashed,
}

impl NetTransition {
    fn current_hash(&self) -> Result<Option<[u8; 32]>, ChangelogV3Error> {
        match (self.current, self.value.as_deref()) {
            (CurrentState::Absent, _) => Ok(None),
            (CurrentState::Hashed(hash), _) => Ok(Some(hash)),
            (CurrentState::Unhashed, Some(value)) => Ok(Some(Sha256::digest(value).into())),
            // Only a retained value is left unhashed; a restoration always
            // stores its digest before dropping the value.
            (CurrentState::Unhashed, None) => Err(ChangelogV3Error::PredecessorMismatch),
        }
    }
}

// The provider receipt work bound charges `size_of::<NetTransition>()`; the
// lazy digest must not change that admitted size.
const _: () =
    assert!(std::mem::size_of::<CurrentState>() == std::mem::size_of::<Option<[u8; 32]>>());

impl Default for AuthoritativeMutationAccumulatorV3 {
    fn default() -> Self {
        Self {
            changes: std::collections::BTreeMap::new(),
            encoded_bytes: 160,
            failure: None,
        }
    }
}

impl AuthoritativeMutationAccumulatorV3 {
    /// Records an already checked sequential mutation. Repeated-key writes
    /// preserve the original precondition, validate each intervening edge, and
    /// retain only the final value. Exact restorations have no net transition.
    pub fn record(&mut self, mutation: AuthoritativeMutationV3) -> Result<(), ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.record_inner(mutation);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    fn record_inner(&mut self, mutation: AuthoritativeMutationV3) -> Result<(), ChangelogV3Error> {
        let AuthoritativeMutationV3 {
            namespace,
            key,
            expected_hash,
            value,
        } = mutation;
        let target = (namespace, key);
        let previous = self.changes.get(&target);
        let original = if let Some(previous) = previous {
            if expected_hash != previous.current_hash()? {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            previous.expected_hash
        } else {
            expected_hash
        };
        let current = match (value.as_deref(), original) {
            (None, _) => CurrentState::Absent,
            // Only an existing predecessor can be restored exactly.
            (Some(bytes), Some(_)) => CurrentState::Hashed(Sha256::digest(bytes).into()),
            (Some(_), None) => CurrentState::Unhashed,
        };
        let is_restoration = match (original, current) {
            (None, CurrentState::Absent) => true,
            (Some(original), CurrentState::Hashed(hash)) => original == hash,
            _ => false,
        };
        let old_bytes = previous.map_or(0, |previous| {
            44 + target.1.len() + previous.value.as_ref().map_or(0, |v| v.len())
        });
        // Keep a bounded observed-state marker even after exact cancellation:
        // forgetting it would let a later write invent a different predecessor.
        // The restored post-image bytes themselves are unnecessary; its digest
        // is enough to validate the next observation without retaining a copy.
        let value = if is_restoration { None } else { value };
        let next_bytes = 44 + target.1.len() + value.as_ref().map_or(0, |v| v.len());
        let encoded_bytes = self
            .encoded_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(next_bytes))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if encoded_bytes > super::frame::MAX_RECEIPT_BYTES
            || (previous.is_none() && self.changes.len() >= crate::MAX_CHANGELOG_FRAME_ENTRIES)
        {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        self.changes.insert(
            target,
            NetTransition {
                expected_hash: original,
                current,
                restored: is_restoration,
                value,
            },
        );
        self.encoded_bytes = encoded_bytes;
        Ok(())
    }

    /// First-party indexed source fold: retain only indices into one immutable
    /// source, then copy each final net value once. None is a checked control
    /// mutation excluded by the physical owner. No mutation permit is granted.
    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub fn fold_borrowed_indexed_v1<'a>(
        count: usize,
        get: impl Fn(
            usize,
        ) -> Result<
            Option<(
                AuthoritativeNamespaceV1,
                &'a [u8],
                Option<[u8; 32]>,
                Option<&'a [u8]>,
            )>,
            ChangelogV3Error,
        >,
    ) -> Result<Vec<AuthoritativeMutationV3>, ChangelogV3Error> {
        if count > MAX_CHANGELOG_FRAME_BYTES / 44 {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let mut indices = Vec::with_capacity(count);
        let mut input_bytes = 160_usize;
        for index in 0..count {
            let Some((namespace, key, expected, value)) = get(index)? else {
                continue;
            };
            AuthoritativeMutationV3::validate(namespace, key, value.map_or(0, <[u8]>::len))?;
            if value.is_none() && expected.is_none() {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            input_bytes = input_bytes
                .checked_add(44 + key.len() + value.map_or(0, <[u8]>::len))
                .filter(|bytes| *bytes <= super::frame::MAX_RECEIPT_BYTES)
                .ok_or(ChangelogV3Error::LimitExceeded)?;
            indices.push(index);
        }
        let mut failure = None;
        indices.sort_unstable_by(|left, right| match (get(*left), get(*right)) {
            (Ok(Some((ln, lk, _, _))), Ok(Some((rn, rk, _, _)))) => {
                (ln, lk, left).cmp(&(rn, rk, right))
            }
            (Err(error), _) | (_, Err(error)) => {
                failure = Some(error);
                left.cmp(right)
            }
            _ => {
                failure = Some(ChangelogV3Error::InvalidEncoding);
                left.cmp(right)
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        let mut result = Vec::with_capacity(indices.len().min(crate::MAX_CHANGELOG_FRAME_ENTRIES));
        let mut cursor = 0;
        let mut groups = 0_usize;
        while cursor < indices.len() {
            let (namespace, key, original, mut value) =
                get(indices[cursor])?.ok_or(ChangelogV3Error::InvalidEncoding)?;
            groups += 1;
            if groups > crate::MAX_CHANGELOG_FRAME_ENTRIES {
                return Err(ChangelogV3Error::LimitExceeded);
            }
            let mut current_hash = value.map(|value| <[u8; 32]>::from(Sha256::digest(value)));
            cursor += 1;
            while cursor < indices.len() {
                let (next_namespace, next_key, expected, next_value) =
                    get(indices[cursor])?.ok_or(ChangelogV3Error::InvalidEncoding)?;
                if (next_namespace, next_key) != (namespace, key) {
                    break;
                }
                if expected != current_hash {
                    return Err(ChangelogV3Error::PredecessorMismatch);
                }
                value = next_value;
                current_hash = value.map(|value| <[u8; 32]>::from(Sha256::digest(value)));
                cursor += 1;
            }
            if original != current_hash {
                result.push(match (value, original) {
                    (Some(value), expected) => {
                        AuthoritativeMutationV3::put(namespace, key, expected, value)?
                    }
                    (None, Some(hash)) => AuthoritativeMutationV3::delete(namespace, key, hash)?,
                    _ => return Err(ChangelogV3Error::InvalidEncoding),
                });
            }
        }
        Ok(result)
    }

    /// Consumes the complete successful sequence as canonical namespace/key order.
    /// A previous bound or precondition failure refuses the entire result.
    pub fn finish(self) -> Result<Vec<AuthoritativeMutationV3>, ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        Ok(self
            .changes
            .into_iter()
            .filter(|(_, transition)| !transition.restored)
            .map(|((namespace, key), transition)| AuthoritativeMutationV3 {
                namespace,
                key,
                expected_hash: transition.expected_hash,
                value: transition.value,
            })
            .collect())
    }
}

impl std::fmt::Debug for AuthoritativeMutationAccumulatorV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeMutationAccumulatorV3([redacted])")
    }
}

// First-party admission only: native shallow receipt/fold workspace, no schema decode.
#[doc(hidden)]
pub fn provider_receipt_work_bound_v1(
    count: usize,
    payload_bytes: usize,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    use std::mem::{align_of, size_of};
    let nodes = count
        .min(crate::MAX_CHANGELOG_FRAME_ENTRIES)
        .checked_add(1)
        .ok_or(BudgetUnavailable)?;
    let node = size_of::<(AuthoritativeNamespaceV1, Box<[u8]>)>()
        .checked_add(size_of::<NetTransition>())
        .and_then(|bytes| bytes.checked_mul(11))
        .and_then(|bytes| {
            bytes.checked_add(256 + align_of::<NetTransition>() + align_of::<Box<[u8]>>())
        })
        .ok_or(BudgetUnavailable)?;
    let tree = nodes
        .checked_mul(node)
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(BudgetUnavailable)?;
    let vectors = count
        .checked_add(4)
        .and_then(|count| count.checked_mul(size_of::<AuthoritativeMutationV3>()))
        .and_then(|bytes| bytes.checked_mul(8))
        .ok_or(BudgetUnavailable)?;
    let buffers = count
        .checked_add(4)
        .and_then(|count| count.checked_mul(16 * (256 + 2 * align_of::<u8>())))
        .ok_or(BudgetUnavailable)?;
    let bytes = payload_bytes
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(tree))
        .and_then(|bytes| bytes.checked_add(vectors))
        .and_then(|bytes| bytes.checked_add(buffers))
        .and_then(|bytes| bytes.checked_add(256 * 1024))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .and_then(|bytes| {
            bytes.checked_add(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        })
        .ok_or(BudgetUnavailable)?;
    Ok(bytes)
}
riffdb_types::inspect_owned_capacity_struct_v1!(AuthoritativeMutationV3 {
    owned: [key, value],
    inline: [namespace, expected_hash]
});
