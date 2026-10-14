//! V3 receipt formation from validated journal mutations, never latest-row reads.

use std::collections::BTreeMap;

use riffdb_storage_api::proto_codec::{
    decode_changelog_transaction_allocator_v3, encode_changelog_transaction_allocator_v3,
};
use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1,
    AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogTransactionAllocator, ChangelogTransactionSequence,
    ChangelogV3Error, CompositeMutationV1, CompositeTableV1, MAX_CHANGELOG_FRAME_BYTES,
    MAX_CHANGELOG_FRAME_ENTRIES, ReplicationAuthorityClassV1,
};
use riffdb_types::DualFrontier;
use sha2::{Digest, Sha256};

use crate::journal::{JournalFrame, JournalFrameKind, JournalMutation, JournalTable};

const RECEIPT_MAGIC: &[u8; 8] = b"RDBCTX03";
const RECEIPT_HEADER_BYTES: usize = 128;
const RECEIPT_CHECKSUM_BYTES: usize = 32;
const DELETE_VALUE_LENGTH: u32 = u32::MAX;
const ORIGINAL_SOURCE_COUNT: usize = 33;
const SOURCE_COUNT: usize = 34;
const FRAME_FOOTER_BYTES: usize = 48;
const MAX_RECEIPT_BYTES: usize =
    MAX_CHANGELOG_FRAME_BYTES - (166 + 4 * ORIGINAL_SOURCE_COUNT + FRAME_FOOTER_BYTES) - 4;

fn catalog_max_receipt_bytes(
    catalog: [u8; 32],
    attribution: ChangelogAttributionV3,
) -> Result<usize, ChangelogV3Error> {
    let count = if catalog == AuthoritativeStateCatalogV1.digest() {
        ORIGINAL_SOURCE_COUNT
    } else if catalog == AuthoritativeStateCatalogV2.digest() {
        SOURCE_COUNT
    } else {
        return Err(ChangelogV3Error::InvalidEncoding);
    };
    if attribution as usize > count {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    Ok(MAX_CHANGELOG_FRAME_BYTES - (166 + 4 * count + FRAME_FOOTER_BYTES) - 4)
}

const fn is_audit_fenced_namespace(namespace: AuthoritativeNamespaceV1) -> bool {
    matches!(
        namespace,
        AuthoritativeNamespaceV1::Audit
            | AuthoritativeNamespaceV1::AuditByRequest
            | AuthoritativeNamespaceV1::AuditByRequestLocators
            | AuthoritativeNamespaceV1::NextAdministrationSequence
    )
}

fn journal_table_from_composite(table: CompositeTableV1) -> Result<JournalTable, ChangelogV3Error> {
    // Invert the existing exhaustive journal-to-composite mapping instead
    // of maintaining a second physical table inventory.
    JournalTable::ALL
        .into_iter()
        .find(|candidate| candidate.composite() == table)
        .ok_or(ChangelogV3Error::InvalidNamespace)
}

/// Constant-size summary of a validated net receipt, computed over borrowed
/// mutations and a streaming SHA-256 digest without allocating `Box<[u8]>`
/// key/value copies or an encoded `Vec<u8>` receipt buffer.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ValidatedReceiptSummaryV3 {
    binding: AuthoritativeTransactionBindingV3,
    attribution: ChangelogAttributionV3,
    history_hash: [u8; 32],
    encoded_len: usize,
    is_empty: bool,
    all_audit_fenced_namespaces: bool,
}

impl ValidatedReceiptSummaryV3 {
    pub(crate) fn from_decoded_receipt_and_bytes(
        receipt: &AuthoritativeTransactionV3,
        verified_encoded_bytes: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        let history_hash = verified_encoded_bytes
            .len()
            .checked_sub(RECEIPT_CHECKSUM_BYTES)
            .and_then(|start| verified_encoded_bytes.get(start..))
            .and_then(|tail| <[u8; 32]>::try_from(tail).ok())
            .ok_or(ChangelogV3Error::InvalidEncoding)?;
        Ok(Self {
            binding: receipt.binding(),
            attribution: receipt.attribution(),
            history_hash,
            encoded_len: verified_encoded_bytes.len(),
            is_empty: receipt.mutations().is_empty(),
            all_audit_fenced_namespaces: receipt
                .mutations()
                .iter()
                .all(|m| is_audit_fenced_namespace(m.namespace())),
        })
    }

    pub(crate) fn from_receipt(
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, ChangelogV3Error> {
        let encoded_len = receipt.encoded_len()?;
        let history_hash = streaming_receipt_history_hash(receipt)?;
        Ok(Self {
            binding: receipt.binding(),
            attribution: receipt.attribution(),
            history_hash,
            encoded_len,
            is_empty: receipt.mutations().is_empty(),
            all_audit_fenced_namespaces: receipt
                .mutations()
                .iter()
                .all(|m| is_audit_fenced_namespace(m.namespace())),
        })
    }

    #[must_use]
    pub(crate) const fn binding(self) -> AuthoritativeTransactionBindingV3 {
        self.binding
    }

    #[must_use]
    pub(crate) const fn attribution(self) -> ChangelogAttributionV3 {
        self.attribution
    }

    #[must_use]
    pub(crate) const fn history_hash(self) -> [u8; 32] {
        self.history_hash
    }

    #[must_use]
    pub(crate) const fn encoded_len(self) -> usize {
        self.encoded_len
    }

    #[must_use]
    pub(crate) const fn is_empty(self) -> bool {
        self.is_empty
    }

    #[must_use]
    pub(crate) const fn all_audit_fenced_namespaces(self) -> bool {
        self.all_audit_fenced_namespaces
    }

    #[must_use]
    pub(crate) const fn point(self) -> ChangelogHistoryPointV3 {
        ChangelogHistoryPointV3::new(
            self.binding.sequence,
            self.history_hash,
            self.binding.covered_frontier,
        )
    }

    pub(crate) fn validate_terminal_receipt(
        self,
        history: ChangelogHistoryStateV3,
    ) -> Result<(), ChangelogV3Error> {
        let max_bytes =
            catalog_max_receipt_bytes(history.lineage().catalog_digest(), self.attribution)?;
        if self.encoded_len > max_bytes {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if self.binding.database_id != history.lineage().database_id()
            || self.binding.history_incarnation != history.lineage().history_incarnation()
            || self.point() != history.tail()
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(())
    }

    pub(crate) fn advance_history(
        self,
        history: ChangelogHistoryStateV3,
    ) -> Result<ChangelogHistoryStateV3, ChangelogV3Error> {
        let max_bytes =
            catalog_max_receipt_bytes(history.lineage().catalog_digest(), self.attribution)?;
        if self.encoded_len > max_bytes {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if self.binding.database_id != history.lineage().database_id()
            || self.binding.history_incarnation != history.lineage().history_incarnation()
            || self.binding.predecessor != Some(history.tail().sequence())
            || self.binding.predecessor_frontier != history.tail().frontier()
            || self.binding.prior_history_hash != history.tail().history_hash()
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        ChangelogHistoryStateV3::new(
            history.lineage(),
            history.anchor(),
            self.point(),
            history.minimum_resume(),
        )
    }
}

/// Computes `receipt.history_hash()` by streaming the canonical header and
/// mutations directly into SHA-256 without allocating an encoded `Vec<u8>`.
pub(crate) fn streaming_receipt_history_hash(
    receipt: &AuthoritativeTransactionV3,
) -> Result<[u8; 32], ChangelogV3Error> {
    let total = receipt.encoded_len()?;
    if total > MAX_RECEIPT_BYTES {
        return Err(ChangelogV3Error::LimitExceeded);
    }
    let mut hasher = Sha256::new();
    update_receipt_header(
        &mut hasher,
        receipt.binding(),
        receipt.attribution(),
        receipt.mutations().len(),
        total,
    )?;
    for mutation in receipt.mutations() {
        update_receipt_mutation(
            &mut hasher,
            mutation.namespace(),
            mutation.key(),
            mutation.expected_hash(),
            mutation.value(),
        )?;
    }
    Ok(hasher.finalize().into())
}

fn update_receipt_header(
    hasher: &mut Sha256,
    binding: AuthoritativeTransactionBindingV3,
    attribution: ChangelogAttributionV3,
    mutation_count: usize,
    total_encoded_bytes: usize,
) -> Result<(), ChangelogV3Error> {
    let count = u32::try_from(mutation_count)
        .map_err(|_| ChangelogV3Error::LimitExceeded)?
        .to_be_bytes();
    let payload_bytes = total_encoded_bytes - RECEIPT_HEADER_BYTES - RECEIPT_CHECKSUM_BYTES;
    let payload = u32::try_from(payload_bytes)
        .map_err(|_| ChangelogV3Error::LimitExceeded)?
        .to_be_bytes();
    let mut header = [0_u8; RECEIPT_HEADER_BYTES];
    header[0..8].copy_from_slice(RECEIPT_MAGIC);
    header[8..10].copy_from_slice(&3_u16.to_be_bytes());
    header[10..26].copy_from_slice(binding.database_id.as_bytes());
    header[26..34].copy_from_slice(&binding.history_incarnation.to_be_bytes());
    header[34..42].copy_from_slice(&binding.predecessor.map_or(0, |v| v.get()).to_be_bytes());
    header[42..50].copy_from_slice(&binding.sequence.get().to_be_bytes());
    header[50..68].copy_from_slice(&binding.predecessor_frontier.to_canonical_bytes());
    header[68..86].copy_from_slice(&binding.covered_frontier.to_canonical_bytes());
    header[86..88].copy_from_slice(&(attribution as u16).to_be_bytes());
    header[88..120].copy_from_slice(&binding.prior_history_hash);
    header[120..124].copy_from_slice(&count);
    header[124..128].copy_from_slice(&payload);
    hasher.update(header);
    Ok(())
}

fn update_receipt_mutation(
    hasher: &mut Sha256,
    namespace: AuthoritativeNamespaceV1,
    key: &[u8],
    expected_hash: Option<[u8; 32]>,
    value: Option<&[u8]>,
) -> Result<(), ChangelogV3Error> {
    let key_len = u32::try_from(key.len())
        .map_err(|_| ChangelogV3Error::LimitExceeded)?
        .to_be_bytes();
    let value_len = value
        .map(|bytes| u32::try_from(bytes.len()))
        .transpose()
        .map_err(|_| ChangelogV3Error::LimitExceeded)?
        .unwrap_or(DELETE_VALUE_LENGTH)
        .to_be_bytes();
    let mut header = [0_u8; 44];
    header[0..2].copy_from_slice(&namespace.tag().to_be_bytes());
    header[2] = u8::from(value.is_none());
    header[3] = u8::from(expected_hash.is_some());
    header[4..8].copy_from_slice(&key_len);
    header[8..12].copy_from_slice(&value_len);
    if let Some(hash) = expected_hash {
        header[12..44].copy_from_slice(&hash);
    }
    hasher.update(header);
    hasher.update(key);
    if let Some(value) = value {
        hasher.update(value);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum BorrowedCurrentState {
    Absent,
    Hashed([u8; 32]),
    Unhashed,
}

struct BorrowedNetTransition<'a> {
    expected_hash: Option<[u8; 32]>,
    current: BorrowedCurrentState,
    restored: bool,
    value: Option<&'a [u8]>,
}

impl BorrowedNetTransition<'_> {
    fn current_hash(&self) -> Result<Option<[u8; 32]>, ChangelogV3Error> {
        match (self.current, self.value) {
            (BorrowedCurrentState::Absent, _) => Ok(None),
            (BorrowedCurrentState::Hashed(hash), _) => Ok(Some(hash)),
            (BorrowedCurrentState::Unhashed, Some(value)) => Ok(Some(Sha256::digest(value).into())),
            (BorrowedCurrentState::Unhashed, None) => Err(ChangelogV3Error::PredecessorMismatch),
        }
    }
}

struct BorrowedMutationAccumulatorV3<'a> {
    changes: BTreeMap<(AuthoritativeNamespaceV1, &'a [u8]), BorrowedNetTransition<'a>>,
    encoded_bytes: usize,
    failure: Option<ChangelogV3Error>,
}

impl Default for BorrowedMutationAccumulatorV3<'_> {
    fn default() -> Self {
        Self {
            changes: BTreeMap::new(),
            encoded_bytes: 160,
            failure: None,
        }
    }
}

impl<'a> BorrowedMutationAccumulatorV3<'a> {
    fn record(
        &mut self,
        namespace: AuthoritativeNamespaceV1,
        key: &'a [u8],
        expected_hash: Option<[u8; 32]>,
        value: Option<&'a [u8]>,
    ) -> Result<(), ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.record_inner(namespace, key, expected_hash, value);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    fn record_inner(
        &mut self,
        namespace: AuthoritativeNamespaceV1,
        key: &'a [u8],
        expected_hash: Option<[u8; 32]>,
        value: Option<&'a [u8]>,
    ) -> Result<(), ChangelogV3Error> {
        if value.is_none() && expected_hash.is_none() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
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
        let value_len = value.map_or(0, <[u8]>::len);
        let mutation_bytes = 44_usize
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value_len))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if mutation_bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
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
        let current = match (value, original) {
            (None, _) => BorrowedCurrentState::Absent,
            (Some(bytes), Some(_)) => BorrowedCurrentState::Hashed(Sha256::digest(bytes).into()),
            (Some(_), None) => BorrowedCurrentState::Unhashed,
        };
        let is_restoration = match (original, current) {
            (None, BorrowedCurrentState::Absent) => true,
            (Some(original), BorrowedCurrentState::Hashed(hash)) => original == hash,
            _ => false,
        };
        let old_bytes = previous.map_or(0, |previous| {
            44 + key.len() + previous.value.map_or(0, <[u8]>::len)
        });
        let value = if is_restoration { None } else { value };
        let next_bytes = 44 + key.len() + value.map_or(0, <[u8]>::len);
        let encoded_bytes = self
            .encoded_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(next_bytes))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if encoded_bytes > MAX_RECEIPT_BYTES
            || (previous.is_none() && self.changes.len() >= MAX_CHANGELOG_FRAME_ENTRIES)
        {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        self.changes.insert(
            target,
            BorrowedNetTransition {
                expected_hash: original,
                current,
                restored: is_restoration,
                value,
            },
        );
        self.encoded_bytes = encoded_bytes;
        Ok(())
    }

    fn into_summary(
        self,
        binding: AuthoritativeTransactionBindingV3,
        source: ChangelogAttributionV3,
        catalog: [u8; 32],
    ) -> Result<ValidatedReceiptSummaryV3, ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let mut net_count = 0_usize;
        let mut encoded_len = 160_usize;
        let mut all_audit_fenced_namespaces = true;
        for ((namespace, key), transition) in &self.changes {
            if transition.restored {
                continue;
            }
            net_count += 1;
            all_audit_fenced_namespaces &= is_audit_fenced_namespace(*namespace);
            let mut_bytes = 44_usize
                .checked_add(key.len())
                .and_then(|n| n.checked_add(transition.value.map_or(0, <[u8]>::len)))
                .ok_or(ChangelogV3Error::LimitExceeded)?;
            encoded_len = encoded_len
                .checked_add(mut_bytes)
                .ok_or(ChangelogV3Error::LimitExceeded)?;
        }
        if net_count > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        // Delegate binding, attribution, frontier, and catalog validation to the
        // authoritative constructor without cloning the mutation payloads.
        let probe = if net_count == 0 {
            Vec::new()
        } else {
            vec![AuthoritativeMutationV3::delete(
                AuthoritativeNamespaceV1::Entities,
                &[0],
                [0; 32],
            )?]
        };
        AuthoritativeTransactionV3::new_for_catalog(binding, source, probe, catalog)?;
        let max_bytes = catalog_max_receipt_bytes(catalog, source)?;
        if encoded_len > MAX_RECEIPT_BYTES || encoded_len > max_bytes {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let mut hasher = Sha256::new();
        update_receipt_header(&mut hasher, binding, source, net_count, encoded_len)?;
        for ((namespace, key), transition) in &self.changes {
            if transition.restored {
                continue;
            }
            update_receipt_mutation(
                &mut hasher,
                *namespace,
                key,
                transition.expected_hash,
                transition.value,
            )?;
        }
        Ok(ValidatedReceiptSummaryV3 {
            binding,
            attribution: source,
            history_hash: hasher.finalize().into(),
            encoded_len,
            is_empty: net_count == 0,
            all_audit_fenced_namespaces,
        })
    }
}

/// Checks the one-step physical allocation carried by a journal mutation.
/// This is shape/source validation, not permission to apply it: replay still
/// requires the retained, fully validated V3 activation roots and predecessor.
/// Position one is reserved for Immediate activation and cannot be journaled.
pub(crate) fn journal_allocator_assignment(
    mutation: &JournalMutation,
) -> Result<ChangelogTransactionSequence, ChangelogV3Error> {
    allocator_assignment(
        mutation.table(),
        mutation.key(),
        mutation.expected_hash(),
        mutation.value(),
    )
}

fn allocator_assignment(
    table: JournalTable,
    key: &[u8],
    expected: Option<[u8; 32]>,
    value: Option<&[u8]>,
) -> Result<ChangelogTransactionSequence, ChangelogV3Error> {
    let (Some(expected), Some(value)) = (expected, value) else {
        return Err(ChangelogV3Error::InvalidEncoding);
    };
    if table != JournalTable::Meta
        || AuthoritativeStateCatalogV1.lookup(table.label(), key)
            != Some(AuthoritativeNamespaceV1::NextChangelogTransaction)
    {
        return Err(ChangelogV3Error::InvalidNamespace);
    }
    let decoded = decode_changelog_transaction_allocator_v3(value)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let assigned = match *decoded.value() {
        ChangelogTransactionAllocator::Next(next) => next
            .get()
            .checked_sub(1)
            .and_then(ChangelogTransactionSequence::new)
            .ok_or(ChangelogV3Error::PredecessorMismatch)?,
        ChangelogTransactionAllocator::Exhausted => ChangelogTransactionSequence::new(u64::MAX)
            .ok_or(ChangelogV3Error::SequenceExhausted)?,
    };
    if assigned.get() == 1 {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let before = ChangelogTransactionAllocator::Next(assigned);
    let after = encode_changelog_transaction_allocator_v3(before.allocate_one()?.1)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let before = encode_changelog_transaction_allocator_v3(before)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let expected_hash: [u8; 32] = Sha256::digest(before.as_bytes()).into();
    if expected != expected_hash || value != after.as_bytes() {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    Ok(assigned)
}

/// Forms one net receipt from the exact admitted/recovered frame. The caller
/// binds its predecessor to the known-durable activation/checkpoint or prior
/// validated suffix receipt, and owns the durable-record semantic validation.
/// No source may be synthesized for a pre-activation frame without its allocator.
pub(crate) fn receipt_from_journal_for_catalog(
    frame: &JournalFrame,
    binding: AuthoritativeTransactionBindingV3,
    catalog: [u8; 32],
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || binding.database_id != frame.database_id()
        || binding.predecessor_frontier
            != DualFrontier::new(
                frame.predecessor_sequence(),
                frame.predecessor_administration_sequence(),
            )
        || binding.covered_frontier
            != DualFrontier::new(
                frame.covered_sequence(),
                frame.covered_administration_sequence(),
            )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let source = match frame.kind() {
        JournalFrameKind::Command => ChangelogAttributionV3::JournaledApplicationGroup,
        JournalFrameKind::ServiceAudit => ChangelogAttributionV3::JournaledServiceAudit,
        JournalFrameKind::RetentionPrune => ChangelogAttributionV3::RetentionPrune,
        JournalFrameKind::SourceReclamation => ChangelogAttributionV3::HistoryReclamation,
    };
    receipt_from_sources(binding, source, catalog, frame.mutations(), |mutation| {
        Ok((
            mutation.table(),
            mutation.key(),
            mutation.expected_hash(),
            mutation.value(),
        ))
    })
}

/// Same receipt fold for the live checkpoint's retained validated mutations.
/// It borrows the original source, not latest overlay rows, and does not decode
/// a journal frame or duplicate its mutation payloads before coalescing.
pub(crate) fn receipt_from_validated_mutations_for_catalog(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: &[CompositeMutationV1],
    catalog: [u8; 32],
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    receipt_from_sources(binding, source, catalog, mutations, |mutation| {
        let table = JournalTable::ALL
            .into_iter()
            .find(|table| table.composite() == mutation.table())
            .ok_or(ChangelogV3Error::InvalidNamespace)?;
        Ok((
            table,
            mutation.key(),
            mutation.expected_hash(),
            mutation.value(),
        ))
    })
}

type MutationSource<'a> = (JournalTable, &'a [u8], Option<[u8; 32]>, Option<&'a [u8]>);

fn receipt_from_sources<'a, T>(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    catalog: [u8; 32],
    mutations: &'a [T],
    get: impl Fn(&'a T) -> Result<MutationSource<'a>, ChangelogV3Error>,
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || !matches!(
            source,
            ChangelogAttributionV3::JournaledApplicationGroup
                | ChangelogAttributionV3::JournaledServiceAudit
                | ChangelogAttributionV3::RetentionPrune
                | ChangelogAttributionV3::HistoryReclamation
        )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let mut allocator_seen = false;
    for mutation in mutations {
        let (table, key, expected_hash, value) = get(mutation)?;
        let namespace = AuthoritativeStateCatalogV1
            .lookup(table.label(), key)
            .ok_or(ChangelogV3Error::InvalidNamespace)?;
        if namespace == AuthoritativeNamespaceV1::NextChangelogTransaction {
            if allocator_seen {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            allocator_seen = true;
            if allocator_assignment(table, key, expected_hash, value)? != binding.sequence {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            continue;
        }
        if source == ChangelogAttributionV3::HistoryReclamation {
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative {
            // The closed journal table set has no derived projection payloads.
            // Its only V3 control mutation is the checked allocator above.
            return Err(ChangelogV3Error::InvalidNamespace);
        }
    }
    if !allocator_seen {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    let changes =
        AuthoritativeMutationAccumulatorV3::fold_borrowed_indexed_v1(mutations.len(), |index| {
            let (table, key, expected, value) = get(&mutations[index])?;
            let namespace = AuthoritativeStateCatalogV1
                .lookup(table.label(), key)
                .ok_or(ChangelogV3Error::InvalidNamespace)?;
            Ok(
                (namespace != AuthoritativeNamespaceV1::NextChangelogTransaction)
                    .then_some((namespace, key, expected, value)),
            )
        })?;
    AuthoritativeTransactionV3::new_for_catalog(binding, source, changes, catalog)
}

fn check_journal_frame_binding(
    frame: &JournalFrame,
    binding: AuthoritativeTransactionBindingV3,
) -> Result<ChangelogAttributionV3, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || binding.database_id != frame.database_id()
        || binding.predecessor_frontier
            != DualFrontier::new(
                frame.predecessor_sequence(),
                frame.predecessor_administration_sequence(),
            )
        || binding.covered_frontier
            != DualFrontier::new(
                frame.covered_sequence(),
                frame.covered_administration_sequence(),
            )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    Ok(match frame.kind() {
        JournalFrameKind::Command => ChangelogAttributionV3::JournaledApplicationGroup,
        JournalFrameKind::ServiceAudit => ChangelogAttributionV3::JournaledServiceAudit,
        JournalFrameKind::RetentionPrune => ChangelogAttributionV3::RetentionPrune,
        JournalFrameKind::SourceReclamation => ChangelogAttributionV3::HistoryReclamation,
    })
}

pub(crate) fn summary_from_journal_for_catalog(
    frame: &JournalFrame,
    binding: AuthoritativeTransactionBindingV3,
    catalog: [u8; 32],
) -> Result<ValidatedReceiptSummaryV3, ChangelogV3Error> {
    let source = check_journal_frame_binding(frame, binding)?;
    fold_borrowed_sources(
        binding,
        source,
        frame.mutations().iter().map(|mutation| {
            Ok((
                mutation.table(),
                mutation.key(),
                mutation.expected_hash(),
                mutation.value(),
            ))
        }),
    )?
    .into_summary(binding, source, catalog)
}

pub(crate) fn summary_from_validated_mutations_for_catalog(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: &[CompositeMutationV1],
    catalog: [u8; 32],
) -> Result<ValidatedReceiptSummaryV3, ChangelogV3Error> {
    fold_borrowed_sources(
        binding,
        source,
        mutations.iter().map(|mutation| {
            Ok((
                journal_table_from_composite(mutation.table())?,
                mutation.key(),
                mutation.expected_hash(),
                mutation.value(),
            ))
        }),
    )?
    .into_summary(binding, source, catalog)
}

fn fold_borrowed_sources<'a>(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: impl IntoIterator<Item = Result<MutationSource<'a>, ChangelogV3Error>>,
) -> Result<BorrowedMutationAccumulatorV3<'a>, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || !matches!(
            source,
            ChangelogAttributionV3::JournaledApplicationGroup
                | ChangelogAttributionV3::JournaledServiceAudit
                | ChangelogAttributionV3::RetentionPrune
                | ChangelogAttributionV3::HistoryReclamation
        )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let mut allocator_seen = false;
    let mut changes = BorrowedMutationAccumulatorV3::default();
    for mutation in mutations {
        let (table, key, expected_hash, value) = mutation?;
        let namespace = AuthoritativeStateCatalogV1
            .lookup(table.label(), key)
            .ok_or(ChangelogV3Error::InvalidNamespace)?;
        if namespace == AuthoritativeNamespaceV1::NextChangelogTransaction {
            if allocator_seen {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            allocator_seen = true;
            if allocator_assignment(table, key, expected_hash, value)? != binding.sequence {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            continue;
        }
        if source == ChangelogAttributionV3::HistoryReclamation {
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative {
            // The closed journal table set has no derived projection payloads.
            // Its only V3 control mutation is the checked allocator above.
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        changes.record(namespace, key, expected_hash, value)?;
    }
    if !allocator_seen {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    Ok(changes)
}

// Frozen V1 fixture entry points; production always passes its checked lineage.
#[cfg(test)]
pub(crate) fn receipt_from_journal(
    frame: &JournalFrame,
    binding: AuthoritativeTransactionBindingV3,
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    receipt_from_journal_for_catalog(frame, binding, AuthoritativeStateCatalogV1.digest())
}

#[cfg(test)]
pub(crate) fn receipt_from_validated_mutations(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: &[CompositeMutationV1],
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    receipt_from_validated_mutations_for_catalog(
        binding,
        source,
        mutations,
        AuthoritativeStateCatalogV1.digest(),
    )
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3;
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
        ChangelogTransactionAllocator, ChangelogTransactionSequence, ChangelogV3Error,
    };
    use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

    use crate::journal::{JournalFrame, JournalMutation, JournalTable};

    fn binding() -> AuthoritativeTransactionBindingV3 {
        AuthoritativeTransactionBindingV3 {
            database_id: DatabaseId::from_unix_milliseconds_and_random(
                1_700_000_000_000,
                [0x71; 10],
            )
            .unwrap(),
            history_incarnation: 1,
            predecessor: ChangelogTransactionSequence::new(1),
            sequence: ChangelogTransactionSequence::new(2).unwrap(),
            predecessor_frontier: DualFrontier::INITIAL,
            covered_frontier: DualFrontier::new(CommitSequence::new(1), None),
            prior_history_hash: [0x71; 32],
        }
    }

    fn allocator_mutation() -> JournalMutation {
        let before = ChangelogTransactionAllocator::Next(binding().sequence);
        let after = before.allocate_one().unwrap().1;
        JournalMutation::replace(
            JournalTable::Meta,
            N::NextChangelogTransaction
                .metadata_key()
                .unwrap()
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(before)
                .unwrap()
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(after)
                .unwrap()
                .into_bytes(),
        )
        .unwrap()
    }

    fn frame(extra: Vec<JournalMutation>) -> JournalFrame {
        let mut mutations = vec![
            JournalMutation::put(
                JournalTable::Commits,
                vec![1],
                b"opaque-codec-vector".to_vec(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Entities,
                b"entity".to_vec(),
                b"original",
                b"overwritten".to_vec(),
            )
            .unwrap(),
            JournalMutation::delete_matching(
                JournalTable::Entities,
                b"entity".to_vec(),
                b"overwritten",
            )
            .unwrap(),
        ];
        mutations.extend(extra);
        JournalFrame::command(
            binding().database_id,
            None,
            CommitSequence::new(1),
            None,
            None,
            1,
            [0x44; 32],
            mutations,
        )
        .unwrap()
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_receipt_formation_preserves_original_delete_precondition_without_row_reads() {
        let source = frame(vec![allocator_mutation()]);
        let encoded = source.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let receipt = super::receipt_from_journal(&decoded, binding()).unwrap();
        assert_eq!(receipt.binding(), binding());
        assert_eq!(receipt.mutations().len(), 2);
        let entity = &receipt.mutations()[0];
        assert_eq!(entity.namespace(), N::Entities);
        assert_eq!(entity.value(), None);
        assert!(entity.matches_prior(Some(b"original")));
        assert!(!entity.matches_prior(Some(b"overwritten")));
        assert_eq!(
            receipt.encode().unwrap(),
            super::receipt_from_journal(&source, binding())
                .unwrap()
                .encode()
                .unwrap()
        );
        assert_eq!(source.encode().unwrap().as_bytes(), encoded.as_bytes());
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn retained_checkpoint_mutations_form_the_exact_recovered_receipt() {
        let source = frame(vec![allocator_mutation()]);
        let encoded = source.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let retained: Vec<_> = source
            .mutations()
            .iter()
            .map(|mutation| mutation.composite().unwrap())
            .collect();
        let receipt = super::receipt_from_validated_mutations(
            binding(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
            &retained,
        )
        .unwrap();
        assert_eq!(
            receipt.encode().unwrap(),
            super::receipt_from_journal(&decoded, binding())
                .unwrap()
                .encode()
                .unwrap()
        );
        let summary = super::summary_from_validated_mutations_for_catalog(
            binding(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
            &retained,
            riffdb_storage_api::AuthoritativeStateCatalogV1.digest(),
        )
        .unwrap();
        assert_eq!(summary.history_hash(), receipt.history_hash().unwrap());
        assert_eq!(summary.encoded_len(), receipt.encode().unwrap().len());
        assert!(receipt.mutations()[0].matches_prior(Some(b"original")));
        assert_eq!(receipt.mutations()[0].value(), None);
        for attribution in [
            riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
            riffdb_storage_api::ChangelogAttributionV3::V3Activation,
        ] {
            assert!(
                super::receipt_from_validated_mutations(binding(), attribution, &retained).is_err()
            );
        }
        let mut missing = retained.clone();
        missing.pop();
        assert!(
            super::receipt_from_validated_mutations(
                binding(),
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &missing,
            )
            .is_err()
        );
        let mut duplicate = retained.clone();
        duplicate.push(allocator_mutation().composite().unwrap());
        assert!(
            super::receipt_from_validated_mutations(
                binding(),
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &duplicate,
            )
            .is_err()
        );
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn standalone_audit_receipt_carries_the_checked_physical_allocator_without_application_advance()
    {
        let mut audit_binding = binding();
        audit_binding.covered_frontier =
            riffdb_types::DualFrontier::new(None, riffdb_types::AdministrationSequence::new(1));
        let frame = JournalFrame::service_audit(
            audit_binding.database_id,
            None,
            None,
            riffdb_types::AdministrationSequence::new(1),
            1,
            [0x44; 32],
            vec![
                JournalMutation::put(
                    JournalTable::Audit,
                    vec![1],
                    b"opaque-audit-codec-vector".to_vec(),
                )
                .unwrap(),
                allocator_mutation(),
            ],
        )
        .expect("an attributed standalone audit must carry its V3 allocator source");
        let encoded = frame.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let receipt = super::receipt_from_journal(&decoded, audit_binding).unwrap();
        assert_eq!(receipt.binding(), audit_binding);
        assert_eq!(
            receipt.attribution(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit
        );
        assert_eq!(receipt.mutations().len(), 1);
        assert_eq!(receipt.mutations()[0].namespace(), N::Audit);
        assert_eq!(decoded.encode().unwrap().as_bytes(), encoded.as_bytes());
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_allocator_requires_exact_current_envelopes_and_one_non_activation_step() {
        fn advance(before: u64, after: ChangelogTransactionAllocator) -> JournalMutation {
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(ChangelogTransactionAllocator::Next(
                    ChangelogTransactionSequence::new(before).unwrap(),
                ))
                .unwrap()
                .as_bytes(),
                encode_changelog_transaction_allocator_v3(after)
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap()
        }
        for assigned in [2, 42, u64::MAX] {
            let before = ChangelogTransactionAllocator::Next(
                ChangelogTransactionSequence::new(assigned).unwrap(),
            );
            let mutation = advance(assigned, before.allocate_one().unwrap().1);
            assert_eq!(
                super::journal_allocator_assignment(&mutation)
                    .unwrap()
                    .get(),
                assigned
            );
        }
        let next = |value| {
            ChangelogTransactionAllocator::Next(ChangelogTransactionSequence::new(value).unwrap())
        };
        for mutation in [
            advance(1, next(2)), // Activation is Immediate, never a journal source.
            advance(2, next(4)), // Skipped assignment.
            advance(2, next(2)), // No advance.
            advance(2, next(1)), // Rollback.
            advance(2, ChangelogTransactionAllocator::Exhausted),
            JournalMutation::put(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(next(3))
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                b"wrong-preimage",
                encode_changelog_transaction_allocator_v3(next(3))
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            JournalMutation::delete_matching(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                b"old",
            )
            .unwrap(),
        ] {
            assert!(super::journal_allocator_assignment(&mutation).is_err());
            assert!(
                JournalFrame::service_audit(
                    binding().database_id,
                    None,
                    None,
                    riffdb_types::AdministrationSequence::new(1),
                    1,
                    [0; 32],
                    vec![
                        JournalMutation::put(JournalTable::Audit, vec![1], vec![1]).unwrap(),
                        mutation
                    ],
                )
                .is_err()
            );
        }
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_allocator_shape_does_not_authorize_replay_into_inactive_or_partial_v3_state() {
        use redb::ReadableDatabase;
        let scope = crate::test_path::ScopedDirectory::new("v3-inactive-refusal");
        let database = redb::Database::create(scope.join("db.redb")).unwrap();
        let key = N::NextChangelogTransaction.metadata_key().unwrap();
        let before = encode_changelog_transaction_allocator_v3(
            ChangelogTransactionAllocator::Next(binding().sequence),
        )
        .unwrap();
        for installed in [false, true] {
            let transaction = database.begin_write().unwrap();
            {
                let mut meta = transaction.open_table(crate::layout::META).unwrap();
                if installed {
                    meta.insert(key, before.as_bytes()).unwrap();
                }
            }
            transaction.commit().unwrap();
            let transaction = database.begin_write().unwrap();
            assert!(super::journal_allocator_assignment(&allocator_mutation()).is_ok());
            assert!(crate::journal::apply_mutation(&transaction, &allocator_mutation()).is_err());
            // Even if the private caller ignores the refusal and commits, the
            // allocator itself must neither appear nor advance without roots.
            transaction.commit().unwrap();
            let snapshot = database.begin_read().unwrap();
            let meta = snapshot.open_table(crate::layout::META).unwrap();
            let observed = meta.get(key).unwrap();
            assert_eq!(
                observed.as_ref().map(|value| value.value()),
                installed.then_some(before.as_bytes())
            );
        }
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_receipt_refuses_missing_duplicate_and_misattributed_allocator_state() {
        assert_eq!(
            super::receipt_from_journal(&frame(vec![]), binding()),
            Err(ChangelogV3Error::InvalidEncoding)
        );
        assert!(
            super::receipt_from_journal(
                &frame(vec![allocator_mutation(), allocator_mutation()]),
                binding()
            )
            .is_err()
        );
        let source = frame(vec![allocator_mutation()]);
        let mut wrong = binding();
        wrong.covered_frontier = wrong.predecessor_frontier;
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        wrong = binding();
        wrong.sequence = ChangelogTransactionSequence::new(3).unwrap();
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        wrong = binding();
        wrong.predecessor = None;
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        let foreign =
            JournalMutation::put(JournalTable::Meta, b"foreign/v1".to_vec(), vec![1]).unwrap();
        assert!(
            super::receipt_from_journal(&frame(vec![allocator_mutation(), foreign]), binding())
                .is_err()
        );
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn borrowed_receipt_summaries_match_encoded_receipt_hashes_and_refusals() {
        let source = frame(vec![allocator_mutation()]);
        let encoded_frame = source.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded_frame.as_bytes()).unwrap();
        let retained: Vec<_> = source
            .mutations()
            .iter()
            .map(|mutation| mutation.composite().unwrap())
            .collect();
        let catalog = riffdb_storage_api::AuthoritativeStateCatalogV1.digest();
        let receipt =
            super::receipt_from_journal_for_catalog(&decoded, binding(), catalog).unwrap();
        let encoded_receipt = receipt.encode().unwrap();
        let expected_hash = receipt.history_hash().unwrap();
        let expected_point =
            riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(&receipt).unwrap();

        let journal_summary =
            super::summary_from_journal_for_catalog(&decoded, binding(), catalog).unwrap();
        let composite_summary = super::summary_from_validated_mutations_for_catalog(
            binding(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
            &retained,
            catalog,
        )
        .unwrap();
        let direct_summary = super::ValidatedReceiptSummaryV3::from_receipt(&receipt).unwrap();
        let decoded_summary = super::ValidatedReceiptSummaryV3::from_decoded_receipt_and_bytes(
            &receipt,
            &encoded_receipt,
        )
        .unwrap();

        for summary in [
            journal_summary,
            composite_summary,
            direct_summary,
            decoded_summary,
        ] {
            assert_eq!(summary.history_hash(), expected_hash);
            assert_eq!(summary.point(), expected_point);
            assert_eq!(summary.encoded_len(), encoded_receipt.len());
            assert_eq!(summary.binding(), receipt.binding());
            assert_eq!(summary.attribution(), receipt.attribution());
            assert_eq!(summary.is_empty(), receipt.mutations().is_empty());
        }

        assert_eq!(
            super::summary_from_journal_for_catalog(&frame(vec![]), binding(), catalog).err(),
            Some(ChangelogV3Error::InvalidEncoding)
        );
        let mut wrong = binding();
        wrong.sequence = ChangelogTransactionSequence::new(3).unwrap();
        assert_eq!(
            super::summary_from_journal_for_catalog(&source, wrong, catalog).err(),
            super::receipt_from_journal_for_catalog(&source, wrong, catalog).err()
        );
        assert_eq!(
            super::summary_from_validated_mutations_for_catalog(
                wrong,
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &retained,
                catalog,
            )
            .err(),
            super::receipt_from_validated_mutations_for_catalog(
                wrong,
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &retained,
                catalog,
            )
            .err()
        );
    }
}
