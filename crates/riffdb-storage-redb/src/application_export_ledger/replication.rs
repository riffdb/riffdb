//! Independently proves reciprocal head/member transitions before follower apply.

use std::collections::BTreeSet;

use redb::{ReadableTable, TableHandle, WriteTransaction};
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3};

use super::*;

pub(crate) fn validate_received(
    transaction: &WriteTransaction,
    tables: &BTreeSet<&'static str>,
    receipt: &AuthoritativeTransactionV3,
) -> Result<(), StorageError> {
    let mut head_mutation = None;
    let mut page_mutation = None;
    for mutation in receipt.mutations() {
        let slot = match mutation.namespace() {
            N::ApplicationExportOperations => &mut head_mutation,
            N::ApplicationExportPageCommitments => &mut page_mutation,
            _ => continue,
        };
        // The current closed export owner advances one operation/page per
        // transaction. No cleanup owner admits deletion of retained exports.
        if slot.replace(mutation).is_some() || mutation.value().is_none() {
            return Err(corrupt());
        }
    }
    if head_mutation.is_none() && page_mutation.is_none() {
        return Ok(());
    }
    if receipt.attribution() != ChangelogAttributionV3::ApplicationExportOperation
        || !tables.contains(HEADS.name())
        || !tables.contains(PAGES.name())
    {
        return Err(corrupt());
    }
    let mutation = head_mutation.ok_or_else(corrupt)?;
    let value = mutation.value().ok_or_else(corrupt)?;
    let next = decode_head(mutation.key(), value)?;
    let heads = transaction.open_table(HEADS).map_err(table_error)?;
    let prior = heads
        .get(mutation.key())
        .map_err(precommit_storage_error)?
        .map(|value| decode_head(mutation.key(), value.value()))
        .transpose()?;
    let Head::Compact(next) = next else {
        if page_mutation.is_some() || matches!(prior, Some(Head::Compact(_))) {
            return Err(corrupt());
        }
        return Ok(());
    };
    let prior = match &prior {
        Some(Head::Compact(prior)) => Some(prior),
        None => None,
        Some(Head::Legacy(_)) => return Err(corrupt()),
    };
    let append = page_mutation
        .map(|mutation| {
            if mutation.expected_hash().is_some() {
                return Err(corrupt());
            }
            let value = mutation.value().ok_or_else(corrupt)?;
            let entry = decode_application_export_page_commitment_v1(value)
                .map_err(codec_error)?
                .into_parts()
                .0;
            entry.validate_key(mutation.key()).map_err(|_| corrupt())?;
            Ok((entry, mutation.key().len() + value.len()))
        })
        .transpose()?;
    next.validate_transition(
        prior,
        append.as_ref().map(|(entry, charge)| (entry, *charge)),
        mutation.key().len() + value.len(),
        0, // The source reserves terminal bytes; replay verifies retained bytes.
    )
    .map_err(|_| corrupt())?;
    if prior.is_none() {
        let pages = transaction.open_table(PAGES).map_err(table_error)?;
        require_empty(next.operation_id(), &pages)?;
    }
    Ok(())
}
