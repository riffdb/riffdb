//! Read-only catalog metadata from the exact startup or received predecessor.
use crate::error::{codec_error, precommit_storage_error, table_error};
use redb::{ReadableTable, TableDefinition};
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, CatalogRepository, StorageError};

#[derive(Clone, Copy)]
pub(super) enum CatalogRead<'a> {
    Retained(&'a redb::ReadTransaction),
    Received(
        &'a redb::WriteTransaction,
        &'a std::collections::BTreeSet<&'static str>,
    ),
}

impl CatalogRead<'_> {
    fn read<T>(
        self,
        namespace: N,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T, StorageError>,
    ) -> Result<Option<T>, StorageError> {
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
        match self {
            Self::Retained(transaction) => decode_row(
                &transaction.open_table(definition).map_err(table_error)?,
                key,
                decode,
            ),
            Self::Received(transaction, tables) => {
                if !tables.contains(namespace.table()) {
                    return Err(super::corrupt());
                }
                decode_row(
                    &transaction.open_table(definition).map_err(table_error)?,
                    key,
                    decode,
                )
            }
        }
    }
}

fn decode_row<T>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<T, StorageError>,
) -> Result<Option<T>, StorageError> {
    table
        .get(key)
        .map_err(precommit_storage_error)?
        .map(|row| decode(row.value()))
        .transpose()
}

impl CatalogRepository for CatalogRead<'_> {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StorageError> {
        self.read(
            N::CatalogActive,
            crate::layout::CATALOG_ACTIVE_KEY.as_slice(),
            |bytes| {
                crate::codec::decode_active_catalog_pointer_v1(bytes)
                    .map(|value| value.into_parts().0)
            },
        )
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<riffdb_storage_api::StoredContractBundleV1>, StorageError> {
        let key = crate::keys::encode_contract_bundle_key(lineage, version)
            .map_err(|_| super::corrupt())?;
        self.read(N::ContractBundles, &key, |bytes| {
            crate::codec::decode_contract_bundle_v1(bytes).map(|value| value.into_parts().0)
        })
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: riffdb_types::ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::StoredContractMigrationEdgeV1>, StorageError> {
        let key = crate::keys::encode_contract_write_retirement_key(predecessor);
        let Some(retirement) = self.read(N::ContractWriteRetirements, &key, |bytes| {
            riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(bytes)
                .map(|value| value.into_parts().0)
                .map_err(codec_error)
        })?
        else {
            return Ok(None);
        };
        if retirement.artifacts().parent() != predecessor {
            return Err(super::corrupt());
        }
        let key = crate::keys::encode_contract_migration_operation_key(retirement.operation_id());
        let migration = self
            .read(N::ContractMigrations, &key, |bytes| {
                riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(bytes)
                    .map(|value| value.into_parts().0)
                    .map_err(codec_error)
            })?
            .ok_or_else(super::corrupt)?;
        riffdb_storage_api::StoredContractMigrationEdgeV1::new(retirement, migration)
            .map(Some)
            .map_err(|_| super::corrupt())
    }
}
