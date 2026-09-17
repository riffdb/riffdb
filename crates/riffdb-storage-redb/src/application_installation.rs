//! Durable exact application-installation campaign repository.

use redb::ReadableTable;
use riffdb_storage_api::{
    ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
    StorageError, StorageErrorKind, StoredApplicationInstallationCampaignV1,
};
use riffdb_types::ApplicationInstallationCampaignId;

use crate::codec::{
    decode_application_installation_campaign_v1, encode_application_installation_campaign_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::layout::APPLICATION_INSTALLATION_CAMPAIGNS;
use crate::store::RedbOperationalPorts;

fn decode_row(
    key: &[u8],
    value: &[u8],
) -> Result<StoredApplicationInstallationCampaignV1, StorageError> {
    let campaign = decode_application_installation_campaign_v1(value)?
        .into_parts()
        .0;
    if key != campaign.campaign_id().as_bytes() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(campaign)
}

impl ApplicationInstallationCampaignRepository for RedbOperationalPorts {
    fn read_application_installation_campaign(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
    ) -> Result<Option<StoredApplicationInstallationCampaignV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
            .map_err(table_error)?;
        let Some(value) = table
            .get(campaign_id.as_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        decode_row(campaign_id.as_bytes(), value.value()).map(Some)
    }

    fn compare_and_swap_application_installation_campaign(
        &mut self,
        expected: Option<&StoredApplicationInstallationCampaignV1>,
        replacement: &StoredApplicationInstallationCampaignV1,
    ) -> Result<ApplicationInstallationCampaignWriteResultV1, StorageError> {
        if expected.is_some_and(|expected| {
            expected.campaign_id() != replacement.campaign_id()
                || expected.contract_lineage() != replacement.contract_lineage()
                || expected.plan_hash() != replacement.plan_hash()
        }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_attributed_write(
            riffdb_storage_api::ChangelogAttributionV3::ApplicationInstallationCampaign,
        )?;
        let mut table = access
            .transaction()?
            .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
            .map_err(table_error)?;
        let campaign_id = replacement.campaign_id();
        let key = campaign_id.as_bytes();
        let current = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_row(key, value.value()))
            .transpose()?;
        if current.as_ref() == Some(replacement) {
            drop(table);
            access.abort()?;
            return Ok(ApplicationInstallationCampaignWriteResultV1::Unchanged);
        }
        if current.as_ref() != expected {
            drop(table);
            access.abort()?;
            return Ok(ApplicationInstallationCampaignWriteResultV1::CompareMismatch);
        }
        let encoded = encode_application_installation_campaign_v1(replacement)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ApplicationInstallationCampaign)?;
        Ok(ApplicationInstallationCampaignWriteResultV1::Applied)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use riffdb_storage_api::{
        ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
        DatabaseInitializationPort, StoredApplicationInstallationCampaignV1,
    };
    use riffdb_types::{
        ApplicationInstallationCampaignId, ApplicationInstallationPlanHash, ContractLineage,
        DatabaseId,
    };

    use crate::store::{RedbDormantPorts, RedbStore};

    /// Whole-directory scope: the database and every side file it grows live
    /// in one [`crate::test_path::ScopedDirectory`] removed on drop — pass,
    /// fail, or panic.
    struct TestPath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new() -> Self {
            let scope = crate::test_path::ScopedDirectory::new("installation");
            Self(scope.join("db.redb"), scope)
        }
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn record(state: &[u8]) -> StoredApplicationInstallationCampaignV1 {
        StoredApplicationInstallationCampaignV1::new(
            ApplicationInstallationCampaignId::from_bytes(uuid(2)).expect("campaign"),
            ContractLineage::new("TicketDesk").expect("lineage"),
            ApplicationInstallationPlanHash::from_bytes([3; 32]),
            state.to_vec(),
        )
        .expect("record")
    }

    // req: OUT-001, OUT-002, TXN-042
    #[test]
    fn campaign_compare_and_swap_is_durable_and_retry_safe() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let dormant = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate");
        let first = record(b"state-one\n");
        let next = record(b"state-two\n");

        assert_eq!(
            ports
                .compare_and_swap_application_installation_campaign(None, &first)
                .expect("insert"),
            ApplicationInstallationCampaignWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .compare_and_swap_application_installation_campaign(None, &first)
                .expect("retry"),
            ApplicationInstallationCampaignWriteResultV1::Unchanged
        );
        assert_eq!(
            ports
                .compare_and_swap_application_installation_campaign(None, &next)
                .expect("mismatch"),
            ApplicationInstallationCampaignWriteResultV1::CompareMismatch
        );
        assert_eq!(
            ports
                .compare_and_swap_application_installation_campaign(Some(&first), &next)
                .expect("advance"),
            ApplicationInstallationCampaignWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .read_application_installation_campaign(next.campaign_id())
                .expect("read"),
            Some(next.clone())
        );
        assert!(
            ports
                .fresh_locator_public_and_private_roles_match_for_test()
                .expect("installation lane preserves both roles")
        );
        drop(ports);

        let mut reopened = RedbStore::open(&path.0).expect("reopen");
        reopened
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("observe initialized database");
        let reopened = RedbDormantPorts {
            pending_v3_activation: None,
            shared: reopened.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("reactivate");
        assert_eq!(
            reopened
                .read_application_installation_campaign(next.campaign_id())
                .expect("read after reopen"),
            Some(next)
        );
    }
}
