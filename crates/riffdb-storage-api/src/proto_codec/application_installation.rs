//! Durable codec for opaque exact application-installation campaign state.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationInstallationPlanHash, ContractLineage,
};

use crate::{EncodedPageItem, StoredApplicationInstallationCampaignV1};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed,
    storage_result,
};

const APPLICATION_INSTALLATION_CAMPAIGN: &str =
    "riffdb.storage.v1.StoredApplicationInstallationCampaignV1";

/// Encodes one exact opaque installation campaign state.
pub fn encode_application_installation_campaign_v1(
    value: &StoredApplicationInstallationCampaignV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        APPLICATION_INSTALLATION_CAMPAIGN,
        &wire::StoredApplicationInstallationCampaignV1 {
            campaign_id: value.campaign_id().as_bytes().to_vec(),
            contract_lineage: value.contract_lineage().as_str().to_owned(),
            plan_hash: value.plan_hash().as_bytes().to_vec(),
            canonical_state: value.canonical_state().to_vec(),
        },
    )
}

/// Decodes one exact opaque installation campaign state.
pub fn decode_application_installation_campaign_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredApplicationInstallationCampaignV1>, DurableCodecError> {
    decode_message::<wire::StoredApplicationInstallationCampaignV1, _, _>(
        APPLICATION_INSTALLATION_CAMPAIGN,
        encoded,
        |value| {
            storage_result(StoredApplicationInstallationCampaignV1::new(
                ApplicationInstallationCampaignId::from_bytes(fixed(value.campaign_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                ContractLineage::new(value.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                ApplicationInstallationPlanHash::from_bytes(fixed(value.plan_hash)?),
                value.canonical_state,
            ))
        },
    )
}
