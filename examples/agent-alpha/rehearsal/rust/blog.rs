#![forbid(unsafe_code)]

mod generated;

use std::env;
use std::path::Path;

use generated::{
    AgentBlogClient, CreateSiteInput, ModerationQueueParams, ModerationQueueResult,
    PostBySlugParams, PostBySlugResult, PostPageParams, PostPageResult, PublicFeedParams,
    PublicFeedResult,
};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, QueryOptions, StableApplicationClient,
    load_protected_bearer_credential,
};

const SITE_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b20";
const POST_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22";

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::args().nth(1).ok_or("missing RiffDB endpoint")?;
    let credential_path = env::args_os()
        .nth(2)
        .ok_or("missing application credential")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = StableApplicationClient::connect_uri(endpoint).await?;
    let mut application = AgentBlogClient::new(
        client,
        CallMetadata::authenticated(credential),
        AttemptBudget::new(3).ok_or("invalid attempt budget")?,
    );

    let command = application
        .create_site(CreateSiteInput {
            name: "Acme Engineering".to_owned(),
            site_id: SITE_ID.to_owned(),
            idempotency_key: "blog-site-acme".to_owned(),
        })
        .await?;
    let commit_sequence = command
        .commit_sequence
        .ok_or("seed replay did not retain its commit sequence")?;
    if !command.replayed {
        return Err("seed replay unexpectedly created another site".into());
    }
    let options = || QueryOptions::new().read_after_commit(commit_sequence);

    let moderation = application
        .moderation_queue_with_options(
            ModerationQueueParams {
                site_id: SITE_ID.to_owned(),
                status: "Approved".to_owned(),
                after: None,
                limit: 25,
            },
            options(),
        )
        .await?;
    if !matches!(moderation.value, ModerationQueueResult::Found(_)) {
        return Err("moderation queue did not return Found".into());
    }
    let slug = application
        .post_by_slug_with_options(
            PostBySlugParams {
                site_id: SITE_ID.to_owned(),
                slug: "safe-application-data".to_owned(),
            },
            options(),
        )
        .await?;
    if !matches!(slug.value, PostBySlugResult::Found(_)) {
        return Err("slug lookup did not return Found".into());
    }
    let page = application
        .post_page_with_options(
            PostPageParams {
                site_id: SITE_ID.to_owned(),
                post_id: POST_ID.to_owned(),
                comments_after: None,
            },
            options(),
        )
        .await?;
    if !matches!(page.value, PostPageResult::Found(_)) {
        return Err("post page did not return Found".into());
    }
    let feed = application
        .public_feed_with_options(
            PublicFeedParams {
                site_id: SITE_ID.to_owned(),
                status: "Published".to_owned(),
                after: None,
                limit: 20,
            },
            options(),
        )
        .await?;
    if !matches!(feed.value, PublicFeedResult::Found(_)) {
        return Err("public feed did not return Found".into());
    }

    let application_head = [
        moderation.application_head,
        slug.application_head,
        page.application_head,
        feed.application_head,
    ]
    .into_iter()
    .max()
    .ok_or("query evidence is empty")?;
    if application_head < commit_sequence {
        return Err("read-after-commit evidence regressed".into());
    }
    println!(
        "riffdb-rehearsal-v2\tblog\t{commit_sequence}\t{application_head}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        hex(&command.plan_hash),
        page.identity.contract_lineage,
        page.identity.contract_version,
        hex(&page.identity.contract_bundle_hash),
        hex(&page.identity.module_hash),
        page.identity.query_name,
        hex(&page.identity.plan_hash),
    );
    Ok(())
}
