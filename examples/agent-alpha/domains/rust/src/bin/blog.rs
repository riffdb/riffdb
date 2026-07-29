#![forbid(unsafe_code)]

use std::env;
use std::path::Path;

use agent_alpha_domains_rust::blog::{AgentBlogClient, PostPageParams, PostPageResult};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, StableApplicationClient, load_protected_bearer_credential,
};

const SITE_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b20";
const POST_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22";

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
    let PostPageResult::Found(page) = application
        .post_page(PostPageParams {
            site_id: SITE_ID.to_owned(),
            post_id: POST_ID.to_owned(),
            comments_after: None,
        })
        .await?
    else {
        return Err("blog page did not return Found".into());
    };
    println!("{}", page.post.title);
    Ok(())
}
