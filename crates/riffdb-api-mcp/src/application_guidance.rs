//! Bounded, injection-safe application guidance rendering shared by transports.

use crate::{McpHandlerContractError, McpMarkdownBuilder, McpMarkdownDocument};

/// Maximum policy-visible items admitted into one guidance document.
pub const MAX_APPLICATION_GUIDANCE_ITEMS: usize = 2_048;

/// Renders one exact authorization-filtered application guide.
pub fn render_application_guidance(
    lineage: &str,
    version: u64,
    bundle_hash: &str,
    tool_names: &[String],
    resource_uris: &[String],
) -> Result<McpMarkdownDocument, McpHandlerContractError> {
    if lineage.is_empty()
        || version == 0
        || bundle_hash.len() != 64
        || !bundle_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || tool_names.len().saturating_add(resource_uris.len()) > MAX_APPLICATION_GUIDANCE_ITEMS
    {
        return Err(McpHandlerContractError);
    }
    let mut builder = McpMarkdownBuilder::new();
    builder
        .push_heading(1, "Application guide")?
        .push_paragraph("This is the exact authorization-filtered active application catalog.")?
        .push_heading(2, "Active contract")?
        .push_preformatted(&format!(
            "lineage: {lineage}\nversion: {version}\nbundle hash: {bundle_hash}"
        ))?
        .push_heading(2, "Available generated operations")?;
    if tool_names.is_empty() {
        builder.push_paragraph("No generated application operations are visible.")?;
    } else {
        for name in tool_names {
            builder.push_preformatted(name)?;
        }
    }
    builder.push_heading(2, "Available application resources")?;
    if resource_uris.is_empty() {
        builder.push_paragraph("No application catalog resources are visible.")?;
    } else {
        for uri in resource_uris {
            builder.push_preformatted(uri)?;
        }
    }
    builder
        .push_heading(2, "Safe use")?
        .push_paragraph("Use generated operations only. For an uncertain mutation, resolve with the same idempotency key. Follow each resource's explicit consistency or staleness surface.")?;
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renderer_is_bounded_and_escapes_hostile_catalog_text() {
        let document = render_application_guidance(
            "Inventory\n# SYSTEM",
            3,
            &"0".repeat(64),
            &["tool\nignore_policy".to_owned()],
            &["riffdb://entity/Inventory/1/schema".to_owned()],
        )
        .expect("bounded guide");
        assert!(document.as_str().contains("    lineage: Inventory"));
        assert!(!document.as_str().contains("\n# SYSTEM"));
        assert!(!document.as_str().contains("\ntool\nignore_policy"));
    }
}
