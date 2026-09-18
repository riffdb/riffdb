use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::iter::Peekable;

use serde_json::Value;

use crate::{MAX_MCP_DISCOVERY_PAGE_ITEMS, MCP_OUTBOUND_MESSAGE_MAX_BYTES, decode_mcp_cursor};

/// Maximum complete hosted tool inventory accepted by the V6 parity proof.
///
/// This matches the service cursor's 31 fixed plus 4,096 dynamic candidates.
pub const MAX_MCP_PARITY_TOOL_ITEMS: usize = 4_127;
/// Maximum pages retained only as distinct cursor identities by the parity proof.
pub const MAX_MCP_PARITY_PAGES: usize = MAX_MCP_PARITY_TOOL_ITEMS;

/// Validates the collision and ordering boundary of one authority-derived expectation stream.
///
/// Fixed descriptors must already be in registry order. Dynamic descriptors must be in
/// strictly increasing exact-name order. Only bounded names are retained for collision
/// detection; schema documents remain in their caller-owned page/iterator storage.
pub fn validate_expected_mcp_descriptor_order(
    fixed: &[Value],
    dynamic: &[Value],
) -> Result<(), McpCatalogParityError> {
    if fixed
        .len()
        .checked_add(dynamic.len())
        .is_none_or(|total| total > MAX_MCP_PARITY_TOOL_ITEMS)
    {
        return Err(McpCatalogParityError);
    }
    let mut names = BTreeSet::new();
    for descriptor in fixed {
        let name = exact_descriptor_name(descriptor)?;
        if !names.insert(name) {
            return Err(McpCatalogParityError);
        }
    }
    let mut previous = None;
    for descriptor in dynamic {
        let name = exact_descriptor_name(descriptor)?;
        if previous.is_some_and(|previous| previous >= name) || !names.insert(name) {
            return Err(McpCatalogParityError);
        }
        previous = Some(name);
    }
    Ok(())
}

fn exact_descriptor_name(descriptor: &Value) -> Result<&str, McpCatalogParityError> {
    let name = descriptor
        .as_object()
        .and_then(|descriptor| descriptor.get("name"))
        .and_then(Value::as_str)
        .ok_or(McpCatalogParityError)?;
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(McpCatalogParityError);
    }
    Ok(name)
}

/// Streaming exact-descriptor comparison state for generated-catalog evidence.
///
/// The caller owns the expected iterator. This state retains only bounded cursor
/// identities and counters; it never accumulates hosted descriptors or schemas.
#[derive(Clone, Debug, Default)]
pub struct McpFullSchemaParityVerifier {
    seen_cursors: BTreeSet<[u8; 16]>,
    pages: usize,
    items: usize,
    complete: bool,
}

impl McpFullSchemaParityVerifier {
    /// Starts one empty streaming comparison.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            seen_cursors: BTreeSet::new(),
            pages: 0,
            items: 0,
            complete: false,
        }
    }

    /// Compares one complete JSON-RPC `tools/list` response byte-for-byte by descriptor.
    ///
    /// `expected` must already be ordered fixed-registry first and dynamic-name
    /// lexicographically, after policy visibility and collision checks.
    pub fn verify_response<I>(
        &mut self,
        response: &[u8],
        expected: &mut Peekable<I>,
    ) -> Result<(), McpCatalogParityError>
    where
        I: Iterator<Item = Value>,
    {
        if self.complete
            || response.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES
            || self.pages == MAX_MCP_PARITY_PAGES
        {
            return Err(McpCatalogParityError);
        }
        let response = crate::bounded_json::parse_unique(response, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
            .map_err(|_| McpCatalogParityError)?;
        let envelope = response.as_object().ok_or(McpCatalogParityError)?;
        if envelope.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || !envelope.contains_key("id")
            || envelope.contains_key("error")
        {
            return Err(McpCatalogParityError);
        }
        let result = envelope
            .get("result")
            .and_then(Value::as_object)
            .ok_or(McpCatalogParityError)?;
        if result
            .keys()
            .any(|key| key != "tools" && key != "nextCursor")
        {
            return Err(McpCatalogParityError);
        }
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .ok_or(McpCatalogParityError)?;
        if tools.len() > MAX_MCP_DISCOVERY_PAGE_ITEMS || self.pages > 0 && tools.is_empty() {
            return Err(McpCatalogParityError);
        }
        for actual in tools {
            if self.items == MAX_MCP_PARITY_TOOL_ITEMS
                || expected.next().is_none_or(|expected| expected != *actual)
            {
                return Err(McpCatalogParityError);
            }
            self.items += 1;
        }

        match result.get("nextCursor") {
            None => {
                if expected.peek().is_some() {
                    return Err(McpCatalogParityError);
                }
                self.complete = true;
            }
            Some(Value::String(cursor)) => {
                let cursor = decode_mcp_cursor(cursor).map_err(|_| McpCatalogParityError)?;
                if tools.is_empty()
                    || expected.peek().is_none()
                    || !self.seen_cursors.insert(cursor)
                {
                    return Err(McpCatalogParityError);
                }
            }
            Some(_) => return Err(McpCatalogParityError),
        }
        self.pages += 1;
        Ok(())
    }

    /// Reports whether an exact terminal page consumed the expected iterator.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// Returns the number of descriptors compared without retaining them.
    #[must_use]
    pub const fn compared_items(&self) -> usize {
        self.items
    }
}

/// A hosted full-schema page did not equal the authority-derived expected stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpCatalogParityError;

impl fmt::Display for McpCatalogParityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP catalog parity check failed")
    }
}

impl Error for McpCatalogParityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode_mcp_cursor;
    use serde_json::json;

    fn descriptor(index: usize) -> Value {
        json!({
            "annotations": {
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "readOnlyHint": true,
            },
            "inputSchema": {"additionalProperties": false, "type": "object"},
            "name": format!("riffdb_query_{index:04}"),
            "outputSchema": {"additionalProperties": false, "type": "object"},
        })
    }

    fn response(tools: &[Value], cursor: Option<[u8; 16]>) -> Vec<u8> {
        let mut result = json!({"tools": tools});
        if let Some(cursor) = cursor {
            result["nextCursor"] = Value::String(encode_mcp_cursor(cursor));
        }
        serde_json::to_vec(&json!({"id": 1, "jsonrpc": "2.0", "result": result})).expect("response")
    }

    fn rejects(expected: &[Value], pages: &[Vec<u8>]) {
        let mut expected = expected.iter().cloned().peekable();
        let mut verifier = McpFullSchemaParityVerifier::new();
        assert!(
            pages
                .iter()
                .any(|page| verifier.verify_response(page, &mut expected).is_err())
        );
    }

    // req: MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043, MCP-045, DX-044, DX-047, DX-049
    #[test]
    fn full_schema_parity_stream_rejects_page_cursor_and_descriptor_drift() {
        let expected = (0..1_024).map(descriptor).collect::<Vec<_>>();
        let pages = [
            response(&expected[..500], Some([1; 16])),
            response(&expected[500..1_000], Some([2; 16])),
            response(&expected[1_000..], None),
        ];
        let mut stream = expected.iter().cloned().peekable();
        let mut verifier = McpFullSchemaParityVerifier::new();
        for page in &pages {
            verifier
                .verify_response(page, &mut stream)
                .expect("exact streaming page");
        }
        assert!(verifier.is_complete());
        assert_eq!(verifier.compared_items(), 1_024);

        let pair = [descriptor(0), descriptor(1)];
        rejects(&pair, &[response(&pair[..1], None)]); // missing
        rejects(&pair[..1], &[response(&pair, None)]); // extra
        rejects(
            &pair,
            &[response(&[pair[0].clone(), pair[0].clone()], None)],
        ); // duplicate
        rejects(
            &pair,
            &[response(&[pair[1].clone(), pair[0].clone()], None)],
        ); // reordered
        let mut malformed = pair[0].clone();
        malformed["annotations"]["readOnlyHint"] = Value::Bool(false);
        rejects(&pair[..1], &[response(&[malformed], None)]); // metadata differs
        rejects(
            &pair,
            &[response(&pair[..1], Some([3; 16])), response(&[], None)],
        ); // empty continuation
        rejects(&pair[..1], &[response(&pair[..1], Some([4; 16]))]); // cursor past end

        let cursor = encode_mcp_cursor([0xab; 16]);
        let invalid_cursor = serde_json::to_vec(&json!({
            "id": 1,
            "jsonrpc": "2.0",
            "result": {"nextCursor": cursor.to_uppercase(), "tools": [pair[0].clone()]},
        }))
        .expect("response");
        rejects(&pair, &[invalid_cursor]);
        rejects(
            &[descriptor(0), descriptor(1), descriptor(2)],
            &[
                response(&[descriptor(0)], Some([6; 16])),
                response(&[descriptor(1)], Some([6; 16])),
            ],
        ); // cursor loop

        let oversized_items = (0..=MAX_MCP_DISCOVERY_PAGE_ITEMS)
            .map(descriptor)
            .collect::<Vec<_>>();
        rejects(&oversized_items, &[response(&oversized_items, None)]);
        let oversized_response = vec![b' '; MCP_OUTBOUND_MESSAGE_MAX_BYTES + 1];
        rejects(&[], &[oversized_response]);

        let fixed = [descriptor(0)];
        let dynamic = [descriptor(1), descriptor(2)];
        validate_expected_mcp_descriptor_order(&fixed, &dynamic)
            .expect("fixed then lexicographic dynamic order");
        assert!(
            validate_expected_mcp_descriptor_order(&fixed, &[descriptor(0)]).is_err(),
            "fixed/dynamic collision"
        );
        assert!(
            validate_expected_mcp_descriptor_order(&[], &[descriptor(1), descriptor(1)],).is_err(),
            "dynamic/dynamic collision"
        );
        assert!(
            validate_expected_mcp_descriptor_order(&[], &[descriptor(2), descriptor(1)],).is_err(),
            "dynamic ordering"
        );
        let malformed = serde_json::to_vec(&json!({
            "id": 1,
            "jsonrpc": "2.0",
            "result": {"tools": [{"name": "riffdb_query_broken"}]},
        }))
        .expect("malformed descriptor response");
        rejects(&[descriptor(0)], &[malformed]);
    }
}
