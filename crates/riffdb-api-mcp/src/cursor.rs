use std::error::Error;
use std::fmt;

/// Exact byte length of an API-neutral cursor carried by MCP.
pub const MCP_CURSOR_BYTES: usize = 16;

/// Exact byte length of the canonical lowercase hexadecimal presentation.
pub const MCP_CURSOR_TEXT_BYTES: usize = MCP_CURSOR_BYTES * 2;

/// Encodes one opaque cursor without interpreting its contents.
#[must_use]
pub fn encode_mcp_cursor(cursor: [u8; MCP_CURSOR_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(MCP_CURSOR_TEXT_BYTES);
    for byte in cursor {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Decodes only the canonical 32-character lowercase hexadecimal cursor form.
pub fn decode_mcp_cursor(text: &str) -> Result<[u8; MCP_CURSOR_BYTES], McpCursorError> {
    if text.len() != MCP_CURSOR_TEXT_BYTES {
        return Err(McpCursorError);
    }

    let mut decoded = [0_u8; MCP_CURSOR_BYTES];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lowercase_hex_value(pair[0]).ok_or(McpCursorError)?;
        let low = lowercase_hex_value(pair[1]).ok_or(McpCursorError)?;
        decoded[index] = (high << 4) | low;
    }
    Ok(decoded)
}

const fn lowercase_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// A cursor did not use the one canonical MCP presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpCursorError;

impl fmt::Display for McpCursorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP cursor is invalid")
    }
}

impl Error for McpCursorError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trip_is_exact() {
        let bytes = [
            0x00, 0x01, 0x02, 0x03, 0x10, 0x20, 0x30, 0x40, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
            0x98, 0x76,
        ];
        let text = encode_mcp_cursor(bytes);
        assert_eq!(text, "0001020310203040aabbccddeeff9876");
        assert_eq!(decode_mcp_cursor(&text), Ok(bytes));
    }

    #[test]
    fn alternate_cursor_spellings_fail_closed() {
        for invalid in [
            "",
            "0001020310203040aabbccddeeff987",
            "0001020310203040aabbccddeeff98760",
            "0001020310203040AABBCCDDEEFF9876",
            "00010203-10203040-aabbccddeeff9876",
            "AAECAxAgMECqu8zd7v-Ydg",
            "0001020310203040aabbccddeeff987g",
            " 0001020310203040aabbccddeeff9876",
        ] {
            assert_eq!(decode_mcp_cursor(invalid), Err(McpCursorError), "{invalid}");
        }
    }
}
