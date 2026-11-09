//! Bounded W3C version-00 context. Trace identity never conveys data authority.
use std::fmt;

/// One syntactically checked W3C `traceparent`, without baggage or tracestate.
#[derive(Clone, Eq, PartialEq)]
pub struct W3cTraceParent {
    text: String,
    trace_id: [u8; 16],
    parent_span_id: [u8; 8],
    flags: u8,
}
impl W3cTraceParent {
    /// Accepts exactly 55 ASCII bytes, lowercase hexadecimal, version 00 and nonzero IDs.
    pub fn parse(value: &str) -> Result<Self, TraceParentError> {
        let bytes = value.as_bytes();
        if bytes.len() != 55
            || bytes.get(..3) != Some(b"00-")
            || bytes[35] != b'-'
            || bytes[52] != b'-'
        {
            return Err(TraceParentError);
        }
        let mut trace_id = [0; 16];
        let mut parent_span_id = [0; 8];
        decode(&bytes[3..35], &mut trace_id)?;
        decode(&bytes[36..52], &mut parent_span_id)?;
        let mut flags = [0];
        decode(&bytes[53..], &mut flags)?;
        if trace_id == [0; 16] || parent_span_id == [0; 8] {
            return Err(TraceParentError);
        }
        Ok(Self {
            text: value.to_owned(),
            trace_id,
            parent_span_id,
            flags: flags[0],
        })
    }
    /// Canonical bounded ASCII HTTP/gRPC metadata, containing only trace identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }
    /// Checked nonzero remote trace identifier.
    #[must_use]
    pub const fn trace_id_bytes(&self) -> [u8; 16] {
        self.trace_id
    }
    /// Checked nonzero remote parent identifier.
    #[must_use]
    pub const fn parent_span_id_bytes(&self) -> [u8; 8] {
        self.parent_span_id
    }
    /// Remote sampled bit. Receivers must still enforce their own local sampling policy.
    #[must_use]
    pub const fn sampled(&self) -> bool {
        self.flags & 1 == 1
    }
}
fn decode(bytes: &[u8], output: &mut [u8]) -> Result<(), TraceParentError> {
    let nibble = |value| match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(TraceParentError),
    };
    if bytes.len() != output.len() * 2 {
        return Err(TraceParentError);
    }
    for (pair, output) in bytes.chunks_exact(2).zip(output) {
        *output = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(())
}
impl fmt::Debug for W3cTraceParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("W3cTraceParent([REDACTED])")
    }
}
/// Closed value-free failure to construct trace propagation metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceParentError;
impl fmt::Display for TraceParentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("traceparent requires W3C version 00, exact lowercase hexadecimal and nonzero identifiers")
    }
}
impl std::error::Error for TraceParentError {}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, END-007, END-008
    #[test]
    fn traceparent_has_one_bounded_non_authoritative_syntax_and_no_baggage() {
        let value = format!("00-{}-{}-01", "a1".repeat(16), "b2".repeat(8));
        let parsed = W3cTraceParent::parse(&value).unwrap();
        assert_eq!(parsed.as_str(), value);
        assert_eq!(parsed.trace_id_bytes(), [0xa1; 16]);
        assert_eq!(parsed.parent_span_id_bytes(), [0xb2; 8]);
        assert!(parsed.sampled());
        assert!(!format!("{parsed:?}").contains("a1a1"));
        for invalid in [
            value.to_uppercase(),
            value.replace("00-", "01-"),
            value.replace("a1", "00"),
            value.replace("b2", "00"),
            value.clone() + ",other",
            value.clone() + "-extra",
            value.clone() + "\n",
            value.replace("a1", "秘密"),
            "secret-canary".to_owned(),
        ] {
            let error = W3cTraceParent::parse(&invalid).unwrap_err();
            assert!(!error.to_string().contains("secret-canary"));
        }
        assert!(
            !W3cTraceParent::parse(&(value[..53].to_owned() + "00"))
                .unwrap()
                .sampled()
        );
    }
}
