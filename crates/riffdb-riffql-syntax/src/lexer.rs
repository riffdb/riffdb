use crate::{
    DiagnosticCode, MAX_SOURCE_BYTES, MAX_SYNTAX_ITEMS, ParseDiagnostic, ParseDiagnostics, Span,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Ident(String),
    Parameter(String),
    Unsigned(String),
    String(String),
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    Colon,
    Comma,
    Semicolon,
    Dot,
    Question,
    Equal,
    EqualEqual,
    BangEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    AndAnd,
    Pipe,
    OrOr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) span: Span,
}

pub(crate) fn lex(source: &str) -> Result<Vec<Token>, ParseDiagnostics> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(ParseDiagnostics::one(ParseDiagnostic::new(
            DiagnosticCode::SourceTooLong,
            Span::checked(0, source.len().min(u32::MAX as usize)).unwrap_or(Span {
                start: 0,
                end: u32::MAX,
            }),
            "RiffQL source exceeds the byte limit",
            Some("submit a smaller query"),
        )));
    }
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match bytes[offset] {
            byte if byte.is_ascii_whitespace() => offset += 1,
            b'/' if bytes.get(offset + 1) == Some(&b'/') => {
                offset += 2;
                while offset < bytes.len() && bytes[offset] != b'\n' {
                    offset += 1;
                }
            }
            b'$' => {
                let start = offset;
                offset += 1;
                let name_start = offset;
                offset = consume_identifier(bytes, offset);
                if offset == name_start {
                    return Err(invalid(start, offset, "invalid parameter token"));
                }
                push(
                    &mut tokens,
                    TokenKind::Parameter(source[name_start..offset].to_owned()),
                    start,
                    offset,
                )?;
            }
            byte if byte == b'_' || byte.is_ascii_alphabetic() => {
                let start = offset;
                offset = consume_identifier(bytes, offset);
                push(
                    &mut tokens,
                    TokenKind::Ident(source[start..offset].to_owned()),
                    start,
                    offset,
                )?;
            }
            byte if byte.is_ascii_digit() => {
                let start = offset;
                while offset < bytes.len() && bytes[offset].is_ascii_digit() {
                    offset += 1;
                }
                push(
                    &mut tokens,
                    TokenKind::Unsigned(source[start..offset].to_owned()),
                    start,
                    offset,
                )?;
            }
            b'"' => {
                let start = offset;
                let (value, end) = string_literal(source, offset)?;
                offset = end;
                push(&mut tokens, TokenKind::String(value), start, offset)?;
            }
            byte => {
                let start = offset;
                offset += 1;
                let kind = match byte {
                    b'{' => TokenKind::LeftBrace,
                    b'}' => TokenKind::RightBrace,
                    b'(' => TokenKind::LeftParen,
                    b')' => TokenKind::RightParen,
                    b':' => TokenKind::Colon,
                    b',' => TokenKind::Comma,
                    b';' => TokenKind::Semicolon,
                    b'.' => TokenKind::Dot,
                    b'?' => TokenKind::Question,
                    b'=' if bytes.get(offset) == Some(&b'=') => {
                        offset += 1;
                        TokenKind::EqualEqual
                    }
                    b'=' => TokenKind::Equal,
                    b'!' if bytes.get(offset) == Some(&b'=') => {
                        offset += 1;
                        TokenKind::BangEqual
                    }
                    b'<' if bytes.get(offset) == Some(&b'=') => {
                        offset += 1;
                        TokenKind::LessEqual
                    }
                    b'<' => TokenKind::Less,
                    b'>' if bytes.get(offset) == Some(&b'=') => {
                        offset += 1;
                        TokenKind::GreaterEqual
                    }
                    b'>' => TokenKind::Greater,
                    b'&' if bytes.get(offset) == Some(&b'&') => {
                        offset += 1;
                        TokenKind::AndAnd
                    }
                    b'|' if bytes.get(offset) == Some(&b'|') => {
                        offset += 1;
                        TokenKind::OrOr
                    }
                    b'|' => TokenKind::Pipe,
                    _ => return Err(invalid(start, offset, "invalid RiffQL token")),
                };
                push(&mut tokens, kind, start, offset)?;
            }
        }
    }
    Ok(tokens)
}

fn consume_identifier(bytes: &[u8], mut offset: usize) -> usize {
    while bytes
        .get(offset)
        .is_some_and(|byte| *byte == b'_' || byte.is_ascii_alphanumeric())
    {
        offset += 1;
    }
    offset
}

fn push(
    tokens: &mut Vec<Token>,
    kind: TokenKind,
    start: usize,
    end: usize,
) -> Result<(), ParseDiagnostics> {
    if tokens.len() == MAX_SYNTAX_ITEMS {
        return Err(ParseDiagnostics::one(ParseDiagnostic::new(
            DiagnosticCode::TooManySyntaxItems,
            Span::checked(start, end).expect("bounded source span"),
            "RiffQL token limit exceeded",
            Some("reduce query complexity"),
        )));
    }
    tokens.push(Token {
        kind,
        span: Span::checked(start, end).expect("bounded source span"),
    });
    Ok(())
}

fn invalid(start: usize, end: usize, summary: &'static str) -> ParseDiagnostics {
    ParseDiagnostics::one(ParseDiagnostic::new(
        DiagnosticCode::InvalidToken,
        Span::checked(start, end).expect("bounded source span"),
        summary,
        None,
    ))
}

fn string_literal(source: &str, start: usize) -> Result<(String, usize), ParseDiagnostics> {
    let bytes = source.as_bytes();
    let mut offset = start + 1;
    let mut value = String::new();
    while offset < bytes.len() {
        match bytes[offset] {
            b'"' => return Ok((value, offset + 1)),
            b'\\' => {
                offset += 1;
                let Some(escaped) = bytes.get(offset).copied() else {
                    break;
                };
                let character = match escaped {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'b' => '\u{0008}',
                    b'f' => '\u{000c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    _ => return Err(invalid(start, offset + 1, "invalid string escape")),
                };
                value.push(character);
                offset += 1;
            }
            byte if byte < 0x20 => {
                return Err(invalid(start, offset + 1, "invalid string character"));
            }
            _ => {
                let character = source[offset..]
                    .chars()
                    .next()
                    .expect("offset is in source");
                value.push(character);
                offset += character.len_utf8();
            }
        }
    }
    Err(invalid(start, source.len(), "unterminated string literal"))
}
