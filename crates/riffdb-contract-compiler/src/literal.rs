//! Canonical source-literal decoding shared by analysis and typed lowering.

/// Decodes one parser-validated JSON string lexeme without normalization.
pub(crate) fn decode_json_string_lexeme(lexeme: &str) -> Option<String> {
    let body = lexeme.strip_prefix('"')?.strip_suffix('"')?;
    let mut chars = body.chars();
    let mut output = String::with_capacity(body.len());
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        match chars.next()? {
            '"' => output.push('"'),
            '\\' => output.push('\\'),
            '/' => output.push('/'),
            'b' => output.push('\u{0008}'),
            'f' => output.push('\u{000c}'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'u' => {
                let first = read_hex_quad(&mut chars)?;
                let scalar = if (0xd800..=0xdbff).contains(&first) {
                    if chars.next()? != '\\' || chars.next()? != 'u' {
                        return None;
                    }
                    let second = read_hex_quad(&mut chars)?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return None;
                    }
                    0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
                } else if (0xdc00..=0xdfff).contains(&first) {
                    return None;
                } else {
                    u32::from(first)
                };
                output.push(char::from_u32(scalar)?);
            }
            _ => return None,
        }
    }
    Some(output)
}

fn read_hex_quad(chars: &mut impl Iterator<Item = char>) -> Option<u16> {
    let mut value = 0_u16;
    for _ in 0..4 {
        value = value.checked_mul(16)?;
        value = value.checked_add(u16::try_from(chars.next()?.to_digit(16)?).ok()?)?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_json_escapes_and_surrogate_pairs() {
        assert_eq!(
            decode_json_string_lexeme(r#""a\n\u0062""#).as_deref(),
            Some("a\nb")
        );
        assert_eq!(
            decode_json_string_lexeme(r#""\ud83d\ude00""#).as_deref(),
            Some("😀")
        );
        assert_eq!(decode_json_string_lexeme(r#""\ud83d""#), None);
    }
}
