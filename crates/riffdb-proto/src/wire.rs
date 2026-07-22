//! Allocation-free Protobuf preflight for bounded public messages.

use riffdb_errors::{MAX_VALIDATION_ISSUES, MAX_VALIDATION_PATH_SEGMENTS};
use riffdb_types::{
    MAX_BYTES_VALUE_BYTES, MAX_CANONICAL_DOCUMENT_BYTES, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH,
    MAX_RECORD_FIELDS, MAX_STRING_BYTES,
};

use crate::{
    MAX_DURABILITY_MODE_BYTES, MAX_EXECUTE_REQUEST_BYTES, MAX_EXECUTE_RESPONSE_BYTES,
    MAX_PROTOCOL_NAME_BYTES, MAX_PROVENANCE_URI_BYTES, MAX_PUBLIC_ERROR_BYTES,
};

const PROST_RECURSION_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreflightError {
    Malformed,
    LimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvelopePreflightError {
    Malformed,
    InvalidRecordType,
    InvalidSchemaHashLength,
    NonCanonical,
}

#[derive(Clone, Copy)]
pub(crate) struct EnvelopePreflight<'a> {
    pub(crate) storage_format_version: u32,
    pub(crate) record_type: &'a [u8],
    pub(crate) payload: &'a [u8],
    pub(crate) payload_crc32c: u32,
    pub(crate) schema_hash: &'a [u8],
}

#[derive(Clone, Copy)]
pub(crate) struct Field<'a> {
    pub(crate) number: u32,
    pub(crate) wire_type: u8,
    pub(crate) bytes: &'a [u8],
    varint: Option<u64>,
}

impl Field<'_> {
    pub(crate) fn require_wire(self, expected: u8) -> Result<Self, PreflightError> {
        if self.wire_type == expected {
            Ok(self)
        } else {
            Err(PreflightError::Malformed)
        }
    }

    pub(crate) fn require_varint(self) -> Result<u64, PreflightError> {
        self.require_wire(0)?
            .varint
            .ok_or(PreflightError::Malformed)
    }
}

pub(crate) fn bounded_message(input: &[u8], maximum: usize) -> Result<(), PreflightError> {
    if input.len() > maximum {
        return Err(PreflightError::LimitExceeded);
    }
    let mut cursor = Cursor::new(input);
    while cursor.next()?.is_some() {}
    Ok(())
}

pub(crate) struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    pub(crate) const fn input_is_empty(&self) -> bool {
        self.position == self.input.len()
    }

    pub(crate) fn next(&mut self) -> Result<Option<Field<'a>>, PreflightError> {
        if self.position == self.input.len() {
            return Ok(None);
        }

        let key = self.read_varint()?;
        if key > u64::from(u32::MAX) {
            return Err(PreflightError::Malformed);
        }
        let number = u32::try_from(key >> 3).map_err(|_| PreflightError::Malformed)?;
        let wire_type = u8::try_from(key & 0x07).map_err(|_| PreflightError::Malformed)?;
        if number == 0 {
            return Err(PreflightError::Malformed);
        }

        let mut varint = None;
        let bytes = match wire_type {
            0 => {
                varint = Some(self.read_varint()?);
                &[][..]
            }
            1 => self.read_exact(8)?,
            2 => {
                let raw_length = self.read_varint()?;
                let length = usize::try_from(raw_length).map_err(|_| PreflightError::Malformed)?;
                self.read_exact(length)?
            }
            3 => {
                self.skip_group(number, 1)?;
                &[][..]
            }
            4 => return Err(PreflightError::Malformed),
            5 => self.read_exact(4)?,
            _ => return Err(PreflightError::Malformed),
        };
        Ok(Some(Field {
            number,
            wire_type,
            bytes,
            varint,
        }))
    }

    fn skip_group(&mut self, expected_end: u32, depth: usize) -> Result<(), PreflightError> {
        if depth > PROST_RECURSION_LIMIT {
            return Err(PreflightError::LimitExceeded);
        }
        loop {
            let key = self.read_varint()?;
            if key > u64::from(u32::MAX) {
                return Err(PreflightError::Malformed);
            }
            let number = u32::try_from(key >> 3).map_err(|_| PreflightError::Malformed)?;
            let wire_type = u8::try_from(key & 0x07).map_err(|_| PreflightError::Malformed)?;
            if number == 0 {
                return Err(PreflightError::Malformed);
            }
            match wire_type {
                0 => {
                    self.read_varint()?;
                }
                1 => {
                    self.read_exact(8)?;
                }
                2 => {
                    let raw_length = self.read_varint()?;
                    let length =
                        usize::try_from(raw_length).map_err(|_| PreflightError::Malformed)?;
                    self.read_exact(length)?;
                }
                3 => self.skip_group(number, depth + 1)?,
                4 if number == expected_end => return Ok(()),
                4 => return Err(PreflightError::Malformed),
                5 => {
                    self.read_exact(4)?;
                }
                _ => return Err(PreflightError::Malformed),
            }
        }
    }

    pub(crate) fn read_varint(&mut self) -> Result<u64, PreflightError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = *self
                .input
                .get(self.position)
                .ok_or(PreflightError::Malformed)?;
            self.position += 1;
            if shift == 63 && byte > 1 {
                return Err(PreflightError::Malformed);
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(PreflightError::Malformed)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], PreflightError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(PreflightError::Malformed)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(PreflightError::Malformed)?;
        self.position = end;
        Ok(value)
    }
}

pub(crate) fn value(input: &[u8]) -> Result<(), PreflightError> {
    if input.len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(PreflightError::LimitExceeded);
    }
    value_at_depth(input, 0)
}

fn value_at_depth(input: &[u8], depth: usize) -> Result<(), PreflightError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(PreflightError::LimitExceeded);
    }
    let mut cursor = Cursor::new(input);
    let mut saw_kind = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1..=4 => {
                field.require_wire(0)?;
                claim_singular(&mut saw_kind)?;
            }
            5 => {
                claim_singular(&mut saw_kind)?;
                decimal(field.require_wire(2)?.bytes)?;
            }
            6 => {
                claim_singular(&mut saw_kind)?;
                money(field.require_wire(2)?.bytes)?;
            }
            7 => {
                claim_singular(&mut saw_kind)?;
                check_length(field.require_wire(2)?.bytes, MAX_STRING_BYTES)?;
            }
            8 => {
                claim_singular(&mut saw_kind)?;
                check_length(field.require_wire(2)?.bytes, MAX_BYTES_VALUE_BYTES)?;
            }
            9 => {
                claim_singular(&mut saw_kind)?;
                check_exact_length(field.require_wire(2)?.bytes, 16)?;
            }
            10 => {
                claim_singular(&mut saw_kind)?;
                date(field.require_wire(2)?.bytes)?;
            }
            11 => {
                claim_singular(&mut saw_kind)?;
                timestamp(field.require_wire(2)?.bytes)?;
            }
            12 => {
                claim_singular(&mut saw_kind)?;
                enum_value(field.require_wire(2)?.bytes)?;
            }
            13 => {
                claim_singular(&mut saw_kind)?;
                list(field.require_wire(2)?.bytes, depth)?;
            }
            14 => {
                claim_singular(&mut saw_kind)?;
                record(field.require_wire(2)?.bytes, depth)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn decimal(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_coefficient = false;
    let mut saw_scale = false;
    while let Some(field) = cursor.next()? {
        if field.number == 1 {
            claim_singular(&mut saw_coefficient)?;
            let coefficient = field.require_wire(2)?.bytes;
            if coefficient.is_empty() || coefficient.len() > 16 {
                return Err(PreflightError::LimitExceeded);
            }
        } else if field.number == 2 {
            claim_singular(&mut saw_scale)?;
            field.require_wire(0)?;
        }
    }
    Ok(())
}

fn money(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_currency = false;
    let mut saw_amount = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_currency)?;
                check_exact_length(field.require_wire(2)?.bytes, 3)?;
            }
            2 => {
                claim_singular(&mut saw_amount)?;
                decimal(field.require_wire(2)?.bytes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn date(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_days = false;
    while let Some(field) = cursor.next()? {
        if field.number == 1 {
            claim_singular(&mut saw_days)?;
            field.require_wire(0)?;
        }
    }
    Ok(())
}

fn timestamp(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_seconds = false;
    let mut saw_nanos = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_seconds)?;
                field.require_wire(0)?;
            }
            2 => {
                claim_singular(&mut saw_nanos)?;
                field.require_wire(0)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn enum_value(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_type = false;
    let mut saw_variant = false;
    let mut saw_name = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_type)?;
                field.require_wire(0)?;
            }
            2 => {
                claim_singular(&mut saw_variant)?;
                field.require_wire(0)?;
            }
            3 => {
                claim_singular(&mut saw_name)?;
                check_length(field.require_wire(2)?.bytes, MAX_PROTOCOL_NAME_BYTES)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn list(input: &[u8], depth: usize) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut count = 0_usize;
    while let Some(field) = cursor.next()? {
        if field.number == 1 {
            count = count.checked_add(1).ok_or(PreflightError::LimitExceeded)?;
            if count > MAX_LIST_ENTRIES {
                return Err(PreflightError::LimitExceeded);
            }
            value_at_depth(field.require_wire(2)?.bytes, depth + 1)?;
        }
    }
    Ok(())
}

fn record(input: &[u8], depth: usize) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut count = 0_usize;
    while let Some(field) = cursor.next()? {
        if field.number == 1 {
            count = count.checked_add(1).ok_or(PreflightError::LimitExceeded)?;
            if count > MAX_RECORD_FIELDS {
                return Err(PreflightError::LimitExceeded);
            }
            value_field(field.require_wire(2)?.bytes, depth)?;
        }
    }
    Ok(())
}

pub(crate) fn value_record(input: &[u8]) -> Result<(), PreflightError> {
    if input.len() > MAX_CANONICAL_DOCUMENT_BYTES {
        return Err(PreflightError::LimitExceeded);
    }
    record(input, 0)
}

fn value_field(input: &[u8], depth: usize) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_field_id = false;
    let mut saw_name = false;
    let mut saw_value = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_field_id)?;
                field.require_wire(0)?;
            }
            2 => {
                claim_singular(&mut saw_name)?;
                check_length(field.require_wire(2)?.bytes, MAX_PROTOCOL_NAME_BYTES)?;
            }
            3 => {
                claim_singular(&mut saw_value)?;
                value_at_depth(field.require_wire(2)?.bytes, depth + 1)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn execute_request(input: &[u8]) -> Result<(), PreflightError> {
    execute(
        input,
        MAX_EXECUTE_REQUEST_BYTES,
        4,
        &[(1, 16), (2, MAX_PROTOCOL_NAME_BYTES)],
    )
}

pub(crate) fn execute_response(input: &[u8]) -> Result<(), PreflightError> {
    execute(
        input,
        MAX_EXECUTE_RESPONSE_BYTES,
        6,
        &[
            (4, 32),
            (5, MAX_PROTOCOL_NAME_BYTES),
            (7, MAX_PROVENANCE_URI_BYTES),
            (8, MAX_DURABILITY_MODE_BYTES),
        ],
    )
}

fn execute(
    input: &[u8],
    maximum: usize,
    value_field_number: u32,
    byte_limits: &[(u32, usize)],
) -> Result<(), PreflightError> {
    if input.len() > maximum {
        return Err(PreflightError::LimitExceeded);
    }
    let mut cursor = Cursor::new(input);
    let maximum_known_field = byte_limits
        .iter()
        .map(|(number, _)| *number)
        .max()
        .unwrap_or(value_field_number)
        .max(value_field_number);
    let mut seen = [false; 8];
    while let Some(field) = cursor.next()? {
        if (1..=maximum_known_field).contains(&field.number) {
            let index = usize::try_from(field.number - 1).map_err(|_| PreflightError::Malformed)?;
            claim_singular(seen.get_mut(index).ok_or(PreflightError::Malformed)?)?;
        }
        if field.number == value_field_number {
            value(field.require_wire(2)?.bytes)?;
        } else if let Some((_, limit)) = byte_limits
            .iter()
            .find(|(number, _)| *number == field.number)
        {
            check_length(field.require_wire(2)?.bytes, *limit)?;
        }
    }
    Ok(())
}

pub(crate) fn public_error(input: &[u8]) -> Result<(), PreflightError> {
    if input.len() > MAX_PUBLIC_ERROR_BYTES {
        return Err(PreflightError::LimitExceeded);
    }
    let mut cursor = Cursor::new(input);
    let mut saw_details = false;
    let mut saw_kind = false;
    let mut saw_code = false;
    let mut saw_safe_message = false;
    let mut saw_recovery_action = false;
    let mut saw_incident_id = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_kind)?;
                field.require_wire(0)?;
            }
            2 => {
                claim_singular(&mut saw_code)?;
                check_length(field.require_wire(2)?.bytes, 256)?;
            }
            3 => {
                claim_singular(&mut saw_safe_message)?;
                check_length(field.require_wire(2)?.bytes, 256)?;
            }
            4 => {
                claim_singular(&mut saw_recovery_action)?;
                field.require_wire(0)?;
            }
            5 => {
                claim_singular(&mut saw_details)?;
                validation_issues(field.require_wire(2)?.bytes)?;
            }
            6 => {
                claim_singular(&mut saw_details)?;
                contract_mismatch_details(field.require_wire(2)?.bytes)?;
            }
            7 => {
                claim_singular(&mut saw_incident_id)?;
                check_exact_length(field.require_wire(2)?.bytes, 16)?;
            }
            8 => {
                claim_singular(&mut saw_details)?;
                execution_failure_details(field.require_wire(2)?.bytes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn stored_envelope(
    input: &[u8],
) -> Result<EnvelopePreflight<'_>, EnvelopePreflightError> {
    let mut cursor = Cursor::new(input);
    let mut storage_format_version = 0;
    let mut record_type_bytes = &[][..];
    let mut payload_bytes = &[][..];
    let mut payload_crc32c = 0;
    let mut schema_hash_bytes = &[][..];
    let mut record_type = false;
    let mut payload = false;
    let mut version = false;
    let mut checksum = false;
    let mut schema_hash = false;
    while let Some(field) = cursor
        .next()
        .map_err(|_| EnvelopePreflightError::Malformed)?
    {
        match field.number {
            1 => {
                claim_envelope_singular(&mut version)?;
                let raw_version = field
                    .require_varint()
                    .map_err(|_| EnvelopePreflightError::Malformed)?;
                storage_format_version =
                    u32::try_from(raw_version).map_err(|_| EnvelopePreflightError::NonCanonical)?;
            }
            2 => {
                claim_envelope_singular(&mut record_type)?;
                let bytes = field
                    .require_wire(2)
                    .map_err(|_| EnvelopePreflightError::Malformed)?
                    .bytes;
                if bytes.len() > crate::envelope::MAX_RECORD_TYPE_BYTES {
                    return Err(EnvelopePreflightError::InvalidRecordType);
                }
                record_type_bytes = bytes;
            }
            3 => {
                claim_envelope_singular(&mut payload)?;
                payload_bytes = field
                    .require_wire(2)
                    .map_err(|_| EnvelopePreflightError::Malformed)?
                    .bytes;
            }
            4 => {
                claim_envelope_singular(&mut checksum)?;
                let bytes = field
                    .require_wire(5)
                    .map_err(|_| EnvelopePreflightError::Malformed)?
                    .bytes;
                payload_crc32c = u32::from_le_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| EnvelopePreflightError::Malformed)?,
                );
            }
            5 => {
                claim_envelope_singular(&mut schema_hash)?;
                let bytes = field
                    .require_wire(2)
                    .map_err(|_| EnvelopePreflightError::Malformed)?
                    .bytes;
                if bytes.len() != 32 {
                    return Err(EnvelopePreflightError::InvalidSchemaHashLength);
                }
                schema_hash_bytes = bytes;
            }
            _ => {}
        }
    }
    Ok(EnvelopePreflight {
        storage_format_version,
        record_type: record_type_bytes,
        payload: payload_bytes,
        payload_crc32c,
        schema_hash: schema_hash_bytes,
    })
}

fn claim_envelope_singular(seen: &mut bool) -> Result<(), EnvelopePreflightError> {
    if *seen {
        return Err(EnvelopePreflightError::NonCanonical);
    }
    *seen = true;
    Ok(())
}

fn validation_issues(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut count = 0_usize;
    while let Some(field) = cursor.next()? {
        if field.number == 1 {
            count += 1;
            if count > MAX_VALIDATION_ISSUES {
                return Err(PreflightError::LimitExceeded);
            }
            validation_issue(field.require_wire(2)?.bytes)?;
        }
    }
    Ok(())
}

fn validation_issue(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut count = 0_usize;
    let mut saw_code = false;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                claim_singular(&mut saw_code)?;
                field.require_wire(0)?;
            }
            2 => {
                count += 1;
                if count > MAX_VALIDATION_PATH_SEGMENTS {
                    return Err(PreflightError::LimitExceeded);
                }
                validation_path_segment(field.require_wire(2)?.bytes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validation_path_segment(input: &[u8]) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_segment = false;
    while let Some(field) = cursor.next()? {
        if matches!(field.number, 1 | 2) {
            claim_singular(&mut saw_segment)?;
            field.require_wire(0)?;
        }
    }
    Ok(())
}

fn contract_mismatch_details(input: &[u8]) -> Result<(), PreflightError> {
    singular_varint_message(input, 1)
}

fn execution_failure_details(input: &[u8]) -> Result<(), PreflightError> {
    singular_varint_message(input, 1)
}

fn singular_varint_message(input: &[u8], field_number: u32) -> Result<(), PreflightError> {
    let mut cursor = Cursor::new(input);
    let mut saw_value = false;
    while let Some(field) = cursor.next()? {
        if field.number == field_number {
            claim_singular(&mut saw_value)?;
            field.require_wire(0)?;
        }
    }
    Ok(())
}

pub(crate) fn claim_singular(seen: &mut bool) -> Result<(), PreflightError> {
    if *seen {
        return Err(PreflightError::Malformed);
    }
    *seen = true;
    Ok(())
}

pub(crate) fn check_length(bytes: &[u8], maximum: usize) -> Result<(), PreflightError> {
    if bytes.len() > maximum {
        Err(PreflightError::LimitExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn check_exact_length(bytes: &[u8], expected: usize) -> Result<(), PreflightError> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(PreflightError::LimitExceeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn varint(mut value: usize) -> Vec<u8> {
        let mut encoded = Vec::new();
        loop {
            let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit in u8");
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            encoded.push(byte);
            if value == 0 {
                return encoded;
            }
        }
    }

    fn length_delimited(field_number: usize, payload: &[u8]) -> Vec<u8> {
        let mut encoded = varint((field_number << 3) | 2);
        encoded.extend(varint(payload.len()));
        encoded.extend(payload);
        encoded
    }

    fn list_value(child_count: usize) -> Vec<u8> {
        let mut list = Vec::with_capacity(child_count * 2);
        for _ in 0..child_count {
            list.extend_from_slice(&[0x0a, 0x00]);
        }
        length_delimited(13, &list)
    }

    fn nested_list(depth: usize) -> Vec<u8> {
        let mut value = vec![0x08, 0x00];
        for _ in 0..depth {
            let list = length_delimited(1, &value);
            value = length_delimited(13, &list);
        }
        value
    }

    fn public_validation(issue_count: usize, path_count: usize) -> Vec<u8> {
        let mut issue = Vec::new();
        for _ in 0..path_count {
            issue.extend_from_slice(&[0x12, 0x00]);
        }
        let mut issues = Vec::new();
        for _ in 0..issue_count {
            issues.extend(length_delimited(1, &issue));
        }
        length_delimited(5, &issues)
    }

    #[test]
    fn list_count_is_checked_before_message_allocation() {
        assert_eq!(value(&list_value(MAX_LIST_ENTRIES)), Ok(()));
        let over_limit = list_value(MAX_LIST_ENTRIES + 1);
        assert_eq!(value(&over_limit), Err(PreflightError::LimitExceeded));
        assert_eq!(
            crate::decode_value(&over_limit),
            Err(crate::ValueValidationError::PreflightLimitExceeded)
        );
    }

    #[test]
    fn nesting_depth_is_checked_before_message_allocation() {
        assert_eq!(value(&nested_list(MAX_NESTING_DEPTH)), Ok(()));
        assert_eq!(
            value(&nested_list(MAX_NESTING_DEPTH + 1)),
            Err(PreflightError::LimitExceeded)
        );
    }

    #[test]
    fn public_error_counts_are_checked_before_allocation() {
        assert_eq!(
            public_error(&public_validation(MAX_VALIDATION_ISSUES, 0)),
            Ok(())
        );
        let over_issue_limit = public_validation(MAX_VALIDATION_ISSUES + 1, 0);
        assert_eq!(
            public_error(&over_issue_limit),
            Err(PreflightError::LimitExceeded)
        );
        assert_eq!(
            crate::decode_public_error(&over_issue_limit),
            Err(crate::PublicErrorWireError::PreflightLimitExceeded)
        );
        assert_eq!(
            public_error(&public_validation(1, MAX_VALIDATION_PATH_SEGMENTS)),
            Ok(())
        );
        assert_eq!(
            public_error(&public_validation(1, MAX_VALIDATION_PATH_SEGMENTS + 1)),
            Err(PreflightError::LimitExceeded)
        );
    }

    #[test]
    fn duplicate_known_message_fields_cannot_merge_around_limits() {
        let null = [0x08, 0x00];
        let mut request = length_delimited(4, &null);
        request.extend(length_delimited(4, &null));
        assert_eq!(execute_request(&request), Err(PreflightError::Malformed));
        assert_eq!(
            crate::decode_execute_request(&request),
            Err(crate::ExecuteWireError::MalformedEncoding)
        );

        let validation = public_validation(1, 0);
        let mut error = validation.clone();
        error.extend(validation);
        assert_eq!(public_error(&error), Err(PreflightError::Malformed));

        let execution_failure = length_delimited(8, &[0x08, 0x01]);
        let mut duplicate_execution = execution_failure.clone();
        duplicate_execution.extend(execution_failure.clone());
        assert_eq!(
            public_error(&duplicate_execution),
            Err(PreflightError::Malformed)
        );

        let mut mixed_details = public_validation(1, 0);
        mixed_details.extend(execution_failure);
        assert_eq!(public_error(&mixed_details), Err(PreflightError::Malformed));

        let duplicate_request_scalar = [0x18, 0x01, 0x18, 0x02];
        assert_eq!(
            execute_request(&duplicate_request_scalar),
            Err(PreflightError::Malformed)
        );

        let duplicate_error_kind = [0x08, 0x01, 0x08, 0x02];
        assert_eq!(
            public_error(&duplicate_error_kind),
            Err(PreflightError::Malformed)
        );

        let mixed_path_segment = [0x08, 0x01, 0x10, 0x02];
        let mut issue = vec![0x08, 0x01];
        issue.extend(length_delimited(2, &mixed_path_segment));
        let issues = length_delimited(1, &issue);
        let nested_duplicate = length_delimited(5, &issues);
        assert_eq!(
            public_error(&nested_duplicate),
            Err(PreflightError::Malformed)
        );
    }

    #[test]
    fn duplicate_singular_fields_in_nested_values_cannot_merge() {
        let duplicate_decimal = [0x0a, 0x01, 0x0a, 0x01];
        assert_eq!(
            value(&length_delimited(5, &duplicate_decimal)),
            Err(PreflightError::Malformed)
        );

        let duplicate_money_currency = [0x0a, 0x03, b'U', b'S', b'D', 0x0a, 0x03, b'E', b'U', b'R'];
        assert_eq!(
            value(&length_delimited(6, &duplicate_money_currency)),
            Err(PreflightError::Malformed)
        );

        for (field_number, nested) in [
            (10, vec![0x08, 0x00, 0x08, 0x02]),
            (11, vec![0x10, 0x01, 0x10, 0x02]),
            (12, vec![0x08, 0x01, 0x08, 0x02]),
        ] {
            assert_eq!(
                value(&length_delimited(field_number, &nested)),
                Err(PreflightError::Malformed)
            );
        }

        let duplicate_field_name = [0x12, 0x01, b'a', 0x12, 0x01, b'b', 0x1a, 0x02, 0x08, 0x00];
        let record = length_delimited(1, &duplicate_field_name);
        assert_eq!(
            value(&length_delimited(14, &record)),
            Err(PreflightError::Malformed)
        );
    }

    #[test]
    fn bounded_unknown_groups_are_ignored() {
        let mut value = vec![0x08, 0x00];
        value.extend(varint((99 << 3) | 3));
        value.extend_from_slice(&[0x08, 0x01]);
        value.extend(varint((99 << 3) | 4));
        assert_eq!(super::value(&value), Ok(()));
    }
}
