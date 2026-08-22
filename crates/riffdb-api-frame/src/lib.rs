#![forbid(unsafe_code)]

//! Bounded transport framing for exact generated application operations.

use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_proto::{
    MAX_PUBLIC_REQUEST_BYTES, MAX_PUBLIC_RESPONSE_BYTES, PublicMessage, decode_public_message, v1,
};

/// Exact eight-byte preface and frame magic for protocol version 1.
pub const FRAME_MAGIC_V1: [u8; 8] = *b"RIFFDBF1";
/// Exact protocol version carried independently from the magic.
pub const FRAME_PROTOCOL_V1: u8 = 1;
/// Fixed version-1 header length.
pub const FRAME_HEADER_BYTES: usize = 24;
/// Maximum accepted frame payload across both directions.
pub const MAX_FRAME_PAYLOAD_BYTES: usize = MAX_PUBLIC_RESPONSE_BYTES;

/// Closed direction and payload family carried by one frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameKind {
    /// One strict [`v1::ApplicationSessionRequest`].
    Request = 1,
    /// One strict [`v1::ApplicationSessionResponse`].
    Response = 2,
}

impl FrameKind {
    fn from_byte(value: u8) -> Result<Self, FrameError> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            _ => Err(FrameError::UnknownKind),
        }
    }

    const fn maximum_payload_bytes(self) -> usize {
        match self {
            Self::Request => MAX_PUBLIC_REQUEST_BYTES,
            Self::Response => MAX_PUBLIC_RESPONSE_BYTES,
        }
    }
}

/// One fully bounded, structurally checked transport frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    kind: FrameKind,
    correlation_id: u64,
    payload: Vec<u8>,
}

impl Frame {
    /// Encodes one already strict public session request.
    pub fn request(value: &v1::ApplicationSessionRequest) -> Result<Self, FrameError> {
        Self::from_public(FrameKind::Request, value.correlation_id, value)
    }

    /// Encodes one already strict public session response.
    pub fn response(value: &v1::ApplicationSessionResponse) -> Result<Self, FrameError> {
        Self::from_public(FrameKind::Response, value.correlation_id, value)
    }

    fn from_public<M: PublicMessage + Message>(
        kind: FrameKind,
        correlation_id: u64,
        value: &M,
    ) -> Result<Self, FrameError> {
        if correlation_id == 0 {
            return Err(FrameError::ZeroCorrelation);
        }
        value
            .validate_structure()
            .map_err(|_| FrameError::InvalidPayload)?;
        let mut payload = Vec::with_capacity(value.encoded_len());
        value
            .encode(&mut payload)
            .map_err(|_| FrameError::InvalidPayload)?;
        if payload.len() > kind.maximum_payload_bytes() {
            return Err(FrameError::PayloadTooLarge);
        }
        Ok(Self {
            kind,
            correlation_id,
            payload,
        })
    }

    /// Returns the closed payload kind.
    #[must_use]
    pub const fn kind(&self) -> FrameKind {
        self.kind
    }

    /// Returns the nonzero connection-scoped correlation identity.
    #[must_use]
    pub const fn correlation_id(&self) -> u64 {
        self.correlation_id
    }

    /// Returns the exact strict-Protobuf payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the exact encoded frame length.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        FRAME_HEADER_BYTES + self.payload.len()
    }

    /// Appends the canonical version-1 frame bytes.
    pub fn encode(&self, output: &mut Vec<u8>) {
        output.reserve(self.encoded_len());
        output.extend_from_slice(&FRAME_MAGIC_V1);
        output.push(FRAME_PROTOCOL_V1);
        output.push(self.kind as u8);
        output.extend_from_slice(&0_u16.to_be_bytes());
        output.extend_from_slice(&self.correlation_id.to_be_bytes());
        output.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        output.extend_from_slice(&self.payload);
    }

    /// Decodes exactly one complete canonical frame.
    pub fn decode_exact(input: &[u8]) -> Result<Self, FrameError> {
        if input.len() < FRAME_HEADER_BYTES {
            return Err(FrameError::Truncated);
        }
        if input[..8] != FRAME_MAGIC_V1 {
            return Err(FrameError::InvalidMagic);
        }
        if input[8] != FRAME_PROTOCOL_V1 {
            return Err(FrameError::UnsupportedVersion);
        }
        let kind = FrameKind::from_byte(input[9])?;
        if input[10..12] != [0, 0] {
            return Err(FrameError::NonzeroFlags);
        }
        let correlation_id = u64::from_be_bytes(
            input[12..20]
                .try_into()
                .map_err(|_| FrameError::Truncated)?,
        );
        if correlation_id == 0 {
            return Err(FrameError::ZeroCorrelation);
        }
        let payload_len = u32::from_be_bytes(
            input[20..24]
                .try_into()
                .map_err(|_| FrameError::Truncated)?,
        ) as usize;
        if payload_len > kind.maximum_payload_bytes() {
            return Err(FrameError::PayloadTooLarge);
        }
        let expected = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or(FrameError::PayloadTooLarge)?;
        if input.len() < expected {
            return Err(FrameError::Truncated);
        }
        if input.len() != expected {
            return Err(FrameError::TrailingBytes);
        }
        let frame = Self {
            kind,
            correlation_id,
            payload: input[FRAME_HEADER_BYTES..].to_vec(),
        };
        frame.validate_payload()?;
        Ok(frame)
    }

    /// Decodes and validates the request payload and correlation binding.
    pub fn decode_request(&self) -> Result<v1::ApplicationSessionRequest, FrameError> {
        if self.kind != FrameKind::Request {
            return Err(FrameError::DirectionMismatch);
        }
        let value = decode_public_message::<v1::ApplicationSessionRequest>(&self.payload)
            .map_err(|_| FrameError::InvalidPayload)?;
        if value.correlation_id != self.correlation_id {
            return Err(FrameError::CorrelationMismatch);
        }
        Ok(value)
    }

    /// Decodes and validates the response payload and correlation binding.
    pub fn decode_response(&self) -> Result<v1::ApplicationSessionResponse, FrameError> {
        if self.kind != FrameKind::Response {
            return Err(FrameError::DirectionMismatch);
        }
        let value = decode_public_message::<v1::ApplicationSessionResponse>(&self.payload)
            .map_err(|_| FrameError::InvalidPayload)?;
        if value.correlation_id != self.correlation_id {
            return Err(FrameError::CorrelationMismatch);
        }
        Ok(value)
    }

    fn validate_payload(&self) -> Result<(), FrameError> {
        match self.kind {
            FrameKind::Request => self.decode_request().map(|_| ()),
            FrameKind::Response => self.decode_response().map(|_| ()),
        }
    }
}

/// Closed, value-free framed-protocol failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameError {
    /// The fixed protocol magic did not match.
    InvalidMagic,
    /// The protocol version is unknown.
    UnsupportedVersion,
    /// The closed frame kind is unknown.
    UnknownKind,
    /// A reserved flag bit was nonzero.
    NonzeroFlags,
    /// Correlation identities are always nonzero.
    ZeroCorrelation,
    /// The declared payload exceeds its directional ceiling.
    PayloadTooLarge,
    /// The input ended before the declared complete frame.
    Truncated,
    /// Bytes remained after the declared complete frame.
    TrailingBytes,
    /// The strict public Protobuf payload was invalid.
    InvalidPayload,
    /// Header and payload correlation identities differed.
    CorrelationMismatch,
    /// A request was decoded as a response or vice versa.
    DirectionMismatch,
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid bounded application frame")
    }
}

impl Error for FrameError {}
