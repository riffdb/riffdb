//! Strict bounded codec used by every generated RiffDB RPC.

use std::marker::PhantomData;

#[cfg(feature = "server")]
use riffdb_errors::{ApplicationError, ApplicationErrorCode, ApplicationErrorContext};
#[cfg(feature = "server")]
use riffdb_proto::encode_application_error;
use riffdb_proto::{PublicMessage, decode_public_message, validate_public_message};
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
#[cfg(feature = "server")]
use tonic::codegen::Bytes;
use tonic_prost::prost::bytes::Buf;

const INVALID_MESSAGE: &str = "invalid protobuf message";

pub(crate) struct StrictProstCodec<Encode, Decode> {
    marker: PhantomData<(Encode, Decode)>,
}

impl<Encode, Decode> Default for StrictProstCodec<Encode, Decode> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<Encode, Decode> Codec for StrictProstCodec<Encode, Decode>
where
    Encode: PublicMessage + Send + 'static,
    Decode: PublicMessage + Send + 'static,
{
    type Encode = Encode;
    type Decode = Decode;
    type Encoder = StrictProstEncoder<Encode>;
    type Decoder = StrictProstDecoder<Decode>;

    fn encoder(&mut self) -> Self::Encoder {
        StrictProstEncoder {
            marker: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        StrictProstDecoder {
            marker: PhantomData,
        }
    }
}

pub(crate) struct StrictProstEncoder<MessageType> {
    marker: PhantomData<MessageType>,
}

impl<MessageType> Encoder for StrictProstEncoder<MessageType>
where
    MessageType: PublicMessage,
{
    type Item = MessageType;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        validate_public_message(&item)
            .map_err(|_| Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE))?;
        item.encode(destination)
            .map_err(|_| Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE))
    }
}

pub(crate) struct StrictProstDecoder<MessageType> {
    marker: PhantomData<MessageType>,
}

impl<MessageType> Decoder for StrictProstDecoder<MessageType>
where
    MessageType: PublicMessage,
{
    type Item = MessageType;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let length = source.remaining();
        if length > MessageType::MAX_ENCODED_BYTES {
            source.advance(length);
            return Err(invalid_message_status::<MessageType>());
        }
        let bytes = source.copy_to_bytes(length);
        decode_public_message(bytes.as_ref())
            .map(Some)
            .map_err(|_| invalid_message_status::<MessageType>())
    }
}

fn invalid_message_status<MessageType: PublicMessage>() -> Status {
    let _ = MessageType::APPLICATION_OPERATION;
    #[cfg(feature = "server")]
    if let Some(operation) = MessageType::APPLICATION_OPERATION {
        let error = ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            operation,
            ApplicationErrorContext::empty(),
            None,
        );
        return Status::with_details(
            tonic::Code::InvalidArgument,
            error.safe_message(),
            Bytes::from(encode_application_error(&error)),
        );
    }
    Status::invalid_argument(INVALID_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_proto::{app::v1 as app_v1, decode_application_error, v1};

    const _: () = {
        assert!(v1::ExecuteCommandRequest::MAX_ENCODED_BYTES > 0);
        assert!(v1::ExecuteCommandResponse::MAX_ENCODED_BYTES > 0);
    };

    #[test]
    fn strict_decoder_rejects_duplicate_singular_fields() {
        let bytes = [0x0a, 0x01, b'a', 0x0a, 0x01, b'b'];
        assert!(decode_public_message::<v1::ValidateContractRequest>(&bytes).is_err());
    }

    #[test]
    fn application_codec_rejection_uses_the_application_envelope() {
        let status = invalid_message_status::<app_v1::ExecuteQueryRequest>();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let error = decode_application_error(status.details()).expect("application error");
        assert_eq!(error.code(), ApplicationErrorCode::InvalidRequest);
        assert_eq!(
            error.operation(),
            riffdb_errors::ApplicationOperation::ExecuteQuery
        );
        assert_eq!(error.context().trace_id(), None);
    }
}
