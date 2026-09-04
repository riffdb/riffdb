//! Strict bounded codec for generated client RPCs.

use std::marker::PhantomData;

use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic_prost::prost::Message;
use tonic_prost::prost::bytes::{Buf, BufMut};

use crate::{PublicMessage, decode_public_message, validate_public_message_encoded_len};

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
        let encoded_len = validate_public_message_encoded_len(&item)
            .map_err(|_| Status::internal("an internal error occurred"))?;
        if destination.remaining_mut() < encoded_len {
            return Err(Status::internal("an internal error occurred"));
        }
        Message::encode_raw(&item, destination);
        Ok(())
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
            return Err(Status::invalid_argument(INVALID_MESSAGE));
        }
        let bytes = source.copy_to_bytes(length);
        decode_public_message(bytes.as_ref())
            .map(Some)
            .map_err(|_| Status::invalid_argument(INVALID_MESSAGE))
    }
}
