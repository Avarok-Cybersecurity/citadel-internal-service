use crate::connector::ClientError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::Display;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

pub struct SerializingCodec<T> {
    pub inner: LengthDelimitedCodec,
    pub _pd: std::marker::PhantomData<T>,
}

#[derive(Debug)]
pub struct CodecError {
    pub reason: String,
}

impl Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl Error for CodecError {}

impl From<std::io::Error> for CodecError {
    fn from(value: std::io::Error) -> Self {
        Self {
            reason: format!("IO error: {:?}", value),
        }
    }
}

impl From<CodecError> for std::io::Error {
    fn from(value: CodecError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, value.reason)
    }
}

impl From<CodecError> for ClientError {
    fn from(value: CodecError) -> Self {
        ClientError::CodecError(value.reason)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(value: std::io::Error) -> Self {
        ClientError::SendError(value.to_string())
    }
}

impl<T: Serialize> Encoder<T> for SerializingCodec<T> {
    type Error = std::io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode2::serialize(&item)
            .map_err(|rr| CodecError {
                reason: format!("Failed to serialize value: {:?}", rr),
            })?
            .into();

        self.inner.encode(bytes, dst).map_err(|rr| CodecError {
            reason: format!("Failed to encode value: {:?}", rr),
        })?;

        Ok(())
    }
}

impl<T: DeserializeOwned> Decoder for SerializingCodec<T> {
    type Item = T;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let bytes = self.inner.decode(src).map_err(|rr| CodecError {
            reason: format!("Failed to decode value: {:?}", rr),
        })?;

        if let Some(bytes) = bytes {
            let value: T = bincode2::deserialize(&bytes).map_err(|rr| CodecError {
                reason: format!("Failed to deserialize value: {:?}", rr),
            })?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}
