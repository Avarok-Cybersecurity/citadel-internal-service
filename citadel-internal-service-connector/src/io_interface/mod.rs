use async_trait::async_trait;
use citadel_internal_service_types::InternalServicePayload;
use futures::{Sink, Stream};

pub mod in_memory;
pub mod tcp;

#[async_trait]
pub trait IOInterface: Sized + Send + 'static {
    type Sink: Sink<InternalServicePayload, Error = std::io::Error> + Unpin + Send + 'static;
    type Stream: Stream<Item = std::io::Result<InternalServicePayload>> + Unpin + Send + 'static;
    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)>;
}
