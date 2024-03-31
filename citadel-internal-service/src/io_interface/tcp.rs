use crate::io_interface::IOInterface;
use citadel_internal_service_connector::codec::SerializingCodec;
use citadel_internal_service_connector::util::wrap_tcp_conn;
use citadel_internal_service_types::InternalServicePayload;
use citadel_sdk::async_trait;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

pub struct TcpIOInterface {
    pub(crate) listener: TcpListener,
}

impl TcpIOInterface {
    pub async fn new<T: ToSocketAddrs>(bind_address: T) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_address).await?;
        Ok(Self { listener })
    }
}

#[async_trait]
impl IOInterface for TcpIOInterface {
    type Sink = SplitSink<
        Framed<TcpStream, SerializingCodec<InternalServicePayload>>,
        InternalServicePayload,
    >;
    type Stream = SplitStream<Framed<TcpStream, SerializingCodec<InternalServicePayload>>>;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        self.listener
            .accept()
            .await
            .ok()
            .map(|(stream, _)| wrap_tcp_conn(stream).split())
    }
}
