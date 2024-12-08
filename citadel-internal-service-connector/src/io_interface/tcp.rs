use crate::codec::SerializingCodec;
use crate::connector::{wrap_tcp_conn, InternalServiceConnector, WrappedSink, WrappedStream};
use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{InternalServicePayload, InternalServiceResponse};
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

pub struct TcpIOInterface {
    pub listener: TcpListener,
}

impl TcpIOInterface {
    pub async fn new<T: ToSocketAddrs>(bind_address: T) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_address).await?;
        Ok(Self { listener })
    }
}

impl InternalServiceConnector<TcpIOInterface> {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = TcpStream::connect(addr).await?;
        let (sink, mut stream) = wrap_tcp_conn(conn).split();
        let greeter_packet = stream
            .next()
            .await
            .ok_or("Failed to receive greeting packet")??;
        if matches!(
            greeter_packet,
            InternalServicePayload::Response(
                InternalServiceResponse::ServiceConnectionAccepted { .. }
            )
        ) {
            let stream = WrappedStream { inner: stream };
            let sink = WrappedSink { inner: sink };
            Ok(Self { sink, stream })
        } else {
            Err("Failed to receive greeting packet")?
        }
    }

    pub fn split(self) -> (WrappedSink<TcpIOInterface>, WrappedStream<TcpIOInterface>) {
        (self.sink, self.stream)
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
