use crate::codec::SerializingCodec;
use crate::connector::{wrap_tcp_conn, InternalServiceConnector, WrappedSink, WrappedStream};
use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{InternalServicePayload, InternalServiceResponse};
use citadel_io::tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use citadel_io::tokio_util::codec::Framed;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;

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

/// Counterpart to the assertion in `websockets.rs`. A native process runs as the user and can already open any file the agent
/// could, so refusing it a path protects nothing -- and the file-transfer
/// integration tests send by path over exactly this interface.
///
/// Compile-time, like that one: a `#[test]` here would sit behind a feature
/// the default CI invocation does not enable, and would pass by never
/// running.
const _: () = assert!(
    <TcpIOInterface as IOInterface>::CALLER_CAN_ALREADY_READ_LOCAL_FILES,
    "a native caller must keep the ability to name a file by path"
);

#[async_trait]
impl IOInterface for TcpIOInterface {
    type Sink = SplitSink<
        Framed<TcpStream, SerializingCodec<InternalServicePayload>>,
        InternalServicePayload,
    >;
    type Stream = SplitStream<Framed<TcpStream, SerializingCodec<InternalServicePayload>>>;

    /// A native process on this machine runs as the user and can already open
    /// any file the agent could, so refusing a path would protect nothing and
    /// would break the CLI and desktop contract.
    const CALLER_CAN_ALREADY_READ_LOCAL_FILES: bool = true;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        self.listener
            .accept()
            .await
            .ok()
            .map(|(stream, _)| wrap_tcp_conn(stream).split())
    }
}
