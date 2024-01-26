use crate::codec::{CodecError, SerializingCodec};
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::{Decoder, Framed, LengthDelimitedCodec};

pub struct InternalServiceConnector {
    pub sink: WrappedSink,
    pub stream: WrappedStream,
}

pub struct WrappedStream {
    inner: SplitStream<Framed<TcpStream, SerializingCodec<InternalServicePayload>>>,
}

pub struct WrappedSink {
    inner: SplitSink<
        Framed<TcpStream, SerializingCodec<InternalServicePayload>>,
        InternalServicePayload,
    >,
}

impl InternalServiceConnector {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = TcpStream::connect(addr).await?;
        let (sink, mut stream) = wrap_tcp_conn(conn).split();
        let greeter_packet = stream
            .next()
            .await
            .ok_or("Failed to receive greeting packet")??;
        if matches!(
            greeter_packet,
            InternalServicePayload::Response(InternalServiceResponse::ServiceConnectionAccepted(_))
        ) {
            let stream = WrappedStream { inner: stream };
            let sink = WrappedSink { inner: sink };
            Ok(Self { sink, stream })
        } else {
            Err("Failed to receive greeting packet")?
        }
    }

    pub fn split(self) -> (WrappedSink, WrappedStream) {
        (self.sink, self.stream)
    }
}

impl Stream for WrappedStream {
    type Item = InternalServiceResponse;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = futures::ready!(self.inner.poll_next_unpin(cx));
        match item {
            Some(Ok(InternalServicePayload::Response(response))) => Poll::Ready(Some(response)),

            _ => Poll::Ready(None),
        }
    }
}

impl Sink<InternalServiceRequest> for WrappedSink {
    type Error = CodecError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: InternalServiceRequest,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(InternalServicePayload::Request(item))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

pub fn wrap_tcp_conn(
    conn: TcpStream,
) -> Framed<TcpStream, SerializingCodec<InternalServicePayload>> {
    let length_delimited = LengthDelimitedCodec::builder()
        .length_field_offset(0) // default value
        .max_frame_length(1024 * 1024 * 64) // 64 MB
        .length_field_type::<u32>()
        .length_adjustment(0)
        .new_codec();

    let serializing_codec = SerializingCodec {
        inner: length_delimited,
        _pd: std::marker::PhantomData,
    };
    serializing_codec.framed(conn)
}
