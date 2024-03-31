use crate::codec::SerializingCodec;
use crate::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed, LengthDelimitedCodec};

pub struct InternalServiceConnector<T: IOInterface> {
    pub sink: WrappedSink<T>,
    pub stream: WrappedStream<T>,
}

pub struct WrappedStream<T: IOInterface> {
    pub inner: T::Stream,
}

pub struct WrappedSink<T: IOInterface> {
    pub inner: T::Sink,
}

impl<T: IOInterface> Stream for WrappedStream<T> {
    type Item = InternalServiceResponse;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = futures::ready!(self.inner.poll_next_unpin(cx));
        match item {
            Some(Ok(InternalServicePayload::Response(response))) => Poll::Ready(Some(response)),

            _ => Poll::Ready(None),
        }
    }
}

impl<T: IOInterface> Sink<InternalServiceRequest> for WrappedSink<T> {
    type Error = std::io::Error;

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
