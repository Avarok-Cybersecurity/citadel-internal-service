#[cfg(not(target_arch = "wasm32"))]
use crate::codec::SerializingCodec;
use crate::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::tokio::net::TcpStream;
#[cfg(not(target_arch = "wasm32"))]
use citadel_io::tokio_util::codec::{Decoder, Framed, LengthDelimitedCodec};
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct InternalServiceConnector<T: IOInterface> {
    pub sink: WrappedSink<T>,
    pub stream: WrappedStream<T>,
}

impl<T: IOInterface> InternalServiceConnector<T> {
    pub async fn from_io(mut io: T) -> Option<Self> {
        let (sink, stream) = io.next_connection().await?;
        Some(Self {
            sink: WrappedSink { inner: sink },
            stream: WrappedStream::new(stream),
        })
    }
}

pub struct WrappedStream<T: IOInterface> {
    pub inner: T::Stream,
    /// Consecutive items that were not a response, for the backstop in `poll_next`.
    undecodable_in_a_row: usize,
}

impl<T: IOInterface> WrappedStream<T> {
    /// The only way to build one, so the counter cannot be forgotten at a new
    /// site. There are three construction sites across two crates, and a struct
    /// literal at a fourth would have to re-decide what the field starts at.
    pub fn new(inner: T::Stream) -> Self {
        Self {
            inner,
            undecodable_in_a_row: 0,
        }
    }
}

pub struct WrappedSink<T: IOInterface> {
    pub inner: T::Sink,
}

impl<T: IOInterface> Stream for WrappedStream<T> {
    type Item = InternalServiceResponse;

    /// One unreadable frame is not the end of the stream.
    ///
    /// This was `_ => Poll::Ready(None)`, which collapsed three different things
    /// into "the peer hung up": a genuine end of stream, a single frame that
    /// failed to decode, and a `Request` arriving where a `Response` belongs.
    ///
    /// The middle one is reachable without anything being broken. There is no
    /// `#[serde(other)]` anywhere in the wire types, so an agent one release ahead
    /// of the client emits a variant the client cannot parse -- and that ended the
    /// messenger's inbound task, which the TypeScript client reads as "Stream
    /// closed", restarting the socket and clearing every messenger handle. One
    /// unknown frame per restart, dead after three.
    ///
    /// The WASM read loop already skips such items and keeps reading; this is the
    /// same decision one layer down, where it had not been made.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        /// Enough that a burst of version skew is survivable, few enough that a
        /// stream erroring without consuming cannot spin forever. A decoder that
        /// cannot read ANYTHING is a different fault from one bad frame, and this
        /// is where the two stop being treated the same.
        const GIVE_UP_AFTER: usize = 64;

        loop {
            let item = futures::ready!(self.inner.poll_next_unpin(cx));
            match item {
                Some(Ok(InternalServicePayload::Response(response))) => {
                    self.undecodable_in_a_row = 0;
                    return Poll::Ready(Some(response));
                }
                // The peer really did hang up.
                None => return Poll::Ready(None),
                other => {
                    self.undecodable_in_a_row += 1;
                    let seen = self.undecodable_in_a_row;
                    match other {
                        Some(Err(err)) => {
                            citadel_logging::warn!(target: "citadel", "[CONNECTOR] Skipping an undecodable frame ({seen} in a row): {err:?}");
                        }
                        _ => {
                            citadel_logging::warn!(target: "citadel", "[CONNECTOR] Skipping a payload that is not a response ({seen} in a row)");
                        }
                    }
                    if seen >= GIVE_UP_AFTER {
                        citadel_logging::error!(target: "citadel", "[CONNECTOR] {seen} consecutive unreadable frames; ending the stream. This is a decoder or version mismatch, not one bad message.");
                        return Poll::Ready(None);
                    }
                }
            }
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

#[cfg(not(target_arch = "wasm32"))]
pub fn wrap_tcp_conn(
    conn: citadel_io::tokio::net::TcpStream,
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
