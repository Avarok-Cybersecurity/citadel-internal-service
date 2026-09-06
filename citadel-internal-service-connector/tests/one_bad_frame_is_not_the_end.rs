//! One unreadable frame must not end the inbound stream.
//!
//! `WrappedStream::poll_next` was `_ => Poll::Ready(None)`, which collapsed three
//! different things into "the peer hung up":
//!
//!   1. a genuine end of stream,
//!   2. a single frame that failed to decode,
//!   3. a `Request` arriving where a `Response` belongs.
//!
//! The second is reachable without anything being broken. There is no
//! `#[serde(other)]` anywhere in the wire types, so an agent one release ahead of
//! the client emits a variant the client cannot parse. That ended the messenger's
//! inbound task, which the TypeScript client reads as "Stream closed" — restarting
//! the socket and clearing every messenger handle. One unknown frame per restart,
//! dead after three, and nothing in the log said a frame had been dropped.
//!
//! The test double below implements the real `IOInterface`, so what is exercised
//! is the real `WrappedStream` over a real `Stream` — the only thing invented is a
//! stream that can yield the items a live one yields.

use citadel_internal_service_connector::connector::WrappedStream;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse, MessageSendFailure,
};
use futures::{Sink, Stream, StreamExt};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

/// A stream of exactly the items handed to it, in order.
struct Scripted(std::vec::IntoIter<io::Result<InternalServicePayload>>);

impl Stream for Scripted {
    type Item = io::Result<InternalServicePayload>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.next())
    }
}

/// Never used; `WrappedStream` needs an interface only for its associated types.
struct NullSink;

impl Sink<InternalServicePayload> for NullSink {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _: InternalServicePayload) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct ScriptedInterface;

// `#[async_trait]`, as every other impl of this trait uses — the trait is declared
// with it, so a bare `async fn` here fails on lifetime elision.
#[async_trait::async_trait]
impl IOInterface for ScriptedInterface {
    type Sink = NullSink;
    type Stream = Scripted;
    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        None
    }
    const CALLER_CAN_ALREADY_READ_LOCAL_FILES: bool = false;
}

fn a_response(message: &str) -> InternalServicePayload {
    InternalServicePayload::Response(InternalServiceResponse::MessageSendFailure(
        MessageSendFailure {
            cid: 1,
            message: message.to_string(),
            request_id: Some(Uuid::nil()),
        },
    ))
}

/// A frame the decoder could not read — the version-skew case.
fn undecodable() -> io::Result<InternalServicePayload> {
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unknown variant `SomethingFromANewerAgent`",
    ))
}

/// A request arriving on the response stream — protocol confusion, not an ending.
fn a_request() -> InternalServicePayload {
    InternalServicePayload::Request(InternalServiceRequest::GetSessions {
        request_id: Uuid::nil(),
    })
}

fn drain(items: Vec<io::Result<InternalServicePayload>>) -> Vec<String> {
    let stream: WrappedStream<ScriptedInterface> = WrappedStream::new(Scripted(items.into_iter()));
    futures::executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .map(|response| match response {
            InternalServiceResponse::MessageSendFailure(f) => f.message,
            other => format!("unexpected: {other:?}"),
        })
        .collect()
}

#[test]
fn an_undecodable_frame_is_skipped_and_the_rest_arrive() {
    let seen = drain(vec![
        Ok(a_response("before")),
        undecodable(),
        Ok(a_response("after")),
    ]);

    assert_eq!(
        seen,
        vec!["before".to_string(), "after".to_string()],
        "a frame the decoder could not read ended the stream, so everything after it was lost"
    );
}

#[test]
fn a_request_on_the_response_stream_is_skipped_too() {
    let seen = drain(vec![
        Ok(a_response("before")),
        Ok(a_request()),
        Ok(a_response("after")),
    ]);

    assert_eq!(seen, vec!["before".to_string(), "after".to_string()]);
}

#[test]
fn the_end_of_the_stream_is_still_the_end() {
    // The control. A `poll_next` that skipped everything and never returned
    // `None` would satisfy both tests above and hang every consumer instead.
    let seen = drain(vec![Ok(a_response("only"))]);

    assert_eq!(seen, vec!["only".to_string()]);
}

#[test]
fn a_decoder_that_reads_nothing_at_all_still_ends() {
    // The other control, and the reason for the consecutive-error cap: skipping
    // forever turns a broken decoder — a genuinely different fault from one bad
    // message — into a consumer that waits and never learns why.
    let flood: Vec<_> = (0..200).map(|_| undecodable()).collect();

    let seen = drain(flood);

    assert!(
        seen.is_empty(),
        "nothing decodable was sent, so nothing should arrive"
    );
}
