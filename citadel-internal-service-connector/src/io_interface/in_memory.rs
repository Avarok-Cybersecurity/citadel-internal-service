use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::InternalServicePayload;
use futures::Sink;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct InMemoryInterface {
    pub sink: Option<futures::channel::mpsc::UnboundedSender<InternalServicePayload>>,
    pub stream: Option<futures::channel::mpsc::UnboundedReceiver<InternalServicePayload>>,
}

#[async_trait]
impl IOInterface for InMemoryInterface {
    type Sink = InMemorySink;
    type Stream = InMemoryStream;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        // This can only be called once
        if let Some((sink, stream)) = self.sink.take().zip(self.stream.take()) {
            Some((InMemorySink(sink), InMemoryStream(stream)))
        } else {
            futures::future::pending().await
        }
    }
}

pub struct InMemorySink(pub futures::channel::mpsc::UnboundedSender<InternalServicePayload>);

impl Sink<InternalServicePayload> for InMemorySink {
    type Error = std::io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_ready(cx)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: InternalServicePayload,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.0)
            .start_send(item)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_flush(cx)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_close(cx)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }
}

pub struct InMemoryStream(pub futures::channel::mpsc::UnboundedReceiver<InternalServicePayload>);

impl futures::Stream for InMemoryStream {
    type Item = std::io::Result<InternalServicePayload>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
        }
    }
}
