use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use citadel_logging::tracing::log;
use futures::Sink;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct InMemoryInterface {
    pub sink: Option<tokio::sync::mpsc::UnboundedSender<InternalServicePayload>>,
    pub stream: Option<tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>>,
}

impl InMemoryInterface {
    pub fn from_request_response_pair(
        sink: tokio::sync::mpsc::UnboundedSender<InternalServiceRequest>,
        mut stream: tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    ) -> Self {
        let (tx_to_sink, mut rx_for_sink) =
            tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();
        let (tx_for_stream, rx_for_stream) =
            tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();
        let sink_mapped_task = async move {
            while let Some(InternalServicePayload::Request(outbound)) = rx_for_sink.recv().await {
                if let Err(err) = sink.send(outbound) {
                    log::error!(target: "citadel", "Error sending to sink: {:?}", err);
                    return;
                }
            }

            log::error!(target: "citadel", "Sink mapped channel closed");
        };

        let stream_mapped_task = async move {
            while let Some(response) = stream.recv().await {
                if let Err(err) = tx_for_stream.send(InternalServicePayload::Response(response)) {
                    log::error!(target: "citadel", "Error sending to sink: {:?}", err);
                    return;
                }
            }

            log::error!(target: "citadel", "Stream mapped channel closed");
        };

        let task = async move {
            tokio::select! {
                _ = sink_mapped_task => {},
                _ = stream_mapped_task => {}
            }
        };

        drop(tokio::spawn(task));

        Self {
            sink: Some(tx_to_sink),
            stream: Some(rx_for_stream),
        }
    }
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

pub struct InMemorySink(pub tokio::sync::mpsc::UnboundedSender<InternalServicePayload>);

impl Sink<InternalServicePayload> for InMemorySink {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: InternalServicePayload) -> Result<(), Self::Error> {
        self.0
            .send(item)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub struct InMemoryStream(pub tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>);

impl futures::Stream for InMemoryStream {
    type Item = std::io::Result<InternalServicePayload>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().0.poll_recv(cx).map(|r| r.map(Ok))
    }
}
