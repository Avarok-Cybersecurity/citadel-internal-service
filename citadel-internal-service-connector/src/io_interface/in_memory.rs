use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
// The `log` FACADE, not citadel_logging (which wraps tracing). The WASM client
// installs console_log -- a log-facade logger -- and no tracing subscriber
// anywhere, so every `log::` macro here went nowhere in the browser. The
// dependency was already added for this fix; the `use` was never changed.
use futures::Sink;
use log;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct InMemoryInterface {
    pub sink: Option<citadel_io::tokio::sync::mpsc::UnboundedSender<InternalServicePayload>>,
    pub stream: Option<citadel_io::tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>>,
}

impl InMemoryInterface {
    pub fn from_request_response_pair(
        sink: citadel_io::tokio::sync::mpsc::UnboundedSender<InternalServiceRequest>,
        mut stream: citadel_io::tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    ) -> Self {
        let (tx_to_sink, mut rx_for_sink) =
            citadel_io::tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();
        let (tx_for_stream, rx_for_stream) =
            citadel_io::tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();
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
            citadel_io::tokio::select! {
                _ = sink_mapped_task => {},
                _ = stream_mapped_task => {}
            }
        };

        drop(citadel_io::tokio::spawn(task));

        Self {
            sink: Some(tx_to_sink),
            stream: Some(rx_for_stream),
        }
    }
}

/// Counterpart to the assertion in `websockets.rs`. In-process: the caller IS this process.
///
/// Compile-time, like that one: a `#[test]` here would sit behind a feature
/// the default CI invocation does not enable, and would pass by never
/// running.
const _: () = assert!(
    <InMemoryInterface as IOInterface>::CALLER_CAN_ALREADY_READ_LOCAL_FILES,
    "a native caller must keep the ability to name a file by path"
);

#[async_trait]
impl IOInterface for InMemoryInterface {
    type Sink = InMemorySink;
    type Stream = InMemoryStream;

    /// In-process: the caller IS this process, so it can read anything the
    /// agent can by definition.
    const CALLER_CAN_ALREADY_READ_LOCAL_FILES: bool = true;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        // This can only be called once
        if let Some((sink, stream)) = self.sink.take().zip(self.stream.take()) {
            Some((InMemorySink(sink), InMemoryStream(stream)))
        } else {
            futures::future::pending().await
        }
    }
}

pub struct InMemorySink(pub citadel_io::tokio::sync::mpsc::UnboundedSender<InternalServicePayload>);

impl Sink<InternalServicePayload> for InMemorySink {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: InternalServicePayload) -> Result<(), Self::Error> {
        self.0
            .send(item)
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub struct InMemoryStream(
    pub citadel_io::tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>,
);

impl futures::Stream for InMemoryStream {
    type Item = std::io::Result<InternalServicePayload>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().0.poll_recv(cx).map(|r| r.map(Ok))
    }
}
