use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::InternalServicePayload;
// The `log` FACADE, not citadel_logging (which wraps tracing). The WASM client
// installs console_log -- a log-facade logger -- and no tracing subscriber
// anywhere, so every `log::` macro here went nowhere in the browser. The
// dependency was already added for this fix; the `use` was never changed.
use futures::{Sink, SinkExt, Stream, StreamExt};
use log;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_interface::origin_policy::OriginPolicy;
use citadel_io::tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::handshake::server::{ErrorResponse, Request, Response},
    tungstenite::http::StatusCode,
    tungstenite::{Error as TungsteniteError, Message},
    WebSocketStream,
};

pub struct WebSocketInterface {
    listener: TcpListener,
    origins: OriginPolicy,
}

impl WebSocketInterface {
    /// Bind, admitting only handshakes `origins` permits.
    ///
    /// The policy is a required argument rather than an option with a default:
    /// the convenient default is `Any`, which is exactly the hole this closes,
    /// and a caller that has not thought about it should be made to.
    pub async fn new(addr: SocketAddr, origins: OriginPolicy) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, origins })
    }
}

/// Refuse the handshake unless its `Origin` is permitted.
///
/// This runs during the handshake, so a refused page gets an HTTP 403 and no
/// WebSocket at all — it never reaches `handle_request` and cannot be counted
/// as a connection.
// The Err type is tungstenite's `ErrorResponse`, fixed by the callback trait
// this closure has to satisfy. It cannot be boxed without failing to implement
// the trait, and it is constructed at most once per refused handshake.
#[allow(clippy::result_large_err)]
fn origin_check(
    origins: &OriginPolicy,
) -> impl FnOnce(&Request, Response) -> Result<Response, ErrorResponse> + '_ {
    move |request: &Request, response: Response| {
        // A header that is not valid UTF-8 is not an origin we listed, so it
        // is treated as present-and-unrecognised rather than as absent.
        let origin: Option<&str> = request
            .headers()
            .get("origin")
            .map(|value| value.to_str().unwrap_or("<invalid>"));

        if origins.permits(origin) {
            return Ok(response);
        }

        log::warn!(
            target: "citadel",
            "WebSocket handshake REFUSED for origin {:?}: not in the configured allowlist",
            origin.unwrap_or("<none>")
        );
        let mut refusal = ErrorResponse::new(Some(
            "origin not permitted by this agent's allowlist".to_string(),
        ));
        *refusal.status_mut() = StatusCode::FORBIDDEN;
        Err(refusal)
    }
}

/// The browser interface must never claim its caller can already read local
/// files: that constant is the whole of the `SendFile` path confinement, and
/// flipping it re-opens an arbitrary-file-read to page script in one character.
///
/// Asserted at COMPILE time, not in a test. The first version of this was a
/// `#[test]` behind `#[cfg(feature = "websockets")]` -- and the connector has
/// `default = []` while CI runs a bare `cargo nextest run`, so it was filtered
/// out of every run that mattered. It passed by never executing, which is the
/// failure mode this repository keeps finding. A `const` assertion fails the
/// BUILD of the very module whose behaviour it constrains, so it cannot be
/// skipped by a feature set or a test filter.
const _: () = assert!(
    !<WebSocketInterface as IOInterface>::CALLER_CAN_ALREADY_READ_LOCAL_FILES,
    "page script cannot read the filesystem; crediting it with that access \
     would let a SendFile name any path on the machine"
);

#[async_trait]
impl IOInterface for WebSocketInterface {
    type Sink = WebSocketSink;
    type Stream = WebSocketStream_;

    /// Script in a browser page cannot read the filesystem, so an accepted
    /// absolute path is a real escalation rather than a convenience. It keeps
    /// `PickFileRef` and `ByteContents`, which are both driven by a choice the
    /// user actually made.
    const CALLER_CAN_ALREADY_READ_LOCAL_FILES: bool = false;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    log::debug!(target: "citadel", "New WebSocket connection from {}", addr);

                    match accept_hdr_async(stream, origin_check(&self.origins)).await {
                        Ok(ws_stream) => {
                            let (sink, stream) = ws_stream.split();
                            return Some((
                                WebSocketSink { inner: sink },
                                WebSocketStream_ { inner: stream },
                            ));
                        }
                        Err(err) => {
                            log::error!(target: "citadel", "WebSocket handshake failed: {}", err);
                            continue;
                        }
                    }
                }
                Err(err) => {
                    log::error!(target: "citadel", "Failed to accept TCP connection: {}", err);
                    continue;
                }
            }
        }
    }
}

pub struct WebSocketSink {
    inner: futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl Sink<InternalServicePayload> for WebSocketSink {
    type Error = std::io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map_err(websocket_error_to_io_error)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: InternalServicePayload,
    ) -> Result<(), Self::Error> {
        let serialized = serde_json::to_string(&item)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let message = Message::Text(serialized);
        Pin::new(&mut self.inner)
            .start_send(message)
            .map_err(websocket_error_to_io_error)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(websocket_error_to_io_error)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(websocket_error_to_io_error)
    }
}

pub struct WebSocketStream_ {
    inner: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
}

impl Stream for WebSocketStream_ {
    type Item = std::io::Result<InternalServicePayload>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match futures::ready!(Pin::new(&mut self.inner).poll_next(cx)) {
            Some(Ok(Message::Text(data))) => {
                match serde_json::from_str::<InternalServicePayload>(&data) {
                    Ok(payload) => Poll::Ready(Some(Ok(payload))),
                    Err(e) => {
                        log::error!(target: "citadel", "Failed to deserialize WebSocket JSON message: {}", e);
                        Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        ))))
                    }
                }
            }
            Some(Ok(Message::Binary(data))) => {
                // Fallback: try to parse binary data as JSON string
                match std::str::from_utf8(&data) {
                    Ok(text) => match serde_json::from_str::<InternalServicePayload>(text) {
                        Ok(payload) => Poll::Ready(Some(Ok(payload))),
                        Err(e) => {
                            log::error!(target: "citadel", "Failed to deserialize WebSocket binary message as JSON: {}", e);
                            Poll::Ready(Some(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e,
                            ))))
                        }
                    },
                    Err(e) => {
                        log::error!(target: "citadel", "WebSocket binary message is not valid UTF-8: {}", e);
                        Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        ))))
                    }
                }
            }
            Some(Ok(Message::Close(_))) => {
                log::debug!(target: "citadel", "WebSocket connection closed");
                Poll::Ready(None)
            }
            Some(Ok(msg)) => {
                log::warn!(target: "citadel", "Unexpected WebSocket message type: {:?}", msg);
                // Skip non-text/binary messages and continue
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(e)) => {
                log::error!(target: "citadel", "WebSocket error: {}", e);
                Poll::Ready(Some(Err(websocket_error_to_io_error(e))))
            }
            None => {
                log::debug!(target: "citadel", "WebSocket stream ended");
                Poll::Ready(None)
            }
        }
    }
}

fn websocket_error_to_io_error(err: TungsteniteError) -> std::io::Error {
    match err {
        TungsteniteError::Io(io_err) => io_err,
        other => std::io::Error::other(other),
    }
}

// WebSocket client for testing
pub struct WebSocketClient {
    ws_stream: WebSocketStream<TcpStream>,
}

impl WebSocketClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        let url = format!("ws://{}/", addr);
        let (ws_stream, _) = tokio_tungstenite::client_async(url, stream).await?;
        Ok(Self { ws_stream })
    }

    pub async fn send(
        &mut self,
        payload: InternalServicePayload,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let serialized = serde_json::to_string(&payload)?;
        let message = Message::Text(serialized);
        self.ws_stream.send(message).await?;
        Ok(())
    }

    pub async fn send_json_string(
        &mut self,
        json_string: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::Text(json_string);
        self.ws_stream.send(message).await?;
        Ok(())
    }

    pub async fn receive(
        &mut self,
    ) -> Result<Option<InternalServicePayload>, Box<dyn std::error::Error>> {
        if let Some(message) = self.ws_stream.next().await {
            match message? {
                Message::Text(data) => {
                    let payload = serde_json::from_str::<InternalServicePayload>(&data)?;
                    Ok(Some(payload))
                }
                Message::Binary(data) => {
                    // Fallback: try to parse binary data as JSON string
                    let text = std::str::from_utf8(&data)?;
                    let payload = serde_json::from_str::<InternalServicePayload>(text)?;
                    Ok(Some(payload))
                }
                Message::Close(_) => Ok(None),
                _ => Ok(None), // Skip other message types
            }
        } else {
            Ok(None)
        }
    }

    pub async fn receive_json_string(
        &mut self,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if let Some(message) = self.ws_stream.next().await {
            match message? {
                Message::Text(data) => Ok(Some(data)),
                Message::Binary(data) => {
                    let text = std::str::from_utf8(&data)?;
                    Ok(Some(text.to_string()))
                }
                Message::Close(_) => Ok(None),
                _ => Ok(None), // Skip other message types
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_internal_service_types::{
        InternalServiceRequest, InternalServiceResponse, SecBuffer,
    };
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_websocket_interface() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let mut interface = WebSocketInterface::new(addr, OriginPolicy::Any)
            .await
            .unwrap();
        let bound_addr = interface.listener.local_addr().unwrap();

        // Spawn server task
        let server_task = tokio::spawn(async move {
            if let Some((mut sink, mut stream)) = interface.next_connection().await {
                // Echo received messages back
                while let Some(Ok(payload)) = stream.next().await {
                    log::info!(target: "citadel", "Server received: {:?}", payload);

                    // Echo back a response
                    let response = match payload {
                        InternalServicePayload::Request(InternalServiceRequest::Connect {
                            request_id,
                            ..
                        }) => InternalServicePayload::Response(
                            InternalServiceResponse::ConnectSuccess(
                                citadel_internal_service_types::ConnectSuccess {
                                    cid: 12345,
                                    request_id: Some(request_id),
                                },
                            ),
                        ),
                        _ => {
                            // Generic response for other requests
                            InternalServicePayload::Response(
                                InternalServiceResponse::ConnectSuccess(
                                    citadel_internal_service_types::ConnectSuccess {
                                        cid: 12345,
                                        request_id: None,
                                    },
                                ),
                            )
                        }
                    };

                    if let Err(e) = sink.send(response).await {
                        log::error!(target: "citadel", "Failed to send response: {}", e);
                        break;
                    }
                }
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client and connect
        let mut client = WebSocketClient::connect(bound_addr).await.unwrap();

        // Send a test message
        let request_id = Uuid::new_v4();
        let request = InternalServicePayload::Request(InternalServiceRequest::Connect {
            request_id,
            username: "test_user".to_string(),
            password: SecBuffer::from(b"password".to_vec()),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: Some(Duration::from_secs(30)),
            session_security_settings: Default::default(),
            server_password: None,
        });

        client.send(request).await.unwrap();

        // Receive response
        let response = client.receive().await.unwrap();
        assert!(response.is_some());

        match response.unwrap() {
            InternalServicePayload::Response(InternalServiceResponse::ConnectSuccess(success)) => {
                assert_eq!(success.cid, 12345);
                assert_eq!(success.request_id, Some(request_id));
            }
            _ => panic!("Expected ConnectSuccess response"),
        }

        // Clean up
        server_task.abort();
    }

    #[tokio::test]
    async fn test_websocket_json_format() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let mut interface = WebSocketInterface::new(addr, OriginPolicy::Any)
            .await
            .unwrap();
        let bound_addr = interface.listener.local_addr().unwrap();

        // Spawn server task that echoes JSON
        let server_task = tokio::spawn(async move {
            if let Some((mut sink, mut stream)) = interface.next_connection().await {
                if let Some(Ok(payload)) = stream.next().await {
                    log::info!(target: "citadel", "Server received JSON payload: {:?}", payload);

                    // Send back a simple response
                    let response =
                        InternalServicePayload::Response(InternalServiceResponse::ConnectSuccess(
                            citadel_internal_service_types::ConnectSuccess {
                                cid: 99999,
                                request_id: None,
                            },
                        ));

                    if let Err(e) = sink.send(response).await {
                        log::error!(target: "citadel", "Failed to send response: {}", e);
                    }
                }
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client and test JSON format
        let mut client = WebSocketClient::connect(bound_addr).await.unwrap();

        // Create a proper request payload and serialize it to JSON
        let request_payload = InternalServicePayload::Request(InternalServiceRequest::Connect {
            request_id: Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
            username: "frontend_user".to_string(),
            password: SecBuffer::from(b"password".to_vec()),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: Some(Duration::from_secs(30)),
            session_security_settings: Default::default(),
            server_password: None,
        });

        // Convert to JSON string to show what the frontend should send
        let json_payload = serde_json::to_string(&request_payload).unwrap();
        log::info!(target: "citadel", "Sending JSON payload: {}", json_payload);

        client.send_json_string(json_payload).await.unwrap();

        // Receive response as JSON string
        let response_json = client.receive_json_string().await.unwrap();
        assert!(response_json.is_some());

        let json_str = response_json.unwrap();
        log::info!(target: "citadel", "Received JSON response: {}", json_str);

        // Verify it's valid JSON and contains expected fields
        assert!(json_str.contains("\"Response\""));
        assert!(json_str.contains("\"ConnectSuccess\""));
        assert!(json_str.contains("\"cid\":99999"));

        // Also verify we can parse it back
        let parsed_response: InternalServicePayload = serde_json::from_str(&json_str).unwrap();
        match parsed_response {
            InternalServicePayload::Response(InternalServiceResponse::ConnectSuccess(success)) => {
                assert_eq!(success.cid, 99999);
            }
            _ => panic!("Expected ConnectSuccess response"),
        }

        // Clean up
        server_task.abort();
    }

    #[tokio::test]
    async fn test_websocket_multiple_messages() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let mut interface = WebSocketInterface::new(addr, OriginPolicy::Any)
            .await
            .unwrap();
        let bound_addr = interface.listener.local_addr().unwrap();

        // Spawn server task that handles multiple messages
        let server_task = tokio::spawn(async move {
            if let Some((mut sink, mut stream)) = interface.next_connection().await {
                let mut message_count = 0;

                while let Some(Ok(payload)) = stream.next().await {
                    message_count += 1;
                    log::info!(target: "citadel", "Server received message {}: {:?}", message_count, payload);

                    // Send back a response with the message count
                    let response =
                        InternalServicePayload::Response(InternalServiceResponse::ConnectSuccess(
                            citadel_internal_service_types::ConnectSuccess {
                                cid: message_count,
                                request_id: None,
                            },
                        ));

                    if let Err(e) = sink.send(response).await {
                        log::error!(target: "citadel", "Failed to send response: {}", e);
                        break;
                    }

                    // Stop after 3 messages
                    if message_count >= 3 {
                        break;
                    }
                }
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client and send multiple messages
        let mut client = WebSocketClient::connect(bound_addr).await.unwrap();

        for i in 1..=3 {
            let request = InternalServicePayload::Request(InternalServiceRequest::Connect {
                request_id: Uuid::new_v4(),
                username: format!("test_user_{}", i),
                password: SecBuffer::from(b"password".to_vec()),
                connect_mode: Default::default(),
                udp_mode: Default::default(),
                keep_alive_timeout: Some(Duration::from_secs(30)),
                session_security_settings: Default::default(),
                server_password: None,
            });

            client.send(request).await.unwrap();

            // Receive response
            let response = client.receive().await.unwrap();
            assert!(response.is_some());

            match response.unwrap() {
                InternalServicePayload::Response(InternalServiceResponse::ConnectSuccess(
                    success,
                )) => {
                    assert_eq!(success.cid, i);
                }
                _ => panic!("Expected ConnectSuccess response"),
            }
        }

        // Clean up
        server_task.abort();
    }

    #[tokio::test]
    async fn test_websocket_connection_close() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let mut interface = WebSocketInterface::new(addr, OriginPolicy::Any)
            .await
            .unwrap();
        let bound_addr = interface.listener.local_addr().unwrap();

        // Spawn server task
        let server_task = tokio::spawn(async move {
            if let Some((mut sink, mut stream)) = interface.next_connection().await {
                // Wait for one message then close
                if let Some(Ok(_payload)) = stream.next().await {
                    log::info!(target: "citadel", "Server received message, closing connection");
                    let _ = sink.close().await;
                }
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client and send a message
        let mut client = WebSocketClient::connect(bound_addr).await.unwrap();

        let request = InternalServicePayload::Request(InternalServiceRequest::Connect {
            request_id: Uuid::new_v4(),
            username: "test_user".to_string(),
            password: SecBuffer::from(b"password".to_vec()),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: Some(Duration::from_secs(30)),
            session_security_settings: Default::default(),
            server_password: None,
        });

        client.send(request).await.unwrap();

        // The connection should be closed by the server
        let response = client.receive().await.unwrap();
        assert!(response.is_none(), "Expected connection to be closed");

        // Clean up
        server_task.abort();
    }

    /// The unit tests in `origin_policy` prove the DECISION. These prove the
    /// decision is WIRED to the handshake — a policy nothing consults is the
    /// classic control that operates on nothing.
    ///
    /// Both use a real TCP handshake against a real listener, because that is
    /// the only place `Origin` exists.
    mod origin_enforcement {
        use super::*;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        async fn listener_allowing(spec: &str) -> (WebSocketInterface, SocketAddr) {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let interface = WebSocketInterface::new(addr, OriginPolicy::parse(spec).unwrap())
                .await
                .unwrap();
            let bound = interface.listener.local_addr().unwrap();
            (interface, bound)
        }

        /// Connect with an explicit `Origin`, returning whether the handshake
        /// completed. Nothing else about the connection is asserted: what is
        /// under test is admission.
        async fn handshake_with_origin(bound: SocketAddr, origin: &str) -> bool {
            let mut request = format!("ws://{bound}").into_client_request().unwrap();
            request
                .headers_mut()
                .insert("Origin", origin.parse().unwrap());
            tokio_tungstenite::connect_async(request).await.is_ok()
        }

        #[tokio::test]
        async fn a_listed_origin_completes_the_handshake() {
            let (mut interface, bound) = listener_allowing("http://localhost:5291").await;
            let server = tokio::spawn(async move { interface.next_connection().await.is_some() });
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert!(
                handshake_with_origin(bound, "http://localhost:5291").await,
                "the configured origin was refused"
            );
            assert!(
                tokio::time::timeout(Duration::from_secs(5), server)
                    .await
                    .expect("the accept loop should have yielded a connection")
                    .unwrap(),
                "the handshake succeeded but no connection reached the service"
            );
        }

        /// A page the user merely visited. Before the check, this handshake
        /// completed and the page could then call `GetSessions`.
        #[tokio::test]
        async fn an_unlisted_origin_is_refused_at_the_handshake() {
            let (mut interface, bound) = listener_allowing("http://localhost:5291").await;
            // The accept loop `continue`s past a failed handshake, so it must
            // still be pending afterwards — proving the refusal happened before
            // any connection was produced, not after.
            let server = tokio::spawn(async move { interface.next_connection().await.is_some() });
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert!(
                !handshake_with_origin(bound, "https://evil.example").await,
                "a hostile origin completed the handshake"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(500), server)
                    .await
                    .is_err(),
                "the refused handshake still produced a connection"
            );
        }
    }
}
