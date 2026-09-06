use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::InternalServicePayload;
// The `log` FACADE, not citadel_logging (which wraps tracing). The WASM client
// installs console_log -- a log-facade logger -- and no tracing subscriber
// anywhere, so every `log::` macro here went nowhere in the browser. The
// dependency was already added for this fix; the `use` was never changed.
use citadel_io::tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use futures::{Sink, SinkExt, Stream, StreamExt};
use log;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::io_interface::origin_policy::OriginPolicy;
use citadel_io::tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use citadel_io::tokio::net::{TcpListener, TcpStream};
use std::io;
use std::sync::Arc;
use tokio_rustls::{rustls, TlsAcceptor};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::handshake::server::{ErrorResponse, Request, Response},
    tungstenite::http::StatusCode,
    tungstenite::{Error as TungsteniteError, Message},
    WebSocketStream,
};

/// What the WebSocket runs over: a bare socket, or one wrapped in TLS.
///
/// The agent needs both. A UI served from the same machine reaches it over
/// loopback, where plain is right and a certificate would be ceremony. A HOSTED
/// UI cannot: the page is HTTPS, and a browser refuses to open a `ws://` socket
/// from an HTTPS page as mixed content. `wss://` is not a preference there, it
/// is the only thing the browser will do.
///
/// An enum rather than a generic parameter because the sink and stream types
/// are named in `IOInterface`'s associated types; making them generic would
/// spread a parameter through the whole interface for a choice made once, at
/// bind time.
pub enum AgentTransport {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl AsyncRead for AgentTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AgentTransport::Plain(inner) => Pin::new(inner).poll_read(cx, buf),
            AgentTransport::Tls(inner) => Pin::new(inner.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AgentTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            AgentTransport::Plain(inner) => Pin::new(inner).poll_write(cx, buf),
            AgentTransport::Tls(inner) => Pin::new(inner.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AgentTransport::Plain(inner) => Pin::new(inner).poll_flush(cx),
            AgentTransport::Tls(inner) => Pin::new(inner.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AgentTransport::Plain(inner) => Pin::new(inner).poll_shutdown(cx),
            AgentTransport::Tls(inner) => Pin::new(inner.as_mut()).poll_shutdown(cx),
        }
    }
}

pub struct WebSocketInterface {
    /// Taken when the accept loop starts, so it can move into that task.
    listener: Option<TcpListener>,
    origins: OriginPolicy,
    /// `Some` when the listener serves TLS. See `AgentTransport`.
    tls: Option<TlsAcceptor>,
    /// Completed handshakes, in the order they finished.
    incoming: Option<UnboundedReceiver<(WebSocketSink, WebSocketStream_)>>,
}

/// How long a peer may take to finish the WebSocket upgrade.
///
/// A browser completes it in milliseconds. This is generous for a loaded
/// machine and still bounded, which is the whole point: the handshake used to
/// have no limit at all.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

impl WebSocketInterface {
    /// Bind, admitting only handshakes `origins` permits.
    ///
    /// The policy is a required argument rather than an option with a default:
    /// the convenient default is `Any`, which is exactly the hole this closes,
    /// and a caller that has not thought about it should be made to.
    pub async fn new(addr: SocketAddr, origins: OriginPolicy) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener: Some(listener),
            origins,
            tls: None,
            incoming: None,
        })
    }

    /// Bind and serve `wss://`, presenting `certificate_chain` for `private_key`.
    ///
    /// Both are PEM. The chain is leaf-first, as every ACME client writes it;
    /// the key is PKCS#8 or the older RSA form.
    ///
    /// This exists because a hosted page cannot reach a plain-WebSocket agent at
    /// all. `work.avarok.net` published `wss://local.avarok.net:12345` as the
    /// agent origin and nothing terminated TLS there, so every visitor got
    /// `ERR_SSL_PROTOCOL_ERROR` and the app never started.
    pub async fn new_tls(
        addr: SocketAddr,
        origins: OriginPolicy,
        certificate_chain: &[u8],
        private_key: &[u8],
    ) -> std::io::Result<Self> {
        let config = tls_config(certificate_chain, private_key)?;
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener: Some(listener),
            origins,
            tls: Some(TlsAcceptor::from(Arc::new(config))),
            incoming: None,
        })
    }

    /// The bound address, while the listener has not yet been moved into the
    /// accept task. Tests bind port 0 and need to learn what they got.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener
            .as_ref()
            .ok_or_else(|| std::io::Error::other("the listener has moved into the accept task"))?
            .local_addr()
    }
}

/// Build a rustls server config from PEM bytes.
///
/// Every failure is reported with what was wrong, because the alternative is an
/// agent that exits with "invalid certificate" and leaves the operator guessing
/// which of the two files it meant.
fn tls_config(
    certificate_chain: &[u8],
    private_key: &[u8],
) -> std::io::Result<rustls::ServerConfig> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut &certificate_chain[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("certificate chain is not valid PEM: {e}"),
            )
        })?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certificate chain contained no CERTIFICATE blocks",
        ));
    }

    let key = rustls_pemfile::private_key(&mut &private_key[..])
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("private key is not valid PEM: {e}"),
            )
        })?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "private key contained no PRIVATE KEY block",
            )
        })?;

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        // The commonest cause is a key that does not match the chain, and
        // rustls says so; passing its message through saves an hour.
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("certificate and key do not form a usable pair: {e}"),
            )
        })
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

    /// The next connection whose handshake has COMPLETED.
    ///
    /// The handshake used to be awaited here, inline, before this function
    /// returned -- and the only caller is a serial
    /// `while let Some(..) = io.next_connection().await` loop. So one local
    /// process that opened a TCP connection to the agent and sent nothing
    /// parked the accept loop FOREVER: every later tab, reload or new account
    /// got a socket that never completed, and nothing appeared in the log,
    /// because no bytes ever reached `handle_request`. A suspended laptop's
    /// half-open TCP or a port scanner did it by accident; anything on the
    /// machine could do it on purpose, and the agent holds decrypted P2P
    /// plaintext.
    ///
    /// Two changes, and BOTH are needed. Spawning each handshake means a stalled
    /// one no longer blocks the others -- but without a bound, stalled sockets
    /// accumulate, so `HANDSHAKE_TIMEOUT` closes them. And a timeout alone would
    /// not have been enough either: with the handshake still inline, repeated
    /// connect-and-stall would occupy the loop continuously, one timeout at a
    /// time.
    ///
    /// Origin enforcement is unchanged: `origin_check` still runs inside the
    /// handshake, so a refused page gets a 403 and never becomes a connection.
    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        if self.incoming.is_none() {
            let listener = self.listener.take()?;
            let origins = self.origins.clone();
            let tls = self.tls.clone();
            let (tx, rx) = unbounded_channel();
            self.incoming = Some(rx);

            // The JoinHandle is dropped on purpose: the acceptor lives for the
            // life of the process, and a per-handshake task cleans itself up.
            drop(citadel_io::tokio::task::spawn(async move {
                loop {
                    let (stream, addr) = match listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(err) => {
                            log::error!(target: "citadel", "Failed to accept TCP connection: {}", err);
                            continue;
                        }
                    };
                    log::debug!(target: "citadel", "New WebSocket connection from {}", addr);

                    let origins = origins.clone();
                    let tx = tx.clone();
                    let tls = tls.clone();
                    drop(citadel_io::tokio::task::spawn(async move {
                        // The TLS handshake is inside the SAME timeout as the
                        // WebSocket upgrade, and inside the same spawned task.
                        // A peer that opens a socket and sends no ClientHello is
                        // exactly the stall the timeout was added for; putting
                        // TLS outside it would reopen that hole one layer down.
                        let upgrade = async {
                            let transport = match tls {
                                Some(acceptor) => AgentTransport::Tls(Box::new(
                                    acceptor
                                        .accept(stream)
                                        .await
                                        .map_err(TungsteniteError::Io)?,
                                )),
                                None => AgentTransport::Plain(stream),
                            };
                            accept_hdr_async(transport, origin_check(&origins)).await
                        };
                        match citadel_io::tokio::time::timeout(HANDSHAKE_TIMEOUT, upgrade).await {
                            Ok(Ok(ws_stream)) => {
                                let (sink, stream) = ws_stream.split();
                                let _ = tx.send((
                                    WebSocketSink { inner: sink },
                                    WebSocketStream_ { inner: stream },
                                ));
                            }
                            Ok(Err(err)) => {
                                log::error!(target: "citadel", "WebSocket handshake failed: {}", err);
                            }
                            Err(_elapsed) => {
                                log::warn!(
                                    target: "citadel",
                                    "WebSocket handshake from {} did not complete within {:?}; closing",
                                    addr,
                                    HANDSHAKE_TIMEOUT
                                );
                            }
                        }
                    }));
                }
            }));
        }

        self.incoming.as_mut()?.recv().await
    }
}

pub struct WebSocketSink {
    inner: futures_util::stream::SplitSink<WebSocketStream<AgentTransport>, Message>,
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
    inner: futures_util::stream::SplitStream<WebSocketStream<AgentTransport>>,
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
        let bound_addr = interface.local_addr().unwrap();

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
        let bound_addr = interface.local_addr().unwrap();

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
        let bound_addr = interface.local_addr().unwrap();

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
        let bound_addr = interface.local_addr().unwrap();

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
    /// A socket that connects and says nothing must not stop anyone else.
    ///
    /// The handshake used to be awaited inline inside `next_connection`, whose
    /// only caller is a serial `while let` loop. So one local process that
    /// opened a TCP connection to the agent and sent no bytes parked the accept
    /// loop FOREVER -- every later tab, reload or new account got a socket that
    /// never completed, with nothing in the log, because no bytes reached
    /// `handle_request`. Any process on the machine could do it, deliberately or
    /// by accident, and the agent holds decrypted P2P plaintext.
    ///
    /// This test stalls first and connects second, which is the order that
    /// mattered: with the old code the second connection could never be
    /// accepted, so the test hangs until its timeout rather than failing on an
    /// assertion.
    #[tokio::test]
    async fn a_silent_socket_does_not_block_the_next_connection() {
        use citadel_io::tokio::net::TcpStream as RawStream;

        let mut interface =
            WebSocketInterface::new("127.0.0.1:0".parse().unwrap(), OriginPolicy::Any)
                .await
                .expect("bind");
        let addr = interface.local_addr().expect("addr");

        // The attacker: connect, send nothing, hold it open for the whole test.
        let _silent = RawStream::connect(addr).await.expect("silent connect");

        // A real client, arriving after it.
        let client = tokio::spawn(async move {
            tokio_tungstenite::connect_async(format!("ws://{addr}"))
                .await
                .map(|_| ())
        });

        let accepted = tokio::time::timeout(Duration::from_secs(5), interface.next_connection())
            .await
            .expect(
                "a silent socket blocked the accept loop -- the handshake is being awaited inline \
                 again, and one connection that sends nothing denies the agent to everyone",
            );

        assert!(
            accepted.is_some(),
            "the real client's handshake completed but no connection was handed to the caller"
        );
        let _ = tokio::time::timeout(Duration::from_secs(5), client).await;
    }

    /// And the stalled one is eventually closed rather than accumulating.
    ///
    /// Spawning alone would leave a stalled socket resident for the life of the
    /// process, so enough of them would still exhaust the agent. The bound is
    /// The TLS listener presents a certificate; the plain one does not.
    ///
    /// Asserted through a real TLS client handshake rather than by inspecting
    /// the config, because what broke was observable only end to end: the agent
    /// bound a plain socket, and `work.avarok.net` -- an HTTPS page, which a
    /// browser forbids from opening `ws://` -- got ERR_SSL_PROTOCOL_ERROR from
    /// every visitor's machine.
    ///
    /// A self-signed certificate generated here, so the test needs no fixture
    /// and cannot be satisfied by the shipped one.
    #[tokio::test]
    async fn the_tls_listener_completes_a_tls_handshake() {
        let cert = rcgen::generate_simple_self_signed(vec!["local.test".to_string()])
            .expect("generate a self-signed certificate");
        let chain = cert.cert.pem();
        let key = cert.signing_key.serialize_pem();

        let mut interface = WebSocketInterface::new_tls(
            "127.0.0.1:0".parse().unwrap(),
            OriginPolicy::Any,
            chain.as_bytes(),
            key.as_bytes(),
        )
        .await
        .expect("bind with TLS");
        let addr = interface.local_addr().expect("bound address");

        // Drive the accept loop; the handshake completes in the spawned task.
        let server = tokio::spawn(async move { interface.next_connection().await.is_some() });

        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut chain.as_bytes()).map(|c| c.unwrap()) {
            roots.add(c).expect("trust the self-signed certificate");
        }
        let config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
        let tcp = citadel_io::tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect");
        let tls = connector
            .connect("local.test".try_into().unwrap(), tcp)
            .await
            .expect("the listener must complete a TLS handshake");

        // And the WebSocket upgrade rides over it, which is what the browser does.
        let (_ws, _resp) = tokio_tungstenite::client_async("ws://local.test/", tls)
            .await
            .expect("the WebSocket upgrade must complete over TLS");

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .expect("the accept loop must yield")
                .expect("the accept task must not panic"),
            "a completed TLS+WebSocket handshake must surface as a connection",
        );
    }

    /// what makes spawning safe; this pins that the bound exists and is not
    /// something absurd.
    #[test]
    fn the_handshake_is_bounded() {
        assert!(
            HANDSHAKE_TIMEOUT <= Duration::from_secs(30),
            "a handshake bound this loose is not a bound: {HANDSHAKE_TIMEOUT:?}"
        );
        assert!(
            HANDSHAKE_TIMEOUT >= Duration::from_secs(1),
            "a browser on a loaded machine needs more than {HANDSHAKE_TIMEOUT:?}"
        );
    }

    mod origin_enforcement {
        use super::*;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        async fn listener_allowing(spec: &str) -> (WebSocketInterface, SocketAddr) {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let interface = WebSocketInterface::new(addr, OriginPolicy::parse(spec).unwrap())
                .await
                .unwrap();
            let bound = interface.local_addr().unwrap();
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
