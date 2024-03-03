use crate::util;
use crate::util::{WrappedSink, WrappedStream};
use citadel_internal_service_types::{
    ConnectMode, ConnectSuccess, InternalServicePayload, InternalServiceRequest,
    InternalServiceResponse, PeerConnectSuccess, PeerRegisterSuccess, RegisterSuccess, SecBuffer,
    SessionSecuritySettings, UdpMode,
};
use futures::{SinkExt, StreamExt};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use uuid::Uuid;

pub struct InternalServiceConnector {
    pub sink: WrappedSink,
    pub stream: WrappedStream,
}

#[derive(Debug, Clone)]
pub enum ClientError {
    ConnectionToInternalServiceFailed(String),
    InternalServiceDisconnected,
    InternalServiceInvalidResponse(String),
    CodecError(String),
    SendError(String),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl Error for ClientError {}

macro_rules! scan_for_response {
    ($stream:expr, $required_packet:pat) => {{
        loop {
            match $stream.next().await {
                Some(response) => {
                    if response.is_error() {
                        return Err(ClientError::InternalServiceInvalidResponse(format!(
                            "{response:?}"
                        )));
                    }

                    if matches!(response, $required_packet) {
                        break response;
                    }

                    citadel_logging::trace!("Service Connector - Unrelated response: {response:?}");
                }
                None => return Err(ClientError::InternalServiceDisconnected)?,
            }
        }
    }};
}

impl InternalServiceConnector {
    /// Establishes a connection with the Internal Service running at the given address. Returns an
    /// InternalServiceConnector that can be used to easily interface with the Internal Service.
    pub async fn connect_to_service<T: ToSocketAddrs>(addr: T) -> Result<Self, ClientError> {
        let conn = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::ConnectionToInternalServiceFailed(err.to_string()))?;
        let (sink, mut stream) = util::wrap_tcp_conn(conn).split();
        let greeter_packet = stream
            .next()
            .await
            .ok_or(ClientError::InternalServiceDisconnected)??;
        if matches!(
            greeter_packet,
            InternalServicePayload::Response(InternalServiceResponse::ServiceConnectionAccepted(_))
        ) {
            let stream = WrappedStream { inner: stream };
            let sink = WrappedSink { inner: sink };
            Ok(Self { sink, stream })
        } else {
            Err(ClientError::InternalServiceInvalidResponse(format!(
                "{greeter_packet:?}"
            )))?
        }
    }

    pub fn split(self) -> (WrappedSink, WrappedStream) {
        (self.sink, self.stream)
    }

    /// Sends a request to register at server running at the given address. Returns a Result with
    /// an InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn register<T: Into<SocketAddr>, S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        server_address: T,
        full_name: S,
        username: S,
        proposed_password: R,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<RegisterSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: server_address.into(),
            full_name: full_name.into(),
            username: username.into(),
            proposed_password: proposed_password.into(),
            session_security_settings,
            connect_after_register: false,
        };

        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::RegisterSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::RegisterSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to register at server running at the given address. Uses the default values
    /// except for proposed credentials and the target server's address. Returns a Result with an
    /// InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn register_with_defaults<
        T: Into<SocketAddr>,
        S: Into<String>,
        R: Into<SecBuffer>,
    >(
        &mut self,
        server_address: T,
        full_name: S,
        username: S,
        proposed_password: R,
    ) -> Result<RegisterSuccess, ClientError> {
        self.register(
            server_address,
            full_name,
            username,
            proposed_password,
            Default::default(),
        )
        .await
    }

    /// Sends a request to register at server running at the given address. Sends a request to
    /// connect immediately following a successful registration. Returns a Result with an
    /// InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn register_and_connect<T: Into<SocketAddr>, S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        server_address: T,
        full_name: S,
        username: S,
        proposed_password: R,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<ConnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: server_address.into(),
            full_name: full_name.into(),
            username: username.into(),
            proposed_password: proposed_password.into(),
            session_security_settings,
            connect_after_register: true,
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::ConnectSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::ConnectSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to connect to the current server with the given credentials. Returns a
    /// Result with an InternalServiceResponse that specifies whether or not the request
    /// was successfully sent.
    pub async fn connect<S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        username: S,
        password: R,
        connect_mode: ConnectMode,
        udp_mode: UdpMode,
        keep_alive_timeout: Option<Duration>,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<ConnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::Connect {
            request_id: Uuid::new_v4(),
            username: username.into(),
            password: password.into(),
            connect_mode,
            udp_mode,
            keep_alive_timeout,
            session_security_settings,
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::ConnectSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::ConnectSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to connect to the current server with the given credentials. Uses default
    /// values for all parameters other than credentials. Returns a Result with an
    /// InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn connect_with_defaults<S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        username: S,
        password: R,
    ) -> Result<ConnectSuccess, ClientError> {
        self.connect(
            username,
            password,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
    }

    /// Sends a request to register with peer with CID peer_cid. Returns a Result with an
    /// InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn peer_register<T: Into<u64>>(
        &mut self,
        cid: T,
        peer_cid: T,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<PeerRegisterSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
            session_security_settings,
            connect_after_register: false,
        };

        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::PeerRegisterSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::PeerRegisterSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to register with peer with CID peer_cid. Uses the default values except for
    /// proposed credentials. Returns a Result with an InternalServiceResponse that specifies
    /// whether or not the request was successfully sent.
    pub async fn peer_register_with_defaults<T: Into<u64>>(
        &mut self,
        cid: T,
        peer_cid: T,
    ) -> Result<PeerRegisterSuccess, ClientError> {
        self.peer_register(cid, peer_cid, Default::default()).await
    }

    /// Sends a request to register with peer with CID peer_cid. Sends a request to
    /// connect immediately following a successful registration. Returns a Result with an
    /// InternalServiceResponse that specifies whether or not the request was successfully sent.
    pub async fn peer_register_and_connect<T: Into<u64>>(
        &mut self,
        cid: T,
        peer_cid: T,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<PeerConnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
            session_security_settings,
            connect_after_register: true,
        };

        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::PeerConnectSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::PeerConnectSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to connect to peer with CID peer_cid. Returns a
    /// Result with an InternalServiceResponse that specifies whether or not the request
    /// was successfully sent.
    pub async fn peer_connect<T: Into<u64>>(
        &mut self,
        cid: T,
        peer_cid: T,
        udp_mode: UdpMode,
        session_security_settings: SessionSecuritySettings,
    ) -> Result<PeerConnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
            udp_mode,
            session_security_settings,
        };

        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::PeerConnectSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::PeerConnectSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a request to connect to peer with CID peer_cid. Uses default values for connection
    /// parameters. Returns a Result with an InternalServiceResponse that specifies whether or
    /// not the request was successfully sent.
    pub async fn peer_connect_with_defaults<T: Into<u64>>(
        &mut self,
        cid: T,
        peer_cid: T,
    ) -> Result<PeerConnectSuccess, ClientError> {
        self.peer_connect(cid, peer_cid, Default::default(), Default::default())
            .await
    }

    /// Sends a raw request to the internal service
    pub async fn send_raw_request(
        &mut self,
        request: InternalServiceRequest,
    ) -> Result<(), ClientError> {
        self.sink.send(request).await?;
        Ok(())
    }
}
