use crate::connector::*;
use crate::scan_for_response;
use async_trait::async_trait;
// use citadel_internal_service_types::{DisconnectSuccess, InternalServicePayload, MessageSendSuccess, PeerDisconnectSuccess, SecurityLevel, SendFileRequestSuccess, TransferType};
use futures::stream::{SplitSink, SplitStream};
use futures::SinkExt;
use tokio::net::TcpListener;

use crate::codec::SerializingCodec;
use crate::io_interface::IOInterface;
// use citadel_internal_service_types::{
//     ConnectMode, ConnectSuccess, InternalServiceRequest, InternalServiceResponse,
//     PeerConnectSuccess, PeerRegisterSuccess, RegisterSuccess, SecBuffer, SessionSecuritySettings,
//     UdpMode,
// };
use citadel_internal_service_types::*;
use futures::StreamExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;
use uuid::Uuid;

pub struct TcpIOInterface {
    pub listener: TcpListener,
}

impl TcpIOInterface {
    pub async fn new<T: ToSocketAddrs>(bind_address: T) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_address).await?;
        Ok(Self { listener })
    }
}

impl InternalServiceConnector<TcpIOInterface> {
    /// Establishes a connection with the Internal Service running at the given address. Returns an
    /// InternalServiceConnector that can be used to easily interface with the Internal Service.
    pub async fn connect_to_service<S: ToSocketAddrs>(addr: S) -> Result<Self, ClientError> {
        let conn = TcpStream::connect(addr)
            .await
            .map_err(|err| ClientError::ConnectionToInternalServiceFailed(err.to_string()))?;
        let (sink, mut stream) = wrap_tcp_conn(conn).split();
        let greeter_packet = stream
            .next()
            .await
            .ok_or(ClientError::InternalServiceDisconnected)??;
        if matches!(
            greeter_packet,
            InternalServicePayload::Response(
                InternalServiceResponse::ServiceConnectionAccepted { .. }
            )
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

    /// Sends a request to register at server running at the given address. Returns a Result with
    /// a RegisterSuccess on success or ClientError on failure.
    pub async fn register<U: Into<SocketAddr>, S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        server_address: U,
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
    /// except for proposed credentials and the target server's address. Returns a Result with a
    /// RegisterSuccess on success or ClientError on failure.
    pub async fn register_with_defaults<
        U: Into<SocketAddr>,
        S: Into<String>,
        R: Into<SecBuffer>,
    >(
        &mut self,
        server_address: U,
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
    /// connect immediately following a successful registration. Returns a Result with a
    /// ConnectSuccess on success or ClientError on failure.
    pub async fn register_and_connect<U: Into<SocketAddr>, S: Into<String>, R: Into<SecBuffer>>(
        &mut self,
        server_address: U,
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
    /// Result with a ConnectSuccess on success or ClientError on failure.
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
    /// values for all parameters other than credentials. Returns a Result with a ConnectSuccess
    /// on success or ClientError on failure.
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

    /// Sends a request to register with peer with CID peer_cid. Returns a Result with a
    /// PeerRegisterSuccess on success or ClientError on failure.
    pub async fn peer_register<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
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
    /// proposed credentials. Returns a Result with a PeerRegisterSuccess on success or
    /// ClientError on failure.
    pub async fn peer_register_with_defaults<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
    ) -> Result<PeerRegisterSuccess, ClientError> {
        self.peer_register(cid, peer_cid, Default::default()).await
    }

    /// Sends a request to register with peer with CID peer_cid. Sends a request to
    /// connect immediately following a successful registration. Returns a Result with a
    /// PeerConnectSuccess on success or ClientError on failure.
    pub async fn peer_register_and_connect<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
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

    /// Sends a request to register with peer with CID peer_cid. Sends a request to
    /// connect immediately following a successful registration. Requests use the default
    /// SessionSecuritySettings Value. Returns a Result with a PeerConnectSuccess on
    /// success or ClientError on failure.
    pub async fn peer_register_and_connect_with_defaults<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
    ) -> Result<PeerConnectSuccess, ClientError> {
        self.peer_register_and_connect(cid, peer_cid, Default::default())
            .await
    }

    /// Sends a request to connect to peer with CID peer_cid. Returns a
    /// Result with a PeerConnectSuccess on success or ClientError on failure.
    pub async fn peer_connect<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
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
    /// parameters. Returns a Result with a PeerConnectSuccess on success or ClientError on failure.
    pub async fn peer_connect_with_defaults<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
    ) -> Result<PeerConnectSuccess, ClientError> {
        self.peer_connect(cid, peer_cid, Default::default(), Default::default())
            .await
    }

    /// Sends a message to given Peer or server if no peer CID was given. Returns a
    /// Result with a MessageSendSuccess on success or ClientError on failure.
    pub async fn message<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        message: Vec<u8>,
        security_level: SecurityLevel,
    ) -> Result<MessageSendSuccess, ClientError> {
        let peer_cid = peer_cid.map(|i| i.into());
        let outbound_request = InternalServiceRequest::Message {
            request_id: Uuid::new_v4(),
            message,
            cid: cid.into(),
            peer_cid,
            security_level,
        };

        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::MessageSendSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::MessageSendSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a message to given Peer or server if no peer CID was given. Uses the default
    /// security level. Returns a Result with a MessageSendSuccess on success or ClientError
    /// on failure.
    pub async fn message_with_defaults<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        message: Vec<u8>,
    ) -> Result<MessageSendSuccess, ClientError> {
        self.message(cid, peer_cid, message, Default::default())
            .await
    }

    /// Disconnects from connected server. Returns a Result with a DisconnectSuccess on success
    /// or ClientError on failure.
    pub async fn disconnect<S: Into<u64>>(
        &mut self,
        cid: S,
    ) -> Result<DisconnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::Disconnect {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::DisconnectSuccess(success) =
            scan_for_response!(self.stream, InternalServiceResponse::DisconnectSuccess(..))
        else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Disconnects from the given peer. Returns a Result with a PeerDisconnectSuccess on
    /// success or ClientError on failure.
    pub async fn peer_disconnect<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
    ) -> Result<PeerDisconnectSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::PeerDisconnect {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::PeerDisconnectSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::PeerDisconnectSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Requests to Send a file to the given peer or server if no peer CID was given. Allows for
    /// the chunk size to be controlled. Returns a Result with a SendFileRequestSuccess
    /// on success or Client Error on failure.
    pub async fn file_send<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        source: R,
        chunk_size: Option<usize>,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: source.into(),
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            chunk_size,
            transfer_type: TransferType::FileTransfer,
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Requests to Send a file to the given peer or server if no peer CID was given. Uses the
    /// default chunk size. Returns a Result with a SendFileRequestSuccess
    /// on success or Client Error on failure.
    pub async fn file_send_with_defaults<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        source: R,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: source.into(),
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            chunk_size: None,
            transfer_type: TransferType::FileTransfer,
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Respond to a File Transfer Request by accepting or declining. A Download Location can be
    /// given for the received file. Returns a Result with a SendFileRequestSuccess
    /// on success or Client Error on failure.
    pub async fn respond_file_transfer<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: S,
        object_id: u64,
        accept: bool,
        download_location: Option<R>,
    ) -> Result<FileTransferStatusNotification, ClientError> {
        let outbound_request = InternalServiceRequest::RespondFileTransfer {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
            object_id,
            accept,
            download_location: download_location.map(|i| i.into()),
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::FileTransferStatusNotification(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::FileTransferStatusNotification(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Respond to a File Transfer Request by accepting or declining. Download Location is set to
    /// Default. Returns a Result with a SendFileRequestSuccess on success or Client Error
    /// on failure.
    pub async fn respond_file_transfer_with_defaults<S: Into<u64>>(
        &mut self,
        cid: S,
        peer_cid: S,
        object_id: u64,
        accept: bool,
    ) -> Result<FileTransferStatusNotification, ClientError> {
        let outbound_request = InternalServiceRequest::RespondFileTransfer {
            request_id: Uuid::new_v4(),
            cid: cid.into(),
            peer_cid: peer_cid.into(),
            object_id,
            accept,
            download_location: None,
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::FileTransferStatusNotification(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::FileTransferStatusNotification(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Requests to Send a file for REVFS (Remote Encrypted Virtual File System) at the given peer
    /// or server if no peer CID was given. Allows for the chunk size to be controlled. Returns a
    /// Result with a SendFileRequestSuccess on success or Client Error on failure.
    pub async fn revfs_push<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        source: R,
        chunk_size: Option<usize>,
        virtual_path: R,
        security_level: SecurityLevel,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: source.into(),
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            chunk_size,
            transfer_type: TransferType::RemoteEncryptedVirtualFilesystem {
                virtual_path: virtual_path.into(),
                security_level,
            },
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Requests to Send a file for REVFS (Remote Encrypted Virtual File System) at the given peer
    /// or server if no peer CID was given. Allows for the chunk size to be controlled. Returns a
    /// Result with a SendFileRequestSuccess on success or Client Error on failure.
    pub async fn revfs_push_with_defaults<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        source: R,
        virtual_path: R,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: source.into(),
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            chunk_size: None,
            transfer_type: TransferType::RemoteEncryptedVirtualFilesystem {
                virtual_path: virtual_path.into(),
                security_level: Default::default(),
            },
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Downloads a file from REVFS (Remote Encrypted Virtual File System) at the given peer
    /// or server if no peer CID was given. Uses a custom Security Level and can delete the file
    /// upon pulling. Returns a Result with a SendFileRequestSuccess on success or Client Error
    /// on failure.
    pub async fn revfs_pull<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        virtual_directory: R,
        security_level: Option<SecurityLevel>,
        delete_on_pull: bool,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::DownloadFile {
            virtual_directory: virtual_directory.into(),
            security_level,
            delete_on_pull,
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            request_id: Uuid::new_v4(),
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Downloads a file from REVFS (Remote Encrypted Virtual File System) at the given peer
    /// or server if no peer CID was given. Uses default Security Level and does not delete the
    /// file on pull. Returns a Result with a SendFileRequestSuccess on success or Client Error
    /// on failure.
    pub async fn revfs_pull_with_defaults<S: Into<u64>, R: Into<PathBuf>>(
        &mut self,
        cid: S,
        peer_cid: Option<S>,
        virtual_directory: R,
        security_level: Option<SecurityLevel>,
        delete_on_pull: bool,
    ) -> Result<SendFileRequestSuccess, ClientError> {
        let outbound_request = InternalServiceRequest::DownloadFile {
            virtual_directory: virtual_directory.into(),
            security_level,
            delete_on_pull,
            cid: cid.into(),
            peer_cid: peer_cid.map(|i| i.into()),
            request_id: Uuid::new_v4(),
        };
        self.send_raw_request(outbound_request).await?;
        let InternalServiceResponse::SendFileRequestSuccess(success) = scan_for_response!(
            self.stream,
            InternalServiceResponse::SendFileRequestSuccess(..)
        ) else {
            panic!("Unreachable")
        };
        Ok(success)
    }

    /// Sends a raw request to the internal service
    pub async fn send_raw_request(
        &mut self,
        request: InternalServiceRequest,
    ) -> Result<(), ClientError> {
        self.sink
            .inner
            .send(InternalServicePayload::Request(request))
            .await?;
        Ok(())
    }

    pub fn split(self) -> (WrappedSink<TcpIOInterface>, WrappedStream<TcpIOInterface>) {
        (self.sink, self.stream)
    }
}

#[async_trait]
impl IOInterface for TcpIOInterface {
    type Sink = SplitSink<
        Framed<TcpStream, SerializingCodec<InternalServicePayload>>,
        InternalServicePayload,
    >;
    type Stream = SplitStream<Framed<TcpStream, SerializingCodec<InternalServicePayload>>>;

    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)> {
        self.listener
            .accept()
            .await
            .ok()
            .map(|(stream, _)| wrap_tcp_conn(stream).split())
    }
}
