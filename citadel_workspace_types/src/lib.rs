use bytes::BytesMut;
use citadel_sdk::prelude::VirtualObjectMetadata;
pub use citadel_sdk::prelude::{
    ConnectMode, ObjectTransferStatus, SecBuffer, SecurityLevel, SessionSecuritySettings, UdpMode,
    UserIdentifier,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectSuccess {
    pub cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionFailure {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterSuccess {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterFailure {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConnectionAccepted {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageSent {
    pub cid: u64,
    pub peer_cid: Option<u64>, // TODO: investigate passing a message hash or a trace id
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageSendError {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageReceived {
    pub message: BytesMut,
    pub cid: u64,
    pub peer_cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Disconnected {
    pub cid: u64,
    pub peer_cid: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DisconnectFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SendFileSuccess {
    pub cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SendFileFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadFileSuccess {
    pub cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadFileFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteVirtualFileSuccess {
    pub cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteVirtualFileFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConnectSuccess {
    pub cid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConnectFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerDisconnectSuccess {
    pub cid: u64,
    pub ticket: u128,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerDisconnectFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRegisterSuccess {
    pub cid: u64,
    pub peer_cid: u64,
    pub username: String,
    // TODO: add access to MutualPeer
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRegisterFailure {
    pub cid: u64,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBSetKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBSetKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBDeleteKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBDeleteKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetAllKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub map: HashMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetAllKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBClearAllKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBClearAllKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferRequest {
    pub cid: u64,
    pub peer_cid: u64,
    pub metadata: VirtualObjectMetadata, // TODO: metadata: VirtualObjectMetadata
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferStatus {
    pub cid: u64,
    pub object_id: u32,
    pub success: bool,
    pub response: bool,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferTick {
    pub uuid: Uuid,
    pub cid: u64,
    pub peer_cid: u64,
    pub status: String,
    //TODO: make status ObjectTransferStatus -- Needs to serialize and deserialize
    // or could keep as a string and modify temporary handling of status?
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServiceResponse {
    ConnectSuccess(ConnectSuccess),
    ConnectionFailure(ConnectionFailure),
    RegisterSuccess(RegisterSuccess),
    RegisterFailure(RegisterFailure),
    ServiceConnectionAccepted(ServiceConnectionAccepted),
    MessageSent(MessageSent),
    MessageSendError(MessageSendError),
    MessageReceived(MessageReceived),
    Disconnected(Disconnected),
    DisconnectFailure(DisconnectFailure),
    SendFileSuccess(SendFileSuccess),
    SendFileFailure(SendFileFailure),
    FileTransferRequest(FileTransferRequest),
    FileTransferStatus(FileTransferStatus),
    FileTransferTick(FileTransferTick),
    DownloadFileSuccess(DownloadFileSuccess),
    DownloadFileFailure(DownloadFileFailure),
    DeleteVirtualFileSuccess(DeleteVirtualFileSuccess),
    DeleteVirtualFileFailure(DeleteVirtualFileFailure),
    PeerConnectSuccess(PeerConnectSuccess),
    PeerConnectFailure(PeerConnectFailure),
    PeerDisconnectSuccess(PeerDisconnectSuccess),
    PeerDisconnectFailure(PeerDisconnectFailure),
    PeerRegisterSuccess(PeerRegisterSuccess),
    PeerRegisterFailure(PeerRegisterFailure),
    LocalDBGetKVSuccess(LocalDBGetKVSuccess),
    LocalDBGetKVFailure(LocalDBGetKVFailure),
    LocalDBSetKVSuccess(LocalDBSetKVSuccess),
    LocalDBSetKVFailure(LocalDBSetKVFailure),
    LocalDBDeleteKVSuccess(LocalDBDeleteKVSuccess),
    LocalDBDeleteKVFailure(LocalDBDeleteKVFailure),
    LocalDBGetAllKVSuccess(LocalDBGetAllKVSuccess),
    LocalDBGetAllKVFailure(LocalDBGetAllKVFailure),
    LocalDBClearAllKVSuccess(LocalDBClearAllKVSuccess),
    LocalDBClearAllKVFailure(LocalDBClearAllKVFailure),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
        username: String,
        password: SecBuffer,
        connect_mode: ConnectMode,
        udp_mode: UdpMode,
        keep_alive_timeout: Option<Duration>,
        session_security_settings: SessionSecuritySettings,
    },
    Register {
        uuid: Uuid,
        server_addr: SocketAddr,
        full_name: String,
        username: String,
        proposed_password: SecBuffer,
        connect_after_register: bool,
        default_security_settings: SessionSecuritySettings,
    },
    Message {
        uuid: Uuid,
        message: Vec<u8>,
        cid: u64,
        // if None, send to server, otherwise, send to p2p
        peer_cid: Option<u64>,
        security_level: SecurityLevel,
    },
    Disconnect {
        uuid: Uuid,
        cid: u64,
    },
    SendFile {
        uuid: Uuid,
        source: PathBuf,
        cid: u64,
        is_refvs: bool,
        peer_cid: Option<u64>,
        chunk_size: Option<usize>,
        virtual_directory: Option<PathBuf>,
        security_level: Option<SecurityLevel>,
    },
    RespondFileTransfer {
        uuid: Uuid,
        cid: u64,
        peer_cid: u64,
        object_id: u32,
        accept: bool,
        download_location: Option<PathBuf>,
    },
    DownloadFile {
        virtual_directory: PathBuf,
        security_level: Option<SecurityLevel>,
        delete_on_pull: bool,
        cid: u64,
        peer_cid: Option<u64>,
        uuid: Uuid,
    },
    DeleteVirtualFile {
        virtual_directory: PathBuf,
        cid: u64,
        peer_cid: Option<u64>,
        uuid: Uuid,
    },
    StartGroup {
        initial_users_to_invite: Option<Vec<UserIdentifier>>,
        cid: u64,
        uuid: Uuid,
    },
    PeerConnect {
        uuid: Uuid,
        cid: u64,
        username: String,
        peer_cid: u64,
        peer_username: String,
        udp_mode: UdpMode,
        session_security_settings: SessionSecuritySettings,
    },
    PeerDisconnect {
        uuid: Uuid,
        cid: u64,
        peer_cid: u64,
    },
    PeerRegister {
        uuid: Uuid,
        cid: u64,
        peer_id: UserIdentifier,
        connect_after_register: bool,
    },
    LocalDBGetKV {
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBSetKV {
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
        value: Vec<u8>,
    },
    LocalDBDeleteKV {
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBGetAllKV {
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
    },
    LocalDBClearAllKV {
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
    },
}
