use bytes::BytesMut;
use citadel_sdk::prelude::{SecBuffer, SecurityLevel, SessionSecuritySettings, UdpMode};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServiceResponse {
    ConnectSuccess {
        cid: u64,
    },
    ConnectionFailure {
        message: String,
    },
    RegisterSuccess {
        id: Uuid,
    },
    RegisterFailure {
        message: String,
    },
    ServiceConnectionAccepted {
        id: Uuid,
    },
    MessageReceived {
        message: BytesMut,
        cid: u64,
        peer_cid: u64,
    },
    MessageSent {
        cid: u64,
        // uuid: Uuid,
    },
    DisconnectSuccess(u64),
    DisconnectFailed(u64),
    PeerConnectSuccess{
        cid: u64,
    },
    PeerConnectFailure{
        cid: u64,
        message: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
        username: String,
        password: SecBuffer,
    },
    Register {
        uuid: Uuid,
        server_addr: SocketAddr,
        full_name: String,
        username: String,
        proposed_password: SecBuffer,
    },
    Message {
        uuid: Uuid,
        message: Vec<u8>,
        cid: u64,
        user_cid: u64,
        security_level: SecurityLevel,
    },
    Disconnect {
        uuid: Uuid,
        cid: u64,
    },
    SendFile {},
    DownloadFile {},
    PeerConnect {
        uuid: Uuid,
        cid: u64,
        username: String,
        peer_cid: u64,
        peer_username: String,
        udp_mode: UdpMode,
        session_security_settings: SessionSecuritySettings
    },
}
