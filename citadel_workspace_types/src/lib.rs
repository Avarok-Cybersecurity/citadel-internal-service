use bytes::BytesMut;
use citadel_sdk::prelude::{ConnectMode, ObjectSource, SecBuffer, SecureProtocolPacket, SecurityLevel, SessionSecuritySettings, Ticket, TransferType, UdpMode, UserIdentifier};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
        server_addr: SocketAddr,
        username: String,
        password: SecBuffer,
        connect_mode: ConnectMode,
        udp_mode: UdpMode,
        keep_alive_timeout: Option<Duration>,
        session_security_settings: SessionSecuritySettings
    },
    ConnectSuccess {
        cid: u64,
    },
    ConnectionFailure {
        message: String,
    },
    Register {
        uuid: Uuid,
        server_addr: SocketAddr,
        full_name: String,
        username: String,
        proposed_password: SecBuffer,
        default_security_settings: SessionSecuritySettings
    },
    RegisterSuccess {
        id: Uuid,
    },
    RegisterFailure {
        message: String,
    },
    Message {
        message: SecureProtocolPacket,
        cid: u64,
        security_level: SecurityLevel,
    },
    MessageReceived {
        message: SecureProtocolPacket,
        cid: u64,
        peer_cid: u64,
        security_level: SecurityLevel
    },
    Disconnect {
        implicated_cid: u64,             // Everything other than the implicated_cid would be used to craft the disconnect PeerSignal
        ticket: Ticket,
        cid_opt: Option<u64>,
        success: bool,
        v_conn_type: Option<VirtualConnectionType>,
        message: String
    },
    SendFile {
        source: dyn ObjectSource,
        cid: u64,
        transfer_security_level: SecurityLevel,
        chunk_size: usize,
        transfer_type: TransferType
    },
    DownloadFile {
        virtual_path: dyn Into<PathBuf>,
        transfer_security_level: SecurityLevel,
        delete_on_pull: bool
    },

    StartGroup {
        initial_users_to_invite: Option <Vec<UserIdentifier>>,
        session_security_settings: SessionSecuritySettings
    },

    ServiceConnectionAccepted {
        id: Uuid,
    }, // response
       // ResponseConnect {},
}
