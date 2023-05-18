use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use citadel_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
        server_addr: dyn ToSocketAddrs<Iter=()>,
        username: String,
        password: SecBuffer,
        //connect_mode: ConnectMode,
        //udp_mode: UdpMode,
        //keep_alive_timeout: Option<Duration>,
        //session_security_settings: SessionSecuritySettings
    },
    Register {
        full_name: String,
        username: String,
        proposed_password: SecBuffer,
        server_addr: dyn ToSocketAddrs<Iter=()>,
        udp_mode: UdpMode,
        default_security_settings: SessionsSecuritySettings
    },
    Message {
        message: SecureProtocolPacket,
        cid: u64,
        security_level: SecurityLevel
    },
    MessageReceived {
        message: SecureProtocolPacket,
        cid: u64,
        peer_cid: u64
    },
    Disconnect {
        ticket: Ticket,
        cid_opt: Option<u64>,
        success: bool,
        v_conn_type: Option<VirtualConnectionType>,
        message: String
    },
    SendFile {
        source: ObjectSource,
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
    ServiceConnectionAccepted {
        id: Uuid
    },
    StartGroup {
        initial_users_to_invite: Option <Vec<UserIdentifier>>,
        session_security_settings: SessionSecuritySettings
    },

    // response
    //ResponseConnect {}
}
