use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use citadel_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
pub enum InternalServicePayload {
    Connect {auth: AuthenticationRequest, udp_mode: UdpMode, keep_alive_timeout: Option<Duration>, session_security_settings: SessionSecuritySettings, on_channel_received: dyn FnOnce(ConnectionSuccess, ClientServerRemote) },
    Register {full_name: String, username: String, proposed_password: SecBuffer, server_addr: dyn ToSocketAddrs<Iter=()>, udp_mode: UdpMode, default_security_settings: SessionsSecuritySettings, on_channel_received: dyn FnOnce(ConnectionSuccess, ClientServerRemote) },
    Message {message: SecureProtocolPacket, cid: cid, security_level: SecurityLevel},
    Disconnect {ticket: Ticket, cid_opt: Option<u64>, success: bool, v_conn_type: Option<VirtualConnectionType>, message: String},
    SendFile {source: ObjectSource, cid: cid, transfer_security_level: SecurityLevel, chunk_size: usize, transfer_type: TransferType},
    DownloadFile {virtual_path: dyn Into<PathBuf>, transfer_security_level: SecurityLevel, delete_on_pull: bool},
    //StartGroup {},

    // response
    //ResponseConnect {}
}
