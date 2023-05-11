use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum InternalServicePayload {
    Connect {auth: AuthenticationRequest, connect_mode: ConnectMode, udp_mode: UdpMode, keep_alive_timeout: Option<Duration>, session_security_settings: SessionSecuritySettings},
    Register {addr: dyn ToSocketAddrs<Iter=()>, full_name: String, username: String, proposed_password: SecBuffer, default_security_settings: SessionsSecuritySettings},
    Message {message: SecBuffer, cid: cid, security_level: SecurityLevel},
    Disconnect {ticket: Ticket, cid_opt: Option<u64>, success: bool, v_conn_type: Option<VirtualConnectionType>, message: String},
    SendFile {source: ObjectSource, cid: cid, transfer_security_level: SecurityLevel, chunk_size: usize, transfer_type: TransferType},
    DownloadFile {virtual_path: dyn Into<PathBuf>, transfer_security_level: SecurityLevel, delete_on_pull: bool},
    //StartGroup {},

    // response
    //ResponseConnect {}
}
