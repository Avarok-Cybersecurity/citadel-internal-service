use bytes::BytesMut;
use citadel_sdk::prelude::SecurityLevel;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {},
    Register {},
    Message {
        message: Vec<u8>,
        cid: u64,
        security_level: SecurityLevel,
    },
    MessageReceived {
        message: BytesMut,
        cid: u64,
        peer_cid: u64,
    },
    Disconnect {},
    SendFile {},
    DownloadFile {},
    // response
    // ResponseConnect {},
}
