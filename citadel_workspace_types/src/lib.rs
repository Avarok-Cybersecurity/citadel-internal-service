use bytes::BytesMut;
use citadel_sdk::prelude::SecurityLevel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
    },
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

    ServiceConnectionAccepted {
        id: Uuid,
    }, // response
       // ResponseConnect {},
}
