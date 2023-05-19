use bytes::BytesMut;
use citadel_sdk::prelude::{SecBuffer, SecurityLevel};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServicePayload {
    Connect {
        uuid: Uuid,
        username: String,
        password: SecBuffer,
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
    },
    RegisterSuccess {
        id: Uuid,
    },
    RegisterFailure {
        message: String
    },
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

