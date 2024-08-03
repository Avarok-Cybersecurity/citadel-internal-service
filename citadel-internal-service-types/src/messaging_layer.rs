use crate::InternalServiceResponse;
use citadel_types::prelude::SecurityLevel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// What the consumer receives from the underlying messaging service
#[derive(Serialize, Deserialize)]
pub enum MessengerUpdate {
    Message { message: CWMessage },
    Other { response: InternalServiceResponse },
}

#[derive(Serialize, Deserialize)]
pub enum CWProtocolMessage {
    Message { message: CWMessage },
    MessageAck { id: u64 },
    Poll { ids: Vec<u64> },
    PollResponse { messages: Vec<CWMessage> },
}

pub struct OutgoingCWMessage {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub contents: Vec<u8>,
    pub request_id: Uuid,
    pub security_level: Option<SecurityLevel>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CWMessage {
    pub id: u64,
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub contents: Vec<u8>,
}
