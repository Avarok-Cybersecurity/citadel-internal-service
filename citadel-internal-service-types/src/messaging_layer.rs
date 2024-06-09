use serde::{Deserialize, Serialize};
use crate::InternalServiceResponse;


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
}

#[derive(Serialize, Deserialize)]
pub struct CWMessage {
    pub id: u64,
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub contents: Vec<u8>,
}