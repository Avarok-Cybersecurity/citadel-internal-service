use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum InternalServicePayload {
    Connect {},
    Register {},
    Message {},
    Disconnect {},
    SendFile {},
    DownloadFile {},
    StartGroup {},

    // response
    ResponseConnect {}
}
