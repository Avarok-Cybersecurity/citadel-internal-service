use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum InternalServicePayload {
    Connect {},
    Register {},
    Message {},
    Disconnect {},
    SendFile {},
    DownloadFile {},
    // response
    // ResponseConnect {},
}
