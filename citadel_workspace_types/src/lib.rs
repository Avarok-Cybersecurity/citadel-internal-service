use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
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
