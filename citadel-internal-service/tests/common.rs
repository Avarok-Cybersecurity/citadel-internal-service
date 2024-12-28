use citadel_internal_service::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::*;
use citadel_sdk::prelude::*;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub fn setup_log() {
    let _ = env_logger::try_init();
}

pub fn server_info_skip_cert_verification() -> (SocketAddr, SocketAddr) {
    let bind_address = "127.0.0.1:0".parse().unwrap();
    let server_address = "127.0.0.1:0".parse().unwrap();
    (bind_address, server_address)
}

pub async fn register_p2p(
    service: &mut CitadelWorkspaceService,
    cid: u64,
    request_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = service
        .send(InternalServiceRequest::PeerRegister {
            cid,
            request_id,
            connect_after_register: false,
            peer_session_password: None,
        })
        .await?;

    if let InternalServiceResponse::PeerRegisterSuccess = response {
        Ok(())
    } else {
        Err("Failed to register peer".into())
    }
}

pub async fn connect_p2p(
    service: &mut CitadelWorkspaceService,
    cid: u64,
    peer_cid: u64,
    request_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = service
        .send(InternalServiceRequest::PeerConnect {
            cid,
            peer_cid,
            request_id,
            peer_session_password: None,
        })
        .await?;

    if let InternalServiceResponse::ConnectSuccess = response {
        Ok(())
    } else {
        Err("Failed to connect peers".into())
    }
}
