use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, LocalDBGetKVFailure, LocalDBGetKVSuccess,
};
use citadel_sdk::backend_kv_store::BackendHandler;
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::LocalDBGetKV {
        request_id,
        cid,
        peer_cid,
        key,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };

    let mut lock = this.server_connection_map.lock().await;
    let response = match lock.get_mut(&cid) {
        None => InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
            cid,
            peer_cid,
            message: "Server connection not found".to_string(),
            request_id: Some(request_id),
        }),
        Some(conn) => {
            if let Some(peer_cid) = peer_cid {
                if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                    let peer_remote = peer.remote.clone();
                    drop(lock);
                    backend_handler_get(&peer_remote, cid, Some(peer_cid), key, Some(request_id))
                        .await
                } else {
                    InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                        cid,
                        peer_cid: Some(peer_cid),
                        message: "Peer connection not found".to_string(),
                        request_id: Some(request_id),
                    })
                }
            } else {
                let remote = conn.client_server_remote.clone();
                drop(lock);
                backend_handler_get(&remote, cid, peer_cid, key, Some(request_id)).await
            }
        }
    };

    Some(HandledRequestResult { response, uuid })
}

pub async fn backend_handler_get(
    remote: &impl BackendHandler,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
    request_id: Option<Uuid>,
) -> InternalServiceResponse {
    match remote.get(&key).await {
        Ok(value) => {
            if let Some(value) = value {
                InternalServiceResponse::LocalDBGetKVSuccess(LocalDBGetKVSuccess {
                    cid,
                    peer_cid,
                    key,
                    value,
                    request_id,
                })
            } else {
                InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                    cid,
                    peer_cid,
                    message: "Key not found".to_string(),
                    request_id,
                })
            }
        }
        Err(err) => InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
            cid,
            peer_cid,
            message: err.into_string(),
            request_id,
        }),
    }
}
