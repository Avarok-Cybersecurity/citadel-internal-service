use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, LocalDBGetKVFailure, LocalDBGetKVSuccess,
    KEY_NOT_FOUND,
};
use citadel_sdk::backend_kv_store::BackendHandler;
use citadel_sdk::prelude::Ratchet;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
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

    // An unknown CID is a normal client input — the dev backend drops accounts
    // on restart while browsers keep their CIDs — and it used to panic this
    // task, so the request was never answered at all.
    let remote = match super::generate_remote(this.remote(), cid, peer_cid).await {
        Ok(remote) => remote,
        Err(err) => {
            let response = InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                cid,
                peer_cid,
                message: err.into_string(),
                request_id: Some(request_id),
            });
            return Some(HandledRequestResult { response, uuid });
        }
    };
    let response = backend_handler_get(&remote, cid, peer_cid, key, Some(request_id)).await;

    Some(HandledRequestResult { response, uuid })
}

pub async fn backend_handler_get<R: Ratchet>(
    remote: &impl BackendHandler<R>,
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
                    message: KEY_NOT_FOUND.to_string(),
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
