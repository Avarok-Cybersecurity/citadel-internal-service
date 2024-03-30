use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_types::{
    DownloadFileFailure, DownloadFileSuccess, InternalServiceRequest, InternalServiceResponse,
};
use citadel_logging::{error, info};
use citadel_sdk::prelude::{NetworkError, NodeRequest, PullObject, TargetLockedRemote};
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::DownloadFile {
        virtual_directory,
        security_level,
        delete_on_pull,
        cid,
        peer_cid,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let security_level = security_level.unwrap_or_default();
    let mut lock = this.server_connection_map.lock().await;
    let response = match lock.get_mut(&cid) {
        Some(conn) => {
            let result = if let Some(peer_cid) = peer_cid {
                if let Some(peer_remote) = conn.peers.get_mut(&peer_cid) {
                    let request = NodeRequest::PullObject(PullObject {
                        v_conn: *peer_remote.remote.user(),
                        virtual_dir: virtual_directory,
                        delete_on_pull,
                        transfer_security_level: security_level,
                    });

                    drop(lock);
                    remote.send(request).await
                } else {
                    Err(NetworkError::msg("Peer Connection Not Found"))
                }
            } else {
                let request = NodeRequest::PullObject(PullObject {
                    v_conn: *conn.client_server_remote.user(),
                    virtual_dir: virtual_directory,
                    delete_on_pull,
                    transfer_security_level: security_level,
                });

                drop(lock);
                remote.send(request).await
            };

            match result {
                Ok(_) => InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                    cid,
                    request_id: Some(request_id),
                }),

                Err(err) => InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                }),
            }
        }
        None => {
            error!(target: "citadel","server connection not found");
            InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                cid,
                message: String::from("Server Connection Not Found"),
                request_id: Some(request_id),
            })
        }
    };

    Some(HandledRequestResult { response, uuid })
}
