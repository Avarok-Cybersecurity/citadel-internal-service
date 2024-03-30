use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, SendFileRequestFailure, SendFileRequestSuccess,
};
use citadel_logging::{error, info};
use citadel_sdk::prelude::{
    NetworkError, NodeRequest, SendObject, TargetLockedRemote, VirtualTargetType,
};
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::SendFile {
        request_id,
        source,
        cid,
        peer_cid,
        chunk_size,
        transfer_type,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote().clone();

    let lock = this.server_connection_map.lock().await;
    match lock.get(&cid) {
        Some(conn) => {
            let result = if let Some(peer_cid) = peer_cid {
                if let Some(peer_remote) = conn.peers.get(&peer_cid) {
                    let request = NodeRequest::SendObject(SendObject {
                        source: Box::new(source),
                        chunk_size,
                        implicated_cid: cid,
                        v_conn_type: *peer_remote.remote.user(),
                        transfer_type,
                    });

                    drop(lock);
                    remote.send(request).await
                } else {
                    Err(NetworkError::msg("Peer Connection Not Found"))
                }
            } else {
                let request = NodeRequest::SendObject(SendObject {
                    source: Box::new(source),
                    chunk_size,
                    implicated_cid: cid,
                    v_conn_type: VirtualTargetType::LocalGroupServer {
                        implicated_cid: cid,
                    },
                    transfer_type,
                });

                drop(lock);

                remote.send(request).await
            };

            match result {
                Ok(_) => {
                    info!(target: "citadel","InternalServiceRequest Send File Success");
                    let response =
                        InternalServiceResponse::SendFileRequestSuccess(SendFileRequestSuccess {
                            cid,
                            request_id: Some(request_id),
                        });

                    Some(HandledRequestResult { response, uuid })
                }

                Err(err) => {
                    error!(target: "citadel","InternalServiceRequest Send File Failure");
                    let response =
                        InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        });

                    Some(HandledRequestResult { response, uuid })
                }
            }
        }

        None => {
            error!(target: "citadel","server connection not found");
            let response =
                InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                    cid,
                    message: "Server Connection Not Found".into(),
                    request_id: Some(request_id),
                });

            Some(HandledRequestResult { response, uuid })
        }
    }
}
