use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    DeleteVirtualFileFailure, DeleteVirtualFileSuccess, InternalServiceRequest,
    InternalServiceResponse,
};
use citadel_logging::error;
use citadel_sdk::prelude::{DeleteObject, NetworkError, NodeRequest, Ratchet, VirtualTargetType};
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::DeleteVirtualFile {
        virtual_directory,
        cid,
        peer_cid,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let mut lock = this.server_connection_map.lock().await;
    let response = match lock.get_mut(&cid) {
        Some(conn) => {
            let result = if let Some(peer_cid) = peer_cid {
                if let Some(_peer_remote) = conn.peers.get_mut(&peer_cid) {
                    let request = NodeRequest::DeleteObject(DeleteObject {
                        v_conn: VirtualTargetType::LocalGroupPeer {
                            session_cid: cid,
                            peer_cid,
                        },
                        virtual_dir: virtual_directory,
                        security_level: Default::default(),
                    });

                    drop(lock);
                    remote.send(request).await
                } else {
                    Err(NetworkError::msg("Peer Connection Not Found"))
                }
            } else {
                let request = NodeRequest::DeleteObject(DeleteObject {
                    v_conn: VirtualTargetType::LocalGroupServer { session_cid: cid },
                    virtual_dir: virtual_directory,
                    security_level: Default::default(),
                });

                drop(lock);
                remote.send(request).await
            };

            match result {
                Ok(_) => {
                    InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                        cid,
                        request_id: Some(request_id),
                    })
                }

                Err(err) => {
                    InternalServiceResponse::DeleteVirtualFileFailure(DeleteVirtualFileFailure {
                        cid,
                        message: err.into_string(),
                        request_id: Some(request_id),
                    })
                }
            }
        }
        None => {
            error!(target: "citadel","server connection not found");
            InternalServiceResponse::DeleteVirtualFileFailure(DeleteVirtualFileFailure {
                cid,
                message: String::from("Server Connection Not Found"),
                request_id: Some(request_id),
            })
        }
    };

    Some(HandledRequestResult { response, uuid })
}
