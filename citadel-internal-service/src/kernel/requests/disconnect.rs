use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    DisconnectFailure, DisconnectNotification, InternalServiceRequest, InternalServiceResponse,
};
use citadel_logging::info;
use citadel_sdk::prelude::{DisconnectFromHypernode, NodeRequest};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Disconnect { request_id, cid } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let request = NodeRequest::DisconnectFromHypernode(DisconnectFromHypernode {
        implicated_cid: cid,
    });

    this.server_connection_map.lock().await.remove(&cid);

    match remote.send(request).await {
        Ok(_res) => {
            let disconnect_success =
                InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                    cid,
                    peer_cid: None,
                    request_id: Some(request_id),
                });
            Some(HandledRequestResult {
                response: disconnect_success,
                uuid,
            })
        }
        Err(err) => {
            let error_message = format!("Failed to disconnect {err:?}");
            info!(target: "citadel", "{error_message}");
            let disconnect_failure =
                InternalServiceResponse::DisconnectFailure(DisconnectFailure {
                    cid,
                    message: error_message,
                    request_id: Some(request_id),
                });
            Some(HandledRequestResult {
                response: disconnect_failure,
                uuid,
            })
        }
    }
}
