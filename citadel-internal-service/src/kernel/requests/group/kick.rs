use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GroupKickFailure, GroupKickSuccess, InternalServiceRequest, InternalServiceResponse,
};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupKick {
        cid,
        peer_cid,
        group_key,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };

    let mut server_connection_map = this.server_connection_map.lock().await;
    let response = if let Some(connection) = server_connection_map.get_mut(&cid) {
        if let Some(group_connection) = connection.groups.get_mut(&group_key) {
            let group_sender = group_connection.tx.clone();
            drop(server_connection_map);
            match group_sender.kick(peer_cid).await {
                Ok(_) => InternalServiceResponse::GroupKickSuccess(GroupKickSuccess {
                    cid,
                    group_key,
                    request_id: Some(request_id),
                }),
                Err(err) => InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                    cid,
                    message: err.to_string(),
                    request_id: Some(request_id),
                }),
            }
        } else {
            InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                cid,
                message: "Could Not Kick from Group - GroupChannel not found".to_string(),
                request_id: Some(request_id),
            })
        }
    } else {
        InternalServiceResponse::GroupKickFailure(GroupKickFailure {
            cid,
            message: "Could Not Kick from Group - GroupChannel not found".to_string(),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
