use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GroupInviteFailure, GroupInviteSuccess, InternalServiceRequest, InternalServiceResponse,
};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupInvite {
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
            match group_sender.invite(peer_cid).await {
                Ok(_) => InternalServiceResponse::GroupInviteSuccess(GroupInviteSuccess {
                    cid,
                    group_key,
                    request_id: Some(request_id),
                }),
                Err(err) => InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
                    cid,
                    message: err.to_string(),
                    request_id: Some(request_id),
                }),
            }
        } else {
            InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
                cid,
                message: "Could Not Invite to Group - Group Connection not found".to_string(),
                request_id: Some(request_id),
            })
        }
    } else {
        InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
            cid,
            message: "Could Not Invite to Group - Connection not found".to_string(),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
