use crate::io_interface::IOInterface;
use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    GroupEndFailure, GroupEndSuccess, InternalServiceRequest, InternalServiceResponse,
};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupEnd {
        cid,
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
            match group_sender.end().await {
                Ok(_) => InternalServiceResponse::GroupEndSuccess(GroupEndSuccess {
                    cid,
                    group_key,
                    request_id: Some(request_id),
                }),
                Err(err) => InternalServiceResponse::GroupEndFailure(GroupEndFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                }),
            }
        } else {
            InternalServiceResponse::GroupEndFailure(GroupEndFailure {
                cid,
                message: "Could Not Leave Group - Group Connection not found".to_string(),
                request_id: Some(request_id),
            })
        }
    } else {
        InternalServiceResponse::GroupEndFailure(GroupEndFailure {
            cid,
            message: "Could Not Leave Group - Connection not found".to_string(),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
