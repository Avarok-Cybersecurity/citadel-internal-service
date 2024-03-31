use crate::io_interface::IOInterface;
use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    GroupListGroupsFailure, GroupListGroupsSuccess, InternalServiceRequest, InternalServiceResponse,
};
use citadel_sdk::prelude::ProtocolRemoteTargetExt;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupListGroupsFor {
        cid,
        peer_cid,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };

    let mut server_connection_map = this.server_connection_map.lock().await;
    let response = if let Some(connection) = server_connection_map.get_mut(&cid) {
        if let Some(peer_cid) = peer_cid {
            if let Some(peer_connection) = connection.peers.get_mut(&peer_cid) {
                let peer_remote = peer_connection.remote.clone();
                drop(server_connection_map);
                match peer_remote.list_owned_groups().await {
                    Ok(groups) => {
                        InternalServiceResponse::GroupListGroupsSuccess(GroupListGroupsSuccess {
                            cid,
                            peer_cid: Some(peer_cid),
                            request_id: Some(request_id),
                            group_list: Some(groups),
                        })
                    }

                    Err(err) => {
                        InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
                            cid,
                            message: err.to_string(),
                            request_id: Some(request_id),
                        })
                    }
                }
            } else {
                InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
                    cid,
                    message: "Could Not List Groups - Peer not found".to_string(),
                    request_id: Some(request_id),
                })
            }
        } else {
            let remote = connection.client_server_remote.clone();
            drop(server_connection_map);

            match remote.list_owned_groups().await {
                Ok(groups) => {
                    InternalServiceResponse::GroupListGroupsSuccess(GroupListGroupsSuccess {
                        cid,
                        peer_cid: None,
                        request_id: Some(request_id),
                        group_list: Some(groups),
                    })
                }

                Err(err) => {
                    InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
                        cid,
                        message: err.to_string(),
                        request_id: Some(request_id),
                    })
                }
            }
        }
    } else {
        InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
            cid,
            message: "Could Not List Groups - Connection not found".to_string(),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
