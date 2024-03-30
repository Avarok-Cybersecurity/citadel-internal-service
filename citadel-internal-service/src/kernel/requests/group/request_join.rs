use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    GroupRequestJoinFailure, GroupRequestJoinSuccess, InternalServiceRequest,
    InternalServiceResponse,
};
use citadel_sdk::prelude::{
    GroupBroadcast, GroupBroadcastCommand, GroupEvent, NodeRequest, NodeResult,
};
use futures::StreamExt;
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupRequestJoin {
        cid,
        group_key,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };

    let mut server_connection_map = this.server_connection_map.lock().await;
    let response = if let Some(connection) = server_connection_map.get_mut(&cid) {
        let target_cid = group_key.cid;
        if let Some(peer_connection) = connection.peers.get_mut(&target_cid) {
            let peer_remote = peer_connection.remote.clone();
            drop(server_connection_map);
            let group_request = GroupBroadcast::RequestJoin {
                sender: cid,
                key: group_key,
            };
            let request = NodeRequest::GroupBroadcastCommand(GroupBroadcastCommand {
                implicated_cid: cid,
                command: group_request,
            });
            match peer_remote.send_callback_subscription(request).await {
                Ok(mut subscription) => {
                    let mut result = Err("Group Request Join Failed".to_string());
                    while let Some(evt) = subscription.next().await {
                        if let NodeResult::GroupEvent(GroupEvent {
                            implicated_cid: _,
                            ticket: _,
                            event:
                                GroupBroadcast::RequestJoinPending {
                                    result: signal_result,
                                    key: _key,
                                },
                        }) = evt
                        {
                            result = signal_result;
                            break;
                        }
                    }
                    match result {
                        Ok(_) => InternalServiceResponse::GroupRequestJoinSuccess(
                            GroupRequestJoinSuccess {
                                cid,
                                group_key,
                                request_id: Some(request_id),
                            },
                        ),
                        Err(err) => InternalServiceResponse::GroupRequestJoinFailure(
                            GroupRequestJoinFailure {
                                cid,
                                message: err.to_string(),
                                request_id: Some(request_id),
                            },
                        ),
                    }
                }
                Err(err) => {
                    InternalServiceResponse::GroupRequestJoinFailure(GroupRequestJoinFailure {
                        cid,
                        message: err.to_string(),
                        request_id: Some(request_id),
                    })
                }
            }
        } else {
            InternalServiceResponse::GroupRequestJoinFailure(GroupRequestJoinFailure {
                cid,
                message: "Could not Request to join Group - Peer not found".to_string(),
                request_id: Some(request_id),
            })
        }
    } else {
        InternalServiceResponse::GroupRequestJoinFailure(GroupRequestJoinFailure {
            cid,
            message: "Could not Request to join Group - Connection not found".to_string(),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
