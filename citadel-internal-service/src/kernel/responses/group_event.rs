use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GroupDisconnectNotification, GroupEndNotification, GroupInviteNotification,
    GroupJoinRequestNotification, GroupLeaveNotification, GroupMemberStateChangeNotification,
    GroupMembershipResponse, GroupMessageNotification, GroupMessageResponse,
    GroupRequestJoinDeclineResponse, GroupRequestJoinPendingNotification, InternalServiceResponse,
};
use citadel_sdk::prelude::{GroupBroadcast, GroupEvent, NetworkError, Ratchet};

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    group_event: GroupEvent,
) -> Result<(), NetworkError> {
    let server_connection_map = &this.server_connection_map;
    let group_broadcast = group_event.event;
    let implicated_cid = group_event.session_cid;
    let tcp_connection_map = &this.tcp_connection_map;

    let mut server_connection_map = server_connection_map.lock().await;
    if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
        let response = match group_broadcast {
            GroupBroadcast::Invitation {
                sender: peer_cid,
                key: group_key,
            } => Some(InternalServiceResponse::GroupInviteNotification(
                GroupInviteNotification {
                    cid: implicated_cid,
                    peer_cid,
                    group_key,
                    request_id: None,
                },
            )),

            GroupBroadcast::RequestJoin {
                sender: peer_cid,
                key: group_key,
            } => connection
                .groups
                .get_mut(&group_key)
                .map(|_group_connection| {
                    InternalServiceResponse::GroupJoinRequestNotification(
                        GroupJoinRequestNotification {
                            cid: implicated_cid,
                            peer_cid,
                            group_key,
                            request_id: None,
                        },
                    )
                }),

            GroupBroadcast::AcceptMembership { target: _, key: _ } => None,

            GroupBroadcast::DeclineMembership { target: _, key } => {
                Some(InternalServiceResponse::GroupRequestJoinDeclineResponse(
                    GroupRequestJoinDeclineResponse {
                        cid: implicated_cid,
                        group_key: key,
                        request_id: None,
                    },
                ))
            }

            GroupBroadcast::Message {
                sender: peer_cid,
                key: group_key,
                message,
            } => connection
                .groups
                .get_mut(&group_key)
                .map(|_group_connection| {
                    InternalServiceResponse::GroupMessageNotification(GroupMessageNotification {
                        cid: implicated_cid,
                        peer_cid,
                        message: message.into_buffer(),
                        group_key,
                        request_id: None,
                    })
                }),

            GroupBroadcast::MessageResponse {
                key: group_key,
                success,
            } => connection
                .groups
                .get_mut(&group_key)
                .map(|_group_connection| {
                    InternalServiceResponse::GroupMessageResponse(GroupMessageResponse {
                        cid: implicated_cid,
                        success,
                        group_key,
                        request_id: None,
                    })
                }),

            GroupBroadcast::MemberStateChanged {
                key: group_key,
                state,
            } => Some(InternalServiceResponse::GroupMemberStateChangeNotification(
                GroupMemberStateChangeNotification {
                    cid: implicated_cid,
                    group_key,
                    state,
                    request_id: None,
                },
            )),

            GroupBroadcast::LeaveRoomResponse {
                key: group_key,
                success,
                message,
            } => Some(InternalServiceResponse::GroupLeaveNotification(
                GroupLeaveNotification {
                    cid: implicated_cid,
                    group_key,
                    success,
                    message,
                    request_id: None,
                },
            )),

            GroupBroadcast::EndResponse {
                key: group_key,
                success,
            } => Some(InternalServiceResponse::GroupEndNotification(
                GroupEndNotification {
                    cid: implicated_cid,
                    group_key,
                    success,
                    request_id: None,
                },
            )),

            GroupBroadcast::Disconnected { key: group_key } => connection
                .groups
                .get_mut(&group_key)
                .map(|_group_connection| {
                    InternalServiceResponse::GroupDisconnectNotification(
                        GroupDisconnectNotification {
                            cid: implicated_cid,
                            group_key,
                            request_id: None,
                        },
                    )
                }),

            GroupBroadcast::AddResponse {
                key: _group_key,
                failed_to_invite_list: _failed_to_invite_list,
            } => None,

            GroupBroadcast::AcceptMembershipResponse { key, success } => {
                connection.groups.get_mut(&key).map(|_group_connection| {
                    InternalServiceResponse::GroupMembershipResponse(GroupMembershipResponse {
                        cid: implicated_cid,
                        group_key: key,
                        success,
                        request_id: None,
                    })
                })
            }

            GroupBroadcast::KickResponse {
                key: _group_key,
                success: _success,
            } => None,

            GroupBroadcast::ListResponse {
                groups: _group_list,
            } => None,

            GroupBroadcast::CreateResponse { key: _group_key } => None,

            GroupBroadcast::GroupNonExists { key: _group_key } => None,

            GroupBroadcast::RequestJoinPending { result, key } => Some(
                InternalServiceResponse::GroupRequestJoinPendingNotification(
                    GroupRequestJoinPendingNotification {
                        cid: implicated_cid,
                        group_key: key,
                        result,
                        request_id: None,
                    },
                ),
            ),

            _ => None,
        };
        match response {
            Some(internal_service_response) => {
                if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
                    let associated_tcp_connection = connection.associated_tcp_connection;
                    drop(server_connection_map);
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        internal_service_response,
                        associated_tcp_connection,
                    )
                    .await?;
                }
            }
            None => {
                citadel_logging::warn!(target: "citadel", "No handler generated for GroupBroadcast")
            }
        }
    }

    Ok(())
}
