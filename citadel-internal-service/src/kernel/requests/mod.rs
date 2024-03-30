use crate::kernel::CitadelWorkspaceService;
use async_recursion::async_recursion;
use citadel_internal_service_types::*;
use citadel_logging::info;
use citadel_logging::tracing::log;

use citadel_sdk::prelude::*;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use uuid::Uuid;

pub(crate) struct HandledRequestResult {
    pub response: InternalServiceResponse,
    pub uuid: Uuid,
}

mod connect;
mod delete_virtual_file;
mod disconnect;
mod download_file;
mod get_account_information;
mod get_sessions;
mod message;
mod register;
mod respond_file_transfer;
mod send_file;

mod group;
mod local_db;
mod peer;

#[async_recursion]
pub async fn handle_request(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    command: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    match &command {
        InternalServiceRequest::GetAccountInformation { .. } => {
            return get_account_information::handle(this, uuid, command).await
        }
        InternalServiceRequest::GetSessions { .. } => {
            return get_sessions::handle(this, uuid, command).await
        }
        InternalServiceRequest::Connect { .. } => {
            return connect::handle(this, uuid, command).await
        }
        InternalServiceRequest::Register { .. } => {
            return register::handle(this, uuid, command).await
        }
        InternalServiceRequest::Message { .. } => {
            return message::handle(this, uuid, command).await
        }

        InternalServiceRequest::Disconnect { .. } => {
            return disconnect::handle(this, uuid, command).await
        }

        InternalServiceRequest::SendFile { .. } => {
            return send_file::handle(this, uuid, command).await
        }

        InternalServiceRequest::RespondFileTransfer { .. } => {
            return respond_file_transfer::handle(this, uuid, command).await
        }

        InternalServiceRequest::DownloadFile { .. } => {
            return download_file::handle(this, uuid, command).await
        }

        InternalServiceRequest::DeleteVirtualFile { .. } => {
            return delete_virtual_file::handle(this, uuid, command).await
        }

        InternalServiceRequest::ListRegisteredPeers { .. } => {
            return peer::list_registered::handle(this, uuid, command).await
        }

        InternalServiceRequest::ListAllPeers { .. } => {
            return peer::list_all::handle(this, uuid, command).await
        }

        InternalServiceRequest::PeerRegister { .. } => {
            return peer::register::handle(this, uuid, command).await
        }

        InternalServiceRequest::PeerConnect { .. } => {
            return peer::connect::handle(this, uuid, command).await
        }

        InternalServiceRequest::PeerDisconnect { .. } => {
            return peer::disconnect::handle(this, uuid, command).await
        }

        InternalServiceRequest::LocalDBGetKV { .. } => {
            return local_db::get_kv::handle(this, uuid, command).await
        }

        InternalServiceRequest::LocalDBSetKV { .. } => {
            return local_db::set_kv::handle(this, uuid, command).await
        }

        InternalServiceRequest::LocalDBDeleteKV { .. } => {
            return local_db::delete_kv::handle(this, uuid, command).await
        }

        InternalServiceRequest::LocalDBGetAllKV { .. } => {
            return local_db::get_all_kv::handle(this, uuid, command).await
        }

        InternalServiceRequest::LocalDBClearAllKV { .. } => {
            return local_db::clear_all_kv::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupCreate { .. } => {
            return group::create::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupLeave { .. } => {
            return group::leave::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupEnd { .. } => {
            return group::end::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupMessage { .. } => {
            return group::message::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupInvite { .. } => {
            return group::invite::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupKick { .. } => {
            return group::kick::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupListGroupsFor { .. } => {
            return group::group_list_groups::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupRespondRequest { .. } => {
            return group::respond_request::handle(this, uuid, command).await
        }

        InternalServiceRequest::GroupRequestJoin { .. } => {
            return group::request_join::handle(this, uuid, command).await
        }
    };
}

pub(crate) fn spawn_group_channel_receiver(
    group_key: MessageGroupKey,
    implicated_cid: u64,
    uuid: Uuid,
    mut rx: GroupChannelRecvHalf,
    tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    // Handler/Receiver for Group Channel Broadcasts that aren't handled in on_node_event_received in Kernel
    let group_channel_receiver = async move {
        while let Some(inbound_group_broadcast) = rx.next().await {
            // Gets UnboundedSender to the TCP client to forward Broadcasts
            match tcp_connection_map.lock().await.get(&uuid) {
                Some(entry) => {
                    log::trace!(target:"citadel", "User {implicated_cid:?} Received Group Broadcast: {inbound_group_broadcast:?}");
                    let message = match inbound_group_broadcast {
                        GroupBroadcastPayload::Message { payload, sender } => {
                            Some(InternalServiceResponse::GroupMessageNotification(
                                GroupMessageNotification {
                                    cid: implicated_cid,
                                    peer_cid: sender,
                                    message: payload.into_buffer(),
                                    group_key,
                                    request_id: None,
                                },
                            ))
                        }
                        GroupBroadcastPayload::Event { payload } => match payload {
                            GroupBroadcast::RequestJoin { sender, key: _ } => {
                                Some(InternalServiceResponse::GroupJoinRequestNotification(
                                    GroupJoinRequestNotification {
                                        cid: implicated_cid,
                                        peer_cid: sender,
                                        group_key,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::MemberStateChanged { key: _, state } => {
                                Some(InternalServiceResponse::GroupMemberStateChangeNotification(
                                    GroupMemberStateChangeNotification {
                                        cid: implicated_cid,
                                        group_key,
                                        state,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::EndResponse { key, success } => {
                                Some(InternalServiceResponse::GroupEndNotification(
                                    GroupEndNotification {
                                        cid: implicated_cid,
                                        group_key: key,
                                        success,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::Disconnected { key } => {
                                Some(InternalServiceResponse::GroupDisconnectNotification(
                                    GroupDisconnectNotification {
                                        cid: implicated_cid,
                                        group_key: key,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::MessageResponse { key, success } => {
                                Some(InternalServiceResponse::GroupMessageResponse(
                                    GroupMessageResponse {
                                        cid: implicated_cid,
                                        group_key: key,
                                        success,
                                        request_id: None,
                                    },
                                ))
                            }
                            // GroupBroadcast::Create { .. } => {},
                            // GroupBroadcast::LeaveRoom { .. } => {},
                            // GroupBroadcast::End { .. } => {},
                            // GroupBroadcast::Add { .. } => {},
                            // GroupBroadcast::AddResponse { .. } => {},
                            // GroupBroadcast::AcceptMembership { .. } => {},
                            // GroupBroadcast::DeclineMembership { .. } => {},
                            // GroupBroadcast::AcceptMembershipResponse { .. } => {},
                            // GroupBroadcast::DeclineMembershipResponse { .. } => {},
                            // GroupBroadcast::Kick { .. } => {},
                            // GroupBroadcast::KickResponse { .. } => {},
                            // GroupBroadcast::ListGroupsFor { .. } => {},
                            // GroupBroadcast::ListResponse { .. } => {},
                            // GroupBroadcast::Invitation { .. } => {},
                            // GroupBroadcast::CreateResponse { .. } => {},
                            // GroupBroadcast::RequestJoinPending { .. } => {},
                            _ => None,
                        },
                    };

                    // Forward Group Broadcast to TCP Client if it was one of the handled broadcasts
                    if let Some(message) = message {
                        if let Err(err) = entry.send(message) {
                            info!(target: "citadel", "Group Channel Forward To TCP Client Failed: {err:?}");
                        }
                    }
                }
                None => {
                    info!(target:"citadel","Connection not found when Group Channel Broadcast Received");
                }
            }
        }
    };

    // Spawns the above Handler for Group Channel Broadcasts not handled in Node Events
    tokio::task::spawn(group_channel_receiver);
}
