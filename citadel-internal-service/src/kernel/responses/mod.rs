use crate::kernel::{
    requests, send_response_to_tcp_client, spawn_tick_updater, CitadelWorkspaceService,
    GroupConnection,
};
use citadel_internal_service_types::{
    DisconnectNotification, FileTransferRequestNotification, GroupChannelCreateSuccess,
    InternalServiceResponse, PeerConnectNotification, PeerRegisterNotification,
};
use citadel_logging::info;
use citadel_sdk::prelude::{
    NetworkError, NodeResult, ObjectTransferOrientation, PeerConnectionType, PeerSignal,
    VirtualTargetType,
};

mod disconnect;
mod object_transfer_handle;

mod group_channel_created;
pub(crate) mod group_event;
mod peer_event;

pub async fn handle_node_result(
    this: &CitadelWorkspaceService,
    result: NodeResult,
) -> Result<(), NetworkError> {
    info!(target: "citadel", "NODE EVENT RECEIVED WITH MESSAGE: {result:?}");
    match result {
        NodeResult::Disconnect(dc) => return disconnect::handle(this, dc).await,
        NodeResult::ObjectTransferHandle(object_transfer_handle) => {
            return object_transfer_handle::handle(this, object_transfer_handle).await
        }
        NodeResult::GroupChannelCreated(group_channel_created) => {
            return group_channel_created::handle(this, group_channel_created).await
        }
        NodeResult::PeerEvent(event) => return peer_event::handle(this, event).await,

        NodeResult::GroupEvent(group_event) => return group_event::handle(this, group_event).await,

        evt => {
            citadel_logging::warn!(target: "citadel", "Unhandled node result: {evt:?}")
        }
    }

    Ok(())
}
