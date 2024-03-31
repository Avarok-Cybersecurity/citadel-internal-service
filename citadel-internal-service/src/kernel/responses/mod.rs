use crate::kernel::CitadelWorkspaceService;

use crate::io_interface::IOInterface;
use citadel_logging::info;
use citadel_sdk::prelude::{NetworkError, NodeResult};

mod disconnect;
mod object_transfer_handle;

mod group_channel_created;
pub(crate) mod group_event;
mod peer_event;

pub async fn handle_node_result<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
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
