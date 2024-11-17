use crate::kernel::{send_response_to_tcp_client, spawn_tick_updater, CitadelWorkspaceService};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{FileTransferRequestNotification, InternalServiceResponse};
use citadel_logging::info;
use citadel_sdk::prelude::{NetworkError, ObjectTransferHandle, ObjectTransferOrientation};

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    object_transfer_handle: ObjectTransferHandle,
) -> Result<(), NetworkError> {
    let metadata = object_transfer_handle.handle.metadata.clone();
    let object_id = metadata.object_id;
    let implicated_cid = object_transfer_handle.implicated_cid;
    let peer_cid = if object_transfer_handle.handle.receiver != implicated_cid {
        object_transfer_handle.handle.receiver
    } else {
        object_transfer_handle.handle.source
    };
    let object_transfer_handler = object_transfer_handle.handle;

    citadel_logging::info!(target: "citadel", "Orientation: {:?}", object_transfer_handler.orientation);
    citadel_logging::info!(target: "citadel", "ObjectTransferHandle has implicated_cid: {implicated_cid:?} and peer_cid {peer_cid:?}");

    // When we receive a handle, there are two possibilities:
    // A: We are the sender of the file transfer, in which case we can assume the adjacent node
    // already accepted the file transfer request, and therefore we can spawn a task to forward
    // the ticks immediately
    //
    // B: We are the receiver of the file transfer. We need to wait for the TCP client to accept
    // the request, thus, we need to store it. UNLESS, this is an revfs pull, in which case we
    // allow the transfer to proceed immediately since the protocol auto accepts these requests
    if let ObjectTransferOrientation::Receiver { is_revfs_pull } =
        object_transfer_handler.orientation
    {
        info!(target: "citadel", "Receiver Obtained ObjectTransferHandler");

        let mut server_connection_map = this.server_connection_map.lock().await;
        if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
            let uuid = connection.associated_tcp_connection;

            if is_revfs_pull {
                spawn_tick_updater(
                    object_transfer_handler,
                    implicated_cid,
                    Some(peer_cid),
                    &mut server_connection_map,
                    this.tcp_connection_map.clone(),
                    None,
                );
            } else {
                // Send an update to the TCP client that way they can choose to accept or reject the transfer
                let response = InternalServiceResponse::FileTransferRequestNotification(
                    FileTransferRequestNotification {
                        cid: implicated_cid,
                        peer_cid,
                        metadata,
                        request_id: None,
                    },
                );

                connection.add_object_transfer_handler(
                    peer_cid,
                    object_id,
                    Some(object_transfer_handler),
                );

                drop(server_connection_map);

                send_response_to_tcp_client(&this.tcp_connection_map, response, uuid).await?;
            }
        }
    } else {
        // Sender - Must spawn a task to relay status updates to TCP client. When receiving this handle,
        // we know the opposite node agreed to the connection thus we can spawn
        let mut server_connection_map = this.server_connection_map.lock().await;
        info!(target: "citadel", "Sender Obtained ObjectTransferHandler");
        spawn_tick_updater(
            object_transfer_handler,
            implicated_cid,
            Some(peer_cid),
            &mut server_connection_map,
            this.tcp_connection_map.clone(),
            None,
        );
    }

    Ok(())
}
