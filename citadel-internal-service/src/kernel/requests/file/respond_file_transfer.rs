use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{spawn_tick_updater, CitadelWorkspaceService};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    FileTransferStatusNotification, InternalServiceRequest, InternalServiceResponse,
};
use citadel_sdk::prelude::Ratchet;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    // TODO: Factor-in download location?
    let InternalServiceRequest::RespondFileTransfer {
        cid,
        peer_cid,
        object_id,
        accept,
        download_location: _,
        request_id,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let mut server_connection_map = this.server_connection_map.lock().await;

    let response = if let Some(connection) = server_connection_map.get_mut(&cid) {
        if let Some(mut handler) = connection.take_file_transfer_handle(peer_cid, object_id) {
            if let Some(mut owned_handler) = handler.take() {
                let result = if accept {
                    let accept_result = owned_handler.accept();
                    spawn_tick_updater(
                        owned_handler,
                        cid,
                        Some(peer_cid),
                        &mut server_connection_map,
                        this.tcp_connection_map.clone(),
                        Some(request_id),
                    );

                    accept_result
                } else {
                    owned_handler.decline()
                };
                match result {
                    Ok(_) => InternalServiceResponse::FileTransferStatusNotification(
                        FileTransferStatusNotification {
                            cid,
                            object_id,
                            success: true,
                            response: accept,
                            message: None,
                            request_id: Some(request_id),
                        },
                    ),

                    Err(err) => InternalServiceResponse::FileTransferStatusNotification(
                        FileTransferStatusNotification {
                            cid,
                            object_id,
                            success: false,
                            response: accept,
                            message: Option::from(err.into_string()),
                            request_id: Some(request_id),
                        },
                    ),
                }
            } else {
                InternalServiceResponse::FileTransferStatusNotification(
                    FileTransferStatusNotification {
                        cid,
                        object_id,
                        success: false,
                        response: accept,
                        message: Some("File transfer handlernot found".to_string()),
                        request_id: Some(request_id),
                    },
                )
            }
        } else {
            InternalServiceResponse::FileTransferStatusNotification(
                FileTransferStatusNotification {
                    cid,
                    object_id,
                    success: false,
                    response: accept,
                    message: Some("File transfer not found".to_string()),
                    request_id: Some(request_id),
                },
            )
        }
    } else {
        InternalServiceResponse::FileTransferStatusNotification(FileTransferStatusNotification {
            cid,
            object_id,
            success: false,
            response: accept,
            message: Some("Connection not found".to_string()),
            request_id: Some(request_id),
        })
    };

    Some(HandledRequestResult { response, uuid })
}
