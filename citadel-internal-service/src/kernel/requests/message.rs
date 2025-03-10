use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, MessageSendFailure, MessageSendSuccess,
};
use citadel_logging::info;
use citadel_sdk::prelude::Ratchet;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Message {
        request_id,
        message,
        cid,
        peer_cid,
        security_level,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };

    let mut server_connection_map = this.server_connection_map.lock().await;
    match server_connection_map.get_mut(&cid) {
        Some(conn) => {
            let sink = if let Some(peer_cid) = peer_cid {
                // send to peer
                if let Some(peer_conn) = conn.peers.get_mut(&peer_cid) {
                    peer_conn.sink.set_security_level(security_level);
                    &mut peer_conn.sink
                } else {
                    // TODO: refactor all connection not found messages, we have too many duplicates
                    citadel_logging::error!(target: "citadel","connection not found");
                    let response =
                        InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                            cid,
                            message: format!("Connection for {cid} not found"),
                            request_id: Some(request_id),
                        });

                    return Some(HandledRequestResult { response, uuid });
                }
            } else {
                // send to server
                conn.sink_to_server.set_security_level(security_level);
                &mut conn.sink_to_server
            };

            // Note: not dropping the lock should not hold up the conn map for long
            // if it does, we can always use a tx/rx pair
            // drop(server_connection_map);

            if let Err(err) = sink.send(message).await {
                let response = InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                    cid,
                    message: format!("Error sending message: {err:?}"),
                    request_id: Some(request_id),
                });

                Some(HandledRequestResult { response, uuid })
            } else {
                let response = InternalServiceResponse::MessageSendSuccess(MessageSendSuccess {
                    cid,
                    peer_cid,
                    request_id: Some(request_id),
                });

                Some(HandledRequestResult { response, uuid })
            }
        }
        None => {
            info!(target: "citadel","connection not found");
            let response = InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                cid,
                message: format!("Connection for {cid} not found"),
                request_id: Some(request_id),
            });

            Some(HandledRequestResult { response, uuid })
        }
    }
}
