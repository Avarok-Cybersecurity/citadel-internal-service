use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GetSessionsResponse, InternalServiceRequest, InternalServiceResponse, PeerSessionInformation,
    SessionInformation,
};
use citadel_sdk::prelude::{Ratchet, TargetLockedRemote};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GetSessions { request_id } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let server_connection_map = &this.server_connection_map;
    let lock = server_connection_map.lock().await;
    let mut sessions = Vec::new();
    for (cid, connection) in lock.iter() {
        if connection.associated_tcp_connection == uuid {
            let mut session = SessionInformation {
                cid: *cid,
                peer_connections: HashMap::new(),
            };
            for (peer_cid, conn) in connection.peers.iter() {
                session.peer_connections.insert(
                    *peer_cid,
                    PeerSessionInformation {
                        cid: *cid,
                        peer_cid: *peer_cid,
                        peer_username: conn
                            .remote
                            .target_username()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    },
                );
            }
            sessions.push(session);
        }
    }

    let response = InternalServiceResponse::GetSessionsResponse(GetSessionsResponse {
        cid: 0,
        sessions,
        request_id: Some(request_id),
    });

    Some(HandledRequestResult { response, uuid })
}
