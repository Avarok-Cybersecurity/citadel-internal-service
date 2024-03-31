use crate::io_interface::IOInterface;
use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, PeerDisconnectFailure, PeerDisconnectSuccess,
};
use citadel_sdk::prelude::{NodeRequest, PeerCommand, PeerConnectionType, PeerSignal};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::PeerDisconnect {
        request_id,
        cid,
        peer_cid,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let request = NodeRequest::PeerCommand(PeerCommand {
        implicated_cid: cid,
        command: PeerSignal::Disconnect {
            peer_conn_type: PeerConnectionType::LocalGroupPeer {
                implicated_cid: cid,
                peer_cid,
            },
            disconnect_response: None,
        },
    });

    let mut lock = this.server_connection_map.lock().await;

    let response = match lock.get_mut(&cid) {
        None => InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
            cid,
            message: "Server connection not found".to_string(),
            request_id: Some(request_id),
        }),
        Some(conn) => match conn.peers.get_mut(&peer_cid) {
            None => InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
                cid,
                message: format!("Peer Connection {peer_cid }Not Found"),
                request_id: Some(request_id),
            }),
            Some(_) => {
                drop(lock);

                match remote.send(request).await {
                    Ok(_) => {
                        this.server_connection_map
                            .lock()
                            .await
                            .get_mut(&cid)
                            .map(|r| r.clear_peer_connection(peer_cid));

                        InternalServiceResponse::PeerDisconnectSuccess(PeerDisconnectSuccess {
                            cid,
                            request_id: Some(request_id),
                        })
                    }
                    Err(err) => {
                        InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
                            cid,
                            message: format!("Failed to disconnect: {err:?}"),
                            request_id: Some(request_id),
                        })
                    }
                }
            }
        },
    };

    Some(HandledRequestResult { response, uuid })
}
