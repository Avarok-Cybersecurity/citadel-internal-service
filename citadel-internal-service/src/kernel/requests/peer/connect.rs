use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, MessageNotification, PeerConnectFailure,
    PeerConnectSuccess,
};
use citadel_logging::{error, info};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::{ProtocolRemoteExt, ProtocolRemoteTargetExt, VirtualTargetType};
use futures::StreamExt;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::PeerConnect {
        request_id,
        cid,
        peer_cid,
        udp_mode,
        session_security_settings,
        peer_session_password,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let already_connected = this
        .server_connection_map
        .lock()
        .await
        .get(&cid)
        .map(|r| r.peers.contains_key(&peer_cid))
        .unwrap_or(false);

    if already_connected {
        let response = InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess {
            cid,
            peer_cid,
            request_id: Some(request_id),
        });

        return Some(HandledRequestResult { response, uuid });
    }

    let client_to_server_remote = ClientServerRemote::new(
        VirtualTargetType::LocalGroupPeer {
            implicated_cid: cid,
            peer_cid,
        },
        remote.clone(),
        session_security_settings,
    );

    let response = match client_to_server_remote.find_target(cid, peer_cid).await {
        Ok(symmetric_identifier_handle_ref) => {
            match symmetric_identifier_handle_ref
                .connect_to_peer_custom(session_security_settings, udp_mode, peer_session_password)
                .await
            {
                Ok(peer_connect_success) => {
                    let (sink, mut stream) = peer_connect_success.channel.split();
                    this.server_connection_map
                        .lock()
                        .await
                        .get_mut(&cid)
                        .unwrap()
                        .add_peer_connection(peer_cid, sink, peer_connect_success.remote);

                    let hm_for_conn = this.tcp_connection_map.clone();

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message =
                                InternalServiceResponse::MessageNotification(MessageNotification {
                                    message: message.into_buffer(),
                                    cid,
                                    peer_cid,
                                    request_id: Some(request_id),
                                });
                            match hm_for_conn.lock().await.get(&uuid) {
                                Some(entry) => {
                                    if let Err(err) = entry.send(message) {
                                        error!(target:"citadel","Error sending message to client: {err:?}");
                                    }
                                }
                                None => {
                                    info!(target:"citadel","Hash map connection not found")
                                }
                            }
                        }
                    };

                    tokio::spawn(connection_read_stream);

                    InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess {
                        cid,
                        peer_cid,
                        request_id: Some(request_id),
                    })
                }

                Err(err) => InternalServiceResponse::PeerConnectFailure(PeerConnectFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                }),
            }
        }

        Err(err) => InternalServiceResponse::PeerConnectFailure(PeerConnectFailure {
            cid,
            message: err.into_string(),
            request_id: Some(request_id),
        }),
    };

    Some(HandledRequestResult { response, uuid })
}
