use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    DisconnectNotification, InternalServiceResponse, PeerConnectNotification,
    PeerRegisterNotification,
};
use citadel_logging::info;
use citadel_sdk::prelude::{
    GroupEvent, NetworkError, PeerConnectionType, PeerEvent, PeerSignal, Ratchet,
};

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    event: PeerEvent,
) -> Result<(), NetworkError> {
    match event.event {
        PeerSignal::Disconnect {
            peer_conn_type:
                PeerConnectionType::LocalGroupPeer {
                    session_cid,
                    peer_cid,
                },
            disconnect_response: _,
        } => {
            if let Some(conn) = this.clear_peer_connection(session_cid, peer_cid).await {
                let response =
                    InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                        cid: session_cid,
                        peer_cid: Some(peer_cid),
                        request_id: None,
                    });
                send_response_to_tcp_client(
                    &this.tcp_connection_map,
                    response,
                    conn.associated_tcp_connection,
                )
                .await?;
            }
        }
        PeerSignal::BroadcastConnected {
            session_cid,
            group_broadcast,
        } => {
            let evt = GroupEvent {
                session_cid,
                ticket: event.ticket,
                event: group_broadcast,
            };
            return super::group_event::handle(this, evt).await;
        }
        PeerSignal::PostRegister {
            peer_conn_type:
                PeerConnectionType::LocalGroupPeer {
                    session_cid: peer_cid,
                    peer_cid: session_cid,
                },
            inviter_username,
            invitee_username: _,
            ticket_opt: _,
            invitee_response: _,
        } => {
            info!(target: "citadel", "User {session_cid:?} received Register Request from {peer_cid:?}");
            let mut server_connection_map = this.server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&session_cid) {
                let response =
                    InternalServiceResponse::PeerRegisterNotification(PeerRegisterNotification {
                        cid: session_cid,
                        peer_cid,
                        peer_username: inviter_username,
                        request_id: None,
                    });

                let associated_tcp_connection = connection.associated_tcp_connection;
                drop(server_connection_map);
                send_response_to_tcp_client(
                    &this.tcp_connection_map,
                    response,
                    associated_tcp_connection,
                )
                .await?;
            }
        }
        PeerSignal::PostConnect {
            peer_conn_type:
                PeerConnectionType::LocalGroupPeer {
                    session_cid: peer_cid,
                    peer_cid: session_cid,
                },
            ticket_opt: _,
            invitee_response: _,
            session_security_settings,
            udp_mode,
            session_password: _,
        } => {
            info!(target: "citadel", "User {session_cid:?} received Connect Request from {peer_cid:?}");
            let mut server_connection_map = this.server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&session_cid) {
                let response =
                    InternalServiceResponse::PeerConnectNotification(PeerConnectNotification {
                        cid: session_cid,
                        peer_cid,
                        session_security_settings,
                        udp_mode,
                        request_id: None,
                    });

                let associated_tcp_connection = connection.associated_tcp_connection;
                drop(server_connection_map);
                send_response_to_tcp_client(
                    &this.tcp_connection_map,
                    response,
                    associated_tcp_connection,
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}
