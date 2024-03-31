use crate::io_interface::IOInterface;
use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_types::{DisconnectNotification, InternalServiceResponse};
use citadel_sdk::prelude::{Disconnect, NetworkError, VirtualTargetType};

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    disconnect: Disconnect,
) -> Result<(), NetworkError> {
    if let Some(conn) = disconnect.v_conn_type {
        let (signal, conn_uuid) = match conn {
            VirtualTargetType::LocalGroupServer { implicated_cid } => {
                let mut server_connection_map = this.server_connection_map.lock().await;
                if let Some(conn) = server_connection_map.remove(&implicated_cid) {
                    (
                        InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                            cid: implicated_cid,
                            peer_cid: None,
                            request_id: None,
                        }),
                        conn.associated_tcp_connection,
                    )
                } else {
                    return Ok(());
                }
            }
            VirtualTargetType::LocalGroupPeer {
                implicated_cid,
                peer_cid,
            } => {
                if let Some(conn) = this.clear_peer_connection(implicated_cid, peer_cid).await {
                    (
                        InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                            cid: implicated_cid,
                            peer_cid: Some(peer_cid),
                            request_id: None,
                        }),
                        conn.associated_tcp_connection,
                    )
                } else {
                    return Ok(());
                }
            }
            _ => return Ok(()),
        };

        return send_response_to_tcp_client(&this.tcp_connection_map, signal, conn_uuid).await;
    } else {
        citadel_logging::warn!(target: "citadel", "The disconnect request does not contain a connection type")
    }

    Ok(())
}
