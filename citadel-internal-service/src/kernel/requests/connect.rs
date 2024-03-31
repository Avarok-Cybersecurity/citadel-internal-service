use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{create_client_server_remote, CitadelWorkspaceService, Connection};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    ConnectFailure, InternalServiceRequest, InternalServiceResponse, MessageNotification,
};
use citadel_sdk::prelude::{AuthenticationRequest, ProtocolRemoteExt};
use futures::StreamExt;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Connect {
        request_id,
        username,
        password,
        connect_mode,
        udp_mode,
        keep_alive_timeout,
        session_security_settings,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    match remote
        .connect(
            AuthenticationRequest::credentialed(username, password),
            connect_mode,
            udp_mode,
            keep_alive_timeout,
            session_security_settings,
        )
        .await
    {
        Ok(conn_success) => {
            let cid = conn_success.cid;

            let (sink, mut stream) = conn_success.channel.split();
            let client_server_remote = create_client_server_remote(
                stream.vconn_type,
                remote.clone(),
                session_security_settings,
            );
            let connection_struct = Connection::new(sink, client_server_remote, uuid);
            this.server_connection_map
                .lock()
                .await
                .insert(cid, connection_struct);

            let hm_for_conn = this.tcp_connection_map.clone();

            let response = InternalServiceResponse::ConnectSuccess(
                citadel_internal_service_types::ConnectSuccess {
                    cid,
                    request_id: Some(request_id),
                },
            );

            let connection_read_stream = async move {
                while let Some(message) = stream.next().await {
                    let message =
                        InternalServiceResponse::MessageNotification(MessageNotification {
                            message: message.into_buffer(),
                            cid,
                            peer_cid: 0,
                            request_id: Some(request_id),
                        });
                    let lock = hm_for_conn.lock().await;
                    match lock.get(&uuid) {
                        Some(entry) => {
                            if let Err(err) = entry.send(message) {
                                citadel_logging::error!(target:"citadel","Error sending message to client: {err:?}");
                            }
                        }
                        None => {
                            citadel_logging::info!(target:"citadel","Hash map connection not found")
                        }
                    }
                }
            };

            tokio::spawn(connection_read_stream);

            Some(HandledRequestResult { response, uuid })
        }

        Err(err) => {
            let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                message: err.into_string(),
                request_id: Some(request_id),
            });

            Some(HandledRequestResult { response, uuid })
        }
    }
}
