use crate::kernel::requests::{handle_request, HandledRequestResult};
use crate::kernel::{send_response_to_tcp_client, CitadelWorkspaceService};
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use citadel_logging::info;
use citadel_sdk::prelude::ProtocolRemoteExt;
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Register {
        request_id,
        server_addr,
        full_name,
        username,
        proposed_password,
        connect_after_register,
        session_security_settings,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    info!(target: "citadel", "About to connect to server {server_addr:?} for user {username}");
    match remote
        .register(
            server_addr,
            full_name,
            username.clone(),
            proposed_password.clone(),
            session_security_settings,
        )
        .await
    {
        Ok(_res) => match connect_after_register {
            false => {
                let response = InternalServiceResponse::RegisterSuccess(
                    citadel_internal_service_types::RegisterSuccess {
                        request_id: Some(request_id),
                    },
                );

                Some(HandledRequestResult { response, uuid })
            }
            true => {
                let connect_command = InternalServiceRequest::Connect {
                    username,
                    password: proposed_password,
                    keep_alive_timeout: None,
                    udp_mode: Default::default(),
                    connect_mode: Default::default(),
                    session_security_settings,
                    request_id,
                };

                return handle_request(this, uuid, connect_command).await;
            }
        },
        Err(err) => {
            let response = InternalServiceResponse::RegisterFailure(
                citadel_internal_service_types::RegisterFailure {
                    message: err.into_string(),
                    request_id: Some(request_id),
                },
            );

            Some(HandledRequestResult { response, uuid })
        }
    }
}
