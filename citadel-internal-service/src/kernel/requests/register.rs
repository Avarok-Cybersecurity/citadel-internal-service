use crate::kernel::requests::{handle_request, HandledRequestResult};
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use citadel_logging::info;
use citadel_sdk::prelude::ProtocolRemoteExt;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
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
        server_password,
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
            server_password.clone(),
        )
        .await
    {
        Ok(_res) => match connect_after_register {
            false => {
                let response = InternalServiceResponse::RegisterSuccess(
                    citadel_internal_service_types::RegisterSuccess {
                        cid: 0,
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
                    server_password,
                };

                handle_request(this, uuid, connect_command).await
            }
        },
        Err(err) => {
            let response = InternalServiceResponse::RegisterFailure(
                citadel_internal_service_types::RegisterFailure {
                    cid: 0,
                    message: err.into_string(),
                    request_id: Some(request_id),
                },
            );

            Some(HandledRequestResult { response, uuid })
        }
    }
}
