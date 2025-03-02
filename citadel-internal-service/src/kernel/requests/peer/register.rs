use crate::kernel::requests::{handle_request, HandledRequestResult};
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, PeerRegisterFailure, PeerRegisterSuccess,
};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::{
    ProtocolRemoteExt, ProtocolRemoteTargetExt, Ratchet, VirtualTargetType,
};
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::PeerRegister {
        request_id,
        cid,
        peer_cid,
        session_security_settings,
        connect_after_register,
        peer_session_password,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let client_to_server_remote = ClientServerRemote::new(
        VirtualTargetType::LocalGroupServer { session_cid: cid },
        remote.clone(),
        session_security_settings,
        None,
        None,
    );

    let response = match client_to_server_remote.propose_target(cid, peer_cid).await {
        Ok(symmetric_identifier_handle_ref) => {
            match symmetric_identifier_handle_ref.register_to_peer().await {
                Ok(_peer_register_success) => {
                    let account_manager = symmetric_identifier_handle_ref.account_manager();
                    match account_manager.find_target_information(cid, peer_cid).await {
                        Ok(target_information) => {
                            let (_, mutual_peer) = target_information.unwrap();
                            match connect_after_register {
                                true => {
                                    let connect_command = InternalServiceRequest::PeerConnect {
                                        cid,
                                        peer_cid: mutual_peer.cid,
                                        udp_mode: Default::default(),
                                        session_security_settings,
                                        request_id,
                                        peer_session_password,
                                    };

                                    return handle_request(this, uuid, connect_command).await;
                                }
                                false => InternalServiceResponse::PeerRegisterSuccess(
                                    PeerRegisterSuccess {
                                        cid,
                                        peer_cid: mutual_peer.cid,
                                        peer_username: mutual_peer
                                            .username
                                            .clone()
                                            .unwrap_or_default(),
                                        request_id: Some(request_id),
                                    },
                                ),
                            }
                        }
                        Err(err) => {
                            InternalServiceResponse::PeerRegisterFailure(PeerRegisterFailure {
                                cid,
                                message: err.into_string(),
                                request_id: Some(request_id),
                            })
                        }
                    }
                }

                Err(err) => InternalServiceResponse::PeerRegisterFailure(PeerRegisterFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                }),
            }
        }

        Err(err) => InternalServiceResponse::PeerRegisterFailure(PeerRegisterFailure {
            cid,
            message: err.into_string(),
            request_id: Some(request_id),
        }),
    };

    Some(HandledRequestResult { response, uuid })
}
