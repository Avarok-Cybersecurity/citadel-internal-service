use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, ListRegisteredPeersFailure,
    ListRegisteredPeersResponse, PeerInformation,
};
use citadel_sdk::prelude::ProtocolRemoteExt;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::ListRegisteredPeers { request_id, cid } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    match remote.get_local_group_mutual_peers(cid).await {
        Ok(peers) => {
            let peers = ListRegisteredPeersResponse {
                cid,
                peers: peers
                    .clone()
                    .into_iter()
                    .filter(|peer| peer.cid != cid)
                    .map(|peer| {
                        (
                            peer.cid,
                            PeerInformation {
                                cid,
                                online_status: peer.is_online,
                                name: peer.full_name,
                                username: peer.username,
                            },
                        )
                    })
                    .collect(),
                request_id: Some(request_id),
            };

            let response = InternalServiceResponse::ListRegisteredPeersResponse(peers);
            Some(HandledRequestResult { response, uuid })
        }

        Err(err) => {
            let response =
                InternalServiceResponse::ListRegisteredPeersFailure(ListRegisteredPeersFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                });

            Some(HandledRequestResult { response, uuid })
        }
    }
}
