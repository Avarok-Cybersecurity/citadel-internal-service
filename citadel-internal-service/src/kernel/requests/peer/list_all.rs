use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, ListAllPeersFailure, ListAllPeersResponse,
};
use citadel_sdk::prelude::ProtocolRemoteExt;
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::ListAllPeers { request_id, cid } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    match remote.get_local_group_peers(cid, None).await {
        Ok(peers) => {
            let peers = ListAllPeersResponse {
                cid,
                online_status: peers
                    .into_iter()
                    .filter(|peer| peer.cid != cid)
                    .map(|peer| (peer.cid, peer.is_online))
                    .collect(),
                request_id: Some(request_id),
            };

            let response = InternalServiceResponse::ListAllPeersResponse(peers);
            Some(HandledRequestResult { response, uuid })
        }

        Err(err) => {
            let response = InternalServiceResponse::ListAllPeersFailure(ListAllPeersFailure {
                cid,
                message: err.into_string(),
                request_id: Some(request_id),
            });

            Some(HandledRequestResult { response, uuid })
        }
    }
}
