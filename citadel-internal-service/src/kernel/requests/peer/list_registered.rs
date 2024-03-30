use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    InternalServiceRequest, InternalServiceResponse, ListRegisteredPeersFailure,
    ListRegisteredPeersResponse, PeerSessionInformation,
};
use citadel_sdk::prelude::{ProtocolRemoteExt, TargetLockedRemote};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn handle(
    this: &CitadelWorkspaceService,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::ListRegisteredPeers { request_id, cid } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    match remote.get_local_group_mutual_peers(cid).await {
        Ok(peers) => {
            let mut accounts = HashMap::new();
            for peer in &peers {
                // TOOD: Do not unwrap below
                let peer_username = remote
                    .find_target(cid, peer.cid)
                    .await
                    .unwrap()
                    .target_username()
                    .cloned()
                    .unwrap_or_default();
                accounts.insert(
                    peer.cid,
                    PeerSessionInformation {
                        cid,
                        peer_cid: peer.cid,
                        peer_username,
                    },
                );
            }

            let peers = ListRegisteredPeersResponse {
                cid,
                peers: accounts,
                online_status: peers
                    .iter()
                    .map(|peer| (peer.cid, peer.is_online))
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
