use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    AccountInformation, Accounts, InternalServiceRequest, InternalServiceResponse,
    PeerSessionInformation,
};
use citadel_sdk::prelude::{CNACMetadata, NodeRemote};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GetAccountInformation { request_id, cid } = request else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let mut accounts_ret = HashMap::new();
    let accounts = remote
        .account_manager()
        .get_persistence_handler()
        .get_clients_metadata(None)
        .await
        .unwrap_or_default();

    // We are only interested in the is_personal=True accounts
    let filtered_accounts = accounts
        .into_iter()
        .filter(|r| r.is_personal)
        .collect::<Vec<_>>();

    if let Some(cid) = cid {
        let account = filtered_accounts.into_iter().find(|r| r.cid == cid);
        if let Some(account) = account {
            add_account_to_map(&mut accounts_ret, account, remote, request_id).await;
        }
    } else {
        for account in filtered_accounts {
            add_account_to_map(&mut accounts_ret, account, remote, request_id).await;
        }
    }

    let response = InternalServiceResponse::GetAccountInformationResponse(Accounts {
        accounts: accounts_ret,
        request_id: Some(request_id),
    });

    Some(HandledRequestResult { response, uuid })
}

async fn add_account_to_map(
    accounts_ret: &mut HashMap<u64, AccountInformation>,
    account: CNACMetadata,
    remote: &NodeRemote,
    request_id: Uuid,
) {
    let username = account.username.clone();
    let full_name = account.full_name.clone();
    let mut peers = HashMap::new();

    // Get all the peers for this CID
    let peer_cids = remote
        .account_manager()
        .get_hyperlan_peer_list(account.cid)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let peers_info = remote
        .account_manager()
        .get_persistence_handler()
        .get_hyperlan_peers(account.cid, &peer_cids)
        .await
        .unwrap_or_default();

    for peer in peers_info {
        peers.insert(
            peer.cid,
            PeerSessionInformation {
                cid: account.cid,
                peer_cid: peer.cid,
                peer_username: peer.username.unwrap_or_default(),
            },
        );
    }

    accounts_ret.insert(
        account.cid,
        AccountInformation {
            username,
            full_name,
            peers,
            request_id: Some(request_id),
        },
    );
}
