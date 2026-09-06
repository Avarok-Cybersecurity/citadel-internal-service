use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    AccountInformation, Accounts, InternalServiceRequest, InternalServiceResponse,
    PeerSessionInformation,
};
use citadel_sdk::prelude::{CNACMetadata, NodeRemote, Ratchet};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
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
            add_account_to_map(&mut accounts_ret, account, remote).await;
        }
    } else {
        for account in filtered_accounts {
            add_account_to_map(&mut accounts_ret, account, remote).await;
        }
    }

    let response = InternalServiceResponse::GetAccountInformationResponse(Accounts {
        cid: 0,
        accounts: accounts_ret,
        request_id: Some(request_id),
    });

    Some(HandledRequestResult { response, uuid })
}

async fn add_account_to_map<R: Ratchet>(
    accounts_ret: &mut HashMap<u64, AccountInformation>,
    account: CNACMetadata,
    remote: &NodeRemote<R>,
) {
    let username = account.username.clone();
    let full_name = account.full_name.clone();
    let mut peers = HashMap::new();

    // Get all the peers for this CID
    // best-effort: this response is informational and has no consumer that acts
    // destructively on an empty peer list -- nothing removes or forgets a peer
    // because this came back short. An unreadable list is reported as no peers
    // and logged, which is the honest degradation here; the alternative is
    // failing an account-information request over a cache miss.
    let peer_cids = match remote
        .account_manager()
        .get_hyperlan_peer_list(account.cid)
        .await
    {
        Ok(Some(list)) => list,
        Ok(None) => Default::default(),
        Err(err) => {
            // best-effort: this response is informational and no consumer acts
            // destructively on a short peer list -- nothing removes or forgets
            // a peer because this came back empty. Failing an
            // account-information request over a cache miss would be worse.
            citadel_sdk::logging::warn!(
                target: "citadel",
                "[GetAccountInformation] Could not read the peer list for {}: {:?}; reporting none",
                account.cid, err
            );
            Default::default()
        }
    };
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
        },
    );
}
