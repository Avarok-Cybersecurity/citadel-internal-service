pub mod accept;
pub mod connect;
pub mod disconnect;
pub mod disconnect_outcome;
pub mod list_all;
pub mod list_registered;
pub mod register;
pub mod respond_register;

// Re-export for use by response handlers
pub use disconnect::{cleanup_state, DisconnectedConnection};

use citadel_internal_service_types::PeerInformation;
use citadel_sdk::prelude::{
    NetworkError, NodeRemote, NodeRequest, NodeResult, PeerCommand, PeerInfo, PeerResponse,
    PeerSignal, Ratchet,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Duration;

/// Generous backstop on a peer-list callback subscription. The server replies
/// to `GetMutuals` / `GetRegisteredPeers` (with `Some` or `None`) within
/// milliseconds in normal operation, so this only fires if the subscription
/// genuinely wedges (a protocol stall that never yields the terminal event).
/// It is far longer than the old 5-second wrapper — that one fired on every
/// `response: None` (the bug this refactor fixed); this one only bounds a true
/// hang so discovery can't block forever.
const PEER_LIST_TIMEOUT: Duration = Duration::from_secs(30);

/// A peer returned by a server peer-list query, paired with its online flag.
pub(crate) type PeerWithOnline = (PeerInfo, bool);

/// Convert a server `GetMutuals` / `GetRegisteredPeers` reply into a flat peer
/// list.
///
/// Only two replies are authoritative for these commands:
/// * `None` — the server's normal answer when the account has **zero**
///   registered/mutual peers (a brand-new account, or one whose registration
///   hasn't been mutually accepted yet) → `Ok([])`.
/// * `Some(RegisteredCids(..))` — the populated list → `Ok(peers)`.
///
/// Any other `Some(..)` variant is a protocol/authorization anomaly (e.g. a
/// denial), NOT "zero peers": return `Err` so the handler surfaces a failure
/// rather than masking it as an empty list.
pub(crate) fn peers_from_response(
    response: Option<PeerResponse>,
) -> Result<Vec<PeerWithOnline>, NetworkError> {
    match response {
        None => Ok(Vec::new()),
        Some(PeerResponse::RegisteredCids(peer_info, is_onlines)) => Ok(peer_info
            .into_iter()
            .zip(is_onlines)
            .filter_map(|(info, online)| info.map(|info| (info, online)))
            .collect()),
        Some(_) => Err(NetworkError::msg(
            "unexpected peer-list response variant (expected RegisteredCids or None)",
        )),
    }
}

/// Send a peer-list `PeerCommand` to the server and drive its callback
/// subscription to the single terminal reply, returning the peers `matcher`
/// extracts.
///
/// Why this exists instead of the SDK's `get_local_group_mutual_peers` /
/// `get_local_group_peers`: those helpers loop on the callback stream and only
/// match `response: Some(PeerResponse::RegisteredCids(..))`. When the account
/// has no registered/mutual peers the server legitimately replies
/// `response: None` (`get_hyperlan_peer_list` / `get_registered_impersonal_cids`
/// return `None` for an empty set), which those loops ignore — so they await the
/// stream forever and only unblock via the caller's timeout. That hang is why
/// both list handlers previously wrapped the SDK call in a 5-second `timeout`
/// and returned an empty list, adding a 5s stall to every discovery poll for any
/// user without established peers (i.e. every brand-new account). Here the
/// `matcher` treats the terminal reply — `Some` or `None` — as authoritative and
/// returns at once.
///
/// Liveness: `PEER_LIST_TIMEOUT` bounds the WHOLE operation — subscription
/// creation AND the reply loop — so neither a stalled `send_callback_subscription`
/// nor a wedged stream can hang discovery. Only an explicit terminal reply
/// yields `Ok`: a genuine "no peers" result is the server's `response: None`
/// (matcher → `Ok([])`). A subscription that closes before replying, an
/// unexpected response variant, or a timeout all return `Err`, so a dropped link
/// / protocol anomaly surfaces as a discovery failure instead of masquerading as
/// "zero peers". The `matcher` returns `Some(Ok(..))`/`Some(Err(..))` for the
/// terminal reply and `None` for unrelated events.
pub(crate) async fn query_server_peer_list<R: Ratchet>(
    remote: &NodeRemote<R>,
    cid: u64,
    command: PeerSignal,
    matcher: impl Fn(NodeResult<R>) -> Option<Result<Vec<PeerWithOnline>, NetworkError>>,
) -> Result<Vec<PeerWithOnline>, NetworkError> {
    let request = NodeRequest::PeerCommand(PeerCommand {
        session_cid: cid,
        command,
    });

    // The subscription creation is INSIDE the timeout: a remote that stalls
    // while setting up the callback would otherwise hang here unbounded.
    let drive = async {
        let mut stream = remote.send_callback_subscription(request).await?;
        while let Some(event) = stream.next().await {
            if let Some(result) = matcher(event.into_result()?) {
                return result.map(Some);
            }
        }
        // Stream ended without a terminal reply (e.g. the C2S link dropped).
        Ok(None)
    };

    match tokio::time::timeout(PEER_LIST_TIMEOUT, drive).await {
        Ok(Ok(Some(peers))) => Ok(peers),
        Ok(Ok(None)) => Err(NetworkError::msg(
            "peer-list subscription closed before a terminal reply (connection dropped?)",
        )),
        Ok(Err(e)) => Err(e),
        Err(_elapsed) => Err(NetworkError::msg(format!(
            "peer-list query exceeded {PEER_LIST_TIMEOUT:?} with no server reply",
        ))),
    }
}

/// Build the `cid -> PeerInformation` map both list handlers return from a raw
/// peer list: drop self, and fall back to the registration-time username cache
/// only when the server supplied an empty/absent username.
pub(crate) fn build_peer_information_map(
    session_cid: u64,
    peers: Vec<PeerWithOnline>,
    username_cache: &HashMap<(u64, u64), String>,
) -> HashMap<u64, PeerInformation> {
    peers
        .into_iter()
        .filter(|(info, _)| info.cid != session_cid)
        .map(|(info, online_status)| {
            let username = Some(info.username)
                .filter(|u| !u.is_empty())
                .or_else(|| username_cache.get(&(session_cid, info.cid)).cloned());
            (
                info.cid,
                PeerInformation {
                    cid: info.cid,
                    online_status,
                    name: Some(info.full_name),
                    username,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(cid: u64, username: &str, full_name: &str) -> PeerInfo {
        PeerInfo {
            cid,
            username: username.to_string(),
            full_name: full_name.to_string(),
        }
    }

    /// The crux of the discovery-hang fix: the server's `None` reply (the normal
    /// "zero registered peers" answer for a brand-new account) must map to an
    /// empty (and successful) list. The old SDK helper ignored `None` and hung
    /// until a 5s timeout; here it is just an empty `Ok`.
    #[test]
    fn none_response_maps_to_empty_list() {
        assert!(peers_from_response(None)
            .expect("None is a success")
            .is_empty());
    }

    /// A populated reply maps peers with their online flags and drops the
    /// `None` placeholder entries the protocol can include.
    #[test]
    fn registered_cids_response_maps_peers_and_drops_none_entries() {
        let response = Some(PeerResponse::RegisteredCids(
            vec![
                Some(peer(2, "bob", "Bob B")),
                None,
                Some(peer(3, "carol", "Carol C")),
            ],
            vec![true, false, false],
        ));
        let peers = peers_from_response(response).expect("RegisteredCids is a success");
        assert_eq!(peers.len(), 2, "the None placeholder entry must be dropped");
        assert_eq!(peers[0].0.cid, 2);
        assert!(peers[0].1, "bob should be online");
        assert_eq!(peers[1].0.cid, 3);
        assert!(!peers[1].1, "carol should be offline");
    }

    /// An unexpected `Some(..)` variant (not `RegisteredCids`) is a protocol
    /// anomaly, not "zero peers" — it must surface as an error so the handler
    /// returns a failure instead of masking it as an empty list.
    #[test]
    fn unexpected_response_variant_is_an_error() {
        // `PeerResponse::Decline` is a non-RegisteredCids variant.
        assert!(peers_from_response(Some(PeerResponse::Decline)).is_err());
    }

    /// The map drops self, preserves real usernames/names, and falls back to the
    /// registration-time cache only when the server gave an empty username.
    #[test]
    fn build_map_filters_self_and_applies_cache_fallback() {
        let mut cache = HashMap::new();
        cache.insert((1u64, 4u64), "cached_dave".to_string());
        let peers = vec![
            (peer(1, "me", "Me"), true),     // self (cid == session) -> filtered
            (peer(2, "bob", "Bob B"), true), // normal username
            (peer(4, "", "Dave D"), false),  // empty username -> cache fallback
        ];
        let map = build_peer_information_map(1, peers, &cache);

        assert!(!map.contains_key(&1), "self must be filtered out");
        assert_eq!(map.len(), 2);

        let bob = map.get(&2).expect("bob present");
        assert_eq!(bob.username.as_deref(), Some("bob"));
        assert_eq!(bob.name.as_deref(), Some("Bob B"));
        assert!(bob.online_status);

        let dave = map.get(&4).expect("dave present");
        assert_eq!(
            dave.username.as_deref(),
            Some("cached_dave"),
            "empty server username should fall back to the registration cache"
        );
        assert!(!dave.online_status);
    }
}
