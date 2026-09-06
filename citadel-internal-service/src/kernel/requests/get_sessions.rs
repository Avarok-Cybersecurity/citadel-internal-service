use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GetSessionsResponse, InternalServiceRequest, InternalServiceResponse, PeerSessionInformation,
    SessionInformation,
};
use citadel_sdk::logging::{debug, info, tracing};
use citadel_sdk::prelude::{Ratchet, TargetLockedRemote};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GetSessions { request_id } = request else {
        unreachable!("Should never happen if programmed properly")
    };

    // Log current state at request start.
    //
    // Behind `log_enabled!`: `session_usernames` CLONES every username, and
    // both vectors are built under a read lock on the connection map for a line
    // the default filter drops. The leader tab polls GetSessions every 5
    // seconds for as long as a browser is open, so this ran ~17,000 times a day
    // per browser, allocating a String per session each time, to produce
    // nothing.
    if tracing::enabled!(target: "citadel", tracing::Level::INFO) {
        let lock = this.server_connection_map.read();
        let session_cids: Vec<u64> = lock.keys().copied().collect();
        let session_usernames: Vec<String> = lock.values().map(|c| c.username.clone()).collect();
        info!(target: "citadel", "GetSessions: Request received. server_connection_map has {} sessions. CIDs: {:?}, Usernames: {:?}",
            lock.len(), session_cids, session_usernames);
    }

    // The SDK is NOT queried here, and the reconciliation this handler was
    // named for does not exist.
    //
    // There was a query, and a filter computing "stale C2S sessions" to remove
    // from it -- with every branch of that filter returning false, the last one
    // under a comment reading "Actually, let's preserve all sessions and let
    // explicit disconnect handle cleanup". The decision was taken and the
    // machinery left standing: a round trip whose result was discarded, a lock,
    // a list that was always empty, and a loop over it.
    //
    // Worse than no code, because CLAUDE.md documents this as one of three
    // places a session can be removed and it is not one. Sessions are removed
    // on Disconnect and on Deregister. This handler reports what the connection
    // map holds.

    Some(build_response_from_internal_state(this, uuid, request_id))
}

/// Helper function to build GetSessionsResponse from current internal state
fn build_response_from_internal_state<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request_id: Uuid,
) -> HandledRequestResult {
    let lock = this.server_connection_map.read();
    let username_cache = this.peer_username_cache.read();
    let mut sessions = Vec::new();

    info!(target: "citadel", "GetSessions: Found {} total sessions in server_connection_map", lock.len());

    for (cid, connection) in lock.iter() {
        let conn_id = connection
            .associated_localhost_connection
            .load(Ordering::Relaxed);
        // debug!, not info!: this is one line per session per poll, and the
        // messenger polls at 1Hz, so it is O(sessions^2) lines/sec on a fully
        // idle system -- 17% of the whole internal-service log.
        debug!(target: "citadel", "GetSessions: Session {} for user {} associated with connection {}", cid, connection.username, conn_id);

        let mut session = SessionInformation {
            cid: *cid,
            username: connection.username.clone(),
            server_address: connection.server_address.clone(),
            peer_connections: HashMap::new(),
        };

        for (peer_cid, conn) in connection.peers.iter() {
            // Try remote username first, then fall back to cached username
            let peer_username = conn
                .remote
                .as_ref()
                .and_then(|r| r.target_username())
                .map(ToString::to_string)
                .or_else(|| username_cache.get(&(*cid, *peer_cid)).cloned())
                .unwrap_or_default();

            session.peer_connections.insert(
                *peer_cid,
                PeerSessionInformation {
                    cid: *cid,
                    peer_cid: *peer_cid,
                    peer_username,
                },
            );
        }
        sessions.push(session);
    }

    let response = InternalServiceResponse::GetSessionsResponse(GetSessionsResponse {
        cid: 0,
        sessions,
        request_id: Some(request_id),
    });

    HandledRequestResult { response, uuid }
}
