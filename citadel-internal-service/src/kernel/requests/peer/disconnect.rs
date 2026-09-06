use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{CitadelWorkspaceService, Connection, PeerConnection};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    DisconnectNotification, InternalServiceRequest, InternalServiceResponse, PeerDisconnectFailure,
};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::{
    NetworkError, NodeRemote, ProtocolRemoteExt, ProtocolRemoteTargetExt, Ratchet,
    VirtualTargetType,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use uuid::Uuid;

/// Timeout for SDK disconnect operations.
/// User-initiated disconnects should not hang indefinitely - if the SDK doesn't
/// respond within this time, we proceed with internal state cleanup anyway.
const SDK_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Represents a connection removed from internal state, pending SDK disconnect.
///
/// CRITICAL: This enum holds the connection struct ALIVE until SDK disconnect completes.
/// This prevents the RAII Drop impl from firing a redundant disconnect signal while
/// the SDK disconnect is still in progress.
///
/// Flow:
/// 1. `cleanup_state()` removes from map, returns this enum (struct stays alive)
/// 2. `disconnect_removed()` calls SDK disconnect on the target-locked remote
/// 3. Enum drops AFTER disconnect completes (RAII Drop is now harmless)
pub enum DisconnectedConnection<R: Ratchet> {
    /// C2S session - the Connection contains the target-locked ClientServerRemote
    C2S {
        connection: Connection<R>,
        cid: u64,
        tcp_uuid: Uuid,
    },
    /// P2P peer connection
    P2P {
        #[allow(dead_code)] // Kept alive for RAII - prevents Drop during SDK disconnect
        peer_connection: PeerConnection<R>,
        cid: u64,
        peer_cid: u64,
        tcp_uuid: Uuid,
    },
}

/// Disconnects a peer or C2S connection at the SDK/protocol layer.
///
/// On success, guarantees the peer/session has been disconnected from the network AND cleared from the SDK.
/// This function does NOT clear the session from the InternalService's server_connection_map.
/// The caller should call `cleanup_state` after this succeeds.
///
/// # Arguments
/// * `remote` - NodeRemote for SDK operations
/// * `request_id` - UUID for tracking the request
/// * `cid` - Session CID
/// * `peer_cid` - Some(peer_cid) for P2P disconnect, None for C2S disconnect
#[allow(dead_code)] // Kept for potential future use - currently using cleanup_state + disconnect_removed pattern
pub async fn disconnect_any<R: Ratchet>(
    remote: &NodeRemote<R>,
    request_id: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
) -> Result<InternalServiceResponse, NetworkError> {
    if let Some(target_cid) = peer_cid {
        remote
            .find_target(cid, target_cid)
            .await?
            .disconnect()
            .await?;
    } else {
        // Construct a c2s remote purely for the purpose of disconnecting from the central server
        ClientServerRemote::new(
            VirtualTargetType::LocalGroupServer { session_cid: cid },
            remote.clone(),
            Default::default(),
            None,
            None,
        )
        .disconnect()
        .await?;
    }

    // Perform a sanity check: verify session/peer is no longer in SDK
    let still_in_protocol = remote.sessions().await.map(|sessions| {
        let conn = sessions.sessions.iter().find(|sess| sess.cid == cid);
        if let Some(conn) = conn {
            if let Some(peer_cid) = peer_cid {
                conn.connections
                    .iter()
                    .any(|c| c.peer_cid.unwrap_or(0) == peer_cid)
            } else {
                // C2S still exists
                true
            }
        } else {
            false
        }
    })?;

    if still_in_protocol {
        return Err(NetworkError::msg(format!(
            "Client or peer is still in protocol: cid={cid}, peer_cid={peer_cid:?}"
        )));
    }

    Ok(InternalServiceResponse::DisconnectNotification(
        DisconnectNotification {
            cid,
            peer_cid,
            request_id: Some(request_id),
        },
    ))
}

/// Removes a session or peer from internal state and returns it for SDK disconnect.
///
/// CRITICAL: Returns the removed struct wrapped in `DisconnectedConnection` so it stays
/// alive until SDK disconnect completes. This prevents RAII Drop from triggering a
/// redundant disconnect signal.
///
/// # Arguments
/// * `server_connection_map` - The internal service's session map
/// * `cid` - Session CID
/// * `peer_cid` - Some(peer_cid) for P2P cleanup, None for C2S cleanup
///
/// # Returns
/// The removed connection wrapped in enum, or None if already removed.
pub fn cleanup_state<R: Ratchet>(
    server_connection_map: &Arc<RwLock<HashMap<u64, Connection<R>>>>,
    cid: u64,
    peer_cid: Option<u64>,
) -> Option<DisconnectedConnection<R>> {
    if let Some(target_cid) = peer_cid {
        // P2P cleanup - remove peer from session
        let mut lock = server_connection_map.write();
        if let Some(sess) = lock.get_mut(&cid) {
            if let Some(peer_conn) = sess.peers.remove(&target_cid) {
                let tcp_uuid = peer_conn
                    .associated_localhost_connection
                    .load(Ordering::Relaxed);
                citadel_sdk::logging::info!(
                    "[cleanup_state] Removed peer {target_cid} from session {cid}"
                );
                return Some(DisconnectedConnection::P2P {
                    peer_connection: peer_conn,
                    cid,
                    peer_cid: target_cid,
                    tcp_uuid,
                });
            }
        }
        citadel_sdk::logging::warn!(
            "[cleanup_state] Peer {target_cid} already removed from session {cid}"
        );
        None
    } else {
        // C2S cleanup - remove entire session
        let mut lock = server_connection_map.write();
        let count_before = lock.len();
        let session_keys: Vec<u64> = lock.keys().copied().collect();
        citadel_sdk::logging::info!(
            "[cleanup_state] BEFORE removal: {} sessions in map, CIDs: {:?}",
            count_before,
            session_keys
        );
        if let Some(conn) = lock.remove(&cid) {
            let tcp_uuid = conn.associated_localhost_connection.load(Ordering::Relaxed);
            let count_after = lock.len();
            let remaining_keys: Vec<u64> = lock.keys().copied().collect();
            citadel_sdk::logging::info!(
                "[cleanup_state] Removed C2S session {cid}. AFTER removal: {} sessions, remaining CIDs: {:?}",
                count_after,
                remaining_keys
            );
            return Some(DisconnectedConnection::C2S {
                connection: conn,
                cid,
                tcp_uuid,
            });
        }
        citadel_sdk::logging::warn!("[cleanup_state] Session {cid} already removed (not in map)");
        None
    }
}

/// Disconnects a removed connection at the SDK layer.
///
/// CRITICAL: The `disconnected` enum holds the struct alive until this function completes,
/// preventing RAII Drop from triggering a redundant disconnect signal.
///
/// After this function returns (success or failure), the enum drops and RAII cleanup
/// is harmless since SDK disconnect has already completed or failed.
pub async fn disconnect_removed<R: Ratchet>(
    remote: &NodeRemote<R>,
    disconnected: &DisconnectedConnection<R>,
) -> Result<(), NetworkError> {
    match disconnected {
        DisconnectedConnection::C2S {
            connection, cid, ..
        } => {
            citadel_sdk::logging::info!(
                "[disconnect_removed] Calling SDK disconnect on C2S session {} via target-locked ClientServerRemote",
                cid
            );
            connection.client_server_remote.disconnect().await?;

            // Sanity check: verify session is no longer in SDK
            let still_in_protocol = remote
                .sessions()
                .await
                .map(|sessions| sessions.sessions.iter().any(|sess| sess.cid == *cid))?;

            if still_in_protocol {
                return Err(NetworkError::msg(format!(
                    "C2S session {} still in protocol after disconnect",
                    cid
                )));
            }
            Ok(())
        }
        DisconnectedConnection::P2P { cid, peer_cid, .. } => {
            citadel_sdk::logging::info!(
                "[disconnect_removed] Calling SDK disconnect on P2P peer {} from session {}",
                peer_cid,
                cid
            );
            remote
                .find_target(*cid, *peer_cid)
                .await?
                .disconnect()
                .await?;

            // Sanity check: verify peer is no longer in SDK
            let still_in_protocol = remote.sessions().await.map(|sessions| {
                if let Some(sess) = sessions.sessions.iter().find(|s| s.cid == *cid) {
                    sess.connections
                        .iter()
                        .any(|c| c.peer_cid == Some(*peer_cid))
                } else {
                    false
                }
            })?;

            if still_in_protocol {
                return Err(NetworkError::msg(format!(
                    "P2P peer {} still in protocol after disconnect",
                    peer_cid
                )));
            }
            Ok(())
        }
    }
}

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let (request_id, cid, peer_cid) = match request {
        InternalServiceRequest::PeerDisconnect {
            request_id,
            cid,
            peer_cid,
        } => (request_id, cid, Some(peer_cid)),
        InternalServiceRequest::Disconnect { request_id, cid } => (request_id, cid, None),
        _ => unreachable!("Should never happen if programmed properly"),
    };

    // Check if connection exists in internal service state
    let (session_exists, peer_session_exists) = {
        let conns = this.server_connection_map.read();
        let session_exists = conns.contains_key(&cid);
        let peer_session_exists = session_exists
            && peer_cid
                .map(|p| {
                    conns
                        .get(&cid)
                        .map(|conn| conn.peers.contains_key(&p))
                        .unwrap_or(false)
                })
                .unwrap_or(false);
        (session_exists, peer_session_exists)
    };

    if !session_exists {
        return Some(HandledRequestResult {
            response: InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
                cid,
                message: "disconnect: Server connection not found".to_string(),
                request_id: Some(request_id),
            }),
            uuid,
        });
    }

    if peer_cid.is_some() && !peer_session_exists {
        return Some(HandledRequestResult {
            response: InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
                cid,
                message: "disconnect: Peer connection not found".to_string(),
                request_id: Some(request_id),
            }),
            uuid,
        });
    }

    // Clear orphan mode BEFORE any cleanup
    // This prevents a race condition where the WebSocket drops during/after disconnect,
    // causing the orphan handler to preserve the session that we're trying to remove.
    if peer_cid.is_none() {
        // Only for C2S disconnects (full session disconnect)
        let conn_id = {
            let conns = this.server_connection_map.read();
            conns
                .get(&cid)
                .map(|conn| conn.associated_localhost_connection.load(Ordering::Relaxed))
        };
        if let Some(conn_id) = conn_id {
            citadel_sdk::logging::info!(
                "[Disconnect] Clearing orphan mode for connection {:?} before disconnecting CID {}",
                conn_id,
                cid
            );
            this.orphan_sessions.write().remove(&conn_id);
        }
    }

    // STEP 1: Remove from internal state FIRST, get back the struct
    // The struct stays alive in the enum, preventing RAII Drop from firing during SDK disconnect
    this.prune_cid_scoped_state(cid, peer_cid);
    let Some(disconnected) = cleanup_state(&this.server_connection_map, cid, peer_cid) else {
        // Already removed - shouldn't happen due to earlier checks, but handle gracefully
        citadel_sdk::logging::warn!(
            "[Disconnect] Session/peer already removed during disconnect for CID {} peer {:?}",
            cid,
            peer_cid
        );
        return Some(HandledRequestResult {
            response: InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                cid,
                peer_cid,
                request_id: Some(request_id),
            }),
            uuid,
        });
    };

    // STEP 2: Call SDK disconnect while holding the struct alive
    // This uses the target-locked ClientServerRemote (for C2S) or find_target (for P2P)
    let sdk_result = timeout(
        SDK_DISCONNECT_TIMEOUT,
        disconnect_removed(this.remote(), &disconnected),
    )
    .await;

    // Reported, not swallowed.
    //
    // Both failure arms used to log "Proceeding anyway" and fall through to the
    // success notification below. The map entry is already gone by then, so a
    // caller told "disconnected" while the protocol session survived is wedged:
    // `connect` finds no entry, calls `remote.connect()`, and the protocol
    // refuses with `SessionManagerSessionAlreadyExists`; `ClaimSession` and
    // `DisconnectOrphan` both answer "not found" because the entry is gone. No
    // wire command can reach the session that is still there, until the agent
    // restarts.
    //
    // `connection_management.rs` handles this correctly and its comment says
    // "`peer/disconnect.rs` has always done this correctly -- so this is that
    // fix, propagated". It was not: this function awaited `disconnect_removed`
    // and then discarded the result. The fix was propagated FROM a file that
    // never had it, which is why nothing here looked wrong.
    let outcome: crate::kernel::requests::peer::disconnect_outcome::SdkDisconnect = match sdk_result
    {
        Ok(Ok(())) => {
            citadel_sdk::logging::info!(
                "[Disconnect] SDK disconnect succeeded for CID {} peer {:?}",
                cid,
                peer_cid
            );
            crate::kernel::requests::peer::disconnect_outcome::SdkDisconnect::Succeeded
        }
        Ok(Err(err)) => {
            citadel_sdk::logging::warn!(
                "[Disconnect] SDK disconnect failed for CID {} peer {:?}: {:?}",
                cid,
                peer_cid,
                err
            );
            crate::kernel::requests::peer::disconnect_outcome::SdkDisconnect::Failed(format!(
                "{err:?}"
            ))
        }
        Err(_elapsed) => {
            citadel_sdk::logging::warn!(
                "[Disconnect] SDK disconnect timed out after {:?} for CID {} peer {:?}",
                SDK_DISCONNECT_TIMEOUT,
                cid,
                peer_cid
            );
            crate::kernel::requests::peer::disconnect_outcome::SdkDisconnect::TimedOut
        }
    };

    // STEP 3: Get the TCP UUID from the enum before dropping
    let tcp_uuid = match &disconnected {
        DisconnectedConnection::C2S { tcp_uuid, .. } => *tcp_uuid,
        DisconnectedConnection::P2P { tcp_uuid, .. } => *tcp_uuid,
    };

    // STEP 4: Enum drops here (end of scope) - RAII cleanup is now safe since SDK disconnect completed
    drop(disconnected);

    citadel_sdk::logging::info!(
        "[Disconnect] Completed for CID {} peer {:?}, notifying TCP client {:?}",
        cid,
        peer_cid,
        tcp_uuid
    );

    // Success only when the protocol session actually went away.
    Some(HandledRequestResult {
        response: outcome.into_response(cid, peer_cid, request_id, SDK_DISCONNECT_TIMEOUT),
        uuid,
    })
}
