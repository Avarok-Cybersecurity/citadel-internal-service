use crate::kernel::requests::connection_management_auth::{
    may_disconnect, may_release, Authorization, SessionOwner,
};
use crate::kernel::requests::connection_management_claim;
use crate::kernel::requests::peer::disconnect::{disconnect_removed, DisconnectedConnection};
use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::*;
use citadel_sdk::logging::{info, warn};
use citadel_sdk::prelude::*;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// What the connection map says about `session_cid`, or `None` if there is no
/// such session.
///
/// "Orphaned" is decided the same way every other site in this file decides it:
/// the owning uuid is no longer in `tx_to_localhost_clients`. Read here, once,
/// so the authorization decision and the `only_if_orphaned` check cannot drift
/// apart into disagreeing about the same session.
pub(super) fn owner_of<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    session_cid: u64,
) -> Option<SessionOwner> {
    let owner_uuid = {
        let map = this.server_connection_map.read();
        map.get(&session_cid)
            .map(|conn| conn.associated_localhost_connection.load(Ordering::Relaxed))?
    };
    let live = this
        .tx_to_localhost_clients
        .read()
        .contains_key(&owner_uuid);
    Some(if live {
        SessionOwner::Live(owner_uuid)
    } else {
        SessionOwner::Orphaned
    })
}

/// Turn a refusal into the response the caller gets.
pub(super) fn refusal(
    session_cid: u64,
    request_id: Uuid,
    conn_id: Uuid,
    error: String,
) -> HandledRequestResult {
    warn!(target: "citadel", "ConnectionManagement REFUSED for session {session_cid} from connection {conn_id}: {error}");
    HandledRequestResult {
        response: InternalServiceResponse::ConnectionManagementFailure(
            ConnectionManagementFailure {
                cid: session_cid,
                request_id: Some(request_id),
                error,
            },
        ),
        uuid: conn_id,
    }
}

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    conn_id: Uuid,
    command: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    if let InternalServiceRequest::ConnectionManagement {
        request_id,
        management_command,
    } = command
    {
        info!(target: "citadel", "Handling connection management command: {:?}", management_command);

        let response = match management_command {
            ConfigCommand::SetConnectionOrphan {
                allow_orphan_sessions,
            } => {
                // Set orphan mode for this connection
                this.orphan_sessions
                    .write()
                    .insert(conn_id, allow_orphan_sessions);

                let message = if allow_orphan_sessions {
                    "Orphan mode enabled for connection"
                } else {
                    "Orphan mode disabled for connection"
                };

                InternalServiceResponse::ConnectionManagementSuccess(ConnectionManagementSuccess {
                    cid: 0, // Connection management is not associated with a specific session
                    request_id: Some(request_id),
                    message: message.to_string(),
                })
            }

            ConfigCommand::ClaimSession {
                session_cid,
                only_if_orphaned,
            } => {
                return connection_management_claim::claim_session(
                    this,
                    conn_id,
                    request_id,
                    session_cid,
                    only_if_orphaned,
                )
                .await
            }

            ConfigCommand::DisconnectOrphan { session_cid } => {
                return disconnect_orphan(this, conn_id, request_id, session_cid).await
            }

            ConfigCommand::ReleaseSession { session_cid } => {
                // Mark the session as "released" - simulate orphan by setting associated_tcp_connection
                // to a UUID that's not in tcp_connection_map, making it appear orphaned.
                // The session stays in server_connection_map and becomes immediately claimable.

                // Releasing means "this tab is done with it". Releasing a
                // session another connection is actively using marked it
                // reclaimable out from under its owner.
                if let Some(owner) = owner_of(this, session_cid) {
                    if let Authorization::Refuse(error) = may_release(owner, conn_id, session_cid) {
                        return Some(refusal(session_cid, request_id, conn_id, error));
                    }
                }

                let server_connection_map = this.server_connection_map.read();
                if let Some(connection) = server_connection_map.get(&session_cid) {
                    // Use nil UUID to mark as orphaned - this UUID won't exist in tcp_connection_map
                    let orphan_marker = Uuid::nil();
                    connection
                        .associated_localhost_connection
                        .store(orphan_marker, Ordering::Relaxed);

                    info!(target: "citadel", "ReleaseSession: Session {} marked as orphaned (released by tab)", session_cid);

                    InternalServiceResponse::ConnectionManagementSuccess(
                        ConnectionManagementSuccess {
                            cid: session_cid,
                            request_id: Some(request_id),
                            message: format!(
                                "Session {} released and marked as orphaned",
                                session_cid
                            ),
                        },
                    )
                } else {
                    InternalServiceResponse::ConnectionManagementFailure(
                        ConnectionManagementFailure {
                            cid: session_cid,
                            request_id: Some(request_id),
                            error: format!("Session {} not found", session_cid),
                        },
                    )
                }
            }
        };

        Some(HandledRequestResult {
            response,
            uuid: conn_id,
        })
    } else {
        warn!(target: "citadel", "Connection management handler received wrong command type");
        None
    }
}

/// Disconnect one orphan session, or every orphan session.
///
/// Removing the map entry is not a disconnect. Nothing in `Connection` tears the
/// SDK session down when it drops — the only `Drop` impls in the protocol are on
/// the receive halves, and the C2S receive half is not in `Connection` at all; it
/// lives in the task spawned by the connect handler and keeps running. So this
/// used to remove the entry, answer "Disconnected orphan session X", and leave a
/// `SessionState::Connected` session behind with its keepalives going.
///
/// The account was then wedged until the process restarted: the next `Connect`
/// finds no map entry, calls `remote.connect()`, and the protocol refuses it with
/// `SessionManagerSessionAlreadyExists`; `ClaimSession` and `Disconnect` both
/// answer "not found" because the entry is gone. No wire command could reach the
/// session that was still there.
///
/// This was written as "`peer/disconnect.rs` has always done this correctly — it
/// awaits `disconnect_removed` for the same removal — so this is that fix,
/// propagated." It awaited it and then DISCARDED the result, logging
/// "Proceeding anyway" and returning the success notification either way. The
/// fix was propagated from a file that never had it, which is why nothing there
/// looked wrong for as long as it did. Both now report.
///
/// The map lock is taken and released BEFORE the SDK work: `disconnect_removed`
/// awaits, and holding a `parking_lot` write guard across an await would block
/// every other session's handler on this one.
async fn disconnect_orphan<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    conn_id: Uuid,
    request_id: Uuid,
    session_cid: Option<u64>,
) -> Option<HandledRequestResult> {
    let mut removed: Vec<DisconnectedConnection<R>> = Vec::new();
    let bulk = session_cid.is_none();

    {
        let mut server_connection_map = this.server_connection_map.write();

        if let Some(session_cid) = session_cid {
            let owner = {
                let tcp_connection_map = this.tx_to_localhost_clients.read();
                server_connection_map.get(&session_cid).map(|connection| {
                    let uuid = connection
                        .associated_localhost_connection
                        .load(Ordering::Relaxed);
                    if tcp_connection_map.contains_key(&uuid) {
                        SessionOwner::Live(uuid)
                    } else {
                        SessionOwner::Orphaned
                    }
                })
            };
            if let Some(owner) = owner {
                if let Authorization::Refuse(error) = may_disconnect(owner, conn_id, session_cid) {
                    drop(server_connection_map);
                    return Some(refusal(session_cid, request_id, conn_id, error));
                }
            }

            match server_connection_map.remove(&session_cid) {
                Some(connection) => {
                    // Beside the removal, not in a later loop: the CID-keyed
                    // kernel maps outlive the entry otherwise, and this was the
                    // one C2S teardown path that never pruned them. Lock order
                    // is connection map then CID-scoped maps, which is the order
                    // every other site takes and the only order any site takes.
                    this.prune_cid_scoped_state(session_cid, None);
                    let tcp_uuid = connection
                        .associated_localhost_connection
                        .load(Ordering::Relaxed);
                    removed.push(DisconnectedConnection::C2S {
                        connection,
                        cid: session_cid,
                        tcp_uuid,
                    });
                }
                None => {
                    drop(server_connection_map);
                    return Some(HandledRequestResult {
                        response: InternalServiceResponse::ConnectionManagementFailure(
                            ConnectionManagementFailure {
                                cid: session_cid,
                                request_id: Some(request_id),
                                error: format!("Orphan session {session_cid} not found"),
                            },
                        ),
                        uuid: conn_id,
                    });
                }
            }
        } else {
            let orphaned_sessions: Vec<u64> = {
                let tcp_connection_map = this.tx_to_localhost_clients.read();
                server_connection_map
                    .iter()
                    .filter(|(_, connection)| {
                        let conn_id = connection
                            .associated_localhost_connection
                            .load(Ordering::Relaxed);
                        !tcp_connection_map.contains_key(&conn_id)
                    })
                    .map(|(cid, _)| *cid)
                    .collect()
            };

            for cid in orphaned_sessions {
                if let Some(connection) = server_connection_map.remove(&cid) {
                    // Beside the removal — see the single-session branch above.
                    this.prune_cid_scoped_state(cid, None);
                    let tcp_uuid = connection
                        .associated_localhost_connection
                        .load(Ordering::Relaxed);
                    removed.push(DisconnectedConnection::C2S {
                        connection,
                        cid,
                        tcp_uuid,
                    });
                }
            }
        }
    }

    // Now the SDK, with no lock held.
    //
    // A failure here is reported, not swallowed: the entry is already gone from
    // the map, so a caller told "disconnected" when the protocol session
    // survived would be in exactly the wedged state this function exists to
    // prevent, with no way left to address it.
    let mut failures: Vec<String> = Vec::new();
    for disconnected in &removed {
        if let Err(err) = disconnect_removed(this.remote(), disconnected).await {
            let cid = match disconnected {
                DisconnectedConnection::C2S { cid, .. } => *cid,
                DisconnectedConnection::P2P { cid, .. } => *cid,
            };
            warn!(target: "citadel", "[DisconnectOrphan] SDK disconnect failed for session {cid}: {err:?}");
            failures.push(format!("{cid}: {err}"));
        }
    }

    let count = removed.len();
    let reported_cid = if bulk { 0 } else { session_cid.unwrap_or(0) };

    let response = if failures.is_empty() {
        InternalServiceResponse::ConnectionManagementSuccess(ConnectionManagementSuccess {
            cid: reported_cid,
            request_id: Some(request_id),
            message: if bulk {
                format!("Disconnected {count} orphan sessions")
            } else {
                format!("Disconnected orphan session {reported_cid}")
            },
        })
    } else {
        InternalServiceResponse::ConnectionManagementFailure(ConnectionManagementFailure {
            cid: reported_cid,
            request_id: Some(request_id),
            error: format!(
                "Removed {count} orphan session(s), but the protocol disconnect failed for {}: {}",
                failures.len(),
                failures.join("; ")
            ),
        })
    };

    Some(HandledRequestResult {
        response,
        uuid: conn_id,
    })
}
