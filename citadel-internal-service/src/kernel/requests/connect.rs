//! C2S Connection Handler
//!
//! ## Protocol Semantics (CRITICAL)
//!
//! ### C2S (Client-to-Server)
//! - **Registration**: ONE-TIME per user. Creates permanent CID. Persisted in backend.
//! - **Connection**: Can happen MANY TIMES after registration. Reuses existing CID.
//! - **No re-registration**: The protocol has NO notion of re-registering a user.
//!
//! ### P2P (Peer-to-Peer)
//! - **Registration**: ONE-TIME per peer pair. Consent to communicate. Persisted.
//! - **Connection**: Can happen MANY TIMES after P2P registration.
//! - **No re-registration**: The protocol has NO notion of re-registering peers.
//!
//! ### Key Insight
//! If a user gets a NEW CID after reconnection, it means a NEW ACCOUNT was registered.
//! CID is PERMANENT per account - not per session.
//!
//! ### Connect vs Register
//! - `register.rs` → `remote.register()` → Creates NEW account with NEW CID
//! - `connect.rs` (this file) → `remote.connect()` → Connects to EXISTING account, SAME CID

use crate::kernel::requests::HandledRequestResult;
use crate::kernel::{create_client_server_remote, CitadelWorkspaceService, Connection};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    AtomicUuid, ConnectFailure, InternalServiceRequest, InternalServiceResponse,
    MessageNotification,
};
use citadel_sdk::prelude::{AuthenticationRequest, ProtocolRemoteExt, Ratchet};
use futures::StreamExt;
use std::sync::Arc;
use uuid::Uuid;

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Connect {
        request_id,
        username,
        password,
        connect_mode,
        udp_mode,
        keep_alive_timeout,
        session_security_settings,
        server_password,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    // The SDK takes ownership of the password when it carries it to the server,
    // so the fingerprint has to be derived from a copy taken here.
    let password_for_fingerprint = password.clone();

    // GUARD 1: Prevent duplicate concurrent connection attempts for same username
    // This fixes TOCTOU race conditions where two Connect requests arrive simultaneously
    {
        let mut connecting = this.connecting_usernames.lock();
        if connecting.contains(&username) {
            citadel_sdk::logging::warn!(target: "citadel", "[Connect] BLOCKED: Connection already in progress for user {}", username);
            let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                cid: 0,
                message: format!("Connection already in progress for user {}", username),
                request_id: Some(request_id),
            });
            return Some(HandledRequestResult { response, uuid });
        }
        connecting.insert(username.clone());
    }

    // Helper to cleanup connecting_usernames on function exit
    let cleanup_username = |this: &CitadelWorkspaceService<T, R>, username: &str| {
        this.connecting_usernames.lock().remove(username);
    };

    // GUARD 2: Session reuse check - prevent duplicate SDK sessions for same username
    // This prevents the race condition where ClaimSession + second Connect resets ratchet
    let existing_cid = {
        let lock = this.server_connection_map.read();
        lock.iter()
            .find(|(_, conn)| conn.username == username)
            .map(|(cid, _)| *cid)
    };

    if let Some(cid) = existing_cid {
        citadel_sdk::logging::info!(target: "citadel", "[Connect] Found existing session {} for user {}, checking SDK...", cid, username);

        // Query SDK to see if session is actually active
        let sdk_active = match remote.sessions().await {
            Ok(sessions) => sessions.sessions.iter().any(|sess| sess.cid == cid),
            Err(e) => {
                // A FAILED query is not an empty answer. This assumed
                // "inactive", and the branch that assumption reaches is
                // destructive: it removes the map entry, prunes CID-scoped
                // state, and then runs the SDK connect against a session the
                // SDK may still hold -- the ratchet reset the
                // SessionAlreadyActive branch exists to prevent. Refuse and
                // let the caller retry; a transient stream error must not
                // cost the user a live session.
                citadel_sdk::logging::warn!(target: "citadel", "[Connect] Failed to query SDK sessions: {:?}; refusing rather than assuming the session is gone", e);
                cleanup_username(this, &username);
                let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                    cid,
                    message: format!(
                        "Could not determine whether session {} is still active: {:?}. \
                         Nothing was changed; try again.",
                        cid, e
                    ),
                    request_id: Some(request_id),
                });
                return Some(HandledRequestResult { response, uuid });
            }
        };

        if sdk_active {
            // Prove the caller knows the password before handing them the
            // session. This branch never reaches the SDK, so nothing else in it
            // ever looks at the password: it used to re-point the session's
            // message stream to the caller and return the real CID on the
            // strength of a username alone. Any client of this agent's socket
            // could name a live username and take over its stream.
            //
            // See kernel/credential_fingerprint.rs for why this is a recorded
            // fingerprint and not a local credential check -- the short version
            // is that authentication belongs to the server, re-connecting here
            // would reset the ratchet this branch exists to protect, and the
            // SDK's client-side `validate_credentials` rejects every password.
            let presented = crate::kernel::credential_fingerprint::derive(
                remote,
                &username,
                password_for_fingerprint,
            )
            .await;
            let authorized = {
                let lock = this.server_connection_map.read();
                lock.get(&cid).is_some_and(|conn| {
                    crate::kernel::credential_fingerprint::matches(
                        conn.credential_fingerprint.as_ref(),
                        presented.as_ref(),
                    )
                })
            };

            if !authorized {
                citadel_sdk::logging::warn!(target: "citadel", "[Connect] REFUSED reuse of session {} for user {}: the password does not match the one that opened it", cid, username);
                cleanup_username(this, &username);
                // Deliberately the same message a wrong password on a fresh
                // account gets, and no CID: telling the caller that a session
                // exists for this username would make the handler an oracle for
                // who is signed in on this agent.
                let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                    cid: 0,
                    message: "Invalid username or password".to_string(),
                    request_id: Some(request_id),
                });
                return Some(HandledRequestResult { response, uuid });
            }

            // Session is active in both internal state and SDK - inform frontend
            citadel_sdk::logging::info!(target: "citadel", "[Connect] Session {} already active for user {} - returning SessionAlreadyActive", cid, username);

            // Update TCP mapping to new connection
            {
                let lock = this.server_connection_map.read();
                if let Some(conn) = lock.get(&cid) {
                    conn.associated_localhost_connection
                        .store(uuid, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Return SessionAlreadyActive to let frontend know the session was already connected
            // This allows the frontend to gracefully handle the case (e.g., redirect to workspace)
            let response = InternalServiceResponse::SessionAlreadyActive(
                citadel_internal_service_types::SessionAlreadyActive {
                    cid,
                    username: username.clone(),
                    message: "Session already active. Use the navbar to switch sessions or proceed to workspace.".to_string(),
                    request_id: Some(request_id),
                },
            );

            cleanup_username(this, &username);
            return Some(HandledRequestResult { response, uuid });
        } else {
            // Internal has session but SDK doesn't - clean up stale state
            citadel_sdk::logging::info!(target: "citadel", "[Connect] Clearing stale session {} for user {} (SDK session disconnected)", cid, username);
            this.server_connection_map.write().remove(&cid);
            this.prune_cid_scoped_state(cid, None);
            // Allow SDK protocol layer to stabilize after stale session cleanup
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    // Save username for cleanup (will be moved into SDK connect)
    let username_for_cleanup = username.clone();

    // Proceed with new connection (no existing session or stale session was cleaned)
    match remote
        .connect(
            AuthenticationRequest::credentialed(username, password),
            connect_mode,
            udp_mode,
            keep_alive_timeout,
            session_security_settings,
            server_password,
        )
        .await
    {
        Ok(conn_success) => {
            let cid = conn_success.cid;
            citadel_sdk::logging::info!(target: "citadel", "[Connect] SUCCESS: cid={}", cid);

            // A `GetActiveSessions` subscription stood here, between the
            // successful SDK connect and building the `Connection`, so
            // `ConnectSuccess` waited on it. It was labelled DEBUG and its only
            // consumer was the `info!` that printed the result.
            //
            // Its `.next().await` was UNBOUNDED. Every other SDK query in this
            // tree carries a limit -- PEER_LIST_TIMEOUT, PEER_SEND_TIMEOUT, the
            // 30s connect_to_peer_custom -- and a subscription that never
            // yields would have left login permanently unanswered, with the last
            // log line reading "Querying active sessions after connect...",
            // which reads as an SDK connect failure rather than as a discarded
            // debug query.
            //
            // The liveness check that is actually used elsewhere is
            // `remote.sessions()`, which does not go through a subscription.

            let (sink, mut stream) = conn_success.split();
            let client_server_remote = create_client_server_remote(
                stream.vconn_type,
                remote.clone(),
                session_security_settings,
            );

            // Refuse, for the same reason the server-address read below refuses.
            //
            // This was `.ok().flatten().unwrap_or_else(|| "#INVALID_USERNAME")`,
            // and the session is RECORDED under whatever this produces. GUARD 2
            // above compares the next Connect's username against
            // `conn.username`, so a session stored as `#INVALID_USERNAME` matches
            // nothing: the guard sees no existing session, and a second SDK
            // connect runs against a live one -- which is the ratchet reset that
            // guard exists to prevent.
            //
            // The fix landed on the `server_address` read fifteen lines down and
            // not on this one, which is the same shape of miss: an unreadable
            // value replaced by a placeholder that every later comparison fails
            // against.
            let username = match remote.account_manager().get_username_by_cid(cid).await {
                Ok(Some(username)) => username,
                Ok(None) | Err(_) => {
                    citadel_sdk::logging::warn!(
                        target: "citadel",
                        "[Connect] Could not read the username for {}; reporting the connect as \
                         failed rather than recording the session under a placeholder no later \
                         request will match",
                        cid
                    );
                    cleanup_username(this, &username_for_cleanup);
                    let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                        cid,
                        message: format!(
                            "Connected, but could not determine the username for session {}. \
                             Nothing is recorded under a name that would not match; try again.",
                            cid
                        ),
                        request_id: Some(request_id),
                    });
                    return Some(HandledRequestResult { response, uuid });
                }
            };

            // Get server address from the CNAC's connection info.
            //
            // `.ok().flatten()...unwrap_or_default()` turned an unreadable CNAC
            // into an EMPTY address, right after a connect the server had
            // accepted. The UI keys every stored session on
            // `username@serverAddress` -- auto-reconnect, sign-out records and
            // findSessionForServer all do -- so an empty one never matches its
            // stored record: the live session reads as "not active", gets
            // reconnected, is answered SessionAlreadyActive, and the account
            // ends up in the dead state auto-reconnect used to leave behind.
            //
            // Refuse instead. The session is up either way; what we cannot do
            // is report it under a name nothing will match.
            let server_address = match remote
                .account_manager()
                .get_persistence_handler()
                .get_cnac_by_cid(cid)
                .await
            {
                Ok(Some(cnac)) => cnac.get_connect_info().addr.to_string(),
                Ok(None) | Err(_) => {
                    citadel_sdk::logging::warn!(
                        target: "citadel",
                        "[Connect] Could not read the server address for {}; reporting the \
                         connect as failed rather than under an address nothing matches",
                        cid
                    );
                    // `username_for_cleanup`, not the shadowed `username`.
                    //
                    // GUARD 1 inserted the REQUEST's username into
                    // `connecting_usernames`; the binding in scope here is the
                    // SDK-derived one that shadows it. Removing the wrong key
                    // leaves the request's username in the set, and GUARD 1 then
                    // refuses that user every subsequent attempt until the agent
                    // restarts. The two other exits below already use it.
                    cleanup_username(this, &username_for_cleanup);
                    let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                        cid,
                        message: format!(
                            "Connected, but could not determine the server address for session \
                             {}. Nothing is recorded under a name that would not match; try again.",
                            cid
                        ),
                        request_id: Some(request_id),
                    });
                    return Some(HandledRequestResult { response, uuid });
                }
            };

            // Recorded from the password the SERVER just accepted, so a later
            // reuse request has something to prove itself against.
            let fingerprint = crate::kernel::credential_fingerprint::derive(
                remote,
                &username,
                password_for_fingerprint,
            )
            .await;

            let connection_struct = Connection::new(
                sink,
                client_server_remote,
                Arc::new(AtomicUuid::new(uuid)),
                username,
                server_address,
                fingerprint,
            );
            this.server_connection_map
                .write()
                .insert(cid, connection_struct);

            let hm_for_conn = this.tx_to_localhost_clients.clone();
            let server_conn_map = this.server_connection_map.clone();

            let response = InternalServiceResponse::ConnectSuccess(
                citadel_internal_service_types::ConnectSuccess {
                    cid,
                    request_id: Some(request_id),
                },
            );

            let connection_read_stream = async move {
                while let Some(message) = stream.next().await {
                    let message =
                        InternalServiceResponse::MessageNotification(MessageNotification {
                            message: message.into_buffer().into(),
                            cid,
                            peer_cid: 0,
                            request_id: Some(request_id),
                        });

                    // Get the current associated TCP connection for this session (may have changed via ClaimSession)
                    let server_lock = server_conn_map.read();
                    let current_tcp_uuid = server_lock
                        .get(&cid)
                        .map(|conn| {
                            conn.associated_localhost_connection
                                .load(std::sync::atomic::Ordering::Relaxed)
                        })
                        .unwrap_or(uuid);
                    drop(server_lock);

                    let lock = hm_for_conn.read();
                    match lock.get(&current_tcp_uuid) {
                        Some(entry) => {
                            if let Err(err) = entry.send(message) {
                                citadel_sdk::logging::error!(target:"citadel","Error sending message to client: {err:?}");
                            }
                        }
                        None => {
                            citadel_sdk::logging::info!(target:"citadel","Hash map connection not found for TCP uuid: {}", current_tcp_uuid)
                        }
                    }
                }
            };

            tokio::spawn(connection_read_stream);

            cleanup_username(this, &username_for_cleanup);
            Some(HandledRequestResult { response, uuid })
        }

        Err(err) => {
            let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                cid: 0,
                message: err.into_string(),
                request_id: Some(request_id),
            });

            cleanup_username(this, &username_for_cleanup);
            Some(HandledRequestResult { response, uuid })
        }
    }
}
