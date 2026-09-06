//! C2S Registration Handler
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
//! ### Register vs Connect
//! - `register.rs` (this file) → `remote.register()` → Creates NEW account with NEW CID
//! - `connect.rs` → `remote.connect()` → Connects to EXISTING account, SAME CID

use crate::kernel::requests::{handle_request, HandledRequestResult};
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use citadel_sdk::logging::info;
use citadel_sdk::prelude::{ProtocolRemoteExt, Ratchet};
use uuid::Uuid;

pub async fn handle<T: IOInterface + Sync, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::Register {
        request_id,
        server_addr,
        full_name,
        username,
        proposed_password,
        connect_after_register,
        session_security_settings,
        server_password,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    // Resolve HERE, not in the browser.
    //
    // `server_addr` arrives as `host:port`. It used to be a `SocketAddr`, which
    // forced the page to resolve a hostname itself -- with a DNS-over-HTTPS
    // fetch to `https://dns.google/resolve`. A hosted UI's Content-Security-
    // Policy refuses that connection, so on work.avarok.net every hostname
    // address timed out after 30 seconds while a raw IP worked, and where it
    // did work it disclosed each user's server to a third party.
    //
    // A failed lookup is answered, not logged: registration is a foreground
    // action and "Registration timed out" is what the user saw for a name that
    // simply does not resolve.
    let server_addr = match tokio::net::lookup_host(&server_addr).await {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                return Some(HandledRequestResult {
                    response: InternalServiceResponse::RegisterFailure(
                        citadel_internal_service_types::RegisterFailure {
                            cid: 0,
                            message: format!("{server_addr} resolved to no addresses"),
                            request_id: Some(request_id),
                        },
                    ),
                    uuid,
                });
            }
        },
        Err(err) => {
            return Some(HandledRequestResult {
                response: InternalServiceResponse::RegisterFailure(
                    citadel_internal_service_types::RegisterFailure {
                        cid: 0,
                        message: format!("could not resolve {server_addr}: {err}"),
                        request_id: Some(request_id),
                    },
                ),
                uuid,
            });
        }
    };

    info!(target: "citadel", "About to connect to server {server_addr:?} for user {username}");
    match remote
        .register(
            server_addr,
            full_name,
            username.clone(),
            proposed_password.clone(),
            session_security_settings,
            server_password.clone(),
        )
        .await
    {
        Ok(res) => match connect_after_register {
            false => {
                let response = InternalServiceResponse::RegisterSuccess(
                    citadel_internal_service_types::RegisterSuccess {
                        cid: res.cid,
                        request_id: Some(request_id),
                    },
                );

                Some(HandledRequestResult { response, uuid })
            }
            true => {
                let connect_command = InternalServiceRequest::Connect {
                    username,
                    password: proposed_password,
                    keep_alive_timeout: None,
                    udp_mode: Default::default(),
                    connect_mode: Default::default(),
                    session_security_settings,
                    request_id,
                    server_password,
                };

                handle_request(this, uuid, connect_command).await
            }
        },
        Err(err) => {
            let response = InternalServiceResponse::RegisterFailure(
                citadel_internal_service_types::RegisterFailure {
                    cid: 0,
                    message: err.into_string(),
                    request_id: Some(request_id),
                },
            );

            Some(HandledRequestResult { response, uuid })
        }
    }
}
