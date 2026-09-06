use crate::kernel::ext::IOInterfaceExt;
use crate::kernel::media::{
    media_lane, MediaLaneTx, PeerMediaSession, UdpState, MEDIA_LANE_CAPACITY,
};
use crate::kernel::requests::{handle_request, HandledRequestResult};
use crate::kernel::session_route::SessionRoute;
use citadel_internal_service_connector::connector::{
    InternalServiceConnector, WrappedSink, WrappedStream,
};
use citadel_internal_service_connector::io_interface::in_memory::{
    InMemoryInterface, InMemorySink, InMemoryStream,
};
#[cfg(feature = "websockets")]
use citadel_internal_service_connector::io_interface::origin_policy::OriginPolicy;
use citadel_internal_service_connector::io_interface::tcp::TcpIOInterface;
#[cfg(feature = "websockets")]
use citadel_internal_service_connector::io_interface::websockets::WebSocketInterface;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::*;
use citadel_sdk::logging::{error, info, warn};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::remote_specialization::PeerRemote;
use citadel_sdk::prelude::VirtualTargetType;
use citadel_sdk::prelude::*;
use futures::stream::StreamExt;
use futures::{Sink, SinkExt};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver as OneshotReceiver;
use uuid::Uuid;

pub(crate) mod credential_fingerprint;
pub(crate) mod ext;
pub(crate) mod group_channels;
pub(crate) mod media;
pub(crate) mod picked_files;
pub(crate) mod requests;
pub(crate) mod responses;
pub(crate) mod revfs_correlation;
pub(crate) mod session_route;

pub type RatchetType = StackedRatchet;

pub mod cid_scoped_state;

pub struct CitadelWorkspaceService<T, R: Ratchet> {
    pub remote: Option<NodeRemote<R>>,
    /// Session connection map - use .read() for lookups, .write() for insert/remove
    /// CRITICAL: Never hold lock across .await points - use block pattern to extract needed data
    pub server_connection_map: Arc<RwLock<HashMap<u64, Connection<R>>>>,
    /// tx_to_localhost_clients was formerly "tcp_connection_map". Still the same functionality, but more accurately
    /// represents that this map is used for sending messages to localhost clients who COULD be TCP, but NOT necessarily TCP
    /// like websocket clients for browser clients.
    pub tx_to_localhost_clients:
        Arc<RwLock<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    /// The bounded, latest-frame lane to each localhost client, for media only.
    ///
    /// Parallel to `tx_to_localhost_clients` rather than replacing it: the two
    /// carry traffic with opposite requirements. Control must arrive, so it
    /// queues without limit; media must be timely, so it is capped and the
    /// oldest frame is evicted when a client falls behind. One queue cannot
    /// honour both, and sharing the unbounded one meant a slow browser
    /// accumulated stale video until the connection ended.
    pub media_lanes: Arc<RwLock<HashMap<Uuid, MediaLaneTx>>>,
    pub orphan_sessions: Arc<RwLock<HashMap<Uuid, bool>>>, // Maps TCP connection ID to orphan mode
    /// Stores pending PeerConnect signals awaiting UI acceptance.
    /// Key is (session_cid, peer_cid), value is the original PeerSignal for responding.
    pub pending_peer_connect_signals: Arc<RwLock<HashMap<(u64, u64), PeerSignal>>>,
    /// Stores pending PeerRegister signals awaiting UI acceptance.
    /// Key is (session_cid, peer_cid), value is the original PostRegister PeerSignal for responding.
    pub pending_peer_registrations: Arc<RwLock<HashMap<(u64, u64), PeerSignal>>>,
    /// Cache for peer usernames received from registration events.
    /// Key is (session_cid, peer_cid), value is the peer's username.
    /// Used as fallback when SDK's get_local_group_mutual_peers returns empty username.
    pub peer_username_cache: Arc<RwLock<HashMap<(u64, u64), String>>>,
    /// Tracks usernames currently being connected to prevent duplicate concurrent connection attempts.
    /// This prevents TOCTOU race conditions where two Connect requests arrive simultaneously.
    pub connecting_usernames: Arc<Mutex<HashSet<String>>>,
    io: Arc<RwLock<Option<T>>>,
}

impl<T, R: Ratchet> Clone for CitadelWorkspaceService<T, R> {
    fn clone(&self) -> Self {
        CitadelWorkspaceService {
            remote: self.remote.clone(),
            server_connection_map: self.server_connection_map.clone(),
            tx_to_localhost_clients: self.tx_to_localhost_clients.clone(),
            media_lanes: self.media_lanes.clone(),
            orphan_sessions: self.orphan_sessions.clone(),
            pending_peer_connect_signals: self.pending_peer_connect_signals.clone(),
            pending_peer_registrations: self.pending_peer_registrations.clone(),
            peer_username_cache: self.peer_username_cache.clone(),
            connecting_usernames: self.connecting_usernames.clone(),
            io: self.io.clone(),
        }
    }
}

impl<T: IOInterface, R: Ratchet> From<T> for CitadelWorkspaceService<T, R> {
    fn from(io: T) -> Self {
        CitadelWorkspaceService {
            remote: None,
            server_connection_map: Arc::new(RwLock::new(Default::default())),
            tx_to_localhost_clients: Arc::new(RwLock::new(Default::default())),
            media_lanes: Arc::new(RwLock::new(Default::default())),
            orphan_sessions: Arc::new(RwLock::new(Default::default())),
            pending_peer_connect_signals: Arc::new(RwLock::new(Default::default())),
            pending_peer_registrations: Arc::new(RwLock::new(Default::default())),
            peer_username_cache: Arc::new(RwLock::new(Default::default())),
            connecting_usernames: Arc::new(Mutex::new(HashSet::new())),
            io: Arc::new(RwLock::new(Some(io))),
        }
    }
}

impl<T: IOInterface, R: Ratchet> CitadelWorkspaceService<T, R> {
    pub fn new(io: T) -> Self {
        io.into()
    }

    pub fn remote(&self) -> &NodeRemote<R> {
        self.remote.as_ref().expect("Kernel not loaded")
    }
}

impl<R: Ratchet> CitadelWorkspaceService<TcpIOInterface, R> {
    pub async fn new_tcp(
        bind_address: SocketAddr,
    ) -> std::io::Result<CitadelWorkspaceService<TcpIOInterface, R>> {
        Ok(TcpIOInterface::new(bind_address).await?.into())
    }

    #[cfg(feature = "websockets")]
    /// `origins` is required, not optional: see
    /// `io_interface::origin_policy` for why the convenient default is the
    /// hole this closes.
    pub async fn new_websocket(
        bind_address: SocketAddr,
        origins: OriginPolicy,
    ) -> std::io::Result<CitadelWorkspaceService<WebSocketInterface, R>> {
        let ws_server_io = WebSocketInterface::new(bind_address, origins).await?;
        Ok(ws_server_io.into())
    }
}

impl<R: Ratchet> CitadelWorkspaceService<InMemoryInterface, R> {
    /// Generates an in-memory service connector and kernel. This is useful for programs that do not need
    /// networking to connect between the application and the internal service
    pub fn new_in_memory() -> (
        InternalServiceConnector<InMemoryInterface>,
        CitadelWorkspaceService<InMemoryInterface, R>,
    ) {
        let (tx_to_consumer, rx_from_consumer) = tokio::sync::mpsc::unbounded_channel();
        let (tx_to_svc, rx_from_svc) = tokio::sync::mpsc::unbounded_channel();
        let connector = InternalServiceConnector {
            sink: WrappedSink {
                inner: InMemorySink(tx_to_svc),
            },
            stream: WrappedStream::new(InMemoryStream(rx_from_consumer)),
        };
        let kernel = InMemoryInterface {
            sink: Some(tx_to_consumer),
            stream: Some(rx_from_svc),
        }
        .into();
        (connector, kernel)
    }
}

/// Wrapper around PeerChannelSendHalf that allows cloning for async-safe access.
/// This enables us to drop the RwLock on server_connection_map before awaiting sends.
pub type AsyncSink<R> = Arc<tokio::sync::Mutex<PeerChannelSendHalf<R>>>;

/// Information about a file picked via the native file picker dialog.
/// Stored temporarily to allow subsequent SendFile requests to reference the picked file.
#[derive(Debug, Clone)]
pub struct PickedFileInfo {
    /// Full path to the picked file
    pub file_path: PathBuf,
    /// File name (basename)
    pub file_name: String,
    /// File size in bytes
    pub file_size: u64,
    /// When the file was picked (for expiration/cleanup)
    pub picked_at: Instant,
}

#[allow(dead_code)]
pub struct Connection<R: Ratchet> {
    pub sink_to_server: AsyncSink<R>,
    pub client_server_remote: ClientServerRemote<R>,
    pub peers: HashMap<u64, PeerConnection<R>>,
    pub(crate) associated_localhost_connection: Arc<AtomicUuid>,
    pub c2s_file_transfer_handlers: HashMap<ObjectId, Option<ObjectTransferHandler>>,
    /// Group channels this session is a member of. Not a plain HashMap: the
    /// map was insert-only, so entries outlived the membership they described
    /// and a GroupMessage to a group the user had left still found a working
    /// sender and reported success. See kernel/group_channels.rs for how
    /// entries expire when this session leaves or ends a group.
    pub groups: group_channels::GroupChannels,
    pub username: String,
    pub server_address: String,
    /// Storage for files picked via PickFile command.
    /// Key is the request_id from the PickFile request.
    /// Used to resolve FileSource::PickFileRef in SendFile commands.
    pub picked_files: HashMap<Uuid, PickedFileInfo>,
    /// Pending REVFS pull/push request ids, consumed when the matching
    /// ObjectTransferHandle arrives so its ticks carry the browser's
    /// request_id instead of the meaningless TCP-connection uuid fallback.
    /// See kernel/revfs_correlation.rs for the mechanism.
    pub revfs_correlations: revfs_correlation::RevfsCorrelations,
    /// The client-side password hash this session was opened with.
    ///
    /// Consulted when a later `Connect` names this session's username, so the
    /// session cannot be handed over -- message stream and all -- to a caller
    /// who only knew the username. See kernel/credential_fingerprint.rs for why
    /// this is a recorded fingerprint rather than a local credential check.
    pub credential_fingerprint: Option<Vec<u8>>,
}

#[allow(dead_code)]
pub struct PeerConnection<R: Ratchet> {
    pub sink: AsyncSink<R>,
    /// Optional PeerRemote for advanced operations (file transfers, etc.)
    /// May be None for acceptor-side connections where we only have the channel.
    remote: Option<PeerRemote<R>>,
    handler_map: HashMap<ObjectId, Option<ObjectTransferHandler>>,
    associated_localhost_connection: Arc<AtomicUuid>,
    /// Where this peer's UDP transport currently lives. The SDK delivers the
    /// channel at most once per peer connection, so media sessions borrow the
    /// halves through this state machine and return them on close — consuming
    /// them would make every call after the first impossible.
    pub udp: UdpState<R>,
    /// The live call with this peer, if any. Dropping it stops the inbound pump.
    ///
    /// Boxed because a MediaSession carries the packetizer's 256-entry
    /// per-track sequence table — over a kilobyte — and this struct is stored
    /// per peer whether or not there is ever a call. Inline, every peer paid for
    /// a call almost none of them make.
    pub media: Option<Box<PeerMediaSession<R>>>,
    /// Bumped by every media close/teardown. An open that had to await captures
    /// this first and refuses to install its session if the value moved, so a
    /// close landing mid-open wins instead of leaving a zombie session.
    pub media_generation: u64,
    /// The localhost connection whose `MediaOpen` is currently awaiting a UDP
    /// channel, if one is.
    ///
    /// A close bumps `media_generation` even when no session exists yet, so
    /// that a close landing mid-open cancels it rather than letting a zombie
    /// pump install itself. That bump is unauthenticated on its own: once a
    /// reconnect hands the client a fresh uuid, a delayed `MediaClose` from the
    /// dead connection would cancel the NEW connection's open. Recording who is
    /// opening lets the close tell "the owner changed its mind" from "a stale
    /// connection is cancelling someone else's call".
    pub media_pending_owner: Option<Uuid>,
}

#[allow(dead_code)]
pub struct GroupConnection {
    key: MessageGroupKey,
    tx: GroupChannelSendHalf,
    cid: u64,
}

impl<R: Ratchet> Connection<R> {
    fn new(
        sink: PeerChannelSendHalf<R>,
        client_server_remote: ClientServerRemote<R>,
        associated_tcp_connection: Arc<AtomicUuid>,
        username: String,
        server_address: String,
        credential_fingerprint: Option<Vec<u8>>,
    ) -> Self {
        Connection {
            peers: HashMap::new(),
            sink_to_server: Arc::new(tokio::sync::Mutex::new(sink)),
            client_server_remote,
            associated_localhost_connection: associated_tcp_connection,
            c2s_file_transfer_handlers: HashMap::new(),
            username,
            groups: group_channels::GroupChannels::new(),
            server_address,
            picked_files: HashMap::new(),
            revfs_correlations: revfs_correlation::RevfsCorrelations::default(),
            credential_fingerprint,
        }
    }

    fn add_peer_connection(
        &mut self,
        peer_cid: u64,
        sink: PeerChannelSendHalf<R>,
        remote: PeerRemote<R>,
        udp_rx: Option<OneshotReceiver<UdpChannel<R>>>,
    ) {
        self.upsert_peer_connection(peer_cid, sink, Some(remote), udp_rx);
    }

    /// Add a peer connection without a PeerRemote (for acceptor-side channels)
    pub fn add_peer_connection_channel_only(
        &mut self,
        peer_cid: u64,
        sink: PeerChannelSendHalf<R>,
        udp_rx: Option<OneshotReceiver<UdpChannel<R>>>,
    ) {
        self.upsert_peer_connection(peer_cid, sink, None, udp_rx);
    }

    /// Insert-or-update. On update the fresh sink replaces the stale one, but a
    /// live media session and its UDP transport survive: blind `insert` used to
    /// replace the whole entry on duplicate PeerChannelCreated events or mid-call
    /// re-handshakes, and the replaced entry's Drop aborted the call's inbound
    /// pump — silently destroying a working call.
    fn upsert_peer_connection(
        &mut self,
        peer_cid: u64,
        sink: PeerChannelSendHalf<R>,
        remote: Option<PeerRemote<R>>,
        udp_rx: Option<OneshotReceiver<UdpChannel<R>>>,
    ) {
        match self.peers.entry(peer_cid) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let peer = entry.get_mut();
                peer.sink = Arc::new(tokio::sync::Mutex::new(sink));
                if remote.is_some() {
                    peer.remote = remote;
                }
                // A fresh UDP offer is adopted only between calls: a live call
                // keeps the transport it is using, and an open mid-await keeps
                // its `Opening` marker so its commit logic stays single-owner.
                if peer.media.is_none() && !matches!(peer.udp, UdpState::Opening) {
                    if let Some(rx) = udp_rx {
                        // APPENDED, not assigned. A simultaneous connect makes
                        // two peer connections and the SDK offers a UDP channel
                        // once per connection, so the second offer used to
                        // overwrite the first and drop it -- and when the
                        // surviving connection was not the one whose offer was
                        // kept, the receiver never fired and every call to that
                        // peer failed with "no UDP channel within 5s".
                        peer.udp.offer(rx);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PeerConnection {
                    sink: Arc::new(tokio::sync::Mutex::new(sink)),
                    remote,
                    handler_map: HashMap::new(),
                    associated_localhost_connection: self.associated_localhost_connection.clone(),
                    udp: UdpState::from_optional_channel(udp_rx),
                    media: None,
                    media_generation: 0,
                    media_pending_owner: None,
                });
            }
        }
    }

    fn add_object_transfer_handler(
        &mut self,
        peer_cid: u64,
        object_id: ObjectId,
        handler: Option<ObjectTransferHandler>,
    ) {
        if self.session_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.insert(object_id, handler);
        } else {
            // P2P
            if let Some(peer_connection) = self.peers.get_mut(&peer_cid) {
                peer_connection.handler_map.insert(object_id, handler);
            }
        }
    }

    pub fn add_group_channel(
        &mut self,
        group_key: MessageGroupKey,
        group_channel: GroupConnection,
    ) {
        // The insert wraps the SDK send half so the entry expires when this
        // session leaves or ends the group — a bare HashMap insert left the
        // entry (and the membership it implied) alive for the whole session.
        self.groups.insert(group_key, group_channel);
    }

    fn take_file_transfer_handle(
        &mut self,
        peer_cid: u64,
        object_id: ObjectId,
    ) -> Option<Option<ObjectTransferHandler>> {
        if self.session_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.remove(&object_id)
        } else {
            // P2P
            let peer_connection = self.peers.get_mut(&peer_cid)?;
            peer_connection.handler_map.remove(&object_id)
        }
    }

    /// Returns the CID of this C2S connection
    fn session_cid(&self) -> u64 {
        self.client_server_remote.user().get_session_cid()
    }
}

impl<T: IOInterface, R: Ratchet> CitadelWorkspaceService<T, R> {
    // Query SDK for active sessions. Useful for when determining if there is asymmetry between the inner protocol
    // and the internal service
    pub async fn client_or_peer_in_protocol(
        &self,
        cid: u64,
        peer_cid: Option<u64>,
    ) -> Result<bool, NetworkError> {
        self.remote().sessions().await.map(|sessions| {
            let conn = sessions.sessions.iter().find(|sess| sess.cid == cid);
            if let Some(conn) = conn {
                if let Some(peer_cid) = peer_cid {
                    conn.connections
                        .iter()
                        .any(|conn| conn.peer_cid.unwrap_or(0) == peer_cid)
                } else {
                    // C2S connected already
                    true
                }
            } else {
                false
            }
        })
    }
}

#[async_trait]
impl<T: IOInterface + Sync, R: Ratchet> NetKernel<R> for CitadelWorkspaceService<T, R> {
    fn load_remote(&mut self, node_remote: NodeRemote<R>) -> Result<(), NetworkError> {
        self.remote = Some(node_remote);
        Ok(())
    }

    async fn on_start(&self) -> Result<(), NetworkError> {
        let this = self.clone();
        let remote = self.remote.clone().ok_or_else(|| {
            NetworkError::msg("Kernel remote not initialized when on_start called")
        })?;
        let remote_for_closure = remote.clone();
        let mut io = self.io.write().take().expect("Already called");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let tcp_connection_map = &self.tx_to_localhost_clients;
        let media_lanes = &self.media_lanes;
        let server_connection_map = &self.server_connection_map;

        let listener_task = async move {
            while let Some((sink, stream)) = io.next_connection().await {
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServiceResponse>();
                // Media gets a second, bounded lane to the same socket. Created
                // here beside the reliable one so every connection has both for
                // its whole life, rather than appearing when a call starts and
                // needing a null case everywhere else.
                let (media_tx, media_rx) = media_lane(MEDIA_LANE_CAPACITY);
                let id = Uuid::new_v4();
                tcp_connection_map.write().insert(id, tx1);
                media_lanes.write().insert(id, media_tx);
                io.spawn_connection_handler(
                    sink,
                    stream,
                    tx.clone(),
                    rx1,
                    media_rx,
                    id,
                    tcp_connection_map.clone(),
                    media_lanes.clone(),
                    server_connection_map.clone(),
                    self.orphan_sessions.clone(),
                );
            }
            Ok(())
        };

        let _server_connection_map = &self.server_connection_map;

        let inbound_command_task = async move {
            while let Some((command, conn_id)) = rx.recv().await {
                let this = this.clone();

                let task = async move {
                    if let Some(HandledRequestResult { response, uuid }) =
                        handle_request(&this, conn_id, command).await
                    {
                        if let Err(err) = send_response_to_tcp_client(
                            &this.tx_to_localhost_clients,
                            response,
                            uuid,
                        ) {
                            // The TCP connection no longer exists. Delete it from both maps
                            error!(target: "citadel", "Failed to send response to TCP client: {err:?}");
                            this.tx_to_localhost_clients.write().remove(&uuid);
                            if let Some(lane) = this.media_lanes.write().remove(&uuid) {
                                lane.close();
                            }
                            this.server_connection_map.write().retain(|_, v| {
                                v.associated_localhost_connection.load(Ordering::Relaxed) != uuid
                            });
                        }
                    }
                };

                // Spawn the task to allow for parallel request handling
                drop(tokio::task::spawn(task));
            }
            Ok(())
        };

        let res = tokio::select! {
            res0 = listener_task => res0,
            res1 = inbound_command_task => res1,
        };

        warn!(target: "citadel", "Shutting down service because a critical task finished. {res:?}");
        remote_for_closure.shutdown().await?;
        res
    }

    async fn on_node_event_received(&self, message: NodeResult<R>) -> Result<(), NetworkError> {
        // Log and continue. Returning Err here does NOT fail one event — the
        // SDK's KernelExecutor treats it as fatal:
        //
        //   if let Err(err) = kernel_ref.on_node_event_received(message).await {
        //       log::error!(target: "citadel", "Kernel threw an error: {:?}. Will end", err);
        //       citadel_server_remote.clone().shutdown().await?;
        //
        // so ONE failed delivery shut down the entire local agent — every
        // session for every account multiplexed through it — and with the
        // default in-memory backend, every account with it.
        //
        // The reachable triggers are ordinary: a P2P channel arriving for a
        // session that was just removed from the map (connect.rs removes and
        // then sleeps 200ms before reconnecting), or a send to a tcp entry
        // whose receiver was dropped a moment ago because a tab closed. Neither
        // means the node cannot continue; both used to end it.
        //
        // Err is reserved for conditions that genuinely mean this kernel cannot
        // keep running. A per-session routing failure is not one.
        if let Err(error) = responses::handle_node_result(self, message).await {
            error!(target: "citadel", "[Kernel] Failed to handle node event: {error:?}. Continuing — this is not fatal to the agent.");
        }
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}

fn send_response_to_tcp_client(
    hash_map: &Arc<RwLock<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    response: InternalServiceResponse,
    uuid: Uuid,
) -> Result<(), NetworkError> {
    let map = hash_map.read();

    match map.get(&uuid) {
        Some(sender) => sender.send(response).map_err(|err| {
            NetworkError::generic(format!("Failed to send response to TCP client: {err:?}"))
        }),
        None => {
            // Log a warning instead of returning an error that crashes the service
            warn!(target: "citadel", "TCP connection not found: {uuid:?} - response will be dropped");
            Ok(())
        }
    }
}

// TODO: return scoped wrapper type
fn create_client_server_remote<R: Ratchet>(
    conn_type: VirtualTargetType,
    remote: NodeRemote<R>,
    security_settings: SessionSecuritySettings,
) -> ClientServerRemote<R> {
    ClientServerRemote::new(conn_type, remote, security_settings, None, None)
}

pub(crate) async fn sink_send_payload<T: IOInterface>(
    payload: InternalServiceResponse,
    sink: &mut T::Sink,
) -> Result<(), <T::Sink as Sink<InternalServicePayload>>::Error> {
    sink.send(InternalServicePayload::Response(payload)).await
}

pub(crate) fn send_to_kernel(
    request: InternalServiceRequest,
    sender: &UnboundedSender<(InternalServiceRequest, Uuid)>,
    conn_id: Uuid,
) -> Result<(), NetworkError> {
    sender.send((request, conn_id))?;
    Ok(())
}

fn spawn_tick_updater<R: Ratchet>(
    object_transfer_handler: ObjectTransferHandler,
    implicated_cid: u64,
    peer_cid: Option<u64>,
    server_connection_map: &mut HashMap<u64, Connection<R>>,
    tcp_connection_map: Arc<RwLock<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    request_id: Option<Uuid>,
) {
    let mut handle_inner = object_transfer_handler.inner;
    if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
        let uuid = connection
            .associated_localhost_connection
            .load(Ordering::Relaxed);
        // The REQUEST id may be frozen -- it names the request that started the
        // transfer and does not change. The ROUTE may not: a reclaim re-points
        // this session mid-transfer and every remaining tick has to follow it.
        // See kernel/session_route.rs.
        let request_id = Some(request_id.unwrap_or(uuid));
        let route = SessionRoute::new(
            connection.associated_localhost_connection.clone(),
            tcp_connection_map,
        );
        let sender_status_updater = async move {
            while let Some(status) = handle_inner.next().await {
                let status_message = status.clone();
                let message = InternalServiceResponse::FileTransferTickNotification(
                    FileTransferTickNotification {
                        cid: implicated_cid,
                        peer_cid,
                        status: status_message,
                        request_id,
                    },
                );
                match route.send(message) {
                    Some(target) => {
                        info!(target: "citadel", "File Transfer Status Tick Sent to {target:?}: {status:?}")
                    }
                    None => {
                        warn!(target: "citadel", "No localhost connection owns CID {implicated_cid} - File Transfer Status Tick dropped")
                    }
                }

                // Outside the delivery result on purpose. The transfer is over
                // whether or not anybody was listening; keeping the task alive
                // because a tab happened to be closed is how these leak.
                if matches!(
                    status,
                    ObjectTransferStatus::TransferComplete
                        | ObjectTransferStatus::ReceptionComplete
                ) {
                    info!(target: "citadel", "File Transfer Completed - Ending Tick Updater");
                    break;
                }
            }
            info!(target:"citadel", "Spawned Tick Updater has ended for {implicated_cid:?}");
        };
        tokio::task::spawn(sender_status_updater);
    } else {
        info!(target: "citadel", "tick_updater: Server Connection Not Found")
    }
}
