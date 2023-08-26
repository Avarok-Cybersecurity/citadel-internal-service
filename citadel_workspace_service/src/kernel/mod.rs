use crate::kernel::request_handler::handle_request;
use bytes::Bytes;
use citadel_logging::{error, info, warn};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::VirtualTargetType;
use citadel_sdk::prelude::*;
use citadel_workspace_lib::{deserialize, serialize_payload, wrap_tcp_conn};
use citadel_workspace_types::*;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

pub(crate) mod request_handler;

pub struct CitadelWorkspaceService {
    pub remote: Option<NodeRemote>,
    pub bind_address: SocketAddr,
    pub server_connection_map: Arc<Mutex<HashMap<u64, Connection>>>,
    pub tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
}

impl CitadelWorkspaceService {
    pub fn new(bind_address: SocketAddr) -> Self {
        Self {
            remote: None,
            bind_address,
            server_connection_map: Arc::new(Mutex::new(Default::default())),
            tcp_connection_map: Arc::new(Mutex::new(Default::default())),
        }
    }
}

#[allow(dead_code)]
pub struct Connection {
    sink_to_server: PeerChannelSendHalf,
    client_server_remote: ClientServerRemote,
    peers: HashMap<u64, PeerConnection>,
    associated_tcp_connection: Uuid,
    c2s_file_transfer_handlers: HashMap<u64, Option<ObjectTransferHandler>>,
}

#[allow(dead_code)]
struct PeerConnection {
    sink: PeerChannelSendHalf,
    remote: SymmetricIdentifierHandle,
    handler_map: HashMap<u64, Option<ObjectTransferHandler>>,
    associated_tcp_connection: Uuid,
}

impl Connection {
    fn new(
        sink: PeerChannelSendHalf,
        client_server_remote: ClientServerRemote,
        associated_tcp_connection: Uuid,
    ) -> Self {
        Connection {
            peers: HashMap::new(),
            sink_to_server: sink,
            client_server_remote,
            associated_tcp_connection,
            c2s_file_transfer_handlers: HashMap::new(),
        }
    }

    fn add_peer_connection(
        &mut self,
        peer_cid: u64,
        sink: PeerChannelSendHalf,
        remote: SymmetricIdentifierHandle,
    ) {
        self.peers.insert(
            peer_cid,
            PeerConnection {
                sink,
                remote,
                handler_map: HashMap::new(),
                associated_tcp_connection: self.associated_tcp_connection,
            },
        );
    }

    fn clear_peer_connection(&mut self, peer_cid: u64) -> Option<PeerConnection> {
        self.peers.remove(&peer_cid)
    }

    fn add_object_transfer_handler(
        &mut self,
        peer_cid: u64,
        object_id: u64,
        handler: Option<ObjectTransferHandler>,
    ) {
        if self.implicated_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.insert(object_id, handler);
        } else {
            // P2P
            if let Some(peer_connection) = self.peers.get_mut(&peer_cid) {
                peer_connection.handler_map.insert(object_id, handler);
            }
        }
    }

    // fn remove_object_transfer_handler(&mut self, peer_cid: u64, object_id: u32) -> Option<Option<ObjectTransferHandler>> {
    //     if self.implicated_cid() == peer_cid {
    //         // C2S
    //         self.c2s_file_transfer_handlers.remove(&object_id)
    //     } else {
    //         // P2P
    //         if let Some(peer_connection) = self.peers.get_mut(&peer_cid) {
    //             peer_connection.handler_map.remove(&object_id)
    //         }
    //         else{None}
    //     }
    // }

    fn take_file_transfer_handle(
        &mut self,
        peer_cid: u64,
        object_id: u64,
    ) -> Option<Option<ObjectTransferHandler>> {
        if self.implicated_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.remove(&object_id)
        } else {
            // P2P
            let peer_connection = self.peers.get_mut(&peer_cid)?;
            peer_connection.handler_map.remove(&object_id)
        }
    }

    /// Returns the CID of this C2S connection
    fn implicated_cid(&self) -> u64 {
        self.client_server_remote.user().get_implicated_cid()
    }
}

impl CitadelWorkspaceService {
    async fn clear_peer_connection(
        &self,
        implicated_cid: u64,
        peer_cid: u64,
    ) -> Option<PeerConnection> {
        self.server_connection_map
            .lock()
            .await
            .get_mut(&implicated_cid)?
            .clear_peer_connection(peer_cid)
    }
}

#[async_trait]
impl NetKernel for CitadelWorkspaceService {
    fn load_remote(&mut self, node_remote: NodeRemote) -> Result<(), NetworkError> {
        self.remote = Some(node_remote);
        Ok(())
    }

    async fn on_start(&self) -> Result<(), NetworkError> {
        let mut remote = self.remote.clone().unwrap();
        let remote_for_closure = remote.clone();
        let listener = tokio::net::TcpListener::bind(self.bind_address).await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<InternalServiceRequest>();

        let tcp_connection_map = &self.tcp_connection_map;
        let listener_task = async move {
            while let Ok((conn, _addr)) = listener.accept().await {
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServiceResponse>();
                let id = Uuid::new_v4();
                tcp_connection_map.lock().await.insert(id, tx1);
                handle_connection(conn, tx.clone(), rx1, id);
            }
            Ok(())
        };

        let server_connection_map = &self.server_connection_map;

        let inbound_command_task = async move {
            while let Some(command) = rx.recv().await {
                // TODO: handle error once payload_handler is fallible
                handle_request(
                    command,
                    server_connection_map,
                    &mut remote,
                    tcp_connection_map,
                )
                .await;
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

    async fn on_node_event_received(&self, message: NodeResult) -> Result<(), NetworkError> {
        match message {
            NodeResult::Disconnect(disconnect) => {
                if let Some(conn) = disconnect.v_conn_type {
                    let (signal, conn_uuid) = match conn {
                        VirtualTargetType::LocalGroupServer { implicated_cid } => {
                            let mut server_connection_map = self.server_connection_map.lock().await;
                            if let Some(conn) = server_connection_map.remove(&implicated_cid) {
                                (
                                    InternalServiceResponse::Disconnected(Disconnected {
                                        cid: implicated_cid,
                                        peer_cid: None,
                                        request_id: None,
                                    }),
                                    conn.associated_tcp_connection,
                                )
                            } else {
                                return Ok(());
                            }
                        }
                        VirtualTargetType::LocalGroupPeer {
                            implicated_cid,
                            peer_cid,
                        } => {
                            if let Some(conn) =
                                self.clear_peer_connection(implicated_cid, peer_cid).await
                            {
                                (
                                    InternalServiceResponse::Disconnected(Disconnected {
                                        cid: implicated_cid,
                                        peer_cid: Some(peer_cid),
                                        request_id: None,
                                    }),
                                    conn.associated_tcp_connection,
                                )
                            } else {
                                return Ok(());
                            }
                        }
                        _ => return Ok(()),
                    };

                    send_response_to_tcp_client(&self.tcp_connection_map, signal, conn_uuid).await
                }
            }
            NodeResult::ObjectTransferHandle(object_transfer_handle) => {
                let metadata = object_transfer_handle.handle.metadata.clone();
                let object_id = metadata.object_id;
                let object_transfer_handler = object_transfer_handle.handle;

                let (implicated_cid, peer_cid) = if matches!(
                    object_transfer_handler.orientation,
                    ObjectTransferOrientation::Receiver {
                        is_revfs_pull: true
                    }
                ) {
                    // When this is a REVFS pull reception handle, THIS node is the source of the file.
                    // The other node, i.e. the peer, is the receiver who is requesting the file.
                    (
                        object_transfer_handler.source,
                        object_transfer_handler.receiver,
                    )
                } else {
                    (
                        object_transfer_handler.receiver,
                        object_transfer_handler.source,
                    )
                };

                citadel_logging::info!(target: "citadel", "Orientation: {:?}", object_transfer_handler.orientation);

                // When we receive a handle, there are two possibilities:
                // A: We are the sender of the file transfer, in which case we can assume the adjacent node
                // already accepted the file transfer request, and therefore we can spawn a task to forward
                // the ticks immediately
                //
                // B: We are the receiver of the file transfer. We need to wait for the TCP client to accept
                // the request, thus, we need to store it. UNLESS, this is an revfs pull, in which case we
                // allow the transfer to proceed immediately since the protocol auto accepts these requests
                if let ObjectTransferOrientation::Receiver { is_revfs_pull } =
                    object_transfer_handler.orientation
                {
                    info!(target: "citadel", "Receiver Obtained ObjectTransferHandler");

                    let mut server_connection_map = self.server_connection_map.lock().await;
                    if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
                        let uuid = connection.associated_tcp_connection;

                        if is_revfs_pull {
                            spawn_tick_updater(
                                object_transfer_handler,
                                implicated_cid,
                                peer_cid,
                                &mut server_connection_map,
                                self.tcp_connection_map.clone(),
                            );
                        } else {
                            // Send an update to the TCP client that way they can choose to accept or reject the transfer
                            let response =
                                InternalServiceResponse::FileTransferRequest(FileTransferRequest {
                                    cid: implicated_cid,
                                    peer_cid,
                                    metadata,
                                });
                            send_response_to_tcp_client(&self.tcp_connection_map, response, uuid)
                                .await;
                            connection.add_object_transfer_handler(
                                peer_cid,
                                object_id,
                                Some(object_transfer_handler),
                            );
                        }
                    }
                } else {
                    // Sender - Must spawn a task to relay status updates to TCP client. When receiving this handle,
                    // we know the opposite node agreed to the connection thus we can spawn
                    let mut server_connection_map = self.server_connection_map.lock().await;
                    info!(target: "citadel", "Sender Obtained ObjectTransferHandler");
                    spawn_tick_updater(
                        object_transfer_handler,
                        implicated_cid,
                        peer_cid,
                        &mut server_connection_map,
                        self.tcp_connection_map.clone(),
                    );
                }
            }
            NodeResult::PeerEvent(event) => {
                if let PeerSignal::Disconnect(
                    PeerConnectionType::LocalGroupPeer {
                        implicated_cid,
                        peer_cid,
                    },
                    _,
                ) = event.event
                {
                    if let Some(conn) = self.clear_peer_connection(implicated_cid, peer_cid).await {
                        let response = InternalServiceResponse::Disconnected(Disconnected {
                            cid: implicated_cid,
                            peer_cid: Some(peer_cid),
                            request_id: None,
                        });
                        send_response_to_tcp_client(
                            &self.tcp_connection_map,
                            response,
                            conn.associated_tcp_connection,
                        )
                        .await;
                    }
                }
            }

            _ => {}
        }
        // TODO: handle disconnect properly by removing entries from the hashmap
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}

async fn send_response_to_tcp_client(
    hash_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    response: InternalServiceResponse,
    uuid: Uuid,
) {
    hash_map
        .lock()
        .await
        .get(&uuid)
        .unwrap()
        .send(response)
        .unwrap()
}

fn create_client_server_remote(
    conn_type: VirtualTargetType,
    remote: NodeRemote,
) -> ClientServerRemote {
    ClientServerRemote::new(conn_type, remote)
}

async fn sink_send_payload(
    payload: &InternalServiceResponse,
    sink: &mut SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
) {
    let payload = serialize_payload(payload);
    match sink.send(payload.into()).await {
        Ok(_) => (),
        Err(_) => info!(target: "citadel", "w task: sink send err"),
    }
}

fn send_to_kernel(
    payload_to_send: &[u8],
    sender: &UnboundedSender<InternalServiceRequest>,
) -> Result<(), NetworkError> {
    if let Some(payload) = deserialize(payload_to_send) {
        sender.send(payload)?;
        Ok(())
    } else {
        error!(target: "citadel", "w task: failed to deserialize payload");
        Ok(())
    }
}

fn handle_connection(
    conn: TcpStream,
    to_kernel: UnboundedSender<InternalServiceRequest>,
    mut from_kernel: tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    conn_id: Uuid,
) {
    tokio::task::spawn(async move {
        let framed = wrap_tcp_conn(conn);
        let (mut sink, mut stream) = framed.split();

        let write_task = async move {
            let response =
                InternalServiceResponse::ServiceConnectionAccepted(ServiceConnectionAccepted {
                    id: conn_id,
                    request_id: None,
                });

            sink_send_payload(&response, &mut sink).await;

            while let Some(kernel_response) = from_kernel.recv().await {
                sink_send_payload(&kernel_response, &mut sink).await;
            }
        };

        let read_task = async move {
            while let Some(message) = stream.next().await {
                match message {
                    Ok(message) => {
                        if let Err(err) = send_to_kernel(&message, &to_kernel) {
                            error!(target: "citadel", "Failed to send to kernel: {:?}", err);
                            break;
                        }
                    }
                    Err(_) => {
                        warn!(target: "citadel", "Bad message from client");
                    }
                }
            }
            info!(target: "citadel", "Disconnected");
        };

        tokio::select! {
            res0 = write_task => res0,
            res1 = read_task => res1,
        }
    });
}

fn spawn_tick_updater(
    object_transfer_handler: ObjectTransferHandler,
    implicated_cid: u64,
    peer_cid: u64,
    server_connection_map: &mut HashMap<u64, Connection>,
    tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    let mut handle_inner = object_transfer_handler.inner;
    if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
        let uuid = connection.associated_tcp_connection;
        let sender_status_updater = async move {
            while let Some(status) = handle_inner.next().await {
                let status_message = status.clone();
                match tcp_connection_map.lock().await.get(&uuid) {
                    Some(entry) => {
                        let message = InternalServiceResponse::FileTransferTick(FileTransferTick {
                            uuid,
                            cid: implicated_cid,
                            peer_cid,
                            status: status_message,
                        });
                        match entry.send(message.clone()) {
                            Ok(res) => res,
                            Err(_) => {
                                info!(target: "citadel", "File Transfer Status Tick Not Sent")
                            }
                        }

                        if matches!(
                            status,
                            ObjectTransferStatus::TransferComplete { .. }
                                | ObjectTransferStatus::ReceptionComplete
                        ) {
                            break;
                        }
                    }
                    None => {
                        info!(target:"citadel","Connection not found during File Transfer Status Tick")
                    }
                }
            }
        };
        tokio::task::spawn(sender_status_updater);
    } else {
        info!(target: "citadel", "Server Connection Not Found")
    }
}
