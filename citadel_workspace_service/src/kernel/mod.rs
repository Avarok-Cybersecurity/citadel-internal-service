use bytes::Bytes;
use citadel_logging::{error, info, warn};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::VirtualTargetType;
use citadel_sdk::prelude::*;
use citadel_workspace_lib::{deserialize, serialize_payload, wrap_tcp_conn};
use citadel_workspace_types::*;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use payload_handler::payload_handler;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

pub(crate) mod payload_handler;

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
    c2s_file_transfer_handlers: HashMap<u32, Option<ObjectTransferHandler>>,
}

#[allow(dead_code)]
struct PeerConnection {
    sink: PeerChannelSendHalf,
    remote: SymmetricIdentifierHandle,
    handler_map: HashMap<u32, Option<ObjectTransferHandler>>,
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
            },
        );
    }

    fn clear_peer_connection(&mut self, peer_cid: u64) -> Option<PeerConnection> {
        self.peers.remove(&peer_cid)
    }

    fn add_object_transfer_handler(
        &mut self,
        peer_cid: u64,
        object_id: u32,
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
        object_id: u32,
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

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();

        let tcp_connection_map = &self.tcp_connection_map.clone();
        let listener_task = async move {
            while let Ok((conn, _addr)) = listener.accept().await {
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServiceResponse>();
                let id = Uuid::new_v4();
                tcp_connection_map.lock().await.insert(id, tx1);
                handle_connection(conn, tx.clone(), rx1, id);
            }
            Ok(())
        };

        let server_connection_map = self.server_connection_map.clone();

        let inbound_command_task = async move {
            while let Some(command) = rx.recv().await {
                // TODO: handle error once payload_handler is fallible
                payload_handler(
                    command,
                    &server_connection_map,
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
                    match conn {
                        VirtualTargetType::LocalGroupServer { implicated_cid } => {
                            let mut server_connection_map = self.server_connection_map.lock().await;
                            server_connection_map.remove(&implicated_cid);
                            // TODO: send disconnect signal to the TCP connection
                            // interested in this c2s connection
                        }
                        VirtualTargetType::LocalGroupPeer {
                            implicated_cid,
                            peer_cid,
                        } => {
                            let _did_remove = self
                                .clear_peer_connection(implicated_cid, peer_cid)
                                .await
                                .is_some();

                            // TODO: send disconnect signal to the TCP connection
                        }
                        _ => {}
                    }
                }
            }
            NodeResult::ObjectTransferHandle(object_transfer_handle) => {
                let object_transfer_handler = object_transfer_handle.handle;
                let implicated_cid = object_transfer_handler.receiver;
                let peer_cid = object_transfer_handler.source;
                // TODO: Utilize metadata to store handler by object id
                let object_id: u32 = object_transfer_handler.source as u32;

                let mut server_connection_map = self.server_connection_map.lock().await;
                if let Some(connection) = server_connection_map.get_mut(&implicated_cid) {
                    connection.add_object_transfer_handler(
                        peer_cid,
                        object_id,
                        Some(object_transfer_handler),
                    );
                    let uuid = connection.associated_tcp_connection;
                    // TODO: add metadata to the request we send to TCP client
                    let response =
                        InternalServiceResponse::FileTransferRequest(FileTransferRequest {
                            cid: implicated_cid,
                            peer_cid,
                            metadata: VirtualObjectMetadata {
                                name: "".to_string(),
                                date_created: "".to_string(),
                                author: "".to_string(),
                                plaintext_length: 0,
                                group_count: 0,
                                object_id,
                                cid: 0,
                                transfer_type: TransferType::FileTransfer,
                            },
                        });
                    send_response_to_tcp_client(&self.tcp_connection_map, response, uuid).await;
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
                    let _did_remove = self
                        .clear_peer_connection(implicated_cid, peer_cid)
                        .await
                        .is_some();

                    let server_conn_map = self.server_connection_map.clone();
                    let lock = server_conn_map.lock().await;
                    if let Some(my_uuid) = lock.get(&implicated_cid) {
                        let uuid = my_uuid.associated_tcp_connection;
                        let response = InternalServiceResponse::Disconnected(Disconnected {
                            cid: implicated_cid,
                            peer_cid: Some(peer_cid),
                        });
                        send_response_to_tcp_client(&self.tcp_connection_map, response, uuid).await;
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
    sender: &UnboundedSender<InternalServicePayload>,
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
    to_kernel: UnboundedSender<InternalServicePayload>,
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
