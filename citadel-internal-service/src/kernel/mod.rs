use crate::kernel::ext::IOInterfaceExt;
use crate::kernel::requests::{handle_request, HandledRequestResult};
use citadel_internal_service_connector::connector::{
    InternalServiceConnector, WrappedSink, WrappedStream,
};
use citadel_internal_service_connector::io_interface::in_memory::{
    InMemoryInterface, InMemorySink, InMemoryStream,
};
use citadel_internal_service_connector::io_interface::tcp::TcpIOInterface;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::*;
use citadel_logging::{error, info, warn};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::remote_specialization::PeerRemote;
use citadel_sdk::prelude::VirtualTargetType;
use citadel_sdk::prelude::*;
use futures::stream::StreamExt;
use futures::{Sink, SinkExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use uuid::Uuid;

pub(crate) mod ext;
pub(crate) mod requests;
pub(crate) mod responses;

pub struct CitadelWorkspaceService<T> {
    pub remote: Option<NodeRemote>,
    pub server_connection_map: Arc<Mutex<HashMap<u64, Connection>>>,
    pub tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    io: Arc<Mutex<Option<T>>>,
}

impl<T> Clone for CitadelWorkspaceService<T> {
    fn clone(&self) -> Self {
        CitadelWorkspaceService {
            remote: self.remote.clone(),
            server_connection_map: self.server_connection_map.clone(),
            tcp_connection_map: self.tcp_connection_map.clone(),
            io: self.io.clone(),
        }
    }
}

impl<T: IOInterface> From<T> for CitadelWorkspaceService<T> {
    fn from(io: T) -> Self {
        CitadelWorkspaceService {
            remote: None,
            server_connection_map: Arc::new(Mutex::new(Default::default())),
            tcp_connection_map: Arc::new(Mutex::new(Default::default())),
            io: Arc::new(Mutex::new(Some(io))),
        }
    }
}

impl<T: IOInterface> CitadelWorkspaceService<T> {
    pub fn new(io: T) -> Self {
        io.into()
    }

    pub fn remote(&self) -> &NodeRemote {
        self.remote.as_ref().expect("Kernel not loaded")
    }
}

impl CitadelWorkspaceService<TcpIOInterface> {
    pub async fn new_tcp(
        bind_address: SocketAddr,
    ) -> std::io::Result<CitadelWorkspaceService<TcpIOInterface>> {
        Ok(TcpIOInterface::new(bind_address).await?.into())
    }
}

impl CitadelWorkspaceService<InMemoryInterface> {
    /// Generates an in-memory service connector and kernel. This is useful for programs that do not need
    /// networking to connect between the application and the internal service
    pub fn new_in_memory() -> (
        InternalServiceConnector<InMemoryInterface>,
        CitadelWorkspaceService<InMemoryInterface>,
    ) {
        let (tx_to_consumer, rx_from_consumer) = futures::channel::mpsc::unbounded();
        let (tx_to_svc, rx_from_svc) = futures::channel::mpsc::unbounded();
        let connector = InternalServiceConnector {
            sink: WrappedSink {
                inner: InMemorySink(tx_to_svc),
            },
            stream: WrappedStream {
                inner: InMemoryStream(rx_from_svc),
            },
        };
        let kernel = InMemoryInterface {
            sink: Some(tx_to_consumer),
            stream: Some(rx_from_consumer),
        }
        .into();
        (connector, kernel)
    }
}

#[allow(dead_code)]
pub struct Connection {
    pub sink_to_server: PeerChannelSendHalf,
    pub client_server_remote: ClientServerRemote,
    pub peers: HashMap<u64, PeerConnection>,
    pub(crate) associated_tcp_connection: Uuid,
    pub c2s_file_transfer_handlers: HashMap<u64, Option<ObjectTransferHandler>>,
    pub groups: HashMap<MessageGroupKey, GroupConnection>,
}

#[allow(dead_code)]
pub struct PeerConnection {
    sink: PeerChannelSendHalf,
    remote: PeerRemote,
    handler_map: HashMap<u64, Option<ObjectTransferHandler>>,
    associated_tcp_connection: Uuid,
}

#[allow(dead_code)]
pub struct GroupConnection {
    key: MessageGroupKey,
    tx: GroupChannelSendHalf,
    cid: u64,
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
            groups: HashMap::new(),
        }
    }

    fn add_peer_connection(
        &mut self,
        peer_cid: u64,
        sink: PeerChannelSendHalf,
        remote: PeerRemote,
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

    pub fn add_group_channel(
        &mut self,
        group_key: MessageGroupKey,
        group_channel: GroupConnection,
    ) {
        self.groups.insert(group_key, group_channel);
    }

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

impl<T: IOInterface> CitadelWorkspaceService<T> {
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
impl<T: IOInterface> NetKernel for CitadelWorkspaceService<T> {
    fn load_remote(&mut self, node_remote: NodeRemote) -> Result<(), NetworkError> {
        self.remote = Some(node_remote);
        Ok(())
    }

    async fn on_start(&self) -> Result<(), NetworkError> {
        let this = self.clone();
        let remote = self.remote.clone().unwrap();
        let remote_for_closure = remote.clone();
        let mut io = self.io.lock().await.take().expect("Already called");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let tcp_connection_map = &self.tcp_connection_map;
        let server_connection_map = &self.server_connection_map;

        let listener_task = async move {
            while let Some((sink, stream)) = io.next_connection().await {
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServiceResponse>();
                let id = Uuid::new_v4();
                tcp_connection_map.lock().await.insert(id, tx1);
                io.spawn_connection_handler(
                    sink,
                    stream,
                    tx.clone(),
                    rx1,
                    id,
                    tcp_connection_map.clone(),
                    server_connection_map.clone(),
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
                        if let Err(err) =
                            send_response_to_tcp_client(&this.tcp_connection_map, response, uuid)
                                .await
                        {
                            // The TCP connection no longer exists. Delete it from both maps
                            error!(target: "citadel", "Failed to send response to TCP client: {err:?}");
                            this.tcp_connection_map.lock().await.remove(&uuid);
                            this.server_connection_map
                                .lock()
                                .await
                                .retain(|_, v| v.associated_tcp_connection != uuid);
                        }
                    }
                };

                // Spawn the task to allow for parallel request handling
                tokio::task::spawn(task);
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
        responses::handle_node_result(self, message).await
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}

async fn send_response_to_tcp_client(
    hash_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    response: InternalServiceResponse,
    uuid: Uuid,
) -> Result<(), NetworkError> {
    hash_map
        .lock()
        .await
        .get(&uuid)
        .ok_or_else(|| NetworkError::Generic(format!("TCP connection not found: {:?}", uuid)))?
        .send(response)
        .map_err(|err| {
            NetworkError::Generic(format!("Failed to send response to TCP client: {err:?}"))
        })
}

fn create_client_server_remote(
    conn_type: VirtualTargetType,
    remote: NodeRemote,
    security_settings: SessionSecuritySettings,
) -> ClientServerRemote {
    ClientServerRemote::new(conn_type, remote, security_settings)
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

fn spawn_tick_updater(
    object_transfer_handler: ObjectTransferHandler,
    implicated_cid: u64,
    peer_cid: Option<u64>,
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
                        let message = InternalServiceResponse::FileTransferTickNotification(
                            FileTransferTickNotification {
                                cid: implicated_cid,
                                peer_cid,
                                status: status_message,
                                request_id: None,
                            },
                        );
                        match entry.send(message.clone()) {
                            Ok(_res) => {
                                info!(target: "citadel", "File Transfer Status Tick Sent {status:?}");
                            }
                            Err(err) => {
                                warn!(target: "citadel", "File Transfer Status Tick Not Sent: {err:?}");
                            }
                        }

                        if matches!(
                            status,
                            ObjectTransferStatus::TransferComplete { .. }
                                | ObjectTransferStatus::ReceptionComplete
                        ) {
                            info!(target: "citadel", "File Transfer Completed - Ending Tick Updater");
                            break;
                        }
                    }
                    None => {
                        warn!(target:"citadel","Connection not found during File Transfer Status Tick")
                    }
                }
            }
            info!(target:"citadel", "Spawned Tick Updater has ended for {implicated_cid:?}");
        };
        tokio::task::spawn(sender_status_updater);
    } else {
        info!(target: "citadel", "Server Connection Not Found")
    }
}
