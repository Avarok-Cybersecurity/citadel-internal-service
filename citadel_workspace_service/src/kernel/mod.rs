use bytes::Bytes;
use citadel_logging::{error, info, warn};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::VirtualTargetType;
use citadel_sdk::prelude::*;
use citadel_workspace_lib::{deserialize, serialize_payload, wrap_tcp_conn};
use citadel_workspace_types::{
    Disconnected, InternalServicePayload, InternalServiceResponse, ServiceConnectionAccepted,
};
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
    c2s_file_transfer_handlers: HashMap<u32, ObjectTransferHandler>,
}

#[allow(dead_code)]
struct PeerConnection {
    sink: PeerChannelSendHalf,
    remote: SymmetricIdentifierHandle,
    handler_map: HashMap<u32, ObjectTransferHandler>,
}

impl PeerConnection {
    fn add_handler(&mut self, key: u32, handler: ObjectTransferHandler) {
        self.handler_map.insert(key, handler);
    }
    /*fn remove_handler(&mut self, key: u32) {
        self.handler_map.remove(&key);
    }*/
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
        handler: ObjectTransferHandler,
    ) {
        if self.implicated_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.insert(object_id, handler);
        } else {
            // P2P
            if let Some(peer_connection) = self.peers.get_mut(&peer_cid) {
                peer_connection.add_handler(object_id, handler);
            }
        }
    }

    /*fn remove_object_transfer_handler(&mut self, peer_cid: u64, object_id: u32) {
        if self.implicated_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.remove(&object_id);
        } else {
            // P2P
            if let Some(peer_connection) = self.peers.get_mut(&peer_cid) {
                peer_connection.remove_handler(object_id);
            }
        }
    }*/

    fn get_file_transfer_handle(
        &mut self,
        peer_cid: u64,
        object_id: u32,
    ) -> Option<&mut ObjectTransferHandler> {
        if self.implicated_cid() == peer_cid {
            // C2S
            self.c2s_file_transfer_handlers.get_mut(&object_id)
        } else {
            // P2P
            let peer_connection = self.peers.get_mut(&peer_cid)?;
            peer_connection.handler_map.get_mut(&object_id)
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

        citadel_logging::warn!(target: "citadel", "Shutting down service because a critical task finished. {res:?}");
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
                        object_transfer_handler,
                    );
                    let uuid = connection.associated_tcp_connection;
                    // TODO: add metadata to the request we send to TCP client
                    let response = InternalServiceResponse::FileTransferRequest {
                        cid: implicated_cid,
                        peer_cid,
                    };
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

pub fn wrap_tcp_conn(conn: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    LengthDelimitedCodec::builder()
        .length_field_offset(0) // default value
        .max_frame_length(1024 * 1024 * 64) // 64 MB
        .length_field_type::<u32>()
        .length_adjustment(0) // default value
        .new_framed(conn)
}

fn serialize_payload(payload: &InternalServiceResponse) -> Vec<u8> {
    bincode2::serialize(&payload).unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use core::panic;
    use futures::stream::SplitSink;
    use std::error::Error;
    use std::future::Future;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::UnboundedReceiver;

    fn spawn_services<F1, F2>(internal_service_a: F1, internal_service_b: F2)
    where
        F1: Future + Send + 'static,
        F2: Future + Send + 'static,
        F1::Output: Send + 'static,
        F2::Output: Send + 'static,
    {
        let internal_services = async move {
            tokio::select! {
                _res0 = internal_service_a => (),
                _res1 = internal_service_b => (),
            }

            // citadel_logging::error!(target: "citadel", "Internal service error: vital service ended");
            // std::process::exit(1);
        };

        tokio::task::spawn(internal_services);
    }

    async fn send(
        sink: &mut SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
        command: InternalServicePayload,
    ) -> Result<(), Box<dyn Error>> {
        let command = bincode2::serialize(&command)?;
        sink.send(command.into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_register_connect() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");

        let internal_service_kernel = CitadelWorkspaceService::new(bind_address_internal_service);
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let (to_service, mut from_service, uuid, cid) = register_and_connect_to_server(
            bind_address_internal_service,
            server_bind_address,
            "John Doe",
            "john.doe",
            "secret",
        )
        .await
        .unwrap();
        let disconnect_command = InternalServicePayload::Disconnect { uuid, cid };
        to_service.send(disconnect_command).unwrap();
        let disconnect_response = from_service.recv().await.unwrap();

        assert!(matches!(
            disconnect_response,
            InternalServiceResponse::Disconnected { .. }
        ));

        Ok(())
    }

    async fn register_and_connect_to_server<
        T: Into<String>,
        R: Into<String>,
        S: Into<SecBuffer>,
    >(
        internal_service_addr: SocketAddr,
        server_addr: SocketAddr,
        full_name: T,
        username: R,
        password: S,
    ) -> Result<
        (
            UnboundedSender<InternalServicePayload>,
            UnboundedReceiver<InternalServiceResponse>,
            Uuid,
            u64,
        ),
        Box<dyn Error>,
    > {
        let conn = TcpStream::connect(internal_service_addr).await?;
        info!(target: "citadel", "connected to the TCP stream");
        let framed = wrap_tcp_conn(conn);
        info!(target: "citadel", "wrapped tcp connection");

        let (mut sink, mut stream) = framed.split();

        let first_packet = stream.next().await.unwrap()?;
        info!(target: "citadel", "First packet");
        let greeter_packet: InternalServiceResponse = bincode2::deserialize(&*first_packet)?;

        info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        let username = username.into();
        let full_name = full_name.into();
        let password = password.into();

        if let InternalServiceResponse::ServiceConnectionAccepted { id } = greeter_packet {
            let register_command = InternalServicePayload::Register {
                uuid: id,
                server_addr,
                full_name,
                username: username.clone(),
                proposed_password: password.clone(),
                default_security_settings: Default::default(),
                connect_after_register: false,
            };
            send(&mut sink, register_command).await?;

            let second_packet = stream.next().await.unwrap()?;
            let response_packet: InternalServiceResponse = bincode2::deserialize(&*second_packet)?;
            if let InternalServiceResponse::RegisterSuccess { id } = response_packet {
                // now, connect to the server
                let command = InternalServicePayload::Connect {
                    username,
                    password,
                    connect_mode: Default::default(),
                    udp_mode: Default::default(),
                    keep_alive_timeout: None,
                    uuid: id,
                    session_security_settings: Default::default(),
                };

                send(&mut sink, command).await?;

                let next_packet = stream.next().await.unwrap()?;
                let response_packet: InternalServiceResponse =
                    bincode2::deserialize(&*next_packet)?;
                if let InternalServiceResponse::ConnectSuccess { cid } = response_packet {
                    let (to_service, from_service) = tokio::sync::mpsc::unbounded_channel();
                    let service_to_test = async move {
                        // take messages from the service and send them to from_service
                        while let Some(msg) = stream.next().await {
                            let msg = msg.unwrap();
                            let msg_deserialized: InternalServiceResponse =
                                bincode2::deserialize(&*msg).unwrap();
                            info!(target = "citadel", "Service to test {:?}", msg_deserialized);
                            to_service.send(msg_deserialized).unwrap();
                        }
                    };

                    let (to_service_sender, mut from_test) = tokio::sync::mpsc::unbounded_channel();
                    let test_to_service = async move {
                        while let Some(msg) = from_test.recv().await {
                            info!(target = "citadel", "Test to service {:?}", msg);
                            send(&mut sink, msg).await.unwrap();
                        }
                    };

                    spawn_services(service_to_test, test_to_service);

                    return Ok((to_service_sender, from_service, id, cid));
                } else {
                    Err(generic_error("Connection to server was not a success"))
                }
            } else {
                Err(generic_error("Registration to server was not a success"))
            }
        } else {
            Err(generic_error("Wrong packet type"))
        }
    }

    fn generic_error<T: ToString>(msg: T) -> Box<dyn Error> {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            msg.to_string(),
        ))
    }

    // test
    #[tokio::test]
    async fn message_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55518".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info_reactive(
            |conn, _remote| async move {
                let (sink, mut stream) = conn.channel.split();

                while let Some(_message) = stream.next().await {
                    let send_message = "pong".into();
                    sink.send_message(send_message).await.unwrap();
                    info!("MessageSent");
                    ()
                }
                Ok(())
            },
            |_| (),
        );

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel = CitadelWorkspaceService::new(bind_address_internal_service);
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let (to_service, mut from_service, uuid, cid) = register_and_connect_to_server(
            bind_address_internal_service,
            server_bind_address,
            "John Doe",
            "john.doe",
            "secret",
        )
        .await
        .unwrap();

        let serialized_message = bincode2::serialize("Message Test").unwrap();
        let message_command = InternalServicePayload::Message {
            uuid,
            message: serialized_message,
            cid,
            peer_cid: None,
            security_level: SecurityLevel::Standard,
        };
        to_service.send(message_command).unwrap();
        let deserialized_message_response = from_service.recv().await.unwrap();
        info!(target: "citadel","{deserialized_message_response:?}");

        if let InternalServiceResponse::MessageSent { cid, .. } = deserialized_message_response {
            info!(target:"citadel", "Message {cid}");
            let deserialized_message_response = from_service.recv().await.unwrap();
            if let InternalServiceResponse::MessageReceived {
                message,
                cid,
                peer_cid: _,
            } = deserialized_message_response
            {
                println!("{message:?}");
                assert_eq!(SecBuffer::from("pong"), message);
                info!(target:"citadel", "Message sending success {cid}");
            } else {
                panic!("Message sending is not right");
            }
        } else {
            panic!("Message sending failed");
        }

        let disconnect_command = InternalServicePayload::Disconnect { uuid, cid };
        to_service.send(disconnect_command).unwrap();
        let disconnect_response = from_service.recv().await.unwrap();

        assert!(matches!(
            disconnect_response,
            InternalServiceResponse::Disconnected { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn connect_after_register_true() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55568".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info_reactive(
            |conn, _remote| async move {
                let (sink, mut stream) = conn.channel.split();

                while let Some(_message) = stream.next().await {
                    let send_message = "pong".into();
                    sink.send_message(send_message).await.unwrap();
                    info!("MessageSent");
                }
                Ok(())
            },
            |_| (),
        );

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel = CitadelWorkspaceService::new(bind_address_internal_service);
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        // begin mocking the GUI/CLI access
        let conn = TcpStream::connect(bind_address_internal_service).await?;
        info!(target: "citadel", "connected to the TCP stream");
        let framed = wrap_tcp_conn(conn);
        info!(target: "citadel", "wrapped tcp connection");

        let (mut sink, mut stream) = framed.split();

        let first_packet = stream.next().await.unwrap()?;
        info!(target: "citadel", "First packet");
        let greeter_packet: InternalServiceResponse = bincode2::deserialize(&*first_packet)?;

        info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        if let InternalServiceResponse::ServiceConnectionAccepted { id } = greeter_packet {
            let register_command = InternalServicePayload::Register {
                uuid: id,
                server_addr: server_bind_address,
                full_name: String::from("John"),
                username: String::from("john_doe"),
                proposed_password: String::from("test12345").into_bytes().into(),
                default_security_settings: Default::default(),
                connect_after_register: true,
            };
            send(&mut sink, register_command).await?;

            let second_packet = stream.next().await.unwrap()?;
            let response_packet: InternalServiceResponse = bincode2::deserialize(&*second_packet)?;

            if let InternalServiceResponse::ConnectSuccess { cid: _ } = response_packet {
                Ok(())
            } else {
                panic!("Registration to server was not a success")
            }
        } else {
            panic!("Wrong packet type");
        }
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55526".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55547".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel_a =
            CitadelWorkspaceService::new(bind_address_internal_service_a);
        let internal_service_a = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_a)
            .unwrap();

        let internal_service_kernel_b =
            CitadelWorkspaceService::new(bind_address_internal_service_b);

        let internal_service_b = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_b)
            .unwrap();

        spawn_services(internal_service_a, internal_service_b);

        // give time for both the server and internal service to run
        tokio::time::sleep(Duration::from_millis(2000)).await;
        info!(target: "citadel", "about to connect to internal service");
        let (to_service_a, mut from_service_a, uuid_a, cid_a) = register_and_connect_to_server(
            bind_address_internal_service_a,
            server_bind_address,
            "Peer A",
            "peer.a",
            "secret_a",
        )
        .await
        .unwrap();
        let (to_service_b, mut from_service_b, uuid_b, cid_b) = register_and_connect_to_server(
            bind_address_internal_service_b,
            server_bind_address,
            "Peer B",
            "peer.b",
            "secret_b",
        )
        .await
        .unwrap();

        // now, both peers are connected and registered to the central server. Now, we
        // need to have them peer-register to each other
        to_service_a
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_a,
                cid: cid_a,
                peer_id: cid_b.into(),
                connect_after_register: false,
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_b,
                cid: cid_b,
                peer_id: cid_a.into(),
                connect_after_register: false,
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();

        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_b);
                assert_eq!(peer_cid, cid_b);
                assert_eq!(username, "peer.a");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_a);
                assert_eq!(peer_cid, cid_a);
                assert_eq!(username, "peer.b");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        to_service_a
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_a,
                cid: cid_a,
                username: String::from("peer.a"),
                peer_cid: cid_b,
                peer_username: String::from("peer.b"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_b,
                cid: cid_b,
                username: String::from("peer.b"),
                peer_cid: cid_a,
                peer_username: String::from("peer.a"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_b);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_a);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_message_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel_a =
            CitadelWorkspaceService::new(bind_address_internal_service_a);
        let internal_service_a = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_a)
            .unwrap();

        let internal_service_kernel_b =
            CitadelWorkspaceService::new(bind_address_internal_service_b);

        let internal_service_b = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_b)
            .unwrap();

        spawn_services(internal_service_a, internal_service_b);

        // give time for both the server and internal service to run
        tokio::time::sleep(Duration::from_millis(2000)).await;
        info!(target: "citadel", "about to connect to internal service");
        let (to_service_a, mut from_service_a, uuid_a, cid_a) = register_and_connect_to_server(
            bind_address_internal_service_a,
            server_bind_address,
            "Peer A",
            "peer.a",
            "secret_a",
        )
        .await
        .unwrap();
        let (to_service_b, mut from_service_b, uuid_b, cid_b) = register_and_connect_to_server(
            bind_address_internal_service_b,
            server_bind_address,
            "Peer B",
            "peer.b",
            "secret_b",
        )
        .await
        .unwrap();

        // now, both peers are connected and registered to the central server. Now, we
        // need to have them peer-register to each other
        to_service_a
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_a,
                cid: cid_a,
                peer_id: cid_b.into(),
                connect_after_register: false,
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_b,
                cid: cid_b,
                peer_id: cid_a.into(),
                connect_after_register: false,
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();

        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_b);
                assert_eq!(peer_cid, cid_b);
                assert_eq!(username, "peer.a");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_a);
                assert_eq!(peer_cid, cid_a);
                assert_eq!(username, "peer.b");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        to_service_a
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_a,
                cid: cid_a,
                username: String::from("peer.a"),
                peer_cid: cid_b,
                peer_username: String::from("peer.b"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_b,
                cid: cid_b,
                username: String::from("peer.b"),
                peer_cid: cid_a,
                peer_username: String::from("peer.a"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_b);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_a);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let service_a_message = Vec::from("Hello World");
        let service_a_message_payload = InternalServicePayload::Message {
            uuid: uuid_a,
            message: service_a_message.clone(),
            cid: cid_a,
            peer_cid: Some(cid_b),
            security_level: Default::default(),
        };
        to_service_a.send(service_a_message_payload).unwrap();
        let deserialized_service_a_message_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_message_response:?}");

        if let InternalServiceResponse::MessageSent { cid: cid_b, .. } =
            &deserialized_service_a_message_response
        {
            info!(target:"citadel", "Message {cid_b}");
            let deserialized_service_a_message_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::MessageReceived {
                message,
                cid: cid_a,
                peer_cid: _cid_b,
            } = deserialized_service_a_message_response
            {
                assert_eq!(&*service_a_message, &*message);
                info!(target:"citadel", "Message sending success {cid_a}");
            } else {
                panic!("Message sending is not right");
            }
        } else {
            panic!("Message sending failed: {deserialized_service_a_message_response:?}");
        }

        Ok(())
    }

    // TODO: make this function register+connect to the server
    // Then, peer register and peer connect to each peer, returning
    // the handles at the end of this function
    // async fn connect_and_register_peers() {}

    pub struct ReceiverFileTransferKernel(pub Option<NodeRemote>, pub Arc<AtomicBool>);

    #[async_trait]
    impl NetKernel for ReceiverFileTransferKernel {
        fn load_remote(&mut self, node_remote: NodeRemote) -> Result<(), NetworkError> {
            self.0 = Some(node_remote);
            Ok(())
        }

        async fn on_start(&self) -> Result<(), NetworkError> {
            Ok(())
        }

        async fn on_node_event_received(&self, message: NodeResult) -> Result<(), NetworkError> {
            citadel_logging::trace!(target: "citadel", "SERVER received {:?}", message);
            if let NodeResult::ObjectTransferHandle(ObjectTransferHandle {
                ticket: _,
                mut handle,
            }) = map_errors(message)?
            {
                let mut path = None;
                // accept the transfer
                handle
                    .accept()
                    .map_err(|err| NetworkError::msg(err.into_string()))?;

                use futures::StreamExt;
                while let Some(status) = handle.next().await {
                    match status {
                        ObjectTransferStatus::ReceptionComplete => {
                            citadel_logging::trace!(target: "citadel", "Server has finished receiving the file!");
                            let cmp = include_bytes!("../../../resources/test.txt");
                            let streamed_data =
                                tokio::fs::read(path.clone().unwrap()).await.unwrap();
                            assert_eq!(
                                cmp,
                                streamed_data.as_slice(),
                                "Original data and streamed data does not match"
                            );

                            self.1.store(true, std::sync::atomic::Ordering::Relaxed);
                            self.0.clone().unwrap().shutdown().await?;
                        }

                        ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                            path = Some(file_path);
                            assert_eq!(vfm.get_target_name(), "test.txt")
                        }

                        _ => {}
                    }
                }
            }

            Ok(())
        }

        async fn on_stop(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }
    }

    pub fn server_info_file_transfer<'a>(
        switch: Arc<AtomicBool>,
    ) -> (NodeFuture<'a, ReceiverFileTransferKernel>, SocketAddr) {
        let port = citadel_sdk::test_common::get_unused_tcp_port();
        let bind_addr = SocketAddr::from_str(&format!("127.0.0.1:{port}")).unwrap();
        let server = citadel_sdk::test_common::server_test_node(
            bind_addr,
            ReceiverFileTransferKernel(None, switch),
            |_| {},
        );
        (server, bind_addr)
    }

    #[tokio::test]
    async fn standard_file_transfer_c2s_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55518".parse().unwrap();

        // TCP client (GUI, CLI) -> Internal Service -> Receiver File Transfer Kernel server
        let server_success = &Arc::new(AtomicBool::new(false));
        let (server, server_bind_address) = server_info_file_transfer(server_success.clone());

        tokio::task::spawn(server);

        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel = CitadelWorkspaceService::new(bind_address_internal_service);
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let (to_service, mut from_service, uuid, cid) = register_and_connect_to_server(
            bind_address_internal_service,
            server_bind_address,
            "John Doe",
            "john.doe",
            "secret",
        )
        .await
        .unwrap();

        let file_to_send = PathBuf::from("resources/test.txt");
        let file_transfer_command = InternalServicePayload::SendFileStandard {
            uuid,
            source: file_to_send,
            cid,
            peer_cid: None,
            chunk_size: None,
        };
        to_service.send(file_transfer_command).unwrap();
        let file_transfer_response = from_service.recv().await.unwrap();
        info!(target: "citadel","{file_transfer_response:?}");

        let disconnect_command = InternalServicePayload::Disconnect { uuid, cid };
        to_service.send(disconnect_command).unwrap();
        let _disconnect_response = from_service.recv().await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_standard_file_transfer(
    ) -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel_a =
            CitadelWorkspaceService::new(bind_address_internal_service_a);
        let internal_service_a = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_a)
            .unwrap();

        let internal_service_kernel_b =
            CitadelWorkspaceService::new(bind_address_internal_service_b);

        let internal_service_b = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel_b)
            .unwrap();

        spawn_services(internal_service_a, internal_service_b);

        // give time for both the server and internal service to run
        tokio::time::sleep(Duration::from_millis(2000)).await;
        info!(target: "citadel", "about to connect to internal service");
        let (to_service_a, mut from_service_a, uuid_a, cid_a) = register_and_connect_to_server(
            bind_address_internal_service_a,
            server_bind_address,
            "Peer A",
            "peer.a",
            "secret_a",
        )
        .await
        .unwrap();
        let (to_service_b, mut from_service_b, uuid_b, cid_b) = register_and_connect_to_server(
            bind_address_internal_service_b,
            server_bind_address,
            "Peer B",
            "peer.b",
            "secret_b",
        )
        .await
        .unwrap();

        // now, both peers are connected and registered to the central server. Now, we
        // need to have them peer-register to each other
        to_service_a
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_a,
                cid: cid_a,
                peer_id: cid_b.into(),
                connect_after_register: false,
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerRegister {
                uuid: uuid_b,
                cid: cid_b,
                peer_id: cid_a.into(),
                connect_after_register: false,
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();

        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_b);
                assert_eq!(peer_cid, cid_b);
                assert_eq!(username, "peer.a");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            } => {
                assert_eq!(cid, cid_a);
                assert_eq!(peer_cid, cid_a);
                assert_eq!(username, "peer.b");
            }
            _ => {
                panic!("Didn't get the PeerRegisterSuccess");
            }
        }

        to_service_a
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_a,
                cid: cid_a,
                username: String::from("peer.a"),
                peer_cid: cid_b,
                peer_username: String::from("peer.b"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        to_service_b
            .send(InternalServicePayload::PeerConnect {
                uuid: uuid_b,
                cid: cid_b,
                username: String::from("peer.b"),
                peer_cid: cid_a,
                peer_username: String::from("peer.a"),
                udp_mode: Default::default(),
                session_security_settings: Default::default(),
            })
            .unwrap();

        let item = from_service_b.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_b);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess { cid } => {
                assert_eq!(cid, cid_a);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let file_to_send = PathBuf::from("resources/test.txt");
        let send_file_to_service_b_payload = InternalServicePayload::SendFileStandard {
            uuid: uuid_a,
            source: file_to_send,
            cid: cid_a,
            peer_cid: Some(cid_b),
            chunk_size: None,
        };
        to_service_a.send(send_file_to_service_b_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::FileTransferStatus { .. } = &deserialized_service_a_payload_response
        {
            info!(target:"citadel", "File Transfer Request {cid_b}");
            let deserialized_service_a_payload_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferRequest { .. } =
                deserialized_service_a_payload_response
            {
                let file_transfer_accept_payload =
                    InternalServicePayload::RespondFileTransferStandard {
                        uuid: uuid_b,
                        cid: cid_b,
                        peer_cid: cid_a,
                        object_id: cid_a as u32,
                        accept: true,
                    };
                to_service_b.send(file_transfer_accept_payload).unwrap();
                info!(target:"citadel", "Accepted File Transfer {cid_b}");

                /*let mut service_b_lock = internal_service_kernel_b.server_connection_map.lock().await;
                let service_b_connection = service_b_lock.get_mut(&cid_b).unwrap();
                if let Some(service_b_handle) =
                    service_b_connection.get_file_transfer_handle(cid_a, cid_a as u32)
                {
                    let mut path = None;
                    while let Some(status) = service_b_handle.next().await {
                        match status {
                            ObjectTransferStatus::ReceptionComplete => {
                                citadel_logging::trace!(target: "citadel", "Server has finished receiving the file!");
                                let cmp = include_bytes!("../../../resources/test.txt");
                                let streamed_data =
                                    tokio::fs::read(path.clone().unwrap()).await.unwrap();
                                assert_eq!(
                                    cmp,
                                    streamed_data.as_slice(),
                                    "Original data and streamed data does not match"
                                );
                            }

                            ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                                path = Some(file_path);
                                assert_eq!(vfm.get_target_name(), "test.txt")
                            }

                            _ => {}
                        }
                    }
                } else {
                    panic!("File Transfer Failed on data stream");
                }*/

            } else {
                panic!("File Transfer P2P Failure");
            }
        } else {
            panic!("File Transfer Request failed: {deserialized_service_a_payload_response:?}");
        }

        Ok(())
    }
}
