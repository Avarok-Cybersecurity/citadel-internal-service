#![allow(dead_code)]
use citadel_internal_service::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::connector::{InternalServiceConnector, WrappedSink};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    FileTransferTickNotification, InternalServiceRequest, InternalServiceResponse,
    PeerConnectNotification, PeerConnectSuccess, PeerRegisterNotification, PeerRegisterSuccess,
};
use citadel_logging::info;
use citadel_sdk::prefabs::server::client_connect_listener::ClientConnectListenerKernel;
use citadel_sdk::prefabs::server::empty::EmptyKernel;
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::*;
use core::panic;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub fn setup_log() {
    citadel_logging::setup_log();
    std::panic::set_hook(Box::new(|info| {
        citadel_logging::error!(target: "citadel", "Panic: {:?}", info);
        std::process::exit(1);
    }));
}

pub struct RegisterAndConnectItems<T: Into<String>, S: Into<SecBuffer>, R: Into<PreSharedKey>> {
    pub internal_service_addr: SocketAddr,
    pub server_addr: SocketAddr,
    pub full_name: T,
    pub username: T,
    pub password: S,
    pub pre_shared_key: Option<R>,
}

pub type InternalServicesFutures =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + 'static>>;

pub type PeerReturnHandle = (
    UnboundedSender<InternalServiceRequest>,
    UnboundedReceiver<InternalServiceResponse>,
    u64,
);

pub trait PeerServiceHandles {
    fn take_next_service_handle(&mut self) -> PeerReturnHandle;
}

impl PeerServiceHandles for Vec<PeerReturnHandle> {
    fn take_next_service_handle(&mut self) -> PeerReturnHandle {
        self.remove(0)
    }
}

pub fn generic_error<T: ToString>(msg: T) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.to_string(),
    ))
}

pub async fn register_and_connect_to_server<
    T: Into<String>,
    S: Into<SecBuffer>,
    R: Into<PreSharedKey>,
>(
    services_to_create: Vec<RegisterAndConnectItems<T, S, R>>,
) -> Result<
    Vec<(
        UnboundedSender<InternalServiceRequest>,
        UnboundedReceiver<InternalServiceResponse>,
        u64,
    )>,
    Box<dyn Error>,
> {
    info!(target = "citadel", "Registering and Connecting To Server");
    let mut return_results: Vec<(
        UnboundedSender<InternalServiceRequest>,
        UnboundedReceiver<InternalServiceResponse>,
        u64,
    )> = Vec::new();

    for item in services_to_create {
        let (mut sink, mut stream) = InternalServiceConnector::connect(item.internal_service_addr)
            .await?
            .split();

        let username = item.username.into();
        let full_name = item.full_name.into();
        let password = item.password.into();
        let server_password = item.pre_shared_key;
        let session_security_settings = SessionSecuritySettingsBuilder::default().build().unwrap();

        info!(target = "citadel", "Sending Register Request");
        let register_command = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: item.server_addr,
            full_name,
            username: username.clone(),
            proposed_password: password.clone(),
            session_security_settings,
            connect_after_register: false,
            server_password: server_password.clone(),
        };
        send(&mut sink, register_command).await.unwrap();

        let response_packet = stream.next().await.unwrap();

        if let InternalServiceResponse::RegisterSuccess(
            citadel_internal_service_types::RegisterSuccess { request_id: _ },
        ) = response_packet
        {
            info!(
                target = "citadel",
                "RegisterSuccess Received, Now Connecting"
            );
            // now, connect to the server
            let command = InternalServiceRequest::Connect {
                username,
                password,
                connect_mode: Default::default(),
                udp_mode: Default::default(),
                keep_alive_timeout: None,
                session_security_settings,
                request_id: Uuid::new_v4(),
                server_password,
            };

            send(&mut sink, command).await.unwrap();

            let response_packet = stream.next().await.unwrap();
            if let InternalServiceResponse::ConnectSuccess(
                citadel_internal_service_types::ConnectSuccess { cid, request_id: _ },
            ) = response_packet
            {
                info!(
                    target = "citadel",
                    "ConnectSuccess Received, Creating Service Channels"
                );
                let (to_service, from_service) = tokio::sync::mpsc::unbounded_channel();
                let service_to_test = async move {
                    // take messages from the service and send them to from_service
                    while let Some(msg) = stream.next().await {
                        info!(target = "citadel", "Service to test {msg:?}");
                        to_service.send(msg).unwrap();
                    }
                };

                let (to_service_sender, mut from_test) = tokio::sync::mpsc::unbounded_channel();
                let test_to_service = async move {
                    while let Some(msg) = from_test.recv().await {
                        info!(target = "citadel", "Test to service {:?}", msg);
                        send(&mut sink, msg).await.unwrap();
                    }
                };

                let mut internal_services: Vec<InternalServicesFutures> = Vec::new();
                internal_services.push(Box::pin(async move {
                    test_to_service.await;
                    Ok(())
                }));
                internal_services.push(Box::pin(async move {
                    service_to_test.await;
                    Ok(())
                }));
                spawn_services(internal_services);
                return_results.push((to_service_sender, from_service, cid));
            } else {
                panic!("Connection to server was not a success");
            }
        } else {
            panic!("Registration to server was not a success");
        }
    }
    Ok(return_results)
}

pub async fn register_and_connect_to_server_then_peers(
    int_svc_addrs: Vec<SocketAddr>,
    server_session_password: Option<PreSharedKey>,
    peer_session_password: Option<PreSharedKey>,
) -> Result<Vec<PeerReturnHandle>, Box<dyn Error>> {
    // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
    let (server, server_bind_address) =
        if let Some(server_session_password) = server_session_password {
            server_info_skip_cert_verification_with_password(server_session_password.clone())
        } else {
            server_info_skip_cert_verification()
        };
    tokio::task::spawn(server);
    let mut internal_services: Vec<InternalServicesFutures> = Vec::new();

    // Spawn Internal Services with given addresses
    for int_svc_addr_iter in int_svc_addrs.clone() {
        let bind_address_internal_service = int_svc_addr_iter;

        info!(target: "citadel", "Internal Service Spawning");
        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)
            .unwrap();

        // Add NodeFuture for Internal Service to Vector to be spawned
        internal_services.push(Box::pin(async move {
            match internal_service.await {
                Err(err) => Err(Box::from(err)),
                _ => Ok(()),
            }
        }));
    }
    spawn_services(internal_services);

    // Give time for both the Server and Internal Service to run
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Set Info for Vector of Peers
    let mut to_spawn: Vec<RegisterAndConnectItems<String, Vec<u8>, PreSharedKey>> = Vec::new();
    for (peer_number, int_svc_addr_iter) in int_svc_addrs.clone().iter().enumerate() {
        let bind_address_internal_service = *int_svc_addr_iter;
        to_spawn.push(RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service,
            server_addr: server_bind_address,
            full_name: format!("Peer {}", peer_number),
            username: format!("peer.{}", peer_number),
            password: format!("secret_{}", peer_number).into_bytes().to_owned(),
            pre_shared_key: server_session_password.clone(),
        });
    }

    // Registers and Connects all peers to Server
    let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();

    info!(
        target = "citadel",
        "Starting Registration and Connection between peers"
    );
    // Registers and Connects all peers to Each Other Peer
    for service_index in 0..returned_service_info.len() {
        let (item, neighbor_items) = {
            let (_, second) = returned_service_info.split_at_mut(service_index);
            let (element, remainder) = second.split_at_mut(1);
            (&mut element[0], remainder)
        };

        let (ref mut to_service_a, ref mut from_service_a, cid_a) = item;
        for neighbor in neighbor_items {
            let (ref mut to_service_b, ref mut from_service_b, cid_b) = neighbor;
            let session_security_settings =
                SessionSecuritySettingsBuilder::default().build().unwrap();
            register_p2p(
                to_service_a,
                from_service_a,
                *cid_a,
                to_service_b,
                from_service_b,
                *cid_b,
                session_security_settings,
                Some(peer_session_password.clone()),
            )
            .await?;

            connect_p2p(
                to_service_a,
                from_service_a,
                *cid_a,
                to_service_b,
                from_service_b,
                *cid_b,
                session_security_settings,
                Some(peer_session_password.clone()),
            )
            .await?;
        }
    }
    Ok(returned_service_info)
}

pub async fn register_p2p(
    to_service_a: &mut UnboundedSender<InternalServiceRequest>,
    from_service_a: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_a: u64,
    to_service_b: &mut UnboundedSender<InternalServiceRequest>,
    from_service_b: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_b: u64,
    session_security_settings: SessionSecuritySettings,
    session_password: Some(PreSharedKey),
) -> Result<(), Box<dyn Error>> {
    // Service A Requests to Register with Service B
    to_service_a
        .send(InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid_a,
            peer_cid: cid_b,
            session_security_settings,
            connect_after_register: false,
            peer_session_password: Some(session_password.clone()),
        })
        .unwrap();

    // Service B receives Register Request from Service A
    let inbound_response = from_service_b.recv().await.unwrap();
    match inbound_response {
        InternalServiceResponse::PeerRegisterNotification(PeerRegisterNotification {
            cid,
            peer_cid,
            peer_username: _,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_b);
            assert_eq!(peer_cid, cid_a);
        }
        _ => {
            panic!(
                "Peer B didn't get the PeerRegisterNotification, instead got {inbound_response:?}"
            );
        }
    }

    // Service B Sends Register Request to Accept
    to_service_b
        .send(InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid_b,
            peer_cid: cid_a,
            session_security_settings,
            connect_after_register: false,
            peer_session_password: Some(session_password),
        })
        .unwrap();

    // Receive Register Success Responses
    let resp = from_service_a.recv().await.unwrap();
    let InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess { cid, peer_cid, .. }) =
        resp
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_a);
    assert_eq!(peer_cid, cid_b);

    let resp = from_service_b.recv().await.unwrap();
    let InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess { cid, peer_cid, .. }) =
        resp
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_b);
    assert_eq!(peer_cid, cid_a);

    Ok(())
}

pub async fn connect_p2p(
    to_service_a: &mut UnboundedSender<InternalServiceRequest>,
    from_service_a: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_a: u64,
    to_service_b: &mut UnboundedSender<InternalServiceRequest>,
    from_service_b: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_b: u64,
    session_security_settings: SessionSecuritySettings,
    session_password: Some(PreSharedKey),
) -> Result<(), Box<dyn Error>> {
    // Service A Requests To Connect
    to_service_a
        .send(InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: cid_a,
            peer_cid: cid_b,
            udp_mode: Default::default(),
            session_security_settings,
            peer_session_password: Some(session_password.clone()),
        })
        .unwrap();

    // Service B Receives Connect Request from Service A
    let inbound_response = from_service_b.recv().await.unwrap();
    match inbound_response {
        InternalServiceResponse::PeerConnectNotification(PeerConnectNotification {
            cid,
            peer_cid,
            session_security_settings: _,
            udp_mode: _,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_b);
            assert_eq!(peer_cid, cid_a);
        }
        _ => {
            panic!("Peer B didn't get the PeerConnectNotification");
        }
    }

    // Service B Sends Connect Request to Accept
    to_service_b
        .send(InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: cid_b,
            peer_cid: cid_a,
            udp_mode: Default::default(),
            session_security_settings,
            peer_session_password: Some(session_password),
        })
        .unwrap();

    // Receive Connect Success Responses
    let signal = from_service_a.recv().await.unwrap();
    let InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, peer_cid, .. }) =
        signal
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_a);
    assert_eq!(peer_cid, cid_b);
    let signal = from_service_b.recv().await.unwrap();
    let InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, peer_cid, .. }) =
        signal
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_b);
    assert_eq!(peer_cid, cid_a);

    Ok(())
}

pub fn spawn_services(futures_to_spawn: Vec<InternalServicesFutures>) {
    let services_to_spawn = async move {
        let (returned_future, _, _) = futures::future::select_all(futures_to_spawn).await;
        match returned_future {
            Ok(_) => {
                info!(target: "citadel","Vital Internal Service Ended");
            }
            Err(err) => {
                citadel_logging::error!(target: "citadel", "Internal service error: {err:?}");
            }
        }
    };
    tokio::task::spawn(services_to_spawn);
}

pub async fn send<T: IOInterface>(
    sink: &mut WrappedSink<T>,
    command: InternalServiceRequest,
) -> Result<(), Box<dyn Error>> {
    sink.send(command).await?;
    Ok(())
}

pub fn server_test_node_skip_cert_verification<'a, K: NetKernel + 'a>(
    kernel: K,
    opts: impl FnOnce(&mut NodeBuilder),
) -> (NodeFuture<'a, K>, SocketAddr) {
    let mut builder = NodeBuilder::default();
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind_addr = tcp_listener.local_addr().unwrap();
    let builder = builder
        .with_node_type(NodeType::Server(bind_addr))
        .with_insecure_skip_cert_verification()
        .with_underlying_protocol(
            ServerUnderlyingProtocol::from_tcp_listener(tcp_listener).unwrap(),
        );

    (opts)(builder);

    (builder.build(kernel).unwrap(), bind_addr)
}

pub fn server_test_node_skip_cert_verification_with_password<'a, K: NetKernel + 'a>(
    kernel: K,
    server_password: PreSharedKey,
    opts: impl FnOnce(&mut NodeBuilder),
) -> (NodeFuture<'a, K>, SocketAddr) {
    let mut builder = NodeBuilder::default();
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind_addr = tcp_listener.local_addr().unwrap();
    let builder = builder
        .with_node_type(NodeType::Server(bind_addr))
        .with_server_password(server_password)
        .with_insecure_skip_cert_verification()
        .with_underlying_protocol(
            ServerUnderlyingProtocol::from_tcp_listener(tcp_listener).unwrap(),
        );

    (opts)(builder);

    (builder.build(kernel).unwrap(), bind_addr)
}

pub fn server_info_skip_cert_verification<'a>() -> (NodeFuture<'a, EmptyKernel>, SocketAddr) {
    server_test_node_skip_cert_verification(EmptyKernel, |_| {})
}

pub fn server_info_skip_cert_verification_with_password<'a>(
    server_password: PreSharedKey,
) -> (NodeFuture<'a, EmptyKernel>, SocketAddr) {
    server_test_node_skip_cert_verification_with_password(EmptyKernel, server_password, |_| {})
}

pub fn server_info_reactive_skip_cert_verification<'a, F: 'a, Fut: 'a>(
    f: F,
    opts: impl FnOnce(&mut NodeBuilder),
) -> (NodeFuture<'a, Box<dyn NetKernel + 'a>>, SocketAddr)
where
    F: Fn(ConnectionSuccess, ClientServerRemote) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), NetworkError>> + Send + Sync,
{
    server_test_node_skip_cert_verification(
        Box::new(ClientConnectListenerKernel::new(f)) as Box<dyn NetKernel>,
        opts,
    )
}

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
        if let NodeResult::ObjectTransferHandle(object_transfer_handle) = message {
            let mut handle = object_transfer_handle.handle;
            let mut path = None;
            let mut is_revfs = false;
            // Automatically accept the transfer
            handle.accept().unwrap();

            use futures::StreamExt;
            while let Some(status) = handle.next().await {
                match status {
                    ObjectTransferStatus::ReceptionComplete => {
                        citadel_logging::trace!(target: "citadel", "Server has finished receiving the file!");
                        let mut cmp_path = PathBuf::from("..");
                        cmp_path.push("resources");
                        cmp_path.push("test");
                        cmp_path.set_extension("txt");
                        let cmp_data = tokio::fs::read(cmp_path).await.unwrap();
                        let streamed_data = tokio::fs::read(path.clone().unwrap()).await.unwrap();
                        if is_revfs {
                            assert_ne!(
                                cmp_data.as_slice(),
                                streamed_data.as_slice(),
                                "Original data and streamed data match - Should not match"
                            );
                        } else {
                            assert_eq!(
                                cmp_data.as_slice(),
                                streamed_data.as_slice(),
                                "Original data and streamed data do not match"
                            );
                        }
                    }
                    ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                        is_revfs = matches!(
                            vfm.transfer_type,
                            TransferType::RemoteEncryptedVirtualFilesystem { .. }
                        );
                        path = Some(file_path);
                        assert_eq!(vfm.name, "test.txt")
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
    let (server, bind_addr) =
        server_test_node_skip_cert_verification(ReceiverFileTransferKernel(None, switch), |_| {});
    (server, bind_addr)
}

pub async fn exhaust_stream_to_file_completion(
    cmp_path: PathBuf,
    svc: &mut UnboundedReceiver<InternalServiceResponse>,
) {
    // Exhaust the stream for the receiver
    let mut path = None;
    let mut is_revfs = false;
    let cmp_file_name = cmp_path
        .file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap();
    loop {
        let tick_response = svc.recv().await.unwrap();
        match tick_response {
            InternalServiceResponse::FileTransferTickNotification(
                FileTransferTickNotification {
                    cid: _,
                    peer_cid: _,
                    status,
                },
            ) => match status {
                ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                    path = Some(file_path);
                    is_revfs = matches!(
                        vfm.transfer_type,
                        TransferType::RemoteEncryptedVirtualFilesystem { .. }
                    );
                    info!(target: "citadel", "File Transfer (Receiving) Beginning");
                    assert_eq!(vfm.name, cmp_file_name)
                }
                ObjectTransferStatus::ReceptionTick(..) => {
                    info!(target: "citadel", "File Transfer (Receiving) Tick");
                }
                ObjectTransferStatus::ReceptionComplete => {
                    info!(target: "citadel", "File Transfer (Receiving) Completed");
                    let cmp_data = tokio::fs::read(cmp_path.clone()).await.unwrap();
                    let streamed_data = tokio::fs::read(
                        path.clone()
                            .expect("Never received the ReceptionBeginning tick!"),
                    )
                    .await
                    .unwrap();
                    if is_revfs {
                        // The locally stored contents should NEVER be the same as the plaintext for REVFS
                        assert_ne!(
                            cmp_data.as_slice(),
                            streamed_data.as_slice(),
                            "Original data and streamed data does not match"
                        );
                    } else {
                        assert_eq!(
                            cmp_data.as_slice(),
                            streamed_data.as_slice(),
                            "Original data and streamed data does not match"
                        );
                    }

                    return;
                }
                ObjectTransferStatus::TransferComplete => {
                    info!(target: "citadel", "File Transfer (Sending) Completed");
                    return;
                }
                ObjectTransferStatus::TransferBeginning => {
                    info!(target: "citadel", "File Transfer (Sending) Beginning");
                }
                ObjectTransferStatus::TransferTick(..) => {}
                _ => {
                    panic!("File Send Reception Status Yielded Unexpected Response")
                }
            },
            unexpected_response => {
                citadel_logging::warn!(target: "citadel", "Unexpected signal {unexpected_response:?}")
            }
        }
    }
}

pub async fn test_kv_for_service(
    to_service: &UnboundedSender<InternalServiceRequest>,
    from_service: &mut UnboundedReceiver<InternalServiceResponse>,
    cid: u64,
    peer_cid: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    // test get_all_kv
    to_service.send(InternalServiceRequest::LocalDBGetAllKV {
        cid,
        peer_cid,
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetAllKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(resp.map.len(), 0);
        assert_eq!(peer_cid, resp.peer_cid);
    } else {
        panic!("Didn't get the LocalDBGetAllKVSuccess");
    }

    // test set_kv
    let value = Vec::from("Hello, World!");
    to_service.send(InternalServiceRequest::LocalDBSetKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        value: value.clone(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBSetKVSuccess(resp) = from_service.recv().await.unwrap() {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
    } else {
        panic!("Didn't get the LocalDBSetKVSuccess");
    }

    // test get_kv
    to_service.send(InternalServiceRequest::LocalDBGetKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetKVSuccess(resp) = from_service.recv().await.unwrap() {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
        assert_eq!(&resp.value, &value);
    } else {
        panic!("Didn't get the LocalDBGetKVSuccess");
    }

    // test get_all_kv
    to_service.send(InternalServiceRequest::LocalDBGetAllKV {
        cid,
        peer_cid,
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetAllKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(resp.map.len(), 1);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(
            resp.map,
            HashMap::from([("tmp".to_string(), value.clone())])
        );
    } else {
        panic!("Didn't get the LocalDBGetAllKVSuccess");
    }

    // test delete_kv
    to_service.send(InternalServiceRequest::LocalDBDeleteKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBDeleteKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
    } else {
        panic!("Didn't get the LocalDBDeleteKVSuccess");
    }

    Ok(())
}
