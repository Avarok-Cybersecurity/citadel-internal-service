#![allow(dead_code)]

use bytes::Bytes;
use citadel_logging::info;
use citadel_sdk::prefabs::server::client_connect_listener::ClientConnectListenerKernel;
use citadel_sdk::prefabs::server::empty::EmptyKernel;
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::*;
use citadel_workspace_lib::wrap_tcp_conn;
use citadel_workspace_service::kernel::CitadelWorkspaceService;
use citadel_workspace_types::{
    InternalServiceRequest, InternalServiceResponse, PeerConnectSuccess, PeerRegisterSuccess,
    ServiceConnectionAccepted,
};
use core::panic;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

pub async fn register_and_connect_to_server<
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
        UnboundedSender<InternalServiceRequest>,
        UnboundedReceiver<InternalServiceResponse>,
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
    let greeter_packet: InternalServiceResponse = bincode2::deserialize(&first_packet)?;

    info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        let username = item.username.into();
        let full_name = item.full_name.into();
        let password = item.password.into();
        let session_security_settings = SessionSecuritySettingsBuilder::default()
            // .with_crypto_params(EncryptionAlgorithm::AES_GCM_256 + KemAlgorithm::Kyber + SigAlgorithm::None)
            .build()
            .unwrap();

        if let InternalServiceResponse::ServiceConnectionAccepted(ServiceConnectionAccepted) =
            greeter_packet
        {
            let register_command = InternalServiceRequest::Register {
                request_id: Uuid::new_v4(),
                server_addr: item.server_addr,
                full_name,
                username: username.clone(),
                proposed_password: password.clone(),
                session_security_settings,
                connect_after_register: false,
            };
            send(&mut sink, register_command).await.unwrap();

            let second_packet = stream.next().await.unwrap().unwrap();
            let response_packet: InternalServiceResponse =
                bincode2::deserialize(&second_packet).unwrap();
            if let InternalServiceResponse::RegisterSuccess(
                citadel_workspace_types::RegisterSuccess { request_id: _ },
            ) = response_packet
            {
                // now, connect to the server
                let command = InternalServiceRequest::Connect {
                    username,
                    password,
                    connect_mode: Default::default(),
                    udp_mode: Default::default(),
                    keep_alive_timeout: None,
                    session_security_settings,
                    request_id: Uuid::new_v4(),
                };

            send(&mut sink, command).await?;

            let next_packet = stream.next().await.unwrap()?;
            let response_packet: InternalServiceResponse = bincode2::deserialize(&next_packet)?;
            if let InternalServiceResponse::ConnectSuccess(
                citadel_workspace_types::ConnectSuccess { cid, request_id: _ },
            ) = response_packet
            {
                let (to_service, from_service) = tokio::sync::mpsc::unbounded_channel();
                let service_to_test = async move {
                    // take messages from the service and send them to from_service
                    while let Some(msg) = stream.next().await {
                        let msg = msg.unwrap();
                        let msg_deserialized: InternalServiceResponse =
                            bincode2::deserialize(&msg).unwrap();
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

                Ok((to_service_sender, from_service, cid))
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

pub async fn register_and_connect_to_server_then_peers(
    a_int_svc_addr: SocketAddr,
    b_int_svc_addr: SocketAddr,
) -> Result<PeerReturnHandle, Box<dyn Error>> {
    // internal service for peer A
    let bind_address_internal_service_a = a_int_svc_addr;
    // internal service for peer B
    let bind_address_internal_service_b = b_int_svc_addr;

    // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
    let (server, server_bind_address) = server_info_skip_cert_verification();

    tokio::task::spawn(server);
    info!(target: "citadel", "sub server spawn");
    let internal_service_kernel_a = CitadelWorkspaceService::new(bind_address_internal_service_a);
    let internal_service_a = NodeBuilder::default()
        .with_node_type(NodeType::Peer)
        // .with_backend(BackendType::InMemory) We need a filesystem backend for this test
        .with_insecure_skip_cert_verification()
        .build(internal_service_kernel_a)
        .unwrap();

    let internal_service_kernel_b = CitadelWorkspaceService::new(bind_address_internal_service_b);

    let internal_service_b = NodeBuilder::default()
        .with_node_type(NodeType::Peer)
        // .with_backend(BackendType::InMemory) We need a filesystem backend for this test
        .with_insecure_skip_cert_verification()
        .build(internal_service_kernel_b)
        .unwrap();

    spawn_services(internal_service_a, internal_service_b);

    // give time for both the server and internal service to run
    tokio::time::sleep(Duration::from_millis(2000)).await;
    info!(target: "citadel", "about to connect to internal service");
    let (to_service_a, mut from_service_a, cid_a) = register_and_connect_to_server(
        bind_address_internal_service_a,
        server_bind_address,
        "Peer A",
        "peer.a",
        "secret_a",
    )
    .await
    .unwrap();
    let (to_service_b, mut from_service_b, cid_b) = register_and_connect_to_server(
        bind_address_internal_service_b,
        server_bind_address,
        "Peer B",
        "peer.b",
        "secret_b",
    )
    .await
    .unwrap();
    let session_security_settings = SessionSecuritySettingsBuilder::default()
        // .with_crypto_params(EncryptionAlgorithm::AES_GCM_256 + KemAlgorithm::Kyber + SigAlgorithm::None)
        .build()
        .unwrap();

            // now, both peers are connected and registered to the central server. Now, we
            // need to have them peer-register to each other
            to_service_a
                .send(InternalServiceRequest::PeerRegister {
                    request_id: Uuid::new_v4(),
                    cid: *cid_a,
                    peer_cid: (*cid_b).into(),
                    session_security_settings,
                    connect_after_register: false,
                })
                .unwrap();

            to_service_b
                .send(InternalServiceRequest::PeerRegister {
                    request_id: Uuid::new_v4(),
                    cid: *cid_b,
                    peer_cid: (*cid_a).into(),
                    session_security_settings,
                    connect_after_register: false,
                })
                .unwrap();

    let item = from_service_b.recv().await.unwrap();

    match item {
        InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess {
            cid,
            peer_cid,
            peer_username,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_b);
            assert_eq!(peer_cid, cid_b);
            assert_eq!(peer_username, "peer.a");
        }
        _ => {
            panic!("Didn't get the PeerRegisterSuccess");
        }
    }

    let item = from_service_a.recv().await.unwrap();
    match item {
        InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess {
            cid,
            peer_cid,
            peer_username,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_a);
            assert_eq!(peer_cid, cid_a);
            assert_eq!(peer_username, "peer.b");
        }
        _ => {
            panic!("Didn't get the PeerRegisterSuccess");
        }
    }

            to_service_a
                .send(InternalServiceRequest::PeerConnect {
                    request_id: Uuid::new_v4(),
                    cid: *cid_a,
                    peer_cid: *cid_b,
                    udp_mode: Default::default(),
                    session_security_settings,
                })
                .unwrap();

            to_service_b
                .send(InternalServiceRequest::PeerConnect {
                    request_id: Uuid::new_v4(),
                    cid: *cid_b,
                    peer_cid: *cid_a,
                    udp_mode: Default::default(),
                    session_security_settings,
                })
                .unwrap();

    let item = from_service_b.recv().await.unwrap();
    match item {
        InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, request_id: _ }) => {
            assert_eq!(cid, cid_b);
        }
        _ => {
            info!(target = "citadel", "{:?}", item);
            panic!("Didn't get the PeerConnectSuccess");
        }
    }

    let item = from_service_a.recv().await.unwrap();
    match item {
        InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, request_id: _ }) => {
            assert_eq!(cid, cid_a);
            Ok((
                to_service_a,
                from_service_a,
                to_service_b,
                from_service_b,
                cid_a,
                cid_b,
            ))
        }
        _ => {
            info!(target = "citadel", "{:?}", item);
            panic!("Didn't get the PeerConnectSuccess");
        }
    }
}

pub type PeerReturnHandle = (
    UnboundedSender<InternalServiceRequest>,
    UnboundedReceiver<InternalServiceResponse>,
    UnboundedSender<InternalServiceRequest>,
    UnboundedReceiver<InternalServiceResponse>,
    u64,
    u64,
);

pub fn generic_error<T: ToString>(msg: T) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.to_string(),
    ))
}

pub fn spawn_services<F1, F2>(internal_service_a: F1, internal_service_b: F2)
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

pub async fn send(
    sink: &mut SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
    command: InternalServiceRequest,
) -> Result<(), Box<dyn Error>> {
    let command = bincode2::serialize(&command)?;
    sink.send(command.into()).await?;
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

pub fn server_info_skip_cert_verification<'a>() -> (NodeFuture<'a, EmptyKernel>, SocketAddr) {
    server_test_node_skip_cert_verification(EmptyKernel::default(), |_| {})
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
            // accept the transfer
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
