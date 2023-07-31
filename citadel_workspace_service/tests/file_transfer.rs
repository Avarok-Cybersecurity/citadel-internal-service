#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use citadel_logging::info;
    use citadel_sdk::prefabs::server::client_connect_listener::ClientConnectListenerKernel;
    use citadel_sdk::prefabs::server::empty::EmptyKernel;
    use citadel_sdk::prefabs::ClientServerRemote;
    use citadel_sdk::prelude::*;
    use citadel_workspace_lib::wrap_tcp_conn;
    use citadel_workspace_service::kernel::CitadelWorkspaceService;
    use citadel_workspace_types::{
        DeleteVirtualFileSuccess, DisconnectFailure, DownloadFileSuccess, FileTransferRequest,
        FileTransferStatus, FileTransferTick, InternalServicePayload, InternalServiceResponse,
        PeerConnectSuccess, PeerRegisterSuccess, SendFileFailure, SendFileSuccess,
        ServiceConnectionAccepted,
    };
    use core::panic;
    use futures::stream::SplitSink;
    use futures::{SinkExt, StreamExt};
    use std::error::Error;
    use std::future::Future;
    use std::net::SocketAddr;
    use std::panic::{resume_unwind, set_hook, take_hook};
    use std::path::PathBuf;
    use std::process::exit;
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::spawn;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use uuid::Uuid;

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
        let greeter_packet: InternalServiceResponse = bincode2::deserialize(&first_packet)?;

        info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        let username = username.into();
        let full_name = full_name.into();
        let password = password.into();

        if let InternalServiceResponse::ServiceConnectionAccepted(ServiceConnectionAccepted {
            id,
        }) = greeter_packet
        {
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
            let response_packet: InternalServiceResponse = bincode2::deserialize(&second_packet)?;
            if let InternalServiceResponse::RegisterSuccess(
                citadel_workspace_types::RegisterSuccess { id },
            ) = response_packet
            {
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
                let response_packet: InternalServiceResponse = bincode2::deserialize(&next_packet)?;
                if let InternalServiceResponse::ConnectSuccess(
                    citadel_workspace_types::ConnectSuccess { cid },
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

                    Ok((to_service_sender, from_service, id, cid))
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
            match message {
                NodeResult::ObjectTransferHandle(object_transfer_handle) => {
                    let mut handle = object_transfer_handle.handle;
                    let mut path = None;
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
                                let streamed_data =
                                    tokio::fs::read(path.clone().unwrap()).await.unwrap();
                                assert_eq!(
                                    cmp_data.as_slice(),
                                    streamed_data.as_slice(),
                                    "Original data and streamed data does not match"
                                );
                            }
                            ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                                path = Some(file_path);
                                assert_eq!(vfm.name, "test.txt")
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }

            Ok(())
        }

        async fn on_stop(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }
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

    pub fn server_info_file_transfer<'a>(
        switch: Arc<AtomicBool>,
    ) -> (NodeFuture<'a, ReceiverFileTransferKernel>, SocketAddr) {
        let (server, bind_addr) = server_test_node_skip_cert_verification(
            ReceiverFileTransferKernel(None, switch),
            |_| {},
        );
        (server, bind_addr)
    }

    type PeerReturnHandle = (
        UnboundedSender<InternalServicePayload>,
        UnboundedReceiver<InternalServiceResponse>,
        UnboundedSender<InternalServicePayload>,
        UnboundedReceiver<InternalServiceResponse>,
        Uuid,
        Uuid,
        u64,
        u64,
    );

    async fn register_and_connect_to_server_then_peers(
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
        let internal_service_kernel_a =
            CitadelWorkspaceService::new(bind_address_internal_service_a);
        let internal_service_a = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel_a)
            .unwrap();

        let internal_service_kernel_b =
            CitadelWorkspaceService::new(bind_address_internal_service_b);

        let internal_service_b = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
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
            InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            }) => {
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
            InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess {
                cid,
                peer_cid,
                username,
            }) => {
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
            InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid }) => {
                assert_eq!(cid, cid_b);
            }
            _ => {
                info!(target = "citadel", "{:?}", item);
                panic!("Didn't get the PeerConnectSuccess");
            }
        }

        let item = from_service_a.recv().await.unwrap();
        match item {
            InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid }) => {
                assert_eq!(cid, cid_a);
                Ok((
                    to_service_a,
                    from_service_a,
                    to_service_b,
                    from_service_b,
                    uuid_a,
                    uuid_b,
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

    #[tokio::test]
    async fn standard_file_transfer_c2s_test() -> Result<(), Box<dyn Error>> {
        // Causes panics in spawned threads to be caught
        let orig_hook = take_hook();
        set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            exit(1);
        }));

        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55518".parse().unwrap();

        // TCP client (GUI, CLI) -> Internal Service -> Receiver File Transfer Kernel server
        let server_success = &Arc::new(AtomicBool::new(false));
        //let (server, server_bind_address) = server_info_file_transfer(server_success.clone());
        let (server, server_bind_address) = server_info_file_transfer(server_success.clone());

        tokio::task::spawn(server);

        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel = CitadelWorkspaceService::new(bind_address_internal_service);
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
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

        let cmp_path = PathBuf::from("../resources/test.txt");

        let file_transfer_command = InternalServicePayload::SendFile {
            uuid,
            source: cmp_path.clone(),
            cid,
            is_revfs: false,
            peer_cid: None,
            chunk_size: None,
            virtual_directory: None,
            security_level: None,
        };
        to_service.send(file_transfer_command).unwrap();
        let file_transfer_response = from_service.recv().await.unwrap();
        match file_transfer_response {
            InternalServiceResponse::SendFileSuccess(SendFileSuccess { cid }) => {
                info!(target: "citadel", "File Sending Request Success");
            }
            InternalServiceResponse::SendFileFailure(SendFileFailure { cid, message }) => {
                panic!("File Send Failed: {message:?}")
            }
            _ => {
                panic!("File Send Error - Unhandled Response")
            }
        }

        tokio::time::sleep(Duration::from_millis(2000)).await;

        let disconnect_command = InternalServicePayload::Disconnect { uuid, cid };
        to_service.send(disconnect_command).unwrap();
        let _disconnect_response = from_service.recv().await.unwrap();

        tokio::time::sleep(Duration::from_millis(1000)).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_standard_file_transfer(
    ) -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let (
            to_service_a,
            mut from_service_a,
            to_service_b,
            mut from_service_b,
            uuid_a,
            uuid_b,
            cid_a,
            cid_b,
        ) = register_and_connect_to_server_then_peers(
            bind_address_internal_service_a,
            bind_address_internal_service_b,
        )
        .await?;

        let mut file_to_send = PathBuf::from("..");
        file_to_send.push("resources");
        file_to_send.push("test");
        file_to_send.set_extension("txt");

        let send_file_to_service_b_payload = InternalServicePayload::SendFile {
            uuid: uuid_a,
            source: file_to_send,
            cid: cid_a,
            is_revfs: false,
            peer_cid: Some(cid_b),
            chunk_size: None,
            virtual_directory: None,
            security_level: None,
        };
        to_service_a.send(send_file_to_service_b_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        //info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::FileTransferStatus(FileTransferStatus { .. }) =
            &deserialized_service_a_payload_response
        {
            info!(target:"citadel", "File Transfer Request {cid_b}");
            let deserialized_service_a_payload_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferRequest(FileTransferRequest { .. }) =
                deserialized_service_a_payload_response
            {
                let file_transfer_accept_payload = InternalServicePayload::RespondFileTransfer {
                    uuid: uuid_b,
                    cid: cid_b,
                    peer_cid: cid_a,
                    object_id: cid_a as u32,
                    accept: true,
                    download_location: None,
                };
                to_service_b.send(file_transfer_accept_payload).unwrap();
                info!(target:"citadel", "Accepted File Transfer {cid_b}");

                let mut path = None;
                while let Some(deserialized_service_a_payload_response) =
                    from_service_a.recv().await
                {
                    if let InternalServiceResponse::FileTransferTick(FileTransferTick {
                        uuid,
                        cid,
                        peer_cid,
                        status,
                    }) = deserialized_service_a_payload_response
                    {
                        match status {
                            ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                                path = Some(file_path);
                                info!(target: "citadel","File Transfer Beginning");
                                assert_eq!(vfm.name, "test.txt")
                            }
                            ObjectTransferStatus::ReceptionTick(_, _, _) => {
                                info!(target: "citadel","File Transfer Tick");
                            }
                            ObjectTransferStatus::ReceptionComplete => {
                                info!(target: "citadel","File Transfer Complete");
                                let mut cmp_path = PathBuf::from("..");
                                cmp_path.push("resources");
                                cmp_path.push("test");
                                cmp_path.set_extension("txt");
                                let cmp_data = tokio::fs::read(cmp_path.clone()).await.unwrap();
                                let streamed_data =
                                    tokio::fs::read(path.clone().unwrap()).await.unwrap();
                                assert_eq!(
                                    cmp_data.as_slice(),
                                    streamed_data.as_slice(),
                                    "Original data and streamed data does not match"
                                );
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                let mut cmp_path = PathBuf::from("..");
                cmp_path.push("resources");
                cmp_path.push("test");
                cmp_path.set_extension("txt");
                let cmp_data = tokio::fs::read(cmp_path.clone()).await.unwrap();
                let streamed_data = tokio::fs::read(path.clone().unwrap()).await.unwrap();
                assert_eq!(
                    cmp_data.as_slice(),
                    streamed_data.as_slice(),
                    "Original data and streamed data does not match"
                );
            } else {
                panic!("File Transfer P2P Failure");
            }
        } else {
            panic!("File Transfer Request failed: {deserialized_service_a_payload_response:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_c2s_revfs() -> Result<(), Box<dyn Error>> {
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
            .with_insecure_skip_cert_verification()
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

        // Push file to REVFS
        let file_to_send = PathBuf::from("../resources/test.txt");
        let virtual_path = PathBuf::from("/test.txt");
        let file_transfer_command = InternalServicePayload::SendFile {
            uuid,
            source: file_to_send,
            cid,
            is_revfs: true,
            peer_cid: None,
            chunk_size: None,
            virtual_directory: Some(virtual_path),
            security_level: None,
        };
        to_service.send(file_transfer_command).unwrap();
        let file_transfer_response = from_service.recv().await.unwrap();
        match file_transfer_response {
            InternalServiceResponse::SendFileSuccess(SendFileSuccess { cid: response_cid }) => {
                assert_eq!(cid, response_cid);
            }
            _ => {
                info!(target = "citadel", "{:?}", file_transfer_response);
                panic!("Didn't get the REVFS SendFileSuccess");
            }
        }
        info!(target: "citadel","{file_transfer_response:?}");

        // Download/Pull file from REVFS - Don't delete on pull
        let virtual_path = PathBuf::from("/test.txt");
        let file_download_command = InternalServicePayload::DownloadFile {
            virtual_directory: virtual_path,
            security_level: None,
            delete_on_pull: false,
            cid,
            peer_cid: None,
            uuid,
        };
        to_service.send(file_download_command).unwrap();
        let file_download_response = from_service.recv().await.unwrap();
        match file_download_response {
            InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                cid: response_cid,
            }) => {
                assert_eq!(cid, response_cid);
            }
            _ => {
                info!(target = "citadel", "{:?}", file_download_response);
                panic!("Didn't get the REVFS DownloadFileSuccess");
            }
        }
        info!(target: "citadel","{file_download_response:?}");

        // Delete file from REVFS
        let virtual_path = PathBuf::from("/test.txt");
        let file_delete_command = InternalServicePayload::DeleteVirtualFile {
            virtual_directory: virtual_path,
            cid,
            peer_cid: None,
            uuid,
        };
        to_service.send(file_delete_command).unwrap();
        let file_delete_command = from_service.recv().await.unwrap();
        match file_delete_command {
            InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                cid: response_cid,
            }) => {
                assert_eq!(cid, response_cid);
            }
            _ => {
                info!(target = "citadel", "{:?}", file_delete_command);
                panic!("Didn't get the REVFS DeleteVirtualFileSuccess");
            }
        }
        info!(target: "citadel","{file_delete_command:?}");

        let disconnect_command = InternalServicePayload::Disconnect { uuid, cid };
        to_service.send(disconnect_command).unwrap();
        let _disconnect_response = from_service.recv().await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_revfs() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let (
            to_service_a,
            mut from_service_a,
            to_service_b,
            mut from_service_b,
            uuid_a,
            uuid_b,
            cid_a,
            cid_b,
        ) = register_and_connect_to_server_then_peers(
            bind_address_internal_service_a,
            bind_address_internal_service_b,
        )
        .await?;

        // Push file to REVFS on peer
        let file_to_send = PathBuf::from("../resources/test.txt");
        let virtual_path = PathBuf::from("/vfs/virtual_test.txt");
        let send_file_to_service_b_payload = InternalServicePayload::SendFile {
            uuid: uuid_a,
            source: file_to_send,
            cid: cid_a,
            is_revfs: true,
            peer_cid: Some(cid_b),
            chunk_size: None,
            virtual_directory: Some(virtual_path),
            security_level: None,
        };
        to_service_a.send(send_file_to_service_b_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::FileTransferStatus(FileTransferStatus { .. }) =
            &deserialized_service_a_payload_response
        {
            info!(target:"citadel", "File Transfer Request {cid_b}");
            let deserialized_service_a_payload_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferRequest(FileTransferRequest { .. }) =
                deserialized_service_a_payload_response
            {
                let file_transfer_accept_payload = InternalServicePayload::RespondFileTransfer {
                    uuid: uuid_b,
                    cid: cid_b,
                    peer_cid: cid_a,
                    object_id: cid_a as u32,
                    accept: true,
                    download_location: None,
                };
                to_service_b.send(file_transfer_accept_payload).unwrap();
                info!(target:"citadel", "Accepted File Transfer {cid_b}");
            } else {
                panic!("File Transfer P2P Failure");
            }
        } else {
            panic!("File Transfer Request failed: {deserialized_service_a_payload_response:?}");
        }

        // Download P2P REVFS file - without delete on pull
        let virtual_path = PathBuf::from("/vfs/virtual_test.txt");
        let download_file_command = InternalServicePayload::DownloadFile {
            virtual_directory: virtual_path.clone(),
            security_level: None,
            delete_on_pull: false,
            cid: cid_a,
            peer_cid: Some(cid_b),
            uuid: uuid_a,
        };
        to_service_a.send(download_file_command).unwrap();
        let download_file_response = from_service_a.recv().await.unwrap();
        match download_file_response {
            InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                cid: response_cid,
            }) => {
                assert_eq!(cid_a, response_cid);
            }
            _ => {
                info!(target = "citadel", "{:?}", download_file_response);
                panic!("Didn't get the REVFS DownloadFileSuccess");
            }
        }
        info!(target: "citadel","{download_file_response:?}");

        // Delete file on Peer REVFS
        let delete_file_command = InternalServicePayload::DeleteVirtualFile {
            virtual_directory: virtual_path,
            cid: cid_a,
            peer_cid: Some(cid_b),
            uuid: uuid_a,
        };
        to_service_a.send(delete_file_command).unwrap();
        let delete_file_response = from_service_a.recv().await.unwrap();
        match download_file_response {
            InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                cid: response_cid,
            }) => {
                assert_eq!(cid_a, response_cid);
            }
            _ => {
                info!(target = "citadel", "{:?}", delete_file_response);
                panic!("Didn't get the REVFS DownloadFileSuccess");
            }
        }
        info!(target: "citadel","{delete_file_response:?}");

        Ok(())
    }
}
