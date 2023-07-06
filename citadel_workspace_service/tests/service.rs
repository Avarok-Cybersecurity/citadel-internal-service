#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use citadel_logging::info;
    use citadel_sdk::prelude::*;
    use citadel_workspace_service::kernel::{wrap_tcp_conn, CitadelWorkspaceService};
    use citadel_workspace_types::{
        InternalServicePayload, InternalServiceResponse, MessageReceived, MessageSent,
        PeerConnectSuccess, PeerRegisterSuccess, ServiceConnectionAccepted,
    };
    use core::panic;
    use futures::stream::SplitSink;
    use futures::{SinkExt, StreamExt};
    use std::collections::HashMap;
    use std::error::Error;
    use std::future::Future;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::TcpStream;
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

        if let InternalServiceResponse::MessageSent(MessageSent { cid, .. }) =
            deserialized_message_response
        {
            info!(target:"citadel", "Message {cid}");
            let deserialized_message_response = from_service.recv().await.unwrap();
            if let InternalServiceResponse::MessageReceived(MessageReceived {
                message,
                cid,
                peer_cid: _,
            }) = deserialized_message_response
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
        let greeter_packet: InternalServiceResponse = bincode2::deserialize(&first_packet)?;

        info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        if let InternalServiceResponse::ServiceConnectionAccepted(ServiceConnectionAccepted {
            id,
        }) = greeter_packet
        {
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
            let response_packet: InternalServiceResponse = bincode2::deserialize(&second_packet)?;

            if let InternalServiceResponse::ConnectSuccess(
                citadel_workspace_types::ConnectSuccess { cid: _ },
            ) = response_packet
            {
                Ok(())
            } else {
                panic!("Registration to server was not a success")
            }
        } else {
            panic!("Wrong packet type");
        }
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
    async fn test_citadel_workspace_service_peer_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        let _ = register_and_connect_to_server_then_peers(
            "127.0.0.1:55526".parse().unwrap(),
            "127.0.0.1:55527".parse().unwrap(),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_peer_message_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let (
            to_service_a,
            mut from_service_a,
            _to_service_b,
            mut from_service_b,
            uuid_a,
            _uuid_b,
            cid_a,
            cid_b,
        ) = register_and_connect_to_server_then_peers(
            bind_address_internal_service_a,
            bind_address_internal_service_b,
        )
        .await?;

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

        if let InternalServiceResponse::MessageSent(MessageSent { cid: cid_b, .. }) =
            &deserialized_service_a_message_response
        {
            info!(target:"citadel", "Message {cid_b}");
            let deserialized_service_a_message_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::MessageReceived(MessageReceived {
                message,
                cid: cid_a,
                peer_cid: _cid_b,
            }) = deserialized_service_a_message_response
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

    #[tokio::test]
    async fn test_c2s_kv() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(CitadelWorkspaceService::new(
                bind_address_internal_service_a,
            ))
            .unwrap();

        spawn_services(internal_service, server);
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let (to_service_a, mut from_service_a, uuid, cid) = register_and_connect_to_server(
            bind_address_internal_service_a,
            server_bind_address,
            "peer a",
            "peer.a",
            "password",
        )
        .await?;

        test_kv_for_service(&to_service_a, &mut from_service_a, uuid, cid, None).await
    }

    #[tokio::test]
    async fn test_p2p_kv() -> Result<(), Box<dyn Error>> {
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

        test_kv_for_service(
            &to_service_a,
            &mut from_service_a,
            uuid_a,
            cid_a,
            Some(cid_b),
        )
        .await?;
        test_kv_for_service(
            &to_service_b,
            &mut from_service_b,
            uuid_b,
            cid_b,
            Some(cid_a),
        )
        .await?;
        Ok(())
    }

    async fn test_kv_for_service(
        to_service: &UnboundedSender<InternalServicePayload>,
        from_service: &mut UnboundedReceiver<InternalServiceResponse>,
        uuid: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
    ) -> Result<(), Box<dyn Error>> {
        // test get_all_kv
        to_service.send(InternalServicePayload::LocalDBGetAllKV {
            uuid,
            cid,
            peer_cid,
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
        to_service.send(InternalServicePayload::LocalDBSetKV {
            uuid,
            cid,
            peer_cid,
            key: "tmp".to_string(),
            value: value.clone(),
        })?;

        if let InternalServiceResponse::LocalDBSetKVSuccess(resp) =
            from_service.recv().await.unwrap()
        {
            assert_eq!(resp.cid, cid);
            assert_eq!(peer_cid, resp.peer_cid);
            assert_eq!(resp.key, "tmp");
        } else {
            panic!("Didn't get the LocalDBSetKVSuccess");
        }

        // test get_kv
        to_service.send(InternalServicePayload::LocalDBGetKV {
            uuid,
            cid,
            peer_cid,
            key: "tmp".to_string(),
        })?;

        if let InternalServiceResponse::LocalDBGetKVSuccess(resp) =
            from_service.recv().await.unwrap()
        {
            assert_eq!(resp.cid, cid);
            assert_eq!(peer_cid, resp.peer_cid);
            assert_eq!(resp.key, "tmp");
            assert_eq!(&resp.value, &value);
        } else {
            panic!("Didn't get the LocalDBGetKVSuccess");
        }

        // test get_all_kv
        to_service.send(InternalServicePayload::LocalDBGetAllKV {
            uuid,
            cid,
            peer_cid,
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
        to_service.send(InternalServicePayload::LocalDBDeleteKV {
            uuid,
            cid,
            peer_cid,
            key: "tmp".to_string(),
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
}
