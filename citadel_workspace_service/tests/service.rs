mod common;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, register_and_connect_to_server_then_peers, send,
        server_info_reactive_skip_cert_verification, server_info_skip_cert_verification,
        spawn_services, test_kv_for_service,
    };
    use citadel_logging::info;
    use citadel_sdk::prelude::*;
    use citadel_workspace_lib::wrap_tcp_conn;
    use citadel_workspace_service::kernel::CitadelWorkspaceService;
    use citadel_workspace_types::{
        InternalServiceRequest, InternalServiceResponse, MessageReceived, MessageSent,
        ServiceConnectionAccepted,
    };
    use core::panic;
    use futures::StreamExt;
    use std::error::Error;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_citadel_workspace_service_register_connect() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = server_info_skip_cert_verification();

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
        let disconnect_command = InternalServiceRequest::Disconnect {
            uuid,
            cid,
            request_id: Uuid::new_v4(),
        };
        to_service.send(disconnect_command).unwrap();
        let disconnect_response = from_service.recv().await.unwrap();

        assert!(matches!(
            disconnect_response,
            InternalServiceResponse::Disconnected { .. }
        ));

        Ok(())
    }

    // test
    #[tokio::test]
    async fn message_test() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55518".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = server_info_reactive_skip_cert_verification(
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

        let serialized_message = bincode2::serialize("Message Test").unwrap();
        let message_command = InternalServiceRequest::Message {
            uuid,
            message: serialized_message,
            cid,
            peer_cid: None,
            security_level: SecurityLevel::Standard,
            request_id: Uuid::new_v4(),
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
                request_id: _,
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

        let disconnect_command = InternalServiceRequest::Disconnect {
            uuid,
            cid,
            request_id: Uuid::new_v4(),
        };
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
        let (server, server_bind_address) = server_info_reactive_skip_cert_verification(
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
            .with_insecure_skip_cert_verification()
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
            request_id: _,
        }) = greeter_packet
        {
            let register_command = InternalServiceRequest::Register {
                uuid: id,
                server_addr: server_bind_address,
                full_name: String::from("John"),
                username: String::from("john_doe"),
                proposed_password: String::from("test12345").into_bytes().into(),
                default_security_settings: Default::default(),
                connect_after_register: true,
                request_id: Uuid::new_v4(),
            };
            send(&mut sink, register_command).await?;

            let second_packet = stream.next().await.unwrap()?;
            let response_packet: InternalServiceResponse = bincode2::deserialize(&second_packet)?;

            if let InternalServiceResponse::ConnectSuccess(
                citadel_workspace_types::ConnectSuccess {
                    cid: _,
                    request_id: _,
                },
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
        let service_a_message_payload = InternalServiceRequest::Message {
            uuid: uuid_a,
            message: service_a_message.clone(),
            cid: cid_a,
            peer_cid: Some(cid_b),
            security_level: Default::default(),
            request_id: Uuid::new_v4(),
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
                request_id: _,
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
        let (server, server_bind_address) = server_info_skip_cert_verification();

        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
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
}
