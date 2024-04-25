mod common;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, register_and_connect_to_server_then_peers, send,
        server_info_reactive_skip_cert_verification, server_info_skip_cert_verification,
        server_info_skip_cert_verification_with_password, spawn_services, test_kv_for_service,
        InternalServicesFutures, RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_connector::connector::InternalServiceConnector;
    use citadel_internal_service_types::{
        InternalServiceRequest, InternalServiceResponse, MessageNotification, MessageSendSuccess,
    };
    use citadel_logging::info;
    use citadel_sdk::prelude::*;
    use core::panic;
    use futures::{SinkExt, StreamExt};
    use std::error::Error;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_internal_service_register_connect() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = server_info_skip_cert_verification();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");

        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let to_spawn = vec![RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service,
            server_addr: server_bind_address,
            full_name: "John Doe",
            username: "john.doe",
            password: "secret",
            pre_shared_key: None::<PreSharedKey>,
        }];
        let returned_service_info = register_and_connect_to_server(to_spawn).await;
        let mut service_vec = returned_service_info.unwrap();
        if let Some((to_service, from_service, cid)) = service_vec.get_mut(0_usize) {
            let disconnect_command = InternalServiceRequest::Disconnect {
                cid: *cid,
                request_id: Uuid::new_v4(),
            };
            to_service.send(disconnect_command).unwrap();
            let disconnect_response = from_service.recv().await.unwrap();

            assert!(matches!(
                disconnect_response,
                InternalServiceResponse::DisconnectNotification { .. }
            ));

            Ok(())
        } else {
            panic!("Service Spawn Error")
        }
    }

    #[tokio::test]
    async fn test_internal_service_server_password() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) =
            server_info_skip_cert_verification_with_password("SecretPassword".as_bytes().into());

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");

        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let to_spawn = vec![RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service,
            server_addr: server_bind_address,
            full_name: "John Doe",
            username: "john.doe",
            password: "secret",
            pre_shared_key: Some(PreSharedKey::from("SecretPassword".as_bytes())),
        }];
        let returned_service_info = register_and_connect_to_server(to_spawn).await;
        let mut service_vec = returned_service_info.unwrap();
        if let Some((to_service, from_service, cid)) = service_vec.get_mut(0_usize) {
            let disconnect_command = InternalServiceRequest::Disconnect {
                cid: *cid,
                request_id: Uuid::new_v4(),
            };
            to_service.send(disconnect_command).unwrap();
            let disconnect_response = from_service.recv().await.unwrap();

            assert!(matches!(
                disconnect_response,
                InternalServiceResponse::DisconnectNotification { .. }
            ));

            Ok(())
        } else {
            panic!("Service Spawn Error")
        }
    }

    #[tokio::test]
    async fn test_internal_service_server_password_negative_case() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();

        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) =
            server_info_skip_cert_verification_with_password("SecretPassword".as_bytes().into());

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");

        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run
        let (mut sink, mut stream) =
            InternalServiceConnector::connect(bind_address_internal_service)
                .await?
                .split();

        tokio::time::sleep(Duration::from_millis(2000)).await;

        // Send Register WITHOUT PSK when it is required
        info!(target: "citadel", "C2S Attempting to register without PSK");
        let register_command = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: server_bind_address,
            full_name: "Full Name".into(),
            username: "Username".into(),
            proposed_password: "Password".into(),
            session_security_settings: Default::default(),
            connect_after_register: false,
            server_password: None,
        };
        sink.send(register_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::RegisterSuccess(
            citadel_internal_service_types::RegisterSuccess { request_id: _ },
        ) = response_packet
        {
            panic!("Received Unexpected RegisterSuccess");
        }

        // Send Register with INCORRECT PSK
        info!(target: "citadel", "C2S Attempting to register with Incorrect PSK");
        let register_command = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: server_bind_address,
            full_name: "Full Name".into(),
            username: "Username".into(),
            proposed_password: "Password".into(),
            session_security_settings: Default::default(),
            connect_after_register: false,
            server_password: Some(PreSharedKey::from("IncorrectPassword".as_bytes())),
        };
        sink.send(register_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::RegisterSuccess(
            citadel_internal_service_types::RegisterSuccess { request_id: _ },
        ) = response_packet
        {
            panic!("Received Unexpected RegisterSuccess");
        }

        // Send Register with correct PSK
        info!(target: "citadel", "C2S Attempting to register with correct PSK");
        let register_command = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: server_bind_address,
            full_name: "Full Name".into(),
            username: "Username".into(),
            proposed_password: "Password".into(),
            session_security_settings: Default::default(),
            connect_after_register: false,
            server_password: Some(PreSharedKey::from("SecretPassword".as_bytes())),
        };
        sink.send(register_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::RegisterSuccess(
            citadel_internal_service_types::RegisterSuccess { request_id: _ },
        ) = response_packet
        {
            info!(target: "citadel", "Successfully Registered to Server using Pre-Shared Key");
        } else {
            panic!("Didn't Receive Expected RegisterSuccess");
        }

        // Send Connect WITHOUT PSK when it is required
        info!(target: "citadel", "C2S Attempting to connect without PSK");
        let connect_command = InternalServiceRequest::Connect {
            request_id: Uuid::new_v4(),
            username: "Username".into(),
            password: "Password".into(),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: None,
            session_security_settings: Default::default(),
            server_password: None,
        };
        sink.send(connect_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::ConnectSuccess(
            citadel_internal_service_types::ConnectSuccess {
                cid: _,
                request_id: _,
            },
        ) = response_packet
        {
            panic!("Received Unexpected ConnectSuccess");
        }

        // Send Connect with INCORRECT PSK
        info!(target: "citadel", "C2S Attempting to connect with incorrect PSK");
        let connect_command = InternalServiceRequest::Connect {
            request_id: Uuid::new_v4(),
            username: "Username".into(),
            password: "Password".into(),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: None,
            session_security_settings: Default::default(),
            server_password: Some(PreSharedKey::from("IncorrectPassword".as_bytes())),
        };
        sink.send(connect_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::ConnectSuccess(
            citadel_internal_service_types::ConnectSuccess {
                cid: _,
                request_id: _,
            },
        ) = response_packet
        {
            panic!("Received Unexpected ConnectSuccess");
        }

        // Send Connect with correct PSK
        info!(target: "citadel", "C2S Attempting to connect with correct PSK");
        let connect_command = InternalServiceRequest::Connect {
            request_id: Uuid::new_v4(),
            username: "Username".into(),
            password: "Password".into(),
            connect_mode: Default::default(),
            udp_mode: Default::default(),
            keep_alive_timeout: None,
            session_security_settings: Default::default(),
            server_password: Some(PreSharedKey::from("SecretPassword".as_bytes())),
        };
        sink.send(connect_command).await.unwrap();
        let response_packet = stream.next().await.unwrap();
        if let InternalServiceResponse::ConnectSuccess(
            citadel_internal_service_types::ConnectSuccess {
                cid: _,
                request_id: _,
            },
        ) = response_packet
        {
            info!(target: "citadel", "Successfully Connected to Server using Pre-Shared Key");
        } else {
            panic!("Didn't Receive Expected ConnectSuccess");
        }

        Ok(())
    }

    // test
    #[tokio::test]
    async fn message_test() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
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
        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        let to_spawn = vec![RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service,
            server_addr: server_bind_address,
            full_name: "John Doe",
            username: "john.doe",
            password: "secret",
            pre_shared_key: None::<PreSharedKey>,
        }];
        let returned_service_info = register_and_connect_to_server(to_spawn).await;
        let mut service_vec = returned_service_info.unwrap();
        if let Some((to_service, from_service, cid)) = service_vec.get_mut(0_usize) {
            let serialized_message = bincode2::serialize("Message Test").unwrap();
            let message_command = InternalServiceRequest::Message {
                message: serialized_message,
                cid: *cid,
                peer_cid: None,
                security_level: SecurityLevel::Standard,
                request_id: Uuid::new_v4(),
            };
            to_service.send(message_command).unwrap();
            let deserialized_message_response = from_service.recv().await.unwrap();
            info!(target: "citadel","{deserialized_message_response:?}");

            if let InternalServiceResponse::MessageSendSuccess(MessageSendSuccess { cid, .. }) =
                deserialized_message_response
            {
                info!(target:"citadel", "Message {cid}");
                let deserialized_message_response = from_service.recv().await.unwrap();
                if let InternalServiceResponse::MessageNotification(MessageNotification {
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
                cid: *cid,
                request_id: Uuid::new_v4(),
            };
            to_service.send(disconnect_command).unwrap();
            let disconnect_response = from_service.recv().await.unwrap();

            assert!(matches!(
                disconnect_response,
                InternalServiceResponse::DisconnectNotification { .. }
            ));

            Ok(())
        } else {
            panic!("Service Spawn Error")
        }
    }

    #[tokio::test]
    async fn connect_after_register_true() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
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
        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
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
        let (mut sink, mut stream) =
            InternalServiceConnector::connect(bind_address_internal_service)
                .await?
                .split();

        let register_command = InternalServiceRequest::Register {
            server_addr: server_bind_address,
            full_name: String::from("John"),
            username: String::from("john_doe"),
            proposed_password: String::from("test12345").into_bytes().into(),
            session_security_settings: Default::default(),
            connect_after_register: true,
            request_id: Uuid::new_v4(),
            server_password: None,
        };

        send(&mut sink, register_command).await?;

        let response_packet = stream.next().await.unwrap();

        if let InternalServiceResponse::ConnectSuccess(
            citadel_internal_service_types::ConnectSuccess {
                cid: _,
                request_id: _,
            },
        ) = response_packet
        {
            Ok(())
        } else {
            panic!("Registration to server was not a success")
        }
    }

    async fn test_list_peers(
        cid: u64,
        peer_cid: u64,
        to_service: &tokio::sync::mpsc::UnboundedSender<InternalServiceRequest>,
        from_service: &mut tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    ) {
        // Test that service A views the right information
        let svc_a_request = InternalServiceRequest::ListAllPeers {
            request_id: Uuid::new_v4(),
            cid,
        };

        to_service.send(svc_a_request).unwrap();

        let resp = from_service.recv().await.unwrap();
        if let InternalServiceResponse::ListAllPeersResponse(list) = resp {
            assert_eq!(list.online_status.len(), 1);
            assert!(list.online_status.contains_key(&peer_cid))
        } else {
            panic!("Invalid ListAllPeers response")
        }

        let svc_a_request = InternalServiceRequest::ListRegisteredPeers {
            request_id: Uuid::new_v4(),
            cid,
        };

        to_service.send(svc_a_request).unwrap();

        let resp = from_service.recv().await.unwrap();
        if let InternalServiceResponse::ListRegisteredPeersResponse(list) = resp {
            assert_eq!(list.online_status.len(), 1);
            assert!(list.online_status.contains_key(&peer_cid))
        } else {
            panic!("Invalid ListAllPeers response")
        }
    }

    #[tokio::test]
    async fn test_internal_service_peer_test() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let _ = register_and_connect_to_server_then_peers(
            vec![
                "127.0.0.1:55526".parse().unwrap(),
                "127.0.0.1:55527".parse().unwrap(),
            ],
            None,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_peer_with_psk() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let _ = register_and_connect_to_server_then_peers(
            vec![
                "127.0.0.1:55526".parse().unwrap(),
                "127.0.0.1:55527".parse().unwrap(),
            ],
            Some(PreSharedKey::from("SecretServerPassword".as_bytes())),
            Some(PreSharedKey::from("SecretPeerPassword".as_bytes())),
        )
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn peer_connect_psk_attempt(
        peer_a_sink: &mut UnboundedSender<InternalServiceRequest>,
        peer_a_stream: &mut UnboundedReceiver<InternalServiceResponse>,
        peer_a_cid: u64,
        peer_b_sink: &mut UnboundedSender<InternalServiceRequest>,
        peer_b_stream: &mut UnboundedReceiver<InternalServiceResponse>,
        peer_b_cid: u64,
        expected_psk: Option<PreSharedKey>,
        given_psk: Option<PreSharedKey>,
        expects_success: bool,
    ) -> Result<(), Box<dyn Error>> {
        // Peer B Initiates Peer Connection
        info!(target: "citadel", "Peer B Sending First Connect Request with Expected PSK");
        let peer_connect = InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: peer_b_cid,
            peer_cid: peer_a_cid,
            udp_mode: Default::default(),
            session_security_settings: Default::default(),
            peer_session_password: expected_psk,
        };
        peer_b_sink.send(peer_connect).unwrap();

        info!(target: "citadel", "Peer A Waiting to Receive Connect Notification");
        let _register_connect_notification = peer_a_stream.recv().await.unwrap();

        // Peer Responds with Connect Request with given PSK - may succeed or fail
        info!(target: "citadel", "Peer A Sending Connect Request with Given PSK");
        let peer_connect = InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: peer_a_cid,
            peer_cid: peer_b_cid,
            udp_mode: Default::default(),
            session_security_settings: Default::default(),
            peer_session_password: given_psk,
        };
        peer_a_sink.send(peer_connect).unwrap();
        info!(target: "citadel", "Peer A Waiting for Connect Response");
        let inbound_response = peer_a_stream.recv().await.unwrap();
        if expects_success {
            if let InternalServiceResponse::PeerConnectSuccess(..) = inbound_response {
                info!(target: "citadel", "Peer A Successfully Connected as Expected");
            } else {
                panic!(
                    "Peer Connection Unexpectedly Failed when using the correct Peer Session Password"
                );
            }
            let register_request_response = peer_b_stream.recv().await.unwrap();
            if let InternalServiceResponse::PeerConnectSuccess(..) = register_request_response {
                info!(target: "citadel", "Peer B Received Expected PeerConnectSuccess Response");
            } else {
                panic!("Peer Connection Unexpectedly Failed with correct Peer Session Password");
            }
        } else {
            if let InternalServiceResponse::PeerConnectFailure(..) = inbound_response {
                info!(target: "citadel", "Peer A Failed to Connect as Expected");
            } else {
                panic!(
                    "Peer Connection Unexpectedly Succeeded with incorrect Peer Session Password"
                );
            }
            let register_request_response = peer_b_stream.recv().await.unwrap();
            if let InternalServiceResponse::PeerConnectFailure(..) = register_request_response {
                info!(target: "citadel", "Peer B Received Expected PeerConnectFailure Response");
            } else {
                panic!("Peer Connection Unexpectedly Succeeded with missing Peer Session Password");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_peer_with_psk_negative_case() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let int_svc_addrs = vec![
            "127.0.0.1:55526".parse().unwrap(),
            "127.0.0.1:55527".parse().unwrap(),
        ];

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
        let mut to_spawn: Vec<RegisterAndConnectItems<String, String, Vec<u8>, PreSharedKey>> =
            Vec::new();
        for (peer_number, int_svc_addr_iter) in int_svc_addrs.clone().iter().enumerate() {
            let bind_address_internal_service = *int_svc_addr_iter;
            to_spawn.push(RegisterAndConnectItems {
                internal_service_addr: bind_address_internal_service,
                server_addr: server_bind_address,
                full_name: format!("Peer {}", peer_number),
                username: format!("peer.{}", peer_number),
                password: format!("secret_{}", peer_number).into_bytes().to_owned(),
                pre_shared_key: None,
            });
        }

        // Registers and Connects all peers to Server
        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (first, second) = returned_service_info.split_at_mut(1usize);
        let (ref mut peer_a_sink, ref mut peer_a_stream, peer_a_cid) = &mut first[0];
        let (ref mut peer_b_sink, ref mut peer_b_stream, peer_b_cid) = &mut second[0];

        // Peer B Initiates Peer Registration
        info!(target: "citadel", "Peer B Sending First Register Request");
        let peer_register = InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: *peer_b_cid,
            peer_cid: *peer_a_cid,
            session_security_settings: Default::default(),
            connect_after_register: false,
            peer_session_password: None,
        };
        peer_b_sink.send(peer_register).unwrap();

        info!(target: "citadel", "Peer A Waiting to Receive Register Notification");
        let _register_request_notification = peer_a_stream.recv().await.unwrap();

        // Peer Register with Correct PSK when it is expected
        info!(target: "citadel", "Peer A Sending Register Request with Correct PSK");
        let peer_register = InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: *peer_a_cid,
            peer_cid: *peer_b_cid,
            session_security_settings: Default::default(),
            connect_after_register: false,
            peer_session_password: None,
        };
        peer_a_sink.send(peer_register).unwrap();
        info!(target: "citadel", "Peer A Waiting for Register Response");
        let inbound_response = peer_a_stream.recv().await.unwrap();
        if let InternalServiceResponse::PeerRegisterSuccess(..) = inbound_response {
            info!(target: "citadel", "Peer A Received Register Response");
        } else {
            panic!("Peer Registration Unexpectedly Failed with correct Peer Register");
        }

        let _register_request_response = peer_b_stream.recv().await.unwrap();

        // Peer Register with Incorrect PSK when it is expected
        peer_connect_psk_attempt(
            peer_a_sink,
            peer_a_stream,
            *peer_a_cid,
            peer_b_sink,
            peer_b_stream,
            *peer_b_cid,
            Some(PreSharedKey::from("PeerSessionPassword".as_bytes())),
            None,
            false,
        )
        .await
        .unwrap();

        // Peer Register WITHOUT PSK when it is expected
        peer_connect_psk_attempt(
            peer_a_sink,
            peer_a_stream,
            *peer_a_cid,
            peer_b_sink,
            peer_b_stream,
            *peer_b_cid,
            Some(PreSharedKey::from("PeerSessionPassword".as_bytes())),
            Some(PreSharedKey::from("IncorrectPassword".as_bytes())),
            false,
        )
        .await
        .unwrap();

        // Peer Register WITH PSK when it is NOT expected
        peer_connect_psk_attempt(
            peer_a_sink,
            peer_a_stream,
            *peer_a_cid,
            peer_b_sink,
            peer_b_stream,
            *peer_b_cid,
            None,
            Some(PreSharedKey::from("UnexpectedPassword".as_bytes())),
            false,
        )
        .await
        .unwrap();

        // Peer Register with Correct PSK when it is expected after having failed
        peer_connect_psk_attempt(
            peer_a_sink,
            peer_a_stream,
            *peer_a_cid,
            peer_b_sink,
            peer_b_stream,
            *peer_b_cid,
            Some(PreSharedKey::from("PeerSessionPassword".as_bytes())),
            Some(PreSharedKey::from("PeerSessionPassword".as_bytes())),
            true,
        )
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_peer_test_list_peers() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(
            vec![
                "127.0.0.1:55526".parse().unwrap(),
                "127.0.0.1:55527".parse().unwrap(),
            ],
            None,
            None,
        )
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec.as_mut_slice().split_at_mut(1_usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0_usize).unwrap();
        let (to_service_b, from_service_b, cid_b) = peer_two.get_mut(0_usize).unwrap();

        // Test that service A views the right information
        test_list_peers(*cid_a, *cid_b, to_service_a, from_service_a).await;
        // Test that service B views the right information
        test_list_peers(*cid_b, *cid_a, to_service_b, from_service_b).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_peer_message_test() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(
            vec![
                bind_address_internal_service_a,
                bind_address_internal_service_b,
            ],
            None,
            None,
        )
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec.as_mut_slice().split_at_mut(1_usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0_usize).unwrap();
        let (_to_service_b, from_service_b, cid_b) = peer_two.get_mut(0_usize).unwrap();

        let service_a_message = Vec::from("Hello World");
        let service_a_message_payload = InternalServiceRequest::Message {
            message: service_a_message.clone(),
            cid: *cid_a,
            peer_cid: Some(*cid_b),
            security_level: Default::default(),
            request_id: Uuid::new_v4(),
        };
        to_service_a.send(service_a_message_payload).unwrap();
        let deserialized_service_a_message_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_message_response:?}");

        if let InternalServiceResponse::MessageSendSuccess(MessageSendSuccess {
            cid: cid_b, ..
        }) = &deserialized_service_a_message_response
        {
            info!(target:"citadel", "Message {cid_b}");
            let deserialized_service_a_message_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::MessageNotification(MessageNotification {
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
        crate::common::setup_log();
        let (server, server_bind_address) = server_info_skip_cert_verification();

        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(CitadelWorkspaceService::new_tcp(bind_address_internal_service_a).await?)
            .unwrap();

        let mut internal_services: Vec<InternalServicesFutures> = Vec::new();
        internal_services.push(Box::pin(async move {
            match internal_service.await {
                Err(err) => Err(Box::from(err)),
                _ => Ok(()),
            }
        }));
        internal_services.push(Box::pin(async move {
            match server.await {
                Err(err) => Err(Box::from(err)),
                _ => Ok(()),
            }
        }));
        spawn_services(internal_services);
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let to_spawn = vec![RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service_a,
            server_addr: server_bind_address,
            full_name: "peer a",
            username: "peer.a",
            password: "password",
            pre_shared_key: None::<PreSharedKey>,
        }];
        let returned_service_info = register_and_connect_to_server(to_spawn).await;
        let mut service_vec = returned_service_info.unwrap();
        if let Some((to_service_a, from_service_a, cid)) = service_vec.get_mut(0_usize) {
            test_kv_for_service(to_service_a, from_service_a, *cid, None).await
        } else {
            panic!("Service Spawn Error")
        }
    }

    #[tokio::test]
    async fn test_p2p_kv() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(
            vec![
                bind_address_internal_service_a,
                bind_address_internal_service_b,
            ],
            None,
            None,
        )
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec.as_mut_slice().split_at_mut(1_usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0_usize).unwrap();
        let (to_service_b, from_service_b, cid_b) = peer_two.get_mut(0_usize).unwrap();

        test_kv_for_service(to_service_a, from_service_a, *cid_a, Some(*cid_b)).await?;
        test_kv_for_service(to_service_b, from_service_b, *cid_b, Some(*cid_a)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_forward_peer_requests() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let mut internal_services: Vec<InternalServicesFutures> = Vec::new();
        let internal_service_addresses = vec![
            "127.0.0.1:55536".parse().unwrap(),
            "127.0.0.1:55537".parse().unwrap(),
        ];
        for internal_service_address in internal_service_addresses.clone() {
            let bind_address_internal_service = internal_service_address;
            let internal_service_kernel =
                CitadelWorkspaceService::new_tcp(bind_address_internal_service).await?;
            let internal_service = NodeBuilder::default()
                .with_node_type(NodeType::Peer)
                .with_insecure_skip_cert_verification()
                .build(internal_service_kernel)
                .unwrap();

            internal_services.push(Box::pin(async move {
                match internal_service.await {
                    Err(err) => Err(Box::from(err)),
                    _ => Ok(()),
                }
            }));
        }
        spawn_services(internal_services);
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let mut to_spawn: Vec<RegisterAndConnectItems<String, String, Vec<u8>, PreSharedKey>> =
            Vec::new();
        for (peer_number, internal_service_address) in
            internal_service_addresses.clone().iter().enumerate()
        {
            let bind_address_internal_service = *internal_service_address;
            to_spawn.push(RegisterAndConnectItems {
                internal_service_addr: bind_address_internal_service,
                server_addr: server_bind_address,
                full_name: format!("Peer {}", peer_number),
                username: format!("peer.{}", peer_number),
                password: format!("secret_{}", peer_number).into_bytes().to_owned(),
                pre_shared_key: None,
            });
        }

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();

        for service_index in 0..returned_service_info.len() {
            let (item, neighbor_items) = {
                let (_, second) = returned_service_info.split_at_mut(service_index);
                let (element, remainder) = second.split_at_mut(1);
                (&mut element[0], remainder)
            };

            let (ref mut to_service_a, ref mut from_service_a, cid_a) = item;
            for neighbor in neighbor_items {
                let (ref mut to_service_b, ref mut from_service_b, cid_b) = neighbor;
                crate::common::register_p2p(
                    to_service_a,
                    from_service_a,
                    *cid_a,
                    to_service_b,
                    from_service_b,
                    *cid_b,
                    SessionSecuritySettings::default(),
                    None::<PreSharedKey>,
                )
                .await?;
                crate::common::connect_p2p(
                    to_service_a,
                    from_service_a,
                    *cid_a,
                    to_service_b,
                    from_service_b,
                    *cid_b,
                    SessionSecuritySettings::default(),
                    None::<PreSharedKey>,
                )
                .await?;
            }
        }
        Ok(())
    }
}
