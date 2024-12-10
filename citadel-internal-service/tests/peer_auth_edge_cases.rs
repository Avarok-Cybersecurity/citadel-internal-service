use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, server_info_skip_cert_verification,
        RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{
        InternalServiceRequest, InternalServiceResponse, PeerConnectFailure,
        PeerConnectSuccess, PeerDisconnectNotification, PeerEvent,
    };
    use citadel_sdk::prelude::*;
    use std::error::Error;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    async fn setup_test_environment() -> Result<Vec<(UnboundedSender<InternalServiceRequest>, UnboundedReceiver<InternalServiceResponse>, u64)>, Box<dyn Error>> {
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let peers = vec![
            RegisterAndConnectItems {
                internal_service_addr: "127.0.0.1:55783".parse().unwrap(),
                server_addr: server_bind_address,
                full_name: "Peer A".to_string(),
                username: "peer.a".to_string(),
                password: "secret_a".into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
            RegisterAndConnectItems {
                internal_service_addr: "127.0.0.1:55784".parse().unwrap(),
                server_addr: server_bind_address,
                full_name: "Peer B".to_string(),
                username: "peer.b".to_string(),
                password: "secret_b".into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
        ];

        let service_vec = register_and_connect_to_server(peers).await?;
        Ok(service_vec)
    }

    #[tokio::test]
    async fn test_peer_auth_invalid_credentials() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service_a, mut from_service_a, cid_a) = service_vec.remove(0);
        let (_, _, cid_b) = service_vec.remove(0);

        // Try to connect with invalid PSK
        let invalid_psk = PreSharedKey::generate();
        to_service_a
            .send(InternalServiceRequest::PeerConnect {
                peer_cid: cid_b,
                pre_shared_key: Some(invalid_psk),
            })
            .unwrap();

        // Verify connection fails
        let mut connect_failed = false;
        while let Ok(response) = from_service_a.try_recv() {
            if let InternalServiceResponse::PeerConnectFailure(_) = response {
                connect_failed = true;
                break;
            }
        }
        assert!(connect_failed);

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_auth_multiple_connections() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service_a, mut from_service_a, cid_a) = service_vec.remove(0);
        let (_, _, cid_b) = service_vec.remove(0);

        // Try to establish multiple connections to the same peer
        for _ in 0..3 {
            to_service_a
                .send(InternalServiceRequest::PeerConnect {
                    peer_cid: cid_b,
                    pre_shared_key: None,
                })
                .unwrap();
        }

        // Count successful connections (should only be one)
        let mut connect_success_count = 0;
        while let Ok(response) = from_service_a.try_recv() {
            match response {
                InternalServiceResponse::PeerConnectSuccess(_) => {
                    connect_success_count += 1;
                }
                _ => {}
            }
            if connect_success_count > 1 {
                panic!("Multiple connections established to same peer!");
            }
        }
        assert_eq!(connect_success_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_auth_connection_timeout() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service_a, mut from_service_a, _) = service_vec.remove(0);

        // Try to connect to non-existent peer
        let nonexistent_cid = 999999;
        to_service_a
            .send(InternalServiceRequest::PeerConnect {
                peer_cid: nonexistent_cid,
                pre_shared_key: None,
            })
            .unwrap();

        // Verify connection times out
        let mut connect_failed = false;
        while let Ok(response) = from_service_a.try_recv() {
            if let InternalServiceResponse::PeerConnectFailure(_) = response {
                connect_failed = true;
                break;
            }
        }
        assert!(connect_failed);

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_auth_reconnect_after_failure() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service_a, mut from_service_a, cid_a) = service_vec.remove(0);
        let (_, _, cid_b) = service_vec.remove(0);

        // First try with invalid PSK
        let invalid_psk = PreSharedKey::generate();
        to_service_a
            .send(InternalServiceRequest::PeerConnect {
                peer_cid: cid_b,
                pre_shared_key: Some(invalid_psk),
            })
            .unwrap();

        // Wait for failure
        let mut first_connect_failed = false;
        while let Ok(response) = from_service_a.try_recv() {
            if let InternalServiceResponse::PeerConnectFailure(_) = response {
                first_connect_failed = true;
                break;
            }
        }
        assert!(first_connect_failed);

        // Try again with no PSK
        to_service_a
            .send(InternalServiceRequest::PeerConnect {
                peer_cid: cid_b,
                pre_shared_key: None,
            })
            .unwrap();

        // Verify second attempt succeeds
        let mut second_connect_succeeded = false;
        while let Ok(response) = from_service_a.try_recv() {
            if let InternalServiceResponse::PeerConnectSuccess(_) = response {
                second_connect_succeeded = true;
                break;
            }
        }
        assert!(second_connect_succeeded);

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_auth_rapid_connect_disconnect() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service_a, mut from_service_a, cid_a) = service_vec.remove(0);
        let (_, _, cid_b) = service_vec.remove(0);

        // Perform rapid connect/disconnect cycles
        for _ in 0..5 {
            // Connect
            to_service_a
                .send(InternalServiceRequest::PeerConnect {
                    peer_cid: cid_b,
                    pre_shared_key: None,
                })
                .unwrap();

            // Wait for connection
            let mut connected = false;
            while let Ok(response) = from_service_a.try_recv() {
                if let InternalServiceResponse::PeerConnectSuccess(_) = response {
                    connected = true;
                    break;
                }
            }
            assert!(connected);

            // Immediately disconnect
            to_service_a
                .send(InternalServiceRequest::PeerDisconnect {
                    peer_cid: cid_b,
                })
                .unwrap();

            // Wait for disconnect
            let mut disconnected = false;
            while let Ok(response) = from_service_a.try_recv() {
                if let InternalServiceResponse::PeerDisconnectSuccess(_) = response {
                    disconnected = true;
                    break;
                }
            }
            assert!(disconnected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_auth_concurrent_connections() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        // Create multiple peers
        let mut peers = Vec::new();
        for i in 0..5 {
            peers.push(RegisterAndConnectItems {
                internal_service_addr: format!("127.0.0.1:{}", 55785 + i).parse().unwrap(),
                server_addr: server_bind_address,
                full_name: format!("Peer {}", i),
                username: format!("peer.{}", i),
                password: format!("secret_{}", i).into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            });
        }

        let mut service_vec = register_and_connect_to_server(peers).await?;
        let (to_service_main, mut from_service_main, cid_main) = service_vec.remove(0);

        // Try to connect to all other peers concurrently
        let peer_cids: Vec<u64> = service_vec.iter().map(|(_, _, cid)| *cid).collect();
        for &peer_cid in &peer_cids {
            to_service_main
                .send(InternalServiceRequest::PeerConnect {
                    peer_cid,
                    pre_shared_key: None,
                })
                .unwrap();
        }

        // Count successful connections
        let mut connect_success_count = 0;
        while let Ok(response) = from_service_main.try_recv() {
            if let InternalServiceResponse::PeerConnectSuccess(_) = response {
                connect_success_count += 1;
                if connect_success_count == peer_cids.len() {
                    break;
                }
            }
        }
        assert_eq!(connect_success_count, peer_cids.len());

        Ok(())
    }
}
