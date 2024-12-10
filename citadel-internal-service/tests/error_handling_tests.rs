use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, server_info_skip_cert_verification,
        RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{
        InternalServiceRequest, InternalServiceResponse, MessageSendFailure,
        PeerConnectFailure, GroupCreateFailure,
    };
    use citadel_sdk::prelude::*;
    use std::error::Error;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use bytes::BytesMut;
    use uuid::Uuid;

    async fn setup_test_environment() -> Result<(UnboundedSender<InternalServiceRequest>, UnboundedReceiver<InternalServiceResponse>, u64), Box<dyn Error>> {
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let service_addr: SocketAddr = "127.0.0.1:55790".parse().unwrap();
        let service = CitadelWorkspaceService::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::InMemory)
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let peer = RegisterAndConnectItems {
            internal_service_addr: service_addr,
            server_addr: server_bind_address,
            full_name: "Test Peer".to_string(),
            username: "test.peer".to_string(),
            password: "secret".into_bytes().to_owned(),
            pre_shared_key: None::<PreSharedKey>,
        };

        let mut service_vec = register_and_connect_to_server(vec![peer]).await?;
        let (to_service, from_service, cid) = service_vec.remove(0);
        Ok((to_service, from_service, cid))
    }

    #[tokio::test]
    async fn test_invalid_request_handling() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, _) = setup_test_environment().await?;

        // Test invalid peer CID
        let invalid_cid = 999999;
        to_service
            .send(InternalServiceRequest::Message {
                peer_cid: invalid_cid,
                message: BytesMut::from("test message"),
            })
            .unwrap();

        // Verify error response
        let mut error_received = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::MessageSendFailure(_) = response {
                error_received = true;
                break;
            }
        }
        assert!(error_received);

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_group_operations() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, _) = setup_test_environment().await?;

        // Try to send message to non-existent group
        let invalid_group_id = Uuid::new_v4();
        to_service
            .send(InternalServiceRequest::GroupMessage {
                group_id: invalid_group_id,
                message: BytesMut::from("test message"),
            })
            .unwrap();

        // Verify error response
        let mut error_received = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::GroupMessageFailure(_) = response {
                error_received = true;
                break;
            }
        }
        assert!(error_received);

        Ok(())
    }

    #[tokio::test]
    async fn test_malformed_requests() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Test empty message
        to_service
            .send(InternalServiceRequest::Message {
                peer_cid: cid,
                message: BytesMut::new(),
            })
            .unwrap();

        // Test very large message
        let large_message = vec![b'x'; 10 * 1024 * 1024]; // 10MB
        to_service
            .send(InternalServiceRequest::Message {
                peer_cid: cid,
                message: BytesMut::from(&large_message[..]),
            })
            .unwrap();

        // Count error responses
        let mut error_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::MessageSendFailure(_) = response {
                error_count += 1;
                if error_count == 2 {
                    break;
                }
            }
        }
        assert_eq!(error_count, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_timeout_handling() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, _) = setup_test_environment().await?;

        // Try to connect to non-responsive peer
        let nonexistent_cid = 999999;
        to_service
            .send(InternalServiceRequest::PeerConnect {
                peer_cid: nonexistent_cid,
                pre_shared_key: None,
            })
            .unwrap();

        // Verify timeout error
        let mut timeout_error = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::PeerConnectFailure(_) = response {
                timeout_error = true;
                break;
            }
        }
        assert!(timeout_error);

        Ok(())
    }

    #[tokio::test]
    async fn test_recovery_after_errors() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // First, trigger some errors
        for _ in 0..3 {
            to_service
                .send(InternalServiceRequest::Message {
                    peer_cid: 999999, // invalid CID
                    message: BytesMut::from("test message"),
                })
                .unwrap();
        }

        // Wait for error responses
        let mut error_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::MessageSendFailure(_) = response {
                error_count += 1;
                if error_count == 3 {
                    break;
                }
            }
        }

        // Now try valid operations
        let group_id = Uuid::new_v4();
        to_service
            .send(InternalServiceRequest::GroupCreate {
                group_id,
                initial_users: vec![],
            })
            .unwrap();

        // Verify successful operation
        let mut success = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::GroupCreateSuccess(_) = response {
                success = true;
                break;
            }
        }
        assert!(success);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_error_handling() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, _) = setup_test_environment().await?;

        // Send multiple invalid requests concurrently
        for i in 0..5 {
            let invalid_cid = 999999 + i;
            to_service
                .send(InternalServiceRequest::Message {
                    peer_cid: invalid_cid,
                    message: BytesMut::from("test message"),
                })
                .unwrap();

            let invalid_group_id = Uuid::new_v4();
            to_service
                .send(InternalServiceRequest::GroupMessage {
                    group_id: invalid_group_id,
                    message: BytesMut::from("test message"),
                })
                .unwrap();
        }

        // Count error responses
        let mut message_errors = 0;
        let mut group_errors = 0;
        while let Ok(response) = from_service.try_recv() {
            match response {
                InternalServiceResponse::MessageSendFailure(_) => message_errors += 1,
                InternalServiceResponse::GroupMessageFailure(_) => group_errors += 1,
                _ => {}
            }
            if message_errors == 5 && group_errors == 5 {
                break;
            }
        }
        assert_eq!(message_errors, 5);
        assert_eq!(group_errors, 5);

        Ok(())
    }
}
