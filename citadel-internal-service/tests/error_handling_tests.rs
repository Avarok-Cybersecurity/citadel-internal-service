use citadel_internal_service::kernel::CitadelWorkspaceService;
use citadel_internal_service_types::{
    DisconnectNotification, InternalServiceRequest, InternalServiceResponse, MessageGroupKey, UserIdentifier,
};
use citadel_sdk::prelude::*;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task;
use uuid::Uuid;
use bytes::BytesMut;

mod common;

#[cfg(test)]
mod tests {
    use crate::common::setup_log;
    use citadel_internal_service_test_common::{
        register_and_connect_to_server, server_info_skip_cert_verification, RegisterAndConnectItems,
    };
    use super::*;

    async fn setup_test_environment() -> Result<
        (
            UnboundedSender<InternalServiceRequest>,
            UnboundedReceiver<InternalServiceResponse>,
            u64,
        ),
        Box<dyn std::error::Error>,
    > {
        let (bind_address, server_address) = server_info_skip_cert_verification();
        let service_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let service = CitadelWorkspaceService::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::InMemory)
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        task::spawn(internal_service);
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let peer = RegisterAndConnectItems {
            internal_service_addr: service_addr,
            server_addr: server_address,
            full_name: "Test Peer".to_string(),
            username: "test.peer".to_string(),
            password: "secret".as_bytes().to_vec(),
            pre_shared_key: None,
        };

        let mut service_vec = register_and_connect_to_server(vec![peer]).await?;
        let (to_service, from_service, cid) = service_vec.remove(0);

        Ok((to_service, from_service, cid))
    }

    #[tokio::test]
    async fn test_invalid_request_handling() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Test invalid peer CID
        let invalid_cid = 999999;
        to_service
            .send(InternalServiceRequest::Message {
                request_id: Uuid::new_v4(),
                cid,
                peer_cid: Some(invalid_cid),
                message: "test message".as_bytes().to_vec(),
                security_level: SecurityLevel::Standard,
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
    async fn test_invalid_group_operations() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Try to send message to non-existent group
        let invalid_group_id = Uuid::new_v4();
        to_service
            .send(InternalServiceRequest::GroupMessage {
                cid,
                request_id: Uuid::new_v4(),
                group_key: MessageGroupKey::new(cid, invalid_group_id.as_u128()),
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
    async fn test_malformed_requests() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Test empty message
        to_service
            .send(InternalServiceRequest::Message {
                request_id: Uuid::new_v4(),
                cid,
                peer_cid: Some(cid),
                message: Vec::new(),
                security_level: SecurityLevel::Standard,
            })
            .unwrap();

        // Test very large message
        let large_message = vec![b'x'; 10 * 1024 * 1024]; // 10MB
        to_service
            .send(InternalServiceRequest::Message {
                request_id: Uuid::new_v4(),
                cid,
                peer_cid: Some(cid),
                message: large_message.to_vec(),
                security_level: SecurityLevel::Standard,
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
    async fn test_request_timeout_handling() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
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
    async fn test_recovery_after_errors() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // First, trigger some errors
        for _ in 0..3 {
            to_service
                .send(InternalServiceRequest::Message {
                    request_id: Uuid::new_v4(),
                    cid,
                    peer_cid: Some(999999), // invalid CID
                    message: "test message".as_bytes().to_vec(),
                    security_level: SecurityLevel::Standard,
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
                cid,
                request_id: Uuid::new_v4(),
                initial_users_to_invite: Some(vec![]),
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
    async fn test_concurrent_error_handling() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();
        let (to_service, mut from_service, _) = setup_test_environment().await?;

        // Send multiple invalid requests concurrently
        for i in 0..5 {
            let invalid_cid = 999999 + i;
            to_service
                .send(InternalServiceRequest::Message {
                    request_id: Uuid::new_v4(),
                    cid: 0,
                    peer_cid: Some(invalid_cid),
                    message: "test message".as_bytes().to_vec(),
                    security_level: SecurityLevel::Standard,
                })
                .unwrap();

            let invalid_group_id = Uuid::new_v4();
            to_service
                .send(InternalServiceRequest::GroupMessage {
                    cid,
                    request_id: Uuid::new_v4(),
                    group_key: MessageGroupKey::new(cid, invalid_group_id.as_u128()),
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
