use citadel_internal_service_test_common as common;
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, server_info_skip_cert_verification, RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
    use citadel_sdk::prelude::*;
    use std::error::Error;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use uuid::Uuid;

    async fn setup_test_environment() -> Result<
        (
            UnboundedSender<InternalServiceRequest>,
            UnboundedReceiver<InternalServiceResponse>,
            u64,
        ),
        Box<dyn Error>,
    > {
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let service_addr: SocketAddr = "127.0.0.1:55782".parse().unwrap();
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
    async fn test_kv_store_large_values() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Test with a large value (1MB)
        let large_value = vec![b'x'; 1024 * 1024];
        to_service
            .send(InternalServiceRequest::LocalDBSetKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "large_key".to_string(),
                value: large_value,
            })
            .unwrap();

        // Verify set operation succeeded
        let mut set_success = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                set_success = true;
                break;
            }
        }
        assert!(set_success);

        // Retrieve and verify large value
        to_service
            .send(InternalServiceRequest::LocalDBGetKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "large_key".to_string(),
            })
            .unwrap();

        let mut get_success = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success) = response {
                assert_eq!(success.value, large_value);
                get_success = true;
                break;
            }
        }
        assert!(get_success);

        Ok(())
    }

    #[tokio::test]
    async fn test_kv_store_concurrent_operations() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Perform multiple set operations concurrently
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i).into_bytes();
            to_service
                .send(InternalServiceRequest::LocalDBSetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key,
                    value: value.to_vec(),
                })
                .unwrap();
        }

        // Count successful set operations
        let mut set_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                set_success_count += 1;
                if set_success_count == 10 {
                    break;
                }
            }
        }
        assert_eq!(set_success_count, 10);

        // Verify all values were stored correctly
        for i in 0..10 {
            let key = format!("key_{}", i);
            to_service
                .send(InternalServiceRequest::LocalDBGetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key,
                })
                .unwrap();
        }

        let mut get_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success) = response {
                let expected_value = format!("value_{}", get_success_count).into_bytes();
                assert_eq!(success.value, expected_value);
                get_success_count += 1;
                if get_success_count == 10 {
                    break;
                }
            }
        }
        assert_eq!(get_success_count, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_kv_store_special_keys() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Test special characters in keys
        let special_keys = vec![
            "key with spaces",
            "key_with_@#$%^&*",
            "很長的鑰匙", // Unicode characters
            "",           // Empty key
        ];

        // Set values for special keys
        for key in special_keys.iter() {
            to_service
                .send(InternalServiceRequest::LocalDBSetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key.to_string(),
                    value: b"test_value".to_vec(),
                })
                .unwrap();
        }

        // Verify all sets succeeded
        let mut set_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                set_success_count += 1;
                if set_success_count == special_keys.len() {
                    break;
                }
            }
        }
        assert_eq!(set_success_count, special_keys.len());

        // Try to retrieve values for special keys
        for key in special_keys.iter() {
            to_service
                .send(InternalServiceRequest::LocalDBGetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key.to_string(),
                })
                .unwrap();
        }

        // Verify all gets succeeded
        let mut get_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success) = response {
                assert_eq!(success.value, b"test_value");
                get_success_count += 1;
                if get_success_count == special_keys.len() {
                    break;
                }
            }
        }
        assert_eq!(get_success_count, special_keys.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_kv_store_delete_and_clear() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (to_service, mut from_service, cid) = setup_test_environment().await?;

        // Set up some initial key-value pairs
        for i in 0..5 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i).into_bytes();
            to_service
                .send(InternalServiceRequest::LocalDBSetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key,
                    value: value.to_vec(),
                })
                .unwrap();
        }

        // Wait for all sets to complete
        let mut set_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                set_success_count += 1;
                if set_success_count == 5 {
                    break;
                }
            }
        }

        // Delete specific keys
        to_service
            .send(InternalServiceRequest::LocalDBDeleteKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "key_0".to_string(),
            })
            .unwrap();

        to_service
            .send(InternalServiceRequest::LocalDBDeleteKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "key_1".to_string(),
            })
            .unwrap();

        // Verify deletes succeeded
        let mut delete_success_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBDeleteKVSuccess(_) = response {
                delete_success_count += 1;
                if delete_success_count == 2 {
                    break;
                }
            }
        }
        assert_eq!(delete_success_count, 2);

        // Try to get deleted keys
        to_service
            .send(InternalServiceRequest::LocalDBGetKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "key_0".to_string(),
            })
            .unwrap();

        to_service
            .send(InternalServiceRequest::LocalDBGetKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
                key: "key_1".to_string(),
            })
            .unwrap();

        // Verify gets return empty results
        let mut get_empty_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success) = response {
                assert!(success.value.is_empty());
                get_empty_count += 1;
                if get_empty_count == 2 {
                    break;
                }
            }
        }
        assert_eq!(get_empty_count, 2);

        // Clear all remaining key-value pairs
        to_service
            .send(InternalServiceRequest::LocalDBClearAllKV {
                cid: Some(cid),
                request_id: Uuid::new_v4(),
                peer_cid: None,
            })
            .unwrap();

        // Verify clear succeeded
        let mut clear_success = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBClearAllKVSuccess(_) = response {
                clear_success = true;
                break;
            }
        }
        assert!(clear_success);

        // Try to get remaining keys
        for i in 2..5 {
            let key = format!("key_{}", i);
            to_service
                .send(InternalServiceRequest::LocalDBGetKV {
                    cid: Some(cid),
                    request_id: Uuid::new_v4(),
                    peer_cid: None,
                    key: key,
                })
                .unwrap();
        }

        // Verify all gets return empty results
        let mut get_empty_count = 0;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success) = response {
                assert!(success.value.is_empty());
                get_empty_count += 1;
                if get_empty_count == 3 {
                    break;
                }
            }
        }
        assert_eq!(get_empty_count, 3);

        Ok(())
    }
}
