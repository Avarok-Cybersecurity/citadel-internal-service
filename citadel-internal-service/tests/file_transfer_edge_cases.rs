use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::*;
    use citadel_internal_service_types::{
        FileTransferTickNotification, InternalServiceRequest, InternalServiceResponse,
        ObjectTransferStatus,
    };
    use citadel_logging::info;
    use citadel_sdk::prelude::*;
    use std::error::Error;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use tokio::time::Duration;
    use uuid::Uuid;

    async fn setup_test_environment() -> Result<
        Vec<(
            UnboundedSender<InternalServiceRequest>,
            UnboundedReceiver<InternalServiceResponse>,
            u64,
        )>,
        Box<dyn Error>,
    > {
        let bind_address: SocketAddr = "127.0.0.1:55523".parse().unwrap();
        let server_bind_address: SocketAddr = "127.0.0.1:55524".parse().unwrap();

        // Start the server first
        let (server, _) = server_info_skip_cert_verification();
        tokio::spawn(server);

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let service_vec = register_and_connect_to_server(vec![RegisterAndConnectItems {
            internal_service_addr: bind_address,
            server_addr: server_bind_address,
            full_name: "Test User",
            username: "test.user",
            password: "password",
            pre_shared_key: None::<PreSharedKey>,
        }])
        .await?;

        Ok(service_vec)
    }

    #[tokio::test]
    async fn test_file_transfer_nonexistent_file() -> Result<(), Box<dyn Error>> {
        println!("test_file_transfer_nonexistent_file");
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        println!("checkpoint 1");
        // Test non-existent file
        let nonexistent_path = PathBuf::from("nonexistent_file.txt");
        println!("checkpoint 2");

        to_service.send(InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: nonexistent_path.clone(),
            cid: *cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: None,
            chunk_size: None,
        })?;
        println!("checkpoint 3");

        match from_service.recv().await.unwrap() {
            InternalServiceResponse::SendFileRequestFailure(failure) => {
                info!(target: "citadel", "Expected failure for non-existent file: {:?}", failure);
            }
            _ => panic!("Expected failure response for non-existent file"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_empty_file() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create empty file
        let empty_file_path = PathBuf::from("empty_test_file.txt");
        fs::write(&empty_file_path, "")?;

        to_service.send(InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: empty_file_path.clone(),
            cid: *cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: None,
            chunk_size: None,
        })?;

        match from_service.recv().await.unwrap() {
            InternalServiceResponse::SendFileRequestSuccess(_) => {
                info!(target: "citadel", "Successfully handled empty file");
            }
            _ => panic!("Unexpected response for empty file transfer"),
        }

        fs::remove_file(empty_file_path)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_large_file() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create large file (10MB)
        let large_file_path = PathBuf::from("large_test_file.txt");
        let large_content = vec![b'x'; 10 * 1024 * 1024];
        fs::write(&large_file_path, &large_content)?;

        to_service.send(InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: large_file_path.clone(),
            cid: *cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: None,
            chunk_size: Some(1024 * 1024), // 1MB chunks
        })?;

        match from_service.recv().await.unwrap() {
            InternalServiceResponse::SendFileRequestSuccess(_) => {
                info!(target: "citadel", "Successfully initiated large file transfer");
            }
            _ => panic!("Unexpected response for large file transfer initiation"),
        }

        // Wait for transfer completion
        let mut transfer_complete = false;
        while !transfer_complete {
            match from_service.recv().await.unwrap() {
                InternalServiceResponse::FileTransferTickNotification(
                    FileTransferTickNotification { status, .. },
                ) => {
                    if let ObjectTransferStatus::TransferComplete = status {
                        transfer_complete = true;
                        info!(target: "citadel", "Large file transfer completed");
                    }
                }
                _ => continue,
            }
        }

        fs::remove_file(large_file_path)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_concurrent() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create multiple test files
        let file_paths: Vec<PathBuf> = (0..3)
            .map(|i| {
                let path = PathBuf::from(format!("concurrent_test_file_{}.txt", i));
                fs::write(&path, format!("Content for file {}", i)).unwrap();
                path
            })
            .collect();

        // Start multiple transfers concurrently
        for path in &file_paths {
            to_service.send(InternalServiceRequest::SendFile {
                request_id: Uuid::new_v4(),
                source: path.clone(),
                cid: *cid,
                transfer_type: TransferType::FileTransfer,
                peer_cid: None,
                chunk_size: None,
            })?;
        }

        // Track completed transfers
        let mut completed_transfers = 0;
        while completed_transfers < file_paths.len() {
            match from_service.recv().await.unwrap() {
                InternalServiceResponse::SendFileRequestSuccess(_) => {
                    info!(target: "citadel", "Transfer initiated");
                }
                InternalServiceResponse::FileTransferTickNotification(
                    FileTransferTickNotification { status, .. },
                ) => {
                    if let ObjectTransferStatus::TransferComplete = status {
                        completed_transfers += 1;
                        info!(target: "citadel", "Transfer completed ({}/{})", completed_transfers, file_paths.len());
                    }
                }
                _ => continue,
            }
        }

        // Cleanup
        for path in file_paths {
            fs::remove_file(path)?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_interrupted() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create test file
        let file_path = PathBuf::from("interrupt_test_file.txt");
        let content = vec![b'x'; 5 * 1024 * 1024]; // 5MB file
        fs::write(&file_path, &content)?;

        // Start transfer with small chunks to increase transfer time
        to_service.send(InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_path.clone(),
            cid: *cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: None,
            chunk_size: Some(1024), // 1KB chunks for slower transfer
        })?;

        // Wait for transfer to start
        match from_service.recv().await.unwrap() {
            InternalServiceResponse::SendFileRequestSuccess(_) => {
                info!(target: "citadel", "Transfer started");
            }
            _ => panic!("Unexpected response for transfer initiation"),
        }

        // Let transfer run for a bit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Attempt to start another transfer of the same file (should fail or handle gracefully)
        to_service.send(InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_path.clone(),
            cid: *cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: None,
            chunk_size: None,
        })?;

        // Check response
        match from_service.recv().await.unwrap() {
            InternalServiceResponse::SendFileRequestFailure(failure) => {
                info!(target: "citadel", "Expected failure for concurrent transfer of same file: {:?}", failure);
            }
            _ => {
                info!(target: "citadel", "Note: System allows concurrent transfers of the same file");
            }
        }

        fs::remove_file(file_path)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_special_characters() -> Result<(), Box<dyn Error>> {
        println!("test_file_transfer_special_characters");
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create a file with special characters
        let file_path = PathBuf::from("test_file_@#$%^&.txt");
        fs::write(&file_path, "test content").unwrap();

        // Test file transfer
        let transfer_result = send_file_and_wait_for_completion(
            to_service,
            from_service,
            cid,
            file_path.clone(),
        ).await;

        fs::remove_file(file_path).unwrap();
        assert!(transfer_result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_zero_bytes_chunks() -> Result<(), Box<dyn Error>> {
        println!("test_file_transfer_zero_bytes_chunks");
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create a file with alternating zero and non-zero chunks
        let file_path = PathBuf::from("test_zero_chunks.bin");
        let mut content = Vec::new();
        for i in 0..1000 {
            if i % 2 == 0 {
                content.extend_from_slice(&[0u8; 1024]);
            } else {
                content.extend_from_slice(&[1u8; 1024]);
            }
        }
        fs::write(&file_path, &content).unwrap();

        // Test file transfer
        let transfer_result = send_file_and_wait_for_completion(
            to_service,
            from_service,
            cid,
            file_path.clone(),
        ).await;

        fs::remove_file(file_path).unwrap();
        assert!(transfer_result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transfer_cancel_mid_transfer() -> Result<(), Box<dyn Error>> {
        println!("test_file_transfer_cancel_mid_transfer");
        crate::common::setup_log();
        let mut service_vec = setup_test_environment().await?;
        let (to_service, ref mut from_service, cid) = service_vec.get_mut(0).unwrap();

        // Create a large file
        let file_path = PathBuf::from("test_cancel.bin");
        let content = vec![1u8; 10 * 1024 * 1024]; // 10MB file
        fs::write(&file_path, &content).unwrap();

        // Start file transfer
        let transfer_id = Uuid::new_v4();
        to_service.send(InternalServiceRequest::FileTransfer {
            transfer_id,
            file_path: file_path.clone(),
            peer_cid: None,
        }).unwrap();

        // Wait for transfer to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel transfer
        to_service.send(InternalServiceRequest::CancelFileTransfer {
            transfer_id,
        }).unwrap();

        // Verify cancellation
        let mut cancelled = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::FileTransferStatus(status) = response {
                if status.status == ObjectTransferStatus::Cancelled {
                    cancelled = true;
                    break;
                }
            }
        }

        fs::remove_file(file_path).unwrap();
        assert!(cancelled);
        Ok(())
    }

    async fn send_file_and_wait_for_completion(
        to_service: &UnboundedSender<InternalServiceRequest>,
        from_service: &mut UnboundedReceiver<InternalServiceResponse>,
        cid: u64,
        file_path: PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        let transfer_id = Uuid::new_v4();
        to_service.send(InternalServiceRequest::FileTransfer {
            transfer_id,
            file_path,
            peer_cid: None,
        }).unwrap();

        let mut completed = false;
        while let Ok(response) = from_service.try_recv() {
            if let InternalServiceResponse::FileTransferStatus(status) = response {
                if status.status == ObjectTransferStatus::Complete {
                    completed = true;
                    break;
                }
            }
        }

        if !completed {
            return Err("File transfer did not complete".into());
        }
        Ok(())
    }
}
