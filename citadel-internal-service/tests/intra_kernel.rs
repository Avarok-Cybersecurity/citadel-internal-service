use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::{
        exhaust_stream_to_file_completion, get_free_port, register_and_connect_to_server,
        server_info_skip_cert_verification, RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{
        DeleteVirtualFileSuccess, DownloadFileSuccess, FileTransferRequestNotification,
        FileTransferStatusNotification, InternalServiceRequest, InternalServiceResponse,
        MessageNotification, MessageSendFailure, MessageSendSuccess, SendFileRequestSuccess,
    };
    use citadel_sdk::prelude::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_intra_kernel_service_and_peers() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification::<StackedRatchet>();
        tokio::task::spawn(server);

        let service_addr = format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        let service = CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::InMemory)
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Now with both the server and the IS running, we can test both peers trying to connect, then to each other
        // via p2p
        let to_spawn = vec![
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 0".to_string(),
                username: "peer.0".to_string(),
                password: "secret_0".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
        ];

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (mut peer_0_tx, mut peer_0_rx, peer_0_cid) = returned_service_info.remove(0);
        let (mut peer_1_tx, mut peer_1_rx, peer_1_cid) = returned_service_info.remove(0);

        crate::common::register_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        citadel_logging::info!(target: "citadel", "P2P Register complete");
        crate::common::connect_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_intra_kernel_peer_message() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification::<StackedRatchet>();
        tokio::task::spawn(server);

        let service_addr = format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        let service = CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::InMemory)
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Now with both the server and the IS running, we can test both peers trying to connect, then to each other
        // via p2p
        let to_spawn = vec![
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 0".to_string(),
                username: "peer.0".to_string(),
                password: "secret_0".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
        ];

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (mut peer_0_tx, mut peer_0_rx, peer_0_cid) = returned_service_info.remove(0);
        let (mut peer_1_tx, mut peer_1_rx, peer_1_cid) = returned_service_info.remove(0);

        crate::common::register_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        citadel_logging::info!(target: "citadel", "P2P Register complete");
        crate::common::connect_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        let message_request = InternalServiceRequest::Message {
            request_id: Uuid::new_v4(),
            message: "Test Message From Peer 0.".to_string().into_bytes(),
            cid: peer_0_cid,
            peer_cid: Some(peer_1_cid),
            security_level: Default::default(),
        };
        peer_0_tx.send(message_request)?;
        match peer_0_rx.recv().await.unwrap() {
            InternalServiceResponse::MessageSendSuccess(MessageSendSuccess { .. }) => {
                citadel_logging::info!(target: "citadel", "Message Successfully Sent from Peer 0 to Peer 1.");
            }
            InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                cid: _,
                message,
                request_id: _,
            }) => {
                panic!("Message Sending Failed With Error: {message:?}")
            }
            _ => {
                panic!("Received Unexpected Response When Expecting MessageSend Response.")
            }
        }
        match peer_1_rx.recv().await.unwrap() {
            InternalServiceResponse::MessageNotification(MessageNotification {
                message,
                cid: _,
                peer_cid: _,
                request_id: _,
            }) => {
                citadel_logging::info!(target: "citadel", "Message from Peer 0 Successfully Received at Peer 1: {message:?}");
            }
            _ => {
                panic!("Received Unexpected Response When Expecting MessageSend Response.")
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_intra_kernel_send_file() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification::<StackedRatchet>();
        tokio::task::spawn(server);

        let service_addr = format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        let service = CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Now with both the server and the IS running, we can test both peers trying to connect, then to each other
        // via p2p
        let to_spawn = vec![
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 0".to_string(),
                username: "peer.0".to_string(),
                password: "secret_0".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
        ];

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (mut peer_0_tx, mut peer_0_rx, peer_0_cid) = returned_service_info.remove(0);
        let (mut peer_1_tx, mut peer_1_rx, peer_1_cid) = returned_service_info.remove(0);

        crate::common::register_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        citadel_logging::info!(target: "citadel", "P2P Register complete");
        crate::common::connect_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;

        let file_to_send = PathBuf::from("../resources/test.txt");

        let send_file_to_service_1_payload = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_to_send,
            cid: peer_0_cid,
            transfer_type: TransferType::FileTransfer,
            peer_cid: Some(peer_1_cid),
            chunk_size: None,
        };
        peer_0_tx.send(send_file_to_service_1_payload).unwrap();
        citadel_logging::info!(target:"citadel", "File Transfer Request Sent from {peer_0_cid:?}");

        citadel_logging::info!(target:"citadel", "File Transfer Request Sent Successfully {peer_0_cid:?}");
        let deserialized_service_1_payload_response = peer_1_rx.recv().await.unwrap();
        if let InternalServiceResponse::FileTransferRequestNotification(
            FileTransferRequestNotification { metadata, .. },
        ) = deserialized_service_1_payload_response
        {
            citadel_logging::info!(target:"citadel", "File Transfer Request {peer_1_cid:?}");

            let file_transfer_accept = InternalServiceRequest::RespondFileTransfer {
                cid: peer_1_cid,
                peer_cid: peer_0_cid,
                object_id: metadata.object_id as _,
                accept: true,
                download_location: None,
                request_id: Uuid::new_v4(),
            };
            peer_1_tx.send(file_transfer_accept).unwrap();
            citadel_logging::info!(target:"citadel", "Accepted File Transfer {peer_1_cid:?}");

            let file_transfer_accept = peer_1_rx.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferStatusNotification(
                FileTransferStatusNotification {
                    cid: _,
                    object_id: _,
                    success,
                    response,
                    message: _,
                    request_id: _,
                },
            ) = file_transfer_accept
            {
                if success && response {
                    citadel_logging::info!(target:"citadel", "File Transfer Accept Success {peer_1_cid:?}");
                    // continue to status ticks
                } else {
                    panic!("Service 1 Accept Response Failure - Success: {success:?} Response {response:?}")
                }
            } else {
                panic!("Unhandled Service 1 response")
            }

            // Exhaust the stream for the receiver
            exhaust_stream_to_file_completion(
                PathBuf::from("../resources/test.txt"),
                &mut peer_1_rx,
            )
            .await;
            // Exhaust the stream for the sender
            exhaust_stream_to_file_completion(
                PathBuf::from("../resources/test.txt"),
                &mut peer_0_rx,
            )
            .await;
        } else {
            panic!("File Transfer P2P Failure");
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_intra_kernel_revfs() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification::<StackedRatchet>();
        tokio::task::spawn(server);

        let service_addr = format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        let service = CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(service_addr).await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Now with both the server and the IS running, we can test both peers trying to connect, then to each other
        // via p2p
        let to_spawn = vec![
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 0".to_string(),
                username: "peer.0".to_string(),
                password: "secret_0".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
                pre_shared_key: None::<PreSharedKey>,
            },
        ];

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (mut peer_0_tx, mut peer_0_rx, peer_0_cid) = returned_service_info.remove(0);
        let (mut peer_1_tx, mut peer_1_rx, peer_1_cid) = returned_service_info.remove(0);

        crate::common::register_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;
        citadel_logging::info!(target: "citadel", "P2P Register complete");
        crate::common::connect_p2p(
            &mut peer_0_tx,
            &mut peer_0_rx,
            peer_0_cid,
            &mut peer_1_tx,
            &mut peer_1_rx,
            peer_1_cid,
            SessionSecuritySettings::default(),
            None::<PreSharedKey>,
        )
        .await?;

        // Push file to REVFS on peer
        let file_to_send = PathBuf::from("../resources/test.txt");
        let virtual_path = PathBuf::from("/vfs/test.txt");
        let send_file_peer_1_tx_payload = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_to_send.clone(),
            cid: peer_0_cid,
            transfer_type: TransferType::RemoteEncryptedVirtualFilesystem {
                virtual_path: virtual_path.clone(),
                security_level: Default::default(),
            },
            peer_cid: Some(peer_1_cid),
            chunk_size: None,
        };
        peer_0_tx.send(send_file_peer_1_tx_payload).unwrap();
        let deserialized_service_a_payload_response = peer_0_rx.recv().await.unwrap();
        citadel_logging::info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::SendFileRequestSuccess(SendFileRequestSuccess { .. }) =
            &deserialized_service_a_payload_response
        {
            citadel_logging::info!(target:"citadel", "File Transfer Request {peer_1_cid}");
            let deserialized_service_a_payload_response = peer_1_rx.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferRequestNotification(
                FileTransferRequestNotification { metadata, .. },
            ) = deserialized_service_a_payload_response
            {
                let file_transfer_accept_payload = InternalServiceRequest::RespondFileTransfer {
                    cid: peer_1_cid,
                    peer_cid: peer_0_cid,
                    object_id: metadata.object_id,
                    accept: true,
                    download_location: None,
                    request_id: Uuid::new_v4(),
                };
                peer_1_tx.send(file_transfer_accept_payload).unwrap();
                citadel_logging::info!(target:"citadel", "Accepted File Transfer {peer_1_cid}");
            } else {
                panic!("File Transfer P2P Failure");
            }
        } else {
            panic!("File Transfer Request failed: {deserialized_service_a_payload_response:?}");
        }

        exhaust_stream_to_file_completion(file_to_send.clone(), &mut peer_1_rx).await;
        exhaust_stream_to_file_completion(file_to_send.clone(), &mut peer_0_rx).await;

        citadel_logging::info!(target: "citadel", "Peer 0 Requesting to Download File");

        // Download P2P REVFS file - without delete on pull
        let download_file_command = InternalServiceRequest::DownloadFile {
            virtual_directory: virtual_path.clone(),
            security_level: Default::default(),
            delete_on_pull: false,
            cid: peer_0_cid,
            peer_cid: Some(peer_1_cid),
            request_id: Uuid::new_v4(),
        };
        peer_0_tx.send(download_file_command).unwrap();
        citadel_logging::info!(target: "citadel", "Peer 0 Waiting for DownloadFileSuccess Response");
        let download_file_response = peer_0_rx.recv().await.unwrap();
        match download_file_response {
            InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                cid: response_cid,
                request_id: _,
            }) => {
                assert_eq!(peer_0_cid, response_cid);
            }
            _ => {
                panic!("Didn't get the REVFS DownloadFileSuccess - instead got {download_file_response:?}");
            }
        }

        exhaust_stream_to_file_completion(file_to_send.clone(), &mut peer_1_rx).await;
        exhaust_stream_to_file_completion(file_to_send.clone(), &mut peer_0_rx).await;

        citadel_logging::info!(target: "citadel", "Peer 0 Requesting to Delete File");

        // Delete file on Peer REVFS
        let delete_file_command = InternalServiceRequest::DeleteVirtualFile {
            virtual_directory: virtual_path,
            cid: peer_0_cid,
            peer_cid: Some(peer_1_cid),
            request_id: Uuid::new_v4(),
        };
        peer_0_tx.send(delete_file_command).unwrap();
        let delete_file_response = peer_0_rx.recv().await.unwrap();
        match delete_file_response {
            InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                cid: response_cid,
                request_id: _,
            }) => {
                assert_eq!(peer_0_cid, response_cid);
            }
            _ => {
                panic!("Didn't get the REVFS DeleteVirtualFileSuccess - instead got {delete_file_response:?}");
            }
        }
        citadel_logging::info!(target: "citadel","{delete_file_response:?}");

        Ok(())
    }
}
