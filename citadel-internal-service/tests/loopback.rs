mod common;

#[cfg(test)]
mod tests {
    use crate::common::{
        exhaust_stream_to_file_completion, register_and_connect_to_server,
        server_info_skip_cert_verification, RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{
        FileTransferRequestNotification, FileTransferStatusNotification, InternalServiceRequest,
        InternalServiceResponse,
    };
    use citadel_sdk::prelude::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_2_peers_1_service() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let service_addr = "127.0.0.1:55778".parse().unwrap();
        let service = CitadelWorkspaceService::new(service_addr);

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
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
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
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_loopback_send_file() -> Result<(), Box<dyn std::error::Error>> {
        crate::common::setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let service_addr = "127.0.0.1:55778".parse().unwrap();
        let service = CitadelWorkspaceService::new(service_addr);

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
            },
            RegisterAndConnectItems {
                internal_service_addr: service_addr,
                server_addr: server_bind_address,
                full_name: "Peer 1".to_string(),
                username: "peer.1".to_string(),
                password: "secret_1".to_string().into_bytes().to_owned(),
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
}
