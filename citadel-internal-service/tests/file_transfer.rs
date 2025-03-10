use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::{
        exhaust_stream_to_file_completion, get_free_port, register_and_connect_to_server,
        register_and_connect_to_server_then_peers, server_info_file_transfer,
        RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_types::{
        DeleteVirtualFileSuccess, DownloadFileFailure, DownloadFileSuccess,
        FileTransferRequestNotification, FileTransferStatusNotification, InternalServiceRequest,
        InternalServiceResponse, SendFileRequestFailure, SendFileRequestSuccess,
    };
    use citadel_logging::info;
    use citadel_sdk::prelude::*;
    use core::panic;
    use std::error::Error;
    use std::net::SocketAddr;
    use std::panic::{set_hook, take_hook};
    use std::path::PathBuf;
    use std::process::exit;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_internal_service_standard_file_transfer_c2s() -> Result<(), Box<dyn Error>> {
        // Causes panics in spawned threads to be caught
        let orig_hook = take_hook();
        set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            exit(1);
        }));

        crate::common::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();

        // TCP client (GUI, CLI) -> Internal Service -> Receiver File Transfer Kernel server
        let server_success = &Arc::new(AtomicBool::new(false));
        //let (server, server_bind_address) = server_info_file_transfer(server_success.clone());
        let (server, server_bind_address) =
            server_info_file_transfer::<StackedRatchet>(server_success.clone());

        tokio::task::spawn(server);

        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel =
            CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(bind_address_internal_service)
                .await?;
        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_node_type(NodeType::Peer)
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
            let cmp_path = PathBuf::from("../resources/test.txt");

            let file_transfer_command = InternalServiceRequest::SendFile {
                request_id: Uuid::new_v4(),
                source: cmp_path.clone(),
                cid: *cid,
                transfer_type: TransferType::FileTransfer,
                peer_cid: None,
                chunk_size: None,
            };
            to_service.send(file_transfer_command).unwrap();
            exhaust_stream_to_file_completion(cmp_path, from_service).await;

            Ok(())
        } else {
            panic!("Service Spawn Error")
        }
    }

    #[tokio::test]
    async fn test_internal_service_peer_standard_file_transfer() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
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

        let file_to_send = PathBuf::from("../resources/test.txt");

        let send_file_to_service_b_payload = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_to_send,
            cid: *cid_a,
            transfer_type: TransferType::FileTransfer,
            peer_cid: Some(*cid_b),
            chunk_size: None,
        };
        to_service_a.send(send_file_to_service_b_payload).unwrap();
        info!(target:"citadel", "File Transfer Request Sent from {cid_a:?}");

        info!(target:"citadel", "File Transfer Request Sent Successfully {cid_a:?}");
        let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
        if let InternalServiceResponse::FileTransferRequestNotification(
            FileTransferRequestNotification { metadata, .. },
        ) = deserialized_service_b_payload_response
        {
            info!(target:"citadel", "File Transfer Request {cid_b:?}");

            let file_transfer_accept = InternalServiceRequest::RespondFileTransfer {
                cid: *cid_b,
                peer_cid: *cid_a,
                object_id: metadata.object_id as _,
                accept: true,
                download_location: None,
                request_id: Uuid::new_v4(),
            };
            to_service_b.send(file_transfer_accept).unwrap();
            info!(target:"citadel", "Accepted File Transfer {cid_b:?}");

            let file_transfer_accept = from_service_b.recv().await.unwrap();
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
                    info!(target:"citadel", "File Transfer Accept Success {cid_b:?}");
                    // continue to status ticks
                } else {
                    panic!("Service B Accept Response Failure - Success: {success:?} Response {response:?}")
                }
            } else {
                panic!("Unhandled Service B response")
            }

            // Exhaust the stream for the receiver
            exhaust_stream_to_file_completion(
                PathBuf::from("../resources/test.txt"),
                from_service_b,
            )
            .await;
            // Exhaust the stream for the sender
            exhaust_stream_to_file_completion(
                PathBuf::from("../resources/test.txt"),
                from_service_a,
            )
            .await;
        } else {
            panic!("File Transfer P2P Failure");
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_internal_service_c2s_revfs() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        info!(target: "citadel", "above server spawn");
        let bind_address_internal_service: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();

        // TCP client (GUI, CLI) -> Internal Service -> Receiver File Transfer Kernel server
        let server_success = &Arc::new(AtomicBool::new(false));
        let (server, server_bind_address) =
            server_info_file_transfer::<StackedRatchet>(server_success.clone());

        tokio::task::spawn(server);

        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel =
            CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(bind_address_internal_service)
                .await?;

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_node_type(NodeType::Peer)
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
            // Push file to REVFS
            let file_to_send = PathBuf::from("../resources/test.txt");
            let virtual_path = PathBuf::from("/vfs/test.txt");
            let file_transfer_command = InternalServiceRequest::SendFile {
                request_id: Uuid::new_v4(),
                source: file_to_send.clone(),
                cid: *cid,
                transfer_type: TransferType::RemoteEncryptedVirtualFilesystem {
                    virtual_path: virtual_path.clone(),
                    security_level: Default::default(),
                },
                peer_cid: None,
                chunk_size: None,
            };
            to_service.send(file_transfer_command).unwrap();
            let file_transfer_response = from_service.recv().await.unwrap();
            if let InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                cid: _,
                message,
                request_id: _,
            }) = file_transfer_response
            {
                panic!("Send File Failure: {message:?}")
            }

            // Wait for the sender to complete the transfer
            exhaust_stream_to_file_completion(file_to_send.clone(), from_service).await;

            // Download/Pull file from REVFS - Don't delete on pull
            let file_download_command = InternalServiceRequest::DownloadFile {
                virtual_directory: virtual_path.clone(),
                security_level: None,
                delete_on_pull: false,
                cid: *cid,
                peer_cid: None,
                request_id: Uuid::new_v4(),
            };
            to_service.send(file_download_command).unwrap();
            let download_file_response = from_service.recv().await.unwrap();
            if let InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                cid: _,
                message,
                request_id: _,
            }) = download_file_response
            {
                panic!("Download File Failure: {message:?}")
            }

            // Exhaust the download request
            exhaust_stream_to_file_completion(file_to_send.clone(), from_service).await;

            // Delete file from REVFS
            let file_delete_command = InternalServiceRequest::DeleteVirtualFile {
                virtual_directory: virtual_path.clone(),
                cid: *cid,
                peer_cid: None,
                request_id: Uuid::new_v4(),
            };
            to_service.send(file_delete_command).unwrap();
            info!(target: "citadel","DeleteVirtualFile Request sent to server");

            let file_delete_command = from_service.recv().await.unwrap();

            match file_delete_command {
                InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                    cid: response_cid,
                    request_id: _,
                }) => {
                    assert_eq!(*cid, response_cid);
                    info!(target: "citadel","CID Comparison Yielded Success");
                }
                _ => {
                    info!(target = "citadel", "{:?}", file_delete_command);
                    panic!("Didn't get the REVFS DeleteVirtualFileSuccess");
                }
            }
            info!(target: "citadel","{file_delete_command:?}");

            Ok(())
        } else {
            panic!("Service Spawn Error");
        }
    }

    #[tokio::test]
    async fn test_internal_service_peer_revfs() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr =
            format!("127.0.0.1:{}", get_free_port()).parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
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

        // Push file to REVFS on peer
        let file_to_send = PathBuf::from("../resources/test.txt");
        let virtual_path = PathBuf::from("/vfs/test.txt");
        let send_file_to_service_b_payload = InternalServiceRequest::SendFile {
            request_id: Uuid::new_v4(),
            source: file_to_send.clone(),
            cid: *cid_a,
            transfer_type: TransferType::RemoteEncryptedVirtualFilesystem {
                virtual_path: virtual_path.clone(),
                security_level: Default::default(),
            },
            peer_cid: Some(*cid_b),
            chunk_size: None,
        };
        to_service_a.send(send_file_to_service_b_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::SendFileRequestSuccess(SendFileRequestSuccess { .. }) =
            &deserialized_service_a_payload_response
        {
            info!(target:"citadel", "File Transfer Request {cid_b}");
            let deserialized_service_a_payload_response = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::FileTransferRequestNotification(
                FileTransferRequestNotification { metadata, .. },
            ) = deserialized_service_a_payload_response
            {
                let file_transfer_accept_payload = InternalServiceRequest::RespondFileTransfer {
                    cid: *cid_b,
                    peer_cid: *cid_a,
                    object_id: metadata.object_id,
                    accept: true,
                    download_location: None,
                    request_id: Uuid::new_v4(),
                };
                to_service_b.send(file_transfer_accept_payload).unwrap();
                info!(target:"citadel", "Accepted File Transfer {cid_b}");
            } else {
                panic!("File Transfer P2P Failure");
            }
        } else {
            panic!("File Transfer Request failed: {deserialized_service_a_payload_response:?}");
        }

        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","{deserialized_service_a_payload_response:?}");

        exhaust_stream_to_file_completion(file_to_send.clone(), from_service_b).await;
        exhaust_stream_to_file_completion(file_to_send.clone(), from_service_a).await;

        // Download P2P REVFS file - without delete on pull
        let download_file_command = InternalServiceRequest::DownloadFile {
            virtual_directory: virtual_path.clone(),
            security_level: None,
            delete_on_pull: false,
            cid: *cid_a,
            peer_cid: Some(*cid_b),
            request_id: Uuid::new_v4(),
        };
        to_service_a.send(download_file_command).unwrap();
        let download_file_response = from_service_a.recv().await.unwrap();
        match download_file_response {
            InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                cid: response_cid,
                request_id: _,
            }) => {
                assert_eq!(*cid_a, response_cid);
            }
            _ => {
                panic!("Didn't get the REVFS DownloadFileSuccess - instead got {download_file_response:?}");
            }
        }

        exhaust_stream_to_file_completion(file_to_send.clone(), from_service_b).await;
        exhaust_stream_to_file_completion(file_to_send.clone(), from_service_a).await;

        // Delete file on Peer REVFS
        let delete_file_command = InternalServiceRequest::DeleteVirtualFile {
            virtual_directory: virtual_path,
            cid: *cid_a,
            peer_cid: Some(*cid_b),
            request_id: Uuid::new_v4(),
        };
        to_service_a.send(delete_file_command).unwrap();
        let delete_file_response = from_service_a.recv().await.unwrap();
        match delete_file_response {
            InternalServiceResponse::DeleteVirtualFileSuccess(DeleteVirtualFileSuccess {
                cid: response_cid,
                request_id: _,
            }) => {
                assert_eq!(*cid_a, response_cid);
            }
            _ => {
                panic!("Didn't get the REVFS DeleteVirtualFileSuccess - instead got {delete_file_response:?}");
            }
        }
        info!(target: "citadel","{delete_file_response:?}");

        Ok(())
    }
}
