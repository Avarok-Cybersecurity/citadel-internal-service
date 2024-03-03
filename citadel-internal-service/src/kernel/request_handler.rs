use crate::kernel::{
    create_client_server_remote, send_response_to_tcp_client, spawn_tick_updater, Connection,
    GroupConnection,
};
use async_recursion::async_recursion;
use citadel_internal_service_types::*;
use citadel_logging::tracing::log;
use citadel_logging::{error, info};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::remote_specialization::PeerRemote;
use citadel_sdk::prelude::results::PeerRegisterStatus;
use citadel_sdk::prelude::*;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

use uuid::Uuid;
#[async_recursion]
pub async fn handle_request(
    command: InternalServiceRequest,
    uuid: Uuid,
    server_connection_map: &Arc<Mutex<HashMap<u64, Connection>>>,
    remote: &mut NodeRemote,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    match command {
        InternalServiceRequest::ListRegisteredPeers { request_id, cid } => {
            match remote.get_local_group_mutual_peers(cid).await {
                Ok(peers) => {
                    let mut accounts = HashMap::new();
                    for peer in &peers {
                        // TOOD: Do not unwrap below
                        let peer_username = remote
                            .find_target(cid, peer.cid)
                            .await
                            .unwrap()
                            .target_username()
                            .cloned()
                            .unwrap_or_default();
                        accounts.insert(
                            peer.cid,
                            PeerSessionInformation {
                                cid,
                                peer_cid: peer.cid,
                                peer_username,
                            },
                        );
                    }

                    let peers = ListRegisteredPeersResponse {
                        cid,
                        peers: accounts,
                        online_status: peers
                            .iter()
                            .map(|peer| (peer.cid, peer.is_online))
                            .collect(),
                        request_id: Some(request_id),
                    };

                    let response = InternalServiceResponse::ListRegisteredPeersResponse(peers);
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }

                Err(err) => {
                    let response = InternalServiceResponse::ListRegisteredPeersFailure(
                        ListRegisteredPeersFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        },
                    );
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }
            }
        }
        InternalServiceRequest::ListAllPeers { request_id, cid } => {
            match remote.get_local_group_peers(cid, None).await {
                Ok(peers) => {
                    let peers = ListAllPeersResponse {
                        cid,
                        online_status: peers
                            .into_iter()
                            .filter(|peer| peer.cid != cid)
                            .map(|peer| (peer.cid, peer.is_online))
                            .collect(),
                        request_id: Some(request_id),
                    };

                    let response = InternalServiceResponse::ListAllPeersResponse(peers);
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }

                Err(err) => {
                    let response =
                        InternalServiceResponse::ListAllPeersFailure(ListAllPeersFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        });
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }
            }
        }
        InternalServiceRequest::GetAccountInformation { request_id, cid } => {
            async fn add_account_to_map(
                accounts_ret: &mut HashMap<u64, AccountInformation>,
                account: CNACMetadata,
                remote: &NodeRemote,
            ) {
                let username = account.username.clone();
                let full_name = account.full_name.clone();
                let mut peers = HashMap::new();

                // Get all the peers for this CID
                let peer_cids = remote
                    .account_manager()
                    .get_hyperlan_peer_list(account.cid)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                let peers_info = remote
                    .account_manager()
                    .get_persistence_handler()
                    .get_hyperlan_peers(account.cid, &peer_cids)
                    .await
                    .unwrap_or_default();

                for peer in peers_info {
                    peers.insert(
                        peer.cid,
                        PeerSessionInformation {
                            cid: account.cid,
                            peer_cid: peer.cid,
                            peer_username: peer.username.unwrap_or_default(),
                        },
                    );
                }

                accounts_ret.insert(
                    account.cid,
                    AccountInformation {
                        username,
                        full_name,
                        peers,
                    },
                );
            }

            let mut accounts_ret = HashMap::new();

            let accounts = remote
                .account_manager()
                .get_persistence_handler()
                .get_clients_metadata(None)
                .await
                .unwrap_or_default();
            // We are only interested in the is_personal=True accounts
            let filtered_accounts = accounts
                .into_iter()
                .filter(|r| r.is_personal)
                .collect::<Vec<_>>();

            if let Some(cid) = cid {
                let account = filtered_accounts.into_iter().find(|r| r.cid == cid);
                if let Some(account) = account {
                    add_account_to_map(&mut accounts_ret, account, remote).await;
                }
            } else {
                for account in filtered_accounts {
                    add_account_to_map(&mut accounts_ret, account, remote).await;
                }
            }

            let response = InternalServiceResponse::GetAccountInformationResponse(Accounts {
                accounts: accounts_ret,
                request_id: Some(request_id),
            });

            send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
        }
        // Return all the sessions for the given TCP connection
        InternalServiceRequest::GetSessions { request_id } => {
            let lock = server_connection_map.lock().await;
            let mut sessions = Vec::new();
            for (cid, connection) in lock.iter() {
                if connection.associated_tcp_connection == uuid {
                    let mut session = SessionInformation {
                        cid: *cid,
                        peer_connections: HashMap::new(),
                    };
                    for (peer_cid, conn) in connection.peers.iter() {
                        session.peer_connections.insert(
                            *peer_cid,
                            PeerSessionInformation {
                                cid: *cid,
                                peer_cid: *peer_cid,
                                peer_username: conn
                                    .remote
                                    .target_username()
                                    .cloned()
                                    .unwrap_or_default(),
                            },
                        );
                    }
                    sessions.push(session);
                }
            }

            let response = InternalServiceResponse::GetSessionsResponse(GetSessionsResponse {
                sessions,
                request_id: Some(request_id),
            });
            send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
        }
        InternalServiceRequest::Connect {
            username,
            password,
            connect_mode,
            udp_mode,
            keep_alive_timeout,
            session_security_settings,
            request_id,
        } => {
            match remote
                .connect(
                    AuthenticationRequest::credentialed(username, password),
                    connect_mode,
                    udp_mode,
                    keep_alive_timeout,
                    session_security_settings,
                )
                .await
            {
                Ok(conn_success) => {
                    let cid = conn_success.cid;

                    let (sink, mut stream) = conn_success.channel.split();
                    let client_server_remote = create_client_server_remote(
                        stream.vconn_type,
                        remote.clone(),
                        session_security_settings,
                    );
                    let connection_struct = Connection::new(sink, client_server_remote, uuid);
                    server_connection_map
                        .lock()
                        .await
                        .insert(cid, connection_struct);

                    let hm_for_conn = tcp_connection_map.clone();

                    let response = InternalServiceResponse::ConnectSuccess(
                        citadel_internal_service_types::ConnectSuccess {
                            cid,
                            request_id: Some(request_id),
                        },
                    );

                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message =
                                InternalServiceResponse::MessageNotification(MessageNotification {
                                    message: message.into_buffer(),
                                    cid,
                                    peer_cid: 0,
                                    request_id: Some(request_id),
                                });
                            match hm_for_conn.lock().await.get(&uuid) {
                                Some(entry) => match entry.send(message) {
                                    Ok(res) => res,
                                    Err(_) => info!(target: "citadel", "tx not sent"),
                                },
                                None => {
                                    info!(target:"citadel","Hash map connection not found")
                                }
                            }
                        }
                    };
                    tokio::spawn(connection_read_stream);
                }

                Err(err) => {
                    let response = InternalServiceResponse::ConnectFailure(ConnectFailure {
                        message: err.into_string(),
                        request_id: Some(request_id),
                    });
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }
            };
        }
        InternalServiceRequest::Register {
            server_addr,
            full_name,
            username,
            proposed_password,
            connect_after_register,
            session_security_settings,
            request_id,
        } => {
            match remote
                .register(
                    server_addr,
                    full_name,
                    username.clone(),
                    proposed_password.clone(),
                    session_security_settings,
                )
                .await
            {
                Ok(_res) => match connect_after_register {
                    false => {
                        let response = InternalServiceResponse::RegisterSuccess(
                            citadel_internal_service_types::RegisterSuccess {
                                request_id: Some(request_id),
                            },
                        );
                        send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                    }
                    true => {
                        let connect_command = InternalServiceRequest::Connect {
                            username,
                            password: proposed_password,
                            keep_alive_timeout: None,
                            udp_mode: Default::default(),
                            connect_mode: Default::default(),
                            session_security_settings,
                            request_id,
                        };

                        let server_connection_map = server_connection_map.clone();
                        let mut remote = remote.clone();
                        let tcp_connection_map = tcp_connection_map.clone();
                        tokio::task::spawn(async move {
                            handle_request(
                                connect_command,
                                uuid,
                                &server_connection_map,
                                &mut remote,
                                &tcp_connection_map,
                            )
                            .await
                        });
                    }
                },
                Err(err) => {
                    citadel_logging::error!(target: "citadel", "Failure on register: {err:?}");
                    let response = InternalServiceResponse::RegisterFailure(
                        citadel_internal_service_types::RegisterFailure {
                            message: err.into_string(),
                            request_id: Some(request_id),
                        },
                    );
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                }
            };
        }
        InternalServiceRequest::Message {
            message,
            cid,
            peer_cid,
            security_level,
            request_id,
        } => {
            match server_connection_map.lock().await.get_mut(&cid) {
                Some(conn) => {
                    if let Some(peer_cid) = peer_cid {
                        // send to peer
                        if let Some(peer_conn) = conn.peers.get_mut(&peer_cid) {
                            peer_conn.sink.set_security_level(security_level);
                            // TODO no unwraps on send_message. We need to handle errors properly
                            peer_conn.sink.send_message(message.into()).await.unwrap();
                        } else {
                            // TODO: refactor all connection not found messages, we have too many duplicates
                            info!(target: "citadel","connection not found");
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                                    cid,
                                    message: format!("Connection for {cid} not found"),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    } else {
                        // send to server
                        conn.sink_to_server.set_security_level(security_level);
                        conn.sink_to_server
                            .send_message(message.into())
                            .await
                            .unwrap();
                    }

                    let response =
                        InternalServiceResponse::MessageSendSuccess(MessageSendSuccess {
                            cid,
                            peer_cid,
                            request_id: Some(request_id),
                        });
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                    info!(target: "citadel", "Into the message handler command send")
                }
                None => {
                    info!(target: "citadel","connection not found");
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::MessageSendFailure(MessageSendFailure {
                            cid,
                            message: format!("Connection for {cid} not found"),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            };
        }

        InternalServiceRequest::Disconnect { cid, request_id } => {
            let request = NodeRequest::DisconnectFromHypernode(DisconnectFromHypernode {
                implicated_cid: cid,
            });
            server_connection_map.lock().await.remove(&cid);
            match remote.send(request).await {
                Ok(res) => {
                    let disconnect_success =
                        InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                            cid,
                            peer_cid: None,
                            request_id: Some(request_id),
                        });
                    send_response_to_tcp_client(tcp_connection_map, disconnect_success, uuid).await;
                    info!(target: "citadel", "Disconnected {res:?}")
                }
                Err(err) => {
                    let error_message = format!("Failed to disconnect {err:?}");
                    info!(target: "citadel", "{error_message}");
                    let disconnect_failure =
                        InternalServiceResponse::DisconnectFailure(DisconnectFailure {
                            cid,
                            message: error_message,
                            request_id: Some(request_id),
                        });
                    send_response_to_tcp_client(tcp_connection_map, disconnect_failure, uuid).await;
                }
            };
        }

        InternalServiceRequest::SendFile {
            source,
            cid,
            peer_cid,
            chunk_size,
            transfer_type,
            request_id,
        } => {
            let lock = server_connection_map.lock().await;
            match lock.get(&cid) {
                Some(conn) => {
                    let result = if let Some(peer_cid) = peer_cid {
                        if let Some(peer_remote) = conn.peers.get(&peer_cid) {
                            let peer_remote = &peer_remote.remote.clone();
                            drop(lock);
                            let scan_for_status = |status| async move {
                                match status {
                                    NodeResult::ObjectTransferHandle(ObjectTransferHandle {
                                        ticket: _ticket,
                                        mut handle,
                                    }) => {
                                        let original_target_cid = handle.receiver;
                                        let original_source_cid = handle.source;
                                        let handle_metadata = handle.metadata.clone();

                                        if let ObjectTransferOrientation::Receiver {
                                            is_revfs_pull: _,
                                        } = handle.orientation
                                        {
                                            if original_target_cid == peer_cid {
                                                let mut lock = server_connection_map.lock().await;
                                                // Route the signal to the intra-kernel user if possible
                                                if let Some(peer) =
                                                    lock.get_mut(&original_target_cid)
                                                {
                                                    let peer_uuid = peer.associated_tcp_connection;
                                                    send_response_to_tcp_client(
                                                        tcp_connection_map,
                                                        InternalServiceResponse::FileTransferRequestNotification(
                                                            FileTransferRequestNotification {
                                                                cid: original_target_cid,
                                                                peer_cid: original_source_cid,
                                                                metadata: handle_metadata.clone(),
                                                            },
                                                        ),
                                                        peer_uuid,
                                                    )
                                                        .await;
                                                    peer.add_object_transfer_handler(
                                                        cid,
                                                        handle_metadata.object_id,
                                                        Some(handle),
                                                    );
                                                    return Ok(Some(()));
                                                }
                                            }
                                            return Ok(None);
                                        } else {
                                            while let Some(res) = handle.next().await {
                                                log::trace!(target: "citadel", "Client received RES {:?}", res);
                                                if let ObjectTransferStatus::TransferComplete = res
                                                {
                                                    return Ok(Some(()));
                                                }
                                            }
                                        }
                                        Err(NetworkError::msg("Failed To Receive TransferComplete Response During FileTransfer"))
                                    }

                                    res => {
                                        log::error!(target: "citadel", "Invalid NodeResult for FileTransfer request received: {:?}", res);
                                        Err(NetworkError::msg(
                                            "Invalid Response Received During FileTransfer",
                                        ))
                                    }
                                }
                            };
                            peer_remote
                                .send_file_with_custom_opts_and_with_fn(
                                    source,
                                    chunk_size.unwrap_or_default(),
                                    transfer_type,
                                    scan_for_status,
                                )
                                .await
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        conn.client_server_remote
                            .send_file_with_custom_opts(
                                source,
                                chunk_size.unwrap_or_default(),
                                transfer_type,
                            )
                            .await
                    };

                    match result {
                        Ok(_) => {
                            info!(target: "citadel","InternalServiceRequest Send File Success");
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::SendFileRequestSuccess(
                                    SendFileRequestSuccess {
                                        cid,
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }

                        Err(err) => {
                            info!(target: "citadel","InternalServiceRequest Send File Failure");
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::SendFileRequestFailure(
                                    SendFileRequestFailure {
                                        cid,
                                        message: err.into_string(),
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }
                    }
                }
                None => {
                    info!(target: "citadel","server connection not found");
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                            cid,
                            message: "Server Connection Not Found".into(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            };
        }

        InternalServiceRequest::RespondFileTransfer {
            cid,
            peer_cid,
            object_id,
            accept,
            download_location: _,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(mut handler) = connection.take_file_transfer_handle(peer_cid, object_id)
                {
                    if let Some(mut owned_handler) = handler.take() {
                        let result = if accept {
                            let accept_result = owned_handler.accept();
                            spawn_tick_updater(
                                owned_handler,
                                cid,
                                peer_cid,
                                &mut server_connection_map,
                                tcp_connection_map.clone(),
                            );

                            accept_result
                        } else {
                            owned_handler.decline()
                        };
                        match result {
                            Ok(_) => {
                                send_response_to_tcp_client(
                                    tcp_connection_map,
                                    InternalServiceResponse::FileTransferStatusNotification(
                                        FileTransferStatusNotification {
                                            cid,
                                            object_id,
                                            success: true,
                                            response: accept,
                                            message: None,
                                            request_id: Some(request_id),
                                        },
                                    ),
                                    uuid,
                                )
                                .await;
                            }

                            Err(err) => {
                                send_response_to_tcp_client(
                                    tcp_connection_map,
                                    InternalServiceResponse::FileTransferStatusNotification(
                                        FileTransferStatusNotification {
                                            cid,
                                            object_id,
                                            success: false,
                                            response: accept,
                                            message: Option::from(err.into_string()),
                                            request_id: Some(request_id),
                                        },
                                    ),
                                    uuid,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        }

        InternalServiceRequest::DownloadFile {
            virtual_directory,
            security_level,
            delete_on_pull,
            cid,
            peer_cid,
            request_id,
        } => {
            let security_level = security_level.unwrap_or_default();

            match server_connection_map.lock().await.get_mut(&cid) {
                Some(conn) => {
                    let result = if let Some(peer_cid) = peer_cid {
                        if let Some(peer_remote) = conn.peers.get_mut(&peer_cid) {
                            let request = NodeRequest::PullObject(PullObject {
                                v_conn: *peer_remote.remote.user(),
                                virtual_dir: virtual_directory,
                                delete_on_pull,
                                transfer_security_level: security_level,
                            });

                            peer_remote.remote.send(request).await
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        let request = NodeRequest::PullObject(PullObject {
                            v_conn: *conn.client_server_remote.user(),
                            virtual_dir: virtual_directory,
                            delete_on_pull,
                            transfer_security_level: security_level,
                        });

                        conn.client_server_remote.send(request).await
                    };

                    match result {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess {
                                    cid,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }

                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                                    cid,
                                    message: err.into_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                }
                None => {
                    info!(target: "citadel","server connection not found");
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                            cid,
                            message: String::from("Server Connection Not Found"),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            };
        }

        InternalServiceRequest::DeleteVirtualFile {
            virtual_directory,
            cid,
            peer_cid,
            request_id,
        } => {
            match server_connection_map.lock().await.get_mut(&cid) {
                Some(conn) => {
                    let result = if let Some(peer_cid) = peer_cid {
                        if let Some(_peer_remote) = conn.peers.get_mut(&peer_cid) {
                            let request = NodeRequest::DeleteObject(DeleteObject {
                                v_conn: VirtualTargetType::LocalGroupPeer {
                                    implicated_cid: cid,
                                    peer_cid,
                                },
                                virtual_dir: virtual_directory,
                                security_level: Default::default(),
                            });
                            remote.send(request).await
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        let request = NodeRequest::DeleteObject(DeleteObject {
                            v_conn: VirtualTargetType::LocalGroupServer {
                                implicated_cid: cid,
                            },
                            virtual_dir: virtual_directory,
                            security_level: Default::default(),
                        });
                        remote.send(request).await
                    };

                    match result {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::DeleteVirtualFileSuccess(
                                    DeleteVirtualFileSuccess {
                                        cid,
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }

                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::DeleteVirtualFileFailure(
                                    DeleteVirtualFileFailure {
                                        cid,
                                        message: err.into_string(),
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }
                    }
                }
                None => {
                    info!(target: "citadel","server connection not found");
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::DeleteVirtualFileFailure(
                            DeleteVirtualFileFailure {
                                cid,
                                message: String::from("Server Connection Not Found"),
                                request_id: Some(request_id),
                            },
                        ),
                        uuid,
                    )
                    .await;
                }
            };
        }

        InternalServiceRequest::PeerRegister {
            cid,
            peer_cid,
            connect_after_register,
            session_security_settings,
            request_id,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
                session_security_settings,
            );

            let remote_for_scanner = &remote;
            let scan_for_status = |status| async move {
                if let NodeResult::PeerEvent(PeerEvent {
                    event:
                        PeerSignal::PostRegister {
                            peer_conn_type,
                            inviter_username: _,
                            invitee_username: _,
                            ticket_opt: _,
                            invitee_response: resp,
                        },
                    ticket: _,
                }) = status
                {
                    match resp {
                        Some(PeerResponse::Accept(..)) => Ok(Some(PeerRegisterStatus::Accepted)),
                        Some(PeerResponse::Decline) => Ok(Some(PeerRegisterStatus::Declined)),
                        Some(PeerResponse::Timeout) => Ok(Some(PeerRegisterStatus::Failed { reason: Some("Timeout on register request. Peer did not accept in time. Try again later".to_string()) })),
                        _ => {
                            // This may be a signal needing to be routed
                            if peer_conn_type.get_original_target_cid() == peer_cid {
                                // If the target peer is the same as the peer_cid, that means that
                                // the peer is in the same kernel space. Route the signal through the TCP client
                                let lock = server_connection_map.lock().await;
                                if let Some(peer) = lock.get(&peer_cid) {
                                    let username_of_sender = remote_for_scanner.account_manager().get_username_by_cid(cid).await
                                        .map_err(|err| NetworkError::msg(format!("Unable to get username for cid: {cid}. Error: {err:?}")))?
                                        .ok_or_else(|| NetworkError::msg(format!("Unable to get username for cid: {cid}")))?;

                                    let peer_uuid = peer.associated_tcp_connection;
                                    drop(lock);

                                    log::debug!(target: "citadel", "Routing signal meant for other peer in intra-kernel space");
                                    send_response_to_tcp_client(
                                        tcp_connection_map,
                                        InternalServiceResponse::PeerRegisterNotification(
                                            PeerRegisterNotification {
                                                cid: peer_cid,
                                                peer_cid: cid,
                                                peer_username: username_of_sender,
                                                request_id: Some(request_id),
                                            },
                                        ),
                                        peer_uuid
                                    ).await
                                }
                            }

                            Ok(None)
                        }
                    }
                } else {
                    Ok(None)
                }
            };

            match client_to_server_remote.propose_target(cid, peer_cid).await {
                Ok(symmetric_identifier_handle_ref) => {
                    match symmetric_identifier_handle_ref
                        .register_to_peer_with_fn(scan_for_status)
                        .await
                    {
                        Ok(_peer_register_success) => {
                            let account_manager = symmetric_identifier_handle_ref.account_manager();
                            if let Ok(target_information) =
                                account_manager.find_target_information(cid, peer_cid).await
                            {
                                let (_, mutual_peer) = target_information.unwrap();
                                match connect_after_register {
                                    true => {
                                        let connect_command = InternalServiceRequest::PeerConnect {
                                            cid,
                                            peer_cid: mutual_peer.cid,
                                            udp_mode: Default::default(),
                                            session_security_settings,
                                            request_id,
                                        };

                                        let server_connection_map = server_connection_map.clone();
                                        let mut remote = remote.clone();
                                        let tcp_connection_map = tcp_connection_map.clone();
                                        tokio::task::spawn(async move {
                                            handle_request(
                                                connect_command,
                                                uuid,
                                                &server_connection_map,
                                                &mut remote,
                                                &tcp_connection_map,
                                            )
                                            .await;
                                        });
                                    }
                                    false => {
                                        send_response_to_tcp_client(
                                            tcp_connection_map,
                                            InternalServiceResponse::PeerRegisterSuccess(
                                                PeerRegisterSuccess {
                                                    cid,
                                                    peer_cid: mutual_peer.cid,
                                                    peer_username: mutual_peer
                                                        .username
                                                        .clone()
                                                        .unwrap_or_default(),
                                                    request_id: Some(request_id),
                                                },
                                            ),
                                            uuid,
                                        )
                                        .await;
                                    }
                                }
                            }
                        }

                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::PeerRegisterFailure(PeerRegisterFailure {
                                    cid,
                                    message: err.into_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::PeerRegisterFailure(PeerRegisterFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServiceRequest::PeerConnect {
            cid,
            peer_cid,
            udp_mode,
            session_security_settings,
            request_id,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupPeer {
                    implicated_cid: cid,
                    peer_cid,
                },
                remote.clone(),
                session_security_settings,
            );

            match client_to_server_remote.find_target(cid, peer_cid).await {
                Ok(symmetric_identifier_handle) => {
                    let symmetric_identifier_handle = &symmetric_identifier_handle.into_owned();

                    let remote_for_scanner = &remote;
                    let signal_scanner = |status| async move {
                        match status {
                            NodeResult::PeerChannelCreated(PeerChannelCreated {
                                ticket: _,
                                channel,
                                udp_rx_opt,
                            }) => {
                                let username =
                                    symmetric_identifier_handle.target_username().cloned();

                                let remote = PeerRemote {
                                    inner: (*remote_for_scanner).clone(),
                                    peer: symmetric_identifier_handle
                                        .try_as_peer_connection()
                                        .await?
                                        .as_virtual_connection(),
                                    username,
                                    session_security_settings,
                                };

                                Ok(Some(results::PeerConnectSuccess {
                                    remote,
                                    channel,
                                    udp_channel_rx: udp_rx_opt,
                                    incoming_object_transfer_handles: None,
                                }))
                            }

                            NodeResult::PeerEvent(PeerEvent {
                                event:
                                    PeerSignal::PostConnect {
                                        peer_conn_type: _,
                                        ticket_opt: _,
                                        invitee_response: Some(PeerResponse::Decline),
                                        ..
                                    },
                                ..
                            }) => Err(NetworkError::msg(format!(
                                "Peer {peer_cid} declined connection"
                            ))),

                            NodeResult::PeerEvent(PeerEvent {
                                event:
                                    PeerSignal::PostConnect {
                                        peer_conn_type,
                                        ticket_opt: _,
                                        invitee_response: None,
                                        ..
                                    },
                                ..
                            }) => {
                                // Route the signal to the intra-kernel user
                                if peer_conn_type.get_original_target_cid() == peer_cid {
                                    let lock = server_connection_map.lock().await;
                                    if let Some(conn) = lock.get(&peer_cid) {
                                        let peer_uuid = conn.associated_tcp_connection;
                                        send_response_to_tcp_client(
                                            tcp_connection_map,
                                            InternalServiceResponse::PeerConnectNotification(
                                                PeerConnectNotification {
                                                    cid: peer_cid,
                                                    peer_cid: cid,
                                                    session_security_settings,
                                                    udp_mode,
                                                    request_id: Some(request_id),
                                                },
                                            ),
                                            peer_uuid,
                                        )
                                        .await
                                    }
                                }

                                Ok(None)
                            }

                            _ => Ok(None),
                        }
                    };

                    match symmetric_identifier_handle
                        .connect_to_peer_with_fn(
                            session_security_settings,
                            udp_mode,
                            signal_scanner,
                        )
                        .await
                    {
                        Ok(peer_connect_success) => {
                            let (sink, mut stream) = peer_connect_success.channel.split();
                            server_connection_map
                                .lock()
                                .await
                                .get_mut(&cid)
                                .unwrap()
                                .add_peer_connection(
                                    peer_cid,
                                    sink,
                                    symmetric_identifier_handle.clone(),
                                );

                            let hm_for_conn = tcp_connection_map.clone();

                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess {
                                    cid,
                                    peer_cid,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                            let connection_read_stream = async move {
                                while let Some(message) = stream.next().await {
                                    let message = InternalServiceResponse::MessageNotification(
                                        MessageNotification {
                                            message: message.into_buffer(),
                                            cid,
                                            peer_cid,
                                            request_id: Some(request_id),
                                        },
                                    );
                                    match hm_for_conn.lock().await.get(&uuid) {
                                        Some(entry) => match entry.send(message) {
                                            Ok(res) => res,
                                            Err(_) => {
                                                // TODO: tell the TCP connection that we couldn't send the message
                                                // and, we need to remove the connection from the hashmap
                                                error!(target: "citadel", "tx not sent")
                                            }
                                        },
                                        None => {
                                            info!(target:"citadel","Hash map connection not found")
                                        }
                                    }
                                }
                            };
                            tokio::spawn(connection_read_stream);
                        }

                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::PeerConnectFailure(PeerConnectFailure {
                                    cid,
                                    message: err.into_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::PeerConnectFailure(PeerConnectFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }
        InternalServiceRequest::PeerDisconnect {
            cid,
            peer_cid,
            request_id,
        } => {
            let request = NodeRequest::PeerCommand(PeerCommand {
                implicated_cid: cid,
                command: PeerSignal::Disconnect {
                    peer_conn_type: PeerConnectionType::LocalGroupPeer {
                        implicated_cid: cid,
                        peer_cid,
                    },
                    disconnect_response: None,
                },
            });

            match server_connection_map.lock().await.get_mut(&cid) {
                None => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
                            cid,
                            message: "Server connection not found".to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
                Some(conn) => match conn.peers.get_mut(&cid) {
                    None => {
                        // TODO: handle none case
                    }
                    Some(target_peer) => match target_peer.remote.send(request).await {
                        Ok(ticket) => {
                            conn.clear_peer_connection(peer_cid);
                            let peer_disconnect_success =
                                InternalServiceResponse::PeerDisconnectSuccess(
                                    PeerDisconnectSuccess {
                                        cid,
                                        request_id: Some(request_id),
                                    },
                                );
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                peer_disconnect_success,
                                uuid,
                            )
                            .await;
                            info!(target: "citadel", "Disconnected Peer{ticket:?}")
                        }
                        Err(network_error) => {
                            let error_message = format!("Failed to disconnect {network_error:?}");
                            info!(target: "citadel", "{error_message}");
                            let peer_disconnect_failure =
                                InternalServiceResponse::PeerDisconnectFailure(
                                    PeerDisconnectFailure {
                                        cid,
                                        message: error_message,
                                        request_id: Some(request_id),
                                    },
                                );
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                peer_disconnect_failure,
                                uuid,
                            )
                            .await;
                        }
                    },
                },
            }
        }
        InternalServiceRequest::LocalDBGetKV {
            cid,
            peer_cid,
            key,
            request_id,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
            Some(conn) => {
                if let Some(peer_cid) = peer_cid {
                    if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                        backend_handler_get(
                            &peer.remote,
                            tcp_connection_map,
                            uuid,
                            cid,
                            Some(peer_cid),
                            key,
                            Some(request_id),
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                                cid,
                                peer_cid: Some(peer_cid),
                                message: "Peer connection not found".to_string(),
                                request_id: Some(request_id),
                            }),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    backend_handler_get(
                        &conn.client_server_remote,
                        tcp_connection_map,
                        uuid,
                        cid,
                        peer_cid,
                        key,
                        Some(request_id),
                    )
                    .await;
                }
            }
        },
        InternalServiceRequest::LocalDBSetKV {
            cid,
            peer_cid,
            key,
            value,
            request_id,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
            Some(conn) => {
                if let Some(peer_cid) = peer_cid {
                    if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                        backend_handler_set(
                            &peer.remote,
                            tcp_connection_map,
                            uuid,
                            cid,
                            Some(peer_cid),
                            key,
                            value,
                            Some(request_id),
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
                                cid,
                                peer_cid: Some(peer_cid),
                                message: "Peer connection not found".to_string(),
                                request_id: Some(request_id),
                            }),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    backend_handler_set(
                        &conn.client_server_remote,
                        tcp_connection_map,
                        uuid,
                        cid,
                        peer_cid,
                        key,
                        value,
                        Some(request_id),
                    )
                    .await;
                }
            }
        },
        InternalServiceRequest::LocalDBDeleteKV {
            cid,
            peer_cid,
            key,
            request_id,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBDeleteKVFailure(LocalDBDeleteKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
            Some(conn) => {
                if let Some(peer_cid) = peer_cid {
                    if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                        backend_handler_delete(
                            &peer.remote,
                            tcp_connection_map,
                            uuid,
                            cid,
                            Some(peer_cid),
                            key,
                            Some(request_id),
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBDeleteKVFailure(
                                LocalDBDeleteKVFailure {
                                    cid,
                                    peer_cid: Some(peer_cid),
                                    message: "Peer connection not found".to_string(),
                                    request_id: Some(request_id),
                                },
                            ),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    backend_handler_delete(
                        &conn.client_server_remote,
                        tcp_connection_map,
                        uuid,
                        cid,
                        peer_cid,
                        key,
                        Some(request_id),
                    )
                    .await;
                }
            }
        },
        InternalServiceRequest::LocalDBGetAllKV {
            cid,
            peer_cid,
            request_id,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetAllKVFailure(LocalDBGetAllKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
            Some(conn) => {
                if let Some(peer_cid) = peer_cid {
                    if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                        backend_handler_get_all(
                            &peer.remote,
                            tcp_connection_map,
                            uuid,
                            cid,
                            Some(peer_cid),
                            Some(request_id),
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBGetAllKVFailure(
                                LocalDBGetAllKVFailure {
                                    cid,
                                    peer_cid: Some(peer_cid),
                                    message: "Peer connection not found".to_string(),
                                    request_id: Some(request_id),
                                },
                            ),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    backend_handler_get_all(
                        &conn.client_server_remote,
                        tcp_connection_map,
                        uuid,
                        cid,
                        peer_cid,
                        Some(request_id),
                    )
                    .await;
                }
            }
        },
        InternalServiceRequest::LocalDBClearAllKV {
            cid,
            peer_cid,
            request_id,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBClearAllKVFailure(LocalDBClearAllKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
            Some(conn) => {
                if let Some(peer_cid) = peer_cid {
                    if let Some(peer) = conn.peers.get_mut(&peer_cid) {
                        backend_handler_clear_all(
                            &peer.remote,
                            tcp_connection_map,
                            uuid,
                            cid,
                            Some(peer_cid),
                            Some(request_id),
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBClearAllKVFailure(
                                LocalDBClearAllKVFailure {
                                    cid,
                                    peer_cid: Some(peer_cid),
                                    message: "Peer connection not found".to_string(),
                                    request_id: Some(request_id),
                                },
                            ),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    backend_handler_clear_all(
                        &conn.client_server_remote,
                        tcp_connection_map,
                        uuid,
                        cid,
                        peer_cid,
                        Some(request_id),
                    )
                    .await;
                }
            }
        },

        InternalServiceRequest::GroupCreate {
            cid,
            request_id,
            initial_users_to_invite,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
                Default::default(),
            );
            match client_to_server_remote
                .create_group(initial_users_to_invite)
                .await
            {
                Ok(group_channel) => {
                    // Store the group connection in map
                    let key = group_channel.key();
                    let group_cid = group_channel.cid();
                    let (tx, rx) = group_channel.split();
                    match server_connection_map.lock().await.get_mut(&cid) {
                        Some(conn) => {
                            conn.add_group_channel(
                                key,
                                GroupConnection {
                                    key,
                                    cid: group_cid,
                                    tx,
                                },
                            );

                            let uuid = conn.associated_tcp_connection;
                            spawn_group_channel_receiver(
                                key,
                                cid,
                                uuid,
                                rx,
                                tcp_connection_map.clone(),
                            );

                            // Relay success to TCP client
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
                                    cid,
                                    group_key: key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }

                        None => {
                            todo!()
                        }
                    }
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupCreateFailure(GroupCreateFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServiceRequest::GroupLeave {
            cid,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(group_connection) = connection.groups.get_mut(&group_key) {
                    let group_sender = group_connection.tx.clone();
                    drop(server_connection_map);
                    match group_sender.leave().await {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupLeaveSuccess(GroupLeaveSuccess {
                                    cid,
                                    group_key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupLeaveFailure(GroupLeaveFailure {
                                    cid,
                                    message: err.into_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupLeaveFailure(GroupLeaveFailure {
                            cid,
                            message: "Could Not Leave Group - Group Connection not found"
                                .to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupLeaveFailure(GroupLeaveFailure {
                        cid,
                        message: "Could Not Leave Group - Connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupEnd {
            cid,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(group_connection) = connection.groups.get_mut(&group_key) {
                    let group_sender = group_connection.tx.clone();
                    drop(server_connection_map);
                    match group_sender.end().await {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupEndSuccess(GroupEndSuccess {
                                    cid,
                                    group_key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupEndFailure(GroupEndFailure {
                                    cid,
                                    message: err.into_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupEndFailure(GroupEndFailure {
                            cid,
                            message: "Could Not Leave Group - Group Connection not found"
                                .to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupEndFailure(GroupEndFailure {
                        cid,
                        message: "Could Not Leave Group - Connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupMessage {
            cid,
            message,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(group_connection) = connection.groups.get_mut(&group_key) {
                    let group_sender = group_connection.tx.clone();
                    drop(server_connection_map);
                    match group_sender.send_message(message.into()).await {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupMessageSuccess(GroupMessageSuccess {
                                    cid,
                                    group_key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupMessageFailure(GroupMessageFailure {
                                    cid,
                                    message: err.to_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupMessageFailure(GroupMessageFailure {
                            cid,
                            message: "Could Not Message Group - Group Connection not found"
                                .to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupMessageFailure(GroupMessageFailure {
                        cid,
                        message: "Could Not Message Group - Connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupInvite {
            cid,
            peer_cid,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(group_connection) = connection.groups.get_mut(&group_key) {
                    let group_sender = group_connection.tx.clone();
                    drop(server_connection_map);
                    match group_sender.invite(peer_cid).await {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupInviteSuccess(GroupInviteSuccess {
                                    cid,
                                    group_key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
                                    cid,
                                    message: err.to_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
                            cid,
                            message: "Could Not Invite to Group - Group Connection not found"
                                .to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupInviteFailure(GroupInviteFailure {
                        cid,
                        message: "Could Not Invite to Group - Connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupKick {
            cid,
            peer_cid,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(group_connection) = connection.groups.get_mut(&group_key) {
                    let group_sender = group_connection.tx.clone();
                    drop(server_connection_map);
                    match group_sender.kick(peer_cid).await {
                        Ok(_) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupKickSuccess(GroupKickSuccess {
                                    cid,
                                    group_key,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                                    cid,
                                    message: err.to_string(),
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                            cid,
                            message: "Could Not Kick from Group - GroupChannel not found"
                                .to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                        cid,
                        message: "Could Not Kick from Group - GroupChannel not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupListGroupsFor {
            cid,
            peer_cid,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(peer_connection) = connection.peers.get_mut(&peer_cid) {
                    let peer_remote = peer_connection.remote.clone();
                    drop(server_connection_map);
                    let request = NodeRequest::GroupBroadcastCommand(GroupBroadcastCommand {
                        implicated_cid: cid,
                        command: GroupBroadcast::ListGroupsFor { cid: peer_cid },
                    });
                    if let Ok(mut subscription) =
                        peer_remote.send_callback_subscription(request).await
                    {
                        if let Some(evt) = subscription.next().await {
                            if let NodeResult::GroupEvent(GroupEvent {
                                implicated_cid: _,
                                ticket: _,
                                event: GroupBroadcast::ListResponse { groups },
                            }) = evt
                            {
                                send_response_to_tcp_client(
                                    tcp_connection_map,
                                    InternalServiceResponse::GroupListGroupsSuccess(
                                        GroupListGroupsSuccess {
                                            cid,
                                            peer_cid,
                                            group_list: Some(groups),
                                            request_id: Some(request_id),
                                        },
                                    ),
                                    uuid,
                                )
                                .await;
                            } else {
                                send_response_to_tcp_client(
                                    tcp_connection_map,
                                    InternalServiceResponse::GroupListGroupsFailure(
                                        GroupListGroupsFailure {
                                            cid,
                                            message: "Could Not List Groups - Failed".to_string(),
                                            request_id: Some(request_id),
                                        },
                                    ),
                                    uuid,
                                )
                                .await;
                            }
                        } else {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupListGroupsFailure(
                                    GroupListGroupsFailure {
                                        cid,
                                        message: "Could Not List Groups - Subscription Error"
                                            .to_string(),
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::GroupListGroupsFailure(
                                GroupListGroupsFailure {
                                    cid,
                                    message: "Could Not List Groups - Subscription Error"
                                        .to_string(),
                                    request_id: Some(request_id),
                                },
                            ),
                            uuid,
                        )
                        .await;
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
                            cid,
                            message: "Could Not List Groups - Peer not found".to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupListGroupsFailure(GroupListGroupsFailure {
                        cid,
                        message: "Could Not List Groups - Connection not found".to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }

        InternalServiceRequest::GroupRespondRequest {
            cid,
            peer_cid,
            group_key,
            response,
            request_id,
            invitation,
        } => {
            let group_request = if response {
                GroupBroadcast::AcceptMembership {
                    target: if invitation { cid } else { peer_cid },
                    key: group_key,
                }
            } else {
                GroupBroadcast::DeclineMembership {
                    target: if invitation { cid } else { peer_cid },
                    key: group_key,
                }
            };
            let request = NodeRequest::GroupBroadcastCommand(GroupBroadcastCommand {
                implicated_cid: cid,
                command: group_request,
            });
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                if let Some(peer_connection) = connection.peers.get_mut(&peer_cid) {
                    let peer_remote = peer_connection.remote.clone();
                    match peer_remote.send_callback_subscription(request).await {
                        Ok(mut subscription) => {
                            let mut result = false;
                            if invitation {
                                while let Some(evt) = subscription.next().await {
                                    match evt {
                                        // When accepting an invite, we expect a GroupChannelCreated in response
                                        NodeResult::GroupChannelCreated(GroupChannelCreated {
                                            ticket: _,
                                            channel,
                                        }) => {
                                            let key = channel.key();
                                            let group_cid = channel.cid();
                                            let (tx, rx) = channel.split();
                                            connection.add_group_channel(
                                                key,
                                                GroupConnection {
                                                    key,
                                                    tx,
                                                    cid: group_cid,
                                                },
                                            );

                                            let uuid = connection.associated_tcp_connection;
                                            spawn_group_channel_receiver(
                                                key,
                                                cid,
                                                uuid,
                                                rx,
                                                tcp_connection_map.clone(),
                                            );

                                            result = true;
                                            break;
                                        }
                                        NodeResult::GroupEvent(GroupEvent {
                                            implicated_cid: _,
                                            ticket: _,
                                            event:
                                                GroupBroadcast::AcceptMembershipResponse {
                                                    key: _,
                                                    success,
                                                },
                                        }) => {
                                            result = success;
                                            // if !result {
                                            //     break;
                                            // }
                                            break;
                                        }
                                        NodeResult::GroupEvent(GroupEvent {
                                            implicated_cid: _,
                                            ticket: _,
                                            event:
                                                GroupBroadcast::DeclineMembershipResponse {
                                                    key: _,
                                                    success,
                                                },
                                        }) => {
                                            result = success;
                                            break;
                                        }
                                        _ => {}
                                    };
                                }
                                drop(server_connection_map);
                            } else {
                                // For now we return a Success response - we did, in fact, receive the KernelStreamSubscription
                                result = true;
                            }
                            match result {
                                true => {
                                    send_response_to_tcp_client(
                                        tcp_connection_map,
                                        InternalServiceResponse::GroupRespondRequestSuccess(
                                            GroupRespondRequestSuccess {
                                                cid,
                                                group_key,
                                                request_id: Some(request_id),
                                            },
                                        ),
                                        uuid,
                                    )
                                    .await;
                                }
                                false => {
                                    send_response_to_tcp_client(
                                        tcp_connection_map,
                                        InternalServiceResponse::GroupRespondRequestFailure(
                                            GroupRespondRequestFailure {
                                                cid,
                                                message: "Group Invite Response Failed."
                                                    .to_string(),
                                                request_id: Some(request_id),
                                            },
                                        ),
                                        uuid,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupRespondRequestFailure(
                                    GroupRespondRequestFailure {
                                        cid,
                                        message: err.to_string(),
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }
                    }
                }
            }
        }

        InternalServiceRequest::GroupRequestJoin {
            cid,
            group_key,
            request_id,
        } => {
            let mut server_connection_map = server_connection_map.lock().await;
            if let Some(connection) = server_connection_map.get_mut(&cid) {
                let target_cid = group_key.cid;
                if let Some(peer_connection) = connection.peers.get_mut(&target_cid) {
                    let peer_remote = peer_connection.remote.clone();
                    drop(server_connection_map);
                    let group_request = GroupBroadcast::RequestJoin {
                        sender: cid,
                        key: group_key,
                    };
                    let request = NodeRequest::GroupBroadcastCommand(GroupBroadcastCommand {
                        implicated_cid: cid,
                        command: group_request,
                    });
                    match peer_remote.send_callback_subscription(request).await {
                        Ok(mut subscription) => {
                            let mut result = Err("Group Request Join Failed".to_string());
                            while let Some(evt) = subscription.next().await {
                                if let NodeResult::GroupEvent(GroupEvent {
                                    implicated_cid: _,
                                    ticket: _,
                                    event:
                                        GroupBroadcast::RequestJoinPending {
                                            result: signal_result,
                                            key: _key,
                                        },
                                }) = evt
                                {
                                    result = signal_result;
                                    break;
                                }
                            }
                            match result {
                                Ok(_) => {
                                    send_response_to_tcp_client(
                                        tcp_connection_map,
                                        InternalServiceResponse::GroupRequestJoinSuccess(
                                            GroupRequestJoinSuccess {
                                                cid,
                                                group_key,
                                                request_id: Some(request_id),
                                            },
                                        ),
                                        uuid,
                                    )
                                    .await;
                                }
                                Err(err) => {
                                    send_response_to_tcp_client(
                                        tcp_connection_map,
                                        InternalServiceResponse::GroupRequestJoinFailure(
                                            GroupRequestJoinFailure {
                                                cid,
                                                message: err.to_string(),
                                                request_id: Some(request_id),
                                            },
                                        ),
                                        uuid,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(err) => {
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::GroupRequestJoinFailure(
                                    GroupRequestJoinFailure {
                                        cid,
                                        message: err.to_string(),
                                        request_id: Some(request_id),
                                    },
                                ),
                                uuid,
                            )
                            .await;
                        }
                    }
                } else {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupRequestJoinFailure(GroupRequestJoinFailure {
                            cid,
                            message: "Could not Request to join Group - Peer not found".to_string(),
                            request_id: Some(request_id),
                        }),
                        uuid,
                    )
                    .await;
                }
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::GroupRequestJoinFailure(GroupRequestJoinFailure {
                        cid,
                        message: "Could not Request to join Group - Connection not found"
                            .to_string(),
                        request_id: Some(request_id),
                    }),
                    uuid,
                )
                .await;
            }
        }
    }
}

async fn backend_handler_get(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
    request_id: Option<Uuid>,
) {
    match remote.get(&key).await {
        Ok(value) => {
            if let Some(value) = value {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetKVSuccess(LocalDBGetKVSuccess {
                        cid,
                        peer_cid,
                        key,
                        value,
                        request_id,
                    }),
                    uuid,
                )
                .await;
            } else {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                        cid,
                        peer_cid,
                        message: "Key not found".to_string(),
                        request_id,
                    }),
                    uuid,
                )
                .await;
            }
        }
        Err(err) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                    cid,
                    peer_cid,
                    message: err.into_string(),
                    request_id,
                }),
                uuid,
            )
            .await;
        }
    }
}

// backend_handler_set
#[allow(clippy::too_many_arguments)]
async fn backend_handler_set(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
    value: Vec<u8>,
    request_id: Option<Uuid>,
) {
    match remote.set(&key, value).await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBSetKVSuccess(LocalDBSetKVSuccess {
                    cid,
                    peer_cid,
                    key,
                    request_id,
                }),
                uuid,
            )
            .await;
        }
        Err(err) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
                    cid,
                    peer_cid,
                    message: err.into_string(),
                    request_id,
                }),
                uuid,
            )
            .await;
        }
    }
}

// backend handler delete
async fn backend_handler_delete(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
    request_id: Option<Uuid>,
) {
    match remote.remove(&key).await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBDeleteKVSuccess(LocalDBDeleteKVSuccess {
                    cid,
                    peer_cid,
                    key,
                    request_id,
                }),
                uuid,
            )
            .await;
        }
        Err(err) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBDeleteKVFailure(LocalDBDeleteKVFailure {
                    cid,
                    peer_cid,
                    message: err.into_string(),
                    request_id,
                }),
                uuid,
            )
            .await;
        }
    }
}

// backend handler get_all
async fn backend_handler_get_all(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    request_id: Option<Uuid>,
) {
    match remote.get_all().await {
        Ok(map) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBGetAllKVSuccess(LocalDBGetAllKVSuccess {
                    cid,
                    peer_cid,
                    map,
                    request_id,
                }),
                uuid,
            )
            .await;
        }
        Err(err) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBGetAllKVFailure(LocalDBGetAllKVFailure {
                    cid,
                    peer_cid,
                    message: err.into_string(),
                    request_id,
                }),
                uuid,
            )
            .await;
        }
    }
}

// backend handler clear all
async fn backend_handler_clear_all(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    request_id: Option<Uuid>,
) {
    match remote.remove_all().await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBClearAllKVSuccess(LocalDBClearAllKVSuccess {
                    cid,
                    peer_cid,
                    request_id,
                }),
                uuid,
            )
            .await;
        }
        Err(err) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBClearAllKVFailure(LocalDBClearAllKVFailure {
                    cid,
                    peer_cid,
                    message: err.into_string(),
                    request_id,
                }),
                uuid,
            )
            .await;
        }
    }
}

pub(crate) fn spawn_group_channel_receiver(
    group_key: MessageGroupKey,
    implicated_cid: u64,
    uuid: Uuid,
    mut rx: GroupChannelRecvHalf,
    tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    // Handler/Receiver for Group Channel Broadcasts that aren't handled in on_node_event_received in Kernel
    let group_channel_receiver = async move {
        while let Some(inbound_group_broadcast) = rx.next().await {
            // Gets UnboundedSender to the TCP client to forward Broadcasts
            match tcp_connection_map.lock().await.get(&uuid) {
                Some(entry) => {
                    log::trace!(target:"citadel", "User {implicated_cid:?} Received Group Broadcast: {inbound_group_broadcast:?}");
                    let message = match inbound_group_broadcast {
                        GroupBroadcastPayload::Message { payload, sender } => {
                            Some(InternalServiceResponse::GroupMessageNotification(
                                GroupMessageNotification {
                                    cid: implicated_cid,
                                    peer_cid: sender,
                                    message: payload.into_buffer(),
                                    group_key,
                                    request_id: None,
                                },
                            ))
                        }
                        GroupBroadcastPayload::Event { payload } => match payload {
                            GroupBroadcast::RequestJoin { sender, key: _ } => {
                                Some(InternalServiceResponse::GroupJoinRequestNotification(
                                    GroupJoinRequestNotification {
                                        cid: implicated_cid,
                                        peer_cid: sender,
                                        group_key,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::MemberStateChanged { key: _, state } => {
                                Some(InternalServiceResponse::GroupMemberStateChangeNotification(
                                    GroupMemberStateChangeNotification {
                                        cid: implicated_cid,
                                        group_key,
                                        state,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::EndResponse { key, success } => {
                                Some(InternalServiceResponse::GroupEndNotification(
                                    GroupEndNotification {
                                        cid: implicated_cid,
                                        group_key: key,
                                        success,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::Disconnected { key } => {
                                Some(InternalServiceResponse::GroupDisconnectNotification(
                                    GroupDisconnectNotification {
                                        cid: implicated_cid,
                                        group_key: key,
                                        request_id: None,
                                    },
                                ))
                            }
                            GroupBroadcast::MessageResponse { key, success } => {
                                Some(InternalServiceResponse::GroupMessageResponse(
                                    GroupMessageResponse {
                                        cid: implicated_cid,
                                        group_key: key,
                                        success,
                                        request_id: None,
                                    },
                                ))
                            }
                            // GroupBroadcast::Create { .. } => {},
                            // GroupBroadcast::LeaveRoom { .. } => {},
                            // GroupBroadcast::End { .. } => {},
                            // GroupBroadcast::Add { .. } => {},
                            // GroupBroadcast::AddResponse { .. } => {},
                            // GroupBroadcast::AcceptMembership { .. } => {},
                            // GroupBroadcast::DeclineMembership { .. } => {},
                            // GroupBroadcast::AcceptMembershipResponse { .. } => {},
                            // GroupBroadcast::DeclineMembershipResponse { .. } => {},
                            // GroupBroadcast::Kick { .. } => {},
                            // GroupBroadcast::KickResponse { .. } => {},
                            // GroupBroadcast::ListGroupsFor { .. } => {},
                            // GroupBroadcast::ListResponse { .. } => {},
                            // GroupBroadcast::Invitation { .. } => {},
                            // GroupBroadcast::CreateResponse { .. } => {},
                            // GroupBroadcast::RequestJoinPending { .. } => {},
                            _ => None,
                        },
                    };

                    // Forward Group Broadcast to TCP Client if it was one of the handled broadcasts
                    if let Some(message) = message {
                        if let Err(err) = entry.send(message) {
                            info!(target: "citadel", "Group Channel Forward To TCP Client Failed: {err:?}");
                        }
                    }
                }
                None => {
                    info!(target:"citadel","Connection not found when Group Channel Broadcast Received");
                }
            }
        }
    };

    // Spawns the above Handler for Group Channel Broadcasts not handled in Node Events
    tokio::task::spawn(group_channel_receiver);
}
