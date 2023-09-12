use crate::kernel::{create_client_server_remote, send_response_to_tcp_client, spawn_tick_updater, Connection, GroupConnection};
use async_recursion::async_recursion;
use citadel_logging::{error, info};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::*;
use citadel_workspace_types::{AccountInformation, Accounts, ConnectionFailure, DeleteVirtualFileFailure, DeleteVirtualFileSuccess, DisconnectFailure, Disconnected, DownloadFileFailure, DownloadFileSuccess, FileTransferStatus, GetSessions, InternalServiceRequest, InternalServiceResponse, LocalDBClearAllKVFailure, LocalDBClearAllKVSuccess, LocalDBDeleteKVFailure, LocalDBDeleteKVSuccess, LocalDBGetAllKVFailure, LocalDBGetAllKVSuccess, LocalDBGetKVFailure, LocalDBGetKVSuccess, LocalDBSetKVFailure, LocalDBSetKVSuccess, MessageReceived, MessageSendError, MessageSent, PeerConnectFailure, PeerConnectSuccess, PeerDisconnectFailure, PeerDisconnectSuccess, PeerRegisterFailure, PeerRegisterSuccess, PeerSessionInformation, SendFileFailure, SendFileRequestSent, SessionInformation, GroupCreateFailure, GroupCreateSuccess};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

use uuid::Uuid;
#[async_recursion]
pub async fn handle_request(
    command: InternalServiceRequest,
    server_connection_map: &Arc<Mutex<HashMap<u64, Connection>>>,
    remote: &mut NodeRemote,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    match command {
        InternalServiceRequest::GetAccountInformation {
            uuid,
            request_id,
            cid,
        } => {
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

            let response = InternalServiceResponse::GetAccountInformation(Accounts {
                accounts: accounts_ret,
                request_id: Some(request_id),
            });

            send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
        }
        // Return all the sessions for the given TCP connection
        InternalServiceRequest::GetSessions { uuid, request_id } => {
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

            let response = InternalServiceResponse::GetSessions(GetSessions {
                sessions,
                request_id: Some(request_id),
            });
            send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
        }
        InternalServiceRequest::Connect {
            uuid,
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
                    let client_server_remote =
                        create_client_server_remote(stream.vconn_type, remote.clone());
                    let connection_struct = Connection::new(sink, client_server_remote, uuid);
                    server_connection_map
                        .lock()
                        .await
                        .insert(cid, connection_struct);

                    let hm_for_conn = tcp_connection_map.clone();

                    let response = InternalServiceResponse::ConnectSuccess(
                        citadel_workspace_types::ConnectSuccess {
                            cid,
                            request_id: Some(request_id),
                        },
                    );

                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message =
                                InternalServiceResponse::MessageReceived(MessageReceived {
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
                    let response = InternalServiceResponse::ConnectionFailure(ConnectionFailure {
                        message: err.into_string(),
                        request_id: Some(request_id),
                    });
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }
            };
        }
        InternalServiceRequest::Register {
            uuid,
            server_addr,
            full_name,
            username,
            proposed_password,
            connect_after_register,
            default_security_settings,
            request_id,
        } => {
            info!(target: "citadel", "About to connect to server {server_addr:?} for user {username}");
            match remote
                .register(
                    server_addr,
                    full_name,
                    username.clone(),
                    proposed_password.clone(),
                    default_security_settings,
                )
                .await
            {
                Ok(_res) => match connect_after_register {
                    false => {
                        let response = InternalServiceResponse::RegisterSuccess(
                            citadel_workspace_types::RegisterSuccess {
                                id: uuid,
                                request_id: Some(request_id),
                            },
                        );
                        send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                    }
                    true => {
                        let connect_command = InternalServiceRequest::Connect {
                            uuid,
                            username,
                            password: proposed_password,
                            keep_alive_timeout: None,
                            udp_mode: Default::default(),
                            connect_mode: Default::default(),
                            session_security_settings: default_security_settings,
                            request_id,
                        };

                        handle_request(
                            connect_command,
                            server_connection_map,
                            remote,
                            tcp_connection_map,
                        )
                        .await
                    }
                },
                Err(err) => {
                    let response = InternalServiceResponse::RegisterFailure(
                        citadel_workspace_types::RegisterFailure {
                            message: err.into_string(),
                            request_id: Some(request_id),
                        },
                    );
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                }
            };
        }
        InternalServiceRequest::Message {
            uuid,
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
                                InternalServiceResponse::MessageSendError(MessageSendError {
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

                    let response = InternalServiceResponse::MessageSent(MessageSent {
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
                        InternalServiceResponse::MessageSendError(MessageSendError {
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

        InternalServiceRequest::Disconnect {
            cid,
            uuid,
            request_id,
        } => {
            let request = NodeRequest::DisconnectFromHypernode(DisconnectFromHypernode {
                implicated_cid: cid,
                v_conn_type: VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
            });
            server_connection_map.lock().await.remove(&cid);
            match remote.send(request).await {
                Ok(res) => {
                    let disconnect_success = InternalServiceResponse::Disconnected(Disconnected {
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
            uuid,
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
                            let request = NodeRequest::SendObject(SendObject {
                                source: Box::new(source),
                                chunk_size,
                                implicated_cid: cid,
                                v_conn_type: *peer_remote.remote.user(),
                                transfer_type,
                            });

                            remote.send(request).await
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        let request = NodeRequest::SendObject(SendObject {
                            source: Box::new(source),
                            chunk_size,
                            implicated_cid: cid,
                            v_conn_type: VirtualTargetType::LocalGroupServer {
                                implicated_cid: cid,
                            },
                            transfer_type,
                        });

                        remote.send(request).await
                    };

                    match result {
                        Ok(_) => {
                            info!(target: "citadel","InternalServiceRequest Send File Success");
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::SendFileRequestSent(SendFileRequestSent {
                                    cid,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                        }

                        Err(err) => {
                            info!(target: "citadel","InternalServiceRequest Send File Failure");
                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::SendFileFailure(SendFileFailure {
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
                        InternalServiceResponse::SendFileFailure(SendFileFailure {
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
            uuid,
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
                                    InternalServiceResponse::FileTransferStatus(
                                        FileTransferStatus {
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
                                    InternalServiceResponse::FileTransferStatus(
                                        FileTransferStatus {
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
            uuid,
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
            uuid,
            request_id,
        } => {
            match server_connection_map.lock().await.get_mut(&cid) {
                Some(conn) => {
                    let result = if let Some(peer_cid) = peer_cid {
                        if let Some(peer_remote) = conn.peers.get_mut(&peer_cid) {
                            peer_remote
                                .remote
                                .remote_encrypted_virtual_filesystem_delete(virtual_directory)
                                .await
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        conn.client_server_remote
                            .remote_encrypted_virtual_filesystem_delete(virtual_directory)
                            .await
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
            uuid,
            cid,
            peer_id: peer_username,
            connect_after_register,
            request_id,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );

            match client_to_server_remote
                .propose_target(cid, peer_username.clone())
                .await
            {
                Ok(symmetric_identifier_handle_ref) => {
                    match symmetric_identifier_handle_ref.register_to_peer().await {
                        Ok(_peer_register_success) => {
                            let account_manager = symmetric_identifier_handle_ref.account_manager();
                            let this_username = account_manager
                                .get_username_by_cid(cid)
                                .await
                                .ok()
                                .flatten()
                                .unwrap_or_default();
                            if let Ok(target_information) = account_manager
                                .find_target_information(cid, peer_username.clone())
                                .await
                            {
                                let (peer_cid, mutual_peer) = target_information.unwrap();
                                match connect_after_register {
                                    true => {
                                        let connect_command = InternalServiceRequest::PeerConnect {
                                            uuid,
                                            cid,
                                            username: this_username.clone(),
                                            peer_cid,
                                            peer_username: mutual_peer
                                                .username
                                                .clone()
                                                .unwrap_or_default(),
                                            udp_mode: Default::default(),
                                            session_security_settings: Default::default(),
                                            request_id,
                                        };

                                        handle_request(
                                            connect_command,
                                            server_connection_map,
                                            remote,
                                            tcp_connection_map,
                                        )
                                        .await;
                                    }
                                    false => {
                                        println!("{:?}", mutual_peer);
                                        send_response_to_tcp_client(
                                            tcp_connection_map,
                                            InternalServiceResponse::PeerRegisterSuccess(
                                                PeerRegisterSuccess {
                                                    cid,
                                                    peer_cid,
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
            uuid,
            cid,
            username,
            peer_cid,
            peer_username,
            udp_mode,
            session_security_settings,
            request_id,
        } => {
            // TODO: check to see if peer is already in the hashmap
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupPeer {
                    implicated_cid: cid,
                    peer_cid,
                },
                remote.clone(),
            );
            match client_to_server_remote
                .find_target(username, peer_username)
                .await
            {
                // username or cid?
                Ok(symmetric_identifier_handle_ref) => {
                    match symmetric_identifier_handle_ref
                        .connect_to_peer_custom(session_security_settings, udp_mode)
                        .await
                    {
                        Ok(peer_connect_success) => {
                            let connection_cid = peer_connect_success.channel.get_peer_cid();
                            let (sink, mut stream) = peer_connect_success.channel.split();
                            server_connection_map
                                .lock()
                                .await
                                .get_mut(&cid)
                                .unwrap()
                                .add_peer_connection(
                                    peer_cid,
                                    sink,
                                    symmetric_identifier_handle_ref.into_owned(),
                                );

                            let hm_for_conn = tcp_connection_map.clone();

                            send_response_to_tcp_client(
                                tcp_connection_map,
                                InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess {
                                    cid,
                                    request_id: Some(request_id),
                                }),
                                uuid,
                            )
                            .await;
                            let connection_read_stream = async move {
                                while let Some(message) = stream.next().await {
                                    let message =
                                        InternalServiceResponse::MessageReceived(MessageReceived {
                                            message: message.into_buffer(),
                                            cid: connection_cid,
                                            peer_cid,
                                            request_id: Some(request_id),
                                        });
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
            uuid,
            cid,
            peer_cid,
            request_id,
        } => {
            let request = NodeRequest::PeerCommand(PeerCommand {
                implicated_cid: cid,
                command: PeerSignal::Disconnect(
                    PeerConnectionType::LocalGroupPeer {
                        implicated_cid: cid,
                        peer_cid,
                    },
                    None,
                ),
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
            uuid,
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
            uuid,
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
            uuid,
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
            uuid,
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
            uuid,
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
            uuid,
            cid,
            request_id,
            initial_users_to_invite
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );
            match client_to_server_remote
                .create_group(initial_users_to_invite)
                .await
            {
                Ok(group_channel) => {
                    //Store the group channel in map
                    let group_key = group_channel.key();
                    server_connection_map
                        .lock()
                        .await
                        .get_mut(&cid)
                        .unwrap()
                        .add_group_connection(group_key, group_channel);

                    // Relay success to TCP client
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
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
        },

        InternalServiceRequest::GroupLeave {
            uuid,
            cid,
            group_key,
            request_id
        } => {

        },

        InternalServiceRequest::GroupEnd { .. } => {},
        InternalServiceRequest::GroupMessage { .. } => {},
        InternalServiceRequest::GroupInvite { .. } => {},
        InternalServiceRequest::GroupRespondInviteRequest { .. } => {},
        InternalServiceRequest::GroupKick { .. } => {},
        InternalServiceRequest::GroupListGroupsFor { .. } => {},
        InternalServiceRequest::GroupRequestJoin { .. } => {},
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
