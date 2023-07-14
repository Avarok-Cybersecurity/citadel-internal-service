use crate::kernel::{create_client_server_remote, send_response_to_tcp_client, Connection};
use async_recursion::async_recursion;
use citadel_logging::{error, info};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::*;
use citadel_workspace_types::{
    ConnectionFailure, DeleteVirtualFileFailure, DeleteVirtualFileSuccess, DisconnectFailure,
    Disconnected, DownloadFileFailure, DownloadFileSuccess, FileTransferStatus,
    InternalServicePayload, InternalServiceResponse, LocalDBClearAllKVFailure,
    LocalDBClearAllKVSuccess, LocalDBDeleteKVFailure, LocalDBDeleteKVSuccess,
    LocalDBGetAllKVFailure, LocalDBGetAllKVSuccess, LocalDBGetKVFailure, LocalDBGetKVSuccess,
    LocalDBSetKVFailure, LocalDBSetKVSuccess, MessageReceived, MessageSendError, MessageSent,
    PeerConnectFailure, PeerConnectSuccess, PeerDisconnectFailure, PeerDisconnectSuccess,
    PeerRegisterFailure, PeerRegisterSuccess, SendFileFailure, SendFileSuccess,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

use uuid::Uuid;

// TODO: wrap all matched error below inside a function that takes f(NetworkError, InternalServicePayload) -> PayloadHandlerError
/*pub struct PayloadHandlerError {
    pub error: NetworkError,
    pub response_payload: InternalServicePayload,
}*/

#[async_recursion]
pub async fn payload_handler(
    command: InternalServicePayload,
    server_connection_map: &Arc<Mutex<HashMap<u64, Connection>>>,
    remote: &mut NodeRemote,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
) {
    match command {
        InternalServicePayload::Connect {
            uuid,
            username,
            password,
            connect_mode,
            udp_mode,
            keep_alive_timeout,
            session_security_settings,
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
                        citadel_workspace_types::ConnectSuccess { cid },
                    );

                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message =
                                InternalServiceResponse::MessageReceived(MessageReceived {
                                    message: message.into_buffer(),
                                    cid,
                                    peer_cid: 0,
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
                    });
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                }
            };
        }
        InternalServicePayload::Register {
            uuid,
            server_addr,
            full_name,
            username,
            proposed_password,
            connect_after_register,
            default_security_settings,
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
                Ok(_res) => {
                    match connect_after_register {
                        false => {
                            // TODO: add trace ID to ensure uniqueness of request
                            let response = InternalServiceResponse::RegisterSuccess(
                                citadel_workspace_types::RegisterSuccess { id: uuid },
                            );
                            send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                        }
                        true => {
                            let connect_command = InternalServicePayload::Connect {
                                uuid,
                                username,
                                password: proposed_password,
                                keep_alive_timeout: None,
                                udp_mode: Default::default(),
                                connect_mode: Default::default(),
                                session_security_settings: default_security_settings,
                            };

                            payload_handler(
                                connect_command,
                                server_connection_map,
                                remote,
                                tcp_connection_map,
                            )
                            .await
                        }
                    }
                }
                Err(err) => {
                    let response = InternalServiceResponse::RegisterFailure(
                        citadel_workspace_types::RegisterFailure {
                            message: err.into_string(),
                        },
                    );
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await
                }
            };
        }
        InternalServicePayload::Message {
            uuid,
            message,
            cid,
            peer_cid,
            security_level,
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
                        InternalServiceResponse::MessageSent(MessageSent { cid, peer_cid });
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
                        }),
                        uuid,
                    )
                    .await;
                }
            };
        }

        InternalServicePayload::Disconnect { cid, uuid } => {
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
                        });
                    send_response_to_tcp_client(tcp_connection_map, disconnect_failure, uuid).await;
                }
            };
        }

        InternalServicePayload::SendFile {
            uuid,
            source,
            cid,
            is_refvs,
            peer_cid,
            chunk_size,
            virtual_directory,
            security_level,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );

            let chunk_size = chunk_size.unwrap_or_default();
            let security_level = security_level.unwrap_or_default();
            let virtual_directory = virtual_directory.unwrap_or_default();

            let result = if let Some(peer_cid) = peer_cid {
                match client_to_server_remote.find_target(cid, peer_cid).await {
                    Ok(peer_remote) => {
                        if is_refvs {
                            peer_remote
                                .remote_encrypted_virtual_filesystem_push_custom_chunking(
                                    source,
                                    virtual_directory,
                                    chunk_size,
                                    security_level,
                                )
                                .await
                        } else {
                            peer_remote
                                .send_file_with_custom_opts(
                                    source,
                                    chunk_size,
                                    TransferType::FileTransfer,
                                )
                                .await
                        }
                    }
                    Err(err) => {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::SendFileFailure(SendFileFailure {
                                cid,
                                message: err.to_string(),
                            }),
                            uuid,
                        )
                        .await;
                        Err(err)
                    }
                }
            } else {
                // TODO: move the TransferType to the enum in the TCP client request
                if is_refvs {
                    client_to_server_remote
                        .send_file_with_custom_opts(source, chunk_size, TransferType::FileTransfer)
                        .await
                } else {
                    client_to_server_remote
                        .remote_encrypted_virtual_filesystem_push_custom_chunking(
                            source,
                            virtual_directory,
                            chunk_size,
                            security_level,
                        )
                        .await
                }
            };

            match result {
                Ok(_) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::SendFileSuccess(SendFileSuccess { cid }),
                        uuid,
                    )
                    .await;
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::SendFileFailure(SendFileFailure {
                            cid,
                            message: err.into_string(),
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServicePayload::RespondFileTransfer {
            uuid,
            cid,
            peer_cid,
            object_id,
            accept,
            download_location: _,
        } => {
            if let Some(connection) = server_connection_map.lock().await.get_mut(&cid) {
                if let Some(mut handler) = connection.take_file_transfer_handle(peer_cid, object_id)
                {
                    if let Some(mut owned_handler) = handler.take() {
                        let connection_map_clone = tcp_connection_map.clone();
                        let result = if accept {
                            let accept_result = owned_handler.accept();
                            let tcp_client_metadata_updater = async move {
                                let mut _path = None;
                                while let Some(status) = owned_handler.next().await {
                                    let status_string = match status {
                                        ObjectTransferStatus::ReceptionBeginning(file_path, _) => {
                                            _path = Some(file_path);
                                            "ReceptionBeginning"
                                        }
                                        ObjectTransferStatus::ReceptionTick(_, _, _) => {
                                            "ReceptionTick"
                                        }
                                        ObjectTransferStatus::ReceptionComplete => {
                                            //TODO: Update to use metadata
                                            "ReceptionComplete"
                                        }
                                        _ => "ReceptionEnded",
                                    };

                                    let message = InternalServiceResponse::FileTransferTick(
                                        citadel_workspace_types::FileTransferTick {
                                            uuid,
                                            cid,
                                            peer_cid,
                                            status: String::from(status_string),
                                        },
                                    );
                                    match connection_map_clone.lock().await.get(&uuid) {
                                        Some(entry) => match entry.send(message) {
                                            Ok(res) => res,
                                            Err(_) => {
                                                info!(target: "citadel", "File Transfer Status Tick Not Sent")
                                            }
                                        },
                                        None => {
                                            info!(target:"citadel","Connection not found during File Transfer Status Tick")
                                        }
                                    }
                                }
                            };
                            tokio::task::spawn(tcp_client_metadata_updater);
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

        InternalServicePayload::DownloadFile {
            virtual_directory,
            security_level,
            delete_on_pull,
            cid,
            peer_cid,
            uuid,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );

            let security_level = security_level.unwrap_or_default();

            let result = if let Some(peer_cid) = peer_cid {
                match client_to_server_remote.find_target(cid, peer_cid).await {
                    Ok(peer_remote) => {
                        peer_remote
                            .remote_encrypted_virtual_filesystem_pull(
                                virtual_directory,
                                security_level,
                                delete_on_pull,
                            )
                            .await
                    }
                    Err(err) => {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::DownloadFileFailure(DownloadFileFailure {
                                cid,
                                message: err.to_string(),
                            }),
                            uuid,
                        )
                        .await;
                        Err(err)
                    }
                }
            } else {
                client_to_server_remote
                    .remote_encrypted_virtual_filesystem_pull(
                        virtual_directory,
                        security_level,
                        delete_on_pull,
                    )
                    .await
            };

            match result {
                Ok(_) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::DownloadFileSuccess(DownloadFileSuccess { cid }),
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
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServicePayload::DeleteVirtualFile {
            virtual_directory,
            cid,
            peer_cid,
            uuid,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );

            let result = if let Some(peer_cid) = peer_cid {
                match client_to_server_remote.find_target(cid, peer_cid).await {
                    Ok(peer_remote) => {
                        peer_remote
                            .remote_encrypted_virtual_filesystem_delete(virtual_directory)
                            .await
                    }
                    Err(err) => {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::DeleteVirtualFileFailure(
                                DeleteVirtualFileFailure {
                                    cid,
                                    message: err.to_string(),
                                },
                            ),
                            uuid,
                        )
                        .await;
                        Err(err)
                    }
                }
            } else {
                client_to_server_remote
                    .remote_encrypted_virtual_filesystem_delete(virtual_directory)
                    .await
            };

            match result {
                Ok(_) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::DeleteVirtualFileSuccess(
                            DeleteVirtualFileSuccess { cid },
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
                            },
                        ),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServicePayload::StartGroup {
            initial_users_to_invite,
            cid,
            uuid: _uuid,
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
                Ok(_group_channel) => {}

                Err(_err) => {}
            }
        }

        InternalServicePayload::PeerRegister {
            uuid,
            cid,
            peer_id: peer_username,
            connect_after_register,
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
                            if let Ok(target_information) = account_manager
                                .find_target_information(cid, peer_username.clone())
                                .await
                            {
                                let (peer_cid, mutual_peer) = target_information.unwrap();
                                match connect_after_register {
                                    true => {
                                        let connect_command = InternalServicePayload::PeerConnect {
                                            uuid,
                                            cid,
                                            username: mutual_peer.username.unwrap(),
                                            peer_cid,
                                            peer_username: String::from("peer.a"),
                                            udp_mode: Default::default(),
                                            session_security_settings: Default::default(),
                                        };

                                        payload_handler(
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
                                                    username: mutual_peer.username.unwrap(),
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
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServicePayload::PeerConnect {
            uuid,
            cid,
            username,
            peer_cid,
            peer_username,
            udp_mode,
            session_security_settings,
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
                        }),
                        uuid,
                    )
                    .await;
                }
            }
        }
        InternalServicePayload::PeerDisconnect {
            uuid,
            cid,
            peer_cid,
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
                                    PeerDisconnectSuccess { cid, ticket: 0 },
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
        InternalServicePayload::LocalDBGetKV {
            uuid,
            cid,
            peer_cid,
            key,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
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
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
                                cid,
                                peer_cid: Some(peer_cid),
                                message: "Peer connection not found".to_string(),
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
                    )
                    .await;
                }
            }
        },
        InternalServicePayload::LocalDBSetKV {
            uuid,
            cid,
            peer_cid,
            key,
            value,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
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
                        )
                        .await;
                    } else {
                        send_response_to_tcp_client(
                            tcp_connection_map,
                            InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
                                cid,
                                peer_cid: Some(peer_cid),
                                message: "Peer connection not found".to_string(),
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
                    )
                    .await;
                }
            }
        },
        InternalServicePayload::LocalDBDeleteKV {
            uuid,
            cid,
            peer_cid,
            key,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBDeleteKVFailure(LocalDBDeleteKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
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
                    )
                    .await;
                }
            }
        },
        InternalServicePayload::LocalDBGetAllKV {
            uuid,
            cid,
            peer_cid,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBGetAllKVFailure(LocalDBGetAllKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
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
                    )
                    .await;
                }
            }
        },
        InternalServicePayload::LocalDBClearAllKV {
            uuid,
            cid,
            peer_cid,
        } => match server_connection_map.lock().await.get_mut(&cid) {
            None => {
                send_response_to_tcp_client(
                    tcp_connection_map,
                    InternalServiceResponse::LocalDBClearAllKVFailure(LocalDBClearAllKVFailure {
                        cid,
                        peer_cid,
                        message: "Server connection not found".to_string(),
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
                    )
                    .await;
                }
            }
        },
    }
}

async fn backend_handler_get(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
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
                }),
                uuid,
            )
            .await;
        }
    }
}

// backend_handler_set
async fn backend_handler_set(
    remote: &impl BackendHandler,
    tcp_connection_map: &Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    uuid: Uuid,
    cid: u64,
    peer_cid: Option<u64>,
    key: String,
    value: Vec<u8>,
) {
    match remote.set(&key, value).await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBSetKVSuccess(LocalDBSetKVSuccess {
                    cid,
                    peer_cid,
                    key,
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
) {
    match remote.remove(&key).await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBDeleteKVSuccess(LocalDBDeleteKVSuccess {
                    cid,
                    peer_cid,
                    key,
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
) {
    match remote.get_all().await {
        Ok(map) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBGetAllKVSuccess(LocalDBGetAllKVSuccess {
                    cid,
                    peer_cid,
                    map,
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
) {
    match remote.remove_all().await {
        Ok(_) => {
            send_response_to_tcp_client(
                tcp_connection_map,
                InternalServiceResponse::LocalDBClearAllKVSuccess(LocalDBClearAllKVSuccess {
                    cid,
                    peer_cid,
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
                }),
                uuid,
            )
            .await;
        }
    }
}
