use crate::kernel::{create_client_server_remote, send_response_to_tcp_client, Connection};
use async_recursion::async_recursion;
use citadel_logging::{error, info};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::*;
use citadel_workspace_types::{InternalServicePayload, InternalServiceResponse};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

use uuid::Uuid;

// TODO: wrap all matched error below inside a function that takes f(NetworkError, InternalServicePayload) -> PayloadHandlerError
pub struct PayloadHandlerError {
    pub error: NetworkError,
    pub response_payload: InternalServicePayload
}

#[async_recursion]
pub async fn payload_handler(
    command: InternalServicePayload,
    server_connection_map: &Arc<Mutex<HashMap<u64, Connection>>>,
    remote: &mut NodeRemote,
    tcp_connection_map: &Arc<
        tokio::sync::Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>,
    >,
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

                    let response = InternalServiceResponse::ConnectSuccess { cid };

                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message = InternalServiceResponse::MessageReceived {
                                message: message.into_buffer(),
                                cid,
                                peer_cid: 0,
                            };
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
                    let response = InternalServiceResponse::ConnectionFailure {
                        message: err.into_string(),
                    };
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
                            let response = InternalServiceResponse::RegisterSuccess { id: uuid };
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
                    let response = InternalServiceResponse::RegisterFailure {
                        message: err.into_string(),
                    };
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
                                InternalServiceResponse::MessageSendError {
                                    cid,
                                    message: format!("Connection for {cid} not found"),
                                },
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

                    let response = InternalServiceResponse::MessageSent { cid, peer_cid };
                    send_response_to_tcp_client(tcp_connection_map, response, uuid).await;
                    info!(target: "citadel", "Into the message handler command send")
                }
                None => {
                    info!(target: "citadel","connection not found");
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::MessageSendError {
                            cid,
                            message: format!("Connection for {cid} not found"),
                        },
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
                    let disconnect_success = InternalServiceResponse::Disconnected {
                        cid,
                        peer_cid: None,
                    };
                    send_response_to_tcp_client(tcp_connection_map, disconnect_success, uuid).await;
                    info!(target: "citadel", "Disconnected {res:?}")
                }
                Err(err) => {
                    let error_message = format!("Failed to disconnect {err:?}");
                    info!(target: "citadel", "{error_message}");
                    let disconnect_failure = InternalServiceResponse::DisconnectFailure {
                        cid,
                        message: error_message,
                    };
                    send_response_to_tcp_client(tcp_connection_map, disconnect_failure, uuid).await;
                }
            };
        }

        InternalServicePayload::SendFileStandard {
            uuid,
            source,
            cid,
            peer_cid,
            chunk_size,
        } => {
            let client_to_server_remote = ClientServerRemote::new(
                VirtualTargetType::LocalGroupServer {
                    implicated_cid: cid,
                },
                remote.clone(),
            );

            let chunk_size = chunk_size.unwrap_or_default();

            let result = if let Some(peer_cid) = peer_cid {
                match client_to_server_remote.find_target(cid, peer_cid).await {
                    Ok(peer_remote) => {
                        peer_remote.send_file_with_custom_opts(source, chunk_size, TransferType::FileTransfer).await
                    },
                    Err(err) => {
                        // TODO: handle the error properly
                        return;
                    }
                }
            } else {
                client_to_server_remote.send_file_with_custom_opts(source, chunk_size, TransferType::FileTransfer).await
            };

            match result
            {
                Ok(_) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::SendFileSuccess { cid },
                        uuid,
                    )
                    .await;
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::SendFileFailure {
                            cid,
                            message: err.into_string(),
                        },
                        uuid,
                    )
                    .await;
                }
            }
        }

        InternalServicePayload::DownloadFile {
            virtual_path: _,
            transfer_security_level: _,
            delete_on_pull: _,
            cid: _,
            uuid: _,
        } => {
            // let mut client_to_server_remote = ClientServerRemote::new(VirtualTargetType::LocalGroupServer { implicated_cid: cid }, remote.clone());
            // match client_to_server_remote.(virtual_path, transfer_security_level, delete_on_pull).await {
            //     Ok(_) => {

            //     },
            //     Err(err) => {

            //     }
            // }
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
                                            InternalServiceResponse::PeerRegisterSuccess {
                                                cid,
                                                peer_cid,
                                                username: mutual_peer.username.unwrap(),
                                            },
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
                                InternalServiceResponse::PeerRegisterFailure {
                                    cid,
                                    message: err.into_string(),
                                },
                                uuid,
                            )
                            .await;
                        }
                    }
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::PeerRegisterFailure {
                            cid,
                            message: err.into_string(),
                        },
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
                                InternalServiceResponse::PeerConnectSuccess { cid },
                                uuid,
                            )
                            .await;
                            let connection_read_stream = async move {
                                while let Some(message) = stream.next().await {
                                    let message = InternalServiceResponse::MessageReceived {
                                        message: message.into_buffer(),
                                        cid: connection_cid,
                                        peer_cid,
                                    };
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
                                InternalServiceResponse::PeerConnectFailure {
                                    cid,
                                    message: err.into_string(),
                                },
                                uuid,
                            )
                            .await;
                        }
                    }
                }

                Err(err) => {
                    send_response_to_tcp_client(
                        tcp_connection_map,
                        InternalServiceResponse::PeerConnectFailure {
                            cid,
                            message: err.into_string(),
                        },
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
                        InternalServiceResponse::PeerDisconnectFailure {
                            cid,
                            message: "Server connection not found".to_string(),
                        },
                        uuid,
                    )
                    .await;
                }
                Some(conn) => match conn.peers.get_mut(&cid) {
                    None => {}
                    Some(target_peer) => match target_peer.remote.send(request).await {
                        Ok(ticket) => {
                            conn.clear_peer_connection(peer_cid);
                            let peer_disconnect_success =
                                InternalServiceResponse::PeerDisconnectSuccess { cid, ticket: 0 };
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
                                InternalServiceResponse::PeerDisconnectFailure {
                                    cid,
                                    message: error_message,
                                };
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
    }
}
