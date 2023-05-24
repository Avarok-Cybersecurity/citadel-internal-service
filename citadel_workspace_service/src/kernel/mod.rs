use bytes::Bytes;
use citadel_logging::info;
use citadel_sdk::prelude::*;
use citadel_workspace_types::{InternalServicePayload, InternalServiceResponse};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use citadel_sdk::prefabs::ClientServerRemote;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

pub struct CitadelWorkspaceService {
    pub remote: Option<NodeRemote>,
    // 127.0.0.1:55555
    pub bind_address: SocketAddr,
}

#[async_trait]
impl NetKernel for CitadelWorkspaceService {
    fn load_remote(&mut self, node_remote: NodeRemote) -> Result<(), NetworkError> {
        self.remote = Some(node_remote);
        Ok(())
    }

    async fn on_start(&self) -> Result<(), NetworkError> {
        let mut remote = self.remote.clone().unwrap();
        let listener = tokio::net::TcpListener::bind(self.bind_address).await?;
        //from TCP to command handler
        //read task
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();

        let hm: &Arc<tokio::sync::Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>> =
            &Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let listener_task = async move {
            while let Ok((conn, _addr)) = listener.accept().await {
                //from command handler to the TCP write tak in handle_connection
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServiceResponse>();
                let id = Uuid::new_v4();
                hm.lock().await.insert(id, tx1);
                handle_connection(conn, tx.clone(), rx1, id);
            }
            Ok(())
        };

        let mut connection_map: HashMap<u64, PeerChannelSendHalf> = HashMap::new();

        let inbound_command_task = async move {
            while let Some(command) = rx.recv().await {
                payload_handler(command, &mut connection_map, &mut remote, hm).await;
            }
            Ok(())
        };

        tokio::select! {
            res0 = listener_task => res0,
            res1 = inbound_command_task => res1,
        }
    }

    async fn on_node_event_received(&self, _message: NodeResult) -> Result<(), NetworkError> {
        // TODO: handle disconnect properly by removing entries from the hashmap
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}

async fn send_response_to_tcp_client(
    hash_map: &Arc<tokio::sync::Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
    response: InternalServiceResponse,
    uuid: Uuid,
) {
    hash_map
        .lock()
        .await
        .get(&uuid)
        .unwrap()
        .send(response)
        .unwrap()
}

async fn payload_handler(
    command: InternalServicePayload,
    connection_map: &mut HashMap<u64, PeerChannelSendHalf>,
    remote: &mut NodeRemote,
    hm: &Arc<tokio::sync::Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
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
                .connect(AuthenticationRequest::credentialed(username, password), connect_mode, udp_mode, keep_alive_timeout, session_security_settings)
                .await
            {
                Ok(conn_success) => {
                    let cid = conn_success.cid;

                    let (sink, mut stream) = conn_success.channel.split();
                    connection_map.insert(cid, sink);

                    let hm_for_conn = hm.clone();

                    let response = InternalServiceResponse::ConnectSuccess { cid };

                    send_response_to_tcp_client(hm, response, uuid).await;

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
                    send_response_to_tcp_client(hm, response, uuid).await;
                }
            };
        }
        InternalServicePayload::Register {
            uuid,
            server_addr,
            full_name,
            username,
            proposed_password,
            default_security_settings,
        } => {
            citadel_logging::info!(target: "citadel", "About to connect to server {server_addr:?} for user {username}");
            match remote
                .register(server_addr, full_name, username, proposed_password, default_security_settings)
                .await
            {
                Ok(_res) => {
                    // TODO: add trace ID to ensure uniqueness of request
                    let response = InternalServiceResponse::RegisterSuccess { id: uuid };
                    send_response_to_tcp_client(hm, response, uuid).await
                }
                Err(err) => {
                    let response = InternalServiceResponse::RegisterFailure {
                        message: err.into_string(),
                    };
                    send_response_to_tcp_client(hm, response, uuid).await
                }
            };
        }
        InternalServicePayload::Message {
            uuid,
            message,
            cid,
            user_cid: _,
            security_level,
        } => {
            match connection_map.get_mut(&cid) {
                Some(sink) => {
                    sink.set_security_level(security_level);
                    sink.send_message(message.into()).await.unwrap();
                    send_response_to_tcp_client(hm, InternalServiceResponse::MessageSent { cid }, uuid).await;
                }
                None => {
                    info!(target: "citadel","connection not found");
                    send_response_to_tcp_client(hm, InternalServiceResponse::MessageSendError { cid, message: format!("Connection for {cid} not found") }, uuid).await;
                }
            };
        }

        InternalServicePayload::Disconnect { cid, uuid } => {
            let request = NodeRequest::DisconnectFromHypernode(DisconnectFromHypernode {
                implicated_cid: cid,
                v_conn_type: VirtualTargetType::LocalGroupServer { implicated_cid: cid }
            });
            connection_map.remove(&cid);
            match remote.send(request).await {
                Ok(res) => {
                    let disconnect_success = InternalServiceResponse::DisconnectSuccess { cid };
                    send_response_to_tcp_client(hm, disconnect_success, uuid).await;
                    info!(target: "citadel", "Disconnected {res:?}")
                }
                Err(err) => {
                    let error_message = format!("Failed to disconnect {err:?}");
                    info!(target: "citadel", "{error_message}");
                    let disconnect_failure = InternalServiceResponse::DisconnectFailure {
                        cid,
                        message: error_message,
                    };
                    send_response_to_tcp_client(hm, disconnect_failure, uuid).await;
                },
            };
        }
        InternalServicePayload::SendFile {
            uuid,
            source,
            cid,
            chunk_size,
            transfer_type
        } => {
            let mut client_to_server_remote = ClientServerRemote::new(VirtualTargetType::LocalGroupServer { implicated_cid: cid }, remote.clone());
            match client_to_server_remote.send_file_with_custom_opts(source, chunk_size, transfer_type).await {
                Ok(_) => {
                    send_response_to_tcp_client(hm, InternalServiceResponse::SendFileSuccess { cid }, uuid).await;
                },

                Err(err) => {
                    send_response_to_tcp_client(hm, InternalServiceResponse::SendFileFailure { cid, message: err.into_string() }, uuid).await;
                }
            }
        }

        InternalServicePayload::DownloadFile {
            virtual_path,
            transfer_security_level,
            delete_on_pull,
            cid,
            uuid
        } => {
            /*let mut client_to_server_remote = ClientServerRemote::new(VirtualTargetType::LocalGroupServer { implicated_cid: cid }, remote.clone());
            match client_to_server_remote.(virtual_path, transfer_security_level, delete_on_pull).await {
                Ok(_) => {

                },
                Err(err) => {

                }
            }*/
        }

        InternalServicePayload::StartGroup {
            initial_users_to_invite,
            cid,
            uuid
        } => {
            let mut client_to_server_remote = ClientServerRemote::new(VirtualTargetType::LocalGroupServer { implicated_cid: cid }, remote.clone());
            match client_to_server_remote.create_group(initial_users_to_invite).await {
                Ok(group_channel) => {

                },

                Err(err) => {

                }
            }
        }

    }
}

pub fn wrap_tcp_conn(conn: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    LengthDelimitedCodec::builder()
        .length_field_offset(0) // default value
        .max_frame_length(1024 * 1024 * 64) // 64 MB
        .length_field_type::<u32>()
        .length_adjustment(0) // default value
        // `num_skip` is not needed, the default is to skip
        .new_framed(conn)
}

fn serialize_payload(payload: &InternalServiceResponse) -> Vec<u8> {
    bincode2::serialize(&payload).unwrap()
}

async fn sink_send_payload(
    payload: &InternalServiceResponse,
    sink: &mut SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
) {
    let payload = serialize_payload(payload);
    match sink.send(payload.into()).await {
        Ok(_) => (),
        Err(_) => info!(target: "citadel", "w task: sink send err"),
    }
}

fn deserialize(message: &[u8]) -> InternalServicePayload {
    bincode2::deserialize(message).unwrap()
}

fn send_to_kernel(payload_to_send: &[u8], sender: &UnboundedSender<InternalServicePayload>) {
    let payload = deserialize(payload_to_send);
    sender.send(payload).unwrap();
}

fn handle_connection(
    conn: tokio::net::TcpStream,
    to_kernel: tokio::sync::mpsc::UnboundedSender<InternalServicePayload>,
    mut from_kernel: tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    conn_id: Uuid,
) {
    tokio::task::spawn(async move {
        let framed = wrap_tcp_conn(conn);
        let (mut sink, mut stream) = framed.split();

        let write_task = async move {
            let response = InternalServiceResponse::ServiceConnectionAccepted { id: conn_id };

            sink_send_payload(&response, &mut sink).await;

            while let Some(kernel_response) = from_kernel.recv().await {
                sink_send_payload(&kernel_response, &mut sink).await;
            }
        };

        let read_task = async move {
            while let Some(message) = stream.next().await {
                send_to_kernel(&message.unwrap(), &to_kernel);
            }
            info!(target: "citadel", "Disconnected");
        };

        tokio::select! {
            res0 = write_task => res0,
            res1 = read_task => res1,
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream::SplitSink;
    use std::error::Error;
    use std::time::Duration;
    use tokio::net::TcpStream;

    async fn send(
        sink: &mut SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>,
        command: InternalServicePayload,
    ) -> Result<(), Box<dyn Error>> {
        let command = bincode2::serialize(&command)?;
        sink.send(command.into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        info!(target: "citadel", "above server spawn");

        let bind_address_internal_service: SocketAddr = "127.0.0.1:55556".parse().unwrap();
        // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
        let (server, server_bind_address) = citadel_sdk::test_common::server_info();

        tokio::task::spawn(server);
        info!(target: "citadel", "sub server spawn");
        let internal_service_kernel = CitadelWorkspaceService {
            remote: None,
            bind_address: bind_address_internal_service,
        };
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .build(internal_service_kernel)?;

        tokio::task::spawn(internal_service);

        // give time for both the server and internal service to run

        tokio::time::sleep(Duration::from_millis(2000)).await;

        info!(target: "citadel", "about to connect to internal service");

        // begin mocking the GUI/CLI access
        let conn = TcpStream::connect(bind_address_internal_service).await?;
        info!(target: "citadel", "connected to the TCP stream");
        let framed = wrap_tcp_conn(conn);
        info!(target: "citadel", "wrapped tcp connection");

        let (mut sink, mut stream) = framed.split();

        let first_packet = stream.next().await.unwrap()?;
        info!(target: "citadel", "First packet");
        let greeter_packet: InternalServiceResponse = bincode2::deserialize(&*first_packet)?;

        info!(target: "citadel", "Greeter packet {greeter_packet:?}");

        if let InternalServiceResponse::ServiceConnectionAccepted { id } = greeter_packet {
            let register_command = InternalServicePayload::Register {
                uuid: id,
                server_addr: server_bind_address,
                full_name: String::from("John"),
                username: String::from("john_doe"),
                proposed_password: "test12345".into(),
                default_security_settings: Default::default(),
            };
            send(&mut sink, register_command).await?;

            let second_packet = stream.next().await.unwrap()?;
            let response_packet: InternalServiceResponse = bincode2::deserialize(&*second_packet)?;
            if let InternalServiceResponse::RegisterSuccess { id } = response_packet {
                // now, connect to the server
                let command = InternalServicePayload::Connect {
                    username: String::from("john_doe"),
                    password: "test12345".into(),
                    connect_mode: Default::default(),
                    udp_mode: Default::default(),
                    keep_alive_timeout: None,
                    uuid: id,
                    session_security_settings: Default::default(),
                };

                send(&mut sink, command).await?;

                let next_packet = stream.next().await.unwrap()?;
                let response_packet: InternalServiceResponse =
                    bincode2::deserialize(&*next_packet)?;
                if let InternalServiceResponse::ConnectSuccess { cid } = response_packet {
                    let disconnect_command = InternalServicePayload::Disconnect { uuid: id, cid };

                    send(&mut sink, disconnect_command).await?;
                    let next_packet = stream.next().await.unwrap()?;
                    let response_disconnect_packet: InternalServiceResponse =
                        bincode2::deserialize(&*next_packet)?;

                    if let InternalServiceResponse::DisconnectSuccess(cid) =
                        response_disconnect_packet
                    {
                        info!(target:"citadel", "Disconnected {cid}");
                        Ok(())
                    } else {
                        panic!("Disconnection failed");
                    }
                } else {
                    panic!("Connection to server was not a success")
                }
            } else {
                panic!("Registration to server was not a success")
            }
        } else {
            panic!("Wrong packet type");
        }
    }
}
