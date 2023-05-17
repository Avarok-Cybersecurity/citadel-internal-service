use citadel_logging::info;
use citadel_sdk::prelude::*;
use citadel_workspace_types::InternalServicePayload;
use futures::stream::StreamExt;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::LengthDelimitedCodec;
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
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();

        let ref hm = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let listener_task = async move {
            while let Ok((conn, _addr)) = listener.accept().await {
                let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<InternalServicePayload>();
                let id = Uuid::new_v4();
                handle_connection(conn, tx.clone(), rx1, id);
                hm.lock().await.insert(id, tx1);
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

    async fn on_node_event_received(&self, message: NodeResult) -> Result<(), NetworkError> {
        todo!()
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        todo!()
    }
}

async fn payload_handler(
    command: InternalServicePayload,
    connection_map: &mut HashMap<u64, PeerChannelSendHalf>,
    remote: &mut NodeRemote,
    hm: &Arc<tokio::sync::Mutex<HashMap<Uuid, UnboundedSender<InternalServicePayload>>>>,
) {
    match command {
        InternalServicePayload::Connect { uuid } => {
            let response_to_internal_client = match remote
                .connect_with_defaults(AuthenticationRequest::credentialed(1, "12134"))
                .await
            {
                //adde or self.bind_addr??
                Ok(conn_success) => {
                    let cid = conn_success.cid;

                    let (sink, mut stream) = conn_success.channel.split();
                    connection_map.insert(cid, sink);

                    let hm_for_conn = hm.clone();

                    let connection_read_stream = async move {
                        while let Some(message) = stream.next().await {
                            let message = InternalServicePayload::MessageReceived {
                                message: message.into_buffer(),
                                cid: cid,
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
                    NetworkError::InternalError("Error");
                }
            };
        }
        InternalServicePayload::Register { .. } => {}
        InternalServicePayload::Message {
            message,
            cid,
            security_level,
        } => {
            match connection_map.get_mut(&cid) {
                Some(sink) => {
                    sink.set_security_level(security_level);
                    sink.send_message(message.into()).await.unwrap();
                }
                None => info!(target: "citadel","connection not found"),
            };
        }
        InternalServicePayload::MessageReceived { .. } => {}
        InternalServicePayload::Disconnect { .. } => {}
        InternalServicePayload::SendFile { .. } => {}
        InternalServicePayload::DownloadFile { .. } => {}
        InternalServicePayload::ServiceConnectionAccepted { .. } => {}
    };
}

fn handle_connection(
    conn: tokio::net::TcpStream,
    to_kernel: tokio::sync::mpsc::UnboundedSender<InternalServicePayload>,
    mut from_kernel: tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>,
    conn_id: Uuid,
) {
    tokio::task::spawn(async move {
        let mut framed = LengthDelimitedCodec::builder()
            .length_field_offset(0) // default value
            .max_frame_length(1024 * 1024 * 64) // 64 MB
            .length_field_type::<u32>()
            .length_adjustment(0) // default value
            // `num_skip` is not needed, the default is to skip
            .new_framed(conn);

        let (mut sink, mut stream) = framed.split();

        let write_task = async move {
            let response = InternalServicePayload::ServiceConnectionAccepted { id: conn_id };
            match bincode2::serialize(&response) {
                Ok(res) => {
                    match sink.send(res.into()).await {
                        Ok(_) => (),
                        Err(_) => info!(target: "citadel", "w task: sink send err"),
                    };
                }
                Err(_) => info!(target: "citadel", "write task: serialization err"),
            };

            while let Some(kernel_response) = from_kernel.recv().await {
                match bincode2::serialize(&kernel_response) {
                    Ok(k_res) => {
                        match sink.send(k_res.into()).await {
                            Ok(_) => (),
                            Err(_) => info!(target: "citadel", "w task: sink send err"),
                        };
                    }
                    Err(_) => info!(target: "citadel", "write task: serialization err"),
                };
                ()
            }
        };

        let read_task = async move {
            while let Some(message) = stream.next().await {
                match bincode2::deserialize(&*message.unwrap()) {
                    Ok(request) => {
                        match to_kernel.send(request) {
                            Ok(res) => res,
                            Err(_) => info!(target: "citadel", "r task: sink send err"),
                        };
                    }
                    Err(_) => info!(target: "citadel", "read task deserialization err"),
                }
                ()
            }
        };

        tokio::select! {
            res0 = write_task => res0,
            res1 = read_task => res1,
        };
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_citadel_workspace_service() {
        let bind_address: SocketAddr = "127.0.0.1:".parse().unwrap(); // Random available port
        let mut conn = TcpStream::connect(bind_address).await.unwrap();
        let conn_id = Uuid::new_v4();

        //I think I have to initialize the CitadelWorkspaceService {
        //  remote
        //  conn
        // }
        // to be able to access the functionality implemented already?

        let (mut sink, stream) = conn.split();

        let command = InternalServicePayload::Connect {
            uuid: Uuid::new_v4(),
        };
    }
}
