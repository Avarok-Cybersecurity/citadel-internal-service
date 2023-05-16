use citadel_sdk::prelude::*;
use citadel_workspace_types::InternalServicePayload;
use futures::stream::StreamExt;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
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

        let mut connection_map = HashMap::new();

        let inbound_command_task = async move {
            while let Some(command) = rx.recv().await {
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
                                        hm_for_conn.lock().await.get(&uuid).unwrap().send(message);
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
                        let sink = connection_map.get_mut(&cid).unwrap();
                        sink.set_security_level(security_level);
                        sink.send_message(message.into()).await?;
                        // todo!("Proper error handling")
                    }
                    InternalServicePayload::MessageReceived { .. } => {}
                    InternalServicePayload::Disconnect { .. } => {}
                    InternalServicePayload::SendFile { .. } => {}
                    InternalServicePayload::DownloadFile { .. } => {}
                    InternalServicePayload::ServiceConnectionAccepted { .. } => {}
                }
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
            let serialized_response = bincode2::serialize(&response).unwrap();
            sink.send(serialized_response.into()).await.unwrap();
            while let Some(kernel_response) = from_kernel.recv().await {
                let serialized_response = bincode2::serialize(&kernel_response).unwrap();
                sink.send(serialized_response.into()).await.unwrap();
                ()
            }
        };

        let read_task = async move {
            while let Some(message) = stream.next().await {
                let request: InternalServicePayload =
                    bincode2::deserialize(&*message.unwrap()).unwrap();
                to_kernel.send(request).unwrap();
                ()
            }
        };

        tokio::select! {
            res0 = write_task => res0,
            res1 = read_task => res1,
        };
    });
}
