use std::collections::HashMap;
use std::net::SocketAddr;
use citadel_sdk::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::LengthDelimitedCodec;
use citadel_workspace_types::InternalServicePayload;
use uuid::Uuid; // UUID for cid

pub struct CitadelWorkspaceService {
    pub remote: Option<NodeRemote>,
    // 127.0.0.1:55555
    pub bind_address: SocketAddr
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

        let listener_task = async move {
            while let Ok((conn, addr)) = listener.accept().await? {
                handle_connection(conn, tx.clone(), rx.clone());
                //let local_cid = Uuid::new_v4();
            }
        };

        let mut connection_map = HashMap::new();

        let inbound_command_task = async move {
            while let Some(command) = rx.recv().await {
                match command {
                    InternalServicePayload::StartGroup {  } => {

                    }
                    InternalServicePayload::Connect { auth, udp_mode, keep_alive_timeout, session_security_settings, on_channel_received } => {
                        let response_to_internal_client = match remote.connect().await {
                            Ok(conn_success) => {
                                let cid = conn_success.cid;
                                let connection_task_to_this_server = async move {
                                    let read_task = async move {

                                    };

                                    let write_task = async move {

                                    };

                                    tokio::select! {
                                        res0 = read_task => res0,
                                        res1 = write_task => res1,
                                    }
                                };
                            },

                            Err(err) => {

                            }
                        };


                    }
                    InternalServicePayload::Register { full_name, username, proposed_password, server_addr, udp_mode, default_security_settings, on_channel_received } => {}
                    InternalServicePayload::Message { message, cid, security_level } => {
                        let (sink, stream) = connection_map.get_mut(&cid).unwrap();
                        sink.set_security_level(security_level);
                        sink.send_message(message).await?;
                    }
                    InternalServicePayload::Disconnect { ticket, cid_opt, success, v_conn_type, message } => {}
                    InternalServicePayload::SendFile { source, cid, transfer_security_level, chunk_size, transfer_type } => {}
                    InternalServicePayload::DownloadFile { virtual_path, transfer_security_level, delete_on_pull } => {}
                }
            }
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

fn handle_connection(conn: tokio::net::TcpStream, to_kernel: tokio::sync::mpsc::UnboundedSender<InternalServicePayload>, mut from_kernel: tokio::sync::mpsc::UnboundedReceiver<InternalServicePayload>) {
    tokio::task::spawn(async move {
        let framed = LengthDelimitedCodec::builder()
            .length_field_offset(0) // default value
            .max_frame_length(1024 * 1024 * 64) // 64 MB
            .length_field_type::<u32>()
            .length_adjustment(0) // default value
            // `num_skip` is not needed, the default is to skip
            .new_framed(conn);

        let (mut sink, stream) = framed.split();

        let write_task = async move {
            while let Some(kernel_response) = from_kernel.recv().await {
                let serialized_response = bincode2::serialize(&kernel_response)?;
                sink.send(serialized_response.into()).await?;
            }

            Ok(())
        };

        let read_task = async move {
            while let Some(message) = stream.next().await {
                let message = message?;
                let request: InternalServicePayload  = bincode2::deserialize(&message)?;
                to_kernel.send(request)?;
            }
        };

        let result = tokio::select! {
            res0 = write_task => res0,
            res1 = read_task => res1,
        };
    });
}