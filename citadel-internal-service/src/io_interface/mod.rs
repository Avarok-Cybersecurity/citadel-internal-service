use crate::kernel::{send_to_kernel, sink_send_payload, CitadelWorkspaceService, Connection};
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
    ServiceConnectionAccepted,
};
use citadel_logging::{error, info, warn};
use citadel_sdk::async_trait;
use futures::{Sink, Stream, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tcp::TcpIOInterface;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod in_memory;
pub mod tcp;

#[async_trait]
pub trait IOInterface: Sized + Send + 'static {
    type Sink: Sink<InternalServicePayload, Error = std::io::Error> + Unpin + Send + 'static;
    type Stream: Stream<Item = std::io::Result<InternalServicePayload>> + Unpin + Send + 'static;
    async fn next_connection(&mut self) -> Option<(Self::Sink, Self::Stream)>;

    #[allow(clippy::too_many_arguments)]
    fn spawn_connection_handler(
        &mut self,
        mut sink: Self::Sink,
        mut stream: Self::Stream,
        to_kernel: UnboundedSender<(InternalServiceRequest, Uuid)>,
        mut from_kernel: UnboundedReceiver<InternalServiceResponse>,
        conn_id: Uuid,
        tcp_connection_map: Arc<Mutex<HashMap<Uuid, UnboundedSender<InternalServiceResponse>>>>,
        server_connection_map: Arc<Mutex<HashMap<u64, Connection>>>,
    ) {
        tokio::task::spawn(async move {
            let write_task = async move {
                let response =
                    InternalServiceResponse::ServiceConnectionAccepted(ServiceConnectionAccepted);

                if let Err(err) = sink_send_payload::<Self>(response, &mut sink).await {
                    error!(target: "citadel", "Failed to send to client: {err:?}");
                    return;
                }

                while let Some(kernel_response) = from_kernel.recv().await {
                    if let Err(err) = sink_send_payload::<Self>(kernel_response, &mut sink).await {
                        error!(target: "citadel", "Failed to send to client: {err:?}");
                        return;
                    }
                }
            };

            let read_task = async move {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => {
                            if let InternalServicePayload::Request(request) = message {
                                if let Err(err) = send_to_kernel(request, &to_kernel, conn_id) {
                                    error!(target: "citadel", "Failed to send to kernel: {:?}", err);
                                    break;
                                }
                            }
                        }
                        Err(_) => {
                            warn!(target: "citadel", "Bad message from client");
                        }
                    }
                }
                info!(target: "citadel", "Disconnected");
            };

            tokio::select! {
                res0 = write_task => res0,
                res1 = read_task => res1,
            }

            tcp_connection_map.lock().await.remove(&conn_id);
            let mut server_connection_map = server_connection_map.lock().await;
            // Remove all connections whose associated_tcp_connection is conn_id
            server_connection_map.retain(|_, v| v.associated_tcp_connection != conn_id);
        });
    }
}

impl CitadelWorkspaceService<TcpIOInterface> {
    pub async fn new_tcp(
        bind_address: SocketAddr,
    ) -> std::io::Result<CitadelWorkspaceService<TcpIOInterface>> {
        Ok(TcpIOInterface::new(bind_address).await?.into())
    }
}
