use crate::messenger::{InternalMessage, MessengerTx, StreamKey, WrappedMessage};
use async_trait::async_trait;
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use dashmap::DashMap;
use intersession_layer_messaging::testing::InMemoryBackend;
use intersession_layer_messaging::{Backend, BackendError};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

#[derive(Clone)]
pub struct CitadelWorkspaceBackend {
    cid: u64,
    expected_requests: Arc<DashMap<Uuid, ()>>,
    bypass_ism_outbound_tx: UnboundedSender<(StreamKey, InternalMessage)>,
}

impl CitadelWorkspaceBackend {
    fn send_request(
        &self,
        request: InternalServiceRequest,
    ) -> Result<(), Box<BackendError<WrappedMessage>>> {
        let stream_key = StreamKey::bypass_ism();
        let message = InternalMessage::Message(WrappedMessage {
            source_id: self.cid,
            destination_id: 0, // Does not matter
            message_id: 0,     // Does not matter either, bypassing ISM
            contents: request.into(),
        });

        self.bypass_ism_outbound_tx
            .send((stream_key, message))
            .map_err(|err| {
                let reason = err.to_string();
                let InternalMessage::Message(message) = err.0 .1 else {
                    unreachable!("Bypass ISM should only send messages");
                };

                BackendError::SendFailed { reason, message }
            })?;

        Ok(())
    }
}

#[async_trait]
impl Backend<WrappedMessage> for CitadelWorkspaceBackend {
    async fn store_outbound(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        Ok(())
    }

    async fn store_inbound(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        Ok(())
    }

    async fn clear_message_inbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        Ok(())
    }

    async fn clear_message_outbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        Ok(())
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        Ok(vec![])
    }

    async fn get_pending_inbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        Ok(vec![])
    }
    // Simple K/V interface for tracker state
    async fn store_value(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), BackendError<WrappedMessage>> {
        Ok(())
    }

    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<WrappedMessage>> {
        Ok(None)
    }
}

#[async_trait]
pub trait CitadelBackendExt: Backend<WrappedMessage> + Clone + Send + Sync + 'static {
    async fn new(
        cid: u64,
        handle: &MessengerTx<Self>,
    ) -> Result<Self, BackendError<WrappedMessage>>;
    /// Inspects a payload to see if it is relevant to the backend. If it is, the response
    /// is not returned. Otherwise, the response is returned to the caller for further processing.
    async fn inspect_received_payload(
        &self,
        response: InternalServiceResponse,
    ) -> Result<Option<InternalServiceResponse>, BackendError<WrappedMessage>> {
        Ok(Some(response))
    }
}

#[async_trait]
impl CitadelBackendExt for InMemoryBackend<WrappedMessage> {
    async fn new(
        _cid: u64,
        _handle: &MessengerTx<Self>,
    ) -> Result<Self, BackendError<WrappedMessage>> {
        Ok(Self::default())
    }
}

#[async_trait]
impl CitadelBackendExt for CitadelWorkspaceBackend {
    async fn new(
        cid: u64,
        handle: &MessengerTx<Self>,
    ) -> Result<Self, BackendError<WrappedMessage>> {
        Ok(Self {
            cid,
            expected_requests: Arc::new(DashMap::new()),
            bypass_ism_outbound_tx: handle.bypass_ism_outbound_tx.clone(),
        })
    }
}
