use crate::messenger::{InternalMessage, MessengerTx, StreamKey, WrappedMessage};
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use dashmap::DashMap;
use intersession_layer_messaging::testing::InMemoryBackend;
use intersession_layer_messaging::{Backend, BackendError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

#[derive(Clone)]
pub struct CitadelWorkspaceBackend {
    cid: u64,
    expected_requests: Arc<DashMap<Uuid, tokio::sync::oneshot::Sender<InternalServiceResponse>>>,
    bypass_ism_outbound_tx: UnboundedSender<(StreamKey, InternalMessage)>,
}

// HashMap<peer_cid, HashMap<message_id, wrapped_message>>
type State = HashMap<u64, HashMap<u64, WrappedMessage>>;
impl CitadelWorkspaceBackend {
    async fn wait_for_response(&self, request_id: Uuid) -> Option<InternalServiceResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.expected_requests.insert(request_id, tx);
        rx.await.ok()
    }

    async fn get_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        // Step 1: Build the request to get the inbound map
        // Step 2: call self.wait_for_response()
    }

    async fn get_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        // Step 1: Build the request to get the inbound map
        // Step 2: call self.wait_for_response()
    }

    async fn sync_inbound_state(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let value = bincode2::serialize(&state).unwrap();
        let key = create_key_for(self.cid, INBOUND_MESSAGE_PREFIX);

        let outbound_request =
            InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
                request_id,
                cid: self.cid,
                peer_cid: None,
                key,
                value,
            });

        let message_wrapped = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: 0,
            contents: outbound_request,
        };

        let stream_key = StreamKey::bypass_ism();
        let internal_message = InternalMessage::Message(message_wrapped);

        self.bypass_ism_outbound_tx
            .send((stream_key, internal_message))
            .map_err(|err| BackendError::StorageError(err.to_string()))
    }

    async fn sync_outbound_state(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let value = bincode2::serialize(&state).unwrap();
        let key = create_key_for(self.cid, OUTBOUND_MESSAGE_PREFIX);

        let outbound_request =
            InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
                request_id,
                cid: self.cid,
                peer_cid: None,
                key,
                value,
            });

        let message_wrapped = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: 0,
            contents: outbound_request,
        };

        let stream_key = StreamKey::bypass_ism();
        let internal_message = InternalMessage::Message(message_wrapped);

        self.bypass_ism_outbound_tx
            .send((stream_key, internal_message))
            .map_err(|err| BackendError::StorageError(err.to_string()))
    }
}

fn create_key_for(session_cid: u64, prefix: &str) -> String {
    format!("__{}-{}", prefix, session_cid)
}

const OUTBOUND_MESSAGE_PREFIX: &str = "outbound-citadel-messenger";
const INBOUND_MESSAGE_PREFIX: &str = "inbound-citadel-messenger";

#[async_trait]
impl Backend<WrappedMessage> for CitadelWorkspaceBackend {
    async fn store_outbound(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let message_id = message.message_id;
        let peer_cid = message.destination_id;
        let request_id = if let InternalServicePayload::Request(request) = &message.contents {
            request.request_id().copied().unwrap_or_default()
        } else {
            return Err(BackendError::StorageError(
                "Invalid message contents".to_string(),
            ));
        };

        let mut outbound = self.get_outbound_map().await?;
        outbound
            .entry(peer_cid)
            .or_default()
            .insert(message_id, message);

        self.sync_outbound_state(request_id, outbound).await
    }

    async fn store_inbound(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let message_id = message.message_id;
        let peer_cid = message.destination_id;
        let request_id = if let InternalServicePayload::Request(request) = &message.contents {
            request.request_id().copied().unwrap_or_default()
        } else {
            return Err(BackendError::StorageError(
                "Invalid message contents".to_string(),
            ));
        };

        let mut inbound = self.get_inbound_map().await?;
        inbound
            .entry(peer_cid)
            .or_default()
            .insert(message_id, message);

        self.sync_inbound_state(request_id, inbound).await
    }

    async fn clear_message_inbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        // HashMap<local_cid, HashMap<peer_cid, HashMap<message_id, message_contents>>>
        // get map, delete item, then sync map
        Ok(())
    }

    async fn clear_message_outbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        // get map, delete item, sync map
        Ok(())
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        // get map, run iterator over all and collect into vec
        Ok(vec![])
    }

    async fn get_pending_inbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        // get map, run iterator over all and collect into vec
        Ok(vec![])
    }

    // Simple K/V interface for tracker state
    async fn store_value(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), BackendError<WrappedMessage>> {
        // make the 'key' unique using the same method as we have (use cid, and some __prefix)
        // then store to the backend using LocalDBSetKV
        Ok(())
    }

    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<WrappedMessage>> {
        // make the 'key' unique using the same method as we have (use cid, and some __prefix)
        // then pull from the backend using LocalDBGetKV
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

    async fn inspect_received_payload(
        &self,
        response: InternalServiceResponse,
    ) -> Result<Option<InternalServiceResponse>, BackendError<WrappedMessage>> {
        if let Some(id) = response.request_id() {
            if let Some((_, tx)) = self.expected_requests.remove(id) {
                if let Err(err) = tx.send(response) {
                    Ok(Some(err))
                } else {
                    Ok(None)
                }
            } else {
                Ok(Some(response))
            }
        } else {
            Ok(Some(response))
        }
    }
}
