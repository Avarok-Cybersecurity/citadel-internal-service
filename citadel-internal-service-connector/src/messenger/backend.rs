use crate::messenger::{InternalMessage, MessengerTx, StreamKey, WrappedMessage};
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use dashmap::DashMap;
use intersession_layer_messaging::{Backend, BackendError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time;
use uuid::Uuid;

#[derive(Clone)]
pub struct CitadelWorkspaceBackend {
    pub cid: u64,
    expected_requests: Arc<DashMap<Uuid, tokio::sync::oneshot::Sender<InternalServiceResponse>>>,
    bypass_ism_outbound_tx: Option<UnboundedSender<(StreamKey, InternalMessage)>>,
    next_message_id: Arc<std::sync::atomic::AtomicU64>,
}

// HashMap<peer_cid, HashMap<message_id, wrapped_message>>
type State = HashMap<u64, HashMap<u64, WrappedMessage>>;

// Constants for storage prefixes
pub const INBOUND_MESSAGE_PREFIX: &str = "inbound_messages";
pub const OUTBOUND_MESSAGE_PREFIX: &str = "outbound_messages";

impl CitadelWorkspaceBackend {
    fn next_message_id(&self) -> u64 {
        self.next_message_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    async fn wait_for_response(&self, request_id: Uuid) -> Option<InternalServiceResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.expected_requests.insert(request_id, tx);

        // Add a timeout to prevent infinite waiting
        match time::timeout(std::time::Duration::from_secs(5), rx).await {
            Ok(result) => result.ok(),
            Err(_) => {
                // Remove the request from expected_requests if it times out
                self.expected_requests.remove(&request_id);
                citadel_logging::warn!(target: "citadel", "Timeout waiting for response to request_id: {}", request_id);
                None
            }
        }
    }

    /// Sends a message to the network layer
    pub async fn send_to_network(
        &self,
        request: InternalServiceRequest,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0, // Send to the internal service
            message_id: self.next_message_id(),
            contents: InternalServicePayload::Request(request),
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message.clone());
            tx.send((stream_key, internal_message)).map_err(|err| {
                BackendError::StorageError(format!("Failed to send message: {}", err))
            })?;
        }

        Ok(())
    }

    /// Generic function to get a map (inbound or outbound)
    pub async fn get_map(&self, prefix: &str) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = format!("{}-{}", prefix, self.cid);

        let request = InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
        };

        self.send_to_network(request).await?;

        if let Some(response) = self.wait_for_response(request_id).await {
            match response {
                InternalServiceResponse::LocalDBGetKVSuccess(success_response) => {
                    citadel_logging::debug!(target: "citadel", "[GET_MAP] Got {} map successfully", prefix);
                    let state: State =
                        bincode2::deserialize(&success_response.value).map_err(|err| {
                            BackendError::StorageError(format!(
                                "Failed to deserialize {} map: {}",
                                prefix, err
                            ))
                        })?;
                    Ok(state)
                }
                InternalServiceResponse::LocalDBGetKVFailure(failure_response) => {
                    let failure_message = failure_response.message;
                    if failure_message == "Key not found" {
                        citadel_logging::debug!(target: "citadel", "[GET_MAP] {} map not found, initializing new one", prefix);
                        self.initialize_map(prefix).await
                    } else {
                        Err(BackendError::StorageError(format!(
                            "Failed to get {} map: {}",
                            prefix, failure_message
                        )))
                    }
                }
                _ => Err(BackendError::StorageError(format!(
                    "Unexpected response when getting {} map",
                    prefix
                ))),
            }
        } else {
            // If we get no response, initialize a new map as a fallback
            citadel_logging::warn!(target: "citadel", "[GET_MAP] No response received for {} map, initializing new one", prefix);
            Ok(State::new())
        }
    }

    /// Generic function to initialize a map (inbound or outbound)
    async fn initialize_map(&self, prefix: &str) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = format!("{}-{}", prefix, self.cid);
        let new_state = State::new();

        let value = bincode2::serialize(&new_state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize {} map: {}", prefix, err))
        })?;

        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        };

        self.send_to_network(request).await?;

        if let Some(response) = self.wait_for_response(request_id).await {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                citadel_logging::debug!(target: "citadel", "[INITIALIZE_MAP] Initialized {} map successfully", prefix);
                Ok(new_state)
            } else {
                Err(BackendError::StorageError(format!(
                    "Failed to initialize {} map",
                    prefix
                )))
            }
        } else {
            // If we get no response, assume the initialization worked
            citadel_logging::warn!(target: "citadel", "[INITIALIZE_MAP] No response received when initializing {} map, assuming success", prefix);
            Ok(new_state)
        }
    }

    /// Generic function to update a map (inbound or outbound)
    pub async fn update_map(
        &self,
        prefix: &str,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let key = format!("{}-{}", prefix, self.cid);

        let value = bincode2::serialize(&state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize {} map: {}", prefix, err))
        })?;

        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        };

        self.send_to_network(request).await?;

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[UPDATE_MAP] Updated {} map successfully", prefix);
            Ok(())
        } else {
            // If we get no response, assume the update worked
            citadel_logging::warn!(target: "citadel", "[UPDATE_MAP] No response received when updating {} map, assuming success", prefix);
            Ok(())
        }
    }

    // Convenience methods that use the generic functions
    async fn get_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        self.get_map(INBOUND_MESSAGE_PREFIX).await
    }

    async fn get_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        self.get_map(OUTBOUND_MESSAGE_PREFIX).await
    }

    async fn update_inbound_map(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        self.update_map(INBOUND_MESSAGE_PREFIX, request_id, state)
            .await
    }

    async fn update_outbound_map(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        self.update_map(OUTBOUND_MESSAGE_PREFIX, request_id, state)
            .await
    }

    pub fn add_expected_request(&self, request_id: Uuid) {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        self.expected_requests.insert(request_id, tx);
    }
}

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
            Uuid::new_v4()
        };

        citadel_logging::debug!(target: "citadel", "[STORE_OUTBOUND] Storing outbound message: source_id={}, destination_id={}, message_id={}", 
            message.source_id, message.destination_id, message.message_id);

        let mut outbound = match self.get_outbound_map().await {
            Ok(map) => map,
            Err(e) => {
                // If we get a delivery error, log it and create a new map
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[STORE_OUTBOUND] Failed to get outbound map due to delivery error, creating new one");
                    State::new()
                } else {
                    return Err(e);
                }
            }
        };

        let peer_messages = outbound.entry(peer_cid).or_insert_with(HashMap::new);
        peer_messages.insert(message_id, message);

        match self.update_outbound_map(request_id, outbound).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If we get a delivery error, log it and return success
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[STORE_OUTBOUND] Failed to update outbound map due to delivery error, assuming success");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn store_inbound(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let message_id = message.message_id;
        let peer_cid = message.source_id; // Use source_id for inbound messages
        let request_id = if let InternalServicePayload::Request(request) = &message.contents {
            request.request_id().copied().unwrap_or_default()
        } else {
            Uuid::new_v4()
        };

        citadel_logging::debug!(target: "citadel", "[STORE_INBOUND] Storing inbound message: source_id={}, destination_id={}, message_id={}", 
            message.source_id, message.destination_id, message.message_id);

        let mut inbound = match self.get_inbound_map().await {
            Ok(map) => map,
            Err(e) => {
                // If we get a delivery error, log it and create a new map
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[STORE_INBOUND] Failed to get inbound map due to delivery error, creating new one");
                    State::new()
                } else {
                    return Err(e);
                }
            }
        };

        let peer_messages = inbound.entry(peer_cid).or_insert_with(HashMap::new);
        peer_messages.insert(message_id, message);

        match self.update_inbound_map(request_id, inbound).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If we get a delivery error, log it and return success
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[STORE_INBOUND] Failed to update inbound map due to delivery error, assuming success");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn clear_message_inbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let mut inbound = match self.get_inbound_map().await {
            Ok(map) => map,
            Err(e) => {
                // If we get a delivery error, log it and create a new map
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[CLEAR_INBOUND] Failed to get inbound map due to delivery error, creating new one");
                    State::new()
                } else {
                    return Err(e);
                }
            }
        };

        if let Some(peer_messages) = inbound.get_mut(&peer_id) {
            peer_messages.remove(&message_id);
        }

        match self.update_inbound_map(Uuid::new_v4(), inbound).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If we get a delivery error, log it and return success
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[CLEAR_INBOUND] Failed to update inbound map due to delivery error, assuming success");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn clear_message_outbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let mut outbound = match self.get_outbound_map().await {
            Ok(map) => map,
            Err(e) => {
                // If we get a delivery error, log it and create a new map
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[CLEAR_OUTBOUND] Failed to get outbound map due to delivery error, creating new one");
                    State::new()
                } else {
                    return Err(e);
                }
            }
        };

        if let Some(peer_messages) = outbound.get_mut(&peer_id) {
            peer_messages.remove(&message_id);
        }

        match self.update_outbound_map(Uuid::new_v4(), outbound).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If we get a delivery error, log it and return success
                let err_str = format!("{:?}", e);
                if err_str.contains("Failed to deliver message") {
                    citadel_logging::warn!(target: "citadel", "[CLEAR_OUTBOUND] Failed to update outbound map due to delivery error, assuming success");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        loop {
            match self.get_outbound_map().await {
                Ok(outbound) => return Ok(outbound
                    .values()
                    .flat_map(|messages| messages.values().cloned())
                    .collect()),
                Err(e) => {
                    // If we get a delivery error, log it and return an empty vector
                    let err_str = format!("{:?}", e);
                    if err_str.contains("Failed to deliver message") || err_str.contains("get_kv: Server connection not found") {
                        citadel_logging::warn!(target: "citadel", "[GET_PENDING_OUTBOUND] Failed to get outbound map due to likely no connection up yet");
                        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                        continue
                    } else {
                        return Err(e)
                    }
                }
            }
        }
    }

    async fn get_pending_inbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        loop {
            match self.get_inbound_map().await {
                Ok(inbound) => return Ok(inbound
                    .values()
                    .flat_map(|messages| messages.values().cloned())
                    .collect()),
                Err(e) => {

                    // If we get a delivery error, log it and return an empty vector
                    let err_str = format!("{:?}", e);
                    if err_str.contains("Failed to deliver message") || err_str.contains("get_kv: Server connection not found") {
                        citadel_logging::warn!(target: "citadel", "[GET_PENDING_INBOUND] Failed to get inbound map likely due to likely no connection up yet");
                        tokio::time::sleep(Duration::from_millis(5000)).await;
                        continue;
                    } else {
                        return Err(e)
                    }
                }
            }
        }
    }

    async fn store_value(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let unique_key = format!("{}-{}", key, self.cid);

        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key: unique_key,
            value: value.to_vec(),
        };

        self.send_to_network(request).await?;

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[STORE_VALUE] Stored value for key={}", key);
            Ok(())
        } else {
            // If we get no response, assume the store worked
            citadel_logging::warn!(target: "citadel", "[STORE_VALUE] No response received when storing value for key={}, assuming success", key);
            Ok(())
        }
    }

    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let unique_key = format!("{}-{}", key, self.cid);

        let request = InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key: unique_key,
        };

        self.send_to_network(request).await?;

        if let Some(response) = self.wait_for_response(request_id).await {
            citadel_logging::debug!(target: "citadel", "[LOAD_VALUE] Loaded value for key={}", key);
            match response {
                InternalServiceResponse::LocalDBGetKVSuccess(success) => Ok(Some(success.value)),
                _ => Ok(None),
            }
        } else {
            // If we get no response, assume the key doesn't exist
            citadel_logging::warn!(target: "citadel", "[LOAD_VALUE] No response received when loading value for key={}, assuming key doesn't exist", key);
            Ok(None)
        }
    }
}

#[async_trait]
pub trait CitadelBackendExt: Backend<WrappedMessage> + Clone + Send + Sync + 'static {
    /// Creates a new instance of the backend
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
impl CitadelBackendExt for CitadelWorkspaceBackend {
    async fn new(
        cid: u64,
        handle: &MessengerTx<Self>,
    ) -> Result<Self, BackendError<WrappedMessage>> {
        Ok(Self {
            cid,
            expected_requests: Arc::new(DashMap::new()),
            bypass_ism_outbound_tx: Some(handle.bypass_ism_outbound_tx.clone()),
            next_message_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    async fn inspect_received_payload(
        &self,
        response: InternalServiceResponse,
    ) -> Result<Option<InternalServiceResponse>, BackendError<WrappedMessage>> {
        citadel_logging::debug!(target: "citadel", "Inspecting received payload: {:?}", response);

        if let Some(id) = response.request_id() {
            if let Some(tx) = self.expected_requests.remove(id) {
                let _ = tx.1.send(response.clone());
                return Ok(None);
            }
        }

        Ok(Some(response))
    }
}
