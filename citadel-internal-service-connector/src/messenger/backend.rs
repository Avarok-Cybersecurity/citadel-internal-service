use crate::messenger::{InternalMessage, MessengerTx, StreamKey, WrappedMessage};
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use dashmap::DashMap;
use intersession_layer_messaging::{Backend, BackendError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone)]
pub struct CitadelWorkspaceBackend {
    cid: u64,
    expected_requests: Arc<DashMap<Uuid, tokio::sync::oneshot::Sender<InternalServiceResponse>>>,
    bypass_ism_outbound_tx: Option<UnboundedSender<(StreamKey, InternalMessage)>>,
    next_message_id: Arc<std::sync::atomic::AtomicU64>,
}

// HashMap<peer_cid, HashMap<message_id, wrapped_message>>
type State = HashMap<u64, HashMap<u64, WrappedMessage>>;

impl CitadelWorkspaceBackend {
    fn next_message_id(&self) -> u64 {
        self.next_message_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    #[allow(dead_code)]
    async fn wait_for_response(&self, request_id: Uuid) -> Option<InternalServiceResponse> {
        citadel_logging::debug!(target: "citadel", "[WAIT_FOR_RESPONSE] Starting wait_for_response for request_id={}", request_id);
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        loop {
            if start.elapsed() > timeout {
                citadel_logging::error!(target: "citadel", "[WAIT_FOR_RESPONSE] Timeout waiting for response to request_id={}", request_id);
                return None;
            }

            let (tx, rx) = tokio::sync::oneshot::channel();
            self.expected_requests.insert(request_id, tx);
            match rx.await {
                Ok(response) => {
                    citadel_logging::debug!(target: "citadel", "[WAIT_FOR_RESPONSE] Found response for request_id={}", request_id);
                    return Some(response);
                }
                Err(_) => {
                    citadel_logging::debug!(target: "citadel", "[WAIT_FOR_RESPONSE] No response yet for request_id={}, waiting...", request_id);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn process_message(
        &self,
        message: WrappedMessage,
    ) -> Result<(), BackendError<WrappedMessage>> {
        citadel_logging::debug!(target: "citadel", "[PROCESS_MESSAGE] Processing message: {:?}", message);

        // If this is a response to a request we made, handle it
        if let InternalServicePayload::Response(response) = &message.contents {
            if let Some(request_id) = response.request_id() {
                citadel_logging::debug!(target: "citadel", "[PROCESS_MESSAGE] Found response for request_id={}", request_id);
                if let Some(tx) = self.expected_requests.remove(request_id) {
                    let _ = tx.1.send(response.clone());
                    return Ok(());
                }
            }
        }

        // Store the message in the appropriate map
        match message.destination_id {
            id if id == self.cid => {
                // This is a message for us, store it in inbound
                citadel_logging::debug!(target: "citadel", "[PROCESS_MESSAGE] Storing inbound message from {}", message.source_id);
                self.store_inbound(message).await
            }
            _ => {
                // This is a message for someone else, store it in outbound
                citadel_logging::debug!(target: "citadel", "[PROCESS_MESSAGE] Storing outbound message to {}", message.destination_id);
                self.store_outbound(message).await
            }
        }
    }

    #[allow(dead_code)]
    async fn send_message(
        &self,
        peer_cid: u64,
        payload: InternalServicePayload,
    ) -> Result<InternalServiceResponse, BackendError<WrappedMessage>> {
        citadel_logging::debug!(target: "citadel", "[SEND_MESSAGE] Sending message to peer_cid={} with payload={:?}", peer_cid, payload);

        let request_id = if let InternalServicePayload::Request(request) = &payload {
            request.request_id().copied().unwrap_or_default()
        } else {
            return Err(BackendError::StorageError(
                "Invalid message contents".to_string(),
            ));
        };

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: peer_cid,
            message_id: self.next_message_id(),
            contents: payload,
        };

        // Store the message in the outbound map
        self.store_outbound(message.clone()).await?;

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message.clone());
            tx.send((stream_key, internal_message))
                .map_err(|_| BackendError::StorageError("Failed to send message".to_string()))?;
        }

        // Wait for the response
        citadel_logging::debug!(target: "citadel", "[SEND_MESSAGE] Waiting for response to request_id={}", request_id);
        self.wait_for_response(request_id)
            .await
            .ok_or(BackendError::StorageError(
                "Timeout waiting for response".to_string(),
            ))
    }

    async fn get_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = create_key_for(self.cid, INBOUND_MESSAGE_PREFIX);

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if let Some(response) = self.wait_for_response(request_id).await {
            match response {
                InternalServiceResponse::LocalDBGetKVSuccess(success_response) => {
                    citadel_logging::debug!(target: "citadel", "[GET_INBOUND_MAP] Got inbound map successfully");
                    let state: State =
                        bincode2::deserialize(&success_response.value).map_err(|err| {
                            BackendError::StorageError(format!(
                                "Failed to deserialize inbound map: {}",
                                err
                            ))
                        })?;
                    Ok(state)
                }
                InternalServiceResponse::LocalDBGetKVFailure(failure_response) => {
                    let failure_message = failure_response.message;
                    if failure_message == "Key not found" {
                        citadel_logging::debug!(target: "citadel", "[GET_INBOUND_MAP] Inbound map not found, initializing new one");
                        self.initialize_inbound_map().await
                    } else {
                        Err(BackendError::StorageError(format!(
                            "Failed to get inbound map: {}",
                            failure_message
                        )))
                    }
                }
                _ => Err(BackendError::StorageError(
                    "Unexpected response when getting inbound map".to_string(),
                )),
            }
        } else {
            Err(BackendError::StorageError(
                "No response received when getting inbound map".to_string(),
            ))
        }
    }

    async fn initialize_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = create_key_for(self.cid, INBOUND_MESSAGE_PREFIX);
        let new_state = State::new();

        let value = bincode2::serialize(&new_state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize inbound map: {}", err))
        })?;

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if let Some(response) = self.wait_for_response(request_id).await {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                citadel_logging::debug!(target: "citadel", "[INITIALIZE_INBOUND_MAP] Initialized inbound map successfully");
                Ok(new_state)
            } else {
                Err(BackendError::StorageError(
                    "Failed to initialize inbound map".to_string(),
                ))
            }
        } else {
            Err(BackendError::StorageError(
                "No response received when initializing inbound map".to_string(),
            ))
        }
    }

    async fn update_inbound_map(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let key = create_key_for(self.cid, INBOUND_MESSAGE_PREFIX);

        let value = bincode2::serialize(&state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize inbound map: {}", err))
        })?;

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[UPDATE_INBOUND_MAP] Updated inbound map successfully");
            Ok(())
        } else {
            Err(BackendError::StorageError(
                "No response received when updating inbound map".to_string(),
            ))
        }
    }

    async fn get_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = create_key_for(self.cid, OUTBOUND_MESSAGE_PREFIX);

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if let Some(response) = self.wait_for_response(request_id).await {
            match response {
                InternalServiceResponse::LocalDBGetKVSuccess(success_response) => {
                    citadel_logging::debug!(target: "citadel", "[GET_OUTBOUND_MAP] Got outbound map successfully");
                    let state: State =
                        bincode2::deserialize(&success_response.value).map_err(|err| {
                            BackendError::StorageError(format!(
                                "Failed to deserialize outbound map: {}",
                                err
                            ))
                        })?;
                    Ok(state)
                }
                InternalServiceResponse::LocalDBGetKVFailure(failure_response) => {
                    let failure_message = failure_response.message;
                    if failure_message == "Key not found" {
                        citadel_logging::debug!(target: "citadel", "[GET_OUTBOUND_MAP] Outbound map not found, initializing new one");
                        self.initialize_outbound_map().await
                    } else {
                        Err(BackendError::StorageError(format!(
                            "Failed to get outbound map: {}",
                            failure_message
                        )))
                    }
                }
                _ => Err(BackendError::StorageError(
                    "Unexpected response when getting outbound map".to_string(),
                )),
            }
        } else {
            Err(BackendError::StorageError(
                "No response received when getting outbound map".to_string(),
            ))
        }
    }

    async fn initialize_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = create_key_for(self.cid, OUTBOUND_MESSAGE_PREFIX);
        let new_state = State::new();

        let value = bincode2::serialize(&new_state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize outbound map: {}", err))
        })?;

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[INITIALIZE_OUTBOUND_MAP] Initialized outbound map successfully");
            Ok(new_state)
        } else {
            Err(BackendError::StorageError(
                "No response received when initializing outbound map".to_string(),
            ))
        }
    }

    async fn update_outbound_map(
        &self,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let key = create_key_for(self.cid, OUTBOUND_MESSAGE_PREFIX);

        let value = bincode2::serialize(&state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize outbound map: {}", err))
        })?;

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[UPDATE_OUTBOUND_MAP] Updated outbound map successfully");
            Ok(())
        } else {
            Err(BackendError::StorageError(
                "No response received when updating outbound map".to_string(),
            ))
        }
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

        let mut outbound = self.get_outbound_map().await?;
        let peer_messages = outbound.entry(peer_cid).or_insert_with(HashMap::new);
        peer_messages.insert(message_id, message);

        self.update_outbound_map(request_id, outbound).await?;

        Ok(())
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

        let mut inbound = self.get_inbound_map().await?;
        let peer_messages = inbound.entry(peer_cid).or_insert_with(HashMap::new);
        peer_messages.insert(message_id, message);

        self.update_inbound_map(request_id, inbound).await?;

        Ok(())
    }

    async fn clear_message_inbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let mut inbound = self.get_inbound_map().await?;
        if let Some(peer_messages) = inbound.get_mut(&peer_id) {
            peer_messages.remove(&message_id);
        }
        self.update_inbound_map(Uuid::new_v4(), inbound).await
    }

    async fn clear_message_outbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        let mut outbound = self.get_outbound_map().await?;
        if let Some(peer_messages) = outbound.get_mut(&peer_id) {
            peer_messages.remove(&message_id);
        }
        self.update_outbound_map(Uuid::new_v4(), outbound).await
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        let outbound = self.get_outbound_map().await?;
        Ok(outbound
            .values()
            .flat_map(|messages| messages.values().cloned())
            .collect())
    }

    async fn get_pending_inbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        let inbound = self.get_inbound_map().await?;
        Ok(inbound
            .values()
            .flat_map(|messages| messages.values().cloned())
            .collect())
    }

    async fn store_value(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let unique_key = create_key_for(self.cid, key);

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key: unique_key,
            value: value.to_vec(),
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0, // Send to the internal service
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if self.wait_for_response(request_id).await.is_some() {
            citadel_logging::debug!(target: "citadel", "[STORE_VALUE] Stored value for key={}", key);
            Ok(())
        } else {
            Err(BackendError::StorageError(format!(
                "Failed to store value for key={}",
                key
            )))
        }
    }

    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let unique_key = create_key_for(self.cid, key);

        let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key: unique_key,
        });

        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0, // Send to the internal service
            message_id: self.next_message_id(),
            contents: request,
        };

        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            let stream_key = StreamKey {
                cid: 0,
                stream_id: 1,
            };
            let internal_message = InternalMessage::Message(message);
            tx.send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;
        }

        if let Some(response) = self.wait_for_response(request_id).await {
            citadel_logging::debug!(target: "citadel", "[LOAD_VALUE] Loaded value for key={}", key);
            match response {
                InternalServiceResponse::LocalDBGetKVSuccess(success) => Ok(Some(success.value)),
                _ => Ok(None),
            }
        } else {
            Err(BackendError::StorageError(format!(
                "Failed to load value for key={}",
                key
            )))
        }
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
        citadel_logging::debug!(target: "citadel", "[CP0] Inspecting received payload: {:?}", response);
        if let Some(id) = response.request_id() {
            citadel_logging::debug!(target: "citadel", "[CP1] Inspecting received payload: {:?}", response);
            if let Some(tx) = self.expected_requests.remove(id) {
                citadel_logging::debug!(target: "citadel", "[CP2] Inspecting received payload: {:?}", response);
                let _ = tx.1.send(response.clone());
                citadel_logging::debug!(target: "citadel", "[CP3] Inspecting received payload");
                return Ok(None);
            }
        }

        citadel_logging::debug!(target: "citadel", "[CP4] Inspecting received payload: {:?}", response);
        Ok(Some(response))
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

fn create_key_for(session_cid: u64, prefix: &str) -> String {
    format!("{}-{}", prefix, session_cid)
}

const INBOUND_MESSAGE_PREFIX: &str = "__inbound-citadel-messenger";
const OUTBOUND_MESSAGE_PREFIX: &str = "__outbound-citadel-messenger";
