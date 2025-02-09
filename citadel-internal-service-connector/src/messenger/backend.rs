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

    async fn get_registered_peers(&self) -> Result<Vec<u64>, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let request =
            InternalServicePayload::Request(InternalServiceRequest::ListRegisteredPeers {
                request_id,
                cid: self.cid,
            });
        let message = WrappedMessage {
            source_id: self.cid,
            destination_id: 0,
            message_id: 0,
            contents: request,
        };
        let stream_key = StreamKey::bypass_ism();
        let internal_message = InternalMessage::Message(message);
        self.bypass_ism_outbound_tx
            .send((stream_key, internal_message))
            .map_err(|err| BackendError::StorageError(err.to_string()))?;

        if let Some(response) = self.wait_for_response(request_id).await {
            if let InternalServiceResponse::ListRegisteredPeersResponse(success_response) = response
            {
                Ok(success_response.peers.iter().map(|p| *p.0).collect())
            } else {
                Err(BackendError::StorageError(
                    "Failed to get registered peers".to_string(),
                ))
            }
        } else {
            Err(BackendError::StorageError(
                "No response received when fetching registered peers".to_string(),
            ))
        }
    }

    async fn get_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let peers = self.get_registered_peers().await?;

        let mut state: State = HashMap::new();

        for peer_cid in peers {
            let request_id = Uuid::new_v4();
            let key = create_key_for(self.cid, &format!("{}{}", INBOUND_MESSAGE_PREFIX, peer_cid));

            let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBGetKV {
                request_id,
                cid: self.cid,
                peer_cid: Some(peer_cid),
                key,
            });

            let message = WrappedMessage {
                source_id: self.cid,
                destination_id: 0,
                message_id: 0,
                contents: request,
            };

            let stream_key = StreamKey::bypass_ism();
            let internal_message = InternalMessage::Message(message);

            self.bypass_ism_outbound_tx
                .send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;

            if let Some(response) = self.wait_for_response(request_id).await {
                if let InternalServiceResponse::LocalDBGetKVSuccess(success_response) = response {
                    let peer_messages: HashMap<u64, WrappedMessage> =
                        bincode2::deserialize(&success_response.value).map_err(|err| {
                            BackendError::StorageError(format!(
                                "Failed to deserialize peer messages: {}",
                                err
                            ))
                        })?;
                    state.insert(peer_cid, peer_messages);
                } else {
                    return Err(BackendError::StorageError(
                        "Failed to get inbound messages".to_string(),
                    ));
                }
            } else {
                return Err(BackendError::StorageError(
                    "No response received when fetching inbound messages".to_string(),
                ));
            }
        }

        Ok(state)
    }

    async fn get_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        let peers = self.get_registered_peers().await?;

        let mut state: State = HashMap::new();

        for peer_cid in peers {
            let request_id = Uuid::new_v4();
            let key = create_key_for(
                self.cid,
                &format!("{}{}", OUTBOUND_MESSAGE_PREFIX, peer_cid),
            );

            let request = InternalServicePayload::Request(InternalServiceRequest::LocalDBGetKV {
                request_id,
                cid: self.cid,
                peer_cid: Some(peer_cid),
                key,
            });

            let message = WrappedMessage {
                source_id: self.cid,
                destination_id: 0,
                message_id: 0,
                contents: request,
            };

            let stream_key = StreamKey::bypass_ism();
            let internal_message = InternalMessage::Message(message);

            self.bypass_ism_outbound_tx
                .send((stream_key, internal_message))
                .map_err(|err| BackendError::StorageError(err.to_string()))?;

            if let Some(response) = self.wait_for_response(request_id).await {
                if let InternalServiceResponse::LocalDBGetKVSuccess(success_response) = response {
                    let peer_messages: HashMap<u64, WrappedMessage> =
                        bincode2::deserialize(&success_response.value).map_err(|err| {
                            BackendError::StorageError(format!(
                                "Failed to deserialize peer messages: {}",
                                err
                            ))
                        })?;
                    state.insert(peer_cid, peer_messages);
                } else {
                    return Err(BackendError::StorageError(
                        "Failed to get outbound messages".to_string(),
                    ));
                }
            } else {
                return Err(BackendError::StorageError(
                    "No response received when fetching outbound messages".to_string(),
                ));
            }
        }

        Ok(state)
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
        let mut inbound = self.get_inbound_map().await?;
        if let Some(peer_messages) = inbound.get_mut(&peer_id) {
            peer_messages.remove(&message_id);
        }
        let request_id = Uuid::new_v4();
        self.sync_inbound_state(request_id, inbound).await
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
        let request_id = Uuid::new_v4();
        self.sync_outbound_state(request_id, outbound).await
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        let outbound = self.get_outbound_map().await?;
        Ok(outbound
            .values()
            .flat_map(|peer_messages| peer_messages.values().cloned())
            .collect())
    }

    async fn get_pending_inbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        let inbound = self.get_inbound_map().await?;
        Ok(inbound
            .values()
            .flat_map(|peer_messages| peer_messages.values().cloned())
            .collect())
    }

    // Simple K/V interface for tracker state
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
            destination_id: 0,
            message_id: 0,
            contents: request,
        };

        let stream_key = StreamKey::bypass_ism();
        let internal_message = InternalMessage::Message(message);

        self.bypass_ism_outbound_tx
            .send((stream_key, internal_message))
            .map_err(|err| BackendError::StorageError(err.to_string()))?;

        if let Some(response) = self.wait_for_response(request_id).await {
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                Ok(())
            } else {
                Err(BackendError::StorageError(format!(
                    "Failed to store value for key: {}",
                    key
                )))
            }
        } else {
            Err(BackendError::StorageError(format!(
                "No response when storing value for key: {}",
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
            destination_id: 0,
            message_id: 0,
            contents: request,
        };

        let stream_key = StreamKey::bypass_ism();
        let internal_message = InternalMessage::Message(message);

        self.bypass_ism_outbound_tx
            .send((stream_key, internal_message))
            .map_err(|err| BackendError::StorageError(err.to_string()))?;

        if let Some(response) = self.wait_for_response(request_id).await {
            if let InternalServiceResponse::LocalDBGetKVSuccess(success_response) = response {
                Ok(Some(success_response.value))
            } else {
                Ok(None)
            }
        } else {
            Err(BackendError::StorageError(format!(
                "Failed to load value for key: {}",
                key
            )))
        }
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
