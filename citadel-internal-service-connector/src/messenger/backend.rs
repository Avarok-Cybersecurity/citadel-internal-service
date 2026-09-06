use crate::messenger::backend_map::{mutate, MapStore, State};
use crate::messenger::{sleep_internal, timeout_internal, BypasserTx, MessengerTx, WrappedMessage};
use async_trait::async_trait;
use citadel_internal_service_types::{
    BatchedResponseData, InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
    KEY_NOT_FOUND,
};
use citadel_io::tokio::sync::Mutex;
use dashmap::DashMap;
use intersession_layer_messaging::{Backend, BackendError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone)]
pub struct CitadelWorkspaceBackend {
    pub cid: u64,
    expected_requests:
        Arc<DashMap<Uuid, citadel_io::tokio::sync::oneshot::Sender<InternalServiceResponse>>>,
    bypass_ism_outbound_tx: Option<BypasserTx>,
    // Each map is one serialized blob under one key, so every mutation is a
    // read-whole/modify/write-whole. Two of them interleaving lose one of the
    // two changes -- and the lost one was reported `Ok`. Held across read AND
    // write; see messenger/backend_map.rs for the interleave and its limits.
    // Separate gates because the two maps are separate keys and never mutate
    // together.
    outbound_gate: Arc<Mutex<()>>,
    inbound_gate: Arc<Mutex<()>>,
}

// Constants for storage prefixes
pub const INBOUND_MESSAGE_PREFIX: &str = "inbound_messages";
pub const OUTBOUND_MESSAGE_PREFIX: &str = "outbound_messages";

/// Did that write actually happen?
///
/// Separated from the socket so the decision can be tested exhaustively against
/// real response values instead of a mocked agent, and so the three write paths
/// share ONE answer. They did not: `update_map` and `store_value` asked
/// `wait_for_response(..).is_some()` — the presence of a reply — while
/// `store_values_batched` matched the variant and carried a comment saying all
/// three "must agree". A `LocalDBSetKVFailure` is a reply, and the agent sends
/// one on a backend error, on a failed `propose_target`, and on the ownership-gate
/// refusal. So a refused write returned `Ok(())`, the map read as stored, ILM read
/// as queued, and the sender saw a message as sent that nothing would retransmit.
pub(crate) fn write_outcome(
    response: Option<InternalServiceResponse>,
    what: &str,
) -> Result<(), BackendError<WrappedMessage>> {
    match response {
        Some(InternalServiceResponse::LocalDBSetKVSuccess(_)) => Ok(()),
        Some(other) => Err(BackendError::StorageError(format!(
            "Writing {what} was refused or failed: {other:?}"
        ))),
        // A timeout is not a success either; the caller must be able to retry.
        None => Err(BackendError::StorageError(format!(
            "Timed out writing {what}; the change may not be stored"
        ))),
    }
}

/// Absent, present, or unreadable — three outcomes, not two.
///
/// `load_values_batched` folded every non-success into `None`, so a backend
/// error read as "no such key". `MessageTracker::new` then starts with an empty
/// delivery frontier: already-received messages are re-delivered, ACK state is
/// reset and the next-id counter restarts, on an error that should have failed
/// initialisation. `get_map` draws this distinction and explains it; this is the
/// same mechanism in the batched path, which it was never carried to.
pub(crate) fn read_outcome(
    response: InternalServiceResponse,
    key: &str,
) -> Result<Option<Vec<u8>>, BackendError<WrappedMessage>> {
    match response {
        InternalServiceResponse::LocalDBGetKVSuccess(success) => Ok(Some(success.value)),
        InternalServiceResponse::LocalDBGetKVFailure(failure)
            if failure.message == KEY_NOT_FOUND =>
        {
            Ok(None)
        }
        InternalServiceResponse::LocalDBGetKVFailure(failure) => Err(BackendError::StorageError(
            format!("Failed to read key={key}: {}", failure.message),
        )),
        other => Err(BackendError::StorageError(format!(
            "Unexpected response reading key={key}: {other:?}"
        ))),
    }
}

impl CitadelWorkspaceBackend {
    async fn wait_for_response(&self, request_id: Uuid) -> Option<InternalServiceResponse> {
        let (tx, rx) = citadel_io::tokio::sync::oneshot::channel();
        self.expected_requests.insert(request_id, tx);
        citadel_logging::info!(target: "citadel", "[BACKEND-WAIT] Waiting for response to request_id: {} (CID: {})", request_id, self.cid);

        // Add a timeout to prevent infinite waiting (using platform-agnostic timeout)
        match timeout_internal(Duration::from_secs(5), rx).await {
            Ok(result) => {
                let response = result.ok();
                citadel_logging::info!(target: "citadel", "[BACKEND-WAIT] Received response for request_id {}: {:?}", request_id, response.as_ref().map(|r| std::any::type_name_of_val(r)));
                response
            }
            Err(_) => {
                // Remove the request from expected_requests if it times out
                self.expected_requests.remove(&request_id);
                citadel_logging::warn!(target: "citadel", "[BACKEND-WAIT] TIMEOUT waiting for response to request_id: {} (CID: {}, pending requests: {})",
                    request_id, self.cid, self.expected_requests.len());
                None
            }
        }
    }

    /// Sends a message to the network layer
    pub async fn send_to_network(
        &self,
        request: InternalServiceRequest,
    ) -> Result<(), BackendError<WrappedMessage>> {
        citadel_logging::info!(target: "citadel", "[BACKEND-NETWORK] send_to_network called for CID {} with request: {:?}", self.cid, std::any::type_name_of_val(&request));
        // Send the message to the network layer
        if let Some(tx) = &self.bypass_ism_outbound_tx {
            tx.send(request).await.map_err(|err| {
                citadel_logging::error!(target: "citadel", "[BACKEND-NETWORK] Failed to send bypass message: {}", err);
                BackendError::StorageError(format!("Failed to send bypass message: {err}"))
            })?;
            citadel_logging::info!(target: "citadel", "[BACKEND-NETWORK] Successfully sent to bypass channel");
        } else {
            citadel_logging::error!(target: "citadel", "[BACKEND-NETWORK] bypass_ism_outbound_tx is None!");
            return Err(BackendError::StorageError(
                "Failed to send bypass message: bypass_ism_outbound_tx is None".to_string(),
            ));
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
                                "Failed to deserialize {prefix} map: {err}"
                            ))
                        })?;
                    Ok(state)
                }
                InternalServiceResponse::LocalDBGetKVFailure(failure_response) => {
                    let failure_message = failure_response.message;
                    if failure_message == KEY_NOT_FOUND {
                        citadel_logging::debug!(target: "citadel", "[GET_MAP] {} map not found, initializing new one", prefix);
                        self.initialize_map(prefix).await
                    } else {
                        Err(BackendError::StorageError(format!(
                            "Failed to get {prefix} map: {failure_message}"
                        )))
                    }
                }
                _ => Err(BackendError::StorageError(format!(
                    "Unexpected response when getting {prefix} map"
                ))),
            }
        } else {
            // A timeout is NOT "the map is empty".
            //
            // This used to return an empty map, and every caller here is a
            // read-modify-write over the WHOLE queue: get the map, change one
            // entry, write it back. So one slow LocalDB read during a send
            // replaced the entire pending queue with a map containing only the
            // new message — silently erasing every other queued message, each of
            // whose senders had already been shown "sent". Genuine absence is a
            // different answer ("Key not found", handled above) and still
            // initializes.
            Err(BackendError::StorageError(format!(
                "Timed out reading the {prefix} map; refusing to treat that as an empty queue"
            )))
        }
    }

    /// Generic function to initialize a map (inbound or outbound)
    async fn initialize_map(&self, prefix: &str) -> Result<State, BackendError<WrappedMessage>> {
        let request_id = Uuid::new_v4();
        let key = format!("{}-{}", prefix, self.cid);
        let new_state = State::new();

        let value = bincode2::serialize(&new_state).map_err(|err| {
            BackendError::StorageError(format!("Failed to serialize {prefix} map: {err}"))
        })?;

        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        };

        self.send_to_network(request).await?;

        // Was correct on its own terms and worded differently from the other two;
        // now literally the same decision, so they cannot drift apart again.
        write_outcome(
            self.wait_for_response(request_id).await,
            &format!("the initial {prefix} map"),
        )?;
        citadel_logging::debug!(target: "citadel", "[INITIALIZE_MAP] Initialized {} map successfully", prefix);
        Ok(new_state)
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
            BackendError::StorageError(format!("Failed to serialize {prefix} map: {err}"))
        })?;

        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid: self.cid,
            peer_cid: None,
            key,
            value,
        };

        self.send_to_network(request).await?;

        write_outcome(
            self.wait_for_response(request_id).await,
            &format!("the {prefix} map"),
        )
        .inspect(|_| {
            citadel_logging::debug!(target: "citadel", "[UPDATE_MAP] Updated {} map successfully", prefix);
        })
    }

    // Convenience methods that use the generic functions
    async fn get_inbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        self.get_map(INBOUND_MESSAGE_PREFIX).await
    }

    async fn get_outbound_map(&self) -> Result<State, BackendError<WrappedMessage>> {
        self.get_map(OUTBOUND_MESSAGE_PREFIX).await
    }

    // There is deliberately no `update_inbound_map` / `update_outbound_map`
    // convenience pair any more. They existed only to be called right after
    // `get_*_map`, and that read-then-write with nothing between them holding
    // the two halves together IS the lost-update bug. `backend_map::mutate` is
    // now the only way to write either map, so a future caller cannot
    // reconstruct the unsynchronised sequence without noticing.

    pub fn add_expected_request(&self, request_id: Uuid) {
        let (tx, _rx) = citadel_io::tokio::sync::oneshot::channel();
        self.expected_requests.insert(request_id, tx);
    }

    /// Sends multiple requests in a single batch and waits for all responses.
    /// This is more efficient than sequential requests as it:
    /// 1. Uses a single network roundtrip
    /// 2. Backend executes all requests in parallel
    /// 3. Avoids sequential await blocking in WASM
    ///
    /// Returns responses in the same order as the input requests.
    pub async fn send_batched(
        &self,
        requests: Vec<InternalServiceRequest>,
    ) -> Result<Vec<InternalServiceResponse>, BackendError<WrappedMessage>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let batch_request_id = Uuid::new_v4();
        citadel_logging::info!(target: "citadel", "[SEND_BATCHED] Sending {} requests in batch, request_id={}", requests.len(), batch_request_id);

        let batched_request = InternalServiceRequest::Batched {
            request_id: batch_request_id,
            commands: requests,
        };

        self.send_to_network(batched_request).await?;

        if let Some(response) = self.wait_for_response(batch_request_id).await {
            match response {
                InternalServiceResponse::BatchedResponse(BatchedResponseData {
                    results, ..
                }) => Ok(results),
                other => {
                    citadel_logging::warn!(target: "citadel", "[SEND_BATCHED] Unexpected response type: {:?}", other);
                    Err(BackendError::StorageError(
                        "Unexpected response type for batched request".to_string(),
                    ))
                }
            }
        } else {
            citadel_logging::warn!(target: "citadel", "[SEND_BATCHED] Timeout waiting for batched response");
            Err(BackendError::StorageError(
                "Timeout waiting for batched response".to_string(),
            ))
        }
    }

    /// Loads multiple values in a single batched request.
    /// More efficient than calling load_value() multiple times.
    pub async fn load_values_batched(
        &self,
        keys: &[&str],
    ) -> Result<Vec<Option<Vec<u8>>>, BackendError<WrappedMessage>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Build batch of LocalDBGetKV requests
        let requests: Vec<InternalServiceRequest> = keys
            .iter()
            .map(|key| InternalServiceRequest::LocalDBGetKV {
                request_id: Uuid::new_v4(),
                cid: self.cid,
                peer_cid: None,
                key: format!("{}-{}", key, self.cid),
            })
            .collect();

        let responses = self.send_batched(requests).await?;

        let mut results: Vec<Option<Vec<u8>>> = Vec::with_capacity(responses.len());
        for (index, resp) in responses.into_iter().enumerate() {
            let key = keys.get(index).copied().unwrap_or("<unknown>");
            results.push(read_outcome(resp, key)?);
        }

        Ok(results)
    }
}

/// The two I/O halves `backend_map::mutate` drives. Thin wrappers over the
/// existing generic map functions, named separately so the serialisation can be
/// tested against a fake instead of a running agent.
#[async_trait]
impl MapStore for CitadelWorkspaceBackend {
    async fn read_map(&self, prefix: &str) -> Result<State, BackendError<WrappedMessage>> {
        self.get_map(prefix).await
    }

    async fn write_map(
        &self,
        prefix: &str,
        request_id: Uuid,
        state: State,
    ) -> Result<(), BackendError<WrappedMessage>> {
        self.update_map(prefix, request_id, state).await
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

        mutate(
            self,
            &self.outbound_gate,
            OUTBOUND_MESSAGE_PREFIX,
            request_id,
            move |outbound| {
                outbound
                    .entry(peer_cid)
                    .or_insert_with(HashMap::new)
                    .insert(message_id, message);
            },
        )
        .await
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

        mutate(
            self,
            &self.inbound_gate,
            INBOUND_MESSAGE_PREFIX,
            request_id,
            move |inbound| {
                inbound
                    .entry(peer_cid)
                    .or_insert_with(HashMap::new)
                    .insert(message_id, message);
            },
        )
        .await
    }

    async fn clear_message_inbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        mutate(
            self,
            &self.inbound_gate,
            INBOUND_MESSAGE_PREFIX,
            Uuid::new_v4(),
            move |inbound| {
                if let Some(peer_messages) = inbound.get_mut(&peer_id) {
                    peer_messages.remove(&message_id);
                }
            },
        )
        .await
    }

    async fn clear_message_outbound(
        &self,
        peer_id: u64,
        message_id: u64,
    ) -> Result<(), BackendError<WrappedMessage>> {
        mutate(
            self,
            &self.outbound_gate,
            OUTBOUND_MESSAGE_PREFIX,
            Uuid::new_v4(),
            move |outbound| {
                if let Some(peer_messages) = outbound.get_mut(&peer_id) {
                    peer_messages.remove(&message_id);
                }
            },
        )
        .await
    }

    /// One read-modify-write for the whole set.
    ///
    /// Acknowledgement is cumulative, so a single ACK routinely retires a whole
    /// send window. Clearing them one at a time meant a full queue read AND a
    /// full queue write per covered id: O(window) round trips to the agent and
    /// O(window^2) bytes serialised, for one ACK.
    async fn clear_messages_outbound(
        &self,
        peer_id: u64,
        message_ids: &[u64],
    ) -> Result<(), BackendError<WrappedMessage>> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let message_ids = message_ids.to_vec();
        mutate(
            self,
            &self.outbound_gate,
            OUTBOUND_MESSAGE_PREFIX,
            Uuid::new_v4(),
            move |outbound| {
                if let Some(peer_messages) = outbound.get_mut(&peer_id) {
                    for message_id in &message_ids {
                        peer_messages.remove(message_id);
                    }
                }
            },
        )
        .await
    }

    async fn get_pending_outbound(
        &self,
    ) -> Result<Vec<WrappedMessage>, BackendError<WrappedMessage>> {
        loop {
            match self.get_outbound_map().await {
                Ok(outbound) => {
                    return Ok(outbound
                        .values()
                        .flat_map(|messages| messages.values().cloned())
                        .collect())
                }
                Err(e) => {
                    // If we get a delivery error, log it and return an empty vector
                    let err_str = format!("{e:?}");
                    if err_str.contains("Failed to deliver message")
                        || err_str.contains("get_kv: Server connection not found")
                    {
                        citadel_logging::warn!(target: "citadel", "[GET_PENDING_OUTBOUND] Failed to get outbound map due to likely no connection up yet");
                        sleep_internal(Duration::from_millis(5000)).await;
                        continue;
                    } else {
                        return Err(e);
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
                Ok(inbound) => {
                    return Ok(inbound
                        .values()
                        .flat_map(|messages| messages.values().cloned())
                        .collect())
                }
                Err(e) => {
                    // If we get a delivery error, log it and return an empty vector
                    let err_str = format!("{e:?}");
                    if err_str.contains("Failed to deliver message")
                        || err_str.contains("get_kv: Server connection not found")
                    {
                        citadel_logging::warn!(target: "citadel", "[GET_PENDING_INBOUND] Failed to get inbound map likely due to likely no connection up yet");
                        sleep_internal(Duration::from_millis(5000)).await;
                        continue;
                    } else {
                        return Err(e);
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

        write_outcome(
            self.wait_for_response(request_id).await,
            &format!("the value for key={key}"),
        )
        .inspect(|_| {
            citadel_logging::debug!(target: "citadel", "[STORE_VALUE] Stored value for key={}", key);
        })
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

        // The singular twin of `load_values_batched`, and it had the same defect
        // twice: `_ => Ok(None)` turned a backend error into an absent key, and a
        // TIMEOUT returned `Ok(None)` under a comment that said so out loud --
        // "assume the key doesn't exist". Neither was flagged; both were found by
        // grepping the mechanism after fixing the batched path.
        //
        // This is how the delivery frontier and the next-id counter are read. A
        // read that failed, reported as "nothing stored", restarts the counter and
        // re-delivers messages the peer has already seen.
        match self.wait_for_response(request_id).await {
            Some(response) => {
                let value = read_outcome(response, key)?;
                citadel_logging::debug!(target: "citadel", "[LOAD_VALUE] Loaded value for key={}", key);
                Ok(value)
            }
            None => Err(BackendError::StorageError(format!(
                "Timed out reading key={key}; whether it exists is unknown"
            ))),
        }
    }

    async fn load_values_batched(
        &self,
        keys: &[&str],
    ) -> Result<Vec<Option<Vec<u8>>>, BackendError<WrappedMessage>> {
        // Delegate to the inherent method that uses batched network requests
        CitadelWorkspaceBackend::load_values_batched(self, keys).await
    }

    /// One round trip for the whole set, mirroring `load_values_batched`.
    ///
    /// The inbound path writes the receipt map and the per-peer high-water mark
    /// for every arriving message, inline in the single sequential listener.
    /// Two separate `store_value` calls meant two round trips to the agent per
    /// message, each with its own five-second `wait_for_response` window in
    /// which one lost response freezes ALL inbound processing -- ACKs included,
    /// so the senders start retransmitting into a receiver that is not reading.
    async fn store_values_batched(
        &self,
        entries: &[(&str, Vec<u8>)],
    ) -> Result<(), BackendError<WrappedMessage>> {
        if entries.is_empty() {
            return Ok(());
        }

        let requests: Vec<InternalServiceRequest> = entries
            .iter()
            .map(|(key, value)| InternalServiceRequest::LocalDBSetKV {
                request_id: Uuid::new_v4(),
                cid: self.cid,
                peer_cid: None,
                key: format!("{}-{}", key, self.cid),
                value: value.clone(),
            })
            .collect();

        let responses = self.send_batched(requests).await?;

        // A missing or failed acknowledgement is a failure, not a silence to
        // step over: `update_map` and `store_value` both refuse to report an
        // unacknowledged write as success, and this must agree with them.
        for (index, response) in responses.iter().enumerate() {
            if !matches!(response, InternalServiceResponse::LocalDBSetKVSuccess(_)) {
                let key = entries[index].0;
                return Err(BackendError::StorageError(format!(
                    "Batched store for key={key} was not acknowledged: {response:?}"
                )));
            }
        }
        if responses.len() != entries.len() {
            return Err(BackendError::StorageError(format!(
                "Batched store expected {} acknowledgements, got {}",
                entries.len(),
                responses.len()
            )));
        }
        Ok(())
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
            outbound_gate: Arc::new(Mutex::new(())),
            inbound_gate: Arc::new(Mutex::new(())),
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

#[cfg(test)]
mod response_classification {
    //! What counts as a stored write, and what counts as an absent key.
    //!
    //! These two questions were answered four different ways across five call
    //! sites in this file, and two of the answers were wrong in the direction
    //! that loses data silently. Testing the decisions rather than the sockets
    //! is why there is nothing mocked here: the functions are pure, so the whole
    //! space of responses can be walked with real values.
    use super::*;
    use citadel_internal_service_types::{
        LocalDBGetKVFailure, LocalDBGetKVSuccess, LocalDBSetKVFailure, LocalDBSetKVSuccess,
    };

    fn set_ok() -> InternalServiceResponse {
        InternalServiceResponse::LocalDBSetKVSuccess(LocalDBSetKVSuccess {
            cid: 1,
            peer_cid: None,
            key: "k".into(),
            request_id: None,
        })
    }

    fn set_failed(message: &str) -> InternalServiceResponse {
        InternalServiceResponse::LocalDBSetKVFailure(LocalDBSetKVFailure {
            cid: 1,
            peer_cid: None,
            message: message.into(),
            request_id: None,
        })
    }

    fn get_ok(value: &[u8]) -> InternalServiceResponse {
        InternalServiceResponse::LocalDBGetKVSuccess(LocalDBGetKVSuccess {
            cid: 1,
            peer_cid: None,
            key: "k".into(),
            value: value.to_vec(),
            request_id: None,
        })
    }

    fn get_failed(message: &str) -> InternalServiceResponse {
        InternalServiceResponse::LocalDBGetKVFailure(LocalDBGetKVFailure {
            cid: 1,
            peer_cid: None,
            message: message.into(),
            request_id: None,
        })
    }

    #[test]
    fn an_acknowledged_write_is_a_write() {
        assert!(write_outcome(Some(set_ok()), "the outbound map").is_ok());
    }

    #[test]
    fn a_refused_write_is_not_a_write() {
        // The whole finding. `.is_some()` said yes to every one of these, so the
        // sender saw a message as sent that nothing would ever retransmit.
        for message in [
            "Backend error",
            "propose_target failed",
            "This request is not permitted for this session",
        ] {
            let outcome = write_outcome(Some(set_failed(message)), "the outbound map");
            assert!(
                outcome.is_err(),
                "a LocalDBSetKVFailure({message:?}) must not report a stored write"
            );
        }
    }

    #[test]
    fn a_write_answered_by_the_wrong_variant_is_not_a_write() {
        // A response addressed to this request that is not a set-KV answer at all
        // is a protocol confusion, not a success.
        assert!(write_outcome(Some(get_ok(b"x")), "the outbound map").is_err());
    }

    #[test]
    fn an_unanswered_write_is_not_a_write() {
        assert!(write_outcome(None, "the outbound map").is_err());
    }

    #[test]
    fn a_stored_value_reads_back() {
        assert_eq!(
            read_outcome(get_ok(b"hello"), "k").unwrap(),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn a_missing_key_is_absent_not_an_error() {
        // The one case that legitimately maps to None -- and it is keyed to the
        // constant the agent writes, not to a string retyped here.
        assert_eq!(read_outcome(get_failed(KEY_NOT_FOUND), "k").unwrap(), None);
    }

    #[test]
    fn a_failed_read_is_not_an_absent_key() {
        // `_ => None` made these indistinguishable from the case above, which is
        // how a backend error became an empty delivery frontier.
        let outcome = read_outcome(get_failed("Backend error: disk failure"), "k");
        assert!(
            outcome.is_err(),
            "a failed read must not read as an absent key"
        );
    }

    #[test]
    fn the_agent_and_this_module_agree_on_what_missing_means() {
        // The two sides of KEY_NOT_FOUND live in different crates and are compared
        // with `==`. If the agent reworded its message, every genuine miss would
        // become a hard error; this asserts the exact value both sides share.
        assert_eq!(KEY_NOT_FOUND, "Key not found");
    }
}
