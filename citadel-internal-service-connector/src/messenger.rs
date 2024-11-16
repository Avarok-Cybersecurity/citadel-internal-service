use crate::connector::InternalServiceConnector;
use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse, SecurityLevel,
    SessionInformation,
};
use citadel_logging::tracing::log;
use dashmap::DashMap;
use futures::future::Either;
use futures::{SinkExt, StreamExt};
use intersession_layer_messaging::{
    Backend, DeliveryError, MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport, ILM,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;

/// A multiplexer for the InternalServiceConnector that allows for multiple handles
pub struct CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    // Local subscriptions where the key is the CID
    txs_to_inbound: Arc<DashMap<StreamKey, UnboundedSender<InternalMessage>>>,
    bypass_ism_tx_to_outbound: UnboundedSender<(StreamKey, InternalMessage)>,
    /// Periodically refreshed by the Messenger
    connected_peers: Arc<RwLock<HashMap<u64, SessionInformation>>>,
    // Contains a list of requests that were invoked by the background task and not to be delivered
    // to ISM or the end user
    background_invoked_requests: Arc<parking_lot::Mutex<HashSet<Uuid>>>,
    is_running: Arc<AtomicBool>,
    backend: B,
    final_tx: UnboundedSender<InternalServiceResponse>,
}

impl<B> Clone for CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            txs_to_inbound: self.txs_to_inbound.clone(),
            bypass_ism_tx_to_outbound: self.bypass_ism_tx_to_outbound.clone(),
            connected_peers: self.connected_peers.clone(),
            background_invoked_requests: self.background_invoked_requests.clone(),
            is_running: self.is_running.clone(),
            final_tx: self.final_tx.clone(),
            backend: self.backend.clone(),
        }
    }
}

pub type InternalMessage = Payload<WrappedMessage>;
const POLL_CONNECTED_PEERS_REFRESH_PERIOD: Duration = Duration::from_millis(1000);
const ISM_STREAM_ID: u8 = 0;
const BYPASS_ISM_STREAM_ID: u8 = 1;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamKey {
    pub cid: u64,
    pub stream_id: u8,
}

pub struct CitadelWorkspaceBackend {}

type CitadelWorkspaceISM<B> = ILM<WrappedMessage, B, LocalDeliveryTx, ISMHandle<B>>;

pub struct LocalDeliveryTx {
    final_tx: UnboundedSender<InternalServiceResponse>,
}

#[async_trait]
impl intersession_layer_messaging::local_delivery::LocalDelivery<WrappedMessage>
    for LocalDeliveryTx
{
    async fn deliver(&self, message: WrappedMessage) -> Result<(), DeliveryError> {
        let InternalServicePayload::Response(response) = message.contents else {
            return Err(DeliveryError::BadInput);
        };

        self.final_tx
            .send(response)
            .map_err(|_| DeliveryError::ChannelClosed)
    }
}

impl<B> CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    pub async fn new<T: IOInterface>(
        connector: InternalServiceConnector<T>,
        backend: B,
    ) -> Result<(Self, UnboundedReceiver<InternalServiceResponse>), MessengerError> {
        let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel();
        // background layer
        let (bypass_ism_tx_to_outbound, rx_to_outbound) = tokio::sync::mpsc::unbounded_channel();
        let this = Self {
            backend,
            txs_to_inbound: Arc::new(Default::default()),
            connected_peers: Arc::new(Default::default()),
            is_running: Arc::new(AtomicBool::new(true)),
            background_invoked_requests: Arc::new(parking_lot::Mutex::new(Default::default())),
            bypass_ism_tx_to_outbound,
            final_tx,
        };

        this.spawn_background_tasks(connector, rx_to_outbound);

        Ok((this, final_rx))
    }

    pub async fn multiplex(&self, cid: u64) -> Result<MessengerTx<B>, MessengerError> {
        let stream_key = StreamKey {
            cid,
            stream_id: ISM_STREAM_ID,
        };
        let (ism_handle, background_handle) = create_ipc_handles(self.clone(), stream_key);
        let local_delivery_wrapper = LocalDeliveryTx {
            final_tx: self.final_tx.clone(),
        };
        let backend = self.backend.clone();
        let ism = ILM::new(backend, local_delivery_wrapper, ism_handle)
            .await
            .map_err(|err| MessengerError::OtherError {
                reason: format!("{err:?}"),
            })?;

        let BackgroundHandle {
            mut background_from_ism_outbound,
            background_to_ism_inbound,
        } = background_handle;
        self.txs_to_inbound
            .insert(stream_key, background_to_ism_inbound);

        let handle = MessengerTx {
            bypass_ism_outbound_tx: self.bypass_ism_tx_to_outbound.clone(),
            messenger: self.clone(),
            stream_key,
            ism,
        };

        // We only have to spawn a single task: the one that listens for messages from the ISM and forwards them to the background task
        let this = self.clone();
        let task = async move {
            while let Some(message) = background_from_ism_outbound.recv().await {
                if !this.is_running() {
                    return;
                }

                if let Err(err) = this.bypass_ism_tx_to_outbound.send((stream_key, message)) {
                    log::error!(target: "citadel", "Error while sending message outbound: {err:?}");
                    break;
                }
            }
        };

        drop(tokio::task::spawn(task));

        Ok(handle)
    }

    fn spawn_background_tasks<T: IOInterface>(
        &self,
        connector: InternalServiceConnector<T>,
        mut rx_to_outbound: UnboundedReceiver<(StreamKey, InternalMessage)>,
    ) {
        let InternalServiceConnector::<T> {
            mut sink,
            mut stream,
        } = connector;

        let this = self.clone();
        let tx_to_local_user_clone = self.final_tx.clone();
        let network_inbound_task = async move {
            while let Some(network_message) = stream.next().await {
                if !this.is_running() {
                    return;
                }

                match network_message {
                    // TODO: Add support for group messaging
                    InternalServiceResponse::MessageNotification(message) => {
                        // deserialize and relay to ISM
                        match bincode2::deserialize(&message.message) {
                            Ok(ism_message) => {
                                // Assume this is an ISM message
                                let stream_key = StreamKey {
                                    cid: message.cid,
                                    stream_id: ISM_STREAM_ID,
                                };
                                if let Some(tx) = this.txs_to_inbound.get(&stream_key) {
                                    if let Err(err) = tx.send(ism_message) {
                                        log::error!(target: "citadel", "Error while sending message to ISM: {err:?}");
                                    }
                                } else {
                                    // TODO: enqueue for later use
                                    log::warn!(target: "citadel", "Received message for unknown stream key: {stream_key:?}");
                                }
                            }
                            Err(err) => {
                                log::error!(target: "citadel", "Error while deserializing ISM message: {err:?}");
                            }
                        }
                    }
                    non_ism_message => {
                        if let InternalServiceResponse::GetSessionsResponse(sessions) =
                            &non_ism_message
                        {
                            let mut lock = this.connected_peers.write();
                            this.connected_peers.write().clear();

                            for session in &sessions.sessions {
                                let cid = session.cid;
                                let value = session.clone();
                                lock.insert(cid, value);
                            }
                        }

                        if let Some(request_id) = non_ism_message.request_id() {
                            if this.background_invoked_requests.lock().remove(request_id) {
                                // This was a request invoked by the background task. Do not deliver to ISM
                                continue;
                            }
                        }

                        // Send to default direct handle
                        if let Err(err) = tx_to_local_user_clone.send(non_ism_message) {
                            log::error!(target: "citadel", "Error while sending message to local user: {err:?}");
                            return;
                        }
                    }
                }
            }
        };

        let this = self.clone();
        // Takes messages sent through ISM and funnels them here
        let ism_to_background_task_outbound = async move {
            while let Some((stream_key, message_internal)) = rx_to_outbound.recv().await {
                if !this.is_running() {
                    return;
                }

                // We get one of two types of messages here:
                // 1) Handle -> ISM -> Here, in which case, it is for messaging between nodes, or;
                // 2) Handle -> Here, in which case, it is a request for the internal service
                match message_internal {
                    Payload::Message(message) if stream_key.stream_id == BYPASS_ISM_STREAM_ID => {
                        // This is a message for the internal service
                        let InternalServicePayload::Request(request) = message.contents else {
                            log::warn!(target: "citadel", "Received a message with no destination that was not a request: {message:?}");
                            continue;
                        };

                        if let Err(err) = sink.send(request).await {
                            log::error!(target: "citadel", "Error while sending ISM message to outbound network: {err:?}")
                        }
                    }

                    ism_proto if stream_key.stream_id == ISM_STREAM_ID => {
                        // This is a message for another node. We must construct a message request
                        // For inbound logic, we assume all message types use the ISM proto, otherwise, fail
                        let serialized_message = bincode2::serialize(&ism_proto)
                            .expect("Should be able to serialize message");
                        // TODO: Add support for group messaging
                        let message_request = InternalServiceRequest::Message {
                            request_id: Uuid::new_v4(),
                            message: serialized_message,
                            cid: stream_key.cid,
                            peer_cid: None,
                            security_level: Default::default(),
                        };

                        if let Err(err) = sink.send(message_request).await {
                            log::error!(target: "citadel", "Error while sending ISM message to outbound network: {err:?}")
                        }
                    }

                    other => {
                        log::warn!(target: "citadel", "Received a message for an invalid stream {stream_key:?}: {other:?}");
                    }
                }
            }
        };

        let this = self.clone();
        let periodic_session_status_poller = async move {
            let bypass_key = StreamKey {
                cid: LOOPBACK_ONLY,
                stream_id: BYPASS_ISM_STREAM_ID,
            };

            let mut ticker = tokio::time::interval(POLL_CONNECTED_PEERS_REFRESH_PERIOD);
            loop {
                if !this.is_running() {
                    return;
                }

                ticker.tick().await;
                let mut background_invoked_requests = this.background_invoked_requests.lock();
                // Update the state for each client that has a running ISM instance
                let local_cids: Vec<u64> =
                    this.txs_to_inbound.iter().map(|r| r.key().cid).collect();
                for source_cid in local_cids {
                    let request_id = Uuid::new_v4();
                    background_invoked_requests.insert(request_id);
                    // The inbound network task will automatically update the state. All this task has to do
                    // is send the request
                    let request = InternalMessage::Message(WrappedMessage {
                        source_id: source_cid,
                        destination_id: LOOPBACK_ONLY,
                        message_id: 0,
                        contents: InternalServicePayload::Request(
                            InternalServiceRequest::GetSessions { request_id },
                        ),
                    });
                    if let Err(err) = this.bypass_ism_tx_to_outbound.send((bypass_key, request)) {
                        log::error!(target: "citadel", "Error while sending session status poller request: {err:?}");
                        return;
                    }
                }
            }
        };

        let this = self.clone();
        let task = async move {
            tokio::select! {
                _ = network_inbound_task => {
                    log::warn!(target: "citadel", "Network inbound task ended. Messenger is shutting down")
                },

                _ = ism_to_background_task_outbound => {
                    log::warn!(target: "citadel", "Local handle to ISM outbound task ended. Messenger is shutting down")
                }

                _ = periodic_session_status_poller => {
                    log::warn!(target: "citadel", "Periodic session status poller ended. Messenger is shutting down")
                }
            }

            this.is_running.store(false, Ordering::SeqCst);
        };

        drop(tokio::task::spawn(task));
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

pub struct MessengerTx<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    bypass_ism_outbound_tx: UnboundedSender<(StreamKey, InternalMessage)>,
    messenger: CitadelWorkspaceMessenger<B>,
    stream_key: StreamKey,
    ism: CitadelWorkspaceISM<B>,
}

impl<B> Drop for MessengerTx<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Remove the handle from the list of active handles
        self.messenger.txs_to_inbound.remove(&self.stream_key);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WrappedMessage {
    source_id: u64,
    destination_id: u64,
    message_id: u64,
    contents: InternalServicePayload,
}

/// When a message is sent for purposes of polling the internal service
const LOOPBACK_ONLY: u64 = 0;

impl MessageMetadata for WrappedMessage {
    type PeerId = u64;
    type MessageId = u64;
    type Contents = InternalServicePayload;

    fn source_id(&self) -> Self::PeerId {
        self.source_id
    }

    fn destination_id(&self) -> Self::PeerId {
        self.destination_id
    }

    fn message_id(&self) -> Self::MessageId {
        self.message_id
    }

    fn contents(&self) -> &Self::Contents {
        &self.contents
    }

    fn construct_from_parts(
        source_id: Self::PeerId,
        destination_id: Self::PeerId,
        message_id: Self::MessageId,
        contents: impl Into<Self::Contents>,
    ) -> Self {
        Self {
            source_id,
            destination_id,
            message_id,
            contents: contents.into(),
        }
    }
}

#[derive(Debug)]
pub enum MessengerError {
    SendError {
        reason: String,
        message: Either<Vec<u8>, InternalServicePayload>,
    },
    OtherError {
        reason: String,
    },
    InitError {
        reason: String,
    },
}

impl Display for MessengerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for MessengerError {}

impl<B> MessengerTx<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    pub async fn send_message_to(
        &self,
        peer_cid: u64,
        message: impl Into<Vec<u8>>,
    ) -> Result<(), MessengerError> {
        self.send_message_to_with_security_level(peer_cid, Default::default(), message)
            .await
    }

    pub async fn send_message_to_with_security_level(
        &self,
        peer_cid: u64,
        security_level: SecurityLevel,
        message: impl Into<Vec<u8>>,
    ) -> Result<(), MessengerError> {
        let payload = InternalServicePayload::Request(InternalServiceRequest::Message {
            request_id: Uuid::new_v4(),
            message: message.into(),
            cid: self.stream_key.cid,
            peer_cid: Some(peer_cid),
            security_level,
        });

        // Send to ISM layer. ISM will then send to the background task using the sink in the UnderlyingNetworkTransport impl
        self.send_message_to_ism(peer_cid, payload).await
    }

    /// Sends an arbitrary request to the internal service. Not processed by the ISM layer.
    pub async fn send_request(
        &self,
        request: impl Into<InternalServicePayload>,
    ) -> Result<(), MessengerError> {
        let payload = Payload::Message(WrappedMessage {
            source_id: self.stream_key.cid,
            destination_id: LOOPBACK_ONLY,
            message_id: 0, // Does not matter since this will bypass ISM
            contents: request.into(),
        });

        self.bypass_ism_outbound_tx
            .send((self.stream_key, payload))
            .map_err(|err| {
                let reason = err.to_string();
                match err.0 .1 {
                    Payload::Message(message) => MessengerError::SendError {
                        reason,
                        message: Either::Right(message.contents),
                    },
                    _ => MessengerError::OtherError { reason },
                }
            })
    }

    async fn send_message_to_ism(
        &self,
        peer_cid: u64,
        request: impl Into<InternalServicePayload>,
    ) -> Result<(), MessengerError> {
        self.ism
            .send_to(peer_cid, request.into())
            .await
            .map_err(|err| match err {
                NetworkError::SendFailed { reason, message } => match message.contents {
                    InternalServicePayload::Request(InternalServiceRequest::Message {
                        message,
                        ..
                    }) => MessengerError::SendError {
                        reason,
                        message: Either::Left(message),
                    },
                    other => MessengerError::SendError {
                        reason,
                        message: Either::Right(other),
                    },
                },
                err => MessengerError::OtherError {
                    reason: format!("{err:?}"),
                },
            })
    }
}

/// This implements UnderlyingSessionTransport. It is responsible for sending messages from ISM
/// to the background outbound task. It is also responsible for receiving messages from the background task
/// and forwarding them to the ISM for processing.
struct ISMHandle<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    ism_to_background_outbound: UnboundedSender<InternalMessage>,
    ism_from_background_inbound: Mutex<UnboundedReceiver<InternalMessage>>,
    messenger_ptr: CitadelWorkspaceMessenger<B>,
    stream_key: StreamKey,
}

/// This is what the background will use to interact with the ISM
struct BackgroundHandle {
    background_from_ism_outbound: UnboundedReceiver<InternalMessage>,
    background_to_ism_inbound: UnboundedSender<InternalMessage>,
}

fn create_ipc_handles<B>(
    messenger_ptr: CitadelWorkspaceMessenger<B>,
    stream_key: StreamKey,
) -> (ISMHandle<B>, BackgroundHandle)
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    let (ism_to_background_outbound, background_from_ism_outbound) =
        tokio::sync::mpsc::unbounded_channel();
    let (background_to_ism_inbound, ism_from_background_inbound) =
        tokio::sync::mpsc::unbounded_channel();

    (
        ISMHandle {
            ism_to_background_outbound,
            ism_from_background_inbound: Mutex::new(ism_from_background_inbound),
            messenger_ptr,
            stream_key,
        },
        BackgroundHandle {
            background_from_ism_outbound,
            background_to_ism_inbound,
        },
    )
}

#[async_trait]
impl<B> UnderlyingSessionTransport for ISMHandle<B>
where
    B: Backend<WrappedMessage> + Clone + Send + Sync + 'static,
{
    type Message = WrappedMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.ism_from_background_inbound
            .try_lock()
            .expect("There should be only one caller polling for messages")
            .recv()
            .await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>> {
        self.ism_to_background_outbound
            .send(message)
            .map_err(|err| NetworkError::SendFailed {
                reason: err.to_string(),
                message: err.0,
            })
    }

    async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId> {
        let cid = self.local_id();
        if let Some(sess) = self.messenger_ptr.connected_peers.read().get(&cid) {
            sess.peer_connections.keys().copied().collect()
        } else {
            log::warn!(target: "citadel", "No session information found for {cid}");
            vec![]
        }
    }

    fn local_id(&self) -> <Self::Message as MessageMetadata>::PeerId {
        self.stream_key.cid
    }
}
