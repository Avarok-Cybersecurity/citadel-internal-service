use crate::connector::{InternalServiceConnector, WrappedSink, WrappedStream};
use crate::io_interface::IOInterface;
use async_trait::async_trait;
use citadel_internal_service_types::{
    InternalServicePayload, InternalServiceRequest, InternalServiceResponse,
};
use citadel_logging::tracing::log;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use intersession_layer_messaging::{
    Backend, MessageMetadata, MessageSystem, NetworkError, Payload, UnderlyingSessionTransport,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

/// A multiplexer for the InternalServiceConnector that allows for multiple handles
pub struct CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Send + Sync + 'static,
{
    // Local subscriptions where the key is the CID
    txs_to_inbound: Arc<DashMap<StreamKey, UnboundedSender<InternalMessage>>>,
    tx_to_outbound: UnboundedSender<(StreamKey, InternalMessage)>,
    /// Periodically refreshed by the Messenger
    connected_peers: Arc<RwLock<HashSet<u64>>>,
    is_running: Arc<AtomicBool>,
    ism: Arc<parking_lot::Mutex<Option<CitadelWorkspaceISM<B>>>>,
}

impl<B> Clone for CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            txs_to_inbound: self.txs_to_inbound.clone(),
            tx_to_outbound: self.tx_to_outbound.clone(),
            connected_peers: self.connected_peers.clone(),
            is_running: self.is_running.clone(),
            ism: self.ism.clone(),
        }
    }
}

pub type InternalMessage = Payload<WrappedMessage>;
const POLL_CONNECTED_PEERS_REFRESH_PERIOD: Duration = Duration::from_millis(1000);

#[derive(Copy, Clone, Debug)]
pub struct StreamKey {
    pub cid: u64,
}

pub struct CitadelWorkspaceBackend {}

type CitadelWorkspaceISM<B> = MessageSystem<
    WrappedMessage,
    B,
    UnboundedSender<WrappedMessage>,
    CitadelWorkspaceMessengerHandle<B>,
>;

impl<B> CitadelWorkspaceMessenger<B>
where
    B: Backend<WrappedMessage> + Send + Sync + 'static,
{
    pub async fn new<T: IOInterface>(
        connector: InternalServiceConnector<T>,
        backend: B,
    ) -> Result<Self, NetworkError> {
        let (primary_tx, primary_rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx_to_outbound, rx_to_outbound) = tokio::sync::mpsc::unbounded_channel();

        let this = Self {
            tx_to_outbound: tx_to_outbound.clone(),
            txs_to_inbound: Arc::new(Default::default()),
            connected_peers: Arc::new(Default::default()),
            is_running: Arc::new(AtomicBool::new(true)),
            ism: Arc::new(parking_lot::Mutex::new(None)),
        };

        let primary_multiplexed_key = StreamKey { cid: 0 };
        let primary_multiplexed_handle = this.multiplex(primary_multiplexed_key);
        // Create the ISM for the primary handle. The ISM will handle mapping messages the local user sends to us, then
        // sending outbound to the background task via the handle. The ISM will also be responsible for receiving messages from the handle.
        // The ISM will automatically handle this for us in the background. The ISM does not interact with the actual networking layer directly;
        // it instead works over the multiplexed layer that uses handles
        let ism = MessageSystem::new(backend, primary_tx, primary_multiplexed_handle)
            .await
            .map_err(NetworkError::BackendError)?;

        let _ = this.ism.lock().replace(ism);

        this.spawn_background_tasks(connector, primary_rx, rx_to_outbound);

        Ok(this)
    }

    fn spawn_background_tasks<T: IOInterface>(
        &self,
        connector: InternalServiceConnector<T>,
        mut primary_rx: UnboundedReceiver<WrappedMessage>,
        mut rx_to_outbound: UnboundedReceiver<(StreamKey, InternalMessage)>,
    ) {
        let InternalServiceConnector::<T> {
            mut sink,
            mut stream,
        } = connector;

        #[derive(Serialize, Deserialize)]
        struct WireMessage {
            original_contents: Vec<u8>,
            message_type: InternalMessage,
        }

        // Must be responsible for mapping request ID's to StreamKeys for the response phase
        let this = self.clone();
        let network_inbound_task = async move {
            while let Some(network_message) = stream.next().await {
                if !this.is_running() {
                    return;
                }

                // TODO: Take this message, send it to the ISM layer by doing dashmap lookup.
            }
        };

        let this = self.clone();
        let ism_inbound_task = async move {
            while let Some(message) = primary_rx.recv().await {
                if !this.is_running() {
                    return;
                }

                // TODO: Take the message the ISM delivered locally, send it to the local us
            }
        };

        let this = self.clone();
        let local_handle_to_ism_outbound_task = async move {
            while let Some((stream_key, message)) = rx_to_outbound.recv().await {
                if !this.is_running() {
                    return;
                }

                match message {
                    Payload::Message(message) => {
                        // TODO: for here and network_inbound_task, replace the message contents field in the
                        // request, if applicable. Otherwise send/forward as usual.
                        // TODO: just look for the Payload::Message(InternalServicePayload::Request(request)) type, and update
                        // the contents. Otherwise just
                        let request = match message.contents {
                            Payload::Message(InternalServicePayload::Request(request)) => request,
                            Payload::Poll { from_id, to_id } => {
                                // TODO: Map to a simple message type. Ensure dest peer is connected first
                            }
                        };

                        if let Err(err) = sink.send(request).await {
                            log::error!(target: "citadel", "Error while sending ISM message to outbound network: {err:?}")
                        }
                    }
                }
            }
        };

        let task = async move {
            tokio::select! {
                _ = network_inbound_task => {
                    log::warn!(target: "citadel", "Network inbound task ended")
                },

                _ = ism_inbound_task => {
                    log::warn!(target: "citadel", "ISM inbound task ended")
                }

                _ = local_handle_to_ism_outbound_task => {
                    log::warn!(target: "citadel", "Local handle to ISM outbound task ended")
                }
            }
        };

        drop(tokio::task::spawn(task));
    }

    pub fn multiplex(&self, key: StreamKey) -> CitadelWorkspaceMessengerHandle<B> {
        assert_ne!(key.cid, 0);
        let (tx_from_inbound_network, rx_from_inbound_network) =
            tokio::sync::mpsc::unbounded_channel();
        let tx_to_background = self.tx_to_outbound.clone();
        self.txs_to_inbound.insert(key, tx_from_inbound_network);
        CitadelWorkspaceMessengerHandle {
            stream: Mutex::new(rx_from_inbound_network),
            sink: tx_to_background,
            messenger_ptr: self.clone(),
            key,
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

pub struct CitadelWorkspaceMessengerHandle<B>
where
    B: Backend<WrappedMessage> + Send + Sync + 'static,
{
    stream: Mutex<tokio::sync::mpsc::UnboundedReceiver<InternalMessage>>,
    sink: tokio::sync::mpsc::UnboundedSender<(StreamKey, InternalMessage)>,
    messenger_ptr: CitadelWorkspaceMessenger<B>,
    key: StreamKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WrappedMessage {
    source_id: u64,
    destination_id: u64,
    message_id: u64,
    contents: Payload<InternalServicePayload>,
}

/// When a message is sent for purposes of polling the internal service
const NO_DESTINATION: u64 = 0;

impl MessageMetadata for WrappedMessage {
    type PeerId = u64;
    type MessageId = u64;
    type Contents = Payload<InternalServicePayload>;

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

#[async_trait]
impl<B> UnderlyingSessionTransport for CitadelWorkspaceMessengerHandle<B>
where
    B: Backend<WrappedMessage> + Send + Sync + 'static,
{
    type Message = WrappedMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        let message = self
            .stream
            .try_lock()
            .expect("There should be only one caller polling for messages")
            .recv()
            .await?;

        Some(message)
    }

    async fn send_message(&self, message: Payload<Self::Message>) -> Result<(), NetworkError> {
        // This sends to background task
        self.sink
            .send((self.key, message))
            .map_err(|err| NetworkError::SendFailed(err.to_string()))
    }

    async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId> {
        // let connected_peers_request = InternalServiceRequest::GetSessions { request_id: Uuid::new_v4() };
        self.messenger_ptr.connected_peers.read().iter().collect()
    }

    fn peer_id(&self) -> <Self::Message as MessageMetadata>::PeerId {
        self.key.cid
    }
}
