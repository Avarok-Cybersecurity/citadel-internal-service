use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Add;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};

const OUTBOUND_POLL: Duration = Duration::from_millis(100);
const INBOUND_POLL: Duration = Duration::from_millis(100);

#[async_trait]
pub trait MessageMetadata: Debug + Send + Sync + 'static {
    type PeerId: Default
        + Add<usize, Output = Self::MessageId>
        + Display
        + Hash
        + Eq
        + Copy
        + Ord
        + Send
        + Sync
        + 'static;
    type MessageId: Add<usize, Output = Self::MessageId>
        + Eq
        + PartialEq
        + Display
        + Hash
        + Ord
        + PartialOrd
        + Copy
        + Send
        + Sync
        + 'static;

    fn source_id(&self) -> Self::PeerId;
    fn destination_id(&self) -> Self::PeerId;
    fn message_id(&self) -> Self::MessageId;
    fn contents(&self) -> &[u8];
}

#[async_trait]
pub trait Network {
    type Message: MessageMetadata + Send + Sync + 'static;

    async fn next_message(&self) -> Option<Payload<Self::Message>>;
    async fn send_message(&self, message: Payload<Self::Message>) -> Result<(), NetworkError>;
    async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId>;
    fn peer_id(&self) -> <Self::Message as MessageMetadata>::PeerId;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Payload<M: MessageMetadata> {
    Ack {
        from_id: M::PeerId,
        to_id: M::PeerId,
        message_id: M::MessageId,
    },
    Message(M),
    Poll {
        from_id: M::PeerId,
        to_id: M::PeerId,
    },
}

#[derive(Debug)]
pub enum NetworkError {
    SendFailed(String),
    ConnectionError(String),
    BackendError(BackendError),
    ShutdownFailed(String),
    SystemShutdown,
}

#[derive(Debug)]
pub enum BackendError {
    StorageError(String),
    NotFound,
}

#[derive(Debug, Copy, Clone)]
pub enum DeliveryError {
    NoReceiver,
    ChannelClosed,
}

// Modified Backend trait to handle both outbound and inbound messages
#[async_trait]
pub trait Backend<M: MessageMetadata> {
    async fn store_outbound(&self, message: M) -> Result<(), BackendError>;
    async fn store_inbound(&self, message: M) -> Result<(), BackendError>;
    async fn clear_message_inbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError>;
    async fn clear_message_outbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError>;
    async fn get_pending_outbound(&self) -> Result<Vec<M>, BackendError>;
    async fn get_pending_inbound(&self) -> Result<Vec<M>, BackendError>;
}

#[async_trait]
pub trait LocalDelivery<M: MessageMetadata> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError>;
}

// Add this new struct to track last ACKed message IDs
pub struct MessageTracker<M: MessageMetadata> {
    last_acked: DashMap<M::PeerId, M::MessageId>,
    last_sent: DashMap<M::PeerId, M::MessageId>,
}

impl<M: MessageMetadata> MessageTracker<M> {
    fn new() -> Self {
        Self {
            last_acked: Default::default(),
            last_sent: Default::default(),
        }
    }

    fn update_ack(&self, peer_id: M::PeerId, msg_id: M::MessageId) {
        self.last_acked.insert(peer_id, msg_id);
    }

    fn can_send(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) -> bool {
        let last_acked = self.last_acked.get(peer_id);
        let last_sent = self.last_sent.get(peer_id);

        match (last_acked, last_sent) {
            (None, None) => true,
            (None, Some(_)) => false, // Wait for first ACK
            (Some(last_acked), Some(last_sent)) => *msg_id > *last_acked && *msg_id > *last_sent,
            (Some(last_acked), None) => *msg_id > *last_acked,
        }
    }

    fn mark_sent(&self, peer_id: M::PeerId, msg_id: M::MessageId) {
        self.last_sent.insert(peer_id, msg_id);
    }
}

pub struct MessageSystem<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: Network<Message = M> + Send + Sync + 'static,
{
    backend: Arc<B>,
    local_delivery: Arc<Mutex<Option<L>>>,
    network: Arc<N>,
    is_running: Arc<AtomicBool>,
    tracker: Arc<MessageTracker<M>>,
    poll_inbound_tx: tokio::sync::mpsc::UnboundedSender<()>,
    poll_outbound_tx: tokio::sync::mpsc::UnboundedSender<()>,
    known_peers: Arc<Mutex<Vec<M::PeerId>>>,
}

impl<M, B, L, N> Clone for MessageSystem<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: Network<Message = M> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            local_delivery: self.local_delivery.clone(),
            network: self.network.clone(),
            is_running: self.is_running.clone(),
            tracker: self.tracker.clone(),
            poll_inbound_tx: self.poll_inbound_tx.clone(),
            poll_outbound_tx: self.poll_outbound_tx.clone(),
            known_peers: self.known_peers.clone(),
        }
    }
}

impl<M, B, L, N> MessageSystem<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: Network<Message = M> + Send + Sync + 'static,
{
    pub fn new(backend: B, local_delivery: L, network: N) -> Self {
        let (poll_inbound_tx, poll_inbound_rx) = tokio::sync::mpsc::unbounded_channel();
        let (poll_outbound_tx, poll_outbound_rx) = tokio::sync::mpsc::unbounded_channel();

        let this = Self {
            backend: Arc::new(backend),
            local_delivery: Arc::new(Mutex::new(Some(local_delivery))),
            network: Arc::new(network),
            is_running: Arc::new(AtomicBool::new(true)),
            tracker: Arc::new(MessageTracker::new()),
            poll_inbound_tx,
            poll_outbound_tx,
            known_peers: Arc::new(Mutex::new(Vec::new())),
        };

        this.spawn_background_tasks(poll_inbound_rx, poll_outbound_rx);

        this
    }

    fn spawn_background_tasks(
        &self,
        mut poll_inbound_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        mut poll_outbound_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        // Spawn outbound processing task
        let self_clone = self.clone();
        let is_alive = self.is_running.clone();

        let outbound_handle = tokio::spawn(async move {
            loop {
                if !self_clone.can_run() {
                    break;
                }

                tokio::select! {
                    biased;
                    res0 = poll_outbound_rx.recv() => {
                        if res0.is_none() {
                            log::warn!(target: "citadel", "Poll outbound channel closed");
                        }
                    },
                    _res1 = sleep(OUTBOUND_POLL) => {},
                }

                self_clone.process_outbound().await;
            }
        });

        // Spawn inbound processing task
        let self_clone = self.clone();
        let inbound_handle = tokio::spawn(async move {
            loop {
                if !self_clone.can_run() {
                    break;
                }

                tokio::select! {
                    biased;
                    res0 = poll_inbound_rx.recv() => {
                        if res0.is_none() {
                            log::warn!(target: "citadel", "Poll inbound channel closed");
                        }
                    },
                    _res1 = sleep(INBOUND_POLL) => {},
                }

                self_clone.process_inbound().await;
            }
        });

        // Spawn network listener task
        let self_clone = self.clone();
        let network_io_handle = tokio::spawn(async move {
            loop {
                if !self_clone.can_run() {
                    break;
                }

                self_clone.process_next_network_message().await;
            }
        });

        // Spawn task that periodically polls for connected peers to help establish intersession recovery
        let self_clone = self.clone();
        let peer_polling_handle = tokio::spawn(async move {
            loop {
                if !self_clone.can_run() {
                    break;
                }

                let connected_peers_now = self_clone
                    .network
                    .connected_peers()
                    .await
                    .into_iter()
                    .sorted()
                    .collect::<Vec<_>>();
                let mut current_peers_lock = self_clone.known_peers.lock().await;
                let current_peers_previous = current_peers_lock
                    .iter()
                    .copied()
                    .sorted()
                    .collect::<Vec<_>>();
                if connected_peers_now != current_peers_previous {
                    log::info!(target: "citadel", "Connected peers changed, sending poll for refresh in state");

                    // Now, send a poll to each new connected peer
                    for peer_id in connected_peers_now
                        .iter()
                        .filter(|id| !current_peers_previous.contains(id))
                    {
                        if let Err(e) = self_clone
                            .network
                            .send_message(Payload::Poll {
                                from_id: self_clone.network.peer_id(),
                                to_id: *peer_id,
                            })
                            .await
                        {
                            log::error!(target: "citadel", "Failed to send poll to new peer: {:?}", e);
                        }
                    }

                    *current_peers_lock = connected_peers_now;
                }

                sleep(Duration::from_secs(5)).await;
            }
        });

        // Spawn a task that selects all three handles, and on any of them finishing, it will
        // set the atomic bool to false
        let self_clone = self.clone();
        drop(tokio::spawn(async move {
            tokio::select! {
                _ = outbound_handle => {
                    log::error!(target: "citadel", "Outbound processing task prematurely ended");
                },
                _ = inbound_handle => {
                    log::error!(target: "citadel", "Inbound processing task prematurely ended");
                },
                _ = network_io_handle => {
                    log::error!(target: "citadel", "Network IO task prematurely ended");
                },
                _ = peer_polling_handle => {
                    log::error!(target: "citadel", "Peer polling task prematurely ended");
                },
            }

            is_alive.store(false, std::sync::atomic::Ordering::Relaxed);
            drop(self_clone.local_delivery.lock().await.take());
        }));
    }

    async fn process_outbound(&self) {
        let pending_messages = match self.backend.get_pending_outbound().await {
            Ok(messages) => messages,
            Err(e) => {
                log::error!(target: "citadel", "Failed to get pending outbound messages: {:?}", e);
                return;
            }
        };

        // Group messages by PeerId
        let mut grouped_messages: HashMap<M::PeerId, Vec<M>> = HashMap::new();
        for msg in pending_messages {
            grouped_messages
                .entry(msg.destination_id())
                .or_default()
                .push(msg);
        }

        let connected_peers = &self.network.connected_peers().await;
        let self_clone = &self.clone();
        // Process each peer's messages concurrently
        futures::stream::iter(grouped_messages).for_each_concurrent(None, |(peer_id, mut messages)|  {
            async move {
                if !connected_peers.contains(&peer_id) {
                    log::warn!(target: "citadel", "Peer {peer_id} is not connected, skipping message until later");
                    return;
                }

                // Sort messages by MessageId
                messages.sort_by_key(|m| m.message_id());

                // Find the first message we can send based on ACKs
                'peer: for msg in messages {
                    let message_id = msg.message_id();
                    if self_clone.tracker.can_send(&peer_id, &message_id) {
                        log::trace!(target: "citadel", "[CAN SEND] message: {:?}", msg);
                        if let Err(e) = self_clone.network.send_message(Payload::Message(msg)).await {
                            log::error!(target: "citadel", "Failed to send message: {:?}", e);
                        } else {
                            self_clone.tracker.mark_sent(peer_id, message_id);
                            // Stop after sending the first message that can be sent
                            break 'peer;
                        }
                    } else {
                        log::trace!(target: "citadel", "[CANNOT SEND] message: {:?}", msg);
                        // If we can't send the current message, stop processing this group
                        break;
                    }
                }
            }
        }).await
    }

    async fn process_inbound(&self) {
        let pending_messages = match self.backend.get_pending_inbound().await {
            Ok(messages) => messages,
            Err(e) => {
                log::error!(target: "citadel", "Failed to get pending inbound messages: {:?}", e);
                return;
            }
        };

        // Sort the pending messages in order by MessageID
        let pending_messages: Vec<M> = pending_messages
            .into_iter()
            .sorted_by_key(|r| r.message_id())
            .collect();

        log::trace!(target: "citadel", "Processing inbound messages: {:?}", pending_messages);
        if let Some(delivery) = self.local_delivery.lock().await.as_ref() {
            for message in pending_messages {
                match delivery.deliver(message.clone()).await {
                    Ok(()) => {
                        // Create and send ACK
                        if let Err(e) = self
                            .network
                            .send_message(self.create_ack_message(&message))
                            .await
                        {
                            log::error!(target: "citadel", "Failed to send ACK: {e:?}");
                            continue;
                        }

                        // Clear delivered message from backend
                        if let Err(e) = self
                            .backend
                            .clear_message_inbound(message.source_id(), message.message_id())
                            .await
                        {
                            log::error!(target: "citadel", "Failed to clear delivered message: {e:?}");
                        }
                    }
                    Err(e) => {
                        log::error!(target: "citadel", "Failed to deliver message: {e:?}");
                    }
                }
            }
        } else {
            log::warn!(target: "citadel", "Unable to deliver messages since local delivery has been dropped");
        }
    }

    // Modify process_network_messages to update the tracker
    async fn process_next_network_message(&self) {
        if let Some(message) = self.network.next_message().await {
            match message {
                Payload::Poll { .. } => {
                    // This will trigger process_outbound() which already sends
                    // the next unacknowledged message due to head of line blocking
                    if self.poll_outbound_tx.send(()).is_err() {
                        log::warn!(target: "citadel", "Failed to send poll signal for outbound messages");
                    }
                }
                Payload::Ack {
                    from_id,
                    message_id,
                    to_id,
                } => {
                    if to_id != self.network.peer_id() {
                        log::warn!(target: "citadel", "Received ACK for another peer");
                        return;
                    }

                    // Update the tracker with the new ACK
                    self.tracker.update_ack(from_id, message_id);

                    log::trace!(target: "citadel", "Received ACK from peer {from_id}, message # {message_id}");
                    if let Err(e) = self
                        .backend
                        .clear_message_outbound(from_id, message_id)
                        .await
                    {
                        log::error!(target: "citadel", "Failed to clear ACKed message: {e:?}");
                    }

                    // Poll any pending outbound messages
                    if self.poll_outbound_tx.send(()).is_err() {
                        log::warn!(target: "citadel", "Failed to send poll signal for outbound messages");
                    }
                }
                Payload::Message(msg) => {
                    if msg.destination_id() != self.network.peer_id() {
                        log::warn!(target: "citadel", "Received message for another peer");
                        return;
                    }

                    if let Err(e) = self.backend.store_inbound(msg).await {
                        log::error!(target: "citadel", "Failed to store inbound message: {e:?}");
                    }

                    if self.poll_inbound_tx.send(()).is_err() {
                        log::warn!(target: "citadel", "Failed to send poll signal for inbound messages");
                    }
                }
            }
        }
    }

    pub async fn send_message(&self, message: M) -> Result<(), NetworkError> {
        if message.source_id() != self.network.peer_id() {
            return Err(NetworkError::SendFailed(
                "Source ID does not match network peer ID".into(),
            ));
        }

        if message.destination_id() == self.network.peer_id() {
            return Err(NetworkError::SendFailed(
                "Cannot send message to self".into(),
            ));
        }

        if self.is_running.load(std::sync::atomic::Ordering::Relaxed) {
            self.backend
                .store_outbound(message)
                .await
                .map_err(NetworkError::BackendError)?;

            self.poll_outbound_tx
                .send(())
                .map_err(|_| NetworkError::SystemShutdown)?;
            Ok(())
        } else {
            Err(NetworkError::SystemShutdown)
        }
    }

    fn create_ack_message(&self, original_message: &M) -> Payload<M> {
        // Must send an ACK back with a flipped order of the source and destination
        Payload::Ack {
            from_id: original_message.destination_id(),
            to_id: original_message.source_id(),
            message_id: original_message.message_id(),
        }
    }

    /// Shutdown the message system gracefully
    /// This will stop the background tasks and wait for pending messages to be processed
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), NetworkError> {
        self.is_running
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // Wait for pending messages to be processed
        tokio::time::timeout(timeout, async {
            while !self
                .backend
                .get_pending_outbound()
                .await
                .map_err(NetworkError::BackendError)?
                .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_, NetworkError>(())
        })
        .await
        .map_err(|err| NetworkError::ShutdownFailed(err.to_string()))??;

        Ok(())
    }

    fn can_run(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// Example InMemoryBackend implementation
#[derive(Clone)]
pub struct InMemoryBackend<M: MessageMetadata> {
    outbound: Mailbox<M::PeerId, M::MessageId, M>,
    inbound: Mailbox<M::PeerId, M::MessageId, M>,
}

type Mailbox<PeerId, MessageId, Message> = Arc<RwLock<HashMap<(PeerId, MessageId), Message>>>;

impl<M: MessageMetadata> InMemoryBackend<M> {
    pub fn new() -> Self {
        Self {
            outbound: Arc::new(RwLock::new(HashMap::new())),
            inbound: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<M: MessageMetadata> Default for InMemoryBackend<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<M: MessageMetadata + Clone + Send + Sync + 'static> Backend<M> for InMemoryBackend<M> {
    async fn store_outbound(&self, message: M) -> Result<(), BackendError> {
        let mut outbound = self.outbound.write().await;
        let (source_id, destination_id, message_id) = (
            message.source_id(),
            message.destination_id(),
            message.message_id(),
        );

        if outbound
            .insert((destination_id, message_id), message)
            .is_some()
        {
            log::warn!(target: "citadel",
                "Overwriting existing message in outbound storage dest={}/id={}",
                source_id,
                message_id
            );
        }
        Ok(())
    }

    async fn store_inbound(&self, message: M) -> Result<(), BackendError> {
        let mut inbound = self.inbound.write().await;
        let (source_id, message_id) = (message.source_id(), message.message_id());

        if inbound.insert((source_id, message_id), message).is_some() {
            log::warn!(target: "citadel",
                "Overwriting existing message in inbound storage src={}/id={}",
                source_id,
                message_id
            );
        }
        Ok(())
    }

    async fn clear_message_inbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError> {
        // Try to remove from both outbound and inbound
        let mut inbound = self.inbound.write().await;
        if inbound.remove(&(peer_id, message_id)).is_none() {
            log::warn!(target: "citadel",
                "Failed to clear message from inbound storage src={}/id={}",
                peer_id,
                message_id
            );
        }

        Ok(())
    }

    async fn clear_message_outbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError> {
        let mut outbound = self.outbound.write().await;
        if outbound.remove(&(peer_id, message_id)).is_none() {
            log::warn!(target: "citadel",
                "Failed to clear message from outbound storage dest={}/id={}",
                peer_id,
                message_id
            );
        }

        Ok(())
    }

    async fn get_pending_outbound(&self) -> Result<Vec<M>, BackendError> {
        let outbound = self.outbound.read().await;
        Ok(outbound.values().cloned().collect())
    }

    async fn get_pending_inbound(&self) -> Result<Vec<M>, BackendError> {
        let inbound = self.inbound.read().await;
        Ok(inbound.values().cloned().collect())
    }
}

#[async_trait]
impl<M: MessageMetadata> LocalDelivery<M> for tokio::sync::mpsc::UnboundedSender<M> {
    async fn deliver(&self, message: M) -> Result<(), DeliveryError> {
        self.send(message).map_err(|_| DeliveryError::ChannelClosed)
    }
}

pub struct CitadelBackend {}

#[allow(unused_variables)]
#[async_trait]
impl<M: MessageMetadata> Backend<M> for CitadelBackend {
    async fn store_outbound(&self, message: M) -> Result<(), BackendError> {
        todo!()
    }

    async fn store_inbound(&self, message: M) -> Result<(), BackendError> {
        todo!()
    }

    async fn clear_message_inbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError> {
        todo!()
    }

    async fn clear_message_outbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError> {
        todo!()
    }

    async fn get_pending_outbound(&self) -> Result<Vec<M>, BackendError> {
        todo!()
    }

    async fn get_pending_inbound(&self) -> Result<Vec<M>, BackendError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::FuturesOrdered;
    use futures::StreamExt;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::sync::Mutex;

    pub struct InMemoryNetwork<M: MessageMetadata> {
        messages: InMemoryMessageQueue<M::PeerId, M>,
        my_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Payload<M>>>>,
        my_id: M::PeerId,
    }

    pub type InMemoryMessageQueue<PeerId, M> =
        Arc<RwLock<HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<Payload<M>>>>>;

    impl<M: MessageMetadata> Clone for InMemoryNetwork<M> {
        fn clone(&self) -> Self {
            Self {
                messages: self.messages.clone(),
                my_rx: self.my_rx.clone(),
                my_id: self.my_id,
            }
        }
    }

    impl<M: MessageMetadata> InMemoryNetwork<M> {
        pub fn new() -> Self {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let mut map = HashMap::new();
            map.insert(M::PeerId::default(), tx);
            Self {
                messages: Arc::new(RwLock::new(map)),
                my_rx: Arc::new(Mutex::new(rx)),
                my_id: M::PeerId::default(),
            }
        }

        pub async fn add_peer(&self, id: M::PeerId) -> Self {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.messages.write().await.insert(id, tx);
            Self {
                messages: self.messages.clone(),
                my_rx: Arc::new(Mutex::new(rx)),
                my_id: id,
            }
        }

        pub async fn send_to_peer(
            &self,
            id: M::PeerId,
            message: Payload<M>,
        ) -> Result<(), NetworkError> {
            if let Some(tx) = self.messages.read().await.get(&id) {
                tx.send(message)
                    .map_err(|_| NetworkError::SendFailed("Failed to send message".into()))
            } else {
                Err(NetworkError::ConnectionError("Peer not found".into()))
            }
        }
    }

    #[async_trait]
    impl<M: MessageMetadata> Network for InMemoryNetwork<M> {
        type Message = M;

        async fn next_message(&self) -> Option<Payload<Self::Message>> {
            self.my_rx.lock().await.recv().await
        }

        async fn send_message(&self, message: Payload<Self::Message>) -> Result<(), NetworkError> {
            match &message {
                Payload::Message(msg) => {
                    let peer_id = msg.destination_id();
                    self.send_to_peer(peer_id, message).await
                }
                Payload::Ack { to_id, .. } => self.send_to_peer(*to_id, message).await,
                Payload::Poll { to_id, .. } => self.send_to_peer(*to_id, message).await,
            }
        }

        async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId> {
            self.messages.read().await.keys().cloned().collect()
        }

        fn peer_id(&self) -> <Self::Message as MessageMetadata>::PeerId {
            self.my_id
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestMessage {
        job_id: usize,
        source_id: usize,
        destination_id: usize,
        message_id: usize,
        contents: Vec<u8>,
    }

    #[async_trait]
    impl MessageMetadata for TestMessage {
        type PeerId = usize;
        type MessageId = usize;

        fn source_id(&self) -> Self::PeerId {
            self.source_id
        }

        fn destination_id(&self) -> Self::PeerId {
            self.destination_id
        }

        fn message_id(&self) -> Self::MessageId {
            self.message_id
        }

        fn contents(&self) -> &[u8] {
            &self.contents
        }
    }

    #[tokio::test]
    async fn test_message_system() {
        let mut peer_futures = FuturesOrdered::new();
        const NUM_PEERS: usize = 3;
        let network = InMemoryNetwork::<TestMessage>::new();

        for this_peer_id in 0..NUM_PEERS {
            let network = network.add_peer(this_peer_id).await;
            let backend = InMemoryBackend::<TestMessage>::default();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let message_system = MessageSystem::new(backend, tx, network.clone());

            let future = async move {
                let message_contents = vec![1, 2, 3];
                // Every peer starts by sending a message to every other peer except themself
                for destination_id in 0..NUM_PEERS {
                    if destination_id == this_peer_id {
                        continue;
                    }

                    let message = TestMessage {
                        job_id: 1,
                        source_id: this_peer_id,
                        destination_id,
                        message_id: 1,
                        contents: message_contents.clone(),
                    };

                    message_system.send_message(message).await.unwrap();
                }

                // Check if the message was received by the destination peer
                let mut received_messages = vec![];
                // Receive NUM_PEERS-1 messages
                for _ in 0..NUM_PEERS - 1 {
                    if let Some(received_message) = rx.recv().await {
                        received_messages.push(received_message);
                    }
                }

                // Assertions over all messages
                for received_message in received_messages {
                    assert_eq!(received_message.contents(), &message_contents);
                    assert_ne!(received_message.source_id(), this_peer_id);
                    assert_eq!(received_message.destination_id(), this_peer_id);
                }

                1usize
            };

            peer_futures.push_back(Box::pin(future) as Pin<Box<dyn Future<Output = usize>>>);
        }

        let sum = peer_futures
            .collect::<Vec<usize>>()
            .await
            .iter()
            .sum::<usize>();
        assert_eq!(sum, NUM_PEERS);
    }

    #[tokio::test]
    async fn test_send_message_to_self() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend, tx, network.clone());

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 1,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_message(message).await;
        assert!(matches!(result, Err(NetworkError::SendFailed(_))));
    }

    #[tokio::test]
    async fn test_send_message_with_mismatched_source_id() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend, tx, network.clone());

        let message = TestMessage {
            job_id: 1,
            source_id: 2, // Mismatched source ID
            destination_id: 1,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_message(message).await;
        assert!(matches!(result, Err(NetworkError::SendFailed(_))));
    }

    #[tokio::test]
    async fn test_handle_acks_properly() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        message_system.send_message(message.clone()).await.unwrap();

        let ack = Payload::Ack {
            from_id: 2,
            to_id: 1,
            message_id: 1,
        };

        network.send_to_peer(1, ack).await.unwrap();

        // wait some time for internal state to update
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Check if the message was cleared from the backend
        let pending_outbound = backend.get_pending_outbound().await.unwrap();
        assert!(pending_outbound.is_empty());
    }

    #[tokio::test]
    async fn test_message_ordering() {
        citadel_logging::setup_log_no_panic_hook();
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;

        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend, tx, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        let messages = vec![
            TestMessage {
                job_id: 1,
                source_id: 1,
                destination_id: 2,
                message_id: 1,
                contents: vec![1],
            },
            TestMessage {
                job_id: 1,
                source_id: 1,
                destination_id: 2,
                message_id: 0,
                contents: vec![0],
            },
        ];

        for message in messages {
            log::info!(target: "citadel", "Sending message: {:?}", message);
            message_system.send_message(message).await.unwrap();
        }

        // Check if the messages are received in order
        let mut received_messages = vec![];
        for _ in 0..2 {
            if let Some(received_message) = rx2.recv().await {
                received_messages.push(received_message);
            }
        }

        assert_eq!(received_messages[0].message_id, 0);
        assert_eq!(received_messages[1].message_id, 1);
        assert_eq!(received_messages[0].contents(), &[0]);
        assert_eq!(received_messages[1].contents(), &[1]);
    }

    #[tokio::test]
    async fn test_stop_message_system() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend, tx, network.clone());

        message_system
            .is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_message(message).await;
        assert!(matches!(result, Err(NetworkError::SystemShutdown)));
    }

    #[tokio::test]
    async fn test_stress_send_large_number_of_messages() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend, tx, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        const NUM_MESSAGES: u8 = 255;
        let mut messages = vec![];

        for i in 0..NUM_MESSAGES {
            let message = TestMessage {
                job_id: 1,
                source_id: 1,
                destination_id: 2,
                message_id: i as _,
                contents: vec![i],
            };
            messages.push(message);
        }

        for message in messages {
            message_system.send_message(message).await.unwrap();
        }

        let mut received_count = 0;
        while let Some(received_message) = rx2.recv().await {
            assert_eq!(received_message.contents(), &[received_count]);
            received_count += 1;
            if received_count == NUM_MESSAGES {
                break;
            }
        }

        assert_eq!(received_count, NUM_MESSAGES);
    }

    #[tokio::test]
    async fn test_invalid_peer_handling() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        // Try to send to non-existent peer
        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 999, // Non-existent peer
            message_id: 1,
            contents: vec![1],
        };

        // Message should be stored but fail to send
        message_system.send_message(message).await.unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // The message should remain in the backend
        let pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_message_handling() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        // Send the same message twice
        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        message_system.send_message(message.clone()).await.unwrap();
        message_system.send_message(message.clone()).await.unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify that the message has been overwritten.
        let pending = backend.get_pending_outbound().await.unwrap();
        assert!(pending.is_empty());

        // Verify only one message is received
        let received = rx2.recv().await;
        assert!(received.is_some());
        assert!(rx2.try_recv().is_err()); // No second message should be received
    }

    #[tokio::test]
    async fn test_message_system_shutdown_cleanup() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        // Send a message
        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        message_system.send_message(message).await.unwrap();

        // Shutdown the system
        message_system
            .is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify local delivery is dropped
        let local_delivery = message_system.local_delivery.lock().await;
        assert!(local_delivery.is_none());
    }

    #[tokio::test]
    async fn test_message_persistence_until_delivery() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        // Send message
        message_system.send_message(message.clone()).await.unwrap();

        // Verify message is in the backend
        let pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 1);

        // Wait some time
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Message should still be in backend since no ACK was received
        let still_pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(still_pending.len(), 1);
        assert_eq!(still_pending[0].message_id(), message.message_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bidirectional_parallel_messaging() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1, tx1, network.clone());
        let message_system2 = MessageSystem::new(backend2, tx2, network2.clone());

        // Peer 1 sends to Peer 2
        let message1 = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        // Peer 2 sends to Peer 1
        let message2 = TestMessage {
            job_id: 1,
            source_id: 2,
            destination_id: 1,
            message_id: 1,
            contents: vec![2],
        };

        // Send messages in parallel using separate threads
        let send_handle1 = tokio::spawn({
            let message_system1 = message_system1.clone();
            let message1 = message1.clone();
            async move { message_system1.send_message(message1).await }
        });

        let send_handle2 = tokio::spawn({
            let message_system2 = message_system2.clone();
            let message2 = message2.clone();
            async move { message_system2.send_message(message2).await }
        });

        let res1 = send_handle1.await.unwrap();
        let res2 = send_handle2.await.unwrap();

        assert!(res1.is_ok());
        assert!(res2.is_ok());

        // Receive messages
        let received1 = rx1.recv().await.expect("Peer 1 should receive a message");
        let received2 = rx2.recv().await.expect("Peer 2 should receive a message");

        // Verify messages
        assert_eq!(received1.source_id(), 2);
        assert_eq!(received1.destination_id(), 1);
        assert_eq!(received1.contents(), &[2]);

        assert_eq!(received2.source_id(), 1);
        assert_eq!(received2.destination_id(), 2);
        assert_eq!(received2.contents(), &[1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bidirectional_messaging_stress() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1, tx1, network.clone());
        let message_system2 = MessageSystem::new(backend2, tx2, network2.clone());

        const NUM_MESSAGES: u8 = 255;

        // Create tasks for peer 1 sending messages
        let send_task1 = tokio::spawn({
            let message_system1 = message_system1.clone();
            async move {
                for i in 0..NUM_MESSAGES {
                    let message = TestMessage {
                        job_id: 1,
                        source_id: 1,
                        destination_id: 2,
                        message_id: i as _,
                        contents: vec![i],
                    };
                    message_system1.send_message(message).await.unwrap();
                }
            }
        });

        // Create tasks for peer 2 sending messages
        let send_task2 = tokio::spawn({
            let message_system2 = message_system2.clone();
            async move {
                for i in 0..NUM_MESSAGES {
                    let message = TestMessage {
                        job_id: 1,
                        source_id: 2,
                        destination_id: 1,
                        message_id: i as _,
                        contents: vec![i],
                    };
                    message_system2.send_message(message).await.unwrap();
                }
            }
        });

        // Create tasks for receiving messages
        let receive_task1 = tokio::spawn(async move {
            let mut received = 0;
            while received < NUM_MESSAGES {
                if let Ok(Some(msg)) =
                    tokio::time::timeout(Duration::from_secs(5), rx1.recv()).await
                {
                    assert_eq!(msg.source_id(), 2);
                    assert_eq!(msg.destination_id(), 1);
                    assert_eq!(msg.contents(), &[received]);
                    received += 1;
                } else {
                    panic!("Timeout waiting for messages at peer 1");
                }
            }
        });

        let receive_task2 = tokio::spawn(async move {
            let mut received = 0;
            while received < NUM_MESSAGES {
                if let Ok(Some(msg)) =
                    tokio::time::timeout(Duration::from_secs(5), rx2.recv()).await
                {
                    assert_eq!(msg.source_id(), 1);
                    assert_eq!(msg.destination_id(), 2);
                    assert_eq!(msg.contents(), &[received]);
                    received += 1;
                } else {
                    panic!("Timeout waiting for messages at peer 2");
                }
            }
        });

        // Wait for all tasks to complete
        let results =
            futures::future::join_all(vec![send_task1, send_task2, receive_task1, receive_task2])
                .await;

        // Check if any task failed
        for result in results {
            result.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_message_system_shutdown_during_send() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        // Start sending messages
        let send_handle = tokio::spawn({
            let message_system = message_system.clone();
            async move {
                let mut results = Vec::new();
                for i in 0..10000 {
                    let message = TestMessage {
                        job_id: 1,
                        source_id: 1,
                        destination_id: 2,
                        message_id: i,
                        contents: vec![1],
                    };
                    let result = message_system.send_message(message).await;
                    results.push(result);
                }
                results
            }
        });

        // Wait a bit then shutdown the system
        tokio::time::sleep(Duration::from_millis(10)).await;
        message_system
            .is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Check results
        let results = send_handle.await.unwrap();
        assert!(results
            .iter()
            .any(|r| matches!(r, Err(NetworkError::SystemShutdown))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_empty_message_contents() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1, tx1, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![], // Empty contents
        };

        message_system1.send_message(message.clone()).await.unwrap();
        let received = rx2.recv().await.expect("Should receive message");
        assert!(received.contents().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_local_delivery_drop() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        // Drop local delivery
        {
            let mut local_delivery = message_system.local_delivery.lock().await;
            *local_delivery = None;
        }

        // Try to process inbound messages
        let message = TestMessage {
            job_id: 1,
            source_id: 2,
            destination_id: 1,
            message_id: 1,
            contents: vec![1],
        };

        // Store message directly in backend
        backend.store_inbound(message).await.unwrap();

        // Wait some time to ensure processing cycle has run
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Message should still be in backend since delivery failed
        let pending = backend.get_pending_inbound().await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_max_message_id() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1, tx1, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: usize::MAX, // Maximum possible message ID
            contents: vec![1],
        };

        message_system1.send_message(message.clone()).await.unwrap();
        let received = rx2.recv().await.expect("Should receive message");
        assert_eq!(received.message_id(), usize::MAX);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_backend_error_handling() {
        struct FailingBackend;

        #[async_trait]
        impl Backend<TestMessage> for FailingBackend {
            async fn store_outbound(&self, _: TestMessage) -> Result<(), BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn store_inbound(&self, _: TestMessage) -> Result<(), BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn clear_message_inbound(&self, _: usize, _: usize) -> Result<(), BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn clear_message_outbound(&self, _: usize, _: usize) -> Result<(), BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn get_pending_outbound(&self) -> Result<Vec<TestMessage>, BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn get_pending_inbound(&self) -> Result<Vec<TestMessage>, BackendError> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
        }

        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(FailingBackend, tx, network.clone());

        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        let result = message_system.send_message(message).await;
        assert!(matches!(
            result,
            Err(NetworkError::BackendError(BackendError::StorageError(_)))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_intersession_recovery() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1.clone(), tx1, network.clone());

        // Send messages before peer 2 connects
        for i in 0..3 {
            let message = TestMessage {
                job_id: 1,
                source_id: 1,
                destination_id: 2,
                message_id: i,
                contents: vec![i as u8],
            };
            message_system1.send_message(message).await.unwrap();
        }

        // Verify messages are in the backend
        let pending = backend1.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 3);

        // Now create peer 2's message system - this should trigger the peer polling mechanism
        let network2 = network.add_peer(2).await;
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        // Should receive all messages in order due to polling
        for i in 0..3 {
            match tokio::time::timeout(Duration::from_secs(6), rx2.recv()).await {
                Ok(Some(received)) => {
                    assert_eq!(received.message_id(), i);
                    assert_eq!(received.contents(), &[i as u8]);
                }
                Ok(None) => panic!("Channel closed"),
                Err(_) => panic!("Timeout waiting for message {}", i),
            }
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = MessageSystem::new(backend1.clone(), tx1, network.clone());
        let _message_system2 = MessageSystem::new(backend2, tx2, network2);

        // Send some messages
        for i in 0..3 {
            let message = TestMessage {
                job_id: 1,
                source_id: 1,
                destination_id: 2,
                message_id: i,
                contents: vec![i as u8],
            };
            message_system1.send_message(message).await.unwrap();
        }

        // Give some time for ACKs to be processed
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Now start graceful shutdown with a reasonable timeout
        let shutdown_result = message_system1.shutdown(Duration::from_secs(1)).await;
        assert!(shutdown_result.is_ok());

        // Verify no pending messages in backend
        let pending = backend1.get_pending_outbound().await.unwrap();
        assert!(pending.is_empty());

        // Verify system is actually shutdown
        assert!(!message_system1.can_run());

        // Verify sending new messages fails
        let new_message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 2,
            message_id: 100,
            contents: vec![0],
        };
        let send_result = message_system1.send_message(new_message).await;
        assert!(matches!(send_result, Err(NetworkError::SystemShutdown)));
    }

    #[tokio::test]
    async fn test_shutdown_timeout() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = MessageSystem::new(backend.clone(), tx, network.clone());

        // Send a message to a non-existent peer (will never be delivered)
        let message = TestMessage {
            job_id: 1,
            source_id: 1,
            destination_id: 999,
            message_id: 1,
            contents: vec![1],
        };
        message_system.send_message(message).await.unwrap();

        // Try to shutdown with a very short timeout
        let shutdown_result = message_system.shutdown(Duration::from_millis(10)).await;
        assert!(matches!(
            shutdown_result,
            Err(NetworkError::ShutdownFailed(_))
        ));

        // System should still be marked as not running even if shutdown timed out
        assert!(!message_system.can_run());
    }
}
