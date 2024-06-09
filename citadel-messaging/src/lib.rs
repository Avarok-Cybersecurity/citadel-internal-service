use citadel_internal_service_connector::connector::InternalServiceConnector;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::messaging_layer::{
    CWMessage, CWProtocolMessage, MessengerUpdate, OutgoingCWMessage,
};
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use log::log;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Messenger<T: IOInterface> {
    connection: T::Sink,
    tx_to_subscriber: UnboundedSender<MessengerUpdate>,
    rx_from_messenger: Option<UnboundedReceiver<MessengerUpdate>>,
    internal_service_listeners:
        Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<InternalServiceResponse>>>>,
    is_running: Arc<AtomicBool>,
}

impl<T: IOInterface> Messenger<T> {
    pub fn new(connection: InternalServiceConnector<T>) -> Self {
        let (tx_to_subscriber, rx_from_messenger) = tokio::sync::mpsc::unbounded_channel();
        let InternalServiceConnector { sink, stream } = connection;

        let tx_to_subscriber_clone = tx_to_subscriber.clone();

        let internal_service_listeners = Arc::new(Mutex::new(HashMap::new()));
        let interal_service_listeners_clone = internal_service_listeners.clone();

        struct DropSafe {
            inner: Arc<AtomicBool>,
        }

        impl Drop for DropSafe {
            fn drop(&mut self) {
                self.inner.store(false, Ordering::Relaxed);
            }
        }

        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = is_running.clone();

        let receive_task = async move {
            let _ = DropSafe {
                inner: is_running_clone,
            };
            while let Some(response) = stream.next().await {
                let mut lock = interal_service_listeners_clone.lock().await;
                if let Some(tx) = lock.remove(response.request_id()) {
                    if let Err(err) = tx.send(response) {
                        // Send through subscriber as backup
                        if let Err(err) = tx_to_subscriber_clone.send(err) {}
                    }
                } else {
                    // Send through normal channel
                    let signal = MessengerUpdate::Other { response };
                    if let Err(err) = tx_to_subscriber_clone.send(signal) {}
                }
            }
        };

        tokio::task::spawn(receive_task);

        Self {
            connection: sink,
            tx_to_subscriber,
            rx_from_messenger: Some(rx_from_messenger),
            internal_service_listeners,
            is_running,
        }
    }

    pub fn take_messenger_update_handle(&mut self) -> Option<UnboundedReceiver<MessengerUpdate>> {
        self.rx_from_messenger.take()
    }

    pub async fn send_new_message(&mut self, message: OutgoingCWMessage) -> std::io::Result<()> {
        let latest_id = self
            .latest_received_message_id(message.cid, message.peer_cid)
            .await?;
        let next_id = latest_id.wrapping_add(1);

        let message = CWMessage {
            id: next_id,
            cid: message.cid,
            peer_cid: message.peer_cid,
            contents: message.contents,
        };

        // TODO: figure out if our last id was already acknowledged, otherwise, enqueue the message

        Ok(())
    }

    pub async fn register_received_messages(
        &self,
        messages: &Vec<CWMessage>,
    ) -> std::io::Result<()> {
    }

    async fn store_to_backend(&mut self, messages: &Vec<CWMessage>) -> std::io::Result<()> {
        let messages_ordered_by_cid =
            messages
                .iter()
                .fold(HashMap::<u64, Vec<&CWMessage>>::new(), |mut acc, msg| {
                    acc.entry(msg.cid).or_default().push(msg);
                    acc
                });

        for (cid, messages) in messages_ordered_by_cid {
            let mut largest_id = 0;

            for message in messages {
                let command = InternalServiceRequest::LocalDBSetKV {
                    request_id: Uuid::new_v4(),
                    cid,
                    peer_cid: message.peer_cid,
                    key: generate_message_key_for_cid(message.cid),
                    value: message.contents.clone(),
                };

                if largest_id < message.id {
                    largest_id = message.id;
                }

                self.connection.send(command).await?;
            }

            // TODO: fix this cid/peer_cid stuff
            let request_for_largest_received = InternalServiceRequest::LocalDBGetKV {
                request_id: Uuid::new_v4(),
                cid: 0,
                peer_cid: None,
                key: generate_highest_message_id_key_for_cid_received(cid),
            };

            self.connection.send(request_for_largest_received).await?;
        }

        Ok(())
    }

    pub async fn wait_for_response(
        &self,
        request_id: Uuid,
    ) -> std::io::Result<InternalServiceResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.register_listener_internal(request_id, tx).await;
        Ok(rx.await?)
    }

    async fn register_listener_internal(
        &self,
        uuid: Uuid,
        tx: tokio::sync::oneshot::Sender<InternalServiceResponse>,
    ) {
        self.internal_service_listeners
            .lock()
            .await
            .insert(uuid, tx);
    }

    pub fn into_inner(self) -> InternalServiceConnector<T> {
        self.connection
    }

    async fn latest_received_message_id(
        &self,
        cid: u64,
        peer_cid: Option<u64>,
    ) -> std::io::Result<u64> {
        let request_id = Uuid::new_v4();
        let request_for_largest = InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid,
            peer_cid,
            key: generate_highest_message_id_key_for_cid_received(cid, peer_cid),
        };

        // TODO: latest message WE sent

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.register_listener_internal(request_id, tx).await;
        self.connection.send(request_for_largest).await?;
        if let InternalServiceResponse::LocalDBGetKVSuccess(value) = rx.await? {
            let highest_value = be_vec_to_u64(&value.value)
                .ok_or_else(|| generic_std_error("Invalid highest CID encoding"))?;
            Ok(highest_value)
        } else {
            return Err(generic_std_error("Failed to get latest message id"));
        }
    }

    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
}

impl<T: IOInterface> Deref for Messenger<T> {
    type Target = InternalServiceConnector<T>;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<T: IOInterface> DerefMut for Messenger<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<T: IOInterface> From<InternalServiceConnector<T>> for Messenger<T> {
    fn from(value: InternalServiceConnector<T>) -> Self {
        Self::new(value)
    }
}

fn generate_message_key_for_cid(cid: u64) -> String {
    format!("__internal__cw-workspace-messages-{cid}")
}

fn generate_highest_message_id_key_for_cid_received(cid: u64, peer_cid: Option<u64>) -> String {
    format!("__internal__cw-workspace-messages-highest-id-{cid}-{peer_cid:?}")
}

fn u64_to_be_vec(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn be_vec_to_u64(value: &[u8]) -> Option<u64> {
    if value.len() != 8 {
        return None;
    }

    let mut ret = [0u8; 8];
    ret.copy_from_slice(value);

    Some(u64::from_be_bytes(ret))
}

fn generic_std_error<T: Into<String>>(message: T) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message.into())
}
