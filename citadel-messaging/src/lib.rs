use citadel_internal_service_connector::connector::{InternalServiceConnector, WrappedSink};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::messaging_layer::{
    CWMessage, MessengerUpdate, OutgoingCWMessage,
};
use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
use futures::sink::SinkExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Messenger<T: IOInterface> {
    connection: WrappedSink<T>,
    tx_to_subscriber: UnboundedSender<MessengerUpdate>,
    internal_service_senders:
        Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<MessengerUpdate>>>>,
    internal_service_listeners:
        Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Receiver<MessengerUpdate>>>>,
    is_running: Arc<AtomicBool>,
}

#[derive(Serialize, Deserialize)]
struct ConnectionPairDatabase {
    // message ID => Message
    sent_messages: HashMap<u64, MessengerUpdate>,
    received_messages: HashMap<u64, MessengerUpdate>,
    greatest_acknowledged_id: Option<u64>,
    implicated_cid: u64,
    peer_cid: Option<u64>,
}

impl<T: IOInterface> Messenger<T> {
    pub fn new(
        connection: InternalServiceConnector<T>,
    ) -> (
        UnboundedSender<OutgoingCWMessage>,
        UnboundedReceiver<MessengerUpdate>,
    ) {
        let (tx_to_subscriber, rx_from_messenger) = tokio::sync::mpsc::unbounded_channel();
        let (
            tx_to_internal_outbound_message_receiver,
            mut rx_from_internal_outbound_message_receiver,
        ) = tokio::sync::mpsc::unbounded_channel();
        let InternalServiceConnector { sink, mut stream } = connection;

        let tx_to_subscriber_clone = tx_to_subscriber.clone();

        let internal_service_listeners = Arc::new(Mutex::new(HashMap::new()));
        let internal_service_senders = Arc::new(Mutex::new(HashMap::new()));
        let internal_service_senders_clone = internal_service_senders.clone();

        let mut this = Self {
            connection: sink,
            tx_to_subscriber,
            internal_service_listeners,
            internal_service_senders,
            is_running,
        };

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

            let task0 = async move {
                while let Some(message) = rx_from_internal_outbound_message_receiver.recv().await {
                    if let MessengerUpdate::Message { message } = message {
                        // Store the message to the backend
                        let messages = vec![message];
                        if let Err(err) = this.register_received_messages(&messages).await {
                            // Send through subscriber as backup
                            if let Err(_err) = tx_to_subscriber_clone.send(MessengerUpdate::Other {
                                response: InternalServiceResponse::Error(err.to_string()),
                            }) {}
                        }
                    }
                }
            };

            let task1 = async move {
                while let Some(response) = stream.next().await {
                    let mut lock = internal_service_senders_clone.lock().await;

                    if let Some(uuid) = response.request_id() {
                        if let Some(tx) = lock.remove(uuid) {
                            if let Err(response) = tx.send(response) {
                                // Send through subscriber as backup
                                if let Err(_err) = tx_to_subscriber_clone.send(response) {}
                            }

                            continue;
                        }
                    }

                    // Send through normal channel
                    let signal = MessengerUpdate::Other { response };
                    if let Err(_err) = tx_to_subscriber_clone.send(signal) {}
                }
            };

            tokio::try_join!(task0, task1)
                .map_err(|err| generic_std_error(format!("Failed to receive message: {}", err)))?;
        };

        tokio::task::spawn(receive_task);

        (tx_to_internal_outbound_message_receiver, rx_from_messenger)
    }

    /// Take self exclusively to ensure no multiple simultaneous access to sending messages outbound
    /// This function is expected to be c
    async fn send_new_message(&mut self, message: OutgoingCWMessage) -> std::io::Result<()> {
        if !self.ensure_all_local_outbound_messages_received().await? {
            // If not all messages are received, we should enqueue the message for later background processing
            // NOTE: This message, once it is up for sending, will need to call the below code
            todo!();
        }

        // By now, we know that:
        // * All our latest outbound messages have been received, and;
        // * We are ready to send a new message
        // Therefore, we can send the message outbound automatically and unconditionally
        self.send_message_unconditional(message).await?;

        Ok(())
    }

    async fn send_message_unconditional(
        &mut self,
        message: OutgoingCWMessage,
    ) -> std::io::Result<()> {
        let latest_id = self
            .latest_received_message_id(message.cid, message.peer_cid)
            .await?;

        let next_id = latest_id.wrapping_add(1);

        let request_id = message.request_id;
        let security_level = message.security_level.unwrap_or_default();

        let message_id = next_id;
        let cid = message.cid;
        let peer_cid = message.peer_cid;

        let message = CWMessage {
            id: message_id,
            cid,
            peer_cid,
            contents: message.contents,
        };

        // Update our local record of sent messages
        self.visit_database(message.cid, message.peer_cid, |mut db| {
            db.sent_messages.insert(
                message_id,
                MessengerUpdate::Message {
                    message: message.clone(),
                },
            );
            db
        })
        .await?;

        // Before sending, register an rx listener for the response message for a given request_id
        self.register_listener_internal(request_id).await;

        let messenger_update = MessengerUpdate::Message { message };

        // TODO: Ensure deserializing code of `message` expects `MessengerUpdate`
        let request = InternalServiceRequest::Message {
            request_id,
            message: bincode2::serialize(&messenger_update).map_err(|err| {
                generic_std_error(format!("Failed to serialize message: {}", err))
            })?,
            cid,
            peer_cid,
            security_level,
        };

        self.connection.send(request).await?;

        Ok(())
    }

    async fn ensure_all_local_outbound_messages_received(&mut self) -> std::io::Result<bool> {
        todo!()
    }

    async fn register_received_messages(
        &mut self,
        messages: &Vec<CWMessage>,
    ) -> std::io::Result<()> {
        for message in messages {
            self.visit_database(message.cid, message.peer_cid, |mut db| {
                db.received_messages.insert(
                    message.id,
                    MessengerUpdate::Message {
                        message: message.clone(),
                    },
                );

                if let Some(current_greatest) = db.greatest_acknowledged_id {
                    if message.id > current_greatest {
                        db.greatest_acknowledged_id = Some(message.id);
                    }
                } else {
                    db.greatest_acknowledged_id = Some(message.id);
                }

                db
            })
            .await?;

            // Update the highest received message id
            let request_id = Uuid::new_v4();
            let request = InternalServiceRequest::LocalDBSetKV {
                request_id,
                cid: message.cid,
                peer_cid: message.peer_cid,
                key: generate_highest_message_id_key_for_cid_received(
                    message.cid,
                    message.peer_cid,
                ),
                value: u64_to_be_vec(message.id),
            };

            let response = self.send_and_wait_for_response(request).await?;
            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                // Continue
            } else {
                return Err(generic_std_error(
                    "Failed to update highest received message id",
                ));
            }
        }

        Ok(())
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
            let mut largest_cid = None;
            let mut largest_peer_cid = None;

            for message in messages {
                let request_id = Uuid::new_v4();
                let command = InternalServiceRequest::LocalDBSetKV {
                    request_id,
                    cid,
                    peer_cid: message.peer_cid,
                    key: generate_message_database_key_for_connection_pair(
                        message.cid,
                        message.peer_cid,
                    ),
                    value: message.contents.clone(),
                };

                if largest_id < message.id {
                    largest_id = message.id;
                    largest_cid = Some(message.cid);
                    largest_peer_cid = message.peer_cid;
                }

                let response = self.send_and_wait_for_response(command).await?;
                if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
                    // Continue
                } else {
                    return Err(generic_std_error("Failed to store message"));
                }
            }

            let cid = largest_cid.ok_or_else(|| generic_std_error("Failed to get largest CID"))?;
            let peer_cid = largest_peer_cid;
            let request_id = Uuid::new_v4();

            let request_for_largest_received = InternalServiceRequest::LocalDBSetKV {
                request_id,
                cid,
                peer_cid,
                value: u64_to_be_vec(largest_id),
                key: generate_highest_message_id_key_for_cid_received(cid, peer_cid),
            };

            if let InternalServiceResponse::LocalDBSetKVSuccess(_) = self
                .send_and_wait_for_response(request_for_largest_received)
                .await?
            {
                // Continue
            } else {
                return Err(generic_std_error("Failed to get largest message id"));
            }
        }

        Ok(())
    }

    async fn send_and_wait_for_response(
        &mut self,
        internal_service_request: InternalServiceRequest,
    ) -> std::io::Result<InternalServiceResponse> {
        let request_id = internal_service_request
            .request_id()
            .copied()
            .ok_or_else(|| {
                generic_std_error("Failed to get request id for internal service request")
            })?;

        self.register_listener_internal(request_id).await;
        self.connection.send(internal_service_request).await?;

        let rx = self
            .internal_service_listeners
            .lock()
            .await
            .remove(&request_id)
            .ok_or_else(|| generic_std_error("Failed to get listener for request"))?;

        let recv = rx.await.map_err(|_| {
            generic_std_error(format!(
                "Failed to get response for request: {request_id:?}"
            ))
        });

        match recv.map_err(|err| generic_std_error(format!("Failed to get response: {err}")))? {
            update @ MessengerUpdate::Message { .. } => {
                self.tx_to_subscriber
                    .send(update)
                    .map_err(|err| generic_std_error(format!("Failed to send message: {err}")))?;
            }

            MessengerUpdate::Other { response } => Ok(response),
        }
    }

    async fn register_listener_internal(&self, uuid: Uuid) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.internal_service_senders.lock().await.insert(uuid, tx);
        self.internal_service_listeners
            .lock()
            .await
            .insert(uuid, rx);
    }

    async fn latest_received_message_id(
        &mut self,
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

        let response = self.send_and_wait_for_response(request_for_largest).await?;
        if let InternalServiceResponse::LocalDBGetKVSuccess(value) = response {
            let highest_value = be_vec_to_u64(&value.value)
                .ok_or_else(|| generic_std_error("Invalid highest CID encoding"))?;
            Ok(highest_value)
        } else {
            return Err(generic_std_error("Failed to get latest message id"));
        }
    }

    /// This functions retrieves the data, deserializes it, and then calls the visitor function with the deserialized data
    /// The visitor function should return the new data to be stored
    async fn visit_database<F: FnOnce(ConnectionPairDatabase) -> ConnectionPairDatabase>(
        &mut self,
        cid: u64,
        peer_cid: Option<u64>,
        visitor: F,
    ) -> std::io::Result<()> {
        let request_id = Uuid::new_v4();
        let key = generate_message_database_key_for_connection_pair(cid, peer_cid);
        let request = InternalServiceRequest::LocalDBGetKV {
            request_id,
            cid,
            peer_cid,
            key: key.clone(),
        };

        let response = self.send_and_wait_for_response(request).await?;

        let mut db = if let InternalServiceResponse::LocalDBGetKVSuccess(value) = response {
            let db = bincode2::deserialize(&value.value).map_err(|err| {
                generic_std_error(format!("Failed to deserialize database: {}", err))
            })?;
            db
        } else {
            ConnectionPairDatabase {
                sent_messages: HashMap::new(),
                received_messages: HashMap::new(),
                greatest_acknowledged_id: None,
                implicated_cid: cid,
                peer_cid,
            }
        };

        db = visitor(db);

        let serialized = bincode2::serialize(&db)
            .map_err(|err| generic_std_error(format!("Failed to serialize database: {}", err)))?;

        let request_id = Uuid::new_v4();
        let request = InternalServiceRequest::LocalDBSetKV {
            request_id,
            cid,
            peer_cid,
            key,
            value: serialized,
        };

        let response = self.send_and_wait_for_response(request).await?;

        if let InternalServiceResponse::LocalDBSetKVSuccess(_) = response {
            Ok(())
        } else {
            Err(generic_std_error("Failed to store database"))
        }
    }

    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
}

fn generate_message_database_key_for_connection_pair(cid: u64, peer_cid: Option<u64>) -> String {
    format!(
        "__internal__cw-workspace-messages-{cid}-{}",
        peer_cid.unwrap_or_default()
    )
}

fn generate_highest_message_id_key_for_cid_received(cid: u64, peer_cid: Option<u64>) -> String {
    format!(
        "__internal__cw-workspace-messages-highest-id-{cid}-{:?}",
        peer_cid.unwrap_or_default()
    )
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
