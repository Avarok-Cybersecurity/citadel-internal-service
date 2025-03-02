use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::register_and_connect_to_server_then_peers;
    use citadel_internal_service_connector::connector::InternalServiceConnector;
    use citadel_internal_service_connector::io_interface::in_memory::InMemoryInterface;
    use citadel_internal_service_connector::io_interface::IOInterface;
    use citadel_internal_service_connector::messenger::backend::CitadelBackendExt;
    use citadel_internal_service_connector::messenger::backend::CitadelWorkspaceBackend;
    use citadel_internal_service_connector::messenger::{CitadelWorkspaceMessenger, MessengerTx};
    use citadel_internal_service_test_common::PeerServiceHandles;
    use citadel_internal_service_types::{InternalServiceRequest, InternalServiceResponse};
    use citadel_sdk::prelude::StackedRatchet;
    use futures::{SinkExt, StreamExt};
    use std::error::Error;
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::ops::DerefMut;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::Mutex;
    use tokio::time;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_connector_mapping() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
                vec![
                    bind_address_internal_service_a,
                    bind_address_internal_service_b,
                ],
                None,
                None,
            )
            .await?;

        let (to_service_a, from_service_a, cid_a) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_b, from_service_b, cid_b) =
            peer_return_handle_vec.take_next_service_handle();

        let io = InMemoryInterface::from_request_response_pair(to_service_a, from_service_a);
        let mut connector_a = InternalServiceConnector::from_io(io).await.ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotConnected,
                "Unable to create in memory interface",
            )
        })?;

        let io = InMemoryInterface::from_request_response_pair(to_service_b, from_service_b);
        let mut connector_b = InternalServiceConnector::from_io(io).await.ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotConnected,
                "Unable to create in memory interface",
            )
        })?;

        test_get_sessions_connector(&mut connector_a, 1, cid_a).await?;
        test_get_sessions_connector(&mut connector_b, 1, cid_b).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_messenger_requests_and_session_state() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
                vec![
                    bind_address_internal_service_a,
                    bind_address_internal_service_b,
                ],
                None,
                None,
            )
            .await?;

        let (to_service_a, from_service_a, cid_a) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_b, from_service_b, cid_b) =
            peer_return_handle_vec.take_next_service_handle();

        let (messenger_a, mut rx_a) = get_messenger(to_service_a, from_service_a).await?;
        let (messenger_b, mut rx_b) = get_messenger(to_service_b, from_service_b).await?;

        let tx_a = messenger_a.multiplex(cid_a).await?;
        let tx_b = messenger_b.multiplex(cid_b).await?;

        assert_eq!(tx_a.local_cid(), cid_a);
        assert_eq!(tx_b.local_cid(), cid_b);

        test_get_sessions_messenger_get_sessions(&tx_a, &mut rx_a, 1, cid_a).await?;
        test_get_sessions_messenger_get_sessions(&tx_b, &mut rx_b, 1, cid_b).await?;

        assert_eq!(tx_a.get_connected_peers().await, vec![cid_b]);
        assert_eq!(tx_b.get_connected_peers().await, vec![cid_a]);

        Ok(())
    }

    #[tokio::test]
    /// Have every client connect to every other client and send messages via a ping/pong test to every other client
    async fn test_messenger_messaging() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        // internal service for peer C
        let bind_address_internal_service_c: SocketAddr = "127.0.0.1:55538".parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
                vec![
                    bind_address_internal_service_a,
                    bind_address_internal_service_b,
                    bind_address_internal_service_c,
                ],
                None,
                None,
            )
            .await?;

        let (to_service_a, from_service_a, cid_a) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_b, from_service_b, cid_b) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_c, from_service_c, cid_c) =
            peer_return_handle_vec.take_next_service_handle();

        let (messenger_a, rx_a) = get_messenger(to_service_a, from_service_a).await?;
        let (messenger_b, rx_b) = get_messenger(to_service_b, from_service_b).await?;
        let (messenger_c, rx_c) = get_messenger(to_service_c, from_service_c).await?;

        let tx_a = messenger_a.multiplex(cid_a).await?;
        let tx_b = messenger_b.multiplex(cid_b).await?;
        let tx_c = messenger_c.multiplex(cid_c).await?;

        // Wrap each element in a mutex to allow concurrent access
        let txs = [
            Mutex::new((tx_a, rx_a)),
            Mutex::new((tx_b, rx_b)),
            Mutex::new((tx_c, rx_c)),
        ];
        let clients = [cid_a, cid_b, cid_c];

        assert_eq!(txs.len(), clients.len());

        // Run test_ping_ping between every pair of clients
        // Do NOT wait for connection in messaging layer to
        // prove enqueueing works
        for i in 0..txs.len() {
            for j in 0..txs.len() {
                if i != j {
                    for _ in 0..10 {
                        let mut i_locked = txs[i].lock().await;
                        let mut j_locked = txs[j].lock().await;
                        let (tx_0, rx_0) = i_locked.deref_mut();
                        let (tx_1, rx_1) = j_locked.deref_mut();

                        test_ping_pong(tx_0, rx_0, tx_1, rx_1).await?;
                    }
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_backend_ping_pong() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55636".parse().unwrap();
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55637".parse().unwrap();

        let mut peer_return_handle_vec =
            register_and_connect_to_server_then_peers::<StackedRatchet>(
                vec![
                    bind_address_internal_service_a,
                    bind_address_internal_service_b,
                ],
                None,
                None,
            )
            .await?;

        let (to_service_a, from_service_a, cid_a) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_b, from_service_b, cid_b) =
            peer_return_handle_vec.take_next_service_handle();

        let io_a = InMemoryInterface::from_request_response_pair(to_service_a, from_service_a);
        let io_b = InMemoryInterface::from_request_response_pair(to_service_b, from_service_b);

        let connector_a = InternalServiceConnector::from_io(io_a)
            .await
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::NotConnected,
                    "Unable to create in memory interface",
                )
            })?;

        let connector_b = InternalServiceConnector::from_io(io_b)
            .await
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::NotConnected,
                    "Unable to create in memory interface",
                )
            })?;

        let (messenger_a, mut rx_a) =
            CitadelWorkspaceMessenger::<CitadelWorkspaceBackend>::new(connector_a);
        let (messenger_b, mut rx_b) =
            CitadelWorkspaceMessenger::<CitadelWorkspaceBackend>::new(connector_b);

        let tx_a = messenger_a.multiplex(cid_a).await?;
        let tx_b = messenger_b.multiplex(cid_b).await?;

        let _backend_a = CitadelWorkspaceBackend::new(cid_a, &tx_a).await.unwrap();
        let _backend_b = CitadelWorkspaceBackend::new(cid_b, &tx_b).await.unwrap();

        assert_eq!(tx_a.local_cid(), cid_a);
        assert_eq!(tx_b.local_cid(), cid_b);

        test_get_sessions_messenger_get_sessions(&tx_a, &mut rx_a, 1, cid_a).await?;
        test_get_sessions_messenger_get_sessions(&tx_b, &mut rx_b, 1, cid_b).await?;

        assert_eq!(tx_a.get_connected_peers().await, vec![cid_b]);
        assert_eq!(tx_b.get_connected_peers().await, vec![cid_a]);

        // Only run the ping-pong test once instead of 10 times to avoid potential infinite loops
        let timeout_result = time::timeout(
            std::time::Duration::from_secs(5),
            test_ping_pong(&tx_a, &mut rx_a, &tx_b, &mut rx_b),
        )
        .await;

        match timeout_result {
            Ok(result) => result?,
            Err(_) => return Err("Ping-pong test timed out after 5 seconds".into()),
        }

        Ok(())
    }

    async fn test_ping_pong<B>(
        tx_a: &MessengerTx<B>,
        rx_a: &mut UnboundedReceiver<InternalServiceResponse>,
        tx_b: &MessengerTx<B>,
        rx_b: &mut UnboundedReceiver<InternalServiceResponse>,
    ) -> Result<(), Box<dyn Error>>
    where
        B: CitadelBackendExt,
    {
        let cid_a = tx_a.local_cid();
        let cid_b = tx_b.local_cid();

        let ping = b"Ping" as &[u8];
        let pong = b"Pong" as &[u8];

        let task_a = async move {
            tx_a.send_message_to(cid_b, ping)
                .await
                .expect("Failed to send message");
            let resp = rx_b
                .recv()
                .await
                .expect("Expected a message from the internal server");

            if let InternalServiceResponse::MessageNotification(message) = &resp {
                assert_eq!(message.cid, cid_b);
                assert_eq!(message.peer_cid, cid_a);
                assert_eq!(message.message, ping);
            } else {
                panic!("Expected a MessageNotification, got: {resp:?}");
            }
        };

        let task_b = async move {
            tx_b.send_message_to(cid_a, pong)
                .await
                .expect("Failed to send message");
            let resp = rx_a
                .recv()
                .await
                .expect("Expected a message from the internal server");
            if let InternalServiceResponse::MessageNotification(message) = &resp {
                assert_eq!(message.cid, cid_a);
                assert_eq!(message.peer_cid, cid_b);
                assert_eq!(message.message, pong);
            } else {
                panic!("Expected a MessageNotification, got: {resp:?}");
            }
        };

        // Send messages concurrently
        tokio::join!(task_a, task_b);

        Ok(())
    }

    async fn test_get_sessions_messenger_get_sessions<B>(
        tx: &MessengerTx<B>,
        rx: &mut UnboundedReceiver<InternalServiceResponse>,
        sess_count: usize,
        cid: u64,
    ) -> Result<(), Box<dyn Error>>
    where
        B: CitadelBackendExt,
    {
        let request = InternalServiceRequest::GetSessions {
            request_id: Uuid::new_v4(),
        };

        let inspector = |response| {
            if let InternalServiceResponse::GetSessionsResponse(response) = &response {
                assert_eq!(response.sessions.len(), sess_count);
                assert!(response.sessions.iter().any(|r| r.cid == cid));
            } else {
                panic!("Expected a GetSessionsResponse, got: {response:?}");
            }
        };

        test_get_sessions_messenger_request_response(tx, rx, request, inspector).await?;

        Ok(())
    }

    async fn test_get_sessions_messenger_request_response<B, F>(
        tx: &MessengerTx<B>,
        rx: &mut UnboundedReceiver<InternalServiceResponse>,
        request: InternalServiceRequest,
        response_inspector: F,
    ) -> Result<(), Box<dyn Error>>
    where
        B: CitadelBackendExt,
        F: FnOnce(InternalServiceResponse),
    {
        tx.send_request(request).await?;

        let response = rx
            .recv()
            .await
            .expect("Expected a response from the internal server");
        response_inspector(response);

        Ok(())
    }

    async fn test_get_sessions_connector<T: IOInterface>(
        connector: &mut InternalServiceConnector<T>,
        sess_count: usize,
        cid: u64,
    ) -> Result<(), Box<dyn Error>> {
        connector
            .sink
            .send(InternalServiceRequest::GetSessions {
                request_id: Uuid::new_v4(),
            })
            .await?;
        let response = connector
            .stream
            .next()
            .await
            .expect("Expected a response from the internal server");
        if let InternalServiceResponse::GetSessionsResponse(response) = response {
            assert_eq!(response.sessions.len(), sess_count);
            assert!(response.sessions.iter().any(|r| r.cid == cid));
        } else {
            panic!("Expected a GetSessionsResponse");
        }

        Ok(())
    }

    async fn get_messenger(
        to_service: tokio::sync::mpsc::UnboundedSender<InternalServiceRequest>,
        from_service: tokio::sync::mpsc::UnboundedReceiver<InternalServiceResponse>,
    ) -> Result<
        (
            CitadelWorkspaceMessenger<CitadelWorkspaceBackend>,
            UnboundedReceiver<InternalServiceResponse>,
        ),
        Box<dyn Error>,
    > {
        let io = InMemoryInterface::from_request_response_pair(to_service, from_service);
        let connector = InternalServiceConnector::from_io(io).await.ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotConnected,
                "Unable to create in memory interface",
            )
        })?;
        let (messenger, rx) = CitadelWorkspaceMessenger::new(connector);
        Ok((messenger, rx))
    }
}
