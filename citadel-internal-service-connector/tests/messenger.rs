use citadel_internal_service_test_common as common;

#[cfg(test)]
mod tests {
    use crate::common::register_and_connect_to_server_then_peers;
    use citadel_internal_service_connector::connector::InternalServiceConnector;
    use citadel_internal_service_connector::io_interface::tcp::TcpIOInterface;
    use citadel_internal_service_connector::messenger::{
        CitadelWorkspaceMessenger, WrappedMessage,
    };
    use citadel_internal_service_test_common::PeerServiceHandles;
    use citadel_internal_service_types::InternalServiceResponse;
    use intersession_layer_messaging::testing::InMemoryBackend;
    use std::error::Error;
    use std::net::SocketAddr;
    use tokio::sync::mpsc::UnboundedReceiver;

    #[tokio::test]
    async fn test_messenger() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        // internal service for peer C
        let bind_address_internal_service_c: SocketAddr = "127.0.0.1:55538".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(
            vec![
                bind_address_internal_service_a,
                bind_address_internal_service_b,
                bind_address_internal_service_c,
            ],
            None,
            None,
        )
        .await?;

        let (to_service_a, mut from_service_a, cid_a) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_b, mut from_service_b, cid_b) =
            peer_return_handle_vec.take_next_service_handle();
        let (to_service_c, mut from_service_c, cid_c) =
            peer_return_handle_vec.take_next_service_handle();

        let (messenger_a, rx_a) = get_messenger(bind_address_internal_service_a).await?;
        let (messenger_b, rx_b) = get_messenger(bind_address_internal_service_b).await?;
        let (messenger_c, rx_c) = get_messenger(bind_address_internal_service_c).await?;

        let tx_a = messenger_a.multiplex(cid_a);
        let tx_b = messenger_b.multiplex(cid_b);
        let tx_c = messenger_c.multiplex(cid_c);

        Ok(())
    }

    async fn get_messenger(
        internal_service_addr: SocketAddr,
    ) -> Result<
        (
            CitadelWorkspaceMessenger<InMemoryBackend<WrappedMessage>>,
            UnboundedReceiver<InternalServiceResponse>,
        ),
        Box<dyn Error>,
    > {
        let connector = InternalServiceConnector::connect(internal_service_addr).await?;
        let backend = InMemoryBackend::default();
        let (messenger, rx) = CitadelWorkspaceMessenger::new(connector, backend).await?;
        Ok((messenger, rx))
    }
}
