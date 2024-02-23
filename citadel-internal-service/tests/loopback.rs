mod common;

#[cfg(test)]
mod tests {
    use crate::common::{
        register_and_connect_to_server, server_info_skip_cert_verification, RegisterAndConnectItems,
    };
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_logging::setup_log;
    use citadel_sdk::prelude::*;

    #[tokio::test]
    async fn test_2_peers_1_service() -> Result<(), Box<dyn std::error::Error>> {
        setup_log();

        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let service_addr = "127.0.0.1:55778".parse().unwrap();
        let service = CitadelWorkspaceService::new(service_addr);

        let internal_service = NodeBuilder::default()
            .with_backend(BackendType::InMemory)
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(service)?;

        tokio::task::spawn(internal_service);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Now with both the server and the IS running, we can test both peers trying to connect, then to each other
        // via p2p
        let mut to_spawn = vec![];
        to_spawn.push(RegisterAndConnectItems {
            internal_service_addr: service_addr,
            server_addr: server_bind_address,
            full_name: format!("Peer 0"),
            username: format!("peer.0"),
            password: format!("secret_0").into_bytes().to_owned(),
        });

        to_spawn.push(RegisterAndConnectItems {
            internal_service_addr: service_addr,
            server_addr: server_bind_address,
            full_name: format!("Peer 1"),
            username: format!("peer.1"),
            password: format!("secret_1").into_bytes().to_owned(),
        });

        let mut returned_service_info = register_and_connect_to_server(to_spawn).await.unwrap();
        let (peer_0_tx, peer_0_rx, peer_0_cid) = returned_service_info.remove(0);
        let (peer_1_tx, peer_1_rx, peer_1_cid) = returned_service_info.remove(0);

        Ok(())
    }
}
