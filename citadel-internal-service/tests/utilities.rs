mod common;

#[cfg(test)]
mod tests {
    use crate::common::server_info_skip_cert_verification;
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_connector::connector::{ClientError, InternalServiceConnector};
    use citadel_internal_service_connector::io_interface::tcp::TcpIOInterface;
    use citadel_internal_service_connector::scan_for_response;
    use citadel_internal_service_types::InternalServiceResponse::PeerConnectSuccess;
    use citadel_internal_service_types::{ConnectSuccess, InternalServiceResponse};
    use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType, SecBuffer};
    use std::error::Error;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_utilities_service_and_server() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);
        let _result = connector_service_and_server(
            server_bind_address,
            SocketAddr::from_str("127.0.0.1:23457")?,
            "my name",
            "myusername",
            "password",
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_utilities_register_and_connect_methods() -> Result<(), Box<dyn Error>> {
        // Setup Logging and Start Server
        crate::common::setup_log();
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        // Start Internal Service
        let internal_service_kernel =
            CitadelWorkspaceService::new_tcp(SocketAddr::from_str("127.0.0.1:23457")?).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)
            .unwrap();
        tokio::task::spawn(internal_service);

        let (full_name, username, password) =
            ("full name", "myusername", SecBuffer::from("password"));

        // Connect to Internal Service via TCP
        let mut service_connector =
            InternalServiceConnector::connect_to_service(SocketAddr::from_str("127.0.0.1:23457")?)
                .await?;
        // Register to Server
        service_connector
            .register_with_defaults(server_bind_address, full_name, username, password.clone())
            .await?;
        // Connect to Server
        service_connector
            .connect_with_defaults(username, password)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_utilities_peer_register_and_connect() -> Result<(), Box<dyn Error>> {
        crate::common::setup_log();
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);
        let (mut service_connector_0, cid_0) = connector_service_and_server(
            server_bind_address,
            SocketAddr::from_str("127.0.0.1:23457")?,
            "name 0",
            "username0",
            "password0",
        )
        .await?;
        let (mut service_connector_1, cid_1) = connector_service_and_server(
            server_bind_address,
            SocketAddr::from_str("127.0.0.1:23458")?,
            "name 1",
            "username1",
            "password1",
        )
        .await?;

        let peer_0_register_and_connect = tokio::task::spawn(async move {
            service_connector_0
                .peer_register_and_connect_with_defaults(cid_0, cid_1)
                .await
        });
        let peer_1_register_and_connect = tokio::task::spawn(async move {
            service_connector_1
                .peer_register_and_connect_with_defaults(cid_1, cid_0)
                .await
        });
        let result =
            futures::future::join_all([peer_0_register_and_connect, peer_1_register_and_connect])
                .await;
        if result.iter().all(|i| i.is_ok()) {
            citadel_logging::info!(target: "citadel", "Peers Registration and Connection to each other was successful");
        } else {
            panic!("Peer Register and Connect Error")
        }
        // service_connector_0.peer_register_with_defaults(cid_0, cid_1).await?;
        // service_connector_1.peer_register_with_defaults(cid_1, cid_0).await?;
        //
        // service_connector_0.peer_connect_with_defaults(cid_0, cid_1).await?;
        // service_connector_1.peer_connect_with_defaults(cid_1, cid_0).await?;
        Ok(())
    }

    async fn connector_service_and_server<S: Into<String>, R: Into<SecBuffer>>(
        server_addr: SocketAddr,
        service_addr: SocketAddr,
        full_name: S,
        username: S,
        password: R,
    ) -> Result<(InternalServiceConnector<TcpIOInterface>, u64), Box<dyn Error>> {
        let internal_service_kernel = CitadelWorkspaceService::new_tcp(service_addr).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::InMemory)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)
            .unwrap();
        tokio::task::spawn(internal_service);

        // Connect to Internal Service via TCP
        let mut service_connector =
            InternalServiceConnector::connect_to_service(service_addr).await?;
        match service_connector
            .register_and_connect(
                server_addr,
                full_name,
                username,
                password,
                Default::default(),
            )
            .await
        {
            Ok(ConnectSuccess { cid, request_id: _ }) => Ok((service_connector, cid)),
            Err(err) => Err(Box::from(err)),
        }
    }
}
