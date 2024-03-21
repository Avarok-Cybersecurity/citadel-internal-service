mod common;

#[cfg(test)]
mod tests {
    use crate::common::server_info_skip_cert_verification;
    use citadel_internal_service::kernel::CitadelWorkspaceService;
    use citadel_internal_service_connector::connector::InternalServiceConnector;
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
            CitadelWorkspaceService::new(SocketAddr::from_str("127.0.0.1:23457")?);
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
        let _service_connector_0 = connector_service_and_server(
            server_bind_address,
            SocketAddr::from_str("127.0.0.1:23457")?,
            "name 0",
            "username0",
            "password0",
        )
        .await?;
        let _service_connector_1 = connector_service_and_server(
            server_bind_address,
            SocketAddr::from_str("127.0.0.1:23458")?,
            "name 1",
            "username1",
            "password1",
        )
        .await?;
        // TODO: Add Peer Register and Connect - Requires CID from Responses above
        Ok(())
    }

    async fn connector_service_and_server<S: Into<String>, R: Into<SecBuffer>>(
        server_addr: SocketAddr,
        service_addr: SocketAddr,
        full_name: S,
        username: S,
        password: R,
    ) -> Result<InternalServiceConnector, Box<dyn Error>> {
        let internal_service_kernel = CitadelWorkspaceService::new(service_addr);
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
        service_connector
            .register_and_connect(
                server_addr,
                full_name,
                username,
                password,
                Default::default(),
            )
            .await?;
        Ok(service_connector)
    }
}
