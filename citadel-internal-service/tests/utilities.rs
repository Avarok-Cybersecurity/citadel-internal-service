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
        let _result =
            connector_service_and_server(SocketAddr::from_str("127.0.0.1:23457")?).await?;

        Ok(())
    }

    async fn connector_service_and_server(
        addr: SocketAddr,
    ) -> Result<InternalServiceConnector, Box<dyn Error>> {
        let (server, server_bind_address) = server_info_skip_cert_verification();
        tokio::task::spawn(server);

        let internal_service_kernel = CitadelWorkspaceService::new(addr.clone());
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_backend(BackendType::Filesystem("filesystem".into()))
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)
            .unwrap();
        tokio::task::spawn(internal_service);

        // Connect to Internal Service via TCP
        let mut service_connector = InternalServiceConnector::connect_to_service(addr).await?;

        service_connector
            .register_and_connect(
                server_bind_address,
                "Full Name",
                "myusername",
                SecBuffer::from("password"),
                Default::default(),
            )
            .await?;
        Ok(service_connector)
    }
}
