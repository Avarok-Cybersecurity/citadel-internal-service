use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
pub mod kernel;

#[tokio::main]
async fn main() {
    static PORT: u16 = 3000;

    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), PORT);

    let kernel = kernel::CitadelWorkspaceService::new(socket);

    let server = NodeBuilder::default()
        .with_node_type(NodeType::Peer)
        .with_backend(BackendType::InMemory)
        .build(kernel);

    let _res = server.unwrap().await;
}
