use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

pub mod kernel;

#[tokio::main]
async fn main() {
    static PORT: u16 = 3000;

    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), PORT);

    let _kernel = kernel::CitadelWorkspaceService {
        remote: None,
        bind_address: socket,
        server_connection_map: Arc::new(parking_lot::Mutex::new(Default::default())),
    };
}
