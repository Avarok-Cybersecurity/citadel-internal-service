use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub mod kernel;

#[tokio::main]
async fn main() {
    static PORT: u16 = 3000;

    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), PORT);

    let kernel = kernel::CitadelWorkspaceService {
        remote: None,
        bind_address: socket,
    };
}
