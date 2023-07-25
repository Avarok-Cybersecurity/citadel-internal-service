use citadel_sdk::prefabs::server::empty::EmptyKernel;
use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType};
use std::error::Error;
use std::net::SocketAddr;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    citadel_logging::setup_log();
    let opts: Options = Options::from_args();
    let service = EmptyKernel;
    NodeBuilder::default()
        .with_backend(BackendType::InMemory)
        .with_node_type(NodeType::server(opts.bind)?)
        .build(service)?
        .await?;

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "citadel-service-bin",
    about = "Used for running a local service for citadel applications"
)]
struct Options {
    #[structopt(short, long)]
    bind: SocketAddr,
}
