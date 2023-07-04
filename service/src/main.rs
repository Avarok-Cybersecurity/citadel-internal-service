use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType};
use citadel_workspace_service::kernel::CitadelWorkspaceService;
use std::error::Error;
use std::net::SocketAddr;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    citadel_logging::setup_log();
    let opts: Options = Options::from_args();
    let service = CitadelWorkspaceService::new(opts.bind);
    NodeBuilder::default()
        .with_backend(BackendType::InMemory) // TODO: parameterize this in the opts
        .with_node_type(NodeType::Peer) // We will only use the service to create outbound protocol connections
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
