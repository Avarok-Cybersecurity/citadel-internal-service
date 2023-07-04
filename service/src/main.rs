use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType};
use citadel_workspace_service::kernel::CitadelWorkspaceService;
use std::error::Error;
use std::net::SocketAddr;
use structopt::Opts;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opts: Options = Opts::from_args();
    let service = CitadelWorkspaceService::new(opts.bind);
    NodeBuilder::default()
        .with_backend(BackendType::InMemory) // TODO: parameterize this in the opts
        .with_node_type(NodeType::Peer) // We will only use the service to create outbound protocol connections
        .build(service)?
        .await?;

    Ok(())
}

struct Options {
    #[structopt(short, long)]
    bind: SocketAddr,
}
