use citadel_internal_service::kernel::CitadelWorkspaceService;
use citadel_sdk::prelude::{BackendType, NodeBuilder, NodeType};
use std::error::Error;
use std::net::SocketAddr;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    citadel_logging::setup_log();
    let opts: Options = Options::from_args();
    let service = CitadelWorkspaceService::new_tcp(opts.bind).await?;

    let mut builder = NodeBuilder::default();
    let mut builder = builder
        .with_backend(BackendType::InMemory) // TODO: parameterize this in the opts
        .with_node_type(NodeType::Peer);

    if opts.dangerous.unwrap_or(false) {
        builder = builder.with_insecure_skip_cert_verification()
    }

    builder.build(service)?.await?;

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
    #[structopt(short, long)]
    dangerous: Option<bool>,
}
