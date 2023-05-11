pub mod kernel;

#[tokio::main]
async fn main() {
    let kernel = kernel::CitadelWorkspaceService;
}
