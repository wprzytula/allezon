use clap::Parser;
use std::net::ToSocketAddrs;
use tracing::log;

mod endpoints;
mod mock;
mod scylla;
#[cfg(test)]
mod tests;
mod types;
mod utils;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    address: String,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    #[arg(short, long, default_value = "127.0.0.1:9042")]
    scylla_uri: String,

    #[arg(short, long, action)]
    mock: bool,
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();
    let args = Args::parse();

    let socket_address = (args.address, args.port)
        .to_socket_addrs()
        .expect("Failed to parse socket address")
        .next()
        .expect("Failed to parse socket address");

    let router: axum::Router;

    if !args.mock {
        router = endpoints::build_router(scylla::Session::new(&args.scylla_uri).await);
        log::info!("Connected to Scylla on {}", args.scylla_uri);
    } else {
        router = endpoints::build_router(mock::System::new());
        log::info!("Starting in mock mode");
    }

    log::info!("Starting server on {}", socket_address);
    let server = axum::Server::bind(&socket_address)
        .serve(router.into_make_service())
        .with_graceful_shutdown(shutdown_signal());
    server.await.unwrap();
}

#[ignore = "Not yet written"]
#[test]
fn unit_test() {
    todo!("This is yet to be thought up")
}

const _ENDPOINT: &str = "localhost:443";

#[ignore = "Not yet written"]
#[tokio::test]
async fn use_case_1() {
    let _client = reqwest::Client::builder().build().unwrap();
}
