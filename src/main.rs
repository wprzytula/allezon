use clap::Parser;
use std::net::ToSocketAddrs;

mod endpoints;
mod mock;
pub mod types;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    address: String,

    #[arg(short, long, default_value_t = 9042)]
    port: u16,
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let socket_address = (args.address, args.port)
        .to_socket_addrs()
        .expect("Failed to parse socket address")
        .next()
        .expect("Failed to parse socket address");
    println!("Starting server on {}", socket_address);

    let router = endpoints::build_router(mock::System::new());
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
