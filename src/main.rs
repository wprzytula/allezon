use std::net::SocketAddr;

mod endpoints;
mod mock;
pub mod types;

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

#[tokio::main]
async fn main() {
    let router = endpoints::build_router(mock::System::new());
    let server = axum::Server::bind(&SocketAddr::from(([127, 0, 0, 5], 9042)))
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
