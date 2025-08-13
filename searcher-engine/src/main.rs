use std::{env, net::SocketAddr};

use dotenvy::dotenv;
use env_logger::Env;
use log::info;
use tonic::transport::Server;
use common_utils::logging;

use searcher_engine::{
    hub::Hub,
    block_relayer,
    block_engine,
    searcher,
    inter_region,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    env::set_var("RUST_LOG", env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()));
    env_logger::Builder::from_env(Env::default().default_filter_or("debug"))
        .format_timestamp_nanos()
        .init();
    logging::init();

    tokio::spawn(common_utils::metrics::serve(([0,0,0,0], 8300)));

    let addr: SocketAddr = env::var("SEARCHER_ENGINE_GRPC")
        .unwrap_or_else(|_| "0.0.0.0:50052".into())
        .parse()?;

    let hub = std::sync::Arc::new(Hub::new());

    info!("🚀 searcher‑engine listening on {addr}");

    Server::builder()
        .add_service(searcher::rpc_service(hub.clone()))
        .add_service(block_engine::service(hub.clone()))
        .add_service(block_relayer::RelayerStubServer::new(
            block_relayer::RelayerStub::new(hub.clone()),
        ))
        .add_service(inter_region::service(hub.clone()))
        .add_service(searcher_engine::arbitrage_feed::service(hub.clone()))
        .serve(addr)
        .await?;

    Ok(())
}