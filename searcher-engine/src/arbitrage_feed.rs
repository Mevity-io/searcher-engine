use std::pin::Pin;

use tonic::{Request, Response, Status};

use crate::hub::SharedHub;
use crate::pb::{
    arbitrage_stream_server::{ArbitrageStream, ArbitrageStreamServer},
    Empty, ArbitrageTxBatch,
};
use crate::filter_feed::stream as arb_stream;

type OutStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<ArbitrageTxBatch, Status>> + Send + 'static>>;

pub struct FeedSvc { hub: SharedHub }
impl FeedSvc { pub fn new(hub: SharedHub) -> Self { Self { hub } } }

#[tonic::async_trait]
impl ArbitrageStream for FeedSvc {
    type SubscribeArbitrageStream = OutStream;

    async fn subscribe_arbitrage(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<Self::SubscribeArbitrageStream>, Status> {
        // Hub → Zero‑copy Arbitrage stream
        Ok(Response::new(arb_stream::build(self.hub.clone())))
    }
}

pub fn service(hub: SharedHub) -> ArbitrageStreamServer<FeedSvc> {
    ArbitrageStreamServer::new(FeedSvc::new(hub))
}