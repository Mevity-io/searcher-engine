// searcher_engine/src/block_engine.rs
use tonic::{Request, Response, Status};
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use std::pin::Pin;

use jito_protos::bundle::{BundleUuid};
use crate::pb;
use pb::block_engine_stream_server::{BlockEngineStream, BlockEngineStreamServer};
use pb::Empty;
use crate::{hub::SharedHub};

type BundleStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<BundleUuid, Status>> + Send + 'static>>;

pub struct BlockEngineSvc { hub: SharedHub }
impl BlockEngineSvc { pub fn new(hub: SharedHub) -> Self { Self { hub } } }

#[tonic::async_trait]
impl BlockEngineStream for BlockEngineSvc {
    type SubscribeBundlesStream = BundleStream;

    async fn subscribe_bundles(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        if let Some(peer) = _req.remote_addr() {
            log::debug!("🔌 BlockEngine subscribed bundles from {peer}");
        } else {
            log::debug!("🔌 BlockEngine subscribed bundles");
        }

        // Hub → Bundle → BundleUuid 변환
        let rx = self.hub.subscribe_bundles();
        let st = ReceiverStream::new(rx).map(|b: BundleUuid| {
            log::debug!("💠 BlockEngine.subscribe_bundles – uuid={}", b.uuid);
            common_utils::metrics::SEARCHER_BUNDLE_OUT_TOTAL.inc();
            Ok(b)
        });

        Ok(Response::new(Box::pin(st) as BundleStream))
    }
}

pub fn service(hub: SharedHub) -> BlockEngineStreamServer<BlockEngineSvc> {
    BlockEngineStreamServer::new(BlockEngineSvc::new(hub))
}