use tonic::{Request, Response, Status};
use crate::interregion::inter_region_server::{InterRegion, InterRegionServer};
use crate::interregion::{PacketWrapper};
use crate::{hub::SharedHub, detect::LOCAL_REGION};
use crate::interregion::Empty;

pub struct InterSvc { hub: SharedHub }
impl InterSvc { pub fn new(hub: SharedHub) -> Self { Self { hub } } }

#[tonic::async_trait]
impl InterRegion for InterSvc {
    async fn send_packet(
        &self,
        req: Request<PacketWrapper>,
    ) -> Result<Response<Empty>, Status> {
        let msg = req.into_inner();

        if msg.src_region == *LOCAL_REGION { return Ok(Response::new(Default::default())); }
        if let Some(batch) = msg.batch {
            self.hub.publish_remote_packet(batch);
        }
        Ok(Response::new(Default::default()))
    }
    async fn send_bundle(
        &self,
        req: Request<jito_protos::bundle::BundleUuid>,
    ) -> Result<Response<Empty>, Status> {
        self.hub.publish_bundle(req.into_inner());  // validator 로 즉시 fan‑out
        Ok(Response::new(Default::default()))
    }
}
pub fn service(hub: SharedHub) -> InterRegionServer<InterSvc> {
    InterRegionServer::new(InterSvc::new(hub))
}