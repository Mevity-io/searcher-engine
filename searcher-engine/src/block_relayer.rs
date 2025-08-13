// searcher-engine/src/block_relayer.rs
use std::{pin::Pin, time::Duration};

use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use jito_protos::block_engine::{
    block_engine_relayer_server::{BlockEngineRelayer, BlockEngineRelayerServer},
    packet_batch_update,
    AccountsOfInterestRequest, AccountsOfInterestUpdate,
    ProgramsOfInterestRequest, ProgramsOfInterestUpdate,
    PacketBatchUpdate, StartExpiringPacketStreamResponse,
};

use crate::hub::SharedHub;

/* -------------------------------------------------------------------- */
/*                               Service                                */
/* -------------------------------------------------------------------- */
pub struct RelayerStub {
    hub: SharedHub,
}
impl RelayerStub {
    pub fn new(hub: SharedHub) -> Self {
        Self { hub }
    }
}

/* ---------- Stream type aliases ---------- */
type PacketHbStream =
    Pin<Box<dyn Stream<Item = Result<StartExpiringPacketStreamResponse, Status>> + Send>>;
type AoiStream =
    Pin<Box<dyn Stream<Item = Result<AccountsOfInterestUpdate, Status>> + Send>>;
type PoiStream =
    Pin<Box<dyn Stream<Item = Result<ProgramsOfInterestUpdate, Status>> + Send>>;

/* -------------------------------------------------------------------- */
/*                          Trait implementation                         */
/* -------------------------------------------------------------------- */
#[tonic::async_trait]
impl BlockEngineRelayer for RelayerStub {
    /* ---- (A) bi‑directional ExpiringPacketStream ---- */
    type StartExpiringPacketStreamStream = PacketHbStream;

    async fn start_expiring_packet_stream(
        &self,
        req: Request<tonic::Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        let peer = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".into());
        log::info!("🔌 RELAYER connected from {peer}");

        /* inbound gRPC 스트림 */
        let mut inbound = req.into_inner();
        let hub = self.hub.clone();

        /* ───────────────────────── inbound → Hub │ fan‑out ───────────────────────── */
        tokio::spawn(async move {
            while let Some(update) = inbound.next().await {
                match update {
                    Ok(pkt_upd) => match pkt_upd.msg {
                        Some(packet_batch_update::Msg::Batches(b)) => {
                            if let Some(batch) = b.batch {
                                let n0 = batch.packets.len();
                                let filtered = crate::blacklist::filter_batch(batch);
                                let n1 = filtered.packets.len();

                                hub.publish_local_packet(filtered);
                                log::trace!("➡️  RELAYER → Hub  ({} pkt → {} pkt)", n0, n1);
                            }
                        }
                        Some(packet_batch_update::Msg::Heartbeat(_)) => {
                            log::trace!("💓 RELAYER heartbeat");
                        }
                        None => {
                            log::warn!("⚠️  PacketBatchUpdate.msg == None");
                        }
                    },
                    Err(e) => {
                        log::error!("🛑 gRPC inbound stream error: {e}");
                        break;
                    }
                }
            }
            log::warn!("🛑 gRPC inbound stream closed for {peer}");
        });

        /* ────────────────────── outbound: 1 s heartbeat stream ───────────────────── */
        let (tx, rx) = mpsc::channel::<StartExpiringPacketStreamResponse>(4);
        tokio::spawn(async move {
            let mut intv = tokio::time::interval(Duration::from_secs(1));
            loop {
                if tx.send(StartExpiringPacketStreamResponse { heartbeat: None }).await.is_err() {
                    break;
                }
                intv.tick().await;
            }
        });

        let out_stream = ReceiverStream::new(rx).map(Ok::<_, Status>);
        Ok(Response::new(Box::pin(out_stream) as PacketHbStream))
    }

    type SubscribeAccountsOfInterestStream = AoiStream;
    async fn subscribe_accounts_of_interest(
        &self,
        _req: Request<AccountsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeAccountsOfInterestStream>, Status> {
        Err(Status::unimplemented("AOI stream not supported in stub"))
    }

    type SubscribeProgramsOfInterestStream = PoiStream;
    async fn subscribe_programs_of_interest(
        &self,
        _req: Request<ProgramsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeProgramsOfInterestStream>, Status> {
        Err(Status::unimplemented("POI stream not supported in stub"))
    }
}

/* -------------------------------------------------------------------- */
/*                    re‑export gRPC server constructor                  */
/* -------------------------------------------------------------------- */
pub type RelayerStubServer = BlockEngineRelayerServer<RelayerStub>;
