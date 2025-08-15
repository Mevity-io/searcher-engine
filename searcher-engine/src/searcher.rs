use std::pin::Pin;
use std::sync::Arc;
use jito_protos::{bundle::BundleUuid, shared::Header};
use prost_types::Timestamp;

use common_utils::logging::{log_to_clickhouse, ClickhouseLog};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{hub::SharedHub, utils::gen_bundle_uuid};

use futures_util::{stream, StreamExt};
use redis::AsyncCommands;      
use jito_protos::{
    bundle::BundleResult,
    searcher::{
        searcher_service_server::{SearcherService, SearcherServiceServer},
        ConnectedLeadersRegionedRequest, ConnectedLeadersRegionedResponse,
        ConnectedLeadersRequest, ConnectedLeadersResponse,
        GetRegionsRequest, GetRegionsResponse, GetTipAccountsRequest,
        GetTipAccountsResponse, NextScheduledLeaderRequest,
        NextScheduledLeaderResponse, PendingTxNotification,
        PendingTxSubscriptionRequest, SendBundleRequest,
        SendBundleResponse, SubscribeBundleResultsRequest,
    },
};

use crate::peer_manager::PEER_MGR;
use crate::detect::LOCAL_REGION;

pub struct SearcherRpcSvc {
    redis_url: String,
    hub: SharedHub
}

impl SearcherRpcSvc {
    pub fn new(h: SharedHub) -> Self {
        let redis_url =
            std::env::var("REDIS_SERVER").unwrap_or_else(|_| "redis://127.0.0.1/".into());
        Self { redis_url: redis_url, hub: h }
    }

    async fn read_redis<T: redis::FromRedisValue>(&self, key: &str) -> Result<Option<T>, Status> {
        let cli  = redis::Client::open(self.redis_url.clone())
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut con = cli
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        con.get(key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }
}

type PendingStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<PendingTxNotification, Status>> + Send>>;
type EmptyBdlStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<BundleResult, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl SearcherService for SearcherRpcSvc {
    type SubscribePendingTransactionsStream = PendingStream;
    type SubscribeBundleResultsStream       = EmptyBdlStream;

    async fn send_bundle(
        &self,
        req: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        /* ── 0. IP Record ───────────────────────────────────────── */
        let remote_addr = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".into());

        /* ── 1. UUID  ───────────────────────────────── */
        let bundle_id = gen_bundle_uuid();

        /* ── 2-1. Blacklist + extract TX sigs ───────────────────── */
        let bundle = req.into_inner();

        if let Some(inner) = bundle.clone().bundle {
            if crate::blacklist::bundle_has_blacklisted(&inner.packets) {
                let bdl = bundle_id.clone();
                tokio::spawn(async move {
                    if let Ok(mut conn) = crate::hub::get_redis_conn().await {
                        let _ = conn.set_ex::<_, _, ()>(
                            format!("{}_status", bdl),
                            "dropped_blacklist", 6*60*60
                        ).await;
                    }
                });
                // common_utils::metrics::SEARCHER_BUNDLE_DROP_TOTAL.inc();
                return Ok(Response::new(SendBundleResponse { uuid: bundle_id }));
            }
        }

        // get tx sigs
        let mut tx_sigs = Vec::with_capacity(bundle.bundle.as_ref().map_or(0, |b| b.packets.len()));
        if let Some(inner) = bundle.bundle.as_ref() {
            for pkt in &inner.packets {
                if let Some(sig) = crate::tx_fast::fast_first_signature_b58(&pkt.data) {
                    if !sig.is_empty() {
                        tx_sigs.push(sig);
                    }
                }
            }
        }

        /* ── 2-2. Detect Front-run ───────────────────── */
        if !tx_sigs.is_empty() {
            if let Ok(mut conn) = crate::hub::get_redis_conn().await {
                // EXISTS batch pipeline
                let mut pipe = redis::pipe();
                for sig in &tx_sigs {
                    pipe.cmd("EXISTS").arg(sig);
                }

                let exists_vec: Vec<i32> = pipe
                    .query_async(&mut conn)
                    .await
                    .unwrap_or_else(|e| { log::warn!("redis EXISTS pipeline failed: {e}"); vec![0; tx_sigs.len()] });

                // Front-run detection
                let mut seen_searcher_tx = false;
                for (sig, ex) in tx_sigs.iter().zip(exists_vec.into_iter()) {
                    let exists = ex != 0;
                    if exists {
                        if seen_searcher_tx {
                            log::warn!("🔍 Detected Front-run: mempool TX {sig} is preceding");
                            // ClickHouse 로그 (비동기 큐)
                            log_to_clickhouse(ClickhouseLog {
                                bundle_id: bundle_id.clone(),
                                tx_sig: sig.clone(),
                                stage: "SEARCHER.bundle_dropped".into(),
                                node: "searcher".into(),
                                from_addr: remote_addr.clone(),
                                description: "front-running detected → dropped".into(),
                                ..Default::default()
                            });
                            // Redis 상태 업데이트는 fire-and-forget
                            let bdl = bundle_id.clone();
                            tokio::spawn(async move {
                                if let Ok(mut c2) = crate::hub::get_redis_conn().await {
                                    let _ = c2.set_ex::<_, _, ()>(format!("{bdl}_status"), "dropped", 6*60*60).await;
                                }
                            });
                            common_utils::metrics::SEARCHER_BUNDLE_DROP_TOTAL.inc();
                            return Ok(Response::new(SendBundleResponse { uuid: bundle_id }));
                        }
                    } else {
                        seen_searcher_tx = true;
                    }
                }
            }
        }

        let leader_info: String = self.read_redis::<String>("next_leader").await?
                              .ok_or_else(|| Status::unavailable("next_leader"))?;
        let (leader_id, _slot) = leader_info.split_once(';').unwrap();
        let leader_region_key = format!("{}_region", leader_id);
        let leader_region: String = self.read_redis(&leader_region_key).await?
                                        .unwrap_or_else(|| LOCAL_REGION.clone());

        /* 2‑B. Header.ts */
        let mut inner = bundle.clone().bundle.unwrap();
        if inner.header.is_none() || inner.header.as_ref().unwrap().ts.is_none() {
            inner.header = Some(Header {
                ts: Some(Timestamp::from(std::time::SystemTime::now())),
            });
        }

        /* ── 3. No Front-run → Hub publish ────────────────────── */
        let bundle = BundleUuid {bundle:Some(inner),uuid:bundle_id.clone()};
        if leader_region == *LOCAL_REGION {
            self.hub.publish_bundle(bundle);
        } else {
            PEER_MGR.forward_bundle(bundle, &leader_region).await;
        }

        /* ── 4. Redis (tx list·queue·status) ────────────────────────── */
        let ttl = 6 * 60 * 60; // 6 h
        let sig_join = tx_sigs.join(";");
        let bdl = bundle_id.clone();
        tokio::spawn(async move {
            if let Ok(mut conn) = crate::hub::get_redis_conn().await {
                let mut pipe = redis::pipe();
                pipe.cmd("SETEX").arg(&bdl).arg(ttl).arg(&sig_join);
                pipe.cmd("LPUSH").arg("bundle_queue").arg(&bdl);
                pipe.cmd("SETEX").arg(format!("{bdl}_status")).arg(ttl).arg("submitted");
                let _: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
            } else {
                log::warn!("send_bundle: failed to get redis conn for fire-and-forget write");
            }
        });

        /* ── 5. ClickHouse Logging (Accepted) ─────────────────────────── */
        for sig in &tx_sigs {
            log_to_clickhouse(ClickhouseLog {
                bundle_id: bundle_id.clone(),
                tx_sig: sig.clone(),
                stage: "SEARCHER.send_bundle".into(),
                node: "searcher".into(),
                from_addr: remote_addr.clone(),
                description: "bundle forwarded to Hub".into(),
                ..Default::default()
            });
        }

        common_utils::metrics::SEARCHER_BUNDLE_IN_TOTAL.inc();

        return Ok(Response::new(SendBundleResponse { uuid: bundle_id }));
    }

    async fn subscribe_bundle_results(
        &self,
        _req: Request<SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty()) as EmptyBdlStream))
    }

    async fn subscribe_pending_transactions(
        &self,
        _request: Request<PendingTxSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribePendingTransactionsStream>, Status> {
        /* ── 0. Remote IP ───────────────────────────────────── */        
        let remote_addr = _request
            .remote_addr()
            .map(|addr| {
                let s = addr.to_string();
                log::debug!("🔌 SEARCHER subscribed packets from {}", s);
                s
            })
            .unwrap_or_else(|| {
                log::debug!("🔌 SEARCHER subscribed packets");
                "<unknown>".to_string()
            });
        let remote_addr = Arc::new(remote_addr);
        
        /* ── 1. Hub Packet Subscription + Logging ─────────────────── */
        let rx = self.hub.subscribe_packets();
        let st = ReceiverStream::new(rx)
            .inspect({
                let remote_addr = Arc::clone(&remote_addr);
                move |batch| {
                    let n = batch.packets.len();
                    log::debug!("⬅️ SEARCHER.subscribe_pending – {} pkt(s)", n);

                    let sig = first_sig_from_packets(&batch.packets).unwrap_or_default();
                    log_to_clickhouse(ClickhouseLog {
                        tx_sig: sig,
                        stage: "SEARCHER.subscribe_pending".into(),
                        node: "searcher".into(),
                        to_addr: remote_addr.as_str().to_string(),
                        description: format!("streamed {} packets", n),
                        ..Default::default()
                    });
                    common_utils::metrics::PKT_OUT_TOTAL.inc();
                }
            })
            /* PacketBatch → PendingTxNotification */
            .map(|batch| {
                let ts = Timestamp::from(std::time::SystemTime::now());
                Ok(PendingTxNotification {
                    server_side_ts: Some(ts.clone()),
                    expiration_time: None,
                    transactions: batch.packets,
                })
            });

        Ok(Response::new(Box::pin(st) as PendingStream))
    }

    async fn get_next_scheduled_leader(
        &self,
        _req: Request<NextScheduledLeaderRequest>,
    ) -> Result<Response<NextScheduledLeaderResponse>, Status> {
        let current_slot: u64 = self
            .read_redis::<u64>("current_slot")
            .await?
            .ok_or_else(|| Status::unavailable("current_slot not found"))?;

        let next_leader: String = self
            .read_redis::<String>("next_leader")
            .await?
            .ok_or_else(|| Status::unavailable("next_leader not found"))?;

        // "<identity>;<abs_slot>"
        let mut parts = next_leader.split(';');
        let identity  = parts
            .next()
            .ok_or_else(|| Status::internal("malformed next_leader"))?
            .to_string();
        let slot_str  = parts
            .next()
            .ok_or_else(|| Status::internal("malformed next_leader"))?;
        let abs_slot: u64 = slot_str
            .parse()
            .map_err(|_| Status::internal("invalid slot in next_leader"))?;

        Ok(Response::new(NextScheduledLeaderResponse {
            current_slot,
            next_leader_slot: abs_slot,
            next_leader_identity: identity,
            next_leader_region: "".into(), 
        }))
    }

    async fn get_connected_leaders(
        &self,
        _req: Request<ConnectedLeadersRequest>,
    ) -> Result<Response<ConnectedLeadersResponse>, Status> {
        // "next_leader" = "<identity>;<abs_slot>"
        let nl: String = self
            .read_redis::<String>("next_leader")
            .await?
            .ok_or_else(|| Status::unavailable("next_leader not found"))?;
        let (id, slot) = nl.split_once(';')
            .ok_or_else(|| Status::internal("malformed next_leader"))?;
        let abs_slot = slot.parse::<u64>()
            .map_err(|_| Status::internal("invalid slot"))?;

        use jito_protos::searcher::SlotList;
        let mut map = std::collections::HashMap::new();
        map.insert(id.to_string(), SlotList { slots: vec![abs_slot] });

        Ok(Response::new(ConnectedLeadersResponse { connected_validators: map }))
    }

    async fn get_connected_leaders_regioned(
        &self,
        _req: Request<ConnectedLeadersRegionedRequest>,
    ) -> Result<Response<ConnectedLeadersRegionedResponse>, Status> {
        let inner = self.get_connected_leaders(Request::new(ConnectedLeadersRequest {})).await?
            .into_inner();
        let mut outer = std::collections::HashMap::new();
        let region = LOCAL_REGION.clone();
        outer.insert(region, inner);
        Ok(Response::new(ConnectedLeadersRegionedResponse { connected_validators: outer }))
    }
    async fn get_tip_accounts(
        &self,
        _req: Request<GetTipAccountsRequest>,
    ) -> Result<Response<GetTipAccountsResponse>, Status> {
        Err(Status::unimplemented("get_tip_accounts not supported"))
    }
    async fn get_regions(
        &self,
        _req: Request<GetRegionsRequest>,
    ) -> Result<Response<GetRegionsResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}

fn first_sig_from_packets(packets: &[jito_protos::packet::Packet]) -> Option<String> {
    for pkt in packets {
        if let Some(s) = crate::tx_fast::fast_first_signature_b58(&pkt.data) {
            return Some(s);
        }
    }
    None
}

/* ────────── Server Builder helper ────────── */
pub fn rpc_service(hub: SharedHub) -> SearcherServiceServer<SearcherRpcSvc> {
    SearcherServiceServer::new(SearcherRpcSvc::new(hub))
}