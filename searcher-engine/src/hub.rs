// searcher-engine/src/hub.rs
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use chrono::Utc;
use jito_protos::{
    bundle::BundleUuid,
    packet::PacketBatch,
};
use log::{debug, warn};
use redis::aio::MultiplexedConnection;
use tokio::sync::{broadcast, mpsc};
use tokio::sync::Mutex as TokioMutex;

use rayon::prelude::*;
use crate::tx_fast::fast_first_signature_b58;

/* ───────── Redis helpers ───────── */
use once_cell::sync::Lazy;

static REDIS_URL: Lazy<String> =
    Lazy::new(|| std::env::var("REDIS_SERVER").unwrap_or_else(|_| "redis://127.0.0.1/".into()));
static REDIS_CLIENT: Lazy<redis::Client> =
    Lazy::new(|| redis::Client::open(REDIS_URL.clone()).expect("redis client"));
static REDIS_CONN: Lazy<TokioMutex<Option<MultiplexedConnection>>> =
    Lazy::new(|| TokioMutex::new(None));

pub(crate) async fn get_redis_conn() -> redis::RedisResult<MultiplexedConnection> {
    let mut g = REDIS_CONN.lock().await;
    if let Some(c) = g.as_ref() {
        return Ok(c.clone());
    }
    let c = REDIS_CLIENT.get_multiplexed_async_connection().await?;
    *g = Some(c.clone());
    Ok(c)
}
/* ─────────────────────────────────────────────────────────── */

use crate::peer_manager::PEER_MGR;

pub type SharedHub = Arc<Hub>;

fn cap(env: &str, defv: usize) -> usize {
    std::env::var(env)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(defv)
}

#[allow(dead_code)]
pub struct Hub {
    /* 패킷: 링버퍼 */
    pkt_tx: broadcast::Sender<Arc<PacketBatch>>,

    /* 번들: 링버퍼 */
    bdl_tx: broadcast::Sender<BundleUuid>,

    /* 서명 인덱싱 워커 큐 */
    sig_tx: mpsc::Sender<Arc<PacketBatch>>,
    sig_ttl_sec: usize,

    /* 메트릭용 구독자 카운터 */
    pkt_sub_count: Arc<AtomicI64>,
}

impl Hub {
    pub fn new() -> Self {
        // Ring Buffer Size
        let (pkt_tx, _pkt_rx0) = broadcast::channel(cap("PACKET_RING_CAP", 2048));
        let (bdl_tx, _bdl_rx0) = broadcast::channel(cap("BUNDLE_RING_CAP", 1024));

        // Indexing Signatures
        let (sig_tx, mut sig_rx) = mpsc::channel::<Arc<PacketBatch>>(cap("SIG_INDEX_Q_CAP", 4096));
        let sig_ttl_sec: usize = std::env::var("PKT_SIG_TTL_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3000);

        // 백그라운드 서명 인덱서 워커
        tokio::spawn(async move {
            while let Some(arc_batch) = sig_rx.recv().await {
                let sigs = match tokio::task::spawn_blocking({
                    let arc_batch = arc_batch.clone();
                    move || {
                        let pkts = &arc_batch.packets;
                        let par_min: usize = std::env::var("ARB_PAR_MIN").ok()
                            .and_then(|s| s.parse().ok()).unwrap_or(8);

                        if pkts.len() >= par_min {
                            pkts.par_iter()
                                .filter_map(|p| fast_first_signature_b58(&p.data))
                                .collect::<Vec<String>>()
                        } else {
                            pkts.iter()
                                .filter_map(|p| fast_first_signature_b58(&p.data))
                                .collect::<Vec<String>>()
                        }
                    }
                }).await {
                    Ok(v) => v,
                    Err(_) => { warn!("signature indexing task join error"); continue; }
                };

                if sigs.is_empty() {
                    continue;
                }

                if let Ok(mut conn) = get_redis_conn().await {
                    let now = Utc::now().timestamp_millis().to_string();
                    let mut pipe = redis::pipe();
                    for s in sigs {
                        pipe.cmd("SETEX").arg(s).arg(sig_ttl_sec).arg(&now);
                    }
                    let _: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
                }
            }
        });

        Self {
            pkt_tx,
            bdl_tx,
            sig_tx,
            sig_ttl_sec,
            pkt_sub_count: Arc::new(AtomicI64::new(0)),
        }
    }

    /* ── Packets ───────────────────────────────────────────── */

    pub fn subscribe_packets(&self) -> mpsc::Receiver<PacketBatch> {
        let cap = cap("PACKET_SUB_CAP", 512);
        let (tx, rx) = mpsc::channel::<PacketBatch>(cap);

        // broadcast Receiver (각 구독자마다 독립)
        let mut b_rx = self.pkt_tx.subscribe();

        let subs = self.pkt_sub_count.clone();
        let cur = subs.fetch_add(1, Ordering::Relaxed) + 1;
        common_utils::metrics::SEARCHER_COUNT.set(cur);

        tokio::spawn(async move {
            loop {
                match b_rx.recv().await {
                    Ok(arc_batch) => {
                        match tx.try_send((*arc_batch).clone()) {
                            Ok(_) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                common_utils::metrics::PACKET_DROP_TOTAL.inc();
                                debug!("[hub] drop to slow mpsc subscriber (packets)");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("[hub] packet subscriber lagged: dropped {} items", n);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            let cur = subs.fetch_sub(1, Ordering::Relaxed) - 1;
            common_utils::metrics::SEARCHER_COUNT.set(cur);
        });

        rx
    }

    pub fn publish_local_packet(&self, batch: PacketBatch) {
        let arc = Arc::new(batch);

        let _ = self.pkt_tx.send(arc.clone());

        common_utils::metrics::PKT_IN_TOTAL.inc_by(arc.packets.len() as f64);

        if let Err(mpsc::error::TrySendError::Full(_)) = self.sig_tx.try_send(arc.clone()) {
            debug!("[hub] sig-index queue full — skip indexing for this batch");
        }

        tokio::spawn(async move {
            PEER_MGR.forward_packet((*arc).clone()).await;
        });
    }

    pub fn publish_remote_packet(&self, batch: PacketBatch) {
        let arc = Arc::new(batch);
        let _ = self.pkt_tx.send(arc.clone());
        common_utils::metrics::PKT_IN_TOTAL.inc_by(arc.packets.len() as f64);

        if let Err(mpsc::error::TrySendError::Full(_)) = self.sig_tx.try_send(arc) {
            debug!("[hub] sig-index queue full — skip indexing for this batch");
        }
    }

    pub fn subscribe_packets_arc(&self) -> tokio::sync::broadcast::Receiver<std::sync::Arc<PacketBatch>> {
        self.pkt_tx.subscribe()
    }

    /* ── Bundles ───────────────────────────────────────────── */

    pub fn subscribe_bundles(&self) -> mpsc::Receiver<BundleUuid> {
        let cap = cap("BUNDLE_SUB_CAP", 1024);
        let (tx, rx) = mpsc::channel::<BundleUuid>(cap);
        let mut b_rx = self.bdl_tx.subscribe();

        tokio::spawn(async move {
            loop {
                match b_rx.recv().await {
                    Ok(bdl) => {
                        match tx.try_send(bdl) {
                            Ok(_) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                common_utils::metrics::BUNDLE_DROP_TOTAL.inc();
                                debug!("[hub] drop to slow mpsc subscriber (bundles)");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("[hub] bundle subscriber lagged: dropped {} items", n);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        rx
    }

    pub fn publish_bundle(&self, bdl: BundleUuid) {
        let _ = self.bdl_tx.send(bdl);
    }
}