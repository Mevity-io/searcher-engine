// searcher-engine/src/peer_manager.rs
use anyhow::Result;
use jito_protos::bundle::BundleUuid;
use jito_protos::{packet::PacketBatch};
use lazy_static::lazy_static;
use reqwest;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time::sleep};
use tonic::transport::{Channel, Endpoint};
use futures_util::future::join_all;

use crate::interregion::{InterRegionClient, PacketWrapper};
use crate::detect::LOCAL_REGION;

/* ───────────────────────────── */
pub struct PeerManager {
    peers: RwLock<HashMap<String /*region*/, InterRegionClient<Channel>>>,
    api_host: String,
    api_key: String,
}

impl PeerManager {
    pub fn new(api_host: String, api_key: String) -> Arc<Self> {
        Arc::new(Self {
            peers: RwLock::new(HashMap::new()),
            api_host,
            api_key,
        })
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.refresh().await {
                    log::warn!("peer refresh error: {e}");
                }
                sleep(Duration::from_secs(3600)).await; // 1 h
            }
        });
    }

    async fn refresh(&self) -> Result<()> {
        let url = format!("{}/api/nodes?api_key={}", self.api_host, self.api_key);
        let json: serde_json::Value = reqwest::get(&url).await?.json().await?;
        let list = json["data"].as_array().ok_or_else(|| anyhow::anyhow!("bad json"))?;

        let mut map = self.peers.write().await;

        /* New Connections */
        for n in list {
            if n["category"] != "SEARCHER" { continue; }
            let region = n["region"].as_str().unwrap().to_string();
            if region == *LOCAL_REGION { continue; }

            let host = n["ip"].as_str().unwrap();
            if map.contains_key(&region) { continue; }

            let chan = Endpoint::from_shared(format!("http://{host}"))?
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await?;
            map.insert(region.clone(), InterRegionClient::new(chan));
            log::info!("🌐 peer added: {region}");
        }

        /* Remove Disconnected Peers */
        map.retain(|reg, _| {
            list.iter().any(|n| {
                n["category"] == "SEARCHER" && n["region"].as_str().unwrap() == reg
            })
        });
        Ok(())
    }

    /* ── Forward ────────────────────────────────────────────── */
    pub async fn forward_packet(&self, batch: PacketBatch) {
        let msg = PacketWrapper { src_region: LOCAL_REGION.clone(), batch: Some(batch.clone()) };

        let peers: Vec<(String, InterRegionClient<Channel>)> =
            self.peers.read().await.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        join_all(peers.into_iter().map(|(reg, mut cli)| {
            let msg = msg.clone();
            async move {
                let _ = cli.send_packet(msg).await;
                log::debug!("➡️  packet → {reg}");
            }
        })).await;
    }

    pub async fn forward_bundle(&self, bundle: BundleUuid, target_region: &str) {
        if let Some(mut cli) = self.peers.read().await.get(target_region).cloned() {
            let _ = cli.send_bundle(bundle).await;
            log::debug!("💎 bundle → {target_region}");
            return;
        }
        log::warn!("❌ no peer for region {target_region}");
    }
}

/* ── Global Singleton ─────────────────────────────────────────────── */
lazy_static! {
    pub static ref PEER_MGR: Arc<PeerManager> = {
        let host = std::env::var("MEVITY_HOST").unwrap_or_else(|_| "https://".into());
        let key  = std::env::var("MEVITY_API_KEY").expect("MEVITY_API_KEY");
        let mgr  = PeerManager::new(host, key);
        mgr.clone().start();
        mgr
    };
}