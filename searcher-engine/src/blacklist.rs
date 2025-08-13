// searcher-engine/src/blacklist.rs
use once_cell::sync::Lazy;
use rayon::prelude::*;
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::{Arc, RwLock}, time::Duration};
use log::{info, warn};
use jito_protos::packet::{Packet, PacketBatch};

static REFRESH_SECS_DEFAULT: u64 = 3600;   // 1h
static PAR_MIN_DEFAULT: usize = 16;        // 병렬 필터 임계치

/// RwLock<Arc<Set>>: 쓰기 시 Arc 교체, 읽기는 Arc clone(락 해제 후 O(1) 조회).
struct BlStore {
    inner: RwLock<Arc<HashSet<Pubkey>>>,
}
impl BlStore {
    fn new() -> Self {
        Self { inner: RwLock::new(Arc::new(HashSet::new())) }
    }
    fn snapshot(&self) -> Arc<HashSet<Pubkey>> {
        self.inner.read().unwrap().clone()
    }
    fn replace(&self, new_set: HashSet<Pubkey>) {
        *self.inner.write().unwrap() = Arc::new(new_set);
    }
}

#[allow(private_interfaces)]
pub static BLACKLIST: Lazy<BlStore> = Lazy::new(BlStore::new);

/// Background refresh
pub fn init() {
    let host = std::env::var("MEVITY_HOST").unwrap_or_else(|_| "https://".into());
    let key  = std::env::var("MEVITY_API_KEY").unwrap_or_default();
    let secs = std::env::var("BLACKLIST_REFRESH_SECS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(REFRESH_SECS_DEFAULT);

    tokio::spawn(async move {
        loop {
            if let Err(e) = refresh_once(&host, &key).await {
                warn!("blacklist refresh error: {e}");
            }
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
    });
}

async fn refresh_once(host: &str, key: &str) -> anyhow::Result<()> {
    if key.is_empty() { 
        warn!("MEVITY_API_KEY not set; blacklist disabled");
        return Ok(());
    }
    let url = format!("{host}/api/blacklists?api_key={key}");
    let json: Value = reqwest::get(&url).await?.json().await?;

    // Scheme: data: [string] or data: [{address: string}]
    let mut set = HashSet::<Pubkey>::new();
    if let Some(arr) = json.get("data").and_then(|v| v.as_array()) {
        for it in arr {
            if let Some(addr) = it.as_str().or_else(|| it.get("address").and_then(|v| v.as_str())) {
                if let Ok(pk) = addr.parse::<Pubkey>() { set.insert(pk); }
            }
        }
    } else if let Some(arr) = json.get("wallets").and_then(|v| v.as_array()) {
        for it in arr {
            if let Some(addr) = it.as_str() {
                if let Ok(pk) = addr.parse::<Pubkey>() { set.insert(pk); }
            }
        }
    }

    BLACKLIST.replace(set);
    let n = BLACKLIST.snapshot().len();
    info!("🔒 blacklist refreshed: {} wallet(s)", n);
    Ok(())
}

fn extract_keys(data: &[u8]) -> Option<Vec<Pubkey>> {
    crate::tx_fast::extract_static_account_keys(data)
}

/// Filter : Drop Blacklisted packets
pub fn filter_batch(batch: PacketBatch) -> PacketBatch {
    let bl = BLACKLIST.snapshot();
    if bl.is_empty() { return batch; }

    let par_min: usize = std::env::var("BL_PAR_MIN").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(PAR_MIN_DEFAULT);

    let keep = |pkt: Packet| -> Option<Packet> {
        if let Some(keys) = extract_keys(&pkt.data) {
            if keys.iter().any(|k| bl.contains(k)) {
                // common_utils::metrics::PKT_BLACKLIST_DROP_TOTAL.inc(); // (metrics 추가 시 활성화)
                return None;
            }
        }
        Some(pkt)
    };

    let packets = if batch.packets.len() >= par_min {
        batch.packets.into_par_iter().filter_map(keep).collect()
    } else {
        batch.packets.into_iter().filter_map(keep).collect()
    };
    PacketBatch { packets }
}

/// Check blacklist in bundle
pub fn bundle_has_blacklisted(packets: &[Packet]) -> bool {
    let bl = BLACKLIST.snapshot();
    if bl.is_empty() { return false; }
    for pkt in packets {
        if let Some(keys) = extract_keys(&pkt.data) {
            if keys.iter().any(|k| bl.contains(k)) { return true; }
        }
    }
    false
}