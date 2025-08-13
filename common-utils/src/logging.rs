use once_cell::sync::{Lazy, OnceCell};
use serde::Serialize;
use chrono::{Utc, DateTime};
use reqwest::Client;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use futures_util::StreamExt;
use log::{warn};

#[derive(Debug, Default, Serialize, Clone)]
pub struct ClickhouseLog {
    pub tx_sig: String,
    pub bundle_id: String,
    pub stage: String,
    pub node: String,
    pub from_node: String,
    pub from_addr: String,
    pub to_node: String,
    pub to_addr: String,
    pub description: String,
    pub slot: Option<u64>,
    pub block_time: Option<DateTime<Utc>>,
    pub timestamp: String,
}

/* ───── ① Global Async Client ───── */
pub static CH_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .pool_idle_timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(64)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("build ClickHouse client")
});

static SENDER: OnceCell<mpsc::Sender<ClickhouseLog>> = OnceCell::new();

/// **Init – `tokio::spawn` for async workers **
pub fn init() {
    let (tx, rx) = mpsc::channel(1000);
    SENDER.set(tx).ok();

    // 비동기 스트림 소비 태스크
    tokio::spawn(async move {
        let mut stream = ReceiverStream::new(rx);

        while let Some(mut log) = stream.next().await {
            log.timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();

            let body = serde_json::to_string(&log)
                .expect("serialize log") + "\n";

            let req = CH_CLIENT
                .post(format!(
                    "{}/?query=INSERT INTO tx_tracking_logs FORMAT JSONEachRow",
                    std::env::var("CLICKHOUSE_URL")
                        .unwrap_or_else(|_| "http://127.0.0.1:8123".into())
                ))
                .basic_auth(
                    std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
                    Some(&std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default()),
                )
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::CONNECTION, "keep-alive")  
                .body(body);

            match req.send().await {
                Ok(resp) => {
                    let _ = resp.bytes().await;        // discard & drain
                }
                Err(e) => warn!("ClickHouse write error: {e}"),
            }
        }
    });
}

#[allow(dead_code)]
pub fn log_to_clickhouse(log: ClickhouseLog) {
    if let Some(tx) = SENDER.get() {
        let _ = tx.try_send(log);
    }
}