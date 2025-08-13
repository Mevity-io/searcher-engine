// searcher-engine/src/detect.rs
use once_cell::sync::Lazy;
use serde_json::Value;
use std::net::IpAddr;

/// Current Node's REGION
pub static LOCAL_REGION: Lazy<String> =
    Lazy::new(|| detect_region().unwrap_or_else(|| "UNKNOWN".into()));

fn detect_region() -> Option<String> {
    /* 1) Public IP */
    let my_ip: IpAddr = {
        let text = reqwest::blocking::get("https://api.ipify.org").ok()?.text().ok()?;
        text.parse().ok()?
    };

    /* 2) Mevity API */
    let host = std::env::var("MEVITY_HOST").unwrap_or_else(|_| "https://".into());
    let key  = std::env::var("MEVITY_API_KEY").ok()?;
    let url  = format!("{host}/api/nodes?api_key={key}");
    let json: Value = reqwest::blocking::get(url).ok()?.json().ok()?;

    for n in json["data"].as_array()? {
        if n["category"] != "SEARCHER" { continue; }
        let ip = n["ip"].as_str()?.split(':').next()?;
        if ip.parse::<IpAddr>().ok()? == my_ip {
            return Some(n["region"].as_str()?.to_string());
        }
    }
    None
}