//searcher_engine/src/utils.rs
use rand::{rng, RngCore};

/// Jito Style(32 bytes random → 64hex) UUID 
pub fn gen_bundle_uuid() -> String {
    
    let mut bytes = [0u8; 32];
    rng().fill_bytes(&mut bytes);
    hex::encode(bytes)                    // 64‑byte hex String
}