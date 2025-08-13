// searcher-engine/src/tx_fast.rs
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::message::{VersionedMessage, v0, legacy};

/// Quickly extract the first signature (base58) from
/// bincode-serialized VersionedTransaction bytes.
/// Fallback to full bincode deserialization if it fails.
pub fn fast_first_signature_b58(data: &[u8]) -> Option<String> {
    // Vec<Signature> Header(u64) + Signature 64bytes
    if data.len() >= 8 + 64 {
        // len(u64 LE)
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&data[0..8]);
        let n = u64::from_le_bytes(len_bytes) as usize;

        if n > 0 && data.len() >= 8 + 64 {
            // Base58 encode a 64-byte slice directly (no copy).
            let sig_b58 = bs58::encode(&data[8..8 + 64]).into_string();
            return Some(sig_b58);
        }
    }

    // Safe fallback: full deserialization
    if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(data) {
        if let Some(s) = tx.signatures.get(0) {
            return Some(s.to_string());
        }
    }
    None
}

/// bincode-serialized VersionedTransaction → extracts static account keys
/// w/o LUT
pub fn extract_static_account_keys(data: &[u8]) -> Option<Vec<Pubkey>> {
    if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(data) {
        match tx.message {
            VersionedMessage::Legacy(legacy::Message { account_keys, .. }) => Some(account_keys),
            VersionedMessage::V0(v0::Message { account_keys, .. }) => Some(account_keys),
        }
    } else { None }
}