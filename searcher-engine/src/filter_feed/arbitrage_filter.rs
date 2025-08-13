use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use jito_protos::packet::Packet;
use solana_sdk::{
    instruction::CompiledInstruction,
    message::{v0, legacy, VersionedMessage},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use crate::tx_fast::fast_first_signature_b58;

/* ── Target Program IDs ───────────────────────────────────── */
pub const RAYDIUM_CLMM:      &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
pub const RAYDIUM_CPMM:      &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const RAYDIUM_V4:        &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const ORCA_WHIRLPOOL:    &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const METEORA_DLMM:      &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const METEORA_DAMM_V1:   &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";
pub const METEORA_DAMM_V2:   &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
pub const SABER_STABLE_SWAP: &str = "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ";
pub const CROPPER_WHIRLPOOL: &str = "H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt";
pub const MARGINFI_V2:       &str = "MFv2hWf31Z9kbCa1snEPYctyafyhdvnV7FZnsebVacA";
pub const PUMPSWAP:          &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

pub static TARGET_SET: Lazy<HashSet<Pubkey>> = Lazy::new(|| {
    [
        RAYDIUM_CLMM, RAYDIUM_CPMM, RAYDIUM_V4, ORCA_WHIRLPOOL,
        METEORA_DLMM, METEORA_DAMM_V1, METEORA_DAMM_V2, SABER_STABLE_SWAP,
        CROPPER_WHIRLPOOL, MARGINFI_V2, PUMPSWAP,
    ]
    .iter()
    .filter_map(|s| s.parse().ok())
    .collect()
});

pub static LABELS: Lazy<HashMap<Pubkey, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    macro_rules! ins { ($k:expr,$v:expr)=>{ m.insert(Pubkey::from_str($k).unwrap(), $v); } }
    ins!(RAYDIUM_CLMM, "Raydium CLMM");
    ins!(RAYDIUM_CPMM, "Raydium CPMM");
    ins!(RAYDIUM_V4,   "Raydium AMM v4");
    ins!(ORCA_WHIRLPOOL, "Orca Whirlpools");
    ins!(METEORA_DLMM, "Meteora DLMM");
    ins!(METEORA_DAMM_V1, "Meteora Pools v1");
    ins!(METEORA_DAMM_V2, "Meteora DAMM v2");
    ins!(SABER_STABLE_SWAP, "Saber StableSwap");
    ins!(CROPPER_WHIRLPOOL, "Cropper Whirlpool");
    ins!(MARGINFI_V2, "marginfi v2");
    ins!(PUMPSWAP, "Pump.fun AMM");
    m
});

/* ── Byte Parser ───────────────────────────────────────── */
#[inline]
fn u16_le(b:&[u8], o:usize) -> Option<(u16,usize)> {
    if b.len() < o + 2 { return None; }
    let arr: [u8;2] = b[o..o+2].try_into().ok()?;
    Some((u16::from_le_bytes(arr), o+2))
}
#[inline]
fn u64_le(b:&[u8], o:usize) -> Option<(u64,usize)> {
    if b.len() < o + 8 { return None; }
    let arr: [u8;8] = b[o..o+8].try_into().ok()?;
    Some((u64::from_le_bytes(arr), o+8))
}
#[inline]
fn i64_le(b:&[u8], o:usize) -> Option<(i64,usize)> {
    if b.len() < o + 8 { return None; }
    let arr: [u8;8] = b[o..o+8].try_into().ok()?;
    Some((i64::from_le_bytes(arr), o+8))
}
#[inline]
fn u128_le(b:&[u8], o:usize) -> Option<(u128,usize)> {
    if b.len() < o + 16 { return None; }
    let arr: [u8;16] = b[o..o+16].try_into().ok()?;
    Some((u128::from_le_bytes(arr), o+16))
}

/* ── Anchor discriminator ─────────────────────────────── */
fn disc(name: &str) -> [u8;8] {
    let mut h = Sha256::new();
    h.update(format!("global:{name}").as_bytes());
    let out = h.finalize();
    let mut d = [0u8;8];
    d.copy_from_slice(&out[..8]);
    d
}

/* ── Parser Type: fn Pointer ------───────────────────── */
type Parser = fn(&[u8]) -> (&'static str, Vec<(String,String)>);

/* ── Dispatch Table ──────────────────────── */
pub static ANCHOR_DISPATCH: Lazy<HashMap<Pubkey, HashMap<[u8;8], Parser>>> = Lazy::new(|| {
    let mut outer: HashMap<Pubkey, HashMap<[u8;8], Parser>> = HashMap::new();

    // Pump.fun
    let mut pump: HashMap<[u8;8], Parser> = HashMap::new();
    pump.insert(disc("buy"), |r:&[u8]|{
        let (base_out, o1) = u64_le(r,0).unwrap_or((0,0));
        let (max_qin, _ )  = u64_le(r,o1).unwrap_or((0,0));
        ("buy", vec![
            ("base_amount_out".into(), base_out.to_string()),
            ("max_quote_amount_in".into(), max_qin.to_string()),
        ])
    });
    pump.insert(disc("sell"), |r:&[u8]|{
        let (base_in, o1)  = u64_le(r,0).unwrap_or((0,0));
        let (min_qout, _)  = u64_le(r,o1).unwrap_or((0,0));
        ("sell", vec![
            ("base_amount_in".into(), base_in.to_string()),
            ("min_quote_amount_out".into(), min_qout.to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(PUMPSWAP).unwrap(), pump);

    // Raydium CLMM
    let mut clmm: HashMap<[u8;8], Parser> = HashMap::new();
    clmm.insert(disc("swap_v2"), |r:&[u8]|{
        let (amount, o1) = u64_le(r,0).unwrap_or((0,0));
        let (oth   , o2) = u64_le(r,o1).unwrap_or((0,0));
        let (sqrt  , o3) = u128_le(r,o2).unwrap_or((0,0));
        let is_base_input = r.get(o3).copied().unwrap_or(0) != 0;
        ("swap_v2", vec![
            ("amount".into(), amount.to_string()),
            ("other_amount_threshold".into(), oth.to_string()),
            ("sqrt_price_limit_x64".into(), sqrt.to_string()),
            ("is_base_input".into(), (is_base_input as u8).to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(RAYDIUM_CLMM).unwrap(), clmm);

    // Raydium CPMM
    let mut cpmm: HashMap<[u8;8], Parser> = HashMap::new();
    cpmm.insert(disc("swap_base_input"), |r:&[u8]|{
        let (ain, o1) = u64_le(r,0).unwrap_or((0,0));
        let (min, _)  = u64_le(r,o1).unwrap_or((0,0));
        ("swap_base_input", vec![
            ("amount_in".into(), ain.to_string()),
            ("minimum_amount_out".into(), min.to_string()),
        ])
    });
    cpmm.insert(disc("swap_base_output"), |r:&[u8]|{
        let (max_in, o1) = u64_le(r,0).unwrap_or((0,0));
        let (aout  , _)  = u64_le(r,o1).unwrap_or((0,0));
        ("swap_base_output", vec![
            ("max_amount_in".into(), max_in.to_string()),
            ("amount_out".into(), aout.to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(RAYDIUM_CPMM).unwrap(), cpmm);

    // Orca Whirlpools
    let mut whirl: HashMap<[u8;8], Parser> = HashMap::new();
    whirl.insert(disc("swap"), |r:&[u8]|{
        let (amount, o1) = u64_le(r,0).unwrap_or((0,0));
        let (oth   , o2) = u64_le(r,o1).unwrap_or((0,0));
        let (sqrt  , o3) = u128_le(r,o2).unwrap_or((0,0));
        let amount_specified_is_input = r.get(o3).copied().unwrap_or(0) != 0;
        let a_to_b = r.get(o3+1).copied().unwrap_or(0) != 0;
        ("swap", vec![
            ("amount".into(), amount.to_string()),
            ("other_amount_threshold".into(), oth.to_string()),
            ("sqrt_price_limit".into(), sqrt.to_string()),
            ("amount_specified_is_input".into(), (amount_specified_is_input as u8).to_string()),
            ("a_to_b".into(), (a_to_b as u8).to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(ORCA_WHIRLPOOL).unwrap(), whirl);

    // Meteora DLMM
    let mut dlmm: HashMap<[u8;8], Parser> = HashMap::new();
    dlmm.insert(disc("swap"), |r:&[u8]|{
        let (ain, o1) = u64_le(r,0).unwrap_or((0,0));
        let (min, _)  = u64_le(r,o1).unwrap_or((0,0));
        ("swap", vec![
            ("amount_in".into(), ain.to_string()),
            ("min_amount_out".into(), min.to_string()),
        ])
    });
    dlmm.insert(disc("initialize_bin_array"), |r:&[u8]|{
        let (idx, _) = i64_le(r,0).unwrap_or((0,0));
        ("initialize_bin_array", vec![("index".into(), idx.to_string())])
    });
    dlmm.insert(disc("rebalance_liquidity"), |r:&[u8]|{
        #[inline]
        fn i32_le(b:&[u8], o:usize) -> Option<(i32,usize)> {
            if b.len() < o + 4 { return None; }
            let arr: [u8;4] = b[o..o+4].try_into().ok()?;
            Some((i32::from_le_bytes(arr), o+4))
        }
        let (active, o1) = i32_le(r,0).unwrap_or((0,0));
        let (slip  , o2) = u16_le(r,o1).unwrap_or((0,0));
        let fee    = r.get(o2).copied().unwrap_or(0) != 0;
        let rew    = r.get(o2+1).copied().unwrap_or(0) != 0;
        let (min_x , o3) = u64_le(r, o2+2).unwrap_or((0,0));
        let (max_x , o4) = u64_le(r, o3).unwrap_or((0,0));
        let (min_y , o5) = u64_le(r, o4).unwrap_or((0,0));
        let (max_y , _ ) = u64_le(r, o5).unwrap_or((0,0));
        ("rebalance_liquidity", vec![
            ("active_id".into(), active.to_string()),
            ("max_active_bin_slippage".into(), slip.to_string()),
            ("should_claim_fee".into(), (fee as u8).to_string()),
            ("should_claim_reward".into(), (rew as u8).to_string()),
            ("min_withdraw_x_amount".into(), min_x.to_string()),
            ("max_deposit_x_amount".into(), max_x.to_string()),
            ("min_withdraw_y_amount".into(), min_y.to_string()),
            ("max_deposit_y_amount".into(), max_y.to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(METEORA_DLMM).unwrap(), dlmm);

    // Meteora DAMM v2
    let mut damm2: HashMap<[u8;8], Parser> = HashMap::new();
    damm2.insert(disc("swap"), |r:&[u8]|{
        let (ain, o1) = u64_le(r,0).unwrap_or((0,0));
        let (min, _)  = u64_le(r,o1).unwrap_or((0,0));
        ("swap", vec![
            ("amount_in".into(), ain.to_string()),
            ("minimum_amount_out".into(), min.to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(METEORA_DAMM_V2).unwrap(), damm2);

    // Meteora Pools v1
    let mut damm1: HashMap<[u8;8], Parser> = HashMap::new();
    damm1.insert(disc("swap"), |r:&[u8]|{
        let (ain, o1) = u64_le(r,0).unwrap_or((0,0));
        let (min, _)  = u64_le(r,o1).unwrap_or((0,0));
        ("swap", vec![
            ("inAmount".into(), ain.to_string()),
            ("minimumOutAmount".into(), min.to_string()),
        ])
    });
    outer.insert(Pubkey::from_str(METEORA_DAMM_V1).unwrap(), damm1);

    outer
});

/* ── Anchor Decode ───────────────────────────────────── */
#[inline]
fn decode_anchor(pid: &Pubkey, data: &[u8]) -> Option<(&'static str, Vec<(String,String)>)> {
    if data.len() < 8 { return None; }
    let disc_bytes: [u8;8] = data[..8].try_into().ok()?;
    let rest = &data[8..];
    let map = ANCHOR_DISPATCH.get(pid)?;
    let f = map.get(&disc_bytes)?;
    Some(f(rest))
}

/* ── Raydium v4(None-Anchor) ─────────────────────────── */
#[inline]
fn decode_raydium_v4(data: &[u8]) -> Option<(&'static str, Vec<(String,String)>)> {
    if data.is_empty() { return None; }
    if data[0] != 9 { return Some(("unknown", vec![("tag".into(), data[0].to_string())])); }
    if data.len() < 1+16 { return None; }
    let mut o = 1;
    let amount_in = u64::from_le_bytes(data[o..o+8].try_into().ok()?); o+=8;
    let min_out   = u64::from_le_bytes(data[o..o+8].try_into().ok()?);
    Some(("swap", vec![
        ("amount_in".into(), amount_in.to_string()),
        ("minimum_amount_out".into(), min_out.to_string()),
    ]))
}

#[inline]
fn get_static_keys<'a>(msg: &'a VersionedMessage) -> (&'a Vec<Pubkey>, &'a Vec<CompiledInstruction>) {
    match msg {
        VersionedMessage::Legacy(legacy::Message { account_keys, instructions, .. }) => (account_keys, instructions),
        VersionedMessage::V0(v0::Message { account_keys, instructions, .. }) => (account_keys, instructions),
    }
}

/* ── Decode Results ───────────────────────────────────── */
#[derive(Default, Clone)]
pub struct DecodedIx {
    pub program_id: Pubkey,
    pub program_label: &'static str,
    pub method: String,
    pub attrs: Vec<(String,String)>,
    pub raw: Vec<u8>,
}

/* ── Packet → (sig, matched ixs, raw_tx) ───────────── */
pub fn try_filter_tx(pkt: &Packet) -> Option<(String, Vec<DecodedIx>, Vec<u8>)> {
    let sig = fast_first_signature_b58(&pkt.data)?;

    let Ok(tx): Result<VersionedTransaction, _> = bincode::deserialize(&pkt.data) else { return None; };

    let (keys, ixs) = get_static_keys(&tx.message);
    let mut out: Vec<DecodedIx> = Vec::new();

    for cix in ixs {
        let pid = match keys.get(cix.program_id_index as usize) { Some(k)=>k, None=>continue };
        if !TARGET_SET.contains(pid) { continue; }

        let label = *LABELS.get(pid).unwrap_or(&"");
        let data = cix.data.as_slice();

        if let Some((name, attrs)) = decode_anchor(pid, data) {
            out.push(DecodedIx {
                program_id: *pid, program_label: label, method: name.into(),
                attrs, raw: data.to_vec()
            });
            continue;
        }
        if pid.to_string() == RAYDIUM_V4 {
            if let Some((name, attrs)) = decode_raydium_v4(data) {
                out.push(DecodedIx {
                    program_id: *pid, program_label: label, method: name.into(),
                    attrs, raw: data.to_vec()
                });
                continue;
            }
        }
        out.push(DecodedIx {
            program_id: *pid, program_label: label, method: "unknown".into(),
            attrs: vec![("data_len".into(), data.len().to_string())],
            raw: data.to_vec()
        });
    }

    if out.is_empty() { return None; }
    Some((sig, out, pkt.data.clone()))
}