use std::pin::Pin;

use futures_util::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tonic::Status;

use crate::hub::SharedHub;
use crate::pb::{ArbitrageIx, ArbitrageIxKv, ArbitrageTx, ArbitrageTxBatch};
use super::arbitrage_filter::try_filter_tx;
use rayon::prelude::*; // ⬅ 추가

pub type ArbitrageStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<ArbitrageTxBatch, Status>> + Send + 'static>>;

pub fn build(hub: SharedHub) -> ArbitrageStream {
    // Zero-copy
    let b_rx = hub.subscribe_packets_arc();

    let st = BroadcastStream::new(b_rx).filter_map(|res| async move {
        let arc_batch = match res {
            Ok(v) => v,
            Err(BroadcastStreamRecvError::Lagged(_)) => return None, // Only Recent Ones
        };

        let pkts = &arc_batch.packets;
        let par_min: usize = std::env::var("ARB_PAR_MIN").ok()
            .and_then(|s| s.parse().ok()).unwrap_or(8);

        // Parallel/Sequential
        let picked: Vec<ArbitrageTx> = if pkts.len() >= par_min {
            pkts.par_iter().filter_map(|pkt| {
                let (sig, ixes, raw_tx) = try_filter_tx(pkt)?;
                let ixs = ixes.into_iter().map(|ix| {
                    let attrs = ix.attrs.into_iter()
                        .map(|(k, v)| ArbitrageIxKv { key: k, val: v })
                        .collect();
                    ArbitrageIx {
                        program_id: ix.program_id.to_string(),
                        program_label: ix.program_label.to_string(),
                        method: ix.method,
                        attrs,
                        raw: ix.raw,
                    }
                }).collect();
                Some(ArbitrageTx { signature: sig, raw_tx, ixs })
            }).collect()
        } else {
            pkts.iter().filter_map(|pkt| {
                let (sig, ixes, raw_tx) = try_filter_tx(pkt)?;
                let ixs = ixes.into_iter().map(|ix| {
                    let attrs = ix.attrs.into_iter()
                        .map(|(k, v)| ArbitrageIxKv { key: k, val: v })
                        .collect();
                    ArbitrageIx {
                        program_id: ix.program_id.to_string(),
                        program_label: ix.program_label.to_string(),
                        method: ix.method,
                        attrs,
                        raw: ix.raw,
                    }
                }).collect();
                Some(ArbitrageTx { signature: sig, raw_tx, ixs })
            }).collect()
        };

        if picked.is_empty() { None } else { Some(Ok(ArbitrageTxBatch { txs: picked })) }
    });

    Box::pin(st)
}