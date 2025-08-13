// searcher-engine/src/lib.rs
pub mod pb {
    tonic::include_proto!("searcherengine");
}

/* ─── inter‑region proto ─── */
pub mod interregion {
    tonic::include_proto!("interregion");

    pub use self::inter_region_client::InterRegionClient;
    pub use self::inter_region_server::{InterRegion, InterRegionServer};
    // pub use self::PacketWrapper;
}

/* ─── Internal Modules ─── */
pub mod detect;
pub mod hub;
pub mod block_relayer;
pub mod searcher;
pub mod block_engine;
pub mod peer_manager;
pub mod inter_region;
pub mod utils;
pub mod blacklist;
pub mod arbitrage_feed;  
pub mod filter_feed;
pub mod tx_fast;

/* ─── re‑export ─── */
pub use detect::LOCAL_REGION;
pub use peer_manager::PEER_MGR;

