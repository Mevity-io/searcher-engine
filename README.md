# Searcher Engine

A high-performance **Solana MEV Searcher Engine** built in Rust, designed to connect to [Jito](https://jito.wtf) block engines, relayers, and peer searchers for low-latency transaction flow, arbitrage detection, and bundle submission.

## Overview

This repository contains a Rust workspace with multiple crates:

- **`jito-protos/`** – Protobuf definitions and gRPC service bindings for Jito Solana components (block engine, relayer, searcher, shredstream, etc.).
- **`searcher-engine/`** – Main runtime for handling Solana packet streams, filtering arbitrage opportunities, managing peer regions, and submitting bundles.
- **`utils/`** – Python scripts for mocking block/searcher engines and testing arbitrage targeting.

Core features include:

- **gRPC APIs** for both Searchers and Validators  
- **Packet ingestion & fan-out** via local hub + inter-region peer relays  
- **Arbitrage filtering** for targeted Solana AMMs (Raydium, Orca, Meteora, Pump.fun, etc.)  
- **Redis integration** for leader scheduling & TX tracking  
- **ClickHouse logging** for bundle lifecycle events  
- **Metrics** exposed via Prometheus

## Architecture

- **Hub** – Central async broadcaster for packets & bundles
- **Peer Manager** – Maintains connections to other searcher engines in different regions
- **Arbitrage Feed** – Zero-copy stream of filtered, decoded AMM swap transactions
- **Front-run Protection** – Drops bundles containing already-seen mempool TXs
- **Inter-Region Service** – Shares packet & bundle data between regions

## Getting Started

### 1. Clone & Build
```bash
git clone https://github.com/Mevity-io/searcher-engine.git
cd searcher-engine
cargo build --release
```

### 2. Configure
Copy `.env.example` to `.env` and set required environment variables:

```env
RPC_SERVER=https://api.mainnet-beta.solana.com
MEVITY_HOST=https://your-mevity-api
MEVITY_API_KEY=yourkey
REDIS_SERVER=redis://127.0.0.1/
SEARCHER_ENGINE_GRPC=0.0.0.0:50052
```

### 3. Run
```bash
cargo run -p searcher-engine
```

## Development

- **Proto Compilation** – `prost` + `tonic-build`  
- **Async Runtime** – `tokio` with multi-threaded scheduler  
- **Parallel Parsing** – `rayon` for CPU-bound transaction decoding  
- **Fast TX Sig Extraction** – Zero-copy base58 encoder for `VersionedTransaction`

## License

MIT License. See [LICENSE](LICENSE) for details.
