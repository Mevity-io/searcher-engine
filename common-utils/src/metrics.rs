use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use lazy_static::lazy_static;
use prometheus::{
    gather, register_counter, register_histogram, register_int_gauge, Counter, Encoder, Histogram, IntGauge, TextEncoder
};

lazy_static! {
    pub static ref BUNDLE_IN_TOTAL:  Counter   =
        register_counter!("bundle_in_total",  "Bundles Received").unwrap();
    pub static ref BUNDLE_OUT_TOTAL: Counter   =
        register_counter!("bundle_out_total", "Bundles Transmit").unwrap();
    pub static ref SEARCHER_BUNDLE_IN_TOTAL:  Counter   =
        register_counter!("searcher_bundle_in_total",  "Searcher Bundles Received").unwrap();
    pub static ref SEARCHER_BUNDLE_OUT_TOTAL: Counter   =
        register_counter!("searcher_bundle_out_total", "Searcher Bundles Transmit").unwrap();
    pub static ref SEARCHER_BUNDLE_DROP_TOTAL: Counter   =
        register_counter!("searcher_bundle_drop_total", "Searcher Bundles Drop").unwrap();
    pub static ref SEARCHER_COUNT: IntGauge =
        register_int_gauge!("searcher_count", "searcher Connection").unwrap();
    pub static ref PKT_IN_TOTAL:     Counter   =
        register_counter!("packet_in_total",  "Packets Received").unwrap();
    pub static ref PKT_OUT_TOTAL:     Counter   =
        register_counter!("packet_out_total",  "Packets Transmit").unwrap();
    pub static ref PACKET_DROP_TOTAL:  Counter =
        register_counter!("packet_drop_total",  "Dropped packets due to slow subscriber").unwrap();
    pub static ref BUNDLE_DROP_TOTAL:  Counter =
        register_counter!("bundle_drop_total",  "Dropped bundles due to slow subscriber").unwrap();
    pub static ref SEARCHER_PACKET_LATENCY_MS:       Histogram =
        register_histogram!(
            "seacher_packet_latency_ms",
            "relayer → searcher Latency(ms)",
            vec![1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
        )
        .unwrap();
    pub static ref SEARCHER_BUNDLE_LATENCY_MS:       Histogram =
        register_histogram!(
            "seacher_bundle_latency_ms",
            "searcher → validator Latency(ms)",
            vec![1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
        )
        .unwrap();
}

/// Background HTTP `/metrics` Server
pub async fn serve(addr: ([u8; 4], u16)) {
    let make_svc = make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(|_req: Request<Body>| async {
            let metric_families = gather();
            let mut buf = Vec::new();
            TextEncoder::new()
                .encode(&metric_families, &mut buf)
                .unwrap();
            Ok::<_, hyper::Error>(Response::new(Body::from(buf)))
        }))
    });

    let server = Server::bind(&addr.into()).serve(make_svc);

    println!(
        "📊 metrics listening on {}:{}",
        addr.0
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join("."),
        addr.1
    );

    if let Err(e) = server.await {
        eprintln!("metrics server error: {e}");
    }
}