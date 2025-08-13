fn main() {
    tonic_build::configure()
        // .extern_path(".google.protobuf.Empty", "::prost_types::Empty")
        .extern_path(".packet",               "::jito_protos::packet")
        .extern_path(".bundle",               "::jito_protos::bundle")
        .compile(
            &["protos/searcher_engine.proto", "protos/inter_region.proto",],
            &["protos", "../jito-protos/protos"],
        )
        .unwrap();
}