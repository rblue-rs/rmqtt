fn main() {
    cc::Build::new()
        .include("third_party/dart")
        .flag_if_supported("-Wno-unused-parameter")
        .file("third_party/dart/dart_api_dl.c")
        .file("src/dart_bridge.c")
        .compile("mqtt_dart_api_dl");
    println!("cargo:rerun-if-changed=src/ownership.rs");
    println!("cargo:rerun-if-changed=src/dart_bridge.c");
    println!("cargo:rerun-if-changed=third_party/dart");
    let root = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    println!("cargo:rerun-if-changed=src/lib.rs");
    let config = cbindgen::Config::from_file(root.join("bindings/c/cbindgen.toml")).unwrap();
    cbindgen::Builder::new()
        .with_crate(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .with_config(config)
        .generate()
        .expect("generate C header")
        .write_to_file(root.join("bindings/c/include/mqtt_client.h"));
}
