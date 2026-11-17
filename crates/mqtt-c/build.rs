fn main() {
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
