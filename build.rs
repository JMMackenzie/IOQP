//! Here, we generate Rust code from a proto file before project compilation.

fn main() {
    prost_build::compile_protos(&["src/ciff_proto/common-index-format-v1.proto"], &["src/"])
        .unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}
