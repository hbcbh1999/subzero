extern crate cbindgen;

use std::env;
use std::path::PathBuf;
// use cbindgen::Config;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = target_dir().join(format!("{}.h", package_name.replace("-ffi", ""))).display().to_string();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_namespaces(&["subzero" /*,"ffi"*/])
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(output_file);
}

/// Find the location of the `target/` directory. Note that this may be
/// overridden by `cmake`, so we also need to check the `CARGO_TARGET_DIR`
/// variable.
fn target_dir() -> PathBuf {
    if let Ok(target) = env::var("CARGO_TARGET_DIR") {
        PathBuf::from(target)
    } else {
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("..")
            .join("target")
            .join(env::var("PROFILE").unwrap())
    }
}
