This is the subzero's internal core library which is Rust-based and compiled to WASM.
It's Responsible parsing and formatting queries.

build with

```
wasm-pack build --target web
wasm-pack build --target nodejs
```

new mode
```
cargo build --package subzero-wasm --target=wasm32-unknown-unknown --release
wasm-bindgen --out-dir=pkg --target=web --omit-default-module-path ./target/wasm32-unknown-unknown/release/subzero_wasm.wasm
```