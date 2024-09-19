This directory contans a library that wraps the subzero core library in a WASM module which is used by the [js-bindings](../js-bindings) code which exposes the core library to the js/ts environment.

## Building

```bash
wasm-pack build --release --target web --out-dir=pkg-web
wasm-pack build --release --target nodejs --out-dir=pkg-node
```