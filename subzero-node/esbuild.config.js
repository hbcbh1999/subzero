/* eslint-disable */
let wasmPlugin = {
    name: 'wasm',
    setup(build) {
        let path = require('path')
        let fs = require('fs')

        // Resolve ".wasm" files to a path with a namespace
        build.onResolve({ filter: /\.wasm$/ }, args => {
            if (args.resolveDir === '') {
                return // Ignore unresolvable paths
            }
            return {
                path: path.isAbsolute(args.path) ? args.path : path.join(args.resolveDir, args.path),
                namespace: 'wasm-binary',
            }
        })

        // Virtual modules in the "wasm-binary" namespace contain the
        // actual bytes of the WebAssembly file. This uses esbuild's
        // built-in "binary" loader instead of manually embedding the
        // binary data inside JavaScript code ourselves.
        build.onLoad({ filter: /.*/, namespace: 'wasm-binary' }, async (args) => ({
            contents: await fs.promises.readFile(args.path),
            loader: 'binary',
        }))
    },
}

let file_header = `/**
 * @license Subzero
 * 
 * Copyright (c) subZero Cloud S.R.L
 *
 * See LICENSE.txt file for more info.
 */

 /* eslint-disable */
 /* tslint:disable */
`;
require('esbuild').build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    platform: 'neutral',
    mainFields: ['module', 'main'],
    outfile: 'dist/index.js',
    minify: false,
    sourcemap: true,
    banner: {
        js: file_header,
    },
    loader: {
        '.sql': 'text',
    },
    plugins: [wasmPlugin],
}).catch(err => {
    process.stderr.write(err.stderr);
    process.exit(1)
});
 /* eslint-disable */