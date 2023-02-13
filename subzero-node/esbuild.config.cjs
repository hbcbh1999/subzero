/* eslint-disable */

let esbuild = require('esbuild');
let x = require('esbuild-plugin-copy');
let fs = require('fs');

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

//load package.json
let pkgCommon = require('./package.json');
delete pkgCommon.devDependencies;
delete pkgCommon.scripts;
pkgCommon.module = 'index.js';
pkgCommon.main = 'index.js';
pkgCommon.types = 'index.d.ts';
pkgCommon.type = 'module';
pkgCommon.files = ['index.js', 'index.d.ts', 'index.js.map', '*.wasm'];


// Build for browser/worker
esbuild.build({
    entryPoints: ['src/worker.ts'],
    bundle: true,
    platform: 'neutral',
    external: ['fs','path'],
    mainFields: ['module', 'main'],
    outfile: 'dist-worker/index.js',
    //minify: true,
    sourcemap: true,
    banner: {js: file_header},
    loader: { '.sql': 'text', '.wasm': 'copy' },
    plugins: [
        x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
        x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),
    ]
})
    .then(() => {
        let pkg = Object.assign({}, pkgCommon);
        pkg.name = '@subzerocloud/worker';
        fs.writeFileSync('dist-worker/package.json', JSON.stringify(pkg, null, 2));
    })
    .catch(err => {
        process.stderr.write(err.stderr);
        process.exit(1)
    });

// Build for nodejs
esbuild.build({
    entryPoints: ['src/nodejs.ts'],
    bundle: true,
    platform: 'node',
    external: ['fs','path'],
    mainFields: ['module', 'main'],
    outfile: 'dist-nodejs/index.js',
    //minify: true,
    sourcemap: true,
    banner: {js: file_header},
    loader: { '.sql': 'text', '.wasm': 'copy' },
    plugins: [
        x.copy({
            assets: {
                from: ['../subzero-wasm/pkg-node/subzero_wasm_bg.wasm'],
                to: ['subzero_wasm_bg.wasm']
            }
        }),
        x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
        x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),

    ]

})
    .then(() => {
        let pkg = Object.assign({}, pkgCommon);
        pkg.name = '@subzerocloud/nodejs';
        fs.writeFileSync('dist-nodejs/package.json', JSON.stringify(pkg, null, 2));
    })
    .catch(err => {
        process.stderr.write(err.stderr);
        process.exit(1)
    });

 /* eslint-disable */