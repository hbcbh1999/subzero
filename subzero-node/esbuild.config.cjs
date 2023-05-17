/* eslint-disable */

let esbuild = require('esbuild');
let x = require('esbuild-plugin-copy');
let fs = require('fs');
// const cjs_to_esm_plugin = {
//     name: 'cjs-to-esm',
//     setup(build) {
//       build.onResolve({ filter: /.*/ }, args => {
//         if (args.importer === '') return { path: args.path, namespace: 'c2e' }
//       })
//       build.onLoad({ filter: /.*/, namespace: 'c2e' }, args => {
//         const keys = Object.keys(require(args.path)).join(', ')
//         const path = JSON.stringify(args.path)
//         const resolveDir = __dirname
//         return { contents: `export { ${keys} } from ${path}`, resolveDir }
//       })
//     },
//   }

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
let fix_node_esbuild = `
import { createRequire } from "module";
import { fileURLToPath as urlESMPluginFileURLToPath } from "url";
import { dirname as pathESMPluginDirname} from "path";
var require = createRequire(import.meta.url);
var __filename =urlESMPluginFileURLToPath(import.meta.url);
var __dirname = pathESMPluginDirname(urlESMPluginFileURLToPath(import.meta.url));
`;

//load package.json
let pkgCommon = require('./package.json');
delete pkgCommon.devDependencies;
delete pkgCommon.scripts;
pkgCommon.module = 'index.js';
pkgCommon.main = 'index.js';
pkgCommon.types = 'index.d.ts';
pkgCommon.type = 'module';
pkgCommon.private = false;
pkgCommon.files = ['index.js', 'index.d.ts', 'index.js.map', '*.wasm'];


// Build for browser/worker
// esbuild.build({
//     entryPoints: ['src/web.ts'],
//     bundle: true,
//     platform: 'neutral',
//     external: ['fs','path'],
//     mainFields: ['module', 'main'],
//     outfile: 'dist-web/index.js',
//     minify: true,
//     sourcemap: true,
//     banner: {js: file_header},
//     loader: { '.sql': 'text', '.wasm': 'copy' },
//     plugins: [
//         //cjs_to_esm_plugin,
//         x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
//         x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),
//     ]
// })
// .then(() => {
//     let pkg = Object.assign({}, pkgCommon);
//     pkg.name = '@subzerocloud/web';
//     fs.writeFileSync('dist-web/package.json', JSON.stringify(pkg, null, 2));
// })
// .catch(err => {
//     process.stderr.write(err.stderr);
//     process.exit(1)
// });

// Build for nodejs
// esbuild.build({
//     entryPoints: ['src/nodejs.ts'],
//     bundle: true,
//     platform: 'node',
//     // format: 'esm',
//     mainFields: ['module', 'main'],
//     //external: ['fs','path','util'],
//     outfile: 'dist-nodejs/index.js',
//     //minify: true,
//     sourcemap: true,
//     //banner: {js: file_header + "\n" + fix_node_esbuild},
//     banner: {js: file_header + "\n"},
    
//     loader: { '.sql': 'text', '.wasm': 'copy' },
//     plugins: [
//         //cjs_to_esm_plugin,
//         x.copy({
//             assets: {
//                 from: ['../subzero-wasm/pkg-node/subzero_wasm_bg.wasm'],
//                 to: ['subzero_wasm_bg.wasm']
//             }
//         }),
//         x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
//         x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),
//     ]
// })
// .then(() => {
//     let pkg = Object.assign({}, pkgCommon);
//     pkg.name = '@subzerocloud/nodejs';
//     //pkg.name = '@subzerocloud/rest';
//     delete pkg.type;
//     delete pkg.module;
//     fs.writeFileSync('dist-nodejs/package.json', JSON.stringify(pkg, null, 2));
// })
// .catch(err => {
//     process.stderr.write(err.stderr);
//     process.exit(1)
// });


esbuild.build({
    entryPoints: ['src/rest.ts'],
    bundle: true,
    platform: 'node',
    // format: 'esm',
    mainFields: ['module', 'main'],
    //external: ['fs','path','util'],
    outfile: 'dist-rest/index.js',
    //minify: true,
    sourcemap: true,
    //banner: {js: file_header + "\n" + fix_node_esbuild},
    banner: {js: file_header + "\n"},
    
    loader: { '.sql': 'text', '.wasm': 'copy' },
    plugins: [
        //cjs_to_esm_plugin,
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
    //pkg.name = '@subzerocloud/nodejs';
    pkg.name = '@subzerocloud/rest';
    delete pkg.type;
    delete pkg.module;
    fs.writeFileSync('dist-rest/package.json', JSON.stringify(pkg, null, 2));
})
.catch(err => {
    process.stderr.write(err.stderr);
    process.exit(1)
});

// Build for bundler
// esbuild.build({
//     entryPoints: ['src/bundler.ts'],
//     //bundle: true,
//     platform: 'node',
//     format: 'esm',
//     mainFields: ['module', 'main'],
//     //external: ['fs','path','util'],
//     outfile: 'dist-bundler/index.js',
//     //minify: true,
//     //sourcemap: true,
//     //banner: {js: file_header + "\n" + fix_node_esbuild},
//     banner: {js: file_header + "\n"},
    
//     loader: { '.sql': 'text', '.wasm': 'copy' },
//     plugins: [
//         //cjs_to_esm_plugin,
//         // x.copy({
//         //     assets: {
//         //         from: ['../subzero-wasm/pkg-bundler/subzero_wasm_bg.js'],
//         //         to: ['subzero_wasm_bg.js']
//         //     }
//         // }),
//         x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
//         x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),

//     ]

// })
// .then(() => {
//     let pkg = Object.assign({}, pkgCommon);
//     //pkg.name = '@subzerocloud/nodejs';
//     pkg.name = '@subzerocloud/rest';
//     pkg.bundledDependencies = {
//         'subzero-wasm': "file:../subzero-wasm/pkg-bundler"
//     };
//     // delete pkg.type;
//     // delete pkg.module;
//     fs.writeFileSync('dist-bundler/package.json', JSON.stringify(pkg, null, 2));
// })
// .catch(err => {
//     process.stderr.write(err.stderr);
//     process.exit(1)
// });


// Build for deno
// esbuild.build({
//     entryPoints: ['src/deno.ts'],
//     bundle: true,
//     platform: 'neutral',
//     format: 'esm',
//     external: ['fs','path'],
//     mainFields: ['module', 'main'],
//     outfile: 'dist-deno/index.js',
//     minify: true,
//     sourcemap: true,
//     banner: {js: file_header},
//     loader: { '.sql': 'text', '.wasm': 'copy' },
//     plugins: [
//         //cjs_to_esm_plugin,
//         x.copy({
//             assets: {
//                 from: ['../subzero-wasm/pkg-deno/subzero_wasm_bg.wasm'],
//                 to: ['subzero_wasm_bg.wasm']
//             }
//         }),
//         x.copy({ assets: { from: ['./README.md'], to: ['README.md'] } }),
//         x.copy({assets: {from: ['../LICENSE.txt'],to: ['LICENSE.txt']}}),
//     ]
// })
//     .then(() => {
//         let pkg = Object.assign({}, pkgCommon);
//         pkg.name = '@subzerocloud/deno';
//         fs.writeFileSync('dist-deno/package.json', JSON.stringify(pkg, null, 2));
//     })
//     .catch(err => {
//         process.stderr.write(err.stderr);
//         process.exit(1)
//     });


 /* eslint-disable */