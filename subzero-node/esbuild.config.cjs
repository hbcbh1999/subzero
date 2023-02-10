/* eslint-disable */




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
    minify: true,
    sourcemap: true,
    banner: {
        js: file_header,
    },
    loader: {
        '.sql': 'text',
        '.wasm': 'copy',
    },
    plugins: [
        
    ],
}).catch(err => {
    process.stderr.write(err.stderr);
    process.exit(1)
});
 /* eslint-disable */