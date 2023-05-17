import dts from "rollup-plugin-dts";

const config = [
    // {
    //     input: "./declarations/web.d.ts",
    //     external: ['http'],
    //     output: [{ file: "./dist-web/index.d.ts", format: "es" }],
    //     plugins: [dts()],
    // },
    {
        input: "./declarations/nodejs.d.ts",
        external: ['http'],
        output: [{ file: "./dist-nodejs/index.d.ts", format: "es" }],
        plugins: [dts()],
    },
    // {
    //     input: "./declarations/bundler.d.ts",
    //     external: ['http'],
    //     output: [{ file: "./dist-bundler/index.d.ts", format: "es" }],
    //     plugins: [dts()],
    // },
    // {
    //     input: "./declarations/deno.d.ts",
    //     external: ['http'],
    //     output: [{ file: "./dist-deno/index.d.ts", format: "es" }],
    //     plugins: [dts()],
    // },
];

export default config;