import dts from "rollup-plugin-dts";

const config = [
    {
        input: "./declarations/worker.d.ts",
        output: [{ file: "./dist-worker/index.d.ts", format: "es" }],
        plugins: [dts()],
    },
    {
        input: "./declarations/nodejs.d.ts",
        output: [{ file: "./dist-nodejs/index.d.ts", format: "es" }],
        plugins: [dts()],
    },
];

export default config;