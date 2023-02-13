import { Backend } from '../../subzero-wasm/pkg-node/subzero_wasm.js'
import { SubzeroInternal, DbType } from './subzero'
export * from './subzero'
const wasmPromise = Promise.resolve() // in node wasm is already loaded

export class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
    async init() {
        await super.init(wasmPromise)
    }
}