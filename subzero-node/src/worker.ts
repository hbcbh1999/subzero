import init, { Backend } from '../../subzero-wasm/pkg-web/subzero_wasm.js'
import wasmbin from '../../subzero-wasm/pkg-web/subzero_wasm_bg.wasm'
import { SubzeroInternal, DbType } from './subzero'
export * from './subzero'
const wasmPromise = init(wasmbin)

export class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
    async init() {
        await super.init(wasmPromise)
    }
}