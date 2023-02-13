import {initSync, Backend } from '../../subzero-wasm/pkg-web/subzero_wasm.js'
import wasmbin from '../../subzero-wasm/pkg-web/subzero_wasm_bg.wasm'
import { SubzeroInternal, DbType } from './subzero'
export * from './subzero'

initSync(wasmbin)

export class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
}