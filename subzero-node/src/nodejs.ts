import { Backend } from '../../subzero-wasm/pkg-node/subzero_wasm.js'
import { SubzeroInternal, DbType } from './subzero'
export * from './subzero'

export class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
}