import init, {initSync, Backend } from '../../subzero-wasm/pkg-web/subzero_wasm.js'
import wasmbin from '../../subzero-wasm/pkg-web/subzero_wasm_bg.wasm'
import { SubzeroInternal, DbType } from './subzero'

let wasmPromise: Promise<any>
if (typeof wasmbin === 'object') {
    try {
        initSync(wasmbin)
    }
    catch (e) {
        wasmPromise = init(wasmbin)
    }
}
else {
    wasmPromise = init(wasmbin)
}


export default class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions, wasmPromise)
    }
}

export type {
    DbType,
    Query,
    Parameters,
    Statement,
    GetParameters,
    Method,
    Body,
    Headers,
    Cookies,
    Env,
} from './subzero'


export {
    SqliteTwoStepStatement,
    SubzeroError,
    fmtContentRangeHeader,
    fmtPostgreSqlEnv,
    fmtMySqlEnv,
    getIntrospectionQuery,
    getRawIntrospectionQuery,
    parseRangeHeader,
    statusFromPgErrorCode
} from './subzero'
