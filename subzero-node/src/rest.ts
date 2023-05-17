import { Backend } from '../../subzero-wasm/pkg-node/subzero_wasm.js'
import { DbType, SubzeroInternal } from './subzero'
export default class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
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
