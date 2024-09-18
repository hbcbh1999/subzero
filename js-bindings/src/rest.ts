import { Backend } from '../../wasm/pkg-node/subzero_wasm.js'
import { SubzeroInternal, initInternal } from './subzero'
import type { DbType, DbPool, InitOptions } from './subzero'
import type { Express} from 'express'
export * from './subzero'

export default class Subzero extends SubzeroInternal {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
}

export async function init(
    app: Express,
    dbType: DbType,
    dbPool: DbPool,
    dbSchemas: string[],
    options: InitOptions = {},
): Promise<SubzeroInternal | undefined> {
    return await initInternal(Backend, app, dbType, dbPool, dbSchemas, options)
}
