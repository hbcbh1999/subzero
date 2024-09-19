// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
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
