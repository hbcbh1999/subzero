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
import { beforeAll, afterAll, jest } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import Subzero, { getIntrospectionQuery, Env } from '../rest';
import { runPermissionsTest, runSelectTest, runUpdateTest, runInsertTest } from './shared/shared'

// Declare global variables
sqlite3.verbose();
let db: Database<sqlite3.Database>;
let subzero: Subzero;
const base_url = 'http://localhost:3000/rest';

beforeAll(async () => {
  console.warn = jest.fn();
  //jest.useFakeTimers();
  db = await open({ filename: ':memory:', driver: sqlite3.Database });
  // Read the init SQL file
  const loadSql = fs.readFileSync(path.join(__dirname, 'load.sql')).toString().split(';');

  loadSql.forEach(async (sql) => await db.exec(sql));

  // note: although we have a second parameter that lists the schemas
  // in case of sqlite it is not used in the query (sqlite does not have the notion of schemas)
  // that is why we don't read/use the `parameters` from the result
  const permissions = JSON.parse(fs.readFileSync(path.join(__dirname, 'permissions.json')).toString());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeholder_values = new Map<string, any>([['permissions.json', permissions]]);
  const { query } = getIntrospectionQuery('sqlite', 'public', placeholder_values);
  const result = await db.get(query);
  const schema = JSON.parse(result.json_schema);

  //initialize the subzero instance
  subzero = new Subzero('sqlite', schema);
  // await subzero.init();

  //let t = await db.all('select rowid as rowid from projects where id in (select value from json_each($1))', ['[1, 2, 3]']);
  //console.log('test', t);
});

// execute the queries for a given parsed request
async function run(role: string, request: Request, env?: Env) {
  env = env || [];
  
  
  if (request.method == 'GET') {
    const { query, parameters } = await subzero.fmtStatement('public', '/rest/', role, request, env);

    const result = await db.get(query, parameters);
    
    // console.log('result', result.body);
    return JSON.parse(result.body);
  } else {
    const statement = await subzero.fmtTwoStepStatement('public', '/rest/', role, request, env);
    const { query: mutate_query, parameters: mutate_parameters } = statement.fmtMutateStatement();
    //console.log(mutate_query,"\n",mutate_parameters);
    const result = await db.all(mutate_query, mutate_parameters);
    statement.setMutatedRows(result);
    const returnRepresentation = request.headers.get('Prefer')?.includes('return=representation');
    if (!returnRepresentation) {
      return null;
    }
    //console.log(result);
    //const ids = result.map((r) => r[Object.keys(r)[0]].toString());
    //console.log('ids',ids);
    const { query: select_query, parameters: select_parameters } = statement.fmtSelectStatement();
    //console.log(select_query,"\n",select_parameters);
    const result2 = await db.get(select_query, select_parameters);
    //console.log(result2);
    return JSON.parse(result2.body);
  }
}

runPermissionsTest('sqlite', base_url, run);
runSelectTest('sqlite', base_url, run);
runUpdateTest('sqlite', base_url, run);
runInsertTest('sqlite', base_url, run);

afterAll(async () => {
  await db.close();
  jest.runOnlyPendingTimers();
  //jest.useRealTimers();

});
