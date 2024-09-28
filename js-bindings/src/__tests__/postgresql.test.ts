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
import { expect, test, beforeAll, describe, afterAll, jest } from '@jest/globals';
import Subzero, { Statement, getIntrospectionQuery, fmtPostgreSqlEnv, Env } from '../rest';
import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import { runPermissionsTest, runSelectTest, runUpdateTest, runInsertTest } from './shared/shared'
import dotenv from 'dotenv';
dotenv.config({ path: `${__dirname}/../../../.github/.env`});

const { POSTGRES_USER,POSTGRES_PASSWORD,POSTGRES_DB} = process.env;

const dbPool = new Pool({
  connectionString: `postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}`,
})
function normalize_statement(s: Statement) {
  return {
    query: s.query.replace(/\s+/g, ' ').trim(),
    parameters: s.parameters,
  };
}
const base_url = 'http://localhost:3000/rest';
//const subzero = new Subzero('postgresql', schema);
let subzero: Subzero;
beforeAll(async () => {
  console.warn = jest.fn();
  //jest.useFakeTimers();
  const db = await dbPool.connect();
  const permissions = JSON.parse(fs.readFileSync(path.join(__dirname, 'permissions.json')).toString());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeholder_values = new Map<string, any>([['permissions.json', permissions]]);
  const { query, parameters } = getIntrospectionQuery('postgresql', 'public', placeholder_values);
  const result = await db.query(query, parameters);
  //const s = JSON.parse(result.rows[0].json_schema)
  //console.log(s.schemas[0].objects.map((o: any) => o.name))
  const schema = JSON.parse(result.rows[0].json_schema);

  //initialize the subzero instance
  subzero = new Subzero('postgresql', schema);
  db.release();
});

// execute the queries for a given parsed request
async function run(role: string, req: Request, queryEnv?: Env) {
  const method = req.method || 'GET';
  const schema = 'public';
  const env = queryEnv || [];
  const prefix = `/rest/`;
  const { query: envQuery, parameters: envParameters } = fmtPostgreSqlEnv(env);
  const { query, parameters } = await subzero.fmtStatement(schema, prefix, role, req, env);
  let result;
  const db = await dbPool.connect();
  try {
    const txMode = method === 'GET' ? 'READ ONLY' : 'READ WRITE';
    await db.query(`BEGIN ISOLATION LEVEL READ COMMITTED ${txMode}`);
    await db.query(envQuery, envParameters);
    result = (await db.query(query, parameters)).rows[0];
    await db.query('ROLLBACK');
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  } finally {
    db.release();
  }
  return result.body?JSON.parse(result.body):null;
}


describe('query shape tests', () => {
  test('main query', async () => {
    expect(
      normalize_statement(
        await subzero.fmtStatement(
          'public',
          '/rest/',
          'anonymous',
          new Request(`${base_url}/tasks?select=id,name&id=eq.1`, {
            method: 'GET',
            headers: {
              Accept: 'application/json',
            },
          }),
          [
            ['role', 'anonymous'],
            ['request', '{"method":"GET"}'],
          ], // env
        ),
      ),
    ).toStrictEqual(
      normalize_statement({
        query: `
            with 
            env as materialized (select $1 as "request",$2 as "role")
            , _subzero_query as (
                select "public"."tasks"."id", "public"."tasks"."name" from "public"."tasks", env where "public"."tasks"."id" = $3
                and ((true))
            )
            , _subzero_count_query AS (select 1)
            select
                pg_catalog.count(_subzero_t) as page_total,
                null::bigint as total_result_set,
                coalesce(json_agg(_subzero_t), '[]')::character varying as body,
                true as constraints_satisfied,
                nullif(current_setting('response.headers', true), '') as response_headers,
                nullif(current_setting('response.status', true), '') as response_status
            from ( select * from _subzero_query ) _subzero_t
            `,
        parameters: ['{"method":"GET"}', 'anonymous', 1],
      }),
    );
  });
});

runPermissionsTest('postgresql', base_url, run);
runSelectTest('postgresql', base_url, run);
runUpdateTest('postgresql', base_url, run);
runInsertTest('postgresql', base_url, run);

afterAll(async () => {
  await dbPool.end();
  jest.runOnlyPendingTimers();
  //jest.useRealTimers();
});