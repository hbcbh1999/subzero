import { beforeAll, afterAll, } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import Subzero, { getIntrospectionQuery, Env } from '../nodejs';
import { runPemissionsTest, runSelectTest, runUpdateTest } from './shared/shared'

// Declare global variables
sqlite3.verbose();
let db: Database<sqlite3.Database>;
let subzero: Subzero;
const base_url = 'http://localhost:3000/rest';

beforeAll(async () => {
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
    const statement = await subzero.fmtSqliteTwoStepStatement('public', '/rest/', role, request, env);
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

runPemissionsTest(base_url, run);
runSelectTest(base_url, run);
runUpdateTest(base_url, run);

afterAll(async () => {
  await db.close();
});
